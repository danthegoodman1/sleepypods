//! Admission before tonic connection spawning, with finite setup and write stalls.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};
use tonic::{codegen::tokio_stream::Stream, transport::server::Connected};

#[derive(Clone, Debug)]
pub(crate) struct ConnectionProgress {
    started: Arc<AtomicBool>,
    cancellation: crate::runtime_work::Cancellation,
}
impl ConnectionProgress {
    pub fn request_started(&self) {
        self.started.store(true, Ordering::Relaxed);
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

pub(crate) struct BoundedIncoming {
    listener: TcpListener,
    capacity: Arc<Semaphore>,
    setup_timeout: Duration,
    write_timeout: Duration,
}
impl BoundedIncoming {
    pub async fn bind(
        address: std::net::SocketAddr,
        capacity: Arc<Semaphore>,
        setup_timeout: Duration,
        write_timeout: Duration,
    ) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(address).await?,
            capacity,
            setup_timeout,
            write_timeout,
        })
    }
    #[cfg(test)]
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}
impl Stream for BoundedIncoming {
    type Item = io::Result<BoundedConnection>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Bound rejection work per poll too: a socket flood cannot monopolize
        // the runtime thread. No per-socket task is allocated for rejected peers.
        for _ in 0..32 {
            let (stream, _) = match self.listener.poll_accept(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Ok(socket)) => socket,
            };
            if let Ok(permit) = self.capacity.clone().try_acquire_owned() {
                let _ = stream.set_nodelay(true);
                let cancellation = crate::runtime_work::Cancellation::new();
                let signal = cancellation.clone();
                return Poll::Ready(Some(Ok(BoundedConnection {
                    stream,
                    _permit: permit,
                    progress: ConnectionProgress {
                        started: Arc::new(AtomicBool::new(false)),
                        cancellation,
                    },
                    closed: Box::pin(async move { signal.cancelled().await }),
                    setup: Box::pin(tokio::time::sleep(self.setup_timeout)),
                    idle: Box::pin(tokio::time::sleep(Duration::from_secs(60))),
                    write_timeout: self.write_timeout,
                    stalled_write: None,
                    stalled_flush: None,
                    stalled_shutdown: None,
                })));
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

pub(crate) struct BoundedConnection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    progress: ConnectionProgress,
    closed: Pin<Box<dyn Future<Output = ()> + Send>>,
    setup: Pin<Box<Sleep>>,
    idle: Pin<Box<Sleep>>,
    write_timeout: Duration,
    stalled_write: Option<Pin<Box<Sleep>>>,
    stalled_flush: Option<Pin<Box<Sleep>>>,
    stalled_shutdown: Option<Pin<Box<Sleep>>>,
}
impl Connected for BoundedConnection {
    type ConnectInfo = ConnectionProgress;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.progress.clone()
    }
}
impl AsyncRead for BoundedConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.progress.cancellation.is_cancelled() || self.closed.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "control-plane response delivery deadline expired",
            )));
        }
        if (!self.progress.started.load(Ordering::Relaxed)
            && self.setup.as_mut().poll(cx).is_ready())
            || self.idle.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "control-plane connection setup/read inactivity expired",
            )));
        }
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buffer);
        if result.is_ready() && buffer.filled().len() > before {
            self.idle
                .as_mut()
                .reset(Instant::now() + Duration::from_secs(60));
        }
        result
    }
}
impl BoundedConnection {
    fn write_result<T>(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<T>>,
        operation: u8,
    ) -> Poll<io::Result<T>> {
        if self.progress.cancellation.is_cancelled() || self.closed.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "control-plane response delivery deadline expired",
            )));
        }
        let slot = match operation {
            0 => &mut self.stalled_write,
            1 => &mut self.stalled_flush,
            _ => &mut self.stalled_shutdown,
        };
        if result.is_ready() {
            *slot = None;
            return result;
        }
        let timer = slot.get_or_insert_with(|| Box::pin(tokio::time::sleep(self.write_timeout)));
        if timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "control-plane socket write stalled",
            )));
        }
        Poll::Pending
    }
}
impl AsyncWrite for BoundedConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.progress.cancellation.is_cancelled() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "control-plane connection cancelled",
            )));
        }
        let result = Pin::new(&mut self.stream).poll_write(cx, buffer);
        self.write_result(cx, result, 0)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.progress.cancellation.is_cancelled() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "control-plane connection cancelled",
            )));
        }
        let result = Pin::new(&mut self.stream).poll_flush(cx);
        self.write_result(cx, result, 1)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.progress.cancellation.is_cancelled() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "control-plane connection cancelled",
            )));
        }
        let result = Pin::new(&mut self.stream).poll_shutdown(cx);
        self.write_result(cx, result, 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::codegen::tokio_stream::StreamExt;

    #[tokio::test]
    async fn actual_listener_rejects_overload_expires_setup_and_recovers() {
        let capacity = Arc::new(Semaphore::new(1));
        let incoming = BoundedIncoming::bind(
            "127.0.0.1:0".parse().unwrap(),
            capacity.clone(),
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown, receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .layer(crate::api::admission::RpcAdmissionLayer::new(4))
                .add_service(crate::runtime::tests::noop_operator_service())
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = receiver.await;
                }),
        );
        let mut stalled = TcpStream::connect(address).await.unwrap();
        stalled.write_all(b"PRI * HTTP/2.0\r\n").await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while capacity.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut rejected = TcpStream::connect(address).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let _ = tokio::time::timeout(Duration::from_secs(1), stalled.read(&mut [0]))
            .await
            .unwrap();
        drop(stalled);
        tokio::time::timeout(Duration::from_secs(1), async {
            while capacity.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut client =
            crate::api::pb::operator_control_plane_client::OperatorControlPlaneClient::connect(
                format!("http://{address}"),
            )
            .await
            .unwrap();
        for _ in 0..2 {
            let status = client
                .get_instance(crate::api::pb::GetInstanceRequest {
                    instance_id: "anything".into(),
                })
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::Internal);
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        drop(client);
        shutdown.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(capacity.available_permits(), 1);
    }

    // Tonic's encoder has no exact size hint; keep that property while making
    // the single frame EOS observable, including after gRPC-web base64 encoding.
    struct SingleResponseBody(Option<bytes::Bytes>);
    impl http_body::Body for SingleResponseBody {
        type Data = bytes::Bytes;
        type Error = tonic::Status;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.0.take().map(|data| Ok(http_body::Frame::data(data))))
        }
        fn is_end_stream(&self) -> bool {
            self.0.is_none()
        }
    }

    #[derive(Clone)]
    struct FlowControlledService {
        subscriptions: Arc<Semaphore>,
        lifetime: Duration,
    }
    impl tonic::server::NamedService for FlowControlledService {
        const NAME: &'static str = "sleepypods.controlplane.v1.ProxyControlPlane";
    }
    impl tower::Service<tonic::codegen::http::Request<tonic::body::Body>> for FlowControlledService {
        type Response = tonic::codegen::http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(
            &mut self,
            request: tonic::codegen::http::Request<tonic::body::Body>,
        ) -> Self::Future {
            let mut response = tonic::codegen::http::Response::new(tonic::body::Body::new(
                SingleResponseBody(Some(bytes::Bytes::from(vec![7; 4096]))),
            ));
            response
                .headers_mut()
                .insert("content-type", "application/grpc".parse().unwrap());
            if request.uri().path().ends_with("/Subscribe")
                || request.uri().path().ends_with("/WatchTlsCertificates")
            {
                let Ok(permit) = self.subscriptions.clone().try_acquire_owned() else {
                    return std::future::ready(Ok(tonic::Status::resource_exhausted(
                        "subscription capacity",
                    )
                    .into_http()));
                };
                response
                    .extensions_mut()
                    .insert(crate::api::admission::SubscriptionLease {
                        permit: Arc::new(permit),
                        lifetime: self.lifetime,
                    });
            }
            std::future::ready(Ok(response))
        }
    }

    #[tokio::test]
    async fn responsive_h2_withheld_credit_retains_final_data_admission_then_recovers() {
        use http_body_util::BodyExt;
        for subscription in [false, true] {
            let sockets = Arc::new(Semaphore::new(2));
            let incoming = BoundedIncoming::bind(
                "127.0.0.1:0".parse().unwrap(),
                sockets,
                Duration::from_secs(1),
                Duration::from_millis(50),
            )
            .await
            .unwrap();
            let address = incoming.local_addr().unwrap();
            let (shutdown, receiver) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .http2_keepalive_interval(Some(Duration::from_millis(20)))
                    .http2_keepalive_timeout(Some(Duration::from_millis(20)))
                    .layer(
                        crate::api::admission::RpcAdmissionLayer::with_delivery_timeout(
                            1,
                            Duration::from_millis(150),
                        ),
                    )
                    .add_service(FlowControlledService {
                        subscriptions: Arc::new(Semaphore::new(1)),
                        lifetime: Duration::from_millis(150),
                    })
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = receiver.await;
                    }),
            );
            let path = if subscription {
                "Subscribe"
            } else {
                "WakeInstance"
            };
            let request = || {
                tonic::codegen::http::Request::builder()
                    .method("POST")
                    .uri(format!(
                        "http://{address}/sleepypods.controlplane.v1.ProxyControlPlane/{path}"
                    ))
                    .header("content-type", "application/grpc")
                    .body(http_body_util::Empty::<bytes::Bytes>::new())
                    .unwrap()
            };
            let socket = TcpStream::connect(address).await.unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .initial_stream_window_size(1)
                    .handshake(hyper_util::rt::TokioIo::new(socket))
                    .await
                    .unwrap();
            let peer = tokio::spawn(connection); // Hyper answers PING while withholding stream credit.
            let first = sender.send_request(request()).await.unwrap();
            let rejected = sender.send_request(request()).await.unwrap();
            assert_eq!(
                rejected.headers()["grpc-status"],
                "8",
                "the final queued DATA still owns capacity"
            );
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert!(
                !peer.is_finished(),
                "responsive peer remains alive before delivery expiry"
            );
            tokio::time::timeout(Duration::from_secs(2), peer)
                .await
                .unwrap()
                .unwrap()
                .unwrap_or_default();
            drop(first);
            drop(sender);
            let socket = TcpStream::connect(address).await.unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .handshake(hyper_util::rt::TokioIo::new(socket))
                    .await
                    .unwrap();
            let recovered_peer = tokio::spawn(connection);
            let recovered = sender.send_request(request()).await.unwrap();
            assert!(!recovered.headers().contains_key("grpc-status"));
            assert_eq!(
                recovered
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .len(),
                4096
            );
            drop(sender);
            recovered_peer.abort();
            shutdown.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn native_tls_progress_survives_setup_and_cancels_withheld_delivery() {
        use http_body_util::BodyExt;
        for path in ["WakeInstance", "Subscribe", "WatchTlsCertificates"] {
            let rcgen::CertifiedKey { cert, signing_key } =
                rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let mut roots = rustls::RootCertStore::empty();
            roots.add(cert.der().clone()).unwrap();
            let mut tls_client = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            tls_client.alpn_protocols = vec![b"h2".to_vec()];
            let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_client));
            let mut server_jobs = tokio::task::JoinSet::new();
            let mut peer_jobs = tokio::task::JoinSet::new();
            let sockets = Arc::new(Semaphore::new(2));
            let incoming = BoundedIncoming::bind(
                "127.0.0.1:0".parse().unwrap(),
                sockets,
                Duration::from_millis(100),
                Duration::from_millis(50),
            )
            .await
            .unwrap();
            let address = incoming.local_addr().unwrap();
            let (shutdown, receiver) = tokio::sync::oneshot::channel();
            server_jobs.spawn(
                tonic::transport::Server::builder()
                    .tls_config(tonic::transport::ServerTlsConfig::new().identity(
                        tonic::transport::Identity::from_pem(
                            cert.pem(),
                            signing_key.serialize_pem(),
                        ),
                    ))
                    .unwrap()
                    .http2_keepalive_interval(Some(Duration::from_millis(20)))
                    .http2_keepalive_timeout(Some(Duration::from_millis(20)))
                    .layer(
                        crate::api::admission::RpcAdmissionLayer::with_delivery_timeout(
                            1,
                            Duration::from_millis(300),
                        ),
                    )
                    .add_service(FlowControlledService {
                        subscriptions: Arc::new(Semaphore::new(1)),
                        lifetime: Duration::from_millis(300),
                    })
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = receiver.await;
                    }),
            );
            let request = || {
                tonic::codegen::http::Request::builder()
                    .method("POST")
                    .uri(format!(
                        "http://{address}/sleepypods.controlplane.v1.ProxyControlPlane/{path}"
                    ))
                    .header("content-type", "application/grpc")
                    .body(http_body_util::Empty::<bytes::Bytes>::new())
                    .unwrap()
            };
            let socket = connector
                .connect(
                    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    TcpStream::connect(address).await.unwrap(),
                )
                .await
                .unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .initial_stream_window_size(1)
                    .handshake(hyper_util::rt::TokioIo::new(socket))
                    .await
                    .unwrap();
            let peer = peer_jobs.spawn(connection); // Hyper answers PING while withholding stream credit.
            let first = sender.send_request(request()).await.unwrap();
            let rejected = sender.send_request(request()).await.unwrap();
            assert_eq!(
                rejected.headers()["grpc-status"],
                "8",
                "the final queued DATA still owns capacity"
            );
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                !peer.is_finished(),
                "the same verified TLS/H2 connection survives its 100ms setup deadline before delivery expiry"
            );
            tokio::time::timeout(Duration::from_secs(2), peer_jobs.join_next())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap_or_default();
            drop(first);
            drop(sender);
            let socket = connector
                .connect(
                    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    TcpStream::connect(address).await.unwrap(),
                )
                .await
                .unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .handshake(hyper_util::rt::TokioIo::new(socket))
                    .await
                    .unwrap();
            let recovered_peer = peer_jobs.spawn(connection);
            let recovered = sender.send_request(request()).await.unwrap();
            assert!(!recovered.headers().contains_key("grpc-status"));
            assert_eq!(
                recovered
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .len(),
                4096
            );
            drop(sender);
            recovered_peer.abort();
            while peer_jobs.join_next().await.is_some() {}
            shutdown.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(2), server_jobs.join_next())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn grpc_web_text_encoded_final_data_retains_rpc_admission_then_recovers() {
        use http_body_util::BodyExt;
        {
            let subscription = false;
            let sockets = Arc::new(Semaphore::new(2));
            let incoming = BoundedIncoming::bind(
                "127.0.0.1:0".parse().unwrap(),
                sockets,
                Duration::from_secs(1),
                Duration::from_millis(50),
            )
            .await
            .unwrap();
            let address = incoming.local_addr().unwrap();
            let (shutdown, receiver) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(
                crate::runtime::admitted_grpc_web_server_builder(
                    crate::api::admission::RpcAdmissionLayer::with_delivery_timeout(
                        1,
                        Duration::from_millis(150),
                    ),
                )
                .http2_keepalive_interval(Some(Duration::from_millis(20)))
                .http2_keepalive_timeout(Some(Duration::from_millis(20)))
                .add_service(FlowControlledService {
                    subscriptions: Arc::new(Semaphore::new(1)),
                    lifetime: Duration::from_millis(150),
                })
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = receiver.await;
                }),
            );
            let path = if subscription {
                "Subscribe"
            } else {
                "WakeInstance"
            };
            let request = || {
                tonic::codegen::http::Request::builder()
                    .method("POST")
                    .uri(format!(
                        "http://{address}/sleepypods.controlplane.v1.ProxyControlPlane/{path}"
                    ))
                    .header("content-type", "application/grpc-web-text+proto")
                    .header("accept", "application/grpc-web-text+proto")
                    .body(http_body_util::Empty::<bytes::Bytes>::new())
                    .unwrap()
            };
            let socket = TcpStream::connect(address).await.unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .initial_stream_window_size(1)
                    .handshake(hyper_util::rt::TokioIo::new(socket))
                    .await
                    .unwrap();
            let peer = tokio::spawn(connection); // Hyper answers PING while withholding stream credit.
            let first = sender.send_request(request()).await.unwrap();
            let rejected = sender.send_request(request()).await.unwrap();
            assert_eq!(
                rejected.headers()["content-type"],
                "application/grpc-web-text+proto"
            );
            assert_eq!(
                rejected.headers()["grpc-status"],
                "8",
                "the final queued DATA still owns capacity"
            );
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert!(
                !peer.is_finished(),
                "responsive peer remains alive before delivery expiry"
            );
            tokio::time::timeout(Duration::from_secs(2), peer)
                .await
                .unwrap()
                .unwrap()
                .unwrap_or_default();
            drop(first);
            drop(sender);
            let socket = TcpStream::connect(address).await.unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .handshake(hyper_util::rt::TokioIo::new(socket))
                    .await
                    .unwrap();
            let recovered_peer = tokio::spawn(connection);
            let recovered = sender.send_request(request()).await.unwrap();
            assert!(!recovered.headers().contains_key("grpc-status"));
            assert_eq!(
                recovered.headers()["content-type"],
                "application/grpc-web-text+proto"
            );
            assert_eq!(
                recovered
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .len(),
                5464
            );
            drop(sender);
            recovered_peer.abort();
            shutdown.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn cancellation_is_latched_across_repeated_io_polls() {
        let mut incoming = BoundedIncoming::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(Semaphore::new(1)),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let _peer = TcpStream::connect(incoming.local_addr().unwrap())
            .await
            .unwrap();
        let mut accepted = incoming.next().await.unwrap().unwrap();
        accepted.progress.cancel();
        for _ in 0..3 {
            assert_eq!(
                accepted.read(&mut [0]).await.unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted
            );
            assert_eq!(
                accepted.write(&[0]).await.unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted
            );
            assert_eq!(
                accepted.flush().await.unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted
            );
            assert_eq!(
                accepted.shutdown().await.unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted
            );
        }
    }

    #[tokio::test]
    async fn unconsumed_socket_write_is_bounded_and_releases_connection_capacity() {
        let capacity = Arc::new(Semaphore::new(1));
        let mut incoming = BoundedIncoming::bind(
            "127.0.0.1:0".parse().unwrap(),
            capacity.clone(),
            Duration::from_secs(1),
            Duration::from_millis(30),
        )
        .await
        .unwrap();
        let _peer = TcpStream::connect(incoming.local_addr().unwrap())
            .await
            .unwrap();
        let mut accepted = incoming.next().await.unwrap().unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            std::future::poll_fn(|cx| {
                let bytes = [0; 65536];
                for _ in 0..128 {
                    match Pin::new(&mut accepted).poll_write(cx, &bytes) {
                        Poll::Ready(Ok(_)) => continue,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err::<(), _>(error)),
                        Poll::Pending => {
                            assert!(
                                Pin::new(&mut accepted).poll_flush(cx).is_ready(),
                                "TCP flush succeeds while writes stall"
                            );
                            return Poll::Pending;
                        }
                    }
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            }),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        drop(accepted);
        assert_eq!(capacity.available_permits(), 1);
    }
}

#[cfg(test)]
#[path = "runtime_io/tls_setup_tests.rs"]
mod tls_setup_tests;
