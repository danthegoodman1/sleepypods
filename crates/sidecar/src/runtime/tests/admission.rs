use super::*;
use proxy_core::{ProxyAdmission, ProxyResourceConfig};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinSet;

struct Frames(mpsc::Receiver<Result<Frame<Bytes>, Infallible>>);
impl Body for Frames {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0.poll_recv(cx)
    }
}
struct Fixture {
    addr: SocketAddr,
    admission: ProxyAdmission,
    collector: proxy_core::observability::prometheus::RuntimeActiveStreamsCollector,
    shutdown: Shutdown,
    runtime: JoinHandle<Result<(), SidecarRuntimeError>>,
    app: JoinHandle<()>,
    upgrades: Arc<AsyncMutex<JoinSet<()>>>,
    frames: mpsc::Sender<Result<Frame<Bytes>, Infallible>>,
    received: mpsc::Receiver<String>,
}
impl Fixture {
    async fn start(resources: ProxyResourceConfig) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let app_port = listener.local_addr().unwrap().port();
        let (frames, body) = mpsc::channel(4);
        let body = Arc::new(AsyncMutex::new(Some(body)));
        let (sent, received) = mpsc::channel(32);
        let upgrades = Arc::new(AsyncMutex::new(JoinSet::new()));
        let app_upgrades = upgrades.clone();
        let held_uploads = Arc::new(AsyncMutex::new(Vec::<Incoming>::new()));
        let app = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                while connections.try_join_next().is_some() {}
                let held_uploads = held_uploads.clone();
                let body = body.clone();
                let sent = sent.clone();
                let upgrades = app_upgrades.clone();
                connections.spawn(serve_app_connection(
                    stream,
                    move |mut request: Request<Incoming>| {
                        let held_uploads = held_uploads.clone();
                        let body = body.clone();
                        let sent = sent.clone();
                        let upgrades = upgrades.clone();
                        async move {
                            let path = request.uri().path().to_owned();
                            sent.send(path.clone()).await.unwrap();
                            if path == "/early" {
                                held_uploads.lock().await.push(request.into_body());
                                return Ok(Response::new(
                                    Full::new(Bytes::from_static(b"ok")).boxed_unsync(),
                                ));
                            }
                            if path == "/large" {
                                return Ok(Response::new(
                                    Full::new(Bytes::from(vec![1; 1024 * 1024])).boxed_unsync(),
                                ));
                            }
                            if path == "/stall" {
                                return std::future::pending().await;
                            }
                            if path == "/stream" {
                                return Ok::<_, Infallible>(
                                    Response::builder()
                                        .header("content-type", "application/grpc")
                                        .body(
                                            Frames(body.lock().await.take().unwrap())
                                                .boxed_unsync(),
                                        )
                                        .unwrap(),
                                );
                            }
                            if path == "/ws" {
                                let response =
                                    tokio_tungstenite::tungstenite::handshake::server::create_response_with_body(
                                        &request,
                                        || Full::new(Bytes::new()).boxed_unsync(),
                                    )
                                    .unwrap();
                                let upgrade = hyper::upgrade::on(&mut request);
                                upgrades.lock().await.spawn(async move {
                                    let mut ws =
                                        tokio_tungstenite::WebSocketStream::from_raw_socket(
                                            TokioIo::new(upgrade.await.unwrap()),
                                            tokio_tungstenite::tungstenite::protocol::Role::Server,
                                            None,
                                        )
                                        .await;
                                    while let Some(message) = ws.next().await {
                                        let Ok(message) = message else { break };
                                        if message.is_close() {
                                            let _ = ws.flush().await;
                                            break;
                                        }
                                        if ws.send(message).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                                return Ok(response);
                            }
                            if path == "/upload" {
                                let bytes = request.into_body().collect().await.unwrap().to_bytes();
                                return Ok(Response::new(Full::new(bytes).boxed_unsync()));
                            }
                            Ok(Response::new(
                                Full::new(Bytes::from_static(b"ok")).boxed_unsync(),
                            ))
                        }
                    },
                ));
            }
        });
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = SidecarRuntimeConfig::new(
            addr,
            app_port,
            InstanceId::new("admission").unwrap(),
            Generation::new(1),
            IdleReportConfig::new(
                Duration::from_secs(3600),
                Duration::from_secs(1),
                Duration::from_secs(10),
            )
            .unwrap(),
            Duration::from_millis(100),
        )
        .unwrap()
        .with_resource_config(resources);
        let admission = config.admission().clone();
        let collector = config.active_streams_collector();
        let shutdown = Shutdown::new();
        let runtime = tokio::spawn(serve_http_listener_with_idle(
            listener,
            config,
            FakeReportIdleClient::new(),
            shutdown.clone(),
        ));
        Self {
            addr,
            admission,
            collector,
            shutdown,
            runtime,
            app,
            upgrades,
            frames,
            received,
        }
    }
    async fn stop(self) {
        self.shutdown.shutdown();
        let _ = self.runtime.await.unwrap();
        self.app.abort();
        let _ = self.app.await;
        let mut upgrades = self.upgrades.lock().await;
        upgrades.abort_all();
        while upgrades.join_next().await.is_some() {}
        assert_eq!(self.admission.connections.in_flight(), 0);
        assert_eq!(self.admission.requests.in_flight(), 0);
        assert_eq!(self.admission.handshakes.in_flight(), 0);
    }
    async fn request(&self, path: &str) -> Response<Incoming> {
        Client::builder(TokioExecutor::new())
            .build_http::<Full<Bytes>>()
            .request(
                Request::builder()
                    .uri(format!("http://{}{path}", self.addr))
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }
}
fn limits(connections: usize, requests: usize, handshakes: usize) -> ProxyResourceConfig {
    ProxyResourceConfig::default()
        .with_limits(connections, requests, handshakes, 4)
        .unwrap()
        .with_timeouts(
            Duration::from_millis(200),
            Duration::from_millis(150),
            Duration::from_millis(150),
        )
        .unwrap()
}
async fn count(limiter: &proxy_core::AdmissionLimiter, expected: usize) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        while limiter.in_flight() != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
async fn closed(stream: &mut TcpStream) {
    let result = tokio::time::timeout(TEST_TIMEOUT, stream.read(&mut [0; 1]))
        .await
        .unwrap();
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "overload socket remained open: {result:?}"
    );
}

#[tokio::test]
async fn silent_handshake_flood_is_bounded_before_tasks_and_recovers() {
    let fixture = Fixture::start(limits(8, 8, 1)).await;
    let mut held = TcpStream::connect(fixture.addr).await.unwrap();
    count(&fixture.admission.handshakes, 1).await;
    for _ in 0..32 {
        let mut excess = TcpStream::connect(fixture.addr).await.unwrap();
        closed(&mut excess).await;
        assert!(fixture.admission.connections.in_flight() <= 1);
        assert_eq!(fixture.admission.handshakes.in_flight(), 1);
    }
    held.write_all(b"PRI * HTTP/2.0\r\n").await.unwrap();
    closed(&mut held).await;
    count(&fixture.admission.handshakes, 0).await;
    assert_eq!(fixture.request("/ok").await.status(), StatusCode::OK);
    fixture.stop().await;
}

#[tokio::test]
async fn first_request_overload_releases_setup_work_while_keepalive_stays_open() {
    let fixture = Fixture::start(limits(8, 1, 8)).await;
    let reserved = fixture.admission.requests.try_acquire().unwrap();
    let stream = TcpStream::connect(fixture.addr).await.unwrap();
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let response = client
        .send_request(
            Request::builder()
                .uri("/overloaded")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    response.into_body().collect().await.unwrap();
    let sink = proxy_core::observability::prometheus::PrometheusMetricsSink::new();
    fixture.collector.collect(sink.clone());
    assert!(
        sink.render()
            .contains("sleepypods_runtime_active_streams 0\n"),
        "{}",
        sink.render()
    );
    drop(reserved);
    let response = client
        .send_request(
            Request::builder()
                .uri("/ok")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap();
    drop(client);
    driver.abort();
    let _ = driver.await;
    fixture.stop().await;
}

#[tokio::test]
async fn keepalive_socket_limit_does_not_hold_request_capacity() {
    let fixture = Fixture::start(limits(1, 1, 1)).await;
    let stream = TcpStream::connect(fixture.addr).await.unwrap();
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    client
        .send_request(
            Request::builder()
                .uri("/ok")
                .header("host", "test")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap();
    count(&fixture.admission.requests, 0).await;
    assert_eq!(fixture.admission.connections.in_flight(), 1);
    assert_eq!(fixture.admission.handshakes.in_flight(), 0);
    let mut excess = TcpStream::connect(fixture.addr).await.unwrap();
    closed(&mut excess).await;
    drop(client);
    driver.abort();
    let _ = driver.await;
    count(&fixture.admission.connections, 0).await;
    assert_eq!(fixture.request("/ok").await.status(), StatusCode::OK);
    fixture.stop().await;
}

#[tokio::test]
async fn h2_request_capacity_covers_stream_body_and_recovers_on_cancel() {
    let mut fixture = Fixture::start(limits(8, 1, 8)).await;
    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build_http::<Full<Bytes>>();
    let addr = fixture.addr;
    let request = |path: &str| {
        Request::builder()
            .uri(format!("http://{addr}{path}"))
            .version(http::Version::HTTP_2)
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let response = client.request(request("/stream")).await.unwrap();
    assert_eq!(fixture.received.recv().await.unwrap(), "/stream");
    count(&fixture.admission.requests, 1).await;
    for _ in 0..16 {
        assert_eq!(
            client.request(request("/excess")).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert!(
        fixture.received.try_recv().is_err(),
        "overload reached application"
    );
    tokio::time::sleep(Duration::from_millis(450)).await;
    fixture
        .frames
        .send(Ok(Frame::data(Bytes::from_static(GRPC_MESSAGE))))
        .await
        .unwrap();
    let mut body = response.into_body();
    assert_eq!(
        body.frame().await.unwrap().unwrap().into_data().unwrap(),
        GRPC_MESSAGE
    );
    drop(body);
    count(&fixture.admission.requests, 0).await;
    assert_eq!(
        client.request(request("/ok")).await.unwrap().status(),
        StatusCode::OK
    );
    fixture.stop().await;
}

#[tokio::test]
async fn websocket_transfers_socket_and_request_permits_past_http101() {
    let fixture = Fixture::start(limits(8, 1, 8)).await;
    let (mut ws, _) = connect_async(format!("ws://{}/ws", fixture.addr))
        .await
        .unwrap();
    count(&fixture.admission.requests, 1).await;
    count(&fixture.admission.handshakes, 0).await;
    assert_eq!(fixture.admission.connections.in_flight(), 1);
    assert_eq!(
        fixture.request("/ok").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    tokio::time::sleep(Duration::from_millis(450)).await;
    ws.send(Message::Text("still active".into())).await.unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap(),
        Message::Text("still active".into())
    );
    ws.close(None).await.unwrap();
    let _ = ws.next().await;
    drop(ws);
    count(&fixture.admission.requests, 0).await;
    assert_eq!(fixture.request("/ok").await.status(), StatusCode::OK);
    fixture.stop().await;
}

#[tokio::test]
async fn stalled_upstream_headers_timeout_and_release_request_permit() {
    let fixture = Fixture::start(limits(8, 1, 8)).await;
    assert_eq!(
        fixture.request("/stall").await.status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    count(&fixture.admission.requests, 0).await;
    assert_eq!(fixture.request("/ok").await.status(), StatusCode::OK);
    fixture.stop().await;
}

#[tokio::test]
async fn progressing_upload_resets_header_deadline_without_total_lifetime_cap() {
    let fixture = Fixture::start(limits(8, 1, 8)).await;
    let (send, receive) = mpsc::channel(1);
    let client = Client::builder(TokioExecutor::new()).build_http::<Frames>();
    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{}/upload", fixture.addr))
        .body(Frames(receive))
        .unwrap();
    let response = tokio::spawn(async move { client.request(request).await.unwrap() });
    for _ in 0..6 {
        send.send(Ok(Frame::data(Bytes::from_static(b"a"))))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(send);
    let response = response.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "aaaaaa"
    );
    fixture.stop().await;
}

#[tokio::test]
async fn http2_advertises_transport_stream_bound_before_any_application_request() {
    let fixture = Fixture::start(limits(8, 8, 8).with_limits(8, 8, 8, 1).unwrap()).await;
    let mut stream = TcpStream::connect(fixture.addr).await.unwrap();
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")
        .await
        .unwrap();
    let mut header = [0; 9];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[3], 4, "first frame is SETTINGS");
    let len = ((header[0] as usize) << 16) | ((header[1] as usize) << 8) | header[2] as usize;
    let mut settings = vec![0; len];
    stream.read_exact(&mut settings).await.unwrap();
    assert!(
        settings.as_chunks::<6>().0.contains(&[0, 3, 0, 0, 0, 1]),
        "missing max concurrent streams=1: {settings:?}"
    );
    assert_eq!(fixture.admission.requests.in_flight(), 0);
    fixture.stop().await;
}

#[tokio::test]
async fn h2_flow_control_stall_closes_connection_and_releases_queued_body_capacity() {
    let fixture = Fixture::start(limits(8, 2, 8)).await;
    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .http2_initial_stream_window_size(1)
        .build_http::<Full<Bytes>>();
    let request = |path: &str| {
        Request::builder()
            .uri(format!("http://{}{path}", fixture.addr))
            .version(http::Version::HTTP_2)
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let response = client.request(request("/large")).await.unwrap();
    count(&fixture.admission.requests, 1).await;
    // The peer's connection driver continues reading frames and responding to
    // PING; only this stream's receive credit is withheld by not polling its body.
    let other = client.request(request("/ok")).await.unwrap();
    assert_eq!(other.status(), StatusCode::OK);
    assert_eq!(other.into_body().collect().await.unwrap().to_bytes(), "ok");
    count(&fixture.admission.requests, 0).await;
    assert!(
        response.into_body().collect().await.is_err(),
        "stalled body survived its delivery deadline"
    );
    assert_eq!(fixture.request("/ok").await.status(), StatusCode::OK);
    fixture.stop().await;
}

#[tokio::test]
async fn tcp_listener_limits_sessions_preserves_quiet_streams_and_bounds_pending_writes() {
    let upstream = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let app_port = upstream.local_addr().unwrap().port();
    let (accepted, observed) = oneshot::channel();
    let app = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        accepted.send(()).unwrap();
        // A quiet established stream is valid beyond its pending-write budget.
        let byte = stream.read_u8().await.unwrap();
        stream.write_u8(byte).await.unwrap();
        std::future::pending::<()>().await;
    });
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = SidecarRuntimeConfig::new(
        addr,
        app_port,
        InstanceId::new("tcp-admission").unwrap(),
        Generation::new(1),
        IdleReportConfig::new(
            Duration::from_secs(3600),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .unwrap(),
        Duration::from_millis(100),
    )
    .unwrap()
    .with_resource_config(limits(1, 1, 1));
    let admission = config.admission().clone();
    let shutdown = Shutdown::new();
    let runtime = tokio::spawn(serve_tcp_listener_with_idle(
        listener,
        config,
        FakeReportIdleClient::new(),
        shutdown.clone(),
    ));
    let mut client = TcpStream::connect(addr).await.unwrap();
    observed.await.unwrap();
    count(&admission.requests, 1).await;
    let mut excess = TcpStream::connect(addr).await.unwrap();
    closed(&mut excess).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    client.write_u8(42).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap(), 42);
    let writer = tokio::spawn(async move {
        let chunk = [1; 65536];
        for _ in 0..2048 {
            if client.write_all(&chunk).await.is_err() {
                break;
            }
        }
    });
    count(&admission.requests, 0).await;
    count(&admission.connections, 0).await;
    writer.abort();
    let _ = writer.await;
    shutdown.shutdown();
    runtime.await.unwrap().unwrap();
    app.abort();
    let _ = app.await;
}

#[tokio::test]
async fn final_eos_bytes_retain_drain_work_until_delivery_or_cancellation() {
    let fixture = Fixture::start(
        limits(8, 2, 8)
            .with_timeouts(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_millis(500),
            )
            .unwrap(),
    )
    .await;
    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .http2_initial_stream_window_size(1)
        .build_http::<Full<Bytes>>();
    let response = client
        .request(
            Request::builder()
                .uri(format!("http://{}/ok", fixture.addr))
                .version(http::Version::HTTP_2)
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let sink = proxy_core::observability::prometheus::PrometheusMetricsSink::new();
    fixture.collector.collect(sink.clone());
    assert!(
        sink.render()
            .contains("sleepypods_runtime_active_streams 1\n"),
        "a two-byte EOS body with one byte credit must retain work: {}",
        sink.render()
    );
    assert_eq!(fixture.admission.requests.in_flight(), 1);
    drop(response);
    count(&fixture.admission.requests, 0).await;
    fixture.collector.collect(sink.clone());
    assert!(sink
        .render()
        .contains("sleepypods_runtime_active_streams 0\n"));
    fixture.stop().await;
}

#[tokio::test]
async fn early_response_keeps_request_capacity_while_upload_remains_active() {
    let fixture = Fixture::start(
        limits(8, 1, 8)
            .with_timeouts(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap(),
    )
    .await;
    let (send, body) = mpsc::channel(1);
    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build_http::<Frames>();
    let response = client
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{}/early", fixture.addr))
                .version(http::Version::HTTP_2)
                .body(Frames(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "ok"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        fixture.admission.requests.in_flight(),
        1,
        "an early response does not finish its active upload"
    );
    assert_eq!(
        fixture.request("/ok").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(send);
    drop(client);
    count(&fixture.admission.requests, 0).await;
    fixture.stop().await;
}

#[tokio::test]
async fn early_response_upload_eos_flow_control_stall_retains_work_then_recovers() {
    let upstream = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let app_port = upstream.local_addr().unwrap().port();
    let held_uploads = Arc::new(AsyncMutex::new(Vec::<Incoming>::new()));
    let app = tokio::spawn(async move {
        let mut sessions = JoinSet::new();
        loop {
            let (stream, _) = upstream.accept().await.unwrap();
            while sessions.try_join_next().is_some() {}
            let held_uploads = held_uploads.clone();
            sessions.spawn(async move {
                let _ = http2::Builder::new(TokioExecutor::new())
                    .initial_stream_window_size(1)
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request: Request<Incoming>| {
                            let held_uploads = held_uploads.clone();
                            async move {
                                if request.uri().path() == "/early" {
                                    held_uploads.lock().await.push(request.into_body());
                                }
                                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                                    b"ok",
                                ))))
                            }
                        }),
                    )
                    .await;
            });
        }
    });
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = SidecarRuntimeConfig::new(
        addr,
        app_port,
        InstanceId::new("upload-credit").unwrap(),
        Generation::new(1),
        IdleReportConfig::new(
            Duration::from_secs(3600),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .unwrap(),
        Duration::from_millis(100),
    )
    .unwrap()
    .with_app_protocol(proxy_core::AppProtocol::H2c)
    .with_resource_config(
        limits(8, 1, 8)
            .with_timeouts(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_millis(300),
            )
            .unwrap(),
    );
    let admission = config.admission().clone();
    let collector = config.active_streams_collector();
    let shutdown = Shutdown::new();
    let runtime = tokio::spawn(serve_http_listener_with_idle(
        listener,
        config,
        FakeReportIdleClient::new(),
        shutdown.clone(),
    ));
    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build_http::<Full<Bytes>>();
    // Warm the pooled h2 connection so its initial SETTINGS window is applied
    // before the two-byte upload; an optimistic first stream can use 65535 bytes.
    client
        .request(
            Request::builder()
                .uri(format!("http://{addr}/ok"))
                .version(http::Version::HTTP_2)
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap();
    count(&admission.requests, 0).await;
    let response = client
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/early"))
                .version(http::Version::HTTP_2)
                .body(Full::new(Bytes::from_static(b"ab")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "ok"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        admission.requests.in_flight(),
        1,
        "last queued EOS upload byte retains admission"
    );
    let sink = proxy_core::observability::prometheus::PrometheusMetricsSink::new();
    collector.collect(sink.clone());
    assert!(
        sink.render()
            .contains("sleepypods_runtime_active_streams 1\n"),
        "queued upload retains drain activity"
    );
    count(&admission.requests, 0).await;
    collector.collect(sink.clone());
    assert!(sink
        .render()
        .contains("sleepypods_runtime_active_streams 0\n"));
    let response = client
        .request(
            Request::builder()
                .uri(format!("http://{addr}/ok"))
                .version(http::Version::HTTP_2)
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "ok",
        "a fresh upstream connection recovers"
    );
    shutdown.shutdown();
    runtime.await.unwrap().unwrap();
    app.abort();
    let _ = app.await;
}

/// Serves one app connection over HTTP/1.1 or HTTP/2, with upgrades.
async fn serve_app_connection<F, Fut, B>(stream: TcpStream, handler: F)
where
    F: Fn(Request<Incoming>) -> Fut,
    Fut: std::future::Future<Output = Result<Response<B>, Infallible>> + Send + 'static,
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let admission = proxy_core::ProxyAdmission::new(ProxyResourceConfig::default());
    let handshake = admission.handshakes.try_acquire().unwrap();
    let io = admission.admit_io(stream).unwrap();
    proxy_core::serve_http_connection_admitted(
        io,
        Shutdown::new(),
        admission,
        handshake,
        None,
        handler,
    )
    .await;
}
