use std::{
    convert::Infallible,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::{
    AppHttpProxyError, DrainError, DrainTracker, HttpProxyError, ProxyResourceConfig, TcpProxyError,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

use super::{SidecarConfigError, SidecarProxy, SidecarProxyConfig};

const HTTP_REQUEST_BODY: &[u8] = b"sidecar preserves this request body";
const HTTP_RESPONSE_BODY: &[u8] = b"sidecar preserves this response body";
const TCP_REQUEST: &[u8] = b"client bytes for the local workload";
const TCP_RESPONSE: &[u8] = b"workload response bytes";
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[test]
fn config_rejects_zero_and_builds_loopback_targets() {
    assert_eq!(
        SidecarProxyConfig::new(0).expect_err("port 0 is rejected"),
        SidecarConfigError::ZeroAppPort
    );

    let config = SidecarProxyConfig::new(8080).expect("non-zero port is valid");

    assert_eq!(
        config.app_addr(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)
    );
}

#[tokio::test]
async fn drain_rejects_new_http_forwarding_before_dialing_upstream() {
    let sidecar = sidecar_for_app_port(9);
    sidecar.start_drain();

    let error = match sidecar.forward_http(empty_http_request()).await {
        Ok(_) => panic!("draining sidecar should reject HTTP forwarding"),
        Err(error) => error,
    };

    assert!(sidecar.is_draining());
    assert_http_draining(error);
}

#[tokio::test]
async fn drain_rejects_new_tcp_forwarding_before_dialing_upstream() {
    let sidecar = sidecar_for_app_port(9);
    let (_client, server_side) = connected_tcp_pair().await;

    sidecar.start_drain();
    let error = sidecar
        .forward_tcp(server_side)
        .await
        .expect_err("draining sidecar should reject TCP forwarding");

    assert!(sidecar.is_draining());
    assert_tcp_draining(error);
}

#[tokio::test]
async fn http_forwarding_preserves_request_response_and_releases_active_count() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|mut request: Request<Incoming>| async move {
                    assert_eq!(request.method(), "PATCH");
                    assert_eq!(
                        request
                            .uri()
                            .path_and_query()
                            .expect("path and query present")
                            .as_str(),
                        "/v1/workload?preserve=true"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("x-preserve")
                            .expect("normal header is preserved"),
                        "yes"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("forwarded")
                            .expect("forwarded header is preserved"),
                        "for=203.0.113.10;proto=https"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("x-forwarded-for")
                            .expect("x-forwarded-for is preserved"),
                        "203.0.113.11"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("x-forwarded-proto")
                            .expect("x-forwarded-proto is preserved"),
                        "https"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("x-forwarded-host")
                            .expect("x-forwarded-host is preserved"),
                        "edge.example.com"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("x-forwarded-prefix")
                            .expect("x-forwarded-prefix is preserved"),
                        "/edge"
                    );
                    assert!(request.headers().get("connection").is_none());
                    assert!(request.headers().get("x-remove").is_none());

                    let body = request
                        .body_mut()
                        .collect()
                        .await
                        .expect("upstream reads request body")
                        .to_bytes();
                    assert_eq!(body, Bytes::from_static(HTTP_REQUEST_BODY));

                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::ACCEPTED)
                            .header("x-upstream", "ok")
                            .body(Full::new(Bytes::from_static(HTTP_RESPONSE_BODY)))
                            .expect("response builds"),
                    )
                }),
            )
            .await
            .expect("upstream serves request");
    });

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let request = Request::builder()
        .method("PATCH")
        .uri("/v1/workload?preserve=true")
        .header("x-preserve", "yes")
        .header("forwarded", "for=203.0.113.10;proto=https")
        .header("x-forwarded-for", "203.0.113.11")
        .header("x-forwarded-proto", "https")
        .header("x-forwarded-host", "edge.example.com")
        .header("x-forwarded-prefix", "/edge")
        .header("connection", "x-remove")
        .header("x-remove", "drop")
        .body(Full::new(Bytes::from_static(HTTP_REQUEST_BODY)))
        .expect("request builds");

    let response = sidecar
        .forward_http(request)
        .await
        .expect("sidecar forwards HTTP request");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response
            .headers()
            .get("x-upstream")
            .expect("header present"),
        "ok"
    );
    assert_eq!(sidecar.active_count(), 1);

    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body is readable")
        .to_bytes();
    assert_eq!(body, Bytes::from_static(HTTP_RESPONSE_BODY));

    sidecar.wait_for_active_count(0).await;
    assert_eq!(sidecar.active_count(), 0);
    drop(sidecar);
    upstream_task.await.expect("upstream task completed");
}

#[tokio::test]
async fn http_active_count_remains_one_while_response_body_is_held() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|_request: Request<Incoming>| async move {
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                        HTTP_RESPONSE_BODY,
                    ))))
                }),
            )
            .await
            .expect("upstream serves request");
    });

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let request = Request::builder()
        .uri("/")
        .body(Full::new(Bytes::new()))
        .expect("request builds");

    let response = sidecar
        .forward_http(request)
        .await
        .expect("sidecar forwards HTTP request");

    assert_eq!(sidecar.active_count(), 1);

    drop(response);
    sidecar.wait_for_active_count(0).await;
    assert_eq!(sidecar.active_count(), 0);
    drop(sidecar);
    upstream_task.await.expect("upstream task completed");
}

#[tokio::test]
async fn drain_waits_for_active_http_response_body_then_completes() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|_request: Request<Incoming>| async move {
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                        HTTP_RESPONSE_BODY,
                    ))))
                }),
            )
            .await
            .expect("upstream serves request");
    });

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let response = sidecar
        .forward_http(empty_http_request())
        .await
        .expect("sidecar forwards HTTP request");
    assert_eq!(sidecar.active_count(), 1);

    let drain_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move { sidecar.drain().await }
    });

    tokio::task::yield_now().await;
    assert!(sidecar.is_draining());
    assert!(!drain_task.is_finished());

    drop(response);
    sidecar.wait_for_active_count(0).await;

    assert_eq!(drain_task.await.expect("drain task completed"), Ok(()));
    drop(sidecar);
    upstream_task.await.expect("upstream task completed");
}

#[tokio::test]
async fn tcp_forwarding_preserves_bidirectional_bytes_and_releases_active_count() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        let mut received = vec![0; TCP_REQUEST.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("upstream reads proxied bytes");
        assert_eq!(received, TCP_REQUEST);

        stream
            .write_all(TCP_RESPONSE)
            .await
            .expect("upstream writes response bytes");

        let mut eof = [0; 1];
        assert_eq!(stream.read(&mut eof).await.expect("upstream reads eof"), 0);
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener
        .local_addr()
        .expect("proxy listener has address");

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let proxy_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move {
            let (client, _) = proxy_listener
                .accept()
                .await
                .expect("proxy accepts client connection");

            sidecar.forward_tcp(client).await
        }
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");

    client
        .write_all(TCP_REQUEST)
        .await
        .expect("client writes request bytes");

    sidecar.wait_for_active_count(1).await;
    assert_eq!(sidecar.active_count(), 1);

    let mut response = vec![0; TCP_RESPONSE.len()];
    client
        .read_exact(&mut response)
        .await
        .expect("client reads response bytes");
    assert_eq!(response, TCP_RESPONSE);

    client.shutdown().await.expect("client half-closes");

    proxy_task
        .await
        .expect("proxy task completed")
        .expect("tcp forwarding succeeds");

    upstream_task.await.expect("upstream task completed");
    sidecar.wait_for_active_count(0).await;
    assert_eq!(sidecar.active_count(), 0);
}

#[tokio::test]
async fn tcp_active_count_remains_one_while_connection_is_open() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let (upstream_read_tx, upstream_read_rx) = oneshot::channel();
    let (release_upstream_tx, release_upstream_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        let mut byte = [0; 1];
        stream
            .read_exact(&mut byte)
            .await
            .expect("upstream reads proxied byte");
        assert_eq!(byte, [b'x']);
        upstream_read_tx
            .send(())
            .expect("test waits for upstream read");
        release_upstream_rx
            .await
            .expect("test releases upstream connection");
        stream.shutdown().await.expect("upstream half-closes");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener
        .local_addr()
        .expect("proxy listener has address");

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let proxy_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move {
            let (client, _) = proxy_listener
                .accept()
                .await
                .expect("proxy accepts client connection");

            sidecar.forward_tcp(client).await
        }
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(b"x")
        .await
        .expect("client writes one byte");

    upstream_read_rx
        .await
        .expect("upstream received proxied byte");
    sidecar.wait_for_active_count(1).await;
    assert_eq!(sidecar.active_count(), 1);

    release_upstream_tx
        .send(())
        .expect("upstream release receiver is active");
    client.shutdown().await.expect("client half-closes");

    proxy_task
        .await
        .expect("proxy task completed")
        .expect("tcp forwarding succeeds");

    upstream_task.await.expect("upstream task completed");
    sidecar.wait_for_active_count(0).await;
    assert_eq!(sidecar.active_count(), 0);
}

#[tokio::test]
async fn drain_waits_for_active_tcp_connection_then_completes() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let (upstream_read_tx, upstream_read_rx) = oneshot::channel();
    let (release_upstream_tx, release_upstream_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        let mut byte = [0; 1];
        stream
            .read_exact(&mut byte)
            .await
            .expect("upstream reads proxied byte");
        assert_eq!(byte, [b'x']);
        upstream_read_tx
            .send(())
            .expect("test waits for upstream read");
        release_upstream_rx
            .await
            .expect("test releases upstream connection");
        stream.shutdown().await.expect("upstream half-closes");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener
        .local_addr()
        .expect("proxy listener has address");

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let proxy_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move {
            let (client, _) = proxy_listener
                .accept()
                .await
                .expect("proxy accepts client connection");

            sidecar.forward_tcp(client).await
        }
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(b"x")
        .await
        .expect("client writes one byte");

    upstream_read_rx
        .await
        .expect("upstream received proxied byte");
    sidecar.wait_for_active_count(1).await;
    assert_eq!(sidecar.active_count(), 1);

    let drain_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move { sidecar.drain().await }
    });

    tokio::task::yield_now().await;
    assert!(sidecar.is_draining());
    assert!(!drain_task.is_finished());

    release_upstream_tx
        .send(())
        .expect("upstream release receiver is active");
    client.shutdown().await.expect("client half-closes");

    proxy_task
        .await
        .expect("proxy task completed")
        .expect("tcp forwarding succeeds");

    upstream_task.await.expect("upstream task completed");
    assert_eq!(drain_task.await.expect("drain task completed"), Ok(()));
    assert_eq!(sidecar.active_count(), 0);
}

#[tokio::test]
async fn drain_completes_after_active_http_upstream_disconnects() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let (upstream_accepted_tx, upstream_accepted_rx) = oneshot::channel();
    let (release_upstream_tx, release_upstream_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        let mut buffer = [0; 128];
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .expect("upstream reads request bytes");
        assert!(bytes_read > 0);
        upstream_accepted_tx
            .send(())
            .expect("test waits for upstream accept");
        release_upstream_rx
            .await
            .expect("test releases upstream connection before response");
        drop(stream);
    });

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let proxy_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move { sidecar.forward_http(empty_http_request()).await }
    });

    expect_within(upstream_accepted_rx, "upstream HTTP disconnect accept")
        .await
        .expect("upstream accepted sidecar request");
    expect_within(
        sidecar.wait_for_active_count(1),
        "active HTTP upstream-disconnect accounting",
    )
    .await;

    let drain_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move { sidecar.drain().await }
    });

    tokio::task::yield_now().await;
    assert!(!drain_task.is_finished());

    release_upstream_tx
        .send(())
        .expect("upstream release receiver is active");

    expect_within(proxy_task, "HTTP upstream disconnect proxy completion")
        .await
        .expect("proxy task completed")
        .expect_err("http forwarding surfaces upstream disconnect");

    expect_within(upstream_task, "HTTP upstream disconnect task")
        .await
        .expect("upstream task completed");
    assert_eq!(
        expect_within(drain_task, "HTTP upstream disconnect drain")
            .await
            .expect("drain task completed"),
        Ok(())
    );
    assert_eq!(sidecar.active_count(), 0);
}

#[tokio::test]
async fn drain_completes_after_active_tcp_client_disconnects() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let (upstream_read_tx, upstream_read_rx) = oneshot::channel();
    let (upstream_eof_tx, upstream_eof_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts sidecar connection");

        let mut byte = [0; 1];
        stream
            .read_exact(&mut byte)
            .await
            .expect("upstream reads proxied byte");
        assert_eq!(byte, [b'x']);
        upstream_read_tx
            .send(())
            .expect("test waits for upstream read");

        assert_eq!(stream.read(&mut byte).await.expect("upstream reads eof"), 0);
        upstream_eof_tx.send(()).expect("test waits for eof");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener
        .local_addr()
        .expect("proxy listener has address");

    let sidecar = sidecar_for_app_port(upstream_addr.port());
    let proxy_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move {
            let (client, _) = proxy_listener
                .accept()
                .await
                .expect("proxy accepts client connection");

            sidecar.forward_tcp(client).await
        }
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(b"x")
        .await
        .expect("client writes one byte");

    expect_within(upstream_read_rx, "upstream TCP read")
        .await
        .expect("upstream received proxied byte");
    expect_within(
        sidecar.wait_for_active_count(1),
        "active TCP client-disconnect accounting",
    )
    .await;

    let drain_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move { sidecar.drain().await }
    });

    tokio::task::yield_now().await;
    assert!(!drain_task.is_finished());

    drop(client);
    expect_within(upstream_eof_rx, "upstream TCP EOF")
        .await
        .expect("upstream observes client eof");

    expect_within(proxy_task, "TCP client disconnect proxy completion")
        .await
        .expect("proxy task completed")
        .expect("tcp forwarding completes after client disconnect");

    expect_within(upstream_task, "TCP client disconnect upstream task")
        .await
        .expect("upstream task completed");
    assert_eq!(
        expect_within(drain_task, "TCP client disconnect drain")
            .await
            .expect("drain task completed"),
        Ok(())
    );
    assert_eq!(sidecar.active_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn drain_grace_timeout_surfaces_active_work() {
    let grace_timeout = Duration::from_secs(5);
    let sidecar = sidecar_for_app_port_with_grace(8080, grace_timeout);
    let _permit = sidecar
        .drain_tracker()
        .try_acquire()
        .expect("active work admitted");

    let drain_task = tokio::spawn({
        let sidecar = sidecar.clone();
        async move { sidecar.drain().await }
    });

    tokio::task::yield_now().await;
    assert!(sidecar.is_draining());

    tokio::time::advance(grace_timeout).await;

    assert_eq!(
        drain_task
            .await
            .expect("drain task completed")
            .expect_err("active work should exceed grace timeout"),
        DrainError::GraceTimeout {
            timeout: grace_timeout,
            active: 1,
        }
    );
}

/// A proxy and the drain tracker its runtime shares with it.
#[derive(Clone)]
struct Sidecar {
    proxy: Arc<SidecarProxy>,
    drain: DrainTracker,
}

impl Deref for Sidecar {
    type Target = SidecarProxy;

    fn deref(&self) -> &SidecarProxy {
        &self.proxy
    }
}

impl Sidecar {
    fn start_drain(&self) {
        self.drain.start_drain();
    }

    fn is_draining(&self) -> bool {
        self.drain.is_draining()
    }

    fn active_count(&self) -> usize {
        self.drain.active_count()
    }

    async fn wait_for_active_count(&self, expected: usize) {
        self.drain.wait_for_active_count(expected).await;
    }

    fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }
}

fn sidecar_for_app_port(app_port: u16) -> Sidecar {
    sidecar_for_app_port_with_grace(app_port, Duration::from_secs(5))
}

fn sidecar_for_app_port_with_grace(app_port: u16, grace_timeout: Duration) -> Sidecar {
    let resources = ProxyResourceConfig::default()
        .with_timeouts(
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .expect("test timeouts are valid");
    let config = SidecarProxyConfig::new(app_port)
        .expect("sidecar config builds")
        .with_resource_config(resources);
    let drain = DrainTracker::new(grace_timeout);
    Sidecar {
        proxy: Arc::new(SidecarProxy::new(config, drain.clone())),
        drain,
    }
}

fn empty_http_request() -> Request<Full<Bytes>> {
    Request::builder()
        .uri("/")
        .body(Full::new(Bytes::new()))
        .expect("request builds")
}

async fn connected_tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("test listener binds");
    let address = listener.local_addr().expect("listener has local addr");
    let client = tokio::spawn(async move { TcpStream::connect(address).await });
    let (server, _) = listener.accept().await.expect("listener accepts client");
    let client = client
        .await
        .expect("client task completed")
        .expect("client connects");

    (client, server)
}

async fn expect_within<F>(future: F, label: &'static str) -> F::Output
where
    F: Future,
{
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{label} timed out"))
}

fn assert_http_draining(error: AppHttpProxyError) {
    match error {
        AppHttpProxyError::Proxy(HttpProxyError::Drain(DrainError::Draining)) => {}
        other => panic!("expected HTTP drain rejection, got {other:?}"),
    }
}

fn assert_tcp_draining(error: TcpProxyError) {
    match error {
        TcpProxyError::Drain(DrainError::Draining) => {}
        other => panic!("expected TCP drain rejection, got {other:?}"),
    }
}
