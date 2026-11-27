use std::{
    convert::Infallible,
    error::Error,
    fmt,
    future::{pending, Future},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Request, Response, StatusCode, Uri};
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::Incoming,
    server::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use proxy_core::{DrainError, Shutdown};
use sleepypods_types::{Generation, InstanceId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{protocol::CloseFrame, Message},
};

use crate::{
    runtime::{
        serve_http_listener_with_idle, serve_tcp_listener_with_idle, SidecarRuntimeConfig,
        SidecarRuntimeError,
    },
    IdleReportConfig, ReportIdleClient, ReportIdleFuture, ReportIdleRequest, ReportIdleResponse,
};

const IDLE_TIMEOUT: Duration = Duration::from_millis(50);
const RETRY_BACKOFF: Duration = Duration::from_millis(10);
const DRAIN_GRACE_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_TIMEOUT: Duration = Duration::from_secs(2);
const TCP_REQUEST: &[u8] = b"runtime tcp request bytes";
const TCP_RESPONSE: &[u8] = b"runtime tcp response bytes";
const GRPC_MESSAGE: &[u8] = b"\0\0\0\0\0";

#[tokio::test]
async fn initial_http_setup_holds_idle_across_slow_first_handoff_and_fast_second_waiter() {
    let (upstream_addr, _, upstream_task) = spawn_keep_alive_upstream().await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let reports = client.requests();
    let (addr, runtime) = spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;
    let mut slow = TcpStream::connect(addr).await.unwrap();
    slow.write_all(b"GET /keepalive-two HTTP/1.1\r\nHost: test\r\n")
        .await
        .unwrap();
    let mut fast = TcpStream::connect(addr).await.unwrap();
    write_http_request(&mut fast, "/keepalive-one").await;
    assert!(read_http_response(&mut fast, b"keepalive-one")
        .await
        .starts_with("HTTP/1.1 200"));
    tokio::time::sleep(IDLE_TIMEOUT * 3).await;
    assert_recorded_count(&reports, 0);
    slow.write_all(b"\r\n").await.unwrap();
    assert!(read_http_response(&mut slow, b"keepalive-two")
        .await
        .starts_with("HTTP/1.1 200"));
    wait_for_recorded_count(&reports, 1).await;
    // Both public sockets remain open. The temporary setup work has transferred
    // and released, so idle keepalive does not suppress the ordinary idle report.
    drop(slow);
    drop(fast);
    shutdown.shutdown();
    runtime.await.unwrap().unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn initial_http_setup_timeout_releases_idle_work_without_any_request() {
    let app = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let resources = proxy_core::ProxyResourceConfig::default()
        .with_timeouts(
            Duration::from_millis(150),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
    let config = SidecarRuntimeConfig::new(
        addr,
        app.local_addr().unwrap().port(),
        InstanceId::new("setup").unwrap(),
        Generation::new(1),
        IdleReportConfig::new(IDLE_TIMEOUT, RETRY_BACKOFF, Duration::from_secs(10)).unwrap(),
        DRAIN_GRACE_TIMEOUT,
    )
    .unwrap()
    .with_resource_config(resources);
    let admission = config.admission().clone();
    let client = FakeReportIdleClient::new();
    let reports = client.requests();
    let shutdown = Shutdown::new();
    let runtime = tokio::spawn(serve_http_listener_with_idle(
        listener,
        config,
        client,
        shutdown.clone(),
    ));
    let mut stream = TcpStream::connect(addr).await.unwrap();
    while admission.handshakes.in_flight() != 1 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(IDLE_TIMEOUT * 2).await;
    assert_recorded_count(&reports, 0);
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, stream.read(&mut [0; 1]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    wait_for_recorded_count(&reports, 1).await;
    assert_eq!(admission.handshakes.in_flight(), 0);
    shutdown.shutdown();
    runtime.await.unwrap().unwrap();
}

type RecordedRequests = Arc<Mutex<Vec<ReportIdleRequest>>>;

#[derive(Debug, Clone)]
struct FakeReportIdleClient {
    requests: RecordedRequests,
}

#[derive(Debug, Clone, Copy)]
struct FakeReportIdleError;

#[tokio::test]
async fn http_request_reaches_upstream_and_activity_delays_idle_report() {
    let (release_upstream, upstream_released) = oneshot::channel();
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) = spawn_blocked_upstream(
        upstream_released,
        upstream_received,
        "/workload?from=runtime",
        Bytes::from_static(b"runtime-ok"),
    )
    .await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let (runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to runtime");
    write_http_request(&mut stream, "/workload?from=runtime").await;
    upstream_received_rx
        .await
        .expect("upstream receives request");

    tokio::time::sleep(IDLE_TIMEOUT + RETRY_BACKOFF).await;
    assert_recorded_count(&requests, 0);

    release_upstream.send(()).expect("upstream release sent");
    let response = read_http_response(&mut stream, b"runtime-ok").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.ends_with("runtime-ok"), "{response}");

    tokio::time::sleep(IDLE_TIMEOUT).await;
    wait_for_recorded_count(&requests, 1).await;

    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn http_keep_alive_reuses_connection_without_holding_idle_permit() {
    let (upstream_addr, upstream_connections, upstream_task) = spawn_keep_alive_upstream().await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let (runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;
    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to runtime");

    write_http_request(&mut stream, "/keepalive-one").await;
    let first = read_http_response(&mut stream, b"keepalive-one").await;
    assert!(first.starts_with("HTTP/1.1 200 OK"), "{first}");
    wait_for_recorded_count(&requests, 1).await;

    write_http_request(&mut stream, "/keepalive-two").await;
    let second = read_http_response(&mut stream, b"keepalive-two").await;
    assert!(second.starts_with("HTTP/1.1 200 OK"), "{second}");
    assert_eq!(upstream_connections.load(Ordering::Acquire), 1);

    drop(stream);
    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn a_claim_for_another_instance_closes_before_the_app_and_a_claim_for_this_one_is_served() {
    let (upstream_addr, upstream_connections, upstream_task) = spawn_keep_alive_upstream().await;
    let shutdown = Shutdown::new();
    let (runtime_addr, runtime_task) = spawn_runtime(
        upstream_addr.port(),
        FakeReportIdleClient::new(),
        shutdown.clone(),
    )
    .await;

    // A refused claim ends in a clean end of stream, even with a request
    // still unread behind it.
    let request = format!(
        "GET /keepalive-one HTTP/1.1\r\nx-padding: {}\r\n\r\n",
        "a".repeat(8192)
    );
    for opening in [
        b"\0sleepypods-instance other-instance\n".to_vec(),
        [
            &b"\0sleepypods-instance other-instance\n"[..],
            request.as_bytes(),
        ]
        .concat(),
        [
            &b"\0sleepypods-instance test-instance\n"[..],
            request.as_bytes(),
        ]
        .concat(),
    ] {
        let mut stale = TcpStream::connect(runtime_addr).await.unwrap();
        stale.write_all(&opening).await.unwrap();
        let mut reply = Vec::new();
        stale.read_to_end(&mut reply).await.unwrap();
        assert!(reply.is_empty(), "{reply:?}");
    }
    assert_eq!(upstream_connections.load(Ordering::Acquire), 0);

    let claim = b"\0sleepypods-instance test-instance\n";
    let mut own = TcpStream::connect(runtime_addr).await.unwrap();
    own.write_all(claim).await.unwrap();
    let mut echo = [0; 35];
    own.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, claim);
    for path in ["/keepalive-one", "/keepalive-two"] {
        write_http_request(&mut own, path).await;
        let response = read_http_response(&mut own, path[1..].as_bytes()).await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    }
    assert_eq!(upstream_connections.load(Ordering::Acquire), 1);

    drop(own);
    shutdown.shutdown();
    runtime_task.await.unwrap().unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn idle_report_fires_without_http_activity() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("unused upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream has addr");
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let (_runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    wait_for_recorded_count(&requests, 1).await;

    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
}

#[tokio::test]
async fn h2c_grpc_shaped_stream_delays_idle_report_until_stream_closes() {
    let (release_upstream, upstream_released) = oneshot::channel();
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) =
        spawn_blocked_h2_grpc_upstream(upstream_released, upstream_received).await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let runtime_addr = listener.local_addr().unwrap();
    let runtime_task = spawn_runtime_for_app(
        upstream_addr.port(),
        proxy_core::AppProtocol::H2c,
        listener,
        runtime_addr,
        client,
        shutdown.clone(),
    )
    .await;

    let stream = TcpStream::connect(runtime_addr).await.unwrap();
    let (mut h2_client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    let driver = tokio::spawn(connection);
    // A complete HTTP/2 preface with no first stream still owns initial public
    // setup. The first request may arrive after the ordinary idle interval.
    tokio::time::sleep(IDLE_TIMEOUT * 3).await;
    assert_recorded_count(&requests, 0);
    let uri: Uri = format!("http://{runtime_addr}/grpc.health.v1.Health/Watch")
        .parse()
        .expect("h2 URI parses");
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .version(http::Version::HTTP_2)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(Full::new(Bytes::from_static(GRPC_MESSAGE)))
        .expect("h2 grpc-shaped request builds");

    let response_task = tokio::spawn(async move { h2_client.send_request(request).await });
    expect_within(upstream_received_rx, "h2 upstream request receipt")
        .await
        .expect("upstream receives h2 request");

    let mut response = expect_within(response_task, "h2 response headers")
        .await
        .expect("h2 response task joins")
        .expect("h2 request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("grpc content type is preserved"),
        "application/grpc"
    );

    let frame = expect_within(
        response.body_mut().frame(),
        "initial h2 grpc response frame",
    )
    .await
    .expect("h2 response has initial frame")
    .expect("initial h2 response frame is valid");
    assert_eq!(
        frame.data_ref().expect("initial h2 frame has data"),
        &Bytes::from_static(GRPC_MESSAGE)
    );

    tokio::time::sleep(IDLE_TIMEOUT + RETRY_BACKOFF).await;
    assert_recorded_count(&requests, 0);

    release_upstream.send(()).expect("upstream release sent");
    expect_within(response.into_body().collect(), "h2 response body release")
        .await
        .expect("h2 response body is readable")
        .to_bytes();
    wait_for_recorded_count(&requests, 1).await;

    driver.abort();
    let _ = driver.await;
    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn websocket_session_delays_idle_report_until_session_closes() {
    let (release_upstream, upstream_released) = oneshot::channel();
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) =
        spawn_blocked_websocket_upstream(upstream_released, upstream_received).await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let (runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    let (mut websocket, _) = connect_async(format!("ws://{runtime_addr}/socket"))
        .await
        .expect("client connects to runtime websocket");
    websocket
        .send(Message::Text("active websocket".into()))
        .await
        .expect("client sends websocket message");
    expect_within(upstream_received_rx, "websocket upstream message receipt")
        .await
        .expect("upstream receives websocket message");

    tokio::time::sleep(IDLE_TIMEOUT + RETRY_BACKOFF).await;
    assert_recorded_count(&requests, 0);

    release_upstream.send(()).expect("upstream release sent");
    let close = expect_within(websocket.next(), "websocket close frame")
        .await
        .expect("client receives close")
        .expect("close frame is valid");
    assert!(matches!(close, Message::Close(Some(frame)) if frame.reason == "idle-test-done"));
    drop(websocket);
    wait_for_recorded_count(&requests, 1).await;

    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn shutdown_stops_accepting_and_waits_for_active_request_to_finish() {
    let (release_upstream, upstream_released) = oneshot::channel();
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) = spawn_blocked_upstream(
        upstream_released,
        upstream_received,
        "/during-shutdown",
        Bytes::from_static(b"drained"),
    )
    .await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let (runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to runtime");
    write_http_request(&mut stream, "/during-shutdown").await;
    upstream_received_rx
        .await
        .expect("upstream receives request");

    shutdown.shutdown();
    tokio::task::yield_now().await;
    assert!(!runtime_task.is_finished());

    release_upstream.send(()).expect("upstream release sent");
    let response = read_http_response(&mut stream, b"drained").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.ends_with("drained"), "{response}");

    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn shutdown_returns_after_drain_timeout_when_request_hangs() {
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) =
        spawn_hanging_upstream(upstream_received, "/hangs-past-drain").await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let (runtime_addr, runtime_task) = spawn_runtime_with_drain_timeout(
        upstream_addr.port(),
        client,
        shutdown.clone(),
        Duration::from_millis(50),
    )
    .await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to runtime");
    write_http_request(&mut stream, "/hangs-past-drain").await;
    upstream_received_rx
        .await
        .expect("upstream receives request");

    shutdown.shutdown();

    let runtime_result = tokio::time::timeout(Duration::from_secs(1), runtime_task)
        .await
        .expect("runtime returns after drain timeout")
        .expect("runtime task joins");

    assert!(matches!(
        runtime_result,
        Err(SidecarRuntimeError::Drain(DrainError::GraceTimeout {
            active: 2,
            ..
        }))
    ));

    upstream_task.abort();
    let _ = upstream_task.await;
}

#[tokio::test]
async fn tcp_bytes_are_forwarded_between_client_and_upstream() {
    let (upstream_addr, upstream_task) = spawn_tcp_response_upstream().await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let (runtime_addr, runtime_task) =
        spawn_tcp_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to tcp runtime");
    stream
        .write_all(TCP_REQUEST)
        .await
        .expect("client writes tcp request bytes");

    let mut response = vec![0; TCP_RESPONSE.len()];
    stream
        .read_exact(&mut response)
        .await
        .expect("client reads tcp response bytes");
    assert_eq!(response, TCP_RESPONSE);

    stream.shutdown().await.expect("client half-closes");

    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn open_tcp_stream_delays_idle_report_until_it_closes() {
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (release_upstream, upstream_released) = oneshot::channel();
    let (upstream_addr, upstream_task) =
        spawn_blocked_tcp_upstream(upstream_received, upstream_released).await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let (runtime_addr, runtime_task) =
        spawn_tcp_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to tcp runtime");
    stream
        .write_all(b"x")
        .await
        .expect("client writes tcp byte");
    upstream_received_rx
        .await
        .expect("upstream receives proxied byte");

    tokio::time::sleep(IDLE_TIMEOUT + RETRY_BACKOFF).await;
    assert_recorded_count(&requests, 0);

    release_upstream.send(()).expect("upstream release sent");
    stream.shutdown().await.expect("client half-closes");
    wait_for_recorded_count(&requests, 1).await;

    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn shutdown_returns_after_drain_timeout_when_tcp_stream_hangs() {
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) = spawn_hanging_tcp_upstream(upstream_received).await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let (runtime_addr, runtime_task) = spawn_tcp_runtime_with_drain_timeout(
        upstream_addr.port(),
        client,
        shutdown.clone(),
        Duration::from_millis(50),
    )
    .await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to tcp runtime");
    stream
        .write_all(b"x")
        .await
        .expect("client writes tcp byte");
    upstream_received_rx
        .await
        .expect("upstream receives proxied byte");

    shutdown.shutdown();

    let runtime_result = tokio::time::timeout(Duration::from_secs(1), runtime_task)
        .await
        .expect("runtime returns after drain timeout")
        .expect("runtime task joins");

    assert!(matches!(
        runtime_result,
        Err(SidecarRuntimeError::Drain(DrainError::GraceTimeout {
            active: 1,
            ..
        }))
    ));

    upstream_task.abort();
    let _ = upstream_task.await;
}

impl FakeReportIdleClient {
    fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> RecordedRequests {
        Arc::clone(&self.requests)
    }
}

impl ReportIdleClient for FakeReportIdleClient {
    type Error = FakeReportIdleError;

    fn report_idle(
        &mut self,
        request: ReportIdleRequest,
    ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error> {
        let requests = self.requests();
        Box::pin(async move {
            requests
                .lock()
                .expect("recorded requests lock is not poisoned")
                .push(request.clone());

            Ok(ReportIdleResponse::Accepted {
                instance_id: request.instance_id().clone(),
                generation: request.generation(),
            })
        })
    }
}

impl fmt::Display for FakeReportIdleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fake report-idle error")
    }
}

impl Error for FakeReportIdleError {}

struct HeldGrpcBody {
    sent_message: bool,
    release: oneshot::Receiver<()>,
}

impl HeldGrpcBody {
    fn new(release: oneshot::Receiver<()>) -> Self {
        Self {
            sent_message: false,
            release,
        }
    }
}

impl Body for HeldGrpcBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if !self.sent_message {
            self.sent_message = true;
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(GRPC_MESSAGE)))));
        }

        match Pin::new(&mut self.release).poll(cx) {
            Poll::Ready(_) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn spawn_runtime(
    app_port: u16,
    client: FakeReportIdleClient,
    shutdown: Shutdown,
) -> (
    SocketAddr,
    JoinHandle<Result<(), crate::runtime::SidecarRuntimeError>>,
) {
    spawn_runtime_with_drain_timeout(app_port, client, shutdown, DRAIN_GRACE_TIMEOUT).await
}

async fn spawn_runtime_with_drain_timeout(
    app_port: u16,
    client: FakeReportIdleClient,
    shutdown: Shutdown,
    drain_grace_timeout: Duration,
) -> (
    SocketAddr,
    JoinHandle<Result<(), crate::runtime::SidecarRuntimeError>>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("runtime listener binds");
    let runtime_addr = listener.local_addr().expect("runtime listener has addr");
    let config = runtime_config(runtime_addr, app_port, drain_grace_timeout);

    let task = tokio::spawn(serve_http_listener_with_idle(
        listener, config, client, shutdown,
    ));
    tokio::task::yield_now().await;

    (runtime_addr, task)
}

fn runtime_config(
    runtime_addr: SocketAddr,
    app_port: u16,
    drain_grace_timeout: Duration,
) -> SidecarRuntimeConfig {
    SidecarRuntimeConfig::new(
        runtime_addr,
        app_port,
        InstanceId::new("test-instance").expect("instance id is valid"),
        Generation::new(7),
        IdleReportConfig::new(IDLE_TIMEOUT, RETRY_BACKOFF, Duration::from_secs(10))
            .expect("idle config is valid"),
        drain_grace_timeout,
    )
    .expect("runtime config is valid")
}

/// A sidecar in front of an app port that serves `app_protocol`.
async fn spawn_runtime_for_app(
    app_port: u16,
    app_protocol: proxy_core::AppProtocol,
    listener: impl proxy_core::Accept + Send + 'static,
    runtime_addr: SocketAddr,
    client: FakeReportIdleClient,
    shutdown: Shutdown,
) -> JoinHandle<Result<(), crate::runtime::SidecarRuntimeError>> {
    let config =
        runtime_config(runtime_addr, app_port, DRAIN_GRACE_TIMEOUT).with_app_protocol(app_protocol);
    tokio::spawn(serve_http_listener_with_idle(
        listener, config, client, shutdown,
    ))
}

async fn spawn_tcp_runtime(
    app_port: u16,
    client: FakeReportIdleClient,
    shutdown: Shutdown,
) -> (
    SocketAddr,
    JoinHandle<Result<(), crate::runtime::SidecarRuntimeError>>,
) {
    spawn_tcp_runtime_with_drain_timeout(app_port, client, shutdown, DRAIN_GRACE_TIMEOUT).await
}

async fn spawn_tcp_runtime_with_drain_timeout(
    app_port: u16,
    client: FakeReportIdleClient,
    shutdown: Shutdown,
    drain_grace_timeout: Duration,
) -> (
    SocketAddr,
    JoinHandle<Result<(), crate::runtime::SidecarRuntimeError>>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("runtime listener binds");
    let runtime_addr = listener.local_addr().expect("runtime listener has addr");
    let config = SidecarRuntimeConfig::new(
        runtime_addr,
        app_port,
        InstanceId::new("test-instance").expect("instance id is valid"),
        Generation::new(7),
        IdleReportConfig::new(IDLE_TIMEOUT, RETRY_BACKOFF, Duration::from_secs(10))
            .expect("idle config is valid"),
        drain_grace_timeout,
    )
    .expect("runtime config is valid");

    let task = tokio::spawn(serve_tcp_listener_with_idle(
        listener, config, client, shutdown,
    ));
    tokio::task::yield_now().await;

    (runtime_addr, task)
}

async fn spawn_blocked_upstream(
    release: oneshot::Receiver<()>,
    received: oneshot::Sender<()>,
    expected_path: &'static str,
    body: Bytes,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");
    let release = Arc::new(Mutex::new(Some(release)));

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts runtime");
        let release = Arc::clone(&release);
        let received = Arc::new(Mutex::new(Some(received)));

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request: Request<Incoming>| {
                    let release = Arc::clone(&release);
                    let received = Arc::clone(&received);
                    let body = body.clone();
                    async move {
                        assert_eq!(
                            request
                                .uri()
                                .path_and_query()
                                .expect("path and query present")
                                .as_str(),
                            expected_path
                        );
                        if let Some(received) = received
                            .lock()
                            .expect("received lock is not poisoned")
                            .take()
                        {
                            received.send(()).expect("test waits for upstream request");
                        }
                        let release = release
                            .lock()
                            .expect("release lock is not poisoned")
                            .take()
                            .expect("upstream handles one request");
                        release.await.expect("upstream release received");

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(body))
                                .expect("upstream response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves request");
    });

    (addr, task)
}

async fn spawn_hanging_upstream(
    received: oneshot::Sender<()>,
    expected_path: &'static str,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");
    let received = Arc::new(Mutex::new(Some(received)));

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts runtime");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request: Request<Incoming>| {
                    let received = Arc::clone(&received);
                    async move {
                        assert_eq!(
                            request
                                .uri()
                                .path_and_query()
                                .expect("path and query present")
                                .as_str(),
                            expected_path
                        );
                        if let Some(received) = received
                            .lock()
                            .expect("received lock is not poisoned")
                            .take()
                        {
                            received.send(()).expect("test waits for upstream request");
                        }
                        pending::<Result<Response<Full<Bytes>>, Infallible>>().await
                    }
                }),
            )
            .await
            .expect("upstream serves request");
    });

    (addr, task)
}

async fn spawn_keep_alive_upstream() -> (SocketAddr, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");
    let connections = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let task_connections = Arc::clone(&connections);
    let task_requests = Arc::clone(&requests);

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts runtime");
        task_connections.fetch_add(1, Ordering::AcqRel);

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request: Request<Incoming>| {
                    let requests = Arc::clone(&task_requests);
                    async move {
                        requests.fetch_add(1, Ordering::AcqRel);
                        let body = match request.uri().path() {
                            "/keepalive-one" => Bytes::from_static(b"keepalive-one"),
                            "/keepalive-two" => Bytes::from_static(b"keepalive-two"),
                            path => panic!("unexpected path {path}"),
                        };

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(body))
                                .expect("upstream response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves keep-alive connection");
        assert_eq!(requests.load(Ordering::Acquire), 2);
    });

    (addr, connections, task)
}

async fn spawn_blocked_h2_grpc_upstream(
    release: oneshot::Receiver<()>,
    received: oneshot::Sender<()>,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("h2 upstream listener binds");
    let addr = listener
        .local_addr()
        .expect("h2 upstream listener has addr");
    let received = Arc::new(Mutex::new(Some(received)));
    let release = Arc::new(Mutex::new(Some(release)));

    let task = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("h2 upstream accepts runtime");

        http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let received = Arc::clone(&received);
                    let release = Arc::clone(&release);

                    async move {
                        assert_eq!(request.version(), http::Version::HTTP_2);
                        assert_eq!(request.method(), "POST");
                        assert_eq!(request.uri().path(), "/grpc.health.v1.Health/Watch");
                        assert_eq!(
                            request
                                .headers()
                                .get("content-type")
                                .expect("grpc content type is preserved"),
                            "application/grpc"
                        );
                        let body = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("upstream reads grpc-shaped body")
                            .to_bytes();
                        assert_eq!(body, Bytes::from_static(GRPC_MESSAGE));

                        if let Some(received) = received
                            .lock()
                            .expect("received lock is not poisoned")
                            .take()
                        {
                            received.send(()).expect("test waits for h2 request");
                        }

                        let release = release
                            .lock()
                            .expect("release lock is not poisoned")
                            .take()
                            .expect("upstream handles one h2 request");

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/grpc")
                                .body(HeldGrpcBody::new(release))
                                .expect("h2 grpc-shaped response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("h2 upstream serves request");
    });

    (addr, task)
}

async fn spawn_blocked_websocket_upstream(
    release: oneshot::Receiver<()>,
    received: oneshot::Sender<()>,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("websocket upstream listener binds");
    let addr = listener
        .local_addr()
        .expect("websocket upstream listener has addr");

    let task = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("websocket upstream accepts runtime");
        let mut websocket = accept_async(stream)
            .await
            .expect("websocket upstream accepts handshake");

        let message = expect_within(websocket.next(), "websocket upstream message")
            .await
            .expect("websocket upstream receives message")
            .expect("websocket message is valid");
        assert_eq!(message, Message::Text("active websocket".into()));
        received.send(()).expect("test waits for websocket message");

        release.await.expect("websocket upstream release received");
        websocket
            .send(Message::Close(Some(CloseFrame {
                code: 1000.into(),
                reason: "idle-test-done".into(),
            })))
            .await
            .expect("websocket upstream sends close");
        websocket
            .flush()
            .await
            .expect("websocket upstream flushes close");
    });

    (addr, task)
}

async fn spawn_tcp_response_upstream() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");

    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("upstream accepts runtime");
        let mut received = vec![0; TCP_REQUEST.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("upstream reads tcp request bytes");
        assert_eq!(received, TCP_REQUEST);

        stream
            .write_all(TCP_RESPONSE)
            .await
            .expect("upstream writes tcp response bytes");

        let mut eof = [0; 1];
        assert_eq!(stream.read(&mut eof).await.expect("upstream reads eof"), 0);
    });

    (addr, task)
}

async fn spawn_blocked_tcp_upstream(
    received: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");

    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("upstream accepts runtime");
        let mut byte = [0; 1];
        stream
            .read_exact(&mut byte)
            .await
            .expect("upstream reads proxied byte");
        assert_eq!(byte, [b'x']);
        received.send(()).expect("test waits for upstream byte");
        release.await.expect("upstream release received");
        stream.shutdown().await.expect("upstream half-closes");
    });

    (addr, task)
}

async fn spawn_hanging_tcp_upstream(received: oneshot::Sender<()>) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");

    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("upstream accepts runtime");
        let mut byte = [0; 1];
        stream
            .read_exact(&mut byte)
            .await
            .expect("upstream reads proxied byte");
        assert_eq!(byte, [b'x']);
        received.send(()).expect("test waits for upstream byte");
        pending::<()>().await;
    });

    (addr, task)
}

async fn write_http_request(stream: &mut TcpStream, path: &str) {
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
        .await
        .expect("client writes request");
}

async fn read_http_response(stream: &mut TcpStream, expected_body: &[u8]) -> String {
    let mut response = Vec::new();
    let mut chunk = [0; 1024];

    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .expect("client reads response");
        assert_ne!(read, 0, "connection closed before response body arrived");
        response.extend_from_slice(&chunk[..read]);
        if response.ends_with(expected_body) {
            break;
        }
    }

    String::from_utf8(response).expect("response is utf8")
}

/// Waits for at least `expected` reports. An idle sidecar keeps reporting.
async fn wait_for_recorded_count(requests: &RecordedRequests, expected: usize) {
    tokio::time::timeout(
        IDLE_TIMEOUT + RETRY_BACKOFF + Duration::from_secs(1),
        async {
            while recorded_count(requests) < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        },
    )
    .await
    .expect("recorded request count reached before timeout");
}

async fn expect_within<F>(future: F, label: &'static str) -> F::Output
where
    F: Future,
{
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{label} timed out"))
}

fn assert_recorded_count(requests: &RecordedRequests, expected: usize) {
    assert_eq!(recorded_count(requests), expected);
}

fn recorded_count(requests: &RecordedRequests) -> usize {
    requests
        .lock()
        .expect("recorded requests lock is not poisoned")
        .len()
}

mod admission;
mod protocol;
