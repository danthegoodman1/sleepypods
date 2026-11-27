//! The app receives each client protocol in the protocol its port declares,
//! with the client's host, and the listener keeps serving through faults.
use std::collections::VecDeque;

use http::{HeaderMap, Version};
use http_body_util::Empty;
use proxy_core::{Accept, AppProtocol};

use super::*;

/// What the app observed of one request.
#[derive(Clone, Debug)]
struct Seen {
    version: Version,
    authority: Option<String>,
    headers: HeaderMap,
}

type Recorded = Arc<Mutex<Vec<Seen>>>;

async fn record(
    seen: Recorded,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    seen.lock().unwrap().push(Seen {
        version: request.version(),
        authority: request.uri().authority().map(|a| a.to_string()),
        headers: request.headers().clone(),
    });
    Ok(Response::new(Full::new(Bytes::from_static(b"served"))))
}

/// An app that serves only the one HTTP version.
async fn spawn_app(protocol: AppProtocol) -> (u16, Recorded) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Recorded::default();
    let recorded = seen.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            let service = service_fn(move |request| record(seen.clone(), request));
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                match protocol {
                    AppProtocol::Http1 => {
                        let _ = http1::Builder::new().serve_connection(io, service).await;
                    }
                    AppProtocol::H2c => {
                        let _ = http2::Builder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await;
                    }
                }
            });
        }
    });
    (port, recorded)
}

struct Sidecar {
    addr: SocketAddr,
    shutdown: Shutdown,
    task: JoinHandle<Result<(), SidecarRuntimeError>>,
    reports: RecordedRequests,
}

async fn spawn_sidecar(app_port: u16, protocol: AppProtocol) -> Sidecar {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    spawn_sidecar_on(listener, addr, app_port, protocol).await
}

async fn spawn_sidecar_on(
    listener: impl Accept + Send + 'static,
    addr: SocketAddr,
    app_port: u16,
    protocol: AppProtocol,
) -> Sidecar {
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let reports = client.requests();
    let task =
        spawn_runtime_for_app(app_port, protocol, listener, addr, client, shutdown.clone()).await;
    Sidecar {
        addr,
        shutdown,
        task,
        reports,
    }
}

impl Sidecar {
    async fn stop(self) {
        self.shutdown.shutdown();
        expect_within(self.task, "sidecar stops")
            .await
            .unwrap()
            .unwrap();
    }
}

async fn http1_get(addr: SocketAddr, extra: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /hello HTTP/1.1\r\nHost: app.example.test\r\nConnection: close\r\n{extra}\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    expect_within(stream.read_to_string(&mut response), "HTTP/1.1 response")
        .await
        .unwrap();
    response
}

async fn http2_request(addr: SocketAddr, content_type: Option<&str>) -> Response<Incoming> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(connection);
    let mut request = Request::builder()
        .method("POST")
        .uri("http://app.example.test/hello")
        .header("te", "trailers");
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    expect_within(
        sender.send_request(request.body(Empty::<Bytes>::new()).unwrap()),
        "HTTP/2 response",
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn http1_and_http2_clients_reach_an_http1_only_app_with_their_host() {
    let (app_port, seen) = spawn_app(AppProtocol::Http1).await;
    let sidecar = spawn_sidecar(app_port, AppProtocol::Http1).await;

    assert!(http1_get(sidecar.addr, "").await.ends_with("served"));
    let response = http2_request(sidecar.addr, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "served"
    );

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for request in seen {
        assert_eq!(request.version, Version::HTTP_11);
        assert_eq!(request.headers["host"], "app.example.test");
        assert!(!request.headers.contains_key("te"));
    }
    sidecar.stop().await;
}

#[tokio::test]
async fn http1_and_http2_clients_reach_an_h2c_app_with_their_authority() {
    let (app_port, seen) = spawn_app(AppProtocol::H2c).await;
    let sidecar = spawn_sidecar(app_port, AppProtocol::H2c).await;

    assert!(http1_get(sidecar.addr, "TE: trailers\r\n")
        .await
        .ends_with("served"));
    let response = http2_request(sidecar.addr, Some("application/grpc")).await;
    assert_eq!(response.status(), StatusCode::OK);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for request in seen {
        assert_eq!(request.version, Version::HTTP_2);
        assert_eq!(request.authority.as_deref(), Some("app.example.test"));
        assert_eq!(request.headers["te"], "trailers");
    }
    sidecar.stop().await;
}

#[tokio::test]
async fn grpc_to_an_http1_app_port_answers_unimplemented_in_grpc_terms() {
    let (app_port, seen) = spawn_app(AppProtocol::Http1).await;
    let sidecar = spawn_sidecar(app_port, AppProtocol::Http1).await;

    let response = http2_request(sidecar.addr, Some("application/grpc")).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/grpc");
    assert_eq!(response.headers()["grpc-status"], "12");
    assert!(response.headers().contains_key("grpc-message"));
    assert!(seen.lock().unwrap().is_empty(), "the app never sees it");
    sidecar.stop().await;
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn websocket_reaches_the_app_over_http1_on_an_h2c_app_port_with_its_host() {
    use tokio_tungstenite::tungstenite::handshake::server::{
        ErrorResponse, Request as WsRequest, Response as WsResponse,
    };
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let app_port = listener.local_addr().unwrap().port();
    let app = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut host = None;
        let mut websocket = tokio_tungstenite::accept_hdr_async(
            stream,
            |request: &WsRequest, response: WsResponse| -> Result<WsResponse, ErrorResponse> {
                host = request.headers().get("host").cloned();
                Ok(response)
            },
        )
        .await
        .unwrap();
        let message = websocket.next().await.unwrap().unwrap();
        websocket.send(message).await.unwrap();
        let _ = websocket.next().await;
        host
    });
    let sidecar = spawn_sidecar(app_port, AppProtocol::H2c).await;

    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(format!(
            "ws://{}/socket",
            sidecar.addr
        ))
        .unwrap();
    request
        .headers_mut()
        .insert("host", "app.example.test".parse().unwrap());
    let (mut client, _) = connect_async(request).await.unwrap();
    client.send(Message::Text("hello".into())).await.unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::Text("hello".into())
    );
    client.close(None).await.unwrap();
    drop(client);

    let host = expect_within(app, "app finishes").await.unwrap();
    assert_eq!(host.unwrap(), "app.example.test");
    sidecar.stop().await;
}

/// Fails `accept` with the queued errors, then accepts real connections.
struct FailingAccept {
    errors: VecDeque<std::io::Error>,
    inner: TcpListener,
}

impl Accept for FailingAccept {
    async fn accept(&mut self) -> std::io::Result<(TcpStream, SocketAddr)> {
        match self.errors.pop_front() {
            Some(error) => Err(error),
            None => self.inner.accept().await,
        }
    }
}

#[tokio::test]
async fn accept_errors_leave_the_sidecar_serving_and_shutdown_closes_the_listener() {
    const EMFILE: i32 = 24;
    let (app_port, _) = spawn_app(AppProtocol::Http1).await;
    let inner = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = inner.local_addr().unwrap();
    let listener = FailingAccept {
        errors: [
            std::io::Error::from_raw_os_error(EMFILE),
            std::io::ErrorKind::ConnectionAborted.into(),
        ]
        .into(),
        inner,
    };
    let sidecar = spawn_sidecar_on(listener, addr, app_port, AppProtocol::Http1).await;

    for _ in 0..2 {
        assert!(http1_get(addr, "").await.ends_with("served"));
    }

    sidecar.stop().await;
    let refused = TcpStream::connect(addr).await.unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::ConnectionRefused);
}

#[tokio::test]
async fn an_accepted_idle_report_leaves_the_sidecar_serving_and_reporting() {
    let (app_port, _) = spawn_app(AppProtocol::Http1).await;
    let sidecar = spawn_sidecar(app_port, AppProtocol::Http1).await;

    wait_for_recorded_count(&sidecar.reports, 1).await;
    // The control plane may revive the instance on this pod: it serves again,
    assert!(http1_get(sidecar.addr, "").await.ends_with("served"));
    // and after a new quiet interval it reports again.
    let reported = recorded_count(&sidecar.reports);
    wait_for_recorded_count(&sidecar.reports, reported + 1).await;
    assert!(http1_get(sidecar.addr, "").await.ends_with("served"));
    sidecar.stop().await;
}
