use super::*;
use tokio_tungstenite::client_async;

struct NoIdle;
impl sidecar::ReportIdleClient for NoIdle {
    type Error = Infallible;
    fn report_idle(
        &mut self,
        _: sidecar::ReportIdleRequest,
    ) -> sidecar::ReportIdleFuture<'_, sidecar::ReportIdleResponse, Infallible> {
        Box::pin(std::future::pending())
    }
}

struct Chain {
    edge: SocketAddr,
    sidecar: SocketAddr,
    shutdown: Shutdown,
    edge_task: JoinHandle<Result<(), FrontlineListenerError>>,
    sidecar_task: JoinHandle<Result<(), sidecar::runtime::SidecarRuntimeError>>,
    app_task: JoinHandle<()>,
}

impl Chain {
    async fn stop(self) {
        self.shutdown.shutdown();
        self.edge_task.await.unwrap().unwrap();
        self.sidecar_task.await.unwrap().unwrap();
        self.app_task.abort();
        let _ = self.app_task.await;
    }
}

/// The app's answer to one request. `/plain` reports what the app observed
/// in response headers, so each client can assert it.
async fn app_response(mut request: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    use tokio_tungstenite::tungstenite::handshake::server::create_response_with_body;
    let path = request.uri().path().to_owned();
    if path == "/plain" {
        let host = request
            .uri()
            .authority()
            .map(|authority| authority.to_string())
            .or_else(|| {
                let host = request.headers().get("host")?;
                Some(host.to_str().unwrap().to_owned())
            })
            .unwrap_or_default();
        return Ok(Response::builder()
            .header("x-seen-version", format!("{:?}", request.version()))
            .header("x-seen-host", host)
            .header(
                "x-seen-forwarded-host",
                request.headers()["x-forwarded-host"].clone(),
            )
            .body(Full::new(Bytes::from_static(b"plain-ok")))
            .unwrap());
    }
    if path == "/reject" || path == "/forbidden" {
        let status = if path == "/forbidden" {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::UNAUTHORIZED
        };
        return Ok(Response::builder()
            .status(status)
            .header("www-authenticate", "Bearer realm=socket")
            .header("set-cookie", "retry=1; HttpOnly")
            .body(Full::new(Bytes::from_static(b"authentication required")))
            .unwrap());
    }
    assert_eq!(request.version(), Version::HTTP_11);
    assert_eq!(request.headers()["host"], "ws.example.com");
    assert_eq!(request.headers()["authorization"], "Bearer test-token");
    assert_eq!(request.headers()["cookie"], "session=abc");
    assert_eq!(request.headers()["origin"], "https://app.example.com");
    assert_eq!(request.headers()["x-application-header"], "keep-me");
    assert_eq!(request.headers()["x-padding"].as_bytes().len(), 4096);
    assert_eq!(
        request.uri().path_and_query().unwrap().as_str(),
        "/socket?room=blue"
    );
    assert_frontline_forwarded_headers(request.headers(), "http", "ws.example.com");
    assert!(!request.headers().contains_key("x-hop-only"));
    // The peers negotiate the subprotocol and extensions end to end.
    assert_eq!(
        request.headers()["sec-websocket-protocol"],
        "chat.v1, chat.v2"
    );
    assert_eq!(
        request.headers()["sec-websocket-extensions"],
        "permessage-deflate"
    );
    let mut response = create_response_with_body(&request, || Full::new(Bytes::new())).unwrap();
    for (name, value) in [
        ("sec-websocket-protocol", "chat.v2"),
        ("sec-websocket-extensions", "permessage-deflate"),
        ("set-cookie", "session=new; HttpOnly"),
        ("set-cookie", "theme=dark"),
        ("x-application-response", "accepted"),
    ] {
        response.headers_mut().append(name, value.parse().unwrap());
    }
    let upgraded = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        let socket = upgraded.await.unwrap();
        let mut socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            TokioIo::new(socket),
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        while let Some(Ok(message)) = socket.next().await {
            if message.is_close() {
                let _ = socket.flush().await;
                break;
            }
            socket.send(message).await.unwrap();
        }
    });
    Ok(response)
}

/// Frontline, a sidecar and an app. An `http1` app serves HTTP/1.1 only. An
/// `h2c` app serves prior-knowledge HTTP/2, and HTTP/1.1 for upgrades.
async fn chain(app_protocol: proxy_core::AppProtocol) -> Chain {
    let app = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let app_addr = app.local_addr().unwrap();
    let app_task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            let (stream, _) = app.accept().await.unwrap();
            while tasks.try_join_next().is_some() {}
            tasks.spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(app_response);
                match app_protocol {
                    proxy_core::AppProtocol::Http1 => {
                        let _ = http1::Builder::new()
                            .serve_connection(io, service)
                            .with_upgrades()
                            .await;
                    }
                    proxy_core::AppProtocol::H2c => {
                        let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection_with_upgrades(io, service)
                            .await;
                    }
                }
            });
        }
    });
    let sidecar_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let sidecar_addr = sidecar_listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let sidecar_config = sidecar::runtime::SidecarRuntimeConfig::new(
        sidecar_addr,
        app_addr.port(),
        InstanceId::new("protocol-test").unwrap(),
        Generation::new(7),
        sidecar::IdleReportConfig::new(
            Duration::from_secs(3600),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .unwrap(),
        Duration::from_secs(2),
    )
    .unwrap()
    .with_app_protocol(app_protocol);
    let sidecar_task = tokio::spawn(sidecar::runtime::serve_http_listener_with_idle(
        sidecar_listener,
        sidecar_config,
        NoIdle,
        shutdown.clone(),
    ));
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("ws.example.com", "/"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{sidecar_addr}"), 3)),
        ),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let edge = listener.local_addr().unwrap();
    let runtime = runtime_with_state(
        route_client.clone(),
        DrainTracker::new(Duration::from_secs(2)),
    );
    let edge_task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));
    Chain {
        edge,
        sidecar: sidecar_addr,
        shutdown,
        edge_task,
        sidecar_task,
        app_task,
    }
}

fn authenticated_request(addr: SocketAddr) -> WsRequest {
    let mut request = format!("ws://{addr}/socket?room=blue")
        .into_client_request()
        .unwrap();
    for (name, value) in [
        ("host", "ws.example.com"),
        ("authorization", "Bearer test-token"),
        ("cookie", "session=abc"),
        ("origin", "https://app.example.com"),
        ("x-application-header", "keep-me"),
        ("sec-websocket-protocol", "chat.v1, chat.v2"),
        ("sec-websocket-extensions", "permessage-deflate"),
        ("forwarded", "for=spoof"),
        ("x-forwarded-for", "spoof"),
        ("x-forwarded-host", "spoof"),
        ("x-forwarded-proto", "https"),
        ("x-forwarded-prefix", "/spoof"),
        ("x-hop-only", "drop-me"),
    ] {
        request.headers_mut().insert(
            http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    request.headers_mut().insert(
        "connection",
        "keep-alive, x-hop-only, X-Forwarded-For, X-Forwarded-Proto, X-Forwarded-Host, Upgrade"
            .parse()
            .unwrap(),
    );
    request
        .headers_mut()
        .insert("x-padding", "a".repeat(4096).parse().unwrap());
    request
}

/// What the app reported for a `/plain` request.
fn seen(headers: &HeaderMap) -> (String, String, String) {
    let value = |name: &str| headers[name].to_str().unwrap().to_owned();
    (
        value("x-seen-version"),
        value("x-seen-host"),
        value("x-seen-forwarded-host"),
    )
}

#[tokio::test]
async fn http1_http2_and_websocket_clients_reach_both_app_protocols_with_the_client_host() {
    for (app_protocol, app_version) in [
        (proxy_core::AppProtocol::Http1, "HTTP/1.1"),
        (proxy_core::AppProtocol::H2c, "HTTP/2.0"),
    ] {
        let chain = chain(app_protocol).await;
        let expected = (
            app_version.to_owned(),
            "ws.example.com".to_owned(),
            "ws.example.com".to_owned(),
        );

        let stream = TcpStream::connect(chain.edge).await.unwrap();
        let (mut http1, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(connection);
        let request = Request::builder()
            .uri("/plain")
            .header("host", "ws.example.com")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = http1.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(seen(response.headers()), expected, "HTTP/1.1 client");

        let stream = TcpStream::connect(chain.edge).await.unwrap();
        let (mut http2, connection) =
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                .await
                .unwrap();
        tokio::spawn(connection);
        let request = Request::builder()
            .uri("http://ws.example.com/plain")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = http2.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.version(), Version::HTTP_2);
        assert_eq!(seen(response.headers()), expected, "HTTP/2 client");

        let (mut socket, response) = connect_async(authenticated_request(chain.edge))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        socket.send(Message::Text("echo".into())).await.unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Text("echo".into())
        );
        socket.close(None).await.unwrap();
        let _ = socket.next().await;
        drop(socket);

        chain.stop().await;
    }
}

#[tokio::test]
async fn authenticated_websocket_preserves_handshake_through_both_proxies_after_keepalive() {
    let chain = chain(proxy_core::AppProtocol::Http1).await;
    // Exercise the sidecar's actual second request upgrade path directly too;
    // frontline creates a fresh sidecar connection for its WebSocket hop.
    for addr in [chain.edge, chain.sidecar] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /plain HTTP/1.1\r\nHost: ws.example.com\r\nX-Forwarded-Host: ws.example.com\r\n\r\n")
            .await
            .unwrap();
        read_until(&mut stream, b"plain-ok").await;
        let mut request = authenticated_request(addr);
        if addr == chain.sidecar {
            // Sidecar trusts the edge policy. Supply its canonical output.
            proxy_core::apply_forwarded_header_policy(
                &mut request,
                "127.0.0.1".parse().unwrap(),
                "http",
            );
        }
        let (mut socket, response) = client_async(request, stream).await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(response.headers()["sec-websocket-protocol"], "chat.v2");
        assert_eq!(
            response.headers()["sec-websocket-extensions"],
            "permessage-deflate"
        );
        assert_eq!(response.headers()["x-application-response"], "accepted");
        assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
        socket
            .send(Message::Text("authenticated echo".into()))
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Text("authenticated echo".into())
        );
        socket.close(None).await.unwrap();
        let _ = socket.next().await;
    }
    chain.stop().await;
}

#[tokio::test]
async fn websocket_rejection_status_headers_and_complete_body_survive_both_proxies() {
    let chain = chain(proxy_core::AppProtocol::Http1).await;
    for (path, expected) in [
        ("/reject", StatusCode::UNAUTHORIZED),
        ("/forbidden", StatusCode::FORBIDDEN),
    ] {
        let mut request = format!("ws://{}{path}", chain.edge)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("host", "ws.example.com".parse().unwrap());
        // Hyper verifies the complete response body, avoiding tungstenite's prefix-only
        // body behavior for a failed client handshake.
        let stream = TcpStream::connect(chain.edge).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let task = tokio::spawn(connection.with_upgrades());
        let response = sender
            .send_request(request.map(|_| Full::new(Bytes::new())))
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(
            response.headers()["www-authenticate"],
            "Bearer realm=socket"
        );
        assert_eq!(response.headers()["set-cookie"], "retry=1; HttpOnly");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "authentication required"
        );
        task.abort();
    }
    chain.stop().await;
}
