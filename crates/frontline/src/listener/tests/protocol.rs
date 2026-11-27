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
    edge_task: JoinHandle<Result<(), super::super::FrontlineHttpListenerError>>,
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

async fn chain() -> Chain {
    let app = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let app_addr = app.local_addr().unwrap();
    let app_task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            let (stream, _) = app.accept().await.unwrap();
            while tasks.try_join_next().is_some() {}
            tasks.spawn(proxy_core::serve_http_connection(
                stream,
                Shutdown::new(),
                |mut request| async move {
                    if request.uri().path() == "/plain" {
                        return Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                            b"plain-ok",
                        ))));
                    }
                    if request.uri().path() == "/reject" || request.uri().path() == "/forbidden" {
                        let status = if request.uri().path() == "/forbidden" {
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
                    if request.uri().path() == "/bad-protocol"
                        || request.uri().path() == "/bad-extension"
                    {
                        let mut response = proxy_core::websocket_upgrade_response(
                            &request,
                            Full::new(Bytes::new()),
                        )
                        .unwrap();
                        if request.uri().path() == "/bad-protocol" {
                            response
                                .headers_mut()
                                .insert("sec-websocket-protocol", "not-offered".parse().unwrap());
                        } else {
                            response.headers_mut().insert(
                                "sec-websocket-extensions",
                                "permessage-deflate".parse().unwrap(),
                            );
                        }
                        return Ok(response);
                    }
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
                    assert!(!request.headers().contains_key("sec-websocket-extensions"));
                    let mut response =
                        proxy_core::websocket_upgrade_response(&request, Full::new(Bytes::new()))
                            .unwrap();
                    response
                        .headers_mut()
                        .insert("sec-websocket-protocol", "chat.v2".parse().unwrap());
                    response
                        .headers_mut()
                        .append("set-cookie", "session=new; HttpOnly".parse().unwrap());
                    response
                        .headers_mut()
                        .append("set-cookie", "theme=dark".parse().unwrap());
                    response
                        .headers_mut()
                        .insert("x-application-response", "accepted".parse().unwrap());
                    let upgraded = hyper::upgrade::on(&mut request);
                    tokio::spawn(async move {
                        let socket = upgraded.await.unwrap();
                        let mut socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
                            TokioIo::new(socket),
                            tokio_tungstenite::tungstenite::protocol::Role::Server,
                            None,
                        )
                        .await;
                        while let Some(message) = socket.next().await {
                            let message = message.unwrap();
                            if message.is_close() {
                                let _ = socket.flush().await;
                                break;
                            }
                            socket.send(message).await.unwrap();
                        }
                    });
                    Ok(response)
                },
            ));
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
        sidecar::IdleReportConfig::new(Duration::from_secs(3600), Duration::from_secs(1)).unwrap(),
        Duration::from_secs(2),
    )
    .unwrap();
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

#[tokio::test]
async fn authenticated_websocket_preserves_handshake_through_both_proxies_after_keepalive() {
    let chain = chain().await;
    // Exercise the sidecar's actual second request upgrade path directly too;
    // frontline creates a fresh sidecar connection for its WebSocket hop.
    for addr in [chain.edge, chain.sidecar] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /plain HTTP/1.1\r\nHost: ws.example.com\r\n\r\n")
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
        assert_eq!(response.headers()["x-application-response"], "accepted");
        assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
        assert!(!response.headers().contains_key("sec-websocket-extensions"));
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
    let chain = chain().await;
    let mut request = format!("ws://{}/reject", chain.edge)
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
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
    chain.stop().await;
}

#[tokio::test]
async fn websocket_forbidden_and_invalid_negotiation_never_publish_101() {
    let chain = chain().await;
    for (path, expected) in [
        ("/forbidden", StatusCode::FORBIDDEN),
        ("/bad-protocol", StatusCode::BAD_GATEWAY),
        ("/bad-extension", StatusCode::BAD_GATEWAY),
    ] {
        let mut request = format!("ws://{}{path}", chain.edge)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("host", "ws.example.com".parse().unwrap());
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "chat.v1".parse().unwrap());
        let error = connect_async(request).await.unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("expected HTTP rejection");
        };
        assert_eq!(response.status(), expected);
    }
    chain.stop().await;
}
