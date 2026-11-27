use super::super::{serve_frontline_listeners, FrontlineListeners};
use super::*;
use proxy_core::{ProxyAdmission, ProxyResourceConfig};

async fn closed(stream: &mut TcpStream) {
    let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut [0; 1]))
        .await
        .unwrap();
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "excess or stalled socket was not closed: {result:?}"
    );
}
async fn count(limiter: &proxy_core::AdmissionLimiter, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while limiter.in_flight() != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tls_and_sni_setup_share_global_admission_with_http_and_recover() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::CREATED, READY_RESPONSE).await;
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-admission"),
            http_identity("app.example.com", "/secure"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let resources = ProxyResourceConfig::default()
        .with_limits(2, 1, 1, 1)
        .unwrap()
        .with_timeouts(
            Duration::from_millis(150),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
    let admission = ProxyAdmission::new(resources);
    let runtime = runtime_with_state(
        state,
        FakeRouteClient::default(),
        DrainTracker::new(Duration::from_secs(1)),
    );
    let http = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let http_addr = http.local_addr().unwrap();
    let tls = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let tls_addr = tls.local_addr().unwrap();
    let sni = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let sni_addr = sni.local_addr().unwrap();
    let cert = test_cert("app.example.com");
    let client_config = client_config_trusting(cert.cert.clone());
    let fixture = crate::certificates::test_support::CertificateFixture::new();
    let store = fixture.cache.clone();
    fixture
        .publish("app.example.com", vec![cert.cert], cert.key)
        .unwrap();
    let adapter = FrontlineTlsAdapter::new(store).with_resource_config(resources);
    let shutdown = Shutdown::new();
    let frontline = tokio::spawn(serve_frontline_listeners(
        FrontlineListeners {
            http,
            tls_termination: Some(tls),
            tls_passthrough: Some(sni),
        },
        admission.clone(),
        runtime,
        adapter,
        shutdown.clone(),
    ));
    for target in [tls_addr, sni_addr] {
        let mut held = TcpStream::connect(target).await.unwrap();
        held.write_all(&[22, 3, 1, 0, 42]).await.unwrap();
        count(&admission.handshakes, 1).await;
        for _ in 0..8 {
            let mut excess = TcpStream::connect(http_addr).await.unwrap();
            closed(&mut excess).await;
        }
        assert_eq!(admission.connections.in_flight(), 1);
        closed(&mut held).await;
        count(&admission.handshakes, 0).await;
    }
    let response = raw_https_request(
        tls_addr,
        "app.example.com",
        "app.example.com",
        "/secure",
        TLS_REQUEST_BODY,
        client_config,
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 201 Created"), "{response}");
    shutdown.shutdown();
    frontline.await.unwrap().unwrap();
    upstream_task.await.unwrap();
    assert_eq!(admission.connections.in_flight(), 0);
    assert_eq!(admission.requests.in_flight(), 0);
    assert_eq!(admission.handshakes.in_flight(), 0);
    fixture.finish().await;
}

#[test]
fn frontline_environment_applies_and_validates_resource_overrides() {
    let config = crate::FrontlineEnvConfig::from_vars([
        ("SLEEPYPODS_FRONTLINE_LISTEN_ADDR", "127.0.0.1:8080"),
        (
            "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
            "http://127.0.0.1:50051",
        ),
        ("SLEEPYPODS_PROXY_MAX_CONNECTIONS", "7"),
        ("SLEEPYPODS_PROXY_SETUP_TIMEOUT_MS", "123"),
    ])
    .unwrap();
    assert_eq!(config.listener().resource_config().max_connections(), 7);
    assert_eq!(
        config.listener().resource_config().setup_timeout(),
        Duration::from_millis(123)
    );
    assert!(crate::FrontlineEnvConfig::from_vars([
        ("SLEEPYPODS_FRONTLINE_LISTEN_ADDR", "127.0.0.1:8080"),
        (
            "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
            "http://127.0.0.1:50051"
        ),
        ("SLEEPYPODS_PROXY_MAX_REQUESTS", "0"),
    ])
    .is_err());
}

#[tokio::test]
async fn sni_write_override_bounds_only_pending_writes_and_preserves_quiet_sessions() {
    let upstream = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let hello = client_hello(Some(sni_extension(b"db.example.com")));
    let expected = hello.clone();
    let (ready, started) = oneshot::channel();
    let app = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let mut prefix = vec![0; expected.len()];
        stream.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix, expected);
        ready.send(()).unwrap();
        let value = stream.read_u8().await.unwrap();
        stream.write_u8(value).await.unwrap();
        std::future::pending::<()>().await;
    });
    let resources = ProxyResourceConfig::default()
        .with_limits(1, 1, 1, 1)
        .unwrap()
        .with_timeouts(
            Duration::from_millis(150),
            Duration::from_secs(1),
            Duration::from_millis(150),
        )
        .unwrap();
    let admission = ProxyAdmission::new(resources);
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-sni-limit"),
            sni_identity("db.example.com"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("tcp://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let runtime = runtime_with_state(
        state,
        FakeRouteClient::default(),
        DrainTracker::new(Duration::from_secs(1)),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let runtime = tokio::spawn(serve_frontline_listeners(
        FrontlineListeners {
            http: TcpListener::bind(("127.0.0.1", 0)).await.unwrap(),
            tls_termination: None,
            tls_passthrough: Some(listener),
        },
        admission.clone(),
        runtime,
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()),
        shutdown.clone(),
    ));
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&hello).await.unwrap();
    started.await.unwrap();
    count(&admission.requests, 1).await;
    count(&admission.handshakes, 0).await;
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

/// Fails `accept` with the queued errors, then accepts real connections.
struct FailingAccept {
    errors: std::collections::VecDeque<std::io::Error>,
    inner: TcpListener,
}

impl proxy_core::Accept for FailingAccept {
    async fn accept(&mut self) -> std::io::Result<(TcpStream, SocketAddr)> {
        match self.errors.pop_front() {
            Some(error) => Err(error),
            None => self.inner.accept().await,
        }
    }
}

#[tokio::test]
async fn accept_errors_leave_frontline_serving_and_shutdown_closes_the_listener() {
    const EMFILE: i32 = 24;
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(2, StatusCode::CREATED, READY_RESPONSE).await;
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-accept"),
            http_identity("app.example.com", "/ready"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
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
    let shutdown = Shutdown::new();
    let frontline = tokio::spawn(serve_frontline_listeners(
        FrontlineListeners::http(listener),
        ProxyAdmission::new(ProxyResourceConfig::default()),
        runtime_with_state(
            state,
            FakeRouteClient::default(),
            DrainTracker::new(Duration::from_secs(1)),
        ),
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()),
        shutdown.clone(),
    ));

    for _ in 0..2 {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /ready HTTP/1.1\r\nHost: app.example.com\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 201 Created"), "{response}");
    }

    shutdown.shutdown();
    frontline.await.unwrap().unwrap();
    let refused = TcpStream::connect(addr).await.unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::ConnectionRefused);
    upstream_task.abort();
}
