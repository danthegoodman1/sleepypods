use super::super::{
    serve_http_listener_with_shared, serve_tls_passthrough_listener_with_shared,
    serve_tls_termination_listener_with_shared, SharedFrontlineHttpRuntime,
};
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
    tokio::time::timeout(Duration::from_secs(2), limiter.wait_for_in_flight(expected))
        .await
        .unwrap();
}

#[tokio::test]
async fn tls_and_sni_setup_share_global_admission_with_http_and_recover() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::CREATED, READY_RESPONSE).await;
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("app.example.com", "/secure"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
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
    let shared = SharedFrontlineHttpRuntime::from_runtime(
        runtime_with_state(
            route_client.clone(),
            DrainTracker::new(Duration::from_secs(1)),
        ),
        admission.clone(),
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
    let http_task = tokio::spawn(serve_http_listener_with_shared(
        http,
        shared.clone(),
        shutdown.clone(),
    ));
    let tls_task = tokio::spawn(serve_tls_termination_listener_with_shared(
        tls,
        shared.clone(),
        adapter.clone(),
        shutdown.clone(),
    ));
    let sni_task = tokio::spawn(serve_tls_passthrough_listener_with_shared(
        sni,
        shared,
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
    http_task.await.unwrap().unwrap();
    tls_task.await.unwrap().unwrap();
    sni_task.await.unwrap().unwrap();
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
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        sni_identity("db.example.com"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("tcp://{upstream_addr}"), 3)),
        ),
    );
    let shared = SharedFrontlineHttpRuntime::from_runtime(
        runtime_with_state(
            route_client.clone(),
            DrainTracker::new(Duration::from_secs(1)),
        ),
        admission.clone(),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let runtime = tokio::spawn(serve_tls_passthrough_listener_with_shared(
        listener,
        shared,
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()).with_resource_config(resources),
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
