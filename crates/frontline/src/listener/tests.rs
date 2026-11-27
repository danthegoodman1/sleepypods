use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{HeaderMap, Request, Response, StatusCode, Version};
use http_body_util::{channel::Channel, BodyExt, Full};
use hyper::client::conn::http2 as client_http2;
use hyper::server::conn::http2;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use proxy_core::{DrainTracker, Shutdown};
use rcgen::generate_simple_self_signed;
use sleepypods_api::{
    BackendEndpoint, BackendGeneration, Generation, Http01ChallengeKey, Http01ChallengeRecord,
    InstanceId, InstanceState, PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
    RouteKind, RouteRule,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::{
    rustls::{
        pki_types::{CertificateDer, PrivateKeyDer, ServerName},
        ClientConfig, RootCertStore,
    },
    TlsConnector,
};
use tokio_tungstenite::{
    accept_hdr_async, connect_async,
    tungstenite::{
        client::IntoClientRequest,
        handshake::server::{
            Callback, ErrorResponse, Request as WsRequest, Response as WsResponse,
        },
        protocol::CloseFrame,
        Bytes as WsBytes, Message,
    },
};

use super::{
    serve_frontline, serve_frontline_listeners, FrontlineHttpListenerConfig,
    FrontlineListenerError, FrontlineListeners, FrontlineListenersConfig,
    FrontlineTlsPassthroughListenerConfig, FrontlineTlsTerminationListenerConfig,
};
use crate::{
    route::testing::{FakeControlPlane, FakeWake},
    FrontlineHttpRuntime, FrontlineRouteCoordinator, FrontlineTlsAdapter,
    Http01ChallengeResolveFuture, Http01ChallengeResolver, RouteLookupCounters,
    RouteSubscriptionClient, TlsCertificateStore, WakeClient,
};

/// A frontline that serves plain HTTP on `listener` with default bounds.
async fn serve_http_listener<RouteClient, Wake, Http01>(
    listener: TcpListener,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
) -> Result<(), FrontlineListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let admission = proxy_core::ProxyAdmission::new(proxy_core::ProxyResourceConfig::default());
    serve_http_listener_with_admission(listener, runtime, shutdown, admission).await
}

async fn serve_http_listener_with_admission<RouteClient, Wake, Http01>(
    listener: TcpListener,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
    admission: proxy_core::ProxyAdmission,
) -> Result<(), FrontlineListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    serve_frontline_listeners(
        FrontlineListeners::http(listener),
        admission,
        runtime,
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()),
        shutdown,
    )
    .await
}

const READY_RESPONSE: &[u8] = b"ready-from-listener-upstream";
const TLS_REQUEST_BODY: &[u8] = b"tls-listener-request";

#[tokio::test]
async fn actual_serve_boundary_rejects_oversized_programmatic_activation_budget() {
    let coordinator =
        FrontlineRouteCoordinator::new(4, FakeControlPlane::default(), FakeWake::default())
            .with_route_deadline(Duration::from_secs(131));
    let runtime = FrontlineHttpRuntime::new(coordinator, DrainTracker::new(Duration::from_secs(1)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut owned_connection = TcpStream::connect(addr).await.unwrap();
    let result = serve_http_listener(listener, runtime, Shutdown::new()).await;
    assert!(matches!(
        result,
        Err(FrontlineListenerError::InitialActivationBudgetExceeded)
    ));
    let mut byte = [0; 1];
    let closed = tokio::time::timeout(Duration::from_secs(1), owned_connection.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0))
            || closed.is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionReset),
        "the connection to the owned rejected listener must close without starting tasks"
    );
}

#[derive(Clone, Debug, Default)]
struct FakeHttp01Resolver {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

impl FakeHttp01Resolver {
    fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().expect("HTTP-01 calls lock").clone()
    }
}

impl Http01ChallengeResolver for FakeHttp01Resolver {
    type Error = Infallible;

    fn resolve_http01_challenge(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Http01ChallengeResolveFuture<'_, Self::Error> {
        self.calls
            .lock()
            .expect("HTTP-01 calls lock")
            .push((key.host().as_str().to_owned(), key.token().to_owned()));
        Box::pin(async { Ok::<Option<Http01ChallengeRecord>, Infallible>(None) })
    }
}

#[tokio::test]
async fn listener_forwards_http_request_through_ready_route() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("app.example.com", "/ready"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let (addr, shutdown, task) = spawn_frontline_listener(route_client.clone()).await;

    let response = listener_request(addr, "app.example.com", "/ready?via=listener").await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("response body reads")
            .to_bytes(),
        Bytes::from_static(READY_RESPONSE)
    );
    assert_eq!(route_client.subscribes(), 1);

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn listener_applies_forwarded_header_trust_policy_to_http_upstream() {
    let (upstream_addr, upstream_task) =
        spawn_forwarded_header_asserting_http_upstream("http").await;
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("app.example.com", "/forwarded"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let (addr, shutdown, task) = spawn_frontline_listener(route_client.clone()).await;
    let client = Client::builder(TokioExecutor::new()).build_http();

    let response = client
        .request(
            Request::builder()
                .uri(format!("http://{addr}/forwarded"))
                .header("host", "app.example.com")
                .header("forwarded", "for=198.51.100.10;proto=https")
                .header("x-forwarded-for", "198.51.100.11")
                .header("x-forwarded-proto", "https")
                .header("x-forwarded-host", "spoof.example.com")
                .header("x-forwarded-prefix", "/spoofed")
                .header(
                    "connection",
                    "x-forwarded-for, x-forwarded-proto, x-forwarded-host",
                )
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
        )
        .await
        .expect("listener request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(route_client.subscribes(), 1);

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn frontline_runtime_wires_tls_termination_listener_to_http_forwarding() {
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
    let http_addr = reserve_addr().await;
    let tls_addr = reserve_addr().await;
    let cert = test_cert("app.example.com");
    let client_config = client_config_trusting(cert.cert.clone());
    let fixture = crate::certificates::test_support::CertificateFixture::new();
    let store = fixture.cache.clone();
    fixture
        .publish("app.example.com", vec![cert.cert], cert.key)
        .expect("cert inserts");
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(
        route_client.clone(),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let config = FrontlineListenersConfig::new(FrontlineHttpListenerConfig::new(http_addr))
        .with_tls_termination(Some(FrontlineTlsTerminationListenerConfig::new(tls_addr)));
    let task = tokio::spawn(serve_frontline(
        config,
        runtime,
        FrontlineTlsAdapter::new(store),
        shutdown.clone(),
    ));

    let response = raw_https_request(
        tls_addr,
        "app.example.com",
        "app.example.com",
        "/secure?via=listener",
        TLS_REQUEST_BODY,
        client_config,
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 201 Created"), "{response}");
    assert!(
        response.ends_with("ready-from-listener-upstream"),
        "{response}"
    );
    assert_eq!(route_client.subscribes(), 1);

    shutdown.shutdown();
    task.await
        .expect("frontline task joins")
        .expect("frontline exits");
    upstream_task.await.expect("upstream task joins");
    fixture.finish().await;
}

#[tokio::test]
async fn tls_termination_applies_forwarded_header_trust_policy_to_http_upstream() {
    let (upstream_addr, upstream_task) =
        spawn_forwarded_header_asserting_http_upstream("https").await;
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("app.example.com", "/secure-forwarded"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let http_addr = reserve_addr().await;
    let tls_addr = reserve_addr().await;
    let cert = test_cert("app.example.com");
    let client_config = client_config_trusting(cert.cert.clone());
    let fixture = crate::certificates::test_support::CertificateFixture::new();
    let store = fixture.cache.clone();
    fixture
        .publish("app.example.com", vec![cert.cert], cert.key)
        .expect("cert inserts");
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(
        route_client.clone(),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let config = FrontlineListenersConfig::new(FrontlineHttpListenerConfig::new(http_addr))
        .with_tls_termination(Some(FrontlineTlsTerminationListenerConfig::new(tls_addr)));
    let task = tokio::spawn(serve_frontline(
        config,
        runtime,
        FrontlineTlsAdapter::new(store),
        shutdown.clone(),
    ));

    let response = raw_https_request_with_headers(
        tls_addr,
        "app.example.com",
        "app.example.com",
        "/secure-forwarded",
        TLS_REQUEST_BODY,
        client_config,
        &[
            ("Forwarded", "for=198.51.100.10;proto=http"),
            ("X-Forwarded-For", "198.51.100.11"),
            ("X-Forwarded-Proto", "http"),
            ("X-Forwarded-Host", "spoof.example.com"),
            ("X-Forwarded-Prefix", "/spoofed"),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert_eq!(route_client.subscribes(), 1);

    shutdown.shutdown();
    task.await
        .expect("frontline task joins")
        .expect("frontline exits");
    upstream_task.await.expect("upstream task joins");
    fixture.finish().await;
}

#[tokio::test]
async fn frontline_runtime_wires_tls_passthrough_listener_to_sni_route_and_preserves_prefix() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    let hello = client_hello(Some(sni_extension(b"Db.Example.COM.")));
    let tail = b"database startup tail";
    let mut expected = hello.clone();
    expected.extend_from_slice(tail);
    let upstream_response = Bytes::from_static(b"database response bytes");
    let upstream_task = tokio::spawn({
        let upstream_response = upstream_response.clone();
        async move {
            let (mut stream, _) = upstream_listener.accept().await.expect("upstream accepts");
            let mut received = vec![0; expected.len()];
            stream
                .read_exact(&mut received)
                .await
                .expect("upstream reads preserved bytes");
            assert_eq!(received, expected);
            stream
                .write_all(&upstream_response)
                .await
                .expect("upstream writes response");
            stream.shutdown().await.expect("upstream shuts down");
        }
    });

    let route_client = FakeControlPlane::default();
    let request_identity = sni_identity("db.example.com");
    seed(
        &route_client,
        request_identity,
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("tcp://{upstream_addr}"), 3)),
        ),
    );
    let http_addr = reserve_addr().await;
    let passthrough_addr = reserve_addr().await;
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(
        route_client.clone(),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let config = FrontlineListenersConfig::new(FrontlineHttpListenerConfig::new(http_addr))
        .with_tls_passthrough(Some(FrontlineTlsPassthroughListenerConfig::new(
            passthrough_addr,
        )));
    let task = tokio::spawn(serve_frontline(
        config,
        runtime,
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()),
        shutdown.clone(),
    ));

    let mut client = connect_tcp(passthrough_addr).await;
    client.write_all(&hello).await.expect("client writes hello");
    client.write_all(tail).await.expect("client writes tail");
    client
        .shutdown()
        .await
        .expect("client write half shuts down");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("client reads response");

    assert_eq!(response, upstream_response);
    assert_eq!(
        route_client.subscribes_for(RouteKind::Sni, "db.example.com"),
        1
    );
    assert_eq!(route_client.subscribes(), 1);

    shutdown.shutdown();
    task.await
        .expect("frontline task joins")
        .expect("frontline exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn tls_passthrough_listener_rejects_malformed_or_missing_sni_without_route_lookup() {
    let route_client = FakeControlPlane::default();
    let http_addr = reserve_addr().await;
    let passthrough_addr = reserve_addr().await;
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(
        route_client.clone(),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let config = FrontlineListenersConfig::new(FrontlineHttpListenerConfig::new(http_addr))
        .with_tls_passthrough(Some(FrontlineTlsPassthroughListenerConfig::new(
            passthrough_addr,
        )));
    let task = tokio::spawn(serve_frontline(
        config,
        runtime,
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()),
        shutdown.clone(),
    ));

    let mut malformed = connect_tcp(passthrough_addr).await;
    malformed
        .write_all(b"GET / HTTP/1.1\r\n\r\n")
        .await
        .expect("malformed client writes");
    malformed
        .shutdown()
        .await
        .expect("malformed client shuts down");
    let malformed_response = read_to_end_or_reset(&mut malformed).await;

    let mut missing_sni = connect_tcp(passthrough_addr).await;
    missing_sni
        .write_all(&client_hello(None))
        .await
        .expect("missing-SNI client writes");
    missing_sni
        .shutdown()
        .await
        .expect("missing-SNI client shuts down");
    let missing_sni_response = read_to_end_or_reset(&mut missing_sni).await;

    assert!(malformed_response.is_empty());
    assert!(missing_sni_response.is_empty());
    assert_eq!(route_client.subscribes(), 0);

    shutdown.shutdown();
    task.await
        .expect("frontline task joins")
        .expect("frontline exits");
}

#[tokio::test]
async fn listener_h2c_grpc_shaped_request_routes_and_preserves_body_trailers() {
    let (upstream_addr, upstream_task) = spawn_h2c_grpc_upstream().await;
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("grpc.example.com", "/grpc.Test/Echo"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let (addr, shutdown, task) = spawn_frontline_listener(route_client.clone()).await;
    let stream = TcpStream::connect(addr).await.expect("h2c client connects");
    let (mut sender, connection) =
        client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .expect("h2c client handshake");
    let client_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let response = sender
        .send_request(
            Request::builder()
                .version(Version::HTTP_2)
                .method("POST")
                .uri("http://grpc.example.com/grpc.Test/Echo")
                .header("content-type", "application/grpc")
                .body(Full::new(Bytes::from_static(b"\0\0\0\0\x05world")))
                .expect("grpc request builds"),
        )
        .await
        .expect("h2c listener request succeeds");

    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content-type"),
        "application/grpc"
    );
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("response body and trailers read");
    let trailers = collected
        .trailers()
        .cloned()
        .expect("grpc trailers forwarded");
    assert_eq!(
        collected.to_bytes(),
        Bytes::from_static(b"\0\0\0\0\x05hello")
    );
    assert_eq!(trailers.get("grpc-status").expect("grpc-status"), "0");
    assert_eq!(trailers.get("grpc-message").expect("grpc-message"), "ok");
    assert_eq!(route_client.subscribes(), 1);

    shutdown.shutdown();
    client_task.await.expect("h2c client task joins");
    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn listener_websocket_forwards_cached_ready_route_bidirectionally_and_closes() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("ws.example.com", "/socket"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let (path_seen_tx, path_seen_rx) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener.accept().await.expect("upstream accepts");
        let mut websocket = accept_hdr_async(
            stream,
            AssertListenerWebSocketRequest {
                path_seen_tx: Some(path_seen_tx),
            },
        )
        .await
        .expect("upstream accepts websocket");

        let text = websocket
            .next()
            .await
            .expect("upstream receives text")
            .expect("text frame valid");
        assert_eq!(text, Message::Text("frontline text".into()));
        websocket
            .send(Message::Text("backend text".into()))
            .await
            .expect("upstream sends text");

        let binary = websocket
            .next()
            .await
            .expect("upstream receives binary")
            .expect("binary frame valid");
        assert_eq!(
            binary,
            Message::Binary(WsBytes::from_static(b"frontline bytes"))
        );
        websocket
            .send(Message::Binary(WsBytes::from_static(b"backend bytes")))
            .await
            .expect("upstream sends binary");

        let close = websocket
            .next()
            .await
            .expect("upstream receives close")
            .expect("close frame valid");
        assert!(matches!(close, Message::Close(Some(_))));
        websocket.flush().await.expect("upstream flushes close");
    });
    let (addr, shutdown, task) = spawn_frontline_listener(route_client.clone()).await;
    let mut request = format!("ws://{addr}/socket?room=blue")
        .into_client_request()
        .expect("websocket request builds");
    request
        .headers_mut()
        .insert("host", "ws.example.com".parse().expect("host header"));
    request.headers_mut().insert(
        "forwarded",
        "for=198.51.100.10;proto=https"
            .parse()
            .expect("forwarded header"),
    );
    request.headers_mut().insert(
        "x-forwarded-for",
        "198.51.100.11".parse().expect("x-forwarded-for header"),
    );
    request.headers_mut().insert(
        "x-forwarded-proto",
        "https".parse().expect("x-forwarded-proto header"),
    );
    request.headers_mut().insert(
        "x-forwarded-host",
        "spoof.example.com"
            .parse()
            .expect("x-forwarded-host header"),
    );
    request.headers_mut().insert(
        "x-forwarded-prefix",
        "/spoofed".parse().expect("x-forwarded-prefix header"),
    );

    let (mut client, response) = connect_async(request)
        .await
        .expect("client connects through listener websocket");

    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    path_seen_rx.await.expect("upstream saw websocket path");

    client
        .send(Message::Text("frontline text".into()))
        .await
        .expect("client sends text");
    let from_upstream = client
        .next()
        .await
        .expect("client receives text")
        .expect("text frame valid");
    assert_eq!(from_upstream, Message::Text("backend text".into()));

    client
        .send(Message::Binary(WsBytes::from_static(b"frontline bytes")))
        .await
        .expect("client sends binary");
    let from_upstream = client
        .next()
        .await
        .expect("client receives binary")
        .expect("binary frame valid");
    assert_eq!(
        from_upstream,
        Message::Binary(WsBytes::from_static(b"backend bytes"))
    );

    client
        .send(Message::Close(Some(CloseFrame {
            code: 1000.into(),
            reason: "done".into(),
        })))
        .await
        .expect("client sends close");
    let close = client
        .next()
        .await
        .expect("client receives close")
        .expect("close frame valid");
    assert!(matches!(close, Message::Close(Some(frame)) if frame.reason == "done"));
    assert_eq!(route_client.subscribes(), 1);
    drop(client);

    upstream_task.await.expect("upstream task joins");
    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_websocket_backend_refusal_expires_without_upgrade() {
    let refused_addr = reserve_addr().await;
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("ws-refused.example.com", "/socket"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{refused_addr}"), 3)),
        ),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let resources = proxy_core::ProxyResourceConfig::default()
        .with_timeouts(
            Duration::from_millis(120),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
    let admission = proxy_core::ProxyAdmission::new(resources);
    let runtime = runtime_with_state(
        route_client.clone(),
        DrainTracker::new(Duration::from_secs(1)),
    );
    let task = tokio::spawn(serve_http_listener_with_admission(
        listener,
        runtime,
        shutdown.clone(),
        admission.clone(),
    ));

    // The listener must prove the upstream WebSocket is reachable before it
    // returns 101 to the client, so a refused backend maps to an HTTP error.
    let status = tokio::time::timeout(
        Duration::from_secs(1),
        raw_websocket_upgrade_status(addr, Some("ws-refused.example.com")),
    )
    .await
    .unwrap();

    assert!(status.starts_with("HTTP/1.1 504"), "{status}");
    assert_eq!(route_client.subscribes(), 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while admission.requests.in_flight() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_non_challenge_request_bypasses_http01_resolver_lock() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let route_client = FakeControlPlane::default();
    let http01_resolver = FakeHttp01Resolver::default();
    seed(
        &route_client,
        http_identity("app.example.com", "/normal"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = Shutdown::new();
    let runtime = FrontlineHttpRuntime::with_http01_resolver(
        FrontlineRouteCoordinator::new(4, route_client.clone(), FakeWake::default()),
        http01_resolver.clone(),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));

    let response = listener_request(addr, "app.example.com", "/normal").await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("response body reads")
            .to_bytes(),
        Bytes::from_static(READY_RESPONSE)
    );
    assert!(http01_resolver.calls().is_empty());
    assert_eq!(route_client.subscribes(), 1);

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn listener_route_miss_returns_not_found() {
    let route_client = FakeControlPlane::default();
    let (addr, shutdown, task) = spawn_frontline_listener(route_client.clone()).await;

    let response = listener_request(addr, "missing.example.com", "/").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        route_client.subscribes_for(RouteKind::Http, "missing.example.com"),
        1
    );

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_invalid_or_missing_host_returns_bad_request_without_control_plane_call() {
    let route_client = FakeControlPlane::default();
    let (addr, shutdown, task) = spawn_frontline_listener(route_client.clone()).await;

    let invalid = listener_request(addr, "localhost", "/").await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let mut raw = TcpStream::connect(addr).await.expect("raw client connects");
    raw.write_all(b"GET / HTTP/1.1\r\n\r\n")
        .await
        .expect("raw request writes");
    let status = read_status_line(&mut raw).await;
    assert!(status.starts_with("HTTP/1.1 400"), "{status}");

    assert_eq!(route_client.subscribes(), 0);

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_websocket_invalid_or_missing_host_rejects_without_control_plane_call() {
    let route_client = FakeControlPlane::default();
    let (addr, shutdown, task) = spawn_frontline_listener(route_client.clone()).await;

    let missing_host_status = raw_websocket_upgrade_status(addr, None).await;
    assert!(
        missing_host_status.starts_with("HTTP/1.1 400"),
        "{missing_host_status}"
    );

    let invalid_host_status = raw_websocket_upgrade_status(addr, Some("localhost")).await;
    assert!(
        invalid_host_status.starts_with("HTTP/1.1 400"),
        "{invalid_host_status}"
    );

    assert_eq!(route_client.subscribes(), 0);

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_shutdown_drains_active_http_response() {
    let (upstream_addr, release_upstream, upstream_task) = spawn_chunked_upstream().await;
    let route_client = FakeControlPlane::default();
    let drain = DrainTracker::new(Duration::from_secs(5));
    seed(
        &route_client,
        http_identity("stream.example.com", "/stream"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(route_client, drain.clone());
    let mut task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));

    let mut client = TcpStream::connect(addr).await.expect("raw client connects");
    client
        .write_all(b"GET /stream HTTP/1.1\r\nHost: stream.example.com\r\n\r\n")
        .await
        .expect("raw request writes");
    read_until(&mut client, b"hello").await;
    drain.wait_for_active_count(1).await;

    shutdown.shutdown();
    tokio::select! {
        result = &mut task => panic!("listener exited before response drained: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }

    release_upstream.send(()).expect("release upstream");
    read_until(&mut client, b"0\r\n\r\n").await;

    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

async fn spawn_frontline_listener(
    route_client: FakeControlPlane,
) -> (
    SocketAddr,
    Shutdown,
    JoinHandle<Result<(), FrontlineListenerError>>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = Shutdown::new();
    let drain = DrainTracker::new(Duration::from_secs(5));
    let runtime = runtime_with_state(route_client, drain.clone());
    let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));

    (addr, shutdown, task)
}

fn runtime_with_state(
    route_client: FakeControlPlane,
    drain: DrainTracker,
) -> FrontlineHttpRuntime<FakeControlPlane, FakeWake> {
    FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::new(4, route_client, FakeWake::default()),
        drain,
    )
}

/// Gives `identity`'s host one rule that covers every path on it.
fn seed(route_client: &FakeControlPlane, identity: RouteIdentity, entry: RouteEntry) {
    let rule_identity = match &identity {
        RouteIdentity::Http { host, .. } => RouteIdentity::Http {
            host: host.clone(),
            path: None,
        },
        RouteIdentity::Sni { host } => RouteIdentity::Sni { host: host.clone() },
    };
    route_client.set_rules(
        identity.kind(),
        identity.host().as_str(),
        vec![RouteRule {
            identity: rule_identity,
            entry,
        }],
    );
}

async fn listener_request(addr: SocketAddr, host: &str, path: &str) -> Response<Incoming> {
    let client = Client::builder(TokioExecutor::new()).build_http();
    client
        .request(
            Request::builder()
                .uri(format!("http://{addr}{path}"))
                .header("host", host)
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
        )
        .await
        .expect("listener request succeeds")
}

async fn raw_https_request(
    addr: SocketAddr,
    server_name: &'static str,
    host: &str,
    path: &str,
    body: &[u8],
    config: ClientConfig,
) -> String {
    raw_https_request_with_headers(addr, server_name, host, path, body, config, &[]).await
}

async fn raw_https_request_with_headers(
    addr: SocketAddr,
    server_name: &'static str,
    host: &str,
    path: &str,
    body: &[u8],
    config: ClientConfig,
    extra_headers: &[(&str, &str)],
) -> String {
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from(server_name)
        .expect("server name")
        .to_owned();
    let tcp = connect_tcp(addr).await;
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .expect("tls client connects");

    let mut request = format!("POST {path} HTTP/1.1\r\nHost: {host}\r\n");
    for (name, value) in extra_headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str(&format!(
        "Connection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        std::str::from_utf8(body).expect("request body is utf8")
    ));
    tls.write_all(request.as_bytes())
        .await
        .expect("client writes request");

    let mut response = Vec::new();
    tls.read_to_end(&mut response)
        .await
        .expect("client reads response");
    String::from_utf8(response).expect("response is utf8")
}

async fn reserve_addr() -> SocketAddr {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("reserve listener binds");
    listener.local_addr().expect("reserve listener addr")
}

async fn connect_tcp(addr: SocketAddr) -> TcpStream {
    let mut last_error = None;
    for _ in 0..50 {
        match TcpStream::connect(addr).await {
            Ok(stream) => return stream,
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    panic!(
        "client connects to {addr}: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "no connection attempt made".to_owned())
    );
}

async fn raw_websocket_upgrade_status(addr: SocketAddr, host: Option<&str>) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("raw client connects");
    let host = host
        .map(|host| format!("Host: {host}\r\n"))
        .unwrap_or_default();
    stream
        .write_all(
            format!(
                "GET /socket HTTP/1.1\r\n\
                 {host}\
                 Connection: Upgrade\r\n\
                 Upgrade: websocket\r\n\
                 Sec-WebSocket-Version: 13\r\n\
                 Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                 \r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("raw websocket request writes");

    read_status_line(&mut stream).await
}

async fn spawn_http_upstream(
    requests: usize,
    status: StatusCode,
    body: &'static [u8],
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let addr = listener.local_addr().expect("upstream addr");
    let task = tokio::spawn(async move {
        for _ in 0..requests {
            let (stream, _) = listener.accept().await.expect("upstream accepts");
            http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |mut request: Request<Incoming>| async move {
                        let _ = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("request body reads");
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .body(Full::new(Bytes::from_static(body)))
                                .expect("response builds"),
                        )
                    }),
                )
                .await
                .expect("upstream serves");
        }
    });

    (addr, task)
}

async fn spawn_forwarded_header_asserting_http_upstream(
    expected_proto: &'static str,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let addr = listener.local_addr().expect("upstream addr");
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts");
        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| async move {
                    assert_frontline_forwarded_headers(
                        request.headers(),
                        expected_proto,
                        "app.example.com",
                    );
                    let _ = request
                        .body_mut()
                        .collect()
                        .await
                        .expect("request body reads");
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from_static(READY_RESPONSE)))
                            .expect("response builds"),
                    )
                }),
            )
            .await
            .expect("upstream serves");
    });

    (addr, task)
}

fn assert_frontline_forwarded_headers(
    headers: &HeaderMap,
    expected_proto: &str,
    expected_host: &str,
) {
    assert!(headers.get("forwarded").is_none());
    assert_eq!(
        headers
            .get("x-forwarded-for")
            .expect("canonical x-forwarded-for"),
        "127.0.0.1"
    );
    assert_eq!(
        headers
            .get("x-forwarded-proto")
            .expect("canonical x-forwarded-proto"),
        expected_proto
    );
    assert_eq!(
        headers
            .get("x-forwarded-host")
            .expect("canonical x-forwarded-host"),
        expected_host
    );
    assert!(headers.get("x-forwarded-prefix").is_none());
}

struct AssertListenerWebSocketRequest {
    path_seen_tx: Option<oneshot::Sender<()>>,
}

impl Callback for AssertListenerWebSocketRequest {
    fn on_request(
        mut self,
        request: &WsRequest,
        response: WsResponse,
    ) -> Result<WsResponse, ErrorResponse> {
        assert_eq!(
            request
                .uri()
                .path_and_query()
                .expect("websocket path query")
                .as_str(),
            "/socket?room=blue"
        );
        assert_frontline_forwarded_headers(request.headers(), "http", "ws.example.com");
        self.path_seen_tx
            .take()
            .expect("path signal unused")
            .send(())
            .expect("test waits for websocket path");
        Ok(response)
    }
}

async fn spawn_h2c_grpc_upstream() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let addr = listener.local_addr().expect("upstream addr");
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts");

        http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|mut request: Request<Incoming>| async move {
                    assert_eq!(request.version(), Version::HTTP_2);
                    assert_eq!(
                        request.uri().path_and_query().expect("path query").as_str(),
                        "/grpc.Test/Echo"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("content-type")
                            .expect("grpc content-type"),
                        "application/grpc"
                    );
                    let body = request
                        .body_mut()
                        .collect()
                        .await
                        .expect("request body reads")
                        .to_bytes();
                    assert_eq!(body, Bytes::from_static(b"\0\0\0\0\x05world"));

                    let (mut sender, body) = Channel::<Bytes, Infallible>::new(2);
                    tokio::spawn(async move {
                        sender
                            .send_data(Bytes::from_static(b"\0\0\0\0\x05hello"))
                            .await
                            .expect("sends grpc response body");
                        let mut trailers = HeaderMap::new();
                        trailers.insert("grpc-status", "0".parse().expect("status header"));
                        trailers.insert("grpc-message", "ok".parse().expect("message header"));
                        sender
                            .send_trailers(trailers)
                            .await
                            .expect("sends grpc trailers");
                    });

                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "application/grpc")
                            .body(body)
                            .expect("response builds"),
                    )
                }),
            )
            .await
            .expect("upstream serves h2");
    });

    (addr, task)
}

async fn spawn_chunked_upstream() -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let addr = listener.local_addr().expect("upstream addr");
    let (release_tx, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("upstream accepts");
        read_until(&mut stream, b"\r\n\r\n").await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n")
            .await
            .expect("first response chunk writes");
        release_rx.await.expect("release signal");
        stream
            .write_all(b"0\r\n\r\n")
            .await
            .expect("final response chunk writes");
    });

    (addr, release_tx, task)
}

async fn read_status_line(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut one = [0; 1];
    loop {
        let read = stream.read(&mut one).await.expect("status reads");
        assert_ne!(read, 0, "connection closed before status line");
        bytes.push(one[0]);
        if bytes.ends_with(b"\r\n") {
            break;
        }
    }

    String::from_utf8(bytes).expect("status is utf8")
}

async fn read_until(stream: &mut TcpStream, needle: &[u8]) {
    let mut bytes = Vec::new();
    let mut buffer = [0; 256];
    while !bytes.windows(needle.len()).any(|window| window == needle) {
        let read = stream.read(&mut buffer).await.expect("stream reads");
        assert_ne!(read, 0, "connection closed before needle");
        bytes.extend_from_slice(&buffer[..read]);
    }
}

async fn read_to_end_or_reset(stream: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    match stream.read_to_end(&mut bytes).await {
        Ok(_) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => bytes,
        Err(error) => panic!("client reads close or reset: {error}"),
    }
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

fn route_binding_id(value: &str) -> RouteBindingId {
    RouteBindingId::new(value).expect("route binding ID")
}

fn backend(uri: impl Into<String>) -> BackendEndpoint {
    BackendEndpoint::new(uri).expect("backend")
}

fn http_identity(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("host"),
        path: Some(PathPrefix::new(path).expect("path")),
    }
}

fn sni_identity(host: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::exact(host).expect("host"),
    }
}

fn test_cert(host: &str) -> TestCert {
    let rcgen::CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec![host.to_owned()]).expect("cert generates");

    TestCert {
        cert: cert.der().clone(),
        key: PrivateKeyDer::from(signing_key),
    }
}

struct TestCert {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

fn client_config_trusting(cert: CertificateDer<'static>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(cert).expect("root cert inserts");
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

fn route_entry(
    state: InstanceState,
    instance_generation: u64,
    backend_uri_and_generation: Option<(String, u64)>,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: route_binding_id("route-a"),
        instance_id: instance_id("instance-a"),
        instance_state: state,
        instance_generation: Generation::new(instance_generation),
        backend: backend_uri_and_generation
            .as_ref()
            .map(|(uri, _generation)| backend(uri.clone())),
        pod_backend: None,
        backend_generation: backend_uri_and_generation
            .map(|(_uri, generation)| BackendGeneration::new(generation)),
    }
}

fn client_hello(extensions: Option<Vec<u8>>) -> Vec<u8> {
    record(client_hello_handshake(extensions))
}

fn client_hello_handshake(extensions: Option<Vec<u8>>) -> Vec<u8> {
    handshake(client_hello_body(extensions))
}

fn client_hello_body(extensions: Option<Vec<u8>>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0x11; 32]);
    body.push(0);
    push_u16(&mut body, 2);
    body.extend_from_slice(&[0x13, 0x01]);
    body.push(1);
    body.push(0);

    if let Some(extensions) = extensions {
        push_u16(&mut body, extensions.len());
        body.extend_from_slice(&extensions);
    }

    body
}

fn handshake(body: Vec<u8>) -> Vec<u8> {
    let mut handshake = Vec::new();
    handshake.push(0x01);
    push_u24(&mut handshake, body.len());
    handshake.extend_from_slice(&body);
    handshake
}

fn record(payload: Vec<u8>) -> Vec<u8> {
    let mut record = Vec::new();
    record.extend_from_slice(&[0x16, 0x03, 0x03]);
    push_u16(&mut record, payload.len());
    record.extend_from_slice(&payload);
    record
}

fn sni_extension(hostname: &[u8]) -> Vec<u8> {
    let mut name = Vec::new();
    name.push(0);
    push_u16(&mut name, hostname.len());
    name.extend_from_slice(hostname);

    let mut extension_data = Vec::new();
    push_u16(&mut extension_data, name.len());
    extension_data.extend_from_slice(&name);

    extension(0, extension_data)
}

fn extension(extension_type: u16, data: Vec<u8>) -> Vec<u8> {
    let mut extension = Vec::new();
    push_u16(&mut extension, extension_type as usize);
    push_u16(&mut extension, data.len());
    extension.extend_from_slice(&data);
    extension
}

fn push_u16(bytes: &mut Vec<u8>, value: usize) {
    let value = u16::try_from(value).expect("test value fits in u16");
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u24(bytes: &mut Vec<u8>, value: usize) {
    assert!(value <= 0x00ff_ffff, "test value fits in u24");
    bytes.extend_from_slice(&[
        ((value >> 16) & 0xff) as u8,
        ((value >> 8) & 0xff) as u8,
        (value & 0xff) as u8,
    ]);
}

#[tokio::test]
async fn listener_first_cold_request_waits_through_accepted_and_waking_until_ready() {
    let (upstream, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let identity = http_identity("app.example.com", "/cold");
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        identity.clone(),
        route_entry(InstanceState::Cold, 7, None),
    );
    let wake = FakeWake::default();
    let runtime = FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::new(8, route_client.clone(), wake.clone()),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));
    let request = tokio::spawn(listener_request(addr, "app.example.com", "/cold"));

    // The wake is accepted, the instance wakes, and then it runs. Each change
    // reaches the proxy as its host's rule set.
    tokio::time::timeout(Duration::from_secs(2), async {
        while wake.calls().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the cold request asks for a wake");
    seed(
        &route_client,
        identity.clone(),
        route_entry(InstanceState::Waking, 8, None),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!request.is_finished());
    seed(
        &route_client,
        identity,
        route_entry(
            InstanceState::Running,
            8,
            Some((format!("http://{upstream}"), 3)),
        ),
    );

    let response = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(READY_RESPONSE)
    );
    assert_eq!(
        wake.calls().len(),
        1,
        "a waking instance is waited on, not woken again"
    );
    assert_eq!(
        route_client.subscribes(),
        1,
        "the pushes carried every change"
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn listener_counts_one_cache_miss_then_one_hit_per_request() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let route_client = FakeControlPlane::default();
    seed(
        &route_client,
        http_identity("app.example.com", "/"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    );
    let lookups = RouteLookupCounters::default();
    let runtime = FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::new(4, route_client.clone(), FakeWake::default())
            .with_lookup_counters(lookups.clone()),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));
    for _ in 0..2 {
        let response = listener_request(addr, "app.example.com", "/").await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        response.into_body().collect().await.unwrap();
    }

    assert_eq!(lookups.totals(), (1, 1), "one hit after one miss");
    assert_eq!(route_client.subscribes(), 1);
    shutdown.shutdown();
    task.await.unwrap().unwrap();
    upstream_task.await.unwrap();
}

mod protocol;

mod admission;
