use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, Http01ChallengeKey,
    Http01ChallengeRecord, InstanceId, InstanceState, PathPrefix, RouteBindingId, RouteEntry,
    RouteHost, RouteIdentity,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Mutex as TokioMutex},
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
    FrontlineHttpRuntime, FrontlineRouteCoordinator, FrontlineRouteOutcome, FrontlineRouteResolver,
    FrontlineTlsAdapter, Http01ChallengeResolveFuture, Http01ChallengeResolver, RouteRequestId,
    RouteSubscriptionClient, RouteSubscriptionFuture, SubscribeControlPlaneOutput, SubscriptionId,
    SubscriptionState, TlsCertificateStore, WakeClient, WakeClientFuture, WakeInstanceRequest,
    WakeInstanceResponse, WakeTracker,
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
type BlockingSubscribeResponse =
    oneshot::Receiver<Result<SubscribeControlPlaneOutput, TestRouteError>>;

#[tokio::test]
async fn actual_serve_boundary_rejects_oversized_programmatic_activation_budget() {
    let coordinator = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::from_parts(SubscriptionState::new(4), FakeRouteClient::default()),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
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
struct FakeRouteClient {
    calls: Arc<Mutex<Vec<RouteClientCall>>>,
    subscribe_responses: Arc<Mutex<VecDeque<Result<SubscribeControlPlaneOutput, TestRouteError>>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RouteClientCall {
    Subscribe {
        request_id: RouteRequestId,
        identity: RouteIdentity,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestRouteError;

#[derive(Clone, Debug, Default)]
struct FakeWakeClient {
    calls: Arc<Mutex<Vec<WakeInstanceRequest>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestWakeError;

#[derive(Clone, Debug, Default)]
struct FakeHttp01Resolver {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

#[derive(Clone, Debug, Default)]
struct BlockingRouteClient {
    calls: Arc<Mutex<Vec<RouteClientCall>>>,
    subscribe_responses: Arc<Mutex<VecDeque<BlockingSubscribeResponse>>>,
    subscribe_notifications: Arc<Mutex<VecDeque<oneshot::Sender<()>>>>,
}

impl FakeRouteClient {
    fn push_subscribe_response(&self, response: SubscribeControlPlaneOutput) {
        self.subscribe_responses
            .lock()
            .expect("responses lock")
            .push_back(Ok(response));
    }

    fn calls(&self) -> Vec<RouteClientCall> {
        self.calls.lock().expect("calls lock").clone()
    }
}

impl BlockingRouteClient {
    fn push_subscribe_response_channel(
        &self,
    ) -> oneshot::Sender<Result<SubscribeControlPlaneOutput, TestRouteError>> {
        let (tx, rx) = oneshot::channel();
        self.subscribe_responses
            .lock()
            .expect("responses lock")
            .push_back(rx);
        tx
    }

    fn notify_next_subscribe(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.subscribe_notifications
            .lock()
            .expect("notifications lock")
            .push_back(tx);
        rx
    }

    fn calls(&self) -> Vec<RouteClientCall> {
        self.calls.lock().expect("calls lock").clone()
    }
}

impl RouteSubscriptionClient for FakeRouteClient {
    type Error = TestRouteError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(RouteClientCall::Subscribe {
                request_id,
                identity,
            });
        let response = self
            .subscribe_responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .expect("queued subscribe response");
        Box::pin(async move { response })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
}

impl RouteSubscriptionClient for BlockingRouteClient {
    type Error = TestRouteError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(RouteClientCall::Subscribe {
                request_id,
                identity,
            });
        if let Some(notification) = self
            .subscribe_notifications
            .lock()
            .expect("notifications lock")
            .pop_front()
        {
            let _ = notification.send(());
        }
        let response = self
            .subscribe_responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .expect("queued subscribe response channel");
        Box::pin(async move { response.await.expect("subscribe response sent") })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
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

impl WakeClient for FakeWakeClient {
    type Error = TestWakeError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        self.calls.lock().expect("calls lock").push(request);
        Box::pin(async { panic!("wake should not be called in listener tests") })
    }
}

impl fmt::Display for TestRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("route error")
    }
}

impl std::error::Error for TestRouteError {}

impl fmt::Display for TestWakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("wake error")
    }
}

impl std::error::Error for TestWakeError {}

#[tokio::test]
async fn listener_forwards_http_request_through_ready_route() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-ready"),
            http_identity("app.example.com", "/ready"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let (addr, shutdown, task) = spawn_frontline_listener(state, route_client.clone()).await;

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
    assert!(route_client.calls().is_empty());

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
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-forwarded-http"),
            http_identity("app.example.com", "/forwarded"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let (addr, shutdown, task) = spawn_frontline_listener(state, route_client.clone()).await;
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
    assert!(route_client.calls().is_empty());

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
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-tls"),
            http_identity("app.example.com", "/secure"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
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
        state,
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
    assert!(route_client.calls().is_empty());

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
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-forwarded-tls"),
            http_identity("app.example.com", "/secure-forwarded"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
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
        state,
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
    assert!(route_client.calls().is_empty());

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

    let route_client = FakeRouteClient::default();
    let request_identity = sni_identity("db.example.com");
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-sni"),
        request_identity.clone(),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("tcp://{upstream_addr}"), 3)),
        ),
    ));
    let http_addr = reserve_addr().await;
    let passthrough_addr = reserve_addr().await;
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(
        SubscriptionState::new(4),
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
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request_identity,
        }]
    );

    shutdown.shutdown();
    task.await
        .expect("frontline task joins")
        .expect("frontline exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn tls_passthrough_listener_rejects_malformed_or_missing_sni_without_route_lookup() {
    let route_client = FakeRouteClient::default();
    let http_addr = reserve_addr().await;
    let passthrough_addr = reserve_addr().await;
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(
        SubscriptionState::new(4),
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
    assert!(route_client.calls().is_empty());

    shutdown.shutdown();
    task.await
        .expect("frontline task joins")
        .expect("frontline exits");
}

#[tokio::test]
async fn listener_h2c_grpc_shaped_request_routes_and_preserves_body_trailers() {
    let (upstream_addr, upstream_task) = spawn_h2c_grpc_upstream().await;
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-h2c"),
            http_identity("grpc.example.com", "/grpc.Test/Echo"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let (addr, shutdown, task) = spawn_frontline_listener(state, route_client.clone()).await;
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
    assert!(route_client.calls().is_empty());

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
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-websocket"),
            http_identity("ws.example.com", "/socket"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
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
    let (addr, shutdown, task) = spawn_frontline_listener(state, route_client.clone()).await;
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
    assert!(route_client.calls().is_empty());
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
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-websocket-refused"),
            http_identity("ws-refused.example.com", "/socket"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{refused_addr}"), 3)),
            ),
        ),
        now(),
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
        state,
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
    assert!(route_client.calls().is_empty());
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
    let route_client = FakeRouteClient::default();
    let http01_resolver = FakeHttp01Resolver::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-normal"),
            http_identity("app.example.com", "/normal"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = Shutdown::new();
    let runtime = FrontlineHttpRuntime::with_http01_resolver(
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(state, route_client.clone()),
            WakeTracker::new(),
            FakeWakeClient::default(),
        ),
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
    assert!(route_client.calls().is_empty());

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn listener_route_miss_returns_not_found() {
    let route_client = FakeRouteClient::default();
    let request_identity = http_identity("missing.example.com", "/");
    route_client.push_subscribe_response(miss_response(
        generated_request_id(1),
        request_identity.clone(),
    ));
    let (addr, shutdown, task) =
        spawn_frontline_listener(SubscriptionState::new(4), route_client.clone()).await;

    let response = listener_request(addr, "missing.example.com", "/").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request_identity,
        }]
    );

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_invalid_or_missing_host_returns_bad_request_without_control_plane_call() {
    let route_client = FakeRouteClient::default();
    let (addr, shutdown, task) =
        spawn_frontline_listener(SubscriptionState::new(4), route_client.clone()).await;

    let invalid = listener_request(addr, "localhost", "/").await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let mut raw = TcpStream::connect(addr).await.expect("raw client connects");
    raw.write_all(b"GET / HTTP/1.1\r\n\r\n")
        .await
        .expect("raw request writes");
    let status = read_status_line(&mut raw).await;
    assert!(status.starts_with("HTTP/1.1 400"), "{status}");

    assert!(route_client.calls().is_empty());

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_websocket_invalid_or_missing_host_rejects_without_control_plane_call() {
    let route_client = FakeRouteClient::default();
    let (addr, shutdown, task) =
        spawn_frontline_listener(SubscriptionState::new(4), route_client.clone()).await;

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

    assert!(route_client.calls().is_empty());

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_shutdown_drains_active_http_response() {
    let (upstream_addr, release_upstream, upstream_task) = spawn_chunked_upstream().await;
    let route_client = FakeRouteClient::default();
    let drain = DrainTracker::new(Duration::from_secs(5));
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-stream"),
            http_identity("stream.example.com", "/stream"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(state, route_client, drain.clone());
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

#[tokio::test]
async fn listener_shared_coordinator_prevents_duplicate_in_flight_subscribe_route() {
    let route_client = BlockingRouteClient::default();
    let release_first_subscribe = route_client.push_subscribe_response_channel();
    let first_subscribe_started = route_client.notify_next_subscribe();
    let coordinator = Arc::new(TokioMutex::new(FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client.clone()),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )));
    let identity = http_identity("app.example.com", "/same");

    let (first_locked_tx, first_locked_rx) = oneshot::channel();
    let first_coordinator = Arc::clone(&coordinator);
    let first_identity = identity.clone();
    let first = tokio::spawn(async move {
        let mut coordinator = first_coordinator.lock().await;
        let _ = first_locked_tx.send(());
        coordinator.route(first_identity, now()).await
    });
    first_locked_rx.await.expect("first route holds lock");
    first_subscribe_started
        .await
        .expect("first subscribe starts");
    assert!(
        coordinator.try_lock().is_err(),
        "coordinator lock remains held while first subscribe is in flight"
    );

    let second_coordinator = Arc::clone(&coordinator);
    let second_identity = identity.clone();
    let second = tokio::spawn(async move {
        let mut coordinator = second_coordinator.lock().await;
        coordinator.route(second_identity, now()).await
    });
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: identity.clone(),
        }]
    );

    release_first_subscribe
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("sub-shared"),
            identity.clone(),
            route_entry(
                InstanceState::Running,
                7,
                Some(("http://127.0.0.1:1".to_owned(), 3)),
            ),
        )))
        .expect("first subscribe response is received");

    let first = tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .expect("first route completes")
        .expect("first route task joins")
        .expect("first route succeeds");
    let second = tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .expect("second route completes")
        .expect("second route task joins")
        .expect("second route succeeds");

    assert!(matches!(first, FrontlineRouteOutcome::Ready(_)));
    assert!(matches!(second, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity,
        }]
    );
}

async fn spawn_frontline_listener(
    state: SubscriptionState,
    route_client: FakeRouteClient,
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
    let runtime = runtime_with_state(state, route_client, drain.clone());
    let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));

    (addr, shutdown, task)
}

fn runtime_with_state(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    drain: DrainTracker,
) -> FrontlineHttpRuntime<FakeRouteClient, FakeWakeClient> {
    FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(state, route_client),
            WakeTracker::new(),
            FakeWakeClient::default(),
        ),
        drain,
    )
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

fn now() -> Instant {
    Instant::now()
}

fn ttl(seconds: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_secs(seconds))
}

fn request_id(value: &str) -> RouteRequestId {
    RouteRequestId::new(value).expect("request ID")
}

fn generated_request_id(index: u64) -> RouteRequestId {
    request_id(&format!("req:{index}"))
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription ID")
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
        backend_generation: backend_uri_and_generation
            .map(|(_uri, generation)| BackendGeneration::new(generation)),
    }
}

fn resolved_response(
    request_id: RouteRequestId,
    subscription_id: SubscriptionId,
    matched_identity: RouteIdentity,
    entry: RouteEntry,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteResolved {
        request_id,
        subscription_id,
        matched_identity,
        entry,
        cache_policy: ttl(30),
    }
}

fn miss_response(
    request_id: RouteRequestId,
    identity: RouteIdentity,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteMiss {
        request_id,
        request_identity: identity,
        negative_cache_policy: ttl(30),
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

#[derive(Clone)]
struct AcceptedWakeClient(Arc<std::sync::atomic::AtomicUsize>);
impl WakeClient for AcceptedWakeClient {
    type Error = TestWakeError;
    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async move {
            Ok(WakeInstanceResponse::WakeStarted {
                instance_id: request.instance_id,
                generation: request.expected_generation.next(),
            })
        })
    }
}
#[tokio::test]
async fn listener_first_cold_request_waits_through_accepted_and_waking_until_ready() {
    let (upstream, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let identity = http_identity("app.example.com", "/cold");
    let mut state = SubscriptionState::new(8);
    state.cache_mut().insert_positive(
        subscription_id("cold"),
        identity.clone(),
        route_entry(InstanceState::Cold, 7, None),
        CachePolicy::new(Duration::from_secs(30)),
        now(),
    );
    let client = FakeRouteClient::default();
    client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("waking"),
        identity.clone(),
        route_entry(InstanceState::Waking, 8, None),
    ));
    client.push_subscribe_response(resolved_response(
        generated_request_id(2),
        subscription_id("ready"),
        identity,
        route_entry(
            InstanceState::Running,
            8,
            Some((format!("http://{upstream}"), 3)),
        ),
    ));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let runtime = FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(state, client),
            WakeTracker::new(),
            AcceptedWakeClient(calls.clone()),
        ),
        DrainTracker::new(Duration::from_secs(5)),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        listener_request(addr, "app.example.com", "/cold"),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(READY_RESPONSE)
    );
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "Waking is observed instead of repeatedly dispatching wake"
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn listener_records_one_cache_miss_then_one_hit_per_request() {
    use proxy_core::observability::{
        metrics::RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME,
        recorder::{InMemoryObservability, ObservabilityEvent, EVENT_ROUTE_CACHE_LOOKUP},
    };
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        request_id("req:1"),
        subscription_id("observed"),
        http_identity("app.example.com", "/"),
        route_entry(
            InstanceState::Running,
            7,
            Some((format!("http://{upstream_addr}"), 3)),
        ),
    ));
    let sink = InMemoryObservability::default();
    let runtime = FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::with_observability(
            FrontlineRouteResolver::new(4, route_client.clone()),
            WakeTracker::new(),
            FakeWakeClient::default(),
            sink.recorder(),
        ),
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
    let events = sink.events();
    let outcomes: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ObservabilityEvent::Metric(metric)
                if metric.name() == RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME =>
            {
                assert_eq!(metric.value(), 1.0);
                Some(metric.labels()[0].value())
            }
            _ => None,
        })
        .collect();
    assert_eq!(outcomes, ["miss", "hit"]);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event, ObservabilityEvent::Log(log) if log.name() == EVENT_ROUTE_CACHE_LOOKUP,
            ))
            .count(),
        2
    );
    assert_eq!(route_client.calls().len(), 1);
    shutdown.shutdown();
    task.await.unwrap().unwrap();
    upstream_task.await.unwrap();
}

mod protocol;

mod admission;
