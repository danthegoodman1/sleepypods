//! Frontline tries a ready backend's Pod address first, and only a sidecar of
//! the route's own instance serves there.
use super::*;
use proxy_core::pod_route::POD_ATTEMPT_TIMEOUT;
use tokio::time::Instant;

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

type Seen = Arc<Mutex<Vec<String>>>;

/// An app that answers with its name and records every request it receives.
struct App {
    name: &'static str,
    seen: Seen,
    task: JoinHandle<()>,
}

impl App {
    async fn start(name: &'static str) -> (Self, SocketAddr) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Seen::default();
        let recorded = seen.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                while connections.try_join_next().is_some() {}
                let recorded = recorded.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request| {
                        let recorded = recorded.clone();
                        async move { Ok::<_, Infallible>(app_response(name, &recorded, request)) }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (Self { name, seen, task }, addr)
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for App {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn app_response(
    name: &'static str,
    seen: &Seen,
    mut request: Request<Incoming>,
) -> Response<Full<Bytes>> {
    use tokio_tungstenite::tungstenite::handshake::server::create_response_with_body;
    let host = request
        .headers()
        .get("host")
        .map(|host| host.to_str().unwrap().to_owned())
        .unwrap_or_default();
    seen.lock()
        .unwrap()
        .push(format!("{} {host}", request.uri().path()));
    if request.uri().path() != "/socket" {
        return Response::builder()
            .header("x-app", name)
            .header("x-seen-host", host)
            .body(Full::new(Bytes::from_static(name.as_bytes())))
            .unwrap();
    }
    let mut response = create_response_with_body(&request, || Full::new(Bytes::new())).unwrap();
    response
        .headers_mut()
        .insert("x-app", name.parse().unwrap());
    let upgraded = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        let mut socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            TokioIo::new(upgraded.await.unwrap()),
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        while let Some(Ok(message)) = socket.next().await {
            if message.is_close() {
                break;
            }
            let _ = socket.send(message).await;
        }
    });
    response
}

/// A sidecar of `instance` in front of the app at `app`.
async fn sidecar_for(
    instance: &str,
    app: SocketAddr,
    shutdown: &Shutdown,
) -> (
    SocketAddr,
    JoinHandle<Result<(), sidecar::runtime::SidecarRuntimeError>>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = sidecar::runtime::SidecarRuntimeConfig::new(
        addr,
        app.port(),
        instance_id(instance),
        Generation::new(7),
        sidecar::IdleReportConfig::new(
            Duration::from_secs(3600),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .unwrap(),
        Duration::from_secs(2),
    )
    .unwrap();
    let task = tokio::spawn(sidecar::runtime::serve_http_listener_with_idle(
        listener,
        config,
        NoIdle,
        shutdown.clone(),
    ));
    (addr, task)
}

/// A route to `instance` whose Service is `service` and whose Pod address is
/// `pod`.
fn pod_route_entry(instance: &str, service: SocketAddr, pod: SocketAddr) -> RouteEntry {
    RouteEntry {
        route_binding_id: route_binding_id(&format!("route-{instance}")),
        instance_id: instance_id(instance),
        instance_state: InstanceState::Running,
        instance_generation: Generation::new(7),
        backend: Some(backend(format!("http://{service}"))),
        pod_backend: Some(backend(format!("http://{pod}"))),
        backend_generation: Some(BackendGeneration::new(3)),
    }
}

/// An address whose SYNs are dropped, as a Service's is before kube-proxy has
/// programmed it: a listener whose accept queue one connection fills.
#[cfg(target_os = "linux")]
async fn unroutable() -> (SocketAddr, TcpListener, TcpStream) {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(0).unwrap();
    let addr = listener.local_addr().unwrap();
    let filler = TcpStream::connect(addr).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), TcpStream::connect(addr))
            .await
            .is_err(),
        "the full accept queue drops SYNs"
    );
    (addr, listener, filler)
}

/// An address that refuses connections.
async fn refusing() -> SocketAddr {
    TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}

/// A listener that accepts and never answers, as a server that waits for its
/// client to speak does.
async fn silent() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            held.push(listener.accept().await.unwrap());
        }
    });
    (addr, task)
}

struct Edge {
    addr: SocketAddr,
    shutdown: Shutdown,
    task: JoinHandle<Result<(), FrontlineListenerError>>,
}

impl Edge {
    async fn start(routes: &[(&str, RouteEntry)], shutdown: &Shutdown) -> Self {
        let route_client = FakeControlPlane::default();
        for (host, entry) in routes {
            seed(&route_client, http_identity(host, "/"), entry.clone());
        }
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let runtime = runtime_with_state(route_client, DrainTracker::new(Duration::from_secs(2)));
        let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));
        Self {
            addr,
            shutdown: shutdown.clone(),
            task,
        }
    }

    async fn stop(self) {
        self.shutdown.shutdown();
        self.task.await.unwrap().unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum Client {
    Http1,
    Http2,
    WebSocket,
}

const CLIENTS: [Client; 3] = [Client::Http1, Client::Http2, Client::WebSocket];

/// Sends one request for `host` in `client`'s protocol and returns the app
/// that served it.
async fn request(edge: SocketAddr, client: Client, host: &str, path: &str) -> String {
    let stream = TcpStream::connect(edge).await.unwrap();
    match client {
        Client::Http1 => {
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await
                    .unwrap();
            tokio::spawn(connection);
            let request = Request::builder()
                .uri(path)
                .header("host", host)
                .body(Full::new(Bytes::new()))
                .unwrap();
            served_by(sender.send_request(request).await.unwrap(), host)
        }
        Client::Http2 => {
            let (mut sender, connection) =
                client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                    .await
                    .unwrap();
            tokio::spawn(connection);
            let request = Request::builder()
                .uri(format!("http://{host}{path}"))
                .body(Full::new(Bytes::new()))
                .unwrap();
            served_by(sender.send_request(request).await.unwrap(), host)
        }
        Client::WebSocket => {
            let mut request = format!("ws://{edge}/socket").into_client_request().unwrap();
            request.headers_mut().insert("host", host.parse().unwrap());
            let (mut socket, response) = tokio_tungstenite::client_async(request, stream)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
            socket.send(Message::Text("echo".into())).await.unwrap();
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Text("echo".into())
            );
            socket.close(None).await.unwrap();
            response.headers()["x-app"].to_str().unwrap().to_owned()
        }
    }
}

fn served_by(response: Response<Incoming>, host: &str) -> String {
    assert_eq!(response.status(), StatusCode::OK);
    // The app sees the client's host; the Pod route the hop carries never
    // reaches it.
    assert_eq!(response.headers()["x-seen-host"], host);
    response.headers()["x-app"].to_str().unwrap().to_owned()
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_ready_pod_address_serves_before_its_service_is_routable() {
    let shutdown = Shutdown::new();
    let (app, app_addr) = App::start("app-a").await;
    let (sidecar, sidecar_task) = sidecar_for("instance-a", app_addr, &shutdown).await;
    let (service, _service, _filler) = unroutable().await;
    let edge = Edge::start(
        &[(
            "a.example.test",
            pod_route_entry("instance-a", service, sidecar),
        )],
        &shutdown,
    )
    .await;

    for client in CLIENTS {
        let started = Instant::now();
        assert_eq!(
            request(edge.addr, client, "a.example.test", "/first").await,
            "app-a",
            "{client:?}"
        );
        let elapsed = started.elapsed();
        assert!(elapsed < POD_ATTEMPT_TIMEOUT, "{client:?}: {elapsed:?}");
    }
    assert_eq!(app.seen().len(), CLIENTS.len());

    edge.stop().await;
    sidecar_task.await.unwrap().unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_refusing_unroutable_or_silent_pod_address_falls_back_to_the_service() {
    let shutdown = Shutdown::new();
    let (app, app_addr) = App::start("app-a").await;
    let (sidecar, sidecar_task) = sidecar_for("instance-a", app_addr, &shutdown).await;
    let (unroutable, _unroutable, _filler) = unroutable().await;
    let (silent, silent_task) = silent().await;
    let pods = [
        ("refusing", refusing().await, Duration::ZERO),
        ("unroutable", unroutable, POD_ATTEMPT_TIMEOUT),
        ("silent", silent, POD_ATTEMPT_TIMEOUT),
    ];

    for (name, pod, attempt) in pods {
        for client in CLIENTS {
            // A fresh frontline pays the Pod attempt once.
            let host = format!("{name}-{client:?}.example.test").to_lowercase();
            let edge = Edge::start(
                &[(&host, pod_route_entry("instance-a", sidecar, pod))],
                &shutdown,
            )
            .await;
            let started = Instant::now();
            assert_eq!(
                request(edge.addr, client, &host, "/fallback").await,
                "app-a"
            );
            let elapsed = started.elapsed();
            assert!(elapsed >= attempt, "{name} {client:?}: {elapsed:?}");
            assert!(
                elapsed < attempt + Duration::from_millis(500),
                "{name} {client:?}: {elapsed:?}"
            );

            // Later connections go straight to the Service.
            let started = Instant::now();
            let concurrent = (0..4).map(|_| request(edge.addr, client, &host, "/again"));
            for served in futures_util::future::join_all(concurrent).await {
                assert_eq!(served, "app-a");
            }
            assert!(
                started.elapsed() < POD_ATTEMPT_TIMEOUT,
                "{name} {client:?}: {:?}",
                started.elapsed()
            );
            edge.task.abort();
        }
    }
    assert_eq!(app.seen().len(), pods.len() * CLIENTS.len() * 5);

    silent_task.abort();
    shutdown.shutdown();
    sidecar_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_sidecar_of_another_instance_at_the_pod_address_never_reaches_its_app() {
    let shutdown = Shutdown::new();
    let (own_app, own_app_addr) = App::start("app-a").await;
    let (own, own_task) = sidecar_for("instance-a", own_app_addr, &shutdown).await;
    let (other_app, other_app_addr) = App::start("app-b").await;
    let (other, other_task) = sidecar_for("instance-b", other_app_addr, &shutdown).await;
    let edge = Edge::start(
        &[("a.example.test", pod_route_entry("instance-a", own, other))],
        &shutdown,
    )
    .await;

    for client in CLIENTS {
        let concurrent = (0..4).map(|_| request(edge.addr, client, "a.example.test", "/mine"));
        for served in futures_util::future::join_all(concurrent).await {
            assert_eq!(served, "app-a", "{client:?}");
        }
    }

    assert_eq!(own_app.seen().len(), CLIENTS.len() * 4);
    assert_eq!(other_app.seen(), Vec::<String>::new(), "{}", other_app.name);
    edge.stop().await;
    own_task.await.unwrap().unwrap();
    other_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn instances_that_share_a_pod_address_never_share_a_connection() {
    let shutdown = Shutdown::new();
    let (app_a, app_a_addr) = App::start("app-a").await;
    let (sidecar_a, task_a) = sidecar_for("instance-a", app_a_addr, &shutdown).await;
    let (app_b, app_b_addr) = App::start("app-b").await;
    let (sidecar_b, task_b) = sidecar_for("instance-b", app_b_addr, &shutdown).await;
    // Instance B's route still names the Pod address instance A now holds.
    let edge = Edge::start(
        &[
            (
                "a.example.test",
                pod_route_entry("instance-a", sidecar_a, sidecar_a),
            ),
            (
                "b.example.test",
                pod_route_entry("instance-b", sidecar_b, sidecar_a),
            ),
        ],
        &shutdown,
    )
    .await;

    for round in 0..3 {
        for client in CLIENTS {
            for (host, app) in [("a.example.test", "app-a"), ("b.example.test", "app-b")] {
                assert_eq!(
                    request(edge.addr, client, host, "/shared").await,
                    app,
                    "round {round} {client:?} {host}"
                );
            }
        }
    }

    let hosts = |app: &App| {
        app.seen()
            .into_iter()
            .map(|seen| seen.split_once(' ').unwrap().1.to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(hosts(&app_a), vec!["a.example.test"; 3 * CLIENTS.len()]);
    assert_eq!(hosts(&app_b), vec!["b.example.test"; 3 * CLIENTS.len()]);
    edge.stop().await;
    task_a.await.unwrap().unwrap();
    task_b.await.unwrap().unwrap();
}

#[tokio::test]
async fn tls_passthrough_keeps_the_service_address() {
    let upstream = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let trap = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let trap_addr = trap.local_addr().unwrap();
    let hello = client_hello(Some(sni_extension(b"db.example.com")));
    let upstream_task = tokio::spawn({
        let hello = hello.clone();
        async move {
            let (mut stream, _) = upstream.accept().await.unwrap();
            let mut received = vec![0; hello.len()];
            stream.read_exact(&mut received).await.unwrap();
            assert_eq!(received, hello);
            stream.write_all(b"served").await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    let route_client = FakeControlPlane::default();
    let mut entry = pod_route_entry("instance-a", upstream_addr, trap_addr);
    entry.backend = Some(backend(format!("tcp://{upstream_addr}")));
    entry.pod_backend = Some(backend(format!("tcp://{trap_addr}")));
    seed(&route_client, sni_identity("db.example.com"), entry);
    let http_addr = reserve_addr().await;
    let passthrough_addr = reserve_addr().await;
    let shutdown = Shutdown::new();
    let config = FrontlineListenersConfig::new(FrontlineHttpListenerConfig::new(http_addr))
        .with_tls_passthrough(Some(FrontlineTlsPassthroughListenerConfig::new(
            passthrough_addr,
        )));
    let task = tokio::spawn(serve_frontline(
        config,
        runtime_with_state(route_client, DrainTracker::new(Duration::from_secs(5))),
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()),
        shutdown.clone(),
    ));

    let mut client = connect_tcp(passthrough_addr).await;
    client.write_all(&hello).await.unwrap();
    client.shutdown().await.unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();

    assert_eq!(response, b"served");
    upstream_task.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), trap.accept())
            .await
            .is_err(),
        "passthrough never dials the Pod address"
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}
