use std::{convert::Infallible, error::Error, fmt, io, net::SocketAddr, time::Instant};

use bytes::Bytes;
use http::StatusCode;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use proxy_core::{websocket_upgrade_response, DrainError, Shutdown};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::Mutex,
    task::{JoinError, JoinSet},
};

use crate::{
    is_http01_challenge_candidate_path,
    runtime::{
        record_http01_error, record_http01_response, resolve_http01_response,
        resolve_http_route_shared, route_outcome_or_forward_response,
    },
    FrontlineForwardContext, FrontlineForwarder, FrontlineHttpRuntime, FrontlineRouteOutcome,
    FrontlineTlsAdapter, Http01ChallengeResolver, RouteSubscriptionClient,
    SharedFrontlineRouteCoordinator, WakeClient,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineHttpListenerConfig {
    listen_addr: SocketAddr,
    resources: proxy_core::ProxyResourceConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineTlsTerminationListenerConfig {
    listen_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineTlsPassthroughListenerConfig {
    listen_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineListenersConfig {
    http: FrontlineHttpListenerConfig,
    tls_termination: Option<FrontlineTlsTerminationListenerConfig>,
    tls_passthrough: Option<FrontlineTlsPassthroughListenerConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontlineListenerKind {
    Http,
    TlsTermination,
    TlsPassthrough,
}

#[derive(Debug)]
pub enum FrontlineHttpListenerError {
    InitialActivationBudgetExceeded,
    Bind { addr: SocketAddr, source: io::Error },
    Accept(io::Error),
    Drain(DrainError),
}

#[derive(Debug)]
pub enum FrontlineListenerError {
    InitialActivationBudgetExceeded,
    Bind {
        kind: FrontlineListenerKind,
        addr: SocketAddr,
        source: io::Error,
    },
    Accept {
        kind: FrontlineListenerKind,
        source: io::Error,
    },
    Drain(DrainError),
    Task(JoinError),
}

struct SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    coordinator: SharedFrontlineRouteCoordinator<RouteClient, Wake>,
    http01_resolver: std::sync::Arc<Mutex<Http01>>,
    forwarder: FrontlineForwarder,
    drain: proxy_core::DrainTracker,
    websocket_tasks: std::sync::Arc<Mutex<JoinSet<()>>>,
    observability: proxy_core::observability::recorder::ObservabilityRecorder,
    admission: proxy_core::ProxyAdmission,
}

impl FrontlineHttpListenerConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            resources: proxy_core::ProxyResourceConfig::default(),
        }
    }

    pub fn with_resource_config(mut self, config: proxy_core::ProxyResourceConfig) -> Self {
        self.resources = config;
        self
    }

    pub fn resource_config(&self) -> proxy_core::ProxyResourceConfig {
        self.resources
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl FrontlineTlsTerminationListenerConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl FrontlineTlsPassthroughListenerConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl FrontlineListenersConfig {
    pub fn new(http: FrontlineHttpListenerConfig) -> Self {
        Self {
            http,
            tls_termination: None,
            tls_passthrough: None,
        }
    }

    pub fn with_tls_termination(
        mut self,
        tls_termination: Option<FrontlineTlsTerminationListenerConfig>,
    ) -> Self {
        self.tls_termination = tls_termination;
        self
    }

    pub fn with_tls_passthrough(
        mut self,
        tls_passthrough: Option<FrontlineTlsPassthroughListenerConfig>,
    ) -> Self {
        self.tls_passthrough = tls_passthrough;
        self
    }

    pub fn http(&self) -> FrontlineHttpListenerConfig {
        self.http
    }

    pub fn tls_termination(&self) -> Option<FrontlineTlsTerminationListenerConfig> {
        self.tls_termination
    }

    pub fn tls_passthrough(&self) -> Option<FrontlineTlsPassthroughListenerConfig> {
        self.tls_passthrough
    }
}

pub async fn serve_http<RouteClient, Wake, Http01>(
    config: FrontlineHttpListenerConfig,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    if !crate::config::initial_activation_budget_valid(
        runtime.coordinator().route_deadline(),
        config.resources,
    ) {
        return Err(FrontlineHttpListenerError::InitialActivationBudgetExceeded);
    }
    let listener = TcpListener::bind(config.listen_addr())
        .await
        .map_err(|source| FrontlineHttpListenerError::Bind {
            addr: config.listen_addr(),
            source,
        })?;

    serve_http_listener_with_admission(
        listener,
        runtime,
        shutdown,
        proxy_core::ProxyAdmission::new(config.resources),
    )
    .await
}

pub async fn serve_frontline<RouteClient, Wake, Http01>(
    config: FrontlineListenersConfig,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    tls_adapter: FrontlineTlsAdapter,
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
    if !crate::config::initial_activation_budget_valid(
        runtime.coordinator().route_deadline(),
        config.http.resources,
    ) {
        return Err(FrontlineListenerError::InitialActivationBudgetExceeded);
    }
    let tls_adapter = tls_adapter.with_resource_config(config.http.resources);
    let shared = SharedFrontlineHttpRuntime::from_runtime(
        runtime,
        proxy_core::ProxyAdmission::new(config.http.resources),
    );
    let mut listeners = JoinSet::new();

    listeners.spawn({
        let shared = shared.clone();
        let shutdown = shutdown.clone();
        async move {
            let listener =
                bind_listener(FrontlineListenerKind::Http, config.http.listen_addr()).await?;
            serve_http_listener_with_shared(listener, shared, shutdown)
                .await
                .map_err(FrontlineListenerError::from)
        }
    });

    if let Some(tls_termination) = config.tls_termination {
        listeners.spawn({
            let shared = shared.clone();
            let tls_adapter = tls_adapter.clone();
            let shutdown = shutdown.clone();
            async move {
                let listener = bind_listener(
                    FrontlineListenerKind::TlsTermination,
                    tls_termination.listen_addr(),
                )
                .await?;
                serve_tls_termination_listener_with_shared(listener, shared, tls_adapter, shutdown)
                    .await
            }
        });
    }

    if let Some(tls_passthrough) = config.tls_passthrough {
        listeners.spawn({
            let shared = shared.clone();
            let tls_adapter = tls_adapter.clone();
            let shutdown = shutdown.clone();
            async move {
                let listener = bind_listener(
                    FrontlineListenerKind::TlsPassthrough,
                    tls_passthrough.listen_addr(),
                )
                .await?;
                serve_tls_passthrough_listener_with_shared(listener, shared, tls_adapter, shutdown)
                    .await
            }
        });
    }

    let mut first_error = None;
    while let Some(result) = listeners.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                shutdown.shutdown();
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                shutdown.shutdown();
                if first_error.is_none() {
                    first_error = Some(FrontlineListenerError::Task(error));
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub async fn serve_http_listener<RouteClient, Wake, Http01>(
    listener: TcpListener,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    serve_http_listener_with_admission(
        listener,
        runtime,
        shutdown,
        proxy_core::ProxyAdmission::new(proxy_core::ProxyResourceConfig::default()),
    )
    .await
}

pub async fn serve_http_listener_with_admission<RouteClient, Wake, Http01>(
    listener: TcpListener,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
    admission: proxy_core::ProxyAdmission,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    if !crate::config::initial_activation_budget_valid(
        runtime.coordinator().route_deadline(),
        admission.config(),
    ) {
        return Err(FrontlineHttpListenerError::InitialActivationBudgetExceeded);
    }
    let shared = SharedFrontlineHttpRuntime::from_runtime(runtime, admission);
    serve_http_listener_with_shared(listener, shared, shutdown).await
}

async fn bind_listener(
    kind: FrontlineListenerKind,
    addr: SocketAddr,
) -> Result<TcpListener, FrontlineListenerError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| FrontlineListenerError::Bind { kind, addr, source })
}

async fn serve_http_listener_with_shared<RouteClient, Wake, Http01>(
    listener: TcpListener,
    shared: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        reap_completed_tasks(&mut connections);
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(FrontlineHttpListenerError::Accept(error));
                        break;
                    }
                };
                let _ = stream.set_nodelay(true);
                let stream = match shared.admission.admit_io(stream) { Ok(stream) => stream, Err(_) => continue };
                let handshake = match shared.admission.handshakes.try_acquire() { Ok(permit) => permit, Err(_) => continue };
                let runtime = shared.clone();
                let connection_shutdown = shutdown.clone();
                let forwarding_context = FrontlineForwardContext::http(peer_addr.ip());

                connections.spawn(async move {
                    serve_http_connection(stream, runtime, connection_shutdown, forwarding_context, handshake)
                        .await;
                });
            }
        }
    }

    let drain_result = shared.drain.drain().await;
    shared.abort_websocket_tasks().await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(FrontlineHttpListenerError::Drain)
}

impl<RouteClient, Wake, Http01> Clone for SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    fn clone(&self) -> Self {
        Self {
            coordinator: self.coordinator.clone(),
            http01_resolver: self.http01_resolver.clone(),
            forwarder: self.forwarder.clone(),
            drain: self.drain.clone(),
            websocket_tasks: self.websocket_tasks.clone(),
            observability: self.observability.clone(),
            admission: self.admission.clone(),
        }
    }
}

async fn serve_tls_termination_listener_with_shared<RouteClient, Wake, Http01>(
    listener: TcpListener,
    shared: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    tls_adapter: FrontlineTlsAdapter,
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
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        reap_completed_tasks(&mut connections);
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(FrontlineListenerError::Accept {
                            kind: FrontlineListenerKind::TlsTermination,
                            source: error,
                        });
                        break;
                    }
                };
                let _ = stream.set_nodelay(true);
                let stream = match shared.admission.admit_io(stream) { Ok(stream) => stream, Err(_) => continue };
                let handshake = match shared.admission.handshakes.try_acquire() { Ok(permit) => permit, Err(_) => continue };
                let runtime = shared.clone();
                let tls_adapter = tls_adapter.clone();
                let connection_shutdown = shutdown.clone();
                let forwarding_context = FrontlineForwardContext::https(peer_addr.ip());

                connections.spawn(async move {
                    let terminated = tokio::select! {
                        _ = connection_shutdown.cancelled() => return,
                        result = tokio::time::timeout(runtime.admission.config().setup_timeout(), tls_adapter.terminate(stream)) => match result {
                            Ok(Ok(terminated)) => terminated, _ => return,
                        }
                    };

                    serve_http_connection(
                        terminated.stream,
                        runtime,
                        connection_shutdown,
                        forwarding_context,
                        handshake,
                    )
                    .await;
                });
            }
        }
    }

    let drain_result = shared.drain.drain().await;
    shared.abort_websocket_tasks().await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(FrontlineListenerError::Drain)
}

async fn serve_tls_passthrough_listener_with_shared<RouteClient, Wake, Http01>(
    listener: TcpListener,
    shared: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    tls_adapter: FrontlineTlsAdapter,
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
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        reap_completed_tasks(&mut connections);
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(FrontlineListenerError::Accept {
                            kind: FrontlineListenerKind::TlsPassthrough,
                            source: error,
                        });
                        break;
                    }
                };
                let _ = stream.set_nodelay(true);
                let stream = match shared.admission.admit_io(stream) { Ok(stream) => stream, Err(_) => continue };
                let handshake = match shared.admission.handshakes.try_acquire() { Ok(permit) => permit, Err(_) => continue };
                let runtime = shared.clone();
                let tls_adapter = tls_adapter.clone();

                connections.spawn(async move {
                    runtime
                        .handle_tls_passthrough_connection(stream, tls_adapter, handshake)
                        .await;
                });
            }
        }
    }

    let drain_result = shared.drain.drain().await;
    shared.abort_websocket_tasks().await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(FrontlineListenerError::Drain)
}

async fn serve_http_connection<RouteClient, Wake, Http01, IO>(
    stream: IO,
    runtime: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
    forwarding_context: FrontlineForwardContext,
    handshake: proxy_core::AdmissionPermit,
) where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    proxy_core::serve_http_connection_admitted(
        stream,
        shutdown,
        runtime.admission.clone(),
        handshake,
        None,
        move |request| {
            let runtime = runtime.clone();
            async move { Ok::<_, Infallible>(runtime.handle(request, forwarding_context).await) }
        },
    )
    .await;
}

impl<RouteClient, Wake, Http01> SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + 'static,
    Http01: Http01ChallengeResolver + Send,
    Http01::Error: Send,
{
    fn from_runtime(
        runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
        admission: proxy_core::ProxyAdmission,
    ) -> Self {
        let (coordinator, http01_resolver, forwarder, drain, observability) = runtime.into_parts();

        Self {
            coordinator: coordinator.into_shared(),
            http01_resolver: std::sync::Arc::new(Mutex::new(http01_resolver)),
            forwarder: forwarder.with_resource_config(admission.config()),
            admission,
            drain,
            websocket_tasks: std::sync::Arc::new(Mutex::new(JoinSet::new())),
            observability,
        }
    }

    async fn abort_websocket_tasks(&self) {
        let mut websocket_tasks = self.websocket_tasks.lock().await;
        websocket_tasks.abort_all();
        while websocket_tasks.join_next().await.is_some() {}
    }

    async fn handle(
        &self,
        request: http::Request<Incoming>,
        forwarding_context: FrontlineForwardContext,
    ) -> http::Response<crate::FrontlineRuntimeBody> {
        if is_http01_challenge_candidate_path(request.uri().path()) {
            let http01_response = {
                let mut resolver = self.http01_resolver.lock().await;
                resolve_http01_response(&mut *resolver, &request).await
            };
            match http01_response {
                Ok(Some(response)) => {
                    record_http01_response(&self.observability, response.status());
                    return response;
                }
                Ok(None) => {}
                Err(error) => {
                    record_http01_error(&self.observability, &error);
                    return crate::runtime::http01_intercept_error_response(error);
                }
            }
        }

        if proxy_core::is_websocket_upgrade(&request) {
            return self.handle_websocket(request, forwarding_context).await;
        }

        let outcome = resolve_http_route_shared(&self.coordinator, &request, Instant::now()).await;

        match outcome {
            Ok(outcome) => {
                route_outcome_or_forward_response(
                    &self.forwarder,
                    outcome,
                    request,
                    Some(forwarding_context),
                )
                .await
            }
            Err(error) => crate::runtime::route_resolution_error_response(error),
        }
    }

    async fn handle_tls_passthrough_connection<IO: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut stream: IO,
        tls_adapter: FrontlineTlsAdapter,
        handshake: proxy_core::AdmissionPermit,
    ) {
        let client_hello = match tokio::time::timeout(
            self.admission.config().setup_timeout(),
            tls_adapter.read_passthrough_client_hello(&mut stream),
        )
        .await
        {
            Ok(Ok(client_hello)) => client_hello,
            _ => return,
        };
        drop(handshake);
        let _request = match self.admission.requests.try_acquire() {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let outcome = self
            .coordinator
            .route(
                client_hello.identity().clone().into_identity(),
                Instant::now(),
            )
            .await;
        let ready = match outcome {
            Ok(FrontlineRouteOutcome::Ready(ready)) => ready,
            Ok(_) | Err(_) => return,
        };
        let _permit = match self.drain.try_acquire() {
            Ok(permit) => permit,
            Err(_error) => return,
        };

        // TLS passthrough forwards encrypted bytes after ClientHello routing; HTTP
        // headers are opaque here and cannot be mutated.
        let _ = tls_adapter
            .passthrough_prefixed(&ready, stream, client_hello)
            .await;
    }

    async fn handle_websocket(
        &self,
        mut request: http::Request<Incoming>,
        forwarding_context: FrontlineForwardContext,
    ) -> http::Response<crate::FrontlineRuntimeBody> {
        match websocket_upgrade_response(&request, ()) {
            Ok(_) => {}
            Err(_error) => return status_response(StatusCode::BAD_REQUEST),
        };

        let outcome = resolve_http_route_shared(&self.coordinator, &request, Instant::now()).await;

        match outcome {
            Ok(FrontlineRouteOutcome::Ready(ready)) => {
                let (response, upgrade) = match self
                    .forwarder
                    .prepare_websocket_upgrade(&ready, &mut request, forwarding_context)
                    .await
                {
                    Ok(upgrade) => upgrade,
                    Err(crate::FrontlineForwardError::WebSocket(error)) => {
                        return proxy_core::websocket_error_response(&error)
                            .map(|body| body.map_err(|error| match error {}).boxed_unsync())
                    }
                    Err(_) => return status_response(StatusCode::BAD_GATEWAY),
                };
                let mut websocket_tasks = self.websocket_tasks.lock().await;
                reap_completed_tasks(&mut websocket_tasks);
                websocket_tasks.spawn(async move {
                    let _ = upgrade.run().await;
                });
                response.map(|body| body.map_err(|error| match error {}).boxed_unsync())
            }
            Ok(outcome) => crate::runtime::route_outcome_response(outcome),
            Err(error) => crate::runtime::route_resolution_error_response(error),
        }
    }
}

fn reap_completed_tasks(tasks: &mut JoinSet<()>) {
    while tasks.try_join_next().is_some() {}
}

fn status_response(status: StatusCode) -> http::Response<crate::FrontlineRuntimeBody> {
    http::Response::builder()
        .status(status)
        .body(empty_body())
        .expect("status-only frontline listener response builds")
}

fn empty_body() -> crate::FrontlineRuntimeBody {
    Full::new(Bytes::new())
        .map_err(|error| match error {})
        .boxed_unsync()
}

impl fmt::Display for FrontlineHttpListenerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitialActivationBudgetExceeded => {
                f.write_str("route and initial setup exceed the 190-second activation budget")
            }
            Self::Bind { addr, source } => {
                write!(
                    f,
                    "failed to bind frontline HTTP listener on {addr}: {source}"
                )
            }
            Self::Accept(error) => write!(f, "failed to accept frontline HTTP connection: {error}"),
            Self::Drain(error) => write!(f, "frontline HTTP listener drain failed: {error}"),
        }
    }
}

impl From<FrontlineHttpListenerError> for FrontlineListenerError {
    fn from(error: FrontlineHttpListenerError) -> Self {
        match error {
            FrontlineHttpListenerError::InitialActivationBudgetExceeded => {
                Self::InitialActivationBudgetExceeded
            }
            FrontlineHttpListenerError::Bind { addr, source } => Self::Bind {
                kind: FrontlineListenerKind::Http,
                addr,
                source,
            },
            FrontlineHttpListenerError::Accept(source) => Self::Accept {
                kind: FrontlineListenerKind::Http,
                source,
            },
            FrontlineHttpListenerError::Drain(error) => Self::Drain(error),
        }
    }
}

impl fmt::Display for FrontlineListenerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http => f.write_str("HTTP"),
            Self::TlsTermination => f.write_str("TLS termination"),
            Self::TlsPassthrough => f.write_str("TLS passthrough"),
        }
    }
}

impl fmt::Display for FrontlineListenerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitialActivationBudgetExceeded => {
                f.write_str("route and initial setup exceed the 190-second activation budget")
            }
            Self::Bind { kind, addr, source } => {
                write!(
                    f,
                    "failed to bind frontline {kind} listener on {addr}: {source}"
                )
            }
            Self::Accept { kind, source } => {
                write!(f, "failed to accept frontline {kind} connection: {source}")
            }
            Self::Drain(error) => write!(f, "frontline listener drain failed: {error}"),
            Self::Task(error) => write!(f, "frontline listener task failed: {error}"),
        }
    }
}

impl Error for FrontlineHttpListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InitialActivationBudgetExceeded => None,
            Self::Bind { source, .. } => Some(source),
            Self::Accept(error) => Some(error),
            Self::Drain(error) => Some(error),
        }
    }
}

impl Error for FrontlineListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InitialActivationBudgetExceeded => None,
            Self::Bind { source, .. } | Self::Accept { source, .. } => Some(source),
            Self::Drain(error) => Some(error),
            Self::Task(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
