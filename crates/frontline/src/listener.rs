use std::{convert::Infallible, error::Error, fmt, io, net::SocketAddr, sync::Arc, time::Instant};

use bytes::Bytes;
use http::StatusCode;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use proxy_core::{serve_listener, Accept, DrainError, Shutdown};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinSet,
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

/// The listening sockets of one frontline: plain HTTP, and optionally TLS
/// termination and TLS passthrough.
#[derive(Debug)]
pub struct FrontlineListeners<L = TcpListener> {
    pub http: L,
    pub tls_termination: Option<L>,
    pub tls_passthrough: Option<L>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontlineListenerKind {
    Http,
    TlsTermination,
    TlsPassthrough,
}

#[derive(Debug)]
pub enum FrontlineListenerError {
    InitialActivationBudgetExceeded,
    Bind {
        kind: FrontlineListenerKind,
        addr: SocketAddr,
        source: io::Error,
    },
    Drain(DrainError),
}

/// State every connection of one frontline shares.
struct SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    coordinator: SharedFrontlineRouteCoordinator<RouteClient, Wake>,
    http01_resolver: Mutex<Http01>,
    forwarder: FrontlineForwarder,
    tls_adapter: FrontlineTlsAdapter,
    drain: proxy_core::DrainTracker,
    websocket_tasks: Mutex<JoinSet<()>>,
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

impl FrontlineListeners {
    /// Binds every configured address before any listener serves.
    pub async fn bind(config: &FrontlineListenersConfig) -> Result<Self, FrontlineListenerError> {
        async fn bind(
            kind: FrontlineListenerKind,
            addr: SocketAddr,
        ) -> Result<TcpListener, FrontlineListenerError> {
            TcpListener::bind(addr)
                .await
                .map_err(|source| FrontlineListenerError::Bind { kind, addr, source })
        }

        Ok(Self {
            http: bind(FrontlineListenerKind::Http, config.http.listen_addr).await?,
            tls_termination: match config.tls_termination {
                Some(tls) => {
                    Some(bind(FrontlineListenerKind::TlsTermination, tls.listen_addr).await?)
                }
                None => None,
            },
            tls_passthrough: match config.tls_passthrough {
                Some(tls) => {
                    Some(bind(FrontlineListenerKind::TlsPassthrough, tls.listen_addr).await?)
                }
                None => None,
            },
        })
    }
}

impl<L> FrontlineListeners<L> {
    /// A frontline that serves plain HTTP only.
    pub fn http(http: L) -> Self {
        Self {
            http,
            tls_termination: None,
            tls_passthrough: None,
        }
    }
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
    let listeners = FrontlineListeners::bind(&config).await?;
    let admission = proxy_core::ProxyAdmission::new(config.http.resources);
    serve_frontline_listeners(listeners, admission, runtime, tls_adapter, shutdown).await
}

/// Serves all listeners with one shared admission state until shutdown, then
/// closes the listening sockets and gives active work the drain grace period.
pub async fn serve_frontline_listeners<L, RouteClient, Wake, Http01>(
    listeners: FrontlineListeners<L>,
    admission: proxy_core::ProxyAdmission,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    tls_adapter: FrontlineTlsAdapter,
    shutdown: Shutdown,
) -> Result<(), FrontlineListenerError>
where
    L: Accept,
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let resources = admission.config();
    if !crate::config::initial_activation_budget_valid(
        runtime.coordinator().route_deadline(),
        resources,
    ) {
        return Err(FrontlineListenerError::InitialActivationBudgetExceeded);
    }
    let shared = Arc::new(SharedFrontlineHttpRuntime::from_runtime(
        runtime,
        admission,
        tls_adapter.with_resource_config(resources),
    ));

    let http = serve_listener(listeners.http, &shutdown, |stream, peer| {
        let (stream, handshake) = shared.admit(stream)?;
        let context = FrontlineForwardContext::http(peer.ip());
        Some(serve_http_connection(
            stream,
            shared.clone(),
            shutdown.clone(),
            context,
            handshake,
        ))
    });
    let tls_termination = serve_optional(listeners.tls_termination, &shutdown, |stream, peer| {
        let (stream, handshake) = shared.admit(stream)?;
        let (shared, shutdown) = (shared.clone(), shutdown.clone());
        let context = FrontlineForwardContext::https(peer.ip());
        Some(async move {
            let setup = shared.admission.config().setup_timeout();
            let terminated = tokio::select! {
                _ = shutdown.cancelled() => return,
                result = tokio::time::timeout(setup, shared.tls_adapter.terminate(stream)) => match result {
                    Ok(Ok(terminated)) => terminated,
                    _ => return,
                }
            };
            serve_http_connection(terminated.stream, shared, shutdown, context, handshake).await;
        })
    });
    let tls_passthrough = serve_optional(listeners.tls_passthrough, &shutdown, |stream, _| {
        let (stream, handshake) = shared.admit(stream)?;
        let shared = shared.clone();
        Some(async move {
            shared
                .handle_tls_passthrough_connection(stream, handshake)
                .await;
        })
    });
    let connections = tokio::join!(http, tls_termination, tls_passthrough);

    let drained = shared.drain.drain().await;
    let mut websocket_tasks = shared.websocket_tasks.lock().await;
    websocket_tasks.abort_all();
    while websocket_tasks.join_next().await.is_some() {}
    for mut connections in [connections.0, connections.1, connections.2] {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }

    drained.map_err(FrontlineListenerError::Drain)
}

async fn serve_optional<L, F, Fut>(
    listener: Option<L>,
    shutdown: &Shutdown,
    connection: F,
) -> JoinSet<()>
where
    L: Accept,
    F: FnMut(TcpStream, SocketAddr) -> Option<Fut>,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    match listener {
        Some(listener) => serve_listener(listener, shutdown, connection).await,
        None => JoinSet::new(),
    }
}

async fn serve_http_connection<RouteClient, Wake, Http01, IO>(
    stream: IO,
    runtime: Arc<SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>>,
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
        tls_adapter: FrontlineTlsAdapter,
    ) -> Self {
        let (coordinator, http01_resolver, forwarder, drain, observability) = runtime.into_parts();

        Self {
            coordinator: coordinator.into_shared(),
            http01_resolver: Mutex::new(http01_resolver),
            forwarder: forwarder.with_resource_config(admission.config()),
            tls_adapter,
            admission,
            drain,
            websocket_tasks: Mutex::new(JoinSet::new()),
            observability,
        }
    }

    /// Takes a connection slot and a setup slot, or closes the socket.
    fn admit(
        &self,
        stream: TcpStream,
    ) -> Option<(
        proxy_core::AdmittedIo<TcpStream>,
        proxy_core::AdmissionPermit,
    )> {
        let stream = self.admission.admit_io(stream).ok()?;
        let handshake = self.admission.handshakes.try_acquire().ok()?;
        Some((stream, handshake))
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
        handshake: proxy_core::AdmissionPermit,
    ) {
        let client_hello = match tokio::time::timeout(
            self.admission.config().setup_timeout(),
            self.tls_adapter.read_passthrough_client_hello(&mut stream),
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
        let _ = self
            .tls_adapter
            .passthrough_prefixed(&ready, stream, client_hello)
            .await;
    }

    async fn handle_websocket(
        &self,
        mut request: http::Request<Incoming>,
        forwarding_context: FrontlineForwardContext,
    ) -> http::Response<crate::FrontlineRuntimeBody> {
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
                while websocket_tasks.try_join_next().is_some() {}
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
            Self::Drain(error) => write!(f, "frontline listener drain failed: {error}"),
        }
    }
}

impl Error for FrontlineListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InitialActivationBudgetExceeded => None,
            Self::Bind { source, .. } => Some(source),
            Self::Drain(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
