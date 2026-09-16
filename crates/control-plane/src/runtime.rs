use std::{
    collections::HashMap,
    error::Error,
    fmt,
    net::{AddrParseError, SocketAddr},
    sync::Arc,
};

use sleepypods_observability::{
    metrics::{
        EXCLUSIVITY_KEYS_HELD, MATERIALIZATIONS_NONTERMINAL,
        MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
    },
    prometheus::{serve_prometheus_metrics_with_collector, PrometheusMetricsSink},
    recorder::{
        CompositeObservabilitySink, MetricObservation, ObservabilityRecorder,
        StderrObservabilitySink,
    },
};
use tokio::{sync::watch, task::JoinSet};
use tonic::transport::server::Router;
use tower::layer::util::{Identity, Stack};
use tower_http::cors::CorsLayer;

use crate::{
    api::{
        operator_grpc_service_with_store_and_route_events,
        proxy_grpc_service_with_store_and_route_events,
        sidecar_grpc_service_with_store_and_route_events, RouteSubscriptionBroker,
    },
    auth::{AuthConfig, ControlPlaneAuth, InvalidStaticBearerTokens, StaticBearerTokens},
    config::{ControlPlaneConfig, PostgresStoreConfig, StoreProviderConfig, StoreProviderName},
    materialization::{InvalidMaterializationTarget, MaterializationState, MaterializationTarget},
    materializer::{
        KubernetesMaterializer, KubernetesMaterializerClient, RetryingKubernetesMaterializerClient,
    },
    postgres::PostgresStore,
    reconciler::{MaterializationReconciler, MaterializationReconcilerConfig},
    store::{ControlPlaneStore, RetryingControlPlaneStore},
    KubeMaterializerClient,
};

/// Carries the proxy and sidecar services, which in-cluster workloads reach.
pub const CONTROL_PLANE_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR";
/// Carries the operator service alone, on a listener a workload has no reason
/// to reach. Keeping the two apart lets a NetworkPolicy separate them.
pub const OPERATOR_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_OPERATOR_LISTEN_ADDR";
pub const OPERATOR_GRPC_WEB_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR";
pub const STORE_PROVIDER_ENV: &str = "SLEEPYPODS_STORE_PROVIDER";
pub const POSTGRES_URL_ENV: &str = "SLEEPYPODS_POSTGRES_URL";
pub const CLUSTER_ID_ENV: &str = "SLEEPYPODS_CLUSTER_ID";
pub const NAMESPACE_ENV: &str = "SLEEPYPODS_NAMESPACE";
pub const AUTH_MODE_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_AUTH_MODE";
pub const AUTH_OPERATOR_TOKEN_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN";
pub const AUTH_PROXY_TOKEN_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN";
pub const AUTH_SIDECAR_TOKEN_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN";
pub const METRICS_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR";

pub type NativeControlPlaneRouter =
    Router<Stack<crate::api::admission::RpcAdmissionLayer, Identity>>;
pub type OperatorGrpcWebLayers = Stack<
    tonic_web::GrpcWebLayer,
    Stack<crate::api::admission::RpcAdmissionLayer, Stack<CorsLayer, Identity>>,
>;
pub type OperatorGrpcWebRouter = Router<OperatorGrpcWebLayers>;
pub type RuntimeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub listen_addr: SocketAddr,
    pub operator_listen_addr: SocketAddr,
    pub operator_grpc_web_listen_addr: Option<SocketAddr>,
    pub metrics_listen_addr: Option<SocketAddr>,
    pub control_plane: ControlPlaneConfig,
    pub target: MaterializationTarget,
    pub api_limits: crate::api::admission::ApiLimits,
    pub security: crate::runtime_security::RuntimeSecurityConfig,
}

#[derive(Debug)]
pub enum RuntimeConfigError {
    InvalidSecurity(&'static str),
    MissingEnv {
        name: &'static str,
    },
    InvalidListenAddr {
        name: &'static str,
        value: String,
        source: AddrParseError,
    },
    InvalidStoreProvider {
        value: String,
    },
    InvalidPostgresConfig(crate::ids::EmptyStringError),
    InvalidPostgresSetting {
        name: &'static str,
    },
    InvalidMaterializationTarget(InvalidMaterializationTarget),
    InvalidAuthMode {
        value: String,
    },
    InvalidAuthConfig(InvalidStaticBearerTokens),
}

impl RuntimeConfig {
    pub fn from_env() -> Result<Self, RuntimeConfigError> {
        Self::from_key_values(std::env::vars())
    }

    pub fn from_key_values<I, K, V>(values: I) -> Result<Self, RuntimeConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values = values
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<HashMap<_, _>>();

        let listen_addr = parse_required_socket_addr(&values, CONTROL_PLANE_LISTEN_ADDR_ENV)?;
        let operator_listen_addr = parse_required_socket_addr(&values, OPERATOR_LISTEN_ADDR_ENV)?;
        let operator_grpc_web_listen_addr =
            parse_optional_socket_addr(&values, OPERATOR_GRPC_WEB_LISTEN_ADDR_ENV)?;
        let metrics_listen_addr = parse_optional_socket_addr(&values, METRICS_LISTEN_ADDR_ENV)?;
        let provider = required_value(&values, STORE_PROVIDER_ENV)?;
        let store = match provider.parse::<StoreProviderName>().map_err(|_| {
            RuntimeConfigError::InvalidStoreProvider {
                value: provider.to_owned(),
            }
        })? {
            StoreProviderName::Postgres => {
                let url = required_value(&values, POSTGRES_URL_ENV)?;
                let mut config = PostgresStoreConfig::new(url)
                    .map_err(RuntimeConfigError::InvalidPostgresConfig)?;
                if let Some(value) =
                    postgres_positive_integer(&values, "SLEEPYPODS_POSTGRES_MAX_CONNECTIONS")?
                {
                    config.max_connections = usize::try_from(value).map_err(|_| {
                        RuntimeConfigError::InvalidPostgresSetting {
                            name: "SLEEPYPODS_POSTGRES_MAX_CONNECTIONS",
                        }
                    })?;
                }
                for (name, setting) in [
                    (
                        "SLEEPYPODS_OPERATION_TIMEOUT_MS",
                        &mut config.operation_timeout,
                    ),
                    (
                        "SLEEPYPODS_POSTGRES_POOL_WAIT_TIMEOUT_MS",
                        &mut config.pool_wait_timeout,
                    ),
                    (
                        "SLEEPYPODS_POSTGRES_CONNECTION_TIMEOUT_MS",
                        &mut config.connection_timeout,
                    ),
                    (
                        "SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS",
                        &mut config.statement_timeout,
                    ),
                ] {
                    if let Some(value) = postgres_positive_integer(&values, name)? {
                        *setting = std::time::Duration::from_millis(value);
                    }
                }
                config.idempotency_retention = postgres_positive_integer(
                    &values,
                    "SLEEPYPODS_POSTGRES_IDEMPOTENCY_RETENTION_MS",
                )?
                .map(std::time::Duration::from_millis);
                config
                    .validate_limits()
                    .map_err(|name| RuntimeConfigError::InvalidPostgresSetting { name })?;
                StoreProviderConfig::Postgres(config)
            }
        };
        let auth = parse_auth_config(&values)?;
        let security = crate::runtime_security::RuntimeSecurityConfig::parse(
            &values,
            !matches!(auth, AuthConfig::NoAuth),
        )
        .map_err(RuntimeConfigError::InvalidSecurity)?;
        if security.sealing_keys_file.is_some() {
            let StoreProviderConfig::Postgres(pg) = &store;
            if pg.max_connections < 2 {
                return Err(RuntimeConfigError::InvalidSecurity(
                    "certificate delivery requires at least two PostgreSQL connections",
                ));
            }
        }
        let target = MaterializationTarget::new(
            required_value(&values, CLUSTER_ID_ENV)?,
            required_value(&values, NAMESPACE_ENV)?,
        )
        .map_err(RuntimeConfigError::InvalidMaterializationTarget)?;

        let mut api_limits = crate::api::admission::ApiLimits::default();
        for (name, setting, maximum) in [
            (
                "SLEEPYPODS_CONTROL_PLANE_MAX_CONNECTIONS",
                &mut api_limits.accepted_connections,
                4096,
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_MAX_RPCS",
                &mut api_limits.rpc_concurrency,
                4096,
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_MAX_SUBSCRIPTION_STREAMS",
                &mut api_limits.subscription_streams,
                1024,
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_MAX_SUBSCRIPTIONS_PER_STREAM",
                &mut api_limits.subscriptions_per_stream,
                4096,
            ),
        ] {
            if let Some(value) = postgres_positive_integer(&values, name)? {
                if value > maximum {
                    return Err(RuntimeConfigError::InvalidPostgresSetting { name });
                }
                *setting = value as usize;
            }
        }
        for (name, setting) in [
            (
                "SLEEPYPODS_CONTROL_PLANE_UNARY_DELIVERY_TIMEOUT_MS",
                &mut api_limits.unary_delivery_timeout,
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_SETUP_TIMEOUT_MS",
                &mut api_limits.setup_timeout,
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_WRITE_TIMEOUT_MS",
                &mut api_limits.write_timeout,
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_LOOKUP_TIMEOUT_MS",
                &mut api_limits.lookup_timeout,
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_RESPONSE_TIMEOUT_MS",
                &mut api_limits.response_timeout,
            ),
        ] {
            if let Some(value) = postgres_positive_integer(&values, name)? {
                if value > 60000 {
                    return Err(RuntimeConfigError::InvalidPostgresSetting { name });
                }
                *setting = std::time::Duration::from_millis(value);
            }
        }
        // A subscription stream lives across many requests and the proxy keeps
        // its route cache when one rotates, so the stream lifetime is allowed
        // past the 60s request-timeout cap the shared duration loop applies to
        // request-scoped settings. A longer lifetime rotates streams less often.
        if let Some(value) =
            postgres_positive_integer(&values, "SLEEPYPODS_CONTROL_PLANE_SUBSCRIPTION_LIFETIME_MS")?
        {
            if value > 600_000 {
                return Err(RuntimeConfigError::InvalidPostgresSetting {
                    name: "SLEEPYPODS_CONTROL_PLANE_SUBSCRIPTION_LIFETIME_MS",
                });
            }
            api_limits.subscription_lifetime = std::time::Duration::from_millis(value);
        }
        // The positive route cache TTL bounds how long a dropped invalidation
        // can go unnoticed, so it is allowed past the 60s request-timeout cap
        // the shared duration loop applies to request-scoped settings.
        if let Some(value) = postgres_positive_integer(
            &values,
            "SLEEPYPODS_CONTROL_PLANE_POSITIVE_ROUTE_CACHE_TTL_MS",
        )? {
            if value > 600_000 {
                return Err(RuntimeConfigError::InvalidPostgresSetting {
                    name: "SLEEPYPODS_CONTROL_PLANE_POSITIVE_ROUTE_CACHE_TTL_MS",
                });
            }
            api_limits.positive_route_cache_ttl = std::time::Duration::from_millis(value);
        }
        Ok(Self {
            security,
            api_limits,
            listen_addr,
            operator_listen_addr,
            operator_grpc_web_listen_addr,
            metrics_listen_addr,
            control_plane: ControlPlaneConfig::new(store, auth),
            target,
        })
    }
}

/// The listener in-cluster workloads reach, carrying the proxy and sidecar
/// services. The operator service stays off it, so a compromised workload finds
/// nothing but the two APIs its own components already speak.
pub fn workload_router_with_tls<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    auth_config: AuthConfig,
    route_events: RouteSubscriptionBroker,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> RuntimeResult<NativeControlPlaneRouter>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let auth = ControlPlaneAuth::from_config(auth_config, ObservabilityRecorder::global());
    Ok(grpc_server(route_events.clone(), tls)?
        .add_service(tonic::service::interceptor::InterceptedService::new(
            proxy_grpc_service_with_store_and_route_events(
                Arc::clone(&store),
                materializer.clone(),
                target.clone(),
                route_events.clone(),
            ),
            auth.interceptor(
                crate::api::PROXY_SERVICE_NAME,
                crate::auth::CallerRole::Proxy,
            ),
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            sidecar_grpc_service_with_store_and_route_events(
                store,
                materializer,
                target,
                route_events,
            ),
            auth.interceptor(
                crate::api::SIDECAR_SERVICE_NAME,
                crate::auth::CallerRole::Sidecar,
            ),
        )))
}

/// The listener an operator reaches, carrying the operator service alone.
pub fn operator_router_with_tls<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    auth_config: AuthConfig,
    route_events: RouteSubscriptionBroker,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> RuntimeResult<NativeControlPlaneRouter>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let auth = ControlPlaneAuth::from_config(auth_config, ObservabilityRecorder::global());
    Ok(grpc_server(route_events.clone(), tls)?.add_service(
        tonic::service::interceptor::InterceptedService::new(
            operator_grpc_service_with_store_and_route_events(
                store,
                materializer,
                target,
                route_events,
            ),
            auth.interceptor(
                crate::api::OPERATOR_SERVICE_NAME,
                crate::auth::CallerRole::Operator,
            ),
        ),
    ))
}

/// The transport settings both native listeners share.
fn grpc_server(
    route_events: RouteSubscriptionBroker,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> RuntimeResult<
    tonic::transport::server::Server<Stack<crate::api::admission::RpcAdmissionLayer, Identity>>,
> {
    let mut server = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    Ok(server
        .timeout(std::time::Duration::from_secs(10))
        .max_concurrent_streams(32)
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(5)))
        .layer(route_events.admission.clone()))
}

pub fn operator_grpc_web_router<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> OperatorGrpcWebRouter
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    operator_grpc_web_router_with_route_events(
        store,
        materializer,
        target,
        AuthConfig::NoAuth,
        RouteSubscriptionBroker::new(),
    )
}

fn operator_grpc_web_router_with_route_events<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    auth_config: AuthConfig,
    route_events: RouteSubscriptionBroker,
) -> OperatorGrpcWebRouter
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let auth = ControlPlaneAuth::from_config(auth_config, ObservabilityRecorder::global());
    admitted_grpc_web_server_builder(route_events.admission.clone())
        .timeout(std::time::Duration::from_secs(10))
        .max_concurrent_streams(32)
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(5)))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            operator_grpc_service_with_store_and_route_events(
                store,
                materializer,
                target,
                route_events,
            ),
            auth.interceptor(
                crate::api::OPERATOR_SERVICE_NAME,
                crate::auth::CallerRole::Operator,
            ),
        ))
}

// CORS adds headers only; admission must wrap the gRPC-web body transformation,
// including its newly allocated base64 output, to retain final delivery ownership.
pub(crate) fn admitted_grpc_web_server_builder(
    admission: crate::api::admission::RpcAdmissionLayer,
) -> tonic::transport::Server<OperatorGrpcWebLayers> {
    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(crate::api::server::operator_grpc_web_cors_layer())
        .layer(admission)
        .layer(tonic_web::GrpcWebLayer::new())
}

pub async fn run_from_env() -> RuntimeResult<()> {
    let config = RuntimeConfig::from_env()?;
    let prometheus = install_runtime_observability(config.metrics_listen_addr.is_some());
    let store = connect_store(&config.control_plane.store, config.security.sealer()?).await?;
    let kube_client = KubeMaterializerClient::try_default().await?;
    let materializer = KubernetesMaterializer::new(
        RetryingKubernetesMaterializerClient::with_default_policy(kube_client),
    );

    serve(config, store, materializer, prometheus).await?;

    Ok(())
}

pub async fn serve<C>(
    config: RuntimeConfig,
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    prometheus: Option<PrometheusMetricsSink>,
) -> RuntimeResult<()>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let sockets = Arc::new(tokio::sync::Semaphore::new(
        config.api_limits.accepted_connections,
    ));
    let native_incoming = crate::runtime_io::BoundedIncoming::bind(
        config.listen_addr,
        sockets.clone(),
        config.api_limits.setup_timeout,
        config.api_limits.write_timeout,
    )
    .await?;
    let operator_incoming = crate::runtime_io::BoundedIncoming::bind(
        config.operator_listen_addr,
        sockets.clone(),
        config.api_limits.setup_timeout,
        config.api_limits.write_timeout,
    )
    .await?;
    let web_incoming = if let Some(address) = config.operator_grpc_web_listen_addr {
        Some(
            crate::runtime_io::BoundedIncoming::bind(
                address,
                sockets,
                config.api_limits.setup_timeout,
                config.api_limits.write_timeout,
            )
            .await?,
        )
    } else {
        None
    };
    let materializer = materializer_with_runtime_auth(materializer, &config.control_plane.auth)
        .with_sidecar_control_plane_transport(
            config.security.public_endpoint.clone(),
            config.security.ca_pem.clone(),
        );
    let route_events = RouteSubscriptionBroker::with_limits(config.api_limits.clone());
    let workload_router = workload_router_with_tls(
        Arc::clone(&store),
        materializer.clone(),
        config.target.clone(),
        config.control_plane.auth.clone(),
        route_events.clone(),
        config.security.tls(config.api_limits.setup_timeout)?,
    )?;
    let operator_router = operator_router_with_tls(
        Arc::clone(&store),
        materializer.clone(),
        config.target.clone(),
        config.control_plane.auth.clone(),
        route_events.clone(),
        config.security.tls(config.api_limits.setup_timeout)?,
    )?;
    let (shutdown_tx, _) = watch::channel(false);
    let native_shutdown = shutdown_tx.subscribe();
    let reconciler_shutdown = shutdown_tx.subscribe();
    let reconciler = MaterializationReconciler::new(
        Arc::clone(&store),
        materializer.clone(),
        config.target.clone(),
        MaterializationReconcilerConfig::default(),
        ObservabilityRecorder::global(),
    )
    .with_route_events(route_events.clone());
    let mut listeners = JoinSet::new();
    listeners.spawn(async move {
        reconciler
            .run_until_shutdown(reconciler_shutdown)
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
    });
    let dispatcher_store = store.clone();
    let dispatcher_events = route_events.clone();
    let dispatcher_shutdown = shutdown_tx.subscribe();
    listeners.spawn(async move {
        dispatch_route_changes(dispatcher_store, dispatcher_events, dispatcher_shutdown).await
    });
    let maintenance_store = store.clone();
    let maintenance_shutdown = shutdown_tx.subscribe();
    listeners.spawn(async move { maintain_runtime(maintenance_store, maintenance_shutdown).await });
    listeners.spawn({
        let native_shutdown = native_shutdown.clone();
        async move {
            workload_router
                .serve_with_incoming_shutdown(native_incoming, wait_for_shutdown(native_shutdown))
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        }
    });
    listeners.spawn({
        let operator_shutdown = native_shutdown.clone();
        async move {
            operator_router
                .serve_with_incoming_shutdown(
                    operator_incoming,
                    wait_for_shutdown(operator_shutdown),
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        }
    });

    if let Some(web_incoming) = web_incoming {
        let operator_grpc_web_router = operator_grpc_web_router_with_route_events(
            Arc::clone(&store),
            materializer.clone(),
            config.target.clone(),
            config.control_plane.auth.clone(),
            route_events.clone(),
        );
        let operator_grpc_web_shutdown = native_shutdown.clone();
        listeners.spawn(async move {
            operator_grpc_web_router
                .serve_with_incoming_shutdown(
                    web_incoming,
                    wait_for_shutdown(operator_grpc_web_shutdown),
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        });
    }

    if let Some(metrics_addr) = config.metrics_listen_addr {
        let prometheus = prometheus.unwrap_or_default();
        let metrics_shutdown = native_shutdown.clone();
        let store = Arc::clone(&store);
        listeners.spawn(async move {
            serve_prometheus_metrics_with_collector(
                metrics_addr,
                prometheus,
                wait_for_shutdown(metrics_shutdown),
                move |sink| {
                    let store = Arc::clone(&store);
                    async move {
                        record_materialization_operational_metrics(store, sink).await;
                    }
                },
            )
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        });
    }

    let result = supervise_runtime(
        listeners,
        shutdown_tx,
        route_events.clone(),
        shutdown_signal(),
    )
    .await;
    route_events.shutdown();
    result
}

async fn supervise_runtime(
    mut tasks: JoinSet<RuntimeResult<()>>,
    shutdown: watch::Sender<bool>,
    route_events: RouteSubscriptionBroker,
    signal: impl std::future::Future<Output = ()>,
) -> RuntimeResult<()> {
    tokio::pin!(signal);
    let mut first_error = tokio::select! {
        _ = &mut signal => None,
        result = tasks.join_next() => Some(match result {
            Some(Ok(Err(error))) => error,
            Some(Err(error)) => Box::new(error) as Box<dyn Error + Send + Sync>,
            _ => Box::new(std::io::Error::other("critical runtime task exited unexpectedly")) as Box<dyn Error + Send + Sync>,
        }),
    };
    route_events.shutdown();
    shutdown.send_replace(true);
    let drained = tokio::time::timeout(std::time::Duration::from_secs(25), async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| Box::new(error));
                }
            }
        }
    })
    .await;
    if drained.is_err() {
        first_error.get_or_insert_with(|| {
            Box::new(std::io::Error::other("runtime shutdown deadline exceeded"))
        });
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    first_error.map_or(Ok(()), Err)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub async fn dispatch_route_changes(
    store: Arc<dyn ControlPlaneStore>,
    events: RouteSubscriptionBroker,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeResult<()> {
    // Subscribers attach after this process starts and resolve against current
    // state, so retained history predating it carries no information they lack.
    // Replaying it also bursts the broadcast channel at the moment the control
    // plane is coldest, and a subscriber that lags is dropped and made to reset.
    let mut cursor: Option<u64> = None;
    let mut failures = 0;
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => { events.shutdown(); return Ok(()); },
            _ = interval.tick() => {
                let Some(start) = cursor else {
                    match store.load_route_change_revision().await {
                        Ok(revision) => { failures = 0; cursor = Some(revision); },
                        Err(error) => {
                            failures += 1;
                            if failures >= 5 { return Err(Box::new(error)); }
                        }
                    }
                    continue;
                };
                let batch = match store.load_route_changes(start, 1024).await {
                    Ok(batch) => { failures = 0; batch },
                    Err(error) => {
                        events.reset(); failures += 1;
                        if failures >= 5 { return Err(Box::new(error)); }
                        continue;
                    }
                };
                if batch.reset { events.reset(); }
                for event in batch.events { events.publish_durable(&event)?; }
                cursor = Some(batch.cursor);
            }
        }
    }
}

async fn maintain_runtime(
    store: Arc<dyn ControlPlaneStore>,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeResult<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut failures = 0;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = interval.tick() => match store.maintain_runtime_records(1024).await {
                Ok(_) => failures = 0,
                Err(error) => { failures += 1; if failures >= 5 { return Err(Box::new(error)); } }
            }
        }
    }
}

fn install_runtime_observability(metrics_enabled: bool) -> Option<PrometheusMetricsSink> {
    if !metrics_enabled {
        let _ = ObservabilityRecorder::install_stderr_global();
        return None;
    }

    let prometheus = PrometheusMetricsSink::new();
    let _ = ObservabilityRecorder::install_global(Arc::new(CompositeObservabilitySink::new(vec![
        Arc::new(StderrObservabilitySink),
        Arc::new(prometheus.clone()),
    ])));
    Some(prometheus)
}

async fn record_materialization_operational_metrics(
    store: Arc<dyn ControlPlaneStore>,
    sink: PrometheusMetricsSink,
) {
    for state in MaterializationState::BACKLOG_STATES {
        let state = state.metric_label();
        sink.record_observation(MetricObservation::new(
            MATERIALIZATIONS_NONTERMINAL,
            vec![state],
            0.0,
        ));
        sink.record_observation(MetricObservation::new(
            MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
            vec![state],
            0.0,
        ));
    }

    for state in MaterializationState::HELD_KEY_STATES {
        let state = state.metric_label();
        sink.record_observation(MetricObservation::new(
            EXCLUSIVITY_KEYS_HELD,
            vec![state],
            0.0,
        ));
    }

    let Ok(metrics) = store.load_materialization_operational_metrics().await else {
        return;
    };

    use sleepypods_observability::metrics::{
        MATERIALIZATION_EFFECTS_UNCERTAIN, MATERIALIZATION_FAILURES_BLOCKED,
    };
    for (descriptor, count) in [
        (MATERIALIZATION_EFFECTS_UNCERTAIN, metrics.uncertain_effects),
        (MATERIALIZATION_FAILURES_BLOCKED, metrics.blocked_failures),
    ] {
        sink.record_observation(MetricObservation::new(descriptor, vec![], count as f64));
    }
    for state in metrics.backlog_states {
        let label = state.state.metric_label();
        sink.record_observation(MetricObservation::new(
            MATERIALIZATIONS_NONTERMINAL,
            vec![label],
            state.count as f64,
        ));
        sink.record_observation(MetricObservation::new(
            MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
            vec![label],
            state
                .oldest_age
                .map(|age| age.as_secs_f64())
                .unwrap_or_default(),
        ));
    }

    for state in metrics.held_key_states {
        let label = state.state.metric_label();
        sink.record_observation(MetricObservation::new(
            EXCLUSIVITY_KEYS_HELD,
            vec![label],
            state.exclusivity_keys_held as f64,
        ));
    }
}

fn materializer_with_runtime_auth<C>(
    materializer: KubernetesMaterializer<C>,
    auth_config: &AuthConfig,
) -> KubernetesMaterializer<C>
where
    C: KubernetesMaterializerClient,
{
    materializer.with_sidecar_control_plane_token(auth_config.sidecar_bearer_token().cloned())
}

async fn connect_store(
    config: &StoreProviderConfig,
    sealer: Option<Arc<crate::certificate::CertificateSealer>>,
) -> Result<Arc<dyn ControlPlaneStore>, crate::StoreError> {
    match config {
        StoreProviderConfig::Postgres(config) => {
            let mut store = PostgresStore::connect(config).await?;
            if let Some(sealer) = sealer {
                store = store.with_certificate_sealer(sealer);
            }
            let store: Arc<dyn ControlPlaneStore> = Arc::new(store);
            Ok(Arc::new(RetryingControlPlaneStore::with_default_policy(
                store,
            )))
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn required_value<'a>(
    values: &'a HashMap<String, String>,
    name: &'static str,
) -> Result<&'a str, RuntimeConfigError> {
    values
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(RuntimeConfigError::MissingEnv { name })
}

fn postgres_positive_integer(
    values: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<u64>, RuntimeConfigError> {
    values
        .get(name)
        .map(|value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0 && *value <= i64::MAX as u64)
                .ok_or(RuntimeConfigError::InvalidPostgresSetting { name })
        })
        .transpose()
}

fn parse_required_socket_addr(
    values: &HashMap<String, String>,
    name: &'static str,
) -> Result<SocketAddr, RuntimeConfigError> {
    let value = required_value(values, name)?;
    parse_socket_addr(name, value)
}

fn parse_optional_socket_addr(
    values: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<SocketAddr>, RuntimeConfigError> {
    let Some(value) = values
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };

    parse_socket_addr(name, value).map(Some)
}

fn parse_socket_addr(name: &'static str, value: &str) -> Result<SocketAddr, RuntimeConfigError> {
    value
        .parse()
        .map_err(|source| RuntimeConfigError::InvalidListenAddr {
            name,
            value: value.to_owned(),
            source,
        })
}

fn parse_auth_config(values: &HashMap<String, String>) -> Result<AuthConfig, RuntimeConfigError> {
    let mode = required_value(values, AUTH_MODE_ENV)?;
    match mode.trim().to_ascii_lowercase().as_str() {
        "no-auth" => Ok(AuthConfig::NoAuth),
        "static-bearer-token" | "static-bearer-tokens" => {
            let tokens = StaticBearerTokens::new(
                required_value(values, AUTH_OPERATOR_TOKEN_ENV)?,
                required_value(values, AUTH_PROXY_TOKEN_ENV)?,
                required_value(values, AUTH_SIDECAR_TOKEN_ENV)?,
            )
            .map_err(RuntimeConfigError::InvalidAuthConfig)?;
            Ok(AuthConfig::static_bearer_tokens(tokens))
        }
        _ => Err(RuntimeConfigError::InvalidAuthMode {
            value: mode.to_owned(),
        }),
    }
}

impl fmt::Display for RuntimeConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSecurity(reason) => f.write_str(reason),
            Self::MissingEnv { name } => write!(f, "{name} is required"),
            Self::InvalidListenAddr {
                name,
                value,
                source,
            } => write!(
                f,
                "{name} value {value:?} is not a valid listen address: {source}"
            ),
            Self::InvalidStoreProvider { value } => {
                write!(f, "{STORE_PROVIDER_ENV} value {value:?} is not supported")
            }
            Self::InvalidPostgresConfig(source) => source.fmt(f),
            Self::InvalidPostgresSetting { name } => write!(
                f,
                "{name} must be a positive integer within the documented Postgres setting limits"
            ),
            Self::InvalidMaterializationTarget(source) => source.fmt(f),
            Self::InvalidAuthMode { value } => {
                write!(f, "{AUTH_MODE_ENV} value {value:?} is not supported")
            }
            Self::InvalidAuthConfig(source) => source.fmt(f),
        }
    }
}

impl Error for RuntimeConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidListenAddr { source, .. } => Some(source),
            Self::InvalidPostgresConfig(source) => Some(source),
            Self::InvalidMaterializationTarget(source) => Some(source),
            Self::InvalidAuthConfig(source) => Some(source),
            Self::MissingEnv { .. }
            | Self::InvalidPostgresSetting { .. }
            | Self::InvalidStoreProvider { .. }
            | Self::InvalidAuthMode { .. }
            | Self::InvalidSecurity(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        http01::{
            DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
            Http01ChallengeRecord, PutHttp01ChallengeRequest,
        },
        instance::{
            CompareAndSwapInstanceStateRequest, CreateInstanceRequest, CreateInstanceResult,
            DeleteInstanceRequest, GetInstanceRequest, InstanceRecord,
        },
        manifest::KubernetesObject,
        materialization::{
            BackendEndpoint, CompleteWakeRequest, CompleteWakeResult,
            LoadReadyMaterializationRequest, MaterializationBacklogOperationalMetrics,
            MaterializationHeldKeysOperationalMetrics, MaterializationOperationalMetrics,
            MaterializationRecord, MaterializationState, RecordMaterializationRequest,
            RenderedObjectRef,
        },
        materializer::{KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer},
        route::{
            CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
            ResolveRouteRequest, RouteBindingRecord, RouteDependencyLookup, RouteDependencySet,
            RouteResolution,
        },
        store::{StoreError, StoreFuture, StoreResult},
        workload::{
            CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest,
            WorkloadClassVersion,
        },
        ControlPlaneStore, KubernetesMaterializerClient,
    };

    use super::*;

    #[test]
    fn postgres_runtime_settings_are_explicit_and_positive() {
        let mut env = valid_env();
        env.extend([
            ("SLEEPYPODS_POSTGRES_MAX_CONNECTIONS", "1"),
            ("SLEEPYPODS_POSTGRES_POOL_WAIT_TIMEOUT_MS", "123"),
            ("SLEEPYPODS_POSTGRES_CONNECTION_TIMEOUT_MS", "456"),
            ("SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS", "789"),
            ("SLEEPYPODS_POSTGRES_IDEMPOTENCY_RETENTION_MS", "60000"),
        ]);
        let config = RuntimeConfig::from_key_values(env).unwrap();
        let StoreProviderConfig::Postgres(config) = config.control_plane.store;
        assert_eq!(config.max_connections, 1);
        assert_eq!(config.pool_wait_timeout.as_millis(), 123);
        assert_eq!(config.connection_timeout.as_millis(), 456);
        assert_eq!(config.statement_timeout.as_millis(), 789);
        assert_eq!(config.idempotency_retention.unwrap().as_millis(), 60000);
        for invalid in ["0", "-1", "abc", "18446744073709551615"] {
            let mut env = valid_env();
            env.push(("SLEEPYPODS_POSTGRES_MAX_CONNECTIONS", invalid));
            assert!(matches!(
                RuntimeConfig::from_key_values(env),
                Err(RuntimeConfigError::InvalidPostgresSetting { .. })
            ));
        }
        let config = RuntimeConfig::from_key_values(valid_env()).unwrap();
        let StoreProviderConfig::Postgres(config) = config.control_plane.store;
        assert_eq!(config.idempotency_retention, None);
    }

    #[test]
    fn postgres_runtime_settings_enforce_the_programmatic_limits() {
        for (name, maximum) in [
            (
                "SLEEPYPODS_POSTGRES_MAX_CONNECTIONS",
                PostgresStoreConfig::MAX_CONNECTIONS as u128,
            ),
            (
                "SLEEPYPODS_POSTGRES_POOL_WAIT_TIMEOUT_MS",
                PostgresStoreConfig::MAX_TIMEOUT.as_millis(),
            ),
            (
                "SLEEPYPODS_POSTGRES_CONNECTION_TIMEOUT_MS",
                PostgresStoreConfig::MAX_TIMEOUT.as_millis(),
            ),
            (
                "SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS",
                PostgresStoreConfig::MAX_TIMEOUT.as_millis(),
            ),
            (
                "SLEEPYPODS_POSTGRES_IDEMPOTENCY_RETENTION_MS",
                PostgresStoreConfig::MAX_IDEMPOTENCY_RETENTION.as_millis(),
            ),
        ] {
            for value in [1, maximum] {
                let value = value.to_string();
                let mut env = valid_env();
                env.push((name, &value));
                assert!(
                    RuntimeConfig::from_key_values(env).is_ok(),
                    "{name}={value}"
                );
            }
            for value in [
                "0".to_owned(),
                "-1".to_owned(),
                "1.5".to_owned(),
                (maximum + 1).to_string(),
                i64::MAX.to_string(),
                u64::MAX.to_string(),
            ] {
                let mut env = valid_env();
                env.push((name, &value));
                assert!(
                    matches!(RuntimeConfig::from_key_values(env),
                    Err(RuntimeConfigError::InvalidPostgresSetting { name: actual }) if actual == name),
                    "{name}={value}"
                );
            }
        }
    }

    #[test]
    fn env_config_requires_the_operator_listen_addr() {
        assert!(matches!(
            RuntimeConfig::from_key_values(valid_env_without(OPERATOR_LISTEN_ADDR_ENV)),
            Err(RuntimeConfigError::MissingEnv {
                name: OPERATOR_LISTEN_ADDR_ENV
            })
        ));
    }

    /// The listener a workload reaches carries no operator service, so a caller
    /// that finds the port still finds no operator method behind it.
    #[tokio::test]
    async fn the_workload_listener_serves_no_operator_method() {
        let store: Arc<dyn ControlPlaneStore> = Arc::new(NoopStore);
        let materializer = KubernetesMaterializer::new(NoopKubernetesClient);
        let target = MaterializationTarget::new("cluster-a", "apps").expect("target");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let router = workload_router_with_tls(
            store,
            materializer,
            target,
            AuthConfig::NoAuth,
            RouteSubscriptionBroker::new(),
            None,
        )
        .expect("workload router");
        let mut served = JoinSet::new();
        served.spawn(async move {
            router
                .serve_with_incoming(
                    tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(listener),
                )
                .await
        });

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let status =
            crate::api::pb::operator_control_plane_client::OperatorControlPlaneClient::new(
                channel.clone(),
            )
            .get_instance(crate::api::pb::GetInstanceRequest {
                instance_id: "instance-a".to_owned(),
            })
            .await
            .expect_err("the workload listener carries no operator service");
        assert_eq!(status.code(), tonic::Code::Unimplemented);

        // The services a workload does need answer on the same listener.
        let reachable =
            crate::api::pb::sidecar_control_plane_client::SidecarControlPlaneClient::new(channel)
                .report_idle(crate::api::pb::SidecarReportIdleRequest::default())
                .await
                .expect_err("the request is invalid, but the service is present");
        assert_ne!(reachable.code(), tonic::Code::Unimplemented);

        served.abort_all();
    }

    #[test]
    fn subscription_lifetime_reaches_past_the_request_timeout_cap() {
        let name = "SLEEPYPODS_CONTROL_PLANE_SUBSCRIPTION_LIFETIME_MS";

        for value in ["1", "60000", "300000", "600000"] {
            let mut env = valid_env();
            env.push((name, value));
            assert!(
                RuntimeConfig::from_key_values(env).is_ok(),
                "{name}={value}"
            );
        }

        for value in ["0", "-1", "1.5", "600001"] {
            let mut env = valid_env();
            env.push((name, value));
            assert!(
                matches!(RuntimeConfig::from_key_values(env),
                Err(RuntimeConfigError::InvalidPostgresSetting { name: actual }) if actual == name),
                "{name}={value}"
            );
        }
    }

    #[test]
    fn request_scoped_timeouts_stay_within_a_minute() {
        for name in [
            "SLEEPYPODS_CONTROL_PLANE_UNARY_DELIVERY_TIMEOUT_MS",
            "SLEEPYPODS_CONTROL_PLANE_SETUP_TIMEOUT_MS",
            "SLEEPYPODS_CONTROL_PLANE_WRITE_TIMEOUT_MS",
            "SLEEPYPODS_CONTROL_PLANE_LOOKUP_TIMEOUT_MS",
            "SLEEPYPODS_CONTROL_PLANE_RESPONSE_TIMEOUT_MS",
        ] {
            let mut env = valid_env();
            env.push((name, "60000"));
            assert!(RuntimeConfig::from_key_values(env).is_ok(), "{name}=60000");

            let mut env = valid_env();
            env.push((name, "60001"));
            assert!(
                matches!(RuntimeConfig::from_key_values(env),
                Err(RuntimeConfigError::InvalidPostgresSetting { name: actual }) if actual == name),
                "{name}=60001"
            );
        }
    }

    #[test]
    fn env_config_requires_listen_addr() {
        let error =
            RuntimeConfig::from_key_values(valid_env_without(CONTROL_PLANE_LISTEN_ADDR_ENV))
                .expect_err("listen address is required");

        assert!(matches!(
            error,
            RuntimeConfigError::MissingEnv {
                name: CONTROL_PLANE_LISTEN_ADDR_ENV
            }
        ));
    }

    #[test]
    fn env_config_rejects_unknown_store_provider() {
        let error = RuntimeConfig::from_key_values(
            valid_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == STORE_PROVIDER_ENV {
                        (key, "sqlite")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("unknown store provider is invalid");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidStoreProvider { value } if value == "sqlite"
        ));
    }

    #[test]
    fn env_config_rejects_invalid_native_listen_addr() {
        let error = RuntimeConfig::from_key_values(
            valid_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == CONTROL_PLANE_LISTEN_ADDR_ENV {
                        (key, "localhost")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("invalid listen address is rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidListenAddr {
                name: CONTROL_PLANE_LISTEN_ADDR_ENV,
                value,
                ..
            } if value == "localhost"
        ));
    }

    #[test]
    fn env_config_defaults_grpc_web_listener_to_absent() {
        let config = RuntimeConfig::from_key_values(valid_env())
            .expect("valid config without grpc-web listener");

        assert_eq!(config.operator_grpc_web_listen_addr, None);
    }

    #[test]
    fn env_config_accepts_optional_grpc_web_listener() {
        let mut values = valid_env();
        values.push((OPERATOR_GRPC_WEB_LISTEN_ADDR_ENV, "127.0.0.1:50052"));

        let config = RuntimeConfig::from_key_values(values).expect("valid config");

        assert_eq!(
            config.operator_grpc_web_listen_addr,
            Some("127.0.0.1:50052".parse().expect("socket address"))
        );
    }

    #[test]
    fn env_config_defaults_metrics_listener_to_absent() {
        let config = RuntimeConfig::from_key_values(valid_env())
            .expect("valid config without metrics listener");

        assert_eq!(config.metrics_listen_addr, None);
    }

    #[test]
    fn env_config_accepts_optional_metrics_listener() {
        let mut values = valid_env();
        values.push((METRICS_LISTEN_ADDR_ENV, "127.0.0.1:19090"));

        let config = RuntimeConfig::from_key_values(values).expect("valid config");

        assert_eq!(
            config.metrics_listen_addr,
            Some("127.0.0.1:19090".parse().expect("socket address"))
        );
    }

    #[test]
    fn env_config_parses_valid_config() {
        let config = RuntimeConfig::from_key_values(valid_env()).expect("valid config");

        assert_eq!(
            config.listen_addr,
            "127.0.0.1:50051".parse().expect("socket address")
        );
        assert_eq!(config.target.cluster_id(), "cluster-a");
        assert_eq!(config.target.namespace(), "apps");
        assert_eq!(
            config.control_plane.store.provider_name(),
            StoreProviderName::Postgres
        );
        assert_eq!(config.control_plane.auth, AuthConfig::NoAuth);
    }

    #[test]
    fn env_config_requires_explicit_auth_mode() {
        let error = RuntimeConfig::from_key_values(valid_env_without(AUTH_MODE_ENV))
            .expect_err("auth mode is required");

        assert!(matches!(
            error,
            RuntimeConfigError::MissingEnv {
                name: AUTH_MODE_ENV
            }
        ));
    }

    #[test]
    fn env_config_parses_static_bearer_token_auth() {
        let config = RuntimeConfig::from_key_values(static_auth_env()).expect("valid config");
        let AuthConfig::StaticBearerTokens(tokens) = config.control_plane.auth else {
            panic!("expected static bearer token config");
        };

        assert_eq!(
            tokens
                .token_for(crate::auth::CallerRole::Operator)
                .authorization_header_value()
                .expect("header value")
                .to_str()
                .expect("ascii"),
            "Bearer operator-secret"
        );
        assert_eq!(
            tokens
                .token_for(crate::auth::CallerRole::Proxy)
                .authorization_header_value()
                .expect("header value")
                .to_str()
                .expect("ascii"),
            "Bearer proxy-secret"
        );
        assert_eq!(
            tokens
                .token_for(crate::auth::CallerRole::Sidecar)
                .authorization_header_value()
                .expect("header value")
                .to_str()
                .expect("ascii"),
            "Bearer sidecar-secret"
        );
    }

    #[test]
    fn serve_materializer_gets_sidecar_token_from_runtime_auth_config() {
        let config = RuntimeConfig::from_key_values(static_auth_env()).expect("valid config");
        let materializer = materializer_with_runtime_auth(
            KubernetesMaterializer::new(NoopKubernetesClient),
            &config.control_plane.auth,
        );

        let token = materializer
            .sidecar_control_plane_token()
            .expect("static auth sidecar token is attached to materializer");

        assert_eq!(
            token
                .authorization_header_value()
                .expect("header")
                .to_str()
                .expect("ascii"),
            "Bearer sidecar-secret"
        );
    }

    #[test]
    fn env_config_static_auth_fails_closed_when_credentials_are_missing() {
        let error = RuntimeConfig::from_key_values(
            static_auth_env()
                .into_iter()
                .filter(|(key, _)| *key != AUTH_PROXY_TOKEN_ENV)
                .collect::<Vec<_>>(),
        )
        .expect_err("missing proxy token is rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::MissingEnv {
                name: AUTH_PROXY_TOKEN_ENV
            }
        ));
    }

    #[test]
    fn env_config_static_auth_rejects_malformed_env_tokens() {
        let error = RuntimeConfig::from_key_values(
            static_auth_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == AUTH_OPERATOR_TOKEN_ENV {
                        (key, "has space")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("malformed token is rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidAuthConfig(InvalidStaticBearerTokens::InvalidToken {
                role: crate::auth::CallerRole::Operator,
                ..
            })
        ));
    }

    #[test]
    fn env_config_static_auth_requires_distinct_role_tokens() {
        let error = RuntimeConfig::from_key_values(
            static_auth_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == AUTH_SIDECAR_TOKEN_ENV {
                        (key, "proxy-secret")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("duplicate role tokens are rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidAuthConfig(InvalidStaticBearerTokens::DuplicateToken {
                first: crate::auth::CallerRole::Proxy,
                second: crate::auth::CallerRole::Sidecar
            })
        ));
    }

    #[test]
    fn constructs_a_router_for_each_listener() {
        let store: Arc<dyn ControlPlaneStore> = Arc::new(NoopStore);
        let materializer = KubernetesMaterializer::new(NoopKubernetesClient);
        let target = MaterializationTarget::new("cluster-a", "apps").expect("target");

        let _workload = workload_router_with_tls(
            Arc::clone(&store),
            materializer.clone(),
            target.clone(),
            AuthConfig::NoAuth,
            RouteSubscriptionBroker::new(),
            None,
        )
        .expect("workload router");
        let _operator = operator_router_with_tls(
            Arc::clone(&store),
            materializer.clone(),
            target.clone(),
            AuthConfig::NoAuth,
            RouteSubscriptionBroker::new(),
            None,
        )
        .expect("operator router");
        let _operator_grpc_web = operator_grpc_web_router(store, materializer, target);
    }

    #[tokio::test]
    async fn materialization_operational_metrics_make_stuck_work_alertable_by_state() {
        let sink = PrometheusMetricsSink::new();
        let store: Arc<dyn ControlPlaneStore> = Arc::new(OperationalMetricsStore);

        record_materialization_operational_metrics(store, sink.clone()).await;

        let rendered = sink.render();
        assert_state_sample(
            &rendered,
            "sleepypods_materializations_nonterminal",
            "pending",
            "2",
        );
        assert_state_sample(
            &rendered,
            "sleepypods_materializations_nonterminal",
            "deleting",
            "1",
        );
        assert_state_sample(
            &rendered,
            "sleepypods_materialization_oldest_nonterminal_age_seconds",
            "pending",
            "11",
        );
        assert_state_sample(
            &rendered,
            "sleepypods_materialization_oldest_nonterminal_age_seconds",
            "deleting",
            "7",
        );
        assert!(!rendered.contains("sleepypods_materializations_nonterminal{state=\"ready\"}"));
        assert!(!rendered.contains(
            "sleepypods_materialization_oldest_nonterminal_age_seconds{state=\"ready\"}"
        ));
        assert_state_sample(
            &rendered,
            "sleepypods_exclusivity_keys_held",
            "pending",
            "0",
        );
        assert_state_sample(
            &rendered,
            "sleepypods_exclusivity_keys_held",
            "deleting",
            "0",
        );
        assert_state_sample(&rendered, "sleepypods_exclusivity_keys_held", "ready", "3");
        assert_state_sample(&rendered, "sleepypods_exclusivity_keys_held", "failed", "1");
    }

    fn assert_state_sample(rendered: &str, metric: &str, state: &str, value: &str) {
        let sample = format!("{metric}{{state=\"{state}\"}} {value}\n");
        assert!(
            rendered.contains(&sample),
            "missing state-only metric sample {sample:?} in rendered output:\n{rendered}"
        );
    }

    #[test]
    fn certificate_runtime_requires_tls_auth_and_reserved_database_capacity() {
        let extra = [
            (
                "SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE",
                "/test/identity.pem",
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE",
                "/test/identity.key",
            ),
            (
                "SLEEPYPODS_CONTROL_PLANE_PUBLIC_ENDPOINT",
                "https://cp.platform.example:50051",
            ),
            (
                "SLEEPYPODS_CERTIFICATE_SEALING_KEYS_FILE",
                "/test/sealing.json",
            ),
        ];
        let mut values = static_auth_env();
        values.extend(extra);
        assert!(RuntimeConfig::from_key_values(values.clone()).is_ok());
        values.push(("SLEEPYPODS_POSTGRES_MAX_CONNECTIONS", "1"));
        assert!(matches!(
            RuntimeConfig::from_key_values(values),
            Err(RuntimeConfigError::InvalidSecurity(_))
        ));
        for omitted in [
            "SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE",
            "SLEEPYPODS_CONTROL_PLANE_PUBLIC_ENDPOINT",
        ] {
            let mut values = static_auth_env();
            values.extend(extra.into_iter().filter(|(k, _)| *k != omitted));
            assert!(matches!(
                RuntimeConfig::from_key_values(values),
                Err(RuntimeConfigError::InvalidSecurity(_))
            ));
        }
        let mut values = valid_env();
        values.extend(extra);
        assert!(matches!(
            RuntimeConfig::from_key_values(values),
            Err(RuntimeConfigError::InvalidSecurity(_))
        ));
    }

    fn valid_env() -> Vec<(&'static str, &'static str)> {
        vec![
            (CONTROL_PLANE_LISTEN_ADDR_ENV, "127.0.0.1:50051"),
            (OPERATOR_LISTEN_ADDR_ENV, "127.0.0.1:50053"),
            (STORE_PROVIDER_ENV, "postgres"),
            (
                POSTGRES_URL_ENV,
                "postgres://sleepypods@example.com/sleepypods",
            ),
            (CLUSTER_ID_ENV, "cluster-a"),
            (NAMESPACE_ENV, "apps"),
            (AUTH_MODE_ENV, "no-auth"),
        ]
    }

    fn static_auth_env() -> Vec<(&'static str, &'static str)> {
        valid_env()
            .into_iter()
            .map(|(key, value)| {
                if key == AUTH_MODE_ENV {
                    (key, "static-bearer-token")
                } else {
                    (key, value)
                }
            })
            .chain([
                (AUTH_OPERATOR_TOKEN_ENV, "operator-secret"),
                (AUTH_PROXY_TOKEN_ENV, "proxy-secret"),
                (AUTH_SIDECAR_TOKEN_ENV, "sidecar-secret"),
            ])
            .collect()
    }

    fn valid_env_without(name: &str) -> Vec<(&'static str, &'static str)> {
        valid_env()
            .into_iter()
            .filter(|(key, _)| *key != name)
            .collect()
    }

    #[derive(Clone, Debug)]
    pub(super) struct NoopKubernetesClient;

    impl KubernetesMaterializerClient for NoopKubernetesClient {
        fn apply_object<'a>(
            &'a self,
            _object: &'a KubernetesObject,
            _precondition: Option<&'a crate::projection::LiveObjectIdentity>,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn delete_object<'a>(
            &'a self,
            _object: &'a RenderedObjectRef,
            _precondition: &'a crate::projection::LiveObjectIdentity,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn wait_for_pvc_bound<'a>(
            &'a self,
            _namespace: &'a str,
            _name: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn wait_for_readiness<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
            Box::pin(async {
                BackendEndpoint::new("http://example")
                    .map_err(|error| crate::KubernetesClientError::new(error.to_string()))
            })
        }

        fn ensure_no_descendants<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
            _instance_id: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn inspect_object<'a>(
            &'a self,
            _object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<
            'a,
            KubernetesClientResult<crate::projection::ProjectionObjectInspection>,
        > {
            Box::pin(async {
                Ok(crate::projection::ProjectionObjectInspection::Present(
                    crate::projection::LiveObjectMetadata {
                        persistent_volume_reclaim_policy: Some("Retain".into()),
                        identity: crate::projection::LiveObjectIdentity {
                            uid: "test-uid".into(),
                            resource_version: "1".into(),
                        },
                        labels: Default::default(),
                        annotations: Default::default(),
                        deleting: false,
                        finalizers: Vec::new(),
                    },
                ))
            })
        }

        fn verify_retained_bindings<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> crate::materializer::KubernetesClientFuture<
            'a,
            crate::materializer::KubernetesClientResult<()>,
        > {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug)]
    pub(super) struct NoopStore;

    #[derive(Debug)]
    struct OperationalMetricsStore;

    impl ControlPlaneStore for OperationalMetricsStore {
        unexpected_store_methods!(
            publish_certificate,
            get_certificate_metadata,
            set_tls_binding,
            get_tls_binding,
            remove_certificate,
            resolve_tls_certificate,
            reencrypt_certificate,
            snapshot_tls_bindings,
            load_route_changes,
            load_route_change_revision,
            load_materialization_work_status,
            record_materialization_failure,
            enqueue_materialization,
            maintain_runtime_records,
            accept_wake,
            request_instance_deletion,
            finalize_instance_deletions,
            create_instance,
            get_instance,
            delete_instance,
            create_workload_class_version,
            load_workload_class_version,
            create_route_binding,
            get_route_binding,
            delete_route_binding,
            list_route_bindings_for_instance,
            resolve_route,
            compare_and_swap_instance_state,
            record_materialization,
            load_ready_materialization,
            load_active_materialization,
            load_materialization,
            complete_wake,
            begin_sleep,
            finalize_sleep,
            list_materialization_reconciliation_candidates,
            claim_materialization_reconciliation,
            begin_materialization_effect,
            acknowledge_materialization_effect,
            renew_materialization_reconciliation_lease,
            release_materialization_reconciliation_lease,
            complete_wake_reconciliation,
            finalize_sleep_reconciliation,
            delete_materialization_reconciliation,
            force_delete_materialization,
            force_release_exclusivity_key,
            lookup_route_dependencies,
            put_http01_challenge,
            resolve_http01_challenge,
            delete_http01_challenge,
            expire_http01_challenges
        );

        fn load_materialization_operational_metrics(
            &self,
        ) -> StoreFuture<'_, StoreResult<MaterializationOperationalMetrics>> {
            Box::pin(async {
                Ok(MaterializationOperationalMetrics::new(
                    vec![
                        MaterializationBacklogOperationalMetrics::new(
                            MaterializationState::Pending,
                            2,
                            Some(std::time::Duration::from_secs(11)),
                        ),
                        MaterializationBacklogOperationalMetrics::new(
                            MaterializationState::Deleting,
                            1,
                            Some(std::time::Duration::from_secs(7)),
                        ),
                    ],
                    vec![
                        MaterializationHeldKeysOperationalMetrics::new(
                            MaterializationState::Ready,
                            3,
                        ),
                        MaterializationHeldKeysOperationalMetrics::new(
                            MaterializationState::Failed,
                            1,
                        ),
                    ],
                ))
            })
        }
    }

    impl ControlPlaneStore for NoopStore {
        unexpected_store_methods!(
            publish_certificate,
            get_certificate_metadata,
            set_tls_binding,
            get_tls_binding,
            remove_certificate,
            resolve_tls_certificate,
            reencrypt_certificate,
            snapshot_tls_bindings,
            load_route_changes,
            load_route_change_revision,
            load_materialization_work_status,
            record_materialization_failure,
            enqueue_materialization,
            maintain_runtime_records,
            accept_wake,
            request_instance_deletion,
            finalize_instance_deletions,
            list_route_bindings_for_instance,
            load_materialization,
            list_materialization_reconciliation_candidates,
            load_materialization_operational_metrics,
            claim_materialization_reconciliation,
            begin_materialization_effect,
            acknowledge_materialization_effect,
            renew_materialization_reconciliation_lease,
            release_materialization_reconciliation_lease,
            complete_wake_reconciliation,
            finalize_sleep_reconciliation,
            delete_materialization_reconciliation,
            force_delete_materialization,
            force_release_exclusivity_key
        );

        fn create_instance<'a>(
            &'a self,
            _request: CreateInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
            not_implemented()
        }

        fn get_instance<'a>(
            &'a self,
            _request: GetInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
            not_implemented()
        }

        fn delete_instance<'a>(
            &'a self,
            _request: DeleteInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn create_workload_class_version<'a>(
            &'a self,
            _request: CreateWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
            not_implemented()
        }

        fn load_workload_class_version<'a>(
            &'a self,
            _request: LoadWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
            not_implemented()
        }

        fn create_route_binding<'a>(
            &'a self,
            _request: CreateRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
            not_implemented()
        }

        fn get_route_binding<'a>(
            &'a self,
            _request: GetRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
            not_implemented()
        }

        fn delete_route_binding<'a>(
            &'a self,
            _request: DeleteRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn resolve_route<'a>(
            &'a self,
            _request: ResolveRouteRequest,
        ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
            not_implemented()
        }

        fn compare_and_swap_instance_state<'a>(
            &'a self,
            _request: CompareAndSwapInstanceStateRequest,
        ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
            not_implemented()
        }

        fn record_materialization<'a>(
            &'a self,
            _request: RecordMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
            not_implemented()
        }

        fn load_ready_materialization<'a>(
            &'a self,
            _request: LoadReadyMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn load_active_materialization<'a>(
            &'a self,
            _request: crate::LoadActiveMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn complete_wake<'a>(
            &'a self,
            _request: CompleteWakeRequest,
        ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
            not_implemented()
        }

        fn begin_sleep<'a>(
            &'a self,
            _request: crate::BeginSleepRequest,
        ) -> StoreFuture<'a, StoreResult<crate::BeginSleepResult>> {
            not_implemented()
        }

        fn finalize_sleep<'a>(
            &'a self,
            _request: crate::FinalizeSleepRequest,
        ) -> StoreFuture<'a, StoreResult<crate::FinalizeSleepResult>> {
            not_implemented()
        }

        fn lookup_route_dependencies<'a>(
            &'a self,
            _request: RouteDependencyLookup,
        ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
            not_implemented()
        }

        fn put_http01_challenge<'a>(
            &'a self,
            _request: PutHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
            not_implemented()
        }

        fn resolve_http01_challenge<'a>(
            &'a self,
            _key: Http01ChallengeKey,
        ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
            not_implemented()
        }

        fn delete_http01_challenge<'a>(
            &'a self,
            _request: DeleteHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn expire_http01_challenges<'a>(
            &'a self,
            _request: ExpireHttp01ChallengesRequest,
        ) -> StoreFuture<'a, StoreResult<usize>> {
            not_implemented()
        }
    }

    fn not_implemented<'a, T>() -> StoreFuture<'a, StoreResult<T>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }
}

#[cfg(test)]
mod supervision_tests {
    use super::*;
    #[tokio::test]
    async fn critical_failure_cancels_and_awaits_other_owned_tasks() {
        let (shutdown, mut receiver) = watch::channel(false);
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = finished.clone();
        let mut jobs = JoinSet::new();
        jobs.spawn(async move {
            let _ = receiver.changed().await;
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        jobs.spawn(async {
            Err(
                Box::new(std::io::Error::other("critical dispatcher failure"))
                    as Box<dyn Error + Send + Sync>,
            )
        });
        let error = supervise_runtime(
            jobs,
            shutdown,
            RouteSubscriptionBroker::new(),
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("critical dispatcher"));
        assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_reports_child_failure_and_still_drains_other_tasks() {
        let (shutdown, mut first) = watch::channel(false);
        let mut second = first.clone();
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = finished.clone();
        let mut jobs = JoinSet::new();
        jobs.spawn(async move {
            first.changed().await.unwrap();
            Err(Box::new(std::io::Error::other("failure while draining"))
                as Box<dyn Error + Send + Sync>)
        });
        jobs.spawn(async move {
            second.changed().await.unwrap();
            tokio::task::yield_now().await;
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        let error = supervise_runtime(jobs, shutdown, RouteSubscriptionBroker::new(), async {})
            .await
            .unwrap_err();
        assert!(error.to_string().contains("failure while draining"));
        assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_dispatcher_cancels_active_subscription_before_listener_drain() {
        use crate::api::pb::proxy_control_plane_client::ProxyControlPlaneClient;
        use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
        let broker = RouteSubscriptionBroker::new();
        let incoming = crate::runtime_io::BoundedIncoming::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(tokio::sync::Semaphore::new(2)),
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        let service = crate::api::proxy_grpc_service_with_store_and_route_events(
            Arc::new(tests::NoopStore),
            KubernetesMaterializer::new(tests::NoopKubernetesClient),
            MaterializationTarget::new("cluster-a", "apps").unwrap(),
            broker.clone(),
        );
        let admission = broker.admission.clone();
        let mut jobs = JoinSet::new();
        jobs.spawn(async move {
            tonic::transport::Server::builder()
                .layer(admission)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, wait_for_shutdown(receiver))
                .await?;
            Ok(())
        });
        let mut client = ProxyControlPlaneClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let (_requests, incoming) = tokio::sync::mpsc::channel(1);
        let mut responses = client
            .subscribe(ReceiverStream::new(incoming))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(broker.streams.available_permits(), 63);
        jobs.spawn(async {
            Err(Box::new(std::io::Error::other("dispatcher exited"))
                as Box<dyn Error + Send + Sync>)
        });
        let supervisor = tokio::spawn(supervise_runtime(
            jobs,
            shutdown,
            broker.clone(),
            std::future::pending(),
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), responses.message())
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), supervisor)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("dispatcher exited"));
        assert!(broker.cancellation.is_cancelled());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_handles_sigterm() {
        use std::io::BufRead;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::supervision_tests::sigterm_fixture",
                "--ignored",
                "--nocapture",
            ])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = std::io::BufReader::new(child.stdout.take().unwrap()).lines();
        while lines.next().unwrap().unwrap() != "sigterm-ready" {}
        assert!(std::process::Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success());
        let status = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(status.success());
    }
    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "subprocess fixture for process_handles_sigterm"]
    async fn sigterm_fixture() {
        let signal = shutdown_signal();
        tokio::pin!(signal);
        tokio::select! { _ = &mut signal => panic!("unexpected signal"), _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {} }
        println!("sigterm-ready");
        signal.await;
    }
}
