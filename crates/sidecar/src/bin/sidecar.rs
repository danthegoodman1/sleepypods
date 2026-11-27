use std::{env, error::Error, net::SocketAddr, process, time::Duration};

use proxy_core::{
    observability::{
        prometheus::{serve_prometheus_metrics, PrometheusMetricsSink},
        recorder::{CompositeObservabilitySink, ObservabilityRecorder, StderrObservabilitySink},
    },
    Shutdown,
};
use sidecar::{
    runtime::{serve_http_listener_with_idle, serve_tcp_listener_with_idle, SidecarRuntimeConfig},
    AppProtocol, GrpcSidecarControlPlaneClient, IdleReportConfig,
};
use sleepypods_api::{
    pb::sidecar_control_plane_client::SidecarControlPlaneClient, BearerToken, Generation,
    InstanceId, OptionalBearerTokenInterceptor,
};

const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

fn main() {
    if let Err(error) = run_with_runtime(run()) {
        eprintln!("sidecar runtime failed: {error}");
        process::exit(1);
    }
}

// Owned async cleanup finishes in run() first. A canceled blocking OS resolver
// must not pin final process shutdown through Tokio's default Runtime drop.
fn run_with_runtime(
    future: impl std::future::Future<Output = Result<(), Box<dyn Error + Send + Sync>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(future);
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    result
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let env = EnvConfig::from_env()?;
    let setup_timeout = env.runtime.admission().config().setup_timeout();
    let endpoint = sleepypods_api::transport::native_endpoint(
        env.control_plane_endpoint.clone(),
        env.control_plane_ca_pem.as_deref(),
    )?
    .connect_timeout(setup_timeout);
    let shutdown = Shutdown::new();
    let shutdown_task = spawn_shutdown_signal(shutdown.clone())?;
    let result = run_with_connection(env, async move { endpoint.connect().await }, shutdown).await;
    shutdown_task.abort();
    let _ = shutdown_task.await;
    result
}

// Keeping connection setup inside this lifecycle makes health publication ordering and
// cancellation testable without changing the production connector or forwarding path.
async fn run_with_connection(
    env: EnvConfig,
    connection: impl std::future::Future<
        Output = Result<tonic::transport::Channel, tonic::transport::Error>,
    >,
    shutdown: Shutdown,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let prometheus = install_runtime_observability(env.metrics_listen_addr.is_some());
    let setup_timeout = env.runtime.admission().config().setup_timeout();
    let channel = tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        result = tokio::time::timeout(setup_timeout, connection) => result??,
    };
    let interceptor =
        OptionalBearerTokenInterceptor::new(env.control_plane_sidecar_token.as_ref())?;
    let client = GrpcSidecarControlPlaneClient::new(SidecarControlPlaneClient::with_interceptor(
        channel,
        interceptor,
    ))
    .with_pod_uid(env.pod_uid);
    // Publish health only after the actual proxy socket exists and initial CP setup succeeds.
    // Keeping its listener outside the forwarding path avoids resetting TCP idle activity.
    let proxy_listener = tokio::net::TcpListener::bind(env.runtime.listen_addr()).await?;
    let readiness_listener = match env.readiness_listen_addr {
        Some(addr) => Some(tokio::net::TcpListener::bind(addr).await?),
        None => None,
    };
    let app_addr = env.runtime.proxy().app_addr();
    let active_streams = env.runtime.active_streams_collector();
    let proxy_shutdown = shutdown.clone();
    let metrics_shutdown = shutdown.clone();
    let first_error = std::sync::Mutex::new(None);
    tokio::join!(
        finish_listener(&shutdown, &first_error, async {
            match env.runtime_mode {
                SidecarRuntimeMode::Http => {
                    serve_http_listener_with_idle(
                        proxy_listener,
                        env.runtime,
                        client,
                        proxy_shutdown,
                    )
                    .await
                }
                SidecarRuntimeMode::Tcp => {
                    serve_tcp_listener_with_idle(
                        proxy_listener,
                        env.runtime,
                        client,
                        proxy_shutdown,
                    )
                    .await
                }
            }
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        }),
        finish_listener(&shutdown, &first_error, async {
            if let (Some(metrics_addr), Some(prometheus)) = (env.metrics_listen_addr, prometheus) {
                serve_prometheus_metrics(
                    metrics_addr,
                    prometheus,
                    metrics_shutdown.cancelled(),
                    move |sink| {
                        let active_streams = active_streams.clone();
                        async move {
                            active_streams.collect(sink);
                        }
                    },
                )
                .await?;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }),
        finish_listener(&shutdown, &first_error, async {
            if let Some(listener) = readiness_listener {
                sidecar::readiness::serve_readiness(listener, app_addr, shutdown.clone()).await;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }),
    );
    match first_error
        .into_inner()
        .expect("listener failure lock poisoned")
    {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

// A failed sibling signals the same normal shutdown path, then join! keeps polling
// every listener through its bounded drain and task cleanup before returning the first error.
async fn finish_listener(
    shutdown: &Shutdown,
    first_error: &std::sync::Mutex<Option<Box<dyn Error + Send + Sync>>>,
    listener: impl std::future::Future<Output = Result<(), Box<dyn Error + Send + Sync>>>,
) {
    if let Err(error) = listener.await {
        first_error
            .lock()
            .expect("listener failure lock poisoned")
            .get_or_insert(error);
        shutdown.shutdown();
    }
}

fn install_runtime_observability(metrics_enabled: bool) -> Option<PrometheusMetricsSink> {
    if !metrics_enabled {
        let _ = ObservabilityRecorder::install_stderr_global();
        return None;
    }

    let prometheus = PrometheusMetricsSink::new();
    let _ = ObservabilityRecorder::install_global(std::sync::Arc::new(
        CompositeObservabilitySink::new(vec![
            std::sync::Arc::new(StderrObservabilitySink),
            std::sync::Arc::new(prometheus.clone()),
        ]),
    ));
    Some(prometheus)
}

#[cfg(unix)]
fn spawn_shutdown_signal(
    shutdown: Shutdown,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error + Send + Sync>> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    Ok(tokio::spawn(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if result.is_ok() {
                    shutdown.shutdown();
                }
            },
            _ = terminate.recv() => {
                shutdown.shutdown();
            }
        }
    }))
}

#[cfg(not(unix))]
fn spawn_shutdown_signal(
    shutdown: Shutdown,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error + Send + Sync>> {
    Ok(tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown.shutdown();
        }
    }))
}

#[derive(Debug)]
struct EnvConfig {
    runtime: SidecarRuntimeConfig,
    runtime_mode: SidecarRuntimeMode,
    pod_uid: String,
    control_plane_endpoint: String,
    control_plane_ca_pem: Option<String>,
    control_plane_sidecar_token: Option<BearerToken>,
    metrics_listen_addr: Option<SocketAddr>,
    readiness_listen_addr: Option<SocketAddr>,
}

impl EnvConfig {
    fn from_env() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let listen_addr: SocketAddr = required_env("SLEEPYPODS_SIDECAR_LISTEN_ADDR")?.parse()?;
        let app_port = required_env("SLEEPYPODS_APP_PORT")?.parse()?;
        let instance_id = InstanceId::new(required_env("SLEEPYPODS_INSTANCE_ID")?)?;
        let generation = Generation::new(required_env("SLEEPYPODS_INSTANCE_GENERATION")?.parse()?);
        let pod_uid = required_env("SLEEPYPODS_POD_UID")?;
        if pod_uid.trim().is_empty() {
            return Err(Box::new(MissingEnvVar("SLEEPYPODS_POD_UID")));
        }
        let control_plane_endpoint = required_env("SLEEPYPODS_CONTROL_PLANE_ENDPOINT")?;
        let control_plane_ca_pem =
            env::var(sleepypods_api::transport::CONTROL_PLANE_TLS_CA_PEM_ENV).ok();
        let control_plane_sidecar_token =
            optional_bearer_token("SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN")?;
        let idle_timeout = duration_from_env_ms("SLEEPYPODS_IDLE_TIMEOUT_MS", 300_000)?;
        let retry_backoff = duration_from_env_ms("SLEEPYPODS_IDLE_RETRY_BACKOFF_MS", 5_000)?;
        let drain_grace_timeout =
            duration_from_env_ms("SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS", 30_000)?;
        let metrics_listen_addr = optional_socket_addr("SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR")?;
        let readiness_listen_addr =
            optional_socket_addr("SLEEPYPODS_SIDECAR_READINESS_LISTEN_ADDR")?;
        if let Some(addr) = readiness_listen_addr {
            if addr.port() == 0
                || addr.port() == app_port
                || addr.port() == listen_addr.port()
                || metrics_listen_addr.is_some_and(|metrics| metrics.port() == addr.port())
            {
                return Err("sidecar readiness port must be nonzero and differ from app, proxy, and metrics ports".into());
            }
        }
        let runtime_mode = SidecarRuntimeMode::from_env()?;
        let app_protocol = match env::var("SLEEPYPODS_APP_PROTOCOL") {
            Ok(value) => value.parse()?,
            Err(_) => AppProtocol::default(),
        };
        let resources = proxy_core::ProxyResourceConfig::from_vars(|name| env::var(name).ok())?;
        let idle_report =
            IdleReportConfig::new(idle_timeout, retry_backoff, resources.setup_timeout())?;
        let runtime = SidecarRuntimeConfig::new(
            listen_addr,
            app_port,
            instance_id,
            generation,
            idle_report,
            drain_grace_timeout,
        )?
        .with_resource_config(resources)
        .with_app_protocol(app_protocol);

        Ok(Self {
            runtime,
            runtime_mode,
            pod_uid,
            control_plane_endpoint,
            control_plane_ca_pem,
            control_plane_sidecar_token,
            metrics_listen_addr,
            readiness_listen_addr,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SidecarRuntimeMode {
    Http,
    Tcp,
}

impl SidecarRuntimeMode {
    fn from_env() -> Result<Self, InvalidSidecarRuntimeMode> {
        match env::var("SLEEPYPODS_SIDECAR_MODE") {
            Ok(value) => Self::parse(value),
            Err(_) => Ok(Self::Http),
        }
    }

    fn parse(value: String) -> Result<Self, InvalidSidecarRuntimeMode> {
        match value.as_str() {
            "http" => Ok(Self::Http),
            "tcp" => Ok(Self::Tcp),
            _ => Err(InvalidSidecarRuntimeMode(value)),
        }
    }
}

fn required_env(name: &'static str) -> Result<String, MissingEnvVar> {
    env::var(name).map_err(|_| MissingEnvVar(name))
}

fn optional_bearer_token(
    name: &'static str,
) -> Result<Option<BearerToken>, Box<dyn Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) => Ok(Some(BearerToken::new(name, value)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Box::new(error)),
    }
}

fn duration_from_env_ms(
    name: &'static str,
    default_ms: u64,
) -> Result<Duration, Box<dyn Error + Send + Sync>> {
    let value = match env::var(name) {
        Ok(value) => value.parse()?,
        Err(_) => default_ms,
    };

    Ok(Duration::from_millis(value))
}

fn optional_socket_addr(
    name: &'static str,
) -> Result<Option<SocketAddr>, Box<dyn Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) => parse_optional_socket_addr_value(name, Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Box::new(error)),
    }
}

fn parse_optional_socket_addr_value(
    _name: &'static str,
    value: Option<String>,
) -> Result<Option<SocketAddr>, Box<dyn Error + Send + Sync>> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        })
        .transpose()
}

#[derive(Debug)]
struct MissingEnvVar(&'static str);

impl std::fmt::Display for MissingEnvVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} is required", self.0)
    }
}

impl Error for MissingEnvVar {}

#[derive(Debug)]
struct InvalidSidecarRuntimeMode(String);

impl std::fmt::Display for InvalidSidecarRuntimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SLEEPYPODS_SIDECAR_MODE must be either http or tcp, got {:?}",
            self.0
        )
    }
}

impl Error for InvalidSidecarRuntimeMode {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_listener_value_is_optional() {
        assert_eq!(
            parse_optional_socket_addr_value("SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR", None)
                .expect("missing value is valid"),
            None
        );
    }

    #[test]
    fn metrics_listener_value_parses_socket_addr() {
        assert_eq!(
            parse_optional_socket_addr_value(
                "SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR",
                Some("127.0.0.1:19092".to_owned()),
            )
            .expect("socket address parses"),
            Some("127.0.0.1:19092".parse().expect("socket address"))
        );
    }

    #[test]
    fn metrics_listener_value_rejects_invalid_socket_addr() {
        assert!(parse_optional_socket_addr_value(
            "SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR",
            Some("localhost".to_owned()),
        )
        .is_err());
    }
}

#[cfg(test)]
#[path = "sidecar/readiness_tests.rs"]
mod readiness_tests;

#[cfg(test)]
#[path = "../../../../tests/support/runtime_shutdown.rs"]
mod runtime_shutdown_tests;
