use std::{error::Error, process, time::Duration};

use frontline::{
    serve_frontline, FrontlineEnvConfig, FrontlineHttpRuntime, FrontlineRouteCoordinator,
    FrontlineRouteResolver, FrontlineTlsAdapter, GrpcProxyControlPlaneClient,
    GrpcProxyHttp01Resolver, TlsCertificateStore, WakeTracker,
};
use proxy_core::{
    observability::{
        prometheus::{
            serve_prometheus_metrics_with_collector, PrometheusMetricsSink,
            RuntimeActiveStreamsCollector,
        },
        recorder::{
            CompositeObservabilitySink, FilteredStderrObservabilitySink, ObservabilityRecorder,
        },
    },
    DrainTracker, Shutdown,
};
use sleepypods_api::{
    pb::proxy_control_plane_client::ProxyControlPlaneClient, OptionalBearerTokenInterceptor,
};
use tokio::time::{sleep, Instant};
use tonic::transport::Endpoint;

const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
const CONTROL_PLANE_CONNECT_RETRY_BACKOFF: Duration = Duration::from_secs(1);
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

fn main() {
    if let Err(error) = run_with_runtime(run()) {
        eprintln!("frontline runtime failed: {error}");
        process::exit(1);
    }
}

// run() completes owned listener/drain and signal-task cleanup before this
// runtime boundary. A blocking OS DNS lookup may outlive its canceled async
// handle; do not let Tokio's default blocking-worker join pin process shutdown.
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
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let env = FrontlineEnvConfig::from_env()?;
    let prometheus = install_runtime_observability(env.metrics_listen_addr().is_some());
    let observability = ObservabilityRecorder::global();
    let shutdown = Shutdown::new();
    let shutdown_task = spawn_shutdown_signal(shutdown.clone())?;
    let result = run_with_shutdown(env, prometheus, observability, shutdown).await;
    shutdown_task.abort();
    let _ = shutdown_task.await;
    result
}

async fn run_with_shutdown(
    env: FrontlineEnvConfig,
    prometheus: Option<PrometheusMetricsSink>,
    observability: ObservabilityRecorder,
    shutdown: Shutdown,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let endpoint = sleepypods_api::transport::native_endpoint(
        env.control_plane_endpoint().to_owned(),
        env.control_plane_ca_pem(),
    )?;
    let Some(channel) =
        connect_control_plane(endpoint, env.control_plane_endpoint(), &shutdown).await?
    else {
        return Ok(());
    };
    let proxy_interceptor = OptionalBearerTokenInterceptor::new(env.control_plane_proxy_token())?;
    let route_client = GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::with_interceptor(
        channel.clone(),
        proxy_interceptor.clone(),
    ));
    let wake_client = GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::with_interceptor(
        channel.clone(),
        proxy_interceptor.clone(),
    ));
    let http01_resolver = GrpcProxyHttp01Resolver::new(ProxyControlPlaneClient::with_interceptor(
        channel.clone(),
        proxy_interceptor.clone(),
    ));
    let certificate_client = std::sync::Arc::new(
        frontline::certificates::GrpcCertificateResolver::new(channel, proxy_interceptor),
    );
    let (tls_certificates, certificate_worker) =
        TlsCertificateStore::with_observability(certificate_client.clone(), observability.clone());
    let certificate_worker = certificate_worker.with_watch(certificate_client);
    let resolver = FrontlineRouteResolver::with_observability(
        env.route_cache_capacity(),
        route_client,
        observability.clone(),
    );
    let coordinator = FrontlineRouteCoordinator::with_observability(
        resolver,
        WakeTracker::new(),
        wake_client,
        observability.clone(),
    )
    .with_wake_deadline(env.wake_instance_timeout())
    .with_route_deadline(env.route_timeout());
    let drain = DrainTracker::with_observability(env.drain_grace_timeout(), observability.clone());
    let active_streams = RuntimeActiveStreamsCollector::default();
    active_streams.attach(drain.clone());
    let runtime = FrontlineHttpRuntime::with_http01_resolver_and_observability(
        coordinator,
        http01_resolver,
        drain,
        observability,
    );
    let worker_shutdown = shutdown.clone();
    let certificate_metrics = tls_certificates.clone();
    let serve = async {
        let result: Result<(), Box<dyn Error + Send + Sync>> = async {
            if let (Some(metrics_addr), Some(prometheus)) = (env.metrics_listen_addr(), prometheus)
            {
                let metrics_shutdown = shutdown.clone();
                tokio::try_join!(
                    async {
                        serve_frontline(
                            env.listeners(),
                            runtime,
                            FrontlineTlsAdapter::new(tls_certificates),
                            shutdown.clone(),
                        )
                        .await
                        .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
                    },
                    async {
                        serve_prometheus_metrics_with_collector(
                            metrics_addr,
                            prometheus,
                            metrics_shutdown.cancelled(),
                            move |sink| {
                                let active_streams = active_streams.clone();
                                let certificate_metrics = certificate_metrics.clone();
                                async move {
                                    active_streams.collect(sink.clone());
                                    certificate_metrics.collect_metrics(&sink);
                                }
                            },
                        )
                        .await
                        .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
                    },
                )?;
            } else {
                serve_frontline(
                    env.listeners(),
                    runtime,
                    FrontlineTlsAdapter::new(tls_certificates),
                    shutdown.clone(),
                )
                .await?;
            }

            Ok(())
        }
        .await;
        shutdown.shutdown();
        result
    };
    let (result, ()) = tokio::join!(serve, certificate_worker.run(worker_shutdown));
    result
}

// The filtered stderr sink keeps lifecycle and error events visible while
// dropping metric samples and the per-request route-cache lookup event, so
// stderr observability performs no formatted writes on the forward path.
fn install_runtime_observability(metrics_enabled: bool) -> Option<PrometheusMetricsSink> {
    if !metrics_enabled {
        let _ = ObservabilityRecorder::install_global(std::sync::Arc::new(
            FilteredStderrObservabilitySink,
        ));
        return None;
    }

    let prometheus = PrometheusMetricsSink::new();
    let _ = ObservabilityRecorder::install_global(std::sync::Arc::new(
        CompositeObservabilitySink::new(vec![
            std::sync::Arc::new(FilteredStderrObservabilitySink),
            std::sync::Arc::new(prometheus.clone()),
        ]),
    ));
    Some(prometheus)
}

async fn connect_control_plane(
    endpoint: Endpoint,
    endpoint_label: &str,
    shutdown: &Shutdown,
) -> Result<Option<tonic::transport::Channel>, Box<dyn Error + Send + Sync>> {
    connect_control_plane_with(endpoint_label, shutdown, || endpoint.connect()).await
}

// Only initial Channel setup is retried. The injected attempt keeps deadline and
// cancellation tests deterministic without moving request retries into startup.
async fn connect_control_plane_with<F, Fut>(
    endpoint_label: &str,
    shutdown: &Shutdown,
    mut connect: F,
) -> Result<Option<tonic::transport::Channel>, Box<dyn Error + Send + Sync>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<tonic::transport::Channel, tonic::transport::Error>>,
{
    let deadline = Instant::now() + CONTROL_PLANE_CONNECT_TIMEOUT;
    let retry = async {
        loop {
            if shutdown.is_shutdown() {
                return Ok(None);
            }
            if Instant::now() >= deadline {
                return Err(startup_timeout());
            }
            let result = connect().await;
            // An attempt can finish on the deadline (or fail to yield while it
            // passes). Never accept a late success or begin another retry sleep.
            if shutdown.is_shutdown() {
                return Ok(None);
            }
            if Instant::now() >= deadline {
                return Err(startup_timeout());
            }
            match result {
                Ok(channel) => return Ok(Some(channel)),
                Err(error) => {
                    eprintln!(
                        "frontline waiting for control-plane endpoint {endpoint_label}: {error}"
                    );
                    sleep(CONTROL_PLANE_CONNECT_RETRY_BACKOFF).await;
                }
            }
        }
    };
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => Ok(None),
        _ = tokio::time::sleep_until(deadline) => Err(startup_timeout()),
        result = retry => result,
    }
}

fn startup_timeout() -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "frontline control-plane startup exceeded 60 seconds",
    ))
}

#[cfg(unix)]
fn spawn_shutdown_signal(shutdown: Shutdown) -> std::io::Result<tokio::task::JoinHandle<()>> {
    // Register SIGTERM before starting Channel.connect, so an unready process
    // observes Kubernetes termination while it is still in startup.
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    Ok(tokio::spawn(async move {
        tokio::select! {
            _ = interrupt.recv() => shutdown.shutdown(),
            _ = terminate.recv() => shutdown.shutdown(),
        }
    }))
}

#[cfg(not(unix))]
fn spawn_shutdown_signal(shutdown: Shutdown) -> std::io::Result<tokio::task::JoinHandle<()>> {
    Ok(tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown.shutdown();
        }
    }))
}

#[cfg(test)]
#[path = "frontline/startup_tests.rs"]
mod startup_tests;

#[cfg(test)]
#[path = "../../../../tests/support/runtime_shutdown.rs"]
mod runtime_shutdown_tests;
