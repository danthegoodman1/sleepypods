use std::{convert::Infallible, error::Error, fmt, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use http::{header::CONTENT_TYPE, HeaderValue, Request, Response, StatusCode};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::body::Incoming;
use proxy_core::{
    observability::{prometheus::RuntimeActiveStreamsCollector, recorder::ObservabilityRecorder},
    serve_listener, Accept, AppHttpProxyError, AppProtocol, DrainError, DrainTracker,
    HttpProxyError, Shutdown,
};
use sleepypods_types::{Generation, InstanceId};
use tokio::{sync::Mutex, task::JoinSet};

use crate::{
    IdleDetector, IdleReportConfig, ReportIdleClient, SidecarConfigError, SidecarProxy,
    SidecarProxyConfig,
};

type BoxError = Box<dyn Error + Send + Sync>;
type RuntimeBody = UnsyncBoxBody<Bytes, BoxError>;
type Upgrades = Arc<Mutex<JoinSet<()>>>;

#[derive(Clone, Debug)]
pub struct SidecarRuntimeConfig {
    listen_addr: SocketAddr,
    proxy: SidecarProxyConfig,
    instance_id: InstanceId,
    generation: Generation,
    idle_report: IdleReportConfig,
    drain_grace_timeout: Duration,
    active_streams_collector: RuntimeActiveStreamsCollector,
    admission: proxy_core::ProxyAdmission,
}

#[derive(Debug)]
pub enum SidecarRuntimeError {
    Config(SidecarConfigError),
    Drain(DrainError),
}

impl SidecarRuntimeConfig {
    pub fn new(
        listen_addr: SocketAddr,
        app_port: u16,
        instance_id: InstanceId,
        generation: Generation,
        idle_report: IdleReportConfig,
        drain_grace_timeout: Duration,
    ) -> Result<Self, SidecarRuntimeError> {
        Ok(Self {
            listen_addr,
            proxy: SidecarProxyConfig::new(app_port)?,
            instance_id,
            generation,
            idle_report,
            drain_grace_timeout,
            active_streams_collector: RuntimeActiveStreamsCollector::default(),
            admission: proxy_core::ProxyAdmission::new(proxy_core::ProxyResourceConfig::default()),
        })
    }

    pub fn with_resource_config(mut self, config: proxy_core::ProxyResourceConfig) -> Self {
        self.proxy = self.proxy.with_resource_config(config);
        self.admission = proxy_core::ProxyAdmission::new(config);
        self
    }

    pub fn with_app_protocol(mut self, app_protocol: AppProtocol) -> Self {
        self.proxy = self.proxy.with_app_protocol(app_protocol);
        self
    }

    pub fn admission(&self) -> &proxy_core::ProxyAdmission {
        &self.admission
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn proxy(&self) -> &SidecarProxyConfig {
        &self.proxy
    }

    pub fn active_streams_collector(&self) -> RuntimeActiveStreamsCollector {
        self.active_streams_collector.clone()
    }
}

/// The proxy, its drain tracker and the idle-report task for one listener.
struct Serving {
    proxy: Arc<SidecarProxy>,
    drain: DrainTracker,
    idle_task: tokio::task::JoinHandle<()>,
}

impl Serving {
    fn start<Client>(config: &SidecarRuntimeConfig, mut client: Client) -> Self
    where
        Client: ReportIdleClient + Send + 'static,
        Client::Error: Send + 'static,
    {
        let observability = ObservabilityRecorder::global();
        let drain =
            DrainTracker::with_observability(config.drain_grace_timeout, observability.clone());
        config.active_streams_collector.attach(drain.clone());
        let proxy = Arc::new(SidecarProxy::new(config.proxy.clone(), drain.clone()));
        let idle = IdleDetector::with_observability(
            config.instance_id.clone(),
            config.generation,
            config.idle_report,
            drain.clone(),
            observability,
        );
        // Idle reports never start a drain: an instance the control plane
        // revives keeps serving on this pod. Only shutdown ends the listener.
        let idle_task = tokio::spawn(async move { match idle.run(&mut client).await {} });
        Self {
            proxy,
            drain,
            idle_task,
        }
    }

    /// Waits out the drain grace period, then stops every remaining task.
    async fn finish(
        self,
        mut connections: JoinSet<()>,
        upgrades: Option<Upgrades>,
    ) -> Result<(), SidecarRuntimeError> {
        let drained = self.proxy.drain().await;
        self.idle_task.abort();
        let _ = self.idle_task.await;
        if let Some(upgrades) = upgrades {
            let mut upgrades = upgrades.lock().await;
            upgrades.abort_all();
            while upgrades.join_next().await.is_some() {}
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        drained.map_err(SidecarRuntimeError::Drain)
    }
}

pub async fn serve_http_listener_with_idle<Client>(
    listener: impl Accept,
    config: SidecarRuntimeConfig,
    client: Client,
    shutdown: Shutdown,
) -> Result<(), SidecarRuntimeError>
where
    Client: ReportIdleClient + Send + 'static,
    Client::Error: Send + 'static,
{
    let serving = Serving::start(&config, client);
    let upgrades: Upgrades = Arc::new(Mutex::new(JoinSet::new()));

    let connections = serve_listener(listener, &shutdown, |stream, _| {
        let Ok(handshake) = config.admission.handshakes.try_acquire() else {
            return Err(stream);
        };
        let stream = config.admission.admit_io(stream)?;
        // A refused socket is not activity, so drain work starts last.
        let Ok(initial_work) = serving.drain.try_acquire() else {
            return Err(stream.into_inner());
        };
        let proxy = serving.proxy.clone();
        let upgrades = upgrades.clone();
        Ok(proxy_core::serve_http_connection_admitted(
            stream,
            shutdown.clone(),
            config.admission.clone(),
            handshake,
            Some(initial_work),
            move |request| forward_or_error(proxy.clone(), request, upgrades.clone()),
        ))
    })
    .await;

    serving.finish(connections, Some(upgrades)).await
}

pub async fn serve_tcp_listener_with_idle<Client>(
    listener: impl Accept,
    config: SidecarRuntimeConfig,
    client: Client,
    shutdown: Shutdown,
) -> Result<(), SidecarRuntimeError>
where
    Client: ReportIdleClient + Send + 'static,
    Client::Error: Send + 'static,
{
    let serving = Serving::start(&config, client);

    let connections = serve_listener(listener, &shutdown, |stream, _| {
        let (Ok(connection), Ok(request)) = (
            config.admission.connections.try_acquire(),
            config.admission.requests.try_acquire(),
        ) else {
            return Err(stream);
        };
        let proxy = serving.proxy.clone();
        Ok(async move {
            let (_connection, _request) = (connection, request);
            let _ = proxy.forward_tcp(stream).await;
        })
    })
    .await;

    serving.finish(connections, None).await
}

async fn forward_or_error(
    proxy: Arc<SidecarProxy>,
    mut request: Request<Incoming>,
    upgrades: Upgrades,
) -> Result<Response<RuntimeBody>, Infallible> {
    if proxy_core::is_websocket_upgrade(&request) {
        return Ok(match proxy.prepare_websocket_upgrade(&mut request).await {
            Ok((response, upgrade)) => {
                let mut tasks = upgrades.lock().await;
                while tasks.try_join_next().is_some() {}
                tasks.spawn(async move {
                    let _ = upgrade.run().await;
                });
                response.map(box_runtime_body)
            }
            Err(error) => proxy_core::websocket_error_response(&error).map(box_runtime_body),
        });
    }
    Ok(match proxy.forward_http(request).await {
        Ok(response) => response.map(box_runtime_body),
        Err(error) => proxy_error_response(error),
    })
}

fn box_runtime_body<B>(body: B) -> RuntimeBody
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

fn proxy_error_response(error: AppHttpProxyError) -> Response<RuntimeBody> {
    let mut response = Response::new(box_runtime_body(Full::new(Bytes::new())));
    *response.status_mut() = match error {
        AppHttpProxyError::GrpcRequiresH2c => {
            // A gRPC client reads its status from these headers. An HTTP error
            // status would reach it as an opaque transport failure.
            let headers = response.headers_mut();
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
            headers.insert("grpc-status", HeaderValue::from_static("12"));
            headers.insert(
                "grpc-message",
                HeaderValue::from_static(
                    "app port serves HTTP/1.1; gRPC requires the h2c app protocol",
                ),
            );
            StatusCode::OK
        }
        AppHttpProxyError::Proxy(HttpProxyError::Drain(_) | HttpProxyError::UpstreamSaturated) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        AppHttpProxyError::Proxy(HttpProxyError::UpstreamHeaderTimeout) => {
            StatusCode::GATEWAY_TIMEOUT
        }
        AppHttpProxyError::Proxy(HttpProxyError::RequestRewrite(_) | HttpProxyError::Client(_)) => {
            StatusCode::BAD_GATEWAY
        }
    };
    response
}

impl From<SidecarConfigError> for SidecarRuntimeError {
    fn from(error: SidecarConfigError) -> Self {
        Self::Config(error)
    }
}

impl fmt::Display for SidecarRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),
            Self::Drain(error) => write!(f, "sidecar drain failed: {error}"),
        }
    }
}

impl Error for SidecarRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Drain(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
