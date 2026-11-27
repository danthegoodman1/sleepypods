//! Sidecar-local proxy wiring.

pub mod control_plane_transport;
pub mod idle;
pub mod readiness;
pub mod runtime;

use std::{
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
};

use bytes::Bytes;
pub use control_plane_transport::{
    GrpcSidecarControlPlaneClient, GrpcSidecarControlPlaneError, ReportIdleClient,
    ReportIdleFuture, ReportIdleResponse, ReportIdleUnavailableReason, SidecarProtocolAdapterError,
};
use http::{Request, Response, Uri};
use http_body::Body;
use hyper::body::Incoming;
pub use idle::{
    IdleDetector, IdleObservation, IdleReportConfig, IdleReportConfigError, ReportIdleRequest,
};
pub use proxy_core::AppProtocol;
use proxy_core::{
    AppHttpProxy, AppHttpProxyError, DrainError, DrainTracker, TcpProxy, TcpProxyConfig,
    TcpProxyError, TrackedBody, WebSocketProxy, WebSocketProxyError,
};
use tokio::net::TcpStream;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SidecarProxyConfig {
    app_protocol: AppProtocol,
    app_origin: Uri,
    app_addr: SocketAddr,
    resources: proxy_core::ProxyResourceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SidecarConfigError {
    ZeroAppPort,
}

/// Forwards every accepted connection to the app container on loopback.
#[derive(Debug)]
pub struct SidecarProxy {
    config: SidecarProxyConfig,
    drain: DrainTracker,
    http: AppHttpProxy,
    websocket: WebSocketProxy,
    tcp: TcpProxy,
}

impl SidecarProxyConfig {
    pub fn new(app_port: u16) -> Result<Self, SidecarConfigError> {
        let app_port = NonZeroU16::new(app_port).ok_or(SidecarConfigError::ZeroAppPort)?;
        let app_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), app_port.get());
        let app_origin = format!("http://{app_addr}")
            .parse()
            .expect("loopback HTTP origin built from a non-zero u16 is valid");

        Ok(Self {
            app_protocol: AppProtocol::default(),
            app_origin,
            app_addr,
            resources: proxy_core::ProxyResourceConfig::default(),
        })
    }

    /// The protocol the app port serves for HTTP requests.
    pub fn with_app_protocol(mut self, app_protocol: AppProtocol) -> Self {
        self.app_protocol = app_protocol;
        self
    }

    pub fn with_resource_config(mut self, config: proxy_core::ProxyResourceConfig) -> Self {
        self.resources = config;
        self
    }

    pub fn app_addr(&self) -> SocketAddr {
        self.app_addr
    }
}

impl SidecarProxy {
    pub fn new(config: SidecarProxyConfig, drain: DrainTracker) -> Self {
        let http = AppHttpProxy::new(
            drain.clone(),
            config.resources,
            config.app_addr,
            config.app_protocol,
        );
        let websocket = WebSocketProxy::with_config(
            drain.clone(),
            proxy_core::WebSocketProxyConfig {
                handshake_timeout: config.resources.setup_timeout(),
                write_timeout: config.resources.write_idle_timeout(),
                ..proxy_core::WebSocketProxyConfig::default()
            },
        );
        let tcp = TcpProxy::new(
            drain.clone(),
            TcpProxyConfig {
                connect_timeout: config.resources.setup_timeout(),
                write_idle_timeout: config.resources.write_idle_timeout(),
                ..TcpProxyConfig::default()
            },
        );

        Self {
            config,
            drain,
            http,
            websocket,
            tcp,
        }
    }

    pub async fn drain(&self) -> Result<(), DrainError> {
        self.drain.drain().await
    }

    pub async fn forward_http<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<TrackedBody<Incoming>>, AppHttpProxyError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        self.http.proxy(request).await
    }

    pub async fn forward_tcp(&self, client: TcpStream) -> Result<(), TcpProxyError> {
        self.tcp.proxy(client, self.config.app_addr).await
    }

    /// WebSocket upgrades reach the app over HTTP/1.1 on either app protocol.
    pub async fn prepare_websocket_upgrade<B>(
        &self,
        request: &mut Request<B>,
    ) -> Result<
        (
            Response<http_body_util::Full<Bytes>>,
            proxy_core::WebSocketUpgrade,
        ),
        WebSocketProxyError,
    > {
        self.websocket
            .prepare_upgrade(request, &self.config.app_origin)
            .await
    }
}

impl fmt::Display for SidecarConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAppPort => write!(f, "sidecar app port must be non-zero"),
        }
    }
}

impl Error for SidecarConfigError {}

#[cfg(test)]
mod tests;
