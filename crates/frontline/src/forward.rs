use std::{
    error::Error,
    fmt,
    net::{IpAddr, SocketAddr},
};

use bytes::Bytes;
use http::{uri::InvalidUri, Request, Response, Uri};
use http_body::Body;
use hyper::body::Incoming;
use proxy_core::{
    apply_forwarded_header_policy, DrainTracker, HttpProxy, HttpProxyError, PodRoute, TrackedBody,
    WebSocketProxy, WebSocketProxyError,
};

use crate::wake::ReadyBackend;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug)]
pub struct FrontlineForwarder {
    http: HttpProxy,
    websocket: WebSocketProxy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineForwardContext {
    peer_ip: IpAddr,
    proto: ForwardedProto,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardedProto {
    Http,
    Https,
}

#[derive(Debug)]
pub enum FrontlineForwardError {
    Backend(BackendForwardError),
    Http(HttpProxyError),
    WebSocket(WebSocketProxyError),
}

#[derive(Debug)]
pub enum BackendForwardError {
    InvalidUri(InvalidUri),
    MissingScheme,
    MissingAuthority,
    UnsupportedHttpScheme(String),
}

impl FrontlineForwarder {
    pub fn new(drain: DrainTracker) -> Self {
        Self {
            http: HttpProxy::new(drain.clone()),
            websocket: WebSocketProxy::new(drain),
        }
    }

    pub fn with_resource_config(mut self, config: proxy_core::ProxyResourceConfig) -> Self {
        let drain = self.http.drain_tracker().clone();
        self.http = HttpProxy::with_config(drain.clone(), config);
        self.websocket = WebSocketProxy::with_config(
            drain,
            proxy_core::WebSocketProxyConfig {
                handshake_timeout: config.setup_timeout(),
                write_timeout: config.write_idle_timeout(),
                ..proxy_core::WebSocketProxyConfig::default()
            },
        );
        self
    }

    pub async fn forward_http<B>(
        &self,
        ready: &ReadyBackend,
        request: Request<B>,
    ) -> Result<Response<TrackedBody<Incoming>>, FrontlineForwardError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        let upstream_origin = http_upstream_origin(ready)?;
        self.http
            .proxy(request, &upstream_origin)
            .await
            .map_err(FrontlineForwardError::Http)
    }

    pub async fn forward_http_with_context<B>(
        &self,
        ready: &ReadyBackend,
        mut request: Request<B>,
        context: FrontlineForwardContext,
    ) -> Result<Response<TrackedBody<Incoming>>, FrontlineForwardError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        context.apply_to_request(&mut request);
        self.forward_http(ready, request).await
    }

    pub async fn prepare_websocket_upgrade<B>(
        &self,
        ready: &ReadyBackend,
        request: &mut Request<B>,
        context: FrontlineForwardContext,
    ) -> Result<
        (
            Response<http_body_util::Full<Bytes>>,
            proxy_core::WebSocketUpgrade,
        ),
        FrontlineForwardError,
    > {
        context.apply_to_request(request);
        let upstream_origin = http_upstream_origin(ready)?;
        self.websocket
            .prepare_upgrade(request, &upstream_origin)
            .await
            .map_err(FrontlineForwardError::WebSocket)
    }
}

impl FrontlineForwardContext {
    pub fn http(peer_ip: IpAddr) -> Self {
        Self {
            peer_ip,
            proto: ForwardedProto::Http,
        }
    }

    pub fn https(peer_ip: IpAddr) -> Self {
        Self {
            peer_ip,
            proto: ForwardedProto::Https,
        }
    }

    pub fn apply_to_request<B>(&self, request: &mut Request<B>) {
        apply_forwarded_header_policy(request, self.peer_ip, self.proto.as_str());
    }
}

impl ForwardedProto {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

/// The Service origin of a ready backend, carrying its Pod route when the
/// control plane recorded a Pod address. HTTP and WebSocket forwarding try
/// the Pod first.
pub fn http_upstream_origin(ready: &ReadyBackend) -> Result<Uri, BackendForwardError> {
    let uri: Uri = ready
        .backend
        .uri()
        .parse()
        .map_err(BackendForwardError::InvalidUri)?;
    let scheme = uri.scheme_str().ok_or(BackendForwardError::MissingScheme)?;
    if scheme != "http" {
        return Err(BackendForwardError::UnsupportedHttpScheme(
            scheme.to_string(),
        ));
    }
    require_authority(&uri)?;
    Ok(pod_route(ready)
        .and_then(|route| route.origin(&uri).ok())
        .unwrap_or(uri))
}

/// The Pod route of a ready backend: an `http` Pod address with an IP host.
fn pod_route(ready: &ReadyBackend) -> Option<PodRoute> {
    let pod: Uri = ready.pod_backend.as_ref()?.uri().parse().ok()?;
    if pod.scheme_str() != Some("http") {
        return None;
    }
    let address = pod.authority()?.as_str().parse::<SocketAddr>().ok()?;
    PodRoute::new(
        ready.instance_id.as_str(),
        ready.backend_generation?.get(),
        address,
    )
    .ok()
}

fn require_authority(uri: &Uri) -> Result<&http::uri::Authority, BackendForwardError> {
    uri.authority().ok_or(BackendForwardError::MissingAuthority)
}

impl From<BackendForwardError> for FrontlineForwardError {
    fn from(error: BackendForwardError) -> Self {
        Self::Backend(error)
    }
}

impl fmt::Display for FrontlineForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend forwarding target is invalid: {error}"),
            Self::Http(error) => write!(f, "{error}"),
            Self::WebSocket(error) => write!(f, "{error}"),
        }
    }
}

impl Error for FrontlineForwardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::Http(error) => Some(error),
            Self::WebSocket(error) => Some(error),
        }
    }
}

impl fmt::Display for BackendForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(error) => write!(f, "backend URI is invalid: {error}"),
            Self::MissingScheme => write!(f, "backend URI is missing a scheme"),
            Self::MissingAuthority => write!(f, "backend URI is missing an authority"),
            Self::UnsupportedHttpScheme(scheme) => {
                write!(f, "backend URI scheme {scheme:?} is not supported for HTTP")
            }
        }
    }
}

impl Error for BackendForwardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUri(error) => Some(error),
            Self::MissingScheme | Self::MissingAuthority | Self::UnsupportedHttpScheme(_) => None,
        }
    }
}

#[cfg(test)]
mod tests;
