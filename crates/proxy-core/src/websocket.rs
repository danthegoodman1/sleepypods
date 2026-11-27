//! WebSocket upgrades, relayed as bytes.
//!
//! The proxy completes the HTTP/1.1 upgrade with the upstream before it answers
//! the client, passes the handshake headers through in both directions, and
//! then copies the two upgraded streams. It never parses a frame: the peers
//! negotiate the key, subprotocol and extensions end to end, and a session
//! holds only the relay's fixed buffers.
use std::{error::Error, fmt, io, time::Duration};

use bytes::Bytes;
use http::{
    header::{CONNECTION, CONTENT_LENGTH, HOST, UPGRADE},
    HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri,
};
use http_body_util::{BodyExt, Empty, Full};
use hyper::upgrade::{OnUpgrade, Upgraded};
use hyper_util::rt::TokioIo;
use tokio::time::timeout;

use crate::{
    drain::{DrainError, DrainPermit, DrainTracker},
    http::{header_contains_token, strip_hop_by_hop_headers},
    tcp::proxy_streams_with_timeouts,
};

/// A rejection body larger than this fails the handshake instead of reaching
/// the client truncated.
const MAX_REJECTION_BODY: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct WebSocketProxyConfig {
    /// Bounds the upstream connection and handshake, and then the client's
    /// switch to the upgraded stream.
    pub handshake_timeout: Duration,
    /// Bounds a pending write or shutdown in either direction.
    pub write_timeout: Duration,
    /// Ends a session after both directions stay silent this long. `None`
    /// keeps a quiet session open; TCP keepalive finds a lost peer.
    pub idle_timeout: Option<Duration>,
    /// Ends a session this long after one peer closed its stream while the
    /// other still holds its own open.
    pub close_timeout: Duration,
}

impl Default for WebSocketProxyConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(60),
            idle_timeout: None,
            close_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WebSocketProxy {
    drain: DrainTracker,
    config: WebSocketProxyConfig,
}

/// An upgrade the upstream accepted. The session starts once the caller has
/// returned the paired `101` response to the client.
#[derive(Debug)]
pub struct WebSocketUpgrade {
    downstream: OnUpgrade,
    upstream: Upgraded,
    admission: Option<crate::http::UpgradeAdmission>,
    config: WebSocketProxyConfig,
    _permit: DrainPermit,
}

#[derive(Debug)]
pub enum WebSocketProxyError {
    Drain(DrainError),
    /// The request is not a WebSocket upgrade, or the client connection did not
    /// switch protocols.
    ClientHandshake(io::Error),
    /// The upstream was unreachable or did not complete the handshake.
    UpstreamConnect(io::Error),
    /// The upstream answered the handshake with a complete HTTP response.
    UpstreamRejected(Box<Response<Bytes>>),
    Proxy(io::Error),
}

impl WebSocketProxy {
    pub fn new(drain: DrainTracker) -> Self {
        Self::with_config(drain, WebSocketProxyConfig::default())
    }

    pub fn with_config(drain: DrainTracker, config: WebSocketProxyConfig) -> Self {
        Self { drain, config }
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    /// Completes the upstream handshake for `request` at `upstream_origin` and
    /// returns the `101` response to send to the client.
    pub async fn prepare_upgrade<B>(
        &self,
        request: &mut Request<B>,
        upstream_origin: &Uri,
    ) -> Result<(Response<Full<Bytes>>, WebSocketUpgrade), WebSocketProxyError> {
        if !is_websocket_upgrade(request) {
            return Err(WebSocketProxyError::ClientHandshake(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid WebSocket upgrade request",
            )));
        }
        let permit = self.drain.try_acquire()?;
        let handshake = upstream_handshake(request, upstream_origin)?;
        let (accepted, upstream) = timeout(
            self.config.handshake_timeout,
            connect_upstream(upstream_origin, handshake, self.config.handshake_timeout),
        )
        .await
        .map_err(|_| {
            WebSocketProxyError::UpstreamConnect(io::Error::new(
                io::ErrorKind::TimedOut,
                "upstream WebSocket handshake timeout",
            ))
        })??;

        let mut response = Response::new(Full::new(Bytes::new()));
        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        *response.headers_mut() = end_to_end_headers(accepted);
        mark_upgrade(response.headers_mut());
        let upgrade = WebSocketUpgrade {
            admission: request
                .extensions_mut()
                .remove::<crate::http::UpgradeAdmission>(),
            downstream: hyper::upgrade::on(request),
            upstream,
            config: self.config,
            _permit: permit,
        };
        Ok((response, upgrade))
    }
}

/// The client's handshake as the upstream receives it: the same headers, with
/// this hop's own `Connection` and `Upgrade` fields.
fn upstream_handshake<B>(
    request: &Request<B>,
    upstream_origin: &Uri,
) -> Result<Request<Empty<Bytes>>, WebSocketProxyError> {
    let authority = upstream_origin.authority().ok_or_else(|| {
        WebSocketProxyError::UpstreamConnect(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing upstream authority",
        ))
    })?;
    let mut handshake = Request::new(Empty::new());
    *handshake.method_mut() = Method::GET;
    *handshake.uri_mut() = request
        .uri()
        .path_and_query()
        .cloned()
        .map(Uri::from)
        .unwrap_or_else(|| Uri::from_static("/"));
    *handshake.headers_mut() = end_to_end_headers(request.headers().clone());
    mark_upgrade(handshake.headers_mut());
    if !handshake.headers().contains_key(HOST) {
        let host = HeaderValue::from_str(authority.as_str())
            .expect("a URI authority is a valid header value");
        handshake.headers_mut().insert(HOST, host);
    }
    Ok(handshake)
}

fn end_to_end_headers(mut headers: HeaderMap) -> HeaderMap {
    strip_hop_by_hop_headers(&mut headers);
    headers.remove(CONTENT_LENGTH);
    headers
}

fn mark_upgrade(headers: &mut HeaderMap) {
    headers.insert(CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
}

/// Sends the handshake over a new upstream connection and returns the `101`
/// headers with the upgraded stream. Hyper reads a rejection body in full; a
/// rejection becomes [`WebSocketProxyError::UpstreamRejected`].
async fn connect_upstream(
    upstream_origin: &Uri,
    handshake: Request<Empty<Bytes>>,
    setup_budget: Duration,
) -> Result<(HeaderMap, Upgraded), WebSocketProxyError> {
    let connect = |error: io::Error| WebSocketProxyError::UpstreamConnect(error);
    let aborted = |error: hyper::Error| {
        WebSocketProxyError::UpstreamConnect(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            error,
        ))
    };
    let host = upstream_origin.host().unwrap_or_default();
    // http::Uri retains brackets around an IPv6 authority. Tuple resolution
    // expects the bare address; brackets otherwise become an invalid DNS name.
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let port = upstream_origin.port_u16().unwrap_or(80);
    let stream = crate::connect::connect_tcp_attempts((host, port), setup_budget)
        .await
        .map_err(connect)?;
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(MAX_REJECTION_BODY)
        .handshake(TokioIo::new(stream))
        .await
        .map_err(aborted)?;
    let _driver = ConnectionDriver(tokio::spawn(connection.with_upgrades()));
    let mut response = sender.send_request(handshake).await.map_err(aborted)?;

    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        let (mut parts, mut body) = response.into_parts();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(aborted)?;
            if let Some(data) = frame.data_ref() {
                if bytes.len() + data.len() > MAX_REJECTION_BODY {
                    return Err(connect(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WebSocket rejection body exceeds 64 KiB",
                    )));
                }
                bytes.extend_from_slice(data);
            }
        }
        parts.headers = end_to_end_headers(parts.headers);
        return Err(WebSocketProxyError::UpstreamRejected(Box::new(
            Response::from_parts(parts, Bytes::from(bytes)),
        )));
    }
    if !header_contains_token(response.headers(), UPGRADE, "websocket") {
        return Err(connect(io::Error::new(
            io::ErrorKind::InvalidData,
            "upstream switched to a protocol other than WebSocket",
        )));
    }
    let upgraded = hyper::upgrade::on(&mut response).await.map_err(aborted)?;
    Ok((response.into_parts().0.headers, upgraded))
}

/// Aborts the upstream HTTP connection task when setup ends early. After a
/// completed upgrade the task has already handed its socket to the session.
struct ConnectionDriver<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for ConnectionDriver<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl WebSocketUpgrade {
    /// Relays the session until both peers close or a deadline ends it.
    pub async fn run(self) -> Result<(), WebSocketProxyError> {
        let (request_admission, handshake_admission) = self
            .admission
            .map(|guard| (Some(guard.request), Some(guard.handshake)))
            .unwrap_or((None, None));
        let _request_admission = request_admission;
        let _permit = self._permit;
        let downstream = timeout(self.config.handshake_timeout, self.downstream)
            .await
            .map_err(|_| {
                WebSocketProxyError::ClientHandshake(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "client WebSocket upgrade timeout",
                ))
            })?
            .map_err(|error| {
                WebSocketProxyError::ClientHandshake(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    error,
                ))
            })?;
        drop(handshake_admission);
        proxy_streams_with_timeouts(
            TokioIo::new(downstream),
            TokioIo::new(self.upstream),
            self.config.idle_timeout,
            self.config.write_timeout,
            Some(self.config.close_timeout),
        )
        .await
        .map_err(WebSocketProxyError::Proxy)
    }
}

pub fn is_websocket_upgrade<B>(request: &Request<B>) -> bool {
    request.method() == Method::GET
        && header_contains_token(request.headers(), CONNECTION, "upgrade")
        && header_contains_token(request.headers(), UPGRADE, "websocket")
}

/// The response for a failed upgrade. An upstream rejection keeps its status,
/// end-to-end headers and body.
pub fn websocket_error_response(error: &WebSocketProxyError) -> Response<Full<Bytes>> {
    let status = match error {
        WebSocketProxyError::UpstreamRejected(rejection) => {
            let mut response = Response::new(Full::new(rejection.body().clone()));
            *response.status_mut() = rejection.status();
            *response.headers_mut() = rejection.headers().clone();
            return response;
        }
        WebSocketProxyError::Drain(_) => StatusCode::SERVICE_UNAVAILABLE,
        WebSocketProxyError::ClientHandshake(_) => StatusCode::BAD_REQUEST,
        WebSocketProxyError::UpstreamConnect(error) if error.kind() == io::ErrorKind::TimedOut => {
            StatusCode::GATEWAY_TIMEOUT
        }
        WebSocketProxyError::UpstreamConnect(_) | WebSocketProxyError::Proxy(_) => {
            StatusCode::BAD_GATEWAY
        }
    };
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    response
}

impl From<DrainError> for WebSocketProxyError {
    fn from(error: DrainError) -> Self {
        Self::Drain(error)
    }
}

impl fmt::Display for WebSocketProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drain(error) => write!(f, "{error}"),
            Self::ClientHandshake(error) => write!(f, "websocket client handshake failed: {error}"),
            Self::UpstreamConnect(error) => write!(f, "websocket upstream connect failed: {error}"),
            Self::UpstreamRejected(response) => write!(
                f,
                "websocket upstream rejected the handshake with status {}",
                response.status()
            ),
            Self::Proxy(error) => write!(f, "websocket proxy failed: {error}"),
        }
    }
}

impl Error for WebSocketProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Drain(error) => Some(error),
            Self::ClientHandshake(error) | Self::UpstreamConnect(error) | Self::Proxy(error) => {
                Some(error)
            }
            Self::UpstreamRejected(_) => None,
        }
    }
}
