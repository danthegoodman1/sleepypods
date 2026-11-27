//! Proxy primitives shared by the frontline and sidecar binaries.

pub mod accounting;
pub mod admission;
mod connect;
mod delivery;
pub mod drain;
pub mod http;
pub mod listener;
pub mod observability;
pub mod pod_route;
pub mod resources;
pub mod shutdown;
pub mod tcp;
pub mod timeout;
pub mod tls;
pub mod websocket;

pub use accounting::{ActiveConnection, ActiveConnectionCounter};
pub use admission::{AdmissionError, AdmissionLimiter, AdmissionPermit};
pub use connect::connect_tcp;
pub use drain::{DrainError, DrainPermit, DrainTracker};
pub use http::{
    apply_forwarded_header_policy, prepare_reverse_proxy_request, serve_http_connection_admitted,
    strip_hop_by_hop_headers, upstream_request_uri, AppHttpProxy, AppHttpProxyError, AppProtocol,
    HttpProxy, HttpProxyError, InvalidAppProtocol, ReverseProxyRequestError, TrackedBody,
};
pub use listener::{close_refused, configure_socket, serve_listener, Accept};
pub use pod_route::{read_instance_claim, InvalidPodRoute, PodRoute, SidecarIo};
pub use resources::{AdmittedIo, InvalidProxyResourceConfig, ProxyAdmission, ProxyResourceConfig};
pub use shutdown::Shutdown;
pub use tcp::{proxy_streams_with_timeouts, TcpProxy, TcpProxyConfig, TcpProxyError};
pub use timeout::{with_timeout, TimeoutError};
pub use tls::{
    parse_tls_client_hello_sni, read_tls_client_hello_prefix,
    read_tls_client_hello_prefix_with_limit, TlsClientHelloError, TlsClientHelloPrefix,
    TlsClientHelloSni, MAX_TLS_CLIENT_HELLO_PREFIX_LEN,
};
pub use websocket::{
    is_websocket_upgrade, websocket_error_response, WebSocketProxy, WebSocketProxyConfig,
    WebSocketProxyError, WebSocketUpgrade,
};
