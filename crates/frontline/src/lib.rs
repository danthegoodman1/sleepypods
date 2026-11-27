//! Frontline route-resolution and HTTP listener primitives.
//!
//! This crate models local route state, forwarding adapters, control-plane
//! transports, and the minimal HTTP listener runtime used by the frontline
//! binary.

pub mod cache;
pub mod certificates;
pub mod config;
pub mod control_plane;
pub mod control_plane_transport;
pub mod forward;
pub mod http01;
pub mod identity;
pub mod listener;
pub mod route;
pub mod runtime;
pub mod subscription;
pub mod tls;
pub mod wake;

pub use cache::{HostEntry, HostKey, RouteSnapshot, RouteTable};
pub use config::{FrontlineEnvConfig, FrontlineEnvConfigError};
pub use control_plane::{
    http01_challenge_key_to_proto, http01_challenge_record_from_proto,
    proxy_wake_response_from_proto, route_stream_reply_from_proto, route_stream_request_to_proto,
    wake_instance_request_to_proto, ProxyProtocolAdapterError,
};
pub use control_plane_transport::{
    GrpcProxyControlPlaneClient, GrpcProxyControlPlaneError, GrpcProxyHttp01Resolver,
    GrpcProxyHttp01ResolverError,
};
pub use forward::{
    http_upstream_origin, websocket_upstream_url, BackendForwardError, FrontlineForwardContext,
    FrontlineForwardError, FrontlineForwarder,
};
pub use http01::{
    http01_challenge_key, http01_challenge_token, intercept_http01_challenge,
    is_http01_challenge_candidate_path, Http01ChallengeResolveFuture, Http01ChallengeResolver,
    Http01ChallengeResponse, Http01InterceptDecision, Http01InterceptError,
    NoopHttp01ChallengeResolver, HTTP01_CHALLENGE_PREFIX, HTTP01_CONTENT_TYPE,
};
pub use identity::{RequestIdentityError, RouteRequestIdentity};
pub use listener::{
    serve_frontline, serve_http, serve_http_listener, serve_http_listener_with_admission,
    FrontlineHttpListenerConfig, FrontlineHttpListenerError, FrontlineListenerError,
    FrontlineListenerKind, FrontlineListenersConfig, FrontlineTlsPassthroughListenerConfig,
    FrontlineTlsTerminationListenerConfig,
};
pub use route::{
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome,
    RouteLookupCounters, SharedFrontlineRouteCoordinator, WakeClient, WakeClientFuture,
};
pub use runtime::{FrontlineHttpRuntime, FrontlineRuntimeBody};
pub use subscription::{
    RouteStream, RouteStreamEvent, RouteStreamReply, RouteStreamRequest, RouteSubscriptionClient,
    RouteSubscriptionFuture, SubscribeErrorCode, SubscriptionId,
};
pub use tls::{
    passthrough_backend_addr, FrontlineTlsAdapter, TerminatedTls, TlsCertificateStore,
    TlsPassthrough, TlsPassthroughBackendError, TlsPassthroughClientHello, TlsPassthroughError,
    TlsTerminationError,
};
pub use wake::{
    route_disposition, ReadyBackend, RouteDisposition, WakeInstanceRequest, WakeInstanceResponse,
    WakeUnavailable, WakeUnavailableReason,
};
