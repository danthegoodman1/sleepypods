//! The frontline proxy: host-keyed route cache, route coordinator, wake
//! requests, HTTP-01 interception, certificate delivery and the HTTP, TLS
//! termination and SNI passthrough listeners.

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

pub use cache::{HostKey, RouteSnapshot, RouteTable};
pub use config::FrontlineEnvConfig;
pub use control_plane_transport::{
    GrpcProxyControlPlaneClient, GrpcProxyControlPlaneError, GrpcProxyHttp01Resolver,
};
pub use identity::RequestIdentityError;
pub use listener::{serve_frontline, serve_frontline_listeners, FrontlineListeners};
pub use route::{
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome,
    RouteLookupCounters, SharedFrontlineRouteCoordinator, WakeClient, WakeClientFuture,
};
pub use runtime::FrontlineHttpRuntime;
pub use subscription::{
    RouteStream, RouteStreamEvent, RouteStreamReply, RouteStreamRequest, RouteSubscriptionClient,
    RouteSubscriptionFuture, SubscribeErrorCode,
};
pub use tls::{FrontlineTlsAdapter, TlsCertificateStore};
pub use wake::{WakeInstanceRequest, WakeInstanceResponse};

// Paths the crate's own modules import from its root.
pub(crate) use control_plane::{
    http01_challenge_key_to_proto, http01_challenge_record_from_proto,
    proxy_wake_response_from_proto, route_stream_reply_from_proto, route_stream_request_to_proto,
    wake_instance_request_to_proto, ProxyProtocolAdapterError,
};
pub(crate) use forward::{FrontlineForwardContext, FrontlineForwardError, FrontlineForwarder};
pub(crate) use http01::{
    intercept_http01_challenge, is_http01_challenge_candidate_path, Http01ChallengeResolveFuture,
    Http01ChallengeResolver, Http01InterceptDecision, Http01InterceptError,
    NoopHttp01ChallengeResolver,
};
pub(crate) use identity::RouteRequestIdentity;
pub(crate) use listener::{
    FrontlineHttpListenerConfig, FrontlineListenersConfig, FrontlineTlsPassthroughListenerConfig,
    FrontlineTlsTerminationListenerConfig,
};
pub(crate) use runtime::FrontlineRuntimeBody;
pub(crate) use subscription::SubscriptionId;
#[cfg(test)]
pub(crate) use tls::TlsTerminationError;
#[cfg(test)]
pub(crate) use wake::WakeUnavailableReason;
pub(crate) use wake::{route_disposition, ReadyBackend, RouteDisposition, WakeUnavailable};
