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
pub mod matcher;
pub mod resolver;
pub mod route;
pub mod runtime;
pub mod subscription;
pub mod tls;
pub mod wake;

pub use cache::{
    CacheInsertResult, CacheLookup, CacheLookupHit, CacheLookupStatus, NegativeCacheEntry,
    PositiveCacheEntry, RouteCache,
};
pub use config::{FrontlineEnvConfig, FrontlineEnvConfigError};
pub use control_plane::{
    http01_challenge_key_to_proto, http01_challenge_record_from_proto,
    proxy_subscribe_input_to_proto, proxy_subscribe_response_from_proto,
    proxy_wake_response_from_proto, wake_instance_request_to_proto, ProxyProtocolAdapterError,
};
pub use control_plane_transport::{
    GrpcProxyControlPlaneClient, GrpcProxyControlPlaneError, GrpcProxyHttp01Resolver,
    GrpcProxyHttp01ResolverError,
};
pub use forward::{
    http_upstream_origin, BackendForwardError, FrontlineForwardContext, FrontlineForwardError,
    FrontlineForwarder,
};
pub use http01::{
    http01_challenge_key, http01_challenge_token, intercept_http01_challenge,
    is_http01_challenge_candidate_path, Http01ChallengeResolveFuture, Http01ChallengeResolver,
    Http01ChallengeResponse, Http01InterceptDecision, Http01InterceptError,
    NoopHttp01ChallengeResolver, HTTP01_CHALLENGE_PREFIX, HTTP01_CONTENT_TYPE,
};
pub use identity::{RequestIdentityError, RouteRequestIdentity};
pub use listener::{
    serve_frontline, serve_frontline_listeners, FrontlineHttpListenerConfig,
    FrontlineListenerError, FrontlineListenerKind, FrontlineListeners, FrontlineListenersConfig,
    FrontlineTlsPassthroughListenerConfig, FrontlineTlsTerminationListenerConfig,
};
pub use matcher::{MatchedRoute, RouteMatcher, RouteRule};
pub use resolver::{
    FrontlineRouteResolution, FrontlineRouteResolver, FrontlineRouteResolverError,
    RouteResolverProtocolError, RouteSubscriptionClient, RouteSubscriptionEvent,
    RouteSubscriptionFuture, UnexpectedSubscribeResponseKind,
};
pub use route::{
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome,
    SharedFrontlineRouteCoordinator, WakeClient, WakeClientFuture,
};
pub use runtime::{FrontlineHttpRuntime, FrontlineRuntimeBody};
pub use subscription::{
    ApplyControlPlaneMessageOutcome, ApplyUpdateOutcome, InvalidationReason, ProxySubscribeInput,
    RouteRequestId, SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState,
    UnsubscribeOutcome,
};
pub use tls::{
    passthrough_backend_addr, FrontlineTlsAdapter, TerminatedTls, TlsCertificateStore,
    TlsPassthroughBackendError, TlsPassthroughClientHello, TlsPassthroughError,
    TlsTerminationError,
};
pub use wake::{
    route_wake_decision, validate_route_update, validate_wake_response, ReadyBackend,
    RouteUpdateDisposition, RouteWakeDecision, StaleWakeObservation, WakeAdmission,
    WakeInstanceRequest, WakeInstanceResponse, WakeReason, WakeResponseDisposition, WakeTracker,
    WakeUnavailable, WakeUnavailableReason, WakeWait, WakeWaitReason,
};
