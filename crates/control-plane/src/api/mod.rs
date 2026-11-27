//! Protobuf-defined control-plane API and transport scaffolding.

pub mod admission;
mod certificate_watch;
mod certificates;
mod proxy;
mod route_events;
mod route_subscriptions;
pub mod server;
mod sidecar;
mod template;

pub use sleepypods_api::pb;

pub use proxy::{
    proxy_grpc_service_with_store, proxy_grpc_service_with_store_and_route_events,
    StoreBackedProxyApi, StoreBackedProxyGrpcService, PROXY_SERVICE_NAME,
};
pub use route_events::RouteSubscriptionBroker;
pub use server::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_service_with_store,
    operator_grpc_web_cors_layer, operator_grpc_web_server_builder, OperatorApiPlaceholder,
    StoreBackedOperatorApi, StoreBackedOperatorGrpcService, OPERATOR_SERVICE_NAME,
    OPERATOR_UNARY_METHODS,
};
pub use sidecar::{
    sidecar_grpc_service_with_store, StoreBackedSidecarApi, StoreBackedSidecarGrpcService,
    SIDECAR_SERVICE_NAME,
};
