//! Protobuf-defined control-plane API and transport scaffolding.

pub mod admission;
mod certificate_watch;
mod certificates;
mod proxy;
mod route_events;
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
    operator_grpc_service_with_store_and_route_events, operator_grpc_web_cors_layer,
    StoreBackedOperatorApi, StoreBackedOperatorGrpcService, OPERATOR_SERVICE_NAME,
};
pub use sidecar::{
    sidecar_grpc_service_with_store, sidecar_grpc_service_with_store_and_route_events,
    StoreBackedSidecarApi, StoreBackedSidecarGrpcService, SIDECAR_SERVICE_NAME,
};
