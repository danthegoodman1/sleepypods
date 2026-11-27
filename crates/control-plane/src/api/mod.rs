//! Protobuf-defined control-plane API and transport scaffolding.

pub mod admission;
mod certificate_watch;
mod certificates;
pub mod grpc_web;
mod proxy;
mod route_events;
pub mod server;
mod sidecar;
mod template;

pub use sleepypods_api::pb;

use sleepypods_observability::recorder::{
    LifecycleLogEvent, LogField, ObservabilityRecorder, EVENT_CONTROL_PLANE_STORE_ERROR,
    FIELD_ERROR_MESSAGE,
};
use tonic::Status;

use crate::store::StoreError;

pub use grpc_web::operator_grpc_web_cors_layer;
pub use proxy::{
    proxy_grpc_service_with_store, proxy_grpc_service_with_store_and_route_events,
    StoreBackedProxyApi, StoreBackedProxyGrpcService, PROXY_SERVICE_NAME,
};
pub use route_events::RouteSubscriptionBroker;
pub use server::{
    operator_grpc_service_with_store_and_route_events, StoreBackedOperatorApi,
    StoreBackedOperatorGrpcService, OPERATOR_SERVICE_NAME,
};
pub use sidecar::{
    sidecar_grpc_service_with_store, sidecar_grpc_service_with_store_and_route_events,
    StoreBackedSidecarApi, StoreBackedSidecarGrpcService, SIDECAR_SERVICE_NAME,
};

/// Maps a store failure to the status a caller receives. Unavailable and
/// internal failures carry backend text: that goes to the log, and the caller
/// gets the code with a fixed message.
pub(crate) fn store_error_to_status(error: StoreError) -> Status {
    match error {
        StoreError::SleepDeferred { retry_after } => {
            let mut status = Status::failed_precondition(format!(
                "automatic sleep deferred for {} ms after activation",
                retry_after.as_millis()
            ));
            status.metadata_mut().insert(
                sleepypods_api::IDLE_RETRY_AFTER_METADATA,
                retry_after
                    .as_millis()
                    .min(sleepypods_api::INITIAL_ACTIVATION_TIMEOUT.as_millis())
                    .to_string()
                    .parse()
                    .expect("decimal metadata"),
            );
            status
        }
        StoreError::InvalidArgument { message } => Status::invalid_argument(message),
        StoreError::NotFound { resource } => Status::not_found(format!("{resource} not found")),
        StoreError::AlreadyExists { resource } => {
            Status::already_exists(format!("{resource} already exists"))
        }
        StoreError::GenerationConflict { expected, actual } => Status::failed_precondition(
            format!("generation conflict: expected generation {expected}, found {actual}"),
        ),
        StoreError::ExclusivityConflict {
            cluster_id,
            namespace,
            key_name,
            owner_instance_id,
            owner_generation,
        } => {
            let mut message = format!(
                "exclusivity key {key_name:?} is already held for target {cluster_id}/{namespace}"
            );
            if let Some(owner_instance_id) = owner_instance_id {
                message.push_str(&format!(" by instance {owner_instance_id}"));
            }
            if let Some(owner_generation) = owner_generation {
                message.push_str(&format!(" generation {owner_generation}"));
            }
            Status::failed_precondition(message)
        }
        StoreError::LeaseConflict { message } => Status::aborted(message),
        StoreError::Unavailable { message } => {
            log_store_error("unavailable", message);
            Status::unavailable("control-plane store is unavailable")
        }
        StoreError::Internal { message } => {
            log_store_error("internal", message);
            Status::internal("control-plane store operation failed")
        }
    }
}

fn log_store_error(kind: &'static str, message: String) {
    ObservabilityRecorder::global().record_log(LifecycleLogEvent::new(
        EVENT_CONTROL_PLANE_STORE_ERROR,
        vec![
            LogField::error_reason(kind),
            LogField::new(FIELD_ERROR_MESSAGE, message),
        ],
    ));
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::store_error_to_status;
    use crate::store::StoreError;

    #[test]
    fn backend_text_stays_out_of_client_statuses() {
        for (error, code) in [
            (
                StoreError::internal("Postgres query failed: code=XX000 message=row detail"),
                Code::Internal,
            ),
            (
                StoreError::unavailable("Postgres connection closed: db.internal:5432"),
                Code::Unavailable,
            ),
        ] {
            let status = store_error_to_status(error);
            assert_eq!(status.code(), code);
            assert!(!status.message().contains("Postgres"), "{status:?}");
        }

        let status = store_error_to_status(StoreError::invalid_argument(
            "instance delete requires state deleting",
        ));
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), "instance delete requires state deleting");
    }
}
