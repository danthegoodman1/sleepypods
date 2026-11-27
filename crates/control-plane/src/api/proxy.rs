use std::sync::Arc;

use sleepypods_observability::recorder::ObservabilityRecorder;
use tonic::{Request, Response, Status};

use crate::{
    api::{
        pb::{self, proxy_control_plane_server::ProxyControlPlaneServer},
        route_events::RouteSubscriptionBroker,
        store_error_to_status,
    },
    ids::{BackendGeneration, Generation, InstanceId},
    instance::{self as domain_instance, InstanceState},
    materialization::{MaterializationRecord, MaterializationTarget},
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    route::{self as domain_route},
    store::ControlPlaneStore,
    wake::{self, WakeInstanceError, WakeInstanceResult, WakeUnavailableReason},
};

pub const PROXY_SERVICE_NAME: &str = "sleepypods.controlplane.v1.ProxyControlPlane";

#[derive(Clone)]
pub struct StoreBackedProxyApi<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    observability: ObservabilityRecorder,
    route_events: RouteSubscriptionBroker,
}

impl<C> StoreBackedProxyApi<C> {
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
    ) -> Self {
        Self::with_observability(
            store,
            materializer,
            target,
            ObservabilityRecorder::default(),
        )
    }

    pub fn with_observability(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self::with_observability_and_route_events(
            store,
            materializer,
            target,
            observability,
            RouteSubscriptionBroker::new(),
        )
    }

    pub fn with_route_events(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
        route_events: RouteSubscriptionBroker,
    ) -> Self {
        Self::with_observability_and_route_events(
            store,
            materializer,
            target,
            ObservabilityRecorder::default(),
            route_events,
        )
    }

    pub fn with_observability_and_route_events(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
        observability: ObservabilityRecorder,
        route_events: RouteSubscriptionBroker,
    ) -> Self {
        Self {
            store,
            materializer,
            target,
            observability,
            route_events,
        }
    }
}

pub type StoreBackedProxyGrpcService<C> = ProxyControlPlaneServer<StoreBackedProxyApi<C>>;

/// The largest message the proxy service sends, sized for the largest host rule
/// set route creation admits. A message over it would end its stream.
pub(super) const MAX_PROXY_RESPONSE_BYTES: usize = crate::route::MAX_HOST_RULES_BYTES;

pub fn proxy_grpc_service_with_store<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> StoreBackedProxyGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    proxy_grpc_service_with_store_and_route_events(
        store,
        materializer,
        target,
        RouteSubscriptionBroker::new(),
    )
}

pub fn proxy_grpc_service_with_store_and_route_events<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    route_events: RouteSubscriptionBroker,
) -> StoreBackedProxyGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    ProxyControlPlaneServer::new(StoreBackedProxyApi::with_observability_and_route_events(
        store,
        materializer,
        target,
        ObservabilityRecorder::global(),
        route_events,
    ))
    .max_decoding_message_size(512 * 1024)
    .max_encoding_message_size(MAX_PROXY_RESPONSE_BYTES)
}

#[tonic::async_trait]
impl<C> pb::proxy_control_plane_server::ProxyControlPlane for StoreBackedProxyApi<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    type SubscribeStream = super::route_subscriptions::SubscribeStream;
    type WatchTlsCertificatesStream = super::certificate_watch::WatchStream;
    async fn watch_tls_certificates(
        &self,
        request: Request<tonic::Streaming<pb::WatchTlsCertificatesRequest>>,
    ) -> Result<Response<Self::WatchTlsCertificatesStream>, Status> {
        super::certificate_watch::watch(self.store.clone(), self.route_events.clone(), request)
            .await
    }

    async fn resolve_http01_challenge(
        &self,
        request: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("key is required"))
            .and_then(super::server::http01_key_from_proto)?;
        let challenge = self
            .store
            .resolve_http01_challenge(key)
            .await
            .map_err(store_error_to_status)?
            .map(super::server::http01_to_proto)
            .transpose()?;

        Ok(Response::new(pb::ResolveHttp01ChallengeResponse {
            challenge,
        }))
    }

    async fn resolve_tls_certificate(
        &self,
        request: Request<pb::ResolveTlsCertificateRequest>,
    ) -> Result<Response<pb::ResolveTlsCertificateResponse>, Status> {
        super::certificates::resolve(self.store.as_ref(), request).await
    }

    async fn wake_instance(
        &self,
        request: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        let request = proxy_wake_request_from_proto(request.into_inner(), self.target.clone())?;
        let instance_id = request.instance_id.as_str().to_owned();

        let result = wake::wake_instance_with_observability(
            self.store.as_ref(),
            &self.materializer,
            request,
            self.observability.clone(),
        )
        .await;
        let response = match result {
            Ok(WakeInstanceResult::AlreadyRunning {
                instance,
                materialization,
            }) => proxy_ready_response(&instance, &materialization)?,
            Ok(WakeInstanceResult::AlreadyWaking { instance }) => pb::ProxyWakeInstanceResponse {
                outcome: Some(pb::proxy_wake_instance_response::Outcome::StillWaking(
                    pb::ProxyWakeStillWakingResult {
                        instance_id: instance.id.as_str().to_owned(),
                        instance_generation: instance.generation.get(),
                    },
                )),
            },
            Err(error) => proxy_wake_error_response(instance_id, error)?,
        };

        Ok(Response::new(response))
    }

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        super::route_subscriptions::open(
            Arc::clone(&self.store),
            self.target.clone(),
            &self.route_events,
            request.into_inner(),
        )
    }
}

fn proxy_wake_request_from_proto(
    request: pb::ProxyWakeInstanceRequest,
    target: MaterializationTarget,
) -> Result<wake::WakeInstanceRequest, Status> {
    let mut wake_request = wake::WakeInstanceRequest::new(
        InstanceId::new(request.instance_id).map_err(invalid_argument_status)?,
        Generation::new(request.expected_generation),
        target,
    );
    if let Some(backend_generation) = request.backend_generation {
        wake_request =
            wake_request.with_backend_generation(BackendGeneration::new(backend_generation));
    }

    Ok(wake_request)
}

fn proxy_ready_response(
    instance: &domain_instance::InstanceRecord,
    materialization: &MaterializationRecord,
) -> Result<pb::ProxyWakeInstanceResponse, Status> {
    match materialization.backend.as_ref() {
        Some(backend) => Ok(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
                pb::ProxyWakeReadyResult {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                    backend_uri: backend.uri().to_owned(),
                    backend_generation: materialization.backend_generation.get(),
                },
            )),
        }),
        None => Err(Status::failed_precondition(format!(
            "ready materialization for instance {} has no backend endpoint",
            instance.id.as_str()
        ))),
    }
}

fn proxy_wake_error_response(
    request_instance_id: String,
    error: WakeInstanceError,
) -> Result<pb::ProxyWakeInstanceResponse, Status> {
    match error {
        WakeInstanceError::NotFound => Err(Status::not_found("instance not found")),
        WakeInstanceError::GenerationConflict { expected, actual } => {
            Ok(pb::ProxyWakeInstanceResponse {
                outcome: Some(
                    pb::proxy_wake_instance_response::Outcome::GenerationConflict(
                        pb::ProxyWakeGenerationConflictResult {
                            instance_id: request_instance_id,
                            expected_generation: expected.get(),
                            actual_generation: actual.get(),
                        },
                    ),
                ),
            })
        }
        WakeInstanceError::Unavailable { instance, reason } => Ok(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Unavailable(
                pb::ProxyWakeUnavailableResult {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                    reason: proxy_unavailable_reason_to_proto(reason) as i32,
                },
            )),
        }),
        WakeInstanceError::ReadyMaterializationNotFound { instance, .. } => {
            Err(Status::failed_precondition(format!(
                "ready materialization was not found for instance {} and configured target",
                instance.id.as_str()
            )))
        }
        WakeInstanceError::WorkloadClassNotFound { instance } => {
            Err(Status::failed_precondition(format!(
                "workload class version was not found for instance {}",
                instance.id.as_str()
            )))
        }
        WakeInstanceError::Render { instance, source } => Err(Status::internal(format!(
            "manifest render failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::SleepPolicy { instance, source } => Err(Status::internal(format!(
            "sleep policy resolution failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::Materializer { instance, source } => Err(Status::unavailable(format!(
            "materialization failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::Store(error) => Err(store_error_to_status(error)),
    }
}

fn proxy_unavailable_reason_to_proto(
    reason: WakeUnavailableReason,
) -> pb::ProxyWakeUnavailableReason {
    match reason {
        WakeUnavailableReason::Deleting => pb::ProxyWakeUnavailableReason::Deleting,
        WakeUnavailableReason::Deleted => pb::ProxyWakeUnavailableReason::Deleted,
    }
}

pub(super) fn route_entry_to_proto(entry: domain_route::RouteEntry) -> pb::ProxyRouteEntry {
    pb::ProxyRouteEntry {
        route_binding_id: entry.route_binding_id.as_str().to_owned(),
        instance_id: entry.instance_id.as_str().to_owned(),
        instance_state: instance_state_to_proto(entry.instance_state) as i32,
        instance_generation: entry.instance_generation.get(),
        backend_uri: entry.backend.map(|backend| backend.uri().to_owned()),
        backend_generation: entry
            .backend_generation
            .map(|backend_generation| backend_generation.get()),
    }
}

fn instance_state_to_proto(state: InstanceState) -> pb::InstanceState {
    match state {
        InstanceState::Cold => pb::InstanceState::Cold,
        InstanceState::Waking => pb::InstanceState::Waking,
        InstanceState::Running => pb::InstanceState::Running,
        InstanceState::Draining => pb::InstanceState::Draining,
        InstanceState::Failed => pb::InstanceState::Failed,
        InstanceState::Deleting => pb::InstanceState::Deleting,
        InstanceState::Deleted => pb::InstanceState::Deleted,
    }
}

fn invalid_argument_status(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}
