use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use sleepypods_observability::recorder::ObservabilityRecorder;
use tokio::sync::broadcast;
use tonic::{
    codegen::tokio_stream::{self, wrappers::ReceiverStream},
    Request, Response, Status,
};

use crate::{
    api::{
        pb::{self, proxy_control_plane_server::ProxyControlPlaneServer},
        route_events::{RouteBindingChange, RouteSubscriptionBroker},
    },
    ids::{BackendGeneration, Generation, InstanceId},
    instance::{self as domain_instance, InstanceState},
    materialization::{MaterializationRecord, MaterializationTarget},
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    route::{self as domain_route},
    store::{ControlPlaneStore, StoreError},
    wake::{self, WakeInstanceError, WakeInstanceResult, WakeUnavailableReason},
};

pub const PROXY_SERVICE_NAME: &str = "sleepypods.controlplane.v1.ProxyControlPlane";
const SUBSCRIBE_RESPONSE_BUFFER: usize = 16;

type ProxySubscribeResponseStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<pb::ProxySubscribeResponse, Status>>
            + Send
            + 'static,
    >,
>;

#[derive(Clone, Debug)]
struct ActiveRouteSubscription {
    route_binding_id: crate::ids::RouteBindingId,
    instance_id: InstanceId,
    request_identity: domain_route::RouteIdentity,
    matched_identity: domain_route::RouteIdentity,
    protocol: domain_route::ProtocolRoute,
}

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
    .max_encoding_message_size(1024 * 1024)
}

#[tonic::async_trait]
impl<C> pb::proxy_control_plane_server::ProxyControlPlane for StoreBackedProxyApi<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    type SubscribeStream = ProxySubscribeResponseStream;
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
            .map_err(super::server::store_error_to_status)?
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
            Err(error) => {
                if let Some(instance) = failed_wake_instance(&error) {
                    notify_instance_routes_changed(
                        self.store.as_ref(),
                        &self.route_events,
                        instance.id.clone(),
                    )
                    .await?;
                }
                proxy_wake_error_response(instance_id, error)?
            }
        };

        Ok(Response::new(response))
    }

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let permit = self
            .route_events
            .streams
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("subscription stream capacity exhausted"))?;
        let permit = Arc::new(permit);
        let mut requests = request.into_inner();
        let store = Arc::clone(&self.store);
        let target = self.target.clone();
        // Register before resolving any snapshot, so a concurrent committed
        // change remains queued until the corresponding dependency is installed.
        let mut route_events = self.route_events.subscribe();
        let limits = self.route_events.limits.clone();
        let cancellation = self.route_events.cancellation.clone();
        let (responses, response_stream) = tokio::sync::mpsc::channel(SUBSCRIBE_RESPONSE_BUFFER);
        let producer_permit = permit.clone();
        let producer_broker = self.route_events.clone();
        let task = tokio::spawn(async move {
            let _producer_permit = producer_permit;
            // The tonic service value may be dropped once it returns response headers.
            let _producer_broker = producer_broker;
            let mut subscriptions = HashMap::new();
            let mut next_subscription_number = 0_u64;
            let expiry = tokio::time::sleep(limits.subscription_lifetime);
            tokio::pin!(expiry);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = responses.closed() => return,
                    _ = &mut expiry => return,
                    request = requests.message() => {
                        let request = match request { Ok(Some(request)) => request, _ => return };
                        let adding = matches!(request.input, Some(pb::proxy_subscribe_request::Input::SubscribeRoute(_)));
                        if adding && subscriptions.len() >= limits.subscriptions_per_stream {
                            let _ = responses.try_send(Err(Status::resource_exhausted("subscription entry capacity exhausted")));
                            return;
                        }
                        let response = tokio::select! {
                            _ = cancellation.cancelled() => return,
                            _ = &mut expiry => return,
                            result = tokio::time::timeout(limits.lookup_timeout, handle_subscribe_request(store.as_ref(), target.clone(), request, &mut subscriptions, &mut next_subscription_number, limits.positive_route_cache_ttl)) => {
                                match result { Ok(response) => response, Err(_) => Err(Status::deadline_exceeded("route lookup timed out")) }
                            }
                        };
                        match response {
                            Ok(Some(response)) => if !send_subscription_response(&responses, Ok(response), &limits, &cancellation).await { return; },
                            Ok(None) => {},
                            Err(status) => { let _ = send_subscription_response(&responses, Err(status), &limits, &cancellation).await; return; }
                        }
                    }
                    event = route_events.recv() => {
                        let event = match event {
                            Ok(RouteBindingChange::Reset) | Err(broadcast::error::RecvError::Lagged(_)) => return,
                            Ok(event) => event,
                            Err(broadcast::error::RecvError::Closed) => return,
                        };
                        for response in invalidations_for_route_event(&mut subscriptions, &event) {
                            if !send_subscription_response(&responses, Ok(response), &limits, &cancellation).await { return; }
                        }
                    }
                }
            }
        });
        let mut response: Response<Self::SubscribeStream> =
            Response::new(Box::pin(OwnedSubscriptionStream {
                receiver: ReceiverStream::new(response_stream),
                task,
                _permit: permit.clone(),
            }));
        response
            .extensions_mut()
            .insert(super::admission::SubscriptionLease {
                permit,
                lifetime: self.route_events.limits.subscription_lifetime,
            });
        Ok(response)
    }
}

async fn notify_instance_routes_changed(
    _store: &dyn ControlPlaneStore,
    route_events: &RouteSubscriptionBroker,
    instance_id: InstanceId,
) -> Result<(), Status> {
    route_events.notify_instance_changed(instance_id);
    Ok(())
}

fn failed_wake_instance(error: &WakeInstanceError) -> Option<&crate::instance::InstanceRecord> {
    match error {
        WakeInstanceError::WorkloadClassNotFound { instance }
        | WakeInstanceError::Render { instance, .. }
        | WakeInstanceError::SleepPolicy { instance, .. }
        | WakeInstanceError::Materializer { instance, .. } => Some(instance),
        WakeInstanceError::NotFound
        | WakeInstanceError::GenerationConflict { .. }
        | WakeInstanceError::Unavailable { .. }
        | WakeInstanceError::ReadyMaterializationNotFound { .. }
        | WakeInstanceError::Store(_) => None,
    }
}

fn invalidations_for_route_event(
    subscriptions: &mut HashMap<String, ActiveRouteSubscription>,
    event: &RouteBindingChange,
) -> Vec<pb::ProxySubscribeResponse> {
    let mut invalidated = Vec::new();
    subscriptions.retain(|subscription_id, dependency| {
        if subscription_invalidated_by_event(dependency, event) {
            invalidated.push(subscription_id.clone());
            false
        } else {
            true
        }
    });

    invalidated
        .into_iter()
        .map(|subscription_id| pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteInvalidated(
                pb::ProxyRouteInvalidatedResponse {
                    subscription_id,
                    reason: route_change_reason_to_proto(event) as i32,
                },
            )),
        })
        .collect()
}

fn subscription_invalidated_by_event(
    subscription: &ActiveRouteSubscription,
    event: &RouteBindingChange,
) -> bool {
    match event {
        RouteBindingChange::Reset => true,
        RouteBindingChange::Instance(id) => subscription.instance_id == *id,
        RouteBindingChange::Route {
            route_binding_id,
            removed,
            identity,
            protocol,
        } => {
            if subscription.route_binding_id == *route_binding_id {
                return true;
            }
            if *removed {
                return false;
            }
            let (Some(identity), Some(protocol)) = (identity, protocol) else {
                return false;
            };
            if subscription.protocol != *protocol {
                return false;
            }
            let Some(new_score) =
                domain_route::route_match_score(identity, &subscription.request_identity)
            else {
                return false;
            };
            let Some(cached_score) = domain_route::route_match_score(
                &subscription.matched_identity,
                &subscription.request_identity,
            ) else {
                return true;
            };
            new_score > cached_score
        }
    }
}
fn route_change_reason_to_proto(event: &RouteBindingChange) -> pb::ProxyRouteInvalidationReason {
    match event {
        RouteBindingChange::Route { removed: true, .. } => {
            pb::ProxyRouteInvalidationReason::RouteRemoved
        }
        _ => pb::ProxyRouteInvalidationReason::RouteChanged,
    }
}

async fn handle_subscribe_request(
    store: &dyn ControlPlaneStore,
    target: MaterializationTarget,
    request: pb::ProxySubscribeRequest,
    subscriptions: &mut HashMap<String, ActiveRouteSubscription>,
    next_subscription_number: &mut u64,
    positive_cache_ttl: Duration,
) -> Result<Option<pb::ProxySubscribeResponse>, Status> {
    match request
        .input
        .ok_or_else(|| Status::invalid_argument("subscribe request input is required"))?
    {
        pb::proxy_subscribe_request::Input::SubscribeRoute(request) => subscribe_route(
            store,
            target,
            request,
            subscriptions,
            next_subscription_number,
            positive_cache_ttl,
        )
        .await
        .map(Some),
        pb::proxy_subscribe_request::Input::Unsubscribe(request) => {
            let subscription_id = non_empty_field(request.subscription_id, "subscription_id")?;
            subscriptions.remove(&subscription_id);
            // Unsubscribe is idempotent and does not acknowledge; later update sources will
            // consult this per-stream map before sending subscription-targeted messages.
            Ok(None)
        }
    }
}

async fn subscribe_route(
    store: &dyn ControlPlaneStore,
    target: MaterializationTarget,
    request: pb::ProxySubscribeRouteRequest,
    subscriptions: &mut HashMap<String, ActiveRouteSubscription>,
    next_subscription_number: &mut u64,
    positive_cache_ttl: Duration,
) -> Result<pb::ProxySubscribeResponse, Status> {
    let request_id = non_empty_field(request.request_id, "request_id")?;
    let identity = request
        .identity
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("identity required"))?;
    validate_identity_size(identity)?;
    let request_identity = request
        .identity
        .ok_or_else(|| Status::invalid_argument("identity is required"))
        .and_then(route_identity_from_proto)?;

    match store
        .resolve_route(domain_route::ResolveRouteRequest::new(
            request_identity.clone(),
            target.clone(),
        ))
        .await
        .map_err(store_error_to_status)?
    {
        domain_route::RouteResolution::Resolved {
            matched_identity,
            entry,
        } => {
            let subscription_id = next_subscription_id(next_subscription_number);
            subscriptions.insert(
                subscription_id.clone(),
                ActiveRouteSubscription {
                    route_binding_id: entry.route_binding_id.clone(),
                    instance_id: entry.instance_id.clone(),
                    request_identity: request_identity.clone(),
                    matched_identity: matched_identity.clone(),
                    protocol: protocol_for_route_identity(&matched_identity),
                },
            );

            Ok(pb::ProxySubscribeResponse {
                output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
                    pb::ProxyRouteResolvedResponse {
                        request_id,
                        subscription_id,
                        matched_identity: Some(route_identity_to_proto(matched_identity)),
                        route: Some(route_entry_to_proto(entry)),
                        cache_policy: Some(cache_policy_to_proto(domain_route::CachePolicy::new(
                            positive_cache_ttl,
                        ))),
                    },
                )),
            })
        }
        domain_route::RouteResolution::Miss { negative_cache } => Ok(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
                pb::ProxyRouteMissResponse {
                    request_id,
                    request_identity: Some(route_identity_to_proto(request_identity)),
                    negative_cache_policy: Some(cache_policy_to_proto(negative_cache)),
                },
            )),
        }),
    }
}

fn protocol_for_route_identity(
    identity: &domain_route::RouteIdentity,
) -> domain_route::ProtocolRoute {
    match identity {
        domain_route::RouteIdentity::Http { .. } => domain_route::ProtocolRoute::Http,
        domain_route::RouteIdentity::Sni { .. } => domain_route::ProtocolRoute::TlsSni,
    }
}

fn next_subscription_id(next_subscription_number: &mut u64) -> String {
    *next_subscription_number += 1;
    format!("sub:{next_subscription_number}")
}

fn non_empty_field(value: String, field: &'static str) -> Result<String, Status> {
    if value.len() > 4096 {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds 4096 bytes"
        )));
    }
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!(
            "{field} must not be empty"
        )));
    }

    Ok(value)
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

fn route_identity_from_proto(
    identity: pb::RouteIdentity,
) -> Result<domain_route::RouteIdentity, Status> {
    match identity
        .kind
        .ok_or_else(|| Status::invalid_argument("route identity kind is required"))?
    {
        pb::route_identity::Kind::Http(http) => Ok(domain_route::RouteIdentity::Http {
            host: http
                .host
                .ok_or_else(|| Status::invalid_argument("HTTP route host is required"))
                .and_then(route_host_from_proto)?,
            path: http
                .path_prefix
                .map(domain_route::PathPrefix::new)
                .transpose()
                .map_err(invalid_argument_status)?,
        }),
        pb::route_identity::Kind::Sni(sni) => Ok(domain_route::RouteIdentity::Sni {
            host: sni
                .host
                .ok_or_else(|| Status::invalid_argument("SNI route host is required"))
                .and_then(route_host_from_proto)?,
        }),
    }
}

fn route_host_from_proto(host: pb::RouteHost) -> Result<domain_route::RouteHost, Status> {
    match pb::RouteHostKind::try_from(host.kind)
        .map_err(|_| Status::invalid_argument("route host kind is invalid"))?
    {
        pb::RouteHostKind::Exact => {
            domain_route::RouteHost::exact(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::WildcardSuffix => {
            domain_route::RouteHost::wildcard_suffix(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::Unspecified => {
            Err(Status::invalid_argument("route host kind is required"))
        }
    }
}

fn route_identity_to_proto(identity: domain_route::RouteIdentity) -> pb::RouteIdentity {
    let kind = match identity {
        domain_route::RouteIdentity::Http { host, path } => {
            pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(route_host_to_proto(host)),
                path_prefix: path.map(|path| path.as_str().to_owned()),
            })
        }
        domain_route::RouteIdentity::Sni { host } => {
            pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
                host: Some(route_host_to_proto(host)),
            })
        }
    };

    pb::RouteIdentity { kind: Some(kind) }
}

fn route_host_to_proto(host: domain_route::RouteHost) -> pb::RouteHost {
    pb::RouteHost {
        kind: route_host_kind_to_proto(host.kind()) as i32,
        host: host.as_str().to_owned(),
    }
}

fn route_host_kind_to_proto(kind: domain_route::RouteHostKind) -> pb::RouteHostKind {
    match kind {
        domain_route::RouteHostKind::Exact => pb::RouteHostKind::Exact,
        domain_route::RouteHostKind::WildcardSuffix => pb::RouteHostKind::WildcardSuffix,
    }
}

fn route_entry_to_proto(entry: domain_route::RouteEntry) -> pb::ProxyRouteEntry {
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

fn cache_policy_to_proto(policy: domain_route::CachePolicy) -> pb::ProxyCachePolicy {
    pb::ProxyCachePolicy {
        ttl_millis: policy.ttl().as_millis().try_into().unwrap_or(u64::MAX),
    }
}

fn invalid_argument_status(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}

fn store_error_to_status(error: StoreError) -> Status {
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
        StoreError::IdempotencyResourceDeleted { resource } => {
            Status::failed_precondition(format!("idempotent replay refers to a deleted {resource}"))
        }
        StoreError::IdempotencyConflict => {
            Status::already_exists("idempotency key was already used for a different request")
        }
        StoreError::Unavailable { message } => Status::unavailable(message),
        StoreError::Internal { message } => Status::internal(message),
    }
}

struct OwnedSubscriptionStream {
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    receiver: ReceiverStream<Result<pb::ProxySubscribeResponse, Status>>,
    task: tokio::task::JoinHandle<()>,
}
impl tokio_stream::Stream for OwnedSubscriptionStream {
    type Item = Result<pb::ProxySubscribeResponse, Status>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}
impl Drop for OwnedSubscriptionStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn send_subscription_response(
    responses: &tokio::sync::mpsc::Sender<Result<pb::ProxySubscribeResponse, Status>>,
    response: Result<pb::ProxySubscribeResponse, Status>,
    limits: &super::admission::ApiLimits,
    cancellation: &crate::runtime_work::Cancellation,
) -> bool {
    tokio::select! { _ = cancellation.cancelled() => false, result = tokio::time::timeout(limits.response_timeout, responses.send(response)) => matches!(result, Ok(Ok(()))) }
}
fn validate_identity_size(identity: &pb::RouteIdentity) -> Result<(), Status> {
    let (host, path) = match identity.kind.as_ref() {
        Some(pb::route_identity::Kind::Http(http)) => {
            (http.host.as_ref(), http.path_prefix.as_deref())
        }
        Some(pb::route_identity::Kind::Sni(sni)) => (sni.host.as_ref(), None),
        None => return Err(Status::invalid_argument("route identity required")),
    };
    if host.is_some_and(|host| host.host.len() > 253) || path.is_some_and(|path| path.len() > 4096)
    {
        return Err(Status::invalid_argument(
            "route identity exceeds supported length",
        ));
    }
    Ok(())
}
