//! Shared transport fixture, deliberately smaller than PostgreSQL.
//!
//! Seeded rows support API mapping, state transitions and fault injection. This
//! fixture does not model SQL transactions, durable change delivery, retention or
//! distributed ownership: those contracts remain covered by real Postgres tests.
#![allow(dead_code)]

use control_plane::InstanceState as DomainInstanceState;
use control_plane::{materialization::*, *};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

#[derive(Default)]
pub struct TestStore {
    instances: Mutex<BTreeMap<String, InstanceRecord>>,
    workload_classes: Mutex<BTreeMap<(String, u64), WorkloadClassVersion>>,
    route_bindings: Mutex<BTreeMap<String, RouteBindingRecord>>,
    materializations: Mutex<Vec<MaterializationRecord>>,
    route_resolution: Mutex<Option<FakeRouteResolution>>,
    resolve_route_requests: Mutex<usize>,
    resolve_gate: Mutex<Option<Arc<Notify>>>,
    fail_ready_materialization_lookup: bool,
    get_error: Mutex<Option<&'static str>>,
    transition_requests: Mutex<Vec<CompareAndSwapInstanceStateRequest>>,
    delete_requests: Mutex<Vec<DeleteInstanceRequest>>,
    http01: Mutex<BTreeMap<(String, String), Http01ChallengeRecord>>,
    reconciliation_claim_owners: Mutex<Vec<String>>,
    sleep_requests: Mutex<Vec<BeginSleepRequest>>,
    defer_sleep_for: Mutex<Option<Duration>>,
    ready_age: Mutex<Option<Duration>>,
}

impl TestStore {
    pub fn sleep_requests(&self) -> Vec<BeginSleepRequest> {
        self.sleep_requests.lock().unwrap().clone()
    }
    /// Explicit transport fault only; this fixture does not model DB clock age.
    pub fn defer_sleep_for(&self, duration: Duration) {
        *self.defer_sleep_for.lock().unwrap() = Some(duration);
    }
    pub fn set_ready_age(&self, duration: Duration) {
        *self.ready_age.lock().unwrap() = Some(duration);
    }
    pub fn with_workload_class(workload: WorkloadClassVersion) -> Self {
        let store = Self::default();
        store.seed_workload_class(workload);
        store
    }
    pub fn failing_ready_lookup() -> Self {
        Self {
            fail_ready_materialization_lookup: true,
            ..Self::default()
        }
    }
    pub fn set_resolve_gate(&self, gate: Arc<Notify>) {
        *self.resolve_gate.lock().unwrap() = Some(gate);
    }
    pub fn seed_instance(&self, instance: InstanceRecord) {
        self.instances
            .lock()
            .unwrap()
            .insert(instance.id.as_str().to_owned(), instance);
    }
    pub fn seed_workload_class(&self, workload: WorkloadClassVersion) {
        self.workload_classes.lock().unwrap().insert(
            (
                workload.reference.class_id.as_str().to_owned(),
                workload.reference.version.get(),
            ),
            workload,
        );
    }
    pub fn set_replicas(&self, replicas: Option<u32>) {
        for workload in self.workload_classes.lock().unwrap().values_mut() {
            workload.template.workload.replicas = replicas;
        }
    }
    pub fn seed_materialization(&self, record: MaterializationRecord) {
        let mut records = self.materializations.lock().unwrap();
        records.retain(|old| old.instance_id != record.instance_id || old.target != record.target);
        records.push(record);
    }
    pub fn seed_ready_materialization(&self, record: MaterializationRecord) {
        self.seed_materialization(record);
    }
    pub fn seed_route_resolved(&self, matched_identity: RouteIdentity, entry: RouteEntry) {
        self.route_bindings.lock().unwrap().insert(
            entry.route_binding_id.as_str().to_owned(),
            RouteBindingRecord {
                id: entry.route_binding_id.clone(),
                instance_id: entry.instance_id.clone(),
                protocol: protocol_for_identity(&matched_identity),
                identity: matched_identity.clone(),
            },
        );
        *self.route_resolution.lock().unwrap() = Some(FakeRouteResolution::Resolved {
            matched_identity,
            entry,
        });
    }
    pub fn seed_route_miss(&self, ttl: Duration) {
        *self.route_resolution.lock().unwrap() = Some(FakeRouteResolution::Miss {
            negative_cache: CachePolicy::new(ttl),
        });
    }
    pub fn seed_route_error_unavailable(&self, message: &str) {
        *self.route_resolution.lock().unwrap() =
            Some(FakeRouteResolution::Unavailable(message.into()));
    }
    pub fn resolve_route_requests(&self) -> usize {
        *self.resolve_route_requests.lock().unwrap()
    }
    pub fn fail_get_with(&self, error: FakeStoreError) {
        let FakeStoreError::Unavailable(message) = error;
        *self.get_error.lock().unwrap() = Some(message);
    }
    pub fn instance(&self) -> InstanceRecord {
        self.instances
            .lock()
            .unwrap()
            .values()
            .next()
            .expect("seeded instance")
            .clone()
    }
    pub fn instance_exists(&self, id: &str) -> bool {
        self.instances.lock().unwrap().contains_key(id)
    }
    pub fn materialization(&self) -> Option<MaterializationRecord> {
        self.materializations.lock().unwrap().first().cloned()
    }
    pub fn materializations(&self) -> Vec<MaterializationRecord> {
        self.materializations.lock().unwrap().clone()
    }
    pub fn transition_requests(&self) -> Vec<CompareAndSwapInstanceStateRequest> {
        self.transition_requests.lock().unwrap().clone()
    }
    pub fn delete_requests(&self) -> Vec<DeleteInstanceRequest> {
        self.delete_requests.lock().unwrap().clone()
    }
    pub fn reconciliation_claim_owners(&self) -> Vec<String> {
        self.reconciliation_claim_owners.lock().unwrap().clone()
    }

    fn transition_instance(
        &self,
        instance_id: InstanceId,
        expected_generation: Generation,
        next_state: DomainInstanceState,
        reason: StateTransitionReason,
    ) -> StoreResult<InstanceRecord> {
        let mut instances = self.instances.lock().unwrap();
        let instance = instances
            .get_mut(instance_id.as_str())
            .ok_or(StoreError::NotFound {
                resource: "instance",
            })?;
        if instance.generation != expected_generation {
            return Err(StoreError::GenerationConflict {
                expected: expected_generation,
                actual: instance.generation,
            });
        }
        control_plane::instance::validate_instance_state_transition(
            instance.state,
            next_state,
            &reason,
        )
        .map_err(|error| StoreError::invalid_argument(error.to_string()))?;
        self.transition_requests
            .lock()
            .unwrap()
            .push(CompareAndSwapInstanceStateRequest {
                instance_id,
                expected_generation,
                next_state,
                reason,
            });
        instance.state = next_state;
        instance.generation = expected_generation.next();
        Ok(instance.clone())
    }
    fn mark_materialization(
        &self,
        instance_id: InstanceId,
        target: MaterializationTarget,
        state: MaterializationState,
        instance_generation: Option<Generation>,
        rendered_objects: Option<Vec<RenderedObjectRef>>,
    ) -> Option<MaterializationRecord> {
        let mut records = self.materializations.lock().unwrap();
        let record = records.iter_mut().find(|record| {
            record.instance_id == instance_id
                && record.target == target
                && record.state != MaterializationState::Deleted
        })?;
        record.state = state;
        record.backend = None;
        if let Some(generation) = instance_generation {
            record.instance_generation = generation;
        }
        if let Some(objects) = rendered_objects {
            record.rendered_objects = objects;
        }
        if state == MaterializationState::Deleted {
            record.exclusivity_keys.clear();
            record.reconciliation_lease = None;
        }
        Some(record.clone())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FakeStoreError {
    Unavailable(&'static str),
}
#[derive(Clone, Debug)]
enum FakeRouteResolution {
    Resolved {
        matched_identity: RouteIdentity,
        entry: RouteEntry,
    },
    Miss {
        negative_cache: CachePolicy,
    },
    Unavailable(String),
}

fn protocol_for_identity(identity: &RouteIdentity) -> ProtocolRoute {
    match identity {
        RouteIdentity::Http { .. } => ProtocolRoute::Http,
        RouteIdentity::Sni { .. } => ProtocolRoute::TlsSni,
    }
}
fn fake_materialization_record(request: RecordMaterializationRequest) -> MaterializationRecord {
    MaterializationRecord {
        id: MaterializationId::new(format!(
            "{}:{}:{}",
            request.instance_id,
            request.target.cluster_id(),
            request.target.namespace()
        ))
        .unwrap(),
        instance_id: request.instance_id,
        instance_generation: request.instance_generation,
        projection_generation: request.projection_generation,
        target: request.target,
        state: request.state,
        backend: request.backend,
        backend_generation: request.backend_generation,
        rendered_objects: request.rendered_objects,
        exclusivity_keys: request.exclusivity_keys,
        reconciliation_lease: None,
    }
}

impl ControlPlaneStore for TestStore {
    // Transport fixtures do not pretend to supply persistence/runtime semantics.
    unexpected_store_methods!(
        publish_certificate,
        get_certificate_metadata,
        set_tls_binding,
        get_tls_binding,
        remove_certificate,
        resolve_tls_certificate,
        reencrypt_certificate,
        snapshot_tls_bindings,
        load_route_changes,
        load_route_change_revision,
        maintain_runtime_records,
        load_materialization_operational_metrics,
        lookup_route_dependencies
    );
    fn create_instance<'a>(
        &'a self,
        request: control_plane::CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        Box::pin(async move {
            let instance = InstanceRecord {
                id: request.instance_id,
                workload_class: request.workload_class,
                values: request.values,
                state: DomainInstanceState::Cold,
                generation: Generation::new(0),
            };
            self.instances
                .lock()
                .expect("fake store lock is available")
                .insert(instance.id.as_str().to_owned(), instance.clone());

            Ok(CreateInstanceResult {
                instance,
                route_bindings: Vec::new(),
                idempotency_replayed: false,
            })
        })
    }

    fn get_instance<'a>(
        &'a self,
        request: control_plane::GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move {
            if let Some(message) = *self.get_error.lock().unwrap() {
                return Err(StoreError::unavailable(message));
            }
            Ok(self
                .instances
                .lock()
                .expect("fake store lock is available")
                .get(request.instance_id.as_str())
                .cloned())
        })
    }

    fn create_workload_class_version<'a>(
        &'a self,
        request: control_plane::CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::WorkloadClassVersion>> {
        Box::pin(async move {
            let workload_class = request.workload_class_version;
            let key = (
                workload_class.reference.class_id.as_str().to_owned(),
                workload_class.reference.version.get(),
            );
            self.workload_classes
                .lock()
                .expect("fake store lock is available")
                .insert(key, workload_class.clone());

            Ok(workload_class)
        })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: control_plane::LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::WorkloadClassVersion>>> {
        Box::pin(async move {
            let key = (
                request.reference.class_id.as_str().to_owned(),
                request.reference.version.get(),
            );
            Ok(self
                .workload_classes
                .lock()
                .expect("fake store lock is available")
                .get(&key)
                .cloned())
        })
    }

    fn create_route_binding<'a>(
        &'a self,
        request: control_plane::CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteBindingRecord>> {
        Box::pin(async move {
            let record = control_plane::RouteBindingRecord {
                id: request.route_binding_id,
                instance_id: request.instance_id,
                identity: request.identity,
                protocol: request.protocol,
            };
            self.route_bindings
                .lock()
                .expect("fake store lock is available")
                .insert(record.id.as_str().to_owned(), record.clone());

            Ok(record)
        })
    }

    fn get_route_binding<'a>(
        &'a self,
        request: control_plane::GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::RouteBindingRecord>>> {
        Box::pin(async move {
            Ok(self
                .route_bindings
                .lock()
                .expect("fake store lock is available")
                .get(request.route_binding_id.as_str())
                .cloned())
        })
    }

    fn delete_route_binding<'a>(
        &'a self,
        request: control_plane::DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            Ok(self
                .route_bindings
                .lock()
                .expect("fake store lock is available")
                .remove(request.route_binding_id.as_str())
                .is_some())
        })
    }

    fn list_route_bindings_for_instance<'a>(
        &'a self,
        request: control_plane::ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<control_plane::RouteBindingRecord>>> {
        Box::pin(async move {
            Ok(self
                .route_bindings
                .lock()
                .expect("fake store lock is available")
                .values()
                .filter(|route_binding| route_binding.instance_id == request.instance_id)
                .cloned()
                .collect())
        })
    }

    fn put_http01_challenge<'a>(
        &'a self,
        request: control_plane::PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::Http01ChallengeRecord>> {
        Box::pin(async move {
            let record = control_plane::Http01ChallengeRecord::new(
                request.key().clone(),
                request.key_authorization().to_owned(),
                request.expires_at(),
                UNIX_EPOCH,
            )
            .expect("service parsed a valid HTTP-01 challenge");
            let key = (
                record.key().host().as_str().to_owned(),
                record.key().token().to_owned(),
            );
            self.http01
                .lock()
                .expect("fake store lock is available")
                .insert(key, record.clone());

            Ok(record)
        })
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: control_plane::Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::Http01ChallengeRecord>>> {
        Box::pin(async move {
            Ok(self
                .http01
                .lock()
                .expect("fake store lock is available")
                .get(&(key.host().as_str().to_owned(), key.token().to_owned()))
                .cloned()
                .filter(|record| record.expires_at() > SystemTime::now()))
        })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        request: control_plane::DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            Ok(self
                .http01
                .lock()
                .expect("fake store lock is available")
                .remove(&(
                    request.key().host().as_str().to_owned(),
                    request.key().token().to_owned(),
                ))
                .is_some())
        })
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        request: control_plane::ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        Box::pin(async move {
            let mut records = self.http01.lock().expect("fake store lock is available");
            let expired_keys = records
                .iter()
                .filter(|(_, record)| record.expires_at() <= request.now)
                .take(request.limit.unwrap_or(usize::MAX))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let expired = expired_keys.len();
            for key in expired_keys {
                records.remove(&key);
            }

            Ok(expired)
        })
    }

    fn record_materialization<'a>(
        &'a self,
        request: control_plane::RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::MaterializationRecord>> {
        Box::pin(async move {
            let record = fake_materialization_record(request);
            let mut materializations = self
                .materializations
                .lock()
                .expect("fake store lock is available");
            materializations.retain(|existing| {
                !(existing.instance_id == record.instance_id && existing.target == record.target)
            });
            materializations.push(record.clone());

            Ok(record)
        })
    }

    fn load_ready_materialization<'a>(
        &'a self,
        request: control_plane::materialization::LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async move {
            if self.fail_ready_materialization_lookup {
                return Err(StoreError::unavailable("separate backend lookup failed"));
            }
            Ok(self
                .materializations
                .lock()
                .expect("fake store lock is available")
                .iter()
                .find(|materialization| {
                    materialization.instance_id == request.instance_id
                        && materialization.instance_generation == request.instance_generation
                        && materialization.target == request.target
                        && materialization.state == MaterializationState::Ready
                })
                .cloned())
        })
    }

    fn load_active_materialization<'a>(
        &'a self,
        request: control_plane::LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .materializations
                .lock()
                .expect("fake store lock is available")
                .iter()
                .find(|materialization| {
                    materialization.instance_id == request.instance_id
                        && materialization.target == request.target
                        && materialization.state != MaterializationState::Deleted
                })
                .cloned())
        })
    }

    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        request: control_plane::ListMaterializationReconciliationCandidatesRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .materializations
                .lock()
                .unwrap()
                .iter()
                .filter(|record| {
                    matches!(
                        record.state,
                        MaterializationState::Pending | MaterializationState::Deleting
                    ) && request
                        .target
                        .as_ref()
                        .is_none_or(|target| &record.target == target)
                })
                .take(request.limit)
                .cloned()
                .collect())
        })
    }

    fn resolve_route<'a>(
        &'a self,
        request: control_plane::ResolveRouteRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteResolution>> {
        Box::pin(async move {
            *self
                .resolve_route_requests
                .lock()
                .expect("fake store lock is available") += 1;
            let gate = self.resolve_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            match self
                .route_resolution
                .lock()
                .expect("fake store lock is available")
                .clone()
            {
                Some(FakeRouteResolution::Resolved {
                    matched_identity,
                    mut entry,
                }) => {
                    // The store contract returns the selected target's backend
                    // as part of route resolution, independent of a later read.
                    entry.backend = None;
                    entry.backend_generation = None;
                    if let Some(materialization) = self
                        .materializations
                        .lock()
                        .expect("fake store lock is available")
                        .iter()
                        .find(|materialization| {
                            materialization.instance_id == entry.instance_id
                                && materialization.instance_generation == entry.instance_generation
                                && materialization.target == request.target
                                && materialization.state == MaterializationState::Ready
                        })
                    {
                        entry.backend = materialization.backend.clone();
                        entry.backend_generation = entry
                            .backend
                            .as_ref()
                            .map(|_| materialization.backend_generation);
                    }
                    Ok(RouteResolution::Resolved {
                        matched_identity,
                        entry,
                    })
                }
                Some(FakeRouteResolution::Miss { negative_cache }) => {
                    Ok(RouteResolution::Miss { negative_cache })
                }
                Some(FakeRouteResolution::Unavailable(message)) => {
                    Err(StoreError::unavailable(message))
                }
                None => Err(StoreError::internal("fake store method is not implemented")),
            }
        })
    }

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        Box::pin(async move {
            self.sleep_requests.lock().unwrap().push(request.clone());
            if let Some(retry_after) = *self.defer_sleep_for.lock().unwrap() {
                return Err(StoreError::SleepDeferred { retry_after });
            }

            let updated = self.transition_instance(
                request.instance_id.clone(),
                request.expected_running_generation,
                DomainInstanceState::Draining,
                StateTransitionReason::IdleReported,
            )?;
            let materialization = self.mark_materialization(
                request.instance_id,
                request.target,
                MaterializationState::Deleting,
                None,
                None,
            );

            Ok(BeginSleepResult {
                instance: updated,
                materialization,
            })
        })
    }

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        Box::pin(async move {
            let updated = self.transition_instance(
                request.instance_id.clone(),
                request.expected_draining_generation,
                DomainInstanceState::Cold,
                StateTransitionReason::DrainCompleted,
            )?;
            let materialization = self.mark_materialization(
                request.instance_id,
                request.target,
                MaterializationState::Deleted,
                Some(updated.generation),
                Some(Vec::new()),
            );

            Ok(FinalizeSleepResult {
                instance: updated,
                materialization,
            })
        })
    }
    fn load_materialization_work_status(
        &self,
        id: control_plane::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<Option<control_plane::runtime_work::MaterializationWorkStatus>>>
    {
        let _ = id;
        Box::pin(async {
            Ok(self.ready_age.lock().unwrap().map(|age| {
                control_plane::runtime_work::MaterializationWorkStatus {
                    ready_age: Some(age),
                    ..Default::default()
                }
            }))
        })
    }

    fn record_materialization_failure(
        &self,
        request: control_plane::runtime_work::RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.release_materialization_reconciliation_lease(
            ReleaseMaterializationReconciliationLeaseRequest::new(
                request.materialization_id,
                request.owner,
                request.attempt,
                request.generation,
            ),
        )
    }

    fn enqueue_materialization(
        &self,
        id: control_plane::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        Box::pin(async move {
            Ok(self.materializations.lock().unwrap().iter().any(|record| {
                record.id == id
                    && matches!(
                        record.state,
                        MaterializationState::Pending | MaterializationState::Deleting
                    )
                    && record.reconciliation_lease.is_none()
            }))
        })
    }

    fn accept_wake<'a>(
        &'a self,
        request: control_plane::materialization::AcceptWakeRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async move {
            let instance = self.transition_instance(
                request.pending.instance_id.clone(),
                request.expected_generation,
                DomainInstanceState::Waking,
                StateTransitionReason::WakeRequested,
            )?;
            assert_eq!(instance.generation, request.pending.instance_generation);
            self.seed_materialization(fake_materialization_record(request.pending));
            Ok(instance)
        })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async move {
            self.transition_instance(
                request.instance_id,
                request.expected_generation,
                request.next_state,
                request.reason,
            )
        })
    }

    fn request_instance_deletion<'a>(
        &'a self,
        request: control_plane::instance::RequestInstanceDeletion,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let mut instances = self.instances.lock().unwrap();
            let Some(instance) = instances.get_mut(request.instance_id.as_str()) else {
                return Ok(false);
            };
            if instance.generation != request.expected_generation
                && !(instance.state == DomainInstanceState::Deleting
                    && instance.generation == request.expected_generation.next())
            {
                return Err(StoreError::GenerationConflict {
                    expected: request.expected_generation,
                    actual: instance.generation,
                });
            }
            if instance.state != DomainInstanceState::Deleting {
                instance.state = DomainInstanceState::Deleting;
                instance.generation = instance.generation.next();
            }
            for record in self
                .materializations
                .lock()
                .unwrap()
                .iter_mut()
                .filter(|record| {
                    record.instance_id == request.instance_id
                        && record.state != MaterializationState::Deleted
                })
            {
                record.state = MaterializationState::Deleting;
                record.backend = None;
                record.reconciliation_lease = None;
            }
            Ok(true)
        })
    }

    fn finalize_instance_deletions<'a>(
        &'a self,
        limit: usize,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        Box::pin(async move {
            let candidates: Vec<_> = {
                let instances = self.instances.lock().unwrap();
                let records = self.materializations.lock().unwrap();
                instances
                    .values()
                    .filter(|instance| {
                        instance.state == DomainInstanceState::Deleting
                            && !records.iter().any(|record| {
                                record.instance_id == instance.id
                                    && record.state != MaterializationState::Deleted
                            })
                    })
                    .take(limit)
                    .map(|instance| instance.id.clone())
                    .collect()
            };
            let mut count = 0;
            for instance_id in candidates {
                count += usize::from(
                    self.delete_instance(DeleteInstanceRequest::new(instance_id))
                        .await?,
                );
            }
            Ok(count)
        })
    }

    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            self.delete_requests.lock().unwrap().push(request.clone());
            let mut instances = self.instances.lock().unwrap();
            let Some(instance) = instances.get(request.instance_id.as_str()) else {
                return Ok(false);
            };
            if instance.state != DomainInstanceState::Deleting {
                return Err(StoreError::invalid_argument(
                    "hard delete requires deleting state",
                ));
            }
            let mut records = self.materializations.lock().unwrap();
            if records.iter().any(|record| {
                record.instance_id == request.instance_id
                    && record.state != MaterializationState::Deleted
            }) {
                return Err(StoreError::invalid_argument(
                    "hard delete requires completed cleanup",
                ));
            }
            instances.remove(request.instance_id.as_str());
            records.retain(|record| record.instance_id != request.instance_id);
            self.route_bindings
                .lock()
                .unwrap()
                .retain(|_, record| record.instance_id != request.instance_id);
            Ok(true)
        })
    }

    fn load_materialization<'a>(
        &'a self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .materializations
                .lock()
                .unwrap()
                .iter()
                .find(|record| record.id == request.materialization_id)
                .cloned())
        })
    }

    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut records = self.materializations.lock().unwrap();
            let Some(record) = records
                .iter_mut()
                .find(|record| record.id == request.materialization_id)
            else {
                return Ok(None);
            };
            // A store compares stored expiry against its own clock.
            let now = SystemTime::now();
            if record
                .reconciliation_lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at > now)
            {
                return Ok(None);
            }
            self.reconciliation_claim_owners
                .lock()
                .unwrap()
                .push(request.owner.clone());
            let attempt = record
                .reconciliation_lease
                .as_ref()
                .map_or(1, |lease| lease.attempt + 1);
            record.reconciliation_lease = Some(MaterializationReconciliationLease {
                owner: request.owner,
                expires_at: now + request.lease_ttl,
                attempt,
            });
            Ok(Some(record.clone()))
        })
    }

    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let mut records = self.materializations.lock().unwrap();
            let Some(record) = records.iter_mut().find(|record| {
                record.id == request.materialization_id
                    && record.instance_generation == request.instance_generation
            }) else {
                return Ok(false);
            };
            let Some(lease) = record
                .reconciliation_lease
                .as_mut()
                .filter(|lease| lease.owner == request.owner && lease.attempt == request.attempt)
            else {
                return Ok(false);
            };
            lease.expires_at = SystemTime::now() + request.lease_ttl;
            Ok(true)
        })
    }

    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let mut records = self.materializations.lock().unwrap();
            let Some(record) = records.iter_mut().find(|record| {
                record.id == request.materialization_id
                    && record.instance_generation == request.instance_generation
                    && record.reconciliation_lease.as_ref().is_some_and(|lease| {
                        lease.owner == request.owner && lease.attempt == request.attempt
                    })
            }) else {
                return Ok(false);
            };
            record.reconciliation_lease = None;
            Ok(true)
        })
    }

    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        Box::pin(async move {
            let instance = self.transition_instance(
                request.instance_id.clone(),
                request.expected_waking_generation,
                DomainInstanceState::Running,
                StateTransitionReason::MaterializationReady,
            )?;
            let projection_generation = self
                .materializations
                .lock()
                .unwrap()
                .iter()
                .find(|record| {
                    record.instance_id == request.instance_id && record.target == request.target
                })
                .map_or(instance.generation, |record| record.projection_generation);
            let mut materialization = fake_materialization_record(RecordMaterializationRequest {
                instance_id: request.instance_id,
                instance_generation: instance.generation,
                projection_generation,
                target: request.target,
                state: MaterializationState::Ready,
                backend: Some(request.backend),
                backend_generation: request.backend_generation,
                rendered_objects: request.rendered_objects,
                exclusivity_keys: request.exclusivity_keys,
            });
            materialization.projection_generation = projection_generation;
            self.seed_materialization(materialization.clone());
            Ok(CompleteWakeResult {
                instance,
                materialization,
            })
        })
    }

    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        self.complete_wake(request.complete)
    }

    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        self.finalize_sleep(request.finalize)
    }

    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut records = self.materializations.lock().unwrap();
            let Some(record) = records.iter_mut().find(|record| {
                record.id == request.materialization_id
                    && record.instance_generation == request.instance_generation
                    && record.reconciliation_lease.as_ref().is_some_and(|lease| {
                        lease.owner == request.lease_owner && lease.attempt == request.attempt
                    })
            }) else {
                return Ok(None);
            };
            record.state = MaterializationState::Deleted;
            record.backend = None;
            record.rendered_objects.clear();
            record.exclusivity_keys.clear();
            record.reconciliation_lease = None;
            Ok(Some(record.clone()))
        })
    }

    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut records = self.materializations.lock().unwrap();
            let Some(record) = records
                .iter_mut()
                .find(|record| record.id == request.materialization_id)
            else {
                return Ok(None);
            };
            let previous = record.clone();
            record.state = MaterializationState::Deleted;
            record.backend = None;
            record.rendered_objects.clear();
            record.exclusivity_keys.clear();
            record.reconciliation_lease = None;
            Ok(Some(previous))
        })
    }

    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>> {
        Box::pin(async move {
            let mut affected = Vec::new();
            for record in self
                .materializations
                .lock()
                .unwrap()
                .iter_mut()
                .filter(|record| {
                    record.target == request.target
                        && record.state != MaterializationState::Deleted
                        && record.exclusivity_keys.iter().any(|key| {
                            key.name == request.key_name && key.value == request.key_value
                        })
                })
            {
                affected.push(record.clone());
                record
                    .exclusivity_keys
                    .retain(|key| key.name != request.key_name || key.value != request.key_value);
            }
            Ok(ForceReleaseExclusivityKeyResult {
                updated_materializations: affected.len(),
                affected_materializations: affected,
            })
        })
    }

    fn begin_materialization_effect<'a>(
        &'a self,
        request: control_plane::materialization::MaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        let _ = request; // Durable effect ambiguity is exercised against PostgreSQL, not this transport fixture.
        Box::pin(async { Ok(true) })
    }

    fn acknowledge_materialization_effect<'a>(
        &'a self,
        request: control_plane::materialization::AcknowledgeMaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        let _ = request;
        Box::pin(async { Ok(true) })
    }
}

pub fn workload_class() -> control_plane::WorkloadClassVersion {
    control_plane::WorkloadClassVersion {
        reference: control_plane::WorkloadClassVersionRef::new(
            control_plane::WorkloadClassId::new("class-1").expect("class id is valid"),
            Generation::new(1),
        ),
        template_generation: Generation::new(1),
        template: control_plane::ManifestTemplate {
            workload: control_plane::WorkloadTemplate {
                kind: control_plane::WorkloadKind::Deployment,
                name: control_plane::TemplateText::literal("app"),
                replicas: None,
                app_container: control_plane::ContainerTemplate {
                    name: "app".to_owned(),
                    image: control_plane::TemplateText::literal("example/app:1"),
                    ports: vec![control_plane::ContainerPortTemplate {
                        name: Some("http".to_owned()),
                        container_port: 8080,
                    }],
                    env: Vec::new(),
                },
            },
            service: Some(control_plane::ServiceTemplate {
                name: control_plane::TemplateText::literal("svc"),
                ports: vec![control_plane::ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            sidecar: control_plane::SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: control_plane::TemplateText::literal("sleepypods/sidecar:test"),
                listen_port: 15000,
                mode: None,
            },
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        },
        default_values: BTreeMap::new(),
        value_schema: control_plane::WorkloadValueSchema::new(true),
        sleep_policy: control_plane::WorkloadSleepPolicy::new(120_000, 5_000, 30_000)
            .expect("sleep policy is valid"),
        exclusivity_keys: Vec::new(),
    }
}

/// The same observable transport-facing lifecycle checks run against this fixture
/// and PostgreSQL. SQL concurrency, retention and fencing have separate DB gates.
pub async fn lifecycle_conformance(store: &dyn ControlPlaneStore) -> StoreResult<()> {
    let class = workload_class();
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let created = store
        .create_instance(CreateInstanceRequest::new(
            IdempotencyKey::new("shared-conformance").unwrap(),
            InstanceId::new("shared-conformance").unwrap(),
            class.reference,
        ))
        .await?
        .instance;
    let target = MaterializationTarget::new("shared-cluster", "apps").unwrap();
    let pending = RecordMaterializationRequest::new(
        created.id.clone(),
        created.generation.next(),
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    let waking = store
        .accept_wake(AcceptWakeRequest {
            expected_generation: created.generation,
            pending: pending.clone(),
        })
        .await?;
    assert_eq!(waking.state, InstanceState::Waking);
    assert_eq!(waking.generation, created.generation.next());
    assert!(matches!(
        store
            .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
                created.id.clone(),
                created.generation,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested
            ))
            .await,
        Err(StoreError::GenerationConflict { .. })
    ));
    let ready = store
        .complete_wake(CompleteWakeRequest::new(
            created.id.clone(),
            waking.generation,
            target.clone(),
            BackendEndpoint::new("http://shared.example:8080").unwrap(),
            BackendGeneration::new(2),
        ))
        .await?;
    assert_eq!(ready.instance.state, InstanceState::Running);
    assert_eq!(
        ready.materialization.projection_generation,
        pending.projection_generation
    );
    assert_eq!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                created.id.clone(),
                ready.instance.generation,
                target.clone()
            ))
            .await?,
        Some(ready.materialization.clone())
    );
    assert!(store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            created.id.clone(),
            MaterializationTarget::new("other-cluster", "apps").unwrap()
        ))
        .await?
        .is_none());
    let draining = store
        .begin_sleep(BeginSleepRequest::new(
            created.id.clone(),
            ready.instance.generation,
            target.clone(),
        ))
        .await?;
    assert_eq!(draining.instance.state, InstanceState::Draining);
    assert!(draining.materialization.as_ref().unwrap().backend.is_none());
    assert_eq!(
        draining
            .materialization
            .as_ref()
            .unwrap()
            .projection_generation,
        pending.projection_generation
    );
    let cold = store
        .finalize_sleep(FinalizeSleepRequest::new(
            created.id.clone(),
            draining.instance.generation,
            target,
        ))
        .await?;
    assert_eq!(cold.instance.state, InstanceState::Cold);
    assert!(cold.materialization.unwrap().rendered_objects.is_empty());
    assert!(
        store
            .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                instance_id: created.id.clone(),
                expected_generation: cold.instance.generation
            })
            .await?
    );
    assert_eq!(store.finalize_instance_deletions(1).await?, 1);
    assert!(store
        .get_instance(GetInstanceRequest::new(created.id))
        .await?
        .is_none());
    Ok(())
}
