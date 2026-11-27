//! Shared transport fixture, deliberately smaller than PostgreSQL.
//!
//! Seeded rows support API mapping, state transitions and fault injection. This
//! fixture does not model SQL transactions, change-history retention or
//! distributed ownership: those contracts remain covered by real Postgres tests.
//! Its route-change log holds what a test records, plus route binding writes.
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
    route_entries: Mutex<BTreeMap<String, RouteEntry>>,
    route_error: Mutex<Option<String>>,
    resolve_route_requests: Mutex<usize>,
    resolve_gate: Mutex<Option<Arc<Notify>>>,
    route_changes: Mutex<Vec<serde_json::Value>>,
    route_change_read_failures: Mutex<usize>,
    route_change_signal: Arc<Notify>,
    fail_ready_materialization_lookup: bool,
    get_error: Mutex<Option<&'static str>>,
    transition_requests: Mutex<Vec<CompareAndSwapInstanceStateRequest>>,
    delete_requests: Mutex<Vec<DeleteInstanceRequest>>,
    http01: Mutex<BTreeMap<(String, String), Http01ChallengeRecord>>,
    reconciliation_claim_owners: Mutex<Vec<String>>,
    sleep_requests: Mutex<Vec<BeginSleepRequest>>,
    defer_sleep_for: Mutex<Option<Duration>>,
    ready_age: Mutex<Option<Duration>>,
    /// Materializations whose last attempt failed. A store schedules their
    /// next attempt after a backoff; this fixture never reaches it.
    failed_attempts: Mutex<Vec<control_plane::ids::MaterializationId>>,
}

/// A reconciler configuration whose periodic scan suits a test's patience.
pub fn reconciler_config() -> control_plane::MaterializationReconcilerConfig {
    control_plane::MaterializationReconcilerConfig {
        interval: Duration::from_millis(20),
        ..Default::default()
    }
}

/// Runs the production reconciler loop until `done` holds, then stops it.
pub async fn reconcile_until<C, Done, DoneFuture>(
    reconciler: control_plane::MaterializationReconciler<C>,
    mut done: Done,
) where
    C: control_plane::KubernetesMaterializerClient + Clone + 'static,
    Done: FnMut() -> DoneFuture,
    DoneFuture: std::future::Future<Output = bool>,
{
    let (shutdown, stopped) = tokio::sync::watch::channel(false);
    let driver = tokio::spawn(reconciler.run_until_shutdown(stopped));
    let reached = tokio::time::timeout(Duration::from_secs(60), async {
        while !done().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    shutdown.send_replace(true);
    driver
        .await
        .expect("reconciler task joins")
        .expect("reconciler stops cleanly");
    reached.expect("reconciler reached the expected state");
}

impl TestStore {
    /// Makes every failed attempt due again, as an elapsed backoff does.
    pub fn retry_failed_attempts(&self) {
        self.failed_attempts.lock().unwrap().clear();
    }
    /// True once the reconciler for `target` has nothing left to start: every
    /// wake and cleanup has completed or recorded a failed attempt, no lease
    /// is held, and every instance deletion without cleanup is finalized.
    pub fn lifecycle_settled(&self, target: &MaterializationTarget) -> bool {
        let failed = self.failed_attempts.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let records = self.materializations.lock().unwrap();
        let work_settled = records
            .iter()
            .filter(|record| &record.target == target)
            .all(|record| {
                record.reconciliation_lease.is_none()
                    && (!matches!(
                        record.state,
                        MaterializationState::Pending | MaterializationState::Deleting
                    ) || failed.contains(&record.id))
            });
        let deletions_finalized = instances.values().all(|instance| {
            instance.state != DomainInstanceState::Deleting
                || records.iter().any(|record| {
                    record.instance_id == instance.id
                        && record.state != MaterializationState::Deleted
                })
        });
        work_settled && deletions_finalized
    }
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
    /// Appends an instance change to the route-change log and wakes the dispatcher.
    pub fn record_instance_change(&self, instance_id: &str) {
        self.record_route_change(
            serde_json::json!({"kind": "instance", "instance_id": instance_id}),
        );
    }
    pub fn record_route_change(&self, payload: serde_json::Value) {
        self.route_changes.lock().unwrap().push(payload);
        self.route_change_signal.notify_one();
    }
    /// Appends without waking, as a store with no push notification would.
    pub fn record_instance_change_quietly(&self, instance_id: &str) {
        self.route_changes
            .lock()
            .unwrap()
            .push(serde_json::json!({"kind": "instance", "instance_id": instance_id}));
    }
    pub fn fail_route_change_reads(&self, count: usize) {
        *self.route_change_read_failures.lock().unwrap() = count;
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
    /// Adds or replaces one binding. Its instance state and generation come
    /// from `entry` until an instance with that ID is seeded; its backend
    /// always comes from the target's ready materialization.
    pub fn seed_route_resolved(&self, identity: RouteIdentity, entry: RouteEntry) {
        self.route_bindings.lock().unwrap().insert(
            entry.route_binding_id.as_str().to_owned(),
            RouteBindingRecord {
                id: entry.route_binding_id.clone(),
                instance_id: entry.instance_id.clone(),
                identity,
            },
        );
        self.route_entries
            .lock()
            .unwrap()
            .insert(entry.route_binding_id.as_str().to_owned(), entry);
        *self.route_error.lock().unwrap() = None;
    }
    pub fn remove_route(&self, route_binding_id: &str) {
        self.route_bindings.lock().unwrap().remove(route_binding_id);
        self.route_entries.lock().unwrap().remove(route_binding_id);
    }
    pub fn clear_routes(&self) {
        self.route_bindings.lock().unwrap().clear();
        self.route_entries.lock().unwrap().clear();
        *self.route_error.lock().unwrap() = None;
    }
    pub fn seed_route_error_unavailable(&self, message: &str) {
        *self.route_error.lock().unwrap() = Some(message.into());
    }
    pub fn clear_route_error(&self) {
        *self.route_error.lock().unwrap() = None;
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
        self.record_instance_change(instance.id.as_str());
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
        if state == MaterializationState::Deleted {
            record.backend = None;
        }
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
        self.record_instance_change(record.instance_id.as_str());
        Some(record.clone())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FakeStoreError {
    Unavailable(&'static str),
}
fn fake_materialization_record(request: RecordMaterializationRequest) -> MaterializationRecord {
    MaterializationRecord {
        pod_backend: None,
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

/// The payload the Postgres route-change trigger writes for a route binding.
pub fn route_change_payload(record: &RouteBindingRecord, removed: bool) -> serde_json::Value {
    let (identity_kind, host, path_prefix) = match &record.identity {
        RouteIdentity::Http { host, path } => {
            ("http", host, path.as_ref().map(|path| path.as_str()))
        }
        RouteIdentity::Sni { host } => ("sni", host, None),
    };
    serde_json::json!({
        "kind": "route",
        "removed": removed,
        "route_binding_id": record.id.as_str(),
        "instance_id": record.instance_id.as_str(),
        "identity_kind": identity_kind,
        "host_kind": if host.is_wildcard() { "wildcard_suffix" } else { "exact" },
        "host": host.as_str(),
        "path_prefix": path_prefix,
    })
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
        reencrypt_certificates_sealed_by,
        snapshot_tls_bindings,
        maintain_runtime_records,
        load_materialization_operational_metrics,
        lookup_route_dependencies
    );
    fn load_route_changes(
        &self,
        cursor: u64,
        limit: u32,
    ) -> StoreFuture<'_, StoreResult<control_plane::runtime_work::DurableRouteChanges>> {
        Box::pin(async move {
            let mut failures = self.route_change_read_failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                return Err(StoreError::unavailable(
                    "route change history is unavailable",
                ));
            }
            let changes = self.route_changes.lock().unwrap();
            let events: Vec<_> = changes
                .iter()
                .skip(cursor as usize)
                .take(limit as usize)
                .cloned()
                .collect();
            Ok(control_plane::runtime_work::DurableRouteChanges {
                cursor: cursor + events.len() as u64,
                reset: false,
                events,
            })
        })
    }
    fn load_route_change_revision(&self) -> StoreFuture<'_, StoreResult<u64>> {
        Box::pin(async move { Ok(self.route_changes.lock().unwrap().len() as u64) })
    }
    fn route_change_signal(&self) -> Arc<Notify> {
        self.route_change_signal.clone()
    }
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
            let route_bindings = request
                .route_bindings
                .into_iter()
                .map(|route| control_plane::RouteBindingRecord {
                    id: route.id,
                    instance_id: instance.id.clone(),
                    identity: route.identity,
                })
                .collect::<Vec<_>>();
            let mut stored = self
                .route_bindings
                .lock()
                .expect("fake store lock is available");
            for record in &route_bindings {
                stored.insert(record.id.as_str().to_owned(), record.clone());
            }

            Ok(CreateInstanceResult {
                instance,
                route_bindings,
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
            };
            self.route_bindings
                .lock()
                .expect("fake store lock is available")
                .insert(record.id.as_str().to_owned(), record.clone());
            self.record_route_change(route_change_payload(&record, false));

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
            let removed = self
                .route_bindings
                .lock()
                .expect("fake store lock is available")
                .remove(request.route_binding_id.as_str());
            if let Some(record) = &removed {
                self.record_route_change(route_change_payload(record, true));
            }
            Ok(removed.is_some())
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

    fn resolve_host_rules<'a>(
        &'a self,
        request: control_plane::ResolveHostRulesRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::HostRules>> {
        Box::pin(async move {
            *self
                .resolve_route_requests
                .lock()
                .expect("fake store lock is available") += 1;
            let gate = self.resolve_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            if let Some(message) = self.route_error.lock().unwrap().clone() {
                return Err(StoreError::unavailable(message));
            }
            let bindings = self.route_bindings.lock().unwrap();
            let entries = self.route_entries.lock().unwrap();
            let instances = self.instances.lock().unwrap();
            let materializations = self.materializations.lock().unwrap();
            let rules = bindings
                .values()
                .filter(|binding| {
                    binding
                        .identity
                        .applies_to(request.kind, request.host.as_str())
                })
                .map(|binding| {
                    let seeded = entries.get(binding.id.as_str());
                    let instance = instances.get(binding.instance_id.as_str());
                    let instance_state = instance
                        .map(|instance| instance.state)
                        .or(seeded.map(|entry| entry.instance_state))
                        .unwrap_or(DomainInstanceState::Cold);
                    let instance_generation = instance
                        .map(|instance| instance.generation)
                        .or(seeded.map(|entry| entry.instance_generation))
                        .unwrap_or(Generation::new(1));
                    // The backend is the target's ready materialization for
                    // the instance's current generation, as in the store query.
                    let ready = materializations.iter().find(|materialization| {
                        materialization.instance_id == binding.instance_id
                            && materialization.instance_generation == instance_generation
                            && materialization.target == request.target
                            && materialization.state == MaterializationState::Ready
                    });
                    let backend = ready.and_then(|materialization| materialization.backend.clone());
                    control_plane::RouteRule {
                        identity: binding.identity.clone(),
                        entry: RouteEntry {
                            route_binding_id: binding.id.clone(),
                            instance_id: binding.instance_id.clone(),
                            instance_state,
                            instance_generation,
                            backend_generation: backend
                                .as_ref()
                                .and(ready.map(|ready| ready.backend_generation)),
                            backend,
                        },
                    }
                })
                .collect();
            Ok(control_plane::HostRules::ranked(rules))
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
        self.failed_attempts
            .lock()
            .unwrap()
            .push(request.materialization_id.clone());
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

    fn claim_next_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimNextMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut records = self.materializations.lock().unwrap();
            // A store compares stored expiry against its own clock.
            let now = SystemTime::now();
            let failed = self.failed_attempts.lock().unwrap();
            let Some(record) = records.iter_mut().find(|record| {
                record.state == request.state
                    && record.target == request.target
                    && !failed.contains(&record.id)
                    && record
                        .reconciliation_lease
                        .as_ref()
                        .is_none_or(|lease| lease.expires_at <= now)
            }) else {
                return Ok(None);
            };
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
                pod_backend: None,
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
}

pub fn workload_class() -> control_plane::WorkloadClassVersion {
    control_plane::WorkloadClassVersion {
        reference: control_plane::WorkloadClassVersionRef::new(
            control_plane::WorkloadClassId::new("class-1").expect("class id is valid"),
            Generation::new(1),
        ),
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
                termination_grace_period_seconds: None,
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
                app_protocol: Default::default(),
            },
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        },
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
    assert_eq!(
        draining.materialization.as_ref().unwrap().backend,
        ready.materialization.backend,
        "a draining projection keeps its backend, so a wake inside the grace can revive it"
    );
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
