//! Reconciler tests drive `run_until_shutdown` against an in-memory store and
//! an in-memory cluster under tokio's paused clock, so every wait is exact.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use sleepypods_observability::recorder::{InMemoryObservability, ObservabilityEvent};
use tokio::time::Instant;

use super::*;
use crate::{
    ids::{BackendGeneration, Generation, InstanceId, MaterializationId, WorkloadClassId},
    instance::InstanceValues,
    manifest::{
        render_manifests, ContainerPortTemplate, ContainerTemplate, KubernetesObject,
        ManifestTemplate, RenderedManifest, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
        TemplateText, WorkloadKind, WorkloadTemplate, LABEL_INSTANCE_ID,
    },
    materialization::{
        CompleteWakeResult, FinalizeSleepResult, MaterializationReconciliationLease,
        MaterializationTarget, RenderedObjectRef,
    },
    materializer::{
        is_workload_ref, rendered_object_ref, rendered_object_refs, KubernetesClientError,
        KubernetesClientFuture, KubernetesClientResult,
    },
    projection::{LiveObjectIdentity, LiveObjectMetadata, ProjectionObjectInspection},
    route::{ListRouteBindingsForInstanceRequest, RouteBindingRecord},
    runtime_work::{MaterializationWorkStatus, RecordMaterializationFailure},
    store::StoreFuture,
    workload::{WorkloadClassVersion, WorkloadClassVersionRef, WorkloadValueSchema},
    WorkloadSleepPolicy,
};

const OWNER: &str = "test-owner";
const POD_START: Duration = Duration::from_secs(3);
const POD_STOP: Duration = Duration::from_secs(20);
/// The plan's bound on what the control plane may add to a wake under load.
const WAKE_BOUND: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------- fixtures

fn target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("valid target")
}

fn class() -> WorkloadClassVersion {
    WorkloadClassVersion {
        reference: WorkloadClassVersionRef::new(
            WorkloadClassId::new("class-a").expect("valid class id"),
            Generation::new(1),
        ),
        template_generation: Generation::new(3),
        template: ManifestTemplate {
            workload: WorkloadTemplate {
                kind: WorkloadKind::Deployment,
                name: TemplateText::literal("app"),
                replicas: None,
                app_container: ContainerTemplate {
                    name: "app".to_owned(),
                    image: TemplateText::literal("example/app:1"),
                    ports: vec![ContainerPortTemplate {
                        name: Some("http".to_owned()),
                        container_port: 8080,
                    }],
                    env: Vec::new(),
                },
            },
            service: Some(ServiceTemplate {
                name: TemplateText::literal("svc"),
                ports: vec![ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            sidecar: SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: TemplateText::literal("sleepypods/sidecar:test"),
                listen_port: 15000,
                mode: None,
            },
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        },
        default_values: InstanceValues::new(),
        value_schema: WorkloadValueSchema::new(true),
        sleep_policy: WorkloadSleepPolicy::new(120_000, 5_000, 30_000).expect("valid sleep policy"),
        exclusivity_keys: vec![],
    }
}

fn instance(id: &str, state: InstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id).expect("valid instance id"),
        workload_class: class().reference,
        values: InstanceValues::new(),
        state,
        generation: Generation::new(generation),
    }
}

/// The manifest a projection of `instance` at `projection` renders, stamped
/// with the materialization's ownership.
fn stamped_manifest(row: &MaterializationRecord) -> RenderedManifest {
    let class = class();
    let mut projected = instance(row.instance_id.as_str(), InstanceState::Running, 0);
    projected.generation = row.projection_generation;
    let manifest = render_manifests(RenderManifestRequest {
        template: &class.template,
        instance: &projected,
        sleep_policy: class
            .sleep_policy
            .resolve(&projected.values)
            .expect("sleep policy resolves"),
        namespace: "apps",
        template_generation: Some(class.template_generation),
    })
    .expect("manifest renders");
    ProjectionPlan::from_manifest(row, &manifest)
        .expect("plan derives")
        .manifest()
        .expect("plan carries its manifest")
        .clone()
}

fn row(id: &str, state: MaterializationState, stamp: u64) -> MaterializationRecord {
    let projection = match state {
        MaterializationState::Pending => stamp + 1,
        _ => stamp,
    };
    let mut row = MaterializationRecord {
        pod_backend: None,
        id: MaterializationId::new(format!("{id}:cluster-a:apps")).expect("valid id"),
        instance_id: InstanceId::new(id).expect("valid instance id"),
        instance_generation: Generation::new(stamp),
        projection_generation: Generation::new(projection),
        target: target(),
        state,
        backend: None,
        backend_generation: BackendGeneration::new(stamp),
        rendered_objects: Vec::new(),
        exclusivity_keys: vec![],
        reconciliation_lease: None,
    };
    row.rendered_objects =
        rendered_object_refs(&stamped_manifest(&row)).expect("rendered refs derive");
    row
}

fn workload_ref(row: &MaterializationRecord) -> RenderedObjectRef {
    row.rendered_objects
        .iter()
        .find(|object| is_workload_ref(object))
        .expect("a workload is rendered")
        .clone()
}

fn service_ref(row: &MaterializationRecord) -> RenderedObjectRef {
    row.rendered_objects
        .iter()
        .find(|object| object.kind == "Service")
        .expect("a Service is rendered")
        .clone()
}

fn config() -> MaterializationReconcilerConfig {
    MaterializationReconcilerConfig {
        owner: OWNER.to_owned(),
        ..MaterializationReconcilerConfig::default()
    }
}

// ------------------------------------------------------------------- store

struct Row {
    record: MaterializationRecord,
    lease_until: Option<Instant>,
    next_attempt: Instant,
    deadline: Option<Instant>,
    failure_count: u32,
    parked: bool,
}

#[derive(Default)]
struct StoreState {
    instances: BTreeMap<String, InstanceRecord>,
    rows: BTreeMap<String, Row>,
    failures: Vec<RecordMaterializationFailure>,
    running_at: BTreeMap<String, Instant>,
    cold_at: BTreeMap<String, Instant>,
    claims: usize,
    renewals: usize,
    releases: usize,
    marked_deleted: usize,
    /// Renewals inside this window fail as a store outage would.
    renew_outage: Option<(Instant, Instant)>,
    /// Renewals from this time on are refused.
    renew_refused_from: Option<Instant>,
    /// Scans that fail before the store answers again.
    unavailable_scans: usize,
    /// Every call inside this window fails as a store outage would.
    outage: Option<(Instant, Instant)>,
}

impl StoreState {
    fn reachable(&self) -> StoreResult<()> {
        let now = Instant::now();
        if self
            .outage
            .is_some_and(|(from, until)| from <= now && now < until)
        {
            return Err(StoreError::unavailable("injected store outage"));
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct FakeStore(Arc<Mutex<StoreState>>);

impl FakeStore {
    fn state(&self) -> std::sync::MutexGuard<'_, StoreState> {
        self.0.lock().expect("store state")
    }

    /// A Waking instance with its Pending materialization, as `accept_wake`
    /// commits them.
    fn add_wake(&self, id: &str) -> MaterializationRecord {
        self.add_wake_at(id, 7)
    }

    fn add_wake_at(&self, id: &str, generation: u64) -> MaterializationRecord {
        let record = row(id, MaterializationState::Pending, generation);
        let mut state = self.state();
        state.instances.insert(
            id.to_owned(),
            instance(id, InstanceState::Waking, generation),
        );
        state
            .rows
            .insert(record.id.as_str().to_owned(), new_row(&record));
        record
    }

    /// A Draining instance with its Deleting materialization past its drain
    /// deadline, as `begin_sleep` commits them.
    fn add_sleep(&self, id: &str) -> MaterializationRecord {
        let record = row(id, MaterializationState::Deleting, 7);
        let mut state = self.state();
        state
            .instances
            .insert(id.to_owned(), instance(id, InstanceState::Draining, 8));
        state
            .rows
            .insert(record.id.as_str().to_owned(), new_row(&record));
        record
    }

    fn instance_state(&self, id: &str) -> InstanceState {
        self.state().instances[id].state
    }

    fn row_state(&self, record: &MaterializationRecord) -> MaterializationState {
        self.state().rows[record.id.as_str()].record.state
    }

    fn held(state: &StoreState, id: &str, owner: &str, attempt: u64) -> bool {
        state.rows.get(id).is_some_and(|row| {
            row.lease_until.is_some_and(|until| until > Instant::now())
                && row
                    .record
                    .reconciliation_lease
                    .as_ref()
                    .is_some_and(|lease| lease.owner == owner && lease.attempt == attempt)
        })
    }

    fn lease_conflict() -> StoreError {
        StoreError::LeaseConflict {
            message: "lease is not held".to_owned(),
        }
    }
}

fn new_row(record: &MaterializationRecord) -> Row {
    Row {
        record: record.clone(),
        lease_until: None,
        next_attempt: Instant::now(),
        deadline: None,
        failure_count: 0,
        parked: false,
    }
}

impl ControlPlaneStore for FakeStore {
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
        enqueue_materialization,
        maintain_runtime_records,
        accept_wake,
        request_instance_deletion,
        create_instance,
        delete_instance,
        create_workload_class_version,
        create_route_binding,
        get_route_binding,
        delete_route_binding,
        resolve_route,
        compare_and_swap_instance_state,
        record_materialization,
        load_ready_materialization,
        load_active_materialization,
        load_materialization,
        complete_wake,
        begin_sleep,
        finalize_sleep,
        load_materialization_operational_metrics,
        force_delete_materialization,
        force_release_exclusivity_key,
        lookup_route_dependencies,
        put_http01_challenge,
        resolve_http01_challenge,
        delete_http01_challenge,
        expire_http01_challenges
    );

    fn list_route_bindings_for_instance<'a>(
        &'a self,
        _request: ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<RouteBindingRecord>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn finalize_instance_deletions(&self, _limit: usize) -> StoreFuture<'_, StoreResult<usize>> {
        Box::pin(async { Ok(0) })
    }

    fn load_materialization_work_status(
        &self,
        id: MaterializationId,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationWorkStatus>>> {
        Box::pin(async move {
            let state = self.state();
            state.reachable()?;
            Ok(state.rows.get(id.as_str()).and_then(|row| {
                row.deadline.map(|deadline| MaterializationWorkStatus {
                    operation_deadline_unix_millis: 1,
                    operation_remaining: Some(deadline.saturating_duration_since(Instant::now())),
                    ..Default::default()
                })
            }))
        })
    }

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move {
            let state = self.state();
            state.reachable()?;
            Ok(state.instances.get(request.instance_id.as_str()).cloned())
        })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        _request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        Box::pin(async { Ok(Some(class())) })
    }

    fn claim_next_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimNextMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut state = self.state();
            state.reachable()?;
            if state.unavailable_scans > 0 {
                state.unavailable_scans -= 1;
                return Err(StoreError::unavailable("injected store outage"));
            }
            let now = Instant::now();
            let Some(row) = state
                .rows
                .values_mut()
                .filter(|row| {
                    row.record.state == request.state
                        && !row.parked
                        && row.next_attempt <= now
                        && row.lease_until.is_none_or(|until| until <= now)
                })
                .min_by_key(|row| (row.next_attempt, row.record.id.as_str().to_owned()))
            else {
                return Ok(None);
            };
            let attempt = row
                .record
                .reconciliation_lease
                .as_ref()
                .map_or(1, |lease| lease.attempt + 1);
            row.lease_until = Some(now + request.lease_ttl);
            row.record.reconciliation_lease = Some(MaterializationReconciliationLease {
                owner: request.owner,
                expires_at: std::time::SystemTime::now() + request.lease_ttl,
                attempt,
            });
            let claimed = row.record.clone();
            state.claims += 1;
            Ok(Some(claimed))
        })
    }

    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let mut state = self.state();
            state.reachable()?;
            let now = Instant::now();
            if state
                .renew_outage
                .is_some_and(|(from, until)| from <= now && now < until)
            {
                return Err(StoreError::unavailable("injected store outage"));
            }
            if state.renew_refused_from.is_some_and(|from| from <= now) {
                return Ok(false);
            }
            let id = request.materialization_id.as_str();
            if !Self::held(&state, id, &request.owner, request.attempt)
                || state.rows[id].record.state != request.expected_state
            {
                return Ok(false);
            }
            state.rows.get_mut(id).expect("held row").lease_until = Some(now + request.lease_ttl);
            state.renewals += 1;
            Ok(true)
        })
    }

    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let mut state = self.state();
            state.reachable()?;
            let id = request.materialization_id.as_str();
            if !Self::held(&state, id, &request.owner, request.attempt) {
                return Ok(false);
            }
            let row = state.rows.get_mut(id).expect("held row");
            row.lease_until = None;
            row.next_attempt = Instant::now();
            state.releases += 1;
            Ok(true)
        })
    }

    fn record_materialization_failure(
        &self,
        request: RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        Box::pin(async move {
            let mut state = self.state();
            state.reachable()?;
            let id = request.materialization_id.as_str().to_owned();
            if !Self::held(&state, &id, &request.owner, request.attempt)
                || state.rows[&id].record.state != request.expected_state
            {
                return Ok(false);
            }
            let now = Instant::now();
            let row = state.rows.get_mut(&id).expect("held row");
            let waking = row.record.state == MaterializationState::Pending;
            let expired = waking && row.deadline.is_some_and(|deadline| deadline <= now);
            row.lease_until = None;
            if waking && (request.permanent || expired) {
                row.record.state = MaterializationState::Deleting;
                row.failure_count = 0;
                row.next_attempt = now;
                let instance_id = row.record.instance_id.as_str().to_owned();
                let instance = state.instances.get_mut(&instance_id).expect("instance");
                instance.state = InstanceState::Failed;
                instance.generation = instance.generation.next();
            } else {
                row.failure_count += 1;
                row.next_attempt = now + Duration::from_secs(1 << row.failure_count.min(5));
                row.parked = request.permanent;
            }
            state.failures.push(request);
            Ok(true)
        })
    }

    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        Box::pin(async move {
            let mut state = self.state();
            state.reachable()?;
            let id = request.materialization_id.as_str().to_owned();
            if !Self::held(&state, &id, &request.lease_owner, request.attempt) {
                return Err(Self::lease_conflict());
            }
            let instance_id = request.complete.instance_id.as_str().to_owned();
            let instance = state.instances.get_mut(&instance_id).expect("instance");
            instance.state = InstanceState::Running;
            instance.generation = instance.generation.next();
            let instance = instance.clone();
            let row = state.rows.get_mut(&id).expect("held row");
            row.lease_until = None;
            row.record.state = MaterializationState::Ready;
            row.record.instance_generation = instance.generation;
            row.record.backend = Some(request.complete.backend);
            let materialization = row.record.clone();
            state.running_at.insert(instance_id, Instant::now());
            Ok(CompleteWakeResult {
                instance,
                materialization,
            })
        })
    }

    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        Box::pin(async move {
            let mut state = self.state();
            state.reachable()?;
            let id = request.materialization_id.as_str().to_owned();
            if !Self::held(&state, &id, &request.lease_owner, request.attempt) {
                return Err(Self::lease_conflict());
            }
            let instance_id = request.finalize.instance_id.as_str().to_owned();
            let instance = state.instances.get_mut(&instance_id).expect("instance");
            instance.state = InstanceState::Cold;
            instance.generation = instance.generation.next();
            let instance = instance.clone();
            let row = state.rows.get_mut(&id).expect("held row");
            row.lease_until = None;
            row.record.state = MaterializationState::Deleted;
            state.cold_at.insert(instance_id, Instant::now());
            Ok(FinalizeSleepResult {
                instance,
                materialization: None,
            })
        })
    }

    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut state = self.state();
            state.reachable()?;
            let id = request.materialization_id.as_str().to_owned();
            if !Self::held(&state, &id, &request.lease_owner, request.attempt) {
                return Err(Self::lease_conflict());
            }
            let row = state.rows.get_mut(&id).expect("held row");
            row.lease_until = None;
            row.record.state = MaterializationState::Deleted;
            let record = row.record.clone();
            state.marked_deleted += 1;
            Ok(Some(record))
        })
    }
}

// ----------------------------------------------------------------- cluster

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Create,
    Patch,
    Delete,
}

/// What an injected fault does to one mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// The request never reaches the API server; the client sees a reset.
    Lost,
    /// The request is applied; the client sees a 5xx or a reset.
    AppliedUnseen,
    /// The client gives up after its timeout; the request was not applied.
    StalledLost,
    /// The client gives up after its timeout; the request was applied.
    StalledApplied,
    /// The API server refuses the request for good.
    Refused,
    /// The request takes this long and then succeeds.
    Slow(Duration),
}

struct Live {
    metadata: LiveObjectMetadata,
    object_ref: RenderedObjectRef,
    resource_version: u64,
    /// When a workload was scaled up. A workload with no time here has zero
    /// replicas and no Pod.
    active_since: Option<Instant>,
    /// When a deleted object, and its Pod, are gone.
    gone_at: Option<Instant>,
    finalizer: bool,
}

struct ClusterState {
    objects: BTreeMap<String, Live>,
    faults: Vec<(Op, &'static str, Fault)>,
    never_ready: BTreeSet<String>,
    pod_start: Duration,
    pod_stop: Duration,
    next_uid: u64,
    creates: usize,
    patches: usize,
    deletes: usize,
}

#[derive(Clone)]
struct FakeCluster(Arc<Mutex<ClusterState>>);

impl Default for FakeCluster {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(ClusterState {
            objects: BTreeMap::new(),
            faults: Vec::new(),
            never_ready: BTreeSet::new(),
            pod_start: POD_START,
            pod_stop: POD_STOP,
            next_uid: 0,
            creates: 0,
            patches: 0,
            deletes: 0,
        })))
    }
}

fn key(object: &RenderedObjectRef) -> String {
    format!("{}/{}/{}", object.kind, object.namespace, object.name)
}

impl ClusterState {
    /// Removes every deleted object whose Pod has stopped.
    fn settle(&mut self) {
        let now = Instant::now();
        self.objects
            .retain(|_, live| live.finalizer || live.gone_at.is_none_or(|gone| gone > now));
    }

    fn insert(&mut self, object: &KubernetesObject, active: bool) {
        self.next_uid += 1;
        let object_ref = rendered_object_ref(object);
        let mut metadata = LiveObjectMetadata::from_rendered_object(object);
        metadata.identity = LiveObjectIdentity {
            uid: format!("uid-{}", self.next_uid),
            resource_version: "1".to_owned(),
        };
        self.objects.insert(
            key(&object_ref),
            Live {
                metadata,
                object_ref,
                resource_version: 1,
                active_since: active.then(Instant::now),
                gone_at: None,
                finalizer: false,
            },
        );
    }

    fn take_fault(&mut self, op: Op, kind: &str) -> Option<Fault> {
        let index = self
            .faults
            .iter()
            .position(|(fault_op, fault_kind, _)| *fault_op == op && *fault_kind == kind)?;
        Some(self.faults.remove(index).2)
    }

    fn matches(&self, object: &RenderedObjectRef, identity: &LiveObjectIdentity) -> bool {
        self.objects
            .get(&key(object))
            .is_some_and(|live| live.gone_at.is_none() && live.metadata.identity == *identity)
    }
}

impl FakeCluster {
    fn state(&self) -> std::sync::MutexGuard<'_, ClusterState> {
        self.0.lock().expect("cluster state")
    }

    fn fault(&self, op: Op, kind: &'static str, fault: Fault) {
        self.state().faults.push((op, kind, fault));
    }

    /// The projection a completed wake leaves behind: every object present,
    /// owned, and its workload running.
    fn materialize(&self, row: &MaterializationRecord) {
        let mut state = self.state();
        for rendered in &stamped_manifest(row).objects {
            state.insert(&rendered.object, true);
        }
    }

    fn present(&self, object: &RenderedObjectRef) -> bool {
        let mut state = self.state();
        state.settle();
        state.objects.contains_key(&key(object))
    }

    /// Whether the workload has a Pod.
    fn running(&self, object: &RenderedObjectRef) -> bool {
        let mut state = self.state();
        state.settle();
        state
            .objects
            .get(&key(object))
            .is_some_and(|live| live.active_since.is_some())
    }

    fn mutations(&self) -> (usize, usize, usize) {
        let state = self.state();
        (state.creates, state.patches, state.deletes)
    }

    /// Runs one mutation through any fault injected for it.
    async fn mutate(
        &self,
        op: Op,
        object: &RenderedObjectRef,
        apply: impl FnOnce(&mut ClusterState) -> KubernetesClientResult<()>,
    ) -> KubernetesClientResult<()> {
        let fault = {
            let mut state = self.state();
            state.settle();
            state.take_fault(op, &object.kind)
        };
        let unseen = || KubernetesClientError::stale("injected unknown outcome");
        match fault {
            None => apply(&mut self.state()),
            Some(Fault::Lost) => Err(unseen()),
            Some(Fault::AppliedUnseen) => {
                apply(&mut self.state())?;
                Err(unseen())
            }
            Some(Fault::StalledLost) => {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Err(unseen())
            }
            Some(Fault::StalledApplied) => {
                apply(&mut self.state())?;
                tokio::time::sleep(Duration::from_secs(10)).await;
                Err(unseen())
            }
            Some(Fault::Refused) => Err(KubernetesClientError::new("injected refusal")),
            Some(Fault::Slow(duration)) => {
                tokio::time::sleep(duration).await;
                apply(&mut self.state())
            }
        }
    }
}

impl KubernetesMaterializerClient for FakeCluster {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let object_ref = rendered_object_ref(object);
            match precondition {
                None => {
                    self.mutate(Op::Create, &object_ref, |state| {
                        state.settle();
                        if state.objects.contains_key(&key(&object_ref)) {
                            return Err(KubernetesClientError::stale("already exists"));
                        }
                        state.insert(object, false);
                        state.creates += 1;
                        Ok(())
                    })
                    .await
                }
                Some(identity) => {
                    self.mutate(Op::Patch, &object_ref, |state| {
                        state.settle();
                        if !state.matches(&object_ref, identity) {
                            return Err(KubernetesClientError::stale("conflict"));
                        }
                        let live = state
                            .objects
                            .get_mut(&key(&object_ref))
                            .expect("matched object");
                        let desired = LiveObjectMetadata::from_rendered_object(object);
                        live.metadata.labels = desired.labels;
                        live.metadata.annotations = desired.annotations;
                        live.resource_version += 1;
                        live.metadata.identity.resource_version = live.resource_version.to_string();
                        if is_workload_ref(&object_ref) {
                            live.active_since.get_or_insert_with(Instant::now);
                        }
                        state.patches += 1;
                        Ok(())
                    })
                    .await
                }
            }
        })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.mutate(Op::Delete, object, |state| {
                state.settle();
                if !state.objects.contains_key(&key(object)) {
                    return Ok(());
                }
                if !state.matches(object, precondition) {
                    return Err(KubernetesClientError::stale("conflict"));
                }
                let stop = state.pod_stop;
                let live = state.objects.get_mut(&key(object)).expect("matched object");
                let lingers = if live.active_since.is_some() {
                    stop
                } else {
                    Duration::ZERO
                };
                live.gone_at = Some(Instant::now() + lingers);
                live.metadata.deleting = true;
                state.deletes += 1;
                Ok(())
            })
            .await
        })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async move {
            let workload = objects
                .iter()
                .find(|object| is_workload_ref(object))
                .expect("a workload is rendered");
            loop {
                let ready_at = {
                    let mut state = self.state();
                    state.settle();
                    let start = state.pod_start;
                    state.objects.get(&key(workload)).and_then(|live| {
                        let instance = live.metadata.labels.get(LABEL_INSTANCE_ID)?;
                        if state.never_ready.contains(instance) {
                            return None;
                        }
                        Some(live.active_since? + start)
                    })
                };
                match ready_at {
                    Some(ready_at) if ready_at <= Instant::now() => {
                        return Ok(BackendEndpoint::new("http://10.96.0.10:80").expect("backend"));
                    }
                    Some(ready_at) => tokio::time::sleep_until(ready_at).await,
                    None => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            let mut state = self.state();
            state.settle();
            Ok(match state.objects.get(&key(object)) {
                Some(live) => ProjectionObjectInspection::Present(live.metadata.clone()),
                None => ProjectionObjectInspection::Missing,
            })
        })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let mut state = self.state();
            state.settle();
            let pods = state.objects.values().any(|live| {
                live.active_since.is_some()
                    && is_workload_ref(&live.object_ref)
                    && live
                        .metadata
                        .labels
                        .get(LABEL_INSTANCE_ID)
                        .map(String::as_str)
                        == Some(instance_id)
            });
            if pods {
                Err(KubernetesClientError::transient("Pods still exist"))
            } else {
                Ok(())
            }
        })
    }
}

// ----------------------------------------------------------------- harness

struct Harness {
    store: FakeStore,
    cluster: FakeCluster,
    sink: InMemoryObservability,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), StoreError>>,
    started: Instant,
}

fn start(store: &FakeStore, cluster: &FakeCluster) -> Harness {
    start_with(store, cluster, config())
}

fn start_with(
    store: &FakeStore,
    cluster: &FakeCluster,
    config: MaterializationReconcilerConfig,
) -> Harness {
    let sink = InMemoryObservability::default();
    let reconciler = MaterializationReconciler::new(
        Arc::new(store.clone()),
        KubernetesMaterializer::new(cluster.clone()),
        target(),
        config,
        sink.recorder(),
    );
    let (shutdown, receiver) = watch::channel(false);
    Harness {
        store: store.clone(),
        cluster: cluster.clone(),
        sink,
        shutdown,
        task: tokio::spawn(reconciler.run_until_shutdown(receiver)),
        started: Instant::now(),
    }
}

impl Harness {
    /// Lets virtual time run until `condition` holds, failing after `limit`.
    async fn until(&self, limit: Duration, what: &str, condition: impl Fn(&Harness) -> bool) {
        let deadline = Instant::now() + limit;
        while !condition(self) {
            assert!(
                Instant::now() < deadline,
                "{what} did not happen within {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn stop(self) -> Result<(), StoreError> {
        self.shutdown.send_replace(true);
        self.task.await.expect("reconciler task")
    }

    fn failures(&self) -> usize {
        self.store.state().failures.len()
    }

    fn metric(&self, name: &str) -> Vec<f64> {
        self.sink
            .events()
            .iter()
            .filter_map(|event| match event {
                ObservabilityEvent::Metric(metric) if metric.name() == name => Some(metric.value()),
                _ => None,
            })
            .collect()
    }
}

// ------------------------------------------------------- wake and sleep

#[tokio::test(start_paused = true)]
async fn wake_creates_the_workload_idle_then_scales_it_up_and_completes() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let wake = store.add_wake("tenant");
    let harness = start(&store, &cluster);

    harness
        .until(POD_START + WAKE_BOUND, "the wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Running
        })
        .await;

    // Service and Deployment are created; only the Deployment is then patched.
    assert_eq!(cluster.mutations(), (2, 1, 0));
    assert!(cluster.running(&workload_ref(&wake)));
    assert_eq!(store.row_state(&wake), MaterializationState::Ready);
    assert_eq!(harness.failures(), 0);
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_normal_sleep_waits_for_the_pod_to_stop_and_records_no_failure() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let sleep = store.add_sleep("tenant");
    cluster.materialize(&sleep);
    let harness = start(&store, &cluster);

    harness
        .until(POD_STOP + WAKE_BOUND, "the sleep", |h| {
            h.store.instance_state("tenant") == InstanceState::Cold
        })
        .await;

    let cold_after = store.state().cold_at["tenant"] - harness.started;
    assert!(
        cold_after >= POD_STOP,
        "finalized while the Pod was stopping"
    );
    assert!(cold_after <= POD_STOP + Duration::from_secs(1));
    assert!(sleep
        .rendered_objects
        .iter()
        .all(|object| !cluster.present(object)));
    assert_eq!(harness.failures(), 0);
    assert!(harness
        .metric(RUNTIME_MATERIALIZATION_FAILURES_TOTAL.name())
        .is_empty());
    assert_eq!(
        store.state().claims,
        1,
        "one attempt covers the whole sleep"
    );
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_hundred_cold_instances_reach_running_within_pod_start_plus_the_bound() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    for index in 0..100 {
        store.add_wake(&format!("tenant-{index}"));
    }
    let harness = start(&store, &cluster);

    harness
        .until(POD_START + WAKE_BOUND, "every wake", |h| {
            h.store.state().running_at.len() == 100
        })
        .await;
    let slowest = *store.state().running_at.values().max().expect("wakes") - harness.started;
    assert!(
        slowest <= POD_START + WAKE_BOUND,
        "slowest wake took {slowest:?}"
    );
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn never_ready_instances_leave_a_healthy_wake_within_the_bound() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    for index in 0..3 {
        let id = format!("stuck-{index}");
        store.add_wake(&id);
        cluster.state().never_ready.insert(id);
    }
    let harness = start(&store, &cluster);
    tokio::time::sleep(Duration::from_secs(30)).await;

    let asked = Instant::now();
    store.add_wake("healthy");
    crate::runtime_work::signal_lifecycle_work();
    harness
        .until(POD_START + WAKE_BOUND, "the healthy wake", |h| {
            h.store.instance_state("healthy") == InstanceState::Running
        })
        .await;
    assert!(store.state().running_at["healthy"] - asked <= POD_START + WAKE_BOUND);
    assert_eq!(store.instance_state("stuck-0"), InstanceState::Waking);
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn fifty_simultaneous_sleeps_leave_a_healthy_wake_within_the_bound() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    for index in 0..50 {
        cluster.materialize(&store.add_sleep(&format!("idle-{index}")));
    }
    store.add_wake("healthy");
    let harness = start(&store, &cluster);

    harness
        .until(POD_START + WAKE_BOUND, "the healthy wake", |h| {
            h.store.instance_state("healthy") == InstanceState::Running
        })
        .await;
    harness
        .until(POD_STOP + WAKE_BOUND, "every sleep", |h| {
            h.store.state().cold_at.len() == 50
        })
        .await;
    assert_eq!(harness.failures(), 0);
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn cleanup_beyond_its_limit_waits_without_delaying_wakes() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    for index in 0..4 {
        cluster.materialize(&store.add_sleep(&format!("idle-{index}")));
    }
    store.add_wake("healthy");
    let harness = start_with(
        &store,
        &cluster,
        MaterializationReconcilerConfig {
            cleanup_limit: 2,
            ..config()
        },
    );

    harness
        .until(POD_START + WAKE_BOUND, "the healthy wake", |h| {
            h.store.instance_state("healthy") == InstanceState::Running
        })
        .await;
    // Two cleanups run at a time; the next two start when the first two end.
    harness
        .until(POD_STOP * 2 + WAKE_BOUND, "every sleep", |h| {
            h.store.state().cold_at.len() == 4
        })
        .await;
    let last = *store.state().cold_at.values().max().expect("sleeps") - harness.started;
    assert!(last >= POD_STOP * 2);
    harness.stop().await.expect("clean shutdown");
}

// --------------------------------------------------------------- scheduling

#[tokio::test(start_paused = true)]
async fn a_signal_starts_committed_work_without_waiting_for_the_scan() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let harness = start_with(
        &store,
        &cluster,
        MaterializationReconcilerConfig {
            interval: Duration::from_secs(3600),
            ..config()
        },
    );
    tokio::time::sleep(Duration::from_secs(5)).await;

    store.add_wake("tenant");
    crate::runtime_work::signal_lifecycle_work();
    harness
        .until(Duration::from_secs(1), "the claim", |h| {
            h.store.state().claims == 1
        })
        .await;
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_finished_job_frees_its_place_at_once() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    store.add_wake("first");
    store.add_wake("second");
    let harness = start_with(
        &store,
        &cluster,
        MaterializationReconcilerConfig {
            interval: Duration::from_secs(3600),
            wake_limit: 1,
            ..config()
        },
    );

    harness
        .until(POD_START * 2 + Duration::from_secs(1), "both wakes", |h| {
            h.store.state().running_at.len() == 2
        })
        .await;
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn the_periodic_scan_finds_work_no_signal_announced() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let harness = start(&store, &cluster);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Another replica accepted this wake.
    store.add_wake("tenant");
    harness
        .until(Duration::from_secs(2), "the claim", |h| {
            h.store.state().claims == 1
        })
        .await;
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_failing_store_is_retried_with_backoff_and_the_reconciler_keeps_running() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    store.add_wake("tenant");
    store.state().unavailable_scans = 6;
    let harness = start(&store, &cluster);

    harness
        .until(Duration::from_secs(120), "the wake after the outage", |h| {
            h.store.instance_state("tenant") == InstanceState::Running
        })
        .await;
    assert!(!harness.task.is_finished());
    let availability = harness.metric(CONTROL_PLANE_STORE_AVAILABLE.name());
    assert_eq!(availability.first(), Some(&0.0));
    assert_eq!(availability.last(), Some(&1.0));
    // 2 s, 4 s, 8 s, then capped at 15 s for the remaining three failures.
    assert!(store.state().running_at["tenant"] - harness.started >= Duration::from_secs(59));
    harness.stop().await.expect("clean shutdown");
}

#[test]
fn store_backoff_doubles_from_the_interval_and_is_capped() {
    let interval = Duration::from_secs(1);
    assert_eq!(store_backoff(interval, 0), None);
    assert_eq!(store_backoff(interval, 1), Some(Duration::from_secs(2)));
    assert_eq!(store_backoff(interval, 3), Some(Duration::from_secs(8)));
    assert_eq!(store_backoff(interval, 40), Some(STORE_BACKOFF_MAX));
}

#[test]
fn limits_come_from_the_environment_within_bounds() {
    let from = |wake: Option<&str>, cleanup: Option<&str>| {
        MaterializationReconcilerConfig::from_values(|name| {
            match name {
                WAKE_LIMIT_ENV => wake,
                _ => cleanup,
            }
            .map(str::to_owned)
        })
    };
    let defaults = from(None, None).expect("defaults");
    assert_eq!((defaults.wake_limit, defaults.cleanup_limit), (256, 64));
    let set = from(Some("512"), Some(" 8 ")).expect("valid limits");
    assert_eq!((set.wake_limit, set.cleanup_limit), (512, 8));
    for invalid in ["0", "4097", "-1", "many"] {
        assert!(from(Some(invalid), None).is_err(), "{invalid}");
        assert!(from(None, Some(invalid)).is_err(), "{invalid}");
    }
}

// --------------------------------------------------------- lease and cancel

#[tokio::test(start_paused = true)]
async fn a_long_wait_for_readiness_renews_the_lease() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    cluster.state().pod_start = Duration::from_secs(90);
    store.add_wake("tenant");
    let harness = start(&store, &cluster);

    harness
        .until(Duration::from_secs(95), "the wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Running
        })
        .await;
    assert_eq!(store.state().claims, 1, "the lease never expired");
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn each_mutation_renews_the_lease_before_it_is_sent() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    store.add_wake("tenant");
    let harness = start(&store, &cluster);

    harness
        .until(POD_START + WAKE_BOUND, "the wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Running
        })
        .await;
    let (creates, patches, _) = cluster.mutations();
    assert_eq!(store.state().renewals, creates + patches);
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_refused_renewal_sends_no_mutation() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    store.add_wake("tenant");
    store.state().renew_refused_from = Some(Instant::now());
    let harness = start(&store, &cluster);

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(cluster.mutations(), (0, 0, 0));
    assert_eq!(store.instance_state("tenant"), InstanceState::Waking);
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_store_outage_shorter_than_the_lease_does_not_interrupt_the_wake() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    cluster.state().pod_start = Duration::from_secs(50);
    store.add_wake("tenant");
    let harness = start(&store, &cluster);
    let from = harness.started + Duration::from_secs(15);
    store.state().renew_outage = Some((from, from + Duration::from_secs(30)));

    harness
        .until(Duration::from_secs(55), "the wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Running
        })
        .await;
    assert_eq!(
        store.state().claims,
        1,
        "the first attempt survived the outage"
    );
    assert_eq!(harness.failures(), 0);
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_thirty_second_store_outage_delays_a_wake_and_a_sleep_without_failing_them() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    cluster.state().pod_start = Duration::from_secs(10);
    store.add_wake("waking");
    cluster.materialize(&store.add_sleep("sleeping"));
    let harness = start(&store, &cluster);
    let from = harness.started + Duration::from_secs(5);
    let until = from + Duration::from_secs(30);
    store.state().outage = Some((from, until));

    // The Pod is ready at 10 s and the old Pod gone at 20 s, both mid-outage.
    harness
        .until(Duration::from_secs(45), "both transitions", |h| {
            h.store.instance_state("waking") == InstanceState::Running
                && h.store.instance_state("sleeping") == InstanceState::Cold
        })
        .await;
    assert!(store.state().running_at["waking"] >= until);
    assert!(store.state().cold_at["sleeping"] >= until);
    assert_eq!(
        store.state().claims,
        2,
        "each first attempt survived the outage"
    );
    assert_eq!(harness.failures(), 0);
    assert!(!harness.task.is_finished());
    assert!(harness
        .metric(CONTROL_PLANE_STORE_AVAILABLE.name())
        .contains(&0.0));
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn renewal_errors_end_the_work_only_at_the_leases_own_expiry() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    cluster.state().never_ready.insert("tenant".to_owned());
    store.add_wake("tenant");
    let harness = start(&store, &cluster);
    let from = harness.started + Duration::from_secs(15);
    store.state().renew_outage = Some((from, from + Duration::from_secs(3600)));

    // The creates and the scale-up renewed the lease at the start; it lasts 60 s.
    tokio::time::sleep(Duration::from_secs(55)).await;
    assert_eq!(store.state().claims, 1);
    harness
        .until(Duration::from_secs(10), "the next claim", |h| {
            h.store.state().claims == 2
        })
        .await;
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn an_accepted_delete_stops_the_wake_at_its_next_renewal_and_cleanup_follows_at_once() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    cluster.state().pod_start = Duration::from_secs(40);
    let wake = store.add_wake("tenant");
    let harness = start(&store, &cluster);
    tokio::time::sleep(Duration::from_secs(10)).await;

    // DeleteInstance moves the instance and its Pending row without taking
    // the lease.
    {
        let mut state = store.state();
        state.instances.get_mut("tenant").expect("instance").state = InstanceState::Deleting;
        state
            .rows
            .get_mut(wake.id.as_str())
            .expect("row")
            .record
            .state = MaterializationState::Deleting;
    }

    // The heartbeat at 20 s is refused; the wake releases its claim and
    // cleanup claims the row without waiting out the 60 s lease.
    harness
        .until(Duration::from_secs(11), "the cleanup claim", |h| {
            h.store.state().claims == 2
        })
        .await;
    assert_eq!(store.state().releases, 1);
    assert_eq!(
        harness.failures(),
        0,
        "a lost lease is not this attempt's to report"
    );
    harness
        .until(POD_STOP + WAKE_BOUND, "the cleanup", |h| {
            h.store.row_state(&wake) == MaterializationState::Deleted
        })
        .await;
    assert!(wake
        .rendered_objects
        .iter()
        .all(|object| !cluster.present(object)));
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn shutdown_lets_a_dispatched_mutation_finish_and_releases_the_lease() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let wake = store.add_wake("tenant");
    cluster.fault(
        Op::Create,
        "Deployment",
        Fault::Slow(Duration::from_secs(5)),
    );
    let harness = start(&store, &cluster);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (store, cluster) = (harness.store.clone(), harness.cluster.clone());
    harness.stop().await.expect("clean shutdown");

    assert!(
        cluster.present(&workload_ref(&wake)),
        "the create completed"
    );
    assert!(
        !cluster.running(&workload_ref(&wake)),
        "nothing scaled it up"
    );
    assert_eq!(store.state().releases, 1);
    assert!(store.state().failures.is_empty());
}

// ------------------------------------------------------- injected faults

#[tokio::test(start_paused = true)]
async fn a_wake_recovers_from_a_fault_on_any_mutation() {
    for (op, kind) in [
        (Op::Create, "Service"),
        (Op::Create, "Deployment"),
        (Op::Patch, "Deployment"),
    ] {
        for fault in [
            Fault::Lost,
            Fault::AppliedUnseen,
            Fault::StalledLost,
            Fault::StalledApplied,
        ] {
            let (store, cluster) = (FakeStore::default(), FakeCluster::default());
            let wake = store.add_wake("tenant");
            cluster.fault(op, kind, fault);
            let harness = start(&store, &cluster);

            harness
                .until(
                    Duration::from_secs(10) + POD_START + WAKE_BOUND,
                    &format!("the wake after {fault:?} on {op:?} {kind}"),
                    |h| h.store.instance_state("tenant") == InstanceState::Running,
                )
                .await;
            assert!(cluster.running(&workload_ref(&wake)));
            assert_eq!(harness.failures(), 0, "{fault:?} on {op:?} {kind}");
            harness.stop().await.expect("clean shutdown");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_sleep_recovers_from_a_fault_on_any_delete() {
    for kind in ["Service", "Deployment"] {
        for fault in [
            Fault::Lost,
            Fault::AppliedUnseen,
            Fault::StalledLost,
            Fault::StalledApplied,
        ] {
            let (store, cluster) = (FakeStore::default(), FakeCluster::default());
            let sleep = store.add_sleep("tenant");
            cluster.materialize(&sleep);
            cluster.fault(Op::Delete, kind, fault);
            let harness = start(&store, &cluster);

            harness
                .until(
                    Duration::from_secs(10) + POD_STOP + WAKE_BOUND,
                    &format!("the sleep after {fault:?} on delete {kind}"),
                    |h| h.store.instance_state("tenant") == InstanceState::Cold,
                )
                .await;
            assert!(sleep
                .rendered_objects
                .iter()
                .all(|object| !cluster.present(object)));
            assert_eq!(harness.failures(), 0, "{fault:?} on delete {kind}");
            harness.stop().await.expect("clean shutdown");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn repeated_unknown_outcomes_end_the_attempt_and_the_next_one_recovers() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    store.add_wake("tenant");
    for _ in 0..APPLY_ROUNDS_IN_ONE_ATTEMPT {
        cluster.fault(Op::Create, "Deployment", Fault::Lost);
    }
    let harness = start(&store, &cluster);

    harness
        .until(Duration::from_secs(20), "the wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Running
        })
        .await;
    let failures = store.state().failures.clone();
    assert_eq!(failures.len(), 1);
    assert!(!failures[0].permanent);
    assert_eq!(store.state().claims, 2);
    harness.stop().await.expect("clean shutdown");
}

/// `ProjectionReconciler::apply` makes this many rounds before it gives up.
const APPLY_ROUNDS_IN_ONE_ATTEMPT: usize = 4;

#[tokio::test(start_paused = true)]
async fn a_create_that_lands_after_cleanup_starts_no_pod_and_the_next_wake_adopts_it() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let sleep = store.add_sleep("tenant");
    cluster.materialize(&sleep);
    let harness = start(&store, &cluster);
    harness
        .until(POD_STOP + WAKE_BOUND, "the sleep", |h| {
            h.store.instance_state("tenant") == InstanceState::Cold
        })
        .await;

    // A create from the previous incarnation arrives: the API server stores a
    // workload with zero replicas.
    let late = stamped_manifest(&sleep)
        .objects
        .into_iter()
        .find(|rendered| is_workload_ref(&rendered_object_ref(&rendered.object)))
        .expect("a workload is rendered");
    cluster.state().insert(&late.object, false);
    tokio::time::sleep(Duration::from_secs(300)).await;
    assert!(
        !cluster.running(&workload_ref(&sleep)),
        "a late create runs nothing"
    );

    let wake = store.add_wake_at("tenant", 10);
    crate::runtime_work::signal_lifecycle_work();
    harness
        .until(POD_START + WAKE_BOUND, "the next wake", |h| {
            h.store.row_state(&wake) == MaterializationState::Ready
        })
        .await;
    assert!(cluster.running(&workload_ref(&wake)));
    assert_eq!(harness.failures(), 0);
    harness.stop().await.expect("clean shutdown");
}

// ------------------------------------------------ ownership and outcomes

#[tokio::test(start_paused = true)]
async fn a_wake_refuses_an_object_it_does_not_own_and_the_instance_fails() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let wake = store.add_wake("tenant");
    // Another owner's Service holds the rendered name.
    let foreign = row("someone-else", MaterializationState::Deleting, 7);
    let mut service = stamped_manifest(&foreign)
        .objects
        .into_iter()
        .find(|rendered| rendered_object_ref(&rendered.object).kind == "Service")
        .expect("a Service is rendered");
    if let KubernetesObject::Service(service) = &mut service.object {
        service.metadata.name = service_ref(&wake).name;
    }
    cluster.state().insert(&service.object, false);
    let harness = start(&store, &cluster);

    harness
        .until(Duration::from_secs(5), "the failed wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Failed
        })
        .await;
    // Cleanup meets the same object and parks; it deletes nothing.
    harness
        .until(Duration::from_secs(5), "the parked cleanup", |h| {
            h.store.state().rows[wake.id.as_str()].parked
        })
        .await;
    assert_eq!(cluster.mutations(), (0, 0, 0));
    assert!(cluster.present(&service_ref(&wake)));
    assert!(store
        .state()
        .failures
        .iter()
        .all(|failure| failure.permanent));
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_wake_takes_over_its_own_leftovers_from_an_older_generation() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let older = row("tenant", MaterializationState::Deleting, 5);
    cluster.materialize(&older);
    let wake = store.add_wake("tenant");
    let harness = start(&store, &cluster);

    harness
        .until(POD_START + WAKE_BOUND, "the wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Running
        })
        .await;
    // Both leftovers are patched in place; nothing is created.
    assert_eq!(cluster.mutations(), (0, 2, 0));
    assert!(cluster.running(&workload_ref(&wake)));
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_wake_refuses_objects_stamped_by_a_newer_generation() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let newer = row("tenant", MaterializationState::Deleting, 20);
    cluster.materialize(&newer);
    store.add_wake("tenant");
    let harness = start(&store, &cluster);

    harness
        .until(Duration::from_secs(5), "the failed wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Failed
        })
        .await;
    assert_eq!(cluster.mutations(), (0, 0, 0));
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_wake_refused_for_good_fails_and_its_cleanup_starts_at_once() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let wake = store.add_wake("tenant");
    cluster.fault(Op::Create, "Deployment", Fault::Refused);
    let harness = start(&store, &cluster);

    harness
        .until(Duration::from_secs(2), "the cleaned-up failed wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Failed
                && h.store.row_state(&wake) == MaterializationState::Deleted
        })
        .await;
    assert!(!cluster.present(&service_ref(&wake)));
    assert_eq!(store.state().marked_deleted, 1);
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_wake_past_its_operation_deadline_fails() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let wake = store.add_wake("tenant");
    cluster.state().never_ready.insert("tenant".to_owned());
    store
        .state()
        .rows
        .get_mut(wake.id.as_str())
        .expect("row")
        .deadline = Some(Instant::now() + Duration::from_secs(30));
    let harness = start(&store, &cluster);

    harness
        .until(Duration::from_secs(50), "the failed wake", |h| {
            h.store.instance_state("tenant") == InstanceState::Failed
        })
        .await;
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn cleanup_runs_past_its_deadline_and_through_a_held_object() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let sleep = store.add_sleep("tenant");
    cluster.materialize(&sleep);
    store
        .state()
        .rows
        .get_mut(sleep.id.as_str())
        .expect("row")
        .deadline = Some(Instant::now());
    cluster
        .state()
        .objects
        .get_mut(&key(&workload_ref(&sleep)))
        .expect("workload")
        .finalizer = true;
    let harness = start(&store, &cluster);

    // Each attempt waits for absence, then reports a failure that is never
    // terminal; the next attempt follows after its backoff.
    tokio::time::sleep(Duration::from_secs(200)).await;
    assert_eq!(store.instance_state("tenant"), InstanceState::Draining);
    let failures = store.state().failures.clone();
    assert!(failures.len() >= 2);
    assert!(failures.iter().all(|failure| !failure.permanent));
    assert!(!store.state().rows[sleep.id.as_str()].parked);

    // The finalizer's owner lets go.
    cluster
        .state()
        .objects
        .get_mut(&key(&workload_ref(&sleep)))
        .expect("workload")
        .finalizer = false;
    harness
        .until(Duration::from_secs(120), "the sleep", |h| {
            h.store.instance_state("tenant") == InstanceState::Cold
        })
        .await;
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn cleanup_of_a_row_its_instance_left_behind_marks_it_deleted() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let sleep = store.add_sleep("tenant");
    cluster.materialize(&sleep);
    // The instance failed a later wake; this row is debris.
    store.state().instances.insert(
        "tenant".to_owned(),
        instance("tenant", InstanceState::Failed, 12),
    );
    let harness = start(&store, &cluster);

    harness
        .until(POD_STOP + WAKE_BOUND, "the cleanup", |h| {
            h.store.row_state(&sleep) == MaterializationState::Deleted
        })
        .await;
    assert_eq!(store.instance_state("tenant"), InstanceState::Failed);
    assert!(store.state().cold_at.is_empty());
    harness.stop().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_wake_its_instance_moved_past_deletes_what_it_applied() {
    let (store, cluster) = (FakeStore::default(), FakeCluster::default());
    let wake = store.add_wake("tenant");
    cluster.materialize(&wake);
    store.state().instances.insert(
        "tenant".to_owned(),
        instance("tenant", InstanceState::Deleting, 9),
    );
    let harness = start(&store, &cluster);

    harness
        .until(POD_STOP + WAKE_BOUND, "the cleanup", |h| {
            h.store.row_state(&wake) == MaterializationState::Deleted
        })
        .await;
    assert!(wake
        .rendered_objects
        .iter()
        .all(|object| !cluster.present(object)));
    harness.stop().await.expect("clean shutdown");
}

#[test]
fn only_an_ownership_conflict_parks_cleanup() {
    let refused = || KubernetesClientError::new("403");
    let apply = |source| {
        MaterializationReconcileError::Projection(ProjectionError::Apply {
            observations: Vec::new(),
            source: Box::new(MaterializerError::Apply {
                object: Box::new(service_ref(&row(
                    "tenant",
                    MaterializationState::Pending,
                    7,
                ))),
                source,
            }),
        })
    };
    let conflict = MaterializationReconcileError::Projection(ProjectionError::OwnershipConflict {
        observations: Vec::new(),
    });
    let pending = MaterializationState::Pending;
    let deleting = MaterializationState::Deleting;

    assert!(conflict.permanent(pending));
    assert!(conflict.permanent(deleting));
    assert!(apply(refused()).permanent(pending));
    assert!(!apply(refused()).permanent(deleting));
    assert!(!apply(KubernetesClientError::stale("503")).permanent(pending));
    assert!(!apply(KubernetesClientError::transient("429")).permanent(pending));
    assert!(MaterializationReconcileError::StaleDesiredRefs.permanent(pending));
    assert!(!MaterializationReconcileError::StaleDesiredRefs.permanent(deleting));
    assert!(!MaterializationReconcileError::Deadline.permanent(pending));
}

#[test]
fn operation_budget_uses_database_duration_and_deducts_request_time() {
    for absolute in [1, 1_000, i64::MAX] {
        let status = MaterializationWorkStatus {
            operation_deadline_unix_millis: absolute,
            operation_remaining: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        assert_eq!(
            operation_budget(Some(status.clone()), Duration::from_millis(50)).unwrap(),
            Duration::from_millis(150)
        );
        assert_eq!(
            operation_budget(Some(status), Duration::from_millis(250)).unwrap(),
            Duration::ZERO
        );
    }
    assert!(operation_budget(
        Some(MaterializationWorkStatus {
            operation_deadline_unix_millis: 1,
            ..Default::default()
        }),
        Duration::ZERO
    )
    .is_err());
    assert_eq!(
        operation_budget(None, Duration::from_secs(1)).unwrap(),
        Duration::from_secs(599)
    );
}
