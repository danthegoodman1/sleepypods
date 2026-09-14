pub use sleepypods_api::materialization::{
    BackendEndpoint, InvalidMaterializationTarget, MaterializationTarget,
};

use std::time::{Duration, SystemTime};

use crate::ids::{BackendGeneration, Generation, InstanceId, MaterializationId};
use crate::instance::InstanceRecord;
use crate::workload::RenderedExclusivityKey;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationRecord {
    pub id: MaterializationId,
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    /// Immutable ownership/sidecar incarnation; independent of the instance CAS revision.
    pub projection_generation: Generation,
    pub target: MaterializationTarget,
    pub state: MaterializationState,
    pub backend: Option<BackendEndpoint>,
    pub backend_generation: BackendGeneration,
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub exclusivity_keys: Vec<RenderedExclusivityKey>,
    pub reconciliation_lease: Option<MaterializationReconciliationLease>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordMaterializationRequest {
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    /// Immutable ownership/sidecar incarnation; independent of the instance CAS revision.
    pub projection_generation: Generation,
    pub target: MaterializationTarget,
    pub state: MaterializationState,
    pub backend: Option<BackendEndpoint>,
    pub backend_generation: BackendGeneration,
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub exclusivity_keys: Vec<RenderedExclusivityKey>,
}

/// The immutable projection prepared before accepting a wake. The store commits
/// both the state transition and discoverable work in one transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptWakeRequest {
    pub expected_generation: Generation,
    pub pending: RecordMaterializationRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteWakeRequest {
    pub instance_id: InstanceId,
    pub expected_waking_generation: Generation,
    pub target: MaterializationTarget,
    pub backend: BackendEndpoint,
    pub backend_generation: BackendGeneration,
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub exclusivity_keys: Vec<RenderedExclusivityKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadReadyMaterializationRequest {
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadActiveMaterializationRequest {
    pub instance_id: InstanceId,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadMaterializationRequest {
    pub materialization_id: MaterializationId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginSleepRequest {
    pub instance_id: InstanceId,
    pub expected_running_generation: Generation,
    pub target: MaterializationTarget,
    pub drain_grace_timeout: Duration,
    /// Automatic idle sleep only: elapsed time since this generation became
    /// Ready, checked under the same transaction as the Running transition.
    /// Explicit operator sleep leaves this unset.
    pub minimum_ready_age: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeSleepRequest {
    pub instance_id: InstanceId,
    pub expected_draining_generation: Generation,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationReconciliationLease {
    pub owner: String,
    pub expires_at: SystemTime,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListMaterializationReconciliationCandidatesRequest {
    pub target: Option<MaterializationTarget>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationOperationalMetrics {
    pub uncertain_effects: u64,
    pub blocked_failures: u64,
    pub backlog_states: Vec<MaterializationBacklogOperationalMetrics>,
    pub held_key_states: Vec<MaterializationHeldKeysOperationalMetrics>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationBacklogOperationalMetrics {
    pub state: MaterializationState,
    pub count: u64,
    pub oldest_age: Option<std::time::Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationHeldKeysOperationalMetrics {
    pub state: MaterializationState,
    pub exclusivity_keys_held: u64,
}

/// The longest lease a store must accept, matching the store's other timeout
/// bounds. Anything longer outlives the process that would renew it.
pub const MAX_RECONCILIATION_LEASE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Reconciliation leases are requested as a lifetime, never as an instant. The
/// store starts every lease from its own clock so one process's offset cannot
/// lengthen or shorten the window other processes wait before reclaiming work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimMaterializationReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub lease_ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewMaterializationReconciliationLeaseRequest {
    pub expected_state: MaterializationState,
    pub instance_generation: Generation,
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub lease_ttl: Duration,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseMaterializationReconciliationLeaseRequest {
    pub instance_generation: Generation,
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteWakeReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub lease_owner: String,
    pub complete: CompleteWakeRequest,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeSleepReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub lease_owner: String,
    pub finalize: FinalizeSleepRequest,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteMaterializationReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub lease_owner: String,
    pub expected_state: MaterializationState,
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    pub target: MaterializationTarget,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceDeleteMaterializationRequest {
    pub materialization_id: MaterializationId,
    pub operator: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceReleaseExclusivityKeyRequest {
    pub target: MaterializationTarget,
    pub key_name: String,
    pub key_value: String,
    pub operator: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceReleaseExclusivityKeyResult {
    pub updated_materializations: usize,
    pub affected_materializations: Vec<MaterializationRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteWakeResult {
    pub instance: InstanceRecord,
    pub materialization: MaterializationRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginSleepResult {
    pub instance: InstanceRecord,
    pub materialization: Option<MaterializationRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeSleepResult {
    pub instance: InstanceRecord,
    pub materialization: Option<MaterializationRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MaterializationState {
    Pending,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedObjectRef {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

impl RecordMaterializationRequest {
    pub fn new(
        instance_id: InstanceId,
        instance_generation: Generation,
        target: MaterializationTarget,
        state: MaterializationState,
        backend_generation: BackendGeneration,
    ) -> Self {
        Self {
            instance_id,
            instance_generation,
            projection_generation: if state == MaterializationState::Pending {
                instance_generation.next()
            } else {
                instance_generation
            },
            target,
            state,
            backend: None,
            backend_generation,
            rendered_objects: Vec::new(),
            exclusivity_keys: Vec::new(),
        }
    }
}

impl CompleteWakeRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_waking_generation: Generation,
        target: MaterializationTarget,
        backend: BackendEndpoint,
        backend_generation: BackendGeneration,
    ) -> Self {
        Self {
            instance_id,
            expected_waking_generation,
            target,
            backend,
            backend_generation,
            rendered_objects: Vec::new(),
            exclusivity_keys: Vec::new(),
        }
    }
}

impl LoadReadyMaterializationRequest {
    pub fn new(
        instance_id: InstanceId,
        instance_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            instance_generation,
            target,
        }
    }
}

impl LoadActiveMaterializationRequest {
    pub fn new(instance_id: InstanceId, target: MaterializationTarget) -> Self {
        Self {
            instance_id,
            target,
        }
    }
}

impl LoadMaterializationRequest {
    pub fn new(materialization_id: MaterializationId) -> Self {
        Self { materialization_id }
    }
}

impl BeginSleepRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_running_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            expected_running_generation,
            target,
            drain_grace_timeout: Duration::ZERO,
            minimum_ready_age: None,
        }
    }

    pub fn with_drain_grace_timeout(mut self, drain_grace_timeout: Duration) -> Self {
        self.drain_grace_timeout = drain_grace_timeout;
        self
    }

    pub fn with_minimum_ready_age(mut self, minimum_ready_age: Duration) -> Self {
        self.minimum_ready_age = Some(minimum_ready_age);
        self
    }
}

impl FinalizeSleepRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_draining_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            expected_draining_generation,
            target,
        }
    }
}

impl ListMaterializationReconciliationCandidatesRequest {
    pub fn for_target(mut self, target: MaterializationTarget) -> Self {
        self.target = Some(target);
        self
    }
    pub fn new(limit: usize) -> Self {
        Self {
            target: None,
            limit,
        }
    }
}

impl MaterializationOperationalMetrics {
    pub fn new(
        backlog_states: Vec<MaterializationBacklogOperationalMetrics>,
        held_key_states: Vec<MaterializationHeldKeysOperationalMetrics>,
    ) -> Self {
        Self {
            backlog_states,
            held_key_states,
            uncertain_effects: 0,
            blocked_failures: 0,
        }
    }
}

impl MaterializationBacklogOperationalMetrics {
    pub fn new(
        state: MaterializationState,
        count: u64,
        oldest_age: Option<std::time::Duration>,
    ) -> Self {
        Self {
            state,
            count,
            oldest_age,
        }
    }
}

impl MaterializationHeldKeysOperationalMetrics {
    pub fn new(state: MaterializationState, exclusivity_keys_held: u64) -> Self {
        Self {
            state,
            exclusivity_keys_held,
        }
    }
}

impl ClaimMaterializationReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        owner: impl Into<String>,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            materialization_id,
            owner: owner.into(),
            lease_ttl,
        }
    }
}

impl RenewMaterializationReconciliationLeaseRequest {
    pub fn new(
        materialization_id: MaterializationId,
        owner: impl Into<String>,
        attempt: u64,
        instance_generation: Generation,
        lease_ttl: Duration,
        expected_state: MaterializationState,
    ) -> Self {
        Self {
            expected_state,
            instance_generation,
            attempt,
            materialization_id,
            owner: owner.into(),
            lease_ttl,
        }
    }
}

impl ReleaseMaterializationReconciliationLeaseRequest {
    pub fn new(
        materialization_id: MaterializationId,
        owner: impl Into<String>,
        attempt: u64,
        instance_generation: Generation,
    ) -> Self {
        Self {
            instance_generation,
            attempt,
            materialization_id,
            owner: owner.into(),
        }
    }
}

impl CompleteWakeReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        lease_owner: impl Into<String>,
        attempt: u64,
        complete: CompleteWakeRequest,
    ) -> Self {
        Self {
            attempt,
            materialization_id,
            lease_owner: lease_owner.into(),
            complete,
        }
    }
}

impl FinalizeSleepReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        lease_owner: impl Into<String>,
        attempt: u64,
        finalize: FinalizeSleepRequest,
    ) -> Self {
        Self {
            attempt,
            materialization_id,
            lease_owner: lease_owner.into(),
            finalize,
        }
    }
}

impl DeleteMaterializationReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        lease_owner: impl Into<String>,
        attempt: u64,
        expected_state: MaterializationState,
        instance_id: InstanceId,
        instance_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            attempt,
            materialization_id,
            lease_owner: lease_owner.into(),
            expected_state,
            instance_id,
            instance_generation,
            target,
        }
    }
}

impl ForceDeleteMaterializationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        operator: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            materialization_id,
            operator: operator.into(),
            reason: reason.into(),
        }
    }
}

impl ForceReleaseExclusivityKeyRequest {
    pub fn new(
        target: MaterializationTarget,
        key_name: impl Into<String>,
        key_value: impl Into<String>,
        operator: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            target,
            key_name: key_name.into(),
            key_value: key_value.into(),
            operator: operator.into(),
            reason: reason.into(),
        }
    }
}

impl MaterializationState {
    pub const BACKLOG_STATES: &'static [Self] = &[Self::Pending, Self::Deleting];
    pub const HELD_KEY_STATES: &'static [Self] =
        &[Self::Pending, Self::Ready, Self::Failed, Self::Deleting];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }

    pub const fn metric_label(self) -> sleepypods_observability::metrics::MetricLabel {
        sleepypods_observability::metrics::MetricLabel::state(self.as_str())
    }
}

/// One potentially dispatched mutation. This durable barrier survives lease expiry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationEffectRequest {
    pub effect_id: u64,
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub attempt: u64,
    pub instance_generation: Generation,
    pub expected_state: MaterializationState,
    pub operation: &'static str,
    pub object: RenderedObjectRef,
    pub precondition: Option<crate::projection::LiveObjectIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcknowledgeMaterializationEffectRequest {
    pub instance_generation: Generation,
    pub effect_id: u64,
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub attempt: u64,
}
