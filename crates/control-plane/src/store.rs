use crate::certificate::*;
use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use crate::{
    http01::{
        DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
        Http01ChallengeRecord, PutHttp01ChallengeRequest,
    },
    instance::{
        CompareAndSwapInstanceStateRequest, CreateInstanceRequest, CreateInstanceResult,
        DeleteInstanceRequest, GetInstanceRequest, InstanceRecord,
    },
    materialization::{
        BeginSleepRequest, BeginSleepResult, ClaimMaterializationReconciliationRequest,
        CompleteWakeReconciliationRequest, CompleteWakeRequest, CompleteWakeResult,
        DeleteMaterializationReconciliationRequest, FinalizeSleepReconciliationRequest,
        FinalizeSleepRequest, FinalizeSleepResult, ForceDeleteMaterializationRequest,
        ForceReleaseExclusivityKeyRequest, ForceReleaseExclusivityKeyResult,
        ListMaterializationReconciliationCandidatesRequest, LoadActiveMaterializationRequest,
        LoadMaterializationRequest, LoadReadyMaterializationRequest,
        MaterializationOperationalMetrics, MaterializationRecord, RecordMaterializationRequest,
        ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        ListRouteBindingsForInstanceRequest, ResolveRouteRequest, RouteBindingRecord,
        RouteDependencyLookup, RouteDependencySet, RouteResolution,
    },
    workload::{
        CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, WorkloadClassVersion,
    },
    RetryPolicy,
};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type StoreResult<T> = Result<T, StoreError>;

/// Complete persistence capabilities required by the production control plane.
///
/// Implementations must implement every operation explicitly. Test fixtures may
/// reject capabilities outside their declared scenario, but production stores
/// cannot silently defer missing capabilities to a runtime error.
///
/// ```compile_fail
/// use control_plane::ControlPlaneStore;
/// struct IncompleteStore;
/// impl ControlPlaneStore for IncompleteStore {}
/// ```
pub trait ControlPlaneStore: Send + Sync {
    fn publish_certificate(
        &self,
        request: PublishCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>>;
    fn get_certificate_metadata(
        &self,
        id: CertificateId,
    ) -> StoreFuture<'_, StoreResult<Option<CertificateMetadata>>>;
    fn set_tls_binding(
        &self,
        request: SetTlsBindingRequest,
    ) -> StoreFuture<'_, StoreResult<TlsBinding>>;
    fn get_tls_binding(&self, hostname: TlsHostname) -> StoreFuture<'_, StoreResult<TlsBinding>>;
    fn remove_certificate(
        &self,
        request: RemoveCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>>;
    fn resolve_tls_certificate(
        &self,
        request: ResolveTlsCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<TlsCertificateResolution>>;
    fn reencrypt_certificate(
        &self,
        request: ReencryptCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>>;

    fn snapshot_tls_bindings(
        &self,
        hostnames: Vec<TlsHostname>,
        known_revision: Option<CertificateRevision>,
    ) -> StoreFuture<'_, StoreResult<Option<TlsBindingSnapshot>>>;

    fn load_route_changes(
        &self,
        cursor: u64,
        limit: u32,
    ) -> StoreFuture<'_, StoreResult<crate::runtime_work::DurableRouteChanges>>;

    fn load_route_change_revision(&self) -> StoreFuture<'_, StoreResult<u64>>;

    fn load_materialization_work_status(
        &self,
        id: crate::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<Option<crate::runtime_work::MaterializationWorkStatus>>>;

    fn record_materialization_failure(
        &self,
        request: crate::runtime_work::RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>>;

    fn enqueue_materialization(
        &self,
        id: crate::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<bool>>;

    fn maintain_runtime_records(&self, limit: u32) -> StoreFuture<'_, StoreResult<u64>>;

    fn accept_wake<'a>(
        &'a self,
        request: crate::materialization::AcceptWakeRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>>;

    fn request_instance_deletion<'a>(
        &'a self,
        request: crate::instance::RequestInstanceDeletion,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn finalize_instance_deletions<'a>(
        &'a self,
        limit: usize,
    ) -> StoreFuture<'a, StoreResult<usize>>;

    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>>;

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>>;

    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn create_workload_class_version<'a>(
        &'a self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>>;

    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>>;

    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>>;

    fn get_route_binding<'a>(
        &'a self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>>;

    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn list_route_bindings_for_instance<'a>(
        &'a self,
        request: ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<RouteBindingRecord>>>;

    fn resolve_route<'a>(
        &'a self,
        request: ResolveRouteRequest,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>>;

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>>;

    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>>;

    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>>;

    fn load_active_materialization<'a>(
        &'a self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>>;

    fn load_materialization<'a>(
        &'a self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>>;

    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>>;

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>>;

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>>;

    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        request: ListMaterializationReconciliationCandidatesRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>>;

    fn load_materialization_operational_metrics(
        &self,
    ) -> StoreFuture<'_, StoreResult<MaterializationOperationalMetrics>>;

    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>>;

    fn begin_materialization_effect<'a>(
        &'a self,
        request: crate::materialization::MaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn acknowledge_materialization_effect<'a>(
        &'a self,
        request: crate::materialization::AcknowledgeMaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>>;

    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>>;

    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>>;

    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>>;

    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>>;

    fn lookup_route_dependencies<'a>(
        &'a self,
        request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>>;

    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>>;

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>>;

    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn expire_http01_challenges<'a>(
        &'a self,
        request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>>;
}

#[derive(Clone)]
pub struct RetryingControlPlaneStore {
    inner: Arc<dyn ControlPlaneStore>,
    policy: RetryPolicy,
}

#[derive(Debug)]
pub enum StoreError {
    SleepDeferred {
        retry_after: std::time::Duration,
    },
    InvalidArgument {
        message: String,
    },
    NotFound {
        resource: &'static str,
    },
    AlreadyExists {
        resource: &'static str,
    },
    GenerationConflict {
        expected: crate::ids::Generation,
        actual: crate::ids::Generation,
    },
    ExclusivityConflict {
        cluster_id: String,
        namespace: String,
        key_name: String,
        owner_instance_id: Option<String>,
        owner_generation: Option<crate::ids::Generation>,
    },
    IdempotencyConflict,
    IdempotencyResourceDeleted {
        resource: &'static str,
    },
    LeaseConflict {
        message: String,
    },
    Unavailable {
        message: String,
    },
    Internal {
        message: String,
    },
}

impl StoreError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

impl RetryingControlPlaneStore {
    pub fn new(inner: Arc<dyn ControlPlaneStore>, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }

    pub fn with_default_policy(inner: Arc<dyn ControlPlaneStore>) -> Self {
        Self::new(inner, RetryPolicy::default())
    }

    pub fn inner(&self) -> &Arc<dyn ControlPlaneStore> {
        &self.inner
    }

    pub fn policy(&self) -> RetryPolicy {
        self.policy
    }
}

impl fmt::Debug for RetryingControlPlaneStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryingControlPlaneStore")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl ControlPlaneStore for RetryingControlPlaneStore {
    // Certificate mutations are one-shot: an unavailable response can follow commit.
    fn publish_certificate(
        &self,
        request: PublishCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.inner.publish_certificate(request)
    }
    fn get_certificate_metadata(
        &self,
        id: CertificateId,
    ) -> StoreFuture<'_, StoreResult<Option<CertificateMetadata>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.get_certificate_metadata(id.clone())
        })
    }
    fn set_tls_binding(
        &self,
        request: SetTlsBindingRequest,
    ) -> StoreFuture<'_, StoreResult<TlsBinding>> {
        self.inner.set_tls_binding(request)
    }
    fn get_tls_binding(&self, hostname: TlsHostname) -> StoreFuture<'_, StoreResult<TlsBinding>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.get_tls_binding(hostname.clone())
        })
    }
    fn remove_certificate(
        &self,
        request: RemoveCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.inner.remove_certificate(request)
    }
    fn resolve_tls_certificate(
        &self,
        request: ResolveTlsCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<TlsCertificateResolution>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.resolve_tls_certificate(request.clone())
        })
    }
    fn reencrypt_certificate(
        &self,
        request: ReencryptCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.inner.reencrypt_certificate(request)
    }

    fn snapshot_tls_bindings(
        &self,
        hostnames: Vec<TlsHostname>,
        known_revision: Option<CertificateRevision>,
    ) -> StoreFuture<'_, StoreResult<Option<TlsBindingSnapshot>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.snapshot_tls_bindings(hostnames.clone(), known_revision)
        })
    }

    fn load_route_changes(
        &self,
        cursor: u64,
        limit: u32,
    ) -> StoreFuture<'_, StoreResult<crate::runtime_work::DurableRouteChanges>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_route_changes(cursor, limit)
        })
    }
    fn load_route_change_revision(&self) -> StoreFuture<'_, StoreResult<u64>> {
        retry_store_operation(&self.inner, self.policy, |store| {
            store.load_route_change_revision()
        })
    }
    fn load_materialization_work_status(
        &self,
        id: crate::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<Option<crate::runtime_work::MaterializationWorkStatus>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_materialization_work_status(id.clone())
        })
    }
    fn record_materialization_failure(
        &self,
        request: crate::runtime_work::RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.record_materialization_failure(request.clone())
        })
    }
    fn enqueue_materialization(
        &self,
        id: crate::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        // An ID-only reschedule must not clear a later attempt's backoff after uncertain success.
        self.inner.enqueue_materialization(id)
    }
    fn maintain_runtime_records(&self, limit: u32) -> StoreFuture<'_, StoreResult<u64>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.maintain_runtime_records(limit)
        })
    }

    fn accept_wake<'a>(
        &'a self,
        request: crate::materialization::AcceptWakeRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.accept_wake(request.clone())
        })
    }
    fn request_instance_deletion<'a>(
        &'a self,
        request: crate::instance::RequestInstanceDeletion,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.request_instance_deletion(request.clone())
        })
    }
    fn finalize_instance_deletions<'a>(
        &'a self,
        limit: usize,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.finalize_instance_deletions(limit)
        })
    }

    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        // The idempotency retention window can expire during a retry; the wrapper cannot prove replay identity.
        self.inner.create_instance(request)
    }

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.get_instance(request.clone())
        })
    }

    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        // An instance ID alone does not fence a newly created incarnation after a lost response.
        self.inner.delete_instance(request)
    }

    fn create_workload_class_version<'a>(
        &'a self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.create_workload_class_version(request.clone())
        })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_workload_class_version(request.clone())
        })
    }

    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
        // Idempotency records can expire before replay; do not recreate a deleted binding automatically.
        self.inner.create_route_binding(request)
    }

    fn get_route_binding<'a>(
        &'a self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.get_route_binding(request.clone())
        })
    }

    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        // A reused route ID could refer to a replacement after the first delete committed.
        self.inner.delete_route_binding(request)
    }

    fn list_route_bindings_for_instance<'a>(
        &'a self,
        request: ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<RouteBindingRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.list_route_bindings_for_instance(request.clone())
        })
    }

    fn resolve_route<'a>(
        &'a self,
        request: ResolveRouteRequest,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.resolve_route(request.clone())
        })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.compare_and_swap_instance_state(request.clone())
        })
    }

    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        // A same-generation upsert can overwrite newer projection state and clear its lease.
        self.inner.record_materialization(request)
    }

    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_ready_materialization(request.clone())
        })
    }

    fn load_active_materialization<'a>(
        &'a self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_active_materialization(request.clone())
        })
    }

    fn load_materialization<'a>(
        &'a self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_materialization(request.clone())
        })
    }

    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.complete_wake(request.clone())
        })
    }

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.begin_sleep(request.clone())
        })
    }

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.finalize_sleep(request.clone())
        })
    }

    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        request: ListMaterializationReconciliationCandidatesRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.list_materialization_reconciliation_candidates(request.clone())
        })
    }

    fn load_materialization_operational_metrics(
        &self,
    ) -> StoreFuture<'_, StoreResult<MaterializationOperationalMetrics>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_materialization_operational_metrics()
        })
    }

    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        // A claim has no expected incarnation/attempt fence; uncertain acquisition is not replayed.
        self.inner.claim_materialization_reconciliation(request)
    }

    fn begin_materialization_effect<'a>(
        &'a self,
        request: crate::materialization::MaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        // Dispatch is not replayed after an uncertain begin commit. The fenced
        // client can acknowledge a known-not-dispatched operation explicitly.
        self.inner.begin_materialization_effect(request)
    }

    fn acknowledge_materialization_effect<'a>(
        &'a self,
        request: crate::materialization::AcknowledgeMaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.acknowledge_materialization_effect(request.clone())
        })
    }

    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.renew_materialization_reconciliation_lease(request.clone())
        })
    }

    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.release_materialization_reconciliation_lease(request.clone())
        })
    }

    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.complete_wake_reconciliation(request.clone())
        })
    }

    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.finalize_sleep_reconciliation(request.clone())
        })
    }

    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.delete_materialization_reconciliation(request.clone())
        })
    }

    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        // An ID-only operator override must not affect a replacement after uncertain success.
        self.inner.force_delete_materialization(request)
    }

    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>> {
        // A released key may be acquired by a different owner before a retry.
        self.inner.force_release_exclusivity_key(request)
    }

    fn lookup_route_dependencies<'a>(
        &'a self,
        request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.lookup_route_dependencies(request.clone())
        })
    }

    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
        // A newer authorization may replace this host/token after an uncertain write.
        self.inner.put_http01_challenge(request)
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.resolve_http01_challenge(key.clone())
        })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        // The same host/token may already contain a replacement challenge.
        self.inner.delete_http01_challenge(request)
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.expire_http01_challenges(request.clone())
        })
    }
}

fn retry_store_operation<'a, T, O, Fut>(
    inner: &'a Arc<dyn ControlPlaneStore>,
    policy: RetryPolicy,
    mut operation: O,
) -> StoreFuture<'a, StoreResult<T>>
where
    T: Send + 'a,
    O: FnMut(&'a dyn ControlPlaneStore) -> Fut + Send + 'a,
    Fut: Future<Output = StoreResult<T>> + Send + 'a,
{
    Box::pin(async move {
        policy
            .retry_if(|| operation(inner.as_ref()), StoreError::is_retryable)
            .await
    })
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SleepDeferred { retry_after } => write!(
                f,
                "automatic sleep deferred for {} ms after activation",
                retry_after.as_millis()
            ),
            Self::InvalidArgument { message } => write!(f, "invalid store argument: {message}"),
            Self::NotFound { resource } => write!(f, "{resource} not found"),
            Self::AlreadyExists { resource } => write!(f, "{resource} already exists"),
            Self::GenerationConflict { expected, actual } => {
                write!(
                    f,
                    "generation conflict: expected generation {expected}, found {actual}"
                )
            }
            Self::ExclusivityConflict {
                cluster_id,
                namespace,
                key_name,
                owner_instance_id,
                owner_generation,
            } => {
                write!(
                    f,
                    "exclusivity key {key_name:?} is already held for target {cluster_id}/{namespace}"
                )?;
                if let Some(owner_instance_id) = owner_instance_id {
                    write!(f, " by instance {owner_instance_id}")?;
                }
                if let Some(owner_generation) = owner_generation {
                    write!(f, " generation {owner_generation}")?;
                }
                Ok(())
            }
            Self::LeaseConflict { message } => {
                write!(f, "reconciliation ownership conflict: {message}")
            }
            Self::IdempotencyResourceDeleted { resource } => {
                write!(f, "idempotent replay refers to a deleted {resource}")
            }
            Self::IdempotencyConflict => {
                f.write_str("idempotency key was already used for a different request")
            }
            Self::Unavailable { message } => write!(f, "store unavailable: {message}"),
            Self::Internal { message } => write!(f, "internal store error: {message}"),
        }
    }
}

impl Error for StoreError {}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };

    use crate::InstanceId;

    use super::*;

    fn assert_dyn_safe<T: ControlPlaneStore + ?Sized>() {}

    #[test]
    fn control_plane_store_trait_is_dyn_safe() {
        assert_dyn_safe::<dyn ControlPlaneStore>();
    }

    #[test]
    fn lease_conflicts_and_deleted_replays_are_not_retryable() {
        assert!(!StoreError::LeaseConflict {
            message: "ownership lost".to_owned()
        }
        .is_retryable());
        assert!(!StoreError::IdempotencyResourceDeleted {
            resource: "instance"
        }
        .is_retryable());
        assert!(StoreError::unavailable("connection interrupted").is_retryable());
    }

    #[tokio::test]
    async fn retrying_control_plane_store_retries_unavailable_until_success() {
        let inner = Arc::new(FakeRetryStore::with_get_instance_errors(vec![
            StoreError::unavailable("database is starting"),
            StoreError::unavailable("database is still starting"),
        ]));
        let store = RetryingControlPlaneStore::new(inner.clone(), immediate_retry_policy());

        let result = store
            .get_instance(GetInstanceRequest {
                instance_id: InstanceId::new("retry-instance").expect("instance id"),
            })
            .await
            .expect("transient unavailable errors are retried");

        assert_eq!(result, None);
        assert_eq!(inner.get_instance_calls(), 3);
    }

    #[tokio::test]
    async fn retrying_control_plane_store_does_not_retry_permanent_errors() {
        let inner = Arc::new(FakeRetryStore::with_get_instance_errors(vec![
            StoreError::invalid_argument("bad instance id"),
        ]));
        let store = RetryingControlPlaneStore::new(inner.clone(), immediate_retry_policy());

        let error = store
            .get_instance(GetInstanceRequest {
                instance_id: InstanceId::new("retry-instance").expect("instance id"),
            })
            .await
            .expect_err("permanent errors are not retried");

        assert!(matches!(error, StoreError::InvalidArgument { .. }));
        assert_eq!(inner.get_instance_calls(), 1);
    }

    #[tokio::test]
    async fn retrying_control_plane_store_stops_after_max_attempts() {
        let inner = Arc::new(FakeRetryStore::with_get_instance_errors(vec![
            StoreError::unavailable("database is down"),
            StoreError::unavailable("database is still down"),
            StoreError::unavailable("database remains down"),
        ]));
        let store = RetryingControlPlaneStore::new(
            inner.clone(),
            RetryPolicy::new(2, Duration::ZERO, Duration::ZERO),
        );

        let error = store
            .get_instance(GetInstanceRequest {
                instance_id: InstanceId::new("retry-instance").expect("instance id"),
            })
            .await
            .expect_err("last transient error is returned after attempts are exhausted");

        assert!(matches!(error, StoreError::Unavailable { .. }));
        assert_eq!(inner.get_instance_calls(), 2);
    }

    fn immediate_retry_policy() -> RetryPolicy {
        RetryPolicy::new(4, Duration::ZERO, Duration::ZERO)
    }

    #[derive(Debug)]
    struct FakeRetryStore {
        get_instance_errors: Mutex<VecDeque<StoreError>>,
        get_instance_calls: AtomicUsize,
    }

    impl FakeRetryStore {
        fn with_get_instance_errors(errors: Vec<StoreError>) -> Self {
            Self {
                get_instance_errors: Mutex::new(VecDeque::from(errors)),
                get_instance_calls: AtomicUsize::new(0),
            }
        }

        fn get_instance_calls(&self) -> usize {
            self.get_instance_calls.load(Ordering::SeqCst)
        }
    }

    impl ControlPlaneStore for FakeRetryStore {
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
            load_materialization_work_status,
            record_materialization_failure,
            enqueue_materialization,
            maintain_runtime_records,
            accept_wake,
            request_instance_deletion,
            finalize_instance_deletions,
            list_route_bindings_for_instance,
            load_materialization,
            list_materialization_reconciliation_candidates,
            load_materialization_operational_metrics,
            claim_materialization_reconciliation,
            begin_materialization_effect,
            acknowledge_materialization_effect,
            renew_materialization_reconciliation_lease,
            release_materialization_reconciliation_lease,
            complete_wake_reconciliation,
            finalize_sleep_reconciliation,
            delete_materialization_reconciliation,
            force_delete_materialization,
            force_release_exclusivity_key
        );

        fn create_instance<'a>(
            &'a self,
            _request: CreateInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
            not_implemented()
        }

        fn get_instance<'a>(
            &'a self,
            _request: GetInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
            self.get_instance_calls.fetch_add(1, Ordering::SeqCst);
            let error = self
                .get_instance_errors
                .lock()
                .expect("fake store lock")
                .pop_front();

            Box::pin(async move {
                match error {
                    Some(error) => Err(error),
                    None => Ok(None),
                }
            })
        }

        fn delete_instance<'a>(
            &'a self,
            _request: DeleteInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn create_workload_class_version<'a>(
            &'a self,
            _request: CreateWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
            not_implemented()
        }

        fn load_workload_class_version<'a>(
            &'a self,
            _request: LoadWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
            not_implemented()
        }

        fn create_route_binding<'a>(
            &'a self,
            _request: CreateRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
            not_implemented()
        }

        fn get_route_binding<'a>(
            &'a self,
            _request: GetRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
            not_implemented()
        }

        fn delete_route_binding<'a>(
            &'a self,
            _request: DeleteRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn resolve_route<'a>(
            &'a self,
            _request: ResolveRouteRequest,
        ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
            not_implemented()
        }

        fn compare_and_swap_instance_state<'a>(
            &'a self,
            _request: CompareAndSwapInstanceStateRequest,
        ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
            not_implemented()
        }

        fn record_materialization<'a>(
            &'a self,
            _request: RecordMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
            not_implemented()
        }

        fn load_ready_materialization<'a>(
            &'a self,
            _request: LoadReadyMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn load_active_materialization<'a>(
            &'a self,
            _request: LoadActiveMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn complete_wake<'a>(
            &'a self,
            _request: CompleteWakeRequest,
        ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
            not_implemented()
        }

        fn begin_sleep<'a>(
            &'a self,
            _request: BeginSleepRequest,
        ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
            not_implemented()
        }

        fn finalize_sleep<'a>(
            &'a self,
            _request: FinalizeSleepRequest,
        ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
            not_implemented()
        }

        fn lookup_route_dependencies<'a>(
            &'a self,
            _request: RouteDependencyLookup,
        ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
            not_implemented()
        }

        fn put_http01_challenge<'a>(
            &'a self,
            _request: PutHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
            not_implemented()
        }

        fn resolve_http01_challenge<'a>(
            &'a self,
            _key: Http01ChallengeKey,
        ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
            not_implemented()
        }

        fn delete_http01_challenge<'a>(
            &'a self,
            _request: DeleteHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn expire_http01_challenges<'a>(
            &'a self,
            _request: ExpireHttp01ChallengesRequest,
        ) -> StoreFuture<'a, StoreResult<usize>> {
            not_implemented()
        }
    }

    fn not_implemented<'a, T>() -> StoreFuture<'a, StoreResult<T>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }
}
