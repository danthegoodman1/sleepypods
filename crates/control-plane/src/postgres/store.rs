//! The store traits over the Postgres operations. Each operation either
//! repeats while the database is unavailable or runs once; an `Unavailable`
//! answer can follow a commit, so only operations that are safe to repeat
//! repeat.
use std::future::Future;

use crate::certificate::*;
use crate::{
    http01::{
        DeleteHttp01ChallengeRequest, Http01ChallengeKey, Http01ChallengeRecord,
        PutHttp01ChallengeRequest,
    },
    ids::MaterializationId,
    instance::{
        CreateInstanceRequest, CreateInstanceResult, GetInstanceRequest, InstanceRecord,
        RequestInstanceDeletion,
    },
    materialization::{
        AcceptWakeRequest, BeginSleepRequest, BeginSleepResult, ClaimNextMaterializationRequest,
        CompleteWakeReconciliationRequest, CompleteWakeResult,
        DeleteMaterializationReconciliationRequest, FinalizeSleepReconciliationRequest,
        FinalizeSleepResult, ForceDeleteMaterializationRequest, ForceReleaseExclusivityKeyRequest,
        ForceReleaseExclusivityKeyResult, LoadActiveMaterializationRequest,
        LoadMaterializationRequest, LoadReadyMaterializationRequest,
        MaterializationOperationalMetrics, MaterializationRecord,
        ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest, HostRules,
        ResolveHostRulesRequest, RouteBindingRecord,
    },
    runtime_work::{DurableRouteChanges, MaterializationWorkStatus, RecordMaterializationFailure},
    store::{
        CertificateStore, LifecycleReads, OperatorStore, ReconcilerStore, RouteStore, RuntimeStore,
        SleepStore, StoreError, StoreFuture, StoreResult, TlsWatchStore, WakeStore,
    },
    workload::{
        CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, WorkloadClassVersion,
    },
};

use super::{
    certificate_ops, connection::PostgresStore, http01_ops, instance_ops, lifecycle_ops,
    materialization_ops, route_ops, runtime_work,
};

impl PostgresStore {
    /// Runs an operation that is safe to repeat, again while the database
    /// is unavailable, as the store's retry policy allows.
    fn repeated<'a, T, F, Fut>(&'a self, mut operation: F) -> StoreFuture<'a, StoreResult<T>>
    where
        T: Send + 'a,
        F: FnMut() -> Fut + Send + 'a,
        Fut: Future<Output = StoreResult<T>> + Send + 'a,
    {
        Box::pin(async move {
            self.retry
                .retry_if(&mut operation, StoreError::is_retryable)
                .await
        })
    }

    /// Runs an operation once: after an uncertain answer, a repeat could act
    /// on a resource that has since changed hands.
    fn once<'a, T>(
        &'a self,
        operation: impl Future<Output = StoreResult<T>> + Send + 'a,
    ) -> StoreFuture<'a, StoreResult<T>> {
        Box::pin(operation)
    }
}

impl LifecycleReads for PostgresStore {
    fn get_instance(
        &self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'_, StoreResult<Option<InstanceRecord>>> {
        self.repeated(move || instance_ops::get_instance(self, request.clone()))
    }

    fn load_workload_class_version(
        &self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'_, StoreResult<Option<WorkloadClassVersion>>> {
        self.repeated(move || instance_ops::load_workload_class_version(self, request.clone()))
    }

    fn load_active_materialization(
        &self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>> {
        self.repeated(move || {
            materialization_ops::load_active_materialization(self, request.clone())
        })
    }

    fn load_materialization_work_status(
        &self,
        id: MaterializationId,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationWorkStatus>>> {
        self.repeated(move || runtime_work::status(self, id.clone()))
    }
}

impl WakeStore for PostgresStore {
    fn load_ready_materialization(
        &self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>> {
        self.repeated(move || {
            materialization_ops::load_ready_materialization(self, request.clone())
        })
    }

    /// A repeat after a lost answer finds the wake accepted and returns it.
    fn accept_wake(
        &self,
        request: AcceptWakeRequest,
    ) -> StoreFuture<'_, StoreResult<InstanceRecord>> {
        self.repeated(move || lifecycle_ops::accept_wake(self, request.clone()))
    }
}

impl SleepStore for PostgresStore {
    /// A repeat after a lost answer meets a generation conflict, which the
    /// caller reads as a drain already under way.
    fn begin_sleep(
        &self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'_, StoreResult<BeginSleepResult>> {
        self.repeated(move || materialization_ops::begin_sleep(self, request.clone()))
    }
}

impl ReconcilerStore for PostgresStore {
    /// Runs once: a claim whose answer is lost leaves a lease nobody renews,
    /// and a repeat would take a second row. The lost lease expires.
    fn claim_next_materialization_reconciliation(
        &self,
        request: ClaimNextMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>> {
        self.once(materialization_ops::claim_next_materialization_reconciliation(self, request))
    }

    fn renew_materialization_reconciliation_lease(
        &self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.repeated(move || {
            materialization_ops::renew_materialization_reconciliation_lease(self, request.clone())
        })
    }

    fn release_materialization_reconciliation_lease(
        &self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.repeated(move || {
            materialization_ops::release_materialization_reconciliation_lease(self, request.clone())
        })
    }

    fn complete_wake_reconciliation(
        &self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'_, StoreResult<CompleteWakeResult>> {
        self.repeated(move || {
            materialization_ops::complete_wake_reconciliation(self, request.clone())
        })
    }

    fn finalize_sleep_reconciliation(
        &self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'_, StoreResult<FinalizeSleepResult>> {
        self.repeated(move || {
            materialization_ops::finalize_sleep_reconciliation(self, request.clone())
        })
    }

    fn delete_materialization_reconciliation(
        &self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>> {
        self.repeated(move || {
            materialization_ops::delete_materialization_reconciliation(self, request.clone())
        })
    }

    fn record_materialization_failure(
        &self,
        request: RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.repeated(move || runtime_work::record_failure(self, request.clone()))
    }

    fn finalize_instance_deletions(&self, limit: usize) -> StoreFuture<'_, StoreResult<usize>> {
        self.repeated(move || lifecycle_ops::finalize_instance_deletions(self, limit))
    }
}

impl OperatorStore for PostgresStore {
    fn create_workload_class_version(
        &self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'_, StoreResult<WorkloadClassVersion>> {
        self.repeated(move || instance_ops::create_workload_class_version(self, request.clone()))
    }

    /// Runs once: after a lost answer and a delete, a repeat would create the
    /// instance again.
    fn create_instance(
        &self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'_, StoreResult<CreateInstanceResult>> {
        self.once(instance_ops::create_instance(self, request))
    }

    fn request_instance_deletion(
        &self,
        request: RequestInstanceDeletion,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.repeated(move || lifecycle_ops::request_instance_deletion(self, request.clone()))
    }

    /// Runs once: after a lost answer and a delete, a repeat would create the
    /// binding again.
    fn create_route_binding(
        &self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'_, StoreResult<RouteBindingRecord>> {
        self.once(route_ops::create_route_binding(self, request))
    }

    fn get_route_binding(
        &self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'_, StoreResult<Option<RouteBindingRecord>>> {
        self.repeated(move || route_ops::get_route_binding(self, request.clone()))
    }

    /// Runs once: the ID may name a replacement once the first delete has
    /// committed.
    fn delete_route_binding(
        &self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.once(route_ops::delete_route_binding(self, request))
    }

    fn load_materialization(
        &self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>> {
        self.repeated(move || materialization_ops::load_materialization(self, request.clone()))
    }

    /// Runs once: a repeat after a lost answer could clear the backoff of the
    /// attempt that followed.
    fn enqueue_materialization(&self, id: MaterializationId) -> StoreFuture<'_, StoreResult<bool>> {
        self.once(runtime_work::enqueue(self, id))
    }

    /// Runs once: the ID may name a replacement once the first override has
    /// committed.
    fn force_delete_materialization(
        &self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>> {
        self.once(materialization_ops::force_delete_materialization(
            self, request,
        ))
    }

    /// Runs once: another owner may take the released key before a repeat.
    fn force_release_exclusivity_key(
        &self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'_, StoreResult<ForceReleaseExclusivityKeyResult>> {
        self.once(materialization_ops::force_release_exclusivity_key(
            self, request,
        ))
    }

    /// Runs once: a newer authorization may replace this host and token
    /// after an uncertain write.
    fn put_http01_challenge(
        &self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'_, StoreResult<Http01ChallengeRecord>> {
        self.once(http01_ops::put_http01_challenge(self, request))
    }

    /// Runs once: the host and token may already hold a replacement.
    fn delete_http01_challenge(
        &self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.once(http01_ops::delete_http01_challenge(self, request))
    }
}

impl RouteStore for PostgresStore {
    fn resolve_host_rules(
        &self,
        request: ResolveHostRulesRequest,
    ) -> StoreFuture<'_, StoreResult<HostRules>> {
        self.repeated(move || route_ops::resolve_host_rules(self, request.clone()))
    }

    fn resolve_http01_challenge(
        &self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'_, StoreResult<Option<Http01ChallengeRecord>>> {
        self.repeated(move || http01_ops::resolve_http01_challenge(self, key.clone()))
    }
}

/// Certificate writes run once: after an uncertain answer the caller reads the
/// metadata before it chooses another conditional write.
impl CertificateStore for PostgresStore {
    fn publish_certificate(
        &self,
        request: PublishCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.once(certificate_ops::publish(self, request))
    }
    fn get_certificate_metadata(
        &self,
        id: CertificateId,
    ) -> StoreFuture<'_, StoreResult<Option<CertificateMetadata>>> {
        self.repeated(move || certificate_ops::get_metadata(self, id.clone()))
    }
    fn set_tls_binding(
        &self,
        request: SetTlsBindingRequest,
    ) -> StoreFuture<'_, StoreResult<TlsBinding>> {
        self.once(certificate_ops::set_binding(self, request))
    }
    fn get_tls_binding(&self, hostname: TlsHostname) -> StoreFuture<'_, StoreResult<TlsBinding>> {
        self.repeated(move || certificate_ops::get_binding(self, hostname.clone()))
    }
    fn remove_certificate(
        &self,
        request: RemoveCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.once(certificate_ops::remove(self, request))
    }
    fn resolve_tls_certificate(
        &self,
        request: ResolveTlsCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<TlsCertificateResolution>> {
        self.repeated(move || certificate_ops::resolve(self, request.clone()))
    }
    fn reencrypt_certificate(
        &self,
        request: ReencryptCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.once(certificate_ops::reencrypt(self, request))
    }
    fn reencrypt_certificates_sealed_by(
        &self,
        sealing_key_id: String,
    ) -> StoreFuture<'_, StoreResult<SealingKeyReencryption>> {
        self.once(certificate_ops::reencrypt_sealed_by(self, sealing_key_id))
    }
}

impl TlsWatchStore for PostgresStore {
    fn snapshot_tls_bindings(
        &self,
        hostnames: Vec<TlsHostname>,
        known_revision: Option<CertificateRevision>,
    ) -> StoreFuture<'_, StoreResult<Option<TlsBindingSnapshot>>> {
        self.repeated(move || certificate_ops::snapshot(self, hostnames.clone(), known_revision))
    }
}

impl RuntimeStore for PostgresStore {
    fn load_route_changes(
        &self,
        cursor: u64,
        limit: u32,
    ) -> StoreFuture<'_, StoreResult<DurableRouteChanges>> {
        self.repeated(move || runtime_work::changes(self, cursor, limit))
    }

    fn load_route_change_revision(&self) -> StoreFuture<'_, StoreResult<u64>> {
        self.repeated(move || runtime_work::revision(self))
    }

    fn route_change_signal(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.route_changes.subscribe()
    }

    /// Each pass holds its batch limit; a repeat counts only what it removed.
    fn maintain_runtime_records(&self, limit: u32) -> StoreFuture<'_, StoreResult<u64>> {
        self.repeated(move || runtime_work::maintain(self, limit))
    }

    fn load_materialization_operational_metrics(
        &self,
    ) -> StoreFuture<'_, StoreResult<MaterializationOperationalMetrics>> {
        self.repeated(move || materialization_ops::load_materialization_operational_metrics(self))
    }
}
