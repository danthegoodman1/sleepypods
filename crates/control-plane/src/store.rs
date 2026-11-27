//! The persistence contract, one trait per consumer.
//!
//! Each consumer names the trait it calls, so a test fake implements the
//! operations of the consumer it stands in for and no others. Lifecycle reads
//! that several consumers share live in [`LifecycleReads`]. The service
//! traits at the end gather what each API service and the whole control plane
//! run on; every store that implements the parts implements them.
//!
//! A store implements every operation of a trait it takes on: there are no
//! default bodies that fail at runtime.
//!
//! ```compile_fail
//! use control_plane::store::LifecycleReads;
//! struct IncompleteStore;
//! impl LifecycleReads for IncompleteStore {}
//! ```
use crate::certificate::*;
use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

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
    workload::{
        CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, WorkloadClassVersion,
    },
};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type StoreResult<T> = Result<T, StoreError>;

/// The instance and projection reads wake, sleep, the reconciler and the
/// operator service share.
pub trait LifecycleReads: Send + Sync {
    fn get_instance(
        &self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'_, StoreResult<Option<InstanceRecord>>>;

    fn load_workload_class_version(
        &self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'_, StoreResult<Option<WorkloadClassVersion>>>;

    /// The target's projection that is not yet Deleted.
    fn load_active_materialization(
        &self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>>;

    fn load_materialization_work_status(
        &self,
        id: MaterializationId,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationWorkStatus>>>;
}

/// What a proxy's wake request needs: the Ready backend of a running instance,
/// and durable acceptance of a wake.
pub trait WakeStore: LifecycleReads {
    fn load_ready_materialization(
        &self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>>;

    fn accept_wake(
        &self,
        request: AcceptWakeRequest,
    ) -> StoreFuture<'_, StoreResult<InstanceRecord>>;
}

/// What a sidecar's idle report needs: the start of a drain.
pub trait SleepStore: LifecycleReads {
    fn begin_sleep(
        &self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'_, StoreResult<BeginSleepResult>>;
}

/// What the reconciler drives lifecycle work through: claims, leases, and the
/// fenced completions and failures of claimed work.
pub trait ReconcilerStore: LifecycleReads {
    fn claim_next_materialization_reconciliation(
        &self,
        request: ClaimNextMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>>;

    fn renew_materialization_reconciliation_lease(
        &self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'_, StoreResult<bool>>;

    fn release_materialization_reconciliation_lease(
        &self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'_, StoreResult<bool>>;

    fn complete_wake_reconciliation(
        &self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'_, StoreResult<CompleteWakeResult>>;

    fn finalize_sleep_reconciliation(
        &self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'_, StoreResult<FinalizeSleepResult>>;

    fn delete_materialization_reconciliation(
        &self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>>;

    fn record_materialization_failure(
        &self,
        request: RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>>;

    /// Removes up to `limit` Deleting instances whose cleanup has finished.
    fn finalize_instance_deletions(&self, limit: usize) -> StoreFuture<'_, StoreResult<usize>>;
}

/// What the operator service administers: classes, instances, routes,
/// HTTP-01 challenges and the audited overrides.
pub trait OperatorStore: LifecycleReads {
    fn create_workload_class_version(
        &self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'_, StoreResult<WorkloadClassVersion>>;

    fn create_instance(
        &self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'_, StoreResult<CreateInstanceResult>>;

    fn request_instance_deletion(
        &self,
        request: RequestInstanceDeletion,
    ) -> StoreFuture<'_, StoreResult<bool>>;

    fn create_route_binding(
        &self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'_, StoreResult<RouteBindingRecord>>;

    fn get_route_binding(
        &self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'_, StoreResult<Option<RouteBindingRecord>>>;

    fn delete_route_binding(
        &self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'_, StoreResult<bool>>;

    fn load_materialization(
        &self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>>;

    fn enqueue_materialization(&self, id: MaterializationId) -> StoreFuture<'_, StoreResult<bool>>;

    fn force_delete_materialization(
        &self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'_, StoreResult<Option<MaterializationRecord>>>;

    fn force_release_exclusivity_key(
        &self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'_, StoreResult<ForceReleaseExclusivityKeyResult>>;

    fn put_http01_challenge(
        &self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'_, StoreResult<Http01ChallengeRecord>>;

    fn delete_http01_challenge(
        &self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'_, StoreResult<bool>>;
}

/// What proxies resolve: the rule set of a host and an ACME HTTP-01 answer.
pub trait RouteStore: Send + Sync {
    fn resolve_host_rules(
        &self,
        request: ResolveHostRulesRequest,
    ) -> StoreFuture<'_, StoreResult<HostRules>>;

    fn resolve_http01_challenge(
        &self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'_, StoreResult<Option<Http01ChallengeRecord>>>;
}

/// Certificates and their hostname bindings: the operator's administration
/// and a proxy's resolution of one hostname.
pub trait CertificateStore: Send + Sync {
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
    /// Re-seals a batch of the active certificates held under `sealing_key_id`
    /// with the active key.
    fn reencrypt_certificates_sealed_by(
        &self,
        sealing_key_id: String,
    ) -> StoreFuture<'_, StoreResult<SealingKeyReencryption>>;
}

/// What a proxy's certificate watch polls: the bindings of its hostnames.
pub trait TlsWatchStore: Send + Sync {
    fn snapshot_tls_bindings(
        &self,
        hostnames: Vec<TlsHostname>,
        known_revision: Option<CertificateRevision>,
    ) -> StoreFuture<'_, StoreResult<Option<TlsBindingSnapshot>>>;
}

/// What the control plane's background work reads: route-change history for
/// the dispatcher, record maintenance and operational metrics.
pub trait RuntimeStore: Send + Sync {
    fn load_route_changes(
        &self,
        cursor: u64,
        limit: u32,
    ) -> StoreFuture<'_, StoreResult<DurableRouteChanges>>;

    fn load_route_change_revision(&self) -> StoreFuture<'_, StoreResult<u64>>;

    /// Wakes its waiter when route-change history may have advanced. A store
    /// that cannot push returns a signal that stays quiet, and the caller's
    /// poll finds the change.
    fn route_change_signal(&self) -> Arc<tokio::sync::Notify> {
        Arc::new(tokio::sync::Notify::new())
    }

    fn maintain_runtime_records(&self, limit: u32) -> StoreFuture<'_, StoreResult<u64>>;

    fn load_materialization_operational_metrics(
        &self,
    ) -> StoreFuture<'_, StoreResult<MaterializationOperationalMetrics>>;
}

/// The proxy service: wakes, route rules, HTTP-01 answers and certificates.
pub trait ProxyServiceStore: WakeStore + RouteStore + CertificateStore + TlsWatchStore {}
impl<T: WakeStore + RouteStore + CertificateStore + TlsWatchStore + ?Sized> ProxyServiceStore
    for T
{
}

/// The operator service: administration and certificates.
pub trait OperatorServiceStore: OperatorStore + CertificateStore {}
impl<T: OperatorStore + CertificateStore + ?Sized> OperatorServiceStore for T {}

/// The listener in-cluster workloads reach: the proxy and sidecar services.
pub trait WorkloadServiceStore: ProxyServiceStore + SleepStore {}
impl<T: ProxyServiceStore + SleepStore + ?Sized> WorkloadServiceStore for T {}

/// Everything the control plane runs on.
pub trait ControlPlaneStore:
    WorkloadServiceStore + OperatorServiceStore + ReconcilerStore + RuntimeStore
{
}
impl<T> ControlPlaneStore for T where
    T: WorkloadServiceStore + OperatorServiceStore + ReconcilerStore + RuntimeStore + ?Sized
{
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
            Self::Unavailable { message } => write!(f, "store unavailable: {message}"),
            Self::Internal { message } => write!(f, "internal store error: {message}"),
        }
    }
}

impl Error for StoreError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_dyn_safe<T: ControlPlaneStore + ?Sized>() {}

    #[test]
    fn control_plane_store_trait_is_dyn_safe() {
        assert_dyn_safe::<dyn ControlPlaneStore>();
    }

    #[test]
    fn lease_conflicts_are_not_retryable() {
        assert!(!StoreError::LeaseConflict {
            message: "ownership lost".to_owned()
        }
        .is_retryable());
        assert!(StoreError::unavailable("connection interrupted").is_retryable());
    }
}
