use control_plane::certificate::*;
#[macro_use]
#[path = "support/unexpected_store.rs"]
mod unexpected_store;
mod support;
use control_plane::{materialization::*, *};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

#[derive(Default)]
struct CapabilityProbe {
    calls: Mutex<Vec<(&'static str, String)>>,
}
impl CapabilityProbe {
    fn fail<T>(&self, name: &'static str, arguments: String) -> StoreFuture<'_, StoreResult<T>> {
        self.calls.lock().unwrap().push((name, arguments));
        Box::pin(async move { Err(StoreError::unavailable(format!("probe {name}"))) })
    }
    fn check(&self, name: &'static str, arguments: String, attempts: usize) {
        let calls = std::mem::take(&mut *self.calls.lock().unwrap());
        assert_eq!(
            calls,
            vec![(name, arguments); attempts],
            "forwarding/replay policy for {name}"
        );
    }
}
impl ControlPlaneStore for CapabilityProbe {
    fn publish_certificate(
        &self,
        request: PublishCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.fail("publish_certificate", format!("{request:?}"))
    }
    fn get_certificate_metadata(
        &self,
        id: CertificateId,
    ) -> StoreFuture<'_, StoreResult<Option<CertificateMetadata>>> {
        self.fail("get_certificate_metadata", format!("{id:?}"))
    }
    fn set_tls_binding(
        &self,
        request: SetTlsBindingRequest,
    ) -> StoreFuture<'_, StoreResult<TlsBinding>> {
        self.fail("set_tls_binding", format!("{request:?}"))
    }
    fn get_tls_binding(&self, hostname: TlsHostname) -> StoreFuture<'_, StoreResult<TlsBinding>> {
        self.fail("get_tls_binding", format!("{hostname:?}"))
    }
    fn remove_certificate(
        &self,
        request: RemoveCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.fail("remove_certificate", format!("{request:?}"))
    }
    fn resolve_tls_certificate(
        &self,
        request: ResolveTlsCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<TlsCertificateResolution>> {
        self.fail("resolve_tls_certificate", format!("{request:?}"))
    }
    fn reencrypt_certificate(
        &self,
        request: ReencryptCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        self.fail("reencrypt_certificate", format!("{request:?}"))
    }

    fn snapshot_tls_bindings(
        &self,
        hosts: Vec<TlsHostname>,
        known: Option<CertificateRevision>,
    ) -> StoreFuture<'_, StoreResult<Option<TlsBindingSnapshot>>> {
        self.fail("snapshot_tls_bindings", format!("{hosts:?}, {known:?}"))
    }

    fn load_route_changes(
        &self,
        cursor: u64,
        limit: u32,
    ) -> StoreFuture<'_, StoreResult<control_plane::runtime_work::DurableRouteChanges>> {
        self.fail("load_route_changes", format!("{cursor:?}, {limit:?}"))
    }
    fn load_route_change_revision(&self) -> StoreFuture<'_, StoreResult<u64>> {
        self.fail("load_route_change_revision", "()".to_owned())
    }
    fn load_materialization_work_status(
        &self,
        id: control_plane::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<Option<control_plane::runtime_work::MaterializationWorkStatus>>>
    {
        self.fail("load_materialization_work_status", format!("{id:?}"))
    }
    fn record_materialization_failure(
        &self,
        request: control_plane::runtime_work::RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.fail("record_materialization_failure", format!("{request:?}"))
    }
    fn enqueue_materialization(
        &self,
        id: control_plane::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        self.fail("enqueue_materialization", format!("{id:?}"))
    }
    fn maintain_runtime_records(&self, limit: u32) -> StoreFuture<'_, StoreResult<u64>> {
        self.fail("maintain_runtime_records", format!("{limit:?}"))
    }
    fn accept_wake<'a>(
        &'a self,
        request: control_plane::materialization::AcceptWakeRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        self.fail("accept_wake", format!("{request:?}"))
    }
    fn request_instance_deletion<'a>(
        &'a self,
        request: control_plane::instance::RequestInstanceDeletion,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail("request_instance_deletion", format!("{request:?}"))
    }
    fn finalize_instance_deletions<'a>(
        &'a self,
        limit: usize,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        self.fail("finalize_instance_deletions", format!("{limit:?}"))
    }
    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        self.fail("create_instance", format!("{request:?}"))
    }
    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        self.fail("get_instance", format!("{request:?}"))
    }
    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail("delete_instance", format!("{request:?}"))
    }
    fn create_workload_class_version<'a>(
        &'a self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
        self.fail("create_workload_class_version", format!("{request:?}"))
    }
    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        self.fail("load_workload_class_version", format!("{request:?}"))
    }
    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
        self.fail("create_route_binding", format!("{request:?}"))
    }
    fn get_route_binding<'a>(
        &'a self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
        self.fail("get_route_binding", format!("{request:?}"))
    }
    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail("delete_route_binding", format!("{request:?}"))
    }
    fn list_route_bindings_for_instance<'a>(
        &'a self,
        request: ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<RouteBindingRecord>>> {
        self.fail("list_route_bindings_for_instance", format!("{request:?}"))
    }
    fn resolve_route<'a>(
        &'a self,
        request: ResolveRouteRequest,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
        self.fail("resolve_route", format!("{request:?}"))
    }
    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        self.fail("compare_and_swap_instance_state", format!("{request:?}"))
    }
    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        self.fail("record_materialization", format!("{request:?}"))
    }
    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        self.fail("load_ready_materialization", format!("{request:?}"))
    }
    fn load_active_materialization<'a>(
        &'a self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        self.fail("load_active_materialization", format!("{request:?}"))
    }
    fn load_materialization<'a>(
        &'a self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        self.fail("load_materialization", format!("{request:?}"))
    }
    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        self.fail("complete_wake", format!("{request:?}"))
    }
    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        self.fail("begin_sleep", format!("{request:?}"))
    }
    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        self.fail("finalize_sleep", format!("{request:?}"))
    }
    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        request: ListMaterializationReconciliationCandidatesRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
        self.fail(
            "list_materialization_reconciliation_candidates",
            format!("{request:?}"),
        )
    }
    fn load_materialization_operational_metrics(
        &self,
    ) -> StoreFuture<'_, StoreResult<MaterializationOperationalMetrics>> {
        self.fail("load_materialization_operational_metrics", "()".to_owned())
    }
    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        self.fail(
            "claim_materialization_reconciliation",
            format!("{request:?}"),
        )
    }
    fn begin_materialization_effect<'a>(
        &'a self,
        request: control_plane::materialization::MaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail("begin_materialization_effect", format!("{request:?}"))
    }
    fn acknowledge_materialization_effect<'a>(
        &'a self,
        request: control_plane::materialization::AcknowledgeMaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail("acknowledge_materialization_effect", format!("{request:?}"))
    }
    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail(
            "renew_materialization_reconciliation_lease",
            format!("{request:?}"),
        )
    }
    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail(
            "release_materialization_reconciliation_lease",
            format!("{request:?}"),
        )
    }
    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        self.fail("complete_wake_reconciliation", format!("{request:?}"))
    }
    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        self.fail("finalize_sleep_reconciliation", format!("{request:?}"))
    }
    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        self.fail(
            "delete_materialization_reconciliation",
            format!("{request:?}"),
        )
    }
    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        self.fail("force_delete_materialization", format!("{request:?}"))
    }
    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>> {
        self.fail("force_release_exclusivity_key", format!("{request:?}"))
    }
    fn lookup_route_dependencies<'a>(
        &'a self,
        request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
        self.fail("lookup_route_dependencies", format!("{request:?}"))
    }
    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
        self.fail("put_http01_challenge", format!("{request:?}"))
    }
    fn resolve_http01_challenge<'a>(
        &'a self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
        self.fail("resolve_http01_challenge", format!("{key:?}"))
    }
    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        self.fail("delete_http01_challenge", format!("{request:?}"))
    }
    fn expire_http01_challenges<'a>(
        &'a self,
        request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        self.fail("expire_http01_challenges", format!("{request:?}"))
    }
}

#[tokio::test]
async fn every_required_capability_forwards_arguments_and_has_an_explicit_replay_policy() {
    let probe = Arc::new(CapabilityProbe::default());
    let store = RetryingControlPlaneStore::new(
        probe.clone(),
        RetryPolicy::new(2, Duration::ZERO, Duration::ZERO),
    );
    let instance = InstanceId::new("capability-instance").unwrap();
    let materialization = MaterializationId::new("capability-materialization").unwrap();
    let route = RouteBindingId::new("capability-route").unwrap();
    let generation = Generation::new(17);
    let target = MaterializationTarget::new("capability-cluster", "capability-namespace").unwrap();
    let identity = RouteIdentity::Http {
        host: RouteHost::exact("capability.example").unwrap(),
        path: Some(PathPrefix::new("/proof").unwrap()),
    };
    let class = support::workload_class();
    let record = RecordMaterializationRequest::new(
        instance.clone(),
        generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(29),
    );
    let complete = CompleteWakeRequest::new(
        instance.clone(),
        generation,
        target.clone(),
        BackendEndpoint::new("http://capability.example:8080").unwrap(),
        BackendGeneration::new(31),
    );
    let finalize = FinalizeSleepRequest::new(instance.clone(), generation, target.clone());
    let now = SystemTime::now();
    let key = Http01ChallengeKey::new("challenge.example", "token-41").unwrap();
    macro_rules! check {
        ($attempts:literal, $method:ident, $argument:expr) => {{
            let argument = $argument;
            let expected = format!("{argument:?}");
            let error = store
                .$method(argument)
                .await
                .expect_err("probe error is forwarded");
            assert_eq!(
                error.to_string(),
                concat!("store unavailable: probe ", stringify!($method))
            );
            probe.check(stringify!($method), expected, $attempts);
        }};
    }
    let cert_id = CertificateId::new("certificate-probe").unwrap();
    let cert_rev = CertificateRevision::new(17).unwrap();
    let tls_host = TlsHostname::new("tls.example").unwrap();
    check!(
        1,
        publish_certificate,
        PublishCertificateRequest {
            id: cert_id.clone(),
            expected_version: cert_rev,
            bundle: CertificateBundle::new(vec![vec![1, 2]], vec![3, 4]).unwrap()
        }
    );
    check!(2, get_certificate_metadata, cert_id.clone());
    check!(
        1,
        set_tls_binding,
        SetTlsBindingRequest {
            hostname: tls_host.clone(),
            expected_revision: cert_rev,
            certificate_id: Some(cert_id.clone())
        }
    );
    check!(2, get_tls_binding, tls_host.clone());
    check!(
        1,
        remove_certificate,
        RemoveCertificateRequest {
            id: cert_id.clone(),
            expected_version: cert_rev
        }
    );
    check!(
        2,
        resolve_tls_certificate,
        ResolveTlsCertificateRequest {
            hostname: tls_host,
            known_view_revision: Some(cert_rev)
        }
    );
    check!(
        1,
        reencrypt_certificate,
        ReencryptCertificateRequest {
            id: cert_id,
            expected_version: cert_rev,
            expected_sealing_revision: CertificateRevision::new(31).unwrap()
        }
    );
    let hosts = vec![TlsHostname::new("snapshot.example").unwrap()];
    for known in [None, Some(cert_rev)] {
        assert!(store
            .snapshot_tls_bindings(hosts.clone(), known)
            .await
            .is_err());
        probe.check("snapshot_tls_bindings", format!("{hosts:?}, {known:?}"), 2);
    }
    assert!(store.load_route_changes(73, 19).await.is_err());
    probe.check("load_route_changes", "73, 19".into(), 2);
    assert!(store.load_route_change_revision().await.is_err());
    probe.check("load_route_change_revision", "()".into(), 2);
    check!(2, load_materialization_work_status, materialization.clone());
    check!(
        2,
        record_materialization_failure,
        control_plane::runtime_work::RecordMaterializationFailure {
            expected_state: control_plane::MaterializationState::Pending,
            materialization_id: materialization.clone(),
            owner: "failure-owner".into(),
            attempt: 11,
            generation,
            permanent: true,
            message: "classified failure".into(),
        }
    );
    check!(1, enqueue_materialization, materialization.clone());
    check!(2, maintain_runtime_records, 13_u32);
    check!(
        2,
        accept_wake,
        AcceptWakeRequest {
            expected_generation: generation,
            pending: record.clone()
        }
    );
    check!(
        2,
        request_instance_deletion,
        control_plane::instance::RequestInstanceDeletion {
            instance_id: instance.clone(),
            expected_generation: generation
        }
    );
    check!(2, finalize_instance_deletions, 7_usize);
    check!(
        1,
        create_instance,
        CreateInstanceRequest::new(
            IdempotencyKey::new("instance-key").unwrap(),
            instance.clone(),
            class.reference.clone()
        )
        .with_values(std::collections::BTreeMap::from([(
            "sentinel".into(),
            "value".into()
        )]))
    );
    check!(2, get_instance, GetInstanceRequest::new(instance.clone()));
    check!(
        1,
        delete_instance,
        DeleteInstanceRequest::new(instance.clone())
    );
    check!(
        2,
        create_workload_class_version,
        CreateWorkloadClassVersionRequest::new(class.clone())
    );
    check!(
        2,
        load_workload_class_version,
        LoadWorkloadClassVersionRequest::new(class.reference.clone())
    );
    check!(
        1,
        create_route_binding,
        CreateRouteBindingRequest::new(
            IdempotencyKey::new("route-key").unwrap(),
            route.clone(),
            instance.clone(),
            identity.clone(),
            ProtocolRoute::Http
        )
    );
    check!(
        2,
        get_route_binding,
        GetRouteBindingRequest::new(route.clone())
    );
    check!(
        1,
        delete_route_binding,
        DeleteRouteBindingRequest::new(route.clone())
    );
    check!(
        2,
        list_route_bindings_for_instance,
        ListRouteBindingsForInstanceRequest::new(instance.clone())
    );
    check!(
        2,
        resolve_route,
        ResolveRouteRequest::new(identity, target.clone())
    );
    check!(
        2,
        compare_and_swap_instance_state,
        CompareAndSwapInstanceStateRequest::new(
            instance.clone(),
            generation,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested
        )
    );
    check!(1, record_materialization, record);
    check!(
        2,
        load_ready_materialization,
        LoadReadyMaterializationRequest::new(instance.clone(), generation, target.clone())
    );
    check!(
        2,
        load_active_materialization,
        LoadActiveMaterializationRequest::new(instance.clone(), target.clone())
    );
    check!(
        2,
        load_materialization,
        LoadMaterializationRequest::new(materialization.clone())
    );
    check!(2, complete_wake, complete.clone());
    check!(
        2,
        begin_sleep,
        BeginSleepRequest::new(instance.clone(), generation, target.clone())
            .with_drain_grace_timeout(Duration::from_millis(321))
            .with_minimum_ready_age(Duration::from_secs(190))
    );
    check!(2, finalize_sleep, finalize.clone());
    check!(
        2,
        list_materialization_reconciliation_candidates,
        ListMaterializationReconciliationCandidatesRequest::new(23).for_target(target.clone())
    );
    assert!(store
        .load_materialization_operational_metrics()
        .await
        .is_err());
    probe.check("load_materialization_operational_metrics", "()".into(), 2);
    check!(
        1,
        claim_materialization_reconciliation,
        ClaimMaterializationReconciliationRequest::new(
            materialization.clone(),
            "claim-owner",
            Duration::from_secs(41)
        )
    );
    check!(
        1,
        begin_materialization_effect,
        MaterializationEffectRequest {
            effect_id: 41,
            materialization_id: materialization.clone(),
            owner: "effect-owner".into(),
            attempt: 43,
            instance_generation: generation,
            expected_state: MaterializationState::Pending,
            operation: "apply",
            object: RenderedObjectRef {
                api_version: "v1".into(),
                kind: "Service".into(),
                namespace: "apps".into(),
                name: "capability".into()
            },
            precondition: Some(control_plane::projection::LiveObjectIdentity {
                uid: "uid-47".into(),
                resource_version: "rv-53".into()
            }),
        }
    );
    check!(
        2,
        acknowledge_materialization_effect,
        AcknowledgeMaterializationEffectRequest {
            effect_id: 59,
            materialization_id: materialization.clone(),
            owner: "ack-owner".into(),
            attempt: 61,
            instance_generation: generation
        }
    );
    check!(
        2,
        renew_materialization_reconciliation_lease,
        RenewMaterializationReconciliationLeaseRequest::new(
            materialization.clone(),
            "renew-owner",
            67,
            generation,
            Duration::from_secs(71),
            MaterializationState::Pending
        )
    );
    check!(
        2,
        release_materialization_reconciliation_lease,
        ReleaseMaterializationReconciliationLeaseRequest::new(
            materialization.clone(),
            "release-owner",
            73,
            generation
        )
    );
    check!(
        2,
        complete_wake_reconciliation,
        CompleteWakeReconciliationRequest::new(
            materialization.clone(),
            "complete-owner",
            79,
            complete
        )
    );
    check!(
        2,
        finalize_sleep_reconciliation,
        FinalizeSleepReconciliationRequest::new(
            materialization.clone(),
            "finalize-owner",
            83,
            finalize
        )
    );
    check!(
        2,
        delete_materialization_reconciliation,
        DeleteMaterializationReconciliationRequest::new(
            materialization.clone(),
            "delete-owner",
            89,
            MaterializationState::Deleting,
            instance,
            generation,
            target.clone()
        )
    );
    check!(
        1,
        force_delete_materialization,
        ForceDeleteMaterializationRequest::new(materialization, "operator", "fenced old process")
    );
    check!(
        1,
        force_release_exclusivity_key,
        ForceReleaseExclusivityKeyRequest::new(
            target,
            "storage",
            "disk",
            "operator",
            "fenced old process"
        )
    );
    check!(
        2,
        lookup_route_dependencies,
        RouteDependencyLookup::new(route)
    );
    check!(
        1,
        put_http01_challenge,
        PutHttp01ChallengeRequest::new(
            key.clone(),
            "authorization",
            now + Duration::from_secs(97),
            now
        )
        .unwrap()
    );
    check!(2, resolve_http01_challenge, key.clone());
    check!(
        1,
        delete_http01_challenge,
        DeleteHttp01ChallengeRequest::new(key)
    );
    check!(
        2,
        expire_http01_challenges,
        ExpireHttp01ChallengesRequest::new(now).with_limit(101)
    );
}

#[tokio::test]
async fn shared_transport_fixture_satisfies_the_postgres_backed_lifecycle_contract() {
    support::lifecycle_conformance(&support::TestStore::default())
        .await
        .unwrap();
}
