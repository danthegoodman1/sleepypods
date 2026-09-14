//! Explicit opt-in for capabilities a narrow test fixture must never exercise.
//! This macro is included only by unit/integration tests, never production code.
//! Adding a production capability still requires updating every test fixture.

macro_rules! unexpected_store_methods {
    () => {};
    (publish_certificate $(, $rest:ident)* $(,)?) => {
        fn publish_certificate(&self, _request: control_plane::certificate::PublishCertificateRequest) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::certificate::CertificateMetadata>> { panic!("unexpected test store capability: publish_certificate") }
        unexpected_store_methods!($($rest),*);
    };
    (get_certificate_metadata $(, $rest:ident)* $(,)?) => {
        fn get_certificate_metadata(&self, _id: control_plane::certificate::CertificateId) -> control_plane::StoreFuture<'_, control_plane::StoreResult<Option<control_plane::certificate::CertificateMetadata>>> { panic!("unexpected test store capability: get_certificate_metadata") }
        unexpected_store_methods!($($rest),*);
    };
    (set_tls_binding $(, $rest:ident)* $(,)?) => {
        fn set_tls_binding(&self, _request: control_plane::certificate::SetTlsBindingRequest) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::certificate::TlsBinding>> { panic!("unexpected test store capability: set_tls_binding") }
        unexpected_store_methods!($($rest),*);
    };
    (get_tls_binding $(, $rest:ident)* $(,)?) => {
        fn get_tls_binding(&self, _hostname: control_plane::certificate::TlsHostname) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::certificate::TlsBinding>> { panic!("unexpected test store capability: get_tls_binding") }
        unexpected_store_methods!($($rest),*);
    };
    (remove_certificate $(, $rest:ident)* $(,)?) => {
        fn remove_certificate(&self, _request: control_plane::certificate::RemoveCertificateRequest) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::certificate::CertificateMetadata>> { panic!("unexpected test store capability: remove_certificate") }
        unexpected_store_methods!($($rest),*);
    };
    (resolve_tls_certificate $(, $rest:ident)* $(,)?) => {
        fn resolve_tls_certificate(&self, _request: control_plane::certificate::ResolveTlsCertificateRequest) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::certificate::TlsCertificateResolution>> { panic!("unexpected test store capability: resolve_tls_certificate") }
        unexpected_store_methods!($($rest),*);
    };
    (reencrypt_certificate $(, $rest:ident)* $(,)?) => {
        fn reencrypt_certificate(&self, _request: control_plane::certificate::ReencryptCertificateRequest) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::certificate::CertificateMetadata>> { panic!("unexpected test store capability: reencrypt_certificate") }
        unexpected_store_methods!($($rest),*);
    };

    (snapshot_tls_bindings $(, $rest:ident)* $(,)?) => {
        fn snapshot_tls_bindings(&self, _: Vec<control_plane::certificate::TlsHostname>, _: Option<control_plane::certificate::CertificateRevision>) -> control_plane::StoreFuture<'_, control_plane::StoreResult<Option<control_plane::certificate::TlsBindingSnapshot>>> { panic!("unexpected test store capability: snapshot_tls_bindings") }
        unexpected_store_methods!($($rest),*);
    };


    (load_route_changes $(, $rest:ident)* $(,)?) => {
    fn load_route_changes(
        &self,
        _cursor: u64,
        _limit: u32,
    ) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::runtime_work::DurableRouteChanges>> {
        panic!("unexpected test store capability: load_route_changes")
    }
        unexpected_store_methods!($($rest),*);
    };
    (load_route_change_revision $(, $rest:ident)* $(,)?) => {
    fn load_route_change_revision(&self) -> control_plane::StoreFuture<'_, control_plane::StoreResult<u64>> {
        panic!("unexpected test store capability: load_route_change_revision")
    }
        unexpected_store_methods!($($rest),*);
    };
    (load_materialization_work_status $(, $rest:ident)* $(,)?) => {
    fn load_materialization_work_status(
        &self,
        _id: control_plane::ids::MaterializationId,
    ) -> control_plane::StoreFuture<'_, control_plane::StoreResult<Option<control_plane::runtime_work::MaterializationWorkStatus>>> {
        panic!("unexpected test store capability: load_materialization_work_status")
    }
        unexpected_store_methods!($($rest),*);
    };
    (record_materialization_failure $(, $rest:ident)* $(,)?) => {
    fn record_materialization_failure(
        &self,
        _request: control_plane::runtime_work::RecordMaterializationFailure,
    ) -> control_plane::StoreFuture<'_, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: record_materialization_failure")
    }
        unexpected_store_methods!($($rest),*);
    };
    (enqueue_materialization $(, $rest:ident)* $(,)?) => {
    fn enqueue_materialization(
        &self,
        _id: control_plane::ids::MaterializationId,
    ) -> control_plane::StoreFuture<'_, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: enqueue_materialization")
    }
        unexpected_store_methods!($($rest),*);
    };
    (maintain_runtime_records $(, $rest:ident)* $(,)?) => {
    fn maintain_runtime_records(&self, _limit: u32) -> control_plane::StoreFuture<'_, control_plane::StoreResult<u64>> {
        panic!("unexpected test store capability: maintain_runtime_records")
    }
        unexpected_store_methods!($($rest),*);
    };
    (accept_wake $(, $rest:ident)* $(,)?) => {
    fn accept_wake<'a>(
        &'a self,
        _request: control_plane::materialization::AcceptWakeRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::InstanceRecord>> {
        panic!("unexpected test store capability: accept_wake")
    }
        unexpected_store_methods!($($rest),*);
    };
    (request_instance_deletion $(, $rest:ident)* $(,)?) => {
    fn request_instance_deletion<'a>(
        &'a self,
        _request: control_plane::instance::RequestInstanceDeletion,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: request_instance_deletion")
    }
        unexpected_store_methods!($($rest),*);
    };
    (finalize_instance_deletions $(, $rest:ident)* $(,)?) => {
    fn finalize_instance_deletions<'a>(
        &'a self,
        _limit: usize,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<usize>> {
        panic!("unexpected test store capability: finalize_instance_deletions")
    }
        unexpected_store_methods!($($rest),*);
    };
    (create_instance $(, $rest:ident)* $(,)?) => {
    fn create_instance<'a>(
        &'a self,
        _request: control_plane::CreateInstanceRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::CreateInstanceResult>> {
        panic!("unexpected test store capability: create_instance")
    }
        unexpected_store_methods!($($rest),*);
    };
    (get_instance $(, $rest:ident)* $(,)?) => {
    fn get_instance<'a>(
        &'a self,
        _request: control_plane::GetInstanceRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::InstanceRecord>>> {
        panic!("unexpected test store capability: get_instance")
    }
        unexpected_store_methods!($($rest),*);
    };
    (delete_instance $(, $rest:ident)* $(,)?) => {
    fn delete_instance<'a>(
        &'a self,
        _request: control_plane::DeleteInstanceRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: delete_instance")
    }
        unexpected_store_methods!($($rest),*);
    };
    (create_workload_class_version $(, $rest:ident)* $(,)?) => {
    fn create_workload_class_version<'a>(
        &'a self,
        _request: control_plane::CreateWorkloadClassVersionRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::WorkloadClassVersion>> {
        panic!("unexpected test store capability: create_workload_class_version")
    }
        unexpected_store_methods!($($rest),*);
    };
    (load_workload_class_version $(, $rest:ident)* $(,)?) => {
    fn load_workload_class_version<'a>(
        &'a self,
        _request: control_plane::LoadWorkloadClassVersionRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::WorkloadClassVersion>>> {
        panic!("unexpected test store capability: load_workload_class_version")
    }
        unexpected_store_methods!($($rest),*);
    };
    (create_route_binding $(, $rest:ident)* $(,)?) => {
    fn create_route_binding<'a>(
        &'a self,
        _request: control_plane::CreateRouteBindingRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::RouteBindingRecord>> {
        panic!("unexpected test store capability: create_route_binding")
    }
        unexpected_store_methods!($($rest),*);
    };
    (get_route_binding $(, $rest:ident)* $(,)?) => {
    fn get_route_binding<'a>(
        &'a self,
        _request: control_plane::GetRouteBindingRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::RouteBindingRecord>>> {
        panic!("unexpected test store capability: get_route_binding")
    }
        unexpected_store_methods!($($rest),*);
    };
    (delete_route_binding $(, $rest:ident)* $(,)?) => {
    fn delete_route_binding<'a>(
        &'a self,
        _request: control_plane::DeleteRouteBindingRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: delete_route_binding")
    }
        unexpected_store_methods!($($rest),*);
    };
    (list_route_bindings_for_instance $(, $rest:ident)* $(,)?) => {
    fn list_route_bindings_for_instance<'a>(
        &'a self,
        _request: control_plane::ListRouteBindingsForInstanceRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Vec<control_plane::RouteBindingRecord>>> {
        panic!("unexpected test store capability: list_route_bindings_for_instance")
    }
        unexpected_store_methods!($($rest),*);
    };
    (resolve_route $(, $rest:ident)* $(,)?) => {
    fn resolve_route<'a>(
        &'a self,
        _request: control_plane::ResolveRouteRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::RouteResolution>> {
        panic!("unexpected test store capability: resolve_route")
    }
        unexpected_store_methods!($($rest),*);
    };
    (compare_and_swap_instance_state $(, $rest:ident)* $(,)?) => {
    fn compare_and_swap_instance_state<'a>(
        &'a self,
        _request: control_plane::CompareAndSwapInstanceStateRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::InstanceRecord>> {
        panic!("unexpected test store capability: compare_and_swap_instance_state")
    }
        unexpected_store_methods!($($rest),*);
    };
    (record_materialization $(, $rest:ident)* $(,)?) => {
    fn record_materialization<'a>(
        &'a self,
        _request: control_plane::RecordMaterializationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::MaterializationRecord>> {
        panic!("unexpected test store capability: record_materialization")
    }
        unexpected_store_methods!($($rest),*);
    };
    (load_ready_materialization $(, $rest:ident)* $(,)?) => {
    fn load_ready_materialization<'a>(
        &'a self,
        _request: control_plane::materialization::LoadReadyMaterializationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::MaterializationRecord>>> {
        panic!("unexpected test store capability: load_ready_materialization")
    }
        unexpected_store_methods!($($rest),*);
    };
    (load_active_materialization $(, $rest:ident)* $(,)?) => {
    fn load_active_materialization<'a>(
        &'a self,
        _request: control_plane::LoadActiveMaterializationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::MaterializationRecord>>> {
        panic!("unexpected test store capability: load_active_materialization")
    }
        unexpected_store_methods!($($rest),*);
    };
    (load_materialization $(, $rest:ident)* $(,)?) => {
    fn load_materialization<'a>(
        &'a self,
        _request: control_plane::LoadMaterializationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::MaterializationRecord>>> {
        panic!("unexpected test store capability: load_materialization")
    }
        unexpected_store_methods!($($rest),*);
    };
    (complete_wake $(, $rest:ident)* $(,)?) => {
    fn complete_wake<'a>(
        &'a self,
        _request: control_plane::CompleteWakeRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::CompleteWakeResult>> {
        panic!("unexpected test store capability: complete_wake")
    }
        unexpected_store_methods!($($rest),*);
    };
    (begin_sleep $(, $rest:ident)* $(,)?) => {
    fn begin_sleep<'a>(
        &'a self,
        _request: control_plane::BeginSleepRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::BeginSleepResult>> {
        panic!("unexpected test store capability: begin_sleep")
    }
        unexpected_store_methods!($($rest),*);
    };
    (finalize_sleep $(, $rest:ident)* $(,)?) => {
    fn finalize_sleep<'a>(
        &'a self,
        _request: control_plane::FinalizeSleepRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::FinalizeSleepResult>> {
        panic!("unexpected test store capability: finalize_sleep")
    }
        unexpected_store_methods!($($rest),*);
    };
    (list_materialization_reconciliation_candidates $(, $rest:ident)* $(,)?) => {
    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        _request: control_plane::ListMaterializationReconciliationCandidatesRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Vec<control_plane::MaterializationRecord>>> {
        panic!("unexpected test store capability: list_materialization_reconciliation_candidates")
    }
        unexpected_store_methods!($($rest),*);
    };
    (load_materialization_operational_metrics $(, $rest:ident)* $(,)?) => {
    fn load_materialization_operational_metrics(
        &self,
    ) -> control_plane::StoreFuture<'_, control_plane::StoreResult<control_plane::materialization::MaterializationOperationalMetrics>> {
        panic!("unexpected test store capability: load_materialization_operational_metrics")
    }
        unexpected_store_methods!($($rest),*);
    };
    (claim_materialization_reconciliation $(, $rest:ident)* $(,)?) => {
    fn claim_materialization_reconciliation<'a>(
        &'a self,
        _request: control_plane::ClaimMaterializationReconciliationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::MaterializationRecord>>> {
        panic!("unexpected test store capability: claim_materialization_reconciliation")
    }
        unexpected_store_methods!($($rest),*);
    };
    (begin_materialization_effect $(, $rest:ident)* $(,)?) => {
    fn begin_materialization_effect<'a>(
        &'a self,
        _request: control_plane::materialization::MaterializationEffectRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: begin_materialization_effect")
    }
        unexpected_store_methods!($($rest),*);
    };
    (acknowledge_materialization_effect $(, $rest:ident)* $(,)?) => {
    fn acknowledge_materialization_effect<'a>(
        &'a self,
        _request: control_plane::materialization::AcknowledgeMaterializationEffectRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: acknowledge_materialization_effect")
    }
        unexpected_store_methods!($($rest),*);
    };
    (renew_materialization_reconciliation_lease $(, $rest:ident)* $(,)?) => {
    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        _request: control_plane::RenewMaterializationReconciliationLeaseRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: renew_materialization_reconciliation_lease")
    }
        unexpected_store_methods!($($rest),*);
    };
    (release_materialization_reconciliation_lease $(, $rest:ident)* $(,)?) => {
    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        _request: control_plane::ReleaseMaterializationReconciliationLeaseRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: release_materialization_reconciliation_lease")
    }
        unexpected_store_methods!($($rest),*);
    };
    (complete_wake_reconciliation $(, $rest:ident)* $(,)?) => {
    fn complete_wake_reconciliation<'a>(
        &'a self,
        _request: control_plane::CompleteWakeReconciliationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::CompleteWakeResult>> {
        panic!("unexpected test store capability: complete_wake_reconciliation")
    }
        unexpected_store_methods!($($rest),*);
    };
    (finalize_sleep_reconciliation $(, $rest:ident)* $(,)?) => {
    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        _request: control_plane::FinalizeSleepReconciliationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::FinalizeSleepResult>> {
        panic!("unexpected test store capability: finalize_sleep_reconciliation")
    }
        unexpected_store_methods!($($rest),*);
    };
    (delete_materialization_reconciliation $(, $rest:ident)* $(,)?) => {
    fn delete_materialization_reconciliation<'a>(
        &'a self,
        _request: control_plane::DeleteMaterializationReconciliationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::MaterializationRecord>>> {
        panic!("unexpected test store capability: delete_materialization_reconciliation")
    }
        unexpected_store_methods!($($rest),*);
    };
    (force_delete_materialization $(, $rest:ident)* $(,)?) => {
    fn force_delete_materialization<'a>(
        &'a self,
        _request: control_plane::ForceDeleteMaterializationRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::MaterializationRecord>>> {
        panic!("unexpected test store capability: force_delete_materialization")
    }
        unexpected_store_methods!($($rest),*);
    };
    (force_release_exclusivity_key $(, $rest:ident)* $(,)?) => {
    fn force_release_exclusivity_key<'a>(
        &'a self,
        _request: control_plane::ForceReleaseExclusivityKeyRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::ForceReleaseExclusivityKeyResult>> {
        panic!("unexpected test store capability: force_release_exclusivity_key")
    }
        unexpected_store_methods!($($rest),*);
    };
    (lookup_route_dependencies $(, $rest:ident)* $(,)?) => {
    fn lookup_route_dependencies<'a>(
        &'a self,
        _request: control_plane::RouteDependencyLookup,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::RouteDependencySet>>> {
        panic!("unexpected test store capability: lookup_route_dependencies")
    }
        unexpected_store_methods!($($rest),*);
    };
    (put_http01_challenge $(, $rest:ident)* $(,)?) => {
    fn put_http01_challenge<'a>(
        &'a self,
        _request: control_plane::PutHttp01ChallengeRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<control_plane::Http01ChallengeRecord>> {
        panic!("unexpected test store capability: put_http01_challenge")
    }
        unexpected_store_methods!($($rest),*);
    };
    (resolve_http01_challenge $(, $rest:ident)* $(,)?) => {
    fn resolve_http01_challenge<'a>(
        &'a self,
        _key: control_plane::Http01ChallengeKey,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<Option<control_plane::Http01ChallengeRecord>>> {
        panic!("unexpected test store capability: resolve_http01_challenge")
    }
        unexpected_store_methods!($($rest),*);
    };
    (delete_http01_challenge $(, $rest:ident)* $(,)?) => {
    fn delete_http01_challenge<'a>(
        &'a self,
        _request: control_plane::DeleteHttp01ChallengeRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<bool>> {
        panic!("unexpected test store capability: delete_http01_challenge")
    }
        unexpected_store_methods!($($rest),*);
    };
    (expire_http01_challenges $(, $rest:ident)* $(,)?) => {
    fn expire_http01_challenges<'a>(
        &'a self,
        _request: control_plane::ExpireHttp01ChallengesRequest,
    ) -> control_plane::StoreFuture<'a, control_plane::StoreResult<usize>> {
        panic!("unexpected test store capability: expire_http01_challenges")
    }
        unexpected_store_methods!($($rest),*);
    };
}
