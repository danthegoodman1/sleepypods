//! Response loss is injected only after an actual PostgreSQL operation commits.
//! A second real writer replaces the row before the wrapper observes Unavailable.
use super::*;
use control_plane::{
    materialization::*, CreateInstanceResult, DeleteHttp01ChallengeRequest, Http01ChallengeRecord,
    RetryPolicy, RetryingControlPlaneStore, RouteBindingRecord, StoreFuture, StoreResult,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

type AfterCommit = Arc<dyn Fn() -> StoreFuture<'static, StoreResult<()>> + Send + Sync>;
struct LostResponseStore {
    inner: PostgresStore,
    after_commit: AfterCommit,
    calls: AtomicUsize,
}
impl LostResponseStore {
    async fn lost<T>(&self, value: T) -> StoreResult<T> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            (self.after_commit)().await?;
            Err(StoreError::unavailable(
                "response lost after committed mutation",
            ))
        } else {
            Ok(value)
        }
    }
}
impl ControlPlaneStore for LostResponseStore {
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
        maintain_runtime_records,
        accept_wake,
        request_instance_deletion,
        finalize_instance_deletions,
        get_instance,
        create_workload_class_version,
        load_workload_class_version,
        get_route_binding,
        list_route_bindings_for_instance,
        resolve_route,
        compare_and_swap_instance_state,
        load_ready_materialization,
        load_active_materialization,
        load_materialization,
        complete_wake,
        begin_sleep,
        finalize_sleep,
        list_materialization_reconciliation_candidates,
        load_materialization_operational_metrics,
        begin_materialization_effect,
        renew_materialization_reconciliation_lease,
        release_materialization_reconciliation_lease,
        complete_wake_reconciliation,
        finalize_sleep_reconciliation,
        delete_materialization_reconciliation,
        lookup_route_dependencies,
        resolve_http01_challenge,
        expire_http01_challenges
    );
    fn record_materialization_failure(
        &self,
        request: control_plane::runtime_work::RecordMaterializationFailure,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        Box::pin(async move {
            let result = self.inner.record_materialization_failure(request).await?;
            self.lost(result).await
        })
    }
    fn enqueue_materialization(
        &self,
        id: control_plane::ids::MaterializationId,
    ) -> StoreFuture<'_, StoreResult<bool>> {
        Box::pin(async move {
            let result = self.inner.enqueue_materialization(id).await?;
            self.lost(result).await
        })
    }
    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        Box::pin(async move {
            let result = self.inner.create_instance(request).await?;
            self.lost(result).await
        })
    }
    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let result = self.inner.delete_instance(request).await?;
            self.lost(result).await
        })
    }
    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
        Box::pin(async move {
            let result = self.inner.create_route_binding(request).await?;
            self.lost(result).await
        })
    }
    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let result = self.inner.delete_route_binding(request).await?;
            self.lost(result).await
        })
    }
    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        Box::pin(async move {
            let result = self.inner.record_materialization(request).await?;
            self.lost(result).await
        })
    }
    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let result = self
                .inner
                .claim_materialization_reconciliation(request)
                .await?;
            self.lost(result).await
        })
    }
    fn acknowledge_materialization_effect<'a>(
        &'a self,
        request: control_plane::materialization::AcknowledgeMaterializationEffectRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let result = self
                .inner
                .acknowledge_materialization_effect(request)
                .await?;
            self.lost(result).await
        })
    }
    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let result = self.inner.force_delete_materialization(request).await?;
            self.lost(result).await
        })
    }
    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>> {
        Box::pin(async move {
            let result = self.inner.force_release_exclusivity_key(request).await?;
            self.lost(result).await
        })
    }
    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
        Box::pin(async move {
            let result = self.inner.put_http01_challenge(request).await?;
            self.lost(result).await
        })
    }
    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let result = self.inner.delete_http01_challenge(request).await?;
            self.lost(result).await
        })
    }
}
fn lossy(
    inner: &PostgresStore,
    after_commit: impl Fn() -> StoreFuture<'static, StoreResult<()>> + Send + Sync + 'static,
) -> (RetryingControlPlaneStore, Arc<LostResponseStore>) {
    let fault = Arc::new(LostResponseStore {
        inner: inner.clone(),
        after_commit: Arc::new(after_commit),
        calls: AtomicUsize::new(0),
    });
    (
        RetryingControlPlaneStore::new(
            fault.clone(),
            RetryPolicy::new(2, Duration::ZERO, Duration::ZERO),
        ),
        fault,
    )
}
fn assert_one_shot<T: std::fmt::Debug>(result: StoreResult<T>, fault: &LostResponseStore) {
    assert!(
        matches!(result, Err(StoreError::Unavailable { .. })),
        "uncertain outcome reaches caller: {result:?}"
    );
    assert_eq!(
        fault.calls.load(Ordering::SeqCst),
        1,
        "replacement cannot be touched by automatic replay"
    );
}

#[tokio::test]
async fn postgres_retry_boundaries_preserve_real_replacements() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!(
            "skipping Postgres retry boundary conformance; SLEEPYPODS_POSTGRES_URL is required"
        );
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let mut config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
    config.idempotency_retention = Some(Duration::from_millis(1));
    let pg = PostgresStore::connect(&config).await?;
    let result = replacement_checks(&pg).await;
    drop(pg);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    task.abort();
    result
}

async fn replacement_checks(pg: &PostgresStore) -> TestResult {
    super::transport_support::lifecycle_conformance(pg).await?;
    let class = workload_class("retry-boundary-class", 1);
    pg.create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let owner = pg
        .create_instance(create_instance_request(
            "retry-owner",
            "retry-owner",
            class.reference.clone(),
            vec![],
        ))
        .await?
        .instance;
    let target = MaterializationTarget::new("retry-cluster", "apps")?;

    // Force operations are ID/key scoped. A second writer installs a new projection
    // or key owner after commit, before the old caller observes response loss.
    for operation in ["force-delete", "force-release", "record"] {
        let instance = pg
            .create_instance(create_instance_request(
                operation,
                operation,
                class.reference.clone(),
                vec![],
            ))
            .await?
            .instance;
        let mut request = RecordMaterializationRequest::new(
            instance.id.clone(),
            instance.generation,
            target.clone(),
            MaterializationState::Pending,
            BackendGeneration::new(1),
        );
        request.exclusivity_keys = vec![RenderedExclusivityKey {
            name: "storage".into(),
            value: operation.into(),
        }];
        let initial = pg.record_materialization(request.clone()).await?;
        let mut replacement = request.clone();
        replacement.backend_generation = BackendGeneration::new(2);
        replacement.rendered_objects = vec![RenderedObjectRef {
            api_version: "v1".into(),
            kind: "Service".into(),
            namespace: "apps".into(),
            name: format!("{operation}-replacement"),
        }];
        let next = pg.clone();
        let replacement_id = initial.id.clone();
        let (store, fault) = lossy(pg, move || {
            let db = next.clone();
            let request = replacement.clone();
            let id = replacement_id.clone();
            Box::pin(async move {
                db.record_materialization(request).await?;
                db.claim_materialization_reconciliation(
                    ClaimMaterializationReconciliationRequest::new(
                        id,
                        "replacement-owner",
                        Duration::from_secs(60),
                    ),
                )
                .await?;
                Ok(())
            })
        });
        match operation {
            "force-delete" => assert_one_shot(
                store
                    .force_delete_materialization(ForceDeleteMaterializationRequest::new(
                        initial.id.clone(),
                        "operator",
                        "old process fenced",
                    ))
                    .await,
                &fault,
            ),
            "force-release" => assert_one_shot(
                store
                    .force_release_exclusivity_key(ForceReleaseExclusivityKeyRequest::new(
                        target.clone(),
                        "storage",
                        operation,
                        "operator",
                        "old process fenced",
                    ))
                    .await,
                &fault,
            ),
            "record" => {
                // Same backend generation makes replay able to overwrite state;
                // a monotonic backend counter alone is not an exact write fence.
                request.backend_generation = BackendGeneration::new(2);
                assert_one_shot(store.record_materialization(request).await, &fault);
            }
            _ => unreachable!(),
        }
        let current = pg
            .load_materialization(LoadMaterializationRequest::new(initial.id))
            .await?
            .unwrap();
        assert_eq!(current.state, MaterializationState::Pending);
        assert_eq!(current.backend_generation, BackendGeneration::new(2));
        assert_eq!(
            current.rendered_objects[0].name,
            format!("{operation}-replacement")
        );
        assert_eq!(
            current.exclusivity_keys.len(),
            1,
            "replacement ownership survives response loss"
        );
        assert_eq!(
            current.reconciliation_lease.unwrap().owner,
            "replacement-owner"
        );
    }

    let original_route = create_route_binding_request(
        "delete-route-old",
        "delete-route",
        owner.id.as_str(),
        http_identity("old.retry.example", None),
        ProtocolRoute::Http,
    );
    pg.create_route_binding(original_route.clone()).await?;
    let replacement_route = create_route_binding_request(
        "delete-route-new",
        "delete-route",
        owner.id.as_str(),
        http_identity("new.retry.example", None),
        ProtocolRoute::Http,
    );
    let next = pg.clone();
    let replacement = replacement_route.clone();
    let (store, fault) = lossy(pg, move || {
        let db = next.clone();
        let request = replacement.clone();
        Box::pin(async move {
            db.create_route_binding(request).await?;
            Ok(())
        })
    });
    assert_one_shot(
        store
            .delete_route_binding(DeleteRouteBindingRequest::new(
                original_route.route_binding_id,
            ))
            .await,
        &fault,
    );
    assert_eq!(
        pg.get_route_binding(GetRouteBindingRequest::new(
            replacement_route.route_binding_id
        ))
        .await?
        .unwrap()
        .identity,
        replacement_route.identity
    );

    for operation in ["put", "delete"] {
        let key = Http01ChallengeKey::new("retry.example", operation)?;
        let now = SystemTime::now();
        let original = PutHttp01ChallengeRequest::new(
            key.clone(),
            "old-authorization",
            now + Duration::from_secs(300),
            now,
        )?;
        pg.put_http01_challenge(original.clone()).await?;
        let replacement = PutHttp01ChallengeRequest::new(
            key.clone(),
            "replacement-authorization",
            now + Duration::from_secs(600),
            now,
        )?;
        let next = pg.clone();
        let (store, fault) = lossy(pg, move || {
            let db = next.clone();
            let request = replacement.clone();
            Box::pin(async move {
                db.put_http01_challenge(request).await?;
                Ok(())
            })
        });
        if operation == "put" {
            assert_one_shot(store.put_http01_challenge(original).await, &fault);
        } else {
            assert_one_shot(
                store
                    .delete_http01_challenge(control_plane::DeleteHttp01ChallengeRequest::new(
                        key.clone(),
                    ))
                    .await,
                &fault,
            );
        }
        assert_eq!(
            pg.resolve_http01_challenge(key)
                .await?
                .unwrap()
                .key_authorization(),
            "replacement-authorization"
        );
    }

    // A legacy ID-only delete must not delete a fresh incarnation, even if that
    // new incarnation has independently entered Deleting as well.
    let old = pg
        .create_instance(create_instance_request(
            "delete-old",
            "delete-reused",
            class.reference.clone(),
            vec![],
        ))
        .await?
        .instance;
    pg.request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
        instance_id: old.id.clone(),
        expected_generation: old.generation,
    })
    .await?;
    let replacement = create_instance_request(
        "delete-new",
        "delete-reused",
        class.reference.clone(),
        vec![],
    );
    let next = pg.clone();
    let (store, fault) = lossy(pg, move || {
        let db = next.clone();
        let request = replacement.clone();
        Box::pin(async move {
            let created = db.create_instance(request).await?.instance;
            db.request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                instance_id: created.id,
                expected_generation: created.generation,
            })
            .await?;
            Ok(())
        })
    });
    assert_one_shot(
        store
            .delete_instance(DeleteInstanceRequest::new(old.id.clone()))
            .await,
        &fault,
    );
    let current = pg
        .get_instance(GetInstanceRequest::new(old.id))
        .await?
        .unwrap();
    assert!(current.generation > old.generation);
    assert_eq!(current.state, InstanceState::Deleting);

    // The configured one-millisecond idempotency retention is intentionally
    // shorter than the response-loss window. Automatic create replay must not
    // recreate a deleted resource once its original replay record has expired.
    let create = create_instance_request(
        "create-lost",
        "create-lost",
        class.reference.clone(),
        vec![],
    );
    let next = pg.clone();
    let id = create.instance_id.clone();
    let (store, fault) = lossy(pg, move || {
        let db = next.clone();
        let id = id.clone();
        Box::pin(async move {
            let current = db
                .get_instance(GetInstanceRequest::new(id.clone()))
                .await?
                .unwrap();
            db.request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                instance_id: id.clone(),
                expected_generation: current.generation,
            })
            .await?;
            db.delete_instance(DeleteInstanceRequest::new(id)).await?;
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(())
        })
    });
    assert_one_shot(store.create_instance(create.clone()).await, &fault);
    assert!(pg
        .get_instance(GetInstanceRequest::new(create.instance_id.clone()))
        .await?
        .is_none());
    let deliberately_recreated = pg.create_instance(create).await?.instance;
    assert!(
        deliberately_recreated.generation > Generation::new(0),
        "expired replay really permits a new incarnation"
    );

    let create = create_route_binding_request(
        "create-route-lost",
        "create-route-lost",
        owner.id.as_str(),
        http_identity("create.retry.example", None),
        ProtocolRoute::Http,
    );
    let next = pg.clone();
    let id = create.route_binding_id.clone();
    let (store, fault) = lossy(pg, move || {
        let db = next.clone();
        let id = id.clone();
        Box::pin(async move {
            db.delete_route_binding(DeleteRouteBindingRequest::new(id))
                .await?;
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(())
        })
    });
    assert_one_shot(store.create_route_binding(create.clone()).await, &fault);
    assert!(pg
        .get_route_binding(GetRouteBindingRequest::new(create.route_binding_id.clone()))
        .await?
        .is_none());
    assert_eq!(
        pg.create_route_binding(create.clone()).await?.id,
        create.route_binding_id,
        "expired replay really permits recreation"
    );
    exact_fence_retries(pg, &class, &target).await?;
    Ok(())
}

async fn exact_fence_retries(
    pg: &PostgresStore,
    class: &WorkloadClassVersion,
    target: &MaterializationTarget,
) -> TestResult {
    let instance = pg
        .create_instance(create_instance_request(
            "retry-fenced",
            "retry-fenced",
            class.reference.clone(),
            vec![],
        ))
        .await?
        .instance;
    let pending = pg
        .record_materialization(RecordMaterializationRequest::new(
            instance.id,
            instance.generation,
            target.clone(),
            MaterializationState::Pending,
            BackendGeneration::new(1),
        ))
        .await?;
    let claimed = pg
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending.id.clone(),
            "fenced-owner",
            Duration::from_secs(60),
        ))
        .await?
        .unwrap();
    let attempt = claimed.reconciliation_lease.as_ref().unwrap().attempt;
    let effect = MaterializationEffectRequest {
        effect_id: 1,
        materialization_id: pending.id.clone(),
        owner: "fenced-owner".into(),
        attempt,
        instance_generation: pending.instance_generation,
        expected_state: MaterializationState::Pending,
        operation: "apply",
        object: RenderedObjectRef {
            api_version: "v1".into(),
            kind: "Service".into(),
            namespace: "apps".into(),
            name: "retry-fenced".into(),
        },
        precondition: None,
    };
    assert!(pg.begin_materialization_effect(effect.clone()).await?);
    let ack = AcknowledgeMaterializationEffectRequest {
        instance_generation: pending.instance_generation,
        effect_id: 1,
        materialization_id: pending.id.clone(),
        owner: "fenced-owner".into(),
        attempt,
    };
    let next = pg.clone();
    let newer_effect = MaterializationEffectRequest {
        effect_id: 2,
        ..effect
    };
    let (store, fault) = lossy(pg, move || {
        let db = next.clone();
        let effect = newer_effect.clone();
        Box::pin(async move {
            assert!(db.begin_materialization_effect(effect).await?);
            Ok(())
        })
    });
    assert!(
        !store.acknowledge_materialization_effect(ack).await?,
        "exact old ACK retry is a no-op"
    );
    assert_eq!(fault.calls.load(Ordering::SeqCst), 2);
    let status = pg
        .load_materialization_work_status(pending.id.clone())
        .await?
        .unwrap();
    assert_eq!(
        status.uncertain_effect.unwrap().effect_id,
        2,
        "old retry cannot erase later effect"
    );
    assert!(
        pg.acknowledge_materialization_effect(AcknowledgeMaterializationEffectRequest {
            instance_generation: pending.instance_generation,
            effect_id: 2,
            materialization_id: pending.id.clone(),
            owner: "fenced-owner".into(),
            attempt
        })
        .await?
    );
    let (store, fault) = lossy(pg, || Box::pin(async { Ok(()) }));
    assert!(
        !store
            .record_materialization_failure(
                control_plane::runtime_work::RecordMaterializationFailure {
                    expected_state: control_plane::MaterializationState::Pending,
                    materialization_id: pending.id.clone(),
                    owner: "fenced-owner".into(),
                    attempt,
                    generation: pending.instance_generation,
                    permanent: false,
                    message: "temporary failure".into()
                }
            )
            .await?,
        "replayed failure cannot clear a newer owner or increment twice"
    );
    assert_eq!(fault.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        pg.load_materialization_work_status(pending.id)
            .await?
            .unwrap()
            .failure_count,
        1
    );
    Ok(())
}
