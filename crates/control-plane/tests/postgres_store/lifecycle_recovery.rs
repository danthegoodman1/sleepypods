//! Store contracts the lifecycle driver recovers on: per-state claims, cleanup
//! that retries until it succeeds, force delete that leaves a wakeable
//! instance, revive during drain grace, and one lock order.
use super::*;
use control_plane::{
    api::{pb, StoreBackedProxyApi, StoreBackedSidecarApi},
    instance::RequestInstanceDeletion,
    runtime_work::RecordMaterializationFailure,
    CompleteWakeResult, ForceDeleteMaterializationRequest, InstanceRecord, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient, KubernetesObject,
    MaterializationRecord,
};
use pb::{
    proxy_control_plane_server::ProxyControlPlane,
    sidecar_control_plane_server::SidecarControlPlane,
};
use std::{future::Future, sync::Arc};

pub(super) const LEASE: Duration = Duration::from_secs(60);
const SESSIONS_PER_STORE: usize = 4;
/// The ready Pod's own address, as a completed wake records it.
const POD_BACKEND: &str = "http://10.244.0.7:15000";

/// Runs `body` against a store and a raw session in a schema of their own.
pub(super) async fn with_store<Body, BodyFuture>(body: Body) -> TestResult
where
    Body: FnOnce(PostgresStore, tokio_postgres::Client) -> BodyFuture,
    BodyFuture: Future<Output = TestResult>,
{
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping lifecycle recovery contracts; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result = async {
        // The store's sessions carry the schema as their application name, so
        // a test can find them in pg_stat_activity.
        let url = connection_url_with_search_path(&base, &schema);
        let separator = if url.contains('?') { '&' } else { '?' };
        let mut config =
            PostgresStoreConfig::new(format!("{url}{separator}application_name={schema}"))?;
        // These tests share one database server with the rest of the suite.
        config.max_connections = SESSIONS_PER_STORE;
        let store = PostgresStore::connect(&config).await?;
        let (raw, connection) =
            tokio_postgres::connect(&connection_url_with_search_path(&base, &schema), NoTls)
                .await?;
        let raw_task = tokio::spawn(connection);
        let result = body(store, raw).await;
        raw_task.abort();
        result
    }
    .await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    admin_task.abort();
    result
}

pub(super) async fn class(store: &PostgresStore, id: &str) -> TestResult<WorkloadClassVersion> {
    let class = workload_class(id, 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    Ok(class)
}

/// Sends the proxy's wake for the instance at the generation given.
async fn wake(
    store: &PostgresStore,
    target: &MaterializationTarget,
    instance: &InstanceRecord,
) -> TestResult<pb::ProxyWakeInstanceResponse> {
    Ok(StoreBackedProxyApi::new(
        Arc::new(store.clone()),
        KubernetesMaterializer::new(LifecycleKubernetes::default()),
        target.clone(),
    )
    .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
        instance_id: instance.id.as_str().to_owned(),
        expected_generation: instance.generation.get(),
        backend_generation: None,
    }))
    .await?
    .into_inner())
}

async fn instance(store: &PostgresStore, id: &InstanceId) -> TestResult<InstanceRecord> {
    Ok(store
        .get_instance(GetInstanceRequest::new(id.clone()))
        .await?
        .ok_or("instance is gone")?)
}

async fn active(
    store: &PostgresStore,
    target: &MaterializationTarget,
    id: &InstanceId,
) -> TestResult<Option<MaterializationRecord>> {
    Ok(store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            id.clone(),
            target.clone(),
        ))
        .await?)
}

/// A new instance whose wake is accepted: it is Waking with a Pending row.
pub(super) async fn accepted(
    store: &PostgresStore,
    class: &WorkloadClassVersion,
    target: &MaterializationTarget,
    id: &str,
) -> TestResult<MaterializationRecord> {
    let cold = store
        .create_instance(create_instance_request(id, class.reference.clone(), vec![]))
        .await?
        .instance;
    wake(store, target, &cold).await?;
    Ok(active(store, target, &cold.id)
        .await?
        .ok_or("accepted wake has no row")?)
}

/// Claims the target's due Pending row and completes its wake.
async fn complete(
    store: &PostgresStore,
    target: &MaterializationTarget,
) -> TestResult<CompleteWakeResult> {
    let claimed = claim_next(
        store,
        MaterializationState::Pending,
        target,
        "driver",
        LEASE,
    )
    .await?
    .ok_or("no Pending row is due")?;
    let attempt = claimed.reconciliation_lease.as_ref().unwrap().attempt;
    let mut complete = complete_for_reconciled_pending(&claimed, "http://10.0.0.9:8080");
    complete.pod_backend = Some(BackendEndpoint::new(POD_BACKEND)?);
    Ok(store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            attempt,
            complete,
        ))
        .await?)
}

/// A new instance woken to Running.
pub(super) async fn running(
    store: &PostgresStore,
    class: &WorkloadClassVersion,
    target: &MaterializationTarget,
    id: &str,
) -> TestResult<CompleteWakeResult> {
    accepted(store, class, target, id).await?;
    complete(store, target).await
}

async fn schedule_at(raw: &tokio_postgres::Client, id: &str, next_attempt: i64) -> TestResult {
    raw.execute(
        "UPDATE materializations SET next_attempt_at_unix_millis = $2 WHERE materialization_id = $1",
        &[&id, &next_attempt],
    )
    .await?;
    Ok(())
}

pub(super) fn target(cluster: &str) -> MaterializationTarget {
    MaterializationTarget::new(cluster, "apps").expect("valid target")
}

#[tokio::test]
async fn postgres_claim_next_takes_the_due_row_of_one_state() -> TestResult {
    with_store(|store, mut raw| async move {
        let class = class(&store, "claim-class").await?;
        let claim = |state, target: MaterializationTarget| {
            let store = store.clone();
            async move { claim_next(&store, state, &target, "claimer", LEASE).await }
        };

        // One state per claim, in schedule order, within one target.
        let mixed = target("claim-mixed");
        let mut pending = Vec::new();
        for (name, next_attempt) in [("claim-p1", 3_i64), ("claim-p2", 1), ("claim-p3", 2)] {
            let row = accepted(&store, &class, &mixed, name).await?;
            schedule_at(&raw, row.id.as_str(), next_attempt).await?;
            pending.push(row.id);
        }
        let doomed = accepted(&store, &class, &mixed, "claim-d1").await?;
        let waking = instance(&store, &doomed.instance_id).await?;
        store
            .request_instance_deletion(RequestInstanceDeletion {
                instance_id: waking.id,
                expected_generation: waking.generation,
            })
            .await?;
        accepted(&store, &class, &target("claim-other"), "claim-elsewhere").await?;

        let cleanup = claim(MaterializationState::Deleting, mixed.clone())
            .await?
            .ok_or("the Deleting row is due")?;
        assert_eq!(cleanup.id, doomed.id);
        assert!(
            claim(MaterializationState::Deleting, mixed.clone())
                .await?
                .is_none(),
            "a live lease hides its row"
        );
        let mut order = Vec::new();
        while let Some(claimed) = claim(MaterializationState::Pending, mixed.clone()).await? {
            assert_eq!(claimed.target, mixed, "another target's row was claimed");
            assert_eq!(claimed.state, MaterializationState::Pending);
            order.push(claimed.id);
        }
        assert_eq!(
            order,
            [pending[1].clone(), pending[2].clone(), pending[0].clone()],
            "claims follow the schedule"
        );
        assert!(claim(MaterializationState::Pending, target("claim-empty"))
            .await?
            .is_none());

        // A cleanup is due only once its drain deadline has passed.
        let draining = target("claim-draining");
        let awake = running(&store, &class, &draining, "claim-drain").await?;
        store
            .begin_sleep(
                BeginSleepRequest::new(
                    awake.instance.id.clone(),
                    awake.instance.generation,
                    draining.clone(),
                )
                .with_drain_grace_timeout(Duration::from_secs(3600)),
            )
            .await?;
        assert!(
            claim(MaterializationState::Deleting, draining.clone())
                .await?
                .is_none(),
            "a claim may not skip the drain grace"
        );
        raw.execute(
            "UPDATE materializations SET drain_not_before_unix_millis = 0 WHERE materialization_id = $1",
            &[&awake.materialization.id.as_str()],
        )
        .await?;
        assert!(claim(MaterializationState::Deleting, draining)
            .await?
            .is_some());

        // A row another claimer has locked is skipped, never waited for.
        let contended = target("claim-contended");
        let first = accepted(&store, &class, &contended, "claim-q1").await?;
        let second = accepted(&store, &class, &contended, "claim-q2").await?;
        schedule_at(&raw, first.id.as_str(), 1).await?;
        schedule_at(&raw, second.id.as_str(), 2).await?;
        let holder = raw.transaction().await?;
        holder
            .query_one(
                "SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE",
                &[&first.id.as_str()],
            )
            .await?;
        let skipped = tokio::time::timeout(
            Duration::from_secs(5),
            claim(MaterializationState::Pending, contended.clone()),
        )
        .await??
        .ok_or("the unlocked row is claimable")?;
        assert_eq!(skipped.id, second.id);
        holder.rollback().await?;
        let released = claim(MaterializationState::Pending, contended)
            .await?
            .ok_or("the released row is claimable")?;
        assert_eq!(released.id, first.id);

        let raced = target("claim-raced");
        accepted(&store, &class, &raced, "claim-r1").await?;
        accepted(&store, &class, &raced, "claim-r2").await?;
        let (left, right) = tokio::join!(
            claim(MaterializationState::Pending, raced.clone()),
            claim(MaterializationState::Pending, raced.clone())
        );
        let (left, right) = (left?.unwrap(), right?.unwrap());
        assert_ne!(left.id, right.id, "two claimers took one row");

        // Wakes and cleanup claim independently: a cleanup backlog of any size
        // leaves a wake claimable.
        let backlog = target("claim-backlog");
        for index in 0..40 {
            let row = accepted(&store, &class, &backlog, &format!("claim-backlog-{index}")).await?;
            let waking = instance(&store, &row.instance_id).await?;
            store
                .request_instance_deletion(RequestInstanceDeletion {
                    instance_id: waking.id,
                    expected_generation: waking.generation,
                })
                .await?;
        }
        let wake = accepted(&store, &class, &backlog, "claim-backlog-wake").await?;
        let claimed = claim(MaterializationState::Pending, backlog)
            .await?
            .ok_or("a cleanup backlog hid a wake")?;
        assert_eq!(claimed.id, wake.id);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_force_delete_leaves_a_wakeable_instance() -> TestResult {
    with_store(|store, raw| async move {
        let class = class(&store, "force-class").await?;
        let force = |id: control_plane::MaterializationId| {
            store.force_delete_materialization(ForceDeleteMaterializationRequest::new(
                id,
                "operator",
                "old driver fenced; cluster cleanup confirmed",
            ))
        };
        // Whatever state the force delete leaves, it has a work row or can
        // accept a wake.
        let assert_settled = |settled: &InstanceRecord, row: &Option<MaterializationRecord>| {
            assert!(
                row.is_some()
                    || !matches!(
                        settled.state,
                        InstanceState::Waking | InstanceState::Running | InstanceState::Draining
                    ),
                "{settled:?} has no work row"
            );
        };

        // Waking becomes Failed, and the next wake runs to Running.
        let waking_target = target("force-waking");
        let pending = accepted(&store, &class, &waking_target, "force-waking").await?;
        let before = instance(&store, &pending.instance_id).await?;
        force(pending.id.clone()).await?;
        let settled = instance(&store, &pending.instance_id).await?;
        assert_eq!(settled.state, InstanceState::Failed);
        assert_eq!(settled.generation, before.generation.next());
        assert_settled(
            &settled,
            &active(&store, &waking_target, &settled.id).await?,
        );
        wake(&store, &waking_target, &settled).await?;
        assert_eq!(
            complete(&store, &waking_target).await?.instance.state,
            InstanceState::Running
        );

        // Running becomes Cold.
        let running_target = target("force-running");
        let awake = running(&store, &class, &running_target, "force-running").await?;
        force(awake.materialization.id.clone()).await?;
        let settled = instance(&store, &awake.instance.id).await?;
        assert_eq!(settled.state, InstanceState::Cold);
        assert_eq!(settled.generation, awake.instance.generation.next());
        assert_settled(
            &settled,
            &active(&store, &running_target, &settled.id).await?,
        );
        wake(&store, &running_target, &settled).await?;
        assert_eq!(
            complete(&store, &running_target).await?.instance.state,
            InstanceState::Running
        );

        // Draining becomes Cold, and a wake deferred during the drain starts.
        for deferred in [false, true] {
            let name = format!("force-draining-{deferred}");
            let draining_target = target(&name);
            let awake = running(&store, &class, &draining_target, &name).await?;
            let drain = store
                .begin_sleep(BeginSleepRequest::new(
                    awake.instance.id.clone(),
                    awake.instance.generation,
                    draining_target.clone(),
                ))
                .await?;
            if deferred {
                // The drain deadline has passed, so this wake waits for the sleep.
                wake(&store, &draining_target, &drain.instance).await?;
                assert_eq!(
                    instance(&store, &awake.instance.id).await?.state,
                    InstanceState::Draining
                );
            }
            force(awake.materialization.id.clone()).await?;
            let settled = instance(&store, &awake.instance.id).await?;
            let row = active(&store, &draining_target, &settled.id).await?;
            assert_settled(&settled, &row);
            if deferred {
                assert_eq!(settled.state, InstanceState::Waking);
                assert_eq!(
                    row.map(|row| row.state),
                    Some(MaterializationState::Pending)
                );
            } else {
                assert_eq!(settled.state, InstanceState::Cold);
                assert_eq!(settled.generation, drain.instance.generation.next());
                wake(&store, &draining_target, &settled).await?;
            }
            assert_eq!(
                complete(&store, &draining_target).await?.instance.state,
                InstanceState::Running
            );
        }
        assert_eq!(
            raw.query_one("SELECT count(*) FROM deferred_wake_intents", &[])
                .await?
                .get::<_, i64>(0),
            0,
            "an activated wake leaves no intent behind"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_cleanup_retries_until_it_succeeds_and_a_failed_wake_ends() -> TestResult {
    with_store(|store, raw| async move {
        let class = class(&store, "retry-class").await?;
        let expire = |id: control_plane::MaterializationId| {
            let raw = &raw;
            async move {
                raw.execute(
                    "UPDATE materializations SET operation_deadline_unix_millis = 1 WHERE materialization_id = $1",
                    &[&id.as_str()],
                )
                .await
            }
        };
        let fail = |claimed: &MaterializationRecord, permanent| {
            store.record_materialization_failure(RecordMaterializationFailure {
                expected_state: claimed.state,
                materialization_id: claimed.id.clone(),
                owner: "driver".into(),
                attempt: claimed.reconciliation_lease.as_ref().unwrap().attempt,
                generation: claimed.instance_generation,
                permanent,
                message: "controlled failure".into(),
            })
        };
        let status = |id: control_plane::MaterializationId| {
            let store = &store;
            async move {
                Ok::<_, Box<dyn Error + Send + Sync>>(
                    store
                        .load_materialization_work_status(id)
                        .await?
                        .ok_or("work status is gone")?,
                )
            }
        };
        // A new instance that is Draining with its cleanup due.
        let draining = |name: &'static str| {
            let (store, class) = (&store, &class);
            async move {
                let cleanup_target = target(name);
                let awake = running(store, class, &cleanup_target, name).await?;
                store
                    .begin_sleep(BeginSleepRequest::new(
                        awake.instance.id.clone(),
                        awake.instance.generation,
                        cleanup_target.clone(),
                    ))
                    .await?;
                Ok::<_, Box<dyn Error + Send + Sync>>((cleanup_target, awake))
            }
        };

        // A cleanup past its operation deadline stays ordinary retryable work.
        let (late_target, late) = draining("retry-late").await?;
        expire(late.materialization.id.clone()).await?;
        let claimed = claim_next(&store, MaterializationState::Deleting, &late_target, "driver", LEASE)
            .await?
            .ok_or("the cleanup is due")?;
        assert!(fail(&claimed, false).await?);
        let late_status = status(claimed.id.clone()).await?;
        assert_eq!(late_status.failure_kind.as_deref(), Some("transient"));
        assert_eq!(late_status.failure_count, 1);
        assert!(
            claim_next(&store, MaterializationState::Deleting, &late_target, "driver", LEASE)
                .await?
                .is_none(),
            "the retry waits out its backoff"
        );
        schedule_at(&raw, claimed.id.as_str(), 0).await?;
        assert!(
            claim_next(&store, MaterializationState::Deleting, &late_target, "driver", LEASE)
                .await?
                .is_some(),
            "a cleanup past its deadline was parked"
        );
        assert_eq!(
            instance(&store, &late.instance.id).await?.state,
            InstanceState::Draining
        );

        // Only a failure the driver marks permanent parks a cleanup, and an
        // operator's reconcile request re-arms it.
        let (parked_target, parked) = draining("retry-parked").await?;
        let claimed =
            claim_next(&store, MaterializationState::Deleting, &parked_target, "driver", LEASE)
                .await?
                .ok_or("the cleanup is due")?;
        assert!(fail(&claimed, true).await?);
        schedule_at(&raw, claimed.id.as_str(), 0).await?;
        assert_eq!(
            status(claimed.id.clone()).await?.failure_kind.as_deref(),
            Some("permanent")
        );
        assert!(
            claim_next(&store, MaterializationState::Deleting, &parked_target, "driver", LEASE)
                .await?
                .is_none(),
            "an ownership conflict parks the cleanup"
        );
        assert_eq!(
            store
                .load_materialization_operational_metrics()
                .await?
                .blocked_failures,
            1
        );
        assert!(store.enqueue_materialization(claimed.id.clone()).await?);
        assert!(
            claim_next(&store, MaterializationState::Deleting, &parked_target, "driver", LEASE)
                .await?
                .is_some()
        );
        assert_eq!(
            active(&store, &parked_target, &parked.instance.id)
                .await?
                .map(|row| row.exclusivity_keys),
            Some(parked.materialization.exclusivity_keys),
            "a parked cleanup keeps its keys"
        );

        // A wake past its deadline ends: the instance is Failed and the cleanup
        // of whatever the wake created is due at once.
        let wake_target = target("retry-wake");
        let pending = accepted(&store, &class, &wake_target, "retry-wake").await?;
        expire(pending.id.clone()).await?;
        let claimed = claim_next(&store, MaterializationState::Pending, &wake_target, "driver", LEASE)
            .await?
            .ok_or("the wake is due")?;
        assert!(fail(&claimed, false).await?);
        assert_eq!(
            instance(&store, &pending.instance_id).await?.state,
            InstanceState::Failed
        );
        let ended = status(pending.id.clone()).await?;
        assert_eq!(ended.failure_kind.as_deref(), Some("deadline"));
        assert_eq!(ended.failure_count, 0, "cleanup starts with a fresh backoff");
        assert_eq!(ended.wake_failure_message, "controlled failure");
        let cleanup = claim_next(&store, MaterializationState::Deleting, &wake_target, "driver", LEASE)
            .await?
            .ok_or("a failed wake's cleanup waited")?;
        assert_eq!(cleanup.id, pending.id);
        assert_eq!(
            cleanup.rendered_objects, pending.rendered_objects,
            "the failed wake keeps its inventory for cleanup"
        );
        Ok(())
    })
    .await
}

/// Accepts every idle member, so the store decides each report's outcome.
#[derive(Clone, Default)]
struct IdleMember(LifecycleKubernetes);

impl KubernetesMaterializerClient for IdleMember {
    fn verify_idle_member<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        _identity: &'a control_plane::materializer::IdleMemberIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.0.apply_object(object, precondition)
    }
    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.0.delete_object(object, precondition)
    }
    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        self.0.wait_for_readiness(objects)
    }
}

#[tokio::test]
async fn postgres_pod_address_is_recorded_and_cleared_with_the_backend() -> TestResult {
    with_store(|store, _raw| async move {
        let class = class(&store, "pod-address-class").await?;
        let target = target("pod-address");
        let awake = running(&store, &class, &target, "pod-address").await?;
        let pod = |row: &MaterializationRecord| {
            row.pod_backend
                .as_ref()
                .map(|backend| backend.uri().to_owned())
        };
        assert_eq!(pod(&awake.materialization).as_deref(), Some(POD_BACKEND));
        assert_ne!(
            awake.materialization.pod_backend,
            awake.materialization.backend
        );

        // The Pod runs through the drain grace.
        store
            .begin_sleep(
                BeginSleepRequest::new(
                    awake.instance.id.clone(),
                    awake.instance.generation,
                    target.clone(),
                )
                .with_drain_grace_timeout(Duration::ZERO),
            )
            .await?;
        let draining = active(&store, &target, &awake.instance.id)
            .await?
            .ok_or("the draining row is gone")?;
        assert_eq!(pod(&draining).as_deref(), Some(POD_BACKEND));
        assert!(draining.backend.is_some());

        // Finalizing the sleep clears both addresses.
        let claimed = claim_next(
            &store,
            MaterializationState::Deleting,
            &target,
            "driver",
            LEASE,
        )
        .await?
        .ok_or("the cleanup is not due")?;
        let slept = instance(&store, &awake.instance.id).await?;
        store
            .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
                claimed.id.clone(),
                "driver",
                claimed.reconciliation_lease.as_ref().unwrap().attempt,
                FinalizeSleepRequest::new(slept.id.clone(), slept.generation, target.clone()),
            ))
            .await?;
        let deleted = store
            .load_materialization(LoadMaterializationRequest::new(claimed.id))
            .await?
            .ok_or("the row is gone")?;
        assert_eq!(deleted.state, MaterializationState::Deleted);
        assert_eq!((deleted.backend, deleted.pod_backend), (None, None));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_route_rules_carry_the_pod_address_of_the_ready_backend() -> TestResult {
    with_store(|store, _raw| async move {
        let class = class(&store, "pod-route-class").await?;
        let target = target("pod-route");
        let identity = http_identity("pod-route.example.test", None);
        let cold = store
            .create_instance(create_instance_request(
                "pod-route",
                class.reference.clone(),
                vec![identity.clone()],
            ))
            .await?
            .instance;
        let addresses = || async {
            let entry = resolve_route(&store, identity.clone(), target.clone())
                .await?
                .ok_or("the binding no longer resolves")?;
            let uri = |backend: Option<BackendEndpoint>| backend.map(|b| b.uri().to_owned());
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                uri(entry.backend),
                uri(entry.pod_backend),
            ))
        };
        assert_eq!(addresses().await?, (None, None));

        wake(&store, &target, &cold).await?;
        let awake = complete(&store, &target).await?;
        assert_eq!(
            addresses().await?,
            (
                Some("http://10.0.0.9:8080".to_owned()),
                Some(POD_BACKEND.to_owned())
            )
        );

        // A draining instance has no ready backend to route to.
        store
            .begin_sleep(BeginSleepRequest::new(
                awake.instance.id.clone(),
                awake.instance.generation,
                target.clone(),
            ))
            .await?;
        assert_eq!(addresses().await?, (None, None));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_wake_during_drain_grace_revives_the_projection() -> TestResult {
    use pb::{
        proxy_wake_instance_response::Outcome as Wake,
        sidecar_report_idle_response::Outcome as Idle,
    };
    with_store(|store, raw| async move {
        let class = class(&store, "revive-class").await?;
        let grace = Duration::from_secs(3600);
        let drain = |awake: &InstanceRecord, target: &MaterializationTarget, grace| {
            store.begin_sleep(
                BeginSleepRequest::new(awake.id.clone(), awake.generation, target.clone())
                    .with_drain_grace_timeout(grace),
            )
        };
        let intents = || async {
            Ok::<_, tokio_postgres::Error>(
                raw.query_one("SELECT count(*) FROM deferred_wake_intents", &[])
                    .await?
                    .get::<_, i64>(0),
            )
        };

        // Inside the grace no driver has touched the projection, so the wake
        // returns it to Ready and the instance to Running in one step.
        let revived_target = target("revive");
        let awake = running(&store, &class, &revived_target, "revive").await?;
        let draining = drain(&awake.instance, &revived_target, grace).await?.instance;
        let response = wake(&store, &revived_target, &draining).await?;
        let Some(Wake::Ready(ready)) = response.outcome else {
            return Err(format!("a wake during drain grace was not served: {response:?}").into());
        };
        let revived = instance(&store, &awake.instance.id).await?;
        assert_eq!(revived.state, InstanceState::Running);
        assert_eq!(revived.generation, draining.generation.next());
        assert_eq!(ready.instance_generation, revived.generation.get());
        let projection = active(&store, &revived_target, &revived.id)
            .await?
            .ok_or("the revived projection is gone")?;
        assert_eq!(projection.state, MaterializationState::Ready);
        assert_eq!(projection.instance_generation, revived.generation);
        assert_eq!(
            projection.projection_generation, awake.materialization.projection_generation,
            "the sidecar's projection generation is stable across a revive"
        );
        assert_eq!(projection.backend, awake.materialization.backend);
        assert_eq!(
            projection.pod_backend.as_ref().map(BackendEndpoint::uri),
            Some(POD_BACKEND),
            "the Pod is the same one, so its address stands"
        );
        assert_eq!(
            projection.backend_generation.get(),
            awake.materialization.backend_generation.get() + 1
        );
        assert_eq!(projection.rendered_objects, awake.materialization.rendered_objects);
        assert_eq!(intents().await?, 0);
        assert!(
            claim_next(&store, MaterializationState::Deleting, &revived_target, "driver", LEASE)
                .await?
                .is_none(),
            "a revived projection has no cleanup"
        );

        // The Ready transition restarts the activation floor.
        assert!(matches!(
            store
                .begin_sleep(
                    BeginSleepRequest::new(revived.id.clone(), revived.generation, revived_target.clone())
                        .with_minimum_ready_age(sleepypods_api::INITIAL_ACTIVATION_TIMEOUT)
                )
                .await,
            Err(StoreError::SleepDeferred { retry_after }) if retry_after > Duration::from_secs(180)
        ));

        // The sidecar still reports the projection generation it was rendered
        // with, although the instance has moved on three generations.
        let sidecar = StoreBackedSidecarApi::new(
            Arc::new(store.clone()),
            KubernetesMaterializer::new(IdleMember::default()),
            revived_target.clone(),
        );
        let report = |expected_generation: u64| {
            let sidecar = &sidecar;
            let instance_id = revived.id.as_str().to_owned();
            async move {
                sidecar
                    .report_idle(tonic::Request::new(pb::SidecarReportIdleRequest {
                        instance_id,
                        expected_generation,
                        active_count: 0,
                        pod_uid: "pod-uid".into(),
                    }))
                    .await
                    .map(|response| response.into_inner().outcome)
            }
        };
        let stable = projection.projection_generation.get();
        let deferred = report(stable).await.expect_err("the floor restarted");
        assert_eq!(deferred.code(), tonic::Code::FailedPrecondition);
        raw.execute(
            "UPDATE materializations SET state_entered_at_unix_millis = state_entered_at_unix_millis - 400000 WHERE materialization_id = $1",
            &[&projection.id.as_str()],
        )
        .await?;
        assert!(matches!(
            report(revived.generation.get()).await?,
            Some(Idle::GenerationConflict(_))
        ));
        let Some(Idle::Accepted(accepted)) = report(stable).await? else {
            return Err("an idle report after a revive was not accepted".into());
        };
        assert_eq!(accepted.instance_generation, revived.generation.get() + 1);
        assert!(
            matches!(report(stable).await?, Some(Idle::AlreadyDraining(_))),
            "a draining instance answers its own Pod with AlreadyDraining"
        );
        assert!(matches!(
            report(stable + 1).await?,
            Some(Idle::GenerationConflict(_))
        ));

        // Past the drain deadline teardown may have begun, so the wake waits
        // for the sleep to finish.
        let late_target = target("revive-late");
        let awake = running(&store, &class, &late_target, "revive-late").await?;
        let draining = drain(&awake.instance, &late_target, Duration::ZERO).await?.instance;
        assert!(matches!(
            wake(&store, &late_target, &draining).await?.outcome,
            Some(Wake::StillWaking(_))
        ));
        assert_eq!(
            instance(&store, &awake.instance.id).await?.state,
            InstanceState::Draining
        );
        assert_eq!(intents().await?, 1);

        // A projection a driver holds is never revived, whatever its deadline.
        let held_target = target("revive-held");
        let awake = running(&store, &class, &held_target, "revive-held").await?;
        let draining = drain(&awake.instance, &held_target, grace).await?.instance;
        raw.execute(
            "UPDATE materializations SET reconcile_owner = 'driver', reconcile_lease_expires_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + 60000 WHERE materialization_id = $1",
            &[&awake.materialization.id.as_str()],
        )
        .await?;
        assert!(matches!(
            wake(&store, &held_target, &draining).await?.outcome,
            Some(Wake::StillWaking(_))
        ));
        assert_eq!(
            active(&store, &held_target, &awake.instance.id)
                .await?
                .map(|row| row.state),
            Some(MaterializationState::Deleting)
        );
        Ok(())
    })
    .await
}

/// A wake or sleep completion locks the instance before its materialization,
/// as every acceptance does, and a cleanup completion locks the materialization
/// alone, so a completion and a deletion never deadlock.
#[tokio::test]
async fn postgres_completions_and_deletion_share_one_lock_order() -> TestResult {
    with_store(|store, raw| async move {
        let class = class(&store, "order-class").await?;
        let sessions: String = raw.query_one("SELECT current_schema()::text", &[]).await?.get(0);
        let (mut locker, connection) = tokio_postgres::connect(
            &connection_url_with_search_path(&std::env::var("SLEEPYPODS_POSTGRES_URL")?, &sessions),
            NoTls,
        )
        .await?;
        let locker_task = tokio::spawn(connection);
        for completion in ["complete-wake", "finalize-sleep", "mark-deleted"] {
            for deletion_first in [false, true] {
                let name = format!("order-{completion}-{deletion_first}");
                let order_target = target(&name);
                // Claimed work for each completion: a wake, a sleep's cleanup,
                // and the cleanup of a wake whose instance is being deleted.
                let pending = accepted(&store, &class, &order_target, &name).await?;
                let claimed = match completion {
                    "complete-wake" => {
                        claim_next(&store, MaterializationState::Pending, &order_target, "driver", LEASE).await?
                    }
                    "finalize-sleep" => {
                        let awake = complete(&store, &order_target).await?;
                        store
                            .begin_sleep(BeginSleepRequest::new(
                                awake.instance.id.clone(),
                                awake.instance.generation,
                                order_target.clone(),
                            ))
                            .await?;
                        claim_next(&store, MaterializationState::Deleting, &order_target, "driver", LEASE).await?
                    }
                    _ => {
                        let waking = instance(&store, &pending.instance_id).await?;
                        store
                            .request_instance_deletion(RequestInstanceDeletion {
                                instance_id: waking.id,
                                expected_generation: waking.generation,
                            })
                            .await?;
                        claim_next(&store, MaterializationState::Deleting, &order_target, "driver", LEASE).await?
                    }
                }
                .ok_or("the work is due")?;
                let attempt = claimed.reconciliation_lease.as_ref().unwrap().attempt;
                let current = instance(&store, &claimed.instance_id).await?;

                let complete_store = store.clone();
                let complete_claimed = claimed.clone();
                let draining_generation = current.generation;
                let finish = move || async move {
                    let (store, claimed) = (complete_store, complete_claimed);
                    match completion {
                        "complete-wake" => store
                            .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
                                claimed.id.clone(),
                                "driver",
                                attempt,
                                complete_for_reconciled_pending(&claimed, "http://10.0.0.9:8080"),
                            ))
                            .await
                            .map(|_| ()),
                        "finalize-sleep" => store
                            .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
                                claimed.id.clone(),
                                "driver",
                                attempt,
                                FinalizeSleepRequest::new(
                                    claimed.instance_id.clone(),
                                    draining_generation,
                                    claimed.target.clone(),
                                ),
                            ))
                            .await
                            .map(|_| ()),
                        _ => store
                            .delete_materialization_reconciliation(
                                DeleteMaterializationReconciliationRequest::new(
                                    claimed.id.clone(),
                                    "driver",
                                    attempt,
                                    claimed.state,
                                    claimed.instance_id.clone(),
                                    claimed.instance_generation,
                                    claimed.target.clone(),
                                ),
                            )
                            .await
                            .map(|_| ()),
                    }
                };
                let delete_store = store.clone();
                let delete = move || async move {
                    delete_store
                        .request_instance_deletion(RequestInstanceDeletion {
                            instance_id: current.id,
                            expected_generation: current.generation,
                        })
                        .await
                        .map(|_| ())
                };

                // Both operations queue behind this session's instance lock.
                let holder = locker.transaction().await?;
                holder
                    .query_one(
                        "SELECT instance_id FROM instances WHERE instance_id = $1 FOR UPDATE",
                        &[&claimed.instance_id.as_str()],
                    )
                    .await?;
                // The first waiter queues on the holder's transaction and the
                // second on the first, so count the store's sessions waiting on
                // any lock. The holder's own transaction would read one cached
                // snapshot of the activity view; the observer reads a fresh one.
                let blocked = |count: i64| {
                    let (raw, sessions) = (&raw, &sessions);
                    async move {
                        tokio::time::timeout(Duration::from_secs(5), async {
                            loop {
                                let waiting: i64 = raw
                                    .query_one(
                                        "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'",
                                        &[sessions],
                                    )
                                    .await?
                                    .get(0);
                                if waiting >= count {
                                    return Ok::<_, tokio_postgres::Error>(());
                                }
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                        })
                        .await
                    }
                };
                let mut operations = tokio::task::JoinSet::new();
                // A cleanup completion finishes while the instance stays locked.
                let cleanup = completion == "mark-deleted";
                let waiters = if cleanup { 1 } else { 2 };
                let unblocked = |_| format!("{name}: a cleanup completion waits for the instance");
                if deletion_first {
                    operations.spawn(delete());
                    blocked(1).await??;
                    if cleanup {
                        tokio::time::timeout(Duration::from_secs(5), finish()).await.map_err(unblocked)??;
                    } else {
                        operations.spawn(finish());
                    }
                } else {
                    if cleanup {
                        tokio::time::timeout(Duration::from_secs(5), finish()).await.map_err(unblocked)??;
                    } else {
                        operations.spawn(finish());
                        blocked(1).await??;
                    }
                    operations.spawn(delete());
                }
                blocked(waiters).await??;
                // Neither waits while holding the materialization.
                holder
                    .query_one(
                        "SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE NOWAIT",
                        &[&claimed.id.as_str()],
                    )
                    .await
                    .map_err(|error| format!("{name}: an operation holds the materialization while it waits for the instance: {error}"))?;
                holder.commit().await?;
                tokio::time::timeout(Duration::from_secs(10), async {
                    while let Some(joined) = operations.join_next().await {
                        // Either may lose to the other; neither may deadlock.
                        let finished = joined.map_err(|error| format!("{name}: {error}"))?;
                        if let Err(StoreError::Unavailable { message }) = finished {
                            return Err(format!("{name}: {message}"));
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(|_| format!("{name}: the operations did not finish"))??;
            }
        }
        locker_task.abort();
        Ok(())
    })
    .await
}

/// Pods that become ready, and stop, at a fixed time.
#[derive(Clone)]
struct PodsSettleAt(LifecycleKubernetes, tokio::time::Instant);

impl KubernetesMaterializerClient for PodsSettleAt {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.0.apply_object(object, precondition)
    }
    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.0.delete_object(object, precondition)
    }
    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async move {
            tokio::time::sleep_until(self.1).await;
            self.0.wait_for_readiness(objects).await
        })
    }
    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<
        'a,
        KubernetesClientResult<control_plane::projection::ProjectionObjectInspection>,
    > {
        self.0.inspect_object(object)
    }
    fn ensure_no_descendants<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        if tokio::time::Instant::now() < self.1 {
            return Box::pin(async {
                Err(control_plane::KubernetesClientError::transient(
                    "Pods are stopping",
                ))
            });
        }
        self.0.ensure_no_descendants(objects, instance_id)
    }
    fn verify_retained_bindings<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.0.verify_retained_bindings(objects)
    }
}

/// The reconciler rides out a 30 s database outage that resets its sessions
/// mid-wake and mid-sleep: it keeps running, both instances reach their
/// state once the database returns, and no failure is recorded. The wake's
/// Pod becomes ready, and the sleep's Pod stops, inside the outage.
#[tokio::test]
async fn postgres_reconciler_rides_out_a_thirty_second_outage() -> TestResult {
    use sleepypods_observability::{
        metrics::{
            CONTROL_PLANE_STORE_AVAILABLE, RECONCILER_CLAIMS_TOTAL,
            RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
        },
        recorder::{InMemoryObservability, ObservabilityEvent},
    };
    const OUTAGE: Duration = Duration::from_secs(30);
    with_store(|store, raw| async move {
        let base = std::env::var("SLEEPYPODS_POSTGRES_URL")?;
        let class = class(&store, "outage-class").await?;
        let target = target("outage");
        let awake = running(&store, &class, &target, "outage-sleep").await?;
        store
            .begin_sleep(
                BeginSleepRequest::new(
                    awake.instance.id.clone(),
                    awake.instance.generation,
                    target.clone(),
                )
                .with_drain_grace_timeout(Duration::ZERO),
            )
            .await?;
        let waking = accepted(&store, &class, &target, "outage-wake").await?;
        let schema: String = raw
            .query_one("SELECT current_schema()::text", &[])
            .await?
            .get(0);

        let relay = postgres_relay::Relay::start(&base).await?;
        let mut relayed = PostgresStoreConfig::new(connection_url_with_search_path(
            &relay.url(&base)?,
            &schema,
        ))?;
        relayed.max_connections = SESSIONS_PER_STORE;
        let relayed: Arc<dyn ControlPlaneStore> =
            Arc::new(PostgresStore::connect(&relayed).await?,);
        let sink = InMemoryObservability::default();
        let kube = LifecycleKubernetes::default();
        let driver = control_plane::MaterializationReconciler::new(
            relayed,
            KubernetesMaterializer::new(PodsSettleAt(
                kube.clone(),
                tokio::time::Instant::now() + Duration::from_secs(5),
            )),
            target.clone(),
            control_plane::MaterializationReconcilerConfig {
                owner: "outage-driver".into(),
                ..Default::default()
            },
            sink.recorder(),
        );
        let (shutdown, stopped) = tokio::sync::watch::channel(false);
        let running = tokio::spawn(driver.run_until_shutdown(stopped));

        // The driver holds both rows, and the wake has created and scaled up
        // its workload: the Service, the Deployment and its activation.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let held: i64 = raw
                    .query_one(
                        "SELECT count(*) FROM materializations WHERE reconcile_owner = 'outage-driver'",
                        &[],
                    )
                    .await?
                    .get(0);
                if held == 2 && kube.applies.load(std::sync::atomic::Ordering::SeqCst) == 3 {
                    return Ok::<_, tokio_postgres::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "the driver did not claim the wake and the sleep")??;
        // The outage starts while the wake's completion is in flight.
        relay.cut_at(waking.instance_id.as_str().as_bytes());
        tokio::time::timeout(Duration::from_secs(15), relay.wait_until_cut())
            .await
            .map_err(|_| "the wake never sent its completion")?;
        tokio::time::sleep(OUTAGE).await;
        assert!(!running.is_finished(), "the reconciler stopped in the outage");
        relay.restore();

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let woke = instance(&store, &waking.instance_id).await?.state;
                let slept = instance(&store, &awake.instance.id).await?.state;
                if (woke, slept) == (InstanceState::Running, InstanceState::Cold) {
                    return Ok::<_, Box<dyn Error + Send + Sync>>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| "the wake and the sleep did not finish after the outage")??;
        assert!(!running.is_finished(), "the reconciler stopped after the outage");
        let metric = |name: &str| {
            sink.events()
                .iter()
                .filter_map(|event| match event {
                    ObservabilityEvent::Metric(metric) if metric.name() == name => {
                        Some(metric.value())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            metric(RECONCILER_CLAIMS_TOTAL.name()).len(),
            2,
            "each first attempt survived the outage"
        );
        assert!(
            metric(RUNTIME_MATERIALIZATION_FAILURES_TOTAL.name()).is_empty(),
            "the outage ended an attempt"
        );
        assert!(
            metric(CONTROL_PLANE_STORE_AVAILABLE.name()).contains(&0.0),
            "the outage went unreported"
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            while metric(CONTROL_PLANE_STORE_AVAILABLE.name()).last() != Some(&1.0) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| "the store never reported available again")?;

        shutdown.send_replace(true);
        running.await??;
        Ok(())
    })
    .await
}

/// Deleting a Running instance tears it down at once: drain grace belongs to
/// sleep. A deletion accepted during a drain keeps that drain's deadline.
#[tokio::test]
async fn postgres_delete_of_a_running_instance_has_no_drain_grace() -> TestResult {
    with_store(|store, raw| async move {
        let class = class(&store, "delete-grace-class").await?;
        let drain_deadline = |id: control_plane::MaterializationId| {
            let raw = &raw;
            async move {
                Ok::<_, tokio_postgres::Error>(
                    raw.query_one(
                        "SELECT drain_not_before_unix_millis FROM materializations WHERE materialization_id = $1",
                        &[&id.as_str()],
                    )
                    .await?
                    .get::<_, i64>(0),
                )
            }
        };
        let delete = |current: InstanceRecord| {
            store.request_instance_deletion(RequestInstanceDeletion {
                instance_id: current.id,
                expected_generation: current.generation,
            })
        };

        let running_target = target("delete-running");
        let awake = running(&store, &class, &running_target, "delete-running").await?;
        assert!(delete(awake.instance.clone()).await?);
        assert_eq!(drain_deadline(awake.materialization.id.clone()).await?, 0);
        let cleanup =
            claim_next(&store, MaterializationState::Deleting, &running_target, "driver", LEASE)
                .await?
                .ok_or("deleting a Running instance did not start cleanup at once")?;
        assert_eq!(cleanup.id, awake.materialization.id);

        let draining_target = target("delete-draining");
        let awake = running(&store, &class, &draining_target, "delete-draining").await?;
        let draining = store
            .begin_sleep(
                BeginSleepRequest::new(
                    awake.instance.id.clone(),
                    awake.instance.generation,
                    draining_target.clone(),
                )
                .with_drain_grace_timeout(Duration::from_secs(3600)),
            )
            .await?;
        let deadline = drain_deadline(awake.materialization.id.clone()).await?;
        assert!(delete(draining.instance).await?);
        assert_eq!(
            drain_deadline(awake.materialization.id.clone()).await?,
            deadline,
            "deletion keeps the drain's deadline"
        );
        assert!(
            claim_next(&store, MaterializationState::Deleting, &draining_target, "driver", LEASE)
                .await?
                .is_none(),
            "deletion during a drain skipped its grace"
        );
        Ok(())
    })
    .await
}
