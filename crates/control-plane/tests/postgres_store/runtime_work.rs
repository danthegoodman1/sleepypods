use super::*;
use control_plane::{api::RouteSubscriptionBroker, RetryingControlPlaneStore};
use std::sync::Arc;

#[tokio::test]
async fn postgres_runtime_ordered_notifications_and_retention() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
    let pg = PostgresStore::connect(&config).await?;
    let result = ordered_notifications(pg, &config, &admin).await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    admin_task.abort();
    result
}

async fn ordered_notifications(
    pg: PostgresStore,
    config: &PostgresStoreConfig,
    admin: &tokio_postgres::Client,
) -> TestResult {
    let class = workload_class("runtime-events", 1);
    pg.create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let store = Arc::new(RetryingControlPlaneStore::with_default_policy(Arc::new(pg)));
    for id in ["order-a", "order-b"] {
        store
            .create_instance(create_instance_request(
                id,
                id,
                class.reference.clone(),
                vec![http_route(&format!("{id}.example.test"), None)],
            ))
            .await?;
    }
    let initial = store.load_route_changes(0, 1024).await?;
    assert!(!initial.reset);
    assert_eq!(
        initial.events.len(),
        4,
        "bundled instance and route writes both persist intent"
    );
    let cursor = initial.cursor;
    let (mut first, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let first_task = tokio::spawn(connection);
    let tx = first.transaction().await?;
    tx.execute("UPDATE instances SET state = 'waking', generation = generation + 1 WHERE instance_id = 'order-a'", &[]).await?;
    let (second, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let second_task = tokio::spawn(connection);
    let contender = tokio::spawn(async move {
        second.execute("/* runtime-second-commit */ UPDATE instances SET state = 'waking', generation = generation + 1 WHERE instance_id = 'order-b'", &[]).await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%runtime-second-commit%' AND pid <> pg_backend_pid())", &[]).await.unwrap().get(0);
            if blocked { break; }
            tokio::task::yield_now().await;
        }
    }).await?;
    assert_eq!(
        store.load_route_changes(cursor, 1024).await?.cursor,
        cursor,
        "uncommitted revision ranges are invisible"
    );
    tx.commit().await?;
    contender.await??;
    let committed = store.load_route_changes(cursor, 1024).await?;
    assert_eq!(
        committed
            .events
            .iter()
            .map(|event| event["instance_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["order-a", "order-b"]
    );
    assert_eq!(
        committed.cursor,
        cursor + 2,
        "later transaction cannot publish an overtaking revision"
    );
    let tx = first.transaction().await?;
    tx.execute("UPDATE instances SET state = 'failed', generation = generation + 1 WHERE instance_id = 'order-a'", &[]).await?;
    tx.rollback().await?;
    assert_eq!(
        store.load_route_change_revision().await?,
        committed.cursor,
        "rollback removes both state and intent"
    );

    // Every process independently replays history; neither consumes another's events.
    let left = RouteSubscriptionBroker::new();
    let right = RouteSubscriptionBroker::new();
    let mut left_events = left.subscribe();
    let mut right_events = right.subscribe();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let a = tokio::spawn(control_plane::runtime::dispatch_route_changes(
        store.clone(),
        left,
        receiver.clone(),
    ));
    let b = tokio::spawn(control_plane::runtime::dispatch_route_changes(
        store.clone(),
        right,
        receiver,
    ));
    for _ in 0..committed.cursor {
        let a = tokio::time::timeout(Duration::from_secs(3), left_events.recv()).await??;
        let b = tokio::time::timeout(Duration::from_secs(3), right_events.recv()).await??;
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
    }
    first.execute("UPDATE instances SET state = 'running', generation = generation + 1 WHERE instance_id = 'order-a'", &[]).await?;
    for events in [&mut left_events, &mut right_events] {
        let event = tokio::time::timeout(Duration::from_secs(3), events.recv()).await??;
        assert!(format!("{event:?}").contains("order-a"));
    }
    shutdown.send_replace(true);
    a.await??;
    b.await??;

    // Clock skew must never prune an interior hole that MIN(revision) cannot detect.
    let before = store.load_route_change_revision().await?;
    for state in ["failed", "cold", "waking"] {
        first.execute("UPDATE instances SET state = $1, generation = generation + 1 WHERE instance_id = 'order-b'", &[&state]).await?;
    }
    first
        .execute(
            "UPDATE route_change_outbox SET created_at_unix_millis = 0 WHERE revision <> $1",
            &[&((before + 2) as i64)],
        )
        .await?;
    store.maintain_runtime_records(1024).await?;
    let preserved = store.load_route_changes(before + 1, 1024).await?;
    assert!(!preserved.reset);
    assert_eq!(
        preserved.events.len(),
        2,
        "old timestamp behind a newer revision is retained"
    );
    assert!(
        store.load_route_changes(0, 1024).await?.reset,
        "prefix retention forces lagging processes to reset"
    );

    // Statement ranges preserve payload count without repeated counter-row updates.
    let baseline = store.load_route_change_revision().await?;
    let started = std::time::Instant::now();
    first.execute("INSERT INTO route_bindings(route_binding_id,instance_id,identity_key,identity_kind,host_kind,host,path_prefix,protocol) SELECT 'bulk-' || n, 'order-a', 'bulk-key-' || n, 'http', 'exact', 'bulk-' || n || '.example.test', NULL, 'http' FROM generate_series(1,100003) n", &[]).await?;
    assert_eq!(store.load_route_change_revision().await?, baseline + 100003);
    assert_eq!(
        first
            .query_one("SELECT count(*) FROM route_change_outbox", &[])
            .await?
            .get::<_, i64>(0),
        100000
    );
    assert!(store.load_route_changes(baseline, 1024).await?.reset);
    let tail = store.load_route_changes(baseline + 100000, 1024).await?;
    assert_eq!(tail.events.len(), 3);
    assert!(tail
        .events
        .iter()
        .all(|event| event["kind"] == "route" && event["removed"] == false));
    first.execute("UPDATE route_bindings SET path_prefix = '/new' WHERE route_binding_id IN ('bulk-1', 'bulk-2')", &[]).await?;
    let updated = store.load_route_changes(baseline + 100003, 1024).await?;
    assert_eq!(updated.events.len(), 2);
    assert!(updated
        .events
        .iter()
        .all(|event| event["path_prefix"] == "/new"));
    first
        .execute(
            "DELETE FROM route_bindings WHERE route_binding_id IN ('bulk-1', 'bulk-2')",
            &[],
        )
        .await?;
    let deleted = store.load_route_changes(updated.cursor, 1024).await?;
    assert_eq!(deleted.events.len(), 2);
    assert!(deleted.events.iter().all(|event| event["removed"] == true));
    eprintln!(
        "phase6 ordered outbox bulk100003/retention/targeted updates passed in {:?}",
        started.elapsed()
    );
    first_task.abort();
    second_task.abort();
    Ok(())
}

#[derive(Clone, Default)]
struct SchedulerKubernetes {
    inner: LifecycleKubernetes,
    behavior: Arc<std::sync::Mutex<BTreeMap<String, &'static str>>>,
    waiting: Arc<std::sync::atomic::AtomicUsize>,
    readiness_cancelled: Arc<std::sync::atomic::AtomicUsize>,
    operation_gate: Option<Arc<tokio::sync::Semaphore>>,
    cleanup_gate: Option<Arc<SupersessionCleanupGate>>,
}

struct ReadinessCancellation(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for ReadinessCancellation {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

struct SupersessionCleanupGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl control_plane::KubernetesMaterializerClient for SchedulerKubernetes {
    fn apply_object<'a>(
        &'a self,
        object: &'a control_plane::KubernetesObject,
        condition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        Box::pin(async move {
            let json = object.to_kubernetes_json();
            let id = json["metadata"]["labels"]["sleepypods.io/instance-id"]
                .as_str()
                .unwrap_or("");
            let behavior = self.behavior.lock().unwrap().get(id).copied();
            if matches!(
                behavior,
                Some("apply-permanent" | "apply-transient" | "apply-hang")
            ) {
                self.waiting
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.operation_gate
                    .as_ref()
                    .expect("apply gate")
                    .acquire()
                    .await
                    .expect("open gate")
                    .forget();
                return Err(if behavior == Some("apply-permanent") {
                    control_plane::KubernetesClientError::new("forbidden stale Pending apply")
                } else {
                    control_plane::KubernetesClientError::transient(
                        "unavailable stale Pending apply",
                    )
                });
            }
            match behavior {
                Some("permanent") => {
                    Err(control_plane::KubernetesClientError::new("forbidden apply"))
                }
                Some("transient") => Err(control_plane::KubernetesClientError::transient(
                    "temporarily unavailable",
                )),
                Some("uncertain") => Err(control_plane::KubernetesClientError::uncertain(
                    "request reply lost",
                )),
                _ => self.inner.apply_object(object, condition).await,
            }
        })
    }
    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        condition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        self.inner.delete_object(object, condition)
    }
    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        self.inner.wait_for_pvc_bound(namespace, name)
    }
    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> control_plane::KubernetesClientFuture<
        'a,
        control_plane::KubernetesClientResult<BackendEndpoint>,
    > {
        Box::pin(async move {
            let id = objects.iter().find_map(|object| {
                self.inner
                    .objects
                    .lock()
                    .unwrap()
                    .get(&lifecycle_object_key(object))
                    .and_then(|live| live.labels.get("sleepypods.io/instance-id").cloned())
            });
            let behavior = id.and_then(|id| self.behavior.lock().unwrap().get(&id).copied());
            if behavior == Some("hang") {
                let _cancelled = ReadinessCancellation(self.readiness_cancelled.clone());
                self.waiting
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            self.inner.wait_for_readiness(objects).await
        })
    }
    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> control_plane::KubernetesClientFuture<
        'a,
        control_plane::KubernetesClientResult<
            control_plane::projection::ProjectionObjectInspection,
        >,
    > {
        self.inner.inspect_object(object)
    }
    fn ensure_no_descendants<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        id: &'a str,
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        Box::pin(async move {
            if let Some(gate) = &self.cleanup_gate {
                gate.entered.notify_one();
                gate.release.acquire().await.expect("cleanup gate").forget();
            }
            self.inner.ensure_no_descendants(objects, id).await
        })
    }
    fn verify_retained_bindings<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        self.inner.verify_retained_bindings(objects)
    }
}

#[tokio::test]
async fn postgres_runtime_fair_scheduling_failure_policy_and_restart() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
    let pg = PostgresStore::connect(&config).await?;
    let result = scheduling(pg, &config).await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    admin_task.abort();
    result
}

async fn scheduling(pg: PostgresStore, config: &PostgresStoreConfig) -> TestResult {
    use control_plane::{
        api::{pb, StoreBackedProxyApi},
        KubernetesMaterializer, MaterializationReconciler, MaterializationReconcilerConfig,
    };
    use pb::{
        operator_control_plane_server::OperatorControlPlane,
        proxy_control_plane_server::ProxyControlPlane,
    };
    let class = workload_class("runtime-scheduler", 1);
    pg.create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let store = Arc::new(RetryingControlPlaneStore::with_default_policy(Arc::new(pg)));
    let (raw, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let raw_task = tokio::spawn(connection);
    let client = SchedulerKubernetes::default();
    let materializer = KubernetesMaterializer::new(client.clone());
    let target = MaterializationTarget::new("fair-cluster", "apps")?;
    let driver = |target: MaterializationTarget| {
        MaterializationReconciler::new(
            store.clone(),
            materializer.clone(),
            target,
            MaterializationReconcilerConfig {
                interval: Duration::from_millis(20),
                concurrency_limit: 2,
                batch_size: 2,
                ..Default::default()
            },
            sleepypods_observability::recorder::ObservabilityRecorder::noop(),
        )
    };
    let accept = |id: String, target: MaterializationTarget| {
        let store = store.clone();
        let class = class.reference.clone();
        let materializer = materializer.clone();
        async move {
            let cold = store
                .create_instance(create_instance_request(
                    &format!("create-{id}"),
                    &id,
                    class,
                    vec![],
                ))
                .await?
                .instance;
            StoreBackedProxyApi::new(store.clone(), materializer, target.clone())
                .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
                    instance_id: id,
                    expected_generation: cold.generation.get(),
                    backend_generation: None,
                }))
                .await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(
                store
                    .load_active_materialization(LoadActiveMaterializationRequest::new(
                        cold.id, target,
                    ))
                    .await?
                    .unwrap(),
            )
        }
    };
    let healthy = accept("healthy-delete".into(), target.clone()).await?;
    driver(target.clone()).run_once().await;
    let ready = store
        .get_instance(GetInstanceRequest::new(healthy.instance_id.clone()))
        .await?
        .unwrap();
    assert_eq!(ready.state, InstanceState::Running);
    for index in 0..8 {
        let id = format!("hanging-{index}");
        client.behavior.lock().unwrap().insert(id.clone(), "hang");
        accept(id, target.clone()).await?;
    }
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(driver(target.clone()).run_until_shutdown(receiver));
    tokio::time::timeout(Duration::from_secs(3), async {
        while client.waiting.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    store
        .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
            instance_id: ready.id.clone(),
            expected_generation: ready.generation,
        })
        .await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if store
                .get_instance(GetInstanceRequest::new(ready.id.clone()))
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    shutdown.send_replace(true);
    running.await??;
    assert_eq!(
        raw.query_one("SELECT count(*) FROM materialization_effects", &[])
            .await?
            .get::<_, i64>(0),
        0,
        "readiness cancellation is not quarantined"
    );
    assert_eq!(
        raw.query_one(
            "SELECT count(*) FROM materializations WHERE reconcile_owner IS NOT NULL",
            &[]
        )
        .await?
        .get::<_, i64>(0),
        0,
        "shutdown releases definite read-only attempts"
    );

    for behavior in ["permanent", "transient", "uncertain"] {
        let id = format!("failure-{behavior}");
        client.behavior.lock().unwrap().insert(id.clone(), behavior);
        let isolated = MaterializationTarget::new(format!("failure-{behavior}"), "apps")?;
        let pending = accept(id.clone(), isolated.clone()).await?;
        driver(isolated.clone()).run_once().await;
        if behavior == "uncertain" {
            // Model an ambiguous conditional update's stored observation for the API mapping gate.
            raw.execute("UPDATE materialization_effects SET expected_uid = 'observed-uid', expected_resource_version = 'observed-rv' WHERE materialization_id = $1", &[&pending.id.as_str()]).await?;
        }
        let status = store
            .load_materialization_work_status(pending.id.clone())
            .await?
            .unwrap();
        let diagnostics = control_plane::api::StoreBackedOperatorApi::new(
            store.clone(),
            materializer.clone(),
            isolated.clone(),
        )
        .reconcile_materialization(tonic::Request::new(pb::ReconcileMaterializationRequest {
            materialization_id: pending.id.as_str().into(),
            status_only: true,
        }))
        .await?
        .into_inner();
        assert!(diagnostics.found && !diagnostics.attempted);
        assert_eq!(diagnostics.failure_kind, behavior);
        assert_eq!(diagnostics.failure_message, status.failure_message);
        assert_eq!(
            diagnostics.operation_deadline_unix_millis,
            status.operation_deadline_unix_millis
        );
        assert_eq!(
            diagnostics.next_attempt_at_unix_millis,
            status.next_attempt_at_unix_millis
        );
        assert_eq!(diagnostics.failure_count, status.failure_count);
        if let Some(effect) = &status.uncertain_effect {
            let exposed = diagnostics.uncertain_effect.unwrap();
            assert_eq!(exposed.owner, effect.owner);
            assert_eq!(exposed.lease_attempt, effect.attempt);
            assert_eq!(exposed.instance_generation, effect.generation.get());
            assert_eq!(exposed.effect_id, effect.effect_id);
            assert_eq!(exposed.object.unwrap().name, effect.object.name);
            assert_eq!(exposed.expected_uid, "observed-uid");
            assert_eq!(exposed.expected_resource_version, "observed-rv");
        }

        assert_eq!(status.failure_kind.as_deref(), Some(behavior));
        assert_eq!(status.failure_count, 1);
        assert!(
            status.next_attempt_at_unix_millis
                > SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64
        );
        assert_eq!(status.uncertain_effect.is_some(), behavior == "uncertain");
        if behavior == "permanent" {
            assert_eq!(
                store
                    .get_instance(GetInstanceRequest::new(pending.instance_id.clone()))
                    .await?
                    .unwrap()
                    .state,
                InstanceState::Failed
            );
            let cleanup = store
                .load_materialization(LoadMaterializationRequest::new(pending.id.clone()))
                .await?
                .unwrap();
            assert_eq!(cleanup.state, MaterializationState::Deleting);
            assert_eq!(
                cleanup.rendered_objects, pending.rendered_objects,
                "terminalization retains cleanup inventory"
            );
            client.behavior.lock().unwrap().remove(&id);
            raw.execute("UPDATE materializations SET next_attempt_at_unix_millis = 0 WHERE materialization_id = $1", &[&pending.id.as_str()]).await?;
            driver(isolated.clone()).run_once().await;
            let final_status = store
                .load_materialization_work_status(pending.id.clone())
                .await?
                .unwrap();
            assert!(
                final_status.wake_failure_message.contains("forbidden"),
                "wake reason survives safe cleanup"
            );
            assert_eq!(
                store
                    .load_materialization(LoadMaterializationRequest::new(pending.id.clone()))
                    .await?
                    .unwrap()
                    .state,
                MaterializationState::Deleted
            );
            let failed = store
                .get_instance(GetInstanceRequest::new(pending.instance_id.clone()))
                .await?
                .unwrap();
            StoreBackedProxyApi::new(store.clone(), materializer.clone(), isolated)
                .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
                    instance_id: id,
                    expected_generation: failed.generation.get(),
                    backend_generation: None,
                }))
                .await?;
        } else if behavior == "transient" {
            raw.execute("UPDATE materializations SET next_attempt_at_unix_millis = 0 WHERE materialization_id = $1", &[&pending.id.as_str()]).await?;
            let claimed = store
                .claim_materialization_reconciliation(
                    ClaimMaterializationReconciliationRequest::new(
                        pending.id.clone(),
                        "expired-failure",
                        Duration::from_secs(30),
                    ),
                )
                .await?
                .unwrap();
            let revision = store.load_route_change_revision().await?;
            raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 0 WHERE materialization_id = $1", &[&pending.id.as_str()]).await?;
            assert!(
                !store
                    .record_materialization_failure(
                        control_plane::runtime_work::RecordMaterializationFailure {
                            expected_state: control_plane::MaterializationState::Pending,
                            materialization_id: pending.id.clone(),
                            owner: "expired-failure".into(),
                            attempt: claimed.reconciliation_lease.unwrap().attempt,
                            generation: pending.instance_generation,
                            permanent: true,
                            message: "late publication".into()
                        }
                    )
                    .await?
            );
            assert_eq!(
                store.load_route_change_revision().await?,
                revision,
                "lease-only updates and rejected publication emit no semantic event"
            );
        } else {
            assert!(
                !store.enqueue_materialization(pending.id.clone()).await?,
                "normal admin scheduling cannot clear uncertainty"
            );
        }
    }
    let deadline_target = MaterializationTarget::new("deadline-target", "apps")?;
    client
        .behavior
        .lock()
        .unwrap()
        .insert("failure-deadline".into(), "hang");
    let deadline = accept("failure-deadline".into(), deadline_target.clone()).await?;
    // Readiness must actually start before this test can claim that deadline
    // cancellation is read-only. A fixed one-second horizon permits setup; the
    // held row delays only failure publication until that same DB deadline.
    let (mut publication_blocker, blocker_connection) =
        tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let mut deadline_connections = tokio::task::JoinSet::new();
    deadline_connections.spawn(blocker_connection);
    let original_deadline:i64=raw.query_one("UPDATE materializations SET operation_deadline_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + 1000 WHERE materialization_id = $1 RETURNING operation_deadline_unix_millis", &[&deadline.id.as_str()]).await?.get(0);
    let waiting_before = client.waiting.load(std::sync::atomic::Ordering::SeqCst);
    let cancelled_before = client
        .readiness_cancelled
        .load(std::sync::atomic::Ordering::SeqCst);
    let deadline_driver = driver(deadline_target.clone());
    let mut deadline_jobs = tokio::task::JoinSet::new();
    deadline_jobs.spawn(async move { deadline_driver.run_once().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.waiting.load(std::sync::atomic::Ordering::SeqCst) == waiting_before {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let publication_lock = publication_blocker.transaction().await?;
    let locked_at:i64=publication_lock.query_one("SELECT (extract(epoch from clock_timestamp())*1000)::bigint FROM materializations WHERE materialization_id=$1 FOR UPDATE",&[&deadline.id.as_str()]).await?.get(0);
    assert!(
        locked_at < original_deadline,
        "readiness and publication barrier must precede the fixed deadline"
    );
    let expired_at = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let now: i64 = raw
                .query_one(
                    "SELECT (extract(epoch from clock_timestamp())*1000)::bigint",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if now >= original_deadline {
                break now;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await?;
    publication_lock.rollback().await?;
    tokio::time::timeout(Duration::from_secs(3), deadline_jobs.join_next())
        .await?
        .expect("deadline job")?;
    assert_eq!(
        client
            .readiness_cancelled
            .load(std::sync::atomic::Ordering::SeqCst),
        cancelled_before + 1,
        "the admitted readiness read is cancelled before terminal publication completes"
    );
    deadline_connections.abort_all();
    while deadline_connections.join_next().await.is_some() {}
    eprintln!("causal readiness deadline: locked_at={locked_at} original_deadline={original_deadline} publication_released_at={expired_at}");
    let diagnostics = control_plane::api::StoreBackedOperatorApi::new(
        store.clone(),
        materializer.clone(),
        deadline_target,
    )
    .reconcile_materialization(tonic::Request::new(pb::ReconcileMaterializationRequest {
        materialization_id: deadline.id.as_str().into(),
        status_only: true,
    }))
    .await?
    .into_inner();
    assert_eq!(diagnostics.failure_kind, "deadline");
    assert!(!diagnostics.attempted);
    assert!(
        diagnostics.uncertain_effect.is_none(),
        "deadline during readiness is a definite read-only cancellation"
    );
    assert!(!diagnostics.wake_failure_message.is_empty());
    assert_eq!(
        store
            .get_instance(GetInstanceRequest::new(deadline.instance_id))
            .await?
            .unwrap()
            .state,
        InstanceState::Failed
    );
    raw_task.abort();
    eprintln!("phase6 continuous driver advances deletion through >batch hanging wakes; classified failures and retry-after-cleanup passed");
    Ok(())
}

// A Delete accepted during Pending must supersede that exact attempt's reads and
// failure publication. Exercise the production wrapper, APIs and continuous driver;
// no second lifecycle RPC or direct mutation is used to finish deletion.
#[tokio::test]
async fn postgres_runtime_delete_supersedes_pending_attempt() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let mut connections = tokio::task::JoinSet::new();
    connections.spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let url = connection_url_with_search_path(&base, &schema);
    let separator = if url.contains('?') { '&' } else { '?' };
    let config =
        PostgresStoreConfig::new(format!("{url}{separator}application_name=6l-supersession"))?;
    let result = async {
        let pg = PostgresStore::connect(&config).await?;
        let store = Arc::new(RetryingControlPlaneStore::with_default_policy(Arc::new(pg)));
        let (raw, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        connections.spawn(connection);
        let mut failures = Vec::new();
        for behavior in ["hang", "apply-permanent", "apply-transient", "apply-hang"] {
            if let Err(error) = delete_supersedes_pending_case(store.clone(), &raw, behavior).await
            {
                failures.push(format!("{behavior}: {error}"));
            }
        }
        if let Err(error) = deletion_and_failure_use_one_lock_order(store.clone(), &config).await {
            failures.push(format!("lock-order: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("\n").into())
        }
    }
    .await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    connections.shutdown().await;
    result
}

async fn delete_supersedes_pending_case(
    store: Arc<RetryingControlPlaneStore>,
    raw: &tokio_postgres::Client,
    behavior: &'static str,
) -> TestResult {
    use control_plane::{
        api::{pb, StoreBackedOperatorApi, StoreBackedProxyApi},
        KubernetesMaterializer, MaterializationReconciler, MaterializationReconcilerConfig,
    };
    use pb::{
        operator_control_plane_server::OperatorControlPlane,
        proxy_control_plane_server::ProxyControlPlane,
    };
    let id = format!("supersede-{behavior}");
    let class = workload_class(&id, 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let cold = store
        .create_instance(create_instance_request(&id, &id, class.reference, vec![]))
        .await?
        .instance;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let definite_apply = matches!(behavior, "apply-permanent" | "apply-transient");
    let cleanup_gate = definite_apply.then(|| {
        Arc::new(SupersessionCleanupGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        })
    });
    let client = SchedulerKubernetes {
        operation_gate: Some(gate.clone()),
        cleanup_gate: cleanup_gate.clone(),
        ..Default::default()
    };
    client.behavior.lock().unwrap().insert(id.clone(), behavior);
    let target = MaterializationTarget::new(&id, "apps")?;
    let materializer = KubernetesMaterializer::new(client.clone());
    StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone())
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            instance_id: id.clone(),
            expected_generation: cold.generation.get(),
            backend_generation: None,
        }))
        .await?;
    let pending = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            cold.id.clone(),
            target.clone(),
        ))
        .await?
        .unwrap();
    let driver = MaterializationReconciler::new(
        store.clone(),
        materializer.clone(),
        target.clone(),
        MaterializationReconcilerConfig {
            owner: format!("owner-{behavior}"),
            interval: Duration::from_millis(10),
            // Short read-preemption heartbeat; definite apply errors finish before
            // the first heartbeat so failure publication must independently be fenced.
            lease_ttl: if matches!(behavior, "hang" | "apply-hang") {
                Duration::from_millis(120)
            } else {
                Duration::from_secs(30)
            },
            concurrency_limit: 2,
            batch_size: 2,
        },
        sleepypods_observability::recorder::ObservabilityRecorder::noop(),
    );
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut jobs = tokio::task::JoinSet::new();
    let driver_started = tokio::time::Instant::now();
    jobs.spawn(driver.run_until_shutdown(receiver));
    let result: TestResult = async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while client.waiting.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|error| format!("waiting for {behavior} callback: {error}"))?;
        let before = supersession_snapshot(raw, pending.id.as_str()).await?;
        let waking = store
            .get_instance(GetInstanceRequest::new(cold.id.clone()))
            .await?
            .unwrap();
        if waking.state != InstanceState::Waking {
            return Err(format!("expected Waking, got {waking:?}").into());
        }
        // This is an observation budget, not a lifecycle policy. Definite errors
        // must hand off before the first 10s heartbeat and the old 30s lease, but
        // real database/cleanup I/O is not required to finish within one second.
        let completion_budget = if definite_apply {
            Duration::from_secs(5)
        } else {
            Duration::from_secs(1)
        };
        let delete_started = tokio::time::Instant::now();
        let completion_deadline = delete_started + completion_budget;
        let accept = async {
            let accepted = StoreBackedOperatorApi::new(store.clone(), materializer, target)
                .delete_instance(tonic::Request::new(pb::DeleteInstanceRequest {
                    instance_id: id.clone(),
                    expected_generation: Some(waking.generation.get()),
                }))
                .await?
                .into_inner();
            if !accepted.accepted {
                return Err("Delete was not accepted".into());
            }
            supersession_snapshot(raw, pending.id.as_str()).await
        };
        let after_accept = if definite_apply {
            tokio::time::timeout_at(completion_deadline, accept).await??
        } else {
            accept.await?
        };
        let finished = tokio::time::timeout_at(
            if definite_apply {
                completion_deadline
            } else {
                // Preserve the original short-read and uncertain-effect cases.
                tokio::time::Instant::now() + completion_budget
            },
            async {
                if behavior != "apply-hang" {
                    gate.add_permits(1);
                }
                if let Some(cleanup_gate) = &cleanup_gate {
                    cleanup_gate.entered.notified().await;
                    let handoff = supersession_snapshot(raw, pending.id.as_str()).await?;
                    let old: serde_json::Value = serde_json::from_str(&before)?;
                    let accepted: serde_json::Value = serde_json::from_str(&after_accept)?;
                    let current: serde_json::Value = serde_json::from_str(&handoff)?;
                    // The real Deleting attempt is held before finalization, so
                    // absence cannot erase a stale failure or lease handoff bug.
                    if old["attempt"] != 1
                        || current["attempt"] != 2
                        || current["state"] != "deleting"
                        || current["instance_state"] != "deleting"
                        || current["instance_generation"] != 2
                        || current["materialization_generation"] != 1
                        || current["owner"] != old["owner"]
                        || current["failure_count"] != 0
                        || !current["failure_kind"].is_null()
                        || current["failure_requires_cleanup"] != false
                        || current["effect_count"] != 0
                        || current["operation_deadline"] != accepted["operation_deadline"]
                    {
                        return Err(format!("invalid supersession handoff: {handoff}").into());
                    }
                    let old_lease_expiry = old["lease_expires"]
                        .as_i64()
                        .ok_or("old attempt has no lease expiry")?;
                    let handoff_at = current["now"]
                        .as_i64()
                        .ok_or("handoff has no database timestamp")?;
                    // A later lease snapshot may already reflect renewal. The
                    // driver's start is a conservative lower bound for its first
                    // heartbeat, independent of any intervening database delay.
                    if tokio::time::Instant::now() >= driver_started + Duration::from_secs(10)
                        || handoff_at >= old_lease_expiry - 20_000
                    {
                        return Err("definite error did not settle before its first heartbeat".into());
                    }
                    eprintln!("6L definite handoff {behavior}: old={before}; current={handoff}; elapsed={:?}", delete_started.elapsed());
                    if behavior == "apply-transient" {
                        // Controlled valid descendant-inspection latency proves
                        // a one-second absence deadline conflates safe handoff
                        // with I/O completion. This does not emulate WAL timing.
                        tokio::time::sleep_until(delete_started + Duration::from_millis(1_100)).await;
                        let held = store
                            .get_instance(GetInstanceRequest::new(cold.id.clone()))
                            .await?
                            .ok_or("instance finalized while cleanup inspection was held")?;
                        if held.state != InstanceState::Deleting || held.generation != Generation::new(2) {
                            return Err("deletion identity changed while cleanup inspection was held".into());
                        }
                    }
                    cleanup_gate.release.add_permits(1);
                }
                loop {
                    if store
                        .get_instance(GetInstanceRequest::new(cold.id.clone()))
                        .await?
                        .is_none()
                    {
                        return Ok::<_, Box<dyn Error + Send + Sync>>(());
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            },
        )
        .await;
        let after = supersession_snapshot(raw, pending.id.as_str()).await?;
        eprintln!(
            "6L supersession {behavior}: before={before}; accepted={after_accept}; after={after}"
        );
        if behavior == "apply-hang" {
            let durable = store
                .load_materialization_work_status(pending.id.clone())
                .await?
                .unwrap();
            if durable.failure_count != 0 || durable.failure_kind.is_some() {
                return Err("stale durable failure was published".into());
            }
            if finished.is_ok() {
                return Err("uncertain dispatched work was finalized".into());
            }
            let status = StoreBackedOperatorApi::new(
                store.clone(),
                KubernetesMaterializer::new(client.clone()),
                pending.target.clone(),
            )
            .reconcile_materialization(tonic::Request::new(pb::ReconcileMaterializationRequest {
                materialization_id: pending.id.as_str().into(),
                status_only: true,
            }))
            .await?
            .into_inner();
            let effect = status
                .uncertain_effect
                .as_ref()
                .ok_or("missing durable uncertain-effect diagnostics")?;
            if status.failure_count != 0
                || status.failure_kind != "uncertain"
                || effect.lease_attempt != 1
            {
                return Err(format!(
                    "stale failure was published or effect identity changed: {status:?}"
                )
                .into());
            }
            if store
                .release_materialization_reconciliation_lease(
                    ReleaseMaterializationReconciliationLeaseRequest::new(
                        pending.id.clone(),
                        format!("owner-{behavior}"),
                        1,
                        pending.instance_generation,
                    ),
                )
                .await?
            {
                return Err("released uncertain dispatched attempt".into());
            }
            if store
                .claim_materialization_reconciliation(
                    ClaimMaterializationReconciliationRequest::new(
                        pending.id.clone(),
                        "replacement",
            Duration::from_secs(30),
        ),
                )
                .await?
                .is_some()
            {
                return Err("claimed uncertain attempt after its short lease expired".into());
            }
            if store.finalize_instance_deletions(10).await? != 0 {
                return Err("finalized instance with uncertain effect".into());
            }
            return Ok(());
        }
        match finished {
            Ok(result) => result?,
            Err(_) => {
                return Err(format!("accepted Delete did not finish within {completion_budget:?}; {after}").into())
            }
        }
        if !client.inner.objects.lock().unwrap().is_empty() {
            return Err("Delete completed while managed objects remain".into());
        }
        Ok(())
    }
    .await;
    // Every failure, including the expected red result, owns cooperative shutdown
    // and joins the driver before schema removal. JoinSet also owns caller abort.
    if let Err(error) = &result {
        eprintln!(
            "6L early {behavior} error={error}; state={}",
            supersession_snapshot(raw, pending.id.as_str())
                .await
                .unwrap_or_else(|error| format!("diagnostic failed: {error}"))
        );
    }
    if result.is_err() {
        let activity = raw.query_one("SELECT COALESCE(json_agg(x), '[]'::json)::text FROM (SELECT pid,state,wait_event_type,wait_event,pg_blocking_pids(pid) AS blockers,left(query,600) AS query FROM pg_stat_activity WHERE application_name='6l-supersession' AND pid<>pg_backend_pid() LIMIT 16) x", &[]).await.map(|row| row.get::<_,String>(0));
        let locks = raw.query_one("SELECT COALESCE(json_agg(x), '[]'::json)::text FROM (SELECT l.pid,l.locktype,l.mode,l.granted,l.relation::regclass::text AS relation,l.transactionid FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.application_name='6l-supersession' AND a.pid<>pg_backend_pid() LIMIT 64) x", &[]).await.map(|row| row.get::<_,String>(0));
        eprintln!("6L diagnostic {behavior}: applies={} readiness={} activity={activity:?} locks={locks:?}", client.inner.applies.load(std::sync::atomic::Ordering::SeqCst), client.waiting.load(std::sync::atomic::Ordering::SeqCst));
    }
    shutdown.send_replace(true);
    let drained = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(joined) = jobs.join_next().await {
            joined??;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await;
    match drained {
        Ok(result) => result?,
        Err(error) => {
            jobs.shutdown().await;
            return Err(format!("draining {behavior} driver: {error}").into());
        }
    }
    result
}

async fn supersession_snapshot(raw: &tokio_postgres::Client, id: &str) -> TestResult<String> {
    Ok(raw.query_opt("SELECT json_build_object('state', m.state, 'instance_state', i.state, 'instance_generation', i.generation, 'materialization_generation', m.instance_generation, 'now', (extract(epoch from clock_timestamp()) * 1000)::bigint, 'operation_deadline', m.operation_deadline_unix_millis, 'owner', m.reconcile_owner, 'attempt', m.reconcile_attempt, 'lease_expires', m.reconcile_lease_expires_at_unix_millis, 'failure_kind', m.failure_kind, 'failure_count', m.failure_count, 'failure_requires_cleanup', m.failure_requires_cleanup, 'effect_count', (SELECT count(*) FROM materialization_effects e WHERE e.materialization_id=m.materialization_id))::text FROM materializations m JOIN instances i USING(instance_id) WHERE materialization_id=$1", &[&id]).await?.map(|row| row.get::<_, String>(0)).unwrap_or_else(|| "absent".into()))
}

async fn deletion_and_failure_use_one_lock_order(
    store: Arc<RetryingControlPlaneStore>,
    config: &PostgresStoreConfig,
) -> TestResult {
    use control_plane::{
        api::{pb, StoreBackedOperatorApi, StoreBackedProxyApi},
        KubernetesMaterializer,
    };
    use pb::{
        operator_control_plane_server::OperatorControlPlane,
        proxy_control_plane_server::ProxyControlPlane,
    };
    let id = "supersede-lock-order";
    let class = workload_class(id, 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let cold = store
        .create_instance(create_instance_request(id, id, class.reference, vec![]))
        .await?
        .instance;
    let target = MaterializationTarget::new(id, "apps")?;
    let materializer = KubernetesMaterializer::new(SchedulerKubernetes::default());
    StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone())
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            instance_id: id.into(),
            expected_generation: cold.generation.get(),
            backend_generation: None,
        }))
        .await?;
    let pending = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            cold.id.clone(),
            target.clone(),
        ))
        .await?
        .unwrap();
    let claimed = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending.id.clone(),
            "lock-order-owner",
            Duration::from_secs(30),
        ))
        .await?
        .unwrap();
    let separator = if config.connection_url().contains('?') {
        '&'
    } else {
        '?'
    };
    let delete_store = Arc::new(RetryingControlPlaneStore::with_default_policy(Arc::new(
        PostgresStore::connect(&PostgresStoreConfig::new(format!(
            "{}{separator}application_name=6l-delete-order",
            config.connection_url()
        ))?)
        .await?,
    )));
    let failure_store = Arc::new(RetryingControlPlaneStore::with_default_policy(Arc::new(
        PostgresStore::connect(&PostgresStoreConfig::new(format!(
            "{}{separator}application_name=6l-failure-order",
            config.connection_url()
        ))?)
        .await?,
    )));
    let (mut blocker, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let mut connections = tokio::task::JoinSet::new();
    connections.spawn(connection);
    let (observer, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    connections.spawn(connection);
    let transaction = blocker.transaction().await?;
    transaction
        .query_one(
            "SELECT instance_id FROM instances WHERE instance_id=$1 FOR UPDATE",
            &[&id],
        )
        .await?;
    let mut jobs = tokio::task::JoinSet::new();
    jobs.spawn(async move {
        let accepted = StoreBackedOperatorApi::new(delete_store, materializer, target)
            .delete_instance(tonic::Request::new(pb::DeleteInstanceRequest {
                instance_id: id.into(),
                expected_generation: Some(1),
            }))
            .await?
            .into_inner()
            .accepted;
        Ok::<_, Box<dyn Error + Send + Sync>>(("delete", accepted))
    });
    wait_for_named_database_lock(&observer, "6l-delete-order").await?;
    jobs.spawn(async move {
        let published = failure_store
            .record_materialization_failure(
                control_plane::runtime_work::RecordMaterializationFailure {
                    expected_state: MaterializationState::Pending,
                    materialization_id: claimed.id,
                    owner: "lock-order-owner".into(),
                    attempt: claimed.reconciliation_lease.unwrap().attempt,
                    generation: claimed.instance_generation,
                    permanent: true,
                    message: "old Pending permanent failure".into(),
                },
            )
            .await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(("failure", published))
    });
    wait_for_named_database_lock(&observer, "6l-failure-order").await?;
    // Both operations queue on the same instance first. A stale failure must not
    // hold the materialization while waiting for our instance lock (deadlock).
    transaction.query_one("SELECT materialization_id FROM materializations WHERE materialization_id=$1 FOR UPDATE NOWAIT", &[&pending.id.as_str()]).await?;
    transaction.commit().await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(joined) = jobs.join_next().await {
            let (operation, changed) = joined??;
            if changed != (operation == "delete") {
                return Err(format!("unexpected {operation} result: {changed}").into());
            }
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    let status = store
        .load_materialization_work_status(pending.id.clone())
        .await?
        .unwrap();
    if status.failure_count != 0 || status.failure_kind.is_some() {
        return Err("queued stale failure poisoned accepted deletion".into());
    }
    if store
        .renew_materialization_reconciliation_lease(
            RenewMaterializationReconciliationLeaseRequest::new(
                pending.id.clone(),
                "lock-order-owner",
                1,
                pending.instance_generation,
                Duration::from_secs(30),
                MaterializationState::Pending,
            ),
        )
        .await?
    {
        return Err("queued stale Pending renewal succeeded".into());
    }
    if !store
        .release_materialization_reconciliation_lease(
            ReleaseMaterializationReconciliationLeaseRequest::new(
                pending.id.clone(),
                "lock-order-owner",
                1,
                pending.instance_generation,
            ),
        )
        .await?
    {
        return Err("exact no-effect superseded lease did not release".into());
    }
    eprintln!("6L deletion/failure instance-first lock ordering: delete accepted, stale failure/renew rejected, exact release succeeded");
    connections.shutdown().await;
    Ok(())
}

async fn wait_for_named_database_lock(client: &tokio_postgres::Client, name: &str) -> TestResult {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", &[&name]).await?.get::<_, bool>(0) { return Ok::<_, tokio_postgres::Error>(()); }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await??;
    Ok(())
}

#[tokio::test]
async fn postgres_failure_deadline_classification_uses_post_lock_database_time() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let mut connections = tokio::task::JoinSet::new();
    connections.spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
    let store = PostgresStore::connect(&config).await?;
    let mut owned = tokio::task::JoinSet::new();
    owned.spawn(async move { failure_deadline_lock_boundary(store, config).await });
    let result = tokio::time::timeout(Duration::from_secs(10), owned.join_next()).await;
    owned.abort_all();
    while owned.join_next().await.is_some() {}
    let cleanup = admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    drop(admin);
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    cleanup?;
    result?.expect("owned deadline regression")?
}

async fn failure_deadline_lock_boundary(
    store: PostgresStore,
    config: PostgresStoreConfig,
) -> TestResult {
    use control_plane::{
        api::{pb, StoreBackedProxyApi},
        KubernetesMaterializer,
    };
    use pb::proxy_control_plane_server::ProxyControlPlane;
    let class = workload_class("lock-deadline", 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let cold = store
        .create_instance(create_instance_request(
            "lock-deadline",
            "lock-deadline",
            class.reference,
            vec![],
        ))
        .await?
        .instance;
    let target = MaterializationTarget::new("lock-deadline", "apps")?;
    let store = Arc::new(store);
    StoreBackedProxyApi::new(
        store.clone(),
        KubernetesMaterializer::new(SchedulerKubernetes::default()),
        target.clone(),
    )
    .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
        instance_id: cold.id.as_str().into(),
        expected_generation: cold.generation.get(),
        backend_generation: None,
    }))
    .await?;
    let pending = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(cold.id, target))
        .await?
        .unwrap();
    let claimed = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending.id.clone(),
            "deadline-lock-owner",
            Duration::from_secs(30),
        ))
        .await?
        .unwrap();
    let (raw, raw_conn) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let (mut blocker, blocker_conn) =
        tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let mut connections = tokio::task::JoinSet::new();
    connections.spawn(raw_conn);
    connections.spawn(blocker_conn);
    let original_deadline:i64=raw.query_one("UPDATE materializations SET operation_deadline_unix_millis=(extract(epoch from clock_timestamp())*1000)::bigint+250 WHERE materialization_id=$1 RETURNING operation_deadline_unix_millis",&[&pending.id.as_str()]).await?.get(0);
    let initial_status = store
        .load_materialization_work_status(pending.id.clone())
        .await?
        .unwrap();
    assert_eq!(
        initial_status.operation_deadline_unix_millis,
        original_deadline
    );
    assert!(initial_status.operation_remaining.is_some_and(
        |remaining| remaining > Duration::ZERO && remaining <= Duration::from_millis(250)
    ));
    let blocker_pid: i32 = blocker
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    let transaction = blocker.transaction().await?;
    transaction.query_one("SELECT materialization_id FROM materializations WHERE materialization_id=$1 FOR UPDATE",&[&pending.id.as_str()]).await?;
    let writer = store.clone();
    let id = pending.id.clone();
    let attempt = claimed.reconciliation_lease.unwrap().attempt;
    let mut writers = tokio::task::JoinSet::new();
    writers.spawn(async move {
        writer
            .record_materialization_failure(
                control_plane::runtime_work::RecordMaterializationFailure {
                    expected_state: MaterializationState::Pending,
                    materialization_id: id,
                    owner: "deadline-lock-owner".into(),
                    attempt,
                    generation: pending.instance_generation,
                    permanent: false,
                    message: "controlled transient failure waits across deadline".into(),
                },
            )
            .await
    });
    let queued_at=tokio::time::timeout(Duration::from_secs(2),async {
        loop {
            let row=raw.query_one("SELECT (extract(epoch from clock_timestamp())*1000)::bigint, EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",&[&blocker_pid]).await.unwrap();
            if row.get::<_,bool>(1) {break row.get::<_,i64>(0);}
            tokio::task::yield_now().await;
        }
    }).await?;
    assert!(
        queued_at < original_deadline,
        "failure SELECT must begin before original deadline"
    );
    let released_at = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let now: i64 = raw
                .query_one(
                    "SELECT (extract(epoch from clock_timestamp())*1000)::bigint",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if now > original_deadline {
                break now;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await?;
    let expired_status = store
        .load_materialization_work_status(pending.id.clone())
        .await?
        .unwrap();
    assert_eq!(
        expired_status.operation_deadline_unix_millis,
        original_deadline
    );
    assert_eq!(
        expired_status.operation_remaining,
        Some(Duration::ZERO),
        "expired is explicit zero, never the missing-budget fallback"
    );
    transaction.rollback().await?;
    assert!(writers.join_next().await.unwrap()??);
    let status = store
        .load_materialization_work_status(pending.id.clone())
        .await?
        .unwrap();
    eprintln!("deadline lock boundary: queued_at={queued_at} original_deadline={original_deadline} released_at={released_at} persisted_kind={:?} persisted_message={:?}",status.failure_kind,status.failure_message);
    assert_eq!(status.failure_kind.as_deref(), Some("deadline"));
    assert!(status.uncertain_effect.is_none());
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}
