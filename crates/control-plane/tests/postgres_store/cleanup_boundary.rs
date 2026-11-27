//! Synthetic late-descendant disappearance, with real time, PostgreSQL, and
//! the production scheduler. This does not reproduce an uncaptured kind failure.
use super::*;
use control_plane::{
    api::{pb, StoreBackedProxyApi},
    instance::RequestInstanceDeletion,
    KubernetesClientError, KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
    KubernetesMaterializerClient, KubernetesObject, MaterializationReconciler,
    MaterializationReconcilerConfig, RetryingControlPlaneStore,
};
use pb::proxy_control_plane_server::ProxyControlPlane;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

const DESCENDANT: &str = "controlled owned terminating Pod remains";

#[derive(Clone, Default)]
struct TerminatingDescendant {
    inner: LifecycleKubernetes,
    present: Arc<AtomicBool>,
    observations: Arc<Mutex<Vec<(u64, bool)>>>,
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl KubernetesMaterializerClient for TerminatingDescendant {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.inner.apply_object(object, precondition)
    }
    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.inner.delete_object(object, precondition)
    }
    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.inner.wait_for_pvc_bound(namespace, name)
    }
    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        self.inner.wait_for_readiness(objects)
    }
    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<
        'a,
        KubernetesClientResult<control_plane::projection::ProjectionObjectInspection>,
    > {
        self.inner.inspect_object(object)
    }
    fn ensure_no_descendants<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        _instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let present = self.present.load(Ordering::SeqCst);
            let now = unix_millis();
            self.observations.lock().unwrap().push((now, present));
            eprintln!(
                "CLEANUP_OBSERVATION {}",
                serde_json::json!({"at":now,"descendant_present":present})
            );
            if present {
                Err(KubernetesClientError::transient(DESCENDANT))
            } else {
                Ok(())
            }
        })
    }
    fn verify_retained_bindings<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.inner.verify_retained_bindings(objects)
    }
}

fn require(condition: bool, explanation: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(explanation.into())
    }
}

async fn snapshot(raw: &tokio_postgres::Client) -> TestResult<serde_json::Value> {
    Ok(raw.query_one("SELECT jsonb_build_object(
      'now', (extract(epoch from clock_timestamp()) * 1000)::bigint,
      'instance_state', (SELECT state FROM instances WHERE instance_id = m.instance_id),
      'instance_generation', (SELECT generation FROM instances WHERE instance_id = m.instance_id),
      'state', state, 'generation', instance_generation,
      'started', state_entered_at_unix_millis, 'deadline', operation_deadline_unix_millis,
      'next_attempt', next_attempt_at_unix_millis,
      'failure_count', failure_count, 'failure_kind', failure_kind,
      'failure_message', failure_message, 'cleanup_required', failure_requires_cleanup,
      'owner', reconcile_owner, 'lease', reconcile_lease_expires_at_unix_millis,
      'effects', (SELECT count(*) FROM materialization_effects e WHERE e.materialization_id = m.materialization_id))
      FROM materializations m WHERE instance_id = 'cleanup-boundary'", &[]).await?.get(0))
}

#[tokio::test]
async fn healthy_cleanup_retry_can_outlast_sixty_second_observation() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping real-time cleanup boundary: SLEEPYPODS_POSTGRES_URL is required");
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result = async {
        let mut config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
        config.operation_timeout = Duration::from_secs(90);
        let pg = PostgresStore::connect(&config).await?;
        pg.run_migrations().await?;
        let (raw, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let raw_task = tokio::spawn(connection);
        let result = exercise(pg, &raw).await;
        drop(raw);
        raw_task.abort();
        let _ = raw_task.await;
        result
    }
    .await;
    let cleanup = admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    drop(admin);
    admin_task.abort();
    let _ = admin_task.await;
    cleanup?;
    result
}

async fn exercise(pg: PostgresStore, raw: &tokio_postgres::Client) -> TestResult {
    let class = workload_class("cleanup-boundary", 1);
    pg.create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let store = Arc::new(RetryingControlPlaneStore::with_default_policy(Arc::new(pg)));
    let cold = store
        .create_instance(create_instance_request(
            "cleanup-boundary",
            class.reference,
            vec![],
        ))
        .await?
        .instance;
    let target = MaterializationTarget::new("cleanup-boundary", "apps")?;
    let client = TerminatingDescendant::default();
    let materializer = KubernetesMaterializer::new(client.clone());
    let accepted = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone())
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            instance_id: cold.id.as_str().to_owned(),
            expected_generation: cold.generation.get(),
            backend_generation: None,
        }))
        .await?
        .into_inner();
    require(
        matches!(
            accepted.outcome,
            Some(pb::proxy_wake_instance_response::Outcome::StillWaking(_))
        ),
        "initial wake was not accepted",
    )?;
    let driver = MaterializationReconciler::new(
        store.clone(),
        materializer,
        target,
        MaterializationReconcilerConfig::default(),
        sleepypods_observability::recorder::ObservabilityRecorder::noop(),
    );
    driver.run_once().await;
    let ready = store
        .get_instance(GetInstanceRequest::new(cold.id))
        .await?
        .ok_or("initial instance missing")?;
    require(
        ready.state == InstanceState::Running && ready.generation.get() == 2,
        "initial wake did not become exact Running2",
    )?;
    client.present.store(true, Ordering::SeqCst);
    let delete_started = tokio::time::Instant::now();
    let delete_sent_at = unix_millis();
    // Exactly one deletion request. No enqueue, retry override, or clock edits.
    store
        .request_instance_deletion(RequestInstanceDeletion {
            instance_id: ready.id.clone(),
            expected_generation: ready.generation,
        })
        .await?;
    let initial = snapshot(raw).await?;
    eprintln!(
        "CLEANUP_DELETE {}",
        serde_json::json!({"sent_at": delete_sent_at, "accepted":initial})
    );
    let deadline = initial["deadline"]
        .as_u64()
        .ok_or("missing operation deadline")?;
    let started = initial["started"].as_u64().ok_or("missing start")?;
    require(
        deadline - started == 90_000,
        "delete did not persist the unchanged 90s operation budget",
    )?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut running = tokio::spawn(driver.run_until_shutdown(receiver));
    let result = tokio::time::timeout(Duration::from_secs(95), async {
        let mut seen_count = 0;
        let fifth = tokio::time::timeout(Duration::from_secs(50), async {
            loop {
                let state = snapshot(raw).await?;
                let count = state["failure_count"].as_u64().ok_or("missing failure count")?;
                if count != seen_count {
                    eprintln!("CLEANUP_PERSISTED {state}");
                    seen_count = count;
                }
                require(state["state"] == "deleting", "cleanup unexpectedly became terminal before fifth failure")?;
                require(state["failure_kind"].is_null() || state["failure_kind"] == "transient", "cleanup became permanent, uncertain, or expired")?;
                if count == 5 && state["owner"].is_null() { break Ok::<_, Box<dyn Error + Send + Sync>>(state); }
                require(count < 6, "missed fifth cleanup failure")?;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await??;
        require(fifth["failure_message"].as_str().is_some_and(|message| message.contains(DESCENDANT)), "fifth failure was not the controlled descendant inspection")?;
        require(fifth["effects"] == 0 && fifth["cleanup_required"] == false, "fifth failure left uncertainty or failed-wake cleanup")?;
        let next_attempt = fifth["next_attempt"].as_u64().ok_or("missing next retry")?;
        require(next_attempt > delete_sent_at + 60_000 && next_attempt < deadline, "real next retry does not straddle the 60s observation before the operation deadline")?;
        let previous_observation = *client.observations.lock().unwrap().last().ok_or("missing cleanup observation")?;
        require(previous_observation.1, "last observation did not see the descendant")?;
        client.present.store(false, Ordering::SeqCst);
        let absent_at = unix_millis();
        eprintln!("CLEANUP_ABSENCE {}", serde_json::json!({"previous_observation":previous_observation.0,"absent_at":absent_at,"next_attempt":next_attempt,"original_deadline":deadline}));
        require(previous_observation.0 <= absent_at && absent_at < next_attempt, "absence did not occur between observations")?;
        tokio::time::sleep_until(delete_started + Duration::from_secs(60)).await;
        let at_sixty = snapshot(raw).await?;
        eprintln!("CLEANUP_AT_SIXTY {at_sixty}");
        require(at_sixty["state"] == "deleting" && at_sixty["instance_state"] == "deleting" && at_sixty["instance_generation"] == 3 && at_sixty["generation"] == 2, "original 60s observation should still see exact Deleting3")?;
        require(at_sixty["deadline"] == deadline && at_sixty["next_attempt"] == next_attempt, "operation deadline or retry was changed")?;
        require(at_sixty["now"].as_u64().is_some_and(|now| now < next_attempt), "next cleanup retry is not still in the future at 60s")?;
        require(at_sixty["failure_count"] == 5 && at_sixty["failure_kind"] == "transient" && at_sixty["effects"] == 0 && at_sixty["owner"].is_null() && at_sixty["cleanup_required"] == false, "60s wait is not healthy bounded pending work")?;
        require(!client.present.load(Ordering::SeqCst), "descendant reappeared")?;
        loop {
            if store.get_instance(GetInstanceRequest::new(ready.id.clone())).await?.is_none() { break; }
            require(unix_millis() < deadline, "scheduler did not finish within the original operation deadline")?;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let final_state: serde_json::Value = raw.query_one("SELECT jsonb_build_object(
            'now', (extract(epoch from clock_timestamp()) * 1000)::bigint,
            'instances', (SELECT count(*) FROM instances WHERE instance_id = 'cleanup-boundary'),
            'materializations', (SELECT count(*) FROM materializations WHERE instance_id = 'cleanup-boundary'),
            'effects', (SELECT count(*) FROM materialization_effects))", &[]).await?.get(0);
        eprintln!("CLEANUP_COMPLETE {final_state}");
        require(final_state["instances"] == 0 && final_state["materializations"] == 0 && final_state["effects"] == 0, "cleanup did not finalize exact instance/materialization/effect absence")?;
        require(unix_millis() < deadline, "final deletion exceeded original operation deadline")?;
        let observations = client.observations.lock().unwrap();
        require(observations.len() == 6 && observations[..5].iter().all(|(_, present)| *present) && !observations[5].1, "actual scheduler did not observe five held and one absent descendant")?;
        require(observations[5].0 >= next_attempt, "cleanup ran before persisted retry eligibility")?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    }).await;
    shutdown.send_replace(true);
    let joined = match tokio::time::timeout(Duration::from_secs(25), &mut running).await {
        Ok(result) => result
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
            .and_then(|result| {
                result.map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
            }),
        Err(error) => {
            running.abort();
            let _ = running.await;
            Err(Box::new(error) as Box<dyn Error + Send + Sync>)
        }
    };
    joined?;
    result?
}
