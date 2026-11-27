//! Cleanup while a deleted workload's Pod is still stopping, with real time,
//! PostgreSQL and the production scheduler.
use super::*;
use control_plane::{
    api::{pb, StoreBackedProxyApi},
    instance::RequestInstanceDeletion,
    KubernetesClientError, KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
    KubernetesMaterializerClient, KubernetesObject, MaterializationReconciler,
    RetryingControlPlaneStore,
};
use pb::proxy_control_plane_server::ProxyControlPlane;
use std::sync::{Arc, Mutex};

const DESCENDANT: &str = "controlled owned terminating Pod remains";
/// A Pod that stops well inside one cleanup attempt's wait for absence.
const QUICK_STOP: Duration = Duration::from_secs(3);
/// A Pod that outlives one attempt's wait and the operation deadline.
const SLOW_STOP: Duration = Duration::from_secs(62);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Reports each instance's Pod as terminating until its stop time.
#[derive(Clone, Default)]
struct TerminatingDescendant {
    inner: LifecycleKubernetes,
    stops: Arc<Mutex<BTreeMap<String, tokio::time::Instant>>>,
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
        instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let stopping = self
                .stops
                .lock()
                .unwrap()
                .get(instance_id)
                .is_some_and(|stop| tokio::time::Instant::now() < *stop);
            if stopping {
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

/// The cleanup row of one instance while its cleanup is unfinished.
async fn cleanup_row(
    raw: &tokio_postgres::Client,
    instance_id: &str,
) -> TestResult<Option<serde_json::Value>> {
    Ok(raw
        .query_opt(
            "SELECT jsonb_build_object(
                'state', state, 'failure_count', failure_count, 'failure_kind', failure_kind,
                'failure_message', failure_message,
                'past_deadline', operation_deadline_unix_millis
                    <= (extract(epoch from clock_timestamp()) * 1000)::bigint)
             FROM materializations WHERE instance_id = $1 AND state <> 'deleted'",
            &[&instance_id],
        )
        .await?
        .map(|row| row.get(0)))
}

#[tokio::test]
async fn cleanup_waits_for_a_stopping_pod_and_retries_past_its_deadline() -> TestResult {
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
        config.operation_timeout = OPERATION_TIMEOUT;
        let pg = PostgresStore::connect(&config).await?;
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
    let target = MaterializationTarget::new("cleanup-boundary", "apps")?;
    let client = TerminatingDescendant::default();
    let materializer = KubernetesMaterializer::new(client.clone());
    let driver = || {
        MaterializationReconciler::new(
            store.clone(),
            materializer.clone(),
            target.clone(),
            transport_support::reconciler_config(),
            sleepypods_observability::recorder::ObservabilityRecorder::noop(),
        )
    };
    let instances = ["cleanup-quick", "cleanup-slow"];
    let api = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone());
    for id in instances {
        let cold = store
            .create_instance(create_instance_request(id, class.reference.clone(), vec![]))
            .await?
            .instance;
        api.wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            instance_id: id.to_owned(),
            expected_generation: cold.generation.get(),
            backend_generation: None,
        }))
        .await?;
    }
    let running = |id: &'static str| {
        let store = store.clone();
        async move {
            store
                .get_instance(GetInstanceRequest::new(InstanceId::new(id).unwrap()))
                .await
                .ok()
                .flatten()
                .filter(|instance| instance.state == InstanceState::Running)
        }
    };
    transport_support::reconcile_until(driver(), || async {
        running(instances[0]).await.is_some() && running(instances[1]).await.is_some()
    })
    .await;

    // One deletion request each. No enqueue, retry override or clock edit follows.
    let deleted_at = tokio::time::Instant::now();
    for (id, stop) in instances.into_iter().zip([QUICK_STOP, SLOW_STOP]) {
        client
            .stops
            .lock()
            .unwrap()
            .insert(id.to_owned(), deleted_at + stop);
        let ready = running(id).await.ok_or("instance left Running")?;
        store
            .request_instance_deletion(RequestInstanceDeletion {
                instance_id: ready.id,
                expected_generation: ready.generation,
            })
            .await?;
    }

    let (shutdown, stopped) = tokio::sync::watch::channel(false);
    let mut running_driver = tokio::spawn(driver().run_until_shutdown(stopped));
    let result: TestResult = async {
        // A Pod that is stopping is progress: the attempt waits for it, records
        // no failure, and finishes within the absence poll of the Pod's stop.
        loop {
            let Some(row) = cleanup_row(raw, instances[0]).await? else {
                break;
            };
            if row["failure_count"] != 0 || !row["failure_kind"].is_null() {
                return Err(format!("a normal cleanup recorded a failure: {row}").into());
            }
            if deleted_at.elapsed() > QUICK_STOP + Duration::from_secs(5) {
                return Err(format!("cleanup outlasted its Pod's stop: {row}").into());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if deleted_at.elapsed() < QUICK_STOP {
            return Err("cleanup finished while its Pod was still stopping".into());
        }

        // A Pod that outlives one attempt's wait costs a retry after a backoff.
        // The cleanup passes its operation deadline on the way and still
        // finishes on its own once the Pod is gone.
        let mut retried_past_deadline = false;
        loop {
            let Some(row) = cleanup_row(raw, instances[1]).await? else {
                break;
            };
            if row["state"] != "deleting"
                || !(row["failure_kind"].is_null() || row["failure_kind"] == "transient")
            {
                return Err(format!("cleanup was parked: {row}").into());
            }
            if row["failure_count"] == 1 {
                if row["past_deadline"] != true
                    || !row["failure_message"]
                        .as_str()
                        .is_some_and(|message| message.contains("cleanup blocked"))
                {
                    return Err(format!("unexpected cleanup failure: {row}").into());
                }
                retried_past_deadline = true;
            }
            if deleted_at.elapsed() > SLOW_STOP + Duration::from_secs(20) {
                return Err(format!("cleanup did not finish after its Pod stopped: {row}").into());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if deleted_at.elapsed() < SLOW_STOP {
            return Err("cleanup finished while its Pod was still stopping".into());
        }
        if !retried_past_deadline {
            return Err("the slow cleanup never recorded its timed-out attempt".into());
        }
        Ok(())
    }
    .await;
    shutdown.send_replace(true);
    let joined = match tokio::time::timeout(Duration::from_secs(25), &mut running_driver).await {
        Ok(result) => result
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
            .and_then(|result| {
                result.map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
            }),
        Err(error) => {
            running_driver.abort();
            let _ = running_driver.await;
            Err(Box::new(error) as Box<dyn Error + Send + Sync>)
        }
    };
    joined?;
    result
}
