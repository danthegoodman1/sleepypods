//! Process loss at each exclusivity-lock boundary, against real PostgreSQL.
//!
//! A control plane that dies mid-sequence leaves its reconciliation lease behind
//! with no owner to renew it. These tests reproduce that by claiming a lease and
//! then expiring it in the database, which is what the row looks like once the
//! process holding it is gone.
//!
//! One invariant runs through all three: an exclusivity key stays held for as
//! long as a durable materialization row survives, so a crash never hands the
//! key to a second instance, and recovery is always another owner claiming the
//! same work rather than the key changing hands.
use super::*;

/// Where the process dies relative to the Kubernetes objects it owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashPoint {
    /// The key is acquired and the row recorded; nothing is applied yet.
    BeforeApply,
    /// Objects are applied and recorded; the row has not reached Ready.
    AfterApplyBeforeReady,
    /// The row is being torn down and its objects are partly gone.
    DuringCleanup,
}

#[tokio::test]
async fn postgres_crash_at_each_lock_boundary_keeps_the_key_and_yields_to_recovery() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping crash boundary coverage; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    for point in [
        CrashPoint::BeforeApply,
        CrashPoint::AfterApplyBeforeReady,
        CrashPoint::DuringCleanup,
    ] {
        let schema = unique_schema_name();
        let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
        let admin_task = tokio::spawn(connection);
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await?;
        let config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
        let store = PostgresStore::connect(&config).await?;
        // Expiring a lease needs a connection inside the test's schema; the
        // admin connection sits outside it.
        let (raw, raw_connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let raw_task = tokio::spawn(raw_connection);
        let result = crash_boundary(&store, &raw, point).await;
        raw_task.abort();
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await?;
        admin_task.abort();
        result?;
    }
    Ok(())
}

async fn crash_boundary(
    store: &PostgresStore,
    raw: &tokio_postgres::Client,
    point: CrashPoint,
) -> TestResult {
    let workload_class = exclusive_workload_class();
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            workload_class.clone(),
        ))
        .await?;
    let target = MaterializationTarget::new("cluster-crash", "apps").expect("valid target");

    let owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-crash-owner",
        "disk-crash",
        "license-crash",
    )
    .await?;
    // Accepting the wake acquires the key, so every crash point is reached
    // with the key already held.
    let keys = workload_class
        .render_exclusivity_keys(&owner.instance.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let refs = if point == CrashPoint::BeforeApply {
        vec![]
    } else {
        // An owner that reached the apply recorded what it created, so recovery
        // inherits the inventory it has to finish or tear down.
        vec![RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: "StatefulSet".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-crash-owner".to_owned(),
        }]
    };
    let record = accept_projection(store, &owner.instance, &target, refs, keys).await?;
    assert_eq!(
        record.rendered_objects.is_empty(),
        point == CrashPoint::BeforeApply,
        "{point:?}: the row carries the inventory the owner had reached"
    );

    if point == CrashPoint::DuringCleanup {
        assert!(
            store
                .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                    instance_id: owner.instance.id.clone(),
                    expected_generation: record.instance_generation,
                })
                .await?,
            "deletion is accepted while the key is held"
        );
    }

    // The process claims the work, then dies. An abandoned lease is one whose
    // owner stopped renewing it.
    let state = if point == CrashPoint::DuringCleanup {
        MaterializationState::Deleting
    } else {
        MaterializationState::Pending
    };
    let claimed = claim_next(
        store,
        state,
        &target,
        "crashed-owner",
        Duration::from_secs(30),
    )
    .await?
    .expect("the crashed owner claims the work first");
    assert_eq!(claimed.id, record.id);
    raw.execute(
        "UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 0 \
             WHERE materialization_id = $1",
        &[&record.id.as_str()],
    )
    .await?;

    // The key outlives the process that took it.
    let contender = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-crash-contender",
        "disk-crash",
        "license-contender",
    )
    .await?;
    let contender_keys = workload_class
        .render_exclusivity_keys(&contender.instance.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let conflict = store
        .accept_wake(accept_request(
            &contender.instance,
            &target,
            vec![],
            contender_keys,
        ))
        .await
        .expect_err("a crashed owner still holds its key");
    assert_exclusivity_conflict(conflict, "disk", Some("instance-crash-owner"));

    // Recovery is another owner claiming the same work.
    let recovered = claim_next(
        store,
        state,
        &target,
        "recovery-owner",
        Duration::from_secs(30),
    )
    .await?
    .expect("an expired lease lets a fresh owner claim the work");
    assert_eq!(recovered.id, record.id);
    let lease = recovered
        .reconciliation_lease
        .expect("the recovered claim carries a lease");
    assert!(
        lease.attempt > claimed.reconciliation_lease.expect("first lease").attempt,
        "{point:?}: recovery counts as a later attempt"
    );

    Ok(())
}

/// A workload mutation the lifecycle driver sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    /// Creating the workload, with zero replicas.
    Create,
    /// The conditional patch that scales the workload up.
    Activate,
    Delete,
}

/// What goes wrong around one mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// The API server answers 5xx.
    ServerError,
    /// The connection breaks before a response arrives.
    ConnectionReset,
    /// The request outlasts the client's timeout.
    Stall,
    /// The control-plane process dies around the request.
    ProcessLoss,
}

#[derive(Clone, Copy, Debug)]
struct Injection {
    mutation: Mutation,
    fault: Fault,
    /// Whether the API server applied the request the driver lost track of.
    landed: bool,
}

/// How long the stalled request takes to give up. The real client reports an
/// unknown outcome after ten seconds; the driver's handling is the same.
const STALL: Duration = Duration::from_millis(1500);
/// Short enough that work a dead process held is reclaimed within the test.
const FAULT_LEASE_TTL: Duration = Duration::from_secs(2);

struct LiveObject {
    metadata: control_plane::projection::LiveObjectMetadata,
    /// A workload starts Pods only once it is scaled up.
    active: bool,
}

/// An API server that checks UID and resourceVersion preconditions, keeps a
/// created workload at zero replicas until it is patched, and fails one armed
/// workload mutation.
#[derive(Clone, Default)]
struct FaultyKubernetes {
    objects: std::sync::Arc<std::sync::Mutex<BTreeMap<String, LiveObject>>>,
    versions: std::sync::Arc<std::sync::atomic::AtomicU64>,
    armed: std::sync::Arc<std::sync::Mutex<Option<Injection>>>,
    process_lost: std::sync::Arc<tokio::sync::Notify>,
    most_active: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl FaultyKubernetes {
    fn arm(&self, injection: Injection) {
        *self.armed.lock().unwrap() = Some(injection);
    }

    fn next_version(&self) -> String {
        (self
            .versions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1)
        .to_string()
    }

    fn is_workload(object: &RenderedObjectRef) -> bool {
        matches!(object.kind.as_str(), "Deployment" | "StatefulSet")
    }

    fn conflict(message: &str) -> control_plane::KubernetesClientError {
        control_plane::KubernetesClientError::stale(message)
    }

    fn create_or_patch(
        &self,
        object: &control_plane::KubernetesObject,
        precondition: Option<&control_plane::projection::LiveObjectIdentity>,
    ) -> control_plane::KubernetesClientResult<()> {
        let object_ref = control_plane::rendered_object_ref(object);
        let key = lifecycle_object_key(&object_ref);
        let mut objects = self.objects.lock().unwrap();
        let mut metadata =
            control_plane::projection::LiveObjectMetadata::from_rendered_object(object);
        match (objects.get(&key), precondition) {
            (Some(_), None) => return Err(Self::conflict("409 AlreadyExists")),
            (None, Some(_)) => return Err(Self::conflict("404 NotFound")),
            (Some(live), Some(expected)) if live.metadata.identity != *expected => {
                return Err(Self::conflict("409 Conflict"));
            }
            (Some(live), Some(_)) => metadata.identity.uid = live.metadata.identity.uid.clone(),
            (None, None) => metadata.identity.uid = format!("uid-{}", self.next_version()),
        }
        metadata.identity.resource_version = self.next_version();
        objects.insert(
            key,
            LiveObject {
                metadata,
                active: precondition.is_some() && Self::is_workload(&object_ref),
            },
        );
        let active = objects.values().filter(|live| live.active).count();
        self.most_active
            .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn delete(
        &self,
        object: &RenderedObjectRef,
        precondition: &control_plane::projection::LiveObjectIdentity,
    ) -> control_plane::KubernetesClientResult<()> {
        let mut objects = self.objects.lock().unwrap();
        let key = lifecycle_object_key(object);
        match objects.get(&key) {
            Some(live) if live.metadata.identity != *precondition => {
                Err(Self::conflict("409 Conflict"))
            }
            _ => {
                objects.remove(&key);
                Ok(())
            }
        }
    }

    /// Runs one workload mutation, failing it when it is the armed one.
    async fn inject(
        &self,
        mutation: Mutation,
        apply: impl FnOnce() -> control_plane::KubernetesClientResult<()>,
    ) -> control_plane::KubernetesClientResult<()> {
        let injection = self
            .armed
            .lock()
            .unwrap()
            .take_if(|injection| injection.mutation == mutation);
        let Some(injection) = injection else {
            return apply();
        };
        if injection.landed {
            apply()?;
        }
        match injection.fault {
            Fault::ServerError => Err(Self::conflict("503 Service Unavailable")),
            Fault::ConnectionReset => Err(Self::conflict("connection reset by peer")),
            Fault::Stall => {
                tokio::time::sleep(STALL).await;
                Err(Self::conflict("mutation timed out; its outcome is unknown"))
            }
            Fault::ProcessLoss => {
                self.process_lost.notify_one();
                std::future::pending().await
            }
        }
    }
}

impl control_plane::KubernetesMaterializerClient for FaultyKubernetes {
    fn apply_object<'a>(
        &'a self,
        object: &'a control_plane::KubernetesObject,
        precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        Box::pin(async move {
            if !Self::is_workload(&control_plane::rendered_object_ref(object)) {
                return self.create_or_patch(object, precondition);
            }
            let mutation = if precondition.is_some() {
                Mutation::Activate
            } else {
                Mutation::Create
            };
            self.inject(mutation, || self.create_or_patch(object, precondition))
                .await
        })
    }
    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        Box::pin(async move {
            if !Self::is_workload(object) {
                return self.delete(object, precondition);
            }
            self.inject(Mutation::Delete, || self.delete(object, precondition))
                .await
        })
    }
    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> control_plane::KubernetesClientFuture<
        'a,
        control_plane::KubernetesClientResult<BackendEndpoint>,
    > {
        Box::pin(async move {
            let live = self.objects.lock().unwrap();
            if objects.iter().any(|object| {
                live.get(&lifecycle_object_key(object))
                    .is_some_and(|live| live.active)
            }) {
                Ok(BackendEndpoint::new("http://10.0.0.1:80").unwrap())
            } else {
                Err(control_plane::KubernetesClientError::transient(
                    "no ready endpoint: the workload has no Pod",
                ))
            }
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
        Box::pin(async move {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(&lifecycle_object_key(object))
                .map(|live| {
                    control_plane::projection::ProjectionObjectInspection::Present(
                        live.metadata.clone(),
                    )
                })
                .unwrap_or(control_plane::projection::ProjectionObjectInspection::Missing))
        })
    }
    fn ensure_no_descendants<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        _instance_id: &'a str,
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        Box::pin(async move {
            let live = self.objects.lock().unwrap();
            if objects
                .iter()
                .any(|object| live.contains_key(&lifecycle_object_key(object)))
            {
                return Err(control_plane::KubernetesClientError::transient(
                    "Pods still exist",
                ));
            }
            Ok(())
        })
    }
    fn verify_retained_bindings<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> control_plane::KubernetesClientFuture<'a, control_plane::KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Every transient fault around every workload mutation, and loss of the
/// process on either side of it, ends with the instance Running or Cold. No
/// operator call is made, and two instances never hold one exclusivity key.
#[tokio::test]
async fn postgres_lifecycle_recovers_from_each_fault_around_each_mutation() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping fault recovery coverage; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
    let result = async {
        let store = PostgresStore::connect(&config).await?;
        let class = exclusive_workload_class();
        store
            .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
            .await?;
        let mut scenarios = tokio::task::JoinSet::new();
        // Each scenario owns its target, instances and key; a few run at once.
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(6));
        let mut number = 0;
        for mutation in [Mutation::Create, Mutation::Activate, Mutation::Delete] {
            for fault in [
                Fault::ServerError,
                Fault::ConnectionReset,
                Fault::Stall,
                Fault::ProcessLoss,
            ] {
                for landed in [false, true] {
                    number += 1;
                    let injection = Injection {
                        mutation,
                        fault,
                        landed,
                    };
                    let (store, class, slots) =
                        (store.clone(), class.reference.clone(), slots.clone());
                    scenarios.spawn(async move {
                        let _slot = slots.acquire_owned().await.expect("open semaphore");
                        fault_recovery(store, class, number, injection)
                            .await
                            .map_err(|error| format!("{injection:?}: {error}"))
                    });
                }
            }
        }
        let mut failures = Vec::new();
        while let Some(scenario) = scenarios.join_next().await {
            if let Err(failure) = scenario? {
                failures.push(failure);
            }
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
    admin_task.abort();
    result
}

async fn fault_recovery(
    pg: PostgresStore,
    class: WorkloadClassVersionRef,
    number: usize,
    injection: Injection,
) -> TestResult {
    use control_plane::api::{pb, StoreBackedProxyApi};
    use pb::proxy_control_plane_server::ProxyControlPlane;
    use std::sync::Arc;

    let target = MaterializationTarget::new(format!("fault-{number}"), "apps")?;
    let disk = format!("disk-{number}");
    let mut instances = Vec::new();
    for role in ["owner", "contender"] {
        let id = format!("fault-{number}-{role}");
        instances.push(
            create_exclusive_instance(&pg, &class, &id, &disk, &format!("license-{id}"))
                .await?
                .instance,
        );
    }
    let (owner, contender) = (instances[0].clone(), instances[1].clone());
    let store =
        Arc::new(control_plane::RetryingControlPlaneStore::with_default_policy(Arc::new(pg)));
    let kube = FaultyKubernetes::default();
    let materializer = control_plane::KubernetesMaterializer::new(kube.clone());
    let wake = |instance: &control_plane::InstanceRecord| {
        let api = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone());
        let request = pb::ProxyWakeInstanceRequest {
            instance_id: instance.id.as_str().to_owned(),
            expected_generation: instance.generation.get(),
            backend_generation: None,
        };
        async move { api.wake_instance(tonic::Request::new(request)).await }
    };
    let current = |id: InstanceId| {
        let store = store.clone();
        async move {
            store
                .get_instance(GetInstanceRequest::new(id))
                .await?
                .ok_or_else(|| StoreError::internal("instance vanished"))
        }
    };
    // Drives the lifecycle until the instance reaches `state`. When the process
    // is lost, its driver is dropped with nothing released, and a fresh driver
    // under a new owner takes over once the abandoned lease expires.
    let drivers = std::sync::atomic::AtomicUsize::new(0);
    let drive = |id: InstanceId, state: InstanceState| {
        let (store, kube, materializer, target, current) =
            (&store, &kube, &materializer, &target, &current);
        let drivers = &drivers;
        async move {
            loop {
                let owner = format!(
                    "fault-{number}-driver-{}",
                    drivers.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                );
                let driver = control_plane::MaterializationReconciler::new(
                    store.clone(),
                    materializer.clone(),
                    target.clone(),
                    control_plane::MaterializationReconcilerConfig {
                        owner,
                        interval: Duration::from_millis(20),
                        lease_ttl: FAULT_LEASE_TTL,
                        ..Default::default()
                    },
                    sleepypods_observability::recorder::ObservabilityRecorder::noop(),
                );
                let (shutdown, stopped) = tokio::sync::watch::channel(false);
                let running = tokio::spawn(driver.run_until_shutdown(stopped));
                let reached = async {
                    while current(id.clone()).await?.state != state {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok::<_, StoreError>(())
                };
                tokio::select! {
                    _ = kube.process_lost.notified() => {
                        running.abort();
                        let _ = running.await;
                    }
                    reached = tokio::time::timeout(Duration::from_secs(60), reached) => {
                        shutdown.send_replace(true);
                        running.await??;
                        reached.map_err(|_| format!("{id} did not reach {state:?}"))??;
                        return Ok::<_, Box<dyn Error + Send + Sync>>(());
                    }
                }
            }
        }
    };

    if injection.mutation != Mutation::Delete {
        kube.arm(injection);
    }
    wake(&owner).await?;
    // The key is held from acceptance, so the contender's wake is refused.
    let _ = wake(&contender).await;
    assert_eq!(
        current(contender.id.clone()).await?.state,
        InstanceState::Cold,
        "a second instance woke while the key was held"
    );
    drive(owner.id.clone(), InstanceState::Running).await?;

    if injection.mutation == Mutation::Delete {
        kube.arm(injection);
    }
    let running = current(owner.id.clone()).await?;
    store
        .begin_sleep(BeginSleepRequest::new(
            running.id.clone(),
            running.generation,
            target.clone(),
        ))
        .await?;
    let _ = wake(&contender).await;
    assert_eq!(
        current(contender.id.clone()).await?.state,
        InstanceState::Cold,
        "a second instance woke while the key was held"
    );
    drive(owner.id.clone(), InstanceState::Cold).await?;
    assert!(
        kube.objects.lock().unwrap().is_empty(),
        "the key was released before every object was proven absent"
    );
    assert!(
        kube.armed.lock().unwrap().is_none(),
        "the fault was never reached"
    );

    // The released key passes to the contender, and only now.
    wake(&contender).await?;
    drive(contender.id.clone(), InstanceState::Running).await?;
    assert_eq!(
        kube.most_active.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "two workloads holding one key ran at once"
    );
    Ok(())
}
