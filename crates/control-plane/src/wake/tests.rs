use super::*;
use crate::{
    ids::WorkloadClassId,
    instance::InstanceValues,
    manifest::{
        ContainerPortTemplate, ContainerTemplate, EnvVarTemplate, KubernetesObject,
        ManifestTemplate, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
        WorkloadKind, WorkloadTemplate,
    },
    materialization::{AcceptWakeRequest, BackendEndpoint, RenderedObjectRef},
    materializer::{KubernetesClientFuture, KubernetesClientResult},
    sleep_policy::WorkloadSleepPolicy,
    store::{LifecycleReads, StoreFuture, StoreResult},
    workload::{WorkloadClassVersion, WorkloadClassVersionRef, WorkloadValueSchema},
};
use std::sync::Mutex;

struct AcceptanceStore {
    instance: InstanceRecord,
    accepted: Mutex<Vec<AcceptWakeRequest>>,
    reject: bool,
    /// The projection a wake during drain grace returns to Ready.
    revived: Option<MaterializationRecord>,
}
impl AcceptanceStore {
    fn new(state: InstanceState) -> Self {
        Self {
            instance: InstanceRecord {
                id: InstanceId::new("acceptance").unwrap(),
                workload_class: workload_ref(),
                values: InstanceValues::new(),
                state,
                generation: Generation::new(8),
            },
            accepted: Mutex::new(Vec::new()),
            reject: false,
            revived: None,
        }
    }
}
impl LifecycleReads for AcceptanceStore {
    fn get_instance<'a>(
        &'a self,
        _: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async { Ok(Some(self.instance.clone())) })
    }
    fn load_workload_class_version<'a>(
        &'a self,
        _: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        Box::pin(async { Ok(Some(workload_class())) })
    }
    fn load_active_materialization<'a>(
        &'a self,
        _: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async { Ok(None) })
    }
    fn load_materialization_work_status<'a>(
        &'a self,
        _: crate::ids::MaterializationId,
    ) -> StoreFuture<'a, StoreResult<Option<crate::runtime_work::MaterializationWorkStatus>>> {
        Box::pin(async { Ok(None) })
    }
}
impl WakeStore for AcceptanceStore {
    fn accept_wake<'a>(
        &'a self,
        request: AcceptWakeRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async move {
            if self.reject {
                return Err(StoreError::unavailable("injected atomic commit failure"));
            }
            let mut result = self.instance.clone();
            if let Some(revived) = &self.revived {
                result.state = InstanceState::Running;
                result.generation = revived.instance_generation;
            } else if result.state != InstanceState::Draining {
                result.state = InstanceState::Waking;
                result.generation = request.pending.instance_generation;
            }
            self.accepted.lock().unwrap().push(request);
            Ok(result)
        })
    }
    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .revived
                .clone()
                .filter(|revived| revived.instance_generation == request.instance_generation))
        })
    }
}
/// Every Kubernetes operation traps, including cleanup/readiness. The API must
/// accept promptly even when the actual cluster is unreachable indefinitely.
struct NoKubernetes;
impl KubernetesMaterializerClient for NoKubernetes {
    fn apply_object<'a>(
        &'a self,
        _: &'a KubernetesObject,
        _precondition: Option<&'a crate::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        panic!("API must not apply")
    }
    fn delete_object<'a>(
        &'a self,
        _: &'a RenderedObjectRef,
        _precondition: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        panic!("API must not delete")
    }
    fn wait_for_readiness<'a>(
        &'a self,
        _: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        panic!("API must not wait for readiness")
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        _instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn inspect_object<'a>(
        &'a self,
        _object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<
        'a,
        KubernetesClientResult<crate::projection::ProjectionObjectInspection>,
    > {
        Box::pin(async {
            Ok(crate::projection::ProjectionObjectInspection::Present(
                crate::projection::LiveObjectMetadata {
                    persistent_volume_reclaim_policy: Some("Retain".into()),
                    identity: crate::projection::LiveObjectIdentity {
                        uid: "test-uid".into(),
                        resource_version: "1".into(),
                    },
                    labels: Default::default(),
                    annotations: Default::default(),
                    deleting: false,
                    finalizers: Vec::new(),
                },
            ))
        })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> crate::materializer::KubernetesClientFuture<
        'a,
        crate::materializer::KubernetesClientResult<()>,
    > {
        Box::pin(async { Ok(()) })
    }
}
fn request() -> WakeInstanceRequest {
    WakeInstanceRequest::new(
        InstanceId::new("acceptance").unwrap(),
        Generation::new(8),
        MaterializationTarget::new("cluster-a", "apps").unwrap(),
    )
}

#[tokio::test]
async fn acceptance_prepares_one_complete_projection_without_kubernetes_io() {
    let store = AcceptanceStore::new(InstanceState::Cold);
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        wake_instance(
            &store,
            &KubernetesMaterializer::new(NoKubernetes),
            request(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(result, WakeInstanceResult::AlreadyWaking { instance } if instance.state == InstanceState::Waking && instance.generation == Generation::new(9))
    );
    let accepted = store.accepted.lock().unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].pending.instance_generation, Generation::new(9));
    assert_eq!(
        accepted[0].pending.projection_generation,
        Generation::new(10)
    );
    assert!(!accepted[0].pending.rendered_objects.is_empty());
}

#[tokio::test]
async fn wake_during_drain_only_persists_future_intent() {
    let store = AcceptanceStore::new(InstanceState::Draining);
    let result = wake_instance(
        &store,
        &KubernetesMaterializer::new(NoKubernetes),
        request(),
    )
    .await
    .unwrap();
    assert!(
        matches!(result, WakeInstanceResult::AlreadyWaking { instance } if instance.state == InstanceState::Draining && instance.generation == Generation::new(8))
    );
    let accepted = store.accepted.lock().unwrap();
    assert_eq!(accepted[0].pending.instance_generation, Generation::new(10));
    assert_eq!(
        accepted[0].pending.projection_generation,
        Generation::new(11)
    );
}

#[tokio::test]
async fn wake_during_drain_grace_answers_with_the_revived_projection() {
    let mut store = AcceptanceStore::new(InstanceState::Draining);
    let backend = BackendEndpoint::new("http://10.96.0.10:80").unwrap();
    store.revived = Some(MaterializationRecord {
        pod_backend: None,
        id: crate::ids::MaterializationId::new("acceptance:cluster-a:apps").unwrap(),
        instance_id: store.instance.id.clone(),
        instance_generation: Generation::new(9),
        projection_generation: Generation::new(7),
        target: request().target,
        state: MaterializationState::Ready,
        backend: Some(backend.clone()),
        backend_generation: BackendGeneration::new(8),
        rendered_objects: Vec::new(),
        exclusivity_keys: Vec::new(),
        reconciliation_lease: None,
    });

    let result = wake_instance(
        &store,
        &KubernetesMaterializer::new(NoKubernetes),
        request(),
    )
    .await
    .unwrap();

    let WakeInstanceResult::AlreadyRunning {
        instance,
        materialization,
    } = result
    else {
        panic!("a revived instance is Running, got {result:?}");
    };
    assert_eq!(instance.state, InstanceState::Running);
    assert_eq!(instance.generation, Generation::new(9));
    assert_eq!(materialization.backend, Some(backend));
    assert_eq!(materialization.projection_generation, Generation::new(7));
}

#[tokio::test]
async fn commit_failure_never_claims_acceptance_or_calls_kubernetes() {
    let mut store = AcceptanceStore::new(InstanceState::Cold);
    store.reject = true;
    assert!(matches!(
        wake_instance(
            &store,
            &KubernetesMaterializer::new(NoKubernetes),
            request()
        )
        .await,
        Err(WakeInstanceError::Store(StoreError::Unavailable { .. }))
    ));
    assert!(store.accepted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn duplicate_waking_and_stale_or_terminal_requests_do_not_schedule_work() {
    for state in [
        InstanceState::Waking,
        InstanceState::Deleting,
        InstanceState::Deleted,
    ] {
        let store = AcceptanceStore::new(state);
        let result = wake_instance(
            &store,
            &KubernetesMaterializer::new(NoKubernetes),
            request(),
        )
        .await;
        assert!(
            result.is_err(),
            "no accepted projection exists for the requested target"
        );
        assert!(store.accepted.lock().unwrap().is_empty());
    }
    let store = AcceptanceStore::new(InstanceState::Cold);
    let mut stale = request();
    stale.expected_generation = Generation::new(7);
    assert!(matches!(
        wake_instance(&store, &KubernetesMaterializer::new(NoKubernetes), stale).await,
        Err(WakeInstanceError::GenerationConflict { .. })
    ));
    assert!(store.accepted.lock().unwrap().is_empty());
}

fn workload_class() -> WorkloadClassVersion {
    WorkloadClassVersion {
        reference: workload_ref(),
        template: deployment_template(),
        value_schema: WorkloadValueSchema::new(true),
        sleep_policy: WorkloadSleepPolicy::new(120_000, 5_000, 30_000).expect("valid sleep policy"),
        exclusivity_keys: vec![],
    }
}

fn workload_ref() -> WorkloadClassVersionRef {
    WorkloadClassVersionRef::new(
        WorkloadClassId::new("web").expect("workload class ID"),
        Generation::new(1),
    )
}

fn deployment_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: TemplateText::literal("app-acme"),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("example/app:1"),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: vec![EnvVarTemplate {
                    name: "TENANT".to_owned(),
                    value: TemplateText::literal("acme"),
                }],
            },
            termination_grace_period_seconds: None,
        },
        service: Some(ServiceTemplate {
            name: TemplateText::literal("svc-acme"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: None,
            app_protocol: Default::default(),
        },
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}
