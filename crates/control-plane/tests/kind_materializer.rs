use std::{
    collections::BTreeMap,
    env,
    error::Error,
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::{
    projection::{ProjectionPlan, ProjectionReconciler},
    render_manifests, BackendGeneration, ContainerPortTemplate, ContainerTemplate, Generation,
    InstanceId, InstanceRecord, InstanceState, InstanceValues, KubeMaterializerClient,
    KubeMaterializerClientConfig, KubernetesMaterializer, KubernetesMaterializerClient,
    ManifestTemplate, MaterializationId, MaterializationRecord, MaterializationState,
    MaterializationTarget, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
    PersistentVolumeSourceTemplate, RenderManifestRequest, RenderedManifest, RenderedObjectRef,
    ResolvedSleepPolicy, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
    TemplateTextPart, VolumeTemplate, WorkloadClassId, WorkloadClassVersionRef, WorkloadKind,
    WorkloadTemplate,
};
use k8s_openapi::api::{
    apps::v1::StatefulSet,
    core::v1::{
        Container, EnvVar, Namespace, Node, PersistentVolume, PersistentVolumeClaim,
        PersistentVolumeClaimVolumeSource, Pod, PodSpec, Service, Volume, VolumeMount,
    },
};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::{
    api::{DeleteParams, ListParams, LogParams, ObjectMeta, Patch, PatchParams, PostParams},
    Api, Client, Error as KubeError,
};
use tokio::time::{sleep, Instant};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PVC_NAME: &str = "sleepypods-kind-pvc";
const WORKLOAD_NAME: &str = "sleepypods-kind-app";
const HELPER_IMAGE: &str = "busybox:1.36";
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const OBJECT_DELETION_TIMEOUT: Duration = Duration::from_secs(90);
const HELPER_POD_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
#[ignore = "requires SLEEPYPODS_KIND_TEST=1 and a disposable single-node kind/current kube context"]
async fn materializes_stateful_set_with_static_host_path_volume() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_TEST").as_deref() != Ok("1") {
        eprintln!("skipping kind materializer test because SLEEPYPODS_KIND_TEST=1 is not set");
        return Ok(());
    }

    install_rustls_crypto_provider();

    let client = Client::try_default().await?;
    verify_single_node_cluster(client.clone()).await?;

    let namespace = unique_namespace()?;
    create_namespace(client.clone(), &namespace).await?;

    let result = run_materializer_lifecycle(client.clone(), &namespace).await;
    let namespace_delete = delete_namespace(client, &namespace).await;

    result?;
    namespace_delete?;
    Ok(())
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

async fn run_materializer_lifecycle(client: Client, namespace: &str) -> TestResult<()> {
    let pv_seed = pv_name(namespace);
    let marker = marker_value(namespace);
    let config = KubeMaterializerClientConfig {
        readiness_timeout: Duration::from_secs(90),
        ..KubeMaterializerClientConfig::default()
    };
    let kube_client = KubeMaterializerClient::with_config(client.clone(), config)?;
    let materializer = KubernetesMaterializer::new(kube_client);
    let projection = ProjectionReconciler::new(&materializer);
    let manifest = kind_manifest(namespace, &pv_seed)?;
    let refs = rendered_refs(&manifest);
    let name = |kind: &str| {
        refs.iter()
            .find(|object| object.kind == kind)
            .unwrap()
            .name
            .clone()
    };
    let pv_name = name("PersistentVolume");
    let pvc_name = name("PersistentVolumeClaim");
    let workload_name = name("StatefulSet");
    let record = MaterializationRecord {
        pod_backend: None,
        id: MaterializationId::new(format!("kind-materializer:kind:{namespace}"))?,
        instance_id: InstanceId::new("kind-materializer")?,
        instance_generation: Generation::new(1),
        projection_generation: Generation::new(1),
        target: MaterializationTarget::new("kind", namespace)?,
        state: MaterializationState::Pending,
        backend: None,
        backend_generation: BackendGeneration::new(1),
        rendered_objects: refs.clone(),
        exclusivity_keys: Vec::new(),
        reconciliation_lease: None,
    };
    let plan = ProjectionPlan::from_manifest(&record, &manifest)?;

    let lifecycle_result: TestResult<()> = async {
        // The workload is created without waiting for the claim; its Pod is
        // ready only once the claim is Bound and mounted.
        projection.apply(&plan).await?;
        let backend = projection.wait_for_readiness(&plan).await?;
        verify_pv_and_bound_pvc_exist(client.clone(), namespace, &pv_name, &pvc_name).await?;
        verify_backend_uri(client.clone(), backend.uri(), namespace, &workload_name).await?;

        write_marker(client.clone(), namespace, &pvc_name, &marker).await?;

        // Hold the old Pod after foreground workload deletion and prove cleanup
        // keeps waiting until that member is actually gone.
        let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
        let pod = pods.list(&ListParams::default().labels("sleepypods.io/instance-id=kind-materializer")).await?.items.into_iter().next().ok_or("ready workload has no Pod")?;
        let pod_name = pod.metadata.name.as_deref().ok_or("Pod name missing")?;
        let uid = pod.metadata.uid.as_deref().ok_or("Pod UID missing")?;
        pods.patch(pod_name, &PatchParams::default(), &Patch::Merge(serde_json::json!({"metadata":{"uid":uid,"resourceVersion":pod.metadata.resource_version,"finalizers":["sleepypods.io/phase6bd-test-hold"]}}))).await?;
        let cleanup = projection.delete_owned(&plan);
        tokio::pin!(cleanup);
        let held = tokio::time::timeout(Duration::from_secs(10), &mut cleanup).await;
        pods.patch(pod_name, &PatchParams::default(), &Patch::Merge(serde_json::json!({"metadata":{"uid":uid,"finalizers":null}}))).await?;
        assert!(held.is_err(), "terminating old member must hold cleanup ownership");
        cleanup.await?;
        wait_for_rendered_objects_deleted(
            client.clone(),
            namespace,
            &pv_name,
            &pvc_name,
            &workload_name,
        )
        .await?;

        projection.apply(&plan).await?;
        let backend = projection.wait_for_readiness(&plan).await?;
        verify_pv_and_bound_pvc_exist(client.clone(), namespace, &pv_name, &pvc_name).await?;
        verify_backend_uri(client.clone(), backend.uri(), namespace, &workload_name).await?;

        verify_marker_present(client.clone(), namespace, &pvc_name, &marker).await?;
        Ok(())
    }
    .await;

    let delete_result = projection.delete_owned(&plan).await;
    let deletion_wait_result =
        wait_for_rendered_objects_deleted(client, namespace, &pv_name, &pvc_name, &workload_name)
            .await;

    lifecycle_result?;
    delete_result?;
    deletion_wait_result?;
    Ok(())
}

async fn verify_single_node_cluster(client: Client) -> TestResult<()> {
    let nodes: Api<Node> = Api::all(client);
    let node_count = nodes.list(&ListParams::default()).await?.items.len();
    if node_count != 1 {
        return Err(format!(
            "kind materializer hostPath continuity test requires exactly one Kubernetes node; found {node_count}"
        )
        .into());
    }

    Ok(())
}

/// The backend is the Service's ClusterIP and port.
async fn verify_backend_uri(
    client: Client,
    uri: &str,
    namespace: &str,
    workload_name: &str,
) -> TestResult<()> {
    let services: Api<Service> = Api::namespaced(client, namespace);
    let cluster_ip = services
        .get(workload_name)
        .await?
        .spec
        .and_then(|spec| spec.cluster_ip)
        .ok_or("Service has no ClusterIP")?;
    let expected = format!("http://{cluster_ip}:8080");
    if uri != expected {
        return Err(format!("expected backend URI {expected}, got {uri}").into());
    }

    Ok(())
}

fn kind_manifest(namespace: &str, pv_name: &str) -> TestResult<RenderedManifest> {
    let template = ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::StatefulSet,
            name: TemplateText::literal(WORKLOAD_NAME),
            replicas: Some(1),
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("registry.k8s.io/pause:3.10"),
                ports: vec![ContainerPortTemplate {
                    name: Some("app".to_owned()),
                    container_port: 8080,
                }],
                env: Vec::new(),
            },
            termination_grace_period_seconds: None,
        },
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("nginx:1.27-alpine"),
            listen_port: 80,
            mode: None,
            app_protocol: Default::default(),
        },
        service: Some(ServiceTemplate {
            name: TemplateText::literal(WORKLOAD_NAME),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 8080,
                target_port: 8080,
            }],
        }),
        volumes: vec![VolumeTemplate {
            name: "data".to_owned(),
            mount_path: TemplateText::literal("/data"),
            pv_name: TemplateText::literal(pv_name),
            pvc_name: TemplateText::literal(PVC_NAME),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            capacity: TemplateText::literal("1Mi"),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
            storage_class_name: Some(TemplateText::literal("sleepypods-kind-static")),
            source: PersistentVolumeSourceTemplate::HostPath {
                path: TemplateText::from_parts([
                    TemplateTextPart::literal("/tmp/sleepypods-kind/"),
                    TemplateTextPart::literal(namespace),
                ]),
                type_: Some(TemplateText::literal("DirectoryOrCreate")),
            },
        }],
        raw_objects: Vec::new(),
    };
    let instance = InstanceRecord {
        id: InstanceId::new("kind-materializer")?,
        workload_class: WorkloadClassVersionRef::new(
            WorkloadClassId::new("kind")?,
            Generation::new(1),
        ),
        values: InstanceValues::new(),
        state: InstanceState::Cold,
        generation: Generation::new(1),
    };

    let mut manifest = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance,
        sleep_policy: resolved_sleep_policy(),
        namespace,
    })?;
    // This isolated PV/projection gate deliberately uses nginx as a serving
    // fixture, without a control plane or the production sidecar. Keep a real
    // readiness probe against that fixture's listener. Production sidecar/app
    // readiness (including the private health port) belongs to the cold E2E gates.
    for object in &mut manifest.objects {
        if object.object.kind() != "StatefulSet" {
            continue;
        }
        let mut workload: StatefulSet = serde_json::from_value(object.object.to_kubernetes_json())?;
        let probe = workload
            .spec
            .as_mut()
            .and_then(|spec| spec.template.spec.as_mut())
            .and_then(|pod| pod.containers.get_mut(1))
            .and_then(|sidecar| sidecar.readiness_probe.as_mut())
            .and_then(|probe| probe.http_get.as_mut())
            .ok_or("rendered sidecar readiness missing")?;
        probe.path = Some("/".to_owned());
        probe.port = IntOrString::Int(80);
        object.object = workload.into();
    }
    Ok(manifest)
}

fn resolved_sleep_policy() -> ResolvedSleepPolicy {
    ResolvedSleepPolicy {
        idle_timeout_ms: 300_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 30_000,
    }
}

async fn verify_pv_and_bound_pvc_exist(
    client: Client,
    namespace: &str,
    pv_name: &str,
    pvc_name: &str,
) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(client.clone());
    let pv = pvs.get(pv_name).await?;
    let expected_path = format!("/tmp/sleepypods-kind/{namespace}");
    let actual_path = pv
        .spec
        .as_ref()
        .and_then(|spec| spec.host_path.as_ref())
        .map(|source| source.path.as_str());
    if actual_path != Some(expected_path.as_str()) {
        return Err(format!(
            "expected PersistentVolume {pv_name} hostPath {expected_path}, got {actual_path:?}"
        )
        .into());
    }

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client, namespace);
    let pvc = pvcs.get(pvc_name).await?;
    let phase = pvc
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if phase != Some("Bound") {
        return Err(format!(
            "expected PersistentVolumeClaim {namespace}/{pvc_name} to be Bound, got {phase:?}"
        )
        .into());
    }

    Ok(())
}

async fn write_marker(
    client: Client,
    namespace: &str,
    pvc_name: &str,
    marker: &str,
) -> TestResult<()> {
    run_pvc_helper_pod(
        client,
        namespace,
        pvc_name,
        "sleepypods-marker-write",
        marker,
        &[
            "sh",
            "-c",
            "printf '%s' \"$MARKER_VALUE\" > /data/sleepypods-marker",
        ],
    )
    .await
}

async fn verify_marker_present(
    client: Client,
    namespace: &str,
    pvc_name: &str,
    marker: &str,
) -> TestResult<()> {
    run_pvc_helper_pod(
        client,
        namespace,
        pvc_name,
        "sleepypods-marker-read",
        marker,
        &[
            "sh",
            "-c",
            "test \"$(cat /data/sleepypods-marker)\" = \"$MARKER_VALUE\"",
        ],
    )
    .await
}

async fn run_pvc_helper_pod(
    client: Client,
    namespace: &str,
    pvc_name: &str,
    name: &str,
    marker: &str,
    command: &[&str],
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let pod = pvc_helper_pod(namespace, pvc_name, name, marker, command);

    pods.create(&PostParams::default(), &pod).await?;
    let result = wait_for_helper_pod_success(pods.clone(), name).await;
    let delete_result = delete_pod_if_present(pods.clone(), name).await;
    let deletion_wait_result = wait_for_absence(
        || {
            let pods = pods.clone();
            async move { pods.get(name).await }
        },
        format!("Pod {namespace}/{name}"),
        OBJECT_DELETION_TIMEOUT,
    )
    .await;

    result?;
    delete_result?;
    deletion_wait_result?;
    Ok(())
}

fn pvc_helper_pod(
    namespace: &str,
    pvc_name: &str,
    name: &str,
    marker: &str,
    command: &[&str],
) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: Some(BTreeMap::from([(
                "sleepypods.io/kind-test-helper".to_owned(),
                "true".to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".to_owned()),
            containers: vec![Container {
                name: "marker".to_owned(),
                image: Some(HELPER_IMAGE.to_owned()),
                command: Some(command.iter().map(|part| (*part).to_owned()).collect()),
                env: Some(vec![EnvVar {
                    name: "MARKER_VALUE".to_owned(),
                    value: Some(marker.to_owned()),
                    ..EnvVar::default()
                }]),
                volume_mounts: Some(vec![VolumeMount {
                    name: "data".to_owned(),
                    mount_path: "/data".to_owned(),
                    ..VolumeMount::default()
                }]),
                ..Container::default()
            }],
            volumes: Some(vec![Volume {
                name: "data".to_owned(),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                    claim_name: pvc_name.to_owned(),
                    read_only: Some(false),
                }),
                ..Volume::default()
            }]),
            ..PodSpec::default()
        }),
        ..Pod::default()
    }
}

async fn wait_for_helper_pod_success(pods: Api<Pod>, name: &str) -> TestResult<()> {
    let deadline = Instant::now() + HELPER_POD_TIMEOUT;

    loop {
        let pod = pods.get(name).await?;
        let phase = pod
            .status
            .as_ref()
            .and_then(|status| status.phase.as_deref());
        match phase {
            Some("Succeeded") => return Ok(()),
            Some("Failed") => {
                let logs = pods
                    .logs(name, &LogParams::default())
                    .await
                    .unwrap_or_default();
                return Err(format!("helper Pod {name} failed; logs: {logs}").into());
            }
            _ => {}
        }

        sleep_until_next_poll(
            deadline,
            format!("timed out waiting for helper Pod {name} to succeed"),
        )
        .await?;
    }
}

async fn delete_pod_if_present(pods: Api<Pod>, name: &str) -> TestResult<()> {
    match pods.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

async fn wait_for_rendered_objects_deleted(
    client: Client,
    namespace: &str,
    pv_name: &str,
    pvc_name: &str,
    workload_name: &str,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    wait_for_absence(
        || {
            let stateful_sets = stateful_sets.clone();
            async move { stateful_sets.get(workload_name).await }
        },
        format!("StatefulSet {namespace}/{workload_name}"),
        OBJECT_DELETION_TIMEOUT,
    )
    .await?;

    let services: Api<Service> = Api::namespaced(client.clone(), namespace);
    wait_for_absence(
        || {
            let services = services.clone();
            async move { services.get(workload_name).await }
        },
        format!("Service {namespace}/{workload_name}"),
        OBJECT_DELETION_TIMEOUT,
    )
    .await?;

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    wait_for_absence(
        || {
            let pvcs = pvcs.clone();
            async move { pvcs.get(pvc_name).await }
        },
        format!("PersistentVolumeClaim {namespace}/{pvc_name}"),
        OBJECT_DELETION_TIMEOUT,
    )
    .await?;

    let pvs: Api<PersistentVolume> = Api::all(client);
    wait_for_absence(
        || {
            let pvs = pvs.clone();
            async move { pvs.get(pv_name).await }
        },
        format!("PersistentVolume {pv_name}"),
        OBJECT_DELETION_TIMEOUT,
    )
    .await?;
    Ok(())
}

async fn wait_for_absence<K, Fut>(
    mut get: impl FnMut() -> Fut,
    description: String,
    timeout: Duration,
) -> TestResult<()>
where
    Fut: Future<Output = Result<K, KubeError>>,
{
    let deadline = Instant::now() + timeout;

    loop {
        match get().await {
            Ok(_) => {}
            Err(error) if is_not_found(&error) => return Ok(()),
            Err(error) => return Err(Box::new(error)),
        }

        sleep_until_next_poll(
            deadline,
            format!("timed out waiting for {description} to be deleted"),
        )
        .await?;
    }
}

async fn sleep_until_next_poll(deadline: Instant, timeout_message: String) -> TestResult<()> {
    let now = Instant::now();
    if now >= deadline {
        return Err(timeout_message.into());
    }

    sleep(POLL_INTERVAL.min(deadline - now)).await;
    Ok(())
}

async fn create_namespace(client: Client, namespace: &str) -> TestResult<()> {
    let namespaces: Api<Namespace> = Api::all(client);
    let object = Namespace {
        metadata: ObjectMeta {
            name: Some(namespace.to_owned()),
            labels: Some(BTreeMap::from([(
                "sleepypods.io/kind-test".to_owned(),
                "true".to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        ..Namespace::default()
    };
    namespaces.create(&PostParams::default(), &object).await?;
    Ok(())
}

async fn delete_namespace(client: Client, namespace: &str) -> TestResult<()> {
    let namespaces: Api<Namespace> = Api::all(client);
    match namespaces.delete(namespace, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

fn rendered_refs(manifest: &RenderedManifest) -> Vec<RenderedObjectRef> {
    manifest
        .objects
        .iter()
        .map(|object| object.object.object_ref())
        .collect()
}

fn unique_namespace() -> TestResult<String> {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(format!(
        "sleepypods-kind-{millis}-{}-{sequence}",
        std::process::id()
    ))
}

fn pv_name(namespace: &str) -> String {
    format!("{namespace}-pv")
}

fn marker_value(namespace: &str) -> String {
    format!("{namespace}-marker")
}

fn is_not_found(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(status) if status.is_not_found())
}

#[tokio::test]
#[ignore = "requires SLEEPYPODS_KIND_TEST=1 and isolated kind cluster"]
async fn conditional_secret_mutations_preserve_same_name_replacements() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    install_rustls_crypto_provider();
    let client = Client::try_default().await?;
    let namespace = unique_namespace()?;
    create_namespace(client.clone(), &namespace).await?;
    let result: TestResult<()> = async {
        let kube = KubeMaterializerClient::new(client.clone());
        let object: control_plane::KubernetesObject = k8s_openapi::api::core::v1::Secret {
            metadata: ObjectMeta {
                name: Some("conditional-secret".into()),
                namespace: Some(namespace.clone()),
                ..ObjectMeta::default()
            },
            type_: Some("Opaque".into()),
            string_data: Some(BTreeMap::from([("token".into(), "test-only".into())])),
            ..Default::default()
        }
        .into();
        let object_ref = object.object_ref();
        kube.apply_object(&object, None).await?;
        let control_plane::projection::ProjectionObjectInspection::Present(old) =
            kube.inspect_object(&object_ref).await?
        else {
            return Err("created Secret missing".into());
        };
        let secrets: Api<k8s_openapi::api::core::v1::Secret> =
            Api::namespaced(client.clone(), &namespace);
        secrets
            .delete(&object_ref.name, &DeleteParams::default())
            .await?;
        wait_for_absence(
            || secrets.get(&object_ref.name),
            "old test Secret".into(),
            OBJECT_DELETION_TIMEOUT,
        )
        .await?;
        kube.apply_object(&object, None).await?;
        let replacement = secrets.get(&object_ref.name).await?;
        assert_ne!(
            replacement.metadata.uid.as_deref(),
            Some(old.identity.uid.as_str())
        );
        assert!(kube
            .apply_object(&object, Some(&old.identity))
            .await
            .is_err());
        assert!(kube
            .delete_object(&object_ref, &old.identity)
            .await
            .is_err());
        assert_eq!(
            secrets.get(&object_ref.name).await?.metadata.uid,
            replacement.metadata.uid
        );
        Ok(())
    }
    .await;
    delete_namespace(client, &namespace).await?;
    result
}
