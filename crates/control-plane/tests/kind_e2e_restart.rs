#[path = "support/http_once.rs"]
mod http_once;

use std::{
    collections::HashMap,
    env,
    error::Error,
    net::SocketAddr,
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient,
    proxy_control_plane_client::ProxyControlPlaneClient, proxy_wake_instance_response,
    route_identity, template_text_part, ContainerPortTemplate, ContainerTemplate,
    CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    DeleteHttp01ChallengeRequest, DeleteInstanceRequest, DeleteRouteBindingRequest, EnvVarTemplate,
    ExpireHttp01ChallengesRequest, GetInstanceRequest, Http01ChallengeKey, HttpRouteIdentity,
    Instance, InstanceState as PbInstanceState, ManifestTemplate, ProtocolRoute,
    ProxyWakeInstanceRequest, PutHttp01ChallengeRequest, ReconcileMaterializationRequest,
    ReconcileMaterializationResponse, ResolveHttp01ChallengeRequest, RouteHost, RouteHostKind,
    RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
    TemplateTextPart, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate,
    WorkloadValueFieldRule, WorkloadValueSchema,
};
use k8s_openapi::api::{
    apps::v1::{Deployment, ReplicaSet},
    core::v1::{Pod, Service},
};
use kube::{
    api::{DeleteParams, ListParams, Patch, PatchParams},
    Api, Client,
};
use serde_json::json;
use tokio::time::{sleep, Instant};
use tonic::{
    transport::{Channel, Endpoint},
    Code,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const NORMAL_CLASS_ID: &str = "restart-routing";
const FAST_SLEEP_CLASS_ID: &str = "restart-fast-sleep";
const LATE_CLASS_ID: &str = "restart-late-wake";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const APP_MARKER: &str = "sleepypods-routing-app";
const CONTROL_PLANE_DEPLOYMENT: &str = "sleepypods-control-plane";
const CONTROL_PLANE_LABEL: &str = "app.kubernetes.io/name=sleepypods-control-plane";
const INSTANCE_ID_LABEL: &str = "sleepypods.io/instance-id";
const INSTANCE_GENERATION_LABEL: &str = "sleepypods.io/instance-generation";
const WAKE_HOST: &str = "wake.restart.sleepypods.test";
const SLEEP_HOST: &str = "sleep.restart.sleepypods.test";
const DELETE_HOST: &str = "delete.restart.sleepypods.test";
const REASSIGN_HOST: &str = "reassign.restart.sleepypods.test";
const HTTP01_HOST: &str = "http01.restart.sleepypods.test";
const HTTP01_TOKEN: &str = "restart-token";
const HTTP01_KEY_AUTHORIZATION: &str = "restart-token.key-authorization";
const HTTP01_EXPIRING_TOKEN: &str = "restart-expiring-token";
const HTTP01_EXPIRING_KEY_AUTHORIZATION: &str = "restart-expiring-token.key-authorization";

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-restart.sh or an equivalent kind deployment"]
async fn control_plane_restart_recovery_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!("skipping restart kind E2E because SLEEPYPODS_KIND_E2E_RESTART=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_workload_class(
        &mut operator,
        LATE_CLASS_ID,
        &config.late_app_image,
        &config.sidecar_image,
        sleep_policy(900_000),
        "late",
    )
    .await?;
    create_workload_class(
        &mut operator,
        NORMAL_CLASS_ID,
        &config.app_image,
        &config.sidecar_image,
        sleep_policy(900_000),
        "normal",
    )
    .await?;
    create_workload_class(
        &mut operator,
        FAST_SLEEP_CLASS_ID,
        &config.app_image,
        &config.sidecar_image,
        sleep_policy(5_000),
        "fast-sleep",
    )
    .await?;

    eprintln!("==> restart E2E: wake recovery");
    restart_during_wake_recovers_without_stale_backend(&mut operator, kube.clone(), &config)
        .await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: sleep recovery");
    restart_during_sleep_report_recovers(&mut operator, kube.clone(), &config).await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: delete recovery");
    accepted_delete_survives_restart_without_another_mutation(&mut operator, kube.clone(), &config)
        .await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: HTTP-01 recovery");
    http01_challenges_survive_restart_and_cleanup(&mut operator, kube.clone(), &config).await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: route reassignment recovery");
    route_reassignment_after_restart_does_not_serve_stale_backend(&mut operator, kube, &config)
        .await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    frontline_addr: SocketAddr,
    cluster_name: String,
    app_image: String,
    late_app_image: String,
    sidecar_image: String,
    control_plane_replicas: i32,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: String,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-restart".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19751".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19780".to_owned())
                .parse()?,
            cluster_name: env::var("SLEEPYPODS_KIND_CLUSTER")
                .unwrap_or_else(|_| "sleepypods-e2e-restart-test".to_owned()),
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/routing-app:kind-e2e-restart".to_owned()),
            late_app_image: env::var("SLEEPYPODS_E2E_LATE_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/routing-app-late:kind-e2e-restart".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-restart".to_owned()),
            control_plane_replicas: env::var("SLEEPYPODS_E2E_CONTROL_PLANE_REPLICAS")
                .ok()
                .map(|value| value.parse::<i32>())
                .transpose()?
                .unwrap_or(1),
        })
    }
}

async fn restart_during_wake_recovers_without_stale_backend(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: LATE_CLASS_ID,
            instance_id: "restart-wake",
            route_id: "restart-wake-route",
            host: WAKE_HOST,
            target: "wake",
        },
        "wake",
    )
    .await?;
    let created = wait_for_instance_state(
        operator,
        "restart-wake",
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;

    // Complete the sole wake acceptance before restart. A pending frontend
    // request could otherwise issue another Wake RPC while recovery is checked.
    let mut proxy = ProxyControlPlaneClient::new(
        Endpoint::from_shared(config.operator_endpoint.clone())?
            .connect()
            .await?,
    );
    let accepted = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: "restart-wake".into(),
            expected_generation: created.generation,
            backend_generation: None,
        })
        .await?
        .into_inner();
    if !matches!(
        accepted.outcome,
        Some(proxy_wake_instance_response::Outcome::StillWaking(_))
    ) {
        return Err(
            format!("expected durable wake acceptance before restart, got {accepted:?}").into(),
        );
    }
    drop(proxy);

    let waking = wait_for_instance_state(
        operator,
        "restart-wake",
        PbInstanceState::Waking,
        Duration::from_secs(60),
    )
    .await?;
    if waking.generation <= created.generation {
        return Err(format!(
            "wake interruption did not advance generation beyond {}; got {}",
            created.generation, waking.generation
        )
        .into());
    }

    // Stop every old process, including both replicas in the HA gate, before
    // allowing the accepted wake to recover from durable state alone.
    scale_control_plane(kube.clone(), &config.namespace, 0).await?;
    scale_control_plane(
        kube.clone(),
        &config.namespace,
        config.control_plane_replicas,
    )
    .await?;
    load_late_image_into_kind(config)?;
    delete_workload_pods(kube.clone(), &config.namespace, "restart-wake").await?;
    // An operator channel opened right after the pod restart rides a
    // port-forward that may still target the old terminating control-plane
    // pod and break once it exits, so this wait reconnects on transport
    // errors instead of failing the scenario on a dead channel.
    let running = wait_for_instance_state_reconnecting(
        &config.operator_endpoint,
        "restart-wake",
        PbInstanceState::Running,
        Duration::from_secs(180),
    )
    .await?;
    let expected_running_generation = waking.generation + 1;
    if running.generation != expected_running_generation {
        return Err(format!(
            "autonomous recovery should complete exactly one Waking-to-Running transition from generation {} to {}; got {}",
            waking.generation, expected_running_generation, running.generation
        )
        .into());
    }
    // Wake stamps rendered objects with the projected Running generation
    // (the Waking generation plus one), so the live objects must carry the
    // generation the instance ended at, not the one it woke from.
    assert_workload_generation(kube, &config.namespace, "restart-wake", running.generation).await?;
    let response = http_get(config.frontline_addr, WAKE_HOST, "/").await?;
    assert_instance_response(&response, "HTTP after autonomous wake recovery", "wake")?;

    Ok(())
}

async fn restart_during_sleep_report_recovers(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: FAST_SLEEP_CLASS_ID,
            instance_id: "restart-sleep",
            route_id: "restart-sleep-route",
            host: SLEEP_HOST,
            target: "sleep",
        },
        "sleep",
    )
    .await?;
    eprintln!("==> restart E2E: sleep recovery setup created");
    wait_for_instance_response(
        config,
        "sleep setup wake",
        SLEEP_HOST,
        "/",
        "sleep",
        Duration::from_secs(180),
    )
    .await?;
    wait_for_instance_state(
        operator,
        "restart-sleep",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    eprintln!("==> restart E2E: sleep recovery instance running");

    scale_control_plane(kube.clone(), &config.namespace, 0).await?;
    eprintln!("==> restart E2E: sleep recovery control-plane scaled down");
    sleep(Duration::from_secs(7)).await;
    scale_control_plane(
        kube.clone(),
        &config.namespace,
        config.control_plane_replicas,
    )
    .await?;
    eprintln!("==> restart E2E: sleep recovery control-plane scaled up");
    wait_for_instance_state_reconnecting(
        &config.operator_endpoint,
        "restart-sleep",
        PbInstanceState::Cold,
        sleepypods_api::INITIAL_ACTIVATION_TIMEOUT + Duration::from_secs(180),
    )
    .await?;
    eprintln!("==> restart E2E: sleep recovery instance cold");
    wait_for_workload_absent(
        kube,
        &config.namespace,
        "restart-sleep",
        Duration::from_secs(60),
    )
    .await?;

    Ok(())
}

async fn accepted_delete_survives_restart_without_another_mutation(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: NORMAL_CLASS_ID,
            instance_id: "restart-delete",
            route_id: "restart-delete-route",
            host: DELETE_HOST,
            target: "delete",
        },
        "delete",
    )
    .await?;
    wait_for_instance_response(
        config,
        "delete setup wake",
        DELETE_HOST,
        "/",
        "delete",
        Duration::from_secs(180),
    )
    .await?;
    wait_for_instance_state(
        operator,
        "restart-delete",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;

    let delete_generation = get_instance(operator, "restart-delete").await?.generation;
    let selector = format!("{INSTANCE_ID_LABEL}=restart-delete");
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), &config.namespace);
    let deployment = single_labeled_object(
        "Deployment",
        "restart-delete",
        deployments
            .list(&ListParams::default().labels(&selector))
            .await?,
    )?;
    let materialization_id = deployment
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| {
            annotations.get(control_plane::projection::ANNOTATION_MATERIALIZATION_ID)
        })
        .ok_or("restart-delete workload has no materialization identity")?
        .clone();
    let pods: Api<Pod> = Api::namespaced(kube.clone(), &config.namespace);
    let pod = single_labeled_object(
        "Pod",
        "restart-delete",
        pods.list(&ListParams::default().labels(&selector)).await?,
    )?;
    let pod_name = pod
        .metadata
        .name
        .clone()
        .ok_or("restart-delete Pod has no name")?;
    let pod_uid = pod
        .metadata
        .uid
        .clone()
        .ok_or("restart-delete Pod has no UID")?;
    const FINALIZER: &str = "test.sleepypods.io/restart-delete";
    let mut finalizers = pod.metadata.finalizers.clone().unwrap_or_default();
    if !finalizers.iter().any(|value| value == FINALIZER) {
        finalizers.push(FINALIZER.into());
    }
    let result: TestResult<()> = async {
        // Installation is inside the cleanup scope: even an ambiguous patch
        // reply must lead to an attempt to remove our exact finalizer.
        pods.patch(&pod_name, &PatchParams::default(), &Patch::Merge(json!({"metadata": {
            "uid": pod_uid, "resourceVersion": pod.metadata.resource_version, "finalizers": finalizers,
        }}))).await?;
        let deleted = operator
            .delete_instance(DeleteInstanceRequest {
                expected_generation: Some(delete_generation),
                instance_id: "restart-delete".into(),
            })
            .await?
            .into_inner();
        if !deleted.accepted {
            return Err("delete acceptance was not persisted before restart".into());
        }
        let deleting = wait_for_instance_state(
            operator,
            "restart-delete",
            PbInstanceState::Deleting,
            Duration::from_secs(30),
        )
        .await?;
        if deleting.generation != delete_generation + 1 {
            return Err("delete acceptance must advance generation exactly once".into());
        }
        let before = wait_for_blocked_delete(
            operator,
            &pods,
            &pod_name,
            &pod_uid,
            &materialization_id,
            FINALIZER,
            0,
        )
        .await?;

        // No further Delete or enqueue RPC occurs after this point.
        scale_control_plane(kube.clone(), &config.namespace, 0).await?;
        let stopped_pod = pods.get(&pod_name).await?;
        assert_held_delete_pod(&stopped_pod, &pod_uid, FINALIZER)?;
        scale_control_plane(
            kube.clone(),
            &config.namespace,
            config.control_plane_replicas,
        )
        .await?;
        let after = wait_for_instance_state_reconnecting(
            &config.operator_endpoint,
            "restart-delete",
            PbInstanceState::Deleting,
            Duration::from_secs(30),
        )
        .await?;
        if after.generation != deleting.generation {
            return Err("restart changed accepted deletion generation".into());
        }
        let mut recovered_operator = connect_operator(&config.operator_endpoint).await?;
        // A pass can finish while old replicas drain. Take a fresh restored
        // baseline so the following increment proves a new controller pass.
        let restored = recovered_operator
            .reconcile_materialization(ReconcileMaterializationRequest {
                materialization_id: materialization_id.clone(),
                status_only: true,
            })
            .await?
            .into_inner();
        if !restored.found || restored.failure_count < before.failure_count {
            return Err("restored deletion lost durable failure progress".into());
        }
        let retained = wait_for_blocked_delete(
            &mut recovered_operator,
            &pods,
            &pod_name,
            &pod_uid,
            &materialization_id,
            FINALIZER,
            restored.failure_count,
        )
        .await?;
        if retained.observed_refs != before.observed_refs {
            return Err("blocked restart cleanup lost durable inventory".into());
        }
        release_delete_finalizer(&pods, &pod_name, &pod_uid, FINALIZER).await?;
        assert_instance_not_found(&mut recovered_operator, "restart-delete").await?;
        wait_for_workload_absent(
            kube.clone(),
            &config.namespace,
            "restart-delete",
            Duration::from_secs(60),
        )
        .await?;
        Ok(())
    }
    .await;
    // Always release only this test's finalizer, including on failed assertions.
    let cleanup = release_delete_finalizer(&pods, &pod_name, &pod_uid, FINALIZER).await;
    result.and(cleanup)
}

fn assert_held_delete_pod(pod: &Pod, uid: &str, finalizer: &str) -> TestResult<()> {
    if pod.metadata.uid.as_deref() != Some(uid)
        || pod.metadata.deletion_timestamp.is_none()
        || !pod
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|values| values.iter().any(|value| value == finalizer))
    {
        return Err(
            "blocked cleanup must retain the same terminating Pod and test finalizer".into(),
        );
    }
    Ok(())
}

async fn wait_for_blocked_delete(
    operator: &mut OperatorControlPlaneClient<Channel>,
    pods: &Api<Pod>,
    name: &str,
    uid: &str,
    materialization_id: &str,
    finalizer: &str,
    previous_failures: u32,
) -> TestResult<ReconcileMaterializationResponse> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let pod = pods.get(name).await?;
        let status = operator
            .reconcile_materialization(ReconcileMaterializationRequest {
                materialization_id: materialization_id.into(),
                status_only: true,
            })
            .await?
            .into_inner();
        if status.found
            && status.state == "Deleting"
            && !status.observed_refs.is_empty()
            && status.failure_count > previous_failures
            && status.lease_owner.is_empty()
            && status.uncertain_effect.is_none()
            && pod.metadata.deletion_timestamp.is_some()
        {
            if status.attempted {
                return Err("status-only inspection must not schedule effects".into());
            }
            assert_held_delete_pod(&pod, uid, finalizer)?;
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "no definite blocked cleanup pass before restart deadline: {status:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn release_delete_finalizer(
    pods: &Api<Pod>,
    name: &str,
    uid: &str,
    finalizer: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let Some(pod) = pods.get_opt(name).await? else {
            return Ok(());
        };
        if pod.metadata.uid.as_deref() != Some(uid) {
            return Err("refusing to change a replacement Pod's finalizers".into());
        }
        let mut finalizers = pod.metadata.finalizers.clone().unwrap_or_default();
        if !finalizers.iter().any(|value| value == finalizer) {
            return Ok(());
        }
        finalizers.retain(|value| value != finalizer);
        match pods.patch(name, &PatchParams::default(), &Patch::Merge(json!({"metadata": {
            "uid": uid, "resourceVersion": pod.metadata.resource_version, "finalizers": finalizers,
        }}))).await {
            Ok(_) => return Ok(()),
            Err(kube::Error::Api(error)) if error.code == 409 && Instant::now() < deadline => {
                sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn route_reassignment_after_restart_does_not_serve_stale_backend(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "restart-reassign-old",
        "old",
        "reassign-old",
    )
    .await?;
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "restart-reassign-new",
        "new",
        "reassign-new",
    )
    .await?;
    create_route(
        operator,
        "restart-reassign-old-route",
        "restart-reassign-old",
        REASSIGN_HOST,
        "reassign-old",
    )
    .await?;
    let aliases = [
        (
            "restart-reassign-old-alias-route",
            "restart-reassign-old",
            "old-setup.restart.sleepypods.test",
            "old",
        ),
        (
            "restart-reassign-new-alias-route",
            "restart-reassign-new",
            "new-setup.restart.sleepypods.test",
            "new",
        ),
    ];
    for (route_id, instance_id, host, _) in aliases {
        create_route(operator, route_id, instance_id, host, route_id).await?;
    }
    // Prewarm both backends without resolving REASSIGN_HOST through the
    // frontend. Its first later lookup will have a fresh positive cache TTL.
    let mut proxy = ProxyControlPlaneClient::new(
        Endpoint::from_shared(config.operator_endpoint.clone())?
            .connect()
            .await?,
    );
    for instance_id in ["restart-reassign-old", "restart-reassign-new"] {
        let instance = get_instance(operator, instance_id).await?;
        let wake = proxy
            .wake_instance(ProxyWakeInstanceRequest {
                instance_id: instance_id.into(),
                expected_generation: instance.generation,
                backend_generation: None,
            })
            .await?
            .into_inner();
        if !matches!(
            wake.outcome,
            Some(
                proxy_wake_instance_response::Outcome::StillWaking(_)
                    | proxy_wake_instance_response::Outcome::Ready(_)
            )
        ) {
            return Err(format!("reassignment prewarm was not accepted: {wake:?}").into());
        }
        wait_for_instance_state(
            operator,
            instance_id,
            PbInstanceState::Running,
            Duration::from_secs(180),
        )
        .await?;
    }
    drop(proxy);

    restart_control_plane_pod(kube, &config.namespace, &config.operator_endpoint).await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    // Running does not establish a frontend subscription or Service connection.
    // Complete both paths through distinct exact identities before starting the
    // untouched proof key's cache-age clock. One request per alias; any failure
    // is fatal. These aliases cannot populate a different exact cache key.
    for (_, _, host, target) in aliases {
        let response = tokio::time::timeout(
            Duration::from_secs(140),
            http_get_with_timeout(config.frontline_addr, host, "/", Duration::from_secs(130)),
        )
        .await
        .map_err(|_| {
            format!("reassignment alias setup for {host} exceeded its readiness/setup budget")
        })??;
        assert_instance_response(&response, "post-restart alias setup", target)?;
    }
    // This fixture is idle: allow the four 250ms dispatcher ticks following
    // restart to settle before populating the never-before-resolved cache key.
    sleep(Duration::from_secs(1)).await;
    let cached_at = Instant::now();
    let old = tokio::time::timeout(
        Duration::from_secs(1),
        http_get_with_timeout(
            config.frontline_addr,
            REASSIGN_HOST,
            "/",
            Duration::from_secs(1),
        ),
    )
    .await
    .map_err(|_| "initial cache warmup exceeded total 1s fixture budget")??;
    assert_instance_response(&old, "fresh pre-cutover cache entry", "old")?;

    // Bound mutation setup too, keeping the cache entry younger than the
    // notification deadline. No request runs between these commits.
    tokio::time::timeout(Duration::from_secs(1), async {
        let deleted = operator
            .delete_route_binding(DeleteRouteBindingRequest {
                route_binding_id: "restart-reassign-old-route".to_owned(),
            })
            .await?
            .into_inner();
        if !deleted.deleted {
            return Err("route reassignment did not delete the old route binding".into());
        }
        create_route(
            &mut operator,
            "restart-reassign-new-route",
            "restart-reassign-new",
            REASSIGN_HOST,
            "reassign-new",
        )
        .await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| "healthy route cutover commits exceeded 1s fixture budget")??;

    let delivery_started = Instant::now();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = http_get_with_timeout(
                config.frontline_addr,
                REASSIGN_HOST,
                "/",
                Duration::from_millis(500),
            )
            .await?;
            if response.status != 200 {
                return Err(
                    format!("healthy reassignment returned HTTP {}", response.status).into(),
                );
            }
            if response_body_identifies(&response, "new") {
                assert_instance_response(&response, "notification convergence", "new")?;
                break;
            }
            if !response_body_identifies(&response, "old") {
                return Err("reassignment served an unknown backend".into());
            }
            assert_instance_response(&response, "notification delivery in progress", "old")?;
            // An old cache result is allowed only during bounded notification
            // delivery; this is not an instantaneous cutover contract.
            sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| {
        "route notification did not converge within 3s, well inside the positive cache TTL"
    })??;
    // Convergence has to beat the positive cache TTL by a wide margin, so that
    // what this proves is notification delivery rather than an entry aging out.
    if cached_at.elapsed() >= Duration::from_secs(5) {
        return Err("route freshness proof exceeded its pre-TTL fixture budget".into());
    }
    eprintln!(
        "route reassignment converged through notification in {:?}",
        delivery_started.elapsed()
    );

    // Once freshness is observed, every subsequent response must remain fresh.
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..10 {
            let response = http_get_with_timeout(
                config.frontline_addr,
                REASSIGN_HOST,
                "/",
                Duration::from_millis(500),
            )
            .await?;
            assert_instance_response(&response, "after route freshness observed", "new")?;
            sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| "post-freshness checks exceeded 2s fixture budget")??;

    Ok(())
}

async fn http01_challenges_survive_restart_and_cleanup(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: NORMAL_CLASS_ID,
            instance_id: "restart-http01",
            route_id: "restart-http01-route",
            host: HTTP01_HOST,
            target: "http01",
        },
        "http01",
    )
    .await?;
    wait_for_instance_response(
        config,
        "HTTP-01 normal route setup",
        HTTP01_HOST,
        "/",
        "http01",
        Duration::from_secs(180),
    )
    .await?;

    put_http01_challenge(
        operator,
        HTTP01_HOST,
        HTTP01_TOKEN,
        HTTP01_KEY_AUTHORIZATION,
        SystemTime::now() + Duration::from_secs(90),
    )
    .await?;

    restart_control_plane_pod(kube.clone(), &config.namespace, &config.operator_endpoint).await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let resolved = ProxyControlPlaneClient::connect(config.operator_endpoint.clone())
        .await?
        .resolve_http01_challenge(ResolveHttp01ChallengeRequest {
            key: Some(http01_key(HTTP01_HOST, HTTP01_TOKEN)),
        })
        .await?
        .into_inner()
        .challenge
        .ok_or("HTTP-01 challenge did not resolve after control-plane restart")?;
    if resolved.key_authorization != HTTP01_KEY_AUTHORIZATION {
        return Err(format!(
            "resolved key authorization {:?}, expected {:?}",
            resolved.key_authorization, HTTP01_KEY_AUTHORIZATION
        )
        .into());
    }

    let inserted_challenge_path = challenge_path(HTTP01_TOKEN);
    let challenge = wait_for_http01_response(
        config,
        "HTTP-01 inserted challenge after restart",
        HTTP01_HOST,
        &inserted_challenge_path,
        HTTP01_KEY_AUTHORIZATION,
        Duration::from_secs(30),
    )
    .await?;
    assert_http01_content_type(&challenge, "inserted challenge after restart")?;

    let deleted = operator
        .delete_http01_challenge(DeleteHttp01ChallengeRequest {
            key: Some(http01_key(HTTP01_HOST, HTTP01_TOKEN)),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("expected HTTP-01 delete after restart to remove challenge".into());
    }
    let after_delete = wait_for_status_without_app(
        config,
        "HTTP-01 deleted challenge after restart",
        HTTP01_HOST,
        &inserted_challenge_path,
        404,
        Duration::from_secs(30),
    )
    .await?;
    if after_delete.body.contains(HTTP01_KEY_AUTHORIZATION) {
        return Err("deleted HTTP-01 key authorization was still served after restart".into());
    }

    put_http01_challenge(
        &mut operator,
        HTTP01_HOST,
        HTTP01_EXPIRING_TOKEN,
        HTTP01_EXPIRING_KEY_AUTHORIZATION,
        SystemTime::now() + Duration::from_secs(90),
    )
    .await?;
    restart_control_plane_pod(kube, &config.namespace, &config.operator_endpoint).await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let expiring_path = challenge_path(HTTP01_EXPIRING_TOKEN);
    wait_for_http01_response(
        config,
        "HTTP-01 expiring challenge after restart",
        HTTP01_HOST,
        &expiring_path,
        HTTP01_EXPIRING_KEY_AUTHORIZATION,
        Duration::from_secs(30),
    )
    .await?;

    // Survival and expiry have separate deadlines: restart may legitimately
    // take longer than the short expiry window. Renew the same persisted token
    // after recovery, then prove the new authoritative expiry is honored.
    let renewed_expiry = SystemTime::now() + Duration::from_secs(5);
    put_http01_challenge(
        &mut operator,
        HTTP01_HOST,
        HTTP01_EXPIRING_TOKEN,
        HTTP01_EXPIRING_KEY_AUTHORIZATION,
        renewed_expiry,
    )
    .await?;
    let renewed = ProxyControlPlaneClient::connect(config.operator_endpoint.clone())
        .await?
        .resolve_http01_challenge(ResolveHttp01ChallengeRequest {
            key: Some(http01_key(HTTP01_HOST, HTTP01_EXPIRING_TOKEN)),
        })
        .await?
        .into_inner()
        .challenge
        .ok_or("renewed HTTP-01 token is missing")?;
    if renewed.expires_at_unix_millis != unix_millis(renewed_expiry)?
        || renewed.key_authorization != HTTP01_EXPIRING_KEY_AUTHORIZATION
    {
        return Err("HTTP-01 renewal did not persist its exact value and deadline".into());
    }
    sleep(Duration::from_secs(6)).await;
    let after_expiry = wait_for_status_without_app(
        config,
        "HTTP-01 expiring challenge after expiry",
        HTTP01_HOST,
        &expiring_path,
        404,
        Duration::from_secs(30),
    )
    .await?;
    if after_expiry
        .body
        .contains(HTTP01_EXPIRING_KEY_AUTHORIZATION)
    {
        return Err("expired HTTP-01 key authorization was still served after restart".into());
    }
    if ProxyControlPlaneClient::connect(config.operator_endpoint.clone())
        .await?
        .resolve_http01_challenge(ResolveHttp01ChallengeRequest {
            key: Some(http01_key(HTTP01_HOST, HTTP01_EXPIRING_TOKEN)),
        })
        .await?
        .into_inner()
        .challenge
        .is_some()
    {
        return Err("expired HTTP-01 token still resolves authoritatively".into());
    }
    // Maintenance may already have collected the row. The manual pass is
    // idempotent; authoritative absence, not which worker deleted it, matters.
    operator
        .expire_http01_challenges(ExpireHttp01ChallengesRequest {
            now_unix_millis: unix_millis(SystemTime::now())?,
            limit: Some(10),
        })
        .await?
        .into_inner();
    if operator
        .delete_http01_challenge(DeleteHttp01ChallengeRequest {
            key: Some(http01_key(HTTP01_HOST, HTTP01_EXPIRING_TOKEN)),
        })
        .await?
        .into_inner()
        .deleted
    {
        return Err("expired HTTP-01 row remained after bounded cleanup".into());
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct InstanceRouteSpec<'a> {
    class_id: &'a str,
    instance_id: &'a str,
    route_id: &'a str,
    host: &'a str,
    target: &'a str,
}

async fn create_workload_class(
    operator: &mut OperatorControlPlaneClient<Channel>,
    class_id: &str,
    app_image: &str,
    sidecar_image: &str,
    sleep_policy: WorkloadSleepPolicy,
    idempotency_suffix: &str,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: format!("kind-e2e-restart-{idempotency_suffix}-class"),
            class_id: class_id.to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: HashMap::from([(
                    "target".to_owned(),
                    WorkloadValueFieldRule {
                        required: true,
                        default_value: None,
                    },
                )]),
                allow_extra: false,
            }),
            template_generation: 1,
            template: Some(manifest_template(app_image, sidecar_image)),
            sleep_policy: Some(sleep_policy),
            exclusivity_keys: vec![],
        })
        .await?;

    Ok(())
}

async fn create_instance_and_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    spec: InstanceRouteSpec<'_>,
    idempotency_suffix: &str,
) -> TestResult<()> {
    create_instance(
        operator,
        spec.class_id,
        spec.instance_id,
        spec.target,
        idempotency_suffix,
    )
    .await?;
    create_route(
        operator,
        spec.route_id,
        spec.instance_id,
        spec.host,
        idempotency_suffix,
    )
    .await
}

async fn create_instance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    class_id: &str,
    instance_id: &str,
    target: &str,
    idempotency_suffix: &str,
) -> TestResult<()> {
    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: format!("kind-e2e-restart-{idempotency_suffix}-instance"),
            instance_id: instance_id.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: class_id.to_owned(),
                version: 1,
            }),
            values: HashMap::from([("target".to_owned(), target.to_owned())]),
        })
        .await?;

    Ok(())
}

async fn create_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    route_id: &str,
    instance_id: &str,
    host: &str,
    idempotency_suffix: &str,
) -> TestResult<()> {
    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: format!("kind-e2e-restart-{idempotency_suffix}-route"),
            route_binding_id: route_id.to_owned(),
            instance_id: instance_id.to_owned(),
            identity: Some(http_route_identity(host)),
            protocol: ProtocolRoute::Http as i32,
        })
        .await?;

    Ok(())
}

async fn connect_operator(endpoint: &str) -> TestResult<OperatorControlPlaneClient<Channel>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let channel = Endpoint::from_shared(endpoint.to_owned())?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        match channel {
            Ok(channel) => {
                let mut client = OperatorControlPlaneClient::new(channel);
                match probe_operator(&mut client).await {
                    Ok(()) => {
                        sleep(Duration::from_millis(500)).await;
                        match probe_operator(&mut client).await {
                            Ok(()) => return Ok(client),
                            Err(error) if Instant::now() < deadline => {
                                eprintln!(
                                    "waiting for stable operator gRPC endpoint {endpoint}: {error}"
                                );
                                sleep(Duration::from_secs(1)).await;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    Err(error) if Instant::now() < deadline => {
                        eprintln!("waiting for stable operator gRPC endpoint {endpoint}: {error}");
                        sleep(Duration::from_secs(1)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for operator gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn probe_operator(
    client: &mut OperatorControlPlaneClient<Channel>,
) -> Result<(), tonic::Status> {
    match client
        .get_instance(GetInstanceRequest {
            instance_id: "connectivity-probe".to_owned(),
        })
        .await
    {
        Err(status) if status.code() == Code::NotFound => Ok(()),
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn get_instance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<Instance> {
    Ok(operator
        .get_instance(GetInstanceRequest {
            instance_id: instance_id.to_owned(),
        })
        .await?
        .into_inner())
}

async fn assert_instance_not_found(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match operator
            .get_instance(GetInstanceRequest {
                instance_id: instance_id.to_owned(),
            })
            .await
        {
            Err(status) if status.code() == Code::NotFound => return Ok(()),
            Ok(_) if Instant::now() < deadline => sleep(Duration::from_millis(100)).await,
            other => return Err(format!("instance cleanup did not complete: {other:?}").into()),
        }
    }
}

async fn wait_for_instance_state(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let instance = get_instance(operator, instance_id).await?;
        let actual = instance_state(&instance);
        if actual == expected {
            return Ok(instance);
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for instance {instance_id} to reach {expected:?}; last state was {actual:?} generation {}",
                instance.generation
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_instance_state_reconnecting(
    operator_endpoint: &str,
    instance_id: &str,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut operator = connect_operator(operator_endpoint).await?;
        match get_instance(&mut operator, instance_id).await {
            Ok(instance) => {
                let actual = instance_state(&instance);
                if actual == expected {
                    return Ok(instance);
                }

                if Instant::now() >= deadline {
                    return Err(format!(
                        "timed out waiting for instance {instance_id} to reach {expected:?}; last state was {actual:?} generation {}",
                        instance.generation
                    )
                    .into());
                }
            }
            Err(error) if Instant::now() < deadline => {
                eprintln!(
                    "waiting for instance {instance_id} after reconnectable operator error: {error}"
                );
            }
            Err(error) => return Err(error),
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn instance_state(instance: &Instance) -> PbInstanceState {
    PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified)
}

async fn restart_control_plane_pod(
    kube: Client,
    namespace: &str,
    operator_endpoint: &str,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let original = pods
        .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
        .await?;
    let original_uids = original
        .iter()
        .filter_map(|pod| pod.metadata.uid.clone())
        .collect::<Vec<_>>();
    let selected = original
        .into_iter()
        .find(pod_ready)
        .ok_or("no control-plane pod found to restart")?;
    let old_name = selected
        .metadata
        .name
        .clone()
        .ok_or("control-plane pod is missing name")?;
    let old_uid = selected
        .metadata
        .uid
        .clone()
        .ok_or("control-plane pod is missing UID")?;

    pods.delete(&old_name, &DeleteParams::default()).await?;
    wait_for_replacement_control_plane_pod(
        pods,
        &old_uid,
        &original_uids,
        Duration::from_secs(120),
    )
    .await?;
    connect_operator(operator_endpoint).await?;

    Ok(())
}

async fn scale_control_plane(kube: Client, namespace: &str, replicas: i32) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    deployments
        .patch(
            CONTROL_PLANE_DEPLOYMENT,
            &PatchParams::default(),
            &Patch::Merge(json!({ "spec": { "replicas": replicas } })),
        )
        .await?;
    wait_for_control_plane_ready_replicas(
        kube,
        namespace,
        replicas as usize,
        Duration::from_secs(120),
    )
    .await
}

async fn wait_for_replacement_control_plane_pod(
    pods: Api<Pod>,
    old_uid: &str,
    original_uids: &[String],
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let listed = pods
            .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
            .await?;
        let old_absent = !listed
            .iter()
            .any(|pod| pod.metadata.uid.as_deref() == Some(old_uid));
        let replacement_ready = listed.iter().any(|pod| {
            pod_ready(pod)
                && pod
                    .metadata
                    .uid
                    .as_ref()
                    .is_some_and(|uid| !original_uids.contains(uid))
        });
        if old_absent && replacement_ready {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err("timed out waiting for replacement control-plane pod".into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_control_plane_ready_replicas(
    kube: Client,
    namespace: &str,
    expected: usize,
    timeout: Duration,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        let listed = pods
            .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
            .await?;
        let ready = listed.iter().filter(|pod| pod_ready(pod)).count();
        if expected == 0 {
            if listed.items.is_empty() {
                return Ok(());
            }
        } else if ready == expected {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {expected} ready control-plane replicas; got {ready}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn pod_ready(pod: &Pod) -> bool {
    if pod.metadata.deletion_timestamp.is_some() {
        return false;
    }

    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn load_late_image_into_kind(config: &E2eConfig) -> TestResult<()> {
    let status = Command::new("kind")
        .args([
            "load",
            "docker-image",
            config.late_app_image.as_str(),
            "--name",
            config.cluster_name.as_str(),
        ])
        .status()?;
    if !status.success() {
        return Err(format!(
            "kind load docker-image {} --name {} failed with status {status}",
            config.late_app_image, config.cluster_name
        )
        .into());
    }

    Ok(())
}

async fn delete_workload_pods(kube: Client, namespace: &str, instance_id: &str) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let selector = format!("{INSTANCE_ID_LABEL}={instance_id}");
    for pod in pods.list(&ListParams::default().labels(&selector)).await? {
        if let Some(name) = pod.metadata.name {
            let _ = pods.delete(&name, &DeleteParams::default()).await;
        }
    }

    Ok(())
}

async fn assert_workload_generation(
    kube: Client,
    namespace: &str,
    instance_id: &str,
    generation: u64,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let selector = format!("{INSTANCE_ID_LABEL}={instance_id}");
    let expected = generation.to_string();
    let deployment = single_labeled_object(
        "Deployment",
        instance_id,
        deployments
            .list(&ListParams::default().labels(&selector))
            .await?,
    )?;
    let service = single_labeled_object(
        "Service",
        instance_id,
        services
            .list(&ListParams::default().labels(&selector))
            .await?,
    )?;
    assert_object_generation_label(
        "Deployment",
        instance_id,
        &deployment.metadata.labels,
        &expected,
    )?;
    assert_object_generation_label("Service", instance_id, &service.metadata.labels, &expected)?;

    Ok(())
}

fn single_labeled_object<K: Clone>(
    kind: &str,
    instance_id: &str,
    objects: kube::api::ObjectList<K>,
) -> TestResult<K> {
    let mut items = objects.items.into_iter();
    let object = items
        .next()
        .ok_or_else(|| format!("no {kind} found for instance {instance_id}"))?;
    if items.next().is_some() {
        return Err(format!("multiple {kind} objects found for instance {instance_id}").into());
    }

    Ok(object)
}

fn assert_object_generation_label(
    kind: &str,
    name: &str,
    labels: &Option<std::collections::BTreeMap<String, String>>,
    expected: &str,
) -> TestResult<()> {
    let actual = labels
        .as_ref()
        .and_then(|labels| labels.get(INSTANCE_GENERATION_LABEL))
        .ok_or_else(|| format!("{kind} {name} is missing {INSTANCE_GENERATION_LABEL} label"))?;
    if actual != expected {
        return Err(format!(
            "{kind} {name} generation label was {actual:?}, expected {expected:?}"
        )
        .into());
    }

    Ok(())
}

async fn wait_for_workload_absent(
    kube: Client,
    namespace: &str,
    instance_id: &str,
    timeout: Duration,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pods: Api<Pod> = Api::namespaced(kube.clone(), namespace);
    let replica_sets: Api<ReplicaSet> = Api::namespaced(kube, namespace);
    let selector = format!("{INSTANCE_ID_LABEL}={instance_id}");
    let deadline = Instant::now() + timeout;
    loop {
        let deployment_absent = deployments
            .list(&ListParams::default().labels(&selector))
            .await?
            .items
            .is_empty();
        let service_absent = services
            .list(&ListParams::default().labels(&selector))
            .await?
            .items
            .is_empty();
        let pods_absent = pods
            .list(&ListParams::default().labels(&selector))
            .await?
            .items
            .is_empty();
        let replica_sets_absent = replica_sets
            .list(&ListParams::default().labels(&selector))
            .await?
            .items
            .is_empty();
        if deployment_absent && service_absent && pods_absent && replica_sets_absent {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for workload objects in {namespace} for instance {instance_id} to be deleted"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_instance_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    wait_for_instance_response_rejecting_stale(config, context, host, path, target, &[], timeout)
        .await
}

async fn wait_for_instance_response_rejecting_stale(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    stale_targets: &[&str],
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response)
                if response.status == 200 && response_body_identifies(&response, target) =>
            {
                assert_instance_response(&response, context, target)?;
                return Ok(response);
            }
            Ok(response) => {
                for stale in stale_targets {
                    if response.status == 200 && response_body_identifies(&response, stale) {
                        return Err(format!(
                            "{context} served stale target {stale:?} after restart/reassignment: {:?}",
                            response.body
                        )
                        .into());
                    }
                }
                format!(
                    "frontline returned HTTP {} with body {:?}",
                    response.status, response.body
                )
            }
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for successful frontend response for {context} host {host} path {path}: {last_error}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_status_without_app(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    expected_status: u16,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == expected_status => {
                if response.body.contains(APP_MARKER) {
                    return Err(format!(
                        "{context} unexpectedly reached an app body for host {host} path {path}: {:?}",
                        response.body
                    )
                    .into());
                }
                return Ok(response);
            }
            Ok(response) => format!(
                "frontline returned HTTP {} with body {:?}",
                response.status, response.body
            ),
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for HTTP {expected_status} for {context} host {host} path {path}: {last_error}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn put_http01_challenge(
    operator: &mut OperatorControlPlaneClient<Channel>,
    host: &str,
    token: &str,
    key_authorization: &str,
    expires_at: SystemTime,
) -> TestResult<()> {
    operator
        .put_http01_challenge(PutHttp01ChallengeRequest {
            key: Some(http01_key(host, token)),
            key_authorization: key_authorization.to_owned(),
            expires_at_unix_millis: unix_millis(expires_at)?,
        })
        .await?;
    Ok(())
}

async fn wait_for_http01_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    expected_body: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == 200 && response.body == expected_body => {
                return Ok(response);
            }
            Ok(response) => format!(
                "frontline returned HTTP {} with body {:?}",
                response.status, response.body
            ),
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {context} at {path}: {last_error}").into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn http_get(addr: SocketAddr, host: &str, path: &str) -> TestResult<HttpResponse> {
    http_get_with_timeout(addr, host, path, Duration::from_secs(180)).await
}

async fn http_get_with_timeout(
    addr: SocketAddr,
    host: &str,
    path: &str,
    request_timeout: Duration,
) -> TestResult<HttpResponse> {
    let response = http_once::get_once(addr, host, path, request_timeout).await?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| Ok((name.as_str().to_owned(), value.to_str()?.to_owned())))
        .collect::<Result<_, http::header::ToStrError>>()?;
    Ok(HttpResponse {
        status,
        headers,
        body: response.into_body(),
    })
}

fn assert_instance_response(
    response: &HttpResponse,
    context: &str,
    target: &str,
) -> TestResult<()> {
    if response.status != 200 {
        return Err(format!("{context} returned HTTP {}", response.status).into());
    }
    if !response.body.contains(APP_MARKER) {
        return Err(format!(
            "{context} body did not include routing app marker: {:?}",
            response.body
        )
        .into());
    }
    if !response_body_identifies(response, target) {
        return Err(format!(
            "{context} body did not identify target {target:?}: {:?}",
            response.body
        )
        .into());
    }

    Ok(())
}

fn response_body_identifies(response: &HttpResponse, target: &str) -> bool {
    response.body.contains(&format!("instance={target}\n"))
}

fn assert_http01_content_type(response: &HttpResponse, context: &str) -> TestResult<()> {
    let content_type = response
        .headers
        .get("content-type")
        .ok_or_else(|| format!("{context} HTTP-01 response is missing content-type"))?;
    if content_type != "text/plain" {
        return Err(format!(
            "{context} HTTP-01 content-type was {content_type:?}, expected \"text/plain\""
        )
        .into());
    }
    Ok(())
}

fn manifest_template(app_image: &str, sidecar_image: &str) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(target_text("restart-", "")),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "app".to_owned(),
                image: Some(literal_text(app_image)),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: APP_PORT,
                }],
                env: vec![EnvVarTemplate {
                    name: "SLEEPYPODS_E2E_INSTANCE".to_owned(),
                    value: Some(target_text("", "")),
                }],
            }),
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
        }),
        service: Some(ServiceTemplate {
            name: Some(target_text("restart-", "")),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: APP_PORT,
                target_port: APP_PORT,
            }],
        }),
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn http_route_identity(host: &str) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
            host: Some(RouteHost {
                kind: RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
            path_prefix: None,
        })),
    }
}

fn sleep_policy(idle_timeout_ms: u64) -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms,
        idle_retry_backoff_ms: 500,
        drain_grace_timeout_ms: 500,
        idle_timeout_override: None,
    }
}

fn http01_key(host: &str, token: &str) -> Http01ChallengeKey {
    Http01ChallengeKey {
        host: host.to_owned(),
        token: token.to_owned(),
    }
}

fn challenge_path(token: &str) -> String {
    format!("/.well-known/acme-challenge/{token}")
}

fn unix_millis(time: SystemTime) -> TestResult<i64> {
    Ok(i64::try_from(time.duration_since(UNIX_EPOCH)?.as_millis())?)
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}

fn target_text(prefix: &str, suffix: &str) -> TemplateText {
    TemplateText {
        parts: vec![
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(prefix.to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue("target".to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(suffix.to_owned())),
            },
        ],
    }
}

#[tokio::test]
async fn restart_framed_response_returns_before_eof_and_preserves_headers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (status, body) in [(200, "complete routing response\n"), (502, "bad gateway\n")] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut buffer = [0; 1024];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
                assert!(request.len() <= 1024);
            }
            assert!(request.starts_with(b"GET /proof HTTP/1.1\r\n"));
            let response = format!(
                "HTTP/1.0 {status} Test\r\nContent-Length: {}\r\nX-Proof: preserved\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            // EOF cannot arrive until the caller has obtained the full response.
            let _ = tokio::time::timeout(Duration::from_secs(1), hold).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
        });
        let response =
            http_get_with_timeout(address, "proof.test", "/proof", Duration::from_millis(100))
                .await;
        let _ = release.send(());
        server.await.unwrap();
        let response = response.expect("a complete framed response does not require EOF");
        assert_eq!(response.status, status);
        assert_eq!(response.body, body);
        assert_eq!(
            response.headers.get("x-proof").map(String::as_str),
            Some("preserved")
        );
    }
}
