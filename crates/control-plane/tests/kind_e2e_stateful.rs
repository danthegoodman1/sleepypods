#[path = "support/http_once.rs"]
mod http_once;

use std::{
    collections::{BTreeMap, HashMap},
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, persistent_volume_source_template,
    route_identity, template_text_part, ContainerPortTemplate, ContainerTemplate,
    CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    DeleteInstanceRequest, GetInstanceRequest, HostPathVolumeSourceTemplate, HttpRouteIdentity,
    Instance, InstanceState as PbInstanceState, ManifestTemplate, PersistentVolumeAccessMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, ProjectionObservation,
    ProtocolRoute, ReconcileMaterializationRequest, ReconcileMaterializationResponse, RouteHost,
    RouteHostKind, RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
    TemplateText, TemplateTextPart, VolumeTemplate, WorkloadClassVersionRef,
    WorkloadExclusivityKey, WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate,
    WorkloadValueFieldRule, WorkloadValueSchema,
};
use control_plane::projection::{LABEL_MANAGED_BY, LABEL_MANAGED_BY_VALUE};
use control_plane::{render_instance_scoped_name, InstanceId};
use k8s_openapi::{
    api::{
        apps::v1::StatefulSet,
        core::v1::{
            PersistentVolume, PersistentVolumeClaim, Pod, Service, ServicePort, ServiceSpec,
        },
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{
    api::{DeleteParams, ListParams, ObjectMeta, Patch, PatchParams, PostParams},
    Api, Client, Error as KubeError,
};
use serde_json::json;
use tokio::time::{sleep, Instant};
use tonic::{
    transport::{Channel, Endpoint},
    Code,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLASS_ID: &str = "stateful-web";
const INSTANCE_ID: &str = "e2e-stateful";
const ROUTE_ID: &str = "e2e-stateful-route";
const ROUTE_HOST: &str = "stateful.sleepypods.test";
const TENANT_VALUE: &str = "stateful";
const WORKLOAD_NAME: &str = "e2e-stateful-app";
// Expected suffix: first eight SHA-256 hex digits of the complete INSTANCE_ID.
const RENDERED_WORKLOAD_NAME: &str = "e2e-stateful-app-1b49ed6c883bce91";
const RENDERED_PVC_NAME: &str = "e2e-stateful-pvc-1b49ed6c883bce91";
const RENDERED_PV_NAME: &str = "e2e-stateful-pv-1b49ed6c883bce91";
const VOLUME_NAME: &str = "data";
const MOUNT_PATH: &str = "/data";
const CLUSTER_ID: &str = "kind-e2e-stateful";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const STATEFUL_CLEANUP_TIMEOUT: Duration = Duration::from_secs(180);
const CONTROL_PLANE_LABEL: &str = "app.kubernetes.io/name=sleepypods-control-plane";
const PROJECTION_DRIFT_FINALIZER: &str = "sleepypods.io/kind-e2e-projection-drift-blocker";
const EXCLUSIVE_CLASS_ID: &str = "stateful-exclusive";
const EXCLUSIVE_OWNER_INSTANCE_ID: &str = "m12ownera";
const EXCLUSIVE_BLOCKED_INSTANCE_ID: &str = "m12blockb";
const EXCLUSIVE_OTHER_INSTANCE_ID: &str = "m12otherc";
const EXCLUSIVE_OWNER_ROUTE_ID: &str = "m12-owner-route";
const EXCLUSIVE_BLOCKED_ROUTE_ID: &str = "m12-blocked-route";
const EXCLUSIVE_OTHER_ROUTE_ID: &str = "m12-other-route";
const EXCLUSIVE_OWNER_HOST: &str = "m12-owner.sleepypods.test";
const EXCLUSIVE_BLOCKED_HOST: &str = "m12-blocked.sleepypods.test";
const EXCLUSIVE_OTHER_HOST: &str = "m12-other.sleepypods.test";
const EXCLUSIVE_OWNER_TENANT: &str = "m12-owner";
const EXCLUSIVE_BLOCKED_TENANT: &str = "m12-blocked";
const EXCLUSIVE_OTHER_TENANT: &str = "m12-other";
const EXCLUSIVE_SHARED_HANDLE: &str = "opaque-shared-handle";
const EXCLUSIVE_OTHER_HANDLE: &str = "opaque-other-handle";

#[test]
fn rendered_name_constants_match_instance_scoped_naming() {
    let instance_id = InstanceId::new(INSTANCE_ID).unwrap();
    for (constant, base) in [
        (RENDERED_WORKLOAD_NAME, WORKLOAD_NAME),
        (RENDERED_PVC_NAME, "e2e-stateful-pvc"),
        (RENDERED_PV_NAME, "e2e-stateful-pv"),
    ] {
        assert_eq!(
            constant,
            render_instance_scoped_name(base, &instance_id),
            "rendered name constant no longer matches the control plane's instance-scoped naming"
        );
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-stateful.sh or an equivalent kind deployment"]
async fn stateful_volume_lifecycle_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_STATEFUL").as_deref() != Ok("1") {
        eprintln!("skipping stateful kind E2E because SLEEPYPODS_KIND_E2E_STATEFUL=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let marker = unique_marker()?;

    assert_single_node_cluster(kube.clone()).await?;
    create_operator_resources(&mut operator, &config).await?;
    let created = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    assert_eq!(created.generation, 0);

    let write_path = format!("/write/{marker}");
    eprintln!("stateful E2E: cold write through frontline");
    // Do not replay the cold write: the first request must survive the readiness wait.
    let first = tokio::time::timeout(
        Duration::from_secs(140),
        http_get(config.frontline_addr, ROUTE_HOST, &write_path),
    )
    .await??;
    assert_response(&first, "cold write request", "wrote:")?;
    let running = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    if running.generation <= created.generation {
        return Err(format!(
            "expected wake to advance generation beyond {}, got {}",
            created.generation, running.generation
        )
        .into());
    }
    assert_materialized_stateful_objects(kube.clone(), &config).await?;

    let read_path = format!("/read/{marker}");
    eprintln!("stateful E2E: mounted read through warm frontend path");
    let second = wait_for_frontline_response(
        &config,
        "mounted read request",
        &read_path,
        "read:",
        Duration::from_secs(30),
    )
    .await?;
    assert_response(&second, "hot read request", "read:")?;

    let cold_after_idle = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        sleepypods_api::INITIAL_ACTIVATION_TIMEOUT + STATEFUL_CLEANUP_TIMEOUT,
    )
    .await?;
    if cold_after_idle.generation <= running.generation {
        return Err(format!(
            "expected idle sleep to advance generation beyond {}, got {}",
            running.generation, cold_after_idle.generation
        )
        .into());
    }
    wait_for_materialized_objects_deleted(
        kube.clone(),
        &config.namespace,
        STATEFUL_CLEANUP_TIMEOUT,
    )
    .await?;

    sleep(Duration::from_secs(11)).await;
    eprintln!("stateful E2E: single re-wake read through frontline");
    let rewake = tokio::time::timeout(
        Duration::from_secs(140),
        http_get_once(
            config.frontline_addr,
            ROUTE_HOST,
            &read_path,
            Duration::from_secs(130),
        ),
    )
    .await??;
    assert_response(&rewake, "re-wake read request", &format!("read:{marker}\n"))?;
    let running_after_rewake = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    if running_after_rewake.generation <= cold_after_idle.generation {
        return Err(format!(
            "expected re-wake to advance generation beyond {}, got {}",
            cold_after_idle.generation, running_after_rewake.generation
        )
        .into());
    }
    assert_materialized_stateful_objects(kube.clone(), &config).await?;

    let deleted = operator
        .delete_instance(DeleteInstanceRequest {
            expected_generation: Some(running_after_rewake.generation),
            instance_id: INSTANCE_ID.to_owned(),
        })
        .await?
        .into_inner();
    if !deleted.accepted {
        return Err(
            "expected deployed operator DeleteInstance to delete the running instance".into(),
        );
    }
    wait_for_materialized_objects_deleted(kube, &config.namespace, STATEFUL_CLEANUP_TIMEOUT)
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-exclusivity.sh or an equivalent kind deployment"]
async fn stateful_exclusivity_keys_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_EXCLUSIVITY").as_deref() != Ok("1") {
        eprintln!(
            "skipping exclusivity kind E2E because SLEEPYPODS_KIND_E2E_EXCLUSIVITY=1 is not set"
        );
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let marker = unique_marker()?;

    assert_single_node_cluster(kube.clone()).await?;
    create_exclusivity_resources(&mut operator, &config).await?;
    let owner_created = wait_for_named_instance_state(
        &mut operator,
        EXCLUSIVE_OWNER_INSTANCE_ID,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    let blocked_created = wait_for_named_instance_state(
        &mut operator,
        EXCLUSIVE_BLOCKED_INSTANCE_ID,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;

    eprintln!("exclusivity E2E: waking owner with shared key");
    let owner_path = format!("/write/{marker}-owner");
    let owner_response = tokio::time::timeout(
        Duration::from_secs(140),
        http_get(config.frontline_addr, EXCLUSIVE_OWNER_HOST, &owner_path),
    )
    .await??;
    assert_response(&owner_response, "owner wake", "wrote:")?;
    let owner_running = wait_for_named_instance_state(
        &mut operator,
        EXCLUSIVE_OWNER_INSTANCE_ID,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    if owner_running.generation <= owner_created.generation {
        return Err(format!(
            "expected owner wake to advance generation beyond {}, got {}",
            owner_created.generation, owner_running.generation
        )
        .into());
    }
    assert_materialized_objects_for_instance(
        kube.clone(),
        &config.namespace,
        EXCLUSIVE_OWNER_INSTANCE_ID,
    )
    .await?;

    eprintln!("exclusivity E2E: restarting control-plane while owner holds key");
    restart_control_plane_pod(kube.clone(), &config.namespace, &config.operator_endpoint).await?;

    eprintln!("exclusivity E2E: same key is rejected without applying objects");
    let blocked_path = format!("/write/{marker}-blocked");
    let started = Instant::now();
    let blocked_response = tokio::time::timeout(
        Duration::from_secs(60),
        http_get(config.frontline_addr, EXCLUSIVE_BLOCKED_HOST, &blocked_path),
    )
    .await??;
    if blocked_response.status != 503 {
        return Err(format!(
            "same-key admission must reject the single request with503, got {}",
            blocked_response.status
        )
        .into());
    }
    if started.elapsed() > Duration::from_secs(65) {
        return Err(format!(
            "same-key contention was not bounded; elapsed {:?}",
            started.elapsed()
        )
        .into());
    }
    if blocked_response.body.contains("wrote:") {
        return Err("same-key contention unexpectedly reached the blocked backend".into());
    }
    let mut observer = connect_operator(&config.operator_endpoint).await?;
    // Failed atomic admission rolls back Waking, inventory and reservations.
    // It does not publish an accepted lifecycle failure or consume a generation.
    assert_instance_generation(
        &mut observer,
        EXCLUSIVE_BLOCKED_INSTANCE_ID,
        PbInstanceState::Cold,
        blocked_created.generation,
    )
    .await?;
    let blocked_status = observer
        .reconcile_materialization(ReconcileMaterializationRequest {
            status_only: true,
            instance_id: EXCLUSIVE_BLOCKED_INSTANCE_ID.to_owned(),
        })
        .await?
        .into_inner();
    if blocked_status.found {
        return Err("rejected exclusivity admission must not create a materialization".into());
    }
    assert_no_materialized_objects_for_instance(
        kube.clone(),
        &config.namespace,
        EXCLUSIVE_BLOCKED_INSTANCE_ID,
    )
    .await?;
    assert_instance_generation(
        &mut observer,
        EXCLUSIVE_OWNER_INSTANCE_ID,
        PbInstanceState::Running,
        owner_running.generation,
    )
    .await?;
    assert_materialized_objects_for_instance(
        kube.clone(),
        &config.namespace,
        EXCLUSIVE_OWNER_INSTANCE_ID,
    )
    .await?;
    let owner_read = tokio::time::timeout(
        Duration::from_secs(10),
        http_get(
            config.frontline_addr,
            EXCLUSIVE_OWNER_HOST,
            &format!("/read/{marker}-owner"),
        ),
    )
    .await??;
    assert_response(
        &owner_read,
        "owner retained data after rejected admission",
        "read:",
    )?;

    eprintln!("exclusivity E2E: unrelated key can wake independently");
    let other_path = format!("/write/{marker}-other");
    let other_response = tokio::time::timeout(
        Duration::from_secs(140),
        http_get(config.frontline_addr, EXCLUSIVE_OTHER_HOST, &other_path),
    )
    .await??;
    assert_response(&other_response, "unrelated key wake", "wrote:")?;
    wait_for_named_instance_state_with_reconnect(
        &config.operator_endpoint,
        EXCLUSIVE_OTHER_INSTANCE_ID,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    assert_materialized_objects_for_instance(
        kube.clone(),
        &config.namespace,
        EXCLUSIVE_OTHER_INSTANCE_ID,
    )
    .await?;

    eprintln!("exclusivity E2E: delete owner releases shared key after cleanup");
    let deleted = delete_instance_with_reconnect(
        &config.operator_endpoint,
        EXCLUSIVE_OWNER_INSTANCE_ID,
        Duration::from_secs(60),
    )
    .await?;
    if !deleted.deleted {
        return Err("expected owner DeleteInstance to delete the running instance".into());
    }
    wait_for_materialized_objects_deleted_for_instance(
        kube.clone(),
        &config.namespace,
        EXCLUSIVE_OWNER_INSTANCE_ID,
        STATEFUL_CLEANUP_TIMEOUT,
    )
    .await?;

    let recovered_response = tokio::time::timeout(
        Duration::from_secs(140),
        http_get(config.frontline_addr, EXCLUSIVE_BLOCKED_HOST, &blocked_path),
    )
    .await??;
    assert_response(
        &recovered_response,
        "shared key wake after owner cleanup",
        "wrote:",
    )?;
    let blocked_running = wait_for_named_instance_state_with_reconnect(
        &config.operator_endpoint,
        EXCLUSIVE_BLOCKED_INSTANCE_ID,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    if blocked_running.generation <= blocked_created.generation {
        return Err(format!(
            "expected released-key wake to advance generation beyond {}, got {}",
            blocked_created.generation, blocked_running.generation
        )
        .into());
    }
    assert_materialized_objects_for_instance(
        kube.clone(),
        &config.namespace,
        EXCLUSIVE_BLOCKED_INSTANCE_ID,
    )
    .await?;

    for instance_id in [EXCLUSIVE_BLOCKED_INSTANCE_ID, EXCLUSIVE_OTHER_INSTANCE_ID] {
        let deleted = delete_instance_with_reconnect(
            &config.operator_endpoint,
            instance_id,
            Duration::from_secs(60),
        )
        .await?;
        if deleted.deleted {
            wait_for_materialized_objects_deleted_for_instance(
                kube.clone(),
                &config.namespace,
                instance_id,
                STATEFUL_CLEANUP_TIMEOUT,
            )
            .await?;
        }
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-projection-drift.sh or an equivalent kind deployment"]
async fn projection_drift_and_finalizer_safety_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_STATEFUL").as_deref() != Ok("1")
        || env::var("SLEEPYPODS_KIND_E2E_PROJECTION_DRIFT").as_deref() != Ok("1")
    {
        eprintln!(
            "skipping projection drift kind E2E because SLEEPYPODS_KIND_E2E_STATEFUL=1 and SLEEPYPODS_KIND_E2E_PROJECTION_DRIFT=1 are not set"
        );
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let marker = unique_marker()?;

    let result =
        run_projection_drift_and_finalizer_safety(kube.clone(), &config, &mut operator, marker)
            .await;
    let cleanup = cleanup_projection_drift_injections(kube, &config.namespace).await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup_error)) => {
            Err(format!("projection drift kind E2E cleanup failed: {cleanup_error}").into())
        }
        (Err(error), Err(cleanup_error)) => Err(format!(
            "projection drift kind E2E failed: {error}; cleanup also failed: {cleanup_error}"
        )
        .into()),
    }
}

async fn run_projection_drift_and_finalizer_safety(
    kube: Client,
    config: &E2eConfig,
    operator: &mut OperatorControlPlaneClient<Channel>,
    marker: String,
) -> TestResult<()> {
    assert_single_node_cluster(kube.clone()).await?;
    create_projection_drift_resources(operator, config).await?;
    let created =
        wait_for_instance_state(operator, PbInstanceState::Cold, Duration::from_secs(30)).await?;

    let write_path = format!("/write/{marker}");
    eprintln!("projection drift E2E: waking stateful backend");
    let response = wait_for_frontline_response(
        config,
        "projection drift wake",
        &write_path,
        "wrote:",
        Duration::from_secs(180),
    )
    .await?;
    assert_response(&response, "projection drift wake", "wrote:")?;
    let running =
        wait_for_instance_state(operator, PbInstanceState::Running, Duration::from_secs(30))
            .await?;
    if running.generation <= created.generation {
        return Err(format!(
            "expected projection drift wake to advance generation beyond {}, got {}",
            created.generation, running.generation
        )
        .into());
    }
    assert_materialized_stateful_objects(kube.clone(), config).await?;

    let materialization_id = config.materialization_id(INSTANCE_ID);
    let baseline = reconcile_materialization(operator, INSTANCE_ID).await?;
    assert_ready_reconcile_report_only(&baseline, &materialization_id)?;
    assert_no_unowned_projection_observations(&baseline.projection_observations, "baseline")?;
    expect_projection_observation(
        &baseline.projection_observations,
        "v1",
        "Service",
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        "ready",
    )?;

    eprintln!("projection drift E2E: deleting an owned Service ref");
    let original_service = service_api(kube.clone(), &config.namespace)
        .get(RENDERED_WORKLOAD_NAME)
        .await?;
    delete_service_if_present(kube.clone(), &config.namespace, RENDERED_WORKLOAD_NAME).await?;
    wait_for_service_absent(
        kube.clone(),
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        Duration::from_secs(30),
    )
    .await?;

    let missing = reconcile_materialization(operator, INSTANCE_ID).await?;
    assert_ready_reconcile_report_only(&missing, &materialization_id)?;
    assert_no_unowned_projection_observations(&missing.projection_observations, "missing Service")?;
    expect_projection_observation(
        &missing.projection_observations,
        "v1",
        "Service",
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        "missing",
    )?;
    let unready = expect_projection_observation(
        &missing.projection_observations,
        "v1",
        "Service",
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        "unready",
    )?;
    if unready.reason != "service_missing" {
        return Err(format!(
            "expected deleted Service readiness reason service_missing, got {:?}",
            unready.reason
        )
        .into());
    }
    sleep(Duration::from_secs(2)).await;
    assert_service_absent(kube.clone(), &config.namespace, RENDERED_WORKLOAD_NAME).await?;
    assert_instance_generation(
        operator,
        INSTANCE_ID,
        PbInstanceState::Running,
        running.generation,
    )
    .await?;

    eprintln!("projection drift E2E: creating an unowned same-name Service collision");
    create_unowned_service_collision(kube.clone(), &config.namespace, &original_service).await?;
    let unowned = reconcile_materialization(operator, INSTANCE_ID).await?;
    assert_ready_reconcile_report_only(&unowned, &materialization_id)?;
    let unowned_service = expect_projection_observation(
        &unowned.projection_observations,
        "v1",
        "Service",
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        "present_unowned",
    )?;
    if unowned_service.reason != "managed_by_mismatch" {
        return Err(format!(
            "expected unowned Service reason managed_by_mismatch, got {:?}",
            unowned_service.reason
        )
        .into());
    }
    assert_unowned_service_present(kube.clone(), &config.namespace, RENDERED_WORKLOAD_NAME).await?;

    add_stateful_set_finalizer(
        kube.clone(),
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        PROJECTION_DRIFT_FINALIZER,
    )
    .await?;
    let accepted = operator
        .delete_instance(DeleteInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
            expected_generation: Some(running.generation),
        })
        .await?
        .into_inner();
    if !accepted.accepted {
        return Err(
            "deletion must be durably accepted while the injected finalizer blocks cleanup".into(),
        );
    }
    let blocked_collision = reconcile_materialization(operator, INSTANCE_ID).await?;
    expect_projection_observation(
        &blocked_collision.projection_observations,
        "v1",
        "Service",
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        "present_unowned",
    )?;
    assert_unowned_service_present(kube.clone(), &config.namespace, RENDERED_WORKLOAD_NAME).await?;
    assert_instance_generation(
        operator,
        INSTANCE_ID,
        PbInstanceState::Deleting,
        running.generation + 1,
    )
    .await?;

    eprintln!("projection drift E2E: removing injected unowned Service");
    delete_service_if_present(kube.clone(), &config.namespace, RENDERED_WORKLOAD_NAME).await?;
    wait_for_service_absent(
        kube.clone(),
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        Duration::from_secs(30),
    )
    .await?;
    restore_owned_service(kube.clone(), &config.namespace, &original_service).await?;
    eprintln!(
        "projection drift E2E: accepted cleanup now blocked by the owned StatefulSet finalizer"
    );
    let _ = reconcile_materialization(operator, INSTANCE_ID).await?;

    let blocked = wait_for_reconcile_observation(
        operator,
        INSTANCE_ID,
        ReconcileObservationExpectation {
            api_version: "apps/v1",
            kind: "StatefulSet",
            namespace: &config.namespace,
            name: RENDERED_WORKLOAD_NAME,
            state: "deleting_owned",
        },
        Duration::from_secs(30),
    )
    .await?;
    if blocked.state != "Deleting" {
        return Err(format!(
            "expected blocked materialization state Deleting, got {}",
            blocked.state
        )
        .into());
    }
    let blocked_stateful_set = expect_projection_observation(
        &blocked.projection_observations,
        "apps/v1",
        "StatefulSet",
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        "deleting_owned",
    )?;
    if !blocked_stateful_set
        .finalizers
        .iter()
        .any(|finalizer| finalizer == PROJECTION_DRIFT_FINALIZER)
    {
        return Err(format!(
            "expected deleting StatefulSet observation to include finalizer {PROJECTION_DRIFT_FINALIZER}, got {:?}",
            blocked_stateful_set.finalizers
        )
        .into());
    }

    eprintln!("projection drift E2E: removing finalizer and completing cleanup");
    remove_stateful_set_finalizer(
        kube.clone(),
        &config.namespace,
        RENDERED_WORKLOAD_NAME,
        PROJECTION_DRIFT_FINALIZER,
    )
    .await?;
    let deleted = delete_instance_until_cleanup_complete(
        &config.operator_endpoint,
        INSTANCE_ID,
        STATEFUL_CLEANUP_TIMEOUT,
    )
    .await?;
    if !deleted.deleted {
        return Err("expected final DeleteInstance retry to remove the instance".into());
    }
    wait_for_materialized_objects_deleted_for_instance(
        kube,
        &config.namespace,
        INSTANCE_ID,
        STATEFUL_CLEANUP_TIMEOUT,
    )
    .await?;

    Ok(())
}

async fn cleanup_projection_drift_injections(kube: Client, namespace: &str) -> TestResult<()> {
    let mut errors = Vec::new();
    if let Err(error) = remove_stateful_set_finalizer(
        kube.clone(),
        namespace,
        RENDERED_WORKLOAD_NAME,
        PROJECTION_DRIFT_FINALIZER,
    )
    .await
    {
        errors.push(format!("remove StatefulSet finalizer: {error}"));
    }
    if let Err(error) = delete_service_if_present(kube, namespace, RENDERED_WORKLOAD_NAME).await {
        errors.push(format!("delete injected Service: {error}"));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

#[derive(Clone, Debug)]
struct E2eConfig {
    cluster_id: String,
    namespace: String,
    operator_endpoint: String,
    frontline_addr: SocketAddr,
    app_image: String,
    sidecar_image: String,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            cluster_id: env::var("SLEEPYPODS_E2E_CLUSTER_ID")
                .unwrap_or_else(|_| CLUSTER_ID.to_owned()),
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-stateful".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19151".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19180".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/stateful-app:kind-e2e-stateful".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-stateful".to_owned()),
        })
    }

    fn host_path(&self) -> String {
        format!("/tmp/sleepypods-kind-e2e/{TENANT_VALUE}")
    }

    fn materialization_id(&self, instance_id: &str) -> String {
        format!("{instance_id}:{}:{}", self.cluster_id, self.namespace)
    }
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
    operator: &mut OperatorControlPlaneClient<Channel>,
) -> Result<(), tonic::Status> {
    match operator
        .get_instance(GetInstanceRequest {
            instance_id: "kind-e2e-probe-missing".to_owned(),
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(status)
            if matches!(
                status.code(),
                Code::NotFound | Code::InvalidArgument | Code::FailedPrecondition
            ) =>
        {
            Ok(())
        }
        Err(status) => Err(status),
    }
}

#[derive(Clone, Copy, Debug)]
struct DeleteInstanceOutcome {
    deleted: bool,
}

async fn delete_instance_with_reconnect(
    endpoint: &str,
    instance_id: &str,
    timeout: Duration,
) -> TestResult<DeleteInstanceOutcome> {
    delete_instance_until_cleanup_complete(endpoint, instance_id, timeout).await
}

async fn delete_instance_until_cleanup_complete(
    endpoint: &str,
    instance_id: &str,
    timeout: Duration,
) -> TestResult<DeleteInstanceOutcome> {
    let deadline = Instant::now() + timeout;
    let mut accepted = false;
    loop {
        let mut operator = connect_operator(endpoint).await?;
        match operator
            .get_instance(GetInstanceRequest {
                instance_id: instance_id.to_owned(),
            })
            .await
        {
            Err(status) if status.code() == Code::NotFound => {
                return Ok(DeleteInstanceOutcome { deleted: true });
            }
            Ok(response) => {
                let current = response.into_inner();
                if !accepted && current.state != PbInstanceState::Deleting as i32 {
                    operator
                        .delete_instance(DeleteInstanceRequest {
                            instance_id: instance_id.to_owned(),
                            expected_generation: Some(current.generation),
                        })
                        .await?;
                }
                accepted = true;
            }
            Err(status)
                if retryable_operator_transport_status(&status) && Instant::now() < deadline => {}
            Err(status) => return Err(status.into()),
        }
        if Instant::now() >= deadline {
            return Err("accepted instance deletion did not complete".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn retryable_operator_transport_status(status: &tonic::Status) -> bool {
    status.code() == Code::Unknown && status.message().contains("transport error")
}

async fn create_operator_resources(
    operator: &mut OperatorControlPlaneClient<Channel>,
    config: &E2eConfig,
) -> TestResult<()> {
    create_stateful_operator_resources(operator, config, "kind-e2e-stateful", 15_000).await
}

async fn create_projection_drift_resources(
    operator: &mut OperatorControlPlaneClient<Channel>,
    config: &E2eConfig,
) -> TestResult<()> {
    create_stateful_operator_resources(operator, config, "kind-e2e-projection-drift", 900_000).await
}

async fn create_stateful_operator_resources(
    operator: &mut OperatorControlPlaneClient<Channel>,
    config: &E2eConfig,
    idempotency_scope: &str,
    idle_timeout_ms: u64,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            class_id: CLASS_ID.to_owned(),
            version: 1,
            value_schema: Some(required_values(&["tenant"])),
            template: Some(manifest_template(config)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        })
        .await?;

    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: format!("{idempotency_scope}-create-instance"),
            instance_id: INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: HashMap::from([("tenant".to_owned(), TENANT_VALUE.to_owned())]),
            routes: Vec::new(),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: format!("{idempotency_scope}-create-route"),
            route_binding_id: ROUTE_ID.to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            identity: Some(RouteIdentity {
                kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
                    host: Some(RouteHost {
                        kind: RouteHostKind::Exact as i32,
                        host: ROUTE_HOST.to_owned(),
                    }),
                    path_prefix: None,
                })),
            }),
            protocol: ProtocolRoute::Http as i32,
        })
        .await?;

    Ok(())
}

async fn create_exclusivity_resources(
    operator: &mut OperatorControlPlaneClient<Channel>,
    config: &E2eConfig,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            class_id: EXCLUSIVE_CLASS_ID.to_owned(),
            version: 1,
            value_schema: Some(required_values(&["tenant", "volume_handle"])),
            template: Some(manifest_template(config)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 900_000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk".to_owned(),
                value: Some(instance_value_text("volume_handle")),
            }],
        })
        .await?;

    create_exclusive_instance_and_route(
        operator,
        "owner",
        EXCLUSIVE_OWNER_INSTANCE_ID,
        EXCLUSIVE_OWNER_ROUTE_ID,
        EXCLUSIVE_OWNER_HOST,
        EXCLUSIVE_OWNER_TENANT,
        EXCLUSIVE_SHARED_HANDLE,
    )
    .await?;
    create_exclusive_instance_and_route(
        operator,
        "blocked",
        EXCLUSIVE_BLOCKED_INSTANCE_ID,
        EXCLUSIVE_BLOCKED_ROUTE_ID,
        EXCLUSIVE_BLOCKED_HOST,
        EXCLUSIVE_BLOCKED_TENANT,
        EXCLUSIVE_SHARED_HANDLE,
    )
    .await?;
    create_exclusive_instance_and_route(
        operator,
        "other",
        EXCLUSIVE_OTHER_INSTANCE_ID,
        EXCLUSIVE_OTHER_ROUTE_ID,
        EXCLUSIVE_OTHER_HOST,
        EXCLUSIVE_OTHER_TENANT,
        EXCLUSIVE_OTHER_HANDLE,
    )
    .await?;

    Ok(())
}

async fn create_exclusive_instance_and_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    suffix: &str,
    instance_id: &str,
    route_id: &str,
    route_host: &str,
    tenant: &str,
    volume_handle: &str,
) -> TestResult<()> {
    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: format!("kind-e2e-exclusivity-create-{suffix}"),
            instance_id: instance_id.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: EXCLUSIVE_CLASS_ID.to_owned(),
                version: 1,
            }),
            values: HashMap::from([
                ("tenant".to_owned(), tenant.to_owned()),
                ("volume_handle".to_owned(), volume_handle.to_owned()),
            ]),
            routes: Vec::new(),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: format!("kind-e2e-exclusivity-route-{suffix}"),
            route_binding_id: route_id.to_owned(),
            instance_id: instance_id.to_owned(),
            identity: Some(RouteIdentity {
                kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
                    host: Some(RouteHost {
                        kind: RouteHostKind::Exact as i32,
                        host: route_host.to_owned(),
                    }),
                    path_prefix: None,
                })),
            }),
            protocol: ProtocolRoute::Http as i32,
        })
        .await?;

    Ok(())
}

async fn wait_for_instance_state(
    operator: &mut OperatorControlPlaneClient<Channel>,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    wait_for_named_instance_state(operator, INSTANCE_ID, expected, timeout).await
}

async fn wait_for_named_instance_state(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let instance = operator
            .get_instance(GetInstanceRequest {
                instance_id: instance_id.to_owned(),
            })
            .await?
            .into_inner();
        let actual =
            PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified);
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

async fn assert_instance_generation(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    expected_state: PbInstanceState,
    expected_generation: u64,
) -> TestResult<()> {
    let instance = operator
        .get_instance(GetInstanceRequest {
            instance_id: instance_id.to_owned(),
        })
        .await?
        .into_inner();
    let actual_state =
        PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified);
    if actual_state != expected_state || instance.generation != expected_generation {
        return Err(format!(
            "expected instance {instance_id} to remain {expected_state:?} generation {expected_generation}, got {actual_state:?} generation {}",
            instance.generation
        )
        .into());
    }

    Ok(())
}

async fn reconcile_materialization(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<ReconcileMaterializationResponse> {
    Ok(operator
        .reconcile_materialization(ReconcileMaterializationRequest {
            status_only: false,
            instance_id: instance_id.to_owned(),
        })
        .await?
        .into_inner())
}

fn assert_ready_reconcile_report_only(
    response: &ReconcileMaterializationResponse,
    materialization_id: &str,
) -> TestResult<()> {
    if !response.found {
        return Err(format!("expected materialization {materialization_id} to be found").into());
    }
    if response.materialization_id != materialization_id {
        return Err(format!(
            "expected materialization id {materialization_id}, got {}",
            response.materialization_id
        )
        .into());
    }
    if response.state != "Ready" {
        return Err(format!(
            "expected ReconcileMaterialization to report Ready state, got {}",
            response.state
        )
        .into());
    }
    if response.attempted {
        return Err("Ready materialization drift inspection must remain report-only in V1".into());
    }

    Ok(())
}

fn expect_projection_observation<'a>(
    observations: &'a [ProjectionObservation],
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    state: &str,
) -> TestResult<&'a ProjectionObservation> {
    observations
        .iter()
        .find(|observation| {
            observation.state == state
                && observation.r#ref.as_ref().is_some_and(|object_ref| {
                    object_ref.api_version == api_version
                        && object_ref.kind == kind
                        && object_ref.namespace == namespace
                        && object_ref.name == name
                })
        })
        .ok_or_else(|| {
            format!(
                "expected projection observation {api_version} {kind} {namespace}/{name} state {state}; got {}",
                projection_observation_summary(observations)
            )
            .into()
        })
}

fn assert_no_unowned_projection_observations(
    observations: &[ProjectionObservation],
    context: &str,
) -> TestResult<()> {
    let unowned = observations
        .iter()
        .filter(|observation| {
            matches!(
                observation.state.as_str(),
                "present_unowned" | "deleting_unowned"
            )
        })
        .collect::<Vec<_>>();
    if unowned.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "expected no unowned projection observations for {context}; got {}",
            projection_observation_summary(observations)
        )
        .into())
    }
}

struct ReconcileObservationExpectation<'a> {
    api_version: &'a str,
    kind: &'a str,
    namespace: &'a str,
    name: &'a str,
    state: &'a str,
}

async fn wait_for_reconcile_observation(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    expectation: ReconcileObservationExpectation<'_>,
    timeout: Duration,
) -> TestResult<ReconcileMaterializationResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let response = reconcile_materialization(operator, instance_id).await?;
        if expect_projection_observation(
            &response.projection_observations,
            expectation.api_version,
            expectation.kind,
            expectation.namespace,
            expectation.name,
            expectation.state,
        )
        .is_ok()
        {
            return Ok(response);
        }

        if Instant::now() >= deadline {
            let last_summary = projection_observation_summary(&response.projection_observations);
            return Err(format!(
                "timed out waiting for projection observation {} {} {}/{} state {}; last observations: {last_summary}",
                expectation.api_version,
                expectation.kind,
                expectation.namespace,
                expectation.name,
                expectation.state
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn projection_observation_summary(observations: &[ProjectionObservation]) -> String {
    observations
        .iter()
        .map(|observation| {
            let object_ref = observation
                .r#ref
                .as_ref()
                .map(|object_ref| {
                    format!(
                        "{} {}/{}",
                        object_ref.kind, object_ref.namespace, object_ref.name
                    )
                })
                .unwrap_or_else(|| "<missing-ref>".to_owned());
            format!("{object_ref}:{}:{}", observation.state, observation.reason)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

async fn wait_for_named_instance_state_with_reconnect(
    endpoint: &str,
    instance_id: &str,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut operator = connect_operator(endpoint).await?;
        match operator
            .get_instance(GetInstanceRequest {
                instance_id: instance_id.to_owned(),
            })
            .await
        {
            Ok(response) => {
                let instance = response.into_inner();
                let actual = PbInstanceState::try_from(instance.state)
                    .unwrap_or(PbInstanceState::Unspecified);
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
            Err(status)
                if retryable_operator_transport_status(&status) && Instant::now() < deadline =>
            {
                eprintln!(
                    "retrying GetInstance for {instance_id} after transient operator transport error: {status}"
                );
            }
            Err(status) => return Err(status.into()),
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_frontline_response(
    config: &E2eConfig,
    context: &str,
    path: &str,
    expected_body: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    wait_for_frontline_response_for_host(config, ROUTE_HOST, context, path, expected_body, timeout)
        .await
}

async fn wait_for_frontline_response_for_host(
    config: &E2eConfig,
    host: &str,
    context: &str,
    path: &str,
    expected_body: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == 200 && response.body.contains(expected_body) => {
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
                "timed out waiting for successful frontline response for {context} host {host} path {path}: {}",
                last_error
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn http_get(addr: SocketAddr, host: &str, path: &str) -> TestResult<HttpResponse> {
    let host = host.to_owned();
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || http_get_blocking(addr, &host, &path))
        .await
        .map_err(|error| format!("HTTP request task failed: {error}"))?
}

// The shared helper preserves single-connection framing and cancellation ownership.
async fn http_get_once(
    addr: SocketAddr,
    host: &str,
    path: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let response = http_once::get_once(addr, host, path, timeout).await?;
    Ok(HttpResponse {
        status: response.status().as_u16(),
        body: response.into_body(),
    })
}

fn http_get_blocking(addr: SocketAddr, host: &str, path: &str) -> TestResult<HttpResponse> {
    let timeout = Duration::from_secs(180);
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    let raw = String::from_utf8_lossy(&bytes);
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| format!("HTTP response from {addr} did not include a status line"))?
        .parse::<u16>()?;

    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

fn assert_response(response: &HttpResponse, context: &str, expected_body: &str) -> TestResult<()> {
    if response.status != 200 {
        return Err(format!("{context} returned HTTP {}", response.status).into());
    }
    if !response.body.contains(expected_body) {
        return Err(format!(
            "{context} body did not include {expected_body:?}: {:?}",
            response.body
        )
        .into());
    }

    Ok(())
}

async fn assert_materialized_stateful_objects(kube: Client, config: &E2eConfig) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), &config.namespace);
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), &config.namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), &config.namespace);
    let pods: Api<Pod> = Api::namespaced(kube, &config.namespace);

    let pv = pvs.get(RENDERED_PV_NAME).await?;
    let pvc = pvcs.get(RENDERED_PVC_NAME).await?;
    let stateful_set = stateful_sets.get(RENDERED_WORKLOAD_NAME).await?;
    let service = services.get(RENDERED_WORKLOAD_NAME).await?;

    assert_resource_order(&pv, &pvc, &stateful_set)?;
    assert_pv_and_pvc(&pv, &pvc, config)?;
    assert_stateful_set_shape(&stateful_set, config)?;
    assert_service_targets_sidecar(&service)?;
    assert_stateful_pod_mounts_pvc(&pods).await?;

    Ok(())
}

fn service_api(kube: Client, namespace: &str) -> Api<Service> {
    Api::namespaced(kube, namespace)
}

async fn delete_service_if_present(kube: Client, namespace: &str, name: &str) -> TestResult<()> {
    let services = service_api(kube, namespace);
    match services.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(error) if kube_error_is_not_found(&error) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn wait_for_service_absent(
    kube: Client,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> TestResult<()> {
    let services = service_api(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        if is_not_found(services.get(name).await) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timed out waiting for Service {namespace}/{name} to be absent").into(),
            );
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn assert_service_absent(kube: Client, namespace: &str, name: &str) -> TestResult<()> {
    let services = service_api(kube, namespace);
    match services.get(name).await {
        Ok(_) => Err(format!("expected Service {namespace}/{name} to remain absent").into()),
        Err(error) if kube_error_is_not_found(&error) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn create_unowned_service_collision(
    kube: Client,
    namespace: &str,
    original: &Service,
) -> TestResult<()> {
    let original_spec = original
        .spec
        .as_ref()
        .ok_or("original Service is missing spec")?;
    let ports = original_spec
        .ports
        .clone()
        .ok_or("original Service is missing ports")?
        .into_iter()
        .map(unowned_service_port)
        .collect::<Vec<_>>();
    let mut labels = BTreeMap::new();
    labels.insert(
        "sleepypods.io/kind-e2e".to_owned(),
        "projection-drift-unowned".to_owned(),
    );
    let service = Service {
        metadata: ObjectMeta {
            name: Some(RENDERED_WORKLOAD_NAME.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(ports),
            selector: original_spec.selector.clone(),
            type_: Some("ClusterIP".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };

    service_api(kube, namespace)
        .create(&PostParams::default(), &service)
        .await?;
    Ok(())
}

async fn restore_owned_service(
    kube: Client,
    namespace: &str,
    original: &Service,
) -> TestResult<()> {
    let original_spec = original
        .spec
        .as_ref()
        .ok_or("original Service is missing spec")?;
    let service = Service {
        metadata: ObjectMeta {
            name: Some(RENDERED_WORKLOAD_NAME.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: original.metadata.labels.clone(),
            annotations: original.metadata.annotations.clone(),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: original_spec.ports.clone(),
            selector: original_spec.selector.clone(),
            type_: Some("ClusterIP".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };

    service_api(kube, namespace)
        .create(&PostParams::default(), &service)
        .await?;
    Ok(())
}

fn unowned_service_port(mut port: ServicePort) -> ServicePort {
    port.node_port = None;
    port
}

async fn assert_unowned_service_present(
    kube: Client,
    namespace: &str,
    name: &str,
) -> TestResult<()> {
    let service = service_api(kube, namespace).get(name).await?;
    let labels = service.metadata.labels.unwrap_or_default();
    if labels.get(LABEL_MANAGED_BY).map(String::as_str) == Some(LABEL_MANAGED_BY_VALUE) {
        return Err(format!(
            "Service {namespace}/{name} unexpectedly has SleepyPods ownership label"
        )
        .into());
    }
    if labels.contains_key("sleepypods.io/instance-id") {
        return Err(
            format!("Service {namespace}/{name} unexpectedly has an instance-id label").into(),
        );
    }
    Ok(())
}

async fn add_stateful_set_finalizer(
    kube: Client,
    namespace: &str,
    name: &str,
    finalizer: &str,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube, namespace);
    let stateful_set = stateful_sets.get(name).await?;
    let mut finalizers = stateful_set.metadata.finalizers.unwrap_or_default();
    if !finalizers.iter().any(|value| value == finalizer) {
        finalizers.push(finalizer.to_owned());
    }
    patch_stateful_set_finalizers(&stateful_sets, name, finalizers).await
}

async fn remove_stateful_set_finalizer(
    kube: Client,
    namespace: &str,
    name: &str,
    finalizer: &str,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube, namespace);
    let stateful_set = match stateful_sets.get(name).await {
        Ok(stateful_set) => stateful_set,
        Err(error) if kube_error_is_not_found(&error) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut finalizers = stateful_set.metadata.finalizers.unwrap_or_default();
    let original_len = finalizers.len();
    finalizers.retain(|value| value != finalizer);
    if finalizers.len() == original_len {
        return Ok(());
    }
    patch_stateful_set_finalizers(&stateful_sets, name, finalizers).await
}

async fn patch_stateful_set_finalizers(
    stateful_sets: &Api<StatefulSet>,
    name: &str,
    finalizers: Vec<String>,
) -> TestResult<()> {
    let patch = Patch::Merge(json!({
        "metadata": {
            "finalizers": finalizers,
        }
    }));
    stateful_sets
        .patch(name, &PatchParams::default(), &patch)
        .await?;
    Ok(())
}

async fn assert_materialized_objects_for_instance(
    kube: Client,
    namespace: &str,
    instance_id: &str,
) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let labels = instance_label_selector(instance_id);
    let deadline = Instant::now() + Duration::from_secs(120);

    loop {
        let pv_count = pvs
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len();
        let pvc_count = pvcs
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len();
        let stateful_set_count = stateful_sets
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len();
        let service_count = services
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len();
        let pod_ready = pods
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .iter()
            .any(pod_ready);
        if pv_count == 1
            && pvc_count == 1
            && stateful_set_count == 1
            && service_count == 1
            && pod_ready
        {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for materialized objects for instance {instance_id}; got PV={pv_count} PVC={pvc_count} StatefulSet={stateful_set_count} Service={service_count} pod_ready={pod_ready}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn assert_no_materialized_objects_for_instance(
    kube: Client,
    namespace: &str,
    instance_id: &str,
) -> TestResult<()> {
    let counts = materialized_object_counts(kube, namespace, instance_id).await?;
    if counts.total() != 0 {
        return Err(format!(
            "expected no materialized objects for instance {instance_id}; got PV={} PVC={} StatefulSet={} Service={} Pod={}",
            counts.persistent_volumes,
            counts.persistent_volume_claims,
            counts.stateful_sets,
            counts.services,
            counts.pods
        )
        .into());
    }

    Ok(())
}

fn assert_resource_order(
    pv: &PersistentVolume,
    pvc: &PersistentVolumeClaim,
    stateful_set: &StatefulSet,
) -> TestResult<()> {
    let pv_rv = resource_version(pv.metadata.resource_version.as_deref(), RENDERED_PV_NAME)?;
    let pvc_rv = resource_version(pvc.metadata.resource_version.as_deref(), RENDERED_PVC_NAME)?;
    let stateful_rv = resource_version(
        stateful_set.metadata.resource_version.as_deref(),
        RENDERED_WORKLOAD_NAME,
    )?;
    if !(pv_rv < pvc_rv && pvc_rv < stateful_rv) {
        return Err(format!(
            "expected PV/PVC/StatefulSet resourceVersions to reflect apply order {RENDERED_PV_NAME} < {RENDERED_PVC_NAME} < {RENDERED_WORKLOAD_NAME}; got {pv_rv}, {pvc_rv}, {stateful_rv}"
        )
        .into());
    }
    Ok(())
}

fn resource_version(value: Option<&str>, name: &str) -> TestResult<u64> {
    value
        .ok_or_else(|| format!("{name} is missing metadata.resourceVersion"))?
        .parse()
        .map_err(|error| format!("{name} resourceVersion is not numeric: {error}").into())
}

fn assert_pv_and_pvc(
    pv: &PersistentVolume,
    pvc: &PersistentVolumeClaim,
    config: &E2eConfig,
) -> TestResult<()> {
    let actual_path = pv
        .spec
        .as_ref()
        .and_then(|spec| spec.host_path.as_ref())
        .map(|source| source.path.as_str());
    if actual_path != Some(config.host_path().as_str()) {
        return Err(format!(
            "expected PersistentVolume {RENDERED_PV_NAME} hostPath {}, got {actual_path:?}",
            config.host_path()
        )
        .into());
    }
    let claim_ref = pv
        .spec
        .as_ref()
        .and_then(|spec| spec.claim_ref.as_ref())
        .ok_or("PersistentVolume is missing claimRef")?;
    if claim_ref.namespace.as_deref() != Some(config.namespace.as_str())
        || claim_ref.name.as_deref() != Some(RENDERED_PVC_NAME)
    {
        return Err(format!(
            "expected PersistentVolume claimRef {}/{}, got {:?}/{:?}",
            config.namespace, RENDERED_PVC_NAME, claim_ref.namespace, claim_ref.name
        )
        .into());
    }

    let volume_name = pvc
        .spec
        .as_ref()
        .and_then(|spec| spec.volume_name.as_deref());
    if volume_name != Some(RENDERED_PV_NAME) {
        return Err(format!(
            "expected PersistentVolumeClaim {RENDERED_PVC_NAME} to target PV {RENDERED_PV_NAME}, got {volume_name:?}"
        )
        .into());
    }
    let phase = pvc
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if phase != Some("Bound") {
        return Err(format!(
            "expected PersistentVolumeClaim {RENDERED_PVC_NAME} to be Bound, got {phase:?}"
        )
        .into());
    }
    Ok(())
}

fn assert_stateful_set_shape(stateful_set: &StatefulSet, config: &E2eConfig) -> TestResult<()> {
    let spec = stateful_set
        .spec
        .as_ref()
        .ok_or("materialized StatefulSet is missing spec")?;
    if spec.service_name.as_deref() != Some(RENDERED_WORKLOAD_NAME) {
        return Err(format!(
            "expected StatefulSet serviceName {RENDERED_WORKLOAD_NAME}, got {:?}",
            spec.service_name
        )
        .into());
    }
    let pod_spec = spec
        .template
        .spec
        .as_ref()
        .ok_or("materialized StatefulSet is missing pod spec")?;
    let app = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "app")
        .ok_or("materialized StatefulSet is missing app container")?;
    if app.image.as_deref() != Some(config.app_image.as_str()) {
        return Err(format!(
            "expected app image {}, got {:?}",
            config.app_image, app.image
        )
        .into());
    }
    let app_mount = app
        .volume_mounts
        .as_ref()
        .and_then(|mounts| mounts.iter().find(|mount| mount.name == VOLUME_NAME))
        .ok_or("app container is missing data volume mount")?;
    if app_mount.mount_path != MOUNT_PATH {
        return Err(format!(
            "expected app mount path {MOUNT_PATH}, got {}",
            app_mount.mount_path
        )
        .into());
    }

    let sidecar = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "sleepypods-sidecar")
        .ok_or("materialized StatefulSet is missing sidecar container")?;
    if sidecar.image.as_deref() != Some(config.sidecar_image.as_str()) {
        return Err(format!(
            "expected sidecar image {}, got {:?}",
            config.sidecar_image, sidecar.image
        )
        .into());
    }
    assert_env(sidecar, "SLEEPYPODS_SIDECAR_LISTEN_ADDR", "0.0.0.0:15000")?;
    assert_env(
        sidecar,
        "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
        &format!(
            "http://sleepypods-control-plane.{}.svc.cluster.local:50051",
            config.namespace
        ),
    )?;

    let volume = pod_spec
        .volumes
        .as_ref()
        .and_then(|volumes| volumes.iter().find(|volume| volume.name == VOLUME_NAME))
        .ok_or("StatefulSet pod spec is missing data volume")?;
    let claim_name = volume
        .persistent_volume_claim
        .as_ref()
        .map(|claim| claim.claim_name.as_str());
    if claim_name != Some(RENDERED_PVC_NAME) {
        return Err(format!(
            "expected pod volume to use PVC {RENDERED_PVC_NAME}, got {claim_name:?}"
        )
        .into());
    }

    Ok(())
}

fn assert_service_targets_sidecar(service: &Service) -> TestResult<()> {
    let target_port = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .and_then(|ports| ports.first())
        .and_then(|port| port.target_port.as_ref())
        .ok_or("materialized Service is missing targetPort")?;
    if target_port != &IntOrString::Int(SIDECAR_PORT as i32) {
        return Err(
            format!("expected Service targetPort {SIDECAR_PORT}, got {target_port:?}").into(),
        );
    }
    Ok(())
}

async fn assert_stateful_pod_mounts_pvc(pods: &Api<Pod>) -> TestResult<()> {
    let pod = pods
        .list(&ListParams::default().labels(&format!(
            "sleepypods.io/instance-id={INSTANCE_ID},sleepypods.io/workload-name={RENDERED_WORKLOAD_NAME}"
        )))
        .await?
        .items
        .into_iter()
        .next()
        .ok_or("materialized StatefulSet pod was not found")?;
    let pod_name = pod
        .metadata
        .name
        .as_deref()
        .ok_or("StatefulSet pod is missing metadata.name")?
        .to_owned();
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if phase != Some("Running") {
        return Err(
            format!("expected StatefulSet pod {pod_name} to be Running, got {phase:?}").into(),
        );
    }
    let volume = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.volumes.as_ref())
        .and_then(|volumes| volumes.iter().find(|volume| volume.name == VOLUME_NAME))
        .ok_or("StatefulSet pod is missing data volume")?;
    let claim_name = volume
        .persistent_volume_claim
        .as_ref()
        .map(|claim| claim.claim_name.as_str());
    if claim_name != Some(RENDERED_PVC_NAME) {
        return Err(format!(
            "expected StatefulSet pod to use PVC {RENDERED_PVC_NAME}, got {claim_name:?}"
        )
        .into());
    }
    Ok(())
}

fn assert_env(
    container: &k8s_openapi::api::core::v1::Container,
    name: &str,
    expected: &str,
) -> TestResult<()> {
    let value = container
        .env
        .as_ref()
        .and_then(|vars| vars.iter().find(|var| var.name == name))
        .and_then(|var| var.value.as_deref())
        .ok_or_else(|| format!("missing env var {name}"))?;
    if value != expected {
        return Err(format!("expected env {name}={expected:?}, got {value:?}").into());
    }
    Ok(())
}

async fn wait_for_materialized_objects_deleted(
    kube: Client,
    namespace: &str,
    timeout: Duration,
) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        let stateful_absent = is_not_found(stateful_sets.get(RENDERED_WORKLOAD_NAME).await);
        let service_absent = is_not_found(services.get(RENDERED_WORKLOAD_NAME).await);
        let pvc_absent = is_not_found(pvcs.get(RENDERED_PVC_NAME).await);
        let pv_absent = is_not_found(pvs.get(RENDERED_PV_NAME).await);
        if stateful_absent && service_absent && pvc_absent && pv_absent {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for materialized StatefulSet/Service/PVC/PV objects for {namespace}/{RENDERED_WORKLOAD_NAME} to be deleted"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_materialized_objects_deleted_for_instance(
    kube: Client,
    namespace: &str,
    instance_id: &str,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let counts = materialized_object_counts(kube.clone(), namespace, instance_id).await?;
        if counts.total() == 0 {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for materialized objects for instance {instance_id} to be deleted; got PV={} PVC={} StatefulSet={} Service={} Pod={}",
                counts.persistent_volumes,
                counts.persistent_volume_claims,
                counts.stateful_sets,
                counts.services,
                counts.pods
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Debug)]
struct MaterializedObjectCounts {
    persistent_volumes: usize,
    persistent_volume_claims: usize,
    stateful_sets: usize,
    services: usize,
    pods: usize,
}

impl MaterializedObjectCounts {
    fn total(&self) -> usize {
        self.persistent_volumes
            + self.persistent_volume_claims
            + self.stateful_sets
            + self.services
            + self.pods
    }
}

async fn materialized_object_counts(
    kube: Client,
    namespace: &str,
    instance_id: &str,
) -> TestResult<MaterializedObjectCounts> {
    let labels = instance_label_selector(instance_id);
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pods: Api<Pod> = Api::namespaced(kube, namespace);

    Ok(MaterializedObjectCounts {
        persistent_volumes: pvs
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len(),
        persistent_volume_claims: pvcs
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len(),
        stateful_sets: stateful_sets
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len(),
        services: services
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len(),
        pods: pods
            .list(&ListParams::default().labels(&labels))
            .await?
            .items
            .len(),
    })
}

fn instance_label_selector(instance_id: &str) -> String {
    format!("sleepypods.io/instance-id={instance_id}")
}

async fn restart_control_plane_pod(
    kube: Client,
    namespace: &str,
    operator_endpoint: &str,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let selected = pods
        .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
        .await?
        .into_iter()
        .find(pod_ready)
        .ok_or("no ready control-plane pod found to restart")?;
    let old_name = selected
        .metadata
        .name
        .clone()
        .ok_or("control-plane pod is missing name")?;
    let old_uid = selected.metadata.uid.clone();

    pods.delete(&old_name, &DeleteParams::default()).await?;
    wait_for_replacement_control_plane_pod(pods, old_uid, Duration::from_secs(120)).await?;
    connect_operator(operator_endpoint).await?;

    Ok(())
}

async fn wait_for_replacement_control_plane_pod(
    pods: Api<Pod>,
    old_uid: Option<String>,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let ready = pods
            .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
            .await?
            .into_iter()
            .any(|pod| {
                pod_ready(&pod)
                    && old_uid
                        .as_ref()
                        .is_none_or(|uid| pod.metadata.uid.as_ref() != Some(uid))
            });
        if ready {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err("timed out waiting for replacement control-plane pod".into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn assert_single_node_cluster(kube: Client) -> TestResult<()> {
    let nodes: Api<k8s_openapi::api::core::v1::Node> = Api::all(kube);
    let node_count = nodes.list(&ListParams::default()).await?.items.len();
    if node_count != 1 {
        return Err(format!(
            "stateful hostPath continuity E2E requires exactly one Kubernetes node; found {node_count}"
        )
        .into());
    }
    Ok(())
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

fn kube_error_is_not_found(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(status) if status.is_not_found())
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

fn manifest_template(config: &E2eConfig) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::StatefulSet as i32,
            name: Some(literal_text(WORKLOAD_NAME)),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "app".to_owned(),
                image: Some(literal_text(&config.app_image)),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: APP_PORT,
                }],
                env: Vec::new(),
            }),
            termination_grace_period_seconds: None,
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(&config.sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
            app_protocol: Default::default(),
        }),
        service: Some(ServiceTemplate {
            name: Some(literal_text(WORKLOAD_NAME)),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: APP_PORT,
                target_port: APP_PORT,
            }],
        }),
        volumes: vec![VolumeTemplate {
            name: VOLUME_NAME.to_owned(),
            mount_path: Some(literal_text(MOUNT_PATH)),
            pv_name: Some(tenant_suffixed_text("e2e-", "-pv")),
            pvc_name: Some(tenant_suffixed_text("e2e-", "-pvc")),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
            capacity: Some(literal_text("1Mi")),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
            storage_class_name: Some(literal_text("sleepypods-kind-static")),
            source: Some(PersistentVolumeSourceTemplate {
                kind: Some(persistent_volume_source_template::Kind::HostPath(
                    HostPathVolumeSourceTemplate {
                        path: Some(tenant_suffixed_text("/tmp/sleepypods-kind-e2e/", "")),
                        r#type: Some(literal_text("DirectoryOrCreate")),
                    },
                )),
            }),
        }],
        raw_objects: vec![],
    }
}

/// A schema that requires each value the class templates read.
fn required_values(fields: &[&str]) -> WorkloadValueSchema {
    WorkloadValueSchema {
        fields: fields
            .iter()
            .map(|field| {
                (
                    (*field).to_owned(),
                    WorkloadValueFieldRule {
                        required: true,
                        default_value: None,
                    },
                )
            })
            .collect(),
        allow_extra: true,
    }
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}

fn instance_value_text(field: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::InstanceValue(field.to_owned())),
        }],
    }
}

fn tenant_suffixed_text(prefix: &str, suffix: &str) -> TemplateText {
    TemplateText {
        parts: vec![
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(prefix.to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue("tenant".to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(suffix.to_owned())),
            },
        ],
    }
}

fn unique_marker() -> TestResult<String> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(format!("marker-{millis}-{}", std::process::id()))
}

#[tokio::test]
async fn stateful_one_shot_read_preserves_marker_and_never_retries_error() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (status, body) in [(200, "read:retained-marker\n"), (502, "bad gateway\n")] {
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
            assert!(request.starts_with(b"GET /read/retained-marker HTTP/1.1\r\n"));
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            // Hold EOF until the complete framed response has returned.
            let _ = tokio::time::timeout(Duration::from_secs(1), hold).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
        });
        let response = http_get_once(
            address,
            ROUTE_HOST,
            "/read/retained-marker",
            Duration::from_millis(500),
        )
        .await;
        let _ = release.send(());
        server.await.unwrap();
        let response = response.unwrap();
        assert_eq!(response.status, status);
        assert_eq!(response.body, body);
        let assertion = assert_response(&response, "re-wake read", "read:retained-marker\n");
        assert_eq!(assertion.is_ok(), status == 200);
    }
}

#[tokio::test]
async fn stateful_one_shot_deadline_closes_owned_connection() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut received))
            .await
            .expect("request timeout must close its owned socket")
            .unwrap();
        assert!(received.starts_with(b"GET /read/retained-marker HTTP/1.1\r\n"));
        assert_eq!(
            received.windows(4).filter(|part| *part == b"GET ").count(),
            1
        );
    });
    let error = http_get_once(
        address,
        ROUTE_HOST,
        "/read/retained-marker",
        Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("exceeded"));
    server.await.unwrap();
}

#[tokio::test]
async fn stateful_one_shot_external_cancellation_closes_owned_connection() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
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
        assert!(request.starts_with(b"GET /read/retained-marker HTTP/1.1\r\n"));
        sent.send(()).unwrap();
        let mut remaining = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut remaining))
            .await
            .expect("caller cancellation must close the owned driver socket")
            .unwrap();
        assert!(remaining.is_empty());
    });
    let request = tokio::spawn(http_get_once(
        address,
        ROUTE_HOST,
        "/read/retained-marker",
        Duration::from_secs(130),
    ));
    tokio::time::timeout(Duration::from_secs(1), received)
        .await
        .unwrap()
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    server.await.unwrap();
}
