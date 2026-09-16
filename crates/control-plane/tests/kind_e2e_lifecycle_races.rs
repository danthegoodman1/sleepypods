#[path = "support/deletion_observation.rs"]
mod deletion_observation;

#[path = "support/http_once.rs"]
mod http_once;

use std::{
    collections::HashMap, env, error::Error, net::SocketAddr, process::Command, time::Duration,
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient,
    proxy_control_plane_client::ProxyControlPlaneClient, proxy_subscribe_request,
    proxy_subscribe_response, proxy_wake_instance_response, route_identity,
    sidecar_control_plane_client::SidecarControlPlaneClient, sidecar_report_idle_response,
    template_text_part, ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest,
    CreateRouteBindingRequest, CreateWorkloadClassVersionRequest, DeleteInstanceRequest,
    DeleteRouteBindingRequest, HostPathVolumeSourceTemplate, HttpRouteIdentity, Instance,
    InstanceState as PbInstanceState, ManifestTemplate, PersistentVolumeAccessMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, ProtocolRoute,
    ProxyRouteInvalidationReason, ProxySubscribeRequest, ProxySubscribeRouteRequest,
    ProxyWakeInstanceRequest, ReconcileMaterializationRequest, RouteHost, RouteHostKind,
    RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarReportIdleRequest, SidecarTemplate,
    TemplateText, TemplateTextPart, VolumeTemplate, WorkloadClassVersionRef, WorkloadKind,
    WorkloadSleepPolicy, WorkloadTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
};
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{PersistentVolume, PersistentVolumeClaim, Pod, Service},
    rbac::v1::Role,
};
use kube::{
    api::{DeleteParams, ListParams, PostParams},
    Api, Client, Error as KubeError,
};
use tokio::time::{sleep, timeout, Instant};
use tonic::{
    codegen::tokio_stream::{wrappers::ReceiverStream, StreamExt},
    transport::{Channel, Endpoint},
};

#[path = "support/ready_age_fixture.rs"]
mod ready_age_fixture;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const NORMAL_CLASS_ID: &str = "lifecycle-normal";
const SLEEP_WHILE_WAKING_CLASS_ID: &str = "lifecycle-sleep-waking";
const DELETE_WHILE_WAKING_CLASS_ID: &str = "lifecycle-delete-waking";
const DELETE_WHILE_DRAINING_CLASS_ID: &str = "lifecycle-delete-draining";
const FAILED_RETRY_CLASS_ID: &str = "lifecycle-failed-retry";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const APP_MARKER: &str = "sleepypods-routing-app";
const WORKLOAD_NAME_LABEL: &str = "sleepypods.io/workload-name";
const INSTANCE_GENERATION_LABEL: &str = "sleepypods.io/instance-generation";

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-lifecycle-races.sh or an equivalent kind deployment"]
async fn lifecycle_races_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_LIFECYCLE_RACES").as_deref() != Ok("1") {
        eprintln!(
            "skipping lifecycle-race kind E2E because SLEEPYPODS_KIND_E2E_LIFECYCLE_RACES=1 is not set"
        );
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_workload_class(
        &mut operator,
        WorkloadClassFixture {
            class_id: NORMAL_CLASS_ID,
            app_image: &config.app_image,
            sidecar_image: &config.sidecar_image,
            kind: WorkloadKind::Deployment,
            workload_name: "lifecycle-normal-app",
            volumes: Vec::new(),
            idempotency_suffix: "normal",
        },
    )
    .await?;
    create_workload_class(
        &mut operator,
        WorkloadClassFixture {
            class_id: SLEEP_WHILE_WAKING_CLASS_ID,
            app_image: &config.sleep_while_waking_image,
            sidecar_image: &config.sidecar_image,
            kind: WorkloadKind::Deployment,
            workload_name: "lifecycle-sleep-waking-app",
            volumes: Vec::new(),
            idempotency_suffix: "sleep-waking",
        },
    )
    .await?;
    create_workload_class(
        &mut operator,
        WorkloadClassFixture {
            class_id: DELETE_WHILE_WAKING_CLASS_ID,
            app_image: &config.delete_while_waking_image,
            sidecar_image: &config.sidecar_image,
            kind: WorkloadKind::StatefulSet,
            workload_name: "lifecycle-delete-waking-app",
            volumes: stateful_volumes(
                "lifecycle-delete-waking-pv",
                "lifecycle-delete-waking-pvc",
                "/tmp/sleepypods-kind-e2e-lifecycle-races/delete-waking",
            ),
            idempotency_suffix: "delete-waking",
        },
    )
    .await?;
    create_workload_class(
        &mut operator,
        WorkloadClassFixture {
            class_id: DELETE_WHILE_DRAINING_CLASS_ID,
            app_image: &config.app_image,
            sidecar_image: &config.sidecar_image,
            kind: WorkloadKind::StatefulSet,
            workload_name: "lifecycle-delete-draining-app",
            volumes: stateful_volumes(
                "lifecycle-delete-draining-pv",
                "lifecycle-delete-draining-pvc",
                "/tmp/sleepypods-kind-e2e-lifecycle-races/delete-draining",
            ),
            idempotency_suffix: "delete-draining",
        },
    )
    .await?;
    create_workload_class(
        &mut operator,
        WorkloadClassFixture {
            class_id: FAILED_RETRY_CLASS_ID,
            app_image: &config.failed_retry_image,
            sidecar_image: &config.sidecar_image,
            kind: WorkloadKind::Deployment,
            workload_name: "lifecycle-failed-retry-app",
            volumes: Vec::new(),
            idempotency_suffix: "failed-retry",
        },
    )
    .await?;

    eprintln!("==> lifecycle race E2E: concurrent wake");
    concurrent_wake_calls_converge(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: ReportIdle while waking");
    report_idle_while_waking_cannot_finalize_cleanup(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: delete while waking");
    delete_while_waking_cleans_pending_objects(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: delete while draining");
    delete_while_draining_cleans_deleting_materialization(&mut operator, kube.clone(), &config)
        .await?;

    eprintln!("==> lifecycle race E2E: failed wake retry");
    failed_wake_retry_rejects_stale_generation(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: stale sidecar ReportIdle");
    stale_sidecar_report_is_rejected(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: idle member UID and external replica drift");
    idle_report_requires_current_single_member(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: route reassignment subscription invalidation");
    route_reassignment_invalidates_active_subscription(&mut operator, kube, &config).await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    /// The listener carrying the proxy and sidecar services.
    workload_endpoint: String,
    frontline_addr: SocketAddr,
    cluster_name: String,
    app_image: String,
    sleep_while_waking_image: String,
    delete_while_waking_image: String,
    failed_retry_image: String,
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
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-lifecycle-races".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19851".to_owned()),
            workload_endpoint: env::var("SLEEPYPODS_E2E_WORKLOAD_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19852".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19880".to_owned())
                .parse()?,
            cluster_name: env::var("SLEEPYPODS_KIND_CLUSTER")
                .unwrap_or_else(|_| "sleepypods-e2e-lifecycle-races-test".to_owned()),
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/routing-app:kind-e2e-lifecycle-races".to_owned()),
            sleep_while_waking_image: env::var("SLEEPYPODS_E2E_SLEEP_WHILE_WAKING_IMAGE")
                .unwrap_or_else(|_| {
                    "sleepypods/routing-app-sleep-waking:kind-e2e-lifecycle-races".to_owned()
                }),
            delete_while_waking_image: env::var("SLEEPYPODS_E2E_DELETE_WHILE_WAKING_IMAGE")
                .unwrap_or_else(|_| {
                    "sleepypods/routing-app-delete-waking:kind-e2e-lifecycle-races".to_owned()
                }),
            failed_retry_image: env::var("SLEEPYPODS_E2E_FAILED_RETRY_IMAGE").unwrap_or_else(
                |_| "sleepypods/routing-app-failed-retry:kind-e2e-lifecycle-races".to_owned(),
            ),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-lifecycle-races".to_owned()),
        })
    }
}

async fn concurrent_wake_calls_converge(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-concurrent",
        "lifecycle-concurrent-route",
        "concurrent.lifecycle.sleepypods.test",
        "concurrent",
        "concurrent",
    )
    .await?;
    let created =
        wait_for_instance_state(operator, "lifecycle-concurrent", PbInstanceState::Cold, 30)
            .await?;

    let mut tasks = Vec::new();
    for _ in 0..4 {
        let endpoint = config.workload_endpoint.clone();
        tasks.push(tokio::spawn(async move {
            let mut proxy = connect_proxy(&endpoint).await?;
            let response = proxy
                .wake_instance(ProxyWakeInstanceRequest {
                    instance_id: "lifecycle-concurrent".to_owned(),
                    expected_generation: created.generation,
                    backend_generation: None,
                })
                .await?
                .into_inner();
            Ok::<_, Box<dyn Error + Send + Sync>>(response)
        }));
    }

    let mut accepted = 0;
    let mut conflicts = 0;
    for task in tasks {
        match task.await??.outcome {
            Some(proxy_wake_instance_response::Outcome::Ready(result)) => {
                accepted += 1;
                if result.instance_generation <= created.generation {
                    return Err(format!(
                        "concurrent wake returned non-advanced generation {}",
                        result.instance_generation
                    )
                    .into());
                }
            }
            Some(proxy_wake_instance_response::Outcome::StillWaking(result)) => {
                accepted += 1;
                if result.instance_generation <= created.generation {
                    return Err("accepted wake must advance the generation".into());
                }
            }
            Some(proxy_wake_instance_response::Outcome::GenerationConflict(conflict)) => {
                conflicts += 1;
                if conflict.expected_generation != created.generation {
                    return Err(format!(
                        "wake conflict expected generation {}, wanted {}",
                        conflict.expected_generation, created.generation
                    )
                    .into());
                }
            }
            other => return Err(format!("unexpected concurrent wake outcome: {other:?}").into()),
        }
    }
    if accepted == 0 || accepted + conflicts != 4 {
        return Err(format!(
            "expected accepted wakes and optional generation conflicts, got {accepted}/{conflicts}"
        )
        .into());
    }

    let running = wait_for_instance_state(
        operator,
        "lifecycle-concurrent",
        PbInstanceState::Running,
        30,
    )
    .await?;
    assert_workload_generation(
        kube,
        &config.namespace,
        "lifecycle-concurrent-6fce159e",
        running.generation,
    )
    .await?;
    assert_response_identifies(
        &wait_for_instance_response(
            config,
            "concurrent wake route",
            "concurrent.lifecycle.sleepypods.test",
            "/",
            "concurrent",
            60,
        )
        .await?,
        "concurrent",
    )?;

    Ok(())
}

async fn report_idle_while_waking_cannot_finalize_cleanup(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        SLEEP_WHILE_WAKING_CLASS_ID,
        "lifecycle-sleep-waking",
        "lifecycle-sleep-waking-route",
        "sleep-waking.lifecycle.sleepypods.test",
        "sleep-waking",
        "sleep-waking",
    )
    .await?;
    let wake_addr = config.frontline_addr;
    let wake = tokio::spawn(async move {
        http_get_with_timeout(
            wake_addr,
            "sleep-waking.lifecycle.sleepypods.test",
            "/",
            Duration::from_secs(180),
        )
        .await
    });
    let waking = wait_for_instance_state(
        operator,
        "lifecycle-sleep-waking",
        PbInstanceState::Waking,
        60,
    )
    .await?;

    let mut sidecar = connect_sidecar(&config.workload_endpoint).await?;
    let stale = sidecar
        .report_idle(SidecarReportIdleRequest {
            // Waking instances reject idle observations before membership inspection.
            pod_uid: "waking-non-member".to_owned(),
            instance_id: "lifecycle-sleep-waking".to_owned(),
            expected_generation: waking.generation,
            active_count: 0,
        })
        .await?
        .into_inner();
    match stale.outcome {
        Some(sidecar_report_idle_response::Outcome::Unavailable(unavailable)) => {
            if unavailable.instance_generation != waking.generation {
                return Err("ReportIdle while waking returned the wrong generation".into());
            }
        }
        other => return Err(format!("ReportIdle while waking was not rejected: {other:?}").into()),
    }

    load_image_into_kind(config, &config.sleep_while_waking_image)?;
    delete_workload_pods(
        kube.clone(),
        &config.namespace,
        "lifecycle-sleep-waking-aea16aec",
    )
    .await?;
    let _ = timeout(Duration::from_secs(5), wake).await;
    wait_for_instance_response(
        config,
        "sleep-while-waking retry",
        "sleep-waking.lifecycle.sleepypods.test",
        "/",
        "sleep-waking",
        180,
    )
    .await?;
    let running = wait_for_instance_state(
        operator,
        "lifecycle-sleep-waking",
        PbInstanceState::Running,
        30,
    )
    .await?;
    assert_workload_generation(
        kube,
        &config.namespace,
        "lifecycle-sleep-waking-aea16aec",
        running.generation,
    )
    .await?;

    Ok(())
}

async fn delete_while_waking_cleans_pending_objects(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        DELETE_WHILE_WAKING_CLASS_ID,
        "lifecycle-delete-waking",
        "lifecycle-delete-waking-route",
        "delete-waking.lifecycle.sleepypods.test",
        "delete-waking",
        "delete-waking",
    )
    .await?;
    let wake_addr = config.frontline_addr;
    let wake = tokio::spawn(async move {
        http_get_with_timeout(
            wake_addr,
            "delete-waking.lifecycle.sleepypods.test",
            "/",
            Duration::from_secs(180),
        )
        .await
    });
    wait_for_instance_state(
        operator,
        "lifecycle-delete-waking",
        PbInstanceState::Waking,
        60,
    )
    .await?;
    wait_for_stateful_objects_present(
        kube.clone(),
        &config.namespace,
        "lifecycle-delete-waking-0b1fe29d",
        "lifecycle-delete-waking-pvc-0b1fe29d",
        "lifecycle-delete-waking-pv-0b1fe29d",
        60,
    )
    .await?;
    delete_instance_until_deleted(operator, "lifecycle-delete-waking", 60).await?;
    let _ = timeout(Duration::from_secs(5), wake).await;
    assert_instance_not_found(operator, "lifecycle-delete-waking").await?;
    wait_for_stateful_objects_absent(
        kube,
        &config.namespace,
        "lifecycle-delete-waking-0b1fe29d",
        "lifecycle-delete-waking-pvc-0b1fe29d",
        "lifecycle-delete-waking-pv-0b1fe29d",
        60,
    )
    .await?;
    assert_no_backend_response(
        config,
        "delete-waking.lifecycle.sleepypods.test",
        "delete-waking",
        5,
    )
    .await?;

    Ok(())
}

async fn delete_while_draining_cleans_deleting_materialization(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        DELETE_WHILE_DRAINING_CLASS_ID,
        "lifecycle-delete-draining",
        "lifecycle-delete-draining-route",
        "delete-draining.lifecycle.sleepypods.test",
        "delete-draining",
        "delete-draining",
    )
    .await?;
    wait_for_instance_response(
        config,
        "delete-draining wake",
        "delete-draining.lifecycle.sleepypods.test",
        "/",
        "delete-draining",
        180,
    )
    .await?;
    let running = wait_for_instance_state(
        operator,
        "lifecycle-delete-draining",
        PbInstanceState::Running,
        30,
    )
    .await?;
    wait_for_stateful_objects_present(
        kube.clone(),
        &config.namespace,
        "lifecycle-delete-draining-3cf19f0e",
        "lifecycle-delete-draining-pvc-3cf19f0e",
        "lifecycle-delete-draining-pv-3cf19f0e",
        60,
    )
    .await?;

    let workloads: Api<StatefulSet> = Api::namespaced(kube.clone(), &config.namespace);
    let workload = workloads.get("lifecycle-delete-draining-3cf19f0e").await?;
    let materialization_id = workload
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("sleepypods.io/materialization-id"))
        .ok_or("current StatefulSet has no materialization identity")?
        .clone();
    let mut removed_delete_permission = false;
    let blocked_result: TestResult<()> = async {
        // Record the owned change before dispatch so an ambiguous Role update
        // still reaches restoration; never add a permission absent beforehand.
        set_control_plane_statefulset_delete_permission(
            kube.clone(),
            &config.namespace,
            false,
            &mut removed_delete_permission,
        )
        .await?;
        if !removed_delete_permission {
            return Err("fixture requires the original StatefulSet delete permission".into());
        }
        sleep(Duration::from_secs(2)).await;
        let mut sidecar = connect_sidecar(&config.workload_endpoint).await?;
        let (pod_uid, pod_generation) =
            current_idle_member(kube.clone(), &config.namespace, "lifecycle-delete-draining")
                .await?;
        age_ready_for_controlled_lifecycle_case(
            config,
            "lifecycle-delete-draining",
            running.generation,
            pod_generation,
        )
        .await?;
        let idle_response = sidecar
            .report_idle(SidecarReportIdleRequest {
                pod_uid,
                instance_id: "lifecycle-delete-draining".to_owned(),
                expected_generation: pod_generation,
                active_count: 0,
            })
            .await?
            .into_inner();
        if !matches!(
            idle_response.outcome,
            Some(sidecar_report_idle_response::Outcome::Accepted(_))
        ) {
            return Err(format!("ReportIdle while draining returned {idle_response:?}").into());
        }
        let draining = wait_for_instance_state(
            operator,
            "lifecycle-delete-draining",
            PbInstanceState::Draining,
            30,
        )
        .await?;
        if draining.generation <= running.generation {
            return Err("idle acceptance did not advance the generation".into());
        }
        let deletion = operator
            .delete_instance(DeleteInstanceRequest {
                instance_id: "lifecycle-delete-draining".to_owned(),
                expected_generation: Some(draining.generation),
            })
            .await?
            .into_inner();
        if !deletion.accepted {
            return Err("delete while cleanup is blocked must be durably accepted".into());
        }
        let deleting = wait_for_instance_state(
            operator,
            "lifecycle-delete-draining",
            PbInstanceState::Deleting,
            30,
        )
        .await?;
        if deleting.generation != draining.generation + 1 {
            return Err("Delete acceptance must advance the draining generation once".into());
        }
        // A definite forbidden delete is permanent until an operator corrects
        // permissions and explicitly enqueues recovery. Wait until no old denied
        // attempt can publish a later failure over that recovery enqueue.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let status = operator
                .reconcile_materialization(ReconcileMaterializationRequest {
                    materialization_id: materialization_id.clone(),
                    status_only: true,
                })
                .await?
                .into_inner();
            if status.found
                && status.state == "Deleting"
                && status.failure_kind == "permanent"
                && status.lease_owner.is_empty()
                && status.uncertain_effect.is_none()
                && !status.observed_refs.is_empty()
            {
                let message = status.failure_message.to_ascii_lowercase();
                if !message.contains("failed to delete statefulset")
                    || !message.contains("lifecycle-delete-draining-3cf19f0e")
                    || !(message.contains("forbidden") || message.contains("403"))
                {
                    return Err(format!(
                        "terminal cleanup failed for an unexpected reason: {}",
                        status.failure_message
                    )
                    .into());
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "denied cleanup did not settle with retained inventory: {status:?}"
                )
                .into());
            }
            sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
    .await;
    let restoration = if removed_delete_permission {
        set_control_plane_statefulset_delete_permission(
            kube.clone(),
            &config.namespace,
            true,
            &mut false,
        )
        .await
    } else {
        Ok(())
    };
    match (blocked_result, restoration) {
        (Ok(()), Ok(())) => {}
        (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
        (Err(error), Err(restore)) => {
            return Err(format!(
                "blocked deletion failed: {error}; RBAC restoration failed: {restore}"
            )
            .into());
        }
    }
    // Resume the permanent permission failure through the ordinary scheduler,
    // then observe completion without replaying DeleteInstance.
    let recovery = operator
        .reconcile_materialization(ReconcileMaterializationRequest {
            materialization_id,
            status_only: false,
        })
        .await?
        .into_inner();
    if !recovery.attempted {
        return Err("corrected permanent cleanup failure must accept scheduler recovery".into());
    }
    wait_for_instance_deleted(operator, "lifecycle-delete-draining", 60).await?;
    assert_instance_not_found(operator, "lifecycle-delete-draining").await?;
    wait_for_stateful_objects_absent(
        kube,
        &config.namespace,
        "lifecycle-delete-draining-3cf19f0e",
        "lifecycle-delete-draining-pvc-3cf19f0e",
        "lifecycle-delete-draining-pv-3cf19f0e",
        60,
    )
    .await?;
    assert_no_backend_response(
        config,
        "delete-draining.lifecycle.sleepypods.test",
        "delete-draining",
        5,
    )
    .await?;

    Ok(())
}

async fn failed_wake_retry_rejects_stale_generation(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        FAILED_RETRY_CLASS_ID,
        "lifecycle-failed-retry",
        "lifecycle-failed-retry-route",
        "failed-retry.lifecycle.sleepypods.test",
        "failed-retry",
        "failed-retry",
    )
    .await?;
    wait_for_frontline_status(
        config,
        "failed-retry.lifecycle.sleepypods.test",
        "/",
        503,
        180,
    )
    .await?;
    let failed = wait_for_instance_state(
        operator,
        "lifecycle-failed-retry",
        PbInstanceState::Failed,
        30,
    )
    .await?;

    load_image_into_kind(config, &config.failed_retry_image)?;
    delete_workload_pods(
        kube.clone(),
        &config.namespace,
        "lifecycle-failed-retry-8f1a6842",
    )
    .await?;
    wait_for_instance_response(
        config,
        "failed wake retry",
        "failed-retry.lifecycle.sleepypods.test",
        "/",
        "failed-retry",
        180,
    )
    .await?;
    let running = wait_for_instance_state(
        operator,
        "lifecycle-failed-retry",
        PbInstanceState::Running,
        30,
    )
    .await?;

    let mut proxy = connect_proxy(&config.workload_endpoint).await?;
    let stale = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: "lifecycle-failed-retry".to_owned(),
            expected_generation: failed.generation,
            backend_generation: None,
        })
        .await?
        .into_inner();
    match stale.outcome {
        Some(proxy_wake_instance_response::Outcome::GenerationConflict(conflict)) => {
            if conflict.actual_generation != running.generation {
                return Err(format!(
                    "stale failed wake actual generation was {}, wanted {}",
                    conflict.actual_generation, running.generation
                )
                .into());
            }
        }
        other => return Err(format!("stale failed generation was not rejected: {other:?}").into()),
    }
    assert_workload_generation(
        kube,
        &config.namespace,
        "lifecycle-failed-retry-8f1a6842",
        running.generation,
    )
    .await?;

    Ok(())
}

async fn stale_sidecar_report_is_rejected(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-stale-sidecar",
        "lifecycle-stale-sidecar-route",
        "stale-sidecar.lifecycle.sleepypods.test",
        "stale-sidecar",
        "stale-sidecar",
    )
    .await?;
    wait_for_instance_response(
        config,
        "stale sidecar first wake",
        "stale-sidecar.lifecycle.sleepypods.test",
        "/",
        "stale-sidecar",
        180,
    )
    .await?;
    let first_running = wait_for_instance_state(
        operator,
        "lifecycle-stale-sidecar",
        PbInstanceState::Running,
        30,
    )
    .await?;
    let mut sidecar = connect_sidecar(&config.workload_endpoint).await?;
    let (first_pod_uid, first_pod_generation) =
        current_idle_member(kube.clone(), &config.namespace, "lifecycle-stale-sidecar").await?;
    age_ready_for_controlled_lifecycle_case(
        config,
        "lifecycle-stale-sidecar",
        first_running.generation,
        first_pod_generation,
    )
    .await?;
    sidecar
        .report_idle(SidecarReportIdleRequest {
            pod_uid: first_pod_uid.clone(),
            instance_id: "lifecycle-stale-sidecar".to_owned(),
            expected_generation: first_pod_generation,
            active_count: 0,
        })
        .await?;
    wait_for_workload_absent(
        kube.clone(),
        &config.namespace,
        "lifecycle-stale-sidecar-7f9f001b",
        60,
    )
    .await?;
    wait_for_instance_response(
        config,
        "stale sidecar second wake",
        "stale-sidecar.lifecycle.sleepypods.test",
        "/",
        "stale-sidecar",
        180,
    )
    .await?;
    let second_running = wait_for_instance_state(
        operator,
        "lifecycle-stale-sidecar",
        PbInstanceState::Running,
        30,
    )
    .await?;
    if second_running.generation <= first_running.generation + 2 {
        return Err(format!(
            "second wake generation {} did not move far enough beyond first running {}",
            second_running.generation, first_running.generation
        )
        .into());
    }

    let stale = sidecar
        .report_idle(SidecarReportIdleRequest {
            pod_uid: first_pod_uid,
            instance_id: "lifecycle-stale-sidecar".to_owned(),
            expected_generation: first_pod_generation,
            active_count: 0,
        })
        .await?
        .into_inner();
    match stale.outcome {
        Some(sidecar_report_idle_response::Outcome::GenerationConflict(conflict)) => {
            if conflict.actual_generation != second_running.generation {
                return Err(format!(
                    "stale sidecar conflict actual generation {}, wanted {}",
                    conflict.actual_generation, second_running.generation
                )
                .into());
            }
        }
        other => return Err(format!("stale sidecar report was not rejected: {other:?}").into()),
    }
    let after = get_instance(operator, "lifecycle-stale-sidecar").await?;
    assert_state(&after, PbInstanceState::Running)?;
    assert_eq!(after.generation, second_running.generation);

    Ok(())
}

async fn idle_report_requires_current_single_member(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    let instance_id = "lifecycle-membership";
    create_instance_and_route(
        operator,
        NORMAL_CLASS_ID,
        instance_id,
        "lifecycle-membership-route",
        "membership.lifecycle.sleepypods.test",
        "membership",
        "membership",
    )
    .await?;
    wait_for_instance_response(
        config,
        "idle membership wake",
        "membership.lifecycle.sleepypods.test",
        "/",
        "membership",
        180,
    )
    .await?;
    let running =
        wait_for_instance_state(operator, instance_id, PbInstanceState::Running, 30).await?;
    let (pod_uid, pod_generation) =
        current_idle_member(kube.clone(), &config.namespace, instance_id).await?;
    let mut sidecar = connect_sidecar(&config.workload_endpoint).await?;
    let report = SidecarReportIdleRequest {
        instance_id: instance_id.to_owned(),
        expected_generation: pod_generation,
        active_count: 0,
        pod_uid,
    };
    age_ready_for_controlled_lifecycle_case(
        config,
        instance_id,
        running.generation,
        pod_generation,
    )
    .await?;
    let mut stale = report.clone();
    stale.pod_uid = "replaced-pod-uid".to_owned();
    let rejected = sidecar
        .report_idle(stale)
        .await
        .expect_err("a non-current Pod UID cannot initiate sleep");
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    if rejected
        .metadata()
        .contains_key(sleepypods_api::IDLE_RETRY_AFTER_METADATA)
    {
        return Err("controlled membership check was still deferred by activation age".into());
    }

    // This mutation deliberately violates the managed workload contract. Detection
    // must keep the current workload awake even if the new peer is not ready yet.
    let deployments: Api<Deployment> = Api::namespaced(kube, &config.namespace);
    let controllers = deployments
        .list(&ListParams::default().labels(&format!("sleepypods.io/instance-id={instance_id}")))
        .await?;
    let [controller] = controllers.items.as_slice() else {
        return Err("expected exactly one managed Deployment".into());
    };
    let name = controller
        .metadata
        .name
        .as_deref()
        .ok_or("Deployment name is missing")?;
    let uid = controller
        .metadata
        .uid
        .as_deref()
        .filter(|uid| !uid.is_empty())
        .ok_or("Deployment UID is missing")?;
    let materialization_id = controller
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("sleepypods.io/materialization-id"))
        .filter(|id| !id.is_empty())
        .ok_or("current Deployment has no materialization identity")?;
    set_fixture_deployment_replicas(&deployments, name, uid, 2).await?;
    let rejected = sidecar
        .report_idle(report)
        .await
        .expect_err("external replica drift cannot authorize sleep");
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    if rejected
        .metadata()
        .contains_key(sleepypods_api::IDLE_RETRY_AFTER_METADATA)
    {
        return Err("controlled membership check was still deferred by activation age".into());
    }
    let after = get_instance(operator, instance_id).await?;
    assert_state(&after, PbInstanceState::Running)?;
    assert_eq!(after.generation, running.generation);

    set_fixture_deployment_replicas(&deployments, name, uid, 1).await?;
    // Terminating descendants can miss a cleanup scan, leaving the next real
    // backoff after 60s. Observe the same accepted operation's persisted deadline.
    deletion_observation::delete_with_original_deadline(
        operator,
        &after,
        materialization_id,
        Duration::from_secs(90),
    )
    .await?;
    Ok(())
}

// The Deployment controller may update status between GET and PATCH. Retry only
// a definite conflict, retaining the initially selected incarnation and changing
// no field other than replicas. The same fence applies to restoring one replica.
async fn set_fixture_deployment_replicas(
    deployments: &Api<Deployment>,
    name: &str,
    expected_uid: &str,
    replicas: i32,
) -> TestResult<()> {
    let operation = async {
        if expected_uid.is_empty() {
            return Err("original Deployment UID is missing".into());
        }
        for attempt in 0..8 {
            let current = deployments.get(name).await?;
            if current.metadata.uid.as_deref() != Some(expected_uid) {
                return Err("Deployment UID changed or is missing".into());
            }
            let resource_version = current
                .metadata
                .resource_version
                .as_deref()
                .filter(|version| !version.is_empty())
                .ok_or("Deployment resourceVersion is missing")?;
            let patch = serde_json::json!({
                "metadata": {"uid": expected_uid, "resourceVersion": resource_version},
                "spec": {"replicas": replicas},
            });
            match deployments
                .patch(
                    name,
                    &kube::api::PatchParams::default(),
                    &kube::api::Patch::Merge(patch),
                )
                .await
            {
                Ok(updated) => {
                    if updated.metadata.uid.as_deref() != Some(expected_uid)
                        || updated.spec.as_ref().and_then(|spec| spec.replicas) != Some(replicas)
                    {
                        return Err(
                            "Deployment patch response did not confirm UID and replicas".into()
                        );
                    }
                    return Ok(());
                }
                Err(KubeError::Api(error)) if error.code == 409 && attempt < 7 => {
                    sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!("the final patch attempt returns its result")
    };
    let result: TestResult<()> = match timeout(Duration::from_secs(5), operation).await {
        Ok(result) => result,
        Err(_) => Err("replica mutation exceeded total5s fixture deadline".into()),
    };
    result.map_err(|error| {
        format!("fixture Deployment {name} set replicas={replicas} failed: {error}").into()
    })
}

#[path = "kind_e2e_lifecycle_races/replica_fixture_tests.rs"]
mod replica_fixture_tests;

async fn route_reassignment_invalidates_active_subscription(
    operator: &mut OperatorControlPlaneClient<Channel>,
    _kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-reassign-old",
        "old",
        "reassign-old",
    )
    .await?;
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-reassign-new",
        "new",
        "reassign-new",
    )
    .await?;
    create_route(
        operator,
        "lifecycle-reassign-old-route",
        "lifecycle-reassign-old",
        "reassign.lifecycle.sleepypods.test",
        "reassign-old",
    )
    .await?;
    let aliases = [
        (
            "lifecycle-reassign-old-alias-route",
            "lifecycle-reassign-old",
            "old-setup.lifecycle.sleepypods.test",
            "old",
        ),
        (
            "lifecycle-reassign-new-alias-route",
            "lifecycle-reassign-new",
            "new-setup.lifecycle.sleepypods.test",
            "new",
        ),
    ];
    for (route_id, instance_id, host, _) in aliases {
        create_route(operator, route_id, instance_id, host, route_id).await?;
    }
    // Prewarm through durable acceptance and read-only state observation. Never
    // resolve this frontend host until both backends can already serve traffic.
    let mut proxy = connect_proxy(&config.workload_endpoint).await?;
    for instance_id in ["lifecycle-reassign-old", "lifecycle-reassign-new"] {
        let instance = get_instance(operator, instance_id).await?;
        let wake = proxy
            .wake_instance(ProxyWakeInstanceRequest {
                instance_id: instance_id.to_owned(),
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
        wait_for_instance_state(operator, instance_id, PbInstanceState::Running, 180).await?;
    }
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
        assert_response_identifies(&response, target)?;
    }
    sleep(Duration::from_secs(1)).await;
    let (requests, request_stream) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(request_stream))
        .await?
        .into_inner();
    requests
        .send(subscribe_route_request(
            "lifecycle-reassign-old-subscribe",
            "reassign.lifecycle.sleepypods.test",
        ))
        .await?;
    let resolved = expect_route_resolved(next_subscribe_response(&mut responses).await?);
    if resolved
        .route
        .as_ref()
        .map(|route| route.instance_id.as_str())
        != Some("lifecycle-reassign-old")
    {
        return Err(format!("active subscription did not resolve old route: {resolved:?}").into());
    }

    let cached_at = Instant::now();
    let old = timeout(
        Duration::from_secs(1),
        http_get_with_timeout(
            config.frontline_addr,
            "reassign.lifecycle.sleepypods.test",
            "/",
            Duration::from_secs(1),
        ),
    )
    .await
    .map_err(|_| "initial frontend cache warmup exceeded total1s fixture budget")??;
    assert_response_identifies(&old, "old")?;
    timeout(Duration::from_secs(1), async {
        let deleted = operator
            .delete_route_binding(DeleteRouteBindingRequest {
                route_binding_id: "lifecycle-reassign-old-route".to_owned(),
            })
            .await?
            .into_inner();
        if !deleted.deleted {
            return Err("route cutover did not delete the old binding".into());
        }
        create_route(
            operator,
            "lifecycle-reassign-new-route",
            "lifecycle-reassign-new",
            "reassign.lifecycle.sleepypods.test",
            "reassign-new",
        )
        .await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| "healthy route cutover commits exceeded1s fixture budget")??;

    // Direct subscription delivery and the independently delivered frontend
    // invalidation share a bounded window; one does not imply the other arrived.
    timeout(Duration::from_secs(3), async {
        let invalidated = expect_route_invalidated(next_subscribe_response(&mut responses).await?);
        if invalidated.subscription_id != resolved.subscription_id
            || invalidated.reason != ProxyRouteInvalidationReason::RouteRemoved as i32
        {
            return Err(format!("unexpected route invalidation: {invalidated:?}").into());
        }
        requests
            .send(subscribe_route_request(
                "lifecycle-reassign-new-subscribe",
                "reassign.lifecycle.sleepypods.test",
            ))
            .await?;
        let new_resolved = expect_route_resolved(next_subscribe_response(&mut responses).await?);
        if new_resolved
            .route
            .as_ref()
            .map(|route| route.instance_id.as_str())
            != Some("lifecycle-reassign-new")
        {
            return Err(
                format!("new subscription did not resolve new route: {new_resolved:?}").into(),
            );
        }
        loop {
            let response = http_get_with_timeout(
                config.frontline_addr,
                "reassign.lifecycle.sleepypods.test",
                "/",
                Duration::from_millis(500),
            )
            .await?;
            if response_body_identifies(&response, "new") {
                assert_response_identifies(&response, "new")?;
                break;
            }
            if !response_body_identifies(&response, "old") {
                return Err(
                    format!("cutover returned unknown or failed response: {response:?}").into(),
                );
            }
            assert_response_identifies(&response, "old")?;
            sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| "route notification did not converge within3s (before10s TTL)")??;
    if cached_at.elapsed() >= Duration::from_secs(5) {
        return Err("route freshness proof exceeded its pre-TTL fixture budget".into());
    }
    timeout(Duration::from_secs(2), async {
        for _ in 0..10 {
            let response = http_get_with_timeout(
                config.frontline_addr,
                "reassign.lifecycle.sleepypods.test",
                "/",
                Duration::from_millis(500),
            )
            .await?;
            assert_response_identifies(&response, "new")?;
            sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| "post-freshness checks exceeded2s fixture budget")??;
    Ok(())
}

struct WorkloadClassFixture<'a> {
    class_id: &'a str,
    app_image: &'a str,
    sidecar_image: &'a str,
    kind: WorkloadKind,
    workload_name: &'a str,
    volumes: Vec<VolumeTemplate>,
    idempotency_suffix: &'a str,
}

async fn create_workload_class(
    operator: &mut OperatorControlPlaneClient<Channel>,
    fixture: WorkloadClassFixture<'_>,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: format!("kind-e2e-lifecycle-{}-class", fixture.idempotency_suffix),
            class_id: fixture.class_id.to_owned(),
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
            template: Some(ManifestTemplate {
                workload: Some(workload_template(
                    fixture.kind,
                    fixture.workload_name,
                    fixture.app_image,
                )),
                sidecar: Some(sidecar_template(fixture.sidecar_image)),
                service: Some(service_template(fixture.workload_name)),
                volumes: fixture.volumes,
                raw_objects: vec![],
            }),
            sleep_policy: Some(sleep_policy()),
            exclusivity_keys: vec![],
        })
        .await?;
    Ok(())
}

async fn create_instance_and_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    class_id: &str,
    instance_id: &str,
    route_id: &str,
    host: &str,
    target: &str,
    idempotency_suffix: &str,
) -> TestResult<()> {
    create_instance(operator, class_id, instance_id, target, idempotency_suffix).await?;
    create_route(operator, route_id, instance_id, host, idempotency_suffix).await
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
            idempotency_key: format!("kind-e2e-lifecycle-{idempotency_suffix}-instance"),
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
            idempotency_key: format!("kind-e2e-lifecycle-{idempotency_suffix}-route"),
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
            Ok(channel) => return Ok(OperatorControlPlaneClient::new(channel)),
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for operator gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn connect_proxy(endpoint: &str) -> TestResult<ProxyControlPlaneClient<Channel>> {
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(30))
        .connect()
        .await?;
    Ok(ProxyControlPlaneClient::new(channel))
}

async fn connect_sidecar(endpoint: &str) -> TestResult<SidecarControlPlaneClient<Channel>> {
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(30))
        .connect()
        .await?;
    Ok(SidecarControlPlaneClient::new(channel))
}

async fn get_instance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<Instance> {
    Ok(operator
        .get_instance(control_plane::api::pb::GetInstanceRequest {
            instance_id: instance_id.to_owned(),
        })
        .await?
        .into_inner())
}

async fn assert_instance_not_found(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<()> {
    let error = operator
        .get_instance(control_plane::api::pb::GetInstanceRequest {
            instance_id: instance_id.to_owned(),
        })
        .await
        .expect_err("deleted instance should not be found");
    if error.code() != tonic::Code::NotFound {
        return Err(format!(
            "expected deleted instance {instance_id} to return NotFound, got {:?}: {}",
            error.code(),
            error.message()
        )
        .into());
    }
    Ok(())
}

async fn delete_instance_until_deleted(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    timeout_secs: u64,
) -> TestResult<()> {
    let current = get_instance(operator, instance_id).await?;
    operator
        .delete_instance(DeleteInstanceRequest {
            instance_id: instance_id.to_owned(),
            expected_generation: Some(current.generation),
        })
        .await?;
    wait_for_instance_deleted(operator, instance_id, timeout_secs).await
}

async fn wait_for_instance_deleted(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    timeout_secs: u64,
) -> TestResult<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match operator
            .get_instance(control_plane::api::pb::GetInstanceRequest {
                instance_id: instance_id.to_owned(),
            })
            .await
        {
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(()),
            Ok(_) if Instant::now() < deadline => sleep(Duration::from_millis(100)).await,
            other => return Err(format!("accepted deletion did not complete: {other:?}").into()),
        }
    }
}

async fn wait_for_instance_state(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    expected: PbInstanceState,
    timeout_secs: u64,
) -> TestResult<Instance> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
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

fn assert_state(instance: &Instance, expected: PbInstanceState) -> TestResult<()> {
    let actual = instance_state(instance);
    if actual != expected {
        return Err(format!(
            "expected instance {} state {expected:?}, got {actual:?}",
            instance.instance_id
        )
        .into());
    }
    Ok(())
}

fn instance_state(instance: &Instance) -> PbInstanceState {
    PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified)
}

async fn wait_for_instance_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    timeout_secs: u64,
) -> TestResult<HttpResponse> {
    wait_for_instance_response_rejecting_stale(
        config,
        context,
        host,
        path,
        target,
        &[],
        timeout_secs,
    )
    .await
}

async fn wait_for_instance_response_rejecting_stale(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    stale_targets: &[&str],
    timeout_secs: u64,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response)
                if response.status == 200 && response_body_identifies(&response, target) =>
            {
                assert_response_identifies(&response, target)?;
                return Ok(response);
            }
            Ok(response) => {
                for stale in stale_targets {
                    if response.status == 200 && response_body_identifies(&response, stale) {
                        return Err(format!(
                            "{context} served stale target {stale:?}: {:?}",
                            response.body
                        )
                        .into());
                    }
                }
                format!("HTTP {} body {:?}", response.status, response.body)
            }
            Err(error) => error.to_string(),
        };
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {context} host {host} path {path}: {last_error}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_frontline_status(
    config: &E2eConfig,
    host: &str,
    path: &str,
    status: u16,
    timeout_secs: u64,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == status => return Ok(response),
            Ok(response) if Instant::now() >= deadline => {
                return Err(format!(
                    "timed out waiting for HTTP {status}; got {} body {:?}",
                    response.status, response.body
                )
                .into());
            }
            Err(error) if Instant::now() >= deadline => return Err(error),
            _ => sleep(Duration::from_secs(1)).await,
        }
    }
}

async fn assert_no_backend_response(
    config: &E2eConfig,
    host: &str,
    stale_target: &str,
    attempts: usize,
) -> TestResult<()> {
    for _ in 0..attempts {
        if let Ok(response) = http_get(config.frontline_addr, host, "/").await {
            if response.status == 200 && response_body_identifies(&response, stale_target) {
                return Err(format!(
                    "deleted route served stale backend target {stale_target:?}: {:?}",
                    response.body
                )
                .into());
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    Ok(())
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
    Ok(HttpResponse {
        status: response.status().as_u16(),
        body: response.into_body(),
    })
}

fn response_body_identifies(response: &HttpResponse, target: &str) -> bool {
    response.body.contains(APP_MARKER) && response.body.contains(&format!("instance={target}\n"))
}

fn assert_response_identifies(response: &HttpResponse, target: &str) -> TestResult<()> {
    if response.status != 200 || !response_body_identifies(response, target) {
        return Err(format!(
            "expected HTTP 200 response for target {target:?}, got {} body {:?}",
            response.status, response.body
        )
        .into());
    }
    Ok(())
}

async fn next_subscribe_response(
    responses: &mut tonic::Streaming<control_plane::api::pb::ProxySubscribeResponse>,
) -> TestResult<control_plane::api::pb::ProxySubscribeResponse> {
    timeout(Duration::from_secs(30), responses.next())
        .await?
        .ok_or("subscribe stream closed")?
        .map_err(|error| error.into())
}

fn subscribe_route_request(request_id: &str, host: &str) -> ProxySubscribeRequest {
    ProxySubscribeRequest {
        input: Some(proxy_subscribe_request::Input::SubscribeRoute(
            ProxySubscribeRouteRequest {
                request_id: request_id.to_owned(),
                identity: Some(http_route_identity(host)),
            },
        )),
    }
}

fn expect_route_resolved(
    response: control_plane::api::pb::ProxySubscribeResponse,
) -> control_plane::api::pb::ProxyRouteResolvedResponse {
    let Some(proxy_subscribe_response::Output::RouteResolved(resolved)) = response.output else {
        panic!("expected route resolved response");
    };
    resolved
}

fn expect_route_invalidated(
    response: control_plane::api::pb::ProxySubscribeResponse,
) -> control_plane::api::pb::ProxyRouteInvalidatedResponse {
    let Some(proxy_subscribe_response::Output::RouteInvalidated(invalidated)) = response.output
    else {
        panic!("expected route invalidated response");
    };
    invalidated
}

fn load_image_into_kind(config: &E2eConfig, image: &str) -> TestResult<()> {
    let status = Command::new("kind")
        .args([
            "load",
            "docker-image",
            image,
            "--name",
            &config.cluster_name,
        ])
        .status()?;
    if !status.success() {
        return Err(format!(
            "kind load docker-image {image} --name {} failed with status {status}",
            config.cluster_name
        )
        .into());
    }
    Ok(())
}

async fn delete_workload_pods(
    kube: Client,
    namespace: &str,
    workload_name: &str,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let selector = format!("{WORKLOAD_NAME_LABEL}={workload_name}");
    for pod in pods.list(&ListParams::default().labels(&selector)).await? {
        if let Some(name) = pod.metadata.name {
            let _ = pods.delete(&name, &DeleteParams::default()).await;
        }
    }
    Ok(())
}

async fn set_control_plane_statefulset_delete_permission(
    kube: Client,
    namespace: &str,
    allow: bool,
    changed: &mut bool,
) -> TestResult<()> {
    let roles: Api<Role> = Api::namespaced(kube, namespace);
    let mut role = roles.get("sleepypods-control-plane").await?;
    let rules = role
        .rules
        .as_mut()
        .ok_or("sleepypods-control-plane Role has no rules")?;
    let rule = rules
        .iter_mut()
        .find(|rule| {
            rule.api_groups
                .as_ref()
                .is_some_and(|groups| groups.iter().any(|group| group == "apps"))
                && rule.resources.as_ref().is_some_and(|resources| {
                    resources.iter().any(|resource| resource == "statefulsets")
                })
        })
        .ok_or("sleepypods-control-plane Role has no StatefulSet rule")?;

    let had_permission = rule.verbs.iter().any(|verb| verb == "delete");
    if had_permission == allow {
        return Ok(());
    }
    *changed = true;
    if allow {
        if !rule.verbs.iter().any(|verb| verb == "delete") {
            rule.verbs.push("delete".to_owned());
        }
    } else {
        rule.verbs.retain(|verb| verb != "delete");
    }

    roles
        .replace("sleepypods-control-plane", &PostParams::default(), &role)
        .await?;
    Ok(())
}

async fn assert_workload_generation(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    generation: u64,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let expected = generation.to_string();
    let deployment = deployments.get(workload_name).await?;
    let service = services.get(workload_name).await?;
    assert_object_generation_label(
        "Deployment",
        workload_name,
        &deployment.metadata.labels,
        &expected,
    )?;
    assert_object_generation_label(
        "Service",
        workload_name,
        &service.metadata.labels,
        &expected,
    )?;
    Ok(())
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
    workload_name: &str,
    timeout_secs: u64,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if is_not_found(deployments.get(workload_name).await)
            && is_not_found(services.get(workload_name).await)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for workload {namespace}/{workload_name} absence"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_stateful_objects_present(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    pvc_name: &str,
    pv_name: &str,
    timeout_secs: u64,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let pvs: Api<PersistentVolume> = Api::all(kube);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if stateful_sets.get(workload_name).await.is_ok()
            && services.get(workload_name).await.is_ok()
            && pvcs.get(pvc_name).await.is_ok()
            && pvs.get(pv_name).await.is_ok()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for StatefulSet/Service/PVC/PV for {namespace}/{workload_name}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_stateful_objects_absent(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    pvc_name: &str,
    pv_name: &str,
    timeout_secs: u64,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let pvs: Api<PersistentVolume> = Api::all(kube);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if is_not_found(stateful_sets.get(workload_name).await)
            && is_not_found(services.get(workload_name).await)
            && is_not_found(pvcs.get(pvc_name).await)
            && is_not_found(pvs.get(pv_name).await)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for StatefulSet/Service/PVC/PV cleanup for {namespace}/{workload_name}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

fn workload_template(kind: WorkloadKind, _name: &str, image: &str) -> WorkloadTemplate {
    WorkloadTemplate {
        kind: kind as i32,
        name: Some(target_text("lifecycle-", "")),
        replicas: Some(1),
        app_container: Some(ContainerTemplate {
            name: "app".to_owned(),
            image: Some(literal_text(image)),
            ports: vec![ContainerPortTemplate {
                name: Some("http".to_owned()),
                container_port: APP_PORT,
            }],
            env: vec![control_plane::api::pb::EnvVarTemplate {
                name: "SLEEPYPODS_E2E_INSTANCE".to_owned(),
                value: Some(target_text("", "")),
            }],
        }),
    }
}

fn sidecar_template(image: &str) -> SidecarTemplate {
    SidecarTemplate {
        name: "sleepypods-sidecar".to_owned(),
        image: Some(literal_text(image)),
        listen_port: SIDECAR_PORT,
        mode: None,
    }
}

fn service_template(_name: &str) -> ServiceTemplate {
    ServiceTemplate {
        name: Some(target_text("lifecycle-", "")),
        ports: vec![ServicePortTemplate {
            name: Some("http".to_owned()),
            port: APP_PORT,
            target_port: APP_PORT,
        }],
    }
}

fn sleep_policy() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms: 300_000,
        idle_retry_backoff_ms: 500,
        drain_grace_timeout_ms: 500,
        idle_timeout_override: None,
    }
}

fn stateful_volumes(pv_name: &str, pvc_name: &str, host_path: &str) -> Vec<VolumeTemplate> {
    vec![VolumeTemplate {
        name: "data".to_owned(),
        mount_path: Some(literal_text("/data")),
        pv_name: Some(literal_text(pv_name)),
        pvc_name: Some(literal_text(pvc_name)),
        access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
        capacity: Some(literal_text("1Mi")),
        reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
        storage_class_name: Some(literal_text("sleepypods-kind-static")),
        source: Some(PersistentVolumeSourceTemplate {
            kind: Some(
                control_plane::api::pb::persistent_volume_source_template::Kind::HostPath(
                    HostPathVolumeSourceTemplate {
                        path: Some(literal_text(host_path)),
                        r#type: Some(literal_text("DirectoryOrCreate")),
                    },
                ),
            ),
        }),
    }]
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

// The three synthetic ReportIdle cases exercise Kubernetes membership and
// lifecycle interleavings, not elapsed idle policy. Only their exact current
// Ready record is aged; stateless/stateful/restart gates retain real time.
async fn age_ready_for_controlled_lifecycle_case(
    config: &E2eConfig,
    instance: &str,
    generation: u64,
    projection_generation: u64,
) -> TestResult<()> {
    let sql = ready_age_fixture::age_ready_sql(
        "kind-e2e-lifecycle-races",
        &config.namespace,
        instance,
        generation,
        projection_generation,
    )?;
    let namespace = config.namespace.clone();
    let output = tokio::task::spawn_blocking(move || -> TestResult<_> {
        let mut child = Command::new("kubectl")
            .args([
                "--request-timeout=10s",
                "-n",
                &namespace,
                "exec",
                "deployment/sleepypods-postgres",
                "-c",
                "postgres",
                "--",
                "env",
                "PGOPTIONS=-c statement_timeout=5000",
                "psql",
                "-X",
                "-qAt",
                "-U",
                "sleepypods",
                "-d",
                "sleepypods",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                &sql,
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while child.try_wait()?.is_none() {
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("controlled Ready-age fixture command timed out".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(child.wait_with_output()?)
    })
    .await??;
    if !output.status.success() || String::from_utf8_lossy(&output.stdout).trim() != "aged-ready:1"
    {
        return Err(format!(
            "controlled Ready-age update failed: status={}, stdout={}, stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    eprintln!(
        "lifecycle fixture: controlled Ready age >300s for {instance}, instance generation{generation}, projection generation{projection_generation}"
    );
    Ok(())
}

async fn current_idle_member(
    kube: Client,
    namespace: &str,
    instance_id: &str,
) -> TestResult<(String, u64)> {
    let pods: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(kube, namespace);
    let pods = pods
        .list(
            &kube::api::ListParams::default()
                .labels(&format!("sleepypods.io/instance-id={instance_id}")),
        )
        .await?;
    let [pod] = pods.items.as_slice() else {
        return Err("expected exactly one workload pod".into());
    };
    let uid = pod.metadata.uid.clone().ok_or("pod UID is missing")?;
    let generation = pod
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("sleepypods.io/instance-generation"))
        .ok_or("pod generation is missing")?
        .parse()?;
    Ok((uid, generation))
}
