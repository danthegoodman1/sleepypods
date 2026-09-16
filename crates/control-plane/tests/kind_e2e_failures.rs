use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, persistent_volume_source_template,
    proxy_control_plane_client::ProxyControlPlaneClient, proxy_wake_instance_response,
    route_identity, template_text_part, ContainerPortTemplate, ContainerTemplate,
    CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    HostPathVolumeSourceTemplate, HttpRouteIdentity, Instance, InstanceState as PbInstanceState,
    ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
    PersistentVolumeSourceTemplate, ProtocolRoute, ProxyWakeInstanceRequest,
    RawKubernetesManifestTemplate, ReconcileMaterializationRequest, RouteHost, RouteHostKind,
    RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
    TemplateTextPart, VolumeTemplate, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy,
    WorkloadTemplate, WorkloadValueSchema,
};
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{Event, PersistentVolume, PersistentVolumeClaim, Service},
    discovery::v1::EndpointSlice,
};
use kube::{api::ListParams, Api, Client, Error as KubeError};
use serde_json::json;
use tokio::time::{sleep, Instant};
use tonic::{
    transport::{Channel, Endpoint},
    Code,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const GOOD_CLASS_ID: &str = "failure-good";
const GOOD_INSTANCE_ID: &str = "failure-good";
const GOOD_ROUTE_ID: &str = "failure-good-route";
const GOOD_ROUTE_HOST: &str = "good.failure.sleepypods.test";
const GOOD_WORKLOAD_NAME: &str = "failure-good-app";
const MISSING_ROUTE_HOST: &str = "missing.failure.sleepypods.test";
const INSTANCE_ID_LABEL: &str = "sleepypods.io/instance-id";

const READINESS_CLASS_ID: &str = "failure-readiness";
const READINESS_INSTANCE_ID: &str = "failure-readiness";
const READINESS_ROUTE_ID: &str = "failure-readiness-route";
const READINESS_ROUTE_HOST: &str = "readiness.failure.sleepypods.test";
const READINESS_WORKLOAD_NAME: &str = "failure-readiness-app";
const READINESS_BAD_IMAGE: &str = "sleepypods/missing-readiness-app:kind-e2e-failures";

const UNBOUND_CLASS_ID: &str = "failure-unbound";
const UNBOUND_INSTANCE_ID: &str = "failure-unbound";
const UNBOUND_ROUTE_ID: &str = "failure-unbound-route";
const UNBOUND_ROUTE_HOST: &str = "unbound.failure.sleepypods.test";
const UNBOUND_WORKLOAD_NAME: &str = "failure-unbound-app";
const UNBOUND_PV_NAME: &str = "failure-unbound-pv";
const UNBOUND_FIRST_PVC_NAME: &str = "failure-unbound-a-pvc";
const UNBOUND_SECOND_PVC_NAME: &str = "failure-unbound-b-pvc";
// Explicit test-owned stale reservation, never the UID of a live claim.
const UNBOUND_PREVIOUS_CLAIM_UID: &str = "00000000-0000-4000-8000-000000000001";

const DUPLICATE_VOLUME_CLASS_ID: &str = "failure-duplicate-volume";
const DUPLICATE_VOLUME_INSTANCE_ID: &str = "failure-duplicate-volume";
const INVALID_VOLUME_CLASS_ID: &str = "failure-invalid-volume";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const APP_RESPONSE: &str = "sleepypods-stateless-app";

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-failures.sh or an equivalent kind deployment"]
async fn failure_paths_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_FAILURES").as_deref() != Ok("1") {
        eprintln!(
            "skipping failure-path kind E2E because SLEEPYPODS_KIND_E2E_FAILURES=1 is not set"
        );
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    bad_route_misses_without_materializing_unrelated_instance(&mut operator, kube.clone(), &config)
        .await?;
    invalid_volume_template_is_rejected_by_operator_api(&mut operator, kube.clone(), &config)
        .await?;
    duplicate_volume_inventory_is_rejected_before_acceptance(&mut operator, kube.clone(), &config)
        .await?;
    wake_readiness_failure_is_bounded_and_rejects_stale_proxy_generation(
        &mut operator,
        kube.clone(),
        &config,
    )
    .await?;
    missing_pvc_binding_fails_materialization_without_ready_backend(&mut operator, kube, &config)
        .await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    /// The listener carrying the proxy and sidecar services.
    workload_endpoint: String,
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
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-failures".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19651".to_owned()),
            workload_endpoint: env::var("SLEEPYPODS_E2E_WORKLOAD_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19652".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19680".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/stateless-app:kind-e2e-failures".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-failures".to_owned()),
        })
    }
}

async fn bad_route_misses_without_materializing_unrelated_instance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_class_instance_and_route(
        operator,
        ClassInstanceRoute {
            class_id: GOOD_CLASS_ID,
            instance_id: GOOD_INSTANCE_ID,
            route_id: GOOD_ROUTE_ID,
            route_host: GOOD_ROUTE_HOST,
            workload_name: GOOD_WORKLOAD_NAME,
            app_image: &config.app_image,
            sidecar_image: &config.sidecar_image,
            workload_kind: WorkloadKind::Deployment,
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        },
        "good",
    )
    .await?;

    let created = wait_for_instance_state(
        operator,
        GOOD_INSTANCE_ID,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    assert_eq!(created.generation, 0);

    let miss = wait_for_frontline_status(
        config,
        MISSING_ROUTE_HOST,
        "/",
        404,
        Duration::from_secs(30),
    )
    .await?;
    if miss.body.contains(APP_RESPONSE) {
        return Err(format!(
            "bad route miss unexpectedly returned backend body: {:?}",
            miss.body
        )
        .into());
    }

    let after = get_instance(operator, GOOD_INSTANCE_ID).await?;
    assert_state(&after, PbInstanceState::Cold)?;
    assert_eq!(after.generation, created.generation);
    assert_deployment_and_service_absent(kube, &config.namespace, GOOD_INSTANCE_ID).await?;

    Ok(())
}

async fn invalid_volume_template_is_rejected_by_operator_api(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    let error = operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: "kind-e2e-failure-invalid-volume-class".to_owned(),
            class_id: INVALID_VOLUME_CLASS_ID.to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: Default::default(),
                allow_extra: false,
            }),
            template_generation: 1,
            template: Some(ManifestTemplate {
                workload: Some(workload_template(
                    WorkloadKind::StatefulSet,
                    "failure-invalid-volume-app",
                    &config.app_image,
                )),
                sidecar: Some(sidecar_template(config)),
                service: Some(service_template("failure-invalid-volume-app")),
                volumes: vec![VolumeTemplate {
                    name: "data".to_owned(),
                    mount_path: Some(literal_text("/data")),
                    pv_name: Some(literal_text("failure-invalid-volume-pv")),
                    pvc_name: Some(literal_text("failure-invalid-volume-pvc")),
                    access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
                    capacity: Some(literal_text("1Mi")),
                    reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
                    storage_class_name: Some(literal_text("sleepypods-kind-static")),
                    source: None,
                }],
                raw_objects: vec![],
            }),
            sleep_policy: Some(sleep_policy()),
            exclusivity_keys: vec![],
        })
        .await
        .expect_err("invalid volume template must be rejected by deployed operator API");

    if error.code() != Code::InvalidArgument {
        return Err(format!(
            "expected invalid volume template to return InvalidArgument, got {:?}: {}",
            error.code(),
            error.message()
        )
        .into());
    }
    if !error
        .message()
        .contains("template.volumes.source is required")
    {
        return Err(format!(
            "invalid volume template error did not identify the bad field: {}",
            error.message()
        )
        .into());
    }
    assert_stateful_service_pvc_pv_absent(
        kube,
        &config.namespace,
        "failure-invalid-volume-app",
        "failure-invalid-volume-pvc",
        "failure-invalid-volume-pv",
    )
    .await?;

    Ok(())
}

async fn wake_readiness_failure_is_bounded_and_rejects_stale_proxy_generation(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    let mut proxy = connect_proxy(&config.workload_endpoint).await?;

    create_class_instance_and_route(
        operator,
        ClassInstanceRoute {
            class_id: READINESS_CLASS_ID,
            instance_id: READINESS_INSTANCE_ID,
            route_id: READINESS_ROUTE_ID,
            route_host: READINESS_ROUTE_HOST,
            workload_name: READINESS_WORKLOAD_NAME,
            app_image: READINESS_BAD_IMAGE,
            sidecar_image: &config.sidecar_image,
            workload_kind: WorkloadKind::Deployment,
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        },
        "readiness",
    )
    .await?;
    let created = wait_for_instance_state(
        operator,
        READINESS_INSTANCE_ID,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;

    let started = Instant::now();
    let failed_response = wait_for_frontline_status(
        config,
        READINESS_ROUTE_HOST,
        "/",
        503,
        Duration::from_secs(180),
    )
    .await?;
    if failed_response.body.contains(APP_RESPONSE) {
        return Err("readiness failure unexpectedly returned backend response body".into());
    }
    let elapsed = started.elapsed();
    if elapsed > Duration::from_secs(180) {
        return Err(format!("wake readiness failure was not bounded; elapsed {elapsed:?}").into());
    }

    let failed = wait_for_instance_state(
        operator,
        READINESS_INSTANCE_ID,
        PbInstanceState::Failed,
        Duration::from_secs(30),
    )
    .await?;
    if failed.generation <= created.generation {
        return Err(format!(
            "expected failed wake to advance generation beyond {}, got {}",
            created.generation, failed.generation
        )
        .into());
    }
    assert_deployment_service_no_ready_backend(
        kube.clone(),
        &config.namespace,
        READINESS_INSTANCE_ID,
    )
    .await?;

    let stale_generation_rejection = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: READINESS_INSTANCE_ID.to_owned(),
            expected_generation: created.generation,
            backend_generation: None,
        })
        .await?
        .into_inner();
    match stale_generation_rejection.outcome {
        Some(proxy_wake_instance_response::Outcome::GenerationConflict(conflict)) => {
            if conflict.expected_generation != created.generation
                || conflict.actual_generation != failed.generation
            {
                return Err(format!(
                    "stale WakeInstance conflict reported expected/actual {}/{}, wanted {}/{}",
                    conflict.expected_generation,
                    conflict.actual_generation,
                    created.generation,
                    failed.generation
                )
                .into());
            }
        }
        other => {
            return Err(
                format!("expected stale WakeInstance generation conflict, got {other:?}").into(),
            );
        }
    }
    let after_stale = get_instance(operator, READINESS_INSTANCE_ID).await?;
    assert_state(&after_stale, PbInstanceState::Failed)?;
    assert_eq!(
        after_stale.generation, failed.generation,
        "stale cached route generation must be rejected without changing current instance generation"
    );

    Ok(())
}

async fn duplicate_volume_inventory_is_rejected_before_acceptance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_class_instance_and_route(
        operator,
        ClassInstanceRoute {
            class_id: DUPLICATE_VOLUME_CLASS_ID,
            instance_id: DUPLICATE_VOLUME_INSTANCE_ID,
            route_id: "failure-duplicate-volume-route",
            route_host: "duplicate.failure.sleepypods.test",
            workload_name: "failure-duplicate-volume-app",
            app_image: &config.app_image,
            sidecar_image: &config.sidecar_image,
            workload_kind: WorkloadKind::StatefulSet,
            volumes: duplicate_pv_volume_templates(),
            raw_objects: Vec::new(),
        },
        "duplicate-volume",
    )
    .await?;
    let created = get_instance(operator, DUPLICATE_VOLUME_INSTANCE_ID).await?;
    assert_state(&created, PbInstanceState::Cold)?;
    let mut proxy = connect_proxy(&config.workload_endpoint).await?;
    let error = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: DUPLICATE_VOLUME_INSTANCE_ID.to_owned(),
            expected_generation: created.generation,
            backend_generation: None,
        })
        .await
        .expect_err("duplicate rendered inventory must be rejected before wake acceptance");
    if error.code() != Code::Internal
        || !error
            .message()
            .contains("duplicate rendered Kubernetes object ref v1 PersistentVolume")
    {
        return Err(format!("expected duplicate PV render rejection, got {error}").into());
    }
    let after = get_instance(operator, DUPLICATE_VOLUME_INSTANCE_ID).await?;
    assert_state(&after, PbInstanceState::Cold)?;
    if after.generation != created.generation {
        return Err("invalid duplicate inventory advanced instance generation".into());
    }
    let status = operator
        .reconcile_materialization(ReconcileMaterializationRequest {
            materialization_id: format!(
                "{DUPLICATE_VOLUME_INSTANCE_ID}:kind-e2e-failures:{}",
                config.namespace
            ),
            status_only: true,
        })
        .await?
        .into_inner();
    if status.found {
        return Err("duplicate inventory left a durable materialization despite rejection".into());
    }
    let selector = ListParams::default().labels(&instance_selector(DUPLICATE_VOLUME_INSTANCE_ID));
    assert_no_labeled_objects(
        "PersistentVolume",
        DUPLICATE_VOLUME_INSTANCE_ID,
        Api::<PersistentVolume>::all(kube.clone())
            .list(&selector)
            .await?,
    )?;
    assert_no_labeled_objects(
        "PersistentVolumeClaim",
        DUPLICATE_VOLUME_INSTANCE_ID,
        Api::<PersistentVolumeClaim>::namespaced(kube.clone(), &config.namespace)
            .list(&selector)
            .await?,
    )?;
    assert_no_labeled_objects(
        "StatefulSet",
        DUPLICATE_VOLUME_INSTANCE_ID,
        Api::<StatefulSet>::namespaced(kube.clone(), &config.namespace)
            .list(&selector)
            .await?,
    )?;
    assert_deployment_and_service_absent(kube, &config.namespace, DUPLICATE_VOLUME_INSTANCE_ID)
        .await
}

async fn missing_pvc_binding_fails_materialization_without_ready_backend(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_class_instance_and_route(
        operator,
        ClassInstanceRoute {
            class_id: UNBOUND_CLASS_ID,
            instance_id: UNBOUND_INSTANCE_ID,
            route_id: UNBOUND_ROUTE_ID,
            route_host: UNBOUND_ROUTE_HOST,
            workload_name: UNBOUND_WORKLOAD_NAME,
            app_image: &config.app_image,
            sidecar_image: &config.sidecar_image,
            workload_kind: WorkloadKind::StatefulSet,
            volumes: Vec::new(),
            raw_objects: unbound_static_inventory(&config.namespace),
        },
        "unbound",
    )
    .await?;
    let created = wait_for_instance_state(
        operator,
        UNBOUND_INSTANCE_ID,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;

    let mut proxy = connect_proxy(&config.workload_endpoint).await?;
    let started = Instant::now();
    let accepted = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: UNBOUND_INSTANCE_ID.to_owned(),
            expected_generation: created.generation,
            backend_generation: None,
        })
        .await?
        .into_inner();
    let Some(proxy_wake_instance_response::Outcome::StillWaking(accepted)) = accepted.outcome
    else {
        return Err("valid unbound inventory must receive durable wake acceptance".into());
    };
    if accepted.instance_id != UNBOUND_INSTANCE_ID
        || Some(accepted.instance_generation) != created.generation.checked_add(1)
    {
        return Err(format!("accepted unbound wake must identify exactly the next Waking generation: Cold={}, accepted={accepted:?}", created.generation).into());
    }
    // Observe a real Kubernetes Pending claim before terminal cleanup can remove it.
    wait_for_stale_claim_inventory(
        kube.clone(),
        &config.namespace,
        accepted.instance_generation,
    )
    .await?;
    assert_no_stateful_backend(kube.clone(), &config.namespace).await?;

    // One request waits on the accepted wake; it must never receive a ready backend.
    let failed_response = tokio::time::timeout(
        Duration::from_secs(60),
        http_get(config.frontline_addr, UNBOUND_ROUTE_HOST, "/"),
    )
    .await??;
    if failed_response.status != 503 || failed_response.body.contains(APP_RESPONSE) {
        return Err(format!(
            "unbound PVC must return HTTP 503 without backend body, got {} {:?}",
            failed_response.status, failed_response.body
        )
        .into());
    }
    let failed = wait_for_instance_state(
        operator,
        UNBOUND_INSTANCE_ID,
        PbInstanceState::Failed,
        Duration::from_secs(30),
    )
    .await?;
    if failed.generation < accepted.instance_generation
        || started.elapsed() > Duration::from_secs(90)
    {
        return Err(
            "accepted PVC binding failure did not reach Failed within the bounded gate".into(),
        );
    }
    assert_no_stateful_backend(kube.clone(), &config.namespace).await?;
    wait_for_unbound_inventory_cleanup(kube, &config.namespace).await?;

    Ok(())
}

#[derive(Debug)]
struct ClassInstanceRoute<'a> {
    class_id: &'a str,
    instance_id: &'a str,
    route_id: &'a str,
    route_host: &'a str,
    workload_name: &'a str,
    app_image: &'a str,
    sidecar_image: &'a str,
    workload_kind: WorkloadKind,
    volumes: Vec<VolumeTemplate>,
    raw_objects: Vec<RawKubernetesManifestTemplate>,
}

async fn create_class_instance_and_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    spec: ClassInstanceRoute<'_>,
    idempotency_prefix: &str,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: format!("kind-e2e-failure-{idempotency_prefix}-class"),
            class_id: spec.class_id.to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: Default::default(),
                allow_extra: false,
            }),
            template_generation: 1,
            template: Some(ManifestTemplate {
                workload: Some(workload_template(
                    spec.workload_kind,
                    spec.workload_name,
                    spec.app_image,
                )),
                sidecar: Some(SidecarTemplate {
                    name: "sleepypods-sidecar".to_owned(),
                    image: Some(literal_text(spec.sidecar_image)),
                    listen_port: SIDECAR_PORT,
                    mode: None,
                }),
                service: Some(service_template(spec.workload_name)),
                volumes: spec.volumes,
                raw_objects: spec.raw_objects,
            }),
            sleep_policy: Some(sleep_policy()),
            exclusivity_keys: vec![],
        })
        .await?;

    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: format!("kind-e2e-failure-{idempotency_prefix}-instance"),
            instance_id: spec.instance_id.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: spec.class_id.to_owned(),
                version: 1,
            }),
            values: HashMap::new(),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: format!("kind-e2e-failure-{idempotency_prefix}-route"),
            route_binding_id: spec.route_id.to_owned(),
            instance_id: spec.instance_id.to_owned(),
            identity: Some(RouteIdentity {
                kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
                    host: Some(RouteHost {
                        kind: RouteHostKind::Exact as i32,
                        host: spec.route_host.to_owned(),
                    }),
                    path_prefix: None,
                })),
            }),
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
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let channel = Endpoint::from_shared(endpoint.to_owned())?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        match channel {
            Ok(channel) => return Ok(ProxyControlPlaneClient::new(channel)),
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for proxy gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
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

async fn wait_for_frontline_status(
    config: &E2eConfig,
    host: &str,
    path: &str,
    expected_status: u16,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == expected_status => return Ok(response),
            Ok(response) => format!(
                "frontline returned HTTP {} with body {:?}",
                response.status, response.body
            ),
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for frontline HTTP {expected_status} for host {host} path {path}: {last_error}"
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

async fn assert_deployment_and_service_absent(
    kube: Client,
    namespace: &str,
    instance_id: &str,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    assert_no_labeled_objects(
        "Deployment",
        instance_id,
        deployments
            .list(&ListParams::default().labels(&instance_selector(instance_id)))
            .await?,
    )?;
    assert_no_labeled_objects(
        "Service",
        instance_id,
        services
            .list(&ListParams::default().labels(&instance_selector(instance_id)))
            .await?,
    )?;
    Ok(())
}

async fn assert_stateful_service_pvc_pv_absent(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    pvc_name: &str,
    pv_name: &str,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let pvs: Api<PersistentVolume> = Api::all(kube);
    if !is_not_found(stateful_sets.get(workload_name).await) {
        return Err(format!("unexpected StatefulSet {namespace}/{workload_name} exists").into());
    }
    if !is_not_found(services.get(workload_name).await) {
        return Err(format!("unexpected Service {namespace}/{workload_name} exists").into());
    }
    if !is_not_found(pvcs.get(pvc_name).await) {
        return Err(
            format!("unexpected PersistentVolumeClaim {namespace}/{pvc_name} exists").into(),
        );
    }
    if !is_not_found(pvs.get(pv_name).await) {
        return Err(format!("unexpected PersistentVolume {pv_name} exists").into());
    }
    Ok(())
}

async fn assert_deployment_service_no_ready_backend(
    kube: Client,
    namespace: &str,
    instance_id: &str,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let endpoint_slices: Api<EndpointSlice> = Api::namespaced(kube, namespace);
    let selector = instance_selector(instance_id);
    // Failed wake cleanup is autonomous: either the partial projection remains
    // without a ready backend, or safe cleanup has already removed it.
    let deployment_items = deployments
        .list(&ListParams::default().labels(&selector))
        .await?
        .items;
    let service_items = services
        .list(&ListParams::default().labels(&selector))
        .await?
        .items;
    if deployment_items.len() > 1 || service_items.len() > 1 {
        return Err("failed wake produced duplicate projection objects".into());
    }
    let Some(service) = service_items.into_iter().next() else {
        return Ok(());
    };
    let service_name = service
        .metadata
        .name
        .ok_or_else(|| format!("Service for instance {instance_id} is missing metadata.name"))?;

    let selector = format!("kubernetes.io/service-name={service_name}");
    let slices = endpoint_slices
        .list(&ListParams::default().labels(&selector))
        .await?;
    if slices.iter().any(endpoint_slice_has_ready_endpoint) {
        return Err(
            format!("Service {namespace}/{service_name} unexpectedly has a ready backend").into(),
        );
    }
    Ok(())
}

async fn wait_for_stale_claim_inventory(
    kube: Client,
    namespace: &str,
    generation: u64,
) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let events: Api<Event> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let (Some(pv), Some(pvc)) = (
            pvs.get_opt(UNBOUND_PV_NAME).await?,
            pvcs.get_opt(UNBOUND_FIRST_PVC_NAME).await?,
        ) {
            assert_stale_claim_reservation(&pv, &pvc, namespace)?;
            for (kind, metadata) in [("PV", &pv.metadata), ("PVC", &pvc.metadata)] {
                assert_inventory_ownership_for_accepted_wake(kind, metadata, generation)?;
            }
            let phase = pvc.status.as_ref().and_then(|v| v.phase.as_deref());
            if phase == Some("Pending") {
                // An initial Pending snapshot alone is not proof of a binding
                // fault. Require the real controller's rejection for this UID.
                let uid = pvc.metadata.uid.as_deref().ok_or("live PVC missing UID")?;
                let selector = ListParams::default().fields(&format!("involvedObject.uid={uid}"));
                if events.list(&selector).await?.items.iter().any(|event| {
                    event.reason.as_deref() == Some("FailedBinding")
                        && event.involved_object.uid.as_deref() == Some(uid)
                        && event.message.as_deref().is_some_and(|message| {
                            message.contains(UNBOUND_PV_NAME)
                                && message.contains("already bound to a different claim")
                        })
                }) {
                    eprintln!("confirmed stale claim reservation: pv={UNBOUND_PV_NAME} previous_uid={UNBOUND_PREVIOUS_CLAIM_UID} current_pvc_uid={uid} phase=Pending reason=FailedBinding");
                    return Ok(());
                }
            }
            if phase == Some("Bound") {
                return Err("PVC reserved to another claim UID unexpectedly became Bound".into());
            }
        }
        if Instant::now() >= deadline {
            return Err(
                "did not observe current-UID Pending PVC and stale-reservation FailedBinding before the wake deadline"
                    .into(),
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn assert_stale_claim_reservation(
    pv: &PersistentVolume,
    pvc: &PersistentVolumeClaim,
    namespace: &str,
) -> TestResult<()> {
    let pv_spec = pv.spec.as_ref().ok_or("unbound PV missing spec")?;
    let pvc_spec = pvc.spec.as_ref().ok_or("unbound PVC missing spec")?;
    let claim = pv_spec.claim_ref.as_ref().ok_or("PV missing claimRef")?;
    let current_uid = pvc
        .metadata
        .uid
        .as_deref()
        .ok_or("PVC missing current UID")?;
    if pv.metadata.name.as_deref() != Some(UNBOUND_PV_NAME)
        || pvc.metadata.name.as_deref() != Some(UNBOUND_FIRST_PVC_NAME)
        || pvc.metadata.namespace.as_deref() != Some(namespace)
        || pv_spec.persistent_volume_reclaim_policy.as_deref() != Some("Retain")
        || pv_spec
            .capacity
            .as_ref()
            .and_then(|v| v.get("storage"))
            .map(|v| v.0.as_str())
            != Some("1Mi")
        || claim.name.as_deref() != Some(UNBOUND_FIRST_PVC_NAME)
        || claim.namespace.as_deref() != Some(namespace)
        || claim.uid.as_deref() != Some(UNBOUND_PREVIOUS_CLAIM_UID)
        || current_uid.is_empty()
        || current_uid == UNBOUND_PREVIOUS_CLAIM_UID
        || pvc_spec.volume_name.as_deref() != Some(UNBOUND_PV_NAME)
        || pvc_spec
            .resources
            .as_ref()
            .and_then(|v| v.requests.as_ref())
            .and_then(|v| v.get("storage"))
            .map(|v| v.0.as_str())
            != Some("1Mi")
    {
        return Err("PV/PVC do not match the exact retained stale-claim-UID reservation".into());
    }
    Ok(())
}

fn assert_inventory_ownership_for_accepted_wake(
    kind: &str,
    metadata: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    accepted_generation: u64,
) -> TestResult<()> {
    // Waking is the accepted instance CAS revision. Kubernetes ownership uses
    // the immutable projected Running incarnation selected before acceptance.
    let projection_generation = accepted_generation
        .checked_add(1)
        .ok_or("accepted wake generation cannot have a projected successor")?;
    let labels = metadata
        .labels
        .as_ref()
        .ok_or("unbound inventory missing ownership labels")?;
    if labels.get(INSTANCE_ID_LABEL).map(String::as_str) != Some(UNBOUND_INSTANCE_ID)
        || labels.get("sleepypods.io/instance-generation")
            != Some(&projection_generation.to_string())
        || labels
            .get("app.kubernetes.io/managed-by")
            .map(String::as_str)
            != Some("sleepypods")
    {
        return Err(format!(
            "{kind} ownership mismatch: accepted Waking generation={accepted_generation}, expected projection generation={projection_generation}, actual labels={labels:?}"
        ).into());
    }
    Ok(())
}

async fn assert_no_stateful_backend(kube: Client, namespace: &str) -> TestResult<()> {
    let selector = ListParams::default().labels(&instance_selector(UNBOUND_INSTANCE_ID));
    assert_no_labeled_objects(
        "StatefulSet",
        UNBOUND_INSTANCE_ID,
        Api::<StatefulSet>::namespaced(kube.clone(), namespace)
            .list(&selector)
            .await?,
    )?;
    assert_no_labeled_objects(
        "Service",
        UNBOUND_INSTANCE_ID,
        Api::<Service>::namespaced(kube, namespace)
            .list(&selector)
            .await?,
    )?;
    Ok(())
}

async fn wait_for_unbound_inventory_cleanup(kube: Client, namespace: &str) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube, namespace);
    let selector = ListParams::default().labels(&instance_selector(UNBOUND_INSTANCE_ID));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if pvs.list(&selector).await?.items.is_empty()
            && pvcs.list(&selector).await?.items.is_empty()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("failed unbound inventory did not finish safe object cleanup".into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn instance_selector(instance_id: &str) -> String {
    format!("{INSTANCE_ID_LABEL}={instance_id}")
}

fn assert_no_labeled_objects<K: Clone>(
    kind: &str,
    instance_id: &str,
    objects: kube::api::ObjectList<K>,
) -> TestResult<()> {
    if !objects.items.is_empty() {
        return Err(format!("unexpected {kind} exists for instance {instance_id}").into());
    }

    Ok(())
}

fn endpoint_slice_has_ready_endpoint(slice: &EndpointSlice) -> bool {
    slice.endpoints.iter().any(|endpoint| {
        endpoint
            .conditions
            .as_ref()
            .and_then(|conditions| conditions.ready)
            .unwrap_or(true)
    })
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

fn workload_template(kind: WorkloadKind, name: &str, image: &str) -> WorkloadTemplate {
    WorkloadTemplate {
        kind: kind as i32,
        name: Some(literal_text(name)),
        replicas: Some(1),
        app_container: Some(ContainerTemplate {
            name: "app".to_owned(),
            image: Some(literal_text(image)),
            ports: vec![ContainerPortTemplate {
                name: Some("http".to_owned()),
                container_port: APP_PORT,
            }],
            env: Vec::new(),
        }),
    }
}

fn sidecar_template(config: &E2eConfig) -> SidecarTemplate {
    SidecarTemplate {
        name: "sleepypods-sidecar".to_owned(),
        image: Some(literal_text(&config.sidecar_image)),
        listen_port: SIDECAR_PORT,
        mode: None,
    }
}

fn service_template(name: &str) -> ServiceTemplate {
    ServiceTemplate {
        name: Some(literal_text(name)),
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

fn unbound_static_inventory(namespace: &str) -> Vec<RawKubernetesManifestTemplate> {
    // A capacity mismatch is insufficient: Kubernetes finishes matching
    // prebindings without that check. Reserve this task-owned Retain PV to an
    // explicit prior claim incarnation, keeping exact recorded names and equal
    // capacities. The current claim must stay Pending until the wake deadline.
    [
        json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": { "name": UNBOUND_PV_NAME },
            "spec": {
                "capacity": { "storage": "1Mi" },
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem", "storageClassName": "sleepypods-kind-static",
                "persistentVolumeReclaimPolicy": "Retain",
                "claimRef": { "namespace": namespace, "name": UNBOUND_FIRST_PVC_NAME, "uid": UNBOUND_PREVIOUS_CLAIM_UID },
                "hostPath": { "path": "/tmp/sleepypods-kind-e2e-failures/unbound", "type": "DirectoryOrCreate" }
            }
        }),
        json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": { "name": UNBOUND_FIRST_PVC_NAME, "namespace": namespace },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem", "storageClassName": "sleepypods-kind-static",
                "volumeName": UNBOUND_PV_NAME,
                "resources": { "requests": { "storage": "1Mi" } }
            }
        }),
    ].into_iter().map(|value| RawKubernetesManifestTemplate {
        manifest: Some(literal_text(&value.to_string())),
    }).collect()
}

// Run the same raw inventory through the real renderer without requiring a cluster.
// This prevents another pre-accept template rejection from masquerading as a PVC wait test.
#[test]
fn unbound_inventory_passes_real_render_validation() -> TestResult<()> {
    use control_plane as domain;
    let namespace = "sleepypods-e2e-failures";
    let raw_objects = unbound_static_inventory(namespace)
        .into_iter()
        .map(|raw| {
            let text = raw.manifest.expect("fixture raw manifest exists");
            let [TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(text)),
            }] = text.parts.as_slice()
            else {
                panic!("fixture raw manifest is literal");
            };
            domain::manifest::RawKubernetesManifestTemplate {
                manifest: domain::TemplateText::literal(text),
            }
        })
        .collect();
    let template = domain::ManifestTemplate {
        workload: domain::WorkloadTemplate {
            kind: domain::WorkloadKind::StatefulSet,
            name: domain::TemplateText::literal(UNBOUND_WORKLOAD_NAME),
            replicas: Some(1),
            app_container: domain::ContainerTemplate {
                name: "app".into(),
                image: domain::TemplateText::literal("fixture-app"),
                ports: vec![domain::ContainerPortTemplate {
                    name: Some("http".into()),
                    container_port: APP_PORT as u16,
                }],
                env: Vec::new(),
            },
        },
        sidecar: domain::SidecarTemplate {
            name: "sleepypods-sidecar".into(),
            image: domain::TemplateText::literal("fixture-sidecar"),
            listen_port: SIDECAR_PORT as u16,
            mode: None,
        },
        service: Some(domain::ServiceTemplate {
            name: domain::TemplateText::literal(UNBOUND_WORKLOAD_NAME),
            ports: vec![domain::ServicePortTemplate {
                name: Some("http".into()),
                port: APP_PORT as u16,
                target_port: APP_PORT as u16,
            }],
        }),
        volumes: Vec::new(),
        raw_objects,
    };
    let accepted_generation = domain::Generation::new(1);
    let pending = domain::RecordMaterializationRequest::new(
        domain::InstanceId::new(UNBOUND_INSTANCE_ID)?,
        accepted_generation,
        domain::MaterializationTarget::new("kind-e2e-failures", namespace)?,
        domain::MaterializationState::Pending,
        domain::BackendGeneration::new(1),
    );
    assert_eq!(pending.projection_generation.get(), 2);
    let instance = domain::InstanceRecord {
        id: domain::InstanceId::new(UNBOUND_INSTANCE_ID)?,
        workload_class: domain::WorkloadClassVersionRef {
            class_id: domain::WorkloadClassId::new(UNBOUND_CLASS_ID)?,
            version: domain::Generation::new(1),
        },
        values: Default::default(),
        state: domain::InstanceState::Running,
        generation: pending.projection_generation,
    };
    let rendered = domain::render_manifests(domain::RenderManifestRequest {
        template: &template,
        instance: &instance,
        sleep_policy: domain::ResolvedSleepPolicy {
            idle_timeout_ms: 300_000,
            idle_retry_backoff_ms: 500,
            drain_grace_timeout_ms: 500,
        },
        namespace,
        template_generation: Some(domain::Generation::new(1)),
    })?;
    // The projection plan adds the same final managed-by/ownership stamps used
    // by the production reconciler; the pure renderer alone does not add them.
    let materialization = domain::MaterializationRecord {
        id: domain::MaterializationId::new("fixture-unbound")?,
        instance_id: pending.instance_id,
        instance_generation: pending.instance_generation,
        projection_generation: pending.projection_generation,
        target: pending.target,
        state: pending.state,
        backend: pending.backend,
        backend_generation: pending.backend_generation,
        rendered_objects: pending.rendered_objects,
        exclusivity_keys: pending.exclusivity_keys,
        reconciliation_lease: None,
    };
    let plan = domain::projection::ProjectionPlan::from_manifest(&materialization, &rendered)?;
    let rendered = plan
        .manifest()
        .expect("projection includes rendered manifest");
    let objects = rendered
        .objects
        .iter()
        .map(|object| object.object.to_kubernetes_json())
        .collect::<Vec<_>>();
    let pv: PersistentVolume = serde_json::from_value(
        objects
            .iter()
            .find(|object| object["kind"] == "PersistentVolume")
            .expect("rendered PV")
            .clone(),
    )?;
    let mut pvc: PersistentVolumeClaim = serde_json::from_value(
        objects
            .iter()
            .find(|object| object["kind"] == "PersistentVolumeClaim")
            .expect("rendered PVC")
            .clone(),
    )?;
    for (kind, metadata) in [("PV", &pv.metadata), ("PVC", &pvc.metadata)] {
        assert_inventory_ownership_for_accepted_wake(kind, metadata, accepted_generation.get())?;
        for wrong_generation in [0, accepted_generation.get(), 3] {
            let mut wrong = metadata.clone();
            wrong.labels.as_mut().unwrap().insert(
                "sleepypods.io/instance-generation".into(),
                wrong_generation.to_string(),
            );
            assert!(assert_inventory_ownership_for_accepted_wake(
                kind,
                &wrong,
                accepted_generation.get()
            )
            .is_err());
        }
        for (key, value) in [
            (INSTANCE_ID_LABEL, "another-instance"),
            ("app.kubernetes.io/managed-by", "another-controller"),
        ] {
            let mut wrong = metadata.clone();
            wrong
                .labels
                .as_mut()
                .unwrap()
                .insert(key.into(), value.into());
            assert!(assert_inventory_ownership_for_accepted_wake(
                kind,
                &wrong,
                accepted_generation.get()
            )
            .is_err());
        }
    }
    // Kubernetes assigns the current UID after render; exercise the exact live
    // shape assertion with a distinct UID, then prove losing the reservation
    // or accidentally reserving this incarnation is rejected.
    pvc.metadata.uid = Some("00000000-0000-4000-8000-000000000002".into());
    assert_stale_claim_reservation(&pv, &pvc, namespace)?;
    for uid in [None, pvc.metadata.uid.clone()] {
        let mut broken = pv.clone();
        broken
            .spec
            .as_mut()
            .unwrap()
            .claim_ref
            .as_mut()
            .unwrap()
            .uid = uid;
        assert!(assert_stale_claim_reservation(&broken, &pvc, namespace).is_err());
    }
    let mut same_incarnation = pvc.clone();
    same_incarnation.metadata.uid = Some(UNBOUND_PREVIOUS_CLAIM_UID.into());
    assert!(assert_stale_claim_reservation(&pv, &same_incarnation, namespace).is_err());
    let pv_spec = pv.spec.expect("PV spec");
    let pvc_spec = pvc.spec.expect("PVC spec");
    assert_eq!(pv.metadata.name.as_deref(), Some(UNBOUND_PV_NAME));
    assert_eq!(pvc.metadata.name.as_deref(), Some(UNBOUND_FIRST_PVC_NAME));
    assert_eq!(
        pv_spec.persistent_volume_reclaim_policy.as_deref(),
        Some("Retain")
    );
    assert_eq!(
        pv_spec.claim_ref.expect("explicit claim").name.as_deref(),
        Some(UNBOUND_FIRST_PVC_NAME)
    );
    assert_eq!(pvc_spec.volume_name.as_deref(), Some(UNBOUND_PV_NAME));
    assert_eq!(pv_spec.capacity.expect("PV capacity")["storage"].0, "1Mi");
    assert_eq!(
        pvc_spec
            .resources
            .expect("PVC resources")
            .requests
            .expect("PVC request")["storage"]
            .0,
        "1Mi"
    );
    Ok(())
}

fn duplicate_pv_volume_templates() -> Vec<VolumeTemplate> {
    vec![
        VolumeTemplate {
            name: "data-a".to_owned(),
            mount_path: Some(literal_text("/data-a")),
            pv_name: Some(literal_text(UNBOUND_PV_NAME)),
            pvc_name: Some(literal_text(UNBOUND_FIRST_PVC_NAME)),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
            capacity: Some(literal_text("1Mi")),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
            storage_class_name: Some(literal_text("sleepypods-kind-static")),
            source: Some(host_path_source(
                "/tmp/sleepypods-kind-e2e-failures/unbound",
            )),
        },
        VolumeTemplate {
            name: "data-b".to_owned(),
            mount_path: Some(literal_text("/data-b")),
            pv_name: Some(literal_text(UNBOUND_PV_NAME)),
            pvc_name: Some(literal_text(UNBOUND_SECOND_PVC_NAME)),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
            capacity: Some(literal_text("1Mi")),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
            storage_class_name: Some(literal_text("sleepypods-kind-static")),
            source: Some(host_path_source(
                "/tmp/sleepypods-kind-e2e-failures/unbound",
            )),
        },
    ]
}

fn host_path_source(path: &str) -> PersistentVolumeSourceTemplate {
    PersistentVolumeSourceTemplate {
        kind: Some(persistent_volume_source_template::Kind::HostPath(
            HostPathVolumeSourceTemplate {
                path: Some(literal_text(path)),
                r#type: Some(literal_text("DirectoryOrCreate")),
            },
        )),
    }
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}
