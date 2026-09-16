use std::{
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use control_plane::{
    api::pb::{
        operator_control_plane_client::OperatorControlPlaneClient,
        proxy_control_plane_client::ProxyControlPlaneClient, proxy_wake_instance_response,
        route_identity, sidecar_control_plane_client::SidecarControlPlaneClient,
        template_text_part, ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest,
        CreateRouteBindingRequest, CreateWorkloadClassVersionRequest, GetInstanceRequest,
        HttpRouteIdentity, Instance, InstanceState as PbInstanceState, ManifestTemplate,
        ProtocolRoute, ProxyWakeInstanceRequest, RouteHost, RouteHostKind, RouteIdentity,
        ServicePortTemplate, ServiceTemplate, SidecarReportIdleRequest, SidecarTemplate,
        TemplateText, TemplateTextPart, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy,
        WorkloadTemplate, WorkloadValueSchema,
    },
    render_instance_scoped_name, BearerToken, InstanceId, OptionalBearerTokenInterceptor,
};
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{Secret, Service},
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{Api, Client, Error as KubeError};
use tokio::time::{sleep, Instant};
use tonic::{
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
    Code,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLASS_ID: &str = "stateless-web";
const INSTANCE_ID: &str = "e2e-stateless";
const ABANDONED_INSTANCE_ID: &str = "e2e-stateless-abandoned";
const ROUTE_ID: &str = "e2e-stateless-route";
const ROUTE_HOST: &str = "e2e.sleepypods.test";
const WORKLOAD_NAME: &str = "e2e-app";
// Expected suffix: first eight SHA-256 hex digits of the complete INSTANCE_ID.
const RENDERED_WORKLOAD_NAME: &str = "e2e-app-0efa3edd";
const SIDECAR_TOKEN_SECRET_NAME: &str = "sleepypods-sidecar-token-0efa3edd";
const APP_RESPONSE: &str = "sleepypods-stateless-app";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;

#[test]
fn rendered_name_constants_match_instance_scoped_naming() {
    let instance_id = InstanceId::new(INSTANCE_ID).unwrap();
    for (constant, base) in [
        (RENDERED_WORKLOAD_NAME, WORKLOAD_NAME),
        (SIDECAR_TOKEN_SECRET_NAME, "sleepypods-sidecar-token"),
    ] {
        assert_eq!(
            constant,
            render_instance_scoped_name(base, &instance_id),
            "rendered name constant no longer matches the control plane's instance-scoped naming"
        );
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-stateless.sh or an equivalent kind deployment"]
async fn stateless_http_lifecycle_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_STATELESS").as_deref() != Ok("1") {
        eprintln!("skipping stateless kind E2E because SLEEPYPODS_KIND_E2E_STATELESS=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint, &config.operator_token).await?;

    eprintln!("stateless E2E: control-plane authentication and resource setup");
    assert_invalid_control_plane_credentials_fail(&config).await?;
    create_operator_resources(&mut operator, &config).await?;
    let created = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    assert_eq!(created.generation, 0);

    // A second unrouted instance receives one Wake RPC and no application
    // requests. It must still become idle after the finite activation floor.
    let abandoned_started = begin_abandoned_wake(&mut operator, &config).await?;
    let first_request_started = Instant::now();
    // One request owns the complete 130-second routing/readiness wait, with transport margin.
    eprintln!("stateless E2E: single cold request");
    let first = tokio::time::timeout(
        Duration::from_secs(140),
        http_get(config.frontline_addr, ROUTE_HOST, "/"),
    )
    .await??;
    assert_response(&first, "cold wake request")?;
    let running = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    if running.generation != created.generation + 2 {
        return Err(format!(
            "expected one accepted wake to advance generation from {} by exactly two, got {}",
            created.generation, running.generation
        )
        .into());
    }
    eprintln!("stateless E2E: materialized ownership and sidecar layout");
    assert_materialized_deployment_and_service(kube.clone(), &config).await?;

    eprintln!("stateless E2E: successful hot traffic and dedicated Prometheus counters");
    assert_frontline_hot_cache_metrics(&config).await?;

    eprintln!("stateless E2E: autonomous idle sleep and cleanup");
    let cold_after_idle = wait_for_automatic_idle_floor(
        &mut operator,
        first_request_started,
        abandoned_started,
        running.generation,
    )
    .await?;
    if cold_after_idle.generation <= running.generation {
        return Err(format!(
            "expected idle sleep to advance generation beyond {}, got {}",
            running.generation, cold_after_idle.generation
        )
        .into());
    }
    wait_for_materialized_objects_deleted(kube, &config.namespace, Duration::from_secs(90)).await?;

    sleep(Duration::from_secs(11)).await;
    eprintln!("stateless E2E: single re-wake request after sleep");
    let rewake = tokio::time::timeout(
        Duration::from_secs(140),
        http_get(config.frontline_addr, ROUTE_HOST, "/"),
    )
    .await??;
    assert_response(&rewake, "re-wake request")?;
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

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    /// The listener carrying the proxy and sidecar services.
    workload_endpoint: String,
    frontline_addr: SocketAddr,
    frontline_metrics_addr: SocketAddr,
    app_image: String,
    sidecar_image: String,
    operator_token: String,
    proxy_token: String,
    sidecar_token: String,
    invalid_token: String,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct FrontlineMetricCounts {
    subscribe_route_success: f64,
    cache_hits: f64,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-stateless".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19051".to_owned()),
            workload_endpoint: env::var("SLEEPYPODS_E2E_WORKLOAD_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19052".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19080".to_owned())
                .parse()?,
            frontline_metrics_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_METRICS_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19090".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/stateless-app:kind-e2e-stateless".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-stateless".to_owned()),
            operator_token: env::var("SLEEPYPODS_E2E_OPERATOR_TOKEN")
                .unwrap_or_else(|_| "operator-token".to_owned()),
            proxy_token: env::var("SLEEPYPODS_E2E_PROXY_TOKEN")
                .unwrap_or_else(|_| "proxy-token".to_owned()),
            sidecar_token: env::var("SLEEPYPODS_E2E_SIDECAR_TOKEN")
                .unwrap_or_else(|_| "sidecar-token".to_owned()),
            invalid_token: env::var("SLEEPYPODS_E2E_INVALID_TOKEN")
                .unwrap_or_else(|_| "invalid-token".to_owned()),
        })
    }
}

type AuthenticatedChannel = InterceptedService<Channel, OptionalBearerTokenInterceptor>;

async fn connect_operator(
    endpoint: &str,
    token: &str,
) -> TestResult<OperatorControlPlaneClient<AuthenticatedChannel>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let channel = Endpoint::from_shared(endpoint.to_owned())?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        match channel {
            Ok(channel) => {
                return Ok(OperatorControlPlaneClient::with_interceptor(
                    channel,
                    token_interceptor(token)?,
                ));
            }
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for operator gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn token_interceptor(token: &str) -> TestResult<OptionalBearerTokenInterceptor> {
    let token = BearerToken::new("kind_e2e_token", token.to_owned())?;
    Ok(OptionalBearerTokenInterceptor::new(Some(&token))?)
}

async fn assert_invalid_control_plane_credentials_fail(config: &E2eConfig) -> TestResult<()> {
    let channel = Endpoint::from_shared(config.operator_endpoint.clone())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?;
    // The proxy and sidecar services answer on their own listener.
    let workload_channel = Endpoint::from_shared(config.workload_endpoint.clone())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?;

    let mut operator = OperatorControlPlaneClient::with_interceptor(
        channel.clone(),
        token_interceptor(&config.invalid_token)?,
    );
    let operator_error = operator
        .get_instance(GetInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
        })
        .await
        .expect_err("invalid operator credentials must fail");
    assert_eq!(operator_error.code(), Code::Unauthenticated);

    let mut proxy = ProxyControlPlaneClient::with_interceptor(
        workload_channel.clone(),
        token_interceptor(&config.invalid_token)?,
    );
    let proxy_error = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
            expected_generation: 0,
            backend_generation: None,
        })
        .await
        .expect_err("invalid proxy credentials must fail");
    assert_eq!(proxy_error.code(), Code::Unauthenticated);

    let mut sidecar = SidecarControlPlaneClient::with_interceptor(
        workload_channel,
        token_interceptor(&config.invalid_token)?,
    );
    let sidecar_error = sidecar
        .report_idle(SidecarReportIdleRequest {
            // Authentication must reject this request before membership is inspected.
            pod_uid: "unauthenticated-non-member".to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            expected_generation: 0,
            active_count: 0,
        })
        .await
        .expect_err("invalid sidecar credentials must fail");
    assert_eq!(sidecar_error.code(), Code::Unauthenticated);

    Ok(())
}

async fn create_operator_resources(
    operator: &mut OperatorControlPlaneClient<AuthenticatedChannel>,
    config: &E2eConfig,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: "kind-e2e-create-class".to_owned(),
            class_id: CLASS_ID.to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: Default::default(),
                allow_extra: false,
            }),
            template_generation: 1,
            template: Some(manifest_template(config)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 2_000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        })
        .await?;

    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: "kind-e2e-create-instance".to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: Default::default(),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: "kind-e2e-create-route".to_owned(),
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

async fn begin_abandoned_wake(
    operator: &mut OperatorControlPlaneClient<AuthenticatedChannel>,
    config: &E2eConfig,
) -> TestResult<Instant> {
    let created = operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: "kind-e2e-create-abandoned-instance".to_owned(),
            instance_id: ABANDONED_INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: Default::default(),
        })
        .await?
        .into_inner();
    if created.state != PbInstanceState::Cold as i32 || created.generation != 0 {
        return Err(format!("abandoned fixture must start Cold generation0: {created:?}").into());
    }
    let channel = Endpoint::from_shared(config.workload_endpoint.clone())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?;
    let mut proxy =
        ProxyControlPlaneClient::with_interceptor(channel, token_interceptor(&config.proxy_token)?);
    let started = Instant::now();
    let accepted = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: ABANDONED_INSTANCE_ID.to_owned(),
            expected_generation: created.generation,
            backend_generation: None,
        })
        .await?
        .into_inner();
    if !matches!(
        accepted.outcome,
        Some(proxy_wake_instance_response::Outcome::StillWaking(_))
    ) {
        return Err(format!("abandoned wake was not accepted: {accepted:?}").into());
    }
    Ok(started)
}

async fn wait_for_automatic_idle_floor(
    operator: &mut OperatorControlPlaneClient<AuthenticatedChannel>,
    first_request_started: Instant,
    abandoned_started: Instant,
    expected_running_generation: u64,
) -> TestResult<Instance> {
    let floor = sleepypods_api::INITIAL_ACTIVATION_TIMEOUT;
    // These request-start instants precede durable Ready, giving conservative
    // lower bounds independent of host/database clock offsets. The PG gate
    // separately asserts the exact persisted Ready timestamp boundary.
    let deadline = first_request_started + floor + Duration::from_secs(130);
    let mut abandoned_was_running = false;
    let mut main_cold = None;
    let mut abandoned_cold = false;
    loop {
        for (id, started) in [
            (INSTANCE_ID, first_request_started),
            (ABANDONED_INSTANCE_ID, abandoned_started),
        ] {
            let instance = operator
                .get_instance(GetInstanceRequest {
                    instance_id: id.to_owned(),
                })
                .await?
                .into_inner();
            let state = PbInstanceState::try_from(instance.state)?;
            if id == INSTANCE_ID
                && ((state == PbInstanceState::Running
                    && instance.generation != expected_running_generation)
                    || (state == PbInstanceState::Cold
                        && instance.generation != expected_running_generation + 2))
            {
                return Err(format!(
                    "unexpected extra lifecycle transition before idle proof: {instance:?}"
                )
                .into());
            }
            if id == ABANDONED_INSTANCE_ID {
                if (state == PbInstanceState::Running && instance.generation != 2)
                    || (state == PbInstanceState::Cold && instance.generation != 4)
                {
                    return Err(format!(
                        "abandoned wake had an extra lifecycle transition: {instance:?}"
                    )
                    .into());
                }
                if state == PbInstanceState::Running {
                    abandoned_was_running = true;
                }
            }
            if matches!(state, PbInstanceState::Draining | PbInstanceState::Cold)
                && started.elapsed() < floor
            {
                return Err(format!(
                    "{id} slept before the activation floor: state={state:?}, elapsed={:?}",
                    started.elapsed()
                )
                .into());
            }
            if matches!(
                state,
                PbInstanceState::Failed | PbInstanceState::Deleting | PbInstanceState::Deleted
            ) {
                return Err(format!("{id} entered unexpected state {state:?}").into());
            }
            if state == PbInstanceState::Cold {
                if id == INSTANCE_ID {
                    main_cold = Some(instance);
                } else {
                    if !abandoned_was_running {
                        return Err("abandoned wake never became Running".into());
                    }
                    if !abandoned_cold {
                        eprintln!(
                            "stateless E2E: no-traffic wake became Cold after {:?}",
                            started.elapsed()
                        );
                    }
                    abandoned_cold = true;
                }
            }
        }
        if abandoned_cold {
            if let Some(instance) = main_cold {
                return Ok(instance);
            }
        }
        if Instant::now() >= deadline {
            return Err("automatic idle did not finish within activation floor plus readiness/cleanup allowance".into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_instance_state(
    operator: &mut OperatorControlPlaneClient<AuthenticatedChannel>,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let instance = operator
            .get_instance(GetInstanceRequest {
                instance_id: INSTANCE_ID.to_owned(),
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
                "timed out waiting for instance {INSTANCE_ID} to reach {expected:?}; last state was {actual:?} generation {}",
                instance.generation
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn http_get(
    addr: SocketAddr,
    host: &'static str,
    path: &'static str,
) -> TestResult<HttpResponse> {
    tokio::task::spawn_blocking(move || {
        http_get_blocking(addr, host, path, Duration::from_secs(130))
    })
    .await
    .map_err(|error| format!("HTTP request task failed: {error}"))?
}

fn http_get_blocking(
    addr: SocketAddr,
    host: &str,
    path: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let mut stream = TcpStream::connect_timeout(&addr, timeout.min(Duration::from_secs(5)))
        .map_err(|error| format!("HTTP connect to {addr} for {path} failed: {error}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;

    let deadline = Instant::now() + timeout;
    let bytes = read_content_length_response(|buffer| {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "framed response deadline exceeded",
            ));
        }
        stream.set_read_timeout(Some(remaining))?;
        stream.read(buffer)
    })
    .map_err(|error| format!("HTTP framed read from {addr} for {path} failed: {error}"))?;
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

// The dedicated exporter and BusyBox app fixture send known-length bodies.
// Kubernetes port-forward can delay EOF after those bytes arrive, so EOF is
// not their message boundary.
fn read_content_length_response(
    mut read: impl FnMut(&mut [u8]) -> std::io::Result<usize>,
) -> TestResult<Vec<u8>> {
    const MAX_HEADERS: usize = 16 * 1024;
    const MAX_RESPONSE: usize = 1024 * 1024;
    let mut bytes = Vec::new();
    let mut expected = None;
    let mut buffer = [0; 8192];
    loop {
        let count = read(&mut buffer)
            .map_err(|error| format!("read failed after {} bytes: {error}", bytes.len()))?;
        if count == 0 {
            return Err(format!(
                "truncated framed response: {} bytes, expected {expected:?}",
                bytes.len()
            )
            .into());
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > MAX_RESPONSE {
            return Err("framed response exceeds 1 MiB bound".into());
        }
        if expected.is_none() {
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let head_end = end + 4;
                if head_end > MAX_HEADERS {
                    return Err("framed response headers exceed 16 KiB bound".into());
                }
                let head = std::str::from_utf8(&bytes[..end])?;
                let mut length = None;
                for line in head.lines().skip(1) {
                    let (name, value) = line
                        .split_once(':')
                        .ok_or("malformed framed response header")?;
                    if name.eq_ignore_ascii_case("transfer-encoding") {
                        return Err(
                            "known-length fixture response must use Content-Length framing".into(),
                        );
                    }
                    if name.eq_ignore_ascii_case("content-length") {
                        if length.is_some() {
                            return Err("duplicate response Content-Length".into());
                        }
                        length = Some(value.trim().parse::<usize>()?);
                    }
                }
                let length = length.ok_or("known-length fixture response has no Content-Length")?;
                let total = head_end
                    .checked_add(length)
                    .filter(|total| *total <= MAX_RESPONSE)
                    .ok_or("framed response exceeds 1 MiB bound")?;
                expected = Some(total);
            } else if bytes.len() > MAX_HEADERS {
                return Err("framed response headers exceed 16 KiB bound".into());
            }
        }
        if let Some(expected) = expected {
            if bytes.len() > expected {
                return Err("unexpected bytes after framed response body".into());
            }
            if bytes.len() == expected {
                return Ok(bytes);
            }
        }
    }
}

fn assert_response(response: &HttpResponse, context: &str) -> TestResult<()> {
    if response.status != 200 {
        return Err(format!("{context} returned HTTP {}", response.status).into());
    }
    if !response.body.contains(APP_RESPONSE) {
        return Err(format!(
            "{context} body did not include {APP_RESPONSE:?}: {:?}",
            response.body
        )
        .into());
    }

    Ok(())
}

/// The hardening the platform applies to every pod it renders, read back from
/// the object the API server accepted.
fn assert_pod_hardening(pod_spec: &k8s_openapi::api::core::v1::PodSpec) -> TestResult<()> {
    if pod_spec.automount_service_account_token != Some(false) {
        return Err(format!(
            "materialized pod must decline its ServiceAccount token, got {:?}",
            pod_spec.automount_service_account_token
        )
        .into());
    }
    let seccomp = pod_spec
        .security_context
        .as_ref()
        .and_then(|context| context.seccomp_profile.as_ref())
        .ok_or("materialized pod is missing its seccomp profile")?;
    if seccomp.type_ != "RuntimeDefault" {
        return Err(format!(
            "materialized pod must run the runtime's default seccomp profile, got {:?}",
            seccomp.type_
        )
        .into());
    }
    for container in &pod_spec.containers {
        let escalation = container
            .security_context
            .as_ref()
            .and_then(|context| context.allow_privilege_escalation);
        if escalation != Some(false) {
            return Err(format!(
                "container {:?} must gain no privileges, got {escalation:?}",
                container.name
            )
            .into());
        }
    }
    Ok(())
}

async fn assert_materialized_deployment_and_service(
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), &config.namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), &config.namespace);
    let secrets: Api<Secret> = Api::namespaced(kube, &config.namespace);
    let deployment = deployments.get(RENDERED_WORKLOAD_NAME).await?;
    let pod_spec = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .ok_or("materialized Deployment is missing pod spec")?;
    assert_pod_hardening(pod_spec)?;
    let app = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "app")
        .ok_or("materialized Deployment is missing app container")?;
    if app.image.as_deref() != Some(config.app_image.as_str()) {
        return Err(format!(
            "expected app image {}, got {:?}",
            config.app_image, app.image
        )
        .into());
    }
    let sidecar = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "sleepypods-sidecar")
        .ok_or("materialized Deployment is missing sidecar container")?;
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
    let token_env = sidecar
        .env
        .as_ref()
        .and_then(|vars| {
            vars.iter()
                .find(|var| var.name == "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN")
        })
        .ok_or("sidecar is missing its token Secret reference")?;
    if token_env
        .value
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        return Err("sidecar token must not appear as a plaintext pod environment value".into());
    }
    let secret_ref = token_env
        .value_from
        .as_ref()
        .and_then(|source| source.secret_key_ref.as_ref())
        .ok_or("sidecar token must use valueFrom.secretKeyRef")?;
    if secret_ref.name != SIDECAR_TOKEN_SECRET_NAME
        || secret_ref.key != "token"
        || secret_ref.optional == Some(true)
    {
        return Err(
            "sidecar token must reference the exact required instance token Secret/key".into(),
        );
    }
    let secret = secrets.get(SIDECAR_TOKEN_SECRET_NAME).await?;
    if secret.type_.as_deref() != Some("Opaque")
        || secret.metadata.namespace.as_deref() != Some(config.namespace.as_str())
    {
        return Err("sidecar token Secret has an incorrect type or namespace".into());
    }
    let data = secret
        .data
        .as_ref()
        .ok_or("sidecar token Secret is missing data")?;
    if data.len() != 1
        || data.get("token").map(|value| value.0.as_slice())
            != Some(config.sidecar_token.as_bytes())
    {
        // Do not include either token value in failure output.
        return Err(
            "sidecar token Secret contents do not match the configured fixture token".into(),
        );
    }
    let secret_labels = secret
        .metadata
        .labels
        .as_ref()
        .ok_or("token Secret is missing ownership labels")?;
    let deployment_labels = deployment
        .metadata
        .labels
        .as_ref()
        .ok_or("Deployment is missing ownership labels")?;
    for key in [
        "app.kubernetes.io/managed-by",
        "sleepypods.io/instance-id",
        "sleepypods.io/instance-generation",
    ] {
        if secret_labels.get(key).is_none() || secret_labels.get(key) != deployment_labels.get(key)
        {
            return Err(format!(
                "token Secret ownership label {key} does not match the Deployment"
            )
            .into());
        }
    }
    if secret_labels
        .get("app.kubernetes.io/managed-by")
        .map(String::as_str)
        != Some("sleepypods")
        || secret_labels
            .get("sleepypods.io/instance-id")
            .map(String::as_str)
            != Some(INSTANCE_ID)
    {
        return Err("token Secret is not owned by the expected SleepyPods instance".into());
    }

    let materialization_annotation = "sleepypods.io/materialization-id";
    let secret_materialization = secret
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(materialization_annotation));
    let deployment_materialization = deployment
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(materialization_annotation));
    if secret_materialization.is_none() || secret_materialization != deployment_materialization {
        return Err("token Secret materialization owner does not match the Deployment".into());
    }

    let service = services.get(RENDERED_WORKLOAD_NAME).await?;
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
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        let deployment_absent = is_not_found(deployments.get(RENDERED_WORKLOAD_NAME).await);
        let service_absent = is_not_found(services.get(RENDERED_WORKLOAD_NAME).await);
        if deployment_absent && service_absent {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for materialized Deployment/Service {namespace}/{RENDERED_WORKLOAD_NAME} to be deleted"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn assert_frontline_hot_cache_metrics(config: &E2eConfig) -> TestResult<()> {
    let started = Instant::now();
    let mut last_counts = None;
    // A delayed wake notification can invalidate the first post-cold lookup.
    // Keep traffic active while that finite change propagates, then prove a new
    // hit within five seconds, below the ten-second positive cache TTL. Each
    // request must succeed within one second; request failures are never retried.
    tokio::time::timeout(Duration::from_secs(5), async {
        let initial = frontline_metric_counts(config.frontline_metrics_addr).await?;
        for request_number in 1.. {
            let address = config.frontline_addr;
            let response = tokio::time::timeout(
                Duration::from_secs(1),
                tokio::task::spawn_blocking(move || {
                    http_get_blocking(address, ROUTE_HOST, "/", Duration::from_millis(900))
                }),
            )
            .await
            .map_err(|_| format!("hot request {request_number} exceeded one second"))???;
            assert_response(&response, &format!("hot request {request_number}"))?;
            let counts = frontline_metric_counts(config.frontline_metrics_addr).await?;
            eprintln!(
                "stateless E2E: hot request {request_number} succeeded; counters {counts:?} after {:?}",
                started.elapsed()
            );
            let has_new_hit = counts.cache_hits > initial.cache_hits;
            let has_subscription = counts.subscribe_route_success >= 1.0;
            last_counts = Some(counts);
            if has_new_hit && has_subscription {
                return Ok(());
            }
            sleep(Duration::from_millis(100)).await;
        }
        unreachable!()
    })
    .await
    .map_err(|_| {
        format!("successful hot traffic produced no new cache hit within five seconds; last counters {last_counts:?}")
    })?
}

async fn frontline_metric_counts(metrics_addr: SocketAddr) -> TestResult<FrontlineMetricCounts> {
    // Scrape the dedicated exporter, never the public proxy listener: this
    // observation must not itself create the route-cache hit being asserted.
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || {
            http_get_blocking(
                metrics_addr,
                "localhost",
                "/metrics",
                Duration::from_secs(1),
            )
        }),
    )
    .await
    .map_err(|_| "dedicated metrics scrape exceeded 2s")???;
    if response.status != 200 {
        return Err(format!("dedicated metrics scrape returned HTTP {}", response.status).into());
    }
    Ok(FrontlineMetricCounts {
        subscribe_route_success: prometheus_counter(
            &response.body,
            "sleepypods_runtime_control_plane_calls_total",
            &[("operation", "subscribe_route"), ("outcome", "success")],
        )?,
        cache_hits: prometheus_counter(
            &response.body,
            "sleepypods_runtime_route_cache_lookups_total",
            &[("outcome", "hit")],
        )?,
    })
}

fn prometheus_counter(body: &str, name: &str, labels: &[(&str, &str)]) -> TestResult<f64> {
    let mut total = 0.0;
    for line in body.lines().filter(|line| !line.starts_with('#')) {
        let mut parts = line.split_whitespace();
        let Some(series) = parts.next() else {
            continue;
        };
        let Some((actual_name, actual_labels)) = series.split_once('{') else {
            continue;
        };
        let Some(actual_labels) = actual_labels.strip_suffix('}') else {
            continue;
        };
        if actual_name != name {
            continue;
        }
        let actual_labels = actual_labels.split(',').collect::<Vec<_>>();
        if actual_labels.len() != labels.len()
            || !labels.iter().all(|(key, value)| {
                actual_labels
                    .iter()
                    .any(|actual| *actual == format!("{key}=\"{value}\""))
            })
        {
            continue;
        }
        let value: f64 = parts
            .next()
            .ok_or("matching metric has no counter value")?
            .parse()?;
        if !value.is_finite() || value < 0.0 {
            return Err("matching metric counter is invalid".into());
        }
        total += value;
    }
    Ok(total)
}

#[test]
fn prometheus_metrics_match_exact_counter_labels_and_numeric_values() {
    use sleepypods_observability::{
        metrics::{RUNTIME_CONTROL_PLANE_CALLS_TOTAL, RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL},
        prometheus::PrometheusMetricsSink,
        recorder::MetricObservation,
        Operation, Outcome,
    };
    let exporter = PrometheusMetricsSink::new();
    for observation in [
        MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![
                Outcome::Success.metric_label(),
                Operation::SubscribeRoute.metric_label(),
            ],
            3.0,
        ),
        MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![
                Operation::WakeInstance.metric_label(),
                Outcome::Success.metric_label(),
            ],
            100.0,
        ),
        MetricObservation::new(
            RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL,
            vec![Outcome::Hit.metric_label()],
            2.0,
        ),
        MetricObservation::new(
            RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL,
            vec![Outcome::Miss.metric_label()],
            40.0,
        ),
    ] {
        assert!(exporter.record_observation(observation));
    }
    let rendered = exporter.render();
    let body = rendered.as_str();
    assert_eq!(
        prometheus_counter(
            body,
            "sleepypods_runtime_control_plane_calls_total",
            &[("operation", "subscribe_route"), ("outcome", "success")]
        )
        .unwrap(),
        3.0
    );
    assert_eq!(
        prometheus_counter(
            body,
            "sleepypods_runtime_route_cache_lookups_total",
            &[("outcome", "hit")]
        )
        .unwrap(),
        2.0
    );
    assert_eq!(
        prometheus_counter(
            body,
            "sleepypods_runtime_route_cache_lookups_total_extra",
            &[("outcome", "hit")]
        )
        .unwrap(),
        0.0
    );
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

fn manifest_template(config: &E2eConfig) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
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
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(&config.sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
        }),
        service: Some(ServiceTemplate {
            name: Some(literal_text(WORKLOAD_NAME)),
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

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}

#[test]
fn metrics_complete_content_length_response_does_not_wait_for_port_forward_eof() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody";
    let mut cursor = std::io::Cursor::new(response);
    let bytes = read_content_length_response(|buffer| {
        let count = cursor.read(buffer)?;
        if count == 0 {
            panic!("complete framed response must return before delayed port-forward EOF");
        }
        Ok(count)
    })
    .unwrap();
    assert_eq!(bytes, response);
}

#[test]
fn metrics_scrape_finishes_while_complete_response_socket_stays_open() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (release, hold) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = [0; 1024];
        assert!(socket.read(&mut request).unwrap() > 0);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody")
            .unwrap();
        // The client must finish before this test releases the server socket.
        // A timeout still bounds teardown if the reader regresses to waiting for EOF.
        let _ = hold.recv_timeout(Duration::from_secs(2));
    });
    let response = http_get_blocking(address, "localhost", "/metrics", Duration::from_secs(1));
    let _ = release.send(());
    server.join().unwrap();
    let response = response.unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, "body");
}

#[test]
fn metrics_truncated_or_oversized_content_length_response_is_rejected() {
    for response in [
        "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort",
        "HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Length: 4\r\n\r\nbody",
    ] {
        let mut cursor = std::io::Cursor::new(response.as_bytes());
        assert!(read_content_length_response(|buffer| cursor.read(buffer)).is_err());
    }
}

#[tokio::test]
async fn one_shot_application_error_is_fatal_without_waiting_for_eof() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (release, hold) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut buffer = [0; 1024];
            let count = socket.read(&mut buffer).unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() <= 1024);
        }
        assert!(request.starts_with(b"GET / HTTP/1.1\r\n"));
        socket
            .write_all(
                b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        let _ = hold.recv_timeout(Duration::from_secs(2));
        listener.set_nonblocking(true).unwrap();
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    });
    let response =
        tokio::time::timeout(Duration::from_secs(1), http_get(address, ROUTE_HOST, "/")).await;
    let _ = release.send(());
    server.join().unwrap();
    let response = response.unwrap().unwrap();
    assert_eq!(response.status, 502);
    assert!(assert_response(&response, "re-wake request")
        .unwrap_err()
        .to_string()
        .contains("HTTP 502"));
}
