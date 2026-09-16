use control_plane::api::pb::proxy_control_plane_client::ProxyControlPlaneClient;
use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, route_identity, template_text_part,
    ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest, CreateRouteBindingRequest,
    CreateWorkloadClassVersionRequest, DeleteHttp01ChallengeRequest, EnvVarTemplate,
    ExpireHttp01ChallengesRequest, GetInstanceRequest, Http01ChallengeKey, HttpRouteIdentity,
    Instance, InstanceState as PbInstanceState, ManifestTemplate, ProtocolRoute,
    PutHttp01ChallengeRequest, ResolveHttp01ChallengeRequest, RouteHost, RouteHostKind,
    RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
    TemplateTextPart, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate,
    WorkloadValueFieldRule, WorkloadValueSchema,
};
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{Container, Service},
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{Api, Client};
use tokio::time::{sleep, Instant};
use tonic::transport::{Channel, Endpoint};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLASS_ID: &str = "routing-web";
const EXACT_HOST: &str = "exact.sleepypods.test";
const WILDCARD_SUFFIX: &str = "wild.sleepypods.test";
const WILDCARD_CHILD_HOST: &str = "child.wild.sleepypods.test";
const PATH_HOST: &str = "paths.sleepypods.test";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const TARGETS: &[&str] = &["exact", "wildcard", "path-api", "path-root"];
const HTTP01_TOKEN: &str = "routing-token";
const HTTP01_KEY_AUTHORIZATION: &str = "routing-token.key-authorization";
const HTTP01_EXPIRING_TOKEN: &str = "routing-expiring-token";
const HTTP01_EXPIRING_KEY_AUTHORIZATION: &str = "routing-expiring-token.key-authorization";

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-routing.sh or an equivalent kind deployment"]
async fn routing_and_http01_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_ROUTING").as_deref() != Ok("1") {
        eprintln!("skipping routing kind E2E because SLEEPYPODS_KIND_E2E_ROUTING=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_operator_resources(&mut operator, &config).await?;
    for spec in route_specs() {
        wait_for_instance_state(
            &mut operator,
            spec.instance_id,
            PbInstanceState::Cold,
            Duration::from_secs(30),
        )
        .await?;
    }

    wait_for_instance_response(
        &config,
        "custom exact host",
        EXACT_HOST,
        "/",
        "exact",
        Duration::from_secs(180),
    )
    .await?;
    wait_for_instance_response(
        &config,
        "wildcard child host",
        WILDCARD_CHILD_HOST,
        "/",
        "wildcard",
        Duration::from_secs(180),
    )
    .await?;
    wait_for_status_without_app(
        &config,
        "wildcard base host miss",
        WILDCARD_SUFFIX,
        "/",
        404,
        Duration::from_secs(30),
    )
    .await?;
    wait_for_status_without_app(
        &config,
        "unrelated host miss",
        "unrelated.sleepypods.test",
        "/",
        404,
        Duration::from_secs(30),
    )
    .await?;

    wait_for_instance_response(
        &config,
        "path prefix exact boundary",
        PATH_HOST,
        "/api",
        "path-api",
        Duration::from_secs(180),
    )
    .await?;
    wait_for_instance_response(
        &config,
        "path prefix child boundary",
        PATH_HOST,
        "/api/child",
        "path-api",
        Duration::from_secs(30),
    )
    .await?;
    wait_for_instance_response(
        &config,
        "path prefix near miss falls back to root",
        PATH_HOST,
        "/apix",
        "path-root",
        Duration::from_secs(180),
    )
    .await?;

    for target in TARGETS {
        assert_materialized_deployment_and_service(kube.clone(), &config, target).await?;
    }

    assert_http01_flow(&mut operator, &config).await?;

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct RouteSpec {
    target: &'static str,
    instance_id: &'static str,
    route_id: &'static str,
    host_kind: i32,
    host: &'static str,
    path_prefix: Option<&'static str>,
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
    headers: HashMap<String, String>,
    body: String,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-routing".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19251".to_owned()),
            workload_endpoint: env::var("SLEEPYPODS_E2E_WORKLOAD_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19252".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19280".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/routing-app:kind-e2e-routing".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-routing".to_owned()),
        })
    }
}

fn route_specs() -> [RouteSpec; 4] {
    [
        RouteSpec {
            target: "exact",
            instance_id: "e2e-routing-exact",
            route_id: "e2e-routing-exact-route",
            host_kind: RouteHostKind::Exact as i32,
            host: EXACT_HOST,
            path_prefix: None,
        },
        RouteSpec {
            target: "wildcard",
            instance_id: "e2e-routing-wildcard",
            route_id: "e2e-routing-wildcard-route",
            host_kind: RouteHostKind::WildcardSuffix as i32,
            host: WILDCARD_SUFFIX,
            path_prefix: None,
        },
        RouteSpec {
            target: "path-api",
            instance_id: "e2e-routing-path-api",
            route_id: "e2e-routing-path-api-route",
            host_kind: RouteHostKind::Exact as i32,
            host: PATH_HOST,
            path_prefix: Some("/api"),
        },
        RouteSpec {
            target: "path-root",
            instance_id: "e2e-routing-path-root",
            route_id: "e2e-routing-path-root-route",
            host_kind: RouteHostKind::Exact as i32,
            host: PATH_HOST,
            path_prefix: None,
        },
    ]
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

async fn create_operator_resources(
    operator: &mut OperatorControlPlaneClient<Channel>,
    config: &E2eConfig,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: "kind-e2e-routing-create-class".to_owned(),
            class_id: CLASS_ID.to_owned(),
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
            template: Some(manifest_template(config)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 900_000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        })
        .await?;

    for spec in route_specs() {
        operator
            .create_instance(CreateInstanceRequest {
                idempotency_key: format!("kind-e2e-routing-create-instance-{}", spec.target),
                instance_id: spec.instance_id.to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: CLASS_ID.to_owned(),
                    version: 1,
                }),
                values: HashMap::from([("target".to_owned(), spec.target.to_owned())]),
            })
            .await?;

        operator
            .create_route_binding(CreateRouteBindingRequest {
                idempotency_key: format!("kind-e2e-routing-create-route-{}", spec.target),
                route_binding_id: spec.route_id.to_owned(),
                instance_id: spec.instance_id.to_owned(),
                identity: Some(http_route_identity(
                    spec.host_kind,
                    spec.host,
                    spec.path_prefix,
                )),
                protocol: ProtocolRoute::Http as i32,
            })
            .await?;
    }

    Ok(())
}

async fn wait_for_instance_state(
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

async fn wait_for_instance_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
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
                if response.body.contains("sleepypods-routing-app") {
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
                "timed out waiting for HTTP {expected_status} for {context} host {host} path {path}: {}",
                last_error
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn assert_http01_flow(
    operator: &mut OperatorControlPlaneClient<Channel>,
    config: &E2eConfig,
) -> TestResult<()> {
    let mut proxy = ProxyControlPlaneClient::connect(config.workload_endpoint.clone()).await?;
    wait_for_instance_response(
        config,
        "normal route before HTTP-01 challenge",
        EXACT_HOST,
        "/",
        "exact",
        Duration::from_secs(30),
    )
    .await?;

    put_http01_challenge(
        operator,
        EXACT_HOST,
        HTTP01_TOKEN,
        HTTP01_KEY_AUTHORIZATION,
        SystemTime::now() + Duration::from_secs(60),
    )
    .await?;
    let resolved = proxy
        .resolve_http01_challenge(ResolveHttp01ChallengeRequest {
            key: Some(http01_key(EXACT_HOST, HTTP01_TOKEN)),
        })
        .await?
        .into_inner()
        .challenge
        .ok_or("inserted HTTP-01 challenge did not resolve through proxy API")?;
    if resolved.key_authorization != HTTP01_KEY_AUTHORIZATION {
        return Err(format!(
            "operator resolved key authorization {:?}, expected {:?}",
            resolved.key_authorization, HTTP01_KEY_AUTHORIZATION
        )
        .into());
    }

    let inserted_challenge_path = challenge_path(HTTP01_TOKEN);
    let challenge = wait_for_http01_response(
        config,
        "HTTP-01 inserted challenge",
        &inserted_challenge_path,
        HTTP01_KEY_AUTHORIZATION,
        Duration::from_secs(30),
    )
    .await?;
    assert_http01_content_type(&challenge, "inserted challenge")?;

    wait_for_instance_response(
        config,
        "normal route while HTTP-01 challenge exists",
        EXACT_HOST,
        "/",
        "exact",
        Duration::from_secs(30),
    )
    .await?;

    let deleted = operator
        .delete_http01_challenge(DeleteHttp01ChallengeRequest {
            key: Some(http01_key(EXACT_HOST, HTTP01_TOKEN)),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("expected HTTP-01 delete to remove the inserted challenge".into());
    }
    let after_delete = wait_for_status_without_app(
        config,
        "HTTP-01 deleted challenge",
        EXACT_HOST,
        &inserted_challenge_path,
        404,
        Duration::from_secs(30),
    )
    .await?;
    if after_delete.body.contains(HTTP01_KEY_AUTHORIZATION) {
        return Err("deleted HTTP-01 challenge key authorization was still served".into());
    }

    put_http01_challenge(
        operator,
        EXACT_HOST,
        HTTP01_EXPIRING_TOKEN,
        HTTP01_EXPIRING_KEY_AUTHORIZATION,
        SystemTime::now() + Duration::from_secs(5),
    )
    .await?;
    let expiring_path = challenge_path(HTTP01_EXPIRING_TOKEN);
    wait_for_http01_response(
        config,
        "HTTP-01 expiring challenge before expiry",
        &expiring_path,
        HTTP01_EXPIRING_KEY_AUTHORIZATION,
        Duration::from_secs(30),
    )
    .await?;

    sleep(Duration::from_secs(6)).await;
    let after_expiry = wait_for_status_without_app(
        config,
        "HTTP-01 expiring challenge after expiry",
        EXACT_HOST,
        &expiring_path,
        404,
        Duration::from_secs(30),
    )
    .await?;
    if after_expiry
        .body
        .contains(HTTP01_EXPIRING_KEY_AUTHORIZATION)
    {
        return Err("expired HTTP-01 challenge key authorization was still served".into());
    }
    // The background wall-clock sweep may already have physically removed
    // this naturally expired row. Either delete result is valid after the
    // unchanged frontend 404/no-key-authorization proof above.
    operator
        .delete_http01_challenge(DeleteHttp01ChallengeRequest {
            key: Some(http01_key(EXACT_HOST, HTTP01_EXPIRING_TOKEN)),
        })
        .await?;
    tokio::time::timeout(
        Duration::from_secs(30),
        assert_manual_http01_expiry(operator, &mut proxy),
    )
    .await??;

    wait_for_instance_response(
        config,
        "normal route after HTTP-01 cleanup",
        EXACT_HOST,
        "/",
        "exact",
        Duration::from_secs(30),
    )
    .await?;

    Ok(())
}

async fn assert_manual_http01_expiry(
    operator: &mut OperatorControlPlaneClient<Channel>,
    proxy: &mut ProxyControlPlaneClient<Channel>,
) -> TestResult<()> {
    let target = "routing-manual-expiry-token";
    let sentinel = "routing-later-expiry-token";
    let now = SystemTime::now();
    // This whole phase is bounded to 30s. Background wall-clock GC cannot
    // preempt these future records; only the explicit RPC cutoff reaches them.
    let target_expiry = now + Duration::from_secs(3_600);
    for (token, expiry) in [
        (target, target_expiry),
        (sentinel, now + Duration::from_secs(7_200)),
    ] {
        put_http01_challenge(operator, EXACT_HOST, token, token, expiry).await?;
        assert_proxy_http01_value(proxy, token, Some(token)).await?;
    }
    let request = ExpireHttp01ChallengesRequest {
        now_unix_millis: unix_millis(target_expiry)?,
        limit: Some(1),
    };
    let expired = operator
        .expire_http01_challenges(request)
        .await?
        .into_inner();
    if expired.expired != 1 {
        return Err(format!(
            "manual expiry must delete exactly its future target, got {}",
            expired.expired
        )
        .into());
    }
    assert_proxy_http01_value(proxy, target, None).await?;
    assert_proxy_http01_value(proxy, sentinel, Some(sentinel)).await?;
    let repeated = operator
        .expire_http01_challenges(request)
        .await?
        .into_inner();
    if repeated.expired != 0 {
        return Err("repeated manual expiry must be idempotent at the same cutoff".into());
    }
    let deleted = operator
        .delete_http01_challenge(DeleteHttp01ChallengeRequest {
            key: Some(http01_key(EXACT_HOST, sentinel)),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("later sentinel must survive manual expiry until explicit cleanup".into());
    }
    assert_proxy_http01_value(proxy, sentinel, None).await
}

async fn assert_proxy_http01_value(
    proxy: &mut ProxyControlPlaneClient<Channel>,
    token: &str,
    expected: Option<&str>,
) -> TestResult<()> {
    let challenge = proxy
        .resolve_http01_challenge(ResolveHttp01ChallengeRequest {
            key: Some(http01_key(EXACT_HOST, token)),
        })
        .await?
        .into_inner()
        .challenge;
    if challenge
        .as_ref()
        .map(|record| record.key_authorization.as_str())
        != expected
    {
        return Err(format!(
            "unexpected operator HTTP-01 value for exact token {token}: {challenge:?}"
        )
        .into());
    }
    Ok(())
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
    path: &str,
    expected_body: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, EXACT_HOST, path).await {
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
            return Err(
                format!("timed out waiting for {context} at {path}: {}", last_error).into(),
            );
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
    let headers = head
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect();

    Ok(HttpResponse {
        status,
        headers,
        body: body.to_owned(),
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
    if !response.body.contains("sleepypods-routing-app\n") {
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
    for other in TARGETS.iter().copied().filter(|other| *other != target) {
        if response_body_identifies(response, other) {
            return Err(format!(
                "{context} body identified wrong target {other:?}: {:?}",
                response.body
            )
            .into());
        }
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

async fn assert_materialized_deployment_and_service(
    kube: Client,
    config: &E2eConfig,
    target: &str,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), &config.namespace);
    let services: Api<Service> = Api::namespaced(kube, &config.namespace);
    let workload_name = workload_name(target);
    let deployment = deployments.get(&workload_name).await?;
    let pod_spec = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .ok_or_else(|| format!("materialized Deployment {workload_name} is missing pod spec"))?;
    let app = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "app")
        .ok_or_else(|| {
            format!("materialized Deployment {workload_name} is missing app container")
        })?;
    if app.image.as_deref() != Some(config.app_image.as_str()) {
        return Err(format!(
            "expected app image {}, got {:?}",
            config.app_image, app.image
        )
        .into());
    }
    assert_env(app, "SLEEPYPODS_E2E_INSTANCE", target)?;

    let sidecar = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "sleepypods-sidecar")
        .ok_or_else(|| {
            format!("materialized Deployment {workload_name} is missing sidecar container")
        })?;
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

    let service = services.get(&workload_name).await?;
    let target_port = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .and_then(|ports| ports.first())
        .and_then(|port| port.target_port.as_ref())
        .ok_or_else(|| format!("materialized Service {workload_name} is missing targetPort"))?;
    if target_port != &IntOrString::Int(SIDECAR_PORT as i32) {
        return Err(
            format!("expected Service targetPort {SIDECAR_PORT}, got {target_port:?}").into(),
        );
    }

    Ok(())
}

fn assert_env(container: &Container, name: &str, expected: &str) -> TestResult<()> {
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

fn manifest_template(config: &E2eConfig) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(target_text("e2e-routing-", "")),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "app".to_owned(),
                image: Some(literal_text(&config.app_image)),
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
            image: Some(literal_text(&config.sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
        }),
        service: Some(ServiceTemplate {
            name: Some(target_text("e2e-routing-", "")),
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

fn http_route_identity(host_kind: i32, host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
            host: Some(RouteHost {
                kind: host_kind,
                host: host.to_owned(),
            }),
            path_prefix: path_prefix.map(str::to_owned),
        })),
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

fn workload_name(target: &str) -> String {
    // Independently calculated SHA-256 suffixes for each complete fixture instance ID.
    let suffix = match target {
        "exact" => "b735c72f",
        "wildcard" => "989d17dd",
        "path-api" => "23d08b63",
        "path-root" => "8556a630",
        _ => panic!("unknown fixture target {target}"),
    };
    format!("e2e-routing-{target}-{suffix}")
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
