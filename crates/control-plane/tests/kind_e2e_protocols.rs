use std::{collections::HashMap, env, error::Error, net::SocketAddr, time::Duration};

use bytes::Bytes;
use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, route_identity, template_text_part,
    AppProtocol, ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest,
    CreateRouteBindingRequest, CreateWorkloadClassVersionRequest, EnvVarTemplate,
    GetInstanceRequest, HttpRouteIdentity, Instance, InstanceState as PbInstanceState,
    ManifestTemplate, RouteHost, RouteHostKind, RouteIdentity, ServicePortTemplate,
    ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart, WorkloadClassVersionRef,
    WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate, WorkloadValueFieldRule,
    WorkloadValueSchema,
};
use futures_util::{SinkExt, StreamExt};
use http::{HeaderMap, Method, Request, StatusCode, Version};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http2 as client_http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{Container, Service},
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{Api, Client};
use tokio::{net::TcpStream, time::sleep};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, protocol::CloseFrame, Message},
};
use tonic::transport::{Channel, Endpoint};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// An app port that declares no protocol: the sidecar speaks HTTP/1.1 to it.
const CLASS_ID: &str = "protocol-web";
/// An app port declared `h2c`: the sidecar speaks prior-knowledge HTTP/2 to it.
const H2C_CLASS_ID: &str = "protocol-h2c";
/// An `h2c` app port served by grpcio, which runs gRPC C-core.
const GRPCIO_CLASS_ID: &str = "protocol-grpcio";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const HTTP2_TARGET: &str = "http2";
const GRPC_TARGET: &str = "grpc";
const WEBSOCKET_TARGET: &str = "websocket";
const GRPCIO_TARGET: &str = "grpcio";
const TARGETS: &[&str] = &[HTTP2_TARGET, GRPC_TARGET, WEBSOCKET_TARGET, GRPCIO_TARGET];
const HTTP2_HOST: &str = "h2.protocol.sleepypods.test";
const GRPC_HOST: &str = "grpc.protocol.sleepypods.test";
const WEBSOCKET_HOST: &str = "ws.protocol.sleepypods.test";
const GRPCIO_HOST: &str = "grpcio.protocol.sleepypods.test";
const GRPC_REQUEST_BODY: &[u8] = b"\0\0\0\0\x05world";

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-protocols.sh or an equivalent kind deployment"]
async fn http2_grpc_and_websocket_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_PROTOCOLS").as_deref() != Ok("1") {
        eprintln!("skipping protocols kind E2E because SLEEPYPODS_KIND_E2E_PROTOCOLS=1 is not set");
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

    http2_response(
        &config,
        "cold HTTP/2 route",
        HTTP2_HOST,
        "/h2?round=cold",
        HTTP2_TARGET,
        "HTTP/1.1",
        Duration::from_secs(140),
    )
    .await?;
    wait_for_instance_state(
        &mut operator,
        "e2e-protocol-http2",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    http2_response(
        &config,
        "hot HTTP/2 route",
        HTTP2_HOST,
        "/h2?round=hot",
        HTTP2_TARGET,
        "HTTP/1.1",
        Duration::from_secs(30),
    )
    .await?;
    http1_response(
        &config,
        "HTTP/1.1 client to an http1 app port",
        HTTP2_HOST,
        "/h2?client=http1",
        HTTP2_TARGET,
        "HTTP/1.1",
    )
    .await?;

    grpc_response(
        &config,
        "cold h2c gRPC-shaped route",
        GRPC_HOST,
        "/grpc.Test/Echo",
        GRPC_TARGET,
        Duration::from_secs(140),
    )
    .await?;
    wait_for_instance_state(
        &mut operator,
        "e2e-protocol-grpc",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    grpc_response(
        &config,
        "hot h2c gRPC-shaped route",
        GRPC_HOST,
        "/grpc.Test/Echo",
        GRPC_TARGET,
        Duration::from_secs(30),
    )
    .await?;
    // Every client version reaches the h2c app port as HTTP/2.
    http2_response(
        &config,
        "HTTP/2 client to an h2c app port",
        GRPC_HOST,
        "/plain?client=http2",
        GRPC_TARGET,
        "HTTP/2",
        Duration::from_secs(30),
    )
    .await?;
    http1_response(
        &config,
        "HTTP/1.1 client to an h2c app port",
        GRPC_HOST,
        "/plain?client=http1",
        GRPC_TARGET,
        "HTTP/2",
    )
    .await?;
    // A WebSocket upgrade reaches the same port over HTTP/1.1.
    tokio::time::timeout(
        Duration::from_secs(30),
        websocket_exchange(
            config.frontline_addr,
            GRPC_HOST,
            "/socket?room=h2c",
            GRPC_TARGET,
        ),
    )
    .await??;

    // The first upgrade must carry the Cold route through readiness on this connection.
    tokio::time::timeout(
        Duration::from_secs(140),
        websocket_exchange(
            config.frontline_addr,
            WEBSOCKET_HOST,
            "/socket?room=blue",
            WEBSOCKET_TARGET,
        ),
    )
    .await??;
    wait_for_instance_state(
        &mut operator,
        "e2e-protocol-websocket",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;

    grpcio_calls(&config, &mut operator).await?;

    for spec in route_specs() {
        let app_protocol = if spec.class_id == CLASS_ID {
            "http1"
        } else {
            "h2c"
        };
        assert_materialized_deployment_and_service(
            kube.clone(),
            &config,
            spec.target,
            app_image(&config, spec.class_id),
            app_protocol,
        )
        .await?;
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct RouteSpec {
    class_id: &'static str,
    target: &'static str,
    instance_id: &'static str,
    route_id: &'static str,
    host: &'static str,
    path_prefix: &'static str,
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    frontline_addr: SocketAddr,
    app_image: String,
    grpc_app_image: String,
    sidecar_image: String,
}

#[derive(Debug)]
struct H2Response {
    version: Version,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    trailers: Option<HeaderMap>,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-protocols".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19551".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19580".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/protocol-app:kind-e2e-protocols".to_owned()),
            grpc_app_image: env::var("SLEEPYPODS_E2E_GRPC_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/grpc-app:kind-e2e-protocols".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-protocols".to_owned()),
        })
    }
}

fn route_specs() -> [RouteSpec; 4] {
    [
        RouteSpec {
            class_id: CLASS_ID,
            target: HTTP2_TARGET,
            instance_id: "e2e-protocol-http2",
            route_id: "e2e-protocol-http2-route",
            host: HTTP2_HOST,
            path_prefix: "/h2",
        },
        RouteSpec {
            class_id: H2C_CLASS_ID,
            target: GRPC_TARGET,
            instance_id: "e2e-protocol-grpc",
            route_id: "e2e-protocol-grpc-route",
            host: GRPC_HOST,
            path_prefix: "/",
        },
        RouteSpec {
            class_id: CLASS_ID,
            target: WEBSOCKET_TARGET,
            instance_id: "e2e-protocol-websocket",
            route_id: "e2e-protocol-websocket-route",
            host: WEBSOCKET_HOST,
            path_prefix: "/socket",
        },
        RouteSpec {
            class_id: GRPCIO_CLASS_ID,
            target: GRPCIO_TARGET,
            instance_id: "e2e-protocol-grpcio",
            route_id: "e2e-protocol-grpcio-route",
            host: GRPCIO_HOST,
            path_prefix: "/",
        },
    ]
}

async fn connect_operator(endpoint: &str) -> TestResult<OperatorControlPlaneClient<Channel>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let channel = Endpoint::from_shared(endpoint.to_owned())?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        match channel {
            Ok(channel) => return Ok(OperatorControlPlaneClient::new(channel)),
            Err(error) if tokio::time::Instant::now() < deadline => {
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
    for (class_id, app_protocol) in [
        (CLASS_ID, AppProtocol::Http1),
        (H2C_CLASS_ID, AppProtocol::H2c),
        (GRPCIO_CLASS_ID, AppProtocol::H2c),
    ] {
        operator
            .create_workload_class_version(CreateWorkloadClassVersionRequest {
                class_id: class_id.to_owned(),
                version: 1,
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
                template: Some(manifest_template(
                    config,
                    app_image(config, class_id),
                    app_protocol,
                )),
                sleep_policy: Some(WorkloadSleepPolicy {
                    idle_timeout_ms: 900_000,
                    idle_retry_backoff_ms: 500,
                    drain_grace_timeout_ms: 500,
                    idle_timeout_override: None,
                }),
                exclusivity_keys: vec![],
            })
            .await?;
    }

    for spec in route_specs() {
        operator
            .create_instance(CreateInstanceRequest {
                instance_id: spec.instance_id.to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: spec.class_id.to_owned(),
                    version: 1,
                }),
                values: HashMap::from([("target".to_owned(), spec.target.to_owned())]),
                routes: Vec::new(),
            })
            .await?;

        operator
            .create_route_binding(CreateRouteBindingRequest {
                route_binding_id: spec.route_id.to_owned(),
                instance_id: spec.instance_id.to_owned(),
                identity: Some(http_route_identity(spec.host, Some(spec.path_prefix))),
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
    let deadline = tokio::time::Instant::now() + timeout;
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

        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for instance {instance_id} to reach {expected:?}; last state was {actual:?} generation {}",
                instance.generation
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn http2_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    app_version: &str,
    timeout: Duration,
) -> TestResult<H2Response> {
    checked_h2_response(
        context,
        timeout,
        || {
            h2_request(
                config.frontline_addr,
                Method::GET,
                host,
                path,
                None,
                Bytes::new(),
            )
        },
        |response| assert_http2_response(response, context, target, path, host, app_version),
    )
    .await
}

async fn grpc_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    timeout: Duration,
) -> TestResult<H2Response> {
    checked_h2_response(
        context,
        timeout,
        || {
            h2_request(
                config.frontline_addr,
                Method::POST,
                host,
                path,
                Some("application/grpc"),
                Bytes::from_static(GRPC_REQUEST_BODY),
            )
        },
        |response| assert_grpc_response(response, context, target, path),
    )
    .await
}

// Every invocation sends exactly one request, including the first request to each Cold route.
// The cold-call budget leaves transport margin around the frontline's 130-second route deadline.
async fn checked_h2_response<RequestFuture, MakeRequest, Assert>(
    context: &str,
    timeout: Duration,
    make_request: MakeRequest,
    assert_response: Assert,
) -> TestResult<H2Response>
where
    RequestFuture: std::future::Future<Output = TestResult<H2Response>>,
    MakeRequest: FnOnce() -> RequestFuture,
    Assert: FnOnce(&H2Response) -> TestResult<()>,
{
    let response = tokio::time::timeout(timeout, make_request())
        .await
        .map_err(|_| format!("single {context} request exceeded {timeout:?}"))??;
    assert_response(&response)?;
    Ok(response)
}

async fn h2_request(
    addr: SocketAddr,
    method: Method,
    host: &str,
    path: &str,
    content_type: Option<&str>,
    body: Bytes,
) -> TestResult<H2Response> {
    let stream = TcpStream::connect(addr).await?;
    let (mut sender, connection) =
        client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await?;
    // Dropping a timed-out request also cancels its connection driver.
    let mut connection_tasks = tokio::task::JoinSet::new();
    connection_tasks.spawn(async move {
        let _ = connection.await;
    });

    let mut builder = Request::builder()
        .version(Version::HTTP_2)
        .method(method)
        .uri(format!("http://{host}{path}"));
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
        builder = builder.header("te", "trailers");
    }

    let response = sender.send_request(builder.body(Full::new(body))?).await?;
    let version = response.version();
    let status = response.status();
    let headers = response.headers().clone();
    let collected = response.into_body().collect().await?;
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();
    drop(sender);
    connection_tasks.shutdown().await;

    Ok(H2Response {
        version,
        status,
        headers,
        body,
        trailers,
    })
}

fn assert_http2_response(
    response: &H2Response,
    context: &str,
    target: &str,
    expected_path: &str,
    host: &str,
    app_version: &str,
) -> TestResult<()> {
    if response.version != Version::HTTP_2 {
        return Err(format!("{context} used {:?}, expected HTTP/2", response.version).into());
    }
    if response.status != StatusCode::OK {
        return Err(format!(
            "{context} returned HTTP {} with body {:?}",
            response.status,
            String::from_utf8_lossy(&response.body)
        )
        .into());
    }
    let body = String::from_utf8_lossy(&response.body);
    assert_app_saw(&body, context, target, expected_path, host, app_version)
}

/// The app reports the version it was served in and the host it received.
/// Both follow the app port's declared protocol and the client's authority,
/// whatever version the client spoke to frontline.
fn assert_app_saw(
    body: &str,
    context: &str,
    target: &str,
    expected_path: &str,
    host: &str,
    app_version: &str,
) -> TestResult<()> {
    assert_body_identifies(body, context, target, "http2")?;
    for marker in [
        format!("request_version={app_version}\n"),
        format!("path={expected_path}\n"),
        format!("host={host}\n"),
    ] {
        if !body.contains(&marker) {
            return Err(format!("{context} body lacks {marker:?}: {body:?}").into());
        }
    }
    Ok(())
}

/// Sends one HTTP/1.1 request through frontline and checks what the app saw.
async fn http1_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    app_version: &str,
) -> TestResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let exchange = async {
        let mut stream = TcpStream::connect(config.frontline_addr).await?;
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        TestResult::Ok(response)
    };
    let response = tokio::time::timeout(Duration::from_secs(30), exchange)
        .await
        .map_err(|_| format!("{context} request exceeded 30s"))??;
    if !response.starts_with("HTTP/1.1 200 ") {
        return Err(format!("{context} returned {response:?}").into());
    }
    assert_app_saw(&response, context, target, path, host, app_version)
}

fn assert_grpc_response(
    response: &H2Response,
    context: &str,
    target: &str,
    expected_path: &str,
) -> TestResult<()> {
    if response.version != Version::HTTP_2 {
        return Err(format!("{context} used {:?}, expected HTTP/2", response.version).into());
    }
    if response.status != StatusCode::OK {
        return Err(format!(
            "{context} returned HTTP {} with body {:?}",
            response.status, response.body
        )
        .into());
    }
    let content_type = response
        .headers
        .get("content-type")
        .ok_or_else(|| format!("{context} is missing content-type"))?;
    if content_type != "application/grpc" {
        return Err(format!(
            "{context} content-type was {content_type:?}, expected application/grpc"
        )
        .into());
    }
    let payload = grpc_payload_text(&response.body)?;
    assert_body_identifies(&payload, context, target, "h2c-grpc")?;
    if !payload.contains("request_version=HTTP/2\n") {
        return Err(format!("{context} did not reach the app over HTTP/2: {payload:?}").into());
    }
    if !payload.contains(&format!("path={expected_path}\n")) {
        return Err(format!(
            "{context} did not preserve path/query {expected_path:?}: {payload:?}"
        )
        .into());
    }
    if !payload.contains(&format!("request_body_hex={}\n", hex(GRPC_REQUEST_BODY))) {
        return Err(format!("{context} did not preserve the gRPC-shaped body: {payload:?}").into());
    }
    let trailers = response
        .trailers
        .as_ref()
        .ok_or_else(|| format!("{context} did not include trailers"))?;
    if trailers.get("grpc-status").map(|value| value.as_bytes()) != Some(&b"0"[..]) {
        return Err(format!(
            "{context} grpc-status trailer was {:?}",
            trailers.get("grpc-status")
        )
        .into());
    }
    let expected_message = format!("ok-{target}");
    if trailers.get("grpc-message").map(|value| value.as_bytes())
        != Some(expected_message.as_bytes())
    {
        return Err(format!(
            "{context} grpc-message trailer was {:?}, expected {expected_message:?}",
            trailers.get("grpc-message")
        )
        .into());
    }
    Ok(())
}

/// gRPC through frontline and the sidecar to a C-core server, and gRPC to an
/// `http1` app port.
///
/// C-core answers a request that lacks `te: trailers` with status UNKNOWN and
/// "Missing :te header", so status 0 here means the header crossed both hops.
async fn grpcio_calls(
    config: &E2eConfig,
    operator: &mut OperatorControlPlaneClient<Channel>,
) -> TestResult<()> {
    let instance = format!("e2e-protocol-{GRPCIO_TARGET}");
    for (context, timeout) in [
        ("cold C-core unary call", Duration::from_secs(140)),
        ("hot C-core unary call", Duration::from_secs(30)),
    ] {
        let response = grpc_call(
            config,
            context,
            GRPCIO_HOST,
            "/sleepypods.e2e.Echo/Unary",
            b"hello",
            timeout,
        )
        .await?;
        let messages = assert_grpc_ok(&response, context)?;
        let expected = format!("unary:{GRPCIO_TARGET}:hello").into_bytes();
        if messages != [expected] {
            return Err(format!("{context} returned messages {messages:?}").into());
        }
        wait_for_instance_state(
            operator,
            &instance,
            PbInstanceState::Running,
            Duration::from_secs(30),
        )
        .await?;
    }

    let context = "C-core server-streaming call";
    let response = grpc_call(
        config,
        context,
        GRPCIO_HOST,
        "/sleepypods.e2e.Echo/ServerStream",
        b"3",
        Duration::from_secs(30),
    )
    .await?;
    let messages = assert_grpc_ok(&response, context)?;
    let expected: Vec<Vec<u8>> = (0..3)
        .map(|index| format!("stream:{GRPCIO_TARGET}:{index}").into_bytes())
        .collect();
    if messages != expected {
        return Err(format!("{context} returned messages {messages:?}").into());
    }

    // An `http1` app port cannot serve gRPC. The sidecar answers in gRPC terms
    // and the app never sees the call.
    let context = "gRPC call to an http1 app port";
    let response = grpc_call(
        config,
        context,
        HTTP2_HOST,
        "/h2/sleepypods.e2e.Echo/Unary",
        b"hello",
        Duration::from_secs(30),
    )
    .await?;
    let header = |name: &str| response.headers.get(name).map(|value| value.as_bytes());
    if response.status != StatusCode::OK
        || header("grpc-status") != Some(&b"12"[..])
        || header("grpc-message").is_none()
        || !response.body.is_empty()
    {
        return Err(format!(
            "{context} expected grpc-status 12 in the response headers, got HTTP {} headers {:?} body {:?}",
            response.status, response.headers, response.body
        )
        .into());
    }
    Ok(())
}

/// Sends one gRPC call as a client library frames it: a length-prefixed
/// message, `content-type: application/grpc` and `te: trailers`.
async fn grpc_call(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    message: &[u8],
    timeout: Duration,
) -> TestResult<H2Response> {
    let mut body = vec![0];
    body.extend_from_slice(&(message.len() as u32).to_be_bytes());
    body.extend_from_slice(message);
    let request = h2_request(
        config.frontline_addr,
        Method::POST,
        host,
        path,
        Some("application/grpc"),
        Bytes::from(body),
    );
    tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| format!("{context} exceeded {timeout:?}"))?
}

/// Checks a successful gRPC response and returns its messages.
fn assert_grpc_ok(response: &H2Response, context: &str) -> TestResult<Vec<Vec<u8>>> {
    let status = response
        .trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .map(|value| value.as_bytes());
    if response.version != Version::HTTP_2
        || response.status != StatusCode::OK
        || status != Some(&b"0"[..])
    {
        return Err(format!(
            "{context} failed: {:?} HTTP {} headers {:?} trailers {:?}",
            response.version, response.status, response.headers, response.trailers
        )
        .into());
    }
    let instance = response
        .trailers
        .as_ref()
        .and_then(|trailers| trailers.get("x-e2e-instance"));
    if instance.map(|value| value.as_bytes()) != Some(GRPCIO_TARGET.as_bytes()) {
        return Err(format!("{context} trailing metadata named instance {instance:?}").into());
    }

    let mut messages = Vec::new();
    let mut body = &response.body[..];
    while !body.is_empty() {
        if body.len() < 5 || body[0] != 0 {
            return Err(format!("{context} returned a malformed gRPC frame: {body:?}").into());
        }
        let length = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        if body.len() < 5 + length {
            return Err(format!("{context} returned a truncated gRPC frame: {body:?}").into());
        }
        messages.push(body[5..5 + length].to_vec());
        body = &body[5 + length..];
    }
    Ok(messages)
}

fn grpc_payload_text(body: &[u8]) -> TestResult<String> {
    if body.len() < 5 {
        return Err(format!("gRPC body is too short: {body:?}").into());
    }
    if body[0] != 0 {
        return Err(format!("gRPC compression flag was {}, expected 0", body[0]).into());
    }
    let length = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    if body.len() != 5 + length {
        return Err(format!(
            "gRPC body length prefix was {length}, but body had {} payload bytes",
            body.len().saturating_sub(5)
        )
        .into());
    }
    Ok(String::from_utf8(body[5..].to_vec())?)
}

async fn websocket_exchange(
    addr: SocketAddr,
    host: &str,
    path: &str,
    target: &str,
) -> TestResult<()> {
    let mut request = format!("ws://{addr}{path}").into_client_request()?;
    request
        .headers_mut()
        .insert("host", host.parse().expect("test host is valid"));
    let (mut websocket, response) = connect_async(request).await?;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        return Err(format!(
            "WebSocket handshake returned {}, expected 101",
            response.status()
        )
        .into());
    }

    websocket
        .send(Message::Text("frontline text".into()))
        .await?;
    let text = websocket
        .next()
        .await
        .ok_or("WebSocket closed before text response")??;
    let Message::Text(text) = text else {
        return Err(format!("expected text response, got {text:?}").into());
    };
    assert_body_identifies(&text, "WebSocket text response", target, "websocket")?;
    if !text.contains("text=frontline text\n") || !text.contains(&format!("path={path}\n")) {
        return Err(format!("unexpected WebSocket text response: {text:?}").into());
    }

    websocket
        .send(Message::Binary(Bytes::from_static(b"frontline bytes")))
        .await?;
    let binary = websocket
        .next()
        .await
        .ok_or("WebSocket closed before binary response")??;
    let Message::Binary(binary) = binary else {
        return Err(format!("expected binary response, got {binary:?}").into());
    };
    let binary = String::from_utf8(binary.to_vec())?;
    assert_body_identifies(&binary, "WebSocket binary response", target, "websocket")?;
    if !binary.contains("binary=frontline bytes") || !binary.contains(&format!("path={path}\n")) {
        return Err(format!("unexpected WebSocket binary response: {binary:?}").into());
    }

    websocket
        .send(Message::Close(Some(CloseFrame {
            code: 1000.into(),
            reason: "done".into(),
        })))
        .await?;
    let close = websocket
        .next()
        .await
        .ok_or("WebSocket closed without close frame")??;
    match close {
        Message::Close(Some(frame)) if frame.code == 1000.into() && frame.reason == "done" => {
            Ok(())
        }
        other => Err(format!("unexpected WebSocket close response: {other:?}").into()),
    }
}

fn assert_body_identifies(
    body: &str,
    context: &str,
    target: &str,
    protocol: &str,
) -> TestResult<()> {
    if !body.contains("sleepypods-protocol-app\n") {
        return Err(format!("{context} body did not include protocol app marker: {body:?}").into());
    }
    if !body.contains(&format!("instance={target}\n")) {
        return Err(format!("{context} body did not identify target {target:?}: {body:?}").into());
    }
    if !body.contains(&format!("protocol={protocol}\n")) {
        return Err(
            format!("{context} body did not identify protocol {protocol:?}: {body:?}").into(),
        );
    }
    for other in TARGETS.iter().copied().filter(|other| *other != target) {
        if body.contains(&format!("instance={other}\n")) {
            return Err(
                format!("{context} body identified wrong target {other:?}: {body:?}").into(),
            );
        }
    }
    Ok(())
}

async fn assert_materialized_deployment_and_service(
    kube: Client,
    config: &E2eConfig,
    target: &str,
    app_image: &str,
    app_protocol: &str,
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
    if app.image.as_deref() != Some(app_image) {
        return Err(format!("expected app image {app_image}, got {:?}", app.image).into());
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
    assert_env(sidecar, "SLEEPYPODS_APP_PROTOCOL", app_protocol)?;
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

fn app_image<'a>(config: &'a E2eConfig, class_id: &str) -> &'a str {
    if class_id == GRPCIO_CLASS_ID {
        &config.grpc_app_image
    } else {
        &config.app_image
    }
}

fn manifest_template(
    config: &E2eConfig,
    app_image: &str,
    app_protocol: AppProtocol,
) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(target_text("e2e-protocol-", "")),
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
            termination_grace_period_seconds: None,
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(&config.sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
            app_protocol: app_protocol as i32,
        }),
        service: Some(ServiceTemplate {
            name: Some(target_text("e2e-protocol-", "")),
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

fn http_route_identity(host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
            host: Some(RouteHost {
                kind: RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
            path_prefix: path_prefix.map(str::to_owned),
        })),
    }
}

fn workload_name(target: &str) -> String {
    // Independently calculated SHA-256 suffixes for each complete fixture instance ID.
    let suffix = match target {
        "http2" => "ecdc91511c9b986e",
        "grpc" => "1e03905468da9734",
        "websocket" => "64dd4c6b5648b019",
        "grpcio" => "cd373d1d93902be7",
        _ => panic!("unknown fixture target {target}"),
    };
    format!("e2e-protocol-{target}-{suffix}")
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

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}
