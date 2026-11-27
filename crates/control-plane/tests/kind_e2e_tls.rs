#[path = "kind_e2e_tls/dynamic.rs"]
mod dynamic;
#[path = "support/native_operator.rs"]
mod native_operator;
use native_operator::{connect as connect_operator, Operator};
use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::Arc,
    time::Duration,
};

use control_plane::api::pb::{
    route_identity, template_text_part, ContainerPortTemplate, ContainerTemplate,
    CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    EnvVarTemplate, GetInstanceRequest, HttpRouteIdentity, Instance,
    InstanceState as PbInstanceState, ManifestTemplate, ProtocolRoute, RouteHost, RouteHostKind,
    RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate, SniRouteIdentity,
    TemplateText, TemplateTextPart, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy,
    WorkloadTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
};
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{Container, Service},
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{Api, Client};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, ServerName},
    ClientConfig, ClientConnection, RootCertStore, StreamOwned,
};
use tokio::time::{sleep, Instant};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const TEST_CERT_PEM: &[u8] = include_bytes!("../../../scripts/kind-e2e-tls-cert.pem");
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;

const TERMINATION_CLASS_ID: &str = "tls-termination-web";
const TERMINATION_TARGET: &str = "termination";
const TERMINATION_INSTANCE_ID: &str = "e2e-tls-termination";
const TERMINATION_ROUTE_ID: &str = "e2e-tls-termination-route";
const TERMINATION_HOST: &str = "terminate.sleepypods.test";

const PASSTHROUGH_CLASS_ID: &str = "tls-passthrough-web";
const EXACT_SNI_HOST: &str = "exact.sni.sleepypods.test";
const WILDCARD_SNI_SUFFIX: &str = "wild-sni.sleepypods.test";
const WILDCARD_SNI_CHILD_HOST: &str = "child.wild-sni.sleepypods.test";
const PASSTHROUGH_MISS_HOST: &str = "passthrough.sleepypods.test";
const PASSTHROUGH_TARGETS: &[&str] = &["sni-exact", "sni-wildcard"];

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-tls.sh or an equivalent kind deployment"]
async fn tls_termination_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_TLS").as_deref() != Ok("1") {
        eprintln!("skipping TLS kind E2E because SLEEPYPODS_KIND_E2E_TLS=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_tls_termination_resources(&mut operator, &config).await?;
    wait_for_instance_state(
        &mut operator,
        TERMINATION_INSTANCE_ID,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;

    // The application key exists only in this operator client and encrypted CP
    // storage. No Frontline startup file or preload can authorize this host.
    let app = rcgen::generate_simple_self_signed(vec![TERMINATION_HOST.into()])?;
    let published = operator
        .publish_certificate(control_plane::api::pb::PublishCertificateRequest {
            certificate_id: "kind-termination".into(),
            expected_version: Some(0),
            bundle: Some(control_plane::api::pb::CertificateBundle {
                chain_der: vec![app.cert.der().to_vec()],
                private_key_pkcs8_der: app.signing_key.serialize_der(),
            }),
        })
        .await?
        .into_inner();
    assert_eq!(published.version, 1);
    operator
        .set_tls_binding(control_plane::api::pb::SetTlsBindingRequest {
            hostname: TERMINATION_HOST.into(),
            expected_revision: Some(0),
            certificate_id: Some("kind-termination".into()),
        })
        .await?;
    wait_for_instance_state(
        &mut operator,
        TERMINATION_INSTANCE_ID,
        PbInstanceState::Cold,
        Duration::from_secs(1),
    )
    .await?;

    let response = cold_tls_response(
        config.tls_termination_addr,
        TlsResponseExpectation {
            certificate: Some(app.cert.der().clone()),
            context: "TLS termination",
            sni: TERMINATION_HOST,
            host: TERMINATION_HOST,
            path: "/terminated",
            target: TERMINATION_TARGET,
            mode: "http",
        },
    )
    .await?;
    assert_instance_response(&response, TERMINATION_TARGET, "http", &[TERMINATION_TARGET])?;
    wait_for_instance_state(
        &mut operator,
        TERMINATION_INSTANCE_ID,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    assert_materialized_deployment_and_service(kube, &config, TERMINATION_TARGET, "http", None)
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-tls.sh or an equivalent kind deployment"]
async fn sni_passthrough_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_TLS").as_deref() != Ok("1") {
        eprintln!("skipping TLS kind E2E because SLEEPYPODS_KIND_E2E_TLS=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_sni_passthrough_resources(&mut operator, &config).await?;
    for spec in passthrough_specs() {
        wait_for_instance_state(
            &mut operator,
            spec.instance_id,
            PbInstanceState::Cold,
            Duration::from_secs(30),
        )
        .await?;
    }

    let exact = cold_tls_response(
        config.tls_passthrough_addr,
        TlsResponseExpectation {
            certificate: None,
            context: "exact SNI passthrough",
            sni: EXACT_SNI_HOST,
            host: EXACT_SNI_HOST,
            path: "/passthrough/exact",
            target: "sni-exact",
            mode: "tls",
        },
    )
    .await?;
    assert_instance_response(&exact, "sni-exact", "tls", PASSTHROUGH_TARGETS)?;

    let wildcard = cold_tls_response(
        config.tls_passthrough_addr,
        TlsResponseExpectation {
            certificate: None,
            context: "wildcard SNI passthrough",
            sni: WILDCARD_SNI_CHILD_HOST,
            host: WILDCARD_SNI_CHILD_HOST,
            path: "/passthrough/wildcard",
            target: "sni-wildcard",
            mode: "tls",
        },
    )
    .await?;
    assert_instance_response(&wildcard, "sni-wildcard", "tls", PASSTHROUGH_TARGETS)?;

    assert_tls_route_miss(
        config.tls_passthrough_addr,
        "wildcard suffix base SNI miss",
        WILDCARD_SNI_SUFFIX,
        WILDCARD_SNI_SUFFIX,
        Duration::from_secs(30),
    )
    .await?;
    assert_tls_route_miss(
        config.tls_passthrough_addr,
        "unbound SAN-covered SNI miss",
        PASSTHROUGH_MISS_HOST,
        PASSTHROUGH_MISS_HOST,
        Duration::from_secs(30),
    )
    .await?;

    for spec in passthrough_specs() {
        wait_for_instance_state(
            &mut operator,
            spec.instance_id,
            PbInstanceState::Running,
            Duration::from_secs(30),
        )
        .await?;
        assert_materialized_deployment_and_service(
            kube.clone(),
            &config,
            spec.target,
            "tls",
            Some("tcp"),
        )
        .await?;
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct PassthroughSpec {
    target: &'static str,
    instance_id: &'static str,
    route_id: &'static str,
    host_kind: i32,
    host: &'static str,
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    tls_termination_addr: SocketAddr,
    tls_passthrough_addr: SocketAddr,
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
                .unwrap_or_else(|_| "sleepypods-e2e-tls".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "https://127.0.0.1:19351".to_owned()),
            tls_termination_addr: env::var("SLEEPYPODS_E2E_TLS_TERMINATION_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19443".to_owned())
                .parse()?,
            tls_passthrough_addr: env::var("SLEEPYPODS_E2E_TLS_PASSTHROUGH_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19444".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/tls-app:kind-e2e-tls".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-tls".to_owned()),
        })
    }
}

fn passthrough_specs() -> [PassthroughSpec; 2] {
    [
        PassthroughSpec {
            target: "sni-exact",
            instance_id: "e2e-tls-sni-exact",
            route_id: "e2e-tls-sni-exact-route",
            host_kind: RouteHostKind::Exact as i32,
            host: EXACT_SNI_HOST,
        },
        PassthroughSpec {
            target: "sni-wildcard",
            instance_id: "e2e-tls-sni-wildcard",
            route_id: "e2e-tls-sni-wildcard-route",
            host_kind: RouteHostKind::WildcardSuffix as i32,
            host: WILDCARD_SNI_SUFFIX,
        },
    ]
}

async fn create_tls_termination_resources(
    operator: &mut Operator,
    config: &E2eConfig,
) -> TestResult<()> {
    create_workload_class(operator, config, TERMINATION_CLASS_ID, "http", None).await?;

    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: "kind-e2e-tls-create-termination-instance".to_owned(),
            instance_id: TERMINATION_INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: TERMINATION_CLASS_ID.to_owned(),
                version: 1,
            }),
            values: HashMap::from([("target".to_owned(), TERMINATION_TARGET.to_owned())]),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: "kind-e2e-tls-create-termination-route".to_owned(),
            route_binding_id: TERMINATION_ROUTE_ID.to_owned(),
            instance_id: TERMINATION_INSTANCE_ID.to_owned(),
            identity: Some(http_route_identity(
                RouteHostKind::Exact as i32,
                TERMINATION_HOST,
            )),
            protocol: ProtocolRoute::Http as i32,
        })
        .await?;

    Ok(())
}

async fn create_sni_passthrough_resources(
    operator: &mut Operator,
    config: &E2eConfig,
) -> TestResult<()> {
    create_workload_class(operator, config, PASSTHROUGH_CLASS_ID, "tls", Some("tcp")).await?;

    for spec in passthrough_specs() {
        operator
            .create_instance(CreateInstanceRequest {
                idempotency_key: format!("kind-e2e-tls-create-instance-{}", spec.target),
                instance_id: spec.instance_id.to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: PASSTHROUGH_CLASS_ID.to_owned(),
                    version: 1,
                }),
                values: HashMap::from([("target".to_owned(), spec.target.to_owned())]),
            })
            .await?;

        operator
            .create_route_binding(CreateRouteBindingRequest {
                idempotency_key: format!("kind-e2e-tls-create-route-{}", spec.target),
                route_binding_id: spec.route_id.to_owned(),
                instance_id: spec.instance_id.to_owned(),
                identity: Some(sni_route_identity(spec.host_kind, spec.host)),
                protocol: ProtocolRoute::TlsSni as i32,
            })
            .await?;
    }

    Ok(())
}

async fn create_workload_class(
    operator: &mut Operator,
    config: &E2eConfig,
    class_id: &str,
    app_mode: &str,
    sidecar_mode: Option<&str>,
) -> TestResult<()> {
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
            template: Some(manifest_template(config, app_mode, sidecar_mode)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 900_000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        })
        .await?;
    Ok(())
}

async fn wait_for_instance_state(
    operator: &mut Operator,
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

struct TlsResponseExpectation<'a> {
    certificate: Option<CertificateDer<'static>>,
    context: &'a str,
    sni: &'a str,
    host: &'a str,
    path: &'a str,
    target: &'a str,
    mode: &'a str,
}

async fn cold_tls_response(
    addr: SocketAddr,
    expectation: TlsResponseExpectation<'_>,
) -> TestResult<HttpResponse> {
    // SNI routing can delay the TLS handshake while the Cold workload starts. Keep one
    // connection alive for the complete 130-second route deadline plus transport margin.
    let budget = Duration::from_secs(140);
    let response = tokio::time::timeout(
        budget,
        tls_get(
            addr,
            expectation.sni,
            expectation.host,
            expectation.path,
            budget,
            expectation.certificate,
        ),
    )
    .await
    .map_err(|_| format!("single {} request exceeded {budget:?}", expectation.context))??;
    assert_instance_response(&response, expectation.target, expectation.mode, &[])?;
    Ok(response)
}

async fn assert_tls_route_miss(
    addr: SocketAddr,
    context: &str,
    sni: &str,
    host: &str,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match tls_get(addr, sni, host, "/miss", Duration::from_secs(15), None).await {
            Ok(response) => {
                return Err(format!(
                    "{context} unexpectedly completed TLS/HTTP for SNI {sni}: HTTP {} {:?}",
                    response.status, response.body
                )
                .into());
            }
            Err(error) if is_expected_route_miss_error(&error.to_string()) => return Ok(()),
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for {context} to produce route-miss rejection: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => {
                return Err(format!(
                    "{context} did not produce a route-miss rejection for SNI {sni}: {error}"
                )
                .into());
            }
        }
    }
}

fn is_expected_route_miss_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    !lower.contains("certificate")
        && !lower.contains("notvalidforname")
        && (lower.contains("unexpected end")
            || lower.contains("connection reset")
            || lower.contains("closed connection"))
}

async fn tls_get(
    addr: SocketAddr,
    sni: &str,
    host: &str,
    path: &str,
    io_timeout: Duration,
    certificate: Option<CertificateDer<'static>>,
) -> TestResult<HttpResponse> {
    let sni = sni.to_owned();
    let host = host.to_owned();
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        tls_get_blocking(addr, &sni, &host, &path, io_timeout, certificate)
    })
    .await
    .map_err(|error| format!("TLS request task failed: {error}"))?
}

fn tls_get_blocking(
    addr: SocketAddr,
    sni: &str,
    host: &str,
    path: &str,
    timeout: Duration,
    certificate: Option<CertificateDer<'static>>,
) -> TestResult<HttpResponse> {
    let started = Instant::now();
    let mut roots = RootCertStore::empty();
    let dynamic = certificate.is_some();
    let certificate = match certificate {
        Some(cert) => cert,
        None => CertificateDer::from_pem_slice(TEST_CERT_PEM)?.into_owned(),
    };
    roots.add(certificate.clone())?;
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let server_name = ServerName::try_from(sni.to_owned())?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|error| format!("TLS TCP connect to {addr} for SNI {sni} failed: {error:?}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let conn = ClientConnection::new(Arc::new(config), server_name)?;
    let mut stream = StreamOwned::new(conn, stream);

    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|error| {
        format!(
            "TLS request write to {addr} for SNI {sni} failed after {:?}; handshaking={}, protocol={:?}: {error:?} ({error})",
            started.elapsed(), stream.conn.is_handshaking(), stream.conn.protocol_version()
        )
    })?;
    stream.flush().map_err(|error| {
        format!(
            "TLS request flush to {addr} for SNI {sni} failed after {:?}; handshaking={}, protocol={:?}: {error:?} ({error})",
            started.elapsed(), stream.conn.is_handshaking(), stream.conn.protocol_version()
        )
    })?;

    let mut bytes = Vec::new();
    match stream.read_to_end(&mut bytes) {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof && !bytes.is_empty() => {}
        Err(error) => {
            return Err(format!(
                "TLS response read from {addr} for SNI {sni} failed after {:?}; received_plaintext_bytes={}, handshaking={}, protocol={:?}: {error:?} ({error})",
                started.elapsed(), bytes.len(), stream.conn.is_handshaking(), stream.conn.protocol_version()
            ).into());
        }
    }
    let peer = &stream
        .conn
        .peer_certificates()
        .ok_or("TLS peer certificate absent")?[0];
    assert_eq!(peer, &certificate, "exact verified application peer");
    if dynamic {
        assert_eq!(stream.conn.alpn_protocol(), Some(&b"http/1.1"[..]));
    }
    use sha2::{Digest, Sha256};
    let fingerprint: String = Sha256::digest(peer.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    eprintln!(
        "TLS_PEER sni={sni} fingerprint={fingerprint} protocol={:?} alpn={:?}",
        stream.conn.protocol_version(),
        stream.conn.alpn_protocol()
    );
    let raw = String::from_utf8_lossy(&bytes);
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| format!("TLS response from {addr} did not include a status line"))?
        .parse::<u16>()?;

    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

fn assert_instance_response(
    response: &HttpResponse,
    target: &str,
    mode: &str,
    all_targets: &[&str],
) -> TestResult<()> {
    if response.status != 200 {
        return Err(format!("expected HTTP 200, got {}", response.status).into());
    }
    if !response.body.contains("sleepypods-tls-app\n") {
        return Err(format!("body did not include TLS app marker: {:?}", response.body).into());
    }
    if !response_body_identifies(response, target) {
        return Err(format!(
            "body did not identify target {target:?}: {:?}",
            response.body
        )
        .into());
    }
    if !response.body.contains(&format!("mode={mode}\n")) {
        return Err(format!(
            "body did not identify app mode {mode:?}: {:?}",
            response.body
        )
        .into());
    }
    for other in all_targets.iter().copied().filter(|other| *other != target) {
        if response_body_identifies(response, other) {
            return Err(format!(
                "body identified wrong target {other:?}: {:?}",
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

async fn assert_materialized_deployment_and_service(
    kube: Client,
    config: &E2eConfig,
    target: &str,
    app_mode: &str,
    sidecar_mode: Option<&str>,
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
    assert_env(app, "SLEEPYPODS_E2E_TLS_APP_MODE", app_mode)?;

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
            "https://sleepypods-control-plane.{}.svc.cluster.local:50051",
            config.namespace
        ),
    )?;
    assert_ca_env(sidecar, &env::var("SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM")?)?;
    let service = services.get(&workload_name).await?;
    let backend_scheme = service
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("sleepypods.io/backend-scheme"))
        .map(String::as_str);
    match sidecar_mode {
        Some("tcp") => {
            assert_env(sidecar, "SLEEPYPODS_SIDECAR_MODE", "tcp")?;
            if backend_scheme != Some("tcp") {
                return Err(format!(
                    "expected Service backend scheme annotation tcp, got {backend_scheme:?}"
                )
                .into());
            }
        }
        Some(mode) => {
            assert_env(sidecar, "SLEEPYPODS_SIDECAR_MODE", mode)?;
            if backend_scheme.is_some() {
                return Err(format!(
                    "expected no Service backend scheme annotation for sidecar mode {mode}, got {backend_scheme:?}"
                )
                .into());
            }
        }
        None => {
            if backend_scheme.is_some() {
                return Err(format!(
                    "expected no Service backend scheme annotation, got {backend_scheme:?}"
                )
                .into());
            }
        }
    }

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

fn manifest_template(
    config: &E2eConfig,
    app_mode: &str,
    sidecar_mode: Option<&str>,
) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(target_text("e2e-tls-", "")),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "app".to_owned(),
                image: Some(literal_text(&config.app_image)),
                ports: vec![ContainerPortTemplate {
                    name: Some("app".to_owned()),
                    container_port: APP_PORT,
                }],
                env: vec![
                    EnvVarTemplate {
                        name: "SLEEPYPODS_E2E_INSTANCE".to_owned(),
                        value: Some(target_text("", "")),
                    },
                    EnvVarTemplate {
                        name: "SLEEPYPODS_E2E_TLS_APP_MODE".to_owned(),
                        value: Some(literal_text(app_mode)),
                    },
                ],
            }),
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(&config.sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: sidecar_mode.map(str::to_owned),
        }),
        service: Some(ServiceTemplate {
            name: Some(target_text("e2e-tls-", "")),
            ports: vec![ServicePortTemplate {
                name: Some("app".to_owned()),
                port: APP_PORT,
                target_port: APP_PORT,
            }],
        }),
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn http_route_identity(host_kind: i32, host: &str) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
            host: Some(RouteHost {
                kind: host_kind,
                host: host.to_owned(),
            }),
            path_prefix: None,
        })),
    }
}

fn sni_route_identity(host_kind: i32, host: &str) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Sni(SniRouteIdentity {
            host: Some(RouteHost {
                kind: host_kind,
                host: host.to_owned(),
            }),
        })),
    }
}

fn workload_name(target: &str) -> String {
    // Independently calculated SHA-256 suffixes for each complete fixture instance ID.
    let suffix = match target {
        "termination" => "f589b132",
        "sni-exact" => "24c750bd",
        "sni-wildcard" => "df3d5390",
        _ => panic!("unknown fixture target {target}"),
    };
    format!("e2e-tls-{target}-{suffix}")
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

// Shell command substitution removes final newlines; Kubernetes env-from-Secret
// preserves them. Compare the complete public certificate identities, preserving
// every unrelated rendered environment assertion exactly.
fn assert_ca_env(container: &Container, expected: &str) -> TestResult<()> {
    let actual = container
        .env
        .as_ref()
        .and_then(|vars| {
            vars.iter()
                .find(|var| var.name == "SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM")
        })
        .and_then(|var| var.value.as_deref())
        .ok_or("missing sidecar platform CA")?;
    let decode = |pem: &str| -> TestResult<Vec<CertificateDer<'static>>> {
        let certificates =
            CertificateDer::pem_slice_iter(pem.as_bytes()).collect::<Result<Vec<_>, _>>()?;
        if certificates.is_empty() {
            return Err("platform CA contains no certificate".into());
        }
        Ok(certificates)
    };
    if decode(actual)? != decode(expected)? {
        return Err(
            "rendered sidecar platform trust differs from expected certificate identity".into(),
        );
    }
    Ok(())
}

#[test]
fn rendered_platform_trust_compares_certificate_identity_without_shell_newline_artifacts() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let other = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let make = |value: String| Container {
        name: "sidecar".into(),
        env: Some(vec![k8s_openapi::api::core::v1::EnvVar {
            name: "SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM".into(),
            value: Some(value),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let pem = cert.cert.pem();
    let container = make(pem.clone());
    assert!(assert_ca_env(&container, pem.trim_end_matches('\n')).is_ok());
    assert!(assert_ca_env(&container, &other.cert.pem()).is_err());
    assert!(assert_ca_env(&make(String::new()), "").is_err());
    assert!(assert_ca_env(
        &make("-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----\n".into()),
        &pem
    )
    .is_err());
}
