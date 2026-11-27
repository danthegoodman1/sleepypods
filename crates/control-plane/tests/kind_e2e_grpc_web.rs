use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, OnceLock},
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use control_plane::api::pb::{
    template_text_part, ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest,
    CreateWorkloadClassVersionRequest, DeleteInstanceRequest, DeleteInstanceResponse,
    GetInstanceRequest, GetWorkloadClassVersionRequest, Instance, InstanceState, ManifestTemplate,
    ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart,
    WorkloadClassVersion, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy,
    WorkloadTemplate,
};
use prost::Message;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const OPERATOR_SERVICE_NAME: &str = "sleepypods.controlplane.v1.OperatorControlPlane";
/// The origin the gate lists in `SLEEPYPODS_OPERATOR_GRPC_WEB_ALLOWED_ORIGINS`.
const ORIGIN: &str = "https://operator.example";
const OTHER_ORIGIN: &str = "https://elsewhere.example";
/// The platform certificate names `localhost`, which the port-forward serves.
const SERVER_NAME: &str = "localhost";

static TLS: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
const CLASS_ID: &str = "grpc-web-kind";
const INSTANCE_ID: &str = "grpc-web-kind-instance";

#[test]
#[ignore = "requires scripts/test-kind-e2e-grpc-web.sh or an equivalent kind deployment"]
fn browser_shaped_grpc_web_operator_calls_deployed_control_plane() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_GRPC_WEB").as_deref() != Ok("1") {
        eprintln!("skipping grpc-web kind E2E because SLEEPYPODS_KIND_E2E_GRPC_WEB=1 is not set");
        return Ok(());
    }

    let config = E2eConfig::from_env()?;
    let endpoint = config.grpc_web_endpoint;
    TLS.set(config.tls.clone())
        .map_err(|_| "TLS client configuration is set once")?;

    assert_plaintext_refused(&endpoint)?;
    assert_other_origin_refused(&endpoint, &config.invalid_token)?;
    assert_preflight(&endpoint, "CreateWorkloadClassVersion")?;
    grpc_web_unary_expect_status_with_authorization::<Instance, _>(
        &endpoint,
        "CreateInstance",
        CreateInstanceRequest {
            idempotency_key: "kind-e2e-grpc-web-auth-missing".to_owned(),
            instance_id: "grpc-web-kind-auth-missing".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: Default::default(),
            routes: Vec::new(),
        },
        "16",
        None,
    )?;
    grpc_web_unary_expect_status_with_authorization::<Instance, _>(
        &endpoint,
        "CreateInstance",
        CreateInstanceRequest {
            idempotency_key: "kind-e2e-grpc-web-auth-invalid".to_owned(),
            instance_id: "grpc-web-kind-auth-invalid".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: Default::default(),
            routes: Vec::new(),
        },
        "16",
        Some(&format!("Bearer {}", config.invalid_token)),
    )?;

    let created_class: WorkloadClassVersion = grpc_web_unary(
        &endpoint,
        "CreateWorkloadClassVersion",
        CreateWorkloadClassVersionRequest {
            class_id: CLASS_ID.to_owned(),
            version: 1,
            value_schema: None,
            template: Some(manifest_template()),
            sleep_policy: Some(sleep_policy()),
            exclusivity_keys: vec![],
        },
    )?;
    assert_eq!(
        created_class
            .reference
            .as_ref()
            .expect("created class has reference")
            .class_id,
        CLASS_ID
    );

    let loaded_class: WorkloadClassVersion = grpc_web_unary(
        &endpoint,
        "GetWorkloadClassVersion",
        GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
        },
    )?;
    assert_eq!(loaded_class, created_class);

    let created_instance: Instance = grpc_web_unary(
        &endpoint,
        "CreateInstance",
        CreateInstanceRequest {
            idempotency_key: "kind-e2e-grpc-web-create-instance".to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: Default::default(),
            routes: Vec::new(),
        },
    )?;
    assert_eq!(created_instance.instance_id, INSTANCE_ID);
    assert_eq!(created_instance.state, InstanceState::Cold as i32);

    let loaded_instance: Instance = grpc_web_unary(
        &endpoint,
        "GetInstance",
        GetInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
        },
    )?;
    assert_eq!(loaded_instance, created_instance);

    for missing_instance_id in ["grpc-web-kind-auth-missing", "grpc-web-kind-auth-invalid"] {
        let missing = grpc_web_unary_expect_status::<Instance, _>(
            &endpoint,
            "GetInstance",
            GetInstanceRequest {
                instance_id: missing_instance_id.to_owned(),
            },
            "5",
        )?;
        let message = missing
            .grpc_message()
            .ok_or("missing grpc-message for auth-rejected create probe")?;
        if !message.contains("instance%20not%20found") {
            return Err(format!("expected not-found grpc-message, got {message:?}").into());
        }
    }

    let deleted: DeleteInstanceResponse = grpc_web_unary(
        &endpoint,
        "DeleteInstance",
        DeleteInstanceRequest {
            expected_generation: Some(loaded_instance.generation),
            instance_id: INSTANCE_ID.to_owned(),
        },
    )?;
    assert!(deleted.accepted);

    let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(60);
    while grpc_web_unary::<Instance, _>(
        &endpoint,
        "GetInstance",
        GetInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
        },
    )
    .is_ok()
    {
        if std::time::Instant::now() >= cleanup_deadline {
            return Err("deletion did not converge".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let missing = grpc_web_unary_expect_status::<Instance, _>(
        &endpoint,
        "GetInstance",
        GetInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
        },
        "5",
    )?;
    let message = missing
        .grpc_message()
        .ok_or("missing grpc-message for deleted instance")?;
    if !message.contains("instance%20not%20found") {
        return Err(format!("expected not-found grpc-message, got {message:?}").into());
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    grpc_web_endpoint: SocketAddr,
    invalid_token: String,
    tls: Arc<rustls::ClientConfig>,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: HashMap<String, Vec<String>>,
    body: Vec<u8>,
}

#[derive(Debug)]
struct GrpcWebResponse<M> {
    headers: HashMap<String, Vec<String>>,
    trailers: HashMap<String, String>,
    message: Option<M>,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            grpc_web_endpoint: env::var("SLEEPYPODS_E2E_GRPC_WEB_ENDPOINT")
                .unwrap_or_else(|_| "127.0.0.1:19652".to_owned())
                .parse()?,
            invalid_token: env::var("SLEEPYPODS_E2E_INVALID_TOKEN")
                .unwrap_or_else(|_| "invalid-token".to_owned()),
            tls: tls_client_config(&env::var("SLEEPYPODS_E2E_GRPC_WEB_CA_FILE")?)?,
        })
    }
}

/// Trusts the platform certificate the gate generated. The client offers no
/// ALPN protocol, so the listener serves it HTTP/1.1.
fn tls_client_config(ca_file: &str) -> TestResult<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::{pem::PemObject, CertificateDer};

    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(ca_file)? {
        roots.add(certificate?)?;
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    ))
}

fn assert_plaintext_refused(endpoint: &SocketAddr) -> TestResult<()> {
    let mut stream = TcpStream::connect_timeout(endpoint, Duration::from_secs(10))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(&build_grpc_web_request(
        endpoint,
        "GetInstance",
        ORIGIN,
        &grpc_frame(GetInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
        }),
        None,
    ))?;
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    if response.starts_with(b"HTTP/") {
        return Err("the gRPC-Web listener answered a plaintext request".into());
    }
    Ok(())
}

fn assert_other_origin_refused(endpoint: &SocketAddr, invalid_token: &str) -> TestResult<()> {
    let operator_token =
        env::var("SLEEPYPODS_E2E_OPERATOR_TOKEN").unwrap_or_else(|_| "operator-token".to_owned());
    for token in [operator_token.as_str(), invalid_token] {
        let response = http_request(
            endpoint,
            build_grpc_web_request(
                endpoint,
                "GetInstance",
                OTHER_ORIGIN,
                &grpc_frame(GetInstanceRequest {
                    instance_id: INSTANCE_ID.to_owned(),
                }),
                Some(&format!("Bearer {token}")),
            ),
        )?;
        if response.status != 403 {
            return Err(format!(
                "a page from {OTHER_ORIGIN} must be refused, got HTTP {}",
                response.status
            )
            .into());
        }
        if header_first(&response.headers, "access-control-allow-origin").is_some() {
            return Err("a refused origin must not be named as allowed".into());
        }
    }
    Ok(())
}

impl<M> GrpcWebResponse<M> {
    fn grpc_status(&self) -> Option<&str> {
        self.header_value("grpc-status")
            .or_else(|| self.trailers.get("grpc-status").map(String::as_str))
    }

    fn grpc_message(&self) -> Option<&str> {
        self.header_value("grpc-message")
            .or_else(|| self.trailers.get("grpc-message").map(String::as_str))
    }

    fn header_value(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .and_then(|values| values.first())
            .map(String::as_str)
    }
}

fn assert_preflight(endpoint: &SocketAddr, method: &'static str) -> TestResult<()> {
    let response = http_request(
        endpoint,
        format!(
            concat!(
                "OPTIONS {} HTTP/1.1\r\n",
                "Host: {}\r\n",
                "Origin: {}\r\n",
                "Access-Control-Request-Method: POST\r\n",
                "Access-Control-Request-Headers: authorization,content-type,x-grpc-web,x-sleepypods-operator\r\n",
                "Connection: close\r\n",
                "Content-Length: 0\r\n",
                "\r\n",
            ),
            operator_path(method),
            endpoint,
            ORIGIN
        )
        .into_bytes(),
    )?;

    if !(200..300).contains(&response.status) {
        return Err(format!("preflight returned HTTP {}", response.status).into());
    }
    assert_header_eq(&response, "access-control-allow-origin", ORIGIN)?;
    assert_header_contains(&response, "access-control-allow-methods", "POST")?;
    assert_header_contains(&response, "access-control-allow-methods", "OPTIONS")?;
    for header in [
        "authorization",
        "content-type",
        "x-grpc-web",
        "x-sleepypods-operator",
    ] {
        assert_header_contains(&response, "access-control-allow-headers", header)?;
    }

    Ok(())
}

fn grpc_web_unary<M, R>(endpoint: &SocketAddr, method: &'static str, request: R) -> TestResult<M>
where
    M: Message + Default,
    R: Message,
{
    let response = grpc_web_unary_expect_status(endpoint, method, request, "0")?;
    response
        .message
        .ok_or_else(|| format!("missing grpc-web response message for {method}").into())
}

fn grpc_web_unary_expect_status<M, R>(
    endpoint: &SocketAddr,
    method: &'static str,
    request: R,
    expected_status: &str,
) -> TestResult<GrpcWebResponse<M>>
where
    M: Message + Default,
    R: Message,
{
    let body = grpc_frame(request);
    let authorization = format!(
        "Bearer {}",
        env::var("SLEEPYPODS_E2E_OPERATOR_TOKEN").unwrap_or_else(|_| "operator-token".to_owned())
    );
    let response = http_request(
        endpoint,
        build_grpc_web_request(endpoint, method, ORIGIN, &body, Some(&authorization)),
    )?;
    decode_grpc_web_response(endpoint, method, response, expected_status)
}

fn grpc_web_unary_expect_status_with_authorization<M, R>(
    endpoint: &SocketAddr,
    method: &'static str,
    request: R,
    expected_status: &str,
    authorization: Option<&str>,
) -> TestResult<GrpcWebResponse<M>>
where
    M: Message + Default,
    R: Message,
{
    let body = grpc_frame(request);
    let response = http_request(
        endpoint,
        build_grpc_web_request(endpoint, method, ORIGIN, &body, authorization),
    )?;
    decode_grpc_web_response(endpoint, method, response, expected_status)
}

fn decode_grpc_web_response<M>(
    _endpoint: &SocketAddr,
    method: &'static str,
    response: HttpResponse,
    expected_status: &str,
) -> TestResult<GrpcWebResponse<M>>
where
    M: Message + Default,
{
    if response.status != 200 {
        return Err(format!("{method} returned HTTP {}", response.status).into());
    }
    assert_header_contains(&response, "content-type", "application/grpc-web+proto")?;
    assert_header_eq(&response, "access-control-allow-origin", ORIGIN)?;
    assert_header_contains(&response, "access-control-expose-headers", "grpc-status")?;
    assert_header_contains(&response, "access-control-expose-headers", "grpc-message")?;

    let HttpResponse { headers, body, .. } = response;
    let decoded = decode_grpc_web_body::<M>(&body, headers)?;
    if expected_status == "0"
        && decoded.trailers.get("grpc-status").map(String::as_str) != Some("0")
    {
        return Err(
            format!("{method} did not return grpc-status 0 in a grpc-web trailer frame").into(),
        );
    }
    if decoded.grpc_status() != Some(expected_status) {
        return Err(format!(
            "{method} returned grpc-status {:?}, expected {expected_status}",
            decoded.grpc_status()
        )
        .into());
    }
    Ok(decoded)
}

fn build_grpc_web_request(
    endpoint: &SocketAddr,
    method: &'static str,
    origin: &str,
    body: &[u8],
    authorization: Option<&str>,
) -> Vec<u8> {
    let mut request = format!(
        concat!(
            "POST {} HTTP/1.1\r\n",
            "Host: {}\r\n",
            "Origin: {}\r\n",
            "Content-Type: application/grpc-web+proto\r\n",
            "Accept: application/grpc-web+proto\r\n",
            "X-Grpc-Web: 1\r\n",
            "X-User-Agent: grpc-web-javascript/0.1\r\n",
            "X-Sleepypods-Operator: kind-e2e\r\n",
            "Connection: close\r\n",
            "Content-Length: {}\r\n",
        ),
        operator_path(method),
        endpoint,
        origin,
        body.len()
    )
    .into_bytes();
    if let Some(authorization) = authorization {
        request.extend_from_slice(format!("Authorization: {authorization}\r\n").as_bytes());
    }
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(body);
    request
}

fn http_request(endpoint: &SocketAddr, request: Vec<u8>) -> TestResult<HttpResponse> {
    let tcp = TcpStream::connect_timeout(endpoint, Duration::from_secs(10))?;
    tcp.set_read_timeout(Some(Duration::from_secs(30)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(10)))?;
    let tls = TLS.get().ok_or("TLS client configuration is missing")?;
    let connection = rustls::ClientConnection::new(tls.clone(), SERVER_NAME.try_into()?)?;
    let mut stream = rustls::StreamOwned::new(connection, tcp);
    stream.write_all(&request)?;

    let mut bytes = Vec::new();
    match stream.read_to_end(&mut bytes) {
        Ok(_) => {}
        // `Connection: close` ends the response; the peer may close the socket
        // without a TLS close_notify.
        Err(error) if error.kind() == ErrorKind::UnexpectedEof && !bytes.is_empty() => {}
        Err(error) => return Err(error.into()),
    }
    parse_http_response(&bytes)
}

fn parse_http_response(bytes: &[u8]) -> TestResult<HttpResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("HTTP response did not contain header terminator")?;
    let header_text = std::str::from_utf8(&bytes[..header_end])?;
    let mut lines = header_text.split("\r\n");
    let status_line = lines.next().ok_or("HTTP response missing status line")?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("HTTP response status line missing status code")?
        .parse::<u16>()?;
    let mut headers: HashMap<String, Vec<String>> = HashMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| format!("invalid HTTP header line {line:?}"))?;
        headers
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.trim_start().to_owned());
    }

    let raw_body = &bytes[header_end + 4..];
    let body = if header_contains_value(&headers, "transfer-encoding", "chunked") {
        decode_chunked_body(raw_body)?
    } else if let Some(length) = header_first(&headers, "content-length") {
        let length = length.parse::<usize>()?;
        raw_body
            .get(..length)
            .ok_or("HTTP response body shorter than Content-Length")?
            .to_vec()
    } else {
        raw_body.to_vec()
    };

    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn decode_chunked_body(mut bytes: &[u8]) -> TestResult<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or("chunked response missing chunk length terminator")?;
        let length_text = std::str::from_utf8(&bytes[..line_end])?;
        let length_hex = length_text.split(';').next().unwrap_or(length_text);
        let length = usize::from_str_radix(length_hex.trim(), 16)?;
        bytes = &bytes[line_end + 2..];
        if length == 0 {
            return Ok(decoded);
        }
        if bytes.len() < length + 2 {
            return Err("chunked response ended inside a chunk".into());
        }
        decoded.extend_from_slice(&bytes[..length]);
        if &bytes[length..length + 2] != b"\r\n" {
            return Err("chunked response chunk missing trailing CRLF".into());
        }
        bytes = &bytes[length + 2..];
    }
}

fn grpc_frame<M: Message>(message: M) -> Vec<u8> {
    let mut encoded = BytesMut::new();
    message
        .encode(&mut encoded)
        .expect("protobuf request encodes");

    let mut frame = BytesMut::with_capacity(5 + encoded.len());
    frame.put_u8(0);
    frame.put_u32(encoded.len() as u32);
    frame.extend_from_slice(&encoded);
    frame.to_vec()
}

fn decode_grpc_web_body<M: Message + Default>(
    bytes: &[u8],
    headers: HashMap<String, Vec<String>>,
) -> TestResult<GrpcWebResponse<M>> {
    let mut offset = 0;
    let mut message = None;
    let mut trailers = HashMap::new();

    while offset + 5 <= bytes.len() {
        let frame_type = bytes[offset];
        let length = u32::from_be_bytes(bytes[offset + 1..offset + 5].try_into()?) as usize;
        offset += 5;
        if offset + length > bytes.len() {
            return Err("grpc-web frame length exceeds response body".into());
        }
        let payload = &bytes[offset..offset + length];
        if frame_type & 0x80 != 0 {
            let text = std::str::from_utf8(payload)?;
            for line in text.split("\r\n").filter(|line| !line.is_empty()) {
                let (name, value) = line
                    .split_once(':')
                    .ok_or_else(|| format!("invalid grpc-web trailer line {line:?}"))?;
                trailers.insert(name.to_ascii_lowercase(), value.trim_start().to_owned());
            }
        } else if frame_type == 0 {
            message = Some(M::decode(payload)?);
        } else {
            return Err(format!("unexpected grpc-web frame type {frame_type:#x}").into());
        }
        offset += length;
    }

    if offset != bytes.len() {
        return Err("grpc-web body ended with a partial frame".into());
    }

    Ok(GrpcWebResponse {
        headers,
        trailers,
        message,
    })
}

fn assert_header_eq(response: &HttpResponse, name: &str, expected: &str) -> TestResult<()> {
    let value = header_first(&response.headers, name)
        .ok_or_else(|| format!("missing HTTP header {name}"))?;
    if value != expected {
        return Err(format!("expected HTTP header {name}={expected:?}, got {value:?}").into());
    }
    Ok(())
}

fn assert_header_contains(response: &HttpResponse, name: &str, expected: &str) -> TestResult<()> {
    if !header_contains_value(&response.headers, name, expected) {
        return Err(format!(
            "expected HTTP header {name} to contain {expected:?}, got {:?}",
            response.headers.get(name)
        )
        .into());
    }
    Ok(())
}

fn header_first<'a>(headers: &'a HashMap<String, Vec<String>>, name: &str) -> Option<&'a str> {
    headers
        .get(&name.to_ascii_lowercase())
        .and_then(|values| values.first())
        .map(String::as_str)
}

fn header_contains_value(
    headers: &HashMap<String, Vec<String>>,
    name: &str,
    expected: &str,
) -> bool {
    headers
        .get(&name.to_ascii_lowercase())
        .into_iter()
        .flatten()
        .any(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case(expected))
                || value
                    .to_ascii_lowercase()
                    .contains(&expected.to_ascii_lowercase())
        })
}

fn operator_path(method: &'static str) -> String {
    format!("/{OPERATOR_SERVICE_NAME}/{method}")
}

fn manifest_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(literal_text("grpc-web-kind")),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "app".to_owned(),
                image: Some(literal_text("registry.k8s.io/pause:3.10")),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: Vec::new(),
            }),
            termination_grace_period_seconds: None,
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text("sleepypods/sidecar:kind-e2e-grpc-web")),
            listen_port: 15_000,
            mode: None,
            app_protocol: Default::default(),
        }),
        service: Some(ServiceTemplate {
            name: Some(literal_text("grpc-web-kind")),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 8080,
                target_port: 8080,
            }],
        }),
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn sleep_policy() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms: 900_000,
        idle_retry_backoff_ms: 500,
        drain_grace_timeout_ms: 500,
        idle_timeout_override: None,
    }
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}
