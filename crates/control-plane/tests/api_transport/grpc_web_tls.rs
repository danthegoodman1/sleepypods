//! The gRPC-Web listener serves the platform TLS identity and admits browser
//! pages from configured origins only.
use super::*;

use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::codegen::http::{HeaderMap, StatusCode};
use tonic::codegen::tokio_stream::wrappers::TcpListenerStream;

const ALLOWED_ORIGIN: &str = "https://console.example";
const OTHER_ORIGIN: &str = "https://elsewhere.example";

struct WebListener {
    address: std::net::SocketAddr,
    certificate: rcgen::Certificate,
}

async fn serve_grpc_web() -> WebListener {
    control_plane::install_rustls_crypto_provider();
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("certificate");
    let tls = tonic::transport::ServerTlsConfig::new().identity(
        tonic::transport::Identity::from_pem(cert.pem(), signing_key.serialize_pem()),
    );
    let store = store_with_class("class-1", 1);
    store.seed_instance(domain_instance("web", DomainInstanceState::Cold, 0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    let router = control_plane::runtime::operator_grpc_web_router_with_tls(
        store,
        operator_materializer(FakeKubernetesClient::default()),
        target(),
        auth_config(),
        RouteSubscriptionBroker::new(),
        Some(tls),
        &[HeaderValue::from_static(ALLOWED_ORIGIN)],
    )
    .expect("gRPC-Web router builds");
    tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
    WebListener {
        address,
        certificate: cert,
    }
}

impl WebListener {
    /// One request over a fresh TLS connection that negotiates HTTP/2, as a
    /// browser does.
    async fn send(&self, method: Method, origin: Option<&str>) -> (StatusCode, HeaderMap, Bytes) {
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        roots
            .add(self.certificate.der().clone())
            .expect("root certificate");
        let mut config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![b"h2".to_vec()];
        let tcp = tokio::net::TcpStream::connect(self.address)
            .await
            .expect("TCP connects");
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect("localhost".try_into().expect("server name"), tcp)
            .await
            .expect("TLS handshake completes");
        let (mut sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
                .await
                .expect("HTTP/2 handshake completes");
        tokio::spawn(connection);

        let message = GetInstanceRequest {
            instance_id: "web".to_owned(),
        }
        .encode_to_vec();
        let mut frame = BytesMut::with_capacity(message.len() + 5);
        frame.put_u8(0);
        frame.put_u32(message.len() as u32);
        frame.put_slice(&message);
        let preflight = method == Method::OPTIONS;
        let mut request = Request::builder()
            .method(method)
            .uri(format!(
                "https://localhost:{}{}",
                self.address.port(),
                operator_method_uri("GetInstance")
            ))
            .header(header::CONTENT_TYPE, "application/grpc-web+proto")
            .header("x-grpc-web", "1")
            .header(header::AUTHORIZATION, "Bearer operator-token");
        if preflight {
            request = request
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .header(
                    header::ACCESS_CONTROL_REQUEST_HEADERS,
                    "authorization,content-type,x-grpc-web",
                );
        }
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        let body = if preflight {
            Bytes::new()
        } else {
            frame.freeze()
        };
        let response = sender
            .send_request(request.body(Full::new(body)).expect("request builds"))
            .await
            .expect("the listener answers over TLS");
        let (parts, body) = response.into_parts();
        let body = body.collect().await.expect("response body").to_bytes();
        (parts.status, parts.headers, body)
    }
}

fn grpc_status(headers: &HeaderMap, body: &[u8]) -> Option<String> {
    grpc_web_trailer_value(body, "grpc-status").or_else(|| {
        headers
            .get("grpc-status")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    })
}

#[tokio::test]
async fn grpc_web_listener_refuses_plaintext_when_tls_is_configured() {
    let listener = serve_grpc_web().await;
    let mut plaintext = tokio::net::TcpStream::connect(listener.address)
        .await
        .expect("TCP connects");
    plaintext
        .write_all(
            format!(
                "POST {} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/grpc-web+proto\r\n\
                 authorization: Bearer operator-token\r\ncontent-length: 0\r\n\r\n",
                operator_method_uri("GetInstance")
            )
            .as_bytes(),
        )
        .await
        .expect("request bytes are written");
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), plaintext.read_to_end(&mut response))
        .await
        .expect("the listener closes a plaintext connection");
    assert!(
        !response.starts_with(b"HTTP/"),
        "a TLS listener must not answer plaintext HTTP: {}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn grpc_web_listener_serves_allowed_origins_and_refuses_others() {
    let listener = serve_grpc_web().await;

    let (status, headers, body) = listener.send(Method::POST, Some(ALLOWED_ORIGIN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(grpc_status(&headers, &body).as_deref(), Some("0"));
    assert_eq!(
        headers
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("the allowed origin is echoed"),
        ALLOWED_ORIGIN
    );
    assert_eq!(decode_grpc_instance_response(&body).instance_id, "web");

    let (status, headers, _) = listener.send(Method::OPTIONS, Some(ALLOWED_ORIGIN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("the preflight names the allowed origin"),
        ALLOWED_ORIGIN
    );

    for method in [Method::POST, Method::OPTIONS] {
        let (status, headers, body) = listener.send(method, Some(OTHER_ORIGIN)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
        assert!(grpc_status(&headers, &body).is_none());
    }

    // A client that names no origin is not a browser page; its credential decides.
    let (status, headers, body) = listener.send(Method::POST, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(grpc_status(&headers, &body).as_deref(), Some("0"));
}

/// The kind gate's driver speaks HTTP/1.1 and offers no ALPN protocol.
#[tokio::test]
async fn grpc_web_listener_serves_http1_over_tls_without_alpn() {
    let listener = serve_grpc_web().await;
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots
        .add(listener.certificate.der().clone())
        .expect("root certificate");
    let config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(listener.address)
        .await
        .expect("TCP connects");
    let mut tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect("localhost".try_into().expect("server name"), tcp)
        .await
        .expect("TLS handshake completes");
    let message = GetInstanceRequest {
        instance_id: "web".to_owned(),
    }
    .encode_to_vec();
    let mut request = format!(
        "POST {} HTTP/1.1\r\nhost: localhost\r\norigin: {ALLOWED_ORIGIN}\r\n\
         content-type: application/grpc-web+proto\r\nx-grpc-web: 1\r\n\
         authorization: Bearer operator-token\r\nconnection: close\r\n\
         content-length: {}\r\n\r\n",
        operator_method_uri("GetInstance"),
        message.len() + 5
    )
    .into_bytes();
    request.push(0);
    request.extend_from_slice(&(message.len() as u32).to_be_bytes());
    request.extend_from_slice(&message);
    tls.write_all(&request).await.expect("request is written");

    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), tls.read_to_end(&mut response))
        .await
        .expect("the listener answers and closes");
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.contains(&format!("access-control-allow-origin: {ALLOWED_ORIGIN}")),
        "{response}"
    );
    assert!(response.contains("grpc-status:0"), "{response}");
}
