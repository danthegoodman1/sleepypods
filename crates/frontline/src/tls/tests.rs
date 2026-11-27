use std::{convert::Infallible, sync::Arc};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::DrainTracker;
use rcgen::generate_simple_self_signed;
use sleepypods_api::{BackendEndpoint, BackendGeneration, Generation, InstanceId, RouteIdentity};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    rustls::{
        pki_types::{CertificateDer, PrivateKeyDer, ServerName},
        ClientConfig, RootCertStore,
    },
    TlsConnector,
};

use crate::{
    tls::{
        passthrough_backend_addr, FrontlineTlsAdapter, TlsCertificateStore,
        TlsPassthroughBackendError, TlsPassthroughError, TlsTerminationError,
    },
    FrontlineForwarder, ReadyBackend, RouteRequestIdentity,
};

const REQUEST_BODY: &[u8] = b"tls request body";
const RESPONSE_BODY: &[u8] = b"tls response body";

#[tokio::test]
async fn unknown_sni_certificate_rejects_tls_handshake() {
    let fixture = crate::certificates::test_support::CertificateFixture::new();
    let store = fixture.cache.clone();
    let cert = test_cert("known.example.com");
    let client_config = client_config_trusting(cert.cert.clone());
    fixture
        .publish("known.example.com", vec![cert.cert], cert.key)
        .expect("cert inserts");
    let adapter = FrontlineTlsAdapter::new(store);

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("binds");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accepts");
        adapter
            .terminate(stream)
            .await
            .expect_err("unknown cert rejects")
    });

    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from("unknown.example.com")
        .expect("server name")
        .to_owned();
    let client = TcpStream::connect(addr).await.expect("client connects");
    assert!(connector.connect(server_name, client).await.is_err());

    assert!(matches!(
        server.await.expect("server task completes"),
        TlsTerminationError::UnknownCertificate
    ));
    fixture.finish().await;
}

#[tokio::test]
async fn missing_sni_rejects_tls_handshake() {
    let fixture = crate::certificates::test_support::CertificateFixture::new();
    let store = fixture.cache.clone();
    let cert = test_cert("known.example.com");
    let client_config = client_config_trusting(cert.cert.clone());
    fixture
        .publish("known.example.com", vec![cert.cert], cert.key)
        .expect("cert inserts");
    let adapter = FrontlineTlsAdapter::new(store);

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("binds");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accepts");
        adapter
            .terminate(stream)
            .await
            .expect_err("missing SNI rejects")
    });

    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from("127.0.0.1")
        .expect("ip server name")
        .to_owned();
    let client = TcpStream::connect(addr).await.expect("client connects");
    assert!(connector.connect(server_name, client).await.is_err());

    assert!(matches!(
        server.await.expect("server task completes"),
        TlsTerminationError::MissingSni
    ));
    fixture.finish().await;
}

#[tokio::test]
async fn https_termination_composes_with_frontline_http_forwarding() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    let ready = ready_backend(format!("http://{upstream_addr}"));

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener.accept().await.expect("upstream accepts");
        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|mut request: Request<Incoming>| async move {
                    assert_eq!(request.method(), Method::POST);
                    assert_eq!(
                        request.uri().path_and_query().expect("path query").as_str(),
                        "/secure?via=tls"
                    );
                    let body = request
                        .body_mut()
                        .collect()
                        .await
                        .expect("request body reads")
                        .to_bytes();
                    assert_eq!(body, Bytes::from_static(REQUEST_BODY));

                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::CREATED)
                            .body(Full::new(Bytes::from_static(RESPONSE_BODY)))
                            .expect("response builds"),
                    )
                }),
            )
            .await
            .expect("upstream serves");
    });

    let cert = test_cert("app.example.com");
    let client_config = client_config_trusting(cert.cert.clone());
    let fixture = crate::certificates::test_support::CertificateFixture::new();
    let store = fixture.cache.clone();
    fixture
        .publish("app.example.com", vec![cert.cert], cert.key)
        .expect("cert inserts");
    let adapter = FrontlineTlsAdapter::new(store);
    let forwarder = FrontlineForwarder::new(DrainTracker::new(std::time::Duration::from_secs(5)));
    let tls_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("tls listener binds");
    let tls_addr = tls_listener.local_addr().expect("tls addr");

    let server = tokio::spawn(async move {
        let (stream, _) = tls_listener.accept().await.expect("tls accepts");
        let terminated = adapter.terminate(stream).await.expect("tls terminates");
        match terminated.identity.into_identity() {
            RouteIdentity::Sni { host } => assert_eq!(host.as_str(), "app.example.com"),
            RouteIdentity::Http { .. } => panic!("expected SNI identity"),
        }

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(terminated.stream),
                service_fn(move |request: Request<Incoming>| {
                    let ready = ready.clone();
                    let forwarder = forwarder.clone();
                    async move {
                        let response = forwarder
                            .forward_http(&ready, request)
                            .await
                            .expect("request forwards");
                        Ok::<_, Infallible>(response)
                    }
                }),
            )
            .await
            .expect("terminated HTTP serves");
    });

    let response = raw_https_request(tls_addr, "app.example.com", client_config).await;
    assert!(response.starts_with("HTTP/1.1 201 Created"), "{response}");
    assert!(response.ends_with("tls response body"), "{response}");

    server.await.expect("server task completes");
    upstream_task.await.expect("upstream task completes");
    fixture.finish().await;
}

#[tokio::test]
async fn passthrough_extracts_sni_and_preserves_preread_prefix_and_tail() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    let ready = ready_backend(format!("tcp://{upstream_addr}"));
    let hello = client_hello(Some(sni_extension(b"Db.Example.COM.")));
    let tail = b"opaque database startup bytes";
    let mut expected = hello.clone();
    expected.extend_from_slice(tail);
    let upstream_response = Bytes::from_static(b"upstream tls bytes");

    let upstream_task = tokio::spawn({
        let upstream_response = upstream_response.clone();
        async move {
            let (mut stream, _) = upstream_listener.accept().await.expect("upstream accepts");
            let mut received = vec![0; expected.len()];
            stream
                .read_exact(&mut received)
                .await
                .expect("upstream reads preserved bytes");
            assert_eq!(received, expected);
            stream
                .write_all(&upstream_response)
                .await
                .expect("upstream writes response");
            stream.shutdown().await.expect("upstream shuts down");
        }
    });

    let adapter = FrontlineTlsAdapter::new(TlsCertificateStore::disabled());
    let (mut client, proxy_side) = io::duplex(4096);
    let proxy_task = tokio::spawn(async move { passthrough(&adapter, &ready, proxy_side).await });

    client.write_all(&hello).await.expect("client writes hello");
    client.write_all(tail).await.expect("client writes tail");
    client
        .shutdown()
        .await
        .expect("client write half shuts down");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("client reads response");

    assert_eq!(response, upstream_response);
    let identity = proxy_task
        .await
        .expect("proxy task completes")
        .expect("passthrough succeeds");
    match identity.into_identity() {
        RouteIdentity::Sni { host } => assert_eq!(host.as_str(), "db.example.com"),
        RouteIdentity::Http { .. } => panic!("expected SNI identity"),
    }

    upstream_task.await.expect("upstream task completes");
}

#[tokio::test]
async fn passthrough_waits_for_late_listener_after_refusal_and_sends_prefix_once() {
    use std::time::Duration;

    // Close a real listener and verify refusal before starting SNI setup. A bound
    // but non-listening TcpSocket blackholes SYNs on macOS instead of refusing.
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    assert_eq!(
        TcpStream::connect(address).await.unwrap_err().kind(),
        std::io::ErrorKind::ConnectionRefused
    );
    let ready = ready_backend(format!("tcp://{address}"));
    let hello = client_hello(Some(sni_extension(b"late.example.com")));
    let tail = b"opaque TLS bytes after ClientHello";
    let mut expected = hello.clone();
    expected.extend_from_slice(tail);
    let adapter = FrontlineTlsAdapter::new(TlsCertificateStore::disabled()).with_resource_config(
        proxy_core::ProxyResourceConfig::default()
            .with_timeouts(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap(),
    );
    let (mut client, proxy_side) = io::duplex(4096);
    client.write_all(&hello).await.unwrap();
    client.write_all(tail).await.unwrap();
    client.shutdown().await.unwrap();
    let mut proxy = tokio::spawn(async move { passthrough(&adapter, &ready, proxy_side).await });

    let early = tokio::time::timeout(Duration::from_millis(100), &mut proxy).await;
    assert!(
        early.is_err(),
        "SNI setup must remain pending through a pre-dispatch refusal: {early:?}"
    );
    let listener = TcpListener::bind(address).await.unwrap();
    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut actual = Vec::new();
        socket.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        socket.write_all(b"single upstream response").await.unwrap();
        socket.shutdown().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    });
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, b"single upstream response");
    proxy.await.unwrap().unwrap();
    upstream.await.unwrap();
}

#[tokio::test]
async fn passthrough_rejects_bad_inputs_before_connecting() {
    assert!(matches!(
        passthrough_backend_addr(&ready_backend("http://127.0.0.1:1")),
        Err(TlsPassthroughBackendError::UnsupportedScheme(scheme)) if scheme == "http"
    ));
    assert!(matches!(
        passthrough_backend_addr(&ready_backend("tcp://127.0.0.1")),
        Err(TlsPassthroughBackendError::InvalidAuthority(_))
    ));

    let adapter = FrontlineTlsAdapter::new(TlsCertificateStore::disabled());
    let ready = ready_backend("tcp://127.0.0.1:1");

    let (mut client, proxy_side) = io::duplex(1024);
    client
        .write_all(b"GET / HTTP/1.1\r\n\r\n")
        .await
        .expect("writes non-tls");
    let error = passthrough(&adapter, &ready, proxy_side)
        .await
        .expect_err("non tls rejects");
    assert!(matches!(error, TlsPassthroughError::NotTls(_)));

    let (mut client, proxy_side) = io::duplex(1024);
    client
        .write_all(&client_hello(None))
        .await
        .expect("writes no-sni hello");
    let error = passthrough(&adapter, &ready, proxy_side)
        .await
        .expect_err("missing sni rejects");
    assert!(matches!(error, TlsPassthroughError::MissingSni));

    let (mut client, proxy_side) = io::duplex(1024);
    client
        .write_all(&client_hello(Some(sni_extension(b"localhost"))))
        .await
        .expect("writes invalid sni hello");
    let error = passthrough(&adapter, &ready, proxy_side)
        .await
        .expect_err("invalid sni rejects");
    assert!(matches!(error, TlsPassthroughError::InvalidSni(_)));
}

fn test_cert(host: &str) -> TestCert {
    let rcgen::CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec![host.to_owned()]).expect("cert generates");

    TestCert {
        cert: cert.der().clone(),
        key: PrivateKeyDer::from(signing_key),
    }
}

struct TestCert {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

fn client_config_trusting(cert: CertificateDer<'static>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(cert).expect("root cert inserts");
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

async fn raw_https_request(
    addr: std::net::SocketAddr,
    server_name: &'static str,
    config: ClientConfig,
) -> String {
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from(server_name)
        .expect("server name")
        .to_owned();
    let tcp = TcpStream::connect(addr).await.expect("client connects");
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .expect("tls client connects");

    let request = format!(
        "POST /secure?via=tls HTTP/1.1\r\nHost: app.example.com\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        REQUEST_BODY.len(),
        std::str::from_utf8(REQUEST_BODY).expect("request body is utf8")
    );
    tls.write_all(request.as_bytes())
        .await
        .expect("client writes request");

    let mut response = Vec::new();
    tls.read_to_end(&mut response)
        .await
        .expect("client reads response");
    String::from_utf8(response).expect("response is utf8")
}

fn ready_backend(uri: impl Into<String>) -> ReadyBackend {
    ReadyBackend {
        instance_id: InstanceId::new("instance-a").expect("instance id"),
        instance_generation: Generation::new(7),
        backend: BackendEndpoint::new(uri).expect("backend endpoint"),
        pod_backend: None,
        backend_generation: Some(BackendGeneration::new(11)),
    }
}

fn client_hello(extensions: Option<Vec<u8>>) -> Vec<u8> {
    record(client_hello_handshake(extensions))
}

fn client_hello_handshake(extensions: Option<Vec<u8>>) -> Vec<u8> {
    handshake(client_hello_body(extensions))
}

fn client_hello_body(extensions: Option<Vec<u8>>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0x11; 32]);
    body.push(0);
    push_u16(&mut body, 2);
    body.extend_from_slice(&[0x13, 0x01]);
    body.push(1);
    body.push(0);

    if let Some(extensions) = extensions {
        push_u16(&mut body, extensions.len());
        body.extend_from_slice(&extensions);
    }

    body
}

fn handshake(body: Vec<u8>) -> Vec<u8> {
    let mut handshake = Vec::new();
    handshake.push(0x01);
    push_u24(&mut handshake, body.len());
    handshake.extend_from_slice(&body);
    handshake
}

fn record(payload: Vec<u8>) -> Vec<u8> {
    let mut record = Vec::new();
    record.extend_from_slice(&[0x16, 0x03, 0x03]);
    push_u16(&mut record, payload.len());
    record.extend_from_slice(&payload);
    record
}

fn sni_extension(hostname: &[u8]) -> Vec<u8> {
    let mut name = Vec::new();
    name.push(0);
    push_u16(&mut name, hostname.len());
    name.extend_from_slice(hostname);

    let mut extension_data = Vec::new();
    push_u16(&mut extension_data, name.len());
    extension_data.extend_from_slice(&name);

    extension(0, extension_data)
}

fn extension(extension_type: u16, data: Vec<u8>) -> Vec<u8> {
    let mut extension = Vec::new();
    push_u16(&mut extension, extension_type as usize);
    push_u16(&mut extension, data.len());
    extension.extend_from_slice(&data);
    extension
}

fn push_u16(bytes: &mut Vec<u8>, value: usize) {
    let value = u16::try_from(value).expect("test value fits in u16");
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u24(bytes: &mut Vec<u8>, value: usize) {
    assert!(value <= 0x00ff_ffff, "test value fits in u24");
    bytes.extend_from_slice(&[
        ((value >> 16) & 0xff) as u8,
        ((value >> 8) & 0xff) as u8,
        (value & 0xff) as u8,
    ]);
}

#[tokio::test]
async fn passthrough_setup_timeout_and_cancellation_do_not_redial() {
    use std::time::Duration;
    for cancel in [false, true] {
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let ready = ready_backend(format!("tcp://{address}"));
        let adapter = FrontlineTlsAdapter::new(TlsCertificateStore::disabled())
            .with_resource_config(
                proxy_core::ProxyResourceConfig::default()
                    .with_timeouts(
                        Duration::from_millis(120),
                        Duration::from_secs(1),
                        Duration::from_secs(1),
                    )
                    .unwrap(),
            );
        let (mut client, proxy_side) = io::duplex(4096);
        client
            .write_all(&client_hello(Some(sni_extension(b"late.example.com"))))
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        let task = tokio::spawn(async move { passthrough(&adapter, &ready, proxy_side).await });
        if cancel {
            tokio::time::sleep(Duration::from_millis(30)).await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            let error = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert!(
                matches!(error, TlsPassthroughError::Connect(error) if error.kind() == std::io::ErrorKind::TimedOut)
            );
            assert!(started.elapsed() >= Duration::from_millis(120));
        }
        assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
        let listener = TcpListener::bind(address).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn passthrough_does_not_replay_after_clienthello_dispatch() {
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let ready = ready_backend(format!("tcp://{address}"));
    let hello = client_hello(Some(sni_extension(b"once.example.com")));
    let expected = hello.clone();
    let (mut client, proxy_side) = io::duplex(4096);
    client.write_all(&hello).await.unwrap();
    let task = tokio::spawn(async move {
        let adapter = FrontlineTlsAdapter::new(TlsCertificateStore::disabled());
        passthrough(&adapter, &ready, proxy_side).await
    });
    let (mut upstream, _) = listener.accept().await.unwrap();
    let mut actual = vec![0; expected.len()];
    upstream.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, expected);
    // The upstream ends after receiving TLS bytes; setup recovery must be over.
    drop(upstream);
    client.shutdown().await.unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(response.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err()
    );
}

/// The two steps the passthrough listener runs for one connection.
async fn passthrough<Client>(
    adapter: &FrontlineTlsAdapter,
    ready: &ReadyBackend,
    mut client: Client,
) -> Result<RouteRequestIdentity, TlsPassthroughError>
where
    Client: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let client_hello = adapter.read_passthrough_client_hello(&mut client).await?;
    let identity = client_hello.identity().clone();
    adapter
        .passthrough_prefixed(ready, client, client_hello)
        .await?;
    Ok(identity)
}
