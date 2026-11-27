//! The protocol an app port serves is independent of the protocol its client
//! spoke, and the app receives the client's authority either way.
mod support;

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode, Uri, Version};
use http_body_util::{BodyExt, Empty, Full};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use proxy_core::{
    apply_forwarded_header_policy, AppHttpProxy, AppHttpProxyError, AppProtocol, DrainTracker,
    HttpProxy, HttpProxyError, ProxyResourceConfig, Shutdown,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// What an app observed of one request.
#[derive(Clone, Debug)]
struct Seen {
    version: Version,
    authority: Option<String>,
    headers: HeaderMap,
}

type Recorded = Arc<Mutex<Vec<Seen>>>;

async fn record(
    seen: Recorded,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    seen.lock().unwrap().push(Seen {
        version: request.version(),
        authority: request.uri().authority().map(|a| a.to_string()),
        headers: request.headers().clone(),
    });
    Ok(Response::new(Full::new(Bytes::from_static(b"served"))))
}

/// An app that serves HTTP/1.1 and nothing else.
async fn spawn_http1_app() -> (SocketAddr, Recorded) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Recorded::default();
    let recorded = seen.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            tokio::spawn(hyper::server::conn::http1::Builder::new().serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| record(seen.clone(), request)),
            ));
        }
    });
    (addr, recorded)
}

/// An app that serves prior-knowledge HTTP/2 and nothing else.
async fn spawn_h2c_app() -> (SocketAddr, Recorded) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Recorded::default();
    let recorded = seen.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            tokio::spawn(
                hyper::server::conn::http2::Builder::new(TokioExecutor::new()).serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |request| record(seen.clone(), request)),
                ),
            );
        }
    });
    (addr, recorded)
}

fn app_proxy(app: SocketAddr, protocol: AppProtocol) -> AppHttpProxy {
    AppHttpProxy::new(
        DrainTracker::new(Duration::from_secs(5)),
        ProxyResourceConfig::default(),
        app,
        protocol,
    )
}

/// A request as an HTTP/2 client sends it: the authority is in the target.
fn http2_request(method: Method) -> Request<Empty<Bytes>> {
    let mut request = Request::new(Empty::new());
    *request.method_mut() = method;
    *request.version_mut() = Version::HTTP_2;
    *request.uri_mut() = Uri::from_static("http://app.example.test/hello?name=value");
    request
}

/// A request as an HTTP/1.1 client sends it: the authority is in `Host`.
fn http1_request() -> Request<Empty<Bytes>> {
    let mut request = Request::new(Empty::new());
    *request.uri_mut() = Uri::from_static("/hello?name=value");
    request
        .headers_mut()
        .insert("host", "app.example.test".parse().unwrap());
    request
}

async fn body_of(
    response: Response<impl http_body::Body<Data = Bytes, Error: std::fmt::Debug>>,
) -> Bytes {
    response.into_body().collect().await.unwrap().to_bytes()
}

#[tokio::test]
async fn http2_request_reaches_an_http1_only_app_with_the_client_host() {
    let (app, seen) = spawn_http1_app().await;
    let proxy = app_proxy(app, AppProtocol::Http1);

    let mut request = http2_request(Method::GET);
    request
        .headers_mut()
        .insert("te", "trailers".parse().unwrap());
    let response = proxy.proxy(request).await.expect("the app answers");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_of(response).await, "served");
    let seen = seen.lock().unwrap()[0].clone();
    assert_eq!(seen.version, Version::HTTP_11);
    assert_eq!(seen.headers["host"], "app.example.test");
    assert!(
        !seen.headers.contains_key("te"),
        "TE ends at an HTTP/1.1 hop"
    );
}

#[tokio::test]
async fn http2_client_reaches_an_http1_only_app_through_the_served_connection() {
    let (app, seen) = spawn_http1_app().await;
    let proxy = app_proxy(app, AppProtocol::Http1);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        support::serve_http_connection(stream, Shutdown::new(), move |request| {
            let proxy = proxy.clone();
            async move { proxy.proxy(request).await }
        })
        .await;
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(connection);
    let response = timeout(
        TEST_TIMEOUT,
        sender.send_request(http2_request(Method::POST)),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(body_of(response).await, "served");
    let seen = seen.lock().unwrap()[0].clone();
    assert_eq!(seen.version, Version::HTTP_11);
    assert_eq!(seen.headers["host"], "app.example.test");
}

#[tokio::test]
async fn an_http1_app_receives_the_edge_host_when_the_hop_named_its_own_authority() {
    let (app, seen) = spawn_http1_app().await;
    let proxy = app_proxy(app, AppProtocol::Http1);

    // Frontline addresses the sidecar by its backend name and carries the
    // client's authority in X-Forwarded-Host.
    let mut request = http2_request(Method::GET);
    *request.uri_mut() = Uri::from_static("http://svc.apps.svc.cluster.local:80/hello");
    request
        .headers_mut()
        .insert("x-forwarded-host", "app.example.test".parse().unwrap());
    proxy.proxy(request).await.expect("the app answers");

    let seen = seen.lock().unwrap()[0].clone();
    assert_eq!(seen.headers["host"], "app.example.test");
    assert_eq!(seen.headers["x-forwarded-host"], "app.example.test");
}

#[tokio::test]
async fn h2c_app_receives_te_trailers_and_the_client_authority_from_either_client_version() {
    let (app, seen) = spawn_h2c_app().await;
    let proxy = app_proxy(app, AppProtocol::H2c);

    let mut from_http2 = http2_request(Method::POST);
    from_http2
        .headers_mut()
        .insert("te", "trailers".parse().unwrap());
    from_http2
        .headers_mut()
        .insert("content-type", "application/grpc".parse().unwrap());
    let mut from_http1 = http1_request();
    from_http1
        .headers_mut()
        .insert("te", "trailers, deflate".parse().unwrap());
    from_http1
        .headers_mut()
        .insert("connection", "te".parse().unwrap());
    for request in [from_http2, from_http1] {
        let response = proxy.proxy(request).await.expect("the app answers");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_of(response).await, "served");
    }

    for seen in seen.lock().unwrap().iter() {
        assert_eq!(seen.version, Version::HTTP_2);
        assert_eq!(seen.authority.as_deref(), Some("app.example.test"));
        assert_eq!(seen.headers["te"], "trailers");
        assert!(!seen.headers.contains_key("host"));
    }
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn grpc_to_an_http1_app_port_is_refused_before_it_reaches_the_app() {
    let (app, seen) = spawn_http1_app().await;
    let proxy = app_proxy(app, AppProtocol::Http1);

    for content_type in ["application/grpc", "application/grpc+proto"] {
        let mut request = http2_request(Method::POST);
        request
            .headers_mut()
            .insert("content-type", content_type.parse().unwrap());
        assert!(matches!(
            proxy.proxy(request).await,
            Err(AppHttpProxyError::GrpcRequiresH2c)
        ));
    }
    let mut grpc_web = http1_request();
    grpc_web.headers_mut().insert(
        "content-type",
        "application/grpc-web+proto".parse().unwrap(),
    );
    proxy.proxy(grpc_web).await.expect("gRPC-Web is HTTP/1.1");

    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn forwarded_host_comes_from_the_request_target_when_host_is_absent() {
    let peer = "203.0.113.10".parse().unwrap();

    let mut http2 = http2_request(Method::GET);
    http2
        .headers_mut()
        .insert("x-forwarded-host", "spoof.example.test".parse().unwrap());
    http2
        .headers_mut()
        .insert("te", "trailers".parse().unwrap());
    apply_forwarded_header_policy(&mut http2, peer, "https");
    assert_eq!(http2.headers()["x-forwarded-host"], "app.example.test");
    assert_eq!(http2.headers()["te"], "trailers");

    let mut http1 = http1_request();
    apply_forwarded_header_policy(&mut http1, peer, "http");
    assert_eq!(http1.headers()["x-forwarded-host"], "app.example.test");
}

#[tokio::test]
async fn sidecar_hop_keeps_the_client_version_and_carries_te_trailers_on_http2() {
    let (h2c, h2c_seen) = spawn_h2c_app().await;
    let (http1, http1_seen) = spawn_http1_app().await;
    let proxy = HttpProxy::new(DrainTracker::new(Duration::from_secs(5)));

    let mut request = http2_request(Method::GET);
    request
        .headers_mut()
        .insert("te", "trailers".parse().unwrap());
    let origin = format!("http://{h2c}").parse().unwrap();
    proxy.proxy(request, &origin).await.expect("HTTP/2 hop");
    let mut request = http1_request();
    request
        .headers_mut()
        .insert("te", "trailers".parse().unwrap());
    let origin = format!("http://{http1}").parse().unwrap();
    proxy.proxy(request, &origin).await.expect("HTTP/1.1 hop");

    let seen = h2c_seen.lock().unwrap()[0].clone();
    assert_eq!(seen.version, Version::HTTP_2);
    assert_eq!(seen.headers["te"], "trailers");
    let seen = http1_seen.lock().unwrap()[0].clone();
    assert_eq!(seen.version, Version::HTTP_11);
    assert_eq!(seen.headers["host"], "app.example.test");
    assert!(!seen.headers.contains_key("te"));
}

/// A raw HTTP/2 peer: it completes the connection preface, then reports each
/// frame type it receives and never answers a request or a ping.
async fn spawn_silent_http2_peer() -> (
    SocketAddr,
    tokio::sync::mpsc::UnboundedReceiver<u8>,
    Arc<AtomicUsize>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (frames, received) = tokio::sync::mpsc::unbounded_channel();
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            accepted.fetch_add(1, Ordering::SeqCst);
            let frames = frames.clone();
            tokio::spawn(async move {
                let mut preface = [0_u8; 24];
                if stream.read_exact(&mut preface).await.is_err() {
                    return;
                }
                // An empty SETTINGS frame, then the acknowledgement of the peer's.
                let _ = stream
                    .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0])
                    .await;
                loop {
                    let mut header = [0_u8; 9];
                    if stream.read_exact(&mut header).await.is_err() {
                        let _ = frames.send(u8::MAX);
                        return;
                    }
                    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                    let mut payload = vec![0_u8; length];
                    if stream.read_exact(&mut payload).await.is_err() {
                        return;
                    }
                    let _ = frames.send(header[3]);
                }
            });
        }
    });
    (addr, received, connections)
}

const HEADERS_FRAME: u8 = 1;
const PING_FRAME: u8 = 6;
const CONNECTION_CLOSED: u8 = u8::MAX;

async fn next_frame_of(frames: &mut tokio::sync::mpsc::UnboundedReceiver<u8>, wanted: &[u8]) -> u8 {
    loop {
        let frame = frames.recv().await.expect("peer keeps reporting");
        if wanted.contains(&frame) {
            return frame;
        }
    }
}

fn short_timeouts() -> ProxyResourceConfig {
    ProxyResourceConfig::default()
        .with_timeouts(
            Duration::from_secs(2),
            Duration::from_secs(300),
            Duration::from_secs(60),
        )
        .unwrap()
}

#[tokio::test]
async fn sidecar_hop_pings_a_silent_http2_connection_and_closes_it_unanswered() {
    let (peer, mut frames, _) = spawn_silent_http2_peer().await;
    let proxy = HttpProxy::with_config(DrainTracker::new(Duration::from_secs(5)), short_timeouts());
    let origin: Uri = format!("http://{peer}").parse().unwrap();
    let request = tokio::spawn(async move {
        proxy
            .proxy(http2_request(Method::GET), &origin)
            .await
            .map(|_| ())
    });
    assert_eq!(
        next_frame_of(&mut frames, &[HEADERS_FRAME]).await,
        HEADERS_FRAME
    );

    // The connection exists; from here only timers decide what happens.
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    assert_eq!(next_frame_of(&mut frames, &[PING_FRAME]).await, PING_FRAME);
    assert!(
        started.elapsed() >= Duration::from_secs(29),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        next_frame_of(&mut frames, &[CONNECTION_CLOSED]).await,
        CONNECTION_CLOSED
    );
    // One interval, then the acknowledgement timeout (the setup timeout).
    assert!(
        started.elapsed() <= Duration::from_secs(33),
        "{:?}",
        started.elapsed()
    );
    assert!(matches!(
        request.await.unwrap(),
        Err(HttpProxyError::Client(_))
    ));
}

#[tokio::test]
async fn app_hop_sends_no_http2_pings() {
    let (peer, mut frames, _) = spawn_silent_http2_peer().await;
    let proxy = AppHttpProxy::new(
        DrainTracker::new(Duration::from_secs(5)),
        short_timeouts(),
        peer,
        AppProtocol::H2c,
    );
    let request =
        tokio::spawn(async move { proxy.proxy(http2_request(Method::GET)).await.map(|_| ()) });
    assert_eq!(
        next_frame_of(&mut frames, &[HEADERS_FRAME]).await,
        HEADERS_FRAME
    );

    tokio::time::pause();
    let quiet = timeout(
        Duration::from_secs(240),
        next_frame_of(&mut frames, &[PING_FRAME, CONNECTION_CLOSED]),
    )
    .await;
    assert!(quiet.is_err(), "the app hop stays silent: {quiet:?}");
    request.abort();
}

#[tokio::test]
async fn upstream_header_timeout_keeps_later_requests_off_the_silent_connection() {
    let (peer, mut frames, connections) = spawn_silent_http2_peer().await;
    let config = ProxyResourceConfig::default()
        .with_timeouts(
            Duration::from_secs(10),
            Duration::from_millis(200),
            Duration::from_secs(60),
        )
        .unwrap();
    let proxy = HttpProxy::with_config(DrainTracker::new(Duration::from_secs(5)), config);
    let origin: Uri = format!("http://{peer}").parse().unwrap();

    for expected_connections in [1, 2] {
        let result = proxy.proxy(http2_request(Method::GET), &origin).await;
        assert!(matches!(result, Err(HttpProxyError::UpstreamHeaderTimeout)));
        assert_eq!(
            next_frame_of(&mut frames, &[HEADERS_FRAME]).await,
            HEADERS_FRAME
        );
        assert_eq!(connections.load(Ordering::SeqCst), expected_connections);
    }
}
