#[path = "frontline_load_smoke/certificates.rs"]
mod certificates;
use std::{
    convert::Infallible,
    env,
    error::Error,
    fmt, fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{
    header::{CONTENT_TYPE, HOST},
    HeaderMap, HeaderValue, Request as HttpRequest, Response as HttpResponse, StatusCode, Uri,
    Version,
};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::{body::Incoming, client::conn::http2 as client_http2, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use proxy_core::websocket_upgrade_response;
use rcgen::generate_simple_self_signed;
use sleepypods_api::{
    pb::{
        self,
        proxy_control_plane_server::{ProxyControlPlane, ProxyControlPlaneServer},
    },
    RouteHost,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::{
    rustls::{
        pki_types::{CertificateDer, ServerName},
        ClientConfig as RustlsClientConfig, RootCertStore,
    },
    TlsConnector,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, protocol::Role, Bytes as WsBytes, Message},
    WebSocketStream,
};
use tonic::{
    codegen::tokio_stream::wrappers::ReceiverStream,
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};

type BoxError = Box<dyn Error + Send + Sync>;
type BackendBody = BoxBody<Bytes, BoxError>;

const BACKEND_BODY: &[u8] = b"frontline-load-smoke-ok\n";
const GRPC_BODY: &[u8] = b"\0\0\0\0\x05hello";
const WEBSOCKET_CLIENT_TEXT: &str = "frontline websocket text";
const WEBSOCKET_BACKEND_TEXT: &str = "backend websocket text";
const WEBSOCKET_CLIENT_BINARY: &[u8] = b"frontline websocket bytes";
const WEBSOCKET_BACKEND_BINARY: &[u8] = b"backend websocket bytes";
const DEFAULT_WEBSOCKET_STREAM_BYTES: u64 = 262_144;
const DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE: u64 = 16_384;
const DEFAULT_ROUTE_HOST: &str = "app.example.test";
const DEFAULT_ROUTE_PATH: &str = "/smoke";
const DEFAULT_COLD_ROUTE_PATH: &str = "/cold-smoke";
const DEFAULT_GRPC_BACKEND_ADDR: &str = "0.0.0.0:18082";
const REAL_GRPC_PATH: &str = "/sleepypods.controlplane.v1.ProxyControlPlane/WakeInstance";
const STATS_PATH: &str = "/__sleepypods_load_smoke_stats";
const ROUTE_CACHE_TTL_MILLIS: u64 = 600_000;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const SUBSCRIBE_RESPONSE_BUFFER: usize = 16;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), BoxError> {
    let mut args = env::args();
    let _program = args.next();

    match args.next().as_deref() {
        None | Some("server") => run_server(ServerConfig::from_env()?).await,
        Some("client") => run_client(ClientConfig::from_args(args)?).await,
        Some("cert") => write_self_signed_cert(CertConfig::from_args(args)?),
        Some(command) => Err(Box::new(InvalidArgs(format!(
            "unknown command {command:?}; expected server, client, or cert"
        )))),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ServerConfig {
    backend_addr: SocketAddr,
    grpc_backend_addr: SocketAddr,
    control_plane_addr: SocketAddr,
    backend_uri: String,
    grpc_backend_uri: String,
    route_host: String,
    route_path: String,
    cold_route_path: String,
    websocket_stream_bytes: u64,
    websocket_stream_chunk_size: u64,
}

impl ServerConfig {
    fn from_env() -> Result<Self, InvalidArgs> {
        let backend_addr =
            socket_addr_from_env("SLEEPYPODS_LOAD_SMOKE_BACKEND_ADDR", "0.0.0.0:18080")?;
        let grpc_backend_addr = socket_addr_from_env(
            "SLEEPYPODS_LOAD_SMOKE_GRPC_BACKEND_ADDR",
            DEFAULT_GRPC_BACKEND_ADDR,
        )?;
        let control_plane_addr =
            socket_addr_from_env("SLEEPYPODS_LOAD_SMOKE_CONTROL_PLANE_ADDR", "0.0.0.0:19090")?;
        let backend_uri = env::var("SLEEPYPODS_LOAD_SMOKE_BACKEND_URI")
            .unwrap_or_else(|_| format!("http://127.0.0.1:{}", backend_addr.port()));
        let grpc_backend_uri = env::var("SLEEPYPODS_LOAD_SMOKE_GRPC_BACKEND_URI")
            .unwrap_or_else(|_| format!("http://127.0.0.1:{}", grpc_backend_addr.port()));
        let route_host = env::var("SLEEPYPODS_LOAD_SMOKE_ROUTE_HOST")
            .unwrap_or_else(|_| DEFAULT_ROUTE_HOST.to_owned());
        let route_path = env::var("SLEEPYPODS_LOAD_SMOKE_ROUTE_PATH")
            .unwrap_or_else(|_| DEFAULT_ROUTE_PATH.to_owned());
        let cold_route_path = env::var("SLEEPYPODS_LOAD_SMOKE_COLD_ROUTE_PATH")
            .unwrap_or_else(|_| DEFAULT_COLD_ROUTE_PATH.to_owned());
        let websocket_stream_bytes = parse_positive_u64_env(
            "SLEEPYPODS_LOAD_SMOKE_WEBSOCKET_STREAM_BYTES",
            DEFAULT_WEBSOCKET_STREAM_BYTES,
        )?;
        let websocket_stream_chunk_size = parse_positive_u64_env(
            "SLEEPYPODS_LOAD_SMOKE_WEBSOCKET_STREAM_CHUNK_SIZE",
            DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE,
        )?;

        let route_host = canonical_route_host("SLEEPYPODS_LOAD_SMOKE_ROUTE_HOST", &route_host)?;
        validate_route_path("SLEEPYPODS_LOAD_SMOKE_ROUTE_PATH", &route_path)?;
        validate_route_path("SLEEPYPODS_LOAD_SMOKE_COLD_ROUTE_PATH", &cold_route_path)?;

        Ok(Self {
            backend_addr,
            grpc_backend_addr,
            control_plane_addr,
            backend_uri,
            grpc_backend_uri,
            route_host,
            route_path,
            cold_route_path,
            websocket_stream_bytes,
            websocket_stream_chunk_size,
        })
    }
}

fn socket_addr_from_env(
    name: &'static str,
    default: &'static str,
) -> Result<SocketAddr, InvalidArgs> {
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .map_err(|error| InvalidArgs(format!("{name} must be a socket address: {error}")))
}

fn parse_positive_u64_env(name: &'static str, default: u64) -> Result<u64, InvalidArgs> {
    match env::var(name) {
        Ok(value) => parse_positive_u64(name, &value),
        Err(_) => Ok(default),
    }
}

async fn run_server(config: ServerConfig) -> Result<(), BoxError> {
    let backend_listener = TcpListener::bind(config.backend_addr).await?;
    let backend_addr = backend_listener.local_addr()?;
    let grpc_backend_addr = config.grpc_backend_addr;
    let stats = SmokeStats::default();
    eprintln!("load-smoke backend listening on {backend_addr}");
    eprintln!("load-smoke generated gRPC backend listening on {grpc_backend_addr}");
    eprintln!(
        "load-smoke frontline control plane listening on {}",
        config.control_plane_addr
    );
    eprintln!(
        "load-smoke route host={} path={} cold_path={} backend={} grpc_backend={}",
        config.route_host,
        config.route_path,
        config.cold_route_path,
        config.backend_uri,
        config.grpc_backend_uri
    );
    eprintln!(
        "load-smoke websocket stream bytes={} chunk_size={}",
        config.websocket_stream_bytes, config.websocket_stream_chunk_size
    );

    let backend = serve_backend(
        backend_listener,
        stats.clone(),
        config.websocket_stream_bytes,
        config.websocket_stream_chunk_size,
    );
    let grpc_backend = Server::builder()
        .add_service(ProxyControlPlaneServer::new(GeneratedGrpcBackend::new(
            stats.clone(),
        )))
        .serve(grpc_backend_addr);
    let delivery = certificates::Delivery::from_env(&config.route_host)?;
    let mut service = FakeProxyControlPlane::new(
        config.route_host,
        config.route_path,
        config.cold_route_path,
        config.backend_uri,
        config.grpc_backend_uri,
        stats,
    );
    service.certificates = Some(Arc::new(delivery));
    let control_plane = Server::builder()
        .tls_config(certificates::tls()?)?
        .add_service(ProxyControlPlaneServer::with_interceptor(
            service,
            certificates::auth()?,
        ))
        .serve(config.control_plane_addr);

    tokio::select! {
        result = backend => result,
        result = grpc_backend => result.map_err(|error| Box::new(error) as BoxError),
        result = control_plane => result.map_err(|error| Box::new(error) as BoxError),
    }
}

async fn serve_backend(
    listener: TcpListener,
    stats: SmokeStats,
    websocket_stream_bytes: u64,
    websocket_stream_chunk_size: u64,
) -> Result<(), BoxError> {
    loop {
        let (stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let stats = stats.clone();

        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let stats = stats.clone();

                async move {
                    Ok::<_, Infallible>(
                        backend_response(
                            request,
                            &stats,
                            websocket_stream_bytes,
                            websocket_stream_chunk_size,
                        )
                        .await,
                    )
                }
            });

            if let Err(error) = auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await
            {
                eprintln!("load-smoke backend connection failed: {error}");
            }
        });
    }
}

async fn backend_response<B>(
    request: HttpRequest<B>,
    stats: &SmokeStats,
    websocket_stream_bytes: u64,
    websocket_stream_chunk_size: u64,
) -> HttpResponse<BackendBody>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: fmt::Display,
{
    if is_websocket_upgrade_candidate(&request) {
        return websocket_backend_response(
            request,
            stats,
            websocket_stream_bytes,
            websocket_stream_chunk_size,
        );
    }

    if request.uri().path() == STATS_PATH {
        return stats_response(stats);
    }

    stats.record_backend_http_request();

    if request.headers().get(CONTENT_TYPE).is_some_and(|value| {
        value
            .to_str()
            .is_ok_and(|value| value.eq_ignore_ascii_case("application/grpc"))
    }) {
        return grpc_response(request).await;
    }

    HttpResponse::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain")
        .header("content-length", BACKEND_BODY.len().to_string())
        .header("cache-control", "no-store")
        .body(boxed_body(Bytes::from_static(BACKEND_BODY)))
        .expect("fixed load-smoke response builds")
}

async fn grpc_response<B>(request: HttpRequest<B>) -> HttpResponse<BackendBody>
where
    B: http_body::Body<Data = Bytes>,
    B::Error: fmt::Display,
{
    let collected = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            return bad_request_response(format!(
                "failed to read gRPC-shaped request body: {error}"
            ));
        }
    };

    if collected.as_ref() != GRPC_BODY {
        return bad_request_response(format!(
            "unexpected gRPC-shaped request body length {}",
            collected.len()
        ));
    }

    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    trailers.insert("grpc-message", HeaderValue::from_static("ok"));

    HttpResponse::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("cache-control", "no-store")
        .body(
            Full::new(Bytes::from_static(GRPC_BODY))
                .map_err(|never| match never {})
                .with_trailers(std::future::ready(Some(Ok::<_, BoxError>(trailers))))
                .boxed(),
        )
        .expect("fixed load-smoke gRPC-shaped response builds")
}

fn bad_request_response(message: String) -> HttpResponse<BackendBody> {
    HttpResponse::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("content-type", "text/plain")
        .header("cache-control", "no-store")
        .body(boxed_body(Bytes::from(message)))
        .expect("fixed load-smoke bad request response builds")
}

fn websocket_backend_response<B>(
    mut request: HttpRequest<B>,
    stats: &SmokeStats,
    websocket_stream_bytes: u64,
    websocket_stream_chunk_size: u64,
) -> HttpResponse<BackendBody>
where
    B: Send + 'static,
{
    let response = match websocket_upgrade_response(&request, boxed_body(Bytes::new())) {
        Ok(response) => response,
        Err(_error) => return bad_request_response("invalid WebSocket upgrade".to_owned()),
    };
    let upgraded = hyper::upgrade::on(&mut request);
    let stats = stats.clone();

    tokio::spawn(async move {
        let Ok(upgraded) = upgraded.await else {
            return;
        };
        stats.record_backend_websocket_session();
        if let Err(error) = serve_backend_websocket(
            WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None).await,
            websocket_stream_bytes,
            websocket_stream_chunk_size,
        )
        .await
        {
            eprintln!("load-smoke backend websocket failed: {error}");
        }
    });

    response
}

async fn serve_backend_websocket<S>(
    mut websocket: WebSocketStream<S>,
    stream_bytes: u64,
    chunk_size: u64,
) -> Result<(), BoxError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let text = timeout(REQUEST_TIMEOUT, websocket.next())
        .await
        .map_err(|_| timeout_error("websocket read text"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing text frame"))??;
    if text != Message::Text(WEBSOCKET_CLIENT_TEXT.into()) {
        return Err(Box::new(
            ResponseValidationError::UnexpectedWebSocketMessage(format!("{text:?}")),
        ));
    }

    timeout(
        REQUEST_TIMEOUT,
        websocket.send(Message::Text(WEBSOCKET_BACKEND_TEXT.into())),
    )
    .await
    .map_err(|_| timeout_error("websocket write text"))??;

    let binary = timeout(REQUEST_TIMEOUT, websocket.next())
        .await
        .map_err(|_| timeout_error("websocket read binary"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing binary frame"))??;
    if binary != Message::Binary(WsBytes::from_static(WEBSOCKET_CLIENT_BINARY)) {
        return Err(Box::new(
            ResponseValidationError::UnexpectedWebSocketMessage(format!("{binary:?}")),
        ));
    }

    timeout(
        REQUEST_TIMEOUT,
        websocket.send(Message::Binary(WsBytes::from_static(
            WEBSOCKET_BACKEND_BINARY,
        ))),
    )
    .await
    .map_err(|_| timeout_error("websocket write binary"))??;

    read_and_echo_websocket_stream(&mut websocket, stream_bytes, chunk_size).await?;

    timeout(REQUEST_TIMEOUT, websocket.close(None))
        .await
        .map_err(|_| timeout_error("websocket close"))??;

    Ok(())
}

async fn read_and_echo_websocket_stream<S>(
    websocket: &mut WebSocketStream<S>,
    stream_bytes: u64,
    chunk_size: u64,
) -> Result<(), BoxError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut received = 0;

    while received < stream_bytes {
        let remaining = stream_bytes - received;
        let expected_len = remaining.min(chunk_size) as usize;
        let message = timeout(REQUEST_TIMEOUT, websocket.next())
            .await
            .map_err(|_| timeout_error("websocket read stream chunk"))?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "missing stream chunk")
            })??;
        let Message::Binary(bytes) = message else {
            return Err(Box::new(
                ResponseValidationError::UnexpectedWebSocketMessage(format!("{message:?}")),
            ));
        };

        if bytes.len() != expected_len {
            return Err(Box::new(ResponseValidationError::UnexpectedStreamChunk {
                offset: received,
                len: bytes.len(),
                expected_len,
            }));
        }
        validate_stream_chunk(&bytes, received)?;
        received += bytes.len() as u64;

        timeout(REQUEST_TIMEOUT, websocket.send(Message::Binary(bytes)))
            .await
            .map_err(|_| timeout_error("websocket write stream chunk"))??;
    }

    Ok(())
}

fn make_stream_chunk(offset: u64, len: usize) -> Vec<u8> {
    let mut chunk = (0..len)
        .map(|index| deterministic_stream_byte(offset, index as u64))
        .collect::<Vec<_>>();

    for (index, byte) in offset.to_le_bytes().iter().copied().enumerate().take(len) {
        chunk[index] = byte;
    }

    chunk
}

fn validate_stream_chunk(bytes: &[u8], offset: u64) -> Result<(), ResponseValidationError> {
    let expected = make_stream_chunk(offset, bytes.len());
    for (index, (actual, expected)) in bytes
        .iter()
        .copied()
        .zip(expected.iter().copied())
        .enumerate()
    {
        if actual != expected {
            return Err(ResponseValidationError::UnexpectedStreamByte {
                offset: offset + index as u64,
                expected,
                actual,
            });
        }
    }

    Ok(())
}

fn deterministic_stream_byte(chunk_offset: u64, index: u64) -> u8 {
    let mut value = chunk_offset
        .wrapping_add(index)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) as u8
}

fn is_websocket_upgrade_candidate<B>(request: &HttpRequest<B>) -> bool {
    request
        .headers()
        .get("connection")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        })
        && request
            .headers()
            .get("upgrade")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

fn stats_response(stats: &SmokeStats) -> HttpResponse<BackendBody> {
    let snapshot = stats.snapshot();
    let body = format!(
        "subscribe_route_calls={} wake_instance_calls={} backend_http_requests={} backend_websocket_sessions={} resolve_certificate_calls={}\n",
        snapshot.subscribe_route_calls,
        snapshot.wake_instance_calls,
        snapshot.backend_http_requests,
        snapshot.backend_websocket_sessions,
        stats.resolve_certificate_calls.load(Ordering::Relaxed)
    );

    HttpResponse::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain")
        .header("content-length", body.len().to_string())
        .header("cache-control", "no-store")
        .body(boxed_body(Bytes::from(body)))
        .expect("fixed load-smoke stats response builds")
}

fn boxed_body(bytes: Bytes) -> BackendBody {
    Full::new(bytes).map_err(|never| match never {}).boxed()
}

#[derive(Clone, Debug, Default)]
struct SmokeStats {
    resolve_certificate_calls: Arc<AtomicU64>,
    subscribe_route_calls: Arc<AtomicU64>,
    wake_instance_calls: Arc<AtomicU64>,
    backend_http_requests: Arc<AtomicU64>,
    backend_websocket_sessions: Arc<AtomicU64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SmokeStatsSnapshot {
    subscribe_route_calls: u64,
    wake_instance_calls: u64,
    backend_http_requests: u64,
    backend_websocket_sessions: u64,
}

impl SmokeStats {
    fn record_subscribe_route(&self) {
        self.subscribe_route_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_wake_instance(&self) {
        self.wake_instance_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_backend_http_request(&self) {
        self.backend_http_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_backend_websocket_session(&self) {
        self.backend_websocket_sessions
            .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> SmokeStatsSnapshot {
        SmokeStatsSnapshot {
            subscribe_route_calls: self.subscribe_route_calls.load(Ordering::Relaxed),
            wake_instance_calls: self.wake_instance_calls.load(Ordering::Relaxed),
            backend_http_requests: self.backend_http_requests.load(Ordering::Relaxed),
            backend_websocket_sessions: self.backend_websocket_sessions.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
struct FakeProxyControlPlane {
    route_host: Arc<str>,
    route_path: Arc<str>,
    cold_route_path: Arc<str>,
    backend_uri: Arc<str>,
    grpc_backend_uri: Arc<str>,
    stats: SmokeStats,
    certificates: Option<Arc<certificates::Delivery>>,
}

impl FakeProxyControlPlane {
    fn new(
        route_host: String,
        route_path: String,
        cold_route_path: String,
        backend_uri: String,
        grpc_backend_uri: String,
        stats: SmokeStats,
    ) -> Self {
        Self {
            route_host: route_host.into(),
            route_path: route_path.into(),
            cold_route_path: cold_route_path.into(),
            backend_uri: backend_uri.into(),
            grpc_backend_uri: grpc_backend_uri.into(),
            stats,
            certificates: None,
        }
    }

    fn handle_subscribe_request(
        &self,
        request: pb::ProxySubscribeRequest,
    ) -> Result<Option<pb::ProxySubscribeResponse>, Status> {
        match request
            .input
            .ok_or_else(|| Status::invalid_argument("subscribe request input is required"))?
        {
            pb::proxy_subscribe_request::Input::SubscribeRoute(request) => {
                self.stats.record_subscribe_route();
                Ok(Some(self.subscribe_route_response(request)?))
            }
            pb::proxy_subscribe_request::Input::Unsubscribe(_) => Ok(None),
        }
    }

    fn subscribe_route_response(
        &self,
        request: pb::ProxySubscribeRouteRequest,
    ) -> Result<pb::ProxySubscribeResponse, Status> {
        let request_id = non_empty(request.request_id, "request_id")?;
        let request_identity = request
            .identity
            .ok_or_else(|| Status::invalid_argument("identity is required"))?;

        if self.cold_route_matches(&request_identity) {
            Ok(self.resolved_response(
                request_id,
                self.cold_matched_identity(),
                self.cold_route_entry(),
            ))
        } else if self.real_grpc_route_matches(&request_identity) {
            Ok(self.resolved_response(request_id, request_identity, self.real_grpc_route_entry()))
        } else if self.route_matches(&request_identity) {
            Ok(pb::ProxySubscribeResponse {
                output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
                    pb::ProxyRouteResolvedResponse {
                        subscription_id: format!("frontline-load-smoke:{request_id}"),
                        request_id,
                        matched_identity: Some(self.matched_identity()),
                        route: Some(self.route_entry()),
                        cache_policy: Some(pb::ProxyCachePolicy {
                            ttl_millis: ROUTE_CACHE_TTL_MILLIS,
                        }),
                    },
                )),
            })
        } else {
            Ok(pb::ProxySubscribeResponse {
                output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
                    pb::ProxyRouteMissResponse {
                        request_id,
                        request_identity: Some(request_identity),
                        negative_cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 1_000 }),
                    },
                )),
            })
        }
    }

    fn route_matches(&self, identity: &pb::RouteIdentity) -> bool {
        self.http_identity_matches(identity, self.route_path.as_ref())
    }

    fn real_grpc_route_matches(&self, identity: &pb::RouteIdentity) -> bool {
        let Some(pb::route_identity::Kind::Http(identity)) = identity.kind.as_ref() else {
            return false;
        };

        path_prefix_matches(
            identity.path_prefix.as_deref().unwrap_or("/"),
            REAL_GRPC_PATH,
        )
    }

    fn cold_route_matches(&self, identity: &pb::RouteIdentity) -> bool {
        self.http_identity_matches(identity, self.cold_route_path.as_ref())
    }

    fn http_identity_matches(&self, identity: &pb::RouteIdentity, route_path: &str) -> bool {
        let Some(pb::route_identity::Kind::Http(identity)) = identity.kind.as_ref() else {
            return false;
        };
        let Some(host) = identity.host.as_ref() else {
            return false;
        };

        host.kind == pb::RouteHostKind::Exact as i32
            && host.host == self.route_host.as_ref()
            && path_prefix_matches(identity.path_prefix.as_deref().unwrap_or("/"), route_path)
    }

    fn matched_identity(&self) -> pb::RouteIdentity {
        self.http_matched_identity(self.route_path.as_ref())
    }

    fn cold_matched_identity(&self) -> pb::RouteIdentity {
        self.http_matched_identity(self.cold_route_path.as_ref())
    }

    fn http_matched_identity(&self, route_path: &str) -> pb::RouteIdentity {
        pb::RouteIdentity {
            kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(pb::RouteHost {
                    kind: pb::RouteHostKind::Exact as i32,
                    host: self.route_host.to_string(),
                }),
                path_prefix: Some(route_path.to_owned()),
            })),
        }
    }

    fn resolved_response(
        &self,
        request_id: String,
        matched_identity: pb::RouteIdentity,
        route: pb::ProxyRouteEntry,
    ) -> pb::ProxySubscribeResponse {
        pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
                pb::ProxyRouteResolvedResponse {
                    subscription_id: format!("frontline-load-smoke:{request_id}"),
                    request_id,
                    matched_identity: Some(matched_identity),
                    route: Some(route),
                    cache_policy: Some(pb::ProxyCachePolicy {
                        ttl_millis: ROUTE_CACHE_TTL_MILLIS,
                    }),
                },
            )),
        }
    }

    fn route_entry(&self) -> pb::ProxyRouteEntry {
        self.route_entry_for(
            "frontline-load-smoke-route",
            "frontline-load-smoke-instance",
            &self.backend_uri,
        )
    }

    fn real_grpc_route_entry(&self) -> pb::ProxyRouteEntry {
        self.route_entry_for(
            "frontline-load-smoke-real-grpc-route",
            "frontline-load-smoke-real-grpc-instance",
            &self.grpc_backend_uri,
        )
    }

    fn route_entry_for(
        &self,
        route_binding_id: &str,
        instance_id: &str,
        backend_uri: &str,
    ) -> pb::ProxyRouteEntry {
        pb::ProxyRouteEntry {
            backend_address: None,
            route_binding_id: route_binding_id.to_owned(),
            instance_id: instance_id.to_owned(),
            instance_state: pb::InstanceState::Running as i32,
            instance_generation: 1,
            backend_uri: Some(backend_uri.to_owned()),
            backend_generation: Some(1),
        }
    }

    fn cold_route_entry(&self) -> pb::ProxyRouteEntry {
        pb::ProxyRouteEntry {
            backend_address: None,
            route_binding_id: "frontline-load-smoke-cold-route".to_owned(),
            instance_id: "frontline-load-smoke-cold-instance".to_owned(),
            instance_state: pb::InstanceState::Cold as i32,
            instance_generation: 1,
            backend_uri: None,
            backend_generation: None,
        }
    }
}

#[tonic::async_trait]
impl ProxyControlPlane for FakeProxyControlPlane {
    type WatchTlsCertificatesStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<sleepypods_api::pb::WatchTlsCertificatesResponse, tonic::Status>,
                > + Send,
        >,
    >;
    async fn watch_tls_certificates(
        &self,
        request: tonic::Request<tonic::Streaming<sleepypods_api::pb::WatchTlsCertificatesRequest>>,
    ) -> Result<tonic::Response<Self::WatchTlsCertificatesStream>, tonic::Status> {
        Ok(Response::new(
            self.certificates
                .as_ref()
                .ok_or_else(|| Status::unavailable("fixture certificate delivery not configured"))?
                .watch(request.into_inner()),
        ))
    }

    async fn resolve_http01_challenge(
        &self,
        _: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        panic!("unexpected HTTP01")
    }
    async fn resolve_tls_certificate(
        &self,
        request: Request<pb::ResolveTlsCertificateRequest>,
    ) -> Result<Response<pb::ResolveTlsCertificateResponse>, Status> {
        self.stats
            .resolve_certificate_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(
            self.certificates
                .as_ref()
                .ok_or_else(|| Status::unavailable("fixture certificate delivery not configured"))?
                .resolve(request.into_inner()),
        ))
    }

    type SubscribeStream = ReceiverStream<Result<pb::ProxySubscribeResponse, Status>>;

    async fn wake_instance(
        &self,
        request: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        let request = request.into_inner();
        self.stats.record_wake_instance();

        Ok(Response::new(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
                pb::ProxyWakeReadyResult {
                    backend_address: None,
                    instance_id: request.instance_id,
                    instance_generation: request.expected_generation,
                    backend_uri: self.backend_uri.to_string(),
                    backend_generation: 1,
                },
            )),
        }))
    }

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut requests = request.into_inner();
        let service = self.clone();
        let (responses, response_stream) = tokio::sync::mpsc::channel(SUBSCRIBE_RESPONSE_BUFFER);

        tokio::spawn(async move {
            while let Some(request) = match requests.message().await {
                Ok(request) => request,
                Err(status) => {
                    let _ = responses.send(Err(status)).await;
                    return;
                }
            } {
                match service.handle_subscribe_request(request) {
                    Ok(Some(response)) => {
                        if responses.send(Ok(response)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(status) => {
                        let _ = responses.send(Err(status)).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(response_stream)))
    }
}

#[derive(Clone, Debug)]
struct GeneratedGrpcBackend {
    stats: SmokeStats,
}

impl GeneratedGrpcBackend {
    fn new(stats: SmokeStats) -> Self {
        Self { stats }
    }
}

#[tonic::async_trait]
impl ProxyControlPlane for GeneratedGrpcBackend {
    type WatchTlsCertificatesStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<sleepypods_api::pb::WatchTlsCertificatesResponse, tonic::Status>,
                > + Send,
        >,
    >;
    async fn watch_tls_certificates(
        &self,
        _: tonic::Request<tonic::Streaming<sleepypods_api::pb::WatchTlsCertificatesRequest>>,
    ) -> Result<tonic::Response<Self::WatchTlsCertificatesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "unexpected test certificate watch",
        ))
    }

    async fn resolve_http01_challenge(
        &self,
        _: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        panic!("unexpected HTTP01")
    }
    async fn resolve_tls_certificate(
        &self,
        _: Request<pb::ResolveTlsCertificateRequest>,
    ) -> Result<Response<pb::ResolveTlsCertificateResponse>, Status> {
        panic!("unexpected certificate")
    }

    type SubscribeStream = ReceiverStream<Result<pb::ProxySubscribeResponse, Status>>;

    async fn wake_instance(
        &self,
        request: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        self.stats.record_backend_http_request();
        let request = request.into_inner();
        if request.instance_id.trim().is_empty() {
            return Err(Status::invalid_argument("instance_id is required"));
        }

        Ok(Response::new(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
                pb::ProxyWakeReadyResult {
                    backend_address: None,
                    instance_id: request.instance_id,
                    instance_generation: request.expected_generation,
                    backend_uri: "http://generated-grpc-backend.example.test".to_owned(),
                    backend_generation: 1,
                },
            )),
        }))
    }

    async fn subscribe(
        &self,
        _request: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        Err(Status::unimplemented(
            "load-smoke generated gRPC backend only implements WakeInstance",
        ))
    }
}

fn non_empty(value: String, field: &'static str) -> Result<String, Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!(
            "{field} must not be empty"
        )));
    }

    Ok(value)
}

fn path_prefix_matches(request_path: &str, route_path: &str) -> bool {
    if route_path == "/" || request_path == route_path {
        return true;
    }

    if route_path.ends_with('/') {
        return request_path.starts_with(route_path);
    }

    request_path
        .strip_prefix(route_path)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ClientConfig {
    label: String,
    target: HttpTarget,
    host_header: String,
    requests: u64,
    concurrency: u64,
    protocol: ClientProtocol,
    tls_ca_cert_path: Option<PathBuf>,
    websocket_stream_bytes: u64,
    websocket_stream_chunk_size: u64,
}

impl ClientConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, InvalidArgs> {
        let mut label = "smoke".to_owned();
        let mut url = None;
        let mut host_header = None;
        let mut requests = 100;
        let mut concurrency = 4;
        let mut protocol = ClientProtocol::Http1;
        let mut tls_ca_cert_path = None;
        let mut websocket_stream_bytes = DEFAULT_WEBSOCKET_STREAM_BYTES;
        let mut websocket_stream_chunk_size = DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE;
        let mut args = args.peekable();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--label" => label = next_arg(&mut args, "--label")?,
                "--url" => url = Some(next_arg(&mut args, "--url")?),
                "--host" => host_header = Some(next_arg(&mut args, "--host")?),
                "--requests" => {
                    requests =
                        parse_positive_u64("--requests", &next_arg(&mut args, "--requests")?)?
                }
                "--concurrency" => {
                    concurrency =
                        parse_positive_u64("--concurrency", &next_arg(&mut args, "--concurrency")?)?
                }
                "--protocol" => {
                    protocol = ClientProtocol::parse(&next_arg(&mut args, "--protocol")?)?
                }
                "--tls-ca-cert" => {
                    tls_ca_cert_path = Some(PathBuf::from(next_arg(&mut args, "--tls-ca-cert")?))
                }
                "--websocket-stream-bytes" => {
                    websocket_stream_bytes = parse_positive_u64(
                        "--websocket-stream-bytes",
                        &next_arg(&mut args, "--websocket-stream-bytes")?,
                    )?
                }
                "--websocket-stream-chunk-size" => {
                    websocket_stream_chunk_size = parse_positive_u64(
                        "--websocket-stream-chunk-size",
                        &next_arg(&mut args, "--websocket-stream-chunk-size")?,
                    )?
                }
                _ => {
                    return Err(InvalidArgs(format!(
                        "unknown client argument {arg:?}; expected --label, --url, --host, --requests, --concurrency, --protocol, --tls-ca-cert, --websocket-stream-bytes, or --websocket-stream-chunk-size"
                    )));
                }
            }
        }

        let url = url.ok_or_else(|| InvalidArgs("--url is required".to_owned()))?;
        let host_header =
            host_header.ok_or_else(|| InvalidArgs("--host is required".to_owned()))?;
        validate_header_value("--host", &host_header)?;

        Ok(Self {
            label,
            target: HttpTarget::parse(&url)?,
            host_header,
            requests,
            concurrency,
            protocol,
            tls_ca_cert_path,
            websocket_stream_bytes,
            websocket_stream_chunk_size,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientProtocol {
    Http1,
    H2cGrpc,
    H2TlsGrpc,
    GeneratedGrpc,
    WebSocket,
}

impl ClientProtocol {
    fn parse(value: &str) -> Result<Self, InvalidArgs> {
        match value {
            "http1" => Ok(Self::Http1),
            "h2c-grpc" => Ok(Self::H2cGrpc),
            "h2-tls-grpc" => Ok(Self::H2TlsGrpc),
            "generated-grpc" => Ok(Self::GeneratedGrpc),
            "websocket" => Ok(Self::WebSocket),
            _ => Err(InvalidArgs(format!(
                "--protocol must be http1, h2c-grpc, h2-tls-grpc, generated-grpc, or websocket, got {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CertConfig {
    host: String,
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl CertConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, InvalidArgs> {
        let mut host = None;
        let mut cert_path = None;
        let mut key_path = None;
        let mut args = args.peekable();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--host" => host = Some(next_arg(&mut args, "--host")?),
                "--cert-path" => {
                    cert_path = Some(PathBuf::from(next_arg(&mut args, "--cert-path")?))
                }
                "--key-path" => key_path = Some(PathBuf::from(next_arg(&mut args, "--key-path")?)),
                _ => {
                    return Err(InvalidArgs(format!(
                        "unknown cert argument {arg:?}; expected --host, --cert-path, or --key-path"
                    )));
                }
            }
        }

        let host = host.ok_or_else(|| InvalidArgs("--host is required".to_owned()))?;
        validate_header_value("--host", &host)?;

        Ok(Self {
            host,
            cert_path: cert_path
                .ok_or_else(|| InvalidArgs("--cert-path is required".to_owned()))?,
            key_path: key_path.ok_or_else(|| InvalidArgs("--key-path is required".to_owned()))?,
        })
    }
}

fn write_self_signed_cert(config: CertConfig) -> Result<(), BoxError> {
    let certified_key = generate_simple_self_signed(vec![config.host])?;
    fs::write(config.cert_path, certified_key.cert.pem())?;
    fs::write(config.key_path, certified_key.signing_key.serialize_pem())?;
    Ok(())
}

fn next_arg(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    name: &'static str,
) -> Result<String, InvalidArgs> {
    args.next()
        .ok_or_else(|| InvalidArgs(format!("{name} requires a value")))
}

fn parse_positive_u64(name: &'static str, value: &str) -> Result<u64, InvalidArgs> {
    let value = value
        .parse::<u64>()
        .map_err(|error| InvalidArgs(format!("{name} must be a positive integer: {error}")))?;

    if value == 0 {
        return Err(InvalidArgs(format!("{name} must be greater than zero")));
    }

    Ok(value)
}

fn validate_header_value(name: &'static str, value: &str) -> Result<(), InvalidArgs> {
    if value.trim().is_empty() {
        return Err(InvalidArgs(format!("{name} must not be empty")));
    }

    if value.contains('\r') || value.contains('\n') {
        return Err(InvalidArgs(format!("{name} must not contain CR or LF")));
    }

    Ok(())
}

fn canonical_route_host(name: &'static str, value: &str) -> Result<String, InvalidArgs> {
    validate_header_value(name, value)?;

    RouteHost::exact(value)
        .map(|host| host.as_str().to_owned())
        .map_err(|error| InvalidArgs(format!("{name} must be a valid exact route host: {error}")))
}

fn validate_route_path(name: &'static str, value: &str) -> Result<(), InvalidArgs> {
    if !value.starts_with('/') {
        return Err(InvalidArgs(format!("{name} must start with /")));
    }

    validate_header_value(name, value)
}

async fn run_client(config: ClientConfig) -> Result<(), BoxError> {
    let next_request = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let latencies = LatencyRecorder::new(config.requests);
    let mut tasks = JoinSet::new();
    let (ready_tx, mut ready_rx) = mpsc::channel(config.concurrency as usize);
    let (start_tx, start_rx) = watch::channel(false);

    for _ in 0..config.concurrency {
        let next_request = Arc::clone(&next_request);
        let failures = Arc::clone(&failures);
        let latencies = latencies.clone();
        let target = config.target.clone();
        let host_header = config.host_header.clone();
        let requests = config.requests;
        let protocol = config.protocol;
        let tls_ca_cert_path = config.tls_ca_cert_path.clone();
        let websocket_stream_bytes = config.websocket_stream_bytes;
        let websocket_stream_chunk_size = config.websocket_stream_chunk_size;
        let ready_tx = ready_tx.clone();
        let mut start_rx = start_rx.clone();

        tasks.spawn(async move {
            let tls_connector = match (protocol, tls_ca_cert_path.as_deref()) {
                (ClientProtocol::H2TlsGrpc, Some(ca_cert_path)) => {
                    Some(tls_connector(ca_cert_path)?)
                }
                (ClientProtocol::H2TlsGrpc, None) => {
                    return Err(Box::new(InvalidArgs(
                        "--tls-ca-cert is required for h2-tls-grpc".to_owned(),
                    )) as BoxError);
                }
                _ => None,
            };
            let mut h2_grpc = match protocol {
                ClientProtocol::H2cGrpc => {
                    Some(H2GrpcClient::connect_h2c(target.clone(), host_header.clone()).await?)
                }
                ClientProtocol::H2TlsGrpc => Some(
                    H2GrpcClient::connect_h2_tls(
                        target.clone(),
                        host_header.clone(),
                        tls_connector
                            .as_ref()
                            .expect("h2-tls-grpc connector is initialized"),
                    )
                    .await?,
                ),
                _ => None,
            };
            ready_tx.send(()).await.map_err(|_| {
                Box::new(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "load coordinator dropped readiness channel",
                )) as BoxError
            })?;
            while !*start_rx.borrow() {
                start_rx.changed().await.map_err(|_| {
                    Box::new(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "load coordinator dropped start channel",
                    )) as BoxError
                })?;
            }

            loop {
                let request_id = next_request.fetch_add(1, Ordering::Relaxed);

                if request_id >= requests {
                    break;
                }

                let request_start = Instant::now();
                let result = match protocol {
                    ClientProtocol::Http1 => {
                        send_smoke_request(&target, &host_header, request_id).await
                    }
                    ClientProtocol::H2cGrpc | ClientProtocol::H2TlsGrpc => {
                        h2_grpc
                            .as_mut()
                            .expect("h2 grpc client is initialized")
                            .send(request_id)
                            .await
                    }
                    ClientProtocol::GeneratedGrpc => {
                        send_generated_grpc_request(&target, &host_header, request_id).await
                    }
                    ClientProtocol::WebSocket => {
                        send_websocket_request(
                            &target,
                            &host_header,
                            request_id,
                            websocket_stream_bytes,
                            websocket_stream_chunk_size,
                        )
                        .await
                    }
                };

                match result {
                    Ok(()) => latencies.record(request_start.elapsed()),
                    Err(error) => {
                        failures.fetch_add(1, Ordering::Relaxed);

                        if request_id < 5 {
                            eprintln!("request {request_id} failed: {error}");
                        }
                    }
                }
            }

            Ok::<(), BoxError>(())
        });
    }
    drop(ready_tx);

    for _ in 0..config.concurrency {
        ready_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::other("load worker exited before starting"))?;
    }
    let start = Instant::now();
    start_tx
        .send(true)
        .map_err(|_| io::Error::other("load workers exited before start"))?;

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis().max(1);
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let failures = failures.load(Ordering::Relaxed);
    let rps = config.requests as f64 / elapsed_secs;
    let stream_bytes = match config.protocol {
        ClientProtocol::WebSocket => config
            .requests
            .saturating_mul(config.websocket_stream_bytes),
        ClientProtocol::Http1
        | ClientProtocol::H2cGrpc
        | ClientProtocol::H2TlsGrpc
        | ClientProtocol::GeneratedGrpc => 0,
    };
    let mib_per_s = stream_bytes as f64 / (1024.0 * 1024.0) / elapsed_secs;
    let latency = latencies.snapshot();

    println!(
        "{} requests={} failures={} elapsed_ms={} rps={:.1} stream_bytes={} mib_per_s={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3}",
        config.label,
        config.requests,
        failures,
        elapsed_ms,
        rps,
        stream_bytes,
        mib_per_s,
        latency.p50_ms,
        latency.p95_ms,
        latency.p99_ms,
        latency.max_ms
    );

    if failures > 0 {
        return Err(Box::new(ClientFailures { failures }));
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct LatencyRecorder {
    durations: Arc<Mutex<Vec<Duration>>>,
}

impl LatencyRecorder {
    fn new(capacity: u64) -> Self {
        let capacity = usize::try_from(capacity.min(100_000)).unwrap_or(100_000);
        Self {
            durations: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
        }
    }

    fn record(&self, duration: Duration) {
        self.durations
            .lock()
            .expect("latency recorder mutex is not poisoned")
            .push(duration);
    }

    fn snapshot(&self) -> LatencyStats {
        let durations = self
            .durations
            .lock()
            .expect("latency recorder mutex is not poisoned");
        LatencyStats::from_durations(&durations)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct LatencyStats {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

impl LatencyStats {
    fn from_durations(durations: &[Duration]) -> Self {
        if durations.is_empty() {
            return Self::default();
        }

        let mut values = durations.to_vec();
        values.sort_unstable();

        Self {
            p50_ms: duration_ms(percentile(&values, 50)),
            p95_ms: duration_ms(percentile(&values, 95)),
            p99_ms: duration_ms(percentile(&values, 99)),
            max_ms: duration_ms(*values.last().expect("non-empty latency values")),
        }
    }
}

fn percentile(sorted: &[Duration], percentile: u64) -> Duration {
    debug_assert!(!sorted.is_empty());
    debug_assert!(percentile <= 100);

    let rank = ((sorted.len() as u64 * percentile).div_ceil(100)).max(1);
    sorted[(rank - 1) as usize]
}

fn duration_ms(duration: Duration) -> f64 {
    let ms = duration.as_secs_f64() * 1000.0;
    if ms > 0.0 && ms < 0.001 {
        0.001
    } else {
        ms
    }
}

async fn send_smoke_request(
    target: &HttpTarget,
    host_header: &str,
    request_id: u64,
) -> Result<(), BoxError> {
    let mut stream = timeout(REQUEST_TIMEOUT, TcpStream::connect(target.authority()))
        .await
        .map_err(|_| timeout_error("connect"))??;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nUser-Agent: frontline-load-smoke\r\n\r\n",
        target.request_path(request_id),
        host_header
    );

    timeout(REQUEST_TIMEOUT, stream.write_all(request.as_bytes()))
        .await
        .map_err(|_| timeout_error("write request"))??;

    let mut response = Vec::with_capacity(256);
    timeout(REQUEST_TIMEOUT, stream.read_to_end(&mut response))
        .await
        .map_err(|_| timeout_error("read response"))??;

    validate_response(&response)?;
    Ok(())
}

async fn send_websocket_request(
    target: &HttpTarget,
    host_header: &str,
    request_id: u64,
    stream_bytes: u64,
    chunk_size: u64,
) -> Result<(), BoxError> {
    let mut request = format!(
        "ws://{}{}",
        target.authority(),
        target.request_path(request_id)
    )
    .into_client_request()?;
    request.headers_mut().insert(
        HOST,
        HeaderValue::from_str(host_header)
            .map_err(|error| InvalidArgs(format!("--host must be a valid header: {error}")))?,
    );

    let (mut websocket, response) = timeout(REQUEST_TIMEOUT, connect_async(request))
        .await
        .map_err(|_| timeout_error("websocket connect"))??;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        return Err(Box::new(ResponseValidationError::UnexpectedStatus(
            format!("websocket {}", response.status()),
        )));
    }

    timeout(
        REQUEST_TIMEOUT,
        websocket.send(Message::Text(WEBSOCKET_CLIENT_TEXT.into())),
    )
    .await
    .map_err(|_| timeout_error("websocket write text"))??;
    let text = timeout(REQUEST_TIMEOUT, websocket.next())
        .await
        .map_err(|_| timeout_error("websocket read text"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing text frame"))??;
    if text != Message::Text(WEBSOCKET_BACKEND_TEXT.into()) {
        return Err(Box::new(
            ResponseValidationError::UnexpectedWebSocketMessage(format!("{text:?}")),
        ));
    }

    timeout(
        REQUEST_TIMEOUT,
        websocket.send(Message::Binary(WsBytes::from_static(
            WEBSOCKET_CLIENT_BINARY,
        ))),
    )
    .await
    .map_err(|_| timeout_error("websocket write binary"))??;
    let binary = timeout(REQUEST_TIMEOUT, websocket.next())
        .await
        .map_err(|_| timeout_error("websocket read binary"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing binary frame"))??;
    if binary != Message::Binary(WsBytes::from_static(WEBSOCKET_BACKEND_BINARY)) {
        return Err(Box::new(
            ResponseValidationError::UnexpectedWebSocketMessage(format!("{binary:?}")),
        ));
    }

    send_and_validate_websocket_stream(&mut websocket, stream_bytes, chunk_size).await?;

    timeout(REQUEST_TIMEOUT, websocket.close(None))
        .await
        .map_err(|_| timeout_error("websocket close"))??;

    Ok(())
}

async fn send_and_validate_websocket_stream<S>(
    websocket: &mut WebSocketStream<S>,
    stream_bytes: u64,
    chunk_size: u64,
) -> Result<(), BoxError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut sent = 0;

    while sent < stream_bytes {
        let remaining = stream_bytes - sent;
        let len = remaining.min(chunk_size) as usize;
        let chunk = make_stream_chunk(sent, len);
        timeout(
            REQUEST_TIMEOUT,
            websocket.send(Message::Binary(WsBytes::from(chunk.clone()))),
        )
        .await
        .map_err(|_| timeout_error("websocket write stream chunk"))??;

        let response = timeout(REQUEST_TIMEOUT, websocket.next())
            .await
            .map_err(|_| timeout_error("websocket read stream chunk"))?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "missing stream echo chunk")
            })??;
        let Message::Binary(bytes) = response else {
            return Err(Box::new(
                ResponseValidationError::UnexpectedWebSocketMessage(format!("{response:?}")),
            ));
        };
        if bytes.as_ref() != chunk.as_slice() {
            validate_stream_chunk(&bytes, sent)?;
            return Err(Box::new(ResponseValidationError::UnexpectedStreamChunk {
                offset: sent,
                len: bytes.len(),
                expected_len: len,
            }));
        }

        sent += len as u64;
    }

    Ok(())
}

struct H2GrpcClient {
    target: HttpTarget,
    host_header: String,
    scheme: &'static str,
    sender: client_http2::SendRequest<Full<Bytes>>,
    connection_task: tokio::task::JoinHandle<()>,
}

impl H2GrpcClient {
    async fn connect_h2c(target: HttpTarget, host_header: String) -> Result<Self, BoxError> {
        let stream = timeout(REQUEST_TIMEOUT, TcpStream::connect(target.authority()))
            .await
            .map_err(|_| timeout_error("h2c connect"))??;
        let _ = stream.set_nodelay(true);
        let (sender, connection) = timeout(
            REQUEST_TIMEOUT,
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream)),
        )
        .await
        .map_err(|_| timeout_error("h2c handshake"))??;

        Ok(Self::new(target, host_header, "http", sender, connection))
    }

    async fn connect_h2_tls(
        target: HttpTarget,
        host_header: String,
        tls_connector: &TlsConnector,
    ) -> Result<Self, BoxError> {
        let stream = timeout(REQUEST_TIMEOUT, TcpStream::connect(target.authority()))
            .await
            .map_err(|_| timeout_error("h2 TLS connect"))??;
        let _ = stream.set_nodelay(true);
        let server_name = ServerName::try_from(host_header.to_owned())
            .map_err(|error| InvalidArgs(format!("--host must be a TLS server name: {error}")))?;
        let tls = timeout(REQUEST_TIMEOUT, tls_connector.connect(server_name, stream))
            .await
            .map_err(|_| timeout_error("h2 TLS handshake"))??;

        let negotiated = tls
            .get_ref()
            .1
            .alpn_protocol()
            .ok_or(ResponseValidationError::MissingAlpnProtocol)?;
        if negotiated != b"h2" {
            return Err(Box::new(ResponseValidationError::UnexpectedAlpnProtocol(
                String::from_utf8_lossy(negotiated).into_owned(),
            )));
        }

        let (sender, connection) = timeout(
            REQUEST_TIMEOUT,
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(tls)),
        )
        .await
        .map_err(|_| timeout_error("h2 TLS HTTP/2 handshake"))??;

        Ok(Self::new(target, host_header, "https", sender, connection))
    }

    fn new<IO>(
        target: HttpTarget,
        host_header: String,
        scheme: &'static str,
        sender: client_http2::SendRequest<Full<Bytes>>,
        connection: client_http2::Connection<TokioIo<IO>, Full<Bytes>, TokioExecutor>,
    ) -> Self
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        Self {
            target,
            host_header,
            scheme,
            sender,
            connection_task,
        }
    }

    async fn send(&mut self, request_id: u64) -> Result<(), BoxError> {
        let request = HttpRequest::builder()
            .method("POST")
            .uri(format!(
                "{}://{}{}",
                self.scheme,
                self.host_header,
                self.target.request_path(request_id)
            ))
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, "application/grpc")
            .body(Full::new(Bytes::from_static(GRPC_BODY)))?;
        let response = timeout(REQUEST_TIMEOUT, self.sender.send_request(request))
            .await
            .map_err(|_| timeout_error("h2 send request"))??;

        validate_h2c_grpc_response(response)
            .await
            .map_err(|error| Box::new(error) as BoxError)
    }
}

impl Drop for H2GrpcClient {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

fn tls_connector(ca_cert_path: &Path) -> Result<TlsConnector, BoxError> {
    let mut roots = RootCertStore::empty();
    for cert in load_pem_certificates(ca_cert_path)? {
        roots.add(cert)?;
    }
    let mut config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_pem_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, BoxError> {
    let file = fs::File::open(path)?;
    let mut reader = io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(Box::new(InvalidArgs(format!(
            "TLS CA certificate file {:?} has no certificates",
            path
        ))));
    }
    Ok(certs)
}

async fn send_generated_grpc_request(
    target: &HttpTarget,
    host_header: &str,
    request_id: u64,
) -> Result<(), BoxError> {
    let channel = generated_grpc_channel(target).await?;
    let origin = generated_grpc_origin(target, host_header)?;
    let mut client =
        pb::proxy_control_plane_client::ProxyControlPlaneClient::with_origin(channel, origin);
    let response = timeout(
        REQUEST_TIMEOUT,
        client.wake_instance(pb::ProxyWakeInstanceRequest {
            instance_id: format!("generated-grpc-load-smoke-{request_id}"),
            expected_generation: 7,
            backend_generation: Some(1),
        }),
    )
    .await
    .map_err(|_| timeout_error("generated gRPC request"))??
    .into_inner();

    let Some(pb::proxy_wake_instance_response::Outcome::Ready(ready)) = response.outcome else {
        return Err(Box::new(ResponseValidationError::UnexpectedGrpcOutcome));
    };
    if ready.instance_generation != 7 || ready.backend_generation != 1 {
        return Err(Box::new(ResponseValidationError::UnexpectedGrpcOutcome));
    }

    Ok(())
}

async fn generated_grpc_channel(target: &HttpTarget) -> Result<Channel, BoxError> {
    if target.scheme != TargetScheme::Http {
        return Err(Box::new(InvalidArgs(
            "generated-grpc only supports http:// targets".to_owned(),
        )));
    }
    let endpoint_uri = format!("{}://{}", target.scheme.as_str(), target.authority());
    Ok(Endpoint::from_shared(endpoint_uri)?.connect().await?)
}

fn generated_grpc_origin(target: &HttpTarget, host_header: &str) -> Result<Uri, InvalidArgs> {
    format!("{}://{}", target.scheme.as_str(), host_header)
        .parse()
        .map_err(|error| InvalidArgs(format!("generated gRPC origin is invalid: {error}")))
}

async fn validate_h2c_grpc_response(
    response: HttpResponse<Incoming>,
) -> Result<(), ResponseValidationError> {
    if response.version() != Version::HTTP_2 {
        return Err(ResponseValidationError::UnexpectedVersion(
            response.version(),
        ));
    }

    if response.status() != StatusCode::OK {
        return Err(ResponseValidationError::UnexpectedStatus(format!(
            "{:?} {}",
            response.version(),
            response.status()
        )));
    }

    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .ok_or(ResponseValidationError::MissingHeader("content-type"))?;
    if content_type != "application/grpc" {
        return Err(ResponseValidationError::UnexpectedHeader {
            name: "content-type",
            value: format!("{content_type:?}"),
        });
    }

    if let Some(grpc_status) = response.headers().get("grpc-status") {
        return Err(ResponseValidationError::UnexpectedHeader {
            name: "grpc-status",
            value: format!("initial header {grpc_status:?}"),
        });
    }

    let collected = timeout(REQUEST_TIMEOUT, response.into_body().collect())
        .await
        .map_err(|_| ResponseValidationError::BodyRead("timed out reading body".to_owned()))?
        .map_err(|error| ResponseValidationError::BodyRead(error.to_string()))?;
    let trailers = collected
        .trailers()
        .ok_or(ResponseValidationError::MissingTrailers)?;
    let grpc_status = trailers
        .get("grpc-status")
        .ok_or(ResponseValidationError::MissingHeader("grpc-status"))?;
    if grpc_status != "0" {
        return Err(ResponseValidationError::UnexpectedHeader {
            name: "grpc-status",
            value: format!("{grpc_status:?}"),
        });
    }

    let body = collected.to_bytes();
    if body.as_ref() != GRPC_BODY {
        return Err(ResponseValidationError::UnexpectedBody { len: body.len() });
    }

    Ok(())
}

fn timeout_error(stage: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, format!("timed out during {stage}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HttpTarget {
    scheme: TargetScheme,
    host: String,
    port: u16,
    path: String,
}

impl HttpTarget {
    fn parse(url: &str) -> Result<Self, InvalidArgs> {
        let (scheme, rest) = if let Some(rest) = url.strip_prefix("http://") {
            (TargetScheme::Http, rest)
        } else if let Some(rest) = url.strip_prefix("https://") {
            (TargetScheme::Https, rest)
        } else {
            return Err(InvalidArgs(
                "only http:// and https:// URLs are supported".to_owned(),
            ));
        };
        let (authority, path) = match rest.find('/') {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, "/"),
        };
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| InvalidArgs("URL must include host and port".to_owned()))?;

        if host.is_empty() {
            return Err(InvalidArgs("URL host is required".to_owned()));
        }

        let port = port
            .parse::<u16>()
            .map_err(|error| InvalidArgs(format!("URL port must be a u16: {error}")))?;

        if port == 0 {
            return Err(InvalidArgs("URL port must be greater than zero".to_owned()));
        }

        Ok(Self {
            scheme,
            host: host.to_owned(),
            port,
            path: path.to_owned(),
        })
    }

    fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn request_path(&self, request_id: u64) -> String {
        let separator = if self.path.contains('?') { '&' } else { '?' };
        format!("{}{}smoke_request={request_id}", self.path, separator)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetScheme {
    Http,
    Https,
}

impl TargetScheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

fn validate_response(bytes: &[u8]) -> Result<(), ResponseValidationError> {
    let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err(ResponseValidationError::MissingHeaders);
    };
    let headers = std::str::from_utf8(&bytes[..header_end])
        .map_err(|_| ResponseValidationError::NonUtf8Headers)?;
    let status_line = headers
        .lines()
        .next()
        .ok_or(ResponseValidationError::MissingStatus)?;

    if !status_line.starts_with("HTTP/1.") || !status_line.contains(" 200 ") {
        return Err(ResponseValidationError::UnexpectedStatus(
            status_line.to_owned(),
        ));
    }

    let body = &bytes[header_end + 4..];
    if body != BACKEND_BODY {
        return Err(ResponseValidationError::UnexpectedBody { len: body.len() });
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InvalidArgs(String);

impl fmt::Display for InvalidArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for InvalidArgs {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientFailures {
    failures: u64,
}

impl fmt::Display for ClientFailures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} request(s) failed", self.failures)
    }
}

impl Error for ClientFailures {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResponseValidationError {
    MissingHeaders,
    MissingHeader(&'static str),
    MissingTrailers,
    MissingAlpnProtocol,
    NonUtf8Headers,
    MissingStatus,
    UnexpectedVersion(Version),
    UnexpectedStatus(String),
    UnexpectedHeader {
        name: &'static str,
        value: String,
    },
    UnexpectedAlpnProtocol(String),
    UnexpectedGrpcOutcome,
    UnexpectedBody {
        len: usize,
    },
    UnexpectedWebSocketMessage(String),
    UnexpectedStreamChunk {
        offset: u64,
        len: usize,
        expected_len: usize,
    },
    UnexpectedStreamByte {
        offset: u64,
        expected: u8,
        actual: u8,
    },
    BodyRead(String),
}

impl fmt::Display for ResponseValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeaders => write!(f, "HTTP response headers are missing"),
            Self::MissingHeader(name) => write!(f, "HTTP response header {name} is missing"),
            Self::MissingTrailers => write!(f, "HTTP response trailers are missing"),
            Self::MissingAlpnProtocol => write!(f, "TLS ALPN protocol is missing"),
            Self::NonUtf8Headers => write!(f, "HTTP response headers are not UTF-8"),
            Self::MissingStatus => write!(f, "HTTP response status line is missing"),
            Self::UnexpectedVersion(version) => {
                write!(f, "unexpected HTTP response version {version:?}")
            }
            Self::UnexpectedStatus(status) => write!(f, "unexpected HTTP status line {status:?}"),
            Self::UnexpectedHeader { name, value } => {
                write!(f, "unexpected HTTP response header {name}: {value}")
            }
            Self::UnexpectedAlpnProtocol(protocol) => {
                write!(f, "unexpected TLS ALPN protocol {protocol:?}")
            }
            Self::UnexpectedGrpcOutcome => write!(f, "unexpected generated gRPC response outcome"),
            Self::UnexpectedBody { len } => {
                write!(f, "unexpected HTTP response body length {len}")
            }
            Self::UnexpectedWebSocketMessage(message) => {
                write!(f, "unexpected WebSocket message {message}")
            }
            Self::UnexpectedStreamChunk {
                offset,
                len,
                expected_len,
            } => {
                write!(
                    f,
                    "unexpected WebSocket stream chunk at offset {offset}: len={len} expected_len={expected_len}"
                )
            }
            Self::UnexpectedStreamByte {
                offset,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "unexpected WebSocket stream byte at offset {offset}: expected={expected} actual={actual}"
                )
            }
            Self::BodyRead(error) => write!(f, "failed to read HTTP response body: {error}"),
        }
    }
}

impl Error for ResponseValidationError {}

#[cfg(test)]
mod tests {
    use super::{
        backend_response, make_stream_chunk, path_prefix_matches, validate_response,
        validate_stream_chunk, ClientConfig, ClientProtocol, FakeProxyControlPlane, HttpTarget,
        LatencyStats, SmokeStats, BACKEND_BODY, DEFAULT_WEBSOCKET_STREAM_BYTES,
        DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE, GRPC_BODY, STATS_PATH,
    };
    use crate::REAL_GRPC_PATH;
    use bytes::Bytes;
    use http::{header::CONTENT_TYPE, Request as HttpRequest};
    use http_body_util::{BodyExt, Full};
    use sleepypods_api::pb;
    use std::path::PathBuf;

    #[test]
    fn parses_http_target_with_path_and_query() {
        assert_eq!(
            HttpTarget::parse("http://127.0.0.1:18080/smoke?ready=true").expect("target parses"),
            HttpTarget {
                scheme: super::TargetScheme::Http,
                host: "127.0.0.1".to_owned(),
                port: 18080,
                path: "/smoke?ready=true".to_owned(),
            }
        );
    }

    #[test]
    fn request_path_appends_smoke_request_parameter() {
        let target =
            HttpTarget::parse("http://127.0.0.1:18080/smoke?ready=true").expect("target parses");

        assert_eq!(target.request_path(7), "/smoke?ready=true&smoke_request=7");
    }

    #[test]
    fn client_config_requires_explicit_host_header() {
        let error = ClientConfig::from_args(
            ["--url", "http://127.0.0.1:18080/"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("host is required");

        assert_eq!(error.0, "--host is required");
    }

    #[test]
    fn client_config_rejects_zero_request_count() {
        let error = ClientConfig::from_args(
            [
                "--url",
                "http://127.0.0.1:18080/",
                "--host",
                "app.example.test",
                "--requests",
                "0",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect_err("zero request count is rejected");

        assert_eq!(error.0, "--requests must be greater than zero");
    }

    #[test]
    fn client_config_parses_h2c_grpc_protocol() {
        let config = ClientConfig::from_args(
            [
                "--url",
                "http://127.0.0.1:18080/",
                "--host",
                "app.example.test",
                "--protocol",
                "h2c-grpc",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("h2c gRPC-shaped client config parses");

        assert_eq!(config.protocol, ClientProtocol::H2cGrpc);
    }

    #[test]
    fn client_config_parses_h2_tls_grpc_protocol() {
        let config = ClientConfig::from_args(
            [
                "--url",
                "https://127.0.0.1:18443/",
                "--host",
                "app.example.test",
                "--protocol",
                "h2-tls-grpc",
                "--tls-ca-cert",
                "/tmp/load-smoke.crt",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("h2 TLS gRPC-shaped client config parses");

        assert_eq!(config.protocol, ClientProtocol::H2TlsGrpc);
        assert_eq!(
            config.tls_ca_cert_path,
            Some(PathBuf::from("/tmp/load-smoke.crt"))
        );
    }

    #[test]
    fn client_config_parses_generated_grpc_protocol() {
        let config = ClientConfig::from_args(
            [
                "--url",
                "http://127.0.0.1:18082/",
                "--host",
                "app.example.test",
                "--protocol",
                "generated-grpc",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("generated gRPC client config parses");

        assert_eq!(config.protocol, ClientProtocol::GeneratedGrpc);
    }

    #[test]
    fn client_config_parses_websocket_protocol() {
        let config = ClientConfig::from_args(
            [
                "--url",
                "http://127.0.0.1:18080/",
                "--host",
                "app.example.test",
                "--protocol",
                "websocket",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("WebSocket client config parses");

        assert_eq!(config.protocol, ClientProtocol::WebSocket);
        assert_eq!(
            config.websocket_stream_bytes,
            DEFAULT_WEBSOCKET_STREAM_BYTES
        );
        assert_eq!(
            config.websocket_stream_chunk_size,
            DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE
        );
    }

    #[test]
    fn client_config_parses_websocket_stream_options() {
        let config = ClientConfig::from_args(
            [
                "--url",
                "http://127.0.0.1:18080/",
                "--host",
                "app.example.test",
                "--protocol",
                "websocket",
                "--websocket-stream-bytes",
                "4096",
                "--websocket-stream-chunk-size",
                "1024",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("WebSocket stream config parses");

        assert_eq!(config.websocket_stream_bytes, 4096);
        assert_eq!(config.websocket_stream_chunk_size, 1024);
    }

    #[test]
    fn websocket_stream_chunks_are_deterministic_and_validated() {
        let chunk = make_stream_chunk(8, 32);
        validate_stream_chunk(&chunk, 8).expect("deterministic chunk validates");

        let mut corrupted = chunk;
        corrupted[7] ^= 0xff;
        validate_stream_chunk(&corrupted, 8).expect_err("corrupted chunk is rejected");
    }

    #[test]
    fn websocket_stream_chunks_reject_wrong_default_chunk_offset() {
        let chunk = make_stream_chunk(0, DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE as usize);

        validate_stream_chunk(&chunk, DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE)
            .expect_err("replayed full chunk at a later offset is rejected");
    }

    #[test]
    fn latency_stats_use_nearest_rank_percentiles() {
        let durations = (1..=100)
            .map(std::time::Duration::from_millis)
            .collect::<Vec<_>>();

        assert_eq!(
            LatencyStats::from_durations(&durations),
            LatencyStats {
                p50_ms: 50.0,
                p95_ms: 95.0,
                p99_ms: 99.0,
                max_ms: 100.0,
            }
        );
    }

    #[test]
    fn path_prefix_matching_uses_route_boundaries() {
        assert!(path_prefix_matches("/smoke", "/smoke"));
        assert!(path_prefix_matches("/smoke/request", "/smoke"));
        assert!(!path_prefix_matches("/smoke-test", "/smoke"));
    }

    #[test]
    fn fake_control_plane_matches_known_http_route() {
        let service = FakeProxyControlPlane::new(
            "app.example.test".to_owned(),
            "/smoke".to_owned(),
            "/cold-smoke".to_owned(),
            "http://127.0.0.1:18080".to_owned(),
            "http://127.0.0.1:18082".to_owned(),
            SmokeStats::default(),
        );

        assert!(service.route_matches(&pb::RouteIdentity {
            kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(pb::RouteHost {
                    kind: pb::RouteHostKind::Exact as i32,
                    host: "app.example.test".to_owned(),
                }),
                path_prefix: Some("/smoke".to_owned()),
            })),
        }));
    }

    #[test]
    fn fake_control_plane_counts_subscribe_route_requests() {
        let stats = SmokeStats::default();
        let service = FakeProxyControlPlane::new(
            "app.example.test".to_owned(),
            "/smoke".to_owned(),
            "/cold-smoke".to_owned(),
            "http://127.0.0.1:18080".to_owned(),
            "http://127.0.0.1:18082".to_owned(),
            stats.clone(),
        );

        let request = pb::ProxySubscribeRequest {
            input: Some(pb::proxy_subscribe_request::Input::SubscribeRoute(
                pb::ProxySubscribeRouteRequest {
                    request_id: "request-1".to_owned(),
                    identity: Some(pb::RouteIdentity {
                        kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                            host: Some(pb::RouteHost {
                                kind: pb::RouteHostKind::Exact as i32,
                                host: "app.example.test".to_owned(),
                            }),
                            path_prefix: Some("/smoke".to_owned()),
                        })),
                    }),
                },
            )),
        };

        let response = service
            .handle_subscribe_request(request)
            .expect("subscribe route succeeds");

        assert!(response.is_some());
        assert_eq!(stats.snapshot().subscribe_route_calls, 1);
    }

    #[test]
    fn fake_control_plane_matches_generated_grpc_route_path() {
        let service = FakeProxyControlPlane::new(
            "app.example.test".to_owned(),
            "/smoke".to_owned(),
            "/cold-smoke".to_owned(),
            "http://127.0.0.1:18080".to_owned(),
            "http://127.0.0.1:18082".to_owned(),
            SmokeStats::default(),
        );

        assert!(service.real_grpc_route_matches(&pb::RouteIdentity {
            kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(pb::RouteHost {
                    kind: pb::RouteHostKind::Exact as i32,
                    host: "127.0.0.1".to_owned(),
                }),
                path_prefix: Some(REAL_GRPC_PATH.to_owned()),
            })),
        }));

        let request_identity = pb::RouteIdentity {
            kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(pb::RouteHost {
                    kind: pb::RouteHostKind::Exact as i32,
                    host: "127.0.0.1".to_owned(),
                }),
                path_prefix: Some(REAL_GRPC_PATH.to_owned()),
            })),
        };
        let response = service
            .subscribe_route_response(pb::ProxySubscribeRouteRequest {
                request_id: "generated-grpc-request".to_owned(),
                identity: Some(request_identity.clone()),
            })
            .expect("generated gRPC route resolves");
        let Some(pb::proxy_subscribe_response::Output::RouteResolved(resolved)) = response.output
        else {
            panic!("expected resolved route");
        };
        assert_eq!(resolved.matched_identity, Some(request_identity));
    }

    #[test]
    fn fake_control_plane_returns_cold_route_for_cold_path() {
        let service = FakeProxyControlPlane::new(
            "app.example.test".to_owned(),
            "/smoke".to_owned(),
            "/cold-smoke".to_owned(),
            "http://127.0.0.1:18080".to_owned(),
            "http://127.0.0.1:18082".to_owned(),
            SmokeStats::default(),
        );

        let response = service
            .subscribe_route_response(pb::ProxySubscribeRouteRequest {
                request_id: "cold-request".to_owned(),
                identity: Some(pb::RouteIdentity {
                    kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                        host: Some(pb::RouteHost {
                            kind: pb::RouteHostKind::Exact as i32,
                            host: "app.example.test".to_owned(),
                        }),
                        path_prefix: Some("/cold-smoke".to_owned()),
                    })),
                }),
            })
            .expect("cold route resolves");

        let Some(pb::proxy_subscribe_response::Output::RouteResolved(resolved)) = response.output
        else {
            panic!("expected resolved route");
        };
        let route = resolved.route.expect("route is present");
        assert_eq!(route.instance_state, pb::InstanceState::Cold as i32);
        assert!(route.backend_uri.is_none());
    }

    #[tokio::test]
    async fn backend_stats_endpoint_reports_subscribe_route_count() {
        let stats = SmokeStats::default();
        stats.record_subscribe_route();
        stats.record_subscribe_route();

        let response = backend_response(
            HttpRequest::builder()
                .uri(STATS_PATH)
                .body(Full::new(Bytes::new()))
                .expect("stats request builds"),
            &stats,
            DEFAULT_WEBSOCKET_STREAM_BYTES,
            DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE,
        )
        .await;
        let body = response
            .into_body()
            .collect()
            .await
            .expect("stats body reads")
            .to_bytes();

        assert_eq!(
            body.as_ref(),
            b"subscribe_route_calls=2 wake_instance_calls=0 backend_http_requests=0 backend_websocket_sessions=0\n"
        );
    }

    #[tokio::test]
    async fn backend_returns_grpc_shaped_response_for_grpc_requests() {
        let stats = SmokeStats::default();

        let response = backend_response(
            HttpRequest::builder()
                .uri("/smoke")
                .header(CONTENT_TYPE, "application/grpc")
                .body(Full::new(Bytes::from_static(GRPC_BODY)))
                .expect("gRPC-shaped request builds"),
            &stats,
            DEFAULT_WEBSOCKET_STREAM_BYTES,
            DEFAULT_WEBSOCKET_STREAM_CHUNK_SIZE,
        )
        .await;
        assert_eq!(
            response.headers().get(CONTENT_TYPE).expect("content-type"),
            "application/grpc"
        );
        assert!(response.headers().get("grpc-status").is_none());

        let collected = response
            .into_body()
            .collect()
            .await
            .expect("gRPC-shaped body reads");
        let trailers = collected.trailers().expect("gRPC-shaped trailers");
        assert_eq!(trailers.get("grpc-status").expect("status"), "0");

        let body = collected.to_bytes();
        assert_eq!(body.as_ref(), GRPC_BODY);
    }

    #[test]
    fn response_validation_accepts_expected_http_response() {
        let mut response = b"HTTP/1.1 200 OK\r\ncontent-length: 24\r\n\r\n".to_vec();
        response.extend_from_slice(BACKEND_BODY);

        validate_response(&response).expect("response validates");
    }
}
