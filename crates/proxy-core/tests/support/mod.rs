//! Helpers that drive the entry points the production listeners call.
#![allow(dead_code)]

use std::{error::Error, future::Future, net::SocketAddr, time::Duration};

use http::{Request, Response, Uri};
use http_body::Body;
use hyper::body::Incoming;
use proxy_core::{
    websocket_error_response, AdmissionLimiter, ProxyAdmission, ProxyResourceConfig, Shutdown,
    WebSocketProxy, WebSocketProxyError,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite},
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub const WEBSOCKET_UPGRADE_REQUEST: &str = "GET /once HTTP/1.1\r\nHost: app.example.test\r\n\
    Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
    Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";

/// Serves one connection with its own default admission state.
pub async fn serve_http_connection<IO, F, Fut, B, E>(io: IO, shutdown: Shutdown, handler: F)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(Request<Incoming>) -> Fut,
    Fut: Future<Output = Result<Response<B>, E>> + Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    E: Into<BoxError>,
{
    let admission = ProxyAdmission::new(ProxyResourceConfig::default());
    let handshake = admission
        .handshakes
        .try_acquire()
        .expect("new admission has capacity");
    let io = admission.admit_io(io).expect("new admission has capacity");
    proxy_core::serve_http_connection_admitted(io, shutdown, admission, handshake, None, handler)
        .await;
}

/// Accepts one client connection and proxies its WebSocket upgrade as the
/// listeners do: the request handler prepares the upgrade and returns the
/// response, and a separate task relays the session.
pub async fn spawn_websocket_proxy(
    proxy: WebSocketProxy,
    upstream: SocketAddr,
) -> (SocketAddr, JoinHandle<Result<(), WebSocketProxyError>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let origin: Uri = format!("http://{upstream}").parse().unwrap();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        proxy_websocket_connection(proxy, stream, origin).await
    });
    (addr, task)
}

pub async fn proxy_websocket_connection<IO>(
    proxy: WebSocketProxy,
    client: IO,
    origin: Uri,
) -> Result<(), WebSocketProxyError>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (prepared, outcome) = oneshot::channel();
    let prepared = std::sync::Mutex::new(Some(prepared));
    let connection = tokio::spawn(serve_http_connection(
        client,
        Shutdown::new(),
        move |mut request| {
            let proxy = proxy.clone();
            let origin = origin.clone();
            let prepared = prepared.lock().unwrap().take();
            async move {
                let (response, outcome) = match proxy.prepare_upgrade(&mut request, &origin).await {
                    Ok((response, upgrade)) => (response, Ok(upgrade)),
                    Err(error) => {
                        let mut response = websocket_error_response(&error);
                        response.headers_mut().insert(
                            http::header::CONNECTION,
                            http::HeaderValue::from_static("close"),
                        );
                        (response, Err(error))
                    }
                };
                if let Some(prepared) = prepared {
                    let _ = prepared.send(outcome);
                }
                Ok::<_, std::convert::Infallible>(response)
            }
        },
    ));
    match outcome.await.expect("the client sent an upgrade request") {
        Ok(upgrade) => upgrade.run().await,
        Err(error) => {
            // Let the connection finish writing the error response.
            let _ = tokio::time::timeout(Duration::from_secs(2), connection).await;
            Err(error)
        }
    }
}

/// Reads an HTTP/1.1 message head, up to and including its blank line.
pub async fn read_head<IO: AsyncRead + Unpin>(io: &mut IO) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(io.read_u8().await.expect("peer sends a complete head"));
    }
    String::from_utf8(head).expect("head is UTF-8")
}

/// The value of the first `name` header in a message head.
pub fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().skip(1).find_map(|line| {
        let (field, value) = line.split_once(':')?;
        field.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

pub async fn wait_for_in_flight(limiter: &AdmissionLimiter, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while limiter.in_flight() != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "expected {expected} admitted, found {}",
            limiter.in_flight()
        )
    });
}
