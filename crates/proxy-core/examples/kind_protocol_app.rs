use std::{
    convert::Infallible,
    env,
    error::Error,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{header::HOST, HeaderMap, Request, Response, StatusCode, Version};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::{
    body::Incoming,
    server::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{
            Callback, ErrorResponse, Request as WsRequest, Response as WsResponse,
        },
        Message,
    },
};

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type AppBody = UnsyncBoxBody<Bytes, Infallible>;

const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const MAX_HTTP1_HEADER_BYTES: usize = 64 * 1024;

#[tokio::main]
async fn main() -> AppResult<()> {
    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream).await {
                eprintln!("kind protocol app connection failed: {error}");
            }
        });
    }
}

async fn serve_connection(stream: TcpStream) -> AppResult<()> {
    let Some(accepted) = detect_protocol(stream).await? else {
        return Ok(());
    };

    match accepted.protocol {
        AcceptedProtocol::Http2 => serve_http2(accepted.stream).await,
        AcceptedProtocol::WebSocket => serve_websocket(accepted.stream).await,
        AcceptedProtocol::Http1 => serve_http1(accepted.stream).await,
    }
}

async fn serve_http2(stream: PrefixedTcpStream) -> AppResult<()> {
    http2::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(stream), service_fn(handle_http))
        .await?;
    Ok(())
}

async fn serve_http1(stream: PrefixedTcpStream) -> AppResult<()> {
    http1::Builder::new()
        .keep_alive(false)
        .serve_connection(TokioIo::new(stream), service_fn(handle_http))
        .await?;
    Ok(())
}

async fn handle_http(mut request: Request<Incoming>) -> Result<Response<AppBody>, Infallible> {
    let instance = instance();
    let path = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_owned();

    if request.uri().path() == "/grpc.Test/Echo" {
        let body = request
            .body_mut()
            .collect()
            .await
            .map(|collected| collected.to_bytes())
            .unwrap_or_default();
        return Ok(grpc_response(&instance, &path, request.version(), &body));
    }

    let body = format!(
        "sleepypods-protocol-app\ninstance={instance}\nprotocol=http2\nrequest_version={}\npath={path}\nhost={}\n",
        version_name(request.version()),
        authority(&request),
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .body(boxed_full(body))
        .expect("HTTP response builds"))
}

fn grpc_response(
    instance: &str,
    path: &str,
    version: Version,
    request_body: &Bytes,
) -> Response<AppBody> {
    let payload = format!(
        "sleepypods-protocol-app\ninstance={instance}\nprotocol=h2c-grpc\nrequest_version={}\npath={path}\nrequest_body_hex={}\n",
        version_name(version),
        hex(request_body),
    );
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload.as_bytes());

    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", "0".parse().expect("static status"));
    trailers.insert(
        "grpc-message",
        format!("ok-{instance}").parse().expect("grpc message"),
    );

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .body(GrpcBody::new(Bytes::from(framed), trailers).boxed_unsync())
        .expect("gRPC-shaped response builds")
}

async fn serve_websocket(stream: PrefixedTcpStream) -> AppResult<()> {
    let instance = instance();
    let mut path = String::new();
    let mut websocket = accept_hdr_async(stream, CaptureWebSocketPath { path: &mut path }).await?;

    while let Some(message) = websocket.next().await {
        match message? {
            Message::Text(text) => {
                websocket
                    .send(Message::Text(
                        format!(
                            "sleepypods-protocol-app\ninstance={instance}\nprotocol=websocket\npath={path}\ntext={text}\n"
                        )
                        .into(),
                    ))
                    .await?;
            }
            Message::Binary(bytes) => {
                let mut response = format!(
                    "sleepypods-protocol-app\ninstance={instance}\nprotocol=websocket\npath={path}\nbinary="
                )
                .into_bytes();
                response.extend_from_slice(&bytes);
                websocket.send(Message::Binary(response.into())).await?;
            }
            Message::Ping(bytes) => websocket.send(Message::Pong(bytes)).await?,
            // tungstenite queues the close reply; the next read sends it and ends the stream.
            Message::Close(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    Ok(())
}

struct CaptureWebSocketPath<'a> {
    path: &'a mut String,
}

impl Callback for CaptureWebSocketPath<'_> {
    fn on_request(
        self,
        request: &WsRequest,
        response: WsResponse,
    ) -> Result<WsResponse, ErrorResponse> {
        *self.path = request
            .uri()
            .path_and_query()
            .map(|value| value.as_str().to_owned())
            .unwrap_or_else(|| "/".to_owned());
        Ok(response)
    }
}

fn boxed_full(body: String) -> AppBody {
    Full::new(Bytes::from(body))
        .map_err(|error| match error {})
        .boxed_unsync()
}

#[derive(Debug)]
struct GrpcBody {
    data: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl GrpcBody {
    fn new(data: Bytes, trailers: HeaderMap) -> Self {
        Self {
            data: Some(data),
            trailers: Some(trailers),
        }
    }
}

impl Body for GrpcBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(data) = self.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        if let Some(trailers) = self.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none() && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        self.data
            .as_ref()
            .map(|data| SizeHint::with_exact(data.len() as u64))
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug)]
enum AcceptedProtocol {
    Http1,
    Http2,
    WebSocket,
}

#[derive(Debug)]
struct AcceptedStream {
    protocol: AcceptedProtocol,
    stream: PrefixedTcpStream,
}

async fn detect_protocol(mut stream: TcpStream) -> AppResult<Option<AcceptedStream>> {
    let mut prefix = Vec::with_capacity(HTTP2_PREFACE.len());

    loop {
        if prefix == HTTP2_PREFACE {
            return Ok(Some(accepted(AcceptedProtocol::Http2, prefix, stream)));
        }
        if !HTTP2_PREFACE.starts_with(&prefix) {
            break;
        }

        let Some(byte) = read_one(&mut stream).await? else {
            return Ok(None);
        };
        prefix.push(byte);
    }

    while !headers_complete(&prefix) && prefix.len() < MAX_HTTP1_HEADER_BYTES {
        let Some(byte) = read_one(&mut stream).await? else {
            return Ok(None);
        };
        prefix.push(byte);
    }

    let protocol = if is_websocket_upgrade(&prefix) {
        AcceptedProtocol::WebSocket
    } else {
        AcceptedProtocol::Http1
    };
    Ok(Some(accepted(protocol, prefix, stream)))
}

async fn read_one(stream: &mut TcpStream) -> io::Result<Option<u8>> {
    let mut byte = [0; 1];
    match stream.read_exact(&mut byte).await {
        Ok(_) => Ok(Some(byte[0])),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(error),
    }
}

fn accepted(protocol: AcceptedProtocol, prefix: Vec<u8>, stream: TcpStream) -> AcceptedStream {
    AcceptedStream {
        protocol,
        stream: PrefixedTcpStream::new(prefix, stream),
    }
}

fn headers_complete(buffer: &[u8]) -> bool {
    buffer.windows(4).any(|window| window == b"\r\n\r\n")
}

fn is_websocket_upgrade(buffer: &[u8]) -> bool {
    let headers = String::from_utf8_lossy(buffer);
    let mut has_connection_upgrade = false;
    let mut has_upgrade_websocket = false;

    for line in headers.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_ascii_lowercase();

        if name == "connection"
            && value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        {
            has_connection_upgrade = true;
        }

        if name == "upgrade" && value == "websocket" {
            has_upgrade_websocket = true;
        }
    }

    has_connection_upgrade && has_upgrade_websocket
}

#[derive(Debug)]
struct PrefixedTcpStream {
    prefix: Vec<u8>,
    prefix_offset: usize,
    stream: TcpStream,
}

impl PrefixedTcpStream {
    fn new(prefix: Vec<u8>, stream: TcpStream) -> Self {
        Self {
            prefix,
            prefix_offset: 0,
            stream,
        }
    }
}

impl AsyncRead for PrefixedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.prefix_offset < self.prefix.len() {
            let available = &self.prefix[self.prefix_offset..];
            let copy_len = available.len().min(buffer.remaining());
            buffer.put_slice(&available[..copy_len]);
            self.prefix_offset += copy_len;
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for PrefixedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

fn instance() -> String {
    env::var("SLEEPYPODS_E2E_INSTANCE").unwrap_or_else(|_| "unknown".to_owned())
}

fn authority(request: &Request<Incoming>) -> String {
    request
        .uri()
        .authority()
        .map(|authority| authority.as_str().to_owned())
        .or_else(|| {
            request
                .headers()
                .get(HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

fn version_name(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "unknown",
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

#[cfg(test)]
#[path = "kind_protocol_app/tests.rs"]
mod tests;
