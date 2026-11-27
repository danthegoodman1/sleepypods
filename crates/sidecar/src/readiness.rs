//! Private transport readiness, separate from proxy traffic and idle accounting.

use std::{convert::Infallible, net::SocketAddr, time::Duration};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use proxy_core::{serve_listener, Accept, ProxyAdmission, ProxyResourceConfig, Shutdown};
use tokio::net::TcpStream;

const HEALTH_IO_TIMEOUT: Duration = Duration::from_secs(1);
const APP_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

/// The caller must bind its proxy listener and complete initial CP connection before
/// binding this listener. No health request opens the proxy's public forwarding port.
pub async fn serve_readiness(listener: impl Accept, app_addr: SocketAddr, shutdown: Shutdown) {
    let config = ProxyResourceConfig::default()
        .with_limits(8, 8, 8, 1)
        .expect("fixed health limits are valid")
        .with_timeouts(HEALTH_IO_TIMEOUT, HEALTH_IO_TIMEOUT, HEALTH_IO_TIMEOUT)
        .expect("fixed health deadlines are valid");
    let admission = ProxyAdmission::new(config);
    let mut connections = serve_listener(listener, &shutdown, |stream, _| {
        let Ok(handshake) = admission.handshakes.try_acquire() else {
            return Err(stream);
        };
        let io = admission.admit_io(stream)?;
        let connection = proxy_core::serve_http_connection_admitted(
            io,
            shutdown.clone(),
            admission.clone(),
            handshake,
            None,
            move |request| readiness_response(request, app_addr),
        );
        // Health exchanges are tiny and have a total one-second budget, so
        // idle keepalive clients cannot occupy every health slot indefinitely.
        Ok(async move {
            let _ = tokio::time::timeout(HEALTH_IO_TIMEOUT, connection).await;
        })
    })
    .await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn readiness_response(
    request: Request<Incoming>,
    app_addr: SocketAddr,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let status = if request.uri().path() != "/ready" {
        StatusCode::NOT_FOUND
    } else if request.method() != Method::GET {
        StatusCode::METHOD_NOT_ALLOWED
    } else {
        match tokio::time::timeout(APP_CONNECT_TIMEOUT, TcpStream::connect(app_addr)).await {
            Ok(Ok(_stream)) => StatusCode::OK,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        }
    };
    Ok(Response::builder()
        .status(status)
        .header(http::header::CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::new()))
        .expect("fixed readiness response"))
}

#[cfg(test)]
mod tests;
