use super::*;
use std::{sync::Arc, time::Duration};

use http_body_util::{Empty, Limited};
use proxy_core::{
    AppHttpProxy, AppProtocol, DrainTracker, HttpProxy, ProxyAdmission, ProxyResourceConfig,
    Shutdown,
};
use tokio::{task::JoinSet, time::timeout};

async fn backend(tasks: &mut JoinSet<AppResult<()>>, connections: usize) -> AppResult<SocketAddr> {
    let listener = Arc::new(TcpListener::bind(("127.0.0.1", 0)).await?);
    let address = listener.local_addr()?;
    for _ in 0..connections {
        let listener = listener.clone();
        tasks.spawn(async move {
            let (stream, _) = listener.accept().await?;
            serve_connection(stream).await
        });
    }
    Ok(address)
}

/// One production proxy hop in front of `upstream`.
#[derive(Clone)]
enum Hop {
    /// Frontline: keeps the client's HTTP version to a sidecar.
    Frontline(Arc<HttpProxy>, http::Uri),
    /// Sidecar: speaks the protocol declared for the app port.
    Sidecar(AppHttpProxy),
}

async fn forwarder(
    tasks: &mut JoinSet<AppResult<()>>,
    upstream: SocketAddr,
    app_protocol: Option<AppProtocol>,
) -> AppResult<SocketAddr> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let drain = DrainTracker::new(Duration::from_secs(1));
    let hop = match app_protocol {
        Some(protocol) => Hop::Sidecar(AppHttpProxy::new(
            drain,
            ProxyResourceConfig::default(),
            upstream,
            protocol,
        )),
        None => Hop::Frontline(
            Arc::new(HttpProxy::new(drain)),
            format!("http://{upstream}").parse()?,
        ),
    };
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await?;
        let admission = ProxyAdmission::new(ProxyResourceConfig::default());
        let handshake = admission.handshakes.try_acquire()?;
        let stream = admission.admit_io(stream)?;
        proxy_core::serve_http_connection_admitted(
            stream,
            Shutdown::new(),
            admission,
            handshake,
            None,
            move |mut request| {
                let hop = hop.clone();
                async move {
                    let proxied = match &hop {
                        Hop::Frontline(proxy, origin) => {
                            // The edge names the client's authority for the next hop.
                            proxy_core::apply_forwarded_header_policy(
                                &mut request,
                                "127.0.0.1".parse().expect("loopback address"),
                                "http",
                            );
                            proxy
                                .proxy(request, origin)
                                .await
                                .map_err(|error| format!("{error:?}"))
                        }
                        Hop::Sidecar(proxy) => proxy
                            .proxy(request)
                            .await
                            .map_err(|error| format!("{error:?}")),
                    };
                    proxied.map_err(|error| {
                        io::Error::other(format!("production proxy error: {error}"))
                    })
                }
            },
        )
        .await;
        Ok(())
    });
    Ok(address)
}

async fn h2_requests(
    tasks: &mut JoinSet<AppResult<()>>,
    address: SocketAddr,
    expected_instance: &str,
    app_version: &str,
) -> AppResult<()> {
    let stream = TcpStream::connect(address).await?;
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .max_header_list_size(16 * 1024)
        .handshake(TokioIo::new(stream))
        .await?;
    tasks.spawn(async move {
        connection.await?;
        Ok(())
    });
    // Two requests on this exact sender prove the hop needs no request replay.
    // The app reports the version it was served in and the host it saw.
    for path in ["/a/before", "/a/after"] {
        let response = sender
            .send_request(
                Request::builder()
                    .uri(format!("http://fixture.test{path}"))
                    .body(Empty::<Bytes>::new())?,
            )
            .await?;
        if response.status() != StatusCode::OK || response.version() != Version::HTTP_2 {
            return Err("fixture did not return HTTP/2 200".into());
        }
        let body = Limited::new(response.into_body(), 64 * 1024)
            .collect()
            .await?
            .to_bytes();
        let body = std::str::from_utf8(&body)?;
        for marker in [
            "sleepypods-protocol-app".to_owned(),
            format!("instance={expected_instance}"),
            format!("request_version={app_version}"),
            format!("path={path}"),
            "host=fixture.test".to_owned(),
        ] {
            if !body.lines().any(|line| line == marker) {
                return Err(format!("response lacks exact marker {marker:?}").into());
            }
        }
    }
    Ok(())
}

async fn stop(tasks: &mut JoinSet<AppResult<()>>) -> AppResult<()> {
    tasks.abort_all();
    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Err(error) if error.is_cancelled() => {}
            Ok(Err(error)) => failure = Some(error),
            Err(error) => failure = Some(error.into()),
        }
    }
    failure.map_or(Ok(()), Err)
}

#[tokio::test]
async fn http2_client_reaches_the_app_in_its_declared_protocol_through_two_production_hops(
) -> AppResult<()> {
    for (app_protocol, app_version) in [
        (AppProtocol::H2c, "HTTP/2"),
        (AppProtocol::Http1, "HTTP/1.1"),
    ] {
        let mut tasks = JoinSet::new();
        let result = timeout(Duration::from_secs(5), async {
            // The fixture closes an HTTP/1.1 connection after each response.
            let app = backend(&mut tasks, 3).await?;
            let expected_instance = instance();
            h2_requests(&mut tasks, app, &expected_instance, "HTTP/2").await?;
            let sidecar = forwarder(&mut tasks, app, Some(app_protocol)).await?;
            let frontline = forwarder(&mut tasks, sidecar, None).await?;
            h2_requests(&mut tasks, frontline, &expected_instance, app_version).await
        })
        .await;
        let cleanup = stop(&mut tasks).await;
        result??;
        cleanup?;
    }
    Ok(())
}

#[tokio::test]
async fn websocket_response_preserves_exact_instance_path_and_message() -> AppResult<()> {
    let mut tasks = JoinSet::new();
    let result = timeout(Duration::from_secs(5), async {
        let address = backend(&mut tasks, 1).await?;
        let stream = TcpStream::connect(address).await?;
        let (mut websocket, _) =
            tokio_tungstenite::client_async("ws://fixture.test/b/ws", stream).await?;
        for text in ["before-rotation", "after-rotation"] {
            websocket.send(Message::Text(text.into())).await?;
            let received = websocket.next().await.ok_or("WebSocket closed")??.into_text()?;
            let expected = format!(
                "sleepypods-protocol-app\ninstance={}\nprotocol=websocket\npath=/b/ws\ntext={text}\n",
                instance()
            );
            if received != expected {
                return Err("WebSocket fixture markers differ".into());
            }
        }
        websocket.close(None).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await;
    let cleanup = stop(&mut tasks).await;
    result??;
    cleanup
}
