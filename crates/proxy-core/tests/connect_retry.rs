mod support;

use std::{
    convert::Infallible,
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, Version};
use http_body_util::{BodyExt, Full};
use proxy_core::{DrainTracker, HttpProxy, ProxyResourceConfig, Shutdown};
use tokio::{net::TcpListener, time::timeout};

const BOUND: Duration = Duration::from_secs(2);

#[tokio::test]
async fn refused_http1_connect_waits_for_listener_and_sends_one_request() {
    refused_http_connect(Version::HTTP_11).await;
}

#[tokio::test]
async fn refused_http2_connect_waits_for_listener_and_sends_one_request() {
    refused_http_connect(Version::HTTP_2).await;
}

async fn refused_http_connect(version: Version) {
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let drain = DrainTracker::new(BOUND);
    let proxy = HttpProxy::new(drain.clone());
    let upstream = format!("http://{addr}").parse().unwrap();
    let client_proxy = proxy.clone();
    let mut request = tokio::spawn(async move {
        client_proxy
            .proxy(
                Request::builder()
                    .method("POST")
                    .version(version)
                    .uri("/one?key=blue")
                    .body(Full::new(Bytes::from_static(
                        b"non-idempotent application payload",
                    )))
                    .unwrap(),
                &upstream,
            )
            .await
    });
    assert!(
        timeout(Duration::from_millis(80), &mut request)
            .await
            .is_err(),
        "the first cold request must remain pending while TCP connect is refused"
    );
    assert_eq!(proxy.upstream_connections().in_flight(), 1);
    assert_eq!(drain.active_count(), 1);

    let listener = TcpListener::bind(addr).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let server_calls = calls.clone();
    let shutdown = Shutdown::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let connection = support::serve_http_connection(stream, server_shutdown, move |req| {
            server_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(req.version(), version);
                assert_eq!(req.uri().path_and_query().unwrap(), "/one?key=blue");
                assert_eq!(
                    req.into_body().collect().await.unwrap().to_bytes(),
                    "non-idempotent application payload"
                );
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                    b"one response",
                ))))
            }
        });
        tokio::pin!(connection);
        tokio::select! {
            () = &mut connection => {}
            duplicate = listener.accept() => panic!("unexpected duplicate connection: {duplicate:?}"),
        }
    });
    let response = timeout(BOUND, request).await.unwrap().unwrap().unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "one response"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    timeout(BOUND, drain.wait_for_active_count(0))
        .await
        .unwrap();
    shutdown.shutdown();
    timeout(BOUND, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn reset_after_dispatched_http1_request_is_not_replayed() {
    reset_after_dispatch(Version::HTTP_11).await;
}

#[tokio::test]
async fn reset_after_dispatched_http2_request_is_not_replayed() {
    reset_after_dispatch(Version::HTTP_2).await;
}

async fn reset_after_dispatch(version: Version) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let server_calls = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        socket2::SockRef::from(&stream)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        support::serve_http_connection(stream, Shutdown::new(), move |req| {
            server_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(
                    req.into_body().collect().await.unwrap().to_bytes(),
                    "commit once"
                );
                Err::<Response<Full<Bytes>>, _>(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "response lost after application commit",
                ))
            }
        })
        .await;
        assert!(
            timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "no connector retry may replay a dispatched request"
        );
    });
    let drain = DrainTracker::new(BOUND);
    let proxy = HttpProxy::new(drain.clone());
    let result = timeout(
        BOUND,
        proxy.proxy(
            Request::builder()
                .method("POST")
                .version(version)
                .uri("/commit")
                .body(Full::new(Bytes::from_static(b"commit once")))
                .unwrap(),
            &format!("http://{addr}").parse().unwrap(),
        ),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    timeout(BOUND, server).await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    timeout(BOUND, drain.wait_for_active_count(0))
        .await
        .unwrap();
}

#[tokio::test]
async fn refused_http_connect_expires_and_releases_its_single_pool_permit() {
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let drain = DrainTracker::new(BOUND);
    let config = ProxyResourceConfig::default()
        .with_timeouts(Duration::from_millis(120), BOUND, BOUND)
        .unwrap();
    let proxy = HttpProxy::with_config(drain.clone(), config);
    let result = timeout(
        BOUND,
        proxy.proxy(
            Request::new(Full::new(Bytes::new())),
            &format!("http://{addr}").parse().unwrap(),
        ),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    timeout(
        BOUND,
        support::wait_for_in_flight(proxy.upstream_connections(), 0),
    )
    .await
    .unwrap();
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn cancelled_refused_http_setup_releases_pool_and_drain() {
    for version in [Version::HTTP_11, Version::HTTP_2] {
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = reservation.local_addr().unwrap();
        drop(reservation);
        let drain = DrainTracker::new(BOUND);
        let proxy = HttpProxy::new(drain.clone());
        let client_proxy = proxy.clone();
        let request = tokio::spawn(async move {
            client_proxy
                .proxy(
                    Request::builder()
                        .version(version)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                    &format!("http://{addr}").parse().unwrap(),
                )
                .await
        });
        timeout(
            BOUND,
            support::wait_for_in_flight(proxy.upstream_connections(), 1),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        timeout(
            Duration::from_millis(250),
            support::wait_for_in_flight(proxy.upstream_connections(), 0),
        )
        .await
        .unwrap();
        assert_eq!(drain.active_count(), 0);
        let listener = TcpListener::bind(addr).await.unwrap();
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "canceled connector must not dial later"
        );
    }
}

#[tokio::test]
async fn refused_tcp_connect_waits_then_delivers_bytes_exactly_once() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = reservation.local_addr().unwrap();
    drop(reservation);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (proxy_stream, _) = listener.accept().await.unwrap();
    let drain = DrainTracker::new(BOUND);
    let proxy = proxy_core::TcpProxy::new(
        drain.clone(),
        proxy_core::TcpProxyConfig {
            connect_timeout: BOUND,
            ..Default::default()
        },
    );
    let mut task = tokio::spawn(async move { proxy.proxy(proxy_stream, upstream_addr).await });
    let mut client = client;
    client
        .write_all(b"one TCP application request")
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    assert!(timeout(Duration::from_millis(80), &mut task).await.is_err());
    assert_eq!(drain.active_count(), 1);
    let upstream = TcpListener::bind(upstream_addr).await.unwrap();
    let (mut stream, _) = timeout(BOUND, upstream.accept()).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    timeout(BOUND, stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes, b"one TCP application request");
    stream.write_all(b"one TCP response").await.unwrap();
    stream.shutdown().await.unwrap();
    bytes.clear();
    timeout(BOUND, client.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes, b"one TCP response");
    timeout(BOUND, task).await.unwrap().unwrap().unwrap();
    assert_eq!(drain.active_count(), 0);
    assert!(timeout(Duration::from_millis(100), upstream.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn refused_websocket_connect_waits_then_sends_one_handshake_and_message() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = reservation.local_addr().unwrap();
    drop(reservation);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let drain = DrainTracker::new(BOUND);
    let proxy = proxy_core::WebSocketProxy::with_config(
        drain.clone(),
        proxy_core::WebSocketProxyConfig {
            handshake_timeout: BOUND,
            ..Default::default()
        },
    );
    let proxy_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let origin = format!("http://{upstream_addr}").parse().unwrap();
        support::proxy_websocket_connection(proxy, stream, origin).await
    });
    let mut client = tokio::spawn(connect_async(format!("ws://{proxy_addr}/once")));
    assert!(timeout(Duration::from_millis(80), &mut client)
        .await
        .is_err());
    assert_eq!(drain.active_count(), 1);
    let upstream = TcpListener::bind(upstream_addr).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let server_calls = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = upstream.accept().await.unwrap();
        let mut stream = accept_async(stream).await.unwrap();
        server_calls.fetch_add(1, Ordering::SeqCst);
        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(message, Message::text("commit once"));
        stream.send(message).await.unwrap();
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        let _ = stream.flush().await;
        assert!(timeout(Duration::from_millis(100), upstream.accept())
            .await
            .is_err());
    });
    let (mut client, _) = timeout(BOUND, client).await.unwrap().unwrap().unwrap();
    client.send(Message::text("commit once")).await.unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::text("commit once")
    );
    client.close(None).await.unwrap();
    let _ = client.next().await;
    drop(client);
    let _ = timeout(BOUND, proxy_task).await.unwrap().unwrap();
    timeout(BOUND, server).await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn cancelled_refused_websocket_setup_releases_drain_and_stops_dialing() {
    use tokio::io::AsyncWriteExt;
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let drain = DrainTracker::new(BOUND);
    let proxy = proxy_core::WebSocketProxy::new(drain.clone());
    let (client, mut peer) = tokio::io::duplex(1024);
    let task = tokio::spawn(support::proxy_websocket_connection(
        proxy,
        client,
        format!("http://{addr}").parse().unwrap(),
    ));
    peer.write_all(support::WEBSOCKET_UPGRADE_REQUEST.as_bytes())
        .await
        .unwrap();
    timeout(BOUND, drain.wait_for_active_count(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // The client leaves while the proxy is still dialing.
    drop(peer);
    timeout(BOUND, drain.wait_for_active_count(0))
        .await
        .unwrap();
    task.abort();
    let listener = TcpListener::bind(addr).await.unwrap();
    assert!(timeout(Duration::from_millis(100), listener.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn cancelled_refused_tcp_setup_releases_drain_and_stops_dialing() {
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _client = tokio::net::TcpStream::connect(downstream.local_addr().unwrap())
        .await
        .unwrap();
    let (stream, _) = downstream.accept().await.unwrap();
    let drain = DrainTracker::new(BOUND);
    let proxy = proxy_core::TcpProxy::new(drain.clone(), proxy_core::TcpProxyConfig::default());
    let task = tokio::spawn(async move { proxy.proxy(stream, addr).await });
    timeout(BOUND, drain.wait_for_active_count(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(drain.active_count(), 0);
    let listener = TcpListener::bind(addr).await.unwrap();
    assert!(timeout(Duration::from_millis(100), listener.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn refused_websocket_setup_keeps_timeout_504_and_releases_drain() {
    use tokio::io::AsyncWriteExt;
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let drain = DrainTracker::new(BOUND);
    let budget = Duration::from_millis(120);
    let proxy = proxy_core::WebSocketProxy::with_config(
        drain.clone(),
        proxy_core::WebSocketProxyConfig {
            handshake_timeout: budget,
            ..Default::default()
        },
    );
    let (client, mut peer) = tokio::io::duplex(1024);
    let started = tokio::time::Instant::now();
    let task = tokio::spawn(support::proxy_websocket_connection(
        proxy,
        client,
        format!("http://{address}").parse().unwrap(),
    ));
    peer.write_all(support::WEBSOCKET_UPGRADE_REQUEST.as_bytes())
        .await
        .unwrap();
    let response = timeout(BOUND, support::read_head(&mut peer)).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 504 "), "{response}");
    assert!(started.elapsed() >= budget);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(timeout(BOUND, task).await.unwrap().unwrap().is_err());
    assert_eq!(drain.active_count(), 0);
}

/// A full accept queue drops SYNs, as a Service address does before the node
/// has programmed it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn http_connect_completes_one_short_probe_after_dropped_syns_stop() {
    use socket2::{Domain, Socket, Type};

    const OUTAGE: Duration = Duration::from_millis(300);
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    socket
        .bind(
            &"127.0.0.1:0"
                .parse::<std::net::SocketAddr>()
                .unwrap()
                .into(),
        )
        .unwrap();
    socket.listen(0).unwrap();
    socket.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(socket.into()).unwrap();
    let address = listener.local_addr().unwrap();
    let _filler = tokio::net::TcpStream::connect(address).await.unwrap();
    let server = tokio::spawn(async move {
        tokio::time::sleep(OUTAGE).await;
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(support::serve_http_connection(
                stream,
                Shutdown::new(),
                |_| async {
                    Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from_static(
                        b"ready",
                    ))))
                },
            ));
        }
    });
    let proxy = HttpProxy::new(DrainTracker::new(BOUND));
    let request = Request::builder()
        .uri("/")
        .body(Full::new(Bytes::new()))
        .unwrap();

    let started = tokio::time::Instant::now();
    let response = proxy
        .proxy(request, &format!("http://{address}").parse().unwrap())
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "ready"
    );
    assert!(elapsed >= OUTAGE, "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(900), "{elapsed:?}");
    server.abort();
}
