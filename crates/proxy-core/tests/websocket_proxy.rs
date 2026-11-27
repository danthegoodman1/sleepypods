mod support;

use std::{net::SocketAddr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use proxy_core::{
    DrainError, DrainTracker, WebSocketProxy, WebSocketProxyConfig, WebSocketProxyError,
};
use support::{header, proxy_websocket_connection, read_head, spawn_websocket_proxy};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{
        handshake::derive_accept_key, protocol::CloseFrame, Bytes as WsBytes,
        Error as TungsteniteError, Message,
    },
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

async fn upstream_listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

fn proxy(drain: &DrainTracker) -> WebSocketProxy {
    WebSocketProxy::new(drain.clone())
}

fn drain() -> DrainTracker {
    DrainTracker::new(Duration::from_secs(5))
}

/// Answers one raw upgrade request with `101` plus `extra` header lines and
/// returns the request head with the upgraded socket.
async fn accept_raw_upgrade(listener: TcpListener, extra: &str) -> (String, TcpStream) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let head = read_head(&mut stream).await;
    let accept = derive_accept_key(header(&head, "sec-websocket-key").unwrap().as_bytes());
    stream
        .write_all(
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
                 Sec-WebSocket-Accept: {accept}\r\n{extra}\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    (head, stream)
}

/// Sends a raw upgrade request with `extra` header lines and returns the
/// response head with the upgraded socket.
async fn raw_upgrade(proxy_addr: SocketAddr, extra: &str) -> (String, TcpStream) {
    let mut client = TcpStream::connect(proxy_addr).await.unwrap();
    client
        .write_all(
            format!(
                "GET /socket?room=1 HTTP/1.1\r\nHost: chat.example.com\r\nConnection: Upgrade\r\n\
                 Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
                 Sec-WebSocket-Key: {CLIENT_KEY}\r\n{extra}\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let head = read_head(&mut client).await;
    (head, client)
}

#[tokio::test]
async fn websocket_tunnel_relays_messages_close_and_tracks_lifecycle() {
    let (listener, upstream_addr) = upstream_listener().await;
    let (first_message_tx, first_message_rx) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();

        let first = websocket.next().await.unwrap().unwrap();
        assert_eq!(first, Message::Text("client text".into()));
        first_message_tx.send(()).unwrap();
        websocket
            .send(Message::Text("upstream text".into()))
            .await
            .unwrap();

        let second = websocket.next().await.unwrap().unwrap();
        assert_eq!(
            second,
            Message::Binary(WsBytes::from_static(b"client bytes"))
        );
        websocket
            .send(Message::Binary(WsBytes::from_static(b"upstream bytes")))
            .await
            .unwrap();

        let close = websocket.next().await.unwrap().unwrap();
        assert!(matches!(close, Message::Close(Some(frame)) if frame.reason == "done"));
        let _ = websocket.flush().await;
    });
    let drain = drain();
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;

    let (mut client, _) = connect_async(format!("ws://{proxy_addr}")).await.unwrap();
    client
        .send(Message::Text("client text".into()))
        .await
        .unwrap();
    first_message_rx.await.unwrap();
    assert_eq!(drain.active_count(), 1);
    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::Text("upstream text".into())
    );
    client
        .send(Message::Binary(WsBytes::from_static(b"client bytes")))
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::Binary(WsBytes::from_static(b"upstream bytes"))
    );
    client
        .send(Message::Close(Some(CloseFrame {
            code: 1000.into(),
            reason: "done".into(),
        })))
        .await
        .unwrap();
    let close_response = client.next().await.unwrap().unwrap();
    assert!(matches!(close_response, Message::Close(Some(frame)) if frame.reason == "done"));
    drop(client);

    upstream_task.await.unwrap();
    let _ = timeout(TEST_TIMEOUT, proxy_task).await.unwrap().unwrap();
    drain.wait_for_active_count(0).await;
}

#[tokio::test]
async fn handshake_fields_and_upgraded_bytes_pass_through_unchanged() {
    let (listener, upstream_addr) = upstream_listener().await;
    let upstream_task = tokio::spawn(async move {
        let (head, mut stream) = accept_raw_upgrade(
            listener,
            "Sec-WebSocket-Protocol: chat.v2\r\n\
             Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover\r\n\
             Set-Cookie: session=a\r\nSet-Cookie: theme=b\r\nKeep-Alive: timeout=5\r\n",
        )
        .await;
        let mut frame = [0_u8; 6];
        stream.read_exact(&mut frame).await.unwrap();
        stream.write_all(b"\xc1\x02ok").await.unwrap();
        (head, frame)
    });
    let drain = drain();
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;

    let (response, mut client) = raw_upgrade(
        proxy_addr,
        "Authorization: Bearer token-1\r\nCookie: session=a; theme=b\r\n\
         Origin: https://chat.example.com\r\nSec-WebSocket-Protocol: chat.v1, chat.v2\r\n\
         Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\
         X-Forwarded-Host: edge.example.com\r\nX-App: custom\r\nKeep-Alive: timeout=9\r\n",
    )
    .await;
    // A compressed frame travels as the bytes the client wrote.
    client.write_all(b"\xc1\x84abcd").await.unwrap();
    let mut reply = [0_u8; 4];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"\xc1\x02ok");

    let (request, frame) = upstream_task.await.unwrap();
    assert_eq!(&frame, b"\xc1\x84abcd");
    assert!(request.starts_with("GET /socket?room=1 HTTP/1.1\r\n"));
    for (name, value) in [
        ("host", "chat.example.com"),
        ("authorization", "Bearer token-1"),
        ("cookie", "session=a; theme=b"),
        ("origin", "https://chat.example.com"),
        ("sec-websocket-key", CLIENT_KEY),
        ("sec-websocket-version", "13"),
        ("sec-websocket-protocol", "chat.v1, chat.v2"),
        (
            "sec-websocket-extensions",
            "permessage-deflate; client_max_window_bits",
        ),
        ("x-forwarded-host", "edge.example.com"),
        ("x-app", "custom"),
        ("upgrade", "websocket"),
    ] {
        assert_eq!(header(&request, name), Some(value), "request {name}");
    }
    assert_eq!(header(&request, "keep-alive"), None, "hop-by-hop request");

    assert!(response.starts_with("HTTP/1.1 101 "));
    for (name, value) in [
        (
            "sec-websocket-accept",
            derive_accept_key(CLIENT_KEY.as_bytes()).as_str(),
        ),
        ("sec-websocket-protocol", "chat.v2"),
        (
            "sec-websocket-extensions",
            "permessage-deflate; server_no_context_takeover",
        ),
        ("upgrade", "websocket"),
    ] {
        assert_eq!(header(&response, name), Some(value), "response {name}");
    }
    assert_eq!(header(&response, "keep-alive"), None, "hop-by-hop response");
    let cookies = response
        .lines()
        .filter(|line| line.to_ascii_lowercase().starts_with("set-cookie:"))
        .count();
    assert_eq!(cookies, 2, "repeated Set-Cookie fields are preserved");

    drop(client);
    let _ = timeout(TEST_TIMEOUT, proxy_task).await.unwrap().unwrap();
    drain.wait_for_active_count(0).await;
}

#[tokio::test]
async fn a_withheld_final_fragment_reaches_the_upstream_as_it_arrives() {
    const FRAGMENT: usize = 64 * 1024;
    const FRAGMENTS: usize = 64;
    let (listener, upstream_addr) = upstream_listener().await;
    let upstream_task = tokio::spawn(async move {
        let (_, mut stream) = accept_raw_upgrade(listener, "").await;
        // Each fragment: a 10-byte header, a 4-byte mask and its payload.
        let mut received = vec![0_u8; FRAGMENTS * (FRAGMENT + 14)];
        stream.read_exact(&mut received).await.unwrap();
        stream
    });
    let drain = drain();
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;

    let (_, mut client) = raw_upgrade(proxy_addr, "").await;
    // One 4 MiB message as non-final fragments. The final fragment never comes.
    let mut fragment = vec![0x02, 0xff];
    fragment.extend_from_slice(&(FRAGMENT as u64).to_be_bytes());
    fragment.extend_from_slice(&[0; 4]);
    fragment.resize(FRAGMENT + 14, 7);
    for index in 0..FRAGMENTS {
        fragment[0] = if index == 0 { 0x02 } else { 0x00 };
        client.write_all(&fragment).await.unwrap();
    }

    // The relay holds no message: every fragment has already left it.
    let upstream = timeout(TEST_TIMEOUT, upstream_task).await.unwrap().unwrap();
    assert_eq!(drain.active_count(), 1);

    drop((client, upstream));
    let _ = timeout(TEST_TIMEOUT, proxy_task).await.unwrap().unwrap();
    drain.wait_for_active_count(0).await;
}

#[tokio::test]
async fn a_stalled_direction_leaves_the_other_direction_flowing() {
    const DOWNLOAD: usize = 4 * 1024 * 1024;
    let (listener, upstream_addr) = upstream_listener().await;
    let upstream_task = tokio::spawn(async move {
        let (_, mut stream) = accept_raw_upgrade(listener, "").await;
        // The app writes its whole payload before it reads anything.
        stream.write_all(&vec![5_u8; DOWNLOAD]).await.unwrap();
        stream
    });
    let drain = drain();
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;

    let (_, client) = raw_upgrade(proxy_addr, "").await;
    let (mut client_read, mut client_write) = client.into_split();
    // The client keeps sending. The unread upload fills every buffer on the
    // way to the app and stalls there.
    let upload = tokio::spawn(async move {
        let chunk = vec![3_u8; 64 * 1024];
        while client_write.write_all(&chunk).await.is_ok() {}
    });

    let mut received = vec![0_u8; DOWNLOAD];
    timeout(TEST_TIMEOUT, client_read.read_exact(&mut received))
        .await
        .expect("the download completes while the upload is stalled")
        .unwrap();
    assert!(received.iter().all(|byte| *byte == 5));

    upload.abort();
    drop((client_read, upstream_task.await.unwrap()));
    let _ = timeout(TEST_TIMEOUT, proxy_task).await.unwrap().unwrap();
    drain.wait_for_active_count(0).await;
}

#[tokio::test]
async fn upstream_rejection_keeps_its_status_headers_and_body() {
    let (listener, upstream_addr) = upstream_listener().await;
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_head(&mut stream).await;
        stream
            .write_all(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\nX-Reason: policy\r\n\
                  Connection: close\r\n\r\ndenied",
            )
            .await
            .unwrap();
    });
    let drain = drain();
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;

    let error = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect_err("upstream rejection is returned before 101");
    let TungsteniteError::Http(response) = error else {
        panic!("expected upstream HTTP rejection");
    };
    assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
    assert_eq!(response.headers()["x-reason"], "policy");
    assert_eq!(response.body().as_deref(), Some(b"denied".as_slice()));

    let error = timeout(TEST_TIMEOUT, proxy_task)
        .await
        .unwrap()
        .unwrap()
        .expect_err("proxy reports the rejection");
    assert!(matches!(error, WebSocketProxyError::UpstreamRejected(_)));
    upstream_task.await.unwrap();
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn an_upstream_that_switches_to_another_protocol_is_a_bad_gateway() {
    let (listener, upstream_addr) = upstream_listener().await;
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_head(&mut stream).await;
        stream
            .write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n",
            )
            .await
            .unwrap();
        stream
    });
    let drain = drain();
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;

    let (response, _client) = raw_upgrade(proxy_addr, "").await;
    assert!(response.starts_with("HTTP/1.1 502 "), "{response}");
    assert!(proxy_task.await.unwrap().is_err());
    drop(upstream_task.await.unwrap());
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn session_ends_and_releases_lifecycle_when_either_peer_disconnects() {
    for client_leaves in [true, false] {
        let (listener, upstream_addr) = upstream_listener().await;
        let upstream_task = tokio::spawn(async move { accept_raw_upgrade(listener, "").await.1 });
        let drain = drain();
        let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;
        let (_, mut client) = raw_upgrade(proxy_addr, "").await;
        let mut upstream = upstream_task.await.unwrap();
        assert_eq!(drain.active_count(), 1);

        let mut byte = [0_u8; 1];
        if client_leaves {
            drop(client);
            assert_eq!(upstream.read(&mut byte).await.unwrap(), 0);
            drop(upstream);
        } else {
            drop(upstream);
            assert_eq!(client.read(&mut byte).await.unwrap(), 0);
            drop(client);
        }

        let _ = timeout(TEST_TIMEOUT, proxy_task).await.unwrap().unwrap();
        drain.wait_for_active_count(0).await;
    }
}

#[tokio::test]
async fn a_quiet_session_outlives_the_write_deadline_and_ends_at_its_idle_policy() {
    for idle_timeout in [None, Some(Duration::from_millis(300))] {
        let (listener, upstream_addr) = upstream_listener().await;
        let upstream_task = tokio::spawn(async move { accept_raw_upgrade(listener, "").await.1 });
        let drain = drain();
        let proxy = WebSocketProxy::with_config(
            drain.clone(),
            WebSocketProxyConfig {
                write_timeout: Duration::from_millis(50),
                idle_timeout,
                ..WebSocketProxyConfig::default()
            },
        );
        let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy, upstream_addr).await;
        let (_, mut client) = raw_upgrade(proxy_addr, "").await;
        let mut upstream = upstream_task.await.unwrap();

        // Longer than the write deadline, shorter than the idle deadline.
        tokio::time::sleep(Duration::from_millis(150)).await;
        client.write_all(b"ping").await.unwrap();
        let mut bytes = [0_u8; 4];
        upstream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"ping");

        match idle_timeout {
            None => {
                tokio::time::sleep(Duration::from_millis(450)).await;
                assert!(!proxy_task.is_finished(), "no idle deadline is configured");
                drop((client, upstream));
                let _ = timeout(TEST_TIMEOUT, proxy_task).await.unwrap().unwrap();
            }
            Some(_) => {
                let error = timeout(TEST_TIMEOUT, proxy_task)
                    .await
                    .unwrap()
                    .unwrap()
                    .expect_err("the idle deadline ends a quiet session");
                assert!(matches!(
                    error,
                    WebSocketProxyError::Proxy(error) if error.kind() == std::io::ErrorKind::TimedOut
                ));
            }
        }
        drain.wait_for_active_count(0).await;
    }
}

#[tokio::test]
async fn a_blocked_write_ends_the_session_at_the_write_deadline() {
    let (listener, upstream_addr) = upstream_listener().await;
    let upstream_task = tokio::spawn(async move { accept_raw_upgrade(listener, "").await.1 });
    let drain = drain();
    let proxy = WebSocketProxy::with_config(
        drain.clone(),
        WebSocketProxyConfig {
            write_timeout: Duration::from_millis(200),
            ..WebSocketProxyConfig::default()
        },
    );
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy, upstream_addr).await;
    let (_, mut client) = raw_upgrade(proxy_addr, "").await;
    // The app holds its socket open and never reads.
    let _upstream = upstream_task.await.unwrap();

    let chunk = vec![1_u8; 64 * 1024];
    let error = timeout(TEST_TIMEOUT, async move {
        while !proxy_task.is_finished() && client.write_all(&chunk).await.is_ok() {}
        proxy_task.await.unwrap()
    })
    .await
    .expect("the write deadline ends the session")
    .expect_err("a blocked write is an error");
    assert!(matches!(
        error,
        WebSocketProxyError::Proxy(error) if error.kind() == std::io::ErrorKind::TimedOut
    ));
    drain.wait_for_active_count(0).await;
}

#[tokio::test]
async fn draining_proxy_rejects_a_new_session_with_503() {
    let drain = drain();
    drain.start_drain();
    let (client, mut peer) = tokio::io::duplex(1024);
    let task = tokio::spawn(proxy_websocket_connection(
        proxy(&drain),
        client,
        "http://127.0.0.1:1".parse().unwrap(),
    ));
    peer.write_all(
        format!(
            "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {CLIENT_KEY}\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();

    assert!(read_head(&mut peer).await.starts_with("HTTP/1.1 503 "));
    assert!(matches!(
        task.await.unwrap(),
        Err(WebSocketProxyError::Drain(DrainError::Draining))
    ));
}

#[tokio::test]
async fn upstream_handshake_timeout_returns_504_and_releases_permit() {
    let (listener, upstream_addr) = upstream_listener().await;
    let (accepted, accepted_rx) = oneshot::channel();
    let app_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        accepted.send(()).unwrap();
        let _stream = stream;
        std::future::pending::<()>().await;
    });
    let drain = DrainTracker::new(Duration::from_secs(1));
    let proxy = WebSocketProxy::with_config(
        drain.clone(),
        WebSocketProxyConfig {
            handshake_timeout: Duration::from_millis(100),
            ..WebSocketProxyConfig::default()
        },
    );
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy, upstream_addr).await;
    let client = tokio::spawn(connect_async(format!("ws://{proxy_addr}")));
    accepted_rx.await.unwrap();
    let error = timeout(TEST_TIMEOUT, client)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, TungsteniteError::Http(response) if response.status() == http::StatusCode::GATEWAY_TIMEOUT)
    );
    assert!(proxy_task.await.unwrap().is_err());
    assert_eq!(drain.active_count(), 0);
    app_task.abort();
}

#[tokio::test]
async fn cancelled_handshake_releases_upstream_connection_and_permit() {
    let (listener, upstream_addr) = upstream_listener().await;
    let drain = DrainTracker::new(Duration::from_secs(1));
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;
    let client = tokio::spawn(raw_upgrade(proxy_addr, ""));
    let (mut app, _) = listener.accept().await.unwrap();
    read_head(&mut app).await;
    assert_eq!(drain.active_count(), 1);

    proxy_task.abort();
    let _ = proxy_task.await;
    client.abort();
    let _ = client.await;

    let mut bytes = Vec::new();
    timeout(TEST_TIMEOUT, app.read_to_end(&mut bytes))
        .await
        .expect("the upstream connection closes")
        .unwrap();
    drain.wait_for_active_count(0).await;
}

#[tokio::test]
async fn websocket_proxy_connects_to_an_ipv6_literal_upstream() {
    let Ok(listener) = TcpListener::bind("[::1]:0").await else {
        return;
    };
    let upstream_addr = listener.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move { accept_raw_upgrade(listener, "").await.1 });
    let drain = drain();
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy(&drain), upstream_addr).await;

    let (response, client) = raw_upgrade(proxy_addr, "").await;
    assert!(response.starts_with("HTTP/1.1 101 "));
    drop((client, upstream_task.await.unwrap()));
    let _ = timeout(TEST_TIMEOUT, proxy_task).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_peer_that_keeps_its_stream_open_after_the_other_closed_is_released_at_the_close_timeout()
{
    let (listener, upstream_addr) = upstream_listener().await;
    let upstream_task = tokio::spawn(async move { accept_raw_upgrade(listener, "").await.1 });
    let drain = drain();
    let proxy = WebSocketProxy::with_config(
        drain.clone(),
        WebSocketProxyConfig {
            close_timeout: Duration::from_millis(200),
            ..WebSocketProxyConfig::default()
        },
    );
    let (proxy_addr, proxy_task) = spawn_websocket_proxy(proxy, upstream_addr).await;
    let (_, mut client) = raw_upgrade(proxy_addr, "").await;

    // The app closes. The client sees the end of the stream and stays connected.
    drop(upstream_task.await.unwrap());
    assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);
    assert_eq!(drain.active_count(), 1);

    let started = tokio::time::Instant::now();
    timeout(TEST_TIMEOUT, proxy_task)
        .await
        .expect("the close timeout ends the session")
        .unwrap()
        .expect("a session that ends at the close timeout is not an error");
    assert!(started.elapsed() >= Duration::from_millis(150));
    drain.wait_for_active_count(0).await;
    drop(client);
}
