use std::{io::ErrorKind, net::SocketAddr, time::Duration};

use proxy_core::{
    proxy_streams_with_timeouts, DrainError, DrainTracker, TcpProxy, TcpProxyConfig, TcpProxyError,
};
use socket2::SockRef;
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{advance, timeout},
};

const REQUEST: &[u8] = b"preserve these bytes across the proxy";
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn tcp_proxy_preserves_bytes_and_tracks_connection_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");

        let mut received = vec![0; REQUEST.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("upstream reads proxied bytes");
        assert_eq!(received, REQUEST);

        stream
            .write_all(&received)
            .await
            .expect("upstream writes response bytes");

        let mut eof = [0; 1];
        assert_eq!(stream.read(&mut eof).await.expect("upstream reads eof"), 0);
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener
        .local_addr()
        .expect("proxy listener has address");

    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::from_secs(1),
            ..TcpProxyConfig::default()
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");

    client
        .write_all(REQUEST)
        .await
        .expect("client writes request bytes");

    drain.wait_for_active_count(1).await;
    assert_eq!(drain.active_count(), 1);

    let mut echoed = vec![0; REQUEST.len()];
    client
        .read_exact(&mut echoed)
        .await
        .expect("client reads echoed bytes");
    assert_eq!(echoed, REQUEST);

    client.shutdown().await.expect("client half-closes");

    proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completed successfully");

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn tcp_proxy_bounds_repeated_upstream_refusal_and_releases_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("temporary upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("temporary upstream listener has address");
    drop(upstream_listener);

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::from_secs(1),
            ..TcpProxyConfig::default()
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    let mut eof = [0; 1];
    assert_eq!(client.read(&mut eof).await.expect("client reads eof"), 0);

    let error = proxy_task
        .await
        .expect("proxy task completed")
        .expect_err("proxy reports upstream connect failure");
    assert!(matches!(error, TcpProxyError::ConnectTimeout { .. }));
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn tcp_proxy_surfaces_deterministic_connect_timeout_and_releases_lifecycle() {
    let upstream_addr = SocketAddr::from(([192, 0, 2, 1], 80));

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::ZERO,
            ..TcpProxyConfig::default()
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    let mut eof = [0; 1];
    assert_eq!(
        timeout(TEST_TIMEOUT, client.read(&mut eof))
            .await
            .expect("client observes timeout close before deadline")
            .expect("client reads eof after connect timeout"),
        0
    );

    let error = proxy_task
        .await
        .expect("proxy task completed")
        .expect_err("proxy reports deterministic upstream connect timeout");
    assert!(matches!(
        error,
        TcpProxyError::ConnectTimeout {
            timeout: Duration::ZERO
        }
    ));
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn tcp_proxy_releases_lifecycle_after_client_disconnect() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");

        let mut received = vec![0; REQUEST.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("upstream reads proxied bytes");
        assert_eq!(received, REQUEST);

        let mut eof = [0; 1];
        assert_eq!(stream.read(&mut eof).await.expect("upstream reads eof"), 0);
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::from_secs(1),
            ..TcpProxyConfig::default()
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(REQUEST)
        .await
        .expect("client writes request bytes");
    drain.wait_for_active_count(1).await;
    drop(client);

    proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completes after client disconnect");

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn tcp_proxy_releases_lifecycle_after_upstream_disconnect() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        drop(stream);
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::from_secs(1),
            ..TcpProxyConfig::default()
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    drain.wait_for_active_count(1).await;

    let mut eof = [0; 1];
    assert_eq!(
        timeout(Duration::from_secs(2), client.read(&mut eof))
            .await
            .expect("client observes upstream disconnect before timeout")
            .expect("client reads propagated upstream eof"),
        0
    );
    client
        .shutdown()
        .await
        .expect("client half-closes after propagated upstream eof");

    timeout(Duration::from_secs(2), proxy_task)
        .await
        .expect("proxy task completes after client half-close")
        .expect("proxy task joined")
        .expect("proxy completes cleanly after upstream disconnect");
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn tcp_proxy_reports_os_level_upstream_reset_and_releases_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let (upstream_reset_tx, upstream_reset_rx) = tokio::sync::oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");

        let mut received = vec![0; REQUEST.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("upstream reads proxied bytes before reset");
        assert_eq!(received, REQUEST);

        SockRef::from(&stream)
            .set_linger(Some(Duration::ZERO))
            .expect("upstream enables reset-on-close linger");
        upstream_reset_tx
            .send(())
            .expect("test waits for upstream reset");
        drop(stream);
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::from_secs(1),
            ..TcpProxyConfig::default()
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(REQUEST)
        .await
        .expect("client writes request bytes");
    drain.wait_for_active_count(1).await;
    upstream_reset_rx
        .await
        .expect("upstream performed reset close");

    let error = timeout(TEST_TIMEOUT, proxy_task)
        .await
        .expect("proxy reports upstream reset before timeout")
        .expect("proxy task completed")
        .expect_err("proxy reports OS-level upstream reset");
    match error {
        TcpProxyError::Proxy(error) => assert_eq!(error.kind(), ErrorKind::ConnectionReset),
        other => panic!("expected proxy reset error, got {other:?}"),
    }

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn tcp_proxy_stream_backpressure_stalls_until_upstream_reads() {
    let (mut client, proxy_client) = duplex(64);
    let (proxy_upstream, mut upstream) = duplex(64);
    let payload = vec![b'x'; 1024];
    let expected = payload.clone();

    let mut proxy_task = tokio::spawn(proxy_streams_with_timeouts(
        proxy_client,
        proxy_upstream,
        None,
        Duration::from_secs(3600),
        None,
    ));
    let mut writer = tokio::spawn(async move {
        client
            .write_all(&payload)
            .await
            .expect("client eventually writes full payload");
        client.shutdown().await.expect("client half-closes");
    });

    assert!(
        timeout(Duration::from_millis(25), &mut writer)
            .await
            .is_err(),
        "the relay stops reading the client while the upstream is full"
    );
    assert!(!proxy_task.is_finished());

    let mut received = vec![0; expected.len()];
    upstream
        .read_exact(&mut received)
        .await
        .expect("upstream drains stalled payload");
    assert_eq!(received, expected);
    writer.await.expect("writer task completed");
    upstream.shutdown().await.expect("upstream half-closes");

    (&mut proxy_task)
        .await
        .expect("proxy task completed")
        .expect("proxy completes after both halves close");
}

#[tokio::test]
async fn tcp_proxy_stream_allows_reverse_bytes_after_client_half_close() {
    let (mut client, proxy_client) = duplex(64);
    let (proxy_upstream, mut upstream) = duplex(64);
    let request = b"request before half-close";
    let response = b"response after client half-close";

    let proxy_task = tokio::spawn(proxy_streams_with_timeouts(
        proxy_client,
        proxy_upstream,
        None,
        Duration::from_secs(3600),
        None,
    ));
    client
        .write_all(request)
        .await
        .expect("client writes request bytes");
    client.shutdown().await.expect("client half-closes");

    let mut received = vec![0; request.len()];
    upstream
        .read_exact(&mut received)
        .await
        .expect("upstream reads request bytes");
    assert_eq!(received, request);
    let mut eof = [0; 1];
    assert_eq!(
        timeout(TEST_TIMEOUT, upstream.read(&mut eof))
            .await
            .expect("upstream observes propagated client half-close before timeout")
            .expect("upstream reads client half-close"),
        0
    );

    upstream
        .write_all(response)
        .await
        .expect("upstream writes response after client half-close");
    upstream.shutdown().await.expect("upstream half-closes");

    let mut echoed = vec![0; response.len()];
    client
        .read_exact(&mut echoed)
        .await
        .expect("client reads response after half-close");
    assert_eq!(echoed, response);

    proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completes after both halves close");
}

#[tokio::test(start_paused = true)]
async fn tcp_proxy_stream_idle_timeout_fires_when_both_directions_are_idle() {
    let (_client, proxy_client) = duplex(64);
    let (proxy_upstream, _upstream) = duplex(64);
    let idle_timeout = Duration::from_secs(5);

    // With no bytes flowing either way, the shared stream idle timeout should
    // fail the session with the standard TimedOut taxonomy.
    let proxy_task = tokio::spawn(proxy_streams_with_timeouts(
        proxy_client,
        proxy_upstream,
        Some(idle_timeout),
        idle_timeout,
        None,
    ));

    advance(idle_timeout).await;

    let error = proxy_task
        .await
        .expect("proxy task completed")
        .expect_err("idle proxy session times out");
    assert_eq!(error.kind(), ErrorKind::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn tcp_proxy_stream_one_way_activity_keeps_session_alive() {
    let (mut client, proxy_client) = duplex(64);
    let (proxy_upstream, mut upstream) = duplex(64);
    let idle_timeout = Duration::from_secs(5);

    // Reverse-direction traffic is enough to refresh the shared idle clock, so
    // a quiet client-to-upstream half must not kill the whole stream.
    let proxy_task = tokio::spawn(proxy_streams_with_timeouts(
        proxy_client,
        proxy_upstream,
        Some(idle_timeout),
        idle_timeout,
        None,
    ));

    for byte in *b"abc" {
        advance(idle_timeout - Duration::from_secs(1)).await;
        upstream
            .write_all(&[byte])
            .await
            .expect("upstream writes one-way activity");

        let mut received = [0; 1];
        client
            .read_exact(&mut received)
            .await
            .expect("client receives one-way activity");
        assert_eq!(received, [byte]);
        assert!(
            !proxy_task.is_finished(),
            "one-way activity should keep the proxy session alive"
        );
    }

    drop(client);
    drop(upstream);
    proxy_task.await.expect("proxy task joined").ok();
}

#[tokio::test]
async fn tcp_proxy_drain_times_out_while_connection_is_stalled_then_releases() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let (release_upstream_tx, release_upstream_rx) = tokio::sync::oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        release_upstream_rx
            .await
            .expect("test releases stalled upstream");
        let mut sink = Vec::new();
        stream
            .read_to_end(&mut sink)
            .await
            .expect("upstream drains request");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_millis(25));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::from_secs(1),
            ..TcpProxyConfig::default()
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client.write_all(b"held open").await.expect("client writes");
    drain.wait_for_active_count(1).await;
    drain.start_drain();

    let error = drain
        .wait_for_idle()
        .await
        .expect_err("stalled connection exceeds drain grace");
    assert_eq!(
        error,
        DrainError::GraceTimeout {
            timeout: Duration::from_millis(25),
            active: 1,
        }
    );

    client.shutdown().await.expect("client half-closes");
    release_upstream_tx
        .send(())
        .expect("upstream release signal sends");
    proxy_task.await.expect("proxy task completed").ok();
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn tcp_idle_deadline_terminates_both_directions_blocked_on_write() {
    let (mut client, proxy_client) = tokio::io::duplex(1024);
    let (mut upstream, proxy_upstream) = tokio::io::duplex(1024);
    let idle = Duration::from_secs(5);
    let proxy = tokio::spawn(proxy_streams_with_timeouts(
        proxy_client,
        proxy_upstream,
        Some(idle),
        idle,
        None,
    ));
    let client_writer = tokio::spawn(async move { client.write_all(&vec![1; 128 * 1024]).await });
    let upstream_writer =
        tokio::spawn(async move { upstream.write_all(&vec![2; 128 * 1024]).await });
    tokio::task::yield_now().await;
    advance(idle).await;
    let error = timeout(Duration::from_secs(1), proxy)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    client_writer.abort();
    upstream_writer.abort();
}
