use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn request(addr: SocketAddr, path: &str) -> String {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: readiness\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn readiness_waits_for_loopback_app_and_stops_on_shutdown() {
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = reserved.local_addr().unwrap();
    drop(reserved);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(serve_readiness(listener, app_addr, shutdown.clone()));
    assert!(request(addr, "/ready").await.starts_with("HTTP/1.1 503"));
    assert!(request(addr, "/anything-else")
        .await
        .starts_with("HTTP/1.1 404"));
    let app = TcpListener::bind(app_addr).await.unwrap();
    assert!(request(addr, "/ready").await.starts_with("HTTP/1.1 200"));
    let (mut app_connection, _) = app.accept().await.unwrap();
    let mut byte = [0; 1];
    assert_eq!(
        app_connection.read(&mut byte).await.unwrap(),
        0,
        "health check sends no app request bytes"
    );
    drop(app);
    assert!(request(addr, "/ready").await.starts_with("HTTP/1.1 503"));
    let mut owned_health = TcpStream::connect(addr).await.unwrap();
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(1), owned_health.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0))
            || closed.is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionReset)
    );
}

#[tokio::test]
async fn stalled_health_connections_are_bounded_and_recover() {
    let app = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(serve_readiness(
        listener,
        app.local_addr().unwrap(),
        shutdown.clone(),
    ));
    let mut stalled = Vec::new();
    for _ in 0..8 {
        stalled.push(TcpStream::connect(addr).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut excess = TcpStream::connect(addr).await.unwrap();
    let mut byte = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), excess.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    for mut socket in stalled {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
    assert!(request(addr, "/ready").await.starts_with("HTTP/1.1 200"));
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn health_polling_does_not_create_tcp_proxy_streams_or_delay_idle_reporting() {
    use crate::{
        IdleReportConfig, ReportIdleClient, ReportIdleFuture, ReportIdleRequest, ReportIdleResponse,
    };
    use sleepypods_api::{Generation, InstanceId};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct IdleClient(Arc<AtomicUsize>);
    impl ReportIdleClient for IdleClient {
        type Error = Infallible;
        fn report_idle(
            &mut self,
            request: ReportIdleRequest,
        ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(ReportIdleResponse::Accepted {
                    instance_id: request.instance_id().clone(),
                    generation: request.generation(),
                })
            })
        }
    }
    let app = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = app.local_addr().unwrap();
    let app_task = tokio::spawn(async move {
        loop {
            let _ = app.accept().await.unwrap();
        }
    });
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = crate::runtime::SidecarRuntimeConfig::new(
        proxy.local_addr().unwrap(),
        app_addr.port(),
        InstanceId::new("tcp-health").unwrap(),
        Generation::new(1),
        IdleReportConfig::new(
            Duration::from_millis(50),
            Duration::from_millis(10),
            Duration::from_secs(10),
        )
        .unwrap(),
        Duration::from_millis(100),
    )
    .unwrap();
    let admission = config.admission().clone();
    let reports = Arc::new(AtomicUsize::new(0));
    let shutdown = Shutdown::new();
    let proxy_task = tokio::spawn(crate::runtime::serve_tcp_listener_with_idle(
        proxy,
        config,
        IdleClient(reports.clone()),
        shutdown.clone(),
    ));
    let health = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_addr = health.local_addr().unwrap();
    let health_task = tokio::spawn(serve_readiness(health, app_addr, shutdown.clone()));
    for _ in 0..20 {
        assert!(request(health_addr, "/ready")
            .await
            .starts_with("HTTP/1.1 200"));
        assert_eq!(
            admission.connections.in_flight(),
            0,
            "health must never touch the public TCP listener"
        );
        assert_eq!(admission.requests.in_flight(), 0);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        reports.load(Ordering::SeqCst) > 0,
        "periodic health checks must not prevent the TCP idle report"
    );
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), proxy_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), health_task)
        .await
        .unwrap()
        .unwrap();
    app_task.abort();
    let _ = app_task.await;
}
