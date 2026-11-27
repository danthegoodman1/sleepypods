use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use http::Uri;
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};

use sleepypods_observability::PodRouteOutcome;

use super::{
    claim_line, read_instance_claim, PodRoute, PodRouteConnects, PodRoutes, SidecarIo,
    POD_ATTEMPT_TIMEOUT,
};

/// A sidecar's view of a connection after its claim.
async fn accept_instance_claim<IO>(mut io: IO, instance_id: &str) -> std::io::Result<SidecarIo<IO>>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let first = read_instance_claim(&mut io, instance_id).await?;
    Ok(SidecarIo::new(io, first))
}

/// Routes whose attempts count apart from every other test's.
fn counted_routes() -> (PodRoutes, &'static PodRouteConnects) {
    let connects: &'static PodRouteConnects = Box::leak(Box::new(PodRouteConnects::new()));
    (PodRoutes::counted_in(connects), connects)
}

/// Each outcome's count, in `PodRouteOutcome::ALL` order.
fn counts(connects: &PodRouteConnects) -> [u64; 6] {
    PodRouteOutcome::ALL.map(|outcome| connects.get(outcome))
}

fn route(instance: &str, address: &str) -> PodRoute {
    PodRoute::new(instance, 9, address.parse().unwrap()).unwrap()
}

#[test]
fn a_pod_route_rides_in_the_userinfo_of_its_service_origin() {
    for (pod, service) in [
        ("10.244.1.5:15000", "http://10.96.0.10:80"),
        ("[fd00::5]:15000", "http://[fd00::10]:80"),
        (
            "[::ffff:10.0.0.1]:15000",
            "http://svc.apps.svc.cluster.local:8080",
        ),
    ] {
        let route = route("tenant-a", pod);
        let service: Uri = service.parse().unwrap();
        let origin = route.origin(&service).unwrap();

        assert_eq!(origin.host(), service.host(), "{origin}");
        assert_eq!(origin.port_u16(), service.port_u16(), "{origin}");
        assert_eq!(PodRoute::from_origin(&origin), Some(route), "{origin}");
    }
    let plain: Uri = "http://10.96.0.10:80".parse().unwrap();
    assert_eq!(PodRoute::from_origin(&plain), None);
}

#[test]
fn origins_differ_by_instance_address_and_generation() {
    let service: Uri = "http://10.96.0.10:80".parse().unwrap();
    let address: SocketAddr = "10.244.1.5:15000".parse().unwrap();
    let origins = [
        PodRoute::new("tenant-a", 9, address).unwrap(),
        PodRoute::new("tenant-b", 9, address).unwrap(),
        PodRoute::new("tenant-a", 10, address).unwrap(),
        PodRoute::new("tenant-a", 9, "10.244.1.6:15000".parse().unwrap()).unwrap(),
    ]
    .map(|route| route.origin(&service).unwrap().authority().unwrap().clone());

    for (index, origin) in origins.iter().enumerate() {
        assert!(!origins[index + 1..].contains(origin), "{origin}");
    }
}

#[test]
fn a_pod_route_needs_an_instance_id_that_is_a_dns_label() {
    for invalid in [
        "",
        "Tenant",
        "-tenant",
        "tenant-",
        "ten.ant",
        &"a".repeat(64),
    ] {
        assert!(
            PodRoute::new(invalid, 1, "10.0.0.1:1".parse().unwrap()).is_err(),
            "{invalid:?}"
        );
    }
}

#[tokio::test]
async fn a_sidecar_echoes_a_claim_for_its_own_instance_and_then_serves_the_connection() {
    let (mut client, server) = duplex(1024);
    let sidecar = tokio::spawn(async move {
        let mut io = accept_instance_claim(server, "tenant-a").await.unwrap();
        let mut request = [0; 4];
        io.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"GET ");
    });

    client.write_all(&claim_line("tenant-a")).await.unwrap();
    let mut echo = vec![0; claim_line("tenant-a").len()];
    client.read_exact(&mut echo).await.unwrap();
    assert_eq!(echo, claim_line("tenant-a"));
    client.write_all(b"GET ").await.unwrap();
    sidecar.await.unwrap();
}

#[tokio::test]
async fn a_sidecar_closes_a_claim_for_another_instance_without_an_echo() {
    let (mut client, server) = duplex(1024);
    client.write_all(&claim_line("tenant-b")).await.unwrap();

    let error = accept_instance_claim(server, "tenant-a").await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    let mut echo = Vec::new();
    client.read_to_end(&mut echo).await.unwrap();
    assert!(echo.is_empty());
}

#[tokio::test]
async fn a_sidecar_refuses_a_malformed_claim() {
    let mut followed = claim_line("tenant-a");
    followed.extend_from_slice(b"GET / HTTP/1.1\r\n");
    let mut long = b"\0sleepypods-instance ".to_vec();
    long.extend_from_slice(&[b'a'; 80]);
    for (claim, kind) in [
        (followed, std::io::ErrorKind::InvalidData),
        (long, std::io::ErrorKind::InvalidData),
        (
            b"\0sleepypods-instance tenant-a".to_vec(),
            std::io::ErrorKind::UnexpectedEof,
        ),
    ] {
        let (mut client, server) = duplex(1024);
        client.write_all(&claim).await.unwrap();
        client.shutdown().await.unwrap();
        let error = accept_instance_claim(server, "tenant-a").await.unwrap_err();
        assert_eq!(error.kind(), kind, "{claim:?}");
    }
}

#[tokio::test]
async fn a_plain_http_connection_keeps_its_first_byte() {
    let (mut client, server) = duplex(1024);
    client.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    client.shutdown().await.unwrap();

    let mut io = accept_instance_claim(server, "tenant-a").await.unwrap();
    let mut request = Vec::new();
    io.read_to_end(&mut request).await.unwrap();

    assert_eq!(request, b"GET / HTTP/1.1\r\n\r\n");
}

/// A listener that accepts connections and answers each claim as a sidecar
/// of `instance` does, counting the connections it accepts.
async fn sidecar(instance: &'static str) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if let Ok(mut io) = accept_instance_claim(stream, instance).await {
                    let _ = io.read(&mut [0; 1]).await;
                }
            });
        }
    });
    (address, accepted)
}

#[tokio::test]
async fn a_pod_whose_sidecar_confirms_the_instance_connects() {
    let (address, accepted) = sidecar("tenant-a").await;
    let (routes, connects) = counted_routes();
    let route = PodRoute::new("tenant-a", 1, address).unwrap();

    assert!(routes
        .connect(&route, Duration::from_secs(10))
        .await
        .is_some());
    assert!(routes
        .connect(&route, Duration::from_secs(10))
        .await
        .is_some());
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    assert_eq!(counts(connects), [2, 0, 0, 0, 0, 0]);
}

#[tokio::test]
async fn a_pod_of_another_instance_fails_once_per_backend_generation() {
    let (address, accepted) = sidecar("tenant-b").await;
    let (routes, connects) = counted_routes();
    let stale = PodRoute::new("tenant-a", 1, address).unwrap();

    assert!(routes
        .connect(&stale, Duration::from_secs(10))
        .await
        .is_none());
    assert!(routes
        .connect(&stale, Duration::from_secs(10))
        .await
        .is_none());
    assert_eq!(accepted.load(Ordering::SeqCst), 1);

    // A new backend generation is a new certification of the address.
    let recertified = PodRoute::new("tenant-a", 2, address).unwrap();
    assert!(routes
        .connect(&recertified, Duration::from_secs(10))
        .await
        .is_none());
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    // Two claims the sidecar closed and one attempt skipped as known failed.
    assert_eq!(counts(connects), [0, 0, 0, 2, 0, 1]);
}

#[tokio::test]
async fn a_refusing_pod_address_fails_at_once() {
    let address = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let started = Instant::now();
    let (routes, connects) = counted_routes();

    let connected = routes
        .connect(
            &PodRoute::new("tenant-a", 1, address).unwrap(),
            Duration::from_secs(10),
        )
        .await;

    assert!(connected.is_none());
    assert!(
        started.elapsed() < POD_ATTEMPT_TIMEOUT,
        "{:?}",
        started.elapsed()
    );
    assert_eq!(counts(connects), [0, 1, 0, 0, 0, 0]);
}

#[tokio::test]
async fn a_silent_pod_address_fails_after_the_attempt_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let held = tokio::spawn(async move {
        let mut streams = Vec::new();
        loop {
            streams.push(listener.accept().await.unwrap());
        }
    });
    let started = Instant::now();
    let (routes, connects) = counted_routes();

    let connected = routes
        .connect(
            &PodRoute::new("tenant-a", 1, address).unwrap(),
            Duration::from_secs(10),
        )
        .await;

    assert!(connected.is_none());
    let elapsed = started.elapsed();
    assert!(elapsed >= POD_ATTEMPT_TIMEOUT, "{elapsed:?}");
    assert!(elapsed < POD_ATTEMPT_TIMEOUT * 2, "{elapsed:?}");
    assert_eq!(counts(connects), [0, 0, 1, 0, 0, 0]);
    held.abort();
}

#[tokio::test]
async fn a_pod_attempt_never_outlasts_the_callers_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _client = TcpStream::connect(address).await.unwrap();

    let connected = timeout(
        Duration::from_millis(200),
        PodRoutes::default().connect(
            &PodRoute::new("tenant-a", 1, address).unwrap(),
            Duration::from_millis(50),
        ),
    )
    .await
    .expect("the attempt ends within its budget");

    assert!(connected.is_none());
}
