//! TLS connections close with FIN under the TLS kind gate's flood, which
//! reaches the frontline through `kubectl port-forward`.
use super::super::{serve_frontline_listeners, FrontlineListeners};
use super::*;
use proxy_core::{ProxyAdmission, ProxyResourceConfig};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::time::Instant;

const WARM_HOST: &str = "warm.example.com";
const ROTATION_HOST: &str = "rotation.example.com";
const MISS_LANES: usize = 15;
const FLOOD: Duration = Duration::from_secs(4);
/// How long the CRI port-forward waits for the second direction to end.
const PORT_FORWARD_GRACE: Duration = Duration::from_secs(1);

/// Relays each connection the way the CRI port-forward behind
/// `kubectl port-forward` does. It copies both directions and never
/// half-closes either socket. Once either direction ends, it gives the other
/// one second and then closes both sockets. A frontline socket error ends the
/// whole port-forward, so the relay counts every one.
async fn port_forward(listener: TcpListener, frontline: SocketAddr, failures: Arc<AtomicUsize>) {
    while let Ok((mut client, _)) = listener.accept().await {
        let failures = failures.clone();
        tokio::spawn(async move {
            let Ok(mut server) = TcpStream::connect(frontline).await else {
                failures.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let (mut client_read, mut client_write) = client.split();
            let (mut server_read, mut server_write) = server.split();
            let down = async {
                let mut buffer = [0; 16 * 1024];
                let mut delivering = true;
                loop {
                    match server_read.read(&mut buffer).await {
                        Ok(0) => return,
                        // The relay keeps reading after its client leaves.
                        Ok(read) => {
                            delivering =
                                delivering && client_write.write_all(&buffer[..read]).await.is_ok();
                        }
                        Err(_) => {
                            failures.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }
                }
            };
            let up = async {
                let mut buffer = [0; 16 * 1024];
                loop {
                    match client_read.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => {
                            if server_write.write_all(&buffer[..read]).await.is_err() {
                                failures.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    }
                }
            };
            tokio::pin!(down, up);
            tokio::select! {
                () = &mut down => {
                    let _ = tokio::time::timeout(PORT_FORWARD_GRACE, up).await;
                }
                () = &mut up => {
                    let _ = tokio::time::timeout(PORT_FORWARD_GRACE, down).await;
                }
            }
        });
    }
}

/// Serves `hostname`'s certificate under a two-second authorization lease, so
/// the cache refreshes it during the flood.
fn publish_leased(
    fixture: &crate::certificates::test_support::CertificateFixture,
    hostname: &str,
    revision: u64,
    cert: &TestCert,
) {
    let mut response = crate::certificates::test_support::response(
        hostname,
        revision,
        vec![cert.cert.clone()],
        cert.key.clone_key(),
    );
    response.authorization_ttl_millis = 2_000;
    fixture
        .api
        .responses
        .lock()
        .unwrap()
        .insert(hostname.to_owned(), response);
}

/// A refusal as the gate's driver counts one: an alert, end of stream or a
/// reset.
fn refused(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .and_then(|error| error.downcast_ref::<tokio_rustls::rustls::Error>())
        .is_some_and(|error| matches!(error, tokio_rustls::rustls::Error::AlertReceived(_)))
        || matches!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
        )
}

/// One warm lane alternates two published hostnames, one of them rotating,
/// while fifteen lanes ask for unpublished names. Each port-forwarded
/// connection keeps its setup slot for the relay's grace second, so the warm
/// lane fills the handshake budget, and admission refuses connections whose
/// ClientHello the relay has already delivered. Connections refused at
/// admission, refused for an unknown name, and completed must all close
/// without a reset. A small handshake budget makes saturation certain; the gate
/// fills the default budget of 128 the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_flood_through_port_forward_closes_every_connection_with_fin() {
    let warm = test_cert(WARM_HOST);
    let seed = test_cert(ROTATION_HOST);
    let rotated = test_cert(ROTATION_HOST);
    let mut roots = RootCertStore::empty();
    for cert in [&warm, &seed, &rotated] {
        roots.add(cert.cert.clone()).unwrap();
    }
    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"h2".to_vec()];
    let connector = TlsConnector::from(Arc::new(client));
    let fixture = crate::certificates::test_support::CertificateFixture::new();
    fixture
        .publish(WARM_HOST, vec![warm.cert.clone()], warm.key.clone_key())
        .unwrap();
    publish_leased(&fixture, ROTATION_HOST, 1, &seed);

    let resources = ProxyResourceConfig::default()
        .with_limits(1024, 1024, 32, 128)
        .unwrap();
    let admission = ProxyAdmission::new(resources);
    let tls = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let tls_addr = tls.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let frontline = tokio::spawn(serve_frontline_listeners(
        FrontlineListeners {
            http: TcpListener::bind(("127.0.0.1", 0)).await.unwrap(),
            tls_termination: Some(tls),
            tls_passthrough: None,
        },
        admission.clone(),
        runtime_with_state(
            FakeControlPlane::default(),
            DrainTracker::new(Duration::from_secs(1)),
        ),
        FrontlineTlsAdapter::new(fixture.cache.clone()),
        shutdown.clone(),
    ));
    let relay = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let relay_addr = relay.local_addr().unwrap();
    let failures = Arc::new(AtomicUsize::new(0));
    let relay = tokio::spawn(port_forward(relay, tls_addr, failures.clone()));

    let deadline = Instant::now() + FLOOD;
    let rotation_seen = Arc::new(AtomicBool::new(false));
    let warm_lane = tokio::spawn({
        let connector = connector.clone();
        let rotation_seen = rotation_seen.clone();
        let rotated = rotated.cert.clone();
        async move {
            let (mut completed, mut refusals) = (0, 0);
            for attempt in 0.. {
                if Instant::now() >= deadline {
                    break;
                }
                let host = [WARM_HOST, ROTATION_HOST][attempt % 2];
                let tcp = TcpStream::connect(relay_addr).await.unwrap();
                match connector
                    .connect(ServerName::try_from(host).unwrap(), tcp)
                    .await
                {
                    Ok(stream) => {
                        completed += 1;
                        let leaf = stream.get_ref().1.peer_certificates().unwrap()[0].clone();
                        if leaf == rotated {
                            rotation_seen.store(true, Ordering::Relaxed);
                        }
                    }
                    Err(error) => {
                        assert!(refused(&error), "warm handshake failed: {error}");
                        refusals += 1;
                    }
                }
            }
            (completed, refusals)
        }
    });
    let mut miss_lanes = JoinSet::new();
    for lane in 0..MISS_LANES {
        let connector = connector.clone();
        miss_lanes.spawn(async move {
            let mut refusals = 0;
            while Instant::now() < deadline {
                let name = format!("miss-{lane}-{refusals}.example.com");
                let tcp = TcpStream::connect(relay_addr).await.unwrap();
                let error = tokio::time::timeout(
                    Duration::from_secs(6),
                    connector.connect(ServerName::try_from(name).unwrap(), tcp),
                )
                .await
                .expect("an unpublished name is refused within the deadline")
                .expect_err("an unpublished name has no certificate");
                assert!(refused(&error), "miss handshake failed: {error}");
                refusals += 1;
            }
        });
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    publish_leased(&fixture, ROTATION_HOST, 2, &rotated);

    let (completed, refusals) = warm_lane.await.unwrap();
    while let Some(lane) = miss_lanes.join_next().await {
        lane.unwrap();
    }
    assert!(completed > 0, "warm handshakes progress during the flood");
    assert!(refusals > 0, "the flood fills the handshake budget");
    assert!(
        rotation_seen.load(Ordering::Relaxed),
        "the warm lane observes the rotated certificate"
    );
    // Every relayed connection, including the last ones, ends within the grace.
    tokio::time::sleep(PORT_FORWARD_GRACE + Duration::from_millis(200)).await;
    assert_eq!(
        failures.load(Ordering::Relaxed),
        0,
        "a frontline socket reset or refused relayed bytes"
    );

    relay.abort();
    shutdown.shutdown();
    frontline.await.unwrap().unwrap();
    assert_eq!(admission.connections.in_flight(), 0);
    assert_eq!(admission.handshakes.in_flight(), 0);
    fixture.finish().await;
}
