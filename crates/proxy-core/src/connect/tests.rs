use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::net::TcpListener;

struct AttemptGuard(Arc<AtomicUsize>);
impl AttemptGuard {
    fn new(active: Arc<AtomicUsize>) -> Self {
        assert_eq!(
            active.fetch_add(1, Ordering::SeqCst),
            0,
            "old socket must drop before the next opens"
        );
        Self(active)
    }
}
impl Drop for AttemptGuard {
    fn drop(&mut self) {
        assert_eq!(self.0.fetch_sub(1, Ordering::SeqCst), 1);
    }
}

async fn tcp_attempt<T>(
    cap: Duration,
    future: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    timeout(cap, future)
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

#[tokio::test(start_paused = true)]
async fn stalled_socket_is_dropped_before_fresh_attempt_uses_remaining_budget() {
    let active = Arc::new(AtomicUsize::new(0));
    let calls = AtomicUsize::new(0);
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(10),
        retry_connect(Duration::from_secs(10), |cap| {
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            let active = active.clone();
            async move {
                // Probes double inside the one-second window, then end with it.
                const PROBES: [u64; 4] = [100, 200, 400, 150];
                if attempt < PROBES.len() {
                    assert_eq!(cap, Duration::from_millis(PROBES[attempt]));
                    tcp_attempt(cap, async move {
                        let _socket = AttemptGuard::new(active);
                        std::future::pending::<io::Result<usize>>().await
                    })
                    .await
                } else {
                    assert_eq!(attempt, PROBES.len());
                    assert_eq!(cap, Duration::from_millis(8950));
                    tcp_attempt(cap, async move {
                        let _socket = AttemptGuard::new(active);
                        // Longer than the early cap: don't continually reset slow TCP.
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        Ok(attempt)
                    })
                    .await
                }
            }
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, 4);
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(started.elapsed(), Duration::from_millis(4050));
}

#[tokio::test(start_paused = true)]
async fn persistent_stall_probes_briefly_then_holds_one_attempt_to_the_total_deadline() {
    let active = Arc::new(AtomicUsize::new(0));
    let calls = AtomicUsize::new(0);
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(10),
        retry_connect(Duration::from_secs(10), |cap| {
            calls.fetch_add(1, Ordering::SeqCst);
            let active = active.clone();
            async move {
                tcp_attempt(cap, async move {
                    let _socket = AttemptGuard::new(active);
                    std::future::pending::<io::Result<()>>().await
                })
                .await
            }
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn persistent_refusal_preserves_cadence_and_deadline() {
    let calls = AtomicUsize::new(0);
    let result = timeout(
        Duration::from_millis(120),
        retry_connect(Duration::from_millis(120), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Err::<(), _>(io::Error::from(
                io::ErrorKind::ConnectionRefused,
            )))
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn short_refusal_preserves_remaining_initial_probing_window() {
    let calls = AtomicUsize::new(0);
    let result = timeout(
        Duration::from_secs(4),
        retry_connect(Duration::from_secs(4), |cap| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                match call {
                    0 => Err(io::Error::from(io::ErrorKind::ConnectionRefused)),
                    1..=3 => {
                        let probes = [200, 400, 250];
                        assert_eq!(cap, Duration::from_millis(probes[call - 1]));
                        tcp_attempt(cap, std::future::pending::<io::Result<()>>()).await
                    }
                    4 => {
                        assert_eq!(cap, Duration::from_millis(2950));
                        Ok(())
                    }
                    _ => panic!("unexpected retry"),
                }
            }
        }),
    )
    .await;
    result.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_pending_attempt_and_does_not_redial() {
    let active = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let task_active = active.clone();
    let task_calls = calls.clone();
    let task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(10),
            retry_connect(Duration::from_secs(10), |cap| {
                task_calls.fetch_add(1, Ordering::SeqCst);
                let active = task_active.clone();
                async move {
                    tcp_attempt(cap, async move {
                        let _socket = AttemptGuard::new(active);
                        std::future::pending::<io::Result<()>>().await
                    })
                    .await
                }
            }),
        )
        .await
    });
    tokio::task::yield_now().await;
    assert_eq!(active.load(Ordering::SeqCst), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::advance(Duration::from_secs(20)).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn other_completed_errors_are_not_retried() {
    for kind in [
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::PermissionDenied,
        io::ErrorKind::NotFound,
        io::ErrorKind::InvalidInput,
    ] {
        let calls = AtomicUsize::new(0);
        let result = retry_connect(Duration::from_secs(1), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Err::<(), _>(io::Error::from(kind)))
        })
        .await
        .unwrap_err();
        assert_eq!(result.kind(), kind);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn real_refused_socket_recovers_without_dispatching_bytes() {
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let refused = Arc::new(tokio::sync::Notify::new());
    let observed = refused.clone();
    let task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(1),
            retry_connect(Duration::from_secs(1), |cap| {
                let observed = observed.clone();
                async move {
                    let result = tcp_attempt(cap, TcpStream::connect(address)).await;
                    if result
                        .as_ref()
                        .is_err_and(|error| error.kind() == io::ErrorKind::ConnectionRefused)
                    {
                        observed.notify_one();
                    }
                    result
                }
            }),
        )
        .await
        .unwrap()
        .unwrap()
    });
    timeout(Duration::from_secs(1), refused.notified())
        .await
        .unwrap();
    let listener = TcpListener::bind(address).await.unwrap();
    let connection = task.await.unwrap();
    let (mut accepted, _) = listener.accept().await.unwrap();
    drop(connection);
    use tokio::io::AsyncReadExt;
    assert_eq!(accepted.read(&mut [0; 1]).await.unwrap(), 0);
}

#[derive(Clone)]
struct DelayedResolver {
    calls: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    address: std::net::SocketAddr,
    delay: Duration,
}
impl tower_service::Service<hyper_util::client::legacy::connect::dns::Name> for DelayedResolver {
    type Response = std::iter::Once<std::net::SocketAddr>;
    type Error = io::Error;
    type Future = std::pin::Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;
    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let completed = self.completed.clone();
        let address = self.address;
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(std::iter::once(address))
        })
    }
}

#[tokio::test]
async fn hyper_tcp_timeout_does_not_cancel_or_overlap_slow_dns() {
    use tower_service::Service;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let resolver = DelayedResolver {
        calls: calls.clone(),
        completed: completed.clone(),
        address: listener.local_addr().unwrap(),
        delay: Duration::from_millis(100),
    };
    let mut connector =
        hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(resolver);
    let uri = "http://slow-dns.example/".parse::<http::Uri>().unwrap();
    let caps = Mutex::new(Vec::new());
    let stream = timeout(
        Duration::from_millis(300),
        retry_connect(Duration::from_millis(300), |cap| {
            caps.lock().unwrap().push(cap);
            connector.set_connect_timeout(Some(cap));
            connector.call(uri.clone())
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    let caps = caps.lock().unwrap();
    assert_eq!(caps.len(), 1);
    assert!(!caps[0].is_zero() && caps[0] <= Duration::from_millis(75));
    drop(stream);
}

#[tokio::test(start_paused = true)]
async fn total_deadline_cancels_dns_without_starting_a_replacement_lookup() {
    use tower_service::Service;
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let resolver = DelayedResolver {
        calls: calls.clone(),
        completed: completed.clone(),
        address: "127.0.0.1:12345".parse().unwrap(),
        delay: Duration::from_secs(1),
    };
    let mut connector =
        hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(resolver);
    let uri = "http://slow-dns.example/".parse::<http::Uri>().unwrap();
    let result = timeout(
        Duration::from_millis(200),
        retry_connect(Duration::from_millis(200), |cap| {
            connector.set_connect_timeout(Some(cap));
            connector.call(uri.clone())
        }),
    )
    .await;
    assert!(result.is_err());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(completed.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn multiaddress_refusal_masking_timeout_cannot_starve_slow_replacement() {
    let calls = AtomicUsize::new(0);
    let started = Instant::now();
    let mut early_caps = Vec::new();
    let result = timeout(
        Duration::from_secs(10),
        retry_connect(Duration::from_secs(10), |cap| {
            calls.fetch_add(1, Ordering::SeqCst);
            let probing = started.elapsed() < Duration::from_secs(1);
            if probing {
                early_caps.push(cap);
            }
            async move {
                if probing {
                    // Hyper divides the cap across addresses and reports its first
                    // error: address1 refuses, address2 times out at only cap/2.
                    tokio::time::sleep(cap / 2).await;
                    Err(io::Error::from(io::ErrorKind::ConnectionRefused))
                } else {
                    assert!(cap > Duration::from_secs(8));
                    tcp_attempt(cap, async {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        Ok(())
                    })
                    .await
                }
            }
        }),
    )
    .await;
    result.unwrap().unwrap();
    assert_eq!(
        early_caps,
        [100, 200, 400, 500, 200, 50].map(Duration::from_millis)
    );
    assert_eq!(calls.load(Ordering::SeqCst), early_caps.len() + 1);
}

/// A full accept queue drops SYNs, as a Service address does before the
/// node has programmed it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn connect_completes_one_short_probe_after_dropped_syns_stop() {
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
    // One unaccepted connection fills the queue; every later SYN is dropped.
    let _filler = TcpStream::connect(address).await.unwrap();
    assert!(
        timeout(Duration::from_millis(50), TcpStream::connect(address))
            .await
            .is_err(),
        "the full accept queue drops SYNs"
    );
    let server = tokio::spawn(async move {
        tokio::time::sleep(OUTAGE).await;
        let mut accepted = Vec::new();
        loop {
            accepted.push(listener.accept().await.unwrap());
        }
    });

    let started = Instant::now();
    connect_tcp(address, Duration::from_secs(10)).await.unwrap();
    let elapsed = started.elapsed();

    // The kernel resends a dropped SYN after a second. Short probes send a
    // fresh one sooner: here the 400 ms probe that starts at 400 ms.
    assert!(elapsed >= OUTAGE, "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(900), "{elapsed:?}");
    server.abort();
}
