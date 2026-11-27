//! Capacity gates: real certificates stay cached through a refresh cycle and a
//! flood of unknown hostnames, each kind of entry keeps to its own budget, and
//! fetch slots are shared in turn.
use super::test_support::{response, FakeResolver};
use super::*;
use crate::FrontlineTlsAdapter;
use proxy_core::Shutdown;
use tokio::{sync::oneshot, task::JoinSet};
use tokio_rustls::{
    rustls::{
        self,
        pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
    },
    TlsConnector,
};

/// A control-plane call in flight: its hostname and the handle that lets it
/// answer.
type Held = (String, oneshot::Sender<()>);

/// Answers from the fake API. While gated, each call announces itself and
/// waits for the test to release it.
#[derive(Default)]
struct Gated {
    api: FakeResolver,
    gate: Mutex<Option<mpsc::UnboundedSender<Held>>>,
}
impl CertificateResolver for Gated {
    fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
        let gate = self.gate.lock().unwrap().clone();
        let hostname = request.server_name.clone();
        let response = self.api.resolve(request);
        Box::pin(async move {
            if let Some(gate) = gate {
                let (release, released) = oneshot::channel();
                gate.send((hostname, release))
                    .map_err(|_| CertificateLookupError::Stopped)?;
                released
                    .await
                    .map_err(|_| CertificateLookupError::Stopped)?;
            }
            response.await
        })
    }
}

struct Rig {
    cache: TlsCertificateStore,
    api: Arc<Gated>,
    shutdown: Shutdown,
    tasks: JoinSet<()>,
}
impl Rig {
    fn new() -> Self {
        let api = Arc::new(Gated::default());
        let (cache, worker) = TlsCertificateStore::new(api.clone());
        let shutdown = Shutdown::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(worker.run(shutdown.clone()));
        Self {
            cache,
            api,
            shutdown,
            tasks,
        }
    }
    fn publish(&self, host: &str) -> CertificateDer<'static> {
        let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
        let der = cert.cert.der().clone();
        self.api.api.responses.lock().unwrap().insert(
            host.into(),
            response(
                host,
                1,
                vec![der.clone()],
                PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
            ),
        );
        der
    }
    /// Publishes `count` certificates and installs each through a cold lookup.
    async fn install(&self, count: usize) -> Vec<(String, CertificateDer<'static>)> {
        let mut installed = Vec::with_capacity(count);
        for i in 0..count {
            let host = format!("c{i}.example");
            let der = self.publish(&host);
            assert!(self.cache.resolve(&host).await.unwrap().is_some());
            installed.push((host, der));
        }
        installed
    }
    /// Holds every later control-plane call until the test releases it.
    fn gate(&self) -> mpsc::UnboundedReceiver<Held> {
        let (announce, held) = mpsc::unbounded_channel();
        *self.api.gate.lock().unwrap() = Some(announce);
        held
    }
    /// Fills every fetch slot with a lookup that stays in flight.
    async fn occupy(
        &self,
        held: &mut mpsc::UnboundedReceiver<Held>,
        callers: &mut JoinSet<Result<Option<Arc<ServerConfig>>, CertificateLookupError>>,
    ) -> Vec<oneshot::Sender<()>> {
        let mut releases = Vec::new();
        for i in 0..CERTIFICATE_FETCHES {
            let cache = self.cache.clone();
            callers.spawn(async move { cache.resolve(&format!("busy{i}.example")).await });
            releases.push(started(held).await.1);
        }
        assert_eq!(self.cache.usage().fetches, CERTIFICATE_FETCHES);
        releases
    }
    /// Makes every cached certificate due for refresh at the worker's next scan.
    fn expire_refresh_times(&self) {
        let mut state = self.cache.shared.state.lock().unwrap();
        for view in state.entries.values_mut().filter_map(|e| e.value.as_mut()) {
            view.refresh = Instant::now();
        }
    }
    fn requests(&self) -> usize {
        self.api.api.requests.lock().unwrap().len()
    }
    fn requested(&self, host: &str) -> usize {
        let requests = self.api.api.requests.lock().unwrap();
        requests.iter().filter(|r| r.server_name == host).count()
    }
    fn cached(&self, host: &str) -> bool {
        let state = self.cache.shared.state.lock().unwrap();
        state.entries.contains_key(host)
    }
    async fn finish(mut self) {
        self.shutdown.shutdown();
        while let Some(result) = self.tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(self.cache.usage().fetches, 0);
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        self.shutdown.shutdown();
    }
}

/// Spins instead of sleeping, so a paused clock stays where the test put it.
async fn until(mut condition: impl FnMut() -> bool) {
    let bound = std::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            std::time::Instant::now() < bound,
            "capacity barrier did not complete"
        );
        tokio::task::yield_now().await;
    }
}
/// The next control-plane call to start.
async fn started(held: &mut mpsc::UnboundedReceiver<Held>) -> Held {
    let mut next = None;
    until(|| {
        next = held.try_recv().ok();
        next.is_some()
    })
    .await;
    next.unwrap()
}

/// A full TLS handshake against the adapter. Returns the certificate the
/// client verified.
async fn handshake(
    cache: TlsCertificateStore,
    host: String,
    trusted: CertificateDer<'static>,
) -> Result<CertificateDer<'static>, crate::TlsTerminationError> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(trusted).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let adapter = FrontlineTlsAdapter::new(cache);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (client, server) = tokio::join!(
        connector.connect(ServerName::try_from(host).unwrap(), client),
        adapter.terminate(server)
    );
    server?;
    Ok(client.unwrap().get_ref().1.peer_certificates().unwrap()[0].clone())
}

#[tokio::test]
async fn full_cache_stays_warm_through_a_refresh_cycle_with_three_fetches_in_flight() {
    let rig = Rig::new();
    let installed = rig.install(CERTIFICATE_CACHE_ENTRIES).await;
    assert_eq!(
        rig.cache.usage().certificates,
        CERTIFICATE_CACHE_ENTRIES,
        "installing a full cache evicted certificates"
    );
    assert_eq!(rig.requests(), CERTIFICATE_CACHE_ENTRIES);

    let mut held = rig.gate();
    rig.expire_refresh_times();
    let mut refreshed = 0;
    while refreshed < CERTIFICATE_CACHE_ENTRIES {
        let batch = CERTIFICATE_FETCHES.min(CERTIFICATE_CACHE_ENTRIES - refreshed);
        let mut releases = Vec::with_capacity(batch);
        for _ in 0..batch {
            releases.push(started(&mut held).await.1);
        }
        if batch == CERTIFICATE_FETCHES {
            assert_eq!(rig.cache.usage().fetches, CERTIFICATE_FETCHES);
        }
        assert_eq!(
            rig.cache.usage().certificates,
            CERTIFICATE_CACHE_ENTRIES,
            "a refresh in flight evicted a warm certificate"
        );
        for release in releases {
            release.send(()).unwrap();
        }
        refreshed += batch;
    }
    until(|| rig.cache.usage().fetches == 0).await;
    *rig.api.gate.lock().unwrap() = None;
    assert_eq!(rig.requests(), 2 * CERTIFICATE_CACHE_ENTRIES);
    for (host, _) in &installed {
        assert_eq!(rig.requested(host), 2, "{host} refreshes once per cycle");
    }

    for (host, _) in &installed {
        assert!(rig.cache.resolve(host).await.unwrap().is_some());
    }
    for (host, der) in installed.iter().step_by(CERTIFICATE_CACHE_ENTRIES / 8) {
        let verified = handshake(rig.cache.clone(), host.clone(), der.clone()).await;
        assert_eq!(&verified.unwrap(), der);
    }
    assert_eq!(
        rig.requests(),
        2 * CERTIFICATE_CACHE_ENTRIES,
        "a warm handshake fetched from the control plane"
    );
    assert_eq!(rig.cache.usage().certificates, CERTIFICATE_CACHE_ENTRIES);
    rig.finish().await;
}

#[tokio::test]
async fn unknown_hostnames_leave_every_certificate_cached() {
    let rig = Rig::new();
    let installed = rig.install(64).await;
    for i in 0..2 * CERTIFICATE_NEGATIVE_ENTRIES {
        let unknown = format!("unknown{i}.example");
        assert!(rig.cache.resolve(&unknown).await.unwrap().is_none());
    }
    let usage = rig.cache.usage();
    assert_eq!(
        usage.certificates,
        installed.len(),
        "unknown hostnames evicted certificates"
    );
    assert_eq!(usage.negative_entries, CERTIFICATE_NEGATIVE_ENTRIES);
    let requests = rig.requests();
    for (host, _) in &installed {
        assert!(rig.cache.resolve(host).await.unwrap().is_some());
    }
    assert_eq!(
        rig.requests(),
        requests,
        "a certificate was fetched again after the unknown hostnames"
    );
    assert!(
        rig.cache
            .shared
            .task_high_water
            .load(std::sync::atomic::Ordering::SeqCst)
            <= CERTIFICATE_FETCHES
    );
    rig.finish().await;
}

#[tokio::test(start_paused = true)]
async fn handshake_waits_for_a_busy_fetch_slot_and_succeeds_within_the_lookup_deadline() {
    let rig = Rig::new();
    let der = rig.publish("app.example");
    let mut held = rig.gate();
    let mut busy = JoinSet::new();
    let mut releases = rig.occupy(&mut held, &mut busy).await;

    let arrived = Instant::now();
    let waiting = tokio::spawn(handshake(
        rig.cache.clone(),
        "app.example".into(),
        der.clone(),
    ));
    until(|| {
        assert!(
            !waiting.is_finished(),
            "the handshake ended while every fetch slot was busy"
        );
        rig.cache.usage().queued == 1
    })
    .await;
    tokio::time::advance(CERTIFICATE_LOOKUP_TIMEOUT - Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    assert_eq!(rig.requested("app.example"), 0);

    releases.remove(0).send(()).unwrap();
    let (hostname, release) = started(&mut held).await;
    assert_eq!(hostname, "app.example");
    release.send(()).unwrap();
    until(|| waiting.is_finished()).await;
    assert_eq!(waiting.await.unwrap().unwrap(), der);
    assert!(arrived.elapsed() < CERTIFICATE_LOOKUP_TIMEOUT);

    drop(releases);
    while busy.join_next().await.is_some() {}
    rig.finish().await;
}

#[tokio::test(start_paused = true)]
async fn a_lookup_still_queued_at_its_deadline_fails_and_starts_no_fetch() {
    let rig = Rig::new();
    let mut held = rig.gate();
    let mut callers = JoinSet::new();
    let busy = rig.occupy(&mut held, &mut callers).await;

    tokio::time::advance(Duration::from_secs(1)).await;
    for i in 0..CERTIFICATE_FETCHES {
        let cache = rig.cache.clone();
        callers.spawn(async move { cache.resolve(&format!("ahead{i}.example")).await });
        until(|| rig.cache.usage().queued == i + 1).await;
    }
    let cache = rig.cache.clone();
    let late = tokio::spawn(async move { cache.resolve("late.example").await });
    until(|| rig.cache.usage().queued == CERTIFICATE_FETCHES + 1).await;

    // The lookups ahead take every slot a second later, so their fetches
    // outlast the deadline of the lookup behind them.
    tokio::time::advance(Duration::from_secs(1)).await;
    for release in busy {
        release.send(()).unwrap();
    }
    let mut ahead = Vec::new();
    for _ in 0..CERTIFICATE_FETCHES {
        ahead.push(started(&mut held).await.1);
    }
    assert_eq!(rig.cache.usage().queued, 1);

    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        late.await.unwrap().unwrap_err(),
        CertificateLookupError::Deadline
    );
    ahead.remove(0).send(()).unwrap();
    until(|| rig.cache.usage().queued == 0).await;
    until(|| rig.cache.usage().fetches == CERTIFICATE_FETCHES - 1).await;
    assert_eq!(rig.requested("late.example"), 0);

    drop(ahead);
    while callers.join_next().await.is_some() {}
    rig.finish().await;
}

#[tokio::test(start_paused = true)]
async fn fetch_slots_alternate_between_waiting_handshakes_and_refreshes() {
    let rig = Rig::new();
    let installed = rig.install(4).await;
    let mut held = rig.gate();
    let mut callers = JoinSet::new();
    let mut releases = rig.occupy(&mut held, &mut callers).await;

    for i in 0..installed.len() {
        let cache = rig.cache.clone();
        callers.spawn(async move { cache.resolve(&format!("cold{i}.example")).await });
        until(|| rig.cache.usage().queued == i + 1).await;
    }
    rig.expire_refresh_times();
    tokio::time::advance(Duration::from_millis(1500)).await;
    until(|| rig.cache.usage().queued == 2 * installed.len()).await;

    let mut order = Vec::new();
    for _ in 0..2 * installed.len() {
        releases.remove(0).send(()).unwrap();
        let (hostname, release) = started(&mut held).await;
        order.push(hostname);
        releases.push(release);
    }
    assert!(
        order
            .windows(2)
            .all(|pair| pair[0].starts_with("cold") != pair[1].starts_with("cold")),
        "lookups and refreshes did not take turns: {order:?}"
    );
    let lookups: Vec<_> = order.iter().filter(|h| h.starts_with("cold")).collect();
    assert_eq!(
        lookups,
        [
            "cold0.example",
            "cold1.example",
            "cold2.example",
            "cold3.example"
        ],
        "lookups start in arrival order"
    );

    for release in releases {
        release.send(()).unwrap();
    }
    while callers.join_next().await.is_some() {}
    rig.finish().await;
}

#[tokio::test]
async fn a_full_cache_evicts_its_least_recently_handshaken_certificate() {
    let rig = Rig::new();
    let installed = rig.install(CERTIFICATE_CACHE_ENTRIES).await;
    let material: usize = installed.iter().map(|(_, der)| der.len()).sum();
    let usage = rig.cache.usage();
    assert_eq!(usage.certificates, CERTIFICATE_CACHE_ENTRIES);
    assert_eq!(usage.negative_entries, 0);
    assert!(usage.certificate_bytes > material);

    assert!(rig.cache.resolve("c0.example").await.unwrap().is_some());
    rig.publish("one-more.example");
    assert!(rig
        .cache
        .resolve("one-more.example")
        .await
        .unwrap()
        .is_some());
    assert_eq!(rig.cache.usage().certificates, CERTIFICATE_CACHE_ENTRIES);
    assert!(rig.cached("c0.example"));
    assert!(!rig.cached("c1.example"));
    assert!(rig.cached("one-more.example"));
    rig.finish().await;
}
