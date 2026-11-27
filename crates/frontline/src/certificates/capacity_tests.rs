//! Capacity gates: real certificates stay cached through a flood of unknown
//! hostnames, and each kind of entry keeps to its own budget.
use super::test_support::{response, FakeResolver};
use super::*;
use proxy_core::Shutdown;
use tokio::task::JoinSet;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

struct Rig {
    cache: TlsCertificateStore,
    api: Arc<FakeResolver>,
    shutdown: Shutdown,
    tasks: JoinSet<()>,
}
impl Rig {
    fn new() -> Self {
        let api = Arc::new(FakeResolver::default());
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
        self.api.responses.lock().unwrap().insert(
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
    fn requests(&self) -> usize {
        self.api.requests.lock().unwrap().len()
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
