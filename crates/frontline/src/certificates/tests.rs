use super::test_support::{response, CertificateFixture, FakeResolver};
use super::*;
use proxy_core::Shutdown;
use tokio::{sync::oneshot, task::JoinSet};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
fn material(host: &str) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
    (
        cert.cert.der().clone(),
        PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
}
type Reply = oneshot::Sender<Result<pb::ResolveTlsCertificateResponse, CertificateLookupError>>;
struct Controlled(mpsc::Sender<(pb::ResolveTlsCertificateRequest, Reply)>);
impl CertificateResolver for Controlled {
    fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
        let tx = self.0.clone();
        Box::pin(async move {
            let (reply, rx) = oneshot::channel();
            tx.send((request, reply))
                .await
                .map_err(|_| CertificateLookupError::Stopped)?;
            rx.await.map_err(|_| CertificateLookupError::Stopped)?
        })
    }
}
struct Rig {
    cache: TlsCertificateStore,
    rx: mpsc::Receiver<(pb::ResolveTlsCertificateRequest, Reply)>,
    tasks: JoinSet<()>,
    shutdown: Shutdown,
}
impl Rig {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(32);
        let (cache, worker) = TlsCertificateStore::new(Arc::new(Controlled(tx)));
        let shutdown = Shutdown::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(worker.run(shutdown.clone()));
        Self {
            cache,
            rx,
            tasks,
            shutdown,
        }
    }
    async fn next(&mut self) -> (pb::ResolveTlsCertificateRequest, Reply) {
        tokio::time::timeout(Duration::from_secs(1), self.rx.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn finish(mut self) {
        self.shutdown.shutdown();
        while let Some(r) = self.tasks.join_next().await {
            r.unwrap();
        }
        assert_eq!(self.cache.usage().fetches, 0);
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        self.shutdown.shutdown();
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    let failure_bound = std::time::Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(
            std::time::Instant::now() < failure_bound,
            "barrier condition did not become true within real-time failure bound"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn coalesced_cold_waiters_and_independent_host_then_zero_rpc_warm() {
    let mut rig = Rig::new();
    let mut waiters = JoinSet::new();
    for _ in 0..64 {
        let c = rig.cache.clone();
        waiters.spawn(async move { c.resolve("App.Example.").await });
    }
    let (request, first) = rig.next().await;
    assert_eq!(request.server_name, "app.example");
    until(|| {
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .changed
            .receiver_count()
            == 64
    })
    .await;
    let c = rig.cache.clone();
    let mut other = JoinSet::new();
    other.spawn(async move { c.resolve("other.example").await });
    let (request, reply) = rig.next().await;
    assert_eq!(request.server_name, "other.example");
    let (cert, key) = material("other.example");
    reply
        .send(Ok(response("other.example", 1, vec![cert], key)))
        .unwrap();
    assert!(other.join_next().await.unwrap().unwrap().unwrap().is_some());
    let (cert, key) = material("app.example");
    first
        .send(Ok(response("app.example", 1, vec![cert], key)))
        .unwrap();
    while let Some(result) = waiters.join_next().await {
        assert!(result.unwrap().unwrap().is_some());
    }
    for _ in 0..100 {
        assert!(rig.cache.resolve("app.example").await.unwrap().is_some());
    }
    assert!(rig.rx.try_recv().is_err());
    rig.finish().await;
}
#[tokio::test]
async fn cancelling_one_or_all_waiters_does_not_release_running_fetch_or_poison_cache() {
    let mut rig = Rig::new();
    let mut first = JoinSet::new();
    let c = rig.cache.clone();
    first.spawn(async move { c.resolve("app.example").await });
    let (_, reply) = rig.next().await;
    first.abort_all();
    while first.join_next().await.is_some() {}
    assert_eq!(rig.cache.usage().fetches, 1);
    let mut second = JoinSet::new();
    let c = rig.cache.clone();
    second.spawn(async move { c.resolve("app.example").await });
    until(|| {
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .changed
            .receiver_count()
            == 1
    })
    .await;
    assert!(rig.rx.try_recv().is_err());
    let (cert, key) = material("app.example");
    reply
        .send(Ok(response("app.example", 1, vec![cert], key)))
        .unwrap();
    assert!(second
        .join_next()
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .is_some());
    let c = rig.cache.clone();
    first.spawn(async move { c.resolve("abandoned.example").await });
    let (_, held) = rig.next().await;
    first.abort_all();
    while first.join_next().await.is_some() {}
    assert_eq!(rig.cache.usage().fetches, 1);
    rig.finish().await;
    assert!(held.send(Err(CertificateLookupError::Unavailable)).is_err());
}
#[tokio::test]
async fn late_result_after_eviction_and_reinsertion_cannot_replace_new_view() {
    let mut rig = Rig::new();
    let mut requests = JoinSet::new();
    let c = rig.cache.clone();
    requests.spawn(async move { c.resolve("app.example").await });
    let (_, old) = rig.next().await;
    evict(&mut rig.cache.shared.state.lock().unwrap(), "app.example");
    let c = rig.cache.clone();
    requests.spawn(async move { c.resolve("app.example").await });
    let (_, new) = rig.next().await;
    let (cert, key) = material("app.example");
    new.send(Ok(response("app.example", 2, vec![cert], key)))
        .unwrap();
    until(|| {
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .is_some_and(|v| v.revision == 2)
    })
    .await;
    let (cert, key) = material("app.example");
    old.send(Ok(response("app.example", 1, vec![cert], key)))
        .unwrap();
    while requests.join_next().await.is_some() {}
    until(|| rig.cache.usage().fetches == 0).await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .revision,
        2
    );
    rig.finish().await;
}
#[tokio::test]
async fn current_wall_validity_and_fixed_monotonic_lease_both_gate_warm_selection() {
    use std::sync::atomic::{AtomicI64, Ordering};
    let api = Arc::new(FakeResolver::default());
    let wall = Arc::new(AtomicI64::new(wall_now()));
    let observed = wall.clone();
    let (cache, worker) = TlsCertificateStore::with_clock(
        api.clone(),
        Arc::new(move || observed.load(Ordering::SeqCst)),
    );
    let shutdown = Shutdown::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(worker.run(shutdown.clone()));
    let (cert, key) = material("app.example");
    let value = response("app.example", 1, vec![cert], key);
    api.responses
        .lock()
        .unwrap()
        .insert("app.example".into(), value);
    assert!(cache.resolve("app.example").await.unwrap().is_some());
    let before = cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .clone()
        .unwrap();
    wall.store(
        before.metadata.as_ref().unwrap().not_after_unix_millis,
        Ordering::SeqCst,
    );
    assert!(
        cache.resolve("app.example").await.is_err(),
        "forward wall jump cannot serve expired warm config"
    );
    assert_eq!(
        cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        before.expires
    );
    wall.store(wall_now() - 1000, Ordering::SeqCst);
    assert!(cache.resolve("app.example").await.unwrap().is_some());
    assert_eq!(
        cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        before.expires
    );
    shutdown.shutdown();
    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
}

#[tokio::test]
async fn negative_expiry_and_conditional_refresh_use_original_rpc_start() {
    let mut rig = Rig::new();
    let mut calls = JoinSet::new();
    let c = rig.cache.clone();
    calls.spawn(async move { c.resolve("app.example").await });
    let (_, reply) = rig.next().await;
    let (cert, key) = material("app.example");
    let found = response("app.example", 1, vec![cert], key);
    let metadata = match found.value.clone().unwrap() {
        pb::resolve_tls_certificate_response::Value::Found(v) => v.metadata.unwrap(),
        _ => unreachable!(),
    };
    reply.send(Ok(found)).unwrap();
    assert!(calls.join_next().await.unwrap().unwrap().unwrap().is_some());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(67)).await;
    let (request, reply) = rig.next().await;
    assert_eq!(request.known_view_revision, Some(1));
    let rpc_start = Instant::now();
    tokio::time::advance(Duration::from_secs(1)).await;
    let mut metadata = metadata;
    metadata.sealing_revision += 1;
    metadata.sealing_key_id = Some("rotated-sealer".into());
    reply
        .send(Ok(pb::ResolveTlsCertificateResponse {
            server_name: "app.example".into(),
            view_revision: 1,
            observed_at_unix_millis: wall_now(),
            authorization_ttl_millis: 300_000,
            value: Some(pb::resolve_tls_certificate_response::Value::Unchanged(
                metadata,
            )),
        }))
        .unwrap();
    until(|| !rig.cache.shared.state.lock().unwrap().entries["app.example"].pending).await;
    let expires = {
        let state = rig.cache.shared.state.lock().unwrap();
        state.entries["app.example"].value.as_ref().unwrap().expires
    };
    assert_eq!(
        expires,
        rpc_start + Duration::from_secs(300),
        "receipt cannot add another second to lease"
    );
    rig.finish().await;

    let fixture = CertificateFixture::new();
    let c = fixture.cache.clone();
    calls.spawn(async move { c.resolve("missing.example").await });
    until(|| {
        !fixture.api.requests.lock().unwrap().is_empty()
            && !fixture.cache.shared.state.lock().unwrap().entries["missing.example"].pending
    })
    .await;
    assert!(calls.join_next().await.unwrap().unwrap().unwrap().is_none());
    assert!(fixture
        .cache
        .resolve("missing.example")
        .await
        .unwrap()
        .is_none());
    assert_eq!(fixture.api.requests.lock().unwrap().len(), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    let c = fixture.cache.clone();
    calls.spawn(async move { c.resolve("missing.example").await });
    until(|| {
        fixture.api.requests.lock().unwrap().len() == 2
            && !fixture.cache.shared.state.lock().unwrap().entries["missing.example"].pending
    })
    .await;
    assert!(calls.join_next().await.unwrap().unwrap().unwrap().is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn stalled_lookup_is_cancelled_at_three_seconds_with_no_lease_or_permit_leak() {
    let mut rig = Rig::new();
    let mut calls = JoinSet::new();
    let c = rig.cache.clone();
    calls.spawn(async move { c.resolve("slow.example").await });
    let (_, reply) = rig.next().await;
    tokio::time::pause();
    tokio::time::advance(CERTIFICATE_LOOKUP_TIMEOUT).await;
    assert!(calls.join_next().await.unwrap().unwrap().is_err());
    // Tokio timers round to milliseconds; the worker starts just after its
    // waiter. Advance one timer tick before a busy-yield capacity assertion.
    tokio::time::advance(Duration::from_millis(1)).await;
    until(|| rig.cache.usage().fetches == 0).await;
    assert!(
        rig.cache.shared.state.lock().unwrap().entries["slow.example"]
            .value
            .is_none()
    );
    assert!(reply
        .send(Err(CertificateLookupError::Unavailable))
        .is_err());
    rig.finish().await;
}

#[tokio::test]
async fn malformed_and_delayed_responses_cannot_install_or_extend_a_view() {
    // Exercise the exact installation validator with independently captured
    // response/clock values; the live resolver tests cover its worker wiring.
    let fixture = CertificateFixture::new();
    let (cert, key) = material("app.example");
    fixture.publish("app.example", vec![cert], key).unwrap();
    assert!(fixture
        .cache
        .resolve("app.example")
        .await
        .unwrap()
        .is_some());
    let prior = fixture.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .clone()
        .unwrap();
    let found = fixture.api.responses.lock().unwrap()["app.example"].clone();
    let make_fetch = |prior: Option<View>| Fetch {
        hostname: "app.example".into(),
        generation: 1,
        prior,
        floor: 0,
        _slot: fixture
            .cache
            .shared
            .fetches
            .clone()
            .try_acquire_owned()
            .unwrap(),
    };
    let mut bad = Vec::new();
    let mut wrong_host = found.clone();
    wrong_host.server_name = "other.example".into();
    bad.push((wrong_host, None));
    bad.push((found.clone(), Some(prior.clone()))); // Same-view Found is incoherent.
    let mut same_missing = found.clone();
    same_missing.value = Some(pb::resolve_tls_certificate_response::Value::Missing(
        pb::MissingTlsCertificate {},
    ));
    bad.push((same_missing, Some(prior.clone())));
    let mut unchanged = found.clone();
    unchanged.value = Some(pb::resolve_tls_certificate_response::Value::Unchanged(
        prior.metadata.clone().unwrap(),
    ));
    bad.push((unchanged.clone(), None));
    let mut oversized_sealing_id = unchanged.clone();
    if let Some(pb::resolve_tls_certificate_response::Value::Unchanged(metadata)) =
        &mut oversized_sealing_id.value
    {
        metadata.sealing_key_id = Some("k".repeat(65));
    }
    bad.push((oversized_sealing_id, Some(prior.clone())));
    let mut invalid_sealing_revision = unchanged.clone();
    if let Some(pb::resolve_tls_certificate_response::Value::Unchanged(metadata)) =
        &mut invalid_sealing_revision.value
    {
        metadata.sealing_revision = u64::MAX;
    }
    bad.push((invalid_sealing_revision, Some(prior.clone())));
    unchanged.view_revision += 1;
    bad.push((unchanged, Some(prior.clone())));
    let mut oversized = found.clone();
    if let Some(pb::resolve_tls_certificate_response::Value::Found(v)) = &mut oversized.value {
        v.bundle.as_mut().unwrap().private_key_pkcs8_der = vec![42; 128 * 1024 + 1];
    }
    bad.push((oversized, None));
    let mut wrong_metadata = found.clone();
    if let Some(pb::resolve_tls_certificate_response::Value::Found(v)) = &mut wrong_metadata.value {
        v.metadata.as_mut().unwrap().not_after_unix_millis += 1;
    }
    bad.push((wrong_metadata, None));
    let mut wrong_key = found.clone();
    if let Some(pb::resolve_tls_certificate_response::Value::Found(v)) = &mut wrong_key.value {
        v.bundle.as_mut().unwrap().private_key_pkcs8_der = b"INVALID-KEY-MARKER".to_vec();
    }
    bad.push((wrong_key, None));
    for (response, prior) in bad {
        assert!(
            worker::validate_view(make_fetch(prior), response, Instant::now(), wall_now()).is_err()
        );
    }
    let mut short = found;
    short.authorization_ttl_millis = 1000;
    let view = worker::validate_view(
        make_fetch(None),
        short,
        Instant::now() - Duration::from_secs(2),
        wall_now(),
    )
    .unwrap();
    assert!(
        !usable(&view, Instant::now(), wall_now()),
        "delayed response has no usable remaining lease"
    );
    assert_eq!(
        fixture.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        prior.expires
    );
    fixture.finish().await;
}

#[tokio::test]
async fn short_positive_lease_refreshes_in_the_worker_before_original_expiry() {
    let mut rig = Rig::new();
    let mut callers = JoinSet::new();
    let cache = rig.cache.clone();
    callers.spawn(async move { cache.resolve("app.example").await });
    let (_, reply) = rig.next().await;
    let (cert, key) = material("app.example");
    let mut initial = response("app.example", 1, vec![cert], key);
    initial.authorization_ttl_millis = 10_000;
    let metadata = match initial.value.as_ref().unwrap() {
        pb::resolve_tls_certificate_response::Value::Found(found) => {
            found.metadata.clone().unwrap()
        }
        _ => unreachable!(),
    };
    reply.send(Ok(initial)).unwrap();
    let config = callers
        .join_next()
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    let original_expiry = rig.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    // No caller queues this refresh: the real worker must discover the due
    // ten-second lease while the original authorization is still valid.
    until(|| !rig.rx.is_empty()).await;
    let (request, reply) = rig.next().await;
    assert_eq!(request.server_name, "app.example");
    assert_eq!(request.known_view_revision, Some(1));
    assert!(Instant::now() < original_expiry);
    let refresh_started = Instant::now();
    reply
        .send(Ok(pb::ResolveTlsCertificateResponse {
            server_name: "app.example".into(),
            view_revision: 1,
            observed_at_unix_millis: wall_now(),
            authorization_ttl_millis: 10_000,
            value: Some(pb::resolve_tls_certificate_response::Value::Unchanged(
                metadata,
            )),
        }))
        .unwrap();
    until(|| !rig.cache.shared.state.lock().unwrap().entries["app.example"].pending).await;
    let renewed_expiry = rig.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    assert_eq!(renewed_expiry, refresh_started + Duration::from_secs(10));
    assert!(renewed_expiry > original_expiry);
    tokio::time::advance(original_expiry - Instant::now() + Duration::from_millis(1)).await;
    let selected = rig.cache.resolve("app.example").await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&config, &selected));
    assert!(rig.rx.try_recv().is_err(), "warm selection must not fetch");
    rig.finish().await;
}

#[tokio::test]
async fn cancelled_waiter_and_shutdown_join_the_real_blocking_validation_owner() {
    use futures_util::FutureExt;
    let fixture = CertificateFixture::new();
    let (cert, key) = material("app.example");
    fixture.publish("app.example", vec![cert], key).unwrap();
    let (entered_tx, entered) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    *fixture.cache.shared.validation_gate.lock().unwrap() = Some((entered_tx, blocked));
    let mut calls = JoinSet::new();
    let c = fixture.cache.clone();
    calls.spawn(async move { c.resolve("app.example").await });
    entered.await.unwrap();
    calls.abort_all();
    while calls.join_next().await.is_some() {}
    assert_eq!(fixture.cache.usage().fetches, 1);
    let cache = fixture.cache.clone();
    let mut finish = Box::pin(fixture.finish());
    assert!(finish.as_mut().now_or_never().is_none());
    until(|| cache.shared.state.lock().unwrap().stopped).await;
    assert!(
        finish.as_mut().now_or_never().is_none(),
        "shutdown may not detach an already-started parser"
    );
    assert_eq!(cache.usage().fetches, 1);
    drop(release);
    finish.await;
    assert_eq!(cache.usage().fetches, 0);
}

#[tokio::test]
async fn failed_background_refresh_preserves_only_the_original_authorization_lease() {
    let mut rig = Rig::new();
    let mut calls = JoinSet::new();
    let c = rig.cache.clone();
    calls.spawn(async move { c.resolve("app.example").await });
    let (_, reply) = rig.next().await;
    let (cert, key) = material("app.example");
    reply
        .send(Ok(response("app.example", 1, vec![cert], key)))
        .unwrap();
    assert!(calls.join_next().await.unwrap().unwrap().unwrap().is_some());
    let original = rig.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(67)).await;
    let (_, reply) = rig.next().await;
    reply
        .send(Err(CertificateLookupError::Unavailable))
        .unwrap();
    until(|| !rig.cache.shared.state.lock().unwrap().entries["app.example"].pending).await;
    assert!(rig.cache.resolve("app.example").await.unwrap().is_some());
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        original
    );
    tokio::time::advance(Duration::from_secs(234)).await;
    let c = rig.cache.clone();
    calls.spawn(async move { c.resolve("app.example").await });
    let (_, reply) = rig.next().await;
    reply
        .send(Err(CertificateLookupError::Unavailable))
        .unwrap();
    assert_eq!(
        calls.join_next().await.unwrap().unwrap().unwrap_err(),
        CertificateLookupError::Unavailable
    );
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        original
    );
    rig.finish().await;
}

#[tokio::test]
async fn adapter_clienthello_time_is_part_of_the_same_five_second_setup_deadline() {
    use futures_util::FutureExt;
    use tokio::io::AsyncWriteExt;
    use tokio_rustls::rustls;
    let mut rig = Rig::new();
    let adapter = crate::FrontlineTlsAdapter::new(rig.cache.clone());
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(rustls::RootCertStore::empty())
    .with_no_client_auth();
    let mut client = rustls::ClientConnection::new(
        Arc::new(config),
        rustls::pki_types::ServerName::try_from("app.example").unwrap(),
    )
    .unwrap();
    let mut hello = Vec::new();
    client.write_tls(&mut hello).unwrap();
    assert!(!hello.is_empty());
    let (mut client_io, server_io) = tokio::io::duplex(64 * 1024);
    tokio::time::pause();
    let start = Instant::now();
    let mut handshake = Box::pin(adapter.terminate(server_io));
    assert!(handshake.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(3)).await;
    client_io.write_all(&hello).await.unwrap();
    assert!(handshake.as_mut().now_or_never().is_none());
    let (request, held) = rig.next().await;
    assert_eq!(request.server_name, "app.example");
    assert_eq!(Instant::now() - start, Duration::from_secs(3));
    tokio::time::advance(Duration::from_secs(2) + Duration::from_millis(1)).await;
    assert!(matches!(
        handshake.await,
        Err(crate::TlsTerminationError::Certificate(
            CertificateLookupError::Deadline
        ))
    ));
    assert_eq!(Instant::now() - start, Duration::from_millis(5001));
    assert_eq!(
        rig.cache.usage().fetches,
        1,
        "RPC began after three seconds and has not reached its own3s deadline"
    );
    rig.finish().await;
    assert!(held.send(Err(CertificateLookupError::Unavailable)).is_err());
}
