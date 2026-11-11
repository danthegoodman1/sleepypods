use super::*;
use crate::store::{StoreError, StoreFuture, StoreResult};
use std::sync::Mutex;
#[derive(Default)]
struct TestStore {
    calls: Mutex<Vec<(String, tokio::time::Instant)>>,
    failures: std::sync::atomic::AtomicUsize,
}
impl ControlPlaneStore for TestStore {
    unexpected_store_methods!(
        publish_certificate,
        get_certificate_metadata,
        set_tls_binding,
        get_tls_binding,
        remove_certificate,
        resolve_tls_certificate,
        reencrypt_certificate,
        load_route_changes,
        load_route_change_revision,
        load_materialization_work_status,
        record_materialization_failure,
        enqueue_materialization,
        maintain_runtime_records,
        accept_wake,
        request_instance_deletion,
        finalize_instance_deletions,
        create_instance,
        get_instance,
        delete_instance,
        create_workload_class_version,
        load_workload_class_version,
        create_route_binding,
        get_route_binding,
        delete_route_binding,
        list_route_bindings_for_instance,
        resolve_route,
        compare_and_swap_instance_state,
        record_materialization,
        load_ready_materialization,
        load_active_materialization,
        load_materialization,
        complete_wake,
        begin_sleep,
        finalize_sleep,
        list_materialization_reconciliation_candidates,
        load_materialization_operational_metrics,
        claim_materialization_reconciliation,
        begin_materialization_effect,
        acknowledge_materialization_effect,
        renew_materialization_reconciliation_lease,
        release_materialization_reconciliation_lease,
        complete_wake_reconciliation,
        finalize_sleep_reconciliation,
        delete_materialization_reconciliation,
        force_delete_materialization,
        force_release_exclusivity_key,
        lookup_route_dependencies,
        put_http01_challenge,
        resolve_http01_challenge,
        delete_http01_challenge,
        expire_http01_challenges,
    );
    fn snapshot_tls_bindings(
        &self,
        hosts: Vec<TlsHostname>,
        known: Option<CertificateRevision>,
    ) -> StoreFuture<'_, StoreResult<Option<TlsBindingSnapshot>>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push((
                hosts.first().map_or_else(String::new, ToString::to_string),
                tokio::time::Instant::now(),
            ));
            if self
                .failures
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(StoreError::unavailable(
                    "controlled watch admission pressure",
                ));
            }
            if known == Some(CertificateRevision::ZERO) {
                return Ok(None);
            }
            Ok(Some(TlsBindingSnapshot {
                revision: CertificateRevision::ZERO,
                bindings: hosts
                    .into_iter()
                    .map(|hostname| TlsBinding {
                        hostname,
                        revision: CertificateRevision::ZERO,
                        last_invalidating_revision: CertificateRevision::ZERO,
                        certificate_id: None,
                    })
                    .collect(),
            }))
        })
    }
}
fn input(id: u64, host: &str) -> pb::WatchTlsCertificatesRequest {
    pb::WatchTlsCertificatesRequest {
        registration: id,
        hostnames: vec![host.into()],
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    let bound = std::time::Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(
            std::time::Instant::now() < bound,
            "producer barrier did not complete"
        );
        tokio::task::yield_now().await;
    }
}
#[test]
fn registration_rejects_invalid_replacement_without_changing_accepted_state() {
    let mut state = WatchState::default();
    state.register(input(1, "valid.example")).unwrap();
    let mut duplicate = input(2, "valid.example");
    duplicate.hostnames.push(duplicate.hostnames[0].clone());
    let mut excessive = input(2, "valid.example");
    excessive.hostnames = vec![excessive.hostnames[0].clone(); 1025];
    for request in [
        input(1, "valid.example"),
        input(0, "valid.example"),
        input(2, "Invalid.Example"),
        duplicate,
        excessive,
        input(2, ""),
    ] {
        assert!(state.register(request).is_err());
        assert_eq!(state.registration, 1);
        assert_eq!(state.pending.as_ref().unwrap()[0].as_str(), "valid.example");
    }
}
#[tokio::test]
async fn production_producer_rate_limits_registration_and_isolates_slow_delivery() {
    let store = Arc::new(TestStore::default());
    let broker = RouteSubscriptionBroker::new();
    let (send, requests) = mpsc::channel(1);
    let (responses, mut receive) = mpsc::channel(RESPONSE_QUEUE);
    let permit = Arc::new(
        broker
            .certificate_streams
            .clone()
            .try_acquire_owned()
            .unwrap(),
    );
    let mut tasks = tokio::task::JoinSet::new();
    let decoded = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = decoded.clone();
    tasks.spawn(produce(
        store.clone(),
        broker.clone(),
        tokio_stream::StreamExt::map(ReceiverStream::new(requests), move |request| {
            observed.fetch_add(1, Ordering::SeqCst);
            request
        }),
        responses,
        permit,
    ));
    send.send(Ok(input(1, "slow.example"))).await.unwrap();
    receive.recv().await.unwrap().unwrap(); // Actual first snapshot/query barrier.
    tokio::time::pause();
    tasks.spawn(async move {
        for id in 2..32 {
            if send.send(Ok(input(id, "slow.example"))).await.is_err() {
                break;
            }
        }
    });
    for count in 2..=4 {
        // Observe the producer taking the next registration before moving
        // its clock. A queued request alone does not establish that ordering.
        until(|| decoded.load(Ordering::SeqCst) >= count).await;
        tokio::time::advance(POLL + Duration::from_millis(1)).await;
        until(|| store.calls.lock().unwrap().len() >= count).await;
    }
    // The two-slot response queue is full; the third send is waiting. A
    // different production producer can synchronize while that peer is slow.
    let (fast, requests) = mpsc::channel(1);
    let (responses, mut received) = mpsc::channel(RESPONSE_QUEUE);
    let permit = Arc::new(
        broker
            .certificate_streams
            .clone()
            .try_acquire_owned()
            .unwrap(),
    );
    let fast_decoded = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = fast_decoded.clone();
    tasks.spawn(produce(
        store.clone(),
        broker.clone(),
        tokio_stream::StreamExt::map(ReceiverStream::new(requests), move |request| {
            observed.store(true, Ordering::SeqCst);
            request
        }),
        responses,
        permit,
    ));
    fast.send(Ok(input(1, "fast.example"))).await.unwrap();
    until(|| fast_decoded.load(Ordering::SeqCst)).await;
    tokio::time::advance(POLL + Duration::from_millis(1)).await;
    until(|| {
        store
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(h, _)| h == "fast.example")
    })
    .await;
    assert!(received.try_recv().unwrap().is_ok());
    let times: Vec<_> = store
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(h, _)| h == "slow.example")
        .map(|(_, t)| *t)
        .collect();
    assert_eq!(times.len(), 4);
    assert!(times.windows(2).all(|pair| pair[1] - pair[0] >= POLL));
    broker.shutdown();
    while tasks.join_next().await.is_some() {}
    assert_eq!(
        broker.certificate_streams.available_permits(),
        WATCH_STREAMS
    );
}
#[tokio::test]
async fn transient_registration_pressure_retains_one_request_until_its_fixed_setup_deadline() {
    let store = Arc::new(TestStore::default());
    store.failures.store(2, Ordering::SeqCst);
    let broker = RouteSubscriptionBroker::new();
    let (send, requests) = mpsc::channel(1);
    let (responses, mut received) = mpsc::channel(RESPONSE_QUEUE);
    let permit = Arc::new(
        broker
            .certificate_streams
            .clone()
            .try_acquire_owned()
            .unwrap(),
    );
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(produce(
        store.clone(),
        broker.clone(),
        ReceiverStream::new(requests),
        responses,
        permit,
    ));
    send.send(Ok(input(1, "retry.example"))).await.unwrap();
    let response = tokio::time::timeout(LOOKUP, received.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.registration, 1);
    assert_eq!(response.bindings[0].hostname, "retry.example");
    assert_eq!(
        store.calls.lock().unwrap().len(),
        3,
        "one accepted stream retries bounded read pressure without reconnecting"
    );
    broker.shutdown();
    while tasks.join_next().await.is_some() {}
    assert_eq!(
        broker.certificate_streams.available_permits(),
        WATCH_STREAMS
    );
}

#[tokio::test]
async fn empty_registration_acks_once_and_has_no_idle_database_polls() {
    let store = TestStore::default();
    let mut state = WatchState::default();
    state
        .register(pb::WatchTlsCertificatesRequest {
            registration: 1,
            hostnames: Vec::new(),
        })
        .unwrap();
    let first = state.poll(&store).await.unwrap().unwrap();
    assert_eq!(first.registration, 1);
    assert!(first.bindings.is_empty());
    for _ in 0..4 {
        assert!(state.poll(&store).await.unwrap().is_none());
    }
    assert_eq!(store.calls.lock().unwrap().len(), 1);
    state.register(input(2, "added.example")).unwrap();
    assert_eq!(
        state.poll(&store).await.unwrap().unwrap().bindings[0].hostname,
        "added.example"
    );
    assert_eq!(store.calls.lock().unwrap().len(), 2);
}
