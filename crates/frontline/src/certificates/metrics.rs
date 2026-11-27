//! Scrape owned cache/supervisor state; event labels are fixed shared enums.
use super::*;
use proxy_core::observability::{
    metrics::*,
    prometheus::PrometheusMetricsSink,
    recorder::{MetricObservation, ObservabilityEvent},
};
use std::sync::atomic::Ordering;

impl TlsCertificateStore {
    pub fn collect_metrics(&self, sink: &PrometheusMetricsSink) {
        let now = Instant::now();
        let wall = (self.shared.wall)();
        let state = self.shared.state.lock().unwrap();
        let risk = state
            .entries
            .values()
            .filter(|entry| {
                entry.value.as_ref().is_some_and(|view| {
                    view.config.is_some()
                        && (view.expires.saturating_duration_since(now) <= Duration::from_secs(60)
                            || view.metadata.as_ref().is_some_and(|m| {
                                m.not_after_unix_millis.saturating_sub(wall) <= 60_000
                            }))
                })
            })
            .count();
        drop(state);
        let usage = self.usage();
        for (descriptor, value) in [
            (RUNTIME_CERTIFICATE_ENTRIES, usage.certificates),
            (RUNTIME_CERTIFICATE_NEGATIVE_ENTRIES, usage.negative_entries),
            (RUNTIME_CERTIFICATE_ACCOUNTED_BYTES, usage.certificate_bytes),
            (RUNTIME_CERTIFICATE_FETCHES, usage.fetches),
            (RUNTIME_CERTIFICATE_QUEUE, usage.queued),
            (
                RUNTIME_CERTIFICATE_TASKS,
                self.shared.retained_tasks.load(Ordering::Relaxed),
            ),
            (
                RUNTIME_CERTIFICATE_TASK_HIGH_WATER,
                self.shared.task_high_water.load(Ordering::Relaxed),
            ),
            (
                RUNTIME_CERTIFICATE_WATCHES,
                self.shared.watches.load(Ordering::Relaxed),
            ),
            (RUNTIME_CERTIFICATE_EXPIRY_RISK, risk),
        ] {
            sink.record_observation(MetricObservation::new(descriptor, Vec::new(), value as f64));
        }
    }
    pub(super) fn event(&self, operation: Operation, outcome: Outcome) {
        self.shared.observability.record_lazy(|| {
            ObservabilityEvent::Metric(MetricObservation::new(
                RUNTIME_CERTIFICATE_EVENTS_TOTAL,
                vec![operation.metric_label(), outcome.metric_label()],
                1.0,
            ))
        });
    }
    pub(super) fn tasks(&self, count: usize) {
        self.shared.retained_tasks.store(count, Ordering::Relaxed);
        self.shared
            .task_high_water
            .fetch_max(count, Ordering::Relaxed);
    }
}
pub(super) fn outcome(error: CertificateLookupError) -> Outcome {
    match error {
        CertificateLookupError::Invalid | CertificateLookupError::Unavailable => Outcome::Error,
        CertificateLookupError::Capacity => Outcome::Rejected,
        CertificateLookupError::Deadline => Outcome::Timeout,
        CertificateLookupError::Stopped => Outcome::Canceled,
    }
}
pub(super) struct WatchObservation(TlsCertificateStore);
impl WatchObservation {
    pub fn new(cache: TlsCertificateStore) -> Self {
        cache.shared.watches.fetch_add(1, Ordering::Relaxed);
        cache.event(Operation::CertificateWatch, Outcome::Started);
        Self(cache)
    }
}
impl Drop for WatchObservation {
    fn drop(&mut self) {
        self.0.shared.watches.fetch_sub(1, Ordering::Relaxed);
        self.0.event(Operation::CertificateWatch, Outcome::Closed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proxy_core::Shutdown;
    use tokio::{sync::oneshot, task::JoinSet};
    use tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer;
    type Reply = oneshot::Sender<Result<pb::ResolveTlsCertificateResponse, CertificateLookupError>>;
    struct Resolver(mpsc::Sender<(pb::ResolveTlsCertificateRequest, Reply)>);
    impl CertificateResolver for Resolver {
        fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
            let send = self.0.clone();
            Box::pin(async move {
                let (reply, response) = oneshot::channel();
                send.send((request, reply))
                    .await
                    .map_err(|_| CertificateLookupError::Stopped)?;
                response
                    .await
                    .map_err(|_| CertificateLookupError::Stopped)?
            })
        }
    }
    struct PendingWatch;
    impl CertificateWatcher for PendingWatch {
        fn watch(
            &self,
            interests: mpsc::Receiver<pb::WatchTlsCertificatesRequest>,
        ) -> CertificateWatchFuture {
            Box::pin(async move {
                let _owned_interests = interests;
                std::future::pending().await
            })
        }
    }
    async fn until(mut ready: impl FnMut() -> bool) {
        let bound = std::time::Instant::now() + Duration::from_secs(2);
        while !ready() {
            assert!(
                std::time::Instant::now() < bound,
                "metrics ownership barrier timed out"
            );
            tokio::task::yield_now().await;
        }
    }
    #[tokio::test]
    async fn scrapes_real_owned_work_and_emits_bounded_secret_free_outcomes() {
        let sink = PrometheusMetricsSink::new();
        let (send, mut requests) = mpsc::channel(3);
        let (cache, worker) = TlsCertificateStore::with_observability(
            Arc::new(Resolver(send)),
            ObservabilityRecorder::new(Arc::new(sink.clone())),
        );
        let worker = worker.with_watch(Arc::new(PendingWatch));
        let shutdown = Shutdown::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(worker.run(shutdown.clone()));
        let mut callers = JoinSet::new();
        let lookup = cache.clone();
        callers.spawn(async move { lookup.resolve("secret-host.example").await });
        let (request, reply) = requests.recv().await.unwrap();
        cache.collect_metrics(&sink);
        let pending = sink.render();
        for sample in [
            "entries 0",
            "negative_entries 1",
            "accounted_bytes 0",
            "fetches 1",
            "queue 0",
            "tasks 1",
            "task_high_water 1",
            "watches 0",
            "expiry_risk 0",
        ] {
            assert!(
                pending.contains(&format!("sleepypods_runtime_certificate_{sample}\n")),
                "missing {sample}"
            );
        }
        let cert = rcgen::generate_simple_self_signed(vec![request.server_name.clone()]).unwrap();
        let key = cert.signing_key.serialize_der();
        let material = cert.cert.der().len() + key.len();
        let mut found = crate::certificates::test_support::response(
            &request.server_name,
            1,
            vec![cert.cert.der().clone()],
            PrivatePkcs8KeyDer::from(key).into(),
        );
        found.authorization_ttl_millis = 10_000;
        reply.send(Ok(found)).unwrap();
        assert!(callers
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_some());
        until(|| cache.shared.retained_tasks.load(Ordering::Relaxed) == 0).await;
        until(|| cache.shared.watches.load(Ordering::Relaxed) == 1).await;
        cache.collect_metrics(&sink);
        let installed = sink.render();
        for sample in [
            "entries 1".to_owned(),
            "negative_entries 0".to_owned(),
            format!("accounted_bytes {material}"),
            "watches 1".to_owned(),
            "expiry_risk 1".to_owned(),
        ] {
            assert!(
                installed.contains(&format!("sleepypods_runtime_certificate_{sample}\n")),
                "missing {sample}"
            );
        }
        let before_warm = sink.render();
        for _ in 0..10 {
            assert!(cache
                .resolve("secret-host.example")
                .await
                .unwrap()
                .is_some());
        }
        assert_eq!(
            sink.render(),
            before_warm,
            "warm selection emits no fetch/refresh work"
        );
        let lookup = cache.clone();
        callers.spawn(async move { lookup.resolve("invalid-secret.example").await });
        let (request, reply) = requests.recv().await.unwrap();
        reply
            .send(Ok(pb::ResolveTlsCertificateResponse {
                server_name: request.server_name,
                view_revision: 2,
                observed_at_unix_millis: wall_now(),
                authorization_ttl_millis: 1000,
                value: Some(pb::resolve_tls_certificate_response::Value::Found(
                    pb::FoundTlsCertificate {
                        metadata: None,
                        bundle: Some(pb::CertificateBundle {
                            chain_der: Vec::new(),
                            private_key_pkcs8_der: b"DO-NOT-LOG-KEY".to_vec(),
                        }),
                    },
                )),
            }))
            .unwrap();
        assert!(callers.join_next().await.unwrap().unwrap().is_err());
        shutdown.shutdown();
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        cache.collect_metrics(&sink);
        let finished = sink.render();
        for sample in [
            "entries 0",
            "negative_entries 0",
            "accounted_bytes 0",
            "fetches 0",
            "queue 0",
            "tasks 0",
            "watches 0",
        ] {
            assert!(finished.contains(&format!("sleepypods_runtime_certificate_{sample}\n")));
        }
        for sample in [
            "{operation=\"certificate_fetch\",outcome=\"started\"} 2",
            "{operation=\"certificate_fetch\",outcome=\"success\"} 2",
            "{operation=\"certificate_install\",outcome=\"error\"} 1",
            "{operation=\"certificate_watch\",outcome=\"started\"} 1",
            "{operation=\"certificate_watch\",outcome=\"closed\"} 1",
        ] {
            assert!(finished.contains(sample), "missing {sample}");
        }
        for secret in [
            "secret-host",
            "invalid-secret",
            "DO-NOT-LOG-KEY",
            "private_key",
        ] {
            assert!(!finished.contains(secret));
        }
    }
}
