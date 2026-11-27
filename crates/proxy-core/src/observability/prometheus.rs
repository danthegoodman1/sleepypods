//! Runtime collector adapters for the shared Prometheus exporter.
use super::{
    metrics::{RUNTIME_ACTIVE_STREAMS, RUNTIME_POD_ROUTE_CONNECTS_TOTAL},
    recorder::MetricObservation,
};
use crate::{
    drain::DrainTracker,
    pod_route::{pod_route_connects, PodRouteConnects},
};
pub use sleepypods_observability::prometheus::*;
use sleepypods_observability::PodRouteOutcome;
use std::sync::{Arc, Mutex};

/// Samples `sleepypods_runtime_active_streams` from a `DrainTracker` at
/// metrics-collect time, so permit acquire/release stays free of metric
/// emission on the request path.
#[derive(Clone, Debug, Default)]
pub struct RuntimeActiveStreamsCollector {
    drain: Arc<Mutex<Option<DrainTracker>>>,
}

impl RuntimeActiveStreamsCollector {
    pub fn attach(&self, drain: DrainTracker) {
        *self
            .drain
            .lock()
            .expect("active streams collector lock not poisoned") = Some(drain);
    }

    pub fn collect(&self, sink: PrometheusMetricsSink) {
        let active = self
            .drain
            .lock()
            .expect("active streams collector lock not poisoned")
            .as_ref()
            .map(DrainTracker::active_count)
            .unwrap_or(0);
        sink.record_observation(MetricObservation::new(
            RUNTIME_ACTIVE_STREAMS,
            Vec::new(),
            active as f64,
        ));
    }
}

/// Exports the process's Pod-route attempts at metrics-collect time, so a
/// connect only increments an atomic.
#[derive(Clone, Debug)]
pub struct PodRouteConnectsCollector {
    connects: &'static PodRouteConnects,
    reported: Arc<Mutex<[u64; PodRouteOutcome::ALL.len()]>>,
}

impl Default for PodRouteConnectsCollector {
    fn default() -> Self {
        Self::of(pod_route_connects())
    }
}

impl PodRouteConnectsCollector {
    fn of(connects: &'static PodRouteConnects) -> Self {
        Self {
            connects,
            reported: Arc::default(),
        }
    }

    /// Adds the attempts since the last collection to the exported counter.
    pub fn collect(&self, sink: &PrometheusMetricsSink) {
        let mut reported = self
            .reported
            .lock()
            .expect("pod route collector lock not poisoned");
        for outcome in PodRouteOutcome::ALL {
            let total = self.connects.get(outcome);
            let last = &mut reported[outcome as usize];
            if total > *last {
                sink.record_observation(MetricObservation::new(
                    RUNTIME_POD_ROUTE_CONNECTS_TOTAL,
                    vec![outcome.metric_label()],
                    (total - *last) as f64,
                ));
                *last = total;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    // Proves the scrape-time wiring used by the frontline and sidecar bins:
    // /metrics renders the runtime active-streams gauge from the live
    // DrainTracker count instead of per-permit metric events.
    #[tokio::test]
    async fn metrics_endpoint_samples_runtime_active_streams_via_collector() {
        let sink = PrometheusMetricsSink::new();
        let collector = RuntimeActiveStreamsCollector::default();
        let drain = crate::drain::DrainTracker::new(std::time::Duration::from_secs(5));
        collector.attach(drain.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("metrics listener binds");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve_prometheus_listener(
            listener,
            sink,
            async move {
                let _ = shutdown_rx.await;
            },
            move |sink| {
                let collector = collector.clone();
                async move {
                    collector.collect(sink);
                }
            },
        ));

        let permit = drain.try_acquire().expect("work admitted");
        let with_active = raw_http_get(addr, "/metrics").await;
        drop(permit);
        let after_release = raw_http_get(addr, "/metrics").await;
        let _ = shutdown_tx.send(());
        task.await
            .expect("server task joins")
            .expect("server exits cleanly");

        assert!(with_active.contains("sleepypods_runtime_active_streams 1\n"));
        assert!(after_release.contains("sleepypods_runtime_active_streams 0\n"));
    }

    #[test]
    fn pod_route_connects_export_the_attempts_since_the_last_collection() {
        let connects: &'static PodRouteConnects = Box::leak(Box::new(PodRouteConnects::new()));
        let collector = PodRouteConnectsCollector::of(connects);
        let sink = PrometheusMetricsSink::new();
        connects.count(PodRouteOutcome::Pod);
        connects.count(PodRouteOutcome::Pod);
        connects.count(PodRouteOutcome::ClaimRejected);

        collector.collect(&sink);
        collector.collect(&sink);
        connects.count(PodRouteOutcome::Pod);
        collector.collect(&sink);

        let rendered = sink.render();
        assert!(
            rendered.contains("sleepypods_runtime_pod_route_connects_total{outcome=\"pod\"} 3\n")
        );
        assert!(rendered.contains(
            "sleepypods_runtime_pod_route_connects_total{outcome=\"claim_rejected\"} 1\n"
        ));
        assert!(
            !rendered.contains("sleepypods_runtime_pod_route_connects_total{outcome=\"timeout\"}")
        );
    }

    async fn raw_http_get(addr: SocketAddr, path: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to metrics listener");
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .expect("write request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("read response");
        response
    }
}
