//! Runtime collector adapter for the shared Prometheus exporter.
use super::{metrics::RUNTIME_ACTIVE_STREAMS, recorder::MetricObservation};
use crate::drain::DrainTracker;
pub use sleepypods_observability::prometheus::*;
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
