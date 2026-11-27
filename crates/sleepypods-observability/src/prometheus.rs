use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http::{header::CONTENT_TYPE, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{net::TcpListener, task::JoinSet};

use super::{
    metrics::{MetricDescriptor, MetricKind, MetricLabel, ALL_METRICS},
    recorder::{MetricObservation, ObservabilityEvent, ObservabilitySink},
};

const HISTOGRAM_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone, Debug, Default)]
pub struct PrometheusMetricsSink {
    state: Arc<Mutex<PrometheusState>>,
}

#[derive(Clone, Debug, Default)]
struct PrometheusState {
    counters: HashMap<MetricSeries, f64>,
    gauges: HashMap<MetricSeries, f64>,
    histograms: HashMap<MetricSeries, HistogramSeries>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MetricSeries {
    name: &'static str,
    labels: Vec<MetricLabel>,
}

#[derive(Clone, Debug)]
struct HistogramSeries {
    buckets: Vec<u64>,
    count: u64,
    sum: f64,
}

type MetricsBody = Full<Bytes>;

impl PrometheusMetricsSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn render(&self) -> String {
        self.state
            .lock()
            .expect("prometheus metrics lock not poisoned")
            .render()
    }

    pub fn record_observation(&self, observation: MetricObservation) -> bool {
        let Some(descriptor) = ALL_METRICS
            .iter()
            .copied()
            .find(|descriptor| descriptor.name() == observation.name())
        else {
            return false;
        };
        let Some(series) = validated_series(descriptor, &observation) else {
            return false;
        };
        if !observation.value().is_finite() {
            return false;
        }

        self.state
            .lock()
            .expect("prometheus metrics lock not poisoned")
            .record(descriptor, series, observation.value());
        true
    }
}

impl ObservabilitySink for PrometheusMetricsSink {
    fn record(&self, event: ObservabilityEvent) {
        if let ObservabilityEvent::Metric(observation) = event {
            self.record_observation(observation);
        }
    }
}

impl PrometheusState {
    fn record(&mut self, descriptor: MetricDescriptor, series: MetricSeries, value: f64) {
        match descriptor.kind() {
            MetricKind::Counter => {
                *self.counters.entry(series).or_insert(0.0) += value;
            }
            MetricKind::Gauge => {
                self.gauges.insert(series, value);
            }
            MetricKind::Histogram => {
                self.histograms.entry(series).or_default().observe(value);
            }
        }
    }

    fn render(&self) -> String {
        let mut out = String::new();
        for descriptor in ALL_METRICS {
            out.push_str("# HELP ");
            out.push_str(descriptor.name());
            out.push(' ');
            out.push_str(&escape_help(descriptor.description()));
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str(descriptor.name());
            out.push(' ');
            out.push_str(descriptor.kind().as_str());
            out.push('\n');

            match descriptor.kind() {
                MetricKind::Counter => {
                    for (series, value) in sorted_series_values(&self.counters, *descriptor) {
                        render_sample(&mut out, descriptor.name(), series.labels(), value);
                    }
                }
                MetricKind::Gauge => {
                    for (series, value) in sorted_series_values(&self.gauges, *descriptor) {
                        render_sample(&mut out, descriptor.name(), series.labels(), value);
                    }
                }
                MetricKind::Histogram => {
                    let mut series = self
                        .histograms
                        .iter()
                        .filter(|(series, _)| series.name == descriptor.name())
                        .collect::<Vec<_>>();
                    series.sort_by(|(left, _), (right, _)| {
                        compare_labels(&left.labels, &right.labels)
                    });
                    for (series, histogram) in series {
                        for (bucket, count) in HISTOGRAM_BUCKETS.iter().zip(&histogram.buckets) {
                            render_sample_with_extra_label(
                                &mut out,
                                &format!("{}_bucket", descriptor.name()),
                                series.labels(),
                                ("le", bucket_label(*bucket)),
                                *count as f64,
                            );
                        }
                        render_sample_with_extra_label(
                            &mut out,
                            &format!("{}_bucket", descriptor.name()),
                            series.labels(),
                            ("le", "+Inf".to_owned()),
                            histogram.count as f64,
                        );
                        render_sample(
                            &mut out,
                            &format!("{}_sum", descriptor.name()),
                            series.labels(),
                            histogram.sum,
                        );
                        render_sample(
                            &mut out,
                            &format!("{}_count", descriptor.name()),
                            series.labels(),
                            histogram.count as f64,
                        );
                    }
                }
            }
        }
        out
    }
}

impl HistogramSeries {
    fn observe(&mut self, value: f64) {
        for (index, bucket) in HISTOGRAM_BUCKETS.iter().enumerate() {
            if value <= *bucket {
                self.buckets[index] += 1;
            }
        }
        self.count += 1;
        self.sum += value;
    }
}

impl Default for HistogramSeries {
    fn default() -> Self {
        Self {
            buckets: vec![0; HISTOGRAM_BUCKETS.len()],
            count: 0,
            sum: 0.0,
        }
    }
}

impl MetricSeries {
    fn labels(&self) -> &[MetricLabel] {
        &self.labels
    }
}

pub async fn serve_prometheus_metrics<ShutdownFuture, Collect, CollectFuture>(
    addr: SocketAddr,
    sink: PrometheusMetricsSink,
    shutdown: ShutdownFuture,
    collect: Collect,
) -> io::Result<()>
where
    ShutdownFuture: Future<Output = ()> + Send,
    Collect: Fn(PrometheusMetricsSink) -> CollectFuture + Clone + Send + Sync + 'static,
    CollectFuture: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    serve_prometheus_listener(listener, sink, shutdown, collect).await
}

pub async fn serve_prometheus_listener<ShutdownFuture, Collect, CollectFuture>(
    listener: TcpListener,
    sink: PrometheusMetricsSink,
    shutdown: ShutdownFuture,
    collect: Collect,
) -> io::Result<()>
where
    ShutdownFuture: Future<Output = ()> + Send,
    Collect: Fn(PrometheusMetricsSink) -> CollectFuture + Clone + Send + Sync + 'static,
    CollectFuture: Future<Output = ()> + Send + 'static,
{
    let mut shutdown = std::pin::pin!(shutdown);
    let mut connections = JoinSet::new();
    let capacity = Arc::new(tokio::sync::Semaphore::new(32));
    let mut exit_error = None;

    loop {
        while connections.try_join_next().is_some() {}
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(error);
                        break;
                    }
                };
                let permit = match capacity.clone().try_acquire_owned() { Ok(permit) => permit, Err(_) => continue };
                let sink = sink.clone();
                let collect = collect.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    let service = service_fn(move |request| {
                        handle_metrics_request(request, sink.clone(), collect.clone())
                    });
                    // One bounded scrape per connection: metrics has no streaming API.
                    let mut builder = http1::Builder::new();
                    builder.keep_alive(false).timer(TokioTimer::new())
                        .header_read_timeout(Duration::from_secs(5)).max_buf_size(16 * 1024);
                    let _ = tokio::time::timeout(Duration::from_secs(5),
                        builder.serve_connection(TokioIo::new(stream), service)).await;
                });
            }
        }
    }

    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }
    Ok(())
}

async fn handle_metrics_request<Collect, CollectFuture>(
    request: Request<Incoming>,
    sink: PrometheusMetricsSink,
    collect: Collect,
) -> Result<Response<MetricsBody>, Infallible>
where
    Collect: Fn(PrometheusMetricsSink) -> CollectFuture,
    CollectFuture: Future<Output = ()>,
{
    if request.uri().path() != "/metrics" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"not found\n")))
            .expect("static response is valid"));
    }

    collect(sink.clone()).await;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(sink.render())))
        .expect("static response is valid"))
}

fn validated_series(
    descriptor: MetricDescriptor,
    observation: &MetricObservation,
) -> Option<MetricSeries> {
    let mut labels = Vec::with_capacity(descriptor.label_keys().len());
    for expected_key in descriptor.label_keys() {
        let label = observation
            .labels()
            .iter()
            .find(|label| label.key() == *expected_key)?;
        labels.push(*label);
    }
    if labels.len() != observation.labels().len() {
        return None;
    }
    Some(MetricSeries {
        name: descriptor.name(),
        labels,
    })
}

fn sorted_series_values(
    values: &HashMap<MetricSeries, f64>,
    descriptor: MetricDescriptor,
) -> Vec<(&MetricSeries, f64)> {
    let mut series = values
        .iter()
        .filter(|(series, _)| series.name == descriptor.name())
        .map(|(series, value)| (series, *value))
        .collect::<Vec<_>>();
    series.sort_by(|(left, _), (right, _)| compare_labels(&left.labels, &right.labels));
    series
}

fn compare_labels(left: &[MetricLabel], right: &[MetricLabel]) -> std::cmp::Ordering {
    left.iter()
        .map(|label| (label.key().as_str(), label.value()))
        .cmp(
            right
                .iter()
                .map(|label| (label.key().as_str(), label.value())),
        )
}

fn render_sample(out: &mut String, name: &str, labels: &[MetricLabel], value: f64) {
    out.push_str(name);
    render_labels(out, labels, None);
    out.push(' ');
    out.push_str(&format_number(value));
    out.push('\n');
}

fn render_sample_with_extra_label(
    out: &mut String,
    name: &str,
    labels: &[MetricLabel],
    extra: (&str, String),
    value: f64,
) {
    out.push_str(name);
    render_labels(out, labels, Some(extra));
    out.push(' ');
    out.push_str(&format_number(value));
    out.push('\n');
}

fn render_labels(out: &mut String, labels: &[MetricLabel], extra: Option<(&str, String)>) {
    if labels.is_empty() && extra.is_none() {
        return;
    }
    out.push('{');
    let mut first = true;
    for label in labels {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(label.key().as_str());
        out.push_str("=\"");
        out.push_str(&escape_label_value(label.value()));
        out.push('"');
    }
    if let Some((key, value)) = extra {
        if !first {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(&escape_label_value(&value));
        out.push('"');
    }
    out.push('}');
}

fn escape_help(value: &str) -> String {
    value.replace('\\', r"\\").replace('\n', r"\n")
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}

fn bucket_label(value: f64) -> String {
    format_number(value)
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metrics::{
            KUBERNETES_OPERATIONS_TOTAL, KUBERNETES_OPERATION_DURATION_SECONDS,
            RECONCILER_CLAIMS_TOTAL, RUNTIME_ACTIVE_STREAMS,
        },
        recorder::MetricObservation,
        Operation, Outcome,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn exposition_renders_help_type_and_accumulated_samples() {
        let sink = PrometheusMetricsSink::new();
        assert!(sink.record_observation(MetricObservation::new(
            KUBERNETES_OPERATIONS_TOTAL,
            vec![
                Operation::Apply.metric_label(),
                Outcome::Success.metric_label(),
            ],
            1.0,
        )));
        assert!(sink.record_observation(MetricObservation::new(
            KUBERNETES_OPERATIONS_TOTAL,
            vec![
                Operation::Apply.metric_label(),
                Outcome::Success.metric_label(),
            ],
            2.0,
        )));

        let output = sink.render();

        assert!(output.contains("# HELP sleepypods_kubernetes_operations_total "));
        assert!(output.contains("# TYPE sleepypods_kubernetes_operations_total counter\n"));
        assert!(output.contains(
            "sleepypods_kubernetes_operations_total{operation=\"apply\",outcome=\"success\"} 3\n"
        ));
    }

    #[test]
    fn gauges_retain_latest_value() {
        let sink = PrometheusMetricsSink::new();
        sink.record_observation(MetricObservation::new(RUNTIME_ACTIVE_STREAMS, vec![], 4.0));
        sink.record_observation(MetricObservation::new(RUNTIME_ACTIVE_STREAMS, vec![], 2.0));

        assert!(sink
            .render()
            .contains("sleepypods_runtime_active_streams 2\n"));
    }

    #[test]
    fn rejects_labels_outside_descriptor_allow_list() {
        let sink = PrometheusMetricsSink::new();
        assert!(!sink.record_observation(MetricObservation::new(
            RUNTIME_ACTIVE_STREAMS,
            vec![Outcome::Success.metric_label()],
            1.0,
        )));

        assert!(!sink
            .render()
            .contains("sleepypods_runtime_active_streams 1\n"));
    }

    #[test]
    fn histogram_renders_buckets_count_and_sum() {
        let sink = PrometheusMetricsSink::new();
        sink.record_observation(MetricObservation::new(
            KUBERNETES_OPERATION_DURATION_SECONDS,
            vec![
                Operation::Readiness.metric_label(),
                Outcome::Success.metric_label(),
            ],
            0.2,
        ));
        sink.record_observation(MetricObservation::new(
            KUBERNETES_OPERATION_DURATION_SECONDS,
            vec![
                Operation::Readiness.metric_label(),
                Outcome::Success.metric_label(),
            ],
            12.0,
        ));

        let output = sink.render();

        assert!(output.contains(
            "sleepypods_kubernetes_operation_duration_seconds_bucket{operation=\"readiness\",outcome=\"success\",le=\"0.25\"} 1\n"
        ));
        assert!(output.contains(
            "sleepypods_kubernetes_operation_duration_seconds_bucket{operation=\"readiness\",outcome=\"success\",le=\"+Inf\"} 2\n"
        ));
        assert!(output.contains(
            "sleepypods_kubernetes_operation_duration_seconds_sum{operation=\"readiness\",outcome=\"success\"} 12.2\n"
        ));
        assert!(output.contains(
            "sleepypods_kubernetes_operation_duration_seconds_count{operation=\"readiness\",outcome=\"success\"} 2\n"
        ));
    }

    #[test]
    fn label_values_are_escaped_for_text_exposition() {
        assert_eq!(escape_label_value("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    #[test]
    fn descriptor_label_allow_list_blocks_duplicate_label_keys() {
        let sink = PrometheusMetricsSink::new();
        assert!(!sink.record_observation(MetricObservation::new(
            RECONCILER_CLAIMS_TOTAL,
            vec![
                MetricLabel::state("pending"),
                MetricLabel::state("deleting")
            ],
            1.0,
        )));
    }

    #[tokio::test]
    async fn metrics_socket_flood_is_bounded_and_shutdown_cancels_stalled_collectors() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve_prometheus_listener(
            listener,
            PrometheusMetricsSink::new(),
            async {
                let _ = stopped.await;
            },
            |_| std::future::pending::<()>(),
        ));
        let mut held = Vec::new();
        for _ in 0..32 {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            held.push(stream);
            tokio::task::yield_now().await;
        }
        let mut excess = tokio::net::TcpStream::connect(addr).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), excess.read(&mut [0; 1]))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0) | Err(_)));
        held[0]
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: metrics\r\n\r\n")
            .await
            .unwrap();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn metrics_listener_serves_metrics_without_intercepting_other_paths() {
        let sink = PrometheusMetricsSink::new();
        sink.record_observation(MetricObservation::new(
            KUBERNETES_OPERATIONS_TOTAL,
            vec![
                Operation::Delete.metric_label(),
                Outcome::Error.metric_label(),
            ],
            1.0,
        ));
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
            |_| async {},
        ));

        let metrics = raw_http_get(addr, "/metrics").await;
        let missing = raw_http_get(addr, "/not-metrics").await;
        let _ = shutdown_tx.send(());
        task.await
            .expect("server task joins")
            .expect("server exits cleanly");

        assert!(metrics.starts_with("HTTP/1.1 200 OK"));
        assert!(metrics.contains(
            "sleepypods_kubernetes_operations_total{operation=\"delete\",outcome=\"error\"} 1\n"
        ));
        assert!(missing.starts_with("HTTP/1.1 404 Not Found"));
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
