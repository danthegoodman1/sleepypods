use std::{
    fmt,
    sync::{Arc, Mutex, OnceLock},
};

use super::metrics::{MetricDescriptor, MetricLabel};

pub const EVENT_DRAIN_STARTED: &str = "runtime.drain.started";
pub const EVENT_DRAIN_COMPLETED: &str = "runtime.drain.completed";
pub const EVENT_DRAIN_TIMEOUT: &str = "runtime.drain.timeout";
pub const EVENT_ROUTE_CACHE_LOOKUP: &str = "runtime.route_cache.lookup";
pub const EVENT_SUBSCRIBE_STREAM: &str = "runtime.subscribe_stream.event";
pub const EVENT_WAKE: &str = "runtime.wake.event";
pub const EVENT_MATERIALIZATION_FAILURE: &str = "runtime.materialization.failure";
pub const EVENT_IDLE_REPORT: &str = "runtime.idle_report.event";
pub const EVENT_HTTP01: &str = "runtime.http01.event";
pub const EVENT_CONTROL_PLANE_AUTH: &str = "control_plane.auth.decision";
pub const EVENT_CONTROL_PLANE_STORE_ERROR: &str = "control_plane.store.error";

pub const FIELD_INSTANCE_ID: &str = "instance.id";
pub const FIELD_ROUTE_ID: &str = "route.id";
pub const FIELD_SUBSCRIPTION_ID: &str = "subscription.id";
pub const FIELD_GENERATION: &str = "generation";
pub const FIELD_BACKEND_GENERATION: &str = "backend.generation";
pub const FIELD_CLUSTER_ID: &str = "cluster.id";
pub const FIELD_NAMESPACE: &str = "namespace";
pub const FIELD_ERROR_REASON: &str = "error.reason";
pub const FIELD_ERROR_MESSAGE: &str = "error.message";
pub const FIELD_ACTIVE_COUNT: &str = "active.count";
pub const FIELD_DURATION_MS: &str = "duration.ms";
pub const FIELD_EXCLUSIVITY_ACTION: &str = "exclusivity.action";
pub const FIELD_EXCLUSIVITY_KEY_NAME: &str = "exclusivity.key.name";
pub const FIELD_EXCLUSIVITY_OWNER_INSTANCE_ID: &str = "exclusivity.owner.instance.id";
pub const FIELD_AUTH_DECISION: &str = "auth.decision";
pub const FIELD_AUTH_REASON: &str = "auth.reason";
pub const FIELD_AUTH_CALLER_ROLE: &str = "auth.caller.role";
pub const FIELD_AUTH_REQUIRED_ROLE: &str = "auth.required.role";
pub const FIELD_GRPC_SERVICE: &str = "grpc.service";

pub const LIFECYCLE_FIELDS: &[&str] = &[
    FIELD_INSTANCE_ID,
    FIELD_ROUTE_ID,
    FIELD_SUBSCRIPTION_ID,
    FIELD_GENERATION,
    FIELD_BACKEND_GENERATION,
    FIELD_CLUSTER_ID,
    FIELD_NAMESPACE,
    FIELD_ERROR_REASON,
    FIELD_ERROR_MESSAGE,
    FIELD_ACTIVE_COUNT,
    FIELD_DURATION_MS,
    FIELD_EXCLUSIVITY_ACTION,
    FIELD_EXCLUSIVITY_KEY_NAME,
    FIELD_EXCLUSIVITY_OWNER_INSTANCE_ID,
    FIELD_AUTH_DECISION,
    FIELD_AUTH_REASON,
    FIELD_AUTH_CALLER_ROLE,
    FIELD_AUTH_REQUIRED_ROLE,
    FIELD_GRPC_SERVICE,
];

#[derive(Clone, Debug, PartialEq)]
pub struct MetricObservation {
    name: &'static str,
    labels: Vec<MetricLabel>,
    value: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleLogEvent {
    name: &'static str,
    fields: Vec<LogField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogField {
    key: &'static str,
    value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ObservabilityEvent {
    Metric(MetricObservation),
    Log(LifecycleLogEvent),
}

pub trait ObservabilitySink: Send + Sync {
    fn record(&self, event: ObservabilityEvent);
}

#[derive(Clone)]
pub struct ObservabilityRecorder {
    sink: Option<Arc<dyn ObservabilitySink>>,
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryObservability {
    events: Arc<Mutex<Vec<ObservabilityEvent>>>,
}

#[derive(Debug)]
pub struct StderrObservabilitySink;
/// Stderr sink that keeps lifecycle and error events but drops metric samples
/// and per-request events, so installing stderr observability adds no
/// formatted writes on the hot forwarding path. Metrics are exported through
/// the Prometheus sink at scrape time instead.
#[derive(Debug)]
pub struct FilteredStderrObservabilitySink;
#[derive(Clone)]
pub struct CompositeObservabilitySink {
    sinks: Vec<Arc<dyn ObservabilitySink>>,
}

static GLOBAL_OBSERVABILITY_SINK: OnceLock<Arc<dyn ObservabilitySink>> = OnceLock::new();

impl MetricObservation {
    pub fn new(descriptor: MetricDescriptor, labels: Vec<MetricLabel>, value: f64) -> Self {
        Self {
            name: descriptor.name(),
            labels,
            value,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn labels(&self) -> &[MetricLabel] {
        &self.labels
    }

    pub fn value(&self) -> f64 {
        self.value
    }
}

impl LifecycleLogEvent {
    pub fn new(name: &'static str, fields: Vec<LogField>) -> Self {
        Self { name, fields }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn fields(&self) -> &[LogField] {
        &self.fields
    }

    pub fn field_value(&self, key: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|field| field.key == key)
            .map(|field| field.value.as_str())
    }
}

impl LogField {
    pub fn new(key: &'static str, value: impl ToString) -> Self {
        Self {
            key,
            value: value.to_string(),
        }
    }

    pub fn key(&self) -> &'static str {
        self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn instance_id(value: impl ToString) -> Self {
        Self::new(FIELD_INSTANCE_ID, value)
    }

    pub fn route_id(value: impl ToString) -> Self {
        Self::new(FIELD_ROUTE_ID, value)
    }

    pub fn subscription_id(value: impl ToString) -> Self {
        Self::new(FIELD_SUBSCRIPTION_ID, value)
    }

    pub fn generation(value: impl ToString) -> Self {
        Self::new(FIELD_GENERATION, value)
    }

    pub fn backend_generation(value: impl ToString) -> Self {
        Self::new(FIELD_BACKEND_GENERATION, value)
    }

    pub fn cluster_id(value: impl ToString) -> Self {
        Self::new(FIELD_CLUSTER_ID, value)
    }

    pub fn namespace(value: impl ToString) -> Self {
        Self::new(FIELD_NAMESPACE, value)
    }

    pub fn error_reason(value: impl ToString) -> Self {
        Self::new(FIELD_ERROR_REASON, value)
    }

    pub fn active_count(value: impl ToString) -> Self {
        Self::new(FIELD_ACTIVE_COUNT, value)
    }

    pub fn duration_ms(value: impl ToString) -> Self {
        Self::new(FIELD_DURATION_MS, value)
    }

    pub fn exclusivity_action(value: impl ToString) -> Self {
        Self::new(FIELD_EXCLUSIVITY_ACTION, value)
    }

    pub fn exclusivity_key_name(value: impl ToString) -> Self {
        Self::new(FIELD_EXCLUSIVITY_KEY_NAME, value)
    }

    pub fn exclusivity_owner_instance_id(value: impl ToString) -> Self {
        Self::new(FIELD_EXCLUSIVITY_OWNER_INSTANCE_ID, value)
    }

    pub fn auth_decision(value: impl ToString) -> Self {
        Self::new(FIELD_AUTH_DECISION, value)
    }

    pub fn auth_reason(value: impl ToString) -> Self {
        Self::new(FIELD_AUTH_REASON, value)
    }

    pub fn auth_caller_role(value: impl ToString) -> Self {
        Self::new(FIELD_AUTH_CALLER_ROLE, value)
    }

    pub fn auth_required_role(value: impl ToString) -> Self {
        Self::new(FIELD_AUTH_REQUIRED_ROLE, value)
    }

    pub fn grpc_service(value: impl ToString) -> Self {
        Self::new(FIELD_GRPC_SERVICE, value)
    }
}

impl ObservabilityRecorder {
    pub fn noop() -> Self {
        Self { sink: None }
    }

    pub fn new(sink: Arc<dyn ObservabilitySink>) -> Self {
        Self { sink: Some(sink) }
    }

    pub fn stderr() -> Self {
        Self::new(Arc::new(StderrObservabilitySink))
    }

    pub fn global() -> Self {
        GLOBAL_OBSERVABILITY_SINK
            .get()
            .cloned()
            .map(Self::new)
            .unwrap_or_else(Self::noop)
    }

    pub fn install_global(sink: Arc<dyn ObservabilitySink>) -> bool {
        GLOBAL_OBSERVABILITY_SINK.set(sink).is_ok()
    }

    pub fn install_stderr_global() -> bool {
        Self::install_global(Arc::new(StderrObservabilitySink))
    }

    pub fn record_metric(&self, observation: MetricObservation) {
        self.record_lazy(|| ObservabilityEvent::Metric(observation));
    }

    pub fn record_log(&self, event: LifecycleLogEvent) {
        self.record_lazy(|| ObservabilityEvent::Log(event));
    }

    /// Construct a potentially expensive observation only when a sink exists.
    /// Explicitly configured sinks always receive the constructed event.
    pub fn record_lazy(&self, event: impl FnOnce() -> ObservabilityEvent) {
        if let Some(sink) = &self.sink {
            sink.record(event());
        }
    }
}

impl Default for ObservabilityRecorder {
    fn default() -> Self {
        Self::noop()
    }
}

impl fmt::Debug for ObservabilityRecorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservabilityRecorder")
            .finish_non_exhaustive()
    }
}

impl InMemoryObservability {
    pub fn recorder(&self) -> ObservabilityRecorder {
        ObservabilityRecorder::new(Arc::new(self.clone()))
    }

    pub fn events(&self) -> Vec<ObservabilityEvent> {
        self.events
            .lock()
            .expect("observability event lock not poisoned")
            .clone()
    }
}

impl CompositeObservabilitySink {
    pub fn new(sinks: Vec<Arc<dyn ObservabilitySink>>) -> Self {
        Self { sinks }
    }
}

impl ObservabilitySink for InMemoryObservability {
    fn record(&self, event: ObservabilityEvent) {
        self.events
            .lock()
            .expect("observability event lock not poisoned")
            .push(event);
    }
}

impl ObservabilitySink for StderrObservabilitySink {
    fn record(&self, event: ObservabilityEvent) {
        match event {
            ObservabilityEvent::Metric(metric) => {
                let labels = metric
                    .labels()
                    .iter()
                    .map(|label| format!("{}={}", label.key().as_str(), label.value()))
                    .collect::<Vec<_>>()
                    .join(",");
                eprintln!(
                    "observability.type=metric metric.name={} metric.value={} metric.labels={}",
                    metric.name(),
                    metric.value(),
                    labels
                );
            }
            ObservabilityEvent::Log(log) => {
                let fields = log
                    .fields()
                    .iter()
                    .map(|field| format!("{}={}", field.key(), field.value()))
                    .collect::<Vec<_>>()
                    .join(" ");
                eprintln!(
                    "observability.type=event event.name={} {}",
                    log.name(),
                    fields
                );
            }
        }
    }
}

impl ObservabilitySink for FilteredStderrObservabilitySink {
    fn record(&self, event: ObservabilityEvent) {
        if suppressed_on_stderr(&event) {
            return;
        }

        StderrObservabilitySink.record(event);
    }
}

fn suppressed_on_stderr(event: &ObservabilityEvent) -> bool {
    match event {
        ObservabilityEvent::Metric(_) => true,
        ObservabilityEvent::Log(log) => log.name() == EVENT_ROUTE_CACHE_LOOKUP,
    }
}

impl ObservabilitySink for CompositeObservabilitySink {
    fn record(&self, event: ObservabilityEvent) {
        for sink in &self.sinks {
            sink.record(event.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        suppressed_on_stderr, InMemoryObservability, LifecycleLogEvent, LogField,
        MetricObservation, ObservabilityEvent, EVENT_DRAIN_STARTED, EVENT_ROUTE_CACHE_LOOKUP,
        EVENT_WAKE, FIELD_INSTANCE_ID, LIFECYCLE_FIELDS,
    };
    use crate::metrics::RUNTIME_ACTIVE_STREAMS;

    #[test]
    fn lifecycle_log_fields_include_required_identity_context() {
        for field in [
            "instance.id",
            "route.id",
            "subscription.id",
            "generation",
            "backend.generation",
            "cluster.id",
            "namespace",
            "error.reason",
            "active.count",
            "duration.ms",
            "exclusivity.action",
            "exclusivity.key.name",
            "exclusivity.owner.instance.id",
            "auth.decision",
            "auth.reason",
            "auth.caller.role",
            "auth.required.role",
            "grpc.service",
        ] {
            assert!(LIFECYCLE_FIELDS.contains(&field));
        }
    }

    // The filtered stderr sink must drop every metric sample and the
    // per-request route-cache lookup event (both fire per forwarded request)
    // while letting other lifecycle and error events through.
    #[test]
    fn filtered_stderr_sink_suppresses_metrics_and_per_request_events_only() {
        assert!(suppressed_on_stderr(&ObservabilityEvent::Metric(
            MetricObservation::new(RUNTIME_ACTIVE_STREAMS, vec![], 1.0)
        )));
        assert!(suppressed_on_stderr(&ObservabilityEvent::Log(
            LifecycleLogEvent::new(EVENT_ROUTE_CACHE_LOOKUP, vec![])
        )));
        assert!(!suppressed_on_stderr(&ObservabilityEvent::Log(
            LifecycleLogEvent::new(EVENT_DRAIN_STARTED, vec![])
        )));
        assert!(!suppressed_on_stderr(&ObservabilityEvent::Log(
            LifecycleLogEvent::new(EVENT_WAKE, vec![])
        )));
    }

    #[test]
    fn in_memory_recorder_captures_lifecycle_log_fields() {
        let sink = InMemoryObservability::default();
        let recorder = sink.recorder();

        recorder.record_log(LifecycleLogEvent::new(
            "runtime.test",
            vec![LogField::instance_id("instance-a")],
        ));

        let events = sink.events();
        let ObservabilityEvent::Log(event) = &events[0] else {
            panic!("expected log event");
        };
        assert_eq!(event.field_value(FIELD_INSTANCE_ID), Some("instance-a"));
    }
    #[test]
    fn lazy_observations_skip_noop_construction_and_reach_enabled_sinks() {
        super::ObservabilityRecorder::noop()
            .record_lazy(|| panic!("noop must not construct events"));
        let sink = InMemoryObservability::default();
        let event = ObservabilityEvent::Log(LifecycleLogEvent::new("runtime.test", vec![]));
        sink.recorder().record_lazy(|| event.clone());
        assert_eq!(sink.events(), vec![event]);
    }
}
