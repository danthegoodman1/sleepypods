use super::{Operation, Outcome, Protocol, ProxyState, TlsClientHelloOutcome, TrafficDirection};

/// Metric label keys approved for proxy-core hot paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LabelKey {
    Protocol,
    Direction,
    Operation,
    Outcome,
    State,
}

/// Backend-neutral metric type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// Static metric metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricDescriptor {
    name: &'static str,
    kind: MetricKind,
    unit: Option<&'static str>,
    description: &'static str,
    label_keys: &'static [LabelKey],
}

/// A metric label value built from a bounded enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MetricLabel {
    key: LabelKey,
    value: &'static str,
}

pub const PROXY_ACTIVE_STREAMS_NAME: &str = "sleepypods_proxy_active_streams";
pub const PROXY_ADMISSION_DECISIONS_TOTAL_NAME: &str = "sleepypods_proxy_admission_decisions_total";
pub const PROXY_DRAIN_EVENTS_TOTAL_NAME: &str = "sleepypods_proxy_drain_events_total";
pub const PROXY_FORWARDED_BYTES_TOTAL_NAME: &str = "sleepypods_proxy_forwarded_bytes_total";
pub const PROXY_FORWARDED_MESSAGES_TOTAL_NAME: &str = "sleepypods_proxy_forwarded_messages_total";
pub const PROXY_OPERATION_DURATION_SECONDS_NAME: &str =
    "sleepypods_proxy_operation_duration_seconds";
pub const PROXY_TLS_CLIENT_HELLO_TOTAL_NAME: &str = "sleepypods_proxy_tls_client_hello_total";
pub const RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME: &str =
    "sleepypods_runtime_control_plane_calls_total";
pub const RUNTIME_ACTIVE_STREAMS_NAME: &str = "sleepypods_runtime_active_streams";
pub const RUNTIME_DRAIN_DURATION_SECONDS_NAME: &str = "sleepypods_runtime_drain_duration_seconds";
pub const RUNTIME_HTTP01_RESULTS_TOTAL_NAME: &str = "sleepypods_runtime_http01_results_total";
pub const RUNTIME_MATERIALIZATION_FAILURES_TOTAL_NAME: &str =
    "sleepypods_runtime_materialization_failures_total";
pub const RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME: &str =
    "sleepypods_runtime_route_cache_lookups_total";
pub const RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL_NAME: &str =
    "sleepypods_runtime_subscribe_stream_events_total";
pub const RUNTIME_WAKE_LATENCY_SECONDS_NAME: &str = "sleepypods_runtime_wake_latency_seconds";
pub const RECONCILER_RUNS_TOTAL_NAME: &str = "sleepypods_reconciler_runs_total";
pub const RECONCILER_RUN_DURATION_SECONDS_NAME: &str = "sleepypods_reconciler_run_duration_seconds";
pub const RECONCILER_CANDIDATES_TOTAL_NAME: &str = "sleepypods_reconciler_candidates_total";
pub const RECONCILER_CLAIMS_TOTAL_NAME: &str = "sleepypods_reconciler_claims_total";
pub const RECONCILER_LEASE_RENEWALS_TOTAL_NAME: &str = "sleepypods_reconciler_lease_renewals_total";
pub const MATERIALIZATIONS_NONTERMINAL_NAME: &str = "sleepypods_materializations_nonterminal";
pub const MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS_NAME: &str =
    "sleepypods_materialization_oldest_nonterminal_age_seconds";
pub const EXCLUSIVITY_KEYS_HELD_NAME: &str = "sleepypods_exclusivity_keys_held";
pub const KUBERNETES_OPERATIONS_TOTAL_NAME: &str = "sleepypods_kubernetes_operations_total";
pub const KUBERNETES_OPERATION_DURATION_SECONDS_NAME: &str =
    "sleepypods_kubernetes_operation_duration_seconds";

const PROTOCOL_LABELS: &[LabelKey] = &[LabelKey::Protocol];
const PROTOCOL_OUTCOME_LABELS: &[LabelKey] = &[LabelKey::Protocol, LabelKey::Outcome];
const PROTOCOL_DIRECTION_LABELS: &[LabelKey] = &[LabelKey::Protocol, LabelKey::Direction];
const PROTOCOL_OPERATION_OUTCOME_LABELS: &[LabelKey] =
    &[LabelKey::Protocol, LabelKey::Operation, LabelKey::Outcome];
const STATE_OUTCOME_LABELS: &[LabelKey] = &[LabelKey::State, LabelKey::Outcome];
const OPERATION_OUTCOME_LABELS: &[LabelKey] = &[LabelKey::Operation, LabelKey::Outcome];
const OUTCOME_LABELS: &[LabelKey] = &[LabelKey::Outcome];
const NO_LABELS: &[LabelKey] = &[];

pub const PROXY_ACTIVE_STREAMS: MetricDescriptor = MetricDescriptor::new(
    PROXY_ACTIVE_STREAMS_NAME,
    MetricKind::Gauge,
    Some("streams"),
    "Active proxy streams, requests, or upgraded sessions currently holding work permits.",
    PROTOCOL_LABELS,
);

pub const PROXY_ADMISSION_DECISIONS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_ADMISSION_DECISIONS_TOTAL_NAME,
    MetricKind::Counter,
    Some("decisions"),
    "Admission limiter decisions for new proxy work.",
    PROTOCOL_OUTCOME_LABELS,
);

pub const PROXY_DRAIN_EVENTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_DRAIN_EVENTS_TOTAL_NAME,
    MetricKind::Counter,
    Some("events"),
    "Drain lifecycle events such as start, idle completion, rejection, and grace timeout.",
    STATE_OUTCOME_LABELS,
);

pub const PROXY_FORWARDED_BYTES_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_FORWARDED_BYTES_TOTAL_NAME,
    MetricKind::Counter,
    Some("bytes"),
    "Bytes forwarded by stream-oriented proxy primitives.",
    PROTOCOL_DIRECTION_LABELS,
);

pub const PROXY_FORWARDED_MESSAGES_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_FORWARDED_MESSAGES_TOTAL_NAME,
    MetricKind::Counter,
    Some("messages"),
    "Messages forwarded by message-oriented proxy primitives.",
    PROTOCOL_DIRECTION_LABELS,
);

pub const PROXY_OPERATION_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    PROXY_OPERATION_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    Some("seconds"),
    "Duration of bounded proxy primitive operations.",
    PROTOCOL_OPERATION_OUTCOME_LABELS,
);

pub const PROXY_TLS_CLIENT_HELLO_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_TLS_CLIENT_HELLO_TOTAL_NAME,
    MetricKind::Counter,
    Some("client_hellos"),
    "TLS ClientHello parser outcomes.",
    PROTOCOL_OUTCOME_LABELS,
);

pub const RUNTIME_CONTROL_PLANE_CALLS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME,
    MetricKind::Counter,
    Some("calls"),
    "Control-plane calls made by runtime components.",
    OPERATION_OUTCOME_LABELS,
);

pub const RUNTIME_ACTIVE_STREAMS: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_ACTIVE_STREAMS_NAME,
    MetricKind::Gauge,
    Some("streams"),
    "Active runtime work holding drain permits, including initial public HTTP setup and streams.",
    NO_LABELS,
);

pub const RUNTIME_DRAIN_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_DRAIN_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    Some("seconds"),
    "Runtime drain duration from drain start to idle completion or timeout.",
    OUTCOME_LABELS,
);

pub const RUNTIME_HTTP01_RESULTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_HTTP01_RESULTS_TOTAL_NAME,
    MetricKind::Counter,
    Some("results"),
    "HTTP-01 challenge resolution outcomes observed by runtime components.",
    OUTCOME_LABELS,
);

pub const RUNTIME_MATERIALIZATION_FAILURES_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_MATERIALIZATION_FAILURES_TOTAL_NAME,
    MetricKind::Counter,
    Some("failures"),
    "Materialization failures grouped by bounded operation and outcome.",
    OPERATION_OUTCOME_LABELS,
);

pub const RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME,
    MetricKind::Counter,
    Some("lookups"),
    "Frontline route-cache lookup outcomes.",
    OUTCOME_LABELS,
);

pub const RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL_NAME,
    MetricKind::Counter,
    Some("events"),
    "Proxy Subscribe stream lifecycle events and invalidations.",
    OUTCOME_LABELS,
);

pub const RUNTIME_WAKE_LATENCY_SECONDS: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_WAKE_LATENCY_SECONDS_NAME,
    MetricKind::Histogram,
    Some("seconds"),
    "Control-plane wake latency grouped by bounded outcome.",
    OUTCOME_LABELS,
);

pub const RECONCILER_RUNS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_RUNS_TOTAL_NAME,
    MetricKind::Counter,
    Some("runs"),
    "Materialization reconciler loop runs grouped by bounded outcome.",
    OUTCOME_LABELS,
);

pub const RECONCILER_RUN_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_RUN_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    Some("seconds"),
    "Duration of one materialization reconciler pass.",
    OUTCOME_LABELS,
);

pub const RECONCILER_CANDIDATES_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_CANDIDATES_TOTAL_NAME,
    MetricKind::Counter,
    Some("candidates"),
    "Materialization rows selected as reconciliation candidates.",
    &[LabelKey::State],
);

pub const RECONCILER_CLAIMS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_CLAIMS_TOTAL_NAME,
    MetricKind::Counter,
    Some("claims"),
    "Materialization reconciliation lease claim outcomes by candidate state.",
    STATE_OUTCOME_LABELS,
);

pub const RECONCILER_LEASE_RENEWALS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_LEASE_RENEWALS_TOTAL_NAME,
    MetricKind::Counter,
    Some("renewals"),
    "Materialization reconciliation lease renewal outcomes.",
    OUTCOME_LABELS,
);

pub const MATERIALIZATIONS_NONTERMINAL: MetricDescriptor = MetricDescriptor::new(
    MATERIALIZATIONS_NONTERMINAL_NAME,
    MetricKind::Gauge,
    Some("materializations"),
    "Current non-terminal materializations grouped by bounded state.",
    &[LabelKey::State],
);

pub const MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS: MetricDescriptor = MetricDescriptor::new(
    MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS_NAME,
    MetricKind::Gauge,
    Some("seconds"),
    "Age of the oldest non-terminal materialization grouped by bounded state.",
    &[LabelKey::State],
);

pub const EXCLUSIVITY_KEYS_HELD: MetricDescriptor = MetricDescriptor::new(
    EXCLUSIVITY_KEYS_HELD_NAME,
    MetricKind::Gauge,
    Some("keys"),
    "Rendered exclusivity keys held by non-terminal materializations.",
    &[LabelKey::State],
);

pub const KUBERNETES_OPERATIONS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    KUBERNETES_OPERATIONS_TOTAL_NAME,
    MetricKind::Counter,
    Some("operations"),
    "Controller Kubernetes apply, delete, and readiness operation outcomes.",
    OPERATION_OUTCOME_LABELS,
);

pub const KUBERNETES_OPERATION_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    KUBERNETES_OPERATION_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    Some("seconds"),
    "Controller Kubernetes apply, delete, and readiness operation duration.",
    OPERATION_OUTCOME_LABELS,
);

pub const MATERIALIZATION_EFFECTS_UNCERTAIN: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_materialization_effects_uncertain",
    MetricKind::Gauge,
    Some("effects"),
    "Unresolved Kubernetes effects retaining ownership.",
    &[],
);
pub const MATERIALIZATION_FAILURES_BLOCKED: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_materialization_failures_blocked",
    MetricKind::Gauge,
    Some("materializations"),
    "Permanent or expired operations retaining inventory.",
    &[],
);

pub const RUNTIME_CERTIFICATE_ENTRIES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_entries",
    MetricKind::Gauge,
    Some("entries"),
    "Hostnames with a cached certificate.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_NEGATIVE_ENTRIES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_negative_entries",
    MetricKind::Gauge,
    Some("entries"),
    "Hostnames cached without a certificate: missing, pending or failed lookups.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_ACCOUNTED_BYTES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_accounted_bytes",
    MetricKind::Gauge,
    Some("bytes"),
    "Chain and key bytes held by cached certificates; not process RSS.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_FETCHES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_fetches",
    MetricKind::Gauge,
    Some("operations"),
    "Certificate fetch permits held by queued, running or validating work.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_QUEUE: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_queue",
    MetricKind::Gauge,
    Some("operations"),
    "Queued certificate fetch messages owned by the supervisor channel.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_TASKS: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_tasks",
    MetricKind::Gauge,
    Some("tasks"),
    "Certificate supervisor JoinSet records, including completed unreaped tasks.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_TASK_HIGH_WATER: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_task_high_water",
    MetricKind::Gauge,
    Some("tasks"),
    "Maximum retained certificate supervisor JoinSet records since startup.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_WATCHES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_watches",
    MetricKind::Gauge,
    Some("streams"),
    "Certificate watch sessions in setup, streaming or teardown.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_EXPIRY_RISK: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_expiry_risk",
    MetricKind::Gauge,
    Some("entries"),
    "Retained certificate views whose authorization lease or chain expires within sixty seconds.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_EVENTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_events_total",
    MetricKind::Counter,
    Some("events"),
    "Certificate fetch, refresh, watch, reset and install outcomes without identity labels.",
    OPERATION_OUTCOME_LABELS,
);

pub const ALL_METRICS: &[MetricDescriptor] = &[
    PROXY_ACTIVE_STREAMS,
    PROXY_ADMISSION_DECISIONS_TOTAL,
    PROXY_DRAIN_EVENTS_TOTAL,
    PROXY_FORWARDED_BYTES_TOTAL,
    PROXY_FORWARDED_MESSAGES_TOTAL,
    PROXY_OPERATION_DURATION_SECONDS,
    PROXY_TLS_CLIENT_HELLO_TOTAL,
    RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
    RUNTIME_ACTIVE_STREAMS,
    RUNTIME_DRAIN_DURATION_SECONDS,
    RUNTIME_HTTP01_RESULTS_TOTAL,
    RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
    RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL,
    RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL,
    RUNTIME_WAKE_LATENCY_SECONDS,
    RECONCILER_RUNS_TOTAL,
    RECONCILER_RUN_DURATION_SECONDS,
    RECONCILER_CANDIDATES_TOTAL,
    RECONCILER_CLAIMS_TOTAL,
    RECONCILER_LEASE_RENEWALS_TOTAL,
    MATERIALIZATIONS_NONTERMINAL,
    MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
    EXCLUSIVITY_KEYS_HELD,
    KUBERNETES_OPERATIONS_TOTAL,
    KUBERNETES_OPERATION_DURATION_SECONDS,
    MATERIALIZATION_EFFECTS_UNCERTAIN,
    MATERIALIZATION_FAILURES_BLOCKED,
    RUNTIME_CERTIFICATE_ENTRIES,
    RUNTIME_CERTIFICATE_NEGATIVE_ENTRIES,
    RUNTIME_CERTIFICATE_ACCOUNTED_BYTES,
    RUNTIME_CERTIFICATE_FETCHES,
    RUNTIME_CERTIFICATE_QUEUE,
    RUNTIME_CERTIFICATE_TASKS,
    RUNTIME_CERTIFICATE_TASK_HIGH_WATER,
    RUNTIME_CERTIFICATE_WATCHES,
    RUNTIME_CERTIFICATE_EXPIRY_RISK,
    RUNTIME_CERTIFICATE_EVENTS_TOTAL,
];

impl LabelKey {
    pub const ALL_LOW_CARDINALITY: &'static [Self] = &[
        Self::Protocol,
        Self::Direction,
        Self::Operation,
        Self::Outcome,
        Self::State,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Protocol => "protocol",
            Self::Direction => "direction",
            Self::Operation => "operation",
            Self::Outcome => "outcome",
            Self::State => "state",
        }
    }
}

impl MetricKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

impl MetricDescriptor {
    const fn new(
        name: &'static str,
        kind: MetricKind,
        unit: Option<&'static str>,
        description: &'static str,
        label_keys: &'static [LabelKey],
    ) -> Self {
        Self {
            name,
            kind,
            unit,
            description,
            label_keys,
        }
    }

    pub const fn name(self) -> &'static str {
        self.name
    }

    pub const fn kind(self) -> MetricKind {
        self.kind
    }

    pub const fn unit(self) -> Option<&'static str> {
        self.unit
    }

    pub const fn description(self) -> &'static str {
        self.description
    }

    pub const fn label_keys(self) -> &'static [LabelKey] {
        self.label_keys
    }
}

impl MetricLabel {
    const fn new(key: LabelKey, value: &'static str) -> Self {
        Self { key, value }
    }

    pub const fn key(self) -> LabelKey {
        self.key
    }

    pub const fn value(self) -> &'static str {
        self.value
    }

    pub const fn state(value: &'static str) -> Self {
        Self::new(LabelKey::State, value)
    }
}

impl Protocol {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Protocol, self.as_str())
    }
}

impl TrafficDirection {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Direction, self.as_str())
    }
}

impl Operation {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Operation, self.as_str())
    }
}

impl Outcome {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Outcome, self.as_str())
    }
}

impl ProxyState {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::State, self.as_str())
    }
}

impl TlsClientHelloOutcome {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Outcome, self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        LabelKey, MetricKind, Operation, Outcome, Protocol, ProxyState, TlsClientHelloOutcome,
        TrafficDirection, ALL_METRICS,
    };

    #[test]
    fn metric_descriptors_are_stable() {
        let expected: &[(&str, MetricKind, Option<&str>, &[LabelKey])] = &[
            (
                "sleepypods_proxy_active_streams",
                MetricKind::Gauge,
                Some("streams"),
                &[LabelKey::Protocol],
            ),
            (
                "sleepypods_proxy_admission_decisions_total",
                MetricKind::Counter,
                Some("decisions"),
                &[LabelKey::Protocol, LabelKey::Outcome],
            ),
            (
                "sleepypods_proxy_drain_events_total",
                MetricKind::Counter,
                Some("events"),
                &[LabelKey::State, LabelKey::Outcome],
            ),
            (
                "sleepypods_proxy_forwarded_bytes_total",
                MetricKind::Counter,
                Some("bytes"),
                &[LabelKey::Protocol, LabelKey::Direction],
            ),
            (
                "sleepypods_proxy_forwarded_messages_total",
                MetricKind::Counter,
                Some("messages"),
                &[LabelKey::Protocol, LabelKey::Direction],
            ),
            (
                "sleepypods_proxy_operation_duration_seconds",
                MetricKind::Histogram,
                Some("seconds"),
                &[LabelKey::Protocol, LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_proxy_tls_client_hello_total",
                MetricKind::Counter,
                Some("client_hellos"),
                &[LabelKey::Protocol, LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_control_plane_calls_total",
                MetricKind::Counter,
                Some("calls"),
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_active_streams",
                MetricKind::Gauge,
                Some("streams"),
                &[],
            ),
            (
                "sleepypods_runtime_drain_duration_seconds",
                MetricKind::Histogram,
                Some("seconds"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_http01_results_total",
                MetricKind::Counter,
                Some("results"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_materialization_failures_total",
                MetricKind::Counter,
                Some("failures"),
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_route_cache_lookups_total",
                MetricKind::Counter,
                Some("lookups"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_subscribe_stream_events_total",
                MetricKind::Counter,
                Some("events"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_wake_latency_seconds",
                MetricKind::Histogram,
                Some("seconds"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_runs_total",
                MetricKind::Counter,
                Some("runs"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_run_duration_seconds",
                MetricKind::Histogram,
                Some("seconds"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_candidates_total",
                MetricKind::Counter,
                Some("candidates"),
                &[LabelKey::State],
            ),
            (
                "sleepypods_reconciler_claims_total",
                MetricKind::Counter,
                Some("claims"),
                &[LabelKey::State, LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_lease_renewals_total",
                MetricKind::Counter,
                Some("renewals"),
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_materializations_nonterminal",
                MetricKind::Gauge,
                Some("materializations"),
                &[LabelKey::State],
            ),
            (
                "sleepypods_materialization_oldest_nonterminal_age_seconds",
                MetricKind::Gauge,
                Some("seconds"),
                &[LabelKey::State],
            ),
            (
                "sleepypods_exclusivity_keys_held",
                MetricKind::Gauge,
                Some("keys"),
                &[LabelKey::State],
            ),
            (
                "sleepypods_kubernetes_operations_total",
                MetricKind::Counter,
                Some("operations"),
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_kubernetes_operation_duration_seconds",
                MetricKind::Histogram,
                Some("seconds"),
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_materialization_effects_uncertain",
                MetricKind::Gauge,
                Some("effects"),
                &[],
            ),
            (
                "sleepypods_materialization_failures_blocked",
                MetricKind::Gauge,
                Some("materializations"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_entries",
                MetricKind::Gauge,
                Some("entries"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_negative_entries",
                MetricKind::Gauge,
                Some("entries"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_accounted_bytes",
                MetricKind::Gauge,
                Some("bytes"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_fetches",
                MetricKind::Gauge,
                Some("operations"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_queue",
                MetricKind::Gauge,
                Some("operations"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_tasks",
                MetricKind::Gauge,
                Some("tasks"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_task_high_water",
                MetricKind::Gauge,
                Some("tasks"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_watches",
                MetricKind::Gauge,
                Some("streams"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_expiry_risk",
                MetricKind::Gauge,
                Some("entries"),
                &[],
            ),
            (
                "sleepypods_runtime_certificate_events_total",
                MetricKind::Counter,
                Some("events"),
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
        ];

        assert_eq!(ALL_METRICS.len(), expected.len());
        for (descriptor, (name, kind, unit, labels)) in ALL_METRICS.iter().zip(expected) {
            assert_eq!(descriptor.name(), *name);
            assert_eq!(descriptor.kind(), *kind);
            assert_eq!(descriptor.unit(), *unit);
            assert_eq!(descriptor.label_keys(), *labels);
            assert!(!descriptor.description().is_empty());
        }
    }

    #[test]
    fn metric_names_are_unique() {
        let names = ALL_METRICS
            .iter()
            .map(|descriptor| descriptor.name())
            .collect::<HashSet<_>>();

        assert_eq!(names.len(), ALL_METRICS.len());
    }

    #[test]
    fn metric_descriptors_only_use_approved_low_cardinality_labels() {
        for descriptor in ALL_METRICS {
            for label_key in descriptor.label_keys() {
                assert!(
                    LabelKey::ALL_LOW_CARDINALITY.contains(label_key),
                    "{} uses unapproved metric label {}",
                    descriptor.name(),
                    label_key.as_str()
                );
            }
        }
    }

    #[test]
    fn metric_labels_do_not_include_request_identity_keys() {
        let forbidden = [
            "host",
            "hostname",
            "route",
            "route_id",
            "instance",
            "instance_id",
            "pod",
            "namespace",
            "object",
            "finalizer",
            "volume_handle",
            "path",
            "request",
            "raw_error",
        ];

        for descriptor in ALL_METRICS {
            for label_key in descriptor.label_keys() {
                for forbidden_fragment in forbidden {
                    assert!(
                        !label_key.as_str().contains(forbidden_fragment),
                        "{} uses high-cardinality metric label {}",
                        descriptor.name(),
                        label_key.as_str()
                    );
                }
            }
        }
    }

    #[test]
    fn typed_metric_labels_use_expected_keys() {
        assert_eq!(Protocol::Http.metric_label().key(), LabelKey::Protocol);
        assert_eq!(
            TrafficDirection::ClientToUpstream.metric_label().key(),
            LabelKey::Direction
        );
        assert_eq!(Operation::Forward.metric_label().key(), LabelKey::Operation);
        assert_eq!(Outcome::Success.metric_label().key(), LabelKey::Outcome);
        assert_eq!(ProxyState::Draining.metric_label().key(), LabelKey::State);
        assert_eq!(
            TlsClientHelloOutcome::Malformed.metric_label().key(),
            LabelKey::Outcome
        );
    }
}
