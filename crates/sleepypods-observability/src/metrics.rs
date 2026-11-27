use super::{Operation, Outcome};

/// Metric label keys. Each takes values from a bounded set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LabelKey {
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
    description: &'static str,
    label_keys: &'static [LabelKey],
}

/// A metric label value built from a bounded enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MetricLabel {
    key: LabelKey,
    value: &'static str,
}

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
pub const RECONCILER_CLAIMS_TOTAL_NAME: &str = "sleepypods_reconciler_claims_total";
pub const RECONCILER_LEASE_RENEWALS_TOTAL_NAME: &str = "sleepypods_reconciler_lease_renewals_total";
pub const MATERIALIZATIONS_NONTERMINAL_NAME: &str = "sleepypods_materializations_nonterminal";
pub const MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS_NAME: &str =
    "sleepypods_materialization_oldest_nonterminal_age_seconds";
pub const EXCLUSIVITY_KEYS_HELD_NAME: &str = "sleepypods_exclusivity_keys_held";
pub const KUBERNETES_OPERATIONS_TOTAL_NAME: &str = "sleepypods_kubernetes_operations_total";
pub const KUBERNETES_OPERATION_DURATION_SECONDS_NAME: &str =
    "sleepypods_kubernetes_operation_duration_seconds";

const STATE_OUTCOME_LABELS: &[LabelKey] = &[LabelKey::State, LabelKey::Outcome];
const OPERATION_OUTCOME_LABELS: &[LabelKey] = &[LabelKey::Operation, LabelKey::Outcome];
const OUTCOME_LABELS: &[LabelKey] = &[LabelKey::Outcome];
const NO_LABELS: &[LabelKey] = &[];

pub const RUNTIME_CONTROL_PLANE_CALLS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME,
    MetricKind::Counter,
    "Control-plane calls made by runtime components.",
    OPERATION_OUTCOME_LABELS,
);

pub const RUNTIME_ACTIVE_STREAMS: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_ACTIVE_STREAMS_NAME,
    MetricKind::Gauge,
    "Active runtime work holding drain permits, including initial public HTTP setup and streams.",
    NO_LABELS,
);

pub const RUNTIME_DRAIN_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_DRAIN_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    "Runtime drain duration from drain start to idle completion or timeout.",
    OUTCOME_LABELS,
);

pub const RUNTIME_HTTP01_RESULTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_HTTP01_RESULTS_TOTAL_NAME,
    MetricKind::Counter,
    "HTTP-01 challenge resolution outcomes observed by runtime components.",
    OUTCOME_LABELS,
);

pub const RUNTIME_MATERIALIZATION_FAILURES_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_MATERIALIZATION_FAILURES_TOTAL_NAME,
    MetricKind::Counter,
    "Materialization failures grouped by bounded operation and outcome.",
    OPERATION_OUTCOME_LABELS,
);

pub const RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME,
    MetricKind::Counter,
    "Frontline route-cache lookup outcomes.",
    OUTCOME_LABELS,
);

pub const RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL_NAME,
    MetricKind::Counter,
    "Proxy Subscribe stream lifecycle events and invalidations.",
    OUTCOME_LABELS,
);

pub const RUNTIME_WAKE_LATENCY_SECONDS: MetricDescriptor = MetricDescriptor::new(
    RUNTIME_WAKE_LATENCY_SECONDS_NAME,
    MetricKind::Histogram,
    "Control-plane wake latency grouped by bounded outcome.",
    OUTCOME_LABELS,
);

pub const RECONCILER_RUNS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_RUNS_TOTAL_NAME,
    MetricKind::Counter,
    "Materialization reconciler loop runs grouped by bounded outcome.",
    OUTCOME_LABELS,
);

pub const RECONCILER_RUN_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_RUN_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    "Duration of one materialization reconciler pass.",
    OUTCOME_LABELS,
);

pub const RECONCILER_CLAIMS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_CLAIMS_TOTAL_NAME,
    MetricKind::Counter,
    "Materialization reconciliation lease claim outcomes by candidate state.",
    STATE_OUTCOME_LABELS,
);

pub const RECONCILER_LEASE_RENEWALS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    RECONCILER_LEASE_RENEWALS_TOTAL_NAME,
    MetricKind::Counter,
    "Materialization reconciliation lease renewal outcomes.",
    OUTCOME_LABELS,
);

pub const MATERIALIZATIONS_NONTERMINAL: MetricDescriptor = MetricDescriptor::new(
    MATERIALIZATIONS_NONTERMINAL_NAME,
    MetricKind::Gauge,
    "Current non-terminal materializations grouped by bounded state.",
    &[LabelKey::State],
);

pub const MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS: MetricDescriptor = MetricDescriptor::new(
    MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS_NAME,
    MetricKind::Gauge,
    "Age of the oldest non-terminal materialization grouped by bounded state.",
    &[LabelKey::State],
);

pub const EXCLUSIVITY_KEYS_HELD: MetricDescriptor = MetricDescriptor::new(
    EXCLUSIVITY_KEYS_HELD_NAME,
    MetricKind::Gauge,
    "Rendered exclusivity keys held by non-terminal materializations.",
    &[LabelKey::State],
);

pub const KUBERNETES_OPERATIONS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    KUBERNETES_OPERATIONS_TOTAL_NAME,
    MetricKind::Counter,
    "Controller Kubernetes apply, delete, and readiness operation outcomes.",
    OPERATION_OUTCOME_LABELS,
);

pub const KUBERNETES_OPERATION_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    KUBERNETES_OPERATION_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    "Controller Kubernetes apply, delete, and readiness operation duration.",
    OPERATION_OUTCOME_LABELS,
);

pub const MATERIALIZATION_FAILURES_BLOCKED: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_materialization_failures_blocked",
    MetricKind::Gauge,
    "Cleanups parked by an object another owner holds.",
    &[],
);
pub const CONTROL_PLANE_STORE_AVAILABLE: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_control_plane_store_available",
    MetricKind::Gauge,
    "One while the reconciler reaches the store, zero while it retries an unavailable store.",
    &[],
);

pub const CONTROL_PLANE_AUTH_DECISIONS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_control_plane_auth_decisions_total",
    MetricKind::Counter,
    "Control-plane caller authentication decisions.",
    OUTCOME_LABELS,
);

pub const RUNTIME_CERTIFICATE_ENTRIES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_entries",
    MetricKind::Gauge,
    "Hostnames with a cached certificate.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_NEGATIVE_ENTRIES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_negative_entries",
    MetricKind::Gauge,
    "Hostnames cached without a certificate: missing, pending or failed lookups.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_ACCOUNTED_BYTES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_accounted_bytes",
    MetricKind::Gauge,
    "Chain and key bytes held by cached certificates; not process RSS.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_FETCHES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_fetches",
    MetricKind::Gauge,
    "Certificate fetches running or validating.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_QUEUE: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_queue",
    MetricKind::Gauge,
    "Certificate lookups and refreshes awaiting a fetch slot.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_TASKS: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_tasks",
    MetricKind::Gauge,
    "Certificate supervisor JoinSet records, including completed unreaped tasks.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_TASK_HIGH_WATER: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_task_high_water",
    MetricKind::Gauge,
    "Maximum retained certificate supervisor JoinSet records since startup.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_WATCHES: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_watches",
    MetricKind::Gauge,
    "Certificate watch sessions in setup, streaming or teardown.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_EXPIRY_RISK: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_expiry_risk",
    MetricKind::Gauge,
    "Retained certificate views whose authorization lease or chain expires within sixty seconds.",
    NO_LABELS,
);
pub const RUNTIME_CERTIFICATE_EVENTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    "sleepypods_runtime_certificate_events_total",
    MetricKind::Counter,
    "Certificate fetch, refresh, watch, reset and install outcomes without identity labels.",
    OPERATION_OUTCOME_LABELS,
);

pub const ALL_METRICS: &[MetricDescriptor] = &[
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
    RECONCILER_CLAIMS_TOTAL,
    RECONCILER_LEASE_RENEWALS_TOTAL,
    MATERIALIZATIONS_NONTERMINAL,
    MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
    EXCLUSIVITY_KEYS_HELD,
    KUBERNETES_OPERATIONS_TOTAL,
    KUBERNETES_OPERATION_DURATION_SECONDS,
    MATERIALIZATION_FAILURES_BLOCKED,
    CONTROL_PLANE_STORE_AVAILABLE,
    CONTROL_PLANE_AUTH_DECISIONS_TOTAL,
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
    pub const fn as_str(self) -> &'static str {
        match self {
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
        description: &'static str,
        label_keys: &'static [LabelKey],
    ) -> Self {
        Self {
            name,
            kind,
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{LabelKey, MetricKind, Operation, Outcome, ALL_METRICS};

    #[test]
    fn metric_descriptors_are_stable() {
        let expected: &[(&str, MetricKind, &[LabelKey])] = &[
            (
                "sleepypods_runtime_control_plane_calls_total",
                MetricKind::Counter,
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            ("sleepypods_runtime_active_streams", MetricKind::Gauge, &[]),
            (
                "sleepypods_runtime_drain_duration_seconds",
                MetricKind::Histogram,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_http01_results_total",
                MetricKind::Counter,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_materialization_failures_total",
                MetricKind::Counter,
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_route_cache_lookups_total",
                MetricKind::Counter,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_subscribe_stream_events_total",
                MetricKind::Counter,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_wake_latency_seconds",
                MetricKind::Histogram,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_runs_total",
                MetricKind::Counter,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_run_duration_seconds",
                MetricKind::Histogram,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_claims_total",
                MetricKind::Counter,
                &[LabelKey::State, LabelKey::Outcome],
            ),
            (
                "sleepypods_reconciler_lease_renewals_total",
                MetricKind::Counter,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_materializations_nonterminal",
                MetricKind::Gauge,
                &[LabelKey::State],
            ),
            (
                "sleepypods_materialization_oldest_nonterminal_age_seconds",
                MetricKind::Gauge,
                &[LabelKey::State],
            ),
            (
                "sleepypods_exclusivity_keys_held",
                MetricKind::Gauge,
                &[LabelKey::State],
            ),
            (
                "sleepypods_kubernetes_operations_total",
                MetricKind::Counter,
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_kubernetes_operation_duration_seconds",
                MetricKind::Histogram,
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_materialization_failures_blocked",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_control_plane_store_available",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_control_plane_auth_decisions_total",
                MetricKind::Counter,
                &[LabelKey::Outcome],
            ),
            (
                "sleepypods_runtime_certificate_entries",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_negative_entries",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_accounted_bytes",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_fetches",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_queue",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_tasks",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_task_high_water",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_watches",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_expiry_risk",
                MetricKind::Gauge,
                &[],
            ),
            (
                "sleepypods_runtime_certificate_events_total",
                MetricKind::Counter,
                &[LabelKey::Operation, LabelKey::Outcome],
            ),
        ];

        assert_eq!(ALL_METRICS.len(), expected.len());
        for (descriptor, (name, kind, labels)) in ALL_METRICS.iter().zip(expected) {
            assert_eq!(descriptor.name(), *name);
            assert_eq!(descriptor.kind(), *kind);
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
        assert_eq!(Operation::Apply.metric_label().key(), LabelKey::Operation);
        assert_eq!(Outcome::Success.metric_label().key(), LabelKey::Outcome);
    }
}
