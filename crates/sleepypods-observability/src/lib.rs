//! Metric and lifecycle-event names, their bounded label values, the
//! process-wide recorder and the Prometheus exporter.

pub mod metrics;
pub mod prometheus;
pub mod recorder;

/// Operation label values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    SubscribeRoute,
    Unsubscribe,
    SubscribeStream,
    WakeInstance,
    Materialize,
    ReportIdle,
    Apply,
    Delete,
    Readiness,
    CertificateFetch,
    CertificateRefresh,
    CertificateWatch,
    CertificateInstall,
}

/// Generic low-cardinality operation outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    Success,
    Error,
    Timeout,
    Rejected,
    Canceled,
    Hit,
    Miss,
    Started,
    Closed,
    Updated,
    Invalidated,
    AlreadyRunning,
    AlreadyWaking,
    AlreadyDraining,
}

/// How one connection attempt to a Pod address ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PodRouteOutcome {
    /// The sidecar confirmed the instance; the request uses the Pod.
    Pod,
    /// The Pod address refused the TCP connection.
    Refused,
    /// The connection and claim outlasted the attempt budget.
    Timeout,
    /// The sidecar closed the connection after the claim or echoed another
    /// one: it serves another instance.
    ClaimRejected,
    /// Another connect or claim error, such as an unreachable network.
    Error,
    /// The route failed before at this backend generation, so frontline went
    /// straight to the Service.
    KnownFailed,
}

impl PodRouteOutcome {
    pub const ALL: [Self; 6] = [
        Self::Pod,
        Self::Refused,
        Self::Timeout,
        Self::ClaimRejected,
        Self::Error,
        Self::KnownFailed,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pod => "pod",
            Self::Refused => "refused",
            Self::Timeout => "timeout",
            Self::ClaimRejected => "claim_rejected",
            Self::Error => "error",
            Self::KnownFailed => "known_failed",
        }
    }
}

impl Operation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubscribeRoute => "subscribe_route",
            Self::Unsubscribe => "unsubscribe",
            Self::SubscribeStream => "subscribe_stream",
            Self::WakeInstance => "wake_instance",
            Self::Materialize => "materialize",
            Self::ReportIdle => "report_idle",
            Self::Apply => "apply",
            Self::Delete => "delete",
            Self::Readiness => "readiness",
            Self::CertificateFetch => "certificate_fetch",
            Self::CertificateRefresh => "certificate_refresh",
            Self::CertificateWatch => "certificate_watch",
            Self::CertificateInstall => "certificate_install",
        }
    }
}

impl Outcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Rejected => "rejected",
            Self::Canceled => "canceled",
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Started => "started",
            Self::Closed => "closed",
            Self::Updated => "updated",
            Self::Invalidated => "invalidated",
            Self::AlreadyRunning => "already_running",
            Self::AlreadyWaking => "already_waking",
            Self::AlreadyDraining => "already_draining",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Operation, Outcome, PodRouteOutcome};

    #[test]
    fn label_values_are_stable() {
        for (operation, value) in [
            (Operation::SubscribeRoute, "subscribe_route"),
            (Operation::Unsubscribe, "unsubscribe"),
            (Operation::SubscribeStream, "subscribe_stream"),
            (Operation::WakeInstance, "wake_instance"),
            (Operation::Materialize, "materialize"),
            (Operation::ReportIdle, "report_idle"),
            (Operation::Apply, "apply"),
            (Operation::Delete, "delete"),
            (Operation::Readiness, "readiness"),
            (Operation::CertificateFetch, "certificate_fetch"),
            (Operation::CertificateRefresh, "certificate_refresh"),
            (Operation::CertificateWatch, "certificate_watch"),
            (Operation::CertificateInstall, "certificate_install"),
        ] {
            assert_eq!(operation.as_str(), value);
        }
        for (outcome, value) in [
            (Outcome::Success, "success"),
            (Outcome::Error, "error"),
            (Outcome::Timeout, "timeout"),
            (Outcome::Rejected, "rejected"),
            (Outcome::Canceled, "canceled"),
            (Outcome::Hit, "hit"),
            (Outcome::Miss, "miss"),
            (Outcome::Started, "started"),
            (Outcome::Closed, "closed"),
            (Outcome::Updated, "updated"),
            (Outcome::Invalidated, "invalidated"),
            (Outcome::AlreadyRunning, "already_running"),
            (Outcome::AlreadyWaking, "already_waking"),
            (Outcome::AlreadyDraining, "already_draining"),
        ] {
            assert_eq!(outcome.as_str(), value);
        }
        assert_eq!(
            PodRouteOutcome::ALL.map(PodRouteOutcome::as_str),
            [
                "pod",
                "refused",
                "timeout",
                "claim_rejected",
                "error",
                "known_failed"
            ]
        );
        for (index, outcome) in PodRouteOutcome::ALL.into_iter().enumerate() {
            assert_eq!(outcome as usize, index);
        }
    }
}
