//! Proxy-specific observability adapters. Shared definitions have one owner.
pub use sleepypods_observability::{
    metrics, recorder, trace, Operation, Outcome, Protocol, ProxyState, TlsClientHelloOutcome,
    TrafficDirection,
};
pub mod prometheus;

use crate::{
    admission::AdmissionError,
    drain::DrainError,
    timeout::TimeoutError,
    tls::{TlsClientHelloError, TlsClientHelloSni},
};

impl From<&AdmissionError> for Outcome {
    fn from(_error: &AdmissionError) -> Self {
        Self::Rejected
    }
}

impl From<&DrainError> for Outcome {
    fn from(error: &DrainError) -> Self {
        match error {
            DrainError::Draining => Self::Rejected,
            DrainError::GraceTimeout { .. } => Self::Timeout,
        }
    }
}

impl From<&TimeoutError> for Outcome {
    fn from(_error: &TimeoutError) -> Self {
        Self::Timeout
    }
}

impl From<&TlsClientHelloSni> for TlsClientHelloOutcome {
    fn from(sni: &TlsClientHelloSni) -> Self {
        match sni {
            TlsClientHelloSni::Sni { .. } => Self::Sni,
            TlsClientHelloSni::NoSni => Self::NoSni,
            TlsClientHelloSni::Incomplete { .. } => Self::Incomplete,
        }
    }
}

impl From<&TlsClientHelloError> for TlsClientHelloOutcome {
    fn from(error: &TlsClientHelloError) -> Self {
        match error {
            TlsClientHelloError::NotTls => Self::NotTls,
            TlsClientHelloError::UnsupportedVersion { .. } => Self::UnsupportedVersion,
            TlsClientHelloError::NotClientHello => Self::NotClientHello,
            TlsClientHelloError::RecordTooLarge { .. } => Self::RecordTooLarge,
            TlsClientHelloError::Malformed => Self::Malformed,
            TlsClientHelloError::InvalidHostname => Self::InvalidHostname,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        AdmissionError, DrainError, Operation, Outcome, Protocol, ProxyState, TimeoutError,
        TlsClientHelloError, TlsClientHelloOutcome, TlsClientHelloSni, TrafficDirection,
    };

    #[test]
    fn shared_label_values_are_stable() {
        assert_eq!(
            Protocol::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            ["tcp", "http", "websocket", "tls"]
        );
        assert_eq!(
            TrafficDirection::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            ["client_to_upstream", "upstream_to_client"]
        );
        assert_eq!(
            Operation::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            [
                "accept",
                "admit",
                "connect",
                "forward",
                "rewrite_request",
                "drain",
                "tls_client_hello",
                "route_cache_lookup",
                "subscribe_route",
                "unsubscribe",
                "subscribe_stream",
                "wake_instance",
                "materialize",
                "http01_resolve",
                "report_idle",
                "apply",
                "delete",
                "readiness",
                "certificate_fetch",
                "certificate_refresh",
                "certificate_watch",
                "certificate_install"
            ]
        );
        assert_eq!(
            Outcome::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            [
                "success",
                "error",
                "timeout",
                "rejected",
                "canceled",
                "hit",
                "miss",
                "started",
                "closed",
                "updated",
                "invalidated",
                "already_running",
                "already_waking",
                "already_draining"
            ]
        );
        assert_eq!(
            ProxyState::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            ["accepting", "active", "draining", "idle"]
        );
        assert_eq!(
            TlsClientHelloOutcome::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            [
                "sni",
                "no_sni",
                "incomplete",
                "not_tls",
                "unsupported_version",
                "not_client_hello",
                "record_too_large",
                "malformed",
                "invalid_hostname"
            ]
        );
    }

    #[test]
    fn existing_errors_map_to_bounded_outcomes() {
        assert_eq!(
            Outcome::from(&AdmissionError::Saturated { limit: 5000 }),
            Outcome::Rejected
        );
        assert_eq!(Outcome::from(&DrainError::Draining), Outcome::Rejected);
        assert_eq!(
            Outcome::from(&DrainError::GraceTimeout {
                timeout: Duration::from_secs(999),
                active: 42
            }),
            Outcome::Timeout
        );
        assert_eq!(
            Outcome::from(&TimeoutError::new(Duration::from_secs(123))),
            Outcome::Timeout
        );
    }

    #[test]
    fn tls_client_hello_outcomes_drop_dynamic_values() {
        assert_eq!(
            TlsClientHelloOutcome::from_result(
                &Result::<TlsClientHelloSni, TlsClientHelloError>::Ok(TlsClientHelloSni::Sni {
                    hostname: "tenant-specific.example.test".to_owned()
                })
            ),
            TlsClientHelloOutcome::Sni
        );
        assert_eq!(
            TlsClientHelloOutcome::from_result(
                &Result::<TlsClientHelloSni, TlsClientHelloError>::Ok(
                    TlsClientHelloSni::Incomplete { needed: Some(4096) }
                )
            ),
            TlsClientHelloOutcome::Incomplete
        );
        assert_eq!(
            TlsClientHelloOutcome::from_result(
                &Result::<TlsClientHelloSni, TlsClientHelloError>::Err(
                    TlsClientHelloError::UnsupportedVersion {
                        major: 3,
                        minor: 99
                    }
                )
            ),
            TlsClientHelloOutcome::UnsupportedVersion
        );
        assert_eq!(
            TlsClientHelloOutcome::from_result(
                &Result::<TlsClientHelloSni, TlsClientHelloError>::Err(
                    TlsClientHelloError::RecordTooLarge {
                        len: 1_000_000,
                        max: 16_384
                    }
                )
            ),
            TlsClientHelloOutcome::RecordTooLarge
        );
    }
}
