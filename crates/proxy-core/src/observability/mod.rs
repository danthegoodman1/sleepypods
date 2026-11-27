//! Proxy-specific observability adapters. Shared definitions have one owner.
pub use sleepypods_observability::{metrics, recorder, Operation, Outcome};
pub mod prometheus;

use crate::drain::DrainError;

impl From<&DrainError> for Outcome {
    fn from(error: &DrainError) -> Self {
        match error {
            DrainError::Draining => Self::Rejected,
            DrainError::GraceTimeout { .. } => Self::Timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{DrainError, Outcome};

    #[test]
    fn drain_errors_map_to_bounded_outcomes() {
        assert_eq!(Outcome::from(&DrainError::Draining), Outcome::Rejected);
        assert_eq!(
            Outcome::from(&DrainError::GraceTimeout {
                timeout: Duration::from_secs(999),
                active: 42
            }),
            Outcome::Timeout
        );
    }
}
