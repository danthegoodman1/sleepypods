use std::{error::Error, fmt, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Bounded admission control for in-flight work. Acquisition never waits.
#[derive(Clone, Debug)]
pub struct AdmissionLimiter {
    semaphore: Arc<Semaphore>,
    limit: usize,
}

/// Capacity admitted by an [`AdmissionLimiter`]. Dropping it returns one unit.
#[derive(Debug)]
#[must_use = "dropping the permit releases admission capacity"]
pub struct AdmissionPermit {
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionError {
    Saturated { limit: usize },
}

impl AdmissionLimiter {
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_in_flight)),
            limit: max_in_flight,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn in_flight(&self) -> usize {
        self.limit - self.semaphore.available_permits()
    }

    pub fn try_acquire(&self) -> Result<AdmissionPermit, AdmissionError> {
        self.semaphore
            .clone()
            .try_acquire_owned()
            .map(|permit| AdmissionPermit { _permit: permit })
            .map_err(|_| AdmissionError::Saturated { limit: self.limit })
    }
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Saturated { limit } => write!(
                f,
                "admission limit of {limit} in-flight operation(s) is saturated"
            ),
        }
    }
}

impl Error for AdmissionError {}

#[cfg(test)]
mod tests {
    use super::{AdmissionError, AdmissionLimiter};

    #[test]
    fn try_acquire_enforces_limit_and_releases_on_drop() {
        let limiter = AdmissionLimiter::new(1);
        let permit = limiter.try_acquire().expect("first operation admitted");

        assert_eq!(limiter.in_flight(), 1);
        assert_eq!(
            limiter.try_acquire().expect_err("limit is saturated"),
            AdmissionError::Saturated { limit: 1 }
        );

        drop(permit);

        assert_eq!(limiter.in_flight(), 0);
        let _permit = limiter
            .try_acquire()
            .expect("capacity is returned after drop");
    }
}
