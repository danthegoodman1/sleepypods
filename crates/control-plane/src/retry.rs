use std::{future::Future, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl RetryPolicy {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(max_attempts: usize, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_attempts,
            initial_backoff,
            max_backoff,
        }
    }

    pub async fn retry_if<T, E, O, Fut, P>(
        &self,
        mut operation: O,
        mut should_retry: P,
    ) -> Result<T, E>
    where
        O: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: FnMut(&E) -> bool,
    {
        let max_attempts = self.max_attempts.max(1);
        let mut attempts = 0;
        let mut backoff = self.initial_backoff;

        loop {
            attempts += 1;
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if attempts < max_attempts && should_retry(&error) => {
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    backoff = next_backoff(backoff, self.max_backoff);
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    if current.is_zero() || max.is_zero() {
        return Duration::ZERO;
    }

    current.checked_mul(2).unwrap_or(max).min(max)
}
