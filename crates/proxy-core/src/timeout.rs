use std::{error::Error, fmt, future::Future, time::Duration};

use tokio::time;

/// Error returned when an operation exceeds its configured timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeoutError {
    timeout: Duration,
}

impl TimeoutError {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    #[cfg(test)]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

pub async fn with_timeout<F, T>(timeout: Duration, future: F) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    time::timeout(timeout, future)
        .await
        .map_err(|_| TimeoutError::new(timeout))
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operation timed out after {:?}", self.timeout)
    }
}

impl Error for TimeoutError {}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tokio::time;

    use super::{with_timeout, TimeoutError};

    #[tokio::test(start_paused = true)]
    async fn returns_success_before_timeout() {
        let result = with_timeout(Duration::from_secs(5), async { "ok" }).await;

        assert_eq!(result, Ok("ok"));
    }

    #[tokio::test(start_paused = true)]
    async fn returns_typed_error_after_elapsed_timeout() {
        let completed_after_deadline = Arc::new(AtomicBool::new(false));
        let completed = Arc::clone(&completed_after_deadline);

        let task = tokio::spawn(async move {
            with_timeout(Duration::from_secs(5), async move {
                time::sleep(Duration::from_secs(10)).await;
                completed.store(true, Ordering::Release);
            })
            .await
        });

        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(5)).await;

        assert_eq!(
            task.await.expect("timeout task completed"),
            Err(TimeoutError::new(Duration::from_secs(5)))
        );
        assert!(!completed_after_deadline.load(Ordering::Acquire));
    }
}
