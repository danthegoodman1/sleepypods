use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::accounting::{ActiveConnection, ActiveConnectionCounter, IdleActivityWatch};
use crate::observability::{
    metrics::RUNTIME_DRAIN_DURATION_SECONDS,
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
        EVENT_DRAIN_COMPLETED, EVENT_DRAIN_STARTED, EVENT_DRAIN_TIMEOUT,
    },
    Outcome,
};
use crate::timeout::with_timeout;

/// Tracks active sessions and rejects new work once drain starts.
#[derive(Clone, Debug)]
pub struct DrainTracker {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    draining: AtomicBool,
    active: ActiveConnectionCounter,
    grace_timeout: Duration,
    observability: ObservabilityRecorder,
}

/// Active work admitted before drain started.
#[derive(Debug)]
#[must_use = "dropping the permit releases the active connection count"]
pub struct DrainPermit {
    active: ActiveConnection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainError {
    Draining,
    GraceTimeout { timeout: Duration, active: usize },
}

impl DrainTracker {
    pub fn new(grace_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                draining: AtomicBool::new(false),
                active: ActiveConnectionCounter::new(),
                grace_timeout,
                observability: ObservabilityRecorder::default(),
            }),
        }
    }

    pub fn with_observability(
        grace_timeout: Duration,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                draining: AtomicBool::new(false),
                active: ActiveConnectionCounter::new(),
                grace_timeout,
                observability,
            }),
        }
    }

    pub fn active_count(&self) -> usize {
        self.inner.active.active()
    }

    pub fn is_draining(&self) -> bool {
        self.inner.draining.load(Ordering::SeqCst)
    }

    /// Counts the work before the second drain check, so a drain that starts
    /// concurrently either waits for this permit or sees it refused.
    pub fn try_acquire(&self) -> Result<DrainPermit, DrainError> {
        if self.is_draining() {
            return Err(DrainError::Draining);
        }
        let active = self.inner.active.track();
        if self.is_draining() {
            return Err(DrainError::Draining);
        }

        Ok(DrainPermit { active })
    }

    pub fn start_drain(&self) {
        self.inner.draining.store(true, Ordering::SeqCst);
    }

    pub async fn wait_for_idle(&self) -> Result<(), DrainError> {
        if self.active_count() == 0 {
            return Ok(());
        }

        match with_timeout(self.inner.grace_timeout, self.inner.active.wait_for_zero()).await {
            Ok(()) => Ok(()),
            Err(_) if self.active_count() == 0 => Ok(()),
            Err(_) => Err(DrainError::GraceTimeout {
                timeout: self.inner.grace_timeout,
                active: self.active_count(),
            }),
        }
    }

    pub async fn drain(&self) -> Result<(), DrainError> {
        let started = Instant::now();
        self.start_drain();
        self.inner.observability.record_log(LifecycleLogEvent::new(
            EVENT_DRAIN_STARTED,
            vec![LogField::active_count(self.active_count())],
        ));
        let result = self.wait_for_idle().await;
        let duration = started.elapsed();
        let outcome = match &result {
            Ok(()) => Outcome::Success,
            Err(error) => Outcome::from(error),
        };
        self.inner
            .observability
            .record_metric(MetricObservation::new(
                RUNTIME_DRAIN_DURATION_SECONDS,
                vec![outcome.metric_label()],
                duration.as_secs_f64(),
            ));
        self.inner.observability.record_log(LifecycleLogEvent::new(
            match &result {
                Ok(()) => EVENT_DRAIN_COMPLETED,
                Err(DrainError::GraceTimeout { .. }) => EVENT_DRAIN_TIMEOUT,
                Err(DrainError::Draining) => EVENT_DRAIN_TIMEOUT,
            },
            vec![
                LogField::active_count(self.active_count()),
                LogField::duration_ms(duration.as_millis()),
            ],
        ));
        result
    }

    pub async fn wait_for_active_count(&self, expected: usize) {
        self.inner.active.wait_for_count(expected).await;
    }

    pub fn watch_for_activity_after_idle(&self) -> Option<IdleActivityWatch> {
        self.inner.active.watch_for_activity_after_idle()
    }
}

impl DrainPermit {
    pub fn release(&mut self) {
        if self.active.is_active() {
            self.active.release();
        }
    }
}

impl Drop for DrainPermit {
    fn drop(&mut self) {
        if self.active.is_active() {
            self.active.release();
        }
    }
}

impl fmt::Display for DrainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draining => write!(f, "drain has started; new work is rejected"),
            Self::GraceTimeout { timeout, active } => write!(
                f,
                "drain grace timeout of {:?} elapsed with {} active connection(s)",
                timeout, active
            ),
        }
    }
}

impl Error for DrainError {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::observability::{
        metrics::{RUNTIME_ACTIVE_STREAMS_NAME, RUNTIME_DRAIN_DURATION_SECONDS_NAME},
        recorder::{
            InMemoryObservability, ObservabilityEvent, EVENT_DRAIN_COMPLETED, EVENT_DRAIN_STARTED,
        },
    };

    use super::{DrainError, DrainTracker};

    #[tokio::test]
    async fn drain_rejects_new_work_and_waits_for_existing_work() {
        let tracker = DrainTracker::new(Duration::from_secs(5));
        let mut permit = tracker.try_acquire().expect("connection admitted");

        tracker.start_drain();

        assert_eq!(tracker.try_acquire().unwrap_err(), DrainError::Draining);
        assert!(tracker.is_draining());
        assert_eq!(tracker.active_count(), 1);

        let waiter = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_for_idle().await }
        });

        permit.release();

        assert_eq!(waiter.await.expect("waiter task completed"), Ok(()));
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn drain_reports_grace_timeout_with_active_work() {
        let tracker = DrainTracker::new(Duration::from_secs(5));
        let _permit = tracker.try_acquire().expect("connection admitted");

        tracker.start_drain();

        let waiter = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_for_idle().await }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;

        assert_eq!(
            waiter
                .await
                .expect("waiter task completed")
                .expect_err("drain should time out"),
            DrainError::GraceTimeout {
                timeout: Duration::from_secs(5),
                active: 1,
            }
        );
    }

    #[tokio::test]
    async fn drain_records_duration_metric_and_lifecycle_logs() {
        let sink = InMemoryObservability::default();
        let tracker = DrainTracker::with_observability(Duration::from_secs(5), sink.recorder());

        tracker.drain().await.expect("idle drain succeeds");

        let events = sink.events();
        assert!(events.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Log(log) if log.name() == EVENT_DRAIN_STARTED
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Log(log) if log.name() == EVENT_DRAIN_COMPLETED
                && log.field_value("active.count") == Some("0")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Metric(metric)
                if metric.name() == RUNTIME_DRAIN_DURATION_SECONDS_NAME
                    && metric.labels().iter().any(|label| label.value() == "success")
        )));
    }

    #[test]
    fn drain_permit_updates_active_count_without_emitting_hot_path_gauge() {
        let sink = InMemoryObservability::default();
        let tracker = DrainTracker::with_observability(Duration::from_secs(5), sink.recorder());

        let permit = tracker.try_acquire().expect("work admitted");
        assert_eq!(tracker.active_count(), 1);
        drop(permit);
        assert_eq!(tracker.active_count(), 0);

        let values = sink
            .events()
            .into_iter()
            .filter_map(|event| match event {
                ObservabilityEvent::Metric(metric)
                    if metric.name() == RUNTIME_ACTIVE_STREAMS_NAME =>
                {
                    Some(metric.value())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(values.is_empty());
    }
}
