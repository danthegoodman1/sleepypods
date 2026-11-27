//! Reports a quiet sidecar to the control plane.
//!
//! The detector reports once no work has been active for the full idle
//! interval. While the pod stays quiet it reports again every retry backoff,
//! whatever the control plane answered: a wake during drain returns the
//! instance to Running on this pod, and that pod must still be able to sleep.
//! Any activity ends the current report and starts a full idle interval.
use std::{convert::Infallible, error::Error, fmt, time::Duration};

use proxy_core::{
    observability::{
        metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
        recorder::{
            LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
            EVENT_IDLE_REPORT,
        },
        Operation, Outcome,
    },
    DrainTracker,
};
use sleepypods_types::{Generation, InstanceId};
use tokio::time::timeout;

use crate::control_plane_transport::{ReportIdleClient, ReportIdleResponse};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdleReportConfig {
    idle_timeout: Duration,
    retry_backoff: Duration,
    report_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdleReportConfigError {
    ZeroIdleTimeout,
    ZeroRetryBackoff,
    ZeroReportTimeout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleObservation {
    active_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportIdleRequest {
    instance_id: InstanceId,
    generation: Generation,
    observation: IdleObservation,
}

#[derive(Clone, Debug)]
pub struct IdleDetector {
    instance_id: InstanceId,
    generation: Generation,
    config: IdleReportConfig,
    drain: DrainTracker,
    observability: ObservabilityRecorder,
}

impl IdleReportConfig {
    /// `report_timeout` bounds one `ReportIdle` call, so a control-plane
    /// connection that stops answering costs one retry.
    pub fn new(
        idle_timeout: Duration,
        retry_backoff: Duration,
        report_timeout: Duration,
    ) -> Result<Self, IdleReportConfigError> {
        if idle_timeout.is_zero() {
            return Err(IdleReportConfigError::ZeroIdleTimeout);
        }
        if retry_backoff.is_zero() {
            return Err(IdleReportConfigError::ZeroRetryBackoff);
        }
        if report_timeout.is_zero() {
            return Err(IdleReportConfigError::ZeroReportTimeout);
        }

        Ok(Self {
            idle_timeout,
            retry_backoff,
            report_timeout,
        })
    }
}

impl IdleObservation {
    pub fn zero_active() -> Self {
        Self { active_count: 0 }
    }

    pub fn active_count(&self) -> usize {
        self.active_count
    }
}

impl ReportIdleRequest {
    pub fn new(
        instance_id: InstanceId,
        generation: Generation,
        observation: IdleObservation,
    ) -> Self {
        Self {
            instance_id,
            generation,
            observation,
        }
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn observation(&self) -> &IdleObservation {
        &self.observation
    }
}

impl IdleDetector {
    pub fn with_observability(
        instance_id: InstanceId,
        generation: Generation,
        config: IdleReportConfig,
        drain: DrainTracker,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            instance_id,
            generation,
            config,
            drain,
            observability,
        }
    }

    /// Reports idleness for as long as the sidecar runs.
    pub async fn run<Client>(&self, client: &mut Client) -> Infallible
    where
        Client: ReportIdleClient,
    {
        loop {
            self.wait_until_idle_timeout_elapsed().await;

            while let Some(activity) = self.drain.watch_for_activity_after_idle() {
                let request = self.report_request();
                let activity = activity.wait_for_update();
                tokio::pin!(activity);

                let response = tokio::select! {
                    response = timeout(
                        self.config.report_timeout,
                        client.report_idle(request.clone()),
                    ) => response,
                    _ = &mut activity => break,
                };

                let mut retry_backoff = self.config.retry_backoff;
                let outcome = match &response {
                    Ok(Ok(response)) => {
                        if let ReportIdleResponse::RetryAfter { duration } = response {
                            retry_backoff = retry_backoff.max(*duration);
                        }
                        idle_report_outcome(response)
                    }
                    Ok(Err(_)) => Outcome::Error,
                    Err(_) => Outcome::Timeout,
                };
                self.record_idle_report(&request, outcome);

                if timeout(retry_backoff, &mut activity).await.is_ok() {
                    break;
                }
            }
        }
    }

    async fn wait_until_idle_timeout_elapsed(&self) {
        loop {
            self.drain.wait_for_active_count(0).await;

            let Some(activity) = self.drain.watch_for_activity_after_idle() else {
                continue;
            };
            if timeout(self.config.idle_timeout, activity.wait_for_update())
                .await
                .is_err()
            {
                return;
            }
        }
    }

    fn report_request(&self) -> ReportIdleRequest {
        ReportIdleRequest::new(
            self.instance_id.clone(),
            self.generation,
            IdleObservation {
                active_count: self.drain.active_count(),
            },
        )
    }

    fn record_idle_report(&self, request: &ReportIdleRequest, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![Operation::ReportIdle.metric_label(), outcome.metric_label()],
            1.0,
        ));
        self.observability.record_log(LifecycleLogEvent::new(
            EVENT_IDLE_REPORT,
            vec![
                LogField::instance_id(request.instance_id().as_str()),
                LogField::generation(request.generation().get()),
                LogField::active_count(request.observation().active_count()),
            ],
        ));
    }
}

fn idle_report_outcome(response: &ReportIdleResponse) -> Outcome {
    match response {
        ReportIdleResponse::Accepted { .. } => Outcome::Success,
        ReportIdleResponse::AlreadyDraining { .. } => Outcome::AlreadyDraining,
        ReportIdleResponse::RetryAfter { .. }
        | ReportIdleResponse::GenerationConflict { .. }
        | ReportIdleResponse::Unavailable { .. } => Outcome::Rejected,
    }
}

impl fmt::Display for IdleReportConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIdleTimeout => write!(f, "idle report timeout must be non-zero"),
            Self::ZeroRetryBackoff => write!(f, "idle report retry backoff must be non-zero"),
            Self::ZeroReportTimeout => write!(f, "idle report call timeout must be non-zero"),
        }
    }
}

impl Error for IdleReportConfigError {}

#[cfg(test)]
mod tests;
