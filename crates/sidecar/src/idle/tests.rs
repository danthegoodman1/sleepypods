use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use proxy_core::{
    observability::{
        metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME,
        recorder::{
            InMemoryObservability, ObservabilityEvent, ObservabilityRecorder, EVENT_IDLE_REPORT,
            FIELD_ACTIVE_COUNT, FIELD_INSTANCE_ID,
        },
    },
    DrainTracker,
};
use sleepypods_api::{Generation, InstanceId};

use super::{
    IdleDetector, IdleObservation, IdleReportConfig, IdleReportConfigError, ReportIdleRequest,
};
use crate::control_plane_transport::{
    ReportIdleClient, ReportIdleFuture, ReportIdleResponse, ReportIdleUnavailableReason,
};

const IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_BACKOFF: Duration = Duration::from_secs(2);
const REPORT_TIMEOUT: Duration = Duration::from_secs(3);
const ONE_MILLISECOND: Duration = Duration::from_millis(1);

type RecordedRequests = Arc<Mutex<Vec<ReportIdleRequest>>>;

#[derive(Debug, Clone, Copy)]
struct TestReportError;

#[test]
fn config_rejects_zero_durations() {
    let config = |idle, backoff, report| IdleReportConfig::new(idle, backoff, report);
    assert_eq!(
        config(Duration::ZERO, RETRY_BACKOFF, REPORT_TIMEOUT),
        Err(IdleReportConfigError::ZeroIdleTimeout)
    );
    assert_eq!(
        config(IDLE_TIMEOUT, Duration::ZERO, REPORT_TIMEOUT),
        Err(IdleReportConfigError::ZeroRetryBackoff)
    );
    assert_eq!(
        config(IDLE_TIMEOUT, RETRY_BACKOFF, Duration::ZERO),
        Err(IdleReportConfigError::ZeroReportTimeout)
    );
}

#[tokio::test(start_paused = true)]
async fn report_fires_only_after_zero_active_for_full_idle_timeout() {
    let mut client = FakeReportIdleClient::accepting();
    let requests = client.requests();
    run(&detector_for(drain_tracker()), &mut client, async {
        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 0);
        advance(ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 1);
    })
    .await;
    assert_eq!(requests.lock().unwrap()[0], expected_report_request());
}

#[tokio::test(start_paused = true)]
async fn active_work_prevents_reporting_until_it_closes() {
    let drain = drain_tracker();
    let permit = drain.try_acquire().expect("work admitted");
    let mut client = FakeReportIdleClient::accepting();
    let requests = client.requests();
    run(&detector_for(drain.clone()), &mut client, async {
        advance(IDLE_TIMEOUT * 2).await;
        assert_recorded_count(&requests, 0);

        drop(permit);
        yield_now().await;
        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 0);
        advance(ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 1);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn activity_during_the_idle_interval_restarts_it() {
    for concurrent in [1, 2] {
        let drain = drain_tracker();
        let mut client = FakeReportIdleClient::accepting();
        let requests = client.requests();
        run(&detector_for(drain.clone()), &mut client, async {
            advance(IDLE_TIMEOUT / 2).await;

            let permits = (0..concurrent)
                .map(|_| drain.try_acquire().expect("work admitted"))
                .collect::<Vec<_>>();
            yield_now().await;
            drop(permits);
            yield_now().await;

            advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 0);
            advance(ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 1);
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn an_idle_pod_reports_again_every_backoff_after_an_accepted_report() {
    for first in [accepted_response(), already_draining_response()] {
        let drain = drain_tracker();
        let mut client = FakeReportIdleClient::new([Ok(first)]);
        let requests = client.requests();
        run(&detector_for(drain.clone()), &mut client, async {
            advance(IDLE_TIMEOUT).await;
            assert_recorded_count(&requests, 1);

            advance(RETRY_BACKOFF - ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 1);
            advance(ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 2);
            advance(RETRY_BACKOFF).await;
            assert_recorded_count(&requests, 3);

            // A revived pod serves again, then earns a full idle interval.
            let permit = drain
                .try_acquire()
                .expect("a reported sidecar still admits work");
            yield_now().await;
            advance(IDLE_TIMEOUT * 2).await;
            assert_recorded_count(&requests, 3);
            drop(permit);
            yield_now().await;
            advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 3);
            advance(ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 4);
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_report_that_never_answers_ends_at_its_deadline_and_retries() {
    struct SilentThenAccepting {
        calls: usize,
    }
    impl ReportIdleClient for SilentThenAccepting {
        type Error = TestReportError;
        fn report_idle(
            &mut self,
            _: ReportIdleRequest,
        ) -> ReportIdleFuture<'_, ReportIdleResponse, TestReportError> {
            self.calls += 1;
            let silent = self.calls == 1;
            Box::pin(async move {
                if silent {
                    std::future::pending::<()>().await;
                }
                Ok(accepted_response())
            })
        }
    }
    let sink = InMemoryObservability::default();
    let detector = IdleDetector::with_observability(
        instance_id(),
        generation(),
        config(),
        drain_tracker(),
        sink.recorder(),
    );
    let mut client = SilentThenAccepting { calls: 0 };
    run(&detector, &mut client, async {
        advance(IDLE_TIMEOUT).await;
        advance(REPORT_TIMEOUT - ONE_MILLISECOND).await;
        assert_eq!(report_outcomes(&sink), [""; 0]);
        advance(ONE_MILLISECOND).await;
        assert_eq!(report_outcomes(&sink), ["timeout"]);
        advance(RETRY_BACKOFF).await;
        assert_eq!(report_outcomes(&sink), ["timeout", "success"]);
    })
    .await;
    assert_eq!(client.calls, 2);
}

#[tokio::test(start_paused = true)]
async fn a_burst_during_the_response_poll_preserves_the_original_activity_revision() {
    struct BurstClient {
        drain: DrainTracker,
        calls: Arc<Mutex<usize>>,
        hint: bool,
    }
    impl ReportIdleClient for BurstClient {
        type Error = TestReportError;
        fn report_idle(
            &mut self,
            _: ReportIdleRequest,
        ) -> ReportIdleFuture<'_, ReportIdleResponse, TestReportError> {
            Box::pin(async move {
                let mut calls = self.calls.lock().unwrap();
                *calls += 1;
                if *calls == 1 {
                    drop(self.drain.try_acquire().unwrap());
                    if self.hint {
                        Ok(ReportIdleResponse::RetryAfter {
                            duration: RETRY_BACKOFF,
                        })
                    } else {
                        Err(TestReportError)
                    }
                } else {
                    Ok(accepted_response())
                }
            })
        }
    }
    for hint in [false, true] {
        let drain = drain_tracker();
        let calls = Arc::new(Mutex::new(0));
        let mut client = BurstClient {
            drain: drain.clone(),
            calls: calls.clone(),
            hint,
        };
        run(&detector_for(drain), &mut client, async {
            advance(IDLE_TIMEOUT).await;
            assert_eq!(*calls.lock().unwrap(), 1);
            advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
            assert_eq!(
                *calls.lock().unwrap(),
                1,
                "a completed burst must reset to full idle, not retry backoff"
            );
            advance(ONE_MILLISECOND).await;
            assert_eq!(*calls.lock().unwrap(), 2);
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn activation_retry_hint_avoids_polling_and_activity_rearms_full_idle_interval() {
    let drain = drain_tracker();
    let mut client = FakeReportIdleClient::new([Ok(ReportIdleResponse::RetryAfter {
        duration: Duration::from_secs(190),
    })]);
    let requests = client.requests();
    run(&detector_for(drain.clone()), &mut client, async {
        advance(IDLE_TIMEOUT).await;
        assert_recorded_count(&requests, 1);
        advance(Duration::from_secs(100)).await;
        assert_recorded_count(&requests, 1);
        let permit = drain.try_acquire().unwrap();
        yield_now().await;
        advance(Duration::from_secs(200)).await;
        assert_recorded_count(&requests, 1);
        drop(permit);
        yield_now().await;
        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 1);
        advance(ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 2);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn abandoned_activation_retry_expires_finitely_without_traffic() {
    let mut client = FakeReportIdleClient::new([Ok(ReportIdleResponse::RetryAfter {
        duration: Duration::from_secs(190),
    })]);
    let requests = client.requests();
    run(&detector_for(drain_tracker()), &mut client, async {
        advance(IDLE_TIMEOUT).await;
        advance(Duration::from_secs(190) - ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 1);
        advance(ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 2);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn reports_record_their_outcome_and_identity_fields() {
    let sink = InMemoryObservability::default();
    let detector = IdleDetector::with_observability(
        instance_id(),
        generation(),
        config(),
        drain_tracker(),
        sink.recorder(),
    );
    let mut client = FakeReportIdleClient::new([
        Ok(accepted_response()),
        Ok(already_draining_response()),
        Ok(generation_conflict_response()),
        Ok(unavailable_response()),
        Err(TestReportError),
    ]);
    run(&detector, &mut client, async {
        advance(IDLE_TIMEOUT).await;
        for _ in 0..4 {
            advance(RETRY_BACKOFF).await;
        }
    })
    .await;

    assert_eq!(
        report_outcomes(&sink),
        [
            "success",
            "already_draining",
            "rejected",
            "rejected",
            "error"
        ]
    );
    assert!(sink.events().iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log)
            if log.name() == EVENT_IDLE_REPORT
                && log.field_value(FIELD_INSTANCE_ID) == Some("instance-a")
                && log.field_value(FIELD_ACTIVE_COUNT) == Some("0")
    )));
}

#[tokio::test(start_paused = true)]
async fn refusals_and_transport_errors_retry_after_backoff() {
    for first in [
        Ok(generation_conflict_response()),
        Ok(unavailable_response()),
        Err(TestReportError),
    ] {
        let mut client = FakeReportIdleClient::new([first]);
        let requests = client.requests();
        run(&detector_for(drain_tracker()), &mut client, async {
            advance(IDLE_TIMEOUT).await;
            assert_recorded_count(&requests, 1);
            advance(RETRY_BACKOFF - ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 1);
            advance(ONE_MILLISECOND).await;
            assert_recorded_count(&requests, 2);
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn active_work_during_retry_backoff_resets_to_idle_timeout() {
    let drain = drain_tracker();
    let mut client = FakeReportIdleClient::new([Err(TestReportError)]);
    let requests = client.requests();
    run(&detector_for(drain.clone()), &mut client, async {
        advance(IDLE_TIMEOUT).await;
        assert_recorded_count(&requests, 1);

        let permit = drain.try_acquire().expect("work admitted");
        yield_now().await;
        advance(RETRY_BACKOFF).await;
        assert_recorded_count(&requests, 1);

        drop(permit);
        yield_now().await;
        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 1);
        advance(ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 2);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn active_work_during_report_round_trip_aborts_report_and_rearms() {
    let drain = drain_tracker();
    let mut client = FakeReportIdleClient::accepting().with_response_delay(RETRY_BACKOFF);
    let requests = client.requests();
    run(&detector_for(drain.clone()), &mut client, async {
        advance(IDLE_TIMEOUT).await;
        assert_recorded_count(&requests, 1);

        let permit = drain.try_acquire().expect("work admitted during RTT");
        yield_now().await;
        drop(permit);
        yield_now().await;

        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 1);
        advance(ONE_MILLISECOND).await;
        assert_recorded_count(&requests, 2);
    })
    .await;
}

/// Runs the detector while `script` drives time and activity, then stops it.
async fn run<Client: ReportIdleClient>(
    detector: &IdleDetector,
    client: &mut Client,
    script: impl Future<Output = ()>,
) {
    tokio::select! {
        biased;
        never = detector.run(client) => match never {},
        () = async { yield_now().await; script.await } => {}
    }
}

async fn advance(duration: Duration) {
    tokio::time::advance(duration).await;
    yield_now().await;
}

async fn yield_now() {
    tokio::task::yield_now().await;
}

fn assert_recorded_count(requests: &RecordedRequests, expected: usize) {
    assert_eq!(requests.lock().unwrap().len(), expected);
}

fn report_outcomes(sink: &InMemoryObservability) -> Vec<String> {
    sink.events()
        .iter()
        .filter_map(|event| match event {
            ObservabilityEvent::Metric(metric)
                if metric.name() == RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME =>
            {
                metric.labels().last().map(|label| label.value().to_owned())
            }
            _ => None,
        })
        .collect()
}

fn expected_report_request() -> ReportIdleRequest {
    ReportIdleRequest::new(instance_id(), generation(), IdleObservation::zero_active())
}

fn accepted_response() -> ReportIdleResponse {
    ReportIdleResponse::Accepted {
        instance_id: instance_id(),
        generation: generation(),
    }
}

fn already_draining_response() -> ReportIdleResponse {
    ReportIdleResponse::AlreadyDraining {
        instance_id: instance_id(),
        generation: generation(),
    }
}

fn generation_conflict_response() -> ReportIdleResponse {
    ReportIdleResponse::GenerationConflict {
        instance_id: instance_id(),
        expected_generation: generation(),
        actual_generation: Generation::new(8),
    }
}

fn unavailable_response() -> ReportIdleResponse {
    ReportIdleResponse::Unavailable {
        instance_id: instance_id(),
        generation: generation(),
        reason: ReportIdleUnavailableReason::Waking,
    }
}

fn detector_for(drain: DrainTracker) -> IdleDetector {
    IdleDetector::with_observability(
        instance_id(),
        generation(),
        config(),
        drain,
        ObservabilityRecorder::default(),
    )
}

fn config() -> IdleReportConfig {
    IdleReportConfig::new(IDLE_TIMEOUT, RETRY_BACKOFF, REPORT_TIMEOUT).expect("idle config builds")
}

fn drain_tracker() -> DrainTracker {
    DrainTracker::new(Duration::from_secs(30))
}

fn instance_id() -> InstanceId {
    InstanceId::new("instance-a").expect("instance id")
}

fn generation() -> Generation {
    Generation::new(7)
}

impl fmt::Display for TestReportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test report error")
    }
}

impl std::error::Error for TestReportError {}

/// Answers with the queued responses, then accepts every later report.
struct FakeReportIdleClient {
    responses: VecDeque<Result<ReportIdleResponse, TestReportError>>,
    requests: RecordedRequests,
    response_delay: Option<Duration>,
}

impl FakeReportIdleClient {
    fn new(
        responses: impl IntoIterator<Item = Result<ReportIdleResponse, TestReportError>>,
    ) -> Self {
        Self {
            responses: responses.into_iter().collect(),
            requests: Arc::new(Mutex::new(Vec::new())),
            response_delay: None,
        }
    }

    fn accepting() -> Self {
        Self::new([])
    }

    fn with_response_delay(mut self, response_delay: Duration) -> Self {
        self.response_delay = Some(response_delay);
        self
    }

    fn requests(&self) -> RecordedRequests {
        Arc::clone(&self.requests)
    }
}

impl ReportIdleClient for FakeReportIdleClient {
    type Error = TestReportError;

    fn report_idle(
        &mut self,
        request: ReportIdleRequest,
    ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error> {
        self.requests.lock().unwrap().push(request);
        let response = self
            .responses
            .pop_front()
            .unwrap_or_else(|| Ok(accepted_response()));
        let response_delay = self.response_delay;

        Box::pin(async move {
            if let Some(response_delay) = response_delay {
                tokio::time::sleep(response_delay).await;
            }
            response
        })
    }
}
