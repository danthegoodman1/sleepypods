use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use sleepypods_api::{
    pb::{
        self,
        sidecar_control_plane_client::SidecarControlPlaneClient,
        sidecar_control_plane_server::{SidecarControlPlane, SidecarControlPlaneServer},
    },
    Generation, InstanceId,
};
use tonic::{
    codegen::{http, Service},
    Request, Response, Status,
};

use super::{
    GrpcSidecarControlPlaneClient, GrpcSidecarControlPlaneError, ReportIdleClient,
    ReportIdleResponse, ReportIdleUnavailableReason, SidecarProtocolAdapterError,
};
use crate::{IdleObservation, ReportIdleRequest};

#[tokio::test]
async fn activation_retry_hint_is_bounded_and_mapped_through_generated_transport() {
    for (code, hint, expected) in [
        (tonic::Code::FailedPrecondition, "190000", true),
        (tonic::Code::FailedPrecondition, "190001", false),
        (tonic::Code::FailedPrecondition, "0", false),
        (tonic::Code::FailedPrecondition, "bogus", false),
        (tonic::Code::Unavailable, "1000", false),
    ] {
        let service = FakeSidecarControlPlane::default();
        let mut status = Status::new(code, "activation pending");
        status.metadata_mut().insert(
            sleepypods_api::IDLE_RETRY_AFTER_METADATA,
            hint.parse().unwrap(),
        );
        service.set_response(Err(status));
        let result = test_client(service)
            .report_idle(report_idle_request("instance-a", 7))
            .await;
        assert_eq!(
            matches!(result, Ok(ReportIdleResponse::RetryAfter { .. })),
            expected
        );
    }
}

#[tokio::test]
async fn accepted_response_maps_through_generated_client() {
    let service = FakeSidecarControlPlane::default();
    service.set_response(Ok(pb::SidecarReportIdleResponse {
        outcome: Some(pb::sidecar_report_idle_response::Outcome::Accepted(
            pb::SidecarReportIdleAccepted {
                instance_id: "instance-a".to_owned(),
                instance_generation: 8,
            },
        )),
    }));
    let mut client = test_client(service.clone());

    let response = client
        .report_idle(report_idle_request("instance-a", 7))
        .await
        .expect("report idle succeeds");

    assert_eq!(
        response,
        ReportIdleResponse::Accepted {
            instance_id: instance_id("instance-a"),
            generation: Generation::new(8),
        }
    );
    assert_eq!(
        service.requests(),
        vec![pb::SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-a".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }]
    );
}

#[tokio::test]
async fn already_draining_response_preserves_instance_generation() {
    let service = FakeSidecarControlPlane::default();
    service.set_response(Ok(pb::SidecarReportIdleResponse {
        outcome: Some(pb::sidecar_report_idle_response::Outcome::AlreadyDraining(
            pb::SidecarReportIdleAlreadyDraining {
                instance_id: "instance-draining".to_owned(),
                instance_generation: 11,
            },
        )),
    }));
    let mut client = test_client(service);

    let response = client
        .report_idle(report_idle_request("instance-draining", 10))
        .await
        .expect("report idle succeeds");

    assert_eq!(
        response,
        ReportIdleResponse::AlreadyDraining {
            instance_id: instance_id("instance-draining"),
            generation: Generation::new(11),
        }
    );
}

#[tokio::test]
async fn generation_conflict_preserves_expected_and_actual_generations() {
    let service = FakeSidecarControlPlane::default();
    service.set_response(Ok(pb::SidecarReportIdleResponse {
        outcome: Some(
            pb::sidecar_report_idle_response::Outcome::GenerationConflict(
                pb::SidecarReportIdleGenerationConflict {
                    instance_id: "instance-conflict".to_owned(),
                    expected_generation: 4,
                    actual_generation: 5,
                },
            ),
        ),
    }));
    let mut client = test_client(service);

    let response = client
        .report_idle(report_idle_request("instance-conflict", 4))
        .await
        .expect("report idle succeeds");

    assert_eq!(
        response,
        ReportIdleResponse::GenerationConflict {
            instance_id: instance_id("instance-conflict"),
            expected_generation: Generation::new(4),
            actual_generation: Generation::new(5),
        }
    );
}

#[tokio::test]
async fn unavailable_response_preserves_generation_and_reason() {
    let service = FakeSidecarControlPlane::default();
    service.set_response(Ok(pb::SidecarReportIdleResponse {
        outcome: Some(pb::sidecar_report_idle_response::Outcome::Unavailable(
            pb::SidecarReportIdleUnavailable {
                instance_id: "instance-unavailable".to_owned(),
                instance_generation: 13,
                reason: pb::SidecarReportIdleUnavailableReason::Waking as i32,
            },
        )),
    }));
    let mut client = test_client(service);

    let response = client
        .report_idle(report_idle_request("instance-unavailable", 12))
        .await
        .expect("report idle succeeds");

    assert_eq!(
        response,
        ReportIdleResponse::Unavailable {
            instance_id: instance_id("instance-unavailable"),
            generation: Generation::new(13),
            reason: ReportIdleUnavailableReason::Waking,
        }
    );
}

#[tokio::test]
async fn grpc_status_error_is_surfaced() {
    let service = FakeSidecarControlPlane::default();
    service.set_response(Err(Status::unavailable("control plane unavailable")));
    let mut client = test_client(service);

    let error = client
        .report_idle(report_idle_request("instance-a", 7))
        .await
        .expect_err("status should surface");

    assert!(matches!(
        error,
        GrpcSidecarControlPlaneError::Status(status)
            if status.code() == tonic::Code::Unavailable
    ));
}

#[tokio::test]
async fn missing_response_outcome_is_protocol_error() {
    let service = FakeSidecarControlPlane::default();
    service.set_response(Ok(pb::SidecarReportIdleResponse { outcome: None }));
    let mut client = test_client(service);

    let error = client
        .report_idle(report_idle_request("instance-a", 7))
        .await
        .expect_err("missing outcome should surface");

    assert!(matches!(
        error,
        GrpcSidecarControlPlaneError::Protocol(SidecarProtocolAdapterError::MissingField {
            field: "outcome"
        })
    ));
}

#[tokio::test]
async fn unknown_unavailable_reason_is_protocol_error() {
    let service = FakeSidecarControlPlane::default();
    service.set_response(Ok(pb::SidecarReportIdleResponse {
        outcome: Some(pb::sidecar_report_idle_response::Outcome::Unavailable(
            pb::SidecarReportIdleUnavailable {
                instance_id: "instance-a".to_owned(),
                instance_generation: 7,
                reason: 99,
            },
        )),
    }));
    let mut client = test_client(service);

    let error = client
        .report_idle(report_idle_request("instance-a", 7))
        .await
        .expect_err("unknown reason should surface");

    assert!(matches!(
        error,
        GrpcSidecarControlPlaneError::Protocol(SidecarProtocolAdapterError::InvalidEnum {
            field: "unavailable.reason",
            value: 99,
        })
    ));
}

#[derive(Clone)]
struct InProcessService<S> {
    inner: S,
}

impl<S> InProcessService<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<http::Request<tonic::body::Body>> for InProcessService<S>
where
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        self.inner.call(request)
    }
}

#[derive(Clone, Default)]
struct FakeSidecarControlPlane {
    state: Arc<Mutex<FakeSidecarControlPlaneState>>,
}

#[derive(Default)]
struct FakeSidecarControlPlaneState {
    response: Option<Result<pb::SidecarReportIdleResponse, Status>>,
    requests: Vec<pb::SidecarReportIdleRequest>,
}

#[tonic::async_trait]
impl SidecarControlPlane for FakeSidecarControlPlane {
    async fn report_idle(
        &self,
        request: Request<pb::SidecarReportIdleRequest>,
    ) -> Result<Response<pb::SidecarReportIdleResponse>, Status> {
        let mut state = self.state.lock().expect("fake state");
        state.requests.push(request.into_inner());
        state
            .response
            .take()
            .unwrap_or_else(|| {
                Err(Status::failed_precondition(
                    "test did not configure response",
                ))
            })
            .map(Response::new)
    }
}

impl FakeSidecarControlPlane {
    fn set_response(&self, response: Result<pb::SidecarReportIdleResponse, Status>) {
        self.state.lock().expect("fake state").response = Some(response);
    }

    fn requests(&self) -> Vec<pb::SidecarReportIdleRequest> {
        self.state.lock().expect("fake state").requests.clone()
    }
}

fn test_client(
    service: FakeSidecarControlPlane,
) -> GrpcSidecarControlPlaneClient<
    InProcessService<SidecarControlPlaneServer<FakeSidecarControlPlane>>,
> {
    let server = SidecarControlPlaneServer::new(service);
    GrpcSidecarControlPlaneClient::new(SidecarControlPlaneClient::new(InProcessService::new(
        server,
    )))
    .with_pod_uid("test-pod".to_owned())
}

fn report_idle_request(instance_id: &str, generation: u64) -> ReportIdleRequest {
    ReportIdleRequest::new(
        self::instance_id(instance_id),
        Generation::new(generation),
        IdleObservation::zero_active(),
    )
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

/// A control plane whose host vanished: the connection stays open and nothing
/// answers. The channel's keepalive ends the call, so the next report gets a
/// new connection.
#[tokio::test]
async fn a_silent_control_plane_connection_fails_the_report_at_the_keepalive_deadline() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let silent = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // An empty SETTINGS frame and the acknowledgement of the client's.
        stream
            .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0])
            .await
            .unwrap();
        let mut discard = [0_u8; 4096];
        while stream.read(&mut discard).await.is_ok_and(|read| read > 0) {}
    });
    let channel = sleepypods_api::transport::native_endpoint(format!("http://{addr}"), None)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = GrpcSidecarControlPlaneClient::new(SidecarControlPlaneClient::new(channel));

    // The connection exists; from here only timers decide what happens.
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let result = client
        .report_idle(report_idle_request("instance-a", 7))
        .await;

    assert!(matches!(
        result,
        Err(GrpcSidecarControlPlaneError::Status(_))
    ));
    // One 30-second interval, then the 10-second acknowledgement timeout.
    let elapsed = started.elapsed();
    assert!(
        (std::time::Duration::from_secs(30)..=std::time::Duration::from_secs(45))
            .contains(&elapsed),
        "{elapsed:?}"
    );
    silent.abort();
}
