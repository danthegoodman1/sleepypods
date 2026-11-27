use std::{error::Error, fmt, future::Future, pin::Pin};

use sleepypods_api::{
    pb::{self, sidecar_control_plane_client::SidecarControlPlaneClient},
    Generation, InstanceId,
};
use tonic::codegen::Body;

use crate::ReportIdleRequest;

pub type ReportIdleFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub trait ReportIdleClient {
    type Error;

    fn report_idle(
        &mut self,
        request: ReportIdleRequest,
    ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportIdleResponse {
    /// Expected activation deferral, encoded as bounded status metadata without
    /// changing the protobuf wire contract.
    RetryAfter { duration: std::time::Duration },
    Accepted {
        instance_id: InstanceId,
        generation: Generation,
    },
    AlreadyDraining {
        instance_id: InstanceId,
        generation: Generation,
    },
    GenerationConflict {
        instance_id: InstanceId,
        expected_generation: Generation,
        actual_generation: Generation,
    },
    Unavailable {
        instance_id: InstanceId,
        generation: Generation,
        reason: ReportIdleUnavailableReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportIdleUnavailableReason {
    Cold,
    Waking,
    Draining,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Debug)]
pub struct GrpcSidecarControlPlaneClient<T> {
    client: SidecarControlPlaneClient<T>,
    pod_uid: String,
}

#[derive(Debug)]
pub enum GrpcSidecarControlPlaneError {
    Status(tonic::Status),
    Protocol(SidecarProtocolAdapterError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidecarProtocolAdapterError {
    MissingField {
        field: &'static str,
    },
    InvalidField {
        field: &'static str,
        message: String,
    },
    InvalidEnum {
        field: &'static str,
        value: i32,
    },
}

impl<T> GrpcSidecarControlPlaneClient<T> {
    pub fn new(client: SidecarControlPlaneClient<T>) -> Self {
        Self {
            client,
            pod_uid: String::new(),
        }
    }

    /// UID supplied by Kubernetes downward API; empty identities fail closed at the server.
    pub fn with_pod_uid(mut self, pod_uid: String) -> Self {
        self.pod_uid = pod_uid;
        self
    }

    #[cfg(test)]
    pub fn inner(&self) -> &SidecarControlPlaneClient<T> {
        &self.client
    }

    #[cfg(test)]
    pub fn into_inner(self) -> SidecarControlPlaneClient<T> {
        self.client
    }
}

impl<T> GrpcSidecarControlPlaneClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Future: Send,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    async fn report_idle_via_transport(
        &mut self,
        request: ReportIdleRequest,
    ) -> Result<ReportIdleResponse, GrpcSidecarControlPlaneError> {
        let mut request = report_idle_request_to_proto(request);
        request.pod_uid = self.pod_uid.clone();
        let response = match self.client.report_idle(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                if let Some(duration) = idle_retry_after(&status) {
                    return Ok(ReportIdleResponse::RetryAfter { duration });
                }
                return Err(GrpcSidecarControlPlaneError::Status(status));
            }
        };

        report_idle_response_from_proto(response).map_err(GrpcSidecarControlPlaneError::Protocol)
    }
}

fn idle_retry_after(status: &tonic::Status) -> Option<std::time::Duration> {
    if status.code() != tonic::Code::FailedPrecondition {
        return None;
    }
    let millis = status
        .metadata()
        .get(sleepypods_api::IDLE_RETRY_AFTER_METADATA)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    let duration = std::time::Duration::from_millis(millis);
    (millis > 0 && duration <= sleepypods_api::INITIAL_ACTIVATION_TIMEOUT).then_some(duration)
}

impl<T> ReportIdleClient for GrpcSidecarControlPlaneClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Future: Send,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type Error = GrpcSidecarControlPlaneError;

    fn report_idle(
        &mut self,
        request: ReportIdleRequest,
    ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error> {
        Box::pin(async move { self.report_idle_via_transport(request).await })
    }
}

pub fn report_idle_request_to_proto(request: ReportIdleRequest) -> pb::SidecarReportIdleRequest {
    pb::SidecarReportIdleRequest {
        instance_id: request.instance_id().as_str().to_owned(),
        expected_generation: request.generation().get(),
        active_count: request.observation().active_count() as u64,
        pod_uid: String::new(),
    }
}

pub fn report_idle_response_from_proto(
    response: pb::SidecarReportIdleResponse,
) -> Result<ReportIdleResponse, SidecarProtocolAdapterError> {
    match response
        .outcome
        .ok_or(SidecarProtocolAdapterError::MissingField { field: "outcome" })?
    {
        pb::sidecar_report_idle_response::Outcome::Accepted(response) => {
            Ok(ReportIdleResponse::Accepted {
                instance_id: instance_id(response.instance_id)?,
                generation: Generation::new(response.instance_generation),
            })
        }
        pb::sidecar_report_idle_response::Outcome::AlreadyDraining(response) => {
            Ok(ReportIdleResponse::AlreadyDraining {
                instance_id: instance_id(response.instance_id)?,
                generation: Generation::new(response.instance_generation),
            })
        }
        pb::sidecar_report_idle_response::Outcome::GenerationConflict(response) => {
            Ok(ReportIdleResponse::GenerationConflict {
                instance_id: instance_id(response.instance_id)?,
                expected_generation: Generation::new(response.expected_generation),
                actual_generation: Generation::new(response.actual_generation),
            })
        }
        pb::sidecar_report_idle_response::Outcome::Unavailable(response) => {
            Ok(ReportIdleResponse::Unavailable {
                instance_id: instance_id(response.instance_id)?,
                generation: Generation::new(response.instance_generation),
                reason: unavailable_reason_from_proto(response.reason)?,
            })
        }
    }
}

impl fmt::Display for GrpcSidecarControlPlaneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => write!(f, "sidecar control-plane gRPC status: {status}"),
            Self::Protocol(error) => write!(f, "sidecar control-plane protocol error: {error}"),
        }
    }
}

impl Error for GrpcSidecarControlPlaneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Status(status) => Some(status),
            Self::Protocol(error) => Some(error),
        }
    }
}

impl fmt::Display for SidecarProtocolAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(f, "{field} is required"),
            Self::InvalidField { field, message } => write!(f, "{field} is invalid: {message}"),
            Self::InvalidEnum { field, value } => {
                write!(f, "{field} has invalid enum value {value}")
            }
        }
    }
}

impl Error for SidecarProtocolAdapterError {}

fn instance_id(value: String) -> Result<InstanceId, SidecarProtocolAdapterError> {
    InstanceId::new(value).map_err(|error| SidecarProtocolAdapterError::InvalidField {
        field: error.field(),
        message: error.to_string(),
    })
}

fn unavailable_reason_from_proto(
    value: i32,
) -> Result<ReportIdleUnavailableReason, SidecarProtocolAdapterError> {
    let reason = pb::SidecarReportIdleUnavailableReason::try_from(value).map_err(|_| {
        SidecarProtocolAdapterError::InvalidEnum {
            field: "unavailable.reason",
            value,
        }
    })?;

    match reason {
        pb::SidecarReportIdleUnavailableReason::Cold => Ok(ReportIdleUnavailableReason::Cold),
        pb::SidecarReportIdleUnavailableReason::Waking => Ok(ReportIdleUnavailableReason::Waking),
        pb::SidecarReportIdleUnavailableReason::Draining => {
            Ok(ReportIdleUnavailableReason::Draining)
        }
        pb::SidecarReportIdleUnavailableReason::Failed => Ok(ReportIdleUnavailableReason::Failed),
        pb::SidecarReportIdleUnavailableReason::Deleting => {
            Ok(ReportIdleUnavailableReason::Deleting)
        }
        pb::SidecarReportIdleUnavailableReason::Deleted => Ok(ReportIdleUnavailableReason::Deleted),
        pb::SidecarReportIdleUnavailableReason::Unspecified => {
            Err(SidecarProtocolAdapterError::InvalidEnum {
                field: "unavailable.reason",
                value,
            })
        }
    }
}

#[cfg(test)]
mod tests;
