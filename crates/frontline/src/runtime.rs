use std::{error::Error, time::Instant};

use bytes::Bytes;
use http::{header::HOST, Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use proxy_core::observability::{
    metrics::RUNTIME_HTTP01_RESULTS_TOTAL,
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder, EVENT_HTTP01,
    },
    Outcome,
};
use proxy_core::DrainTracker;

use crate::{
    intercept_http01_challenge, FrontlineForwardContext, FrontlineForwardError, FrontlineForwarder,
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome,
    Http01ChallengeResolver, Http01InterceptDecision, Http01InterceptError,
    NoopHttp01ChallengeResolver, ReadyBackend, RequestIdentityError, RouteSubscriptionClient,
    SharedFrontlineRouteCoordinator, WakeClient,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub type FrontlineRuntimeBody = UnsyncBoxBody<Bytes, BoxError>;

#[derive(Clone, Debug)]
pub struct FrontlineHttpRuntime<RouteClient, Wake, Http01 = NoopHttp01ChallengeResolver> {
    coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
    http01_resolver: Http01,
    forwarder: FrontlineForwarder,
    drain: DrainTracker,
    observability: ObservabilityRecorder,
}

impl<RouteClient, Wake> FrontlineHttpRuntime<RouteClient, Wake, NoopHttp01ChallengeResolver> {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(
        coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
        drain: DrainTracker,
    ) -> Self {
        Self::with_http01_resolver_and_observability(
            coordinator,
            NoopHttp01ChallengeResolver,
            drain,
            ObservabilityRecorder::default(),
        )
    }
}

impl<RouteClient, Wake, Http01> FrontlineHttpRuntime<RouteClient, Wake, Http01> {
    pub fn with_http01_resolver_and_observability(
        coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
        http01_resolver: Http01,
        drain: DrainTracker,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            coordinator,
            http01_resolver,
            forwarder: FrontlineForwarder::new(drain.clone()),
            drain,
            observability,
        }
    }

    pub fn coordinator(&self) -> &FrontlineRouteCoordinator<RouteClient, Wake> {
        &self.coordinator
    }

    #[cfg(test)]
    pub fn http01_resolver(&self) -> &Http01 {
        &self.http01_resolver
    }

    #[cfg(test)]
    pub fn forwarder(&self) -> &FrontlineForwarder {
        &self.forwarder
    }

    #[cfg(test)]
    pub fn drain(&self) -> &DrainTracker {
        &self.drain
    }

    pub fn into_parts(
        self,
    ) -> (
        FrontlineRouteCoordinator<RouteClient, Wake>,
        Http01,
        FrontlineForwarder,
        DrainTracker,
        ObservabilityRecorder,
    ) {
        (
            self.coordinator,
            self.http01_resolver,
            self.forwarder,
            self.drain,
            self.observability,
        )
    }
}

pub(crate) fn record_http01_response(observability: &ObservabilityRecorder, status: StatusCode) {
    let outcome = if status == StatusCode::OK {
        Outcome::Success
    } else {
        Outcome::Miss
    };
    observability.record_metric(MetricObservation::new(
        RUNTIME_HTTP01_RESULTS_TOTAL,
        vec![outcome.metric_label()],
        1.0,
    ));
    observability.record_log(LifecycleLogEvent::new(
        EVENT_HTTP01,
        vec![LogField::new("http.status", status.as_u16())],
    ));
}

pub(crate) fn record_http01_error<E>(
    observability: &ObservabilityRecorder,
    error: &Http01InterceptError<E>,
) {
    let reason = match error {
        Http01InterceptError::MissingHost => "missing_host",
        Http01InterceptError::InvalidHostHeader => "invalid_host_header",
        Http01InterceptError::InvalidTokenSegment => "invalid_token_segment",
        Http01InterceptError::InvalidHost(_) => "invalid_host",
        Http01InterceptError::InvalidChallenge(_) => "invalid_challenge",
        Http01InterceptError::Resolve(_) => "resolve",
    };
    observability.record_metric(MetricObservation::new(
        RUNTIME_HTTP01_RESULTS_TOTAL,
        vec![Outcome::Error.metric_label()],
        1.0,
    ));
    observability.record_log(LifecycleLogEvent::new(
        EVENT_HTTP01,
        vec![LogField::error_reason(reason)],
    ));
}

pub(crate) async fn resolve_http01_response<Http01, B>(
    resolver: &mut Http01,
    request: &Request<B>,
) -> Result<Option<Response<FrontlineRuntimeBody>>, Http01InterceptError<Http01::Error>>
where
    Http01: Http01ChallengeResolver,
{
    match intercept_http01_challenge(request, |key| resolver.resolve_http01_challenge(key)).await? {
        Http01InterceptDecision::PassThrough => Ok(None),
        Http01InterceptDecision::Miss { .. } => Ok(Some(status_response(StatusCode::NOT_FOUND))),
        Http01InterceptDecision::Serve { response, .. } => {
            Ok(Some(response.into_http_response().map(full_runtime_body)))
        }
    }
}

/// Routes a request by its Host header, or its URI authority when it has
/// none, and its path.
pub(crate) async fn resolve_http_route_shared<RouteClient, Wake, B>(
    coordinator: &SharedFrontlineRouteCoordinator<RouteClient, Wake>,
    request: &Request<B>,
    now: Instant,
) -> Result<FrontlineRouteOutcome, FrontlineRouteCoordinatorError<Wake::Error>>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
    Wake::Error: Clone + Send + Sync + 'static,
{
    let host = match request.headers().get(HOST) {
        Some(value) => value.to_str().map_err(|_| {
            FrontlineRouteCoordinatorError::Identity(RequestIdentityError::UnsupportedHostSyntax)
        })?,
        None => request
            .uri()
            .authority()
            .map(|authority| authority.as_str())
            .ok_or(FrontlineRouteCoordinatorError::Identity(
                RequestIdentityError::EmptyHost,
            ))?,
    };
    let path = request.uri().path_and_query().map(|value| value.path());

    coordinator.route_http(host, path, now).await
}

pub(crate) async fn route_outcome_or_forward_response<B>(
    forwarder: &FrontlineForwarder,
    outcome: FrontlineRouteOutcome,
    request: Request<B>,
    forwarding_context: Option<FrontlineForwardContext>,
) -> Response<FrontlineRuntimeBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError>,
{
    match outcome {
        FrontlineRouteOutcome::Ready(ready) => {
            forward_ready_response(forwarder, ready, request, forwarding_context).await
        }
        outcome => route_outcome_response(outcome),
    }
}

async fn forward_ready_response<B>(
    forwarder: &FrontlineForwarder,
    ready: ReadyBackend,
    request: Request<B>,
    forwarding_context: Option<FrontlineForwardContext>,
) -> Response<FrontlineRuntimeBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError>,
{
    let forwarded = match forwarding_context {
        Some(context) => {
            forwarder
                .forward_http_with_context(&ready, request, context)
                .await
        }
        None => forwarder.forward_http(&ready, request).await,
    };

    match forwarded {
        Ok(response) => response.map(box_runtime_body),
        Err(error) => forward_error_response(error),
    }
}

pub(crate) fn route_outcome_response(
    outcome: FrontlineRouteOutcome,
) -> Response<FrontlineRuntimeBody> {
    match outcome {
        FrontlineRouteOutcome::Ready(_) => {
            unreachable!("ready route outcomes are forwarded before response mapping")
        }
        FrontlineRouteOutcome::Miss => status_response(StatusCode::NOT_FOUND),
        FrontlineRouteOutcome::Unavailable(_) => status_response(StatusCode::SERVICE_UNAVAILABLE),
    }
}

/// A request the proxy does not route gets 414 for an over-long path and 400
/// otherwise. A request it could not route in time gets 503.
pub(crate) fn route_resolution_error_response<WakeClientError>(
    error: FrontlineRouteCoordinatorError<WakeClientError>,
) -> Response<FrontlineRuntimeBody> {
    status_response(match error {
        FrontlineRouteCoordinatorError::Identity(RequestIdentityError::PathTooLong) => {
            StatusCode::URI_TOO_LONG
        }
        FrontlineRouteCoordinatorError::Identity(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    })
}

pub(crate) fn http01_intercept_error_response<E>(
    error: Http01InterceptError<E>,
) -> Response<FrontlineRuntimeBody> {
    match error {
        Http01InterceptError::MissingHost
        | Http01InterceptError::InvalidHostHeader
        | Http01InterceptError::InvalidTokenSegment
        | Http01InterceptError::InvalidHost(_)
        | Http01InterceptError::InvalidChallenge(_) => status_response(StatusCode::BAD_REQUEST),
        Http01InterceptError::Resolve(_) => status_response(StatusCode::SERVICE_UNAVAILABLE),
    }
}

fn forward_error_response(error: FrontlineForwardError) -> Response<FrontlineRuntimeBody> {
    let status = match error {
        FrontlineForwardError::Http(proxy_core::HttpProxyError::UpstreamHeaderTimeout) => {
            StatusCode::GATEWAY_TIMEOUT
        }
        FrontlineForwardError::Http(
            proxy_core::HttpProxyError::Drain(_) | proxy_core::HttpProxyError::UpstreamSaturated,
        ) => StatusCode::SERVICE_UNAVAILABLE,
        FrontlineForwardError::Backend(_)
        | FrontlineForwardError::Http(
            proxy_core::HttpProxyError::RequestRewrite(_) | proxy_core::HttpProxyError::Client(_),
        )
        | FrontlineForwardError::WebSocket(_) => StatusCode::BAD_GATEWAY,
    };

    status_response(status)
}

fn status_response(status: StatusCode) -> Response<FrontlineRuntimeBody> {
    Response::builder()
        .status(status)
        .body(empty_body())
        .expect("status-only frontline runtime response builds")
}

fn box_runtime_body<B>(body: B) -> FrontlineRuntimeBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

fn full_runtime_body(body: Bytes) -> FrontlineRuntimeBody {
    Full::new(body)
        .map_err(|error| match error {})
        .boxed_unsync()
}

fn empty_body() -> FrontlineRuntimeBody {
    Full::new(Bytes::new())
        .map_err(|error| match error {})
        .boxed_unsync()
}

#[cfg(test)]
mod tests;
