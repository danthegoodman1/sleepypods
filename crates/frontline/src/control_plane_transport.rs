//! The gRPC transport for the proxy's control-plane calls: the Subscribe
//! stream, wake calls and HTTP-01 lookups.

use std::{error::Error, fmt};

use sleepypods_api::{
    pb::{self, proxy_control_plane_client::ProxyControlPlaneClient},
    Http01ChallengeKey, Http01ChallengeRecord,
};
use tokio::sync::mpsc;
use tonic::codegen::{
    tokio_stream::{wrappers::ReceiverStream, StreamExt},
    Body,
};

use crate::{
    http01_challenge_key_to_proto, http01_challenge_record_from_proto,
    proxy_wake_response_from_proto, route_stream_reply_from_proto, route_stream_request_to_proto,
    wake_instance_request_to_proto, Http01ChallengeResolveFuture, Http01ChallengeResolver,
    ProxyProtocolAdapterError, RouteStream, RouteStreamEvent, RouteSubscriptionClient,
    RouteSubscriptionFuture, WakeClient, WakeClientFuture, WakeInstanceRequest,
    WakeInstanceResponse,
};

/// Requests the coordinator may queue for the stream before it waits for
/// room.
const STREAM_REQUEST_BUFFER: usize = 256;
/// Control-plane messages read ahead of the coordinator. A full buffer holds
/// the reader, and HTTP/2 flow control holds the control plane.
const STREAM_EVENT_BUFFER: usize = 256;

#[derive(Clone, Debug)]
pub struct GrpcProxyControlPlaneClient<T> {
    client: ProxyControlPlaneClient<T>,
}

#[derive(Debug)]
pub struct GrpcProxyHttp01Resolver<T> {
    client: ProxyControlPlaneClient<T>,
}

#[derive(Clone, Debug)]
pub enum GrpcProxyControlPlaneError {
    Status(tonic::Status),
    Protocol(ProxyProtocolAdapterError),
}

#[derive(Clone, Debug)]
pub enum GrpcProxyHttp01ResolverError {
    Status(tonic::Status),
    Protocol(ProxyProtocolAdapterError),
}

impl<T> GrpcProxyControlPlaneClient<T> {
    pub fn new(client: ProxyControlPlaneClient<T>) -> Self {
        Self { client }
    }
}

impl<T> GrpcProxyHttp01Resolver<T> {
    pub fn new(client: ProxyControlPlaneClient<T>) -> Self {
        Self { client }
    }
}

impl<T> GrpcProxyHttp01Resolver<T>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    async fn resolve_http01_challenge_via_transport(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Result<Option<Http01ChallengeRecord>, GrpcProxyHttp01ResolverError> {
        let response = self
            .client
            .resolve_http01_challenge(pb::ResolveHttp01ChallengeRequest {
                key: Some(http01_challenge_key_to_proto(key)),
            })
            .await
            .map_err(GrpcProxyHttp01ResolverError::Status)?
            .into_inner();

        response
            .challenge
            .map(http01_challenge_record_from_proto)
            .transpose()
            .map_err(GrpcProxyHttp01ResolverError::Protocol)
    }
}

impl<T> RouteSubscriptionClient for GrpcProxyControlPlaneClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type Error = GrpcProxyControlPlaneError;

    fn connect(&mut self) -> RouteSubscriptionFuture<'static, RouteStream, Self::Error> {
        let mut client = self.client.clone();
        Box::pin(async move {
            let (requests, outbound) = mpsc::channel(STREAM_REQUEST_BUFFER);
            let responses = client
                .subscribe(ReceiverStream::new(outbound).map(route_stream_request_to_proto))
                .await
                .map_err(GrpcProxyControlPlaneError::Status)?
                .into_inner();
            let (events, inbound) = mpsc::channel(STREAM_EVENT_BUFFER);
            tokio::spawn(read_route_stream(responses, events));
            Ok(RouteStream {
                requests,
                events: inbound,
            })
        })
    }
}

/// Forwards every control-plane message in order, then how the stream ended.
/// It stops, and closes the stream, when the coordinator drops its end.
async fn read_route_stream(
    mut responses: tonic::codec::Streaming<pb::ProxySubscribeResponse>,
    events: mpsc::Sender<RouteStreamEvent>,
) {
    loop {
        let event = tokio::select! {
            _ = events.closed() => return,
            message = responses.message() => match message {
                Ok(Some(response)) => match route_stream_reply_from_proto(response) {
                    Ok(reply) => RouteStreamEvent::Reply(reply),
                    Err(_) => RouteStreamEvent::Failed,
                },
                Ok(None) => RouteStreamEvent::Ended,
                Err(_) => RouteStreamEvent::Failed,
            },
        };
        let last = !matches!(event, RouteStreamEvent::Reply(_));
        if events.send(event).await.is_err() || last {
            return;
        }
    }
}

impl<T> WakeClient for GrpcProxyControlPlaneClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type Error = GrpcProxyControlPlaneError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        let mut client = self.client.clone();
        Box::pin(async move {
            let response = client
                .wake_instance(wake_instance_request_to_proto(request))
                .await
                .map_err(GrpcProxyControlPlaneError::Status)?
                .into_inner();
            proxy_wake_response_from_proto(response).map_err(GrpcProxyControlPlaneError::Protocol)
        })
    }

    fn is_transient(error: &Self::Error) -> bool {
        error.is_transient()
    }
}

impl GrpcProxyControlPlaneError {
    /// A call that failed in transport, or that the control plane answered
    /// with Unavailable or DeadlineExceeded. A status the client built from a
    /// local transport error carries that error as its source; one the control
    /// plane sent carries none.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Status(status) => {
                matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) || status.source().is_some()
            }
            Self::Protocol(_) => false,
        }
    }
}

impl<T> Http01ChallengeResolver for GrpcProxyHttp01Resolver<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type Error = GrpcProxyHttp01ResolverError;

    fn resolve_http01_challenge(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Http01ChallengeResolveFuture<'_, Self::Error> {
        Box::pin(async move { self.resolve_http01_challenge_via_transport(key).await })
    }
}

impl fmt::Display for GrpcProxyControlPlaneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => write!(f, "proxy control-plane gRPC status: {status}"),
            Self::Protocol(error) => write!(f, "proxy control-plane protocol error: {error}"),
        }
    }
}

impl Error for GrpcProxyControlPlaneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Status(status) => Some(status),
            Self::Protocol(error) => Some(error),
        }
    }
}

impl fmt::Display for GrpcProxyHttp01ResolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => write!(f, "proxy HTTP-01 gRPC status: {status}"),
            Self::Protocol(error) => write!(f, "operator HTTP-01 protocol error: {error}"),
        }
    }
}

impl Error for GrpcProxyHttp01ResolverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Status(status) => Some(status),
            Self::Protocol(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
