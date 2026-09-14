use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use sleepypods_api::{
    pb::{self, proxy_control_plane_client::ProxyControlPlaneClient},
    Http01ChallengeKey, Http01ChallengeRecord, RouteIdentity,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tonic::codegen::{tokio_stream::wrappers::ReceiverStream, Body};

use crate::{
    http01_challenge_key_to_proto, http01_challenge_record_from_proto,
    proxy_subscribe_input_to_proto, proxy_subscribe_response_from_proto,
    proxy_wake_response_from_proto, wake_instance_request_to_proto, Http01ChallengeResolveFuture,
    Http01ChallengeResolver, ProxyProtocolAdapterError, ProxySubscribeInput, RouteRequestId,
    RouteSubscriptionClient, RouteSubscriptionEvent, RouteSubscriptionFuture,
    SubscribeControlPlaneOutput, SubscriptionId, WakeClient, WakeClientFuture, WakeInstanceRequest,
    WakeInstanceResponse,
};

const SUBSCRIBE_REQUEST_BUFFER: usize = 16;
const SUBSCRIBE_RESPONSE_BUFFER: usize = 256;
const DEFAULT_SUBSCRIBE_RECONNECT_BACKOFF: Duration = Duration::from_millis(250);
const SUBSCRIBE_DEADLINE: Duration = Duration::from_secs(5);
static NEXT_SUBSCRIPTION_SESSION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct GrpcProxyControlPlaneClient<T> {
    client: ProxyControlPlaneClient<T>,
    transport: Arc<Mutex<SubscriptionTransport<T>>>,
    events: Arc<SubscriptionEvents>,
}
#[derive(Debug)]
pub struct GrpcProxyHttp01Resolver<T> {
    client: ProxyControlPlaneClient<T>,
}
#[derive(Debug)]
struct SubscriptionTransport<T> {
    client: ProxyControlPlaneClient<T>,
    session: Option<GrpcRouteSubscriptionSession>,
    reconnect_backoff: Duration,
    reconnect: bool,
}
#[derive(Debug)]
struct GrpcRouteSubscriptionSession {
    id: u64,
    requests: mpsc::Sender<pb::ProxySubscribeRequest>,
    pending: PendingRouteResponses,
    closed: Arc<AtomicBool>,
    reader: tokio::task::JoinHandle<()>,
}
impl Drop for GrpcRouteSubscriptionSession {
    fn drop(&mut self) {
        self.reader.abort();
    }
}
#[derive(Debug, Default)]
struct SubscriptionEvents {
    queue: StdMutex<VecDeque<GrpcRouteSubscriptionEvent>>,
    notify: tokio::sync::Notify,
    reset_requested: AtomicBool,
}
impl SubscriptionEvents {
    // Overflow is a stream failure, never a blocking send. The single queued
    // close barrier replaces discarded events and invalidates every old entry.
    fn push(&self, event: GrpcRouteSubscriptionEvent) -> bool {
        let mut queue = self.queue.lock().expect("subscription events");
        let accepted = queue.len() < SUBSCRIBE_RESPONSE_BUFFER;
        if accepted {
            queue.push_back(event);
        } else {
            queue.clear();
            queue.push_back(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
        }
        drop(queue);
        self.notify.notify_one();
        accepted
    }
    fn drain(&self) -> Vec<GrpcRouteSubscriptionEvent> {
        self.queue
            .lock()
            .expect("subscription events")
            .drain(..)
            .collect()
    }
}
type PendingRouteResponses = Arc<
    Mutex<
        HashMap<
            RouteRequestId,
            oneshot::Sender<Result<SubscribeControlPlaneOutput, GrpcProxyControlPlaneError>>,
        >,
    >,
>;

#[derive(Debug)]
enum GrpcRouteSubscriptionEvent {
    Message(SubscribeControlPlaneOutput),
    /// The server ended the stream after delivering its queue, which is the
    /// ordinary outcome of the subscription lifetime cap.
    ResponseStreamEnded,
    ResponseStreamClosed,
    Status(tonic::Status),
    Protocol(ProxyProtocolAdapterError),
    UnexpectedRouteResponse {
        request_id: RouteRequestId,
    },
}

#[derive(Clone, Debug)]
pub enum GrpcProxyControlPlaneError {
    Status(tonic::Status),
    SubscribeRequestStreamClosed,
    SubscribeResponseStreamClosed,
    Protocol(ProxyProtocolAdapterError),
    UnexpectedRouteResponse { request_id: RouteRequestId },
}

#[derive(Clone, Debug)]
pub enum GrpcProxyHttp01ResolverError {
    Status(tonic::Status),
    Protocol(ProxyProtocolAdapterError),
}

impl<T: Clone> GrpcProxyControlPlaneClient<T> {
    pub fn new(client: ProxyControlPlaneClient<T>) -> Self {
        Self::with_subscribe_reconnect_backoff(client, DEFAULT_SUBSCRIBE_RECONNECT_BACKOFF)
    }
    pub fn with_subscribe_reconnect_backoff(
        client: ProxyControlPlaneClient<T>,
        backoff: Duration,
    ) -> Self {
        Self {
            transport: Arc::new(Mutex::new(SubscriptionTransport {
                client: client.clone(),
                session: None,
                reconnect_backoff: backoff,
                reconnect: false,
            })),
            client,
            events: Arc::new(SubscriptionEvents::default()),
        }
    }
}
impl<T> GrpcProxyControlPlaneClient<T> {
    pub fn inner(&self) -> &ProxyControlPlaneClient<T> {
        &self.client
    }
    pub fn inner_mut(&mut self) -> &mut ProxyControlPlaneClient<T> {
        &mut self.client
    }
    pub fn into_inner(self) -> ProxyControlPlaneClient<T> {
        self.client
    }
    pub async fn next_update(
        &mut self,
    ) -> Result<SubscribeControlPlaneOutput, GrpcProxyControlPlaneError> {
        loop {
            let notified = self.events.notify.notified();
            let event = self
                .events
                .queue
                .lock()
                .expect("subscription events")
                .pop_front();
            if let Some(event) = event {
                return match event {
                    GrpcRouteSubscriptionEvent::Message(message) => Ok(message),
                    GrpcRouteSubscriptionEvent::ResponseStreamEnded
                    | GrpcRouteSubscriptionEvent::ResponseStreamClosed => {
                        Err(GrpcProxyControlPlaneError::SubscribeResponseStreamClosed)
                    }
                    GrpcRouteSubscriptionEvent::Status(status) => {
                        Err(GrpcProxyControlPlaneError::Status(status))
                    }
                    GrpcRouteSubscriptionEvent::Protocol(error) => {
                        Err(GrpcProxyControlPlaneError::Protocol(error))
                    }
                    GrpcRouteSubscriptionEvent::UnexpectedRouteResponse { request_id } => {
                        Err(GrpcProxyControlPlaneError::UnexpectedRouteResponse { request_id })
                    }
                };
            }
            notified.await;
        }
    }
}
impl<T> GrpcProxyHttp01Resolver<T> {
    pub fn new(client: ProxyControlPlaneClient<T>) -> Self {
        Self { client }
    }

    pub fn inner(&self) -> &ProxyControlPlaneClient<T> {
        &self.client
    }

    pub fn inner_mut(&mut self) -> &mut ProxyControlPlaneClient<T> {
        &mut self.client
    }

    pub fn into_inner(self) -> ProxyControlPlaneClient<T> {
        self.client
    }
}

impl<T> SubscriptionTransport<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    async fn ensure(
        &mut self,
        events: Arc<SubscriptionEvents>,
    ) -> Result<&mut GrpcRouteSubscriptionSession, GrpcProxyControlPlaneError> {
        if events.reset_requested.swap(false, Ordering::AcqRel) {
            self.session = None;
            self.reconnect = true;
        }
        if self
            .session
            .as_ref()
            .is_some_and(|session| session.closed.load(Ordering::Acquire))
        {
            self.session = None;
            self.reconnect = true;
        }
        if self.session.is_none() {
            if self.reconnect {
                tokio::time::sleep(self.reconnect_backoff).await;
            }
            self.reconnect = true;
            let (requests, request_stream) = mpsc::channel(SUBSCRIBE_REQUEST_BUFFER);
            let responses = self
                .client
                .subscribe(ReceiverStream::new(request_stream))
                .await
                .map_err(GrpcProxyControlPlaneError::Status)?
                .into_inner();
            let pending = Arc::new(Mutex::new(HashMap::new()));
            let closed = Arc::new(AtomicBool::new(false));
            let id = NEXT_SUBSCRIPTION_SESSION
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .expect("subscription session ID space exhausted");
            let reader = tokio::spawn(read_subscription_responses(
                responses,
                events,
                pending.clone(),
                closed.clone(),
                requests.clone(),
                id,
            ));
            self.session = Some(GrpcRouteSubscriptionSession {
                id,
                requests,
                pending,
                closed,
                reader,
            });
        }
        Ok(self.session.as_mut().expect("initialized subscription"))
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
    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        let transport = self.transport.clone();
        let events = self.events.clone();
        Box::pin(async move {
            let work = async {
                let (tx, rx) = oneshot::channel();
                {
                    let mut transport = transport.lock().await;
                    let session = transport.ensure(events.clone()).await?;
                    // Keep cancelled request IDs until their replies arrive. They
                    // still occupy the bounded pending budget, and the reader
                    // unsubscribes late successful replies without failing peers.
                    let mut pending = session.pending.lock().await;
                    if pending.len() >= 64 {
                        if pending.values().any(oneshot::Sender::is_closed) {
                            // Abandoned calls cannot pin the session's admission
                            // budget forever when the server never answers them.
                            session.closed.store(true, Ordering::Release);
                            events.push(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
                        }
                        return Err(GrpcProxyControlPlaneError::Status(
                            tonic::Status::resource_exhausted("route subscriptions saturated"),
                        ));
                    }
                    pending.insert(request_id.clone(), tx);
                    let request =
                        proxy_subscribe_input_to_proto(ProxySubscribeInput::SubscribeRoute {
                            request_id: request_id.clone(),
                            identity,
                        });
                    drop(pending);
                    if session.requests.send(request).await.is_err() {
                        session.pending.lock().await.remove(&request_id);
                        session.closed.store(true, Ordering::Release);
                        events.push(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
                        return Err(GrpcProxyControlPlaneError::SubscribeRequestStreamClosed);
                    }
                }
                rx.await.unwrap_or(Err(
                    GrpcProxyControlPlaneError::SubscribeResponseStreamClosed,
                ))
            };
            match tokio::time::timeout(SUBSCRIBE_DEADLINE, work).await {
                Ok(result) => result,
                Err(_) => {
                    // A timed-out stream is ambiguous; close it before reuse. An
                    // abort cannot leave a late response eligible to repopulate cache.
                    let mut transport = transport.lock().await;
                    transport.session = None;
                    events.push(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
                    Err(GrpcProxyControlPlaneError::Status(
                        tonic::Status::deadline_exceeded("subscribe deadline"),
                    ))
                }
            }
        })
    }
    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        let Some(origin) = subscription_id.session() else {
            return Box::pin(async {
                Err(GrpcProxyControlPlaneError::Status(
                    tonic::Status::invalid_argument(
                        "unsubscribe requires a subscription ID returned by this transport",
                    ),
                ))
            });
        };
        let transport = self.transport.clone();
        let events = self.events.clone();
        Box::pin(async move {
            let work = async {
                let mut transport = transport.lock().await;
                let Some(session) = transport.session.as_mut() else {
                    return Ok(());
                };
                // Check the response's originating session after acquiring the
                // transport lock. A deferred cleanup must never unsubscribe a
                // replacement session's reused wire ID.
                if session.id != origin {
                    return Ok(());
                }
                let request = proxy_subscribe_input_to_proto(ProxySubscribeInput::Unsubscribe {
                    subscription_id,
                });
                if session.requests.send(request).await.is_err() {
                    session.closed.store(true, Ordering::Release);
                    events.push(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
                    return Err(GrpcProxyControlPlaneError::SubscribeRequestStreamClosed);
                }
                Ok(())
            };
            match tokio::time::timeout(Duration::from_secs(4), work).await {
                Ok(result) => result,
                Err(_) => {
                    // Persist the reset request even if the caller subsequently
                    // cancels; the next subscribe cannot reuse this session.
                    events.reset_requested.store(true, Ordering::Release);
                    events.push(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
                    Err(GrpcProxyControlPlaneError::Status(
                        tonic::Status::deadline_exceeded("unsubscribe deadline"),
                    ))
                }
            }
        })
    }
    fn reset_subscription(&mut self) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        let transport = self.transport.clone();
        let events = self.events.clone();
        self.events.reset_requested.store(true, Ordering::Release);
        self.events
            .push(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
        Box::pin(async move {
            let mut transport = transport.lock().await;
            // ensure() may have already consumed this reset and connected a
            // new session while the deferred reset future waited for the lock.
            if events.reset_requested.swap(false, Ordering::AcqRel) {
                transport.session = None;
            }
            Ok(())
        })
    }

    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'static, Vec<RouteSubscriptionEvent>, Self::Error> {
        let events = self
            .events
            .drain()
            .into_iter()
            .map(|event| match event {
                GrpcRouteSubscriptionEvent::Message(message) => {
                    RouteSubscriptionEvent::Update(Box::new(message))
                }
                GrpcRouteSubscriptionEvent::ResponseStreamEnded => {
                    RouteSubscriptionEvent::StreamEnded
                }
                // Overflow discards queued events behind this barrier, and a
                // status or protocol failure leaves the session indeterminate.
                // Both must clear authority before callers retry.
                _ => RouteSubscriptionEvent::StreamClosed,
            })
            .collect();
        Box::pin(async move { Ok(events) })
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
            Self::SubscribeRequestStreamClosed => {
                f.write_str("proxy subscribe request stream is closed")
            }
            Self::SubscribeResponseStreamClosed => {
                f.write_str("proxy subscribe response stream closed before a route response")
            }
            Self::Protocol(error) => write!(f, "proxy control-plane protocol error: {error}"),
            Self::UnexpectedRouteResponse { request_id } => write!(
                f,
                "unexpected route response for request {}",
                request_id.as_str()
            ),
        }
    }
}

impl Error for GrpcProxyControlPlaneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Status(status) => Some(status),
            Self::Protocol(error) => Some(error),
            Self::SubscribeRequestStreamClosed
            | Self::SubscribeResponseStreamClosed
            | Self::UnexpectedRouteResponse { .. } => None,
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

fn response_request_id(message: &SubscribeControlPlaneOutput) -> Option<&RouteRequestId> {
    match message {
        SubscribeControlPlaneOutput::RouteResolved { request_id, .. }
        | SubscribeControlPlaneOutput::RouteMiss { request_id, .. } => Some(request_id),
        SubscribeControlPlaneOutput::RouteUpdated { .. }
        | SubscribeControlPlaneOutput::RouteInvalidated { .. } => None,
    }
}

async fn read_subscription_responses(
    mut responses: tonic::codec::Streaming<pb::ProxySubscribeResponse>,
    events: Arc<SubscriptionEvents>,
    pending: PendingRouteResponses,
    closed: Arc<AtomicBool>,
    requests: mpsc::Sender<pb::ProxySubscribeRequest>,
    session: u64,
) {
    loop {
        let failure = match responses.message().await {
            Ok(Some(response)) => {
                match proxy_subscribe_response_from_proto(response) {
                    Ok(mut message) => {
                        match &mut message {
                            SubscribeControlPlaneOutput::RouteResolved {
                                subscription_id, ..
                            }
                            | SubscribeControlPlaneOutput::RouteUpdated {
                                subscription_id, ..
                            }
                            | SubscribeControlPlaneOutput::RouteInvalidated {
                                subscription_id,
                                ..
                            } => {
                                *subscription_id = subscription_id.clone().with_session(session);
                            }
                            SubscribeControlPlaneOutput::RouteMiss { .. } => {}
                        }
                        if let Some(request_id) = response_request_id(&message).cloned() {
                            let response = pending.lock().await.remove(&request_id);
                            if let Some(response) = response {
                                // A cancelled request's eventual response is harmless;
                                // allow the stream reset/TTL cleanup policy to reclaim it.
                                if let Err(Ok(SubscribeControlPlaneOutput::RouteResolved {
                                    subscription_id,
                                    ..
                                })) = response.send(Ok(message))
                                {
                                    let unsubscribe = proxy_subscribe_input_to_proto(
                                        ProxySubscribeInput::Unsubscribe { subscription_id },
                                    );
                                    if requests.try_send(unsubscribe).is_err() {
                                        closed.store(true, Ordering::Release);
                                        events
                                            .push(GrpcRouteSubscriptionEvent::ResponseStreamClosed);
                                        fail_pending_route_responses(&pending, GrpcProxyControlPlaneError::SubscribeResponseStreamClosed).await;
                                        return;
                                    }
                                }
                                continue;
                            }
                            GrpcRouteSubscriptionEvent::UnexpectedRouteResponse { request_id }
                        } else if events.push(GrpcRouteSubscriptionEvent::Message(message)) {
                            continue;
                        } else {
                            closed.store(true, Ordering::Release);
                            fail_pending_route_responses(
                                &pending,
                                GrpcProxyControlPlaneError::SubscribeResponseStreamClosed,
                            )
                            .await;
                            return;
                        }
                    }
                    Err(error) => GrpcRouteSubscriptionEvent::Protocol(error),
                }
            }
            Ok(None) => GrpcRouteSubscriptionEvent::ResponseStreamEnded,
            Err(status) => GrpcRouteSubscriptionEvent::Status(status),
        };
        closed.store(true, Ordering::Release);
        let error = match &failure {
            GrpcRouteSubscriptionEvent::Protocol(error) => {
                GrpcProxyControlPlaneError::Protocol(error.clone())
            }
            GrpcRouteSubscriptionEvent::UnexpectedRouteResponse { request_id } => {
                GrpcProxyControlPlaneError::UnexpectedRouteResponse {
                    request_id: request_id.clone(),
                }
            }
            GrpcRouteSubscriptionEvent::Status(status) => {
                GrpcProxyControlPlaneError::Status(status.clone())
            }
            _ => GrpcProxyControlPlaneError::SubscribeResponseStreamClosed,
        };
        events.push(failure);
        fail_pending_route_responses(&pending, error).await;
        return;
    }
}
async fn fail_pending_route_responses(
    pending: &PendingRouteResponses,
    error: GrpcProxyControlPlaneError,
) {
    for response in std::mem::take(&mut *pending.lock().await).into_values() {
        let _ = response.send(Err(error.clone()));
    }
}
#[cfg(test)]
mod tests;
