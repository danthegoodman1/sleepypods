use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc, time::Instant};

use proxy_core::observability::{
    metrics::{
        RUNTIME_CONTROL_PLANE_CALLS_TOTAL, RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL,
        RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL,
    },
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityEvent, ObservabilityRecorder,
        EVENT_ROUTE_CACHE_LOOKUP, EVENT_SUBSCRIBE_STREAM,
    },
    Operation, Outcome,
};
use sleepypods_api::RouteIdentity;

use crate::{
    matcher::rank_match, ApplyControlPlaneMessageOutcome, CacheInsertResult, CacheLookup,
    CacheLookupHit, InvalidationReason, NegativeCacheEntry, PositiveCacheEntry, RouteRequestId,
    SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState,
};

pub type RouteSubscriptionFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub trait RouteSubscriptionClient {
    type Error: Send;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error>;

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error>;

    /// Discard a session when deferred cleanup cannot be delivered within bounds.
    fn reset_subscription(&mut self) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'static, Vec<RouteSubscriptionEvent>, Self::Error> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteSubscriptionEvent {
    Update(Box<SubscribeControlPlaneOutput>),
    /// The server ended the response stream in order, after delivering
    /// everything it had queued. Subscriptions are gone, but no invalidation
    /// was dropped, so cached answers stay usable until their own TTL.
    StreamEnded,
    /// The session failed or dropped events. A lost invalidation can name any
    /// cached identity, so cached authority goes with it.
    StreamClosed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteResolution {
    Resolved(Arc<PositiveCacheEntry>),
    Miss(Arc<NegativeCacheEntry>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteResolverError<ClientError> {
    Subscribe(ClientError),
    Unsubscribe {
        subscription_id: SubscriptionId,
        source: ClientError,
    },
    Protocol(RouteResolverProtocolError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteResolverProtocolError {
    MismatchedRequestId {
        expected: RouteRequestId,
        actual: RouteRequestId,
    },
    MismatchedMissIdentity {
        expected: RouteIdentity,
        actual: RouteIdentity,
    },
    MismatchedResolvedIdentity {
        requested: RouteIdentity,
        matched: RouteIdentity,
    },
    UnexpectedSubscribeResponse {
        kind: UnexpectedSubscribeResponseKind,
    },
    ResponseDidNotInstallUsableCacheEntry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnexpectedSubscribeResponseKind {
    RouteUpdated,
    RouteInvalidated,
}

#[derive(Clone, Debug)]
pub struct FrontlineRouteResolver<Client> {
    state: SubscriptionState,
    client: Client,
    next_request_id: u64,
    observability: ObservabilityRecorder,
}

impl<Client> PartialEq for FrontlineRouteResolver<Client>
where
    Client: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
            && self.client == other.client
            && self.next_request_id == other.next_request_id
    }
}

impl<Client> Eq for FrontlineRouteResolver<Client> where Client: Eq {}

impl<Client> FrontlineRouteResolver<Client> {
    pub(crate) fn into_parts(self) -> (SubscriptionState, Client, ObservabilityRecorder) {
        (self.state, self.client, self.observability)
    }

    pub fn new(cache_capacity: usize, client: Client) -> Self {
        Self {
            state: SubscriptionState::new(cache_capacity),
            client,
            next_request_id: 0,
            observability: ObservabilityRecorder::default(),
        }
    }

    pub fn from_parts(state: SubscriptionState, client: Client) -> Self {
        Self::from_parts_with_observability(state, client, ObservabilityRecorder::default())
    }

    pub fn with_observability(
        cache_capacity: usize,
        client: Client,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            state: SubscriptionState::new(cache_capacity),
            client,
            next_request_id: 0,
            observability,
        }
    }

    pub fn from_parts_with_observability(
        state: SubscriptionState,
        client: Client,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            state,
            client,
            next_request_id: 0,
            observability,
        }
    }

    pub fn state(&self) -> &SubscriptionState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut SubscriptionState {
        &mut self.state
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    fn next_request_id(&mut self) -> RouteRequestId {
        self.next_request_id = self.next_request_id.saturating_add(1);
        RouteRequestId::new(format!("req:{}", self.next_request_id))
            .expect("resolver-generated request IDs are non-empty")
    }
}

impl<Client> FrontlineRouteResolver<Client>
where
    Client: RouteSubscriptionClient,
{
    pub async fn resolve(
        &mut self,
        identity: RouteIdentity,
        now: Instant,
    ) -> Result<FrontlineRouteResolution, FrontlineRouteResolverError<Client::Error>> {
        self.drain_subscription_events(now).await?;

        let lookup = self.state.cache().lookup(&identity, now);
        self.record_cache_lookup(&lookup);
        match lookup {
            CacheLookup::Hit(CacheLookupHit::Positive(entry)) => {
                return Ok(FrontlineRouteResolution::Resolved(entry));
            }
            CacheLookup::Hit(CacheLookupHit::Negative(entry)) => {
                return Ok(FrontlineRouteResolution::Miss(entry));
            }
            CacheLookup::Expired | CacheLookup::Absent => {}
        }

        let request_id = self.next_request_id();
        let message = match self
            .client
            .subscribe_route(request_id.clone(), identity.clone())
            .await
        {
            Ok(message) => {
                self.record_control_plane_call(Operation::SubscribeRoute, Outcome::Success);
                message
            }
            Err(error) => {
                self.record_control_plane_call(Operation::SubscribeRoute, Outcome::Error);
                return Err(FrontlineRouteResolverError::Subscribe(error));
            }
        };

        validate_subscribe_response(&request_id, &identity, &message)?;

        let outcome = self
            .state
            .apply_resolved_response(identity.clone(), message, now);
        self.unsubscribe_outcome(&outcome).await?;

        match self.state.cache().lookup(&identity, now) {
            CacheLookup::Hit(CacheLookupHit::Positive(entry)) => {
                Ok(FrontlineRouteResolution::Resolved(entry))
            }
            CacheLookup::Hit(CacheLookupHit::Negative(entry)) => {
                Ok(FrontlineRouteResolution::Miss(entry))
            }
            CacheLookup::Expired | CacheLookup::Absent => {
                Err(FrontlineRouteResolverError::Protocol(
                    RouteResolverProtocolError::ResponseDidNotInstallUsableCacheEntry,
                ))
            }
        }
    }

    pub async fn apply_control_plane_message(
        &mut self,
        message: SubscribeControlPlaneOutput,
        now: Instant,
    ) -> Result<ApplyControlPlaneMessageOutcome, FrontlineRouteResolverError<Client::Error>> {
        let outcome = self.state.apply_control_plane_message(message, now);
        self.unsubscribe_outcome(&outcome).await?;
        Ok(outcome)
    }

    pub async fn maintain(
        &mut self,
        now: Instant,
    ) -> Result<(), FrontlineRouteResolverError<Client::Error>> {
        self.drain_subscription_events(now).await?;
        let expired = self.state.cache_mut().expire(now);
        self.unsubscribe_all(expired).await;
        Ok(())
    }

    async fn drain_subscription_events(
        &mut self,
        now: Instant,
    ) -> Result<(), FrontlineRouteResolverError<Client::Error>> {
        let events = self
            .client
            .drain_subscription_events()
            .await
            .map_err(FrontlineRouteResolverError::Subscribe)?;

        for event in events {
            match event {
                RouteSubscriptionEvent::Update(message) => {
                    record_subscribe_message(&self.observability, &message);
                    let outcome = self.state.apply_control_plane_message(*message, now);
                    self.unsubscribe_outcome(&outcome).await?;
                }
                RouteSubscriptionEvent::StreamEnded | RouteSubscriptionEvent::StreamClosed => {
                    record_subscribe_stream_closed(&self.observability);
                    self.state
                        .invalidate_active_subscriptions(InvalidationReason::StreamClosed, now);
                }
            }
        }

        Ok(())
    }

    async fn unsubscribe_outcome(
        &mut self,
        outcome: &ApplyControlPlaneMessageOutcome,
    ) -> Result<(), FrontlineRouteResolverError<Client::Error>> {
        match outcome {
            ApplyControlPlaneMessageOutcome::Resolved(result)
            | ApplyControlPlaneMessageOutcome::Miss(result) => {
                self.unsubscribe_all(result.clone()).await;
                Ok(())
            }
            ApplyControlPlaneMessageOutcome::Updated(crate::ApplyUpdateOutcome::Replaced(
                result,
            )) => {
                self.unsubscribe_all(result.clone()).await;
                Ok(())
            }
            ApplyControlPlaneMessageOutcome::Updated(
                crate::ApplyUpdateOutcome::MissingSubscription
                | crate::ApplyUpdateOutcome::StaleInstanceGeneration { .. }
                | crate::ApplyUpdateOutcome::StaleBackendGeneration { .. },
            )
            | ApplyControlPlaneMessageOutcome::Invalidated { .. } => Ok(()),
        }
    }

    async fn unsubscribe_all(&mut self, result: CacheInsertResult) {
        for subscription_id in result.subscriptions_to_unsubscribe {
            let outcome = self
                .client
                .unsubscribe(subscription_id.clone())
                .await
                .map(|()| {
                    self.record_control_plane_call(Operation::Unsubscribe, Outcome::Success);
                })
                .map_err(|_source| {
                    self.record_control_plane_call(Operation::Unsubscribe, Outcome::Error);
                });
            let _ = outcome;
        }
    }

    fn record_cache_lookup(&self, lookup: &CacheLookup) {
        record_cache_lookup(&self.observability, lookup);
    }

    fn record_control_plane_call(&self, operation: Operation, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![operation.metric_label(), outcome.metric_label()],
            1.0,
        ));
    }
}

pub(crate) fn record_cache_lookup(observability: &ObservabilityRecorder, lookup: &CacheLookup) {
    observability.record_lazy(|| {
        let outcome = match lookup {
            CacheLookup::Hit(_) => Outcome::Hit,
            CacheLookup::Expired | CacheLookup::Absent => Outcome::Miss,
        };
        ObservabilityEvent::Metric(MetricObservation::new(
            RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL,
            vec![outcome.metric_label()],
            1.0,
        ))
    });
    observability.record_lazy(|| {
        let fields = match lookup {
            CacheLookup::Hit(CacheLookupHit::Positive(entry)) => {
                let mut fields = Vec::with_capacity(4);
                if let Some(id) = &entry.subscription_id {
                    fields.push(LogField::subscription_id(id.as_str()));
                }
                fields.push(LogField::route_id(entry.entry.route_binding_id.as_str()));
                fields.push(LogField::instance_id(entry.entry.instance_id.as_str()));
                fields.push(LogField::generation(entry.entry.instance_generation.get()));
                fields
            }
            _ => Vec::new(),
        };
        ObservabilityEvent::Log(LifecycleLogEvent::new(EVENT_ROUTE_CACHE_LOOKUP, fields))
    });
}

pub(crate) fn record_subscribe_message(
    observability: &ObservabilityRecorder,
    message: &SubscribeControlPlaneOutput,
) {
    let (outcome, fields) = match message {
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id,
            entry,
            ..
        } => (
            Outcome::Updated,
            vec![
                LogField::subscription_id(subscription_id.as_str()),
                LogField::route_id(entry.route_binding_id.as_str()),
                LogField::instance_id(entry.instance_id.as_str()),
                LogField::generation(entry.instance_generation.get()),
            ],
        ),
        SubscribeControlPlaneOutput::RouteInvalidated {
            subscription_id, ..
        } => (
            Outcome::Invalidated,
            vec![LogField::subscription_id(subscription_id.as_str())],
        ),
        SubscribeControlPlaneOutput::RouteResolved { .. }
        | SubscribeControlPlaneOutput::RouteMiss { .. } => return,
    };
    observability.record_metric(MetricObservation::new(
        RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL,
        vec![outcome.metric_label()],
        1.0,
    ));
    observability.record_log(LifecycleLogEvent::new(EVENT_SUBSCRIBE_STREAM, fields));
}

pub(crate) fn record_subscribe_stream_closed(observability: &ObservabilityRecorder) {
    observability.record_metric(MetricObservation::new(
        RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL,
        vec![Outcome::Closed.metric_label()],
        1.0,
    ));
    observability.record_log(LifecycleLogEvent::new(
        EVENT_SUBSCRIBE_STREAM,
        vec![LogField::error_reason("response_stream_closed")],
    ));
}

pub(crate) fn validate_subscribe_response<E>(
    request_id: &RouteRequestId,
    identity: &RouteIdentity,
    message: &SubscribeControlPlaneOutput,
) -> Result<(), FrontlineRouteResolverError<E>> {
    match message {
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: actual, ..
        }
        | SubscribeControlPlaneOutput::RouteMiss {
            request_id: actual, ..
        } => {
            if actual != request_id {
                return Err(FrontlineRouteResolverError::Protocol(
                    RouteResolverProtocolError::MismatchedRequestId {
                        expected: request_id.clone(),
                        actual: actual.clone(),
                    },
                ));
            }
        }
        SubscribeControlPlaneOutput::RouteUpdated { .. } => {
            return Err(FrontlineRouteResolverError::Protocol(
                RouteResolverProtocolError::UnexpectedSubscribeResponse {
                    kind: UnexpectedSubscribeResponseKind::RouteUpdated,
                },
            ));
        }
        SubscribeControlPlaneOutput::RouteInvalidated { .. } => {
            return Err(FrontlineRouteResolverError::Protocol(
                RouteResolverProtocolError::UnexpectedSubscribeResponse {
                    kind: UnexpectedSubscribeResponseKind::RouteInvalidated,
                },
            ));
        }
    }

    match message {
        SubscribeControlPlaneOutput::RouteResolved {
            matched_identity, ..
        } => {
            if rank_match(identity, matched_identity).is_none() {
                return Err(FrontlineRouteResolverError::Protocol(
                    RouteResolverProtocolError::MismatchedResolvedIdentity {
                        requested: identity.clone(),
                        matched: matched_identity.clone(),
                    },
                ));
            }
        }
        SubscribeControlPlaneOutput::RouteMiss {
            request_identity, ..
        } => {
            if request_identity != identity {
                return Err(FrontlineRouteResolverError::Protocol(
                    RouteResolverProtocolError::MismatchedMissIdentity {
                        expected: identity.clone(),
                        actual: request_identity.clone(),
                    },
                ));
            }
        }
        SubscribeControlPlaneOutput::RouteUpdated { .. }
        | SubscribeControlPlaneOutput::RouteInvalidated { .. } => {
            unreachable!("unexpected subscribe responses are rejected before validation")
        }
    }

    Ok(())
}

impl<ClientError> fmt::Display for FrontlineRouteResolverError<ClientError>
where
    ClientError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Subscribe(error) => write!(f, "subscribe route failed: {error}"),
            Self::Unsubscribe {
                subscription_id,
                source,
            } => write!(
                f,
                "unsubscribe {} failed: {source}",
                subscription_id.as_str()
            ),
            Self::Protocol(error) => write!(f, "{error}"),
        }
    }
}

impl<ClientError> Error for FrontlineRouteResolverError<ClientError> where
    ClientError: Error + 'static
{
}

impl fmt::Display for RouteResolverProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MismatchedRequestId { expected, actual } => write!(
                f,
                "subscribe response request_id {} did not match {}",
                actual.as_str(),
                expected.as_str()
            ),
            Self::MismatchedMissIdentity { .. } => {
                write!(
                    f,
                    "route miss response identity did not match request identity"
                )
            }
            Self::MismatchedResolvedIdentity { .. } => {
                write!(
                    f,
                    "route resolved response identity did not match request identity"
                )
            }
            Self::UnexpectedSubscribeResponse { kind } => {
                write!(f, "unexpected direct subscribe response: {kind:?}")
            }
            Self::ResponseDidNotInstallUsableCacheEntry => {
                write!(f, "subscribe response did not install a usable cache entry")
            }
        }
    }
}

impl Error for RouteResolverProtocolError {}

#[cfg(test)]
mod tests;
