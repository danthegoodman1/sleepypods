use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use proxy_core::observability::{
    metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
    recorder::{MetricObservation, ObservabilityRecorder},
    Operation, Outcome,
};
use sleepypods_api::{CachePolicy, InstanceState, RouteEntry, RouteIdentity};

#[cfg(test)]
use crate::FrontlineRouteResolution;

use crate::{
    route_wake_decision, validate_wake_response, ApplyUpdateOutcome, FrontlineRouteResolver,
    FrontlineRouteResolverError, NegativeCacheEntry, ReadyBackend, RouteSubscriptionClient,
    StaleWakeObservation, SubscribeControlPlaneOutput, SubscriptionState, WakeAdmission,
    WakeInstanceRequest, WakeInstanceResponse, WakeResponseDisposition, WakeTracker,
    WakeUnavailable, WakeWait,
};

pub type WakeClientFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;
const DEFAULT_WAKE_INSTANCE_DEADLINE: Duration = Duration::from_secs(5);
const ROUTE_ACTOR_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(10);

pub trait WakeClient {
    type Error: Send;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error>;
}

#[derive(Clone, Debug)]
pub struct FrontlineRouteCoordinator<RouteClient, Wake> {
    resolver: FrontlineRouteResolver<RouteClient>,
    wake_tracker: WakeTracker,
    wake_client: Wake,
    wake_deadline: Duration,
    route_deadline: Duration,
    observability: ObservabilityRecorder,
}

#[derive(Debug)]
pub struct SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    state: Arc<tokio::sync::RwLock<SubscriptionState>>,
    commands: tokio::sync::mpsc::Sender<RouteActorCommand<RouteClient::Error, Wake::Error>>,
    flights: RouteFlights<RouteClient::Error, Wake::Error>,
    _task: Arc<RouteActorTask>,
    waiters: Arc<tokio::sync::Semaphore>,
    route_deadline: Duration,
    observability: ObservabilityRecorder,
}

const MAX_ROUTE_FLIGHTS: usize = 64;
// Re-registration is background repair of answers that are already serving, so
// it may never consume the budget a first-time miss needs to reach the control
// plane. Half the flights stay reserved for demand.
const MAX_REREGISTRATION_FLIGHTS: usize = MAX_ROUTE_FLIGHTS / 2;
const MAX_ROUTE_WAITERS: usize = 256;
const SUBSCRIBE_DEADLINE: Duration = Duration::from_secs(5);
#[derive(Debug)]
struct RouteActorTask(tokio::task::JoinHandle<()>);
impl Drop for RouteActorTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<RouteClient, Wake> Clone for SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            commands: self.commands.clone(),
            flights: self.flights.clone(),
            _task: self._task.clone(),
            waiters: self.waiters.clone(),
            route_deadline: self.route_deadline,
            observability: self.observability.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteOutcome {
    Ready(ReadyBackend),
    Miss(Arc<NegativeCacheEntry>),
    Waiting(WakeWait),
    Waking {
        instance_id: sleepypods_api::InstanceId,
        generation: sleepypods_api::Generation,
    },
    Unavailable(WakeUnavailable),
    WakeFailed {
        instance_id: sleepypods_api::InstanceId,
        generation: sleepypods_api::Generation,
        reason: String,
    },
    WakeUnavailable {
        instance_id: sleepypods_api::InstanceId,
        generation: sleepypods_api::Generation,
        reason: String,
    },
    GenerationConflict {
        instance_id: sleepypods_api::InstanceId,
        expected_generation: sleepypods_api::Generation,
        actual_generation: sleepypods_api::Generation,
    },
    RejectedWakeObservation(StaleWakeObservation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteCoordinatorError<RouteClientError, WakeClientError> {
    Resolve(FrontlineRouteResolverError<RouteClientError>),
    CacheUpdate(FrontlineRouteResolverError<RouteClientError>),
    Wake(WakeClientError),
    WakeDeadline(WakeInstanceRequest),
    RouteActorClosed,
    Saturated,
    SubscribeDeadline,
    RouteDeadline,
    InvalidatedDuringResolution,
    RejectedCacheUpdate(ApplyUpdateOutcome),
}

type RouteResult<RouteClientError, WakeClientError> = Result<
    FrontlineRouteOutcome,
    FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>,
>;
type RouteFlights<RouteClientError, WakeClientError> = Arc<
    tokio::sync::Mutex<HashMap<RouteIdentity, Arc<RouteFlight<RouteClientError, WakeClientError>>>>,
>;

#[derive(Debug)]
struct RouteFlight<RouteClientError, WakeClientError> {
    result: tokio::sync::Mutex<Option<RouteResult<RouteClientError, WakeClientError>>>,
    notify: tokio::sync::Notify,
}

#[derive(Debug)]
enum RouteActorCommand<RouteClientError, WakeClientError> {
    Route {
        identity: RouteIdentity,
        now: Instant,
        refresh: bool,
        response: tokio::sync::oneshot::Sender<RouteResult<RouteClientError, WakeClientError>>,
    },
}

impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake> {
    pub fn new(
        resolver: FrontlineRouteResolver<RouteClient>,
        wake_tracker: WakeTracker,
        wake_client: Wake,
    ) -> Self {
        Self {
            resolver,
            wake_tracker,
            wake_client,
            wake_deadline: DEFAULT_WAKE_INSTANCE_DEADLINE,
            route_deadline: Duration::from_secs(130),
            observability: ObservabilityRecorder::default(),
        }
    }

    pub fn with_observability(
        resolver: FrontlineRouteResolver<RouteClient>,
        wake_tracker: WakeTracker,
        wake_client: Wake,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            resolver,
            wake_tracker,
            wake_client,
            wake_deadline: DEFAULT_WAKE_INSTANCE_DEADLINE,
            route_deadline: Duration::from_secs(130),
            observability,
        }
    }

    pub fn with_wake_deadline(mut self, wake_deadline: Duration) -> Self {
        self.wake_deadline = wake_deadline;
        self
    }

    pub(crate) fn route_deadline(&self) -> Duration {
        self.route_deadline
    }

    pub fn with_route_deadline(mut self, deadline: Duration) -> Self {
        self.route_deadline = deadline;
        self
    }

    pub fn resolver(&self) -> &FrontlineRouteResolver<RouteClient> {
        &self.resolver
    }

    pub fn resolver_mut(&mut self) -> &mut FrontlineRouteResolver<RouteClient> {
        &mut self.resolver
    }

    pub fn wake_tracker(&self) -> &WakeTracker {
        &self.wake_tracker
    }

    pub fn wake_tracker_mut(&mut self) -> &mut WakeTracker {
        &mut self.wake_tracker
    }

    pub fn wake_client(&self) -> &Wake {
        &self.wake_client
    }

    pub fn wake_client_mut(&mut self) -> &mut Wake {
        &mut self.wake_client
    }
}

impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + 'static,
{
    pub fn into_shared(self) -> SharedFrontlineRouteCoordinator<RouteClient, Wake> {
        SharedFrontlineRouteCoordinator::new(self)
    }
}

impl<RouteClient, Wake> SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + 'static,
{
    pub fn new(coordinator: FrontlineRouteCoordinator<RouteClient, Wake>) -> Self {
        let FrontlineRouteCoordinator {
            resolver,
            wake_client,
            wake_deadline,
            route_deadline,
            observability,
            ..
        } = coordinator;
        let (initial_state, client, _) = resolver.into_parts();
        let state = Arc::new(tokio::sync::RwLock::new(initial_state));
        let (commands, rx) = tokio::sync::mpsc::channel(64);
        let task_state = state.clone();
        let task = tokio::spawn(route_actor(
            client,
            wake_client,
            wake_deadline,
            observability.clone(),
            task_state,
            rx,
        ));

        Self {
            state,
            commands,
            flights: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            _task: Arc::new(RouteActorTask(task)),
            waiters: Arc::new(tokio::sync::Semaphore::new(MAX_ROUTE_WAITERS)),
            route_deadline,
            observability,
        }
    }

    pub async fn route(
        &self,
        identity: RouteIdentity,
        now: Instant,
    ) -> RouteResult<RouteClient::Error, Wake::Error> {
        if self._task.0.is_finished() {
            return Err(FrontlineRouteCoordinatorError::RouteActorClosed);
        }
        if let Some(outcome) = self.local_route(&identity, now).await {
            return Ok(outcome);
        }

        let _waiter = self
            .waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| FrontlineRouteCoordinatorError::Saturated)?;
        let flight = self.flight_for(identity.clone(), now).await?;
        flight.wait().await
    }

    async fn local_route(
        &self,
        identity: &RouteIdentity,
        now: Instant,
    ) -> Option<FrontlineRouteOutcome> {
        let state = self.state.read().await;
        let lookup = state.cache().lookup(identity, now);
        crate::resolver::record_cache_lookup(&self.observability, &lookup);
        match lookup {
            crate::CacheLookup::Hit(crate::CacheLookupHit::Positive(entry)) => {
                match route_wake_decision(&entry.entry) {
                    crate::RouteWakeDecision::Ready(backend) => {
                        Some(FrontlineRouteOutcome::Ready(backend))
                    }
                    crate::RouteWakeDecision::Unavailable(unavailable) => {
                        Some(FrontlineRouteOutcome::Unavailable(unavailable))
                    }
                    crate::RouteWakeDecision::Wait(wait) => {
                        let _ = wait;
                        None
                    }
                    crate::RouteWakeDecision::Wake { .. } => None,
                }
            }
            crate::CacheLookup::Hit(crate::CacheLookupHit::Negative(entry)) => {
                Some(FrontlineRouteOutcome::Miss(entry))
            }
            crate::CacheLookup::Expired | crate::CacheLookup::Absent => None,
        }
    }

    async fn flight_for(
        &self,
        identity: RouteIdentity,
        now: Instant,
    ) -> Result<
        Arc<RouteFlight<RouteClient::Error, Wake::Error>>,
        FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>,
    > {
        let mut flights = self.flights.lock().await;
        if let Some(flight) = flights.get(&identity) {
            return Ok(flight.clone());
        }

        if flights.len() >= MAX_ROUTE_FLIGHTS {
            return Err(FrontlineRouteCoordinatorError::Saturated);
        }
        let flight = Arc::new(RouteFlight::new());
        flights.insert(identity.clone(), flight.clone());
        spawn_route_flight(
            identity,
            now,
            flight.clone(),
            self.commands.clone(),
            self.flights.clone(),
            self.route_deadline,
        );
        Ok(flight)
    }
}

impl<RouteClientError, WakeClientError> RouteFlight<RouteClientError, WakeClientError>
where
    RouteClientError: Clone,
    WakeClientError: Clone,
{
    fn new() -> Self {
        Self {
            result: tokio::sync::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    async fn complete(&self, result: RouteResult<RouteClientError, WakeClientError>) {
        *self.result.lock().await = Some(result);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> RouteResult<RouteClientError, WakeClientError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(result) = self.result.lock().await.clone() {
                return result;
            }
            notified.await;
        }
    }
}

fn spawn_route_flight<RouteClientError, WakeClientError>(
    identity: RouteIdentity,
    now: Instant,
    flight: Arc<RouteFlight<RouteClientError, WakeClientError>>,
    commands: tokio::sync::mpsc::Sender<RouteActorCommand<RouteClientError, WakeClientError>>,
    flights: RouteFlights<RouteClientError, WakeClientError>,
    deadline: Duration,
) where
    RouteClientError: Clone + Send + 'static,
    WakeClientError: Clone + Send + 'static,
{
    tokio::spawn(async move {
        let work = async {
            let mut refresh = false;
            loop {
                let result = route_via_actor(
                    commands.clone(),
                    identity.clone(),
                    if refresh { Instant::now() } else { now },
                    refresh,
                )
                .await;
                match result {
                    Ok(
                        FrontlineRouteOutcome::Waking { .. }
                        | FrontlineRouteOutcome::Waiting(_)
                        | FrontlineRouteOutcome::GenerationConflict { .. },
                    )
                    | Err(FrontlineRouteCoordinatorError::InvalidatedDuringResolution) => {
                        refresh = true;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    other => break other,
                }
            }
        };
        let result = tokio::time::timeout(deadline, work)
            .await
            .unwrap_or(Err(FrontlineRouteCoordinatorError::RouteDeadline));
        flight.complete(result).await;
        flights.lock().await.remove(&identity);
    });
}

async fn route_via_actor<RouteClientError, WakeClientError>(
    commands: tokio::sync::mpsc::Sender<RouteActorCommand<RouteClientError, WakeClientError>>,
    identity: RouteIdentity,
    now: Instant,
    refresh: bool,
) -> RouteResult<RouteClientError, WakeClientError>
where
    RouteClientError: Send + 'static,
    WakeClientError: Send + 'static,
{
    let (response, result) = tokio::sync::oneshot::channel();
    if commands
        .send(RouteActorCommand::Route {
            identity,
            now,
            refresh,
            response,
        })
        .await
        .is_err()
    {
        return Err(FrontlineRouteCoordinatorError::RouteActorClosed);
    }

    result
        .await
        .unwrap_or(Err(FrontlineRouteCoordinatorError::RouteActorClosed))
}

// Event history is retained only while an operation could return an older
// observation. A finite budget makes an overloaded stream fail closed instead
// of allowing unbounded correlation state or silently forgetting invalidations.
const MAX_OBSERVED_SUBSCRIPTIONS: usize = 4096;

#[derive(Clone, Copy)]
struct RouteObservation {
    id: u64,
    stream_epoch: u64,
    sequence: u64,
}

#[derive(Default)]
struct RouteObservations {
    stream_epoch: u64,
    /// Observations begun before this epoch crossed a session that dropped
    /// events. An orderly rotation advances `stream_epoch` without raising it.
    lossy_from: u64,
    sequence: u64,
    next_id: u64,
    active: HashMap<u64, u64>,
    subscriptions: HashMap<crate::SubscriptionId, u64>,
}

impl RouteObservations {
    fn begin(&mut self) -> RouteObservation {
        self.next_id += 1;
        self.active.insert(self.next_id, self.sequence);
        RouteObservation {
            id: self.next_id,
            stream_epoch: self.stream_epoch,
            sequence: self.sequence,
        }
    }

    /// The stream ended in order. Replies still in flight belong to a stream
    /// this coordinator no longer tracks, and the IDs it issued are dead, but
    /// the session delivered everything it had.
    fn rotate(&mut self) {
        self.stream_epoch += 1;
        self.active.clear();
        self.subscriptions.clear();
    }

    /// The session dropped events, so anything it still owes is unreliable.
    fn reset(&mut self) {
        self.rotate();
        self.lossy_from = self.stream_epoch;
    }

    /// Whether an observation begun at `epoch` has since crossed a session that
    /// dropped events.
    fn crossed_lossy(&self, epoch: u64) -> bool {
        epoch < self.lossy_from
    }

    // False means the bounded history cannot safely correlate another event.
    fn record(&mut self, subscription_id: &crate::SubscriptionId) -> bool {
        if self.active.is_empty() {
            return true;
        }
        if self.subscriptions.len() == MAX_OBSERVED_SUBSCRIPTIONS
            && !self.subscriptions.contains_key(subscription_id)
        {
            return false;
        }
        self.sequence += 1;
        self.subscriptions
            .insert(subscription_id.clone(), self.sequence);
        true
    }

    fn finish(
        &mut self,
        observation: RouteObservation,
        subscription_id: Option<&crate::SubscriptionId>,
    ) -> bool {
        let valid = observation.stream_epoch == self.stream_epoch
            && !subscription_id.is_some_and(|id| {
                self.subscriptions
                    .get(id)
                    .is_some_and(|sequence| *sequence > observation.sequence)
            });
        self.active.remove(&observation.id);
        let oldest = self.active.values().copied().min().unwrap_or(self.sequence);
        self.subscriptions.retain(|_, sequence| *sequence > oldest);
        valid
    }
}

enum PendingRoute<R, W> {
    /// Re-registering a retained answer after its stream closed. No caller is
    /// waiting, so a closed response channel must not be read as "abandoned".
    Reregister {
        identity: RouteIdentity,
        request_id: crate::RouteRequestId,
        observation: RouteObservation,
        result: Result<SubscribeControlPlaneOutput, FrontlineRouteCoordinatorError<R, W>>,
    },
    Subscribe {
        identity: RouteIdentity,
        request_id: crate::RouteRequestId,
        observation: RouteObservation,
        response: tokio::sync::oneshot::Sender<RouteResult<R, W>>,
        result: Result<SubscribeControlPlaneOutput, FrontlineRouteCoordinatorError<R, W>>,
    },
    Wake {
        entry: Arc<crate::PositiveCacheEntry>,
        request: WakeInstanceRequest,
        observation: RouteObservation,
        response: tokio::sync::oneshot::Sender<RouteResult<R, W>>,
        result: Result<WakeInstanceResponse, FrontlineRouteCoordinatorError<R, W>>,
    },
}

async fn route_actor<RouteClient, Wake>(
    mut client: RouteClient,
    mut wake_client: Wake,
    wake_deadline: Duration,
    observability: ObservabilityRecorder,
    state: Arc<tokio::sync::RwLock<SubscriptionState>>,
    mut rx: tokio::sync::mpsc::Receiver<RouteActorCommand<RouteClient::Error, Wake::Error>>,
) where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send + 'static,
{
    let mut maintenance = tokio::time::interval(ROUTE_ACTOR_MAINTENANCE_INTERVAL);
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tasks = tokio::task::JoinSet::new();
    let mut unsubscribes = tokio::task::JoinSet::new();
    let mut tracker = WakeTracker::new();
    let mut observations = RouteObservations::default();
    let mut next_request = 0u64;
    let mut reregister: std::collections::VecDeque<RouteIdentity> =
        std::collections::VecDeque::new();
    loop {
        drain_events(
            &mut client,
            &state,
            &mut observations,
            &mut unsubscribes,
            &observability,
            &mut reregister,
        )
        .await;
        // The read fast path shares this lock, so only take it for writing when
        // something is actually due.
        let now = Instant::now();
        let evicted = if state.read().await.cache().has_expired(now) {
            state
                .write()
                .await
                .cache_mut()
                .expire_limited(now, MAX_ROUTE_FLIGHTS.saturating_sub(unsubscribes.len()))
                .subscriptions_to_unsubscribe
        } else {
            Vec::new()
        };
        defer_unsubscribes(
            evicted,
            &mut client,
            &state,
            &mut observations,
            &mut unsubscribes,
        )
        .await;

        // Retained answers keep serving while this runs, so re-registration is
        // background work: it yields the flight budget to live requests.
        while tasks.len() < MAX_REREGISTRATION_FLIGHTS {
            let Some(identity) = reregister.pop_front() else {
                break;
            };
            if !state
                .read()
                .await
                .cache()
                .needs_reregistration(&identity, Instant::now())
            {
                continue;
            }
            next_request += 1;
            let request_id =
                crate::RouteRequestId::new(format!("req:{next_request}")).expect("request ID");
            let future = client.subscribe_route(request_id.clone(), identity.clone());
            let observation = observations.begin();
            spawn_subscribe(&mut tasks, future, move |result| PendingRoute::Reregister {
                identity,
                request_id,
                observation,
                result,
            });
        }

        tokio::select! {
            biased;
            _ = maintenance.tick() => {},
            Some(completed) = tasks.join_next(), if !tasks.is_empty() => {
                let Ok(completed) = completed else {
                    state.write().await.cache_mut().clear();
                    return;
                };
                // The reader can enqueue an event immediately after delivering
                // a reply. Consume that event before installing the observation.
                drain_events(
                    &mut client, &state, &mut observations, &mut unsubscribes, &observability,
                    &mut reregister,
                ).await;
                match completed {
                    PendingRoute::Reregister { identity, request_id, observation, result } => {
                        let subscription_id = result.as_ref().ok().and_then(resolved_subscription_id).cloned();
                        let same_stream = observation.stream_epoch == observations.stream_epoch;
                        let crossed_lossy = observations.crossed_lossy(observation.stream_epoch);
                        let valid = observations.finish(observation, subscription_id.as_ref());
                        let validation = result.as_ref().ok().map(|message| {
                            crate::resolver::validate_subscribe_response::<RouteClient::Error>(&request_id, &identity, message)
                        });
                        // A reply that crossed another rotation belongs to a
                        // stream this cache no longer tracks. Reclaim it the
                        // same way a crossed Subscribe reply is reclaimed.
                        if !valid || validation.as_ref().is_some_and(Result::is_err) {
                            reclaim_crossed_reply(
                                subscription_id, same_stream, crossed_lossy, &mut client, &state,
                                &mut observations, &mut unsubscribes,
                            ).await;
                            continue;
                        }
                        let Ok(message) = result else { continue };
                        let mut removed = Vec::new();
                        append_unsubscribes(
                            state.write().await.apply_resolved_response(identity, message, Instant::now()),
                            &mut removed,
                        );
                        defer_unsubscribes(
                            removed, &mut client, &state, &mut observations, &mut unsubscribes,
                        ).await;
                    }
                    PendingRoute::Subscribe { identity, request_id, observation, response, result } => {
                        observability.record_metric(MetricObservation::new(
                            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
                            vec![Operation::SubscribeRoute.metric_label(),
                                if result.is_ok() { Outcome::Success } else { Outcome::Error }.metric_label()],
                            1.0,
                        ));
                        let subscription_id = result.as_ref().ok().and_then(resolved_subscription_id).cloned();
                        let same_stream = observation.stream_epoch == observations.stream_epoch;
                        let crossed_lossy = observations.crossed_lossy(observation.stream_epoch);
                        let valid = observations.finish(observation, subscription_id.as_ref());
                        let validation = result.as_ref().ok().map(|message| {
                            crate::resolver::validate_subscribe_response(&request_id, &identity, message)
                        });
                        if !valid || response.is_closed() || validation.as_ref().is_some_and(Result::is_err) {
                            reclaim_crossed_reply(
                                subscription_id, same_stream, crossed_lossy, &mut client, &state,
                                &mut observations, &mut unsubscribes,
                            ).await;
                            let error = match validation {
                                Some(Err(error)) => FrontlineRouteCoordinatorError::Resolve(error),
                                _ => FrontlineRouteCoordinatorError::InvalidatedDuringResolution,
                            };
                            let _ = response.send(Err(error));
                            continue;
                        }
                        let message = match result {
                            Ok(message) => message,
                            Err(error) => { let _ = response.send(Err(error)); continue; }
                        };
                        let mut removed = Vec::new();
                        append_unsubscribes(
                            state.write().await.apply_resolved_response(identity.clone(), message, Instant::now()),
                            &mut removed,
                        );
                        defer_unsubscribes(
                            removed, &mut client, &state, &mut observations, &mut unsubscribes,
                        ).await;
                        match state.read().await.cache().lookup(&identity, Instant::now()) {
                            crate::CacheLookup::Hit(crate::CacheLookupHit::Positive(entry)) => {
                                dispatch_entry(entry, response, &mut observations, &mut tracker, &mut wake_client, wake_deadline, &mut tasks);
                            }
                            crate::CacheLookup::Hit(crate::CacheLookupHit::Negative(entry)) => {
                                let _ = response.send(Ok(FrontlineRouteOutcome::Miss(entry)));
                            }
                            _ => { let _ = response.send(Err(FrontlineRouteCoordinatorError::InvalidatedDuringResolution)); }
                        }
                    }
                    PendingRoute::Wake { entry, request, observation, response, result } => {
                        tracker.complete(&request.instance_id, request.expected_generation);
                        if !observations.finish(observation, Some(&entry.subscription_id)) {
                            let _ = response.send(Err(FrontlineRouteCoordinatorError::InvalidatedDuringResolution));
                            continue;
                        }
                        let result = match result {
                            Ok(reply) => {
                                let disposition = validate_wake_response(&entry.entry, reply);
                                observability.record_metric(MetricObservation::new(
                                    RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
                                    vec![Operation::WakeInstance.metric_label(), wake_response_outcome(&disposition).metric_label()], 1.0,
                                ));
                                if let WakeResponseDisposition::Ready(backend) = &disposition {
                                    let now = Instant::now();
                                    state.write().await.apply_control_plane_message(SubscribeControlPlaneOutput::RouteUpdated {
                                        subscription_id: entry.subscription_id.clone(), matched_identity: entry.matched_identity.clone(),
                                        entry: ready_route_entry(&entry.entry, backend.clone()), cache_policy: CachePolicy::new(entry.expires_at().saturating_duration_since(now)),
                                    }, now);
                                }
                                Ok(wake_outcome(disposition))
                            }
                            Err(error) => Err(error),
                        };
                        let _ = response.send(result);
                    }
                }
            }
            command = rx.recv() => {
                let Some(RouteActorCommand::Route {identity, now, refresh, response}) = command else { break; };
                if response.is_closed() { continue; }
                if tasks.len() >= MAX_ROUTE_FLIGHTS {
                    let _ = response.send(Err(FrontlineRouteCoordinatorError::Saturated));
                    continue;
                }
                let cached = if refresh {
                    let removed = {
                        let mut state = state.write().await;
                        if let crate::CacheLookup::Hit(crate::CacheLookupHit::Positive(entry)) = state.cache().lookup(&identity, now) {
                            state.cache_mut().invalidate_subscription(&entry.subscription_id);
                            vec![entry.subscription_id.clone()]
                        } else { Vec::new() }
                    };
                    defer_unsubscribes(
                        removed, &mut client, &state, &mut observations, &mut unsubscribes,
                    ).await;
                    crate::CacheLookup::Absent
                } else { state.read().await.cache().lookup(&identity, now) };
                match cached {
                    crate::CacheLookup::Hit(crate::CacheLookupHit::Positive(entry)) => {
                        dispatch_entry(entry, response, &mut observations, &mut tracker, &mut wake_client, wake_deadline, &mut tasks);
                    }
                    crate::CacheLookup::Hit(crate::CacheLookupHit::Negative(entry)) => {
                        let _ = response.send(Ok(FrontlineRouteOutcome::Miss(entry)));
                    }
                    _ => {
                        next_request += 1;
                        let request_id = crate::RouteRequestId::new(format!("req:{next_request}")).expect("request ID");
                        let future = client.subscribe_route(request_id.clone(), identity.clone());
                        let observation = observations.begin();
                        spawn_subscribe(&mut tasks, future, move |result| {
                            PendingRoute::Subscribe {identity, request_id, observation, response, result}
                        });
                    }
                }
            }
        }
    }
}

fn resolved_subscription_id(
    message: &SubscribeControlPlaneOutput,
) -> Option<&crate::SubscriptionId> {
    match message {
        SubscribeControlPlaneOutput::RouteResolved {
            subscription_id, ..
        } => Some(subscription_id),
        _ => None,
    }
}

/// Both the demand path and re-registration issue the same call under the same
/// deadline; only the completion they produce differs.
fn spawn_subscribe<RouteClientError, WakeClientError, Finish>(
    tasks: &mut tokio::task::JoinSet<PendingRoute<RouteClientError, WakeClientError>>,
    future: crate::RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, RouteClientError>,
    finish: Finish,
) where
    RouteClientError: Send + 'static,
    WakeClientError: Send + 'static,
    Finish: FnOnce(
            Result<
                SubscribeControlPlaneOutput,
                FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>,
            >,
        ) -> PendingRoute<RouteClientError, WakeClientError>
        + Send
        + 'static,
{
    tasks.spawn(async move {
        let result = match tokio::time::timeout(SUBSCRIBE_DEADLINE, future).await {
            Ok(result) => result.map_err(|error| {
                FrontlineRouteCoordinatorError::Resolve(FrontlineRouteResolverError::Subscribe(
                    error,
                ))
            }),
            Err(_) => Err(FrontlineRouteCoordinatorError::SubscribeDeadline),
        };
        finish(result)
    });
}

/// Reclaim a reply this coordinator refuses to install. On the stream that
/// issued it, drop the subscription. Past an orderly rotation, discard it: its
/// ID died with its stream, so unsubscribing could target a reused ID on the
/// replacement stream, and the ended session left nothing undelivered. Past a
/// session that dropped events, ownership is ambiguous, because `ensure` may
/// have reconnected while the reply was in flight, so reset and give up cached
/// authority with it.
async fn reclaim_crossed_reply<Client: RouteSubscriptionClient>(
    subscription_id: Option<crate::SubscriptionId>,
    same_stream: bool,
    crossed_lossy: bool,
    client: &mut Client,
    state: &tokio::sync::RwLock<SubscriptionState>,
    observations: &mut RouteObservations,
    unsubscribes: &mut tokio::task::JoinSet<bool>,
) where
    Client::Error: 'static,
{
    if same_stream {
        defer_unsubscribes(
            subscription_id.into_iter().collect(),
            client,
            state,
            observations,
            unsubscribes,
        )
        .await;
    } else if crossed_lossy && subscription_id.is_some() {
        reset_route_session(client, state, observations, unsubscribes).await;
    }
}

async fn reset_route_session<Client: RouteSubscriptionClient>(
    client: &mut Client,
    state: &tokio::sync::RwLock<SubscriptionState>,
    observations: &mut RouteObservations,
    unsubscribes: &mut tokio::task::JoinSet<bool>,
) where
    Client::Error: 'static,
{
    // Reached only where events were demonstrably lost: an overflowed
    // correlation budget, a failed transport, or cleanup that could not be
    // delivered. A dropped invalidation may name anything, so cached authority
    // goes with the session. An orderly close loses nothing and rotates instead.
    observations.reset();
    state.write().await.cache_mut().clear();
    *unsubscribes = tokio::task::JoinSet::new();
    let reset = client.reset_subscription();
    unsubscribes.spawn(async move {
        matches!(
            tokio::time::timeout(SUBSCRIBE_DEADLINE, reset).await,
            Ok(Ok(()))
        )
    });
}

async fn defer_unsubscribes<Client: RouteSubscriptionClient>(
    ids: Vec<crate::SubscriptionId>,
    client: &mut Client,
    state: &tokio::sync::RwLock<SubscriptionState>,
    observations: &mut RouteObservations,
    unsubscribes: &mut tokio::task::JoinSet<bool>,
) where
    Client::Error: 'static,
{
    let mut failed = false;
    while let Some(result) = unsubscribes.try_join_next() {
        failed |= !matches!(result, Ok(true));
    }
    if failed || ids.len() + unsubscribes.len() > MAX_ROUTE_FLIGHTS {
        reset_route_session(client, state, observations, unsubscribes).await;
        return;
    }
    for id in ids {
        let unsubscribe = client.unsubscribe(id);
        unsubscribes.spawn(async move {
            matches!(
                tokio::time::timeout(SUBSCRIBE_DEADLINE, unsubscribe).await,
                Ok(Ok(()))
            )
        });
    }
}

async fn drain_events<Client: RouteSubscriptionClient>(
    client: &mut Client,
    state: &tokio::sync::RwLock<SubscriptionState>,
    observations: &mut RouteObservations,
    unsubscribes: &mut tokio::task::JoinSet<bool>,
    observability: &ObservabilityRecorder,
    rotated: &mut std::collections::VecDeque<RouteIdentity>,
) where
    Client::Error: 'static,
{
    let mut removed = Vec::new();
    let mut overflow = false;
    {
        let events = client.drain_subscription_events().await;
        let mut state = state.write().await;
        match events {
            Ok(events) => {
                for event in events {
                    record_subscription_event(observability, &event);
                    match event {
                        crate::RouteSubscriptionEvent::StreamEnded => {
                            observations.rotate();
                            rotated.extend(state.cache_mut().rotate_session());
                        }
                        crate::RouteSubscriptionEvent::StreamClosed => {
                            observations.reset();
                            state.cache_mut().clear();
                        }
                        crate::RouteSubscriptionEvent::Update(message) => {
                            match message.as_ref() {
                                SubscribeControlPlaneOutput::RouteUpdated {
                                    subscription_id,
                                    ..
                                }
                                | SubscribeControlPlaneOutput::RouteInvalidated {
                                    subscription_id,
                                    ..
                                } if !observations.record(subscription_id) => {
                                    overflow = true;
                                    break;
                                }
                                _ => {}
                            }
                            append_unsubscribes(
                                state.apply_control_plane_message(*message, Instant::now()),
                                &mut removed,
                            );
                        }
                    }
                }
            }
            Err(_) => {
                observations.reset();
                state.cache_mut().clear();
            }
        }
    }
    if overflow {
        reset_route_session(client, state, observations, unsubscribes).await;
    } else {
        defer_unsubscribes(removed, client, state, observations, unsubscribes).await;
    }
}

fn append_unsubscribes(
    outcome: crate::ApplyControlPlaneMessageOutcome,
    ids: &mut Vec<crate::SubscriptionId>,
) {
    match outcome {
        crate::ApplyControlPlaneMessageOutcome::Resolved(result)
        | crate::ApplyControlPlaneMessageOutcome::Miss(result)
        | crate::ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::Replaced(result)) => {
            ids.extend(result.subscriptions_to_unsubscribe)
        }
        _ => {}
    }
}
fn record_subscription_event(
    observability: &ObservabilityRecorder,
    event: &crate::RouteSubscriptionEvent,
) {
    match event {
        crate::RouteSubscriptionEvent::Update(message) => {
            crate::resolver::record_subscribe_message(observability, message)
        }
        crate::RouteSubscriptionEvent::StreamEnded
        | crate::RouteSubscriptionEvent::StreamClosed => {
            crate::resolver::record_subscribe_stream_closed(observability)
        }
    }
}
fn dispatch_entry<R: Send + 'static, Wake: WakeClient>(
    entry: Arc<crate::PositiveCacheEntry>,
    response: tokio::sync::oneshot::Sender<RouteResult<R, Wake::Error>>,
    observations: &mut RouteObservations,
    tracker: &mut WakeTracker,
    wake: &mut Wake,
    deadline: Duration,
    tasks: &mut tokio::task::JoinSet<PendingRoute<R, Wake::Error>>,
) where
    Wake::Error: 'static,
{
    if entry.entry.instance_state == InstanceState::Waking {
        let _ = response.send(Ok(FrontlineRouteOutcome::Waiting(WakeWait {
            instance_id: entry.entry.instance_id.clone(),
            generation: entry.entry.instance_generation,
            reason: crate::WakeWaitReason::AlreadyWaking,
        })));
        return;
    }
    let outcome = match route_wake_decision(&entry.entry) {
        crate::RouteWakeDecision::Ready(backend) => FrontlineRouteOutcome::Ready(backend),
        crate::RouteWakeDecision::Wait(wait) => FrontlineRouteOutcome::Waiting(wait),
        crate::RouteWakeDecision::Unavailable(unavailable) => {
            FrontlineRouteOutcome::Unavailable(unavailable)
        }
        crate::RouteWakeDecision::Wake { request, .. } => match tracker.admit(request) {
            WakeAdmission::Wait(wait) => FrontlineRouteOutcome::Waiting(wait),
            WakeAdmission::Start(request) => {
                let future = wake.wake_instance(request.clone());
                let observation = observations.begin();
                tasks.spawn(async move {
                    let result = match tokio::time::timeout(deadline, future).await {
                        Ok(result) => result.map_err(FrontlineRouteCoordinatorError::Wake),
                        Err(_) => Err(FrontlineRouteCoordinatorError::WakeDeadline(
                            request.clone(),
                        )),
                    };
                    PendingRoute::Wake {
                        entry,
                        request,
                        observation,
                        response,
                        result,
                    }
                });
                return;
            }
        },
    };
    let _ = response.send(Ok(outcome));
}
fn wake_outcome(disposition: WakeResponseDisposition) -> FrontlineRouteOutcome {
    match disposition {
        WakeResponseDisposition::Ready(backend) => FrontlineRouteOutcome::Ready(backend),
        WakeResponseDisposition::WakeStarted {
            instance_id,
            generation,
        }
        | WakeResponseDisposition::StillWaking {
            instance_id,
            generation,
        } => FrontlineRouteOutcome::Waking {
            instance_id,
            generation,
        },
        WakeResponseDisposition::Failed {
            instance_id,
            generation,
            reason,
        } => FrontlineRouteOutcome::WakeFailed {
            instance_id,
            generation,
            reason,
        },
        WakeResponseDisposition::Unavailable {
            instance_id,
            generation,
            reason,
        } => FrontlineRouteOutcome::WakeUnavailable {
            instance_id,
            generation,
            reason,
        },
        WakeResponseDisposition::GenerationConflict {
            instance_id,
            expected_generation,
            actual_generation,
        } => FrontlineRouteOutcome::GenerationConflict {
            instance_id,
            expected_generation,
            actual_generation,
        },
        WakeResponseDisposition::Rejected(stale) => {
            FrontlineRouteOutcome::RejectedWakeObservation(stale)
        }
    }
}

// Legacy synchronous helpers are confined to focused unit tests. Production
// HTTP, TLS and SNI all use the shared coordinator above.
#[cfg(test)]
impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    pub async fn route(
        &mut self,
        identity: RouteIdentity,
        now: Instant,
    ) -> Result<
        FrontlineRouteOutcome,
        FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>,
    > {
        let resolution = self
            .resolver
            .resolve(identity, now)
            .await
            .map_err(FrontlineRouteCoordinatorError::Resolve)?;

        let cache_entry = match resolution {
            FrontlineRouteResolution::Resolved(entry) => entry,
            FrontlineRouteResolution::Miss(entry) => return Ok(FrontlineRouteOutcome::Miss(entry)),
        };

        match route_wake_decision(&cache_entry.entry) {
            crate::RouteWakeDecision::Ready(backend) => Ok(FrontlineRouteOutcome::Ready(backend)),
            crate::RouteWakeDecision::Wait(wait) => Ok(FrontlineRouteOutcome::Waiting(wait)),
            crate::RouteWakeDecision::Unavailable(unavailable) => {
                Ok(FrontlineRouteOutcome::Unavailable(unavailable))
            }
            crate::RouteWakeDecision::Wake { request, .. } => {
                match self.wake_tracker.admit(request) {
                    WakeAdmission::Wait(wait) => Ok(FrontlineRouteOutcome::Waiting(wait)),
                    WakeAdmission::Start(request) => {
                        let response = match tokio::time::timeout(
                            self.wake_deadline,
                            self.wake_client.wake_instance(request.clone()),
                        )
                        .await
                        {
                            Err(_elapsed) => {
                                self.record_wake_instance_call(Outcome::Error);
                                self.wake_tracker
                                    .complete(&request.instance_id, request.expected_generation);
                                return Err(FrontlineRouteCoordinatorError::WakeDeadline(request));
                            }
                            Ok(result) => match result {
                                Ok(response) => response,
                                Err(error) => {
                                    self.record_wake_instance_call(Outcome::Error);
                                    self.wake_tracker.complete(
                                        &request.instance_id,
                                        request.expected_generation,
                                    );
                                    return Err(FrontlineRouteCoordinatorError::Wake(error));
                                }
                            },
                        };

                        let disposition = validate_wake_response(&cache_entry.entry, response);
                        self.record_wake_instance_call(wake_response_outcome(&disposition));
                        self.handle_wake_response(cache_entry, disposition, now)
                            .await
                    }
                }
            }
        }
    }

    fn record_wake_instance_call(&self, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![
                Operation::WakeInstance.metric_label(),
                outcome.metric_label(),
            ],
            1.0,
        ));
    }

    async fn handle_wake_response(
        &mut self,
        cache_entry: Arc<crate::PositiveCacheEntry>,
        disposition: WakeResponseDisposition,
        now: Instant,
    ) -> Result<
        FrontlineRouteOutcome,
        FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>,
    > {
        let observed_instance_id = cache_entry.entry.instance_id.clone();
        let observed_generation = cache_entry.entry.instance_generation;
        self.wake_tracker
            .complete(&observed_instance_id, observed_generation);

        match disposition {
            WakeResponseDisposition::WakeStarted {
                instance_id,
                generation,
            }
            | WakeResponseDisposition::StillWaking {
                instance_id,
                generation,
            } => Ok(FrontlineRouteOutcome::Waking {
                instance_id,
                generation,
            }),
            WakeResponseDisposition::Ready(backend) => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                self.apply_ready_wake(cache_entry, backend.clone(), now)
                    .await?;
                Ok(FrontlineRouteOutcome::Ready(backend))
            }
            WakeResponseDisposition::Failed {
                instance_id,
                generation,
                reason,
            } => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::WakeFailed {
                    instance_id,
                    generation,
                    reason,
                })
            }
            WakeResponseDisposition::Unavailable {
                instance_id,
                generation,
                reason,
            } => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::WakeUnavailable {
                    instance_id,
                    generation,
                    reason,
                })
            }
            WakeResponseDisposition::GenerationConflict {
                instance_id,
                expected_generation,
                actual_generation,
            } => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::GenerationConflict {
                    instance_id,
                    expected_generation,
                    actual_generation,
                })
            }
            WakeResponseDisposition::Rejected(stale) => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::RejectedWakeObservation(stale))
            }
        }
    }

    async fn apply_ready_wake(
        &mut self,
        cache_entry: Arc<crate::PositiveCacheEntry>,
        backend: ReadyBackend,
        now: Instant,
    ) -> Result<(), FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>> {
        let remaining_ttl = cache_entry.expires_at().saturating_duration_since(now);
        let update = SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: cache_entry.subscription_id.clone(),
            matched_identity: cache_entry.matched_identity.clone(),
            entry: ready_route_entry(&cache_entry.entry, backend),
            cache_policy: CachePolicy::new(remaining_ttl),
        };

        let outcome = self
            .resolver
            .apply_control_plane_message(update, now)
            .await
            .map_err(FrontlineRouteCoordinatorError::CacheUpdate)?;

        match outcome {
            crate::ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::Replaced(_)) => {
                Ok(())
            }
            crate::ApplyControlPlaneMessageOutcome::Updated(outcome) => {
                Err(FrontlineRouteCoordinatorError::RejectedCacheUpdate(outcome))
            }
            crate::ApplyControlPlaneMessageOutcome::Resolved(_)
            | crate::ApplyControlPlaneMessageOutcome::Miss(_)
            | crate::ApplyControlPlaneMessageOutcome::Invalidated { .. } => {
                unreachable!("RouteUpdated control-plane messages must produce an update outcome")
            }
        }
    }
}

fn ready_route_entry(observed: &RouteEntry, backend: ReadyBackend) -> RouteEntry {
    RouteEntry {
        route_binding_id: observed.route_binding_id.clone(),
        instance_id: observed.instance_id.clone(),
        instance_state: InstanceState::Running,
        instance_generation: backend.instance_generation,
        backend: Some(backend.backend),
        backend_generation: backend.backend_generation,
    }
}

fn wake_response_outcome(disposition: &WakeResponseDisposition) -> Outcome {
    match disposition {
        WakeResponseDisposition::Ready(_) => Outcome::Success,
        WakeResponseDisposition::WakeStarted { .. } => Outcome::Started,
        WakeResponseDisposition::StillWaking { .. } => Outcome::AlreadyWaking,
        WakeResponseDisposition::Failed { .. }
        | WakeResponseDisposition::Unavailable { .. }
        | WakeResponseDisposition::GenerationConflict { .. } => Outcome::Rejected,
        WakeResponseDisposition::Rejected(_) => Outcome::Error,
    }
}

impl<RouteClient, Wake> PartialEq for FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: PartialEq,
    Wake: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.resolver == other.resolver
            && self.wake_tracker == other.wake_tracker
            && self.wake_client == other.wake_client
    }
}

impl<RouteClient, Wake> Eq for FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: Eq,
    Wake: Eq,
{
}

impl<RouteClientError, WakeClientError> fmt::Display
    for FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>
where
    RouteClientError: fmt::Display,
    WakeClientError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolve(error) => write!(f, "route resolution failed: {error}"),
            Self::CacheUpdate(error) => write!(f, "route cache update failed: {error}"),
            Self::Wake(error) => write!(f, "wake request failed: {error}"),
            Self::WakeDeadline(request) => write!(
                f,
                "wake request timed out for instance {} generation {}",
                request.instance_id.as_str(),
                request.expected_generation.get()
            ),
            Self::RouteActorClosed => f.write_str("frontline route actor is closed"),
            Self::Saturated => f.write_str("frontline route admission is saturated"),
            Self::SubscribeDeadline => f.write_str("route subscription timed out"),
            Self::RouteDeadline => f.write_str("route readiness timed out"),
            Self::InvalidatedDuringResolution => {
                f.write_str("route changed during resolution; retry")
            }
            Self::RejectedCacheUpdate(outcome) => {
                write!(f, "ready wake cache update was rejected: {outcome:?}")
            }
        }
    }
}

impl<RouteClientError, WakeClientError> std::error::Error
    for FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>
where
    RouteClientError: fmt::Debug + fmt::Display,
    WakeClientError: fmt::Debug + fmt::Display,
{
}

#[cfg(test)]
mod tests;
