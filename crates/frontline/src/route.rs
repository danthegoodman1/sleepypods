//! Route coordination: the request-side lookup and the actor that keeps the
//! route cache current.
//!
//! A request reads the published cache snapshot. A host the snapshot holds is
//! answered there, with no lock, task hop or control-plane call. A host it
//! lacks is resolved by one flight per host: the actor subscribes to the host
//! and installs the rule set the control plane returns. A request whose route
//! points at an instance that is not running parks on that instance until its
//! rule set changes.
//!
//! The actor owns the Subscribe stream. It reads every control-plane message
//! in stream order, so a rule set and a later change to it are applied in the
//! order the control plane sent them.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use proxy_core::observability::{
    metrics::{
        RUNTIME_CONTROL_PLANE_CALLS_TOTAL, RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL,
        RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL,
    },
    prometheus::PrometheusMetricsSink,
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
        EVENT_SUBSCRIBE_STREAM,
    },
    Operation, Outcome,
};
use sleepypods_api::{
    Generation, HostRules, InstanceId, InstanceState, RouteHost, RouteIdentity, RouteKind,
    RouteRule,
};
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

use crate::{
    cache::{Displaced, HostEntry, HostKey, RouteSnapshot, RouteTable},
    identity::{canonical_request_host, canonical_request_path, route_host},
    route_disposition, ReadyBackend, RequestIdentityError, RouteDisposition, RouteStream,
    RouteStreamEvent, RouteStreamReply, RouteStreamRequest, RouteSubscriptionClient,
    SubscribeErrorCode, SubscriptionId, WakeInstanceRequest, WakeInstanceResponse, WakeUnavailable,
};

pub type WakeClientFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub trait WakeClient {
    /// Shared with every request parked on the instance the call was for.
    type Error: Clone + Send + Sync + 'static;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error>;

    /// Whether the same call may succeed later: the transport failed, or the
    /// control plane could not answer for now. Requests stay parked through a
    /// transient failure, and a definite one fails them.
    fn is_transient(error: &Self::Error) -> bool;
}

/// Hosts being resolved at once, demand and background together.
const MAX_ROUTE_FLIGHTS: usize = 64;
/// Flights that keep cached rule sets current. Demand keeps the other half,
/// so background work cannot crowd out a host the cache has never seen.
const MAX_BACKGROUND_FLIGHTS: usize = MAX_ROUTE_FLIGHTS / 2;
/// Requests waiting on a flight or parked on an instance.
const MAX_ROUTE_WAITERS: usize = 256;
const MAX_WAKE_CALLS: usize = 64;
const SUBSCRIBE_DEADLINE: Duration = Duration::from_secs(5);
const CONNECT_DEADLINE: Duration = Duration::from_secs(5);
const DEFAULT_WAKE_DEADLINE: Duration = Duration::from_secs(5);
const DEFAULT_ROUTE_DEADLINE: Duration = Duration::from_secs(130);
const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(250);
const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(20);
/// How long a parked request trusts the push before it asks for its host's
/// rule set again.
const PARKED_REFRESH_INTERVAL: Duration = Duration::from_millis(1500);
/// The least time between wake calls for one instance generation.
const WAKE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// The wait before the first call after a transient wake failure. Each
/// further failure doubles it, up to `MAX_WAKE_BACKOFF`.
const WAKE_BACKOFF: Duration = Duration::from_millis(250);
const MAX_WAKE_BACKOFF: Duration = Duration::from_secs(2);
/// Times a request resolves a host whose answer left the cache before the
/// request could read it.
const MAX_RESOLVE_ATTEMPTS: usize = 3;
const EXPIRY_BATCH: usize = 64;
/// Stream replies applied before the cache snapshot is published again.
const REPLY_BATCH: usize = 64;
const COMMAND_BUFFER: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteOutcome {
    /// Forward to this backend.
    Ready(ReadyBackend),
    /// No route covers the request.
    Miss,
    /// The route's instance is being deleted.
    Unavailable(WakeUnavailable),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteCoordinatorError<WakeClientError> {
    /// The request names a host or path the proxy does not route.
    Identity(RequestIdentityError),
    /// The control plane refused or failed the lookup for this host.
    Resolve {
        code: SubscribeErrorCode,
        message: String,
    },
    SubscribeDeadline,
    /// The control plane refused the wake call.
    Wake(WakeClientError),
    /// The wake the request waited on left the instance Failed.
    WakeFailed,
    /// The instance did not run within the route deadline.
    RouteDeadline,
    Saturated,
    RouteActorClosed,
}

type RouteResult<W> = Result<FrontlineRouteOutcome, FrontlineRouteCoordinatorError<W>>;
/// Tells a waiting request that its host's flight ended.
type FlightReply<W> = oneshot::Sender<FlightResult<W>>;
type FlightResult<W> = Result<(), FrontlineRouteCoordinatorError<W>>;
/// A wake call's answer, or `None` when none arrived within the wake deadline.
type WakeResult<W> = Option<Result<WakeInstanceResponse, W>>;

/// Route-cache lookups counted without shared writes between cores.
#[derive(Clone, Debug)]
pub struct RouteLookupCounters {
    stripes: Arc<[LookupStripe]>,
    reported: Arc<Mutex<(u64, u64)>>,
}

impl Default for RouteLookupCounters {
    fn default() -> Self {
        Self {
            stripes: (0..LOOKUP_STRIPES)
                .map(|_| LookupStripe::default())
                .collect(),
            reported: Arc::default(),
        }
    }
}

const LOOKUP_STRIPES: usize = 64;

#[derive(Debug, Default)]
#[repr(align(128))]
struct LookupStripe {
    hits: AtomicU64,
    misses: AtomicU64,
}

impl RouteLookupCounters {
    fn stripe(&self) -> &LookupStripe {
        thread_local! {
            static STRIPE: usize = {
                static NEXT: AtomicUsize = AtomicUsize::new(0);
                NEXT.fetch_add(1, Ordering::Relaxed) % LOOKUP_STRIPES
            };
        }
        &self.stripes[STRIPE.with(|stripe| *stripe)]
    }

    fn hit(&self) {
        self.stripe().hits.fetch_add(1, Ordering::Relaxed);
    }

    fn miss(&self) {
        self.stripe().misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Lookups the cache answered and lookups that needed the control plane.
    pub fn totals(&self) -> (u64, u64) {
        self.stripes.iter().fold((0, 0), |(hits, misses), stripe| {
            (
                hits + stripe.hits.load(Ordering::Relaxed),
                misses + stripe.misses.load(Ordering::Relaxed),
            )
        })
    }

    /// Adds the lookups since the last collection to the exported counters.
    pub fn collect(&self, sink: &PrometheusMetricsSink) {
        let (hits, misses) = self.totals();
        let mut reported = self.reported.lock().expect("route lookup counters");
        let (reported_hits, reported_misses) = &mut *reported;
        for (outcome, total, last) in [
            (Outcome::Hit, hits, reported_hits),
            (Outcome::Miss, misses, reported_misses),
        ] {
            if total > *last {
                sink.record_observation(MetricObservation::new(
                    RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL,
                    vec![outcome.metric_label()],
                    (total - *last) as f64,
                ));
                *last = total;
            }
        }
    }
}

/// The parts of a coordinator, before its actor starts.
#[derive(Clone, Debug)]
pub struct FrontlineRouteCoordinator<RouteClient, Wake> {
    cache_capacity: usize,
    route_client: RouteClient,
    wake_client: Wake,
    wake_deadline: Duration,
    route_deadline: Duration,
    reconnect_backoff: Duration,
    observability: ObservabilityRecorder,
    counters: RouteLookupCounters,
}

impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake> {
    pub fn new(cache_capacity: usize, route_client: RouteClient, wake_client: Wake) -> Self {
        Self {
            cache_capacity,
            route_client,
            wake_client,
            wake_deadline: DEFAULT_WAKE_DEADLINE,
            route_deadline: DEFAULT_ROUTE_DEADLINE,
            reconnect_backoff: DEFAULT_RECONNECT_BACKOFF,
            observability: ObservabilityRecorder::default(),
            counters: RouteLookupCounters::default(),
        }
    }

    pub fn with_observability(mut self, observability: ObservabilityRecorder) -> Self {
        self.observability = observability;
        self
    }

    pub fn with_lookup_counters(mut self, counters: RouteLookupCounters) -> Self {
        self.counters = counters;
        self
    }

    pub fn with_wake_deadline(mut self, wake_deadline: Duration) -> Self {
        self.wake_deadline = wake_deadline;
        self
    }

    pub fn with_route_deadline(mut self, deadline: Duration) -> Self {
        self.route_deadline = deadline;
        self
    }

    /// The wait after a failed stream before the next connect. A stream that
    /// ended in order is replaced at once.
    pub fn with_reconnect_backoff(mut self, backoff: Duration) -> Self {
        self.reconnect_backoff = backoff;
        self
    }

    pub(crate) fn route_deadline(&self) -> Duration {
        self.route_deadline
    }
}

impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + Sync + 'static,
{
    /// Starts the actor on the current Tokio runtime.
    pub fn into_shared(self) -> SharedFrontlineRouteCoordinator<RouteClient, Wake> {
        let state = Arc::new(State {
            snapshot: ArcSwap::from_pointee(RouteSnapshot::default()),
            parking: Parking::default(),
            counters: self.counters,
        });
        let (commands, receiver) = mpsc::channel(COMMAND_BUFFER);
        let actor = Actor {
            client: self.route_client,
            wake_client: self.wake_client,
            state: state.clone(),
            commands: receiver,
            table: RouteTable::new(self.cache_capacity),
            session: Session::Idle {
                connect_at: tokio::time::Instant::now(),
            },
            next_subscription: 0,
            pending: HashMap::new(),
            resolving: HashMap::new(),
            registrations: VecDeque::new(),
            outbox: VecDeque::new(),
            wakes: HashMap::new(),
            wake_calls: tokio::task::JoinSet::new(),
            effects: Effects::default(),
            wake_deadline: self.wake_deadline,
            reconnect_backoff: self.reconnect_backoff,
            observability: self.observability,
        };
        SharedFrontlineRouteCoordinator {
            shared: Arc::new(Shared {
                state,
                commands,
                waiters: Arc::new(Semaphore::new(MAX_ROUTE_WAITERS)),
                route_deadline: self.route_deadline,
                actor: ActorTask(tokio::spawn(actor.run())),
            }),
            _client: PhantomData,
        }
    }
}

/// What request tasks and the actor share.
struct State<W> {
    snapshot: ArcSwap<RouteSnapshot>,
    parking: Parking<W>,
    counters: RouteLookupCounters,
}

struct Shared<W> {
    state: Arc<State<W>>,
    commands: mpsc::Sender<Command<W>>,
    waiters: Arc<Semaphore>,
    route_deadline: Duration,
    actor: ActorTask,
}

struct ActorTask(tokio::task::JoinHandle<()>);

impl Drop for ActorTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The request-side handle. Cloning shares one cache and one actor.
pub struct SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    shared: Arc<Shared<Wake::Error>>,
    _client: PhantomData<fn() -> RouteClient>,
}

impl<RouteClient, Wake> Clone for SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            _client: PhantomData,
        }
    }
}

impl<RouteClient, Wake> fmt::Debug for SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedFrontlineRouteCoordinator")
            .finish_non_exhaustive()
    }
}

/// The cache clock. It follows Tokio's clock, which tests can pause.
fn clock() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// What the snapshot says about one request.
enum Lookup {
    Answer(FrontlineRouteOutcome),
    /// The route's instance is not running. `failed` means the instance is
    /// Failed. `version` identifies the rule set this was read from.
    Pending {
        instance: WakeKey,
        wake: bool,
        failed: bool,
        version: u64,
    },
    Absent,
}

impl<RouteClient, Wake> SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
    Wake::Error: Clone + Send + Sync + 'static,
{
    /// Routes a canonical identity, such as the SNI name of a TLS connection.
    pub async fn route(&self, identity: RouteIdentity, now: Instant) -> RouteResult<Wake::Error> {
        let path = match &identity {
            RouteIdentity::Http { path, .. } => path.as_ref().map_or("/", |path| path.as_str()),
            RouteIdentity::Sni { .. } => "",
        };
        self.route_canonical(identity.kind(), identity.host().as_str(), path, now)
            .await
    }

    /// Routes an HTTP request by its Host value and path. A host over 253
    /// bytes or a path over 4,096 bytes is refused without a lookup.
    pub async fn route_http(
        &self,
        host: &str,
        path: Option<&str>,
        now: Instant,
    ) -> RouteResult<Wake::Error> {
        let host =
            canonical_request_host(host).map_err(FrontlineRouteCoordinatorError::Identity)?;
        let path =
            canonical_request_path(path).map_err(FrontlineRouteCoordinatorError::Identity)?;
        self.route_canonical(RouteKind::Http, &host, path, now)
            .await
    }

    /// Lookups the cache answered and lookups that needed the control plane.
    pub fn lookup_counters(&self) -> &RouteLookupCounters {
        &self.shared.state.counters
    }

    async fn route_canonical(
        &self,
        kind: RouteKind,
        host: &str,
        path: &str,
        now: Instant,
    ) -> RouteResult<Wake::Error> {
        if self.shared.actor.0.is_finished() {
            return Err(FrontlineRouteCoordinatorError::RouteActorClosed);
        }
        let first = self.lookup(kind, host, path, now);
        match &first {
            Lookup::Absent => self.shared.state.counters.miss(),
            _ => self.shared.state.counters.hit(),
        }
        if let Lookup::Answer(outcome) = first {
            return Ok(outcome);
        }
        self.wait_for_route(kind, host, path, first).await
    }

    fn lookup(&self, kind: RouteKind, host: &str, path: &str, now: Instant) -> Lookup {
        let snapshot = self.shared.state.snapshot.load();
        let Some(entry) = snapshot.get(kind, host) else {
            return Lookup::Absent;
        };
        if entry.is_expired(now) {
            return Lookup::Absent;
        }
        entry.mark_used();
        let Some(rule) = entry.rules.select(path) else {
            return Lookup::Answer(FrontlineRouteOutcome::Miss);
        };
        match route_disposition(&rule.entry) {
            RouteDisposition::Ready(backend) => {
                Lookup::Answer(FrontlineRouteOutcome::Ready(backend))
            }
            RouteDisposition::Unavailable(unavailable) => {
                Lookup::Answer(FrontlineRouteOutcome::Unavailable(unavailable))
            }
            RouteDisposition::Pending {
                instance_id,
                generation,
                wake,
            } => Lookup::Pending {
                instance: WakeKey {
                    instance_id,
                    generation,
                },
                wake,
                failed: rule.entry.instance_state == InstanceState::Failed,
                version: entry.version,
            },
        }
    }

    /// The path for a request the snapshot cannot answer yet: resolve the
    /// host, then wait for its instance.
    async fn wait_for_route(
        &self,
        kind: RouteKind,
        host: &str,
        path: &str,
        mut seen: Lookup,
    ) -> RouteResult<Wake::Error> {
        let _waiter = self
            .shared
            .waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| FrontlineRouteCoordinatorError::Saturated)?;
        let subscribed_host = route_host(host).map_err(FrontlineRouteCoordinatorError::Identity)?;
        let key = HostKey::new(kind, host);
        let deadline = tokio::time::Instant::now() + self.shared.route_deadline;
        let mut resolves = 0;
        // The instance generation this request last waited on.
        let mut waited_on: Option<WakeKey> = None;
        loop {
            match seen {
                Lookup::Answer(outcome) => return Ok(outcome),
                Lookup::Absent => {
                    if resolves == MAX_RESOLVE_ATTEMPTS {
                        return Err(FrontlineRouteCoordinatorError::Resolve {
                            code: SubscribeErrorCode::Unavailable,
                            message: "the host's rule set did not stay cached".to_owned(),
                        });
                    }
                    resolves += 1;
                    self.resolve(&key, &subscribed_host, None).await?;
                }
                Lookup::Pending {
                    instance,
                    wake,
                    failed,
                    version,
                } => {
                    resolves = 0;
                    // The wake this request waited on ended in Failed. Only a
                    // request that arrives later wakes the instance again.
                    if failed
                        && waited_on.as_ref().is_some_and(|waited| {
                            waited.instance_id == instance.instance_id
                                && waited.generation != instance.generation
                        })
                    {
                        return Err(FrontlineRouteCoordinatorError::WakeFailed);
                    }
                    let mut parked = self.shared.state.parking.park(&instance);
                    // The rule set may have changed between the lookup and
                    // parking; only an unchanged one is worth waiting on.
                    let unchanged = matches!(
                        self.lookup(kind, host, path, clock()),
                        Lookup::Pending { version: current, .. } if current == version
                    );
                    if unchanged {
                        waited_on = Some(instance.clone());
                        if wake {
                            self.command(Command::Wake {
                                instance,
                                host: key.clone(),
                            })
                            .await?;
                        }
                        tokio::select! {
                            failure = parked.changed() => {
                                if let Some(failure) = failure {
                                    return Err(failure);
                                }
                            }
                            _ = tokio::time::sleep(PARKED_REFRESH_INTERVAL) => {
                                // The push may be late or lost. A failed
                                // refresh leaves the cached rule set in place,
                                // and the request keeps waiting on it.
                                let _ = self.resolve(&key, &subscribed_host, Some(version)).await;
                            }
                            _ = tokio::time::sleep_until(deadline) => {
                                return Err(FrontlineRouteCoordinatorError::RouteDeadline);
                            }
                        }
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(FrontlineRouteCoordinatorError::RouteDeadline);
            }
            seen = self.lookup(kind, host, path, clock());
        }
    }

    /// Joins or starts the flight for a host. With `stale_version`, the
    /// flight runs only while the cache still holds that version.
    async fn resolve(
        &self,
        key: &HostKey,
        host: &RouteHost,
        stale_version: Option<u64>,
    ) -> Result<(), FrontlineRouteCoordinatorError<Wake::Error>> {
        let (reply, result) = oneshot::channel();
        self.command(Command::Resolve {
            key: key.clone(),
            host: host.clone(),
            stale_version,
            reply,
        })
        .await?;
        result
            .await
            .unwrap_or(Err(FrontlineRouteCoordinatorError::RouteActorClosed))
    }

    async fn command(
        &self,
        command: Command<Wake::Error>,
    ) -> Result<(), FrontlineRouteCoordinatorError<Wake::Error>> {
        self.shared
            .commands
            .send(command)
            .await
            .map_err(|_| FrontlineRouteCoordinatorError::RouteActorClosed)
    }
}

/// One instance generation that requests wait on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WakeKey {
    instance_id: InstanceId,
    generation: Generation,
}

impl WakeKey {
    fn of(rule: &RouteRule) -> Self {
        Self {
            instance_id: rule.entry.instance_id.clone(),
            generation: rule.entry.instance_generation,
        }
    }
}

/// Requests parked per instance generation. The actor signals a slot when a
/// rule set naming that generation changes or the control plane refuses its
/// wake.
struct Parking<W> {
    slots: Mutex<HashMap<WakeKey, Arc<ParkSlot<W>>>>,
}

struct ParkSlot<W> {
    signal: watch::Sender<ParkSignal<W>>,
    waiters: AtomicUsize,
}

#[derive(Clone)]
struct ParkSignal<W> {
    version: u64,
    /// The signal version at which the wake call failed, and its error.
    failure: Option<(u64, FrontlineRouteCoordinatorError<W>)>,
}

struct Parked<'a, W> {
    parking: &'a Parking<W>,
    key: WakeKey,
    slot: Arc<ParkSlot<W>>,
    signal: watch::Receiver<ParkSignal<W>>,
    parked_at: u64,
}

impl<W> Default for Parking<W> {
    fn default() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }
}

impl<W: Clone> Parking<W> {
    fn park(&self, key: &WakeKey) -> Parked<'_, W> {
        let mut slots = self.slots.lock().expect("route parking");
        let slot = slots
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(ParkSlot {
                    signal: watch::channel(ParkSignal {
                        version: 0,
                        failure: None,
                    })
                    .0,
                    waiters: AtomicUsize::new(0),
                })
            })
            .clone();
        slot.waiters.fetch_add(1, Ordering::Relaxed);
        let mut signal = slot.signal.subscribe();
        let parked_at = signal.borrow_and_update().version;
        Parked {
            parking: self,
            key: key.clone(),
            slot,
            signal,
            parked_at,
        }
    }

    /// Whether any request is parked on `key`.
    fn is_waited_on(&self, key: &WakeKey) -> bool {
        self.slots.lock().expect("route parking").contains_key(key)
    }

    fn signal(&self, key: &WakeKey) {
        if let Some(slot) = self.slots.lock().expect("route parking").get(key) {
            slot.signal.send_modify(|signal| signal.version += 1);
        }
    }

    fn signal_all(&self) {
        for slot in self.slots.lock().expect("route parking").values() {
            slot.signal.send_modify(|signal| signal.version += 1);
        }
    }

    fn fail(&self, key: &WakeKey, error: FrontlineRouteCoordinatorError<W>) {
        if let Some(slot) = self.slots.lock().expect("route parking").get(key) {
            slot.signal.send_modify(|signal| {
                signal.version += 1;
                signal.failure = Some((signal.version, error));
            });
        }
    }
}

impl<W: Clone> Parked<'_, W> {
    /// Waits for the next signal. Returns the wake failure, if one arrived
    /// after this request parked.
    async fn changed(&mut self) -> Option<FrontlineRouteCoordinatorError<W>> {
        if self.signal.changed().await.is_err() {
            return None;
        }
        let signal = self.signal.borrow_and_update();
        match &signal.failure {
            Some((at, error)) if *at > self.parked_at => Some(error.clone()),
            _ => None,
        }
    }
}

impl<W> Drop for Parked<'_, W> {
    fn drop(&mut self) {
        let mut slots = self.parking.slots.lock().expect("route parking");
        if self.slot.waiters.fetch_sub(1, Ordering::Relaxed) == 1 {
            slots.remove(&self.key);
        }
    }
}

enum Command<W> {
    Resolve {
        key: HostKey,
        host: RouteHost,
        stale_version: Option<u64>,
        reply: FlightReply<W>,
    },
    Wake {
        instance: WakeKey,
        host: HostKey,
    },
}

enum Session {
    /// No stream. The next connect starts at `connect_at`.
    Idle {
        connect_at: tokio::time::Instant,
    },
    Connecting(tokio::task::JoinHandle<Option<RouteStream>>),
    Open(RouteStream),
}

enum SessionEvent {
    ConnectDue,
    Connected(Option<RouteStream>),
    Stream(Option<RouteStreamEvent>),
}

/// One host being resolved.
struct Flight<W> {
    key: HostKey,
    host: RouteHost,
    deadline: tokio::time::Instant,
    /// Requests waiting for the answer. A flight with none keeps a cached rule
    /// set current in the background.
    waiters: Vec<FlightReply<W>>,
    background: bool,
    /// Times the control plane reported its subscription limit.
    exhausted: usize,
}

/// The wake calls for one instance generation.
struct WakeCall {
    in_flight: bool,
    /// The earliest start of the next call.
    next_at: Instant,
    /// Set while calls fail transiently: the actor calls again at `next_at`
    /// while requests stay parked, and waits this long after another failure.
    backoff: Option<Duration>,
    /// Hosts whose requests asked for this wake.
    hosts: HashSet<HostKey>,
}

/// What a step decided, applied after the snapshot it depends on is
/// published.
struct Effects<W> {
    replies: Vec<(FlightReply<W>, FlightResult<W>)>,
    signals: Vec<WakeKey>,
    signal_all: bool,
    failures: Vec<(WakeKey, FrontlineRouteCoordinatorError<W>)>,
}

impl<W> Default for Effects<W> {
    fn default() -> Self {
        Self {
            replies: Vec::new(),
            signals: Vec::new(),
            signal_all: false,
            failures: Vec::new(),
        }
    }
}

struct Actor<Client, Wake: WakeClient> {
    client: Client,
    wake_client: Wake,
    state: Arc<State<Wake::Error>>,
    commands: mpsc::Receiver<Command<Wake::Error>>,
    table: RouteTable,
    session: Session,
    next_subscription: SubscriptionId,
    pending: HashMap<SubscriptionId, Flight<Wake::Error>>,
    resolving: HashMap<HostKey, SubscriptionId>,
    /// Carried-over rule sets waiting for a subscription on the current
    /// stream.
    registrations: VecDeque<HostKey>,
    /// Requests for the stream, in order, until its channel has room.
    outbox: VecDeque<RouteStreamRequest>,
    wakes: HashMap<WakeKey, WakeCall>,
    wake_calls: tokio::task::JoinSet<(WakeKey, WakeResult<Wake::Error>)>,
    effects: Effects<Wake::Error>,
    wake_deadline: Duration,
    reconnect_backoff: Duration,
    observability: ObservabilityRecorder,
}

enum Step<W> {
    Session(SessionEvent),
    WakeFinished(WakeKey, WakeResult<W>),
    Command(Command<W>),
    Tick,
    Closed,
}

impl<Client, Wake> Actor<Client, Wake>
where
    Client: RouteSubscriptionClient + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + Sync + 'static,
{
    async fn run(mut self) {
        let mut tick = tokio::time::interval(MAINTENANCE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            self.connect_when_due();
            self.register_carried_hosts();
            self.flush_outbox();
            self.publish();
            let step = tokio::select! {
                event = session_event(&mut self.session) => Step::Session(event),
                Some(finished) = self.wake_calls.join_next(), if !self.wake_calls.is_empty() => {
                    match finished {
                        Ok((key, result)) => Step::WakeFinished(key, result),
                        Err(_) => Step::Closed,
                    }
                }
                command = self.commands.recv() => command.map_or(Step::Closed, Step::Command),
                _ = tick.tick() => Step::Tick,
            };
            match step {
                // The top of the loop connects.
                Step::Session(SessionEvent::ConnectDue) => {}
                Step::Session(SessionEvent::Connected(Some(stream))) => {
                    self.record_call(Operation::SubscribeStream, Outcome::Success);
                    self.on_connected(stream)
                }
                Step::Session(SessionEvent::Connected(None)) => {
                    self.record_call(Operation::SubscribeStream, Outcome::Error);
                    // Nothing was lost: no stream existed to lose it.
                    self.session = Session::Idle {
                        connect_at: tokio::time::Instant::now() + self.reconnect_backoff,
                    };
                }
                Step::Session(SessionEvent::Stream(event)) => {
                    self.on_stream_event(event);
                    // Replies that already arrived share one snapshot.
                    for _ in 1..REPLY_BATCH {
                        let Session::Open(stream) = &mut self.session else {
                            break;
                        };
                        match stream.events.try_recv() {
                            Ok(event) => self.on_stream_event(Some(event)),
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                self.on_stream_event(None)
                            }
                        }
                    }
                }
                Step::WakeFinished(key, result) => self.on_wake_finished(key, result),
                Step::Command(Command::Resolve {
                    key,
                    host,
                    stale_version,
                    reply,
                }) => self.on_resolve(key, host, stale_version, reply),
                Step::Command(Command::Wake { instance, host }) => self.on_wake(instance, host),
                Step::Tick => self.maintain(),
                Step::Closed => {
                    // Request tasks stop serving once the actor is gone.
                    let _ = self.table.clear();
                    self.publish();
                    return;
                }
            }
        }
    }

    fn on_stream_event(&mut self, event: Option<RouteStreamEvent>) {
        match event {
            Some(RouteStreamEvent::Reply(reply)) => self.on_reply(reply),
            Some(RouteStreamEvent::Ended) => self.on_stream_end(true),
            Some(RouteStreamEvent::Failed) | None => self.on_stream_end(false),
        }
    }

    /// Publishes the table when it changed, then releases what waited on it.
    fn publish(&mut self) {
        if let Some(snapshot) = self.table.publish() {
            self.state.snapshot.store(snapshot);
        }
        let effects = std::mem::take(&mut self.effects);
        for (reply, result) in effects.replies {
            let _ = reply.send(result);
        }
        if effects.signal_all {
            self.state.parking.signal_all();
        }
        for key in effects.signals {
            self.state.parking.signal(&key);
        }
        for (key, error) in effects.failures {
            self.state.parking.fail(&key, error);
        }
    }

    fn connect_when_due(&mut self) {
        let Session::Idle { connect_at } = &self.session else {
            return;
        };
        if tokio::time::Instant::now() < *connect_at {
            return;
        }
        let connect = self.client.connect();
        self.session = Session::Connecting(tokio::spawn(async move {
            tokio::time::timeout(CONNECT_DEADLINE, connect)
                .await
                .ok()
                .and_then(Result::ok)
        }));
    }

    fn on_connected(&mut self, stream: RouteStream) {
        self.session = Session::Open(stream);
        // Flights that outlived the last stream ask again on this one.
        let mut flights: Vec<_> = self.pending.iter().collect();
        flights.sort_by_key(|(id, _)| **id);
        for (id, flight) in flights {
            self.outbox.push_back(RouteStreamRequest::Subscribe {
                id: *id,
                kind: flight.key.kind,
                host: flight.host.clone(),
            });
        }
    }

    /// `in_order` means the control plane closed the stream after delivering
    /// everything it had sent: cached rule sets stay and are registered on the
    /// next stream. Any other end may have lost a change, so the cache is
    /// emptied before another request reads it.
    fn on_stream_end(&mut self, in_order: bool) {
        let backoff = if in_order {
            Duration::ZERO
        } else {
            self.reconnect_backoff
        };
        self.session = Session::Idle {
            connect_at: tokio::time::Instant::now() + backoff,
        };
        self.outbox.clear();
        self.registrations.clear();
        if in_order {
            let (kept, displaced) = self.table.end_session();
            self.registrations.extend(kept);
            self.release(displaced);
        } else {
            let displaced = self.table.clear();
            self.release(displaced);
            self.effects.signal_all = true;
        }
        // A background flight belongs to the stream that carried it. A
        // flight with waiters asks again on the next stream.
        let background: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, flight)| flight.background)
            .map(|(id, _)| *id)
            .collect();
        for id in background {
            if let Some(flight) = self.pending.remove(&id) {
                self.resolving.remove(&flight.key);
                if in_order {
                    self.registrations.push_back(flight.key);
                }
            }
        }
        // An orderly end is routine. A failed one cost the whole cache.
        let outcome = if in_order {
            Outcome::Closed
        } else {
            Outcome::Error
        };
        self.record_stream_event(outcome, Vec::new());
    }

    fn on_reply(&mut self, reply: RouteStreamReply) {
        match reply {
            RouteStreamReply::Rules {
                id,
                rules,
                cache_policy,
            } => {
                let Some(key) = self
                    .pending
                    .get(&id)
                    .map(|flight| flight.key.clone())
                    .or_else(|| self.table.host_of(id).cloned())
                else {
                    // A subscription this proxy already released.
                    return;
                };
                if !rules
                    .iter()
                    .all(|rule| rule.identity.applies_to(key.kind, &key.host))
                {
                    // A rule for another host cannot be trusted or placed.
                    self.fail_flight(id, SubscribeErrorCode::Unavailable, "invalid rule set");
                    self.on_stream_end(false);
                    return;
                }
                let pushed = !self.pending.contains_key(&id);
                if pushed && rules.is_empty() {
                    // The host's last binding was removed, and the control
                    // plane dropped the subscription. The next request asks
                    // again, so a binding committed right after it is served
                    // at once.
                    let displaced = self.table.remove_subscription(id);
                    self.release(displaced);
                    self.record_stream_event(Outcome::Invalidated, Vec::new());
                    return;
                }
                let expires_at = clock() + cache_policy.ttl();
                let displaced =
                    self.table
                        .install(key, HostRules::ranked(rules), expires_at, Some(id));
                self.release(displaced);
                if let Some(flight) = self.pending.remove(&id) {
                    self.resolving.remove(&flight.key);
                    self.record_call(Operation::SubscribeRoute, Outcome::Success);
                    for waiter in flight.waiters {
                        self.effects.replies.push((waiter, Ok(())));
                    }
                }
                if pushed {
                    self.record_stream_event(Outcome::Updated, Vec::new());
                }
            }
            RouteStreamReply::Error { id, code, message } => {
                if self.pending.contains_key(&id) {
                    self.on_flight_error(id, code, message);
                } else if self.table.host_of(id).is_some() {
                    // The control plane ended the subscription that kept this host
                    // current.
                    let displaced = self.table.remove_subscription(id);
                    self.release(displaced);
                    self.record_stream_event(
                        Outcome::Invalidated,
                        vec![LogField::error_reason(message)],
                    );
                }
            }
        }
    }

    fn on_flight_error(&mut self, id: SubscriptionId, code: SubscribeErrorCode, message: String) {
        let Some(flight) = self.pending.get_mut(&id) else {
            return;
        };
        if code == SubscribeErrorCode::ResourceExhausted && flight.exhausted < 2 {
            // The control plane holds as many subscriptions as it allows this
            // stream. Release the oldest rule sets and ask again; the release
            // precedes the retry on the stream.
            flight.exhausted += 1;
            let room = 4 * flight.exhausted;
            let retry = RouteStreamRequest::Subscribe {
                id,
                kind: flight.key.kind,
                host: flight.host.clone(),
            };
            // Holding one rule set fewer than the control plane refused at
            // leaves room for the lookup that brings in the next host.
            let held = self.table.positive_len();
            let displaced = self.table.evict_oldest(room);
            self.table.limit_capacity(held.saturating_sub(1));
            self.release(displaced);
            self.outbox.push_back(retry);
            self.record_call(Operation::SubscribeRoute, Outcome::Rejected);
            return;
        }
        self.fail_flight(id, code, &message);
    }

    fn fail_flight(&mut self, id: SubscriptionId, code: SubscribeErrorCode, message: &str) {
        let Some(flight) = self.pending.remove(&id) else {
            return;
        };
        self.resolving.remove(&flight.key);
        self.record_call(Operation::SubscribeRoute, Outcome::Error);
        if flight.waiters.is_empty() && code != SubscribeErrorCode::Unavailable {
            // The control plane refuses this host, so its cached rule set
            // has nothing left to keep it current.
            let displaced = self.table.remove(&flight.key);
            self.release(displaced);
        }
        for waiter in flight.waiters {
            self.effects.replies.push((
                waiter,
                Err(FrontlineRouteCoordinatorError::Resolve {
                    code,
                    message: message.to_owned(),
                }),
            ));
        }
    }

    fn on_resolve(
        &mut self,
        key: HostKey,
        host: RouteHost,
        stale_version: Option<u64>,
        reply: FlightReply<Wake::Error>,
    ) {
        let current = self
            .table
            .get(&key)
            .filter(|entry| !entry.is_expired(clock()))
            .map(|entry| entry.version);
        if current.is_some() && current != stale_version {
            self.effects.replies.push((reply, Ok(())));
            return;
        }
        if let Some(flight) = self
            .resolving
            .get(&key)
            .and_then(|id| self.pending.get_mut(id))
        {
            flight.background = false;
            flight.waiters.push(reply);
            return;
        }
        if self.pending.len() >= MAX_ROUTE_FLIGHTS {
            self.effects
                .replies
                .push((reply, Err(FrontlineRouteCoordinatorError::Saturated)));
            return;
        }
        self.start_flight(key, host, vec![reply]);
    }

    fn start_flight(
        &mut self,
        key: HostKey,
        host: RouteHost,
        waiters: Vec<FlightReply<Wake::Error>>,
    ) {
        self.next_subscription += 1;
        let id = self.next_subscription;
        if matches!(self.session, Session::Open(_)) {
            self.outbox.push_back(RouteStreamRequest::Subscribe {
                id,
                kind: key.kind,
                host: host.clone(),
            });
        }
        self.resolving.insert(key.clone(), id);
        self.pending.insert(
            id,
            Flight {
                key,
                host,
                deadline: tokio::time::Instant::now() + SUBSCRIBE_DEADLINE,
                background: waiters.is_empty(),
                waiters,
                exhausted: 0,
            },
        );
    }

    /// Starts a flight that keeps a cached host current, within the
    /// background budget.
    fn refresh(&mut self, key: HostKey) -> bool {
        if self.resolving.contains_key(&key) {
            return true;
        }
        let background = self
            .pending
            .values()
            .filter(|flight| flight.background)
            .count();
        if background >= MAX_BACKGROUND_FLIGHTS || self.pending.len() >= MAX_ROUTE_FLIGHTS {
            return false;
        }
        let Ok(host) = RouteHost::exact(&*key.host) else {
            return true;
        };
        self.start_flight(key, host, Vec::new());
        true
    }

    fn register_carried_hosts(&mut self) {
        if !matches!(self.session, Session::Open(_)) {
            return;
        }
        while let Some(key) = self.registrations.pop_front() {
            if !self.table.awaits_registration(&key, clock()) {
                continue;
            }
            if !self.refresh(key.clone()) {
                self.registrations.push_front(key);
                return;
            }
        }
    }

    fn on_wake(&mut self, instance: WakeKey, host: HostKey) {
        let now = clock();
        let call = self
            .wakes
            .entry(instance.clone())
            .or_insert_with(|| WakeCall {
                in_flight: false,
                next_at: now,
                backoff: None,
                hosts: HashSet::new(),
            });
        call.hosts.insert(host);
        // A call is running or due later; the request waits for it.
        if call.in_flight || call.backoff.is_some() || now < call.next_at {
            return;
        }
        if self.wake_calls.len() >= MAX_WAKE_CALLS {
            self.wakes.remove(&instance);
            self.effects
                .failures
                .push((instance, FrontlineRouteCoordinatorError::Saturated));
            return;
        }
        self.call_wake(instance, now);
    }

    fn call_wake(&mut self, instance: WakeKey, now: Instant) {
        let Some(call) = self.wakes.get_mut(&instance) else {
            return;
        };
        call.in_flight = true;
        call.next_at = now + WAKE_RETRY_INTERVAL;
        let call = self.wake_client.wake_instance(WakeInstanceRequest {
            instance_id: instance.instance_id.clone(),
            expected_generation: instance.generation,
        });
        let deadline = self.wake_deadline;
        self.wake_calls.spawn(async move {
            let result = tokio::time::timeout(deadline, call).await.ok();
            (instance, result)
        });
    }

    fn on_wake_finished(&mut self, instance: WakeKey, result: WakeResult<Wake::Error>) {
        let Some(call) = self.wakes.get_mut(&instance) else {
            return;
        };
        call.in_flight = false;
        let response = match result {
            Some(Ok(response)) => response,
            Some(Err(error)) if !Wake::is_transient(&error) => {
                call.backoff = None;
                call.hosts.clear();
                self.record_call(Operation::WakeInstance, Outcome::Error);
                self.effects
                    .failures
                    .push((instance, FrontlineRouteCoordinatorError::Wake(error)));
                return;
            }
            failed => {
                // The control plane may be briefly out of reach. The requests
                // stay parked, and `maintain` calls again after the backoff.
                let backoff = call.backoff.unwrap_or(WAKE_BACKOFF);
                call.next_at = clock() + backoff;
                call.backoff = Some((backoff * 2).min(MAX_WAKE_BACKOFF));
                let outcome = match failed {
                    None => Outcome::Timeout,
                    Some(_) => Outcome::Error,
                };
                self.record_call(Operation::WakeInstance, outcome);
                return;
            }
        };
        call.backoff = None;
        let hosts = std::mem::take(&mut call.hosts);
        match response {
            WakeInstanceResponse::StillWaking { .. } => {
                self.record_call(Operation::WakeInstance, Outcome::Started);
            }
            response => {
                // Running, deleted or at another generation: the cached rule
                // set is behind, so each waiting host asks for its own again.
                self.record_call(
                    Operation::WakeInstance,
                    match response {
                        WakeInstanceResponse::AlreadyRunning { .. } => Outcome::AlreadyRunning,
                        _ => Outcome::Rejected,
                    },
                );
                for host in hosts {
                    self.refresh(host);
                }
            }
        }
    }

    /// Calls again for each instance whose wake failed transiently, while
    /// requests still wait on it.
    fn retry_wakes(&mut self, now: Instant) {
        let due: Vec<WakeKey> = self
            .wakes
            .iter()
            .filter(|(_, call)| call.backoff.is_some() && !call.in_flight && now >= call.next_at)
            .map(|(key, _)| key.clone())
            .collect();
        for key in due {
            if !self.state.parking.is_waited_on(&key) {
                // The last waiter left.
                self.wakes.remove(&key);
            } else if self.wake_calls.len() < MAX_WAKE_CALLS {
                self.call_wake(key, now);
            }
        }
    }

    fn maintain(&mut self) {
        let displaced = self.table.expire(clock(), EXPIRY_BATCH);
        self.release(displaced);

        let now = tokio::time::Instant::now();
        let mut ended = Vec::new();
        for (id, flight) in &mut self.pending {
            flight.waiters.retain(|waiter| !waiter.is_closed());
            let abandoned = !flight.background && flight.waiters.is_empty();
            if abandoned || now >= flight.deadline {
                ended.push(*id);
            }
        }
        for id in ended {
            let Some(flight) = self.pending.remove(&id) else {
                continue;
            };
            self.resolving.remove(&flight.key);
            if matches!(self.session, Session::Open(_)) {
                self.outbox
                    .push_back(RouteStreamRequest::Unsubscribe { id });
            }
            if !flight.waiters.is_empty() {
                self.record_call(Operation::SubscribeRoute, Outcome::Timeout);
            }
            for waiter in flight.waiters {
                self.effects.replies.push((
                    waiter,
                    Err(FrontlineRouteCoordinatorError::SubscribeDeadline),
                ));
            }
        }

        let wall = clock();
        self.retry_wakes(wall);
        self.wakes
            .retain(|_, call| call.in_flight || call.backoff.is_some() || wall < call.next_at);
    }

    /// Releases what a table change displaced: the control plane drops the
    /// subscriptions, and requests parked on the instances look again.
    fn release(&mut self, displaced: Displaced) {
        for entry in &displaced.entries {
            self.signal_instances(entry);
        }
        if matches!(self.session, Session::Open(_)) {
            for id in displaced.unsubscribe {
                self.outbox
                    .push_back(RouteStreamRequest::Unsubscribe { id });
                self.record_call(Operation::Unsubscribe, Outcome::Success);
            }
        }
    }

    fn signal_instances(&mut self, entry: &HostEntry) {
        self.effects
            .signals
            .extend(entry.rules.rules().iter().map(WakeKey::of));
    }

    fn flush_outbox(&mut self) {
        let Session::Open(stream) = &self.session else {
            return;
        };
        while let Some(request) = self.outbox.pop_front() {
            match stream.requests.try_send(request) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(request)) => {
                    self.outbox.push_front(request);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(request)) => {
                    // The reader reports how the stream ended.
                    self.outbox.push_front(request);
                    return;
                }
            }
        }
    }

    fn record_call(&self, operation: Operation, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![operation.metric_label(), outcome.metric_label()],
            1.0,
        ));
    }

    fn record_stream_event(&self, outcome: Outcome, fields: Vec<LogField>) {
        self.observability.record_metric(MetricObservation::new(
            RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL,
            vec![outcome.metric_label()],
            1.0,
        ));
        if outcome != Outcome::Updated {
            self.observability
                .record_log(LifecycleLogEvent::new(EVENT_SUBSCRIBE_STREAM, fields));
        }
    }
}

async fn session_event(session: &mut Session) -> SessionEvent {
    match session {
        Session::Idle { connect_at } => {
            tokio::time::sleep_until(*connect_at).await;
            SessionEvent::ConnectDue
        }
        Session::Connecting(connect) => SessionEvent::Connected(connect.await.ok().flatten()),
        Session::Open(stream) => SessionEvent::Stream(stream.events.recv().await),
    }
}

impl<W: fmt::Display> fmt::Display for FrontlineRouteCoordinatorError<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(f, "{error}"),
            Self::Resolve { code, message } => {
                write!(f, "route lookup failed ({code:?}): {message}")
            }
            Self::SubscribeDeadline => f.write_str("route lookup timed out"),
            Self::Wake(error) => write!(f, "wake request failed: {error}"),
            Self::WakeFailed => f.write_str("the instance failed to wake"),
            Self::RouteDeadline => f.write_str("route readiness timed out"),
            Self::Saturated => f.write_str("frontline route admission is saturated"),
            Self::RouteActorClosed => f.write_str("frontline route actor is closed"),
        }
    }
}

impl<W: fmt::Debug + fmt::Display> std::error::Error for FrontlineRouteCoordinatorError<W> {}

#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
