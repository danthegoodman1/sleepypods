use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use proxy_core::observability::{
    metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME,
    recorder::{InMemoryObservability, ObservabilityEvent},
};
use sleepypods_api::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
};

use super::{
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome, RouteFlight,
    SharedFrontlineRouteCoordinator, WakeClient, WakeClientFuture,
};
use crate::{
    ApplyUpdateOutcome, FrontlineRouteResolver, InvalidationReason, ReadyBackend, RouteRequestId,
    RouteSubscriptionClient, RouteSubscriptionFuture, SubscribeControlPlaneOutput, SubscriptionId,
    SubscriptionState, WakeInstanceRequest, WakeInstanceResponse, WakeResponseDisposition,
    WakeTracker, WakeUnavailableReason, WakeWaitReason,
};
use tokio::sync::{oneshot, Notify};

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestRouteClientError {
    SubscribeFailed,
}

impl fmt::Display for TestRouteClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubscribeFailed => f.write_str("subscribe failed"),
        }
    }
}

impl std::error::Error for TestRouteClientError {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestWakeClientError {
    WakeFailed,
}

impl fmt::Display for TestWakeClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WakeFailed => f.write_str("wake failed"),
        }
    }
}

impl std::error::Error for TestWakeClientError {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RouteClientCall {
    Subscribe {
        request_id: RouteRequestId,
        identity: RouteIdentity,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FakeRouteClient {
    calls: Vec<RouteClientCall>,
    subscribe_responses: VecDeque<Result<SubscribeControlPlaneOutput, TestRouteClientError>>,
}

impl FakeRouteClient {
    fn push_subscribe_response(&mut self, response: SubscribeControlPlaneOutput) {
        self.subscribe_responses.push_back(Ok(response));
    }

    fn push_subscribe_error(&mut self, error: TestRouteClientError) {
        self.subscribe_responses.push_back(Err(error));
    }
}

impl RouteSubscriptionClient for FakeRouteClient {
    type Error = TestRouteClientError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        self.calls.push(RouteClientCall::Subscribe {
            request_id,
            identity,
        });
        let response = self
            .subscribe_responses
            .pop_front()
            .expect("queued subscribe response");
        Box::pin(async move { response })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        self.calls
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, Default)]
struct BlockingRouteClient {
    calls: Arc<Mutex<Vec<RouteClientCall>>>,
    subscribe_responses: BlockingRouteResponses,
}

type BlockingRouteResponses = Arc<
    Mutex<VecDeque<oneshot::Receiver<Result<SubscribeControlPlaneOutput, TestRouteClientError>>>>,
>;

impl BlockingRouteClient {
    fn push_subscribe_response_channel(
        &self,
    ) -> oneshot::Sender<Result<SubscribeControlPlaneOutput, TestRouteClientError>> {
        let (tx, rx) = oneshot::channel();
        self.subscribe_responses
            .lock()
            .expect("blocking route responses lock")
            .push_back(rx);
        tx
    }

    async fn wait_for_call_count(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if self.calls.lock().expect("blocking route calls lock").len() >= expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for route call count");
    }

    fn calls(&self) -> Vec<RouteClientCall> {
        self.calls
            .lock()
            .expect("blocking route calls lock")
            .clone()
    }
}

impl RouteSubscriptionClient for BlockingRouteClient {
    type Error = TestRouteClientError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        self.calls
            .lock()
            .expect("blocking route calls lock")
            .push(RouteClientCall::Subscribe {
                request_id,
                identity,
            });
        let response = self
            .subscribe_responses
            .lock()
            .expect("blocking route responses lock")
            .pop_front()
            .expect("queued blocking route response");
        Box::pin(async move { response.await.expect("route response sent") })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        self.calls
            .lock()
            .expect("blocking route calls lock")
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FakeWakeClient {
    calls: Vec<WakeInstanceRequest>,
    responses: VecDeque<Result<WakeInstanceResponse, TestWakeClientError>>,
}

#[derive(Clone, Debug, Default)]
struct BlockingWakeClient {
    calls: Arc<Mutex<Vec<WakeInstanceRequest>>>,
    responses: BlockingWakeResponses,
    wake_started: Arc<Notify>,
}

type BlockingWakeResponses =
    Arc<Mutex<VecDeque<oneshot::Receiver<Result<WakeInstanceResponse, TestWakeClientError>>>>>;

impl FakeWakeClient {
    fn push_response(&mut self, response: WakeInstanceResponse) {
        self.responses.push_back(Ok(response));
    }

    fn push_error(&mut self, error: TestWakeClientError) {
        self.responses.push_back(Err(error));
    }
}

impl BlockingWakeClient {
    fn push_response_channel(
        &self,
    ) -> oneshot::Sender<Result<WakeInstanceResponse, TestWakeClientError>> {
        let (tx, rx) = oneshot::channel();
        self.responses
            .lock()
            .expect("blocking wake responses lock")
            .push_back(rx);
        tx
    }

    async fn wait_for_call_count(&self, expected: usize) {
        loop {
            if self.calls.lock().expect("blocking wake calls lock").len() >= expected {
                return;
            }
            self.wake_started.notified().await;
        }
    }

    fn calls(&self) -> Vec<WakeInstanceRequest> {
        self.calls.lock().expect("blocking wake calls lock").clone()
    }
}

impl WakeClient for BlockingWakeClient {
    type Error = TestWakeClientError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        self.calls
            .lock()
            .expect("blocking wake calls lock")
            .push(request);
        self.wake_started.notify_waiters();
        let response = self
            .responses
            .lock()
            .expect("blocking wake responses lock")
            .pop_front()
            .expect("queued blocking wake response");
        Box::pin(async move { response.await.expect("wake response sent") })
    }
}

impl WakeClient for FakeWakeClient {
    type Error = TestWakeClientError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        self.calls.push(request);
        let response = self.responses.pop_front().expect("queued wake response");
        Box::pin(async move { response })
    }
}

fn now() -> Instant {
    Instant::now()
}

fn ttl(seconds: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_secs(seconds))
}

fn request_id(value: &str) -> RouteRequestId {
    RouteRequestId::new(value).expect("request ID")
}

fn generated_request_id(index: u64) -> RouteRequestId {
    request_id(&format!("req:{index}"))
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription")
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

fn route_binding_id(value: &str) -> RouteBindingId {
    RouteBindingId::new(value).expect("route binding ID")
}

fn backend(generation: u64) -> BackendEndpoint {
    BackendEndpoint::new(format!("http://10.0.0.{generation}:8080")).expect("backend")
}

fn http_request(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: Some(PathPrefix::new(path).expect("valid path")),
    }
}

fn http_rule(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(host).expect("valid host"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path")),
    }
}

fn route_entry(
    state: InstanceState,
    instance_generation: u64,
    backend_generation: Option<u64>,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: route_binding_id("route-a"),
        instance_id: instance_id("instance-a"),
        instance_state: state,
        instance_generation: Generation::new(instance_generation),
        backend: backend_generation.map(backend),
        backend_generation: backend_generation.map(BackendGeneration::new),
    }
}

fn resolved_response(
    request_id: RouteRequestId,
    subscription_id: SubscriptionId,
    matched_identity: RouteIdentity,
    entry: RouteEntry,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteResolved {
        request_id,
        subscription_id,
        matched_identity,
        entry,
        cache_policy: ttl(30),
    }
}

fn miss_response(
    request_id: RouteRequestId,
    identity: RouteIdentity,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteMiss {
        request_id,
        request_identity: identity,
        negative_cache_policy: ttl(30),
    }
}

fn wake_request(generation: u64) -> WakeInstanceRequest {
    WakeInstanceRequest {
        instance_id: instance_id("instance-a"),
        expected_generation: Generation::new(generation),
    }
}

fn ready_wake_response(generation: u64, backend_generation: u64) -> WakeInstanceResponse {
    WakeInstanceResponse::AlreadyRunning {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(generation),
        backend: backend(backend_generation),
        backend_generation: Some(BackendGeneration::new(backend_generation)),
    }
}

fn coordinator_with_state(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    wake_client: FakeWakeClient,
) -> FrontlineRouteCoordinator<FakeRouteClient, FakeWakeClient> {
    FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::from_parts(state, route_client),
        WakeTracker::new(),
        wake_client,
    )
}

fn coordinator_with_wake_client<Wake>(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    wake_client: Wake,
) -> FrontlineRouteCoordinator<FakeRouteClient, Wake> {
    FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::from_parts(state, route_client),
        WakeTracker::new(),
        wake_client,
    )
}

async fn wait_for_shared_flight_ref_count(
    shared: &SharedFrontlineRouteCoordinator<BlockingRouteClient, FakeWakeClient>,
    identity: &RouteIdentity,
    expected: usize,
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let count = shared
                .flights
                .lock()
                .await
                .get(identity)
                .map(Arc::strong_count)
                .unwrap_or_default();
            if count >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for shared route waiters");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_flight_waiters_do_not_lose_completion_notification() {
    for _ in 0..10_000 {
        let flight = Arc::new(RouteFlight::<TestRouteClientError, TestWakeClientError>::new());
        let waiter = {
            let flight = flight.clone();
            tokio::spawn(async move { flight.wait().await })
        };
        tokio::task::yield_now().await;
        flight
            .complete(Err(FrontlineRouteCoordinatorError::RouteActorClosed))
            .await;

        // This hammers the waiter/completer interleaving that used to lose a
        // Notify wakeup between checking the result and registering interest.
        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("completed flight wait does not hang")
            .expect("waiter task joins");
        assert!(matches!(
            result,
            Err(FrontlineRouteCoordinatorError::RouteActorClosed)
        ));
    }
}

#[tokio::test]
async fn running_backend_cached_returns_ready_without_subscribe_or_wake() {
    let started_at = now();
    let request = http_request("app.example.com", "/api/users");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", Some("/api")),
            route_entry(InstanceState::Running, 7, Some(3)),
        ),
        started_at,
    );
    let mut coordinator =
        coordinator_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());

    let outcome = coordinator
        .route(request, started_at)
        .await
        .expect("cached route is ready");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert!(coordinator.resolver().client().calls.is_empty());
    assert!(coordinator.wake_client().calls.is_empty());
}

#[tokio::test]
async fn cold_lazy_subscribe_wakes_updates_cache_and_next_route_is_hot() {
    let now = now();
    let request = http_request("app.example.com", "/api/users");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-1"),
        http_rule("example.com", Some("/api")),
        route_entry(InstanceState::Cold, 4, None),
    ));
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(4, 9));
    let mut coordinator = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        wake_client,
    );

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("cold route wakes");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(
        coordinator.resolver().client().calls,
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request.clone(),
        }]
    );
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(4)]);
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    let outcome = coordinator
        .route(request, now)
        .await
        .expect("ready wake was cached");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(coordinator.resolver().client().calls.len(), 1);
    assert_eq!(coordinator.wake_client().calls.len(), 1);
}

#[tokio::test]
async fn shared_route_hot_hits_do_not_wait_for_in_flight_cold_wake() {
    let started_at = now();
    let hot = http_request("hot.example.com", "/");
    let cold = http_request("cold.example.com", "/");
    let mut state = SubscriptionState::new(8);
    state.apply_control_plane_message(
        resolved_response(
            request_id("hot-initial"),
            subscription_id("sub-hot"),
            hot.clone(),
            route_entry(InstanceState::Running, 7, Some(3)),
        ),
        started_at,
    );
    state.apply_control_plane_message(
        resolved_response(
            request_id("cold-initial"),
            subscription_id("sub-cold"),
            cold.clone(),
            route_entry(InstanceState::Cold, 8, None),
        ),
        started_at,
    );
    let wake_client = BlockingWakeClient::default();
    let release_wake = wake_client.push_response_channel();
    let shared =
        coordinator_with_wake_client(state, FakeRouteClient::default(), wake_client.clone())
            .into_shared();

    // The cold route owns the actor while the hot route should resolve from the
    // shared read cache without waiting for the wake response.
    let cold_shared = shared.clone();
    let cold_task = tokio::spawn(async move { cold_shared.route(cold, now()).await });
    wake_client.wait_for_call_count(1).await;

    let mut hot_tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let shared = shared.clone();
        let hot = hot.clone();
        hot_tasks.spawn(async move { shared.route(hot, now()).await });
    }

    while let Some(result) = hot_tasks.join_next().await {
        let outcome = result
            .expect("hot route task joins")
            .expect("hot route resolves");
        assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    }
    assert_eq!(wake_client.calls(), vec![wake_request(8)]);

    release_wake
        .send(Ok(ready_wake_response(8, 10)))
        .expect("wake response is received");
    let cold_outcome = cold_task
        .await
        .expect("cold route task joins")
        .expect("cold route completes");
    assert!(matches!(cold_outcome, FrontlineRouteOutcome::Ready(_)));
}

#[tokio::test]
async fn shared_route_same_identity_uses_one_in_flight_subscribe_route() {
    let route_client = BlockingRouteClient::default();
    let release_subscribe = route_client.push_subscribe_response_channel();
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client.clone()),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let identity = http_request("single-flight.example.com", "/same");

    let mut tasks = Vec::new();
    for _ in 0..3 {
        let shared = shared.clone();
        let identity = identity.clone();
        tasks.push(tokio::spawn(
            async move { shared.route(identity, now()).await },
        ));
    }
    route_client.wait_for_call_count(1).await;
    wait_for_shared_flight_ref_count(&shared, &identity, 5).await;

    // All same-identity waiters should attach to the explicit RouteFlight while
    // the first subscribe is still blocked, so only one control-plane request is issued.
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: identity.clone(),
        }]
    );

    release_subscribe
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("sub-single-flight"),
            identity.clone(),
            route_entry(InstanceState::Running, 7, Some(3)),
        )))
        .expect("subscribe response is received");

    for task in tasks {
        let outcome = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("shared route completes")
            .expect("shared route task joins")
            .expect("shared route succeeds");
        assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    }
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity,
        }]
    );
}

#[tokio::test]
async fn shared_route_cancelled_mid_wake_does_not_strand_recovery() {
    let started_at = now();
    let cold = http_request("cancel-cold.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("cold-initial"),
            subscription_id("sub-cancel-cold"),
            cold.clone(),
            route_entry(InstanceState::Cold, 9, None),
        ),
        started_at,
    );
    let wake_client = BlockingWakeClient::default();
    let release_wake = wake_client.push_response_channel();
    let shared =
        coordinator_with_wake_client(state, FakeRouteClient::default(), wake_client.clone())
            .into_shared();

    // Dropping this waiter simulates the HTTP request going away. The actor-owned
    // wake must still complete and publish the ready route for later callers.
    let first_shared = shared.clone();
    let first = tokio::spawn(async move { first_shared.route(cold.clone(), now()).await });
    wake_client.wait_for_call_count(1).await;
    first.abort();

    release_wake
        .send(Ok(ready_wake_response(9, 11)))
        .expect("wake response is received");

    let recovered = tokio::time::timeout(
        Duration::from_secs(1),
        shared.route(http_request("cancel-cold.example.com", "/"), now()),
    )
    .await
    .expect("later route does not hang")
    .expect("later route resolves");

    assert!(matches!(recovered, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(wake_client.calls(), vec![wake_request(9)]);
}

#[tokio::test]
async fn cold_wake_records_control_plane_wake_call_metric() {
    let now = now();
    let sink = InMemoryObservability::default();
    let request = http_request("app.example.com", "/api/users");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-1"),
        http_rule("example.com", Some("/api")),
        route_entry(InstanceState::Cold, 4, None),
    ));
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(4, 9));
    let mut coordinator = FrontlineRouteCoordinator::with_observability(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        wake_client,
        sink.recorder(),
    );

    coordinator
        .route(request, now)
        .await
        .expect("cold route wakes");

    assert!(sink.events().iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "wake_instance")
                && metric.labels().iter().any(|label| label.value() == "success")
    )));
}

#[tokio::test]
async fn route_miss_returns_miss_without_wake() {
    let now = now();
    let request = http_request("missing.example.com", "/");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(miss_response(generated_request_id(1), request.clone()));
    let mut coordinator = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        FakeWakeClient::default(),
    );

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("route miss");

    let FrontlineRouteOutcome::Miss(entry) = outcome else {
        panic!("expected miss");
    };
    assert_eq!(entry.request_identity, request);
    assert!(coordinator.wake_client().calls.is_empty());
}

#[tokio::test]
async fn running_route_without_backend_triggers_wake() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Running, 5, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(5, 1));
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let outcome = coordinator
        .route(request, now)
        .await
        .expect("missing backend wakes");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(5)]);
}

#[tokio::test]
async fn waking_route_without_local_pending_wake_resumes_via_control_plane() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Waking, 6, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(7, 2));
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("waking route resumes wake");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(6)]);
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    let cached = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("ready wake was cached");
    assert_eq!(cached.entry.instance_state, InstanceState::Running);
    assert_eq!(cached.entry.instance_generation, Generation::new(7));
    assert_eq!(
        cached.entry.backend_generation,
        Some(BackendGeneration::new(2))
    );
}

#[tokio::test]
async fn waking_route_with_local_pending_wake_waits_without_second_wake_call() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Waking, 6, None),
        ),
        now,
    );
    let mut coordinator =
        coordinator_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());
    coordinator.wake_tracker_mut().admit(wake_request(6));

    let outcome = coordinator.route(request, now).await.expect("waits");

    let FrontlineRouteOutcome::Waiting(wait) = outcome else {
        panic!("expected waiting outcome");
    };
    assert_eq!(wait.reason, WakeWaitReason::DuplicatePendingWake);
    assert!(coordinator.wake_client().calls.is_empty());
}

#[tokio::test]
async fn deleting_and_deleted_routes_are_unavailable_without_wake_call() {
    for (state, reason) in [
        (InstanceState::Deleting, WakeUnavailableReason::Deleting),
        (InstanceState::Deleted, WakeUnavailableReason::Deleted),
    ] {
        let now = now();
        let request = http_request("app.example.com", "/");
        let mut subscription_state = SubscriptionState::new(4);
        subscription_state.apply_resolved_response(
            request.clone(),
            resolved_response(
                request_id("initial"),
                subscription_id("sub-1"),
                http_rule("example.com", None),
                route_entry(state, 6, None),
            ),
            now,
        );
        let mut coordinator = coordinator_with_state(
            subscription_state,
            FakeRouteClient::default(),
            FakeWakeClient::default(),
        );

        let outcome = coordinator.route(request, now).await.expect("unavailable");

        let FrontlineRouteOutcome::Unavailable(unavailable) = outcome else {
            panic!("expected unavailable outcome");
        };
        assert_eq!(unavailable.reason, reason);
        assert!(coordinator.wake_client().calls.is_empty());
    }
}

#[tokio::test]
async fn accepted_wake_response_releases_rpc_tracker() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 8, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(8),
    });
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let first = coordinator
        .route(request.clone(), now)
        .await
        .expect("wake starts");
    assert!(matches!(first, FrontlineRouteOutcome::Waking { .. }));
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    assert_eq!(coordinator.wake_client().calls.len(), 1);
}

#[tokio::test]
async fn stale_wake_response_is_rejected_clears_pending_and_does_not_update_cache() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 9, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(8, 1));
    wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(9),
    });
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("stale response rejected");

    assert!(matches!(
        outcome,
        FrontlineRouteOutcome::RejectedWakeObservation(_)
    ));
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    let cached = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("cached route");
    assert_eq!(cached.entry.instance_state, InstanceState::Cold);
    assert!(cached.entry.backend.is_none());

    let retry = coordinator
        .route(request, now)
        .await
        .expect("pending cleared for retry");

    assert!(matches!(retry, FrontlineRouteOutcome::Waking { .. }));
    assert_eq!(coordinator.wake_client().calls.len(), 2);
}

#[tokio::test]
async fn stale_cold_wake_conflict_then_refreshed_waking_route_resumes_wake() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let matched_identity = http_rule("example.com", None);
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            matched_identity.clone(),
            route_entry(InstanceState::Cold, 5, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(WakeInstanceResponse::GenerationConflict {
        instance_id: instance_id("instance-a"),
        expected_generation: Generation::new(5),
        actual_generation: Generation::new(6),
    });
    wake_client.push_response(ready_wake_response(7, 2));
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let conflict = coordinator
        .route(request.clone(), now)
        .await
        .expect("stale cold wake returns conflict");

    let FrontlineRouteOutcome::GenerationConflict {
        expected_generation,
        actual_generation,
        ..
    } = conflict
    else {
        panic!("expected generation conflict");
    };
    assert_eq!(expected_generation, Generation::new(5));
    assert_eq!(actual_generation, Generation::new(6));
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(5)]);
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    coordinator
        .resolver_mut()
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteUpdated {
                subscription_id: subscription_id("sub-1"),
                matched_identity,
                entry: route_entry(InstanceState::Waking, 6, None),
                cache_policy: ttl(30),
            },
            now,
        )
        .await
        .expect("refreshed waking route applies");

    let ready = coordinator
        .route(request, now)
        .await
        .expect("refreshed waking route resumes wake");

    assert!(matches!(ready, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(
        coordinator.wake_client().calls,
        vec![wake_request(5), wake_request(6)]
    );
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    let cached = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("ready route cached");
    assert_eq!(cached.entry.instance_state, InstanceState::Running);
    assert_eq!(cached.entry.instance_generation, Generation::new(7));
    assert_eq!(
        cached.entry.backend_generation,
        Some(BackendGeneration::new(2))
    );
}

#[tokio::test]
async fn wake_failed_unavailable_and_generation_conflict_map_to_typed_outcomes() {
    let cases = [
        (
            WakeInstanceResponse::Failed {
                instance_id: instance_id("instance-a"),
                generation: Generation::new(10),
                reason: "boom".to_owned(),
            },
            "failed",
        ),
        (
            WakeInstanceResponse::Unavailable {
                instance_id: instance_id("instance-a"),
                generation: Generation::new(10),
                reason: "gone".to_owned(),
            },
            "unavailable",
        ),
        (
            WakeInstanceResponse::GenerationConflict {
                instance_id: instance_id("instance-a"),
                expected_generation: Generation::new(10),
                actual_generation: Generation::new(11),
            },
            "conflict",
        ),
    ];

    for (response, expected) in cases {
        let now = now();
        let request = http_request("app.example.com", "/");
        let mut state = SubscriptionState::new(4);
        state.apply_resolved_response(
            request.clone(),
            resolved_response(
                request_id("initial"),
                subscription_id("sub-1"),
                http_rule("example.com", None),
                route_entry(InstanceState::Cold, 10, None),
            ),
            now,
        );
        let mut wake_client = FakeWakeClient::default();
        wake_client.push_response(response);
        let mut coordinator =
            coordinator_with_state(state, FakeRouteClient::default(), wake_client);

        let outcome = coordinator.route(request, now).await.expect("wake outcome");

        match (expected, outcome) {
            ("failed", FrontlineRouteOutcome::WakeFailed { reason, .. }) => {
                assert_eq!(reason, "boom");
            }
            ("unavailable", FrontlineRouteOutcome::WakeUnavailable { reason, .. }) => {
                assert_eq!(reason, "gone");
            }
            (
                "conflict",
                FrontlineRouteOutcome::GenerationConflict {
                    expected_generation,
                    actual_generation,
                    ..
                },
            ) => {
                assert_eq!(expected_generation, Generation::new(10));
                assert_eq!(actual_generation, Generation::new(11));
            }
            _ => panic!("unexpected outcome"),
        }
        assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    }
}

#[tokio::test]
async fn wake_client_error_surfaces_and_clears_pending_for_retry() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 11, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_error(TestWakeClientError::WakeFailed);
    wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(11),
    });
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let error = coordinator
        .route(request.clone(), now)
        .await
        .expect_err("wake client error");

    assert!(matches!(error, FrontlineRouteCoordinatorError::Wake(_)));
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    let retry = coordinator
        .route(request, now)
        .await
        .expect("retry calls wake again");

    assert!(matches!(retry, FrontlineRouteOutcome::Waking { .. }));
    assert_eq!(coordinator.wake_client().calls.len(), 2);
}

#[tokio::test]
async fn invalidation_during_wake_rejects_ready_update_and_leaves_cache_empty() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 12, None),
        ),
        now,
    );
    let mut coordinator =
        coordinator_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());
    let observed_entry = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("observed cache entry before wake")
        .clone();

    coordinator.wake_tracker_mut().admit(wake_request(12));
    assert_eq!(coordinator.wake_tracker().pending_len(), 1);
    coordinator
        .resolver_mut()
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-1"),
                reason: InvalidationReason::StreamClosed,
            },
            now,
        )
        .await
        .expect("stream invalidation applies");

    let error = coordinator
        .handle_wake_response(
            observed_entry,
            WakeResponseDisposition::Ready(ReadyBackend {
                instance_id: instance_id("instance-a"),
                instance_generation: Generation::new(12),
                backend: backend(5),
                backend_generation: Some(BackendGeneration::new(5)),
            }),
            now,
        )
        .await
        .expect_err("ready observation cannot update invalidated subscription");

    assert_eq!(
        error,
        FrontlineRouteCoordinatorError::RejectedCacheUpdate(
            ApplyUpdateOutcome::MissingSubscription
        )
    );
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    assert!(coordinator.resolver().state().cache().is_empty());
}

#[tokio::test]
async fn route_after_stream_loss_lazily_resubscribes_and_rebuilds_cache() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-old"),
            http_rule("example.com", None),
            route_entry(InstanceState::Running, 13, Some(1)),
        ),
        now,
    );
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-new"),
        http_rule("example.com", None),
        route_entry(InstanceState::Running, 13, Some(2)),
    ));
    let mut coordinator = coordinator_with_state(state, route_client, FakeWakeClient::default());

    coordinator
        .resolver_mut()
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-old"),
                reason: InvalidationReason::StreamClosed,
            },
            now,
        )
        .await
        .expect("stream invalidation removes active subscription");

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("route lazily rebuilds after stream loss");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(
        coordinator.resolver().client().calls,
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request,
        }]
    );
    assert!(coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-old"))
        .is_none());
    assert_eq!(
        coordinator
            .resolver()
            .state()
            .cache()
            .positive_by_subscription(&subscription_id("sub-new"))
            .expect("rebuilt subscription")
            .entry
            .backend_generation,
        Some(BackendGeneration::new(2))
    );
}

#[tokio::test]
async fn route_resolver_error_surfaces_without_wake_call() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_error(TestRouteClientError::SubscribeFailed);
    let mut coordinator = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        FakeWakeClient::default(),
    );

    let error = coordinator
        .route(request, now)
        .await
        .expect_err("resolver error");

    assert!(matches!(error, FrontlineRouteCoordinatorError::Resolve(_)));
    assert!(coordinator.wake_client().calls.is_empty());
}

#[tokio::test]
async fn review_probe_wildcard_cache_must_not_hide_preexisting_exact_route() {
    let mut client = FakeRouteClient::default();
    client.push_subscribe_response(resolved_response(
        request_id("req:1"),
        subscription_id("broad"),
        http_rule("example.com", None),
        route_entry(InstanceState::Running, 7, Some(1)),
    ));
    client.push_subscribe_response(resolved_response(
        request_id("req:2"),
        subscription_id("exact"),
        http_request("private.example.com", "/"),
        route_entry(InstanceState::Running, 7, Some(2)),
    ));
    let mut coordinator =
        coordinator_with_state(SubscriptionState::new(8), client, FakeWakeClient::default());
    coordinator
        .route(http_request("public.example.com", "/"), now())
        .await
        .unwrap();
    let outcome = coordinator
        .route(http_request("private.example.com", "/"), now())
        .await
        .unwrap();
    let FrontlineRouteOutcome::Ready(actual) = outcome else {
        panic!("expected ready");
    };
    assert_eq!(
        actual.backend,
        backend(2),
        "the control plane has an exact route, but the broad cache suppresses its lookup"
    );
}

#[tokio::test]
async fn review_probe_root_path_cache_must_not_hide_preexisting_specific_route() {
    let mut client = FakeRouteClient::default();
    client.push_subscribe_response(resolved_response(
        request_id("req:1"),
        subscription_id("root"),
        http_request("app.example.com", "/"),
        route_entry(InstanceState::Running, 7, Some(1)),
    ));
    client.push_subscribe_response(resolved_response(
        request_id("req:2"),
        subscription_id("private"),
        http_request("app.example.com", "/private"),
        route_entry(InstanceState::Running, 7, Some(2)),
    ));
    let mut coordinator =
        coordinator_with_state(SubscriptionState::new(8), client, FakeWakeClient::default());
    coordinator
        .route(http_request("app.example.com", "/"), now())
        .await
        .unwrap();
    let outcome = coordinator
        .route(http_request("app.example.com", "/private"), now())
        .await
        .unwrap();
    let FrontlineRouteOutcome::Ready(actual) = outcome else {
        panic!("expected ready");
    };
    assert_eq!(
        actual.backend,
        backend(2),
        "the root cache suppresses the more specific path lookup"
    );
}

#[derive(Clone)]
struct ReviewEventClient {
    events: Arc<Mutex<Vec<crate::RouteSubscriptionEvent>>>,
}
impl RouteSubscriptionClient for ReviewEventClient {
    type Error = TestRouteClientError;
    fn subscribe_route(
        &mut self,
        id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        Box::pin(async move { Ok(miss_response(id, identity)) })
    }
    fn unsubscribe(
        &mut self,
        _: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }
    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'static, Vec<crate::RouteSubscriptionEvent>, Self::Error> {
        let events = std::mem::take(&mut *self.events.lock().unwrap());
        Box::pin(async move { Ok(events) })
    }
}

#[tokio::test]
async fn review_probe_hot_invalidation_progresses_during_cold_wake() {
    let mut state = SubscriptionState::new(8);
    let hot = http_request("hot.example.com", "/");
    let cold = http_request("cold.example.com", "/");
    state.apply_control_plane_message(
        resolved_response(
            request_id("hot"),
            subscription_id("sub-hot"),
            hot.clone(),
            route_entry(InstanceState::Running, 7, Some(3)),
        ),
        now(),
    );
    state.apply_control_plane_message(
        resolved_response(
            request_id("cold"),
            subscription_id("sub-cold"),
            cold.clone(),
            route_entry(InstanceState::Cold, 8, None),
        ),
        now(),
    );
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = ReviewEventClient {
        events: events.clone(),
    };
    let wake = BlockingWakeClient::default();
    let _release = wake.push_response_channel();
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::from_parts(state, client),
        WakeTracker::new(),
        wake.clone(),
    )
    .into_shared();
    let cold_shared = shared.clone();
    let _cold_task = tokio::spawn(async move { cold_shared.route(cold, now()).await });
    tokio::time::timeout(Duration::from_secs(1), wake.wait_for_call_count(1))
        .await
        .unwrap();
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::Update(Box::new(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-hot"),
                reason: InvalidationReason::RouteRemoved,
            },
        )));
    tokio::time::sleep(Duration::from_millis(350)).await;
    let outcome = tokio::time::timeout(Duration::from_millis(100), shared.route(hot, now()))
        .await
        .unwrap()
        .unwrap();
    assert!(!matches!(outcome, FrontlineRouteOutcome::Ready(_)), "hot backend remains routable after invalidation because the actor is waiting on another wake");
}

#[derive(Clone)]
struct CompleteAuthority {
    routes: Arc<Mutex<Vec<(RouteIdentity, RouteEntry)>>>,
    events: Arc<Mutex<Vec<crate::RouteSubscriptionEvent>>>,
}
impl RouteSubscriptionClient for CompleteAuthority {
    type Error = TestRouteClientError;
    fn subscribe_route(
        &mut self,
        id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        let matched = self
            .routes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(rule, entry)| {
                crate::matcher::rank_match(&identity, rule)
                    .map(|rank| (rank, rule.clone(), entry.clone()))
            })
            .max_by_key(|(rank, _, _)| *rank);
        let response = match matched {
            Some((_, matched, entry)) => resolved_response(
                id.clone(),
                subscription_id(&format!("sub-{}", id.as_str())),
                matched,
                entry,
            ),
            None => miss_response(id, identity),
        };
        Box::pin(async move { Ok(response) })
    }
    fn unsubscribe(
        &mut self,
        _: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }
    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'static, Vec<crate::RouteSubscriptionEvent>, Self::Error> {
        let events = std::mem::take(&mut *self.events.lock().unwrap());
        Box::pin(async move { Ok(events) })
    }
}
#[tokio::test]
async fn partial_cache_matches_complete_authority_for_http_sni_and_eviction_orders() {
    let cases = [
        (http_rule("example.com", None), 1),
        (http_request("private.example.com", "/"), 2),
        (http_request("app.example.com", "/"), 3),
        (http_request("app.example.com", "/private"), 4),
        (
            RouteIdentity::Sni {
                host: RouteHost::wildcard_suffix("example.com").unwrap(),
            },
            5,
        ),
        (
            RouteIdentity::Sni {
                host: RouteHost::exact("db.example.com").unwrap(),
            },
            6,
        ),
        (
            RouteIdentity::Sni {
                host: RouteHost::wildcard_suffix("nested.example.com").unwrap(),
            },
            7,
        ),
    ];
    let requests = [
        (http_request("public.example.com", "/"), 1),
        (http_request("private.example.com", "/"), 2),
        (http_request("app.example.com", "/public"), 3),
        (http_request("app.example.com", "/private/x"), 4),
        (
            RouteIdentity::Sni {
                host: RouteHost::exact("public.example.com").unwrap(),
            },
            5,
        ),
        (
            RouteIdentity::Sni {
                host: RouteHost::exact("db.example.com").unwrap(),
            },
            6,
        ),
        (
            RouteIdentity::Sni {
                host: RouteHost::exact("db.nested.example.com").unwrap(),
            },
            7,
        ),
    ];
    for capacity in [1, 3, 16] {
        let client = CompleteAuthority {
            routes: Arc::new(Mutex::new(
                cases
                    .iter()
                    .map(|(identity, b)| {
                        (
                            identity.clone(),
                            route_entry(InstanceState::Running, 7, Some(*b)),
                        )
                    })
                    .collect(),
            )),
            events: Arc::new(Mutex::new(Vec::new())),
        };
        let shared = FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::new(capacity, client),
            WakeTracker::new(),
            FakeWakeClient::default(),
        )
        .into_shared();
        // Coprime strides exercise different warmup/eviction permutations.
        for stride in [1, 3, 5] {
            for index in 0..requests.len() * 3 {
                let (identity, expected) = &requests[(index * stride) % requests.len()];
                let FrontlineRouteOutcome::Ready(actual) =
                    shared.route(identity.clone(), now()).await.unwrap()
                else {
                    panic!("expected ready")
                };
                assert_eq!(actual.backend, backend(*expected));
            }
        }
    }
}
#[tokio::test]
async fn new_specific_route_after_warmup_invalidates_the_original_query() {
    let rules = Arc::new(Mutex::new(vec![(
        http_rule("example.com", None),
        route_entry(InstanceState::Running, 7, Some(1)),
    )]));
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = CompleteAuthority {
        routes: rules.clone(),
        events: events.clone(),
    };
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(8, client),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let identity = http_request("private.example.com", "/");
    shared.route(identity.clone(), now()).await.unwrap();
    rules.lock().unwrap().push((
        identity.clone(),
        route_entry(InstanceState::Running, 7, Some(2)),
    ));
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::Update(Box::new(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-req:1"),
                reason: InvalidationReason::RouteChanged,
            },
        )));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let FrontlineRouteOutcome::Ready(actual) = shared.route(identity, now()).await.unwrap() else {
        panic!("ready")
    };
    assert_eq!(actual.backend, backend(2));
}
#[tokio::test]
async fn distinct_flights_are_admitted_before_spawn_and_shutdown_aborts_work() {
    let client = BlockingRouteClient::default();
    let mut replies = Vec::new();
    for _ in 0..super::MAX_ROUTE_FLIGHTS {
        replies.push(client.push_subscribe_response_channel());
    }
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(8, client.clone()),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let mut callers = Vec::new();
    for index in 0..super::MAX_ROUTE_FLIGHTS {
        let shared = shared.clone();
        callers.push(tokio::spawn(async move {
            shared
                .route(
                    http_request(&format!("host{index}.example.com"), "/"),
                    now(),
                )
                .await
        }));
    }
    client.wait_for_call_count(super::MAX_ROUTE_FLIGHTS).await;
    let overloaded = shared
        .route(http_request("overflow.example.com", "/"), now())
        .await;
    assert!(matches!(
        overloaded,
        Err(FrontlineRouteCoordinatorError::Saturated)
    ));
    assert_eq!(shared.flights.lock().await.len(), super::MAX_ROUTE_FLIGHTS);
    for caller in callers {
        caller.abort();
        let _ = caller.await;
    }
    let actor = shared._task.0.abort_handle();
    drop(shared);
    tokio::task::yield_now().await;
    assert!(actor.is_finished());
    tokio::time::timeout(Duration::from_secs(1), async {
        while replies.iter().any(|reply| !reply.is_closed()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown cancels all outstanding subscribe futures");
}
#[tokio::test]
async fn accepted_wake_waits_for_authoritative_readiness_and_releases_tracker() {
    let client = BlockingRouteClient::default();
    let first = client.push_subscribe_response_channel();
    let second = client.push_subscribe_response_channel();
    let mut wake = FakeWakeClient::default();
    wake.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(8),
    });
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(8, client.clone()),
        WakeTracker::new(),
        wake,
    )
    .into_shared();
    let identity = http_request("cold.example.com", "/");
    let caller = {
        let shared = shared.clone();
        let identity = identity.clone();
        tokio::spawn(async move { shared.route(identity, now()).await })
    };
    client.wait_for_call_count(1).await;
    first
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("cold"),
            identity.clone(),
            route_entry(InstanceState::Cold, 7, None),
        )))
        .unwrap();
    client.wait_for_call_count(3).await; // subscribe + unsubscribe + refresh subscribe
    assert!(
        !caller.is_finished(),
        "Accepted must not end the cold request"
    );
    second
        .send(Ok(resolved_response(
            generated_request_id(2),
            subscription_id("ready"),
            identity,
            route_entry(InstanceState::Running, 8, Some(2)),
        )))
        .unwrap();
    assert!(matches!(
        caller.await.unwrap().unwrap(),
        FrontlineRouteOutcome::Ready(_)
    ));
}

#[derive(Clone)]
struct BlockingEventClient {
    client: BlockingRouteClient,
    events: Arc<Mutex<Vec<crate::RouteSubscriptionEvent>>>,
}
impl RouteSubscriptionClient for BlockingEventClient {
    type Error = TestRouteClientError;
    fn subscribe_route(
        &mut self,
        id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        self.client.subscribe_route(id, identity)
    }
    fn unsubscribe(
        &mut self,
        id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        self.client.unsubscribe(id)
    }
    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'static, Vec<crate::RouteSubscriptionEvent>, Self::Error> {
        let events = std::mem::take(&mut *self.events.lock().unwrap());
        Box::pin(async move { Ok(events) })
    }
}
#[tokio::test]
async fn invalidation_progresses_during_a_blocked_subscribe() {
    let client = BlockingRouteClient::default();
    let _blocked = client.push_subscribe_response_channel();
    let hot_reply = client.push_subscribe_response_channel();
    let events = Arc::new(Mutex::new(Vec::new()));
    let hot = http_request("hot.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.cache_mut().insert_positive(
        subscription_id("hot"),
        hot.clone(),
        route_entry(InstanceState::Running, 7, Some(1)),
        ttl(30),
        now(),
    );
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::from_parts(
            state,
            BlockingEventClient {
                client: client.clone(),
                events: events.clone(),
            },
        ),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let blocked = {
        let shared = shared.clone();
        tokio::spawn(async move {
            shared
                .route(http_request("blocked.example.com", "/"), now())
                .await
        })
    };
    client.wait_for_call_count(1).await;
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::Update(Box::new(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("hot"),
                reason: InvalidationReason::RouteRemoved,
            },
        )));
    tokio::time::sleep(Duration::from_millis(30)).await;
    hot_reply
        .send(Ok(miss_response(generated_request_id(2), hot.clone())))
        .unwrap();
    let result = tokio::time::timeout(Duration::from_millis(100), shared.route(hot, now()))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, FrontlineRouteOutcome::Miss(_)));
    blocked.abort();
}
#[tokio::test]
async fn invalidation_before_response_install_cannot_resurrect_backend() {
    let client = BlockingRouteClient::default();
    let old = client.push_subscribe_response_channel();
    let new = client.push_subscribe_response_channel();
    let events = Arc::new(Mutex::new(Vec::new()));
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(
            4,
            BlockingEventClient {
                client: client.clone(),
                events: events.clone(),
            },
        ),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let identity = http_request("app.example.com", "/");
    let task = {
        let shared = shared.clone();
        let identity = identity.clone();
        tokio::spawn(async move { shared.route(identity, now()).await })
    };
    client.wait_for_call_count(1).await;
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::Update(Box::new(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("old"),
                reason: InvalidationReason::RouteChanged,
            },
        )));
    tokio::time::sleep(Duration::from_millis(30)).await;
    old.send(Ok(resolved_response(
        generated_request_id(1),
        subscription_id("old"),
        identity.clone(),
        route_entry(InstanceState::Running, 7, Some(1)),
    )))
    .unwrap();
    client.wait_for_call_count(3).await;
    assert!(
        client.calls().contains(&RouteClientCall::Unsubscribe {
            subscription_id: subscription_id("old"),
        }),
        "discarded successful subscription is reclaimed"
    );
    assert!(!task.is_finished());
    assert!(shared.local_route(&identity, now()).await.is_none());
    new.send(Ok(resolved_response(
        generated_request_id(2),
        subscription_id("new"),
        identity,
        route_entry(InstanceState::Running, 7, Some(2)),
    )))
    .unwrap();
    let FrontlineRouteOutcome::Ready(ready) = task.await.unwrap().unwrap() else {
        panic!("ready")
    };
    assert_eq!(ready.backend, backend(2));
}
#[tokio::test(start_paused = true)]
async fn readiness_deadline_bounds_cancelled_flights_and_pending_actor_tasks() {
    let client = BlockingRouteClient::default();
    let mut replies = Vec::new();
    for _ in 0..super::MAX_ROUTE_FLIGHTS {
        replies.push(client.push_subscribe_response_channel());
    }
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, client.clone()),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .with_route_deadline(Duration::from_millis(1))
    .into_shared();
    for index in 0..super::MAX_ROUTE_FLIGHTS {
        let result = shared
            .route(
                http_request(&format!("deadline{index}.example.com"), "/"),
                now(),
            )
            .await;
        assert!(matches!(
            result,
            Err(FrontlineRouteCoordinatorError::RouteDeadline)
        ));
    }
    let result = shared
        .route(http_request("excess.example.com", "/"), now())
        .await;
    assert!(matches!(
        result,
        Err(FrontlineRouteCoordinatorError::Saturated)
    ));
    assert_eq!(client.calls().len(), super::MAX_ROUTE_FLIGHTS);
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::task::yield_now().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while replies.iter().any(|reply| !reply.is_closed()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscribe deadlines release all pending work");
}

#[tokio::test]
async fn same_identity_waiters_are_bounded_and_recover_after_cancellation() {
    let client = BlockingRouteClient::default();
    let reply = client.push_subscribe_response_channel();
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, client.clone()),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let identity = http_request("waiters.example.com", "/");
    let mut callers = Vec::new();
    for _ in 0..super::MAX_ROUTE_WAITERS {
        let shared = shared.clone();
        let identity = identity.clone();
        callers.push(tokio::spawn(
            async move { shared.route(identity, now()).await },
        ));
    }
    client.wait_for_call_count(1).await;
    wait_for_shared_flight_ref_count(&shared, &identity, super::MAX_ROUTE_WAITERS + 2).await;
    assert!(matches!(
        shared.route(identity.clone(), now()).await,
        Err(FrontlineRouteCoordinatorError::Saturated)
    ));
    assert_eq!(client.calls().len(), 1);
    for caller in callers {
        caller.abort();
        let _ = caller.await;
    }
    reply
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("ready"),
            identity.clone(),
            route_entry(InstanceState::Running, 7, Some(3)),
        )))
        .unwrap();
    assert!(matches!(
        shared.route(identity, now()).await.unwrap(),
        FrontlineRouteOutcome::Ready(_)
    ));
}

#[tokio::test]
async fn cold_resolution_progresses_through_sustained_unrelated_updates() {
    let client = BlockingRouteClient::default();
    let reply = client.push_subscribe_response_channel();
    let events = Arc::new(Mutex::new(Vec::new()));
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(
            4,
            BlockingEventClient {
                client: client.clone(),
                events: events.clone(),
            },
        ),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .with_route_deadline(Duration::from_secs(1))
    .into_shared();
    let identity = http_request("cold.example.com", "/");
    let task = {
        let shared = shared.clone();
        let identity = identity.clone();
        tokio::spawn(async move { shared.route(identity, now()).await })
    };
    client.wait_for_call_count(1).await;
    // Keep delivering distinct unrelated events while the cold RPC is pending.
    // Any stream-wide event counter would reject its one configured response.
    for batch in 0..20 {
        for index in 0..64 {
            events
                .lock()
                .unwrap()
                .push(crate::RouteSubscriptionEvent::Update(Box::new(
                    SubscribeControlPlaneOutput::RouteInvalidated {
                        subscription_id: subscription_id(&format!(
                            "unrelated-{}",
                            batch * 64 + index
                        )),
                        reason: InvalidationReason::BackendChanged,
                    },
                )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    reply
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("cold"),
            identity,
            route_entry(InstanceState::Running, 7, Some(2)),
        )))
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        FrontlineRouteOutcome::Ready(_),
    ));
    assert_eq!(client.calls().len(), 1);
}

#[test]
fn subscription_observation_history_is_bounded_pruned_and_session_scoped() {
    let mut observations = super::RouteObservations::default();
    let old = observations.begin();
    for index in 0..super::MAX_OBSERVED_SUBSCRIPTIONS {
        assert!(observations.record(&subscription_id(&format!("sub-{index}"))));
    }
    assert!(!observations.record(&subscription_id("overflow")));
    assert_eq!(
        observations.subscriptions.len(),
        super::MAX_OBSERVED_SUBSCRIPTIONS
    );
    assert!(observations.finish(old, Some(&subscription_id("unrelated-cold"))));
    assert!(observations.subscriptions.is_empty());
    let old = observations.begin();
    observations.reset();
    let new = observations.begin();
    assert!(!observations.finish(old, Some(&subscription_id("reused"))));
    assert!(observations.finish(new, Some(&subscription_id("reused"))));
    assert!(observations.active.is_empty());
}

#[tokio::test]
async fn discarded_reply_from_old_stream_does_not_unsubscribe_reused_id() {
    let client = BlockingRouteClient::default();
    let old_reply = client.push_subscribe_response_channel();
    let new_reply = client.push_subscribe_response_channel();
    let retry_reply = client.push_subscribe_response_channel();
    let refreshed_reply = client.push_subscribe_response_channel();
    let events = Arc::new(Mutex::new(Vec::new()));
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(
            4,
            BlockingEventClient {
                client: client.clone(),
                events: events.clone(),
            },
        ),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let old_identity = http_request("old.example.com", "/");
    let new_identity = http_request("new.example.com", "/");
    let old = {
        let shared = shared.clone();
        let identity = old_identity.clone();
        tokio::spawn(async move { shared.route(identity, now()).await })
    };
    client.wait_for_call_count(1).await;
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::StreamClosed);
    tokio::time::sleep(Duration::from_millis(30)).await;
    new_reply
        .send(Ok(resolved_response(
            generated_request_id(2),
            subscription_id("reused"),
            new_identity.clone(),
            route_entry(InstanceState::Running, 7, Some(2)),
        )))
        .unwrap();
    assert!(matches!(
        shared.route(new_identity.clone(), now()).await.unwrap(),
        FrontlineRouteOutcome::Ready(_)
    ));
    old_reply
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("reused"),
            old_identity.clone(),
            route_entry(InstanceState::Running, 7, Some(1)),
        )))
        .unwrap();
    client.wait_for_call_count(3).await;
    retry_reply
        .send(Ok(miss_response(generated_request_id(3), old_identity)))
        .unwrap();
    assert!(matches!(
        old.await.unwrap().unwrap(),
        FrontlineRouteOutcome::Miss(_)
    ));
    assert!(
        shared.local_route(&new_identity, now()).await.is_none(),
        "ambiguous session cleanup flushes authority together with subscriptions"
    );
    refreshed_reply
        .send(Ok(resolved_response(
            generated_request_id(4),
            subscription_id("reused"),
            new_identity.clone(),
            route_entry(InstanceState::Running, 7, Some(2)),
        )))
        .unwrap();
    assert!(matches!(
        shared.route(new_identity, now()).await.unwrap(),
        FrontlineRouteOutcome::Ready(_)
    ));
    assert!(!client
        .calls()
        .iter()
        .any(|call| matches!(call, RouteClientCall::Unsubscribe { .. })));
}

#[derive(Default)]
struct FailedCleanupClient {
    resets: Arc<std::sync::atomic::AtomicUsize>,
}
impl RouteSubscriptionClient for FailedCleanupClient {
    type Error = TestRouteClientError;
    fn subscribe_route(
        &mut self,
        _: RouteRequestId,
        _: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        panic!("cleanup must not subscribe")
    }
    fn unsubscribe(
        &mut self,
        _: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        Box::pin(async { Err(TestRouteClientError::SubscribeFailed) })
    }
    fn reset_subscription(&mut self) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        self.resets
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn failed_or_saturated_discard_cleanup_resets_session_and_authority() {
    let mut client = FailedCleanupClient::default();
    let mut initial = SubscriptionState::new(4);
    initial.cache_mut().insert_positive(
        subscription_id("cached"),
        http_request("cached.example.com", "/"),
        route_entry(InstanceState::Running, 7, Some(1)),
        ttl(30),
        now(),
    );
    let state = tokio::sync::RwLock::new(initial);
    let mut observations = super::RouteObservations::default();
    let observation = observations.begin();
    let mut cleanups = tokio::task::JoinSet::new();
    super::defer_unsubscribes(
        vec![subscription_id("discarded")],
        &mut client,
        &state,
        &mut observations,
        &mut cleanups,
    )
    .await;
    tokio::task::yield_now().await;
    super::defer_unsubscribes(
        Vec::new(),
        &mut client,
        &state,
        &mut observations,
        &mut cleanups,
    )
    .await;
    assert_eq!(client.resets.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert!(!observations.finish(observation, None));
    assert!(matches!(
        state
            .read()
            .await
            .cache()
            .lookup(&http_request("cached.example.com", "/"), now()),
        crate::CacheLookup::Absent
    ));
    super::defer_unsubscribes(
        (0..=super::MAX_ROUTE_FLIGHTS)
            .map(|i| subscription_id(&format!("discarded-{i}")))
            .collect(),
        &mut client,
        &state,
        &mut observations,
        &mut cleanups,
    )
    .await;
    assert_eq!(client.resets.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert!(cleanups.len() <= 1);
}

#[tokio::test]
async fn shared_wake_completion_checks_only_its_affected_subscription() {
    for affected in [false, true] {
        let identity = http_request("cold.example.com", "/");
        let mut state = SubscriptionState::new(4);
        state.cache_mut().insert_positive(
            subscription_id("cold"),
            identity.clone(),
            route_entry(InstanceState::Cold, 12, None),
            ttl(30),
            now(),
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let wake = BlockingWakeClient::default();
        let reply = wake.push_response_channel();
        let shared = FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(
                state,
                ReviewEventClient {
                    events: events.clone(),
                },
            ),
            WakeTracker::new(),
            wake.clone(),
        )
        .into_shared();
        let task = {
            let shared = shared.clone();
            tokio::spawn(async move { shared.route(identity, now()).await })
        };
        tokio::time::timeout(Duration::from_secs(1), wake.wait_for_call_count(1))
            .await
            .unwrap();
        events
            .lock()
            .unwrap()
            .push(crate::RouteSubscriptionEvent::Update(Box::new(
                SubscribeControlPlaneOutput::RouteInvalidated {
                    subscription_id: subscription_id(if affected { "cold" } else { "unrelated" }),
                    reason: InvalidationReason::RouteRemoved,
                },
            )));
        tokio::time::sleep(Duration::from_millis(30)).await;
        reply.send(Ok(ready_wake_response(12, 7))).unwrap();
        let result = tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if affected {
            assert!(
                matches!(result, FrontlineRouteOutcome::Miss(_)),
                "invalidated wake cannot return its stale Ready backend"
            );
        } else {
            assert!(
                matches!(result, FrontlineRouteOutcome::Ready(_)),
                "unrelated updates must not reject a valid wake completion"
            );
        }
    }
}

// A stream rotates roughly once a minute. Dropping the cache at that moment
// makes every request in flight depend on a control-plane round trip, so a
// control plane that is slow or saturated turns a rotation into errors rather
// than a slow path: `Saturated` past MAX_ROUTE_WAITERS, `SubscribeDeadline`
// otherwise. Retained answers must keep serving straight through it.
#[tokio::test]
async fn stream_rotation_keeps_serving_cached_routes_without_saturating() {
    let client = BlockingRouteClient::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let identities: Vec<RouteIdentity> = (0..8)
        .map(|index| http_request(&format!("host{index}.example.com"), "/"))
        .collect();
    let replies: Vec<_> = identities
        .iter()
        .map(|_| client.push_subscribe_response_channel())
        .collect();
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(
            64,
            BlockingEventClient {
                client: client.clone(),
                events: events.clone(),
            },
        ),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();

    // Warm the cache: one resolved, registered answer per identity.
    let warm: Vec<_> = identities
        .iter()
        .cloned()
        .map(|identity| {
            let shared = shared.clone();
            tokio::spawn(async move { shared.route(identity, now()).await })
        })
        .collect();
    client.wait_for_call_count(identities.len()).await;
    for (index, reply) in replies.into_iter().enumerate() {
        reply
            .send(Ok(resolved_response(
                generated_request_id(index as u64 + 1),
                subscription_id(&format!("sub:{}", index + 1)),
                identities[index].clone(),
                route_entry(InstanceState::Running, 7, Some(1)),
            )))
            .unwrap();
    }
    for task in warm {
        assert!(matches!(
            task.await.unwrap().unwrap(),
            FrontlineRouteOutcome::Ready(_)
        ));
    }
    let warmed_calls = client.calls().len();

    // The stream closes. Re-registration is deliberately left unanswered, so
    // anything that reaches the control plane here would block or fail.
    let _reregistration: Vec<_> = identities
        .iter()
        .map(|_| client.push_subscribe_response_channel())
        .collect();
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::StreamEnded);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Far more concurrent requests than MAX_ROUTE_WAITERS, all for cached routes.
    let load: Vec<_> = (0..super::MAX_ROUTE_WAITERS * 2)
        .map(|index| {
            let shared = shared.clone();
            let identity = identities[index % identities.len()].clone();
            tokio::spawn(async move { shared.route(identity, now()).await })
        })
        .collect();
    for task in load {
        match task.await.unwrap() {
            Ok(FrontlineRouteOutcome::Ready(_)) => {}
            other => panic!("rotation must not disturb a cached route, got {other:?}"),
        }
    }

    // Re-registration was attempted for the retained identities and is still
    // outstanding; none of the load above added control-plane work.
    let calls = client.calls().len();
    assert!(
        calls > warmed_calls,
        "retained answers are registered again on the new stream"
    );
    assert!(
        calls <= warmed_calls + identities.len(),
        "re-registration is one attempt per retained identity, not per request"
    );
}

// Repairing retained answers must never crowd out a route the cache has never
// seen: that request has nothing to fall back on.
#[tokio::test]
async fn reregistration_leaves_flight_budget_for_a_first_time_miss() {
    let client = BlockingRouteClient::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let cached: Vec<RouteIdentity> = (0..super::MAX_ROUTE_FLIGHTS)
        .map(|index| http_request(&format!("cached{index}.example.com"), "/"))
        .collect();
    let replies: Vec<_> = cached
        .iter()
        .map(|_| client.push_subscribe_response_channel())
        .collect();
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(
            256,
            BlockingEventClient {
                client: client.clone(),
                events: events.clone(),
            },
        ),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();

    let warm: Vec<_> = cached
        .iter()
        .cloned()
        .map(|identity| {
            let shared = shared.clone();
            tokio::spawn(async move { shared.route(identity, now()).await })
        })
        .collect();
    client.wait_for_call_count(cached.len()).await;
    for (index, reply) in replies.into_iter().enumerate() {
        reply
            .send(Ok(resolved_response(
                generated_request_id(index as u64 + 1),
                subscription_id(&format!("sub:{}", index + 1)),
                cached[index].clone(),
                route_entry(InstanceState::Running, 7, Some(1)),
            )))
            .unwrap();
    }
    for task in warm {
        task.await.unwrap().unwrap();
    }

    // Every retained identity wants re-registering, and none of them answer.
    let _stalled: Vec<_> = cached
        .iter()
        .map(|_| client.push_subscribe_response_channel())
        .collect();
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::StreamEnded);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let fresh = http_request("never-seen.example.com", "/");
    let pending = {
        let shared = shared.clone();
        let identity = fresh.clone();
        tokio::spawn(async move { shared.route(identity, now()).await })
    };
    // The stalled re-registrations hold at most half the budget, so this
    // first-time miss still gets a flight and reaches the control plane rather
    // than being rejected as saturated.
    client
        .wait_for_call_count(cached.len() + super::MAX_REREGISTRATION_FLIGHTS + 1)
        .await;
    assert!(
        client.calls().iter().any(|call| matches!(
            call,
            RouteClientCall::Subscribe { identity, .. } if *identity == fresh
        )),
        "a first-time miss must still reach the control plane during re-registration"
    );
    pending.abort();
}

// A stream rotates in order about once a minute, so a reply that crosses one is
// routine rather than a sign of trouble. Its subscription ID died with its
// stream and must not be unsubscribed on the replacement, but the ended session
// delivered everything it had, so no other cached answer owes anything to it.
#[tokio::test]
async fn reply_crossing_an_orderly_rotation_is_discarded_without_costing_authority() {
    let client = BlockingRouteClient::default();
    let old_reply = client.push_subscribe_response_channel();
    let new_reply = client.push_subscribe_response_channel();
    let retry_reply = client.push_subscribe_response_channel();
    let events = Arc::new(Mutex::new(Vec::new()));
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(
            4,
            BlockingEventClient {
                client: client.clone(),
                events: events.clone(),
            },
        ),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let old_identity = http_request("old.example.com", "/");
    let new_identity = http_request("new.example.com", "/");
    let old = {
        let shared = shared.clone();
        let identity = old_identity.clone();
        tokio::spawn(async move { shared.route(identity, now()).await })
    };
    client.wait_for_call_count(1).await;
    events
        .lock()
        .unwrap()
        .push(crate::RouteSubscriptionEvent::StreamEnded);
    tokio::time::sleep(Duration::from_millis(30)).await;
    new_reply
        .send(Ok(resolved_response(
            generated_request_id(2),
            subscription_id("reused"),
            new_identity.clone(),
            route_entry(InstanceState::Running, 7, Some(2)),
        )))
        .unwrap();
    assert!(matches!(
        shared.route(new_identity.clone(), now()).await.unwrap(),
        FrontlineRouteOutcome::Ready(_)
    ));

    // The old stream's reply lands late, carrying an ID the new stream reissued.
    old_reply
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("reused"),
            old_identity.clone(),
            route_entry(InstanceState::Running, 7, Some(1)),
        )))
        .unwrap();
    client.wait_for_call_count(3).await;
    retry_reply
        .send(Ok(miss_response(generated_request_id(3), old_identity)))
        .unwrap();
    assert!(matches!(
        old.await.unwrap().unwrap(),
        FrontlineRouteOutcome::Miss(_)
    ));
    assert!(
        shared.local_route(&new_identity, now()).await.is_some(),
        "an orderly rotation loses no events, so a stale reply costs no authority"
    );
    assert!(!client
        .calls()
        .iter()
        .any(|call| matches!(call, RouteClientCall::Unsubscribe { .. })));
}
