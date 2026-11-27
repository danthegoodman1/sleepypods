//! The frontline route coordinator joined in process to the control plane's
//! Subscribe and WakeInstance service, with the route-change dispatcher
//! running. Time is paused, so a request the cache answers completes on its
//! first poll and every wait is exact.

use std::ops::Range;

use control_plane::RouteBindingRecord;
use frontline::{
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome,
    GrpcProxyControlPlaneClient, GrpcProxyControlPlaneError, RequestIdentityError,
    SharedFrontlineRouteCoordinator, SubscribeErrorCode,
};
use futures_util::FutureExt;

use super::*;

type EdgeClient = GrpcProxyControlPlaneClient<
    InProcessService<StoreBackedProxyGrpcService<FakeKubernetesClient>>,
>;
type RouteResult =
    Result<FrontlineRouteOutcome, FrontlineRouteCoordinatorError<GrpcProxyControlPlaneError>>;

struct Edge {
    store: Arc<FakeWakeStore>,
    broker: RouteSubscriptionBroker,
    kubernetes: FakeKubernetesClient,
    /// Refuses every new call while set, as an unreachable control plane would.
    unreachable: Arc<std::sync::atomic::AtomicBool>,
    routes: SharedFrontlineRouteCoordinator<EdgeClient, EdgeClient>,
    _dispatcher: tokio::sync::watch::Sender<bool>,
}

impl Edge {
    fn new(limits: ApiLimits) -> Self {
        let store = Arc::new(FakeWakeStore::default());
        let broker = RouteSubscriptionBroker::with_limits(limits);
        let kubernetes = FakeKubernetesClient::default();
        let service = InProcessService::new(proxy_grpc_service_with_store_and_route_events(
            store.clone(),
            KubernetesMaterializer::new(kubernetes.clone()),
            proxy_target(),
            broker.clone(),
        ));
        let unreachable = service.unreachable.clone();
        let client = ProxyControlPlaneClient::new(service);
        let routes = FrontlineRouteCoordinator::new(
            1024,
            GrpcProxyControlPlaneClient::new(client.clone()),
            GrpcProxyControlPlaneClient::new(client),
        )
        .into_shared();
        let dispatcher = spawn_route_dispatcher(store.clone(), broker.clone(), 0);
        Self {
            store,
            broker,
            kubernetes,
            unreachable,
            routes,
            _dispatcher: dispatcher,
        }
    }

    /// Commits a binding to a running instance with a ready backend.
    fn bind(&self, binding: &str, identity: RouteIdentity, instance: &str) {
        self.store.seed_route_resolved(
            identity.clone(),
            domain_route_entry(binding, instance, DomainInstanceState::Running),
        );
        self.store
            .seed_ready_materialization(ready_materialization(instance, 7));
        self.store
            .record_route_change(support::route_change_payload(
                &binding_record(binding, identity, instance),
                false,
            ));
    }

    fn unbind(&self, binding: &str, identity: RouteIdentity, instance: &str) {
        self.store.remove_route(binding);
        self.store
            .record_route_change(support::route_change_payload(
                &binding_record(binding, identity, instance),
                true,
            ));
    }

    async fn http(&self, host: &str, path: &str) -> RouteResult {
        self.routes.route_http(host, Some(path), now()).await
    }

    async fn route(&self, identity: RouteIdentity) -> RouteResult {
        self.routes.route(identity, now()).await
    }

    /// The answer when the cache holds one. `None` means the request has to
    /// wait for the control plane.
    fn cached(&self, host: &str, path: &str) -> Option<RouteResult> {
        self.routes
            .route_http(host, Some(path), now())
            .now_or_never()
    }

    /// Binds hosts `h<n>.example.com` to instances of their own and routes one
    /// request to each.
    async fn warm(&self, count: usize) {
        for index in 0..count {
            self.bind(
                &format!("route-{index}"),
                exact_http(&hot_host(index), None),
                &format!("instance-{index}"),
            );
        }
        // The bindings are committed and announced before the first request.
        settle().await;
        for index in 0..count {
            assert_eq!(
                instance(self.http(&hot_host(index), "/").await),
                format!("instance-{index}")
            );
        }
    }

    /// Asserts the cache answers each host, with no lookup.
    fn assert_hot(&self, hosts: Range<usize>) {
        let lookups = self.store.resolve_route_requests();
        for index in hosts {
            let answer = self
                .cached(&hot_host(index), "/")
                .unwrap_or_else(|| panic!("{} is answered from the cache", hot_host(index)));
            assert_eq!(instance(answer), format!("instance-{index}"));
        }
        assert_eq!(self.store.resolve_route_requests(), lookups);
    }
}

fn now() -> std::time::Instant {
    tokio::time::Instant::now().into_std()
}

fn hot_host(index: usize) -> String {
    format!("h{index}.example.com")
}

fn binding_record(binding: &str, identity: RouteIdentity, instance: &str) -> RouteBindingRecord {
    RouteBindingRecord {
        id: RouteBindingId::new(binding).unwrap(),
        instance_id: InstanceId::new(instance).unwrap(),
        identity,
    }
}

fn http_request(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).unwrap(),
        path: Some(PathPrefix::new(path).unwrap()),
    }
}

/// The instance a request is forwarded to.
fn instance(result: RouteResult) -> String {
    match result {
        Ok(FrontlineRouteOutcome::Ready(ready)) => ready.instance_id.as_str().to_owned(),
        other => panic!("expected a ready backend, got {other:?}"),
    }
}

/// The instance a request is forwarded to, or `None` when no route covers it.
fn routed(result: RouteResult) -> Option<String> {
    match result {
        Ok(FrontlineRouteOutcome::Miss) => None,
        other => Some(instance(other)),
    }
}

#[tokio::test(start_paused = true)]
async fn a_5000_byte_path_is_refused_and_256_hot_hosts_stay_cached() {
    let edge = Edge::new(ApiLimits::default());
    edge.warm(256).await;
    let lookups = edge.store.resolve_route_requests();

    let path = format!("/{}", "a".repeat(4_999));
    for host in [hot_host(0), "cold.example.com".to_owned()] {
        assert!(matches!(
            edge.http(&host, &path).await,
            Err(FrontlineRouteCoordinatorError::Identity(
                RequestIdentityError::PathTooLong
            ))
        ));
    }
    settle().await;

    edge.assert_hot(0..256);
    assert_eq!(edge.store.resolve_route_requests(), lookups);
}

#[tokio::test(start_paused = true)]
async fn a_failed_store_lookup_fails_its_own_host_and_256_hot_hosts_stay_cached() {
    let edge = Edge::new(ApiLimits::default());
    edge.warm(256).await;
    edge.bind(
        "route-new",
        exact_http("new.example.com", None),
        "instance-new",
    );
    edge.store.seed_route_error_unavailable("store is offline");

    let failed = edge.http("new.example.com", "/").await;
    assert!(
        matches!(
            failed,
            Err(FrontlineRouteCoordinatorError::Resolve {
                code: SubscribeErrorCode::Unavailable,
                ..
            })
        ),
        "{failed:?}"
    );
    settle().await;
    edge.assert_hot(0..256);

    edge.store.clear_route_error();
    assert_eq!(
        instance(edge.http("new.example.com", "/").await),
        "instance-new"
    );
    edge.assert_hot(0..256);
}

#[tokio::test(start_paused = true)]
async fn the_control_planes_subscription_limit_is_met_locally_and_hot_hosts_stay_cached() {
    let edge = Edge::new(ApiLimits {
        subscriptions_per_stream: 256,
        ..Default::default()
    });
    edge.warm(256).await;
    edge.bind(
        "route-new",
        exact_http("new.example.com", None),
        "instance-new",
    );

    // The proxy releases its four oldest hosts and asks again.
    assert_eq!(
        instance(edge.http("new.example.com", "/").await),
        "instance-new"
    );
    settle().await;
    edge.assert_hot(4..256);

    // A released host returns on demand, within the limit the proxy learned.
    assert_eq!(instance(edge.http(&hot_host(0), "/").await), "instance-0");
    for index in 0..40 {
        let host = format!("more-{index}.example.com");
        edge.bind(
            &format!("more-{index}"),
            exact_http(&host, None),
            "instance-more",
        );
        assert_eq!(instance(edge.http(&host, "/").await), "instance-more");
    }
    edge.assert_hot(200..256);
}

#[tokio::test(start_paused = true)]
async fn stream_rotations_keep_250_hot_hosts_served_from_the_cache() {
    // Streams live 60 seconds and rule sets 90.
    let edge = Edge::new(ApiLimits::default());
    edge.warm(250).await;
    assert_eq!(edge.store.resolve_route_requests(), 250);

    for _ in 0..150 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        edge.assert_hot(0..250);
    }
    assert_eq!(
        edge.store.resolve_route_requests(),
        750,
        "each of two rotations registered every host once"
    );

    // The replacement stream keeps the hosts current.
    edge.bind("route-7", exact_http(&hot_host(7), None), "instance-moved");
    settle().await;
    assert_eq!(
        instance(edge.cached(&hot_host(7), "/").expect("a pushed rule set")),
        "instance-moved"
    );
}

#[tokio::test(start_paused = true)]
async fn a_control_plane_unreachable_at_a_rotation_keeps_250_hot_hosts_served_until_it_returns() {
    let edge = Edge::new(ApiLimits::default());
    edge.warm(250).await;

    // The stream ends in order at 60 seconds and cannot be replaced for 20.
    tokio::time::sleep(Duration::from_secs(59)).await;
    edge.unreachable
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for _ in 0..21 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        edge.assert_hot(0..250);
    }
    assert_eq!(edge.store.resolve_route_requests(), 250);

    edge.unreachable
        .store(false, std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    edge.assert_hot(0..250);
    assert_eq!(
        edge.store.resolve_route_requests(),
        500,
        "the new stream registered every host"
    );
    // The hosts now outlive the rule sets the first stream delivered.
    tokio::time::sleep(Duration::from_secs(30)).await;
    edge.assert_hot(0..250);
}

#[tokio::test(start_paused = true)]
async fn lost_route_changes_clear_the_cache_before_the_next_request_is_served() {
    for lose in [
        (|edge: &Edge| edge.broker.reset()) as fn(&Edge),
        |edge: &Edge| {
            for index in 0..5_000 {
                edge.broker
                    .notify_instance_changed(InstanceId::new(format!("other-{index}")).unwrap());
            }
        },
    ] {
        let edge = Edge::new(ApiLimits::default());
        edge.warm(16).await;
        // A change commits, and the event that announces it is lost.
        edge.store.seed_route_resolved(
            exact_http(&hot_host(0), None),
            domain_route_entry("route-0", "instance-moved", DomainInstanceState::Running),
        );
        edge.store
            .seed_ready_materialization(ready_materialization("instance-moved", 7));
        edge.assert_hot(0..16);

        lose(&edge);
        settle().await;

        for index in 0..16 {
            assert!(
                edge.cached(&hot_host(index), "/").is_none(),
                "no host is served from a cache that may have missed a change"
            );
        }
        assert_eq!(
            instance(edge.http(&hot_host(0), "/").await),
            "instance-moved"
        );
        assert_eq!(instance(edge.http(&hot_host(1), "/").await), "instance-1");
    }
}

#[tokio::test(start_paused = true)]
async fn ten_thousand_paths_on_one_host_cost_one_lookup_and_one_subscription() {
    let edge = Edge::new(ApiLimits {
        subscriptions_per_stream: 1,
        ..Default::default()
    });
    edge.bind(
        "route-app",
        exact_http("app.example.com", None),
        "instance-app",
    );
    settle().await;

    for index in 0..10_000 {
        assert_eq!(
            instance(
                edge.http("app.example.com", &format!("/users/{index}/profile"))
                    .await
            ),
            "instance-app"
        );
    }

    assert_eq!(edge.store.resolve_route_requests(), 1);
    assert_eq!(edge.routes.lookup_counters().totals(), (9_999, 1));
}

/// Every order of `count` items.
fn permutations(count: usize) -> Vec<Vec<usize>> {
    if count == 0 {
        return vec![Vec::new()];
    }
    let mut all = Vec::new();
    for shorter in permutations(count - 1) {
        for position in 0..=shorter.len() {
            let mut order = shorter.clone();
            order.insert(position, count - 1);
            all.push(order);
        }
    }
    all
}

#[tokio::test(start_paused = true)]
async fn a_request_gets_the_same_route_in_every_warm_up_order() {
    let bindings = [
        ("wildcard", wildcard_http("example.com", None)),
        ("exact-root", exact_http("app.example.com", Some("/"))),
        ("exact-api", exact_http("app.example.com", Some("/api"))),
        (
            "exact-api-v1",
            exact_http("app.example.com", Some("/api/v1")),
        ),
        ("sni-wildcard", wildcard_sni("example.com")),
        ("sni-exact", exact_sni("app.example.com")),
    ];
    let requests = [
        (
            http_request("other.example.com", "/api/v1/users"),
            "wildcard",
        ),
        (http_request("app.example.com", "/"), "exact-root"),
        (http_request("app.example.com", "/api/users"), "exact-api"),
        (
            http_request("app.example.com", "/api/v1/users"),
            "exact-api-v1",
        ),
        (exact_sni("other.example.com"), "sni-wildcard"),
        (exact_sni("app.example.com"), "sni-exact"),
    ];

    for order in permutations(requests.len()) {
        let edge = Edge::new(ApiLimits::default());
        for (binding, identity) in &bindings {
            edge.bind(binding, identity.clone(), binding);
        }
        settle().await;
        // Cold in this order, then again from the cache.
        for index in order.iter().chain(&order) {
            let (request, expected) = &requests[*index];
            assert_eq!(
                &instance(edge.route(request.clone()).await),
                expected,
                "warm-up order {order:?}"
            );
        }
        assert_eq!(
            edge.store.resolve_route_requests(),
            4,
            "one lookup for each host of each kind"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_binding_committed_after_warm_up_takes_the_requests_it_ranks_first_for() {
    let edge = Edge::new(ApiLimits::default());
    let cached =
        |host: &str, path: &str| routed(edge.cached(host, path).expect("the cache answers"));
    edge.bind("wildcard", wildcard_http("example.com", None), "wildcard");
    edge.bind("sni-wildcard", wildcard_sni("example.com"), "sni-wildcard");
    for (host, path) in [
        ("app.example.com", "/"),
        ("app.example.com", "/api/users"),
        ("other.example.com", "/"),
    ] {
        assert_eq!(instance(edge.http(host, path).await), "wildcard");
    }
    assert_eq!(
        instance(edge.route(exact_sni("app.example.com")).await),
        "sni-wildcard"
    );

    edge.bind("exact", exact_http("app.example.com", None), "exact");
    settle().await;
    assert_eq!(cached("app.example.com", "/").as_deref(), Some("exact"));
    assert_eq!(
        cached("app.example.com", "/api/users").as_deref(),
        Some("exact")
    );
    assert_eq!(
        cached("other.example.com", "/").as_deref(),
        Some("wildcard")
    );

    edge.bind("api", exact_http("app.example.com", Some("/api")), "api");
    settle().await;
    assert_eq!(cached("app.example.com", "/").as_deref(), Some("exact"));
    assert_eq!(
        cached("app.example.com", "/api/users").as_deref(),
        Some("api")
    );

    edge.bind("sni-exact", exact_sni("app.example.com"), "sni-exact");
    settle().await;
    assert_eq!(
        instance(
            edge.route(exact_sni("app.example.com"))
                .now_or_never()
                .expect("the cache answers")
        ),
        "sni-exact"
    );

    // Removing a binding returns its requests to the next rule.
    edge.unbind("exact", exact_http("app.example.com", None), "exact");
    settle().await;
    assert_eq!(cached("app.example.com", "/").as_deref(), Some("wildcard"));
    assert_eq!(
        cached("app.example.com", "/api/users").as_deref(),
        Some("api")
    );

    // A host whose rules all leave has no route.
    edge.unbind("wildcard", wildcard_http("example.com", None), "wildcard");
    settle().await;
    assert_eq!(routed(edge.http("other.example.com", "/").await), None);
    assert_eq!(cached("app.example.com", "/"), None);
    assert_eq!(
        cached("app.example.com", "/api/users").as_deref(),
        Some("api")
    );
}

#[tokio::test(start_paused = true)]
async fn a_route_moved_by_delete_then_create_is_served_by_the_new_binding_at_once() {
    let edge = Edge::new(ApiLimits::default());
    let host = "moved.example.com";
    edge.bind("old-route", exact_http(host, None), "old");
    settle().await;
    assert_eq!(instance(edge.http(host, "/").await), "old");

    // The proxy learns of the removal before the replacement is committed.
    edge.unbind("old-route", exact_http(host, None), "old");
    settle().await;
    edge.bind("new-route", exact_http(host, None), "new");
    settle().await;

    assert_eq!(instance(edge.http(host, "/").await), "new");
}

/// A deterministic generator, so a failure names the round that produced it.
struct Random(u64);

impl Random {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// The binding the control plane resolves a request to, written out from the
/// rule: among the bindings of the request's kind whose host and path cover
/// it, an exact host wins, then the longer suffix, then the longer path prefix.
fn resolved_binding<'a>(
    bindings: &'a [(String, RouteIdentity)],
    request: &RouteIdentity,
) -> Option<&'a str> {
    let (sni, host, path) = match request {
        RouteIdentity::Http { host, path } => (
            false,
            host.as_str(),
            path.as_ref().map(|path| path.as_str()),
        ),
        RouteIdentity::Sni { host } => (true, host.as_str(), None),
    };
    bindings
        .iter()
        .filter_map(|(binding, identity)| {
            let (bound_host, prefix) = match identity {
                RouteIdentity::Http { host, path } if !sni => {
                    (host, path.as_ref().map(|path| path.as_str()))
                }
                RouteIdentity::Sni { host } if sni => (host, None),
                _ => return None,
            };
            let host_matches = if bound_host.is_wildcard() {
                host.len() > bound_host.as_str().len()
                    && host.ends_with(&format!(".{}", bound_host.as_str()))
            } else {
                host == bound_host.as_str()
            };
            let path_matches = match (prefix, path) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(prefix), Some(path)) => {
                    prefix == "/"
                        || path == prefix
                        || (prefix.ends_with('/') && path.starts_with(prefix))
                        || path.starts_with(&format!("{prefix}/"))
                }
            };
            (host_matches && path_matches).then(|| {
                (
                    (
                        !bound_host.is_wildcard(),
                        bound_host.as_str().len(),
                        prefix.map_or(0, str::len),
                    ),
                    binding.as_str(),
                )
            })
        })
        .max_by_key(|(rank, _)| *rank)
        .map(|(_, binding)| binding)
}

#[tokio::test(start_paused = true)]
async fn local_matching_equals_the_control_planes_resolution_for_random_binding_sets() {
    let exact_hosts = [
        "example.com",
        "a.example.com",
        "b.a.example.com",
        "c.b.a.example.com",
        "ab.example.com",
        "c.ab.example.com",
    ];
    let suffixes = [
        "example.com",
        "a.example.com",
        "b.a.example.com",
        "ab.example.com",
    ];
    let prefixes = [
        None,
        Some("/"),
        Some("/a"),
        Some("/a/"),
        Some("/a/b"),
        Some("/ab"),
    ];
    let paths = ["/", "/a", "/a/", "/a/b", "/a/b/c", "/ab", "/abc", "/b"];
    let mut random = Random(0x9e37_79b9_7f4a_7c15);

    for round in 0..150 {
        let mut identities: Vec<RouteIdentity> = Vec::new();
        for _ in 0..random.below(14) {
            let host = if random.below(2) == 0 {
                RouteHost::exact(random.pick(&exact_hosts)).unwrap()
            } else {
                RouteHost::wildcard_suffix(random.pick(&suffixes)).unwrap()
            };
            let identity = if random.below(4) == 0 {
                RouteIdentity::Sni { host }
            } else {
                RouteIdentity::Http {
                    host,
                    path: random
                        .pick(&prefixes)
                        .map(|path| PathPrefix::new(path).unwrap()),
                }
            };
            // The control plane holds one binding per identity.
            if !identities.contains(&identity) {
                identities.push(identity);
            }
        }
        let bindings: Vec<(String, RouteIdentity)> = identities
            .into_iter()
            .enumerate()
            .map(|(index, identity)| (format!("binding-{index}"), identity))
            .collect();
        let edge = Edge::new(ApiLimits::default());
        for (binding, identity) in &bindings {
            edge.bind(binding, identity.clone(), binding);
        }

        for _ in 0..24 {
            let host = RouteHost::exact(random.pick(&exact_hosts)).unwrap();
            let request = if random.below(4) == 0 {
                RouteIdentity::Sni { host }
            } else {
                RouteIdentity::Http {
                    host,
                    path: Some(PathPrefix::new(*random.pick(&paths)).unwrap()),
                }
            };
            assert_eq!(
                routed(edge.route(request.clone()).await).as_deref(),
                resolved_binding(&bindings, &request),
                "round {round}: {request:?} against {bindings:?}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn sixty_four_hosts_on_waking_instances_leave_an_unrelated_miss_served_and_stop_with_their_clients(
) {
    let edge = Edge::new(ApiLimits::default());
    edge.store.seed_workload_class(domain_workload_class());
    for index in 0..64 {
        let instance = format!("instance-{index}");
        edge.store
            .seed_instance(domain_instance(&instance, DomainInstanceState::Cold, 1));
        let identity = exact_http(&hot_host(index), None);
        edge.store.seed_route_resolved(
            identity.clone(),
            domain_route_entry(
                &format!("route-{index}"),
                &instance,
                DomainInstanceState::Cold,
            ),
        );
        edge.store
            .record_route_change(support::route_change_payload(
                &binding_record(&format!("route-{index}"), identity, &instance),
                false,
            ));
    }

    let waiting: Vec<_> = (0..64)
        .map(|index| {
            let routes = edge.routes.clone();
            tokio::spawn(async move {
                routes
                    .route_http(&hot_host(index), Some(&format!("/page/{index}")), now())
                    .await
            })
        })
        .collect();
    settle().await;
    assert_eq!(
        edge.store.transition_requests().len(),
        64,
        "every cold instance was asked to wake once"
    );
    assert!(waiting.iter().all(|request| !request.is_finished()));

    // A request for another host is resolved while all 64 wait.
    let started = tokio::time::Instant::now();
    assert!(matches!(
        edge.http("unbound.example.com", "/").await,
        Ok(FrontlineRouteOutcome::Miss)
    ));
    assert_eq!(started.elapsed(), Duration::ZERO);

    // While requests wait, their hosts are looked up again every 1.5 seconds.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(waiting.iter().all(|request| !request.is_finished()));
    for request in &waiting {
        request.abort();
    }
    settle().await;

    // With every client gone, nothing is looked up and nothing is woken.
    let lookups = edge.store.resolve_route_requests();
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(edge.store.resolve_route_requests(), lookups);
    assert_eq!(edge.store.transition_requests().len(), 64);

    // A request that arrives once an instance runs is served.
    let request = {
        let routes = edge.routes.clone();
        tokio::spawn(async move { routes.route_http(&hot_host(3), Some("/"), now()).await })
    };
    settle().await;
    assert!(!request.is_finished(), "the instance is still waking");
    for _ in 0..64 {
        reconcile_proxy_work(edge.store.clone(), edge.kubernetes.clone()).await;
        settle().await;
        if request.is_finished() {
            break;
        }
    }
    assert_eq!(instance(request.await.unwrap()), "instance-3");
}

#[tokio::test(start_paused = true)]
async fn a_cold_request_outlasts_a_store_outage_during_its_wake_and_is_served() {
    let edge = Edge::new(ApiLimits::default());
    edge.store.seed_workload_class(domain_workload_class());
    edge.store.seed_instance(domain_instance(
        "instance-cold",
        DomainInstanceState::Cold,
        1,
    ));
    let identity = exact_http("cold.example.com", None);
    edge.store.seed_route_resolved(
        identity.clone(),
        domain_route_entry("route-cold", "instance-cold", DomainInstanceState::Cold),
    );
    edge.store
        .record_route_change(support::route_change_payload(
            &binding_record("route-cold", identity, "instance-cold"),
            false,
        ));
    settle().await;
    // The store fails every wake, and then every lookup, for 30 seconds.
    edge.store
        .fail_get_with(support::FakeStoreError::Unavailable("database is down"));

    let request = {
        let routes = edge.routes.clone();
        tokio::spawn(async move {
            routes
                .route_http("cold.example.com", Some("/"), now())
                .await
        })
    };
    settle().await;
    edge.store.seed_route_error_unavailable("database is down");
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert!(!request.is_finished(), "the cold request still waits");
    assert!(edge.store.transition_requests().is_empty());

    // The store returns. The next wake call is accepted and the instance runs.
    edge.store.clear_get_error();
    edge.store.clear_route_error();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(edge.store.transition_requests().len(), 1);
    for _ in 0..64 {
        reconcile_proxy_work(edge.store.clone(), edge.kubernetes.clone()).await;
        settle().await;
        if request.is_finished() {
            break;
        }
    }
    assert_eq!(instance(request.await.unwrap()), "instance-cold");
}
