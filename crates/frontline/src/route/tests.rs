use std::time::Duration;

use sleepypods_api::{InstanceState, RouteHost, RouteIdentity, RouteKind};
use tokio::task::JoinHandle;

use super::{
    testing::{
        entry, http_rule, http_wildcard_rule, running, FakeControlPlane, FakeWake, TestError,
    },
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome,
    SharedFrontlineRouteCoordinator, MAX_ROUTE_FLIGHTS, MAX_ROUTE_WAITERS,
};
use crate::{
    RequestIdentityError, SubscribeErrorCode, WakeInstanceResponse, WakeUnavailableReason,
};

type Shared = SharedFrontlineRouteCoordinator<FakeControlPlane, FakeWake>;
type RouteResult = Result<FrontlineRouteOutcome, FrontlineRouteCoordinatorError<TestError>>;

struct Harness {
    control: FakeControlPlane,
    wake: FakeWake,
    shared: Shared,
}

fn harness() -> Harness {
    harness_with(|coordinator| coordinator)
}

fn harness_with(
    configure: impl FnOnce(
        FrontlineRouteCoordinator<FakeControlPlane, FakeWake>,
    ) -> FrontlineRouteCoordinator<FakeControlPlane, FakeWake>,
) -> Harness {
    let control = FakeControlPlane::default();
    let wake = FakeWake::default();
    let shared = configure(FrontlineRouteCoordinator::new(
        256,
        control.clone(),
        wake.clone(),
    ))
    .into_shared();
    Harness {
        control,
        wake,
        shared,
    }
}

impl Harness {
    async fn http(&self, host: &str, path: &str) -> RouteResult {
        route(&self.shared, host, path).await
    }

    fn spawn_http(&self, host: &str, path: &str) -> JoinHandle<RouteResult> {
        let shared = self.shared.clone();
        let (host, path) = (host.to_owned(), path.to_owned());
        tokio::spawn(async move { route(&shared, &host, &path).await })
    }

    /// Warms `count` hosts `h<n>.example.com`, each on its own instance.
    async fn warm(&self, count: usize) {
        for index in 0..count {
            let host = format!("h{index}.example.com");
            self.control
                .set_http(&host, running(&format!("instance-{index}")));
            assert_eq!(
                instance(self.http(&host, "/").await),
                format!("instance-{index}")
            );
        }
    }
}

async fn route(shared: &Shared, host: &str, path: &str) -> RouteResult {
    shared.route_http(host, Some(path), super::clock()).await
}

/// Runs every task until all of them wait, without moving the clock on by
/// more than a millisecond.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

fn instance(result: RouteResult) -> String {
    match result {
        Ok(FrontlineRouteOutcome::Ready(ready)) => ready.instance_id.as_str().to_owned(),
        other => panic!("expected a ready backend, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_cached_host_is_answered_without_the_control_plane() {
    let h = harness();
    h.control.set_http("app.example.com", running("app"));

    assert_eq!(instance(h.http("app.example.com", "/").await), "app");
    for index in 0..100 {
        assert_eq!(
            instance(
                h.http("App.Example.COM:8443", &format!("/page/{index}"))
                    .await
            ),
            "app"
        );
    }

    assert_eq!(h.control.subscribes(), 1);
    assert_eq!(h.control.connects(), 1);
    assert_eq!(h.shared.lookup_counters().totals(), (100, 1));
}

#[tokio::test(start_paused = true)]
async fn ten_thousand_paths_on_one_host_cost_one_lookup_and_one_subscription() {
    let h = harness();
    h.control.set_http("app.example.com", running("app"));

    for index in 0..10_000 {
        assert_eq!(
            instance(
                h.http("app.example.com", &format!("/users/{index}/profile"))
                    .await
            ),
            "app"
        );
    }

    assert_eq!(h.control.subscribes(), 1);
    assert_eq!(h.control.live_subscriptions(), 1);
}

#[tokio::test(start_paused = true)]
async fn the_rule_set_is_matched_locally_by_host_and_longest_path() {
    let h = harness();
    h.control.set_rules(
        RouteKind::Http,
        "app.example.com",
        vec![
            http_wildcard_rule("example.com", None, running("wildcard")),
            http_rule("app.example.com", Some("/api"), running("api")),
            http_rule("app.example.com", Some("/api/v1"), running("v1")),
        ],
    );

    assert_eq!(
        instance(h.http("app.example.com", "/api/v1/users").await),
        "v1"
    );
    assert_eq!(instance(h.http("app.example.com", "/api/v2").await), "api");
    assert_eq!(
        instance(h.http("app.example.com", "/apiary").await),
        "wildcard"
    );
    assert_eq!(instance(h.http("app.example.com", "/").await), "wildcard");
    assert_eq!(h.control.subscribes(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_rule_set_for_one_host_answers_no_other_host() {
    let h = harness();
    let wildcard = http_wildcard_rule("example.com", None, running("wildcard"));
    h.control
        .set_rules(RouteKind::Http, "a.example.com", vec![wildcard.clone()]);
    h.control.set_rules(
        RouteKind::Http,
        "b.example.com",
        vec![http_rule("b.example.com", None, running("exact")), wildcard],
    );

    assert_eq!(instance(h.http("a.example.com", "/").await), "wildcard");
    assert_eq!(instance(h.http("b.example.com", "/").await), "exact");
    assert_eq!(h.control.subscribes(), 2);
}

#[tokio::test(start_paused = true)]
async fn sni_hosts_are_cached_apart_from_http_hosts() {
    let h = harness();
    h.control.set_http("db.example.com", running("http"));
    h.control.set_sni("db.example.com", running("sni"));

    let sni = RouteIdentity::Sni {
        host: RouteHost::exact("db.example.com").unwrap(),
    };
    assert_eq!(instance(h.shared.route(sni, super::clock()).await), "sni");
    assert_eq!(instance(h.http("db.example.com", "/").await), "http");
}

#[tokio::test(start_paused = true)]
async fn concurrent_requests_for_one_host_share_one_flight() {
    let h = harness();
    h.control.set_http("app.example.com", running("app"));
    h.control.hold();

    let requests: Vec<_> = (0..50)
        .map(|index| h.spawn_http("app.example.com", &format!("/{index}")))
        .collect();
    settle().await;
    assert_eq!(h.control.subscribes(), 1);

    h.control.release();
    for request in requests {
        assert_eq!(instance(request.await.unwrap()), "app");
    }
    assert_eq!(h.control.subscribes(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_host_with_no_binding_is_a_brief_miss_without_a_subscription() {
    let h = harness();

    assert_eq!(
        h.http("nobody.example.com", "/").await,
        Ok(FrontlineRouteOutcome::Miss)
    );
    assert_eq!(
        h.http("nobody.example.com", "/again").await,
        Ok(FrontlineRouteOutcome::Miss)
    );
    assert_eq!(h.control.subscribes(), 1);
    assert_eq!(h.control.live_subscriptions(), 0);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    h.control
        .set_http("nobody.example.com", running("somebody"));
    assert_eq!(
        instance(h.http("nobody.example.com", "/").await),
        "somebody"
    );
    assert_eq!(h.control.subscribes(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_path_no_rule_covers_is_a_miss_from_the_cached_rule_set() {
    let h = harness();
    h.control.set_rules(
        RouteKind::Http,
        "app.example.com",
        vec![http_rule("app.example.com", Some("/api"), running("api"))],
    );

    assert_eq!(
        instance(h.http("app.example.com", "/api/users").await),
        "api"
    );
    assert_eq!(
        h.http("app.example.com", "/web").await,
        Ok(FrontlineRouteOutcome::Miss)
    );
    assert_eq!(h.control.subscribes(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_oversized_path_or_host_is_refused_before_any_lookup() {
    let h = harness();
    h.warm(3).await;

    let long_path = format!("/{}", "a".repeat(5000));
    assert_eq!(
        h.http("attacker.example.com", &long_path).await,
        Err(FrontlineRouteCoordinatorError::Identity(
            RequestIdentityError::PathTooLong
        ))
    );
    assert_eq!(
        h.http("h0.example.com", &long_path).await,
        Err(FrontlineRouteCoordinatorError::Identity(
            RequestIdentityError::PathTooLong
        ))
    );
    let long_host = format!("{}.example.com", "a".repeat(250));
    assert_eq!(
        h.http(&long_host, "/").await,
        Err(FrontlineRouteCoordinatorError::Identity(
            RequestIdentityError::HostTooLong
        ))
    );
    settle().await;

    assert_eq!(h.control.subscribes(), 3);
    assert_eq!(h.control.connects(), 1);
    for index in 0..3 {
        assert_eq!(
            instance(h.http(&format!("h{index}.example.com"), "/").await),
            format!("instance-{index}")
        );
    }
    assert_eq!(h.control.subscribes(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_failed_lookup_fails_its_own_request_and_no_other_host() {
    let h = harness();
    h.warm(20).await;
    h.control.fail_host(
        RouteKind::Http,
        "broken.example.com",
        SubscribeErrorCode::Unavailable,
    );

    assert!(matches!(
        h.http("broken.example.com", "/").await,
        Err(FrontlineRouteCoordinatorError::Resolve {
            code: SubscribeErrorCode::Unavailable,
            ..
        })
    ));

    assert_eq!(h.control.connects(), 1, "the stream stays up");
    for index in 0..20 {
        assert_eq!(
            instance(h.http(&format!("h{index}.example.com"), "/").await),
            format!("instance-{index}")
        );
    }
    assert_eq!(
        h.control.subscribes(),
        21,
        "every warmed host stayed cached"
    );

    // The failure is not cached: the next request asks again.
    h.control.heal_host(RouteKind::Http, "broken.example.com");
    h.control.set_http("broken.example.com", running("mended"));
    assert_eq!(instance(h.http("broken.example.com", "/").await), "mended");
}

#[tokio::test(start_paused = true)]
async fn the_subscription_limit_is_met_by_releasing_the_oldest_hosts() {
    let h = harness();
    h.control.set_capacity(8);
    h.warm(8).await;
    h.control.set_http("h8.example.com", running("instance-8"));

    assert_eq!(instance(h.http("h8.example.com", "/").await), "instance-8");

    assert_eq!(h.control.connects(), 1, "the stream stays up");
    let after_recovery = h.control.subscribes();
    for index in 4..9 {
        assert_eq!(
            instance(h.http(&format!("h{index}.example.com"), "/").await),
            format!("instance-{index}")
        );
    }
    assert_eq!(
        h.control.subscribes(),
        after_recovery,
        "the newer hosts stayed cached"
    );
    assert!(h.control.live_subscriptions() <= 8);
    // The oldest hosts gave way and resolve again on demand.
    assert_eq!(instance(h.http("h0.example.com", "/").await), "instance-0");
    assert_eq!(h.control.subscribes(), after_recovery + 1);
}

#[tokio::test(start_paused = true)]
async fn a_pushed_rule_set_replaces_the_cached_one() {
    let h = harness();
    h.control.set_http("app.example.com", running("first"));
    assert_eq!(instance(h.http("app.example.com", "/").await), "first");

    h.control.set_http("app.example.com", running("second"));
    settle().await;

    assert_eq!(instance(h.http("app.example.com", "/").await), "second");
    assert_eq!(h.control.subscribes(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_pushed_empty_rule_set_drops_the_host_and_the_next_request_asks_again() {
    let h = harness();
    h.control.set_http("app.example.com", running("app"));
    assert_eq!(instance(h.http("app.example.com", "/").await), "app");

    h.control
        .set_rules(RouteKind::Http, "app.example.com", Vec::new());
    settle().await;

    assert_eq!(
        h.http("app.example.com", "/").await,
        Ok(FrontlineRouteOutcome::Miss)
    );
    assert_eq!(h.control.subscribes(), 2, "the miss is a fresh answer");
    assert_eq!(h.control.live_subscriptions(), 0);
    assert_eq!(h.control.unsubscribes(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_binding_removed_and_replaced_is_served_by_its_replacement_at_once() {
    let h = harness();
    h.control.set_http("app.example.com", running("old"));
    assert_eq!(instance(h.http("app.example.com", "/").await), "old");

    // The old binding is removed and a new one is committed a moment later.
    h.control
        .set_rules(RouteKind::Http, "app.example.com", Vec::new());
    settle().await;
    h.control.set_http("app.example.com", running("new"));
    settle().await;

    assert_eq!(instance(h.http("app.example.com", "/").await), "new");
}

#[tokio::test(start_paused = true)]
async fn a_subscription_the_control_plane_ends_drops_only_its_host() {
    let h = harness();
    h.warm(2).await;

    h.control.fail_host(
        RouteKind::Http,
        "h0.example.com",
        SubscribeErrorCode::Unavailable,
    );
    settle().await;
    h.control.heal_host(RouteKind::Http, "h0.example.com");

    assert_eq!(instance(h.http("h1.example.com", "/").await), "instance-1");
    assert_eq!(h.control.subscribes(), 2);
    assert_eq!(instance(h.http("h0.example.com", "/").await), "instance-0");
    assert_eq!(h.control.subscribes(), 3, "the dropped host resolved again");
}

#[tokio::test(start_paused = true)]
async fn an_expired_rule_set_is_resolved_again() {
    let h = harness();
    h.control.set_ttl(Duration::from_secs(2));
    h.control.set_http("app.example.com", running("app"));
    assert_eq!(instance(h.http("app.example.com", "/").await), "app");

    tokio::time::sleep(Duration::from_millis(2100)).await;

    assert_eq!(instance(h.http("app.example.com", "/").await), "app");
    assert_eq!(h.control.subscribes(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_cold_host_is_woken_once_and_served_when_its_rule_set_reports_running() {
    let h = harness();
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Cold, 7));

    let requests: Vec<_> = (0..10)
        .map(|index| h.spawn_http("app.example.com", &format!("/{index}")))
        .collect();
    settle().await;
    let calls = h.wake.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].instance_id.as_str(), "app");
    assert_eq!(calls[0].expected_generation.get(), 7);

    h.control
        .set_http("app.example.com", entry("app", InstanceState::Waking, 8));
    settle().await;
    assert!(requests.iter().all(|request| !request.is_finished()));

    h.control
        .set_http("app.example.com", entry("app", InstanceState::Running, 8));
    for request in requests {
        assert_eq!(instance(request.await.unwrap()), "app");
    }

    assert_eq!(h.wake.calls().len(), 1);
    assert_eq!(h.control.subscribes(), 1, "the pushes carried every change");
}

#[tokio::test(start_paused = true)]
async fn a_waking_instance_is_not_woken_again() {
    let h = harness();
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Waking, 8));

    let request = h.spawn_http("app.example.com", "/");
    settle().await;
    assert!(h.wake.calls().is_empty());

    h.control
        .set_http("app.example.com", entry("app", InstanceState::Running, 8));
    assert_eq!(instance(request.await.unwrap()), "app");
    assert!(h.wake.calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_draining_failed_or_backendless_instance_is_woken() {
    for (state, generation) in [(InstanceState::Draining, 9), (InstanceState::Failed, 4)] {
        let h = harness();
        h.control
            .set_http("app.example.com", entry("app", state, generation));
        let request = h.spawn_http("app.example.com", "/");
        settle().await;
        assert_eq!(h.wake.calls().len(), 1, "{state:?}");
        assert_eq!(h.wake.calls()[0].expected_generation.get(), generation);
        request.abort();
    }

    let h = harness();
    let mut backendless = entry("app", InstanceState::Running, 5);
    backendless.backend = None;
    h.control.set_http("app.example.com", backendless);
    let request = h.spawn_http("app.example.com", "/");
    settle().await;
    assert_eq!(h.wake.calls().len(), 1);
    request.abort();
}

#[tokio::test(start_paused = true)]
async fn deleting_and_deleted_instances_are_unavailable_without_a_wake() {
    let h = harness();
    for (host, state, reason) in [
        (
            "deleting.example.com",
            InstanceState::Deleting,
            WakeUnavailableReason::Deleting,
        ),
        (
            "deleted.example.com",
            InstanceState::Deleted,
            WakeUnavailableReason::Deleted,
        ),
    ] {
        h.control.set_http(host, entry("gone", state, 3));
        match h.http(host, "/").await {
            Ok(FrontlineRouteOutcome::Unavailable(unavailable)) => {
                assert_eq!(unavailable.reason, reason)
            }
            other => panic!("expected unavailable, got {other:?}"),
        }
    }
    assert!(h.wake.calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn parked_requests_ask_once_per_interval_when_no_push_arrives() {
    let h = harness();
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Cold, 7));

    let requests: Vec<_> = (0..10)
        .map(|index| h.spawn_http("app.example.com", &format!("/{index}")))
        .collect();
    settle().await;
    assert_eq!(h.control.subscribes(), 1);

    // The instance starts running, and the change never reaches the proxy.
    h.control.set_rules_quietly(
        RouteKind::Http,
        "app.example.com",
        vec![http_rule(
            "app.example.com",
            None,
            entry("app", InstanceState::Running, 8),
        )],
    );
    tokio::time::sleep(Duration::from_millis(1600)).await;

    for request in requests {
        assert_eq!(instance(request.await.unwrap()), "app");
    }
    assert_eq!(
        h.control.subscribes(),
        2,
        "ten parked requests share one refresh"
    );
}

#[tokio::test(start_paused = true)]
async fn a_wake_answer_other_than_still_waking_refreshes_the_host_at_once() {
    let h = harness();
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Cold, 7));
    h.wake
        .respond_with(WakeInstanceResponse::GenerationConflict {
            instance_id: sleepypods_api::InstanceId::new("app").unwrap(),
            expected_generation: sleepypods_api::Generation::new(7),
            actual_generation: sleepypods_api::Generation::new(9),
        });
    // The cached rule set is behind: the instance already runs at a newer
    // generation, and that change never reached the proxy.
    h.control.hold();
    let request = h.spawn_http("app.example.com", "/");
    settle().await;
    h.control.set_rules_quietly(
        RouteKind::Http,
        "app.example.com",
        vec![http_rule(
            "app.example.com",
            None,
            entry("app", InstanceState::Running, 9),
        )],
    );
    let started = tokio::time::Instant::now();
    h.control.release();

    assert_eq!(instance(request.await.unwrap()), "app");
    assert!(started.elapsed() < Duration::from_millis(100));
}

#[tokio::test(start_paused = true)]
async fn sixty_four_identities_on_waking_instances_leave_an_unrelated_miss_served() {
    let h = harness();
    // 32 hosts on their own waking instances and 32 paths on one more.
    let mut parked = Vec::new();
    for index in 0..32 {
        let host = format!("cold{index}.example.com");
        h.control.set_http(
            &host,
            entry(&format!("cold-{index}"), InstanceState::Waking, 3),
        );
        parked.push(h.spawn_http(&host, "/"));
    }
    h.control
        .set_http("paths.example.com", entry("paths", InstanceState::Cold, 3));
    for index in 0..32 {
        parked.push(h.spawn_http("paths.example.com", &format!("/page/{index}")));
    }
    settle().await;
    assert!(parked.iter().all(|request| !request.is_finished()));

    h.control
        .set_http("unrelated.example.com", running("unrelated"));
    let started = tokio::time::Instant::now();
    assert_eq!(
        instance(h.http("unrelated.example.com", "/").await),
        "unrelated"
    );
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(
        h.wake.calls().len(),
        1,
        "only the cold instance needed a call"
    );
    assert_eq!(h.control.subscribes(), 34, "one lookup per host");
}

#[tokio::test(start_paused = true)]
async fn requests_that_leave_while_parked_stop_all_work() {
    let h = harness();
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Cold, 7));

    let requests: Vec<_> = (0..64)
        .map(|index| h.spawn_http("app.example.com", &format!("/{index}")))
        .collect();
    settle().await;
    for request in &requests {
        request.abort();
    }
    settle().await;
    let subscribes = h.control.subscribes();
    let wakes = h.wake.calls().len();

    tokio::time::sleep(Duration::from_secs(30)).await;

    assert_eq!(
        h.control.subscribes(),
        subscribes,
        "no refresh with no waiter"
    );
    assert_eq!(h.wake.calls().len(), wakes);
    // Every waiter slot came back.
    let again: Vec<_> = (0..MAX_ROUTE_WAITERS)
        .map(|index| h.spawn_http("app.example.com", &format!("/again/{index}")))
        .collect();
    settle().await;
    assert!(again.iter().all(|request| !request.is_finished()));
}

#[tokio::test(start_paused = true)]
async fn a_failed_wake_call_fails_the_requests_parked_on_that_instance() {
    let h = harness();
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Cold, 7));
    h.control.set_http("other.example.com", running("other"));
    h.wake.fail_with(TestError("wake refused"));

    let requests: Vec<_> = (0..5)
        .map(|index| h.spawn_http("app.example.com", &format!("/{index}")))
        .collect();
    for request in requests {
        assert_eq!(
            request.await.unwrap(),
            Err(FrontlineRouteCoordinatorError::Wake(TestError(
                "wake refused"
            )))
        );
    }
    assert_eq!(h.wake.calls().len(), 1);
    assert_eq!(instance(h.http("other.example.com", "/").await), "other");
}

#[tokio::test(start_paused = true)]
async fn a_wake_call_with_no_answer_fails_at_its_deadline() {
    let h = harness_with(|coordinator| coordinator.with_wake_deadline(Duration::from_millis(300)));
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Cold, 7));
    h.wake.hang();

    let started = tokio::time::Instant::now();
    assert!(matches!(
        h.http("app.example.com", "/").await,
        Err(FrontlineRouteCoordinatorError::WakeDeadline(_))
    ));
    assert_eq!(started.elapsed(), Duration::from_millis(300));
}

#[tokio::test(start_paused = true)]
async fn a_parked_request_fails_at_the_route_deadline() {
    let h = harness_with(|coordinator| coordinator.with_route_deadline(Duration::from_secs(5)));
    h.control
        .set_http("app.example.com", entry("app", InstanceState::Waking, 7));

    let started = tokio::time::Instant::now();
    assert_eq!(
        h.http("app.example.com", "/").await,
        Err(FrontlineRouteCoordinatorError::RouteDeadline)
    );
    assert_eq!(started.elapsed(), Duration::from_secs(5));
}

#[tokio::test(start_paused = true)]
async fn waiters_are_bounded_and_cached_hosts_stay_served() {
    let h = harness();
    h.control.set_http("hot.example.com", running("hot"));
    assert_eq!(instance(h.http("hot.example.com", "/").await), "hot");
    h.control
        .set_http("cold.example.com", entry("cold", InstanceState::Waking, 7));

    let parked: Vec<_> = (0..MAX_ROUTE_WAITERS)
        .map(|index| h.spawn_http("cold.example.com", &format!("/{index}")))
        .collect();
    settle().await;

    assert_eq!(
        h.http("cold.example.com", "/one-too-many").await,
        Err(FrontlineRouteCoordinatorError::Saturated)
    );
    assert_eq!(instance(h.http("hot.example.com", "/").await), "hot");
    drop(parked);
}

#[tokio::test(start_paused = true)]
async fn hosts_in_flight_are_bounded() {
    let h = harness();
    h.control.hold();
    let flights: Vec<_> = (0..MAX_ROUTE_FLIGHTS)
        .map(|index| {
            let host = format!("h{index}.example.com");
            h.control
                .set_http(&host, running(&format!("instance-{index}")));
            h.spawn_http(&host, "/")
        })
        .collect();
    settle().await;

    assert_eq!(
        h.http("one-too-many.example.com", "/").await,
        Err(FrontlineRouteCoordinatorError::Saturated)
    );

    h.control.release();
    for (index, flight) in flights.into_iter().enumerate() {
        assert_eq!(instance(flight.await.unwrap()), format!("instance-{index}"));
    }
}

#[tokio::test(start_paused = true)]
async fn a_flight_ends_when_its_last_waiter_leaves() {
    let h = harness();
    h.control.hold();
    let flights: Vec<_> = (0..MAX_ROUTE_FLIGHTS)
        .map(|index| h.spawn_http(&format!("h{index}.example.com"), "/"))
        .collect();
    settle().await;

    for flight in &flights {
        flight.abort();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(h.control.unsubscribes(), MAX_ROUTE_FLIGHTS);
    // Every flight slot is free again.
    let request = h.spawn_http("new.example.com", "/");
    settle().await;
    assert_eq!(
        h.control.subscribes_for(RouteKind::Http, "new.example.com"),
        1
    );
    request.abort();
}

#[tokio::test(start_paused = true)]
async fn a_lookup_with_no_answer_fails_at_its_deadline() {
    let h = harness();
    h.control.hold();

    let started = tokio::time::Instant::now();
    assert_eq!(
        h.http("app.example.com", "/").await,
        Err(FrontlineRouteCoordinatorError::SubscribeDeadline)
    );
    assert!(started.elapsed() >= Duration::from_secs(5));
    assert!(started.elapsed() < Duration::from_millis(5100));
    settle().await;
    assert_eq!(h.control.unsubscribes(), 1);
}

#[tokio::test(start_paused = true)]
async fn stream_ends_and_connects_are_reported_by_outcome() {
    use proxy_core::observability::recorder::{InMemoryObservability, ObservabilityEvent};

    let sink = InMemoryObservability::default();
    let h = harness_with(|coordinator| coordinator.with_observability(sink.recorder()));
    h.control.set_http("app.example.com", running("app"));
    assert_eq!(instance(h.http("app.example.com", "/").await), "app");
    let count = |name: &str, labels: &[&str]| {
        sink.events()
            .iter()
            .filter(|event| match event {
                ObservabilityEvent::Metric(metric) => {
                    metric.name() == name
                        && metric
                            .labels()
                            .iter()
                            .map(|label| label.value())
                            .eq(labels.iter().copied())
                }
                ObservabilityEvent::Log(_) => false,
            })
            .count()
    };
    let streams = "sleepypods_runtime_subscribe_stream_events_total";
    let calls = "sleepypods_runtime_control_plane_calls_total";

    // A rotation, then a failure whose first reconnect is refused.
    h.control.end_streams();
    settle().await;
    h.control.fail_next_connects(1);
    h.control.fail_streams();
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(count(streams, &["closed"]), 1, "one orderly end");
    assert_eq!(count(streams, &["error"]), 1, "one failed stream");
    assert_eq!(count(calls, &["subscribe_stream", "success"]), 3);
    assert_eq!(count(calls, &["subscribe_stream", "error"]), 1);
}

#[tokio::test(start_paused = true)]
async fn an_orderly_stream_end_keeps_hosts_serving_and_registers_them_again() {
    let h = harness();
    h.warm(250).await;
    // The replacement stream answers nothing yet.
    h.control.hold();
    let started = tokio::time::Instant::now();
    h.control.end_streams();
    settle().await;

    assert_eq!(h.control.connects(), 2, "the proxy reconnects at once");
    assert!(started.elapsed() < Duration::from_millis(10));
    for index in 0..250 {
        assert_eq!(
            instance(h.http(&format!("h{index}.example.com"), "/").await),
            format!("instance-{index}"),
        );
    }
    assert_eq!(h.shared.lookup_counters().totals().1, 250, "no new miss");

    h.control.release();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(h.control.subscribes(), 500);
    assert_eq!(h.control.live_subscriptions(), 250);
}

#[tokio::test(start_paused = true)]
async fn a_host_no_request_used_since_its_registration_leaves_at_the_next_stream_end() {
    let h = harness();
    h.warm(2).await;
    h.control.end_streams();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.control.subscribes(), 4);

    // Only one host is used on the second stream.
    assert_eq!(instance(h.http("h0.example.com", "/").await), "instance-0");
    h.control.end_streams();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(h.control.subscribes(), 5);
    assert!(h.control.is_subscribed(RouteKind::Http, "h0.example.com"));
    assert!(!h.control.is_subscribed(RouteKind::Http, "h1.example.com"));
}

#[tokio::test(start_paused = true)]
async fn registration_after_a_stream_end_leaves_flights_for_new_hosts() {
    let h = harness();
    h.warm(MAX_ROUTE_FLIGHTS * 2).await;
    h.control.hold();
    h.control.end_streams();
    settle().await;

    h.control.set_http("new.example.com", running("new"));
    let request = h.spawn_http("new.example.com", "/");
    settle().await;
    assert_eq!(
        h.control.subscribes_for(RouteKind::Http, "new.example.com"),
        1
    );

    h.control.release();
    assert_eq!(instance(request.await.unwrap()), "new");
}

#[tokio::test(start_paused = true)]
async fn a_flight_carries_over_a_stream_that_ends() {
    let h = harness();
    h.control.set_http("app.example.com", running("app"));
    h.control.hold();
    let request = h.spawn_http("app.example.com", "/");
    settle().await;

    h.control.end_streams();
    settle().await;
    h.control.release();

    assert_eq!(instance(request.await.unwrap()), "app");
    assert_eq!(
        h.control.subscribes_for(RouteKind::Http, "app.example.com"),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_stream_clears_the_cache_before_the_next_request_is_served() {
    let h = harness();
    h.warm(10).await;
    h.control.hold();

    let started = tokio::time::Instant::now();
    h.control.fail_streams();
    settle().await;

    // The cached rule set is gone: the request waits for a new lookup.
    let request = h.spawn_http("h3.example.com", "/");
    settle().await;
    assert!(!request.is_finished());
    assert_eq!(
        h.control.connects(),
        1,
        "a failed stream waits before reconnecting"
    );

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(h.control.connects(), 2);
    assert!(started.elapsed() >= Duration::from_millis(250));
    h.control.release();
    assert_eq!(instance(request.await.unwrap()), "instance-3");
    assert_eq!(
        h.control.subscribes(),
        11,
        "only the requested host resolved again"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_connect_keeps_hosts_carried_over_an_orderly_end() {
    let h = harness();
    h.warm(5).await;
    h.control.fail_next_connects(3);
    h.control.end_streams();
    settle().await;

    for index in 0..5 {
        assert_eq!(
            instance(h.http(&format!("h{index}.example.com"), "/").await),
            format!("instance-{index}")
        );
    }
    assert_eq!(h.control.subscribes(), 5);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        h.control.connects(),
        5,
        "three refused connects, then one accepted"
    );
    assert_eq!(h.control.live_subscriptions(), 5);
}

#[tokio::test(start_paused = true)]
async fn a_rule_for_another_host_fails_the_stream_and_is_not_cached() {
    let h = harness();
    h.control.set_rules(
        RouteKind::Http,
        "a.example.com",
        vec![http_rule("b.example.com", None, running("wrong"))],
    );

    assert!(h.http("a.example.com", "/").await.is_err());
    assert_eq!(
        h.http("b.example.com", "/").await,
        Ok(FrontlineRouteOutcome::Miss),
        "nothing was cached for the other host"
    );
}

#[tokio::test(start_paused = true)]
async fn the_actor_stops_when_every_handle_is_gone_and_a_stopped_actor_refuses_requests() {
    let h = harness();
    h.control.set_http("app.example.com", running("app"));
    assert_eq!(instance(h.http("app.example.com", "/").await), "app");

    h.shared.shared.actor.0.abort();
    settle().await;

    assert_eq!(
        h.http("app.example.com", "/").await,
        Err(FrontlineRouteCoordinatorError::RouteActorClosed)
    );
}
