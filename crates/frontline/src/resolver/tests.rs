use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use proxy_core::observability::{
    metrics::{
        RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME, RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME,
        RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL_NAME,
    },
    recorder::{
        InMemoryObservability, ObservabilityEvent, EVENT_ROUTE_CACHE_LOOKUP, FIELD_ROUTE_ID,
        FIELD_SUBSCRIPTION_ID,
    },
};
use sleepypods_api::{CachePolicy, PathPrefix, RouteHost, RouteIdentity};

use super::{
    FrontlineRouteResolution, FrontlineRouteResolver, FrontlineRouteResolverError,
    RouteResolverProtocolError, RouteSubscriptionClient, RouteSubscriptionEvent,
    RouteSubscriptionFuture, UnexpectedSubscribeResponseKind,
};
use crate::{
    subscription::tests::route_entry, ApplyControlPlaneMessageOutcome, ApplyUpdateOutcome,
    InvalidationReason, RouteRequestId, SubscribeControlPlaneOutput, SubscriptionId,
    SubscriptionState,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestClientError {
    SubscribeFailed,
    UnsubscribeFailed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClientCall {
    Subscribe {
        request_id: RouteRequestId,
        identity: RouteIdentity,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

#[derive(Clone, Debug, Default)]
struct FakeRouteSubscriptionClient {
    calls: Vec<ClientCall>,
    subscribe_responses: VecDeque<Result<SubscribeControlPlaneOutput, TestClientError>>,
    unsubscribe_responses: VecDeque<Result<(), TestClientError>>,
    events: VecDeque<Result<Vec<RouteSubscriptionEvent>, TestClientError>>,
}

impl FakeRouteSubscriptionClient {
    fn with_subscribe_response(response: SubscribeControlPlaneOutput) -> Self {
        let mut client = Self::default();
        client.subscribe_responses.push_back(Ok(response));
        client
    }

    fn with_subscribe_error(error: TestClientError) -> Self {
        let mut client = Self::default();
        client.subscribe_responses.push_back(Err(error));
        client
    }

    fn push_subscribe_response(&mut self, response: SubscribeControlPlaneOutput) {
        self.subscribe_responses.push_back(Ok(response));
    }

    fn push_unsubscribe_response(&mut self, response: Result<(), TestClientError>) {
        self.unsubscribe_responses.push_back(response);
    }

    fn push_events(&mut self, events: Vec<RouteSubscriptionEvent>) {
        self.events.push_back(Ok(events));
    }
}

impl RouteSubscriptionClient for FakeRouteSubscriptionClient {
    type Error = TestClientError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, SubscribeControlPlaneOutput, Self::Error> {
        self.calls.push(ClientCall::Subscribe {
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
        self.calls.push(ClientCall::Unsubscribe { subscription_id });
        let response = self.unsubscribe_responses.pop_front().unwrap_or(Ok(()));
        Box::pin(async move { response })
    }

    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'static, Vec<RouteSubscriptionEvent>, Self::Error> {
        let response = self.events.pop_front().unwrap_or(Ok(Vec::new()));
        Box::pin(async move { response })
    }
}

fn now() -> Instant {
    Instant::now()
}

fn ttl(seconds: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_secs(seconds))
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription")
}

fn request_id(value: &str) -> RouteRequestId {
    RouteRequestId::new(value).expect("request ID")
}

fn generated_request_id(index: u64) -> RouteRequestId {
    request_id(&format!("req:{index}"))
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

fn resolved_response(
    request_id: RouteRequestId,
    subscription_id: SubscriptionId,
    matched_identity: RouteIdentity,
    route: &str,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteResolved {
        request_id,
        subscription_id,
        matched_identity,
        entry: route_entry(route, 1, None),
        cache_policy: ttl(10),
    }
}

fn miss_response(
    request_id: RouteRequestId,
    identity: RouteIdentity,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteMiss {
        request_id,
        request_identity: identity,
        negative_cache_policy: ttl(10),
    }
}

#[tokio::test]
async fn positive_cache_hit_returns_cached_route_without_client_call() {
    let now = now();
    let request = http_request("app.example.com", "/api/users");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", Some("/api")),
            "route-1",
        ),
        now,
    );
    let mut resolver =
        FrontlineRouteResolver::from_parts(state, FakeRouteSubscriptionClient::default());

    let result = resolver
        .resolve(request, now)
        .await
        .expect("positive cache hit");

    let FrontlineRouteResolution::Resolved(entry) = result else {
        panic!("expected resolved route");
    };
    assert_eq!(entry.entry.route_binding_id.as_str(), "route-1");
    assert!(resolver.client().calls.is_empty());
}

#[tokio::test]
async fn positive_cache_hit_records_lookup_metric_and_route_fields() {
    let now = now();
    let sink = InMemoryObservability::default();
    let request = http_request("app.example.com", "/api/users");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", Some("/api")),
            "route-1",
        ),
        now,
    );
    let mut resolver = FrontlineRouteResolver::from_parts_with_observability(
        state,
        FakeRouteSubscriptionClient::default(),
        sink.recorder(),
    );

    resolver.resolve(request, now).await.expect("cache hit");

    let events = sink.events();
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "hit")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log)
            if log.name() == EVENT_ROUTE_CACHE_LOOKUP
                && log.field_value(FIELD_ROUTE_ID) == Some("route-1")
                && log.field_value(FIELD_SUBSCRIPTION_ID) == Some("sub-1")
    )));
}

#[tokio::test]
async fn negative_cache_hit_returns_miss_without_client_call() {
    let now = now();
    let request = http_request("missing.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(miss_response(request_id("initial"), request.clone()), now);
    let mut resolver =
        FrontlineRouteResolver::from_parts(state, FakeRouteSubscriptionClient::default());

    let result = resolver
        .resolve(request.clone(), now)
        .await
        .expect("negative cache hit");

    let FrontlineRouteResolution::Miss(entry) = result else {
        panic!("expected miss");
    };
    assert_eq!(entry.request_identity, request);
    assert!(resolver.client().calls.is_empty());
}

#[tokio::test]
async fn absent_route_subscribes_installs_resolved_route_and_reuses_cache() {
    let now = now();
    let request = http_request("app.example.com", "/api/users");
    let client = FakeRouteSubscriptionClient::with_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-1"),
        http_rule("example.com", Some("/api")),
        "route-1",
    ));
    let mut resolver = FrontlineRouteResolver::new(4, client);

    let result = resolver
        .resolve(request.clone(), now)
        .await
        .expect("subscribe resolves route");

    let FrontlineRouteResolution::Resolved(entry) = result else {
        panic!("expected resolved route");
    };
    assert_eq!(entry.entry.route_binding_id.as_str(), "route-1");
    assert_eq!(
        resolver.client().calls,
        vec![ClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request.clone()
        }]
    );

    resolver
        .resolve(request, now)
        .await
        .expect("later lookup uses cache");
    assert_eq!(resolver.client().calls.len(), 1);
}

#[tokio::test]
async fn absent_route_records_cache_miss_and_control_plane_call() {
    let now = now();
    let sink = InMemoryObservability::default();
    let request = http_request("app.example.com", "/api/users");
    let client = FakeRouteSubscriptionClient::with_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-1"),
        http_rule("example.com", Some("/api")),
        "route-1",
    ));
    let mut resolver = FrontlineRouteResolver::with_observability(4, client, sink.recorder());

    resolver
        .resolve(request, now)
        .await
        .expect("subscribe resolves");

    let events = sink.events();
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_ROUTE_CACHE_LOOKUPS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "miss")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "subscribe_route")
                && metric.labels().iter().any(|label| label.value() == "success")
    )));
}

#[tokio::test]
async fn absent_route_subscribes_installs_miss_and_reuses_negative_cache() {
    let now = now();
    let request = http_request("missing.example.com", "/");
    let client = FakeRouteSubscriptionClient::with_subscribe_response(miss_response(
        generated_request_id(1),
        request.clone(),
    ));
    let mut resolver = FrontlineRouteResolver::new(4, client);

    let result = resolver
        .resolve(request.clone(), now)
        .await
        .expect("subscribe returns miss");

    let FrontlineRouteResolution::Miss(entry) = result else {
        panic!("expected miss");
    };
    assert_eq!(entry.request_identity, request);
    resolver
        .resolve(entry.request_identity.clone(), now)
        .await
        .expect("later lookup uses negative cache");
    assert_eq!(resolver.client().calls.len(), 1);
}

#[tokio::test]
async fn expired_positive_unsubscribes_best_effort_after_refresh() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.cache_mut().insert_positive(
        subscription_id("sub-old"),
        http_rule("example.com", None),
        route_entry("route-old", 1, None),
        ttl(1),
        now,
    );
    let mut client = FakeRouteSubscriptionClient::with_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-new"),
        http_rule("example.com", None),
        "route-new",
    ));
    client.push_unsubscribe_response(Ok(()));
    let mut resolver = FrontlineRouteResolver::from_parts(state, client);

    resolver
        .resolve(request.clone(), now + Duration::from_secs(1))
        .await
        .expect("expired route refreshes");

    assert_eq!(
        resolver.client().calls,
        vec![
            ClientCall::Subscribe {
                request_id: generated_request_id(1),
                identity: request
            },
            ClientCall::Unsubscribe {
                subscription_id: subscription_id("sub-old")
            },
        ]
    );
}

#[tokio::test]
async fn capacity_eviction_unsubscribes_evicted_positive_subscription() {
    let now = now();
    let request = http_request("app.two.example.com", "/");
    let mut state = SubscriptionState::new(1);
    state.apply_resolved_response(
        http_request("app.one.example.com", "/"),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-old"),
            http_rule("one.example.com", None),
            "route-old",
        ),
        now,
    );
    let client = FakeRouteSubscriptionClient::with_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-new"),
        http_rule("two.example.com", None),
        "route-new",
    ));
    let mut resolver = FrontlineRouteResolver::from_parts(state, client);

    resolver
        .resolve(request, now)
        .await
        .expect("subscribe evicts old entry");

    assert_eq!(
        resolver.client().calls,
        vec![
            ClientCall::Subscribe {
                request_id: generated_request_id(1),
                identity: http_request("app.two.example.com", "/")
            },
            ClientCall::Unsubscribe {
                subscription_id: subscription_id("sub-old")
            }
        ]
    );
}

#[tokio::test]
async fn mismatched_request_id_in_subscribe_response_is_rejected() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let client = FakeRouteSubscriptionClient::with_subscribe_response(resolved_response(
        request_id("not-generated-here"),
        subscription_id("sub-1"),
        http_rule("example.com", None),
        "route-1",
    ));
    let mut resolver = FrontlineRouteResolver::new(4, client);

    let error = resolver
        .resolve(request, now)
        .await
        .expect_err("mismatched request ID");

    assert_eq!(
        error,
        FrontlineRouteResolverError::Protocol(RouteResolverProtocolError::MismatchedRequestId {
            expected: generated_request_id(1),
            actual: request_id("not-generated-here")
        })
    );
    assert!(resolver.state().cache().is_empty());
}

#[tokio::test]
async fn unexpected_direct_route_updated_response_is_rejected() {
    let now = now();
    let client = FakeRouteSubscriptionClient::with_subscribe_response(
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry("route-1", 1, None),
            cache_policy: ttl(10),
        },
    );
    let mut resolver = FrontlineRouteResolver::new(4, client);

    let error = resolver
        .resolve(http_request("app.example.com", "/"), now)
        .await
        .expect_err("unexpected update");

    assert_eq!(
        error,
        FrontlineRouteResolverError::Protocol(
            RouteResolverProtocolError::UnexpectedSubscribeResponse {
                kind: UnexpectedSubscribeResponseKind::RouteUpdated
            }
        )
    );
}

#[tokio::test]
async fn unexpected_direct_route_invalidated_response_is_rejected() {
    let now = now();
    let client = FakeRouteSubscriptionClient::with_subscribe_response(
        SubscribeControlPlaneOutput::RouteInvalidated {
            subscription_id: subscription_id("sub-1"),
            reason: InvalidationReason::RouteChanged,
        },
    );
    let mut resolver = FrontlineRouteResolver::new(4, client);

    let error = resolver
        .resolve(http_request("app.example.com", "/"), now)
        .await
        .expect_err("unexpected invalidation");

    assert_eq!(
        error,
        FrontlineRouteResolverError::Protocol(
            RouteResolverProtocolError::UnexpectedSubscribeResponse {
                kind: UnexpectedSubscribeResponseKind::RouteInvalidated
            }
        )
    );
}

#[tokio::test]
async fn subscribe_client_error_is_surfaced() {
    let now = now();
    let client =
        FakeRouteSubscriptionClient::with_subscribe_error(TestClientError::SubscribeFailed);
    let mut resolver = FrontlineRouteResolver::new(4, client);

    let error = resolver
        .resolve(http_request("missing.example.com", "/"), now)
        .await
        .expect_err("subscribe error");

    assert_eq!(
        error,
        FrontlineRouteResolverError::Subscribe(TestClientError::SubscribeFailed)
    );
}

#[tokio::test]
async fn unsubscribe_client_error_does_not_fail_refresh() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.cache_mut().insert_positive(
        subscription_id("sub-old"),
        http_rule("example.com", None),
        route_entry("route-old", 1, None),
        ttl(1),
        now,
    );
    let mut client = FakeRouteSubscriptionClient::default();
    client.push_unsubscribe_response(Err(TestClientError::UnsubscribeFailed));
    client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-new"),
        http_rule("example.com", None),
        "route-new",
    ));
    let mut resolver = FrontlineRouteResolver::from_parts(state, client);

    let result = resolver
        .resolve(
            http_request("app.example.com", "/"),
            now + Duration::from_secs(1),
        )
        .await
        .expect("unsubscribe failure is best effort");

    assert!(matches!(result, FrontlineRouteResolution::Resolved(_)));
    assert_eq!(
        resolver.client().calls,
        vec![
            ClientCall::Subscribe {
                request_id: generated_request_id(1),
                identity: http_request("app.example.com", "/")
            },
            ClientCall::Unsubscribe {
                subscription_id: subscription_id("sub-old")
            },
        ]
    );
}

#[tokio::test]
async fn apply_control_plane_message_handles_update_and_invalidation_without_subscribe() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            "route-old",
        ),
        now,
    );
    let mut resolver =
        FrontlineRouteResolver::from_parts(state, FakeRouteSubscriptionClient::default());

    let update = resolver
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteUpdated {
                subscription_id: subscription_id("sub-1"),
                matched_identity: http_rule("example.com", Some("/api")),
                entry: route_entry("route-new", 2, None),
                cache_policy: ttl(10),
            },
            now,
        )
        .await
        .expect("update applies");
    assert_eq!(
        update,
        ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::Replaced(
            crate::CacheInsertResult::default()
        ))
    );

    let invalidation = resolver
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-1"),
                reason: InvalidationReason::RouteChanged,
            },
            now,
        )
        .await
        .expect("invalidation applies");
    assert_eq!(
        invalidation,
        ApplyControlPlaneMessageOutcome::Invalidated { removed: true }
    );
    assert!(resolver.client().calls.is_empty());
}

#[tokio::test]
async fn apply_control_plane_message_unsubscribes_evicted_subscriptions() {
    let now = now();
    let mut state = SubscriptionState::new(1);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-old"),
            http_rule("one.example.com", None),
            "route-old",
        ),
        now,
    );
    let mut resolver =
        FrontlineRouteResolver::from_parts(state, FakeRouteSubscriptionClient::default());

    resolver
        .apply_control_plane_message(
            resolved_response(
                request_id("push"),
                subscription_id("sub-new"),
                http_rule("two.example.com", None),
                "route-new",
            ),
            now,
        )
        .await
        .expect("evicting push applies");

    assert_eq!(
        resolver.client().calls,
        vec![ClientCall::Unsubscribe {
            subscription_id: subscription_id("sub-old")
        }]
    );
}

#[tokio::test]
async fn stream_close_event_invalidates_hot_positive_before_ttl_and_lazily_rebuilds() {
    let now = now();
    let sink = InMemoryObservability::default();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_resolved_response(
        request.clone(),
        resolved_response(
            request_id("initial"),
            subscription_id("sub-old"),
            http_rule("example.com", None),
            "route-old",
        ),
        now,
    );
    let mut client = FakeRouteSubscriptionClient::with_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-new"),
        http_rule("example.com", None),
        "route-new",
    ));
    client.push_events(vec![RouteSubscriptionEvent::StreamClosed]);
    let mut resolver =
        FrontlineRouteResolver::from_parts_with_observability(state, client, sink.recorder());

    let result = resolver
        .resolve(request.clone(), now)
        .await
        .expect("stream close forces lazy rebuild");

    let FrontlineRouteResolution::Resolved(entry) = result else {
        panic!("expected rebuilt positive route");
    };
    assert_eq!(entry.subscription_id, Some(subscription_id("sub-new")));
    assert_eq!(entry.entry.route_binding_id.as_str(), "route-new");
    assert!(resolver
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-old"))
        .is_none());
    assert_eq!(
        resolver.client().calls,
        vec![ClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request,
        }]
    );
    assert!(sink.events().iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_SUBSCRIBE_STREAM_EVENTS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "closed")
    )));
}
