use std::time::{Duration, Instant};

use sleepypods_api::{
    BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState, PathPrefix,
    RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
};

use super::{
    ApplyControlPlaneMessageOutcome, ApplyUpdateOutcome, InvalidationReason, ProxySubscribeInput,
    RouteRequestId, SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState,
    UnsubscribeOutcome,
};
use crate::{CacheLookupStatus, RouteRequestIdentity};

pub(crate) fn route_entry(
    route_binding_id: &str,
    instance_generation: u64,
    backend_generation: Option<u64>,
) -> RouteEntry {
    route_entry_for_instance(
        route_binding_id,
        &format!("instance-{route_binding_id}"),
        instance_generation,
        backend_generation,
    )
}

pub(crate) fn route_entry_for_instance(
    route_binding_id: &str,
    instance_id: &str,
    instance_generation: u64,
    backend_generation: Option<u64>,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: RouteBindingId::new(route_binding_id).expect("route binding ID"),
        instance_id: InstanceId::new(instance_id).expect("instance ID"),
        instance_state: InstanceState::Running,
        instance_generation: Generation::new(instance_generation),
        backend: None,
        backend_generation: backend_generation.map(BackendGeneration::new),
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

fn http_request(host: &str, path: &str) -> RouteIdentity {
    RouteRequestIdentity::http(host, Some(path))
        .expect("request identity")
        .into_identity()
}

fn http_rule(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(host).expect("valid wildcard"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path")),
    }
}

#[test]
fn route_resolved_inserts_positive_cache_with_subscription_id() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    let outcome = state.apply_resolved_response(
        http_request("app.example.com", "/api/users"),
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", Some("/api")),
            entry: route_entry("route-1", 1, None),
            cache_policy: ttl(10),
        },
        now,
    );

    assert!(matches!(
        outcome,
        ApplyControlPlaneMessageOutcome::Resolved(_)
    ));
    assert_eq!(
        state
            .cache()
            .lookup(&http_request("app.example.com", "/api/users"), now)
            .status(),
        CacheLookupStatus::PositiveHit
    );
    assert!(state
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .is_some());
}

#[test]
fn route_miss_inserts_negative_cache_without_subscription_id() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    let request = http_request("missing.example.com", "/");
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteMiss {
            request_id: request_id("req-1"),
            request_identity: request.clone(),
            negative_cache_policy: ttl(5),
        },
        now,
    );

    assert_eq!(state.cache().positives().count(), 0);
    assert_eq!(state.cache().negatives().len(), 1);
    assert_eq!(
        state.cache().lookup(&request, now).status(),
        CacheLookupStatus::NegativeHit
    );
}

#[test]
fn newer_duplicate_resolve_for_same_rule_replaces_old_subscription() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    let matched_identity = http_rule("example.com", Some("/api"));
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-old"),
            matched_identity: matched_identity.clone(),
            entry: route_entry_for_instance("route-old", "instance-a", 1, Some(1)),
            cache_policy: ttl(10),
        },
        now,
    );

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-2"),
            subscription_id: subscription_id("sub-new"),
            matched_identity: matched_identity.clone(),
            entry: route_entry_for_instance("route-new", "instance-a", 2, Some(2)),
            cache_policy: ttl(10),
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Resolved(crate::CacheInsertResult {
            subscriptions_to_unsubscribe: vec![subscription_id("sub-old")]
        })
    );
    assert!(state
        .cache()
        .positive_by_subscription(&subscription_id("sub-old"))
        .is_none());
    assert_eq!(
        state
            .cache()
            .positive_by_subscription(&subscription_id("sub-new"))
            .expect("new entry")
            .entry
            .route_binding_id
            .as_str(),
        "route-new"
    );
}

#[test]
fn duplicate_resolved_response_for_same_identity_keeps_one_active_subscription() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    let matched_identity = http_rule("example.com", Some("/api"));

    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-first"),
            matched_identity: matched_identity.clone(),
            entry: route_entry_for_instance("route-current", "instance-a", 3, Some(7)),
            cache_policy: ttl(10),
        },
        now,
    );

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-2"),
            subscription_id: subscription_id("sub-duplicate"),
            matched_identity: matched_identity.clone(),
            entry: route_entry_for_instance("route-current", "instance-a", 3, Some(7)),
            cache_policy: ttl(10),
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Resolved(crate::CacheInsertResult {
            subscriptions_to_unsubscribe: vec![subscription_id("sub-first")]
        })
    );
    assert!(state
        .cache()
        .positive_by_subscription(&subscription_id("sub-first"))
        .is_none());
    assert_eq!(state.cache().positives().count(), 1);
    assert_eq!(
        state
            .cache()
            .positive_by_subscription(&subscription_id("sub-duplicate"))
            .expect("duplicate response becomes active")
            .matched_identity,
        matched_identity
    );
}

#[test]
fn stale_duplicate_resolve_for_same_rule_is_not_installed() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    let matched_identity = http_rule("example.com", Some("/api"));
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-new"),
            subscription_id: subscription_id("sub-new"),
            matched_identity: matched_identity.clone(),
            entry: route_entry_for_instance("route-new", "instance-a", 7, Some(4)),
            cache_policy: ttl(10),
        },
        now,
    );

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-old"),
            subscription_id: subscription_id("sub-old"),
            matched_identity,
            entry: route_entry_for_instance("route-old", "instance-a", 6, Some(5)),
            cache_policy: ttl(10),
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Resolved(crate::CacheInsertResult {
            subscriptions_to_unsubscribe: vec![subscription_id("sub-old")]
        })
    );
    assert!(state
        .cache()
        .positive_by_subscription(&subscription_id("sub-old"))
        .is_none());
    assert_eq!(
        state
            .cache()
            .positive_by_subscription(&subscription_id("sub-new"))
            .expect("newer entry")
            .entry
            .route_binding_id
            .as_str(),
        "route-new"
    );
}

#[test]
fn invalidation_removes_only_targeted_subscription() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    for (sub, route, host) in [
        ("sub-1", "route-1", "one.example.com"),
        ("sub-2", "route-2", "two.example.com"),
    ] {
        state.apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteResolved {
                request_id: request_id(route),
                subscription_id: subscription_id(sub),
                matched_identity: http_rule(host, None),
                entry: route_entry(route, 1, None),
                cache_policy: ttl(10),
            },
            now,
        );
    }

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteInvalidated {
            subscription_id: subscription_id("sub-1"),
            reason: InvalidationReason::RouteChanged,
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Invalidated { removed: true }
    );
    assert!(state
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .is_none());
    assert!(state
        .cache()
        .positive_by_subscription(&subscription_id("sub-2"))
        .is_some());
}

#[test]
fn update_replaces_targeted_entry() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry("route-old", 1, Some(1)),
            cache_policy: ttl(10),
        },
        now,
    );

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", Some("/api")),
            entry: route_entry("route-new", 2, Some(2)),
            cache_policy: ttl(20),
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::Replaced(
            crate::CacheInsertResult::default()
        ))
    );
    let entry = state
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("updated entry");
    assert_eq!(entry.entry.route_binding_id.as_str(), "route-new");
}

#[test]
fn duplicate_unsubscribe_is_idempotent() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry("route-1", 1, None),
            cache_policy: ttl(10),
        },
        now,
    );

    assert_eq!(
        state.apply_proxy_input(ProxySubscribeInput::Unsubscribe {
            subscription_id: subscription_id("sub-1")
        }),
        UnsubscribeOutcome::Removed
    );
    assert_eq!(
        state.apply_proxy_input(ProxySubscribeInput::Unsubscribe {
            subscription_id: subscription_id("sub-1")
        }),
        UnsubscribeOutcome::AlreadyAbsent
    );
}

#[test]
fn stale_update_rejects_lower_instance_generation() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry_for_instance("route-current", "instance-a", 7, Some(4)),
            cache_policy: ttl(10),
        },
        now,
    );

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry_for_instance("route-stale", "instance-a", 6, Some(5)),
            cache_policy: ttl(10),
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::StaleInstanceGeneration {
            current: Generation::new(7),
            incoming: Generation::new(6)
        })
    );
}

#[test]
fn stale_update_rejects_lower_backend_generation_when_present() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry_for_instance("route-current", "instance-a", 7, Some(4)),
            cache_policy: ttl(10),
        },
        now,
    );

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry_for_instance("route-stale", "instance-a", 7, Some(3)),
            cache_policy: ttl(10),
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::StaleBackendGeneration {
            current: BackendGeneration::new(4),
            incoming: BackendGeneration::new(3)
        })
    );
}

#[test]
fn update_accepts_reassignment_to_different_instance_with_lower_generation() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry_for_instance("route-current", "instance-a", 7, Some(4)),
            cache_policy: ttl(10),
        },
        now,
    );

    let outcome = state.apply_control_plane_message(
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_rule("example.com", None),
            entry: route_entry_for_instance("route-reassigned", "instance-b", 1, Some(1)),
            cache_policy: ttl(10),
        },
        now,
    );

    assert_eq!(
        outcome,
        ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::Replaced(
            crate::CacheInsertResult::default()
        ))
    );
    let entry = state
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("updated entry");
    assert_eq!(entry.entry.instance_id.as_str(), "instance-b");
    assert_eq!(entry.entry.instance_generation, Generation::new(1));
}
