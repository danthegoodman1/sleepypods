use std::time::Duration;

use sleepypods_api::{
    pb, BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteHostKind, RouteIdentity,
};

use super::{
    proxy_subscribe_input_to_proto, proxy_subscribe_response_from_proto,
    proxy_wake_response_from_proto, wake_instance_request_to_proto, ProxyProtocolAdapterError,
};
use crate::{
    InvalidationReason, ProxySubscribeInput, RouteRequestId, SubscribeControlPlaneOutput,
    SubscriptionId, WakeInstanceRequest, WakeInstanceResponse,
};

fn request_id(value: &str) -> RouteRequestId {
    RouteRequestId::new(value).expect("request ID")
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription ID")
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

fn route_binding_id(value: &str) -> RouteBindingId {
    RouteBindingId::new(value).expect("route binding ID")
}

fn backend(value: &str) -> BackendEndpoint {
    BackendEndpoint::new(value).expect("backend")
}

fn http_identity(host: &str, path: Option<&str>, wildcard: bool) -> RouteIdentity {
    let host = if wildcard {
        RouteHost::wildcard_suffix(host).expect("wildcard host")
    } else {
        RouteHost::exact(host).expect("exact host")
    };

    RouteIdentity::Http {
        host,
        path: path.map(|path| PathPrefix::new(path).expect("path")),
    }
}

fn sni_identity(host: &str, wildcard: bool) -> RouteIdentity {
    let host = if wildcard {
        RouteHost::wildcard_suffix(host).expect("wildcard host")
    } else {
        RouteHost::exact(host).expect("exact host")
    };

    RouteIdentity::Sni { host }
}

fn pb_http_identity(host: &str, path: Option<&str>, kind: pb::RouteHostKind) -> pb::RouteIdentity {
    pb::RouteIdentity {
        kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
            host: Some(pb::RouteHost {
                kind: kind as i32,
                host: host.to_owned(),
            }),
            path_prefix: path.map(str::to_owned),
        })),
    }
}

fn pb_sni_identity(host: &str, kind: pb::RouteHostKind) -> pb::RouteIdentity {
    pb::RouteIdentity {
        kind: Some(pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
            host: Some(pb::RouteHost {
                kind: kind as i32,
                host: host.to_owned(),
            }),
        })),
    }
}

fn route_entry() -> RouteEntry {
    RouteEntry {
        route_binding_id: route_binding_id("route-a"),
        instance_id: instance_id("instance-a"),
        instance_state: InstanceState::Running,
        instance_generation: Generation::new(7),
        backend: Some(backend("http://10.0.0.7:8080")),
        backend_generation: Some(BackendGeneration::new(3)),
    }
}

fn pb_route_entry() -> pb::ProxyRouteEntry {
    pb::ProxyRouteEntry {
        backend_address: None,
        route_binding_id: "route-a".to_owned(),
        instance_id: "instance-a".to_owned(),
        instance_state: pb::InstanceState::Running as i32,
        instance_generation: 7,
        backend_uri: Some("http://10.0.0.7:8080".to_owned()),
        backend_generation: Some(3),
    }
}

fn cache_policy(ttl_millis: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_millis(ttl_millis))
}

fn pb_cache_policy(ttl_millis: u64) -> pb::ProxyCachePolicy {
    pb::ProxyCachePolicy { ttl_millis }
}

#[test]
fn subscribe_route_input_converts_to_proto() {
    let proto = proxy_subscribe_input_to_proto(ProxySubscribeInput::SubscribeRoute {
        request_id: request_id("req:opaque/1"),
        identity: http_identity("app.example.com", Some("/api"), false),
    });

    match proto.input.expect("input") {
        pb::proxy_subscribe_request::Input::SubscribeRoute(request) => {
            assert_eq!(request.request_id, "req:opaque/1");
            let identity = request.identity.expect("identity");
            match identity.kind.expect("kind") {
                pb::route_identity::Kind::Http(http) => {
                    let host = http.host.expect("host");
                    assert_eq!(host.kind, pb::RouteHostKind::Exact as i32);
                    assert_eq!(host.host, "app.example.com");
                    assert_eq!(http.path_prefix.as_deref(), Some("/api"));
                }
                pb::route_identity::Kind::Sni(_) => panic!("expected HTTP identity"),
            }
        }
        pb::proxy_subscribe_request::Input::Unsubscribe(_) => panic!("expected subscribe route"),
    }
}

#[test]
fn unsubscribe_input_converts_to_proto_with_opaque_subscription_id() {
    let proto = proxy_subscribe_input_to_proto(ProxySubscribeInput::Unsubscribe {
        subscription_id: subscription_id("sub:not-a-route-id"),
    });

    match proto.input.expect("input") {
        pb::proxy_subscribe_request::Input::Unsubscribe(request) => {
            assert_eq!(request.subscription_id, "sub:not-a-route-id");
        }
        pb::proxy_subscribe_request::Input::SubscribeRoute(_) => panic!("expected unsubscribe"),
    }
}

#[test]
fn wake_request_converts_to_proto() {
    let proto = wake_instance_request_to_proto(WakeInstanceRequest {
        instance_id: instance_id("instance-a"),
        expected_generation: Generation::new(4),
    });

    assert_eq!(proto.instance_id, "instance-a");
    assert_eq!(proto.expected_generation, 4);
    assert_eq!(proto.backend_generation, None);
}

#[test]
fn route_resolved_response_preserves_route_identity_entry_cache_and_ids() {
    let output = proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
            pb::ProxyRouteResolvedResponse {
                request_id: "req:opaque/1".to_owned(),
                subscription_id: "sub:opaque/route-a-is-not-parsed".to_owned(),
                matched_identity: Some(pb_http_identity(
                    "*.Example.COM.",
                    Some("/api"),
                    pb::RouteHostKind::WildcardSuffix,
                )),
                route: Some(pb_route_entry()),
                cache_policy: Some(pb_cache_policy(12_345)),
            },
        )),
    })
    .expect("valid response");

    assert_eq!(
        output,
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: request_id("req:opaque/1"),
            subscription_id: subscription_id("sub:opaque/route-a-is-not-parsed"),
            matched_identity: http_identity("example.com", Some("/api"), true),
            entry: route_entry(),
            cache_policy: cache_policy(12_345),
        }
    );
}

#[test]
fn route_miss_response_preserves_negative_ttl_and_request_identity() {
    let output = proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
            pb::ProxyRouteMissResponse {
                request_id: "req-miss".to_owned(),
                request_identity: Some(pb_sni_identity("db.example.com", pb::RouteHostKind::Exact)),
                negative_cache_policy: Some(pb_cache_policy(250)),
            },
        )),
    })
    .expect("valid miss");

    assert_eq!(
        output,
        SubscribeControlPlaneOutput::RouteMiss {
            request_id: request_id("req-miss"),
            request_identity: sni_identity("db.example.com", false),
            negative_cache_policy: cache_policy(250),
        }
    );
}

#[test]
fn route_updated_response_converts_existing_variant() {
    let output = proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteUpdated(
            pb::ProxyRouteUpdatedResponse {
                subscription_id: "sub-update".to_owned(),
                matched_identity: Some(pb_http_identity(
                    "app.example.com",
                    None,
                    pb::RouteHostKind::Exact,
                )),
                route: Some(pb_route_entry()),
                cache_policy: Some(pb_cache_policy(500)),
            },
        )),
    })
    .expect("valid update");

    assert_eq!(
        output,
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-update"),
            matched_identity: http_identity("app.example.com", None, false),
            entry: route_entry(),
            cache_policy: cache_policy(500),
        }
    );
}

#[test]
fn route_invalidated_response_converts_all_reasons() {
    for (proto, reason) in [
        (
            pb::ProxyRouteInvalidationReason::RouteRemoved,
            InvalidationReason::RouteRemoved,
        ),
        (
            pb::ProxyRouteInvalidationReason::RouteChanged,
            InvalidationReason::RouteChanged,
        ),
        (
            pb::ProxyRouteInvalidationReason::InstanceChanged,
            InvalidationReason::InstanceChanged,
        ),
        (
            pb::ProxyRouteInvalidationReason::BackendChanged,
            InvalidationReason::BackendChanged,
        ),
        (
            pb::ProxyRouteInvalidationReason::StreamClosed,
            InvalidationReason::StreamClosed,
        ),
    ] {
        let output = proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteInvalidated(
                pb::ProxyRouteInvalidatedResponse {
                    subscription_id: "sub-invalidate".to_owned(),
                    reason: proto as i32,
                },
            )),
        })
        .expect("valid invalidation");

        assert_eq!(
            output,
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-invalidate"),
                reason,
            }
        );
    }
}

#[test]
fn wake_ready_response_preserves_backend_semantics() {
    let output = proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
            pb::ProxyWakeReadyResult {
                backend_address: None,
                instance_id: "instance-a".to_owned(),
                instance_generation: 7,
                backend_uri: "http://10.0.0.7:8080".to_owned(),
                backend_generation: 3,
            },
        )),
    })
    .expect("ready response");

    assert_eq!(
        output,
        WakeInstanceResponse::AlreadyRunning {
            instance_id: instance_id("instance-a"),
            generation: Generation::new(7),
            backend: backend("http://10.0.0.7:8080"),
            backend_generation: Some(BackendGeneration::new(3)),
        }
    );
}

#[test]
fn wake_ready_response_carries_an_observed_backend_address() {
    let output = proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
            pb::ProxyWakeReadyResult {
                backend_address: Some("10.244.1.7:8080".to_owned()),
                instance_id: "instance-a".to_owned(),
                instance_generation: 7,
                backend_uri: "http://10.0.0.7:8080".to_owned(),
                backend_generation: 3,
            },
        )),
    })
    .expect("ready response");

    let WakeInstanceResponse::AlreadyRunning { backend, .. } = output else {
        panic!("expected an already-running response");
    };
    assert_eq!(backend.uri(), "http://10.0.0.7:8080");
    assert_eq!(
        backend.address(),
        Some("10.244.1.7:8080".parse().expect("valid address"))
    );
}

#[test]
fn wake_ready_response_rejects_a_backend_address_that_cannot_be_dialed() {
    let error = proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
            pb::ProxyWakeReadyResult {
                backend_address: Some("not-an-address".to_owned()),
                instance_id: "instance-a".to_owned(),
                instance_generation: 7,
                backend_uri: "http://10.0.0.7:8080".to_owned(),
                backend_generation: 3,
            },
        )),
    })
    .expect_err("an undialable address is rejected");

    assert!(
        matches!(
            error,
            ProxyProtocolAdapterError::InvalidField { field, .. } if field == "backend.address"
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn route_entries_carry_an_observed_backend_address() {
    let entry = super::route_entry_from_proto(pb::ProxyRouteEntry {
        backend_address: Some("[fd00::7]:8080".to_owned()),
        ..pb_route_entry()
    })
    .expect("route entry");

    assert_eq!(
        entry.backend.expect("backend is present").address(),
        Some("[fd00::7]:8080".parse().expect("valid address"))
    );
}

#[test]
fn wake_still_waking_unavailable_and_generation_conflict_convert() {
    let still_waking = proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::StillWaking(
            pb::ProxyWakeStillWakingResult {
                instance_id: "instance-a".to_owned(),
                instance_generation: 8,
            },
        )),
    })
    .expect("still waking");
    assert_eq!(
        still_waking,
        WakeInstanceResponse::StillWaking {
            instance_id: instance_id("instance-a"),
            generation: Generation::new(8),
        }
    );

    let unavailable = proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Unavailable(
            pb::ProxyWakeUnavailableResult {
                instance_id: "instance-a".to_owned(),
                instance_generation: 9,
                reason: pb::ProxyWakeUnavailableReason::Deleting as i32,
            },
        )),
    })
    .expect("unavailable");
    assert_eq!(
        unavailable,
        WakeInstanceResponse::Unavailable {
            instance_id: instance_id("instance-a"),
            generation: Generation::new(9),
            reason: "deleting".to_owned(),
        }
    );

    let conflict = proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
        outcome: Some(
            pb::proxy_wake_instance_response::Outcome::GenerationConflict(
                pb::ProxyWakeGenerationConflictResult {
                    instance_id: "instance-a".to_owned(),
                    expected_generation: 10,
                    actual_generation: 11,
                },
            ),
        ),
    })
    .expect("conflict");
    assert_eq!(
        conflict,
        WakeInstanceResponse::GenerationConflict {
            instance_id: instance_id("instance-a"),
            expected_generation: Generation::new(10),
            actual_generation: Generation::new(11),
        }
    );
}

#[test]
fn malformed_subscribe_responses_return_typed_errors() {
    assert_eq!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse { output: None })
            .expect_err("missing output"),
        ProxyProtocolAdapterError::MissingField { field: "output" }
    );

    assert_eq!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
                pb::ProxyRouteResolvedResponse {
                    request_id: "req".to_owned(),
                    subscription_id: "sub".to_owned(),
                    matched_identity: None,
                    route: Some(pb_route_entry()),
                    cache_policy: Some(pb_cache_policy(100)),
                },
            )),
        })
        .expect_err("missing identity"),
        ProxyProtocolAdapterError::MissingField {
            field: "matched_identity"
        }
    );

    assert_eq!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
                pb::ProxyRouteResolvedResponse {
                    request_id: "req".to_owned(),
                    subscription_id: "sub".to_owned(),
                    matched_identity: Some(pb_http_identity(
                        "app.example.com",
                        Some("/"),
                        pb::RouteHostKind::Exact,
                    )),
                    route: None,
                    cache_policy: Some(pb_cache_policy(100)),
                },
            )),
        })
        .expect_err("missing route"),
        ProxyProtocolAdapterError::MissingField { field: "route" }
    );

    assert_eq!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
                pb::ProxyRouteMissResponse {
                    request_id: "req".to_owned(),
                    request_identity: Some(pb_sni_identity(
                        "db.example.com",
                        pb::RouteHostKind::Exact,
                    )),
                    negative_cache_policy: None,
                },
            )),
        })
        .expect_err("missing cache"),
        ProxyProtocolAdapterError::MissingField {
            field: "negative_cache_policy"
        }
    );
}

#[test]
fn malformed_fields_return_typed_errors() {
    let mut route = pb_route_entry();
    route.instance_state = pb::InstanceState::Unspecified as i32;
    assert_eq!(
        proxy_subscribe_response_from_proto(route_resolved_with_route(route))
            .expect_err("unspecified state"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "instance_state",
            value: pb::InstanceState::Unspecified as i32,
        }
    );

    let mut route = pb_route_entry();
    route.instance_state = 99;
    assert_eq!(
        proxy_subscribe_response_from_proto(route_resolved_with_route(route))
            .expect_err("unknown state"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "instance_state",
            value: 99,
        }
    );

    let mut route = pb_route_entry();
    route.route_binding_id = " ".to_owned();
    assert!(matches!(
        proxy_subscribe_response_from_proto(route_resolved_with_route(route))
            .expect_err("invalid route binding ID"),
        ProxyProtocolAdapterError::InvalidField {
            field: "RouteBindingId",
            ..
        }
    ));

    let response = route_resolved_with_identity(pb_http_identity(
        "app.example.com",
        Some("relative"),
        pb::RouteHostKind::Exact,
    ));
    assert!(matches!(
        proxy_subscribe_response_from_proto(response).expect_err("invalid path"),
        ProxyProtocolAdapterError::InvalidField {
            field: "http.path_prefix",
            ..
        }
    ));

    let response = route_resolved_with_identity(pb_http_identity(
        "localhost",
        Some("/"),
        pb::RouteHostKind::Exact,
    ));
    assert!(matches!(
        proxy_subscribe_response_from_proto(response).expect_err("invalid host"),
        ProxyProtocolAdapterError::InvalidField {
            field: "route_host.host",
            ..
        }
    ));
}

#[test]
fn malformed_ids_and_unavailable_reasons_return_typed_errors() {
    assert!(matches!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
                pb::ProxyRouteResolvedResponse {
                    request_id: " ".to_owned(),
                    subscription_id: "sub".to_owned(),
                    matched_identity: Some(pb_http_identity(
                        "app.example.com",
                        Some("/"),
                        pb::RouteHostKind::Exact,
                    )),
                    route: Some(pb_route_entry()),
                    cache_policy: Some(pb_cache_policy(100)),
                },
            )),
        })
        .expect_err("empty request ID"),
        ProxyProtocolAdapterError::InvalidField {
            field: "request_id",
            ..
        }
    ));

    assert!(matches!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteInvalidated(
                pb::ProxyRouteInvalidatedResponse {
                    subscription_id: "".to_owned(),
                    reason: pb::ProxyRouteInvalidationReason::RouteChanged as i32,
                },
            )),
        })
        .expect_err("empty subscription ID"),
        ProxyProtocolAdapterError::InvalidField {
            field: "subscription_id",
            ..
        }
    ));

    assert_eq!(
        proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Unavailable(
                pb::ProxyWakeUnavailableResult {
                    instance_id: "instance-a".to_owned(),
                    instance_generation: 1,
                    reason: pb::ProxyWakeUnavailableReason::Unspecified as i32,
                },
            )),
        })
        .expect_err("unspecified wake reason"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "wake_unavailable.reason",
            value: pb::ProxyWakeUnavailableReason::Unspecified as i32,
        }
    );
}

#[test]
fn invalid_enums_and_cache_ttl_return_typed_errors() {
    let response = route_resolved_with_identity(pb::RouteIdentity {
        kind: Some(pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
            host: Some(pb::RouteHost {
                kind: 99,
                host: "db.example.com".to_owned(),
            }),
        })),
    });
    assert_eq!(
        proxy_subscribe_response_from_proto(response).expect_err("unknown host kind"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "route_host.kind",
            value: 99,
        }
    );

    assert_eq!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteInvalidated(
                pb::ProxyRouteInvalidatedResponse {
                    subscription_id: "sub".to_owned(),
                    reason: pb::ProxyRouteInvalidationReason::Unspecified as i32,
                },
            )),
        })
        .expect_err("unspecified invalidation reason"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "route_invalidation.reason",
            value: pb::ProxyRouteInvalidationReason::Unspecified as i32,
        }
    );

    assert_eq!(
        proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
                pb::ProxyRouteMissResponse {
                    request_id: "req".to_owned(),
                    request_identity: Some(pb_sni_identity(
                        "db.example.com",
                        pb::RouteHostKind::Exact,
                    )),
                    negative_cache_policy: Some(pb_cache_policy(u64::MAX)),
                },
            )),
        })
        .expect_err("unrepresentable cache TTL"),
        ProxyProtocolAdapterError::InvalidCacheTtl {
            field: "negative_cache_policy",
            ttl_millis: u64::MAX,
        }
    );
}

#[test]
fn missing_wake_outcome_and_bad_ready_backend_return_typed_errors() {
    assert_eq!(
        proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse { outcome: None })
            .expect_err("missing outcome"),
        ProxyProtocolAdapterError::MissingField { field: "outcome" }
    );

    assert!(matches!(
        proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
                pb::ProxyWakeReadyResult {
                    backend_address: None,
                    instance_id: "instance-a".to_owned(),
                    instance_generation: 7,
                    backend_uri: " ".to_owned(),
                    backend_generation: 3,
                },
            )),
        })
        .expect_err("empty backend"),
        ProxyProtocolAdapterError::InvalidField {
            field: "backend.uri",
            ..
        }
    ));
}

#[test]
fn subscription_id_is_opaque_and_not_inferred_from_route_identity() {
    let output = proxy_subscribe_response_from_proto(pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
            pb::ProxyRouteResolvedResponse {
                request_id: "req".to_owned(),
                subscription_id: "not-derived-from-route-or-host".to_owned(),
                matched_identity: Some(pb_http_identity(
                    "tenant.example.com",
                    Some("/"),
                    pb::RouteHostKind::Exact,
                )),
                route: Some(pb::ProxyRouteEntry {
                    backend_address: None,
                    route_binding_id: "different-route-id".to_owned(),
                    instance_id: "different-instance-id".to_owned(),
                    instance_state: pb::InstanceState::Cold as i32,
                    instance_generation: 1,
                    backend_uri: None,
                    backend_generation: None,
                }),
                cache_policy: Some(pb_cache_policy(100)),
            },
        )),
    })
    .expect("valid opaque subscription");

    match output {
        SubscribeControlPlaneOutput::RouteResolved {
            subscription_id,
            entry,
            matched_identity,
            ..
        } => {
            assert_eq!(subscription_id.as_str(), "not-derived-from-route-or-host");
            assert_eq!(entry.route_binding_id.as_str(), "different-route-id");
            match matched_identity {
                RouteIdentity::Http { host, .. } => {
                    assert_eq!(host.kind(), RouteHostKind::Exact);
                    assert_eq!(host.as_str(), "tenant.example.com");
                }
                RouteIdentity::Sni { .. } => panic!("expected HTTP identity"),
            }
        }
        _ => panic!("expected resolved output"),
    }
}

fn route_resolved_with_route(route: pb::ProxyRouteEntry) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
            pb::ProxyRouteResolvedResponse {
                request_id: "req".to_owned(),
                subscription_id: "sub".to_owned(),
                matched_identity: Some(pb_http_identity(
                    "app.example.com",
                    Some("/"),
                    pb::RouteHostKind::Exact,
                )),
                route: Some(route),
                cache_policy: Some(pb_cache_policy(100)),
            },
        )),
    }
}

fn route_resolved_with_identity(identity: pb::RouteIdentity) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
            pb::ProxyRouteResolvedResponse {
                request_id: "req".to_owned(),
                subscription_id: "sub".to_owned(),
                matched_identity: Some(identity),
                route: Some(pb_route_entry()),
                cache_policy: Some(pb_cache_policy(100)),
            },
        )),
    }
}
