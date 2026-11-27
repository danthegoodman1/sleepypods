use std::time::Duration;

use sleepypods_api::{
    pb, BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity, RouteKind, RouteRule,
};

use super::{
    proxy_wake_response_from_proto, route_stream_reply_from_proto, route_stream_request_to_proto,
    wake_instance_request_to_proto, ProxyProtocolAdapterError,
};
use crate::{
    RouteStreamReply, RouteStreamRequest, SubscribeErrorCode, WakeInstanceRequest,
    WakeInstanceResponse,
};

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

fn pb_rule(identity: pb::RouteIdentity, route: pb::ProxyRouteEntry) -> pb::ProxyRouteRule {
    pb::ProxyRouteRule {
        identity: Some(identity),
        route: Some(route),
    }
}

fn pb_rules(rules: Vec<pb::ProxyRouteRule>, ttl_millis: u64) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::Rules(
            pb::ProxyHostRulesResponse {
                subscription_id: 41,
                rules,
                cache_policy: Some(pb_cache_policy(ttl_millis)),
            },
        )),
    }
}

fn rules_with_route(route: pb::ProxyRouteEntry) -> pb::ProxySubscribeResponse {
    pb_rules(
        vec![pb_rule(
            pb_http_identity("app.example.com", Some("/"), pb::RouteHostKind::Exact),
            route,
        )],
        100,
    )
}

fn rules_with_identity(identity: pb::RouteIdentity) -> pb::ProxySubscribeResponse {
    pb_rules(vec![pb_rule(identity, pb_route_entry())], 100)
}

#[test]
fn subscribe_and_unsubscribe_requests_convert_to_proto() {
    for (kind, proto_kind) in [
        (RouteKind::Http, pb::RouteKind::Http),
        (RouteKind::Sni, pb::RouteKind::Sni),
    ] {
        let request = route_stream_request_to_proto(RouteStreamRequest::Subscribe {
            id: 7,
            kind,
            host: RouteHost::exact("App.Example.COM").expect("exact host"),
        });
        assert_eq!(
            request.input,
            Some(pb::proxy_subscribe_request::Input::Subscribe(
                pb::ProxySubscribeHostRequest {
                    subscription_id: 7,
                    kind: proto_kind as i32,
                    host: "app.example.com".to_owned(),
                }
            ))
        );
    }

    assert_eq!(
        route_stream_request_to_proto(RouteStreamRequest::Unsubscribe { id: 7 }).input,
        Some(pb::proxy_subscribe_request::Input::Unsubscribe(
            pb::ProxyUnsubscribeRequest { subscription_id: 7 }
        ))
    );
}

#[test]
fn a_rule_set_reply_preserves_rules_order_and_cache_policy() {
    let reply = route_stream_reply_from_proto(pb_rules(
        vec![
            pb_rule(
                pb_http_identity("app.example.com", Some("/api"), pb::RouteHostKind::Exact),
                pb_route_entry(),
            ),
            pb_rule(
                pb_http_identity("example.com", None, pb::RouteHostKind::WildcardSuffix),
                pb::ProxyRouteEntry {
                    route_binding_id: "route-wild".to_owned(),
                    instance_id: "instance-cold".to_owned(),
                    instance_state: pb::InstanceState::Cold as i32,
                    instance_generation: 1,
                    backend_uri: None,
                    backend_generation: None,
                },
            ),
        ],
        90_000,
    ))
    .expect("rule set");

    assert_eq!(
        reply,
        RouteStreamReply::Rules {
            id: 41,
            rules: vec![
                RouteRule {
                    identity: http_identity("app.example.com", Some("/api"), false),
                    entry: route_entry(),
                },
                RouteRule {
                    identity: http_identity("example.com", None, true),
                    entry: RouteEntry {
                        route_binding_id: route_binding_id("route-wild"),
                        instance_id: instance_id("instance-cold"),
                        instance_state: InstanceState::Cold,
                        instance_generation: Generation::new(1),
                        backend: None,
                        backend_generation: None,
                    },
                },
            ],
            cache_policy: cache_policy(90_000),
        }
    );
}

#[test]
fn an_empty_rule_set_and_an_sni_rule_convert() {
    assert_eq!(
        route_stream_reply_from_proto(pb_rules(Vec::new(), 1_000)).expect("empty rule set"),
        RouteStreamReply::Rules {
            id: 41,
            rules: Vec::new(),
            cache_policy: cache_policy(1_000),
        }
    );

    let reply = route_stream_reply_from_proto(rules_with_identity(pb_sni_identity(
        "db.example.com",
        pb::RouteHostKind::Exact,
    )))
    .expect("SNI rule set");
    match reply {
        RouteStreamReply::Rules { rules, .. } => {
            assert_eq!(rules[0].identity, sni_identity("db.example.com", false));
        }
        other => panic!("expected rules, got {other:?}"),
    }
}

#[test]
fn a_subscription_error_converts_every_code() {
    for (proto, code) in [
        (
            pb::ProxySubscribeErrorCode::InvalidArgument,
            SubscribeErrorCode::InvalidArgument,
        ),
        (
            pb::ProxySubscribeErrorCode::ResourceExhausted,
            SubscribeErrorCode::ResourceExhausted,
        ),
        (
            pb::ProxySubscribeErrorCode::Unavailable,
            SubscribeErrorCode::Unavailable,
        ),
    ] {
        let reply = route_stream_reply_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::Error(
                pb::ProxySubscribeErrorResponse {
                    subscription_id: 9,
                    code: proto as i32,
                    message: "why".to_owned(),
                },
            )),
        })
        .expect("subscription error");
        assert_eq!(
            reply,
            RouteStreamReply::Error {
                id: 9,
                code,
                message: "why".to_owned(),
            }
        );
    }

    for value in [pb::ProxySubscribeErrorCode::Unspecified as i32, 99] {
        assert_eq!(
            route_stream_reply_from_proto(pb::ProxySubscribeResponse {
                output: Some(pb::proxy_subscribe_response::Output::Error(
                    pb::ProxySubscribeErrorResponse {
                        subscription_id: 9,
                        code: value,
                        message: String::new(),
                    },
                )),
            })
            .expect_err("unknown code"),
            ProxyProtocolAdapterError::InvalidEnum {
                field: "subscribe_error.code",
                value,
            }
        );
    }
}

#[test]
fn malformed_replies_return_typed_errors() {
    assert_eq!(
        route_stream_reply_from_proto(pb::ProxySubscribeResponse { output: None })
            .expect_err("missing output"),
        ProxyProtocolAdapterError::MissingField { field: "output" }
    );
    assert_eq!(
        route_stream_reply_from_proto(pb_rules(
            vec![pb::ProxyRouteRule {
                identity: None,
                route: Some(pb_route_entry()),
            }],
            100,
        ))
        .expect_err("missing identity"),
        ProxyProtocolAdapterError::MissingField {
            field: "rule.identity"
        }
    );
    assert_eq!(
        route_stream_reply_from_proto(pb_rules(
            vec![pb::ProxyRouteRule {
                identity: Some(pb_http_identity(
                    "app.example.com",
                    None,
                    pb::RouteHostKind::Exact
                )),
                route: None,
            }],
            100,
        ))
        .expect_err("missing route"),
        ProxyProtocolAdapterError::MissingField {
            field: "rule.route"
        }
    );
    assert_eq!(
        route_stream_reply_from_proto(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::Rules(
                pb::ProxyHostRulesResponse {
                    subscription_id: 1,
                    rules: Vec::new(),
                    cache_policy: None,
                },
            )),
        })
        .expect_err("missing cache policy"),
        ProxyProtocolAdapterError::MissingField {
            field: "cache_policy"
        }
    );
    assert_eq!(
        route_stream_reply_from_proto(pb_rules(Vec::new(), u64::MAX))
            .expect_err("unrepresentable cache TTL"),
        ProxyProtocolAdapterError::InvalidCacheTtl {
            field: "cache_policy",
            ttl_millis: u64::MAX,
        }
    );
}

#[test]
fn malformed_rule_fields_return_typed_errors() {
    let mut route = pb_route_entry();
    route.instance_state = pb::InstanceState::Unspecified as i32;
    assert_eq!(
        route_stream_reply_from_proto(rules_with_route(route)).expect_err("unspecified state"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "instance_state",
            value: pb::InstanceState::Unspecified as i32,
        }
    );

    let mut route = pb_route_entry();
    route.instance_state = 99;
    assert_eq!(
        route_stream_reply_from_proto(rules_with_route(route)).expect_err("unknown state"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "instance_state",
            value: 99,
        }
    );

    let mut route = pb_route_entry();
    route.route_binding_id = " ".to_owned();
    assert!(matches!(
        route_stream_reply_from_proto(rules_with_route(route))
            .expect_err("invalid route binding ID"),
        ProxyProtocolAdapterError::InvalidField {
            field: "RouteBindingId",
            ..
        }
    ));

    assert!(matches!(
        route_stream_reply_from_proto(rules_with_identity(pb_http_identity(
            "app.example.com",
            Some("relative"),
            pb::RouteHostKind::Exact,
        )))
        .expect_err("invalid path"),
        ProxyProtocolAdapterError::InvalidField {
            field: "http.path_prefix",
            ..
        }
    ));

    assert!(matches!(
        route_stream_reply_from_proto(rules_with_identity(pb_http_identity(
            "localhost",
            Some("/"),
            pb::RouteHostKind::Exact,
        )))
        .expect_err("invalid host"),
        ProxyProtocolAdapterError::InvalidField {
            field: "route_host.host",
            ..
        }
    ));

    assert_eq!(
        route_stream_reply_from_proto(rules_with_identity(pb::RouteIdentity {
            kind: Some(pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
                host: Some(pb::RouteHost {
                    kind: 99,
                    host: "db.example.com".to_owned(),
                }),
            })),
        }))
        .expect_err("unknown host kind"),
        ProxyProtocolAdapterError::InvalidEnum {
            field: "route_host.kind",
            value: 99,
        }
    );
}

#[test]
fn an_unspecified_wake_unavailable_reason_is_an_error() {
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
fn wake_ready_response_preserves_backend_semantics() {
    let output = proxy_wake_response_from_proto(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
            pb::ProxyWakeReadyResult {
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
