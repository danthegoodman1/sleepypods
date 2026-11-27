#[macro_use]
#[path = "support/unexpected_store.rs"]
mod unexpected_store;
#[path = "proxy_api_transport/route_edge.rs"]
mod route_edge;
mod support;
use support::TestStore as FakeWakeStore;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use control_plane::api::{
    admission::ApiLimits,
    pb::{
        proxy_control_plane_client::ProxyControlPlaneClient,
        proxy_control_plane_server::ProxyControlPlane, proxy_subscribe_request,
        proxy_subscribe_response, proxy_wake_instance_response, InstanceState, ProxyCachePolicy,
        ProxyHostRulesResponse, ProxyRouteEntry, ProxySubscribeErrorCode,
        ProxySubscribeErrorResponse, ProxySubscribeHostRequest, ProxySubscribeRequest,
        ProxySubscribeResponse, ProxyWakeInstanceRequest, ProxyWakeInstanceResponse,
        ProxyWakeUnavailableReason, RouteHostKind, RouteKind as ProtoRouteKind,
    },
    proxy_grpc_service_with_store, proxy_grpc_service_with_store_and_route_events,
    RouteSubscriptionBroker, StoreBackedProxyApi, StoreBackedProxyGrpcService, PROXY_SERVICE_NAME,
};
use control_plane::projection::{LiveObjectMetadata, ProjectionObjectInspection};
use control_plane::{
    rendered_object_ref, AuthConfig, BackendEndpoint, BackendGeneration, CallerRole,
    ControlPlaneAuth, ControlPlaneStore, Generation, InstanceId, InstanceRecord,
    InstanceState as DomainInstanceState, KubernetesClientError, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient,
    MaterializationId, MaterializationRecord, MaterializationState, MaterializationTarget,
    PathPrefix, RenderedObjectRef, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
    StaticBearerTokens,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{
    header, HeaderMap, HeaderValue, Request, Response as HttpResponse, Version,
};
use tonic::codegen::tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::server::NamedService;
use tonic::Code;
use tower::{Service, ServiceExt};

fn auth_config() -> AuthConfig {
    AuthConfig::static_bearer_tokens(
        StaticBearerTokens::new("operator-token", "proxy-token", "sidecar-token")
            .expect("auth tokens are valid"),
    )
}

fn control_plane_auth() -> ControlPlaneAuth {
    ControlPlaneAuth::from_config(auth_config(), Default::default())
}

fn authenticated_proxy_service(
    store: Arc<dyn ControlPlaneStore>,
    client: FakeKubernetesClient,
) -> tonic::service::interceptor::InterceptedService<
    StoreBackedProxyGrpcService<FakeKubernetesClient>,
    control_plane::ControlPlaneAuthInterceptor,
> {
    tonic::service::interceptor::InterceptedService::new(
        proxy_grpc_service_with_store(store, KubernetesMaterializer::new(client), proxy_target()),
        control_plane_auth().interceptor(PROXY_SERVICE_NAME, CallerRole::Proxy),
    )
}

#[test]
fn generated_api_contains_proxy_wake_shape_without_operator_surface_change() {
    let request = ProxyWakeInstanceRequest {
        instance_id: "instance-1".to_owned(),
        expected_generation: 7,
        backend_generation: Some(11),
    };
    let response = ProxyWakeInstanceResponse {
        outcome: Some(proxy_wake_instance_response::Outcome::StillWaking(
            control_plane::api::pb::ProxyWakeStillWakingResult {
                instance_id: request.instance_id.clone(),
                instance_generation: request.expected_generation,
            },
        )),
    };

    assert_eq!(request.instance_id, "instance-1");
    assert_eq!(request.backend_generation, Some(11));
    assert!(matches!(
        response.outcome,
        Some(proxy_wake_instance_response::Outcome::StillWaking(_))
    ));
    assert_eq!(
        <StoreBackedProxyGrpcService<FakeKubernetesClient> as NamedService>::NAME,
        PROXY_SERVICE_NAME
    );

    assert!(matches!(
        subscribe_http(1, "app.example.com").input,
        Some(proxy_subscribe_request::Input::Subscribe(_))
    ));
}

#[tokio::test]
async fn proxy_wake_cold_instance_accepts_before_driver_publishes_ready_backend() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-cold",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let service = proxy_api(store.clone(), client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-cold".to_owned(),
            expected_generation: 1,
            backend_generation: Some(44),
        }))
        .await
        .expect("cold wake succeeds")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_generation, 2);
    assert_eq!(client.applied_objects_len(), 0);
    reconcile_proxy_work(store, client.clone()).await;
    let ready = expect_ready(
        service
            .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
                instance_id: "instance-cold".to_owned(),
                expected_generation: 3,
                backend_generation: None,
            }))
            .await
            .expect("completed wake is queryable")
            .into_inner(),
    );
    assert_eq!(ready.instance_id, "instance-cold");
    assert_eq!(ready.instance_generation, 3);
    assert_eq!(
        ready.backend_uri,
        "http://svc-acme-69856ec0faae6daf.apps.svc.cluster.local:80"
    );
    assert_eq!(ready.backend_generation, 44);
    assert_eq!(client.applied_objects_len(), 2);
}

#[tokio::test]
async fn proxy_wake_unowned_live_ref_blocks_apply() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-unowned",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    client.set_unowned_live_object(object_ref(
        "v1",
        "Service",
        "apps",
        "svc-acme-51c842a33d8cf7e8",
    ));
    let service = proxy_api(store.clone(), client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-unowned".to_owned(),
            expected_generation: 1,
            backend_generation: None,
        }))
        .await
        .expect("wake intent is accepted before Kubernetes inspection")
        .into_inner();

    expect_accepted(response);
    reconcile_proxy_work(store, client.clone()).await;
    assert_eq!(client.applied_objects_len(), 0);
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_proxy_wake_instance() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-transport",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();

    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(client.clone()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_wake_request(
        ProxyWakeInstanceRequest {
            instance_id: "instance-transport".to_owned(),
            expected_generation: 1,
            backend_generation: Some(55),
        },
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("native gRPC request should route through proxy wake service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("successful gRPC status is returned");

    assert_eq!(status, "0");

    let accepted = expect_accepted(decode_grpc_proxy_wake_response(
        collected.to_bytes().as_ref(),
    ));
    assert_eq!(accepted.instance_id, "instance-transport");
    assert_eq!(accepted.instance_generation, 2);
    assert_eq!(client.applied_objects_len(), 0);
}

#[tokio::test]
async fn native_grpc_proxy_auth_rejects_wake_before_materialization_logic() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-auth-wake",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let store_for_service: Arc<dyn ControlPlaneStore> = store;

    let missing_status = collect_grpc_status(
        authenticated_proxy_service(Arc::clone(&store_for_service), client.clone())
            .oneshot(grpc_proxy_wake_request(
                ProxyWakeInstanceRequest {
                    instance_id: "instance-auth-wake".to_owned(),
                    expected_generation: 1,
                    backend_generation: Some(55),
                },
                "application/grpc",
                Version::HTTP_2,
            ))
            .await
            .expect("missing auth returns gRPC response"),
    )
    .await;
    assert_eq!(missing_status, "16");
    assert_eq!(client.applied_objects_len(), 0);

    let wrong_role_status = collect_grpc_status(
        authenticated_proxy_service(store_for_service, client.clone())
            .oneshot(with_authorization(
                grpc_proxy_wake_request(
                    ProxyWakeInstanceRequest {
                        instance_id: "instance-auth-wake".to_owned(),
                        expected_generation: 1,
                        backend_generation: Some(55),
                    },
                    "application/grpc",
                    Version::HTTP_2,
                ),
                "Bearer operator-token",
            ))
            .await
            .expect("wrong role returns gRPC response"),
    )
    .await;
    assert_eq!(wrong_role_status, "7");
    assert_eq!(client.applied_objects_len(), 0);
}

#[tokio::test]
async fn native_grpc_proxy_auth_accepts_valid_wake_credentials() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-auth-valid",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let store_for_service: Arc<dyn ControlPlaneStore> = store;

    let response = authenticated_proxy_service(store_for_service, client.clone())
        .oneshot(with_authorization(
            grpc_proxy_wake_request(
                ProxyWakeInstanceRequest {
                    instance_id: "instance-auth-valid".to_owned(),
                    expected_generation: 1,
                    backend_generation: Some(56),
                },
                "application/grpc",
                Version::HTTP_2,
            ),
            "Bearer proxy-token",
        ))
        .await
        .expect("valid proxy credentials dispatch to wake handler");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    assert_eq!(grpc_status(&headers, trailers.as_ref()), "0");
    let accepted = expect_accepted(decode_grpc_proxy_wake_response(
        collected.to_bytes().as_ref(),
    ));
    assert_eq!(accepted.instance_id, "instance-auth-valid");
    assert_eq!(client.applied_objects_len(), 0);
}

#[tokio::test(start_paused = true)]
async fn native_grpc_request_dispatches_to_store_backed_proxy_subscribe() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        wildcard_http("example.com", Some("/")),
        domain_route_entry(
            "route-transport",
            "instance-transport",
            DomainInstanceState::Running,
        ),
    );
    let mut proxy = proxy_client_with_route_events(store, RouteSubscriptionBroker::new());
    let mut stream = open_stream(&mut proxy).await;

    stream.send(subscribe_http(7, "app.example.com")).await;

    let rules = stream.rules().await;
    assert_eq!(rules.subscription_id, 7);
    assert_eq!(
        rule_summary(&rules),
        vec![(
            RouteHostKind::WildcardSuffix as i32,
            "example.com".to_owned(),
            Some("/".to_owned()),
            "route-transport".to_owned()
        )]
    );
}

#[tokio::test(start_paused = true)]
async fn native_grpc_proxy_auth_rejects_subscribe_before_route_resolution() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("auth.example.com", Some("/")),
        domain_route_entry("route-auth", "instance-auth", DomainInstanceState::Running),
    );
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();
    let request = subscribe_http(1, "auth.example.com");

    let missing_status = collect_grpc_status(
        authenticated_proxy_service(
            Arc::clone(&store_for_service),
            FakeKubernetesClient::default(),
        )
        .oneshot(grpc_proxy_subscribe_request(
            vec![request.clone()],
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("missing auth returns gRPC response"),
    )
    .await;
    assert_eq!(missing_status, "16");

    let wrong_role_status = collect_grpc_status(
        authenticated_proxy_service(
            Arc::clone(&store_for_service),
            FakeKubernetesClient::default(),
        )
        .oneshot(with_authorization(
            grpc_proxy_subscribe_request(vec![request], "application/grpc", Version::HTTP_2),
            "Bearer sidecar-token",
        ))
        .await
        .expect("wrong role returns gRPC response"),
    )
    .await;
    assert_eq!(wrong_role_status, "7");
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(store.resolve_route_requests(), 0);

    let mut proxy = ProxyControlPlaneClient::with_interceptor(
        InProcessService::new(authenticated_proxy_service(
            store_for_service,
            FakeKubernetesClient::default(),
        )),
        |mut request: tonic::Request<()>| {
            request
                .metadata_mut()
                .insert("authorization", "Bearer proxy-token".parse().unwrap());
            Ok(request)
        },
    );
    let (requests, incoming) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(incoming))
        .await
        .expect("valid proxy credentials dispatch subscribe")
        .into_inner();
    requests
        .send(subscribe_http(1, "auth.example.com"))
        .await
        .unwrap();
    let rules = expect_rules(next_subscribe_response(&mut responses).await);
    assert_eq!(rules.rules.len(), 1);
    assert_eq!(store.resolve_route_requests(), 1);
}

#[tokio::test(start_paused = true)]
async fn proxy_subscribe_returns_the_complete_rule_set_for_the_host_most_specific_first() {
    let store = Arc::new(FakeWakeStore::default());
    for (binding, identity) in [
        (
            "route-wildcard",
            wildcard_http("example.com", Some("/api/v1")),
        ),
        ("route-root", exact_http("service.example.com", None)),
        ("route-api", exact_http("service.example.com", Some("/api"))),
        (
            "route-deep-wildcard",
            wildcard_http("service.example.com", None),
        ),
        ("route-other-host", exact_http("other.example.com", None)),
        ("route-sni", exact_sni("service.example.com")),
    ] {
        store.seed_route_resolved(
            identity,
            domain_route_entry(binding, "instance-resolved", DomainInstanceState::Cold),
        );
    }
    store.seed_ready_materialization(ready_materialization("instance-resolved", 7));
    let mut proxy = proxy_client_with_route_events(store.clone(), RouteSubscriptionBroker::new());
    let mut stream = open_stream(&mut proxy).await;

    stream.send(subscribe_http(3, "service.example.com")).await;

    let rules = stream.rules().await;
    assert_eq!(rules.subscription_id, 3);
    assert_eq!(
        rule_summary(&rules),
        vec![
            (
                RouteHostKind::Exact as i32,
                "service.example.com".to_owned(),
                Some("/api".to_owned()),
                "route-api".to_owned()
            ),
            (
                RouteHostKind::Exact as i32,
                "service.example.com".to_owned(),
                None,
                "route-root".to_owned()
            ),
            (
                RouteHostKind::WildcardSuffix as i32,
                "example.com".to_owned(),
                Some("/api/v1".to_owned()),
                "route-wildcard".to_owned()
            ),
        ],
        "a wildcard suffix covers names under it, and the host itself only as an exact binding"
    );
    assert_eq!(
        rules.rules[0].route,
        Some(ProxyRouteEntry {
            route_binding_id: "route-api".to_owned(),
            instance_id: "instance-resolved".to_owned(),
            instance_state: InstanceState::Cold as i32,
            instance_generation: 7,
            backend_uri: Some("http://svc-acme.apps.svc.cluster.local:80".to_owned()),
            backend_generation: Some(7),
        })
    );
    assert_eq!(
        rules.cache_policy,
        Some(ProxyCachePolicy { ttl_millis: 90_000 })
    );

    stream.send(subscribe_sni(4, "service.example.com")).await;
    let sni = stream.rules().await;
    assert_eq!(rules_bindings(&sni), vec!["route-sni"]);
    assert_eq!(
        store.resolve_route_requests(),
        2,
        "one store read per answer"
    );
}

#[tokio::test(start_paused = true)]
async fn proxy_subscribe_publishes_the_backend_its_one_store_read_resolved() {
    let store = Arc::new(FakeWakeStore::failing_ready_lookup());
    store.seed_route_resolved(
        exact_http("snapshot.example.com", None),
        domain_route_entry(
            "route-snapshot",
            "instance-snapshot",
            DomainInstanceState::Running,
        ),
    );
    store.seed_ready_materialization(ready_materialization("instance-snapshot", 7));

    let rules = subscribe_once(store, "snapshot.example.com").await;

    let route = rules.rules[0].route.clone().expect("route entry");
    assert_eq!(
        route.backend_uri.as_deref(),
        Some("http://svc-acme.apps.svc.cluster.local:80")
    );
    assert_eq!(route.backend_generation, Some(7));
}

#[tokio::test(start_paused = true)]
async fn proxy_subscribe_publishes_no_backend_without_a_ready_materialization() {
    let cases = [
        ("absent", None),
        ("pending", Some(MaterializationState::Pending)),
        ("failed", Some(MaterializationState::Failed)),
        ("deleting", Some(MaterializationState::Deleting)),
        ("deleted", Some(MaterializationState::Deleted)),
    ];

    for (suffix, materialization_state) in cases {
        let instance_id = format!("instance-{suffix}");
        let host = format!("{suffix}.example.com");
        let store = Arc::new(FakeWakeStore::default());
        store.seed_route_resolved(
            exact_http(&host, None),
            domain_route_entry(
                &format!("route-{suffix}"),
                &instance_id,
                DomainInstanceState::Running,
            ),
        );
        if let Some(state) = materialization_state {
            store.seed_materialization(materialization_with_state(&instance_id, 7, state));
        }

        let rules = subscribe_once(store, &host).await;

        let route = rules.rules[0].route.clone().expect("route entry");
        assert_eq!(route.instance_state, InstanceState::Running as i32);
        assert_eq!(route.backend_uri, None, "{suffix} must not publish backend");
        assert_eq!(
            route.backend_generation, None,
            "{suffix} must not publish backend generation"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_host_with_no_binding_gets_an_empty_rule_set_and_holds_no_subscription() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("bound.example.com", None),
        domain_route_entry(
            "route-bound",
            "instance-bound",
            DomainInstanceState::Running,
        ),
    );
    let broker = RouteSubscriptionBroker::with_limits(ApiLimits {
        subscriptions_per_stream: 1,
        ..Default::default()
    });
    let mut proxy = proxy_client_with_route_events(store, broker);
    let mut stream = open_stream(&mut proxy).await;

    stream.send(subscribe_http(1, "missing.example.com")).await;
    let missing = stream.rules().await;
    assert_eq!(missing.subscription_id, 1);
    assert!(missing.rules.is_empty());
    assert_eq!(
        missing.cache_policy,
        Some(ProxyCachePolicy { ttl_millis: 1_000 })
    );

    // The stream's one subscription is still free.
    stream.send(subscribe_http(2, "bound.example.com")).await;
    assert_eq!(rules_bindings(&stream.rules().await), vec!["route-bound"]);
}

#[tokio::test(start_paused = true)]
async fn unsubscribe_is_idempotent_unacknowledged_and_frees_the_subscription() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("unsubscribe.example.com", None),
        domain_route_entry(
            "route-unsubscribe",
            "instance-unsubscribe",
            DomainInstanceState::Running,
        ),
    );
    let broker = RouteSubscriptionBroker::with_limits(ApiLimits {
        subscriptions_per_stream: 1,
        ..Default::default()
    });
    let mut proxy = proxy_client_with_route_events(store, broker.clone());
    let mut stream = open_stream(&mut proxy).await;

    stream
        .send(subscribe_http(1, "unsubscribe.example.com"))
        .await;
    stream.rules().await;
    for id in [1, 1, 99] {
        stream.send(unsubscribe_request(id)).await;
    }
    stream.quiet().await;

    // A released subscription receives no pushes and leaves room for another.
    broker.notify_instance_changed(InstanceId::new("instance-unsubscribe").unwrap());
    stream.quiet().await;
    stream
        .send(subscribe_http(2, "unsubscribe.example.com"))
        .await;
    assert_eq!(stream.rules().await.subscription_id, 2);
}

#[tokio::test(start_paused = true)]
async fn an_invalid_subscribe_ends_that_subscription_and_the_stream_stays_up() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("valid.example.com", None),
        domain_route_entry(
            "route-valid",
            "instance-valid",
            DomainInstanceState::Running,
        ),
    );
    let mut proxy = proxy_client_with_route_events(store.clone(), RouteSubscriptionBroker::new());
    let mut stream = open_stream(&mut proxy).await;

    let oversized = format!("{}.example.com", "a".repeat(242));
    assert_eq!(oversized.len(), 254);
    for (id, request) in [
        (
            1,
            subscribe_request(1, ProtoRouteKind::Unspecified, "valid.example.com"),
        ),
        (2, subscribe_http(2, &oversized)),
        (3, subscribe_http(3, "Valid.Example.com")),
        (4, subscribe_http(4, "valid.example.com.")),
        (5, subscribe_http(5, "localhost")),
    ] {
        stream.send(request).await;
        let error = stream.error().await;
        assert_eq!(error.subscription_id, id);
        assert_eq!(error.code, ProxySubscribeErrorCode::InvalidArgument as i32);
    }
    assert_eq!(
        store.resolve_route_requests(),
        0,
        "refused before any lookup"
    );

    stream.send(subscribe_http(6, "valid.example.com")).await;
    assert_eq!(stream.rules().await.subscription_id, 6);

    // An ID names one live subscription.
    stream.send(subscribe_http(6, "valid.example.com")).await;
    let duplicate = stream.error().await;
    assert_eq!(
        duplicate.code,
        ProxySubscribeErrorCode::InvalidArgument as i32
    );
    assert_eq!(store.resolve_route_requests(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_failed_store_lookup_ends_that_subscription_and_the_stream_stays_up() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("offline.example.com", None),
        domain_route_entry(
            "route-offline",
            "instance-offline",
            DomainInstanceState::Running,
        ),
    );
    store.seed_route_error_unavailable("store is offline");
    let mut proxy = proxy_client_with_route_events(store.clone(), RouteSubscriptionBroker::new());
    let mut stream = open_stream(&mut proxy).await;

    stream.send(subscribe_http(1, "offline.example.com")).await;
    let error = stream.error().await;
    assert_eq!(error.subscription_id, 1);
    assert_eq!(error.code, ProxySubscribeErrorCode::Unavailable as i32);
    assert_eq!(error.message, "control-plane store is unavailable");

    // The same stream, and the same ID, serve the host once the store is back.
    store.clear_route_error();
    stream.send(subscribe_http(1, "offline.example.com")).await;
    assert_eq!(rules_bindings(&stream.rules().await), vec!["route-offline"]);
}

#[tokio::test(start_paused = true)]
async fn a_lookup_that_times_out_ends_that_subscription_and_the_stream_stays_up() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("slow.example.com", None),
        domain_route_entry("route-slow", "instance-slow", DomainInstanceState::Running),
    );
    let gate = Arc::new(tokio::sync::Notify::new());
    store.set_resolve_gate(gate.clone());
    let broker = RouteSubscriptionBroker::with_limits(ApiLimits {
        lookup_timeout: Duration::from_millis(30),
        ..Default::default()
    });
    let mut proxy = proxy_client_with_route_events(store.clone(), broker);
    let mut stream = open_stream(&mut proxy).await;

    let started = tokio::time::Instant::now();
    stream.send(subscribe_http(1, "slow.example.com")).await;
    let error = stream.error().await;
    assert_eq!(error.code, ProxySubscribeErrorCode::Unavailable as i32);
    assert_eq!(started.elapsed(), Duration::from_millis(30));

    gate.notify_one();
    stream.send(subscribe_http(2, "slow.example.com")).await;
    assert_eq!(rules_bindings(&stream.rules().await), vec!["route-slow"]);
}

#[tokio::test(start_paused = true)]
async fn the_subscription_limit_refuses_one_subscription_and_the_stream_stays_up() {
    let store = Arc::new(FakeWakeStore::default());
    for host in ["first.example.com", "second.example.com"] {
        store.seed_route_resolved(
            exact_http(host, None),
            domain_route_entry(host, "instance-limit", DomainInstanceState::Running),
        );
    }
    let broker = RouteSubscriptionBroker::with_limits(ApiLimits {
        subscriptions_per_stream: 1,
        ..Default::default()
    });
    let mut proxy = proxy_client_with_route_events(store.clone(), broker);
    let mut stream = open_stream(&mut proxy).await;

    stream.send(subscribe_http(1, "first.example.com")).await;
    stream.rules().await;
    stream.send(subscribe_http(2, "second.example.com")).await;
    let error = stream.error().await;
    assert_eq!(error.subscription_id, 2);
    assert_eq!(
        error.code,
        ProxySubscribeErrorCode::ResourceExhausted as i32
    );
    assert_eq!(
        store.resolve_route_requests(),
        1,
        "the limit precedes the lookup"
    );

    stream.send(unsubscribe_request(1)).await;
    stream.send(subscribe_http(2, "second.example.com")).await;
    assert_eq!(
        rules_bindings(&stream.rules().await),
        vec!["second.example.com"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_rule_set_too_large_to_send_ends_that_subscription_and_the_stream_stays_up() {
    let store = Arc::new(FakeWakeStore::default());
    // 250 rules of about 4.5 KiB each exceed the 1 MiB response limit.
    for index in 0..250 {
        store.seed_route_resolved(
            exact_http(
                "large.example.com",
                Some(&format!("/{index}/{}", "p".repeat(4_500))),
            ),
            domain_route_entry(
                &format!("large-{index}"),
                "instance-large",
                DomainInstanceState::Running,
            ),
        );
    }
    store.seed_route_resolved(
        exact_http("small.example.com", None),
        domain_route_entry("small", "instance-small", DomainInstanceState::Running),
    );
    let mut proxy = proxy_client_with_route_events(store, RouteSubscriptionBroker::new());
    let mut stream = open_stream(&mut proxy).await;

    stream.send(subscribe_http(1, "large.example.com")).await;
    let error = stream.error().await;
    assert_eq!(error.subscription_id, 1);
    assert_eq!(error.code, ProxySubscribeErrorCode::InvalidArgument as i32);

    stream.send(subscribe_http(2, "small.example.com")).await;
    assert_eq!(rules_bindings(&stream.rules().await), vec!["small"]);
}

#[tokio::test(start_paused = true)]
async fn a_request_without_input_ends_the_stream_as_invalid() {
    let mut proxy = proxy_client_with_route_events(
        Arc::new(FakeWakeStore::default()),
        RouteSubscriptionBroker::new(),
    );
    let mut stream = open_stream(&mut proxy).await;

    stream.send(ProxySubscribeRequest { input: None }).await;

    assert_eq!(
        stream.end().await.unwrap_err().code(),
        Code::InvalidArgument
    );
}

#[tokio::test]
async fn proxy_wake_already_running_returns_ready_backend_without_apply() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-running",
        DomainInstanceState::Running,
        5,
    ));
    store.seed_ready_materialization(ready_materialization("instance-running", 5));
    let client = FakeKubernetesClient::default();
    let service = proxy_api(store, client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-running".to_owned(),
            expected_generation: 5,
            backend_generation: None,
        }))
        .await
        .expect("already-running wake succeeds")
        .into_inner();

    let ready = expect_ready(response);
    assert_eq!(ready.instance_id, "instance-running");
    assert_eq!(ready.instance_generation, 5);
    assert_eq!(
        ready.backend_uri,
        "http://svc-acme.apps.svc.cluster.local:80"
    );
    assert_eq!(ready.backend_generation, 5);
    assert_eq!(client.applied_objects_len(), 0);
}

#[tokio::test]
async fn accepted_proxy_wake_survives_api_drop_and_completes_in_fresh_driver() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-waking",
        DomainInstanceState::Cold,
        5,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let service = proxy_api(store.clone(), client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-waking".to_owned(),
            expected_generation: 5,
            backend_generation: None,
        }))
        .await
        .expect("already-waking wake succeeds")
        .into_inner();

    assert_eq!(expect_accepted(response).instance_generation, 6);
    assert_eq!(client.applied_objects_len(), 0);
    drop(service);
    reconcile_proxy_work(store.clone(), client.clone()).await;
    let ready = store
        .load_ready_materialization(
            control_plane::materialization::LoadReadyMaterializationRequest::new(
                InstanceId::new("instance-waking").unwrap(),
                Generation::new(7),
                proxy_target(),
            ),
        )
        .await
        .unwrap()
        .expect("fresh driver completed accepted work");
    assert_eq!(ready.projection_generation, Generation::new(7));
    assert_eq!(ready.backend_generation, BackendGeneration::new(6));
    assert_eq!(client.applied_objects_len(), 2);
}

#[tokio::test]
async fn proxy_wake_generation_conflict_is_structured_response() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-conflict",
        DomainInstanceState::Cold,
        9,
    ));
    let service = proxy_api(store, FakeKubernetesClient::default());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-conflict".to_owned(),
            expected_generation: 8,
            backend_generation: None,
        }))
        .await
        .expect("generation conflict is a domain response")
        .into_inner();

    let Some(proxy_wake_instance_response::Outcome::GenerationConflict(conflict)) =
        response.outcome
    else {
        panic!("expected generation-conflict response");
    };
    assert_eq!(conflict.instance_id, "instance-conflict");
    assert_eq!(conflict.expected_generation, 8);
    assert_eq!(conflict.actual_generation, 9);
}

#[tokio::test]
async fn proxy_wake_deleting_instance_returns_unavailable() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-deleting",
        DomainInstanceState::Deleting,
        4,
    ));
    let service = proxy_api(store, FakeKubernetesClient::default());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-deleting".to_owned(),
            expected_generation: 4,
            backend_generation: None,
        }))
        .await
        .expect("deleting instance is a domain response")
        .into_inner();

    let Some(proxy_wake_instance_response::Outcome::Unavailable(unavailable)) = response.outcome
    else {
        panic!("expected unavailable response");
    };
    assert_eq!(unavailable.instance_id, "instance-deleting");
    assert_eq!(unavailable.instance_generation, 4);
    assert_eq!(
        unavailable.reason,
        ProxyWakeUnavailableReason::Deleting as i32
    );
}

#[tokio::test]
async fn proxy_wake_invalid_instance_id_returns_invalid_argument() {
    let service = proxy_api(
        Arc::new(FakeWakeStore::default()),
        FakeKubernetesClient::default(),
    );

    let error = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "".to_owned(),
            expected_generation: 1,
            backend_generation: None,
        }))
        .await
        .expect_err("invalid instance ID is rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn proxy_wake_render_failure_returns_transport_error() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-render-failure",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let service = proxy_api_with_target(
        store,
        FakeKubernetesClient::default(),
        MaterializationTarget::new("cluster-a", "Bad_Namespace").expect("target is non-empty"),
    );

    let error = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-render-failure".to_owned(),
            expected_generation: 1,
            backend_generation: None,
        }))
        .await
        .expect_err("render failure is a transport error");

    assert_eq!(error.code(), Code::Internal);
    assert!(error.message().contains("manifest render failed"));
}

#[tokio::test]
async fn accepted_proxy_wake_remains_recoverable_when_readiness_fails() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-projection-failure",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::failing_readiness();
    let service = proxy_api(store.clone(), client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-projection-failure".to_owned(),
            expected_generation: 1,
            backend_generation: None,
        }))
        .await
        .expect("acceptance does not wait for readiness")
        .into_inner();

    expect_accepted(response);
    assert_eq!(client.applied_objects_len(), 0);
    reconcile_proxy_work(store.clone(), client).await;
    let records = store.materializations();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, MaterializationState::Pending);
    assert!(records[0].backend.is_none());
}

#[tokio::test(start_paused = true)]
async fn subscription_streams_are_bounded_end_in_order_at_their_lifetime_and_free_their_slot() {
    let store = Arc::new(FakeWakeStore::default());
    let broker = RouteSubscriptionBroker::with_limits(ApiLimits {
        subscription_streams: 1,
        subscription_lifetime: Duration::from_millis(80),
        ..Default::default()
    });
    let mut proxy = proxy_client_with_route_events(store, broker);
    let opened = tokio::time::Instant::now();
    let mut stream = open_stream(&mut proxy).await;

    let (_other, incoming) = tokio::sync::mpsc::channel(1);
    let refused = proxy
        .subscribe(ReceiverStream::new(incoming))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::ResourceExhausted);

    stream
        .end()
        .await
        .expect("a stream ends in order at its lifetime");
    assert_eq!(opened.elapsed(), Duration::from_millis(80));
    drop(stream);
    settle().await;
    drop(open_stream(&mut proxy).await);
}

#[tokio::test(start_paused = true)]
async fn lost_route_changes_end_the_stream_with_data_loss() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("lossy.example.com", None),
        domain_route_entry(
            "route-lossy",
            "instance-lossy",
            DomainInstanceState::Running,
        ),
    );
    let broker = RouteSubscriptionBroker::new();
    let mut proxy = proxy_client_with_route_events(store, broker.clone());

    // The dispatcher found a gap in the change history.
    let mut reset = open_stream(&mut proxy).await;
    reset.send(subscribe_http(1, "lossy.example.com")).await;
    reset.rules().await;
    broker.reset();
    assert_eq!(reset.end().await.unwrap_err().code(), Code::DataLoss);

    // The stream fell further behind than the broker keeps.
    let mut lagged = open_stream(&mut proxy).await;
    lagged.send(subscribe_http(1, "lossy.example.com")).await;
    lagged.rules().await;
    for index in 0..5_000 {
        broker.notify_instance_changed(InstanceId::new(format!("unrelated-{index}")).unwrap());
    }
    assert_eq!(lagged.end().await.unwrap_err().code(), Code::DataLoss);
}

#[tokio::test(start_paused = true)]
async fn a_proxy_that_stops_reading_ends_the_stream_with_deadline_exceeded_and_frees_its_slot() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("blocked.example.com", None),
        domain_route_entry(
            "route-blocked",
            "instance-blocked",
            DomainInstanceState::Cold,
        ),
    );
    let broker = RouteSubscriptionBroker::with_limits(ApiLimits {
        subscription_streams: 1,
        response_timeout: Duration::from_millis(30),
        ..Default::default()
    });
    let mut proxy = proxy_client_with_route_events(store.clone(), broker);
    let mut stream = open_stream(&mut proxy).await;
    for id in 0..200 {
        stream.send(subscribe_http(id, "blocked.example.com")).await;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    let lookups = store.resolve_route_requests();
    assert!(
        (1..200).contains(&lookups),
        "a full response queue stops the lookups: {lookups}"
    );

    // The stream holds its slot until the proxy lets go of it.
    let (_requests, incoming) = tokio::sync::mpsc::channel(1);
    assert_eq!(
        proxy
            .subscribe(ReceiverStream::new(incoming))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );

    // What was queued arrives first, then the status that ended the stream.
    let mut delivered = 0;
    let status = loop {
        match stream.responses.message().await {
            Ok(Some(_)) => delivered += 1,
            Ok(None) => panic!("a stream the proxy stopped reading ends with a status"),
            Err(status) => break status,
        }
    };
    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert!(delivered > 0 && delivered < 200, "{delivered}");
    assert_eq!(store.resolve_route_requests(), lookups);

    drop(stream);
    settle().await;
    drop(open_stream(&mut proxy).await);
}

#[tokio::test(start_paused = true)]
async fn an_instance_change_pushes_the_rule_set_and_unrelated_churn_pushes_nothing() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("hot.example.com", None),
        domain_route_entry("hot-route", "hot-instance", DomainInstanceState::Running),
    );
    let broker = RouteSubscriptionBroker::new();
    let mut proxy = proxy_client_with_route_events(store.clone(), broker.clone());
    let mut stream = open_stream(&mut proxy).await;
    stream.send(subscribe_http(1, "hot.example.com")).await;
    let first = stream.rules().await;
    assert_eq!(first.rules[0].route.as_ref().unwrap().backend_uri, None);

    for index in 0..100 {
        broker.notify_instance_changed(InstanceId::new(format!("unrelated-{index}")).unwrap());
    }
    stream.quiet().await;
    assert_eq!(store.resolve_route_requests(), 1);

    // The instance's materialization became ready.
    store.seed_ready_materialization(ready_materialization("hot-instance", 7));
    broker.notify_instance_changed(InstanceId::new("hot-instance").unwrap());
    let pushed = stream.rules().await;
    assert_eq!(pushed.subscription_id, 1);
    assert_eq!(
        pushed.rules[0]
            .route
            .as_ref()
            .unwrap()
            .backend_uri
            .as_deref(),
        Some("http://svc-acme.apps.svc.cluster.local:80")
    );
    assert_eq!(store.resolve_route_requests(), 2);
    stream.quiet().await;
}

#[tokio::test(start_paused = true)]
async fn a_binding_change_for_the_host_or_one_of_its_suffixes_pushes_the_new_rule_set() {
    let store = Arc::new(FakeWakeStore::default());
    let broker = RouteSubscriptionBroker::new();
    let bind = |binding: &str, identity: RouteIdentity| {
        store.seed_route_resolved(
            identity.clone(),
            domain_route_entry(binding, "instance-push", DomainInstanceState::Running),
        );
        broker.notify_route_changed(RouteBindingId::new(binding).unwrap(), identity);
    };
    store.seed_route_resolved(
        exact_http("app.team.example.com", None),
        domain_route_entry("root", "instance-push", DomainInstanceState::Running),
    );
    let mut proxy = proxy_client_with_route_events(store.clone(), broker.clone());
    let mut stream = open_stream(&mut proxy).await;
    stream.send(subscribe_http(1, "app.team.example.com")).await;
    assert_eq!(rules_bindings(&stream.rules().await), vec!["root"]);

    bind("api", exact_http("app.team.example.com", Some("/api")));
    assert_eq!(rules_bindings(&stream.rules().await), vec!["api", "root"]);

    bind("team", wildcard_http("team.example.com", None));
    assert_eq!(
        rules_bindings(&stream.rules().await),
        vec!["api", "root", "team"]
    );

    bind("apex", wildcard_http("example.com", Some("/apex")));
    assert_eq!(
        rules_bindings(&stream.rules().await),
        vec!["api", "root", "team", "apex"]
    );

    // Bindings that cannot serve this host leave the subscription alone.
    bind("other-host", exact_http("other.team.example.com", None));
    bind("other-suffix", wildcard_http("app.team.example.com", None));
    bind("other-kind", exact_sni("app.team.example.com"));
    stream.quiet().await;
    assert_eq!(store.resolve_route_requests(), 4);

    // A removed binding is announced with the identity it had.
    store.remove_route("team");
    broker.notify_route_changed(
        RouteBindingId::new("team").unwrap(),
        wildcard_http("team.example.com", None),
    );
    assert_eq!(
        rules_bindings(&stream.rules().await),
        vec!["api", "root", "apex"]
    );
}

#[tokio::test(start_paused = true)]
async fn removing_the_last_binding_pushes_an_empty_rule_set_and_ends_the_subscription() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("leaving.example.com", None),
        domain_route_entry("leaving", "instance-leaving", DomainInstanceState::Running),
    );
    let broker = RouteSubscriptionBroker::with_limits(ApiLimits {
        subscriptions_per_stream: 1,
        ..Default::default()
    });
    let mut proxy = proxy_client_with_route_events(store.clone(), broker.clone());
    let mut stream = open_stream(&mut proxy).await;
    stream.send(subscribe_http(1, "leaving.example.com")).await;
    stream.rules().await;

    store.remove_route("leaving");
    broker.notify_route_changed(
        RouteBindingId::new("leaving").unwrap(),
        exact_http("leaving.example.com", None),
    );

    let emptied = stream.rules().await;
    assert_eq!(emptied.subscription_id, 1);
    assert!(emptied.rules.is_empty());
    assert_eq!(
        emptied.cache_policy,
        Some(ProxyCachePolicy { ttl_millis: 1_000 })
    );
    // Its place is free for another host.
    stream.send(subscribe_http(2, "leaving.example.com")).await;
    assert!(stream.rules().await.rules.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_change_that_commits_during_a_lookup_is_followed_by_the_newer_rule_set() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        exact_http("snapshot.example.com", None),
        domain_route_entry(
            "snapshot-route",
            "snapshot-instance",
            DomainInstanceState::Cold,
        ),
    );
    let gate = Arc::new(tokio::sync::Notify::new());
    store.set_resolve_gate(gate.clone());
    let broker = RouteSubscriptionBroker::new();
    let mut proxy = proxy_client_with_route_events(store.clone(), broker.clone());
    let mut stream = open_stream(&mut proxy).await;
    stream.send(subscribe_http(1, "snapshot.example.com")).await;
    settle().await;
    assert_eq!(store.resolve_route_requests(), 1, "the lookup is in flight");

    // The stream cannot yet know which instances the host depends on.
    broker.notify_instance_changed(InstanceId::new("unrelated-instance").unwrap());
    broker.notify_instance_changed(InstanceId::new("snapshot-instance").unwrap());
    settle().await;
    gate.notify_one();
    stream.rules().await;

    // The change may postdate the snapshot the first answer came from.
    settle().await;
    assert_eq!(store.resolve_route_requests(), 2);
    gate.notify_one();
    assert_eq!(stream.rules().await.subscription_id, 1);
    stream.quiet().await;
    assert_eq!(store.resolve_route_requests(), 2);
}

#[tokio::test(start_paused = true)]
async fn lookups_on_one_stream_run_sixteen_at_a_time() {
    let store = Arc::new(FakeWakeStore::default());
    let gate = Arc::new(tokio::sync::Notify::new());
    store.set_resolve_gate(gate.clone());
    let mut proxy = proxy_client_with_route_events(store.clone(), RouteSubscriptionBroker::new());
    let mut stream = open_stream(&mut proxy).await;

    for id in 0..40 {
        stream
            .send(subscribe_http(id, &format!("h{id}.example.com")))
            .await;
    }
    settle().await;
    assert_eq!(store.resolve_route_requests(), 16);

    // Each finished lookup makes room for the next one waiting.
    gate.notify_waiters();
    settle().await;
    assert_eq!(store.resolve_route_requests(), 32);
    gate.notify_waiters();
    settle().await;
    gate.notify_waiters();
    let mut answered: Vec<u64> = Vec::new();
    for _ in 0..40 {
        answered.push(stream.rules().await.subscription_id);
    }
    answered.sort_unstable();
    assert_eq!(answered, (0..40).collect::<Vec<_>>());
    assert_eq!(store.resolve_route_requests(), 40);
}

fn spawn_route_dispatcher(
    store: Arc<FakeWakeStore>,
    broker: RouteSubscriptionBroker,
    cursor: u64,
) -> tokio::sync::watch::Sender<bool> {
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    tokio::spawn(control_plane::runtime::dispatch_route_changes(
        store, broker, cursor, receiver,
    ));
    shutdown
}

#[tokio::test(start_paused = true)]
async fn dispatcher_publishes_only_changes_committed_after_its_cursor() {
    let store = Arc::new(FakeWakeStore::default());
    store.record_instance_change("before-cursor");
    let cursor = store.load_route_change_revision().await.unwrap();
    let broker = RouteSubscriptionBroker::new();
    let mut events = broker.subscribe();
    let _shutdown = spawn_route_dispatcher(store.clone(), broker, cursor);

    store.record_instance_change("after-cursor");
    let event = events.recv().await.unwrap();
    assert!(format!("{event:?}").contains("after-cursor"), "{event:?}");
    assert!(events.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn dispatcher_reads_on_the_store_signal_without_waiting_for_its_poll() {
    let store = Arc::new(FakeWakeStore::default());
    let broker = RouteSubscriptionBroker::new();
    let mut events = broker.subscribe();
    let _shutdown = spawn_route_dispatcher(store.clone(), broker, 0);
    // Let the dispatcher take its first poll tick and park.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let started = tokio::time::Instant::now();
    store.record_instance_change("signalled");
    events.recv().await.unwrap();
    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "the signal wakes the dispatcher at once"
    );

    let started = tokio::time::Instant::now();
    store.record_instance_change_quietly("polled");
    events.recv().await.unwrap();
    assert!(
        started.elapsed() <= Duration::from_millis(250),
        "the poll finds a change no signal announced"
    );
}

#[tokio::test(start_paused = true)]
async fn dispatcher_read_failure_keeps_subscribers_and_delivers_after_recovery() {
    let store = Arc::new(FakeWakeStore::default());
    let broker = RouteSubscriptionBroker::new();
    let mut events = broker.subscribe();
    let _shutdown = spawn_route_dispatcher(store.clone(), broker, 0);
    tokio::time::sleep(Duration::from_millis(10)).await;

    store.fail_route_change_reads(8);
    store.record_instance_change("during-outage");
    let event = events.recv().await.unwrap();
    assert!(
        format!("{event:?}").contains("during-outage"),
        "the first event after the outage is the change itself, with no reset: {event:?}"
    );
}

#[test]
fn native_grpc_server_can_be_constructed_with_store_backed_proxy_service() {
    let _router = tonic::transport::Server::builder().add_service(proxy_grpc_service_with_store(
        Arc::new(FakeWakeStore::default()),
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    ));

    assert_eq!(
        <StoreBackedProxyGrpcService<FakeKubernetesClient> as NamedService>::NAME,
        PROXY_SERVICE_NAME
    );
}

fn grpc_proxy_wake_request(
    request: ProxyWakeInstanceRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_unary_request(
        request,
        "/sleepypods.controlplane.v1.ProxyControlPlane/WakeInstance",
        content_type,
        version,
    )
}

fn grpc_proxy_subscribe_request(
    requests: Vec<ProxySubscribeRequest>,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_stream_request(
        requests,
        "/sleepypods.controlplane.v1.ProxyControlPlane/Subscribe",
        content_type,
        version,
    )
}

fn grpc_unary_request<M: Message>(
    request: M,
    uri: &'static str,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_stream_request(vec![request], uri, content_type, version)
}

fn grpc_stream_request<M: Message>(
    requests: Vec<M>,
    uri: &'static str,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    let mut body = BytesMut::new();
    for request in requests {
        encode_grpc_message(request, &mut body);
    }

    Request::builder()
        .version(version)
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(body.freeze())))
        .expect("request builds")
}

fn with_authorization(mut request: Request<Body>, value: &'static str) -> Request<Body> {
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, HeaderValue::from_static(value));
    request
}

fn encode_grpc_message<M: Message>(request: M, body: &mut BytesMut) {
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");

    body.put_u8(0);
    body.put_u32(message.len() as u32);
    body.extend_from_slice(&message);
}

fn decode_grpc_proxy_wake_response(bytes: &[u8]) -> ProxyWakeInstanceResponse {
    decode_grpc_message(bytes)
}

async fn collect_grpc_status<B>(response: HttpResponse<B>) -> String
where
    B: tonic::codegen::Body<Data = Bytes>,
    B::Error: std::fmt::Debug,
{
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    grpc_status(&headers, trailers.as_ref())
}

fn grpc_status(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> String {
    trailers
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("gRPC status is returned")
        .to_str()
        .expect("gRPC status is valid")
        .to_owned()
}

fn decode_grpc_message<M: Message + Default>(bytes: &[u8]) -> M {
    assert_eq!(bytes.first(), Some(&0), "gRPC message is uncompressed");
    let length = u32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .expect("gRPC response frame has a length prefix"),
    ) as usize;
    M::decode(&bytes[5..5 + length]).expect("gRPC response decodes")
}

fn subscribe_request(
    subscription_id: u64,
    kind: ProtoRouteKind,
    host: &str,
) -> ProxySubscribeRequest {
    ProxySubscribeRequest {
        input: Some(proxy_subscribe_request::Input::Subscribe(
            ProxySubscribeHostRequest {
                subscription_id,
                kind: kind as i32,
                host: host.to_owned(),
            },
        )),
    }
}

fn subscribe_http(subscription_id: u64, host: &str) -> ProxySubscribeRequest {
    subscribe_request(subscription_id, ProtoRouteKind::Http, host)
}

fn subscribe_sni(subscription_id: u64, host: &str) -> ProxySubscribeRequest {
    subscribe_request(subscription_id, ProtoRouteKind::Sni, host)
}

fn unsubscribe_request(subscription_id: u64) -> ProxySubscribeRequest {
    ProxySubscribeRequest {
        input: Some(proxy_subscribe_request::Input::Unsubscribe(
            control_plane::api::pb::ProxyUnsubscribeRequest { subscription_id },
        )),
    }
}

fn expect_rules(response: ProxySubscribeResponse) -> ProxyHostRulesResponse {
    match response.output {
        Some(proxy_subscribe_response::Output::Rules(rules)) => rules,
        other => panic!("expected a rule set, got {other:?}"),
    }
}

fn expect_error(response: ProxySubscribeResponse) -> ProxySubscribeErrorResponse {
    match response.output {
        Some(proxy_subscribe_response::Output::Error(error)) => error,
        other => panic!("expected a subscription error, got {other:?}"),
    }
}

/// Host kind, host, path prefix and binding of each rule, in the order sent.
fn rule_summary(rules: &ProxyHostRulesResponse) -> Vec<(i32, String, Option<String>, String)> {
    rules
        .rules
        .iter()
        .map(|rule| {
            let identity = rule
                .identity
                .as_ref()
                .and_then(|identity| identity.kind.as_ref());
            let (host, path) = match identity.expect("rule identity") {
                control_plane::api::pb::route_identity::Kind::Http(http) => {
                    (http.host.clone(), http.path_prefix.clone())
                }
                control_plane::api::pb::route_identity::Kind::Sni(sni) => (sni.host.clone(), None),
            };
            let host = host.expect("rule host");
            (
                host.kind,
                host.host,
                path,
                rule.route
                    .as_ref()
                    .expect("rule route")
                    .route_binding_id
                    .clone(),
            )
        })
        .collect()
}

fn rules_bindings(rules: &ProxyHostRulesResponse) -> Vec<String> {
    rule_summary(rules)
        .into_iter()
        .map(|(_, _, _, binding)| binding)
        .collect()
}

async fn next_subscribe_response<S>(responses: &mut S) -> ProxySubscribeResponse
where
    S: tonic::codegen::tokio_stream::Stream<Item = Result<ProxySubscribeResponse, tonic::Status>>
        + Unpin,
{
    tokio::time::timeout(Duration::from_secs(1), responses.next())
        .await
        .expect("subscribe response arrives")
        .expect("subscribe stream remains open")
        .expect("subscribe response succeeds")
}

type ProxyClient =
    ProxyControlPlaneClient<InProcessService<StoreBackedProxyGrpcService<FakeKubernetesClient>>>;

/// One open Subscribe stream and the sender that keeps its request side open.
struct OpenStream {
    requests: tokio::sync::mpsc::Sender<ProxySubscribeRequest>,
    responses: tonic::Streaming<ProxySubscribeResponse>,
}

async fn open_stream(proxy: &mut ProxyClient) -> OpenStream {
    let (requests, incoming) = tokio::sync::mpsc::channel(256);
    let responses = proxy
        .subscribe(ReceiverStream::new(incoming))
        .await
        .expect("the stream opens")
        .into_inner();
    OpenStream {
        requests,
        responses,
    }
}

impl OpenStream {
    async fn send(&self, request: ProxySubscribeRequest) {
        self.requests
            .send(request)
            .await
            .expect("the stream is open");
    }

    async fn rules(&mut self) -> ProxyHostRulesResponse {
        expect_rules(next_subscribe_response(&mut self.responses).await)
    }

    async fn error(&mut self) -> ProxySubscribeErrorResponse {
        expect_error(next_subscribe_response(&mut self.responses).await)
    }

    /// Asserts the control plane has nothing to say.
    async fn quiet(&mut self) {
        let next = tokio::time::timeout(Duration::from_millis(50), self.responses.message()).await;
        assert!(next.is_err(), "unexpected stream activity: {next:?}");
    }

    /// Waits for the stream to end and reports how.
    async fn end(&mut self) -> Result<(), tonic::Status> {
        match tokio::time::timeout(Duration::from_secs(120), self.responses.message())
            .await
            .expect("the stream ends")
        {
            Ok(None) => Ok(()),
            Ok(Some(message)) => panic!("expected the stream to end, got {message:?}"),
            Err(status) => Err(status),
        }
    }
}

/// Subscribes to one HTTP host on a fresh stream and returns its rule set.
async fn subscribe_once(store: Arc<FakeWakeStore>, host: &str) -> ProxyHostRulesResponse {
    let mut proxy = proxy_client_with_route_events(store, RouteSubscriptionBroker::new());
    let mut stream = open_stream(&mut proxy).await;
    stream.send(subscribe_http(1, host)).await;
    stream.rules().await
}

/// Lets every task run until all of them wait.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

#[derive(Clone)]
struct InProcessService<S> {
    inner: S,
    /// Answers every call with `UNAVAILABLE` while set, as an unreachable
    /// control plane would.
    unreachable: Arc<std::sync::atomic::AtomicBool>,
}

impl<S> InProcessService<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            unreachable: Arc::default(),
        }
    }
}

impl<S, B> Service<Request<Body>> for InProcessService<S>
where
    S: Service<Request<Body>, Response = HttpResponse<B>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: tonic::codegen::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<tonic::codegen::StdError>,
{
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Infallible>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if self.unreachable.load(std::sync::atomic::Ordering::SeqCst) {
            return Box::pin(async {
                Ok(tonic::Status::unavailable("the control plane is unreachable").into_http())
            });
        }
        let call = self.inner.call(request);
        Box::pin(async move { Ok(call.await?.map(Body::new)) })
    }
}

#[derive(Clone, Debug, Default)]
struct FakeKubernetesClient {
    applied_objects: Arc<Mutex<Vec<control_plane::KubernetesObject>>>,
    live_objects: Arc<Mutex<BTreeMap<String, ProjectionObjectInspection>>>,
    fail_readiness: bool,
}

impl FakeKubernetesClient {
    fn failing_readiness() -> Self {
        Self {
            applied_objects: Arc::new(Mutex::new(Vec::new())),
            live_objects: Arc::new(Mutex::new(BTreeMap::new())),
            fail_readiness: true,
        }
    }

    fn applied_objects_len(&self) -> usize {
        self.applied_objects
            .lock()
            .expect("fake kubernetes lock is available")
            .len()
    }

    fn set_unowned_live_object(&self, object: RenderedObjectRef) {
        self.live_objects
            .lock()
            .expect("fake kubernetes lock is available")
            .insert(
                object_key(&object),
                ProjectionObjectInspection::Present(LiveObjectMetadata {
                    persistent_volume_reclaim_policy: Some("Retain".into()),
                    identity: control_plane::projection::LiveObjectIdentity {
                        uid: "test-uid".into(),
                        resource_version: "1".into(),
                    },
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                    deleting: false,
                    finalizers: Vec::new(),
                }),
            );
    }
}

fn object_key(object: &RenderedObjectRef) -> String {
    format!(
        "{}|{}|{}|{}",
        object.api_version, object.kind, object.namespace, object.name
    )
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn verify_idle_member<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        identity: &'a control_plane::materializer::IdleMemberIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            if identity.pod_uid == "test-pod" {
                Ok(())
            } else {
                Err(control_plane::KubernetesClientError::new("unknown pod UID"))
            }
        })
    }

    fn apply_object<'a>(
        &'a self,
        object: &'a control_plane::KubernetesObject,
        _precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let object_ref = rendered_object_ref(object);
            self.live_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .insert(
                    object_key(&object_ref),
                    ProjectionObjectInspection::Present(LiveObjectMetadata::from_rendered_object(
                        object,
                    )),
                );
            self.applied_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .push(object.clone());
            Ok(())
        })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        _precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.live_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .remove(&object_key(object));
            Ok(())
        })
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async {
            if self.fail_readiness {
                return Err(KubernetesClientError::new("not ready"));
            }

            BackendEndpoint::new("http://svc-acme-69856ec0faae6daf.apps.svc.cluster.local:80")
                .map_err(|error| KubernetesClientError::new(error.to_string()))
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            Ok(self
                .live_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .get(&object_key(object))
                .cloned()
                .unwrap_or(ProjectionObjectInspection::Missing))
        })
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        _instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<()>,
    > {
        Box::pin(async { Ok(()) })
    }
}

fn proxy_api(
    store: Arc<FakeWakeStore>,
    client: FakeKubernetesClient,
) -> StoreBackedProxyApi<FakeKubernetesClient> {
    proxy_api_with_target(store, client, proxy_target())
}

fn proxy_api_with_target(
    store: Arc<FakeWakeStore>,
    client: FakeKubernetesClient,
    target: MaterializationTarget,
) -> StoreBackedProxyApi<FakeKubernetesClient> {
    StoreBackedProxyApi::new(store, KubernetesMaterializer::new(client), target)
}

fn proxy_client_with_route_events(
    store: Arc<FakeWakeStore>,
    route_events: RouteSubscriptionBroker,
) -> ProxyControlPlaneClient<InProcessService<StoreBackedProxyGrpcService<FakeKubernetesClient>>> {
    ProxyControlPlaneClient::new(InProcessService::new(
        proxy_grpc_service_with_store_and_route_events(
            store,
            KubernetesMaterializer::new(FakeKubernetesClient::default()),
            proxy_target(),
            route_events,
        ),
    ))
}

fn proxy_target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("proxy target is valid")
}

async fn reconcile_proxy_work(store: Arc<FakeWakeStore>, client: FakeKubernetesClient) {
    control_plane::MaterializationReconciler::new(
        store,
        KubernetesMaterializer::new(client),
        proxy_target(),
        Default::default(),
        sleepypods_observability::recorder::ObservabilityRecorder::noop(),
    )
    .run_once()
    .await;
}

fn expect_accepted(
    response: ProxyWakeInstanceResponse,
) -> control_plane::api::pb::ProxyWakeStillWakingResult {
    match response.outcome {
        Some(proxy_wake_instance_response::Outcome::StillWaking(result)) => result,
        other => panic!("expected durable wake acceptance, got {other:?}"),
    }
}

fn expect_ready(
    response: control_plane::api::pb::ProxyWakeInstanceResponse,
) -> control_plane::api::pb::ProxyWakeReadyResult {
    let Some(proxy_wake_instance_response::Outcome::Ready(ready)) = response.outcome else {
        panic!("expected ready response");
    };
    ready
}

fn domain_instance(id: &str, state: DomainInstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id).expect("instance ID is valid"),
        workload_class: domain_workload_ref(),
        values: control_plane::InstanceValues::new(),
        state,
        generation: Generation::new(generation),
    }
}

fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}

fn domain_workload_class() -> control_plane::WorkloadClassVersion {
    control_plane::WorkloadClassVersion {
        reference: domain_workload_ref(),
        template: control_plane::ManifestTemplate {
            workload: control_plane::WorkloadTemplate {
                kind: control_plane::WorkloadKind::Deployment,
                name: control_plane::TemplateText::literal("app-acme"),
                replicas: None,
                app_container: control_plane::ContainerTemplate {
                    name: "app".to_owned(),
                    image: control_plane::TemplateText::literal("example/app:1"),
                    ports: vec![control_plane::ContainerPortTemplate {
                        name: Some("http".to_owned()),
                        container_port: 8080,
                    }],
                    env: Vec::new(),
                },
                termination_grace_period_seconds: None,
            },
            service: Some(control_plane::ServiceTemplate {
                name: control_plane::TemplateText::literal("svc-acme"),
                ports: vec![control_plane::ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            sidecar: control_plane::SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: control_plane::TemplateText::literal("sleepypods/sidecar:test"),
                listen_port: 15000,
                mode: None,
                app_protocol: Default::default(),
            },
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        },
        value_schema: control_plane::WorkloadValueSchema::new(true),
        sleep_policy: control_plane::WorkloadSleepPolicy::new(300_000, 5_000, 30_000)
            .expect("valid sleep policy"),
        exclusivity_keys: vec![],
    }
}

fn domain_workload_ref() -> control_plane::WorkloadClassVersionRef {
    control_plane::WorkloadClassVersionRef::new(
        control_plane::WorkloadClassId::new("class-1").expect("workload class ID is valid"),
        Generation::new(1),
    )
}

fn ready_materialization(instance_id: &str, generation: u64) -> MaterializationRecord {
    materialization_with_state(instance_id, generation, MaterializationState::Ready)
}

fn materialization_with_state(
    instance_id: &str,
    generation: u64,
    state: MaterializationState,
) -> MaterializationRecord {
    materialization_with_state_and_target(instance_id, generation, state, proxy_target())
}

fn materialization_with_state_and_target(
    instance_id: &str,
    generation: u64,
    state: MaterializationState,
    target: MaterializationTarget,
) -> MaterializationRecord {
    MaterializationRecord {
        id: MaterializationId::new(format!(
            "{}:{}:{}",
            instance_id,
            target.cluster_id(),
            target.namespace()
        ))
        .expect("materialization ID is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance ID is valid"),
        instance_generation: Generation::new(generation),
        projection_generation: Generation::new(generation),
        target,
        state,
        backend: Some(
            BackendEndpoint::new("http://svc-acme.apps.svc.cluster.local:80")
                .expect("backend URI is valid"),
        ),
        backend_generation: BackendGeneration::new(generation),
        rendered_objects: Vec::new(),
        exclusivity_keys: vec![],
        reconciliation_lease: None,
    }
}

fn exact_http(host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("route host is valid"),
        path: path_prefix.map(|path| PathPrefix::new(path).expect("path prefix is valid")),
    }
}

fn wildcard_http(suffix: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(suffix).expect("route host is valid"),
        path: path_prefix.map(|path| PathPrefix::new(path).expect("path prefix is valid")),
    }
}

fn exact_sni(host: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::exact(host).expect("route host is valid"),
    }
}

fn wildcard_sni(suffix: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::wildcard_suffix(suffix).expect("route host is valid"),
    }
}

fn domain_route_entry(
    route_binding_id: &str,
    instance_id: &str,
    state: DomainInstanceState,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: RouteBindingId::new(route_binding_id).expect("route binding ID is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance ID is valid"),
        instance_state: state,
        instance_generation: Generation::new(7),
        backend: Some(
            BackendEndpoint::new("http://backend.example.local:8080").expect("backend is valid"),
        ),
        backend_generation: Some(BackendGeneration::new(9)),
    }
}
