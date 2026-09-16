use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use sleepypods_api::{
    pb::{
        self,
        proxy_control_plane_client::ProxyControlPlaneClient,
        proxy_control_plane_server::{ProxyControlPlane, ProxyControlPlaneServer},
    },
    BackendEndpoint, BackendGeneration, BearerToken, CachePolicy, Generation, Http01ChallengeKey,
    InstanceId, InstanceState, OptionalBearerTokenInterceptor, PathPrefix, RouteBindingId,
    RouteEntry, RouteHost, RouteIdentity,
};
use tokio::sync::{mpsc, Notify};
use tonic::{
    codegen::{http, tokio_stream::wrappers::ReceiverStream, Service},
    Request, Response, Status,
};

use super::{
    GrpcProxyControlPlaneClient, GrpcProxyControlPlaneError, GrpcProxyHttp01Resolver,
    GrpcProxyHttp01ResolverError,
};
use crate::{
    Http01ChallengeResolver, InvalidationReason, RouteRequestId, RouteSubscriptionClient,
    SubscribeControlPlaneOutput, SubscriptionId, WakeClient, WakeInstanceRequest,
    WakeInstanceResponse,
};

const HTTP01_EXPIRES_AT_UNIX_MILLIS: i64 = 2_000;

#[tokio::test]
async fn wake_instance_ready_response_maps_through_generated_client() {
    let service = FakeProxyControlPlane::default();
    service.set_wake_response(Ok(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
            pb::ProxyWakeReadyResult {
                backend_address: None,
                instance_id: "instance-a".to_owned(),
                instance_generation: 7,
                backend_uri: "http://10.0.0.7:8080".to_owned(),
                backend_generation: 3,
            },
        )),
    }));
    let mut client = test_client(service.clone());

    let response = client
        .wake_instance(WakeInstanceRequest {
            instance_id: instance_id("instance-a"),
            expected_generation: Generation::new(7),
        })
        .await
        .expect("wake succeeds");

    assert_eq!(
        response,
        WakeInstanceResponse::AlreadyRunning {
            instance_id: instance_id("instance-a"),
            generation: Generation::new(7),
            backend: backend("http://10.0.0.7:8080"),
            backend_generation: Some(BackendGeneration::new(3)),
        }
    );
    assert_eq!(
        service.wake_requests(),
        vec![pb::ProxyWakeInstanceRequest {
            instance_id: "instance-a".to_owned(),
            expected_generation: 7,
            backend_generation: None,
        }]
    );
}

#[tokio::test]
async fn subscribe_route_resolved_response_maps_through_generated_client() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "req-1", "sub-1",
    ))]));
    let mut client = test_client(service.clone());

    let response = client
        .subscribe_route(
            route_request_id("req-1"),
            http_identity("app.example.com", None),
        )
        .await
        .expect("subscribe route succeeds");

    assert_eq!(
        wire_message(response),
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: route_request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_identity("app.example.com", None),
            entry: route_entry(),
            cache_policy: cache_policy(10_000),
        }
    );
    assert_subscribe_route_request(
        &service.wait_for_subscribe_requests(1).await[0],
        "req-1",
        "app.example.com",
    );
}

#[tokio::test]
async fn subscribe_route_miss_response_maps_through_generated_client() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_miss_response(
        "req-miss",
    ))]));
    let mut client = test_client(service);

    let response = client
        .subscribe_route(
            route_request_id("req-miss"),
            http_identity("missing.example.com", None),
        )
        .await
        .expect("subscribe route miss succeeds");

    assert_eq!(
        response,
        SubscribeControlPlaneOutput::RouteMiss {
            request_id: route_request_id("req-miss"),
            request_identity: http_identity("missing.example.com", None),
            negative_cache_policy: cache_policy(5_000),
        }
    );
}

#[tokio::test]
async fn pushed_updates_before_route_response_are_buffered_for_next_update() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![
        Ok(route_updated_response("sub-update")),
        Ok(route_invalidated_response("sub-invalidated")),
        Ok(route_resolved_response("req-buffered", "sub-resolved")),
    ]));
    let mut client = test_client(service);

    let response = client
        .subscribe_route(
            route_request_id("req-buffered"),
            http_identity("app.example.com", None),
        )
        .await
        .expect("matching route response is returned");

    assert!(matches!(
        wire_message(response),
        SubscribeControlPlaneOutput::RouteResolved { .. }
    ));
    assert_eq!(
        wire_message(client.next_update().await.expect("first buffered update")),
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-update"),
            matched_identity: http_identity("app.example.com", None),
            entry: route_entry(),
            cache_policy: cache_policy(15_000),
        }
    );
    assert_eq!(
        wire_message(client.next_update().await.expect("second buffered update")),
        SubscribeControlPlaneOutput::RouteInvalidated {
            subscription_id: subscription_id("sub-invalidated"),
            reason: InvalidationReason::BackendChanged,
        }
    );
}

#[tokio::test]
async fn unsubscribe_sends_request_and_does_not_wait_for_ack() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "first",
        "sub-opaque",
    ))]));
    let mut client = test_client(service.clone());
    let SubscribeControlPlaneOutput::RouteResolved {
        subscription_id, ..
    } = client
        .subscribe_route(
            route_request_id("first"),
            http_identity("app.example.com", None),
        )
        .await
        .expect("subscribe succeeds")
    else {
        panic!("resolved")
    };
    client
        .unsubscribe(subscription_id)
        .await
        .expect("unsubscribe send succeeds");
    let requests = service.wait_for_subscribe_requests(2).await;
    match requests[1].input.as_ref().expect("input") {
        pb::proxy_subscribe_request::Input::Unsubscribe(request) => {
            assert_eq!(request.subscription_id, "sub-opaque");
        }
        _ => panic!("expected unsubscribe request"),
    }
}

#[tokio::test]
async fn subscribe_status_error_is_surfaced() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(vec![Err(
        Status::unavailable("store unavailable"),
    )]));
    let mut client = test_client(service);

    let error = client
        .subscribe_route(
            route_request_id("req-status"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("status should surface");

    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::Status(status) if status.code() == tonic::Code::Unavailable
    ));
}

#[tokio::test]
async fn malformed_subscribe_response_is_protocol_error() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(
        pb::ProxySubscribeResponse { output: None },
    )]));
    let mut client = test_client(service);

    let error = client
        .subscribe_route(
            route_request_id("req-malformed"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("malformed response should surface");

    assert!(matches!(error, GrpcProxyControlPlaneError::Protocol(_)));
}

#[tokio::test]
async fn unknown_route_response_fails_in_flight_subscribe_and_tears_down_session() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "req-unexpected",
        "sub-unexpected",
    ))]));
    let mut client = test_client(service);

    // The response reader must fail the pending matching request before it
    // reports the fatal unknown response, otherwise the subscribe future hangs.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        client.subscribe_route(
            route_request_id("req-pending"),
            http_identity("app.example.com", None),
        ),
    )
    .await
    .expect("in-flight subscribe returns")
    .expect_err("unknown response request ID should fail subscribe");
    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::UnexpectedRouteResponse { request_id }
            if request_id == route_request_id("req-unexpected")
    ));

    let event_error = tokio::time::timeout(Duration::from_secs(1), client.next_update())
        .await
        .expect("fatal unknown response event is delivered")
        .expect_err("unknown response tears down the session");
    assert!(matches!(
        event_error,
        GrpcProxyControlPlaneError::UnexpectedRouteResponse { request_id }
            if request_id == route_request_id("req-unexpected")
    ));
    assert!(client
        .transport
        .lock()
        .await
        .session
        .as_ref()
        .is_some_and(|session| session.closed.load(std::sync::atomic::Ordering::Acquire)));
}

#[tokio::test]
async fn closed_subscribe_response_stream_is_surfaced() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(Vec::new()));
    let mut client = test_client(service);

    let error = client
        .subscribe_route(
            route_request_id("req-closed"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("closed response stream should surface");

    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::SubscribeResponseStreamClosed
    ));
}

#[tokio::test]
async fn subscribe_route_after_response_stream_close_opens_new_stream() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(Vec::new()));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "req-reconnected",
        "sub-reconnected",
    ))]));
    let mut client = test_client(service.clone());

    let first = client
        .subscribe_route(
            route_request_id("req-closed"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("first stream closes before response");
    assert!(matches!(
        first,
        GrpcProxyControlPlaneError::SubscribeResponseStreamClosed
    ));

    let response = client
        .subscribe_route(
            route_request_id("req-reconnected"),
            http_identity("app.example.com", None),
        )
        .await
        .expect("next subscribe uses a fresh stream");

    assert_eq!(
        wire_message(response),
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: route_request_id("req-reconnected"),
            subscription_id: subscription_id("sub-reconnected"),
            matched_identity: http_identity("app.example.com", None),
            entry: route_entry(),
            cache_policy: cache_policy(10_000),
        }
    );
    let requests = service.wait_for_subscribe_requests(2).await;
    assert_subscribe_route_request(&requests[0], "req-closed", "app.example.com");
    assert_subscribe_route_request(&requests[1], "req-reconnected", "app.example.com");
}

#[tokio::test(start_paused = true)]
async fn subscribe_route_reconnect_waits_for_backoff_after_stream_close() {
    let backoff = Duration::from_secs(1);
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(Vec::new()));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "req-after-backoff",
        "sub-after-backoff",
    ))]));
    let mut client = test_client_with_subscribe_reconnect_backoff(service.clone(), backoff);

    let first = client
        .subscribe_route(
            route_request_id("req-before-close"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("first stream closes before response");
    assert!(matches!(
        first,
        GrpcProxyControlPlaneError::SubscribeResponseStreamClosed
    ));
    assert_eq!(service.subscribe_request_count(), 1);

    let mut second = client.subscribe_route(
        route_request_id("req-after-backoff"),
        http_identity("app.example.com", None),
    );

    tokio::select! {
        biased;
        result = &mut second => panic!("second subscribe completed before backoff: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(service.subscribe_request_count(), 1);

    tokio::time::advance(backoff - Duration::from_millis(1)).await;
    tokio::select! {
        biased;
        result = &mut second => panic!("second subscribe completed before backoff elapsed: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(service.subscribe_request_count(), 1);

    tokio::time::advance(Duration::from_millis(1)).await;
    let response = second
        .await
        .expect("second subscribe opens after backoff elapses");
    assert!(matches!(
        wire_message(response),
        SubscribeControlPlaneOutput::RouteResolved { .. }
    ));

    let requests = service.wait_for_subscribe_requests(2).await;
    assert_subscribe_route_request(&requests[0], "req-before-close", "app.example.com");
    assert_subscribe_route_request(&requests[1], "req-after-backoff", "app.example.com");
}

#[tokio::test]
async fn response_stream_close_after_route_response_is_observed_by_response_reader() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(vec![Ok(
        route_resolved_response("req-before-close", "sub-before-close"),
    )]));
    let mut client = test_client(service);

    client
        .subscribe_route(
            route_request_id("req-before-close"),
            http_identity("app.example.com", None),
        )
        .await
        .expect("route response arrives before stream close");

    let error = tokio::time::timeout(Duration::from_secs(1), client.next_update())
        .await
        .expect("response reader observes terminal stream close")
        .expect_err("terminal stream close is surfaced");
    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::SubscribeResponseStreamClosed
    ));
}

#[tokio::test]
async fn pushed_updates_over_response_buffer_are_drained_without_public_cursor() {
    let service = FakeProxyControlPlane::default();
    let mut responses = (0..20)
        .map(|index| Ok(route_updated_response(&format!("sub-update-{index}"))))
        .collect::<Vec<_>>();
    responses.push(Ok(route_resolved_response(
        "req-after-updates",
        "sub-route",
    )));
    service.push_subscribe_action(SubscribeAction::respond(responses));
    let mut client = test_client(service);

    let response = tokio::time::timeout(
        Duration::from_secs(1),
        client.subscribe_route(
            route_request_id("req-after-updates"),
            http_identity("app.example.com", None),
        ),
    )
    .await
    .expect("subscribe completes while upstream response sender backpressures")
    .expect("route response succeeds");
    assert!(matches!(
        wire_message(response),
        SubscribeControlPlaneOutput::RouteResolved { .. }
    ));

    for index in 0..20 {
        let update = tokio::time::timeout(Duration::from_secs(1), client.next_update())
            .await
            .expect("buffered update is delivered")
            .expect("buffered update maps");
        assert_eq!(
            wire_message(update),
            SubscribeControlPlaneOutput::RouteUpdated {
                subscription_id: subscription_id(&format!("sub-update-{index}")),
                matched_identity: http_identity("app.example.com", None),
                entry: route_entry(),
                cache_policy: cache_policy(15_000),
            }
        );
    }
}

#[tokio::test]
async fn closed_subscribe_request_stream_is_surfaced() {
    let service = FakeProxyControlPlane::default();
    let mut client = test_client(service);
    client
        .transport
        .lock()
        .await
        .ensure(client.events.clone())
        .await
        .expect("subscription starts");
    let (closed_requests, closed_receiver) = mpsc::channel(1);
    drop(closed_receiver);
    client
        .transport
        .lock()
        .await
        .session
        .as_mut()
        .expect("session")
        .requests = closed_requests;

    let session_id = client.transport.lock().await.session.as_ref().unwrap().id;
    let error = client
        .unsubscribe(subscription_id("sub-closed").with_session(session_id))
        .await
        .expect_err("closed request stream should surface");

    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::SubscribeRequestStreamClosed
    ));
}

#[tokio::test]
async fn http01_resolve_hit_sends_key_and_maps_challenge_record() {
    let service = FakeHttp01Proxy::default();
    service.set_resolve_http01_response(Ok(pb::ResolveHttp01ChallengeResponse {
        challenge: Some(http01_challenge(
            "app.example.com",
            "token-a",
            "token-a.key",
        )),
    }));
    let mut resolver = test_http01_resolver(service.clone());

    let response = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("App.Example.COM.", "token-a").expect("valid key"),
        )
        .await
        .expect("HTTP-01 resolve succeeds")
        .expect("challenge resolves");

    assert_eq!(response.key().host().as_str(), "app.example.com");
    assert_eq!(response.key().token(), "token-a");
    assert_eq!(response.key_authorization(), "token-a.key");
    assert_eq!(
        service.resolve_http01_requests(),
        vec![pb::ResolveHttp01ChallengeRequest {
            key: Some(pb::Http01ChallengeKey {
                host: "app.example.com".to_owned(),
                token: "token-a".to_owned(),
            })
        }]
    );
}

#[tokio::test]
async fn http01_resolve_with_proxy_token_sends_authorization_metadata() {
    let service = FakeHttp01Proxy::default();
    service.set_resolve_http01_response(Ok(pb::ResolveHttp01ChallengeResponse {
        challenge: Some(http01_challenge(
            "app.example.com",
            "token-a",
            "token-a.key",
        )),
    }));
    let token = BearerToken::new("proxy_token", "proxy-secret").expect("valid token");
    let interceptor = OptionalBearerTokenInterceptor::new(Some(&token)).expect("valid interceptor");
    let server = ProxyControlPlaneServer::new(service.clone());
    let mut resolver = GrpcProxyHttp01Resolver::new(ProxyControlPlaneClient::with_interceptor(
        InProcessService::new(server),
        interceptor,
    ));

    let response = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("app.example.com", "token-a").expect("valid key"),
        )
        .await
        .expect("HTTP-01 resolve succeeds")
        .expect("challenge resolves");

    assert_eq!(response.key_authorization(), "token-a.key");
    assert_eq!(
        service.resolve_http01_authorizations(),
        vec![Some("Bearer proxy-secret".to_owned())]
    );
}

#[tokio::test]
async fn http01_resolve_miss_maps_to_none() {
    let service = FakeHttp01Proxy::default();
    service.set_resolve_http01_response(Ok(pb::ResolveHttp01ChallengeResponse { challenge: None }));
    let mut resolver = test_http01_resolver(service);

    let response = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("missing.example.com", "token-a").expect("valid key"),
        )
        .await
        .expect("HTTP-01 resolve succeeds");

    assert!(response.is_none());
}

#[tokio::test]
async fn http01_resolve_status_error_is_surfaced() {
    let service = FakeHttp01Proxy::default();
    service.set_resolve_http01_response(Err(Status::unavailable("store unavailable")));
    let mut resolver = test_http01_resolver(service);

    let error = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("app.example.com", "token-a").expect("valid key"),
        )
        .await
        .expect_err("status should surface");

    assert!(matches!(
        error,
        GrpcProxyHttp01ResolverError::Status(status)
            if status.code() == tonic::Code::Unavailable
    ));
}

#[tokio::test]
async fn http01_resolve_malformed_challenge_is_protocol_error() {
    let service = FakeHttp01Proxy::default();
    service.set_resolve_http01_response(Ok(pb::ResolveHttp01ChallengeResponse {
        challenge: Some(pb::Http01Challenge {
            key: None,
            key_authorization: "token-a.key".to_owned(),
            expires_at_unix_millis: HTTP01_EXPIRES_AT_UNIX_MILLIS,
        }),
    }));
    let mut resolver = test_http01_resolver(service);

    let error = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("app.example.com", "token-a").expect("valid key"),
        )
        .await
        .expect_err("malformed challenge should surface");

    assert!(matches!(error, GrpcProxyHttp01ResolverError::Protocol(_)));
}

#[derive(Clone)]
struct InProcessService<S> {
    inner: S,
}

impl<S> InProcessService<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<http::Request<tonic::body::Body>> for InProcessService<S>
where
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        self.inner.call(request)
    }
}

#[derive(Clone, Default)]
struct FakeProxyControlPlane {
    state: Arc<Mutex<FakeProxyControlPlaneState>>,
    subscribe_notify: Arc<Notify>,
}

#[derive(Clone, Default)]
struct FakeHttp01Proxy {
    state: Arc<Mutex<FakeHttp01ProxyState>>,
}

#[derive(Default)]
struct FakeProxyControlPlaneState {
    wake_response: Option<Result<pb::ProxyWakeInstanceResponse, Status>>,
    wake_requests: Vec<pb::ProxyWakeInstanceRequest>,
    subscribe_actions: VecDeque<SubscribeAction>,
    subscribe_requests: Vec<pb::ProxySubscribeRequest>,
}

#[derive(Default)]
struct FakeHttp01ProxyState {
    resolve_http01_response: Option<Result<pb::ResolveHttp01ChallengeResponse, Status>>,
    resolve_http01_requests: Vec<pb::ResolveHttp01ChallengeRequest>,
    resolve_http01_authorizations: Vec<Option<String>>,
}

struct SubscribeAction {
    gate: Option<Arc<Notify>>,
    responses: Vec<Result<pb::ProxySubscribeResponse, Status>>,
    close_after: bool,
}

impl SubscribeAction {
    fn respond(responses: Vec<Result<pb::ProxySubscribeResponse, Status>>) -> Self {
        Self {
            responses,
            close_after: false,
            gate: None,
        }
    }

    fn close_after(responses: Vec<Result<pb::ProxySubscribeResponse, Status>>) -> Self {
        Self {
            responses,
            close_after: true,
            gate: None,
        }
    }
}

#[tonic::async_trait]
impl ProxyControlPlane for FakeProxyControlPlane {
    type WatchTlsCertificatesStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<sleepypods_api::pb::WatchTlsCertificatesResponse, tonic::Status>,
                > + Send,
        >,
    >;
    async fn watch_tls_certificates(
        &self,
        _: tonic::Request<tonic::Streaming<sleepypods_api::pb::WatchTlsCertificatesRequest>>,
    ) -> Result<tonic::Response<Self::WatchTlsCertificatesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "unexpected test certificate watch",
        ))
    }

    async fn resolve_http01_challenge(
        &self,
        _: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        panic!("unexpected HTTP01")
    }
    async fn resolve_tls_certificate(
        &self,
        _: Request<pb::ResolveTlsCertificateRequest>,
    ) -> Result<Response<pb::ResolveTlsCertificateResponse>, Status> {
        panic!("unexpected certificate")
    }

    type SubscribeStream = ReceiverStream<Result<pb::ProxySubscribeResponse, Status>>;

    async fn wake_instance(
        &self,
        request: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        let response = {
            let mut state = self.state.lock().expect("fake state");
            state.wake_requests.push(request.into_inner());
            state.wake_response.take().unwrap_or_else(|| {
                Err(Status::failed_precondition(
                    "test did not configure wake response",
                ))
            })
        };

        response.map(Response::new)
    }

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut requests = request.into_inner();
        let state = Arc::clone(&self.state);
        let subscribe_notify = Arc::clone(&self.subscribe_notify);
        let (responses, response_stream) = mpsc::channel(16);

        tokio::spawn(async move {
            loop {
                let request = match requests.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => return,
                    Err(status) => {
                        let _ = responses.send(Err(status)).await;
                        return;
                    }
                };
                let action = {
                    let mut state = state.lock().expect("fake state");
                    state.subscribe_requests.push(request);
                    state
                        .subscribe_actions
                        .pop_front()
                        .unwrap_or_else(|| SubscribeAction::respond(Vec::new()))
                };
                subscribe_notify.notify_waiters();

                if let Some(gate) = action.gate {
                    gate.notified().await;
                }
                for response in action.responses {
                    if responses.send(response).await.is_err() {
                        return;
                    }
                }
                if action.close_after {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(response_stream)))
    }
}

impl FakeProxyControlPlane {
    fn set_wake_response(&self, response: Result<pb::ProxyWakeInstanceResponse, Status>) {
        self.state.lock().expect("fake state").wake_response = Some(response);
    }

    fn push_subscribe_action(&self, action: SubscribeAction) {
        self.state
            .lock()
            .expect("fake state")
            .subscribe_actions
            .push_back(action);
    }

    fn wake_requests(&self) -> Vec<pb::ProxyWakeInstanceRequest> {
        self.state.lock().expect("fake state").wake_requests.clone()
    }

    fn subscribe_request_count(&self) -> usize {
        self.state
            .lock()
            .expect("fake state")
            .subscribe_requests
            .len()
    }

    async fn wait_for_subscribe_requests(&self, count: usize) -> Vec<pb::ProxySubscribeRequest> {
        let timeout = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(timeout);
        loop {
            let requests = self
                .state
                .lock()
                .expect("fake state")
                .subscribe_requests
                .clone();
            if requests.len() >= count {
                return requests;
            }

            tokio::select! {
                _ = self.subscribe_notify.notified() => {}
                _ = &mut timeout => panic!("timed out waiting for {count} subscribe requests"),
            }
        }
    }
}

#[tonic::async_trait]
impl ProxyControlPlane for FakeHttp01Proxy {
    type WatchTlsCertificatesStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<sleepypods_api::pb::WatchTlsCertificatesResponse, tonic::Status>,
                > + Send,
        >,
    >;
    async fn watch_tls_certificates(
        &self,
        _: tonic::Request<tonic::Streaming<sleepypods_api::pb::WatchTlsCertificatesRequest>>,
    ) -> Result<tonic::Response<Self::WatchTlsCertificatesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "unexpected test certificate watch",
        ))
    }

    type SubscribeStream = ReceiverStream<Result<pb::ProxySubscribeResponse, Status>>;
    async fn wake_instance(
        &self,
        _: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        panic!("HTTP01 must not wake")
    }
    async fn subscribe(
        &self,
        _: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        panic!("HTTP01 must not resolve routes")
    }
    async fn resolve_tls_certificate(
        &self,
        _: Request<pb::ResolveTlsCertificateRequest>,
    ) -> Result<Response<pb::ResolveTlsCertificateResponse>, Status> {
        panic!("HTTP01 must not resolve certificates")
    }
    async fn resolve_http01_challenge(
        &self,
        request: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        let response = {
            let authorization = request.metadata().get("authorization").map(|value| {
                value
                    .to_str()
                    .expect("authorization metadata is ASCII")
                    .to_owned()
            });
            let mut state = self.state.lock().expect("fake state");
            state.resolve_http01_authorizations.push(authorization);
            state.resolve_http01_requests.push(request.into_inner());
            state.resolve_http01_response.take().unwrap_or_else(|| {
                Err(Status::failed_precondition(
                    "test did not configure HTTP-01 resolve response",
                ))
            })
        };

        response.map(Response::new)
    }
}

impl FakeHttp01Proxy {
    fn set_resolve_http01_response(
        &self,
        response: Result<pb::ResolveHttp01ChallengeResponse, Status>,
    ) {
        self.state
            .lock()
            .expect("fake state")
            .resolve_http01_response = Some(response);
    }

    fn resolve_http01_requests(&self) -> Vec<pb::ResolveHttp01ChallengeRequest> {
        self.state
            .lock()
            .expect("fake state")
            .resolve_http01_requests
            .clone()
    }

    fn resolve_http01_authorizations(&self) -> Vec<Option<String>> {
        self.state
            .lock()
            .expect("fake state")
            .resolve_http01_authorizations
            .clone()
    }
}

fn test_client(
    service: FakeProxyControlPlane,
) -> GrpcProxyControlPlaneClient<InProcessService<ProxyControlPlaneServer<FakeProxyControlPlane>>> {
    let server = ProxyControlPlaneServer::new(service);
    GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::new(InProcessService::new(server)))
}

fn test_client_with_subscribe_reconnect_backoff(
    service: FakeProxyControlPlane,
    subscribe_reconnect_backoff: Duration,
) -> GrpcProxyControlPlaneClient<InProcessService<ProxyControlPlaneServer<FakeProxyControlPlane>>> {
    let server = ProxyControlPlaneServer::new(service);
    GrpcProxyControlPlaneClient::with_subscribe_reconnect_backoff(
        ProxyControlPlaneClient::new(InProcessService::new(server)),
        subscribe_reconnect_backoff,
    )
}

fn test_http01_resolver(
    service: FakeHttp01Proxy,
) -> GrpcProxyHttp01Resolver<InProcessService<ProxyControlPlaneServer<FakeHttp01Proxy>>> {
    let server = ProxyControlPlaneServer::new(service);
    GrpcProxyHttp01Resolver::new(ProxyControlPlaneClient::new(InProcessService::new(server)))
}

fn route_resolved_response(request_id: &str, subscription_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
            pb::ProxyRouteResolvedResponse {
                request_id: request_id.to_owned(),
                subscription_id: subscription_id.to_owned(),
                matched_identity: Some(pb_http_identity("app.example.com", None)),
                route: Some(pb_route_entry()),
                cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 10_000 }),
            },
        )),
    }
}

fn route_miss_response(request_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
            pb::ProxyRouteMissResponse {
                request_id: request_id.to_owned(),
                request_identity: Some(pb_http_identity("missing.example.com", None)),
                negative_cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 5_000 }),
            },
        )),
    }
}

fn route_updated_response(subscription_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteUpdated(
            pb::ProxyRouteUpdatedResponse {
                subscription_id: subscription_id.to_owned(),
                matched_identity: Some(pb_http_identity("app.example.com", None)),
                route: Some(pb_route_entry()),
                cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 15_000 }),
            },
        )),
    }
}

fn route_invalidated_response(subscription_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteInvalidated(
            pb::ProxyRouteInvalidatedResponse {
                subscription_id: subscription_id.to_owned(),
                reason: pb::ProxyRouteInvalidationReason::BackendChanged as i32,
            },
        )),
    }
}

fn assert_subscribe_route_request(
    request: &pb::ProxySubscribeRequest,
    expected_request_id: &str,
    expected_host: &str,
) {
    match request.input.as_ref().expect("input") {
        pb::proxy_subscribe_request::Input::SubscribeRoute(request) => {
            assert_eq!(request.request_id, expected_request_id);
            assert_eq!(
                request.identity,
                Some(pb_http_identity(expected_host, None))
            );
        }
        pb::proxy_subscribe_request::Input::Unsubscribe(_) => {
            panic!("expected subscribe route request")
        }
    }
}

fn pb_http_identity(host: &str, path: Option<&str>) -> pb::RouteIdentity {
    pb::RouteIdentity {
        kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
            host: Some(pb::RouteHost {
                kind: pb::RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
            path_prefix: path.map(str::to_owned),
        })),
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

fn http01_challenge(host: &str, token: &str, key_authorization: &str) -> pb::Http01Challenge {
    pb::Http01Challenge {
        key: Some(pb::Http01ChallengeKey {
            host: host.to_owned(),
            token: token.to_owned(),
        }),
        key_authorization: key_authorization.to_owned(),
        expires_at_unix_millis: HTTP01_EXPIRES_AT_UNIX_MILLIS,
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

fn http_identity(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("host"),
        path: path.map(|path| PathPrefix::new(path).expect("path")),
    }
}

fn cache_policy(ttl_millis: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_millis(ttl_millis))
}

fn route_request_id(value: &str) -> RouteRequestId {
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

#[tokio::test]
async fn review_probe_refresh_buffered_invalidation_reaches_production_drain() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![
        Ok(route_invalidated_response("old")),
        Ok(route_resolved_response("first", "first-sub")),
    ]));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "second",
        "second-sub",
    ))]));
    let mut client = test_client(service);
    client
        .subscribe_route(
            route_request_id("first"),
            http_identity("app.example.com", None),
        )
        .await
        .unwrap();
    // A second request used to move queued updates to a buffer that drain never read.
    client
        .subscribe_route(
            route_request_id("second"),
            http_identity("other.example.com", None),
        )
        .await
        .unwrap();
    let events = client.drain_subscription_events().await.unwrap();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], crate::RouteSubscriptionEvent::Update(message) if matches!(&**message, SubscribeControlPlaneOutput::RouteInvalidated {subscription_id: id,..} if id.as_str() == "old" && id.session().is_some()))
    );
}
#[tokio::test]
async fn generated_transport_overflow_fails_pending_route_and_preserves_close_barrier() {
    let service = FakeProxyControlPlane::default();
    let mut responses = (0..300)
        .map(|index| Ok(route_invalidated_response(&format!("sub-{index}"))))
        .collect::<Vec<_>>();
    responses.push(Ok(route_resolved_response("overflow", "overflow-sub")));
    service.push_subscribe_action(SubscribeAction::respond(responses));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "reconnect",
        "new-sub",
    ))]));
    let mut client = test_client(service);
    let failed = tokio::time::timeout(
        Duration::from_secs(1),
        client.subscribe_route(
            route_request_id("overflow"),
            http_identity("app.example.com", None),
        ),
    )
    .await
    .expect("overflow cannot deadlock");
    assert!(failed.is_err());
    client
        .subscribe_route(
            route_request_id("reconnect"),
            http_identity("app.example.com", None),
        )
        .await
        .unwrap();
    let events = client.drain_subscription_events().await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, crate::RouteSubscriptionEvent::StreamClosed)),
        "reconnection cannot erase old cache flush"
    );
}

#[tokio::test]
async fn production_coordinator_recovers_generated_transport_overflow_and_flushes_hot_cache() {
    let service = FakeProxyControlPlane::default();
    let mut burst = (0..300)
        .map(|index| Ok(route_invalidated_response(&format!("unrelated-{index}"))))
        .collect::<Vec<_>>();
    burst.push(Ok(route_miss_response("req:1")));
    service.push_subscribe_action(SubscribeAction::respond(burst));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_miss_response(
        "req:2",
    ))]));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "req:3", "new-hot",
    ))]));
    let mut state = crate::SubscriptionState::new(8);
    state.cache_mut().insert_positive(
        subscription_id("old-hot"),
        http_identity("app.example.com", None),
        route_entry(),
        cache_policy(30_000),
        std::time::Instant::now(),
    );
    let shared = crate::FrontlineRouteCoordinator::new(
        crate::FrontlineRouteResolver::from_parts(state, test_client(service.clone())),
        crate::WakeTracker::new(),
        test_client(service.clone()),
    )
    .into_shared();
    let missing = tokio::time::timeout(
        Duration::from_secs(2),
        shared.route(
            http_identity("missing.example.com", None),
            std::time::Instant::now(),
        ),
    )
    .await
    .expect("overflow recovery bounded")
    .unwrap();
    assert!(matches!(missing, crate::FrontlineRouteOutcome::Miss(_)));
    assert!(matches!(
        shared
            .route(
                http_identity("app.example.com", None),
                std::time::Instant::now()
            )
            .await
            .unwrap(),
        crate::FrontlineRouteOutcome::Ready(_)
    ));
    assert_eq!(
        service.subscribe_request_count(),
        3,
        "hot cache was flushed before reconnect authority was installed"
    );
}

#[tokio::test]
async fn cancelled_subscription_response_is_unsubscribed_without_killing_other_requests() {
    let service = FakeProxyControlPlane::default();
    let gate = Arc::new(Notify::new());
    service.push_subscribe_action(SubscribeAction {
        responses: vec![Ok(route_resolved_response("cancelled", "cancelled-sub"))],
        close_after: false,
        gate: Some(gate.clone()),
    });
    let mut client = test_client(service.clone());
    let request = client.subscribe_route(
        route_request_id("cancelled"),
        http_identity("app.example.com", None),
    );
    let task = tokio::spawn(request);
    service.wait_for_subscribe_requests(1).await;
    task.abort();
    let _ = task.await;
    // Admit B while A is still cancelled but has not received its late reply.
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "next", "next-sub",
    ))]));
    let next = tokio::spawn(client.subscribe_route(
        route_request_id("next"),
        http_identity("app.example.com", None),
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let transport = client.transport.lock().await;
            if transport
                .session
                .as_ref()
                .unwrap()
                .pending
                .lock()
                .await
                .len()
                == 2
            {
                break;
            }
            drop(transport);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("B admitted before late A response");
    gate.notify_one();
    assert!(next.await.unwrap().is_ok(), "late A must not fail B");
    let requests = service.wait_for_subscribe_requests(3).await;
    assert!(requests.iter().any(|message| matches!(
        &message.input,
        Some(pb::proxy_subscribe_request::Input::Unsubscribe(request))
            if request.subscription_id == "cancelled-sub"
    )));
}

#[tokio::test]
async fn deferred_reset_does_not_close_a_session_already_reconnected_by_subscribe() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "old", "old-sub",
    ))]));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "new", "new-sub",
    ))]));
    let mut client = test_client(service);
    client
        .subscribe_route(
            route_request_id("old"),
            http_identity("app.example.com", None),
        )
        .await
        .unwrap();
    let reset = client.reset_subscription();
    let events = client.drain_subscription_events().await.unwrap();
    assert_eq!(events, vec![crate::RouteSubscriptionEvent::StreamClosed]);
    client
        .subscribe_route(
            route_request_id("new"),
            http_identity("app.example.com", None),
        )
        .await
        .unwrap();
    reset.await.unwrap();
    assert!(
        client.transport.lock().await.session.is_some(),
        "an already-consumed reset must not close the replacement session"
    );
    assert!(client.drain_subscription_events().await.unwrap().is_empty());
}

// Protocol mapping assertions compare the unchanged wire identity separately
// from the private session ownership carried by all transport responses.
fn wire_message(mut message: SubscribeControlPlaneOutput) -> SubscribeControlPlaneOutput {
    match &mut message {
        SubscribeControlPlaneOutput::RouteResolved {
            subscription_id, ..
        }
        | SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id, ..
        }
        | SubscribeControlPlaneOutput::RouteInvalidated {
            subscription_id, ..
        } => {
            assert!(
                subscription_id.session().is_some(),
                "transport ID must carry session ownership"
            );
            *subscription_id = SubscriptionId::new(subscription_id.as_str()).unwrap();
        }
        SubscribeControlPlaneOutput::RouteMiss { .. } => {}
    }
    message
}

#[tokio::test]
async fn old_subscription_cleanup_cannot_remove_a_reused_id_on_a_replacement_stream() {
    for create_cleanup_after_reconnect in [false, true] {
        let service = FakeProxyControlPlane::default();
        service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
            "old", "reused",
        ))]));
        service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
            "new", "reused",
        ))]));
        let mut client = test_client(service.clone());
        let identity = http_identity("app.example.com", None);
        let SubscribeControlPlaneOutput::RouteResolved {
            subscription_id: old,
            ..
        } = client
            .subscribe_route(route_request_id("old"), identity.clone())
            .await
            .unwrap()
        else {
            panic!("old resolved")
        };
        // Hold the future entirely unpolled to force cleanup to acquire the
        // transport lock only after B has replaced A and reused the wire ID.
        let cleanup = (!create_cleanup_after_reconnect).then(|| client.unsubscribe(old.clone()));
        client.reset_subscription().await.unwrap();
        client.drain_subscription_events().await.unwrap();
        let new_message = client
            .subscribe_route(route_request_id("new"), identity.clone())
            .await
            .unwrap();
        let SubscribeControlPlaneOutput::RouteResolved {
            subscription_id: new,
            ..
        } = &new_message
        else {
            panic!("new resolved")
        };
        assert_eq!(old.as_str(), new.as_str());
        assert_ne!(
            old, *new,
            "equal wire IDs from different sessions must not alias"
        );
        let new = new.clone();
        let mut state = crate::SubscriptionState::new(4);
        state.apply_resolved_response(identity.clone(), new_message, std::time::Instant::now());
        cleanup
            .unwrap_or_else(|| client.unsubscribe(old))
            .await
            .unwrap();
        assert!(
            client.unsubscribe(subscription_id("reused")).await.is_err(),
            "raw IDs cannot target an unproven session"
        );

        // Ordered on B: a probe asks the fake authority to invalidate B's reused
        // subscription. No cleanup for A may have appeared in B's request stream.
        service.push_subscribe_action(SubscribeAction::respond(vec![
            Ok(route_invalidated_response("reused")),
            Ok(route_miss_response("probe")),
        ]));
        client
            .subscribe_route(
                route_request_id("probe"),
                http_identity("missing.example.com", None),
            )
            .await
            .unwrap();
        let requests = service.wait_for_subscribe_requests(3).await;
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| matches!(
            request.input,
            Some(pb::proxy_subscribe_request::Input::SubscribeRoute(_))
        )));
        let events = client.drain_subscription_events().await.unwrap();
        assert_eq!(events.len(), 1);
        let crate::RouteSubscriptionEvent::Update(message) = events.into_iter().next().unwrap()
        else {
            panic!("invalidation")
        };
        assert!(
            matches!(&*message, SubscribeControlPlaneOutput::RouteInvalidated { subscription_id, .. } if subscription_id == &new)
        );
        state.apply_control_plane_message(*message, std::time::Instant::now());
        assert!(matches!(
            state.cache().lookup(&identity, std::time::Instant::now()),
            crate::CacheLookup::Absent
        ));
    }
}
