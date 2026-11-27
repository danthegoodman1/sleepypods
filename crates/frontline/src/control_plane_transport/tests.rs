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
    BackendEndpoint, BackendGeneration, BearerToken, Generation, Http01ChallengeKey, InstanceId,
    OptionalBearerTokenInterceptor, RouteHost, RouteKind,
};
use tokio::sync::{mpsc, Notify};
use tonic::{
    codegen::{http, tokio_stream::wrappers::ReceiverStream, Service},
    Request, Response, Status,
};

use super::{GrpcProxyControlPlaneClient, GrpcProxyHttp01Resolver, GrpcProxyHttp01ResolverError};
use crate::{
    Http01ChallengeResolver, RouteStreamEvent, RouteStreamReply, RouteStreamRequest,
    RouteSubscriptionClient, SubscribeErrorCode, WakeClient, WakeInstanceRequest,
    WakeInstanceResponse,
};

const HTTP01_EXPIRES_AT_UNIX_MILLIS: i64 = 2_000;

#[tokio::test]
async fn wake_instance_ready_response_maps_through_generated_client() {
    let service = FakeProxyControlPlane::default();
    service.set_wake_response(Ok(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
            pb::ProxyWakeReadyResult {
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

fn subscribe(id: u64, host: &str) -> RouteStreamRequest {
    RouteStreamRequest::Subscribe {
        id,
        kind: RouteKind::Http,
        host: RouteHost::exact(host).expect("valid host"),
    }
}

fn rules(id: u64, instance: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::Rules(
            pb::ProxyHostRulesResponse {
                subscription_id: id,
                rules: vec![pb::ProxyRouteRule {
                    identity: Some(pb::RouteIdentity {
                        kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                            host: Some(pb::RouteHost {
                                kind: pb::RouteHostKind::Exact as i32,
                                host: "app.example.com".to_owned(),
                            }),
                            path_prefix: None,
                        })),
                    }),
                    route: Some(pb::ProxyRouteEntry {
                        route_binding_id: "route-a".to_owned(),
                        instance_id: instance.to_owned(),
                        instance_state: pb::InstanceState::Running as i32,
                        instance_generation: 7,
                        backend_uri: Some("http://10.0.0.7:8080".to_owned()),
                        pod_backend_uri: None,
                        backend_generation: Some(3),
                    }),
                }],
                cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 90_000 }),
            },
        )),
    }
}

fn subscription_error(id: u64) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::Error(
            pb::ProxySubscribeErrorResponse {
                subscription_id: id,
                code: pb::ProxySubscribeErrorCode::Unavailable as i32,
                message: "lookup failed".to_owned(),
            },
        )),
    }
}

async fn next(stream: &mut crate::RouteStream) -> Option<RouteStreamEvent> {
    tokio::time::timeout(Duration::from_secs(2), stream.events.recv())
        .await
        .expect("the stream delivers an event")
}

fn reply_instance(event: Option<RouteStreamEvent>) -> (u64, String) {
    match event {
        Some(RouteStreamEvent::Reply(RouteStreamReply::Rules { id, rules, .. })) => {
            (id, rules[0].entry.instance_id.as_str().to_owned())
        }
        other => panic!("expected a rule set, got {other:?}"),
    }
}

#[tokio::test]
async fn a_stream_carries_requests_and_replies_in_order() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(rules(1, "first"))]));
    service.push_subscribe_action(SubscribeAction::respond(Vec::new()));
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(rules(2, "second"))]));
    let mut stream = test_client(service.clone())
        .connect()
        .await
        .expect("the stream opens");

    stream
        .requests
        .send(subscribe(1, "app.example.com"))
        .await
        .unwrap();
    stream
        .requests
        .send(RouteStreamRequest::Unsubscribe { id: 1 })
        .await
        .unwrap();
    stream
        .requests
        .send(subscribe(2, "App.Example.com"))
        .await
        .unwrap();

    assert_eq!(
        reply_instance(next(&mut stream).await),
        (1, "first".to_owned())
    );
    assert_eq!(
        reply_instance(next(&mut stream).await),
        (2, "second".to_owned())
    );
    let requests = service.wait_for_subscribe_requests(3).await;
    assert_eq!(
        requests
            .into_iter()
            .map(|request| request.input.expect("input"))
            .collect::<Vec<_>>(),
        vec![
            pb::proxy_subscribe_request::Input::Subscribe(pb::ProxySubscribeHostRequest {
                subscription_id: 1,
                kind: pb::RouteKind::Http as i32,
                host: "app.example.com".to_owned(),
            }),
            pb::proxy_subscribe_request::Input::Unsubscribe(pb::ProxyUnsubscribeRequest {
                subscription_id: 1,
            }),
            pb::proxy_subscribe_request::Input::Subscribe(pb::ProxySubscribeHostRequest {
                subscription_id: 2,
                kind: pb::RouteKind::Http as i32,
                host: "app.example.com".to_owned(),
            }),
        ]
    );
}

#[tokio::test]
async fn a_rule_set_its_replacement_and_an_error_arrive_as_sent() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![
        Ok(rules(1, "resolved")),
        Ok(rules(1, "changed")),
        Ok(subscription_error(1)),
    ]));
    let mut stream = test_client(service)
        .connect()
        .await
        .expect("the stream opens");
    stream
        .requests
        .send(subscribe(1, "app.example.com"))
        .await
        .unwrap();

    assert_eq!(
        reply_instance(next(&mut stream).await),
        (1, "resolved".to_owned())
    );
    assert_eq!(
        reply_instance(next(&mut stream).await),
        (1, "changed".to_owned())
    );
    assert_eq!(
        next(&mut stream).await,
        Some(RouteStreamEvent::Reply(RouteStreamReply::Error {
            id: 1,
            code: SubscribeErrorCode::Unavailable,
            message: "lookup failed".to_owned(),
        }))
    );
}

#[tokio::test]
async fn an_orderly_end_follows_every_message_the_control_plane_sent() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(vec![Ok(rules(1, "last"))]));
    let mut stream = test_client(service)
        .connect()
        .await
        .expect("the stream opens");
    stream
        .requests
        .send(subscribe(1, "app.example.com"))
        .await
        .unwrap();

    assert_eq!(
        reply_instance(next(&mut stream).await),
        (1, "last".to_owned())
    );
    assert_eq!(next(&mut stream).await, Some(RouteStreamEvent::Ended));
    assert_eq!(next(&mut stream).await, None);
}

#[tokio::test]
async fn a_status_error_fails_the_stream_after_the_messages_before_it() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(vec![
        Ok(rules(1, "delivered")),
        Err(Status::data_loss("route change events were lost")),
    ]));
    let mut stream = test_client(service)
        .connect()
        .await
        .expect("the stream opens");
    stream
        .requests
        .send(subscribe(1, "app.example.com"))
        .await
        .unwrap();

    assert_eq!(
        reply_instance(next(&mut stream).await),
        (1, "delivered".to_owned())
    );
    assert_eq!(next(&mut stream).await, Some(RouteStreamEvent::Failed));
    assert_eq!(next(&mut stream).await, None);
}

#[tokio::test]
async fn a_message_the_proxy_cannot_read_fails_the_stream() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![
        Ok(pb::ProxySubscribeResponse { output: None }),
        Ok(rules(1, "never read")),
    ]));
    let mut stream = test_client(service)
        .connect()
        .await
        .expect("the stream opens");
    stream
        .requests
        .send(subscribe(1, "app.example.com"))
        .await
        .unwrap();

    assert_eq!(next(&mut stream).await, Some(RouteStreamEvent::Failed));
    assert_eq!(next(&mut stream).await, None);
}

#[tokio::test]
async fn dropping_the_stream_closes_the_call() {
    let service = FakeProxyControlPlane::default();
    let stream = test_client(service.clone())
        .connect()
        .await
        .expect("the stream opens");
    assert_eq!(service.closed_streams(), 0);

    drop(stream);

    tokio::time::timeout(Duration::from_secs(2), async {
        while service.closed_streams() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the control plane sees the stream close");
}

#[tokio::test]
async fn a_refused_stream_is_a_connect_error() {
    let service = FakeProxyControlPlane::default();
    service.refuse_streams(Status::resource_exhausted(
        "subscription stream capacity exhausted",
    ));

    let error = test_client(service)
        .connect()
        .await
        .expect_err("the stream is refused");

    assert!(matches!(
        error,
        super::GrpcProxyControlPlaneError::Status(status)
            if status.code() == tonic::Code::ResourceExhausted
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
    refusal: Option<Status>,
    closed_streams: usize,
}

#[derive(Default)]
struct FakeHttp01ProxyState {
    resolve_http01_response: Option<Result<pb::ResolveHttp01ChallengeResponse, Status>>,
    resolve_http01_requests: Vec<pb::ResolveHttp01ChallengeRequest>,
    resolve_http01_authorizations: Vec<Option<String>>,
}

/// What the fake sends when it receives its next request.
struct SubscribeAction {
    responses: Vec<Result<pb::ProxySubscribeResponse, Status>>,
    close_after: bool,
}

impl SubscribeAction {
    fn respond(responses: Vec<Result<pb::ProxySubscribeResponse, Status>>) -> Self {
        Self {
            responses,
            close_after: false,
        }
    }

    fn close_after(responses: Vec<Result<pb::ProxySubscribeResponse, Status>>) -> Self {
        Self {
            responses,
            close_after: true,
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
        if let Some(status) = self.state.lock().expect("fake state").refusal.clone() {
            return Err(status);
        }
        let mut requests = request.into_inner();
        let state = Arc::clone(&self.state);
        let subscribe_notify = Arc::clone(&self.subscribe_notify);
        let (responses, response_stream) = mpsc::channel(16);

        tokio::spawn(async move {
            loop {
                let request = match requests.message().await {
                    Ok(Some(request)) => request,
                    _ => {
                        state.lock().expect("fake state").closed_streams += 1;
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

    fn refuse_streams(&self, status: Status) {
        self.state.lock().expect("fake state").refusal = Some(status);
    }

    fn closed_streams(&self) -> usize {
        self.state.lock().expect("fake state").closed_streams
    }

    fn wake_requests(&self) -> Vec<pb::ProxyWakeInstanceRequest> {
        self.state.lock().expect("fake state").wake_requests.clone()
    }

    async fn wait_for_subscribe_requests(&self, count: usize) -> Vec<pb::ProxySubscribeRequest> {
        let timeout = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(timeout);
        loop {
            let notified = self.subscribe_notify.notified();
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
                _ = notified => {}
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

fn test_http01_resolver(
    service: FakeHttp01Proxy,
) -> GrpcProxyHttp01Resolver<InProcessService<ProxyControlPlaneServer<FakeHttp01Proxy>>> {
    let server = ProxyControlPlaneServer::new(service);
    GrpcProxyHttp01Resolver::new(ProxyControlPlaneClient::new(InProcessService::new(server)))
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

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

fn backend(value: &str) -> BackendEndpoint {
    BackendEndpoint::new(value).expect("backend")
}
