use std::{
    collections::VecDeque,
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime},
};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::{
    observability::{
        metrics::RUNTIME_HTTP01_RESULTS_TOTAL_NAME,
        recorder::{InMemoryObservability, ObservabilityEvent, EVENT_HTTP01},
    },
    DrainTracker,
};
use sleepypods_api::{
    BackendEndpoint, Http01ChallengeKey, Http01ChallengeRecord, InstanceState, RouteKind,
};
use tokio::{net::TcpListener, task::JoinHandle};

use super::{
    http01_intercept_error_response, record_http01_response, resolve_http01_response,
    resolve_http_route_shared, route_outcome_or_forward_response, route_resolution_error_response,
    FrontlineRuntimeBody,
};
use crate::{
    route::testing::{entry, running, FakeControlPlane, FakeWake},
    FrontlineForwarder, FrontlineRouteCoordinator, Http01ChallengeResolveFuture,
    Http01ChallengeResolver, SharedFrontlineRouteCoordinator, SubscribeErrorCode,
};

const READY_RESPONSE: &[u8] = b"ready-from-upstream";
const COLD_RESPONSE: &[u8] = b"cold-woke-upstream";

struct Harness {
    control: FakeControlPlane,
    wake: FakeWake,
    shared: SharedFrontlineRouteCoordinator<FakeControlPlane, FakeWake>,
    forwarder: FrontlineForwarder,
}

fn harness() -> Harness {
    let control = FakeControlPlane::default();
    let wake = FakeWake::default();
    Harness {
        shared: FrontlineRouteCoordinator::new(64, control.clone(), wake.clone()).into_shared(),
        forwarder: FrontlineForwarder::new(DrainTracker::new(Duration::from_secs(5))),
        control,
        wake,
    }
}

impl Harness {
    /// Routes and forwards one request the way the listener does once the
    /// HTTP-01 check has passed it on.
    async fn respond(&self, request: Request<Full<Bytes>>) -> Response<FrontlineRuntimeBody> {
        match resolve_http_route_shared(&self.shared, &request, Instant::now()).await {
            Ok(outcome) => {
                route_outcome_or_forward_response(&self.forwarder, outcome, request, None).await
            }
            Err(error) => route_resolution_error_response(error),
        }
    }

    async fn status(&self, host: &str, path: &str) -> StatusCode {
        self.respond(get(Some(host), path)).await.status()
    }
}

fn get(host: Option<&str>, path: &str) -> Request<Full<Bytes>> {
    let mut request = Request::builder().uri(path);
    if let Some(host) = host {
        request = request.header("host", host);
    }
    request
        .body(Full::new(Bytes::new()))
        .expect("request builds")
}

fn running_at(instance: &str, backend: String) -> sleepypods_api::RouteEntry {
    let mut entry = running(instance);
    entry.backend = Some(BackendEndpoint::new(backend).expect("backend"));
    entry
}

#[tokio::test]
async fn a_ready_route_forwards_the_request_to_its_backend() {
    let (upstream, upstream_task) =
        spawn_http_upstream(2, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let h = harness();
    h.control.set_http(
        "app.example.com",
        running_at("app", format!("http://{upstream}")),
    );

    for path in ["/ready?via=runtime", "/another/path"] {
        let response = h
            .respond(
                Request::builder()
                    .method(Method::PUT)
                    .uri(path)
                    .header("host", "App.Example.COM")
                    .body(Full::new(Bytes::from_static(b"request-body")))
                    .expect("request builds"),
            )
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            collect_body(response).await,
            Bytes::from_static(READY_RESPONSE)
        );
    }

    assert_eq!(h.control.subscribes(), 1);
    assert!(h.wake.calls().is_empty());
    drop(h);
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn a_request_without_a_usable_host_is_a_bad_request_and_costs_no_lookup() {
    let h = harness();

    assert_eq!(
        h.respond(get(None, "/")).await.status(),
        StatusCode::BAD_REQUEST
    );
    for host in ["localhost", " ", "app.example.com:http", "[::1]:8080"] {
        assert_eq!(h.status(host, "/").await, StatusCode::BAD_REQUEST, "{host}");
    }
    let long_host = format!("{}.example.com", "a".repeat(250));
    assert_eq!(h.status(&long_host, "/").await, StatusCode::BAD_REQUEST);

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(h.control.subscribes(), 0);
}

#[tokio::test]
async fn an_oversized_path_is_uri_too_long_and_costs_no_lookup() {
    let h = harness();
    h.control.set_http("app.example.com", running("app"));
    let at_limit = format!("/{}", "a".repeat(4095));
    let over_limit = format!("/{}", "a".repeat(4096));

    assert_eq!(
        h.status("unseen.example.com", &over_limit).await,
        StatusCode::URI_TOO_LONG
    );
    assert_eq!(
        h.status("app.example.com", &over_limit).await,
        StatusCode::URI_TOO_LONG
    );
    // A query string is not part of the routed path.
    let long_query = format!("/?{}", "q".repeat(8000));
    assert_ne!(
        h.status("app.example.com", &long_query).await,
        StatusCode::URI_TOO_LONG
    );
    assert_ne!(
        h.status("app.example.com", &at_limit).await,
        StatusCode::URI_TOO_LONG
    );

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        h.control.subscribes(),
        1,
        "only the routable host was looked up"
    );
    assert_eq!(h.control.connects(), 1);
}

#[tokio::test]
async fn a_host_without_a_route_is_not_found() {
    let h = harness();

    assert_eq!(
        h.status("missing.example.com", "/").await,
        StatusCode::NOT_FOUND
    );
    assert!(h.wake.calls().is_empty());
}

#[tokio::test]
async fn a_cold_route_is_woken_and_forwarded_once_its_instance_runs() {
    let (upstream, upstream_task) = spawn_http_upstream(2, StatusCode::OK, COLD_RESPONSE).await;
    let h = Arc::new(harness());
    h.control
        .set_http("cold.example.com", entry("cold", InstanceState::Cold, 4));

    let first = {
        let h = h.clone();
        tokio::spawn(async move { h.respond(get(Some("cold.example.com"), "/cold")).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first.is_finished());
    assert_eq!(h.wake.calls().len(), 1);
    assert_eq!(h.wake.calls()[0].expected_generation.get(), 4);

    let mut ready = running_at("cold", format!("http://{upstream}"));
    ready.instance_generation = sleepypods_api::Generation::new(5);
    h.control.set_http("cold.example.com", ready);

    let first = first.await.expect("request task joins");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(collect_body(first).await, Bytes::from_static(COLD_RESPONSE));
    let second = h.respond(get(Some("cold.example.com"), "/cold")).await;
    assert_eq!(
        collect_body(second).await,
        Bytes::from_static(COLD_RESPONSE)
    );
    assert_eq!(h.control.subscribes(), 1);
    assert_eq!(h.wake.calls().len(), 1);

    drop(h);
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn an_instance_being_deleted_and_a_failed_lookup_are_service_unavailable() {
    let h = harness();
    h.control.set_http(
        "deleted.example.com",
        entry("gone", InstanceState::Deleted, 3),
    );
    h.control.fail_host(
        RouteKind::Http,
        "broken.example.com",
        SubscribeErrorCode::Unavailable,
    );

    assert_eq!(
        h.status("deleted.example.com", "/").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        h.status("broken.example.com", "/").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(h.wake.calls().is_empty());
}

#[tokio::test]
async fn a_backend_the_proxy_cannot_reach_is_a_bad_gateway() {
    let h = harness();
    h.control.set_http(
        "bad-backend.example.com",
        running_at("bad", "ftp://127.0.0.1:1".to_owned()),
    );

    assert_eq!(
        h.status("bad-backend.example.com", "/").await,
        StatusCode::BAD_GATEWAY
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestHttp01ResolverError;

#[derive(Default)]
struct FakeHttp01Resolver {
    calls: Vec<(String, String)>,
    responses: VecDeque<Result<Option<Http01ChallengeRecord>, TestHttp01ResolverError>>,
}

impl Http01ChallengeResolver for FakeHttp01Resolver {
    type Error = TestHttp01ResolverError;

    fn resolve_http01_challenge(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Http01ChallengeResolveFuture<'_, Self::Error> {
        self.calls
            .push((key.host().as_str().to_owned(), key.token().to_owned()));
        let response = self.responses.pop_front().expect("queued HTTP-01 response");
        Box::pin(async move { response })
    }
}

async fn http01_status(
    resolver: &mut FakeHttp01Resolver,
    host: &str,
    path: &str,
) -> (StatusCode, Bytes) {
    let response = match resolve_http01_response(resolver, &get(Some(host), path)).await {
        Ok(Some(response)) => response,
        Ok(None) => panic!("an HTTP-01 path is answered by the challenge resolver"),
        Err(error) => http01_intercept_error_response(error),
    };
    (response.status(), collect_body(response).await)
}

#[tokio::test]
async fn an_http01_challenge_is_answered_with_its_key_authorization() {
    let key = Http01ChallengeKey::new("app.example.com", "token-a").expect("HTTP-01 key");
    let mut resolver = FakeHttp01Resolver::default();
    resolver
        .responses
        .push_back(Ok(Some(challenge_record(key, "token-a.key"))));

    let (status, body) = http01_status(
        &mut resolver,
        "App.Example.COM:80",
        "/.well-known/acme-challenge/token-a",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from_static(b"token-a.key"));
    assert_eq!(
        resolver.calls,
        vec![("app.example.com".to_owned(), "token-a".to_owned())]
    );
}

#[tokio::test]
async fn an_unknown_http01_token_is_not_found() {
    let mut resolver = FakeHttp01Resolver::default();
    resolver.responses.push_back(Ok(None));

    let (status, _) = http01_status(
        &mut resolver,
        "app.example.com",
        "/.well-known/acme-challenge/missing-token",
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        resolver.calls,
        vec![("app.example.com".to_owned(), "missing-token".to_owned())]
    );
}

#[tokio::test]
async fn malformed_http01_requests_are_refused_and_resolver_errors_are_unavailable() {
    let mut resolver = FakeHttp01Resolver::default();
    for (host, path) in [
        ("localhost", "/.well-known/acme-challenge/token-a"),
        (
            "app.example.com",
            "/.well-known/acme-challenge/token-a/extra",
        ),
    ] {
        let (status, _) = http01_status(&mut resolver, host, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{host}{path}");
    }
    assert!(resolver.calls.is_empty());

    resolver.responses.push_back(Err(TestHttp01ResolverError));
    let (status, _) = http01_status(
        &mut resolver,
        "app.example.com",
        "/.well-known/acme-challenge/token-a",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn an_http01_answer_records_a_result_metric_and_a_log_event() {
    let sink = InMemoryObservability::default();

    record_http01_response(&sink.recorder(), StatusCode::OK);

    let events = sink.events();
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_HTTP01_RESULTS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "success")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log) if log.name() == EVENT_HTTP01
    )));
}

async fn spawn_http_upstream(
    requests: usize,
    status: StatusCode,
    body: &'static [u8],
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let addr = listener.local_addr().expect("upstream addr");
    let remaining = Arc::new(AtomicUsize::new(requests));
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts");
        let service_remaining = Arc::clone(&remaining);
        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let remaining = Arc::clone(&service_remaining);
                    async move {
                        let previous = remaining.fetch_sub(1, Ordering::AcqRel);
                        assert!(previous > 0, "upstream received too many requests");
                        let _ = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("request body reads");
                        let mut response = Response::builder().status(status);
                        if previous == 1 {
                            response = response.header("connection", "close");
                        }

                        Ok::<_, Infallible>(
                            response
                                .body(Full::new(Bytes::from_static(body)))
                                .expect("response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves");
        assert_eq!(remaining.load(Ordering::Acquire), 0);
    });

    (addr, task)
}

async fn collect_body(response: Response<FrontlineRuntimeBody>) -> Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("response body reads")
        .to_bytes()
}

fn challenge_record(key: Http01ChallengeKey, key_authorization: &str) -> Http01ChallengeRecord {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    Http01ChallengeRecord::new(key, key_authorization, now + Duration::from_secs(60), now)
        .expect("challenge record builds")
}
