use std::{convert::Infallible, time::Duration};

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode, Version};
use http_body_util::{channel::Channel, BodyExt, Empty, Full};
use hyper::{
    body::Incoming,
    server::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use proxy_core::DrainTracker;
use sleepypods_api::{BackendEndpoint, BackendGeneration, Generation, InstanceId};
use tokio::net::TcpListener;

use crate::{
    forward::{
        http_upstream_origin, BackendForwardError, FrontlineForwardError, FrontlineForwarder,
    },
    ReadyBackend,
};

const REQUEST_BODY: &[u8] = b"frontline request body";
const RESPONSE_BODY: &[u8] = b"frontline response body";

#[tokio::test]
async fn frontline_http1_forwards_ready_backend_request_and_response() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    let ready = ready_backend(format!("http://{upstream_addr}"));

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener.accept().await.expect("upstream accepts");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|mut request: Request<Incoming>| async move {
                    assert_eq!(request.version(), Version::HTTP_11);
                    assert_eq!(request.method(), Method::PUT);
                    assert_eq!(
                        request.uri().path_and_query().expect("path query").as_str(),
                        "/v1/items?preserve=true"
                    );
                    assert_eq!(
                        request.headers().get("host").expect("host header"),
                        "public.example.test"
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("x-forwarded")
                            .expect("forwarded header"),
                        "kept"
                    );
                    assert!(request.headers().get("connection").is_none());
                    assert!(request.headers().get("x-remove").is_none());

                    let body = request
                        .body_mut()
                        .collect()
                        .await
                        .expect("request body reads")
                        .to_bytes();
                    assert_eq!(body, Bytes::from_static(REQUEST_BODY));

                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::ACCEPTED)
                            .header("x-upstream", "ok")
                            .header("connection", "x-response-remove")
                            .header("x-response-remove", "drop")
                            .body(Full::new(Bytes::from_static(RESPONSE_BODY)))
                            .expect("response builds"),
                    )
                }),
            )
            .await
            .expect("upstream serves");
    });

    let forwarder = FrontlineForwarder::new(DrainTracker::new(Duration::from_secs(5)));
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/v1/items?preserve=true")
        .header("host", "public.example.test")
        .header("x-forwarded", "kept")
        .header("connection", "x-remove")
        .header("x-remove", "drop")
        .body(Full::new(Bytes::from_static(REQUEST_BODY)))
        .expect("request builds");

    let response = forwarder
        .forward_http(&ready, request)
        .await
        .expect("request forwards");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers().get("x-upstream").expect("header"), "ok");
    assert!(response.headers().get("x-response-remove").is_none());
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body reads")
        .to_bytes();
    assert_eq!(body, Bytes::from_static(RESPONSE_BODY));

    drop(forwarder);
    upstream_task.await.expect("upstream task completes");
}

#[tokio::test]
async fn http_forwarding_rejects_invalid_backend_before_connecting() {
    let forwarder = FrontlineForwarder::new(DrainTracker::new(Duration::from_secs(5)));
    let request = Request::builder()
        .uri("/")
        .body(Empty::<Bytes>::new())
        .expect("request builds");

    let error = forwarder
        .forward_http(&ready_backend("ftp://127.0.0.1:1"), request)
        .await
        .expect_err("unsupported scheme rejects");

    assert!(matches!(
        error,
        FrontlineForwardError::Backend(BackendForwardError::UnsupportedHttpScheme(scheme))
            if scheme == "ftp"
    ));

    assert!(matches!(
        http_upstream_origin(&ready_backend("/missing-scheme")),
        Err(BackendForwardError::MissingScheme)
    ));
    assert!(matches!(
        http_upstream_origin(&ready_backend("http://[::1")),
        Err(BackendForwardError::InvalidUri(_))
    ));
}

#[tokio::test]
async fn frontline_http2_cleartext_forwards_body_to_h2_upstream() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    let ready = ready_backend(format!("http://{upstream_addr}"));

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener.accept().await.expect("upstream accepts");

        http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|mut request: Request<Incoming>| async move {
                    assert_eq!(request.version(), Version::HTTP_2);
                    assert_eq!(request.method(), Method::POST);
                    assert_eq!(
                        request.uri().path_and_query().expect("path query").as_str(),
                        "/h2/items"
                    );

                    let body = request
                        .body_mut()
                        .collect()
                        .await
                        .expect("request body reads")
                        .to_bytes();
                    assert_eq!(body, Bytes::from_static(REQUEST_BODY));

                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from_static(RESPONSE_BODY)))
                            .expect("response builds"),
                    )
                }),
            )
            .await
            .expect("upstream serves h2");
    });

    let forwarder = FrontlineForwarder::new(DrainTracker::new(Duration::from_secs(5)));
    let request = Request::builder()
        .version(Version::HTTP_2)
        .method(Method::POST)
        .uri("/h2/items")
        .body(Full::new(Bytes::from_static(REQUEST_BODY)))
        .expect("request builds");

    let response = forwarder
        .forward_http(&ready, request)
        .await
        .expect("request forwards over h2c");

    assert_eq!(response.version(), Version::HTTP_2);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body reads")
        .to_bytes();
    assert_eq!(body, Bytes::from_static(RESPONSE_BODY));

    drop(forwarder);
    upstream_task.await.expect("upstream task completes");
}

#[tokio::test]
async fn frontline_h2c_grpc_shaped_forwarding_preserves_body_and_trailers() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    let ready = ready_backend(format!("http://{upstream_addr}"));
    let grpc_response = Bytes::from_static(b"\0\0\0\0\x05hello");

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener.accept().await.expect("upstream accepts");

        http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let grpc_response = grpc_response.clone();

                    async move {
                        assert_eq!(request.version(), Version::HTTP_2);
                        assert_eq!(
                            request
                                .headers()
                                .get("content-type")
                                .expect("grpc content-type"),
                            "application/grpc"
                        );
                        let body = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("request body reads")
                            .to_bytes();
                        assert_eq!(body, Bytes::from_static(b"\0\0\0\0\x05world"));

                        let (mut sender, body) = Channel::<Bytes, Infallible>::new(2);
                        tokio::spawn(async move {
                            sender
                                .send_data(grpc_response)
                                .await
                                .expect("sends grpc response body");
                            let mut trailers = HeaderMap::new();
                            trailers.insert("grpc-status", "0".parse().expect("status header"));
                            trailers.insert("grpc-message", "ok".parse().expect("message header"));
                            sender
                                .send_trailers(trailers)
                                .await
                                .expect("sends grpc trailers");
                        });

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/grpc")
                                .body(body)
                                .expect("response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves h2");
    });

    let forwarder = FrontlineForwarder::new(DrainTracker::new(Duration::from_secs(5)));
    let request = Request::builder()
        .version(Version::HTTP_2)
        .method(Method::POST)
        .uri("/grpc.Test/Echo")
        .header("content-type", "application/grpc")
        .body(Full::new(Bytes::from_static(b"\0\0\0\0\x05world")))
        .expect("request builds");

    let response = forwarder
        .forward_http(&ready, request)
        .await
        .expect("request forwards over h2c");
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content-type"),
        "application/grpc"
    );

    let collected = response
        .into_body()
        .collect()
        .await
        .expect("response body and trailers read");
    let trailers = collected
        .trailers()
        .cloned()
        .expect("grpc trailers forwarded");
    assert_eq!(
        collected.to_bytes(),
        Bytes::from_static(b"\0\0\0\0\x05hello")
    );
    assert_eq!(trailers.get("grpc-status").expect("grpc-status"), "0");
    assert_eq!(trailers.get("grpc-message").expect("grpc-message"), "ok");

    drop(forwarder);
    upstream_task.await.expect("upstream task completes");
}

fn ready_backend(uri: impl Into<String>) -> ReadyBackend {
    ReadyBackend {
        instance_id: InstanceId::new("instance-a").expect("instance id"),
        instance_generation: Generation::new(7),
        backend: BackendEndpoint::new(uri).expect("backend endpoint"),
        backend_generation: Some(BackendGeneration::new(11)),
    }
}
