mod support;

use std::{
    convert::Infallible,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, StatusCode, Uri};
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Empty, Full};
use hyper::{
    body::Incoming,
    server::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use proxy_core::{DrainError, DrainTracker, HttpProxy, HttpProxyError};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch, Notify},
    time::timeout,
};

const REQUEST_BODY: &[u8] = b"preserve this http request body";
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn http_proxy_preserves_basic_request_response_bytes_and_tracks_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");

    let (request_received_tx, request_received_rx) = oneshot::channel();
    let (release_response_tx, release_response_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let request_received_tx = Arc::new(Mutex::new(Some(request_received_tx)));
        let release_response_rx = Arc::new(Mutex::new(Some(release_response_rx)));

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let request_received_tx = Arc::clone(&request_received_tx);
                    let release_response_rx = Arc::clone(&release_response_rx);

                    async move {
                        let request_received_tx = request_received_tx
                            .lock()
                            .expect("request channel lock is not poisoned")
                            .take();
                        let release_response_rx = release_response_rx
                            .lock()
                            .expect("release channel lock is not poisoned")
                            .take();

                        assert_eq!(request.method(), "POST");
                        assert_eq!(
                            request
                                .uri()
                                .path_and_query()
                                .expect("path and query present")
                                .as_str(),
                            "/v1/items?preserve=true"
                        );
                        assert_eq!(
                            request.headers().get("host").expect("host forwarded"),
                            "public.example.test"
                        );
                        assert_eq!(
                            request
                                .headers()
                                .get("x-preserve")
                                .expect("header forwarded"),
                            "yes"
                        );
                        assert!(request.headers().get("connection").is_none());
                        assert!(request.headers().get("x-remove").is_none());

                        let body = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("upstream reads request body")
                            .to_bytes();
                        assert_eq!(body, Bytes::from_static(REQUEST_BODY));

                        if let Some(tx) = request_received_tx {
                            tx.send(()).expect("test waits for request receipt");
                        }
                        if let Some(rx) = release_response_rx {
                            rx.await.expect("test releases upstream response");
                        }

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::CREATED)
                                .header("x-upstream", "ok")
                                .header("connection", "x-response-remove")
                                .header("x-response-remove", "drop")
                                .body(Full::new(Bytes::from_static(REQUEST_BODY)))
                                .expect("response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves request");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves request");
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(
            format!(
                "POST /v1/items?preserve=true HTTP/1.1\r\n\
                 Host: public.example.test\r\n\
                 Content-Length: {}\r\n\
                 X-Preserve: yes\r\n\
                 Connection: close, x-remove\r\n\
                 X-Remove: no\r\n\
                 \r\n",
                REQUEST_BODY.len()
            )
            .as_bytes(),
        )
        .await
        .expect("client writes headers");
    client
        .write_all(REQUEST_BODY)
        .await
        .expect("client writes body");

    request_received_rx
        .await
        .expect("upstream received proxied request");
    assert_eq!(drain.active_count(), 1);

    release_response_tx
        .send(())
        .expect("upstream response released");

    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("client reads response");
    let (headers, body) = split_http_response(&response);
    assert!(headers.starts_with("HTTP/1.1 201 Created"));
    assert!(headers.contains("x-upstream: ok"));
    assert!(!headers.contains("x-response-remove"));
    assert_eq!(body, REQUEST_BODY);

    proxy_task.await.expect("proxy task completed");
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_serves_two_http1_keep_alive_requests_on_one_connection() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");

    let upstream_task = tokio::spawn(async move {
        loop {
            let (stream, _) = upstream_listener
                .accept()
                .await
                .expect("upstream accepts proxy connection");

            tokio::spawn(async move {
                http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(|request: Request<Incoming>| async move {
                            let body = match request.uri().path() {
                                "/first" => Bytes::from_static(b"one"),
                                "/second" => Bytes::from_static(b"two"),
                                other => Bytes::from(format!("unexpected path {other}")),
                            };

                            Ok::<_, Infallible>(
                                Response::builder()
                                    .header("content-length", body.len().to_string())
                                    .body(Full::new(body))
                                    .expect("response builds"),
                            )
                        }),
                    )
                    .await
                    .ok();
            });
        }
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves keep-alive requests");
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(
            b"GET /first HTTP/1.1\r\nHost: public.example.test\r\n\r\n\
              GET /second HTTP/1.1\r\nHost: public.example.test\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("client writes pipelined keep-alive requests");

    let (first_headers, first_body) = read_http_response(&mut client).await;
    assert!(first_headers.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(first_body, b"one");

    let (second_headers, second_body) = read_http_response(&mut client).await;
    assert!(second_headers.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(second_body, b"two");

    proxy_task.await.expect("proxy task completed");
    upstream_task.abort();
    let _ = upstream_task.await;
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_preserves_chunked_request_body() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");
    let (request_received_tx, request_received_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let request_received_tx = Arc::new(Mutex::new(Some(request_received_tx)));

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let request_received_tx = Arc::clone(&request_received_tx);

                    async move {
                        assert_eq!(request.uri().path(), "/chunked");
                        let body = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("upstream reads chunked request")
                            .to_bytes();
                        assert_eq!(body, Bytes::from_static(b"MozillaDeveloperNetwork"));

                        if let Some(tx) = request_received_tx
                            .lock()
                            .expect("request channel lock is not poisoned")
                            .take()
                        {
                            tx.send(()).expect("test waits for request receipt");
                        }

                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                    }
                }),
            )
            .await
            .expect("upstream serves chunked request");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves chunked request");
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(
            b"POST /chunked HTTP/1.1\r\n\
              Host: public.example.test\r\n\
              Transfer-Encoding: chunked\r\n\
              Connection: close\r\n\
              \r\n\
              7\r\nMozilla\r\n\
              9\r\nDeveloper\r\n\
              7\r\nNetwork\r\n\
              0\r\n\r\n",
        )
        .await
        .expect("client writes chunked request");

    request_received_rx
        .await
        .expect("upstream received chunked request");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("client reads response");
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200 OK"));

    proxy_task.await.expect("proxy task completed");
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_preserves_large_request_and_response_bodies() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");
    let large_body = Bytes::from(
        (0..512 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let expected_body = large_body.clone();

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let expected_body = expected_body.clone();

                    async move {
                        assert_eq!(request.uri().path(), "/large");
                        let body = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("upstream reads large request")
                            .to_bytes();
                        assert_eq!(body, expected_body);

                        Ok::<_, Infallible>(
                            Response::builder()
                                .header("content-length", body.len().to_string())
                                .body(Full::new(body))
                                .expect("large response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves large request");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves large request");
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(
            format!(
                "POST /large HTTP/1.1\r\n\
                 Host: public.example.test\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n",
                large_body.len()
            )
            .as_bytes(),
        )
        .await
        .expect("client writes large request headers");
    client
        .write_all(&large_body)
        .await
        .expect("client writes large request body");

    let mut response = Vec::new();
    expect_within(
        client.read_to_end(&mut response),
        "client reads large response",
    )
    .await
    .expect("client reads large response");
    let (headers, body) = split_http_response(&response);
    assert!(headers.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(body, large_body.as_ref());

    proxy_task.await.expect("proxy task completed");
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_streams_request_body_before_client_finishes_sending() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");
    let (first_chunk_tx, first_chunk_rx) = oneshot::channel();
    let first_chunk_tx = Arc::new(Mutex::new(Some(first_chunk_tx)));

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let first_chunk_tx = Arc::clone(&first_chunk_tx);

                    async move {
                        assert_eq!(request.uri().path(), "/stream-request");
                        let first = request
                            .body_mut()
                            .frame()
                            .await
                            .expect("upstream receives first request frame")
                            .expect("first request frame is valid")
                            .into_data()
                            .expect("first request frame is data");
                        assert_eq!(first, Bytes::from_static(b"first chunk"));

                        if let Some(tx) = first_chunk_tx
                            .lock()
                            .expect("first chunk channel lock is not poisoned")
                            .take()
                        {
                            tx.send(()).expect("test waits for streamed first chunk");
                        }

                        let tail = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("upstream reads request tail")
                            .to_bytes();
                        assert_eq!(tail, Bytes::from_static(b"second chunk"));

                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                            b"streamed",
                        ))))
                    }
                }),
            )
            .await
            .expect("upstream serves streaming request");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves streaming request");
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(
            b"POST /stream-request HTTP/1.1\r\n\
              Host: public.example.test\r\n\
              Transfer-Encoding: chunked\r\n\
              Connection: close\r\n\
              \r\n\
              b\r\nfirst chunk\r\n",
        )
        .await
        .expect("client writes first request chunk");

    first_chunk_rx
        .await
        .expect("upstream received first chunk before request completed");
    assert_eq!(drain.active_count(), 1);

    client
        .write_all(b"c\r\nsecond chunk\r\n0\r\n\r\n")
        .await
        .expect("client writes final request chunk");

    let mut response = Vec::new();
    expect_within(
        client.read_to_end(&mut response),
        "client reads streaming request response",
    )
    .await
    .expect("client reads streaming request response");
    let (headers, body) = split_http_response(&response);
    assert!(headers.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(body, b"streamed");

    proxy_task.await.expect("proxy task completed");
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_streaming_response_holds_lifecycle_until_body_finishes() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");
    let (release_tail_tx, release_tail_rx) = oneshot::channel();
    let release_tail_rx = Arc::new(Mutex::new(Some(release_tail_rx)));

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |_request: Request<Incoming>| {
                    let release_tail_rx = Arc::clone(&release_tail_rx);

                    async move {
                        let (tx, rx) = mpsc::channel(1);
                        let release_tail_rx = release_tail_rx
                            .lock()
                            .expect("release channel lock is not poisoned")
                            .take()
                            .expect("stream release receiver exists");

                        tokio::spawn(async move {
                            tx.send(Bytes::from_static(b"first chunk\n"))
                                .await
                                .expect("first chunk sends");
                            release_tail_rx.await.expect("test releases response tail");
                            tx.send(Bytes::from_static(b"second chunk\n"))
                                .await
                                .expect("second chunk sends");
                        });

                        Ok::<_, Infallible>(Response::new(ReceiverBody { rx }))
                    }
                }),
            )
            .await
            .expect("upstream serves streaming response");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves streaming response");
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(
            b"GET /stream HTTP/1.1\r\nHost: public.example.test\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("client writes request");

    let mut partial = Vec::new();
    while !partial
        .windows(b"first chunk".len())
        .any(|window| window == b"first chunk")
    {
        let mut byte = [0; 1];
        expect_within(
            client.read_exact(&mut byte),
            "client reads first streamed chunk",
        )
        .await
        .expect("client reads first streamed chunk");
        partial.push(byte[0]);
    }
    assert_eq!(drain.active_count(), 1);

    release_tail_tx.send(()).expect("response tail releases");
    expect_within(
        client.read_to_end(&mut partial),
        "client reads remaining streaming response",
    )
    .await
    .expect("client reads remaining response");
    assert!(partial
        .windows(b"second chunk".len())
        .any(|window| window == b"second chunk"));

    proxy_task.await.expect("proxy task completed");
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_cancellation_releases_lifecycle_while_upstream_response_is_pending() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");
    let (request_received_tx, request_received_rx) = oneshot::channel();
    let (release_response_tx, release_response_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let request_received_tx = Arc::new(Mutex::new(Some(request_received_tx)));
        let release_response_rx = Arc::new(Mutex::new(Some(release_response_rx)));

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |_request: Request<Incoming>| {
                    let request_received_tx = Arc::clone(&request_received_tx);
                    let release_response_rx = Arc::clone(&release_response_rx);

                    async move {
                        if let Some(tx) = request_received_tx
                            .lock()
                            .expect("request channel lock is not poisoned")
                            .take()
                        {
                            tx.send(()).expect("test waits for request receipt");
                        }
                        let release_response_rx = release_response_rx
                            .lock()
                            .expect("release channel lock is not poisoned")
                            .take();
                        if let Some(rx) = release_response_rx {
                            let _ = rx.await;
                        }

                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"late"))))
                    }
                }),
            )
            .await
            .ok();
    });

    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());
    let request = Request::builder()
        .uri("/cancel")
        .body(Empty::<Bytes>::new())
        .expect("request builds");

    let proxy_task = tokio::spawn(async move { proxy.proxy(request, &upstream_origin).await });
    request_received_rx
        .await
        .expect("upstream received cancellable request");
    assert_eq!(drain.active_count(), 1);

    proxy_task.abort();
    assert!(proxy_task
        .await
        .expect_err("proxy task is aborted")
        .is_cancelled());
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);

    release_response_tx
        .send(())
        .expect("upstream response release sends");
    upstream_task.await.expect("upstream task completed");
}

#[tokio::test]
async fn http_proxy_handles_concurrent_http2_streams() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");
    let upstream_connections = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(0));
    let (seen_tx, mut seen_rx) = watch::channel(0usize);
    let release_responses = Arc::new(Notify::new());
    let upstream_task = tokio::spawn({
        let upstream_connections = Arc::clone(&upstream_connections);
        let seen = Arc::clone(&seen);
        let seen_tx = seen_tx.clone();
        let release_responses = Arc::clone(&release_responses);

        async move {
            loop {
                let (stream, _) = upstream_listener
                    .accept()
                    .await
                    .expect("upstream accepts h2 proxy connection");
                upstream_connections.fetch_add(1, Ordering::AcqRel);
                let seen = Arc::clone(&seen);
                let seen_tx = seen_tx.clone();
                let release_responses = Arc::clone(&release_responses);

                tokio::spawn(async move {
                    http2::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |request: Request<Incoming>| {
                                let seen = Arc::clone(&seen);
                                let seen_tx = seen_tx.clone();
                                let release_responses = Arc::clone(&release_responses);

                                async move {
                                    let path = request.uri().path().to_owned();
                                    let current = seen.fetch_add(1, Ordering::AcqRel) + 1;
                                    seen_tx.send_replace(current);

                                    expect_within(
                                        release_responses.notified(),
                                        "h2 response release notification",
                                    )
                                    .await;

                                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(path))))
                                }
                            }),
                        )
                        .await
                        .expect("upstream serves h2 request");
                });
            }
        }
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts h2 client connection");

        http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves h2 client connection");
    });

    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build_http::<Empty<Bytes>>();
    let first_uri: Uri = format!("http://{proxy_addr}/first")
        .parse()
        .expect("first URI parses");
    let second_uri: Uri = format!("http://{proxy_addr}/second")
        .parse()
        .expect("second URI parses");

    let first_client = client.clone();
    let second_client = client.clone();
    let first = tokio::spawn(async move { first_client.get(first_uri).await });
    let second = tokio::spawn(async move { second_client.get(second_uri).await });
    expect_within(
        async {
            while *seen_rx.borrow() < 2 {
                seen_rx
                    .changed()
                    .await
                    .expect("h2 request-count watch remains open");
            }
        },
        "both h2 upstream requests arrive before responses are released",
    )
    .await;
    expect_within(
        drain.wait_for_active_count(2),
        "both h2 streams are counted active",
    )
    .await;
    assert_eq!(drain.active_count(), 2);
    assert_eq!(upstream_connections.load(Ordering::Acquire), 1);

    release_responses.notify_waiters();

    let (first, second) = expect_within(
        async move { tokio::join!(first, second) },
        "both h2 responses complete",
    )
    .await;
    let first = first
        .expect("first h2 task completes")
        .expect("first h2 request succeeds");
    let second = second
        .expect("second h2 task completes")
        .expect("second h2 request succeeds");

    assert_eq!(
        first
            .into_body()
            .collect()
            .await
            .expect("first body collects")
            .to_bytes(),
        Bytes::from_static(b"/first")
    );
    assert_eq!(
        second
            .into_body()
            .collect()
            .await
            .expect("second body collects")
            .to_bytes(),
        Bytes::from_static(b"/second")
    );

    drop(client);
    proxy_task.abort();
    let _ = proxy_task.await;
    upstream_task.abort();
    let _ = upstream_task.await;
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_rejects_new_request_after_drain_starts() {
    let drain = DrainTracker::new(Duration::from_secs(5));
    drain.start_drain();
    let proxy = HttpProxy::new(drain);
    let upstream_origin = "http://127.0.0.1:1".parse().expect("upstream URI parses");
    let request = Request::builder()
        .uri("/")
        .body(Empty::<Bytes>::new())
        .expect("request builds");

    let error = proxy
        .proxy(request, &upstream_origin)
        .await
        .expect_err("draining proxy rejects request");

    assert!(matches!(error, HttpProxyError::Drain(DrainError::Draining)));
}

struct ReceiverBody {
    rx: mpsc::Receiver<Bytes>,
}

impl Body for ReceiverBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.rx)
            .poll_recv(cx)
            .map(|maybe_bytes| maybe_bytes.map(|bytes| Ok(Frame::data(bytes))))
    }
}

async fn read_http_response(client: &mut TcpStream) -> (String, Vec<u8>) {
    let mut response = Vec::new();
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut byte = [0; 1];
        expect_within(
            client.read_exact(&mut byte),
            "client reads response headers",
        )
        .await
        .expect("client reads response headers");
        response.push(byte[0]);
    }

    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response has header terminator");
    let headers = String::from_utf8(response[..header_end].to_vec()).expect("headers are utf8");
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| {
                value
                    .trim()
                    .parse::<usize>()
                    .expect("content length parses")
            })
        })
        .expect("response includes content length");
    let mut body = vec![0; content_length];
    expect_within(client.read_exact(&mut body), "client reads response body")
        .await
        .expect("client reads response body");

    (headers, body)
}

async fn expect_within<F>(future: F, context: &str) -> F::Output
where
    F: Future,
{
    timeout(TEST_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{context} timed out after {TEST_TIMEOUT:?}"))
}

fn split_http_response(response: &[u8]) -> (String, &[u8]) {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response has header terminator");
    let headers = String::from_utf8(response[..header_end].to_vec()).expect("headers are utf8");
    let body = &response[header_end + 4..];

    if headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        return (
            headers,
            decode_single_chunk(body).expect("response contains a single chunk"),
        );
    }

    (headers, body)
}

fn decode_single_chunk(body: &[u8]) -> Result<&[u8], Box<dyn Error + Send + Sync>> {
    let size_end = body
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or("chunk size terminator missing")?;
    let size = std::str::from_utf8(&body[..size_end])?;
    let size = usize::from_str_radix(size.trim(), 16)?;
    let chunk_start = size_end + 2;
    let chunk_end = chunk_start + size;

    if body.get(chunk_end..chunk_end + 5) != Some(b"\r\n0\r\n") {
        return Err("single chunk terminator missing".into());
    }

    Ok(&body[chunk_start..chunk_end])
}

#[tokio::test(start_paused = true)]
async fn shared_http_server_bounds_silent_partial_http1_and_partial_h2_setup() {
    for prefix in [
        b"".as_slice(),
        b"GET / HTTP/1.1\r\nHost: example.com\r\n".as_slice(),
        b"PRI * HTTP/2.0\r\n".as_slice(),
    ] {
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::io::AsyncWriteExt::write_all(&mut client, prefix)
            .await
            .unwrap();
        let task = tokio::spawn(support::serve_http_connection(
            server,
            proxy_core::Shutdown::new(),
            |_| async {
                Ok::<_, std::convert::Infallible>(http::Response::new(http_body_util::Full::new(
                    bytes::Bytes::new(),
                )))
            },
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn shared_http_server_does_not_apply_setup_deadline_to_an_active_request() {
    let (client, server) = tokio::io::duplex(4096);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started = std::sync::Arc::new(std::sync::Mutex::new(Some(started_tx)));
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let released = release.clone();
    let task = tokio::spawn(support::serve_http_connection(
        server,
        proxy_core::Shutdown::new(),
        move |_| {
            let started = started.clone();
            let released = released.clone();
            async move {
                started.lock().unwrap().take().unwrap().send(()).unwrap();
                released.notified().await;
                Ok::<_, std::convert::Infallible>(http::Response::new(http_body_util::Full::new(
                    bytes::Bytes::from_static(b"completed"),
                )))
            }
        },
    ));
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(client))
            .await
            .unwrap();
    let client_task = tokio::spawn(connection);
    let request = tokio::spawn(async move {
        sender
            .send_request(http::Request::new(http_body_util::Full::new(
                bytes::Bytes::new(),
            )))
            .await
    });
    started_rx.await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    assert!(!request.is_finished());
    release.notify_one();
    let response = request.await.unwrap().unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "completed"
    );
    client_task.abort();
    task.abort();
}

#[tokio::test]
async fn upstream_pool_bounds_origin_churn_preserves_reuse_and_evicts_idle_sockets() {
    let mut origins = Vec::new();
    let mut tasks = Vec::new();
    let connections = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        origins.push(
            format!("http://{}", listener.local_addr().unwrap())
                .parse::<Uri>()
                .unwrap(),
        );
        let connections = connections.clone();
        tasks.push(tokio::spawn(async move {
            let mut sessions = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                connections.fetch_add(1, Ordering::Relaxed);
                while sessions.try_join_next().is_some() {}
                sessions.spawn(support::serve_http_connection(
                    stream,
                    proxy_core::Shutdown::new(),
                    |_| async {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                    },
                ));
            }
        }));
    }
    let config = proxy_core::ProxyResourceConfig::default()
        .with_upstream_pool(2, 1, Duration::from_millis(100))
        .unwrap();
    let proxy = HttpProxy::with_config(DrainTracker::new(Duration::from_secs(1)), config);
    let request = || {
        Request::builder()
            .uri("/")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    for _ in 0..2 {
        proxy
            .proxy(request(), &origins[0])
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
    }
    assert_eq!(
        connections.load(Ordering::Relaxed),
        1,
        "same-origin keepalive remains pooled"
    );
    proxy
        .proxy(request(), &origins[1])
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap();
    assert_eq!(proxy.upstream_connections().in_flight(), 2);
    assert!(matches!(
        proxy.proxy(request(), &origins[2]).await.unwrap_err(),
        HttpProxyError::UpstreamSaturated
    ));
    timeout(
        TEST_TIMEOUT,
        support::wait_for_in_flight(proxy.upstream_connections(), 0),
    )
    .await
    .unwrap();
    proxy
        .proxy(request(), &origins[2])
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        connections.load(Ordering::Relaxed),
        3,
        "new origin recovers after timed idle eviction"
    );
    drop(proxy);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test(start_paused = true)]
async fn http1_keepalive_connection_outlives_the_setup_timeout_and_ends_at_its_idle_timeout() {
    let (mut client, server) = tokio::io::duplex(4096);
    let mut task = tokio::spawn(support::serve_http_connection(
        server,
        proxy_core::Shutdown::new(),
        |_| async { Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok")))) },
    ));

    // Requests 15 seconds apart: longer than the 10-second setup timeout.
    for _ in 0..3 {
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
            .await
            .unwrap();
        let head = support::read_head(&mut client).await;
        assert!(head.starts_with("HTTP/1.1 200 "), "{head}");
        let mut body = [0_u8; 2];
        client.read_exact(&mut body).await.unwrap();
        tokio::time::advance(Duration::from_secs(15)).await;
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "an idle keep-alive connection stays open"
        );
    }

    // 90 seconds without a request closes it.
    tokio::time::advance(Duration::from_secs(74)).await;
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_secs(2)).await;
    timeout(Duration::from_secs(1), &mut task)
        .await
        .expect("the idle timeout closes the connection")
        .unwrap();
    assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);
}
