use std::{hint::black_box, time::Duration};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures_util::{SinkExt, StreamExt};
use http::{Request, Uri};
use proxy_core::{
    parse_tls_client_hello_sni, prepare_reverse_proxy_request, proxy_streams_with_timeouts,
    strip_hop_by_hop_headers, ActiveConnectionCounter, AdmissionLimiter,
};
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt},
    runtime::Runtime,
};
use tokio_tungstenite::{
    tungstenite::{protocol::Role, Bytes as WsBytes, Message},
    WebSocketStream,
};

const WRITE_TIMEOUT: Duration = Duration::from_secs(60);

fn tcp_forwarding(c: &mut Criterion) {
    let runtime = runtime();
    let payload = Bytes::from_static(&[7; 4096]);

    // Without an idle deadline the relay is a WebSocket session's; with one it
    // is a TCP or SNI-passthrough session's.
    for (name, idle_timeout) in [
        ("tcp/proxy_streams_duplex_4k_round_trip", None),
        (
            "tcp/proxy_streams_with_idle_timeout_duplex_4k_round_trip",
            Some(Duration::from_secs(3600)),
        ),
    ] {
        c.bench_function(name, |b| {
            b.to_async(&runtime).iter(|| {
                let payload = payload.clone();

                async move {
                    let (mut client, proxy_client) = duplex(16 * 1024);
                    let (proxy_upstream, mut upstream) = duplex(16 * 1024);

                    // A session task builds its relay in place, as the listeners do.
                    let proxy_task = tokio::spawn(async move {
                        proxy_streams_with_timeouts(
                            proxy_client,
                            proxy_upstream,
                            idle_timeout,
                            WRITE_TIMEOUT,
                            None,
                        )
                        .await
                    });

                    client.write_all(&payload).await.expect("client writes");
                    client.shutdown().await.expect("client closes writes");

                    let mut received = vec![0; payload.len()];
                    upstream
                        .read_exact(&mut received)
                        .await
                        .expect("upstream reads");
                    black_box(&received);

                    upstream.write_all(&payload).await.expect("upstream writes");
                    upstream.shutdown().await.expect("upstream closes writes");

                    let mut echoed = vec![0; payload.len()];
                    client.read_exact(&mut echoed).await.expect("client reads");
                    black_box(&echoed);

                    proxy_task
                        .await
                        .expect("proxy task joins")
                        .expect("proxy succeeds");
                }
            })
        });
    }
}

fn http_helpers(c: &mut Criterion) {
    let upstream: Uri = "http://127.0.0.1:8080".parse().expect("upstream URI");

    c.bench_function("http/prepare_reverse_proxy_request", |b| {
        b.iter_batched(
            || http_request(()),
            |request| {
                black_box(
                    prepare_reverse_proxy_request(request, black_box(&upstream))
                        .expect("request rewrites"),
                );
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("http/strip_hop_by_hop_headers", |b| {
        b.iter_batched(
            || http_request(()).into_parts().0.headers,
            |mut headers| {
                strip_hop_by_hop_headers(black_box(&mut headers));
                black_box(headers);
            },
            BatchSize::SmallInput,
        )
    });
}

fn websocket_relay(c: &mut Criterion) {
    let runtime = runtime();
    let payload = WsBytes::from_static(&[9; 1024]);

    c.bench_function("websocket/proxy_streams_duplex_binary_round_trip", |b| {
        b.to_async(&runtime).iter(|| {
            let payload = payload.clone();

            async move {
                let (client_io, proxy_client_io) = duplex(16 * 1024);
                let (proxy_upstream_io, upstream_io) = duplex(16 * 1024);

                let mut client =
                    WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
                let mut upstream =
                    WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;

                // The proxy relays the upgraded bytes; only the peers frame them.
                let proxy_task = tokio::spawn(async move {
                    proxy_streams_with_timeouts(
                        proxy_client_io,
                        proxy_upstream_io,
                        None,
                        WRITE_TIMEOUT,
                        None,
                    )
                    .await
                });

                client
                    .send(Message::Binary(payload.clone()))
                    .await
                    .expect("client sends");
                let received = upstream
                    .next()
                    .await
                    .expect("upstream receives")
                    .expect("message is valid");
                black_box(received);

                upstream
                    .send(Message::Binary(payload))
                    .await
                    .expect("upstream sends");
                let echoed = client
                    .next()
                    .await
                    .expect("client receives")
                    .expect("message is valid");
                black_box(echoed);

                client.close(None).await.expect("client closes");
                let close = upstream
                    .next()
                    .await
                    .expect("upstream receives close")
                    .expect("close is valid");
                black_box(close);
                upstream.flush().await.expect("upstream flushes close");
                drop((client, upstream));

                let _ = black_box(proxy_task.await.expect("proxy task joins"));
            }
        })
    });
}

fn tls_sni(c: &mut Criterion) {
    let hello = client_hello(Some(sni_extension(b"tenant.example.test")));
    let fragmented = fragmented_client_hello(&hello, 1);

    c.bench_function("tls/parse_client_hello_sni_single_record", |b| {
        b.iter(|| {
            black_box(parse_tls_client_hello_sni(black_box(&hello)).expect("client hello parses"));
        })
    });

    c.bench_function("tls/parse_client_hello_sni_fragmented_records", |b| {
        b.iter(|| {
            black_box(
                parse_tls_client_hello_sni(black_box(&fragmented))
                    .expect("fragmented client hello parses"),
            );
        })
    });
}

fn admission_and_accounting(c: &mut Criterion) {
    let limiter = AdmissionLimiter::new(1024);
    c.bench_function("admission/try_acquire_release", |b| {
        b.iter(|| {
            let permit = limiter.try_acquire().expect("admitted");
            black_box(limiter.in_flight());
            drop(permit);
        })
    });

    let counter = ActiveConnectionCounter::new();
    c.bench_function("accounting/track_release", |b| {
        b.iter(|| {
            let guard = counter.track();
            black_box(counter.active());
            drop(guard);
        })
    });
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime builds")
}

fn http_request<B>(body: B) -> Request<B> {
    Request::builder()
        .method("POST")
        .uri("/v1/items?preserve=true")
        .header("host", "public.example.test")
        .header("x-forwarded-value", "kept")
        .header("connection", "x-remove, upgrade")
        .header("x-remove", "drop")
        .header("upgrade", "websocket")
        .body(body)
        .expect("request builds")
}

fn client_hello(extensions: Option<Vec<u8>>) -> Vec<u8> {
    record(client_hello_handshake(extensions))
}

fn fragmented_client_hello(hello: &[u8], first_record_payload_len: usize) -> Vec<u8> {
    let handshake = &hello[5..];
    assert!(first_record_payload_len > 0);
    assert!(first_record_payload_len < handshake.len());

    let mut fragmented = record(handshake[..first_record_payload_len].to_vec());
    fragmented.extend(record(handshake[first_record_payload_len..].to_vec()));
    fragmented
}

fn client_hello_handshake(extensions: Option<Vec<u8>>) -> Vec<u8> {
    handshake(client_hello_body(extensions))
}

fn client_hello_body(extensions: Option<Vec<u8>>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0x11; 32]);
    body.push(0);
    push_u16(&mut body, 2);
    body.extend_from_slice(&[0x13, 0x01]);
    body.push(1);
    body.push(0);

    if let Some(extensions) = extensions {
        push_u16(&mut body, extensions.len());
        body.extend_from_slice(&extensions);
    }

    body
}

fn handshake(body: Vec<u8>) -> Vec<u8> {
    let mut handshake = Vec::new();
    handshake.push(0x01);
    push_u24(&mut handshake, body.len());
    handshake.extend_from_slice(&body);
    handshake
}

fn record(payload: Vec<u8>) -> Vec<u8> {
    let mut record = Vec::new();
    record.extend_from_slice(&[0x16, 0x03, 0x03]);
    push_u16(&mut record, payload.len());
    record.extend_from_slice(&payload);
    record
}

fn sni_extension(hostname: &[u8]) -> Vec<u8> {
    let mut name = Vec::new();
    name.push(0);
    push_u16(&mut name, hostname.len());
    name.extend_from_slice(hostname);

    let mut extension_data = Vec::new();
    push_u16(&mut extension_data, name.len());
    extension_data.extend_from_slice(&name);

    extension(0, extension_data)
}

fn extension(extension_type: u16, data: Vec<u8>) -> Vec<u8> {
    let mut extension = Vec::new();
    push_u16(&mut extension, extension_type as usize);
    push_u16(&mut extension, data.len());
    extension.extend_from_slice(&data);
    extension
}

fn push_u16(bytes: &mut Vec<u8>, value: usize) {
    let value = u16::try_from(value).expect("test value fits in u16");
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u24(bytes: &mut Vec<u8>, value: usize) {
    assert!(value <= 0x00ff_ffff, "test value fits in u24");
    bytes.extend_from_slice(&[
        ((value >> 16) & 0xff) as u8,
        ((value >> 8) & 0xff) as u8,
        (value & 0xff) as u8,
    ]);
}

criterion_group!(
    benches,
    tcp_forwarding,
    http_helpers,
    websocket_relay,
    tls_sni,
    admission_and_accounting
);
criterion_main!(benches);
