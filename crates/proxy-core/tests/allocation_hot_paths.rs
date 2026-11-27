use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
};

use http::Request;
use proxy_core::{
    parse_tls_client_hello_sni, strip_hop_by_hop_headers, upstream_request_uri,
    ActiveConnectionCounter, AdmissionLimiter, TlsClientHelloSni,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNTING.with(|counting| {
            if counting.get() {
                ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
            }
        });

        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[test]
fn accounting_and_admission_fast_paths_do_not_allocate_after_setup() {
    let counter = ActiveConnectionCounter::new();
    let limiter = AdmissionLimiter::new(16);
    drop(counter.track());
    drop(limiter.try_acquire().expect("warm admission path"));

    assert_allocations_at_most("active connection track/release", 0, || {
        let guard = counter.track();
        black_box(counter.active());
        drop(guard);
    });

    assert_allocations_at_most("admission try_acquire/release", 0, || {
        let permit = limiter.try_acquire().expect("admitted");
        black_box(limiter.in_flight());
        drop(permit);
    });
}

#[test]
fn http_forwarding_helper_allocations_stay_bounded() {
    let upstream = "http://127.0.0.1:8080".parse().expect("upstream URI");
    let original = "/v1/items?preserve=true".parse().expect("original URI");

    assert_allocations_at_most("upstream request URI rewrite", 2, || {
        black_box(upstream_request_uri(
            black_box(&upstream),
            black_box(&original),
        ))
        .expect("URI rewrites");
    });

    let mut request = Request::builder()
        .uri("/")
        .header("host", "public.example.test")
        .header("x-forwarded-value", "kept")
        .body(())
        .expect("request builds");

    assert_allocations_at_most("strip static hop-by-hop headers", 0, || {
        strip_hop_by_hop_headers(black_box(request.headers_mut()));
    });
}

#[test]
fn tls_sni_parse_allocations_stay_bounded_for_complete_client_hello() {
    let hello = client_hello(Some(sni_extension(b"tenant.example.test")));

    assert_allocations_at_most("TLS ClientHello SNI parse", 16, || {
        let outcome = parse_tls_client_hello_sni(black_box(&hello)).expect("client hello parses");
        assert!(matches!(outcome, TlsClientHelloSni::Sni { .. }));
        black_box(outcome);
    });
}

fn assert_allocations_at_most<F>(label: &str, max: usize, f: F)
where
    F: FnOnce(),
{
    ALLOCATIONS.set(0);
    COUNTING.set(true);
    f();
    COUNTING.set(false);
    let allocations = ALLOCATIONS.get();

    assert!(
        allocations <= max,
        "{label} allocated {allocations} time(s), expected at most {max}"
    );
}

fn client_hello(extensions: Option<Vec<u8>>) -> Vec<u8> {
    record(client_hello_handshake(extensions))
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
