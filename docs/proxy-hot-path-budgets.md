# Proxy Hot-Path Latency Budgets

Milestone 1F adds benchmark harnesses and deterministic allocation checks for
the proxy-core primitives that future frontline and sidecar binaries will call
on hot routing paths. The numbers are budgets and regression signals, not hard
CI latency gates yet, because local and CI hosts vary too much for stable
wall-clock assertions.

Run the harness with:

```sh
cargo bench -p proxy-core --bench proxy_primitives
```

For a quick compile-and-smoke run:

```sh
cargo bench -p proxy-core --bench proxy_primitives -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```

## Frontline Route Lookup Benchmark

Milestone 9F adds a frontend-owned route lookup benchmark for hot positive
route-cache hits with thousands of unrelated cached routes present:

```sh
cargo bench -p frontline --bench route_lookup
```

For a quick compile-and-smoke run:

```sh
cargo bench -p frontline --bench route_lookup -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```

The benchmark covers HTTP exact-host lookup, HTTP wildcard-suffix lookup,
same-host HTTP path-prefix lookup, and SNI lookup. These paths should stay
indexed by route identity components rather than scanning every cached positive
route on each hot-cache hit.

## Criterion Benchmark Regression Gate

Dynamic certificate delivery adds a matched warm TLS handshake workload:

```sh
cargo bench -p frontline --bench tls_handshake -- --save-baseline before-r1
```

Before changing the certificate lookup path, record three rounds (`before-r1`
through `before-r3`) and preserve their raw artifacts separately. Repeat against
the completed implementation on the same machine with the same workload: P-256
certificate, TLS 1.3 with resumption disabled, verified SNI/peer certificate and
`h2` ALPN, one connection at a time, 64 KiB in-memory transport, and one response
byte. Each round uses 2 seconds of warmup and 100 samples over 10 seconds.
Certificate creation, validation and cache population are outside timing. The
completed cache must prove that measured warm handshakes make zero additional
certificate RPCs. This isolates certificate delivery overhead; it does not
measure deployed throughput, resumed sessions or operating-system networking.

Freeze the comparison before implementation: compare the median of the three
round means; warn above 15% regression and fail above 25%. Keep individual round
results so inconsistent measurements remain visible. Existing route, allocation
and deployed load budgets still apply. Raw Criterion data belongs in ignored
local artifacts; record commands, source identifiers and the three means in the
phase evidence document.

Criterion stores benchmark baselines and comparison results under
`target/criterion`. After running the proxy primitive and frontline route lookup
benchmarks, check the recorded Criterion estimates with:

```sh
./scripts/check-criterion-regressions.py
```

The checker reads each benchmark's `change/estimates.json` mean point estimate,
which Criterion reports as relative change from `base` to `new`. Positive change
means slower/higher time. If Criterion change data is not present, the checker
falls back to comparing `new/estimates.json` and `base/estimates.json` mean point
estimates. Missing or malformed estimates fail by default so required gates do
not silently pass without a usable baseline.

Default thresholds warn above a 15% regression and fail above a 25% regression:

```sh
./scripts/check-criterion-regressions.py \
  --warn-percent 15 \
  --fail-percent 25
```

The same knobs are available as environment variables for local scripts or CI:

```sh
SLEEPYPODS_CRITERION_ROOT=target/criterion \
SLEEPYPODS_BENCH_REGRESSION_WARN_PERCENT=15 \
SLEEPYPODS_BENCH_REGRESSION_FAIL_PERCENT=25 \
./scripts/check-criterion-regressions.py
```

For local adoption before a branch has Criterion baselines, pass
`--allow-missing` or set `SLEEPYPODS_BENCH_REGRESSION_ALLOW_MISSING=1`. Required
release or CI gates should omit that knob. The output is one stable line per
benchmark with `benchmark`, `status`, `reason`, `change_percent`, thresholds,
and comparison `source` fields.

To create or refresh a baseline before checking a change, run the relevant
benchmarks once on the target machine, keep the generated `target/criterion`
artifacts locally, then run the benchmark again after the change so Criterion
can populate `change/estimates.json`. Do not check benchmark result artifacts
into the repository.

## Production Image Sidecar HTTP Load Smoke

Milestone 7E starts with a small production-image smoke for the sidecar HTTP/1.1
hot path:

```sh
./scripts/smoke-sidecar-load.sh
```

The script builds the final `sidecar` image with the root `Dockerfile`, starts a
small helper container that serves both a fixed HTTP backend and a fake
`SidecarControlPlane/ReportIdle` gRPC service, then runs the production sidecar
image in the helper container's network namespace. This keeps the sidecar's
`127.0.0.1:<app-port>` upstream target pointed at the same backend measured by
the direct-backend baseline.

The smoke client sends the same request count to the backend directly and
through the sidecar. It reports request count, failures, elapsed milliseconds,
rough RPS, and successful request latency percentiles as stable `key=value`
fields: `p50_ms`, `p95_ms`, `p99_ms`, and `max_ms`. The script fails on request
correctness errors, process startup failures, missing tools or images, malformed
or missing load metrics, non-positive direct baselines, a sidecar/direct RPS
ratio below `SLEEPYPODS_SIDECAR_LOAD_SMOKE_MIN_RATIO`, or p99 added latency
above `SLEEPYPODS_SIDECAR_LOAD_SMOKE_MAX_ADDED_P99_MS`.

By default, this remains a conservative developer smoke:
`SLEEPYPODS_SIDECAR_LOAD_SMOKE_MIN_RATIO=0.10` and
`SLEEPYPODS_SIDECAR_LOAD_SMOKE_MAX_ADDED_P99_MS=1000`. Release-style runs can
set `SLEEPYPODS_SIDECAR_LOAD_SMOKE_STRICT_BUDGETS=1`, which changes the default
gate values to a direct-baseline RPS ratio of `0.80` and max p99 added latency
of `25` ms. The explicit env vars still override those strict defaults.

Useful knobs:

```sh
SLEEPYPODS_SIDECAR_LOAD_SMOKE_REQUESTS=500 \
SLEEPYPODS_SIDECAR_LOAD_SMOKE_CONCURRENCY=16 \
./scripts/smoke-sidecar-load.sh
```

This HTTP smoke only covers the sidecar HTTP/1.1 hot path. It does not cover
frontline routing, Kubernetes, the real control plane, HTTP/2, h2c, gRPC, TCP,
WebSockets, streaming throughput, cold-wake behavior, or protocol paths beyond
HTTP/1.1. Its p99 gate is a same-run direct-baseline sidecar HTTP check, not a
general end-to-end latency SLO.

## Production Image Sidecar TCP Load Smoke

Milestone 7E also includes a small production-image smoke for the sidecar TCP
hot path:

```sh
./scripts/smoke-sidecar-tcp-load.sh
```

The script builds the final `sidecar` image with the root `Dockerfile`
(`BIN=sidecar`), builds the sidecar load-smoke helper image, and starts the
helper with a deterministic TCP echo backend plus a fake
`SidecarControlPlane/ReportIdle` gRPC service. The production sidecar container
joins the helper container's network namespace with
`SLEEPYPODS_SIDECAR_MODE=tcp`, so the sidecar's `127.0.0.1:<app-port>` upstream
target points at the same TCP echo backend used by the direct-backend baseline.

The TCP smoke client opens a fixed number of streams, writes a known byte count
per stream, and validates the exact echoed bytes while reads and writes run
concurrently. It fails on byte mismatches, early close, timeouts, startup or
process failures, and missing tools or images. It reports stream count, echoed
bytes, expected bytes, failures, elapsed milliseconds, rough MiB/s, and
successful stream latency percentiles as stable `key=value` fields: `p50_ms`,
`p95_ms`, `p99_ms`, and `max_ms`. The script fails on malformed or missing
metrics, non-positive direct baselines, a sidecar/direct throughput ratio below
`SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO`, or stream p99 added latency above
`SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MAX_ADDED_P99_MS`.

By default, this remains a conservative developer smoke:
`SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO=0.05` and
`SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MAX_ADDED_P99_MS=10000`. Release-style runs
can set `SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STRICT_BUDGETS=1`, which changes the
default gate values to a direct-baseline throughput ratio of `0.85` and max
stream p99 added latency of `500` ms. The explicit env vars still override
those strict defaults.

Useful knobs:

```sh
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STREAMS=8 \
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONCURRENCY=4 \
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BYTES_PER_STREAM=8388608 \
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CHUNK_SIZE=32768 \
./scripts/smoke-sidecar-tcp-load.sh
```

This smoke only covers plain TCP byte forwarding through the sidecar production
image. It does not cover Kubernetes, the real control plane, frontline routing,
TLS/SNI, HTTP/2, h2c, gRPC, WebSockets, or protocol-specific streaming
behavior. Its throughput and p99 stream gates are
same-run direct-baseline TCP checks, not cross-machine absolute throughput or
latency SLOs.

## Production Image Frontline HTTP, h2/gRPC, WebSocket, and Cold-Wake Load Smoke

Milestone 7E also has a narrow production-image smoke for the frontline
HTTP/1.1, h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, real generated
gRPC, WebSocket, and cold-wake paths:

```sh
./scripts/smoke-frontline-load.sh
```

The script builds the final `frontline` image with the root `Dockerfile`
(`BIN=frontline`), builds a small helper image, and starts that helper with a
fixed HTTP backend plus a fake `ProxyControlPlane` gRPC service. The fake
control plane returns a running route for `Host: app.example.test` and `/smoke`
whose backend URI points at `http://127.0.0.1:<backend-port>` inside the helper
network namespace. It also returns a distinct cold route for `/cold-smoke`.
The production frontline container then joins that namespace, so both the
control-plane endpoint and backend are reached through loopback while the load
client connects to published `127.0.0.1:<port>` sockets and sends the known
Host header.

Before measuring the HTTP/1.1 frontline path, the script waits for one
successful frontline request. That request validates lazy route resolution
through `SubscribeRoute` and warms the route cache; the measured frontline phase
then uses the cached ready route. The helper exposes a small stats endpoint for
the fake control plane, and the script fails if the measured hot-cache
frontline phase increments `subscribe_route_calls`.

After the HTTP/1.1 phase, the script also warms and measures a prior-knowledge
h2c POST with `content-type: application/grpc`, a fixed gRPC-framed request
body asserted by the helper, a fixed gRPC-framed response body, and
`grpc-status: 0` as an HTTP/2 response trailer. Each load worker opens one h2c
connection before the timed window starts and multiplexes all of its measured
requests over that long-lived channel, matching how generated gRPC clients
such as tonic and grpc-go hold long-lived multiplexed channels. The helper
backend accepts both HTTP/1.1 and h2c, so the h2c phase uses the same
direct-backend baseline and the same production frontline image. The h2c phase
also fails if measured hot-cache requests make additional `SubscribeRoute`
calls.

The script generates a temporary self-signed certificate for the route host and
serves it from the helper's authenticated native TLS certificate API. Frontline
receives only its proxy credential and the public platform CA, resolves the
application certificate into memory, and warms/measures the same request over
negotiated HTTP/2-over-TLS. Each load worker completes its TCP, TLS, and
HTTP/2 setup before the timed window starts and multiplexes measured requests
over that single negotiated channel. The client trusts only the generated
certificate and fails unless TLS ALPN negotiates `h2`, so this phase cannot
silently fall back to h2c. This uses the production proxy delivery path with a
fixture resolver; it does not exercise production certificate publication or
encrypted database storage. Its same-run direct baseline is the helper's direct
h2c backend path, so the frontend ratio and p99 gates intentionally include
TLS termination and proxy overhead rather than comparing against a separate
direct TLS backend.

The two h2 phases therefore gate steady-state multiplexed request throughput,
not connection establishment. Per-request connection churn through the proxy
stays covered by the HTTP/1.1 phase, which sends `Connection: close` on every
request, and by the real generated gRPC phase, which opens a new tonic channel
per request.

The smoke also starts a small generated gRPC backend using the existing
generated `ProxyControlPlane` service and measures generated
`ProxyControlPlaneClient/WakeInstance` calls directly to that backend and
through the production frontline HTTP listener. The fake control plane routes
only the generated method path to this backend, and the measured hot-cache phase
fails if it makes additional `SubscribeRoute` calls after warmup.

The script then warms and measures WebSocket forwarding through the same cached
ready route. The helper client performs a real HTTP upgrade, sends text and
binary frames, validates text and binary frames returned by the backend, then
sends a deterministic binary byte stream in configurable frames. The helper
backend validates the exact incoming stream bytes and echoes them; the client
validates the exact echoed bytes. The measured WebSocket phase has a
direct-backend baseline, emits `mib_per_s`, gates frontend/direct streaming
throughput ratio, and fails if it makes additional `SubscribeRoute` calls after
warmup.

Finally, the script sends one request through the distinct cold route. The fake
control plane returns `InstanceState::Cold` without a backend URI, the
production frontline calls `WakeInstance`, the fake control plane returns a
ready backend, and the client request must succeed through the backend. The
helper stats endpoint exposes separate `subscribe_route_calls`,
`wake_instance_calls`, and `backend_http_requests` counters; the cold-wake smoke
fails unless that phase increments each expected counter exactly once. The same
client result emits latency fields and the script gates the single-request p99
against `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_WAKE_MAX_LATENCY_MS`. This is a
fake-control-plane/proxy cold-wake latency gate. It is not a Kubernetes
materialization latency SLO and does not measure real workload creation,
readiness, Service publication, or sidecar startup.

The client sends the same request count directly to the backend and through
frontline for each protocol phase, reporting request count, failures, elapsed
milliseconds, rough RPS, WebSocket stream bytes and MiB/s, and successful
request/session latency percentiles as stable `key=value` fields: `stream_bytes`,
`mib_per_s`, `p50_ms`, `p95_ms`, `p99_ms`, and `max_ms`. The smoke fails on
correctness errors, startup or process failures, missing tools or images,
malformed or missing load metrics, non-positive direct baselines, additional
hot-cache `SubscribeRoute` calls, frontend/direct RPS or WebSocket throughput
ratios below their configured thresholds, p99 added latency above configured
thresholds, or fake-control-plane cold-wake latency above its configured
threshold.

By default, this remains a conservative developer smoke:
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO=0.10`,
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MIN_RATIO=0.10`,
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_H2_TLS_GRPC_MIN_RATIO=0.10`,
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REAL_GRPC_MIN_RATIO=0.10`,
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MIN_RATIO=0.10`,
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_MIN_RATIO=0.10`, each
`*_MAX_ADDED_P99_MS` defaulting to `1000`, and
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_WAKE_MAX_LATENCY_MS=5000`.
Release-style runs can set
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_STRICT_BUDGETS=1`, which changes the default
HTTP/1.1 ratio to `0.80`, h2c gRPC-shaped, h2-over-TLS gRPC-shaped, and real
generated gRPC ratios to `0.75`, WebSocket session ratio to `0.80`, WebSocket
stream throughput ratio to `0.80`, each max p99 added-latency threshold to
`25` ms, and fake-control-plane cold-wake latency to `250` ms. The explicit env
vars still override those strict defaults.

Useful knobs:

```sh
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REQUESTS=500 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONCURRENCY=16 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_REQUESTS=500 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_CONCURRENCY=16 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_H2_TLS_GRPC_REQUESTS=500 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_H2_TLS_GRPC_CONCURRENCY=16 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REAL_GRPC_REQUESTS=500 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REAL_GRPC_CONCURRENCY=16 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_REQUESTS=500 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_CONCURRENCY=16 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_BYTES=1048576 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_CHUNK_SIZE=32768 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HOST=app.example.test \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_PATH=/smoke \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_PATH=/cold-smoke \
./scripts/smoke-frontline-load.sh
```

This smoke only covers frontline HTTP/1.1, prior-knowledge h2c gRPC-shaped,
negotiated HTTP/2 over the production TLS-termination listener, real generated
gRPC unary forwarding, and WebSocket cached ready-route forwarding and
byte-streaming plus one fake-control-plane cold wake through the production
frontline image. The h2c and h2-over-TLS ratio gates measure multiplexed
request throughput over one long-lived HTTP/2 channel per load worker with
connection setup outside the timed window; per-connection setup cost is
exercised by the HTTP/1.1 and generated-gRPC phases instead. The smoke does
not cover Kubernetes, the real control plane, SNI passthrough, TCP, real
Kubernetes cold materialization latency, or route-cache invalidation behavior.

## Production-Image Load Budget Gates

The production-image load smokes compare proxied paths to direct-backend
baselines measured in the same run and environment. Ordinary local runs keep
loose thresholds so they catch extreme regressions without depending on stable
developer Docker timing. Release runs should opt into strict defaults:

```sh
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_STRICT_BUDGETS=1 ./scripts/smoke-frontline-load.sh
SLEEPYPODS_SIDECAR_LOAD_SMOKE_STRICT_BUDGETS=1 ./scripts/smoke-sidecar-load.sh
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STRICT_BUDGETS=1 ./scripts/smoke-sidecar-tcp-load.sh
```

Strict defaults:

| Script/path | Ratio metric | Strict default | p99 added metric | Strict default |
| --- | --- | --- | --- | --- |
| `smoke-frontline-load.sh` HTTP/1.1 | frontline RPS / direct RPS | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO=0.80` | frontend `p99_ms` - direct `p99_ms` | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MAX_ADDED_P99_MS=25` |
| `smoke-frontline-load.sh` h2c gRPC-shaped | frontline RPS / direct RPS | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MIN_RATIO=0.75` | frontend `p99_ms` - direct `p99_ms` | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MAX_ADDED_P99_MS=25` |
| `smoke-frontline-load.sh` h2-over-TLS gRPC-shaped | frontline TLS h2 RPS / direct h2c RPS | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_H2_TLS_GRPC_MIN_RATIO=0.75` | frontend TLS h2 `p99_ms` - direct h2c `p99_ms` | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_H2_TLS_GRPC_MAX_ADDED_P99_MS=25` |
| `smoke-frontline-load.sh` real generated gRPC | frontline generated gRPC RPS / direct generated gRPC RPS | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REAL_GRPC_MIN_RATIO=0.75` | frontend generated gRPC `p99_ms` - direct generated gRPC `p99_ms` | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REAL_GRPC_MAX_ADDED_P99_MS=25` |
| `smoke-frontline-load.sh` WebSocket | frontline sessions/s / direct sessions/s | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MIN_RATIO=0.80` | frontend session `p99_ms` - direct session `p99_ms` | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MAX_ADDED_P99_MS=25` |
| `smoke-frontline-load.sh` WebSocket stream | frontline MiB/s / direct MiB/s | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_MIN_RATIO=0.80` | Covered by WebSocket session p99 gate | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MAX_ADDED_P99_MS=25` |
| `smoke-frontline-load.sh` fake-control-plane cold wake | absolute frontend single-request p99 | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_WAKE_MAX_LATENCY_MS=250` | Not baseline-relative | Not applicable |
| `smoke-sidecar-load.sh` HTTP/1.1 | sidecar RPS / direct RPS | `SLEEPYPODS_SIDECAR_LOAD_SMOKE_MIN_RATIO=0.80` | sidecar `p99_ms` - direct `p99_ms` | `SLEEPYPODS_SIDECAR_LOAD_SMOKE_MAX_ADDED_P99_MS=25` |
| `smoke-sidecar-tcp-load.sh` TCP large streams | sidecar MiB/s / direct MiB/s | `SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO=0.85` | sidecar stream `p99_ms` - direct stream `p99_ms` | `SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MAX_ADDED_P99_MS=500` |

Smoke defaults use the same env vars but remain intentionally loose: frontline
and sidecar HTTP/WebSocket/h2c/h2-TLS/generated-gRPC ratios default to `0.10`,
frontline WebSocket stream throughput ratio defaults to `0.10`, sidecar TCP
throughput ratio defaults to `0.05`, HTTP/WebSocket/h2c/h2-TLS/generated-gRPC
p99 added latency defaults to `1000` ms, fake-control-plane cold-wake latency
defaults to `5000` ms, and TCP stream p99 added latency defaults to `10000` ms.

## Budget Principles

- Hot forwarding paths must not perform control-plane calls, database access,
  global locking, per-request client construction additions, unbounded
  allocation, or heavy instrumentation.
- Cold wake latency is intentionally separate from hot routing latency. Waking a
  workload can be slower; forwarding to an already resolved backend should stay
  local and predictable.
- Benchmarks should use in-memory transports where possible and loopback only
  when a real socket is the behavior under test. They should not depend on
  external services or sleep as a timing mechanism.
- Allocation-sensitive tests guard deterministic pure/helper paths. Network
  benchmark timings are observational and should not fail CI on wall-clock
  noise.

## Measured Paths

| Path | Harness | Budget intent |
| --- | --- | --- |
| TCP forwarding | `tcp/proxy_streams_duplex_4k_round_trip`, `tcp/proxy_streams_with_idle_timeout_duplex_4k_round_trip` | Measure the byte relay without external network variance: first with no idle deadline, as a WebSocket session runs it, then with one, as TCP and SNI passthrough run it. |
| HTTP helper work | `http/prepare_reverse_proxy_request`, `http/strip_hop_by_hop_headers` | Keep request URI rewrite and hop-by-hop header cleanup bounded and simple. |
| WebSocket relay | `websocket/proxy_streams_duplex_binary_round_trip` | Two WebSocket peers exchange a binary message and close through the byte relay, without an external service. The peers frame; the relay copies bytes. |
| TLS ClientHello/SNI parsing | `tls/parse_client_hello_sni_*` | Keep complete and fragmented ClientHello parsing bounded before TLS routing decisions are added. |
| Frontline route lookup | `route_lookup/http_exact_host_many_unrelated`, `route_lookup/http_wildcard_suffix_many_unrelated`, `route_lookup/http_same_host_many_paths`, `route_lookup/sni_exact_host_many_unrelated` | Keep hot positive route-cache lookup indexed with many unrelated cached routes present. |
| Admission/accounting | `admission/try_acquire_release`, `accounting/track_release` | Keep permit/guard operations allocation-free after setup. A permit is a semaphore operation, and a guard is a few atomic operations that wake a task only while one waits for the count. |
| Observability helpers | `observability/label_as_str_and_outcome_mapping` | Keep label and outcome mapping low-cardinality and allocation-free. |

## Provisional Latency Budgets

These initial budgets are intentionally conservative relative to the 1F smoke
run. They are review thresholds for future regressions, not CI pass/fail gates.
If a future change exceeds one of these budgets, the change should either reduce
the overhead, update the benchmark workload, or document why the new cost is
expected.

| Path | Benchmark | Initial budget |
| --- | --- | --- |
| TCP forwarding | `tcp/proxy_streams_duplex_4k_round_trip` | <= 10 us per 4 KiB in-memory round trip |
| HTTP request rewrite | `http/prepare_reverse_proxy_request` | <= 2 us per request |
| HTTP hop-by-hop header cleanup | `http/strip_hop_by_hop_headers` | <= 2 us per header map |
| WebSocket relay | `websocket/proxy_streams_duplex_binary_round_trip` | <= 50 us per 1 KiB in-memory binary round trip |
| TLS ClientHello/SNI parse, single record | `tls/parse_client_hello_sni_single_record` | <= 1 us per complete ClientHello |
| TLS ClientHello/SNI parse, fragmented records | `tls/parse_client_hello_sni_fragmented_records` | <= 2 us per fragmented ClientHello |
| Frontline route lookup | `route_lookup/*` | <= 1 us per hot-cache lookup with 4096 unrelated cached routes or same-host path routes |
| Admission acquire/release | `admission/try_acquire_release` | <= 100 ns per steady-state permit |
| Active connection track/release | `accounting/track_release` | <= 100 ns per steady-state guard |
| Observability label/outcome helpers | `observability/label_as_str_and_outcome_mapping` | <= 50 ns per mapping batch |

## Allocation Budgets

The deterministic allocation tests in `allocation_hot_paths.rs` enforce these
initial budgets. Setup allocations are outside the measured closures; the
budgets describe steady-state helper behavior.

| Path | Test label | Allocation budget |
| --- | --- | --- |
| Observability label helpers | `observability labels` | 0 allocations |
| Active connection track/release | `active connection track/release` | 0 allocations after setup warm-up |
| Admission acquire/release | `admission try_acquire/release` | 0 allocations after setup warm-up |
| HTTP upstream URI rewrite | `upstream request URI rewrite` | <= 2 allocations |
| HTTP static hop-by-hop cleanup | `strip static hop-by-hop headers` | 0 allocations |
| TLS ClientHello/SNI parse | `TLS ClientHello SNI parse` | <= 16 allocations for a complete SNI ClientHello |

Local route-key lookup benchmarks are covered by
`cargo bench -p frontline --bench route_lookup`. The current harness focuses on
frontline hot-cache positive lookup; broader route throughput and tail-latency
gates remain separate load-test work.
