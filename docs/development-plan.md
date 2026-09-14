# SleepyPods Development Plan

This plan turns the SleepyPods production north star into implementation
milestones. Each milestone should have integration coverage, and Kubernetes
behavior should be verified with kind-based end-to-end tests before it is
considered done.

Dynamic certificate delivery is planned separately in the
[dynamic certificate delivery plan](dynamic-certificates-plan.md). Its phased
ledgers track the dynamic-only implementation and replacement of static
application certificate loading.

Milestone statuses, test names and gate results below are historical checkpoints,
not fresh validation or the current recovery contract. For current architecture,
correctness and performance, follow the [codebase review plan](review-remediation-plan.md)
and its evidence-backed ledgers. They supersede conflicting milestone claims.
Accepted lifecycle work either completes, exposes a classified failure, or
remains blocked by an unresolved Kubernetes effect. That uncertainty may persist
indefinitely and prevents ordinary lease takeover from releasing ownership;
explicit settlement follows the [projection recovery contract](projection-safety.md).
Historical references to automatic cleanup or "force-delete" do not authorize
today's audited administrative overrides.

## Implementation Principles

- Prefer the simplest implementation that satisfies the current milestone and
  its tests.
- Keep code surface area small. Scalability and maintainability should come from
  clear boundaries, predictable state machines, and fewer moving parts before
  they come from clever abstractions.
- Avoid "mega files". Split code by responsibility when a file starts combining
  unrelated protocol, state-machine, persistence, or Kubernetes concerns, while
  avoiding abstraction for its own sake.
- Add abstractions only when repeated behavior or testability makes the benefit
  concrete.
- Do not defer useful comments. Comments should explain protocol edge cases,
  lifecycle invariants, reconciliation assumptions, and places where a future
  maintainer could otherwise make a dangerous simplification.
- Keep each sub-phase reviewable. A phase is not done until the narrowest useful
  integration test proves the behavior works.
- Treat low-latency hot paths as an explicit design constraint. Proxy request
  forwarding, stream forwarding, and local route-cache hits should avoid
  control-plane calls, database access, global locks, unbounded allocation, and
  per-request client construction.
- Keep production containers small and explicit. The control plane, frontline
  proxy, and sidecar should use `scratch` final images when practical; if a
  component needs runtime files such as CA roots, timezone data, passwd/group
  entries, or certificates, copy in only those files deliberately.

## Testing Strategy

Use four test layers:

1. Unit tests for pure logic:
   - route key normalization
   - opaque subscription ID handling
   - wildcard and longest-prefix matching
   - state-machine transitions
   - template value validation
   - structured manifest rendering
   - PV/PVC field rendering, including names, labels, access modes, capacity,
     reclaim policy, volume source, and instance value substitution

2. Component integration tests:
   - proxy components against fake control-plane services
   - control plane against a real test database
   - Kubernetes materializer against a real or fake API server where useful
   - Kubernetes object assertions for PV/PVC binding intent, owner labels,
     generation labels, and workload volume mounts

3. Protocol integration tests:
   - HTTP/1.1
   - HTTP/2
   - h2c gRPC
   - gRPC-Web for operator-facing control-plane APIs
   - WebSockets
   - TLS termination
   - TLS/SNI passthrough
   - HTTP-01 challenge handling
   - PostgreSQL/libpq 17+ SNI passthrough with `sslnegotiation=direct`, using
     a real Postgres workload as proof that SNI routing works for application
     code we did not write

4. kind end-to-end tests:
   - build the final production-style images for the control plane, frontline
     proxy, and sidecar
   - load images into kind
   - deploy control plane, frontline proxy, sidecar, and demo workloads
   - create `WorkloadClass`, `Instance`, and `RouteBinding`
   - verify cold wake, hot routing, drain/sleep, and re-wake
   - verify PV/PVC materialization before workload creation, intended binding,
     pod mount behavior, and data continuity across sleep/re-wake
   - verify custom host/SNI routing and HTTP-01 challenge lookup
   - verify PostgreSQL/libpq 17+ connects through TLS/SNI passthrough using
     `sslnegotiation=direct`; pin the image version in CI rather than relying
     on a floating `latest` tag

The kind suite is a release gate. Unit and component tests are not enough for
this project because most failures will happen at Kubernetes object lifecycle,
networking, and readiness boundaries.

Container tests must exercise the same final images that operators would run,
not local binaries or dev-only images. At minimum, test startup, health/readiness,
TLS/CA access, database and Kubernetes API connectivity, non-root execution, and
the absence of accidental runtime dependencies such as shells or package
managers.

Each milestone should name the important success, failure, reconnect, timeout,
and race cases before implementation starts. Avoid broad "works end to end"
claims without assertions for the state that makes the behavior correct.

## Milestone 1: Rust Proxy Primitives

Build the shared network and proxy building blocks used by both frontline and
sidecar binaries.

Scope:

- Tokio runtime setup and structured shutdown.
- Bidirectional TCP stream proxying.
- HTTP reverse proxy helpers.
- WebSocket upgrade and proxying.
- Active request and connection accounting.
- Drain tracker with configurable grace timeout.
- Timeout and backpressure primitives.
- TLS ClientHello/SNI extraction helpers.
- Shared metrics and tracing conventions.
- Hot-path latency budgets and benchmark harnesses for proxy primitives.

Sub-phases:

- 1A: TCP stream proxy, active connection accounting, and drain tracker.
- 1B: HTTP reverse proxy helpers and WebSocket upgrade/proxying.
- 1C: Timeout, backpressure, structured shutdown, and cancellation behavior.
- 1D: TLS ClientHello/SNI extraction helpers.
- 1E: Shared metrics and tracing conventions.
- 1F: Proxy hot-path latency budgets, benchmark harnesses, and allocation checks.

Done when:

- Unit tests cover accounting, drain, timeout, and SNI parsing.
- Integration tests proxy TCP streams, HTTP requests, and WebSocket sessions.
- TCP tests cover byte preservation, half-close behavior, upstream reset, client
  reset, timeout, and backpressure.
- HTTP tests cover HTTP/1.1 keep-alive, chunked bodies, large bodies, streaming
  request/response bodies, HTTP/2 multiplexing, and cancellation.
- WebSocket tests cover upgrade failure, bidirectional traffic, close frames,
  peer disconnect, and backpressure.
- Drain tests prove new work is rejected while existing streams get the grace
  period.
- Hot-path benchmarks cover TCP forwarding, HTTP forwarding, WebSocket relay,
  TLS ClientHello/SNI extraction, local route-key lookup primitives, and
  admission/accounting overhead.
- Benchmark notes separate hot routing latency from cold wake latency; cold wake
  can be slower, but hot proxy paths must not depend on control-plane calls,
  database access, per-request client construction, or unbounded allocation.
- Allocation-sensitive tests or profiles exist for the hot path so regressions
  are visible before the frontline proxy is built on top of these primitives.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Tokio runtime setup and structured shutdown. | `crates/proxy-core/src/shutdown.rs` and `tests/lifecycle.rs` cover cancellation tokens, child tokens, late waiters, and shutdown propagation. |
| Complete | Bidirectional TCP stream proxying. | `crates/proxy-core/tests/tcp_proxy.rs` proves byte preservation, half-close behavior, and lifecycle accounting. |
| Complete | HTTP reverse proxy helpers. | `crates/proxy-core/src/http.rs` tests request rewriting and hop-by-hop stripping; `tests/http_proxy.rs` covers basic request/response forwarding and drain rejection. |
| Complete | WebSocket upgrade and proxying. | `crates/proxy-core/tests/websocket_proxy.rs` covers bidirectional messages, close propagation, lifecycle, and drain rejection. |
| Complete | Active request and connection accounting. | `crates/proxy-core/src/accounting.rs` and `src/admission.rs` cover guard lifetime, idempotent release, waiters, limits, and cancellation. |
| Complete | Drain tracker with configurable grace timeout. | `crates/proxy-core/src/drain.rs` covers rejecting new work, waiting for active work, and timeout reporting. |
| Complete | Timeout and backpressure primitives. | `src/timeout.rs` covers typed timeout results; TCP/WebSocket proxy tests exercise async copy paths. |
| Complete | TLS ClientHello/SNI extraction helpers. | `src/tls.rs` covers valid SNI, malformed input, missing SNI, invalid hostnames, and fragmented prefix reads. |
| Complete | Shared metrics and tracing conventions. | `crates/proxy-core/src/observability` descriptor tests assert metric names, labels, and trace fields. |
| Complete | Hot-path latency budgets and benchmark harnesses for proxy primitives. | `crates/proxy-core/benches/proxy_primitives.rs`, `crates/frontline/benches/route_lookup.rs`, and `docs/proxy-hot-path-budgets.md` cover proxy primitives plus local route-key lookup budgets. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 1A: TCP stream proxy, active connection accounting, and drain tracker. | `tests/tcp_proxy.rs`, `src/accounting.rs`, and `src/drain.rs` cover the primitive behavior. |
| Complete | 1B: HTTP reverse proxy helpers and WebSocket upgrade/proxying. | `src/http.rs`, `tests/http_proxy.rs`, and `tests/websocket_proxy.rs` cover helper behavior and core forwarding paths. |
| Complete | 1C: Timeout, backpressure, structured shutdown, and cancellation behavior. | `src/timeout.rs`, `src/shutdown.rs`, and lifecycle tests cover typed timeout and cancellation; deeper reset/backpressure protocol cases remain done-criteria gaps below. |
| Complete | 1D: TLS ClientHello/SNI extraction helpers. | `src/tls.rs` tests cover fragmented and malformed ClientHello handling. |
| Complete | 1E: Shared metrics and tracing conventions. | Observability descriptor tests cover stable metric and trace field definitions. |
| Complete | 1F: Proxy hot-path latency budgets, benchmark harnesses, and allocation checks. | Primitive benches, allocation tests, hot-path budget docs, and the frontline route-key lookup benchmark are present. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Unit tests cover accounting, drain, timeout, and SNI parsing. | `src/accounting.rs`, `src/admission.rs`, `src/drain.rs`, `src/timeout.rs`, and `src/tls.rs` contain targeted tests. |
| Complete | Integration tests proxy TCP streams, HTTP requests, and WebSocket sessions. | `tests/tcp_proxy.rs`, `tests/http_proxy.rs`, and `tests/websocket_proxy.rs` cover these paths. |
| Complete | TCP tests cover byte preservation, half-close, upstream reset, client reset, timeout, and backpressure. | `tests/tcp_proxy.rs` covers byte preservation, half-close, connect errors, deterministic connect timeout, client disconnect, clean upstream disconnect, OS-level upstream reset, drain timeout while stalled, and bounded-duplex backpressure. |
| Complete | HTTP tests cover keep-alive, chunked/large/streaming bodies, HTTP/2 multiplexing, and cancellation. | `tests/http_proxy.rs` covers HTTP/1.1 keep-alive, chunked request bodies, large request/response bodies, streaming request bodies before client completion, streaming response lifecycle, cancellation while upstream is pending, and concurrent HTTP/2 streams. |
| Complete | WebSocket tests cover upgrade failure, bidirectional traffic, close frames, peer disconnect, and backpressure. | `tests/websocket_proxy.rs` covers upgrade failure, bidirectional traffic, close frames, client and upstream peer disconnect, large-frame forwarding, and true slow-downstream-peer backpressure over a bounded stream. |
| Complete | Drain tests prove new work is rejected while existing streams get the grace period. | `src/drain.rs`, `tests/http_proxy.rs`, and `tests/websocket_proxy.rs` cover drain rejection and grace-timeout behavior. |
| Complete | Hot-path benchmarks cover TCP, HTTP, WebSocket, TLS SNI, local route-key lookup, and admission/accounting. | `proxy_primitives` covers TCP, HTTP, WebSocket, TLS SNI, admission, and accounting; `route_lookup` covers local frontline route-key lookup. |
| Complete | Benchmark notes separate hot routing latency from cold wake latency and forbid control-plane calls on hot paths. | `docs/proxy-hot-path-budgets.md` documents hot-path budgets, smoke commands, and current limitations. |
| Complete | Allocation-sensitive tests or profiles exist for the hot path. | `crates/proxy-core/tests/allocation_hot_paths.rs` covers observability, accounting/admission, HTTP helpers, and TLS SNI parsing. |

## Milestone 2: Control Plane Resource Model

Introduce the durable model behind the control-plane API.

Scope:

- Domain-specific `ControlPlaneStore` trait for persistence operations and
  transactional invariants.
- Protobuf-defined control-plane API exposed over native gRPC and gRPC-Web for
  operator-facing methods.
- Postgres as the first store provider.
- Control-plane config for selecting the store provider.
- `WorkloadClass` with immutable versions.
- `Instance` pinned to a `WorkloadClass` version.
- `RouteBinding` for host, wildcard host, SNI, and optional path prefix.
- `Materialization` for active cluster projections.
- HTTP-01 challenge records keyed by `(host, token)`.
- Instance state machine with generation checks.
- Structured manifest rendering from `WorkloadClass + Instance.values`.

Sub-phases:

- 2A: Domain-specific `ControlPlaneStore` trait and provider config shape.
- 2B: Protobuf service definitions, native gRPC server, and gRPC-Web transport
  for operator-facing APIs.
- 2C: Postgres schema, migrations, and store implementation.
- 2D: `WorkloadClass` versioning, schema validation, and immutable version
  behavior.
- 2E: `Instance` APIs, value validation, generation fields, and idempotent
  create/update behavior.
- 2F: `RouteBinding` model, host/SNI/path/wildcard resolver, and uniqueness
  constraints.
- 2G: Instance state machine with generation/CAS transitions.
- 2H: HTTP-01 challenge store with put, resolve, delete, expiry, and GC.
- 2I: Structured manifest renderer for Deployment, StatefulSet, Service, PV,
  and PVC.

Done when:

- Database migrations and store tests pass against a real test database.
- Store conformance tests cover idempotency keys, transaction rollback,
  duplicate route/domain rejection, concurrent create/update conflicts, CAS
  generation failures, materialization generation updates, and provider config
  errors.
- The control plane can construct the configured store provider.
- Native gRPC and gRPC-Web integration tests exercise the same operator-facing
  APIs for workload classes, instances, route bindings, and HTTP-01 challenges.
- gRPC-Web tests cover CORS/preflight behavior when enabled, metadata/auth
  propagation, structured error mapping, and V8-compatible generated clients or
  request encoding.
- Tests document that proxy `Subscribe` is native gRPC-only in V1 and is not
  exposed as a gRPC-Web bidirectional stream.
- State-machine tests cover wake, running, draining, failed, retry, and delete.
- State-machine tests cover concurrent wake calls, sleep while waking, delete
  while waking or draining, failed wake retry, stale sidecar reports, and stale
  materialization updates.
- Route resolver tests cover host normalization, exact host versus wildcard
  precedence, wildcard specificity, longest path-prefix match, SNI/custom-domain
  uniqueness, and misses.
- HTTP-01 store tests cover put, overwrite/idempotency rules, wrong host/token,
  expiry, delete, and garbage collection.
- Manifest rendering tests cover Deployment, StatefulSet, Service, PV, and PVC.
- WorkloadClass version updates cannot mutate existing pinned instances.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Domain-specific `ControlPlaneStore` trait. | `crates/control-plane/src/store.rs` exposes domain operations for instances, workload classes, route bindings, materialization, dependencies, and HTTP-01 records. |
| Complete | Protobuf API over native gRPC and gRPC-Web for operator-facing methods. | `crates/control-plane/tests/api_transport.rs` covers native generated dispatch plus store-backed gRPC-Web HTTP/1.1 `application/grpc-web+proto` framed unary calls for workload class, instance, route binding, and HTTP-01 operator APIs. |
| Complete | Postgres as first store provider. | `crates/control-plane/src/postgres/`, `crates/control-plane/tests/postgres_store.rs`, and migrations implement the first provider. |
| Complete | Control-plane config for selecting the store provider. | `runtime.rs` parses `SLEEPYPODS_STORE_PROVIDER` and Postgres URL config, with runtime tests. |
| Complete | `WorkloadClass` with immutable versions. | Postgres conformance creates and reloads immutable versions and proves v2 does not mutate v1. |
| Complete | `Instance` pinned to a `WorkloadClass` version. | `create_instance` conformance covers pinned class version and value validation. |
| Complete | `RouteBinding` for host, wildcard host, SNI, and optional path prefix. | `postgres/route_ops.rs` resolver tests and conformance cover host/SNI/path matching, uniqueness, and misses. |
| Complete | `Materialization` for active cluster projections. | `postgres/materialization_ops.rs` and conformance cover record/load/complete materialization and backend generation checks. |
| Complete | HTTP-01 challenge records keyed by `(host, token)`. | `postgres/http01_ops.rs` and conformance cover put, resolve, wrong-key miss, repeated put, overwrite, delete, expiry, and GC. |
| Complete | Instance state machine with generation checks. | `instance.rs` transition tests and Postgres conformance cover CAS generation failures, stale sidecar reports, and stale materialization updates. |
| Complete | Structured manifest rendering from `WorkloadClass + Instance.values`. | `crates/control-plane/src/manifest/render.rs` and `manifest/tests.rs` cover templates, Deployment, StatefulSet, Service, PV, and PVC rendering. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 2A: Store trait and provider config shape. | `store.rs` defines the trait and `runtime.rs` parses provider config. |
| Complete | 2B: Protobuf services, native gRPC server, and gRPC-Web operator transport. | `server.rs` builds native gRPC and gRPC-Web operator transports; `api_transport.rs` covers store-backed parity, CORS preflight, metadata/auth header propagation, structured errors, and V8-compatible framed HTTP/1.1 requests. |
| Complete | 2C: Postgres schema, migrations, and store implementation. | Real Postgres conformance applies migrations idempotently and exercises store operations when `SLEEPYPODS_POSTGRES_URL` is set. |
| Complete | 2D: WorkloadClass versioning, schema validation, and immutability. | Conformance covers class version creation/load, schema validation rejects missing/unknown values, and version immutability. |
| Complete | 2E: Instance APIs, value validation, generation fields, and idempotent create/update behavior. | Store-backed API and conformance cover create/get/delete, generation fields, idempotent replay/conflict, and rollback. |
| Complete | 2F: RouteBinding model, resolver, and uniqueness constraints. | Resolver tests cover matching semantics; conformance covers route creation, duplicate rejection, and dependency lookup. |
| Complete | 2G: Instance state machine with generation/CAS transitions. | `instance.rs` and conformance cover legal/illegal transitions and CAS behavior. |
| Complete | 2H: HTTP-01 challenge store. | `http01_ops.rs` and conformance cover put, resolve, wrong-key miss, repeated put, overwrite, delete, expiry, and GC. |
| Complete | 2I: Structured manifest renderer. | `manifest/tests.rs` covers Deployment, StatefulSet, Service, PV, PVC, sidecar config, and validation failures. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Database migrations and store tests pass against a real test database. | `scripts/test-postgres-store.sh` sets `SLEEPYPODS_POSTGRES_URL` from either the caller environment or a disposable `postgres:17-alpine` container and runs `cargo test -p control-plane --test postgres_store -- --nocapture`; verified passing against a disposable real database. |
| Complete | Store conformance covers idempotency, rollback, duplicate routes, conflicts, CAS, materialization generations, and provider config errors. | `postgres_store.rs` conformance covers these cases, including invalid connection URL and transactional rollback after duplicate route identity. |
| Complete | Control plane can construct the configured store provider. | `runtime.rs` tests cover env parsing and provider construction paths. |
| Complete | Native gRPC and gRPC-Web integration tests exercise the same operator APIs. | `api_transport.rs` covers native store-backed dispatch and gRPC-Web store-backed unary calls for representative workload class, instance create/get/delete, route binding create/get/delete, and HTTP-01 put/resolve/delete flows. |
| Complete | gRPC-Web tests cover CORS/preflight, metadata/auth, structured errors, and V8-compatible clients/request encoding. | `api_transport.rs` covers CORS preflight, authorization/custom metadata header propagation through the gRPC-Web layer, store-backed `NotFound` status/message mapping, HTTP/1.1, `application/grpc-web+proto`, and framed protobuf unary bodies. |
| Complete | Tests document proxy `Subscribe` is native gRPC-only in V1 and not grpc-web bidi. | `api_transport.rs` tests assert operator grpc-web unary shape and no proxy Subscribe exposure. |
| Complete | State-machine tests cover wake, running, draining, failed, retry, and delete. | `instance.rs` and Postgres conformance cover lifecycle edges and failed retry/deleting terminal behavior. |
| Complete | State-machine tests cover concurrent wake, sleep while waking, delete while waking/draining, failed retry, stale sidecar reports, and stale materialization updates. | Postgres conformance includes concurrent CAS, invalid transitions, stale reports, and stale materialization rejection. |
| Complete | Route resolver tests cover normalization, wildcard precedence/specificity, path-prefix, SNI uniqueness, and misses. | `postgres/route_ops.rs`, `crates/frontline/src/identity.rs`, and matcher tests cover these semantics. |
| Complete | HTTP-01 store tests cover put, overwrite/idempotency, wrong host/token, expiry, delete, and GC. | `postgres_store_conformance_against_real_database` covers put, resolve, wrong-host miss, wrong-token miss, repeated put, overwrite, delete, expiry, and GC. |
| Complete | Manifest rendering tests cover Deployment, StatefulSet, Service, PV, and PVC. | `manifest/tests.rs` covers all listed object kinds and serialization. |
| Complete | WorkloadClass version updates cannot mutate existing pinned instances. | Conformance proves v2 creation does not change v1 and instances remain pinned to the requested version. |

The store trait should express domain operations rather than generic CRUD or a
generic SQL abstraction. It should include methods for:

- creating instances transactionally with route bindings and idempotency keys
- loading and validating pinned `WorkloadClass` versions
- resolving route identity to route entries
- compare-and-swap instance state transitions by generation
- recording materialization state and backend generation
- route resolution and dependency lookup for active proxy subscriptions
- HTTP-01 challenge put, resolve, delete, expiry, and GC

Start with Postgres only. Future providers such as MySQL should implement the
same trait behind control-plane config. Avoid making public store semantics rely
on Postgres-only behavior such as `LISTEN/NOTIFY`, partial indexes, or JSONB
querying unless there is a portable fallback.

Provider-specific durability semantics should not leak into the control-plane
API or proxy protocol. Database-specific features may be used as latency
optimizations inside one store provider, but correctness must come from portable
state, transactions, uniqueness constraints, idempotency keys, and generation
checks.

V1 route propagation should be lazy and subscription-based. `SubscribeRoute`
handles cache misses on the `Subscribe` stream: it resolves the identity,
registers the proxy as actively interested in the returned route entry, and
returns either `RouteResolved` with an opaque `subscription_id` or `RouteMiss`
with a negative-cache policy. The proxy stores the subscription ID with the local
cache entry and uses it only to apply targeted messages from `Subscribe`.

`Subscribe` should be a bidirectional stream. Proxy-to-control-plane messages
subscribe route identities or unsubscribe opaque subscription IDs;
control-plane-to-proxy messages return lookup results and deliver targeted
invalidations or updates. The control plane should create the subscription before
sending `RouteResolved`, so there is no separate resolve-then-add race in the
proxy protocol.

Polling, provider change streams, versions, ordering keys, and cursors are
control-plane internals. The store provider may use whatever mechanism fits its
durability model, but the proxy protocol must not expose or require a global
monotonic route version, durable global route preload, or resumable cursor.
Reconnect behavior should rebuild through lazy `SubscribeRoute`, not cursor-based
resync.

## Milestone 3: Frontline Route Resolution

Build the frontline-specific routing behavior on top of the shared proxy
primitives.

Scope:

- Host/SNI/path identity extraction.
- Canonical route key generation.
- Opaque subscription ID storage and invalidation handling.
- Exact host and SNI lookup.
- Wildcard host lookup.
- Longest path-prefix matching.
- Bounded local route cache and `Subscribe` subscribe/unsubscribe stream
  handling.
- `SubscribeRoute` fallback on local miss.
- `WakeInstance` flow when a route is Cold or missing a backend.
- Stale generation rejection.
- HTTP-01 challenge lookup through `ResolveHTTP01Challenge`.

Sub-phases:

- 3A: Route key normalization and local exact/wildcard/path-prefix matcher.
- 3B: Bounded local route cache, cache TTLs, and negative caching.
- 3C: `SubscribeRoute` cache-miss path, `RouteResolved`/`RouteMiss` handling,
  and opaque subscription ID storage.
- 3D: `Subscribe` bidirectional stream with `Unsubscribe` input and
  subscription-targeted invalidations.
- 3E: `WakeInstance` flow, Waking wait behavior, and stale generation
  rejection.
- 3F: HTTP/1.1, HTTP/2, h2c gRPC, and WebSocket forwarding.
- 3G: HTTPS termination, SNI certificate selection, and TLS/SNI passthrough.
- 3H: HTTP-01 challenge interception and `ResolveHTTP01Challenge` lookup.

Done when:

- Fake-control-plane integration tests cover cold wake, hot route, miss, stale
  generation, targeted route update, targeted invalidation, and stream
  reconnect.
- Route matching tests cover exact host precedence over wildcard, wildcard suffix
  specificity, longest path-prefix precedence, host case normalization, optional
  port handling, trailing-dot handling, and wildcard misses.
- Cache tests cover positive TTL expiry, negative TTL expiry, bounded eviction,
  `Unsubscribe` on eviction, refresh after invalidation, and stale backend
  rejection after generation changes.
- Subscription tests cover route subscription, miss responses without
  subscription IDs, unsubscribe, idempotent duplicate unsubscribe, and
  invalidation after resolve.
- Subscription tests cover duplicate in-flight `SubscribeRoute` requests for the
  same identity, route reassignment to another instance, invalidation during
  wake, and stream backpressure.
- Stream reconnect tests prove the proxy does not rely on public cursors and can
  resubscribe kept identities or rebuild stale cache entries through lazy
  `SubscribeRoute`.
- Protocol tests cover HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS
  termination, SNI passthrough, and HTTP-01.
- Protocol tests include chunked and large HTTP bodies, streaming request and
  response bodies, HTTP/2 multiplexing, h2c gRPC trailers and status propagation,
  WebSocket close/backpressure, TLS passthrough byte preservation, and TCP
  half-close behavior.
- PostgreSQL SNI passthrough tests use PostgreSQL/libpq 17+ with
  `sslnegotiation=direct` so the frontline proxy can route on standard TLS SNI
  without understanding the PostgreSQL startup protocol.
- TLS tests cover SNI certificate selection, missing certificate behavior, cert
  rotation, and passthrough for unknown or non-HTTP TLS traffic.
- HTTP-01 tests cover wrong host, wrong token, expired challenge, deleted
  challenge, response content type, and challenge path precedence before normal
  route resolution.
- Unknown Host/SNI negative caching protects the fake control plane from repeat
  misses.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Host/SNI/path identity extraction. | `crates/frontline/src/identity.rs` and `tls.rs` tests cover host normalization, optional ports, trailing dots, path defaults, and SNI canonicalization. |
| Complete | Canonical route key generation. | `identity.rs`, `matcher.rs`, and route resolver tests use canonical `RouteIdentity` values for cache and lookup. |
| Complete | Opaque subscription ID storage and invalidation handling. | `subscription.rs` tests cover storing subscription IDs, targeted invalidation, update replacement, stale update rejection, and idempotent unsubscribe. |
| Complete | Exact host and SNI lookup. | `matcher.rs` and Postgres route resolver tests cover exact host/SNI matches and misses. |
| Complete | Wildcard host lookup. | `matcher.rs` covers wildcard suffix matching and specificity. |
| Complete | Longest path-prefix matching. | `matcher.rs` covers longest path-prefix and segment-boundary behavior. |
| Complete | Bounded local route cache and `Subscribe` subscribe/unsubscribe stream handling. | `cache/tests.rs` and `resolver/tests.rs` cover TTLs, negative cache, eviction, unsubscribe on eviction, and pushed updates/invalidations. |
| Complete | `SubscribeRoute` fallback on local miss. | `resolver/tests.rs` covers positive/negative cache hits without calls and cache-miss subscribe behavior. |
| Complete | `WakeInstance` flow when a route is Cold or missing a backend. | `route/tests.rs` and `runtime/tests.rs` cover cold wake, waiting, running-without-backend wake, stale response retry, and errors. |
| Complete | Stale generation rejection. | `subscription.rs`, `cache/tests.rs`, and `route/tests.rs` reject stale instance/backend generations. |
| Complete | HTTP-01 challenge lookup through `ResolveHTTP01Challenge`. | `http01.rs`, `runtime.rs`, `listener.rs`, and `control_plane_transport.rs` tests cover challenge interception before route resolution, hit/miss/error behavior, listener wiring, and generated operator-client lookup. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 3A: Route key normalization and local matcher. | `identity.rs` and `matcher.rs` tests cover normalization, exact/wildcard precedence, and path-prefix matching. |
| Complete | 3B: Bounded cache, TTLs, and negative caching. | `cache/tests.rs` covers positive/negative TTL, bounded eviction, stale rejection, and reassignment. |
| Complete | 3C: SubscribeRoute cache miss and opaque subscription IDs. | `resolver/tests.rs` and `subscription.rs` cover resolved/miss handling and subscription ID storage. |
| Complete | 3D: Subscribe stream with Unsubscribe and targeted invalidations. | `control_plane_transport/tests.rs` and resolver tests cover unsubscribe input, pushed update, and pushed invalidation handling on the client side. |
| Complete | 3E: WakeInstance flow, Waking wait, and stale generation rejection. | `route/tests.rs` covers cold/running/waking/deleting states, wake failures, stale responses, and retry. |
| Complete | 3F: HTTP/1.1, HTTP/2, h2c gRPC, and WebSocket forwarding. | `forward/tests.rs` covers HTTP/1.1, HTTP/2 cleartext, h2c gRPC-shaped body/trailer forwarding, and WebSocket relay helpers; `listener/tests.rs` covers HTTP/1.1 ready-route forwarding, h2c prior-knowledge gRPC body/trailer preservation through route-cache resolution, cached ready WebSocket listener upgrade/route/relay with preserved path/query and bidirectional frames/close, and invalid/missing WebSocket Host rejection without control-plane calls. |
| Complete | 3G: HTTPS termination, SNI certificate selection, and TLS/SNI passthrough. | `config.rs` tests cover optional TLS listener env, static PEM loading, and invalid values/files; `bin/frontline.rs` now calls the shared multi-listener runtime with the env-loaded TLS certificate store; `listener/tests.rs` covers TLS termination through the wired listener to existing HTTP forwarding, SNI passthrough route resolution before backend connect, ClientHello prefix preservation, and malformed/missing SNI fail-closed behavior; `tls/tests.rs` still covers certificate selection/rotation, missing cert/SNI rejection, and passthrough helper behavior. |
| Complete | 3H: HTTP-01 interception and `ResolveHTTP01Challenge` lookup. | Frontline runtime and listener tests cover challenge-first routing behavior; transport tests cover `GrpcOperatorHttp01Resolver` hit, miss, status error, and malformed response handling. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Fake-control-plane tests cover cold wake, hot route, miss, stale generation, targeted update, invalidation, and stream reconnect. | Runtime/resolver/route tests cover cold/hot/miss/stale and pushed update/invalidation; `control_plane_transport.rs` drains actual Subscribe response stream close as a terminal event, and resolver tests prove that event invalidates active cached positives before TTL and lazily rebuilds on the next request. |
| Complete | Route matching tests cover precedence, wildcard specificity, path-prefix, normalization, ports, trailing dots, and misses. | `identity.rs` and `matcher.rs` cover these cases. |
| Complete | Cache tests cover positive/negative TTL, eviction, unsubscribe on eviction, refresh after invalidation, and stale backend rejection. | `cache/tests.rs` and `resolver/tests.rs` cover the listed cache behavior. |
| Complete | Subscription tests cover route subscription, miss without subscription ID, unsubscribe, duplicate unsubscribe, and invalidation after resolve. | `subscription.rs` and `resolver/tests.rs` cover these flows. |
| Complete | Subscription tests cover duplicate in-flight SubscribeRoute, reassignment, invalidation during wake, and stream backpressure. | Reassignment, duplicate resolved responses, invalidation during wake, and response-buffer backpressure are covered; `listener.rs` has a deterministic shared-coordinator test proving duplicate outstanding SubscribeRoute calls for the same identity cannot occur through the serialized listener route-resolution path. |
| Complete | Stream reconnect tests prove lazy resubscribe/rebuild without public cursors. | `control_plane_transport.rs` proves a closed Subscribe stream is dropped and the next SubscribeRoute opens a fresh stream, and drains actual post-response stream close as a terminal event; resolver tests prove stream-close invalidation triggers lazy resubscribe/rebuild without public cursors. |
| Complete | Protocol tests cover HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS termination, SNI passthrough, and HTTP-01. | Helper/component tests cover pieces; `scripts/test-kind-e2e-routing.sh` covers full-platform HTTP/1.1 route identity and HTTP-01 challenge behavior through deployed components; `scripts/test-kind-e2e-protocols.sh` covers full-platform HTTP/2, h2c gRPC-shaped request/body plus `grpc-status`/`grpc-message` trailers, and WebSocket text/binary/clean-close behavior through deployed production control-plane, frontline, sidecar, Postgres, and Kubernetes API; `scripts/test-kind-e2e-tls.sh` covers full-platform TLS termination and exact/wildcard SNI passthrough through deployed production control-plane, frontline, sidecar, Postgres, and Kubernetes API; `scripts/test-kind-e2e-libpq-sni.sh` covers real PostgreSQL/libpq 17 direct TLS negotiation through SNI passthrough. |
| Complete | Protocol tests include large/streaming HTTP, HTTP/2 multiplexing, gRPC trailers/status, WebSocket backpressure, TLS passthrough bytes, and TCP half-close. | `http_proxy.rs` covers large request/response bodies, streaming request and response bodies, and concurrent HTTP/2 streams; `forward/tests.rs` and `listener/tests.rs` cover h2c gRPC body plus `grpc-status`/`grpc-message` trailers; `websocket_proxy.rs` covers bounded downstream backpressure and large binary frame forwarding; `tls/tests.rs` covers passthrough ClientHello prefix plus tail preservation; `tcp_proxy.rs` covers reverse bytes after client half-close. |
| Complete | PostgreSQL/libpq 17+ SNI passthrough with `sslnegotiation=direct`. | `scripts/test-kind-e2e-libpq-sni.sh` builds and loads production `control-plane`, `frontline`, and `sidecar` images, deploys real Postgres for the control-plane store, creates a TLS-enabled Postgres workload through WorkloadClass/Instance/SNI RouteBinding APIs, and runs pinned Postgres/libpq 17 `psql` Jobs proving `sslnegotiation=direct` reaches the marker table through the frontline SNI passthrough listener while an unbound SNI host fails. |
| Complete | TLS tests cover certificate selection, missing certificate, cert rotation, and passthrough for non-HTTP TLS traffic. | `crates/frontline/src/tls.rs` helper tests cover canonical certificate selection/rotation, missing SNI/cert rejection, and passthrough prefix preservation. |
| Complete | HTTP-01 tests cover wrong host/token, expired/deleted challenge, content type, and precedence. | `http01.rs`, `runtime.rs`, `control_plane_transport.rs`, and store tests cover challenge matching, invalid host/token, wrong-key miss, miss/expiry/delete behavior, content type, generated client lookup, and challenge-path precedence. |
| Complete | Unknown Host/SNI negative caching protects fake control plane from repeat misses. | `cache/tests.rs` and `resolver/tests.rs` cover negative cache hits and expiry without repeat control-plane calls. |

## Milestone 4: Sidecar Idle Proxy

Build the sidecar-specific local proxy and idle reporting behavior.

Scope:

- Local forwarding to `127.0.0.1:<app-port>`.
- HTTP and TCP forwarding modes.
- Active request and connection tracking.
- Idle detection.
- Drain handling.
- `ReportIdle` control-plane call.
- Graceful shutdown behavior when the workload is being deleted.

Sub-phases:

- 4A: Local HTTP forwarding to `127.0.0.1:<app-port>`.
- 4B: Local TCP forwarding to `127.0.0.1:<app-port>`.
- 4C: Active request and connection tracking.
- 4D: Idle detection and `ReportIdle` call.
- 4E: Drain and graceful shutdown behavior.

Done when:

- Integration tests prove active HTTP requests, h2c gRPC streams, WebSockets, and
  raw TCP connections prevent idle reporting.
- Idle tests prove `ReportIdle` fires after the configured timeout only after all
  active requests and connections close.
- Idle/report tests cover duplicate reports, stale generation reports, control
  plane rejection, retry/backoff, and sidecar restart.
- Drain tests prove the sidecar stops accepting new work and lets active work
  finish within the grace period.
- Drain tests cover SIGTERM, upstream failure, client disconnect, grace expiry
  with active streams, and forced shutdown after the hard deadline.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Local forwarding to `127.0.0.1:<app-port>`. | `crates/sidecar/src/tests.rs` and `runtime/tests.rs` cover forwarding to loopback upstreams. |
| Complete | HTTP and TCP forwarding modes. | `bin/sidecar.rs` selects `http` or `tcp`; component/runtime tests cover both modes. |
| Complete | Active request and connection tracking. | Sidecar tests hold HTTP bodies/TCP streams open and assert active count behavior. |
| Complete | Idle detection. | `idle.rs` tests cover no-active timeout, active work suppression, timer reset, and non-duplicated successful reports. |
| Complete | Drain handling. | Sidecar tests cover rejecting new HTTP/TCP work, waiting for active work, grace timeout, and shutdown-triggered drain. |
| Complete | `ReportIdle` control-plane call. | `idle/control_plane.rs` and `control_plane_transport/tests.rs` cover accepted, already-draining, stale generation, unavailable, and transport errors. |
| Complete | Graceful shutdown behavior when the workload is being deleted. | `runtime/tests.rs` covers shutdown stop-accepting behavior, active request wait, and timeout return. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 4A: Local HTTP forwarding. | Sidecar HTTP tests cover request/response forwarding and active request accounting. |
| Complete | 4B: Local TCP forwarding. | Sidecar TCP tests cover byte forwarding and active connection accounting. |
| Complete | 4C: Active request and connection tracking. | Component tests cover active HTTP body and open TCP stream tracking. |
| Complete | 4D: Idle detection and `ReportIdle` call. | `idle.rs` and `idle/control_plane.rs` cover timeout, retry, terminal outcomes, and active-work reset. |
| Complete | 4E: Drain and graceful shutdown behavior. | Component/runtime tests cover drain rejection, grace timeout, and shutdown-triggered drain. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Active HTTP requests, h2c gRPC streams, WebSockets, and raw TCP connections prevent idle reporting. | `runtime/tests.rs` covers HTTP, raw TCP, h2c gRPC-shaped streams, and WebSocket sessions delaying idle reports until active work closes. |
| Complete | Idle tests prove `ReportIdle` fires after timeout only after active work closes. | `idle.rs` covers active work suppression and reset before reporting. |
| Complete | Idle/report tests cover duplicate reports, stale generation, control-plane rejection, retry/backoff, and sidecar restart. | Duplicate reports, stale generation, rejection/unavailable outcomes, retry/backoff, and detector reconstruction are covered; `sidecar_process_lifecycle.rs` starts the real `sidecar` binary against a fake gRPC control plane, terminates it with SIGTERM, restarts it on the same port, and verifies it listens again without stale state. |
| Complete | Drain tests prove the sidecar stops accepting new work and lets active work finish within the grace period. | `src/tests.rs` and `runtime/tests.rs` cover drain rejection and active-work waiting. |
| Complete | Drain tests cover SIGTERM, upstream failure, client disconnect, grace expiry with active streams, and forced shutdown after hard deadline. | Shutdown-triggered drain, grace expiry, upstream HTTP disconnect, and TCP client disconnect are covered; the Unix process lifecycle tests send OS SIGTERM to the real binary and assert a hanging active request exits after `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS` with the expected drain-timeout failure status. |

## Milestone 5: Kubernetes Materializer

Render and reconcile active Kubernetes objects from control-plane state.

Scope:

- Render PV, PVC, Service, Deployment, and StatefulSet objects.
- Apply PV/PVC before workloads and wait for PVC Bound.
- Validate PV/PVC specs for names, labels, access modes, capacity, reclaim
  policy, volume source, instance value substitution, and intended binding.
- Validate workload volume mounts point at the rendered PVC and expected mount
  path.
- Inject sidecar into rendered pod templates.
- Service targets the sidecar port.
- Sidecar targets the local app port.
- Validate Service selectors and target ports route to the sidecar, and sidecar
  upstream configuration can only route to the local app port.
- Wait for readiness through Pods or EndpointSlices.
- Delete materialized objects on sleep/delete.
- Leave backing provider volumes untouched.

Sub-phases:

- 5A: Structured object rendering for PV, PVC, Service, Deployment, and
  StatefulSet.
- 5B: Kubernetes apply/update/delete client and ownership labels.
- 5C: PV/PVC spec correctness, intended binding, apply order, and PVC Bound
  wait.
- 5D: Deployment/StatefulSet rendering with sidecar injection.
- 5E: Service rendering that targets the sidecar port.
- 5F: Readiness detection through Pods or EndpointSlices.
- 5G: Sleep/delete cleanup for workloads, Services, PVCs, and PVs.
- 5H: kind materialization lifecycle suite with a static volume backend.

Done when:

- Component tests validate rendered objects and ordering.
- Component tests validate Service selector/targetPort wiring, sidecar container
  args/env for the local upstream, and that the generated config cannot proxy
  back to the Service that targets the sidecar.
- Component tests validate PV/PVC rendered fields, binding selectors or
  `volumeName`, ownership/generation labels, and workload volume mounts.
- kind tests prove cold wake creates PV/PVC before StatefulSet and the PVC binds
  to the intended PV.
- kind tests prove the workload can read/write at the expected mount path.
- kind tests prove sleep deletes workload, Service, PVC, and PV.
- kind tests prove re-wake recreates manifests from the same instance values and
  preserves data when using the same backing static volume.
- Failure tests cover missing or bad volume handles, PVCs that never bind, wrong
  access modes, and stale manifest generation.
- Readiness tests prove routes are not published until Pods or EndpointSlices are
  ready, and are withdrawn when the materialization becomes unready.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Render PV, PVC, Service, Deployment, and StatefulSet objects. | `crates/control-plane/src/manifest/tests.rs` covers all rendered object kinds and serialization. |
| Complete | Apply PV/PVC before workloads and wait for PVC Bound. | `materializer.rs` tests apply StatefulSet manifests with PVC-bound wait before Service/workload. |
| Complete | Validate PV/PVC specs, source, substitution, and intended binding. | Manifest tests cover names, labels, access modes, capacity, reclaim policy, hostPath source, `volumeName`, and template substitution failures. |
| Complete | Validate workload volume mounts point at rendered PVC and expected mount path. | Manifest tests cover rendered volume mounts for StatefulSet volume templates. |
| Complete | Inject sidecar into rendered pod templates. | Manifest render tests verify sidecar container/env wiring. |
| Complete | Service targets the sidecar port. | Manifest tests validate service port and target port wiring. |
| Complete | Sidecar targets the local app port. | Manifest tests validate sidecar local upstream env and reject app target conflicts. |
| Complete | Validate Service selectors/target ports and local-only sidecar upstream. | Manifest validation rejects invalid service ports and sidecar/app port conflicts. |
| Complete | Wait for readiness through Pods or EndpointSlices. | `kube_materializer.rs` and `materializer.rs` tests cover EndpointSlice readiness and backend URI creation. |
| Complete | Delete materialized objects on sleep/delete. | Component paths now clean recorded Kubernetes refs before finalizing state: `sidecar_api_transport.rs` covers `ReportIdle` sleep cleanup, and `api_transport.rs` covers operator delete cleanup success, no-active-materialization, target filtering, stale active materialization generation handling, failure preservation, and retry. `scripts/test-kind-e2e-stateful.sh` also proves deployed `ReportIdle` cleanup and deployed operator delete of a Running instance remove the rendered StatefulSet, Service, PVC, and PV. |
| Complete | Leave backing provider volumes untouched. | `scripts/test-kind-materializer.sh` deletes rendered objects and rematerializes with preserved hostPath data. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 5A: Structured object rendering. | Manifest tests cover PV, PVC, Service, Deployment, and StatefulSet rendering. |
| Complete | 5B: Kubernetes apply/update/delete client and ownership labels. | `materializer.rs` and `kube_materializer.rs` tests cover apply/delete refs and ownership/generation labels. |
| Complete | 5C: PV/PVC correctness, intended binding, apply order, and PVC Bound wait. | Manifest and materializer tests cover validation, apply ordering, and bound wait failures. |
| Complete | 5D: Deployment/StatefulSet rendering with sidecar injection. | Manifest tests cover both workload kinds and injected sidecar config. |
| Complete | 5E: Service rendering that targets the sidecar port. | Manifest tests cover service selectors and sidecar target port. |
| Complete | 5F: Readiness through Pods or EndpointSlices. | Kube materializer readiness tests cover ready EndpointSlice semantics. |
| Complete | 5G: Sleep/delete cleanup for workloads, Services, PVCs, and PVs. | Sleep cleanup is wired through sidecar `ReportIdle`; operator delete cleanup now receives materializer/target context and deletes recorded refs before store deletion, with component tests for success, no active materialization, target filtering, stale active materialization generation handling, failure preservation, and retry. `scripts/test-kind-e2e-stateful.sh` covers the same cleanup through deployed production images, Postgres, and Kubernetes for StatefulSet/Service/PVC/PV objects. |
| Complete | 5H: kind materialization lifecycle suite with static volume backend. | `scripts/test-kind-materializer.sh` runs the ignored kind materializer lifecycle test with static hostPath data continuity. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Component tests validate rendered objects and ordering. | `manifest/tests.rs` and `materializer.rs` cover object shape and apply order. |
| Complete | Component tests validate Service wiring, sidecar local upstream env, and no proxy-back-to-Service config. | Manifest tests cover Service target port, sidecar upstream env, and reject conflicting app/sidecar ports. |
| Complete | Component tests validate PV/PVC fields, labels, and volume mounts. | Manifest tests cover PV/PVC fields, owner/generation labels, and workload mounts. |
| Complete | kind tests prove cold wake creates PV/PVC before StatefulSet and PVC binds intended PV. | `scripts/test-kind-e2e-stateful.sh` runs `kind_e2e_stateful.rs` through deployed production control-plane/frontline/sidecar images and asserts PV/PVC/StatefulSet resource-version ordering plus PVC `Bound` status and `volumeName` binding to the intended PV. |
| Complete | kind tests prove workload can read/write expected mount path. | `scripts/test-kind-materializer.sh` writes and verifies a marker through the mounted hostPath volume. |
| Complete | kind tests prove sleep deletes workload, Service, PVC, and PV. | `scripts/test-kind-e2e-stateful.sh` proves deployed sidecar `ReportIdle` returns the StatefulSet instance to `Cold` and deletes the rendered StatefulSet, Service, PVC, and PV. |
| Complete | kind tests prove re-wake recreates manifests from same values and preserves static-volume data. | `scripts/test-kind-materializer.sh` rematerializes the manifest and verifies the previous marker remains; `scripts/test-kind-e2e-stateful.sh` re-wakes through the deployed frontline and reads the marker previously written to the static hostPath-backed PVC. |
| Complete | Failure tests cover missing/bad volume handles, PVCs never bind, wrong access modes, and stale manifest generation. | Manifest tests reject missing/empty CSI volume handles plus unsupported/duplicate access modes; materializer tests cover PVC bind failures and stale generation labels/annotations being rejected before apply. |
| Complete | Readiness tests prove routes are not published until ready and withdrawn when unready. | `proxy_api_transport.rs` proves `SubscribeRoute` resolves the route while publishing a backend only from a ready materialization for the configured target/current instance generation, and withholds/withdraws backend publication for no materialization, pending, failed, deleting, deleted, stale generation, and wrong target. This is component/API evidence only; subscribed push update/invalidation delivery and full-platform kind proof remain tracked in M9. |

## Milestone 6: End-to-End V1

Wire the control plane, frontline proxy, sidecar, and materializer together.

Scope:

- Create instance through control-plane API.
- Resolve route lazily through the frontline proxy.
- Cold request wakes instance.
- Hot request routes from local cache.
- Sidecar reports idle.
- Control plane drains and sleeps materialization.
- Custom host and wildcard host route to the right instance.
- HTTP-01 challenge insert, resolve, serve, and delete flow works.

Sub-phases:

- 6A: Stateless Deployment cold wake, hot route, idle drain, sleep, and re-wake.
- 6B: StatefulSet with static PV/PVC templates cold wake, hot route, mounted
  write/read, idle drain, sleep, and re-wake with data continuity.
- 6C: Custom host, wildcard host, SNI, and optional path-prefix routing.
- 6D: Protocol matrix: HTTP/1.1, HTTP/2, h2c gRPC, gRPC-Web control-plane
  access, WebSockets, TLS termination, SNI passthrough, and
  PostgreSQL/libpq 17+ over SNI with `sslnegotiation=direct`.
- 6E: HTTP-01 insert, resolve, serve, delete, and expired-token behavior.
- 6F: Failure-path matrix: wake timeout, bad route, missing PVC binding, bad
  volume template, stale proxy generation, and control-plane restart.
- 6G: Lifecycle race matrix: concurrent wake calls, sleep while waking, delete
  while waking, delete while draining, failed wake retry, stale sidecar report,
  and route reassignment with active proxy subscriptions.

Done when:

- kind E2E passes for stateless Deployment.
- kind E2E passes for StatefulSet with static PV/PVC templates, intended binding,
  mounted write/read, sleep, re-wake, and data continuity.
- kind E2E passes for HTTP/1.1, HTTP/2, h2c gRPC, gRPC-Web control-plane access,
  WebSockets, TLS termination, and SNI passthrough.
- kind E2E proves a real PostgreSQL/libpq 17+ deployment can connect through
  TLS/SNI passthrough with `sslnegotiation=direct`, using a pinned Postgres
  image in CI.
- Failure-path E2E covers wake timeout, bad route, missing PVC binding, bad
  volume template, and stale proxy generation.
- Lifecycle-race E2E proves generation checks prevent stale sidecar reports,
  stale materializations, and stale proxy cache entries from changing current
  instance state.
- Control-plane restart E2E covers restart during wake, sleep, delete, route
  reassignment, and HTTP-01 challenge handling.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Create instance through control-plane API. | `scripts/test-kind-e2e-stateless.sh` deploys the real control-plane image against Postgres and Kubernetes; `kind_e2e_stateless.rs` calls `CreateWorkloadClassVersion`, `CreateInstance`, and `CreateRouteBinding` through the deployed operator gRPC API. |
| Complete | Resolve route lazily through the frontline proxy. | `kind_e2e_stateless.rs` sends a Host-based request through the deployed frontline, which resolves the route through the deployed control plane and reaches the materialized sidecar/app. |
| Complete | Cold request wakes instance. | `kind_e2e_stateless.rs` proves the first frontend request wakes a Cold stateless instance to Running and materializes the Deployment and Service in kind. |
| Complete | Hot request routes from local cache. | `kind_e2e_stateless.rs` sends a second frontend request and asserts deployed frontline logs show exactly one successful `SubscribeRoute` call plus at least one route-cache hit. |
| Complete | Sidecar reports idle. | `kind_e2e_stateless.rs` uses a short WorkloadClass sleep policy and waits for the deployed sidecar `ReportIdle` path to return the instance to `Cold` and delete the rendered Deployment/Service; `kind_e2e_stateful.rs` covers the same deployed path for StatefulSet/Service/PVC/PV cleanup. |
| Complete | Control plane drains and sleeps materialization. | `sidecar_api_transport.rs` covers `ReportIdle` beginning sleep, deleting rendered Kubernetes object refs through the materializer, marking the materialization deleted, and returning the instance to `Cold`; `kind_e2e_stateful.rs` verifies that full-platform sleep deletes rendered StatefulSet, Service, PVC, and PV objects; `wake::tests` covers the Draining/deleting wake guard. |
| Complete | Custom host and wildcard host route to right instance. | `scripts/test-kind-e2e-routing.sh` runs a full-platform kind gate with production control-plane, frontline, sidecar, Postgres, and Kubernetes API; `kind_e2e_routing.rs` creates exact and wildcard RouteBindings through the deployed operator API, sends HTTP/1.1 traffic through the deployed frontline, verifies unique backend response bodies for the intended instances, and proves the wildcard base/unrelated hosts do not match. |
| Complete | HTTP-01 insert, resolve, serve, and delete flow works. | `scripts/test-kind-e2e-routing.sh` runs the deployed control-plane/frontline/Postgres path; `kind_e2e_routing.rs` inserts HTTP-01 challenges through the operator API, resolves/serves them through `/.well-known/acme-challenge/{token}` on a host with a normal route, verifies challenge precedence and content type, deletes the challenge, and verifies it no longer serves. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 6A: Stateless Deployment cold wake, hot route, idle drain, sleep, and re-wake. | `scripts/test-kind-e2e-stateless.sh` runs the full-platform stateless lifecycle in kind, including cold wake, hot-cache request, deployed sidecar idle sleep, Deployment/Service deletion, and re-wake. |
| Complete | 6B: StatefulSet static PV/PVC lifecycle with data continuity. | `scripts/test-kind-e2e-stateful.sh` runs the deployed control-plane, frontline, sidecar, Postgres, and Kubernetes API; `kind_e2e_stateful.rs` creates the WorkloadClass/Instance/RouteBinding through the operator API, writes and reads a marker through the frontline-mounted PVC path, verifies idle sleep cleanup, re-wakes, and reads the preserved marker. Hot-cache metrics remain scoped to the stateless gate. |
| Complete | 6C: Custom host, wildcard host, SNI, and path-prefix routing. | `scripts/test-kind-e2e-routing.sh` covers custom exact host, wildcard host, wildcard misses, and path-prefix routing including `/api`, `/api/child`, and `/apix` boundary behavior through deployed production components; `scripts/test-kind-e2e-tls.sh` covers exact SNI passthrough, wildcard SNI passthrough, wildcard-base miss, and SAN-covered unbound SNI miss through deployed production control-plane, frontline, sidecar, Postgres, and Kubernetes API. |
| Complete | 6D: Full protocol matrix, including PostgreSQL/libpq SNI passthrough. | Helper/component tests cover pieces, including gRPC-Web operator transport; `scripts/test-kind-e2e-protocols.sh` covers full-platform HTTP/2, h2c gRPC-shaped trailers/status propagation, and WebSockets; `scripts/test-kind-e2e-tls.sh` covers full-platform TLS termination and SNI passthrough; `scripts/test-kind-e2e-grpc-web.sh` covers the deployed control-plane grpc-web browser-shaped operator path; `scripts/test-kind-e2e-libpq-sni.sh` covers real PostgreSQL/libpq 17 `sslnegotiation=direct` through SNI passthrough using a pinned test image. |
| Complete | 6E: HTTP-01 insert, resolve, serve, delete, and expired-token behavior. | `scripts/test-kind-e2e-routing.sh` runs a full-platform deployed HTTP-01 gate; `kind_e2e_routing.rs` puts challenges through the operator API, verifies operator resolve and deployed frontline serving, confirms challenge-path precedence over a normal route, deletes a challenge and verifies it no longer serves, then inserts a short-lived challenge and verifies it stops serving after expiry before `ExpireHttp01Challenges` removes it. |
| Complete | 6F: Failure-path matrix. | `scripts/test-kind-e2e-failures.sh` covers full-platform wake/readiness timeout, bad route miss, missing PVC binding, bad volume template rejection, and stale proxy generation rejection through deployed production control-plane/frontline/sidecar images, real Postgres, and the real Kubernetes API. `scripts/test-kind-e2e-restart.sh` adds full-platform restart evidence for wake, sleep, delete, HTTP-01 recovery, and route reassignment after control-plane restart. |
| Complete | 6G: Lifecycle race matrix. | `scripts/test-kind-e2e-lifecycle-races.sh` passed on 2026-06-24 and covers full-platform concurrent wake convergence, `ReportIdle` while waking rejection, delete while waking cleanup after pending StatefulSet/Service/PVC/PV refs are recorded, delete while Draining/deleting materialization cleanup of StatefulSet/Service/PVC/PV objects with no stale backend, failed wake retry stale-generation rejection, stale sidecar `ReportIdle` rejection, and active proxy subscription invalidation plus subsequent new-backend routing after route reassignment through deployed production control-plane/frontline/sidecar images, real Postgres, and the real Kubernetes API. Focused wake/materializer unit coverage now proves rendered refs are derived without Kubernetes I/O, `WakeInstance` records durable Pending refs before first apply, pending-record failures apply nothing, post-Pending delete races clean refs applied by the racing wake before returning readiness/complete failures, and PVC-bound failure leaves Pending PV/PVC/Service/workload refs with no backend for delete/retry cleanup. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | kind E2E passes for stateless Deployment. | `scripts/test-kind-e2e-stateless.sh` passed on 2026-06-23 using real production control-plane, frontline, and sidecar images, a real Postgres dependency, and the real Kubernetes API. |
| Complete | kind E2E passes for StatefulSet with PV/PVC, mounted IO, sleep, re-wake, and data continuity. | `scripts/test-kind-e2e-stateful.sh` passed on 2026-06-23 using real production control-plane, frontline, and sidecar images, real Postgres, and the real Kubernetes API; the gate verifies PV/PVC binding, mounted marker write/read, `ReportIdle` cleanup, re-wake, data continuity, and deployed operator delete cleanup of active StatefulSet/Service/PVC/PV objects. |
| Complete | kind E2E passes for HTTP/1.1, HTTP/2, h2c gRPC, grpc-web, WebSockets, TLS termination, and SNI passthrough. | `scripts/test-kind-e2e-routing.sh` covers full-platform HTTP/1.1 route identity and HTTP-01 behavior; `scripts/test-kind-e2e-protocols.sh` covers full-platform HTTP/2, h2c gRPC-shaped response body plus trailers, and WebSockets; `scripts/test-kind-e2e-tls.sh` covers full-platform TLS termination and exact/wildcard SNI passthrough; `scripts/test-kind-e2e-grpc-web.sh` builds and loads the production control-plane image, deploys it with real Postgres into kind, enables the grpc-web listener, and `kind_e2e_grpc_web.rs` verifies browser-shaped HTTP/1.1 CORS preflight plus grpc-web framed store-backed operator create/get/delete/not-found behavior; `scripts/test-kind-e2e-libpq-sni.sh` covers real PostgreSQL/libpq 17 direct TLS negotiation through SNI passthrough. |
| Complete | kind E2E proves real PostgreSQL/libpq 17+ SNI passthrough with pinned image. | `scripts/test-kind-e2e-libpq-sni.sh` passed on 2026-06-24 using production control-plane/frontline/sidecar images, real Postgres for the control-plane store, a test-only TLS Postgres workload image based on exact `postgres:17.5-alpine3.22`, WorkloadClass/Instance/SNI RouteBinding APIs, and in-cluster `psql` Jobs with `sslnegotiation=direct`; the positive query returned `sleepypods-libpq-sni`, and the unbound SNI host failed. |
| Complete | Failure-path E2E covers wake timeout, bad route, missing PVC binding, bad volume template, and stale proxy generation. | `scripts/test-kind-e2e-failures.sh` builds and loads production `control-plane`, `frontline`, and `sidecar` images, deploys real Postgres/control-plane/frontline into kind, and `kind_e2e_failures.rs` uses deployed operator/proxy APIs plus Kubernetes assertions to prove an unbound host returns a miss without waking an unrelated instance, an image-pull/readiness failure fails wake in bounded time without publishing a backend, a deliberately unbound duplicate-PV PVC path fails before Service/StatefulSet backend publication, an invalid volume template is rejected by the operator API with `InvalidArgument`, and a stale proxy `WakeInstance` generation is rejected with a structured generation conflict after the failed wake advances the instance generation. |
| Complete | Lifecycle-race E2E prevents stale sidecar, materialization, and proxy cache updates. | `scripts/test-kind-e2e-lifecycle-races.sh` passed on 2026-06-24 using production `control-plane`, `frontline`, and `sidecar` images plus real Postgres and Kubernetes API; it proves stale sidecar reports are rejected, pending materialization refs recorded during wake allow delete-while-waking cleanup of StatefulSet/Service/PVC/PV objects, delete while Draining/deleting materialization cleans StatefulSet/Service/PVC/PV objects and serves no stale backend, stale failed-wake generations cannot publish after a later successful wake, and active route subscriptions invalidate/re-resolve on reassignment before subsequent requests reach the new backend. Unit coverage now proves rendered refs are durable before Kubernetes apply/PVC-bound/readiness waits, pending-record failures apply nothing, post-Pending delete races trigger cleanup of refs applied by the losing wake, and PVC-bound failures keep a Pending active materialization with all rendered refs and no backend for later delete or retry. |
| Complete | Control-plane restart E2E covers restart during wake, sleep, delete, route reassignment, and HTTP-01. | `scripts/test-kind-e2e-restart.sh` passed on 2026-06-24 using production `control-plane`, `frontline`, and `sidecar` images plus `postgres:17-alpine` and test workload images, deployed real Postgres/control-plane/frontline into kind, and `kind_e2e_restart.rs` proved wake retry from durable `Waking` state, sleep cleanup after `ReportIdle` during control-plane unavailability, delete cleanup retry, HTTP-01 put/serve/resolve/delete/expiry, and immediate post-restart route reassignment without serving the stale old backend. |

## Milestone 7: Hardening

Make V1 operationally credible.

Scope:

- Metrics and tracing for wake latency, route-cache hits, control-plane calls,
  drain duration, active streams, and materialization failures.
- Structured logs with instance ID, route ID, generation, and cluster.
- Backoff and retry policies.
- Proxy `Subscribe` stream reconnect and cache rebuild through lazy
  `SubscribeRoute`, without proxy-visible versions or cursors.
- Control-plane restart recovery from database state.
- Minimal production images for the control plane, frontline proxy, and sidecar.
- Load tests for route lookup and hot proxy path, including request rate,
  streaming throughput, and tail latency against same-environment direct-backend
  baselines.
- Soak tests for repeated wake/sleep cycles.

Sub-phases:

- 7A: Metrics, tracing, and structured log fields.
- 7B: Control-plane restart recovery during wake, sleep, and delete.
- 7C: Proxy `Subscribe` reconnect, lazy cache rebuild, and stale backend
  recovery.
- 7D: Minimal final images and container runtime smoke tests.
- 7E: Load tests for route lookup, hot proxy path, HTTP request rate,
  h2/h2c/gRPC behavior, TCP throughput, and WebSocket throughput.
- 7F: kind wake/sleep soak tests and leaked-object detection.
- 7G: Operator-facing runbook and metric name documentation.

Milestone 7F starts with the existing materializer lifecycle kind test as a
small soak target:

- `./scripts/test-kind-materializer.sh` runs the single disposable-cluster
  check.
- `SLEEPYPODS_KIND_SOAK_ITERATIONS=3 ./scripts/soak-kind-materializer.sh`
  reuses one kind cluster across repeated runs.
- `SLEEPYPODS_KIND_SOAK_ITERATIONS=2 ./scripts/soak-kind-full-wake-sleep.sh`
  reuses one kind cluster across repeated full-platform stateless Deployment
  and StatefulSet/PV/PVC wake/sleep runs.

The soaks fail fast on the first failed iteration. The materializer soak waits
for leaked `sleepypods.io/kind-test=true` namespaces and
`sleepypods.io/instance-id=kind-materializer` PersistentVolumes to disappear
after each successful iteration. The full wake/sleep soak waits after every
stateless and stateful cycle for leaked test namespaces, materialized workloads,
Services, PVCs, PVs, and stateful cluster RBAC objects to disappear.

Done when:

- Automated tests cover restart during wake, sleep, and delete.
- Metrics tests assert key counters/histograms and labels for wake latency,
  route-cache hits/misses, subscribe stream events, invalidations, active
  streams, drain duration, materialization failures, and HTTP-01 results.
- Structured log tests or golden assertions cover instance ID, route ID,
  subscription ID where relevant, generation, cluster, namespace, and error
  reason on important lifecycle paths.
- Repeated kind wake/sleep soak passes without leaked Kubernetes objects.
- Load-test targets for route lookup, hot proxy path, and cold wake latency are
  defined before 7D starts, and tests fail if those targets regress.
- Proxy load tests run against the same production images used by kind E2E and
  compare against direct-backend baselines from the same test environment.
- Hot-cache HTTP/1.1 request rate stays within 20% of direct-backend baseline.
- Hot-cache h2, h2c, and gRPC request rate stays within 25% of direct-backend
  baseline.
- TCP large-stream throughput stays within 10-15% of direct-backend baseline.
- WebSocket streaming throughput stays within 15-20% of direct-backend baseline.
- Hot-cache p99 added latency stays below a documented absolute budget where
  stable, or within 25% of direct-backend baseline where timing is noisy.
- Once baseline numbers are established, benchmark regressions warn above
  10-15% and fail above 20-25% unless the change explicitly updates the
  accepted budget.
- Hot-cache route handling makes zero control-plane calls under load.
- Route lookup and hot proxy path meet target latency under load.
- Retry/backoff tests cover transient database errors, Kubernetes API conflicts,
  proxy stream disconnects, and materializer reconcile retries.
- Container tests prove the final control-plane, frontline proxy, and sidecar
  images start successfully, run as non-root, include only required runtime
  files, can access required CA certificates, and are the images used by kind E2E
  and soak tests.
- Image-size budgets for the three production images are defined before 7D
  starts, and tests or CI checks fail if they regress without an explicit update.
- Dashboards or metric names are documented enough for operators to wire up.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Metrics and tracing for wake latency, cache hits, control-plane calls, drain duration, active streams, and materialization failures. | Backend-neutral runtime observability records and tests V1 lifecycle metrics for wake latency/outcomes, route-cache hit/miss, control-plane calls including frontend wake RPCs, Subscribe stream close/update/invalidation, active streams, drain duration, materialization failures, sidecar idle reports, and HTTP-01 results; production binaries install the process-wide stderr sink boundary, and `docs/operator-runbook.md` documents the structured observation format and dashboard wiring. This earlier no-exporter scope was superseded by Milestone 15 and the shared observability extraction in the remediation plan. |
| Complete | Structured logs with instance ID, route ID, generation, and cluster. | Production runtime entrypoints now emit through the same process-wide recorder boundary, and lifecycle log-event assertions cover route/subscription IDs in frontline cache and Subscribe paths, instance/generation/cluster/namespace/error fields in control-plane wake/materialization failures, sidecar idle-report identity fields, drain fields, and HTTP-01 result events. |
| Complete | Backoff and retry policies. | Sidecar idle retry/backoff, proxy Subscribe reconnect/lazy rebuild with reconnect backoff, materializer client retry for transient Kubernetes apply/delete/PVC/readiness failures, and runtime `RetryingControlPlaneStore` retry for transient `StoreError::Unavailable` are covered. |
| Complete | Proxy `Subscribe` reconnect and lazy cache rebuild. | `control_plane_transport.rs` covers reconnect after a closed Subscribe response stream and drains actual stream close as a terminal event; resolver tests prove active cached positives are invalidated before TTL and lazily rebuilt by the next request. |
| Complete | Control-plane restart recovery from database state. | V1 component/API recovery is covered for on-demand retries against durable store state: frontend route coordination retries refreshed `Waking` entries through `WakeInstance`, wake resumes a persisted `Waking` generation through materialization completion, sidecar idle retry resumes `Draining` sleep cleanup after service recreation, and operator delete retry replays active-materialization cleanup after service recreation. `scripts/test-kind-e2e-restart.sh` passed on 2026-06-24 and adds full-platform kind evidence for wake, sleep, delete, HTTP-01, and route reassignment recovery without serving the stale old backend before positive TTL expiry. |
| Complete | Minimal production images for control plane, frontline, and sidecar. | `Dockerfile` builds distroless non-root images, `scripts/smoke-images.sh` covers runtime-file/non-root/CA/startup-error/image-size checks, and `scripts/test-kind-e2e-stateless.sh` builds, loads, starts, and connects the production control-plane, frontline, and sidecar images in kind. |
| Complete | Load tests for route lookup and hot proxy path. | `scripts/smoke-frontline-load.sh`, sidecar load smokes, `crates/frontline/benches/route_lookup.rs`, and `docs/proxy-hot-path-budgets.md` cover route lookup and production-image hot proxy paths. The smokes compare proxied results to same-run direct-backend baselines, emit stable p50/p95/p99/max latency fields plus WebSocket `mib_per_s`, enforce configurable request-rate/throughput ratios and p99 added-latency gates, keep zero-extra-`SubscribeRoute` assertions for measured hot-cache frontline HTTP/1.1, h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, real generated gRPC, and WebSocket phases, and gate fake-control-plane cold-wake latency through the production frontline image. |
| Complete | Indexed frontline route matcher for hot-path lookup. | `RouteCache` keys positives by the exact requested identity, so a lookup is one hash probe regardless of how many routes are cached; cache tests cover precedence/lifecycle behavior and `route_lookup` benchmarks hot positive lookups with 4096 unrelated routes. |
| Complete | Soak tests for repeated wake/sleep cycles. | `scripts/soak-kind-full-wake-sleep.sh` passed on 2026-06-24 with two stateless Deployment and two StatefulSet/PV/PVC full-platform cycles using production `control-plane`, `frontline`, and `sidecar` images, real Postgres, and the real Kubernetes API; it checks leaked test namespaces, workloads, Services, PVCs, PVs, and stateful cluster RBAC after each cycle. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 7A: Metrics, tracing, and structured log fields. | Backend-neutral runtime observability facade, process-wide stderr sink wiring in production entrypoints, stable lifecycle metric descriptors, and focused assertions now cover wake, route cache, control-plane calls, Subscribe events, drain, active streams, materialization failures, sidecar idle reports, and HTTP-01 result paths; `docs/operator-runbook.md` now documents metric names, event fields, and dashboard wiring from those stable descriptors. |
| Complete | 7B: Control-plane restart recovery during wake, sleep, and delete. | Component/API restart gates cover frontend reachability after stale Cold generation conflict and refreshed `Waking` route replay, wake replay from `Waking`, sleep cleanup retry from `Draining`/deleting materialization with service recreation, and delete cleanup retry with operator service recreation; `scripts/test-kind-e2e-restart.sh` passed on 2026-06-24 and adds full-platform kind evidence for wake, sleep, delete, HTTP-01, and route reassignment recovery. |
| Complete | 7C: Proxy Subscribe reconnect, lazy cache rebuild, and stale backend recovery. | Transport reconnect and stale backend recovery have component coverage; actual stream-close-driven active cache invalidation before TTL expiry and lazy rebuild on the next request are covered by transport and resolver tests. |
| Complete | 7D: Minimal final images and container runtime smoke tests. | `scripts/smoke-images.sh` enforces non-root/runtime-file/startup-error checks plus image-size budgets, and `scripts/test-kind-e2e-stateless.sh` proves the final production images start and connect in kind for the stateless platform path. |
| Complete | 7E: Load tests for route lookup and proxy protocols. | Conservative local smoke defaults and strict release-style budget knobs now exist for frontend HTTP/1.1, h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, real generated gRPC, WebSocket sessions plus deterministic byte-stream throughput, sidecar HTTP/1.1, and sidecar TCP large streams, all using production images and direct-backend baselines. The route lookup benchmark remains the local indexed-cache gate. |
| Complete | 7F: kind wake/sleep soak and leaked-object detection. | `scripts/soak-kind-full-wake-sleep.sh` passed on 2026-06-24 with two repeated stateless Deployment and two repeated StatefulSet/PV/PVC full-platform wake/sleep cycles, then deleted the disposable kind cluster; the gate fails on leaked test namespaces, workloads, Services, PVCs, PVs, or stateful cluster RBAC after each cycle. |
| Complete | 7G: Operator runbook and metric name documentation. | `docs/operator-runbook.md` documents the stable backend-neutral metric names, low-cardinality labels, structured stderr event names/fields, dashboard guidance, and symptom runbooks for wake, routing, sleep, cleanup, HTTP-01, TLS/SNI, database, Subscribe, and image/startup incidents. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Automated tests cover restart during wake, sleep, and delete. | `route::tests::stale_cold_wake_conflict_then_refreshed_waking_route_resumes_wake`, `route::tests::waking_route_without_local_pending_wake_resumes_via_control_plane`, `wake::tests::restart_during_wake_resumes_waking_generation_without_new_cas`, `sidecar_api_transport::report_idle_restart_during_sleep_resumes_cleanup_from_store_state`, `api_transport::operator_delete_restart_replays_cleanup_then_finalizes_store_delete`, and proxy wake transport coverage exercise restart-shaped retries at component/API level. |
| Complete | Metrics tests assert counters/histograms and labels for lifecycle paths. | Tests assert emitted backend-neutral observations and low-cardinality labels for route cache, control-plane calls including frontend wake RPCs, Subscribe stream events, wake latency, materialization failures, drain duration, active streams, sidecar idle reports, and HTTP-01 results. |
| Complete | Structured log tests/goldens cover lifecycle fields and errors. | Tests assert emitted lifecycle log fields for route ID, subscription ID, instance ID, generation, cluster, namespace, active count, duration, and error reason across frontline, control-plane, sidecar, drain, and HTTP-01 paths. |
| Complete | Repeated kind wake/sleep soak passes without leaked Kubernetes objects. | `SLEEPYPODS_KIND_SOAK_ITERATIONS=2 ./scripts/soak-kind-full-wake-sleep.sh` passed on 2026-06-24, running stateless Deployment and stateful PV/PVC wake/sleep cycles twice through the deployed production images, Postgres, and Kubernetes API, with leaked-object checks after each stateless and stateful cycle. |
| Complete | Load-test targets for route lookup, hot proxy path, and cold wake latency are defined before 7D. | `docs/proxy-hot-path-budgets.md` documents route lookup, hot proxy path, WebSocket byte-streaming, and fake-control-plane cold-wake budget gates. `scripts/smoke-frontline-load.sh` proves one fake-control-plane cold wake through the production frontline image, enforces exactly one `SubscribeRoute`, one `WakeInstance`, and one backend hit, and gates the single-request p99 with relaxed local and strict release-style defaults. This does not claim real Kubernetes materialization latency coverage. |
| Complete | Proxy load tests use production images and direct-backend baselines. | Production-image load smokes use fake control planes plus same-environment direct-backend comparisons for sidecar HTTP/TCP and frontline HTTP/1.1, h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, real generated gRPC, and WebSocket hot-cache forwarding. The h2-over-TLS phase uses a direct h2c backend baseline, so the frontend budget intentionally includes TLS termination overhead. The scripts fail on missing/malformed metrics, non-positive direct baselines, ratio budget failures, p99 added-latency budget failures, and existing correctness/startup/control-plane-call invariants. |
| Complete | Hot-cache HTTP/1.1 request rate stays within 20% of direct backend. | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_STRICT_BUDGETS=1 ./scripts/smoke-frontline-load.sh` defaults `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO` to `0.80` for the production-image hot-cache HTTP/1.1 phase, while local smoke runs keep the conservative `0.10` default. |
| Complete | Hot-cache h2, h2c, and gRPC request rate stays within 25%. | `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_STRICT_BUDGETS=1 ./scripts/smoke-frontline-load.sh` defaults h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, and real generated gRPC frontend/direct ratio gates to `0.75`, with p99 added-latency gates defaulting to `25` ms. The TLS phase uses the production TLS-termination listener with a generated mounted cert and fails unless ALPN negotiates `h2`; the real gRPC phase uses generated `ProxyControlPlaneClient`/`ProxyControlPlaneServer` types rather than hand-built gRPC frames. |
| Complete | TCP large-stream throughput stays within 10-15%. | `SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STRICT_BUDGETS=1 ./scripts/smoke-sidecar-tcp-load.sh` defaults `SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO` to `0.85` for sidecar/direct TCP large-stream throughput, while local smoke runs keep the conservative `0.05` default. |
| Complete | WebSocket streaming throughput stays within 15-20%. | `scripts/smoke-frontline-load.sh` now keeps the short text/binary WebSocket correctness exchange and also streams deterministic binary bytes in configurable frames through direct-backend and hot-cache frontline phases. The helper backend validates exact bytes and echoes them, the client validates exact echoed bytes and emits `mib_per_s`, and strict mode defaults `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_MIN_RATIO=0.80` against the same-run direct baseline. |
| Complete | Hot-cache p99 added latency stays below documented budget or within 25% baseline. | The production-image load smokes emit `p50_ms`, `p95_ms`, `p99_ms`, and `max_ms`, then compare proxied p99 against same-run direct p99 with configurable `*_MAX_ADDED_P99_MS` thresholds. Strict defaults are documented in `docs/proxy-hot-path-budgets.md`; relaxed smoke defaults remain intentionally loose for developer machines. |
| Complete | Benchmark regressions warn above 10-15% and fail above 20-25%. | `scripts/check-criterion-regressions.py` reads Criterion `change/estimates.json` mean changes, falls back to `base`/`new` mean estimate comparison, defaults to warning above 15% and failing above 25%, rejects invalid threshold configuration, and fails required checks on missing/malformed data unless `--allow-missing` is explicit. `scripts/test-criterion-regressions.py` covers improvement, no change, warning regression, failing regression, missing data, malformed data, fallback comparison, and threshold overrides without running live Criterion benches. |
| Complete | Hot-cache route handling makes zero control-plane calls under load. | `scripts/smoke-frontline-load.sh` snapshots the load-smoke helper's `subscribe_route_calls` counter after warmup/direct baseline and fails if measured hot-cache HTTP/1.1, h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, real generated gRPC, or WebSocket frontline phases increment it; helper tests cover the counters, stats response, cold route response, WebSocket protocol selection, generated gRPC route matching, and gRPC-shaped backend response. |
| Complete | Route lookup and hot proxy path meet target latency under load. | Route lookup has a Criterion benchmark and provisional budget, and the production-image load smokes now enforce strict-mode p99 added-latency gates for frontend HTTP/1.1, h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, real generated gRPC, WebSocket sessions, sidecar HTTP/1.1, and sidecar TCP streams against same-run direct baselines. The frontline smoke also gates fake-control-plane cold-wake latency separately from hot-routing latency. |
| Complete | Hot-cache route lookup avoids scanning every positive cached route. | `RouteCache::lookup` resolves the exact requested identity through a hash index rather than ranking cached rules, with cache tests preserving match semantics and route lookup benchmarks covering many unrelated cached routes. |
| Complete | Retry/backoff tests cover transient database errors, Kubernetes conflicts, proxy disconnects, and materializer retries. | Proxy Subscribe transport reconnect after disconnect with reconnect backoff, stream-close invalidation/lazy rebuild, sidecar retry, Kubernetes conflict retry classification, materializer apply/delete/PVC/readiness retry gates, and fake-store tests for transient store retry/permanent no-retry/max-attempt exhaustion are covered. |
| Complete | Container tests prove images start, run non-root, include required files, access CA certs, and are used by kind E2E/soak. | `scripts/smoke-images.sh` checks non-root/no shell/files/CA/image-size/startup-error behavior, `scripts/test-kind-e2e-stateless.sh` proves production-image startup/connectivity in stateless kind E2E, and `scripts/soak-kind-full-wake-sleep.sh` passed on 2026-06-24 using the production `control-plane`, `frontline`, and `sidecar` images for repeated stateless and stateful full wake/sleep cycles. |
| Complete | Image-size budgets are defined and enforced. | `scripts/smoke-images.sh` enforces Docker inspect `.Size` against positive-integer byte budgets with a 256 MiB default and per-component overrides; `scripts/smoke-images.sh` passed on 2026-06-23 with control-plane 49,541,581 bytes, frontline 39,084,845 bytes, and sidecar 39,504,389 bytes. |
| Complete | Dashboards or metric names are documented enough for operators. | `docs/operator-runbook.md` lists every stable `sleepypods_*` metric from `crates/proxy-core/src/observability/metrics.rs` (historical location; current vocabulary is `crates/sleepypods-observability/src/metrics.rs`), the approved low-cardinality labels and values, the structured stderr observation format, and dashboard/alert panels for route-cache hit rate, control-plane calls, wake latency, materialization failures, drain duration, active streams, sidecar idle reports, HTTP-01 results, proxy throughput, TLS/SNI, and load-budget gates. |

## Milestone 8: Phase Status Audit

Audit the implementation against every prior milestone before doing more
feature work.

Scope:

- Review all Milestone 1-7 scope items, sub-phases, and done criteria against
  the current code, tests, scripts, and kind/container evidence.
- Mark each prior item explicitly as either `Complete` or `Incomplete` in this
  development plan.
- For every `Complete` item, include concise evidence such as a test name,
  script name, source file, or command that proves the claim.
- For every `Incomplete` item, name the missing behavior or missing test.
- Do enough inspection to make a definitive status call; absence of evidence is
  `Incomplete`.

Sub-phases:

- 8A: Audit Milestones 1-3 covering proxy primitives, control-plane resource
  model, and frontline route resolution.
- 8B: Audit Milestones 4-5 covering sidecar idle behavior and Kubernetes
  materialization.
- 8C: Audit Milestones 6-7 covering full-platform E2E, hardening, production
  images, load tests, and soak tests.
- 8D: Update this plan with explicit status markers and evidence for every
  prior scope item, sub-phase, and done criterion.
- 8E: Promote every discovered skipped gate into Milestone 9 or a later
  explicitly deferred item.

Done when:

- Every prior scope item, sub-phase, and done criterion is marked `Complete` or
  `Incomplete`.
- Every `Complete` marker has evidence that a reviewer can run or inspect.
- Every `Incomplete` marker has a concrete follow-up location in Milestone 9,
  Milestone 10, or `Deferred`.
- The plan no longer relies on phase numbers alone as proof that behavior exists.

## Milestone 9: V1 Gap Closure and Workload Sleep Policy

Close the skipped V1 functional gates before writing operator-facing docs. Move
sidecar sleep timing from runtime-only environment defaults into explicit
WorkloadClass policy.

Scope:

- Add focused checks for the currently known gaps: sleep finalization after
  `ReportIdle`, Kubernetes cleanup on delete, full-platform kind E2E, frontline
  TLS/SNI runtime wiring, and subscribed route update/invalidation delivery.
- Close Milestone 8 audit gaps marked for M9: proxy/frontline/sidecar protocol
  matrices, indexed frontline route matching, route-key lookup benchmarks,
  grpc-web parity/browser smoke, HTTP-01 runtime wiring, PostgreSQL/libpq SNI E2E, materializer
  readiness/failure gates, restart/reconnect/retry behavior, production-image
  gates, load budgets, and full wake/sleep soak.
- Add runtime observability and structured-log gates for V1 lifecycle paths
  before operator-facing docs freeze metric and log names.
- Add WorkloadClass-owned sleep policy for idle timeout, idle report retry
  backoff, and drain grace.
- Require every WorkloadClass to specify an idle timeout explicitly; there is no
  platform default for the operator-facing sleep timeout.
- Allow per-instance idle-timeout overrides only when the WorkloadClass declares
  an override field and validation bounds.
- Render the resolved policy into sidecar env as
  `SLEEPYPODS_IDLE_TIMEOUT_MS`, `SLEEPYPODS_IDLE_RETRY_BACKOFF_MS`, and
  `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS`.
- Preserve sidecar runtime env parsing only as the execution mechanism for
  rendered manifests, not as a source of platform defaults.

Sub-phases:

- 9A: Gap-check tests for sleep finalization, delete cleanup, full-platform kind
  E2E, frontline TLS/SNI runtime wiring, and subscribed route
  updates/invalidations.
- 9B: WorkloadClass sleep-policy model, validation, and API/proto mapping.
- 9C: Validated per-instance idle-timeout overrides with WorkloadClass-declared
  bounds.
- 9D: Manifest rendering of resolved sidecar sleep-policy env.
- 9E: Component and kind E2E coverage for policy rendering and idle behavior.
- 9F: Remaining protocol/API/runtime audit gaps: indexed frontline route matcher,
  route-key lookup benchmarks, proxy-core reset/backpressure tests, frontline
  TLS/SNI/HTTP-01 runtime wiring, HTTP-01 store overwrite/idempotency and wrong
  host/token tests, sidecar restart during idle/report behavior, grpc-web
  parity/browser smoke, and PostgreSQL/libpq SNI E2E.
- 9G: Hardening audit gaps: materializer readiness/failure route publication,
  restart/reconnect/retry gates, runtime metrics/log assertions, production
  image startup/connectivity checks, load budgets, and full wake/sleep soak.
- 9Y: Forwarded-header trust policy for frontline HTTP, TLS termination, and
  WebSocket forwarding.
- 9Z: Full-platform kind protocol gate for HTTP/2, h2c gRPC, and WebSocket
  forwarding.
- 9AO: Local production-image negotiated h2-over-TLS and real generated gRPC
  load gates.

Phase evidence:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 9Y: Frontline forwarded-header trust policy. | `proxy-core` helper tests cover stripping spoofed `Forwarded`/`X-Forwarded-*` headers and setting canonical `X-Forwarded-For`, `X-Forwarded-Proto`, and `X-Forwarded-Host`; `frontline/src/listener/tests.rs` covers HTTP, TLS termination with `https`, and WebSocket upstream handshakes; `sidecar/src/tests.rs` proves sidecar HTTP preserves existing forwarded headers. TLS/SNI passthrough remains byte-transparent and cannot mutate encrypted HTTP headers. Future extension: trusted-proxy CIDRs and append-to-chain behavior. |
| Complete | 9Z: Full-platform kind protocol gate for HTTP/2, h2c gRPC, and WebSockets. | `scripts/test-kind-e2e-protocols.sh` builds and loads production `control-plane`, `frontline`, and `sidecar` images plus a protocol demo workload image, deploys real Postgres/control-plane/frontline into kind, creates WorkloadClass/Instance/RouteBinding resources through the deployed operator API, and `kind_e2e_protocols.rs` verifies HTTP/2 cold/hot responses, h2c gRPC-shaped body plus `grpc-status`/`grpc-message` trailers, and WebSocket text/binary/clean-close behavior through the deployed sidecar. grpc-web browser is covered by 9AA; PostgreSQL/libpq direct-SNI is covered by 9AB. |
| Complete | 9AA: Deployed grpc-web browser-shaped operator smoke. | `scripts/test-kind-e2e-grpc-web.sh` builds and loads the production `control-plane` image plus `postgres:17-alpine`, deploys real Postgres/control-plane into kind, enables `SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR`, port-forwards the grpc-web listener, and `kind_e2e_grpc_web.rs` uses browser-shaped HTTP/1.1 requests to verify CORS preflight, `application/grpc-web+proto` unary framing, exposed grpc-web status/message headers, response body trailer frames, store-backed WorkloadClass create/get, Instance create/get/delete, and structured `NotFound` after delete. PostgreSQL/libpq direct-SNI is covered by 9AB. |
| Complete | 9AB: PostgreSQL/libpq direct-SNI kind gate. | `scripts/test-kind-e2e-libpq-sni.sh` builds and loads production `control-plane`, `frontline`, and `sidecar` images plus a test-only TLS Postgres workload/client image based on exact `postgres:17.5-alpine3.22`, deploys real Postgres/control-plane/frontline into kind, creates the Postgres workload path through WorkloadClass/Instance/SNI RouteBinding APIs, and runs in-cluster `psql` Jobs proving libpq 17 `sslnegotiation=direct` reaches a marker table through the frontline TLS passthrough listener while an unbound SNI host fails. This proves PostgreSQL/libpq 17+ direct TLS negotiation through SNI passthrough, not every Postgres auth/TLS mode or every database protocol. |
| Complete | 9AC: Full-platform kind failure-path gate. | `scripts/test-kind-e2e-failures.sh` builds and loads production `control-plane`, `frontline`, and `sidecar` images plus `postgres:17-alpine` and a test workload image, deploys real Postgres/control-plane/frontline into kind, creates WorkloadClass/Instance/RouteBinding resources through the deployed operator API, and `kind_e2e_failures.rs` proves bad route miss, bounded readiness wake failure, missing PVC binding failure, invalid volume-template rejection, and stale proxy `WakeInstance` generation rejection through the deployed frontline/control-plane and real Kubernetes objects. Restart and lifecycle-race E2E remain separate rows. |
| Complete | 9AD: Full-platform control-plane restart gate. | `scripts/test-kind-e2e-restart.sh` passed on 2026-06-24 after building and loading production `control-plane`, `frontline`, and `sidecar` images plus `postgres:17-alpine` and test workload images, deploying real Postgres/control-plane/frontline into kind, and running `kind_e2e_restart.rs` through deployed operator/proxy/frontline/sidecar paths plus the real Kubernetes API. The gate proves wake retry from durable `Waking` state after a delayed image pull and control-plane restart, sidecar sleep cleanup after control-plane unavailability, operator delete cleanup retry after control-plane unavailability, HTTP-01 challenge put/serve/resolve/delete/expiry across restart, and immediate post-restart route reassignment without serving the stale old backend before positive TTL expiry. |
| Complete | 9AF: Full-platform lifecycle-race gate. | `scripts/test-kind-e2e-lifecycle-races.sh` passed on 2026-06-24 after building and loading production `control-plane`, `frontline`, and `sidecar` images plus `postgres:17-alpine` and delayed test workload image tags, deploying real Postgres/control-plane/frontline into kind, and running `kind_e2e_lifecycle_races.rs` through deployed operator/proxy/sidecar/frontline paths plus the real Kubernetes API. The gate proves concurrent wake calls converge with one ready generation and generation conflicts for stale callers, `ReportIdle` while waking is rejected without cleanup, delete while waking cleans recorded pending StatefulSet/Service/PVC/PV objects and serves no stale backend, delete while Draining/deleting materialization cleans StatefulSet/Service/PVC/PV objects and serves no stale backend, failed wake retry rejects stale failed generations after a later successful wake, stale sidecar `ReportIdle` is rejected without changing current state, and route reassignment invalidates active proxy subscriptions before subsequent requests reach the new backend. Unit coverage proves applied refs are best-effort deleted if pending materialization recording fails or if apply/PVC-bound wait fails before pending refs are recorded. The remaining 6G gap is process loss or an indefinitely stuck PVC-bound wait before pending refs are recorded. |
| Complete | 9AH: Full kind wake/sleep soak. | `SLEEPYPODS_KIND_SOAK_ITERATIONS=2 ./scripts/soak-kind-full-wake-sleep.sh` passed on 2026-06-24 after creating a disposable kind cluster, building/loading production `control-plane`, `frontline`, and `sidecar` images plus stateless/stateful workload images and `postgres:17-alpine`, and running two stateless Deployment and two StatefulSet/PV/PVC wake/sleep cycles through deployed control-plane/frontline/sidecar paths, real Postgres, and the real Kubernetes API. The gate verifies route backend availability inside each reused E2E driver and checks after every stateless/stateful cycle that no test namespace, materialized workload, Service, PVC, PV, or stateful cluster RBAC object leaked. |
| Complete | 9AI: Local frontline h2c gRPC-shaped load smoke. | `scripts/smoke-frontline-load.sh` now runs a second production-image hot-cache phase using prior-knowledge h2c POST requests with `content-type: application/grpc`, a fixed gRPC-framed request body asserted by the helper, a fixed gRPC-framed response body, `grpc-status: 0` as an HTTP/2 response trailer, a direct-backend baseline, conservative ratio guard, and a zero-extra-`SubscribeRoute` assertion for the measured h2c phase. 9AL adds strict release-style ratio and p99 added-latency gates for this local h2c gRPC-shaped path; 9AO adds negotiated h2-over-TLS and real generated gRPC load-rate gates. |
| Complete | 9AJ: Local frontline WebSocket and cold-wake load smokes. | `scripts/smoke-frontline-load.sh` now adds a production-image hot-cache WebSocket phase that performs real HTTP upgrades, bidirectional text/binary frames, a direct-backend baseline, conservative ratio guard, and zero-extra-`SubscribeRoute` assertion. The same smoke also sends one request through a distinct fake-control-plane cold route and fails unless that phase records exactly one `SubscribeRoute`, one `WakeInstance`, and one backend HTTP hit before the client succeeds. 9AL adds strict release-style session-rate and p99 added-latency gates for the WebSocket hot-cache session path; 9AM adds deterministic byte-stream throughput and fake-control-plane cold-wake latency gates. |
| Complete | 9AK: Durable pending refs before Kubernetes apply. | `wake.rs` now derives validated rendered refs from the rendered manifest and records a Pending materialization before invoking Kubernetes apply, PVC-bound waits, or readiness waits; if the Pending write fails, no Kubernetes object is applied. If delete wins after Pending is recorded and the wake still applies objects, post-apply readiness/complete failures re-check instance state and best-effort delete the refs only when the instance is missing or terminal, avoiding cleanup on generation conflicts where a newer valid materialization may own the refs. Implicit retry backend generations are also raised to any active materialization backend generation so a failed Pending row created with a high caller-supplied backend generation cannot block a later normal retry, while explicit lower caller generations still hit the backend-generation rewind guard. `wake::tests::successful_wake_cas_renders_applies_and_completes_with_waking_generation` snapshots store events at first apply, `wake::tests::record_materialization_failure_prevents_kubernetes_apply` proves failed pending writes apply/delete/wait nothing, `wake::tests::delete_after_pending_before_apply_cleans_objects_applied_by_wake` and `wake::tests::delete_after_pending_before_readiness_failure_cleans_objects_applied_by_wake` cover post-Pending delete races, `wake::tests::pvc_bound_failure_keeps_pending_stateful_refs_for_delete_or_retry` proves PVC-bound failure leaves Pending PV/PVC/Service/StatefulSet refs with no backend for later delete or retry, and `wake::tests::retry_after_failed_high_backend_generation_does_not_rewind_pending_materialization` covers the high caller-supplied failed Pending -> normal retry sequence. `materializer::tests::rendered_object_refs_are_available_without_applying_or_waiting` covers pure ref derivation. `scripts/test-kind-e2e-lifecycle-races.sh` passed on 2026-06-24 after rebuilding production images and rerunning the deployed lifecycle-race matrix. |
| Complete | 9AL: Load budget metrics and gates. | The frontline and sidecar load-smoke clients now emit stable successful operation latency fields (`p50_ms`, `p95_ms`, `p99_ms`, `max_ms`) for HTTP/1.1, h2c gRPC-shaped, negotiated h2-over-TLS gRPC-shaped, real generated gRPC, WebSocket session, sidecar HTTP/1.1, and sidecar TCP stream phases, plus throughput metrics for stream phases. `scripts/smoke-frontline-load.sh`, `scripts/smoke-sidecar-load.sh`, and `scripts/smoke-sidecar-tcp-load.sh` use a small shared budget helper to reject missing/malformed metrics, non-positive direct baselines, request-rate/throughput ratio regressions, absolute latency regressions, and p99 added-latency regressions. Relaxed local smoke defaults remain conservative, while `SLEEPYPODS_*_STRICT_BUDGETS=1` enables documented release-style defaults in `docs/proxy-hot-path-budgets.md`. |
| Complete | 9AM: Cold-wake latency and WebSocket streaming gates. | `scripts/smoke-frontline-load.sh` now gates fake-control-plane cold-wake latency through the production frontline image with `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_WAKE_MAX_LATENCY_MS`, while preserving the exact one `SubscribeRoute`, one `WakeInstance`, and one backend-hit invariant. The same smoke keeps short WebSocket text/binary checks and adds configurable deterministic byte streaming with exact backend/client validation, `mib_per_s` output, direct-backend baseline, hot-cache zero-extra-`SubscribeRoute` assertion, and `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_MIN_RATIO` budget. This closes the local production-image fake-control-plane cold-wake and WebSocket byte-streaming gaps; real Kubernetes cold materialization latency remains outside this phase. |
| Complete | 9AO: Negotiated HTTP/2 and real gRPC load gate. | `scripts/smoke-frontline-load.sh` now generates a temporary route-host certificate, mounts it into the production `frontline` image, enables the TLS-termination listener, warms and measures a negotiated h2-over-TLS gRPC-shaped phase, and fails unless the client observes TLS ALPN `h2`. That phase has a same-run direct h2c backend baseline, zero-extra-`SubscribeRoute` assertion, `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_H2_TLS_GRPC_MIN_RATIO` strict default `0.75`, and `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_H2_TLS_GRPC_MAX_ADDED_P99_MS` strict default `25`. The helper also starts a generated `ProxyControlPlaneServer` backend and measures generated `ProxyControlPlaneClient/WakeInstance` calls directly and through production frontline with a same-run direct generated-gRPC baseline, zero-extra-`SubscribeRoute` assertion, `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REAL_GRPC_MIN_RATIO` strict default `0.75`, and `SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REAL_GRPC_MAX_ADDED_P99_MS` strict default `25`. |

Done when:

- Gap-check tests fail against the current incomplete behavior and pass only when
  the missing V1 behavior is implemented.
- kind E2E proves `ReportIdle` leads to drain, deletion of rendered Kubernetes
  objects, materialization state cleanup, and transition back to `Cold`.
- kind E2E proves deleting an instance cleans up any active materialization and
  leaves no workload, Service, PVC, or PV objects owned by that instance.
- full-platform kind E2E uses the real control-plane, frontline, and sidecar
  images against a real database and Kubernetes API, not fake clients.
- Live database gate `scripts/test-postgres-store.sh` runs the Postgres store
  conformance suite with `SLEEPYPODS_POSTGRES_URL` set, using either a caller
  URL or disposable `postgres:17-alpine`, and proves migrations plus store
  behavior against a real database.
- Runtime tests prove the frontline binary wires HTTP, TLS termination, and
  TLS/SNI passthrough listeners rather than leaving TLS/SNI as library-only
  primitives.
- Subscription tests prove route changes and backend changes are pushed as
  targeted updates or invalidations to actively subscribed proxies.
- Operator APIs accept and return WorkloadClass sleep policy.
- WorkloadClass creation rejects missing or invalid idle timeout values.
- Instance creation rejects idle-timeout overrides unless the WorkloadClass
  explicitly allows them.
- Out-of-bounds instance idle-timeout overrides are rejected before manifest
  rendering.
- Render tests prove the sidecar container receives the resolved timeout env
  values.
- kind E2E proves two workload classes with different idle policies sleep at
  different configured thresholds.
- Proxy-core/frontline/sidecar protocol tests cover the incomplete reset,
  timeout, backpressure, HTTP/2, h2c/gRPC, WebSocket, TLS/SNI, and HTTP-01
  cases identified by the Milestone 8 audit.
- Route-key lookup benchmarks and load gates cover hot-cache route lookup, hot
  proxy paths, cold wake latency, tail latency, and zero control-plane calls on
  hot-cache hits.
- Exact-identity frontline route caching avoids scanning every positive cached route
  on hot-cache hits while preserving exact-host over wildcard-host,
  more-specific wildcard-host over broader wildcard-host, longest path-prefix
  selection, SNI matching, negative-cache semantics, TTL expiry, invalidation,
  and stale generation rejection.
- grpc-web store-backed integration tests cover operator APIs, CORS/preflight,
  metadata/auth propagation, structured errors, and V8-compatible request
  encoding.
- HTTP-01 runtime tests prove challenge interception calls the control plane and
  takes precedence over normal route resolution.
- HTTP-01 store tests prove overwrite/idempotency behavior plus wrong-host and
  wrong-token misses.
- PostgreSQL/libpq 17+ SNI passthrough E2E passes with `sslnegotiation=direct`
  and a pinned test image.
- Materializer readiness/failure tests prove routes publish only after ready,
  withdraw on unready, and reject stale or invalid manifests.
- Restart/reconnect/retry tests cover control-plane recovery, proxy Subscribe
  reconnect, database errors, Kubernetes conflicts, proxy stream disconnects,
  and materializer reconcile retries.
- Sidecar restart tests prove idle/report behavior remains correct across
  process restart or detector reconstruction, including duplicate report
  handling and generation checks.
- Runtime metrics/log tests assert the V1 lifecycle fields that Milestone 10
  documents.
- Production-image tests prove the final images start, run as non-root, have the
  required runtime files and CA roots, meet image-size budgets, and are used by
  kind E2E/load/soak gates.

## Milestone 10: Operator and Contributor Documentation

Write concise documentation for the two supported audiences: operators who run
and use the platform, and contributors who build and change it.

Scope:

- Operator documentation for the resource model, including `WorkloadClass`,
  `Instance`, `RouteBinding`, custom domains, HTTP-01, storage values, wake,
  sleep, delete, and expected limitations.
- Operator task guides for creating a workload class, creating an instance,
  adding a route, adding a custom domain, attaching an existing volume, and
  understanding sleep/wake behavior.
- Operator installation and administration guides for the control plane,
  frontline proxies, sidecars, database, Kubernetes permissions,
  TLS/certificate plumbing, metrics, logs, backups, upgrades, and failure
  recovery.
- Operator runbooks keyed by observable symptoms, logs, metrics, Kubernetes
  objects, and control-plane state.
- Contributor documentation with the smallest useful commands for build, unit
  tests, protocol tests, kind E2E, and code generation.
- Agent-facing repository guide with file map, invariants, source-of-truth docs,
  generated files, test gates, and common task entry points.
- Documentation for metric names, structured log fields, load/latency budgets,
  production-image expectations, and any intentionally deferred limitations.

Sub-phases:

- 10A: Operator-facing concepts, resource model, and request/lifecycle sequence
  diagrams.
- 10B: Operator task guides for workload classes, instances, routes, custom
  domains, HTTP-01, and existing volumes.
- 10C: Operator installation, configuration, database, Kubernetes, TLS, metrics,
  and upgrade guide.
- 10D: Operator troubleshooting and incident runbooks.
- 10E: Contributor local development and test guide.
- 10F: Agent-facing repository guide with file map, invariants, and common task
  entry points.

Done when:

- An operator can create a workload class, instance, route, custom domain, and
  existing-volume-backed workload from docs alone.
- An operator can predict what happens during cold wake, hot route, idle sleep,
  drain, delete, and route/domain changes.
- An operator can install, configure, monitor, back up, upgrade, and troubleshoot
  the platform from docs alone.
- API and protocol docs match the protobuf/Rust types, native gRPC service,
  gRPC-Web operator surface, and contain no stale RPCs.
- Documentation is concise: prefer short task-oriented files, stable headings,
  examples, and explicit invariants over broad narrative prose.
- Contributor and agent-facing guidance calls out source-of-truth files,
  generated files, commands, test gates, and design constraints without
  duplicating full specs.

## Milestone 11: Rendered Kubernetes Name Safety

Prevent different instances from accidentally rendering the same Kubernetes
object names. Operators still choose readable naming templates, but SleepyPods
must enforce the platform invariant before any Kubernetes apply: a rendered
object ref belongs to exactly one active materialization.

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Require `instance_id` to be Kubernetes DNS-label safe at instance creation. | `InstanceId` now rejects invalid DNS labels and over-63-byte IDs; API transport covers invalid operator create requests. |
| Complete | Add a name-rendering helper for instance-scoped Kubernetes objects. | `kubernetes_name::render_instance_scoped_name` appends a stable instance prefix suffix and is used by manifest rendering. |
| Complete | Reserve at least eight characters plus separator for the injected instance ID prefix. | Renderer tests cover short IDs, eight-character long ID prefixes, max-length output, and base truncation with suffix preservation. |
| Complete | Do not encode the Kubernetes object kind into generated names. | Renderer tests prove shared base names render as `<base>-<instance-prefix>` without kind tokens. |
| Complete | Validate every rendered `Deployment`, `StatefulSet`, `Service`, `PVC`, and `PV` name before apply. | Renderer and materializer preflight validate DNS-label names and reject invalid rendered names before Kubernetes apply. |
| Complete | Reject duplicate rendered object refs inside one manifest. | Materializer preflight rejects duplicate `(apiVersion, kind, namespace, name)` refs before any apply. |
| Complete | Reject cross-instance rendered object-ref collisions before apply. | Postgres materialization upsert checks active materializations for other instances; wake collision test proves no Kubernetes apply occurs. |
| Complete | Use correct collision keys for namespaced and cluster-scoped objects. | Materializer validates PV refs with empty namespace; Postgres conformance covers namespaced Service and cluster-scoped PV collisions. |
| Complete | Document custom naming template behavior. | Operator guide documents base templates, injected suffix, kind-free names, validation, and collision failures. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 11A: Instance ID validation tightening in protobuf/API/domain/store paths. | Shared `InstanceId` enforces DNS-label rules; API transport and Postgres mapping use the same type. |
| Complete | 11B: Shared Kubernetes name-rendering helper. | Helper reserves the suffix budget and truncates only the operator base. |
| Complete | 11C: Manifest renderer integration for workload, Service, PVC, and PV names. | Deployment, StatefulSet, Service, PVC, and PV rendering all use the helper. |
| Complete | 11D: Store-level rendered object-ref collision check. | Postgres `record_materialization`/`complete_wake` upsert path rejects collisions with other active materializations. |
| Complete | 11E: Materializer and wake failure behavior. | Materializer preflight rejects invalid/duplicate refs; wake records collision failure as `Failed` without applying Kubernetes objects. |
| Complete | 11F: Operator documentation for naming rules, examples, and collision errors. | `docs/operator-guide.md` and `docs/operator-runbook.md` updated. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Instance creation rejects IDs that cannot safely participate in Kubernetes object names. | `sleepypods-types` and `api_transport` tests cover valid/invalid IDs. |
| Complete | Rendered object names are always valid DNS labels and at most 63 characters. | Manifest and materializer tests cover DNS-label validation and pre-apply rejection. |
| Complete | Long operator-provided base names are truncated while preserving the required instance ID prefix suffix. | Boundary tests assert 63-byte names ending in the injected suffix. |
| Complete | Two different instances using the same base workload, Service, PVC, or PV template cannot both materialize to the same final object ref. | Renderer suffix tests prove normal separation; Postgres conformance rejects explicit namespaced and PV ref collisions. |
| Complete | A name collision is rejected before Kubernetes apply. | Wake test simulates store collision and asserts no PV, PVC, Service, Deployment, or StatefulSet apply/readiness calls occur. |
| Complete | Unit tests cover valid/invalid instance IDs, truncation boundaries, short and long instance IDs, duplicate refs, namespaced collisions, and cluster-scoped PV collisions. | Covered by `cargo test -p control-plane` plus `cargo test -p sleepypods-types`. |
| Complete | kind E2E proves intentionally colliding base templates are separated or rejected safely. | `scripts/test-kind-e2e-stateless.sh` and `scripts/test-kind-e2e-stateful.sh` passed on 2026-06-24 with production images in disposable kind clusters, asserting suffixed rendered names for Deployment, StatefulSet, Service, PVC, and PV objects. |
| Complete | Operator docs cover naming rules. | Operator guide recommends readable base names, explains suffix injection, and states object kind is not encoded. |

## Milestone 12: WorkloadClass Exclusivity Keys

Add an opt-in primitive for workloads that reference external singleton
resources, such as pre-created disks, without making SleepyPods provider-aware.
The default V1 contract remains simple: SleepyPods guarantees at most one active
materialization per `Instance`, but it does not infer that two different
instances share the same external disk, license, network identity, or other
resource just because their template values happen to match.

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Add `WorkloadClass` exclusivity keys rendered from instance values. | `WorkloadExclusivityKeyTemplate` is part of `WorkloadClassVersion`, protobuf create/get includes `repeated WorkloadExclusivityKey`, and `api_transport.rs` round-trips `name: "disk"` with `value: "{{ volume_handle }}"`. |
| Complete | Treat the rendered key as opaque. | Rendering validates only key-name/template shape and non-empty rendered values; store errors and `runtime.wake.event` include key name/holder but never the rendered value. |
| Complete | Acquire exclusivity during wake before Kubernetes PV/PVC/workload objects are applied. | `wake::tests::wake_records_rendered_exclusivity_keys_before_kubernetes_apply` and `exclusivity_conflict_prevents_kubernetes_apply_and_materialization_record` prove keys are recorded with Pending before apply and conflicts apply no objects. |
| Complete | Reject or use only bounded/cancellable waiting when another active materialization holds the same rendered key. | Postgres uses deterministic transaction-scoped `pg_try_advisory_xact_lock` keyed by a stable per-target/name/value advisory ID plus an active-materialization conflict query and returns `StoreError::ExclusivityConflict` immediately. |
| Complete | Acquire multiple exclusivity keys in deterministic sorted order. | Domain rendering and Postgres store normalization sort/deduplicate by `(name, value)`; unit tests cover deterministic ordering. |
| Complete | Release partially acquired keys when later acquisition or wake setup fails. | Keys are acquired inside the same transaction as Pending materialization; failed acquisition/conflict rolls back with no materialization row; `wake::tests::first_apply_failure_after_exclusivity_acquire_releases_pending_key` proves a first Kubernetes apply failure after key acquisition clears the Pending row to Deleted with empty keys. |
| Complete | Keep unrelated exclusivity keys independent. | Postgres conformance `exercise_exclusivity_keys` records different rendered keys while another key is held; acquisition locks are keyed per target/name/value, not an in-memory global mutex. |
| Complete | Release the key only after safe cleanup. | Keys remain on Pending/Ready/Deleting materializations and are cleared only when `finalize_sleep` marks Deleted; delete releases via cascade only after materializer cleanup succeeds. |
| Complete | Bind locks to instance ID, generation, target, and rendered key. | Materialization rows store instance ID, instance generation, target, and rendered keys; stale generation record attempts are rejected before key overwrite in Postgres conformance. |
| Complete | Reconcile exclusivity state from durable active materializations after control-plane restart. | Locks are derived from active materialization rows; Postgres conformance reconnects a recreated store and proves same-key wake remains blocked while the original active row exists. |
| Complete | Emit structured observability for lock acquire and conflict decisions. | Wake observations add `exclusivity.action`, `exclusivity.key.name`, and `exclusivity.owner.instance.id` for acquire/conflict without rendered values; `wake::tests::exclusivity_conflict_observability_uses_bounded_fields` covers conflict fields. |
| Complete | Document explicit operator responsibility for singleton external resources. | `docs/operator-guide.md` and `docs/operator-runbook.md` document opt-in exclusivity keys, opaque values, conflict diagnosis, and cleanup ordering. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 12A: Protobuf, domain model, validation, and Postgres schema for `WorkloadClass` exclusivity keys. | `control_plane.proto`, `workload.rs`, migration `0005_workload_class_exclusivity_keys.sql`, Postgres mapping/persistence, and API validation are implemented. |
| Complete | 12B: Store APIs and transaction semantics for acquiring, observing, and releasing rendered exclusivity keys. | `record_materialization` normalizes keys, takes per-key Postgres advisory locks, checks active materializations, and persists keys transactionally. |
| Complete | 12C: Wake/sleep/delete integration. | Wake renders keys before Pending; sleep keeps keys through Deleting and clears on finalize; delete releases only after materializer cleanup and row deletion. |
| Complete | 12D: Restart reconciliation for locks derived from active materializations. | No separate lock table is needed; recreated Postgres store conflict test proves active rows remain authoritative. |
| Complete | 12E: Operator documentation and runbook updates for exclusive external resources. | Operator guide and runbook updated. |
| Complete | 12F: Unit and component concurrency tests. | Unit/API/wake tests cover validation, pre-apply no-object conflicts, deterministic ordering, bounded observability, first-apply no-object release, and partial-apply held-key behavior; `./scripts/test-postgres-store.sh` passed on 2026-06-24 with same-key contention, different-key independence, stale generation rejection, active-vs-deleted release, and recreated-store behavior against real Postgres. |
| Complete | 12G: kind E2E tests for same-key contention, restart, and cleanup. | `./scripts/test-kind-e2e-exclusivity.sh` passed on 2026-06-24 with production control-plane/frontline/sidecar images, Postgres, Kubernetes PV/PVC/StatefulSet objects, same `{{ volume_handle }}` duplicate rejection with no blocked-instance objects, unrelated handle wake, control-plane pod restart while the owner held the key, and post-delete cleanup release. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Two cold instances with the same rendered exclusivity key cannot both materialize at the same time. | Postgres conformance same-key contender receives `StoreError::ExclusivityConflict`; wake component conflict applies no Kubernetes objects; `./scripts/test-kind-e2e-exclusivity.sh` proves the duplicate `{{ volume_handle }}` stateful instance returns HTTP 503 and creates no PV/PVC/Service/StatefulSet objects. |
| Complete | Same-key concurrent wake has a bounded result. | `pg_try_advisory_xact_lock` avoids indefinite waiting and returns structured conflict when a key is held or being acquired. |
| Complete | Different-key concurrent wakes proceed independently. | Postgres conformance records a different `disk`/`license` pair while another pair remains active; kind M12 gate wakes an unrelated `volume_handle` while the shared handle remains held. |
| Complete | Multi-key acquisition is deadlock-free. | Keys are sorted by `(name, value)` in render and at the store boundary before advisory lock acquisition. |
| Complete | A blocked or rejected wake creates no Kubernetes objects. | Wake conflict and rendered-object collision tests assert no apply/readiness/delete calls; `wake::tests::first_apply_failure_after_exclusivity_acquire_releases_pending_key` proves a first-object apply failure leaves no Kubernetes objects and releases the key; kind M12 gate verifies the blocked same-key instance has zero PV/PVC/Service/StatefulSet/Pod objects by `sleepypods.io/instance-id`. |
| Complete | Sleep/delete releases the key only after rendered Kubernetes objects are gone. | Store finalization clears keys only on Deleted; sidecar/delete cleanup paths call materializer delete before finalizing/deleting rows. |
| Complete | Control-plane restart reconstructs held keys from active durable materializations and prevents duplicate wake after restart. | Postgres conformance reconnects a store and proves duplicate key conflict from the persisted active row; kind M12 gate deletes the control-plane pod while the owner holds the key and then verifies the same-key duplicate remains blocked. |
| Partial | Crash/restart tests cover every lock boundary. | Durable active-row restart behavior is covered in real Postgres and through a real control-plane pod restart in kind while a stateful owner materialization holds the key. Explicit crash injection after key acquire before apply, after apply before ready, and during cleanup remains a gap. |
| Complete | Cleanup failure keeps the key held until cleanup succeeds or operator intervention marks it safe. | Deleting materialization rows retain exclusivity keys and still conflict until `finalize_sleep` clears keys. |
| Complete | Stale generations cannot release, steal, or overwrite another generation's lock. | Postgres conformance stale `record_materialization` attempt returns `GenerationConflict` before changing keys. |
| Partial | Failure tests cover wake failure before apply, wake failure after lock acquire, Kubernetes cleanup failure, delete retry, stale generation, cancellation/timeout, and concurrent wake attempts. | Unit/Postgres paths cover pre-apply conflict, stale generation, no materialization, deleting-held semantics, bounded contention, cleanup-held release, first-apply no-object release, and partial-apply held-key behavior; kind M12 gate covers live duplicate rejection, delete cleanup release, retry across control-plane restart, and unrelated wake independence. Dedicated cancellation/timeout injection and live concurrent wake race tests remain gaps. |
| Complete | kind E2E proves a stateful workload class using `{{ volume_handle }}` as an exclusivity key prevents duplicate attachment while allowing unrelated handles. | `./scripts/test-kind-e2e-exclusivity.sh` passed on 2026-06-24 using `disk={{ volume_handle }}` and real PV/PVC/StatefulSet objects. |
| Complete | kind E2E proves restart safety while an exclusivity key is held. | `./scripts/test-kind-e2e-exclusivity.sh` passed on 2026-06-24 after deleting the control-plane pod while the first stateful owner held the rendered key. |
| Complete | Operator docs clearly state the exclusivity boundary. | Operator guide says SleepyPods does not own external volume lifecycle or infer singleton resources unless a `WorkloadClass` declares an exclusivity key. |

The two Partial rows above are retained as explicit future hardening gaps for
broader crash-injection and cancellation/race coverage. They do not change the
M12 V1 acceptance boundary: opt-in keys are rendered and persisted, same-key
contention is bounded, no-object wake failures release, partial/cleanup-risk
failures keep keys held, stale generations are rejected, active rows survive
restart, and the full-platform stateful kind gate covers the supported
restart-held-key workflow.

## Milestone 13: Control Plane Authentication and Authorization

Add authentication and authorization at the control-plane API boundary. This is
control-plane auth, not application end-user auth and not per-route workload auth.
It covers both the operator-facing APIs and the internal proxy/sidecar APIs, with
separate policy because those callers have different trust models.

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Add an auth layer for native gRPC control-plane services. | `control-plane::auth` defines role-aware interceptors and `runtime.rs` wraps operator, proxy, and sidecar tonic services before handlers run. Covered by `cargo test -p control-plane --test api_transport auth --quiet`, `cargo test -p control-plane --test proxy_api_transport auth --quiet`, and `cargo test -p control-plane --test sidecar_api_transport auth --quiet`. |
| Complete | Add matching auth behavior for gRPC-Web operator APIs. | The gRPC-Web router wraps the operator service with the same role interceptor, keeps `Authorization` CORS support, and returns grpc-web status trailers for auth failures. Covered by `cargo test -p control-plane --test api_transport auth --quiet` and `./scripts/test-kind-e2e-grpc-web.sh`. |
| Complete | Separate operator and runtime caller policy. | `CallerRole` has distinct operator, proxy, and sidecar roles; service interceptors require exactly the matching role. Wrong-role calls return `PermissionDenied`. |
| Complete | Support a simple first auth provider. | Static bearer-token config uses `SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=static-bearer-token` plus distinct operator, proxy, and sidecar token env vars. Frontlines attach proxy/operator tokens from their env, and the control plane injects the sidecar token into rendered pods from runtime auth config instead of WorkloadClass/API state. |
| Complete | Leave room for stronger providers. | Business handlers depend only on service-level interceptors and the `AuthProvider` trait, leaving future mTLS/workload-identity/JWT providers outside handler code. |
| Complete | Fail closed when auth is configured. | Static auth requires all role credentials at startup; missing, malformed, invalid, and wrong-role credentials map to stable `Unauthenticated` or `PermissionDenied` status before store/materializer calls. |
| Complete | Keep explicit development/test mode. | Runtime config now requires `SLEEPYPODS_CONTROL_PLANE_AUTH_MODE`; `no-auth` is an explicit mode used by local/test scripts, not a fallback for malformed static auth. |
| Complete | Emit structured observability for auth decisions. | `proxy-core` includes `control_plane.auth.decision` and auth decision/reason/role/service fields; auth tests assert accepted, missing, invalid, and wrong-role events without credential material. |
| Complete | Document deployment responsibility and examples. | `docs/operator-guide.md` and `docs/operator-runbook.md` document static auth env, no-auth local-only use, runtime role separation, rotation expectations, and network-boundary responsibility. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 13A: Auth domain model, config, and provider trait. | `auth.rs` defines roles, redacted bearer tokens, `AuthProvider`, static-token provider, failure mapping, explicit no-auth mode, and focused provider/config tests. |
| Complete | 13B: Native gRPC server integration. | Native router wraps all three generated services with role-specific `tonic` interceptors before business handlers. |
| Complete | 13C: gRPC-Web integration. | Operator gRPC-Web uses the same operator-role interceptor and accepts browser-shaped `Authorization` requests through CORS. |
| Complete | 13D: Authorization checks by service and method. | Operator, proxy, and sidecar credentials are scoped to their services; wrong-role tests cover operator-to-runtime and runtime-to-operator denial. |
| Complete | 13E: Tests and kind gates. | Unit/component/native/gRPC-Web tests pass, frontline HTTP-01 transport coverage proves the operator token interceptor is used, plus `./scripts/test-kind-e2e-stateless.sh` and `./scripts/test-kind-e2e-grpc-web.sh` with static auth enabled. |
| Complete | 13F: Operator documentation and runbook updates. | Operator guide and runbook now describe configuration, rotation, failure diagnosis, auth observability, and recommended network boundaries. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Native gRPC operator APIs reject unauthenticated and unauthorized requests before store mutation. | `api_transport` auth tests assert missing/wrong-role credentials fail and leave store mutation counters unchanged; valid operator credentials still work. |
| Complete | Native gRPC proxy and sidecar APIs reject unauthenticated and unauthorized requests before wake/sleep/subscribe logic runs. | `proxy_api_transport` and `sidecar_api_transport` auth tests assert missing/wrong-role credentials fail before wake/subscribe/report-idle side effects; valid proxy/sidecar credentials still work. |
| Complete | gRPC-Web operator requests support authenticated browser-shaped calls. | gRPC-Web transport and kind tests cover CORS/preflight, valid `Authorization`, missing/invalid credentials, structured grpc-web status, and no mutation on failure. |
| Complete | Auth failures use stable `Unauthenticated` or `PermissionDenied` status codes. | Provider/interceptor and transport tests assert `Unauthenticated` for missing/malformed/invalid credentials and `PermissionDenied` for wrong-role credentials, with no secret values in status text or auth logs. |
| Complete | Static-token config supports distinct operator, proxy, and sidecar credentials. | `RuntimeConfig` tests cover no-auth, static auth parsing, missing credentials, malformed credentials, duplicate credentials, and missing auth mode fail-closed behavior. |
| Complete | No-auth mode is explicit and documented as local-development only. | Runtime startup requires `SLEEPYPODS_CONTROL_PLANE_AUTH_MODE`; scripts that intentionally run without auth set `no-auth`, while auth E2E scripts use static tokens. |
| Complete | Observability records accepted, rejected, and unauthorized calls without recording credential material. | `auth.rs` and `proxy-core` tests cover accepted, missing, invalid, and wrong-role decisions and assert credential strings are absent from captured events. |
| Complete | kind E2E proves auth-enabled control plane works with deployed frontend and sidecar. | `./scripts/test-kind-e2e-stateless.sh` passed with production images, static auth, authenticated frontline/sidecar runtime calls, runtime-injected sidecar token env, and invalid credential probes; `./scripts/test-kind-e2e-grpc-web.sh` passed for deployed gRPC-Web operator auth. |

## Milestone 14: Materialization Reconciliation and Multi-Controller Recovery

Historical milestone record: the tables below retain evidence from the earlier
implementation. Synchronous API progress, lease-only takeover, and the deferred
permanent-failure policy are superseded by the [remediation plan](review-remediation-plan.md),
especially its durable-intent, effect-fencing and supervised-runtime phases.
Current APIs accept durable intent; the reconciler owns Kubernetes work. An
uncertain mutation can deliberately block takeover until audited recovery.
These historical test names and June gate results are not fresh validation of
the current implementation.

Make non-terminal materializations converge after crashes, partial Kubernetes
side effects, cleanup failures, and multiple control-plane replicas. This phase
closes the M12 crash-boundary gaps without weakening the safety invariant for
stateful resources: exclusivity keys are held while a materialization is
non-terminal, and they are released only after reconciliation proves cleanup is
safe or an explicit operator override is used.

The generalized failure mode is:

```text
DB has a non-terminal materialization, but the control plane does not know
whether the external Kubernetes side effect completed.
```

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Add a materialization reconciler for `Pending` and `Deleting` rows. | Added `MaterializationReconciler` startup/periodic worker with durable claim, lease renewal, bounded batch/concurrency, and shutdown wiring in runtime. Focused component tests cover Pending resume/complete, stale Pending cleanup/force-delete, Deleting cleanup/finalize, cleanup error, lease loss, and generation-race cleanup. |
| Complete | Add durable reconciliation leases. | Migration `0006_materialization_reconciliation_leases.sql` adds `reconcile_owner`, `reconcile_lease_expires_at_unix_millis`, and `reconcile_attempt`; Postgres claim/renew/release use conditional updates. Real Postgres conformance covers claim race, same-owner duplicate claim rejection, expiry takeover, wrong-owner renew, expired same-owner renew/finalization rejection, and release behavior. |
| Complete | Keep reconciliation safe with multiple control-plane replicas. | Store finalization paths require current lease ownership plus materialization id, state, target, and generation; component test covers lease loss before finalize; Postgres conformance covers claim race, expiry takeover, stale-owner rejection, and same-key blocking; `SLEEPYPODS_KIND_E2E_CONTROL_PLANE_REPLICAS=2 ./scripts/test-kind-e2e-restart.sh` passed on 2026-06-24 with two production control-plane replicas against one Postgres/Kubernetes target. |
| Superseded | Make Kubernetes side effects idempotent. | Historical implementation/evidence: Reconciler and synchronous wake/delete paths route apply, readiness, inspection, and owned-only cleanup through the shared projection layer. `./scripts/test-kind-e2e-projection-drift.sh` passed with production images in kind and proves live missing refs, unowned same-name collisions, finalizer-blocked cleanup, and cleanup convergence are handled without deleting foreign objects or publishing stale ownership state. Current behavior and evidence: [remediation plan](review-remediation-plan.md). |
| Complete | Reconstruct desired manifests safely. | Pending reconciliation re-renders from immutable workload class version, instance values/generation, target namespace, resolved sleep policy, and runtime sidecar token, then requires rendered refs to match persisted refs. Persisted refs are the V1 verification boundary; no separate digest is needed for the current object-ref safety contract. |
| Complete | Recover `Pending` materializations. | Implemented current-wake reapply/readiness/lease-owned `complete_wake`, and stale/terminal cleanup through a guarded lease-owned delete transition after Kubernetes delete succeeds. `pending_reconciliation_applies_waits_and_completes_current_wake`, `pending_reconciliation_resumes_running_projection_after_apply_before_complete_crash`, `pending_stale_generation_deletes_refs_and_marks_deleted_after_cleanup`, and `stale_cleanup_does_not_delete_newer_same_id_materialization` cover deterministic recovery, the post-apply/pre-complete crash boundary, and stale same-id race paths. |
| Complete | Recover `Deleting` materializations. | Implemented re-delete recorded refs, tolerate missing refs through materializer delete-if-exists, lease-owned `finalize_sleep`, and no finalize after lease loss. Component tests cover success, missing refs, cleanup failure, lease loss, and generation-race cleanup. |
| Complete | Keep suspicious `Ready` drift out of the critical path for V1. | `Ready` drift is observe/report-only by design for V1: `ReconcileMaterialization` reports missing refs, unowned refs, ownership-generation drift, deletion/finalizer state, and non-blocking readiness state without attempting automatic live backend replacement. `./scripts/test-kind-e2e-projection-drift.sh` passed and proves deployed operator inspection reports missing and unowned Ready Service drift while traffic state remains stable. |
| Superseded | Add explicit operator recovery tools. | Historical implementation/evidence: Added audited store operations and operator gRPC/gRPC-Web RPCs for `ReconcileMaterialization`, `ForceDeleteMaterialization`, and `ForceReleaseExclusivityKey`, with transport tests. `ReconcileMaterialization` inspects state/lease/refs and triggers one synchronous reconciliation attempt for `Pending`/`Deleting`. Current behavior and evidence: [remediation plan](review-remediation-plan.md). |
| Complete | Add low-cardinality reconciliation observability. | Reconciler emits low-cardinality reconciliation events and materialization failure metrics without key values or token material. Runtime metrics expose state-only non-terminal counts, oldest ages, and held-key counts for alerting on stuck work/finalizers/outages; `materialization_operational_metrics_make_stuck_work_alertable_by_state` and `./scripts/test-kind-e2e-metrics.sh` passed. |
| Complete | Document lock semantics and operator runbooks. | Updated `docs/operator-guide.md` and `docs/operator-runbook.md` for durable leases, non-terminal key blocking, force-delete, and unsafe force-release semantics. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 14A: Store schema and lease API. | Added migration, trait methods, Postgres claim/renew/release/finalize operations, and real Postgres conformance for claim races, expiry takeover, renew/finalize ownership checks, stale generation rejection, same-key blocking, and force-release. |
| Complete | 14B: Reconciler core. | Added startup/periodic loop, jitter, batch size, concurrency limit, lease renewal, shutdown handling, and interval-based retry without long DB transactions around Kubernetes calls. |
| Complete | 14C: Pending reconciliation. | Implemented and tested re-render/ref verification, idempotent apply/readiness, lease-owned `complete_wake`, stale-generation cleanup, and no finalize after lease loss. |
| Complete | 14D: Deleting reconciliation. | Implemented and tested cleanup success, already-missing refs, retryable cleanup failure, lease loss before finalize, and stale generation cleanup. |
| Complete | 14E: Two-replica control-plane safety. | Store-level race/expiry semantics are covered with real Postgres; `SLEEPYPODS_KIND_E2E_CONTROL_PLANE_REPLICAS=2 ./scripts/test-kind-e2e-restart.sh` passed on 2026-06-24 and exercised deployed wake, sleep, delete, HTTP-01, and route-reassignment recovery with two control-plane pods sharing Postgres and Kubernetes. |
| Complete | 14F: Failure-injection harness. | Added focused fake Kubernetes/reconciler tests for Pending resume/complete, stale Pending cleanup, Deleting cleanup/finalize, missing refs, cleanup failure, lease loss, and generation-race cleanup; existing wake tests cover failed apply/PVC/readiness boundaries before and after Pending commit. |
| Complete | 14G: Operator recovery APIs and docs. | Added inspected one-shot `ReconcileMaterialization`, audited force-delete/force-release operator RPCs, transport tests, and operator guide/runbook coverage. |

Required failure tests:

| Status | Scenario | Expected behavior |
| --- | --- | --- |
| Complete | Crash before `record_materialization` commits. | `wake::tests::record_materialization_failure_prevents_kubernetes_apply` proves a pending-record failure applies, waits, and deletes nothing; same-key follow-up safety is covered by the Postgres same-key non-terminal conflict and force-release coverage. |
| Complete | Crash after `Pending` commit before first Kubernetes apply. | `pending_reconciliation_applies_waits_and_completes_current_wake` proves the reconciler sees recorded refs, resumes apply/readiness, and completes `Ready`; Postgres conformance proves same-key contenders remain blocked while the row is non-terminal. |
| Superseded | Crash after first apply failure with no objects applied. | Historical implementation/evidence: `wake::tests::first_apply_failure_after_exclusivity_acquire_releases_pending_key` covers the synchronous crash-boundary cleanup path before objects exist; reconciler stale Pending cleanup covers the persisted-row recovery path. Current behavior and evidence: [remediation plan](review-remediation-plan.md). |
| Complete | Crash after PV apply but before PVC apply. | The reconciler replays the full persisted manifest with server-side apply and recorded refs; `pvc_bound_failure_keeps_pending_stateful_refs_for_delete_or_retry` plus `delete_after_pending_before_apply_cleans_objects_applied_by_wake` cover retained refs and cleanup after partial apply. |
| Complete | Crash after PV/PVC apply before Service/workload apply. | Reconciliation reapplies the full desired manifest in deterministic apply order from persisted refs; existing wake failure tests cover retained PV/PVC refs for later delete/retry cleanup. |
| Complete | Crash after workload apply before PVC-bound wait completes. | `pvc_bound_failure_keeps_pending_stateful_refs_for_delete_or_retry` proves the row retains refs without backend for delete/retry cleanup; Pending reconciliation resumes waits/readiness for current wake. |
| Complete | Crash during readiness wait after all objects exist but before backend is recorded. | `pending_reconciliation_applies_waits_and_completes_current_wake` proves readiness is rechecked and `complete_wake` is lease-owned; `./scripts/test-kind-e2e-failures.sh` proves deployed failed readiness does not publish a backend. |
| Complete | Crash after backend is observed but before `complete_wake` commits. | Reconciler rechecks readiness and uses lease-owned `complete_wake`; `pending_reconciliation_resumes_running_projection_after_apply_before_complete_crash` proves objects already applied with the final Running-generation projection are treated as owned during recovery; Postgres conformance rejects stale-owner and stale-generation wake completion. |
| Complete | K8s apply returns retryable/unavailable. | Reconciler releases the lease and leaves the row non-terminal for interval-based retry on materializer apply errors; wake/materializer failure tests prove failed apply does not publish backend or release keys without cleanup. |
| Partial | K8s apply returns permanent invalid-object error. | V1 treats materializer apply errors conservatively as retryable reconciliation failures, keeping keys held. Automatic classification into terminal `Failed` is deferred because unsafe permanent/transient classification could release singleton keys too early; operator `ReconcileMaterialization` plus force tools provide the audited escape hatch. |
| Complete | Delete requested while wake is `Pending` before any apply. | `pending_stale_generation_deletes_refs_and_marks_deleted_after_cleanup` covers stale/terminal Pending cleanup and guarded lease-owned delete after Kubernetes delete succeeds; empty refs are safe no-op cleanup through the same path. |
| Complete | Delete requested while wake is `Pending` after partial apply. | Reconciler deletes recorded refs and force-deletes only after cleanup; `delete_after_pending_before_apply_cleans_objects_applied_by_wake` and `delete_after_pending_before_readiness_failure_cleans_objects_applied_by_wake` cover delete races after Pending commit. |
| Complete | Crash after `begin_sleep` marks materialization `Deleting` before cleanup. | `deleting_reconciliation_deletes_refs_and_finalizes_with_current_lease` covers the row immediately after `begin_sleep`, re-delete, and lease-owned finalize. |
| Complete | Crash after some cleanup deletes succeed. | `deleting_reconciliation_tolerates_missing_refs_and_finalizes` covers idempotent delete-if-exists behavior and finalize after missing refs are tolerated. |
| Complete | Crash after all cleanup succeeds before `finalize_sleep` commits. | The same missing-ref Deleting test covers already-deleted refs before finalize; Postgres conformance verifies lease-owned finalize clears keys and marks `Deleted`. |
| Complete | Cleanup fails because Kubernetes is unavailable. | `reconciler::tests::deleting_reconciliation_keeps_keys_when_cleanup_fails` covers a materializer delete error leaving the `Deleting` row non-terminal with keys held for later retry. |
| Complete | Cleanup fails because one object has a finalizer or deletion is stuck. | Materialization stays `Deleting` and keys remain held when projection cleanup is blocked by owned objects with deletion timestamps/finalizers. `ReconcileMaterialization` reports finalizer-blocked refs for operator inspection, state-only stuck-work metrics make the held work alertable, and `./scripts/test-kind-e2e-projection-drift.sh` passed after injecting and removing a live StatefulSet finalizer through the deployed platform. |
| Complete | Manual Kubernetes drift removes all refs while DB remains `Pending` or `Deleting`. | Current `Pending` rows re-apply from persisted desired refs; stale/terminal `Pending` and `Deleting` rows delete recorded refs idempotently and finalize only after cleanup succeeds or is already gone. |
| Complete | Same instance/generation retries while its own key is held. | Postgres conformance `exercise_materialization_reconciliation_leases` covers same-key self-retry while the current non-terminal materialization holds the key. |
| Complete | Different instance with same key wakes while owner is non-terminal. | Postgres conformance `exercise_materialization_reconciliation_leases` verifies a same-key contender receives `ExclusivityConflict` while the owner remains non-terminal; `./scripts/test-kind-e2e-exclusivity.sh` passed on 2026-06-24 after rebuilding production images and proves deployed same-key rejection without applying objects plus unrelated-key independence. |
| Complete | Stale generation tries to complete wake, finalize sleep, or release keys. | Postgres conformance covers stale-owner and stale-generation rejection for lease-owned `complete_wake`/`finalize_sleep`; force-release requires explicit audited operator override. |
| Complete | Route subscription observes a `Pending`/failed reconciliation row. | `proxy_subscribe_withholds_backend_without_ready_materialization` covers direct SubscribeRoute responses for absent, `Pending`, `Failed`, `Deleting`, and `Deleted` materialization rows and asserts none publish a backend URI or backend generation; `cargo test -p control-plane --test proxy_api_transport proxy_subscribe_withholds_backend_without_ready_materialization --quiet` passed locally. `./scripts/test-kind-e2e-failures.sh` also passed on 2026-06-24 and proves deployed readiness/PVC-bound failure paths return no backend and reject stale proxy wake generations. |
| Complete | Reconciler process loses its lease mid-operation. | `reconciler::tests::deleting_reconciliation_does_not_finalize_after_lease_loss` proves final DB updates stop after lease loss; Kubernetes delete remains idempotent through recorded refs. |
| Complete | Two control-plane replicas claim the same materialization concurrently. | Postgres conformance verifies exactly one conditional claim succeeds in a race. |
| Complete | Lease holder crashes during reconciliation. | Postgres conformance covers lease expiry takeover; the two-replica kind restart gate passed with controller pod deletion/scale-down recovery. |
| Complete | Lease holder is slow and another replica takes over after expiry. | Postgres conformance verifies expired-lease takeover and rejects stale-owner finalization after takeover. |
| Complete | Concurrent API wake/sleep/delete races with reconciliation. | Postgres conformance verifies generation/state/lease ownership guards for stale wake/finalize, expired same-owner finalization rejection, same-key contention, and stale cleanup racing a newer same-id materialization; `deleting_reconciliation_marks_deleted_after_generation_race_cleanup` and `stale_cleanup_does_not_delete_newer_same_id_materialization` cover cleanup plus guarded DB finalization after instance/materialization generation changes. |
| Complete | Rolling restart with two control-plane replicas and active `Pending`/`Deleting` rows. | `SLEEPYPODS_KIND_E2E_CONTROL_PLANE_REPLICAS=2 ./scripts/test-kind-e2e-restart.sh` passed on 2026-06-24, scaling/restarting deployed control-plane pods through wake, sleep, and delete recovery against shared Postgres/Kubernetes. |
| Complete | Full-platform kind gate with two control-plane replicas. | `SLEEPYPODS_KIND_E2E_CONTROL_PLANE_REPLICAS=2 ./scripts/test-kind-e2e-restart.sh` passed on 2026-06-24 after building production images; it deployed two control-plane pods against one Postgres/Kubernetes target and proved wake, sleep, delete, HTTP-01, and route-reassignment recovery. Additional deployed safety gates `./scripts/test-kind-e2e-exclusivity.sh` and `./scripts/test-kind-e2e-failures.sh` also passed on 2026-06-24. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Non-terminal materializations eventually converge when Kubernetes and Postgres are available. | Reconciler component tests cover `Pending` resume/complete, stale Pending cleanup, `Deleting` cleanup/finalize, missing refs, cleanup errors, lease loss, and generation races; Postgres conformance covers lease-owned transitions; two-replica restart kind gate proves deployed convergence. |
| Complete | Exclusivity keys remain held during uncertainty and are released after proven cleanup. | Postgres conformance and deployed exclusivity kind gate prove same-key conflict while non-terminal; Deleting/Pending cleanup tests and Postgres finalize coverage prove keys are cleared only after cleanup/finalize or audited operator override. |
| Complete | Multiple control-plane replicas do not duplicate or corrupt reconciliation. | Postgres conformance proves single claim, lease renewal ownership checks, expiry takeover, and stale-owner finalization rejection; the two-replica restart kind gate passed with deployed Kubernetes apply/delete/recovery paths. |
| Complete | No DB transaction is held across Kubernetes calls. | Reconciler claims/renews/releases via short store calls around materializer calls; final transitions are lease-owned conditional updates rather than long DB transactions or process-local ownership. |
| Complete | Operators have a safe manual escape hatch. | `ReconcileMaterialization` inspects state/lease/refs and triggers one attempt; `ForceDeleteMaterialization` and `ForceReleaseExclusivityKey` require operator/reason audit fields and are covered by transport tests and operator docs/runbook warnings. |
| Superseded | Existing wake/sleep/delete paths remain simple. | Historical implementation/evidence: API paths still attempt synchronous progress; unfinished `Pending`/`Deleting` work is represented as durable materialization rows and resumed by the reconciler rather than process-local special cases. Current behavior and evidence: [remediation plan](review-remediation-plan.md). |

## Milestone 15: Prometheus Export and Controller Operations Metrics

Ship a small, production-useful Prometheus surface for the metrics operators
need to run SleepyPods under failure. Do not duplicate metrics Kubernetes and
the container runtime already provide, such as CPU, memory, restart count,
network bytes, filesystem bytes, or pod scheduling status.

Scope:

- Add an optional Prometheus `/metrics` listener for `control-plane`,
  `frontline`, and `sidecar`, configured with one explicit metrics listen
  address per binary.
- Preserve the existing backend-neutral observability recorder boundary so tests
  and non-Prometheus deployments can still use stderr or another sink.
- Export the existing low-cardinality `sleepypods_*` runtime/proxy metrics
  without changing their names or labels.
- Add only the missing controller-operation metrics needed to diagnose
  reconciliation health, backlog, and lock safety.
- Avoid labels with instance ids, route ids, hostnames, SNI values, volume
  handles, Kubernetes object names, pod names, request paths, tokens, or error
  strings.

Prometheus metrics to add:

| Metric | Kind | Labels | Use |
| --- | --- | --- | --- |
| `sleepypods_reconciler_runs_total` | counter | `outcome` | Scheduler/reconciler loop rate and failures. |
| `sleepypods_reconciler_run_duration_seconds` | histogram | `outcome` | Time spent in one reconciliation pass. |
| `sleepypods_reconciler_candidates_total` | counter | `state` | Number of `Pending`/`Deleting` rows selected for reconciliation. |
| `sleepypods_reconciler_claims_total` | counter | `state`, `outcome` | Lease claim success/rejection/error rate across replicas. |
| `sleepypods_reconciler_lease_renewals_total` | counter | `outcome` | Lease renewal success and lease-loss rate during long Kubernetes operations. |
| `sleepypods_materializations_nonterminal` | gauge | `state` | Current backlog of `Pending` and `Deleting` materializations. |
| `sleepypods_materialization_oldest_nonterminal_age_seconds` | gauge | `state` | Stuck-work age for alerting on held locks/finalizers/outages. |
| `sleepypods_exclusivity_keys_held` | gauge | `state` | Number of held exclusivity keys by materialization state. |
| `sleepypods_kubernetes_operations_total` | counter | `operation`, `outcome` | Apply/delete/readiness call success, failure, and timeout rate. |
| `sleepypods_kubernetes_operation_duration_seconds` | histogram | `operation`, `outcome` | Kubernetes API/materializer latency excluding app traffic proxying. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 15A: Prometheus sink and text exposition. | The historical `crates/proxy-core/src/observability/prometheus.rs` implementation (now `crates/sleepypods-observability/src/prometheus.rs`) aggregates counters, gauges, and histograms from `MetricObservation`, rejects descriptor label mismatches, escapes text exposition, renders deterministic HELP/TYPE/sample lines, and has unit coverage for counters, gauges, histograms, escaping, label allow-listing, and `/metrics` scraping. |
| Complete | 15B: Metrics listener wiring. | `SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR`, `SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR`, and `SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR` opt into a dedicated `/metrics` listener while leaving stderr installed; env parsing tests cover disabled/default and configured listeners, and the exporter listener test proves `/metrics` does not intercept other paths. |
| Complete | 15C: Controller metric descriptors. | `crates/proxy-core/src/observability/metrics.rs` (historical location; current vocabulary is `crates/sleepypods-observability/src/metrics.rs`) defines exactly the Milestone 15 controller descriptors and keeps existing runtime/proxy descriptors unchanged; reconciler tests assert bounded run, candidate, claim, lease-renewal, apply, readiness, and delete observations. |
| Complete | 15D: Store-backed operational gauges. | `ControlPlaneStore::load_materialization_operational_metrics` returns provider-neutral bounded aggregates split between pending/deleting backlog age/count and non-deleted held-key counts, the Postgres implementation uses grouped aggregate queries, and `postgres_store` conformance asserts ready materializations are not counted as backlog while held-key visibility remains available. |
| Complete | 15E: Tests and operator docs. | Unit/local tests cover exposition, label allow-lists, histogram rendering, forbidden high-cardinality labels, env parsing, reconciler metrics, Postgres gauges, local `/metrics` scraping, and state-only stuck-work alertability through `materialization_operational_metrics_make_stuck_work_alertable_by_state`. `docs/operator-guide.md` and `docs/operator-runbook.md` list scrape config, metric names, dashboard panels, alert suggestions, and excluded CPU/memory/restart/network/filesystem/pod scheduling labels. `./scripts/test-kind-e2e-metrics.sh` passed after building and loading production `control-plane`, `frontline`, and `sidecar` images plus `postgres:17-alpine`, deploying dedicated metrics listeners in kind, scraping each `/metrics`, checking non-`/metrics` paths on the metrics listeners, and proving main listeners do not serve Prometheus metrics. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Operators can scrape every production binary through a dedicated metrics listener. | All three binaries have explicit opt-in metrics listen env vars, local exporter scrape coverage verifies `/metrics` and non-`/metrics` isolation, and `./scripts/test-kind-e2e-metrics.sh` passed with production `control-plane`, `frontline`, and `sidecar` images deployed in kind. The gate scrapes each dedicated metrics listener and also verifies the main API/proxy listeners do not serve Prometheus metrics from `/metrics`. |
| Complete | Prometheus output includes existing runtime/proxy metrics plus the small controller set above. | Descriptor tests assert all existing and new metric names, kinds, units, and labels; Prometheus tests render existing runtime gauges and new controller counters/histograms. |
| Complete | Reconciler scheduling health is visible without per-instance labels. | Reconciler tests prove run rate, run duration, candidate state, claim outcomes, and lease-loss/renewal outcomes are recorded with only bounded `state`/`outcome` labels. |
| Complete | Stuck materializations and held exclusivity keys are alertable. | Store-backed gauges expose pending/deleting backlog count and oldest age separately from held-key count by bounded state only, with Postgres conformance coverage. |
| Complete | Kubernetes API/materializer failures are visible without duplicating Kubernetes resource metrics. | Reconciler tests assert apply/readiness success and delete error metrics; descriptors restrict Kubernetes controller metrics to `operation` and `outcome`. |
| Complete | The metric surface remains intentionally small. | Operator guide and runbook updates explicitly exclude CPU, memory, restarts, pod scheduling, network, filesystem, instance ids, hostnames, route ids, SNI values, pod names, request paths, error strings, tokens, and volume handles from SleepyPods-owned Prometheus metrics. |

## Milestone 16: Kubernetes Projection Drift and Finalizer Safety

Centralize Kubernetes drift, ownership, finalizer, and stuck-deletion handling
behind one shared projection layer. The control-plane state machine remains the
policy owner, but all Kubernetes object observation, ownership proof,
apply/delete idempotency, readiness inspection, and finalizer reporting should
flow through one abstraction instead of being scattered across wake, sleep,
delete, and reconciliation paths.

Scope:

- Introduce a shared `ProjectionPlan`/`ProjectionReconciler` style abstraction
  for rendered objects, recorded refs, ownership stamps, apply, delete,
  readiness, and inspect operations.
- Add explicit ownership stamps to every rendered object, including
  materialization id, instance id, generation, rendered hash, and
  `managed-by=sleepypods`.
- Add a structured observation model for each recorded object ref:
  `Missing`, `PresentOwned`, `PresentUnowned`, `DeletingOwned`,
  `DeletingUnowned`, `Ready`, `Unready`, `ApplyRejected`, and `DeleteBlocked`
  with bounded reason/finalizer details.
- Make `Pending` reconciliation use projection observations to re-apply missing
  or partial owned objects, stop on unowned conflicts, and complete only after
  readiness is observed for the current generation.
- Make `Deleting` reconciliation use projection observations to delete only
  owned objects, tolerate missing owned refs, stay non-terminal while owned
  objects are blocked by finalizers, and finalize only after cleanup is proven.
- Keep `Ready` drift conservative in V1: inspect and report recorded-ref
  metadata drift such as missing, unowned, deleting, finalizer-blocked, or
  inspect-failed refs, but do not automatically replace live backends until a
  later policy explicitly opts into repair. Non-blocking readiness/EndpointSlice
  drift and rendered-hash drift for Ready rows require later manifest/hash
  reconstruction work.
- Surface operator-readable drift and finalizer state through
  `ReconcileMaterialization` and runbook guidance without exposing high
  cardinality labels in metrics.
- Preserve the existing safety posture for singleton resources: exclusivity keys
  remain held while cleanup is uncertain, including finalizer-blocked deletes
  and ownership conflicts.

Sub-phases:

- 16A: Projection model and ownership stamp definitions.
- 16B: Kubernetes inspection implementation for Deployment, StatefulSet,
  Service, PVC, and PV refs.
- 16C: Apply/delete/readiness paths routed through the shared projection layer.
- 16D: Pending reconciliation policy using projection observations.
- 16E: Deleting reconciliation policy using projection observations and
  finalizer-blocked delete reporting.
- 16F: Ready drift inspect/report behavior without automatic live repair.
- 16G: Operator API/runbook updates for unowned objects, finalizers, stuck
  deletes, and safe force operations.
- 16H: Extensive fake-Kubernetes, real-API, and kind tests for drift/finalizer
  edge cases.

Status update (2026-06-25):

| Status | Item | Evidence |
| --- | --- | --- |
| Complete | 16A: Projection model and ownership stamp definitions. | Added `projection.rs` with `ProjectionPlan`, `ProjectionReconciler`, bounded `ProjectionObservation`, deterministic rendered hashes, and ownership stamps for materialization id, instance id, instance generation, rendered hash, and `managed-by=sleepypods`. |
| Complete | 16B: Kubernetes inspection implementation. | `KubeMaterializerClient` can inspect live metadata/deletion/finalizers for Deployment, StatefulSet, Service, PVC, and PV refs through the shared projection layer, and can make a non-blocking Service/EndpointSlice readiness observation without polling. |
| Complete | 16C-16E: Apply/delete/readiness paths routed through projection. | Wake and Pending reconciliation call projection apply/readiness, inspecting and rejecting unowned/stale refs before server-side apply and readiness waits. Wake now renders/applies the final Running-generation projection before `complete_wake`, so live ownership labels match the Ready materialization generation. Operator delete, sidecar idle cleanup, wake terminal cleanup, Deleting reconciliation, and stale Pending cleanup route through owned-only projection delete. `./scripts/test-kind-e2e-projection-drift.sh` passed and proves deployed missing-ref, unowned-collision, and finalizer-blocked cleanup behavior. |
| Complete | 16F: Ready drift inspect/report behavior. | `ReconcileMaterialization` returns projection observations for recorded refs, including Ready rows when inspected by operators. This reports missing refs, ownership/generation/materialization-id drift, deletion/finalizer state, `inspect_failed`, and non-blocking Ready/Unready Service/EndpointSlice readiness observations without automatic repair. The projection-drift kind gate passed and proves live Ready Service deletion and unowned same-name replacement are reported without taking destructive action. Rendered-hash mutation repair remains post-V1 because Ready rows do not persist reconstructed manifests. |
| Complete | 16G: Operator API/runbook updates. | Added `projection_observations` to `ReconcileMaterializationResponse`, `ForceDeleteMaterializationResponse`, and `ForceReleaseExclusivityKeyResponse`, including bounded `inspect_failed` observations when Kubernetes inspection itself fails for a scoped materialization/ref. Updated operator guide/runbook guidance for missing refs, unowned refs, inspect failures, finalizer-blocked owned deletes, and force-operation observation limits. |
| Complete | 16H: Tests. | Added focused projection unit tests, real-client readiness-helper tests, wake regression coverage for unowned live refs before apply and final Running-generation projection stamps, reconciler tests for pending recovery after final-generation apply before `complete_wake`, pending unowned/stale stamps, deleting missing, owned, unowned, delete-error, lease-loss, and finalizer-blocked paths, API/sidecar transport coverage for owned-only cleanup, Ready metadata/readiness drift report-only behavior, inspect-failed observations, force-operation projection observations, and low-cardinality metric-label guards for object/finalizer/volume details. `cargo test -p control-plane wake::tests --quiet`, `cargo test -p control-plane reconciler::tests --quiet`, `cargo test -p control-plane --test kind_e2e_stateful projection_drift_and_finalizer_safety_through_deployed_platform -- --ignored --nocapture`, and `./scripts/test-kind-e2e-projection-drift.sh` passed. |

Done when:

- Unit tests cover ownership-stamp rendering for every Kubernetes object type
  and reject missing, malformed, stale, or mismatched stamps.
- Projection observation tests cover missing refs, owned present refs, unowned
  name collisions, generation mismatches, rendered-hash mismatches, deletion
  timestamps, finalizer lists, readiness false, absent endpoints, and unsupported
  object kinds.
- Pending reconciliation tests cover no objects applied, partially applied
  PV/PVC/Service/workload refs, manually deleted owned refs, unowned name
  collisions, stale generation objects, readiness flapping, and Kubernetes API
  read/apply failures.
- Deleting reconciliation tests cover already-missing refs, partial deletion,
  finalizer-blocked PVC/PV/workload refs, unowned same-name objects, Kubernetes
  delete errors, repeated retry, lease loss during delete, and finalization only
  after every owned ref is gone or proven safe.
- Ready drift tests cover manual workload deletion, Service selector mutation,
  endpoint loss, pod unready state, PVC/PV mutation, unowned object replacement,
  and prove V1 reports drift without automatically swapping or repairing the
  backend.
- Force-operation tests prove `ForceDeleteMaterialization` and
  `ForceReleaseExclusivityKey` surface the latest projection observation and
  require audited operator and reason fields before bypassing normal safety.
- Metrics/log tests expose low-cardinality drift/finalizer outcomes without
  labels containing instance ids, object names, route ids, hostnames, volume
  handles, finalizer names, or raw error strings.
- kind E2E tests inject manual deletion, label/annotation ownership drift,
  finalizer-blocked PVC/PV deletion, unowned same-name replacement, and Ready
  drift against production images and prove the reconciler either repairs,
  waits, reports, or blocks exactly according to the materialization state.
- The runbook tells operators how to distinguish safe missing-object cleanup
  from unsafe unowned-object conflicts and when force-release can allow duplicate
  singleton attachment.

## Stretch

The original goal is complete when the plan reaches this line. Do not start
stretch work unless explicitly asked.

### Stretch Phase 1: SleepySockets

Keep client WebSocket connections open at the frontline proxy while allowing the
upstream sidecar/app connection and workload to sleep when no application
messages have passed within the configured TTL.

Scope:

- Make SleepySockets opt-in per route or WorkloadClass; default WebSocket
  behavior remains normal passthrough.
- Terminate/intercept WebSockets at the frontline proxy, keep the client
  connection open, and create or recreate upstream WebSockets to the sidecar/app
  only when needed.
- Change idle accounting for SleepySockets from connection-open activity to
  application-message activity.
- When the message TTL expires, close the upstream WebSocket so the sidecar can
  observe idleness, report idle, and let the control plane sleep the workload.
- When a later client message arrives, wake the instance, recreate the upstream
  WebSocket to the sidecar/app, then forward the queued message.
- Document the application contract: this only works for L7, client-driven or
  resumable WebSocket protocols where losing backend-initiated messages while
  asleep is acceptable.

Done when:

- Frontline tests prove client WebSockets stay open across upstream close,
  workload sleep, wake, upstream reconnect, and message forwarding.
- Sidecar idle tests prove open SleepySockets client sessions do not prevent
  idle reporting when no application messages pass within the TTL.
- E2E tests prove a client message after sleep wakes the workload and reaches
  the app over a newly created upstream WebSocket.
- Tests cover ordering, buffering limits, ping/pong behavior, close behavior,
  backpressure, reconnect failure, and app-level resume/session token handling.
- Operator docs clearly state that SleepySockets is not transparent generic
  WebSocket sleep and requires an app protocol that tolerates upstream reconnect.

### Stretch Phase 2: Operator CLI

Plan and build a small operator-facing CLI for day-to-day SleepyPods
management. The CLI should wrap the stable control-plane API rather than
introducing a second resource model or writing directly to Kubernetes or the
database.

Scope:

- Pick a CLI name, command shape, config file format, authentication inputs, and
  output conventions before implementation starts.
- Support native gRPC first, with room for gRPC-Web or HTTP-compatible transport
  later if browser/V8 environments need to reuse the same command model.
- Provide commands for workload class versions, instances, route bindings,
  HTTP-01 challenges, materialization inspection, one-shot reconciliation, and
  force operations.
- Support concise human output by default and stable JSON output for automation
  and agents.
- Add dry-run and validation commands for `WorkloadClass` templates, instance
  values, route identities, and rendered Kubernetes object summaries before
  operators create resources.
- Keep dangerous operations explicit: force-delete and force-release commands
  must require operator identity, reason text, target identifiers, and a
  deliberate confirmation bypass for non-interactive automation.
- Make errors operator-readable while preserving structured status codes and
  machine-readable details in JSON mode.
- Document example workflows for creating a workload class, creating an
  instance, adding a route/custom domain, inspecting wake/sleep state,
  resolving a stuck materialization, and deleting an instance.

Done when:

- A CLI design document describes command names, flags, config precedence,
  authentication, output formats, exit codes, and dangerous-operation
  confirmation behavior.
- CLI unit tests cover argument parsing, config precedence, redacted auth
  display, JSON output stability, and error formatting.
- CLI integration tests run against a fake or in-process control-plane service
  and cover create/get/delete flows for workload classes, instances, route
  bindings, and HTTP-01 challenges.
- CLI recovery tests cover `ReconcileMaterialization`,
  `ForceDeleteMaterialization`, and `ForceReleaseExclusivityKey`, including
  required audit fields and non-interactive confirmation flags.
- CLI validation tests prove invalid templates, invalid instance values,
  duplicate route identities, unsafe force inputs, and malformed server
  endpoints fail before any unintended write.
- At least one kind E2E path uses the CLI against deployed production
  control-plane images for a full create route, cold wake, sleep, inspect, and
  delete workflow.
- Operator documentation uses CLI examples as the primary user-facing workflow
  while still linking to protobuf/API details for integration authors.

## Deferred

These should not shape V1 implementation details beyond keeping clear extension
points:

- HTTP/3 and QUIC listener.
- Multi-cluster remote forwarding and materialization leases.
- StatefulSet scale above one.
- Managed provider volume creation/deletion.
- Rich route predicates such as headers, ALPN, or arbitrary expressions.
