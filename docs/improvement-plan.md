# Improvement Plan

## Overarching Goal

Make SleepyPods hold its guarantees under ordinary faults and realistic
traffic: one bad request, one failed lookup, one API-server error or one
database blip degrades only the work it touches; every client protocol reaches
the app; a cold request waits for the pod and little else; and the lifecycle
recovers without an operator. Then shrink the code that carries those
guarantees.

This plan comes from a whole-codebase review of `1a472fd` on 2026-11-27 covering
architecture, simplicity, performance and correctness. APIs, schema and
internal architecture may change, with clean breaks. The guarantees stay:
accepted lifecycle intent is durable, a stale driver cannot act on a newer
incarnation, exclusivity is held until absence is proven, drain is finite, a
cached route serves only what the control plane certified, and resources are
bounded. Sections 3A, 4H and 5C propose sharper statements of three of them.

The transactional core is sound and stays: lease fencing on the database
clock, reservation triggers, single-snapshot route resolution, conditional
Kubernetes patch and delete, sidecar idle accounting, role separation and
certificate sealing all held up under review.

### Priorities

P1 is a correctness or availability defect. P2 is a performance or scale limit.
P3 is simplification. Evidence is **Reproduced** (a probe test ran against the
real components), **Traced** (code read end to end) or **Modelled** (an
estimate or design argument that needs measurement or a spike first).

| Priority | Finding and consequence | Evidence | Work |
| --- | --- | --- | --- |
| P1 | One request with a path over 4,096 bytes ends the Subscribe stream, clears the frontline route cache, and its detached flight repeats that every 100 ms for 130 s. One unauthenticated request per 130 s keeps a frontline's routing down. | Reproduced | 1A, 1B |
| P1 | Any failed lookup (store error, timeout, 257th subscription) ends the stream and clears every cached route. Default cache capacity is 1,024 entries against a 256-subscription stream cap. | Reproduced | 1B, 1C |
| P1 | Route TTL equals stream lifetime (60 s each), so from the second rotation cached routes expire inside the 250 ms reconnect backoff. With 100 hot identities about a third of requests fail `Saturated` for ~500 ms each minute. | Reproduced with scaled timers | 1C |
| P1 | A stream that ends after lost events (`Lagged`, `Reset`) looks like an orderly rotation, so the frontline keeps serving the stale route. A change committed while a replica starts is never dispatched to subscribers already attached. | Reproduced (reset); Traced (startup) | 1D |
| P1 | One failed `accept()` (for example `EMFILE`) ends the frontline or sidecar process. | Traced | 1E |
| P1 | An HTTP/2 client cannot reach an HTTP/1.1-only app: each hop copies the client's protocol upstream, and frontline offers `h2` first in ALPN. Browsers on TLS-terminated routes get 502. The kind TLS gate pins its client to `http/1.1`. | Reproduced | 2A |
| P1 | HTTP/2 requests reach the app with the backend address as authority and without `X-Forwarded-Host`; `te: trailers` is stripped, which C-core gRPC servers reject. | Traced | 2B |
| P1 | The route cache, subscriptions and flights are keyed by host plus full path. Every distinct URL costs a control-plane round trip, a database query, a subscription and one of 64 flights, with lookups serialized per frontline. Clients choose paths, so they control every bound. | Traced; hit cost measured | 3A–3D |
| P1 | A waking instance holds one of 64 global flights per requested path for up to 130 s, after its clients disconnect, re-resolving at 10 Hz. 64 paths on cold or stuck hosts block every cache miss on that frontline. | Reproduced | 4F |
| P1 | A 5xx, reset, 10 s stall or cancellation during any Kubernetes mutation leaves an effect row that excludes the materialization from every later claim. Only `ForceDeleteMaterialization` clears it, and that leaves the instance in Waking, Running or Draining with no work row, so it cannot wake until deleted and recreated. | Traced | 5A–5D |
| P1 | One failed statement at a reconciler tick exits the control-plane process, and I/O errors on an open connection are classified as permanent. Restart needs Postgres, so an outage becomes a crash loop. | Traced | 5F |
| P1 | Cleanup that passes 600 s or meets a 401 or 403 is parked until an operator re-arms it; keys stay held and a deferred wake never starts. | Traced | 5E |
| P1 | Every lifecycle write holds one global revision row lock across client round trips, and sessions set no idle-in-transaction timeout. A replica frozen mid-transaction stalls every writer until TCP keepalive reaps the session. | Traced; stall Modelled | 5G, 7B |
| P1 | The certificate cache holds 29–206 certificates, where the documented bound is 1,024. Refreshes evict warm entries, and 1,024 junk SNIs can empty it. | Traced (arithmetic from constants) | 1G |
| P1 | Create RPCs accept classes and instances that cannot render, so the first error appears on a user request. `default_values` is documented and never applied. | Reproduced | 6A, 6C |
| P1 | Rendered names end in 32 bits of hash: `tenant-39588` and `tenant-67738` collide and can never be awake together. | Reproduced (computed) | 6B |
| P1 | The gRPC-Web listener is plaintext even when TLS is configured and allows any origin. | Traced | 6D |
| P2 | The control plane adds about 1.7 s mean and 3.4 s worst to a stateless cold start (3.7 s and 5.5 s with a PVC): a 1.0–1.2 s reconciler tick, 2 s readiness and PVC polls, a 250 ms event poll and a 100 ms frontline refresh. | Traced; totals Modelled | 4B, 4C, 4E, 4F |
| P2 | One replica runs three wakes at once and holds each slot through readiness. Cleanup sorts ahead of wakes: 50 instances idling out together stop wake starts for 27–55 s, and three never-ready instances hold every wake slot for 120 s at a time. | Traced; durations Modelled | 4D |
| P2 | A request during drain waits for grace, teardown and a cold start while the old pod is healthy. Teardown completes only at 2–32 s failure-backoff boundaries, and each normal sleep counts as a failure. | Traced | 4G, 4H |
| P2 | No statement is prepared: each costs two round trips and a fresh plan. Wake acceptance takes 16 round trips, an idle report 20, one Kubernetes mutation 14, a whole wake about 56. | Traced (counted) | 7A |
| P2 | Idle keep-alive connections close after the 10 s setup timeout while upstream pools keep them 30 s; every upstream connect resolves a Service DNS name through `getaddrinfo`. | Traced | 2C, 2D |
| P2 | Long-lived outbound connections have no liveness check; a blackholed peer stalls route resolution or `ReportIdle` for about 15 minutes. | Traced; failure Modelled | 1F |
| P2 | The control plane writes every metric and every accepted auth decision to stderr on a runtime worker. | Traced | 6F |
| P3 | The store trait has 52 methods written four times each; seven have only test callers and four of those bypass fencing. Eleven migrations carry backfills for designs that are gone. | Traced | 7E |
| P3 | About 1,000 frontline production lines serve a resolver only tests use, and six markers rebuild an ordering the stream already has. | Traced; redesign Modelled | 3D |
| P3 | The effect journal exists for one case, a late create. Instance and materialization store one state machine in two rows with four generation stamps. | Modelled | 5C, 7D |
| P3 | WebSockets are parsed and re-framed at both hops with a 64 MiB message buffer each; the manifest model mirrors `k8s-openapi` by hand; idempotency keys duplicate insert-or-compare on caller-chosen IDs. | Traced; savings Modelled | 1H, 8C–8E |

Phase 0 unblocks CI. Phases 1 and 2 are independent, small and next. Phases 3,
4 and 5 are the architectural work and can run in parallel after Phase 1.
Phases 6–8 follow in any order. Item IDs are stable references.

### Decisions for the owner

Each has a recommendation; the phase records the outcome.

- **3A, cache authority by host.** Cache the complete rule set for an exact
  host and match paths locally. A host's answer is complete, so a broad rule
  still cannot hide an exact-host or longer-path rule. Recommended.
- **4H, revive during drain.** A wake that arrives before teardown starts
  returns the instance to Running on the existing pod. Recommended.
- **5C, late creates.** Either create workloads with zero replicas and activate
  them with a conditional patch, which makes a late create inert and removes
  the effect journal, or keep the journal and resolve an uncertain create by
  re-inspection after a settle window longer than the API server's request
  timeout. Both replace operator settlement with bounded automatic recovery.
  Spike the first; fall back to the second.
- **7D, one lifecycle row.** Carry projection, lease, schedule and one
  incarnation stamp on the instance row. Decide after Phases 4 and 5 land.
- **8C, tunnel WebSockets.** Relay upgraded bytes instead of re-framing; this
  changes the per-hop challenge and extension clauses of the protocol
  contract. Recommended.
- **8D, render into `k8s-openapi` types** and drop Deployment and StatefulSet
  from the raw allow-list. Changes the rendered hash of live objects.
- **8E, drop idempotency keys** in favour of insert-or-compare on the
  caller-chosen ID.
- **6C, `default_values`.** Delete it; per-field defaults in the value schema
  already work.

## Implementation Principles

- A fault in one request, tenant or dependency degrades that request, tenant
  or dependency. Streams, caches, scheduler slots and processes are shared;
  keep them serving.
- Bounds count what the operator controls (hosts, instances, connections), and
  defaults on both ends of a protocol agree and are validated at startup.
- Push changes; keep a slow poll as the fallback.
- Recovery is automatic and bounded. Operator RPCs override policy; ordinary
  faults never require them.
- Fence Kubernetes effects with what the API server checks: UID and
  resourceVersion preconditions, and creates that are inert until activated.
- Tests exercise the paths production runs. Delete code that only tests call.
- Break APIs and schema cleanly; add no compatibility shims.
- Where the evidence is Modelled, measure or spike first and keep the smallest
  change that meets the gate.

## Testing Strategy

- Each P1 item starts as a failing regression test that reproduces the finding
  and stays as a permanent test.
- Every Rust change passes the nine steps in `.github/workflows/ci.yml`,
  including `cargo fmt --all --check`, clippy with warnings denied, the
  workspace tests, and the real-Postgres store target with its execution
  checker.
- Lifecycle, routing and protocol changes pass the matching
  `scripts/test-kind-e2e-*.sh` gate. Gates this plan adds join the CI kind job.
- Fault injection uses a fake Kubernetes client and a latency relay in front
  of Postgres: inject 5xx, resets, stalls and process loss at each boundary.
  Reproduce timing-dependent behaviour by delaying I/O.
- Performance items record before and after on one machine: the wake-latency
  gate (4A), database round trips per operation, Criterion `route_lookup` and
  `proxy_primitives`, and the load smokes.

## Phase 0: Green CI on a Pinned Toolchain

Goal:
CI passes on the toolchain it names, and a toolchain release changes CI only
through a commit.

Scope:
- 0A: Replace the three `fetch_update` calls that Rust 1.100 deprecates
  (`crates/frontline/src/control_plane_transport.rs:253`,
  `crates/control-plane/src/reconciler/fenced_client.rs:67`,
  `crates/control-plane/src/api/certificate_watch/tests.rs:75`). On 1.100,
  `cargo clippy --workspace --all-targets -- -D warnings` fails on them.
- 0B: Pin the toolchain in `rust-toolchain.toml` and have CI read it;
  `.github/workflows/ci.yml` installs floating `stable`.

Completion gate:
The nine CI steps pass on the pinned toolchain.

Testing plan:
- Run the CI step list locally on the pinned toolchain, then on a hosted run.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 0A: Replace deprecated `fetch_update` calls | Commit `2c27de5`: three call sites use `try_update`; `cargo clippy --workspace --all-targets -- -D warnings` exits 0 on 1.100.0. |
| Complete | Work | 0B: Pinned toolchain | Commit `2c27de5`: `rust-toolchain.toml` pins 1.100.0; both CI jobs run `rustup toolchain install`; the four image builds use `rust:1.100-bookworm`. |
| Incomplete | Gate | CI green on the pinned toolchain | Missing: hosted CI run. |

## Phase 1: Contain Single-Request and Single-Fault Failures in the Proxies

Goal:
One request, one failed lookup, one lost event or one socket error affects only
its own work on a frontline or sidecar.

Scope:
- 1A: Frontline answers 400 or 414 to a host over 253 bytes or a path over
  4,096 bytes before it subscribes. `crates/frontline/src/identity.rs:65-105`
  applies no bound; `crates/control-plane/src/api/proxy.rs:849-864` rejects the
  identity and `:288-292` ends the stream.
- 1B: Subscribe answers a failed lookup with a per-request error and keeps the
  stream for validation errors, store errors, lookup timeouts and cap
  exhaustion. A frontline flight treats a failed resolve as terminal, bounds
  `InvalidatedDuringResolution` retries, and stops when its last waiter leaves
  (`crates/frontline/src/route.rs:411-438`, `:726-737`).
- 1C: Defaults agree and startup validates them. Positive TTL exceeds stream
  lifetime plus a margin (`crates/control-plane/src/api/admission.rs:41,44`),
  frontline cache capacity fits the server's per-stream cap
  (`crates/frontline/src/config.rs:18`, `admission.rs:40`), and the transport
  reconnects at once after a clean end
  (`crates/frontline/src/control_plane_transport.rs:239-242`).
- 1D: A stream ended by `Lagged`, `Reset` or an enqueue timeout carries a
  terminal error status, so the frontline clears authority
  (`api/proxy.rs:289,296,301`). A dispatcher read failure keeps streams open,
  and the dispatcher loads its cursor before listeners serve
  (`crates/control-plane/src/runtime.rs:543-557`, `:687-715`).
- 1E: One `proxy_core` accept helper serves the six accept loops: continue on
  per-connection errors, back off on resource errors, close the listener when
  shutdown starts, and set nodelay and keepalive on accepted and dialled
  sockets (`crates/frontline/src/listener.rs:385-388,459-465,532-538`;
  `crates/sidecar/src/runtime.rs:180-183,281-284`;
  `crates/sidecar/src/readiness.rs:37`).
- 1F: HTTP/2 and TCP keepalive on the frontline and sidecar control-plane
  channels (`crates/sleepypods-api/src/transport.rs:20`) and on upstream HTTP
  clients (`crates/proxy-core/src/http.rs:79-87,443-448,521-526`); a deadline
  on `ReportIdle` (`crates/sidecar/src/idle/control_plane.rs:53-56`).
- 1G: The certificate cache is bounded by entry count with a separate budget
  for negative entries, and a handshake waits for a fetch slot until its 3 s
  deadline (`crates/frontline/src/certificates/mod.rs:33-49,248-276,310-332`;
  `worker.rs:41-48,306-311`).
- 1H: WebSocket relays cap message and frame size at about 1 MiB
  (`crates/proxy-core/src/websocket.rs:356-357,390-392` take the 64 MiB
  default). 8C supersedes this if accepted.

Out of scope:
- Re-keying the route cache (Phase 3) and waiter parking (4F).

Completion gate:
With 256 routes cached and hot, each of these leaves every unrelated route
serving from cache: a 5,000-byte path, a failed store lookup, a 257th identity,
a stream rotation with 250 hot identities, and an injected `accept()` error. A
lagged stream clears authority before the next request. 1,024 real certificates
stay cached across a refresh cycle.

Testing plan:
- Compose the real control-plane proxy service with the frontline transport,
  actor and cache, as `crates/control-plane/tests/proxy_api_transport.rs`
  does, for 1A–1D.
- Listener tests with an accept shim that returns `EMFILE` and
  `ECONNABORTED`; a blackholed control-plane socket for 1F.
- A certificate-cache test that installs 1,024 real certificates and runs a
  full refresh cycle with three fetches in flight.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 1A: Bound host and path in frontline | Missing: length check, 400/414 response and regression test. |
| Incomplete | Work | 1B: Per-request Subscribe errors and terminal flights | Missing: proto reply, stream kept on lookup failure, flight retry bound, waiter-less flight cancellation. |
| Incomplete | Work | 1C: Agreeing, validated defaults and eager reconnect | Missing: TTL above lifetime, capacity within stream cap, startup validation, reconnect without backoff after a clean end. |
| Incomplete | Work | 1D: Lossy ends clear authority; cursor before serving | Missing: terminal status on `Lagged`/`Reset`, streams kept on read failure, cursor load ahead of listeners. |
| Incomplete | Work | 1E: Shared resilient accept helper | Missing: helper in `proxy_core` and its use by all six loops. |
| Incomplete | Work | 1F: Outbound liveness | Missing: keepalive settings on both control-plane channels and upstream clients; `ReportIdle` deadline. |
| Complete | Work | 1G: Count-bounded certificate cache | Commits `5da8ab7`, `9a4da04`, `e73bd7f`, `1e287de`: 1,024 certificates and 1,024 negative entries in separate budgets; lookups queue for a fetch slot, at most 64 handshakes waiting; worst-case memory about 220 MiB, stated in the operator guide. |
| Incomplete | Work | 1H: WebSocket message cap | Missing: explicit `WebSocketConfig` limits. |
| Incomplete | Gate | Unrelated cached routes keep serving through each fault | Missing: the composed regression tests above, passing. |
| Complete | Test | 1,024 certificates across a refresh cycle | `full_cache_stays_warm_through_a_refresh_cycle_with_three_fetches_in_flight` (the earlier code kept 407 of 1,024), `unknown_hostnames_leave_every_certificate_cached`, and `a_full_lookup_wait_serves_cached_hostnames_and_fails_further_lookups_at_once` in `crates/frontline/src/certificates/capacity_tests.rs`. |
| Incomplete | Test | Routing kind gate | Missing: passing `scripts/test-kind-e2e-routing.sh` run. |

## Phase 2: Deliver Every Client Protocol to the App

Goal:
The protocol a client speaks to frontline is independent of the protocol the
app speaks, and the app sees the client's authority.

Scope:
- 2A: The app-hop protocol is a property of the workload port (`http1` by
  default, `h2c` opt-in, following Kubernetes `appProtocol`), carried to the
  sidecar. `crates/proxy-core/src/http.rs:121,147-151` selects the upstream
  client from `request.version()`;
  `crates/frontline/src/certificates/worker.rs:331` advertises `h2` first.
- 2B: On an HTTP/1.1 app hop, set `Host` from the original authority. Build
  `X-Forwarded-Host` from the URI authority when `Host` is absent. Send
  `te: trailers` upstream on HTTP/2 when the client sent it
  (`http.rs:540,546,633`).
- 2C: Give idle keep-alive its own timeout (for example 90 s) and reject
  configuration where the upstream pool idle time reaches it
  (`http.rs:221-225`; `crates/proxy-core/src/resources.rs:40`).
- 2D: Publish the Service ClusterIP as the backend address, or resolve once at
  route install and keep a `SocketAddr` and parsed `Uri` in the route entry
  (`crates/control-plane/src/kube_materializer.rs:593-596`; `http.rs:476-477`;
  `crates/proxy-core/src/connect.rs:71`).

Out of scope:
- HTTP/3, and TLS session resumption.

Completion gate:
In kind, HTTP/1.1, HTTP/2 and WebSocket clients each reach an HTTP/1.1-only app
and an `h2c` app through frontline and the sidecar, and the app observes the
client's host. A C-core gRPC server on an `h2c` port serves a unary and a
streaming call.

Testing plan:
- A `proxy-core` test that sends an HTTP/2 request to an HTTP/1.1-only
  upstream (today the upstream reports `Parse(VersionH2)`).
- Extend the protocol and TLS kind gates with an `h2`-negotiating client
  against the HTTP/1.1 Python app, and a grpcio (C-core) server.
- A keep-alive test with requests 15 s apart on one connection.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 2A: App protocol per workload port | Missing: class field, sidecar setting, upstream client selection by declared protocol. |
| Incomplete | Work | 2B: Authority and `te: trailers` across hops | Missing: `Host` and `X-Forwarded-Host` from authority; `te` forwarding on HTTP/2. |
| Incomplete | Work | 2C: Separate keep-alive idle timeout | Missing: setting, default and pool-idle validation. |
| Incomplete | Work | 2D: Resolved backend address | Missing: ClusterIP publication or resolve-once route entry. |
| Incomplete | Gate | Every client protocol reaches both app kinds | Missing: kind runs for each client and app combination. |
| Incomplete | Test | HTTP/2 client to HTTP/1.1 upstream | Missing: permanent `proxy-core` regression test. |
| Incomplete | Test | C-core gRPC through both proxies | Missing: grpcio fixture in the protocol gate. |

## Phase 3: Route by Host Rule Set

Goal:
After one lookup for a host, every path on that host resolves locally, and
routing cost and bounds scale with hosts.

Scope:
- 3A (decision): The cache authority is the exact queried host with its
  complete rule set. Update the north star's cache section to match.
- 3B: `SubscribeRoute(kind, host)` returns every binding for that exact host
  plus the wildcard-suffix bindings that apply, each with instance state and
  backend. The subscription depends on those bindings, their instances, and
  any route change for the host or its suffixes; the control plane pushes the
  updated rule set when a dependency changes. Lookups run concurrently per
  stream and subscriptions are indexed by instance
  (`crates/control-plane/src/api/proxy.rs:281-287,351-358`).
- 3C: Frontline keys cache, subscription and flight by (kind, host) and picks
  the route with `rank_match` (`crates/frontline/src/matcher.rs:74-112`), which
  orders like `RESOLVE_ROUTE_SQL`
  (`crates/control-plane/src/postgres/route_ops.rs:124-150`). The read path
  uses a synchronous lock or snapshot swap and counts hits with an atomic
  (`crates/frontline/src/route.rs:57,313-337`).
- 3D: The stream reader forwards every message in order into one bounded
  channel; the actor owns reconnect and tags state with one session number.
  Delete `RouteObservations`, the per-stream markers, and the resolver path
  only tests use (`crates/frontline/src/resolver.rs:105-354`,
  `route.rs:1140-1319`, `runtime.rs:122-174`, `subscription.rs:166-352`,
  `matcher.rs:15-72`); port their tests to the shared coordinator.

Out of scope:
- Header, ALPN or protocol-specific route predicates.

Completion gate:
10,000 distinct paths on one host cost one lookup and one subscription. The
authority-equivalence tests pass in every warm-up order: wildcard before exact,
root before longer path, binding inserted after warm-up, and the SNI
equivalents. `route_lookup` shows a single-task hit at or below its recorded
baseline (157 ns in this review) and throughput that rises with cores (today
eight tasks on separate identities cost 2.0 µs per hit).

Testing plan:
- Property test: for random binding sets and requests, the frontline's local
  match equals `resolve_route` on real Postgres.
- Invalidation tests for each dependency: binding added, removed or
  reassigned for the host or a covering wildcard; instance state change.
- Criterion `route_lookup` at 1, 8 and 32 tasks before and after.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Decision | 3A: Cache authority by host | Needs: owner decision. Recommended: accept. |
| Incomplete | Work | 3B: Host rule-set resolution, dependencies and push | Missing: proto, store query, per-host dependency index, concurrent lookups. |
| Incomplete | Work | 3C: Host-keyed frontline cache with local matching | Missing: re-keyed cache and flights, synchronous read path, atomic hit counter. |
| Incomplete | Work | 3D: One ordered event channel; test-only resolver removed | Missing: actor-owned reconnect, single session number, deleted modules, ported tests. |
| Incomplete | Gate | Path cardinality costs one lookup per host | Missing: 10,000-path test and authority-equivalence suite. |
| Incomplete | Test | Local match equals `resolve_route` | Missing: property test against real Postgres. |
| Incomplete | Test | `route_lookup` scaling | Missing: before/after Criterion results at 1, 8 and 32 tasks. |
| Incomplete | Doc | North-star cache section | Missing: rewrite for host rule sets. |

## Phase 4: Event-Driven, Unthrottled Wake and Sleep

Goal:
A cold request waits for the pod plus at most a few hundred milliseconds, at
any burst size, and a request during drain meets a running pod.

Scope:
- 4A: A wake-latency gate in kind measures cold request to first byte minus
  pod-ready time, and records the baseline before other items land.
- 4B: Wake acceptance, sleep finalization and job completion signal the
  reconciler in-process; the tick remains as the cross-replica fallback
  (`crates/control-plane/src/reconciler.rs:123-165`;
  `crates/control-plane/src/wake.rs:243-250`).
- 4C: Readiness comes from an EndpointSlice watch, and the workload is created
  without waiting for PVC Bound
  (`crates/control-plane/src/kube_materializer.rs:222-292,487`;
  `crates/control-plane/src/materializer.rs:302-306`).
- 4D: Wakes and cleanup have separate capacity and separate claim queries
  (`claim_next` with `FOR UPDATE SKIP LOCKED` per state). A wake frees its
  slot while it waits for readiness, or the wake limit rises to hundreds.
  Limits are configurable (`reconciler.rs:77-87,152-154`;
  `crates/control-plane/src/postgres/materialization_ops.rs:261-299`;
  `runtime.rs:529`).
- 4E: The route-change dispatcher wakes on `LISTEN`/`NOTIFY` with the 250 ms
  poll as fallback, replacing the in-process notifies that publish each local
  write twice (`runtime.rs:678-720`; `reconciler.rs:557-561`).
- 4F: A frontline flight covers the resolve only. Waiters park per (instance,
  generation) under the existing 256-waiter budget, wake on the pushed change
  with a 1–2 s fallback timer, and cancel when the last waiter leaves
  (`crates/frontline/src/route.rs:352-354,411-438`).
- 4G: Cleanup waits for absence inside the attempt and treats a terminating
  pod as progress; the renderer sets `terminationGracePeriodSeconds`
  (`crates/control-plane/src/projection.rs:579-635`;
  `crates/control-plane/src/postgres/runtime_work.rs:155`).
- 4H (decision): A wake during Draining with the Deleting row unclaimed
  returns the materialization to Ready and the instance to Running in one
  transaction; the sidecar re-arms its idle detector
  (`crates/control-plane/src/postgres/lifecycle_ops.rs:132-157`).
- 4I: Frontline connects to the ready Pod address first and falls back to the
  Service ClusterIP. The sidecar verifies the instance ID that frontline sends
  and answers 421 before the app sees a request meant for another instance,
  because Pod addresses are reused outside the control plane's control. TCP
  and SNI passthrough routes carry no identity and keep the ClusterIP. With
  readiness watched (4C), the first connect lands before kube-proxy has
  programmed the Service: in kind, Running-to-first-byte rose from 41 ms to
  1,099 ms mean while endpoint-to-Running fell from 1,583 ms to 18 ms.

Out of scope:
- Pod start time itself (image pulls, scheduling).

Completion gate:
With an idle reconciler, control-plane-added wake latency is at most 200 ms
mean and 500 ms worst for a stateless class. With the fake Kubernetes client,
100 simultaneous cold instances reach Running within pod start plus 5 s; three
never-ready instances or 50 simultaneous sleeps leave a healthy wake inside
the same bound. A normal sleep records no failure. If 4H is accepted, a request
during drain grace is served by the existing pod.

Testing plan:
- 4A gate in the stateless and stateful kind scripts, reported per phase of
  the wake.
- Reconciler tests through `run_until_shutdown` for burst, never-ready and
  sleep-wave scheduling.
- Frontline probe: 64 paths on a waking host leave an unrelated miss served;
  flights end when clients disconnect.
- Lifecycle-races kind gate for 4H: wake at each point of drain.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| In Progress | Test | 4A: Wake-latency gate and baseline | Lifecycle stream measured the unmodified code in kind (12 samples): stateless adds 2,355 ms mean and 2,943 ms worst; stateful 4,780 ms and 9,896 ms. Missing: merged measurement code and the recorded per-phase baseline. |
| Incomplete | Work | 4B: Reconciler signalled on new work | Missing: in-process signal from acceptance, finalization and job completion. |
| Incomplete | Work | 4C: Readiness by watch; no PVC Bound wait | Missing: EndpointSlice watch and reordered apply. |
| Incomplete | Work | 4D: Separate wake and cleanup capacity | Missing: per-state `claim_next`, slot release during readiness wait, configuration. |
| Incomplete | Work | 4E: Dispatcher woken by `NOTIFY` | Missing: listener connection, fallback poll, removal of direct notifies. |
| Incomplete | Work | 4F: Parked waiters per instance | Missing: resolve-only flights, per-instance waiter set, cancellation on last waiter. |
| Incomplete | Work | 4G: Cleanup completes when pods stop | Missing: in-attempt absence wait, rendered termination grace, failure counter untouched by normal sleeps. |
| Incomplete | Decision | 4H: Revive during drain | Needs: owner decision. Recommended: accept. |
| Incomplete | Work | 4I: Pod address first, with sidecar identity check and ClusterIP fallback | Missing: Pod address on the route entry, identity header and 421 in the sidecar, fallback connect, and the 4A measurement after it. |
| Incomplete | Gate | Wake latency and burst bounds | Missing: 4A results at or under 200 ms mean and 500 ms worst; burst, never-ready and sleep-wave tests. |
| Incomplete | Test | Lifecycle-races and stateful kind gates | Missing: passing runs after 4C, 4G and 4H. |

## Phase 5: Lifecycle Recovers Without an Operator

Goal:
After any transient Kubernetes or Postgres fault, or loss of a control-plane
process, every affected instance returns to Running or Cold on its own, and
the control plane keeps serving.

Scope:
- 5A: Cancellation lets a dispatched mutation run to its own 10 s timeout
  (`crates/control-plane/src/reconciler/fenced_client.rs:131-147`). A renewal
  error retries until the lease's own expiry; only a definite refusal counts
  as loss (`reconciler.rs:380-404`). An effect known to be unsent keeps
  retrying its ACK (`fenced_client.rs:85-101`).
- 5B: A claim clears effects that carry UID and resourceVersion (every patch
  and delete) and the attempt re-inspects; the API server rejects the late
  request once the object changes
  (`crates/control-plane/src/kube_materializer.rs:137-150,198-204,792-800`).
- 5C (decision): Late creates. Preferred: create workloads with zero replicas
  and activate with a conditional patch, then delete the effect journal
  (`migrations/0009_fenced_projection_effects.sql`, begin and ACK, the
  `uncertain` kind, its proto message and gauges). Fallback: keep the journal
  and resolve an uncertain create by re-inspection after a settle window
  longer than the API server's request timeout.
- 5D: `ForceDeleteMaterialization` moves the instance in the same transaction:
  Waking to Failed, Running to Cold, Draining to Cold with its deferred wake
  activated (`materialization_ops.rs:607-672`).
- 5E: Cleanup retries at the capped backoff until it succeeds; the oldest-age
  gauge drives the alert. Terminal classification stays for wakes and for
  ownership conflicts (`runtime_work.rs:143-168`).
- 5F: The control plane stays up through store unavailability: failed ticks
  back off (`reconciler.rs:142`), I/O errors on an open connection are
  retryable (`crates/control-plane/src/postgres/error.rs:60-64`), and the
  process reports unready (`runtime.rs:623-662,706-712`).
- 5G: Sessions set `idle_in_transaction_session_timeout` and TCP keepalives
  (`crates/control-plane/src/postgres/connection.rs:33-37`); the three
  reconciliation completions lock the instance before the materialization
  (`materialization_ops.rs:486-560`).

Out of scope:
- Repairing drift of Ready projections.

Completion gate:
Under injected 5xx, connection reset and 10 s stall on each create, patch and
delete, process kill at each boundary around a mutation, and a 30 s Postgres
outage: every affected instance reaches Running or Cold with no operator RPC,
no exclusivity key has two holders, and the control-plane process stays up.
`docs/projection-safety.md` describes automatic recovery.

Testing plan:
- Extend `tests/postgres_store/crash_boundaries.rs` and the reconciler tests
  with a fault-injecting Kubernetes client at every mutation.
- Spike for 5C: zero-replica create and conditional activation for Deployment,
  StatefulSet and raw auxiliary templates; a late create after cleanup leaves
  no pod.
- Force-delete tests from Waking, Running and Draining that end with a
  successful wake.
- Restart and failures kind gates with the API server and Postgres
  interrupted mid-wake and mid-sleep.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: Definite outcomes on cancellation and renewal errors | Missing: mutation runs to timeout, renewal retry until expiry, queued ACK for unsent effects. |
| Incomplete | Work | 5B: Conditional effects clear at claim | Missing: claim change and re-inspection tests for patch and delete. |
| Incomplete | Decision | 5C: Late creates | Needs: spike result and owner decision between inert creates and a settle window. |
| Incomplete | Work | 5D: Force delete leaves a wakeable instance | Missing: instance transition in the force-delete transaction. |
| Incomplete | Work | 5E: Cleanup retries until it succeeds | Missing: non-terminal classification for Deleting rows; alert on oldest age. |
| Incomplete | Work | 5F: Control plane survives store unavailability | Missing: tick tolerance, I/O error classification, readiness reporting. |
| Incomplete | Work | 5G: Session timeouts and one lock order | Missing: connection options and instance-first locking in completions. |
| Incomplete | Gate | Automatic recovery under injected faults | Missing: fault-injection suite passing with no operator RPC. |
| Incomplete | Test | Restart and failures kind gates under interruption | Missing: passing runs with API server and Postgres interrupted. |
| Incomplete | Doc | `docs/projection-safety.md` and runbook | Missing: rewrite for automatic recovery. |

## Phase 6: The API Rejects What Cannot Run

Goal:
Every class and instance the API accepts can wake, every API field does
something, and the operator surface is safe by default.

Scope:
- 6A: `CreateInstance` dry-runs `render_manifests`.
  `CreateWorkloadClassVersion` checks that every referenced value is declared
  and renders once with placeholder values. Add the Kubernetes limits
  (StatefulSet names at most 52 characters, letter-leading Service names), a
  `/` boundary after hostPath literals, the label-value rule for class IDs,
  and strict `RouteHost` validation
  (`crates/control-plane/src/workload.rs:125-143`; `instance.rs:193-203`;
  `manifest/render.rs:1135-1160`; `crates/sleepypods-api/src/route.rs:311-338`).
- 6B: Instance-scoped names carry 16 hex characters of hash
  (`crates/control-plane/src/kubernetes_name.rs:5`).
- 6C: Delete inert API: `default_values`, the class `idempotency_key`,
  `template_generation`, `ProtocolRoute`, `ExpireHttp01Challenges` and
  `OperatorApiPlaceholder`. Expose `routes` on `CreateInstance`, and key the
  materialization admin RPCs by `instance_id`.
- 6D: gRPC-Web serves with the configured TLS identity, and allowed origins
  come from configuration with an empty default
  (`runtime.rs:405-445,487-499`; `api/server.rs:201-218`).
- 6E: Startup compares sealing key IDs in use against the key ring and fails
  on a gap; add bulk re-encryption by sealing key
  (`crates/control-plane/src/certificate/seal.rs:126-131`;
  `runtime_security.rs:80-125`).
- 6F: The control plane installs the filtered stderr sink and counts accepted
  auth decisions (`runtime.rs:740-752`; `auth.rs:199-254`).

Out of scope:
- Tenant or organization permission models.

Completion gate:
Each invalid input in the review's table (missing service template, class ID
with a slash, value that yields an invalid name, undeclared value, hostPath
escape, over-long StatefulSet name) is rejected by its create RPC. An operator
call over gRPC-Web without TLS fails when TLS is configured. One RPC writes no
stderr line on success.

Testing plan:
- API tests per invalid input; a property test that any accepted instance
  renders.
- Collision test over ten million sequential instance IDs.
- gRPC-Web kind gate over TLS with an allowed and a refused origin.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A: Render at create time | Commit `3c09124`: `CreateInstance` dry-runs the render and class creation renders with placeholders; twelve rejection tests in `crates/control-plane/tests/api_transport/create_validation.rs`, plus manifest tests for the hostPath boundary, raw-object suffix and name limits. |
| Complete | Work | 6B: 16-hex name suffix | Commit `9a1e36f`: `ten_million_sequential_instance_ids_render_distinct_names` (ignored by default, zero collisions in 39 s) and `colliding_32_bit_prefixes_render_distinct_names`. |
| Complete | Work | 6C: Inert API removed | Commits `87170fe`, `78aa212`: `default_values`, class `idempotency_key`, `template_generation`, `ExpireHttp01Challenges` and the placeholder service are gone; `create_instance_creates_its_routes_with_the_instance`; admin RPCs take `instance_id`. `ProtocolRoute` goes with Phase 3. |
| Complete | Work | 6D: gRPC-Web TLS and configured origins | Commits `4cce567`, `8760681`: `grpc_web_listener_refuses_plaintext_when_tls_is_configured`, `grpc_web_listener_serves_allowed_origins_and_refuses_others`. |
| Complete | Work | 6E: Sealing key completeness | Commits `1321320`, `cb36fc4`: `postgres_startup_requires_every_sealing_key_in_use` and `postgres_sealing_key_rotation_reencrypts_certificates_in_bulk` (`ReencryptCertificates`). |
| Complete | Work | 6F: Quiet control-plane logging | Commit `833940b`: `successful_rpc_writes_no_stderr_line` and `accepted_decisions_are_counted_and_write_no_log_event`. |
| In Progress | Gate | Accepted means renderable | Property test `every_accepted_instance_renders_at_wake` (175 accepted instances, all render) and the rejection tests pass; workspace 962 passed, real-Postgres target 28 passed at `42222f3`. Missing: the gRPC-Web kind gate over TLS. |
| Complete | Doc | Operator guide | Commit `7e0701b`. |

## Phase 7: Store Cost and Shape

Goal:
Each lifecycle operation costs a handful of round trips, writers stop
serializing across the network, and the store exposes only what production
calls.

Scope:
- 7A: Route every statement through `prepare_cached`, cache immutable
  workload class versions in process, and drop the idle pre-check that
  `begin_sleep` repeats (`crates/control-plane/src/postgres/`, 115 call sites;
  `crates/control-plane/src/idle.rs:194,235-246,268`).
- 7B: Measure lifecycle transitions per second, then narrow the revision lock:
  allocate revisions at commit, or replace the singleton with a transaction-ID
  horizon so writers stop serializing
  (`crates/control-plane/migrations/0010_runtime_work.sql:39,72-88`).
- 7C: If the effect journal survives 5C, store the effect as columns on
  `materializations` so claim, begin, ACK and release are single statements.
- 7D (decision): Carry projection, lease, schedule and one incarnation stamp on
  the instance row, replacing the paired state machines, the generation
  arithmetic and `deferred_wake_intents`.
- 7E: Delete the seven store methods with only test callers
  (`crates/control-plane/src/store.rs:133,163,173,178,198,208,272`), split the
  trait by consumer, move retry into the Postgres operations, and collapse the
  eleven migrations into one baseline without unused columns and states.

Out of scope:
- A second store provider.

Completion gate:
Wake acceptance, an idle report and the fencing around one Kubernetes mutation
each take at most half their current round trips (16, 20 and 14). Measured
transitions per second are recorded before and after 7B. Every store method
has a production caller, and each test fake implements one role.

Testing plan:
- A round-trip counter in the Postgres test harness asserts the budgets.
- Real-Postgres conformance after each item, with setup through `accept_wake`,
  claim and complete.
- A concurrent-writer benchmark through a latency relay for 7B.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A: Prepared statements and class cache | Missing: `prepare_cached` helper, class cache, removed duplicate pre-check. |
| Incomplete | Work | 7B: Narrow revision lock | Missing: baseline measurement, chosen allocation scheme, ordering tests. |
| Incomplete | Work | 7C: Effect barrier on the materialization row | Blocked by: 5C outcome. |
| Incomplete | Decision | 7D: One lifecycle row | Needs: owner decision after Phases 4 and 5. |
| Incomplete | Work | 7E: Store surface and baseline migration | Missing: deleted methods, role traits, in-operation retry, single migration. |
| Incomplete | Gate | Round-trip budgets and caller coverage | Missing: counter assertions and a grep-clean trait. |
| Incomplete | Test | Real-Postgres conformance | Missing: complete `postgres_store` run with the execution checker. |

## Phase 8: Smaller Surface

Goal:
Production modules contain what production runs, and each remaining mirror or
duplicate layer is either removed or kept by decision.

Scope:
- 8A: Delete the reconciler's second scheduler (`run_once`,
  `reconcile_materialization`) and the materializer paths only tests call
  (`crates/control-plane/src/reconciler.rs:192-245`;
  `materializer.rs:220-264,342-403`); port tests to `run_until_shutdown`.
- 8B: Delete proxy API only tests call (for example
  `IdleDetector::report_when_idle`, `serve_http_with_idle`,
  `proxy_websocket_streams`). Measure per-request bookkeeping, then spawn the
  delivery watchdog only for HTTP/2 bodies and share each runtime behind one
  `Arc` (`crates/proxy-core/src/delivery.rs:71-103`; `admission.rs:110-144`).
- 8C (decision): Relay upgraded WebSocket bytes with
  `proxy_streams_with_timeouts` and drop `tokio-tungstenite` from production.
- 8D (decision): Render into `k8s-openapi` types behind one wrapper, and drop
  Deployment and StatefulSet from the raw allow-list
  (`crates/control-plane/src/manifest/objects.rs`, `serialize.rs`).
- 8E (decision): Replace idempotency keys with insert-or-compare
  (`crates/control-plane/src/postgres/idempotency.rs`).
- 8F: Move `docs/review-evidence` (1,016 files, 21 MB) out of the tree, fold
  `sleepypods-types` into `sleepypods-api`, and remove the single-valued
  `SLEEPYPODS_STORE_PROVIDER` setting.

Out of scope:
- Replacing the observability crate.

Completion gate:
No production module exports an item whose only callers are tests. Each
decision is implemented or recorded as declined with its reason. The hot-path
benches and load smokes show no regression.

Testing plan:
- The nine CI steps after each deletion.
- Protocol kind gate and an authenticated, subprotocol and
  permessage-deflate WebSocket test for 8C.
- Criterion `proxy_primitives` and the load smokes before and after 8B.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 8A: One reconciler scheduler | Missing: deleted entry points and ported tests. |
| Incomplete | Work | 8B: Proxy test-only API and per-request bookkeeping | Missing: deletions, measurement, watchdog and `Arc` changes. |
| Incomplete | Decision | 8C: Tunnel WebSockets | Needs: owner decision on the protocol-contract change. Recommended: accept. |
| Incomplete | Decision | 8D: `k8s-openapi` manifest types | Needs: owner decision; coordinate the rendered-hash change with Phase 5. |
| Incomplete | Decision | 8E: Drop idempotency keys | Needs: owner decision and replay edge-case review. |
| Incomplete | Work | 8F: Evidence out of tree; crate and setting cleanup | Missing: moved evidence, folded crate, removed setting, README links. |
| Incomplete | Gate | Production modules hold production code | Missing: caller audit and benches without regression. |
