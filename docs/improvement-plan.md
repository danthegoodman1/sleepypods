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

### Decisions

The owner accepted every recommendation on 2026-11-27.

- **3A, cache authority by host: accepted.** Cache the complete rule set for an
  exact host and match paths locally. A host's answer is complete, so a broad
  rule still cannot hide an exact-host or longer-path rule.
- **4H, revive during drain: accepted.** A wake that arrives before teardown
  starts returns the instance to Running on the existing pod.
- **5C, late creates: inert creates.** The spike passed: a zero-replica
  Deployment, StatefulSet or raw workload starts no Pod, and a conditional
  activation patch with a stale resourceVersion or UID is refused. Workloads
  are created with zero replicas and activated by a conditional patch, and the
  effect journal is deleted.
- **6C, `default_values`: deleted.** Per-field defaults in the value schema
  remain.
- **7D, one lifecycle row: open.** Decide after Phases 4 and 5 land.
- **8C, tunnel WebSockets: accepted.** Upgraded bytes are relayed; the protocol
  contract's per-hop challenge and extension clauses changed with it.
- **8D, render into `k8s-openapi` types: accepted,** with Deployment and
  StatefulSet dropped from the raw allow-list.
- **8E, drop idempotency keys: accepted,** in favour of insert-or-compare on
  the caller-chosen ID.

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
| Complete | Work | 1A: Bound host and path in frontline | Commit `41ce06a`: `an_oversized_path_or_host_is_refused_before_any_lookup`; route bindings are bounded the same way at creation (`c36d6ed`, `route_creation_bounds_the_path_prefix_by_the_longest_routed_path`). |
| Complete | Work | 1B: Per-request Subscribe errors and terminal flights | Commits `41ce06a`, `cf005ea`: `a_failed_store_lookup_ends_that_subscription_and_the_stream_stays_up` and its invalid-request, timeout and limit siblings; `a_flight_ends_when_its_last_waiter_leaves`; `a_lookup_with_no_answer_fails_at_its_deadline`. |
| Complete | Work | 1C: Agreeing, validated defaults and eager reconnect | Commit `41ce06a`: 60 s lifetime and 90 s TTL validated at startup (`route_cache_ttl_outlasts_a_stream_rotation`); frontline capacity 1,024 against a 2,048 per-stream cap, with a smaller learned cap met by releasing the oldest hosts; `an_orderly_stream_end_keeps_hosts_serving_and_registers_them_again`. |
| Complete | Work | 1D: Lossy ends clear authority; cursor before serving | Commits `5d02303`, `41ce06a`: `lost_route_changes_end_the_stream_with_data_loss`, `a_failed_stream_clears_the_cache_before_the_next_request_is_served`, `dispatcher_read_failure_keeps_subscribers_and_delivers_after_recovery`, `dispatcher_publishes_only_changes_committed_after_its_cursor`. |
| Complete | Work | 1E: Shared resilient accept helper | Commit `9603f06`: `proxy_core::serve_listener` serves all six loops; `accept_errors_leave_the_listener_serving`, `shutdown_closes_the_listener_while_connections_drain`, and the sidecar and frontline equivalents inject `EMFILE` and `ECONNABORTED`. Refused sockets close with FIN (`8376cdc`): `refused_sockets_with_unread_input_close_with_fin` and `tls_flood_through_port_forward_closes_every_connection_with_fin`; an RST on a refused socket ended the TLS gate's `kubectl port-forward`. |
| Complete | Work | 1F: Outbound liveness | Commit `9603f06`: HTTP/2 and TCP keepalive on the control-plane channel and the frontline-to-sidecar hop; a header timeout poisons its pooled connection; `a_silent_control_plane_connection_fails_the_report_at_the_keepalive_deadline`, `a_report_that_never_answers_ends_at_its_deadline_and_retries`. The sidecar sends no HTTP/2 pings to the app, because gRPC servers close connections that ping too often. |
| Complete | Work | 1G: Count-bounded certificate cache | Commits `5da8ab7`, `9a4da04`, `e73bd7f`, `1e287de`: 1,024 certificates and 1,024 negative entries in separate budgets; lookups queue for a fetch slot, at most 64 handshakes waiting; worst-case memory about 220 MiB, stated in the operator guide. |
| Complete | Work | 1H: WebSocket message cap | Superseded by 8C (`9603f06`): the relay tunnels bytes and holds no message; `a_withheld_final_fragment_reaches_the_upstream_as_it_arrives`. |
| Complete | Gate | Unrelated cached routes keep serving through each fault | `crates/control-plane/tests/proxy_api_transport/route_edge.rs` joins the frontline coordinator to the real Subscribe service: `a_5000_byte_path_is_refused_and_256_hot_hosts_stay_cached`, `a_failed_store_lookup_fails_its_own_host_and_256_hot_hosts_stay_cached`, `the_control_planes_subscription_limit_is_met_locally_and_hot_hosts_stay_cached`, `stream_rotations_keep_250_hot_hosts_served_from_the_cache`, `lost_route_changes_clear_the_cache_before_the_next_request_is_served`; the listener faults are covered by 1E. |
| Complete | Test | 1,024 certificates across a refresh cycle | `full_cache_stays_warm_through_a_refresh_cycle_with_three_fetches_in_flight` (the earlier code kept 407 of 1,024), `unknown_hostnames_leave_every_certificate_cached`, and `a_full_lookup_wait_serves_cached_hostnames_and_fails_further_lookups_at_once` in `crates/frontline/src/certificates/capacity_tests.rs`. |
| Complete | Test | Routing kind gate | `scripts/test-kind-e2e-routing.sh` passed at `4731fe9` (`routing_and_http01_through_deployed_platform`, 32 s). |

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
| Complete | Work | 2A: App protocol per workload port | Commits `8a2ff7d`, `9603f06`: `SidecarTemplate.app_protocol` renders `SLEEPYPODS_APP_PROTOCOL`; the sidecar's `AppHttpProxy` speaks it; gRPC to an `http1` port answers `grpc-status: 12`. |
| Complete | Work | 2B: Authority and `te: trailers` across hops | Commit `9603f06`: `h2c_app_receives_te_trailers_and_the_client_authority_from_either_client_version`, `forwarded_host_comes_from_the_request_target_when_host_is_absent`. |
| Complete | Work | 2C: Separate keep-alive idle timeout | Commit `9603f06`: `SLEEPYPODS_PROXY_KEEPALIVE_IDLE_TIMEOUT_MS` (90 s); `http1_keepalive_connection_outlives_the_setup_timeout_and_ends_at_its_idle_timeout`, `upstream_pool_idle_must_stay_below_the_keepalive_idle_timeout`. |
| Complete | Work | 2D: Resolved backend address | Commit `1a2a6bd`: the backend is the Service ClusterIP; `pod_backend_uri` records the ready endpoint's Pod address for 4I. |
| Complete | Gate | Every client protocol reaches both app kinds | In process: `http1_http2_and_websocket_clients_reach_both_app_protocols_with_the_client_host`. Protocols kind gate passed at `b3f6848` and `0580dc5` with `http1` and `h2c` classes, and the TLS kind gate (termination, SNI passthrough and the dynamic certificate lifecycle with an `h2` client) passed at `894c4f7`. |
| Complete | Test | HTTP/2 client to HTTP/1.1 upstream | `http2_request_reaches_an_http1_only_app_with_the_client_host` in `crates/proxy-core/tests/http_protocols.rs`. |
| Complete | Test | C-core gRPC through both proxies | Commit `b288172`: the protocols kind gate deploys a grpcio server on an `h2c` class; unary and server-streaming calls return status 0 through frontline and the sidecar (C-core rejects a call without `te: trailers`), and gRPC to an `http1` class gets `grpc-status: 12`. Gate passed at `b3f6848`. |

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
| Complete | Decision | 3A: Cache authority by host | Accepted by the owner on 2026-11-27. |
| Complete | Work | 3B: Host rule-set resolution, dependencies and push | Commits `41ce06a`, `c36d6ed`: `proxy_subscribe_returns_the_complete_rule_set_for_the_host_most_specific_first`, `a_binding_change_for_the_host_or_one_of_its_suffixes_pushes_the_new_rule_set`, `an_instance_change_pushes_…`, `lookups_on_one_stream_run_sixteen_at_a_time`; creation keeps each name within 256 rules and 1 MiB (`tests/postgres_store/host_rule_bounds.rs`). |
| Complete | Work | 3C: Host-keyed frontline cache with local matching | Commits `41ce06a`, `d02bd2c`: cache, flights and subscriptions keyed by (kind, host); `HostRules` in `sleepypods-api` ranks for both the store and frontline; the read path is an `arc-swap` snapshot; the per-request lookup event is gone. |
| Complete | Work | 3D: One ordered event channel; test-only resolver removed | Commit `41ce06a`: `matcher.rs`, `resolver.rs`, `RouteObservations` and the test-only coordinator path are deleted; the actor owns one stream, and proxy-chosen subscription IDs are never reused, so no session number is needed. 43 coordinator tests in `crates/frontline/src/route/tests.rs`. |
| Complete | Gate | Path cardinality costs one lookup per host | `ten_thousand_paths_on_one_host_cost_one_lookup_and_one_subscription`; `a_request_gets_the_same_route_in_every_warm_up_order` (all 720 orders of six HTTP and SNI requests); `a_binding_committed_after_warm_up_takes_the_requests_it_ranks_first_for`. |
| Complete | Test | Local match equals `resolve_route` | `local_matching_equals_postgres_for_random_binding_sets` (40 sets × 24 requests among 100k routes) and `local_matching_equals_the_control_planes_resolution_for_random_binding_sets` (150 sets × 24 requests). |
| Complete | Test | `route_lookup` scaling | Release, 24 cores, one hit per task: 1 task 118 ns → 68 ns; 8 tasks 2.13 µs → 87 ns; 32 tasks 7.71 µs → 182 ns. |
| Complete | Doc | North-star cache section | Commit `de619d4`. |

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
| Complete | Test | 4A: Wake-latency gate and baseline | Commits `e7b4fda`, `2d557ad`: per-phase measurement in the stateless and stateful kind gates (`crates/control-plane/tests/support/wake_latency.rs`). Baseline added latency, mean / worst: stateless 2,355 / 2,943 ms, stateful 4,780 / 9,896 ms. |
| Complete | Work | 4B: Reconciler signalled on new work | Commit `1a2a6bd`: acceptance, finalization, failure recording, deletion, enqueue and finished jobs signal the reconciler; endpoint-to-Running fell from 1,583 ms to 22 ms mean. |
| Complete | Work | 4C: Readiness by watch; no PVC Bound wait | Commit `1a2a6bd`: an EndpointSlice watch reports readiness; the workload is created without waiting for its claim to bind. |
| Complete | Work | 4D: Separate wake and cleanup capacity | Commit `1a2a6bd`: per-state claims with `FOR UPDATE SKIP LOCKED` under `SLEEPYPODS_RECONCILER_WAKE_LIMIT` (256) and `SLEEPYPODS_RECONCILER_CLEANUP_LIMIT` (64); `a_hundred_cold_instances_reach_running_within_pod_start_plus_the_bound`, `cleanup_beyond_its_limit_waits_without_delaying_wakes`. |
| Complete | Work | 4E: Dispatcher woken by `NOTIFY` | Commit `5d02303`: `dispatcher_reads_on_the_store_signal_without_waiting_for_its_poll`; the direct in-process notifies are deleted. |
| Complete | Work | 4F: Parked waiters per instance | Commit `41ce06a`: `sixty_four_hosts_on_waking_instances_leave_an_unrelated_miss_served_and_stop_with_their_clients` (miss answered in zero virtual time; no lookups or wakes for 30 s after clients leave). |
| Complete | Work | 4G: Cleanup completes when pods stop | Commits `1a2a6bd`, `8a2ff7d`: cleanup waits for absence inside the attempt; the renderer sets `terminationGracePeriodSeconds`; `a_normal_sleep_waits_for_the_pod_to_stop_and_records_no_failure`. |
| Complete | Decision | 4H: Revive during drain | Accepted and implemented in `1a2a6bd` and `9603f06`: `postgres_wake_during_drain_grace_revives_the_projection`; the lifecycle-races kind gate's revive case passes. |
| Complete | Work | 4I: Pod address first, with sidecar identity check and ClusterIP fallback | Commits `74814b0`, `6eb4940`, `102f0ca`: route entries carry the ready Pod address; frontline opens a Pod connection with an instance claim the sidecar must echo before any request byte is written, and falls back to the Service within 250 ms; a refused claim closes with FIN before the app sees a request. `a_ready_pod_address_serves_before_its_service_is_routable`, `a_sidecar_of_another_instance_at_the_pod_address_never_reaches_its_app`, `instances_that_share_a_pod_address_never_share_a_connection`, `tls_passthrough_keeps_the_service_address`. |
| Complete | Gate | Wake latency and burst bounds | Burst, never-ready and sleep-wave tests pass (`crates/control-plane/src/reconciler/tests.rs`). Added latency after 4I, 10 samples, mean / worst: stateless 122 / 320 ms (baseline 2,355 / 2,943), stateful 148 / 348 ms (baseline 4,780 / 9,896); stateless, stateful, routing and protocols kind gates pass at `ff66a8b`. |
| Complete | Test | Lifecycle-races and stateful kind gates | Stateless, stateful, failures, restart and lifecycle-races kind gates pass at `926a696` (after merging the route stream). |

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
| Complete | Work | 5A: Definite outcomes on cancellation and renewal errors | Commit `1a2a6bd`: a dispatched mutation runs to its own timeout; a renewal error retries until the lease's own expiry (`HeldLease::renew`). |
| Complete | Work | 5B: Conditional effects clear at claim | Commit `1a2a6bd`: every patch and delete carries UID and resourceVersion, and no effect record remains to clear; `a_wake_recovers_from_a_fault_on_any_mutation`, `a_sleep_recovers_from_a_fault_on_any_delete`. |
| Complete | Decision | 5C: Late creates | Inert creates: workloads are created with zero replicas and activated by a UID-conditioned patch; migration 0013 drops the effect journal (`1a2a6bd`). |
| Complete | Work | 5D: Force delete leaves a wakeable instance | Commit `1a2a6bd`: `postgres_force_delete_leaves_a_wakeable_instance` covers Waking, Running and Draining, each ending Running. |
| Complete | Work | 5E: Cleanup retries until it succeeds | Commit `1a2a6bd`: only an ownership conflict parks cleanup. |
| Complete | Work | 5F: Control plane survives store unavailability | Commits `1a2a6bd`, `cd7177f`: `postgres_reconciler_rides_out_a_thirty_second_outage` resets every session through a TCP relay; the `sleepypods_control_plane_store_available` gauge reports the outage. |
| Complete | Work | 5G: Session timeouts and one lock order | Commits `1a2a6bd`, `8b4dc14`: sessions set `idle_in_transaction_session_timeout` and keepalives; completions lock the instance first. |
| Complete | Gate | Automatic recovery under injected faults | `postgres_lifecycle_recovers_from_each_fault_around_each_mutation` (create, activate and delete × 5xx, reset, 10 s stall and process loss), `postgres_crash_at_each_lock_boundary_keeps_the_key_and_yields_to_recovery`, and the 30 s outage test; every case ends Running or Cold with no operator RPC. |
| In Progress | Test | Restart and failures kind gates under interruption | Both gates pass. API-server and Postgres interruption are covered by fault-injecting fakes and the real-Postgres relay test, not inside the kind gates. |
| Complete | Doc | `docs/projection-safety.md` and runbook | Commit `149bcfb`. |

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
| Complete | Gate | Accepted means renderable | Property test `every_accepted_instance_renders_at_wake` (175 accepted instances, all render) and the rejection tests pass; the gRPC-Web kind gate over TLS (plaintext refused, allowed and refused origins) passed at `093adbb`. |
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
| Complete | Work | 7C: Effect barrier on the materialization row | Superseded by 5C: the effect journal is deleted. |
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
| Complete | Work | 8A: One reconciler scheduler | Commit `1a2a6bd`: `run_once`, `reconcile_materialization` and the test-only materializer paths are deleted; tests drive `run_until_shutdown`. |
| Complete | Work | 8B: Proxy test-only API and per-request bookkeeping | Commits `9603f06`, `7d424a3`: test-only serve, relay and idle entry points deleted; the delivery watchdog runs for HTTP/2 bodies only; each runtime sits behind one `Arc`. Criterion `proxy_primitives`: admission acquire and release 286 ns to 29 ns, accounting 266 ns to 24 ns, header preparation unchanged. |
| Complete | Decision | 8C: Tunnel WebSockets | Accepted and implemented in `9603f06`, `c38541f`: `handshake_fields_and_upgraded_bytes_pass_through_unchanged` (Authorization, Cookie, subprotocol, permessage-deflate); `tokio-tungstenite` is a dev-dependency; WebSocket round trip 27.4 µs to 10.9 µs. |
| Complete | Decision | 8D: `k8s-openapi` manifest types | Accepted and implemented in `0d665aa`, `ca4649f`: one `KubernetesObject` wrapper over `k8s-openapi` types replaces `objects.rs` and `serialize.rs`; golden fixtures blessed against the old renderer are unchanged, so rendered hashes of structured classes are unchanged; raw Deployments and StatefulSets are rejected (`rejects_raw_workload_controllers`). `GetWorkloadClassVersion` keeps its mapping, because the template is a domain type of instance-value text parts. |
| Complete | Decision | 8E: Drop idempotency keys | Accepted and implemented in `0f04eb0`: creates insert under the caller-chosen ID and compare with the stored resource; `postgres_create_after_delete_starts_a_fenced_incarnation`, `postgres_repeat_racing_the_first_create_returns_its_instance`, `postgres_operator_api_answers_repeated_creates`. |
| Incomplete | Work | 8F: Evidence out of tree; crate and setting cleanup | Missing: moved evidence, folded crate, removed setting, README links. |
| Incomplete | Gate | Production modules hold production code | Missing: caller audit and benches without regression. |
