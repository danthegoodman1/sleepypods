# SleepyPods Contributor And Agent Guide

This repo favors the least code that satisfies the current milestone and its
tests. Keep changes small, avoid mega files, and update the development-plan
status when a documented gap actually closes.

## Repo Map

- `crates/control-plane`: API service implementations, durable store trait, Postgres provider,
  route resolver, wake/sleep/delete state machines, manifest rendering,
  Kubernetes materializer, native gRPC, and gRPC-Web operator transport.
- `crates/frontline`: always-on proxy runtime, host-keyed route cache, the route
  coordinator that owns the Subscribe stream and wake coordination, HTTP-01
  interception, HTTP/TLS/SNI listeners, and frontline load-smoke helper.
- `crates/sidecar`: workload-local HTTP/TCP proxy, drain/idle detection,
  `ReportIdle` client, and sidecar load-smoke helper.
- `crates/proxy-core`: shared proxy primitives, TLS ClientHello/SNI parsing,
  accounting, drain, shutdown, timeout, proxy-specific metrics adapters, and hot-path benches.
- `crates/sleepypods-api`: generated protobuf clients/services, resource IDs and
  generations with the shared DNS-label check, routing and HTTP-01/certificate
  contracts, state/backend/target values, and native transport and client
  bearer authentication.
- `crates/sleepypods-certificate`: maintained-library certificate/key/chain and
  hostname validation shared by publication and Frontline installation, without
  persistence or sealing dependencies.
- `crates/sleepypods-observability`: metric and tracing vocabulary, the single
  process-wide recorder, sinks, and Prometheus exporter.
- `scripts`: production-image smokes, kind E2E gates, soak gates, load-budget
  helpers, and Criterion regression checker.
- `docs`: north-star, development plan, operator docs, runbook, contributor
  guide, and hot-path budgets.

## Source Of Truth

- API/proto: `crates/sleepypods-api/proto/sleepypods/controlplane/v1/control_plane.proto`
- Code generation: `crates/sleepypods-api/build.rs`
- Store contract: `crates/control-plane/src/store.rs`
- Postgres provider: `crates/control-plane/src/postgres/`
- Manifest rendering: `crates/control-plane/src/manifest/`; rendered objects
  are `k8s-openapi` types behind `KubernetesObject` in `manifest/object.rs`;
  instance-scoped object names: `crates/control-plane/src/kubernetes_name.rs`
- Create-time validation: `WorkloadClassVersion::validate_renderable` in
  `workload.rs` and `CreateInstanceRequest::validate_renders` in `instance.rs`,
  both called from `api/server.rs`
- Kubernetes materialization: `crates/control-plane/src/kube_materializer.rs`
  and `crates/control-plane/src/materializer.rs`
- Resource/lifecycle semantics: `crates/control-plane/src/workload.rs`,
  `instance.rs`, `wake.rs`, `idle.rs`,
  `sleep_policy.rs`, and `materialization.rs`; shared route and HTTP-01 contracts
  are in `crates/sleepypods-api/src/{route,http01}.rs`.
- Operator/proxy/sidecar services:
  `crates/control-plane/src/api/server.rs`, `api/proxy.rs`, `api/sidecar.rs`;
  the store-error-to-status mapping they share is `store_error_to_status` in
  `api/mod.rs`
- gRPC-Web origins and CORS: `crates/control-plane/src/api/grpc_web.rs`
- Caller authentication and its decision counter:
  `crates/control-plane/src/auth.rs`
- Route rule sets and their ranking: `HostRules` in
  `crates/sleepypods-api/src/route.rs`; the control plane's Subscribe stream:
  `crates/control-plane/src/api/route_subscriptions.rs`
- Frontline route cache and coordinator:
  `crates/frontline/src/cache.rs`, `route.rs`, `subscription.rs`, with test
  fakes in `route/testing.rs`; the two composed in process:
  `crates/control-plane/tests/proxy_api_transport/route_edge.rs`
- Certificate delivery: `crates/frontline/src/certificates/`; shared validation:
  `crates/sleepypods-certificate/src/lib.rs`; encrypted persistence and native
  management/resolution APIs: `crates/control-plane/src/certificate/`,
  `postgres/certificate_ops.rs` and `api/certificates.rs`.
- Sidecar idle: `crates/sidecar/src/idle.rs` and
  `crates/sidecar/src/idle/control_plane.rs`
- Observability: `crates/sleepypods-observability/src/metrics.rs`,
  `recorder.rs`, and `lib.rs`; proxy adapters stay in `crates/proxy-core/src/observability/`.
- Hot-path/load gates: `docs/proxy-hot-path-budgets.md`,
  `crates/proxy-core/benches/proxy_primitives.rs`,
  `crates/frontline/benches/route_lookup.rs`, and `scripts/smoke-*-load.sh`
- kind E2E: `scripts/test-kind-e2e-*.sh` and
  `crates/control-plane/tests/kind_e2e_*.rs`

## Generated Files

`crates/sleepypods-api/build.rs` uses vendored `protoc` through
`tonic_prost_build` to compile the protobuf at build time. Generated Rust lives
under Cargo build output, not checked into the repo.

When changing protobuf messages or services:

1. Edit the `.proto` file first.
2. Update API mapping/server code and tests in `crates/control-plane`.
3. Run the relevant `cargo test` command; code generation happens during the
   build.
4. Do not hand-edit generated output under `target/`.

## Required Commands

Use the smallest gate that covers the change, then widen when shared behavior
or cross-component contracts move.

| Change | Commands |
| --- | --- |
| Any Rust change | `cargo fmt --check`, `cargo test` |
| Whitespace/docs | `git diff --check` |
| Proxy-core primitives | `cargo test -p proxy-core` and, for hot paths, `cargo bench -p proxy-core --bench proxy_primitives -- --sample-size 10 --measurement-time 1 --warm-up-time 1` |
| Frontline route/cache/listener | `cargo test -p frontline`; route lookup changes also run `cargo bench -p frontline --bench route_lookup -- --sample-size 10 --measurement-time 1 --warm-up-time 1` |
| Control-plane API/store/lifecycle | `cargo test -p control-plane`; Postgres-backed store work also run `./scripts/test-postgres-store.sh` |
| PostgreSQL execution gate | `python3 scripts/test-postgres-execution.py`; CI and `test-postgres-store.sh` use the same complete-target skip/count checker |
| Store lifecycle throughput | `SLEEPYPODS_POSTGRES_URL=… SLEEPYPODS_STORE_THROUGHPUT_SECONDS=10 cargo test --release --locked -p control-plane --test store_throughput -- --nocapture` measures transitions per second through a latency relay; without both variables it does nothing |
| Sidecar runtime/idle | `cargo test -p sidecar` |
| API/observability crate boundaries | `./scripts/test-dependency-boundaries.py`, `cargo test -p sleepypods-api -p sleepypods-observability -p proxy-core -p frontline -p sidecar` |
| Production images | `./scripts/smoke-images.sh` |
| Load-budget logic | `./scripts/test-load-budget.sh`, `./scripts/test-criterion-regressions.py`, and the relevant `./scripts/smoke-*-load.sh` |
| Benchmark regression review | `./scripts/check-criterion-regressions.py` after collecting local Criterion baselines |
| Kubernetes materializer | `./scripts/test-kind-materializer.sh` |
| Full platform behavior | the targeted `./scripts/test-kind-e2e-*.sh` gate |
| Protocol fixture | `cargo test --locked -p proxy-core --example kind_protocol_app` executes direct HTTP/2, two-proxy-hop and WebSocket fixture regressions |
| Soak inventory reader/polling | `python3 scripts/test-full-wake-sleep-inventory.py` (mocked Kubernetes; no cluster) |
| Repeated wake/sleep leaks | `./scripts/soak-kind-full-wake-sleep.sh` or `./scripts/soak-kind-materializer.sh` |

No cargo tests are required for docs-only changes unless the docs edit also
changes code.

An ordinary workspace run may self-skip optional database wrappers when no
PostgreSQL URL is set. It is not actual store conformance. The separate database
gate requires a complete unfiltered `postgres_store` target, including certificate
store, native API and watch tests, and rejects skips, ignored cases and incomplete
output. Its checker supplements Cargo's exit status; callers must preserve both.

## kind And Smoke Scripts

Targeted gates live in `scripts/`:

- `test-kind-e2e-stateless.sh`: stateless create/wake/hot-route/sleep/delete,
  then the wake-latency measurement.
- `test-kind-e2e-stateful.sh`: StatefulSet plus static PV/PVC and data
  continuity, then the wake-latency measurement.
- `test-kind-e2e-routing.sh`: exact/wildcard HTTP route and HTTP-01 behavior.
- `test-kind-e2e-protocols.sh`: HTTP/1.1, HTTP/2 and WebSocket clients against an
  `http1` and an `h2c` app port, h2c gRPC-shaped calls, and unary and
  server-streaming calls to a grpcio (C-core) server.
- `test-kind-e2e-tls.sh`: native certificate publication, TLS termination and SNI
  passthrough, plus exact-replica certificate rotation/removal, live protocols,
  lifecycle, outage and restart checks. This explicit release gate includes real
  sleep-floor and five-minute lease waits. Local ignored tests are not deployed
  execution evidence.
- `test-kind-e2e-grpc-web.sh`: deployed browser-shaped gRPC-Web operator path
  over platform TLS, with an allowed origin, a refused origin and a refused
  plaintext request.
- `test-kind-e2e-libpq-sni.sh`: real PostgreSQL/libpq 17 direct SNI
  passthrough.
- `test-kind-e2e-failures.sh`: bad route, readiness failure, an auxiliary
  claim that never binds while the wake completes on readiness, bad volume
  template, and stale generation.
- `test-kind-e2e-restart.sh`: control-plane restart recovery, then dependency
  interruption: Postgres stops for 30 seconds during a wake and during a
  drain, and the kind node's API server stops for 20 seconds during a wake.
  Each instance must reach `Running` or `Cold` with no operator RPC, and no
  control-plane container may restart.
- `test-kind-e2e-lifecycle-races.sh`: concurrent wake/sleep/delete/stale
  lifecycle races.

The stateless and stateful gates end by measuring wake latency. Each sample
sends one cold request to a fresh instance and prints a `wake-latency:` line
with the latency added on top of the Pod's own start (the request's whole
duration minus the time from workload scale-up to Pod ready) and its phases:
request to workload scaled up, Pod ready to ready endpoint, ready endpoint to
`Running`, and `Running` to first byte. A summary line gives the mean and worst
of each. `SLEEPYPODS_KIND_E2E_WAKE_LATENCY_ONLY=1` runs the measurement without
the lifecycle test. `SLEEPYPODS_E2E_WAKE_LATENCY_SAMPLES` sets the sample count
(default 10). `SLEEPYPODS_E2E_WAKE_LATENCY_MEAN_BUDGET_MS` and
`SLEEPYPODS_E2E_WAKE_LATENCY_WORST_BUDGET_MS` fail the gate when the added
latency exceeds them.

## Invariants

- The control-plane API is the only public write path for workload classes,
  instances, routes, certificates, TLS hostname bindings, HTTP-01 tokens,
  lifecycle state, and materialization intent. Do not introduce a direct
  user-facing Kubernetes manifest workflow.
- The database is the source of truth. Proxies, sidecars, and users do not write
  it directly.
- Keep provider-specific semantics behind the store roles in `store.rs` and
  `KubernetesMaterializerClient`. A consumer names the role it calls
  (`WakeStore`, `SleepStore`, `ReconcilerStore`, `OperatorStore`, …), so a test
  fake implements its consumer's roles and nothing else. Every store method has
  a production caller; a test reaches a state through those methods.
- A new store operation either repeats while the database is unavailable or
  runs once, in `postgres/store.rs`, and `replay_policy.rs` lists it. It runs
  once when a repeat after a committed write could act on a replacement.
- Every store statement goes through `postgres/sql.rs`, which prepares it once
  per session. A lifecycle write carries its guard in its own `WHERE` clause
  rather than in a read before it. The schema is one baseline migration,
  `migrations/0001_baseline.sql`.
- `WorkloadClassVersion` is immutable; instances pin a class version.
- A create inserts under the caller-chosen ID or compares with the stored
  resource: the same request returns it and any other returns `AlreadyExists`.
  The store keeps no request log.
- A create RPC accepts only what a wake can render. `CreateWorkloadClassVersion`
  and `CreateInstance` dry-run the renderer, so a new template rule belongs in
  `manifest/render.rs`, where both the create call and the wake enforce it.
- Instance values under the `sleepypods.` prefix belong to the platform. The
  renderer supplies them; schemas and requests cannot set them.
- The fixtures in `crates/control-plane/src/manifest/golden/` pin the JSON the
  control plane applies and each object's rendered hash. After an intended
  change to rendered output, rewrite them with
  `SLEEPYPODS_BLESS_GOLDEN=1 cargo test -p control-plane golden` and review the
  diff. The rendered hash changes with the output, and a live object of the
  same generation that carries the old hash is an ownership mismatch.
- A store failure reaches a caller through `store_error_to_status`: backend
  text goes to the `control_plane.store.error` event and the status carries a
  fixed message.
- Per-request facts are metrics. Control-plane stderr carries lifecycle changes,
  errors and rejections, so a read or an accepted caller writes no line.
- Hot proxy paths must avoid control-plane calls, database access, global locks,
  unbounded allocation, and per-request client construction.
- A proxy chooses its route subscription IDs, and they mean nothing outside its
  stream. Keep them out of public resource identity.
- The route cache answers for an exact host with that host's complete rule set.
  Rank and match rules only through `HostRules`, which the store and the proxy
  share.
- Deployment and StatefulSet automatic sleep require exactly one replica and a current Pod UID; unknown or overlapping membership fails closed.
- HTTP/3, multi-cluster remote forwarding, and rich route predicates are
  deferred.
- Observability metric names and log event fields must match
  `sleepypods-observability/src`; update operator docs when they change.
- Add dependencies through the ecosystem command (`cargo add`, `npm install`,
  etc.) unless there is no suitable command or an exact manual constraint is
  required. Inspect manifest and lockfile diffs afterward.

## Agent Workflow

1. Read `docs/development-plan.md` for the current milestone and any
   `Incomplete` rows before editing.
2. Read the source-of-truth files listed above for the area being changed.
3. Make the smallest complete change. Split files by responsibility only when
   it keeps ownership clear.
4. Run focused tests first, then the release gate that matches the behavior.
5. Update docs and development-plan evidence only after the behavior or docs are
   actually present.
6. Run `git diff --check` before review.
7. Do not commit or push unless the user explicitly asks.
