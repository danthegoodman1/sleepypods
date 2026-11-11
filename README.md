# SleepyPods

[![CI](https://github.com/danthegoodman1/sleepypods/actions/workflows/ci.yml/badge.svg)](https://github.com/danthegoodman1/sleepypods/actions/workflows/ci.yml)

SleepyPods runs many single-tenant workloads on Kubernetes and keeps only the awake ones in the cluster. A sleeping instance is a row in Postgres. The first request to its hostname recreates its Deployment or StatefulSet, Service, PersistentVolume and PersistentVolumeClaim from a reusable workload class, holds the request until the Pod is ready, and then delivers it. Once the instance goes idle, SleepyPods drains it and deletes those objects again, leaving the provider disk behind its volume in place.

Three Rust services do the work. The control plane keeps lifecycle state in Postgres and drives Kubernetes. Frontline, an always-on proxy, routes by Host, path prefix or TLS SNI and wakes cold instances. A sidecar in each workload Pod proxies its traffic and reports when it goes idle. Frontline serves warm routes from a local cache without calling the control plane, and in paired runs against direct connections it kept 93–102% of the throughput of HTTP/1.1, h2c, HTTP/2 over TLS and WebSocket traffic while adding at most 0.46 ms to p99.

The [integration evidence](docs/review-evidence/final-integration/README.md) records every deployed gate, measurement and limit. The [operator guide](docs/operator-guide.md) covers the API, deployment and upgrades.

**Status: experimental.** SleepyPods passes its kind lifecycle, restart, race and soak gates and its PostgreSQL conformance suite, and has run in tests and benchmarks only.

## Features

**A sleeping instance holds no Kubernetes objects.** Sleep deletes the workload, Service, PVC and PV, and the next wake renders them again from Postgres. The cluster grows with the number of awake instances, whatever the total.

**The first request survives the wake.** For a cold route, Frontline calls `WakeInstance` and holds the request, for up to 130 s by default, while the control plane applies the workload and waits for readiness. Frontline rechecks the route every 100 ms and forwards the request once the backend is Running. Up to 64 cold identities wake at once, with up to 256 callers waiting on them.

**Idle instances drain, then sleep.** The sidecar counts open requests and connections. After the class's idle timeout passes with none, it reports idle with its Pod UID. The control plane accepts the report only from the current projection's single ready Pod, stops new routing, waits out the class's drain grace, and deletes the objects. Each activation stays up at least 190 s, the longest Frontline's cold-request handoff can take, so a first request still queued there reaches the Pod.

**One class, many tenants.** A `WorkloadClassVersion` is an immutable template: a one-replica Deployment or StatefulSet, the app container, a Service, PV and PVC templates, a value schema and a sleep policy. Each instance pins a version and supplies validated string values. Values bind into the scalars the class author wrote after the manifest parses, so each value lands inside one scalar and the manifest's structure stays the author's. Rendered Pods decline the ServiceAccount token, run under the runtime's default seccomp profile and gain no privileges.

**Lifecycle work survives crashes.** A wake, sleep or delete commits its intent and its pending work in one Postgres transaction before the RPC returns. Reconciliation runs under database leases that any control-plane replica can take over, so a restarted control plane resumes work without a new client call. When the control plane cannot prove a Pod's membership or the outcome of a Kubernetes write, it keeps the instance awake and its resources reserved.

**Proxy caches follow every change.** Frontline caches each exact Host, path or SNI identity it resolves, under an opaque subscription. Every route or lifecycle write records a change in its transaction, and each control-plane replica pushes invalidations to its own subscribers within one 250 ms poll plus database time. A cached wildcard or root route answers only the identity that resolved it, so the most specific route wins in any warm-up order. A 60 s cache TTL bounds the cost of a lost invalidation.

**HTTP, gRPC, WebSocket and any TLS protocol.** Frontline proxies HTTP/1.1, HTTP/2, h2c, gRPC and WebSocket, terminates TLS, and passes TLS through by SNI. A libpq 17 client connects to a cold Postgres instance through direct SNI and gets its query answered once the instance wakes.

**Certificates come from the control plane.** Operators publish an issued chain and key through the API. The control plane validates them, encrypts the key in Postgres under an external key ring, and pushes binding changes to Frontline. Frontline fetches a hostname's certificate on its first handshake and keeps it in a bounded memory cache, so a warm handshake makes no RPC. Frontline answers ACME HTTP-01 challenges from the control plane, so issuance works before the first certificate exists.

**Exclusive attachment for external resources.** A class can declare exclusivity keys, such as a disk handle taken from the instance values. The control plane acquires each key before its first Kubernetes write and holds it until cleanup finishes, so two instances attach a singleton disk one at a time. Crash-injection tests at each lock boundary show recovery resuming the crashed owner's work while a second instance with the same key still conflicts.

## Performance

These runs compare each path through a production Frontline or sidecar image with a direct connection to the same backend, on one arm64 machine in Docker. Each workload ran three rounds at concurrency 8 (TCP at 2) with zero failed requests. Proxied throughput as a fraction of direct, lowest to highest round:

| Path | Workload per round | Proxied ÷ direct |
|---|---|--:|
| Frontline HTTP/1.1 | 5,000 requests | 0.93–0.98 |
| Frontline h2c, gRPC-shaped | 50,000 requests | 1.00–1.02 |
| Frontline HTTP/2 over TLS, gRPC-shaped | 50,000 requests | 0.93–1.00 |
| Frontline generated gRPC client | 2,000 calls | 0.85–0.91 |
| Frontline WebSocket | 500 exchanges of 1 MiB | 0.98–1.00 |
| Sidecar HTTP/1.1 | 5,000 requests | 0.90–0.98 |
| Sidecar TCP | 32 streams of 32 MiB | 1.00–1.01 |

The most any HTTP, gRPC or WebSocket path added to p99 was 0.46 ms, and 5.4 ms for a 32 MiB TCP stream. In every round, warm Frontline paths made zero route subscriptions. Against a stub control plane, Frontline's own cold path took 1.4–1.6 ms: one subscription, one wake and one backend request. [final-performance.md](docs/review-evidence/final-integration/final-performance.md) has every value and the commands.

In Frontline's handshake benchmark, a warm TLS 1.3 handshake with P-256 and h2 took a median of 220 µs over three runs: 9.6% faster than the static-certificate lookup it replaced. Through three cycles of certificate rotation, sleep and wake, each of two Frontline processes peaked at 17.9 MiB sampled RSS. With 16 certificate streams watching 1,024 hostnames each, the slowest instance read took 9.2 ms and the slowest route subscription 26 ms ([results](docs/review-evidence/dynamic-certificates/phase7/README.md)).

Lifecycle gates on kind, against the production images:

| Gate | Result |
|---|---|
| Control-plane restart, one replica | Wake, sleep, deletion, HTTP-01 and route reassignment recover; route notifications converge in 7.9 ms |
| Control-plane restart, two replicas | Both replicas recover the same work; route notifications converge in 124 ms |
| Wake/sleep soak | Two stateless and two stateful cycles: one-request cold wakes and re-wakes succeed, stateful data survives sleep, a wake with no traffic returns to Cold after 224–225 s, and no test objects leak |
| Lifecycle races | Concurrent wakes, deletion during wake and drain, failed-wake retry, stale observations, replica drift and route reassignment all pass |
| Control-plane outage | A warm Frontline serves cached certificates for 303 s, until their five-minute lease runs out, then refuses new handshakes |

### Trade-offs

- **One Pod per instance.** Automatic sleep relies on one sidecar's idle report, so each instance runs exactly one replica: a `Recreate` Deployment or a one-Pod StatefulSet. Scale by adding instances. An HPA or external scaling on a managed workload keeps it awake.
- **Cold starts wait on Kubernetes.** A cold request waits for scheduling, image pull, volume binding and readiness. Frontline holds it for up to 130 s; the wake itself continues under a ten-minute operation deadline.
- **A minimum awake time.** Each activation stays up at least the longer of 190 s and the class's idle timeout after it turns Ready, even when its only request finished at once.
- **Uncertain writes hold resources.** When a Kubernetes write's outcome is unknown, the instance keeps its objects and exclusivity keys until an operator settles it under the [projection recovery contract](docs/projection-safety.md), however long that takes.
- **Generated gRPC.** A generated gRPC client kept 85–91% of its direct throughput through Frontline, the lowest of the measured paths.
- **Static volumes.** Managed PVs use `Retain` and static binding. SleepyPods renders PV and PVC manifests and leaves creating, snapshotting and deleting provider disks to external tooling.
- **Scope.** Each control plane materializes into one cluster and namespace. HTTP/3, multi-cluster forwarding, routing on headers or ALPN, and an operator CLI are future work.

## Quick start

The stateless kind gate builds the three production images, starts a kind cluster with Postgres, deploys the control plane and Frontline, and walks one instance through creation, a single cold request, hot traffic, idle sleep and a re-wake. It needs Docker, kind, kubectl and Rust.

```console
./scripts/test-kind-e2e-stateless.sh
SLEEPYPODS_KIND_KEEP_CLUSTER=1 SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE=1 ./scripts/test-kind-e2e-stateless.sh   # leave it running
```

Operators call `OperatorControlPlane` from [`control_plane.proto`](crates/sleepypods-api/proto/sleepypods/controlplane/v1/control_plane.proto) over native gRPC or gRPC-Web, with `Authorization: Bearer <operator-token>`. A class, an instance and a route, as request shapes:

```text
CreateWorkloadClassVersion({
  idempotency_key: "web-v1", class_id: "web", version: 1, template_generation: 1,
  value_schema: {fields: {"image": {required: true}}},
  template: {
    workload: {kind: WORKLOAD_KIND_DEPLOYMENT, name: {parts: [{literal: "web"}]},
      app_container: {name: "app", image: {parts: [{instance_value: "image"}]},
        ports: [{name: "http", container_port: 8080}]}},
    sidecar: {name: "sleepypods-sidecar", image: {parts: [{literal: "sleepypods/sidecar:latest"}]}, listen_port: 15000},
    service: {name: {parts: [{literal: "web"}]}, ports: [{name: "http", port: 80, target_port: 8080}]}
  },
  sleep_policy: {idle_timeout_ms: 300000, idle_retry_backoff_ms: 5000, drain_grace_timeout_ms: 30000}
})

CreateInstance({
  idempotency_key: "tenant-a", instance_id: "tenant-a",
  workload_class: {class_id: "web", version: 1},
  values: {"image": "registry.example/web:2026-06-24"}
})

CreateRouteBinding({
  idempotency_key: "tenant-a-app", route_binding_id: "tenant-a-app", instance_id: "tenant-a",
  protocol: PROTOCOL_ROUTE_HTTP,
  identity: {http: {host: {kind: ROUTE_HOST_KIND_EXACT, host: "app.example.com"}, path_prefix: "/"}}
})
```

```console
curl -H 'Host: app.example.com' http://<frontline>/   # wakes tenant-a, then returns its response
```

The root `Dockerfile` builds each binary into a distroless image that runs as UID 65532:

```console
docker build --build-arg BIN=control-plane -t sleepypods/control-plane .   # or frontline, sidecar
```

## Configuration

Each binary reads its configuration from the environment. The main variables, with defaults:

| Variable | Default | Effect |
|---|---|---|
| `SLEEPYPODS_POSTGRES_URL` | required | The control plane's store, with `SLEEPYPODS_STORE_PROVIDER=postgres`. |
| `SLEEPYPODS_CLUSTER_ID`, `SLEEPYPODS_NAMESPACE` | required | The cluster and namespace this control plane materializes into. |
| `SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR` | required | Proxy and sidecar services. Workloads reach this listener. |
| `SLEEPYPODS_CONTROL_PLANE_OPERATOR_LISTEN_ADDR` | required | Operator service. Keep it on a network Pods have no route to. |
| `SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR` | off | gRPC-Web for the operator's unary RPCs. |
| `SLEEPYPODS_CONTROL_PLANE_AUTH_MODE` | required | `static-bearer-token`, with distinct `SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN`, `_PROXY_TOKEN` and `_SIDECAR_TOKEN`; or `no-auth` for local tests. |
| `SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE`, `_KEY_FILE` | off | Native platform TLS. Certificate RPCs require it. |
| `SLEEPYPODS_CERTIFICATE_SEALING_KEYS_FILE` | — | Key ring that encrypts certificate private keys in Postgres. Back it up apart from the database. |
| `SLEEPYPODS_OPERATION_TIMEOUT_MS` | 600000 | Deadline for each wake or cleanup phase. An expired wake fails and queues cleanup. |
| `SLEEPYPODS_CONTROL_PLANE_POSITIVE_ROUTE_CACHE_TTL_MS` | 60000 | How long a proxy keeps a route without hearing about it; at most 600000. Lower values add one unsubscribe and one subscribe per route per proxy per TTL. |
| `SLEEPYPODS_CONTROL_PLANE_SUBSCRIPTION_LIFETIME_MS` | 60000 | Proxy subscription streams rotate after this, carrying their cached routes over; at most 600000. |
| `SLEEPYPODS_FRONTLINE_LISTEN_ADDR` | required | Frontline's HTTP listener. |
| `SLEEPYPODS_CONTROL_PLANE_ENDPOINT` | required | The control plane, as Frontline reaches it. |
| `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR` | off | TLS termination with certificates from the control plane. |
| `SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR` | off | TLS passthrough by SNI. |
| `SLEEPYPODS_ROUTE_CACHE_CAPACITY` | 1024 | Route identities Frontline caches. |
| `SLEEPYPODS_FRONTLINE_ROUTE_TIMEOUT_MS` | 130000 | How long a cold request waits for its instance. Added to the larger of the setup and upstream header timeouts, it must stay within 190 s. |
| `SLEEPYPODS_FRONTLINE_WAKE_INSTANCE_TIMEOUT_MS` | 5000 | Deadline for the wake RPC itself. |
| `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS` | 30000 | How long Frontline drains open connections on shutdown. |
| `SLEEPYPODS_{CONTROL_PLANE,FRONTLINE,SIDECAR}_METRICS_LISTEN_ADDR` | off | Prometheus `/metrics` for each binary. |

The control plane renders each sidecar's configuration into its Pod. Each workload class sets its own sleep policy: `idle_timeout_ms`, `idle_retry_backoff_ms`, `drain_grace_timeout_ms`, and optional bounds for a per-instance idle-timeout override. The [operator guide](docs/operator-guide.md#installation-and-configuration) lists every variable, the admission limits and the Kubernetes permissions the control plane needs.

## Compared with Knative Serving

| | SleepyPods | Knative Serving |
|---|---|---|
| Idle workload | Deleted, with its Service, PV and PVC | Deployment scaled to zero; its objects stay in the cluster |
| Source of truth | Postgres, written through the control-plane API | Kubernetes custom resources |
| Workload | One-replica Deployment or StatefulSet per instance | Deployment per Revision |
| Scaling | Zero or one Pod per instance | Zero to many Pods, autoscaled on concurrency, RPS or CPU |
| Cold request | Frontline holds it until the instance is Ready, up to 130 s | The activator buffers it until a Pod is ready |
| Protocols | HTTP/1.1, HTTP/2, h2c, gRPC and WebSocket; TLS termination; TLS passthrough by SNI for any protocol | HTTP/1.1, HTTP/2, gRPC and WebSocket on one container port |
| Volumes | Static PV and PVC per instance, rendered on wake and deleted on sleep | PVCs behind a feature flag |
| Tenants | Instances of one immutable class version, each with its own values | One Service manifest per workload |
| Certificates | Published through the API, encrypted in Postgres, cached in Frontline | Issued through cert-manager |

## Documentation

- [Operator guide](docs/operator-guide.md): the resource model, the API, certificates, configuration, upgrades and limits.
- [Operator runbook](docs/operator-runbook.md): metrics, structured logs and diagnosis.
- [Production north star](docs/production-north-star.md): the architecture, its principles and the instance state machine.
- [Proxy protocol contract](docs/proxy-protocol-contract.md): how Frontline and the sidecar handle each protocol, activation and idle sleep.
- [Projection safety](docs/projection-safety.md): conditional Kubernetes writes, uncertain effects and their audited recovery.
- [Postgres store contract](docs/postgres-store-contract.md): route resolution, lifecycle transactions, lease fencing and ordered change notifications.
- [Proxy hot-path budgets](docs/proxy-hot-path-budgets.md): benchmarks, load smokes and their regression thresholds.
- [Contributor guide](docs/contributor-guide.md): the repo map, invariants and the gate each kind of change needs.
- [Review remediation plan](docs/review-remediation-plan.md): the current status ledger. The [development plan](docs/development-plan.md) and [dynamic certificate plan](docs/dynamic-certificates-plan.md) record milestone history.
- [Integration evidence](docs/review-evidence/final-integration/README.md): every deployed gate, measurement and limit, with logs.

## Development

```console
cargo test --workspace                                   # unit, transport and lifecycle tests
./scripts/test-postgres-store.sh                         # store conformance against a disposable PostgreSQL 17
./scripts/test-kind-e2e-stateless.sh                     # one of the 13 scripts/test-kind-e2e-*.sh gates
./scripts/soak-kind-full-wake-sleep.sh                   # repeated wake/sleep cycles, then a leaked-object check
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_STRICT_BUDGETS=1 ./scripts/smoke-frontline-load.sh   # Frontline against direct connections
./scripts/smoke-sidecar-load.sh                          # the sidecar's HTTP path; smoke-sidecar-tcp-load.sh for TCP
cargo bench -p frontline --bench route_lookup            # warm route-cache lookups
./scripts/check-criterion-regressions.py                 # warn at 15% slower, fail at 25%
./scripts/test-dependency-boundaries.py                  # keep Frontline and the sidecar free of control-plane and Kubernetes crates
```

| Path | Contents |
|---|---|
| `crates/control-plane/` | the operator, proxy and sidecar APIs, the Postgres store, manifest rendering, the Kubernetes materializer and the wake, sleep and delete state machines |
| `crates/frontline/` | the route cache and matcher, the subscription resolver, wake coordination, HTTP-01, the certificate cache and the HTTP, TLS and SNI listeners |
| `crates/sidecar/` | the workload-local HTTP and TCP proxy, idle detection and `ReportIdle` |
| `crates/proxy-core/` | shared proxy primitives: ClientHello and SNI parsing, accounting, drain, timeouts and hot-path benchmarks |
| `crates/sleepypods-api/` | the protobuf, generated clients and services, routing and certificate contracts, and transport authentication |
| `crates/sleepypods-certificate/` | certificate, key, chain and hostname validation shared by publication and Frontline |
| `crates/sleepypods-observability/` | metric and event vocabulary, the process-wide recorder and the Prometheus exporter |
| `crates/sleepypods-types/` | shared ID and value types |
| `scripts/` | kind gates, soaks, production-image load smokes and benchmark regression checks |
| `docs/` | operator and contributor docs, contracts, plans and review evidence |

CI runs rustfmt, clippy with warnings as errors, the workspace tests, the dependency-boundary check and the full PostgreSQL store suite against Postgres 17 on every push and pull request. A second job builds the production images and runs the stateless and routing kind gates.
