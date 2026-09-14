# SleepyPods Production North Star

SleepyPods should evolve from a Kubernetes PoC into a control-plane-driven
sleepy workload platform. Kubernetes remains the execution substrate, but the
durable product model lives in the control plane database.

## Core Principles

- The control plane API is the only public write path for instances, routes,
domains, volume identities, and lifecycle state.
- The database is the source of truth, but proxies, sidecars, and users do not
write to it directly.
- Kubernetes contains only active, warming, or draining materializations.
Sleeping instances should not require Deployments, StatefulSets, Services,
PVs, or PVCs to remain in the cluster.
- Frontline proxies use lazy route resolution: resolve identities on cache miss,
  keep a bounded local route cache, and receive targeted updates or
  invalidations through opaque subscription IDs returned by the control plane.
- Workloads are described by reusable classes plus per-instance values, not by
one permanent Kubernetes manifest per instance.

## Primary Resources

### WorkloadClass

A reusable template and policy bundle for one kind of workload.

It defines:

- `Deployment` or `StatefulSet` materialization.
- Pod template shape, sidecar injection policy, probes, resources, and env
contracts.
- Service ports and the mapping from public/service ports to sidecar ports and
local app ports.
- Volume templates, including provider, mount path, access mode, reclaim
behavior, and whether PV/PVC objects are materialized only while awake.
- One replica for both Deployment and StatefulSet automatic sleep, with idle
  timeout bounds. Deployment uses Recreate; multi-replica sleep requires a future
  membership-aware activity protocol and is outside the supported contract.
- A schema for allowed instance `values`.

Idle observations are scoped to a Pod UID and active materialization. The control
plane checks all observed instance Pods, including terminating and unready Pods,
before accepting the single member's idle report. Missing/stale observations
and detected replica drift keep the instance awake. Kubernetes membership and
Postgres intent cannot be checked atomically: managed workload mutation is
reserved to the control plane, and node failure/forced Pod deletion cannot carry
a graceful stream-preservation guarantee. Replacement fencing across that
observation window requires a fresh activation before admitting replacement
traffic. See the operator guide for migration of existing classes and sidecars.

`WorkloadClass` is the right name because it matches Kubernetes concepts like
`StorageClass`, `IngressClass`, and `RuntimeClass`: a reusable class of runtime
behavior rather than just a bundle of templates.

`WorkloadClass` versions should be immutable. An `Instance` pins one specific
class version, and updating a class creates a new version rather than mutating
the behavior of existing sleeping instances. Moving an instance to a new class
version should be an explicit upgrade workflow.

### Instance

One tenant/customer/app/database created from a `WorkloadClass`.

It contains:

- Stable instance ID.
- Referenced `WorkloadClass` and version.
- Validated `values` map injected into the class templates.
- Lifecycle state such as `Cold`, `Waking`, `Running`, `Draining`, `Failed`, and
`Deleting`.
- Generation number for compare-and-swap state transitions and stale update
rejection.

The instance is mostly a map, but the map must be schema-validated, versioned,
and kept separate from secrets and operational status. If a workload needs a
pre-existing provider volume, the provider volume handle is just another
validated value mapped into the `WorkloadClass` PV/PVC templates.

### RouteBinding

Maps request identity to an instance.

It should be first-class, not hidden inside arbitrary instance values, because it
needs uniqueness, indexing, validation, certificate state, and fast proxy lookup.

V1 identity inputs are:

- HTTP `Host`.
- Optional HTTP path prefix.
- Custom domains.
- TLS SNI.
- Wildcard or base-domain subdomain mapping.

Future identity inputs can include headers, ALPN, and protocol-specific fields
such as a Postgres startup database or user.

### Materialization

The active Kubernetes projection of an instance generation into one cluster.

It records:

- Instance ID and generation.
- Target cluster and namespace.
- Rendered object names.
- Backend address exposed to proxies.
- Readiness and failure state.

Materializations are transient. They can be deleted on sleep and recreated on
wake from the control-plane database.

## Control Plane Shape

The control plane owns:

- Instance creation, validation, idempotency, and audit logs.
- Route/domain registration and uniqueness.
- PV/PVC manifest rendering from `WorkloadClass` templates and instance values.
- Wake/sleep/delete state machines.
- Template rendering and generation hashes.
- Route resolution and targeted update/invalidation streams for active proxy
  subscriptions.
- Assignment of active materializations to clusters.

Example APIs:

```text
CreateInstance(workload_class, values, routes)
DeleteInstance(instance_id, expected_generation) -> accepted
WakeInstance(instance_id, expected_generation) -> StillWaking | Ready
ReportIdle(instance_id, generation, sidecar_observation)
PutHTTP01Challenge(host, token, key_authorization, expires_at)
ResolveHTTP01Challenge(host, token)
DeleteHTTP01Challenge(host, token)
Subscribe(proxy_id) bidi stream:
  proxy -> SubscribeRoute(request_id, identity)
  proxy -> Unsubscribe(subscription_id)
  control plane -> RouteResolved(request_id, subscription_id, route_entry, cache_policy)
  control plane -> RouteMiss(request_id, negative_cache_policy)
  control plane -> RouteInvalidated(subscription_id, reason)
WatchMaterializations(cluster_id)
```

The database is the source of truth for route bindings, instances, and
materializations. Live proxy update delivery is a control-plane responsibility,
not a direct database responsibility.

The control-plane API should be defined once with protobuf and exposed through
both native gRPC and gRPC-Web for operator-facing APIs. gRPC-Web support lets
browser and other V8-based environments interact with the control plane without
native gRPC transport support. Keep the proxy/control-plane `Subscribe` stream on
native gRPC in V1 because it is bidirectional; gRPC-Web should cover unary and
server-streaming operator APIs unless a future WebSocket-based proxy protocol is
explicitly added.

Template rendering should be structured and constrained. Prefer typed `WorkloadClass` fields plus limited substitution from validated
`Instance.values` over arbitrary text templating or user-supplied executable
logic. Store a rendered generation hash so the controller can reject stale
updates and explain what was materialized.

Control-plane authentication and authorization are operator policy. The platform
should expose clear API boundaries, but tenant/user/org permission models are
left to the operator integrating the control plane.

## Data Plane Shape

Frontline proxies are always on and keep local route state.

The proxy should not assume it can derive an instance ID directly from every
request. It first extracts request identity and canonicalizes it into a local
cache key. That key is only a proxy-local lookup handle; route dependency
tracking is represented by opaque subscription IDs from the control plane.

Examples:

```text
HTTP Host:
  host:app.customer.com

TLS SNI:
  sni:db.customer.com

HTTP Host + path prefix:
  host:app.customer.com|path:/api

Wildcard host:
  host:*.customer.com
```

For the first production routing model, support host-like identity, meaning HTTP
Host or TLS SNI, plus optional HTTP path prefix. Hosts may be exact names or
wildcard suffixes. Richer compound identity such as headers, ALPN, and
protocol-specific fields can be added later after this path is solid.

The proxy keeps a bounded local cache of answers for **exact canonical queried
identities**. A wildcard or prefix in the returned match is metadata: it never
authorizes reuse for an unseen host, path, or SNI name. The control plane owns
exact-host, wildcard-specificity and longest-path ranking. Separate positive and
negative FIFO budgets bound scanner traffic without mutating order on every hit.

```text
exact queried host + full path, or SNI
  -> opaque subscription ID
  -> matched route rule (metadata)
  -> route ID / instance ID / state / generation / backend
```

Request flow:

```text
client request
  -> proxy extracts Host/SNI and optional path
  -> proxy canonicalizes identity into an exact request identity
  -> proxy resolves that exact identity from local cache
  -> if missing: send SubscribeRoute(request ID, identity) on Subscribe stream
  -> control plane returns RouteResolved or RouteMiss for that request ID
  -> if route is unknown: reject
  -> if route is Running with backend: route to backend
  -> if route is Cold or missing backend: call WakeInstance(instance ID)
  -> wait for Running materialization
  -> route request or connection
```

Proxies must receive explicit invalidation or generation updates for active
subscriptions when an instance sleeps, drains, changes route ownership, or is
deleted. TTL-only cache invalidation is not sufficient for production, but TTLs
remain useful as a safety net if a proxy misses an invalidation.

Every canonical host/path and SNI identity is independently resolved. A cache
containing only a broad wildcard or root route cannot hide an exact-host or
longer-path exception. Negative resolutions are cached briefly without creating
subscriptions.

Invalidations and stream loss progress independently of cold requests. The
production coordinator polls its event queue every 10ms; the frontend processing
target is 100ms after an event reaches that queue under supported load.
Commit-to-cache delivery also includes the control plane's 250ms polling interval,
database/runtime scheduling and any backlog, as described in the
[delivery bounds](operator-guide.md#runtime-failure-and-delivery-bounds).
Reaching the subscription lifetime ends a stream in order. Cached answers keep
serving under identifiers the control plane can never reissue, bounded by the TTL
they already carried, while the frontline registers them on the replacement
stream; half the identity flights stay reserved for first-time misses. A bounded
queue overflow closes the subscription stream and clears all cached authority,
because a lost invalidation can name any cached route. Reconnection never erases
this barrier. Responses crossing a consumed invalidation are
re-resolved, preventing an invalidation-before-install race.

Cold requests share up to 64 identity flights with at most 256 waiting callers.
Wake acceptance is not readiness: the frontline waits for Running under
`SLEEPYPODS_FRONTLINE_ROUTE_TIMEOUT_MS` (130000ms by default), refreshing the
authoritative answer every 100ms while waiting. The wake RPC retains its separate
`SLEEPYPODS_FRONTLINE_WAKE_INSTANCE_TIMEOUT_MS` deadline (5000ms by default).
Frontline also rejects configuration where the route timeout plus the greater of
setup and upstream HTTP header idle timeouts exceeds 190 seconds. Defaults are
`130 + max(10, 60) = 190` seconds. This ceiling matches the activation handoff
window described in the [proxy protocol contract](proxy-protocol-contract.md#activation-and-automatic-idle-sleep);
these setup deadlines do not limit the duration of an established stream.

`SubscribeRoute` is the authoritative cache-miss path. The producer observes
dependency events before resolving the identity and installs active subscription
state before returning `RouteResolved`, so a concurrent change cannot be lost
between resolution and subscription setup.
That response includes the current route entry, cache policy, and an opaque
`subscription_id`. The proxy stores that ID with the local cache entry and uses
it only to apply later stream messages; it must not parse the ID or assume it is
a route binding, instance, version, or cursor. The control plane maps that
subscription to the underlying dependencies, such as the matched `RouteBinding`,
`Instance`, and active `Materialization`.

`Subscribe` is a long-lived bidirectional stream. The proxy sends
`SubscribeRoute` when a request misses local cache and sends `Unsubscribe` when
it evicts a cache entry. The control plane streams lookup responses and later
targeted messages for active subscriptions:

```text
SubscribeRoute(request_id, identity)
Unsubscribe(subscription_id)
RouteResolved(request_id, subscription_id, route_entry, cache_policy)
RouteMiss(request_id, negative_cache_policy)
RouteInvalidated(subscription_id, reason)
RouteUpdated(subscription_id, route_entry, cache_policy)  // later optimization
```

V1 can be invalidation-only: remove the cache entry associated with
`subscription_id`, then let the next request send `SubscribeRoute` again. If a
proxy restarts, it loses its cache and subscriptions; it simply subscribes routes
lazily again as requests arrive.

`request_id` is stream-local and only correlates a `SubscribeRoute` request with
its first response because the stream can have multiple in-flight route misses.
`RouteMiss` should not return a subscription ID by default; unknown Host/SNI
scans should get short negative caching without creating unbounded control-plane
subscription state. `Unsubscribe` should be idempotent because cache eviction,
stream reconnect, and invalidation handling can race.

The active subscription registry is in-memory in the control-plane process that
owns the proxy's `Subscribe` stream. Every replica independently reads durable
transactional change history from PostgreSQL and targets its own subscribers;
one replica does not consume another's events. Retention gaps or dispatcher read
failures reset subscriptions. Positive and negative TTLs provide a bounded
fallback. See the [operator guide](operator-guide.md#runtime-failure-and-delivery-bounds)
for the delivery and admission limits.

Polling loops, provider change streams, versions, watch cursors, and ordering
tokens are entirely internal to the control plane and store provider. The proxy
does not send a cursor on reconnect. If `Subscribe` reconnects, the proxy should
drop or mark stale its subscription-backed cache entries, resubscribe any kept
identities through `SubscribeRoute`, and rebuild missing ones lazily as requests
arrive.

Route entries include instance generation so proxies can discard stale backends
after sleep, delete, route reassignment, or failed wake. Subscriptions should be
ephemeral and bounded by cache TTL, heartbeat, stream lifetime, or explicit
unsubscribe when a proxy evicts a local cache entry.

The same model should support:

- HTTP/1.1, HTTP/2, h2c, gRPC over h2c, and WebSockets.
- HTTPS with TLS termination and SNI-based certificate selection.
- TLS passthrough with SNI sniffing and byte-for-byte forwarding.
- Protocol-specific TCP listeners where the protocol exposes identity.

HTTP/3 and QUIC are explicitly out of scope for V1, but the routing core should
not assume TCP-only HTTP semantics. Protocol listeners should feed a shared
identity extraction, route resolution, wake, and backend streaming core so an
HTTP/3 listener can be added later without changing the control-plane resource
model.

TLS and certificate state come from the control plane as part of route/listener
identity. The control plane is responsible for creating and storing certificates
for custom domains. Frontline proxies must support HTTP-01 challenge handling by
calling `ResolveHTTP01Challenge(host, token)` for
`/.well-known/acme-challenge/*` requests before normal route resolution. The
control plane returns the challenge response body when the token is active, or a
miss when the request should continue through normal routing or be rejected.

HTTP-01 challenge records are short-lived control-plane entries keyed by
`(host, token)`. When ACME issuance starts, the ACME owner inserts
`(host, token, keyAuthorization, expiresAt)` through `PutHTTP01Challenge`. The
frontline proxy resolves that pair through `ResolveHTTP01Challenge` and serves
the returned `keyAuthorization` as `text/plain`. After the ACME authorization
succeeds, fails, or is cancelled, the owner calls `DeleteHTTP01Challenge`.
Expired challenge records should also be garbage-collected so stale tokens are
not served indefinitely.

## Workload Shape

Active workloads should usually be rendered as:

```text
Service
  -> sleepy sidecar port in each selected pod
     -> local app container on 127.0.0.1:<app-port>
```

The sidecar should never proxy back to the same Service that targets the
sidecar, because that can loop. The original service port contract should be
used to configure the sidecar's local upstream.

Supported workload kinds:

- Single-replica `Deployment` for stateless workloads, using Recreate.
- Single-replica `StatefulSet` for stable identity, storage, and stateful services.

For V1, both kinds reject explicit zero or multiple replicas. Automatic sleep
requires the supported single-member activity and ownership checks above. It
also requires the current generation's persisted Ready age to reach
`max(190 seconds, resolved class idle timeout)`, alongside the sidecar's full
quiet interval. The unconditional activation floor protects pending first
requests even after another request finishes; a control-plane restart does not
extend it. A wake with no application traffic still becomes eligible after this
finite interval, and the default 300-second class idle timeout continues to
dominate. The internal explicit `BeginSleep` operation can bypass the automatic
floor; there is no deployed operator Sleep RPC. See the
[activation contract](proxy-protocol-contract.md#activation-and-automatic-idle-sleep).

Other Kubernetes workload types are out of scope until these two are robust.

On sleep, the control plane should first stop new routing by publishing a
draining generation. Existing requests/connections get a configurable grace
period before Kubernetes objects are deleted. The pinned workload class supplies
the drain grace period, and sleep acceptance persists its fixed deadline. If active
traffic remains after the grace period, the materialization is deleted anyway and
the old backend generation is invalidated.

## Instance State Machine

Instances use explicit state transitions with generation checks:

```text
Cold
  WakeInstance -> Waking

Waking
  materialization ready -> Running (persist Ready transition time)
  timeout/error -> Failed

Running
  ReportIdle with full quiet interval and Ready age >= max(190s, class idle) -> Draining
  DeleteInstance -> Deleting

Draining
  drain grace elapsed and materialization deleted -> Cold
  new wake requested before deletion completes -> Waking or Running after reconcile

Failed
  WakeInstance retry -> Waking
  DeleteInstance -> Deleting

Deleting
  routes removed, materialization deleted, instance tombstoned -> deleted
```

Materializations should also carry instance ID, instance generation, rendered
hash, cluster, namespace, readiness, and failure reason. Reconciliation must be
idempotent so a controller restart during wake, sleep, or delete can continue
from database state.

## Storage Manifest Lifecycle

For stateful workloads with pre-existing CSI/provider volumes:

```text
Create instance:
  validate instance values, including any provider volume handles
  keep instance Cold

Wake:
  render PV from WorkloadClass template and instance values
  render PVC bound to PV
  wait for PVC Bound
  render Deployment/StatefulSet mounting PVC
  wait for readiness
  publish Running backend

Sleep:
  mark Draining
  invalidate proxy routes for old generation
  delete workload and Service
  delete PVC and PV
  leave backing provider volume untouched
  mark Cold

Delete instance:
  remove routes/domains
  delete any active materialization
  tombstone Instance
```

This keeps Kubernetes object count tied to active workloads while preserving
durable state across sleep. The platform owns Kubernetes manifests, not the
underlying provider volume lifecycle. Provider disk creation, deletion,
snapshotting, and recovery are external responsibilities unless a future managed
storage mode explicitly adds that ownership.

## Implementation Direction

1. Introduce the control-plane resource model in the DB: `WorkloadClass`,
   `Instance`, `RouteBinding`, and `Materialization`.
2. Move tenant creation behind a versioned protobuf control-plane API exposed
   through native gRPC and gRPC-Web for operator clients.
3. Replace header-only routing with route identity extraction for Host and SNI.
4. Add a route subscription API that resolves cache misses on the stream,
   targets later updates by opaque subscription ID, and keeps watch cursors,
   versions, and provider polling internal to the control plane.
5. Render active Kubernetes objects from `WorkloadClass + Instance.values`.
6. Add static PV/PVC materialization from `WorkloadClass` templates and
  instance values.
7. Split wake/sleep/delete into explicit reconciled state machines.
8. Keep the first production scope to `Deployment`, `StatefulSet`, HTTP/1.1,
  HTTP/2, h2c, gRPC, WebSockets, HTTPS termination, SNI passthrough, and  provider-backed durable volumes. Defer HTTP/3/QUIC while preserving protocol  listener boundaries that allow it later.
