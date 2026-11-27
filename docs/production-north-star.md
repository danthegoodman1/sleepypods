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
- Frontline proxies resolve routes lazily: on a cache miss they subscribe to the
  request's exact host, keep that host's complete rule set in a bounded local
  cache, and receive every later rule set for the host on the same subscription.
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
- Backend addresses exposed to proxies: the Service and the ready Pod.
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
- Route resolution and rule-set pushes for active proxy subscriptions.
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
Subscribe bidi stream:
  proxy -> Subscribe(subscription_id, kind, host)
  proxy -> Unsubscribe(subscription_id)
  control plane -> Rules(subscription_id, rules, cache_policy)
  control plane -> Error(subscription_id, code, message)
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
updates and explain what was materialized. A create call accepts only a class or
instance the renderer can render, so the operator who sent the request receives
the error and a wake meets no template error.

Control-plane authentication and authorization are operator policy. The platform
should expose clear API boundaries, but tenant/user/org permission models are
left to the operator integrating the control plane.

## Data Plane Shape

Frontline proxies are always on and keep local route state.

A request names a host: the HTTP Host or the TLS SNI name. The proxy
canonicalizes it to lower case without port or trailing dots, and keys its cache
by listener family and exact host. For HTTP it also keeps the request path. An
HTTP request with a host over 253 bytes is answered 400, and one with a path
over 4,096 bytes 414, before any lookup.

Route bindings carry a host-like identity: an HTTP Host or TLS SNI name, exact
or a wildcard suffix, plus an optional HTTP path prefix. Richer compound
identity such as headers, ALPN, and protocol-specific fields can be added later
after this path is solid.

```text
HTTP Host:                app.customer.com
TLS SNI:                  db.customer.com
HTTP Host + path prefix:  app.customer.com  /api
Wildcard host:            *.customer.com
```

The cache authority is the **exact queried host with its complete rule set**.
One lookup returns every binding for that host and every wildcard-suffix binding
that covers it, each with its instance state, generation and backend addresses.
The set is ranked most specific first: an exact host before a wildcard suffix, a
longer suffix before a shorter one, then a longer path prefix before a shorter
one. The proxy routes a request to the first rule whose path prefix covers the
request path. The store and the proxy share this ranking through `HostRules` in
`sleepypods-api`.

```text
(http | sni, exact host)
  -> subscription ID chosen by the proxy
  -> ranked rules: matched identity, route ID, instance ID, state, generation,
     Service and Pod backend addresses
```

Because the set is complete, every path on a cached host resolves locally, and a
cached wildcard or root rule can never hide an exact-host or longer-path
binding. A rule set answers only its own host; another name under the same
wildcard gets its own lookup. Rule sets and empty answers have separate
first-in, first-out budgets, so lookups for unknown hosts cannot push routed
hosts out, and a cache hit writes no shared state.

Request flow:

```text
client request
  -> proxy extracts Host/SNI and path
  -> proxy looks up the exact host in its local cache
  -> if missing: send Subscribe(subscription ID, kind, host) on the Subscribe stream
  -> control plane answers Rules or Error for that subscription ID
  -> proxy picks the first rule that covers the path
  -> if no rule covers it: reject
  -> if the rule's instance is Running with a backend: route to the backend
  -> if it is Cold, Draining, Failed or has no backend: call WakeInstance(instance ID)
  -> wait for a rule set that reports Running with a backend
  -> route request or connection
```

An HTTP or WebSocket request connects to the ready Pod address first and to the
Service when the Pod address fails. Kube-proxy programs a new Service up to a
second after its endpoint turns ready, so the first request after a wake would
otherwise wait for it. A Pod address can be reused by another instance's Pod,
so the connection opens with a claim naming the instance, and a sidecar serves
only a claim for its own. TLS passthrough uses the Service. The
[proxy protocol contract](proxy-protocol-contract.md#pod-address-first)
describes the claim and the fallback.

The control plane keeps every subscription current. It sends the host's rule set
again when a binding for the host or a covering suffix is added or removed, and
when an instance or materialization behind one of the rules changes. TTLs bound
how long a proxy whose stream is gone can serve what it holds. A host with no
binding gets an empty rule set, cached briefly, and holds no subscription. When
a subscribed host loses its last binding, the proxy drops the host and asks again
on the next request, so a binding committed a moment later is served at once.

One actor in each proxy owns the stream, the cache and reconnection. The stream
reader forwards every message to it in order. Request tasks read an immutable
snapshot of the cache, so a hit takes no lock and throughput rises with cores.
Pushes, expiry and stream loss progress independently of cold requests.
Commit-to-cache delivery is described in the
[delivery bounds](operator-guide.md#runtime-failure-and-delivery-bounds).

A stream ends in one of two ways, and a lookup can fail on its own:

- Reaching the subscription lifetime ends a stream in order. The proxy keeps
  the rule sets that requests used during that stream, opens the replacement at
  once and registers those hosts again in the background; half the lookup
  flights stay reserved for first-time misses. A kept rule set serves until its
  TTL, which outlasts the stream lifetime by at least ten seconds.
- The control plane ends a stream with an error status when route-change
  events were lost or the proxy stopped reading. Such a stream may have missed
  a change to any cached host, so the proxy clears its cache before it serves
  another request and reconnects after a short backoff. A broken connection is
  treated the same way.
- A lookup that fails ends that subscription alone: an invalid host, a store
  error, a lookup timeout or the per-stream subscription limit. At the limit
  the proxy releases its oldest hosts and asks again.

A lookup flight covers the resolve only. Up to 64 hosts resolve at once with at
most 256 waiting callers, and a flight ends when its last waiter leaves.
Requests for an instance that is not yet running park on that instance and
generation, and hold no flight, so hosts on waking instances leave lookups for
other hosts free. The proxy asks for one wake per instance generation. Parked
requests continue when a pushed rule set reports the instance Running; as a
fallback each asks for its host's rule set again every 1.5 seconds. Wake
acceptance is not readiness: the frontline waits for Running under
`SLEEPYPODS_FRONTLINE_ROUTE_TIMEOUT_MS` (130000ms by default). The wake RPC
retains its separate `SLEEPYPODS_FRONTLINE_WAKE_INSTANCE_TIMEOUT_MS` deadline
(5000ms by default). Frontline also rejects configuration where the route
timeout plus the greater of setup and upstream HTTP header idle timeouts exceeds
190 seconds. Defaults are `130 + max(10, 60) = 190` seconds. This ceiling
matches the activation handoff window described in the
[proxy protocol contract](proxy-protocol-contract.md#activation-and-automatic-idle-sleep);
these setup deadlines do not limit the duration of an established stream.

`Subscribe` is a long-lived bidirectional stream. The proxy sends `Subscribe`
when a request misses its cache and `Unsubscribe` when it evicts a host. The
control plane answers each subscription and follows it with later rule sets:

```text
Subscribe(subscription_id, kind, host)
Unsubscribe(subscription_id)
Rules(subscription_id, rules, cache_policy)
Error(subscription_id, code, message)
```

The proxy chooses `subscription_id`. It is unique among the stream's live
subscriptions and names the subscription in every later message. The control
plane registers the subscription for route-change events before it reads the
rule set and reads again when a change the set depends on commits meanwhile, so
a change cannot fall between the lookup and the registration. `Rules` answers
the subscribe and follows every later change. `Error` ends one subscription
with `INVALID_ARGUMENT`, `RESOURCE_EXHAUSTED` or `UNAVAILABLE`; the stream and
its other subscriptions continue. `Unsubscribe` is idempotent and
unacknowledged because cache eviction, stream reconnect and pushes can race. A
proxy that restarts loses its cache and subscriptions and subscribes again as
requests arrive.

The active subscription registry is in-memory in the control-plane process that
owns the proxy's `Subscribe` stream, indexed by host, by binding and by
instance. Every replica independently reads durable transactional change history
from PostgreSQL and targets its own subscribers; one replica does not consume
another's events. A gap in that history ends every stream with an error status.
A failed read of the history is retried with the cursor intact and leaves
streams open. Positive and negative TTLs provide a bounded fallback. See the
[operator guide](operator-guide.md#runtime-failure-and-delivery-bounds) for the
delivery and admission limits.

Polling loops, provider change streams, versions, watch cursors, and ordering
tokens are entirely internal to the control plane and store provider. The proxy
sends no cursor on reconnect.

Route entries include instance generation so proxies can discard stale backends
after sleep, delete, route reassignment, or failed wake. Subscriptions are
ephemeral and bounded by cache TTL, stream lifetime, or explicit unsubscribe
when a proxy evicts a host.

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
extend it, and a revive during drain restarts it. A wake with no application
traffic still becomes eligible after this finite interval, and the default
300-second class idle timeout continues to dominate. The internal explicit
`BeginSleep` operation can bypass the automatic floor; there is no deployed
operator Sleep RPC. See the
[activation contract](proxy-protocol-contract.md#activation-and-automatic-idle-sleep).

Other Kubernetes workload types are out of scope until these two are robust.

On sleep, the control plane first stops new routing by publishing a draining
generation. Existing requests/connections get a configurable grace period
before Kubernetes objects are deleted. The pinned workload class supplies the
drain grace period, and sleep acceptance persists its fixed deadline. A wake
that arrives before that deadline revives the instance: it returns to Running
on the same Pod and projection generation, with a new instance generation and
backend generation, and nothing is torn down. The sidecar keeps reporting while
idle, so a revived Pod sleeps again. If active traffic remains after the grace
period, the materialization is deleted anyway and the old backend generation is
invalidated; a wake that arrives after the deadline is queued until cleanup
finishes.

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
  WakeInstance before the drain deadline -> Running on the same Pod
  WakeInstance after the drain deadline -> Waking once cleanup finishes
  drain deadline elapsed and materialization deleted -> Cold
  DeleteInstance -> Deleting

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
  create PV from WorkloadClass template and instance values
  create PVC bound to PV
  create Deployment/StatefulSet mounting PVC with zero replicas
  scale the workload up; the scheduler holds its Pod until the PVC is Bound
  wait for a ready endpoint
  publish Running backend

Sleep:
  mark Draining
  push the draining generation to subscribed proxies
  wait for the drain deadline; a wake before it revives the instance
  delete workload and Service
  delete PVC and PV
  wait until every object and Pod is absent
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
4. Add a route subscription API that resolves a host's rule set on the stream,
   pushes later rule sets on the same subscription, and keeps watch cursors,
   versions, and provider polling internal to the control plane.
5. Render active Kubernetes objects from `WorkloadClass + Instance.values`.
6. Add static PV/PVC materialization from `WorkloadClass` templates and
  instance values.
7. Split wake/sleep/delete into explicit reconciled state machines.
8. Keep the first production scope to `Deployment`, `StatefulSet`, HTTP/1.1,
  HTTP/2, h2c, gRPC, WebSockets, HTTPS termination, SNI passthrough, and  provider-backed durable volumes. Defer HTTP/3/QUIC while preserving protocol  listener boundaries that allow it later.
