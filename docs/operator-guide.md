# SleepyPods Operator Guide

SleepyPods is operated through the control-plane API. Kubernetes is the
execution substrate, but operators create and change user resources through
`OperatorControlPlane`, not by applying generated workload manifests directly.

The protobuf source of truth is
`crates/sleepypods-api/proto/sleepypods/controlplane/v1/control_plane.proto`.
Generated native gRPC clients and gRPC-Web clients share the ordinary unary
operator RPCs. Certificate operations require authenticated native TLS and are
not exposed through gRPC-Web. There is no stable operator CLI yet, so examples
below use request shapes.

## Resource Model

- `WorkloadClassVersion`: immutable template and policy bundle. It defines a
  single-replica `Deployment` or `StatefulSet`, app container, sidecar template,
  Service, optional PV/PVC volume templates, value schema, default values, and
  sleep policy.
- `Instance`: one workload created from a pinned workload class version plus
  validated string `values`. State is `Cold`, `Waking`, `Running`, `Draining`,
  `Failed`, `Deleting`, or `Deleted`, with a generation for stale-update
  rejection.
- `RouteBinding`: maps an HTTP Host/path or TLS SNI identity to an instance.
  Exact hosts, wildcard suffix hosts, custom domains, and optional HTTP path
  prefixes are first-class resources.
- `Http01Challenge`: ACME HTTP-01 token keyed by `(host, token)`. The frontline
  checks challenge paths before normal route resolution.
- `Certificate`: a versioned, validated leaf-first certificate chain and an
  encrypted private key. It is independent of application routing and lifecycle.
- `TlsBinding`: an exact canonical DNS hostname mapped to a certificate ID, with
  its own revision. Several hostnames may share a certificate, and path routes
  for one hostname may refer to different instances.
- `Materialization`: transient Kubernetes projection of an active instance
  generation. Its persisted `projection_generation` is an immutable Kubernetes
  ownership and sidecar incarnation stamp, separate from the instance CAS
  revision. It records rendered refs, target cluster/namespace, backend URI,
  the backend address observed at readiness, readiness, and failure state. The
  backend address is the ready endpoint's IP and port, offered to proxies that
  can route to it directly; proxies that cannot keep resolving the backend URI.
- `WorkloadSleepPolicy`: idle timeout, idle-report retry backoff, drain grace
  timeout, and optional per-instance idle-timeout override bounds.
- Volume templates: static PV/PVC templates rendered from class template fields
  and instance values. Existing provider volumes are attached by putting the
  provider handle/path into an allowed instance value and substituting it into a
  CSI or hostPath source template.

Sleeping instances should not require active Deployments, StatefulSets,
Services, PVs, or PVCs in the cluster. They are recreated on wake from the
control-plane database.

## Operator API

The V1 operator service is unary-only:

- `CreateWorkloadClassVersion`, `GetWorkloadClassVersion`
- `CreateInstance`, `GetInstance`, `DeleteInstance`
- `CreateRouteBinding`, `GetRouteBinding`, `DeleteRouteBinding`
- `PutHttp01Challenge`, `DeleteHttp01Challenge`, `ExpireHttp01Challenges`
- `PublishCertificate`, `GetCertificateMetadata`, `RemoveCertificate`,
  `ReencryptCertificate`, `SetTlsBinding`, `GetTlsBinding` (native TLS only)
- `ReconcileMaterialization`, `ForceDeleteMaterialization`,
  `ForceReleaseExclusivityKey`

`WakeInstance`, `Subscribe`, `ResolveHttp01Challenge`, `ResolveTlsCertificate`,
`WatchTlsCertificates`, and `ReportIdle` are runtime services for proxies and sidecars. They are not
operator or gRPC-Web APIs.

Control-plane authentication is caller authentication at this API boundary. It
does not authenticate application end users and it does not replace network
policy, gateway, or service-mesh placement for direct control-plane exposure.
The operator service answers on its own listener, so a workload that reaches the
proxy and sidecar listener finds no operator method behind it. Every listener
applies the same role policy:

- Operator credentials call `OperatorControlPlane`.
- Proxy credentials call `ProxyControlPlane`, including HTTP-01 lookup and
  certificate resolution and watches.
- Sidecar credentials call `SidecarControlPlane/ReportIdle`.

The first provider is static bearer tokens. Configure it with
`SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=static-bearer-token` plus distinct
`SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN`,
`SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN`, and
`SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` values. Frontlines use only
`SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN`, including for HTTP-01 lookup. The control
plane injects `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN`
into rendered sidecars from runtime config; operators do not put this token in
WorkloadClass templates. Browser/gRPC-Web and native operator clients send
`Authorization: Bearer <operator-token>`.

`SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=no-auth` is for local development and tests
only. It is explicit; omitting the auth mode or configuring malformed, missing,
or duplicate static tokens fails startup instead of silently disabling auth.
Certificate operations remain unavailable in no-auth mode and over plaintext,
including when a client supplies forwarding headers claiming HTTPS. The current
role model is shared operator/proxy/sidecar authentication, not tenant isolation.
Rotate static tokens by updating the control-plane token set and rolling
callers with the corresponding new role token. During rotation, keep exposure
behind trusted network boundaries because static bearer tokens are shared
secrets.

## Certificate publication and bindings

For a first deployment:

1. Provision independent native platform TLS identity/trust, role credentials and
   the external sealing key ring. Start the control plane against the current
   PostgreSQL schema before configuring its callers.
2. Configure Frontline's verified HTTPS endpoint, public CA trust when needed,
   and proxy credential. Its certificate cache may start empty. HTTP-01
   resolution uses `ProxyControlPlane` and the proxy role; operators publish
   challenge state, so HTTP-01 can work before a certificate exists.
3. Publish the issued application bundle and bind each exact hostname before
   expecting successful application TLS handshakes. Configure the control-plane
   endpoint and trust before materializing workloads and their sidecars.

Deploy matching control-plane and caller protocol versions. Mixed old/new
certificate-delivery callers are not an upgrade guarantee; no compatibility
adapter or static-certificate import path is provided.

Publish an already-issued certificate through authenticated native TLS. Supply
the chain as leaf-first DER bytes and the matching private key as unencrypted
PKCS#8 DER bytes. The complete bundle is limited to 128 KiB, 16 chain entries and
100 SANs. The control plane validates the key, server-auth use, chain validity
and hostname coverage before committing any replacement. Privately issued
certificates are accepted; issuance and ACME renewal are external tooling.

```text
PublishCertificate({
  certificate_id: "customer-example",
  expected_version: 0,
  bundle: {chain_der: [<leaf DER>, <intermediate DER>, <root DER>],
           private_key_pkcs8_der: <PKCS#8 DER>}
})

SetTlsBinding({
  hostname: "app.example.com",
  expected_revision: 0,
  certificate_id: "customer-example"
})
```

Expected version/revision fields are required, including zero for a never-used
resource. Use the returned certificate version to rotate the bundle, and use the
binding's revision to change its certificate or unbind it. An unbind omits
`certificate_id`; the hostname retains its revision. Binding keys are exact DNS
names, normalized to lowercase without a terminal dot. Ports, URLs, IP literals
and wildcard binding keys are rejected. A wildcard SAN can cover explicitly
bound hostnames according to normal certificate hostname rules.

A rotation must cover every hostname currently bound to that certificate; one
certificate can have at most 1,024 bindings. Invalid publication leaves the active
version unchanged. `GetCertificateMetadata` returns validity, DNS names,
fingerprint, version and sealing identity, with no private-key readback. Proxy
resolution atomically returns a complete current view, an unchanged matching
view, or an authoritative miss. Storage and decryption failures remain errors.

`RemoveCertificate` requires the current version, erases its stored material and
unbinds every referencing hostname atomically. The certificate ID remains retired
and cannot be reused. A hostname can subsequently bind to a different certificate
using its current binding revision. These writes do not change application
routes or wake instances.

Certificate writes are not automatically replayed after an ambiguous connection
failure. Read current metadata/binding state and compare versions and the desired
certificate fingerprint before attempting another conditional write.

Private keys are encrypted in Postgres using deployment-provided sealing keys.
Back up those keys separately from the database. When changing sealing keys,
first distribute the new read key to every control-plane replica, then switch
every writer to the new active key. Use `ReencryptCertificate` with the current
certificate version and sealing revision for each live certificate. Verify all
live rows use the new key before retiring the old read key. Before that final
check, settle in-flight and ambiguous database writes from old-key writers;
disconnecting a caller or stopping its transport alone does not prove that a
queued commit cannot still finish. Keep old read keys available through this
settlement and retain keys required to restore older backups. Re-encryption
preserves certificate versions and hostname views.

## Frontline certificate cache

Enable `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR` and configure a verified
HTTPS control-plane endpoint with the proxy bearer token. Frontline starts with
an empty certificate cache. It fetches the exact SNI binding on the first TLS
connection, validates the returned bundle, and keeps the result in memory.
Warm handshakes perform no certificate RPC or certificate parsing. HTTP-01 works
through the proxy API before publication and does not require an application
certificate.

Frontline loads no application certificates from files and writes no certificate
cache to disk. Restarting loses the cache; a subsequent TLS connection needs a
successful resolution. The existing bounded control-plane connection setup still
applies at startup. An empty cache alone does not prevent listener readiness.

The cache admits at most 1,024 hostname entries, including misses and pending
lookups, with 64 MiB of accounted memory and at most three simultaneous fetches.
Memory accounting includes fetch scratch space and configurations still retained
by handshakes or other references after cache eviction. It is an admission budget, not a
process RSS limit; connection buffers and the runtime have separate costs.
The worker reserves four MiB for structures, 32 MiB for watch decoding/state and
eight MiB for each outstanding fetch. These conservative reservations cover
malformed repeated protobuf fields as well as valid bundles; they do not imply
that an idle process allocates that much physical memory. Entry and byte pressure
can evict cached views before their leases expire.
Identical misses share one fetch. Lookup has a three-second deadline within the
overall five-second TLS setup bound; capacity exhaustion fails the handshake.

Positive views refresh after approximately 60 seconds with hostname jitter, or
half the granted lease when that is sooner. The worker polls once per second;
leases shorter than a poll or lookup cannot promise a completed refresh before
expiry, and selection still fails closed at the original deadline.
Each successful authoritative resolution grants at most five minutes of service,
capped by the effective validity of the entire chain and measured conservatively
from the start of the RPC. Failed refreshes never extend that deadline. An
unchanged response can renew only the exact certificate view still held locally.
Authoritative misses are cached for at most one second. During a control-plane
outage, an existing valid view remains usable only until its original deadline.

Frontline opens one native certificate watch when its cache first has interests.
The watch pushes complete binding snapshots containing metadata only. Each
registration supplies an ID and exact hostname list; the server acknowledges it
with an atomic current snapshot. Eviction releases the hostname's interest;
updates coalesce into a bounded complete replacement set. Reconnects synchronize
all current interests through either control-plane replica.

The server checks a private global revision and reads the requested bindings only
when it changes, or when a new registration requires a snapshot. There is no
certificate event history or resume cursor. Each binding carries its view revision
and last-invalidating revision. Binding changes and removal advance both;
ordinary rotation advances only the view revision.

A rotation prompts a refresh while an already valid configuration may remain
usable within its existing lease. If a snapshot's last-invalidating revision is
newer than the retained configuration's actual view revision, that configuration
is discarded before another handshake. This also covers unbind/rebind/rotation
coalesced between polls. Per-host floors, local generations and cache incarnations
fence stale responses; a global revision never authorizes a hostname. Snapshots,
reconnects and failed lookups cannot extend a lease. During a partition, delivery
can be delayed until the original lease expires.

Each control plane admits at most 16 certificate streams. Interests are limited
to 1,024 exact hosts per stream, messages to 512 KiB, and queued responses to two.
Initial registration has a three-second setup bound. A registration or snapshot
poll runs at most once per 250 ms per stream; streams renew after at most 60
seconds. Database watch reads wait in a bounded FIFO queue; a waiting read uses
no ordinary database slot. One watch read or its protocol cleanup owns the watch
database slot at a time. These are local admission bounds, not a database
throughput promise.

TLS 1.2 and 1.3 use full handshakes. Server session storage, session tickets and
early data are disabled, so an attempted resumption cannot bypass current SNI
authorization. Already established connections continue under their existing
connection and drain policy.

## Control-plane transport and sealing configuration

Provision the control plane's native TLS identity independently of application
certificates. Set `SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE` and
`SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE` to its PEM certificate chain and matching
private key. These bootstrap files let proxies establish trust before any
application certificate exists. The identity chain is limited to 64 KiB and its
key file to 16 KiB.

Set `SLEEPYPODS_CONTROL_PLANE_PUBLIC_ENDPOINT` to the verified `https://` endpoint
that rendered sidecars should use. Frontlines use
`SLEEPYPODS_CONTROL_PLANE_ENDPOINT`. HTTPS clients verify the endpoint hostname
and certificate chain against public roots. For a private platform CA, configure
`SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM` with its public PEM trust material (at most
64 KiB); the control plane includes this public trust in rendered sidecars.
The CA setting is rejected with a plaintext endpoint. Application TLS bindings
never provide this bootstrap identity or trust.

Frontline gives each native TCP connection attempt and platform TLS handshake
a two-second timeout, including background reconnects on its shared channel.
These stage deadlines are separate from overall RPC and startup deadlines.
Sidecars use their configured TCP setup timeout and the same two-second TLS
handshake timeout. Canceling an async caller does not stop an operating-system
DNS lookup already running on a blocking worker.

`SLEEPYPODS_CONTROL_PLANE_POSITIVE_ROUTE_CACHE_TTL_MS` sets the positive route
cache lifetime the control plane hands to proxies, sixty seconds by default and
at most ten minutes. Subscription invalidations, not this TTL, keep proxy caches
fresh; the TTL bounds how long a dropped invalidation can go unnoticed. A proxy
keeps its cached answers across an orderly stream rotation and registers them
again on the replacement stream, so the TTL outlives
`SLEEPYPODS_CONTROL_PLANE_SUBSCRIPTION_LIFETIME_MS`; a session that dropped
events discards its cache instead. Lowering the TTL increases control-plane
load: an expiring entry costs one unsubscribe and one subscribe per route per
proxy.

The control plane applies `SLEEPYPODS_CONTROL_PLANE_SETUP_TIMEOUT_MS` (five
seconds by default) to the complete server-side TLS handshake as well as initial
request setup. An incomplete handshake closes at that deadline even if the peer
continues to make slow progress receiving the server's output. The existing
accepted-connection limit bounds concurrent socket ownership.

Configure platform TLS before creating workloads. Sidecars receive their endpoint
and public CA when the control plane materializes them; existing Pods do not
reload those values. A later endpoint or embedded-CA change requires a coordinated
cutover: stop accepting new lifecycle mutations, settle pending work under the
old configuration, and let affected instances complete their normal sleep and
cleanup. Change the control plane and caller configuration once those instances
are Cold, then recreate their sidecars through normal wakes. Keep pending
projections on their original configuration until settled; never rewrite a live
Pod template to bypass its generation or desired-hash fence. Rotating the native
server leaf under the same trusted CA does not itself change sidecar trust.

Set `SLEEPYPODS_CERTIFICATE_SEALING_KEYS_FILE` to a restricted deployment file
containing the following JSON shape. Each key is 32 random bytes encoded as
64 hexadecimal characters; generate it using deployment secret tooling.

```json
{"active_id":"key-2026-09","keys":[{"id":"key-2026-09","key_hex":"<64 hexadecimal characters>"}]}
```

The file is limited to 8 KiB and the key ring to eight distinct IDs. The active
ID must exist in that ring. All other entries remain available for decryption
during a coordinated key rotation. Sealing keys require authenticated native
TLS. Do not put them in application templates, database rows, logs or source
control. Database backups without the corresponding sealing keys cannot restore
certificate service.

## Common Tasks

Create a workload class version:

```text
CreateWorkloadClassVersion({
  idempotency_key: "wc-web-v1-20260624",
  class_id: "web",
  version: 1,
  default_values: {"image": "registry.example/web:2026-06-24"},
  value_schema: {fields: {"tenant": {required: true}, "image": {required: true}},
    allow_extra: false},
  template_generation: 1,
  template: {
    workload: {
      kind: WORKLOAD_KIND_DEPLOYMENT,
      name: {parts: [{literal: "web-"}, {instance_value: "tenant"}]},
      app_container: {name: "app", image: {parts: [{instance_value: "image"}]},
        ports: [{name: "http", container_port: 8080}]}
    },
    sidecar: {name: "sleepypods-sidecar", image: {parts: [{literal: "sidecar:prod"}]}, listen_port: 15000},
    service: {name: {parts: [{literal: "web-"}, {instance_value: "tenant"}]},
      ports: [{name: "http", port: 80, target_port: 8080}]},
    volumes: []
  },
  sleep_policy: {idle_timeout_ms: 300000, idle_retry_backoff_ms: 5000,
    drain_grace_timeout_ms: 30000}
})
```

Create an instance:

```text
CreateInstance({
  idempotency_key: "instance-tenant-a-20260624",
  instance_id: "tenant-a",
  workload_class: {class_id: "web", version: 1},
  values: {"tenant": "tenant-a", "image": "registry.example/web:2026-06-24"}
})
```

### Kubernetes Naming Rules

`instance_id` must be a Kubernetes DNS label: lowercase `a-z`, digits, and
hyphens only, starting and ending alphanumeric, with a maximum length of 63
characters. Invalid IDs are rejected by `CreateInstance` before any instance is
stored.

Workload, Service, PVC, and PV template names are operator-readable base names,
not final Kubernetes object names. On wake, SleepyPods appends an instance
suffix to every instance-scoped object name:

```text
<base-name-truncated-if-needed>-<instance-id-hash>
```

The suffix is the first eight lowercase hexadecimal characters of SHA-256 over
the complete instance ID, including for short IDs. The suffix is preserved and the
operator base name is truncated first so the final name remains a DNS label no
longer than 63 characters. SleepyPods does not add the Kubernetes object kind to
generated names; choose base names such as `web`, `api`, `data-pvc`, or
`tenant-pv` when kind readability is useful.

Explicit custom naming templates are allowed, including templates that render
the same base for a workload and Service. The control plane still injects the
instance hash suffix, validates the final Deployment, StatefulSet, Service, PVC, and
PV names, and rejects duplicate or colliding rendered object refs before any
Kubernetes apply. PersistentVolumes are cluster-scoped and are collision-checked
with an empty namespace; namespaced objects are checked with their rendered
namespace.

Typed manifest templates are the preferred path for supported Kubernetes
fields. Operators can use `ManifestTemplate.raw_objects` as an advanced escape
hatch for unsupported fields that must be preserved exactly in applied
Kubernetes objects. Each raw object is a YAML or JSON manifest held in
`RawKubernetesManifestTemplate.manifest`, and that text uses only the same
limited `TemplateText` instance-value substitution model as typed fields. It is
not an executable templating engine.

Raw objects are limited in V1 to `Deployment`, `StatefulSet`, `Service`, `Secret`,
`PersistentVolume`, and `PersistentVolumeClaim`; standalone raw Pods are unsupported. After substitution, the
control plane parses the manifest, validates `apiVersion`, `kind`,
`metadata.name`, and namespace scope, injects the same SleepyPods ownership
labels and template-generation annotations used by typed objects, and derives
rendered refs before any Kubernetes apply. Conflicting SleepyPods identity
labels are rejected. PersistentVolumes must be cluster-scoped; namespaced raw
objects are applied in the materialization target namespace. Raw refs collide
with typed refs, so a raw object cannot reuse the same
apiVersion/kind/namespace/name as a typed-rendered object.

Raw Deployment/StatefulSet Pod templates retain instance cleanup labels but do
not receive the primary `sleepypods.io/workload-name` label. That label is
reserved: raw metadata, Pod-template labels and selectors may not set it.
Consequently the generated primary Service selects only the structured workload
with its injected sidecar. Auxiliary workloads keep their own labels/selectors
and may use separate raw Services; they do not receive the injected sidecar or
its readiness probe.

The generated sidecar exposes a separate HTTP `GET /ready` listener. The renderer
chooses an unused unprivileged port, starting at 15001 and scanning upward (then
1024–15000); it excludes every declared app port, the proxy port, and known
SleepyPods metrics-listener addresses declared in app environment variables.
It injects `SLEEPYPODS_SIDECAR_READINESS_LISTEN_ADDR=0.0.0.0:<port>` and a numeric
HTTP readiness probe with a one-second period, timeout and failure threshold of
one. The health port is absent from the generated Service and user routes;
network policy must allow kubelet probes to reach it. The new health port is
unnamed to avoid app port-name collisions. The proxy retains its existing
`sleepypods` port name for named raw-Service targets unless an app port already
uses that name; in that previously invalid collision case only the injected
proxy port name is omitted. The generated Service and readiness probe always
use numeric target ports. Custom images must declare
other listeners so port selection can avoid them.

Readiness is transport acceptance: the sidecar has bound its actual proxy
listener and can connect to the configured app port on `127.0.0.1` within 500ms.
Loopback-only apps remain supported. This does not assert arbitrary application
semantic health, nor eliminate the normal race between a successful probe and
a later connection. The probe bypasses proxy activity and idle/drain accounting.
The control plane only accepts nonterminating ready EndpointSlice members owned
by the observed current Service UID; stale slices from a deleted same-name
Service cannot publish readiness.

Upgrade custom sidecar images to implement this environment variable and health
endpoint before using the new renderer. An older image leaves new Pods unready
and wake retries eventually reach the configured operation deadline. Existing
Running Pods are not automatically reprojected by this change: use ordinary
sleep/wake after upgrading to obtain the probe and auxiliary-label isolation.
An already-Pending wake with no old objects can adopt the new renderer. A
partially applied old same-generation projection has an obsolete rendered hash;
it becomes a permanent wake failure and the scheduler queues its recorded refs
for safe cleanup before an ordinary wake retry can recreate it. Existing
uncertain-effect barriers still require their documented recovery process.
Quiesce managed work when correcting legacy raw templates that explicitly set
the reserved selector label; ownership and retained-storage rules still apply.

Add a route or custom domain:

```text
CreateRouteBinding({
  idempotency_key: "route-tenant-a-app",
  route_binding_id: "route-tenant-a-app",
  instance_id: "tenant-a",
  protocol: PROTOCOL_ROUTE_HTTP,
  identity: {http: {host: {kind: ROUTE_HOST_KIND_EXACT, host: "app.example.com"},
    path_prefix: "/"}}
})
```

Use `ROUTE_HOST_KIND_WILDCARD_SUFFIX` for wildcard suffix routing, for example
`host: "*.apps.example.com"`. Use `PROTOCOL_ROUTE_TLS_SNI` with `identity.sni`
for TLS/SNI passthrough routes.

Add an HTTP-01 challenge token:

```text
PutHttp01Challenge({
  key: {host: "app.example.com", token: "token-from-acme"},
  key_authorization: "token-from-acme.account-key-thumbprint",
  expires_at_unix_millis: 1782260000000
})
```

After the ACME check finishes, call `DeleteHttp01Challenge`. Periodically call
`ExpireHttp01Challenges` with the current Unix milliseconds to garbage-collect
expired records.

Attach an existing volume:

1. Add required value fields such as `volume_handle`, `mount_path`, or
   `host_path` to the workload class schema.
2. Reference those values from a `VolumeTemplate` `source.csi.volume_handle` or
   `source.host_path.path`, plus `pv_name`, `pvc_name`, `capacity`, access
   modes, reclaim policy, and optional storage class.
   Static CSI volumes can also set typed secret refs on `source.csi`, including
   `controller_publish_secret_ref`, `node_stage_secret_ref`,
   `node_publish_secret_ref`, `controller_expand_secret_ref`, and
   `node_expand_secret_ref`. Each ref has templated `name` and `namespace`
   fields, so external CSI drivers can receive per-instance secrets such as an
   Archil-style `node_publish_secret_ref`.
3. For singleton external resources that must not be attached by two active
   materializations at once, declare a workload-class `exclusivity_keys` entry
   such as `name: "disk"` and `value: "{{ volume_handle }}"`.
4. Create the instance with the provider volume handle/path in `values`.

The control plane renders PVs first, then PVCs, then Service and workload. PVCs
are bound before the backend is published. Exclusivity keys are opt-in and
opaque to SleepyPods: the control plane does not parse provider disk IDs or infer
shared singleton resources from template values. A rendered key is acquired
before Kubernetes apply starts and stays held until sleep/delete cleanup
finalizes the materialization.

### Materialization Recovery And Force Operations

Every control-plane replica runs materialization reconciliation. `Pending` and
`Deleting` materializations use durable database leases; process-local ownership
is not authoritative. Another replica can take over after lease expiry when no
mutating Kubernetes outcome is unresolved. Final transitions require the current
owner, acquired attempt, materialization id, target, state and generation.
Readiness waits renew their lease against the claimed work state. Accepting Delete
during Pending supersedes that work: the next heartbeat cancels its read wait,
and a definite old failure cannot mark the deletion failed. Cleanup becomes
eligible after the owned attempt settles and its exact lease is safely released;
existing drain grace and uncertain-effect barriers remain authoritative. See [projection safety](projection-safety.md)
for conditional mutations, effect diagnostics and the crash/cancellation boundary
that deliberately retains reservations.

Non-terminal materializations intentionally keep exclusivity keys held. A
different instance with the same rendered key should receive an exclusivity
conflict until the owning materialization reaches `Deleted` or an operator uses
an explicit force operation.

`ReconcileMaterialization` is the first operator action for a stuck
materialization. It loads the row by materialization id, returns current state,
lease metadata, recorded object refs, and live projection observations, and
enqueues eligible `Pending`, `Deleting` or legacy `Failed` work through the same
scheduler. `attempted` means enqueue accepted, not work completed. Set
`status_only=true` to inspect without changing its schedule. Responses include
next attempt, operation deadline, failure count/class/message, original terminal
wake reason and any unresolved effect identity. Enqueue honors active leases,
uncertainty barriers and the immutable drain deadline. Recorded-ref projection observations classify each ref as missing,
owned, unowned, deleting, delete-blocked, or inspect-failed with bounded
reason/finalizer details. Ready-row inspection is metadata-only in V1: it does
not prove EndpointSlice/readiness drift or rendered-hash mutation drift because
the current desired manifest is not reconstructed for recorded refs.

SleepyPods stamps applied Kubernetes objects with `managed-by=sleepypods`,
materialization id, instance id, instance generation, and a deterministic
rendered hash. With no unresolved effect, missing refs can be created during
`Pending` and tolerated during `Deleting`. Owned updates and foreground deletes
require the observed UID/resourceVersion. Cleanup also waits for old Pods and
ReplicaSets, including terminating members; a missing workload object alone is
insufficient.

An `inspect_failed` observation means the control plane could not read that
Kubernetes ref, so cleanup or wake safety is not proven. Treat it as a
retryable Kubernetes/API-access problem unless the bounded reason indicates a
persistent permission or discovery issue.

`ForceDeleteMaterialization` is an emergency cleanup tool for a materialization
whose Kubernetes cleanup has already been inspected. The response includes the
recorded object refs that were present before the row was cleared plus
best-effort projection observations for those refs. Observation does not mutate
Kubernetes objects. It also clears any unresolved effect barrier. Before using
it, fence the old process and request path, prove old mutating API requests can
no longer take effect, then verify object/descendant cleanup and external resource
safety. Process termination or an absent name alone cannot prove that a delayed
create will never arrive. Retain reservations if that proof is unavailable;
record the evidence in the required audit reason.

`ForceReleaseExclusivityKey` removes a rendered key from matching active
materializations without deleting the materialization. It requires the exact
target, key name, key value, operator, and reason. The response includes
best-effort projection observations for materializations that held the released
key when the operation ran. This is a last-resort escape hatch; using it while
external singleton resources still exist can allow two instances to attach the
same resource.

Delete an instance or route:

- Use `DeleteRouteBinding` to remove a route/custom domain.
- Read the instance revision with `GetInstance`, then call `DeleteInstance` with
  `instance_id` and the explicitly present `expected_generation` (including zero).
  `accepted=true` means deletion intent is durable. The instance immediately
  becomes `Deleting`; the reconciler cleans every active materialization before
  `GetInstance` returns `NotFound`. Retry the same revision after a lost response;
  a stale revision cannot delete a replacement using the same ID. Existing drain
  deadlines remain in force. Cleanup failure keeps the instance and reservations
  visible for diagnosis and retry.

Sleep and wake:

- Wake is runtime-driven: a cold route request causes the frontline to call
  `ProxyControlPlane/WakeInstance`.
- Sleep is sidecar-driven: when active work reaches zero for the idle timeout,
  the sidecar drains and reports idle to the control plane.
- Operators can inspect state with `GetInstance`.

## Runtime failure and delivery bounds

Each replica supervises its lifecycle driver, durable event dispatcher, retention
maintenance and listeners. SIGTERM and Ctrl-C stop admission and cooperatively
cancel owned work. Unexpected critical-task failure stops the other components
and exits with an error, so readiness cannot remain successful in a controller
that has silently stopped. Control-plane async shutdown allows 25 seconds before
forced task abort. After owned async cleanup returns, each binary allows up to
one additional second for Tokio runtime teardown, so a leftover blocking DNS
worker cannot pin process exit. Frontline initial control-plane Channel setup
has its separate 60-second overall retry budget and observes SIGINT/SIGTERM
before connecting. Existing data-plane drain grace periods remain unchanged;
none of these async budgets includes that final one-second teardown allowance.

The controller continuously discovers work with bounded active jobs. Deletion is
listed before wakes and has reserved capacity, so a backlog of never-ready wakes
cannot hide healthy cleanup. A controller run metric now measures one discovery
and scheduling scan; work and Kubernetes latency have separate metrics.
Transient failures persist a 2–32 second exponential backoff. The default durable
operation deadline is ten minutes, configured for new phases by
`SLEEPYPODS_OPERATION_TIMEOUT_MS` (1 millisecond to 24 hours). Definite permanent
errors and expired wake deadlines publish Failed and queue safe cleanup. The
original wake reason survives cleanup retries; a new wake can start after cleanup
finishes. Failed cleanup and uncertain effects retain refs and exclusivity.

Every semantic route/lifecycle write records change intent in its transaction.
Each control-plane process independently polls targeted history every 250ms in
batches of 1024. Unrelated instance changes leave existing hot subscriptions
intact. Transactional revision reservation orders commits; it adds a short shared
row-lock section to semantic writes. History retains at most 100000 records and
maintenance removes an expired ten-minute prefix. Lag beyond retained history or
a dispatcher read failure resets subscriptions. Five consecutive dispatcher or
maintenance failures stop the runtime. Under healthy storage, an unbacklogged
change is dispatched within one polling interval plus database/scheduling time;
a backlog requires additional batches. This is not a hard wall-clock guarantee
during storage failure. Positive cache TTL is sixty seconds by default and
negative TTL is one second, providing a bounded fallback even when transport
notification fails. Reaching `SUBSCRIPTION_LIFETIME_MS` ends a stream in order,
and the proxy keeps serving its cached answers while it registers them on the
replacement stream. A session that dropped events discards its cache, because a
lost invalidation can name any cached route.

Native gRPC and optional gRPC-web share finite admission. Defaults and controls:

| Environment variable suffix after `SLEEPYPODS_CONTROL_PLANE_` | Default |
| --- | --- |
| `MAX_CONNECTIONS` | 256 accepted sockets, shared across listeners |
| `MAX_RPCS` | 128 unary handlers/responses including final queued DATA |
| `MAX_SUBSCRIPTION_STREAMS` | 64 owned streams including queued DATA |
| `MAX_SUBSCRIPTIONS_PER_STREAM` | 256 dependency entries |
| `SETUP_TIMEOUT_MS` / `WRITE_TIMEOUT_MS` | 5000 / 5000 |
| `UNARY_DELIVERY_TIMEOUT_MS` | 5000 from response headers to complete delivery |
| `SUBSCRIPTION_LIFETIME_MS` | 60000, at most 600000; then reconnect/refresh |
| `LOOKUP_TIMEOUT_MS` / `RESPONSE_TIMEOUT_MS` | 3000 / 1000 |
| `POSITIVE_ROUTE_CACHE_TTL_MS` | 60000, at most 600000; bounds a missed invalidation |

Admission occurs before connection tasks or protobuf decoding. Each connection
allows 32 H2 streams; protobuf requests/responses are capped at 256KiB/1MiB.
Subscription IDs/request IDs, hosts and paths have byte-length bounds. Producers
have a 16-response queue and stop when enqueue blocks for the response timeout.
Transport capacity may remain held until the separate subscription lifetime;
it does not necessarily return within the producer timeout. Unary delivery uses
a total delivery deadline, not an inactivity timeout. An H2 peer that answers
PING while withholding credit is closed on its delivery/subscription deadline;
public tonic/Hyper cannot reset an already queued final DATA frame individually,
so other streams on that connection are also cancelled. Ordinary active
subscriptions use their explicit lifetime, not the unary delivery deadline.

Maintenance runs every five seconds in bounded batches for HTTP-01 expiry,
opt-in idempotency expiry and event retention. Permanent idempotency tombstones
and instance generation watermarks are preserved.

## Lifecycle Expectations

- Cold wake: the API validates and renders a projection without contacting
  Kubernetes, then atomically commits `Waking` and `Pending` work and returns
  `StillWaking`. The sole reconciler uses a one-second default polling interval,
  applies and waits for readiness. The frontline
  waits within its bounded routing deadline until authoritative updates report a
  ready backend, preserving the first cold request through the asynchronous wake.
  The optional `backend_generation` request is a minimum; acceptance allocates a
  value strictly newer than the prior materialization, including deleted rows.
- Wake during drain: the API durably queues the next incarnation while the
  instance remains `Draining`. Cleanup waits for the complete persisted grace;
  finishing cleanup and promoting the queued wake are one transaction. Delete
  cancels the queued wake in its acceptance transaction.
- Hot route: the frontline uses its local route cache and must not call the
  control plane on measured hot-cache hits.
- Idle sleep and drain: a report from the exact current projection and live Pod
  UID starts a persisted drain deadline. Ordinary and manual reconciliation both
  honor that deadline before deleting recorded objects. A report from an older
  projection cannot authorize sleep of a replacement.
- Delete: operator delete durably accepts generation-checked intent and
  invalidates route dependencies. The reconciler removes active materialization
  objects and finalizes deletion; acceptance does not mean cleanup has finished.
- Route/domain change: create/delete route bindings through the API. Active
  proxy subscriptions are invalidated so the next request resolves the new
  route instead of relying only on TTL expiry.
- Restart recovery: durable state is in Postgres. Control-plane restart resumes
  discoverable work and route notifications without a new client RPC. Ambiguous
  mutating effects retain inventory and reservations until audited recovery; see
  [projection safety](projection-safety.md).
- Persistent disk behavior: PV/PVC manifests are active materialization objects.
  Use reclaim policy and provider volume handles deliberately; data continuity
  comes from the external volume, not from keeping Sleeping Kubernetes objects.
  Declare workload-class exclusivity keys for external resources that require
  single-writer or single-attachment behavior across instances.

## Upgrading lifecycle APIs and state

Migrations 8 and 9 require a coordinated control-plane/client upgrade: stop old
control-plane writers, settle their in-flight mutating Kubernetes requests, apply migrations, and
start the new controller with regenerated operator clients. Mixed old/new
lifecycle drivers are unsupported. The `DeleteInstance` request now requires
presence of `expected_generation`. The old response tag/name `deleted` is
reserved; `accepted` uses tag 2 so old clients cannot misread acceptance as
completed deletion. Poll `GetInstance` for completion instead of repeatedly
issuing deletion to drive cleanup.

Existing Pending ownership stamps are backfilled from the old future Running
stamp; Ready and Deleting stamps retain their deployed generation. An old
`Waking` instance with no Pending record has no durable target, so migration
marks it `Failed` with an advanced revision. An explicit wake retry can then
choose the configured target. Legacy failed projections are queued for cleanup
before any new incarnation can replace their recorded inventory.

Per-ID generation watermarks are permanent and independent of optional
idempotency-key expiry. An ID deleted before watermarks existed is permanently
retired when deletion history is available; create a new ID. If historical
idempotency/deletion records were manually erased before this migration, the
old incarnation is unknowable: use new instance IDs for those historical
resources and quiesce incompatible clients during upgrade. Never restore a
database without its generation watermarks while allowing old clients or
sidecars to continue.

Each reconciler processes only its configured cluster/namespace. Delete marks
all targets as work, but deletion completes only after each target's controller
has proven cleanup. A missing controller keeps that target visible as Deleting;
absence in another cluster cannot release its reservations.

## Installation And Configuration

Run three production components:

- Control plane: one native gRPC listener for the proxy and sidecar services,
  a second for the operator service, and an optional gRPC-Web listener for
  operator unary APIs. Workloads reach the first; keep the operator listeners on
  a network path pods have no route to.
- Frontline: always-on HTTP listener; optional TLS termination and TLS/SNI
  passthrough listeners; connects to the control plane.
- Sidecar: injected into materialized workloads; proxies to the local app port
  and reports idle.

The control-plane service account needs `get`, `list`, `watch`, `create`, `patch`,
`update`, and `delete` for namespaced Secrets as well as its managed Services and
workloads. Pod inspection needs `get` and `list`; ReplicaSet inspection needs
`get` and `list` (membership reads and descendant absence scans). Retain the
existing PVC/PV and EndpointSlice permissions appropriate to the workload.
Secret reads and writes are scoped to the configured target namespace.

Important environment variables:

| Component | Variable |
| --- | --- |
| control plane | `SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR` carries the proxy and sidecar services |
| control plane | `SLEEPYPODS_CONTROL_PLANE_OPERATOR_LISTEN_ADDR` carries the operator service |
| control plane | `SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR` optional |
| control plane | `SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=no-auth` for local tests, or `static-bearer-token` for configured auth |
| control plane | `SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN`, `SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN`, `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled |
| control plane | `SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE`, `SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE` for native platform TLS |
| control plane | `SLEEPYPODS_CONTROL_PLANE_PUBLIC_ENDPOINT` verified HTTPS endpoint for rendered sidecars when native TLS is enabled |
| control plane | `SLEEPYPODS_CERTIFICATE_SEALING_KEYS_FILE` external key ring for certificate storage |
| control plane | `SLEEPYPODS_STORE_PROVIDER=postgres` |
| control plane | `SLEEPYPODS_POSTGRES_URL` |
| control plane | `SLEEPYPODS_CLUSTER_ID`, `SLEEPYPODS_NAMESPACE` |
| control plane | `SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR` optional Prometheus `/metrics` listener |
| frontline | `SLEEPYPODS_FRONTLINE_LISTEN_ADDR` |
| frontline | `SLEEPYPODS_CONTROL_PLANE_ENDPOINT` |
| frontline | `SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN` when control-plane static auth is enabled |
| control plane / frontline / sidecar | `SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM` optional public trust for a private platform CA; control plane injects it into rendered sidecars |
| frontline | `SLEEPYPODS_ROUTE_CACHE_CAPACITY` optional, default `1024` |
| frontline | `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS` optional, default `30000` |
| frontline | `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR` optional |
| frontline | `SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR` optional |
| frontline | `SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR` optional Prometheus `/metrics` listener |
| sidecar | rendered by the control plane: listen address, app port, instance ID, generation, downward-API `SLEEPYPODS_POD_UID`, control-plane endpoint, idle policy, `SLEEPYPODS_SIDECAR_MODE`, and runtime-injected `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled |
| sidecar | `SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR` optional Prometheus `/metrics` listener |

Kubernetes permissions must allow create, conditional merge update, inspection
and foreground delete of rendered Deployments, StatefulSets, Services, Secrets,
PVs and PVCs. Read PVC/Service/EndpointSlice readiness, and grant `get`/`list` for
Pods and ReplicaSets in the target namespace. Failure to inspect ownership,
retained volume bindings or descendants keeps cleanup and exclusivity held.
The default field manager is `sleepypods-control-plane`.

Back up Postgres with generation watermarks and unresolved effect rows. For
migrations 8/9, quiesce old writers and settle their mutating Kubernetes requests
before starting the new driver and regenerated clients; ordinary mixed-version
rolling lifecycle drivers are unsupported. Then update frontlines and recreate
sidecars through normal workload materialization. Keep old images available for
an explicitly coordinated rollback that respects the new state contract.

Managed static volumes require `Retain`; provider disk deletion is outside the
lifecycle API. New classes and rendering of legacy classes reject `Delete`.
Rendered raw PV/PVC inventory must also use explicit Retain and static bindings
within that inventory; dynamic or externally bound claims cannot be automatically
managed. Existing live PVs with Delete/unknown policy, and PVCs whose retained
binding cannot be proven, block the entire cleanup pass before any PVC deletion.
Correct a legacy live policy only with controllers quiesced and an explicit
storage retention decision. Use a Retain class version for subsequent creation;
the controller never silently rewrites the operator's policy.
The managed-storage contract requires exclusive control over PV/PVC specs,
especially reclaim policies and bindings. Quiesce managed work before external
policy or binding edits: a conditional PVC delete cannot atomically fence an
independent change to its PV after the retention preflight.

## Single-replica automatic sleep and upgrades

Automatic sleep has a generation-specific minimum Ready age of
`max(190 seconds, resolved idle_timeout)`, in addition to the sidecar's full quiet
interval. The 190-second activation floor protects pending first requests even
when another request finishes quickly. It also applies to an abandoned wake with
no traffic and to successful short wakes; it is not extended by a control-plane
restart. With the default 300-second idle policy, that longer interval determines
the minimum uptime. The internal explicit `BeginSleep` operation remains able to
bypass this automatic-idle restriction; there is no public operator Sleep RPC.

Frontline rejects configuration whose route timeout plus the greater of setup
and upstream HTTP header idle timeouts exceeds 190 seconds. Defaults are
`130 + max(10, 60) = 190`. This ceiling bounds activation handoff, not an active
stream's lifetime. See [the protocol contract](proxy-protocol-contract.md#activation-and-automatic-idle-sleep)
for setup ownership and activity-interruptible idle deferral.

Automatic sleep supports one structured Deployment or StatefulSet with exactly
one Pod. Deployments render with `Recreate` strategy to prevent normal rollout
surge. Each sidecar reports its Pod UID from the Kubernetes downward API. Before
accepting an idle report, the control plane verifies the pinned class, current
Ready materialization, workload ownership and desired replica count, and exactly
one ready, non-terminating Pod with that UID and incarnation. Deployment pods must
belong to a ReplicaSet owned by the observed Deployment; StatefulSet pods must
belong to that StatefulSet. Unknown membership, extra Pods (including old,
terminating or unready Pods), and extra raw workload controllers refuse sleep.
Auxiliary raw workloads do not participate in an aggregate idle protocol.

The control plane owns the managed workloads, their selectors, replica counts,
and Pod templates. Do not attach an HPA, scale them externally, mutate their Pod
templates, or force-delete/reparent Pods. Detected drift fails closed. The
membership check is a snapshot, not an atomic transaction spanning Kubernetes
and Postgres: mutations or controller replacement after the check can race the
sleep transition. A terminated or failed Pod cannot preserve its active streams;
node failures and forced deletion are outside the graceful-drain guarantee.
Full replacement fencing would require a new activation incarnation before a
replacement Pod is admitted to traffic. Normal control-plane lifecycle changes
remain guarded by instance generation.

Before upgrading, audit pinned class versions for explicit `replicas: 0` or
`replicas > 1`, and raw objects that add Deployment/StatefulSet controllers.
Existing classes with unsupported replica counts remain readable and deletable,
but cannot accept new instances, render a new activation, or authorize automatic
sleep. Auxiliary raw Deployment/StatefulSet manifests remain supported for
creation and rendering, but their instances cannot authorize automatic sleep;
there is no aggregate observation of their activity. Running instances are left
awake; migration is explicit, without scaling or deleting active peers
automatically. Create a new immutable class version with one
replica, move consumers to a new instance through the operator API, and retire
the old instance after its activity has drained. Existing RollingUpdate
Deployments must be rematerialized through the control plane to acquire the
Recreate strategy and downward-API Pod UID.

Deploy the upgraded control plane and required read permissions first. Old
sidecars omit `pod_uid`; their idle requests receive `FailedPrecondition` and
cannot initiate sleep. Roll compatible sidecar images through normal
rematerialization. These fail-closed behaviors intentionally favor keeping a
workload awake over sleeping an unobserved busy peer.

## V1 Limits

- Deployment and StatefulSet replicas must be omitted (defaults to one) or exactly one. Zero and multiple replicas are rejected at class creation, instance creation, and rendering.
- HTTP/3 is deferred.
- Multi-cluster remote forwarding is deferred; V1 materializes into the
  configured target cluster/namespace.
- Rich route predicates beyond host/SNI and optional HTTP path prefix are
  deferred.
- Generated examples are illustrative request shapes until a CLI exists.
- Prometheus metrics are intentionally limited to low-cardinality runtime,
  proxy, reconciler, materialization backlog, held-key, and Kubernetes
  controller-operation metrics. Use Kubernetes/container telemetry for CPU,
  memory, restarts, pod scheduling, network, and filesystem metrics.
