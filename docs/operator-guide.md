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
  Service, optional PV/PVC volume templates, a value schema with per-field
  defaults, and sleep policy.
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
  readiness, and failure state.
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
- `PutHttp01Challenge`, `DeleteHttp01Challenge`
- `PublishCertificate`, `GetCertificateMetadata`, `RemoveCertificate`,
  `ReencryptCertificate`, `ReencryptCertificates`, `SetTlsBinding`,
  `GetTlsBinding` (native TLS only)
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

The gRPC-Web listener serves the platform TLS identity whenever
`SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE` and
`SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE` are set, so the operator token crosses
it encrypted; browsers negotiate HTTP/2 on it.
`SLEEPYPODS_OPERATOR_GRPC_WEB_ALLOWED_ORIGINS` lists the origins whose pages may
call the listener, comma-separated and written as a browser sends them: a
lowercase `https://console.example.com`, with a port only when the page uses
one. The list is empty by default, so name each console origin before its pages
can call. A request whose `Origin` is outside the list receives HTTP 403 before
authentication, and CORS responses name only listed origins. A request with no
`Origin` header comes from a client other than a browser page, and its bearer
token decides it. Startup fails on a malformed origin, and on an origin list
without `SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR`.

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
Back up those keys separately from the database. To rotate a sealing key:

1. Add the new key to the key ring of every control-plane replica and keep the
   current `active_id`.
2. Switch `active_id` to the new key on every replica. New publications seal
   under it.
3. Call `ReencryptCertificates({sealing_key_id: "<old key>"})` until `remaining`
   is zero. Each call re-seals up to sixteen active certificates under the
   active key and returns the number the old key still seals.
   `ReencryptCertificate` re-seals one certificate at its current version and
   sealing revision.
4. Settle in-flight and ambiguous database writes from old-key writers, repeat
   the call to confirm `remaining` is zero, then remove the old key from every
   ring.

At startup the control plane compares the sealing keys of active certificates
with its ring. When a certificate is sealed under a key the ring lacks, the
process exits and names the missing key IDs; restore those keys, or finish
re-encryption on a replica that holds them, then start it again. Disconnecting
a caller or stopping its transport alone does not prove that a queued commit
cannot still finish, so keep the old key in the ring through settlement and
retain the keys required to restore older backups. Re-encryption preserves
certificate versions and hostname views.

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

The cache holds at most 1,024 hostnames with a certificate. A separate budget
holds at most 1,024 hostnames without one: authoritative misses, pending lookups
and failed lookups. A full budget evicts its least recently handshaken hostname,
choosing first among those with no handshake waiting. A hostname without a
certificate therefore displaces only another hostname without one, and only a
1,025th certificate evicts a cached one. At most three fetches run at once.

Entry counts bound cache memory. Validation limits a bundle to 128 KiB and 100
SANs, so a cached certificate retains at most about 160 KiB: 160 MiB for 1,024
maximum-size certificates, and about 10 MiB when chains are a few KiB. A hostname
without a certificate costs about 1 KiB at most. A resolve response is at most
256 KiB on the wire and a watch snapshot 512 KiB; a malformed one expands to at
most 8 MiB per fetch and 32 MiB for the watch while it is decoded and rejected.
The worst case is about 220 MiB. A configuration that is rotated or evicted while
connections still use it stays in memory until those connections close, so
`SLEEPYPODS_PROXY_MAX_CONNECTIONS` bounds how many remain. Connection buffers
and the runtime have separate costs.

Identical misses share one fetch. Lookup has a three-second deadline within the
overall five-second TLS setup bound. A lookup that finds all three fetch slots
busy waits its turn in arrival order until that deadline. At most 64 handshakes
wait on lookups at once, counting those that join a lookup already queued or in
flight for the same hostname; a further lookup fails its handshake at once. The
limit is half the default `SLEEPYPODS_PROXY_MAX_HANDSHAKES` of 128, so a flood
of unknown names leaves handshake permits for hostnames whose certificates are
cached, and those handshakes complete without waiting. Eviction passes over a
hostname with a handshake waiting; with at most 64 waiting, a full budget always
holds another to evict. A lookup whose handshakes have all left starts no fetch.
Background refreshes wait for the same slots in their own queue. When both
queues hold work the slots alternate between them, so a burst of unknown names
leaves refreshes running and a wave of refreshes leaves new hostnames resolving.

Positive views refresh after approximately 60 seconds with hostname jitter, or
half the granted lease when that is sooner. The worker queues due refreshes once
per second and starts them as fetch slots free up; leases shorter than a poll or
lookup cannot promise a completed refresh before expiry, and selection still
fails closed at the original deadline.
Each successful authoritative resolution grants at most five minutes of service,
capped by the effective validity of the entire chain and measured conservatively
from the start of the RPC. Failed refreshes never extend that deadline. An
unchanged response can renew only the exact certificate view still held locally.
Authoritative misses are cached for at most one second. During a control-plane
outage, an existing valid view remains usable only until its original deadline.

Frontline opens one native certificate watch when its cache first holds a
certificate. The watch follows the hostnames that hold a certificate, so its
interests fit the 1,024-host limit of one stream. A hostname without a
certificate has nothing to rotate or revoke; its cached miss expires within one
second, and the next handshake resolves it again. The watch pushes complete
binding snapshots containing metadata only. Each registration supplies an ID and
exact hostname list; the server acknowledges it with an atomic current snapshot.
Eviction and removal release the hostname's interest; updates coalesce into a
bounded complete replacement set. Reconnects synchronize all current interests
through either control-plane replica.

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

`SLEEPYPODS_CONTROL_PLANE_POSITIVE_ROUTE_CACHE_TTL_MS` sets how long a proxy
may serve a host's rule set with no word from the control plane: ninety seconds
by default and at most fifteen minutes. Pushed rule sets keep proxy caches
fresh; the TTL bounds a proxy whose stream is gone. A proxy keeps the rule sets
in use across an orderly stream rotation and registers them again on the
replacement stream, which renews each TTL. The control plane therefore refuses
to start unless the TTL exceeds
`SLEEPYPODS_CONTROL_PLANE_SUBSCRIPTION_LIFETIME_MS` by at least ten seconds. A
stream that ends with an error makes the proxy discard its cache instead.

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
during a coordinated key rotation, and the ring must hold every key that seals
an active certificate or startup fails. Sealing keys require authenticated native
TLS. Do not put them in application templates, database rows, logs or source
control. Database backups without the corresponding sealing keys cannot restore
certificate service.

## Common Tasks

Create a workload class version:

```text
CreateWorkloadClassVersion({
  class_id: "web",
  version: 1,
  value_schema: {fields: {"tenant": {required: true},
      "image": {default_value: "registry.example/web:2026-06-24"}},
    allow_extra: false},
  template: {
    workload: {
      kind: WORKLOAD_KIND_DEPLOYMENT,
      name: {parts: [{literal: "web-"}, {instance_value: "tenant"}]},
      app_container: {name: "app", image: {parts: [{instance_value: "image"}]},
        ports: [{name: "http", container_port: 8080}]},
      termination_grace_period_seconds: 30
    },
    sidecar: {name: "sleepypods-sidecar", image: {parts: [{literal: "sidecar:prod"}]},
      listen_port: 15000, app_protocol: APP_PROTOCOL_HTTP1},
    service: {name: {parts: [{literal: "web-"}, {instance_value: "tenant"}]},
      ports: [{name: "http", port: 80, target_port: 8080}]},
    volumes: []
  },
  sleep_policy: {idle_timeout_ms: 300000, idle_retry_backoff_ms: 5000,
    drain_grace_timeout_ms: 30000}
})
```

A class version is immutable. Repeating an identical create returns the stored
version; a different request for the same `class_id` and `version` returns
`AlreadyExists`.

Each schema field is `required`, carries a `default_value`, or is optional. An
instance that omits a field with a default receives the default when it is
created. Templates and exclusivity keys may read only fields that are required
or defaulted, so every instance of the class carries each value its templates
use. Field names starting with `sleepypods.` belong to the platform.

`sidecar.app_protocol` names the protocol the sidecar speaks to the app
container in HTTP mode: `APP_PROTOCOL_HTTP1` (the default) or `APP_PROTOCOL_H2C`
for apps that serve HTTP/2 without TLS, such as gRPC servers. The control plane
passes it to the sidecar as `SLEEPYPODS_APP_PROTOCOL=http1` or `h2c`. A sidecar
in TCP mode forwards bytes and takes no app protocol.

`workload.termination_grace_period_seconds` sets how long Kubernetes waits
between SIGTERM and SIGKILL when the workload's Pod stops. It defaults to 30 and
has a maximum of 300.

Create an instance, with its routes if it has any:

```text
CreateInstance({
  instance_id: "tenant-a",
  workload_class: {class_id: "web", version: 1},
  values: {"tenant": "tenant-a"},
  routes: [{route_binding_id: "route-tenant-a-app",
    identity: {http: {host: {kind: ROUTE_HOST_KIND_EXACT, host: "app.example.com"},
      path_prefix: "/"}}}]
})
```

The instance and its `routes` commit in one transaction, so the instance is
reachable as soon as the call returns.

Create calls are safe to repeat. A `CreateInstance` whose class version, values
and routes equal the stored instance's returns that instance in its current
state, and a `CreateRouteBinding` that names the same instance and identity
returns the stored binding. Any other request for an existing ID returns
`AlreadyExists` and changes nothing. The comparison reads current state: once a
route is added to or removed from an instance, the request that created it
differs from it. After a delete completes, the same request creates the
resource again, and a recreated instance starts above every generation its
predecessor used, so a caller or sidecar holding an old generation is rejected.

### Create-Time Validation

A create call accepts only what a wake can render, so an error reaches the
operator who sent the request and a wake meets no template error.

`CreateWorkloadClassVersion` returns `InvalidArgument` when:

- `class_id` is outside the Kubernetes label-value form: at most 63 characters
  of letters, digits, `-`, `_` and `.`, starting and ending alphanumeric;
- a template or exclusivity key reads a value the schema leaves optional or
  undeclared;
- the manifest fails to render for a trial instance that supplies every declared
  value, for example a missing Service template, a duplicate volume, or a raw
  object that fails to parse or uses a kind outside the supported list.

`CreateInstance` returns `NotFound` when the class version is absent, and
`InvalidArgument` when the instance's own values render an invalid manifest: a
name that is not a DNS label, a StatefulSet or Service name outside the
Kubernetes limits below, a hostPath outside its directory, or an empty
exclusivity key.

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

The suffix is the first sixteen lowercase hexadecimal characters of SHA-256 over
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

Two kinds have tighter Kubernetes limits, and `CreateInstance` enforces both:

- A StatefulSet's final name has at most 52 characters, because Kubernetes
  appends a revision hash to it in a Pod label. That leaves 35 characters for
  the rendered base name.
- A Service's final name starts with a letter.

Typed manifest templates are the preferred path for supported Kubernetes
fields. Operators can use `ManifestTemplate.raw_objects` as an advanced escape
hatch for auxiliary objects and fields the typed templates do not cover. Each
raw object is a YAML or JSON manifest held in
`RawKubernetesManifestTemplate.manifest`, and that text uses only the same
limited `TemplateText` instance-value substitution model as typed fields. It is
not an executable templating engine.

Raw objects are limited in V1 to `v1` `Service`, `Secret`, `PersistentVolume`,
and `PersistentVolumeClaim`. A class runs one workload, declared in
`template.workload`, which carries the sidecar, Pod hardening and automatic
sleep; class creation rejects a raw `Deployment` or `StatefulSet`, and raw Pods
are unsupported. After substitution, the control plane parses the manifest,
validates `apiVersion`, `kind`, `metadata.name`, and namespace scope, injects
the same SleepyPods ownership labels used by typed objects, and derives
rendered refs before any Kubernetes apply. It reads each raw object as its
Kubernetes kind: a field of the wrong type, such as a port given as a string,
fails the render, and a field the kind does not define is dropped. Conflicting
SleepyPods identity labels are rejected. PersistentVolumes must be
cluster-scoped; namespaced raw objects are applied in the materialization
target namespace. Raw refs collide with typed refs, so a raw object cannot
reuse the same apiVersion/kind/namespace/name as a typed-rendered object.

A raw object's `metadata.name` ends with `-` and the instance suffix, so two
instances of one class render distinct objects. The platform supplies the suffix
as the instance value `sleepypods.instance_suffix`; raw manifests read it, and
the schema leaves it undeclared:

```text
raw_objects: [{manifest: {parts: [
  {literal: "apiVersion: v1\nkind: Secret\nmetadata:\n  name: web-config-"},
  {instance_value: "sleepypods.instance_suffix"},
  {literal: "\nstringData:\n  mode: production\n"}]}}]
```

Raw objects do not receive the primary `sleepypods.io/workload-name` label.
That label is reserved: raw metadata labels and selectors may not set it, so
the generated primary Service selects only the structured workload with its
injected sidecar.

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

A custom sidecar image must implement this environment variable and health
endpoint; without them its Pods never turn ready and the wake fails at its
operation deadline.

Add a route or custom domain:

```text
CreateRouteBinding({
  route_binding_id: "route-tenant-a-app",
  instance_id: "tenant-a",
  identity: {http: {host: {kind: ROUTE_HOST_KIND_EXACT, host: "app.example.com"},
    path_prefix: "/"}}
})
```

Use `ROUTE_HOST_KIND_WILDCARD_SUFFIX` for wildcard suffix routing, for example
`host: "*.apps.example.com"`. Use `identity.sni` for TLS/SNI passthrough
routes; the identity kind selects the protocol.

A route host is an ASCII DNS name or IPv4 address without a port or path, at
most 253 characters long. Each dot-separated label has 1 to 63 letters, digits,
hyphens or underscores and starts and ends without a hyphen. The control plane
lowercases the host and rejects any other form with `InvalidArgument`, so every
stored route names a host a request can carry. A path prefix starts with `/`
and is at most 4,096 bytes, the longest path frontline routes.

A host's rule set holds its exact bindings and the wildcard bindings on its
suffixes, and one Subscribe answer carries it. `CreateRouteBinding` and
`CreateInstance` therefore return `InvalidArgument` for a route that would give
any name more than 256 rules, or rules counting over 1 MiB: each rule counts
its route binding ID, instance ID, host and path prefix plus 512 bytes. A
wildcard counts toward every name under its suffix, so one wildcard can fill
many hosts at once. Route creates take turns across the store, so concurrent
creates cannot overfill a name.

Add an HTTP-01 challenge token:

```text
PutHttp01Challenge({
  key: {host: "app.example.com", token: "token-from-acme"},
  key_authorization: "token-from-acme.account-key-thumbprint",
  expires_at_unix_millis: 1782260000000
})
```

After the ACME check finishes, call `DeleteHttp01Challenge`. The frontline stops
serving a challenge at `expires_at_unix_millis`, and control-plane maintenance
removes expired records.

Attach an existing volume:

1. Add required value fields such as `volume_handle`, `mount_path`, or
   `host_path` to the workload class schema.
2. Reference those values from a `VolumeTemplate` `source.csi.volume_handle` or
   `source.host_path.path`, plus `pv_name`, `pvc_name`, `capacity`, access
   modes, reclaim policy, and optional storage class.
   A hostPath template that reads instance values starts with a literal
   absolute directory ending in `/`, such as `/srv/tenants/`, and the rendered
   path has no `..` segment, so a value chooses a subdirectory of that directory.
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

The control plane applies PVs first, then PVCs, Secrets, the Service and the
workload, without waiting for a claim to bind. The scheduler holds the Pod
until its volume is bound, so the backend is published only after binding.
Exclusivity keys are opt-in and
opaque to SleepyPods: the control plane does not parse provider disk IDs or infer
shared singleton resources from template values. A rendered key is acquired
before Kubernetes apply starts and stays held until sleep/delete cleanup
finalizes the materialization.

### Materialization Recovery And Force Operations

Every control-plane replica runs materialization reconciliation. `Pending` and
`Deleting` materializations use durable database leases; process-local ownership
is not authoritative. Another replica takes over after lease expiry and
continues from an inspection of the live objects. Final transitions require the
current owner, acquired attempt, materialization id, target, state and
generation. Readiness and cleanup waits renew their lease against the claimed
work state, and every Kubernetes mutation renews it first. Accepting Delete
during Pending supersedes that work: its driver stops at the next renewal or
mutation, and a definite old failure cannot mark the deletion failed. Cleanup
becomes eligible once that attempt's lease is released or expires; drain grace
remains authoritative. Lifecycle work recovers from
Kubernetes faults, store outages and process loss without an operator. See
[projection safety](projection-safety.md) for conditional mutations,
zero-replica creates and the recovery table.

Non-terminal materializations intentionally keep exclusivity keys held. A
different instance with the same rendered key should receive an exclusivity
conflict until the owning materialization reaches `Deleted` or an operator uses
an explicit force operation.

`ReconcileMaterialization` inspects a materialization. It takes the
`instance_id`, loads that instance's materialization in this control plane's
cluster and namespace, and returns current state, lease metadata, recorded
object refs, live projection observations, next attempt, operation deadline,
failure count, kind and message, and the original terminal wake reason. Set
`status_only=true` to inspect without changing its schedule. Otherwise it also
clears the failure state and schedules eligible `Pending` or `Deleting` work
through the same scheduler; `attempted` means the schedule was
accepted, not that the work completed. Scheduling honors active leases and the
immutable drain deadline. Work retries on its own, so the scheduling half is
needed only for a cleanup that an ownership conflict parked. Recorded-ref
projection observations classify each ref as missing,
owned, unowned, deleting, delete-blocked, or inspect-failed with bounded
reason/finalizer details. Ready-row inspection is metadata-only in V1: it does
not prove EndpointSlice/readiness drift or rendered-hash mutation drift because
the current desired manifest is not reconstructed for recorded refs.

SleepyPods stamps applied Kubernetes objects with `managed-by=sleepypods`,
materialization id, instance id, instance generation, and a deterministic
rendered hash. Missing refs are created during `Pending` and tolerated during
`Deleting`. A Deployment or StatefulSet is created with zero replicas and
scaled up by an update. Owned updates and foreground deletes require the
observed UID/resourceVersion. Cleanup also waits for old Pods and ReplicaSets,
including terminating members; a missing workload object alone is
insufficient.

An `inspect_failed` observation means the control plane could not read that
Kubernetes ref, so cleanup or wake safety is not proven. Treat it as a
retryable Kubernetes/API-access problem unless the bounded reason indicates a
persistent permission or discovery issue.

`ForceDeleteMaterialization` removes a materialization's record, inventory and
exclusivity keys in one audited transaction, without touching Kubernetes. It
takes the `instance_id`, an operator name and an audit reason. Use it when the
cluster the materialization lived in is gone or its objects were removed by
hand. The same transaction leaves the instance where its next wake can start:
Waking becomes Failed, Running becomes Cold, and Draining becomes Cold with its
deferred wake started. The response includes the recorded object refs that
were present before the row was cleared plus best-effort projection
observations for those refs. Before using it on a live cluster, inspect and
remove the objects and their Pods and verify that no external singleton
resource can still be attached by the old workload: the operation releases the
keys without proving absence. Record the evidence in the required audit reason.

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
  a stale revision cannot delete a replacement using the same ID. Deleting a
  Running instance tears its materialization down at once, with no drain grace.
  Deleting a Draining instance keeps the drain deadline its sleep persisted.
  Cleanup retries until it succeeds; until then the instance and its
  reservations stay visible.

Sleep and wake:

- Wake is runtime-driven: a cold route request causes the frontline to call
  `ProxyControlPlane/WakeInstance`.
- A cold request rides out a brief control-plane or Postgres outage. Frontline
  repeats a wake call, or the waiting request's route lookup, that times out,
  fails in transport, or meets an unavailable control plane, 250 ms after the
  failure and doubling to 2 s, and holds the request until the instance runs or
  `SLEEPYPODS_FRONTLINE_ROUTE_TIMEOUT_MS` passes. Requests waiting on a wake
  that fails get 503; the next request starts a new wake.
- Sleep is sidecar-driven: when active work reaches zero for the idle timeout,
  the sidecar drains and reports idle to the control plane.
- A wake that arrives while an instance drains, before its drain deadline,
  returns it to `Running` on the same Pod.
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

The controller claims due work as it is committed. Wake acceptance, sleep
finalization, a failed wake's cleanup, `DeleteInstance`,
`ReconcileMaterialization` and a finished job each signal the driver in the
same process. A scan every second, plus up to 200 ms of per-replica jitter,
finds work another replica committed, retries that came due and drains whose
deadline passed. Wakes and cleanups have separate limits per replica,
`SLEEPYPODS_RECONCILER_WAKE_LIMIT` (default 256) and
`SLEEPYPODS_RECONCILER_CLEANUP_LIMIT` (default 64), each from 1 to 4096; an
invalid value fails startup. Each kind claims the oldest due row of its own
state with `FOR UPDATE SKIP LOCKED`, so a wave of sleeps and a set of
never-ready wakes cannot delay the other kind, and replicas take different
rows. A wake holds its place while it waits for readiness. The controller run
metric measures one claim scan; work and Kubernetes latency have separate
metrics.

Transient failures persist a 2–32 second exponential backoff. A wake's durable
operation deadline is ten minutes by default, configured by
`SLEEPYPODS_OPERATION_TIMEOUT_MS` (1 millisecond to 24 hours). A wake that
cannot succeed, or passes its deadline, publishes Failed and its cleanup starts
at once. The original wake reason survives cleanup retries; a new wake can
start after cleanup finishes. Cleanup has no deadline: one attempt waits up to
60 seconds for the deleted objects and their Pods to disappear, and attempts
repeat until they are absent. Refs and exclusivity stay held until then.

A store outage does not stop the lifecycle driver or maintenance. The driver
retries its scan after 2 seconds, doubling to 15, and reports
`sleepypods_control_plane_store_available` as 0. Work in flight continues while
its 60-second lease lasts; the next claim resumes the rest. Store sessions set
`idle_in_transaction_session_timeout` to the statement timeout and TCP
keepalives (10 seconds idle, 5 seconds between probes, 3 probes), so a frozen
or vanished session releases its row locks.

Every semantic route/lifecycle write records change intent in its transaction,
and the same trigger sends a PostgreSQL notification that is delivered at
commit. Each control-plane process holds one dedicated listening connection and
reads the change history when notified, in batches of 1024; a 250ms poll covers
a missed notification or a listening connection that is reconnecting. The
dispatcher loads its cursor before any listener serves, so a subscription can
never predate it. Each transaction numbers its changes as it commits, under one
shared row lock held only while the commit completes, so changes become
visible in commit order and an open transaction never stalls another writer.
History retains at most 100000 records and maintenance removes an expired
ten-minute prefix.

A proxy subscribes to one exact host at a time and receives that host's complete
rule set. The control plane sends the set again when a binding for the host or
a covering wildcard suffix is added or removed, and when an instance or
materialization behind one of its rules changes. Changes to other hosts and
instances leave a subscription untouched. Under healthy storage a change reaches
subscribed proxies within the notification, one history read and one rule-set
lookup; a backlog requires additional batches. This is not a hard wall-clock
guarantee during storage failure.

Failures stay as narrow as their cause:

- A lookup that fails ends that subscription with an error code and leaves the
  stream and its other subscriptions in place: an invalid host
  (`INVALID_ARGUMENT`), a store error or lookup timeout (`UNAVAILABLE`), or the
  per-stream subscription limit (`RESOURCE_EXHAUSTED`). A proxy at the limit
  releases its oldest hosts and asks again.
- Route creation keeps every rule set within 256 bindings and one 1MiB
  answer. A lookup that still finds a larger set refuses the host with
  `INVALID_ARGUMENT`; requests for that host fail until bindings are removed,
  and other hosts are unaffected.
- A failed read of the change history leaves streams open and the cursor in
  place. The dispatcher reads again on the next notification or poll.
- Lag beyond retained history, a change record the dispatcher cannot read, or a
  stream that falls 4096 events behind ends the affected streams with
  `DATA_LOSS`. A proxy that leaves its response queue full for
  `RESPONSE_TIMEOUT_MS` ends its stream with `DEADLINE_EXCEEDED`. A proxy
  discards its whole cache after either, because a lost change can name any
  cached host, and reconnects after 250ms.
- Reaching `SUBSCRIPTION_LIFETIME_MS` ends a stream in order. The proxy
  reconnects at once, keeps serving the rule sets in use and registers them on
  the replacement stream.
- A failed maintenance pass is retried at the next interval.

Positive cache TTL is ninety seconds by default and negative TTL is one second.
They bound what a proxy serves when its stream is gone.

Native gRPC and optional gRPC-web share finite admission. Defaults and controls:

| Environment variable suffix after `SLEEPYPODS_CONTROL_PLANE_` | Default |
| --- | --- |
| `MAX_CONNECTIONS` | 256 accepted sockets, shared across listeners |
| `MAX_RPCS` | 128 unary handlers/responses including final queued DATA |
| `MAX_SUBSCRIPTION_STREAMS` | 64 owned streams including queued DATA; each frontline replica holds one |
| `MAX_SUBSCRIPTIONS_PER_STREAM` | 2048 host subscriptions, at most 4096 |
| `SETUP_TIMEOUT_MS` / `WRITE_TIMEOUT_MS` | 5000 / 5000 |
| `UNARY_DELIVERY_TIMEOUT_MS` | 5000 from response headers to complete delivery |
| `SUBSCRIPTION_LIFETIME_MS` | 60000, at most 600000; then the proxy reconnects and registers its hosts again |
| `LOOKUP_TIMEOUT_MS` / `RESPONSE_TIMEOUT_MS` | 3000 / 1000 |
| `POSITIVE_ROUTE_CACHE_TTL_MS` | 90000, at most 900000, and at least `SUBSCRIPTION_LIFETIME_MS` plus 10000 |

Admission occurs before connection tasks or protobuf decoding. Each connection
allows 32 H2 streams. The operator and sidecar services cap protobuf requests
at 256 KiB and the proxy service at 512 KiB; responses are capped at 1 MiB. A
subscribed host is at most 253 bytes. Each stream runs 16 rule-set lookups at
once and queues 64 responses; it ends when a response waits for the response
timeout to be queued. Transport capacity may remain held until the subscription
lifetime plus the response timeout; it does not necessarily return within the
producer timeout. Unary delivery uses
a total delivery deadline, not an inactivity timeout. An H2 peer that answers
PING while withholding credit is closed on its delivery/subscription deadline;
public tonic/Hyper cannot reset an already queued final DATA frame individually,
so other streams on that connection are also cancelled. Ordinary active
subscriptions use their explicit lifetime, not the unary delivery deadline.

Maintenance runs every five seconds in bounded batches for HTTP-01 expiry and
event retention. A failed pass is retried at the next interval. Instance
generation watermarks are preserved.

Frontline connects to a ready instance's Pod address before its Service, and
`sleepypods_runtime_pod_route_connects_total` counts each attempt by outcome:
`pod`, `refused`, `timeout`, `claim_rejected`, `error` or `known_failed` (see
the [runbook](operator-runbook.md#metric-names)). Sustained `timeout` or
`error` means frontline cannot reach Pod addresses: each new backend
generation then costs one attempt of up to 250 ms before frontline uses the
Service.

## Lifecycle Expectations

- Cold wake: the API validates and renders a projection without contacting
  Kubernetes, then atomically commits `Waking` and `Pending` work and returns
  `StillWaking`. The reconciler in the accepting process claims the work at
  once; another replica finds it at its next one-second scan. The driver
  creates the objects, scales the workload up from zero replicas, follows the
  Service's EndpointSlices by watch until an endpoint is ready, and publishes
  the Service ClusterIP and the ready endpoint's Pod address as the backend.
  Frontline connects to the Pod address first, so the first request does not
  wait for kube-proxy to program the Service; a frontline that cannot reach Pod
  addresses spends at most 250 ms on one attempt per backend generation and
  then uses the Service. The frontline
  waits within its bounded routing deadline until authoritative updates report a
  ready backend, preserving the first cold request through the asynchronous wake.
  The optional `backend_generation` request is a minimum; acceptance allocates a
  value strictly newer than the prior materialization, including deleted rows.
- Wake during drain: before the drain deadline, the wake returns the instance
  to `Running` on the same Pod and projection generation, with a new instance
  generation and backend generation, and the activation floor restarts from
  that moment. After the deadline, the API durably queues the next incarnation
  while the instance remains `Draining`; finishing cleanup and promoting the
  queued wake are one transaction. Delete cancels the queued wake in its
  acceptance transaction.
- Hot route: the frontline answers from its local route cache. After one lookup
  for a host, every path on that host resolves locally, and measured hot-cache
  hits must not call the control plane.
- Idle sleep and drain: a report from the exact current projection and live Pod
  UID starts a persisted drain deadline. Ordinary and manual reconciliation both
  honor that deadline before deleting recorded objects. A report from an older
  projection cannot authorize sleep of a replacement. The sidecar reports the
  projection generation it was rendered with, which a revive leaves unchanged.
  A report for an instance already draining on that projection answers
  `AlreadyDraining`; a report for a Cold, Waking, Failed or Deleting instance
  answers `Unavailable` with the state. The sidecar keeps reporting while idle
  after `Accepted` or `AlreadyDraining`, so a revived Pod sleeps again. One
  cleanup attempt waits up to 60 seconds for the objects and Pods to
  disappear; a sleep that finishes within it records no failure.
- Delete: operator delete durably accepts generation-checked intent, and
  subscribed proxies receive the changed rule sets. The reconciler removes
  active materialization objects and finalizes deletion; acceptance does not
  mean cleanup has finished. A Running instance is torn down at once; a
  Draining instance keeps its drain deadline.
- Route/domain change: create/delete route bindings through the API. Proxies
  subscribed to an affected host receive its new rule set, so the change takes
  effect without waiting for a TTL. Moving a route by deleting one binding and
  creating another leaves the host without a route between the two commits.
- Restart recovery: durable state is in Postgres. Control-plane restart resumes
  discoverable work and route notifications without a new client RPC. The next
  claim inspects the live objects and continues the wake or the cleanup; see
  [projection safety](projection-safety.md).
- Persistent disk behavior: PV/PVC manifests are active materialization objects.
  Use reclaim policy and provider volume handles deliberately; data continuity
  comes from the external volume, not from keeping Sleeping Kubernetes objects.
  Declare workload-class exclusivity keys for external resources that require
  single-writer or single-attachment behavior across instances.

## Store schema and lifecycle APIs

The store schema is one baseline migration. The control plane applies it to an
empty database at startup and refuses a database an earlier schema created;
start a deployment from an earlier schema on an empty database and recreate
its classes, instances, routes and certificates. Stop every control-plane
writer and settle its in-flight Kubernetes requests before the new controller
starts; mixed old and new lifecycle drivers are unsupported.

`DeleteInstance` requires `expected_generation` and answers `accepted` once
deletion is durable. Poll `GetInstance` for completion instead of repeating the
deletion to drive cleanup.

Per-ID generation watermarks are permanent. Never restore a database without
its generation watermarks while old clients or sidecars continue.

Each reconciler processes only its configured cluster/namespace. Delete marks
all targets as work, but deletion completes only after each target's controller
has proven cleanup. A missing controller keeps that target visible as Deleting;
absence in another cluster cannot release its reservations.

## Installation And Configuration

Run three production components:

- Control plane: one native gRPC listener for the proxy and sidecar services,
  a second for the operator service, and an optional gRPC-Web listener for
  operator unary APIs. All three serve the platform TLS identity when one is
  configured. Workloads reach the first; keep the operator listeners on a
  network path pods have no route to.
- Frontline: always-on HTTP listener; optional TLS termination and TLS/SNI
  passthrough listeners; connects to the control plane.
- Sidecar: injected into materialized workloads; proxies to the local app port
  and reports idle.

The control-plane service account needs `get`, `list`, `watch`, `create`, `patch`,
`update`, and `delete` for namespaced Secrets as well as its managed Services and
workloads. Pod inspection needs `get` and `list`; ReplicaSet inspection needs
`get` and `list` (membership reads and descendant absence scans). EndpointSlices
need `get`, `list` and `watch`: readiness follows them by watch. Retain the
PVC/PV permissions appropriate to the workload.
Secret reads and writes are scoped to the configured target namespace.

Important environment variables:

| Component | Variable |
| --- | --- |
| control plane | `SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR` carries the proxy and sidecar services |
| control plane | `SLEEPYPODS_CONTROL_PLANE_OPERATOR_LISTEN_ADDR` carries the operator service |
| control plane | `SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR` optional |
| control plane | `SLEEPYPODS_OPERATOR_GRPC_WEB_ALLOWED_ORIGINS` optional comma-separated browser origins for the gRPC-Web listener, empty by default |
| control plane | `SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=no-auth` for local tests, or `static-bearer-token` for configured auth |
| control plane | `SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN`, `SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN`, `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled |
| control plane | `SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE`, `SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE` for native platform TLS |
| control plane | `SLEEPYPODS_CONTROL_PLANE_PUBLIC_ENDPOINT` verified HTTPS endpoint for rendered sidecars when native TLS is enabled |
| control plane | `SLEEPYPODS_CERTIFICATE_SEALING_KEYS_FILE` external key ring for certificate storage |
| control plane | `SLEEPYPODS_POSTGRES_URL` |
| control plane | `SLEEPYPODS_CLUSTER_ID`, `SLEEPYPODS_NAMESPACE` |
| control plane | `SLEEPYPODS_RECONCILER_WAKE_LIMIT`, `SLEEPYPODS_RECONCILER_CLEANUP_LIMIT` optional, defaults `256` and `64`: wakes and cleanups one replica drives at once |
| control plane | `SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR` optional Prometheus `/metrics` listener |
| frontline | `SLEEPYPODS_FRONTLINE_LISTEN_ADDR` |
| frontline | `SLEEPYPODS_CONTROL_PLANE_ENDPOINT` |
| frontline | `SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN` when control-plane static auth is enabled |
| control plane / frontline / sidecar | `SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM` optional public trust for a private platform CA; control plane injects it into rendered sidecars |
| frontline | `SLEEPYPODS_ROUTE_CACHE_CAPACITY` optional, default `1024` hosts with routes and as many hosts without; keep it under the control plane's `MAX_SUBSCRIPTIONS_PER_STREAM` |
| frontline | `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS` optional, default `30000` |
| frontline | `SLEEPYPODS_FRONTLINE_ROUTE_TIMEOUT_MS` optional, default `130000`: how long a request waits for a ready route |
| frontline | `SLEEPYPODS_FRONTLINE_WAKE_INSTANCE_TIMEOUT_MS` optional, default `5000`: one `WakeInstance` call |
| frontline / sidecar | `SLEEPYPODS_PROXY_*` admission, pool and timeout limits; see [data-plane resource limits](proxy-protocol-contract.md#data-plane-resource-limits) |
| frontline | `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR` optional |
| frontline | `SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR` optional |
| frontline | `SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR` optional Prometheus `/metrics` listener |
| sidecar | rendered by the control plane: listen address, app port, instance ID, generation, downward-API `SLEEPYPODS_POD_UID`, control-plane endpoint, idle policy, `SLEEPYPODS_SIDECAR_MODE`, `SLEEPYPODS_APP_PROTOCOL` in HTTP mode, and runtime-injected `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled |
| sidecar | `SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR` optional Prometheus `/metrics` listener |

Kubernetes permissions must allow create, conditional merge update, inspection
and foreground delete of rendered Deployments, StatefulSets, Services, Secrets,
PVs and PVCs. Read Services, list and watch EndpointSlices for readiness, and
grant `get`/`list` for Pods and ReplicaSets in the target namespace. Failure to
inspect ownership, retained volume bindings or descendants keeps cleanup and
exclusivity held.
The default field manager is `sleepypods-control-plane`.

Back up Postgres with its generation watermarks. To change control-plane
versions, quiesce old writers and settle their mutating Kubernetes requests
before starting the new driver and regenerated clients; mixed-version rolling
lifecycle drivers are unsupported. Then update frontlines and recreate sidecars
through normal workload materialization. A version with another store schema
starts on an empty database, as
[Store schema and lifecycle APIs](#store-schema-and-lifecycle-apis) describes.

Managed static volumes require `Retain`; provider disk deletion is outside the
lifecycle API. Class creation and rendering reject `Delete`.
Rendered raw PV/PVC inventory must also use explicit Retain and static bindings
within that inventory; dynamic or externally bound claims cannot be automatically
managed. Existing live PVs with Delete/unknown policy, and PVCs whose retained
binding cannot be proven, block the entire cleanup pass before any PVC deletion.
Correct such a live policy only with controllers quiesced and an explicit
storage retention decision; the controller never rewrites the operator's
policy.
The managed-storage contract requires exclusive control over PV/PVC specs,
especially reclaim policies and bindings. Quiesce managed work before external
policy or binding edits: a conditional PVC delete cannot atomically fence an
independent change to its PV after the retention preflight.

## Single-replica automatic sleep

Automatic sleep has a generation-specific minimum Ready age of
`max(190 seconds, resolved idle_timeout)`, in addition to the sidecar's full quiet
interval. The 190-second activation floor protects pending first requests even
when another request finishes quickly. It also applies to an abandoned wake with
no traffic and to successful short wakes; it is not extended by a control-plane
restart. A revive during drain is a new Ready transition and restarts it. An
idle timeout longer than 190 seconds sets the minimum uptime instead. There is
no operator Sleep RPC.

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
belong to that StatefulSet. Unknown membership and extra Pods (including old,
terminating or unready Pods) refuse sleep.

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

An idle report without `pod_uid` receives `FailedPrecondition` and cannot start
sleep. These fail-closed checks keep a workload awake rather than sleep an
unobserved busy peer.

## V1 Limits

- Deployment and StatefulSet replicas must be omitted (defaults to one) or exactly one. Zero and multiple replicas are rejected at class creation, instance creation, and rendering.
- HTTP/3 is deferred.
- Multi-cluster remote forwarding is deferred; V1 materializes into the
  configured target cluster/namespace.
- Rich route predicates beyond host/SNI and optional HTTP path prefix are
  deferred.
- Generated examples are illustrative request shapes until a CLI exists.
- Prometheus metrics are intentionally limited to low-cardinality runtime,
  reconciler, materialization backlog, held-key, and Kubernetes
  controller-operation metrics. Use Kubernetes/container telemetry for CPU,
  memory, restarts, pod scheduling, network, and filesystem metrics.
