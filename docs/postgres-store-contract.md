# Postgres store contract

Route resolution takes a listener family, an exact host and a materialization
target, and returns the host's complete rule set. One SQL statement selects the
indexed exact-host bindings and every wildcard-suffix binding that covers the
host, ranks host specificity before literal path-prefix length, and joins each
binding's instance and the Ready backend for that target in one PostgreSQL
snapshot. A proxy routes a request to the first rule whose path prefix covers
the request path; `HostRules` in `sleepypods-api` holds that ranking for both
sides, and a property test checks it against Postgres for random binding sets.
A host with more than 256 applicable bindings is refused. Route creation keeps
every name within that count and a 1 MiB budget of rule bytes: it checks the
rule sets a new binding joins after its insert, while it holds a
transaction-scoped advisory lock on route bindings, so concurrent creates take
turns and other writers do not wait for them. An uncommitted concurrent
deletion leaves the old complete snapshot visible; once the deletion commits,
the rule set is empty. This is snapshot consistency, with no promise
that a returned backend stays live after the query completes. The rule sets the
control plane pushes to subscribed proxies carry every later change.

Every write that can change a rule set records its changes in the route-change
outbox as its transaction commits and calls `pg_notify` on a channel named for
the store's schema. PostgreSQL delivers the notification at commit, so a
rolled-back write wakes nobody. Each control-plane process holds one dedicated listening
connection beside its pool and reads the outbox when notified; the outbox, read
by revision, stays the record of what changed. `LISTEN` needs a session
connection: behind a transaction-pooling proxy the listener hears nothing, and
the 250ms outbox poll alone carries delivery.

Kubernetes object reservations are unique by cluster, API group, kind, namespace
and name. API versions of one group share the reservation. PersistentVolume
names are cluster scoped regardless of an incidental namespace field. Logical
exclusivity keys remain scoped to cluster, target namespace, key name and value.
Database triggers maintain the normalized reservation tables atomically whenever
materialization state, refs or keys change; deleted materializations and physical
cascading deletes release their reservations. Lease-only changes skip the work.
The lifecycle driver remains responsible for proving cleanup before it commits
Deleted or clears refs/keys. Reservation constraints do not establish Kubernetes
absence on their own.

## Clock ownership

The store owns every timestamp it compares. Callers pass durations, never
instants: a reconciliation lease is requested as a TTL, and the store starts it
from the same clock that later decides whether the lease has lapsed. Backlog age
and work eligibility are computed the same way. One clock supplies both operands
of every comparison, so a caller whose clock runs fast cannot lengthen its own
lease and a slow one cannot shorten it.

Exclusivity does not rest on any clock. The fencing tokens every write
revalidates carry it: `reconcile_owner`, `reconcile_attempt` and
`instance_generation`. The lease clock only decides when work may be reclaimed
from an owner presumed dead, so clock skew costs a duplicated reconcile pass or
a delayed recovery. [Projection safety](projection-safety.md) describes how a
pass treats objects an earlier pass left.

A store backend without a server clock may designate its own reference; the
contract requires a single monotonic reference per comparison, not this
implementation's `clock_timestamp()`. Lease TTLs must be at least one
millisecond and no longer than `MAX_RECONCILIATION_LEASE_TTL` (24 hours).

Because the store owns the clock, tests elapse a deadline by retiring the stored
timestamp rather than by claiming a later "now".

Reconciliation stores three separate timestamps:

- `state_entered_at_unix_millis` changes only with the materialization state. It
  drives backlog age and is unaffected by lease claims, renewals or releases.
- `next_attempt_at_unix_millis` orders eligible work. Releasing a lease returns
  work to the queue at the current time; later retry policy can delay it.
- `drain_not_before_unix_millis` records the sleep grace deadline independently
  of queue position. Both candidate scans and direct claims enforce it.

A lease conflict is a nonretryable ownership error, distinct from a temporary
database outage.

The schema is one baseline migration,
`crates/control-plane/migrations/0001_baseline.sql`. Startup applies it inside
one transaction protected by a transaction-scoped Postgres advisory lock, so
concurrent processes serialize, and errors, cancellation and disconnect roll
back and release the lock. A database whose recorded migration 1 has another
name fails startup: the store reads no earlier schema, so a deployment from an
earlier schema starts on an empty database.

## Pool and operation budgets

| Environment setting | Default | Meaning |
| --- | --- | --- |
| `SLEEPYPODS_POSTGRES_MAX_CONNECTIONS` | 16 | Maximum connections per store pool |
| `SLEEPYPODS_POSTGRES_POOL_WAIT_TIMEOUT_MS` | 5000 | Maximum checkout queue wait |
| `SLEEPYPODS_POSTGRES_CONNECTION_TIMEOUT_MS` | 5000 | Connection creation and recycling budget |
| `SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS` | 30000 | Server statement deadline, including lock waits |

Pool capacity must be 1–1,024. Timeouts must be whole milliseconds between 1ms
and 24 hours. Environment values are positive integers.
The same limits apply to `PostgresStoreConfig`, including rejecting fractional
milliseconds, and are checked before pool allocation or connection attempts.
A one-connection pool is supported;
workload class creation reuses its held client. The route-change listener's
connection is separate from the pool: allow one more connection per
control-plane process. It reconnects one second after a failure, and the 250ms
outbox poll covers the gap. Pool and query budgets bound one attempt. An
operation that is safe to repeat makes up to four attempts while the database
is unavailable, backing off from 100ms to 2s.

## Statement cost

Every statement runs through `postgres/sql.rs`, which prepares it once per
pooled session, so a later run costs one round trip and no planning. The
process caches immutable workload class versions, up to 1,024 of them. A
lifecycle write carries its guard in its own `WHERE` clause, so no read comes
before it that the write repeats. `begin_sleep` checks the Ready age in its
write, so an idle report reads no work status first.

`round_trips.rs` counts round trips at a relay in front of the database once a
session has served each operation, and holds each to half its earlier count:

| Operation | Round trips | Budget |
| --- | --- | --- |
| Proxy wake of a Cold instance | 5 | 8 |
| Accepted idle report | 6 | 10 |
| Lease check around one Kubernetes mutation | 1 | 7 |

`store_throughput.rs` drives wake, completion, sleep and cleanup cycles
through a relay that delays every round trip, counting four instance
transitions per cycle. Transitions per second on a developer workstation, ten
seconds per point:

| Store | 1 writer, 0.5ms | 8 writers, 0.5ms | 1 writer, 2ms | 8 writers, 2ms |
| --- | --- | --- | --- | --- |
| Revision numbered per statement | 69 | 170 | 28 | 83 |
| Revision numbered at commit | 68 | 332 | 27 | 214 |
| Prepared statements and folded guards | 142 | 354 | 67 | 333 |

Numbering at commit lets concurrent writers overlap; prepared statements and
folded guards shorten each writer's cycle.

## Store roles and automatic retries

`store.rs` defines one trait per consumer. `LifecycleReads` holds the instance,
class and projection reads several consumers share. `WakeStore` serves a
proxy's wake, `SleepStore` a sidecar's idle report, `ReconcilerStore` the
lifecycle driver, `OperatorStore` the operator service, `RouteStore` route and
HTTP-01 resolution, `CertificateStore` certificate administration and
resolution, `TlsWatchStore` the certificate watch and `RuntimeStore` the
dispatcher, maintenance and metrics. The services take the composites they
need (`ProxyServiceStore`, `OperatorServiceStore`, `WorkloadServiceStore`), and
`ControlPlaneStore` gathers every role. A store implements each operation of
every role it takes on, so a missing operation fails to compile.

An `Unavailable` answer can follow a committed write, so `PostgresStore`
repeats an operation only when a repeat is safe: reads, immutable class
creation, generation-checked lifecycle transitions, operations fenced by an
exact lease owner and attempt, and predicate-based maintenance. Maintenance
counts describe the final attempt, not a total across an uncertain earlier
commit; every attempt keeps its batch limit.

These operations run once and report an uncertain outcome to their caller:

- Instance and route creation: a repeat after a concurrent delete would create
  the resource again.
- Route deletion, HTTP-01 challenge put and delete, and the operator's force
  deletion and key release: the name or key can refer to a replacement.
- Reconciliation claim and admin enqueue: neither carries a fence that would
  stop a repeat after another worker advances the row.
- Certificate publication and removal, hostname binding changes and
  re-encryption: the caller reads current metadata and decides whether another
  conditional write is appropriate.

`replay_policy.rs` in the Postgres suite pins this list. A relay in front of
the database loses the answer to each operation's first request: an operation
that repeats answers from its second attempt, and one that runs once reports
`Unavailable`. The replacement tests in `retry_boundaries.rs` lose the answer to
a commit after a second writer has replaced what it touched, and check that
the replacement survives.

The three API transport suites share an explicitly limited test fixture. The
same lifecycle assertions also run against PostgreSQL. The fixture does not
model database transactions, retention or distributed ownership.

## Repeated creates

`create_instance` and `create_route_binding` insert the resource under its
caller-chosen ID and keep no record of the request. When the ID is taken, the
store compares the request with the stored resource:

- An instance matches when its class version, its values after schema defaults
  and its current route bindings (IDs and identities) equal the request's. A
  route binding matches when it names the same instance and identity.
- A matching request returns the stored resource in its current state,
  including an instance whose deletion is pending.
- Any other request returns `AlreadyExists` and changes nothing.

The comparison reads current state from one snapshot. Once a route is added to
or removed from an instance, the request that created the instance differs
from it.

A create that follows a completed delete creates the resource again. The
instance generation watermark starts the new incarnation above every generation
the deleted one used, so stale sidecar and worker observations and stale
`expected_generation` values are rejected. The store therefore sends a create
once: a delete that lands after a lost response stays in effect unless the
caller repeats the create.

A repeat that arrives while the first create is uncommitted waits for it and
then compares with what it stored. A refused or failed create leaves no row and
leaves the generation watermark where it was.

Validation and measurements: [Phase 5 evidence](review-evidence/phase5/README.md).

## Durable lifecycle transitions

Each transition is a conditional `UPDATE` or `INSERT` whose `WHERE` clause
holds its guard: the expected state and generation, and for a completion the
lease owner, attempt and expiry. Under READ COMMITTED, PostgreSQL evaluates
those same-row predicates again after a lock wait, so a write that queued
behind another applies only if its guard still holds. A refused write costs one
more read, which names the error.

`accept_wake` commits the next Waking revision and the complete Pending
projection (refs, reservations, target, projection stamp, backend freshness)
in one transaction. A wake that arrives during Draining persists a deferred
intent; `finalize_sleep_reconciliation` releases the old refs and promotes that
intent in the same transaction. Cancellation after any successful acceptance
therefore leaves work the next reconciler can discover. A prior Failed or Cold
active projection must finish cleanup before a new wake can replace its
recorded inventory.

A wake or sleep transition locks the instance row before its materialization
row. A cleanup completion updates its fenced materialization alone. Neither
order can deadlock against a deletion request, which locks the instance first.

`request_instance_deletion` checks the caller's explicit CAS revision, sets
Deleting, cancels any deferred wake, and marks every nondeleted target Deleting
atomically. It preserves the drain deadline and existing worker lease. A
bounded finalization scan removes only Deleting instances with no unresolved
materializations, including instances that never had a materialization.
Candidate scans are target-scoped in production, and a direct reconciliation
cannot claim another cluster's projection.

`projection_generation` stays fixed from Pending through Ready and Deleting;
instance `generation` remains the CAS revision.
`instance_generation_watermarks` retain per-ID revisions permanently; neither
key expiry nor resource deletion removes them. The optional wake
`backend_generation` is a floor: the transaction allocates at least that value
and strictly above the prior stored value.

Validation: [Phase 6A evidence](review-evidence/phase6a/README.md).

## Lease fencing

Lease renewal and release, every completion and failure publication carry the
owner, the attempt and the materialization's instance generation, so a write
from an earlier incarnation of a reused instance ID changes nothing. The
audited force-delete operation is the explicit override. See
[projection safety](projection-safety.md) for retry semantics, bounded steps
and the documented liveness limitation.

## Runtime work and ordered notifications

The store keeps durable next-attempt, failure and deadline diagnostics and a
targeted route/lifecycle change history. Statement triggers collect bundled
route creation, state transitions, completion, failure and deletion into one
pending row per transaction. Lease-only changes produce no route events. A
deferred constraint trigger runs once as the transaction commits: it takes the
singleton revision row's lock, numbers the transaction's changes, writes them to
the outbox and sends the notification. PostgreSQL makes a commit visible before
it releases the commit's locks, so revisions become visible in commit order and
a reader caught up to a cursor never misses a later revision. The lock is held
only from the numbering to the end of the commit, so an open lifecycle
transaction never stalls another writer; synchronous replication lengthens that
window by the standby's acknowledgement. PostgreSQL sequences alone are
insufficient because allocation is not commit order.

All CP processes read independently; no single consumer deletes acknowledged
records. Reads use one snapshot for revision, retained prefix and event page.
Hard retention is 100000 records; ten-minute expiry removes only a revision prefix
so clock skew cannot hide interior gaps. A retention gap causes a global reset;
normal route/instance events preserve unrelated cached dependencies. Each process
holds one scalar cursor and reads at most 1024 records per batch.

Failure publication locks and checks the acquired generation/owner/attempt and
fresh lease time. Permanent/deadline failed wakes become discoverable cleanup;
failed cleanup keeps inventory and exclusivity. Admin enqueue uses the same
state and grace rules. The operation deadline is persisted at
phase entry using the connection's validated `SLEEPYPODS_OPERATION_TIMEOUT_MS`
(default 600000); restarting or renewing a lease does not extend it.

Status reads sample the remaining operation duration with the database clock.
The worker deducts monotonic elapsed time from the start of that status request,
including pool wait, retries and response delivery. It does not subtract a
process wall clock from a database timestamp. This can cancel work conservatively
early; local cancellation may remain a transient failure until database time
reaches the persisted deadline. Failure publication compares the deadline and
lease against fresh database time after acquiring its row locks, so a lock wait
cannot preserve an obsolete pre-deadline classification.

## Certificate resources and ordered changes

Application certificates and exact hostname TLS bindings are separate from
routes and lifecycle state. Publication accepts a bounded, leaf-first DER chain
and PKCS#8 private key. Validation checks current chain validity, TLS server-auth
usage, matching key, ordered signatures and standards-based DNS SAN coverage.
The supplied terminal certificate is a private trust anchor; publication does
not require a public CA. Usable validity is the intersection of the validated
chain's intervals. Exact binding names use canonical ASCII DNS/A-label syntax;
wildcard certificates may cover explicitly bound names, but wildcard binding
lookup is unsupported.

All certificate-domain mutations first acquire the
`tls_certificate_revision` singleton row lock, then read or change resource
rows. The lock remains held until commit or rollback. This deliberately
serializes infrequent certificate administration, closes rotation-versus-binding
SAN races, and establishes commit-ordered change visibility independently of
route/lifecycle transactions. Resolve reads binding, certificate and observation
time in one joined statement. Subsequent changes are handled by the certificate
notification/freshness contract; a snapshot cannot promise future authorization.

Certificate versions are conditional mutation fences. Version zero creates a
never-used ID; removal permanently retires the ID and atomically erases its
stored chain and private-key envelope. Hostname rows and their revisions survive
unbinding. Rotation, removal and rebinding advance affected hostname revisions,
so a lower certificate version on a different resource cannot be mistaken for an
older view. Rotation validates every current binding before changing anything.
At most 1,024 hostnames may reference one certificate, bounding validation and
binding updates. A failed mutation rolls back its revision changes too.

Each binding also retains a `last_invalidating_revision`. Binding mutations
(including same-ID binding) and removal advance it atomically with the view
revision; ordinary publication/rotation preserves it. It records the most recent
destructive transition even when several mutations occur between watch polls.
A proxy compares it against its retained configuration's actual view revision,
not its possibly newer notification floor.

`snapshot_tls_bindings` reads up to 1,024 requested bindings and a private global
revision in one SQL snapshot, without loading or decrypting bundles. The optional
known global revision skips the scoped hostname query only on equality; a forced
registration always returns a complete snapshot. Native watches periodically
perform this conditional read and push changed snapshots. The global revision
stays on the server as a read optimization; per-host revisions alone fence views.
There is no certificate event outbox, history retention, pagination or reset path.
Snapshots and operator metadata contain no private key material.

Private keys use a versioned AES-256-GCM envelope. Associated data includes the
certificate ID/version, sealing key ID and a digest of the complete chain.
Deployment-provided active/read keys remain outside PostgreSQL; there is no
default sealing key. Re-encryption handles one certificate per call and requires
both its logical version and sealing revision. It changes only the envelope and
sealing revision, preserving the logical certificate and hostname view. Old
sealing keys remain read-capable while every control-plane writer is rolled to
the new active key ID. Only then re-encrypt/check live records; an old-configured
writer could otherwise publish new material under the old key after that check.
Settle in-flight or ambiguous database writes from old-key writers before the
final check and key retirement; a disconnected caller or stopped transport alone
does not rule out a queued commit completing later.
Keep each old key available for as long as required live records or retained
backups need it. The store's row CAS does not coordinate deployment keyring
configuration. An unavailable key or failed authentication is an error, never
an authoritative certificate miss.

Certificate writes run once. Callers inspect metadata after an uncertain outcome
before choosing a new conditional write.
Certificate metadata, bindings, resolution and snapshot reads retain bounded
read retries. Certificate operations have their own local admission limit of
`min(4, pool_capacity - 1)` and two blocking-crypto slots, preserving an ordinary
pool slot. Sealing-enabled runtime configuration requires at least two pool
connections. Cancellation retains the operation permit during blocking work and
while a checked-out session drains sent SQL and queued rollback. The drain has a
five-second local timeout; only a successful protocol round trip returns the
session to the pool. Failure, timeout or task cancellation discards its transport.
This bounds admitted local work and session reuse; remote statement termination
still depends on PostgreSQL and its configured statement timeout.

Certificate snapshot reads have an additional one-slot watch limit per
store. Admission allows at most 17 active or queued calls: 16 native producer
calls plus one previous logical read whose detached protocol drain still owns
the watch slot. A producer can queue its next poll before that drain finishes.
The 17-call bound includes that overlap without admitting another concurrent
watch SQL operation.

Admitted calls wait in Tokio's FIFO semaphore under a three-second bound, then
acquire ordinary certificate and database capacity. Queued calls own no ordinary
operation permit or database connection; cancellation removes their queued
ownership. The producer's fixed setup/query deadline still bounds the whole
operation and is not renewed by admission or retries. The active call retains
its admission, watch and operation permits together through connection checkout,
SQL, protocol drain and discard. Canceling it cannot release capacity while
SQL/session cleanup remains outstanding. Ordinary certificate operations retain
the other default slots; the two-connection minimum still preserves one ordinary
route/lifecycle connection.

The [dynamic certificate plan](dynamic-certificates-plan.md) tracks
the implementation and actual database validation evidence.
