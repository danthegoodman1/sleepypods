# Postgres store contract

Route resolution takes an exact request identity and a materialization target.
One SQL statement selects indexed exact-host and wildcard-suffix candidates,
ranks host specificity before literal path-prefix length, and joins the instance
and the Ready backend for that target in one PostgreSQL snapshot. An uncommitted
concurrent deletion leaves the old complete snapshot visible; once the deletion
commits, resolution returns a miss. This is snapshot consistency, not a promise
that a returned backend stays live after the query completes. Proxy subscription
invalidation provides the subsequent-change contract.

Kubernetes object reservations are unique by cluster, API group, kind, namespace
and name. API versions of one group share the reservation. PersistentVolume
names are cluster scoped regardless of an incidental namespace field. Logical
exclusivity keys remain scoped to cluster, target namespace, key name and value.
Database triggers maintain the normalized reservation tables atomically whenever
materialization state, refs or keys change; deleted materializations and physical
cascading deletes release their reservations. Lease-only changes skip the work.
The lifecycle driver remains responsible for proving cleanup before it commits
Deleted or clears refs/keys. Reservation constraints do not establish Kubernetes
absence on their own. Migration backfill rejects conflicting existing owners
and rolls back rather than selecting one arbitrarily.

## Clock ownership

The store owns every timestamp it compares. Callers pass durations, never
instants: a reconciliation lease is requested as a TTL, and the store starts it
from the same clock that later decides whether the lease has lapsed. Backlog age
and work eligibility are computed the same way. One clock supplies both operands
of every comparison, so a caller whose clock runs fast cannot lengthen its own
lease and a slow one cannot shorten it.

Exclusivity does not rest on any clock. Two clock-free mechanisms carry it: the
fencing tokens revalidated on every write (`reconcile_owner`, `reconcile_attempt`
and `instance_generation`), and the unresolved-effect barrier, which blocks
claims, candidate scans and requeues while a Kubernetes call is in flight. The
lease clock only decides when idle work may be reclaimed from an owner presumed
dead, so clock skew costs a duplicated reconcile pass or a delayed recovery, not
a duplicated effect.

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

For records created before migration 7, exact historical state age cannot be
reconstructed from the old overloaded `updated_at` field. Backfill preserves
its drain deadline and queue position, and caps a future state-age value at the
migration time. New records do not have this ambiguity. A lease conflict is a
nonretryable ownership error, distinct from a temporary database outage.

Startup migrations run inside one transaction protected by a transaction-scoped
Postgres advisory lock. Concurrent processes serialize before metadata/schema
creation. Errors, cancellation and disconnect roll back and release the lock;
a pooled connection cannot retain a manually opened migration transaction.

## Pool and operation budgets

| Environment setting | Default | Meaning |
| --- | --- | --- |
| `SLEEPYPODS_POSTGRES_MAX_CONNECTIONS` | 16 | Maximum connections per store pool |
| `SLEEPYPODS_POSTGRES_POOL_WAIT_TIMEOUT_MS` | 5000 | Maximum checkout queue wait |
| `SLEEPYPODS_POSTGRES_CONNECTION_TIMEOUT_MS` | 5000 | Connection creation and recycling budget |
| `SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS` | 30000 | Server statement deadline, including lock waits |
| `SLEEPYPODS_POSTGRES_IDEMPOTENCY_RETENTION_MS` | unset | Optional replay lifetime for newly created keys |

Pool capacity must be 1–1,024. Timeouts must be whole milliseconds between 1ms
and 24 hours. Optional idempotency retention must be whole milliseconds between
1ms and 100 years (365 days per year); this bound also keeps expiry timestamp
addition within Postgres integer limits. Environment values are positive integers.
The same limits apply to `PostgresStoreConfig`, including rejecting fractional
milliseconds, and are checked before pool allocation or connection attempts.
A one-connection pool is supported;
workload class creation reuses its held client. Pool/query budgets bound an
individual attempt; a retry wrapper can perform additional attempts according
to its retry policy.

## Required capabilities and automatic retries

`ControlPlaneStore` requires every persistence capability at compile time.
PostgreSQL and `RetryingControlPlaneStore` implement every method explicitly;
production implementations cannot inherit runtime "unsupported" behavior.

An `Unavailable` response can follow a committed write. The retry wrapper only
replays operations with a stable predicate, immutable value or exact ownership
fence. Reads, immutable workload-class creation, generation-checked lifecycle
transitions, exact lease/effect/failure operations and predicate-based maintenance
retain bounded retries. Maintenance counts describe the successful final attempt,
not an exact total across an uncertain earlier commit; every attempt retains its
batch limit.

The following mutations run once and return uncertain errors to their caller:

- Instance/route creation: a configured replay record can expire during a retry.
- Legacy instance deletion, route deletion, challenge put/delete and operator
  force deletion/key release: a reused name or key can refer to a replacement.
- Materialization recording: the upsert can overwrite newer same-generation
  state and clear its lease.
- Reconciliation claim and admin enqueue: neither request carries an exact
  incarnation/attempt fence for replay after another worker advances the row.
- Beginning a Kubernetes effect: an uncertain durable begin must not authorize
  another dispatch. Exact effect acknowledgement remains safe to retry.
- Certificate publication/removal, hostname binding changes and key
  re-encryption: an uncertain response requires reading current metadata and
  deciding whether another conditional mutation is appropriate.

Callers must inspect current state before deciding how to resolve these uncertain
outcomes. A one-shot mutation can consequently report `Unavailable` after it
succeeded; the wrapper does not convert uncertainty into a second mutation.
The three API transport suites share an explicitly limited test fixture. The
same lifecycle assertions also run against PostgreSQL, alongside actual
commit/response-loss/replacement tests. The fixture does not model database
transactions, retention or distributed effect ownership.

## Idempotency lifetime and deleted resources

By default, keys are retained for the lifetime of the database. A matching replay
returns the current live resource, preserving the existing behavior; it does not
recreate removed routes or restore the original creation-time resource snapshot.
A different request under the same key is an idempotency conflict. Once the
created resource is deleted, the key becomes a durable tombstone and replay
returns `IdempotencyResourceDeleted` (`FailedPrecondition` on the API). This also
prevents a matching old key from returning a newly created resource with the
same resource ID. Bundled routes are part of the instance creation request;
deleting only one bundled route does not delete the instance's replay record.

Setting a finite retention value explicitly opts new keys into a shorter
window. Expiry is fixed at the first successful creation; replay and later
configuration changes do not extend or shorten an existing key's lifetime.
Legacy keys remain permanent. After expiry, the key may be reused, including for
a different request. Resource-ID/route uniqueness is enforced independently and
can still reject the new creation. This retention setting is therefore an API
contract change that callers must account for when deciding how long to retry.

`PostgresStore::expire_idempotency_records(limit)` deletes at most 1–10,000 expired
records per call, skipping locked records and preserving permanent and unexpired
keys, including tombstones. A create also expires its own requested key before
attempting reuse. Supervised runtime maintenance collects bounded batches every
five seconds; expiry correctness does not depend on that schedule. If collection
wins a race with an expiring replay, the store can return `Unavailable`; the
caller must account for the new key lifetime before choosing to submit again.

Idempotency tombstones alone do not prohibit reusing an instance ID under a new
key. Lifecycle incarnation fencing must independently reject stale sidecar and
worker observations across such a replacement.

Validation and measurements: [Phase 5 evidence](review-evidence/phase5/README.md).

## Durable lifecycle transactions (migration 8)

`accept_wake` commits the next Waking revision and complete Pending projection
(refs, reservations, target, immutable projection stamp, backend freshness) in
one transaction. Requests arriving during Draining persist a deferred intent;
`finalize_sleep` releases the old refs and promotes that intent in the same
transaction. Cancellation after any successful acceptance therefore leaves
work the next reconciler can discover. A prior Failed/Cold active projection
must finish cleanup before a new wake can replace its recorded inventory.

`request_instance_deletion` checks the caller's explicit CAS revision, sets
Deleting, cancels any deferred wake, and marks every nondeleted target Deleting
atomically. It preserves the drain deadline and existing worker lease. A
bounded finalization scan removes only Deleting instances with no unresolved
materializations, including instances that never had a materialization. The
low-level hard-delete method enforces the same cleanup prerequisite. Candidate
scans are target-scoped in production, and a direct reconciliation cannot claim
another cluster's projection.

`projection_generation` stays fixed from Pending through Ready and Deleting;
instance `generation` remains the CAS revision. Migration backfills the actual
legacy projected stamp and converts orphaned old Waking states to explicit
Failed outcomes. `instance_generation_watermarks` retain per-ID revisions
permanently; known pre-upgrade deleted IDs have no trustworthy revision and are
retired. Neither key expiry nor resource deletion removes these watermarks.
See the operator upgrade instructions for incompatible client and erased-history
constraints. Optional wake `backend_generation` is a floor: the transaction
allocates at least that value and strictly above the prior stored value.

Validation: [Phase 6A evidence](review-evidence/phase6a/README.md).

## Lease and Kubernetes effect fencing

Migration 9 introduces `materialization_effects`, a single outstanding mutation
barrier per materialization. Claims, begin and ACK lock the materialization row
before taking a fresh statement snapshot. Lease renew/release and exact effect
ACK include the materialization generation as well as owner/attempt, preventing
aliases after instance ID reuse. ACK additionally requires the individual effect
ID. Unresolved barriers block claims and automatic completion/reservation release;
an inspected absent name cannot clear an ambiguous create. Read/wait failures do
not create barriers. The audited force-delete operation is the explicit override.
See [projection safety](projection-safety.md) for retry semantics, bounded steps,
coordinated upgrade and the documented liveness limitation.


## Runtime work and ordered notifications (migration 10)

Migration 10 adds durable next-attempt/failure/deadline diagnostics and targeted
route/lifecycle event history. Statement triggers record bundled route creation,
state transitions, completion/failure and deletion without post-commit dependency
lookups. Lease-only changes do not produce route events. Each statement reserves
one contiguous revision range by updating a singleton row whose lock is held
until commit. PostgreSQL sequences alone are insufficient because allocation is
not commit order. Concurrent readers only advance through committed ranges.

All CP processes read independently; no single consumer deletes acknowledged
records. Reads use one snapshot for revision, retained prefix and event page.
Hard retention is 100000 records; ten-minute expiry removes only a revision prefix
so clock skew cannot hide interior gaps. A retention gap causes a global reset;
normal route/instance events preserve unrelated cached dependencies. Each process
holds one scalar cursor and reads at most 1024 records per batch.

Failure publication locks and checks the acquired generation/owner/attempt and
fresh lease time. Permanent/deadline failed wakes become discoverable cleanup;
failed cleanup and unresolved effects keep inventory/exclusivity. Admin enqueue
uses the same state/barrier/grace rules. The operation deadline is persisted at
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

## Certificate resources and ordered changes (migration 11)

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

Certificate writes are one-shot through the retry wrapper. Callers inspect
metadata after an uncertain outcome before choosing a new conditional write.
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
