# Dynamic Certificate Delivery Plan

## Goal and scope

Frontline terminates application TLS using certificates published to the control
plane, fetched on demand, and refreshed without restarting proxies or waking
applications. PostgreSQL stores encrypted private keys. Frontline uses a bounded
in-memory cache with no application certificate files, startup loader, disk cache
or static fallback. No backwards compatibility is required.

Certificates and exact hostname bindings are independent of routes and instance
lifecycle. Issuance tooling publishes already-issued bundles. An ACME client,
DNS-01, wildcard binding lookup, external KMS and tenant authorization are outside
this feature. Wildcard SAN certificates may cover explicitly bound hostnames.
Platform TLS identity and trust for the control plane are provisioned separately.

The feature is implemented on `dynamic-certificates`, based on merged main
`456d37e`. Certificate watches push complete current snapshots instead of
replaying a retained event log. Phase 7 records the simplified implementation and
its validation. Publication and exact-commit CI status are tracked in
[PR #2](https://github.com/danthegoodman1/sleepypods/pull/2). Earlier evidence applies
only to its recorded source, images and workload.

## Implementation principles

- Keep certificate material out of routes, notifications and operator metadata.
  Operator credentials manage resources; proxy credentials resolve them over
  authenticated native TLS; sidecar credentials cannot read material.
- Validate before publication and installation: encoding, key/chain match,
  validity, server authentication, hostname coverage and resource limits.
  Invalid publication preserves the active version. Privately issued certificates
  are acceptable; public CA trust is not a publication requirement.
- Use conditional writes, permanent certificate identity retirement, hostname
  tombstones and atomic binding/material resolution. An ambiguous write is not
  blindly retried. Preserve commit ordering under concurrent writers.
- Seal private keys using a versioned authenticated envelope tied to certificate
  identity, version and chain. Key material stays outside the database. Bounded
  conditional re-encryption preserves certificate and hostname view revisions.
- Keep parsing, cryptography and network calls off warm handshakes. Reuse the
  existing TLS configuration replacement, admission and deadline mechanisms.
- Bound entries, fetches, decoded messages, queues and server work. A
  canceled operation retains capacity until blocking work or SQL cleanup ends.
  Preserve capacity for ordinary routing and lifecycle requests.
- Use one metadata-only certificate watch response containing a registration ID
  and complete binding snapshot. Keep its global revision private to the server.
  There is no certificate event outbox, history pagination or reset protocol.
- Retain complexity only where a tested guarantee needs it. Do not add generic
  secret infrastructure, compatibility paths or speculative configuration.

## Delivery and failure contract

| Operation | Required behavior |
| --- | --- |
| Publish or rotate | Validate and atomically commit material, metadata, version and affected hostname revisions. Return metadata only. |
| Bind, rebind or unbind | Conditional atomic mutation advances both the view revision and last-invalidating revision. Preserve the hostname row after unbinding. |
| Remove | Retire the certificate ID, erase stored material, and advance both revisions for every referencing hostname atomically. |
| Resolve | Return coherent `Found`, `Unchanged` or `Missing`; errors are not authoritative misses. Only `Found` carries material. |
| Watch registration | Acknowledge the exact hostname interest set with an atomic complete snapshot. Registration carries no resume cursor. |
| Subsequent push | Check the private global revision; send a complete snapshot when it changes. Per-host revisions alone fence local views. |
| Ordinary rotation | Refresh promptly; a valid retained configuration can serve only within its original authorization lease. |
| Destructive transition | If its last-invalidating revision exceeds the retained configuration's actual view revision, discard that configuration before another handshake. Coalesced unbind/rebind/rotation cannot hide invalidation. |
| Stale response | Per-host floors, fetch generations and cache incarnations prevent stale `Found` or `Unchanged` from restoring permission. Validate the whole snapshot before mutation. |
| Outage, reconnect or notification | Never extend a lease. An exact successful authoritative resolution is required to renew permission. |
| Established connection | Continue under the existing connection/drain policy. Removal does not terminate established sessions. |
| Restart | Start empty and require successful resolution. No local fallback. |
| HTTP-01 | Resolve with proxy credentials before a certificate exists, with no route fallback or application wake. |

Default positive refresh is approximately 60 seconds with jitter; authorization
lasts at most five minutes and never beyond chain validity. Negative caching lasts
at most one second. Lookup is bounded by three seconds within the existing total
TLS setup deadline. Deadlines are derived conservatively from RPC start and
monotonic time. A partition may delay invalidation until the original lease ends.
TLS 1.2 and 1.3 use full handshakes; sessions, tickets and early data cannot bypass
current SNI authorization.

Resource limits are 1,024 hostnames with a certificate, a separate 1,024
hostnames without one, three concurrent fetches, 64 handshakes waiting on
lookups, 128 KiB bundles, 16 chain entries and 100 SANs. These counts and the 256 KiB resolve and 512 KiB watch
message caps bound cache memory at about 220 MiB in the worst case; the operator
guide derives it. The watch admits at most 16 streams per control plane, 1,024
interests per stream, 512 KiB messages and two queued responses.
Registration/query work has a three-second bound, delivery one second, and a
server session at most 60 seconds. Polls occur at most once per 250 ms per
stream through the existing single watch SQL slot and bounded FIFO admission.
These bounds do not promise database throughput.

## Testing strategy

Use injected clocks and explicit barriers for expiry, ordering, cancellation and
races. Exercise real PostgreSQL transactions and native authenticated TLS rather
than counting self-skipped wrappers. Inspect negotiated TLS peer certificates,
ALPN and live connections. Test maximum message allocation and work ownership,
including canceled SQL and blocking validation.

Run final workspace, formatting, strict Clippy, database, dependency, inventory
and checker gates on unchanged source. Use production images for the affected
multi-replica lifecycle, outage, restart and resource checks. Retain source/image
identities, commands, actual execution counts and measurement limits. Compare
warm TLS against the immutable Phase 1 baseline on the same machine and workload;
retain the original warning/failure and load/RSS thresholds.

## Completed implementation stages

These stage records preserve the evidence for their original implementations.
Their event-history details are superseded by Phase 7; they are not validation of
the simplified protocol.

| Phase | Delivered | Evidence |
| --- | --- | --- |
| 1 | Validated encrypted resources, conditional atomic storage, immutable warm TLS baseline | [Storage and baseline](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase1/README.md) |
| 2 | Native encrypted/authenticated publication and resolution, proxy HTTP-01 | [API and transport](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase2/README.md) |
| 3 | Bounded dynamic-only cache, real TLS handshakes, removal of static loading | [Cache and TLS](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase3/README.md) |
| 4 | Push synchronization, ordering fences, bounded watch admission | [Propagation](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase4/README.md) |
| 5 | Production-image lifecycle, resource, performance and recovery proof | [Deployed behavior](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase5/README.md) |
| 6 | Required CI, operational docs and independent whole-feature corrections | [Integration at `15a308e`](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase6/README.md) |

## Phase 7: Authoritative snapshot simplification

Goal: delete certificate history and its recovery machinery while preserving
prompt server push, atomic authorization, fixed leases and bounded resource use.
Route notifications and their outbox are outside this change.

Scope: add `last_invalidating_revision` to binding storage/domain/wire metadata;
advance it on binding mutation and removal, preserving it on rotation. Replace
feed reads with one atomic conditional snapshot query: an equal private global
revision skips the hostname join, while registration forces a complete snapshot.
Remove the certificate outbox, pruning, event types, pagination, reset handling,
wire global cursor and unused standalone revision-read capability. Simplify the
server/client watch state machines without weakening registration correlation,
cache incarnation checks or cancellation ownership. Edit the unmerged migration
directly and update current documentation and PR #2.

Completion gate: independently approved source and evidence; all affected tests
and unchanged performance/resource thresholds pass; current docs and PR describe
the final implementation; hosted CI passes the pushed branch. Do not merge.

| Status | Item | Required evidence |
| --- | --- | --- |
| Complete | Atomic storage and simpler protocol | Independent source/focused approval; real PostgreSQL CAS/rollback, conditional snapshots, same-ID binding, shared revisions, watermark and coalescing tests. [Evidence](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase7/README.md). |
| Complete | Cache and registration correctness | 26 focused cache cases pass, including original-lease retention, destructive watermark fencing, held stale responses, whole-message validation and registration/incarnation races; independently approved. |
| Complete | Database work and bounds | Exact production EXPLAIN skips idle host scan; 16×1,024 disjoint native interests pass idle/churn/ordinary-progress and fixed-deadline checks. Maximum wire/decoded-memory proof passes; independently approved with measured workload limits. |
| Complete | Final local and image validation | All twelve local gates and all six final checks pass; rebuilt production images/packaging, multi-replica dynamic TLS lifecycle, real five-minute outage and sleep/re-wake, sampled RSS, routing/protocol/restart/libpq and unchanged performance budgets. |
| Complete | Final skeptical integration review | Independent approval of source, focused/broad checks, images, deployed/RSS/performance, all release gates, current docs and scoped diff. No actionable findings remain. Owned fixtures are cleaned up. |
| Live PR gate | Publication and hosted CI | [PR #2](https://github.com/danthegoodman1/sleepypods/pull/2) records the published commit, its exact-commit CI run and review status. Require successful checks before merge. |

## References

- [Operator guide](operator-guide.md), [runbook](operator-runbook.md),
  [PostgreSQL contract](postgres-store-contract.md).
- [Contributor gates](contributor-guide.md),
  [performance budgets](proxy-hot-path-budgets.md).
- [Shared API](../crates/sleepypods-api/proto/sleepypods/controlplane/v1/control_plane.proto),
  [certificate cache](../crates/frontline/src/certificates/mod.rs),
  [certificate store](../crates/control-plane/src/postgres/certificate_ops.rs).
