# Codebase Review Remediation Plan

## Overarching Goal

Make routing independent of cache warm-up order, make accepted lifecycle work
survive cancellation and restart, and preserve ownership and drain guarantees
under concurrency. Then reduce control-plane contention, bound resource use, and
simplify the dependency and protocol boundaries. Public APIs and internal
architecture may change to achieve these outcomes.

Review baseline: `dc7cac2`, reviewed 2026-09-04. This updates the existing plan,
preserving Phase 1–7 and work-item identifiers. Previously completed items are
reopened where current evidence exposes a remaining gap. The original
[development plan](development-plan.md) describes product scope; this document is
the current source of truth for review remediation. The initial review was documentation-only; the implementation ledger below records
the subsequently authorized execution.

The useful foundations are the typed domain model, immutable workload versions,
transactional store operations, shared projection layer, pooled HTTP clients,
and broad transport/conformance tests. Keep those. The main architectural
problems are where ownership is split: partial route rules masquerade as an
authoritative routing table, API handlers and reconciliation both drive
Kubernetes, and multiple protocol/event paths implement subtly different rules.

### Priorities and execution order

P1 means a correctness or availability defect to fix before relying on the
production guarantees. P2 means a material scaling or maintenance improvement.
The table records the original review findings. At that review checkpoint only
the four routing probes had demonstrated failing executions; the implementation
ledgers below record subsequent regressions, fault injection and measured gates.

| Priority | Finding and concrete consequence | Owning work |
| --- | --- | --- |
| P1 | A cached wildcard or root path suppresses discovery of an existing exact host or longer path. Requests can reach the wrong instance depending on warm-up order. | 4H |
| P1 | Invalidations wait behind an unrelated wake, can be stranded in a second update buffer, and can deadlock response delivery when the event queue fills. | 4A–4D, 4I |
| P1 | Wake state and recoverable work are committed separately; deleting a Ready instance depends on the RPC continuing. Cancellation/restart can strand Waking or Deleting indefinitely. | 3D, 6A, 6G |
| P1 | Delete invalidation happens after cleanup; bundled route creation emits no change event. A wake during drain or a normal manual reconcile can bypass sleep grace. | 3A, 3C, 6F–6G |
| P1 | Ownership inspection is followed by unconditional Kubernetes writes/deletes; rollback bypasses inspection. A same-name replacement can be overwritten or deleted after inspection. | 6B, 6D |
| P1 | One idle sidecar can sleep a Deployment with another busy replica. The report has no replica identity or aggregate membership. | 8A–8C |
| P1 | WebSocket handshakes forward only forwarding metadata; Cookie, Authorization, Origin and subprotocol negotiation are lost. Authenticated/protocol-specific sockets fail or lose their intended handshake policy. | 7E, 7G |
| P2 | Every route miss scans all bindings of its kind; object reservation takes a table lock; workload-class creation can hold one pool connection while waiting for another. | 5A–5F |
| P2 | Every 100ms maintenance pass clones the whole cache, including indices; positive eviction is insertion/update order despite the LRU name. Tiny hot-lookup benches miss maintenance and churn costs. | 4E |
| P2 | Bounded channels do not bound the detached flights waiting to send into them; listeners and server subscriptions also lack admission limits. | 4J, 7D |
| P2 | Data-plane crates depend on the whole control-plane implementation; production/test protocol paths and store fakes duplicate behavior. | 5C, 7A–7C, 7E |

Execute the routing fixes (4H, 4I, then 4A–4E), the replica contract (8A), and
WebSocket handshake preservation (7G) first. Follow with lifecycle work (3A/3C/3D
and Phase 6 together), store scaling (Phase 5), and the remaining bounds and
structural work. Item IDs are stable references, not a requirement to execute
phases numerically. Phase 6 may introduce the transaction primitives needed by
Phase 5; do not delay correctness for schema optimization. Cosmetic 7F is last.

### Original evidence and implementation decisions

- The four retained routing probes reproduce defects. The other correctness
  findings have concrete source-level failure paths but still require a failing
  regression or fault-injection test before a broad refactor. If the scenario
  cannot occur under the supported contract, correct or remove the finding.
- Full scans, table locking, cache copying and nested pool checkout are present
  in the code. Measure their operational impact before committing to a large
  optimization; retain the smallest change that meets the workload budget.
- Exact-identity caching, one lifecycle driver, an outbox and separate
  incarnation identifiers are proposed solutions, not independently proven
  requirements. Prefer a smaller design if it passes the same correctness and
  recovery gates. Replica restriction versus aggregation is a product decision.
- Cosmetic test-file moves in 7F are optional and do not block completion.
  Perform them only when they simplify an area already being changed. Dependency
  extraction and removal of duplicate production paths have stronger value and
  can be verified through dependency graphs and shared behavioral coverage.

## Implementation Principles

- Keep durable ownership, immutable workload versions, stale-update rejection,
  provider-volume retention, and finite draining. Do not weaken these to make a
  test pass. Describe any intentional contract change explicitly.
- A lazy cache may reuse only the identities or match region the authority has
  actually certified. Prefer caching exact canonical request identities first;
  introduce reusable rule regions only with a completeness/exclusion proof.
- APIs validate and commit intent. A single lifecycle driver reconciles it.
  An accepted wake/delete remains discoverable without another client request.
- Distinguish instance CAS revision, workload/materialization incarnation, and
  lease attempt. A DB lease alone does not fence a late Kubernetes operation.
- Keep response demultiplexing, invalidation and stream-loss handling live while
  any RPC is pending. Bound tasks, waiters and subscriptions, not only queues.
- Prefer one protocol implementation and one transaction body over parallel
  copies. Avoid a new broker, framework or generic store abstraction when the
  existing runtime and Postgres can express the required contract.
- Specify a bounded invalidation window and negative-cache TTL. Do not promise
  instantaneous global consistency, and do not use a five-minute positive TTL
  to conceal dropped events. Keep active streaming workloads free of arbitrary
  total-duration limits; distinguish handshake and inactivity deadlines.

## Testing Strategy

- For Rust changes: `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo test --workspace`. Docs-only changes require `git diff --check`.
- Use deterministic interleavings: cancellation after each durable write,
  replacement between inspect and mutation, lease loss during an API operation,
  invalidations before/between responses, and slow consumers over queue limits.
- Run `./scripts/test-postgres-store.sh` for transactional/schema work. Require
  the real database; the workspace invocation can self-skip that conformance
  body. Existing CI guards against this skip in its separate Postgres step.
- Validate externally visible lifecycle/protocol changes with the relevant
  `scripts/test-kind-e2e-*.sh` gate. A fake Kubernetes API proves local policy,
  not Kubernetes deletion propagation, finalizer or replacement semantics.
- Measure both steady hot forwarding and churn: realistic cache sizes,
  many identities, update bursts, blocked misses, memory/task counts, and
  concurrent independent wakes. Record before/after on the same machine.
  Strict image-smoke ratios need repeated runs (at least three), not a single
  favorable sample. Preserve `docs/proxy-hot-path-budgets.md` unless a measured
  tradeoff justifies changing it.

Review validation: the unmodified workspace reported 657 passed, 14 ignored;
the separate disposable-Postgres run passed both tests and executed real store
conformance. Four temporary routing probes failed on the expected assertions.
Their [patch](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/routing-probes.patch) and
[output](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/routing-probes.log) are retained; the Rust files were
restored after the run. Apply the patch in an isolated checkout of `dc7cac2` and
run `cargo test -p frontline --lib review_probe -- --nocapture` to reproduce.
These failures prove implementation gaps, not remediation completion. Kind,
release load smokes, clippy and benchmarks were not rerun for this docs review.

### Implementation loop

Implementation is authorized for every required outstanding item below. The
coordinator uses separate implementer and skeptical reviewer agents. Work is
complete only after explicit review approval and the relevant integration gates;
an implementer's passing tests alone do not close a phase. The implementation
review preceded publication; later commit and PR preparation are recorded
[separately](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/pr-preparation/README.md).

| Phase | Final execution status | Evidence |
| --- | --- | --- |
| 4 | Implementation, performance and deployed proof approved | [Routing](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase4/README.md), [resource experiment](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase4-resource-envelope/README.md) and [performance](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/final-performance.md) are approved. Final routing, lifecycle and both restart configurations pass pre-TTL reassignment checks. |
| 5 | Approved and gated | [Store evidence](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase5/README.md) retains measured query/lock improvements. The final real-Postgres gate passes eleven database bodies plus invalid-URL validation. |
| 8 | Approved and deployed gate passed | [Replica evidence](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase8/README.md) covers single-replica policy, current Pod UID and fail-closed external drift. The final complete lifecycle driver passes rejection, restoration and cleanup. |
| 3 / 6 | Production implementation and all behavior gates approved | Durable intent, conditional projection, runtime/events, readiness, activation, connector and supersession/heartbeat packets have independent approval. Full lifecycle, both restart configurations, failure paths, exclusivity and four soak cycles pass. The final test-only startup-port fixture correction and workspace/static gate pass. |
| 7 | Required implementation and performance approved | [Protocol](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase7-protocol/README.md), [boundaries](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase7-boundaries/README.md), [admission](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase7-admission/README.md) and [store capabilities](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase7-store-capabilities/README.md) are approved. Cold protocol and three strict default-workload rounds pass with recorded image scope. Optional 7F is deliberately omitted. |

The [final integration ledger](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/README.md) is the
current execution record. All required implementation and validation gates pass.
The final workspace reports 838 passes, zero failures and 16 ignored entries;
827 entries execute locally after eleven database-only self-returns. The separate
PostgreSQL suite passes all 12 entries, including eleven real database bodies.
Formatting, strict all-target clippy and dependency boundaries pass at stable
source. Required deployed lifecycle, restart, protocol, ownership, routing,
exclusivity and four-cycle soak gates pass; the corrected fail-closed inventory
also passes. Repeated performance gates pass with documented warnings and
capacity limits. Independent [final whole-system review](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/final-review.md)
approves closure with no unresolved required findings. These validation records precede PR publication; no production deployment is
claimed.


## Phase 1: Green Gates and CI Baseline

Goal:
Keep existing test and CI coverage as a baseline while adding tests that exercise
the actual production compositions.

Scope:
Preserve 1A–1C and the existing separate real-Postgres CI gate. A green workspace
suite alone is not evidence that the newly identified interleavings work.

Completion gate:
The existing CI checks remain enabled; their local equivalents pass for the
implementation. New regressions become required tests in their owning phases.
A hosted CI run requires a separately requested commit/push.

Testing plan:
Run workspace tests and the real-Postgres script; keep CI's skip/nonzero guards.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Correct wake-error assertion | `crates/control-plane/tests/proxy_api_transport.rs::proxy_wake_projection_failure_returns_transport_error`; workspace tests passed at review baseline. |
| Complete | Work | 1B: Remove inherent `to_string` and lint baseline | Current `reconciler.rs` uses `Display`; final workspace/all-target clippy passes with warnings denied. |
| Complete | Work | 1C: Required CI gates | `.github/workflows/ci.yml` runs fmt, clippy with denied warnings, workspace tests and Postgres with skip/nonzero guards. |
| Complete | Work | 1D: Fail-closed soak discovery | The four inventory reads preserve failures and distinguish verified absence from unavailable discovery. Six mocked regression tests pass locally and independently and are wired into CI; Bash/YAML/source-scope checks pass. [Reviewed packet](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/soak-inventory-fail-closed/README.md). |
| Complete | Test | Supplemental deployed inventory | The corrected read-only inventory passes all four original selector queries with zero objects after the four-cycle soak. [Log and scope](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/final-soak-inventory.json) distinguish this point-in-time evidence from the original reader and retained diagnostic namespaces. |
| Complete | Gate | Existing default-branch CI baseline | Prior evidence retained: CI run `28637514645` at `3fd1997` passed. The implementation-review checkpoint passed local CI-equivalent commands before publication. Subsequent commit, PR and hosted-check preparation are tracked in [review-evidence/pr-preparation](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/pr-preparation/README.md). |
| Complete | Test | Local workspace and real-store baseline | 2026-09-04: `cargo test --workspace` exited 0; `./scripts/test-postgres-store.sh` exited 0, two tests passed including real conformance. |

## Phase 2: Data-Plane Quick Wins

Goal:
Retain the completed pooling, keep-alive and accounting improvements.

Scope:
2A–2F remain the baseline. New cache/event costs belong to Phase 4, and handshake
semantics and admission belong to Phase 7.

Completion gate:
The completed optimizations remain present and their focused behavior tests pass.

Testing plan:
Workspace tests protect behavior; use existing strict image smokes and Criterion
when implementation changes touch forwarding performance.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Pooled HTTP/1 and HTTP/2 clients | `proxy-core/src/http.rs::HttpProxy::new`; `http_proxy_handles_concurrent_http2_streams` verifies connection sharing. |
| Complete | Work | 2B: Sidecar keep-alive | `sidecar/src/runtime.rs`; `http_keep_alive_reuses_connection_without_holding_idle_permit`. |
| Complete | Work | 2C: Bounded protocol setup | Phase 7E replaced the first-request sniffer with shared Hyper HTTP handling. Silent/partial HTTP and h2 setup deadlines pass; established streaming requests remain free of a setup lifetime cap. |
| Complete | Work | 2D: Reap connection tasks | Frontline and sidecar accept loops call their task reapers. This bounds completed results, not active tasks; see 7D. |
| Complete | Work | 2E: Remove per-permit stderr metrics | `proxy-core/src/drain.rs` and `RuntimeActiveStreamsCollector`; collector/filter tests passed in workspace run. |
| Complete | Work | 2F: Shared dependencies and release settings | Root `Cargo.toml` has workspace dependencies and thin LTO, one codegen unit and stripping. |
| Complete | Gate | Previous strict image-smoke baseline | Prior phase evidence: sidecar HTTP ratio 1.052, TCP 1.154; frontline HTTP 1.054, h2c 0.782, TLS h2 0.948, generated gRPC 0.837. These are historical measurements, not fresh measurements. |
| Complete | Test | Existing behavioral coverage | Current workspace tests pass; prior Criterion regression gate passed. Future performance changes require a fresh comparison. |

## Phase 3: Sleep/Wake Product Correctness

Goal:
Stopping routing precedes teardown, drain grace applies to every ordinary entry
point, and deletion makes durable progress. Implement the reopened lifecycle
items with Phase 6 rather than adding another driver.

Scope:
Complete transition notifications and durable deletion; retain the idle-retry,
name and Secret fixes. Centralize sleep eligibility and teardown scheduling.

Completion gate:
A route is invalidated when its instance enters Draining/Deleting even if cleanup
fails; restart or RPC cancellation cannot abandon accepted deletion. Ordinary
reconciliation cannot delete before the recorded drain deadline.

Testing plan:
- Inject finalizer-blocked deletion and cancellation immediately after Deleting
  commits. Restart without retrying the operator RPC; prove eventual completion.
- Prime a wildcard subscription, create an instance with a more-specific bundled
  route, and verify invalidation. Repeat with store failure after mutation.
- Call `ReconcileMaterialization` immediately after `ReportIdle` and prove it
  respects the full grace period. Repeat with a new wake while an existing
  stream is draining; explicitly choose safe drain cancellation or waiting,
  rather than immediate teardown. Run lifecycle-races and restart kind gates.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Lifecycle and route-write invalidations | Transactional targeted history covers route and lifecycle writes, including bundled creation, deletion acceptance and failure. Every CP dispatches independently; real-PG commit ordering, rollback, retention and two-dispatcher regressions pass. Independent 6G review approved. |
| Complete | Work | 3B: Positive TTL policy | Positive fallback is 10s and negative fallback 1s. Healthy unbacklogged dispatch polls every 250ms; bounded history gaps/read failures reset subscriptions. Backlog and runtime/database scheduling add delay. Independent review approved the documented bounded fallback contract and tests. |
| Complete | Work | 3C: Reconciler-driven sleep with enforced grace | Immutable `drain_not_before` is enforced in ordinary and direct claims. Wake during drain persists deferred intent; promotion occurs only after full grace and old cleanup. Real Postgres regressions pass and Phase 6A review approved. Shared admin scheduling (6F) is independently approved; the final merged deployed race gate passes. |
| Complete | Work | 3D: Durable two-phase deletion | Delete acceptance atomically marks the instance and every active materialization Deleting; fresh target-scoped drivers clean and finalize without another RPC. Ready, Pending and zero-materialization deletion regressions pass against actual Postgres, with public asynchronous deletion coverage. Deployed restart and finalizer gates pass. |
| Complete | Work | 3E: Full-ID name qualifier | `kubernetes_name.rs` hashes the full ID; prefix-sharing tests pass. Collision rejection must remain; eight hex characters are not a uniqueness proof. |
| Complete | Work | 3F: Sidecar idle re-arm and activity observation | `sidecar/src/idle/control_plane.rs` retries nonterminal results and races reports against activity; workspace idle tests pass. Phase 8 resolves the replica issue with an explicitly enforced single-replica automatic-sleep contract. |
| Complete | Work | 3G: Initial stream liveness controls | `proxy-core/src/tcp.rs` supplies idle timeout/keepalive; `websocket.rs` supplies close-handshake timeout. The independently approved 7D implementation adds bounded blocked-write/handshake handling and passing ownership/cancellation regressions. |
| Complete | Work | 3H: Sidecar token through Secret | `manifest/render.rs` uses a Secret and `secretKeyRef`; existing manifest tests pass. |
| Complete | Gate | Transition ordering, grace and durable deletion | Actual-Postgres acceptance, cancellation, manual scheduling and grace regressions have independent approval. The final complete lifecycle gate and both restart configurations pass, including membership cleanup within its pinned original operation deadline. |
| Complete | Test | Regression and lifecycle gates | Named API/store regressions are retained in the Phase 6 packets, including 6L stale-work and heartbeat cases. The corrected full lifecycle driver and both final restart configurations pass; logs and stable-source metadata are in the final integration ledger. |

## Phase 4: Authoritative Routing and Independent Event Progress

Goal:
Return the same route as authoritative resolution regardless of cache contents,
and keep invalidation responsive during slow or failed requests. This is the
first implementation priority.

Scope:
- 4H: Cache exact canonical request identities. Retain the matched rule as
  metadata, not permission to route unseen hosts/paths. A future region-based
  cache must carry a complete routing snapshot or explicit exclusions/version.
  This changes the north star's local rule-reuse design while preserving exact
  host, wildcard specificity and longest-path behavior at the authority.
- 4A–4D/4I: One ordered subscription event path; short cache mutations;
  independently progressing subscribe responses, invalidations and bounded wake
  tasks. Make overflow invalidate the stream/cache explicitly rather than
  blocking a response reader that the current request needs.
- 4E: Avoid full cache copies on idle ticks. Measure before choosing a short
  read lock, sharding or snapshots; use incremental updates and an eviction
  policy with explicit cost. Keep positive and negative budgets separate.
- 4J: Bound flights before spawning, including cancelled waiters and distinct
  hostile identities. Add subscribe/send/connect deadlines and shutdown cleanup.

Completion gate:
The four retained probes pass as permanent tests; no stale route survives the
chosen invalidation deadline due to unrelated work; a burst over channel
capacity cannot deadlock; resource use remains bounded under identity churn.

Testing plan:
- End-to-end resolver tests with a partial cache and a complete fake authority:
  wildcard before exact, root before longer path, specificity eviction, route
  insertion after warm-up, SNI equivalents and randomized request order.
- Production coordinator plus generated transport: invalidate during wake,
  refresh-buffer handoff, more than 256 pushed events before a route response,
  stream loss/reconnect with cached entries, and caller cancellation.
- Verify hot routes remain fast during updates; measure 1k/10k/100k entries,
  idle maintenance CPU/allocations, miss throughput, eviction churn and task/RSS
  bounds. Run full-resolve Criterion and strict frontline image smoke.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Independent reads, RPCs and event progress | Actor operations and event consumption progress independently; tests cover invalidation during blocked wake/subscribe and sustained unrelated update churn. Targeted per-subscription event history prevents stale response installation without starving unrelated cold requests. |
| Complete | Work | 4B: Cancellation-safe demux and wake tracking | Pending request correlation survives caller cancellation until its bounded response is handled. Wake readiness waits resolve Accepted/Waking to Running under a configurable 130s routing deadline; cancellation, errors, shutdown and late-response orderings pass. |
| Complete | Work | 4C: Stream-loss invalidation | Stream failure retains a cache-flush barrier across reconnect. Subscription handles carry private globally unique originating-session stamps; reused server IDs cannot alias across streams. Deferred cleanup created before or after reconnect preserves the new subscription and its invalidations. |
| Complete | Work | 4D: Best-effort unsubscribes off critical progress | Unsubscribe work uses bounded deferred cleanup with session reset when cleanup cannot be trusted or completed. Origin stamps are checked under the transport lock; old cleanup cannot remove a replacement stream subscription. Independent review regression passes. |
| Complete | Work | 4E: Cache maintenance and eviction | One shared cache replaces periodic whole-map cloning. Indexed expiry and bounded FIFO bookkeeping keep positive/negative budgets separate. Retained 1k/10k/100k maintenance/churn and concurrent-update measurements include no-op hot resolve 198ns versus 290ns baseline; enabled observation construction is separately measured at 358ns. |
| Complete | Work | 4F: Single HTTP pipeline | The shared coordinator serves the production HTTP/TLS/SNI listener paths. Former mutable HTTP/coordinator helpers are test-only; HTTP-01 and cache observations are exercised through the actual listener. Phase 7 further consolidates protocol serving. |
| Complete | Work | 4G: Frontline upstream-first WebSocket upgrade | `listener.rs:620` connects before returning 101; `listener_websocket_backend_refusal_returns_bad_gateway_without_upgrade` passes. Handshake fidelity and the sidecar are completed and gated in 7E/7G. |
| Complete | Work | 4H: Correct lazy cache authority | Only exact canonical queried identities are cache authority; matched wildcard/path rules remain metadata. All four retained probes are permanent passing regressions, with authority-equivalence permutations, insertion/eviction and SNI coverage. North-star cache semantics updated. |
| Complete | Work | 4I: One lossless, bounded event path | A single ordered bounded transport event queue feeds production consumption. A burst over 256 events fails the stream and preserves the cache-flush barrier; the reader never waits on its event consumer. Composed transport/actor overflow recovery and refresh handoff tests pass. |
| Complete | Work | 4J: Bounded pending work | Admission precedes task allocation: 64 distinct flights, 64 actor operations, 256 waiting callers and bounded transport correlation/cleanup/history. Deadline, shutdown, cancellation, same-identity saturation and distinct-identity recovery tests pass. Broader listener/server limits are Phase 7D. |
| Complete | Gate | Correct routing and bounded invalidation | Independent implementation review approved the retained probes and overflow/reconnect cases. Final routing/HTTP-01, lifecycle reassignment and both restart configurations pass. Reassignment converges before the unchanged fallback TTL; exact recorded timing and source scope are in the integration ledger. |
| Complete | Test | Realistic performance and overload evidence | Full-resolve, concurrent updates, realistic cache maintenance/churn and observation benchmarks are retained. The independently approved [resource experiment](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase4-resource-envelope/README.md) adds exact 64-flight/256-waiter saturation, 40 measured recoveries at two tasks/zero ownership, and 592KiB RSS tail growth with unchanged bounds; its dev/no-op/miss-only scope is explicit. Three refreshed strict image rounds pass on unchanged final data-plane images; the independently audited [performance packet](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/final-performance.md) records all ratios, warning costs and measurement limits. |

## Phase 5: Control-Plane Store Scalability

Goal:
Make route lookup proportional to candidate identities and allow independent
materializations to reserve resources concurrently without sacrificing exclusion.

Scope:
Indexed resolution; normalized resource reservations; shared transaction bodies;
connection, migration and retention policies. Use the durable command/epoch model
from Phase 6 when defining new schema, rather than encoding existing generation
arithmetic into another set of tables.

Completion gate:
A production-size route query touches only candidates, independent wakes do not
wait on a global reservation lock, and collision/exclusivity tests remain green.
Small-pool load and simultaneous migration startup complete within deadlines.

Testing plan:
Run Postgres conformance plus `EXPLAIN (ANALYZE, BUFFERS)` at increasing route
counts. Test concurrent distinct and conflicting reservations, single-connection
class creation, pooled connection saturation, parallel startup, and idempotency
replay before/after deletion and retention expiry. Record latency/lock waits;
an index appearing in a plan alone is insufficient.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: Indexed single-snapshot resolution | Target-aware ResolveRouteRequest uses indexed candidates and instance/backend joins in one SQL statement; proxy subscriptions preserve the result without a second lookup. Snapshot, specificity, target-isolation and 100,000-route tests pass; measured query execution 11.144ms → 0.085ms in the retained fixture. |
| Complete | Work | 5B: Constraint-backed reservations | Migration 0007 backfills normalized object/key reservations with unique constraints and transactional triggers. Real-Postgres competing and independent reservation, cross-version/cluster-scope ownership, rollback and backfill tests pass; independent reservations progressed under an unrelated held row transaction. |
| Complete | Work | 5C: Shared wake/sleep transaction bodies | Ordinary and lease-checked complete-wake/finalize-sleep methods share validated transaction bodies in postgres/materialization_ops.rs; real store conformance passes. Phase 6 may remove obsolete ordinary entry points when the sole driver takes over. |
| Complete | Work | 5D: Lease conflict error taxonomy | StoreError::LeaseConflict distinguishes lost/expired ownership from retryable store unavailability; retry classification regression passes. |
| Complete | Work | 5E: Configurable pool and no nested checkout | One held client serves workload-class insertion/load. Shared configuration validates pool 1–1024 and whole-millisecond 1ms–24h deadlines before allocation/connect; one-slot, saturation and cancellation/reuse tests pass. |
| Complete | Work | 5F: Serialized startup migrations | Startup migrations hold a transaction-scoped advisory lock, validate metadata and roll back on cancellation/error. Four simultaneous startup connections, canceled migration/pool reuse and mismatch/backfill rollback tests pass. |
| Complete | Work | 5G: Explicit idempotency lifetime | Default idempotency records never expire. Explicit optional retention has fixed first-use expiry, bounded GC, deletion tombstones and typed ResourceDeleted replay; real-store tests cover deletion/recreation, retained records and explicit expiry. Runtime maintenance scheduling remains Phase 6 work. |
| Complete | Work | 5H: Separate scheduling and age timestamps | Migration 0007 separates next_attempt_at, drain_not_before and state_entered_at; claims honor drain eligibility and claim/renew/release preserve state age. Real-store grace and stable-age tests pass; retry fairness is Phase 6C. |
| Complete | Gate | Contention and query cost scale as intended | Independent reviewer approved 2026-09-07; retained SQL plans/buffers, real-store concurrency and saturation evidence are in [review-evidence/phase5](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase5/README.md). Measurements establish the documented query-cost improvement, not a universal throughput guarantee. |
| Complete | Test | Migration and store conformance additions | Independent disposable-Postgres run: four tests passed. Review-fix run: 240 library and 35 proxy transport tests, all-target control-plane clippy with denied warnings, and diff check passed. |

## Phase 6: One Durable Lifecycle Driver with Fenced Projection

Goal:
Make wake, sleep and deletion durable operations completed by one reconciler,
with explicit operation identity, bounded retries and safe Kubernetes effects.

Scope:
- 6A: Atomically commit state intent and discoverable work. Wake returns Accepted /
  StillWaking promptly; delete returns accepted state and remains queryable until
  cleanup finishes. Keep compatible wrappers only if they await that same work.
- Preserve the user-visible cold-request guarantee deliberately: frontline waits
  for readiness through subscription updates/resolution under its own bounded
  request deadline. Accepted does not mean Ready. Do not implement enqueue-only
  wake while leaving every cold request to return immediate 503 or wait for TTL.
- Give a materialization an incarnation stable through Pending→Ready, separate
  from the instance CAS revision; give each lease acquisition an attempt fence.
  Define ID reuse/tombstones so an old sidecar cannot act on a recreated instance.
  Map current protobuf generation fields through an explicit migration contract.
- 6B–6D: Use short, idempotent apply/inspect steps, lease renewal where necessary,
  classified failures, fair scheduling, safe rollback/cleanup and conditional
  Kubernetes mutations. Keep exclusivity until absence/termination is proven.
- 6G: Write route-change intent transactionally and dispatch it retryably (a
  small Postgres outbox is sufficient). Keep delivery bookkeeping internal to
  the control plane. Commit/dispatch failure must not silently lose invalidation.

Out of scope:
A general workflow engine, a new message-broker dependency, automatic repair of
live Ready projections, or multi-cluster routing. Do not expand replica fanout
scope merely to implement durable local notifications.

Completion gate:
After every crash/cancellation boundary, accepted work remains discoverable and
either completes, exposes a classified failure, or reports blocked uncertainty
from an unresolved Kubernetes effect. That last state can persist until explicit
operator settlement under the [projection recovery contract](projection-safety.md);
it has no automatic completion deadline. Stale drivers cannot publish, mutate or
delete a newer incarnation. Cleanup uncertainty never releases singleton ownership.
Slow/poison work does not starve unrelated deletion.

Testing plan:
- Crash after state CAS but before work creation, after apply but before publish,
  during delete with finalizers, after durable event commit but before delivery,
  and during lease transfer. Require autonomous recovery without a new RPC.
- Replace an object between inspect and delete/apply; inject same-owner lease
  reacquisition, stale request completion, and rollback after partial apply.
- Never-ready and transient-error workloads beside healthy wake/sleep/delete;
  SIGTERM and reconciler task failure; deadline boundary and ID-reuse tests.
- Kind restart, lifecycle races, stateful retention, and projection drift gates;
  full frontend cold HTTP, gRPC and SNI request behavior against asynchronous wake.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A: Atomic intent and sole lifecycle driver | Atomic wake/Pending and delete/cleanup acceptance survive caller cancellation and fresh-driver restart. Deferred wake preserves grace; stable projection stamps and permanent ID watermarks fence reuse; target-scoped finalization is durable. Independent review approved after actual Postgres 5/5 and public API regressions. Final deployed recovery gates pass. |
| Complete | Work | 6B: Lease timing and attempt fences | Heartbeats cover bounded readiness/apply; owner, attempt and generation fence renew/release/completion. Per-operation durable effect identity prevents late ACKs and ambiguous writes from releasing ownership. Real Postgres lock/snapshot, delayed ACK and ID-reuse races pass. Independent reviewer approved; cooperative pre-dispatch cancellation remains the explicit 6E handoff. |
| Complete | Work | 6C: Failure policy and fair scheduling | Persisted backoff, classified failure/deadline, continuous bounded scheduling and reserved deletion capacity are independently approved. Actual-PG regressions cover deletion beyond a full wake backlog, expired-owner rejection and safe failed-wake cleanup/retry. Cleanup uncertainty retains refs/keys. |
| Complete | Work | 6D: Conditional projection and cleanup | Create-if-absent, UID/RV conditional update/delete and retained partial inventory replace unconditional effects/rollback. Cleanup proves refs and labelled Pods/ReplicaSets absent. Secret apply order fixed. Static storage requires Retain and recorded bindings, checked before any PVC deletion. Actual kind tests prove replacement/finalizer/data continuity; independent review approved. Ambiguous begin/dispatch or sent writes retain reservations under the documented recovery boundary. |
| Complete | Work | 6E: Supervised runtime and termination | Controller, dispatcher, maintenance and listeners have owned supervision, SIGTERM handling and bounded cooperative shutdown. Fatal exits drain workers; known-unsent effects receive exact ACK, uncertain effects retain ownership. Actual subprocess, failed-dispatcher, listener/flow-control and cancellation tests independently pass. Both final deployed restart configurations pass. |
| Complete | Work | 6F: Admin reconciliation through the same scheduler | Normal admin requests enqueue shared work/status and preserve grace; Ready inspection remains available. Actual-PG operator API tests verify failure/deadline and uncertain effect UID/RV diagnostics. Independent review approved; force recovery remains explicit and audited. |
| Complete | Work | 6G: Transactional lifecycle events and notification ordering | Statement-level transactional history reserves contiguous revision ranges with commit ordering. Every CP independently consumes targeted events; history is capped and prefix-pruned, and gaps reset subscriptions. Actual-PG two-transaction, rollback, two-dispatcher, clock-skew and 100003-event bulk tests pass. Unrelated hot subscriptions remain intact. Independent review approved. |
| Complete | Work | 6H: Honest readiness for the first cold request | All three implementation subphases have explicit independent approval: primary-Service isolation and current-Service slices; bounded private readiness with joined listener-failure cleanup; and connection-only refusal retry under existing setup deadlines. Named-port compatibility and old-Pending upgrade recovery are covered. [Evidence](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase6-cold-readiness/README.md) includes 380 passing data-plane tests, actual H1/H2 no-replay and four-path cancellation, plus independently rerun 59 renderer and 10 connector tests. Rebuilt one-shot cold/rewake and four soak-cycle bodies pass; three refreshed strict data-plane load rounds pass. The final complete lifecycle gate also passes. |
| Complete | Work | 6I: Bound startup and final runtime teardown | [Implementation](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase6-startup/README.md) has explicit independent approval. Frontline enforces one absolute 60s initial connection budget and early signal cancellation; all three binaries preserve owned async cleanup before a separate, bounded 1s runtime teardown. This bounds process exit even when blocking DNS workers outlive async cancellation; it does not cancel those workers. 288 relevant tests and strict clippy pass, including actual signal tests and old/new blocked-worker child processes. The observed 75s startup failure's network cause remains unproved. The [final integration evidence](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/README.md) records passing rebuilt images, both restart configurations, four soak-cycle bodies and the completed lifecycle driver. The historical one-request re-wake 502 was not causally recovered; subsequent one-shot re-wake checks pass. The separately approved test-only ownership correction is recorded in 6O; the final workspace/static gate passes. |
| Complete | Work | 6J: Close idle ownership during first handoff | [Production and fixture implementation](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase6-idle-activation/README.md) have explicit independent approval. A durable, generation-fenced 190s initial activation floor and validated combined route/setup/header budget protect first handoff; bounded idle deferral and gapless sidecar HTTP setup ownership close the activity gap. Real Postgres 10, sidecar 71, frontend boundaries 2 and focused deployed-fixture helpers pass. The original 502 transport cause remains unproved. Actual elapsed no-traffic/active wake, both restart configurations and four soak-cycle bodies pass. The final complete lifecycle gate also passes. |
| Complete | Work | 6K: Recover bounded pre-dispatch connection attempts consistently | [Shared connector implementation](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase6-sni-connect/6k-implementation.md) is integrated with explicit independent approval. A finite initial TCP probing window is followed by the remaining whole setup budget; pending DNS is not restarted by short TCP caps. SNI uses the common connector. Existing admission, cancellation, timeout mapping and no application replay are preserved. Policy 10, actual transports 11, SNI 5 and merged data-plane 410 pass. Cold protocol/SNI gates and three refreshed strict data-plane rounds pass with exact image scope in the integration ledger. The final complete lifecycle gate also passes. |
| Complete | Work | 6L: Supersede stale lifecycle work safely | [Final isolated implementation](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase6-intent-supersession/README.md) has explicit independent approval. Claimed-state renewal/failure fences, instance→materialization lock order, safe exact release and per-job cancellation preserve unresolved-effect quarantine. A separately captured self-deadlock is fixed by polling one owned renewal future alongside work. Independent 29 reconciler and 11 real-Postgres gate entries pass without widening the 120ms lease or one-second post-Delete regression bound; 364 local CP tests and strict clippy pass. Checked integration, both rebuilt restart configurations, exclusivity and four soak-cycle bodies pass. The final complete lifecycle gate passes after an independently approved test-only membership deadline correction. The initial deployed timeout's exact interleaving remains unproved. A later [CI fixture correction](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/ci-postgres-handoff/README.md) directly verifies definite-error handoff before the first heartbeat while allowing controlled cleanup latency; the original one-second read-only and unresolved-effect checks remain. |
| Complete | Test | 6M: Align the lifecycle fixture operation deadline with Pod termination | [Captured deadline/grace evidence](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase6-draining-deadline/README.md) proves the artificial 30s operation budget expires before the retry following normal 30s Pod grace. Independent review approves the test-only 90s operation budget, retaining the 60s deletion assertion, 500ms drain grace, fault checks and production 600s default. Both later deployed attempts pass this case; their subsequent failures are recorded separately. |
| Complete | Test | 6N: Observe membership deletion within its original deadline | [Independent proof and fixture review](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/lifecycle-membership-diagnostic/README.md) show healthy cleanup can finish after 60s and before its unchanged 90s operation deadline. The membership observer sends one Delete, pins identity and the original deadline, handles a completion/read race and requires authoritative absence. Five local regressions, the real-Postgres boundary test and the final complete deployed lifecycle gate pass. The original uncaptured timeout's exact cause remains unknown. |
| Complete | Test | 6O: Own startup-test ports through negative assertions | [Correction and independent review](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/sidecar-readiness-port-ownership/README.md) retain the exact controlled-reuse failure. Three cfg(test)-only modules now retain unpublished socket reservations or observe closure on owned connections, preserving exact cancellation/timeout/rejection and positive health behavior. Ten binary and two focused adjacent tests, strict clippy and fmt pass. The exact reviewed source is integrated; no runtime behavior changed. The final complete workspace/static gate also passes. |
| Complete | Gate | Recovery and ownership guarantees | Independent source review and all 12 real-Postgres entries pass. Replacement, finalizer, failure-path and cold protocol gates retain precise image scope. Final 6L lifecycle, both restart configurations, exclusivity, four wake/sleep cycles and fail-closed supplemental inventory pass. |
| Complete | Test | API, store and real-Kubernetes validation | Final workspace/static checks pass at stable source: 838 reported passes, 827 local executions after eleven database self-returns, zero failures and 16 explicit ignores. The real-Postgres gate executes all 12 entries. All required deployed gates pass; logs, exact image scope and fixture corrections are retained in the final integration ledger. |

## Phase 7: Smaller Boundaries, Protocol Fidelity and Resource Limits

Goal:
Separate API contracts from server implementation, converge protocol paths and
make overloaded or stalled peers consume bounded resources.

Scope:
Extract shared contracts and observability without creating a generic framework.
Replace duplicate sidecar sniff/HTTP dispatch with a shared HTTP implementation
that supports upgrades on any keep-alive request. Preserve the full WebSocket
handshake contract, and apply limits before task/stream/subscription allocation.

Completion gate:
Sidecar/frontline normal dependency trees omit Kubernetes/Postgres/server-only
libraries; authenticated/subprotocol WebSockets work through both proxies; stalled
handshakes and overload recover within configured resource budgets.

Testing plan:
- Record `cargo tree` and comparable clean/incremental build times before/after.
  Do not claim linked binary shrinkage from dependency-tree evidence alone.
- Full HTTP/1 keep-alive then upgrade, h2/gRPC streaming, WebSocket Cookie/
  Authorization/Origin forwarding, subprotocol selection/rejection, and refused
  upstream tests through frontline → sidecar → app. Run kind protocol/TLS gates.
- Test distinct misses, open idle connections, TLS/WebSocket handshake stalls,
  unconsumed subscription streams, and both TCP directions blocked on write.
  Require bounded tasks/memory, permit release and recovery after load subsides.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 7A: API/client contract crate | `sleepypods-api` owns one protobuf/build source and shared client contracts. Normal frontline/sidecar dependencies omit control-plane, Kubernetes, Postgres, tonic-web and Axum; CI enforces this. Counts fell 197→111 and 196→102. Independent reviewer approved source, tests and feature isolation. |
| Complete | Work | 7B: Shared observability vocabulary | `sleepypods-observability` owns stable labels, recorder/global sink and exporter; proxy-specific adapters remain in proxy-core. Control-plane no longer depends on proxy-core. Cross-crate recorder identity test and independent review pass. Same-host isolated sidecar check measured 12.889→7.717s clean and 0.134→0.101s unchanged; no linked-size or universal speed claim. |
| Complete | Work | 7C: Required store capabilities and conformance fakes | All 44 production capabilities are required, with compile-fail coverage. Three transport fakes share an explicit fixture and real-PG observable conformance. Retry policy has 12 one-shot mutations and 32 read/fenced/predicate-safe capabilities; actual committed-response-loss, replacement, 1ms retention and exact-ACK regressions pass. Independent review approved. |
| Complete | Work | 7D: Production admission and liveness | Data-plane and CP substages independently approved: finite socket/request/pool/stream bounds, ownership through final upload/response/encoded gRPC-web bytes, and stalled-peer recovery. Data-plane 380 tests and CP listener/admission/transport gates pass. Six listener rounds verified 12 million responses and quantified throughput cost. Full Criterion and all required strict image/deployed gates pass; final evidence retains their exact source scope. |
| Complete | Work | 7E: One sidecar/frontend HTTP and upgrade path | Both listeners use shared Hyper auto HTTP/1/HTTP/2 and upgrades; obsolete first-request sniffer removed. Actual keep-alive-then-upgrade, initial HTTP setup deadline and active streaming tests pass. Independent protocol reviewer approved; final production load and kind protocol gates pass. |
| Complete | Decision | 7F: Optional test layout and cleanup | Omit standalone cosmetic moves. Actual 7C consolidation removes duplicate store semantics and redundant fake implementations; moving unrelated large tests adds no demonstrated value and is not a completion gate. |
| Complete | Work | 7G: WebSocket handshake fidelity | Upstream validation precedes 101; end-to-end auth/cookie/origin/custom headers and complete bounded rejection bodies survive both proxies. Subprotocols are validated, unsupported extensions rejected and hop fields rebuilt. Full-chain tests include trusted metadata despite Connection nominations; IPv6 upstream regression passes. Independent reviewer approved. |
| Complete | Gate | Thin dependencies and bounded, correct protocol runtime | Independent review approved dependency/build measurements, cross-proxy fidelity, admission ownership and stalled-peer recovery in the linked Phase 7 evidence. The final merged cold H2/gRPC/WebSocket and TLS/SNI deployed gates and three refreshed strict image-load rounds pass. Explicit final whole-system review approves completion; no required findings remain. |
| Complete | Test | Required protocol/resource tests | Cross-proxy authenticated/subprotocol handshake, rejection, keep-alive upgrade, connection/task/subscription bounds, backpressure and cancellation tests have independent approval. The final complete workspace/static gate, cold protocol/TLS/SNI gates and three strict data-plane load rounds pass. Exact source/image scope and limits are retained in [review-evidence/final-integration](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/README.md). |

## Phase 8: An Honest Replica and Idle Contract

Goal:
Never declare an instance idle based only on an unrelated idle replica. Prefer a
small explicit supported contract over silently unsafe multi-replica behavior.

Scope:
Recommended immediate policy: allow automatic sleep only for a single supported
replica, validate it when creating a workload class, and reject unsupported
replica counts before accepting instances. If multi-replica Deployment sleep is
required, replace the current ReportIdle contract with member-scoped activity
observations tied to materialization incarnation and known replica membership.
A missing/stale observation must not count as idle. Document how rolling updates,
pod replacement and external scaling interact with that membership.

This intentionally narrows the current Deployment feature surface while retaining
its idle/drain guarantee. Multi-replica support can be restored with the aggregate
protocol; it is not required to fix this defect safely.

Completion gate:
A busy replica prevents sleep, or unsupported multi-replica configurations fail
validation before resource creation. Pod replacement or stale reports cannot
silently satisfy the idle condition for another incarnation.

Testing plan:
Use a two-replica workload with a long stream on one replica and an idle second
replica. For the single-replica policy, assert rejection at the API boundary and
document existing-class migration; for aggregation, add delayed/missing reports,
rescaling, rolling replacement and stale incarnation tests against kind.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | 8A: Choose and publish supported replica semantics | Automatic sleep supports one structured Deployment or StatefulSet with exactly one replica; Deployment uses Recreate. North-star/operator documentation specifies the narrowed contract, upgrade order and exclusive managed-workload ownership requirement. |
| Complete | Work | 8B: Enforce the chosen contract | Class creation, instance validation against stored classes and rendering reject unsupported replica counts. Sidecar ReportIdle carries downward-API Pod UID; control plane checks current Ready materialization, matching ownership/generation and exactly one live ready member. Independent review approved the implementation. |
| Complete | Work | 8C: Existing classes and external changes | Legacy invalid replica templates fail early. Auxiliary raw controllers still render but cannot auto-sleep. Missing UID/permissions, stale/replaced/unready/terminating/extra Pods, controller replacement and external scaling fail closed; the documented membership snapshot does not promise atomicity against later external writes. |
| Complete | Gate | No sleep based on another idle replica | API rejection and production Kubernetes-client checks have independent approval. The final complete deployed lifecycle driver passes wrong-UID rejection, two-replica drift rejection, restoration and cleanup. |
| Complete | Test | Replica and stale-observation coverage | Seven production membership tests, fifteen sidecar API tests, class/render validation and sidecar process tests pass; named cases and logs are in [review-evidence/phase8](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/phase8/README.md). The final complete lifecycle gate passes with its independently approved membership deadline observer. |
