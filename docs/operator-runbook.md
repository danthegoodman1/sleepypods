# SleepyPods Operator Runbook

Runtime observability is backend-neutral. Production binaries install the
process-wide stderr sink, which emits structured lines:

```text
observability.type=metric metric.name=<name> metric.value=<number> metric.labels=<k=v,...>
observability.type=event event.name=<name> <field=value ...>
```

The control plane and frontline write event lines for lifecycle changes, errors
and rejected callers, and export metric samples only through their Prometheus
listeners. Metric samples and accepted callers write no line, so a successful
RPC that changes no lifecycle state leaves stderr untouched; `WakeInstance` is
the exception and writes `runtime.wake.event` for every call. The sidecar also
writes its metric samples to stderr.

Production binaries can also expose an opt-in Prometheus `/metrics` listener.
It is disabled unless an explicit listen address is configured:

| Binary | Env var |
| --- | --- |
| control-plane | `SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR` |
| frontline | `SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR` |
| sidecar | `SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR` |

Bind metrics listeners on an internal address or behind your normal scrape
auth/network policy. Do not expose them on the public proxy listener.

## Certificate API diagnosis

Use authenticated native TLS for certificate operations. Transport trust is
provisioned independently of application certificates; publishing an application
certificate cannot repair the control plane's own TLS identity or caller trust.

| Failure | Check |
| --- | --- |
| Native TLS setup fails | Confirm the endpoint hostname is covered by the platform server certificate and that the caller trusts its CA. Check certificate validity and the configured public CA PEM. |
| `Unauthenticated` or `PermissionDenied` | Confirm the correct role token and native TLS transport. Operators manage certificates; proxies resolve certificates and HTTP-01; sidecars cannot read certificate material. Certificate RPCs reject no-auth, plaintext and gRPC-Web. |
| Publication returns `InvalidArgument` | Check leaf-first DER chain, matching PKCS#8 DER key, effective chain validity, server-auth use, all existing bound hostnames and bundle limits. A failed rotation leaves the active version intact. |
| Resolution returns `InvalidArgument` | Check the hostname and the stored chain's validity. An expired or otherwise invalid active bundle is a validation error, not an authoritative miss. |
| Mutation returns `FailedPrecondition` | Read current metadata or binding revision and reconcile the desired change. Expected versions/revisions are required; a stale value cannot overwrite a newer view. |
| Resolution returns `Missing` | Inspect the exact hostname binding, including whether its certificate was removed. Binding and certificate resources are independent of the application's route. |
| `Unavailable` or deadline errors | Check PostgreSQL health and certificate work saturation. A timeout may follow a committed mutation; inspect current state before another conditional write. |
| Resolution returns `Internal` | Check that each replica has the required sealing read key and consistent key material. Investigate damaged stored material without dumping private keys into diagnostics. Storage/decryption errors are not authoritative misses. |
| Control plane exits at startup with `active certificates are sealed under keys missing from the sealing key ring: <ids>` | Restore the named keys to that replica's key ring. To retire a key, keep it in the ring, call `ReencryptCertificates` for it until `remaining` is zero, then remove it. |
| `ReencryptCertificates` returns `InvalidArgument` | The named key is the ring's active key, whose certificates are current, or the ring lacks it. Call a replica whose ring holds the key being retired. |

Follow the [operator guide](operator-guide.md#control-plane-transport-and-sealing-configuration)
for bootstrap configuration and coordinated endpoint/CA changes. Keep old sealing
keys until old-writer database work is settled, `ReencryptCertificates` reports
zero certificates remaining under them, and retained backups stop requiring them.

## Metric Names

Low-cardinality label keys are `operation`, `outcome`, and `state`.

| Metric | Kind | Labels | Use |
| --- | --- | --- | --- |
| `sleepypods_runtime_control_plane_calls_total` | counter | `operation`, `outcome` | Runtime calls: `subscribe_stream` per Subscribe stream opened or refused, `subscribe_route` per host lookup, `unsubscribe`, `wake_instance`, `report_idle`. |
| `sleepypods_runtime_active_streams` | gauge | none | Active drain work, including initial public HTTP setup and streams; these may briefly overlap for one request. |
| `sleepypods_runtime_drain_duration_seconds` | histogram | `outcome` | Runtime drain duration. |
| `sleepypods_runtime_http01_results_total` | counter | `outcome` | HTTP-01 challenge hits, misses, and errors. |
| `sleepypods_runtime_certificate_entries` | gauge | none | Hostnames with a cached certificate; at most 1,024. |
| `sleepypods_runtime_certificate_negative_entries` | gauge | none | Hostnames cached without a certificate: missing, pending or failed lookups; at most 1,024. |
| `sleepypods_runtime_certificate_accounted_bytes` | gauge | none | Chain and key bytes held by cached certificates; separate from process RSS. |
| `sleepypods_runtime_certificate_fetches` | gauge | none | Fetches running or validating; at most three. |
| `sleepypods_runtime_certificate_queue` | gauge | none | Handshake lookups and background refreshes awaiting a fetch slot. |
| `sleepypods_runtime_certificate_tasks` | gauge | none | Supervisor task records, including completed tasks not yet reaped. |
| `sleepypods_runtime_certificate_task_high_water` | gauge | none | Maximum retained supervisor task records since startup. |
| `sleepypods_runtime_certificate_watches` | gauge | none | Watch sessions in setup, streaming or teardown. |
| `sleepypods_runtime_certificate_expiry_risk` | gauge | none | Retained positive views whose lease or chain expires within 60 seconds, including already expired views. |
| `sleepypods_runtime_certificate_events_total` | counter | `operation`, `outcome` | Fixed certificate fetch, refresh, watch and install outcomes. |
| `sleepypods_runtime_materialization_failures_total` | counter | `operation`, `outcome` | Kubernetes/materialization failure path. |
| `sleepypods_runtime_route_cache_lookups_total` | counter | `outcome` | Frontline requests the route cache answered (`hit`) and requests that needed a lookup (`miss`), exported at scrape time. |
| `sleepypods_runtime_pod_route_connects_total` | counter | `outcome` | Frontline connections to a Pod address, exported at scrape time: `pod` when the sidecar confirmed the instance; otherwise the request used the Service because the address refused the connection (`refused`), the attempt outlasted its 250 ms budget (`timeout`), the sidecar closed the claim or echoed another (`claim_rejected`), another connect error occurred (`error`), or the route already failed at this backend generation (`known_failed`). |
| `sleepypods_runtime_subscribe_stream_events_total` | counter | `outcome` | Subscribe stream events: `closed` for an orderly end, `error` for a failed stream that cost the cache, `updated` for a pushed rule set, `invalidated` for a host the control plane stopped serving. |
| `sleepypods_runtime_wake_latency_seconds` | histogram | `outcome` | Duration of one `WakeInstance` call, from request to the accepted or refused answer; the cold start that follows is not included. |
| `sleepypods_control_plane_auth_decisions_total` | counter | `outcome` | Control-plane caller authentication decisions: `success` for accepted calls, `rejected` otherwise. |
| `sleepypods_reconciler_runs_total` | counter | `outcome` | Scans that claim due wakes and cleanups. |
| `sleepypods_reconciler_run_duration_seconds` | histogram | `outcome` | Duration of one claim scan. |
| `sleepypods_reconciler_claims_total` | counter | `state`, `outcome` | Reconciliation leases claimed, by materialization state. |
| `sleepypods_reconciler_lease_renewals_total` | counter | `outcome` | Lease renewal success, error, and lease-loss outcomes. |
| `sleepypods_control_plane_store_available` | gauge | none | 1 while the reconciler reaches the store, 0 while it retries an unavailable store. |
| `sleepypods_materializations_nonterminal` | gauge | `state` | Current pending/deleting materialization backlog by state. |
| `sleepypods_materialization_failures_blocked` | gauge | none | Cleanups parked by a recorded object name that another owner's object holds. |
| `sleepypods_materialization_oldest_nonterminal_age_seconds` | gauge | `state` | Oldest pending/deleting materialization age by state; for `deleting`, how long cleanup has been retrying. |
| `sleepypods_exclusivity_keys_held` | gauge | `state` | Held rendered exclusivity keys by non-deleted materialization state. |
| `sleepypods_kubernetes_operations_total` | counter | `operation`, `outcome` | Controller apply/delete/readiness operation outcomes. |
| `sleepypods_kubernetes_operation_duration_seconds` | histogram | `operation`, `outcome` | Controller apply/delete/readiness operation duration. |

Known bounded label values:

- `operation`: `subscribe_route`, `unsubscribe`, `subscribe_stream`,
  `wake_instance`, `materialize`, `report_idle`, `apply`, `delete`,
  `readiness`, `certificate_fetch`, `certificate_refresh`, `certificate_watch`,
  `certificate_install`
- `outcome`: `success`, `error`, `timeout`, `rejected`, `canceled`, `hit`,
  `miss`, `started`, `closed`, `updated`, `invalidated`, `already_running`,
  `already_waking`, `already_draining`; Pod-route connects use `pod`,
  `refused`, `timeout`, `claim_rejected`, `error` and `known_failed`
- `state`: `pending`, `ready`, `deleting`

Certificate gauges are sampled from cache and worker ownership; separate gauges
may observe slightly different instants during concurrent work. Check each
against its own bound rather than treating a scrape as an atomic transaction.
A successful fetch outcome records receipt of a response; installation has its
own outcome and may still reject invalid or superseded material. Expiry risk
counts host views, so several bindings for one certificate contribute separately.
No certificate metric labels contain hostnames, certificate IDs or error text.

## Structured Events

Event names:

- `runtime.drain.started`
- `runtime.drain.completed`
- `runtime.drain.timeout`
- `runtime.subscribe_stream.event`
- `runtime.wake.event`
- `runtime.materialization.failure`
- `runtime.materialization.reconciliation`
- `runtime.idle_report.event`
- `runtime.http01.event`
- `control_plane.auth.decision`
- `control_plane.store.error`

Lifecycle field keys:

- `instance.id`
- `generation`
- `cluster.id`
- `namespace`
- `error.reason`
- `error.message`
- `active.count`
- `duration.ms`
- `exclusivity.action`
- `exclusivity.key.name`
- `exclusivity.owner.instance.id`
- `auth.decision`
- `auth.reason`
- `auth.caller.role`
- `auth.required.role`
- `grpc.service`

`runtime.materialization.reconciliation` also carries `operation` and
`outcome`, and `runtime.http01.event` carries `http.status`.

Use IDs and generations for correlation, but do not turn high-cardinality IDs
into metric labels.

## Dashboards And Alerts

Build dashboards from the exact names above:

- Route-cache hit rate:
  `sleepypods_runtime_route_cache_lookups_total{outcome=hit}` divided by hit
  plus miss. Alert on sustained miss spikes or hot-cache load phases making
  `subscribe_route` calls.
- Subscribe stream health:
  `sleepypods_runtime_subscribe_stream_events_total{outcome=closed}` rises
  once per frontline per subscription lifetime. Alert on `outcome=error`, which
  marks a discarded route cache, and on
  `sleepypods_runtime_control_plane_calls_total{operation=subscribe_stream,outcome=error}`,
  which marks a frontline that cannot open its stream.
- Control-plane calls: rate of
  `sleepypods_runtime_control_plane_calls_total` by `operation,outcome`.
  Alert on `outcome=error` for `subscribe_route`, `wake_instance`,
  `report_idle`, or `unsubscribe`.
- Wake acceptance latency: p95/p99 over
  `sleepypods_runtime_wake_latency_seconds` by `outcome`; alert on high latency
  and any sustained `error`. It times the `WakeInstance` call; the stateless
  and stateful kind gates measure the whole cold start.
- Materialization failures: rate of
  `sleepypods_runtime_materialization_failures_total` by `operation,outcome`;
  page on nonzero sustained failures. The counter counts attempts that ended
  without finishing their work. A wake or sleep that completes records none,
  including a sleep that waits for its Pod to stop and work that rides out a
  store outage inside its lease.
- Reconciler health: rate of `sleepypods_reconciler_runs_total` by `outcome`,
  p95/p99 over `sleepypods_reconciler_run_duration_seconds`, and
  `sleepypods_reconciler_claims_total` by `state`. Alert when runs stop or
  `outcome=error` runs persist. Run duration measures one claim scan; use
  Kubernetes operation metrics for worker latency.
- Store availability: alert when
  `sleepypods_control_plane_store_available` stays 0. The reconciler keeps
  running and retries its scan; no wake or cleanup is claimed until the store
  answers.
- Materialization backlog and locks:
  `sleepypods_materializations_nonterminal`,
  `sleepypods_materialization_oldest_nonterminal_age_seconds`, and
  `sleepypods_exclusivity_keys_held` by `state`. Page on old pending work, on
  held keys that do not fall after sleep cleanup, and on the oldest `deleting`
  age: cleanup retries until it succeeds, so that age is the signal that
  something holds its objects.
- Parked cleanup: alert on nonzero
  `sleepypods_materialization_failures_blocked`. It counts cleanups stopped by
  an object the materialization does not own. Follow
  [recovery](projection-safety.md#recovery); do not release reservations to
  clear the alert.
- Kubernetes controller operations: rate and latency for
  `sleepypods_kubernetes_operations_total` and
  `sleepypods_kubernetes_operation_duration_seconds` by `operation,outcome`.
  Use Kubernetes/container metrics for CPU, memory, restarts, pod scheduling,
  network, and filesystem signals instead of adding SleepyPods labels for them.
- Drain duration and active streams:
  `sleepypods_runtime_drain_duration_seconds` (`outcome=timeout` marks a drain
  that reached its grace timeout) and `sleepypods_runtime_active_streams`.
- Sidecar idle reports:
  `sleepypods_runtime_control_plane_calls_total{operation=report_idle}` plus
  `runtime.idle_report.event` fields `instance.id`, `generation`, and
  `active.count`.
- HTTP-01:
  `sleepypods_runtime_http01_results_total` and `runtime.http01.event`.
- Pod-first connects: rate of
  `sleepypods_runtime_pod_route_connects_total` by `outcome`. `pod` should
  dominate after cold wakes. Sustained `timeout` or `error` means frontline
  cannot reach Pod addresses, so each new backend generation's first request
  waits up to 250 ms before using the Service; `claim_rejected` means stale Pod
  addresses that another instance's sidecar now holds.
- Certificate delivery: inspect `sleepypods_runtime_certificate_events_total`
  by operation and outcome. Sustained refresh errors/timeouts together with
  increasing `sleepypods_runtime_certificate_expiry_risk` indicate approaching
  handshake failures. Installation errors require investigation even when fetch
  receipt succeeded. Compare cache and work gauges to their configured bounds
  and observe process RSS separately. A
  `sleepypods_runtime_certificate_queue` that stays above zero means lookups
  or refreshes are waiting for the three fetch slots: check control-plane
  resolve latency, and the rate of `certificate_fetch` `miss` outcomes for a
  flood of unknown SNI names. Watch streams normally reconnect at their
  60-second lifetime; a close event alone is not an outage. Sustained absence of
  a watch while the cache holds certificates calls for checking connectivity and
  control-plane admission.
- Load-budget gates: show the latest results from
  `docs/proxy-hot-path-budgets.md` scripts, especially p99 added latency,
  request-rate/throughput ratios, hot-cache zero-control-plane-call assertions,
  and benchmark regression checker output.
- Control-plane auth: rate of
  `sleepypods_control_plane_auth_decisions_total` by `outcome`; alert on a
  sustained `rejected` rate. Each rejection also writes
  `control_plane.auth.decision` with `grpc.service`, `auth.required.role`,
  `auth.reason`, and `auth.caller.role`. Reasons are `missing`, `malformed`,
  `invalid`, and `wrong_role`; token values are not emitted. Accepted calls
  increment the counter and write no event.

## Symptom Runbooks

Cold wakes are slow or failing:

1. Check `sleepypods_runtime_wake_latency_seconds` by `outcome` for slow or
   refused `WakeInstance` calls, and
   `sleepypods_runtime_pod_route_connects_total` for first requests that
   waited on a Pod address before using the Service. Frontline counts each
   wake attempt in
   `sleepypods_runtime_control_plane_calls_total{operation=wake_instance}`;
   while the control plane is unavailable it repeats the call at most every
   2 seconds and holds the cold request, so `outcome=error` or `timeout`
   with no 503s marks a wake waiting for the control plane to return.
2. Search `event.name=runtime.wake.event` and
   `event.name=runtime.materialization.failure` for `instance.id`,
   `generation`, `cluster.id`, `namespace`, and `error.reason`.
3. Check Kubernetes for rendered PV/PVC, Service, workload readiness, image pull,
   and EndpointSlice publication in the materialization namespace. The
   workload is created with zero replicas and then scaled up; the scheduler
   holds its Pod until the claim binds.
4. Check Postgres availability and the instance state/generation with
   `GetInstance`. A wake that cannot succeed, or passes its operation deadline,
   leaves the instance `Failed` and cleans up; requests waiting on it get 503,
   and the next request starts a fresh wake. `ReconcileMaterialization` with
   `status_only=true` returns the reason in `wake_failure_message`.

Rendered object name collision:

1. The wake call returns `InvalidArgument` with a message containing
   `rendered Kubernetes object ref collision`; `runtime.wake.event` records it
   with `error.reason=store` and no message.
2. The wake failed before Kubernetes apply; inspect the failed instance and the
   owner instance named in the error.
3. Use DNS-label-safe `instance_id` values and readable base templates. The
   control plane appends the first sixteen lowercase hexadecimal characters of
   SHA-256 over the complete instance ID, prefixed by `-`, to final object names
   and truncates the base first.
4. Retry after changing the naming template or deleting/finalizing the
   conflicting active materialization.

Workload exclusivity key conflict:

1. Search `runtime.wake.event` for `error.reason=exclusivity_conflict`.
2. Use `exclusivity.key.name`, `exclusivity.owner.instance.id`, `cluster.id`,
   and `namespace` to identify the held key without relying on provider-specific
   disk or license parsing. Rendered key values are intentionally not logged.
3. Inspect the owner instance and active materialization. Pending, Ready, and
   Deleting materializations keep keys held; Deleted materializations do not.
4. Cleanup retries until every object and Pod is absent, and the key is released
   by that proof. If the owner's cleanup is slow, read its failure message with
   `ReconcileMaterialization` and `status_only=true`, and fix what holds the
   objects. Do not wake a second same-key instance until the first
   materialization has finalized or an operator has deliberately changed the
   workload values/key declaration.
5. If the owner's objects are gone for good (its cluster was removed, or they
   were deleted by hand and verified), use `ForceDeleteMaterialization` with the
   instance ID, operator, and reason. If only the lock must be released, use
   `ForceReleaseExclusivityKey` with the exact target, key name, and key value.
   Both force responses include best-effort projection observations for scoped
   refs. Both release the key without proving absence, so use them only when
   the singleton resource cannot still be attached by the old instance.

Non-terminal materialization is stuck:

1. Search `runtime.materialization.reconciliation` for `operation=claim`,
   `operation=reconcile`, `operation=lease_lost`, `operation=scan`, and
   `outcome=error`.
2. Call `ReconcileMaterialization` with the instance ID and
   `status_only=true`. The response includes state, lease metadata, recorded
   refs, projection observations, failure count, kind and message, and the
   next attempt.
3. `failure_kind=transient` retries on its own at a 2 to 32 second backoff: a
   wake until its operation deadline, cleanup until it succeeds. Fix the fault
   the failure message names; no operator RPC is needed.
4. `failure_kind=permanent` on a `deleting` materialization is an ownership
   conflict: observations show `present_unowned` or `deleting_unowned`. Do not
   force-release exclusivity keys or delete the object through SleepyPods.
   Identify why the live object does not carry this materialization's stamps
   and resolve the collision, then call `ReconcileMaterialization` without
   `status_only` to schedule cleanup again. `attempted` means the schedule was
   accepted; drain grace and a live attempt are never overridden.
5. Treat `delete_blocked` with finalizers as progress waiting on that finalizer.
   Fix or intentionally remove it through the owning controller or a manual
   Kubernetes operation; cleanup finishes on a later attempt.
6. Treat `inspect_failed` as unproven, including when it appears in a force
   operation response. Fix Kubernetes API connectivity, RBAC, discovery, or
   namespace access; the control plane has not proven whether that ref is
   missing, owned, unowned, or finalizer-blocked, and cleanup retries once it
   can read it.
7. Finalization requires every recorded ref and every Pod and ReplicaSet with
   the instance label absent. `missing` observations for the refs alone do not
   finish cleanup.
8. While `sleepypods_control_plane_store_available` is 0, no work is claimed;
   see the Postgres outage runbook below.
9. Use `ForceDeleteMaterialization` only when the materialization's objects are
   gone for good: inspect every ref and descendant, remove what remains, and
   verify no singleton resource can be attached by the old workload. Record
   this evidence in the audit reason. The operation leaves the instance where
   its next wake can start: Waking becomes Failed, Running becomes Cold, and
   Draining becomes Cold with its deferred wake started.

Hot routes are missing or unexpectedly cold:

1. Check route-cache miss rate and
   `sleepypods_runtime_control_plane_calls_total{operation=subscribe_route}` by
   outcome. `rejected` means the control plane refused the host: an invalid
   name, more than 256 applicable bindings, or a rule set over 1MiB. `error`
   and `timeout` mean the lookup failed or went unanswered for five seconds.
2. Confirm the `RouteBinding` identity is exact or wildcard as intended and that
   its kind is HTTP or TLS SNI to match the listener. A wildcard suffix covers
   names under it and leaves the base name itself to an exact binding.
3. Check `sleepypods_runtime_subscribe_stream_events_total{outcome=error}`: a
   frontline discards every cached host when its stream fails, and the next
   request for each host pays one lookup.
4. Verify the instance has a ready materialization backend if state is
   `Running`.

A route change still reaches the old backend:

1. Check `sleepypods_runtime_subscribe_stream_events_total` for
   `outcome=updated`: each pushed rule set counts once on every frontline
   subscribed to the host.
2. Confirm the change committed in the database and that the control plane's
   route-change dispatcher is reading: its listening connection reconnects
   after one second, and the 250ms poll delivers meanwhile.
3. Confirm frontlines hold a stream: `subscribe_stream` calls with
   `outcome=error` mark one that cannot connect, and such a frontline serves
   what it holds until the positive TTL.
4. Send a fresh request after the push; stale serving on a frontline with a
   healthy stream is a release-blocking bug.

Idle sleep is not happening:

Automatic sleep is ineligible until the persisted Ready age reaches the greater
of 190 seconds and the resolved class idle timeout. This expected activation
deferral does not indicate stuck drain work; the sidecar also requires its full
quiet interval after actual traffic.

1. Check `sleepypods_runtime_active_streams` for stuck work.
2. Search `runtime.idle_report.event` for `active.count` and `generation`.
3. Check sidecar `SLEEPYPODS_IDLE_TIMEOUT_MS`,
   `SLEEPYPODS_IDLE_RETRY_BACKOFF_MS`, and
   `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS`.
4. Check control-plane call errors for `operation=report_idle`.
5. Read the report outcome. `already_draining` means the instance is draining on
   the reporting Pod's projection; the sidecar keeps reporting while idle, so a
   Pod revived by a wake during drain sleeps again after its next quiet
   interval and a fresh activation floor. The sidecar records `rejected` when
   the instance has no Running projection to sleep, its generation moved on, or
   the control plane asked it to retry later.

Delete cleanup is stuck or objects leak:

1. Confirm `DeleteInstance` returned and inspect the instance state.
2. Search `runtime.materialization.failure` and `runtime.wake.event` for cleanup
   `error.reason`.
3. Check Kubernetes delete permissions for StatefulSet, Deployment, Service,
   Secret, PVC and PV objects, plus Pod/ReplicaSet list permissions and whether
   finalizers block deletion. One cleanup attempt waits up to 60 seconds for
   deleted objects and their Pods to disappear; after that it retries at a 2 to
   32 second backoff until it succeeds.
4. Compare leaked objects' SleepyPods labels and annotations with
   `ReconcileMaterialization` projection observations. Current-owned objects
   carry `managed-by=sleepypods`, materialization id, instance id, instance
   generation, and rendered-hash stamps.
5. If observations show an unowned same-name object, do not ask SleepyPods to
   delete it. Resolve the name collision or manually remove the object only
   after confirming its real owner.
6. After manual cleanup, wait for reconciliation to finalize: its next attempt
   proves absence and releases the reservations. Use
   `ForceDeleteMaterialization` only when the objects are gone for good and
   that is verified; see [recovery](projection-safety.md#recovery).
7. An object under an instance's rendered name that appears after the
   instance's cleanup finished is a create that arrived late: a workload with
   zero replicas, or a Service, Secret or volume object with no Pod. It runs
   nothing, and the instance's next wake takes it over. For a deleted
   instance, remove it by hand; select by
   `app.kubernetes.io/managed-by=sleepypods` and `sleepypods.io/instance-id`.

HTTP-01 challenge fails:

1. Check `sleepypods_runtime_http01_results_total` and `runtime.http01.event`.
2. Resolve the challenge through `ResolveHttp01Challenge` using the exact host
   and token.
3. Confirm the client path is
   `/.well-known/acme-challenge/{token}` and that the Host header matches.
4. Check token expiry. The frontline stops serving a challenge at its expiry,
   and control-plane maintenance removes the record.

TLS termination or SNI passthrough fails:

1. Check `sleepypods_runtime_certificate_events_total` by `operation` and
   `outcome` for failed fetches and installs, and confirm the client sends SNI:
   frontline closes a ClientHello without a valid server name.
2. For termination, verify `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR`,
   verified HTTPS control-plane connectivity and the proxy bearer token. Inspect
   the exact hostname's `GetTlsBinding` and `GetCertificateMetadata`; the active
   bundle must cover the hostname and its whole chain must still be valid.
   Frontline starts with an empty in-memory cache and has no application
   certificate files. Check cache capacity and lookup failures before restarting;
   a restart requires a successful control-plane resolution.
3. For passthrough, verify
   `SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR` and a TLS SNI route
   binding.
4. Confirm client SNI has an exact TLS binding for termination. A wildcard SAN
   can cover that explicit binding. Passthrough uses its exact or wildcard SNI
   route independently of application certificate delivery.
5. If an update has not appeared, inspect native watch connectivity and stream
   capacity on both the serving proxy and its control plane. Registrations and
   reconnects synchronize complete current bindings. Neither notifications nor failed
   refreshes renew permission to serve: an old valid view lasts only until its
   original lease/chain validity deadline, at most five minutes from resolution.
   Snapshots carrying removal/unbind/rebind invalidations prevent stale selection;
   already established connections follow the normal connection/drain policy.

Postgres or database outage:

1. Search control-plane logs for `event.name=control_plane.store.error` and
   check runtime control-plane call `outcome=error`. The event's `error.reason`
   is `unavailable` or `internal` and its `error.message` carries the backend
   text; callers receive the status code with a fixed message. The reconciler
   reports `sleepypods_control_plane_store_available` as 0 and retries its scan
   after 2 seconds, doubling to 15; maintenance retries every 5 seconds. Wakes
   and cleanups in flight continue while their 60-second lease lasts, and the
   next claim resumes the rest once the store answers.
2. Verify `SLEEPYPODS_POSTGRES_URL`.
3. Restore database availability before restarting frontlines aggressively;
   proxies can keep hot cached routes briefly but cold wakes and misses need the
   control plane.
4. Use backups as the source for disaster recovery; Kubernetes objects are not
   the durable resource model.
5. No operator RPC is needed after the outage. The restart gate
   (`scripts/test-kind-e2e-restart.sh`) stops Postgres for 30 seconds during a
   wake and during a drain and requires both to finish on their own.

Kubernetes API server outage:

1. Search `runtime.materialization.failure` and
   `runtime.materialization.reconciliation` with `operation=reconcile` and
   `outcome=error` for Kubernetes connection errors. A refused or broken API
   connection is transient: a wake retries until its operation deadline and
   cleanup until it succeeds. The control-plane process stays up.
2. Wakes in flight complete once the API server answers and the kubelet
   publishes Pod readiness; no operator RPC is needed. The restart gate stops
   the API server for 20 seconds during a wake and requires it to finish on
   its own.

Proxy `Subscribe` disconnects:

1. Check `sleepypods_runtime_subscribe_stream_events_total`. `closed` is the
   orderly end at the subscription lifetime: the frontline reconnects at once
   and keeps serving from its cache. `error` is a failed stream: the frontline
   discards its cache, reconnects after 250ms, and rebuilds the cache as
   requests arrive.
2. Confirm frontlines can reach `SLEEPYPODS_CONTROL_PLANE_ENDPOINT`. A
   reconnect loop shows as `subscribe_stream` calls with `outcome=error`.
3. Repeated `error` ends with a reachable control plane point at lost route
   changes (`DATA_LOSS`) or a frontline too slow to read its stream
   (`DEADLINE_EXCEEDED`); check control-plane load and
   `SLEEPYPODS_CONTROL_PLANE_RESPONSE_TIMEOUT_MS`.
4. Verify pushes arrive after reconnect by changing a test route and watching
   `outcome=updated`.

Control-plane API returns `Unauthenticated` or `PermissionDenied`:

1. Search `event.name=control_plane.auth.decision` and group by
   `grpc.service`, `auth.required.role`, `auth.reason`, and
   `auth.caller.role`. The control plane writes this event for each rejection.
2. `auth.reason=missing`, `malformed`, or `invalid` maps to
   `Unauthenticated`; check that the caller sends `Authorization: Bearer ...`
   and that its configured token matches the control-plane static token for
   that role.
3. `auth.reason=wrong_role` maps to `PermissionDenied`; verify operator,
   frontline/proxy, and sidecar credentials are not swapped.
4. Do not look for token values in logs. They are intentionally redacted and
   should be rotated through the deployment secret source instead.

Image or container startup failures:

1. Run `./scripts/smoke-images.sh` for production image startup, non-root,
   runtime-file, CA, and image-size checks.
2. Inspect pod events for image pull, missing environment settings or
   service-account permission errors. On the control plane, also check the
   independently provisioned platform TLS and sealing-key files.
3. For sidecars, verify rendered env includes app port, instance ID/generation,
   control-plane endpoint, idle policy, runtime-injected
   `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled, and
   `SLEEPYPODS_SIDECAR_MODE`.
4. For workload startup, check app container readiness before blaming the
   sidecar or frontline.

## Startup and process shutdown timing

Frontline initial control-plane Channel setup has a 60-second total budget,
including retry waits, and can be canceled by SIGINT or SIGTERM before it binds
public listeners. The control plane retains its 25-second async shutdown cap;
data-plane listeners retain their configured drain grace periods. After owned
async cleanup finishes, all three binaries allow up to one additional second
for final Tokio runtime teardown. Account for that separate allowance when
interpreting process exit times and configuring termination grace. It prevents
a blocking DNS worker left behind by a canceled future from pinning process
exit; it does not shorten active-work drain or projection cleanup.
