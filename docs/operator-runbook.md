# SleepyPods Operator Runbook

Runtime observability is backend-neutral. Production binaries install the
process-wide stderr sink, which emits structured lines:

```text
observability.type=metric metric.name=<name> metric.value=<number> metric.labels=<k=v,...>
observability.type=event event.name=<name> <field=value ...>
```

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

Follow the [operator guide](operator-guide.md#control-plane-transport-and-sealing-configuration)
for bootstrap configuration and coordinated endpoint/CA changes. Keep old sealing
keys until old-writer database work is settled, live rows are verified under the
new key, and retained backups no longer require them.

## Metric Names

Low-cardinality label keys are `protocol`, `direction`, `operation`,
`outcome`, and `state`.

| Metric | Kind | Labels | Use |
| --- | --- | --- | --- |
| `sleepypods_proxy_active_streams` | gauge | `protocol` | Active proxy requests, streams, or upgraded sessions. |
| `sleepypods_proxy_admission_decisions_total` | counter | `protocol`, `outcome` | Accepted/rejected new proxy work. |
| `sleepypods_proxy_drain_events_total` | counter | `state`, `outcome` | Drain start, idle completion, rejection, timeout. |
| `sleepypods_proxy_forwarded_bytes_total` | counter | `protocol`, `direction` | TCP/TLS byte forwarding. |
| `sleepypods_proxy_forwarded_messages_total` | counter | `protocol`, `direction` | Message-oriented forwarding, including WebSocket. |
| `sleepypods_proxy_operation_duration_seconds` | histogram | `protocol`, `operation`, `outcome` | Bounded proxy primitive duration. |
| `sleepypods_proxy_tls_client_hello_total` | counter | `protocol`, `outcome` | TLS ClientHello/SNI parser outcomes. |
| `sleepypods_runtime_control_plane_calls_total` | counter | `operation`, `outcome` | Runtime calls such as subscribe, wake, unsubscribe, report idle. |
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
| `sleepypods_runtime_route_cache_lookups_total` | counter | `outcome` | Frontline route-cache hit/miss results. |
| `sleepypods_runtime_subscribe_stream_events_total` | counter | `outcome` | Subscribe stream close/update/invalidation events. |
| `sleepypods_runtime_wake_latency_seconds` | histogram | `outcome` | Control-plane wake latency. |
| `sleepypods_reconciler_runs_total` | counter | `outcome` | Materialization discovery/scheduling scans. |
| `sleepypods_reconciler_run_duration_seconds` | histogram | `outcome` | Duration of one discovery/scheduling scan. |
| `sleepypods_reconciler_candidates_total` | counter | `state` | Rows selected for reconciliation. |
| `sleepypods_reconciler_claims_total` | counter | `state`, `outcome` | Reconciliation lease claim outcomes. |
| `sleepypods_reconciler_lease_renewals_total` | counter | `outcome` | Lease renewal success, error, and lease-loss outcomes. |
| `sleepypods_materializations_nonterminal` | gauge | `state` | Current pending/deleting materialization backlog by state. |
| `sleepypods_materialization_effects_uncertain` | gauge | none | Materializations with unresolved Kubernetes effect barriers. |
| `sleepypods_materialization_failures_blocked` | gauge | none | Materializations with blocked failure status. |
| `sleepypods_materialization_oldest_nonterminal_age_seconds` | gauge | `state` | Oldest pending/deleting materialization age by state. |
| `sleepypods_exclusivity_keys_held` | gauge | `state` | Held rendered exclusivity keys by non-deleted materialization state. |
| `sleepypods_kubernetes_operations_total` | counter | `operation`, `outcome` | Controller apply/delete/readiness operation outcomes. |
| `sleepypods_kubernetes_operation_duration_seconds` | histogram | `operation`, `outcome` | Controller apply/delete/readiness operation duration. |

Known bounded label values:

- `protocol`: `tcp`, `http`, `websocket`, `tls`
- `direction`: `client_to_upstream`, `upstream_to_client`
- `operation`: `accept`, `admit`, `connect`, `forward`, `rewrite_request`,
  `drain`, `tls_client_hello`, `route_cache_lookup`, `subscribe_route`,
  `unsubscribe`, `subscribe_stream`, `wake_instance`, `materialize`,
  `http01_resolve`, `report_idle`, `apply`, `delete`, `readiness`,
  `certificate_fetch`, `certificate_refresh`, `certificate_watch`,
  `certificate_install`
- `outcome`: `success`, `error`, `timeout`, `rejected`, `canceled`, `hit`,
  `miss`, `started`, `closed`, `updated`, `invalidated`, `already_running`,
  `already_waking`, `already_draining`
- `state`: `accepting`, `active`, `draining`, `idle`, `pending`, `ready`,
  `failed`, `deleting`
- TLS ClientHello outcomes use the `outcome` label with `sni`, `no_sni`,
  `incomplete`, `not_tls`, `unsupported_version`, `not_client_hello`,
  `record_too_large`, `malformed`, or `invalid_hostname`.

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
- `runtime.route_cache.lookup`
- `runtime.subscribe_stream.event`
- `runtime.wake.event`
- `runtime.materialization.failure`
- `runtime.materialization.reconciliation`
- `runtime.idle_report.event`
- `runtime.http01.event`
- `control_plane.auth.decision`

Lifecycle field keys:

- `instance.id`
- `route.id`
- `subscription.id`
- `generation`
- `backend.generation`
- `cluster.id`
- `namespace`
- `error.reason`
- `active.count`
- `duration.ms`
- `auth.decision`
- `auth.reason`
- `auth.caller.role`
- `auth.required.role`
- `grpc.service`

Use IDs and generations for correlation, but do not turn high-cardinality IDs
into metric labels.

## Dashboards And Alerts

Build dashboards from the exact names above:

- Route-cache hit rate:
  `sleepypods_runtime_route_cache_lookups_total{outcome=hit}` divided by hit
  plus miss. Alert on sustained miss spikes or hot-cache load phases making
  `subscribe_route` calls.
- Control-plane calls: rate of
  `sleepypods_runtime_control_plane_calls_total` by `operation,outcome`.
  Alert on `outcome=error` for `subscribe_route`, `wake_instance`,
  `report_idle`, or `unsubscribe`.
- Wake latency: p95/p99 over `sleepypods_runtime_wake_latency_seconds` by
  `outcome`; alert on high latency and any sustained `error` or `timeout`.
- Materialization failures: rate of
  `sleepypods_runtime_materialization_failures_total` by `operation,outcome`;
  page on nonzero sustained failures.
- Reconciler health: rate of `sleepypods_reconciler_runs_total`,
  p95/p99 over `sleepypods_reconciler_run_duration_seconds`, and
  `sleepypods_reconciler_claims_total` by `state,outcome`. Alert when runs stop
  or claim errors/rejections spike across replicas. Run duration measures the
  discovery/scheduling scan; use Kubernetes operation metrics for worker latency.
- Materialization backlog and locks:
  `sleepypods_materializations_nonterminal`,
  `sleepypods_materialization_oldest_nonterminal_age_seconds`, and
  `sleepypods_exclusivity_keys_held` by `state`. Page on old pending/deleting
  work or held keys that do not fall after sleep cleanup.
- Blocked recovery: alert on nonzero `sleepypods_materialization_effects_uncertain`
  or sustained `sleepypods_materialization_failures_blocked`. Inspect status and
  follow the [uncertain-effect recovery procedure](projection-safety.md); do not
  automatically release reservations to clear an alert.
- Kubernetes controller operations: rate and latency for
  `sleepypods_kubernetes_operations_total` and
  `sleepypods_kubernetes_operation_duration_seconds` by `operation,outcome`.
  Use Kubernetes/container metrics for CPU, memory, restarts, pod scheduling,
  network, and filesystem signals instead of adding SleepyPods labels for them.
- Drain duration and active streams:
  `sleepypods_runtime_drain_duration_seconds`,
  `sleepypods_runtime_active_streams`, and
  `sleepypods_proxy_drain_events_total{outcome=timeout}`.
- Sidecar idle reports:
  `sleepypods_runtime_control_plane_calls_total{operation=report_idle}` plus
  `runtime.idle_report.event` fields `instance.id`, `generation`, and
  `active.count`.
- HTTP-01:
  `sleepypods_runtime_http01_results_total` and `runtime.http01.event`.
- Proxy throughput:
  `sleepypods_proxy_forwarded_bytes_total`,
  `sleepypods_proxy_forwarded_messages_total`, and
  `sleepypods_proxy_active_streams`.
- TLS/SNI:
  `sleepypods_proxy_tls_client_hello_total` and TLS listener errors.
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
- Control-plane auth: `control_plane.auth.decision` by `grpc.service`,
  `auth.required.role`, `auth.reason`, and `auth.caller.role`. Reasons are
  `accepted`, `missing`, `malformed`, `invalid`, and `wrong_role`; token values
  are not emitted.

## Symptom Runbooks

Cold wakes are slow or failing:

1. Check `sleepypods_runtime_wake_latency_seconds` by `outcome`.
2. Search `event.name=runtime.wake.event` and
   `event.name=runtime.materialization.failure` for `instance.id`,
   `generation`, `cluster.id`, `namespace`, and `error.reason`.
3. Check Kubernetes for rendered PV/PVC, Service, workload readiness, image pull,
   and EndpointSlice publication in the materialization namespace.
4. Check Postgres availability and the instance state/generation with
   `GetInstance`.

Rendered object name collision:

1. Search `runtime.wake.event` for `error.reason=store` and an error containing
   `rendered Kubernetes object ref collision`.
2. The wake failed before Kubernetes apply; inspect the failed instance and the
   owner instance named in the error.
3. Use DNS-label-safe `instance_id` values and readable base templates. The
   control plane appends the first eight lowercase hexadecimal characters of
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
4. If cleanup is stuck, resolve Kubernetes deletion errors first. Do not wake a
   second same-key instance until the first materialization has safely finalized
   or an operator has deliberately changed the workload values/key declaration.
5. If cleanup was externally verified and the old process/request path has been
   fenced so no delayed mutation can take effect, use `ForceDeleteMaterialization` with the materialization id, operator, and
   reason. If only the lock must be released, use `ForceReleaseExclusivityKey`
   with the exact target, key name, and key value. Both force responses include
   best-effort projection observations for scoped refs, but treat key release as
   unsafe unless the singleton resource cannot still be attached by the old
   instance.

Non-terminal materialization is stuck:

1. Search `runtime.materialization.reconciliation` for `operation=claim`,
   `operation=reconcile`, `operation=lease_lost`, and `outcome=error`.
2. Call `ReconcileMaterialization` with the materialization id. It enqueues
   eligible work through the shared scheduler; `attempted` means enqueue accepted.
   Use `status_only=true` for read-only inspection. The response includes state,
   lease metadata, recorded refs, projection observations, failure details,
   scheduling deadline and any unresolved effect identity.
3. Treat `missing` observations as cleanup progress only when no unresolved
   effect barrier remains. Finalization requires all refs and old Pod/ReplicaSet
   members absent. Check [effect diagnostics](projection-safety.md) when a lease
   expired but no new controller can claim the row; name absence cannot resolve
   an old dispatched create.
4. Treat `present_unowned` and `deleting_unowned` as unsafe conflicts. Do not
   force-release exclusivity keys or delete the object through SleepyPods; first
   identify why the live object no longer carries the current SleepyPods
   materialization stamps.
5. Treat `delete_blocked` with finalizers as non-terminal. Fix or intentionally
   remove the Kubernetes finalizer through the owning controller or a manual
   Kubernetes operation, then let reconciliation retry.
6. Treat `inspect_failed` as non-terminal, including when it appears in a force
   operation response. Fix Kubernetes API connectivity, RBAC, discovery, or
   namespace access first; the control plane has not proven whether that ref is
   missing, owned, unowned, or finalizer-blocked.
7. Before `ForceDeleteMaterialization`, fence the old process and request path,
   establish that old accepted API mutations can no longer take effect, inspect
   every ref/descendant, and verify no singleton resource can be attached by the
   old workload. Record this evidence in the audit reason. An absent name or a
   stopped process alone is insufficient; keep the barrier if proof is unavailable.

Hot routes are missing or unexpectedly cold:

1. Check route-cache miss rate and
   `sleepypods_runtime_control_plane_calls_total{operation=subscribe_route}`.
2. Confirm the `RouteBinding` identity is exact or wildcard as intended and that
   the route protocol matches HTTP or TLS SNI.
3. Search `runtime.route_cache.lookup` for `route.id` and `subscription.id`.
4. Verify the instance has a ready materialization backend if state is
   `Running`.

A route change still reaches the old backend:

1. Check `sleepypods_runtime_subscribe_stream_events_total` for
   `outcome=invalidated` or `closed`.
2. Search `runtime.subscribe_stream.event` for the old `subscription.id`.
3. Confirm frontlines can reconnect to the control plane and that route
   reassignment committed in the database.
4. Send a fresh request after invalidation; stale serving before positive TTL
   expiry is a release-blocking bug.

Idle sleep is not happening:

Automatic sleep is ineligible until the persisted Ready age reaches the greater
of 190 seconds and the resolved class idle timeout. This expected activation
deferral does not indicate stuck drain work; the sidecar also requires its full
quiet interval after actual traffic.

1. Check `sleepypods_runtime_active_streams` and
   `sleepypods_proxy_active_streams` for stuck work.
2. Search `runtime.idle_report.event` for `active.count` and `generation`.
3. Check sidecar `SLEEPYPODS_IDLE_TIMEOUT_MS`,
   `SLEEPYPODS_IDLE_RETRY_BACKOFF_MS`, and
   `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS`.
4. Check control-plane call errors for `operation=report_idle`.

Delete cleanup is stuck or objects leak:

1. Confirm `DeleteInstance` returned and inspect the instance state.
2. Search `runtime.materialization.failure` and `runtime.wake.event` for cleanup
   `error.reason`.
3. Check Kubernetes delete permissions for StatefulSet, Deployment, Service,
   Secret, PVC and PV objects, plus Pod/ReplicaSet list permissions and whether finalizers block deletion.
4. Compare leaked objects' SleepyPods labels and annotations with
   `ReconcileMaterialization` projection observations. Current-owned objects
   carry `managed-by=sleepypods`, materialization id, instance id, instance
   generation, and rendered-hash stamps.
5. If observations show an unowned same-name object, do not ask SleepyPods to
   delete it. Resolve the name collision or manually remove the object only
   after confirming its real owner.
6. After manual cleanup, prefer waiting for materialization reconciliation to
   finalize. Use `ForceDeleteMaterialization` only after the old process/request
   path is fenced, delayed effects are excluded, and cleanup is proven. Follow
   the [uncertain-effect recovery procedure](projection-safety.md).

HTTP-01 challenge fails:

1. Check `sleepypods_runtime_http01_results_total` and `runtime.http01.event`.
2. Resolve the challenge through `ResolveHttp01Challenge` using the exact host
   and token.
3. Confirm the client path is
   `/.well-known/acme-challenge/{token}` and that the Host header matches.
4. Check token expiry and run `ExpireHttp01Challenges` after the expected
   validation window.

TLS termination or SNI passthrough fails:

1. Check `sleepypods_proxy_tls_client_hello_total` outcomes, especially
   `no_sni`, `invalid_hostname`, `not_tls`, or `malformed`.
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

1. Check control-plane process logs for store provider errors and runtime
   control-plane call `outcome=error`.
2. Verify `SLEEPYPODS_STORE_PROVIDER=postgres` and `SLEEPYPODS_POSTGRES_URL`.
3. Restore database availability before restarting frontlines aggressively;
   proxies can keep hot cached routes briefly but cold wakes and misses need the
   control plane.
4. Use backups as the source for disaster recovery; Kubernetes objects are not
   the durable resource model.

Proxy `Subscribe` disconnects:

1. Check `sleepypods_runtime_subscribe_stream_events_total{outcome=closed}`.
2. Confirm frontlines can reach `SLEEPYPODS_CONTROL_PLANE_ENDPOINT`.
3. Expect lazy cache rebuild on later requests; sustained reconnect loops should
   also show control-plane call errors.
4. Verify route invalidations are received after reconnect by changing a test
   route and watching for cache miss/re-resolve.

Control-plane API returns `Unauthenticated` or `PermissionDenied`:

1. Search `event.name=control_plane.auth.decision` and group by
   `grpc.service`, `auth.required.role`, `auth.reason`, and
   `auth.caller.role`.
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
