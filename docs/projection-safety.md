# Projection ownership and recovery

A materialization has a stable `projection_generation` for its Kubernetes and
sidecar ownership stamp. Its instance revision changes independently. Every
lease acquisition increments an attempt. Renew, release, failure recording and
completion check the acquired owner, attempt and materialization generation;
a recreated instance ID cannot reuse an old lease even when its deterministic
materialization ID and attempt counter repeat.

## Two authorities

The database lease decides who drives a materialization. The API server decides
whether each mutation still applies. Together they fence a driver that has lost
its lease without any record of the mutations it sent.

- The driver renews its lease immediately before every mutation. A driver that
  has lost the lease can therefore land at most the one request already
  checked.
- A patch or delete carries the UID and resourceVersion the driver observed.
  The API server refuses it once the object changes or is replaced.
- A create has no precondition, so it creates nothing that runs. The
  Deployment or StatefulSet is created with zero replicas. The driver
  then reads the created object's UID and scales that object up with a patch
  conditional on the UID. Only the driver that saw the create succeed knows
  the UID, and cleanup removes the object before it proves absence. This one
  patch leaves the resourceVersion out, because the workload's controller
  writes its status right after creation. A Service, Secret, PersistentVolume
  or PersistentVolumeClaim attaches nothing without a Pod.

A request that arrives late is thus one of three things. A conditional patch
or delete applies only to the exact object version its sender inspected, an
object cleanup must still remove before it proves absence. A create leaves an
object with zero replicas. Neither starts a Pod for a projection that cleanup
has finished, and neither changes an object a newer incarnation has written.

## Apply

Each apply round inspects every rendered object, then walks them in order:
PersistentVolumes, claims, Secrets, Services, workloads. A missing object is
created; a present, owned object is patched on condition that it is unchanged.
A present object with another owner's stamps, or a newer generation of this
instance, is an ownership conflict and stops the wake. A same-instance object
from an older generation is a leftover: the wake patches it in place.

A mutation whose outcome is unknown (a 5xx, a broken connection, a request
timeout) or whose precondition went stale (409) starts another round, up to
four per attempt. The round inspects again, so it creates what is still
missing and patches what arrived. A 429 is retried in place.

The workload is created without waiting for its claim to bind; the scheduler
holds the Pod until the volume is bound.

## Readiness

The primary Service keeps its stable instance and workload selector. Only the
structured workload and its Pod template receive the workload-name label. Raw
objects receive cleanup ownership without it, and a raw label or selector that
names the reserved key is rejected. A class has no raw Pod, Deployment or
StatefulSet, so the structured workload is the only source of Pods.

The injected sidecar has a private HTTP readiness probe on a separate declared
port. Its health listener becomes available only after the proxy binds;
`/ready` also requires a bounded in-Pod loopback connection to the app. Health
traffic does not reach the proxy or its idle and drain accounting.

The driver reads the Service and its EndpointSlices, then follows the slices by
watch. The wake is ready when a slice owned by the observed Service UID has a
ready, nonterminating endpoint with an address. Deleting Services and slices,
and ownerless or old same-name slices, are unready. An unspecified `ready`
condition keeps its documented ready default; explicit false always blocks.
The watch restarts every 30 seconds and whenever it ends, which rereads the
Service. This is a transport observation, not an application health guarantee.

The published backend address is the Service ClusterIP, which is fixed for the
Service's life. A headless Service publishes its DNS name. The driver creates
and deletes the Service, so its address reaches only this projection's Pod.
The completed wake also records the ready Pod's own address, from the ready
EndpointSlice endpoint and its target port, beside it (`pod_backend_uri`).
Routes use the Service address. The Pod address is cleared with the Service
address and kept across a revive, which keeps the same Pod.

## Cleanup

Cleanup inspects every recorded object and deletes, with foreground
propagation and the observed UID and resourceVersion, each one that is present
and owned. It then waits inside the attempt, for up to 60 seconds, until every
recorded object is absent and no Pod or ReplicaSet with the instance label
remains in the target namespaces. A terminating object or Pod is progress: the
attempt keeps waiting, and a sleep that ends within the wait records no
failure. An object that is still present and not terminating is deleted again
in the next poll.

Exclusivity keys and the object inventory are released only by that proof of
absence, or by an audited force operation.

Every managed static PersistentVolume must use `Retain`, including raw
inventory. Class and render validation reject destructive policies and
unsupported bindings. Before any teardown request, cleanup validates every live
volume's reclaim policy and every claim's binding to a recorded volume; a
`Delete` or unknown policy, or an unproven binding, blocks cleanup before
deleting a claim could trigger reclamation. The supported contract assumes
exclusive control over managed object metadata, Pod templates and volume
specifications. Delete preconditions cannot fence a separate actor changing a
volume's policy after that check, so quiesce managed work before editing
storage policy or bindings by hand.

## Leases, timeouts and cancellation

A lease lasts 60 seconds and is renewed every 20 seconds while a wake waits
for readiness or cleanup waits for absence, and before each mutation. The
store refusing a renewal loses the lease at once. A store error is retried
until the lease's own expiry, measured locally from the last successful
renewal, and only then counts as loss; a store outage shorter than the lease
does not interrupt the work. Completion, failure recording and release are
retried the same way.

One Kubernetes mutation or short read is bounded at 10 seconds. A readiness
wait is bounded at 120 seconds per attempt. A wake has an operation deadline
(600 seconds by default) after which it fails; cleanup has none.

Cancellation, from shutdown, lease loss or the wake deadline, stops the work
at its next step. A mutation already dispatched runs to its own timeout first,
so its response is seen. The controller allows 20 seconds to drain and the
runtime 25 seconds.

An accepted Delete changes Pending to Deleting without taking the lease or
changing the projection stamp. Renewal checks the state the attempt claimed,
so the wake's driver stops at its next renewal or mutation, releases its exact
claim, and cleanup starts without waiting for the lease to expire.

A wake or sleep transition locks the instance row before its materialization
row, and a cleanup completion locks its materialization alone, so no two
lifecycle writes wait on each other in opposite orders.

## Recovery

Each of these recovers without an operator:

| Fault | What happens |
| --- | --- |
| 5xx, reset connection or stalled request on a create, patch or delete | The attempt inspects the object again and continues. After four rounds it ends as a transient failure and the next attempt, after a backoff of 2 to 32 seconds, starts from inspection. |
| Control-plane process lost at any point around a mutation | The lease expires within 60 seconds. The next claim inspects the live objects and continues the wake or the cleanup. A create the lost process had in flight either arrived, and is patched or deleted like any other object, or arrives later with zero replicas. |
| Store unavailable | Work in flight continues while its lease lasts. The reconciler backs off between scans, from twice its interval up to 15 seconds, and reports `sleepypods_control_plane_store_available` as 0. The reconciler keeps running and resumes when the store answers. |
| Wake cannot succeed (invalid manifest, refused request, ownership conflict) or passes its deadline | The instance becomes Failed and its materialization is cleaned up at once. The next request starts a fresh wake. |
| Cleanup slowed by a terminating Pod, a finalizer, a volume detach or a refused API call | Cleanup retries until it succeeds, at a backoff capped at 32 seconds. `sleepypods_materialization_oldest_nonterminal_age_seconds{state="deleting"}` shows how long it has been trying. |

One condition parks cleanup for an operator: a recorded object name held by an
object that another owner, or a newer generation of the instance, stamped. The
driver deletes nothing it does not own. `sleepypods_materialization_failures_blocked`
counts these. Resolve the collision, then call `ReconcileMaterialization` to
schedule cleanup again.

A create that arrives after cleanup has proven absence leaves an object with
zero replicas under the instance's rendered name. The instance's next wake
takes it over as an older-generation leftover. If the instance is deleted
before it wakes again, the object stays until an operator removes it; it
carries the `app.kubernetes.io/managed-by=sleepypods` and
`sleepypods.io/instance-id` labels and runs nothing.

`ReconcileMaterialization` with `status_only=true` reports the scheduling
deadline, failure classification and message. Without `status_only` it clears
the failure state and schedules the work again; it never overrides drain grace
or a live attempt.

`ForceDeleteMaterialization` removes a materialization's record, inventory and
exclusivity keys in one audited transaction, without touching Kubernetes, and
leaves the instance where its next wake can start: Waking becomes Failed,
Running becomes Cold, and Draining becomes Cold with its deferred wake
activated. Use it when the cluster the materialization lived in is gone or its
objects were removed by hand. Before using it on a live cluster, inspect and
remove the objects and their Pods, and verify that an external singleton
resource cannot still be attached by the old workload: the force operation
releases the keys without proving absence. Record that evidence in the
required operator and reason fields. The same applies to
`ForceReleaseExclusivityKey`.
