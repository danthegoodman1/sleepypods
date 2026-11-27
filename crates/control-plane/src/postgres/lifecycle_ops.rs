//! Durable lifecycle acceptance. Kubernetes effects belong exclusively to the reconciler.
use deadpool_postgres::GenericClient;

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{
        generation_to_i64, instance_from_row, materialization_id,
        rendered_exclusivity_keys_to_json, rendered_objects_to_json,
    },
    materialization_ops::upsert_materialization,
    sql,
};
use crate::{
    instance::{InstanceRecord, InstanceState},
    materialization::{AcceptWakeRequest, MaterializationState},
    store::{StoreError, StoreResult},
};

/// Accepts a wake: the instance becomes Waking with a Pending projection that
/// carries the rendered refs, reservations, target, projection stamp and a
/// backend generation above any the target used before. A replay of an
/// accepted wake returns the instance as the first acceptance left it.
///
/// The request's incarnation names the path. One past the expected
/// generation wakes a Cold or Failed instance in two statements: the
/// conditional transition, and the projection insert, which needs every
/// earlier projection cleaned up. Two past it wakes a Draining instance,
/// which revives the projection inside its drain grace or defers the wake
/// until the drain finishes.
pub(crate) async fn accept_wake(
    store: &PostgresStore,
    mut request: AcceptWakeRequest,
) -> StoreResult<InstanceRecord> {
    if request.pending.state != MaterializationState::Pending
        || request.pending.projection_generation != request.pending.instance_generation.next()
        || request.pending.backend.is_some()
    {
        return Err(StoreError::invalid_argument(
            "accepted wake projection does not match its next incarnation",
        ));
    }
    let draining = request.pending.instance_generation == request.expected_generation.next().next();
    let accepted = if request.pending.instance_generation == request.expected_generation.next() {
        wake_settled(store, &request).await?
    } else if draining {
        accept_during_drain(store, &mut request).await?
    } else {
        return Err(StoreError::invalid_argument(
            "accepted wake projection does not match its next incarnation",
        ));
    };
    match accepted {
        Some(instance) => {
            if instance.state == InstanceState::Waking {
                crate::runtime_work::signal_lifecycle_work();
            }
            Ok(instance)
        }
        None => refused_wake(store, &request, draining).await,
    }
}

/// Wakes a Cold or Failed instance, or returns `None` when it is in neither
/// state at the expected generation or an earlier projection is still being
/// cleaned up.
async fn wake_settled(
    store: &PostgresStore,
    request: &AcceptWakeRequest,
) -> StoreResult<Option<InstanceRecord>> {
    let pending = &request.pending;
    let id = pending.instance_id.as_str();
    let mut client = store.client().await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    let Some(row) = sql::query_opt(
        &tx,
        "
        UPDATE instances
        SET state = 'waking', generation = $3
        WHERE instance_id = $1 AND generation = $2 AND state IN ('cold', 'failed')
        RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
        ",
        &[
            &id,
            &generation_to_i64(request.expected_generation)?,
            &generation_to_i64(pending.instance_generation)?,
        ],
    )
    .await?
    else {
        return Ok(None);
    };
    // A fresh statement under the instance lock sees every committed cleanup.
    let inserted = sql::query_opt(
        &tx,
        "
        INSERT INTO materializations (
            materialization_id, instance_id, instance_generation, projection_generation,
            cluster_id, namespace, state, backend_generation, rendered_objects, exclusivity_keys
        )
        SELECT $1, $2, $3, $4, $5, $6, 'pending',
            GREATEST($7, COALESCE((SELECT max(backend_generation) + 1 FROM materializations
                WHERE instance_id = $2 AND cluster_id = $5 AND namespace = $6), 0)),
            $8, $9
        WHERE NOT EXISTS (SELECT 1 FROM materializations WHERE instance_id = $2 AND state <> 'deleted')
        ON CONFLICT (instance_id, cluster_id, namespace) DO UPDATE SET
            materialization_id = EXCLUDED.materialization_id,
            instance_generation = EXCLUDED.instance_generation,
            projection_generation = EXCLUDED.projection_generation,
            state = EXCLUDED.state,
            backend_uri = NULL,
            pod_backend_uri = NULL,
            backend_generation = EXCLUDED.backend_generation,
            rendered_objects = EXCLUDED.rendered_objects,
            exclusivity_keys = EXCLUDED.exclusivity_keys,
            reconcile_owner = NULL,
            reconcile_lease_expires_at_unix_millis = NULL
        RETURNING materialization_id
        ",
        &[
            &materialization_id(&pending.instance_id, &pending.target)?.as_str(),
            &id,
            &generation_to_i64(pending.instance_generation)?,
            &generation_to_i64(pending.projection_generation)?,
            &pending.target.cluster_id(),
            &pending.target.namespace(),
            &super::mapping::backend_generation_to_i64(pending.backend_generation)?,
            &rendered_objects_to_json(&pending.rendered_objects),
            &rendered_exclusivity_keys_to_json(&pending.exclusivity_keys),
        ],
    )
    .await?;
    if inserted.is_none() {
        return Err(StoreError::unavailable(
            "previous materialization cleanup is still pending",
        ));
    }
    tx.commit().await.map_err(map_postgres_error)?;
    instance_from_row(&row).map(Some)
}

/// Revives a Draining instance's projection inside its drain grace, or
/// records the wake to start when the drain finishes. Returns `None` when the
/// instance is not Draining at the expected generation.
async fn accept_during_drain(
    store: &PostgresStore,
    request: &mut AcceptWakeRequest,
) -> StoreResult<Option<InstanceRecord>> {
    let id = request.pending.instance_id.as_str().to_owned();
    let mut client = store.client().await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    let Some(row) = sql::query_opt(
        &tx,
        "
        SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
        FROM instances
        WHERE instance_id = $1 AND generation = $2 AND state = 'draining'
        FOR UPDATE
        ",
        &[&id, &generation_to_i64(request.expected_generation)?],
    )
    .await?
    else {
        return Ok(None);
    };
    let current = instance_from_row(&row)?;
    if let Some(revived) = revive_draining(&tx, &current, request).await? {
        tx.commit().await.map_err(map_postgres_error)?;
        return Ok(Some(revived));
    }
    let previous_backend: Option<i64> = sql::query_one(
        &tx,
        "
        SELECT max(backend_generation) FROM materializations
        WHERE instance_id = $1 AND cluster_id = $2 AND namespace = $3
        ",
        &[
            &id,
            &request.pending.target.cluster_id(),
            &request.pending.target.namespace(),
        ],
    )
    .await?
    .get(0);
    if let Some(previous) = previous_backend {
        let next = previous
            .checked_add(1)
            .ok_or_else(|| StoreError::invalid_argument("backend generation exhausted"))?;
        request.pending.backend_generation = std::cmp::max(
            request.pending.backend_generation,
            super::mapping::backend_generation_from_i64(next)?,
        );
    }
    let pending = &request.pending;
    let inserted = sql::execute(
        &tx,
        "
        INSERT INTO deferred_wake_intents (instance_id, instance_generation, projection_generation,
            cluster_id, namespace, backend_generation, rendered_objects, exclusivity_keys)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (instance_id) DO NOTHING
        ",
        &[
            &id,
            &generation_to_i64(pending.instance_generation)?,
            &generation_to_i64(pending.projection_generation)?,
            &pending.target.cluster_id(),
            &pending.target.namespace(),
            &super::mapping::backend_generation_to_i64(pending.backend_generation)?,
            &rendered_objects_to_json(&pending.rendered_objects),
            &rendered_exclusivity_keys_to_json(&pending.exclusivity_keys),
        ],
    )
    .await?;
    if inserted == 0 {
        let row = sql::query_one(
            &tx,
            "SELECT cluster_id, namespace FROM deferred_wake_intents WHERE instance_id = $1",
            &[&id],
        )
        .await?;
        if row.get::<_, String>("cluster_id") != pending.target.cluster_id()
            || row.get::<_, String>("namespace") != pending.target.namespace()
        {
            return Err(StoreError::invalid_argument(
                "wake already accepted for another target",
            ));
        }
    }
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(Some(current))
}

/// Explains a wake neither path accepted, from one read of the instance and
/// its projections. A replay of an accepted wake succeeds here.
async fn refused_wake(
    store: &PostgresStore,
    request: &AcceptWakeRequest,
    draining: bool,
) -> StoreResult<InstanceRecord> {
    let client = store.client().await?;
    let Some(row) = sql::query_opt(
        &client,
        "
        SELECT instance_id, workload_class_id, workload_class_version, values, state, generation,
            EXISTS (SELECT 1 FROM materializations m WHERE m.instance_id = i.instance_id
                AND m.cluster_id = $2 AND m.namespace = $3 AND m.state = 'pending') AS pending_here,
            EXISTS (SELECT 1 FROM materializations m WHERE m.instance_id = i.instance_id
                AND m.state <> 'deleted') AS active
        FROM instances i
        WHERE instance_id = $1
        ",
        &[
            &request.pending.instance_id.as_str(),
            &request.pending.target.cluster_id(),
            &request.pending.target.namespace(),
        ],
    )
    .await?
    else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let current = instance_from_row(&row)?;
    if current.state == InstanceState::Waking
        && current.generation == request.pending.instance_generation
    {
        return if row.get("pending_here") {
            Ok(current)
        } else {
            Err(StoreError::invalid_argument(
                "wake already accepted for another target",
            ))
        };
    }
    if current.generation != request.expected_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_generation,
            actual: current.generation,
        });
    }
    match current.state {
        InstanceState::Draining if !draining => Err(StoreError::invalid_argument(
            "accepted wake projection does not match its next incarnation",
        )),
        InstanceState::Cold | InstanceState::Failed if draining => {
            Err(StoreError::invalid_argument(
                "accepted wake projection does not match its next incarnation",
            ))
        }
        InstanceState::Cold | InstanceState::Failed if row.get::<_, bool>("active") => Err(
            StoreError::unavailable("previous materialization cleanup is still pending"),
        ),
        InstanceState::Cold | InstanceState::Failed | InstanceState::Draining => Err(
            StoreError::unavailable("the instance changed during wake acceptance"),
        ),
        _ => Err(StoreError::invalid_argument(
            "wake acceptance requires cold, failed, or draining instance",
        )),
    }
}

/// Returns a draining instance to Running on its existing projection. Teardown
/// starts only once the drain deadline has passed, and a claim needs that same
/// deadline under this row lock, so a revived projection is one no driver has
/// touched. The projection generation the sidecar holds stays as it is; the
/// Ready transition restarts the activation floor.
async fn revive_draining(
    tx: &impl GenericClient,
    current: &InstanceRecord,
    request: &AcceptWakeRequest,
) -> StoreResult<Option<InstanceRecord>> {
    let id = current.id.as_str();
    let running = current.generation.next();
    let running_db = generation_to_i64(running)?;
    let drained_db = generation_to_i64(current.generation)? - 1;
    let revived = sql::execute(tx, "
            UPDATE materializations
            SET state = 'ready',
                instance_generation = $5,
                backend_generation = backend_generation + 1
            WHERE instance_id = $1
                AND cluster_id = $2
                AND namespace = $3
                AND state = 'deleting'
                AND instance_generation = $4
                AND backend_uri IS NOT NULL
                AND reconcile_owner IS NULL
                AND drain_not_before_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
            ",
            &[
                &id,
                &request.pending.target.cluster_id(),
                &request.pending.target.namespace(),
                &drained_db,
                &running_db,
            ],).await?;
    if revived == 0 {
        return Ok(None);
    }
    let row = sql::query_one(tx, "
            UPDATE instances
            SET state = 'running', generation = $2
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&id, &running_db],).await?;
    instance_from_row(&row).map(Some)
}

/// Called inside the sleep-finalization transaction, after old reservations are
/// released. There is no crash point between finishing the drain and queuing wake.
pub(crate) async fn activate_deferred_wake(
    tx: &impl GenericClient,
    id: &str,
) -> StoreResult<Option<InstanceRecord>> {
    let intent = sql::query_opt(
        tx,
        "DELETE FROM deferred_wake_intents WHERE instance_id = $1 RETURNING *",
        &[&id],
    )
    .await?;
    let Some(intent) = intent else {
        return Ok(None);
    };
    let generation: i64 = intent.get("instance_generation");
    let row = sql::query_opt(tx, "
            UPDATE instances
            SET state = 'waking', generation = $2
            WHERE instance_id = $1
            AND state = 'cold'
            AND generation = $2::bigint - 1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ", &[&id,&generation]).await?
        .ok_or_else(|| StoreError::invalid_argument("deferred wake no longer matches drained incarnation"))?;
    let mut pending = crate::materialization::RecordMaterializationRequest::new(
        crate::ids::InstanceId::new(id).map_err(|e| StoreError::internal(e.to_string()))?,
        super::mapping::generation_from_i64(generation)?,
        crate::materialization::MaterializationTarget::new(
            intent.get::<_, String>("cluster_id"),
            intent.get::<_, String>("namespace"),
        )
        .map_err(|e| StoreError::internal(e.to_string()))?,
        MaterializationState::Pending,
        super::mapping::backend_generation_from_i64(intent.get("backend_generation"))?,
    );
    pending.projection_generation =
        super::mapping::generation_from_i64(intent.get("projection_generation"))?;
    pending.rendered_objects =
        super::mapping::rendered_objects_from_json(intent.get("rendered_objects"))?;
    pending.exclusivity_keys =
        super::mapping::rendered_exclusivity_keys_from_json(intent.get("exclusivity_keys"))?;
    upsert_materialization(tx, &pending).await?;
    Ok(Some(instance_from_row(&row)?))
}

pub(crate) async fn request_instance_deletion(
    store: &PostgresStore,
    request: crate::instance::RequestInstanceDeletion,
) -> StoreResult<bool> {
    let mut client = store.client().await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    generation_to_i64(request.expected_generation)?;
    let id = request.instance_id.as_str();
    let row = sql::query_opt(
        &tx,
        "SELECT state, generation FROM instances WHERE instance_id = $1 FOR UPDATE",
        &[&id],
    )
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let actual = super::mapping::generation_from_i64(row.get("generation"))?;
    let already_accepted =
        row.get::<_, String>("state") == "deleting" && actual == request.expected_generation.next();
    if actual != request.expected_generation && !already_accepted {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_generation,
            actual,
        });
    }
    if row.get::<_, String>("state") != "deleting" {
        generation_to_i64(actual.next())?;
        sql::execute(
            &tx,
            "
            UPDATE instances
            SET state = 'deleting', generation = generation + 1
            WHERE instance_id = $1
            ",
            &[&id],
        )
        .await?;
    }
    sql::execute(
        &tx,
        "DELETE FROM deferred_wake_intents WHERE instance_id = $1",
        &[&id],
    )
    .await?;
    // Preserve any sleep deadline and current lease: deletion does not steal
    // established streams' grace or let another worker overlap a held lease.
    sql::execute(
        &tx,
        "
            UPDATE materializations
            SET state = 'deleting'
            WHERE instance_id = $1
            AND state NOT IN ('deleted','deleting')
            ",
        &[&id],
    )
    .await?;
    tx.commit().await.map_err(map_postgres_error)?;
    crate::runtime_work::signal_lifecycle_work();
    Ok(true)
}

pub(crate) async fn finalize_instance_deletions(
    store: &PostgresStore,
    limit: usize,
) -> StoreResult<usize> {
    let limit = i64::try_from(limit)
        .map_err(|_| StoreError::invalid_argument("deletion batch too large"))?;
    let client = store.client().await?;
    let count = sql::execute(
        &client,
        "
            WITH completed AS (SELECT instance_id
            FROM instances i
            WHERE state = 'deleting'
            AND NOT EXISTS (SELECT 1
            FROM materializations m
            WHERE m.instance_id = i.instance_id
            AND m.state <> 'deleted')
            ORDER BY instance_id
            LIMIT $1
            FOR UPDATE SKIP LOCKED) DELETE
            FROM instances
            WHERE instance_id IN (SELECT instance_id
            FROM completed)
            ",
        &[&limit],
    )
    .await?;
    Ok(count as usize)
}
