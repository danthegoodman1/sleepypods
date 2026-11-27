//! Durable lifecycle acceptance. Kubernetes effects belong exclusively to the reconciler.
use deadpool_postgres::GenericClient;

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{
        generation_to_i64, instance_from_row, rendered_exclusivity_keys_to_json,
        rendered_objects_to_json,
    },
    materialization_ops::upsert_materialization,
};
use crate::{
    instance::{InstanceRecord, InstanceState},
    materialization::{AcceptWakeRequest, MaterializationState},
    store::{StoreError, StoreResult},
};

pub(crate) async fn accept_wake(
    store: &PostgresStore,
    mut request: AcceptWakeRequest,
) -> StoreResult<InstanceRecord> {
    let mut client = store.client().await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    let id = request.pending.instance_id.as_str();
    let row = tx
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&id],
        )
        .await
        .map_err(map_postgres_error)?
        .ok_or(StoreError::NotFound {
            resource: "instance",
        })?;
    let current = instance_from_row(&row)?;
    // A replay after commit observes the same accepted work, including if the
    // caller lost the response. It may never replace an accepted projection.
    if current.state == InstanceState::Waking
        && current.generation == request.pending.instance_generation
    {
        let matching = tx
            .query_one(
                "
            SELECT EXISTS(SELECT 1
            FROM materializations
            WHERE instance_id = $1
            AND cluster_id = $2
            AND namespace = $3
            AND state = 'pending')
            ",
                &[
                    &id,
                    &request.pending.target.cluster_id(),
                    &request.pending.target.namespace(),
                ],
            )
            .await
            .map_err(map_postgres_error)?
            .get::<_, bool>(0);
        if !matching {
            return Err(StoreError::invalid_argument(
                "wake already accepted for another target",
            ));
        }
        tx.commit().await.map_err(map_postgres_error)?;
        return Ok(current);
    }
    if current.generation != request.expected_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_generation,
            actual: current.generation,
        });
    }
    if !matches!(
        current.state,
        InstanceState::Cold | InstanceState::Failed | InstanceState::Draining
    ) {
        return Err(StoreError::invalid_argument(
            "wake acceptance requires cold, failed, or draining instance",
        ));
    }
    if current.state != InstanceState::Draining && tx.query_one("SELECT EXISTS(SELECT 1 FROM materializations WHERE instance_id = $1 AND state <> 'deleted')", &[&id]).await.map_err(map_postgres_error)?.get::<_,bool>(0) {
        return Err(StoreError::unavailable("previous materialization cleanup is still pending"));
    }
    let expected_waking = if current.state == InstanceState::Draining {
        current.generation.next().next()
    } else {
        current.generation.next()
    };
    if request.pending.state != MaterializationState::Pending
        || request.pending.instance_generation != expected_waking
        || request.pending.projection_generation != expected_waking.next()
        || request.pending.backend.is_some()
    {
        return Err(StoreError::invalid_argument(
            "accepted wake projection does not match its next incarnation",
        ));
    }
    let previous_backend: Option<i64> = tx
        .query_one(
            "
            SELECT MAX(backend_generation)
            FROM materializations
            WHERE instance_id = $1
            AND cluster_id = $2
            AND namespace = $3
            ",
            &[
                &id,
                &request.pending.target.cluster_id(),
                &request.pending.target.namespace(),
            ],
        )
        .await
        .map_err(map_postgres_error)?
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
    if current.state == InstanceState::Draining {
        if let Some(revived) = revive_draining(&tx, &current, &request).await? {
            tx.commit().await.map_err(map_postgres_error)?;
            return Ok(revived);
        }
        let generation = generation_to_i64(request.pending.instance_generation)?;
        let projection = generation_to_i64(request.pending.projection_generation)?;
        let backend =
            super::mapping::backend_generation_to_i64(request.pending.backend_generation)?;
        let refs = rendered_objects_to_json(&request.pending.rendered_objects);
        let keys = rendered_exclusivity_keys_to_json(&request.pending.exclusivity_keys);
        let inserted = tx.execute("
            INSERT INTO deferred_wake_intents(instance_id, instance_generation, projection_generation, cluster_id, namespace, backend_generation, rendered_objects, exclusivity_keys)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (instance_id)
            DO NOTHING
            ", &[&id,&generation,&projection,&request.pending.target.cluster_id(),&request.pending.target.namespace(),&backend,&refs,&keys]).await.map_err(map_postgres_error)?;
        if inserted == 0 {
            let row = tx.query_one("SELECT cluster_id, namespace FROM deferred_wake_intents WHERE instance_id = $1", &[&id]).await.map_err(map_postgres_error)?;
            if row.get::<_, String>("cluster_id") != request.pending.target.cluster_id()
                || row.get::<_, String>("namespace") != request.pending.target.namespace()
            {
                return Err(StoreError::invalid_argument(
                    "wake already accepted for another target",
                ));
            }
        }
        tx.commit().await.map_err(map_postgres_error)?;
        return Ok(current);
    }
    let generation = generation_to_i64(expected_waking)?;
    let row = tx.query_one("
            UPDATE instances
            SET state = 'waking', generation = $2
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ", &[&id,&generation]).await.map_err(map_postgres_error)?;
    upsert_materialization(&tx, &request.pending).await?;
    tx.commit().await.map_err(map_postgres_error)?;
    crate::runtime_work::signal_lifecycle_work();
    instance_from_row(&row)
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
    let revived = tx
        .execute(
            "
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
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    if revived == 0 {
        return Ok(None);
    }
    let row = tx
        .query_one(
            "
            UPDATE instances
            SET state = 'running', generation = $2
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&id, &running_db],
        )
        .await
        .map_err(map_postgres_error)?;
    instance_from_row(&row).map(Some)
}

/// Called inside the sleep-finalization transaction, after old reservations are
/// released. There is no crash point between finishing the drain and queuing wake.
pub(crate) async fn activate_deferred_wake(
    tx: &impl GenericClient,
    id: &str,
) -> StoreResult<Option<InstanceRecord>> {
    let intent = tx
        .query_opt(
            "DELETE FROM deferred_wake_intents WHERE instance_id = $1 RETURNING *",
            &[&id],
        )
        .await
        .map_err(map_postgres_error)?;
    let Some(intent) = intent else {
        return Ok(None);
    };
    let generation: i64 = intent.get("instance_generation");
    let row = tx.query_opt("
            UPDATE instances
            SET state = 'waking', generation = $2
            WHERE instance_id = $1
            AND state = 'cold'
            AND generation = $2::bigint - 1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ", &[&id,&generation]).await.map_err(map_postgres_error)?
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
    let row = tx
        .query_opt(
            "SELECT state, generation FROM instances WHERE instance_id = $1 FOR UPDATE",
            &[&id],
        )
        .await
        .map_err(map_postgres_error)?;
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
        tx.execute(
            "
            UPDATE instances
            SET state = 'deleting', generation = generation + 1
            WHERE instance_id = $1
            ",
            &[&id],
        )
        .await
        .map_err(map_postgres_error)?;
    }
    tx.execute(
        "DELETE FROM deferred_wake_intents WHERE instance_id = $1",
        &[&id],
    )
    .await
    .map_err(map_postgres_error)?;
    // Preserve any sleep deadline and current lease: deletion does not steal
    // established streams' grace or let another worker overlap a held lease.
    tx.execute(
        "
            UPDATE materializations
            SET state = 'deleting'
            WHERE instance_id = $1
            AND state NOT IN ('deleted','deleting')
            ",
        &[&id],
    )
    .await
    .map_err(map_postgres_error)?;
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
    let count = client
        .execute(
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
        .await
        .map_err(map_postgres_error)?;
    Ok(count as usize)
}
