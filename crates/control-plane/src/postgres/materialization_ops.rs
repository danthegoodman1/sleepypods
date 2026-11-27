use std::time::Duration;

use deadpool_postgres::GenericClient;

use crate::{
    ids::Generation,
    materialization::{
        BeginSleepRequest, BeginSleepResult, ClaimNextMaterializationRequest,
        CompleteWakeReconciliationRequest, CompleteWakeResult,
        DeleteMaterializationReconciliationRequest, FinalizeSleepReconciliationRequest,
        FinalizeSleepResult, ForceDeleteMaterializationRequest, ForceReleaseExclusivityKeyRequest,
        ForceReleaseExclusivityKeyResult, LoadActiveMaterializationRequest,
        LoadMaterializationRequest, LoadReadyMaterializationRequest,
        MaterializationBacklogOperationalMetrics, MaterializationHeldKeysOperationalMetrics,
        MaterializationOperationalMetrics, MaterializationRecord, MaterializationState,
        RecordMaterializationRequest, ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{
        backend_generation_to_i64, generation_to_i64, instance_from_row, materialization_from_row,
        materialization_id, materialization_state_from_db, materialization_state_to_db,
        rendered_exclusivity_keys_to_json, rendered_objects_to_json,
    },
    sql,
};

pub(crate) async fn load_ready_materialization(
    store: &PostgresStore,
    request: LoadReadyMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;
    let instance_id = request.instance_id.as_str();
    let instance_generation = generation_to_i64(request.instance_generation)?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let row = sql::query_opt(&client, "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE instance_id = $1
                AND instance_generation = $2
                AND cluster_id = $3
                AND namespace = $4
                AND state = 'ready'
            ",
            &[&instance_id, &instance_generation, &cluster_id, &namespace],).await?;

    row.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn load_active_materialization(
    store: &PostgresStore,
    request: LoadActiveMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;

    load_active_materialization_from_client(&client, &request).await
}

pub(crate) async fn load_materialization(
    store: &PostgresStore,
    request: LoadMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;
    let materialization_id = request.materialization_id.as_str();
    let row = sql::query_opt(&client, "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE materialization_id = $1
            ",
            &[&materialization_id],).await?;

    row.as_ref().map(materialization_from_row).transpose()
}

fn normalize_exclusivity_keys(keys: &mut Vec<crate::workload::RenderedExclusivityKey>) {
    keys.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value))
    });
    keys.dedup();
}

/// Begins a drain in one transaction: the instance moves from Running to
/// Draining, and its projection to Deleting with the drain grace as its
/// deadline. With a minimum Ready age the projection must be this
/// generation's Ready one and have been Ready that long; the store refuses
/// with the time left otherwise, and changes nothing.
pub(crate) async fn begin_sleep(
    store: &PostgresStore,
    request: BeginSleepRequest,
) -> StoreResult<BeginSleepResult> {
    let instance_id = request.instance_id.as_str();
    let running = generation_to_i64(request.expected_running_generation)?;
    let draining = generation_to_i64(request.expected_running_generation.next())?;
    let drain_grace = i64::try_from(request.drain_grace_timeout.as_millis())
        .map_err(|_| StoreError::invalid_argument("drain grace timeout is too large"))?;
    let minimum_ready_age = request
        .minimum_ready_age
        .map(|age| {
            i64::try_from(age.as_millis()).map_err(|_| {
                StoreError::invalid_argument("minimum Ready age exceeds database range")
            })
        })
        .transpose()?;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let Some(row) = sql::query_opt(
        &transaction,
        "
        UPDATE instances
        SET state = 'draining', generation = $3
        WHERE instance_id = $1 AND generation = $2 AND state = 'running'
        RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
        ",
        &[&instance_id, &running, &draining],
    )
    .await?
    else {
        return Err(refused_transition(
            &transaction,
            instance_id,
            request.expected_running_generation,
            "only a running instance drains",
        )
        .await);
    };
    let instance = instance_from_row(&row)?;
    let materialization = sql::query_opt(
        &transaction,
        "
        UPDATE materializations
        SET state = 'deleting',
            reconcile_owner = NULL,
            reconcile_lease_expires_at_unix_millis = NULL,
            drain_not_before_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $5
        WHERE instance_id = $1
            AND cluster_id = $2
            AND namespace = $3
            AND instance_generation = $4
            AND state <> 'deleted'
            AND ($6::bigint IS NULL OR (state = 'ready'
                AND (extract(epoch from clock_timestamp()) * 1000)::bigint - state_entered_at_unix_millis >= $6))
        RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
            namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
            exclusivity_keys, reconcile_owner,
            reconcile_lease_expires_at_unix_millis, reconcile_attempt
        ",
        &[
            &instance_id,
            &request.target.cluster_id(),
            &request.target.namespace(),
            &running,
            &drain_grace,
            &minimum_ready_age,
        ],
    )
    .await?;
    let materialization = match materialization {
        Some(row) => Some(materialization_from_row(&row)?),
        None => {
            refused_drain(&transaction, &request, minimum_ready_age).await?;
            None
        }
    };
    transaction.commit().await.map_err(map_postgres_error)?;
    Ok(BeginSleepResult {
        instance,
        materialization,
    })
}

/// Says why a drain left the projection alone, under the instance lock the
/// drain holds. A drain without a minimum Ready age proceeds when the
/// instance has no projection on the target.
async fn refused_drain(
    client: &impl GenericClient,
    request: &BeginSleepRequest,
    minimum_ready_age: Option<i64>,
) -> StoreResult<()> {
    let active = sql::query_opt(
        client,
        "
        SELECT state, instance_generation,
            (extract(epoch from clock_timestamp()) * 1000)::bigint - state_entered_at_unix_millis AS age
        FROM materializations
        WHERE instance_id = $1 AND cluster_id = $2 AND namespace = $3 AND state <> 'deleted'
        ",
        &[
            &request.instance_id.as_str(),
            &request.target.cluster_id(),
            &request.target.namespace(),
        ],
    )
    .await?;
    let generation = |row: &tokio_postgres::Row| {
        super::mapping::generation_from_i64(row.get("instance_generation"))
    };
    let Some(minimum) = minimum_ready_age else {
        return match active {
            Some(row) => Err(StoreError::GenerationConflict {
                expected: request.expected_running_generation,
                actual: generation(&row)?,
            }),
            None => Ok(()),
        };
    };
    match active {
        Some(row)
            if row.get::<_, &str>("state") == "ready"
                && generation(&row)? == request.expected_running_generation =>
        {
            let age: i64 = row.get("age");
            Err(StoreError::SleepDeferred {
                retry_after: Duration::from_millis(minimum.saturating_sub(age).max(1) as u64),
            })
        }
        _ => Err(StoreError::invalid_argument(
            "automatic sleep requires this generation's Ready materialization",
        )),
    }
}

/// The error for an instance transition whose conditional update matched no
/// row: the instance is gone, has moved on, or is in a state the transition
/// does not leave.
async fn refused_transition(
    client: &impl GenericClient,
    instance_id: &str,
    expected: Generation,
    wrong_state: &'static str,
) -> StoreError {
    let current = match sql::query_opt(
        client,
        "SELECT generation FROM instances WHERE instance_id = $1",
        &[&instance_id],
    )
    .await
    {
        Ok(current) => current,
        Err(error) => return error,
    };
    let Some(current) = current else {
        return StoreError::NotFound {
            resource: "instance",
        };
    };
    match super::mapping::generation_from_i64(current.get("generation")) {
        Ok(actual) if actual != expected => StoreError::GenerationConflict { expected, actual },
        Ok(_) => StoreError::invalid_argument(wrong_state),
        Err(error) => error,
    }
}

pub(crate) async fn load_materialization_operational_metrics(
    store: &PostgresStore,
) -> StoreResult<MaterializationOperationalMetrics> {
    let client = store.client().await?;
    // Backlog age subtracts a database-written timestamp, so the database
    // clock supplies the other operand.
    let backlog_rows = sql::query(
        &client,
        "
            SELECT state,
                COUNT(*)::bigint AS backlog_count,
                GREATEST(0::bigint, (extract(epoch from clock_timestamp()) * 1000)::bigint
                    - MIN(state_entered_at_unix_millis)) AS oldest_age_millis
            FROM materializations
            WHERE state IN ('pending', 'deleting')
            GROUP BY state
            ORDER BY state
            ",
        &[],
    )
    .await?;

    let held_key_rows = sql::query(
        &client,
        "
            SELECT state,
                COALESCE(SUM(jsonb_array_length(exclusivity_keys)), 0)::bigint
                    AS exclusivity_keys_held
            FROM materializations
            WHERE state <> 'deleted'
            GROUP BY state
            ORDER BY state
            ",
        &[],
    )
    .await?;

    let mut backlog_states = Vec::with_capacity(backlog_rows.len());
    for row in backlog_rows {
        let state: String = row.get("state");
        let backlog_count: i64 = row.get("backlog_count");
        let age: i64 = row.get("oldest_age_millis");
        backlog_states.push(MaterializationBacklogOperationalMetrics::new(
            materialization_state_from_db(&state)?,
            u64::try_from(backlog_count)
                .map_err(|_| StoreError::internal("materialization backlog count was negative"))?,
            Some(Duration::from_millis(u64::try_from(age).map_err(|_| {
                StoreError::internal("materialization oldest age was negative")
            })?)),
        ));
    }

    let mut held_key_states = Vec::with_capacity(held_key_rows.len());
    for row in held_key_rows {
        let state: String = row.get("state");
        let exclusivity_keys_held: i64 = row.get("exclusivity_keys_held");
        held_key_states.push(MaterializationHeldKeysOperationalMetrics::new(
            materialization_state_from_db(&state)?,
            u64::try_from(exclusivity_keys_held)
                .map_err(|_| StoreError::internal("exclusivity key count was negative"))?,
        ));
    }

    let row = sql::query_one(
        &client,
        "SELECT count(*) AS blocked FROM materializations
             WHERE state IN ('pending', 'deleting') AND failure_kind IN ('permanent', 'deadline')
                AND NOT failure_requires_cleanup",
        &[],
    )
    .await?;
    let mut metrics = MaterializationOperationalMetrics::new(backlog_states, held_key_states);
    metrics.blocked_failures = row.get::<_, i64>("blocked") as u64;
    Ok(metrics)
}

/// Claims the due materialization in one state that has waited longest.
/// `SKIP LOCKED` lets concurrent claimers take different rows without waiting.
/// Eligibility and the new expiry read the database clock, so a lease lasts its
/// requested TTL measured by the clock every other process compares against.
pub(crate) async fn claim_next_materialization_reconciliation(
    store: &PostgresStore,
    request: ClaimNextMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    validate_lease_owner(&request.owner)?;
    let lease_ttl_millis = lease_ttl_millis(request.lease_ttl)?;
    let client = store.client().await?;
    let owner = request.owner.as_str();
    let state = materialization_state_to_db(request.state);
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let row = sql::query_opt(&client, "
            UPDATE materializations
            SET reconcile_owner = $1,
                reconcile_lease_expires_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $2,
                reconcile_attempt = reconcile_attempt + 1
            WHERE materialization_id = (
                SELECT materialization_id
                FROM materializations
                WHERE state = $3
                    AND state IN ('pending', 'deleting')
                    AND cluster_id = $4
                    AND namespace = $5
                    AND (failure_kind IS NULL OR failure_kind = 'transient' OR failure_requires_cleanup)
                    AND next_attempt_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                    AND drain_not_before_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                    AND (
                        reconcile_owner IS NULL
                        OR reconcile_lease_expires_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                    )
                ORDER BY next_attempt_at_unix_millis, materialization_id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            ",
            &[&owner, &lease_ttl_millis, &state, &cluster_id, &namespace],).await?;

    row.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn renew_materialization_reconciliation_lease(
    store: &PostgresStore,
    request: RenewMaterializationReconciliationLeaseRequest,
) -> StoreResult<bool> {
    let client = store.client().await?;
    validate_lease_owner(&request.owner)?;
    let lease_ttl_millis = lease_ttl_millis(request.lease_ttl)?;
    let materialization_id = request.materialization_id.as_str();
    let owner = request.owner.as_str();
    let attempt = lease_attempt(request.attempt)?;
    let generation = generation_to_i64(request.instance_generation)?;
    let expected_state = materialization_state_to_db(request.expected_state);
    // Renewal restarts the TTL from the database clock, exactly like a claim.
    let updated = sql::execute(&client, "
            UPDATE materializations
            SET reconcile_lease_expires_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $3
            WHERE materialization_id = $1
                AND reconcile_owner = $2
                AND reconcile_attempt = $4
                AND instance_generation = $5
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND state IN ('pending', 'deleting') AND state = $6
            ",
            &[&materialization_id, &owner, &lease_ttl_millis, &attempt, &generation, &expected_state],).await?;

    Ok(updated == 1)
}

pub(crate) async fn release_materialization_reconciliation_lease(
    store: &PostgresStore,
    request: ReleaseMaterializationReconciliationLeaseRequest,
) -> StoreResult<bool> {
    let client = store.client().await?;
    validate_lease_owner(&request.owner)?;
    let materialization_id = request.materialization_id.as_str();
    let owner = request.owner.as_str();
    let attempt = lease_attempt(request.attempt)?;
    let generation = generation_to_i64(request.instance_generation)?;
    let updated = sql::execute(&client, "
            UPDATE materializations
            SET reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                next_attempt_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND reconcile_owner = $2
                AND reconcile_attempt = $3
                AND instance_generation = $4
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND state IN ('pending', 'deleting')
            ",
            &[&materialization_id, &owner, &attempt, &generation],).await?;

    Ok(updated == 1)
}

/// Completes a claimed wake in one transaction: the instance moves from
/// Waking to Running, and the claimed Pending projection to Ready with its
/// backend, while the claim's lease holds.
pub(crate) async fn complete_wake_reconciliation(
    store: &PostgresStore,
    request: CompleteWakeReconciliationRequest,
) -> StoreResult<CompleteWakeResult> {
    validate_lease_owner(&request.lease_owner)?;
    let mut complete = request.complete;
    normalize_exclusivity_keys(&mut complete.exclusivity_keys);
    let lease = ReconciliationLease {
        materialization_id: request.materialization_id.as_str(),
        owner: &request.lease_owner,
        attempt: lease_attempt(request.attempt)?,
        state: MaterializationState::Pending,
        instance_id: complete.instance_id.as_str(),
        instance_generation: complete.expected_waking_generation,
        target: &complete.target,
    };
    let waking = generation_to_i64(complete.expected_waking_generation)?;
    let running = generation_to_i64(complete.expected_waking_generation.next())?;
    let backend_generation = backend_generation_to_i64(complete.backend_generation)?;
    let rendered_objects = rendered_objects_to_json(&complete.rendered_objects);
    let exclusivity_keys = rendered_exclusivity_keys_to_json(&complete.exclusivity_keys);
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let Some(row) = sql::query_opt(
        &transaction,
        "
        UPDATE instances
        SET state = 'running', generation = $3
        WHERE instance_id = $1 AND generation = $2 AND state = 'waking'
        RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
        ",
        &[&lease.instance_id, &waking, &running],
    )
    .await?
    else {
        lease.refused(&transaction).await?;
        return Err(refused_transition(
            &transaction,
            lease.instance_id,
            complete.expected_waking_generation,
            "only a waking instance becomes running",
        )
        .await);
    };
    let instance = instance_from_row(&row)?;
    let Some(row) = sql::query_opt(
        &transaction,
        "
        UPDATE materializations
        SET state = 'ready',
            instance_generation = $9,
            backend_uri = $10,
            pod_backend_uri = $11,
            backend_generation = $12,
            rendered_objects = $13,
            exclusivity_keys = $14,
            reconcile_owner = NULL,
            reconcile_lease_expires_at_unix_millis = NULL
        WHERE materialization_id = $1
            AND state = $2
            AND instance_id = $3
            AND instance_generation = $4
            AND cluster_id = $5
            AND namespace = $6
            AND reconcile_owner = $7
            AND reconcile_attempt = $8
            AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
            AND backend_generation <= $12
        RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
            namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
            exclusivity_keys, reconcile_owner,
            reconcile_lease_expires_at_unix_millis, reconcile_attempt
        ",
        &[
            &lease.materialization_id,
            &materialization_state_to_db(lease.state),
            &lease.instance_id,
            &waking,
            &lease.target.cluster_id(),
            &lease.target.namespace(),
            &lease.owner,
            &lease.attempt,
            &running,
            &complete.backend.uri(),
            &complete.pod_backend.as_ref().map(|backend| backend.uri()),
            &backend_generation,
            &rendered_objects,
            &exclusivity_keys,
        ],
    )
    .await?
    else {
        lease.refused(&transaction).await?;
        return Err(backend_generation_rewind_error(
            &transaction,
            lease.instance_id,
            lease.target.cluster_id(),
            lease.target.namespace(),
            complete.backend_generation,
        )
        .await);
    };
    let materialization = materialization_from_row(&row)?;
    transaction.commit().await.map_err(map_postgres_error)?;
    Ok(CompleteWakeResult {
        instance,
        materialization,
    })
}

/// Finishes a claimed drain in one transaction: the instance moves from
/// Draining to Cold, its projection to Deleted with its reservations
/// released, and a wake deferred during the drain becomes Pending.
pub(crate) async fn finalize_sleep_reconciliation(
    store: &PostgresStore,
    request: FinalizeSleepReconciliationRequest,
) -> StoreResult<FinalizeSleepResult> {
    validate_lease_owner(&request.lease_owner)?;
    let finalize = request.finalize;
    let lease = ReconciliationLease {
        materialization_id: request.materialization_id.as_str(),
        owner: &request.lease_owner,
        attempt: lease_attempt(request.attempt)?,
        state: MaterializationState::Deleting,
        instance_id: finalize.instance_id.as_str(),
        instance_generation: previous_generation(finalize.expected_draining_generation)?,
        target: &finalize.target,
    };
    let draining = generation_to_i64(finalize.expected_draining_generation)?;
    let cold = generation_to_i64(finalize.expected_draining_generation.next())?;
    let projected = generation_to_i64(lease.instance_generation)?;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let Some(row) = sql::query_opt(
        &transaction,
        "
        UPDATE instances
        SET state = 'cold', generation = $3
        WHERE instance_id = $1 AND generation = $2 AND state = 'draining'
        RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
        ",
        &[&lease.instance_id, &draining, &cold],
    )
    .await?
    else {
        lease.refused(&transaction).await?;
        return Err(refused_transition(
            &transaction,
            lease.instance_id,
            finalize.expected_draining_generation,
            "only a draining instance becomes cold",
        )
        .await);
    };
    let instance = instance_from_row(&row)?;
    let Some(row) = sql::query_opt(
        &transaction,
        "
        UPDATE materializations
        SET state = 'deleted',
            instance_generation = $9,
            backend_uri = NULL,
            pod_backend_uri = NULL,
            rendered_objects = '[]'::jsonb,
            exclusivity_keys = '[]'::jsonb,
            reconcile_owner = NULL,
            reconcile_lease_expires_at_unix_millis = NULL
        WHERE materialization_id = $1
            AND state = $2
            AND instance_id = $3
            AND instance_generation = $4
            AND cluster_id = $5
            AND namespace = $6
            AND reconcile_owner = $7
            AND reconcile_attempt = $8
            AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
        RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
            namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
            exclusivity_keys, reconcile_owner,
            reconcile_lease_expires_at_unix_millis, reconcile_attempt
        ",
        &[
            &lease.materialization_id,
            &materialization_state_to_db(lease.state),
            &lease.instance_id,
            &projected,
            &lease.target.cluster_id(),
            &lease.target.namespace(),
            &lease.owner,
            &lease.attempt,
            &cold,
        ],
    )
    .await?
    else {
        lease.refused(&transaction).await?;
        return Err(StoreError::internal(
            "drain finalization matched its lease but not its projection",
        ));
    };
    let materialization = materialization_from_row(&row)?;
    let instance = super::lifecycle_ops::activate_deferred_wake(&transaction, lease.instance_id)
        .await?
        .unwrap_or(instance);
    transaction.commit().await.map_err(map_postgres_error)?;
    // A wake deferred during the drain is now Pending.
    crate::runtime_work::signal_lifecycle_work();
    Ok(FinalizeSleepResult {
        instance,
        materialization: Some(materialization),
    })
}

/// Marks a claimed projection Deleted once its cleanup has proven every
/// object absent. One statement checks the lease and releases the
/// reservations.
pub(crate) async fn delete_materialization_reconciliation(
    store: &PostgresStore,
    request: DeleteMaterializationReconciliationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    validate_lease_owner(&request.lease_owner)?;
    let lease = ReconciliationLease {
        materialization_id: request.materialization_id.as_str(),
        owner: &request.lease_owner,
        attempt: lease_attempt(request.attempt)?,
        state: request.expected_state,
        instance_id: request.instance_id.as_str(),
        instance_generation: request.instance_generation,
        target: &request.target,
    };
    let client = store.client().await?;
    let row = sql::query_opt(
        &client,
        "
        UPDATE materializations
        SET state = 'deleted',
            backend_uri = NULL,
            pod_backend_uri = NULL,
            rendered_objects = '[]'::jsonb,
            exclusivity_keys = '[]'::jsonb,
            reconcile_owner = NULL,
            reconcile_lease_expires_at_unix_millis = NULL
        WHERE materialization_id = $1
            AND state = $2
            AND instance_id = $3
            AND instance_generation = $4
            AND cluster_id = $5
            AND namespace = $6
            AND reconcile_owner = $7
            AND reconcile_attempt = $8
            AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
        RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
            namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
            exclusivity_keys, reconcile_owner,
            reconcile_lease_expires_at_unix_millis, reconcile_attempt
        ",
        &[
            &lease.materialization_id,
            &materialization_state_to_db(lease.state),
            &lease.instance_id,
            &generation_to_i64(lease.instance_generation)?,
            &lease.target.cluster_id(),
            &lease.target.namespace(),
            &lease.owner,
            &lease.attempt,
        ],
    )
    .await?;
    match row {
        Some(row) => materialization_from_row(&row).map(Some),
        None => lease.refused(&client).await.map(|()| None),
    }
}

/// The claim a reconciliation write names. Each write repeats these fences in
/// its own statement; this explains a refusal.
struct ReconciliationLease<'a> {
    materialization_id: &'a str,
    owner: &'a str,
    attempt: i64,
    state: MaterializationState,
    instance_id: &'a str,
    instance_generation: Generation,
    target: &'a crate::materialization::MaterializationTarget,
}

impl ReconciliationLease<'_> {
    /// `NotFound` when the claimed projection has moved on, `LeaseConflict`
    /// when another claim holds it or the lease has lapsed, and `Ok` when the
    /// lease still holds.
    async fn refused(&self, client: &impl GenericClient) -> StoreResult<()> {
        let row = sql::query_opt(
            client,
            "
            SELECT reconcile_owner, reconcile_attempt,
                reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint AS live
            FROM materializations
            WHERE materialization_id = $1
                AND state = $2
                AND instance_id = $3
                AND instance_generation = $4
                AND cluster_id = $5
                AND namespace = $6
            ",
            &[
                &self.materialization_id,
                &materialization_state_to_db(self.state),
                &self.instance_id,
                &generation_to_i64(self.instance_generation)?,
                &self.target.cluster_id(),
                &self.target.namespace(),
            ],
        )
        .await?;
        let Some(row) = row else {
            return Err(StoreError::NotFound {
                resource: "materialization",
            });
        };
        if row.get::<_, Option<&str>>("reconcile_owner") != Some(self.owner)
            || row.get::<_, i64>("reconcile_attempt") != self.attempt
        {
            return Err(StoreError::LeaseConflict {
                message: "materialization reconciliation lease is not currently owned".to_owned(),
            });
        }
        if !row.get::<_, Option<bool>>("live").unwrap_or(false) {
            return Err(StoreError::LeaseConflict {
                message: "materialization reconciliation lease is expired".to_owned(),
            });
        }
        Ok(())
    }
}

/// Removes a materialization's record and reservations on an operator's
/// word, and moves its instance to a state the next wake can start from:
/// Waking becomes Failed, Running becomes Cold, and Draining becomes Cold with
/// its deferred wake activated.
pub(crate) async fn force_delete_materialization(
    store: &PostgresStore,
    request: ForceDeleteMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    validate_operator_audit(&request.operator, &request.reason)?;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let materialization_id = request.materialization_id.as_str();
    insert_operator_audit_event(
        &transaction,
        OperatorAuditEvent {
            operation: "force_delete_materialization",
            materialization_id: Some(materialization_id),
            cluster_id: None,
            namespace: None,
            key_name: None,
            operator: &request.operator,
            reason: &request.reason,
        },
    )
    .await?;
    let instance = sql::query_opt(
        &transaction,
        "
            SELECT i.instance_id, i.state
            FROM instances i
            JOIN materializations m USING (instance_id)
            WHERE m.materialization_id = $1
            FOR UPDATE OF i
            ",
        &[&materialization_id],
    )
    .await?;
    let existing = sql::query_opt(&transaction, "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE materialization_id = $1
            FOR UPDATE
            ",
            &[&materialization_id],).await?;
    let existing = existing
        .as_ref()
        .map(materialization_from_row)
        .transpose()?;
    if existing
        .as_ref()
        .is_some_and(|existing| existing.state != MaterializationState::Deleted)
    {
        sql::execute(
            &transaction,
            "
                UPDATE materializations
                SET state = 'deleted',
                    backend_uri = NULL,
                pod_backend_uri = NULL,
                    rendered_objects = '[]'::jsonb,
                    exclusivity_keys = '[]'::jsonb,
                    reconcile_owner = NULL,
                    reconcile_lease_expires_at_unix_millis = NULL
                WHERE materialization_id = $1
                ",
            &[&materialization_id],
        )
        .await?;
        if let Some(instance) = instance {
            let instance_id: String = instance.get("instance_id");
            let state: String = instance.get("state");
            let settled = match state.as_str() {
                "waking" => Some("failed"),
                "running" | "draining" => Some("cold"),
                _ => None,
            };
            if let Some(settled) = settled {
                sql::execute(
                    &transaction,
                    "
                        UPDATE instances
                        SET state = $2,
                            generation = generation + 1
                        WHERE instance_id = $1
                        ",
                    &[&instance_id, &settled],
                )
                .await?;
                if state == "draining" {
                    super::lifecycle_ops::activate_deferred_wake(&transaction, &instance_id)
                        .await?;
                }
            }
        }
    }
    transaction.commit().await.map_err(map_postgres_error)?;
    crate::runtime_work::signal_lifecycle_work();

    Ok(existing)
}

pub(crate) async fn force_release_exclusivity_key(
    store: &PostgresStore,
    request: ForceReleaseExclusivityKeyRequest,
) -> StoreResult<ForceReleaseExclusivityKeyResult> {
    validate_operator_audit(&request.operator, &request.reason)?;
    if request.key_name.trim().is_empty() || request.key_value.trim().is_empty() {
        return Err(StoreError::invalid_argument(
            "force-release key name and value are required",
        ));
    }
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    insert_operator_audit_event(
        &transaction,
        OperatorAuditEvent {
            operation: "force_release_exclusivity_key",
            materialization_id: None,
            cluster_id: Some(request.target.cluster_id()),
            namespace: Some(request.target.namespace()),
            key_name: Some(&request.key_name),
            operator: &request.operator,
            reason: &request.reason,
        },
    )
    .await?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let key_name = request.key_name.as_str();
    let key_value = request.key_value.as_str();
    let affected_rows = sql::query(&transaction, "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE cluster_id = $1
                AND namespace = $2
                AND state <> 'deleted'
                AND materialization_id IN (
                    SELECT reservation.materialization_id
                    FROM materialization_key_reservations reservation
                    WHERE reservation.cluster_id = $1 AND reservation.namespace = $2
                        AND reservation.key_name = $3 AND reservation.key_value = $4
                )
            FOR UPDATE
            ",
            &[&cluster_id, &namespace, &key_name, &key_value],).await?;
    let affected_materializations = affected_rows
        .iter()
        .map(materialization_from_row)
        .collect::<StoreResult<Vec<_>>>()?;
    let updated = sql::execute(
        &transaction,
        "
            UPDATE materializations
            SET exclusivity_keys = COALESCE((
                    SELECT jsonb_agg(key)
                    FROM jsonb_array_elements(exclusivity_keys) AS key
                    WHERE NOT (
                        key ->> 'name' = $3
                        AND key ->> 'value' = $4
                    )
                ), '[]'::jsonb)
            WHERE cluster_id = $1
                AND namespace = $2
                AND state <> 'deleted'
                AND materialization_id IN (
                    SELECT reservation.materialization_id
                    FROM materialization_key_reservations reservation
                    WHERE reservation.cluster_id = $1 AND reservation.namespace = $2
                        AND reservation.key_name = $3 AND reservation.key_value = $4
                )
            ",
        &[&cluster_id, &namespace, &key_name, &key_value],
    )
    .await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(ForceReleaseExclusivityKeyResult {
        updated_materializations: usize::try_from(updated).map_err(|_| {
            StoreError::internal("force-release updated row count did not fit usize")
        })?,
        affected_materializations,
    })
}

/// A lease lifetime the database can add to its own clock. The upper bound
/// matches every other store timeout and keeps the sum inside a bigint.
fn lease_ttl_millis(ttl: Duration) -> StoreResult<i64> {
    if ttl > crate::materialization::MAX_RECONCILIATION_LEASE_TTL {
        return Err(StoreError::invalid_argument(
            "materialization reconciliation lease TTL exceeds 24 hours",
        ));
    }
    // The bound above keeps this value, and the database's sum, inside a bigint.
    let millis = ttl.as_millis() as i64;
    if millis == 0 {
        return Err(StoreError::invalid_argument(
            "materialization reconciliation lease TTL must be at least one millisecond",
        ));
    }
    Ok(millis)
}

fn validate_lease_owner(owner: &str) -> StoreResult<()> {
    if owner.trim().is_empty() {
        return Err(StoreError::invalid_argument(
            "materialization reconciliation owner is required",
        ));
    }
    Ok(())
}

fn previous_generation(generation: Generation) -> StoreResult<Generation> {
    generation
        .get()
        .checked_sub(1)
        .map(Generation::new)
        .ok_or_else(|| StoreError::invalid_argument("generation has no predecessor"))
}

fn validate_operator_audit(operator: &str, reason: &str) -> StoreResult<()> {
    if operator.trim().is_empty() || reason.trim().is_empty() {
        return Err(StoreError::invalid_argument(
            "operator and reason are required for force materialization operations",
        ));
    }
    Ok(())
}

struct OperatorAuditEvent<'a> {
    operation: &'a str,
    materialization_id: Option<&'a str>,
    cluster_id: Option<&'a str>,
    namespace: Option<&'a str>,
    key_name: Option<&'a str>,
    operator: &'a str,
    reason: &'a str,
}

async fn insert_operator_audit_event(
    client: &impl GenericClient,
    event: OperatorAuditEvent<'_>,
) -> StoreResult<()> {
    sql::execute(
        client,
        "
            INSERT INTO materialization_operator_audit_events (
                operation, materialization_id, cluster_id, namespace,
                key_name, operator, reason
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ",
        &[
            &event.operation,
            &event.materialization_id,
            &event.cluster_id,
            &event.namespace,
            &event.key_name,
            &event.operator,
            &event.reason,
        ],
    )
    .await?;
    Ok(())
}

pub(super) async fn upsert_materialization(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<MaterializationRecord> {
    let id = materialization_id(&request.instance_id, &request.target)?;
    let instance_id = request.instance_id.as_str();
    let instance_generation = generation_to_i64(request.instance_generation)?;
    let projection_generation = generation_to_i64(request.projection_generation)?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let state = materialization_state_to_db(request.state);
    let backend_uri = request.backend.as_ref().map(|backend| backend.uri());
    let pod_backend_uri = request.pod_backend.as_ref().map(|backend| backend.uri());
    let backend_generation = backend_generation_to_i64(request.backend_generation)?;
    let rendered_objects = rendered_objects_to_json(&request.rendered_objects);
    let exclusivity_keys = rendered_exclusivity_keys_to_json(&request.exclusivity_keys);
    let materialization_id = id.as_str();

    let row = sql::query_opt(client, "
            INSERT INTO materializations (
                materialization_id,
                instance_id,
                instance_generation,
                cluster_id,
                namespace,
                state,
                backend_uri,
                backend_generation,
                rendered_objects,
                exclusivity_keys,
                projection_generation,
                pod_backend_uri
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (instance_id, cluster_id, namespace)
            DO UPDATE SET
                materialization_id = EXCLUDED.materialization_id,
                instance_generation = EXCLUDED.instance_generation,
                projection_generation = EXCLUDED.projection_generation,
                state = EXCLUDED.state,
                backend_uri = EXCLUDED.backend_uri,
                pod_backend_uri = EXCLUDED.pod_backend_uri,
                backend_generation = EXCLUDED.backend_generation,
                rendered_objects = EXCLUDED.rendered_objects,
                exclusivity_keys = EXCLUDED.exclusivity_keys,
                reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL
            WHERE materializations.backend_generation <= EXCLUDED.backend_generation
            RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            ",
            &[
                &materialization_id,
                &instance_id,
                &instance_generation,
                &cluster_id,
                &namespace,
                &state,
                &backend_uri,
                &backend_generation,
                &rendered_objects,
                &exclusivity_keys,
                &projection_generation,
                &pod_backend_uri,
            ],).await?;

    let Some(row) = row else {
        return Err(backend_generation_rewind_error(
            client,
            instance_id,
            cluster_id,
            namespace,
            request.backend_generation,
        )
        .await);
    };

    materialization_from_row(&row)
}

async fn load_active_materialization_from_client(
    client: &impl GenericClient,
    request: &LoadActiveMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_id = request.instance_id.as_str();
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let row = sql::query_opt(client, "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, pod_backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE instance_id = $1
                AND cluster_id = $2
                AND namespace = $3
                AND state <> 'deleted'
            ",
            &[&instance_id, &cluster_id, &namespace],).await?;

    row.as_ref().map(materialization_from_row).transpose()
}

async fn backend_generation_rewind_error(
    client: &impl GenericClient,
    instance_id: &str,
    cluster_id: &str,
    namespace: &str,
    reported: crate::ids::BackendGeneration,
) -> StoreError {
    match sql::query_opt(
        client,
        "
        SELECT backend_generation
        FROM materializations
        WHERE instance_id = $1 AND cluster_id = $2 AND namespace = $3
        ",
        &[&instance_id, &cluster_id, &namespace],
    )
    .await
    {
        Ok(Some(row)) => {
            let existing: i64 = row.get("backend_generation");
            StoreError::invalid_argument(format!(
                "materialization backend generation rewind rejected: existing backend generation {existing} is newer than reported backend generation {}",
                reported.get()
            ))
        }
        Ok(None) => StoreError::internal(
            "materialization backend generation rewind rejected but existing projection was not found",
        ),
        Err(error) => error,
    }
}

fn lease_attempt(attempt: u64) -> StoreResult<i64> {
    i64::try_from(attempt)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            StoreError::invalid_argument("lease attempt must be a positive signed 64-bit integer")
        })
}
