use std::time::Duration;

use deadpool_postgres::GenericClient;

use crate::{
    ids::Generation,
    instance::{validate_instance_state_transition, InstanceState, StateTransitionReason},
    materialization::{
        BeginSleepRequest, BeginSleepResult, ClaimMaterializationReconciliationRequest,
        CompleteWakeReconciliationRequest, CompleteWakeRequest, CompleteWakeResult,
        DeleteMaterializationReconciliationRequest, FinalizeSleepReconciliationRequest,
        FinalizeSleepRequest, FinalizeSleepResult, ForceDeleteMaterializationRequest,
        ForceReleaseExclusivityKeyRequest, ForceReleaseExclusivityKeyResult,
        ListMaterializationReconciliationCandidatesRequest, LoadActiveMaterializationRequest,
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
        backend_generation_to_i64, generation_to_i64, instance_from_row, instance_state_to_db,
        materialization_from_row, materialization_id, materialization_state_from_db,
        materialization_state_to_db, rendered_exclusivity_keys_to_json, rendered_objects_to_json,
    },
};

pub(crate) async fn record_materialization(
    store: &PostgresStore,
    mut request: RecordMaterializationRequest,
) -> StoreResult<MaterializationRecord> {
    normalize_exclusivity_keys(&mut request.exclusivity_keys);
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    ensure_instance_generation(&transaction, &request).await?;
    let record = upsert_materialization(&transaction, &request).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(record)
}

pub(crate) async fn load_ready_materialization(
    store: &PostgresStore,
    request: LoadReadyMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;
    let instance_id = request.instance_id.as_str();
    let instance_generation = generation_to_i64(request.instance_generation)?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE instance_id = $1
                AND instance_generation = $2
                AND cluster_id = $3
                AND namespace = $4
                AND state = 'ready'
            ",
            &[&instance_id, &instance_generation, &cluster_id, &namespace],
        )
        .await
        .map_err(map_postgres_error)?;

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
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE materialization_id = $1
            ",
            &[&materialization_id],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn complete_wake(
    store: &PostgresStore,
    mut request: CompleteWakeRequest,
) -> StoreResult<CompleteWakeResult> {
    normalize_exclusivity_keys(&mut request.exclusivity_keys);
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let result = complete_wake_in_transaction(&transaction, request).await?;
    transaction.commit().await.map_err(map_postgres_error)?;
    Ok(result)
}

fn normalize_exclusivity_keys(keys: &mut Vec<crate::workload::RenderedExclusivityKey>) {
    keys.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value))
    });
    keys.dedup();
}

pub(crate) async fn begin_sleep(
    store: &PostgresStore,
    request: BeginSleepRequest,
) -> StoreResult<BeginSleepResult> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let instance_id = request.instance_id.as_str();
    let current = transaction
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = current else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let current = instance_from_row(&row)?;

    if current.generation != request.expected_running_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_running_generation,
            actual: current.generation,
        });
    }

    validate_instance_state_transition(
        current.state,
        InstanceState::Draining,
        &StateTransitionReason::IdleReported,
    )
    .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

    if let Some(minimum_ready_age) = request.minimum_ready_age {
        let minimum_millis = i64::try_from(minimum_ready_age.as_millis()).map_err(|_| {
            StoreError::invalid_argument("minimum Ready age exceeds database range")
        })?;
        let generation = generation_to_i64(request.expected_running_generation)?;
        let row = transaction
            .query_opt(
                "SELECT state_entered_at_unix_millis,
                    (extract(epoch from clock_timestamp()) * 1000)::bigint AS now_millis
                 FROM materializations
                 WHERE instance_id = $1 AND cluster_id = $2 AND namespace = $3
                   AND instance_generation = $4 AND state = 'ready'
                 FOR UPDATE",
                &[
                    &instance_id,
                    &request.target.cluster_id(),
                    &request.target.namespace(),
                    &generation,
                ],
            )
            .await
            .map_err(map_postgres_error)?
            .ok_or_else(|| {
                StoreError::invalid_argument(
                    "automatic sleep requires this generation's Ready materialization",
                )
            })?;
        let ready_at: i64 = row.get("state_entered_at_unix_millis");
        let now: i64 = row.get("now_millis");
        let not_before_unix_millis = ready_at.checked_add(minimum_millis).ok_or_else(|| {
            StoreError::invalid_argument("activation deadline exceeds database range")
        })?;
        if now < not_before_unix_millis {
            return Err(StoreError::SleepDeferred {
                retry_after: std::time::Duration::from_millis(
                    (not_before_unix_millis - now) as u64,
                ),
            });
        }
    }

    let draining_generation = request.expected_running_generation.next();
    let draining_generation_db = generation_to_i64(draining_generation)?;
    let draining_state = instance_state_to_db(InstanceState::Draining);
    let row = transaction
        .query_one(
            "
            UPDATE instances
            SET state = $2,
                generation = $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&instance_id, &draining_state, &draining_generation_db],
        )
        .await
        .map_err(map_postgres_error)?;
    let instance = instance_from_row(&row)?;
    let materialization = mark_active_materialization_deleting_for_sleep(
        &transaction,
        &request.instance_id,
        &request.target,
        request.expected_running_generation,
        request.drain_grace_timeout,
    )
    .await?;

    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(BeginSleepResult {
        instance,
        materialization,
    })
}

pub(crate) async fn finalize_sleep(
    store: &PostgresStore,
    request: FinalizeSleepRequest,
) -> StoreResult<FinalizeSleepResult> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let result = finalize_sleep_in_transaction(&transaction, request).await?;
    transaction.commit().await.map_err(map_postgres_error)?;
    Ok(result)
}

pub(crate) async fn list_materialization_reconciliation_candidates(
    store: &PostgresStore,
    request: ListMaterializationReconciliationCandidatesRequest,
) -> StoreResult<Vec<MaterializationRecord>> {
    let client = store.client().await?;
    let limit = i64::try_from(request.limit)
        .map_err(|_| StoreError::invalid_argument("reconciliation candidate limit is too large"))?;
    let cluster = request.target.as_ref().map(|t| t.cluster_id());
    let namespace = request.target.as_ref().map(|t| t.namespace());
    // Eligibility compares stored deadlines against the database clock that
    // wrote them. A caller clock never decides whose work is reclaimable.
    let rows = client
        .query(
            "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE state IN ('pending', 'deleting')
                AND (failure_kind IS NULL OR failure_kind = 'transient' OR failure_requires_cleanup)
                AND ($2::text IS NULL OR (cluster_id = $2 AND namespace = $3))
                AND NOT EXISTS (SELECT 1 FROM materialization_effects e WHERE e.materialization_id = materializations.materialization_id)
                AND next_attempt_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND drain_not_before_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND (
                    reconcile_owner IS NULL
                    OR reconcile_lease_expires_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                )
            ORDER BY (state = 'deleting') DESC, next_attempt_at_unix_millis, materialization_id
            LIMIT $1
            ",
            &[&limit, &cluster, &namespace],
        )
        .await
        .map_err(map_postgres_error)?;

    rows.iter().map(materialization_from_row).collect()
}

pub(crate) async fn load_materialization_operational_metrics(
    store: &PostgresStore,
) -> StoreResult<MaterializationOperationalMetrics> {
    let client = store.client().await?;
    // Backlog age subtracts a database-written timestamp, so the database
    // clock supplies the other operand.
    let backlog_rows = client
        .query(
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
        .await
        .map_err(map_postgres_error)?;

    let held_key_rows = client
        .query(
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
        .await
        .map_err(map_postgres_error)?;

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

    let row = client.query_one("SELECT (SELECT count(*) FROM materialization_effects) AS uncertain, (SELECT count(*) FROM materializations WHERE failure_kind IN ('permanent', 'deadline')) AS blocked", &[]).await.map_err(map_postgres_error)?;
    let mut metrics = MaterializationOperationalMetrics::new(backlog_states, held_key_states);
    metrics.uncertain_effects = row.get::<_, i64>("uncertain") as u64;
    metrics.blocked_failures = row.get::<_, i64>("blocked") as u64;
    Ok(metrics)
}

pub(crate) async fn claim_materialization_reconciliation(
    store: &PostgresStore,
    request: ClaimMaterializationReconciliationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    validate_lease_owner(&request.owner)?;
    let lease_ttl_millis = lease_ttl_millis(request.lease_ttl)?;

    let materialization_id = request.materialization_id.as_str();
    let owner = request.owner.as_str();
    // Lock first, then take a fresh READ COMMITTED snapshot for the barrier.
    // A NOT EXISTS in the locking UPDATE alone can retain a pre-wait snapshot.
    transaction.query_opt("SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE", &[&materialization_id]).await.map_err(map_postgres_error)?;
    // Both the eligibility comparisons and the new expiry read the database
    // clock after the lock wait, so a lease always lasts its requested TTL
    // measured by the same clock every other process compares against.
    let row = transaction
        .query_opt(
            "
            UPDATE materializations
            SET reconcile_owner = $2,
                reconcile_lease_expires_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $3,
                reconcile_attempt = reconcile_attempt + 1,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND state IN ('pending', 'deleting') AND (failure_kind IS NULL OR failure_kind = 'transient' OR failure_requires_cleanup)
                AND NOT EXISTS (SELECT 1 FROM materialization_effects e WHERE e.materialization_id = materializations.materialization_id)
                AND next_attempt_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND drain_not_before_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND (
                    reconcile_owner IS NULL
                    OR reconcile_lease_expires_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
                )
            RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            ",
            &[&materialization_id, &owner, &lease_ttl_millis],
        )
        .await
        .map_err(map_postgres_error)?;

    transaction.commit().await.map_err(map_postgres_error)?;
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
    let updated = client
        .execute(
            "
            UPDATE materializations
            SET reconcile_lease_expires_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND reconcile_owner = $2
                AND reconcile_attempt = $4
                AND instance_generation = $5
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND state IN ('pending', 'deleting') AND state = $6
            ",
            &[&materialization_id, &owner, &lease_ttl_millis, &attempt, &generation, &expected_state],
        )
        .await
        .map_err(map_postgres_error)?;

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
    let updated = client
        .execute(
            "
            UPDATE materializations
            SET reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                next_attempt_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND NOT EXISTS (SELECT 1 FROM materialization_effects e WHERE e.materialization_id = materializations.materialization_id)
                AND reconcile_owner = $2
                AND reconcile_attempt = $3
                AND instance_generation = $4
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND state IN ('pending', 'deleting')
            ",
            &[&materialization_id, &owner, &attempt, &generation],
        )
        .await
        .map_err(map_postgres_error)?;

    Ok(updated == 1)
}

pub(crate) async fn complete_wake_reconciliation(
    store: &PostgresStore,
    request: CompleteWakeReconciliationRequest,
) -> StoreResult<CompleteWakeResult> {
    validate_lease_owner(&request.lease_owner)?;
    let mut complete = request.complete;
    normalize_exclusivity_keys(&mut complete.exclusivity_keys);
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    ensure_reconciliation_lease(
        &transaction,
        request.materialization_id.as_str(),
        (&request.lease_owner, request.attempt),
        MaterializationState::Pending,
        &complete.instance_id,
        complete.expected_waking_generation,
        &complete.target,
    )
    .await?;

    let result = complete_wake_in_transaction(&transaction, complete).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(result)
}

pub(crate) async fn finalize_sleep_reconciliation(
    store: &PostgresStore,
    request: FinalizeSleepReconciliationRequest,
) -> StoreResult<FinalizeSleepResult> {
    validate_lease_owner(&request.lease_owner)?;
    let finalize = request.finalize;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let materialization_generation = previous_generation(finalize.expected_draining_generation)?;
    ensure_reconciliation_lease(
        &transaction,
        request.materialization_id.as_str(),
        (&request.lease_owner, request.attempt),
        MaterializationState::Deleting,
        &finalize.instance_id,
        materialization_generation,
        &finalize.target,
    )
    .await?;

    let result = finalize_sleep_in_transaction(&transaction, finalize).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(result)
}

pub(crate) async fn delete_materialization_reconciliation(
    store: &PostgresStore,
    request: DeleteMaterializationReconciliationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    validate_lease_owner(&request.lease_owner)?;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    ensure_reconciliation_lease(
        &transaction,
        request.materialization_id.as_str(),
        (&request.lease_owner, request.attempt),
        request.expected_state,
        &request.instance_id,
        request.instance_generation,
        &request.target,
    )
    .await?;

    let materialization_id = request.materialization_id.as_str();
    let state = materialization_state_to_db(request.expected_state);
    let instance_id = request.instance_id.as_str();
    let instance_generation = generation_to_i64(request.instance_generation)?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let owner = request.lease_owner.as_str();
    let row = transaction
        .query_opt(
            "
            UPDATE materializations
            SET state = 'deleted',
                backend_uri = NULL,
                rendered_objects = '[]'::jsonb,
                exclusivity_keys = '[]'::jsonb,
                reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND state = $2
                AND instance_id = $3
                AND instance_generation = $4
                AND cluster_id = $5
                AND namespace = $6
                AND reconcile_owner = $7
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
            RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            ",
            &[
                &materialization_id,
                &state,
                &instance_id,
                &instance_generation,
                &cluster_id,
                &namespace,
                &owner,
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    transaction.commit().await.map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

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
    let existing = transaction
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE materialization_id = $1
            FOR UPDATE
            ",
            &[&materialization_id],
        )
        .await
        .map_err(map_postgres_error)?;
    if existing.is_some() {
        transaction
            .execute(
                "DELETE FROM materialization_effects WHERE materialization_id = $1",
                &[&materialization_id],
            )
            .await
            .map_err(map_postgres_error)?;
        transaction
            .execute(
                "
                UPDATE materializations
                SET state = 'deleted',
                    backend_uri = NULL,
                    rendered_objects = '[]'::jsonb,
                    exclusivity_keys = '[]'::jsonb,
                    reconcile_owner = NULL,
                    reconcile_lease_expires_at_unix_millis = NULL,
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE materialization_id = $1
                ",
                &[&materialization_id],
            )
            .await
            .map_err(map_postgres_error)?;
    }
    transaction.commit().await.map_err(map_postgres_error)?;

    existing.as_ref().map(materialization_from_row).transpose()
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
    let affected_rows = transaction
        .query(
            "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
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
            &[&cluster_id, &namespace, &key_name, &key_value],
        )
        .await
        .map_err(map_postgres_error)?;
    let affected_materializations = affected_rows
        .iter()
        .map(materialization_from_row)
        .collect::<StoreResult<Vec<_>>>()?;
    let updated = transaction
        .execute(
            "
            UPDATE materializations
            SET exclusivity_keys = COALESCE((
                    SELECT jsonb_agg(key)
                    FROM jsonb_array_elements(exclusivity_keys) AS key
                    WHERE NOT (
                        key ->> 'name' = $3
                        AND key ->> 'value' = $4
                    )
                ), '[]'::jsonb),
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
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
        .await
        .map_err(map_postgres_error)?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(ForceReleaseExclusivityKeyResult {
        updated_materializations: usize::try_from(updated).map_err(|_| {
            StoreError::internal("force-release updated row count did not fit usize")
        })?,
        affected_materializations,
    })
}

async fn complete_wake_in_transaction(
    transaction: &impl GenericClient,
    request: CompleteWakeRequest,
) -> StoreResult<CompleteWakeResult> {
    let instance_id = request.instance_id.as_str();
    let current = transaction
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = current else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let current = instance_from_row(&row)?;

    if current.generation != request.expected_waking_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_waking_generation,
            actual: current.generation,
        });
    }

    validate_instance_state_transition(
        current.state,
        InstanceState::Running,
        &StateTransitionReason::MaterializationReady,
    )
    .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

    let running_generation = request.expected_waking_generation.next();
    let running_generation_db = generation_to_i64(running_generation)?;
    let running_state = instance_state_to_db(InstanceState::Running);
    let row = transaction
        .query_one(
            "
            UPDATE instances
            SET state = $2,
                generation = $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&instance_id, &running_state, &running_generation_db],
        )
        .await
        .map_err(map_postgres_error)?;
    let instance = instance_from_row(&row)?;

    let mut materialization_request = RecordMaterializationRequest::new(
        request.instance_id.clone(),
        running_generation,
        request.target.clone(),
        MaterializationState::Ready,
        request.backend_generation,
    );
    let prior = load_active_materialization_from_client(
        transaction,
        &LoadActiveMaterializationRequest::new(request.instance_id.clone(), request.target.clone()),
    )
    .await?;
    materialization_request.projection_generation = prior
        .map(|m| m.projection_generation)
        .unwrap_or(running_generation);
    materialization_request.backend = Some(request.backend);
    materialization_request.rendered_objects = request.rendered_objects;
    materialization_request.exclusivity_keys = request.exclusivity_keys;
    let materialization = upsert_materialization(transaction, &materialization_request).await?;

    Ok(CompleteWakeResult {
        instance,
        materialization,
    })
}

async fn finalize_sleep_in_transaction(
    transaction: &impl GenericClient,
    request: FinalizeSleepRequest,
) -> StoreResult<FinalizeSleepResult> {
    let instance_id = request.instance_id.as_str();
    let current = transaction
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = current else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let current = instance_from_row(&row)?;

    if current.generation != request.expected_draining_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_draining_generation,
            actual: current.generation,
        });
    }

    validate_instance_state_transition(
        current.state,
        InstanceState::Cold,
        &StateTransitionReason::DrainCompleted,
    )
    .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

    let cold_generation = request.expected_draining_generation.next();
    let cold_generation_db = generation_to_i64(cold_generation)?;
    let cold_state = instance_state_to_db(InstanceState::Cold);
    let row = transaction
        .query_one(
            "
            UPDATE instances
            SET state = $2,
                generation = $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&instance_id, &cold_state, &cold_generation_db],
        )
        .await
        .map_err(map_postgres_error)?;
    let instance = instance_from_row(&row)?;
    let materialization = mark_active_materialization_state(
        transaction,
        &request.instance_id,
        &request.target,
        MaterializationState::Deleted,
        Some(cold_generation),
        Some(&[]),
        Some(&[]),
    )
    .await?;

    let instance =
        super::lifecycle_ops::activate_deferred_wake(transaction, request.instance_id.as_str())
            .await?
            .unwrap_or(instance);
    Ok(FinalizeSleepResult {
        instance,
        materialization,
    })
}

async fn ensure_reconciliation_lease(
    client: &impl GenericClient,
    materialization_id: &str,
    lease: (&str, u64),
    expected_state: MaterializationState,
    instance_id: &crate::ids::InstanceId,
    instance_generation: Generation,
    target: &crate::materialization::MaterializationTarget,
) -> StoreResult<()> {
    let (owner, attempt) = lease;
    let expected_state = materialization_state_to_db(expected_state);
    let instance_id_value = instance_id.as_str();
    let instance_generation = generation_to_i64(instance_generation)?;
    let cluster_id = target.cluster_id();
    let namespace = target.namespace();
    let row = client
        .query_opt(
            "
            SELECT instance_generation, reconcile_owner, reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE materialization_id = $1
                AND state = $2
                AND instance_id = $3
                AND instance_generation = $4
                AND cluster_id = $5
                AND namespace = $6
            FOR UPDATE
            ",
            &[
                &materialization_id,
                &expected_state,
                &instance_id_value,
                &instance_generation,
                &cluster_id,
                &namespace,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = row else {
        return Err(StoreError::NotFound {
            resource: "materialization",
        });
    };
    let actual_generation: i64 = row.get("instance_generation");
    if actual_generation != instance_generation {
        return Err(StoreError::GenerationConflict {
            expected: Generation::new(instance_generation as u64),
            actual: Generation::new(u64::try_from(actual_generation).map_err(|_| {
                StoreError::internal("stored materialization generation was negative")
            })?),
        });
    }
    let actual_owner: Option<String> = row.get("reconcile_owner");
    if actual_owner.as_deref() != Some(owner)
        || row.get::<_, i64>("reconcile_attempt") != lease_attempt(attempt)?
    {
        return Err(StoreError::LeaseConflict {
            message: "materialization reconciliation lease is not currently owned".to_owned(),
        });
    }
    let expires_at: Option<i64> = row.get("reconcile_lease_expires_at_unix_millis");
    let now: i64 = client
        .query_one(
            "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint AS now",
            &[],
        )
        .await
        .map_err(map_postgres_error)?
        .get("now");
    if expires_at.is_none_or(|expires_at| expires_at <= now) {
        return Err(StoreError::LeaseConflict {
            message: "materialization reconciliation lease is expired".to_owned(),
        });
    }

    if client
        .query_opt(
            "SELECT 1 FROM materialization_effects WHERE materialization_id = $1",
            &[&materialization_id],
        )
        .await
        .map_err(map_postgres_error)?
        .is_some()
    {
        return Err(StoreError::LeaseConflict {
            message: "Kubernetes effect outcome remains unacknowledged".into(),
        });
    }
    Ok(())
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
    client
        .execute(
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
        .await
        .map_err(map_postgres_error)?;
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
    let backend_generation = backend_generation_to_i64(request.backend_generation)?;
    let rendered_objects = rendered_objects_to_json(&request.rendered_objects);
    let exclusivity_keys = rendered_exclusivity_keys_to_json(&request.exclusivity_keys);
    let materialization_id = id.as_str();

    let row = client
        .query_opt(
            "
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
                projection_generation
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (instance_id, cluster_id, namespace)
            DO UPDATE SET
                materialization_id = EXCLUDED.materialization_id,
                instance_generation = EXCLUDED.instance_generation,
                projection_generation = EXCLUDED.projection_generation,
                state = EXCLUDED.state,
                backend_uri = EXCLUDED.backend_uri,
                backend_generation = EXCLUDED.backend_generation,
                rendered_objects = EXCLUDED.rendered_objects,
                exclusivity_keys = EXCLUDED.exclusivity_keys,
                reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materializations.backend_generation <= EXCLUDED.backend_generation
            RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
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
            ],
        )
        .await
        .map_err(map_postgres_error)?;

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
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE instance_id = $1
                AND cluster_id = $2
                AND namespace = $3
                AND state <> 'deleted'
            ",
            &[&instance_id, &cluster_id, &namespace],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

async fn mark_active_materialization_state(
    client: &impl GenericClient,
    instance_id: &crate::ids::InstanceId,
    target: &crate::materialization::MaterializationTarget,
    state: MaterializationState,
    instance_generation: Option<Generation>,
    rendered_objects: Option<&[crate::materialization::RenderedObjectRef]>,
    exclusivity_keys: Option<&[crate::workload::RenderedExclusivityKey]>,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_id = instance_id.as_str();
    let cluster_id = target.cluster_id();
    let namespace = target.namespace();
    let state = materialization_state_to_db(state);
    let rendered_objects = rendered_objects.map(rendered_objects_to_json);
    let exclusivity_keys = exclusivity_keys.map(rendered_exclusivity_keys_to_json);

    let row = if let Some(instance_generation) = instance_generation {
        let instance_generation = generation_to_i64(instance_generation)?;
        client
            .query_opt(
                "
                UPDATE materializations
                SET state = $4,
                    instance_generation = $5,
                    backend_uri = NULL,
                    rendered_objects = COALESCE($6, rendered_objects),
                    exclusivity_keys = COALESCE($7, exclusivity_keys),
                    reconcile_owner = NULL,
                    reconcile_lease_expires_at_unix_millis = NULL,
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE instance_id = $1
                    AND cluster_id = $2
                    AND namespace = $3
                    AND state <> 'deleted'
                RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                    namespace, state, backend_uri, backend_generation, rendered_objects,
                    exclusivity_keys, reconcile_owner,
                    reconcile_lease_expires_at_unix_millis, reconcile_attempt
                ",
                &[
                    &instance_id,
                    &cluster_id,
                    &namespace,
                    &state,
                    &instance_generation,
                    &rendered_objects,
                    &exclusivity_keys,
                ],
            )
            .await
            .map_err(map_postgres_error)?
    } else {
        client
            .query_opt(
                "
                UPDATE materializations
                SET state = $4,
                    backend_uri = NULL,
                    reconcile_owner = NULL,
                    reconcile_lease_expires_at_unix_millis = NULL,
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE instance_id = $1
                    AND cluster_id = $2
                    AND namespace = $3
                    AND state <> 'deleted'
                RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                    namespace, state, backend_uri, backend_generation, rendered_objects,
                    exclusivity_keys, reconcile_owner,
                    reconcile_lease_expires_at_unix_millis, reconcile_attempt
                ",
                &[&instance_id, &cluster_id, &namespace, &state],
            )
            .await
            .map_err(map_postgres_error)?
    };

    row.as_ref().map(materialization_from_row).transpose()
}

async fn mark_active_materialization_deleting_for_sleep(
    client: &impl GenericClient,
    instance_id: &crate::ids::InstanceId,
    target: &crate::materialization::MaterializationTarget,
    expected_instance_generation: Generation,
    drain_grace_timeout: Duration,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_id_value = instance_id.as_str();
    let cluster_id = target.cluster_id();
    let namespace = target.namespace();
    let state = materialization_state_to_db(MaterializationState::Deleting);
    let expected_generation_db = generation_to_i64(expected_instance_generation)?;
    let drain_grace_millis = i64::try_from(drain_grace_timeout.as_millis())
        .map_err(|_| StoreError::invalid_argument("drain grace timeout is too large"))?;
    let row = client
        .query_opt(
            "
            UPDATE materializations
            SET state = $5,
                backend_uri = NULL,
                reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint,
                drain_not_before_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $6
            WHERE instance_id = $1
                AND cluster_id = $2
                AND namespace = $3
                AND instance_generation = $4
                AND state <> 'deleted'
            RETURNING materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            ",
            &[
                &instance_id_value,
                &cluster_id,
                &namespace,
                &expected_generation_db,
                &state,
                &drain_grace_millis,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if let Some(row) = row {
        return materialization_from_row(&row).map(Some);
    }

    reject_active_materialization_generation_mismatch(
        client,
        instance_id,
        target,
        expected_instance_generation,
    )
    .await?;

    Ok(None)
}

async fn reject_active_materialization_generation_mismatch(
    client: &impl GenericClient,
    instance_id: &crate::ids::InstanceId,
    target: &crate::materialization::MaterializationTarget,
    expected_instance_generation: Generation,
) -> StoreResult<()> {
    let request = LoadActiveMaterializationRequest::new(instance_id.clone(), target.clone());
    let Some(active) = load_active_materialization_from_client(client, &request).await? else {
        return Ok(());
    };

    if active.instance_generation == expected_instance_generation {
        return Ok(());
    }

    Err(StoreError::GenerationConflict {
        expected: expected_instance_generation,
        actual: active.instance_generation,
    })
}

async fn ensure_instance_generation(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<()> {
    let instance_id = request.instance_id.as_str();
    let row = client
        .query_opt(
            "SELECT generation FROM instances WHERE instance_id = $1 FOR UPDATE",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;
    let Some(row) = row else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let actual: i64 = row.get("generation");
    let expected = generation_to_i64(request.instance_generation)?;

    if actual == expected {
        Ok(())
    } else {
        Err(StoreError::GenerationConflict {
            expected: request.instance_generation,
            actual: Generation::new(
                u64::try_from(actual)
                    .map_err(|_| StoreError::internal("stored instance generation was negative"))?,
            ),
        })
    }
}

async fn backend_generation_rewind_error(
    client: &impl GenericClient,
    instance_id: &str,
    cluster_id: &str,
    namespace: &str,
    reported: crate::ids::BackendGeneration,
) -> StoreError {
    match client
        .query_opt(
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
        Err(error) => map_postgres_error(error),
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

/// Begin under the same row lock used by lease acquisition and lifecycle commits.
/// The record is deliberately retained if the caller disappears before acknowledgement.
pub(crate) async fn begin_materialization_effect(
    store: &PostgresStore,
    request: crate::materialization::MaterializationEffectRequest,
) -> StoreResult<bool> {
    validate_lease_owner(&request.owner)?;
    let attempt = lease_attempt(request.attempt)?;
    let effect_id = lease_attempt(request.effect_id)?;
    let generation = generation_to_i64(request.instance_generation)?;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let id = request.materialization_id.as_str();
    let state = materialization_state_to_db(request.expected_state);
    transaction.query_opt("SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE", &[&id]).await.map_err(map_postgres_error)?;
    let row = transaction.query_opt(
        "SELECT 1 FROM materializations WHERE materialization_id = $1 AND reconcile_owner = $2 AND reconcile_attempt = $3 AND instance_generation = $4 AND state = $5 AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint",
        &[&id, &request.owner, &attempt, &generation, &state],
    ).await.map_err(map_postgres_error)?;
    if row.is_none() {
        return Ok(false);
    }
    let object = serde_json::json!({"api_version": request.object.api_version, "kind": request.object.kind, "namespace": request.object.namespace, "name": request.object.name});
    let uid = request
        .precondition
        .as_ref()
        .map(|identity| identity.uid.as_str());
    let resource_version = request
        .precondition
        .as_ref()
        .map(|identity| identity.resource_version.as_str());
    let inserted = transaction.execute(
        "INSERT INTO materialization_effects (materialization_id, lease_owner, lease_attempt, instance_generation, operation, object_ref, expected_uid, expected_resource_version, effect_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",
        &[&id, &request.owner, &attempt, &generation, &request.operation, &object, &uid, &resource_version, &effect_id],
    ).await.map_err(map_postgres_error)?;
    let same_operation = if inserted == 0 {
        transaction.query_opt("SELECT 1 FROM materialization_effects WHERE materialization_id = $1 AND lease_owner = $2 AND lease_attempt = $3 AND instance_generation = $4 AND operation = $5 AND object_ref = $6 AND expected_uid IS NOT DISTINCT FROM $7 AND expected_resource_version IS NOT DISTINCT FROM $8 AND effect_id = $9", &[&id, &request.owner, &attempt, &generation, &request.operation, &object, &uid, &resource_version, &effect_id]).await.map_err(map_postgres_error)?.is_some()
    } else {
        false
    };
    transaction.commit().await.map_err(map_postgres_error)?;
    Ok(inserted == 1 || same_operation)
}

pub(crate) async fn acknowledge_materialization_effect(
    store: &PostgresStore,
    request: crate::materialization::AcknowledgeMaterializationEffectRequest,
) -> StoreResult<bool> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let attempt = lease_attempt(request.attempt)?;
    let effect_id = lease_attempt(request.effect_id)?;
    let generation = generation_to_i64(request.instance_generation)?;
    // A definite response still resolves uncertainty after lease expiry. The
    // unresolved barrier prevented any intervening attempt from acquiring ownership.
    // An ambiguous begin may still hold its row lock while committing. Wait for
    // that transaction before taking the snapshot used by this exact ACK.
    transaction.query_opt("SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE", &[&request.materialization_id.as_str()]).await.map_err(map_postgres_error)?;
    let deleted = transaction.execute(
        "DELETE FROM materialization_effects WHERE materialization_id = $1 AND lease_owner = $2 AND lease_attempt = $3 AND effect_id = $4 AND instance_generation = $5",
        &[&request.materialization_id.as_str(), &request.owner, &attempt, &effect_id, &generation],
    ).await.map_err(map_postgres_error)?;
    transaction.commit().await.map_err(map_postgres_error)?;
    Ok(deleted == 1)
}
