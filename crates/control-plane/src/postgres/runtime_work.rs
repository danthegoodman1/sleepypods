use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{generation_from_i64, generation_to_i64, rendered_objects_from_json},
};
use crate::{
    ids::MaterializationId,
    runtime_work::{
        DurableRouteChanges, MaterializationWorkStatus, RecordMaterializationFailure,
        UncertainMaterializationEffect,
    },
    store::{StoreError, StoreResult},
};

pub(super) async fn revision(store: &PostgresStore) -> StoreResult<u64> {
    let client = store.client().await?;
    let row = client
        .query_one(
            "SELECT revision FROM route_change_revision WHERE singleton",
            &[],
        )
        .await
        .map_err(map_postgres_error)?;
    Ok(row.get::<_, i64>(0) as u64)
}

pub(super) async fn changes(
    store: &PostgresStore,
    cursor: u64,
    limit: u32,
) -> StoreResult<DurableRouteChanges> {
    let cursor =
        i64::try_from(cursor).map_err(|_| StoreError::invalid_argument("route cursor overflow"))?;
    let limit = i64::from(limit.clamp(1, 1024));
    let client = store.client().await?;
    // One statement/snapshot makes retained range and event records consistent.
    let row = client.query_one("SELECT revision, (SELECT min(revision) FROM route_change_outbox) AS oldest, COALESCE((SELECT jsonb_agg(jsonb_build_object('revision', revision, 'payload', payload) ORDER BY revision) FROM (SELECT revision, payload FROM route_change_outbox WHERE revision > $1 ORDER BY revision LIMIT $2) events), '[]'::jsonb) AS events FROM route_change_revision WHERE singleton", &[&cursor, &limit]).await.map_err(map_postgres_error)?;
    let current: i64 = row.get("revision");
    let oldest: Option<i64> = row.get("oldest");
    let reset =
        cursor > current || (cursor < current && oldest.is_none_or(|oldest| cursor < oldest - 1));
    let values: serde_json::Value = row.get("events");
    let values = values.as_array().expect("jsonb_agg array");
    let last = values
        .last()
        .and_then(|value| value["revision"].as_u64())
        .unwrap_or(current as u64);
    Ok(DurableRouteChanges {
        cursor: if reset { current as u64 } else { last },
        reset,
        events: if reset {
            vec![]
        } else {
            values
                .iter()
                .map(|value| value["payload"].clone())
                .collect()
        },
    })
}

pub(super) async fn status(
    store: &PostgresStore,
    id: MaterializationId,
) -> StoreResult<Option<MaterializationWorkStatus>> {
    let client = store.client().await?;
    let Some(row) = client.query_opt("SELECT CASE WHEN m.state = 'ready' THEN GREATEST(0::bigint, (extract(epoch from clock_timestamp()) * 1000)::bigint - m.state_entered_at_unix_millis) ELSE NULL END AS ready_age_millis, m.next_attempt_at_unix_millis, m.operation_deadline_unix_millis, CASE WHEN m.operation_deadline_unix_millis > 0 THEN GREATEST(0::bigint, m.operation_deadline_unix_millis - (extract(epoch from clock_timestamp()) * 1000)::bigint) ELSE NULL END AS operation_remaining_millis, m.failure_count, m.failure_kind, m.failure_message, m.wake_failure_message, e.instance_generation AS effect_generation, e.lease_owner, e.lease_attempt, e.effect_id, e.operation, e.object_ref, e.expected_uid, e.expected_resource_version, e.started_at_unix_millis FROM materializations m LEFT JOIN materialization_effects e USING(materialization_id) WHERE materialization_id = $1", &[&id.as_str()]).await.map_err(map_postgres_error)? else { return Ok(None) };
    let effect = if let Some(generation) = row.get::<_, Option<i64>>("effect_generation") {
        let object: serde_json::Value = row.get("object_ref");
        Some(UncertainMaterializationEffect {
            generation: generation_from_i64(generation)?,
            owner: row.get("lease_owner"),
            attempt: row.get::<_, i64>("lease_attempt") as u64,
            effect_id: row.get::<_, i64>("effect_id") as u64,
            operation: row.get("operation"),
            object: rendered_objects_from_json(serde_json::json!([object]))?.remove(0),
            expected_uid: row.get("expected_uid"),
            expected_resource_version: row.get("expected_resource_version"),
            started_at_unix_millis: row.get("started_at_unix_millis"),
        })
    } else {
        None
    };
    Ok(Some(MaterializationWorkStatus {
        ready_age: row
            .get::<_, Option<i64>>("ready_age_millis")
            .map(|millis| std::time::Duration::from_millis(millis as u64)),
        next_attempt_at_unix_millis: row.get("next_attempt_at_unix_millis"),
        operation_deadline_unix_millis: row.get("operation_deadline_unix_millis"),
        operation_remaining: row
            .get::<_, Option<i64>>("operation_remaining_millis")
            .map(|millis| std::time::Duration::from_millis(millis as u64)),
        failure_count: row.get::<_, i32>("failure_count") as u32,
        failure_kind: row.get("failure_kind"),
        failure_message: row.get("failure_message"),
        wake_failure_message: row.get("wake_failure_message"),
        uncertain_effect: effect,
    }))
}

pub(super) async fn record_failure(
    store: &PostgresStore,
    request: RecordMaterializationFailure,
) -> StoreResult<bool> {
    let mut client = store.client().await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    let id = request.materialization_id.as_str();
    let attempt = i64::try_from(request.attempt)
        .map_err(|_| StoreError::invalid_argument("attempt overflow"))?;
    let generation = generation_to_i64(request.generation)?;
    // Match lifecycle acceptance's instance -> materialization lock order. A
    // fresh statement after the instance lock observes a committed Delete before
    // checking the exact claimed work state, including when this call waited.
    if tx.query_opt("SELECT i.instance_id FROM instances i JOIN materializations m USING(instance_id) WHERE m.materialization_id = $1 FOR UPDATE OF i", &[&id]).await.map_err(map_postgres_error)?.is_none() {
        return Ok(false);
    }
    let expected_state = super::mapping::materialization_state_to_db(request.expected_state);
    let Some(row) = tx.query_opt("SELECT state, instance_id, failure_count, reconcile_lease_expires_at_unix_millis, operation_deadline_unix_millis FROM materializations WHERE materialization_id = $1 AND reconcile_owner = $2 AND reconcile_attempt = $3 AND instance_generation = $4 AND state = $5 FOR UPDATE", &[&id, &request.owner, &attempt, &generation, &expected_state]).await.map_err(map_postgres_error)? else { return Ok(false) };
    let now = tx
        .query_one(
            "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .await
        .map_err(map_postgres_error)?
        .get::<_, i64>(0);
    if row
        .get::<_, Option<i64>>("reconcile_lease_expires_at_unix_millis")
        .is_none_or(|expiry| expiry <= now)
    {
        return Ok(false);
    }
    let uncertain = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM materialization_effects WHERE materialization_id = $1)",
            &[&id],
        )
        .await
        .map_err(map_postgres_error)?
        .get::<_, bool>(0);
    // The locking SELECT may have waited across the deadline. Use the same
    // fresh post-lock database time as the lease check, not a pre-wait expression.
    let expired = row.get::<_, i64>("operation_deadline_unix_millis") <= now;
    let terminal = !uncertain && (request.permanent || expired);
    let kind = if uncertain {
        "uncertain"
    } else if expired {
        "deadline"
    } else if request.permanent {
        "permanent"
    } else {
        "transient"
    };
    let count = row.get::<_, i32>("failure_count").saturating_add(1);
    let delay = 1000_i64 * (1_i64 << count.min(5));
    let message = request.message.chars().take(512).collect::<String>();
    let cleanup_required = terminal && row.get::<_, &str>("state") == "pending";
    if cleanup_required {
        tx.execute(
            "UPDATE materializations SET state = 'deleting' WHERE materialization_id = $1",
            &[&id],
        )
        .await
        .map_err(map_postgres_error)?;
        tx.execute("UPDATE instances SET state = 'failed', generation = generation + 1 WHERE instance_id = $1 AND generation = $2 AND state = 'waking'", &[&row.get::<_, String>("instance_id"), &generation]).await.map_err(map_postgres_error)?;
    }
    // Clearing owner makes retry after a committed/lost response an exact no-op.
    tx.execute("UPDATE materializations SET failure_count = $2, failure_kind = $3, failure_message = $4, failure_requires_cleanup = $6, wake_failure_message = CASE WHEN $6 THEN $4 ELSE wake_failure_message END, next_attempt_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $5, reconcile_owner = NULL, reconcile_lease_expires_at_unix_millis = NULL WHERE materialization_id = $1", &[&id, &count, &kind, &message, &delay, &cleanup_required]).await.map_err(map_postgres_error)?;
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(true)
}

pub(super) async fn enqueue(store: &PostgresStore, id: MaterializationId) -> StoreResult<bool> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    transaction.query_opt("SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE", &[&id.as_str()]).await.map_err(map_postgres_error)?;
    // An admin request schedules normal work. It cannot override grace, a live
    // attempt or uncertainty. Failed wakes first clean their retained inventory.
    let changed = transaction.execute("UPDATE materializations SET state = CASE WHEN state = 'failed' THEN 'deleting' ELSE state END, failure_kind = NULL, failure_message = '', failure_count = 0, operation_deadline_unix_millis = GREATEST(drain_not_before_unix_millis, (extract(epoch from clock_timestamp()) * 1000)::bigint) + lifecycle_operation_timeout_millis(), next_attempt_at_unix_millis = GREATEST(drain_not_before_unix_millis, (extract(epoch from clock_timestamp()) * 1000)::bigint) WHERE materialization_id = $1 AND state IN ('pending', 'deleting', 'failed') AND (reconcile_owner IS NULL OR reconcile_lease_expires_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint) AND NOT EXISTS (SELECT 1 FROM materialization_effects e WHERE e.materialization_id = materializations.materialization_id)", &[&id.as_str()]).await.map_err(map_postgres_error)?;
    transaction.commit().await.map_err(map_postgres_error)?;
    Ok(changed == 1)
}

pub(super) async fn maintain(store: &PostgresStore, limit: u32) -> StoreResult<u64> {
    let limit = limit.clamp(1, 1024);
    let idempotency = store.expire_idempotency_records(limit).await?;
    let http01 =
        super::http01_ops::collect_expired_http01_challenges(store, limit as usize).await?;
    let client = store.client().await?;
    let outbox = client.execute("DELETE FROM route_change_outbox WHERE revision IN (SELECT revision FROM route_change_outbox WHERE revision < COALESCE((SELECT min(revision) FROM route_change_outbox WHERE created_at_unix_millis >= (extract(epoch from clock_timestamp()) * 1000)::bigint - 600000), 9223372036854775807) ORDER BY revision LIMIT $1)", &[&i64::from(limit)]).await.map_err(map_postgres_error)?;
    Ok(idempotency + http01 as u64 + outbox)
}
