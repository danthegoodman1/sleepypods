use super::{
    connection::PostgresStore, error::map_postgres_error, mapping::generation_to_i64, sql,
};
use crate::{
    ids::MaterializationId,
    runtime_work::{DurableRouteChanges, MaterializationWorkStatus, RecordMaterializationFailure},
    store::{StoreError, StoreResult},
};

/// Names the channel the route-change trigger notifies at commit for the
/// schema this connection's store lives in.
const ROUTE_CHANGE_CHANNEL: &str = "SELECT 'sleepypods_routes_' || md5(current_schema())";
const LISTENER_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(1);
const LISTENER_KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_secs(30);

/// One dedicated connection that listens for route-change notifications and
/// wakes the dispatcher. It starts with the first subscriber and stops when
/// the store is dropped. Notifications are a wake-up only: the dispatcher reads
/// the outbox by cursor, so a missed notification delays a change by one poll.
#[derive(Clone)]
pub(crate) struct RouteChangeListener {
    shared: std::sync::Arc<ListenerShared>,
}

struct ListenerShared {
    config: tokio_postgres::Config,
    signal: std::sync::Arc<tokio::sync::Notify>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RouteChangeListener {
    pub(super) fn new(mut config: tokio_postgres::Config) -> Self {
        config.keepalives(true);
        config.keepalives_idle(LISTENER_KEEPALIVE_IDLE);
        Self {
            shared: std::sync::Arc::new(ListenerShared {
                config,
                signal: std::sync::Arc::new(tokio::sync::Notify::new()),
                task: std::sync::Mutex::new(None),
            }),
        }
    }

    pub(super) fn subscribe(&self) -> std::sync::Arc<tokio::sync::Notify> {
        let mut task = self.shared.task.lock().expect("route change listener");
        if task.is_none() {
            *task = Some(tokio::spawn(listen_for_route_changes(
                self.shared.config.clone(),
                self.shared.signal.clone(),
            )));
        }
        self.shared.signal.clone()
    }
}

impl Drop for ListenerShared {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().expect("route change listener").take() {
            task.abort();
        }
    }
}

impl std::fmt::Debug for RouteChangeListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteChangeListener")
            .finish_non_exhaustive()
    }
}

async fn listen_for_route_changes(
    config: tokio_postgres::Config,
    signal: std::sync::Arc<tokio::sync::Notify>,
) {
    loop {
        if let Ok((client, mut connection)) = config.connect(tokio_postgres::NoTls).await {
            let notifications = async {
                while let Some(Ok(message)) =
                    std::future::poll_fn(|cx| connection.poll_message(cx)).await
                {
                    if matches!(message, tokio_postgres::AsyncMessage::Notification(_)) {
                        signal.notify_one();
                    }
                }
            };
            tokio::pin!(notifications);
            let listen = async {
                let channel: String = client.query_one(ROUTE_CHANGE_CHANNEL, &[]).await?.get(0);
                client.batch_execute(&format!("LISTEN {channel}")).await
            };
            let listening = tokio::select! {
                _ = &mut notifications => false,
                result = listen => result.is_ok(),
            };
            if listening {
                // Changes committed while no connection was listening.
                signal.notify_one();
                notifications.await;
            }
        }
        tokio::time::sleep(LISTENER_RECONNECT_DELAY).await;
    }
}

pub(super) async fn revision(store: &PostgresStore) -> StoreResult<u64> {
    let client = store.client().await?;
    let row = sql::query_one(
        &client,
        "SELECT revision FROM route_change_revision WHERE singleton",
        &[],
    )
    .await?;
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
    let row = sql::query_one(&client, "SELECT revision, (SELECT min(revision) FROM route_change_outbox) AS oldest, COALESCE((SELECT jsonb_agg(jsonb_build_object('revision', revision, 'payload', payload) ORDER BY revision) FROM (SELECT revision, payload FROM route_change_outbox WHERE revision > $1 ORDER BY revision LIMIT $2) events), '[]'::jsonb) AS events FROM route_change_revision WHERE singleton", &[&cursor, &limit]).await?;
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
    let Some(row) = sql::query_opt(&client, "
            SELECT next_attempt_at_unix_millis,
                operation_deadline_unix_millis,
                CASE WHEN operation_deadline_unix_millis > 0
                    THEN GREATEST(0::bigint, operation_deadline_unix_millis - (extract(epoch from clock_timestamp()) * 1000)::bigint)
                    ELSE NULL END AS operation_remaining_millis,
                failure_count, failure_kind, failure_message, wake_failure_message
            FROM materializations
            WHERE materialization_id = $1
            ",
            &[&id.as_str()],).await?
    else {
        return Ok(None);
    };
    Ok(Some(MaterializationWorkStatus {
        next_attempt_at_unix_millis: row.get("next_attempt_at_unix_millis"),
        operation_deadline_unix_millis: row.get("operation_deadline_unix_millis"),
        operation_remaining: row
            .get::<_, Option<i64>>("operation_remaining_millis")
            .map(|millis| std::time::Duration::from_millis(millis as u64)),
        failure_count: row.get::<_, i32>("failure_count") as u32,
        failure_kind: row.get("failure_kind"),
        failure_message: row.get("failure_message"),
        wake_failure_message: row.get("wake_failure_message"),
    }))
}

/// Records one failed attempt and schedules the next after a capped backoff.
///
/// A wake that fails permanently, or outlives its operation deadline, ends:
/// the instance becomes Failed and the materialization is queued for cleanup.
/// Cleanup has one acceptable outcome, so it retries until it succeeds; only
/// a failure the caller marks permanent (an ownership conflict) parks it.
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
    if sql::query_opt(&tx, "SELECT i.instance_id FROM instances i JOIN materializations m USING(instance_id) WHERE m.materialization_id = $1 FOR UPDATE OF i", &[&id]).await?.is_none() {
        return Ok(false);
    }
    let expected_state = super::mapping::materialization_state_to_db(request.expected_state);
    let Some(row) = sql::query_opt(&tx, "SELECT state, instance_id, failure_count, reconcile_lease_expires_at_unix_millis, operation_deadline_unix_millis FROM materializations WHERE materialization_id = $1 AND reconcile_owner = $2 AND reconcile_attempt = $3 AND instance_generation = $4 AND state = $5 FOR UPDATE", &[&id, &request.owner, &attempt, &generation, &expected_state]).await? else { return Ok(false) };
    let now = sql::query_one(
        &tx,
        "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint",
        &[],
    )
    .await?
    .get::<_, i64>(0);
    if row
        .get::<_, Option<i64>>("reconcile_lease_expires_at_unix_millis")
        .is_none_or(|expiry| expiry <= now)
    {
        return Ok(false);
    }
    let waking = row.get::<_, &str>("state") == "pending";
    // The locking SELECT may have waited across the deadline. Use the same
    // fresh post-lock database time as the lease check, not a pre-wait expression.
    let expired = waking && row.get::<_, i64>("operation_deadline_unix_millis") <= now;
    let terminal = request.permanent || expired;
    let kind = if expired {
        "deadline"
    } else if request.permanent {
        "permanent"
    } else {
        "transient"
    };
    let count = row.get::<_, i32>("failure_count").saturating_add(1);
    let delay = 1000_i64 * (1_i64 << count.min(5));
    let message = request.message.chars().take(512).collect::<String>();
    let cleanup_required = terminal && waking;
    if cleanup_required {
        sql::execute(
            &tx,
            "UPDATE materializations SET state = 'deleting' WHERE materialization_id = $1",
            &[&id],
        )
        .await?;
        sql::execute(&tx, "UPDATE instances SET state = 'failed', generation = generation + 1 WHERE instance_id = $1 AND generation = $2 AND state = 'waking'", &[&row.get::<_, String>("instance_id"), &generation]).await?;
    }
    // A failed wake's cleanup starts at once and with a fresh backoff. Clearing
    // the owner makes a retry after a committed, lost response an exact no-op.
    let (count, delay) = if cleanup_required {
        (0, 0)
    } else {
        (count, delay)
    };
    sql::execute(&tx, "UPDATE materializations SET failure_count = $2, failure_kind = $3, failure_message = $4, failure_requires_cleanup = $6, wake_failure_message = CASE WHEN $6 THEN $4 ELSE wake_failure_message END, next_attempt_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint + $5, reconcile_owner = NULL, reconcile_lease_expires_at_unix_millis = NULL WHERE materialization_id = $1", &[&id, &count, &kind, &message, &delay, &cleanup_required]).await?;
    tx.commit().await.map_err(map_postgres_error)?;
    if cleanup_required {
        crate::runtime_work::signal_lifecycle_work();
    }
    Ok(true)
}

pub(super) async fn enqueue(store: &PostgresStore, id: MaterializationId) -> StoreResult<bool> {
    let client = store.client().await?;
    // An admin request schedules normal work. It cannot override grace or a
    // live attempt.
    let changed = sql::execute(&client, "UPDATE materializations SET failure_kind = NULL, failure_message = '', failure_count = 0, operation_deadline_unix_millis = GREATEST(drain_not_before_unix_millis, (extract(epoch from clock_timestamp()) * 1000)::bigint) + lifecycle_operation_timeout_millis(), next_attempt_at_unix_millis = GREATEST(drain_not_before_unix_millis, (extract(epoch from clock_timestamp()) * 1000)::bigint) WHERE materialization_id = $1 AND state IN ('pending', 'deleting') AND (reconcile_owner IS NULL OR reconcile_lease_expires_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint)", &[&id.as_str()]).await?;
    if changed == 1 {
        crate::runtime_work::signal_lifecycle_work();
    }
    Ok(changed == 1)
}

pub(super) async fn maintain(store: &PostgresStore, limit: u32) -> StoreResult<u64> {
    let limit = limit.clamp(1, 1024);
    let http01 =
        super::http01_ops::collect_expired_http01_challenges(store, limit as usize).await?;
    let client = store.client().await?;
    let outbox = sql::execute(&client, "DELETE FROM route_change_outbox WHERE revision IN (SELECT revision FROM route_change_outbox WHERE revision < COALESCE((SELECT min(revision) FROM route_change_outbox WHERE created_at_unix_millis >= (extract(epoch from clock_timestamp()) * 1000)::bigint - 600000), 9223372036854775807) ORDER BY revision LIMIT $1)", &[&i64::from(limit)]).await?;
    Ok(http01 as u64 + outbox)
}
