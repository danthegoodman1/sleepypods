use crate::{
    http01::{
        DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
        Http01ChallengeRecord, PutHttp01ChallengeRequest,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{http01_from_row, unix_millis_from_system_time},
};

pub(crate) async fn put_http01_challenge(
    store: &PostgresStore,
    request: PutHttp01ChallengeRequest,
) -> StoreResult<Http01ChallengeRecord> {
    let client = store.client().await?;
    let host = request.key().host().as_str();
    let token = request.key().token();
    let key_authorization = request.key_authorization();
    let expires_at_unix_millis = unix_millis_from_system_time(request.expires_at())?;
    let row = client
        .query_one(
            "
            INSERT INTO http01_challenges (
                host,
                token,
                key_authorization,
                expires_at_unix_millis
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (host, token)
            DO UPDATE SET
                key_authorization = EXCLUDED.key_authorization,
                expires_at_unix_millis = EXCLUDED.expires_at_unix_millis,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            RETURNING host, token, key_authorization, expires_at_unix_millis
            ",
            &[&host, &token, &key_authorization, &expires_at_unix_millis],
        )
        .await
        .map_err(map_postgres_error)?;

    http01_from_row(&row)
}

pub(crate) async fn resolve_http01_challenge(
    store: &PostgresStore,
    key: Http01ChallengeKey,
) -> StoreResult<Option<Http01ChallengeRecord>> {
    let client = store.client().await?;
    let host = key.host().as_str();
    let token = key.token();
    // `expires_at_unix_millis` is compared on the database's clock. Gating a
    // serving decision on this process's clock would let a skewed replica
    // withhold a live challenge, or serve one past its expiry, and would let
    // two replicas disagree about the same challenge.
    let row = client
        .query_opt(
            "
            SELECT host, token, key_authorization, expires_at_unix_millis
            FROM http01_challenges
            WHERE host = $1 AND token = $2
            AND expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
            ",
            &[&host, &token],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(http01_from_row).transpose()
}

pub(crate) async fn delete_http01_challenge(
    store: &PostgresStore,
    request: DeleteHttp01ChallengeRequest,
) -> StoreResult<bool> {
    let client = store.client().await?;
    let host = request.key().host().as_str();
    let token = request.key().token();
    let deleted = client
        .execute(
            "DELETE FROM http01_challenges WHERE host = $1 AND token = $2",
            &[&host, &token],
        )
        .await
        .map_err(map_postgres_error)?;

    Ok(deleted > 0)
}

/// Operator-driven expiry at an instant the caller chose.
pub(crate) async fn expire_http01_challenges(
    store: &PostgresStore,
    request: ExpireHttp01ChallengesRequest,
) -> StoreResult<usize> {
    let cutoff = unix_millis_from_system_time(request.now)?;
    delete_expired_http01_challenges(store, Some(cutoff), request.limit).await
}

/// Background collection, which has no instant of its own to honour and so
/// leaves the cutoff to the database clock.
pub(crate) async fn collect_expired_http01_challenges(
    store: &PostgresStore,
    limit: usize,
) -> StoreResult<usize> {
    delete_expired_http01_challenges(store, None, Some(limit)).await
}

/// `None` means "whatever the database calls now", which is also what every
/// read of `expires_at_unix_millis` compares against.
async fn delete_expired_http01_challenges(
    store: &PostgresStore,
    cutoff_unix_millis: Option<i64>,
    limit: Option<usize>,
) -> StoreResult<usize> {
    const CUTOFF: &str = "COALESCE($1, (extract(epoch from clock_timestamp()) * 1000)::bigint)";
    let client = store.client().await?;
    let deleted = if let Some(limit) = limit {
        let limit = i64::try_from(limit).map_err(|_| {
            StoreError::invalid_argument("HTTP-01 expire limit does not fit in Postgres bigint")
        })?;
        client
            .execute(
                &format!(
                    "DELETE FROM http01_challenges
                     WHERE (host, token) IN (
                         SELECT host, token
                         FROM http01_challenges
                         WHERE expires_at_unix_millis <= {CUTOFF}
                         ORDER BY expires_at_unix_millis, host, token
                         LIMIT $2
                     )"
                ),
                &[&cutoff_unix_millis, &limit],
            )
            .await
            .map_err(map_postgres_error)?
    } else {
        client
            .execute(
                &format!("DELETE FROM http01_challenges WHERE expires_at_unix_millis <= {CUTOFF}"),
                &[&cutoff_unix_millis],
            )
            .await
            .map_err(map_postgres_error)?
    };

    usize::try_from(deleted)
        .map_err(|_| StoreError::internal("deleted HTTP-01 count did not fit in usize"))
}
