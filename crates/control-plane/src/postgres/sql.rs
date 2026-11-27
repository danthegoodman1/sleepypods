//! Every statement runs as one its session has prepared. A session prepares
//! each statement once, so a later run costs one round trip and no planning.
use deadpool_postgres::GenericClient;
use tokio_postgres::{types::ToSql, Row};

use super::error::map_postgres_error;
use crate::store::StoreResult;

pub(crate) async fn query(
    client: &impl GenericClient,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> StoreResult<Vec<Row>> {
    let statement = client
        .prepare_cached(sql)
        .await
        .map_err(map_postgres_error)?;
    client
        .query(&statement, params)
        .await
        .map_err(map_postgres_error)
}

pub(crate) async fn query_one(
    client: &impl GenericClient,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> StoreResult<Row> {
    let statement = client
        .prepare_cached(sql)
        .await
        .map_err(map_postgres_error)?;
    client
        .query_one(&statement, params)
        .await
        .map_err(map_postgres_error)
}

pub(crate) async fn query_opt(
    client: &impl GenericClient,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> StoreResult<Option<Row>> {
    let statement = client
        .prepare_cached(sql)
        .await
        .map_err(map_postgres_error)?;
    client
        .query_opt(&statement, params)
        .await
        .map_err(map_postgres_error)
}

pub(crate) async fn execute(
    client: &impl GenericClient,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> StoreResult<u64> {
    let statement = client
        .prepare_cached(sql)
        .await
        .map_err(map_postgres_error)?;
    client
        .execute(&statement, params)
        .await
        .map_err(map_postgres_error)
}
