use deadpool_postgres::GenericClient;

use crate::store::{StoreError, StoreResult};

pub(crate) struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "baseline",
    sql: include_str!("../../migrations/0001_baseline.sql"),
}];

pub(crate) async fn run(client: &mut deadpool_postgres::Client) -> StoreResult<()> {
    // A transaction-scoped lock is released on commit, error, cancellation, or
    // disconnect. The transaction guard queues ROLLBACK before pool reuse.
    let transaction = client.transaction().await.map_err(map_migration_error)?;
    transaction
        .query_one("SELECT pg_advisory_xact_lock(1936748391, 1835624306)", &[])
        .await
        .map_err(map_migration_error)?;
    transaction
        .batch_execute(
            "
        CREATE TABLE IF NOT EXISTS control_plane_schema_migrations (
            version integer PRIMARY KEY,
            name text NOT NULL,
            applied_at_unix_millis bigint NOT NULL DEFAULT (
                extract(epoch from clock_timestamp()) * 1000
            )::bigint
        )",
        )
        .await
        .map_err(map_migration_error)?;
    for migration in MIGRATIONS {
        apply_one(&transaction, migration).await?;
    }
    transaction.commit().await.map_err(map_migration_error)
}

async fn apply_one(client: &impl GenericClient, migration: &Migration) -> StoreResult<()> {
    let existing = client
        .query_opt(
            "SELECT name FROM control_plane_schema_migrations WHERE version = $1",
            &[&migration.version],
        )
        .await
        .map_err(map_migration_error)?;

    if let Some(row) = existing {
        let name: String = row.get("name");
        if name == migration.name {
            return Ok(());
        }

        return Err(StoreError::internal(format!(
            "migration version {} was already applied as {:?}, expected {:?}",
            migration.version, name, migration.name
        )));
    }

    client
        .batch_execute(migration.sql)
        .await
        .map_err(map_migration_error)?;
    client
        .execute(
            "INSERT INTO control_plane_schema_migrations (version, name) VALUES ($1, $2)",
            &[&migration.version, &migration.name],
        )
        .await
        .map_err(map_migration_error)?;

    Ok(())
}

fn map_migration_error(error: tokio_postgres::Error) -> StoreError {
    StoreError::internal(format!("postgres migration failed: {error}"))
}
