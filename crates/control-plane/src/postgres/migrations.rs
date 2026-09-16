use deadpool_postgres::GenericClient;

use crate::store::{StoreError, StoreResult};

pub(crate) struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial_control_plane_store",
        sql: include_str!("../../migrations/0001_initial_control_plane_store.sql"),
    },
    Migration {
        version: 2,
        name: "workload_class_value_schema",
        sql: include_str!("../../migrations/0002_workload_class_value_schema.sql"),
    },
    Migration {
        version: 3,
        name: "workload_class_manifest_template",
        sql: include_str!("../../migrations/0003_workload_class_manifest_template.sql"),
    },
    Migration {
        version: 4,
        name: "workload_class_sleep_policy",
        sql: include_str!("../../migrations/0004_workload_class_sleep_policy.sql"),
    },
    Migration {
        version: 5,
        name: "workload_class_exclusivity_keys",
        sql: include_str!("../../migrations/0005_workload_class_exclusivity_keys.sql"),
    },
    Migration {
        version: 6,
        name: "materialization_reconciliation_leases",
        sql: include_str!("../../migrations/0006_materialization_reconciliation_leases.sql"),
    },
    Migration {
        version: 7,
        name: "store_scalability",
        sql: include_str!("../../migrations/0007_store_scalability.sql"),
    },
    Migration {
        version: 8,
        name: "durable_lifecycle_intent",
        sql: include_str!("../../migrations/0008_durable_lifecycle_intent.sql"),
    },
    Migration {
        version: 9,
        name: "fenced_projection_effects",
        sql: include_str!("../../migrations/0009_fenced_projection_effects.sql"),
    },
    Migration {
        version: 10,
        name: "runtime_work",
        sql: include_str!("../../migrations/0010_runtime_work.sql"),
    },
    Migration {
        version: 11,
        name: "dynamic_certificates",
        sql: include_str!("../../migrations/0011_dynamic_certificates.sql"),
    },
    Migration {
        version: 12,
        name: "backend_address",
        sql: include_str!("../../migrations/0012_backend_address.sql"),
    },
];

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
