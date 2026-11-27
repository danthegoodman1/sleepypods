use tokio_postgres::error::SqlState;

use crate::store::StoreError;

pub(crate) fn map_postgres_error(error: tokio_postgres::Error) -> StoreError {
    if let Some(db_error) = error.as_db_error() {
        if db_error.message() == "instance_id_retired" {
            return StoreError::invalid_argument(
                "instance ID was deleted before incarnation fencing; use a new instance ID",
            );
        }
        if db_error.code().code() == "P0001" {
            if db_error.message() == "rendered_object_ref_collision" {
                return StoreError::invalid_argument(
                    db_error.detail().unwrap_or(db_error.message()),
                );
            }
            if db_error.message() == "exclusivity_conflict" {
                if let Some(detail) = db_error
                    .detail()
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                {
                    return StoreError::ExclusivityConflict {
                        cluster_id: detail["cluster_id"].as_str().unwrap_or_default().to_owned(),
                        namespace: detail["namespace"].as_str().unwrap_or_default().to_owned(),
                        key_name: detail["key_name"].as_str().unwrap_or_default().to_owned(),
                        owner_instance_id: detail["owner_instance_id"].as_str().map(str::to_owned),
                        owner_generation: detail["owner_generation"]
                            .as_u64()
                            .map(crate::ids::Generation::new),
                    };
                }
            }
        }
        return match *db_error.code() {
            SqlState::T_R_SERIALIZATION_FAILURE
            | SqlState::T_R_DEADLOCK_DETECTED
            | SqlState::LOCK_NOT_AVAILABLE
            | SqlState::QUERY_CANCELED
            | SqlState::ADMIN_SHUTDOWN
            | SqlState::CRASH_SHUTDOWN
            | SqlState::CANNOT_CONNECT_NOW => StoreError::unavailable(db_error.message()),
            SqlState::UNIQUE_VIOLATION => StoreError::AlreadyExists {
                resource: unique_violation_resource(db_error.constraint()),
            },
            SqlState::FOREIGN_KEY_VIOLATION => StoreError::NotFound {
                resource: "referenced resource",
            },
            SqlState::CHECK_VIOLATION => StoreError::invalid_argument(format!(
                "request violates stored constraint {}",
                db_error.constraint().unwrap_or("unknown")
            )),
            _ => StoreError::internal(format!(
                "Postgres query failed: code={} message={}",
                db_error.code().code(),
                db_error.message()
            )),
        };
    }

    // A closed connection and an I/O error on an open one both say the server
    // was not reached or did not answer; another attempt can succeed.
    let io = std::error::Error::source(&error).is_some_and(|source| source.is::<std::io::Error>());
    if error.is_closed() || io {
        StoreError::unavailable(format!("Postgres connection failed: {error}"))
    } else {
        StoreError::internal(format!("Postgres query failed: {error}"))
    }
}

fn unique_violation_resource(constraint: Option<&str>) -> &'static str {
    match constraint {
        Some("instances_pkey") => "instance",
        Some("route_bindings_pkey") | Some("route_bindings_identity_key_key") => "route binding",
        Some("workload_class_versions_pkey") => "workload class version",
        Some("http01_challenges_pkey") => "HTTP-01 challenge",
        Some("materializations_pkey")
        | Some("materializations_instance_id_cluster_id_namespace_key") => "materialization",
        _ => "Postgres record",
    }
}
