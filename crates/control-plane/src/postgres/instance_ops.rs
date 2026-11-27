use deadpool_postgres::GenericClient;
use tokio_postgres::IsolationLevel;

use crate::{
    ids::Generation,
    instance::{
        validate_instance_state_transition, CompareAndSwapInstanceStateRequest,
        CreateInstanceRequest, CreateInstanceResult, DeleteInstanceRequest, GetInstanceRequest,
        InstanceRecord, InstanceRoute, InstanceState,
    },
    route::{ProtocolRoute, RouteBindingRecord, RouteIdentity},
    store::{StoreError, StoreResult},
    workload::{
        CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, WorkloadClassVersion,
        WorkloadClassVersionRef,
    },
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{
        generation_to_i64, instance_from_row, instance_state_to_db, manifest_template_to_json,
        protocol_to_db, route_binding_from_row, route_identity_parts, sleep_policy_to_json,
        value_schema_to_json, values_to_json, workload_class_version_from_row,
        workload_exclusivity_keys_to_json,
    },
};

pub(crate) async fn create_instance(
    store: &PostgresStore,
    request: CreateInstanceRequest,
) -> StoreResult<CreateInstanceResult> {
    let mut client = store.client().await?;
    let workload_class = load_workload_class_version_from_client(&client, &request.workload_class)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "workload class version",
        })?;
    let request = request
        .validate_values_against(&workload_class)
        .map_err(|error| StoreError::invalid_argument(error.to_string()))?;
    let instance_id = request.instance_id.as_str();

    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    match insert_instance_and_routes(&transaction, &request).await {
        Ok(created) => {
            transaction.commit().await.map_err(map_postgres_error)?;
            return Ok(created);
        }
        Err(StoreError::AlreadyExists {
            resource: "instance",
        }) => {}
        Err(error) => return Err(error),
    }
    transaction.rollback().await.map_err(map_postgres_error)?;

    // An instance holds this ID. It is the one this request describes when its
    // class, values and routes equal the request's. One snapshot reads all
    // three, so the answer describes a state the instance was in.
    let snapshot = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(map_postgres_error)?;
    let Some(instance) = load_instance(&snapshot, instance_id).await? else {
        return Err(StoreError::unavailable(
            "instance was deleted during its creation; retry request",
        ));
    };
    let route_bindings = load_route_bindings_for_instance(&snapshot, instance_id).await?;
    if instance.workload_class != request.workload_class
        || instance.values != request.values
        || !same_routes(&route_bindings, &request.route_bindings)
    {
        return Err(StoreError::AlreadyExists {
            resource: "instance",
        });
    }

    Ok(CreateInstanceResult {
        instance,
        route_bindings,
    })
}

async fn insert_instance_and_routes(
    client: &impl GenericClient,
    request: &CreateInstanceRequest,
) -> StoreResult<CreateInstanceResult> {
    Ok(CreateInstanceResult {
        instance: insert_instance(client, request).await?,
        route_bindings: insert_route_bindings(client, request).await?,
    })
}

fn same_routes(stored: &[RouteBindingRecord], requested: &[InstanceRoute]) -> bool {
    let mut stored = stored
        .iter()
        .map(|route| (route.id.as_str(), &route.identity))
        .collect::<Vec<_>>();
    let mut requested = requested
        .iter()
        .map(|route| (route.id.as_str(), &route.identity))
        .collect::<Vec<_>>();
    stored.sort_by_key(|(id, _)| *id);
    requested.sort_by_key(|(id, _)| *id);
    stored == requested
}

pub(crate) async fn create_workload_class_version(
    store: &PostgresStore,
    request: CreateWorkloadClassVersionRequest,
) -> StoreResult<WorkloadClassVersion> {
    let desired = request.workload_class_version;
    desired
        .validate()
        .map_err(|error| StoreError::invalid_argument(error.to_string()))?;
    let client = store.client().await?;
    let class_id = desired.reference.class_id.as_str();
    let version = generation_to_i64(desired.reference.version)?;
    let manifest_template = manifest_template_to_json(&desired.template)?;
    let value_schema = value_schema_to_json(&desired.value_schema);
    let sleep_policy = sleep_policy_to_json(&desired.sleep_policy)?;
    let exclusivity_keys = workload_exclusivity_keys_to_json(&desired.exclusivity_keys)?;
    let inserted = client
        .execute(
            "
            INSERT INTO workload_class_versions (
                class_id,
                version,
                manifest_template,
                value_schema,
                sleep_policy,
                exclusivity_keys
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (class_id, version) DO NOTHING
            ",
            &[
                &class_id,
                &version,
                &manifest_template,
                &value_schema,
                &sleep_policy,
                &exclusivity_keys,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    let stored = load_workload_class_version_from_client(&client, &desired.reference)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "workload class version",
        })?;

    if inserted == 0 && stored != desired {
        return Err(StoreError::AlreadyExists {
            resource: "workload class version",
        });
    }

    Ok(stored)
}

pub(crate) async fn load_workload_class_version(
    store: &PostgresStore,
    request: LoadWorkloadClassVersionRequest,
) -> StoreResult<Option<WorkloadClassVersion>> {
    let client = store.client().await?;

    load_workload_class_version_from_client(&client, &request.reference).await
}

pub(crate) async fn get_instance(
    store: &PostgresStore,
    request: GetInstanceRequest,
) -> StoreResult<Option<InstanceRecord>> {
    let client = store.client().await?;

    load_instance(&client, request.instance_id.as_str()).await
}

pub(crate) async fn delete_instance(
    store: &PostgresStore,
    request: DeleteInstanceRequest,
) -> StoreResult<bool> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let instance_id = request.instance_id.as_str();
    let current = transaction
        .query_opt(
            "
            SELECT state
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;
    let Some(row) = current else {
        transaction.commit().await.map_err(map_postgres_error)?;
        return Ok(false);
    };
    let state: String = row.get("state");
    if state != instance_state_to_db(InstanceState::Deleting) {
        return Err(StoreError::invalid_argument(
            "instance delete requires state deleting",
        ));
    }

    if transaction.query_one("SELECT EXISTS(SELECT 1 FROM materializations WHERE instance_id = $1 AND state <> 'deleted')", &[&instance_id]).await.map_err(map_postgres_error)?.get::<_,bool>(0) {
        return Err(StoreError::invalid_argument("instance deletion requires completed materialization cleanup"));
    }
    let deleted = transaction
        .execute(
            "DELETE FROM instances WHERE instance_id = $1 AND state = 'deleting'",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(deleted > 0)
}

pub(crate) async fn compare_and_swap_instance_state(
    store: &PostgresStore,
    request: CompareAndSwapInstanceStateRequest,
) -> StoreResult<InstanceRecord> {
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

    if current.generation != request.expected_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_generation,
            actual: current.generation,
        });
    }

    validate_instance_state_transition(current.state, request.next_state, &request.reason)
        .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

    let next_generation = generation_to_i64(request.expected_generation.next())?;
    let next_state = instance_state_to_db(request.next_state);
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
            &[&instance_id, &next_state, &next_generation],
        )
        .await
        .map_err(map_postgres_error)?;
    let updated = instance_from_row(&row)?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(updated)
}

pub(crate) async fn load_instance(
    client: &impl GenericClient,
    instance_id: &str,
) -> StoreResult<Option<InstanceRecord>> {
    let row = client
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(instance_from_row).transpose()
}

async fn load_workload_class_version_from_client(
    client: &impl GenericClient,
    reference: &WorkloadClassVersionRef,
) -> StoreResult<Option<WorkloadClassVersion>> {
    let class_id = reference.class_id.as_str();
    let version = generation_to_i64(reference.version)?;
    let row = client
        .query_opt(
            "
            SELECT class_id, version, manifest_template, value_schema, sleep_policy,
                exclusivity_keys
            FROM workload_class_versions
            WHERE class_id = $1 AND version = $2
            ",
            &[&class_id, &version],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref()
        .map(workload_class_version_from_row)
        .transpose()
}

async fn insert_instance(
    client: &impl GenericClient,
    request: &CreateInstanceRequest,
) -> StoreResult<InstanceRecord> {
    let instance_id = request.instance_id.as_str();
    let workload_class_id = request.workload_class.class_id.as_str();
    let workload_class_version = generation_to_i64(request.workload_class.version)?;
    let values = values_to_json(&request.values)?;
    let state = instance_state_to_db(InstanceState::Cold);
    let generation = generation_to_i64(Generation::new(0))?;
    let row = client
        .query_one(
            "
            INSERT INTO instances (
                instance_id,
                workload_class_id,
                workload_class_version,
                values,
                state,
                generation
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[
                &instance_id,
                &workload_class_id,
                &workload_class_version,
                &values,
                &state,
                &generation,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    instance_from_row(&row)
}

async fn insert_route_bindings(
    client: &impl GenericClient,
    request: &CreateInstanceRequest,
) -> StoreResult<Vec<RouteBindingRecord>> {
    let mut route_bindings = Vec::with_capacity(request.route_bindings.len());
    for route in &request.route_bindings {
        let route_binding_id = route.id.as_str();
        let instance_id = request.instance_id.as_str();
        let parts = route_identity_parts(&route.identity);
        let protocol = protocol_to_db(match route.identity {
            RouteIdentity::Http { .. } => ProtocolRoute::Http,
            RouteIdentity::Sni { .. } => ProtocolRoute::TlsSni,
        });
        let path_prefix = parts.path_prefix.as_deref();
        let row = client
            .query_one(
                "
                INSERT INTO route_bindings (
                    route_binding_id,
                    instance_id,
                    identity_key,
                    identity_kind,
                    host_kind,
                    host,
                    path_prefix,
                    protocol
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
                ",
                &[
                    &route_binding_id,
                    &instance_id,
                    &parts.key,
                    &parts.identity_kind,
                    &parts.host_kind,
                    &parts.host,
                    &path_prefix,
                    &protocol,
                ],
            )
            .await
            .map_err(map_postgres_error)?;
        route_bindings.push(route_binding_from_row(&row)?);
    }

    Ok(route_bindings)
}

async fn load_route_bindings_for_instance(
    client: &impl GenericClient,
    instance_id: &str,
) -> StoreResult<Vec<RouteBindingRecord>> {
    let rows = client
        .query(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE instance_id = $1
            ORDER BY route_binding_id
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    rows.iter().map(route_binding_from_row).collect()
}
