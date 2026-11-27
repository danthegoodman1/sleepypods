use deadpool_postgres::{GenericClient, Transaction};
use tokio_postgres::IsolationLevel;

use crate::{
    ids::Generation,
    instance::{
        CreateInstanceRequest, CreateInstanceResult, GetInstanceRequest, InstanceRecord,
        InstanceRoute, InstanceState,
    },
    route::RouteBindingRecord,
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
        route_binding_from_row, sleep_policy_to_json, value_schema_to_json, values_to_json,
        workload_class_version_from_row, workload_exclusivity_keys_to_json,
    },
    sql,
};

pub(crate) async fn create_instance(
    store: &PostgresStore,
    request: CreateInstanceRequest,
) -> StoreResult<CreateInstanceResult> {
    let mut client = store.client().await?;
    let workload_class = load_class(store, &client, &request.workload_class)
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
    transaction: &Transaction<'_>,
    request: &CreateInstanceRequest,
) -> StoreResult<CreateInstanceResult> {
    Ok(CreateInstanceResult {
        instance: insert_instance(transaction, request).await?,
        route_bindings: insert_route_bindings(transaction, request).await?,
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
    let inserted = sql::execute(
        &client,
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
    .await?;

    let stored = load_class(store, &client, &desired.reference)
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
    if let Some(class) = store.classes.get(&request.reference) {
        return Ok(Some(class));
    }
    let client = store.client().await?;
    load_class(store, &client, &request.reference).await
}

/// Reads a class version through the process's cache.
async fn load_class(
    store: &PostgresStore,
    client: &impl GenericClient,
    reference: &WorkloadClassVersionRef,
) -> StoreResult<Option<WorkloadClassVersion>> {
    if let Some(class) = store.classes.get(reference) {
        return Ok(Some(class));
    }
    let class = load_workload_class_version_from_client(client, reference).await?;
    if let Some(class) = &class {
        store.classes.insert(class);
    }
    Ok(class)
}

pub(crate) async fn get_instance(
    store: &PostgresStore,
    request: GetInstanceRequest,
) -> StoreResult<Option<InstanceRecord>> {
    let client = store.client().await?;

    load_instance(&client, request.instance_id.as_str()).await
}

pub(crate) async fn load_instance(
    client: &impl GenericClient,
    instance_id: &str,
) -> StoreResult<Option<InstanceRecord>> {
    let row = sql::query_opt(
        client,
        "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            ",
        &[&instance_id],
    )
    .await?;

    row.as_ref().map(instance_from_row).transpose()
}

async fn load_workload_class_version_from_client(
    client: &impl GenericClient,
    reference: &WorkloadClassVersionRef,
) -> StoreResult<Option<WorkloadClassVersion>> {
    let class_id = reference.class_id.as_str();
    let version = generation_to_i64(reference.version)?;
    let row = sql::query_opt(
        client,
        "
            SELECT class_id, version, manifest_template, value_schema, sleep_policy,
                exclusivity_keys
            FROM workload_class_versions
            WHERE class_id = $1 AND version = $2
            ",
        &[&class_id, &version],
    )
    .await?;

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
    let row = sql::query_one(client, "
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
            ],).await?;

    instance_from_row(&row)
}

async fn insert_route_bindings(
    transaction: &Transaction<'_>,
    request: &CreateInstanceRequest,
) -> StoreResult<Vec<RouteBindingRecord>> {
    let mut route_bindings = Vec::with_capacity(request.route_bindings.len());
    for route in &request.route_bindings {
        route_bindings.push(
            super::route_ops::insert_route_binding(
                transaction,
                &route.id,
                &request.instance_id,
                &route.identity,
            )
            .await?,
        );
    }

    Ok(route_bindings)
}

async fn load_route_bindings_for_instance(
    client: &impl GenericClient,
    instance_id: &str,
) -> StoreResult<Vec<RouteBindingRecord>> {
    let rows = sql::query(
        client,
        "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix
            FROM route_bindings
            WHERE instance_id = $1
            ORDER BY route_binding_id
            ",
        &[&instance_id],
    )
    .await?;

    rows.iter().map(route_binding_from_row).collect()
}
