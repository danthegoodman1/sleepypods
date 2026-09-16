use serde_json::Value;

use deadpool_postgres::GenericClient;

use crate::{
    materialization::MaterializationRecord,
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        ListRouteBindingsForInstanceRequest, ResolveRouteRequest, RouteBindingRecord,
        RouteDependencyLookup, RouteDependencySet, RouteEntry, RouteResolution,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    idempotency::{self, CREATE_ROUTE_BINDING_OPERATION},
    instance_ops::load_instance,
    mapping::{
        default_negative_cache_policy, generation_to_i64, materialization_from_row, protocol_to_db,
        route_binding_from_row, route_binding_row_from_row, route_identity_parts, RouteBindingRow,
    },
};

pub(crate) async fn create_route_binding(
    store: &PostgresStore,
    request: CreateRouteBindingRequest,
) -> StoreResult<RouteBindingRecord> {
    super::mapping::validate_route_protocol(&request.spec())?;

    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let fingerprint = idempotency::create_route_binding_fingerprint(&request)?;
    let idempotency_key = request.idempotency_key.as_str();
    let route_binding_id = request.route_binding_id.as_str();
    idempotency::expire_key(&transaction, idempotency_key).await?;
    let inserted = transaction
        .execute(
            "
            INSERT INTO idempotency_records (
                idempotency_key,
                operation,
                request_fingerprint,
                resource_id,
                expires_at_unix_millis
            )
            VALUES ($1, $2, $3, $4, (extract(epoch from clock_timestamp()) * 1000)::bigint + $5::bigint)
            ON CONFLICT (idempotency_key) DO NOTHING
            ",
            &[
                &idempotency_key,
                &CREATE_ROUTE_BINDING_OPERATION,
                &fingerprint,
                &route_binding_id,
                &store.idempotency_retention_millis,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if inserted == 0 {
        let result =
            replay_create_route_binding(&transaction, idempotency_key, &fingerprint).await?;
        transaction.commit().await.map_err(map_postgres_error)?;
        return Ok(result);
    }

    let result = insert_route_binding(&transaction, &request).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(result)
}

pub(crate) async fn get_route_binding(
    store: &PostgresStore,
    request: GetRouteBindingRequest,
) -> StoreResult<Option<RouteBindingRecord>> {
    let client = store.client().await?;

    load_route_binding_record(&client, request.route_binding_id.as_str()).await
}

pub(crate) async fn delete_route_binding(
    store: &PostgresStore,
    request: DeleteRouteBindingRequest,
) -> StoreResult<bool> {
    let client = store.client().await?;
    let route_binding_id = request.route_binding_id.as_str();
    let deleted = client
        .execute(
            "DELETE FROM route_bindings WHERE route_binding_id = $1",
            &[&route_binding_id],
        )
        .await
        .map_err(map_postgres_error)?;

    Ok(deleted > 0)
}

pub(crate) async fn list_route_bindings_for_instance(
    store: &PostgresStore,
    request: ListRouteBindingsForInstanceRequest,
) -> StoreResult<Vec<RouteBindingRecord>> {
    let client = store.client().await?;
    let rows = client
        .query(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE instance_id = $1
            ORDER BY route_binding_id
            ",
            &[&request.instance_id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;

    rows.iter().map(route_binding_from_row).collect()
}

// One statement selects the route and target backend from the same MVCC snapshot.
// SQL matching mirrors route_match_score; literal prefix comparisons avoid LIKE escaping.
pub(crate) const RESOLVE_ROUTE_SQL: &str = "
    WITH selected AS (
        SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
        FROM route_bindings
        WHERE identity_kind = $1
            AND ((host_kind = 'exact' AND host = $2)
                OR (host_kind = 'wildcard_suffix' AND host = ANY($3::text[])))
            AND (path_prefix IS NULL OR ($4::text IS NOT NULL AND (
                path_prefix = '/' OR path_prefix = $4
                OR (right(path_prefix, 1) = '/' AND starts_with($4, path_prefix))
                OR starts_with($4, path_prefix || '/')
            )))
        ORDER BY (host_kind = 'exact') DESC, octet_length(host) DESC,
            COALESCE(octet_length(path_prefix), 0) DESC, route_binding_id
        LIMIT 1
    )
    SELECT selected.*, instances.state, instances.generation,
        ready.backend_uri, ready.backend_address, ready.backend_generation
    FROM selected JOIN instances USING (instance_id)
    LEFT JOIN LATERAL (
        SELECT backend_uri, backend_address, backend_generation FROM materializations
        WHERE instance_id = selected.instance_id
            AND instance_generation = instances.generation
            AND cluster_id = $5 AND namespace = $6 AND state = 'ready'
        LIMIT 1
    ) ready ON true
";

pub(crate) async fn resolve_route(
    store: &PostgresStore,
    request: ResolveRouteRequest,
) -> StoreResult<RouteResolution> {
    let client = store.client().await?;
    let parts = route_identity_parts(&request.identity);
    let suffixes: Vec<&str> = parts
        .host
        .match_indices('.')
        .map(|(index, _)| &parts.host[index + 1..])
        .collect();
    let row = client
        .query_opt(
            RESOLVE_ROUTE_SQL,
            &[
                &parts.identity_kind,
                &parts.host,
                &suffixes,
                &parts.path_prefix,
                &request.target.cluster_id(),
                &request.target.namespace(),
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    let Some(row) = row else {
        return Ok(RouteResolution::Miss {
            negative_cache: default_negative_cache_policy(),
        });
    };
    let route = route_binding_row_from_row(&row)?;
    let state: String = row.get("state");
    let backend_uri: Option<String> = row.get("backend_uri");
    let backend_address: Option<String> = row.get("backend_address");
    let backend_generation: Option<i64> = row.get("backend_generation");
    Ok(RouteResolution::Resolved {
        matched_identity: route.identity,
        entry: RouteEntry {
            route_binding_id: route.id,
            instance_id: route.instance_id,
            instance_state: super::mapping::instance_state_from_db(&state)?,
            instance_generation: super::mapping::generation_from_i64(row.get("generation"))?,
            backend: super::mapping::backend_endpoint_from_row(backend_uri, backend_address)?,
            backend_generation: backend_generation
                .map(super::mapping::backend_generation_from_i64)
                .transpose()?,
        },
    })
}

pub(crate) async fn lookup_route_dependencies(
    store: &PostgresStore,
    request: RouteDependencyLookup,
) -> StoreResult<Option<RouteDependencySet>> {
    let client = store.client().await?;
    let route_binding_id = request.route_binding_id().as_str();
    let Some(route) = load_route_binding(&client, route_binding_id).await? else {
        return Ok(None);
    };
    let instance = load_instance(&client, route.instance_id.as_str())
        .await?
        .ok_or(StoreError::NotFound {
            resource: "instance",
        })?;
    let materialization =
        load_ready_materialization(&client, route.instance_id.as_str(), instance.generation)
            .await?;

    Ok(Some(RouteDependencySet {
        route_binding_id: route.id,
        instance_id: instance.id,
        materialization_generation: materialization.map(|record| record.backend_generation),
    }))
}

async fn replay_create_route_binding(
    client: &impl GenericClient,
    idempotency_key: &str,
    fingerprint: &Value,
) -> StoreResult<RouteBindingRecord> {
    let row = client
        .query_opt(
            "
            SELECT operation, request_fingerprint, resource_id, resource_deleted_at_unix_millis
            FROM idempotency_records
            WHERE idempotency_key = $1
            FOR UPDATE
            ",
            &[&idempotency_key],
        )
        .await
        .map_err(map_postgres_error)?
        .ok_or_else(|| {
            StoreError::unavailable("idempotency key expired during replay; retry request")
        })?;
    let operation: String = row.get("operation");
    let stored_fingerprint: Value = row.get("request_fingerprint");
    let resource_id: String = row.get("resource_id");

    if operation != CREATE_ROUTE_BINDING_OPERATION || stored_fingerprint != *fingerprint {
        return Err(idempotency::idempotency_conflict());
    }

    if row
        .get::<_, Option<i64>>("resource_deleted_at_unix_millis")
        .is_some()
    {
        return Err(StoreError::IdempotencyResourceDeleted {
            resource: "route binding",
        });
    }

    load_route_binding_record(client, &resource_id)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "route binding",
        })
}

async fn insert_route_binding(
    client: &impl GenericClient,
    request: &CreateRouteBindingRequest,
) -> StoreResult<RouteBindingRecord> {
    let parts = route_identity_parts(&request.identity);
    let route_binding_id = request.route_binding_id.as_str();
    let instance_id = request.instance_id.as_str();
    let protocol = protocol_to_db(request.protocol);
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

    route_binding_from_row(&row)
}

async fn load_route_binding_record(
    client: &impl GenericClient,
    route_binding_id: &str,
) -> StoreResult<Option<RouteBindingRecord>> {
    let row = client
        .query_opt(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE route_binding_id = $1
            ",
            &[&route_binding_id],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(route_binding_from_row).transpose()
}

async fn load_route_binding(
    client: &impl GenericClient,
    route_binding_id: &str,
) -> StoreResult<Option<RouteBindingRow>> {
    let row = client
        .query_opt(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE route_binding_id = $1
            ",
            &[&route_binding_id],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(route_binding_row_from_row).transpose()
}

pub(crate) async fn load_ready_materialization(
    client: &impl GenericClient,
    instance_id: &str,
    instance_generation: crate::ids::Generation,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_generation = generation_to_i64(instance_generation)?;
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, projection_generation, cluster_id,
                namespace, state, backend_uri, backend_address, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE instance_id = $1 AND instance_generation = $2 AND state = 'ready'
            ORDER BY backend_generation DESC, materialization_id
            LIMIT 1
            ",
            &[&instance_id, &instance_generation],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}
