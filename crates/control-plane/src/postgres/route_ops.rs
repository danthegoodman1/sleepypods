use serde_json::Value;

use deadpool_postgres::GenericClient;

use crate::{
    materialization::MaterializationRecord,
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest, HostRules,
        ListRouteBindingsForInstanceRequest, ResolveHostRulesRequest, RouteBindingRecord,
        RouteDependencyLookup, RouteDependencySet, RouteEntry, RouteKind, RouteRule,
        MAX_HOST_RULES,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    idempotency::{self, CREATE_ROUTE_BINDING_OPERATION},
    instance_ops::load_instance,
    mapping::{
        generation_to_i64, materialization_from_row, route_binding_from_row,
        route_binding_row_from_row, route_identity_parts, RouteBindingRow,
    },
};

pub(crate) async fn create_route_binding(
    store: &PostgresStore,
    request: CreateRouteBindingRequest,
) -> StoreResult<RouteBindingRecord> {
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
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix
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

// One statement reads every applicable binding, its instance and the target's
// ready backend from the same MVCC snapshot. The order is the ranking
// `HostRules` keeps: exact host, longer suffix, longer path prefix.
pub(crate) const HOST_RULES_SQL: &str = "
    SELECT bindings.route_binding_id, bindings.instance_id, bindings.identity_kind,
        bindings.host_kind, bindings.host, bindings.path_prefix,
        instances.state, instances.generation, ready.backend_uri, ready.backend_generation
    FROM route_bindings bindings JOIN instances USING (instance_id)
    LEFT JOIN LATERAL (
        SELECT backend_uri, backend_generation FROM materializations
        WHERE instance_id = bindings.instance_id
            AND instance_generation = instances.generation
            AND cluster_id = $4 AND namespace = $5 AND state = 'ready'
        LIMIT 1
    ) ready ON true
    WHERE bindings.identity_kind = $1
        AND ((bindings.host_kind = 'exact' AND bindings.host = $2)
            OR (bindings.host_kind = 'wildcard_suffix' AND bindings.host = ANY($3::text[])))
    ORDER BY (bindings.host_kind = 'exact') DESC, octet_length(bindings.host) DESC,
        COALESCE(octet_length(bindings.path_prefix), 0) DESC, bindings.route_binding_id
    LIMIT $6
";

pub(crate) async fn resolve_host_rules(
    store: &PostgresStore,
    request: ResolveHostRulesRequest,
) -> StoreResult<HostRules> {
    let client = store.client().await?;
    let host = request.host.as_str();
    let suffixes: Vec<&str> = host
        .match_indices('.')
        .map(|(index, _)| &host[index + 1..])
        .collect();
    let rows = client
        .query(
            HOST_RULES_SQL,
            &[
                &route_kind_to_db(request.kind),
                &host,
                &suffixes,
                &request.target.cluster_id(),
                &request.target.namespace(),
                &(MAX_HOST_RULES as i64 + 1),
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    if rows.len() > MAX_HOST_RULES {
        return Err(StoreError::invalid_argument(format!(
            "more than {MAX_HOST_RULES} route bindings apply to host {host}"
        )));
    }
    let rules = rows
        .iter()
        .map(|row| {
            let route = route_binding_row_from_row(row)?;
            let state: String = row.get("state");
            let backend_uri: Option<String> = row.get("backend_uri");
            let backend_generation: Option<i64> = row.get("backend_generation");
            Ok(RouteRule {
                identity: route.identity,
                entry: RouteEntry {
                    route_binding_id: route.id,
                    instance_id: route.instance_id,
                    instance_state: super::mapping::instance_state_from_db(&state)?,
                    instance_generation: super::mapping::generation_from_i64(
                        row.get("generation"),
                    )?,
                    backend: backend_uri
                        .map(crate::materialization::BackendEndpoint::new)
                        .transpose()
                        .map_err(|error| StoreError::internal(error.to_string()))?,
                    backend_generation: backend_generation
                        .map(super::mapping::backend_generation_from_i64)
                        .transpose()?,
                },
            })
        })
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(HostRules::ranked(rules))
}

fn route_kind_to_db(kind: RouteKind) -> &'static str {
    match kind {
        RouteKind::Http => "http",
        RouteKind::Sni => "sni",
    }
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
                path_prefix
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix
            ",
            &[
                &route_binding_id,
                &instance_id,
                &parts.key,
                &parts.identity_kind,
                &parts.host_kind,
                &parts.host,
                &path_prefix,
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
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix
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
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix
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
                namespace, state, backend_uri, backend_generation, rendered_objects,
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
