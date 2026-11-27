use std::collections::HashMap;

use deadpool_postgres::{GenericClient, Transaction};

use crate::{
    ids::{InstanceId, RouteBindingId},
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest, HostRules,
        ResolveHostRulesRequest, RouteBindingRecord, RouteEntry, RouteIdentity, RouteKind,
        RouteRule, HOST_RULE_OVERHEAD_BYTES, MAX_HOST_RULES, MAX_HOST_RULES_BYTES,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{route_binding_from_row, route_binding_row_from_row, route_identity_parts},
    sql,
};

pub(crate) async fn create_route_binding(
    store: &PostgresStore,
    request: CreateRouteBindingRequest,
) -> StoreResult<RouteBindingRecord> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    match insert_route_binding(
        &transaction,
        &request.route_binding_id,
        &request.instance_id,
        &request.identity,
    )
    .await
    {
        Ok(created) => {
            transaction.commit().await.map_err(map_postgres_error)?;
            return Ok(created);
        }
        Err(StoreError::AlreadyExists { .. }) => {}
        Err(error) => return Err(error),
    }
    transaction.rollback().await.map_err(map_postgres_error)?;
    // A binding holds this ID or this identity. The binding under this ID is
    // the one the request describes when it names the same instance and identity.
    match load_route_binding_record(&client, request.route_binding_id.as_str()).await? {
        Some(stored)
            if stored.instance_id == request.instance_id && stored.identity == request.identity =>
        {
            Ok(stored)
        }
        _ => Err(StoreError::AlreadyExists {
            resource: "route binding",
        }),
    }
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
    let deleted = sql::execute(
        &client,
        "DELETE FROM route_bindings WHERE route_binding_id = $1",
        &[&route_binding_id],
    )
    .await?;

    Ok(deleted > 0)
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
    let suffixes: Vec<&str> = proper_suffixes(host).collect();
    let rows = sql::query(
        &client,
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
    .await?;
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

/// The proper suffixes of `host`, longest first: the names whose wildcard
/// bindings apply to it.
fn proper_suffixes(host: &str) -> impl Iterator<Item = &str> {
    host.match_indices('.').map(|(index, _)| &host[index + 1..])
}

/// Inserts a binding when every host rule set it joins keeps room for it.
pub(super) async fn insert_route_binding(
    transaction: &Transaction<'_>,
    route_binding_id: &RouteBindingId,
    instance_id: &InstanceId,
    identity: &RouteIdentity,
) -> StoreResult<RouteBindingRecord> {
    let parts = route_identity_parts(identity);
    let route_binding_id = route_binding_id.as_str();
    let instance_id = instance_id.as_str();
    let path_prefix = parts.path_prefix.as_deref();
    let row = sql::query_one(
        transaction,
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
    .await?;
    let created = route_binding_from_row(&row)?;
    check_host_rule_room(transaction, identity).await?;

    Ok(created)
}

// What a binding counts against its rule sets' byte budget, less
// HOST_RULE_OVERHEAD_BYTES.
macro_rules! rule_text_bytes {
    () => {
        "octet_length(route_binding_id) + octet_length(instance_id)
        + octet_length(host) + COALESCE(octet_length(path_prefix), 0)"
    };
}

/// Refuses the binding just inserted when it gives some name a rule set larger
/// than one Subscribe answer carries: more than `MAX_HOST_RULES` rules or
/// `MAX_HOST_RULES_BYTES` bytes.
///
/// A name's rule set holds its exact bindings and the wildcard bindings on its
/// proper suffixes. An exact binding joins its own host's set. A wildcard
/// binding joins the set of every name under its suffix; the largest of those
/// belongs to a bound name, or to an unbound name directly under a wildcard
/// binding at or below the suffix.
///
/// Binding creates take turns on a transaction lock of their own, held until
/// commit, so this check sees every binding committed before this one and a
/// concurrent create waits for this one. Deletes only shrink rule sets and
/// take no part.
async fn check_host_rule_room(
    transaction: &Transaction<'_>,
    identity: &RouteIdentity,
) -> StoreResult<()> {
    #[derive(Clone, Copy, Default)]
    struct Load {
        rules: usize,
        bytes: usize,
    }

    sql::execute(
        transaction,
        "SELECT pg_advisory_xact_lock('route_bindings'::regclass::oid::bigint)",
        &[],
    )
    .await?;
    let kind = route_kind_to_db(identity.kind());
    let host = identity.host().as_str();
    let wildcard = identity.host().is_wildcard();
    let suffixes: Vec<&str> = proper_suffixes(host).collect();
    let mut rows = sql::query(
        transaction,
        concat!(
            "SELECT host_kind, host, ",
            rule_text_bytes!(),
            " AS text_bytes FROM route_bindings
                WHERE identity_kind = $1 AND (host = $2
                    OR (host_kind = 'wildcard_suffix' AND host = ANY($3::text[])))"
        ),
        &[&kind, &host, &suffixes],
    )
    .await?;
    let under = format!(".{host}");
    if wildcard {
        rows.extend(
            sql::query(
                transaction,
                concat!(
                    "SELECT host_kind, host, ",
                    rule_text_bytes!(),
                    " AS text_bytes
                        FROM route_bindings WHERE identity_kind = $1 AND right(host, $2) = $3"
                ),
                &[&kind, &(under.len() as i32), &under],
            )
            .await?,
        );
    }

    let mut exact: HashMap<String, Load> = HashMap::new();
    let mut wildcards: HashMap<String, Load> = HashMap::new();
    for row in &rows {
        let names = if row.get::<_, &str>("host_kind") == "exact" {
            &mut exact
        } else {
            &mut wildcards
        };
        let load = names.entry(row.get("host")).or_default();
        load.rules += 1;
        load.bytes += row.get::<_, i32>("text_bytes") as usize + HOST_RULE_OVERHEAD_BYTES;
    }
    let check = |name: &str, own: Load, names: &str| {
        let load = proper_suffixes(name)
            .filter_map(|suffix| wildcards.get(suffix))
            .fold(own, |total, load| Load {
                rules: total.rules + load.rules,
                bytes: total.bytes + load.bytes,
            });
        let names = format!("{names} {name}");
        if load.rules > MAX_HOST_RULES {
            return Err(StoreError::invalid_argument(format!(
                "the route binding would give {names} more than {MAX_HOST_RULES} route rules"
            )));
        }
        if load.bytes > MAX_HOST_RULES_BYTES {
            return Err(StoreError::invalid_argument(format!(
                "the route binding would give {names} route rules over {MAX_HOST_RULES_BYTES} bytes"
            )));
        }
        Ok(())
    };
    if !wildcard {
        return check(host, exact.get(host).copied().unwrap_or_default(), "host");
    }
    for (name, own) in &exact {
        if name.ends_with(&under) {
            check(name, *own, "host")?;
        }
    }
    for (name, own) in &wildcards {
        if name == host || name.ends_with(&under) {
            check(name, *own, "the names under")?;
        }
    }
    Ok(())
}

async fn load_route_binding_record(
    client: &impl GenericClient,
    route_binding_id: &str,
) -> StoreResult<Option<RouteBindingRecord>> {
    let row = sql::query_opt(
        client,
        "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix
            FROM route_bindings
            WHERE route_binding_id = $1
            ",
        &[&route_binding_id],
    )
    .await?;

    row.as_ref().map(route_binding_from_row).transpose()
}
