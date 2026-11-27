use serde_json::{json, Value};

use crate::{
    instance::CreateInstanceRequest,
    route::{CreateRouteBindingRequest, RouteBindingSpec, RouteHostKind, RouteIdentity},
    store::{StoreError, StoreResult},
};

pub(crate) const CREATE_INSTANCE_OPERATION: &str = "create_instance";
pub(crate) const CREATE_ROUTE_BINDING_OPERATION: &str = "create_route_binding";

pub(crate) fn create_instance_fingerprint(request: &CreateInstanceRequest) -> StoreResult<Value> {
    let route_bindings = request
        .route_bindings
        .iter()
        .map(route_binding_fingerprint)
        .collect::<StoreResult<Vec<_>>>()?;

    Ok(json!({
        "operation": CREATE_INSTANCE_OPERATION,
        "instance_id": request.instance_id.as_str(),
        "workload_class": {
            "class_id": request.workload_class.class_id.as_str(),
            "version": request.workload_class.version.get(),
        },
        "values": request.values,
        "route_bindings": route_bindings,
    }))
}

pub(crate) fn create_route_binding_fingerprint(
    request: &CreateRouteBindingRequest,
) -> StoreResult<Value> {
    Ok(json!({
        "operation": CREATE_ROUTE_BINDING_OPERATION,
        "route_binding_id": request.route_binding_id.as_str(),
        "instance_id": request.instance_id.as_str(),
        "identity": route_identity_fingerprint(&request.identity),
    }))
}

fn route_binding_fingerprint(spec: &RouteBindingSpec) -> StoreResult<Value> {
    Ok(json!({
        "identity": route_identity_fingerprint(&spec.identity),
    }))
}

fn route_identity_fingerprint(identity: &RouteIdentity) -> Value {
    match identity {
        RouteIdentity::Http { host, path } => json!({
            "kind": "http",
            "host": {
                "kind": host_kind_fingerprint(host.kind()),
                "host": host.as_str(),
            },
            "path": path.as_ref().map(|path| path.as_str()),
        }),
        RouteIdentity::Sni { host } => json!({
            "kind": "sni",
            "host": {
                "kind": host_kind_fingerprint(host.kind()),
                "host": host.as_str(),
            },
        }),
    }
}

fn host_kind_fingerprint(kind: RouteHostKind) -> &'static str {
    match kind {
        RouteHostKind::Exact => "exact",
        RouteHostKind::WildcardSuffix => "wildcard_suffix",
    }
}

pub(crate) fn idempotency_conflict() -> StoreError {
    StoreError::IdempotencyConflict
}

/// Expiry is opt-in per key. Deleting only committed expired records preserves
/// all permanent keys and all keys whose original replay window remains open.
pub(crate) async fn expire_records(
    store: &super::connection::PostgresStore,
    limit: u32,
) -> StoreResult<u64> {
    if limit == 0 || limit > 10_000 {
        return Err(StoreError::invalid_argument(
            "idempotency GC batch must be between 1 and 10000",
        ));
    }
    let client = store.client().await?;
    client
        .execute(
            "
        WITH expired AS (
            SELECT idempotency_key FROM idempotency_records
            WHERE expires_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
            ORDER BY expires_at_unix_millis, idempotency_key
            LIMIT $1 FOR UPDATE SKIP LOCKED
        )
        DELETE FROM idempotency_records USING expired
        WHERE idempotency_records.idempotency_key = expired.idempotency_key
    ",
            &[&i64::from(limit)],
        )
        .await
        .map_err(super::error::map_postgres_error)
}

pub(crate) async fn expire_key(
    client: &impl deadpool_postgres::GenericClient,
    key: &str,
) -> StoreResult<()> {
    client
        .execute(
            "
        DELETE FROM idempotency_records WHERE idempotency_key = $1
            AND expires_at_unix_millis <= (extract(epoch from clock_timestamp()) * 1000)::bigint
    ",
            &[&key],
        )
        .await
        .map_err(super::error::map_postgres_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{create_instance_fingerprint, create_route_binding_fingerprint};
    use crate::{
        ids::{Generation, IdempotencyKey, InstanceId, RouteBindingId, WorkloadClassId},
        instance::CreateInstanceRequest,
        route::{
            CreateRouteBindingRequest, PathPrefix, RouteBindingSpec, RouteHost, RouteIdentity,
        },
        workload::WorkloadClassVersionRef,
    };

    #[test]
    fn create_instance_fingerprint_is_canonical_for_normalized_routes() {
        let request = CreateInstanceRequest::new(
            IdempotencyKey::new("key-1").expect("valid key"),
            InstanceId::new("instance-1").expect("valid instance ID"),
            WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-1").expect("valid class ID"),
                Generation::new(1),
            ),
        )
        .with_route_bindings(vec![RouteBindingSpec::new(RouteIdentity::Http {
            host: RouteHost::exact("App.Example.COM.").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        })]);

        let fingerprint = create_instance_fingerprint(&request).expect("fingerprint builds");

        assert_eq!(
            fingerprint["route_bindings"][0]["identity"]["host"]["host"],
            "app.example.com"
        );
    }

    #[test]
    fn create_route_binding_fingerprint_uses_canonical_route_identity() {
        let first = CreateRouteBindingRequest::new(
            IdempotencyKey::new("key-1").expect("valid key"),
            RouteBindingId::new("route-1").expect("valid route ID"),
            InstanceId::new("instance-1").expect("valid instance ID"),
            RouteIdentity::Http {
                host: RouteHost::exact("App.Example.COM.").expect("valid host"),
                path: Some(PathPrefix::new("/api").expect("valid prefix")),
            },
        );
        let second = CreateRouteBindingRequest::new(
            IdempotencyKey::new("key-1").expect("valid key"),
            RouteBindingId::new("route-1").expect("valid route ID"),
            InstanceId::new("instance-1").expect("valid instance ID"),
            RouteIdentity::Http {
                host: RouteHost::exact("app.example.com").expect("valid host"),
                path: Some(PathPrefix::new("/api").expect("valid prefix")),
            },
        );

        assert_eq!(
            create_route_binding_fingerprint(&first).expect("fingerprint builds"),
            create_route_binding_fingerprint(&second).expect("fingerprint builds")
        );
    }
}
