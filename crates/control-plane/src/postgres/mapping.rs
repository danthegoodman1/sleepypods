use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};
use tokio_postgres::Row;

use crate::{
    http01::{Http01ChallengeKey, Http01ChallengeRecord},
    ids::{
        BackendGeneration, Generation, InstanceId, MaterializationId, RouteBindingId,
        WorkloadClassId,
    },
    instance::{InstanceRecord, InstanceState, InstanceValues},
    manifest::ManifestTemplate,
    materialization::{
        BackendAddress, BackendEndpoint, MaterializationReconciliationLease, MaterializationRecord,
        MaterializationState, MaterializationTarget, RenderedObjectRef,
    },
    route::{
        CachePolicy, PathPrefix, ProtocolRoute, RouteBindingRecord, RouteBindingSpec, RouteHost,
        RouteHostKind, RouteIdentity,
    },
    sleep_policy::{IdleTimeoutOverridePolicy, WorkloadSleepPolicy},
    store::{StoreError, StoreResult},
    workload::{
        RenderedExclusivityKey, WorkloadClassVersion, WorkloadClassVersionRef,
        WorkloadExclusivityKeyTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
    },
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RouteBindingRow {
    pub id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RouteIdentityParts {
    pub key: String,
    pub identity_kind: &'static str,
    pub host_kind: &'static str,
    pub host: String,
    pub path_prefix: Option<String>,
}

pub(crate) fn workload_class_version_from_row(row: &Row) -> StoreResult<WorkloadClassVersion> {
    let class_id: String = row.get("class_id");
    let version: i64 = row.get("version");
    let template_generation: i64 = row.get("template_generation");
    let template: Value = row.get("manifest_template");
    let default_values: Value = row.get("default_values");
    let value_schema: Value = row.get("value_schema");
    let sleep_policy: Value = row.get("sleep_policy");
    let exclusivity_keys: Value = row.get("exclusivity_keys");

    Ok(WorkloadClassVersion {
        reference: WorkloadClassVersionRef {
            class_id: WorkloadClassId::new(class_id).map_err(invalid_stored_data)?,
            version: generation_from_i64(version)?,
        },
        template_generation: generation_from_i64(template_generation)?,
        template: manifest_template_from_json(template)?,
        default_values: values_from_json(default_values)?,
        value_schema: value_schema_from_json(value_schema)?,
        sleep_policy: sleep_policy_from_json(sleep_policy)?,
        exclusivity_keys: workload_exclusivity_keys_from_json(exclusivity_keys)?,
    })
}

pub(crate) fn instance_from_row(row: &Row) -> StoreResult<InstanceRecord> {
    let instance_id: String = row.get("instance_id");
    let workload_class_id: String = row.get("workload_class_id");
    let workload_class_version: i64 = row.get("workload_class_version");
    let values: Value = row.get("values");
    let state: String = row.get("state");
    let generation: i64 = row.get("generation");

    Ok(InstanceRecord {
        id: InstanceId::new(instance_id).map_err(invalid_stored_data)?,
        workload_class: WorkloadClassVersionRef {
            class_id: WorkloadClassId::new(workload_class_id).map_err(invalid_stored_data)?,
            version: generation_from_i64(workload_class_version)?,
        },
        values: values_from_json(values)?,
        state: instance_state_from_db(&state)?,
        generation: generation_from_i64(generation)?,
    })
}

pub(crate) fn route_binding_from_row(row: &Row) -> StoreResult<RouteBindingRecord> {
    let binding = route_binding_row_from_row(row)?;

    Ok(RouteBindingRecord {
        id: binding.id,
        instance_id: binding.instance_id,
        identity: binding.identity,
        protocol: binding.protocol,
    })
}

pub(crate) fn route_binding_row_from_row(row: &Row) -> StoreResult<RouteBindingRow> {
    let route_binding_id: String = row.get("route_binding_id");
    let instance_id: String = row.get("instance_id");
    let identity_kind: String = row.get("identity_kind");
    let host_kind: String = row.get("host_kind");
    let host: String = row.get("host");
    let path_prefix: Option<String> = row.get("path_prefix");
    let protocol: String = row.get("protocol");

    Ok(RouteBindingRow {
        id: RouteBindingId::new(route_binding_id).map_err(invalid_stored_data)?,
        instance_id: InstanceId::new(instance_id).map_err(invalid_stored_data)?,
        identity: route_identity_from_parts(&identity_kind, &host_kind, &host, path_prefix)?,
        protocol: protocol_from_db(&protocol)?,
    })
}

/// A stored backend keeps its observed address when one was recorded. Rows
/// written before the address column, and sleeping materializations, carry only
/// the URI.
pub(crate) fn backend_endpoint_from_row(
    uri: Option<String>,
    address: Option<String>,
) -> StoreResult<Option<BackendEndpoint>> {
    let Some(uri) = uri else {
        return Ok(None);
    };
    let backend = match address {
        Some(address) => {
            let address = address
                .parse::<BackendAddress>()
                .map_err(invalid_stored_data)?;
            BackendEndpoint::with_address(uri, address)
        }
        None => BackendEndpoint::new(uri),
    }
    .map_err(invalid_stored_data)?;

    Ok(Some(backend))
}

pub(crate) fn materialization_from_row(row: &Row) -> StoreResult<MaterializationRecord> {
    let materialization_id: String = row.get("materialization_id");
    let instance_id: String = row.get("instance_id");
    let instance_generation: i64 = row.get("instance_generation");
    let cluster_id: String = row.get("cluster_id");
    let namespace: String = row.get("namespace");
    let state: String = row.get("state");
    let backend_uri: Option<String> = row.get("backend_uri");
    let backend_address: Option<String> = row.get("backend_address");
    let backend_generation: i64 = row.get("backend_generation");
    let rendered_objects: Value = row.get("rendered_objects");
    let exclusivity_keys: Value = row.get("exclusivity_keys");
    let reconcile_owner: Option<String> = row.get("reconcile_owner");
    let reconcile_lease_expires_at_unix_millis: Option<i64> =
        row.get("reconcile_lease_expires_at_unix_millis");
    let reconcile_attempt: i64 = row.get("reconcile_attempt");

    Ok(MaterializationRecord {
        id: MaterializationId::new(materialization_id).map_err(invalid_stored_data)?,
        instance_id: InstanceId::new(instance_id).map_err(invalid_stored_data)?,
        instance_generation: generation_from_i64(instance_generation)?,
        projection_generation: generation_from_i64(row.get("projection_generation"))?,
        target: MaterializationTarget::new(cluster_id, namespace).map_err(invalid_stored_data)?,
        state: materialization_state_from_db(&state)?,
        backend: backend_endpoint_from_row(backend_uri, backend_address)?,
        backend_generation: backend_generation_from_i64(backend_generation)?,
        rendered_objects: rendered_objects_from_json(rendered_objects)?,
        exclusivity_keys: rendered_exclusivity_keys_from_json(exclusivity_keys)?,
        reconciliation_lease: reconcile_owner
            .map(|owner| {
                Ok(MaterializationReconciliationLease {
                    owner,
                    expires_at: system_time_from_unix_millis(
                        reconcile_lease_expires_at_unix_millis.ok_or_else(|| {
                            StoreError::internal("stored materialization lease owner had no expiry")
                        })?,
                    )?,
                    attempt: u64::try_from(reconcile_attempt).map_err(|_| {
                        StoreError::internal(
                            "stored materialization reconcile attempt was negative",
                        )
                    })?,
                })
            })
            .transpose()?,
    })
}

pub(crate) fn http01_from_row(row: &Row) -> StoreResult<Http01ChallengeRecord> {
    let host: String = row.get("host");
    let token: String = row.get("token");
    let key_authorization: String = row.get("key_authorization");
    let expires_at_unix_millis: i64 = row.get("expires_at_unix_millis");
    let expires_at = system_time_from_unix_millis(expires_at_unix_millis)?;
    let key = Http01ChallengeKey::new(host, token).map_err(invalid_stored_data)?;

    Http01ChallengeRecord::new(key, key_authorization, expires_at, UNIX_EPOCH)
        .map_err(invalid_stored_data)
}

pub(crate) fn values_to_json(values: &InstanceValues) -> StoreResult<Value> {
    serde_json::to_value(values)
        .map_err(|error| StoreError::internal(format!("failed to encode instance values: {error}")))
}

pub(crate) fn values_from_json(value: Value) -> StoreResult<InstanceValues> {
    serde_json::from_value::<BTreeMap<String, String>>(value).map_err(|error| {
        StoreError::internal(format!(
            "stored instance values were not a string map: {error}"
        ))
    })
}

pub(crate) fn manifest_template_to_json(template: &ManifestTemplate) -> StoreResult<Value> {
    serde_json::to_value(template).map_err(|error| {
        StoreError::internal(format!("failed to encode manifest template: {error}"))
    })
}

pub(crate) fn manifest_template_from_json(value: Value) -> StoreResult<ManifestTemplate> {
    serde_json::from_value(value).map_err(|error| {
        StoreError::internal(format!(
            "stored manifest template was not a supported template shape: {error}"
        ))
    })
}

pub(crate) fn value_schema_to_json(schema: &WorkloadValueSchema) -> Value {
    let fields = schema
        .fields
        .iter()
        .map(|(field, rule)| {
            let mut rule_json = serde_json::Map::new();
            rule_json.insert("required".to_owned(), Value::Bool(rule.required));
            if let Some(default) = &rule.default {
                rule_json.insert("default".to_owned(), Value::String(default.clone()));
            }

            (field.clone(), Value::Object(rule_json))
        })
        .collect();

    json!({
        "allow_extra": schema.allow_extra,
        "fields": Value::Object(fields),
    })
}

pub(crate) fn value_schema_from_json(value: Value) -> StoreResult<WorkloadValueSchema> {
    let Value::Object(mut object) = value else {
        return Err(StoreError::internal(
            "stored workload value schema was not an object",
        ));
    };

    let allow_extra = take_json_bool(&mut object, "allow_extra")?;
    let fields = match object.remove("fields") {
        Some(Value::Object(fields)) => fields,
        _ => {
            return Err(StoreError::internal(
                "stored workload value schema fields were not an object",
            ))
        }
    };
    let mut schema = WorkloadValueSchema::new(allow_extra);

    for (field, value) in fields {
        let Value::Object(mut rule) = value else {
            return Err(StoreError::internal(format!(
                "stored workload value schema field {field:?} was not an object"
            )));
        };
        schema.fields.insert(
            field,
            WorkloadValueFieldRule {
                required: take_json_bool(&mut rule, "required")?,
                default: take_json_optional_string(&mut rule, "default")?,
            },
        );
    }

    Ok(schema)
}

pub(crate) fn sleep_policy_to_json(policy: &WorkloadSleepPolicy) -> StoreResult<Value> {
    policy
        .validate()
        .map_err(|error| StoreError::invalid_argument(error.to_string()))?;
    let idle_timeout_override = policy
        .idle_timeout_override
        .as_ref()
        .map(|override_policy| {
            json!({
                "value_field": override_policy.value_field.clone(),
                "min_idle_timeout_ms": override_policy.min_idle_timeout_ms,
                "max_idle_timeout_ms": override_policy.max_idle_timeout_ms,
            })
        });

    Ok(json!({
        "idle_timeout_ms": policy.idle_timeout_ms,
        "idle_retry_backoff_ms": policy.idle_retry_backoff_ms,
        "drain_grace_timeout_ms": policy.drain_grace_timeout_ms,
        "idle_timeout_override": idle_timeout_override,
    }))
}

pub(crate) fn sleep_policy_from_json(value: Value) -> StoreResult<WorkloadSleepPolicy> {
    let Value::Object(mut object) = value else {
        return Err(StoreError::internal(
            "stored workload sleep policy was not an object",
        ));
    };

    let idle_timeout_ms = take_json_u64(&mut object, "idle_timeout_ms")?;
    let idle_retry_backoff_ms = take_json_u64(&mut object, "idle_retry_backoff_ms")?;
    let drain_grace_timeout_ms = take_json_u64(&mut object, "drain_grace_timeout_ms")?;
    let idle_timeout_override = match object.remove("idle_timeout_override") {
        Some(Value::Object(mut object)) => Some(IdleTimeoutOverridePolicy {
            value_field: take_json_string(&mut object, "value_field")?,
            min_idle_timeout_ms: take_json_u64(&mut object, "min_idle_timeout_ms")?,
            max_idle_timeout_ms: take_json_u64(&mut object, "max_idle_timeout_ms")?,
        }),
        Some(Value::Null) | None => None,
        _ => {
            return Err(StoreError::internal(
                "stored workload sleep policy override was not an object",
            ))
        }
    };
    let policy = WorkloadSleepPolicy {
        idle_timeout_ms,
        idle_retry_backoff_ms,
        drain_grace_timeout_ms,
        idle_timeout_override,
    };
    policy.validate().map_err(invalid_stored_data)?;

    Ok(policy)
}

pub(crate) fn workload_exclusivity_keys_to_json(
    keys: &[WorkloadExclusivityKeyTemplate],
) -> StoreResult<Value> {
    serde_json::to_value(keys).map_err(|error| {
        StoreError::internal(format!(
            "failed to encode workload exclusivity keys: {error}"
        ))
    })
}

pub(crate) fn workload_exclusivity_keys_from_json(
    value: Value,
) -> StoreResult<Vec<WorkloadExclusivityKeyTemplate>> {
    serde_json::from_value(value).map_err(|error| {
        StoreError::internal(format!(
            "stored workload exclusivity keys were not supported key templates: {error}"
        ))
    })
}

pub(crate) fn rendered_objects_to_json(objects: &[RenderedObjectRef]) -> Value {
    Value::Array(
        objects
            .iter()
            .map(|object| {
                json!({
                    "api_version": object.api_version,
                    "kind": object.kind,
                    "namespace": object.namespace,
                    "name": object.name,
                })
            })
            .collect(),
    )
}

pub(crate) fn rendered_objects_from_json(value: Value) -> StoreResult<Vec<RenderedObjectRef>> {
    let Value::Array(values) = value else {
        return Err(StoreError::internal(
            "stored rendered objects were not an array",
        ));
    };

    values
        .into_iter()
        .map(|value| {
            let Value::Object(mut object) = value else {
                return Err(StoreError::internal(
                    "stored rendered object entry was not an object",
                ));
            };

            Ok(RenderedObjectRef {
                api_version: take_json_string(&mut object, "api_version")?,
                kind: take_json_string(&mut object, "kind")?,
                namespace: take_json_string(&mut object, "namespace")?,
                name: take_json_string(&mut object, "name")?,
            })
        })
        .collect()
}

pub(crate) fn rendered_exclusivity_keys_to_json(keys: &[RenderedExclusivityKey]) -> Value {
    Value::Array(
        keys.iter()
            .map(|key| {
                json!({
                    "name": key.name,
                    "value": key.value,
                })
            })
            .collect(),
    )
}

pub(crate) fn rendered_exclusivity_keys_from_json(
    value: Value,
) -> StoreResult<Vec<RenderedExclusivityKey>> {
    let Value::Array(values) = value else {
        return Err(StoreError::internal(
            "stored rendered exclusivity keys were not an array",
        ));
    };

    values
        .into_iter()
        .map(|value| {
            let Value::Object(mut object) = value else {
                return Err(StoreError::internal(
                    "stored rendered exclusivity key entry was not an object",
                ));
            };

            Ok(RenderedExclusivityKey {
                name: take_json_string(&mut object, "name")?,
                value: take_json_string(&mut object, "value")?,
            })
        })
        .collect()
}

pub(crate) fn route_identity_parts(identity: &RouteIdentity) -> RouteIdentityParts {
    match identity {
        RouteIdentity::Http { host, path } => RouteIdentityParts {
            key: route_identity_key(identity),
            identity_kind: "http",
            host_kind: route_host_kind_to_db(host.kind()),
            host: host.as_str().to_owned(),
            path_prefix: path.as_ref().map(|path| path.as_str().to_owned()),
        },
        RouteIdentity::Sni { host } => RouteIdentityParts {
            key: route_identity_key(identity),
            identity_kind: "sni",
            host_kind: route_host_kind_to_db(host.kind()),
            host: host.as_str().to_owned(),
            path_prefix: None,
        },
    }
}

pub(crate) fn route_identity_key(identity: &RouteIdentity) -> String {
    match identity {
        RouteIdentity::Http { host, path } => format!(
            "http:{}:{}:{}",
            route_host_kind_to_db(host.kind()),
            host.as_str(),
            path.as_ref().map(PathPrefix::as_str).unwrap_or("")
        ),
        RouteIdentity::Sni { host } => {
            format!(
                "sni:{}:{}",
                route_host_kind_to_db(host.kind()),
                host.as_str()
            )
        }
    }
}

pub(crate) fn validate_route_protocol(spec: &RouteBindingSpec) -> StoreResult<()> {
    match (&spec.identity, spec.protocol) {
        (RouteIdentity::Http { .. }, ProtocolRoute::Http)
        | (RouteIdentity::Sni { .. }, ProtocolRoute::TlsSni) => Ok(()),
        (RouteIdentity::Http { .. }, ProtocolRoute::TlsSni) => Err(StoreError::invalid_argument(
            "HTTP route identities must use the HTTP protocol route",
        )),
        (RouteIdentity::Sni { .. }, ProtocolRoute::Http) => Err(StoreError::invalid_argument(
            "SNI route identities must use the TLS SNI protocol route",
        )),
    }
}

pub(crate) fn materialization_id(
    instance_id: &InstanceId,
    target: &MaterializationTarget,
) -> StoreResult<MaterializationId> {
    MaterializationId::new(format!(
        "{}:{}:{}",
        instance_id.as_str(),
        target.cluster_id(),
        target.namespace()
    ))
    .map_err(invalid_stored_data)
}

pub(crate) fn instance_state_to_db(state: InstanceState) -> &'static str {
    match state {
        InstanceState::Cold => "cold",
        InstanceState::Waking => "waking",
        InstanceState::Running => "running",
        InstanceState::Draining => "draining",
        InstanceState::Failed => "failed",
        InstanceState::Deleting => "deleting",
        InstanceState::Deleted => "deleted",
    }
}

pub(crate) fn materialization_state_to_db(state: MaterializationState) -> &'static str {
    match state {
        MaterializationState::Pending => "pending",
        MaterializationState::Ready => "ready",
        MaterializationState::Failed => "failed",
        MaterializationState::Deleting => "deleting",
        MaterializationState::Deleted => "deleted",
    }
}

pub(crate) fn protocol_to_db(protocol: ProtocolRoute) -> &'static str {
    match protocol {
        ProtocolRoute::Http => "http",
        ProtocolRoute::TlsSni => "tls_sni",
    }
}

pub(crate) fn generation_to_i64(value: Generation) -> StoreResult<i64> {
    i64::try_from(value.get())
        .map_err(|_| StoreError::invalid_argument("generation does not fit in Postgres bigint"))
}

pub(crate) fn backend_generation_to_i64(value: BackendGeneration) -> StoreResult<i64> {
    i64::try_from(value.get()).map_err(|_| {
        StoreError::invalid_argument("backend generation does not fit in Postgres bigint")
    })
}

pub(crate) fn unix_millis_from_system_time(value: SystemTime) -> StoreResult<i64> {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| StoreError::invalid_argument("system time does not fit in unix millis")),
        Err(error) => {
            let millis = i64::try_from(error.duration().as_millis()).map_err(|_| {
                StoreError::invalid_argument("system time does not fit in unix millis")
            })?;
            Ok(-millis)
        }
    }
}

pub(crate) fn default_negative_cache_policy() -> CachePolicy {
    CachePolicy::new(Duration::from_secs(1))
}

fn route_identity_from_parts(
    identity_kind: &str,
    host_kind: &str,
    host: &str,
    path_prefix: Option<String>,
) -> StoreResult<RouteIdentity> {
    let host = route_host_from_db(host_kind, host)?;
    match identity_kind {
        "http" => Ok(RouteIdentity::Http {
            host,
            path: path_prefix
                .map(PathPrefix::new)
                .transpose()
                .map_err(invalid_stored_data)?,
        }),
        "sni" => Ok(RouteIdentity::Sni { host }),
        other => Err(StoreError::internal(format!(
            "stored route identity kind {other:?} is invalid"
        ))),
    }
}

fn route_host_from_db(host_kind: &str, host: &str) -> StoreResult<RouteHost> {
    match host_kind {
        "exact" => RouteHost::exact(host).map_err(invalid_stored_data),
        "wildcard_suffix" => RouteHost::wildcard_suffix(host).map_err(invalid_stored_data),
        other => Err(StoreError::internal(format!(
            "stored route host kind {other:?} is invalid"
        ))),
    }
}

fn route_host_kind_to_db(kind: RouteHostKind) -> &'static str {
    match kind {
        RouteHostKind::Exact => "exact",
        RouteHostKind::WildcardSuffix => "wildcard_suffix",
    }
}

pub(crate) fn instance_state_from_db(value: &str) -> StoreResult<InstanceState> {
    match value {
        "cold" => Ok(InstanceState::Cold),
        "waking" => Ok(InstanceState::Waking),
        "running" => Ok(InstanceState::Running),
        "draining" => Ok(InstanceState::Draining),
        "failed" => Ok(InstanceState::Failed),
        "deleting" => Ok(InstanceState::Deleting),
        "deleted" => Ok(InstanceState::Deleted),
        other => Err(StoreError::internal(format!(
            "stored instance state {other:?} is invalid"
        ))),
    }
}

pub(crate) fn materialization_state_from_db(value: &str) -> StoreResult<MaterializationState> {
    match value {
        "pending" => Ok(MaterializationState::Pending),
        "ready" => Ok(MaterializationState::Ready),
        "failed" => Ok(MaterializationState::Failed),
        "deleting" => Ok(MaterializationState::Deleting),
        "deleted" => Ok(MaterializationState::Deleted),
        other => Err(StoreError::internal(format!(
            "stored materialization state {other:?} is invalid"
        ))),
    }
}

fn protocol_from_db(value: &str) -> StoreResult<ProtocolRoute> {
    match value {
        "http" => Ok(ProtocolRoute::Http),
        "tls_sni" => Ok(ProtocolRoute::TlsSni),
        other => Err(StoreError::internal(format!(
            "stored route protocol {other:?} is invalid"
        ))),
    }
}

pub(crate) fn generation_from_i64(value: i64) -> StoreResult<Generation> {
    u64::try_from(value)
        .map(Generation::new)
        .map_err(|_| StoreError::internal(format!("stored generation {value} is invalid")))
}

pub(crate) fn backend_generation_from_i64(value: i64) -> StoreResult<BackendGeneration> {
    u64::try_from(value)
        .map(BackendGeneration::new)
        .map_err(|_| StoreError::internal(format!("stored backend generation {value} is invalid")))
}

fn system_time_from_unix_millis(value: i64) -> StoreResult<SystemTime> {
    if value >= 0 {
        Ok(UNIX_EPOCH + Duration::from_millis(value as u64))
    } else {
        Ok(UNIX_EPOCH - Duration::from_millis(value.unsigned_abs()))
    }
}

fn take_json_string(
    object: &mut serde_json::Map<String, Value>,
    field: &'static str,
) -> StoreResult<String> {
    match object.remove(field) {
        Some(Value::String(value)) => Ok(value),
        _ => Err(StoreError::internal(format!(
            "stored rendered object field {field:?} was not a string"
        ))),
    }
}

fn take_json_optional_string(
    object: &mut serde_json::Map<String, Value>,
    field: &'static str,
) -> StoreResult<Option<String>> {
    match object.remove(field) {
        Some(Value::String(value)) => Ok(Some(value)),
        Some(Value::Null) | None => Ok(None),
        _ => Err(StoreError::internal(format!(
            "stored field {field:?} was not a string"
        ))),
    }
}

fn take_json_bool(
    object: &mut serde_json::Map<String, Value>,
    field: &'static str,
) -> StoreResult<bool> {
    match object.remove(field) {
        Some(Value::Bool(value)) => Ok(value),
        _ => Err(StoreError::internal(format!(
            "stored field {field:?} was not a boolean"
        ))),
    }
}

fn take_json_u64(
    object: &mut serde_json::Map<String, Value>,
    field: &'static str,
) -> StoreResult<u64> {
    match object.remove(field).and_then(|value| value.as_u64()) {
        Some(value) => Ok(value),
        None => Err(StoreError::internal(format!(
            "stored field {field:?} was not an unsigned integer"
        ))),
    }
}

fn invalid_stored_data(error: impl std::fmt::Display) -> StoreError {
    StoreError::internal(format!("stored Postgres data is invalid: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{backend_endpoint_from_row, route_identity_key};
    use crate::materialization::BackendAddress;
    use crate::route::{
        route_match_score, PathPrefix, ProtocolRoute, RouteBindingSpec, RouteHost, RouteIdentity,
    };

    #[test]
    fn stored_backend_without_a_uri_is_absent() {
        assert_eq!(
            backend_endpoint_from_row(None, None).expect("absent backend is valid"),
            None
        );
        assert_eq!(
            backend_endpoint_from_row(None, Some("10.244.1.7:8080".to_owned()))
                .expect("absent backend is valid"),
            None
        );
    }

    #[test]
    fn stored_backend_keeps_its_uri_when_no_address_was_recorded() {
        let backend = backend_endpoint_from_row(Some("http://app.apps.svc:80".to_owned()), None)
            .expect("backend is valid")
            .expect("backend is present");

        assert_eq!(backend.uri(), "http://app.apps.svc:80");
        assert_eq!(backend.address(), None);
    }

    #[test]
    fn stored_backend_restores_a_recorded_address() {
        for value in ["10.244.1.7:8080", "[fd00::7]:8080"] {
            let backend = backend_endpoint_from_row(
                Some("http://app.apps.svc:80".to_owned()),
                Some(value.to_owned()),
            )
            .expect("backend is valid")
            .expect("backend is present");

            assert_eq!(
                backend.address(),
                Some(value.parse::<BackendAddress>().expect("valid address"))
            );
        }
    }

    #[test]
    fn stored_backend_rejects_an_address_that_cannot_be_dialed() {
        for value in [
            "not-an-address",
            "10.244.1.7",
            "0.0.0.0:8080",
            "10.244.1.7:0",
        ] {
            let error = backend_endpoint_from_row(
                Some("http://app.apps.svc:80".to_owned()),
                Some(value.to_owned()),
            )
            .expect_err("stored address is invalid");

            assert!(error
                .to_string()
                .contains("stored Postgres data is invalid"));
        }
    }

    #[test]
    fn route_identity_key_uses_normalized_domain_parts() {
        let identity = RouteIdentity::Http {
            host: RouteHost::wildcard_suffix("*.Example.COM.").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        };

        assert_eq!(
            route_identity_key(&identity),
            "http:wildcard_suffix:example.com:/api"
        );
    }

    #[test]
    fn route_match_prefers_specific_hosts_and_paths() {
        let exact = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        };
        let wildcard = RouteIdentity::Http {
            host: RouteHost::wildcard_suffix("example.com").expect("valid host"),
            path: None,
        };
        let lookup = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        };

        assert!(route_match_score(&exact, &lookup) > route_match_score(&wildcard, &lookup));
    }

    #[test]
    fn exact_host_beats_wildcard_even_when_wildcard_has_longer_path() {
        let exact = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: None,
        };
        let wildcard = RouteIdentity::Http {
            host: RouteHost::wildcard_suffix("example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        };
        let lookup = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1/users").expect("valid prefix")),
        };

        assert!(route_match_score(&exact, &lookup) > route_match_score(&wildcard, &lookup));
    }

    #[test]
    fn more_specific_wildcard_suffix_beats_broader_wildcard() {
        let broad = RouteIdentity::Http {
            host: RouteHost::wildcard_suffix("example.com").expect("valid host"),
            path: None,
        };
        let specific = RouteIdentity::Http {
            host: RouteHost::wildcard_suffix("customer.example.com").expect("valid host"),
            path: None,
        };
        let lookup = RouteIdentity::Http {
            host: RouteHost::exact("app.customer.example.com").expect("valid host"),
            path: None,
        };

        assert!(route_match_score(&specific, &lookup) > route_match_score(&broad, &lookup));
    }

    #[test]
    fn longest_path_prefix_wins_within_selected_host_rule() {
        let short = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        };
        let long = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        };
        let lookup = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1/users").expect("valid prefix")),
        };

        assert!(route_match_score(&long, &lookup) > route_match_score(&short, &lookup));
    }

    #[test]
    fn path_prefix_matching_respects_segment_boundaries() {
        let api = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        };
        let exact = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        };
        let child = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/child").expect("valid prefix")),
        };
        let near_miss = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/apix").expect("valid prefix")),
        };

        assert!(route_match_score(&api, &exact).is_some());
        assert!(route_match_score(&api, &child).is_some());
        assert!(route_match_score(&api, &near_miss).is_none());
    }

    #[test]
    fn sni_routes_support_exact_and_wildcard_matching() {
        let exact = RouteIdentity::Sni {
            host: RouteHost::exact("db.example.com").expect("valid host"),
        };
        let wildcard = RouteIdentity::Sni {
            host: RouteHost::wildcard_suffix("example.com").expect("valid host"),
        };
        let lookup = RouteIdentity::Sni {
            host: RouteHost::exact("db.example.com").expect("valid host"),
        };
        let miss = RouteIdentity::Sni {
            host: RouteHost::exact("example.com").expect("valid host"),
        };

        assert!(route_match_score(&exact, &lookup) > route_match_score(&wildcard, &lookup));
        assert!(route_match_score(&wildcard, &miss).is_none());
    }

    #[test]
    fn route_protocol_validation_rejects_mismatched_identity() {
        let spec = RouteBindingSpec::new(
            RouteIdentity::Sni {
                host: RouteHost::exact("db.example.com").expect("valid host"),
            },
            ProtocolRoute::Http,
        );

        assert!(super::validate_route_protocol(&spec).is_err());
    }
}
