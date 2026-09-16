use std::{
    error::Error,
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sleepypods_api::{
    pb, BackendAddress, BackendEndpoint, BackendGeneration, CachePolicy, Generation,
    Http01ChallengeKey, Http01ChallengeRecord, InstanceId, InstanceState, PathPrefix,
    RouteBindingId, RouteEntry, RouteHost, RouteHostKind, RouteIdentity,
};

use crate::{
    InvalidationReason, ProxySubscribeInput, RouteRequestId, SubscribeControlPlaneOutput,
    SubscriptionId, WakeInstanceRequest, WakeInstanceResponse,
};

const INVALID_CACHE_TTL_MILLIS: u64 = u64::MAX;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyProtocolAdapterError {
    MissingField {
        field: &'static str,
    },
    InvalidField {
        field: &'static str,
        message: String,
    },
    InvalidEnum {
        field: &'static str,
        value: i32,
    },
    InvalidCacheTtl {
        field: &'static str,
        ttl_millis: u64,
    },
}

pub fn proxy_subscribe_input_to_proto(input: ProxySubscribeInput) -> pb::ProxySubscribeRequest {
    let input = match input {
        ProxySubscribeInput::SubscribeRoute {
            request_id,
            identity,
        } => pb::proxy_subscribe_request::Input::SubscribeRoute(pb::ProxySubscribeRouteRequest {
            request_id: request_id.as_str().to_owned(),
            identity: Some(route_identity_to_proto(identity)),
        }),
        ProxySubscribeInput::Unsubscribe { subscription_id } => {
            pb::proxy_subscribe_request::Input::Unsubscribe(pb::ProxyUnsubscribeRequest {
                subscription_id: subscription_id.as_str().to_owned(),
            })
        }
    };

    pb::ProxySubscribeRequest { input: Some(input) }
}

pub fn wake_instance_request_to_proto(
    request: WakeInstanceRequest,
) -> pb::ProxyWakeInstanceRequest {
    pb::ProxyWakeInstanceRequest {
        instance_id: request.instance_id.as_str().to_owned(),
        expected_generation: request.expected_generation.get(),
        backend_generation: None,
    }
}

pub fn http01_challenge_key_to_proto(key: Http01ChallengeKey) -> pb::Http01ChallengeKey {
    pb::Http01ChallengeKey {
        host: key.host().as_str().to_owned(),
        token: key.token().to_owned(),
    }
}

pub fn http01_challenge_record_from_proto(
    challenge: pb::Http01Challenge,
) -> Result<Http01ChallengeRecord, ProxyProtocolAdapterError> {
    let key = http01_challenge_key_from_required_proto(challenge.key, "challenge.key")?;
    let expires_at = system_time_from_unix_millis(challenge.expires_at_unix_millis)?;

    Http01ChallengeRecord::new(key, challenge.key_authorization, expires_at, UNIX_EPOCH).map_err(
        |error| ProxyProtocolAdapterError::InvalidField {
            field: "challenge",
            message: error.to_string(),
        },
    )
}

pub fn proxy_subscribe_response_from_proto(
    response: pb::ProxySubscribeResponse,
) -> Result<SubscribeControlPlaneOutput, ProxyProtocolAdapterError> {
    match response
        .output
        .ok_or(ProxyProtocolAdapterError::MissingField { field: "output" })?
    {
        pb::proxy_subscribe_response::Output::RouteResolved(response) => {
            Ok(SubscribeControlPlaneOutput::RouteResolved {
                request_id: route_request_id(response.request_id)?,
                subscription_id: subscription_id(response.subscription_id)?,
                matched_identity: route_identity_from_required_proto(
                    response.matched_identity,
                    "matched_identity",
                )?,
                entry: route_entry_from_required_proto(response.route, "route")?,
                cache_policy: cache_policy_from_required_proto(
                    response.cache_policy,
                    "cache_policy",
                )?,
            })
        }
        pb::proxy_subscribe_response::Output::RouteMiss(response) => {
            Ok(SubscribeControlPlaneOutput::RouteMiss {
                request_id: route_request_id(response.request_id)?,
                request_identity: route_identity_from_required_proto(
                    response.request_identity,
                    "request_identity",
                )?,
                negative_cache_policy: cache_policy_from_required_proto(
                    response.negative_cache_policy,
                    "negative_cache_policy",
                )?,
            })
        }
        pb::proxy_subscribe_response::Output::RouteUpdated(response) => {
            Ok(SubscribeControlPlaneOutput::RouteUpdated {
                subscription_id: subscription_id(response.subscription_id)?,
                matched_identity: route_identity_from_required_proto(
                    response.matched_identity,
                    "matched_identity",
                )?,
                entry: route_entry_from_required_proto(response.route, "route")?,
                cache_policy: cache_policy_from_required_proto(
                    response.cache_policy,
                    "cache_policy",
                )?,
            })
        }
        pb::proxy_subscribe_response::Output::RouteInvalidated(response) => {
            Ok(SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id(response.subscription_id)?,
                reason: invalidation_reason_from_proto(response.reason)?,
            })
        }
    }
}

pub fn proxy_wake_response_from_proto(
    response: pb::ProxyWakeInstanceResponse,
) -> Result<WakeInstanceResponse, ProxyProtocolAdapterError> {
    match response
        .outcome
        .ok_or(ProxyProtocolAdapterError::MissingField { field: "outcome" })?
    {
        pb::proxy_wake_instance_response::Outcome::Ready(response) => {
            Ok(WakeInstanceResponse::AlreadyRunning {
                instance_id: instance_id(response.instance_id)?,
                generation: Generation::new(response.instance_generation),
                backend: backend_endpoint(response.backend_uri, response.backend_address)?,
                backend_generation: Some(BackendGeneration::new(response.backend_generation)),
            })
        }
        pb::proxy_wake_instance_response::Outcome::StillWaking(response) => {
            Ok(WakeInstanceResponse::StillWaking {
                instance_id: instance_id(response.instance_id)?,
                generation: Generation::new(response.instance_generation),
            })
        }
        pb::proxy_wake_instance_response::Outcome::Unavailable(response) => {
            Ok(WakeInstanceResponse::Unavailable {
                instance_id: instance_id(response.instance_id)?,
                generation: Generation::new(response.instance_generation),
                reason: wake_unavailable_reason_from_proto(response.reason)?.to_owned(),
            })
        }
        pb::proxy_wake_instance_response::Outcome::GenerationConflict(response) => {
            Ok(WakeInstanceResponse::GenerationConflict {
                instance_id: instance_id(response.instance_id)?,
                expected_generation: Generation::new(response.expected_generation),
                actual_generation: Generation::new(response.actual_generation),
            })
        }
    }
}

impl fmt::Display for ProxyProtocolAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(f, "{field} is required"),
            Self::InvalidField { field, message } => write!(f, "{field} is invalid: {message}"),
            Self::InvalidEnum { field, value } => {
                write!(f, "{field} has invalid enum value {value}")
            }
            Self::InvalidCacheTtl { field, ttl_millis } => {
                write!(f, "{field} ttl_millis {ttl_millis} is not representable")
            }
        }
    }
}

impl Error for ProxyProtocolAdapterError {}

fn route_request_id(value: String) -> Result<RouteRequestId, ProxyProtocolAdapterError> {
    RouteRequestId::new(value).map_err(|error| ProxyProtocolAdapterError::InvalidField {
        field: error.field(),
        message: error.to_string(),
    })
}

fn subscription_id(value: String) -> Result<SubscriptionId, ProxyProtocolAdapterError> {
    SubscriptionId::new(value).map_err(|error| ProxyProtocolAdapterError::InvalidField {
        field: error.field(),
        message: error.to_string(),
    })
}

fn instance_id(value: String) -> Result<InstanceId, ProxyProtocolAdapterError> {
    InstanceId::new(value).map_err(|error| ProxyProtocolAdapterError::InvalidField {
        field: error.field(),
        message: error.to_string(),
    })
}

fn route_binding_id(value: String) -> Result<RouteBindingId, ProxyProtocolAdapterError> {
    RouteBindingId::new(value).map_err(|error| ProxyProtocolAdapterError::InvalidField {
        field: error.field(),
        message: error.to_string(),
    })
}

fn backend_endpoint(
    uri: String,
    address: Option<String>,
) -> Result<BackendEndpoint, ProxyProtocolAdapterError> {
    let invalid =
        |field, message: String| ProxyProtocolAdapterError::InvalidField { field, message };
    match address {
        Some(address) => {
            let address = address
                .parse::<BackendAddress>()
                .map_err(|error| invalid(error.field(), error.to_string()))?;
            BackendEndpoint::with_address(uri, address)
        }
        None => BackendEndpoint::new(uri),
    }
    .map_err(|error| invalid(error.field(), error.to_string()))
}

fn route_identity_from_required_proto(
    identity: Option<pb::RouteIdentity>,
    field: &'static str,
) -> Result<RouteIdentity, ProxyProtocolAdapterError> {
    route_identity_from_proto(identity.ok_or(ProxyProtocolAdapterError::MissingField { field })?)
}

fn route_entry_from_required_proto(
    route: Option<pb::ProxyRouteEntry>,
    field: &'static str,
) -> Result<RouteEntry, ProxyProtocolAdapterError> {
    route_entry_from_proto(route.ok_or(ProxyProtocolAdapterError::MissingField { field })?)
}

fn cache_policy_from_required_proto(
    policy: Option<pb::ProxyCachePolicy>,
    field: &'static str,
) -> Result<CachePolicy, ProxyProtocolAdapterError> {
    cache_policy_from_proto(
        policy.ok_or(ProxyProtocolAdapterError::MissingField { field })?,
        field,
    )
}

fn route_identity_from_proto(
    identity: pb::RouteIdentity,
) -> Result<RouteIdentity, ProxyProtocolAdapterError> {
    match identity
        .kind
        .ok_or(ProxyProtocolAdapterError::MissingField {
            field: "identity.kind",
        })? {
        pb::route_identity::Kind::Http(identity) => Ok(RouteIdentity::Http {
            host: route_host_from_required_proto(identity.host, "http.host")?,
            path: identity
                .path_prefix
                .map(PathPrefix::new)
                .transpose()
                .map_err(|error| ProxyProtocolAdapterError::InvalidField {
                    field: "http.path_prefix",
                    message: error.to_string(),
                })?,
        }),
        pb::route_identity::Kind::Sni(identity) => Ok(RouteIdentity::Sni {
            host: route_host_from_required_proto(identity.host, "sni.host")?,
        }),
    }
}

fn route_host_from_required_proto(
    host: Option<pb::RouteHost>,
    field: &'static str,
) -> Result<RouteHost, ProxyProtocolAdapterError> {
    route_host_from_proto(host.ok_or(ProxyProtocolAdapterError::MissingField { field })?)
}

fn http01_challenge_key_from_required_proto(
    key: Option<pb::Http01ChallengeKey>,
    field: &'static str,
) -> Result<Http01ChallengeKey, ProxyProtocolAdapterError> {
    http01_challenge_key_from_proto(key.ok_or(ProxyProtocolAdapterError::MissingField { field })?)
}

fn http01_challenge_key_from_proto(
    key: pb::Http01ChallengeKey,
) -> Result<Http01ChallengeKey, ProxyProtocolAdapterError> {
    Http01ChallengeKey::new(key.host, key.token).map_err(|error| {
        ProxyProtocolAdapterError::InvalidField {
            field: "http01.key",
            message: error.to_string(),
        }
    })
}

fn system_time_from_unix_millis(value: i64) -> Result<SystemTime, ProxyProtocolAdapterError> {
    if value >= 0 {
        Ok(UNIX_EPOCH + Duration::from_millis(value as u64))
    } else {
        Ok(UNIX_EPOCH - Duration::from_millis(value.unsigned_abs()))
    }
}

fn route_host_from_proto(host: pb::RouteHost) -> Result<RouteHost, ProxyProtocolAdapterError> {
    match pb::RouteHostKind::try_from(host.kind).map_err(|_| {
        ProxyProtocolAdapterError::InvalidEnum {
            field: "route_host.kind",
            value: host.kind,
        }
    })? {
        pb::RouteHostKind::Exact => {
            RouteHost::exact(host.host).map_err(|error| ProxyProtocolAdapterError::InvalidField {
                field: "route_host.host",
                message: error.to_string(),
            })
        }
        pb::RouteHostKind::WildcardSuffix => {
            RouteHost::wildcard_suffix(host.host).map_err(|error| {
                ProxyProtocolAdapterError::InvalidField {
                    field: "route_host.host",
                    message: error.to_string(),
                }
            })
        }
        pb::RouteHostKind::Unspecified => Err(ProxyProtocolAdapterError::InvalidEnum {
            field: "route_host.kind",
            value: host.kind,
        }),
    }
}

fn route_entry_from_proto(
    route: pb::ProxyRouteEntry,
) -> Result<RouteEntry, ProxyProtocolAdapterError> {
    let backend_address = route.backend_address;
    Ok(RouteEntry {
        route_binding_id: route_binding_id(route.route_binding_id)?,
        instance_id: instance_id(route.instance_id)?,
        instance_state: instance_state_from_proto(route.instance_state)?,
        instance_generation: Generation::new(route.instance_generation),
        backend: route
            .backend_uri
            .map(|uri| backend_endpoint(uri, backend_address))
            .transpose()?,
        backend_generation: route.backend_generation.map(BackendGeneration::new),
    })
}

fn instance_state_from_proto(value: i32) -> Result<InstanceState, ProxyProtocolAdapterError> {
    match pb::InstanceState::try_from(value).map_err(|_| {
        ProxyProtocolAdapterError::InvalidEnum {
            field: "instance_state",
            value,
        }
    })? {
        pb::InstanceState::Cold => Ok(InstanceState::Cold),
        pb::InstanceState::Waking => Ok(InstanceState::Waking),
        pb::InstanceState::Running => Ok(InstanceState::Running),
        pb::InstanceState::Draining => Ok(InstanceState::Draining),
        pb::InstanceState::Failed => Ok(InstanceState::Failed),
        pb::InstanceState::Deleting => Ok(InstanceState::Deleting),
        pb::InstanceState::Deleted => Ok(InstanceState::Deleted),
        pb::InstanceState::Unspecified => Err(ProxyProtocolAdapterError::InvalidEnum {
            field: "instance_state",
            value,
        }),
    }
}

fn cache_policy_from_proto(
    policy: pb::ProxyCachePolicy,
    field: &'static str,
) -> Result<CachePolicy, ProxyProtocolAdapterError> {
    // The control plane emits u64::MAX when a domain TTL cannot fit in the
    // protobuf millisecond field; treat that sentinel as malformed input.
    if policy.ttl_millis == INVALID_CACHE_TTL_MILLIS {
        return Err(ProxyProtocolAdapterError::InvalidCacheTtl {
            field,
            ttl_millis: policy.ttl_millis,
        });
    }

    let ttl = Duration::from_millis(policy.ttl_millis);
    if std::time::Instant::now().checked_add(ttl).is_none() {
        return Err(ProxyProtocolAdapterError::InvalidCacheTtl {
            field,
            ttl_millis: policy.ttl_millis,
        });
    }

    Ok(CachePolicy::new(ttl))
}

fn invalidation_reason_from_proto(
    value: i32,
) -> Result<InvalidationReason, ProxyProtocolAdapterError> {
    match pb::ProxyRouteInvalidationReason::try_from(value).map_err(|_| {
        ProxyProtocolAdapterError::InvalidEnum {
            field: "route_invalidation.reason",
            value,
        }
    })? {
        pb::ProxyRouteInvalidationReason::RouteRemoved => Ok(InvalidationReason::RouteRemoved),
        pb::ProxyRouteInvalidationReason::RouteChanged => Ok(InvalidationReason::RouteChanged),
        pb::ProxyRouteInvalidationReason::InstanceChanged => {
            Ok(InvalidationReason::InstanceChanged)
        }
        pb::ProxyRouteInvalidationReason::BackendChanged => Ok(InvalidationReason::BackendChanged),
        pb::ProxyRouteInvalidationReason::StreamClosed => Ok(InvalidationReason::StreamClosed),
        pb::ProxyRouteInvalidationReason::Unspecified => {
            Err(ProxyProtocolAdapterError::InvalidEnum {
                field: "route_invalidation.reason",
                value,
            })
        }
    }
}

fn wake_unavailable_reason_from_proto(
    value: i32,
) -> Result<&'static str, ProxyProtocolAdapterError> {
    match pb::ProxyWakeUnavailableReason::try_from(value).map_err(|_| {
        ProxyProtocolAdapterError::InvalidEnum {
            field: "wake_unavailable.reason",
            value,
        }
    })? {
        pb::ProxyWakeUnavailableReason::Deleting => Ok("deleting"),
        pb::ProxyWakeUnavailableReason::Deleted => Ok("deleted"),
        pb::ProxyWakeUnavailableReason::Unspecified => {
            Err(ProxyProtocolAdapterError::InvalidEnum {
                field: "wake_unavailable.reason",
                value,
            })
        }
    }
}

fn route_identity_to_proto(identity: RouteIdentity) -> pb::RouteIdentity {
    let kind = match identity {
        RouteIdentity::Http { host, path } => {
            pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(route_host_to_proto(host)),
                path_prefix: path.map(|path| path.as_str().to_owned()),
            })
        }
        RouteIdentity::Sni { host } => pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
            host: Some(route_host_to_proto(host)),
        }),
    };

    pb::RouteIdentity { kind: Some(kind) }
}

fn route_host_to_proto(host: RouteHost) -> pb::RouteHost {
    pb::RouteHost {
        kind: route_host_kind_to_proto(host.kind()) as i32,
        host: host.as_str().to_owned(),
    }
}

fn route_host_kind_to_proto(kind: RouteHostKind) -> pb::RouteHostKind {
    match kind {
        RouteHostKind::Exact => pb::RouteHostKind::Exact,
        RouteHostKind::WildcardSuffix => pb::RouteHostKind::WildcardSuffix,
    }
}

#[cfg(test)]
mod tests;
