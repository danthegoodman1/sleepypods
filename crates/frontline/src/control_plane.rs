use std::{
    error::Error,
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sleepypods_api::{
    pb, BackendEndpoint, BackendGeneration, CachePolicy, Generation, Http01ChallengeKey,
    Http01ChallengeRecord, InstanceId, InstanceState, PathPrefix, RouteBindingId, RouteEntry,
    RouteHost, RouteIdentity, RouteKind, RouteRule,
};

use crate::{
    RouteStreamReply, RouteStreamRequest, SubscribeErrorCode, WakeInstanceRequest,
    WakeInstanceResponse,
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

pub fn route_stream_request_to_proto(request: RouteStreamRequest) -> pb::ProxySubscribeRequest {
    let input = match request {
        RouteStreamRequest::Subscribe { id, kind, host } => {
            pb::proxy_subscribe_request::Input::Subscribe(pb::ProxySubscribeHostRequest {
                subscription_id: id,
                kind: match kind {
                    RouteKind::Http => pb::RouteKind::Http,
                    RouteKind::Sni => pb::RouteKind::Sni,
                } as i32,
                host: host.as_str().to_owned(),
            })
        }
        RouteStreamRequest::Unsubscribe { id } => {
            pb::proxy_subscribe_request::Input::Unsubscribe(pb::ProxyUnsubscribeRequest {
                subscription_id: id,
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

pub fn route_stream_reply_from_proto(
    response: pb::ProxySubscribeResponse,
) -> Result<RouteStreamReply, ProxyProtocolAdapterError> {
    match response
        .output
        .ok_or(ProxyProtocolAdapterError::MissingField { field: "output" })?
    {
        pb::proxy_subscribe_response::Output::Rules(response) => Ok(RouteStreamReply::Rules {
            id: response.subscription_id,
            rules: response
                .rules
                .into_iter()
                .map(route_rule_from_proto)
                .collect::<Result<_, _>>()?,
            cache_policy: cache_policy_from_required_proto(response.cache_policy, "cache_policy")?,
        }),
        pb::proxy_subscribe_response::Output::Error(response) => Ok(RouteStreamReply::Error {
            id: response.subscription_id,
            code: subscribe_error_code_from_proto(response.code)?,
            message: response.message,
        }),
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
                backend: backend_endpoint(response.backend_uri)?,
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

fn backend_endpoint(value: String) -> Result<BackendEndpoint, ProxyProtocolAdapterError> {
    BackendEndpoint::new(value).map_err(|error| ProxyProtocolAdapterError::InvalidField {
        field: error.field(),
        message: error.to_string(),
    })
}

fn route_rule_from_proto(rule: pb::ProxyRouteRule) -> Result<RouteRule, ProxyProtocolAdapterError> {
    Ok(RouteRule {
        identity: route_identity_from_proto(rule.identity.ok_or(
            ProxyProtocolAdapterError::MissingField {
                field: "rule.identity",
            },
        )?)?,
        entry: route_entry_from_proto(rule.route.ok_or(
            ProxyProtocolAdapterError::MissingField {
                field: "rule.route",
            },
        )?)?,
    })
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
    Ok(RouteEntry {
        route_binding_id: route_binding_id(route.route_binding_id)?,
        instance_id: instance_id(route.instance_id)?,
        instance_state: instance_state_from_proto(route.instance_state)?,
        instance_generation: Generation::new(route.instance_generation),
        backend: route.backend_uri.map(backend_endpoint).transpose()?,
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

fn subscribe_error_code_from_proto(
    value: i32,
) -> Result<SubscribeErrorCode, ProxyProtocolAdapterError> {
    match pb::ProxySubscribeErrorCode::try_from(value) {
        Ok(pb::ProxySubscribeErrorCode::InvalidArgument) => Ok(SubscribeErrorCode::InvalidArgument),
        Ok(pb::ProxySubscribeErrorCode::ResourceExhausted) => {
            Ok(SubscribeErrorCode::ResourceExhausted)
        }
        Ok(pb::ProxySubscribeErrorCode::Unavailable) => Ok(SubscribeErrorCode::Unavailable),
        Ok(pb::ProxySubscribeErrorCode::Unspecified) | Err(_) => {
            Err(ProxyProtocolAdapterError::InvalidEnum {
                field: "subscribe_error.code",
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

#[cfg(test)]
mod tests;
