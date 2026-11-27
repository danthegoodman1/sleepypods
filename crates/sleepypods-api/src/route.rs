use std::{error::Error, fmt, time::Duration};

use crate::{
    ids::{BackendGeneration, Generation, InstanceId, RouteBindingId},
    instance::InstanceState,
    materialization::{BackendEndpoint, MaterializationTarget},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouteIdentity {
    Http {
        host: RouteHost,
        path: Option<PathPrefix>,
    },
    Sni {
        host: RouteHost,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteBindingSpec {
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteBindingRecord {
    pub id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRouteBindingRequest {
    pub route_binding_id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetRouteBindingRequest {
    pub route_binding_id: RouteBindingId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteRouteBindingRequest {
    pub route_binding_id: RouteBindingId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListRouteBindingsForInstanceRequest {
    pub instance_id: InstanceId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveRouteRequest {
    pub identity: RouteIdentity,
    pub target: MaterializationTarget,
}

impl ResolveRouteRequest {
    pub fn new(identity: RouteIdentity, target: MaterializationTarget) -> Self {
        Self { identity, target }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteResolution {
    Resolved {
        matched_identity: RouteIdentity,
        entry: RouteEntry,
    },
    Miss {
        negative_cache: CachePolicy,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteEntry {
    pub route_binding_id: RouteBindingId,
    pub instance_id: InstanceId,
    pub instance_state: InstanceState,
    pub instance_generation: Generation,
    pub backend: Option<BackendEndpoint>,
    pub backend_generation: Option<BackendGeneration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteDependencyLookup {
    route_binding_id: RouteBindingId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteDependencySet {
    pub route_binding_id: RouteBindingId,
    pub instance_id: InstanceId,
    pub materialization_generation: Option<BackendGeneration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RouteMatchScore {
    host: usize,
    path: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtocolRoute {
    Http,
    TlsSni,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouteHost {
    kind: RouteHostKind,
    host: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteHostKind {
    Exact,
    WildcardSuffix,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathPrefix(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CachePolicy {
    ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidRouteHost {
    reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidPathPrefix {
    value: String,
}

impl RouteBindingSpec {
    pub fn new(identity: RouteIdentity, protocol: ProtocolRoute) -> Self {
        Self { identity, protocol }
    }
}

impl CreateRouteBindingRequest {
    pub fn new(
        route_binding_id: RouteBindingId,
        instance_id: InstanceId,
        identity: RouteIdentity,
        protocol: ProtocolRoute,
    ) -> Self {
        Self {
            route_binding_id,
            instance_id,
            identity,
            protocol,
        }
    }

    pub fn spec(&self) -> RouteBindingSpec {
        RouteBindingSpec::new(self.identity.clone(), self.protocol)
    }
}

impl GetRouteBindingRequest {
    pub fn new(route_binding_id: RouteBindingId) -> Self {
        Self { route_binding_id }
    }
}

impl DeleteRouteBindingRequest {
    pub fn new(route_binding_id: RouteBindingId) -> Self {
        Self { route_binding_id }
    }
}

impl ListRouteBindingsForInstanceRequest {
    pub fn new(instance_id: InstanceId) -> Self {
        Self { instance_id }
    }
}

impl RouteDependencyLookup {
    pub fn new(route_binding_id: RouteBindingId) -> Self {
        Self { route_binding_id }
    }

    pub fn from_route_entry(entry: &RouteEntry) -> Self {
        Self::new(entry.route_binding_id.clone())
    }

    pub fn route_binding_id(&self) -> &RouteBindingId {
        &self.route_binding_id
    }
}

pub fn route_match_score(
    binding: &RouteIdentity,
    lookup: &RouteIdentity,
) -> Option<RouteMatchScore> {
    match (binding, lookup) {
        (
            RouteIdentity::Http {
                host: binding_host,
                path: binding_path,
            },
            RouteIdentity::Http {
                host: lookup_host,
                path: lookup_path,
            },
        ) => host_score(binding_host, lookup_host).and_then(|host| {
            path_score(binding_path.as_ref(), lookup_path.as_ref())
                .map(|path| RouteMatchScore { host, path })
        }),
        (RouteIdentity::Sni { host: binding_host }, RouteIdentity::Sni { host: lookup_host }) => {
            host_score(binding_host, lookup_host).map(|host| RouteMatchScore { host, path: 0 })
        }
        _ => None,
    }
}

impl RouteHost {
    pub fn exact(host: impl AsRef<str>) -> Result<Self, InvalidRouteHost> {
        Ok(Self {
            kind: RouteHostKind::Exact,
            host: normalize_host(host.as_ref(), false)?,
        })
    }

    pub fn wildcard_suffix(host: impl AsRef<str>) -> Result<Self, InvalidRouteHost> {
        Ok(Self {
            kind: RouteHostKind::WildcardSuffix,
            host: normalize_host(host.as_ref(), true)?,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.host
    }

    pub fn kind(&self) -> RouteHostKind {
        self.kind
    }

    pub fn is_wildcard(&self) -> bool {
        self.kind == RouteHostKind::WildcardSuffix
    }
}

impl PathPrefix {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidPathPrefix> {
        let value = value.into();
        if value.starts_with('/') {
            return Ok(Self(value));
        }

        Err(InvalidPathPrefix { value })
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl CachePolicy {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl InvalidRouteHost {
    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl fmt::Display for InvalidRouteHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason)
    }
}

impl Error for InvalidRouteHost {}

impl InvalidPathPrefix {
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for InvalidPathPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "route path prefix {:?} must start with /", self.value)
    }
}

impl Error for InvalidPathPrefix {}

fn normalize_host(value: &str, wildcard: bool) -> Result<String, InvalidRouteHost> {
    let host = value.trim().trim_end_matches('.').to_ascii_lowercase();
    let host = if wildcard {
        host.strip_prefix("*.").unwrap_or(&host).to_owned()
    } else {
        host
    };

    if host.is_empty() {
        return Err(InvalidRouteHost {
            reason: "route host must not be empty",
        });
    }

    if host.contains('*') {
        return Err(InvalidRouteHost {
            reason: "route host wildcard is only allowed as a leading wildcard suffix",
        });
    }

    if !host.contains('.') {
        return Err(InvalidRouteHost {
            reason: "route host must contain at least one dot",
        });
    }

    Ok(host)
}

fn host_score(binding: &RouteHost, lookup: &RouteHost) -> Option<usize> {
    match binding.kind() {
        RouteHostKind::Exact if binding.as_str() == lookup.as_str() => Some(usize::MAX),
        RouteHostKind::Exact => None,
        RouteHostKind::WildcardSuffix => {
            let lookup_host = lookup.as_str();
            let suffix = binding.as_str();
            let suffix_start = lookup_host.len().checked_sub(suffix.len())?;
            let prefix = lookup_host.get(..suffix_start)?;
            if lookup_host.ends_with(suffix) && prefix.ends_with('.') {
                Some(suffix.len())
            } else {
                None
            }
        }
    }
}

fn path_score(binding: Option<&PathPrefix>, lookup: Option<&PathPrefix>) -> Option<usize> {
    match (binding, lookup) {
        (None, _) => Some(0),
        (Some(_), None) => None,
        (Some(binding), Some(lookup)) if path_prefix_matches(lookup.as_str(), binding.as_str()) => {
            Some(binding.as_str().len())
        }
        (Some(_), Some(_)) => None,
    }
}

fn path_prefix_matches(lookup_path: &str, binding_path: &str) -> bool {
    if binding_path == "/" || lookup_path == binding_path {
        return true;
    }

    if binding_path.ends_with('/') {
        return lookup_path.starts_with(binding_path);
    }

    lookup_path
        .strip_prefix(binding_path)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::{PathPrefix, RouteDependencyLookup, RouteEntry, RouteHost, RouteHostKind};
    use crate::{
        ids::{Generation, InstanceId, RouteBindingId},
        instance::InstanceState,
    };

    #[test]
    fn route_host_normalization_is_stable() {
        let exact = RouteHost::exact(" App.Example.COM. ").expect("valid host");
        let wildcard =
            RouteHost::wildcard_suffix("*.Customer.Example.COM.").expect("valid wildcard");

        assert_eq!(exact.as_str(), "app.example.com");
        assert_eq!(exact.kind(), RouteHostKind::Exact);
        assert_eq!(wildcard.as_str(), "customer.example.com");
        assert_eq!(wildcard.kind(), RouteHostKind::WildcardSuffix);
        assert!(wildcard.is_wildcard());
    }

    #[test]
    fn route_host_rejects_embedded_wildcards() {
        let error = RouteHost::exact("app.*.example.com").expect_err("wildcard is invalid");

        assert_eq!(
            error.reason(),
            "route host wildcard is only allowed as a leading wildcard suffix"
        );
    }

    #[test]
    fn path_prefix_must_start_with_slash() {
        let error = PathPrefix::new("api").expect_err("relative prefixes are invalid");

        assert_eq!(error.value(), "api");
    }

    #[test]
    fn dependency_lookup_uses_matched_route_binding_id() {
        let entry = RouteEntry {
            route_binding_id: RouteBindingId::new("route-1").expect("valid route ID"),
            instance_id: InstanceId::new("instance-1").expect("valid instance ID"),
            instance_state: InstanceState::Running,
            instance_generation: Generation::new(7),
            backend: None,
            backend_generation: None,
        };

        let lookup = RouteDependencyLookup::from_route_entry(&entry);

        assert_eq!(lookup.route_binding_id(), &entry.route_binding_id);
    }
}
