use std::{error::Error, fmt, time::Duration};

use crate::{
    ids::{BackendGeneration, Generation, IdempotencyKey, InstanceId, RouteBindingId},
    instance::InstanceState,
    materialization::{BackendEndpoint, MaterializationTarget},
};

/// The longest host a proxy subscribes to.
pub const MAX_ROUTE_HOST_BYTES: usize = 253;
/// The longest request path a proxy routes.
pub const MAX_ROUTE_PATH_BYTES: usize = 4096;
/// The most bindings one host's rule set may hold.
pub const MAX_HOST_RULES: usize = 256;

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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteBindingRecord {
    pub id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRouteBindingRequest {
    pub idempotency_key: IdempotencyKey,
    pub route_binding_id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
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

/// The listener family an identity routes: HTTP by Host and path, or TLS
/// passthrough by SNI.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RouteKind {
    Http,
    Sni,
}

/// Asks for every binding that can serve one exact host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveHostRulesRequest {
    pub kind: RouteKind,
    pub host: RouteHost,
    pub target: MaterializationTarget,
}

impl ResolveHostRulesRequest {
    pub fn new(kind: RouteKind, host: RouteHost, target: MaterializationTarget) -> Self {
        Self { kind, host, target }
    }
}

/// One binding that applies to a host, with the state of its instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteRule {
    pub identity: RouteIdentity,
    pub entry: RouteEntry,
}

/// The complete rule set for one exact host, most specific rule first: an
/// exact host before a wildcard suffix, a longer suffix before a shorter one,
/// then a longer path prefix before a shorter one. Because the set is
/// complete, the first rule whose path prefix covers a request path is the
/// route for that path, and no binding outside the set can apply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostRules {
    rules: Vec<RouteRule>,
}

impl HostRules {
    /// Orders `rules` by specificity. Every rule must apply to the same host.
    pub fn ranked(mut rules: Vec<RouteRule>) -> Self {
        rules.sort_by(|left, right| {
            rule_rank(&right.identity)
                .cmp(&rule_rank(&left.identity))
                .then_with(|| {
                    left.entry
                        .route_binding_id
                        .cmp(&right.entry.route_binding_id)
                })
        });
        Self { rules }
    }

    /// The route for `path` on this host. SNI rules carry no path and match
    /// every request.
    pub fn select(&self, path: &str) -> Option<&RouteRule> {
        self.rules.iter().find(|rule| {
            rule.identity
                .path_prefix()
                .is_none_or(|prefix| prefix.covers(path))
        })
    }

    pub fn rules(&self) -> &[RouteRule] {
        &self.rules
    }

    pub fn into_rules(self) -> Vec<RouteRule> {
        self.rules
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }
}

/// Exact host first, then the longer host, then the longer path prefix.
fn rule_rank(identity: &RouteIdentity) -> (bool, usize, usize) {
    (
        !identity.host().is_wildcard(),
        identity.host().as_str().len(),
        identity
            .path_prefix()
            .map_or(0, |prefix| prefix.as_str().len()),
    )
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
    pub fn new(identity: RouteIdentity) -> Self {
        Self { identity }
    }
}

impl CreateRouteBindingRequest {
    pub fn new(
        idempotency_key: IdempotencyKey,
        route_binding_id: RouteBindingId,
        instance_id: InstanceId,
        identity: RouteIdentity,
    ) -> Self {
        Self {
            idempotency_key,
            route_binding_id,
            instance_id,
            identity,
        }
    }

    pub fn spec(&self) -> RouteBindingSpec {
        RouteBindingSpec::new(self.identity.clone())
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

impl RouteIdentity {
    pub fn kind(&self) -> RouteKind {
        match self {
            Self::Http { .. } => RouteKind::Http,
            Self::Sni { .. } => RouteKind::Sni,
        }
    }

    pub fn host(&self) -> &RouteHost {
        match self {
            Self::Http { host, .. } | Self::Sni { host } => host,
        }
    }

    pub fn path_prefix(&self) -> Option<&PathPrefix> {
        match self {
            Self::Http { path, .. } => path.as_ref(),
            Self::Sni { .. } => None,
        }
    }

    /// Whether a binding with this identity can serve requests of `kind` for
    /// the exact host `host`.
    pub fn applies_to(&self, kind: RouteKind, host: &str) -> bool {
        self.kind() == kind && self.host().covers(host)
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

    /// Whether this host matches the exact, canonical `host`: an exact host
    /// matches itself, and a wildcard suffix matches every name under it.
    pub fn covers(&self, host: &str) -> bool {
        match self.kind {
            RouteHostKind::Exact => self.host == host,
            RouteHostKind::WildcardSuffix => host
                .strip_suffix(self.host.as_str())
                .is_some_and(|prefix| prefix.ends_with('.')),
        }
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

    /// Whether `path` falls under this prefix. A prefix matches itself and
    /// whole path segments below it; `/` matches every path.
    pub fn covers(&self, path: &str) -> bool {
        let prefix = self.0.as_str();
        if prefix == "/" || path == prefix {
            return true;
        }
        if prefix.ends_with('/') {
            return path.starts_with(prefix);
        }
        path.strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
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

#[cfg(test)]
mod tests {
    use super::{
        HostRules, PathPrefix, RouteDependencyLookup, RouteEntry, RouteHost, RouteHostKind,
        RouteIdentity, RouteKind, RouteRule,
    };
    use crate::{
        ids::{Generation, InstanceId, RouteBindingId},
        instance::InstanceState,
    };

    fn entry(id: &str) -> RouteEntry {
        RouteEntry {
            route_binding_id: RouteBindingId::new(id).expect("valid route ID"),
            instance_id: InstanceId::new(format!("instance-{id}")).expect("valid instance ID"),
            instance_state: InstanceState::Running,
            instance_generation: Generation::new(7),
            backend: None,
            backend_generation: None,
        }
    }

    fn http(host: RouteHost, path: Option<&str>, id: &str) -> RouteRule {
        RouteRule {
            identity: RouteIdentity::Http {
                host,
                path: path.map(|path| PathPrefix::new(path).expect("valid prefix")),
            },
            entry: entry(id),
        }
    }

    fn exact(host: &str) -> RouteHost {
        RouteHost::exact(host).expect("valid host")
    }

    fn wildcard(host: &str) -> RouteHost {
        RouteHost::wildcard_suffix(host).expect("valid host")
    }

    fn selected<'a>(rules: &'a HostRules, path: &str) -> Option<&'a str> {
        rules
            .select(path)
            .map(|rule| rule.entry.route_binding_id.as_str())
    }

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
    fn wildcard_suffix_covers_names_under_it_only() {
        let suffix = wildcard("example.com");

        assert!(suffix.covers("app.example.com"));
        assert!(suffix.covers("a.b.example.com"));
        assert!(!suffix.covers("example.com"));
        assert!(!suffix.covers("badexample.com"));
        assert!(exact("app.example.com").covers("app.example.com"));
        assert!(!exact("app.example.com").covers("other.example.com"));
    }

    #[test]
    fn path_prefix_covers_whole_segments() {
        let api = PathPrefix::new("/api").expect("valid prefix");
        let api_slash = PathPrefix::new("/api/").expect("valid prefix");
        let root = PathPrefix::new("/").expect("valid prefix");

        assert!(root.covers("/any/path"));
        assert!(api.covers("/api"));
        assert!(api.covers("/api/users"));
        assert!(!api.covers("/apiary"));
        assert!(api_slash.covers("/api/users"));
        assert!(api_slash.covers("/api/"));
        assert!(!api_slash.covers("/api"));
    }

    #[test]
    fn identity_applies_to_its_own_kind_and_covered_hosts() {
        let rule = http(wildcard("example.com"), None, "wild");

        assert!(rule.identity.applies_to(RouteKind::Http, "app.example.com"));
        assert!(!rule.identity.applies_to(RouteKind::Sni, "app.example.com"));
        assert!(!rule.identity.applies_to(RouteKind::Http, "example.com"));
    }

    #[test]
    fn exact_host_outranks_a_wildcard_with_a_longer_path() {
        let rules = HostRules::ranked(vec![
            http(wildcard("example.com"), Some("/api/v1"), "wildcard"),
            http(exact("app.example.com"), None, "exact"),
        ]);

        assert_eq!(selected(&rules, "/api/v1/users"), Some("exact"));
    }

    #[test]
    fn longer_wildcard_suffix_outranks_a_shorter_one() {
        let rules = HostRules::ranked(vec![
            http(wildcard("example.com"), None, "broad"),
            http(wildcard("customer.example.com"), None, "specific"),
        ]);

        assert_eq!(selected(&rules, "/"), Some("specific"));
    }

    #[test]
    fn longest_covering_path_prefix_wins_within_a_host() {
        let rules = HostRules::ranked(vec![
            http(exact("app.example.com"), Some("/"), "root"),
            http(exact("app.example.com"), Some("/api/v1"), "v1"),
            http(exact("app.example.com"), Some("/api"), "api"),
        ]);

        assert_eq!(selected(&rules, "/api/v1/users"), Some("v1"));
        assert_eq!(selected(&rules, "/api/v2"), Some("api"));
        assert_eq!(selected(&rules, "/apiary"), Some("root"));
    }

    #[test]
    fn a_path_no_exact_rule_covers_falls_through_to_the_wildcard() {
        let rules = HostRules::ranked(vec![
            http(wildcard("example.com"), None, "wildcard"),
            http(exact("app.example.com"), Some("/api"), "api"),
        ]);

        assert_eq!(selected(&rules, "/api/users"), Some("api"));
        assert_eq!(selected(&rules, "/web"), Some("wildcard"));
    }

    #[test]
    fn a_root_prefix_outranks_an_unset_prefix() {
        let rules = HostRules::ranked(vec![
            http(exact("app.example.com"), None, "unset"),
            http(exact("app.example.com"), Some("/"), "root"),
        ]);

        assert_eq!(selected(&rules, "/anything"), Some("root"));
    }

    #[test]
    fn no_covering_rule_selects_nothing() {
        let rules = HostRules::ranked(vec![http(exact("app.example.com"), Some("/api"), "api")]);

        assert_eq!(selected(&rules, "/web"), None);
        assert_eq!(selected(&HostRules::default(), "/"), None);
    }

    #[test]
    fn sni_rules_match_without_a_path() {
        let rules = HostRules::ranked(vec![
            RouteRule {
                identity: RouteIdentity::Sni {
                    host: wildcard("example.com"),
                },
                entry: entry("wildcard"),
            },
            RouteRule {
                identity: RouteIdentity::Sni {
                    host: exact("db.example.com"),
                },
                entry: entry("exact"),
            },
        ]);

        assert_eq!(selected(&rules, ""), Some("exact"));
    }

    #[test]
    fn dependency_lookup_uses_matched_route_binding_id() {
        let entry = entry("route-1");

        let lookup = RouteDependencyLookup::from_route_entry(&entry);

        assert_eq!(lookup.route_binding_id(), &entry.route_binding_id);
    }
}
