//! Canonical request identity: the exact host a request names and, for HTTP,
//! its path. Requests whose host or path exceed what the control plane routes
//! are refused here, before the proxy subscribes to anything.

use std::{borrow::Cow, error::Error, fmt};

use sleepypods_api::{
    PathPrefix, RouteHost, RouteIdentity, MAX_ROUTE_HOST_BYTES, MAX_ROUTE_PATH_BYTES,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteRequestIdentity {
    identity: RouteIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestIdentityError {
    EmptyHost,
    InvalidHostPort,
    UnsupportedHostSyntax,
    HostTooLong,
    PathTooLong,
    InvalidRouteHost(String),
    InvalidPath(String),
}

impl RouteRequestIdentity {
    pub fn http(host: impl AsRef<str>, path: Option<&str>) -> Result<Self, RequestIdentityError> {
        let host = request_route_host(host.as_ref())?;
        let path = canonical_request_path(path)?;

        Ok(Self {
            identity: RouteIdentity::Http {
                host,
                path: Some(
                    PathPrefix::new(path)
                        .map_err(|_| RequestIdentityError::InvalidPath(path.to_owned()))?,
                ),
            },
        })
    }

    pub fn sni(host: impl AsRef<str>) -> Result<Self, RequestIdentityError> {
        Ok(Self {
            identity: RouteIdentity::Sni {
                host: request_route_host(host.as_ref())?,
            },
        })
    }

    #[cfg(test)]
    pub fn identity(&self) -> &RouteIdentity {
        &self.identity
    }

    pub fn into_identity(self) -> RouteIdentity {
        self.identity
    }
}

impl fmt::Display for RequestIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyHost => f.write_str("request host must not be empty"),
            Self::InvalidHostPort => f.write_str("request host port must be numeric"),
            Self::UnsupportedHostSyntax => f.write_str(
                "request host must not contain a colon except for an optional numeric port",
            ),
            Self::HostTooLong => {
                write!(f, "request host exceeds {MAX_ROUTE_HOST_BYTES} bytes")
            }
            Self::PathTooLong => {
                write!(f, "request path exceeds {MAX_ROUTE_PATH_BYTES} bytes")
            }
            Self::InvalidRouteHost(reason) => write!(f, "invalid request host: {reason}"),
            Self::InvalidPath(path) => write!(f, "request path {path:?} must start with /"),
        }
    }
}

impl Error for RequestIdentityError {}

/// The exact host a request names, in the form the route cache is keyed by:
/// trimmed, without port or trailing dots, lower case. It borrows when the
/// input already has that form.
pub(crate) fn canonical_request_host(value: &str) -> Result<Cow<'_, str>, RequestIdentityError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(RequestIdentityError::EmptyHost);
    }
    if trimmed.starts_with('[') {
        return Err(RequestIdentityError::UnsupportedHostSyntax);
    }
    let host = strip_optional_port(trimmed)?.trim().trim_end_matches('.');
    if host.is_empty() {
        return Err(RequestIdentityError::EmptyHost);
    }
    if host.len() > MAX_ROUTE_HOST_BYTES {
        return Err(RequestIdentityError::HostTooLong);
    }
    if host.contains([':', '/']) {
        return Err(RequestIdentityError::UnsupportedHostSyntax);
    }
    if host.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Ok(Cow::Owned(host.to_ascii_lowercase()))
    } else {
        Ok(Cow::Borrowed(host))
    }
}

/// The request path the route cache matches, `/` when the request has none.
pub(crate) fn canonical_request_path(path: Option<&str>) -> Result<&str, RequestIdentityError> {
    let path = path.filter(|value| !value.is_empty()).unwrap_or("/");
    if path.len() > MAX_ROUTE_PATH_BYTES {
        return Err(RequestIdentityError::PathTooLong);
    }
    if !path.starts_with('/') {
        return Err(RequestIdentityError::InvalidPath(path.to_owned()));
    }
    Ok(path)
}

/// Validates a canonical host as a route host. The control plane subscribes
/// the proxy to exactly this string, so a host whose route form differs from
/// its cache key is refused.
pub(crate) fn route_host(canonical: &str) -> Result<RouteHost, RequestIdentityError> {
    let host = RouteHost::exact(canonical)
        .map_err(|error| RequestIdentityError::InvalidRouteHost(error.to_string()))?;
    if host.as_str() != canonical {
        return Err(RequestIdentityError::InvalidRouteHost(
            "host is not in canonical form".to_owned(),
        ));
    }
    Ok(host)
}

fn request_route_host(value: &str) -> Result<RouteHost, RequestIdentityError> {
    route_host(&canonical_request_host(value)?)
}

fn strip_optional_port(host: &str) -> Result<&str, RequestIdentityError> {
    match host.rsplit_once(':') {
        Some((name, port)) if !name.contains(':') => {
            if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(RequestIdentityError::InvalidHostPort);
            }

            Ok(name)
        }
        Some(_) => Err(RequestIdentityError::UnsupportedHostSyntax),
        None => Ok(host),
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use sleepypods_api::{RouteHost, RouteIdentity, MAX_ROUTE_HOST_BYTES, MAX_ROUTE_PATH_BYTES};

    use super::{
        canonical_request_host, canonical_request_path, RequestIdentityError, RouteRequestIdentity,
    };

    #[test]
    fn http_host_normalization_handles_case_trailing_dot_and_port() {
        let identity = RouteRequestIdentity::http(" App.Example.COM.:443 ", Some("/api"))
            .expect("valid identity")
            .into_identity();

        match identity {
            RouteIdentity::Http { host, path } => {
                assert_eq!(host.as_str(), "app.example.com");
                assert_eq!(path.expect("path").as_str(), "/api");
            }
            RouteIdentity::Sni { .. } => panic!("expected HTTP identity"),
        }
    }

    #[test]
    fn sni_uses_same_host_canonicalization() {
        let identity = RouteRequestIdentity::sni(" Db.Example.COM. ")
            .expect("valid identity")
            .into_identity();

        match identity {
            RouteIdentity::Sni { host } => assert_eq!(host.as_str(), "db.example.com"),
            RouteIdentity::Http { .. } => panic!("expected SNI identity"),
        }
    }

    #[test]
    fn host_normalization_rejects_empty_and_invalid_hosts() {
        assert_eq!(
            RouteRequestIdentity::http("  ", None).expect_err("empty host"),
            RequestIdentityError::EmptyHost
        );
        assert_eq!(
            RouteRequestIdentity::http("app.example.com:http", None).expect_err("bad port"),
            RequestIdentityError::InvalidHostPort
        );
        assert!(matches!(
            RouteRequestIdentity::http("localhost", None).expect_err("invalid host"),
            RequestIdentityError::InvalidRouteHost(_)
        ));
    }

    #[test]
    fn http_identity_defaults_missing_or_empty_path_to_root() {
        for path in [None, Some("")] {
            let identity = RouteRequestIdentity::http("app.example.com", path)
                .expect("valid identity")
                .into_identity();

            match identity {
                RouteIdentity::Http { path, .. } => {
                    assert_eq!(path.expect("path").as_str(), "/");
                }
                RouteIdentity::Sni { .. } => panic!("expected HTTP identity"),
            }
        }
    }

    #[test]
    fn a_canonical_host_is_borrowed_and_matches_the_route_host() {
        let canonical = canonical_request_host("app.example.com").expect("valid host");
        assert!(matches!(canonical, Cow::Borrowed("app.example.com")));

        for input in [
            "App.Example.COM",
            " app.example.com. ",
            "app.example.com:8443",
            "APP.example.com..:80",
        ] {
            let canonical = canonical_request_host(input).expect("valid host");
            assert_eq!(
                canonical,
                RouteHost::exact(&*canonical).expect("valid host").as_str(),
                "{input}"
            );
            assert_eq!(canonical, "app.example.com", "{input}");
        }
    }

    #[test]
    fn a_host_at_the_limit_routes_and_one_byte_more_is_refused() {
        let label = "a".repeat(MAX_ROUTE_HOST_BYTES - ".example.com".len());
        let at_limit = format!("{label}.example.com");
        assert_eq!(at_limit.len(), MAX_ROUTE_HOST_BYTES);
        assert!(RouteRequestIdentity::http(&at_limit, None).is_ok());
        assert!(RouteRequestIdentity::sni(&at_limit).is_ok());

        let over = format!("a{at_limit}");
        assert_eq!(
            RouteRequestIdentity::http(&over, None).expect_err("host too long"),
            RequestIdentityError::HostTooLong
        );
        assert_eq!(
            RouteRequestIdentity::sni(format!("{over}:443")).expect_err("host too long"),
            RequestIdentityError::HostTooLong
        );
    }

    #[test]
    fn a_path_at_the_limit_routes_and_one_byte_more_is_refused() {
        let at_limit = format!("/{}", "a".repeat(MAX_ROUTE_PATH_BYTES - 1));
        assert_eq!(
            canonical_request_path(Some(&at_limit)),
            Ok(at_limit.as_str())
        );

        let over = format!("{at_limit}a");
        assert_eq!(
            canonical_request_path(Some(&over)),
            Err(RequestIdentityError::PathTooLong)
        );
        assert_eq!(
            RouteRequestIdentity::http("app.example.com", Some(&over)).expect_err("path too long"),
            RequestIdentityError::PathTooLong
        );
    }
}
