use sleepypods_types::{EmptyStringError, NonEmptyString};
use std::{
    error::Error,
    fmt,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationTarget {
    cluster_id: String,
    namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendEndpoint {
    uri: NonEmptyString,
    address: Option<BackendAddress>,
}

/// The routable address a ready backend was observed at, alongside the URI a
/// client would otherwise resolve. Callers that can route to it directly skip
/// name resolution; callers that cannot keep using [`BackendEndpoint::uri`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BackendAddress(SocketAddr);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidBackendAddress {
    value: String,
    message: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidMaterializationTarget {
    field: &'static str,
}

impl MaterializationTarget {
    pub fn new(
        cluster_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, InvalidMaterializationTarget> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty() {
            return Err(InvalidMaterializationTarget {
                field: "cluster_id",
            });
        }

        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(InvalidMaterializationTarget { field: "namespace" });
        }

        Ok(Self {
            cluster_id,
            namespace,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl BackendEndpoint {
    pub fn new(uri: impl Into<String>) -> Result<Self, EmptyStringError> {
        Ok(Self {
            uri: NonEmptyString::new("backend.uri", uri)?,
            address: None,
        })
    }

    pub fn with_address(
        uri: impl Into<String>,
        address: BackendAddress,
    ) -> Result<Self, EmptyStringError> {
        Ok(Self {
            uri: NonEmptyString::new("backend.uri", uri)?,
            address: Some(address),
        })
    }

    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }

    /// The observed backend address, when the materializer resolved one.
    pub fn address(&self) -> Option<BackendAddress> {
        self.address
    }
}

impl BackendAddress {
    pub fn new(value: SocketAddr) -> Result<Self, InvalidBackendAddress> {
        let invalid = |message| InvalidBackendAddress {
            value: value.to_string(),
            message,
        };
        if value.port() == 0 {
            return Err(invalid("port must not be zero"));
        }
        let ip = value.ip();
        if ip.is_unspecified() {
            return Err(invalid("address must not be unspecified"));
        }
        if ip.is_multicast() {
            return Err(invalid("address must not be multicast"));
        }

        Ok(Self(value))
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.0
    }

    pub fn ip(&self) -> IpAddr {
        self.0.ip()
    }

    pub fn port(&self) -> u16 {
        self.0.port()
    }
}

impl InvalidBackendAddress {
    pub fn field(&self) -> &'static str {
        "backend.address"
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for BackendAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for BackendAddress {
    type Err = InvalidBackendAddress;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = value
            .parse::<SocketAddr>()
            .map_err(|_| InvalidBackendAddress {
                value: value.to_owned(),
                message: "must be an IP address and port, with IPv6 bracketed",
            })?;

        Self::new(parsed)
    }
}

impl fmt::Display for InvalidBackendAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field(), self.message)
    }
}

impl Error for InvalidBackendAddress {}

impl InvalidMaterializationTarget {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for InvalidMaterializationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "materialization target {} must not be empty", self.field)
    }
}

impl Error for InvalidMaterializationTarget {}

#[cfg(test)]
mod tests {
    use super::{BackendAddress, BackendEndpoint, MaterializationTarget};
    use std::net::SocketAddr;

    #[test]
    fn target_requires_cluster_and_namespace() {
        let error = MaterializationTarget::new("cluster-a", "").expect_err("namespace required");

        assert_eq!(error.field(), "namespace");
    }

    #[test]
    fn target_exposes_validated_fields_by_accessor() {
        let target = MaterializationTarget::new("cluster-a", "default").expect("valid target");

        assert_eq!(target.cluster_id(), "cluster-a");
        assert_eq!(target.namespace(), "default");
    }

    #[test]
    fn backend_endpoint_requires_uri() {
        let error = BackendEndpoint::new(" ").expect_err("backend URI required");

        assert_eq!(error.field(), "backend.uri");
    }

    #[test]
    fn backend_endpoint_has_no_address_until_one_is_observed() {
        let backend = BackendEndpoint::new("http://app.default.svc.cluster.local:80")
            .expect("valid backend URI");

        assert_eq!(backend.address(), None);
    }

    #[test]
    fn backend_endpoint_carries_the_observed_address() {
        let address = "10.244.1.7:8080"
            .parse::<BackendAddress>()
            .expect("valid address");
        let backend =
            BackendEndpoint::with_address("http://app.default.svc.cluster.local:80", address)
                .expect("valid backend URI");

        assert_eq!(backend.uri(), "http://app.default.svc.cluster.local:80");
        assert_eq!(backend.address(), Some(address));
    }

    #[test]
    fn backend_endpoints_with_different_addresses_are_not_equal() {
        let uri = "http://app.default.svc.cluster.local:80";
        let first = BackendEndpoint::with_address(uri, "10.244.1.7:8080".parse().expect("valid"))
            .expect("valid backend URI");
        let second = BackendEndpoint::with_address(uri, "10.244.2.9:8080".parse().expect("valid"))
            .expect("valid backend URI");

        assert_ne!(first, second);
        assert_ne!(first, BackendEndpoint::new(uri).expect("valid backend URI"));
    }

    #[test]
    fn backend_address_accepts_ipv4_and_bracketed_ipv6() {
        let v4 = "10.244.1.7:8080"
            .parse::<BackendAddress>()
            .expect("valid IPv4 address");

        assert_eq!(v4.port(), 8080);
        assert_eq!(v4.to_string(), "10.244.1.7:8080");

        let v6 = "[fd00::7]:8080"
            .parse::<BackendAddress>()
            .expect("valid IPv6 address");

        assert_eq!(v6.port(), 8080);
        assert_eq!(v6.to_string(), "[fd00::7]:8080");
        assert_eq!(
            v6.socket_addr(),
            "[fd00::7]:8080".parse::<SocketAddr>().expect("valid")
        );
    }

    #[test]
    fn backend_address_rejects_addresses_that_cannot_be_dialed() {
        for value in [
            "10.244.1.7:0",
            "0.0.0.0:8080",
            "[::]:8080",
            "224.0.0.1:8080",
            "[ff02::1]:8080",
        ] {
            let error = value
                .parse::<BackendAddress>()
                .expect_err("address is not dialable");

            assert_eq!(error.field(), "backend.address");
            assert_eq!(error.value(), value);
        }
    }

    #[test]
    fn backend_address_rejects_text_that_is_not_an_address_and_port() {
        for value in [
            "",
            "10.244.1.7",
            "fd00::7:8080",
            "app.default.svc.cluster.local:80",
        ] {
            let error = value
                .parse::<BackendAddress>()
                .expect_err("value is not an address and port");

            assert_eq!(error.field(), "backend.address");
            assert_eq!(error.value(), value);
        }
    }
}
