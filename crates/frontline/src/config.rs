use std::{
    collections::HashMap,
    env,
    error::Error,
    fmt,
    net::{AddrParseError, SocketAddr},
    num::ParseIntError,
    time::Duration,
};

use sleepypods_api::{BearerToken, InvalidBearerToken};

use crate::{
    FrontlineHttpListenerConfig, FrontlineListenersConfig, FrontlineTlsPassthroughListenerConfig,
    FrontlineTlsTerminationListenerConfig,
};

const DEFAULT_ROUTE_CACHE_CAPACITY: usize = 1024;
const DEFAULT_DRAIN_GRACE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_WAKE_INSTANCE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_ROUTE_TIMEOUT_MS: u64 = 130_000;
const ROUTE_TIMEOUT_MS: &str = "SLEEPYPODS_FRONTLINE_ROUTE_TIMEOUT_MS";
const FRONTLINE_LISTEN_ADDR: &str = "SLEEPYPODS_FRONTLINE_LISTEN_ADDR";
const CONTROL_PLANE_ENDPOINT: &str = "SLEEPYPODS_CONTROL_PLANE_ENDPOINT";
const CONTROL_PLANE_PROXY_TOKEN: &str = "SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN";
const ROUTE_CACHE_CAPACITY: &str = "SLEEPYPODS_ROUTE_CACHE_CAPACITY";
const DRAIN_GRACE_TIMEOUT_MS: &str = "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS";
const WAKE_INSTANCE_TIMEOUT_MS: &str = "SLEEPYPODS_FRONTLINE_WAKE_INSTANCE_TIMEOUT_MS";
const TLS_TERMINATION_LISTEN_ADDR: &str = "SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR";
const TLS_PASSTHROUGH_LISTEN_ADDR: &str = "SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR";
const METRICS_LISTEN_ADDR: &str = "SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontlineEnvConfig {
    listeners: FrontlineListenersConfig,
    control_plane_endpoint: String,
    control_plane_proxy_token: Option<BearerToken>,
    control_plane_ca_pem: Option<String>,
    route_cache_capacity: usize,
    drain_grace_timeout: Duration,
    wake_instance_timeout: Duration,
    route_timeout: Duration,
    metrics_listen_addr: Option<SocketAddr>,
}

#[derive(Debug)]
pub enum FrontlineEnvConfigError {
    InitialActivationBudgetExceeded,
    InvalidProxyResources(proxy_core::InvalidProxyResourceConfig),
    Missing {
        name: &'static str,
    },
    InvalidSocketAddr {
        name: &'static str,
        value: String,
        source: AddrParseError,
    },
    InvalidUsize {
        name: &'static str,
        value: String,
        source: ParseIntError,
    },
    InvalidDurationMs {
        name: &'static str,
        value: String,
        source: ParseIntError,
    },
    InvalidCertificateDelivery,
    InvalidControlPlaneBearerToken {
        name: &'static str,
        source: InvalidBearerToken,
    },
}

impl FrontlineEnvConfig {
    pub fn from_env() -> Result<Self, FrontlineEnvConfigError> {
        Self::from_vars(env::vars())
    }

    pub fn from_vars<I, K, V>(vars: I) -> Result<Self, FrontlineEnvConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let vars: HashMap<String, String> = vars
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();

        let listen_addr = required(&vars, FRONTLINE_LISTEN_ADDR)?;
        let listen_addr = listen_addr.parse::<SocketAddr>().map_err(|source| {
            FrontlineEnvConfigError::InvalidSocketAddr {
                name: FRONTLINE_LISTEN_ADDR,
                value: listen_addr.clone(),
                source,
            }
        })?;
        let control_plane_endpoint = required(&vars, CONTROL_PLANE_ENDPOINT)?;
        let control_plane_proxy_token = optional_bearer_token(&vars, CONTROL_PLANE_PROXY_TOKEN)?;
        let control_plane_ca_pem = vars
            .get(sleepypods_api::transport::CONTROL_PLANE_TLS_CA_PEM_ENV)
            .cloned();
        let route_cache_capacity =
            optional_usize(&vars, ROUTE_CACHE_CAPACITY, DEFAULT_ROUTE_CACHE_CAPACITY)?;
        let drain_grace_timeout = optional_duration_ms(
            &vars,
            DRAIN_GRACE_TIMEOUT_MS,
            DEFAULT_DRAIN_GRACE_TIMEOUT_MS,
        )?;
        let wake_instance_timeout = optional_duration_ms(
            &vars,
            WAKE_INSTANCE_TIMEOUT_MS,
            DEFAULT_WAKE_INSTANCE_TIMEOUT_MS,
        )?;
        let route_timeout =
            optional_duration_ms(&vars, ROUTE_TIMEOUT_MS, DEFAULT_ROUTE_TIMEOUT_MS)?;
        let tls_termination =
            optional_tls_termination_listener_config(&vars, TLS_TERMINATION_LISTEN_ADDR)?;
        let tls_passthrough = optional_listener_config(&vars, TLS_PASSTHROUGH_LISTEN_ADDR)?
            .map(|listener| FrontlineTlsPassthroughListenerConfig::new(listener.listen_addr()));
        let metrics_listen_addr = optional_listener_config(&vars, METRICS_LISTEN_ADDR)?
            .map(|listener| listener.listen_addr());
        if tls_termination.is_some()
            && (!control_plane_endpoint.starts_with("https://")
                || control_plane_proxy_token.is_none()
                || sleepypods_api::transport::native_endpoint(
                    control_plane_endpoint.clone(),
                    control_plane_ca_pem.as_deref(),
                )
                .is_err())
        {
            return Err(FrontlineEnvConfigError::InvalidCertificateDelivery);
        }
        let listeners = FrontlineListenersConfig::new(
            FrontlineHttpListenerConfig::new(listen_addr).with_resource_config(
                proxy_core::ProxyResourceConfig::from_vars(|name| vars.get(name).cloned())
                    .map_err(FrontlineEnvConfigError::InvalidProxyResources)?,
            ),
        )
        .with_tls_termination(tls_termination)
        .with_tls_passthrough(tls_passthrough);

        let resources = listeners.http().resource_config();
        if !initial_activation_budget_valid(route_timeout, resources) {
            return Err(FrontlineEnvConfigError::InitialActivationBudgetExceeded);
        }

        Ok(Self {
            listeners,
            control_plane_endpoint,
            control_plane_proxy_token,
            control_plane_ca_pem,
            route_cache_capacity,
            drain_grace_timeout,
            wake_instance_timeout,
            route_timeout,
            metrics_listen_addr,
        })
    }

    #[cfg(test)]
    pub fn listener(&self) -> FrontlineHttpListenerConfig {
        self.listeners.http()
    }

    pub fn listeners(&self) -> FrontlineListenersConfig {
        self.listeners
    }

    #[cfg(test)]
    pub fn tls_termination_listener(&self) -> Option<FrontlineTlsTerminationListenerConfig> {
        self.listeners.tls_termination()
    }

    #[cfg(test)]
    pub fn tls_passthrough_listener(&self) -> Option<FrontlineTlsPassthroughListenerConfig> {
        self.listeners.tls_passthrough()
    }

    pub fn control_plane_endpoint(&self) -> &str {
        &self.control_plane_endpoint
    }

    pub fn control_plane_proxy_token(&self) -> Option<&BearerToken> {
        self.control_plane_proxy_token.as_ref()
    }

    pub fn control_plane_ca_pem(&self) -> Option<&str> {
        self.control_plane_ca_pem.as_deref()
    }

    pub fn route_cache_capacity(&self) -> usize {
        self.route_cache_capacity
    }

    pub fn drain_grace_timeout(&self) -> Duration {
        self.drain_grace_timeout
    }

    pub fn route_timeout(&self) -> Duration {
        self.route_timeout
    }

    pub fn wake_instance_timeout(&self) -> Duration {
        self.wake_instance_timeout
    }

    pub fn metrics_listen_addr(&self) -> Option<SocketAddr> {
        self.metrics_listen_addr
    }
}

fn required(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<String, FrontlineEnvConfigError> {
    vars.get(name)
        .cloned()
        .ok_or(FrontlineEnvConfigError::Missing { name })
}

fn optional_usize(
    vars: &HashMap<String, String>,
    name: &'static str,
    default: usize,
) -> Result<usize, FrontlineEnvConfigError> {
    match vars.get(name) {
        Some(value) => value
            .parse()
            .map_err(|source| FrontlineEnvConfigError::InvalidUsize {
                name,
                value: value.clone(),
                source,
            }),
        None => Ok(default),
    }
}

fn optional_listener_config(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<FrontlineHttpListenerConfig>, FrontlineEnvConfigError> {
    let Some(value) = vars.get(name) else {
        return Ok(None);
    };
    let listen_addr = value.parse::<SocketAddr>().map_err(|source| {
        FrontlineEnvConfigError::InvalidSocketAddr {
            name,
            value: value.clone(),
            source,
        }
    })?;

    Ok(Some(FrontlineHttpListenerConfig::new(listen_addr)))
}

fn optional_tls_termination_listener_config(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<FrontlineTlsTerminationListenerConfig>, FrontlineEnvConfigError> {
    optional_listener_config(vars, name).map(|listener| {
        listener.map(|listener| FrontlineTlsTerminationListenerConfig::new(listener.listen_addr()))
    })
}

fn optional_duration_ms(
    vars: &HashMap<String, String>,
    name: &'static str,
    default_ms: u64,
) -> Result<Duration, FrontlineEnvConfigError> {
    let value = match vars.get(name) {
        Some(value) => {
            value
                .parse()
                .map_err(|source| FrontlineEnvConfigError::InvalidDurationMs {
                    name,
                    value: value.clone(),
                    source,
                })?
        }
        None => default_ms,
    };

    Ok(Duration::from_millis(value))
}

fn optional_bearer_token(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<BearerToken>, FrontlineEnvConfigError> {
    let Some(value) = vars.get(name) else {
        return Ok(None);
    };
    BearerToken::new(name, value.clone())
        .map(Some)
        .map_err(|source| FrontlineEnvConfigError::InvalidControlPlaneBearerToken { name, source })
}

impl fmt::Display for FrontlineEnvConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitialActivationBudgetExceeded => f.write_str(
                "frontline route timeout + max(proxy setup timeout, upstream header idle timeout) must not exceed the 190000 ms initial activation budget"
            ),
            Self::InvalidProxyResources(error) => write!(f, "{error}"),
            Self::Missing { name } => write!(f, "{name} is required"),
            Self::InvalidSocketAddr { name, value, .. } => {
                write!(f, "{name} must be a socket address, got {value:?}")
            }
            Self::InvalidUsize { name, value, .. } => {
                write!(f, "{name} must be an unsigned integer, got {value:?}")
            }
            Self::InvalidDurationMs { name, value, .. } => {
                write!(
                    f,
                    "{name} must be a duration in milliseconds, got {value:?}"
                )
            }
            Self::InvalidCertificateDelivery => f.write_str("TLS termination requires verified HTTPS control-plane delivery and a proxy credential"),
            Self::InvalidControlPlaneBearerToken { name, source } => {
                write!(f, "{name} is not a valid bearer token: {source}")
            }
        }
    }
}

impl Error for FrontlineEnvConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidProxyResources(error) => Some(error),
            Self::InvalidSocketAddr { source, .. } => Some(source),
            Self::InvalidUsize { source, .. } | Self::InvalidDurationMs { source, .. } => {
                Some(source)
            }
            Self::InvalidControlPlaneBearerToken { source, .. } => Some(source),
            Self::InitialActivationBudgetExceeded
            | Self::Missing { .. }
            | Self::InvalidCertificateDelivery => None,
        }
    }
}

/// Validated by both environment parsing and every production listener entry.
pub(crate) fn initial_activation_budget_valid(
    route: Duration,
    resources: proxy_core::ProxyResourceConfig,
) -> bool {
    route
        .checked_add(
            resources
                .setup_timeout()
                .max(resources.upstream_header_idle_timeout()),
        )
        .is_some_and(|total| total <= sleepypods_api::INITIAL_ACTIVATION_TIMEOUT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_activation_budget_checks_route_and_both_setup_bounds() {
        let parse = |route: &str, setup: &str, header: &str| {
            FrontlineEnvConfig::from_vars([
                (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
                (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
                (ROUTE_TIMEOUT_MS, route),
                ("SLEEPYPODS_PROXY_SETUP_TIMEOUT_MS", setup),
                ("SLEEPYPODS_PROXY_UPSTREAM_HEADER_IDLE_TIMEOUT_MS", header),
            ])
        };
        assert!(parse("130000", "10000", "60000").is_ok());
        assert!(parse("100000", "90000", "50000").is_ok());
        for (route, setup, header) in [
            ("130001", "10000", "60000"),
            ("100000", "90001", "50000"),
            ("100000", "10000", "90001"),
            ("18446744073709551615", "10000", "60000"),
        ] {
            let error = parse(route, setup, header).unwrap_err();
            assert!(
                matches!(
                    error,
                    FrontlineEnvConfigError::InitialActivationBudgetExceeded
                ),
                "{error}"
            );
            assert!(error.to_string().contains("190000 ms"));
        }
    }

    #[test]
    fn required_values_parse_with_defaults() {
        let config = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
        ])
        .expect("config parses");

        assert_eq!(
            config.listener().listen_addr(),
            "127.0.0.1:8080".parse().unwrap()
        );
        assert_eq!(config.control_plane_endpoint(), "http://127.0.0.1:50051");
        assert!(config.control_plane_proxy_token().is_none());
        assert!(config.control_plane_ca_pem().is_none());
        assert_eq!(config.route_cache_capacity(), DEFAULT_ROUTE_CACHE_CAPACITY);
        assert_eq!(
            config.drain_grace_timeout(),
            Duration::from_millis(DEFAULT_DRAIN_GRACE_TIMEOUT_MS)
        );
        assert_eq!(
            config.wake_instance_timeout(),
            Duration::from_millis(DEFAULT_WAKE_INSTANCE_TIMEOUT_MS)
        );
        assert!(config.tls_termination_listener().is_none());
        assert!(config.tls_passthrough_listener().is_none());
        assert_eq!(config.metrics_listen_addr(), None);
    }

    #[test]
    fn optional_values_override_defaults() {
        let config = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (CONTROL_PLANE_PROXY_TOKEN, "proxy-secret"),
            (ROUTE_CACHE_CAPACITY, "17"),
            (DRAIN_GRACE_TIMEOUT_MS, "250"),
            (WAKE_INSTANCE_TIMEOUT_MS, "750"),
            (METRICS_LISTEN_ADDR, "127.0.0.1:19091"),
        ])
        .expect("config parses");

        assert_eq!(
            config
                .control_plane_proxy_token()
                .expect("proxy token")
                .authorization_header_value()
                .expect("header value")
                .to_str()
                .expect("ascii"),
            "Bearer proxy-secret"
        );
        assert_eq!(config.route_cache_capacity(), 17);
        assert_eq!(config.drain_grace_timeout(), Duration::from_millis(250));
        assert_eq!(config.wake_instance_timeout(), Duration::from_millis(750));
        assert_eq!(
            config.metrics_listen_addr(),
            Some("127.0.0.1:19091".parse().expect("socket address"))
        );
    }

    #[test]
    fn invalid_control_plane_token_is_reported_without_leaking_value() {
        let error = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (CONTROL_PLANE_PROXY_TOKEN, "has space"),
        ])
        .expect_err("token with whitespace is invalid");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::InvalidControlPlaneBearerToken {
                name: CONTROL_PLANE_PROXY_TOKEN,
                ..
            }
        ));
        assert!(!error.to_string().contains("has space"));
    }

    #[test]
    fn missing_required_value_is_reported() {
        let error =
            FrontlineEnvConfig::from_vars([(CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051")])
                .expect_err("listen addr is required");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::Missing {
                name: FRONTLINE_LISTEN_ADDR
            }
        ));
    }

    #[test]
    fn invalid_optional_number_is_reported() {
        let error = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (ROUTE_CACHE_CAPACITY, "nope"),
        ])
        .expect_err("route cache capacity is invalid");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::InvalidUsize {
                name: ROUTE_CACHE_CAPACITY,
                ..
            }
        ));
    }

    #[test]
    fn empty_dynamic_cache_configuration_requires_verified_delivery() {
        let parse = |endpoint, token| {
            FrontlineEnvConfig::from_vars([
                (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
                (CONTROL_PLANE_ENDPOINT, endpoint),
                (CONTROL_PLANE_PROXY_TOKEN, token),
                (TLS_TERMINATION_LISTEN_ADDR, "127.0.0.1:8443"),
            ])
        };
        let config = parse("https://control-plane.example:50051", "proxy-secret").unwrap();
        assert!(config.tls_termination_listener().is_some());
        assert!(parse("http://control-plane.example:50051", "proxy-secret").is_err());
        assert!(parse("https://control-plane.example:50051", "").is_err());
        assert!(parse("https://", "proxy-secret").is_err());
    }

    #[test]
    fn invalid_passthrough_address_is_reported() {
        let error = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (TLS_PASSTHROUGH_LISTEN_ADDR, "not-an-addr"),
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            FrontlineEnvConfigError::InvalidSocketAddr {
                name: TLS_PASSTHROUGH_LISTEN_ADDR,
                ..
            }
        ));
    }
}
