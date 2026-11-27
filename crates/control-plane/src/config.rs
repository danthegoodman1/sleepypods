use std::time::Duration;

use crate::{
    auth::AuthConfig,
    ids::{EmptyStringError, NonEmptyString},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlPlaneConfig {
    pub store: PostgresStoreConfig,
    pub auth: AuthConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresStoreConfig {
    connection_url: NonEmptyString,
    pub max_connections: usize,
    pub pool_wait_timeout: Duration,
    pub connection_timeout: Duration,
    pub statement_timeout: Duration,
    pub operation_timeout: Duration,
    /// None preserves deduplication for the lifetime of the database. A finite
    /// lifetime is an explicit opt-in and is fixed when each key is created.
    pub idempotency_retention: Option<Duration>,
}

impl ControlPlaneConfig {
    pub fn new(store: PostgresStoreConfig, auth: AuthConfig) -> Self {
        Self { store, auth }
    }
}

impl PostgresStoreConfig {
    pub const MAX_CONNECTIONS: usize = 1024;
    pub const MAX_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
    pub const MAX_IDEMPOTENCY_RETENTION: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

    pub fn new(connection_url: impl Into<String>) -> Result<Self, EmptyStringError> {
        Ok(Self {
            connection_url: NonEmptyString::new("postgres.connection_url", connection_url)?,
            max_connections: 16,
            pool_wait_timeout: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(5),
            statement_timeout: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(600),
            idempotency_retention: None,
        })
    }

    pub fn connection_url(&self) -> &str {
        self.connection_url.as_str()
    }

    /// Returns the invalid setting's environment name. Validate both environment
    /// and programmatic configuration before creating a pool or connecting.
    pub(crate) fn validate_limits(&self) -> Result<(), &'static str> {
        if !(1..=Self::MAX_CONNECTIONS).contains(&self.max_connections) {
            return Err("SLEEPYPODS_POSTGRES_MAX_CONNECTIONS");
        }
        for (name, value, maximum) in [
            (
                "SLEEPYPODS_OPERATION_TIMEOUT_MS",
                self.operation_timeout,
                Self::MAX_TIMEOUT,
            ),
            (
                "SLEEPYPODS_POSTGRES_POOL_WAIT_TIMEOUT_MS",
                self.pool_wait_timeout,
                Self::MAX_TIMEOUT,
            ),
            (
                "SLEEPYPODS_POSTGRES_CONNECTION_TIMEOUT_MS",
                self.connection_timeout,
                Self::MAX_TIMEOUT,
            ),
            (
                "SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS",
                self.statement_timeout,
                Self::MAX_TIMEOUT,
            ),
            (
                "SLEEPYPODS_POSTGRES_IDEMPOTENCY_RETENTION_MS",
                self.idempotency_retention
                    .unwrap_or(Duration::from_millis(1)),
                Self::MAX_IDEMPOTENCY_RETENTION,
            ),
        ] {
            if value < Duration::from_millis(1)
                || value > maximum
                || value.subsec_nanos() % 1_000_000 != 0
            {
                return Err(name);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AuthConfig, PostgresStoreConfig};

    #[test]
    fn validates_postgres_connection_url_presence() {
        let error = PostgresStoreConfig::new("\t").expect_err("connection URL is required");

        assert_eq!(error.field(), "postgres.connection_url");
    }

    #[test]
    fn postgres_capacity_is_bounded_before_pool_allocation() {
        let mut config = PostgresStoreConfig::new("postgres://example").unwrap();
        for capacity in [1, PostgresStoreConfig::MAX_CONNECTIONS] {
            config.max_connections = capacity;
            assert_eq!(config.validate_limits(), Ok(()));
        }
        for capacity in [0, PostgresStoreConfig::MAX_CONNECTIONS + 1, usize::MAX] {
            config.max_connections = capacity;
            assert_eq!(
                config.validate_limits(),
                Err("SLEEPYPODS_POSTGRES_MAX_CONNECTIONS")
            );
        }
    }

    #[test]
    fn postgres_durations_are_bounded_whole_milliseconds() {
        type SetDuration = fn(&mut PostgresStoreConfig, Duration);
        let settings: [(&str, SetDuration, Duration); 4] = [
            (
                "SLEEPYPODS_POSTGRES_POOL_WAIT_TIMEOUT_MS",
                |config, value| config.pool_wait_timeout = value,
                PostgresStoreConfig::MAX_TIMEOUT,
            ),
            (
                "SLEEPYPODS_POSTGRES_CONNECTION_TIMEOUT_MS",
                |config, value| config.connection_timeout = value,
                PostgresStoreConfig::MAX_TIMEOUT,
            ),
            (
                "SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS",
                |config, value| config.statement_timeout = value,
                PostgresStoreConfig::MAX_TIMEOUT,
            ),
            (
                "SLEEPYPODS_POSTGRES_IDEMPOTENCY_RETENTION_MS",
                |config, value| config.idempotency_retention = Some(value),
                PostgresStoreConfig::MAX_IDEMPOTENCY_RETENTION,
            ),
        ];
        for (name, set_duration, maximum) in settings {
            let mut config = PostgresStoreConfig::new("postgres://example").unwrap();
            for valid in [Duration::from_millis(1), maximum] {
                set_duration(&mut config, valid);
                assert_eq!(config.validate_limits(), Ok(()), "{name}={valid:?}");
            }
            for invalid in [
                Duration::ZERO,
                Duration::from_nanos(1),
                Duration::from_micros(999),
                Duration::from_micros(1500),
                maximum + Duration::from_millis(1),
                Duration::from_millis(i64::MAX as u64),
                Duration::MAX,
            ] {
                set_duration(&mut config, invalid);
                assert_eq!(config.validate_limits(), Err(name), "{name}={invalid:?}");
            }
        }
    }

    #[test]
    fn control_plane_config_requires_explicit_auth_config() {
        let store = PostgresStoreConfig::new("postgres://example").expect("valid URL");
        let config = super::ControlPlaneConfig::new(store.clone(), AuthConfig::NoAuth);

        assert_eq!(config.store, store);
        assert_eq!(config.auth, AuthConfig::NoAuth);
    }
}
