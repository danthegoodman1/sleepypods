use std::str::FromStr;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::{Config as PgConfig, NoTls};

use crate::{
    config::PostgresStoreConfig,
    store::{StoreError, StoreResult},
};

use super::migrations;

#[derive(Clone, Debug)]
pub struct PostgresStore {
    pub(crate) pool: Pool,
    pub(crate) certificate_work: crate::certificate::work::CertificateWorkLimits,
    pub(crate) certificate_sealer: Option<std::sync::Arc<crate::certificate::CertificateSealer>>,
    pub(crate) route_changes: super::runtime_work::RouteChangeListener,
}

impl PostgresStore {
    pub async fn connect(config: &PostgresStoreConfig) -> StoreResult<Self> {
        config.validate_limits().map_err(|setting| {
            StoreError::invalid_argument(format!("{setting} is outside its supported limits"))
        })?;
        let statement_timeout = config.statement_timeout.as_millis();
        let operation_timeout = config.operation_timeout.as_millis();
        let mut pg_config = parse_connection_url(config.connection_url())?;
        pg_config.connect_timeout(config.connection_timeout);
        let options = format!(
            "{} -c statement_timeout={statement_timeout} -c sleepypods.operation_timeout_ms={operation_timeout}",
            pg_config.get_options().unwrap_or("")
        );
        pg_config.options(&options);
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let route_changes = super::runtime_work::RouteChangeListener::new(pg_config.clone());
        let manager = Manager::from_config(pg_config, NoTls, manager_config);
        let pool = Pool::builder(manager)
            .max_size(config.max_connections)
            .wait_timeout(Some(config.pool_wait_timeout))
            .create_timeout(Some(config.connection_timeout))
            .recycle_timeout(Some(config.connection_timeout))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|error| {
                StoreError::internal(format!("failed to build Postgres pool: {error}"))
            })?;
        let store = Self {
            pool,
            certificate_work: crate::certificate::work::CertificateWorkLimits::new(
                config.max_connections,
            ),
            certificate_sealer: None,
            route_changes,
        };

        store.run_migrations().await?;

        Ok(store)
    }

    /// Supplies deployment-owned sealing keys without storing them in PostgreSQL.
    /// Certificate material operations fail closed until this is configured.
    pub fn with_certificate_sealer(
        mut self,
        sealer: std::sync::Arc<crate::certificate::CertificateSealer>,
    ) -> Self {
        self.certificate_sealer = Some(sealer);
        self
    }

    /// Sealing key IDs that active certificates use and the configured key ring
    /// lacks. Those certificates stay unreadable until the keys return, so the
    /// control plane refuses to start while any are listed.
    pub async fn missing_sealing_key_ids(&self) -> StoreResult<Vec<String>> {
        super::certificate_ops::missing_sealing_key_ids(self).await
    }

    pub async fn run_migrations(&self) -> StoreResult<()> {
        let mut client = self.client().await?;

        migrations::run(&mut client).await
    }

    pub(crate) async fn client(&self) -> StoreResult<deadpool_postgres::Client> {
        self.pool.get().await.map_err(|error| {
            StoreError::unavailable(format!("failed to get Postgres connection: {error}"))
        })
    }
}

fn parse_connection_url(value: &str) -> StoreResult<PgConfig> {
    PgConfig::from_str(value).map_err(|error| {
        StoreError::invalid_argument(format!(
            "postgres.connection_url is not a valid Postgres connection URL: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::parse_connection_url;
    use crate::{config::PostgresStoreConfig, postgres::PostgresStore, store::StoreError};

    #[test]
    fn parse_connection_url_rejects_invalid_urls() {
        let error = parse_connection_url("http://example.com/not-postgres")
            .expect_err("non-Postgres URL is invalid");

        assert!(matches!(error, StoreError::InvalidArgument { .. }));
    }

    #[test]
    fn postgres_store_config_rejects_missing_url_before_provider_construction() {
        let error = PostgresStoreConfig::new(" ").expect_err("missing URL is invalid");

        assert_eq!(error.field(), "postgres.connection_url");
    }

    #[test]
    fn postgres_store_is_control_plane_store() {
        fn assert_store<T: crate::store::ControlPlaneStore>() {}

        assert_store::<PostgresStore>();
    }

    #[tokio::test]
    async fn connect_rejects_invalid_limits_before_parsing_or_allocating() {
        let mut config = PostgresStoreConfig::new("not a Postgres URL").unwrap();
        config.max_connections = usize::MAX;
        let error = PostgresStore::connect(&config).await.unwrap_err();
        assert!(matches!(error, StoreError::InvalidArgument { message }
            if message.contains("SLEEPYPODS_POSTGRES_MAX_CONNECTIONS")));

        config.max_connections = 1;
        config.statement_timeout = std::time::Duration::from_micros(999);
        let error = PostgresStore::connect(&config).await.unwrap_err();
        assert!(matches!(error, StoreError::InvalidArgument { message }
            if message.contains("SLEEPYPODS_POSTGRES_STATEMENT_TIMEOUT_MS")));
    }
}
