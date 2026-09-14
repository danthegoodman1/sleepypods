//! Pooled session checkout for certificate work.
//!
//! A dropped query future can leave SQL queued on the session, so a checkout
//! drains its connection before deadpool's Fast recycling may reuse it. This
//! protocol belongs to the Postgres wire session, not to certificate handling.
use std::sync::Arc;

use crate::{
    certificate::work::{CertificateWork, CertificateWorkPermits},
    store::StoreResult,
};

use super::connection::PostgresStore;

/// A checkout may have sent SQL even when its query future is dropped. Never
/// return that session to Fast recycling until its queued rollback and a final
/// protocol round trip have drained. At most one drain task exists per operation
/// slot. Timeout or task/runtime cancellation discards the session instead.
pub(crate) struct CertificateConnection {
    client: Option<deadpool_postgres::Client>,
    operation: Arc<CertificateWorkPermits>,
}
impl std::ops::Deref for CertificateConnection {
    type Target = deadpool_postgres::Client;
    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for CertificateConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}
struct PendingDrain {
    client: Option<deadpool_postgres::Client>,
    _operation: Arc<CertificateWorkPermits>,
}
impl Drop for PendingDrain {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Remove from pool before ClientWrapper aborts its connection task.
            drop(deadpool_postgres::Client::take(client));
        }
    }
}
impl PendingDrain {
    async fn run(mut self) {
        let drained = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.client.as_ref().unwrap().simple_query(""),
        )
        .await;
        if matches!(drained, Ok(Ok(_))) {
            drop(self.client.take()); // Only this successful path recycles.
        }
    }
}
impl Drop for CertificateConnection {
    fn drop(&mut self) {
        let drain = PendingDrain {
            client: self.client.take(),
            _operation: self.operation.clone(),
        };
        // Holding the operation permit bounds queued/running drain tasks as
        // well as SQL. Dropping this task also discards its client safely.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(drain.run());
        } else {
            drop(drain); // Safe discard even outside an entered runtime.
        }
    }
}
impl CertificateWork {
    pub(crate) async fn client(&self, store: &PostgresStore) -> StoreResult<CertificateConnection> {
        Ok(CertificateConnection {
            client: Some(store.client().await?),
            operation: self.permits(),
        })
    }
}
