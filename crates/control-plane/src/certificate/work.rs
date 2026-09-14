//! Separate certificate work capacity, leaving at least one shared database slot
//! for route/lifecycle work. A detached blocking task retains both its operation
//! and crypto permits until it actually finishes. Holding a permit across a
//! backend session is the backend's own concern; see `postgres::certificate_connection`.
use crate::store::{StoreError, StoreResult};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub(crate) const WATCH_STREAMS: usize = 16;
#[derive(Clone, Debug)]
pub(crate) struct CertificateWorkLimits {
    operations: Arc<Semaphore>,
    crypto: Arc<Semaphore>,
    watch_reads: Arc<Semaphore>,
    watch_admission: Arc<Semaphore>,
}
impl CertificateWorkLimits {
    pub fn new(database_capacity: usize) -> Self {
        Self {
            operations: Arc::new(Semaphore::new(database_capacity.saturating_sub(1).min(4))),
            crypto: Arc::new(Semaphore::new(2)),
            watch_reads: Arc::new(Semaphore::new(1)),
            watch_admission: Arc::new(Semaphore::new(WATCH_STREAMS + 1)),
        }
    }
    pub fn acquire(&self) -> StoreResult<CertificateWork> {
        self.acquire_with_watch(None, None)
    }
    pub async fn acquire_watch(&self) -> StoreResult<CertificateWork> {
        // Fixed-phase pollers must not repeatedly overtake a new registration.
        // Bound admission to 16 producer calls plus one still-draining previous
        // read. A producer may queue its next poll before that drain finishes.
        // A 16-total cap would let the old pollers exclude a new 16th stream
        // before it could join the FIFO, recreating starvation at admission.
        // A queued read owns no ordinary operation permit or database session.
        let admission = self
            .watch_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                StoreError::unavailable("certificate watch admission capacity exhausted")
            })?;
        let watch = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            self.watch_reads.clone().acquire_owned(),
        )
        .await
        .map_err(|_| StoreError::unavailable("certificate watch admission deadline elapsed"))?
        .map_err(|_| StoreError::unavailable("certificate watch admission closed"))?;
        self.acquire_with_watch(Some(watch), Some(admission))
    }
    fn acquire_with_watch(
        &self,
        watch: Option<OwnedSemaphorePermit>,
        admission: Option<OwnedSemaphorePermit>,
    ) -> StoreResult<CertificateWork> {
        let permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| StoreError::unavailable("certificate work capacity exhausted"))?;
        Ok(CertificateWork {
            operation: Arc::new(CertificateWorkPermits {
                _operation: permit,
                _watch: watch,
                _watch_admission: admission,
            }),
            crypto: self.crypto.clone(),
        })
    }
}
// The watch-specific slot shares the exact operation/client/drain lifetime.
// A canceled read cannot release it while SQL is still queued on the session.
pub(crate) struct CertificateWorkPermits {
    _operation: OwnedSemaphorePermit,
    _watch: Option<OwnedSemaphorePermit>,
    _watch_admission: Option<OwnedSemaphorePermit>,
}
pub(crate) struct CertificateWork {
    operation: Arc<CertificateWorkPermits>,
    crypto: Arc<Semaphore>,
}
impl CertificateWork {
    /// Backend session wrappers hold these permits for the whole checkout,
    /// including any drain that outlives a cancelled read.
    pub(crate) fn permits(&self) -> Arc<CertificateWorkPermits> {
        self.operation.clone()
    }
    pub async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> StoreResult<T> + Send + 'static,
    ) -> StoreResult<T> {
        let crypto = self
            .crypto
            .clone()
            .try_acquire_owned()
            .map_err(|_| StoreError::unavailable("certificate crypto capacity exhausted"))?;
        let operation = self.operation.clone();
        tokio::task::spawn_blocking(move || {
            let (_operation, _crypto) = (operation, crypto);
            work()
        })
        .await
        .map_err(|_| StoreError::internal("certificate crypto worker failed"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn poll_once<F: std::future::Future>(
        future: std::pin::Pin<&mut F>,
    ) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }
    #[tokio::test(start_paused = true)]
    async fn watch_fifo_bounds_waiters_and_cancellation_preserves_capacity() {
        let limits = CertificateWorkLimits::new(4);
        let held = limits.acquire_watch().await.unwrap();
        // The logical read returned; only its PendingDrain-style Arc survives.
        let retained_drain = held.operation.clone();
        drop(held);
        let mut waiters = Vec::new();
        for _ in 0..WATCH_STREAMS {
            let limits = limits.clone();
            let mut waiter = Box::pin(async move { limits.acquire_watch().await });
            assert!(poll_once(waiter.as_mut()).is_pending());
            waiters.push(waiter);
        }
        assert!(matches!(
            limits.acquire_watch().await,
            Err(StoreError::Unavailable { .. })
        ));
        assert_eq!(
            limits.operations.available_permits(),
            2,
            "queued reads own no ordinary slots"
        );
        // Cancel the queue head. The next existing waiter must retain its turn
        // ahead of a newly admitted caller, without a queue registry of our own.
        drop(waiters.remove(0));
        let replacement_limits = limits.clone();
        let mut replacement = Box::pin(async move { replacement_limits.acquire_watch().await });
        assert!(poll_once(replacement.as_mut()).is_pending());
        assert!(
            poll_once(waiters[0].as_mut()).is_pending(),
            "drain ownership cannot be overtaken"
        );
        let ordinary = limits.acquire().unwrap();
        assert_eq!(limits.operations.available_permits(), 1);
        drop(ordinary);
        drop(retained_drain);
        for waiter in waiters {
            let next = waiter.await.unwrap();
            assert!(poll_once(replacement.as_mut()).is_pending());
            drop(next);
        }
        drop(replacement.await.unwrap());
        assert_eq!(limits.operations.available_permits(), 3);
        assert_eq!(limits.watch_reads.available_permits(), 1);
        assert_eq!(
            limits.watch_admission.available_permits(),
            WATCH_STREAMS + 1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn watch_admission_deadline_releases_only_queued_ownership() {
        let limits = CertificateWorkLimits::new(2);
        let held = limits.acquire_watch().await.unwrap();
        let mut pending = Box::pin(limits.acquire_watch());
        assert!(poll_once(pending.as_mut()).is_pending());
        tokio::time::advance(std::time::Duration::from_millis(3001)).await;
        assert!(matches!(pending.await, Err(StoreError::Unavailable { .. })));
        assert_eq!(limits.watch_admission.available_permits(), WATCH_STREAMS);
        assert_eq!(limits.watch_reads.available_permits(), 0);
        assert_eq!(limits.operations.available_permits(), 0);
        drop(held);
        assert_eq!(
            limits.watch_admission.available_permits(),
            WATCH_STREAMS + 1
        );
        assert!(limits.acquire_watch().await.is_ok());
    }
    #[tokio::test]
    async fn cancelled_crypto_keeps_both_permits_until_worker_really_returns() {
        let limits = CertificateWorkLimits::new(2);
        let work = limits.acquire().unwrap();
        let (entered_tx, entered) = tokio::sync::oneshot::channel();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            work.blocking(move || {
                let _ = entered_tx.send(());
                let _ = release.recv();
                Ok(())
            })
            .await
        });
        entered.await.unwrap();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        assert_eq!(limits.operations.available_permits(), 0);
        assert_eq!(limits.crypto.available_permits(), 1);
        assert!(
            limits.acquire().is_err(),
            "cancelled RPC must not admit another operation while its worker runs"
        );
        drop(release_tx);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while limits.operations.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(limits.crypto.available_permits(), 2);
        assert!(limits.acquire().is_ok());
    }
    #[test]
    fn operation_capacity_always_reserves_ordinary_database_capacity() {
        for capacity in [1, 2, 4, 16, 1024] {
            let limits = CertificateWorkLimits::new(capacity);
            assert!(limits.operations.available_permits() < capacity);
            assert!(limits.operations.available_permits() <= 4);
        }
    }
}
