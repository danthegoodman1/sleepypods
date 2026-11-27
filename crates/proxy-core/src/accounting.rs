use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use tokio::sync::Notify;

/// Counts active proxy sessions through RAII guards.
#[derive(Clone, Debug)]
pub struct ActiveConnectionCounter {
    inner: Arc<Inner>,
}

/// Watches for any active-count update after an idle baseline is observed.
#[derive(Debug)]
pub struct IdleActivityWatch {
    counter: ActiveConnectionCounter,
    baseline_revision: u64,
}

#[derive(Debug, Default)]
struct Inner {
    active: AtomicUsize,
    revision: AtomicU64,
    /// Woken when the count reaches zero.
    zero: Signal,
    /// Woken on every change.
    change: Signal,
}

/// A wake-up that guards publish only while a task waits for it, so tracking a
/// session costs a few atomic operations when nothing is watching.
#[derive(Debug, Default)]
struct Signal {
    waiters: AtomicUsize,
    notify: Notify,
}

/// A counted active connection.
///
/// Dropping or releasing this guard decrements the active count exactly once.
#[derive(Debug)]
#[must_use = "dropping the guard releases the active connection count"]
pub struct ActiveConnection {
    inner: Option<Arc<Inner>>,
}

impl ActiveConnectionCounter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    pub fn active(&self) -> usize {
        self.inner.active.load(Ordering::SeqCst)
    }

    pub fn track(&self) -> ActiveConnection {
        self.inner.active.fetch_add(1, Ordering::SeqCst);
        self.inner.revision.fetch_add(1, Ordering::SeqCst);
        self.inner.change.publish();

        ActiveConnection {
            inner: Some(Arc::clone(&self.inner)),
        }
    }

    pub async fn wait_for_zero(&self) {
        self.inner.zero.wait_until(|| self.active() == 0).await;
    }

    pub async fn wait_for_count(&self, expected: usize) {
        if expected == 0 {
            return self.wait_for_zero().await;
        }
        self.inner
            .change
            .wait_until(|| self.active() == expected)
            .await;
    }

    pub fn watch_for_activity_after_idle(&self) -> Option<IdleActivityWatch> {
        let baseline_revision = self.revision();

        if self.active() != 0 {
            return None;
        }

        Some(IdleActivityWatch {
            counter: self.clone(),
            baseline_revision,
        })
    }

    fn revision(&self) -> u64 {
        self.inner.revision.load(Ordering::SeqCst)
    }
}

impl IdleActivityWatch {
    pub async fn wait_for_update(self) {
        self.counter
            .inner
            .change
            .wait_until(|| self.counter.revision() > self.baseline_revision)
            .await;
    }
}

impl Signal {
    fn publish(&self) {
        if self.waiters.load(Ordering::SeqCst) != 0 {
            self.notify.notify_waiters();
        }
    }

    /// The waiter registers before it checks `ready`, and a guard changes the
    /// count before it reads `waiters`. Every interleaving therefore ends with
    /// the waiter seeing the change or the guard seeing the waiter.
    async fn wait_until(&self, ready: impl Fn() -> bool) {
        struct Waiting<'a>(&'a AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        self.waiters.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(&self.waiters);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if ready() {
                return;
            }
            notified.await;
        }
    }
}

impl Default for ActiveConnectionCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl ActiveConnection {
    pub fn is_active(&self) -> bool {
        self.inner.is_some()
    }

    pub fn release(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };

        let previous = inner.active.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(
            previous > 0,
            "active connection count underflowed while releasing guard"
        );
        inner.revision.fetch_add(1, Ordering::SeqCst);
        inner.change.publish();
        if previous == 1 {
            inner.zero.publish();
        }
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::ActiveConnectionCounter;

    #[tokio::test]
    async fn active_count_tracks_guard_lifetime() {
        let counter = ActiveConnectionCounter::new();

        let guard = counter.track();
        assert_eq!(counter.active(), 1);

        drop(guard);
        counter.wait_for_zero().await;
        assert_eq!(counter.active(), 0);
    }

    #[tokio::test]
    async fn release_decrements_once() {
        let counter = ActiveConnectionCounter::new();
        let mut guard = counter.track();

        guard.release();
        guard.release();
        drop(guard);

        counter.wait_for_zero().await;
        assert_eq!(counter.active(), 0);
    }

    #[tokio::test]
    async fn waiters_wake_for_zero_and_for_a_specific_count() {
        let counter = ActiveConnectionCounter::new();
        let first = counter.track();

        let two = tokio::spawn({
            let counter = counter.clone();
            async move { counter.wait_for_count(2).await }
        });
        let zero = tokio::spawn({
            let counter = counter.clone();
            async move { counter.wait_for_zero().await }
        });
        tokio::task::yield_now().await;
        assert!(!two.is_finished() && !zero.is_finished());

        let second = counter.track();
        two.await.expect("waiter for two completes");
        assert!(!zero.is_finished());

        drop(first);
        drop(second);
        zero.await.expect("waiter for zero completes");
    }

    #[tokio::test]
    async fn idle_activity_watch_is_absent_when_work_is_already_active() {
        let counter = ActiveConnectionCounter::new();
        let _guard = counter.track();

        assert!(counter.watch_for_activity_after_idle().is_none());
    }

    #[tokio::test]
    async fn idle_activity_watch_resolves_for_brief_active_burst() {
        let counter = ActiveConnectionCounter::new();
        let watch = counter
            .watch_for_activity_after_idle()
            .expect("counter is idle");

        let guard = counter.track();
        drop(guard);

        watch.wait_for_update().await;
        assert_eq!(counter.active(), 0);
    }
}
