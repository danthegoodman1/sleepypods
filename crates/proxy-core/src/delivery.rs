//! Retain admission and drain activity for body bytes until they are delivered.
//!
//! An HTTP/1.1 body stalls only when its socket write stalls, which the socket
//! write deadline bounds. An HTTP/2 stream can stall behind flow control while
//! its connection keeps writing, so HTTP/2 bodies also run a watchdog.
use crate::{AdmissionPermit, Shutdown};
use bytes::Buf;
use std::{
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

pub(crate) struct DeliveryState {
    _permit: Option<Arc<AdmissionPermit>>,
    _upgrade: Option<Arc<AdmissionPermit>>,
    _drain: Option<Arc<crate::DrainPermit>>,
    watchdog: Option<Watchdog>,
}
pub(crate) enum DeliveryFailure {
    Downstream(Shutdown),
    Upstream(Option<hyper_util::client::legacy::connect::CaptureConnection>),
}
impl From<Shutdown> for DeliveryFailure {
    fn from(shutdown: Shutdown) -> Self {
        Self::Downstream(shutdown)
    }
}
impl DeliveryFailure {
    pub(crate) fn cancel(&self) {
        match self {
            Self::Downstream(shutdown) => shutdown.shutdown(),
            Self::Upstream(Some(captured)) => {
                if let Some(connected) = captured.connection_metadata().as_ref() {
                    connected.poison();
                    let mut extensions = http::Extensions::new();
                    connected.get_extras(&mut extensions);
                    if let Some(shutdown) = extensions.get::<crate::resources::UpstreamShutdown>() {
                        shutdown.0.shutdown();
                    }
                }
            }
            Self::Upstream(None) => {}
        }
    }
}

/// Stall detection for one HTTP/2 body.
pub(crate) struct Watchdog {
    progress: Mutex<DeliveryProgress>,
    timeout: Duration,
    failed: DeliveryFailure,
    task: OnceLock<tokio::task::JoinHandle<()>>,
}
struct DeliveryProgress {
    pending: usize,
    last: tokio::time::Instant,
}
impl Watchdog {
    pub(crate) fn new(timeout: Duration, failed: impl Into<DeliveryFailure>) -> Self {
        Self {
            progress: Mutex::new(DeliveryProgress {
                pending: 0,
                last: tokio::time::Instant::now(),
            }),
            timeout,
            failed: failed.into(),
            task: OnceLock::new(),
        }
    }
}
impl DeliveryState {
    pub(crate) fn new(
        permit: Option<Arc<AdmissionPermit>>,
        upgrade: Option<Arc<AdmissionPermit>>,
        drain: Option<Arc<crate::DrainPermit>>,
        watchdog: Option<Watchdog>,
    ) -> Arc<Self> {
        Arc::new(Self {
            _permit: permit,
            _upgrade: upgrade,
            _drain: drain,
            watchdog,
        })
    }
    fn start(self: &Arc<Self>, watchdog: &Watchdog) {
        watchdog.task.get_or_init(|| {
            let weak = Arc::downgrade(self);
            // Retain admission until an aborted task is dropped too. Rapid short
            // responses cannot accumulate watchdogs outside the request limit.
            let permit = self._permit.clone();
            tokio::spawn(async move {
                let _permit = permit;
                loop {
                    let deadline = {
                        let Some(state) = weak.upgrade() else { return };
                        let Some(watchdog) = &state.watchdog else {
                            return;
                        };
                        let progress = watchdog.progress.lock().expect("delivery progress lock");
                        if progress.pending == 0 {
                            tokio::time::Instant::now() + watchdog.timeout
                        } else {
                            progress.last + watchdog.timeout
                        }
                    };
                    tokio::time::sleep_until(deadline).await;
                    let Some(state) = weak.upgrade() else { return };
                    let Some(watchdog) = &state.watchdog else {
                        return;
                    };
                    let progress = watchdog.progress.lock().expect("delivery progress lock");
                    if progress.pending > 0
                        && progress.last + watchdog.timeout <= tokio::time::Instant::now()
                    {
                        // Hyper has no external per-stream reset handle; closing
                        // this connection releases its flow-controlled queues.
                        watchdog.failed.cancel();
                        return;
                    }
                }
            })
        });
    }
}
impl Drop for DeliveryState {
    fn drop(&mut self) {
        if let Some(task) = self.watchdog.as_ref().and_then(|w| w.task.get()) {
            task.abort();
        }
    }
}

pub(crate) struct DeliveryBuf<B: Buf> {
    inner: B,
    state: Arc<DeliveryState>,
}
impl<B: Buf> DeliveryBuf<B> {
    pub(crate) fn new(inner: B, state: Arc<DeliveryState>) -> Self {
        let bytes = inner.remaining();
        if let Some(watchdog) = state.watchdog.as_ref().filter(|_| bytes > 0) {
            {
                // Publish timestamp and zero-to-nonzero transition together, so
                // the watchdog cannot see fresh bytes with an old idle timestamp.
                let mut progress = watchdog.progress.lock().expect("delivery progress lock");
                if progress.pending == 0 {
                    progress.last = tokio::time::Instant::now();
                }
                progress.pending += bytes;
            }
            state.start(watchdog);
        }
        Self { inner, state }
    }
}
impl<B: Buf> Buf for DeliveryBuf<B> {
    fn remaining(&self) -> usize {
        self.inner.remaining()
    }
    fn chunk(&self) -> &[u8] {
        self.inner.chunk()
    }
    fn advance(&mut self, count: usize) {
        self.inner.advance(count);
        if let Some(watchdog) = self.state.watchdog.as_ref().filter(|_| count > 0) {
            let mut progress = watchdog.progress.lock().expect("delivery progress lock");
            progress.pending -= count;
            progress.last = tokio::time::Instant::now();
        }
    }
    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        self.inner.chunks_vectored(dst)
    }
}
impl<B: Buf> Drop for DeliveryBuf<B> {
    fn drop(&mut self) {
        if let Some(watchdog) = &self.state.watchdog {
            watchdog
                .progress
                .lock()
                .expect("delivery progress lock")
                .pending -= self.inner.remaining();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn watched(
        permit: Option<Arc<AdmissionPermit>>,
        timeout: Duration,
        failed: Shutdown,
    ) -> Arc<DeliveryState> {
        DeliveryState::new(permit, None, None, Some(Watchdog::new(timeout, failed)))
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_frame_after_idle_publishes_progress_before_watchdog_can_expire() {
        let failed = Shutdown::new();
        let state = watched(None, Duration::from_secs(1), failed.clone());
        let mut first = DeliveryBuf::new(Bytes::from_static(b"a"), state.clone());
        first.advance(1);
        drop(first);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(!failed.is_shutdown());
        let mut next = DeliveryBuf::new(Bytes::from_static(b"b"), state.clone());
        tokio::task::yield_now().await;
        assert!(!failed.is_shutdown());
        next.advance(1);
        drop(next);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(!failed.is_shutdown());
    }
    #[tokio::test(start_paused = true)]
    async fn stalled_bytes_fail_a_watched_body_after_the_timeout() {
        let failed = Shutdown::new();
        let state = watched(None, Duration::from_secs(1), failed.clone());
        let _stalled = DeliveryBuf::new(Bytes::from_static(b"a"), state.clone());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(failed.is_shutdown());
    }
    #[tokio::test]
    async fn body_completion_aborts_owned_watchdog_and_returns_its_request_slot() {
        let admission = crate::AdmissionLimiter::new(1);
        let state = watched(
            Some(Arc::new(admission.try_acquire().unwrap())),
            Duration::from_secs(3600),
            Shutdown::new(),
        );
        let weak = Arc::downgrade(&state);
        let mut bytes = DeliveryBuf::new(Bytes::from_static(b"ok"), state.clone());
        drop(state);
        assert_eq!(admission.in_flight(), 1, "queued bytes retain capacity");
        bytes.advance(2);
        drop(bytes);
        assert!(
            weak.upgrade().is_none(),
            "watchdog does not retain delivery state"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while admission.in_flight() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn unwatched_bytes_retain_capacity_without_a_task() {
        let admission = crate::AdmissionLimiter::new(1);
        let state = DeliveryState::new(
            Some(Arc::new(admission.try_acquire().unwrap())),
            None,
            None,
            None,
        );
        let mut bytes = DeliveryBuf::new(Bytes::from_static(b"ok"), state);
        assert_eq!(admission.in_flight(), 1, "queued bytes retain capacity");
        bytes.advance(2);
        drop(bytes);
        assert_eq!(admission.in_flight(), 0);
    }
}
