//! Demand-loaded application certificates. The worker owns all asynchronous
//! work; the handle only accesses bounded memory and queues bounded fetches.
//!
//! Entry counts bound the cache. Hostnames holding a certificate and hostnames
//! without one have separate budgets, so a hostname without a certificate
//! displaces only another hostname without one.
//!
//! A fetch waits in one of two queues until the worker has a slot for it. A
//! lookup has a handshake waiting on it; a refresh comes from a notification
//! or a due lease. Slots alternate between the queues when both hold work, so
//! neither a flood of lookups nor a wave of refreshes starves the other. A
//! bounded number of handshakes wait at once; the next one fails immediately.
mod client;
mod metrics;
mod notifications;
mod worker;
pub use client::GrpcCertificateResolver;
pub use notifications::{CertificateWatchFuture, CertificateWatchStream, CertificateWatcher};
#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
mod handshake_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

use proxy_core::observability::{recorder::ObservabilityRecorder, Operation, Outcome};
use sleepypods_api::{pb, TlsHostname};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_rustls::rustls::ServerConfig;
pub use worker::TlsCertificateWorker;

/// Hostnames that may hold a certificate.
pub const CERTIFICATE_CACHE_ENTRIES: usize = 1024;
/// Hostnames cached without a certificate: missing, pending or failed lookups.
pub const CERTIFICATE_NEGATIVE_ENTRIES: usize = 1024;
pub const CERTIFICATE_FETCHES: usize = 3;
/// Handshakes that may wait for a lookup at once, counting those that join a
/// lookup already queued or in flight. A further lookup fails at once. Half
/// the default handshake admission, so lookups for unknown names leave
/// handshake permits for hostnames whose certificates are cached.
pub const CERTIFICATE_LOOKUP_WAITERS: usize = 64;
pub const CERTIFICATE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
pub const CERTIFICATE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
// Worst-case memory follows from these counts and the message caps, about
// 220 MiB in total:
// - Validation admits a bundle of at most 128 KiB with 100 SANs, so a cached
//   certificate retains at most about 160 KiB: 160 MiB for a full cache, and
//   about 10 MiB when chains are a few KiB.
// - A negative entry holds a hostname and bookkeeping, about 1 KiB at most.
// - A resolve response is at most 256 KiB on the wire. Decoding a malformed
//   one of minimal repeated fields expands it to at most 8 MiB before
//   validation rejects it, so three fetches hold at most 24 MiB.
// - A watch snapshot is at most 512 KiB on the wire and expands to at most
//   32 MiB the same way.
// A configuration replaced or evicted while connections still use it lives
// until the last of them closes; the connection limit bounds how many.

pub type CertificateResolveFuture = Pin<
    Box<
        dyn Future<Output = Result<pb::ResolveTlsCertificateResponse, CertificateLookupError>>
            + Send,
    >,
>;
pub trait CertificateResolver: Send + Sync + 'static {
    fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CertificateLookupError {
    Invalid,
    Unavailable,
    Capacity,
    Deadline,
    Stopped,
}
impl fmt::Display for CertificateLookupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid certificate response",
            Self::Unavailable => "certificate resolution unavailable",
            Self::Capacity => "certificate cache capacity exhausted",
            Self::Deadline => "certificate lookup deadline elapsed",
            Self::Stopped => "certificate worker stopped",
        })
    }
}
impl std::error::Error for CertificateLookupError {}

#[derive(Clone)]
pub struct TlsCertificateStore {
    shared: Arc<Shared>,
}
impl fmt::Debug for TlsCertificateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsCertificateStore")
            .field("usage", &self.usage())
            .finish()
    }
}
struct Shared {
    #[cfg(test)]
    validation_gate: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    task_high_water: std::sync::atomic::AtomicUsize,
    retained_tasks: std::sync::atomic::AtomicUsize,
    watches: std::sync::atomic::AtomicUsize,
    observability: ObservabilityRecorder,
    wall: Arc<dyn Fn() -> i64 + Send + Sync>,
    state: Mutex<State>,
    fetches: Arc<Semaphore>,
    /// One permit for each handshake waiting on a lookup.
    lookup_waiters: Semaphore,
    /// Wakes the worker when a fetch joins a queue.
    queued: Notify,
}
struct State {
    interests_changed: watch::Sender<()>,
    entries: HashMap<String, Entry>,
    /// Hostnames awaiting a fetch slot, in request order. Each hostname
    /// appears at most once in each queue.
    lookups: BTreeMap<u64, String>,
    refreshes: BTreeMap<u64, String>,
    serial: u64,
    stopped: bool,
}
impl State {
    fn next_serial(&mut self) -> Result<u64, CertificateLookupError> {
        self.serial = self
            .serial
            .checked_add(1)
            .ok_or(CertificateLookupError::Capacity)?;
        Ok(self.serial)
    }
    /// Queues a fetch for a waiting handshake unless one is running or queued.
    fn queue_lookup(&mut self, hostname: &str, position: u64) {
        if let Some(entry) = self.entries.get_mut(hostname) {
            if !entry.pending && entry.lookup.is_none() {
                entry.lookup = Some(position);
                self.lookups.insert(position, hostname.to_owned());
            }
        }
    }
    /// Queues a fetch that no handshake waits on unless one is running or
    /// queued.
    fn queue_refresh(&mut self, hostname: &str, position: u64) {
        if let Some(entry) = self.entries.get_mut(hostname) {
            if !entry.pending && entry.refresh.is_none() {
                entry.refresh = Some(position);
                self.refreshes.insert(position, hostname.to_owned());
            }
        }
    }
}
struct Entry {
    incarnation: u64,
    floor: u64,
    generation: u64,
    touched: u64,
    value: Option<View>,
    pending: bool,
    /// Positions in `State::lookups` and `State::refreshes`.
    lookup: Option<u64>,
    refresh: Option<u64>,
    changed: watch::Sender<u64>,
    last_error: Option<CertificateLookupError>,
}
impl Entry {
    /// Whether the entry holds a certificate. This decides its budget and its
    /// place in the watch.
    fn certified(&self) -> bool {
        self.value
            .as_ref()
            .is_some_and(|view| view.config.is_some())
    }
}
#[derive(Clone)]
struct View {
    revision: u64,
    config: Option<Arc<ServerConfig>>,
    /// Chain and key bytes behind `config`.
    bytes: usize,
    metadata: Option<pb::CertificateMetadata>,
    expires: Instant,
    refresh: Instant,
}
struct Fetch {
    hostname: String,
    generation: u64,
    prior: Option<View>,
    floor: u64,
    _slot: OwnedSemaphorePermit,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CertificateCacheUsage {
    /// Hostnames holding a certificate.
    pub certificates: usize,
    /// Hostnames cached without one.
    pub negative_entries: usize,
    /// Chain and key bytes held by cached certificates.
    pub certificate_bytes: usize,
    /// Fetches running or validating.
    pub fetches: usize,
    /// Lookups and refreshes awaiting a fetch slot.
    pub queued: usize,
}

impl TlsCertificateStore {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(resolver: Arc<dyn CertificateResolver>) -> (Self, TlsCertificateWorker) {
        Self::with_observability(resolver, ObservabilityRecorder::default())
    }
    pub fn with_observability(
        resolver: Arc<dyn CertificateResolver>,
        observability: ObservabilityRecorder,
    ) -> (Self, TlsCertificateWorker) {
        Self::with_clock_and_observability(resolver, Arc::new(wall_now), observability)
    }
    #[cfg(test)]
    fn with_clock(
        resolver: Arc<dyn CertificateResolver>,
        wall: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> (Self, TlsCertificateWorker) {
        Self::with_clock_and_observability(resolver, wall, ObservabilityRecorder::default())
    }
    fn with_clock_and_observability(
        resolver: Arc<dyn CertificateResolver>,
        wall: Arc<dyn Fn() -> i64 + Send + Sync>,
        observability: ObservabilityRecorder,
    ) -> (Self, TlsCertificateWorker) {
        let shared = Arc::new(Shared {
            #[cfg(test)]
            validation_gate: Mutex::new(None),
            task_high_water: std::sync::atomic::AtomicUsize::new(0),
            retained_tasks: std::sync::atomic::AtomicUsize::new(0),
            watches: std::sync::atomic::AtomicUsize::new(0),
            observability,
            wall,
            state: Mutex::new(State {
                interests_changed: watch::channel(()).0,
                entries: HashMap::new(),
                lookups: BTreeMap::new(),
                refreshes: BTreeMap::new(),
                serial: 0,
                stopped: false,
            }),
            fetches: Arc::new(Semaphore::new(CERTIFICATE_FETCHES)),
            lookup_waiters: Semaphore::new(CERTIFICATE_LOOKUP_WAITERS),
            queued: Notify::new(),
        });
        let cache = Self { shared };
        let worker = TlsCertificateWorker::new(cache.clone(), resolver);
        (cache, worker)
    }
    /// A passthrough-only adapter has no certificate delivery worker.
    #[cfg(any(test, feature = "test-support"))]
    pub fn disabled() -> Self {
        struct Disabled;
        impl CertificateResolver for Disabled {
            fn resolve(&self, _: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
                Box::pin(async { Err(CertificateLookupError::Stopped) })
            }
        }
        let (cache, worker) = Self::new(Arc::new(Disabled));
        drop(worker);
        cache
    }
    pub fn usage(&self) -> CertificateCacheUsage {
        let state = self.shared.state.lock().unwrap();
        let certified = || state.entries.values().filter(|entry| entry.certified());
        let certificates = certified().count();
        CertificateCacheUsage {
            certificates,
            negative_entries: state.entries.len() - certificates,
            certificate_bytes: certified()
                .filter_map(|entry| entry.value.as_ref())
                .map(|view| view.bytes)
                .sum(),
            fetches: CERTIFICATE_FETCHES - self.shared.fetches.available_permits(),
            queued: state.lookups.len() + state.refreshes.len(),
        }
    }
    /// Each waiter remains owned by its admitted handshake. A cached answer
    /// returns at once. Otherwise the handshake takes one of
    /// `CERTIFICATE_LOOKUP_WAITERS` places, or fails with `Capacity` when all
    /// are taken, and waits for a fetch slot and then for its fetch, all
    /// within one deadline. Canceling a waiter does not cancel a running
    /// fetch; a lookup whose waiters all left before it reached a slot starts
    /// no fetch.
    pub async fn resolve(
        &self,
        sni: &str,
    ) -> Result<Option<Arc<ServerConfig>>, CertificateLookupError> {
        let hostname = TlsHostname::new(sni)
            .map_err(|_| CertificateLookupError::Invalid)?
            .to_string();
        let deadline = Instant::now() + CERTIFICATE_LOOKUP_TIMEOUT;
        let (_place, mut receiver) = {
            let mut state = self.shared.state.lock().unwrap();
            if state.stopped {
                return Err(CertificateLookupError::Stopped);
            }
            let serial = state.next_serial()?;
            if let Some(entry) = state.entries.get_mut(&hostname) {
                entry.touched = serial;
                if let Some(view) = &entry.value {
                    if usable(view, Instant::now(), (self.shared.wall)()) {
                        return Ok(view.config.clone());
                    }
                }
            }
            let place = self
                .shared
                .lookup_waiters
                .try_acquire()
                .map_err(|_| CertificateLookupError::Capacity)?;
            if !state.entries.contains_key(&hostname) {
                let (changed, _) = watch::channel(0);
                state.entries.insert(
                    hostname.clone(),
                    Entry {
                        incarnation: serial,
                        floor: 0,
                        generation: serial,
                        touched: serial,
                        value: None,
                        pending: false,
                        lookup: None,
                        refresh: None,
                        changed,
                        last_error: None,
                    },
                );
                enforce_budgets(&mut state, &hostname);
            }
            state.queue_lookup(&hostname, serial);
            self.shared.queued.notify_one();
            (place, state.entries[&hostname].changed.subscribe())
        };
        loop {
            {
                let state = self.shared.state.lock().unwrap();
                let entry = state
                    .entries
                    .get(&hostname)
                    .ok_or(CertificateLookupError::Unavailable)?;
                if let Some(view) = &entry.value {
                    if usable(view, Instant::now(), (self.shared.wall)()) {
                        return Ok(view.config.clone());
                    }
                }
                if !entry.pending && entry.lookup.is_none() {
                    return Err(entry
                        .last_error
                        .unwrap_or(CertificateLookupError::Unavailable));
                }
            }
            tokio::time::timeout_at(deadline, receiver.changed())
                .await
                .map_err(|_| CertificateLookupError::Deadline)?
                .map_err(|_| CertificateLookupError::Unavailable)?;
        }
    }
    fn stop(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.stopped = true;
        state.entries.clear();
        state.lookups.clear();
        state.refreshes.clear();
        state.interests_changed.send_replace(());
    }
}
/// Holds each kind of entry to its budget by evicting one entry of that kind:
/// the least recently handshaken, taken first from those with no handshake
/// waiting. Call it after inserting `keep` or after `keep` gains or loses its
/// certificate; `keep` itself stays.
fn enforce_budgets(state: &mut State, keep: &str) {
    if state.entries.len() <= CERTIFICATE_CACHE_ENTRIES.min(CERTIFICATE_NEGATIVE_ENTRIES) {
        return;
    }
    for (certified, budget) in [
        (true, CERTIFICATE_CACHE_ENTRIES),
        (false, CERTIFICATE_NEGATIVE_ENTRIES),
    ] {
        let kind = || {
            state
                .entries
                .iter()
                .filter(|(_, entry)| entry.certified() == certified)
        };
        if kind().count() <= budget {
            continue;
        }
        let victim = kind()
            .filter(|(hostname, _)| hostname.as_str() != keep)
            .min_by_key(|(_, entry)| (entry.changed.receiver_count() > 0, entry.touched))
            .map(|(hostname, _)| hostname.clone());
        if let Some(hostname) = victim {
            evict(state, &hostname);
        }
    }
}
/// Removes an entry and its queued fetches. Removing a certificate changes
/// what the watch follows.
fn evict(state: &mut State, hostname: &str) {
    let Some(entry) = state.entries.remove(hostname) else {
        return;
    };
    if let Some(position) = entry.lookup {
        state.lookups.remove(&position);
    }
    if let Some(position) = entry.refresh {
        state.refreshes.remove(&position);
    }
    if entry.certified() {
        state.interests_changed.send_replace(());
    }
}

fn wall_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
fn usable(view: &View, now: Instant, wall: i64) -> bool {
    now < view.expires
        && view
            .metadata
            .as_ref()
            .is_none_or(|m| wall >= m.not_before_unix_millis && wall < m.not_after_unix_millis)
}
