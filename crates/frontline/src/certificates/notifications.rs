//! One lazy, owned watch. Full replacement registrations coalesce interests;
//! neither notification cursors nor reconnects grant authorization leases.
use super::*;
use proxy_core::Shutdown;
use sleepypods_api::{CertificateId, CertificateRevision};
use tonic::codegen::tokio_stream::{Stream, StreamExt};

pub type CertificateWatchStream = Pin<
    Box<dyn Stream<Item = Result<pb::WatchTlsCertificatesResponse, CertificateLookupError>> + Send>,
>;
pub type CertificateWatchFuture =
    Pin<Box<dyn Future<Output = Result<CertificateWatchStream, CertificateLookupError>> + Send>>;
pub trait CertificateWatcher: Send + Sync + 'static {
    fn watch(
        &self,
        interests: mpsc::Receiver<pb::WatchTlsCertificatesRequest>,
    ) -> CertificateWatchFuture;
}
const REGISTRATION_INTERVAL: Duration = Duration::from_millis(250);
const SESSION_LIMIT: Duration = Duration::from_secs(65);
struct Registration {
    id: u64,
    incarnations: HashMap<String, u64>,
    request: pb::WatchTlsCertificatesRequest,
}
/// The watch follows the hostnames that hold a certificate. Their budget
/// equals the interest limit of one stream, and a hostname without a
/// certificate has nothing to rotate or revoke.
fn registration(cache: &TlsCertificateStore, id: u64) -> Registration {
    let state = cache.shared.state.lock().unwrap();
    let incarnations: HashMap<String, u64> = state
        .entries
        .iter()
        .filter(|(_, entry)| entry.certified())
        .map(|(hostname, entry)| (hostname.clone(), entry.incarnation))
        .collect();
    Registration {
        id,
        request: pb::WatchTlsCertificatesRequest {
            registration: id,
            hostnames: incarnations.keys().cloned().collect(),
        },
        incarnations,
    }
}
fn has_interests(cache: &TlsCertificateStore) -> bool {
    let state = cache.shared.state.lock().unwrap();
    state.entries.values().any(Entry::certified)
}
pub(super) async fn run(
    cache: TlsCertificateStore,
    watcher: Arc<dyn CertificateWatcher>,
    shutdown: Shutdown,
) {
    let mut changed = cache
        .shared
        .state
        .lock()
        .unwrap()
        .interests_changed
        .subscribe();
    // Per-worker bounded jitter avoids synchronized reconnect waves across
    // replicas without retaining per-host retry records.
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() as u64 % 501);
    let retry = Duration::from_millis(500 + jitter);
    loop {
        if shutdown.is_shutdown() {
            return;
        }
        if !has_interests(&cache) {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = changed.changed() => {},
            }
            continue;
        }
        // No spawned task or historical stream survives a session. The outer
        // fixed bound includes setup, registrations and a silent broken peer.
        {
            let _watch = metrics::WatchObservation::new(cache.clone());
            let result = tokio::select! {
                _ = shutdown.cancelled() => return,
                result = tokio::time::timeout(SESSION_LIMIT, session(&cache, watcher.as_ref(), &mut changed)) => result,
            };
            match result {
                Ok(Err(error)) => cache.event(Operation::CertificateWatch, metrics::outcome(error)),
                Err(_) => cache.event(Operation::CertificateWatch, Outcome::Timeout),
                Ok(Ok(())) => {}
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(retry) => {},
        }
    }
}
async fn session(
    cache: &TlsCertificateStore,
    watcher: &dyn CertificateWatcher,
    changed: &mut watch::Receiver<()>,
) -> Result<(), CertificateLookupError> {
    let mut next_id = 1u64;
    changed.borrow_and_update();
    let first = registration(cache, next_id);
    if first.incarnations.is_empty() {
        return Ok(());
    }
    let (requests, receiver) = mpsc::channel(1);
    requests
        .try_send(first.request.clone())
        .map_err(|_| CertificateLookupError::Capacity)?;
    let mut pending = Some(first);
    let mut acknowledgement = Instant::now() + CERTIFICATE_LOOKUP_TIMEOUT;
    let mut stream = tokio::time::timeout_at(acknowledgement, watcher.watch(receiver))
        .await
        .map_err(|_| CertificateLookupError::Deadline)??;
    let mut registered: Option<Registration> = None;
    let mut next_registration = Instant::now() + REGISTRATION_INTERVAL;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(acknowledgement), if pending.is_some() => return Err(CertificateLookupError::Deadline),
            _ = tokio::time::sleep_until(next_registration), if pending.is_none() => {
                next_registration = Instant::now() + REGISTRATION_INTERVAL;
                if changed.has_changed().unwrap_or(true) {
                    changed.borrow_and_update();
                    next_id = next_id.checked_add(1).ok_or(CertificateLookupError::Capacity)?;
                    let next = registration(cache, next_id);
                    if next.incarnations.is_empty() {
                        return Ok(());
                    }
                    requests.try_send(next.request.clone()).map_err(|_| CertificateLookupError::Capacity)?;
                    pending = Some(next);
                    acknowledgement = Instant::now() + CERTIFICATE_LOOKUP_TIMEOUT;
                }
            }
            message = stream.next() => {
                let message = message.ok_or(CertificateLookupError::Unavailable)??;
                let acknowledges = pending.as_ref().is_some_and(|r| r.id == message.registration);
                let sent = if acknowledges { pending.as_ref() } else { registered.as_ref() }
                    .filter(|r| r.id == message.registration)
                    .ok_or(CertificateLookupError::Invalid)?;
                if message.bindings.len() != sent.incarnations.len() {
                    return Err(CertificateLookupError::Invalid);
                }
                let mut seen = std::collections::HashSet::new();
                for b in &message.bindings {
                    if !sent.incarnations.contains_key(&b.hostname)
                        || !seen.insert(&b.hostname)
                        || CertificateRevision::new(b.revision).is_err()
                        || b.last_invalidating_revision > b.revision
                        || (b.revision == 0 && b.certificate_id.is_some())
                        || (b.revision > 0 && b.last_invalidating_revision == 0)
                        || b.certificate_id.as_ref().is_some_and(|id| CertificateId::new(id.clone()).is_err())
                    {
                        return Err(CertificateLookupError::Invalid);
                    }
                }
                // Validate the complete snapshot before touching any cache entry.
                for binding in message.bindings {
                    apply(cache, &binding.hostname, sent.incarnations[&binding.hostname],
                        binding.revision, binding.certificate_id.as_deref(), binding.last_invalidating_revision)?;
                }
                if acknowledges { registered = pending.take(); }
                cache.event(Operation::CertificateWatch, Outcome::Updated);
            }
        }
    }
}
fn apply(
    cache: &TlsCertificateStore,
    hostname: &str,
    incarnation: u64,
    revision: u64,
    id: Option<&str>,
    last_invalidating_revision: u64,
) -> Result<(), CertificateLookupError> {
    let mut state = cache.shared.state.lock().unwrap();
    let Some(entry) = state
        .entries
        .get(hostname)
        .filter(|e| e.incarnation == incarnation)
    else {
        return Ok(());
    };
    if revision <= entry.floor {
        return Ok(());
    }
    let retain = id.is_some()
        && entry
            .value
            .as_ref()
            .filter(|v| v.revision >= last_invalidating_revision)
            .and_then(|v| v.metadata.as_ref())
            .is_some_and(|m| Some(m.certificate_id.as_str()) == id);
    let serial = state.next_serial()?;
    let entry = state.entries.get_mut(hostname).unwrap();
    let certified = entry.certified();
    let superseded = entry.pending;
    entry.floor = revision;
    entry.generation = serial;
    entry.pending = false;
    if !retain {
        entry.value = None;
    }
    entry.changed.send_modify(|v| *v = v.wrapping_add(1));
    // The invalidation applies now; its fetch waits for a slot.
    state.queue_refresh(hostname, serial);
    if superseded {
        // Handshakes waiting on the superseded fetch keep their turn.
        state.queue_lookup(hostname, serial);
    }
    if certified && !retain {
        state.interests_changed.send_replace(());
        enforce_budgets(&mut state, hostname);
    }
    cache.shared.queued.notify_one();
    Ok(())
}
#[cfg(test)]
mod tests;
