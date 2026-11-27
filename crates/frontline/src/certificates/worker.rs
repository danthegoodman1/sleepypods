use super::*;
use proxy_core::Shutdown;
use sleepypods_api::{CertificateBundle, CertificateId, CertificateRevision};
use sleepypods_certificate::{validate_certificate, validate_hostname};
use tokio::task::JoinSet;
use tokio_rustls::rustls;

pub struct TlsCertificateWorker {
    cache: TlsCertificateStore,
    resolver: Arc<dyn CertificateResolver>,
    watcher: Option<Arc<dyn CertificateWatcher>>,
}
impl TlsCertificateWorker {
    pub(super) fn new(cache: TlsCertificateStore, resolver: Arc<dyn CertificateResolver>) -> Self {
        Self {
            cache,
            resolver,
            watcher: None,
        }
    }
    /// The native runtime supplies the same verified client as unary resolves.
    /// Watches are lazy: a cache that holds no certificate opens no stream.
    pub fn with_watch(mut self, watcher: Arc<dyn CertificateWatcher>) -> Self {
        self.watcher = Some(watcher);
        self
    }
    pub async fn run(self, shutdown: Shutdown) {
        if let Some(watcher) = self.watcher.clone() {
            let cache = self.cache.clone();
            tokio::join!(
                self.run_fetches(shutdown.clone()),
                notifications::run(cache, watcher, shutdown)
            );
        } else {
            self.run_fetches(shutdown).await;
        }
    }
    /// Starts queued fetches as slots free up. On shutdown, cancel network
    /// work, then join every validation already dispatched. Bounded blocking
    /// input and its fetch slot survive caller cancellation; there is no
    /// independent refresh task per hostname.
    async fn run_fetches(self, shutdown: Shutdown) {
        let mut tasks = JoinSet::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh_turn = false;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                _ = self.cache.shared.queued.notified() => {},
                _ = ticker.tick() => self.cache.queue_due_refreshes(),
            }
            while tasks.len() < CERTIFICATE_FETCHES {
                let Some(fetch) = self.cache.next_fetch(&mut refresh_turn) else {
                    break;
                };
                tasks.spawn(fetch_one(
                    self.cache.clone(),
                    self.resolver.clone(),
                    fetch,
                    shutdown.clone(),
                ));
            }
            self.cache.tasks(tasks.len());
        }
        self.cache.stop();
        while tasks.join_next().await.is_some() {
            self.cache.tasks(tasks.len());
        }
    }
}
impl TlsCertificateStore {
    /// Queues a refresh for every certificate whose lease is due.
    fn queue_due_refreshes(&self) {
        let now = Instant::now();
        let mut state = self.shared.state.lock().unwrap();
        let State {
            entries,
            refreshes,
            serial,
            ..
        } = &mut *state;
        for (hostname, entry) in entries {
            let due = entry
                .value
                .as_ref()
                .is_some_and(|view| view.config.is_some() && now >= view.refresh);
            if due && !entry.pending && entry.refresh.is_none() {
                let Some(position) = serial.checked_add(1) else {
                    return;
                };
                *serial = position;
                entry.refresh = Some(position);
                refreshes.insert(position, hostname.clone());
            }
        }
    }
    /// Takes a fetch slot and the next queued hostname. When both queues hold
    /// work, `refresh_turn` says which goes first, and it flips with each
    /// fetch.
    fn next_fetch(&self, refresh_turn: &mut bool) -> Option<Fetch> {
        let slot = self.shared.fetches.clone().try_acquire_owned().ok()?;
        let mut state = self.shared.state.lock().unwrap();
        let State {
            entries,
            lookups,
            refreshes,
            ..
        } = &mut *state;
        loop {
            let refresh = if lookups.is_empty() || refreshes.is_empty() {
                lookups.is_empty()
            } else {
                *refresh_turn
            };
            let queue = if refresh {
                &mut *refreshes
            } else {
                &mut *lookups
            };
            let (_, hostname) = queue.pop_first()?;
            let Some(entry) = entries.get_mut(&hostname) else {
                continue;
            };
            if refresh {
                entry.refresh = None;
            } else {
                entry.lookup = None;
                // Every handshake waiting on this lookup has left.
                if entry.refresh.is_none() && entry.changed.receiver_count() == 0 {
                    continue;
                }
            }
            if let Some(position) = entry.lookup.take() {
                lookups.remove(&position);
            }
            if let Some(position) = entry.refresh.take() {
                refreshes.remove(&position);
            }
            entry.pending = true;
            entry.last_error = None;
            *refresh_turn = !refresh;
            return Some(Fetch {
                hostname,
                generation: entry.generation,
                prior: entry.value.clone(),
                floor: entry.floor,
                _slot: slot,
            });
        }
    }
}
impl Drop for TlsCertificateWorker {
    fn drop(&mut self) {
        self.cache.stop();
        self.cache.tasks(0);
    }
}

async fn fetch_one(
    cache: TlsCertificateStore,
    resolver: Arc<dyn CertificateResolver>,
    fetch: Fetch,
    shutdown: Shutdown,
) {
    let started = Instant::now();
    let wall = (cache.shared.wall)();
    let deadline = started + CERTIFICATE_LOOKUP_TIMEOUT;
    let operation = if fetch.prior.is_some() {
        Operation::CertificateRefresh
    } else {
        Operation::CertificateFetch
    };
    cache.event(operation, Outcome::Started);
    let request = pb::ResolveTlsCertificateRequest {
        server_name: fetch.hostname.clone(),
        known_view_revision: fetch.prior.as_ref().map(|v| v.revision),
    };
    let response = tokio::select! {
        _=shutdown.cancelled()=>Err(CertificateLookupError::Stopped),
        result=tokio::time::timeout_at(deadline,resolver.resolve(request))=>result.unwrap_or(Err(CertificateLookupError::Deadline)),
    };
    let hostname = fetch.hostname.clone();
    let generation = fetch.generation;
    cache.event(
        operation,
        match &response {
            Ok(response) => match response.value {
                Some(pb::resolve_tls_certificate_response::Value::Missing(_)) => Outcome::Miss,
                Some(pb::resolve_tls_certificate_response::Value::Unchanged(_)) => Outcome::Hit,
                _ => Outcome::Success,
            },
            Err(error) => metrics::outcome(*error),
        },
    );
    // The worker task awaits this handle even after deadline/shutdown. The
    // fetch slot moves into the closure, so an abandoned handshake or an
    // aborted outer future leaves it held until validation ends.
    let result = match response {
        Ok(response) => {
            #[cfg(test)]
            let gate = cache.shared.validation_gate.lock().unwrap().take();
            tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if let Some((entered, release)) = gate {
                    let _ = entered.send(());
                    let _ = release.recv();
                }
                validate_view(fetch, response, started, wall)
            })
            .await
            .unwrap_or(Err(CertificateLookupError::Invalid))
        }
        Err(error) => Err(error),
    };
    let mut state = cache.shared.state.lock().unwrap();
    let Some(entry) = state
        .entries
        .get_mut(&hostname)
        .filter(|e| e.generation == generation)
    else {
        drop(state);
        cache.event(Operation::CertificateInstall, Outcome::Canceled);
        return;
    };
    entry.pending = false;
    let certified = entry.certified();
    if shutdown.is_shutdown() || Instant::now() >= deadline {
        entry.last_error = Some(CertificateLookupError::Deadline);
    } else {
        match result {
            Ok(view) if Instant::now() < view.expires => {
                entry.floor = entry.floor.max(view.revision);
                entry.value = Some(view);
                entry.last_error = None;
            }
            Ok(_) => entry.last_error = Some(CertificateLookupError::Invalid),
            Err(error) => entry.last_error = Some(error),
        }
    }
    if entry.last_error.is_some() {
        if let Some(view) = &mut entry.value {
            view.refresh = Instant::now() + Duration::from_secs(60);
        }
    }
    entry.changed.send_modify(|v| *v = v.wrapping_add(1));
    let outcome = entry.last_error.map_or(Outcome::Success, metrics::outcome);
    if entry.certified() != certified {
        state.interests_changed.send_replace(());
        enforce_budgets(&mut state, &hostname);
    }
    drop(state);
    cache.event(Operation::CertificateInstall, outcome);
}

/// Holds the fetch slot until validation ends.
pub(super) fn validate_view(
    fetch: Fetch,
    response: pb::ResolveTlsCertificateResponse,
    started: Instant,
    wall: i64,
) -> Result<View, CertificateLookupError> {
    let invalid = || CertificateLookupError::Invalid;
    if response.server_name != fetch.hostname || response.observed_at_unix_millis < 0 {
        return Err(invalid());
    }
    CertificateRevision::new(response.view_revision).map_err(|_| invalid())?;
    if response.view_revision < fetch.floor {
        return Err(invalid());
    }
    if fetch
        .prior
        .as_ref()
        .is_some_and(|v| response.view_revision < v.revision)
    {
        return Err(invalid());
    }
    let authority = wall.max(response.observed_at_unix_millis);
    let mut ttl = response.authorization_ttl_millis.min(300_000);
    let (config, bytes, metadata) = match response.value.ok_or_else(invalid)? {
        pb::resolve_tls_certificate_response::Value::Missing(_) => {
            if fetch
                .prior
                .as_ref()
                .is_some_and(|v| v.revision == response.view_revision && v.config.is_some())
            {
                return Err(invalid());
            }
            ttl = ttl.min(1000);
            (None, 0, None)
        }
        pb::resolve_tls_certificate_response::Value::Unchanged(metadata) => {
            let prior = fetch.prior.as_ref().ok_or_else(invalid)?;
            if prior.revision != response.view_revision
                || prior.config.is_none()
                || !prior
                    .metadata
                    .as_ref()
                    .is_some_and(|old| material_metadata_matches(old, &metadata))
            {
                return Err(invalid());
            }
            ttl = ttl.min(remaining(&metadata, authority)?);
            (prior.config.clone(), prior.bytes, Some(metadata))
        }
        pb::resolve_tls_certificate_response::Value::Found(found) => {
            if fetch
                .prior
                .as_ref()
                .is_some_and(|v| v.revision == response.view_revision)
            {
                return Err(invalid());
            }
            let metadata = found.metadata.ok_or_else(invalid)?;
            let bundle = found.bundle.ok_or_else(invalid)?;
            let bundle = CertificateBundle::new(bundle.chain_der, bundle.private_key_pkcs8_der)
                .map_err(|_| invalid())?;
            CertificateId::new(metadata.certificate_id.clone()).map_err(|_| invalid())?;
            CertificateRevision::new(metadata.version).map_err(|_| invalid())?;
            if metadata.deleted
                || metadata.version == 0
                || response.view_revision == 0
                || metadata
                    .sealing_key_id
                    .as_ref()
                    .is_some_and(|key| key.len() > 64)
                || CertificateRevision::new(metadata.sealing_revision).is_err()
            {
                return Err(invalid());
            }
            let now = authority
                .saturating_add(started.elapsed().as_millis().min(i64::MAX as u128) as i64);
            let validated = validate_certificate(&bundle, now).map_err(|_| invalid())?;
            validate_hostname(
                bundle.chain_der(),
                &TlsHostname::new(&fetch.hostname).map_err(|_| invalid())?,
            )
            .map_err(|_| invalid())?;
            if metadata.not_before_unix_millis != validated.not_before_unix_millis
                || metadata.not_after_unix_millis != validated.not_after_unix_millis
                || metadata.dns_names != validated.dns_names
                || metadata.leaf_sha256 != validated.leaf_sha256
            {
                return Err(invalid());
            }
            ttl = ttl.min(remaining(&metadata, authority)?);
            let bytes = bundle.chain_der().iter().map(Vec::len).sum::<usize>()
                + bundle.private_key_pkcs8_der().len();
            // Sessions, tickets and early data are disabled, so a
            // configuration retains no session cache.
            let mut config = ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid())?
            .with_no_client_auth()
            .with_single_cert(
                bundle
                    .chain_der()
                    .iter()
                    .cloned()
                    .map(rustls::pki_types::CertificateDer::from)
                    .collect(),
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    bundle.private_key_pkcs8_der().to_vec(),
                )
                .into(),
            )
            .map_err(|_| invalid())?;
            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
            config.send_tls13_tickets = 0;
            config.max_early_data_size = 0;
            (Some(Arc::new(config)), bytes, Some(metadata))
        }
    };
    if ttl == 0 {
        return Err(invalid());
    }
    let expires = started + Duration::from_millis(ttl);
    let jitter = fetch
        .hostname
        .bytes()
        .fold(0u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64))
        % 6001;
    Ok(View {
        revision: response.view_revision,
        config,
        bytes,
        metadata,
        expires,
        // Short authorization/validity windows become due before their hard
        // expiry too. The one-second worker scan and bounded RPC remain best
        // effort; they never extend an unrenewed authorization lease.
        refresh: started + Duration::from_millis((60_000 + jitter).min(ttl / 2)),
    })
}
fn remaining(
    metadata: &pb::CertificateMetadata,
    authority: i64,
) -> Result<u64, CertificateLookupError> {
    if authority < metadata.not_before_unix_millis || authority >= metadata.not_after_unix_millis {
        return Err(CertificateLookupError::Invalid);
    }
    Ok(metadata.not_after_unix_millis.saturating_sub(authority) as u64)
}

fn material_metadata_matches(old: &pb::CertificateMetadata, new: &pb::CertificateMetadata) -> bool {
    !new.deleted
        && new.certificate_id == old.certificate_id
        && new.version == old.version
        && new.not_before_unix_millis == old.not_before_unix_millis
        && new.not_after_unix_millis == old.not_after_unix_millis
        && new.dns_names == old.dns_names
        && new.leaf_sha256 == old.leaf_sha256
        && new
            .sealing_key_id
            .as_ref()
            .is_none_or(|key| key.len() <= 64)
        && CertificateRevision::new(new.sealing_revision).is_ok()
}
