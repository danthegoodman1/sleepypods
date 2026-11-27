//! The route cache: one complete rule set per exact host.
//!
//! The coordinator's actor owns a [`RouteTable`] and is its only writer.
//! Request tasks read a [`RouteSnapshot`] the table publishes, so a lookup
//! takes no lock and writes no shared memory.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use sleepypods_api::{HostRules, RouteKind};

use crate::subscription::SubscriptionId;

/// The cached answer for one host. An empty rule set records that no binding
/// applies to the host.
#[derive(Debug)]
pub struct HostEntry {
    pub rules: HostRules,
    pub expires_at: Instant,
    /// Changes whenever the answer for this host is installed or replaced.
    pub version: u64,
    /// The live subscription that keeps this answer current. `None` for an
    /// empty rule set, and for an answer carried over from an ended stream
    /// until it is registered again.
    subscription: Option<SubscriptionId>,
    /// First-in, first-out position within the positive or negative budget.
    order: u64,
    used: AtomicBool,
}

impl HostEntry {
    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }

    /// Records a hit. After the first hit this only reads.
    pub fn mark_used(&self) {
        if !self.used.load(Ordering::Relaxed) {
            self.used.store(true, Ordering::Relaxed);
        }
    }

    pub fn was_used(&self) -> bool {
        self.used.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn subscription(&self) -> Option<SubscriptionId> {
        self.subscription
    }
}

/// An exact host of one listener family.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HostKey {
    pub kind: RouteKind,
    pub host: Arc<str>,
}

impl HostKey {
    pub fn new(kind: RouteKind, host: &str) -> Self {
        Self {
            kind,
            host: Arc::from(host),
        }
    }
}

/// What request tasks read. Immutable once published.
#[derive(Clone, Debug, Default)]
pub struct RouteSnapshot {
    http: HashMap<Arc<str>, Arc<HostEntry>>,
    sni: HashMap<Arc<str>, Arc<HostEntry>>,
}

impl RouteSnapshot {
    pub fn get(&self, kind: RouteKind, host: &str) -> Option<&Arc<HostEntry>> {
        match kind {
            RouteKind::Http => self.http.get(host),
            RouteKind::Sni => self.sni.get(host),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.http.len() + self.sni.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What a table change displaced: the answers that left the cache and the
/// subscriptions the control plane still holds for them.
#[derive(Debug, Default)]
pub struct Displaced {
    pub entries: Vec<Arc<HostEntry>>,
    pub unsubscribe: Vec<SubscriptionId>,
}

impl Displaced {
    fn take(&mut self, entry: Arc<HostEntry>, keep: Option<SubscriptionId>) {
        if let Some(id) = entry.subscription {
            if Some(id) != keep {
                self.unsubscribe.push(id);
            }
        }
        self.entries.push(entry);
    }
}

/// The actor's working copy of the cache. Rule sets and empty answers have
/// separate first-in, first-out budgets of `capacity` hosts each, so hosts
/// with no route cannot push routed hosts out.
#[derive(Debug)]
pub struct RouteTable {
    capacity: usize,
    snapshot: RouteSnapshot,
    by_subscription: HashMap<SubscriptionId, HostKey>,
    positive_order: BTreeMap<u64, HostKey>,
    negative_order: BTreeMap<u64, HostKey>,
    expiry: BTreeMap<(Instant, u64), HostKey>,
    sequence: u64,
    changed: bool,
}

impl RouteTable {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            snapshot: RouteSnapshot::default(),
            by_subscription: HashMap::new(),
            positive_order: BTreeMap::new(),
            negative_order: BTreeMap::new(),
            expiry: BTreeMap::new(),
            sequence: 0,
            changed: false,
        }
    }

    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Lowers the positive budget to what the control plane accepted.
    pub fn limit_capacity(&mut self, capacity: usize) {
        self.capacity = self.capacity.min(capacity.max(1));
    }

    pub fn positive_len(&self) -> usize {
        self.positive_order.len()
    }

    #[cfg(test)]
    pub fn negative_len(&self) -> usize {
        self.negative_order.len()
    }

    pub fn get(&self, key: &HostKey) -> Option<&Arc<HostEntry>> {
        self.snapshot.get(key.kind, &key.host)
    }

    pub fn host_of(&self, subscription: SubscriptionId) -> Option<&HostKey> {
        self.by_subscription.get(&subscription)
    }

    /// A copy for request tasks, when the table changed since the last one.
    pub fn publish(&mut self) -> Option<Arc<RouteSnapshot>> {
        if !self.changed {
            return None;
        }
        self.changed = false;
        Some(Arc::new(self.snapshot.clone()))
    }

    /// Installs the answer for `key`, replacing what was cached. A replaced
    /// rule set keeps its place in the eviction order.
    pub fn install(
        &mut self,
        key: HostKey,
        rules: HostRules,
        expires_at: Instant,
        subscription: Option<SubscriptionId>,
    ) -> Displaced {
        let mut displaced = Displaced::default();
        self.sequence += 1;
        let version = self.sequence;
        let positive = !rules.is_empty();
        let previous = self.detach(&key);
        let order = match &previous {
            Some(previous) if positive && !previous.rules.is_empty() => previous.order,
            _ => version,
        };
        if let Some(previous) = previous {
            displaced.take(previous, subscription);
        }
        let subscription = subscription.filter(|_| positive);
        let entry = Arc::new(HostEntry {
            rules,
            expires_at,
            version,
            subscription,
            order,
            used: AtomicBool::new(false),
        });
        if let Some(id) = subscription {
            self.by_subscription.insert(id, key.clone());
        }
        if positive {
            self.positive_order.insert(order, key.clone());
        } else {
            self.negative_order.insert(order, key.clone());
        }
        self.expiry.insert((expires_at, version), key.clone());
        self.hosts_mut(key.kind).insert(key.host, entry);
        self.changed = true;
        while self.positive_order.len() > self.capacity {
            let oldest = self.oldest(true).expect("a rule set over capacity");
            displaced.take(self.detach(&oldest).expect("indexed host"), None);
        }
        while self.negative_order.len() > self.capacity {
            let oldest = self.oldest(false).expect("an empty answer over capacity");
            displaced.take(self.detach(&oldest).expect("indexed host"), None);
        }
        displaced
    }

    /// Removes the oldest `count` rule sets to make room.
    pub fn evict_oldest(&mut self, count: usize) -> Displaced {
        let mut displaced = Displaced::default();
        for _ in 0..count {
            let Some(oldest) = self.oldest(true) else {
                break;
            };
            displaced.take(self.detach(&oldest).expect("indexed host"), None);
        }
        displaced
    }

    pub fn remove(&mut self, key: &HostKey) -> Displaced {
        let mut displaced = Displaced::default();
        if let Some(entry) = self.detach(key) {
            displaced.take(entry, None);
        }
        displaced
    }

    /// Removes the answer a subscription keeps current. The control plane has
    /// already dropped the subscription, so nothing is left to release.
    pub fn remove_subscription(&mut self, subscription: SubscriptionId) -> Displaced {
        let mut displaced = Displaced::default();
        if let Some(key) = self.by_subscription.get(&subscription).cloned() {
            if let Some(entry) = self.detach(&key) {
                displaced.entries.push(entry);
            }
        }
        displaced
    }

    #[cfg(test)]
    pub fn next_expiry(&self) -> Option<Instant> {
        self.expiry.keys().next().map(|(expires_at, _)| *expires_at)
    }

    /// Removes up to `limit` answers whose lifetime has passed.
    pub fn expire(&mut self, now: Instant, limit: usize) -> Displaced {
        let mut displaced = Displaced::default();
        for _ in 0..limit {
            let Some(((expires_at, _), key)) = self.expiry.first_key_value() else {
                break;
            };
            if *expires_at > now {
                break;
            }
            let key = key.clone();
            displaced.take(self.detach(&key).expect("indexed host"), None);
        }
        displaced
    }

    /// The stream that kept every subscription current ended in order. Rule
    /// sets a request used stay cached until their own lifetime and are
    /// returned for registration on the next stream; the rest leave now.
    pub fn end_session(&mut self) -> (Vec<HostKey>, Displaced) {
        self.by_subscription.clear();
        let mut kept = Vec::new();
        let mut displaced = Displaced::default();
        let hosts: Vec<HostKey> = self.positive_order.values().cloned().collect();
        for key in hosts {
            let entry = self.get(&key).expect("indexed host").clone();
            if !entry.was_used() {
                displaced.entries.extend(self.detach(&key));
                continue;
            }
            let carried = Arc::new(HostEntry {
                rules: entry.rules.clone(),
                expires_at: entry.expires_at,
                version: entry.version,
                subscription: None,
                order: entry.order,
                used: AtomicBool::new(false),
            });
            self.hosts_mut(key.kind).insert(key.host.clone(), carried);
            kept.push(key);
        }
        self.changed = true;
        (kept, displaced)
    }

    /// Whether the host still holds a rule set that no live subscription
    /// keeps current.
    pub fn awaits_registration(&self, key: &HostKey, now: Instant) -> bool {
        self.get(key).is_some_and(|entry| {
            !entry.rules.is_empty() && entry.subscription.is_none() && !entry.is_expired(now)
        })
    }

    pub fn clear(&mut self) -> Displaced {
        let mut displaced = Displaced::default();
        displaced
            .entries
            .extend(self.snapshot.http.drain().map(|(_, entry)| entry));
        displaced
            .entries
            .extend(self.snapshot.sni.drain().map(|(_, entry)| entry));
        self.by_subscription.clear();
        self.positive_order.clear();
        self.negative_order.clear();
        self.expiry.clear();
        self.changed = true;
        displaced
    }

    fn oldest(&self, positive: bool) -> Option<HostKey> {
        let order = if positive {
            &self.positive_order
        } else {
            &self.negative_order
        };
        order.first_key_value().map(|(_, key)| key.clone())
    }

    fn detach(&mut self, key: &HostKey) -> Option<Arc<HostEntry>> {
        let entry = self.hosts_mut(key.kind).remove(&*key.host)?;
        if let Some(id) = entry.subscription {
            self.by_subscription.remove(&id);
        }
        if entry.rules.is_empty() {
            self.negative_order.remove(&entry.order);
        } else {
            self.positive_order.remove(&entry.order);
        }
        self.expiry.remove(&(entry.expires_at, entry.version));
        self.changed = true;
        Some(entry)
    }

    fn hosts_mut(&mut self, kind: RouteKind) -> &mut HashMap<Arc<str>, Arc<HostEntry>> {
        match kind {
            RouteKind::Http => &mut self.snapshot.http,
            RouteKind::Sni => &mut self.snapshot.sni,
        }
    }
}

#[cfg(test)]
mod tests;
