use crate::subscription::SubscriptionId;
use sleepypods_api::{BackendGeneration, CachePolicy, Generation, RouteEntry, RouteIdentity};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositiveCacheEntry {
    /// `None` once the stream that issued this answer ended. The answer stays
    /// usable until its TTL, but nothing can invalidate it, and it has no ID the
    /// control plane would recognise.
    pub subscription_id: Option<SubscriptionId>,
    pub matched_identity: RouteIdentity,
    pub request_identity: RouteIdentity,
    pub entry: RouteEntry,
    expires_at: Instant,
    sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegativeCacheEntry {
    pub request_identity: RouteIdentity,
    expires_at: Instant,
    sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheLookup {
    Hit(CacheLookupHit),
    Expired,
    Absent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheLookupHit {
    Positive(Arc<PositiveCacheEntry>),
    Negative(Arc<NegativeCacheEntry>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheLookupStatus {
    PositiveHit,
    NegativeHit,
    Expired,
    Absent,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheInsertResult {
    pub subscriptions_to_unsubscribe: Vec<SubscriptionId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StaleRouteEntry {
    InstanceGeneration {
        current: Generation,
        incoming: Generation,
    },
    BackendGeneration {
        current: BackendGeneration,
        incoming: BackendGeneration,
    },
}

/// One stored copy of an identity, shared by every index that points at it, so
/// indexing an answer costs a refcount rather than another string.
type CacheKey = Arc<RouteIdentity>;

/// Exact-identity cache. Both halves are keyed by the identity the caller asked
/// for, so a lookup is one hash probe however many routes are cached. Reads
/// never alter eviction order: each budget uses FIFO insertion order, with
/// O(log n) removal/expiry and O(1) idle maintenance. `by_subscription` holds
/// only answers a live stream still backs, so a control-plane ID reaches the
/// right entry and a dead one reaches nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteCache {
    capacity: usize,
    positives: HashMap<CacheKey, Arc<PositiveCacheEntry>>,
    by_subscription: HashMap<SubscriptionId, CacheKey>,
    positive_order: BTreeMap<u64, CacheKey>,
    positive_expiry: BTreeMap<(Instant, u64), CacheKey>,
    negatives: HashMap<CacheKey, Arc<NegativeCacheEntry>>,
    negative_order: BTreeMap<u64, CacheKey>,
    negative_expiry: BTreeMap<(Instant, u64), CacheKey>,
    sequence: u64,
}

impl PositiveCacheEntry {
    pub fn new(
        subscription_id: SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> Self {
        Self {
            subscription_id: Some(subscription_id),
            request_identity: matched_identity.clone(),
            matched_identity,
            entry,
            expires_at: now + cache_policy.ttl(),
            sequence: 0,
        }
    }

    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }

    /// Whether this answer is still backed by a live control-plane subscription.
    pub fn is_registered(&self) -> bool {
        self.subscription_id.is_some()
    }
}

impl NegativeCacheEntry {
    pub fn new(request_identity: RouteIdentity, cache_policy: CachePolicy, now: Instant) -> Self {
        Self {
            request_identity,
            expires_at: now + cache_policy.ttl(),
            sequence: 0,
        }
    }

    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

impl CacheLookup {
    pub fn status(&self) -> CacheLookupStatus {
        match self {
            Self::Hit(CacheLookupHit::Positive(_)) => CacheLookupStatus::PositiveHit,
            Self::Hit(CacheLookupHit::Negative(_)) => CacheLookupStatus::NegativeHit,
            Self::Expired => CacheLookupStatus::Expired,
            Self::Absent => CacheLookupStatus::Absent,
        }
    }
}

impl RouteCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            positives: HashMap::new(),
            by_subscription: HashMap::new(),
            positive_order: BTreeMap::new(),
            positive_expiry: BTreeMap::new(),
            negatives: HashMap::new(),
            negative_order: BTreeMap::new(),
            negative_expiry: BTreeMap::new(),
            sequence: 0,
        }
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn len(&self) -> usize {
        self.positives.len() + self.negatives.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn lookup(&self, identity: &RouteIdentity, now: Instant) -> CacheLookup {
        if let Some(entry) = self.positives.get(identity) {
            return if entry.is_expired(now) {
                CacheLookup::Expired
            } else {
                CacheLookup::Hit(CacheLookupHit::Positive(entry.clone()))
            };
        }
        match self.negatives.get(identity) {
            Some(entry) if entry.is_expired(now) => CacheLookup::Expired,
            Some(entry) => CacheLookup::Hit(CacheLookupHit::Negative(entry.clone())),
            None => CacheLookup::Absent,
        }
    }
    /// Install an exact identity, for callers that already have a complete answer.
    pub fn insert_positive(
        &mut self,
        subscription_id: SubscriptionId,
        identity: RouteIdentity,
        entry: RouteEntry,
        policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        self.insert_resolved(
            identity.clone(),
            PositiveCacheEntry::new(subscription_id, identity, entry, policy, now),
            now,
        )
    }
    /// Only the queried identity is authoritative; the matching rule is metadata.
    pub fn insert_resolved(
        &mut self,
        request: RouteIdentity,
        mut positive: PositiveCacheEntry,
        now: Instant,
    ) -> CacheInsertResult {
        let mut result = self.expire_limited(now, 64);
        if let Some(existing) = self.positives.get(&request).cloned() {
            if stale_route_entry(&existing.entry, &positive.entry).is_some() {
                result
                    .subscriptions_to_unsubscribe
                    .extend(positive.subscription_id);
                // A rotated answer gets one attempt to register again, so
                // rejecting it here leaves an answer no invalidation can reach.
                // Drop it and let the next request resolve from scratch.
                if existing.subscription_id.is_none() {
                    self.remove_positive(&request);
                }
                return result;
            }
        }
        // Release what this identity held, unless the replacement arrived on the
        // same subscription.
        if let Some(released) = self.remove_positive(&request).and_then(released_id) {
            if Some(&released) != positive.subscription_id.as_ref() {
                result.subscriptions_to_unsubscribe.push(released);
            }
        }
        if let Some(id) = positive.subscription_id.clone() {
            self.remove_positive_by_subscription(&id);
        }
        self.remove_negative(&request);
        positive.request_identity = request.clone();
        self.index_positive(request, positive);
        while self.positives.len() > self.capacity {
            let identity = self.oldest_positive().expect("a positive over capacity");
            result
                .subscriptions_to_unsubscribe
                .extend(self.remove_positive(&identity).and_then(released_id));
        }
        result
    }

    fn index_positive(&mut self, request: RouteIdentity, mut positive: PositiveCacheEntry) {
        self.sequence += 1;
        positive.sequence = self.sequence;
        let key: CacheKey = Arc::new(request);
        if let Some(id) = &positive.subscription_id {
            self.by_subscription.insert(id.clone(), key.clone());
        }
        self.positive_order.insert(positive.sequence, key.clone());
        self.positive_expiry
            .insert((positive.expires_at, positive.sequence), key.clone());
        self.positives.insert(key, Arc::new(positive));
    }

    /// The stream backing every current subscription has ended. Cached answers
    /// stay valid until their own TTL, but their IDs are dead: the next stream
    /// restarts its numbering, so a retained ID would alias a stranger's live
    /// subscription. Drop the IDs and report what has to be registered again.
    pub fn rotate_session(&mut self) -> Vec<RouteIdentity> {
        self.by_subscription.clear();
        let mut identities = Vec::with_capacity(self.positives.len());
        for (key, entry) in &mut self.positives {
            Arc::make_mut(entry).subscription_id = None;
            identities.push((**key).clone());
        }
        identities
    }
    pub fn insert_negative(
        &mut self,
        request: RouteIdentity,
        policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        let mut result = self.expire_limited(now, 64);
        result
            .subscriptions_to_unsubscribe
            .extend(self.remove_positive(&request).and_then(released_id));
        self.remove_negative(&request);
        self.sequence += 1;
        let mut entry = NegativeCacheEntry::new(request, policy, now);
        entry.sequence = self.sequence;
        let key: CacheKey = Arc::new(entry.request_identity.clone());
        self.negative_order.insert(entry.sequence, key.clone());
        self.negative_expiry
            .insert((entry.expires_at, entry.sequence), key.clone());
        self.negatives.insert(key, Arc::new(entry));
        while self.negatives.len() > self.capacity {
            let key = self.negative_order.first_key_value().unwrap().1.clone();
            self.remove_negative(&key);
        }
        result
    }
    /// Whether a rotated answer for this identity is still worth registering
    /// again: re-registration is pointless once it has been replaced, expired,
    /// or already re-registered by a live request.
    pub fn needs_reregistration(&self, identity: &RouteIdentity, now: Instant) -> bool {
        self.positives
            .get(identity)
            .is_some_and(|entry| entry.subscription_id.is_none() && !entry.is_expired(now))
    }
    /// True when `expire_limited` would evict something. Lets a caller skip
    /// taking the write lock on an idle maintenance pass.
    pub fn has_expired(&self, now: Instant) -> bool {
        self.positive_expiry
            .first_key_value()
            .is_some_and(|((expires, _), _)| *expires <= now)
            || self
                .negative_expiry
                .first_key_value()
                .is_some_and(|((expires, _), _)| *expires <= now)
    }
    pub fn expire(&mut self, now: Instant) -> CacheInsertResult {
        self.expire_limited(now, usize::MAX)
    }

    pub fn expire_limited(&mut self, now: Instant, limit: usize) -> CacheInsertResult {
        let mut remaining = limit;
        let mut result = CacheInsertResult::default();
        while let Some(((expires, _), identity)) = self.positive_expiry.first_key_value() {
            if *expires > now || remaining == 0 {
                break;
            }
            remaining -= 1;
            let identity = identity.clone();
            result
                .subscriptions_to_unsubscribe
                .extend(self.remove_positive(&identity).and_then(released_id));
        }
        while let Some(((expires, _), request)) = self.negative_expiry.first_key_value() {
            if *expires > now || remaining == 0 {
                break;
            }
            remaining -= 1;
            let request = request.clone();
            self.remove_negative(&request);
        }
        result
    }
    pub fn invalidate_subscription(&mut self, id: &SubscriptionId) -> bool {
        self.remove_positive_by_subscription(id).is_some()
    }
    /// Drop the answer cached for this identity and report the subscription it
    /// held, for callers that address an answer without knowing whether a live
    /// stream still backs it.
    pub fn invalidate_request(&mut self, identity: &RouteIdentity) -> Option<SubscriptionId> {
        self.remove_positive(identity).and_then(released_id)
    }
    pub fn active_subscription_ids(&self) -> Vec<SubscriptionId> {
        self.by_subscription.keys().cloned().collect()
    }
    pub fn replace_subscription(
        &mut self,
        id: &SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        if let Some(identity) = self.by_subscription.get(id).cloned() {
            self.refresh_positive(&identity, matched_identity, entry, policy, now);
        }
        CacheInsertResult::default()
    }
    /// Update the answer cached for this identity in place, keeping its FIFO
    /// position and whatever subscription it holds.
    pub fn refresh_positive(
        &mut self,
        identity: &RouteIdentity,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        policy: CachePolicy,
        now: Instant,
    ) {
        let Some((key, cached)) = self.positives.get_key_value(identity) else {
            return;
        };
        let (key, previous, seq) = (key.clone(), cached.expires_at, cached.sequence);
        let updated = Arc::make_mut(self.positives.get_mut(identity).expect("just observed"));
        updated.matched_identity = matched_identity;
        updated.entry = entry;
        updated.expires_at = now + policy.ttl();
        let expires_at = updated.expires_at;
        self.positive_expiry.remove(&(previous, seq));
        self.positive_expiry.insert((expires_at, seq), key);
    }
    /// The answer cached for an identity, whether or not a live stream backs it.
    pub fn positive(&self, identity: &RouteIdentity) -> Option<&Arc<PositiveCacheEntry>> {
        self.positives.get(identity)
    }
    pub fn positive_by_subscription(
        &self,
        id: &SubscriptionId,
    ) -> Option<&Arc<PositiveCacheEntry>> {
        self.by_subscription
            .get(id)
            .and_then(|identity| self.positives.get(identity))
    }
    pub fn positives(&self) -> impl Iterator<Item = &Arc<PositiveCacheEntry>> {
        self.positives.values()
    }
    pub fn negatives(&self) -> Vec<Arc<NegativeCacheEntry>> {
        self.negatives.values().cloned().collect()
    }
    pub fn clear(&mut self) {
        *self = Self::new(self.capacity);
    }
    fn oldest_positive(&self) -> Option<CacheKey> {
        self.positive_order
            .first_key_value()
            .map(|(_, key)| key.clone())
    }
    fn remove_positive(&mut self, identity: &RouteIdentity) -> Option<Arc<PositiveCacheEntry>> {
        let removed = self.positives.remove(identity)?;
        if let Some(id) = &removed.subscription_id {
            self.by_subscription.remove(id);
        }
        self.positive_order.remove(&removed.sequence);
        self.positive_expiry
            .remove(&(removed.expires_at, removed.sequence));
        Some(removed)
    }
    fn remove_positive_by_subscription(
        &mut self,
        id: &SubscriptionId,
    ) -> Option<Arc<PositiveCacheEntry>> {
        let identity = self.by_subscription.get(id)?.clone();
        self.remove_positive(&identity)
    }
    fn remove_negative(&mut self, request: &RouteIdentity) -> Option<Arc<NegativeCacheEntry>> {
        let removed = self.negatives.remove(request)?;
        self.negative_order.remove(&removed.sequence);
        self.negative_expiry
            .remove(&(removed.expires_at, removed.sequence));
        Some(removed)
    }
}

/// The ID a removed answer held, which is what its owner has to release. A
/// rotated answer holds none, and unsubscribing a dead ID would reach whatever
/// the next stream gave that number to.
fn released_id(removed: Arc<PositiveCacheEntry>) -> Option<SubscriptionId> {
    removed.subscription_id.clone()
}

pub(crate) fn stale_route_entry(
    current: &RouteEntry,
    incoming: &RouteEntry,
) -> Option<StaleRouteEntry> {
    if current.instance_id != incoming.instance_id {
        return None;
    }

    // Instance and backend generations are only ordered within one instance
    // lineage. A route reassignment to a different instance can legitimately
    // restart generation numbers at a lower value.
    if incoming.instance_generation < current.instance_generation {
        return Some(StaleRouteEntry::InstanceGeneration {
            current: current.instance_generation,
            incoming: incoming.instance_generation,
        });
    }

    if let (Some(current), Some(incoming)) =
        (current.backend_generation, incoming.backend_generation)
    {
        if incoming < current {
            return Some(StaleRouteEntry::BackendGeneration { current, incoming });
        }
    }

    None
}

#[cfg(test)]
mod tests;
