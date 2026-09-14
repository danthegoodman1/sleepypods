use crate::subscription::SubscriptionId;
use sleepypods_api::{BackendGeneration, CachePolicy, Generation, RouteEntry, RouteIdentity};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositiveCacheEntry {
    pub subscription_id: SubscriptionId,
    pub matched_identity: RouteIdentity,
    pub request_identity: RouteIdentity,
    pub entry: RouteEntry,
    expires_at: Instant,
    /// False once the stream that issued `subscription_id` has closed. The
    /// answer stays usable until its TTL, but it can no longer be invalidated
    /// and its ID must never be sent to the control plane again.
    registered: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegativeCacheEntry {
    pub request_identity: RouteIdentity,
    expires_at: Instant,
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

/// Exact-identity cache. Reads never alter eviction order: each budget uses FIFO
/// insertion order, with O(log n) removal/expiry and O(1) idle maintenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteCache {
    capacity: usize,
    positives: Vec<Arc<PositiveCacheEntry>>,
    by_subscription: HashMap<SubscriptionId, usize>,
    by_request: HashMap<RouteIdentity, SubscriptionId>,
    positive_order: BTreeMap<u64, SubscriptionId>,
    positive_sequence: HashMap<SubscriptionId, u64>,
    positive_expiry: BTreeMap<(Instant, u64), SubscriptionId>,
    negatives: HashMap<RouteIdentity, Arc<NegativeCacheEntry>>,
    negative_order: BTreeMap<u64, RouteIdentity>,
    negative_sequence: HashMap<RouteIdentity, u64>,
    negative_expiry: BTreeMap<(Instant, u64), RouteIdentity>,
    sequence: u64,
    local_sequence: u64,
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
            subscription_id,
            request_identity: matched_identity.clone(),
            matched_identity,
            entry,
            expires_at: now + cache_policy.ttl(),
            registered: true,
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
        self.registered
    }
}

impl NegativeCacheEntry {
    pub fn new(request_identity: RouteIdentity, cache_policy: CachePolicy, now: Instant) -> Self {
        Self {
            request_identity,
            expires_at: now + cache_policy.ttl(),
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
            positives: Vec::new(),
            by_subscription: HashMap::new(),
            by_request: HashMap::new(),
            positive_order: BTreeMap::new(),
            positive_sequence: HashMap::new(),
            positive_expiry: BTreeMap::new(),
            negatives: HashMap::new(),
            negative_order: BTreeMap::new(),
            negative_sequence: HashMap::new(),
            negative_expiry: BTreeMap::new(),
            sequence: 0,
            local_sequence: 0,
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
        if let Some(entry) = self
            .by_request
            .get(identity)
            .and_then(|id| self.positive_by_subscription(id))
        {
            if !entry.is_expired(now) {
                return CacheLookup::Hit(CacheLookupHit::Positive(entry.clone()));
            }
        }
        if let Some(entry) = self.negatives.get(identity) {
            return if entry.is_expired(now) {
                CacheLookup::Expired
            } else {
                CacheLookup::Hit(CacheLookupHit::Negative(entry.clone()))
            };
        }
        if self.by_request.contains_key(identity) {
            CacheLookup::Expired
        } else {
            CacheLookup::Absent
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
        if let Some(existing) = self
            .by_request
            .get(&request)
            .and_then(|id| self.positive_by_subscription(id))
            .cloned()
        {
            if stale_route_entry(&existing.entry, &positive.entry).is_some() {
                result
                    .subscriptions_to_unsubscribe
                    .push(positive.subscription_id);
                // A rotated answer gets one attempt to register again, so
                // rejecting it here leaves an answer no invalidation can reach.
                // Drop it and let the next request resolve from scratch.
                if !existing.registered {
                    self.remove_positive(&existing.subscription_id);
                }
                return result;
            }
            let removed = self.remove_positive(&existing.subscription_id);
            if existing.subscription_id != positive.subscription_id
                && removed.is_some_and(|removed| removed.registered)
            {
                result
                    .subscriptions_to_unsubscribe
                    .push(existing.subscription_id.clone());
            }
        }
        self.remove_positive(&positive.subscription_id);
        self.remove_negative(&request);
        positive.request_identity = request.clone();
        self.index_positive(request, positive);
        while self.positives.len() > self.capacity {
            let id = self.positive_order.first_key_value().unwrap().1.clone();
            if self
                .remove_positive(&id)
                .is_some_and(|removed| removed.registered)
            {
                result.subscriptions_to_unsubscribe.push(id);
            }
        }
        result
    }

    fn index_positive(&mut self, request: RouteIdentity, positive: PositiveCacheEntry) {
        let id = positive.subscription_id.clone();
        self.sequence += 1;
        let seq = self.sequence;
        self.by_subscription
            .insert(id.clone(), self.positives.len());
        self.by_request.insert(request, id.clone());
        self.positive_order.insert(seq, id.clone());
        self.positive_sequence.insert(id.clone(), seq);
        self.positive_expiry.insert((positive.expires_at, seq), id);
        self.positives.push(Arc::new(positive));
    }

    /// The stream backing every current subscription has closed. Cached answers
    /// stay valid until their own TTL, but their IDs are dead: the next stream
    /// restarts its numbering, so a retained ID would alias a stranger's live
    /// subscription in `by_subscription`, in an invalidation, or in an
    /// unsubscribe. Re-key each retained answer to an ID the control plane can
    /// never issue, and return what has to be registered again.
    pub fn rotate_session(&mut self) -> Vec<RouteIdentity> {
        let retained = std::mem::take(&mut self.positives);
        self.by_subscription.clear();
        self.by_request.clear();
        self.positive_order.clear();
        self.positive_sequence.clear();
        self.positive_expiry.clear();
        let mut identities = Vec::with_capacity(retained.len());
        for entry in retained {
            let mut entry = Arc::try_unwrap(entry).unwrap_or_else(|shared| (*shared).clone());
            self.local_sequence += 1;
            let Ok(local) = SubscriptionId::new(format!("local:{}", self.local_sequence)) else {
                continue;
            };
            entry.subscription_id = local;
            entry.registered = false;
            let request = entry.request_identity.clone();
            identities.push(request.clone());
            self.index_positive(request, entry);
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
        if let Some(id) = self.by_request.get(&request).cloned() {
            if self
                .remove_positive(&id)
                .is_some_and(|removed| removed.registered)
            {
                result.subscriptions_to_unsubscribe.push(id);
            }
        }
        self.remove_negative(&request);
        self.sequence += 1;
        let seq = self.sequence;
        let entry = Arc::new(NegativeCacheEntry::new(request.clone(), policy, now));
        self.negative_order.insert(seq, request.clone());
        self.negative_sequence.insert(request.clone(), seq);
        self.negative_expiry
            .insert((entry.expires_at, seq), request.clone());
        self.negatives.insert(request, entry);
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
        self.by_request
            .get(identity)
            .and_then(|id| self.positive_by_subscription(id))
            .is_some_and(|entry| !entry.registered && !entry.is_expired(now))
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
        while let Some(((expires, _), id)) = self.positive_expiry.first_key_value() {
            if *expires > now || remaining == 0 {
                break;
            }
            remaining -= 1;
            let id = id.clone();
            if self
                .remove_positive(&id)
                .is_some_and(|removed| removed.registered)
            {
                result.subscriptions_to_unsubscribe.push(id);
            }
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
        self.remove_positive(id).is_some()
    }
    pub fn active_subscription_ids(&self) -> Vec<SubscriptionId> {
        self.positives
            .iter()
            .map(|entry| entry.subscription_id.clone())
            .collect()
    }
    pub fn replace_subscription(
        &mut self,
        id: &SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        if let Some(&index) = self.by_subscription.get(id) {
            let seq = self.positive_sequence[id];
            let old = &self.positives[index];
            self.positive_expiry.remove(&(old.expires_at, seq));
            let updated = Arc::make_mut(&mut self.positives[index]);
            updated.matched_identity = matched_identity;
            updated.entry = entry;
            updated.expires_at = now + policy.ttl();
            self.positive_expiry
                .insert((updated.expires_at, seq), id.clone());
        }
        CacheInsertResult::default()
    }
    pub fn positive_by_subscription(
        &self,
        id: &SubscriptionId,
    ) -> Option<&Arc<PositiveCacheEntry>> {
        self.by_subscription
            .get(id)
            .and_then(|index| self.positives.get(*index))
    }
    pub fn positives(&self) -> &[Arc<PositiveCacheEntry>] {
        &self.positives
    }
    pub fn negatives(&self) -> Vec<Arc<NegativeCacheEntry>> {
        self.negatives.values().cloned().collect()
    }
    pub fn clear(&mut self) {
        *self = Self::new(self.capacity);
    }
    fn remove_positive(&mut self, id: &SubscriptionId) -> Option<Arc<PositiveCacheEntry>> {
        let index = self.by_subscription.remove(id)?;
        let removed = self.positives.swap_remove(index);
        self.by_request.remove(&removed.request_identity);
        let seq = self.positive_sequence.remove(id).unwrap();
        self.positive_order.remove(&seq);
        self.positive_expiry.remove(&(removed.expires_at, seq));
        if let Some(moved) = self.positives.get(index) {
            self.by_subscription
                .insert(moved.subscription_id.clone(), index);
        }
        Some(removed)
    }
    fn remove_negative(&mut self, request: &RouteIdentity) -> Option<Arc<NegativeCacheEntry>> {
        let removed = self.negatives.remove(request)?;
        let seq = self.negative_sequence.remove(request).unwrap();
        self.negative_order.remove(&seq);
        self.negative_expiry.remove(&(removed.expires_at, seq));
        Some(removed)
    }
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
