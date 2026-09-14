use std::{error::Error, fmt, time::Instant};

use sleepypods_api::{BackendGeneration, CachePolicy, Generation, RouteEntry, RouteIdentity};

use crate::{
    cache::{stale_route_entry, CacheInsertResult, StaleRouteEntry},
    PositiveCacheEntry, RouteCache,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId {
    value: String,
    // Local transport ownership is never sent over the wire. Including it in
    // equality and hashing prevents reused server IDs aliasing across sessions.
    session: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RouteRequestId(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxySubscribeInput {
    SubscribeRoute {
        request_id: RouteRequestId,
        identity: RouteIdentity,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscribeControlPlaneOutput {
    RouteResolved {
        request_id: RouteRequestId,
        subscription_id: SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
    },
    RouteMiss {
        request_id: RouteRequestId,
        request_identity: RouteIdentity,
        negative_cache_policy: CachePolicy,
    },
    RouteUpdated {
        subscription_id: SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
    },
    RouteInvalidated {
        subscription_id: SubscriptionId,
        reason: InvalidationReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidationReason {
    RouteRemoved,
    RouteChanged,
    InstanceChanged,
    BackendChanged,
    StreamClosed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyControlPlaneMessageOutcome {
    Resolved(CacheInsertResult),
    Miss(CacheInsertResult),
    Updated(ApplyUpdateOutcome),
    Invalidated { removed: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyUpdateOutcome {
    Replaced(CacheInsertResult),
    MissingSubscription,
    StaleInstanceGeneration {
        current: Generation,
        incoming: Generation,
    },
    StaleBackendGeneration {
        current: BackendGeneration,
        incoming: BackendGeneration,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsubscribeOutcome {
    Removed,
    AlreadyAbsent,
    SubscribeRequestIgnored,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptySubscriptionField {
    field: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionState {
    cache: RouteCache,
}

impl SubscriptionId {
    pub fn new(value: impl Into<String>) -> Result<Self, EmptySubscriptionField> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(EmptySubscriptionField {
                field: "subscription_id",
            });
        }

        Ok(Self {
            value,
            session: None,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub(crate) fn with_session(mut self, session: u64) -> Self {
        self.session = Some(session);
        self
    }

    pub(crate) fn session(&self) -> Option<u64> {
        self.session
    }
}

impl RouteRequestId {
    pub fn new(value: impl Into<String>) -> Result<Self, EmptySubscriptionField> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(EmptySubscriptionField {
                field: "request_id",
            });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl EmptySubscriptionField {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for EmptySubscriptionField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} must not be empty", self.field)
    }
}

impl Error for EmptySubscriptionField {}

impl SubscriptionState {
    pub fn new(cache_capacity: usize) -> Self {
        Self {
            cache: RouteCache::new(cache_capacity),
        }
    }

    pub fn from_cache(cache: RouteCache) -> Self {
        Self { cache }
    }

    pub fn cache(&self) -> &RouteCache {
        &self.cache
    }

    pub fn cache_mut(&mut self) -> &mut RouteCache {
        &mut self.cache
    }

    pub fn apply_proxy_input(&mut self, input: ProxySubscribeInput) -> UnsubscribeOutcome {
        match input {
            ProxySubscribeInput::SubscribeRoute { .. } => {
                UnsubscribeOutcome::SubscribeRequestIgnored
            }
            ProxySubscribeInput::Unsubscribe { subscription_id } => {
                if self.cache.invalidate_subscription(&subscription_id) {
                    UnsubscribeOutcome::Removed
                } else {
                    UnsubscribeOutcome::AlreadyAbsent
                }
            }
        }
    }

    pub fn apply_resolved_response(
        &mut self,
        request_identity: RouteIdentity,
        message: SubscribeControlPlaneOutput,
        now: Instant,
    ) -> ApplyControlPlaneMessageOutcome {
        match message {
            SubscribeControlPlaneOutput::RouteResolved {
                subscription_id,
                matched_identity,
                entry,
                cache_policy,
                ..
            } => {
                let positive = crate::PositiveCacheEntry::new(
                    subscription_id,
                    matched_identity,
                    entry,
                    cache_policy,
                    now,
                );
                ApplyControlPlaneMessageOutcome::Resolved(self.cache.insert_resolved(
                    request_identity,
                    positive,
                    now,
                ))
            }
            other => self.apply_control_plane_message(other, now),
        }
    }

    pub fn apply_control_plane_message(
        &mut self,
        message: SubscribeControlPlaneOutput,
        now: Instant,
    ) -> ApplyControlPlaneMessageOutcome {
        match message {
            SubscribeControlPlaneOutput::RouteResolved {
                subscription_id,
                matched_identity,
                entry,
                cache_policy,
                ..
            } => ApplyControlPlaneMessageOutcome::Resolved(self.cache.insert_positive(
                subscription_id,
                matched_identity,
                entry,
                cache_policy,
                now,
            )),
            SubscribeControlPlaneOutput::RouteMiss {
                request_identity,
                negative_cache_policy,
                ..
            } => ApplyControlPlaneMessageOutcome::Miss(self.cache.insert_negative(
                request_identity,
                negative_cache_policy,
                now,
            )),
            SubscribeControlPlaneOutput::RouteUpdated {
                subscription_id,
                matched_identity,
                entry,
                cache_policy,
            } => ApplyControlPlaneMessageOutcome::Updated(self.apply_update(
                &subscription_id,
                matched_identity,
                entry,
                cache_policy,
                now,
            )),
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id, ..
            } => ApplyControlPlaneMessageOutcome::Invalidated {
                removed: self.cache.invalidate_subscription(&subscription_id),
            },
        }
    }

    pub fn invalidate_active_subscriptions(
        &mut self,
        reason: InvalidationReason,
        now: Instant,
    ) -> Vec<ApplyControlPlaneMessageOutcome> {
        self.cache
            .active_subscription_ids()
            .into_iter()
            .map(|subscription_id| {
                self.apply_control_plane_message(
                    SubscribeControlPlaneOutput::RouteInvalidated {
                        subscription_id,
                        reason: reason.clone(),
                    },
                    now,
                )
            })
            .collect()
    }

    /// Install the ready backend a wake produced on the answer the wake was
    /// issued for. The answer is addressed by the identity it was cached for,
    /// because a wake can outlive the stream that issued its subscription, and
    /// it keeps whatever remains of the lifetime it already had.
    pub fn apply_ready_wake(
        &mut self,
        cached: &PositiveCacheEntry,
        ready: RouteEntry,
        now: Instant,
    ) -> ApplyUpdateOutcome {
        let identity = cached.request_identity.clone();
        let Some(current) = self.cache.positive(&identity) else {
            return ApplyUpdateOutcome::MissingSubscription;
        };
        if let Some(stale) = stale_route_entry(&current.entry, &ready) {
            return stale_update_outcome(stale);
        }
        let remaining = cached.expires_at().saturating_duration_since(now);
        self.cache.refresh_positive(
            &identity,
            cached.matched_identity.clone(),
            ready,
            CachePolicy::new(remaining),
            now,
        );
        ApplyUpdateOutcome::Replaced(CacheInsertResult::default())
    }

    fn apply_update(
        &mut self,
        subscription_id: &SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> ApplyUpdateOutcome {
        let Some(current) = self.cache.positive_by_subscription(subscription_id) else {
            return ApplyUpdateOutcome::MissingSubscription;
        };

        if let Some(stale) = stale_route_entry(&current.entry, &entry) {
            return stale_update_outcome(stale);
        }

        let result = self.cache.replace_subscription(
            subscription_id,
            matched_identity,
            entry,
            cache_policy,
            now,
        );
        ApplyUpdateOutcome::Replaced(result)
    }
}

fn stale_update_outcome(stale: StaleRouteEntry) -> ApplyUpdateOutcome {
    match stale {
        StaleRouteEntry::InstanceGeneration { current, incoming } => {
            ApplyUpdateOutcome::StaleInstanceGeneration { current, incoming }
        }
        StaleRouteEntry::BackendGeneration { current, incoming } => {
            ApplyUpdateOutcome::StaleBackendGeneration { current, incoming }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
