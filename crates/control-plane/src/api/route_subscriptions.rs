//! One proxy Subscribe stream: its host subscriptions, their lookups and the
//! rule sets it pushes.
//!
//! A subscription names one exact host. The stream answers it with the host's
//! complete rule set and sends the set again whenever a binding or instance it
//! depends on changes. A failed lookup ends that subscription alone. The stream
//! itself ends in order at its lifetime or at shutdown, and with an error
//! status when route-change events were lost or the proxy stopped reading.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::sync::{broadcast, mpsc};
use tonic::{
    codegen::tokio_stream::{self, wrappers::ReceiverStream},
    Response, Status,
};

use super::{
    admission::{ApiLimits, SubscriptionLease},
    pb,
    route_events::{RouteBindingChange, RouteSubscriptionBroker},
};
use crate::{
    ids::{InstanceId, RouteBindingId},
    materialization::MaterializationTarget,
    route::{
        CachePolicy, HostRules, ResolveHostRulesRequest, RouteHost, RouteIdentity, RouteKind,
        MAX_ROUTE_HOST_BYTES,
    },
    store::{ControlPlaneStore, StoreError},
};

pub(super) type SubscribeStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<pb::ProxySubscribeResponse, Status>>
            + Send
            + 'static,
    >,
>;

const RESPONSE_BUFFER: usize = 64;
/// Lookups one stream runs at once. The rest wait their turn.
const CONCURRENT_LOOKUPS: usize = 16;
/// Events kept to compare against a lookup that was in flight when they
/// arrived. A lookup that outlives this window is repeated.
const RECENT_EVENTS: usize = 256;

pub(super) fn open(
    store: Arc<dyn ControlPlaneStore>,
    target: MaterializationTarget,
    broker: &RouteSubscriptionBroker,
    requests: tonic::Streaming<pb::ProxySubscribeRequest>,
) -> Result<Response<SubscribeStream>, Status> {
    let permit = broker
        .streams
        .clone()
        .try_acquire_owned()
        .map_err(|_| Status::resource_exhausted("subscription stream capacity exhausted"))?;
    let permit = Arc::new(permit);
    // Events queue from here on, so a change that commits during a lookup is
    // still waiting when the lookup's result is installed.
    let events = broker.subscribe();
    let (responses, response_stream) = mpsc::channel(RESPONSE_BUFFER);
    let stream = HostSubscriptions {
        store,
        target,
        limits: broker.limits.clone(),
        responses,
        subscriptions: HashMap::new(),
        http_hosts: HashMap::new(),
        sni_hosts: HashMap::new(),
        by_instance: HashMap::new(),
        by_binding: HashMap::new(),
        waiting: VecDeque::new(),
        lookups: tokio::task::JoinSet::new(),
        recent: VecDeque::new(),
        events_seen: 0,
    };
    let producer_permit = permit.clone();
    // The tonic service value may be dropped once it returns response headers.
    let producer_broker = broker.clone();
    let task = tokio::spawn(async move {
        let _producer_permit = producer_permit;
        let cancellation = producer_broker.cancellation.clone();
        stream.run(requests, events, cancellation).await
    });
    let mut response: Response<SubscribeStream> =
        Response::new(Box::pin(OwnedSubscriptionStream {
            receiver: ReceiverStream::new(response_stream),
            task: Some(task),
            _permit: permit.clone(),
        }));
    // The stream ends itself at its lifetime. The lease cancels the connection
    // of a proxy that has still not read that end one response timeout later.
    response.extensions_mut().insert(SubscriptionLease {
        permit,
        lifetime: broker.limits.subscription_lifetime + broker.limits.response_timeout,
    });
    Ok(response)
}

struct HostSubscription {
    kind: RouteKind,
    host: RouteHost,
    bindings: Vec<RouteBindingId>,
    instances: Vec<InstanceId>,
    lookup: Lookup,
}

enum Lookup {
    /// Waiting for a lookup slot.
    Queued,
    /// In flight. `since` is the event count when it began; `again` records a
    /// dependency event that arrived meanwhile.
    Running { since: u64, again: bool },
    /// The proxy holds the current rule set.
    Idle,
}

struct HostSubscriptions {
    store: Arc<dyn ControlPlaneStore>,
    target: MaterializationTarget,
    limits: ApiLimits,
    responses: mpsc::Sender<Result<pb::ProxySubscribeResponse, Status>>,
    subscriptions: HashMap<u64, HostSubscription>,
    http_hosts: HashMap<String, HashSet<u64>>,
    sni_hosts: HashMap<String, HashSet<u64>>,
    by_instance: HashMap<InstanceId, HashSet<u64>>,
    by_binding: HashMap<RouteBindingId, HashSet<u64>>,
    waiting: VecDeque<u64>,
    lookups: tokio::task::JoinSet<(u64, Result<HostRules, StoreError>)>,
    recent: VecDeque<(u64, RouteBindingChange)>,
    events_seen: u64,
}

impl HostSubscriptions {
    async fn run(
        mut self,
        mut requests: tonic::Streaming<pb::ProxySubscribeRequest>,
        mut events: broadcast::Receiver<RouteBindingChange>,
        cancellation: crate::runtime_work::Cancellation,
    ) -> Result<(), Status> {
        let expiry = tokio::time::sleep(self.limits.subscription_lifetime);
        tokio::pin!(expiry);
        loop {
            self.start_lookups();
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = self.responses.closed() => return Ok(()),
                _ = &mut expiry => return Ok(()),
                request = requests.message() => match request {
                    Ok(Some(request)) => self.handle_request(request).await?,
                    _ => return Ok(()),
                },
                event = events.recv() => match event {
                    Ok(RouteBindingChange::Reset) | Err(broadcast::error::RecvError::Lagged(_)) => {
                        return Err(Status::data_loss("route change events were lost"));
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    Ok(event) => self.handle_event(event),
                },
                Some(finished) = self.lookups.join_next(), if !self.lookups.is_empty() => {
                    let (id, result) =
                        finished.map_err(|_| Status::internal("route lookup task failed"))?;
                    self.finish_lookup(id, result).await?;
                }
            }
        }
    }

    async fn handle_request(&mut self, request: pb::ProxySubscribeRequest) -> Result<(), Status> {
        match request.input {
            Some(pb::proxy_subscribe_request::Input::Subscribe(request)) => {
                self.subscribe(request).await
            }
            Some(pb::proxy_subscribe_request::Input::Unsubscribe(request)) => {
                self.remove(request.subscription_id);
                Ok(())
            }
            None => Err(Status::invalid_argument(
                "subscribe request input is required",
            )),
        }
    }

    async fn subscribe(&mut self, request: pb::ProxySubscribeHostRequest) -> Result<(), Status> {
        let id = request.subscription_id;
        let (kind, host) = match subscription_key(&request) {
            Ok(key) => key,
            Err(message) => {
                return self
                    .reject(id, pb::ProxySubscribeErrorCode::InvalidArgument, message)
                    .await
            }
        };
        if self.subscriptions.contains_key(&id) {
            return self
                .reject(
                    id,
                    pb::ProxySubscribeErrorCode::InvalidArgument,
                    "subscription_id is already in use",
                )
                .await;
        }
        if self.subscriptions.len() >= self.limits.subscriptions_per_stream {
            return self
                .reject(
                    id,
                    pb::ProxySubscribeErrorCode::ResourceExhausted,
                    "subscription capacity exhausted",
                )
                .await;
        }
        self.hosts_mut(kind)
            .entry(host.as_str().to_owned())
            .or_default()
            .insert(id);
        self.subscriptions.insert(
            id,
            HostSubscription {
                kind,
                host,
                bindings: Vec::new(),
                instances: Vec::new(),
                lookup: Lookup::Queued,
            },
        );
        self.waiting.push_back(id);
        Ok(())
    }

    fn start_lookups(&mut self) {
        while self.lookups.len() < CONCURRENT_LOOKUPS {
            let Some(id) = self.waiting.pop_front() else {
                return;
            };
            let Some(subscription) = self.subscriptions.get_mut(&id) else {
                continue;
            };
            if !matches!(subscription.lookup, Lookup::Queued) {
                continue;
            }
            subscription.lookup = Lookup::Running {
                since: self.events_seen,
                again: false,
            };
            let request = ResolveHostRulesRequest::new(
                subscription.kind,
                subscription.host.clone(),
                self.target.clone(),
            );
            let store = self.store.clone();
            let deadline = self.limits.lookup_timeout;
            self.lookups.spawn(async move {
                let result = tokio::time::timeout(deadline, store.resolve_host_rules(request))
                    .await
                    .unwrap_or_else(|_| Err(StoreError::unavailable("route lookup timed out")));
                (id, result)
            });
        }
    }

    fn handle_event(&mut self, event: RouteBindingChange) {
        let affected: Vec<u64> = match &event {
            RouteBindingChange::Instance(instance_id) => self
                .by_instance
                .get(instance_id)
                .into_iter()
                .flatten()
                .copied()
                .collect(),
            RouteBindingChange::Route {
                route_binding_id,
                identity,
            } => {
                let mut affected: HashSet<u64> = self
                    .by_binding
                    .get(route_binding_id)
                    .cloned()
                    .unwrap_or_default();
                if identity.host().is_wildcard() {
                    affected.extend(
                        self.subscriptions
                            .iter()
                            .filter(|(_, subscription)| {
                                identity.applies_to(subscription.kind, subscription.host.as_str())
                            })
                            .map(|(id, _)| *id),
                    );
                } else if let Some(ids) = self.hosts(identity.kind()).get(identity.host().as_str())
                {
                    affected.extend(ids);
                }
                affected.into_iter().collect()
            }
            RouteBindingChange::Reset => Vec::new(),
        };
        self.events_seen += 1;
        self.recent.push_back((self.events_seen, event));
        if self.recent.len() > RECENT_EVENTS {
            self.recent.pop_front();
        }
        for id in affected {
            let Some(subscription) = self.subscriptions.get_mut(&id) else {
                continue;
            };
            match &mut subscription.lookup {
                Lookup::Idle => {
                    subscription.lookup = Lookup::Queued;
                    self.waiting.push_back(id);
                }
                Lookup::Running { again, .. } => *again = true,
                Lookup::Queued => {}
            }
        }
    }

    async fn finish_lookup(
        &mut self,
        id: u64,
        result: Result<HostRules, StoreError>,
    ) -> Result<(), Status> {
        let Some(subscription) = self.subscriptions.get_mut(&id) else {
            return Ok(());
        };
        let Lookup::Running { since, again } =
            std::mem::replace(&mut subscription.lookup, Lookup::Idle)
        else {
            return Ok(());
        };
        match result {
            Err(error) => {
                self.remove(id);
                let status = super::store_error_to_status(error);
                let code = match status.code() {
                    tonic::Code::InvalidArgument => pb::ProxySubscribeErrorCode::InvalidArgument,
                    _ => pb::ProxySubscribeErrorCode::Unavailable,
                };
                self.reject(id, code, status.message()).await
            }
            Ok(rules) if rules.is_empty() => {
                self.remove(id);
                let ttl = self.limits.negative_route_cache_ttl;
                self.send(rules_response(id, rules, ttl)).await
            }
            Ok(rules) => {
                self.index_dependencies(id, &rules);
                // The lookup read a snapshot taken after it began. An event
                // that arrived since may describe a later commit, so the
                // subscription is looked up once more and the newer set follows.
                if again || self.changed_since(since, id) {
                    if let Some(subscription) = self.subscriptions.get_mut(&id) {
                        subscription.lookup = Lookup::Queued;
                        self.waiting.push_back(id);
                    }
                }
                let ttl = self.limits.positive_route_cache_ttl;
                let response = rules_response(id, rules, ttl);
                if prost::Message::encoded_len(&response) > super::proxy::MAX_PROXY_RESPONSE_BYTES {
                    self.remove(id);
                    return self
                        .reject(
                            id,
                            pb::ProxySubscribeErrorCode::InvalidArgument,
                            "the host's rule set is too large to send",
                        )
                        .await;
                }
                self.send(response).await
            }
        }
    }

    /// Whether an event since `since` touches what the subscription depends on.
    fn changed_since(&self, since: u64, id: u64) -> bool {
        let Some(subscription) = self.subscriptions.get(&id) else {
            return false;
        };
        let oldest_kept = self
            .recent
            .front()
            .map_or(self.events_seen, |(seq, _)| *seq);
        if self.events_seen > since && oldest_kept > since + 1 {
            return true;
        }
        self.recent
            .iter()
            .filter(|(seq, _)| *seq > since)
            .any(|(_, event)| match event {
                RouteBindingChange::Instance(instance_id) => {
                    subscription.instances.contains(instance_id)
                }
                RouteBindingChange::Route {
                    route_binding_id,
                    identity,
                } => {
                    subscription.bindings.contains(route_binding_id)
                        || identity.applies_to(subscription.kind, subscription.host.as_str())
                }
                RouteBindingChange::Reset => true,
            })
    }

    fn index_dependencies(&mut self, id: u64, rules: &HostRules) {
        self.unindex_dependencies(id);
        let Some(subscription) = self.subscriptions.get_mut(&id) else {
            return;
        };
        for rule in rules.rules() {
            let binding = &rule.entry.route_binding_id;
            let instance = &rule.entry.instance_id;
            if !subscription.bindings.contains(binding) {
                subscription.bindings.push(binding.clone());
            }
            if !subscription.instances.contains(instance) {
                subscription.instances.push(instance.clone());
            }
            self.by_binding
                .entry(binding.clone())
                .or_default()
                .insert(id);
            self.by_instance
                .entry(instance.clone())
                .or_default()
                .insert(id);
        }
    }

    fn unindex_dependencies(&mut self, id: u64) {
        let Some(subscription) = self.subscriptions.get_mut(&id) else {
            return;
        };
        for binding in subscription.bindings.drain(..) {
            if let Some(ids) = self.by_binding.get_mut(&binding) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.by_binding.remove(&binding);
                }
            }
        }
        for instance in subscription.instances.drain(..) {
            if let Some(ids) = self.by_instance.get_mut(&instance) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.by_instance.remove(&instance);
                }
            }
        }
    }

    fn remove(&mut self, id: u64) {
        self.unindex_dependencies(id);
        let Some(subscription) = self.subscriptions.remove(&id) else {
            return;
        };
        let hosts = self.hosts_mut(subscription.kind);
        if let Some(ids) = hosts.get_mut(subscription.host.as_str()) {
            ids.remove(&id);
            if ids.is_empty() {
                hosts.remove(subscription.host.as_str());
            }
        }
    }

    fn hosts(&self, kind: RouteKind) -> &HashMap<String, HashSet<u64>> {
        match kind {
            RouteKind::Http => &self.http_hosts,
            RouteKind::Sni => &self.sni_hosts,
        }
    }

    fn hosts_mut(&mut self, kind: RouteKind) -> &mut HashMap<String, HashSet<u64>> {
        match kind {
            RouteKind::Http => &mut self.http_hosts,
            RouteKind::Sni => &mut self.sni_hosts,
        }
    }

    async fn reject(
        &self,
        id: u64,
        code: pb::ProxySubscribeErrorCode,
        message: &str,
    ) -> Result<(), Status> {
        self.send(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::Error(
                pb::ProxySubscribeErrorResponse {
                    subscription_id: id,
                    code: code as i32,
                    message: message.to_owned(),
                },
            )),
        })
        .await
    }

    /// Queues one message for the proxy. A proxy that leaves the queue full
    /// for the response timeout has stopped reading, and the stream ends.
    async fn send(&self, response: pb::ProxySubscribeResponse) -> Result<(), Status> {
        match tokio::time::timeout(
            self.limits.response_timeout,
            self.responses.send(Ok(response)),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Status::cancelled("the proxy closed the stream")),
            Err(_) => Err(Status::deadline_exceeded(
                "the proxy did not accept route messages",
            )),
        }
    }
}

fn subscription_key(
    request: &pb::ProxySubscribeHostRequest,
) -> Result<(RouteKind, RouteHost), &'static str> {
    let kind = match pb::RouteKind::try_from(request.kind) {
        Ok(pb::RouteKind::Http) => RouteKind::Http,
        Ok(pb::RouteKind::Sni) => RouteKind::Sni,
        _ => return Err("route kind is required"),
    };
    if request.host.len() > MAX_ROUTE_HOST_BYTES {
        return Err("host exceeds the supported length");
    }
    let host = RouteHost::exact(&request.host).map_err(|_| "host is invalid")?;
    if host.as_str() != request.host {
        return Err("host must be canonical");
    }
    Ok((kind, host))
}

fn rules_response(
    id: u64,
    rules: HostRules,
    ttl: std::time::Duration,
) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::Rules(
            pb::ProxyHostRulesResponse {
                subscription_id: id,
                rules: rules
                    .into_rules()
                    .into_iter()
                    .map(|rule| pb::ProxyRouteRule {
                        identity: Some(route_identity_to_proto(rule.identity)),
                        route: Some(super::proxy::route_entry_to_proto(rule.entry)),
                    })
                    .collect(),
                cache_policy: Some(cache_policy_to_proto(CachePolicy::new(ttl))),
            },
        )),
    }
}

fn route_identity_to_proto(identity: RouteIdentity) -> pb::RouteIdentity {
    let host = |host: &RouteHost| pb::RouteHost {
        kind: if host.is_wildcard() {
            pb::RouteHostKind::WildcardSuffix
        } else {
            pb::RouteHostKind::Exact
        } as i32,
        host: host.as_str().to_owned(),
    };
    let kind = match &identity {
        RouteIdentity::Http {
            host: route_host,
            path,
        } => pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
            host: Some(host(route_host)),
            path_prefix: path.as_ref().map(|path| path.as_str().to_owned()),
        }),
        RouteIdentity::Sni { host: route_host } => {
            pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
                host: Some(host(route_host)),
            })
        }
    };
    pb::RouteIdentity { kind: Some(kind) }
}

fn cache_policy_to_proto(policy: CachePolicy) -> pb::ProxyCachePolicy {
    pb::ProxyCachePolicy {
        ttl_millis: policy.ttl().as_millis().try_into().unwrap_or(u64::MAX),
    }
}

/// The response stream. It yields every queued message and then the status the
/// producer ended with, so an error reaches the proxy even when the message
/// queue was full.
struct OwnedSubscriptionStream {
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    receiver: ReceiverStream<Result<pb::ProxySubscribeResponse, Status>>,
    task: Option<tokio::task::JoinHandle<Result<(), Status>>>,
}

impl tokio_stream::Stream for OwnedSubscriptionStream {
    type Item = Result<pb::ProxySubscribeResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.receiver).poll_next(cx) {
            Poll::Ready(None) => {}
            other => return other,
        }
        let Some(task) = self.task.as_mut() else {
            return Poll::Ready(None);
        };
        let outcome = match Pin::new(task).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(outcome) => outcome,
        };
        self.task = None;
        Poll::Ready(match outcome {
            Ok(Ok(())) => None,
            Ok(Err(status)) => Some(Err(status)),
            Err(_) => Some(Err(Status::internal("subscription stream task failed"))),
        })
    }
}

impl Drop for OwnedSubscriptionStream {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::rules_response;
    use crate::{
        api::proxy::MAX_PROXY_RESPONSE_BYTES,
        ids::{BackendGeneration, Generation, InstanceId, RouteBindingId},
        instance::InstanceState,
        materialization::BackendEndpoint,
        route::{
            HostRules, PathPrefix, RouteEntry, RouteHost, RouteIdentity, RouteRule,
            HOST_RULE_OVERHEAD_BYTES, MAX_HOST_RULES, MAX_HOST_RULES_BYTES, MAX_ROUTE_PATH_BYTES,
        },
    };

    /// Route creation admits a host's rule set while its rules count at most
    /// `MAX_HOST_RULES` and `MAX_HOST_RULES_BYTES`. The largest such set, with
    /// the longest Service and Pod backend URIs the materializer renders, fits
    /// one answer.
    #[test]
    fn the_largest_rule_set_route_creation_admits_fits_one_answer() {
        let host = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61)
        );
        // A Service name and a namespace are each a label of at most 63 bytes.
        let backend = format!(
            "http://{}.{}.svc.cluster.local:65535",
            "s".repeat(63),
            "n".repeat(63)
        );
        let pod_backend = "http://[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535";
        let instance_id = "i".repeat(InstanceId::MAX_LEN);
        for path_bytes in [1, 1024, 3000, MAX_ROUTE_PATH_BYTES] {
            let mut rules = Vec::new();
            let mut counted = 0;
            loop {
                let route_binding_id = format!("route-{:0>200}", rules.len());
                let path = format!("/{:a>width$}", rules.len(), width = path_bytes - 1);
                let bytes = route_binding_id.len()
                    + instance_id.len()
                    + host.len()
                    + path.len()
                    + HOST_RULE_OVERHEAD_BYTES;
                if rules.len() == MAX_HOST_RULES || counted + bytes > MAX_HOST_RULES_BYTES {
                    break;
                }
                counted += bytes;
                rules.push(RouteRule {
                    identity: RouteIdentity::Http {
                        host: RouteHost::exact(&host).unwrap(),
                        path: Some(PathPrefix::new(path).unwrap()),
                    },
                    entry: RouteEntry {
                        route_binding_id: RouteBindingId::new(route_binding_id).unwrap(),
                        instance_id: InstanceId::new(instance_id.clone()).unwrap(),
                        instance_state: InstanceState::Running,
                        instance_generation: Generation::new(u64::MAX),
                        backend: Some(BackendEndpoint::new(backend.clone()).unwrap()),
                        pod_backend: Some(BackendEndpoint::new(pod_backend).unwrap()),
                        backend_generation: Some(BackendGeneration::new(u64::MAX)),
                    },
                });
            }
            let response = rules_response(
                u64::MAX,
                HostRules::ranked(rules),
                Duration::from_millis(u64::MAX),
            );
            let encoded = prost::Message::encoded_len(&response);
            assert!(
                encoded <= MAX_PROXY_RESPONSE_BYTES,
                "{path_bytes}-byte prefixes encode to {encoded} bytes"
            );
        }
    }
}
