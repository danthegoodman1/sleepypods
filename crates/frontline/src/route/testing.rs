//! A scripted control plane for coordinator, runtime and listener tests.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use sleepypods_api::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity, RouteKind, RouteRule,
};
use tokio::sync::mpsc;

use crate::{
    RouteStream, RouteStreamEvent, RouteStreamReply, RouteStreamRequest, RouteSubscriptionClient,
    RouteSubscriptionFuture, SubscribeErrorCode, SubscriptionId, WakeClient, WakeClientFuture,
    WakeInstanceRequest, WakeInstanceResponse,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TestError(pub &'static str);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for TestError {}

type Host = (RouteKind, String);

/// Answers Subscribe streams from a table of rule sets a test edits.
#[derive(Clone, Default)]
pub(crate) struct FakeControlPlane {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    rules: HashMap<Host, Vec<RouteRule>>,
    errors: HashMap<Host, SubscribeErrorCode>,
    ttl: Option<Duration>,
    capacity: Option<usize>,
    held: bool,
    failing_connects: usize,
    connects: usize,
    subscribes: Vec<Host>,
    unsubscribes: Vec<SubscriptionId>,
    streams: Vec<Stream>,
}

struct Stream {
    events: mpsc::Sender<RouteStreamEvent>,
    subscriptions: HashMap<SubscriptionId, Host>,
    parked: Vec<(SubscriptionId, Host)>,
    open: bool,
}

impl FakeControlPlane {
    /// Replaces the rule set for a host and pushes it to live subscriptions.
    pub(crate) fn set_rules(&self, kind: RouteKind, host: &str, rules: Vec<RouteRule>) {
        let mut state = self.state.lock().unwrap();
        let key = (kind, host.to_owned());
        state.rules.insert(key.clone(), rules);
        state.push(&key);
    }

    /// Replaces the rule set for a host without telling live subscriptions,
    /// as when a change event is lost.
    pub(crate) fn set_rules_quietly(&self, kind: RouteKind, host: &str, rules: Vec<RouteRule>) {
        self.state
            .lock()
            .unwrap()
            .rules
            .insert((kind, host.to_owned()), rules);
    }

    /// One exact-host HTTP binding with no path prefix.
    pub(crate) fn set_http(&self, host: &str, entry: RouteEntry) {
        self.set_rules(RouteKind::Http, host, vec![http_rule(host, None, entry)]);
    }

    pub(crate) fn set_sni(&self, host: &str, entry: RouteEntry) {
        self.set_rules(
            RouteKind::Sni,
            host,
            vec![RouteRule {
                identity: RouteIdentity::Sni {
                    host: RouteHost::exact(host).unwrap(),
                },
                entry,
            }],
        );
    }

    /// Fails lookups for a host, and ends its live subscriptions, with `code`.
    pub(crate) fn fail_host(&self, kind: RouteKind, host: &str, code: SubscribeErrorCode) {
        let mut state = self.state.lock().unwrap();
        let key = (kind, host.to_owned());
        state.errors.insert(key.clone(), code);
        for stream in state.streams.iter_mut().filter(|stream| stream.open) {
            let ended: Vec<_> = stream
                .subscriptions
                .iter()
                .filter(|(_, host)| **host == key)
                .map(|(id, _)| *id)
                .collect();
            for id in ended {
                stream.subscriptions.remove(&id);
                let _ = stream
                    .events
                    .try_send(RouteStreamEvent::Reply(RouteStreamReply::Error {
                        id,
                        code,
                        message: "scripted failure".to_owned(),
                    }));
            }
        }
    }

    pub(crate) fn heal_host(&self, kind: RouteKind, host: &str) {
        self.state
            .lock()
            .unwrap()
            .errors
            .remove(&(kind, host.to_owned()));
    }

    pub(crate) fn set_ttl(&self, ttl: Duration) {
        self.state.lock().unwrap().ttl = Some(ttl);
    }

    /// The most subscriptions one stream holds.
    pub(crate) fn set_capacity(&self, capacity: usize) {
        self.state.lock().unwrap().capacity = Some(capacity);
    }

    /// Withholds answers to Subscribe requests until `release`.
    pub(crate) fn hold(&self) {
        self.state.lock().unwrap().held = true;
    }

    pub(crate) fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.held = false;
        for index in 0..state.streams.len() {
            let parked = std::mem::take(&mut state.streams[index].parked);
            for (id, host) in parked {
                state.answer(index, id, host);
            }
        }
    }

    pub(crate) fn fail_next_connects(&self, count: usize) {
        self.state.lock().unwrap().failing_connects = count;
    }

    /// Ends every open stream in order.
    pub(crate) fn end_streams(&self) {
        self.close_streams(RouteStreamEvent::Ended);
    }

    /// Fails every open stream.
    pub(crate) fn fail_streams(&self) {
        self.close_streams(RouteStreamEvent::Failed);
    }

    fn close_streams(&self, event: RouteStreamEvent) {
        let mut state = self.state.lock().unwrap();
        for stream in state.streams.iter_mut().filter(|stream| stream.open) {
            stream.open = false;
            stream.subscriptions.clear();
            stream.parked.clear();
            let _ = stream.events.try_send(event.clone());
        }
    }

    pub(crate) fn connects(&self) -> usize {
        self.state.lock().unwrap().connects
    }

    pub(crate) fn subscribes(&self) -> usize {
        self.state.lock().unwrap().subscribes.len()
    }

    pub(crate) fn subscribes_for(&self, kind: RouteKind, host: &str) -> usize {
        let key = (kind, host.to_owned());
        self.state
            .lock()
            .unwrap()
            .subscribes
            .iter()
            .filter(|subscribed| **subscribed == key)
            .count()
    }

    pub(crate) fn unsubscribes(&self) -> usize {
        self.state.lock().unwrap().unsubscribes.len()
    }

    /// Subscriptions the open streams hold.
    pub(crate) fn live_subscriptions(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .streams
            .iter()
            .filter(|stream| stream.open)
            .map(|stream| stream.subscriptions.len())
            .sum()
    }

    pub(crate) fn is_subscribed(&self, kind: RouteKind, host: &str) -> bool {
        let key = (kind, host.to_owned());
        self.state
            .lock()
            .unwrap()
            .streams
            .iter()
            .filter(|stream| stream.open)
            .any(|stream| stream.subscriptions.values().any(|host| *host == key))
    }
}

impl State {
    fn handle(&mut self, index: usize, request: RouteStreamRequest) {
        match request {
            RouteStreamRequest::Subscribe { id, kind, host } => {
                let host = (kind, host.as_str().to_owned());
                self.subscribes.push(host.clone());
                if self.held {
                    self.streams[index].parked.push((id, host));
                } else {
                    self.answer(index, id, host);
                }
            }
            RouteStreamRequest::Unsubscribe { id } => {
                self.unsubscribes.push(id);
                self.streams[index].subscriptions.remove(&id);
                self.streams[index]
                    .parked
                    .retain(|(parked, _)| *parked != id);
            }
        }
    }

    fn answer(&mut self, index: usize, id: SubscriptionId, host: Host) {
        let reply = if let Some(code) = self.errors.get(&host) {
            RouteStreamReply::Error {
                id,
                code: *code,
                message: "scripted failure".to_owned(),
            }
        } else if self
            .capacity
            .is_some_and(|capacity| self.streams[index].subscriptions.len() >= capacity)
        {
            RouteStreamReply::Error {
                id,
                code: SubscribeErrorCode::ResourceExhausted,
                message: "subscription capacity exhausted".to_owned(),
            }
        } else {
            let rules = self.rules.get(&host).cloned().unwrap_or_default();
            if !rules.is_empty() {
                self.streams[index].subscriptions.insert(id, host);
            }
            self.rules_reply(id, rules)
        };
        let _ = self.streams[index]
            .events
            .try_send(RouteStreamEvent::Reply(reply));
    }

    fn push(&mut self, host: &Host) {
        let rules = self.rules.get(host).cloned().unwrap_or_default();
        for index in 0..self.streams.len() {
            if !self.streams[index].open {
                continue;
            }
            let ids: Vec<_> = self.streams[index]
                .subscriptions
                .iter()
                .filter(|(_, subscribed)| *subscribed == host)
                .map(|(id, _)| *id)
                .collect();
            for id in ids {
                if rules.is_empty() {
                    self.streams[index].subscriptions.remove(&id);
                }
                let reply = self.rules_reply(id, rules.clone());
                let _ = self.streams[index]
                    .events
                    .try_send(RouteStreamEvent::Reply(reply));
            }
        }
    }

    fn rules_reply(&self, id: SubscriptionId, rules: Vec<RouteRule>) -> RouteStreamReply {
        let ttl = if rules.is_empty() {
            Duration::from_secs(1)
        } else {
            self.ttl.unwrap_or(Duration::from_secs(600))
        };
        RouteStreamReply::Rules {
            id,
            rules,
            cache_policy: CachePolicy::new(ttl),
        }
    }
}

impl RouteSubscriptionClient for FakeControlPlane {
    type Error = TestError;

    fn connect(&mut self) -> RouteSubscriptionFuture<'static, RouteStream, Self::Error> {
        let shared = self.state.clone();
        Box::pin(async move {
            let (requests, mut inbound) = mpsc::channel(4096);
            let (events, outbound) = mpsc::channel(4096);
            let index = {
                let mut state = shared.lock().unwrap();
                state.connects += 1;
                if state.failing_connects > 0 {
                    state.failing_connects -= 1;
                    return Err(TestError("connect refused"));
                }
                state.streams.push(Stream {
                    events,
                    subscriptions: HashMap::new(),
                    parked: Vec::new(),
                    open: true,
                });
                state.streams.len() - 1
            };
            tokio::spawn(async move {
                while let Some(request) = inbound.recv().await {
                    let mut state = shared.lock().unwrap();
                    if state.streams[index].open {
                        state.handle(index, request);
                    }
                }
                shared.lock().unwrap().streams[index].open = false;
            });
            Ok(RouteStream {
                requests,
                events: outbound,
            })
        })
    }
}

/// A scripted wake failure. The coordinator retries a transient one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WakeError {
    Transient(&'static str),
    Refused(&'static str),
}

impl std::fmt::Display for WakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(message) | Self::Refused(message) => f.write_str(message),
        }
    }
}

/// Answers wake calls from a script and records them.
#[derive(Clone, Default)]
pub(crate) struct FakeWake {
    state: Arc<Mutex<WakeState>>,
}

#[derive(Default)]
struct WakeState {
    calls: Vec<(tokio::time::Instant, WakeInstanceRequest)>,
    error: Option<WakeError>,
    response: Option<WakeInstanceResponse>,
    hang: bool,
    in_flight: usize,
    most_in_flight: usize,
}

/// Counts a call as in flight until its future completes or is dropped.
struct InFlight(Arc<Mutex<WakeState>>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.lock().unwrap().in_flight -= 1;
    }
}

impl FakeWake {
    pub(crate) fn calls(&self) -> Vec<WakeInstanceRequest> {
        let state = self.state.lock().unwrap();
        state.calls.iter().map(|(_, call)| call.clone()).collect()
    }

    /// The time between each call and the one before it.
    pub(crate) fn gaps(&self) -> Vec<Duration> {
        let state = self.state.lock().unwrap();
        state
            .calls
            .windows(2)
            .map(|pair| pair[1].0 - pair[0].0)
            .collect()
    }

    /// The most calls that were in flight at once.
    pub(crate) fn most_in_flight(&self) -> usize {
        self.state.lock().unwrap().most_in_flight
    }

    /// Fails every call with `error` until `heal`.
    pub(crate) fn fail_with(&self, error: WakeError) {
        self.state.lock().unwrap().error = Some(error);
    }

    pub(crate) fn heal(&self) {
        self.state.lock().unwrap().error = None;
    }

    /// Answers every call with `response`. Without one, a call is accepted
    /// as still waking.
    pub(crate) fn respond_with(&self, response: WakeInstanceResponse) {
        self.state.lock().unwrap().response = Some(response);
    }

    pub(crate) fn hang(&self) {
        self.state.lock().unwrap().hang = true;
    }
}

impl WakeClient for FakeWake {
    type Error = WakeError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        let mut state = self.state.lock().unwrap();
        state
            .calls
            .push((tokio::time::Instant::now(), request.clone()));
        state.in_flight += 1;
        state.most_in_flight = state.most_in_flight.max(state.in_flight);
        let in_flight = InFlight(self.state.clone());
        let hang = state.hang;
        let result = match (&state.error, &state.response) {
            (Some(error), _) => Err(error.clone()),
            (None, Some(response)) => Ok(response.clone()),
            (None, None) => Ok(WakeInstanceResponse::StillWaking {
                instance_id: request.instance_id,
                generation: request.expected_generation,
            }),
        };
        Box::pin(async move {
            let _in_flight = in_flight;
            if hang {
                std::future::pending::<()>().await;
            }
            result
        })
    }

    fn is_transient(error: &Self::Error) -> bool {
        matches!(error, WakeError::Transient(_))
    }
}

pub(crate) fn entry(instance: &str, state: InstanceState, generation: u64) -> RouteEntry {
    let running = state == InstanceState::Running;
    RouteEntry {
        route_binding_id: RouteBindingId::new(format!("route-{instance}")).unwrap(),
        instance_id: InstanceId::new(instance).unwrap(),
        instance_state: state,
        instance_generation: Generation::new(generation),
        backend: running.then(|| BackendEndpoint::new(format!("http://{instance}:8080")).unwrap()),
        pod_backend: None,
        backend_generation: running.then(|| BackendGeneration::new(generation)),
    }
}

pub(crate) fn running(instance: &str) -> RouteEntry {
    entry(instance, InstanceState::Running, 7)
}

pub(crate) fn http_rule(host: &str, path: Option<&str>, entry: RouteEntry) -> RouteRule {
    RouteRule {
        identity: RouteIdentity::Http {
            host: RouteHost::exact(host).unwrap(),
            path: path.map(|path| PathPrefix::new(path).unwrap()),
        },
        entry,
    }
}

pub(crate) fn http_wildcard_rule(suffix: &str, path: Option<&str>, entry: RouteEntry) -> RouteRule {
    RouteRule {
        identity: RouteIdentity::Http {
            host: RouteHost::wildcard_suffix(suffix).unwrap(),
            path: path.map(|path| PathPrefix::new(path).unwrap()),
        },
        entry,
    }
}
