//! Metadata-only current-state push. The private global revision skips stable
//! SQL reads; hostname revisions and invalidation watermarks carry authority.
use super::{
    admission::SubscriptionLease, certificates, pb, route_events::RouteSubscriptionBroker,
};
use crate::{auth::CallerRole, certificate::*, store::TlsWatchStore};
use std::{
    collections::HashSet,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tonic::{
    codegen::tokio_stream::{self, wrappers::ReceiverStream},
    Request, Response, Status,
};

pub(crate) use crate::certificate::work::WATCH_STREAMS;
const RESPONSE_QUEUE: usize = 2;
const POLL: Duration = Duration::from_millis(250);
const LIFETIME: Duration = Duration::from_secs(60);
const LOOKUP: Duration = Duration::from_secs(3);
const DELIVERY: Duration = Duration::from_secs(1);
static NEXT_WATCH: AtomicU64 = AtomicU64::new(0);
pub(crate) type WatchStream = Pin<
    Box<dyn tokio_stream::Stream<Item = Result<pb::WatchTlsCertificatesResponse, Status>> + Send>,
>;

pub(crate) async fn watch(
    store: Arc<dyn TlsWatchStore>,
    broker: RouteSubscriptionBroker,
    request: Request<tonic::Streaming<pb::WatchTlsCertificatesRequest>>,
) -> Result<Response<WatchStream>, Status> {
    certificates::require_secure(&request, CallerRole::Proxy)?;
    let permit = Arc::new(
        broker
            .certificate_streams
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("certificate watch capacity exhausted"))?,
    );
    let requests = request.into_inner();
    let (responses, receiver) = mpsc::channel(RESPONSE_QUEUE);
    let owned = permit.clone();
    let task = tokio::spawn(produce(store, broker, requests, responses, owned));
    let mut response: Response<WatchStream> = Response::new(Box::pin(OwnedWatch {
        receiver: ReceiverStream::new(receiver),
        task,
        _permit: permit.clone(),
    }));
    response.extensions_mut().insert(SubscriptionLease {
        permit,
        lifetime: LIFETIME,
    });
    Ok(response)
}
// Kept separate so timer/backpressure ownership can be tested with controlled
// decoded input; native TLS/body decoding is exercised by the real API tests.
async fn produce<S>(
    store: Arc<dyn TlsWatchStore>,
    broker: RouteSubscriptionBroker,
    mut requests: S,
    responses: mpsc::Sender<Result<pb::WatchTlsCertificatesResponse, Status>>,
    owned: Arc<tokio::sync::OwnedSemaphorePermit>,
) where
    S: tokio_stream::Stream<Item = Result<pb::WatchTlsCertificatesRequest, Status>> + Unpin,
{
    let _owned = owned;
    let cancellation = broker.cancellation.clone();
    // Per-stream phases avoid a synchronized sixteen-query burst. The store-owned watch
    // permit bounds both active and draining SQL across all streams. Pool=2
    // retains its ordinary slot; default pools retain unary cert slots too.
    let phase = Duration::from_millis((NEXT_WATCH.fetch_add(1, Ordering::Relaxed) % 16) * 15);
    let mut next_poll = tokio::time::Instant::now() + phase;
    let setup = tokio::time::sleep(LOOKUP);
    tokio::pin!(setup);
    let mut state = WatchState::default();
    // The same outer bound covers input decode, SQL, and delivery. A dropped
    // SQL future keeps the existing guarded certificate checkout ownership.
    let _ = tokio::time::timeout(LIFETIME, async {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = responses.closed() => return,
                    _ = &mut setup, if state.revision.is_none() => return,
                    next = tokio_stream::StreamExt::next(&mut requests), if state.pending.is_none() => {
                        let next = match next {
                            Some(Ok(next)) => next,
                            _ => return,
                        };
                        match state.register(next) {
                            Ok(()) => {},
                            Err(error) => {
                                let _ = responses.try_send(Err(error));
                                return;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(next_poll), if state.pending.is_some() || state.revision.is_some() => {
                        next_poll = tokio::time::Instant::now() + POLL;
                        let result = tokio::select! {
                            _ = cancellation.cancelled() => return,
                            _ = &mut setup, if state.revision.is_none() => return,
                            result = tokio::time::timeout(LOOKUP, state.poll(store.as_ref())) => result,
                        };
                        let response = match result {
                            Ok(response) => response,
                            Err(_) => Err(Status::deadline_exceeded("certificate watch query deadline elapsed")),
                        };
                        let terminal = response.is_err();
                        let response = match response {
                            Ok(Some(v)) => Ok(v),
                            Ok(None) => continue,
                            Err(e) => Err(e),
                        };
                        if !matches!(
                            tokio::time::timeout(DELIVERY, responses.send(response)).await,
                            Ok(Ok(()))
                        ) || terminal {
                            return;
                        }
                    }
                }
            }
        }).await;
}
#[derive(Default)]
struct WatchState {
    hosts: Vec<TlsHostname>,
    revision: Option<CertificateRevision>,
    registration: u64,
    pending: Option<Vec<TlsHostname>>,
}
impl WatchState {
    fn register(&mut self, request: pb::WatchTlsCertificatesRequest) -> Result<(), Status> {
        if request.registration <= self.registration {
            return Err(Status::invalid_argument(
                "certificate registration must increase",
            ));
        }
        let hosts = interests(request.hostnames)?;
        self.registration = request.registration;
        self.pending = Some(hosts);
        Ok(())
    }
    // A registration replaces one regular poll, never bypasses the 4/s rate.
    // While pending, input reads stop instead of collecting registration work.
    async fn poll(
        &mut self,
        store: &dyn TlsWatchStore,
    ) -> Result<Option<pb::WatchTlsCertificatesResponse>, Status> {
        if self.pending.is_none() && self.hosts.is_empty() {
            return Ok(None);
        }
        let names = self.pending.as_ref().unwrap_or(&self.hosts);
        let known = if self.pending.is_some() {
            None
        } else {
            self.revision
        };
        let snapshot = match store.snapshot_tls_bindings(names.clone(), known).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) | Err(crate::store::StoreError::Unavailable { .. }) => return Ok(None),
            Err(_) => {
                return Err(Status::unavailable(
                    "certificate synchronization unavailable",
                ))
            }
        };
        if let Some(names) = self.pending.take() {
            self.hosts = names;
        }
        self.revision = Some(snapshot.revision);
        Ok(Some(pb::WatchTlsCertificatesResponse {
            registration: self.registration,
            bindings: snapshot
                .bindings
                .into_iter()
                .map(|b| pb::TlsBinding {
                    hostname: b.hostname.to_string(),
                    certificate_id: b.certificate_id.map(|id| id.to_string()),
                    revision: b.revision.get(),
                    last_invalidating_revision: b.last_invalidating_revision.get(),
                })
                .collect(),
        }))
    }
}
fn interests(values: Vec<String>) -> Result<Vec<TlsHostname>, Status> {
    if values.len() > MAX_CERTIFICATE_BINDINGS {
        return Err(Status::resource_exhausted(
            "certificate interests exceed 1024 hosts",
        ));
    }
    let mut unique = HashSet::new();
    let mut names = Vec::with_capacity(values.len());
    for value in values {
        let name = TlsHostname::new(&value)
            .map_err(|_| Status::invalid_argument("invalid certificate interest"))?;
        if name.as_str() != value || !unique.insert(name.clone()) {
            return Err(Status::invalid_argument("invalid certificate interest"));
        }
        names.push(name);
    }
    Ok(names)
}
struct OwnedWatch {
    receiver: ReceiverStream<Result<pb::WatchTlsCertificatesResponse, Status>>,
    task: tokio::task::JoinHandle<()>,
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}
impl tokio_stream::Stream for OwnedWatch {
    type Item = Result<pb::WatchTlsCertificatesResponse, Status>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}
impl Drop for OwnedWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests;
