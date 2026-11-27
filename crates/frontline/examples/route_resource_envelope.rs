//! Same-host task/RSS regression experiment, not a production memory SLA.
//! Run this example alone: it creates a two-worker runtime and a real frontend
//! listener, with the production shared route coordinator and no-op telemetry.
//! Miss responses isolate route/listener/cache ownership; no upstream pool is
//! exercised. Client workers, HTTP drivers and fixed measurement storage are
//! included in process RSS. Every HTTP connection explicitly closes.
use std::{
    convert::Infallible,
    net::SocketAddr,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bytes::Bytes;
use frontline::{
    serve_frontline_listeners, FrontlineHttpRuntime, FrontlineListeners, FrontlineRouteCoordinator,
    FrontlineTlsAdapter, RouteStream, RouteStreamEvent, RouteStreamReply, RouteStreamRequest,
    RouteSubscriptionClient, RouteSubscriptionFuture, TlsCertificateStore, WakeClient,
    WakeClientFuture, WakeInstanceRequest, WakeInstanceResponse,
};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty, Limited};
use hyper_util::rt::TokioIo;
use proxy_core::{DrainTracker, ProxyAdmission, ProxyResourceConfig, Shutdown};
use sleepypods_api::CachePolicy;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Semaphore},
    task::JoinSet,
    time::{sleep, timeout},
};

const WARMUP: usize = 5;
const MEASURED: usize = 10;
const FLIGHTS: usize = 64;
const WAITERS: usize = 256;
const QUIESCE: Duration = Duration::from_secs(6);
const RSS_TAIL_ALLOWANCE_KIB: usize = 8 * 1024;
const RSS_SPAN_ALLOWANCE_KIB: usize = 16 * 1024;

#[derive(Clone)]
struct ControlPlane {
    gate: Arc<Mutex<Arc<Semaphore>>>,
    calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}
struct ActiveRpc(Arc<AtomicUsize>);
impl Drop for ActiveRpc {
    fn drop(&mut self) {
        assert!(self.0.fetch_sub(1, Ordering::SeqCst) > 0);
    }
}
impl RouteSubscriptionClient for ControlPlane {
    type Error = Infallible;
    /// Opens a stream whose every lookup waits on the gate and then reports
    /// that the host has no route. An unsubscribe cancels its lookup.
    fn connect(&mut self) -> RouteSubscriptionFuture<'static, RouteStream, Self::Error> {
        let control_plane = self.clone();
        Box::pin(async move {
            let (requests, mut inbound) = mpsc::channel(1024);
            let (events, outbound) = mpsc::channel(1024);
            tokio::spawn(async move {
                let mut lookups = std::collections::HashMap::new();
                while let Some(request) = inbound.recv().await {
                    lookups.retain(|_, lookup: &mut tokio::task::JoinHandle<()>| {
                        !lookup.is_finished()
                    });
                    match request {
                        RouteStreamRequest::Subscribe { id, .. } => {
                            control_plane.calls.fetch_add(1, Ordering::SeqCst);
                            let gate = control_plane.gate.lock().unwrap().clone();
                            let active = control_plane.active.clone();
                            let events = events.clone();
                            assert!(active.fetch_add(1, Ordering::SeqCst) < FLIGHTS);
                            let active = ActiveRpc(active);
                            lookups.insert(
                                id,
                                tokio::spawn(async move {
                                    let _active = active;
                                    gate.acquire().await.unwrap().forget();
                                    let _ = events
                                        .send(RouteStreamEvent::Reply(RouteStreamReply::Rules {
                                            id,
                                            rules: Vec::new(),
                                            cache_policy: CachePolicy::new(Duration::from_millis(
                                                100,
                                            )),
                                        }))
                                        .await;
                                }),
                            );
                        }
                        RouteStreamRequest::Unsubscribe { id } => {
                            if let Some(lookup) = lookups.remove(&id) {
                                lookup.abort();
                            }
                        }
                    }
                }
            });
            Ok(RouteStream {
                requests,
                events: outbound,
            })
        })
    }
}
struct NoWake;
impl WakeClient for NoWake {
    type Error = Infallible;
    fn wake_instance(
        &mut self,
        _: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        panic!("miss-only fixture cannot wake an instance")
    }
    fn is_transient(error: &Self::Error) -> bool {
        match *error {}
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Distinct,
    Same,
    Cancel,
    Recovery,
}
impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Distinct => "distinct",
            Self::Same => "same",
            Self::Cancel => "cancel",
            Self::Recovery => "recovery",
        }
    }
    fn width(self) -> usize {
        if matches!(self, Self::Same) {
            384
        } else {
            128
        }
    }
    fn accepted(self) -> usize {
        if matches!(self, Self::Same) {
            WAITERS
        } else {
            FLIGHTS
        }
    }
    fn lookups(self) -> usize {
        if matches!(self, Self::Same) {
            1
        } else {
            FLIGHTS
        }
    }
}

#[derive(Default)]
struct Callers {
    sent: AtomicUsize,
    rejected: AtomicUsize,
    missed: AtomicUsize,
}

async fn request(addr: SocketAddr, host: String, callers: Arc<Callers>) {
    // JoinSet aborts the driver if this worker is externally canceled.
    let mut drivers = JoinSet::new();
    let result = timeout(Duration::from_secs(8), async {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut sender, driver) = hyper::client::conn::http1::Builder::new()
            .max_buf_size(16 * 1024)
            .handshake(TokioIo::new(stream))
            .await
            .unwrap();
        drivers.spawn(driver);
        let response = sender.send_request(
            Request::builder()
                .uri("/")
                .header("host", host)
                .header("connection", "close")
                .body(Empty::<Bytes>::new())
                .unwrap(),
        );
        callers.sent.fetch_add(1, Ordering::SeqCst);
        let response = response.await.unwrap();
        let status = response.status();
        // Hyper handles framing; EOF is not used as proof of complete data.
        let _body = Limited::new(response.into_body(), 16 * 1024)
            .collect()
            .await
            .unwrap();
        match status {
            StatusCode::NOT_FOUND => {
                callers.missed.fetch_add(1, Ordering::SeqCst);
            }
            StatusCode::SERVICE_UNAVAILABLE => {
                callers.rejected.fetch_add(1, Ordering::SeqCst);
            }
            other => panic!("unexpected request status: {other}"),
        }
    })
    .await;
    drivers.abort_all();
    while drivers.join_next().await.is_some() {}
    result.expect("bounded client response completes");
}

fn tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}
fn rss_kib() -> usize {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps must report this child process's current RSS");
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}
fn quiet(admission: &ProxyAdmission, drain: &DrainTracker, cp: &ControlPlane) -> bool {
    admission.connections.in_flight() == 0
        && admission.requests.in_flight() == 0
        && admission.handshakes.in_flight() == 0
        && drain.active_count() == 0
        && cp.active.load(Ordering::SeqCst) == 0
}
async fn quiesce(
    baseline: usize,
    admission: &ProxyAdmission,
    drain: &DrainTracker,
    cp: &ControlPlane,
) {
    timeout(QUIESCE, async {
        let mut consecutive = 0;
        loop {
            if tasks() == baseline && quiet(admission, drain, cp) { consecutive += 1; }
            else { consecutive = 0; }
            if consecutive == 2 { return; }
            sleep(Duration::from_millis(50)).await;
        }
    }).await.unwrap_or_else(|_| panic!(
        "quiescence failed: tasks={} baseline={} sockets={} requests={} handshakes={} drain={} cp={}",
        tasks(), baseline, admission.connections.in_flight(), admission.requests.in_flight(),
        admission.handshakes.in_flight(), drain.active_count(), cp.active.load(Ordering::SeqCst),
    ));
}
#[allow(clippy::too_many_arguments)]
fn sample(
    cycle: usize,
    mode: Mode,
    stage: &str,
    observed_tasks: usize,
    rss: usize,
    admission: &ProxyAdmission,
    drain: &DrainTracker,
    cp: &ControlPlane,
) {
    println!(
        "{cycle},{},{},{stage},{observed_tasks},{rss},{},{},{},{},{},{}",
        mode.name(),
        mode.width(),
        admission.connections.in_flight(),
        admission.requests.in_flight(),
        admission.handshakes.in_flight(),
        drain.active_count(),
        cp.active.load(Ordering::SeqCst),
        cp.calls.load(Ordering::SeqCst)
    );
}

#[allow(clippy::too_many_arguments)]
async fn wave(
    cycle: usize,
    mode: Mode,
    addr: SocketAddr,
    baseline: usize,
    admission: &ProxyAdmission,
    drain: &DrainTracker,
    cp: &ControlPlane,
) -> usize {
    quiesce(baseline, admission, drain, cp).await;
    sample(
        cycle,
        mode,
        "idle",
        tasks(),
        rss_kib(),
        admission,
        drain,
        cp,
    );
    let gate = Arc::new(Semaphore::new(0));
    *cp.gate.lock().unwrap() = gate.clone();
    cp.calls.store(0, Ordering::SeqCst);
    let callers = Arc::new(Callers::default());
    // Bounded per-wave client and result storage; all of it is dropped here.
    let mut jobs = JoinSet::new();
    for index in 0..mode.width() {
        let key = if matches!(mode, Mode::Same) { 0 } else { index };
        // The cache, the flights and the subscriptions are per host.
        jobs.spawn(request(
            addr,
            format!("{cycle}-{}-{key}.resource-envelope.test", mode.name()),
            callers.clone(),
        ));
    }
    // Four possible tasks per caller: client worker, owned HTTP driver,
    // production accepted connection, and response-delivery watchdog. Each
    // admitted route flight also owns one lookup task in the fixture.
    let task_bound = baseline + 4 * mode.width() + 2 * FLIGHTS + 16;
    let mut observed_tasks = tasks();
    timeout(Duration::from_secs(3), async {
        loop {
            observed_tasks = observed_tasks.max(tasks());
            assert!(observed_tasks <= task_bound);
            assert!(cp.active.load(Ordering::SeqCst) <= FLIGHTS);
            if callers.sent.load(Ordering::SeqCst) == mode.width()
                && callers.rejected.load(Ordering::SeqCst) == mode.width() - mode.accepted()
                && admission.requests.in_flight() == mode.accepted()
                && cp.active.load(Ordering::SeqCst) == mode.lookups()
            {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("every caller reaches the expected held/overload boundary");
    assert_eq!(cp.calls.load(Ordering::SeqCst), mode.lookups());
    let mut observed_rss = 0;
    for _ in 0..3 {
        observed_tasks = observed_tasks.max(tasks());
        observed_rss = observed_rss.max(rss_kib());
        assert!(observed_tasks <= task_bound);
        sleep(Duration::from_millis(20)).await;
    }
    sample(
        cycle,
        mode,
        "held_observed_max",
        observed_tasks,
        observed_rss,
        admission,
        drain,
        cp,
    );
    if matches!(mode, Mode::Cancel) {
        // The gate stays shut: a flight ends, and unsubscribes its lookup,
        // once every caller waiting on it has disappeared.
        jobs.abort_all();
        while let Some(result) = jobs.join_next().await {
            if let Err(error) = result {
                assert!(error.is_cancelled());
            }
        }
    } else {
        gate.add_permits(mode.lookups());
        while let Some(result) = jobs.join_next().await {
            result.unwrap();
        }
        assert_eq!(callers.missed.load(Ordering::SeqCst), mode.accepted());
        assert_eq!(
            callers.rejected.load(Ordering::SeqCst),
            mode.width() - mode.accepted()
        );
    }
    drop(jobs);
    drop(callers);
    drop(gate);
    quiesce(baseline, admission, drain, cp).await;
    let rss = rss_kib();
    sample(cycle, mode, "quiescent", tasks(), rss, admission, drain, cp);
    rss
}

async fn run() {
    let cp = ControlPlane {
        gate: Arc::new(Mutex::new(Arc::new(Semaphore::new(0)))),
        calls: Arc::new(AtomicUsize::new(0)),
        active: Arc::new(AtomicUsize::new(0)),
    };
    let coordinator = FrontlineRouteCoordinator::new(64, cp.clone(), NoWake)
        .with_route_deadline(Duration::from_secs(6));
    let drain = DrainTracker::new(Duration::from_secs(1));
    // Default telemetry is no-op: no per-event collector can accumulate data.
    let runtime = FrontlineHttpRuntime::new(coordinator, drain.clone());
    let admission = ProxyAdmission::new(
        ProxyResourceConfig::default()
            .with_limits(512, 512, 512, 32)
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::new();
    let mut server = JoinSet::new();
    server.spawn(serve_frontline_listeners(
        FrontlineListeners::http(listener),
        admission.clone(),
        runtime,
        FrontlineTlsAdapter::new(TlsCertificateStore::disabled()),
        shutdown.clone(),
    ));
    // One listener task, its production shared coordinator actor, and the
    // fixture's end of the actor's Subscribe stream.
    timeout(Duration::from_secs(1), async {
        while tasks() != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let baseline = tasks();
    println!("cycle,mode,width,stage,alive_tasks,rss_kib,sockets,requests,handshakes,drain,active_cp,cp_calls");
    // Exactly forty measured quiescent rows; no per-request/event history.
    let mut quiescent = Vec::with_capacity(MEASURED * 4);
    for cycle in 0..WARMUP + MEASURED {
        for mode in [Mode::Distinct, Mode::Same, Mode::Cancel, Mode::Recovery] {
            let rss = wave(cycle, mode, addr, baseline, &admission, &drain, &cp).await;
            if cycle >= WARMUP {
                quiescent.push((cycle - WARMUP, rss));
            }
        }
    }
    let first = quiescent
        .iter()
        .filter(|(cycle, _)| *cycle < 3)
        .map(|(_, rss)| *rss)
        .max()
        .unwrap();
    let last = quiescent
        .iter()
        .filter(|(cycle, _)| *cycle >= MEASURED - 3)
        .map(|(_, rss)| *rss)
        .max()
        .unwrap();
    let minimum = quiescent.iter().map(|(_, rss)| *rss).min().unwrap();
    let maximum = quiescent.iter().map(|(_, rss)| *rss).max().unwrap();
    println!("RSS_SUMMARY first_three_max_kib={first} last_three_max_kib={last} measured_min_kib={minimum} measured_max_kib={maximum} task_baseline={baseline}");
    assert!(
        last <= first + RSS_TAIL_ALLOWANCE_KIB,
        "predeclared 8MiB tail plateau allowance exceeded"
    );
    assert!(
        maximum - minimum <= RSS_SPAN_ALLOWANCE_KIB,
        "predeclared 16MiB measured span allowance exceeded"
    );
    shutdown.shutdown();
    timeout(Duration::from_secs(2), async {
        while let Some(result) = server.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await
    .unwrap();
    quiesce(0, &admission, &drain, &cp).await;
    println!("PASS measured_cycles={MEASURED} warmup_cycles={WARMUP} source_scope=listener_coordinator_miss_only telemetry=noop");
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(150), run())
                .await
                .expect("entire experiment is bounded");
        });
}
