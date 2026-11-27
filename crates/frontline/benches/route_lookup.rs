use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use frontline::{
    FrontlineRouteCoordinator, HostKey, RouteSnapshot, RouteStream, RouteStreamEvent,
    RouteStreamReply, RouteStreamRequest, RouteSubscriptionClient, RouteSubscriptionFuture,
    RouteTable, SharedFrontlineRouteCoordinator, WakeClient, WakeClientFuture, WakeInstanceRequest,
    WakeInstanceResponse,
};
use sleepypods_api::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, HostRules, InstanceId,
    InstanceState, PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity, RouteKind,
    RouteRule, MAX_HOST_RULES,
};
use tokio::sync::mpsc;

const UNRELATED_HOSTS: usize = 4096;
const TARGET_HOST: &str = "target.example.test";
const TARGET_PATH: &str = "/api/v1/users";
const TTL: Duration = Duration::from_secs(600);

type Shared = SharedFrontlineRouteCoordinator<BenchControlPlane, BenchWakeClient>;

/// Answers every subscription with the host's rule set. With `updates`, it
/// also pushes 64 rule sets every 100 ms.
#[derive(Clone)]
struct BenchControlPlane {
    updates: bool,
}

#[derive(Clone, Debug)]
struct BenchWakeClient;

#[derive(Clone, Debug)]
struct BenchControlPlaneError;

impl RouteSubscriptionClient for BenchControlPlane {
    type Error = BenchControlPlaneError;

    fn connect(&mut self) -> RouteSubscriptionFuture<'static, RouteStream, Self::Error> {
        let updates = self.updates;
        Box::pin(async move {
            let (requests, mut inbound) = mpsc::channel(256);
            let (events, outbound) = mpsc::channel(256);
            tokio::spawn(async move {
                let mut subscribed = Vec::new();
                let mut push = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tokio::select! {
                        request = inbound.recv() => match request {
                            Some(RouteStreamRequest::Subscribe { id, kind, host }) => {
                                subscribed.push((id, kind, host.clone()));
                                let _ = events.send(rules_reply(id, kind, &host)).await;
                            }
                            Some(RouteStreamRequest::Unsubscribe { .. }) => {}
                            None => return,
                        },
                        _ = push.tick(), if updates => {
                            for (id, kind, host) in subscribed.iter().take(64) {
                                let _ = events.send(rules_reply(*id, *kind, host)).await;
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

impl WakeClient for BenchWakeClient {
    type Error = BenchControlPlaneError;

    fn wake_instance(
        &mut self,
        _request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        Box::pin(async { panic!("hot benchmark must not wake") })
    }
}

fn rules_reply(id: u64, kind: RouteKind, host: &RouteHost) -> RouteStreamEvent {
    RouteStreamEvent::Reply(RouteStreamReply::Rules {
        id,
        rules: host_rules(kind, host.as_str()).into_rules(),
        cache_policy: CachePolicy::new(TTL),
    })
}

/// The rule set the benchmark control plane holds for a host: an exact root,
/// two exact path prefixes, and a wildcard that also applies.
fn host_rules(kind: RouteKind, host: &str) -> HostRules {
    let exact = RouteHost::exact(host).expect("valid host");
    let wildcard = RouteHost::wildcard_suffix("example.test").expect("valid suffix");
    let rules = match kind {
        RouteKind::Http => [
            (exact.clone(), None),
            (exact.clone(), Some("/api")),
            (exact, Some(TARGET_PATH)),
            (wildcard, Some("/api")),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (host, path))| RouteRule {
            identity: RouteIdentity::Http {
                host,
                path: path.map(|path| PathPrefix::new(path).expect("valid path")),
            },
            entry: route_entry(index as u64 + 1),
        })
        .collect(),
        RouteKind::Sni => [exact, wildcard]
            .into_iter()
            .enumerate()
            .map(|(index, host)| RouteRule {
                identity: RouteIdentity::Sni { host },
                entry: route_entry(index as u64 + 1),
            })
            .collect(),
    };
    HostRules::ranked(rules)
}

/// One host with as many path rules as a host may have.
fn path_heavy_rules(host: &str) -> HostRules {
    let exact = RouteHost::exact(host).expect("valid host");
    HostRules::ranked(
        (0..MAX_HOST_RULES)
            .map(|index| RouteRule {
                identity: RouteIdentity::Http {
                    host: exact.clone(),
                    path: (index > 0)
                        .then(|| PathPrefix::new(format!("/unrelated-{index}")).unwrap()),
                },
                entry: route_entry(index as u64 + 1),
            })
            .collect(),
    )
}

fn route_entry(generation: u64) -> RouteEntry {
    RouteEntry {
        route_binding_id: RouteBindingId::new(format!("route-{generation}"))
            .expect("route binding ID"),
        instance_id: InstanceId::new(format!("instance-{generation}")).expect("instance ID"),
        instance_state: InstanceState::Running,
        instance_generation: Generation::new(generation),
        backend: Some(
            BackendEndpoint::new(format!("http://127.0.0.1:{}", 10_000 + generation))
                .expect("benchmark backend"),
        ),
        pod_backend: Some(
            BackendEndpoint::new(format!("http://10.244.0.{}:15000", generation % 250))
                .expect("benchmark Pod backend"),
        ),
        backend_generation: Some(BackendGeneration::new(generation)),
    }
}

fn unrelated_host(index: usize) -> String {
    format!("unrelated-{index}.example.test")
}

/// A table holding `hosts` rule sets of `kind`, with `extra` installed last.
fn table(kind: RouteKind, hosts: usize, extra: Option<(&str, HostRules)>) -> RouteTable {
    let expires_at = Instant::now() + TTL;
    let mut table = RouteTable::new(hosts + 8);
    for index in 0..hosts {
        let host = unrelated_host(index);
        table.install(
            HostKey::new(kind, &host),
            host_rules(kind, &host),
            expires_at,
            Some(index as u64),
        );
    }
    if let Some((host, rules)) = extra {
        table.install(HostKey::new(kind, host), rules, expires_at, Some(u64::MAX));
    }
    table
}

fn snapshot(kind: RouteKind, extra: (&str, HostRules)) -> Arc<RouteSnapshot> {
    table(kind, UNRELATED_HOSTS, Some(extra))
        .publish()
        .expect("the table changed")
}

fn select(snapshot: &RouteSnapshot, kind: RouteKind, host: &str, path: &str) -> u64 {
    let entry = snapshot.get(kind, host).expect("cached host");
    entry.mark_used();
    entry
        .rules
        .select(path)
        .expect("a rule covers the path")
        .entry
        .instance_generation
        .get()
}

fn cached_lookup(c: &mut Criterion) {
    let http = snapshot(
        RouteKind::Http,
        (TARGET_HOST, host_rules(RouteKind::Http, TARGET_HOST)),
    );
    c.bench_function("route_lookup/http_host_many_unrelated", |b| {
        b.iter(|| {
            black_box(select(
                &http,
                RouteKind::Http,
                black_box(TARGET_HOST),
                black_box(TARGET_PATH),
            ))
        })
    });

    let heavy = snapshot(
        RouteKind::Http,
        (
            "path-heavy.example.test",
            path_heavy_rules("path-heavy.example.test"),
        ),
    );
    // The root rule ranks last, so this request passes every other rule.
    c.bench_function("route_lookup/http_host_256_path_rules_last_match", |b| {
        b.iter(|| {
            black_box(select(
                &heavy,
                RouteKind::Http,
                black_box("path-heavy.example.test"),
                black_box(TARGET_PATH),
            ))
        })
    });

    let sni = snapshot(
        RouteKind::Sni,
        (TARGET_HOST, host_rules(RouteKind::Sni, TARGET_HOST)),
    );
    c.bench_function("route_lookup/sni_host_many_unrelated", |b| {
        b.iter(|| black_box(select(&sni, RouteKind::Sni, black_box(TARGET_HOST), "")))
    });
}

/// A coordinator whose cache holds `UNRELATED_HOSTS` hosts plus `hot`.
async fn warm_coordinator(
    control_plane: BenchControlPlane,
    observability: Option<proxy_core::observability::recorder::ObservabilityRecorder>,
    hot: &[String],
) -> Shared {
    let mut coordinator = FrontlineRouteCoordinator::new(
        UNRELATED_HOSTS + hot.len() + 8,
        control_plane,
        BenchWakeClient,
    );
    if let Some(observability) = observability {
        coordinator = coordinator.with_observability(observability);
    }
    let shared = coordinator.into_shared();
    let hosts: Vec<String> = (0..UNRELATED_HOSTS)
        .map(unrelated_host)
        .chain(hot.iter().cloned())
        .collect();
    // A coordinator resolves at most 64 hosts at once.
    for batch in hosts.chunks(32) {
        let lookups = batch.iter().map(|host| {
            let shared = shared.clone();
            async move {
                shared
                    .route_http(host, Some(TARGET_PATH), Instant::now())
                    .await
                    .expect("the benchmark control plane answers");
            }
        });
        futures_util::future::join_all(lookups).await;
    }
    shared
}

fn coordinator_hit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime builds");
    let hot = [TARGET_HOST.to_owned()];
    let now = Instant::now();

    let shared = runtime.block_on(warm_coordinator(
        BenchControlPlane { updates: false },
        None,
        &hot,
    ));
    c.bench_function("route_lookup/full_resolve_http_host_hot", |b| {
        b.iter(|| {
            runtime.block_on(async {
                black_box(
                    shared
                        .route_http(black_box(TARGET_HOST), Some(black_box(TARGET_PATH)), now)
                        .await
                        .expect("hot route resolves"),
                );
            });
        })
    });

    let observed = runtime.block_on(warm_coordinator(
        BenchControlPlane { updates: false },
        Some(
            proxy_core::observability::recorder::ObservabilityRecorder::new(Arc::new(
                BenchObservationSink,
            )),
        ),
        &hot,
    ));
    c.bench_function(
        "route_lookup/full_resolve_http_host_with_observations",
        |b| {
            b.iter(|| {
                runtime.block_on(async {
                    black_box(
                        observed
                            .route_http(TARGET_HOST, Some(TARGET_PATH), now)
                            .await
                            .expect("hot route resolves"),
                    )
                })
            });
        },
    );

    let updating = runtime.block_on(warm_coordinator(
        BenchControlPlane { updates: true },
        None,
        &hot,
    ));
    c.bench_function(
        "route_lookup/full_resolve_during_640_updates_per_second",
        |b| {
            b.iter(|| {
                runtime.block_on(async {
                    black_box(
                        updating
                            .route_http(TARGET_HOST, Some(TARGET_PATH), now)
                            .await
                            .unwrap(),
                    )
                })
            });
        },
    );
}

#[derive(Debug)]
struct BenchObservationSink;
impl proxy_core::observability::recorder::ObservabilitySink for BenchObservationSink {
    fn record(&self, event: proxy_core::observability::recorder::ObservabilityEvent) {
        black_box(event);
    }
}

/// Hot hits from 1, 8 and 32 tasks at once, each on its own host. The reported
/// time covers one hit by every task, so it stays level while throughput
/// scales with the tasks.
fn concurrent_hot_hits(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_lookup_scaling");
    for tasks in [1usize, 8, 32] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(tasks)
            .enable_all()
            .build()
            .expect("tokio runtime builds");
        let hot: Vec<String> = (0..32)
            .map(|index| format!("scaling-{index}.example.test"))
            .collect();
        let shared = runtime.block_on(warm_coordinator(
            BenchControlPlane { updates: false },
            None,
            &hot,
        ));
        let now = Instant::now();
        group.bench_with_input(
            BenchmarkId::new("hot_hit_tasks", tasks),
            &tasks,
            |b, &tasks| {
                b.iter_custom(|iters| {
                    runtime.block_on(async {
                        let start = Instant::now();
                        let handles: Vec<_> = (0..tasks)
                            .map(|index| {
                                let shared = shared.clone();
                                tokio::spawn(async move {
                                    // Mixed case, as a client may send it.
                                    let host = format!("Scaling-{index}.Example.test");
                                    for _ in 0..iters {
                                        black_box(
                                            shared
                                                .route_http(
                                                    black_box(&host),
                                                    Some(black_box(TARGET_PATH)),
                                                    now,
                                                )
                                                .await
                                                .expect("hot route"),
                                        );
                                    }
                                })
                            })
                            .collect();
                        for handle in handles {
                            handle.await.expect("task");
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }
    group.finish();
}

fn table_maintenance_and_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_cache_scale");
    for hosts in [1_000, 10_000, 100_000] {
        let now = Instant::now();
        let mut table = table(RouteKind::Http, hosts, None);
        group.bench_with_input(BenchmarkId::new("idle_expire", hosts), &hosts, |b, _| {
            b.iter(|| black_box(table.expire(black_box(now), usize::MAX)));
        });
        let snapshot = table.publish().expect("the table changed");
        let hot = unrelated_host(hosts - 1);
        group.bench_with_input(BenchmarkId::new("hot_lookup", hosts), &hosts, |b, _| {
            b.iter(|| {
                black_box(select(
                    &snapshot,
                    RouteKind::Http,
                    black_box(&hot),
                    black_box(TARGET_PATH),
                ))
            });
        });
        drop(snapshot);
        let rules = host_rules(RouteKind::Http, "churn.example.test");
        let mut next = hosts;
        group.bench_with_input(BenchmarkId::new("insert_evict", hosts), &hosts, |b, _| {
            b.iter(|| {
                next += 1;
                black_box(table.install(
                    HostKey::new(RouteKind::Http, &unrelated_host(next)),
                    rules.clone(),
                    now + TTL,
                    Some(next as u64),
                ));
            });
        });
        // Request tasks see a change once the table is published.
        group.bench_with_input(
            BenchmarkId::new("replace_and_publish", hosts),
            &hosts,
            |b, _| {
                b.iter(|| {
                    table.install(
                        HostKey::new(RouteKind::Http, &unrelated_host(next)),
                        rules.clone(),
                        now + TTL,
                        Some(next as u64),
                    );
                    black_box(table.publish())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    concurrent_hot_hits,
    cached_lookup,
    coordinator_hit,
    table_maintenance_and_churn
);
criterion_main!(benches);
