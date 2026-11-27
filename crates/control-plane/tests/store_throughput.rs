//! Lifecycle transitions per second through a latency relay in front of
//! Postgres, with several writers that each cycle one instance through wake
//! and sleep. The measurement runs when `SLEEPYPODS_STORE_THROUGHPUT_SECONDS`
//! names how long to drive each latency:
//!
//! ```text
//! SLEEPYPODS_POSTGRES_URL=postgres://... SLEEPYPODS_STORE_THROUGHPUT_SECONDS=10 \
//!   cargo test --release -p control-plane --test store_throughput -- --nocapture
//! ```
#[macro_use]
#[path = "support/unexpected_store.rs"]
mod unexpected_store;
#[path = "support/postgres_relay.rs"]
mod postgres_relay;
mod support;

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use control_plane::{
    materialization::AcceptWakeRequest, BackendEndpoint, BackendGeneration, BeginSleepRequest,
    ClaimNextMaterializationRequest, CompleteWakeReconciliationRequest, CompleteWakeRequest,
    ControlPlaneStore, CreateInstanceRequest, CreateWorkloadClassVersionRequest,
    FinalizeSleepReconciliationRequest, FinalizeSleepRequest, InstanceId, InstanceRecord,
    MaterializationState, MaterializationTarget, PostgresStore, PostgresStoreConfig,
    RecordMaterializationRequest,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const WRITERS: [usize; 2] = [1, 8];
const LATENCIES: [Duration; 2] = [Duration::from_micros(500), Duration::from_millis(2)];
const LEASE: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_transitions_per_second_through_a_latency_relay() -> TestResult {
    let (Ok(base), Ok(seconds)) = (
        std::env::var("SLEEPYPODS_POSTGRES_URL"),
        std::env::var("SLEEPYPODS_STORE_THROUGHPUT_SECONDS"),
    ) else {
        return Ok(());
    };
    let duration = Duration::from_secs(seconds.parse()?);
    let schema = format!(
        "throughput_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let (admin, connection) = tokio_postgres::connect(&base, tokio_postgres::NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result = measure(&base, &schema, duration).await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    admin_task.abort();
    result
}

fn with_search_path(url: &str, schema: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-csearch_path%3D{schema}")
}

async fn measure(base: &str, schema: &str, duration: Duration) -> TestResult {
    let direct = PostgresStoreConfig::new(with_search_path(base, schema))?;
    let setup = PostgresStore::connect(&direct).await?;
    let class = support::workload_class();
    setup
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    for (writers, latency) in LATENCIES
        .into_iter()
        .flat_map(|latency| WRITERS.map(|writers| (writers, latency)))
    {
        let relay = postgres_relay::Relay::with_latency(base, latency).await?;
        let mut config = PostgresStoreConfig::new(with_search_path(&relay.url(base)?, schema))?;
        config.max_connections = writers;
        let store: Arc<dyn ControlPlaneStore> = Arc::new(PostgresStore::connect(&config).await?);
        let transitions = Arc::new(AtomicU64::new(0));
        let deadline = Instant::now() + duration;
        let mut running = tokio::task::JoinSet::new();
        for writer in 0..writers {
            let id = InstanceId::new(format!("writer-{writers}-{}-{writer}", latency.as_micros()))?;
            let instance = store
                .create_instance(CreateInstanceRequest::new(id, class.reference.clone()))
                .await?
                .instance;
            let target = MaterializationTarget::new(format!("writer-{writer}"), "apps")?;
            let (store, transitions) = (store.clone(), transitions.clone());
            running.spawn(async move {
                cycle_until(&*store, instance, &target, deadline, &transitions).await
            });
        }
        let started = Instant::now();
        while let Some(finished) = running.join_next().await {
            finished??;
        }
        let elapsed = started.elapsed().as_secs_f64();
        let count = transitions.load(Ordering::SeqCst);
        eprintln!(
            "{writers} writers, {} ms per round trip: {count} transitions in {elapsed:.1} s, {:.0} per second",
            latency.as_secs_f64() * 1000.0,
            count as f64 / elapsed,
        );
    }
    Ok(())
}

/// Wakes and sleeps `instance` until `deadline`. Each cycle is four
/// instance transitions: Cold to Waking to Running to Draining to Cold.
async fn cycle_until(
    store: &dyn ControlPlaneStore,
    mut instance: InstanceRecord,
    target: &MaterializationTarget,
    deadline: Instant,
    transitions: &AtomicU64,
) -> TestResult {
    let mut backend = 1;
    while Instant::now() < deadline {
        backend += 2;
        let pending = RecordMaterializationRequest::new(
            instance.id.clone(),
            instance.generation.next(),
            target.clone(),
            MaterializationState::Pending,
            BackendGeneration::new(backend),
        );
        let waking = store
            .accept_wake(AcceptWakeRequest {
                expected_generation: instance.generation,
                pending,
            })
            .await?;
        let claimed = claim(store, MaterializationState::Pending, target).await?;
        let mut complete = CompleteWakeRequest::new(
            waking.id.clone(),
            waking.generation,
            target.clone(),
            BackendEndpoint::new("http://throughput.apps:80")?,
            claimed.backend_generation,
        );
        complete.rendered_objects = claimed.rendered_objects.clone();
        let running = store
            .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
                claimed.id.clone(),
                "throughput",
                claimed.reconciliation_lease.as_ref().unwrap().attempt,
                complete,
            ))
            .await?
            .instance;
        let draining = store
            .begin_sleep(
                BeginSleepRequest::new(running.id.clone(), running.generation, target.clone())
                    .with_drain_grace_timeout(Duration::ZERO),
            )
            .await?
            .instance;
        let claimed = claim(store, MaterializationState::Deleting, target).await?;
        instance = store
            .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
                claimed.id.clone(),
                "throughput",
                claimed.reconciliation_lease.as_ref().unwrap().attempt,
                FinalizeSleepRequest::new(draining.id, draining.generation, target.clone()),
            ))
            .await?
            .instance;
        transitions.fetch_add(4, Ordering::SeqCst);
    }
    Ok(())
}

async fn claim(
    store: &dyn ControlPlaneStore,
    state: MaterializationState,
    target: &MaterializationTarget,
) -> TestResult<control_plane::MaterializationRecord> {
    Ok(store
        .claim_next_materialization_reconciliation(ClaimNextMaterializationRequest::new(
            state,
            target.clone(),
            "throughput",
            LEASE,
        ))
        .await?
        .ok_or("no row is due")?)
}
