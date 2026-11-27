//! Wake-latency measurement shared by the stateless and stateful kind drivers.
//!
//! A sample sends one cold request through frontline and timestamps, on this
//! process's monotonic clock, each point the wake passes: workload scaled up,
//! Pod ready, EndpointSlice ready, instance Running and first response byte.
//! The latency the platform adds is the request's whole duration minus the
//! Pod's own start (workload scaled up to Pod ready).

use std::{
    env,
    error::Error,
    future::Future,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{PersistentVolumeClaim, Pod},
    discovery::v1::EndpointSlice,
};
use kube::{
    api::{WatchEvent, WatchParams},
    Api, Client,
};
use tokio::sync::oneshot;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const INSTANCE_LABEL: &str = "sleepypods.io/instance-id";
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";
const SAMPLE_DEADLINE: Duration = Duration::from_secs(140);
const RUNNING_POLL: Duration = Duration::from_millis(10);
pub const SAMPLES_ENV: &str = "SLEEPYPODS_E2E_WAKE_LATENCY_SAMPLES";
pub const MEAN_BUDGET_ENV: &str = "SLEEPYPODS_E2E_WAKE_LATENCY_MEAN_BUDGET_MS";
pub const WORST_BUDGET_ENV: &str = "SLEEPYPODS_E2E_WAKE_LATENCY_WORST_BUDGET_MS";

/// A cold instance with one exact-host HTTP route and its rendered Service.
#[derive(Clone, Debug)]
pub struct WakeSample {
    pub instance_id: String,
    pub host: String,
    pub service_name: String,
}

/// One wake, split at the points this driver can observe.
#[derive(Clone, Copy, Debug)]
pub struct WakeLatency {
    /// Cold request sent to first response byte.
    pub total: Duration,
    /// Workload scaled up to Pod ready: the Pod's own start, outside the
    /// platform's control.
    pub pod_start: Duration,
    /// Request sent to the instance leaving its cold state: route resolution
    /// in frontline and wake acceptance.
    pub request_to_accepted: Duration,
    /// Wake accepted to workload scaled up: pickup and apply.
    pub accepted_to_workload: Duration,
    /// Request sent to claim Bound, for classes with a volume.
    pub request_to_claim_bound: Option<Duration>,
    /// Pod ready to a ready EndpointSlice endpoint: Kubernetes publication.
    pub pod_ready_to_endpoint: Duration,
    /// Ready endpoint to instance Running: detection and completion.
    pub endpoint_to_running: Duration,
    /// Instance Running to first byte: route delivery and the first proxied
    /// response.
    pub running_to_first_byte: Duration,
}

impl WakeLatency {
    /// The latency added on top of the Pod's own start.
    pub fn added(&self) -> Duration {
        self.total.saturating_sub(self.pod_start)
    }
}

pub fn sample_count() -> TestResult<usize> {
    match env::var(SAMPLES_ENV) {
        Ok(value) => Ok(value.parse::<usize>()?.max(1)),
        Err(_) => Ok(10),
    }
}

/// A pause that spreads consecutive samples across the phase of any periodic
/// work in the platform, so the mean reflects an arrival at a random time.
pub fn spacing(sample_index: usize) -> Duration {
    Duration::from_millis(300 + (sample_index as u64 * 370) % 1_000)
}

/// Measures one cold wake. `instance_state` returns the instance's protobuf
/// state number; `running_state` is the number that means Running.
pub async fn measure<P, F>(
    kube: Client,
    namespace: &str,
    frontline_addr: SocketAddr,
    sample: &WakeSample,
    running_state: i32,
    mut instance_state: P,
) -> TestResult<WakeLatency>
where
    P: FnMut(String) -> F,
    F: Future<Output = TestResult<i32>>,
{
    let by_instance = format!("{INSTANCE_LABEL}={}", sample.instance_id);
    let by_service = format!("{SERVICE_NAME_LABEL}={}", sample.service_name);
    // A workload is created with zero replicas and then scaled up; its Pod
    // can start from the moment it asks for one.
    let deployment = mark(
        Api::<Deployment>::namespaced(kube.clone(), namespace),
        &by_instance,
        |deployment| {
            deployment
                .spec
                .as_ref()
                .is_some_and(|spec| spec.replicas.unwrap_or(1) > 0)
        },
    )
    .await?;
    let stateful_set = mark(
        Api::<StatefulSet>::namespaced(kube.clone(), namespace),
        &by_instance,
        |stateful_set| {
            stateful_set
                .spec
                .as_ref()
                .is_some_and(|spec| spec.replicas.unwrap_or(1) > 0)
        },
    )
    .await?;
    let claim_bound = mark(
        Api::<PersistentVolumeClaim>::namespaced(kube.clone(), namespace),
        &by_instance,
        |claim| {
            claim
                .status
                .as_ref()
                .and_then(|status| status.phase.as_deref())
                == Some("Bound")
        },
    )
    .await?;
    let pod_ready = mark(
        Api::<Pod>::namespaced(kube.clone(), namespace),
        &by_instance,
        |pod| {
            pod.status
                .as_ref()
                .and_then(|status| status.conditions.as_ref())
                .is_some_and(|conditions| {
                    conditions
                        .iter()
                        .any(|condition| condition.type_ == "Ready" && condition.status == "True")
                })
        },
    )
    .await?;
    let endpoint_ready = mark(
        Api::<EndpointSlice>::namespaced(kube, namespace),
        &by_service,
        |slice| {
            slice.endpoints.iter().any(|endpoint| {
                !endpoint.addresses.is_empty()
                    && endpoint
                        .conditions
                        .as_ref()
                        .and_then(|conditions| conditions.ready)
                        .unwrap_or(true)
            })
        },
    )
    .await?;

    let host = sample.host.clone();
    let cold_state = instance_state(sample.instance_id.clone()).await?;
    let request = Instant::now();
    let response =
        tokio::task::spawn_blocking(move || first_byte(frontline_addr, &host, SAMPLE_DEADLINE));
    let deadline = request + SAMPLE_DEADLINE;
    let mut accepted = None;
    let running = loop {
        let state = instance_state(sample.instance_id.clone()).await?;
        if state != cold_state {
            accepted.get_or_insert_with(Instant::now);
        }
        if state == running_state {
            break Instant::now();
        }
        if Instant::now() >= deadline {
            return Err(format!("{} did not reach Running", sample.instance_id).into());
        }
        tokio::time::sleep(RUNNING_POLL).await;
    };
    let first_byte = response
        .await
        .map_err(|error| format!("cold request task failed: {error}"))??;

    // Every mark precedes the first byte, so each watch has delivered it or
    // will within its own latency. A stateless class has no claim.
    let settle = Duration::from_secs(5);
    let pod_ready = tokio::time::timeout(settle, pod_ready)
        .await
        .map_err(|_| "Pod readiness was never observed")??;
    let endpoint_ready = tokio::time::timeout(settle, endpoint_ready)
        .await
        .map_err(|_| "a ready endpoint was never observed")??;
    let (mut deployment, mut stateful_set, mut claim_bound) =
        (deployment, stateful_set, claim_bound);
    let workload = deployment
        .try_recv()
        .or_else(|_| stateful_set.try_recv())
        .map_err(|_| "workload scale-up was never observed")?;
    let claim_bound = claim_bound.try_recv().ok();
    // The poll that saw the instance leave its cold state can trail the
    // workload's own event by one interval.
    let accepted = accepted.expect("Running follows acceptance").min(workload);

    Ok(WakeLatency {
        total: first_byte.duration_since(request),
        pod_start: pod_ready.saturating_duration_since(workload),
        request_to_accepted: accepted.saturating_duration_since(request),
        accepted_to_workload: workload.saturating_duration_since(accepted),
        request_to_claim_bound: claim_bound.map(|at| at.saturating_duration_since(request)),
        pod_ready_to_endpoint: endpoint_ready.saturating_duration_since(pod_ready),
        endpoint_to_running: running.saturating_duration_since(endpoint_ready),
        running_to_first_byte: first_byte.saturating_duration_since(running),
    })
}

/// Prints every sample and the summary, then enforces the budgets the
/// environment names.
pub fn report(label: &str, samples: &[WakeLatency]) -> TestResult<()> {
    for (index, sample) in samples.iter().enumerate() {
        eprintln!(
            "wake-latency: class={label} sample={index} added_ms={} total_ms={} pod_start_ms={} \
             request_to_accepted_ms={} accepted_to_workload_ms={} request_to_claim_bound_ms={} \
             pod_ready_to_endpoint_ms={} \
             endpoint_to_running_ms={} running_to_first_byte_ms={}",
            sample.added().as_millis(),
            sample.total.as_millis(),
            sample.pod_start.as_millis(),
            sample.request_to_accepted.as_millis(),
            sample.accepted_to_workload.as_millis(),
            sample
                .request_to_claim_bound
                .map_or_else(|| "-".to_owned(), |value| value.as_millis().to_string()),
            sample.pod_ready_to_endpoint.as_millis(),
            sample.endpoint_to_running.as_millis(),
            sample.running_to_first_byte.as_millis(),
        );
    }
    let (added_mean, added_worst) = mean_and_worst(samples.iter().map(WakeLatency::added));
    let phases: [(&str, Vec<Duration>); 6] = [
        (
            "request_to_accepted",
            samples.iter().map(|s| s.request_to_accepted).collect(),
        ),
        (
            "accepted_to_workload",
            samples.iter().map(|s| s.accepted_to_workload).collect(),
        ),
        (
            "pod_ready_to_endpoint",
            samples.iter().map(|s| s.pod_ready_to_endpoint).collect(),
        ),
        (
            "endpoint_to_running",
            samples.iter().map(|s| s.endpoint_to_running).collect(),
        ),
        (
            "running_to_first_byte",
            samples.iter().map(|s| s.running_to_first_byte).collect(),
        ),
        ("pod_start", samples.iter().map(|s| s.pod_start).collect()),
    ];
    let mut summary = format!(
        "wake-latency: class={label} summary samples={} added_mean_ms={added_mean} \
         added_worst_ms={added_worst}",
        samples.len()
    );
    for (name, values) in phases {
        let (mean, worst) = mean_and_worst(values.into_iter());
        summary.push_str(&format!(" {name}_mean_ms={mean} {name}_worst_ms={worst}"));
    }
    eprintln!("{summary}");

    for (name, measured) in [
        (MEAN_BUDGET_ENV, added_mean),
        (WORST_BUDGET_ENV, added_worst),
    ] {
        if let Ok(budget) = env::var(name) {
            let budget = budget.parse::<u128>()?;
            if measured > budget {
                return Err(format!(
                    "{label} wake latency {measured} ms exceeds {name}={budget} ms"
                )
                .into());
            }
        }
    }
    Ok(())
}

fn mean_and_worst(values: impl Iterator<Item = Duration>) -> (u128, u128) {
    let values = values.map(|value| value.as_millis()).collect::<Vec<_>>();
    let worst = values.iter().copied().max().unwrap_or_default();
    let mean = values.iter().sum::<u128>() / values.len().max(1) as u128;
    (mean, worst)
}

/// Starts a watch and resolves with the time its first matching object
/// arrived. The watch is established before this returns.
async fn mark<K>(
    api: Api<K>,
    selector: &str,
    reached: fn(&K) -> bool,
) -> TestResult<oneshot::Receiver<Instant>>
where
    K: Clone + std::fmt::Debug + serde::de::DeserializeOwned + Send + 'static,
{
    let stream = api
        .watch(&WatchParams::default().labels(selector), "0")
        .await?;
    let (mut reached_at, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let mut stream = std::pin::pin!(stream);
        loop {
            tokio::select! {
                _ = reached_at.closed() => return,
                event = stream.next() => match event {
                    Some(Ok(WatchEvent::Added(object) | WatchEvent::Modified(object)))
                        if reached(&object) =>
                    {
                        let _ = reached_at.send(Instant::now());
                        return;
                    }
                    Some(Ok(_)) => {}
                    _ => return,
                },
            }
        }
    });
    Ok(receiver)
}

/// Sends one request and returns the time its first response byte arrived.
/// The response must be a 200.
fn first_byte(addr: SocketAddr, host: &str, timeout: Duration) -> TestResult<Instant> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;
    let mut head = Vec::new();
    let mut buffer = [0; 1024];
    let mut arrived = None;
    while !head.windows(2).any(|pair| pair == b"\r\n") {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err("cold request closed before a status line".into());
        }
        arrived.get_or_insert_with(Instant::now);
        head.extend_from_slice(&buffer[..count]);
        if head.len() > 16 * 1024 {
            return Err("cold request status line exceeds 16 KiB".into());
        }
    }
    let status = String::from_utf8_lossy(&head);
    if status.split_whitespace().nth(1) != Some("200") {
        return Err(format!(
            "cold request returned {:?}",
            status.lines().next().unwrap_or_default()
        )
        .into());
    }
    Ok(arrived.expect("a status line arrived"))
}
