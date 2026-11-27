//! Dependency interruption: Postgres or the Kubernetes API server stops while a
//! wake or a drain is in flight. Each affected instance reaches Running or Cold
//! on its own, and every control-plane container keeps running. After setup the
//! driver calls no operator RPC except `GetInstance` reads.

use std::{collections::BTreeMap, future::Future};

use tokio::{task::JoinHandle, time::sleep_until};

use super::*;

const POSTGRES_WAKE_CLASS_ID: &str = "interrupt-postgres-wake";
const API_WAKE_CLASS_ID: &str = "interrupt-api-wake";
const POSTGRES_DEPLOYMENT: &str = "sleepypods-postgres";
const POSTGRES_LABEL: &str = "app.kubernetes.io/name=sleepypods-postgres";
const POSTGRES_OUTAGE: Duration = Duration::from_secs(30);
const API_SERVER_OUTAGE: Duration = Duration::from_secs(20);
const DRAIN_FINALIZER: &str = "test.sleepypods.io/interrupt-drain";

const POSTGRES_WAKE: InstanceRouteSpec<'static> = InstanceRouteSpec {
    class_id: POSTGRES_WAKE_CLASS_ID,
    instance_id: "interrupt-postgres-wake",
    route_id: "interrupt-postgres-wake-route",
    host: "postgres-wake.interrupt.sleepypods.test",
    target: "postgres-wake",
};
const API_WAKE: InstanceRouteSpec<'static> = InstanceRouteSpec {
    class_id: API_WAKE_CLASS_ID,
    instance_id: "interrupt-api-wake",
    route_id: "interrupt-api-wake-route",
    host: "api-wake.interrupt.sleepypods.test",
    target: "api-wake",
};
const POSTGRES_DRAIN: InstanceRouteSpec<'static> = InstanceRouteSpec {
    class_id: FAST_SLEEP_CLASS_ID,
    instance_id: "interrupt-postgres-drain",
    route_id: "interrupt-postgres-drain-route",
    host: "postgres-drain.interrupt.sleepypods.test",
    target: "postgres-drain",
};

/// Each control-plane Pod by name, with its UID and its containers' total
/// restart count.
type ControlPlanePods = BTreeMap<String, (String, i32)>;

/// A wake whose Pod waits for an image absent from the node.
struct HeldWake {
    /// The cold request that began the wake, and how long it took.
    request: JoinHandle<(Duration, TestResult<HttpResponse>)>,
    waking: Instance,
    control_plane: ControlPlanePods,
}

/// The drain scenario's Pod, which a test finalizer keeps present.
struct HeldPod {
    name: String,
    uid: String,
}

pub(super) async fn dependency_interruptions_recover_without_an_operator(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    for (class_id, image) in [
        (POSTGRES_WAKE_CLASS_ID, &config.postgres_wake_app_image),
        (API_WAKE_CLASS_ID, &config.api_wake_app_image),
    ] {
        create_workload_class(
            operator,
            class_id,
            image,
            &config.sidecar_image,
            sleep_policy(900_000),
        )
        .await?;
    }
    for spec in [POSTGRES_WAKE, API_WAKE, POSTGRES_DRAIN] {
        create_instance_and_route(operator, spec).await?;
    }

    // A drain starts only after the minimum ready age, so the drain scenario's
    // instance wakes first and ages while the two wake scenarios run.
    let held = wake_drain_instance(&kube, config).await?;
    let pods: Api<Pod> = Api::namespaced(kube.clone(), &config.namespace);
    let result = async {
        hold_pod(&pods, &held).await?;
        eprintln!("==> restart E2E: Postgres outage during a wake");
        postgres_outage_during_wake(&kube, config).await?;
        eprintln!("==> restart E2E: API server outage during a wake");
        api_server_outage_during_wake(&kube, config).await?;
        eprintln!("==> restart E2E: Postgres outage during a drain");
        postgres_outage_during_drain(&kube, config, &held).await
    }
    .await;
    // Always release this test's finalizer, including on failed assertions.
    let released = release_delete_finalizer(&pods, &held.name, &held.uid, DRAIN_FINALIZER).await;
    result.and(released)
}

/// Postgres stops while a wake waits for its Pod, and the Pod becomes ready
/// during the outage. The wake completes once Postgres returns.
async fn postgres_outage_during_wake(kube: &Client, config: &E2eConfig) -> TestResult<()> {
    let spec = POSTGRES_WAKE;
    let wake = begin_held_wake(kube, config, spec).await?;
    interrupt_postgres(kube, &config.namespace, async {
        load_image_into_kind(config, &config.postgres_wake_app_image)?;
        // A fresh Pod starts at once instead of after the image pull backoff.
        delete_workload_pods(kube.clone(), &config.namespace, spec.instance_id).await?;
        wait_for_workload_pod(
            kube,
            &config.namespace,
            spec.instance_id,
            true,
            Duration::from_secs(120),
        )
        .await
    })
    .await?;
    finish_interrupted_wake(kube, config, spec, wake).await
}

/// The API server stops while a wake waits for its Pod. The image arrives
/// during the outage and the kubelet starts the Pod at its next pull retry. The
/// wake completes once the API server returns.
async fn api_server_outage_during_wake(kube: &Client, config: &E2eConfig) -> TestResult<()> {
    let spec = API_WAKE;
    let wake = begin_held_wake(kube, config, spec).await?;
    interrupt_api_server(kube, config, async {
        load_image_into_kind(config, &config.api_wake_app_image)
    })
    .await?;
    finish_interrupted_wake(kube, config, spec, wake).await
}

/// Postgres stops while a drain waits for its held Pod to go, and the Pod goes
/// during the outage. The instance reaches Cold once Postgres returns, then
/// wakes again.
async fn postgres_outage_during_drain(
    kube: &Client,
    config: &E2eConfig,
    held: &HeldPod,
) -> TestResult<()> {
    let spec = POSTGRES_DRAIN;
    let pods: Api<Pod> = Api::namespaced(kube.clone(), &config.namespace);
    let draining = wait_for_instance_state_reconnecting(
        &config.operator_endpoint,
        spec.instance_id,
        PbInstanceState::Draining,
        sleepypods_api::INITIAL_ACTIVATION_TIMEOUT + Duration::from_secs(120),
    )
    .await?;
    wait_for_pod_terminating(&pods, held, Duration::from_secs(60)).await?;
    let control_plane = control_plane_pods(kube, &config.namespace).await?;
    interrupt_postgres(kube, &config.namespace, async {
        release_delete_finalizer(&pods, &held.name, &held.uid, DRAIN_FINALIZER).await?;
        wait_for_pod_gone(&pods, held, Duration::from_secs(60)).await
    })
    .await?;

    let recovered = Instant::now();
    let cold = wait_for_instance_state_reconnecting(
        &config.operator_endpoint,
        spec.instance_id,
        PbInstanceState::Cold,
        Duration::from_secs(120),
    )
    .await?;
    eprintln!(
        "==> restart E2E: {} Cold within {:?} of Postgres returning",
        spec.instance_id,
        recovered.elapsed()
    );
    if cold.generation != draining.generation + 1 {
        return Err(format!(
            "the interrupted drain should complete exactly one Draining-to-Cold transition from generation {}; got {}",
            draining.generation, cold.generation
        )
        .into());
    }
    wait_for_workload_absent(
        kube.clone(),
        &config.namespace,
        spec.instance_id,
        Duration::from_secs(60),
    )
    .await?;
    assert_control_plane_stayed_up(kube, &config.namespace, &control_plane).await?;

    wait_for_instance_response(
        config,
        "wake after the interrupted drain",
        spec.host,
        "/",
        spec.target,
        Duration::from_secs(180),
    )
    .await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    wait_for_instance_state(
        &mut operator,
        spec.instance_id,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    Ok(())
}

/// Starts a cold request and returns once the wake has created and activated
/// its workload. The Pod's image is absent from the node, so the wake then
/// waits for readiness until the driver loads the image.
async fn begin_held_wake(
    kube: &Client,
    config: &E2eConfig,
    spec: InstanceRouteSpec<'static>,
) -> TestResult<HeldWake> {
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    wait_for_instance_state(
        &mut operator,
        spec.instance_id,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    let control_plane = control_plane_pods(kube, &config.namespace).await?;
    let frontline = config.frontline_addr;
    let request = tokio::spawn(async move {
        let started = Instant::now();
        let response =
            http_get_with_timeout(frontline, spec.host, "/", Duration::from_secs(180)).await;
        (started.elapsed(), response)
    });
    let waking = wait_for_instance_state(
        &mut operator,
        spec.instance_id,
        PbInstanceState::Waking,
        Duration::from_secs(60),
    )
    .await?;
    wait_for_workload_pod(
        kube,
        &config.namespace,
        spec.instance_id,
        false,
        Duration::from_secs(60),
    )
    .await?;
    Ok(HeldWake {
        request,
        waking,
        control_plane,
    })
}

/// The cold request or a retry is served, the instance completes exactly one
/// Waking-to-Running transition, and no control-plane container restarted.
async fn finish_interrupted_wake(
    kube: &Client,
    config: &E2eConfig,
    spec: InstanceRouteSpec<'static>,
    wake: HeldWake,
) -> TestResult<()> {
    let context = format!("{} after the outage", spec.instance_id);
    let recovered = Instant::now();
    let (took, outcome) = wake.request.await?;
    match outcome {
        Ok(response)
            if response.status == 200 && response_body_identifies(&response, spec.target) =>
        {
            assert_instance_response(&response, &context, spec.target)?;
            eprintln!("==> restart E2E: {context}: the cold request was served after {took:?}");
        }
        outcome => {
            let ended = match outcome {
                Ok(response) => format!("HTTP {} {:?}", response.status, response.body),
                Err(error) => error.to_string(),
            };
            eprintln!(
                "==> restart E2E: {context}: the cold request ended after {took:?} with {ended}; retrying"
            );
            wait_for_instance_response(
                config,
                &context,
                spec.host,
                "/",
                spec.target,
                Duration::from_secs(180),
            )
            .await?;
        }
    }
    let running = wait_for_instance_state_reconnecting(
        &config.operator_endpoint,
        spec.instance_id,
        PbInstanceState::Running,
        Duration::from_secs(60),
    )
    .await?;
    eprintln!(
        "==> restart E2E: {context}: served and Running within {:?} of the dependency returning",
        recovered.elapsed()
    );
    if running.generation != wake.waking.generation + 1 {
        return Err(format!(
            "{context}: the wake should complete exactly one Waking-to-Running transition from generation {}; got {}",
            wake.waking.generation, running.generation
        )
        .into());
    }
    assert_control_plane_stayed_up(kube, &config.namespace, &wake.control_plane).await
}

/// Stops Postgres, runs `during` while it is down, keeps it down for
/// `POSTGRES_OUTAGE` in all, then starts it again on its claim. The instances
/// the driver reads afterwards prove the data survived.
async fn interrupt_postgres<T>(
    kube: &Client,
    namespace: &str,
    during: impl Future<Output = TestResult<T>>,
) -> TestResult<T> {
    let requested = Instant::now();
    let outage: TestResult<T> = async {
        scale_deployment(
            kube.clone(),
            namespace,
            POSTGRES_DEPLOYMENT,
            POSTGRES_LABEL,
            0,
        )
        .await?;
        let stopped = Instant::now();
        eprintln!(
            "==> restart E2E: Postgres stopped {:?} after the request",
            stopped - requested
        );
        let value = during.await?;
        sleep_until(stopped + POSTGRES_OUTAGE).await;
        Ok(value)
    }
    .await;
    // Postgres starts again whatever happened while it was down.
    let restored = scale_deployment(
        kube.clone(),
        namespace,
        POSTGRES_DEPLOYMENT,
        POSTGRES_LABEL,
        1,
    )
    .await;
    eprintln!(
        "==> restart E2E: Postgres ready {:?} after the stop request",
        requested.elapsed()
    );
    let value = outage?;
    restored?;
    Ok(value)
}

/// Stops the kind node's API server, runs `during` while it is down, keeps it
/// down for `API_SERVER_OUTAGE` in all, then starts it again.
async fn interrupt_api_server<T>(
    kube: &Client,
    config: &E2eConfig,
    during: impl Future<Output = TestResult<T>>,
) -> TestResult<T> {
    let node = format!("{}-control-plane", config.cluster_name);
    // The kubelet stops a static Pod whose manifest leaves its directory and
    // keeps it stopped. Stopping the container as well skips the API server's
    // graceful shutdown.
    node_exec(
        &node,
        &[
            "mv",
            &config.api_server_manifest,
            &config.api_server_parked_manifest,
        ],
    )?;
    let outage: TestResult<T> = async {
        let stopped = stop_api_server(&node).await?;
        if let Ok(Ok(version)) =
            tokio::time::timeout(Duration::from_secs(5), kube.apiserver_version()).await
        {
            return Err(format!(
                "the API server answered after its container stopped: {}",
                version.git_version
            )
            .into());
        }
        eprintln!("==> restart E2E: API server stopped");
        let value = during.await?;
        sleep_until(stopped + API_SERVER_OUTAGE).await;
        Ok(value)
    }
    .await;
    // The API server starts again whatever happened while it was down.
    let restored = restore_api_server(kube, config, &node).await;
    let value = outage?;
    restored?;
    Ok(value)
}

/// Stops every running API server container and returns when none runs.
async fn stop_api_server(node: &str) -> TestResult<Instant> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let running = node_exec(
            node,
            &[
                "crictl",
                "ps",
                "--name",
                "kube-apiserver",
                "--state",
                "running",
                "--quiet",
            ],
        )?;
        let containers = running.split_whitespace().collect::<Vec<_>>();
        if containers.is_empty() {
            return Ok(Instant::now());
        }
        if Instant::now() >= deadline {
            return Err(format!("API server containers {containers:?} kept running").into());
        }
        let mut stop = vec!["crictl", "stop", "--timeout", "0"];
        stop.extend(containers);
        // A container that exits first fails this stop; the next listing
        // settles it.
        let _ = node_exec(node, &stop);
        sleep(Duration::from_millis(200)).await;
    }
}

async fn restore_api_server(kube: &Client, config: &E2eConfig, node: &str) -> TestResult<()> {
    node_exec(
        node,
        &[
            "mv",
            &config.api_server_parked_manifest,
            &config.api_server_manifest,
        ],
    )?;
    let restored = Instant::now();
    let deadline = restored + Duration::from_secs(120);
    loop {
        let readyz = http::Request::get("/readyz").body(Vec::new())?;
        let last_error =
            match tokio::time::timeout(Duration::from_secs(5), kube.request_text(readyz)).await {
                Ok(Ok(_)) => {
                    eprintln!(
                        "==> restart E2E: API server ready {:?} after its manifest returned",
                        restored.elapsed()
                    );
                    return Ok(());
                }
                Ok(Err(error)) => error.to_string(),
                Err(_) => "readyz timed out".to_owned(),
            };
        if Instant::now() >= deadline {
            return Err(format!("the API server did not return: {last_error}").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

fn node_exec(node: &str, command: &[&str]) -> TestResult<String> {
    let output = Command::new("docker")
        .arg("exec")
        .arg(node)
        .args(command)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "docker exec {node} {command:?} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Wakes the drain scenario's instance and returns its Pod.
async fn wake_drain_instance(kube: &Client, config: &E2eConfig) -> TestResult<HeldPod> {
    let spec = POSTGRES_DRAIN;
    wait_for_instance_response(
        config,
        "interrupted drain setup wake",
        spec.host,
        "/",
        spec.target,
        Duration::from_secs(180),
    )
    .await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    wait_for_instance_state(
        &mut operator,
        spec.instance_id,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    let pods: Api<Pod> = Api::namespaced(kube.clone(), &config.namespace);
    let selector = format!("{INSTANCE_ID_LABEL}={}", spec.instance_id);
    let pod = single_labeled_object(
        "Pod",
        spec.instance_id,
        pods.list(&ListParams::default().labels(&selector)).await?,
    )?;
    Ok(HeldPod {
        name: pod.metadata.name.ok_or("drain scenario Pod has no name")?,
        uid: pod.metadata.uid.ok_or("drain scenario Pod has no UID")?,
    })
}

/// Adds the test finalizer, so the drain waits for this Pod's absence until the
/// driver releases it.
async fn hold_pod(pods: &Api<Pod>, held: &HeldPod) -> TestResult<()> {
    let pod = pods.get(&held.name).await?;
    if pod.metadata.uid.as_deref() != Some(held.uid.as_str())
        || pod.metadata.deletion_timestamp.is_some()
    {
        return Err("the drain scenario's Pod changed before the driver held it".into());
    }
    let mut finalizers = pod.metadata.finalizers.clone().unwrap_or_default();
    finalizers.push(DRAIN_FINALIZER.into());
    pods.patch(
        &held.name,
        &PatchParams::default(),
        &Patch::Merge(json!({"metadata": {
            "uid": held.uid, "resourceVersion": pod.metadata.resource_version, "finalizers": finalizers,
        }})),
    )
    .await?;
    Ok(())
}

/// Waits until the drain has deleted the held Pod.
async fn wait_for_pod_terminating(
    pods: &Api<Pod>,
    held: &HeldPod,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let pod = pods.get(&held.name).await?;
        if pod.metadata.uid.as_deref() != Some(held.uid.as_str()) {
            return Err("the held drain Pod was replaced".into());
        }
        if pod.metadata.deletion_timestamp.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the drain did not delete its held Pod: {}",
                describe_pods(&[pod])
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_pod_gone(pods: &Api<Pod>, held: &HeldPod, timeout: Duration) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let Some(pod) = pods.get_opt(&held.name).await? else {
            return Ok(());
        };
        if pod.metadata.uid.as_deref() != Some(held.uid.as_str()) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("the released drain Pod stayed: {}", describe_pods(&[pod])).into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Waits for a live Pod of the instance, and for it to be ready when `ready`
/// is set.
async fn wait_for_workload_pod(
    kube: &Client,
    namespace: &str,
    instance_id: &str,
    ready: bool,
    timeout: Duration,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube.clone(), namespace);
    let selector = format!("{INSTANCE_ID_LABEL}={instance_id}");
    let deadline = Instant::now() + timeout;
    loop {
        let listed = pods.list(&ListParams::default().labels(&selector)).await?;
        if listed
            .iter()
            .any(|pod| pod.metadata.deletion_timestamp.is_none() && (!ready || pod_ready(pod)))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for a {} Pod of {instance_id}: {}",
                if ready { "ready" } else { "live" },
                describe_pods(&listed.items)
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

fn describe_pods(pods: &[Pod]) -> String {
    let described = pods
        .iter()
        .map(|pod| {
            let status = pod.status.as_ref();
            let containers = status
                .and_then(|status| status.container_statuses.as_ref())
                .into_iter()
                .flatten()
                .map(|container| {
                    let waiting = container
                        .state
                        .as_ref()
                        .and_then(|state| state.waiting.as_ref())
                        .and_then(|waiting| waiting.reason.as_deref());
                    format!(
                        "{} ready={} restarts={} waiting={waiting:?}",
                        container.name, container.ready, container.restart_count
                    )
                })
                .collect::<Vec<_>>();
            format!(
                "{} phase={:?} deleting={} containers={containers:?}",
                pod.metadata.name.as_deref().unwrap_or("?"),
                status.and_then(|status| status.phase.as_deref()),
                pod.metadata.deletion_timestamp.is_some(),
            )
        })
        .collect::<Vec<_>>();
    if described.is_empty() {
        "no Pods".to_owned()
    } else {
        described.join("; ")
    }
}

async fn control_plane_pods(kube: &Client, namespace: &str) -> TestResult<ControlPlanePods> {
    let pods: Api<Pod> = Api::namespaced(kube.clone(), namespace);
    let listed = pods
        .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
        .await?;
    let snapshot = listed
        .items
        .into_iter()
        .map(|pod| {
            let restarts = pod
                .status
                .as_ref()
                .and_then(|status| status.container_statuses.as_ref())
                .map_or(0, |containers| {
                    containers
                        .iter()
                        .map(|container| container.restart_count)
                        .sum()
                });
            (
                pod.metadata.name.unwrap_or_default(),
                (pod.metadata.uid.unwrap_or_default(), restarts),
            )
        })
        .collect::<ControlPlanePods>();
    if snapshot.is_empty() {
        return Err("no control-plane Pod found".into());
    }
    Ok(snapshot)
}

/// Every control-plane Pod from before the outage still runs, with no
/// container restart and no replacement.
async fn assert_control_plane_stayed_up(
    kube: &Client,
    namespace: &str,
    before: &ControlPlanePods,
) -> TestResult<()> {
    let after = control_plane_pods(kube, namespace).await?;
    if &after != before {
        return Err(format!(
            "the control plane restarted: Pods (UID, restarts) were {before:?}, now {after:?}"
        )
        .into());
    }
    eprintln!("==> restart E2E: control-plane Pods kept running: {after:?}");
    Ok(())
}
