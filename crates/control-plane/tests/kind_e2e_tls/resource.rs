use super::*;
use serde::Serialize;
use tokio::{sync::oneshot, task::JoinSet};

const ROTATION_HOST: &str = "rotation-load.dynamic.sleepypods.test";
const BUCKET_US: [u128; 16] = [
    100,
    200,
    500,
    1000,
    2000,
    5000,
    10000,
    20000,
    50000,
    100000,
    200000,
    500000,
    1000000,
    2000000,
    6000000,
    u128::MAX,
];
#[derive(Default, Serialize)]
struct Counts {
    attempted: u64,
    success: u64,
    rejected: u64,
    timeout: u64,
    latency_sum_us: u128,
    latency_max_us: u128,
    latency_buckets: [u64; 16],
}
impl Counts {
    fn observe(&mut self, result: Outcome, elapsed: u128) {
        self.attempted += 1;
        match result {
            Outcome::Success => self.success += 1,
            Outcome::Rejected => self.rejected += 1,
            Outcome::Timeout => self.timeout += 1,
        }
        self.latency_sum_us += elapsed;
        self.latency_max_us = self.latency_max_us.max(elapsed);
        self.latency_buckets[BUCKET_US
            .iter()
            .position(|limit| elapsed <= *limit)
            .unwrap()] += 1;
    }
}
enum Outcome {
    Success,
    Rejected,
    Timeout,
}
async fn metrics(peer: &Peer, phase: &str) -> TestResult<HashMap<String, f64>> {
    let response =
        http_once::get_once(
            peer.addr(9090),
            "localhost",
            "/metrics",
            Duration::from_secs(3),
        )
        .await
        .map_err(|error| {
            format!(
        "resource phase={phase} operation=metrics pod={} uid={} local={} remote=9090: {error}",
        peer.name(), peer.uid(), peer.addr(9090)
    )
        })?;
    assert_eq!(response.status(), http::StatusCode::OK);
    let mut result: HashMap<String, f64> = HashMap::new();
    for line in response
        .body()
        .lines()
        .filter(|line| line.starts_with("sleepypods_runtime_certificate_"))
    {
        if let Some((name, value)) = line.rsplit_once(' ') {
            result.insert(name.into(), value.parse()?);
        }
    }
    for (key, cap) in [
        ("entries", 1024.0),
        ("negative_entries", 1024.0),
        // 1,024 certificates at the 128 KiB bundle limit.
        ("accounted_bytes", 134217728.0),
        ("fetches", 3.0),
        // Each cached hostname waits at most once as a lookup and once as a refresh.
        ("queue", 4096.0),
        ("tasks", 3.0),
        ("task_high_water", 3.0),
        ("watches", 1.0),
    ] {
        let name = format!("sleepypods_runtime_certificate_{key}");
        let value = *result
            .get(&name)
            .ok_or_else(|| format!("missing metric {name}"))?;
        assert!(
            value >= 0.0 && value <= cap,
            "{name} exceeded fixed envelope: {value}>{cap}"
        );
    }
    assert!(result
        .keys()
        .all(|key| !key.contains("hostname") && !key.contains("certificate_id")));
    Ok(result)
}
fn cold_fetches(values: &HashMap<String, f64>) -> f64 {
    values
        .iter()
        .filter(|(key, _)| {
            key.starts_with("sleepypods_runtime_certificate_events_total{")
                && key.contains("operation=\"certificate_fetch\"")
        })
        .map(|(_, value)| value)
        .sum()
}
async fn zero_work(peer: &Peer, phase: &str) -> TestResult<HashMap<String, f64>> {
    timeout(Duration::from_secs(10), async {
        let mut previous = false;
        loop {
            let values = metrics(peer, phase).await?;
            let quiet = ["fetches", "queue", "tasks"]
                .iter()
                .all(|key| values[&format!("sleepypods_runtime_certificate_{key}")] == 0.0);
            if quiet && previous {
                return Ok::<_, Box<dyn Error + Send + Sync>>(values);
            }
            previous = quiet;
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await?
}
struct Wave<'a> {
    cycle: u64,
    peer: &'a Peer,
    host: &'a str,
    roots: &'a [CertificateDer<'static>],
    current: &'a CertificateDer<'static>,
    previous_rotation: &'a CertificateDer<'static>,
    next_rotation: &'a CertificateDer<'static>,
    path: &'a PathBuf,
}
impl Wave<'_> {
    async fn run(
        &self,
        start: Option<oneshot::Sender<Instant>>,
        samples: &mut Vec<serde_json::Value>,
    ) -> TestResult<()> {
        let before = metrics(self.peer, "before").await?;
        samples.push(serde_json::json!({"phase":"before","cycle":self.cycle,"pod":self.peer.name(),"uid":self.peer.uid(),"unix_millis":now(),"gauges":before}));
        let mut all_roots = self.roots.to_vec();
        all_roots.extend([self.previous_rotation.clone(), self.next_rotation.clone()]);
        let warm_config = Arc::new(client_config(&all_roots, &rustls::version::TLS13, b"h2")?);
        let refusal_config = sessions::refusal_config(&all_roots)?;
        let started = Instant::now();
        let deadline = started + Duration::from_secs(10);
        let peer_context = Arc::new(format!(
            "resource phase=flood cycle={} pod={} uid={} local={} remote=8443",
            self.cycle,
            self.peer.name(),
            self.peer.uid(),
            self.peer.addr(8443)
        ));
        let mut jobs = JoinSet::new();
        let mut miss_live = 0;
        let mut warm_live = false;
        let mut sequence = 0;
        let mut warm = Counts::default();
        let mut warm_sequence = 0;
        let mut installed_new_at = None;
        let mut miss = Counts::default();
        let mut next_sample = Instant::now();
        let mut start = start;
        while Instant::now() < deadline || !jobs.is_empty() {
            while Instant::now() < deadline && (!warm_live || miss_live < 15) {
                assert!(
                    sequence < 1_000_000,
                    "fixture operation safety bound exceeded"
                );
                let is_warm = !warm_live;
                let rotating = is_warm && warm_sequence % 2 == 1;
                let name = if is_warm {
                    warm_sequence += 1;
                    if rotating {
                        ROTATION_HOST.to_owned()
                    } else {
                        self.host.to_owned()
                    }
                } else {
                    format!("miss-{}-{}.dynamic.sleepypods.test", self.cycle, sequence)
                };
                let addr = self.peer.addr(8443);
                let config = if is_warm {
                    warm_config.clone()
                } else {
                    refusal_config.clone()
                };
                let expected = self.current.clone();
                let previous_rotation = self.previous_rotation.clone();
                let next_rotation = self.next_rotation.clone();
                let context = peer_context.clone();
                jobs.spawn(async move {
                    let at = Instant::now();
                    let mut installed_new = false;
                    let result = if !is_warm {
                        match tls_refused(addr, &name, config).await {
                            Ok(true) => Outcome::Rejected,
                            Ok(false) => Outcome::Success,
                            Err(error)
                                if error
                                    .downcast_ref::<tokio::time::error::Elapsed>()
                                    .is_some() =>
                            {
                                Outcome::Timeout
                            }
                            Err(error) => {
                                return Err(format!("{context} lane=miss: {error}").into())
                            }
                        }
                    } else {
                        match handshake(addr, &name, config).await {
                            Ok(stream) => {
                                if is_warm {
                                    let expected = if rotating {
                                        if stream
                                            .get_ref()
                                            .1
                                            .peer_certificates()
                                            .and_then(|chain| chain.first())
                                            == Some(&next_rotation)
                                        {
                                            installed_new = true;
                                            &next_rotation
                                        } else {
                                            &previous_rotation
                                        }
                                    } else {
                                        &expected
                                    };
                                    assert_peer(&stream, expected, &rustls::version::TLS13, b"h2")?;
                                }
                                Outcome::Success
                            }
                            Err(error)
                                if error
                                    .downcast_ref::<tokio::time::error::Elapsed>()
                                    .is_some() =>
                            {
                                Outcome::Timeout
                            }
                            Err(error)
                                if error
                                    .downcast_ref::<std::io::Error>()
                                    .is_some_and(is_tls_refusal) =>
                            {
                                Outcome::Rejected
                            }
                            Err(error) => {
                                return Err(format!("{context} lane=warm: {error}").into())
                            }
                        }
                    };
                    Ok::<_, Box<dyn Error + Send + Sync>>((
                        is_warm,
                        result,
                        at.elapsed().as_micros(),
                        installed_new.then_some((Instant::now(), now())),
                    ))
                });
                if is_warm {
                    warm_live = true;
                } else {
                    miss_live += 1;
                }
                sequence += 1;
            }
            if let Some(start) = start.take() {
                let _ = start.send(started);
            }
            let (is_warm, result, elapsed, installed) =
                jobs.join_next().await.ok_or("missing flood worker")???;
            if let Some((observed, wall)) = installed.filter(|(observed, _)| *observed < deadline) {
                let _ = observed;
                installed_new_at.get_or_insert(wall);
            }
            if is_warm {
                warm_live = false;
                warm.observe(result, elapsed);
            } else {
                miss_live -= 1;
                miss.observe(result, elapsed);
            }
            if Instant::now() >= next_sample {
                next_sample = Instant::now() + Duration::from_millis(250);
                samples.push(serde_json::json!({"phase":"flood","cycle":self.cycle,"pod":self.peer.name(),"uid":self.peer.uid(),"unix_millis":now(),"gauges":metrics(self.peer, "flood").await?}));
                fs::write(self.path, serde_json::to_vec_pretty(samples)?)?;
            }
        }
        assert!(started.elapsed() >= Duration::from_secs(10));
        assert!(miss.attempted > 0);
        assert_eq!(miss.success, 0, "unpublished SNI unexpectedly authorized");
        assert!(
            warm.success > 0,
            "no verified warm request progressed during miss pressure"
        );
        assert_eq!(warm.attempted + miss.attempted, sequence);
        assert!(
            installed_new_at.is_some(),
            "rotated fingerprint was not installed and used during sustained miss pressure"
        );
        zero_work(self.peer, "post-wave-quiet").await?;
        let prepared = handshake(self.peer.addr(8443), self.host, warm_config.clone()).await?;
        assert_peer(&prepared, self.current, &rustls::version::TLS13, b"h2")?;
        drop(prepared);
        let before_warm = metrics(self.peer, "before-warm-recovery").await?;
        let mut recovery = Counts::default();
        for _ in 0..32 {
            let at = Instant::now();
            let stream = handshake(self.peer.addr(8443), self.host, warm_config.clone()).await?;
            assert_peer(&stream, self.current, &rustls::version::TLS13, b"h2")?;
            recovery.observe(Outcome::Success, at.elapsed().as_micros());
        }
        let after_warm = metrics(self.peer, "after-warm-recovery").await?;
        assert_eq!(
            cold_fetches(&before_warm),
            cold_fetches(&after_warm),
            "warm-only handshakes initiated certificate fetches"
        );
        samples.push(serde_json::json!({"phase":"recovery","cycle":self.cycle,"pod":self.peer.name(),"uid":self.peer.uid(),"unix_millis":now(),"wave_duration_millis":started.elapsed().as_millis(),"warm_during_flood":warm,"miss_during_flood":miss,"rotated_installed_unix_millis":installed_new_at,"rotated_fingerprint":fingerprint(self.next_rotation),"post_wave_warm":recovery,"latency_bucket_upper_us":BUCKET_US.map(|n|n.to_string()),"gauges":after_warm}));
        fs::write(self.path, serde_json::to_vec_pretty(samples)?)?;
        Ok(())
    }
}
pub(super) async fn run(
    operator: &mut Operator,
    fronts: &[Peer],
    host: &str,
    roots: &[CertificateDer<'static>],
    current: &CertificateDer<'static>,
    artifact: &mut Artifact,
) -> TestResult<()> {
    let path = artifact
        .path
        .with_file_name("certificate-resource-samples.json");
    let mut samples = Vec::new();
    let seed = rcgen::generate_simple_self_signed(vec![ROTATION_HOST.into()])?;
    publish(operator, "dynamic-resource-rotation", 0, &seed).await?;
    bind(
        operator,
        ROTATION_HOST,
        0,
        Some("dynamic-resource-rotation"),
    )
    .await?;
    converged(
        fronts,
        ROTATION_HOST,
        &[seed.cert.der().clone()],
        seed.cert.der(),
    )
    .await?;
    artifact.record("resource-preseed",serde_json::json!({"certificate_id":"dynamic-resource-rotation","version":1,"hostname":ROTATION_HOST,"fingerprint":fingerprint(seed.cert.der()),"cycles":[1,2,3]}))?;
    fs::write(
        artifact.path.with_file_name("resource-sampling.start"),
        now().to_string(),
    )?;
    sleep(Duration::from_secs(3)).await;
    // Predeclared before measurements:3cycles; each Pod10s;15miss+1warm TLS
    // lanes, one owned API mutation future at first-wave+5s,32warm recovery,
    // two zero-work samples,7s tail (>=5s RSS span). No per-host driver history.
    let mut previous_rotation = seed.cert.der().clone();
    for cycle in 1..=3 {
        let rotating = rcgen::generate_simple_self_signed(vec![ROTATION_HOST.into()])?;
        let (start, ready) = oneshot::channel();
        let traffic = async {
            let mut start = Some(start);
            for peer in fronts {
                Wave {
                    cycle,
                    peer,
                    host,
                    roots,
                    current,
                    previous_rotation: &previous_rotation,
                    next_rotation: rotating.cert.der(),
                    path: &path,
                }
                .run(start.take(), &mut samples)
                .await
                .map_err(|error| {
                    format!(
                        "resource cycle={cycle} pod={} uid={}: {error}",
                        peer.name(),
                        peer.uid()
                    )
                })?;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        };
        let rotation = async {
            let started = ready.await?;
            tokio::time::sleep_until(started + Duration::from_secs(5)).await;
            let at = now();
            let metadata = timeout_at(
                started + Duration::from_secs(10),
                publish(operator, "dynamic-resource-rotation", cycle, &rotating),
            )
            .await??;
            Ok::<_, Box<dyn Error + Send + Sync>>((at, now(), metadata.version))
        };
        let (_, rotation) = tokio::try_join!(traffic, rotation)?;
        assert_eq!(rotation.2, cycle + 1);
        converged(
            fronts,
            ROTATION_HOST,
            &[rotating.cert.der().clone()],
            rotating.cert.der(),
        )
        .await?;
        samples.push(serde_json::json!({"phase":"rotation","cycle":cycle,"started_unix_millis":rotation.0,"completed_unix_millis":rotation.1,"version":rotation.2,"fingerprint":fingerprint(rotating.cert.der())}));
        for peer in fronts {
            let gauges = zero_work(peer, "cycle-tail-start")
                .await
                .map_err(|error| format!("resource cycle={cycle}: {error}"))?;
            samples.push(serde_json::json!({"phase":"cycle-tail-start","cycle":cycle,"pod":peer.name(),"uid":peer.uid(),"unix_millis":now(),"gauges":gauges}));
        }
        fs::write(&path, serde_json::to_vec_pretty(&samples)?)?;
        sleep(Duration::from_secs(7)).await;
        for peer in fronts {
            let gauges = zero_work(peer, "cycle-tail-end")
                .await
                .map_err(|error| format!("resource cycle={cycle}: {error}"))?;
            samples.push(serde_json::json!({"phase":"cycle-tail-end","cycle":cycle,"pod":peer.name(),"uid":peer.uid(),"unix_millis":now(),"gauges":gauges}));
        }
        fs::write(&path, serde_json::to_vec_pretty(&samples)?)?;
        previous_rotation = rotating.cert.der().clone();
    }
    fs::write(
        artifact.path.with_file_name("resource-sampling.done"),
        now().to_string(),
    )?;
    artifact.record("bounded-miss-warm-rotation-workload",serde_json::json!({"cycles":3,"flood_seconds_per_pod_cycle":10,"quiescent_tail_seconds":7,"warm_lane":1,"miss_lanes":15,"verified_post_wave_warm_successes":192,"driver_max_tls_tasks":16,"concurrent_operator_futures":1,"cache_entry_cap":1024,"negative_entry_cap":1024,"accounted_byte_cap":134217728,"fetch_task_cap":3,"queue_cap":4096,"samples":path.file_name().unwrap(),"rss":"separate process sampler"}))?;
    Ok(())
}
