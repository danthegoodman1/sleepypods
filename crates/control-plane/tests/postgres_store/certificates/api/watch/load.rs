//! Maximum disjoint interests on the minimum supported database pool. Raw SQL
//! only seeds the inventory; measured mutations use the real conditional store.
use super::*;

async fn ordinary_progress(
    operator: &mut OperatorControlPlaneClient<Channel>,
    proxy: &mut ProxyControlPlaneClient<Channel>,
) -> TestResult<(f64, f64)> {
    let started = tokio::time::Instant::now();
    let instance = tokio::time::timeout(
        Duration::from_secs(2),
        operator.get_instance(authorized(
            pb::GetInstanceRequest {
                instance_id: "ordinary".into(),
            },
            "operator-secret",
        )),
    )
    .await?;
    assert_eq!(instance.unwrap_err().code(), Code::NotFound);
    let instance_ms = started.elapsed().as_secs_f64() * 1000.0;
    let (send, input) = mpsc::channel(1);
    send.send(pb::ProxySubscribeRequest {
        input: Some(pb::proxy_subscribe_request::Input::SubscribeRoute(
            pb::ProxySubscribeRouteRequest {
                request_id: "ordinary".into(),
                identity: Some(pb::RouteIdentity {
                    kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                        host: Some(pb::RouteHost {
                            kind: pb::RouteHostKind::Exact as i32,
                            host: "ordinary.example".into(),
                        }),
                        path_prefix: Some("/".into()),
                    })),
                }),
            },
        )),
    })
    .await?;
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut route = proxy
            .subscribe(authorized(ReceiverStream::new(input), "proxy-secret"))
            .await?
            .into_inner();
        let response = route.message().await?.ok_or("route closed")?;
        assert!(matches!(
            response.output,
            Some(pb::proxy_subscribe_response::Output::RouteMiss(_))
        ));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    let route_ms = started.elapsed().as_secs_f64() * 1000.0;
    drop(send);
    Ok((instance_ms, route_ms))
}

#[tokio::test]
async fn postgres_sixteen_disjoint_maximum_watches_preserve_ordinary_progress_under_churn(
) -> TestResult {
    database_test(|store, raw, mut config| async move {
        // Sixteen distinct certificates obey the 1024-bindings-per-certificate
        // bound. The 16384 indexed rows are disjoint, not one shared hot set.
        for index in 0..16 {
            store.publish_certificate(publish(&format!("load-{index}"), 0,
                &[&format!("*.s{index}.load.example")])).await?;
        }
        raw.batch_execute("BEGIN; UPDATE tls_certificate_revision SET revision=revision+1 WHERE singleton; INSERT INTO tls_hostname_bindings(hostname,certificate_id,revision,last_invalidating_revision) SELECT 'h'||h||'.s'||s||'.load.example','load-'||s,revision,revision FROM tls_certificate_revision CROSS JOIN generate_series(0,15) s CROSS JOIN generate_series(0,1023) h WHERE singleton; COMMIT").await?;
        assert_eq!(raw.query_one("SELECT count(*) FROM tls_hostname_bindings", &[]).await?.get::<_,i64>(0), 16384);
        let initial_revision = global_revision(&store).await?.get();
        config.max_connections = 2;
        let limited = PostgresStore::connect(&config).await?.with_certificate_sealer(ring("a", &[("a", 7)]));
        let mut servers = tokio::task::JoinSet::new();
        let mut consumers = tokio::task::JoinSet::<TestResult<()>>::new();
        let result = async {
            let served = server(RetryingControlPlaneStore::with_default_policy(Arc::new(limited)), tokens(), true, &mut servers).await?;
            let ca = served.ca;
            let mut proxy =
                ProxyControlPlaneClient::new(channel(served.workload_url, Some(&ca)).await?);
            let mut operator =
                OperatorControlPlaneClient::new(channel(served.operator_url, Some(&ca)).await?);
            let counts = Arc::new(std::array::from_fn::<_,16,_>(|_| AtomicUsize::new(0)));
            let mut senders = Vec::new();
            let mut observed = Vec::new();
            let mut registration_ms = Vec::new();
            for index in 0..16 {
                let names: Vec<_> = (0..1024).map(|h| format!("h{h}.s{index}.load.example")).collect();
                let request = pb::WatchTlsCertificatesRequest { registration: 1, hostnames: names.clone() };
                let started = tokio::time::Instant::now();
                let deadline = started + Duration::from_secs(3);
                let (send, mut events) = tokio::time::timeout_at(deadline, watch(&mut proxy, Some(request))).await??;
                let first = tokio::time::timeout_at(deadline, events.message()).await??.ok_or("initial watch closed")?;
                assert_eq!(first.registration, 1);
                assert_eq!(first.bindings.len(), 1024);
                assert!(first.bindings.iter().all(|b| b.revision == initial_revision));
                registration_ms.push(started.elapsed().as_secs_f64() * 1000.0);
                let (latest, receiver) = tokio::sync::watch::channel(initial_revision);
                observed.push(receiver);
                senders.push(send);
                let counts = counts.clone();
                consumers.spawn(async move {
                    let certificate_id = format!("load-{index}");
                    while let Some(snapshot) = events.message().await? {
                        assert_eq!(snapshot.registration, 1);
                        assert_eq!(snapshot.bindings.len(), names.len());
                        for (binding, name) in snapshot.bindings.iter().zip(&names) {
                            assert_eq!(&binding.hostname, name);
                            assert_eq!(binding.certificate_id.as_deref(), Some(certificate_id.as_str()));
                            assert!(binding.revision >= initial_revision);
                            assert_eq!(binding.last_invalidating_revision, binding.revision);
                        }
                        counts[index].fetch_add(1, Ordering::Relaxed);
                        latest.send_replace(snapshot.bindings[0].revision);
                    }
                    Err("watch closed before cancellation".into())
                });
            }
            let mut unrelated_revision = rev(0);
            let mut expected = [initial_revision; 16];
            for phase in ["idle", "unrelated", "related"] {
                let before: Vec<_> = counts.iter().map(|n| n.load(Ordering::Relaxed)).collect();
                let started = tokio::time::Instant::now();
                let mut ticks = tokio::time::interval(Duration::from_millis(100));
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut latencies = Vec::new();
                let mut writes = 0;
                for tick in 0..32 {
                    ticks.tick().await;
                    if phase == "unrelated" {
                        unrelated_revision = tokio::time::timeout(Duration::from_secs(3), store.set_tls_binding(bind("unrelated.load.example", unrelated_revision, None))).await??.revision;
                        writes += 1;
                    } else if phase == "related" {
                        let index = tick % 16;
                        expected[index] = tokio::time::timeout(Duration::from_secs(3), store.set_tls_binding(bind(&format!("h0.s{index}.load.example"), rev(expected[index]), Some(&format!("load-{index}"))))).await??.revision.get();
                        writes += 1;
                    }
                    latencies.push(ordinary_progress(&mut operator, &mut proxy).await?);
                    assert!(consumers.try_join_next().is_none(), "a maximum-interest stream terminated during {phase}");
                }
                // A fixed end barrier includes propagation of the last related
                // write; this is a deadline, not a sleep assumed to prove order.
                if phase == "related" {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                    for (receiver, expected) in observed.iter_mut().zip(expected) {
                        tokio::time::timeout_at(deadline, async {
                            loop {
                                if *receiver.borrow_and_update() >= expected { break Ok::<_,tokio::sync::watch::error::RecvError>(()); }
                                receiver.changed().await?;
                            }
                        }).await??;
                    }
                }
                let seconds = started.elapsed().as_secs_f64();
                let delivered: Vec<_> = counts.iter().zip(&before).map(|(n,b)| n.load(Ordering::Relaxed)-b).collect();
                if phase == "idle" { assert!(delivered.iter().all(|n| *n == 0)); }
                else { assert!(delivered.iter().all(|n| *n > 0), "each maximum-interest stream must progress"); }
                let maximum = |field: usize| latencies.iter().map(|v| if field == 0 {v.0} else {v.1}).fold(0.0_f64, f64::max);
                println!("maximum_watch_load {}", serde_json::json!({
                    "phase": phase, "streams":16,"hosts_per_stream":1024,"disjoint_rows":16384,"pool_connections":2,
                    "seconds":seconds,"mutations":writes,"requested_mutation_interval_ms":100,
                    "configured_max_polls_per_stream_second":4,"delivered_snapshots":delivered,
                    "delivered_snapshots_per_second":delivered.iter().sum::<usize>() as f64 / seconds,
                    "rate_scope":"delivered changed snapshots; lower bound on completed polls, not idle SQL count",
                    "convergence_scope":"related phase checks every final per-host revision; unrelated phase checks progress, not a private global revision",
                    "ordinary_get_instance":latencies.len(),"ordinary_route_subscribe":latencies.len(),
                    "get_instance_max_ms":maximum(0),"route_subscribe_max_ms":maximum(1),
                    "initial_registration_max_ms":registration_ms.iter().copied().fold(0.0_f64,f64::max)
                }));
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            drop(senders);
            consumers.abort_all();
            tokio::time::timeout_at(deadline, async {
                while consumers.join_next().await.is_some() {}
            }).await?;
            let mut recovered = Vec::new();
            for _ in 0..16 {
                recovered.push(tokio::time::timeout_at(deadline, async {
                    loop {
                        match watch(&mut proxy, Some(registration(1, &["ordinary.example"]))).await {
                            Ok(pair) => break Ok(pair),
                            Err(error) if error.downcast_ref::<tonic::Status>().is_some_and(|s| s.code()==Code::ResourceExhausted) => tokio::task::yield_now().await,
                            Err(error) => break Err(error),
                        }
                    }
                }).await??);
            }
            for (_, events) in &mut recovered {
                let snapshot = tokio::time::timeout_at(deadline, events.message()).await??.ok_or("readmitted watch closed")?;
                assert_eq!(snapshot.bindings.len(), 1);
            }
            Ok(())
        }.await;
        consumers.abort_all(); while consumers.join_next().await.is_some() {}
        servers.abort_all(); while servers.join_next().await.is_some() {}
        result
    }).await
}
