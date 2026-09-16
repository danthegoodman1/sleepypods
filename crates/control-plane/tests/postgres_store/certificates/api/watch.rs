use super::*;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
#[path = "watch/load.rs"]
mod load;
type Events = tonic::Streaming<pb::WatchTlsCertificatesResponse>;
fn registration(number: u64, hosts: &[&str]) -> pb::WatchTlsCertificatesRequest {
    pb::WatchTlsCertificatesRequest {
        registration: number,
        hostnames: hosts.iter().map(|h| (*h).into()).collect(),
    }
}
async fn watch(
    client: &mut ProxyControlPlaneClient<Channel>,
    request: Option<pb::WatchTlsCertificatesRequest>,
) -> TestResult<(mpsc::Sender<pb::WatchTlsCertificatesRequest>, Events)> {
    let (send, recv) = mpsc::channel(1);
    if let Some(request) = request {
        send.send(request).await?;
    }
    let events = client
        .watch_tls_certificates(authorized(ReceiverStream::new(recv), "proxy-secret"))
        .await?
        .into_inner();
    Ok((send, events))
}
async fn next(events: &mut Events) -> TestResult<pb::WatchTlsCertificatesResponse> {
    Ok(
        tokio::time::timeout(Duration::from_secs(4), events.message())
            .await??
            .ok_or("watch closed")?,
    )
}
async fn at_revision(
    events: &mut Events,
    hostname: &str,
    revision: u64,
) -> TestResult<pb::WatchTlsCertificatesResponse> {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let snapshot = events.message().await?.ok_or("watch closed")?;
            if snapshot
                .bindings
                .iter()
                .any(|b| b.hostname == hostname && b.revision == revision)
            {
                return Ok(snapshot);
            }
        }
    })
    .await?
}
#[tokio::test]
async fn postgres_tls_watch_two_replicas_forced_registration_rotation_rebind_and_removal(
) -> TestResult {
    database_test(|store, raw, config| async move {
        let second = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let mut tasks = tokio::task::JoinSet::new();
        let result = async {
            let first_server = server(store.clone(), tokens(), true, &mut tasks).await?;
            let second_server = server(second.clone(), tokens(), true, &mut tasks).await?;
            let (url1, ca1) = (first_server.workload_url, first_server.ca);
            let (url2, ca2) = (second_server.workload_url, second_server.ca);
            let mut first = ProxyControlPlaneClient::new(channel(url1, Some(&ca1)).await?);
            let mut other = ProxyControlPlaneClient::new(channel(url2, Some(&ca2)).await?);
            store
                .publish_certificate(publish("A", 0, &["app.example", "shared.example"]))
                .await?;
            let a = store
                .set_tls_binding(bind("app.example", rev(0), Some("A")))
                .await?;
            let shared = store
                .set_tls_binding(bind("shared.example", rev(0), Some("A")))
                .await?;
            assert_eq!(
                first
                    .resolve_tls_certificate(authorized(query(None), "proxy-secret"))
                    .await?
                    .into_inner()
                    .view_revision,
                a.revision.get()
            );
            // Publication between resolve and registration is included in the forced snapshot.
            second
                .publish_certificate(publish("A", 1, &["app.example", "shared.example"]))
                .await?;
            let (send1, mut events1) = watch(
                &mut first,
                Some(registration(1, &["app.example", "shared.example"])),
            )
            .await?;
            let (send2, mut events2) = watch(
                &mut other,
                Some(registration(1, &["app.example", "shared.example"])),
            )
            .await?;
            let initial = next(&mut events1).await?;
            assert_eq!(initial, next(&mut events2).await?);
            assert_eq!(initial.bindings.len(), 2);
            assert_eq!(initial.bindings[0].revision, initial.bindings[1].revision);
            assert_eq!(
                initial.bindings[0].last_invalidating_revision,
                a.revision.get()
            );
            assert_eq!(
                initial.bindings[1].last_invalidating_revision,
                shared.revision.get()
            );
            let before = global_revision(&store).await?;
            let mut invalid = publish("A", 2, &["app.example", "shared.example"]);
            invalid.bundle = CertificateBundle::new(
                vec![b"bad-chain".to_vec()],
                b"WATCH-SECRET-MARKER".to_vec(),
            )?;
            assert!(second.publish_certificate(invalid).await.is_err());
            assert_eq!(global_revision(&store).await?, before);
            second
                .publish_certificate(publish("A", 2, &["app.example", "shared.example"]))
                .await?;
            let revision = second.get_tls_binding(host("app.example")).await?.revision;
            let rotation1 = at_revision(&mut events1, "app.example", revision.get()).await?;
            assert_eq!(
                rotation1,
                at_revision(&mut events2, "app.example", revision.get()).await?
            );
            assert_eq!(
                rotation1.bindings[0].last_invalidating_revision,
                a.revision.get()
            );
            // Replacement must ACK even though private global state is unchanged.
            send1.send(registration(2, &["app.example"])).await?;
            let forced = next(&mut events1).await?;
            assert_eq!(forced.registration, 2);
            assert_eq!(forced.bindings, rotation1.bindings[..1]);
            second
                .publish_certificate(publish("B", 0, &["app.example"]))
                .await?;
            let rebound = second
                .set_tls_binding(bind("app.example", revision, Some("B")))
                .await?;
            let binding1 = at_revision(&mut events1, "app.example", rebound.revision.get()).await?;
            let binding2 = at_revision(&mut events2, "app.example", rebound.revision.get()).await?;
            assert_eq!(binding1.bindings[0], binding2.bindings[0]);
            assert_eq!(
                binding1.bindings[0].last_invalidating_revision,
                rebound.revision.get()
            );
            assert_eq!(binding1.bindings[0].certificate_id.as_deref(), Some("B"));
            let found = first
                .resolve_tls_certificate(authorized(query(Some(revision.get())), "proxy-secret"))
                .await?
                .into_inner();
            let Some(pb::resolve_tls_certificate_response::Value::Found(found)) = found.value
            else {
                panic!()
            };
            assert_eq!(found.metadata.unwrap().version, 1);
            second
                .remove_certificate(RemoveCertificateRequest {
                    id: id("A"),
                    expected_version: rev(3),
                })
                .await?;
            let removed = second.get_tls_binding(host("shared.example")).await?;
            let removal =
                at_revision(&mut events2, "shared.example", removed.revision.get()).await?;
            assert!(removal.bindings[1].certificate_id.is_none());
            assert_eq!(
                removal.bindings[1].last_invalidating_revision,
                removed.revision.get()
            );
            drop((send1, send2, events1, events2));
            // Global counter jumps cannot change the authority of an unchanged host.
            raw.execute(
                "UPDATE tls_certificate_revision SET revision=revision+100001 WHERE singleton",
                &[],
            )
            .await?;
            let (_send, mut events) =
                watch(&mut other, Some(registration(1, &["app.example"]))).await?;
            let snapshot = next(&mut events).await?;
            assert_eq!(snapshot.bindings[0].revision, rebound.revision.get());
            assert!(
                !format!("{snapshot:?} {rotation1:?} {removal:?}").contains("WATCH-SECRET-MARKER")
            );
            assert_eq!(
                raw.query_one("SELECT count(*) FROM instances", &[])
                    .await?
                    .get::<_, i64>(0),
                0
            );
            Ok(())
        }
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    })
    .await
}
#[tokio::test]
async fn postgres_tls_watch_native_role_size_initial_timeout_and_capacity() -> TestResult {
    database_test(|store, _raw, mut config| async move {
        config.max_connections = 2;
        let limited = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let mut tasks = tokio::task::JoinSet::new();
        let result = async {
            let served = server(limited.clone(), tokens(), true, &mut tasks).await?;
            let (url, ca) = (served.workload_url, served.ca);
            let connection = channel(url, Some(&ca)).await?;
            let mut client = ProxyControlPlaneClient::new(connection.clone());
            for token in ["operator-secret", "sidecar-secret", "wrong-secret"] {
                let input = tonic::codegen::tokio_stream::iter([registration(1, &[])]);
                let error = client
                    .watch_tls_certificates(authorized(input, token))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error.code(),
                    Code::PermissionDenied | Code::Unauthenticated
                ));
            }
            assert_eq!(
                client
                    .watch_tls_certificates(tonic::codegen::tokio_stream::iter([registration(
                        1,
                        &[]
                    )]))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated
            );
            let mut web = ProxyControlPlaneClient::new(WebContentType(connection.clone()));
            let mut input = authorized(
                tonic::codegen::tokio_stream::iter([registration(1, &[])]),
                "proxy-secret",
            );
            input
                .metadata_mut()
                .insert("x-forwarded-proto", "https".parse()?);
            assert_eq!(
                web.watch_tls_certificates(input).await.unwrap_err().code(),
                Code::PermissionDenied
            );
            let mut held = Vec::new();
            for _ in 0..16 {
                held.push(watch(&mut client, None).await?);
            }
            let (extra, recv) = mpsc::channel(1);
            assert_eq!(
                client
                    .watch_tls_certificates(authorized(ReceiverStream::new(recv), "proxy-secret"))
                    .await
                    .unwrap_err()
                    .code(),
                Code::ResourceExhausted
            );
            drop(extra);
            assert!(tokio::time::timeout(
                Duration::from_secs(1),
                limited.get_certificate_metadata(id("missing"))
            )
            .await??
            .is_none());
            assert!(tokio::time::timeout(
                Duration::from_secs(1),
                limited.get_instance(GetInstanceRequest::new(InstanceId::new("ordinary")?))
            )
            .await??
            .is_none());
            for (_, events) in &mut held {
                assert!(
                    tokio::time::timeout(Duration::from_secs(4), events.message())
                        .await??
                        .is_none(),
                    "initial registration must close in3s, not60s"
                );
            }
            drop(held);
            let names: Vec<_> = (0..1024)
                .map(|i| {
                    format!(
                        "{:063}.{}.{}.{}",
                        i,
                        "b".repeat(63),
                        "c".repeat(63),
                        "d".repeat(61)
                    )
                })
                .collect();
            let request = pb::WatchTlsCertificatesRequest {
                registration: 1,
                hostnames: names,
            };
            let (send, mut events) = watch(&mut client, Some(request)).await?;
            let snapshot = next(&mut events).await?;
            assert_eq!(snapshot.bindings.len(), 1024);
            assert!(snapshot
                .bindings
                .iter()
                .all(|b| b.revision == 0 && b.certificate_id.is_none()));
            let oversized = pb::WatchTlsCertificatesRequest {
                registration: 2,
                hostnames: vec!["a".repeat(512 * 1024 + 1)],
            };
            send.send(oversized).await?;
            assert!(!matches!(
                tokio::time::timeout(Duration::from_secs(4), events.message()).await?,
                Ok(Some(_))
            ));
            drop((send, events));
            for (auth, tls) in [(AuthConfig::NoAuth, true), (tokens(), false)] {
                let served = server(store.clone(), auth, tls, &mut tasks).await?;
                let (url, ca) = (served.workload_url, served.ca);
                let mut insecure =
                    ProxyControlPlaneClient::new(channel(url, tls.then_some(ca.as_str())).await?);
                let mut request = authorized(
                    tonic::codegen::tokio_stream::iter([registration(1, &[])]),
                    "proxy-secret",
                );
                request
                    .metadata_mut()
                    .insert("x-forwarded-proto", "https".parse()?);
                assert_eq!(
                    insecure
                        .watch_tls_certificates(request)
                        .await
                        .unwrap_err()
                        .code(),
                    Code::PermissionDenied
                );
            }
            Ok(())
        }
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    })
    .await
}
#[tokio::test]
async fn postgres_watch_read_slot_survives_cancelled_sql_without_starving_ordinary_work(
) -> TestResult {
    database_test(|_store, raw, mut config| async move {
        config.max_connections = 16;
        let store = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let (mut blocker, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { connection.await.unwrap() });
        let pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let tx = blocker.transaction().await?;
        tx.batch_execute("LOCK TABLE tls_hostname_bindings IN ACCESS EXCLUSIVE MODE")
            .await?;
        let mut reads = tokio::task::JoinSet::new();
        let reader = store.clone();
        reads.spawn(async move {
            reader
                .snapshot_tls_bindings(vec![host("held.example")], None)
                .await
        });
        let result: TestResult = async {
                let original_pid = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        if let Some(row) = raw.query_opt(
                            "SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'",
                            &[&pid],
                        ).await? {
                            return Ok::<_, Box<dyn Error + Send + Sync>>(row.get::<_, i32>(0));
                        }
                        tokio::task::yield_now().await;
                    }
                }).await??;
                reads.abort_all();
                while reads.join_next().await.is_some() {}
                let mut queued = store.snapshot_tls_bindings(Vec::new(), None);
                // Drive the actual snapshot future while the cancelled bindings SQL
                // remains blocked. Its initial Pending poll alone proves nothing.
                let (waiting, ordinary) = tokio::join!(
                    tokio::time::timeout(Duration::from_millis(200), queued.as_mut()),
                    tokio::time::timeout(
                        Duration::from_secs(1),
                        store.get_instance(GetInstanceRequest::new(InstanceId::new("ordinary")?)),
                    ),
                );
                assert!(waiting.is_err(), "queued read must wait for the active SQL drain");
                assert!(ordinary??.is_none(), "ordinary work progresses while the watch is queued");
                assert_eq!(raw.query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'",
                    &[&pid],
                ).await?.get::<_,i64>(0), 1, "the original SQL is still blocked");
                drop(queued); // Cancel the FIFO waiter independently of the active drain.
                assert!(tokio::time::timeout(
                    Duration::from_secs(1),
                    store.get_certificate_metadata(id("ordinary-certificate"))
                )
                .await??
                .is_none());
                // Keep the table lock held. The recovery query can reach a new
                // backend only after the original client's bounded drain/discard
                // releases its watch permit. Remote SQL cancellation is not assumed.
                let recovery = store.clone();
                reads.spawn(async move { snapshot(&recovery, Vec::new(), None).await });
                tokio::time::timeout(Duration::from_secs(7), async {
                    loop {
                        let count: i64 = raw.query_one(
                            "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND pid <> $2 AND wait_event_type='Lock'",
                            &[&pid, &original_pid],
                        ).await?.get(0);
                        if count == 1 {
                            return Ok::<_, Box<dyn Error + Send + Sync>>(());
                        }
                        tokio::task::yield_now().await;
                    }
                }).await??;
                Ok(())
            }.await;
        tx.rollback().await?;
        let recovery: TestResult = if result.is_ok() {
            async {
                let recovered = tokio::time::timeout(Duration::from_secs(3), reads.join_next())
                    .await?
                    .expect("owned recovery read")??
                    .expect("forced snapshot");
                assert!(recovered.bindings.is_empty());
                Ok(())
            }
            .await
        } else {
            Ok(())
        };
        reads.abort_all();
        while reads.join_next().await.is_some() {}
        drop(blocker);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result?;
        recovery
    }).await
}
