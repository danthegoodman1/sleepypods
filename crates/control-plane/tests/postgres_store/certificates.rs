//! Real PostgreSQL conformance for certificate authority, not a database fake.
use super::*;
use control_plane::{
    certificate::*, RetryPolicy, RetryingControlPlaneStore, StoreFuture, StoreResult,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio_postgres::Client;

fn id(value: &str) -> CertificateId {
    CertificateId::new(value).unwrap()
}
fn host(value: &str) -> TlsHostname {
    TlsHostname::new(value).unwrap()
}
fn rev(value: u64) -> CertificateRevision {
    CertificateRevision::new(value).unwrap()
}
fn material(names: &[&str]) -> CertificateBundle {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
            .unwrap();
    CertificateBundle::new(vec![cert.der().to_vec()], signing_key.serialize_der()).unwrap()
}
fn ring(active: &str, keys: &[(&str, u8)]) -> Arc<CertificateSealer> {
    Arc::new(
        CertificateSealer::new(
            active,
            keys.iter()
                .map(|(id, key)| SealingKey::new(*id, [*key; 32]).unwrap())
                .collect(),
        )
        .unwrap(),
    )
}
fn publish(name: &str, version: u64, names: &[&str]) -> PublishCertificateRequest {
    PublishCertificateRequest {
        id: id(name),
        expected_version: rev(version),
        bundle: material(names),
    }
}
fn bind(
    name: &str,
    revision: CertificateRevision,
    certificate: Option<&str>,
) -> SetTlsBindingRequest {
    SetTlsBindingRequest {
        hostname: host(name),
        expected_revision: revision,
        certificate_id: certificate.map(id),
    }
}
async fn resolve(store: &PostgresStore, name: &str) -> StoreResult<TlsCertificateResolution> {
    store
        .resolve_tls_certificate(ResolveTlsCertificateRequest {
            hostname: host(name),
            known_view_revision: None,
        })
        .await
}

async fn snapshot(
    store: &PostgresStore,
    hosts: Vec<TlsHostname>,
    known: Option<CertificateRevision>,
) -> StoreResult<Option<TlsBindingSnapshot>> {
    RetryingControlPlaneStore::with_default_policy(Arc::new(store.clone()))
        .snapshot_tls_bindings(hosts, known)
        .await
}
async fn global_revision(store: &PostgresStore) -> StoreResult<CertificateRevision> {
    Ok(snapshot(store, Vec::new(), None)
        .await?
        .expect("forced empty snapshot")
        .revision)
}

async fn database_test<F, Fut>(test: F) -> TestResult
where
    F: FnOnce(PostgresStore, Arc<Client>, PostgresStoreConfig) -> Fut + Send + 'static,
    Fut: Future<Output = TestResult> + Send + 'static,
{
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping real certificate PostgreSQL test: SLEEPYPODS_POSTGRES_URL absent");
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, admin_conn) = tokio_postgres::connect(&base_url, NoTls).await?;
    let admin_task = tokio::spawn(admin_conn);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
    let setup = async {
        let store = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let (raw, raw_conn) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>((store, Arc::new(raw), tokio::spawn(raw_conn)))
    }
    .await;
    let result = match setup {
        Ok((store, raw, raw_task)) => {
            // Owned child plus outer deadline guarantees schema cleanup even if
            // a safety assertion panics or an operation stalls.
            let mut task = tokio::spawn(test(store, raw, config));
            let result = tokio::time::timeout(Duration::from_secs(60), &mut task).await;
            let result = match result {
                Ok(result) => result
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
                    .and_then(|r| r),
                Err(error) => {
                    task.abort();
                    let _ = task.await;
                    Err(Box::new(error) as Box<dyn Error + Send + Sync>)
                }
            };
            raw_task.abort();
            let _ = raw_task.await;
            result
        }
        Err(error) => Err(error),
    };
    let cleanup = admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    drop(admin);
    admin_task.abort();
    let _ = admin_task.await;
    cleanup?;
    result
}

#[tokio::test]
async fn postgres_certificate_cas_rotation_tombstones_and_sealing() -> TestResult {
    database_test(|store, raw, config| async move {
        // A NULL active envelope must not exploit SQL CHECK's UNKNOWN result.
        assert!(raw.execute("INSERT INTO certificates(certificate_id,version,state,not_before_unix_millis,not_after_unix_millis,dns_names,sealing_revision) VALUES('invalid',1,'active',1,2,ARRAY['a.example.test'],1)",&[]).await.is_err());
        assert!(matches!(
            resolve(&store, "a.example.test").await?.value,
            TlsCertificateValue::Missing
        ));
        let request = publish("a", 0, &["a.example.test", "b.example.test"]);
        let original_key = request.bundle.private_key_pkcs8_der().to_vec();
        let original_chain = request.bundle.chain_der().to_vec();
        let first = store.publish_certificate(request.clone()).await?;
        assert_eq!(first.version, rev(1));
        assert_eq!(first.sealing_key_id.as_deref(), Some("a"));
        let stored: Vec<u8> = raw
            .query_one(
                "SELECT sealed_private_key FROM certificates WHERE certificate_id='a'",
                &[],
            )
            .await?
            .get(0);
        assert_ne!(stored, original_key);
        assert!(store.publish_certificate(request.clone()).await.is_err());
        let a = store
            .set_tls_binding(bind("A.Example.Test.", rev(0), Some("a")))
            .await?;
        let b = store
            .set_tls_binding(bind("b.example.test", rev(0), Some("a")))
            .await?;
        let found = resolve(&store, "a.example.test").await?;
        assert_eq!(found.view_revision, a.revision);
        match found.value {
            TlsCertificateValue::Found { metadata, bundle } => {
                assert_eq!(metadata, first);
                assert_eq!(bundle.private_key_pkcs8_der(), original_key);
                assert_eq!(bundle.chain_der(), original_chain);
            }
            _ => panic!("expected coherent certificate material"),
        }
        let unchanged = store
            .resolve_tls_certificate(ResolveTlsCertificateRequest {
                hostname: host("a.example.test"),
                known_view_revision: Some(a.revision),
            })
            .await?;
        assert!(matches!(
            unchanged.value,
            TlsCertificateValue::Unchanged { .. }
        ));
        let before = global_revision(&store).await?;
        assert!(
            store
                .publish_certificate(publish("a", 1, &["a.example.test"]))
                .await
                .is_err(),
            "rotation must cover every bound SAN"
        );
        let wrong_key = CertificateBundle::new(
            original_chain.clone(),
            material(&["a.example.test"])
                .private_key_pkcs8_der()
                .to_vec(),
        )?;
        assert!(store
            .publish_certificate(PublishCertificateRequest {
                bundle: wrong_key,
                expected_version: rev(1),
                id: id("a")
            })
            .await
            .is_err());
        assert_eq!(
            global_revision(&store).await?,
            before,
            "validation failure preserves authority revision"
        );
        assert_eq!(
            store.get_certificate_metadata(id("a")).await?,
            Some(first.clone())
        );
        let second = store
            .publish_certificate(publish("a", 1, &["a.example.test", "b.example.test"]))
            .await?;
        assert_eq!(second.version, rev(2));
        let a2 = store.get_tls_binding(host("a.example.test")).await?;
        let b2 = store.get_tls_binding(host("b.example.test")).await?;
        assert!(a2.revision > a.revision && b2.revision > b.revision);
        assert_eq!(a2.revision, b2.revision);
        assert_eq!(a2.last_invalidating_revision, a.last_invalidating_revision);
        assert_eq!(b2.last_invalidating_revision, b.last_invalidating_revision);
        assert!(store
            .set_tls_binding(bind("a.example.test", a.revision, None))
            .await
            .is_err());
        let unbound = store
            .set_tls_binding(bind("a.example.test", a2.revision, None))
            .await?;
        assert!(matches!(
            resolve(&store, "a.example.test").await?.value,
            TlsCertificateValue::Missing
        ));
        assert!(store
            .set_tls_binding(bind("a.example.test", rev(0), Some("a")))
            .await
            .is_err());
        let rebound = store
            .set_tls_binding(bind("a.example.test", unbound.revision, Some("a")))
            .await?;
        assert!(rebound.revision > unbound.revision);
        // Reconnect uses only the separately supplied keyring, not process state.
        let reconnect = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        assert_eq!(
            reconnect.get_tls_binding(host("a.example.test")).await?,
            rebound
        );
        assert!(matches!(
            resolve(&reconnect, "a.example.test").await?.value,
            TlsCertificateValue::Found { .. }
        ));
        let unconfigured = PostgresStore::connect(&config).await?;
        assert!(resolve(&unconfigured, "a.example.test").await.is_err());
        let wrong = store
            .clone()
            .with_certificate_sealer(ring("a", &[("a", 9)]));
        assert!(resolve(&wrong, "a.example.test").await.is_err());
        // Authenticated ciphertext and chain substitution must be errors, not misses.
        raw.execute("UPDATE certificates SET sealed_private_key=set_byte(sealed_private_key,0,get_byte(sealed_private_key,0)#1) WHERE certificate_id='a'",&[]).await?;
        assert!(resolve(&store, "a.example.test").await.is_err());
        raw.execute("UPDATE certificates SET sealed_private_key=set_byte(sealed_private_key,0,get_byte(sealed_private_key,0)#1) WHERE certificate_id='a'",&[]).await?;
        let changed_chain = material(&["a.example.test", "b.example.test"])
            .chain_der()
            .to_vec();
        let old_chain: Vec<Vec<u8>> = raw
            .query_one(
                "SELECT chain_der FROM certificates WHERE certificate_id='a'",
                &[],
            )
            .await?
            .get(0);
        raw.execute(
            "UPDATE certificates SET chain_der=$1 WHERE certificate_id='a'",
            &[&changed_chain],
        )
        .await?;
        assert!(resolve(&store, "a.example.test").await.is_err());
        raw.execute(
            "UPDATE certificates SET chain_der=$1 WHERE certificate_id='a'",
            &[&old_chain],
        )
        .await?;
        let rotated = store
            .clone()
            .with_certificate_sealer(ring("b", &[("a", 7), ("b", 8)]));
        let authority_before = global_revision(&store).await?;
        let reencrypted = rotated
            .reencrypt_certificate(ReencryptCertificateRequest {
                id: id("a"),
                expected_version: second.version,
                expected_sealing_revision: second.sealing_revision,
            })
            .await?;
        assert_eq!(reencrypted.version, second.version);
        assert_eq!(reencrypted.sealing_key_id.as_deref(), Some("b"));
        assert!(reencrypted.sealing_revision > second.sealing_revision);
        assert_eq!(
            global_revision(&store).await?,
            authority_before,
            "storage-key rotation does not change authority"
        );
        assert!(rotated
            .reencrypt_certificate(ReencryptCertificateRequest {
                id: id("a"),
                expected_version: second.version,
                expected_sealing_revision: second.sealing_revision
            })
            .await
            .is_err());
        let new_only = store
            .clone()
            .with_certificate_sealer(ring("b", &[("b", 8)]));
        assert!(matches!(
            resolve(&new_only, "a.example.test").await?.value,
            TlsCertificateValue::Found { .. }
        ));
        assert!(resolve(&store, "a.example.test").await.is_err());
        // Reassignment to a different ID/version 1 cannot compare certificate
        // versions alone: the hostname authority revision must still advance.
        new_only
            .publish_certificate(publish("different", 0, &["a.example.test"]))
            .await?;
        let prior_view = new_only.get_tls_binding(host("a.example.test")).await?;
        let reassigned = new_only
            .set_tls_binding(bind(
                "a.example.test",
                prior_view.revision,
                Some("different"),
            ))
            .await?;
        assert!(reassigned.revision > prior_view.revision);
        let refresh = new_only
            .resolve_tls_certificate(ResolveTlsCertificateRequest {
                hostname: host("a.example.test"),
                known_view_revision: Some(prior_view.revision),
            })
            .await?;
        assert_eq!(refresh.view_revision, reassigned.revision);
        assert!(
            matches!(refresh.value,TlsCertificateValue::Found{metadata,..} if metadata.id==id("different") && metadata.version==rev(1))
        );
        new_only
            .set_tls_binding(bind("a.example.test", reassigned.revision, Some("a")))
            .await?;
        let removed = new_only
            .remove_certificate(RemoveCertificateRequest {
                id: id("a"),
                expected_version: rev(2),
            })
            .await?;
        assert_eq!(removed.state, CertificateState::Deleted);
        assert_eq!(removed.version, rev(3));
        assert!(
            removed.dns_names.is_empty()
                && removed.leaf_sha256.is_empty()
                && removed.sealing_key_id.is_none()
        );
        let row=raw.query_one("SELECT chain_der IS NULL AND sealed_private_key IS NULL AND seal_nonce IS NULL AND seal_key_id IS NULL AND leaf_sha256 IS NULL AS wiped FROM certificates WHERE certificate_id='a'",&[]).await?;
        assert!(row.get::<_, bool>(0));
        let tombstone = new_only.get_tls_binding(host("a.example.test")).await?;
        assert!(tombstone.certificate_id.is_none() && tombstone.revision > rebound.revision);
        assert!(matches!(
            resolve(&new_only, "a.example.test").await?.value,
            TlsCertificateValue::Missing
        ));
        assert!(new_only
            .publish_certificate(publish("a", 0, &["a.example.test"]))
            .await
            .is_err());
        assert!(
            new_only
                .publish_certificate(publish("a", 3, &["a.example.test"]))
                .await
                .is_err(),
            "matching tombstone version cannot revive identity"
        );
        assert!(new_only
            .reencrypt_certificate(ReencryptCertificateRequest {
                id: id("a"),
                expected_version: rev(2),
                expected_sealing_revision: reencrypted.sealing_revision
            })
            .await
            .is_err());
        // Certificate/hostname operations did not manufacture routes or instances.
        assert_eq!(
            raw.query_one(
                "SELECT (SELECT count(*) FROM instances)+(SELECT count(*) FROM route_bindings)",
                &[]
            )
            .await?
            .get::<_, i64>(0),
            0
        );
        Ok(())
    }).await
}

#[tokio::test]
async fn postgres_certificate_rotation_binding_races_and_atomic_views() -> TestResult {
    database_test(|store, raw, config| async move {
        let independent = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        store
            .publish_certificate(publish("race", 0, &["a.example.test", "b.example.test"]))
            .await?;
        store
            .set_tls_binding(bind("a.example.test", rev(0), Some("race")))
            .await?;
        let rotation = store.publish_certificate(publish("race", 1, &["a.example.test"]));
        let binding = independent.set_tls_binding(bind("b.example.test", rev(0), Some("race")));
        let (rotation, binding) = tokio::join!(rotation, binding);
        assert_ne!(
            rotation.is_ok(),
            binding.is_ok(),
            "serialize SAN-loss rotation against binding creation"
        );
        let b = store.get_tls_binding(host("b.example.test")).await?;
        if b.certificate_id.is_some() {
            assert_eq!(
                store
                    .get_certificate_metadata(id("race"))
                    .await?
                    .unwrap()
                    .version,
                rev(1)
            );
            assert!(matches!(
                resolve(&store, "b.example.test").await?.value,
                TlsCertificateValue::Found { .. }
            ));
        } else {
            assert_eq!(
                store
                    .get_certificate_metadata(id("race"))
                    .await?
                    .unwrap()
                    .version,
                rev(2)
            );
        }
        store
            .publish_certificate(publish("atomic", 0, &["atomic.example.test"]))
            .await?;
        let initial = store
            .set_tls_binding(bind("atomic.example.test", rev(0), Some("atomic")))
            .await?;
        let writer = store.clone();
        let reader = independent.clone();
        let (written, read) = tokio::join!(
            async move {
                for version in 1..=10 {
                    writer
                        .publish_certificate(publish("atomic", version, &["atomic.example.test"]))
                        .await?;
                }
                Ok::<_, StoreError>(())
            },
            async move {
                for _ in 0..30 {
                    let value = resolve(&reader, "atomic.example.test").await?;
                    match value.value {
                        TlsCertificateValue::Found { metadata, bundle } => {
                            assert_eq!(
                                value.view_revision.get(),
                                initial.revision.get() + metadata.version.get() - 1,
                                "snapshot must never combine old binding and new certificate"
                            );
                            assert_eq!(metadata.leaf_sha256, ring_digest(&bundle.chain_der()[0]));
                        }
                        _ => panic!("binding cannot disappear during valid rotations"),
                    }
                }
                Ok::<_, StoreError>(())
            }
        );
        written?;
        read?;
        let current = store.get_certificate_metadata(id("atomic")).await?.unwrap();
        let rotated = independent.with_certificate_sealer(ring("b", &[("a", 7), ("b", 8)]));
        let (reencryption, publication) = tokio::join!(
            rotated.reencrypt_certificate(ReencryptCertificateRequest {
                id: id("atomic"),
                expected_version: current.version,
                expected_sealing_revision: current.sealing_revision
            }),
            store.publish_certificate(publish(
                "atomic",
                current.version.get(),
                &["atomic.example.test"]
            ))
        );
        let latest = publication?;
        assert_eq!(latest.version.get(), current.version.get() + 1);
        if let Ok(older) = reencryption {
            assert_eq!(older.version, current.version);
        }
        let found = resolve(&rotated, "atomic.example.test").await?;
        assert!(
            matches!(found.value,TlsCertificateValue::Found{metadata,..} if metadata.version==latest.version)
        );
        let (removal, reencryption) = tokio::join!(
            rotated.remove_certificate(RemoveCertificateRequest {
                id: id("atomic"),
                expected_version: latest.version
            }),
            rotated.reencrypt_certificate(ReencryptCertificateRequest {
                id: id("atomic"),
                expected_version: latest.version,
                expected_sealing_revision: latest.sealing_revision
            })
        );
        assert_eq!(removal?.state, CertificateState::Deleted);
        if let Ok(older) = reencryption {
            assert_eq!(older.version, latest.version);
        }
        assert!(matches!(
            resolve(&rotated, "atomic.example.test").await?.value,
            TlsCertificateValue::Missing
        ));
        assert!(raw
            .query_one(
                "SELECT sealed_private_key IS NULL FROM certificates WHERE certificate_id='atomic'",
                &[]
            )
            .await?
            .get::<_, bool>(0));
        // Preseed an exact bounded inventory; real mutation enforces the limit,
        // and real rotation exercises the maximum fanout without 1024 RPCs.
        store
            .publish_certificate(publish("bulk", 0, &["*.bulk.example.test"]))
            .await?;
        raw.batch_execute("BEGIN; UPDATE tls_certificate_revision SET revision=revision+1 WHERE singleton; INSERT INTO tls_hostname_bindings(hostname,certificate_id,revision,last_invalidating_revision) SELECT 'h'||n||'.bulk.example.test','bulk',revision,revision FROM tls_certificate_revision CROSS JOIN generate_series(1,1024) n WHERE singleton; COMMIT").await?;
        let before_limit = global_revision(&store).await?;
        assert!(matches!(
            store
                .set_tls_binding(bind("extra.bulk.example.test", rev(0), Some("bulk")))
                .await,
            Err(StoreError::InvalidArgument { .. })
        ));
        assert_eq!(global_revision(&store).await?, before_limit);
        assert_eq!(
            store
                .get_tls_binding(host("extra.bulk.example.test"))
                .await?
                .revision,
            rev(0)
        );
        store
            .publish_certificate(publish("bulk", 1, &["*.bulk.example.test"]))
            .await?;
        let full = snapshot(&store, (1..=1024).map(|n| host(&format!("h{n}.bulk.example.test"))).collect(), None).await?.unwrap();
        assert_eq!(full.bindings.len(), 1024);
        assert_eq!(full.revision.get(), before_limit.get() + 1);
        assert!(full.bindings.iter().all(|b| b.revision == full.revision && b.last_invalidating_revision == before_limit));

        Ok(())
    }).await
}
fn ring_digest(bytes: &[u8]) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .to_vec()
}

#[path = "certificates/snapshot.rs"]
mod snapshot;

/// Injects a lost response only after the real database commit; no fake state.
struct LostCertificateResponse {
    inner: PostgresStore,
    calls: AtomicUsize,
}
impl ControlPlaneStore for LostCertificateResponse {
    unexpected_store_methods!(
        get_certificate_metadata,
        get_tls_binding,
        resolve_tls_certificate,
        snapshot_tls_bindings,
        load_route_changes,
        load_route_change_revision,
        load_materialization_work_status,
        record_materialization_failure,
        enqueue_materialization,
        maintain_runtime_records,
        accept_wake,
        request_instance_deletion,
        finalize_instance_deletions,
        create_instance,
        get_instance,
        delete_instance,
        create_workload_class_version,
        load_workload_class_version,
        create_route_binding,
        get_route_binding,
        delete_route_binding,
        list_route_bindings_for_instance,
        resolve_route,
        compare_and_swap_instance_state,
        record_materialization,
        load_ready_materialization,
        load_active_materialization,
        load_materialization,
        complete_wake,
        begin_sleep,
        finalize_sleep,
        load_materialization_operational_metrics,
        claim_next_materialization_reconciliation,
        renew_materialization_reconciliation_lease,
        release_materialization_reconciliation_lease,
        complete_wake_reconciliation,
        finalize_sleep_reconciliation,
        delete_materialization_reconciliation,
        force_delete_materialization,
        force_release_exclusivity_key,
        lookup_route_dependencies,
        put_http01_challenge,
        resolve_http01_challenge,
        delete_http01_challenge,
        expire_http01_challenges,
    );
    fn publish_certificate(
        &self,
        request: PublishCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.publish_certificate(request).await?;
            Err(StoreError::unavailable(
                "response lost after certificate commit",
            ))
        })
    }
    fn set_tls_binding(
        &self,
        request: SetTlsBindingRequest,
    ) -> StoreFuture<'_, StoreResult<TlsBinding>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.set_tls_binding(request).await?;
            Err(StoreError::unavailable(
                "response lost after certificate commit",
            ))
        })
    }
    fn remove_certificate(
        &self,
        request: RemoveCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.remove_certificate(request).await?;
            Err(StoreError::unavailable(
                "response lost after certificate commit",
            ))
        })
    }
    fn reencrypt_certificate(
        &self,
        request: ReencryptCertificateRequest,
    ) -> StoreFuture<'_, StoreResult<CertificateMetadata>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.reencrypt_certificate(request).await?;
            Err(StoreError::unavailable(
                "response lost after certificate commit",
            ))
        })
    }
}
#[tokio::test]
async fn postgres_certificate_committed_response_loss_is_not_replayed() -> TestResult {
    database_test(|store, _raw, _| async move {
        let store = store.with_certificate_sealer(ring("b", &[("a", 7), ("b", 8)]));
        let loss = Arc::new(LostCertificateResponse {
            inner: store.clone(),
            calls: AtomicUsize::new(0),
        });
        let retry = RetryingControlPlaneStore::new(
            loss.clone(),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(2)),
        );
        let request = publish("lost", 0, &["lost.example.test"]);
        let expected_key = request.bundle.private_key_pkcs8_der().to_vec();
        let expected_chain = request.bundle.chain_der().to_vec();
        assert!(matches!(
            retry.publish_certificate(request.clone()).await,
            Err(StoreError::Unavailable { .. })
        ));
        assert_eq!(loss.calls.load(Ordering::SeqCst), 1);
        let original = store.get_certificate_metadata(id("lost")).await?.unwrap();
        assert_eq!(original.version, rev(1));
        assert!(store.publish_certificate(request).await.is_err());
        assert!(matches!(
            retry
                .set_tls_binding(bind("lost.example.test", rev(0), Some("lost")))
                .await,
            Err(StoreError::Unavailable { .. })
        ));
        assert_eq!(loss.calls.load(Ordering::SeqCst), 2);
        let bound = store.get_tls_binding(host("lost.example.test")).await?;
        assert_eq!(bound.certificate_id, Some(id("lost")));
        match resolve(&store, "lost.example.test").await?.value {
            TlsCertificateValue::Found { bundle, .. } => {
                assert!(
                    bundle.chain_der() == expected_chain,
                    "publication forwards complete chain bytes"
                );
                assert!(
                    bundle.private_key_pkcs8_der() == expected_key,
                    "publication forwards private key bytes without exposing them"
                );
            }
            _ => panic!("committed publication must resolve material"),
        }

        assert!(matches!(
            retry
                .reencrypt_certificate(ReencryptCertificateRequest {
                    id: id("lost"),
                    expected_version: original.version,
                    expected_sealing_revision: original.sealing_revision
                })
                .await,
            Err(StoreError::Unavailable { .. })
        ));
        assert_eq!(loss.calls.load(Ordering::SeqCst), 3);
        let reencrypted = store.get_certificate_metadata(id("lost")).await?.unwrap();
        assert!(reencrypted.sealing_revision > original.sealing_revision);
        assert!(matches!(
            retry
                .publish_certificate(publish("lost", 1, &["lost.example.test"]))
                .await,
            Err(StoreError::Unavailable { .. })
        ));
        assert_eq!(loss.calls.load(Ordering::SeqCst), 4);
        let latest = store.get_certificate_metadata(id("lost")).await?.unwrap();
        assert_eq!(latest.version, rev(2));
        assert!(store
            .set_tls_binding(bind("lost.example.test", bound.revision, None))
            .await
            .is_err());
        assert!(matches!(
            retry
                .remove_certificate(RemoveCertificateRequest {
                    id: id("lost"),
                    expected_version: latest.version
                })
                .await,
            Err(StoreError::Unavailable { .. })
        ));
        assert_eq!(loss.calls.load(Ordering::SeqCst), 5);
        assert_eq!(
            store
                .get_certificate_metadata(id("lost"))
                .await?
                .unwrap()
                .state,
            CertificateState::Deleted
        );
        assert!(matches!(
            resolve(&store, "lost.example.test").await?.value,
            TlsCertificateValue::Missing
        ));
        assert!(store
            .publish_certificate(publish("lost", 3, &["lost.example.test"]))
            .await
            .is_err());
        Ok(())
    })
    .await
}

#[path = "certificates/api.rs"]
mod api;
