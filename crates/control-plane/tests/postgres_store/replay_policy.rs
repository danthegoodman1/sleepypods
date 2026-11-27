//! Which store operations repeat after an uncertain answer. The relay loses
//! the answer to each operation's first request; an operation that repeats
//! answers from its second attempt, and one that runs once reports the loss.
use super::*;
use control_plane::{
    certificate::*, materialization::*, runtime_work::RecordMaterializationFailure,
    DeleteHttp01ChallengeRequest, LoadWorkloadClassVersionRequest, MaterializationId, RetryPolicy,
    StoreResult,
};
use std::{future::Future, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Policy {
    Repeats,
    Once,
}

/// Runs `call` while the relay loses the answer to its first request, and
/// checks the outcome against `policy`.
async fn check<T: std::fmt::Debug>(
    relay: &postgres_relay::Relay,
    name: &str,
    policy: Policy,
    call: impl Future<Output = StoreResult<T>>,
) -> TestResult {
    let lost = relay.answers_lost();
    relay.lose_answer_to(b"");
    tokio::pin!(call);
    let result = tokio::select! {
        result = &mut call => result,
        () = relay.wait_until_answer_held() => {
            relay.drop_held_answer();
            call.await
        }
    };
    if relay.answers_lost() != lost + 1 {
        return Err(format!("{name} answered without reaching the database: {result:?}").into());
    }
    let reported = matches!(result, Err(StoreError::Unavailable { .. }));
    let observed = if reported {
        Policy::Once
    } else {
        Policy::Repeats
    };
    if observed != policy {
        return Err(format!("{name} should be {policy:?}, and answered {result:?}").into());
    }
    Ok(())
}

#[tokio::test]
async fn postgres_operations_repeat_only_when_a_repeat_is_safe() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping the replay policy; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result = async {
        let direct = PostgresStore::connect(&PostgresStoreConfig::new(
            connection_url_with_search_path(&base, &schema),
        )?)
        .await?;
        let relay = postgres_relay::Relay::start(&base).await?;
        let mut config =
            PostgresStoreConfig::new(connection_url_with_search_path(&relay.url(&base)?, &schema))?;
        // Certificate work leaves one session for other work, and a returned
        // session holds its certificate slot while it drains.
        config.max_connections = 3;
        let sealer = CertificateSealer::new(
            "a",
            vec![
                SealingKey::new("a", [7; 32])?,
                SealingKey::new("retired", [8; 32])?,
            ],
        )?;
        let store = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(Arc::new(sealer))
            .with_retry_policy(RetryPolicy::new(3, Duration::ZERO, Duration::ZERO));
        policy_checks(&direct, &store, &relay).await
    }
    .await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    admin_task.abort();
    result
}

async fn policy_checks(
    direct: &PostgresStore,
    store: &PostgresStore,
    relay: &postgres_relay::Relay,
) -> TestResult {
    use Policy::{Once, Repeats};
    let class = workload_class("replay-class", 1);
    direct
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let instance = direct
        .create_instance(create_instance_request(
            "replay-instance",
            class.reference.clone(),
            vec![],
        ))
        .await?
        .instance;
    let target = MaterializationTarget::new("replay-cluster", "apps")?;
    // Lifecycle requests name a generation past the instance's, so each
    // reaches the database and finds nothing to change.
    let generation = Generation::new(5);
    let materialization = MaterializationId::new("replay-materialization")?;
    let route = RouteBindingId::new("replay-route")?;
    let identity = http_identity("replay.example", None);
    let now = SystemTime::now();
    let challenge = Http01ChallengeKey::new("challenge.example", "token")?;
    let certificate = CertificateId::new("replay-certificate")?;
    let revision = CertificateRevision::new(1)?;
    let hostname = TlsHostname::new("tls.example")?;
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["tls.example".into()])?;
    let bundle = CertificateBundle::new(vec![cert.der().to_vec()], signing_key.serialize_der())?;

    check(
        relay,
        "publish_certificate",
        Once,
        store.publish_certificate(PublishCertificateRequest {
            id: certificate.clone(),
            expected_version: CertificateRevision::ZERO,
            bundle,
        }),
    )
    .await?;
    check(
        relay,
        "get_certificate_metadata",
        Repeats,
        store.get_certificate_metadata(certificate.clone()),
    )
    .await?;
    check(
        relay,
        "set_tls_binding",
        Once,
        store.set_tls_binding(SetTlsBindingRequest {
            hostname: hostname.clone(),
            expected_revision: revision,
            certificate_id: Some(certificate.clone()),
        }),
    )
    .await?;
    check(
        relay,
        "get_tls_binding",
        Repeats,
        store.get_tls_binding(hostname.clone()),
    )
    .await?;
    check(
        relay,
        "resolve_tls_certificate",
        Repeats,
        store.resolve_tls_certificate(ResolveTlsCertificateRequest {
            hostname: hostname.clone(),
            known_view_revision: None,
        }),
    )
    .await?;
    check(
        relay,
        "reencrypt_certificate",
        Once,
        store.reencrypt_certificate(ReencryptCertificateRequest {
            id: certificate.clone(),
            expected_version: revision,
            expected_sealing_revision: revision,
        }),
    )
    .await?;
    check(
        relay,
        "reencrypt_certificates_sealed_by",
        Once,
        store.reencrypt_certificates_sealed_by("retired".into()),
    )
    .await?;
    check(
        relay,
        "remove_certificate",
        Once,
        store.remove_certificate(RemoveCertificateRequest {
            id: certificate,
            expected_version: revision,
        }),
    )
    .await?;
    check(
        relay,
        "snapshot_tls_bindings",
        Repeats,
        store.snapshot_tls_bindings(vec![hostname], None),
    )
    .await?;

    check(
        relay,
        "load_route_changes",
        Repeats,
        store.load_route_changes(0, 10),
    )
    .await?;
    check(
        relay,
        "load_route_change_revision",
        Repeats,
        store.load_route_change_revision(),
    )
    .await?;
    check(
        relay,
        "maintain_runtime_records",
        Repeats,
        store.maintain_runtime_records(10),
    )
    .await?;
    check(
        relay,
        "load_materialization_operational_metrics",
        Repeats,
        store.load_materialization_operational_metrics(),
    )
    .await?;

    check(
        relay,
        "get_instance",
        Repeats,
        store.get_instance(GetInstanceRequest::new(instance.id.clone())),
    )
    .await?;
    check(
        relay,
        "load_workload_class_version",
        Repeats,
        store.load_workload_class_version(LoadWorkloadClassVersionRequest::new(
            WorkloadClassVersionRef::new(
                WorkloadClassId::new("uncached-class")?,
                Generation::new(1),
            ),
        )),
    )
    .await?;
    check(
        relay,
        "load_active_materialization",
        Repeats,
        store.load_active_materialization(LoadActiveMaterializationRequest::new(
            instance.id.clone(),
            target.clone(),
        )),
    )
    .await?;
    check(
        relay,
        "load_materialization_work_status",
        Repeats,
        store.load_materialization_work_status(materialization.clone()),
    )
    .await?;
    check(
        relay,
        "load_ready_materialization",
        Repeats,
        store.load_ready_materialization(LoadReadyMaterializationRequest::new(
            instance.id.clone(),
            generation,
            target.clone(),
        )),
    )
    .await?;
    check(
        relay,
        "accept_wake",
        Repeats,
        store.accept_wake(accept_request(&instance, &target, vec![], vec![])),
    )
    .await?;
    check(
        relay,
        "begin_sleep",
        Repeats,
        store.begin_sleep(BeginSleepRequest::new(
            instance.id.clone(),
            generation,
            target.clone(),
        )),
    )
    .await?;

    check(
        relay,
        "claim_next_materialization_reconciliation",
        Once,
        store.claim_next_materialization_reconciliation(ClaimNextMaterializationRequest::new(
            MaterializationState::Pending,
            target.clone(),
            "claim-owner",
            Duration::from_secs(30),
        )),
    )
    .await?;
    check(
        relay,
        "renew_materialization_reconciliation_lease",
        Repeats,
        store.renew_materialization_reconciliation_lease(
            RenewMaterializationReconciliationLeaseRequest::new(
                materialization.clone(),
                "owner",
                1,
                generation,
                Duration::from_secs(30),
                MaterializationState::Pending,
            ),
        ),
    )
    .await?;
    check(
        relay,
        "release_materialization_reconciliation_lease",
        Repeats,
        store.release_materialization_reconciliation_lease(
            ReleaseMaterializationReconciliationLeaseRequest::new(
                materialization.clone(),
                "owner",
                1,
                generation,
            ),
        ),
    )
    .await?;
    check(
        relay,
        "complete_wake_reconciliation",
        Repeats,
        store.complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            materialization.clone(),
            "owner",
            1,
            CompleteWakeRequest::new(
                instance.id.clone(),
                generation,
                target.clone(),
                BackendEndpoint::new("http://10.0.0.1:8080")?,
                BackendGeneration::new(1),
            ),
        )),
    )
    .await?;
    check(
        relay,
        "finalize_sleep_reconciliation",
        Repeats,
        store.finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
            materialization.clone(),
            "owner",
            1,
            FinalizeSleepRequest::new(instance.id.clone(), generation, target.clone()),
        )),
    )
    .await?;
    check(
        relay,
        "delete_materialization_reconciliation",
        Repeats,
        store.delete_materialization_reconciliation(
            DeleteMaterializationReconciliationRequest::new(
                materialization.clone(),
                "owner",
                1,
                MaterializationState::Deleting,
                instance.id.clone(),
                generation,
                target.clone(),
            ),
        ),
    )
    .await?;
    check(
        relay,
        "record_materialization_failure",
        Repeats,
        store.record_materialization_failure(RecordMaterializationFailure {
            expected_state: MaterializationState::Pending,
            materialization_id: materialization.clone(),
            owner: "owner".into(),
            attempt: 1,
            generation,
            permanent: false,
            message: "failure".into(),
        }),
    )
    .await?;
    check(
        relay,
        "finalize_instance_deletions",
        Repeats,
        store.finalize_instance_deletions(10),
    )
    .await?;

    check(
        relay,
        "create_workload_class_version",
        Repeats,
        store.create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            workload_class("replay-created-class", 1),
        )),
    )
    .await?;
    check(
        relay,
        "create_instance",
        Once,
        store.create_instance(create_instance_request(
            "replay-created",
            class.reference.clone(),
            vec![],
        )),
    )
    .await?;
    check(
        relay,
        "request_instance_deletion",
        Repeats,
        store.request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
            instance_id: InstanceId::new("replay-absent")?,
            expected_generation: generation,
        }),
    )
    .await?;
    check(
        relay,
        "create_route_binding",
        Once,
        store.create_route_binding(CreateRouteBindingRequest::new(
            route.clone(),
            instance.id.clone(),
            identity.clone(),
        )),
    )
    .await?;
    check(
        relay,
        "get_route_binding",
        Repeats,
        store.get_route_binding(GetRouteBindingRequest::new(route.clone())),
    )
    .await?;
    check(
        relay,
        "delete_route_binding",
        Once,
        store.delete_route_binding(DeleteRouteBindingRequest::new(route)),
    )
    .await?;
    check(
        relay,
        "load_materialization",
        Repeats,
        store.load_materialization(LoadMaterializationRequest::new(materialization.clone())),
    )
    .await?;
    check(
        relay,
        "enqueue_materialization",
        Once,
        store.enqueue_materialization(materialization.clone()),
    )
    .await?;
    check(
        relay,
        "force_delete_materialization",
        Once,
        store.force_delete_materialization(ForceDeleteMaterializationRequest::new(
            materialization,
            "operator",
            "fenced old process",
        )),
    )
    .await?;
    check(
        relay,
        "force_release_exclusivity_key",
        Once,
        store.force_release_exclusivity_key(ForceReleaseExclusivityKeyRequest::new(
            target.clone(),
            "storage",
            "disk",
            "operator",
            "fenced old process",
        )),
    )
    .await?;
    check(
        relay,
        "put_http01_challenge",
        Once,
        store.put_http01_challenge(PutHttp01ChallengeRequest::new(
            challenge.clone(),
            "authorization",
            now + Duration::from_secs(300),
            now,
        )?),
    )
    .await?;
    check(
        relay,
        "resolve_http01_challenge",
        Repeats,
        store.resolve_http01_challenge(challenge.clone()),
    )
    .await?;
    check(
        relay,
        "delete_http01_challenge",
        Once,
        store.delete_http01_challenge(DeleteHttp01ChallengeRequest::new(challenge)),
    )
    .await?;
    check(
        relay,
        "resolve_host_rules",
        Repeats,
        store.resolve_host_rules(ResolveHostRulesRequest::new(
            control_plane::RouteKind::Http,
            RouteHost::exact("replay.example")?,
            target,
        )),
    )
    .await?;
    Ok(())
}
