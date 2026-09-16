//! Process loss at each exclusivity-lock boundary, against real PostgreSQL.
//!
//! A control plane that dies mid-sequence leaves its reconciliation lease behind
//! with no owner to renew it. These tests reproduce that by claiming a lease and
//! then expiring it in the database, which is what the row looks like once the
//! process holding it is gone.
//!
//! One invariant runs through all three: an exclusivity key stays held for as
//! long as a durable materialization row survives, so a crash never hands the
//! key to a second instance, and recovery is always another owner claiming the
//! same work rather than the key changing hands.
use super::*;

/// Where the process dies relative to the Kubernetes objects it owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashPoint {
    /// The key is acquired and the row recorded; nothing is applied yet.
    BeforeApply,
    /// Objects are applied and recorded; the row has not reached Ready.
    AfterApplyBeforeReady,
    /// The row is being torn down and its objects are partly gone.
    DuringCleanup,
}

#[tokio::test]
async fn postgres_crash_at_each_lock_boundary_keeps_the_key_and_yields_to_recovery() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping crash boundary coverage; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    for point in [
        CrashPoint::BeforeApply,
        CrashPoint::AfterApplyBeforeReady,
        CrashPoint::DuringCleanup,
    ] {
        let schema = unique_schema_name();
        let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
        let admin_task = tokio::spawn(connection);
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await?;
        let config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
        let store = PostgresStore::connect(&config).await?;
        // Expiring a lease needs a connection inside the test's schema; the
        // admin connection sits outside it.
        let (raw, raw_connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let raw_task = tokio::spawn(raw_connection);
        let result = crash_boundary(&store, &raw, point).await;
        raw_task.abort();
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await?;
        admin_task.abort();
        result?;
    }
    Ok(())
}

async fn crash_boundary(
    store: &PostgresStore,
    raw: &tokio_postgres::Client,
    point: CrashPoint,
) -> TestResult {
    let workload_class = exclusive_workload_class();
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            workload_class.clone(),
        ))
        .await?;
    let target = MaterializationTarget::new("cluster-crash", "apps").expect("valid target");

    let owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-crash-owner",
        "instance-crash-owner",
        "disk-crash",
        "license-crash",
    )
    .await?;
    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;

    // Acquiring the key is recording the row, so every crash point is reached
    // with the key already held.
    let mut pending = RecordMaterializationRequest::new(
        owner.instance.id.clone(),
        waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pending.exclusivity_keys = workload_class
        .render_exclusivity_keys(&waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    if point != CrashPoint::BeforeApply {
        // An owner that reached the apply recorded what it created, so recovery
        // inherits the inventory it has to finish or tear down.
        pending.rendered_objects = vec![RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: "StatefulSet".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-crash-owner".to_owned(),
        }];
    }
    let record = store.record_materialization(pending).await?;
    assert_eq!(
        record.rendered_objects.is_empty(),
        point == CrashPoint::BeforeApply,
        "{point:?}: the row carries the inventory the owner had reached"
    );

    if point == CrashPoint::DuringCleanup {
        assert!(
            store
                .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                    instance_id: owner.instance.id.clone(),
                    expected_generation: record.instance_generation,
                })
                .await?,
            "deletion is accepted while the key is held"
        );
    }

    // The process claims the work, then dies. An abandoned lease is one whose
    // owner stopped renewing it.
    let claimed = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            record.id.clone(),
            "crashed-owner",
            Duration::from_secs(30),
        ))
        .await?
        .expect("the crashed owner claims the work first");
    assert_eq!(claimed.id, record.id);
    raw.execute(
        "UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 0 \
             WHERE materialization_id = $1",
        &[&record.id.as_str()],
    )
    .await?;

    // The key outlives the process that took it.
    let contender = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-crash-contender",
        "instance-crash-contender",
        "disk-crash",
        "license-contender",
    )
    .await?;
    let contender_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            contender.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut contender_pending = RecordMaterializationRequest::new(
        contender.instance.id.clone(),
        contender_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    contender_pending.exclusivity_keys = workload_class
        .render_exclusivity_keys(&contender_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let conflict = store
        .record_materialization(contender_pending)
        .await
        .expect_err("a crashed owner still holds its key");
    assert_exclusivity_conflict(conflict, "disk", Some("instance-crash-owner"));

    // Recovery is another owner claiming the same work.
    let recovered = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            record.id.clone(),
            "recovery-owner",
            Duration::from_secs(30),
        ))
        .await?
        .expect("an expired lease lets a fresh owner claim the work");
    let lease = recovered
        .reconciliation_lease
        .expect("the recovered claim carries a lease");
    assert!(
        lease.attempt > claimed.reconciliation_lease.expect("first lease").attempt,
        "{point:?}: recovery counts as a later attempt"
    );

    Ok(())
}
