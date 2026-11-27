//! The real database owns activation age. Backdating test-only state timestamps
//! exercises deadline boundaries without a 190-second wall-clock sleep.
use super::*;
#[path = "../support/ready_age_fixture.rs"]
mod ready_age_fixture;

#[tokio::test]
async fn activation_floor_is_atomic_generation_fenced_and_survives_reconnect() -> TestResult {
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping activation floor: run scripts/test-postgres-store.sh");
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base_url, NoTls).await?;
    let task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
    let store = PostgresStore::connect(&config).await?;
    let (raw, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let raw_task = tokio::spawn(connection);
    let result = check_activation_floor(&store, &config, &raw).await;
    drop(store);
    drop(raw);
    raw_task.abort();
    let _ = raw_task.await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    task.abort();
    let _ = task.await;
    result
}

async fn ready(
    store: &PostgresStore,
    class: &WorkloadClassVersion,
    target: &MaterializationTarget,
    name: &str,
) -> TestResult<control_plane::CompleteWakeResult> {
    let instance = store
        .create_instance(create_instance_request(
            name,
            class.reference.clone(),
            vec![],
        ))
        .await?
        .instance;
    Ok(wake_to_running(store, instance.id, target, vec![], vec![]).await?)
}

async fn check_activation_floor(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
    raw: &tokio_postgres::Client,
) -> TestResult {
    store.run_migrations().await?;
    let class = workload_class("activation", 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let target = MaterializationTarget::new("activation", "apps")?;
    let ready = ready(store, &class, &target, "protected").await?;
    let request = BeginSleepRequest::new(
        ready.instance.id.clone(),
        ready.instance.generation,
        target.clone(),
    )
    .with_minimum_ready_age(sleepypods_api::INITIAL_ACTIVATION_TIMEOUT);
    let cursor = store.load_route_change_revision().await?;
    assert!(
        matches!(store.begin_sleep(request.clone()).await, Err(StoreError::SleepDeferred { retry_after }) if retry_after > Duration::from_secs(180))
    );
    let first_ready_at: i64 = raw.query_one("SELECT state_entered_at_unix_millis FROM materializations WHERE materialization_id = $1", &[&ready.materialization.id.as_str()]).await?.get(0);
    assert_eq!(
        store.load_route_change_revision().await?,
        cursor,
        "deferral commits no state/outbox change"
    );
    assert_eq!(
        store
            .get_instance(GetInstanceRequest::new(ready.instance.id.clone()))
            .await?
            .unwrap()
            .state,
        InstanceState::Running
    );
    let restarted = PostgresStore::connect(config).await?;
    assert!(
        matches!(restarted.begin_sleep(request.clone()).await,
        Err(StoreError::SleepDeferred { retry_after }) if retry_after > Duration::from_secs(180)),
        "a fresh store/process must not restart the activation clock"
    );

    // An admin enqueue of a Ready row is not lifecycle work and moves nothing.
    assert!(
        !store
            .enqueue_materialization(ready.materialization.id.clone())
            .await?,
        "a Ready row is not lifecycle work"
    );
    assert_eq!(raw.query_one("SELECT state_entered_at_unix_millis FROM materializations WHERE materialization_id = $1", &[&ready.materialization.id.as_str()]).await?.get::<_, i64>(0), first_ready_at, "deferrals, an enqueue and a reconnect do not move the persisted deadline");
    let bad_generation = BeginSleepRequest::new(
        ready.instance.id.clone(),
        Generation::new(0),
        target.clone(),
    )
    .with_minimum_ready_age(Duration::ZERO);
    assert!(matches!(
        store.begin_sleep(bad_generation).await,
        Err(StoreError::GenerationConflict { .. })
    ));
    // The deployed synthetic membership cases share this deliberately narrow
    // fixture SQL. Wrong state/target/generation/projection must change nothing.
    for (cluster, namespace, generation, projection) in [
        (
            "wrong-cluster",
            "apps",
            ready.instance.generation.get(),
            ready.materialization.projection_generation.get(),
        ),
        (
            "activation",
            "wrong-namespace",
            ready.instance.generation.get(),
            ready.materialization.projection_generation.get(),
        ),
        (
            "activation",
            "apps",
            0,
            ready.materialization.projection_generation.get(),
        ),
        ("activation", "apps", ready.instance.generation.get(), 0),
    ] {
        let sql = ready_age_fixture::age_ready_sql(
            cluster,
            namespace,
            ready.instance.id.as_str(),
            generation,
            projection,
        )?;
        assert!(raw.batch_execute(&sql).await.is_err());
        assert_eq!(raw.query_one("SELECT state_entered_at_unix_millis FROM materializations WHERE materialization_id = $1", &[&ready.materialization.id.as_str()]).await?.get::<_, i64>(0), first_ready_at);
    }
    assert!(ready_age_fixture::age_ready_sql("bad'cluster", "apps", "instance", 1, 1).is_err());
    let sql = ready_age_fixture::age_ready_sql(
        "activation",
        "apps",
        ready.instance.id.as_str(),
        ready.instance.generation.get(),
        ready.materialization.projection_generation.get(),
    )?;
    raw.batch_execute(&sql).await?;
    // Keep the exact activation-boundary proof distinct from the lifecycle
    // fixture's longer controlled age.
    raw.execute("UPDATE materializations SET state_entered_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint - 190001 WHERE materialization_id = $1", &[&ready.materialization.id.as_str()]).await?;
    let (left, right) = tokio::join!(
        store.begin_sleep(request.clone()),
        restarted.begin_sleep(request)
    );
    assert_eq!(
        usize::from(left.is_ok()) + usize::from(right.is_ok()),
        1,
        "only one automatic sleep may cross the eligible generation"
    );
    let conflict = match left {
        Err(error) => error,
        Ok(_) => right.unwrap_err(),
    };
    assert!(matches!(conflict, StoreError::GenerationConflict { .. }));

    let manual = self::ready(store, &class, &target, "manual").await?;
    store
        .begin_sleep(BeginSleepRequest::new(
            manual.instance.id,
            manual.instance.generation,
            target.clone(),
        ))
        .await?;
    let longer = self::ready(store, &class, &target, "long-idle").await?;
    raw.execute("UPDATE materializations SET state_entered_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint - 190001 WHERE materialization_id = $1", &[&longer.materialization.id.as_str()]).await?;
    assert!(
        matches!(
            store
                .begin_sleep(
                    BeginSleepRequest::new(longer.instance.id, longer.instance.generation, target)
                        .with_minimum_ready_age(Duration::from_secs(300))
                )
                .await,
            Err(StoreError::SleepDeferred { .. })
        ),
        "a longer class idle floor is retained"
    );
    Ok(())
}
