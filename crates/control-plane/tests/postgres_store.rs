#[path = "support/postgres_relay.rs"]
mod postgres_relay;
#[path = "support/mod.rs"]
mod transport_support;
use std::{
    collections::BTreeMap,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::materialization::{
    LoadActiveMaterializationRequest, LoadMaterializationRequest, LoadReadyMaterializationRequest,
};
use control_plane::{
    render_manifests, BackendEndpoint, BackendGeneration, BeginSleepRequest,
    ClaimNextMaterializationRequest, CompleteWakeReconciliationRequest, CompleteWakeRequest,
    ContainerPortTemplate, ContainerTemplate, ControlPlaneStore, CreateInstanceRequest,
    CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    DeleteMaterializationReconciliationRequest, DeleteRouteBindingRequest, EnvVarTemplate,
    FinalizeSleepReconciliationRequest, FinalizeSleepRequest, ForceReleaseExclusivityKeyRequest,
    Generation, GetInstanceRequest, GetRouteBindingRequest, Http01ChallengeKey,
    IdleTimeoutOverridePolicy, InstanceId, InstanceRoute, InstanceState, ManifestTemplate,
    MaterializationState, MaterializationTarget, PathPrefix, PostgresStore, PostgresStoreConfig,
    PutHttp01ChallengeRequest, RecordMaterializationRequest,
    ReleaseMaterializationReconciliationLeaseRequest, RenderManifestRequest,
    RenderedExclusivityKey, RenderedObjectRef, RenewMaterializationReconciliationLeaseRequest,
    ResolveHostRulesRequest, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
    ServicePortTemplate, ServiceTemplate, SidecarTemplate, StoreError, TemplateText,
    TemplateTextPart, WorkloadClassId, WorkloadClassVersion, WorkloadClassVersionRef,
    WorkloadExclusivityKeyTemplate, WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate,
    WorkloadValueFieldRule, WorkloadValueSchema,
};
use control_plane::{
    CertificateStore, LifecycleReads, OperatorStore, ReconcilerStore, RouteStore, RuntimeStore,
    SleepStore, TlsWatchStore, WakeStore,
};
use tokio_postgres::NoTls;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

/// Claims the next due row of `state` in `target`. A scenario keeps at most
/// one such row per target, so this claims that row or nothing.
async fn claim_next(
    store: &(impl ControlPlaneStore + ?Sized),
    state: MaterializationState,
    target: &MaterializationTarget,
    owner: &str,
    lease_ttl: Duration,
) -> Result<Option<control_plane::MaterializationRecord>, StoreError> {
    store
        .claim_next_materialization_reconciliation(ClaimNextMaterializationRequest::new(
            state,
            target.clone(),
            owner,
            lease_ttl,
        ))
        .await
}

/// A wake of the Cold or Failed `instance` into `target`, whose projection
/// holds `refs` and `keys`.
fn accept_request(
    instance: &control_plane::InstanceRecord,
    target: &MaterializationTarget,
    refs: Vec<RenderedObjectRef>,
    keys: Vec<RenderedExclusivityKey>,
) -> control_plane::materialization::AcceptWakeRequest {
    let mut pending = RecordMaterializationRequest::new(
        instance.id.clone(),
        instance.generation.next(),
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pending.rendered_objects = refs;
    pending.exclusivity_keys = keys;
    control_plane::materialization::AcceptWakeRequest {
        expected_generation: instance.generation,
        pending,
    }
}

/// Accepts a wake of the Cold or Failed `instance` into `target`. The
/// instance becomes Waking with a Pending row that holds `refs` and `keys`.
async fn accept_projection(
    store: &(impl ControlPlaneStore + ?Sized),
    instance: &control_plane::InstanceRecord,
    target: &MaterializationTarget,
    refs: Vec<RenderedObjectRef>,
    keys: Vec<RenderedExclusivityKey>,
) -> Result<control_plane::MaterializationRecord, StoreError> {
    store
        .accept_wake(accept_request(instance, target, refs, keys))
        .await?;
    Ok(store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            instance.id.clone(),
            target.clone(),
        ))
        .await?
        .expect("an accepted wake has a Pending row"))
}

/// Claims `row` for `owner`. A claim takes its target's longest-waiting due
/// row, so rows ahead of `row` are claimed and released, which moves them
/// behind it.
async fn claim_row(
    store: &(impl ControlPlaneStore + ?Sized),
    row: &control_plane::MaterializationRecord,
    owner: &str,
) -> Result<control_plane::MaterializationRecord, StoreError> {
    for _ in 0..64 {
        let claimed = claim_next(
            store,
            row.state,
            &row.target,
            owner,
            Duration::from_secs(60),
        )
        .await?
        .ok_or_else(|| StoreError::internal("the row is not due"))?;
        if claimed.id == row.id {
            return Ok(claimed);
        }
        store
            .release_materialization_reconciliation_lease(
                ReleaseMaterializationReconciliationLeaseRequest::new(
                    claimed.id.clone(),
                    owner,
                    claimed.reconciliation_lease.as_ref().unwrap().attempt,
                    claimed.instance_generation,
                ),
            )
            .await?;
    }
    Err(StoreError::internal("the row never came due"))
}

/// Completes the wake of `pending` at `backend` as a driver does: it claims
/// the row and completes it under that lease.
async fn complete_wake_of(
    store: &(impl ControlPlaneStore + ?Sized),
    pending: &control_plane::MaterializationRecord,
    backend: &str,
) -> Result<control_plane::CompleteWakeResult, StoreError> {
    let claimed = claim_row(store, pending, "driver").await?;
    store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            claimed.reconciliation_lease.as_ref().unwrap().attempt,
            complete_for_reconciled_pending(&claimed, backend),
        ))
        .await
}

/// Finishes the drain of `draining`, whose projection is `deleting`, as a
/// driver does: it claims the row and finalizes the sleep under that lease.
async fn finish_sleep_of(
    store: &(impl ControlPlaneStore + ?Sized),
    deleting: &control_plane::MaterializationRecord,
    draining: &control_plane::InstanceRecord,
) -> Result<control_plane::FinalizeSleepResult, StoreError> {
    let claimed = claim_row(store, deleting, "driver").await?;
    store
        .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            claimed.reconciliation_lease.as_ref().unwrap().attempt,
            FinalizeSleepRequest::new(
                draining.id.clone(),
                draining.generation,
                claimed.target.clone(),
            ),
        ))
        .await
}

/// Wakes the Cold instance `instance_id` into `target` and completes the wake,
/// with `refs` and `keys` in its projection.
async fn wake_to_running(
    store: &(impl ControlPlaneStore + ?Sized),
    instance_id: InstanceId,
    target: &MaterializationTarget,
    refs: Vec<RenderedObjectRef>,
    keys: Vec<RenderedExclusivityKey>,
) -> Result<control_plane::CompleteWakeResult, StoreError> {
    let instance = store
        .get_instance(GetInstanceRequest::new(instance_id))
        .await?
        .ok_or(StoreError::NotFound {
            resource: "instance",
        })?;
    let pending = accept_projection(store, &instance, target, refs, keys).await?;
    complete_wake_of(store, &pending, "http://10.0.0.1:8080").await
}

#[tokio::test]
async fn postgres_provider_reports_invalid_connection_url() {
    let config = PostgresStoreConfig::new("http://example.com/not-postgres")
        .expect("non-empty URL reaches provider validation");
    let error = PostgresStore::connect(&config)
        .await
        .expect_err("provider rejects invalid Postgres URL");

    assert!(matches!(error, StoreError::InvalidArgument { .. }));
}

#[tokio::test]
async fn postgres_store_conformance_against_real_database() -> TestResult {
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!(
            "skipping Postgres store conformance; run with \
             SLEEPYPODS_POSTGRES_URL=postgres://user:pass@localhost/db \
             cargo test --package control-plane --test postgres_store -- --nocapture"
        );
        return Ok(());
    };

    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base_url, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("Postgres admin connection error: {error}");
        }
    });

    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;

    let store_url = connection_url_with_search_path(&base_url, &schema);
    let config = PostgresStoreConfig::new(store_url)?;
    let store = PostgresStore::connect(&config).await?;
    let result = run_conformance(&store, &config).await;

    drop(store);
    let cleanup = admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    drop(admin);
    connection_task.abort();

    cleanup?;
    result.map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
}

async fn run_conformance(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
) -> Result<(), StoreError> {
    store.run_migrations().await?;
    store.run_migrations().await?;

    let class = workload_class("class-a", 1);
    let created_class = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    assert_eq!(created_class, class);
    let loaded = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            class.reference.clone(),
        ))
        .await?
        .expect("created workload class version loads");
    assert_eq!(loaded, class);
    let duplicate = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    assert_eq!(duplicate, class);

    let mut changed_class = class.clone();
    changed_class.value_schema.allow_extra = !changed_class.value_schema.allow_extra;
    let conflict = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(changed_class))
        .await
        .expect_err("same class/version with different contents is immutable");
    assert!(matches!(
        conflict,
        StoreError::AlreadyExists {
            resource: "workload class version"
        }
    ));
    let loaded_v1_after_conflict = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            class.reference.clone(),
        ))
        .await?
        .expect("v1 still loads after conflicting create");
    assert_eq!(loaded_v1_after_conflict, class);

    let mut changed_template_class = class.clone();
    changed_template_class.template.workload.app_container.image =
        TemplateText::literal("example/app:changed");
    let template_conflict = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            changed_template_class,
        ))
        .await
        .expect_err("same class/version with a different template is immutable");
    assert!(matches!(
        template_conflict,
        StoreError::AlreadyExists {
            resource: "workload class version"
        }
    ));

    let mut changed_policy_class = class.clone();
    changed_policy_class.sleep_policy =
        WorkloadSleepPolicy::new(120_000, 5_000, 30_000).expect("valid sleep policy");
    let policy_conflict = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(changed_policy_class))
        .await
        .expect_err("same class/version with a different sleep policy is immutable");
    assert!(matches!(
        policy_conflict,
        StoreError::AlreadyExists {
            resource: "workload class version"
        }
    ));

    let class_v2 = workload_class("class-a", 2);
    let created_v2 = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class_v2.clone()))
        .await?;
    assert_eq!(created_v2, class_v2);
    let loaded_v1_after_v2 = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            class.reference.clone(),
        ))
        .await?
        .expect("v1 still loads after v2 is created");
    assert_eq!(loaded_v1_after_v2, class);

    let missing_required = store
        .create_instance(CreateInstanceRequest::new(
            InstanceId::new("instance-missing-required").expect("valid instance ID"),
            class.reference.clone(),
        ))
        .await
        .expect_err("missing required instance values are rejected");
    assert!(matches!(
        missing_required,
        StoreError::InvalidArgument { .. }
    ));

    let unknown_value = store
        .create_instance(
            create_instance_request("instance-unknown-value", class.reference.clone(), vec![])
                .with_values(BTreeMap::from([
                    ("extra".to_owned(), "value".to_owned()),
                    ("tenant".to_owned(), "instance-unknown-value".to_owned()),
                ])),
        )
        .await
        .expect_err("unknown instance values are rejected when schema disallows them");
    assert!(matches!(unknown_value, StoreError::InvalidArgument { .. }));

    let override_class = workload_class_with_idle_override("class-override", 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            override_class.clone(),
        ))
        .await?;
    let out_of_bounds_override = store
        .create_instance(
            create_instance_request(
                "instance-override-out-of-bounds",
                override_class.reference.clone(),
                vec![],
            )
            .with_values(BTreeMap::from([
                (
                    "tenant".to_owned(),
                    "instance-override-out-of-bounds".to_owned(),
                ),
                ("idle_ms".to_owned(), "50000".to_owned()),
            ])),
        )
        .await
        .expect_err("out-of-bounds idle override is rejected before persistence");
    assert!(matches!(
        out_of_bounds_override,
        StoreError::InvalidArgument { .. }
    ));

    let valid_override = store
        .create_instance(
            create_instance_request(
                "instance-override-valid",
                override_class.reference.clone(),
                vec![],
            )
            .with_values(BTreeMap::from([
                ("tenant".to_owned(), "instance-override-valid".to_owned()),
                ("idle_ms".to_owned(), "120000".to_owned()),
            ])),
        )
        .await?;
    let resolved_override = override_class
        .sleep_policy
        .resolve(&valid_override.instance.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    assert_eq!(resolved_override.idle_timeout_ms, 120_000);

    let create = create_instance_request(
        "instance-a",
        class.reference.clone(),
        vec![
            http_identity("app.example.com", Some("/api")),
            sni_identity("db.example.com"),
        ],
    );
    let created = store.create_instance(create.clone()).await?;
    assert_eq!(created.instance.id.as_str(), "instance-a");
    assert_eq!(created.instance.state, InstanceState::Cold);
    assert_eq!(created.instance.generation, Generation::new(0));
    assert_eq!(
        created.instance.values,
        BTreeMap::from([
            ("image".to_owned(), "example/app:1".to_owned()),
            ("tenant".to_owned(), "instance-a".to_owned()),
        ])
    );
    assert_eq!(created.route_bindings.len(), 2);
    let loaded_created = store
        .get_instance(GetInstanceRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
        ))
        .await?
        .expect("created instance loads through public store API");
    assert_eq!(loaded_created, created.instance);
    let rendered = render_manifests(RenderManifestRequest {
        template: &loaded.template,
        instance: &created.instance,
        sleep_policy: loaded
            .sleep_policy
            .resolve(&created.instance.values)
            .map_err(|error| StoreError::internal(error.to_string()))?,
        namespace: "apps",
    })
    .map_err(|error| StoreError::internal(error.to_string()))?;
    let rendered_names = rendered
        .objects
        .iter()
        .map(|object| (object.object.kind(), object.object.name().to_owned()))
        .collect::<Vec<_>>();
    assert!(
        rendered_names
            .iter()
            .any(|(kind, name)| *kind == "Deployment" && name == "app-instance-a-69856ec0faae6daf"),
        "loaded workload class template should render durable instance Deployment, got {rendered_names:?}"
    );
    assert!(
        rendered_names
            .iter()
            .any(|(kind, name)| *kind == "Service" && name == "svc-instance-a-69856ec0faae6daf"),
        "loaded workload class template should render durable instance Service, got {rendered_names:?}"
    );

    // A repeat that describes the stored instance returns it, whether the
    // request omits a defaulted value or names it.
    assert_eq!(store.create_instance(create.clone()).await?, created);
    let explicit_default = create.clone().with_values(BTreeMap::from([
        ("image".to_owned(), "example/app:1".to_owned()),
        ("tenant".to_owned(), "instance-a".to_owned()),
    ]));
    assert_eq!(store.create_instance(explicit_default).await?, created);

    // Any other request for the ID is refused and changes nothing.
    let different_requests = [
        create.clone().with_values(BTreeMap::from([(
            "tenant".to_owned(),
            "different-tenant".to_owned(),
        )])),
        create_instance_request("instance-a", class_v2.reference.clone(), vec![])
            .with_route_bindings(create.route_bindings.clone()),
        create_instance_request("instance-a", class.reference.clone(), vec![]),
        create_instance_request(
            "instance-a",
            class.reference.clone(),
            vec![http_identity("app.example.com", Some("/api"))],
        ),
        create_instance_request(
            "instance-a",
            class.reference.clone(),
            vec![
                http_identity("app.example.com", Some("/api")),
                sni_identity("db.example.com"),
                http_identity("extra.example.com", None),
            ],
        ),
        create_instance_request(
            "instance-a",
            class.reference.clone(),
            vec![
                http_identity("other.example.com", Some("/api")),
                sni_identity("db.example.com"),
            ],
        ),
    ];
    for different in different_requests {
        let refused = store
            .create_instance(different.clone())
            .await
            .expect_err("a different request for an existing instance ID is refused");
        assert!(
            matches!(
                refused,
                StoreError::AlreadyExists {
                    resource: "instance"
                }
            ),
            "{different:?} returned {refused:?}"
        );
    }
    assert_eq!(store.create_instance(create.clone()).await?, created);
    for host in ["extra.example.com", "other.example.com"] {
        assert_eq!(
            resolve_route(
                store,
                http_identity(host, Some("/api")),
                MaterializationTarget::new("cluster-a", "default").unwrap(),
            )
            .await?,
            None,
            "a refused create binds none of its routes"
        );
    }

    let duplicate_route = store
        .create_instance(create_instance_request(
            "instance-rollback",
            class.reference.clone(),
            vec![http_identity("app.example.com", Some("/api"))],
        ))
        .await
        .expect_err("duplicate route identity is rejected");
    assert!(matches!(
        duplicate_route,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));

    let recovered_after_rollback = store
        .create_instance(create_instance_request(
            "instance-rollback",
            class.reference.clone(),
            vec![http_identity("rollback.example.com", None)],
        ))
        .await?;
    assert_eq!(
        recovered_after_rollback.instance.id.as_str(),
        "instance-rollback"
    );

    exercise_route_bindings(store, class.reference.clone()).await?;
    exercise_instance_lifecycle(store, class.reference.clone()).await?;
    exercise_complete_wake(store, class.reference.clone()).await?;
    exercise_rendered_object_ref_collision_rejection(store, class.reference.clone()).await?;
    exercise_exclusivity_keys(store, config).await?;
    exercise_no_object_apply_failure_release(store).await?;
    exercise_materialization_reconciliation_leases(store, config, class.reference.clone()).await?;
    exercise_lease_clock_ownership(store, config, class.reference.clone()).await?;

    // Finalizing removes a Deleting instance whose cleanup has finished, and
    // nothing else.
    let delete_target = store
        .create_instance(create_instance_request(
            "instance-delete",
            class.reference.clone(),
            vec![],
        ))
        .await?;
    assert_eq!(delete_target.instance.generation, Generation::new(0));
    store.finalize_instance_deletions(1024).await?;
    assert!(store
        .get_instance(GetInstanceRequest::new(delete_target.instance.id.clone()))
        .await?
        .is_some());
    assert!(
        store
            .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                instance_id: delete_target.instance.id.clone(),
                expected_generation: Generation::new(0),
            })
            .await?
    );
    store.finalize_instance_deletions(1024).await?;
    assert!(store
        .get_instance(GetInstanceRequest::new(delete_target.instance.id.clone()))
        .await?
        .is_none());

    let delete_wake_race = store
        .create_instance(create_instance_request(
            "instance-delete-wake-race",
            class.reference.clone(),
            vec![],
        ))
        .await?;
    store
        .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
            instance_id: delete_wake_race.instance.id.clone(),
            expected_generation: Generation::new(0),
        })
        .await?;
    let deleting = store
        .get_instance(GetInstanceRequest::new(
            delete_wake_race.instance.id.clone(),
        ))
        .await?
        .expect("a deleting instance remains until its deletion finishes");
    assert_eq!(deleting.state, InstanceState::Deleting);
    assert_eq!(deleting.generation, Generation::new(1));
    let wake_after_delete = store
        .accept_wake(accept_request(
            &deleting,
            &MaterializationTarget::new("cluster-a", "default").unwrap(),
            vec![],
            vec![],
        ))
        .await
        .expect_err("a wake after a delete is rejected");
    assert!(matches!(
        wake_after_delete,
        StoreError::InvalidArgument { .. }
    ));

    let initial_resolution = resolve_route(
        store,
        RouteIdentity::Http {
            host: RouteHost::exact("APP.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        },
        MaterializationTarget::new("cluster-a", "default").unwrap(),
    )
    .await?;
    let route_entry = match initial_resolution {
        Some(entry) => entry,
        None => panic!("route should resolve"),
    };
    assert_eq!(route_entry.instance_id.as_str(), "instance-a");
    assert_eq!(route_entry.backend, None);

    let miss = resolve_route(
        store,
        RouteIdentity::Http {
            host: RouteHost::exact("missing.example.com").expect("valid host"),
            path: None,
        },
        MaterializationTarget::new("cluster-a", "default").unwrap(),
    )
    .await?;
    assert_eq!(miss, None);

    let target = MaterializationTarget::new("cluster-a", "default").expect("valid target");
    let mut wake = accept_request(
        &created.instance,
        &target,
        vec![RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: "default".to_owned(),
            name: "instance-a".to_owned(),
        }],
        vec![],
    );
    wake.pending.backend_generation = BackendGeneration::new(2);
    let waking = store.accept_wake(wake).await?;
    assert_eq!(waking.generation, Generation::new(1));
    assert_eq!(waking.state, InstanceState::Waking);

    let stale = store
        .begin_sleep(BeginSleepRequest::new(
            waking.id.clone(),
            Generation::new(0),
            target.clone(),
        ))
        .await
        .expect_err("stale generation is rejected");
    match stale {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(0));
            assert_eq!(actual, Generation::new(1));
        }
        other => panic!("expected generation conflict, got {other}"),
    }

    let pending_record = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            waking.id.clone(),
            target.clone(),
        ))
        .await?
        .expect("the accepted wake has a Pending row");
    assert_eq!(pending_record.state, MaterializationState::Pending);
    let claimed = claim_row(store, &pending_record, "driver").await?;
    let completed = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            claimed.reconciliation_lease.as_ref().unwrap().attempt,
            complete_for_reconciled_pending(&claimed, "http://10.0.0.10:8080"),
        ))
        .await?;
    let running = completed.instance;
    assert_eq!(running.generation, Generation::new(2));
    assert_eq!(running.state, InstanceState::Running);
    let ready_record = completed.materialization;
    assert_eq!(ready_record.state, MaterializationState::Ready);
    assert_eq!(ready_record.backend_generation, BackendGeneration::new(2));
    assert_eq!(
        ready_record.backend.as_ref().map(BackendEndpoint::uri),
        Some("http://10.0.0.10:8080")
    );

    let resolved_with_backend = resolve_route(
        store,
        RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        },
        MaterializationTarget::new("cluster-a", "default").unwrap(),
    )
    .await?;
    match resolved_with_backend {
        Some(entry) => {
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.10:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(2)));
        }
        None => panic!("route should resolve after materialization"),
    }

    // A second completion under the consumed lease changes nothing.
    let repeated = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            claimed.reconciliation_lease.as_ref().unwrap().attempt,
            complete_for_reconciled_pending(&claimed, "http://10.0.0.9:8080"),
        ))
        .await;
    assert!(repeated.is_err(), "a consumed lease completes nothing");

    let resolved_after_repeat = resolve_route(
        store,
        RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        },
        MaterializationTarget::new("cluster-a", "default").unwrap(),
    )
    .await?;
    match resolved_after_repeat {
        Some(entry) => {
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.10:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(2)));
        }
        None => panic!("route should still resolve"),
    }

    let draining = store
        .begin_sleep(BeginSleepRequest::new(
            running.id.clone(),
            Generation::new(2),
            target.clone(),
        ))
        .await?;
    assert_eq!(draining.instance.generation, Generation::new(3));

    let stale_sidecar_report = store
        .begin_sleep(BeginSleepRequest::new(
            running.id.clone(),
            Generation::new(2),
            target.clone(),
        ))
        .await
        .expect_err("stale sidecar idle reports are rejected");
    match stale_sidecar_report {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(2));
            assert_eq!(actual, Generation::new(3));
        }
        other => panic!("expected stale sidecar generation conflict, got {other}"),
    }

    let resolved_after_generation_advance = resolve_route(
        store,
        RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        },
        MaterializationTarget::new("cluster-a", "default").unwrap(),
    )
    .await?;
    match resolved_after_generation_advance {
        Some(entry) => {
            assert_eq!(entry.instance_generation, Generation::new(3));
            assert_eq!(entry.backend, None);
            assert_eq!(entry.backend_generation, None);
        }
        None => panic!("route binding should still resolve"),
    }

    // A completion from the earlier incarnation cannot land on the drain.
    let stale_completion = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            claimed.reconciliation_lease.as_ref().unwrap().attempt,
            complete_for_reconciled_pending(&claimed, "http://10.0.0.11:8080"),
        ))
        .await;
    assert!(
        stale_completion.is_err(),
        "a stale incarnation's completion is rejected"
    );

    // The next wake's backend generation lands above every earlier one, even
    // when the request names a lower floor.
    let deleting = draining
        .materialization
        .expect("the drain marks the projection deleting");
    let cold = finish_sleep_of(store, &deleting, &draining.instance).await?;
    assert_eq!(cold.instance.state, InstanceState::Cold);
    let rewoken = accept_projection(store, &cold.instance, &target, vec![], vec![]).await?;
    assert!(
        rewoken.backend_generation > BackendGeneration::new(2),
        "backend generation rewound to {:?}",
        rewoken.backend_generation
    );

    exercise_http01(store).await?;

    Ok(())
}

async fn exercise_http01(store: &PostgresStore) -> Result<(), StoreError> {
    let now = SystemTime::now();
    let active_key =
        Http01ChallengeKey::new("Acme.Example.COM.", "token-a").expect("valid challenge key");
    let active_put = PutHttp01ChallengeRequest::with_ttl(
        active_key.clone(),
        "key-auth-a",
        Duration::from_secs(60),
        now,
    )
    .expect("valid challenge");
    let active_record = store.put_http01_challenge(active_put).await?;
    assert_eq!(active_record.key().host().as_str(), "acme.example.com");

    let resolved = store
        .resolve_http01_challenge(active_key.clone())
        .await?
        .expect("active challenge resolves");
    assert_eq!(resolved.key_authorization(), "key-auth-a");

    let wrong_host =
        Http01ChallengeKey::new("wrong.example.com", "token-a").expect("valid challenge key");
    assert!(store.resolve_http01_challenge(wrong_host).await?.is_none());
    let wrong_token =
        Http01ChallengeKey::new("acme.example.com", "wrong-token").expect("valid challenge key");
    assert!(store.resolve_http01_challenge(wrong_token).await?.is_none());

    let repeated_put = PutHttp01ChallengeRequest::with_ttl(
        active_key.clone(),
        "key-auth-a",
        Duration::from_secs(60),
        now,
    )
    .expect("valid repeated challenge");
    let repeated_record = store.put_http01_challenge(repeated_put).await?;
    assert_eq!(repeated_record.key_authorization(), "key-auth-a");
    let repeated_resolved = store
        .resolve_http01_challenge(active_key.clone())
        .await?
        .expect("repeated challenge resolves");
    assert_eq!(repeated_resolved.key_authorization(), "key-auth-a");

    let overwrite_put = PutHttp01ChallengeRequest::with_ttl(
        active_key.clone(),
        "key-auth-overwritten",
        Duration::from_secs(120),
        now,
    )
    .expect("valid overwrite challenge");
    let overwritten_record = store.put_http01_challenge(overwrite_put).await?;
    assert_eq!(
        overwritten_record.key_authorization(),
        "key-auth-overwritten"
    );
    let overwritten_resolved = store
        .resolve_http01_challenge(active_key.clone())
        .await?
        .expect("overwritten challenge resolves");
    assert_eq!(
        overwritten_resolved.key_authorization(),
        "key-auth-overwritten"
    );

    assert!(
        store
            .delete_http01_challenge(control_plane::DeleteHttp01ChallengeRequest::new(
                active_key.clone()
            ))
            .await?
    );
    assert!(store.resolve_http01_challenge(active_key).await?.is_none());

    let expired_key = Http01ChallengeKey::new("expired.example.com", "token-b").expect("valid key");
    let expired_put = PutHttp01ChallengeRequest::new(
        expired_key.clone(),
        "key-auth-b",
        UNIX_EPOCH + Duration::from_secs(1),
        UNIX_EPOCH,
    )
    .expect("test can insert an already wall-clock-expired record");
    store.put_http01_challenge(expired_put).await?;
    assert!(store
        .resolve_http01_challenge(expired_key.clone())
        .await?
        .is_none());

    // Runtime maintenance collects the expired row; an explicit delete then
    // finds nothing left.
    store.maintain_runtime_records(1024).await?;
    assert!(
        !store
            .delete_http01_challenge(control_plane::DeleteHttp01ChallengeRequest::new(
                expired_key
            ))
            .await?
    );

    Ok(())
}

async fn exercise_instance_lifecycle(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let target = |name: &str| MaterializationTarget::new(name, "apps").expect("valid target");

    // Two wakes race for one Cold instance: one projection is accepted, and
    // the wake for another target is refused.
    let concurrent = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "instance-lifecycle-concurrent",
    )
    .await?;
    let (first, second) = tokio::join!(
        store.accept_wake(accept_request(
            &concurrent.instance,
            &target("lifecycle-concurrent-a"),
            vec![],
            vec![],
        )),
        store.accept_wake(accept_request(
            &concurrent.instance,
            &target("lifecycle-concurrent-b"),
            vec![],
            vec![],
        ))
    );
    let mut successes = 0;
    let mut refusals = 0;
    for result in [first, second] {
        match result {
            Ok(record) => {
                successes += 1;
                assert_eq!(record.state, InstanceState::Waking);
                assert_eq!(record.generation, Generation::new(1));
            }
            Err(StoreError::InvalidArgument { .. }) => refusals += 1,
            Err(other) => panic!("expected one accepted wake and one refusal, got {other}"),
        }
    }
    assert_eq!((successes, refusals), (1, 1));

    // A Waking instance cannot begin a sleep; a deletion supersedes the wake.
    let sleep_while_waking = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "instance-lifecycle-sleep-waking",
    )
    .await?;
    let waking_target = target("lifecycle-sleep-waking");
    accept_projection(
        store,
        &sleep_while_waking.instance,
        &waking_target,
        vec![],
        vec![],
    )
    .await?;
    let sleep_during_wake = store
        .begin_sleep(BeginSleepRequest::new(
            sleep_while_waking.instance.id.clone(),
            Generation::new(1),
            waking_target,
        ))
        .await
        .expect_err("sleep while waking is deterministically rejected");
    assert!(matches!(
        sleep_during_wake,
        StoreError::InvalidArgument { .. }
    ));
    let still_waking = store
        .get_instance(GetInstanceRequest::new(
            sleep_while_waking.instance.id.clone(),
        ))
        .await?
        .expect("sleep while waking target still exists");
    assert_eq!(still_waking.state, InstanceState::Waking);
    assert_eq!(still_waking.generation, Generation::new(1));
    assert!(
        store
            .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                instance_id: sleep_while_waking.instance.id.clone(),
                expected_generation: Generation::new(1),
            })
            .await?
    );
    let deleting_from_waking = store
        .get_instance(GetInstanceRequest::new(sleep_while_waking.instance.id))
        .await?
        .expect("a deleting instance remains until cleanup finishes");
    assert_eq!(deleting_from_waking.state, InstanceState::Deleting);
    assert_eq!(deleting_from_waking.generation, Generation::new(2));

    // Running drains to Draining and finishes Cold.
    let drain_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "instance-lifecycle-drain-complete",
    )
    .await?;
    let running = wake_to_running(
        store,
        drain_target.instance.id.clone(),
        &target("lifecycle-drain-complete"),
        vec![],
        vec![],
    )
    .await?;
    assert_eq!(running.instance.generation, Generation::new(2));
    let draining = store
        .begin_sleep(BeginSleepRequest::new(
            drain_target.instance.id.clone(),
            Generation::new(2),
            running.materialization.target.clone(),
        ))
        .await?;
    assert_eq!(draining.instance.generation, Generation::new(3));
    let cold = finish_sleep_of(
        store,
        draining.materialization.as_ref().unwrap(),
        &draining.instance,
    )
    .await?;
    assert_eq!(cold.instance.state, InstanceState::Cold);
    assert_eq!(cold.instance.generation, Generation::new(4));

    // A deletion supersedes a drain.
    let delete_draining_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "instance-lifecycle-delete-draining",
    )
    .await?;
    let running = wake_to_running(
        store,
        delete_draining_target.instance.id.clone(),
        &target("lifecycle-delete-draining"),
        vec![],
        vec![],
    )
    .await?;
    let draining = store
        .begin_sleep(BeginSleepRequest::new(
            delete_draining_target.instance.id.clone(),
            Generation::new(2),
            running.materialization.target.clone(),
        ))
        .await?;
    assert_eq!(draining.instance.generation, Generation::new(3));
    store
        .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
            instance_id: delete_draining_target.instance.id.clone(),
            expected_generation: Generation::new(3),
        })
        .await?;
    let deleting_from_draining = store
        .get_instance(GetInstanceRequest::new(delete_draining_target.instance.id))
        .await?
        .expect("a deleting instance remains until cleanup finishes");
    assert_eq!(deleting_from_draining.state, InstanceState::Deleting);
    assert_eq!(deleting_from_draining.generation, Generation::new(4));

    // A wake that fails permanently leaves a Failed instance, which wakes
    // again once its cleanup has finished.
    let failed_target =
        create_lifecycle_instance(store, workload_class, "instance-lifecycle-failed-retry").await?;
    let retry_target = target("lifecycle-failed-retry");
    let pending = accept_projection(
        store,
        &failed_target.instance,
        &retry_target,
        vec![],
        vec![],
    )
    .await?;
    let claimed = claim_row(store, &pending, "driver").await?;
    assert!(
        store
            .record_materialization_failure(
                control_plane::runtime_work::RecordMaterializationFailure {
                    expected_state: MaterializationState::Pending,
                    materialization_id: claimed.id.clone(),
                    owner: "driver".to_owned(),
                    attempt: claimed.reconciliation_lease.as_ref().unwrap().attempt,
                    generation: claimed.instance_generation,
                    permanent: true,
                    message: "readiness timeout".to_owned(),
                }
            )
            .await?
    );
    let failed = store
        .get_instance(GetInstanceRequest::new(failed_target.instance.id.clone()))
        .await?
        .expect("the failed instance remains");
    assert_eq!(failed.state, InstanceState::Failed);
    assert_eq!(failed.generation, Generation::new(2));
    assert!(
        matches!(
            store
                .accept_wake(accept_request(&failed, &retry_target, vec![], vec![]))
                .await,
            Err(StoreError::Unavailable { .. })
        ),
        "a wake waits for the failed wake's cleanup"
    );
    let cleanup = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            failed.id.clone(),
            retry_target.clone(),
        ))
        .await?
        .expect("the failed wake leaves cleanup");
    let claimed = claim_row(store, &cleanup, "driver").await?;
    store
        .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            claimed.reconciliation_lease.as_ref().unwrap().attempt,
            MaterializationState::Deleting,
            claimed.instance_id.clone(),
            claimed.instance_generation,
            claimed.target.clone(),
        ))
        .await?;
    let retry = store
        .accept_wake(accept_request(&failed, &retry_target, vec![], vec![]))
        .await?;
    assert_eq!(retry.state, InstanceState::Waking);
    assert_eq!(retry.generation, Generation::new(3));

    Ok(())
}

/// The store owns the lease clock. Callers request a lifetime, and the store
/// starts it from the same clock every process compares expiry against, so a
/// caller's offset can neither lengthen nor shorten the window before its work
/// becomes reclaimable.
async fn exercise_lease_clock_ownership(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let raw = raw_client(config).await?;
    let target = MaterializationTarget::new("cluster-lease-clock", "apps").expect("valid target");
    let created = store
        .create_instance(create_instance_request(
            "instance-lease-clock",
            workload_class,
            vec![],
        ))
        .await?;
    let pending = accept_projection(store, &created.instance, &target, vec![], vec![]).await?;

    // Remaining lifetime measured entirely inside the database: both operands
    // come from the clock that wrote the expiry.
    let remaining_millis = |id: control_plane::MaterializationId| {
        let raw = &raw;
        async move {
            let row = raw
                .query_one(
                    "SELECT reconcile_lease_expires_at_unix_millis
                        - (extract(epoch from clock_timestamp()) * 1000)::bigint AS remaining
                     FROM materializations WHERE materialization_id = $1",
                    &[&id.as_str()],
                )
                .await
                .map_err(|error| StoreError::internal(error.to_string()))?;
            Ok::<i64, StoreError>(row.get::<_, i64>("remaining"))
        }
    };

    let claimed = claim_next(
        store,
        MaterializationState::Pending,
        &target,
        "lease-clock-owner",
        Duration::from_secs(600),
    )
    .await?
    .expect("pending work is claimable");
    let attempt = claimed
        .reconciliation_lease
        .as_ref()
        .expect("claim returns lease metadata")
        .attempt;
    let remaining = remaining_millis(pending.id.clone()).await?;
    assert!(
        (594_000..=600_000).contains(&remaining),
        "claim starts its TTL at the database clock, leaving ~600s; got {remaining}ms"
    );

    // Renewal restarts the lifetime from the database clock rather than
    // extending whatever instant a caller might have computed.
    let renewed = store
        .renew_materialization_reconciliation_lease(
            RenewMaterializationReconciliationLeaseRequest::new(
                pending.id.clone(),
                "lease-clock-owner",
                attempt,
                pending.instance_generation,
                Duration::from_secs(30),
                MaterializationState::Pending,
            ),
        )
        .await?;
    assert!(renewed, "the live owner renews its own lease");
    let remaining = remaining_millis(pending.id.clone()).await?;
    assert!(
        (24_000..=30_000).contains(&remaining),
        "renewal restarts the TTL from the database clock, leaving ~30s; got {remaining}ms"
    );

    for (ttl, label) in [
        (Duration::ZERO, "zero"),
        (Duration::from_micros(500), "sub-millisecond"),
        (
            control_plane::materialization::MAX_RECONCILIATION_LEASE_TTL + Duration::from_secs(1),
            "beyond the 24 hour bound",
        ),
    ] {
        let error = claim_next(
            store,
            MaterializationState::Pending,
            &target,
            "lease-clock-owner",
            ttl,
        )
        .await
        .expect_err("a lease TTL must be usable");
        assert!(
            matches!(error, StoreError::InvalidArgument { .. }),
            "a {label} lease TTL is an invalid argument, got {error}"
        );
    }

    // Backlog age subtracts a database-written timestamp, so it too must come
    // from the database clock instead of the caller's.
    let metrics = store.load_materialization_operational_metrics().await?;
    let backlog = metrics
        .backlog_states
        .iter()
        .find(|backlog| backlog.state == MaterializationState::Pending)
        .expect("pending backlog is reported");
    let age = backlog.oldest_age.expect("pending backlog reports an age");
    assert!(
        age < Duration::from_secs(600),
        "backlog age is measured against the database clock; got {age:?}"
    );

    Ok(())
}

async fn create_lifecycle_instance(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
    instance_id: &str,
) -> Result<control_plane::CreateInstanceResult, StoreError> {
    store
        .create_instance(create_instance_request(instance_id, workload_class, vec![]))
        .await
}

async fn create_exclusive_instance(
    store: &PostgresStore,
    workload_class: &WorkloadClassVersionRef,
    instance_id: &str,
    volume_handle: &str,
    license_handle: &str,
) -> Result<control_plane::CreateInstanceResult, StoreError> {
    store
        .create_instance(
            create_instance_request(instance_id, workload_class.clone(), vec![]).with_values(
                BTreeMap::from([
                    ("tenant".to_owned(), instance_id.to_owned()),
                    ("volume_handle".to_owned(), volume_handle.to_owned()),
                    ("license_handle".to_owned(), license_handle.to_owned()),
                ]),
            ),
        )
        .await
}

async fn exercise_rendered_object_ref_collision_rejection(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let owner = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "instance-collision-service-owner",
    )
    .await?;
    let service_target =
        MaterializationTarget::new("cluster-collision", "apps").expect("valid target");
    accept_projection(
        store,
        &owner.instance,
        &service_target,
        vec![object_ref("v1", "Service", "apps", "shared-service")],
        vec![],
    )
    .await?;

    let contender = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "instance-collision-service-contender",
    )
    .await?;
    let service_collision = store
        .accept_wake(accept_request(
            &contender.instance,
            &service_target,
            vec![object_ref("v1", "Service", "apps", "shared-service")],
            vec![],
        ))
        .await
        .expect_err("namespaced rendered object ref collision is rejected");
    assert_collision_error(
        service_collision,
        "v1 Service apps/shared-service",
        "instance-collision-service-owner",
    );
    assert_eq!(
        store
            .get_instance(GetInstanceRequest::new(contender.instance.id.clone()))
            .await?
            .expect("contender instance remains"),
        contender.instance,
        "a refused wake changes nothing"
    );

    let pv_owner =
        create_lifecycle_instance(store, workload_class.clone(), "instance-collision-pv-owner")
            .await?;
    accept_projection(
        store,
        &pv_owner.instance,
        &MaterializationTarget::new("cluster-collision", "pv-owner").expect("valid target"),
        vec![object_ref("v1", "PersistentVolume", "", "shared-pv")],
        vec![],
    )
    .await?;

    let pv_contender =
        create_lifecycle_instance(store, workload_class, "instance-collision-pv-contender").await?;
    let pv_collision = store
        .accept_wake(accept_request(
            &pv_contender.instance,
            &MaterializationTarget::new("cluster-collision", "pv-contender").expect("valid target"),
            vec![object_ref("v1", "PersistentVolume", "", "shared-pv")],
            vec![],
        ))
        .await
        .expect_err("cluster-scoped PV rendered object ref collision is rejected");
    assert_collision_error(
        pv_collision,
        "v1 PersistentVolume /shared-pv",
        "instance-collision-pv-owner",
    );

    Ok(())
}

async fn exercise_exclusivity_keys(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
) -> Result<(), StoreError> {
    let workload_class = exclusive_workload_class();
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            workload_class.clone(),
        ))
        .await?;
    let loaded = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            workload_class.reference.clone(),
        ))
        .await?
        .expect("exclusive workload class loads");
    assert_eq!(loaded.exclusivity_keys, workload_class.exclusivity_keys);

    let target = MaterializationTarget::new("cluster-exclusive", "apps").expect("valid target");
    let keys = |instance: &control_plane::InstanceRecord| {
        workload_class
            .render_exclusivity_keys(&instance.values)
            .map_err(|error| StoreError::internal(error.to_string()))
    };
    let owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-exclusive-owner",
        "disk-a",
        "license-owner",
    )
    .await?;
    let expected_owner_keys = vec![
        control_plane::RenderedExclusivityKey::new("disk", "disk-a"),
        control_plane::RenderedExclusivityKey::new("license", "license-owner"),
    ];
    assert_eq!(keys(&owner.instance)?, expected_owner_keys);
    let owner_record = accept_projection(
        store,
        &owner.instance,
        &target,
        vec![],
        keys(&owner.instance)?,
    )
    .await?;
    assert_eq!(owner_record.exclusivity_keys, expected_owner_keys);
    let operational_metrics = store.load_materialization_operational_metrics().await?;
    let pending_metrics = operational_metrics
        .backlog_states
        .iter()
        .find(|state| state.state == MaterializationState::Pending)
        .expect("pending operational metrics are grouped by state");
    assert!(pending_metrics.count >= 1);
    assert!(pending_metrics.oldest_age.is_some());
    let pending_key_metrics = operational_metrics
        .held_key_states
        .iter()
        .find(|state| state.state == MaterializationState::Pending)
        .expect("pending held-key metrics are grouped by state");
    assert!(pending_key_metrics.exclusivity_keys_held >= 2);

    // A wake request for another target cannot move the held keys.
    let elsewhere = store
        .accept_wake(accept_request(
            &owner.instance,
            &MaterializationTarget::new("cluster-exclusive", "elsewhere").expect("valid target"),
            vec![],
            keys(&owner.instance)?,
        ))
        .await
        .expect_err("an accepted wake keeps its target");
    assert!(matches!(elsewhere, StoreError::InvalidArgument { .. }));
    assert_eq!(
        store
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                owner.instance.id.clone(),
                target.clone(),
            ))
            .await?
            .expect("the owner keeps its projection")
            .exclusivity_keys,
        expected_owner_keys
    );

    let contender = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-exclusive-contender",
        "disk-a",
        "license-contender",
    )
    .await?;
    let contender_wake = accept_request(
        &contender.instance,
        &target,
        vec![],
        keys(&contender.instance)?,
    );
    let conflict = store
        .accept_wake(contender_wake.clone())
        .await
        .expect_err("same rendered key is rejected while active materialization holds it");
    assert_exclusivity_conflict(conflict, "disk", Some("instance-exclusive-owner"));
    assert!(store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            contender.instance.id.clone(),
            target.clone(),
        ))
        .await?
        .is_none());

    let restarted_store = PostgresStore::connect(config).await?;
    let restart_contender = create_exclusive_instance(
        &restarted_store,
        &workload_class.reference,
        "instance-exclusive-restart-contender",
        "disk-a",
        "license-restart",
    )
    .await?;
    let restart_conflict = restarted_store
        .accept_wake(accept_request(
            &restart_contender.instance,
            &target,
            vec![],
            keys(&restart_contender.instance)?,
        ))
        .await
        .expect_err("recreated store still sees active held key");
    assert_exclusivity_conflict(restart_conflict, "disk", Some("instance-exclusive-owner"));

    let different = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-exclusive-different",
        "disk-b",
        "license-different",
    )
    .await?;
    let different_record = accept_projection(
        store,
        &different.instance,
        &target,
        vec![],
        keys(&different.instance)?,
    )
    .await?;
    assert_eq!(
        different_record.exclusivity_keys,
        vec![
            control_plane::RenderedExclusivityKey::new("disk", "disk-b"),
            control_plane::RenderedExclusivityKey::new("license", "license-different"),
        ]
    );

    let completed = complete_wake_of(store, &owner_record, "http://10.0.0.50:8080").await?;
    assert_eq!(
        completed.materialization.exclusivity_keys,
        expected_owner_keys
    );
    let ready_operational_metrics = store.load_materialization_operational_metrics().await?;
    assert!(
        ready_operational_metrics
            .backlog_states
            .iter()
            .all(|state| state.state != MaterializationState::Ready),
        "ready materializations must not be counted as backlog"
    );
    let ready_key_metrics = ready_operational_metrics
        .held_key_states
        .iter()
        .find(|state| state.state == MaterializationState::Ready)
        .expect("ready held-key metrics are grouped by state");
    assert!(ready_key_metrics.exclusivity_keys_held >= 2);

    let begin = store
        .begin_sleep(BeginSleepRequest::new(
            owner.instance.id.clone(),
            completed.instance.generation,
            target.clone(),
        ))
        .await?;
    assert_eq!(begin.instance.state, InstanceState::Draining);
    let deleting = begin
        .materialization
        .clone()
        .expect("materialization is marked deleting");
    assert_eq!(deleting.exclusivity_keys, expected_owner_keys);
    let deleting_conflict = store
        .accept_wake(contender_wake.clone())
        .await
        .expect_err("deleting materialization keeps the key held until finalize");
    assert_exclusivity_conflict(deleting_conflict, "disk", Some("instance-exclusive-owner"));

    let finalized = finish_sleep_of(store, &deleting, &begin.instance).await?;
    assert_eq!(finalized.instance.state, InstanceState::Cold);
    assert_eq!(
        finalized
            .materialization
            .as_ref()
            .expect("deleted materialization returned")
            .exclusivity_keys,
        Vec::<control_plane::RenderedExclusivityKey>::new()
    );
    assert!(store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            owner_record.instance_id,
            target.clone(),
        ))
        .await?
        .is_none());

    let acquired_after_release = store.accept_wake(contender_wake).await?;
    assert_eq!(acquired_after_release.id, contender.instance.id);

    Ok(())
}

async fn exercise_no_object_apply_failure_release(store: &PostgresStore) -> Result<(), StoreError> {
    let workload_class = exclusive_workload_class_with_id("class-exclusive-no-apply-release");
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            workload_class.clone(),
        ))
        .await?;

    let target =
        MaterializationTarget::new("cluster-exclusive-release", "apps").expect("valid target");
    let keys = |instance: &control_plane::InstanceRecord| {
        workload_class
            .render_exclusivity_keys(&instance.values)
            .map_err(|error| StoreError::internal(error.to_string()))
    };
    let owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-exclusive-release-owner",
        "disk-release",
        "license-release",
    )
    .await?;
    let expected_owner_keys = vec![
        control_plane::RenderedExclusivityKey::new("disk", "disk-release"),
        control_plane::RenderedExclusivityKey::new("license", "license-release"),
    ];
    assert_eq!(keys(&owner.instance)?, expected_owner_keys);
    let pending = accept_projection(
        store,
        &owner.instance,
        &target,
        vec![object_ref("v1", "PersistentVolume", "", "pv-release")],
        keys(&owner.instance)?,
    )
    .await?;

    // A wake that fails before it applies anything ends: its instance is
    // Failed, and its cleanup keeps the keys until it proves the objects absent.
    let claimed = claim_row(store, &pending, "driver").await?;
    assert!(
        store
            .record_materialization_failure(
                control_plane::runtime_work::RecordMaterializationFailure {
                    expected_state: MaterializationState::Pending,
                    materialization_id: claimed.id.clone(),
                    owner: "driver".to_owned(),
                    attempt: claimed.reconciliation_lease.as_ref().unwrap().attempt,
                    generation: claimed.instance_generation,
                    permanent: true,
                    message: "materialization failed before apply".to_owned(),
                }
            )
            .await?
    );
    let failed = store
        .get_instance(GetInstanceRequest::new(owner.instance.id.clone()))
        .await?
        .expect("the failed instance remains");
    assert_eq!(failed.state, InstanceState::Failed);
    assert_eq!(failed.generation, owner.instance.generation.next().next());

    let contender = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-exclusive-release-contender",
        "disk-release",
        "license-contender",
    )
    .await?;
    let contender_wake = accept_request(
        &contender.instance,
        &target,
        vec![],
        keys(&contender.instance)?,
    );
    let held = store
        .accept_wake(contender_wake.clone())
        .await
        .expect_err("cleanup keeps the failed wake's keys");
    assert_exclusivity_conflict(held, "disk", Some("instance-exclusive-release-owner"));

    let cleanup = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            owner.instance.id.clone(),
            target.clone(),
        ))
        .await?
        .expect("the failed wake leaves cleanup");
    assert_eq!(cleanup.state, MaterializationState::Deleting);
    let cleanup_claim = claim_row(store, &cleanup, "driver").await?;
    let released = store
        .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
            cleanup_claim.id.clone(),
            "driver",
            cleanup_claim.reconciliation_lease.as_ref().unwrap().attempt,
            MaterializationState::Deleting,
            cleanup_claim.instance_id.clone(),
            cleanup_claim.instance_generation,
            cleanup_claim.target.clone(),
        ))
        .await?
        .expect("cleanup marks the row deleted");
    assert_eq!(released.state, MaterializationState::Deleted);
    assert!(released.rendered_objects.is_empty());
    assert!(released.exclusivity_keys.is_empty());
    assert!(store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            owner.instance.id.clone(),
            target.clone(),
        ))
        .await?
        .is_none());

    let acquired = store.accept_wake(contender_wake).await?;
    assert_eq!(acquired.id, contender.instance.id);

    // A cleanup attempt that lost its lease cannot release the keys.
    let stale_owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "instance-exclusive-stale-release-owner",
        "disk-stale-release",
        "license-stale-release",
    )
    .await?;
    let stale_pending = accept_projection(
        store,
        &stale_owner.instance,
        &target,
        vec![],
        keys(&stale_owner.instance)?,
    )
    .await?;
    let wake_claim = claim_row(store, &stale_pending, "driver").await?;
    let wake_attempt = wake_claim.reconciliation_lease.as_ref().unwrap().attempt;
    store
        .record_materialization_failure(control_plane::runtime_work::RecordMaterializationFailure {
            expected_state: MaterializationState::Pending,
            materialization_id: wake_claim.id.clone(),
            owner: "driver".to_owned(),
            attempt: wake_attempt,
            generation: wake_claim.instance_generation,
            permanent: true,
            message: "materialization failed before apply".to_owned(),
        })
        .await?;
    let stale_release_error = store
        .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
            wake_claim.id.clone(),
            "driver",
            wake_attempt,
            MaterializationState::Deleting,
            wake_claim.instance_id.clone(),
            wake_claim.instance_generation,
            wake_claim.target.clone(),
        ))
        .await
        .expect_err("a consumed lease cannot release pending held keys");
    assert!(matches!(
        stale_release_error,
        StoreError::LeaseConflict { .. }
    ));
    let stale_cleanup = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            stale_owner.instance.id.clone(),
            target.clone(),
        ))
        .await?
        .expect("the stale owner's cleanup remains");
    assert_eq!(stale_cleanup.exclusivity_keys.len(), 2);
    let cleanup_claim = claim_row(store, &stale_cleanup, "driver").await?;
    store
        .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
            cleanup_claim.id.clone(),
            "driver",
            cleanup_claim.reconciliation_lease.as_ref().unwrap().attempt,
            MaterializationState::Deleting,
            cleanup_claim.instance_id,
            cleanup_claim.instance_generation,
            target,
        ))
        .await?;

    Ok(())
}

async fn exercise_materialization_reconciliation_leases(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let raw = raw_client(config).await?;
    let target = MaterializationTarget::new("cluster-reconcile", "apps").expect("valid target");
    let created = store
        .create_instance(create_instance_request(
            "instance-reconcile-pending",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    let pending_record = accept_projection(
        store,
        &created.instance,
        &target,
        vec![RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-reconcile-pending".to_owned(),
        }],
        vec![RenderedExclusivityKey::new("disk", "reconcile-disk")],
    )
    .await?;
    let loaded_pending = store
        .load_materialization(LoadMaterializationRequest::new(pending_record.id.clone()))
        .await?
        .expect("pending materialization loads by id");
    assert_eq!(loaded_pending.state, MaterializationState::Pending);
    assert_eq!(loaded_pending.rendered_objects.len(), 1);

    let owner_a_claim = claim_next(
        store,
        MaterializationState::Pending,
        &target,
        "owner-a",
        Duration::from_secs(30),
    )
    .await?
    .expect("first lease claim succeeds");
    assert_eq!(owner_a_claim.id, pending_record.id);
    assert_eq!(
        owner_a_claim
            .reconciliation_lease
            .as_ref()
            .expect("lease metadata")
            .owner,
        "owner-a"
    );
    assert_eq!(
        owner_a_claim
            .reconciliation_lease
            .as_ref()
            .expect("lease metadata")
            .attempt,
        1
    );

    let owner_b_claim = claim_next(
        store,
        MaterializationState::Pending,
        &target,
        "owner-b",
        Duration::from_secs(30),
    )
    .await?;
    assert!(owner_b_claim.is_none());

    let same_owner_claim = claim_next(
        store,
        MaterializationState::Pending,
        &target,
        "owner-a",
        Duration::from_secs(30),
    )
    .await?;
    assert!(same_owner_claim.is_none());

    let wrong_owner_renewed = store
        .renew_materialization_reconciliation_lease(
            RenewMaterializationReconciliationLeaseRequest::new(
                pending_record.id.clone(),
                "owner-b",
                1,
                pending_record.instance_generation,
                Duration::from_secs(30),
                MaterializationState::Pending,
            ),
        )
        .await?;
    assert!(!wrong_owner_renewed);

    raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&pending_record.id.as_str()]).await.map_err(|e| StoreError::internal(e.to_string()))?;
    let owner_b_takeover = claim_next(
        store,
        MaterializationState::Pending,
        &target,
        "owner-b",
        Duration::from_secs(30),
    )
    .await?
    .expect("expired lease can be claimed by another owner");
    assert_eq!(
        owner_b_takeover
            .reconciliation_lease
            .as_ref()
            .expect("lease metadata")
            .owner,
        "owner-b"
    );
    assert_eq!(
        owner_b_takeover
            .reconciliation_lease
            .as_ref()
            .expect("lease metadata")
            .attempt,
        2
    );

    let stale_owner_complete = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            pending_record.id.clone(),
            "owner-a",
            1,
            complete_for_reconciled_pending(&pending_record, "http://10.0.0.40:8080"),
        ))
        .await
        .expect_err("stale lease owner cannot finalize wake");
    assert!(matches!(
        stale_owner_complete,
        StoreError::LeaseConflict { .. }
    ));

    let completed = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            pending_record.id.clone(),
            "owner-b",
            owner_b_takeover
                .reconciliation_lease
                .as_ref()
                .unwrap()
                .attempt,
            complete_for_reconciled_pending(&pending_record, "http://10.0.0.41:8080"),
        ))
        .await?;
    assert_eq!(completed.instance.state, InstanceState::Running);
    assert_eq!(completed.materialization.state, MaterializationState::Ready);
    assert!(completed.materialization.reconciliation_lease.is_none());

    let expired_instance = store
        .create_instance(create_instance_request(
            "instance-reconcile-expired-pending",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    // This row keeps an expired lease, so it stays due; its own target keeps
    // it out of the later scenarios' claims.
    let expired_target =
        MaterializationTarget::new("cluster-reconcile-expired", "apps").expect("valid target");
    let expired_pending = accept_projection(
        store,
        &expired_instance.instance,
        &expired_target,
        vec![],
        vec![RenderedExclusivityKey::new("disk", "expired-pending-disk")],
    )
    .await?;
    claim_next(
        store,
        MaterializationState::Pending,
        &expired_target,
        "expired-owner",
        Duration::from_secs(30),
    )
    .await?
    .expect("expired lease fixture claim succeeds relative to request clock");
    raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&expired_pending.id.as_str()]).await.map_err(|e| StoreError::internal(e.to_string()))?;
    let expired_renewed = store
        .renew_materialization_reconciliation_lease(
            RenewMaterializationReconciliationLeaseRequest::new(
                expired_pending.id.clone(),
                "expired-owner",
                1,
                expired_pending.instance_generation,
                Duration::from_secs(30),
                MaterializationState::Pending,
            ),
        )
        .await?;
    assert!(!expired_renewed);
    let expired_complete = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            expired_pending.id.clone(),
            "expired-owner",
            1,
            complete_for_reconciled_pending(&expired_pending, "http://10.0.0.42:8080"),
        ))
        .await
        .expect_err("same owner cannot complete wake after lease expiry");
    assert!(matches!(expired_complete, StoreError::LeaseConflict { .. }));
    let expired_delete = store
        .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
            expired_pending.id.clone(),
            "expired-owner",
            1,
            MaterializationState::Pending,
            expired_pending.instance_id.clone(),
            expired_pending.instance_generation,
            expired_pending.target.clone(),
        ))
        .await
        .expect_err("same owner cannot mark deleted after lease expiry");
    assert!(matches!(expired_delete, StoreError::LeaseConflict { .. }));
    let expired_pending_loaded = store
        .load_materialization(LoadMaterializationRequest::new(expired_pending.id.clone()))
        .await?
        .expect("expired pending remains inspectable");
    assert_eq!(expired_pending_loaded.state, MaterializationState::Pending);
    assert_eq!(
        expired_pending_loaded
            .exclusivity_keys
            .first()
            .map(|key| key.value.as_str()),
        Some("expired-pending-disk")
    );

    let race_instance = store
        .create_instance(create_instance_request(
            "instance-reconcile-race-pending",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    let stale_race = accept_projection(
        store,
        &race_instance.instance,
        &target,
        vec![],
        vec![RenderedExclusivityKey::new("disk", "race-disk")],
    )
    .await?;
    claim_row(store, &stale_race, "race-owner").await?;
    // The race owner stops renewing; another driver takes over and completes
    // the wake.
    raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&stale_race.id.as_str()]).await.map_err(|e| StoreError::internal(e.to_string()))?;
    let newer_race = complete_wake_of(store, &stale_race, "http://10.0.0.43:8080").await?;
    assert_eq!(newer_race.materialization.id, stale_race.id);
    let race_running = newer_race.instance;
    let stale_race_delete = store
        .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
            stale_race.id.clone(),
            "race-owner",
            1,
            MaterializationState::Pending,
            stale_race.instance_id.clone(),
            stale_race.instance_generation,
            stale_race.target.clone(),
        ))
        .await
        .expect_err("stale cleanup cannot delete newer same-id materialization");
    assert!(matches!(
        stale_race_delete,
        StoreError::NotFound { .. } | StoreError::LeaseConflict { .. }
    ));
    let race_loaded = store
        .load_materialization(LoadMaterializationRequest::new(stale_race.id.clone()))
        .await?
        .expect("newer same-id materialization remains inspectable");
    assert_eq!(race_loaded.state, MaterializationState::Ready);
    assert_eq!(race_loaded.instance_generation, race_running.generation);
    assert_eq!(
        race_loaded
            .exclusivity_keys
            .first()
            .map(|key| key.value.as_str()),
        Some("race-disk")
    );

    let delete_instance = store
        .create_instance(create_instance_request(
            "instance-reconcile-delete",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    let running = wake_to_running(
        store,
        delete_instance.instance.id.clone(),
        &target,
        vec![RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-reconcile-delete".to_owned(),
        }],
        vec![RenderedExclusivityKey::new("disk", "delete-disk")],
    )
    .await?
    .instance;
    let drain_grace_timeout = Duration::from_secs(60 * 60);
    let begin = store
        .begin_sleep(
            BeginSleepRequest::new(running.id.clone(), running.generation, target.clone())
                .with_drain_grace_timeout(drain_grace_timeout),
        )
        .await?;
    let deleting = begin
        .materialization
        .expect("materialization marked deleting");
    let deleting_id = deleting.id.clone();
    // A claim respects the immutable drain deadline. The store reads its own
    // clock, so elapsing the grace period means retiring the stored deadline
    // rather than claiming a later "now".
    assert!(
        claim_next(
            store,
            MaterializationState::Deleting,
            &target,
            "delete-owner-a",
            Duration::from_secs(30)
        )
        .await?
        .is_none(),
        "manual claim must not bypass drain grace"
    );
    raw.execute("UPDATE materializations SET drain_not_before_unix_millis = 0, next_attempt_at_unix_millis = 0 WHERE materialization_id = $1", &[&deleting_id.as_str()]).await.map_err(|e| StoreError::internal(e.to_string()))?;
    let delete_attempt = claim_next(
        store,
        MaterializationState::Deleting,
        &target,
        "delete-owner-a",
        Duration::from_secs(30),
    )
    .await?
    .expect("delete lease claim succeeds after grace")
    .reconciliation_lease
    .unwrap()
    .attempt;
    let stale_finalize = store
        .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
            deleting_id.clone(),
            "delete-owner-b",
            delete_attempt,
            FinalizeSleepRequest::new(
                begin.instance.id.clone(),
                begin.instance.generation,
                target.clone(),
            ),
        ))
        .await
        .expect_err("wrong owner cannot finalize delete");
    assert!(matches!(stale_finalize, StoreError::LeaseConflict { .. }));
    let finalized = store
        .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
            deleting_id.clone(),
            "delete-owner-a",
            delete_attempt,
            FinalizeSleepRequest::new(
                begin.instance.id,
                begin.instance.generation,
                deleting.target,
            ),
        ))
        .await?;
    assert_eq!(finalized.instance.state, InstanceState::Cold);
    assert_eq!(
        finalized
            .materialization
            .as_ref()
            .map(|record| record.state),
        Some(MaterializationState::Deleted)
    );
    assert!(finalized
        .materialization
        .as_ref()
        .expect("deleted materialization")
        .exclusivity_keys
        .is_empty());
    let loaded_deleted = store
        .load_materialization(LoadMaterializationRequest::new(deleting_id))
        .await?
        .expect("deleted materialization remains inspectable by id");
    assert_eq!(loaded_deleted.state, MaterializationState::Deleted);
    assert!(loaded_deleted.exclusivity_keys.is_empty());

    let expired_delete_instance = store
        .create_instance(create_instance_request(
            "instance-reconcile-expired-delete",
            workload_class,
            vec![],
        ))
        .await?;
    let expired_delete_running = wake_to_running(
        store,
        expired_delete_instance.instance.id.clone(),
        &target,
        vec![],
        vec![RenderedExclusivityKey::new("disk", "expired-delete-disk")],
    )
    .await?
    .instance;
    let expired_begin = store
        .begin_sleep(BeginSleepRequest::new(
            expired_delete_running.id.clone(),
            expired_delete_running.generation,
            target.clone(),
        ))
        .await?;
    let expired_deleting = expired_begin
        .materialization
        .expect("expired deleting materialization exists");
    let expired_attempt = claim_next(
        store,
        MaterializationState::Deleting,
        &target,
        "expired-delete-owner",
        Duration::from_secs(30),
    )
    .await?
    .expect("expired delete lease fixture claim succeeds relative to request clock")
    .reconciliation_lease
    .unwrap()
    .attempt;
    raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&expired_deleting.id.as_str()]).await.map_err(|e| StoreError::internal(e.to_string()))?;
    let expired_finalize = store
        .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
            expired_deleting.id.clone(),
            "expired-delete-owner",
            expired_attempt,
            FinalizeSleepRequest::new(
                expired_begin.instance.id,
                expired_begin.instance.generation,
                expired_deleting.target.clone(),
            ),
        ))
        .await
        .expect_err("same owner cannot finalize delete after lease expiry");
    assert!(matches!(expired_finalize, StoreError::LeaseConflict { .. }));
    let expired_deleting_loaded = store
        .load_materialization(LoadMaterializationRequest::new(expired_deleting.id))
        .await?
        .expect("expired deleting remains inspectable");
    assert_eq!(
        expired_deleting_loaded.state,
        MaterializationState::Deleting
    );
    assert_eq!(
        expired_deleting_loaded
            .exclusivity_keys
            .first()
            .map(|key| key.value.as_str()),
        Some("expired-delete-disk")
    );

    let force_instance = store
        .create_instance(create_instance_request(
            "instance-reconcile-force",
            WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            vec![],
        ))
        .await?;
    let force_target =
        MaterializationTarget::new("cluster-force-release", "apps").expect("valid target");
    accept_projection(
        store,
        &force_instance.instance,
        &force_target,
        vec![],
        vec![RenderedExclusivityKey::new("disk", "force-disk")],
    )
    .await?;
    let force_result = store
        .force_release_exclusivity_key(ForceReleaseExclusivityKeyRequest::new(
            force_target,
            "disk",
            "force-disk",
            "operator-a",
            "manual emergency release after inspected cleanup",
        ))
        .await?;
    assert_eq!(force_result.updated_materializations, 1);
    assert_eq!(force_result.affected_materializations.len(), 1);
    assert_eq!(
        force_result.affected_materializations[0].exclusivity_keys,
        vec![RenderedExclusivityKey::new("disk", "force-disk")]
    );

    let released = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            InstanceId::new("instance-reconcile-force").expect("valid instance id"),
            MaterializationTarget::new("cluster-force-release", "apps").expect("valid target"),
        ))
        .await?
        .expect("materialization remains non-terminal after key release");
    assert!(released.exclusivity_keys.is_empty());

    let release_missing = store
        .release_materialization_reconciliation_lease(
            ReleaseMaterializationReconciliationLeaseRequest::new(
                pending_record.id,
                "owner-b",
                1,
                pending_record.instance_generation,
            ),
        )
        .await?;
    assert!(!release_missing);

    Ok(())
}

fn complete_for_reconciled_pending(
    pending: &control_plane::MaterializationRecord,
    backend: &str,
) -> CompleteWakeRequest {
    let mut complete = CompleteWakeRequest::new(
        pending.instance_id.clone(),
        pending.instance_generation,
        pending.target.clone(),
        BackendEndpoint::new(backend).expect("valid backend"),
        pending.backend_generation,
    );
    complete.rendered_objects = pending.rendered_objects.clone();
    complete.exclusivity_keys = pending.exclusivity_keys.clone();
    complete
}

async fn exercise_complete_wake(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let success = store
        .create_instance(create_instance_request(
            "instance-complete-wake-success",
            workload_class.clone(),
            vec![http_identity("complete-wake.example.com", None)],
        ))
        .await?;
    let instance_id = success.instance.id.clone();
    let route_binding_id = success.route_bindings[0].id.clone();
    let target = MaterializationTarget::new("cluster-complete", "apps").expect("valid target");
    let rendered_objects = vec![
        RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-complete-wake-success".to_owned(),
        },
        RenderedObjectRef {
            api_version: "v1".to_owned(),
            kind: "Service".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-complete-wake-success".to_owned(),
        },
    ];
    let mut wake = accept_request(&success.instance, &target, rendered_objects.clone(), vec![]);
    wake.pending.backend_generation = BackendGeneration::new(6);
    let waking = store.accept_wake(wake).await?;
    assert_eq!(waking.state, InstanceState::Waking);
    assert_eq!(waking.generation, Generation::new(1));
    let pending_record = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            instance_id.clone(),
            target.clone(),
        ))
        .await?
        .expect("the accepted wake has a Pending row");
    assert_eq!(pending_record.instance_generation, Generation::new(1));
    assert_eq!(pending_record.backend_generation, BackendGeneration::new(6));

    let completed = complete_wake_of(store, &pending_record, "http://10.0.0.20:8080").await?;
    assert_eq!(completed.instance.id, instance_id);
    assert_eq!(completed.instance.state, InstanceState::Running);
    assert_eq!(completed.instance.generation, Generation::new(2));
    assert_eq!(completed.materialization.instance_id, completed.instance.id);
    assert_eq!(
        completed.materialization.instance_generation,
        completed.instance.generation
    );
    assert_eq!(completed.materialization.target, target);
    assert_eq!(completed.materialization.state, MaterializationState::Ready);
    assert_eq!(
        completed.materialization.backend_generation,
        BackendGeneration::new(6)
    );
    assert_eq!(
        completed
            .materialization
            .backend
            .as_ref()
            .map(BackendEndpoint::uri),
        Some("http://10.0.0.20:8080")
    );
    assert_eq!(completed.materialization.rendered_objects, rendered_objects);
    let loaded_ready = store
        .load_ready_materialization(LoadReadyMaterializationRequest::new(
            completed.instance.id.clone(),
            completed.instance.generation,
            target.clone(),
        ))
        .await?
        .expect("ready materialization loads by exact target and generation");
    assert_eq!(loaded_ready, completed.materialization);
    assert!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                completed.instance.id.clone(),
                Generation::new(1),
                target.clone(),
            ))
            .await?
            .is_none(),
        "wrong instance generation must not load ready materialization"
    );
    assert!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                completed.instance.id.clone(),
                completed.instance.generation,
                MaterializationTarget::new("cluster-complete", "other").expect("valid target"),
            ))
            .await?
            .is_none(),
        "wrong target must not load ready materialization"
    );

    match resolve_route(
        store,
        http_identity("complete-wake.example.com", None),
        MaterializationTarget::new("cluster-complete", "apps").unwrap(),
    )
    .await?
    {
        Some(entry) => {
            assert_eq!(entry.route_binding_id, route_binding_id);
            assert_eq!(entry.instance_id, completed.instance.id);
            assert_eq!(entry.instance_state, InstanceState::Running);
            assert_eq!(entry.instance_generation, Generation::new(2));
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.20:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(6)));
        }
        None => panic!("route should resolve after the wake completes"),
    }

    let sleep_started = store
        .begin_sleep(BeginSleepRequest::new(
            completed.instance.id.clone(),
            completed.instance.generation,
            target.clone(),
        ))
        .await?;
    assert_eq!(sleep_started.instance.state, InstanceState::Draining);
    assert_eq!(sleep_started.instance.generation, Generation::new(3));
    let deleting_materialization = sleep_started
        .materialization
        .clone()
        .expect("active materialization is marked deleting");
    assert_eq!(
        deleting_materialization.state,
        MaterializationState::Deleting
    );
    assert_eq!(
        deleting_materialization.backend, completed.materialization.backend,
        "a draining projection keeps its backend for a revive"
    );
    assert_eq!(deleting_materialization.rendered_objects, rendered_objects);
    assert!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                completed.instance.id.clone(),
                completed.instance.generation,
                target.clone(),
            ))
            .await?
            .is_none(),
        "a draining materialization is not Ready"
    );
    let active = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            completed.instance.id.clone(),
            target.clone(),
        ))
        .await?
        .expect("deleting materialization remains active until cleanup finalizes");
    assert_eq!(active.state, MaterializationState::Deleting);

    let sleep_finalized =
        finish_sleep_of(store, &deleting_materialization, &sleep_started.instance).await?;
    assert_eq!(sleep_finalized.instance.state, InstanceState::Cold);
    assert_eq!(sleep_finalized.instance.generation, Generation::new(4));
    let deleted_materialization = sleep_finalized
        .materialization
        .expect("materialization is marked deleted");
    assert_eq!(deleted_materialization.state, MaterializationState::Deleted);
    assert_eq!(
        deleted_materialization.instance_generation,
        Generation::new(4)
    );
    assert_eq!(deleted_materialization.backend, None);
    assert!(deleted_materialization.rendered_objects.is_empty());
    assert!(
        store
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                completed.instance.id.clone(),
                target.clone(),
            ))
            .await?
            .is_none(),
        "deleted materialization is no longer active"
    );

    // A completion names the incarnation it woke; an earlier one is refused
    // and changes nothing.
    let stale = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "instance-complete-wake-stale",
    )
    .await?;
    let stale_target =
        MaterializationTarget::new("cluster-complete", "stale").expect("valid target");
    let stale_pending =
        accept_projection(store, &stale.instance, &stale_target, vec![], vec![]).await?;
    let claimed = claim_row(store, &stale_pending, "driver").await?;
    let mut stale_complete = complete_for_reconciled_pending(&claimed, "http://10.0.0.30:8080");
    stale_complete.expected_waking_generation = Generation::new(0);
    let stale_error = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            claimed.id.clone(),
            "driver",
            claimed.reconciliation_lease.as_ref().unwrap().attempt,
            stale_complete,
        ))
        .await
        .expect_err("a completion for an earlier incarnation is rejected");
    assert!(matches!(
        stale_error,
        StoreError::NotFound { .. } | StoreError::GenerationConflict { .. }
    ));
    let stale_after = store
        .get_instance(GetInstanceRequest::new(stale.instance.id.clone()))
        .await?
        .expect("stale completion target still exists");
    assert_eq!(stale_after.state, InstanceState::Waking);
    assert_eq!(stale_after.generation, Generation::new(1));

    // A Cold instance has no wake to complete.
    let cold =
        create_lifecycle_instance(store, workload_class, "instance-complete-wake-cold").await?;
    let cold_target = MaterializationTarget::new("cluster-complete", "cold").expect("valid target");
    let cold_error = store
        .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
            control_plane::materialization_id(&cold.instance.id, &cold_target)
                .expect("valid materialization ID"),
            "driver",
            1,
            CompleteWakeRequest::new(
                cold.instance.id.clone(),
                Generation::new(0),
                cold_target,
                BackendEndpoint::new("http://10.0.0.31:8080").expect("valid backend"),
                BackendGeneration::new(3),
            ),
        ))
        .await
        .expect_err("cold instances cannot complete wake");
    assert!(matches!(cold_error, StoreError::NotFound { .. }));
    let cold_after = store
        .get_instance(GetInstanceRequest::new(cold.instance.id.clone()))
        .await?
        .expect("cold completion target still exists");
    assert_eq!(cold_after.state, InstanceState::Cold);
    assert_eq!(cold_after.generation, Generation::new(0));

    Ok(())
}

async fn exercise_route_bindings(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let route_instance = store
        .create_instance(create_instance_request(
            "instance-routes",
            workload_class,
            vec![],
        ))
        .await?;
    let loaded_before = store
        .get_instance(GetInstanceRequest::new(route_instance.instance.id.clone()))
        .await?
        .expect("route target instance loads");

    let exact_root = store
        .create_route_binding(create_route_binding_request(
            "route-exact-root",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::exact("App.Routes.Example.COM.").expect("valid host"),
                path: None,
            },
        ))
        .await?;
    assert_eq!(
        exact_root.identity,
        http_identity("app.routes.example.com", None)
    );
    let loaded_exact = store
        .get_route_binding(GetRouteBindingRequest::new(exact_root.id.clone()))
        .await?
        .expect("created route binding loads");
    assert_eq!(loaded_exact, exact_root);

    // A repeat that describes the stored binding returns it.
    let repeated_exact = store
        .create_route_binding(create_route_binding_request(
            "route-exact-root",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::exact("app.routes.example.com").expect("valid host"),
                path: None,
            },
        ))
        .await?;
    assert_eq!(repeated_exact, exact_root);

    // Any other request for the binding ID is refused and changes nothing.
    for (instance_id, identity) in [
        (
            "instance-routes",
            http_identity("conflict.routes.example.com", None),
        ),
        ("instance-a", http_identity("app.routes.example.com", None)),
    ] {
        let refused = store
            .create_route_binding(create_route_binding_request(
                "route-exact-root",
                instance_id,
                identity,
            ))
            .await
            .expect_err("a different request for an existing binding ID is refused");
        assert!(matches!(
            refused,
            StoreError::AlreadyExists {
                resource: "route binding"
            }
        ));
    }
    assert_eq!(
        store
            .get_route_binding(GetRouteBindingRequest::new(exact_root.id.clone()))
            .await?,
        Some(exact_root.clone())
    );

    let duplicate_identity = store
        .create_route_binding(create_route_binding_request(
            "route-duplicate-identity",
            "instance-routes",
            http_identity("APP.ROUTES.EXAMPLE.COM.", None),
        ))
        .await
        .expect_err("duplicate normalized route identity is rejected");
    assert!(matches!(
        duplicate_identity,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));

    let duplicate_id = store
        .create_route_binding(create_route_binding_request(
            "route-exact-root",
            "instance-routes",
            http_identity("duplicate-id.routes.example.com", None),
        ))
        .await
        .expect_err("duplicate route binding ID is rejected");
    assert!(matches!(
        duplicate_id,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));

    let missing_instance = store
        .create_route_binding(create_route_binding_request(
            "route-missing-instance",
            "missing-instance",
            http_identity("missing.routes.example.com", None),
        ))
        .await
        .expect_err("route binding must point at an existing instance");
    assert!(matches!(
        missing_instance,
        StoreError::NotFound {
            resource: "referenced resource"
        }
    ));

    let exact_api = store
        .create_route_binding(create_route_binding_request(
            "route-exact-api",
            "instance-routes",
            http_identity("app.routes.example.com", Some("/api")),
        ))
        .await?;
    let exact_api_v1 = store
        .create_route_binding(create_route_binding_request(
            "route-exact-api-v1",
            "instance-routes",
            http_identity("app.routes.example.com", Some("/api/v1")),
        ))
        .await?;
    let wildcard_broad = store
        .create_route_binding(create_route_binding_request(
            "route-wildcard-broad",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("routes.example.com").expect("valid host"),
                path: None,
            },
        ))
        .await?;
    let wildcard_specific = store
        .create_route_binding(create_route_binding_request(
            "route-wildcard-specific",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("customer.routes.example.com")
                    .expect("valid host"),
                path: None,
            },
        ))
        .await?;

    assert_resolves_to(
        store,
        http_identity("app.routes.example.com", Some("/anything")),
        &exact_root.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("app.routes.example.com", Some("/api/v1/users")),
        &exact_api_v1.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("app.routes.example.com", Some("/api")),
        &exact_api.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("app.routes.example.com", Some("/apiary")),
        &exact_root.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("other.routes.example.com", None),
        &wildcard_broad.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("db.customer.routes.example.com", None),
        &wildcard_specific.id,
    )
    .await?;

    let sni_exact = store
        .create_route_binding(create_route_binding_request(
            "route-sni-exact",
            "instance-routes",
            sni_identity("DB.Routes.Example.COM."),
        ))
        .await?;
    let sni_wildcard = store
        .create_route_binding(create_route_binding_request(
            "route-sni-wildcard",
            "instance-routes",
            RouteIdentity::Sni {
                host: RouteHost::wildcard_suffix("routes.example.com").expect("valid host"),
            },
        ))
        .await?;
    let duplicate_sni = store
        .create_route_binding(create_route_binding_request(
            "route-sni-duplicate",
            "instance-routes",
            sni_identity("db.routes.example.com"),
        ))
        .await
        .expect_err("duplicate normalized SNI identity is rejected");
    assert!(matches!(
        duplicate_sni,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));
    assert_resolves_to(store, sni_identity("db.routes.example.com"), &sni_exact.id).await?;
    assert_resolves_to(
        store,
        sni_identity("tenant.routes.example.com"),
        &sni_wildcard.id,
    )
    .await?;

    let miss = resolve_route(
        store,
        http_identity("routes.example.com", None),
        MaterializationTarget::new("cluster-a", "default").unwrap(),
    )
    .await?;
    assert_eq!(
        miss, None,
        "a wildcard suffix does not match its own base name"
    );

    assert!(
        store
            .delete_route_binding(DeleteRouteBindingRequest::new(sni_wildcard.id.clone()))
            .await?
    );
    assert!(store
        .get_route_binding(GetRouteBindingRequest::new(sni_wildcard.id.clone()))
        .await?
        .is_none());
    assert!(
        !store
            .delete_route_binding(DeleteRouteBindingRequest::new(sni_wildcard.id))
            .await?
    );

    let loaded_after = store
        .get_instance(GetInstanceRequest::new(route_instance.instance.id))
        .await?
        .expect("route target instance still loads");
    assert_eq!(loaded_after, loaded_before);

    Ok(())
}

/// The path a proxy matches for a request identity: `/` when an HTTP request
/// has none, and nothing for SNI.
fn request_path(identity: &RouteIdentity) -> &str {
    match identity {
        RouteIdentity::Http { path, .. } => path.as_ref().map_or("/", |path| path.as_str()),
        RouteIdentity::Sni { .. } => "",
    }
}

/// Routes one request as a proxy does: the host's rule set from the store,
/// then the first rule that covers the path.
async fn resolve_route(
    store: &PostgresStore,
    identity: RouteIdentity,
    target: MaterializationTarget,
) -> Result<Option<RouteEntry>, StoreError> {
    let rules = store
        .resolve_host_rules(ResolveHostRulesRequest::new(
            identity.kind(),
            identity.host().clone(),
            target,
        ))
        .await?;
    Ok(rules
        .select(request_path(&identity))
        .map(|rule| rule.entry.clone()))
}

/// The binding Postgres itself picks for one request: among the bindings whose
/// host and path cover it, an exact host first, then the longer suffix, then
/// the longer path prefix.
const RESOLVE_ONE_REQUEST_SQL: &str = "
    SELECT route_binding_id FROM route_bindings
    WHERE identity_kind = $1
        AND ((host_kind = 'exact' AND host = $2)
            OR (host_kind = 'wildcard_suffix'
                AND right($2, octet_length(host) + 1) = '.' || host))
        AND (path_prefix IS NULL OR path_prefix = '/' OR path_prefix = $3
            OR (right(path_prefix, 1) = '/' AND starts_with($3, path_prefix))
            OR starts_with($3, path_prefix || '/'))
    ORDER BY (host_kind = 'exact') DESC, octet_length(host) DESC,
        COALESCE(octet_length(path_prefix), 0) DESC, route_binding_id
    LIMIT 1
";

/// Asserts that matching a request against the host's rule set picks the
/// binding Postgres resolves for that request.
async fn assert_local_match_equals_postgres(
    store: &PostgresStore,
    raw: &tokio_postgres::Client,
    identity: RouteIdentity,
    context: &str,
) -> TestResult {
    let kind = match identity {
        RouteIdentity::Http { .. } => "http",
        RouteIdentity::Sni { .. } => "sni",
    };
    let resolved: Option<String> = raw
        .query_opt(
            RESOLVE_ONE_REQUEST_SQL,
            &[&kind, &identity.host().as_str(), &request_path(&identity)],
        )
        .await?
        .map(|row| row.get(0));
    let local = resolve_route(
        store,
        identity.clone(),
        MaterializationTarget::new("target", "apps")?,
    )
    .await?
    .map(|entry| entry.route_binding_id.as_str().to_owned());
    assert_eq!(local, resolved, "{context}: {identity:?}");
    Ok(())
}

/// A deterministic generator, so a failure names the round that produced it.
struct Random(u64);

impl Random {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// Random binding sets over nested hosts and overlapping path prefixes, each
/// checked with random requests.
async fn local_matching_equals_postgres_for_random_binding_sets(
    store: &PostgresStore,
    raw: &tokio_postgres::Client,
    instance_id: &str,
) -> TestResult {
    let exact_hosts = [
        "prop.example.net",
        "a.prop.example.net",
        "b.a.prop.example.net",
        "c.b.a.prop.example.net",
        "ab.prop.example.net",
    ];
    let suffixes = [
        "prop.example.net",
        "a.prop.example.net",
        "b.a.prop.example.net",
        "ab.prop.example.net",
    ];
    let prefixes = [
        None,
        Some("/"),
        Some("/a"),
        Some("/a/"),
        Some("/a/b"),
        Some("/ab"),
        Some("/a/%_"),
    ];
    let paths = [
        "/", "/a", "/a/", "/a/b", "/a/b/c", "/ab", "/abc", "/b", "/a/%_", "/a/xy",
    ];
    let mut random = Random(0x9e37_79b9_7f4a_7c15);
    for round in 0..40 {
        let mut identities: Vec<RouteIdentity> = Vec::new();
        for _ in 0..random.below(14) {
            let host = if random.below(2) == 0 {
                RouteHost::exact(random.pick(&exact_hosts))?
            } else {
                RouteHost::wildcard_suffix(random.pick(&suffixes))?
            };
            let identity = if random.below(4) == 0 {
                RouteIdentity::Sni { host }
            } else {
                RouteIdentity::Http {
                    host,
                    path: random.pick(&prefixes).map(PathPrefix::new).transpose()?,
                }
            };
            if !identities.contains(&identity) {
                identities.push(identity);
            }
        }
        let mut bindings = Vec::new();
        for (index, identity) in identities.iter().enumerate() {
            let id = format!("prop-{round}-{index}");
            bindings.push(
                store
                    .create_route_binding(create_route_binding_request(
                        &id,
                        instance_id,
                        identity.clone(),
                    ))
                    .await?
                    .id,
            );
        }
        for _ in 0..24 {
            let host = RouteHost::exact(random.pick(&exact_hosts))?;
            let request = if random.below(4) == 0 {
                RouteIdentity::Sni { host }
            } else {
                RouteIdentity::Http {
                    host,
                    path: Some(PathPrefix::new(*random.pick(&paths))?),
                }
            };
            assert_local_match_equals_postgres(
                store,
                raw,
                request,
                &format!("round {round} with {identities:?}"),
            )
            .await?;
        }
        for binding in bindings {
            store
                .delete_route_binding(DeleteRouteBindingRequest::new(binding))
                .await?;
        }
    }
    Ok(())
}

async fn assert_resolves_to(
    store: &PostgresStore,
    identity: RouteIdentity,
    expected_route_binding_id: &RouteBindingId,
) -> Result<(), StoreError> {
    match resolve_route(
        store,
        identity,
        MaterializationTarget::new("cluster-a", "default").unwrap(),
    )
    .await?
    {
        Some(entry) => {
            assert_eq!(&entry.route_binding_id, expected_route_binding_id);
            Ok(())
        }
        None => panic!("route should resolve"),
    }
}

fn workload_class(class_id: &str, version: u64) -> WorkloadClassVersion {
    let image = format!("example/app:{version}");

    WorkloadClassVersion {
        reference: WorkloadClassVersionRef::new(
            WorkloadClassId::new(class_id).expect("valid workload class ID"),
            Generation::new(version),
        ),
        template: workload_manifest_template(),
        value_schema: WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required())
            .with_field(
                "image",
                WorkloadValueFieldRule::optional_with_default(image),
            ),
        sleep_policy: default_sleep_policy(),
        exclusivity_keys: vec![],
    }
}

fn exclusive_workload_class() -> WorkloadClassVersion {
    exclusive_workload_class_with_id("class-exclusive")
}

fn exclusive_workload_class_with_id(class_id: &str) -> WorkloadClassVersion {
    let mut workload_class = workload_class(class_id, 1);
    workload_class.value_schema = workload_class
        .value_schema
        .with_field("volume_handle", WorkloadValueFieldRule::required())
        .with_field("license_handle", WorkloadValueFieldRule::required());
    workload_class.exclusivity_keys = vec![
        WorkloadExclusivityKeyTemplate::new(
            "license",
            TemplateText::instance_value("license_handle"),
        ),
        WorkloadExclusivityKeyTemplate::new("disk", TemplateText::instance_value("volume_handle")),
    ];
    workload_class
}

fn workload_class_with_idle_override(class_id: &str, version: u64) -> WorkloadClassVersion {
    let mut workload_class = workload_class(class_id, version);
    workload_class.value_schema = workload_class
        .value_schema
        .with_field("idle_ms", WorkloadValueFieldRule::optional());
    workload_class.sleep_policy = default_sleep_policy()
        .with_idle_timeout_override(
            IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000)
                .expect("valid override policy"),
        )
        .expect("override policy attaches");
    workload_class
}

fn default_sleep_policy() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy::new(300_000, 5_000, 30_000).expect("valid sleep policy")
}

fn workload_manifest_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: composed_text("app-", "tenant"),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::instance_value("image"),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: vec![EnvVarTemplate {
                    name: "TENANT".to_owned(),
                    value: TemplateText::instance_value("tenant"),
                }],
            },
            termination_grace_period_seconds: None,
        },
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: None,
            app_protocol: Default::default(),
        },
        service: Some(ServiceTemplate {
            name: composed_text("svc-", "tenant"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn composed_text(prefix: &str, field: &str) -> TemplateText {
    TemplateText::from_parts([
        TemplateTextPart::literal(prefix),
        TemplateTextPart::instance_value(field),
    ])
}

fn create_instance_request(
    instance_id: &str,
    workload_class: WorkloadClassVersionRef,
    route_identities: Vec<RouteIdentity>,
) -> CreateInstanceRequest {
    let route_bindings = route_identities
        .into_iter()
        .enumerate()
        .map(|(index, identity)| InstanceRoute {
            id: RouteBindingId::new(format!("{instance_id}:route:{}", index + 1))
                .expect("valid route binding ID"),
            identity,
        })
        .collect();
    CreateInstanceRequest::new(
        InstanceId::new(instance_id).expect("valid instance ID"),
        workload_class,
    )
    .with_values(BTreeMap::from([(
        "tenant".to_owned(),
        instance_id.to_owned(),
    )]))
    .with_route_bindings(route_bindings)
}

fn create_route_binding_request(
    route_binding_id: &str,
    instance_id: &str,
    identity: RouteIdentity,
) -> CreateRouteBindingRequest {
    CreateRouteBindingRequest::new(
        RouteBindingId::new(route_binding_id).expect("valid route binding ID"),
        InstanceId::new(instance_id).expect("valid instance ID"),
        identity,
    )
}

fn http_identity(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path prefix")),
    }
}

fn sni_identity(host: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::exact(host).expect("valid host"),
    }
}

fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}

fn assert_collision_error(error: StoreError, object: &str, owner_instance_id: &str) {
    match error {
        StoreError::InvalidArgument { message } => {
            assert!(
                message.contains("rendered Kubernetes object ref collision"),
                "message {message:?} should identify a rendered object collision"
            );
            assert!(
                message.contains(object),
                "message {message:?} should include collided object {object:?}"
            );
            assert!(
                message.contains(owner_instance_id),
                "message {message:?} should include owner instance {owner_instance_id:?}"
            );
        }
        other => panic!("expected rendered object collision invalid argument, got {other}"),
    }
}

fn assert_exclusivity_conflict(
    error: StoreError,
    expected_key_name: &str,
    expected_owner: Option<&str>,
) {
    match error {
        StoreError::ExclusivityConflict {
            key_name,
            owner_instance_id,
            ..
        } => {
            assert_eq!(key_name, expected_key_name);
            assert_eq!(owner_instance_id.as_deref(), expected_owner);
        }
        other => panic!("expected exclusivity conflict, got {other}"),
    }
}

fn unique_schema_name() -> String {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after epoch")
        .as_nanos();

    format!(
        "sleepypods_test_{}_{}_{}",
        std::process::id(),
        nanos,
        sequence
    )
}

fn connection_url_with_search_path(base_url: &str, schema: &str) -> String {
    let separator = if base_url.contains('?') { '&' } else { '?' };

    format!("{base_url}{separator}options=-csearch_path%3D{schema}")
}

async fn raw_client(config: &PostgresStoreConfig) -> Result<tokio_postgres::Client, StoreError> {
    let (client, connection) = tokio_postgres::connect(config.connection_url(), NoTls)
        .await
        .map_err(|e| StoreError::internal(e.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test]
async fn postgres_scalability_contracts_against_real_database() -> TestResult {
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping Postgres scalability contracts; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    let admin_config = PostgresStoreConfig::new(&base_url)?;
    let admin = raw_client(&admin_config).await?;
    let schema = unique_schema_name();
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
    // Simultaneous startup must serialize before even creating the metadata table.
    let (a, b, c, d) = tokio::join!(
        PostgresStore::connect(&config),
        PostgresStore::connect(&config),
        PostgresStore::connect(&config),
        PostgresStore::connect(&config)
    );
    let store = a?;
    b?;
    c?;
    d?;
    let result = phase5_contracts(&store, &config).await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    result
}

async fn phase5_contracts(store: &PostgresStore, config: &PostgresStoreConfig) -> TestResult {
    let class = workload_class("phase5-class", 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    phase5_single_slot_pool_and_cancelled_migration(config, &class).await?;
    phase5_reservation_concurrency(store, config, &class).await?;
    phase5_route_scale_snapshot_and_target(store, config, &class).await?;
    Ok(())
}

async fn phase5_single_slot_pool_and_cancelled_migration(
    config: &PostgresStoreConfig,
    class: &WorkloadClassVersion,
) -> TestResult {
    let mut config = config.clone();
    config.max_connections = 1;
    config.pool_wait_timeout = Duration::from_millis(100);
    // One attempt, so the checkout budget alone bounds the call.
    let store =
        PostgresStore::connect(&config)
            .await?
            .with_retry_policy(control_plane::RetryPolicy::new(
                1,
                Duration::ZERO,
                Duration::ZERO,
            ));
    // The old nested checkout deadlocked on the already-held only slot.
    tokio::time::timeout(
        Duration::from_secs(2),
        store.create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone())),
    )
    .await??;
    let raw = raw_client(&config).await?;
    raw.query_one("SELECT pg_advisory_lock(1936748391, 1835624306)", &[])
        .await?;
    let migration_store = store.clone();
    let migration = tokio::spawn(async move { migration_store.run_migrations().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = raw.query_one("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = 1936748391 AND objid = 1835624306 AND NOT granted)", &[]).await.unwrap().get(0);
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await?;
    let started = std::time::Instant::now();
    let error = store
        .get_instance(GetInstanceRequest::new(InstanceId::new("absent")?))
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::Unavailable { .. }));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "saturated checkout must be bounded"
    );
    migration.abort();
    assert!(migration.await.unwrap_err().is_cancelled());
    raw.query_one("SELECT pg_advisory_unlock(1936748391, 1835624306)", &[])
        .await?;
    tokio::time::timeout(Duration::from_secs(2), store.run_migrations()).await??;
    assert!(store
        .get_instance(GetInstanceRequest::new(InstanceId::new("absent")?))
        .await?
        .is_none());
    eprintln!(
        "phase5 single_slot_pool_and_cancelled_migration passed; saturation bounded to 100ms"
    );
    Ok(())
}

async fn phase5_reservation_concurrency(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
    class: &WorkloadClassVersion,
) -> TestResult {
    let target = MaterializationTarget::new("phase5", "apps")?;
    let mut requests = Vec::new();
    for index in 0..28 {
        let id = format!("phase5-reservation-{index}");
        let instance = create_lifecycle_instance(store, class.reference.clone(), &id)
            .await?
            .instance;
        requests.push(accept_request(
            &instance,
            &target,
            vec![object_ref("v1", "Service", "apps", &id)],
            vec![RenderedExclusivityKey::new("disk", &id)],
        ));
    }
    let accept = |request: control_plane::materialization::AcceptWakeRequest| {
        let store = store.clone();
        async move {
            let id = request.pending.instance_id.clone();
            store.accept_wake(request).await?;
            Ok::<_, StoreError>(
                store
                    .load_active_materialization(LoadActiveMaterializationRequest::new(
                        id,
                        MaterializationTarget::new("phase5", "apps").unwrap(),
                    ))
                    .await?
                    .expect("an accepted wake has a Pending row"),
            )
        }
    };
    let held = accept(requests[0].clone()).await?;
    let mut raw = raw_client(config).await?;
    let transaction = raw.transaction().await?;
    transaction.execute("UPDATE materializations SET failure_count = failure_count WHERE materialization_id = $1", &[&held.id.as_str()]).await?;
    let legacy = raw_client(config).await?;
    legacy
        .batch_execute("SET lock_timeout = '100ms'; BEGIN")
        .await?;
    let legacy_started = std::time::Instant::now();
    let legacy_error = legacy
        .batch_execute("LOCK TABLE materializations IN SHARE ROW EXCLUSIVE MODE")
        .await
        .unwrap_err();
    assert_eq!(legacy_error.code().unwrap().code(), "55P03");
    let legacy_wait = legacy_started.elapsed();
    legacy.batch_execute("ROLLBACK").await?;
    let mut tasks = tokio::task::JoinSet::new();
    let started = std::time::Instant::now();
    for request in &requests[1..24] {
        let store = store.clone();
        let request = request.clone();
        tasks.spawn(async move { store.accept_wake(request).await });
    }
    // An accept that waited for the held row would wait until the 30-second
    // statement timeout. The bound sits below that and well above the two
    // seconds the accepts take when every round trip costs 140ms, as on an
    // I/O-bound CI database.
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await
    .map_err(|_| "an accept waited for the unrelated held row")?;
    let elapsed = started.elapsed();
    transaction.rollback().await?;
    // Two transactions racing to acquire one key must produce one owner.
    let mut left = requests[24].clone();
    let mut right = requests[25].clone();
    left.pending.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", "shared-race")];
    right.pending.exclusivity_keys = left.pending.exclusivity_keys.clone();
    let (left, right) = tokio::join!(store.accept_wake(left), store.accept_wake(right));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(matches!(
        left.as_ref().err().or(right.as_ref().err()),
        Some(StoreError::ExclusivityConflict { .. })
    ));
    // An object has one Kubernetes API-group identity across API versions.
    let mut owner = requests[26].clone();
    owner.pending.exclusivity_keys.clear();
    owner.pending.rendered_objects = vec![object_ref(
        "apps/v1beta1",
        "Deployment",
        "apps",
        "shared-version",
    )];
    let mut other_version = requests[27].clone();
    other_version.pending.exclusivity_keys.clear();
    other_version.pending.rendered_objects = vec![object_ref(
        "apps/v1",
        "Deployment",
        "apps",
        "shared-version",
    )];
    store.accept_wake(owner).await?;
    assert!(matches!(
        store.accept_wake(other_version).await,
        Err(StoreError::InvalidArgument { .. })
    ));
    // Renewal and release preserve state age; retry queue placement is separate.
    let age_before: i64 = raw.query_one("SELECT state_entered_at_unix_millis FROM materializations WHERE materialization_id = $1", &[&held.id.as_str()]).await?.get(0);
    raw.execute(
        "UPDATE materializations SET next_attempt_at_unix_millis = 0 WHERE materialization_id = $1",
        &[&held.id.as_str()],
    )
    .await?;
    let claimed = claim_next(
        store,
        MaterializationState::Pending,
        &held.target,
        "age-owner",
        Duration::from_secs(60),
    )
    .await?
    .unwrap();
    assert_eq!(claimed.id, held.id, "the earliest due row is claimed first");
    store
        .renew_materialization_reconciliation_lease(
            RenewMaterializationReconciliationLeaseRequest::new(
                held.id.clone(),
                "age-owner",
                1,
                held.instance_generation,
                Duration::from_secs(30),
                MaterializationState::Pending,
            ),
        )
        .await?;
    store
        .release_materialization_reconciliation_lease(
            ReleaseMaterializationReconciliationLeaseRequest::new(
                held.id.clone(),
                "age-owner",
                1,
                held.instance_generation,
            ),
        )
        .await?;
    let age_after: i64 = raw.query_one("SELECT state_entered_at_unix_millis FROM materializations WHERE materialization_id = $1", &[&held.id.as_str()]).await?.get(0);
    assert_eq!(age_before, age_after);
    eprintln!("phase5 reservation_concurrency passed; legacy table lock timed out after {legacy_wait:?}; 23 independent reservations committed in {elapsed:?} while unrelated row transaction held open; one winner per conflicting key");
    Ok(())
}

async fn phase5_route_scale_snapshot_and_target(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
    class: &WorkloadClassVersion,
) -> TestResult {
    let request = create_instance_request("phase5-routing", class.reference.clone(), vec![]);
    let instance = store.create_instance(request).await?.instance;
    let mut raw = raw_client(config).await?;
    raw.execute("INSERT INTO route_bindings(route_binding_id, instance_id, identity_key, identity_kind, host_kind, host) SELECT 'bulk-'||n, $1, 'bulk-'||n, 'http', 'exact', 'bulk-'||n||'.example.net' FROM generate_series(1, 100000) n", &[&instance.id.as_str()]).await?;
    let rules = [
        (
            "broad",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("example.net")?,
                path: None,
            },
        ),
        (
            "specific",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("customer.example.net")?,
                path: Some(PathPrefix::new("/api")?),
            },
        ),
        ("exact", http_identity("app.customer.example.net", None)),
        (
            "path",
            http_identity("app.customer.example.net", Some("/api/v1")),
        ),
        (
            "trailing",
            http_identity("app.customer.example.net", Some("/api/v1/")),
        ),
        (
            "literal",
            http_identity("app.customer.example.net", Some("/api/%_")),
        ),
    ];
    for (name, identity) in &rules {
        store
            .create_route_binding(create_route_binding_request(
                name,
                instance.id.as_str(),
                identity.clone(),
            ))
            .await?;
    }
    for host in [
        "app.customer.example.net",
        "other.customer.example.net",
        "other.example.net",
        "example.net",
    ] {
        for path in [
            None,
            Some("/"),
            Some("/api"),
            Some("/apix"),
            Some("/api/v1"),
            Some("/api/v1/users"),
            Some("/api/%_/child"),
        ] {
            assert_local_match_equals_postgres(
                store,
                &raw,
                http_identity(host, path),
                "fixed rules among 100k unrelated routes",
            )
            .await?;
        }
    }
    local_matching_equals_postgres_for_random_binding_sets(store, &raw, instance.id.as_str())
        .await?;
    // The instance runs in one target; a lookup for another target sees no backend.
    let pending = accept_projection(
        store,
        &instance,
        &MaterializationTarget::new("target", "apps")?,
        vec![],
        vec![],
    )
    .await?;
    complete_wake_of(store, &pending, "http://127.0.0.1:8081").await?;
    assert!(matches!(
        resolve_route(
            store,
            http_identity("app.customer.example.net", None),
            MaterializationTarget::new("other", "apps").unwrap(),
        )
        .await?,
        Some(entry) if entry.backend.is_none()
    ));
    let lookup = || {
        resolve_route(
            store,
            http_identity("app.customer.example.net", None),
            MaterializationTarget::new("target", "apps").unwrap(),
        )
    };
    assert!(
        matches!(lookup().await?, Some(entry) if entry.backend.as_ref().unwrap().uri() == "http://127.0.0.1:8081")
    );
    // During a concurrent uncommitted cascade the complete pre-delete snapshot
    // remains usable; after commit resolution is a miss, never NotFound.
    let deletion = raw.transaction().await?;
    deletion
        .execute(
            "DELETE FROM instances WHERE instance_id = $1",
            &[&instance.id.as_str()],
        )
        .await?;
    assert!(lookup().await?.is_some());
    deletion.commit().await?;
    assert_eq!(lookup().await?, None);
    eprintln!("phase5 route_scale_snapshot_and_target passed; local matching equals Postgres resolution over 100k routes and random binding sets, correct target backend, atomic delete/miss");
    Ok(())
}

/// The schema applies in one transaction: a failure leaves nothing behind, a
/// repeat changes nothing, and a recorded migration under another name stops
/// the store from starting.
#[tokio::test]
async fn postgres_migrations_apply_atomically_once_and_fail_closed() -> TestResult {
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping Postgres migrations; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    let admin = raw_client(&PostgresStoreConfig::new(&base_url)?).await?;
    let schema = unique_schema_name();
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
    let raw = raw_client(&config).await?;
    let result = async {
        // An object the schema would create makes the migration fail midway.
        raw.batch_execute("CREATE TABLE http01_challenges (host text)")
            .await?;
        assert!(PostgresStore::connect(&config).await.is_err());
        let left: Vec<Option<String>> = raw
            .query_one(
                "SELECT to_regclass('instances')::text, to_regclass('control_plane_schema_migrations')::text",
                &[],
            )
            .await
            .map(|row| vec![row.get(0), row.get(1)])?;
        assert_eq!(
            left,
            [None, None],
            "a failed migration rolls back its DDL and records nothing"
        );

        raw.batch_execute("DROP TABLE http01_challenges").await?;
        let store = PostgresStore::connect(&config).await?;
        store.run_migrations().await?;
        raw.execute(
            "UPDATE control_plane_schema_migrations SET name = 'wrong-name' WHERE version = 1",
            &[],
        )
        .await?;
        assert!(
            store.run_migrations().await.is_err(),
            "migration metadata mismatch fails closed"
        );
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    }
    .await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    result
}

#[tokio::test]
async fn postgres_durable_lifecycle_acceptance_and_fresh_driver() -> TestResult {
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base_url, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
    let store = PostgresStore::connect(&config).await?;
    store.run_migrations().await?;
    let result = durable_lifecycle_checks(store).await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    connection_task.abort();
    result
}

#[derive(Clone, Default)]
struct LifecycleKubernetes {
    objects: std::sync::Arc<
        std::sync::Mutex<BTreeMap<String, control_plane::projection::LiveObjectMetadata>>,
    >,
    applies: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    deletes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
fn lifecycle_object_key(object: &RenderedObjectRef) -> String {
    format!(
        "{}|{}|{}|{}",
        object.api_version, object.kind, object.namespace, object.name
    )
}
impl control_plane::materializer::KubernetesMaterializerClient for LifecycleKubernetes {
    fn apply_object<'a>(
        &'a self,
        object: &'a control_plane::manifest::KubernetesObject,
        _precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<()>,
    > {
        Box::pin(async move {
            self.applies
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.objects.lock().unwrap().insert(
                lifecycle_object_key(&object.object_ref()),
                control_plane::projection::LiveObjectMetadata::from_rendered_object(object),
            );
            Ok(())
        })
    }
    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        _precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<()>,
    > {
        Box::pin(async move {
            self.deletes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.objects
                .lock()
                .unwrap()
                .remove(&lifecycle_object_key(object));
            Ok(())
        })
    }
    fn wait_for_readiness<'a>(
        &'a self,
        _: &'a [RenderedObjectRef],
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<BackendEndpoint>,
    > {
        Box::pin(async { Ok(BackendEndpoint::new("http://ready.apps:80").unwrap()) })
    }
    fn ensure_no_descendants<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        _instance_id: &'a str,
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<()>,
    > {
        Box::pin(async { Ok(()) })
    }
    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<
            control_plane::projection::ProjectionObjectInspection,
        >,
    > {
        Box::pin(async move {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(&lifecycle_object_key(object))
                .cloned()
                .map(control_plane::projection::ProjectionObjectInspection::Present)
                .unwrap_or(control_plane::projection::ProjectionObjectInspection::Missing))
        })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<()>,
    > {
        Box::pin(async { Ok(()) })
    }
}

async fn durable_lifecycle_checks(store: PostgresStore) -> TestResult {
    use control_plane::api::{pb, StoreBackedProxyApi};
    use control_plane::instance::RequestInstanceDeletion;
    use control_plane::materializer::KubernetesMaterializer;
    use pb::proxy_control_plane_server::ProxyControlPlane;
    use std::sync::{atomic::Ordering, Arc};
    let class = workload_class("durable-class", 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let store = Arc::new(store);
    let target = MaterializationTarget::new("cluster-a", "apps")?;
    let client = LifecycleKubernetes::default();
    let materializer = KubernetesMaterializer::new(client.clone());
    let api = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone());
    let create = |id: &str| create_instance_request(id, class.reference.clone(), Vec::new());
    let cold = store
        .create_instance(create("accepted-wake"))
        .await?
        .instance;
    let wake = pb::ProxyWakeInstanceRequest {
        backend_generation: Some(44),
        instance_id: cold.id.as_str().to_owned(),
        expected_generation: cold.generation.get(),
    };
    let (left, right) = tokio::join!(
        api.wake_instance(tonic::Request::new(wake.clone())),
        api.wake_instance(tonic::Request::new(wake))
    );
    let responses = [left?, right?];
    assert!(responses.iter().any(|r| matches!(
        r.get_ref().outcome,
        Some(pb::proxy_wake_instance_response::Outcome::StillWaking(_))
    )));
    assert_eq!(
        client.applies.load(Ordering::SeqCst),
        0,
        "acceptance has no Kubernetes effects"
    );
    let pending = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            cold.id.clone(),
            target.clone(),
        ))
        .await?
        .unwrap();
    assert_eq!(pending.backend_generation, BackendGeneration::new(44));
    let stamp = pending.projection_generation;
    assert_eq!(pending.state, MaterializationState::Pending);
    drop(api); // Simulate cancellation/process loss immediately after durable acceptance.
    let observations = sleepypods_observability::recorder::InMemoryObservability::default();
    let fresh_driver = |target: MaterializationTarget| {
        control_plane::MaterializationReconciler::new(
            store.clone(),
            materializer.clone(),
            target,
            transport_support::reconciler_config(),
            observations.recorder(),
        )
    };
    let instance_state = |id: InstanceId| {
        let store = store.clone();
        async move {
            store
                .get_instance(GetInstanceRequest::new(id))
                .await
                .ok()
                .flatten()
                .map(|instance| instance.state)
        }
    };
    // Drives a fresh reconciler until the instance reaches `state`; `None`
    // means the instance is gone.
    let drive_until =
        |target: MaterializationTarget, id: InstanceId, state: Option<InstanceState>| {
            let driver = fresh_driver(target);
            async move {
                transport_support::reconcile_until(driver, || {
                    let observed = instance_state(id.clone());
                    async move { observed.await == state }
                })
                .await;
            }
        };
    // Drives a fresh reconciler for a fixed time, for outcomes that must not happen.
    let drive_for = |target: MaterializationTarget, duration: Duration| {
        let driver = fresh_driver(target);
        async move {
            let until = tokio::time::Instant::now() + duration;
            transport_support::reconcile_until(driver, || async move {
                tokio::time::Instant::now() >= until
            })
            .await;
        }
    };
    drive_until(
        target.clone(),
        cold.id.clone(),
        Some(InstanceState::Running),
    )
    .await;
    let running = store
        .get_instance(GetInstanceRequest::new(cold.id.clone()))
        .await?
        .unwrap();
    assert_eq!(running.state, InstanceState::Running);
    let ready = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            cold.id.clone(),
            target.clone(),
        ))
        .await?
        .unwrap();
    assert_eq!(ready.state, MaterializationState::Ready);
    assert_eq!(
        ready.projection_generation, stamp,
        "ownership stamp must not change on completion"
    );
    assert!(client
        .objects
        .lock()
        .unwrap()
        .values()
        .all(|m| m.labels.get("sleepypods.io/instance-generation") == Some(&stamp.to_string())));

    // The grace outlasts the drive and the revive below on a loaded machine.
    let drain = store
        .begin_sleep(
            BeginSleepRequest::new(running.id.clone(), running.generation, target.clone())
                .with_drain_grace_timeout(Duration::from_secs(60)),
        )
        .await?;
    let deletes_before = client.deletes.load(Ordering::SeqCst);
    drive_for(target.clone(), Duration::from_millis(200)).await;
    assert_eq!(
        client.deletes.load(Ordering::SeqCst),
        deletes_before,
        "a claim may not skip persisted grace"
    );

    // A wake inside the drain grace returns the instance to Running on the
    // projection it already has.
    let api = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone());
    let revived = api
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            backend_generation: Some(1),
            instance_id: running.id.as_str().to_owned(),
            expected_generation: drain.instance.generation.get(),
        }))
        .await?
        .into_inner();
    assert!(
        matches!(
            revived.outcome,
            Some(pb::proxy_wake_instance_response::Outcome::Ready(_))
        ),
        "a wake during drain grace is served by the existing projection: {revived:?}"
    );
    let revived = store
        .get_instance(GetInstanceRequest::new(cold.id.clone()))
        .await?
        .unwrap();
    assert_eq!(revived.state, InstanceState::Running);
    assert_eq!(revived.generation, drain.instance.generation.next());
    let revived_projection = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            cold.id.clone(),
            target.clone(),
        ))
        .await?
        .unwrap();
    assert_eq!(revived_projection.state, MaterializationState::Ready);
    assert_eq!(revived_projection.projection_generation, stamp);
    assert_eq!(revived_projection.backend, ready.backend);
    assert_eq!(
        revived_projection.backend_generation,
        BackendGeneration::new(45)
    );
    assert_eq!(client.deletes.load(Ordering::SeqCst), deletes_before);

    // Once the drain deadline has passed, teardown may have begun, so a wake
    // is deferred and promoted atomically with the completed sleep.
    let drain = store
        .begin_sleep(
            BeginSleepRequest::new(revived.id.clone(), revived.generation, target.clone())
                .with_drain_grace_timeout(Duration::from_secs(1)),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let deferred = api
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            backend_generation: Some(1),
            instance_id: running.id.as_str().to_owned(),
            expected_generation: drain.instance.generation.get(),
        }))
        .await?
        .into_inner();
    assert!(matches!(
        deferred.outcome,
        Some(pb::proxy_wake_instance_response::Outcome::StillWaking(_))
    ));
    drop(api);
    assert_eq!(
        instance_state(cold.id.clone()).await,
        Some(InstanceState::Draining)
    );
    drive_until(
        target.clone(),
        cold.id.clone(),
        Some(InstanceState::Running),
    )
    .await;
    let rewoken = store
        .get_instance(GetInstanceRequest::new(cold.id.clone()))
        .await?
        .unwrap();
    assert_eq!(
        rewoken.generation,
        drain.instance.generation.next().next().next(),
        "the drained instance passed through Cold and Waking: {:?}",
        observations.events()
    );
    assert!(client.deletes.load(Ordering::SeqCst) > deletes_before);
    assert_eq!(
        store
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                cold.id.clone(),
                target.clone()
            ))
            .await?
            .unwrap()
            .backend_generation,
        BackendGeneration::new(46),
        "a lower requested floor advances from the prior Deleted row"
    );

    // Ready, Pending, and zero-materialization deletion all survive loss of the API caller.
    for state in ["ready", "pending", "zero"] {
        let id = format!("delete-{state}");
        let instance = store.create_instance(create(&id)).await?.instance;
        if state != "zero" {
            StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone())
                .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
                    backend_generation: None,
                    instance_id: id.clone(),
                    expected_generation: instance.generation.get(),
                }))
                .await?;
            if state == "ready" {
                drive_until(
                    target.clone(),
                    instance.id.clone(),
                    Some(InstanceState::Running),
                )
                .await;
            }
        }
        let current = store
            .get_instance(GetInstanceRequest::new(instance.id.clone()))
            .await?
            .unwrap();
        let delete = RequestInstanceDeletion {
            instance_id: instance.id.clone(),
            expected_generation: current.generation,
        };
        assert!(store.request_instance_deletion(delete.clone()).await?);
        assert!(
            matches!(
                store
                    .request_instance_deletion(RequestInstanceDeletion {
                        instance_id: instance.id.clone(),
                        expected_generation: Generation::new(u64::MAX)
                    })
                    .await,
                Err(StoreError::InvalidArgument { .. })
            ),
            "out-of-range store revision is rejected before any successor arithmetic"
        );
        assert!(
            store.request_instance_deletion(delete.clone()).await?,
            "lost response replay is idempotent"
        );
        if state != "zero" {
            store.finalize_instance_deletions(1024).await?;
            assert!(
                store
                    .get_instance(GetInstanceRequest::new(instance.id.clone()))
                    .await?
                    .is_some(),
                "a deletion finalizes only once its cleanup has finished"
            );
        }
        drive_until(target.clone(), instance.id.clone(), None).await;
        let replacement = store
            .create_instance(create_instance_request(
                &id,
                class.reference.clone(),
                Vec::new(),
            ))
            .await?
            .instance;
        assert!(replacement.generation > current.generation.next());
        assert!(
            matches!(
                store.request_instance_deletion(delete).await,
                Err(StoreError::GenerationConflict { .. })
            ),
            "old delete cannot delete a replacement ID"
        );
        let old_wake =
            StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone())
                .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
                    backend_generation: None,
                    instance_id: id,
                    expected_generation: current.generation.get(),
                }))
                .await?
                .into_inner();
        assert!(matches!(
            old_wake.outcome,
            Some(pb::proxy_wake_instance_response::Outcome::GenerationConflict(_))
        ));
    }

    let cancel = store
        .create_instance(create("cancel-deferred"))
        .await?
        .instance;
    let cancel_api = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone());
    cancel_api
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            instance_id: cancel.id.as_str().to_owned(),
            expected_generation: cancel.generation.get(),
            backend_generation: None,
        }))
        .await?;
    drive_until(
        target.clone(),
        cancel.id.clone(),
        Some(InstanceState::Running),
    )
    .await;
    let cancel_running = store
        .get_instance(GetInstanceRequest::new(cancel.id.clone()))
        .await?
        .unwrap();
    let cancel_drain = store
        .begin_sleep(
            BeginSleepRequest::new(cancel.id.clone(), cancel_running.generation, target.clone())
                .with_drain_grace_timeout(Duration::from_secs(1)),
        )
        .await?;
    // Past the drain deadline the wake is deferred; deletion then cancels it.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    cancel_api
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            instance_id: cancel.id.as_str().to_owned(),
            expected_generation: cancel_drain.instance.generation.get(),
            backend_generation: None,
        }))
        .await?;
    drop(cancel_api);
    assert_eq!(
        instance_state(cancel.id.clone()).await,
        Some(InstanceState::Draining)
    );
    store
        .request_instance_deletion(RequestInstanceDeletion {
            instance_id: cancel.id.clone(),
            expected_generation: cancel_drain.instance.generation,
        })
        .await?;
    drive_until(target.clone(), cancel.id.clone(), None).await;
    assert!(
        store
            .get_instance(GetInstanceRequest::new(cancel.id))
            .await?
            .is_none(),
        "delete atomically cancels the deferred wake"
    );

    eprintln!("phase6a durable acceptance, fresh-driver recovery, stable projection, grace and ID-reuse fencing passed");
    Ok(())
}

#[tokio::test]
async fn postgres_attempt_and_incarnation_fencing() -> TestResult {
    use std::sync::Arc;
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base_url, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
    let raw_store = PostgresStore::connect(&config).await?;
    raw_store.run_migrations().await?;
    let store = Arc::new(raw_store);
    let (raw, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let raw_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result: TestResult = async {
        let class = workload_class("fence-class", 1);
        store
            .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
            .await?;
        let cold = store
            .create_instance(create_instance_request(
                "fence-instance",
                class.reference.clone(),
                vec![],
            ))
            .await?
            .instance;
        let target = MaterializationTarget::new("fence-cluster", "apps")?;
        let pending = accept_projection(
            store.as_ref(),
            &cold,
            &target,
            vec![object_ref("v1", "Service", "apps", "fence-service")],
            vec![RenderedExclusivityKey::new("singleton", "fence")],
        )
        .await?;
        let claim = || {
            claim_next(
                store.as_ref(),
                MaterializationState::Pending,
                &target,
                "same-owner",
                Duration::from_secs(30),
            )
        };
        let renew = |attempt| {
            store.renew_materialization_reconciliation_lease(
                RenewMaterializationReconciliationLeaseRequest::new(
                    pending.id.clone(),
                    "same-owner",
                    attempt,
                    pending.instance_generation,
                    Duration::from_secs(30),
                    MaterializationState::Pending,
                ),
            )
        };
        let release = |attempt| {
            store.release_materialization_reconciliation_lease(
                ReleaseMaterializationReconciliationLeaseRequest::new(
                    pending.id.clone(),
                    "same-owner",
                    attempt,
                    pending.instance_generation,
                ),
            )
        };

        // The same owner's earlier attempt is fenced by its later one.
        let first_attempt = claim().await?.unwrap().reconciliation_lease.unwrap().attempt;
        raw.execute(
            "UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1",
            &[&pending.id.as_str()],
        )
        .await?;
        let second_attempt = claim().await?.unwrap().reconciliation_lease.unwrap().attempt;
        assert!(second_attempt > first_attempt);
        assert!(!renew(first_attempt).await?);
        assert!(!release(first_attempt).await?);
        assert!(matches!(
            store
                .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
                    pending.id.clone(),
                    "same-owner",
                    first_attempt,
                    complete_for_reconciled_pending(&pending, "http://ready:80"),
                ))
                .await,
            Err(StoreError::LeaseConflict { .. })
        ));
        assert!(matches!(
            store
                .delete_materialization_reconciliation(
                    DeleteMaterializationReconciliationRequest::new(
                        pending.id.clone(),
                        "same-owner",
                        first_attempt,
                        pending.state,
                        pending.instance_id.clone(),
                        pending.instance_generation,
                        target.clone(),
                    )
                )
                .await,
            Err(StoreError::LeaseConflict { .. })
        ));
        assert!(renew(second_attempt).await?, "the live attempt keeps its lease");

        // A recreated instance reuses the deterministic materialization ID and
        // may reuse the attempt number. Its generation fences the old lease.
        store
            .force_delete_materialization(control_plane::ForceDeleteMaterializationRequest::new(
                pending.id.clone(),
                "test-operator",
                "old driver fenced; cluster cleanup confirmed",
            ))
            .await?;
        let settled = store
            .get_instance(GetInstanceRequest::new(pending.instance_id.clone()))
            .await?
            .unwrap();
        assert_eq!(settled.state, InstanceState::Failed);
        store
            .request_instance_deletion(control_plane::instance::RequestInstanceDeletion {
                instance_id: pending.instance_id.clone(),
                expected_generation: settled.generation,
            })
            .await?;
        store.finalize_instance_deletions(10).await?;
        assert!(store
            .get_instance(GetInstanceRequest::new(pending.instance_id.clone()))
            .await?
            .is_none());
        let recreated = store
            .create_instance(create_instance_request(
                pending.instance_id.as_str(),
                class.reference,
                vec![],
            ))
            .await?
            .instance;
        let recreated_pending = accept_projection(
            store.as_ref(),
            &recreated,
            &pending.target,
            pending.rendered_objects.clone(),
            pending.exclusivity_keys.clone(),
        )
        .await?;
        assert_eq!(
            recreated_pending.id, pending.id,
            "materialization names are deterministic across ID reuse"
        );
        assert!(recreated_pending.instance_generation > pending.instance_generation);
        let reused = claim().await?.unwrap();
        assert_eq!(
            reused.reconciliation_lease.as_ref().unwrap().attempt,
            first_attempt,
            "attempt counter may restart after row deletion"
        );
        assert!(!renew(first_attempt).await?);
        assert!(!release(first_attempt).await?);
        assert!(
            store
                .load_materialization(LoadMaterializationRequest::new(pending.id.clone()))
                .await?
                .unwrap()
                .reconciliation_lease
                .is_some(),
            "the old incarnation's stamp cannot release the new lease"
        );
        Ok(())
    }
    .await;
    drop(store);
    drop(raw);
    raw_task.abort();
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    connection_task.abort();
    result
}

#[path = "postgres_store/runtime_work.rs"]
mod runtime_work;

#[path = "postgres_store/retry_boundaries.rs"]
mod retry_boundaries;

#[path = "postgres_store/create_repeat.rs"]
mod create_repeat;

#[path = "postgres_store/activation_idle.rs"]
mod activation_idle;

#[path = "postgres_store/cleanup_boundary.rs"]
mod cleanup_boundary;

#[path = "postgres_store/certificates.rs"]
mod certificates;

#[path = "postgres_store/crash_boundaries.rs"]
mod crash_boundaries;

#[path = "postgres_store/host_rule_bounds.rs"]
mod host_rule_bounds;
#[path = "postgres_store/lifecycle_recovery.rs"]
mod lifecycle_recovery;
#[path = "postgres_store/replay_policy.rs"]
mod replay_policy;
#[path = "postgres_store/round_trips.rs"]
mod round_trips;
