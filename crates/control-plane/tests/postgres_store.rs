#[macro_use]
#[path = "support/unexpected_store.rs"]
mod unexpected_store;
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
    ClaimMaterializationReconciliationRequest, CompareAndSwapInstanceStateRequest,
    CompleteWakeReconciliationRequest, CompleteWakeRequest, ContainerPortTemplate,
    ContainerTemplate, ControlPlaneStore, CreateInstanceRequest, CreateRouteBindingRequest,
    CreateWorkloadClassVersionRequest, DeleteInstanceRequest,
    DeleteMaterializationReconciliationRequest, DeleteRouteBindingRequest, EnvVarTemplate,
    ExpireHttp01ChallengesRequest, FinalizeSleepReconciliationRequest, FinalizeSleepRequest,
    ForceReleaseExclusivityKeyRequest, Generation, GetInstanceRequest, GetRouteBindingRequest,
    Http01ChallengeKey, IdempotencyKey, IdleTimeoutOverridePolicy, InstanceId, InstanceState,
    ListMaterializationReconciliationCandidatesRequest, ManifestTemplate, MaterializationState,
    MaterializationTarget, PathPrefix, PostgresStore, PostgresStoreConfig, ProtocolRoute,
    PutHttp01ChallengeRequest, RecordMaterializationRequest,
    ReleaseMaterializationReconciliationLeaseRequest, RenderManifestRequest,
    RenderedExclusivityKey, RenderedObjectRef, RenewMaterializationReconciliationLeaseRequest,
    ResolveRouteRequest, RouteBindingId, RouteBindingSpec, RouteDependencyLookup, RouteHost,
    RouteIdentity, RouteResolution, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
    StateTransitionReason, StoreError, TemplateText, TemplateTextPart, WorkloadClassId,
    WorkloadClassVersion, WorkloadClassVersionRef, WorkloadExclusivityKeyTemplate, WorkloadKind,
    WorkloadSleepPolicy, WorkloadTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
};
use tokio_postgres::NoTls;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

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
    changed_class.template_generation = Generation::new(2);
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
            IdempotencyKey::new("idem-missing-required").expect("valid idempotency key"),
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
            create_instance_request(
                "idem-unknown-value",
                "instance-unknown-value",
                class.reference.clone(),
                vec![],
            )
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
                "idem-override-out-of-bounds",
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
                "idem-override-valid",
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
        "idem-create-a",
        "instance-a",
        class.reference.clone(),
        vec![
            http_route("app.example.com", Some("/api")),
            sni_route("db.example.com"),
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
    assert!(!created.idempotency_replayed);
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
        template_generation: Some(loaded.template_generation),
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
            .any(|(kind, name)| *kind == "Deployment" && name == "app-instance-a-69856ec0"),
        "loaded workload class template should render durable instance Deployment, got {rendered_names:?}"
    );
    assert!(
        rendered_names
            .iter()
            .any(|(kind, name)| *kind == "Service" && name == "svc-instance-a-69856ec0"),
        "loaded workload class template should render durable instance Service, got {rendered_names:?}"
    );

    let replayed = store.create_instance(create.clone()).await?;
    assert!(replayed.idempotency_replayed);
    assert_eq!(replayed.instance, created.instance);
    assert_eq!(replayed.route_bindings, created.route_bindings);

    let explicit_default_replay = create.clone().with_values(BTreeMap::from([
        ("image".to_owned(), "example/app:1".to_owned()),
        ("tenant".to_owned(), "instance-a".to_owned()),
    ]));
    let replayed_with_explicit_default = store.create_instance(explicit_default_replay).await?;
    assert!(replayed_with_explicit_default.idempotency_replayed);
    assert_eq!(replayed_with_explicit_default.instance, created.instance);
    assert_eq!(
        replayed_with_explicit_default.route_bindings,
        created.route_bindings
    );

    let changed_values_conflict = store
        .create_instance(create.clone().with_values(BTreeMap::from([(
            "tenant".to_owned(),
            "different-tenant".to_owned(),
        )])))
        .await
        .expect_err("same idempotency key with different canonical values conflicts");
    assert!(matches!(
        changed_values_conflict,
        StoreError::IdempotencyConflict
    ));

    let conflict = store
        .create_instance(create_instance_request(
            "idem-create-a",
            "instance-conflict",
            class.reference.clone(),
            vec![http_route("conflict.example.com", None)],
        ))
        .await
        .expect_err("same idempotency key with a different payload conflicts");
    assert!(matches!(conflict, StoreError::IdempotencyConflict));

    let duplicate_route = store
        .create_instance(create_instance_request(
            "idem-rollback",
            "instance-rollback",
            class.reference.clone(),
            vec![http_route("app.example.com", Some("/api"))],
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
            "idem-rollback",
            "instance-rollback",
            class.reference.clone(),
            vec![http_route("rollback.example.com", None)],
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

    let delete_target = store
        .create_instance(create_instance_request(
            "idem-delete",
            "instance-delete",
            class.reference.clone(),
            vec![],
        ))
        .await?;
    assert_eq!(delete_target.instance.generation, Generation::new(0));
    let direct_delete = store
        .delete_instance(DeleteInstanceRequest::new(
            delete_target.instance.id.clone(),
        ))
        .await
        .expect_err("hard delete requires deleting state");
    assert!(matches!(direct_delete, StoreError::InvalidArgument { .. }));
    let deleting_target = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            delete_target.instance.id.clone(),
            Generation::new(0),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting_target.state, InstanceState::Deleting);
    assert_eq!(deleting_target.generation, Generation::new(1));
    assert!(
        store
            .delete_instance(DeleteInstanceRequest::new(
                delete_target.instance.id.clone()
            ))
            .await?
    );
    assert!(store
        .get_instance(GetInstanceRequest::new(delete_target.instance.id.clone()))
        .await?
        .is_none());
    assert!(
        !store
            .delete_instance(DeleteInstanceRequest::new(delete_target.instance.id))
            .await?
    );

    let delete_wake_race = store
        .create_instance(create_instance_request(
            "idem-delete-wake-race",
            "instance-delete-wake-race",
            class.reference.clone(),
            vec![],
        ))
        .await?;
    let deleting = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            delete_wake_race.instance.id.clone(),
            Generation::new(0),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting.generation, Generation::new(1));
    let wake_after_delete = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            delete_wake_race.instance.id,
            Generation::new(1),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await
        .expect_err("wake after delete CAS is rejected");
    assert!(matches!(
        wake_after_delete,
        StoreError::InvalidArgument { .. }
    ));

    let initial_resolution = store
        .resolve_route(ResolveRouteRequest::new(
            RouteIdentity::Http {
                host: RouteHost::exact("APP.example.com").expect("valid host"),
                path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
            },
            MaterializationTarget::new("cluster-a", "default").unwrap(),
        ))
        .await?;
    let route_entry = match initial_resolution {
        RouteResolution::Resolved { entry, .. } => entry,
        RouteResolution::Miss { .. } => panic!("route should resolve"),
    };
    assert_eq!(route_entry.instance_id.as_str(), "instance-a");
    assert_eq!(route_entry.backend, None);

    let miss = store
        .resolve_route(ResolveRouteRequest::new(
            RouteIdentity::Http {
                host: RouteHost::exact("missing.example.com").expect("valid host"),
                path: None,
            },
            MaterializationTarget::new("cluster-a", "default").unwrap(),
        ))
        .await?;
    assert!(matches!(miss, RouteResolution::Miss { .. }));

    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(waking.generation, Generation::new(1));
    assert_eq!(waking.state, InstanceState::Waking);

    let stale = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(0),
            InstanceState::Failed,
            StateTransitionReason::FailureReported("stale writer".to_owned()),
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

    let target = MaterializationTarget::new("cluster-a", "default").expect("valid target");
    let pending = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(1),
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    let pending_record = store.record_materialization(pending).await?;
    assert_eq!(pending_record.state, MaterializationState::Pending);

    let running = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(1),
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await?;
    assert_eq!(running.generation, Generation::new(2));
    assert_eq!(running.state, InstanceState::Running);

    let mut ready = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(2),
        target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(2),
    );
    ready.backend = Some(BackendEndpoint::new("http://10.0.0.10:8080").expect("valid backend"));
    ready.rendered_objects = vec![RenderedObjectRef {
        api_version: "apps/v1".to_owned(),
        kind: "Deployment".to_owned(),
        namespace: "default".to_owned(),
        name: "instance-a".to_owned(),
    }];
    let ready_record = store.record_materialization(ready).await?;
    assert_eq!(ready_record.state, MaterializationState::Ready);
    assert_eq!(ready_record.backend_generation, BackendGeneration::new(2));
    assert_eq!(
        ready_record.backend.as_ref().map(BackendEndpoint::uri),
        Some("http://10.0.0.10:8080")
    );

    let dependencies = store
        .lookup_route_dependencies(RouteDependencyLookup::new(
            route_entry.route_binding_id.clone(),
        ))
        .await?
        .expect("route dependencies load");
    assert_eq!(dependencies.instance_id.as_str(), "instance-a");
    assert_eq!(
        dependencies.materialization_generation,
        Some(BackendGeneration::new(2))
    );

    let resolved_with_backend = store
        .resolve_route(ResolveRouteRequest::new(
            RouteIdentity::Http {
                host: RouteHost::exact("app.example.com").expect("valid host"),
                path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
            },
            MaterializationTarget::new("cluster-a", "default").unwrap(),
        ))
        .await?;
    match resolved_with_backend {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.10:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(2)));
        }
        RouteResolution::Miss { .. } => panic!("route should resolve after materialization"),
    }

    let mut rewind = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(2),
        target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(1),
    );
    rewind.backend = Some(BackendEndpoint::new("http://10.0.0.9:8080").expect("valid backend"));
    let rewind_error = store
        .record_materialization(rewind)
        .await
        .expect_err("lower backend generations must be rejected");
    match rewind_error {
        StoreError::InvalidArgument { message } => {
            assert!(message.contains("backend generation rewind"));
        }
        other => panic!("expected backend rewind invalid argument, got {other}"),
    }

    let resolved_after_rewind_rejection = store
        .resolve_route(ResolveRouteRequest::new(
            RouteIdentity::Http {
                host: RouteHost::exact("app.example.com").expect("valid host"),
                path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
            },
            MaterializationTarget::new("cluster-a", "default").unwrap(),
        ))
        .await?;
    match resolved_after_rewind_rejection {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.10:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(2)));
        }
        RouteResolution::Miss { .. } => panic!("route should still resolve"),
    }

    let draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::SleepRequested,
        ))
        .await?;
    assert_eq!(draining.generation, Generation::new(3));

    let stale_sidecar_report = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::IdleReported,
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

    let resolved_after_generation_advance = store
        .resolve_route(ResolveRouteRequest::new(
            RouteIdentity::Http {
                host: RouteHost::exact("app.example.com").expect("valid host"),
                path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
            },
            MaterializationTarget::new("cluster-a", "default").unwrap(),
        ))
        .await?;
    match resolved_after_generation_advance {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(entry.instance_generation, Generation::new(3));
            assert_eq!(entry.backend, None);
            assert_eq!(entry.backend_generation, None);
        }
        RouteResolution::Miss { .. } => panic!("route binding should still resolve"),
    }

    let dependencies_after_generation_advance = store
        .lookup_route_dependencies(RouteDependencyLookup::new(
            route_entry.route_binding_id.clone(),
        ))
        .await?
        .expect("route dependencies still load");
    assert_eq!(
        dependencies_after_generation_advance.materialization_generation,
        None
    );

    let mut stale_materialization = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(2),
        target,
        MaterializationState::Ready,
        BackendGeneration::new(3),
    );
    stale_materialization.backend =
        Some(BackendEndpoint::new("http://10.0.0.11:8080").expect("valid backend"));
    let stale_materialization_error = store
        .record_materialization(stale_materialization)
        .await
        .expect_err("stale instance generation materialization is rejected");
    match stale_materialization_error {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(2));
            assert_eq!(actual, Generation::new(3));
        }
        other => panic!("expected stale materialization generation conflict, got {other}"),
    }

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

    let expired = store
        .expire_http01_challenges(
            ExpireHttp01ChallengesRequest::new(SystemTime::now()).with_limit(1),
        )
        .await?;
    assert_eq!(expired, 1);
    assert!(store.resolve_http01_challenge(expired_key).await?.is_none());

    Ok(())
}

async fn exercise_instance_lifecycle(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let invalid_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-invalid",
        "instance-lifecycle-invalid",
    )
    .await?;
    assert_eq!(invalid_target.instance.generation, Generation::new(0));

    let invalid = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            invalid_target.instance.id.clone(),
            Generation::new(0),
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await
        .expect_err("cold instances cannot become running directly");
    assert!(matches!(invalid, StoreError::InvalidArgument { .. }));
    let after_invalid = store
        .get_instance(GetInstanceRequest::new(invalid_target.instance.id.clone()))
        .await?
        .expect("invalid transition target still exists");
    assert_eq!(after_invalid.state, InstanceState::Cold);
    assert_eq!(after_invalid.generation, Generation::new(0));

    let concurrent_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-concurrent",
        "instance-lifecycle-concurrent",
    )
    .await?;
    let wake_a = CompareAndSwapInstanceStateRequest::new(
        concurrent_target.instance.id.clone(),
        Generation::new(0),
        InstanceState::Waking,
        StateTransitionReason::WakeRequested,
    );
    let wake_b = wake_a.clone();
    let (first, second) = tokio::join!(
        store.compare_and_swap_instance_state(wake_a),
        store.compare_and_swap_instance_state(wake_b)
    );
    let mut successes = 0;
    let mut conflicts = 0;
    for result in [first, second] {
        match result {
            Ok(record) => {
                successes += 1;
                assert_eq!(record.state, InstanceState::Waking);
                assert_eq!(record.generation, Generation::new(1));
            }
            Err(StoreError::GenerationConflict { expected, actual }) => {
                conflicts += 1;
                assert_eq!(expected, Generation::new(0));
                assert_eq!(actual, Generation::new(1));
            }
            Err(other) => panic!("expected wake success or generation conflict, got {other}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(conflicts, 1);

    let sleep_while_waking = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-sleep-waking",
        "instance-lifecycle-sleep-waking",
    )
    .await?;
    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            sleep_while_waking.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(waking.generation, Generation::new(1));
    let sleep_during_wake = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            sleep_while_waking.instance.id.clone(),
            Generation::new(1),
            InstanceState::Draining,
            StateTransitionReason::SleepRequested,
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
    let deleting_from_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            sleep_while_waking.instance.id,
            Generation::new(1),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting_from_waking.state, InstanceState::Deleting);
    assert_eq!(deleting_from_waking.generation, Generation::new(2));

    let drain_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-drain-complete",
        "instance-lifecycle-drain-complete",
    )
    .await?;
    let running = wake_to_running(store, drain_target.instance.id.clone()).await?;
    assert_eq!(running.generation, Generation::new(2));
    let draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            drain_target.instance.id.clone(),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::IdleReported,
        ))
        .await?;
    assert_eq!(draining.generation, Generation::new(3));
    let cold = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            drain_target.instance.id,
            Generation::new(3),
            InstanceState::Cold,
            StateTransitionReason::DrainCompleted,
        ))
        .await?;
    assert_eq!(cold.state, InstanceState::Cold);
    assert_eq!(cold.generation, Generation::new(4));

    let delete_draining_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-delete-draining",
        "instance-lifecycle-delete-draining",
    )
    .await?;
    wake_to_running(store, delete_draining_target.instance.id.clone()).await?;
    let draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            delete_draining_target.instance.id.clone(),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::SleepRequested,
        ))
        .await?;
    assert_eq!(draining.generation, Generation::new(3));
    let deleting_from_draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            delete_draining_target.instance.id,
            Generation::new(3),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting_from_draining.state, InstanceState::Deleting);
    assert_eq!(deleting_from_draining.generation, Generation::new(4));

    let failed_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-failed-retry",
        "instance-lifecycle-failed-retry",
    )
    .await?;
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            failed_target.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let failed = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            failed_target.instance.id.clone(),
            Generation::new(1),
            InstanceState::Failed,
            StateTransitionReason::FailureReported("readiness timeout".to_owned()),
        ))
        .await?;
    assert_eq!(failed.state, InstanceState::Failed);
    assert_eq!(failed.generation, Generation::new(2));
    let retry = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            failed_target.instance.id,
            Generation::new(2),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(retry.state, InstanceState::Waking);
    assert_eq!(retry.generation, Generation::new(3));

    let terminal_target = create_lifecycle_instance(
        store,
        workload_class,
        "idem-lifecycle-terminal",
        "instance-lifecycle-terminal",
    )
    .await?;
    let deleting = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            terminal_target.instance.id.clone(),
            Generation::new(0),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting.generation, Generation::new(1));
    let deleted = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            terminal_target.instance.id.clone(),
            Generation::new(1),
            InstanceState::Deleted,
            StateTransitionReason::DeleteFinalized,
        ))
        .await?;
    assert_eq!(deleted.state, InstanceState::Deleted);
    assert_eq!(deleted.generation, Generation::new(2));
    let terminal_wake = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            terminal_target.instance.id.clone(),
            Generation::new(2),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await
        .expect_err("deleted instances are terminal");
    assert!(matches!(terminal_wake, StoreError::InvalidArgument { .. }));
    let still_deleted = store
        .get_instance(GetInstanceRequest::new(terminal_target.instance.id))
        .await?
        .expect("deleted terminal target still exists");
    assert_eq!(still_deleted.state, InstanceState::Deleted);
    assert_eq!(still_deleted.generation, Generation::new(2));

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
            "idem-lease-clock",
            "instance-lease-clock",
            workload_class,
            vec![],
        ))
        .await?;
    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            created.instance.id.clone(),
            created.instance.generation,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let pending = store
        .record_materialization(RecordMaterializationRequest::new(
            waking.id.clone(),
            waking.generation,
            target.clone(),
            MaterializationState::Pending,
            BackendGeneration::new(1),
        ))
        .await?;

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

    let claimed = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending.id.clone(),
            "lease-clock-owner",
            Duration::from_secs(600),
        ))
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
        let error = store
            .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
                pending.id.clone(),
                "lease-clock-owner",
                ttl,
            ))
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
    idempotency_key: &str,
    instance_id: &str,
) -> Result<control_plane::CreateInstanceResult, StoreError> {
    store
        .create_instance(create_instance_request(
            idempotency_key,
            instance_id,
            workload_class,
            vec![],
        ))
        .await
}

async fn create_exclusive_instance(
    store: &PostgresStore,
    workload_class: &WorkloadClassVersionRef,
    idempotency_key: &str,
    instance_id: &str,
    volume_handle: &str,
    license_handle: &str,
) -> Result<control_plane::CreateInstanceResult, StoreError> {
    store
        .create_instance(
            create_instance_request(idempotency_key, instance_id, workload_class.clone(), vec![])
                .with_values(BTreeMap::from([
                    ("tenant".to_owned(), instance_id.to_owned()),
                    ("volume_handle".to_owned(), volume_handle.to_owned()),
                    ("license_handle".to_owned(), license_handle.to_owned()),
                ])),
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
        "idem-collision-service-owner",
        "instance-collision-service-owner",
    )
    .await?;
    let owner_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let service_target =
        MaterializationTarget::new("cluster-collision", "apps").expect("valid target");
    let mut owner_materialization = RecordMaterializationRequest::new(
        owner.instance.id.clone(),
        owner_waking.generation,
        service_target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    owner_materialization.rendered_objects =
        vec![object_ref("v1", "Service", "apps", "shared-service")];
    store.record_materialization(owner_materialization).await?;

    let contender = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-collision-service-contender",
        "instance-collision-service-contender",
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
    let mut contender_materialization = RecordMaterializationRequest::new(
        contender.instance.id.clone(),
        contender_waking.generation,
        service_target,
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    contender_materialization.rendered_objects =
        vec![object_ref("v1", "Service", "apps", "shared-service")];
    let service_collision = store
        .record_materialization(contender_materialization)
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
            .expect("contender instance remains")
            .state,
        InstanceState::Waking
    );

    let pv_owner = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-collision-pv-owner",
        "instance-collision-pv-owner",
    )
    .await?;
    let pv_owner_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            pv_owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut pv_owner_materialization = RecordMaterializationRequest::new(
        pv_owner.instance.id.clone(),
        pv_owner_waking.generation,
        MaterializationTarget::new("cluster-collision", "pv-owner").expect("valid target"),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pv_owner_materialization.rendered_objects =
        vec![object_ref("v1", "PersistentVolume", "", "shared-pv")];
    store
        .record_materialization(pv_owner_materialization)
        .await?;

    let pv_contender = create_lifecycle_instance(
        store,
        workload_class,
        "idem-collision-pv-contender",
        "instance-collision-pv-contender",
    )
    .await?;
    let pv_contender_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            pv_contender.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut pv_contender_materialization = RecordMaterializationRequest::new(
        pv_contender.instance.id.clone(),
        pv_contender_waking.generation,
        MaterializationTarget::new("cluster-collision", "pv-contender").expect("valid target"),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pv_contender_materialization.rendered_objects =
        vec![object_ref("v1", "PersistentVolume", "", "shared-pv")];
    let pv_collision = store
        .record_materialization(pv_contender_materialization)
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
    let owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-exclusive-owner",
        "instance-exclusive-owner",
        "disk-a",
        "license-owner",
    )
    .await?;
    let owner_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut owner_materialization = RecordMaterializationRequest::new(
        owner.instance.id.clone(),
        owner_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    owner_materialization.exclusivity_keys = workload_class
        .render_exclusivity_keys(&owner_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let expected_owner_keys = vec![
        control_plane::RenderedExclusivityKey::new("disk", "disk-a"),
        control_plane::RenderedExclusivityKey::new("license", "license-owner"),
    ];
    assert_eq!(owner_materialization.exclusivity_keys, expected_owner_keys);
    let owner_record = store.record_materialization(owner_materialization).await?;
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

    let stale_record = RecordMaterializationRequest::new(
        owner.instance.id.clone(),
        Generation::new(0),
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(2),
    );
    let stale_error = store
        .record_materialization(stale_record)
        .await
        .expect_err("stale instance generation cannot update held keys");
    assert!(matches!(
        stale_error,
        StoreError::GenerationConflict {
            expected,
            actual
        } if expected == Generation::new(0) && actual == owner_waking.generation
    ));

    let contender = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-exclusive-contender",
        "instance-exclusive-contender",
        "disk-a",
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
    let mut contender_materialization = RecordMaterializationRequest::new(
        contender.instance.id.clone(),
        contender_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    contender_materialization.exclusivity_keys = workload_class
        .render_exclusivity_keys(&contender_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let conflict = store
        .record_materialization(contender_materialization.clone())
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
        "idem-exclusive-restart-contender",
        "instance-exclusive-restart-contender",
        "disk-a",
        "license-restart",
    )
    .await?;
    let restart_waking = restarted_store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            restart_contender.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut restart_materialization = RecordMaterializationRequest::new(
        restart_contender.instance.id,
        restart_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    restart_materialization.exclusivity_keys = workload_class
        .render_exclusivity_keys(&restart_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let restart_conflict = restarted_store
        .record_materialization(restart_materialization)
        .await
        .expect_err("recreated store still sees active held key");
    assert_exclusivity_conflict(restart_conflict, "disk", Some("instance-exclusive-owner"));

    let different = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-exclusive-different",
        "instance-exclusive-different",
        "disk-b",
        "license-different",
    )
    .await?;
    let different_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            different.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut different_materialization = RecordMaterializationRequest::new(
        different.instance.id,
        different_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    different_materialization.exclusivity_keys = workload_class
        .render_exclusivity_keys(&different_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let different_record = store
        .record_materialization(different_materialization)
        .await?;
    assert_eq!(
        different_record.exclusivity_keys,
        vec![
            control_plane::RenderedExclusivityKey::new("disk", "disk-b"),
            control_plane::RenderedExclusivityKey::new("license", "license-different"),
        ]
    );

    let mut complete = CompleteWakeRequest::new(
        owner.instance.id.clone(),
        owner_waking.generation,
        target.clone(),
        BackendEndpoint::new("http://10.0.0.50:8080").expect("valid backend"),
        BackendGeneration::new(2),
    );
    complete.exclusivity_keys = expected_owner_keys.clone();
    let completed = store.complete_wake(complete).await?;
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
    assert_eq!(
        begin
            .materialization
            .as_ref()
            .expect("materialization is marked deleting")
            .exclusivity_keys,
        expected_owner_keys
    );
    let deleting_conflict = store
        .record_materialization(contender_materialization.clone())
        .await
        .expect_err("deleting materialization keeps the key held until finalize");
    assert_exclusivity_conflict(deleting_conflict, "disk", Some("instance-exclusive-owner"));

    let finalized = store
        .finalize_sleep(FinalizeSleepRequest::new(
            owner.instance.id,
            begin.instance.generation,
            target.clone(),
        ))
        .await?;
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

    let acquired_after_release = store
        .record_materialization(contender_materialization)
        .await?;
    assert_eq!(acquired_after_release.instance_id, contender.instance.id);

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
    let owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-exclusive-release-owner",
        "instance-exclusive-release-owner",
        "disk-release",
        "license-release",
    )
    .await?;
    let owner_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let expected_owner_keys = vec![
        control_plane::RenderedExclusivityKey::new("disk", "disk-release"),
        control_plane::RenderedExclusivityKey::new("license", "license-release"),
    ];
    let mut pending = RecordMaterializationRequest::new(
        owner.instance.id.clone(),
        owner_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pending.rendered_objects = vec![object_ref("v1", "PersistentVolume", "", "pv-release")];
    pending.exclusivity_keys = workload_class
        .render_exclusivity_keys(&owner_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    assert_eq!(pending.exclusivity_keys, expected_owner_keys);
    store.record_materialization(pending).await?;

    let mut release = RecordMaterializationRequest::new(
        owner.instance.id.clone(),
        owner_waking.generation,
        target.clone(),
        MaterializationState::Deleted,
        BackendGeneration::new(1),
    );
    release.rendered_objects = Vec::new();
    release.exclusivity_keys = Vec::new();
    let released = store.record_materialization(release).await?;
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

    let failed = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            owner.instance.id,
            owner_waking.generation,
            InstanceState::Failed,
            StateTransitionReason::FailureReported(
                "materialization failed before apply".to_owned(),
            ),
        ))
        .await?;
    assert_eq!(failed.generation, owner_waking.generation.next());

    let contender = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-exclusive-release-contender",
        "instance-exclusive-release-contender",
        "disk-release",
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
    let mut contender_materialization = RecordMaterializationRequest::new(
        contender.instance.id.clone(),
        contender_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    contender_materialization.exclusivity_keys = workload_class
        .render_exclusivity_keys(&contender_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    let acquired = store
        .record_materialization(contender_materialization)
        .await?;
    assert_eq!(acquired.instance_id, contender.instance.id);

    let stale_owner = create_exclusive_instance(
        store,
        &workload_class.reference,
        "idem-exclusive-stale-release-owner",
        "instance-exclusive-stale-release-owner",
        "disk-stale-release",
        "license-stale-release",
    )
    .await?;
    let stale_owner_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut stale_pending = RecordMaterializationRequest::new(
        stale_owner.instance.id.clone(),
        stale_owner_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    stale_pending.exclusivity_keys = workload_class
        .render_exclusivity_keys(&stale_owner_waking.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    store.record_materialization(stale_pending).await?;
    let stale_failed = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_owner.instance.id.clone(),
            stale_owner_waking.generation,
            InstanceState::Failed,
            StateTransitionReason::FailureReported(
                "materialization failed before apply".to_owned(),
            ),
        ))
        .await?;
    let stale_release = RecordMaterializationRequest::new(
        stale_owner.instance.id.clone(),
        stale_owner_waking.generation,
        target.clone(),
        MaterializationState::Deleted,
        BackendGeneration::new(1),
    );
    let stale_release_error = store
        .record_materialization(stale_release)
        .await
        .expect_err("stale generation cannot release pending held keys");
    assert!(matches!(
        stale_release_error,
        StoreError::GenerationConflict {
            expected,
            actual
        } if expected == stale_owner_waking.generation && actual == stale_failed.generation
    ));
    let accepted_release = RecordMaterializationRequest::new(
        stale_owner.instance.id,
        stale_failed.generation,
        target,
        MaterializationState::Deleted,
        BackendGeneration::new(1),
    );
    store.record_materialization(accepted_release).await?;

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
            "idem-reconcile-pending",
            "instance-reconcile-pending",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            created.instance.id.clone(),
            created.instance.generation,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut pending = RecordMaterializationRequest::new(
        waking.id.clone(),
        waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pending.rendered_objects = vec![RenderedObjectRef {
        api_version: "apps/v1".to_owned(),
        kind: "Deployment".to_owned(),
        namespace: "apps".to_owned(),
        name: "instance-reconcile-pending".to_owned(),
    }];
    pending.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", "reconcile-disk")];
    let pending_record = store.record_materialization(pending).await?;
    let loaded_pending = store
        .load_materialization(LoadMaterializationRequest::new(pending_record.id.clone()))
        .await?
        .expect("pending materialization loads by id");
    assert_eq!(loaded_pending.state, MaterializationState::Pending);
    assert_eq!(loaded_pending.rendered_objects.len(), 1);

    let candidates = store
        .list_materialization_reconciliation_candidates(
            ListMaterializationReconciliationCandidatesRequest::new(10),
        )
        .await?;
    assert!(candidates
        .iter()
        .any(|candidate| candidate.id == pending_record.id));

    let owner_a_claim = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending_record.id.clone(),
            "owner-a",
            Duration::from_secs(30),
        ))
        .await?
        .expect("first lease claim succeeds");
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

    let owner_b_claim = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending_record.id.clone(),
            "owner-b",
            Duration::from_secs(30),
        ))
        .await?;
    assert!(owner_b_claim.is_none());

    let same_owner_claim = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending_record.id.clone(),
            "owner-a",
            Duration::from_secs(30),
        ))
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
    let owner_b_takeover = store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            pending_record.id.clone(),
            "owner-b",
            Duration::from_secs(30),
        ))
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
            "idem-reconcile-expired-pending",
            "instance-reconcile-expired-pending",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    let expired_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            expired_instance.instance.id.clone(),
            expired_instance.instance.generation,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut expired_pending_request = RecordMaterializationRequest::new(
        expired_waking.id.clone(),
        expired_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    expired_pending_request.exclusivity_keys =
        vec![RenderedExclusivityKey::new("disk", "expired-pending-disk")];
    let expired_pending = store
        .record_materialization(expired_pending_request)
        .await?;
    store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            expired_pending.id.clone(),
            "expired-owner",
            Duration::from_secs(30),
        ))
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
            "idem-reconcile-race-pending",
            "instance-reconcile-race-pending",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    let race_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            race_instance.instance.id.clone(),
            race_instance.instance.generation,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut stale_race_request = RecordMaterializationRequest::new(
        race_waking.id.clone(),
        race_waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    stale_race_request.exclusivity_keys =
        vec![RenderedExclusivityKey::new("disk", "race-old-disk")];
    let stale_race = store.record_materialization(stale_race_request).await?;
    store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            stale_race.id.clone(),
            "race-owner",
            Duration::from_secs(60),
        ))
        .await?
        .expect("race fixture lease claim succeeds");
    let race_running = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            race_waking.id.clone(),
            race_waking.generation,
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await?;
    let mut newer_race_request = RecordMaterializationRequest::new(
        race_waking.id.clone(),
        race_running.generation,
        target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(2),
    );
    newer_race_request.backend =
        Some(BackendEndpoint::new("http://10.0.0.43:8080").expect("valid backend"));
    newer_race_request.exclusivity_keys =
        vec![RenderedExclusivityKey::new("disk", "race-new-disk")];
    let newer_race = store.record_materialization(newer_race_request).await?;
    assert_eq!(newer_race.id, stale_race.id);
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
    assert!(matches!(stale_race_delete, StoreError::NotFound { .. }));
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
        Some("race-new-disk")
    );

    let delete_instance = store
        .create_instance(create_instance_request(
            "idem-reconcile-delete",
            "instance-reconcile-delete",
            workload_class.clone(),
            vec![],
        ))
        .await?;
    let running = wake_to_running(store, delete_instance.instance.id.clone()).await?;
    let mut ready = RecordMaterializationRequest::new(
        running.id.clone(),
        running.generation,
        target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(5),
    );
    ready.rendered_objects = vec![RenderedObjectRef {
        api_version: "apps/v1".to_owned(),
        kind: "Deployment".to_owned(),
        namespace: "apps".to_owned(),
        name: "instance-reconcile-delete".to_owned(),
    }];
    ready.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", "delete-disk")];
    store.record_materialization(ready).await?;
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
    // Candidate scans and direct claims must both respect the immutable drain
    // deadline. The store reads its own clock, so elapsing the grace period
    // means retiring the stored deadline rather than claiming a later "now".
    let before_grace_candidates = store
        .list_materialization_reconciliation_candidates(
            ListMaterializationReconciliationCandidatesRequest::new(100),
        )
        .await?;
    assert!(!before_grace_candidates
        .iter()
        .any(|candidate| candidate.id == deleting_id));
    assert!(
        store
            .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
                deleting_id.clone(),
                "delete-owner-a",
                Duration::from_secs(30),
            ))
            .await?
            .is_none(),
        "manual claim must not bypass drain grace"
    );
    raw.execute("UPDATE materializations SET drain_not_before_unix_millis = 0, next_attempt_at_unix_millis = 0 WHERE materialization_id = $1", &[&deleting_id.as_str()]).await.map_err(|e| StoreError::internal(e.to_string()))?;
    let after_grace_candidates = store
        .list_materialization_reconciliation_candidates(
            ListMaterializationReconciliationCandidatesRequest::new(100),
        )
        .await?;
    assert!(after_grace_candidates
        .iter()
        .any(|candidate| candidate.id == deleting_id));
    store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            deleting_id.clone(),
            "delete-owner-a",
            Duration::from_secs(30),
        ))
        .await?
        .expect("delete lease claim succeeds after grace");
    let stale_finalize = store
        .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
            deleting_id.clone(),
            "delete-owner-b",
            1,
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
            1,
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
            "idem-reconcile-expired-delete",
            "instance-reconcile-expired-delete",
            workload_class,
            vec![],
        ))
        .await?;
    let expired_delete_running =
        wake_to_running(store, expired_delete_instance.instance.id.clone()).await?;
    let mut expired_ready = RecordMaterializationRequest::new(
        expired_delete_running.id.clone(),
        expired_delete_running.generation,
        target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(8),
    );
    expired_ready.exclusivity_keys =
        vec![RenderedExclusivityKey::new("disk", "expired-delete-disk")];
    store.record_materialization(expired_ready).await?;
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
    store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            expired_deleting.id.clone(),
            "expired-delete-owner",
            Duration::from_secs(30),
        ))
        .await?
        .expect("expired delete lease fixture claim succeeds relative to request clock");
    raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&expired_deleting.id.as_str()]).await.map_err(|e| StoreError::internal(e.to_string()))?;
    let expired_finalize = store
        .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
            expired_deleting.id.clone(),
            "expired-delete-owner",
            1,
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
            "idem-reconcile-force",
            "instance-reconcile-force",
            WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            vec![],
        ))
        .await?;
    let force_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            force_instance.instance.id.clone(),
            force_instance.instance.generation,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let force_target =
        MaterializationTarget::new("cluster-force-release", "apps").expect("valid target");
    let mut force_pending = RecordMaterializationRequest::new(
        force_waking.id,
        force_waking.generation,
        force_target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    force_pending.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", "force-disk")];
    store.record_materialization(force_pending).await?;
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

async fn wake_to_running(
    store: &PostgresStore,
    instance_id: InstanceId,
) -> Result<control_plane::InstanceRecord, StoreError> {
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            instance_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            instance_id,
            Generation::new(1),
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await
}

async fn exercise_complete_wake(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let success = store
        .create_instance(create_instance_request(
            "idem-complete-wake-success",
            "instance-complete-wake-success",
            workload_class.clone(),
            vec![http_route("complete-wake.example.com", None)],
        ))
        .await?;
    let instance_id = success.instance.id.clone();
    let route_binding_id = success.route_bindings[0].id.clone();
    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            instance_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(waking.state, InstanceState::Waking);
    assert_eq!(waking.generation, Generation::new(1));

    let target = MaterializationTarget::new("cluster-complete", "apps").expect("valid target");
    let pending = RecordMaterializationRequest::new(
        instance_id.clone(),
        waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(6),
    );
    let pending_record = store.record_materialization(pending).await?;
    assert_eq!(pending_record.instance_generation, Generation::new(1));
    assert_eq!(pending_record.backend_generation, BackendGeneration::new(6));

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
    let mut complete = CompleteWakeRequest::new(
        instance_id.clone(),
        waking.generation,
        target.clone(),
        BackendEndpoint::new("http://10.0.0.20:8080").expect("valid backend"),
        BackendGeneration::new(7),
    );
    complete.rendered_objects = rendered_objects.clone();
    let completed = store.complete_wake(complete).await?;
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
        BackendGeneration::new(7)
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
    let non_ready_target =
        MaterializationTarget::new("cluster-complete", "pending").expect("valid target");
    store
        .record_materialization(RecordMaterializationRequest::new(
            completed.instance.id.clone(),
            completed.instance.generation,
            non_ready_target.clone(),
            MaterializationState::Pending,
            BackendGeneration::new(1),
        ))
        .await?;
    assert!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                completed.instance.id.clone(),
                completed.instance.generation,
                non_ready_target,
            ))
            .await?
            .is_none(),
        "non-ready materialization must not load"
    );

    match store
        .resolve_route(ResolveRouteRequest::new(
            http_identity("complete-wake.example.com", None),
            MaterializationTarget::new("cluster-complete", "apps").unwrap(),
        ))
        .await?
    {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(entry.route_binding_id, route_binding_id);
            assert_eq!(entry.instance_id, completed.instance.id);
            assert_eq!(entry.instance_state, InstanceState::Running);
            assert_eq!(entry.instance_generation, Generation::new(2));
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.20:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(7)));
        }
        RouteResolution::Miss { .. } => panic!("route should resolve after complete_wake"),
    }
    let dependencies = store
        .lookup_route_dependencies(RouteDependencyLookup::new(route_binding_id))
        .await?
        .expect("route dependencies load after complete_wake");
    assert_eq!(
        dependencies.materialization_generation,
        Some(BackendGeneration::new(7))
    );

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
        .expect("active materialization is marked deleting");
    assert_eq!(
        deleting_materialization.state,
        MaterializationState::Deleting
    );
    assert_eq!(deleting_materialization.backend, None);
    assert_eq!(deleting_materialization.rendered_objects, rendered_objects);
    let active = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            completed.instance.id.clone(),
            target.clone(),
        ))
        .await?
        .expect("deleting materialization remains active until cleanup finalizes");
    assert_eq!(active.state, MaterializationState::Deleting);

    let sleep_finalized = store
        .finalize_sleep(FinalizeSleepRequest::new(
            completed.instance.id.clone(),
            sleep_started.instance.generation,
            target.clone(),
        ))
        .await?;
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

    let stale_sleep = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-begin-sleep-stale-materialization",
        "instance-begin-sleep-stale-materialization",
    )
    .await?;
    let stale_sleep_id = stale_sleep.instance.id.clone();
    let stale_sleep_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_sleep_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let stale_sleep_target =
        MaterializationTarget::new("cluster-complete", "stale-sleep").expect("valid target");
    let stale_sleep_materialization = store
        .record_materialization(RecordMaterializationRequest::new(
            stale_sleep_id.clone(),
            stale_sleep_waking.generation,
            stale_sleep_target.clone(),
            MaterializationState::Ready,
            BackendGeneration::new(1),
        ))
        .await?;
    assert_eq!(
        stale_sleep_materialization.instance_generation,
        Generation::new(1)
    );
    let stale_sleep_running = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_sleep_id.clone(),
            stale_sleep_waking.generation,
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await?;
    assert_eq!(stale_sleep_running.generation, Generation::new(2));
    let stale_sleep_error = store
        .begin_sleep(BeginSleepRequest::new(
            stale_sleep_id.clone(),
            stale_sleep_running.generation,
            stale_sleep_target.clone(),
        ))
        .await
        .expect_err("begin_sleep rejects stale active materialization generation");
    match stale_sleep_error {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(2));
            assert_eq!(actual, Generation::new(1));
        }
        other => panic!("expected stale materialization generation conflict, got {other}"),
    }
    let active_after_rejected_sleep = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            stale_sleep_id,
            stale_sleep_target,
        ))
        .await?
        .expect("stale materialization remains active after rejected sleep");
    assert_eq!(
        active_after_rejected_sleep.state,
        MaterializationState::Ready
    );
    assert_eq!(
        active_after_rejected_sleep.instance_generation,
        Generation::new(1)
    );

    let stale = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-complete-wake-stale",
        "instance-complete-wake-stale",
    )
    .await?;
    let stale_id = stale.instance.id.clone();
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let stale_target =
        MaterializationTarget::new("cluster-complete", "stale").expect("valid target");
    let stale_error = store
        .complete_wake(CompleteWakeRequest::new(
            stale_id.clone(),
            Generation::new(0),
            stale_target.clone(),
            BackendEndpoint::new("http://10.0.0.30:8080").expect("valid backend"),
            BackendGeneration::new(5),
        ))
        .await
        .expect_err("stale complete_wake generation is rejected");
    match stale_error {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(0));
            assert_eq!(actual, Generation::new(1));
        }
        other => panic!("expected stale complete_wake generation conflict, got {other}"),
    }
    let stale_after = store
        .get_instance(GetInstanceRequest::new(stale_id.clone()))
        .await?
        .expect("stale complete_wake target still exists");
    assert_eq!(stale_after.state, InstanceState::Waking);
    assert_eq!(stale_after.generation, Generation::new(1));
    let stale_probe = RecordMaterializationRequest::new(
        stale_id,
        Generation::new(1),
        stale_target,
        MaterializationState::Pending,
        BackendGeneration::new(4),
    );
    let stale_probe_record = store.record_materialization(stale_probe).await?;
    assert_eq!(
        stale_probe_record.backend_generation,
        BackendGeneration::new(4)
    );

    let cold = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-complete-wake-cold",
        "instance-complete-wake-cold",
    )
    .await?;
    let cold_id = cold.instance.id.clone();
    let cold_target = MaterializationTarget::new("cluster-complete", "cold").expect("valid target");
    let cold_error = store
        .complete_wake(CompleteWakeRequest::new(
            cold_id.clone(),
            Generation::new(0),
            cold_target.clone(),
            BackendEndpoint::new("http://10.0.0.31:8080").expect("valid backend"),
            BackendGeneration::new(3),
        ))
        .await
        .expect_err("cold instances cannot complete wake");
    assert!(matches!(cold_error, StoreError::InvalidArgument { .. }));
    let cold_after = store
        .get_instance(GetInstanceRequest::new(cold_id.clone()))
        .await?
        .expect("cold complete_wake target still exists");
    assert_eq!(cold_after.state, InstanceState::Cold);
    assert_eq!(cold_after.generation, Generation::new(0));
    let cold_probe = RecordMaterializationRequest::new(
        cold_id,
        Generation::new(0),
        cold_target,
        MaterializationState::Pending,
        BackendGeneration::new(2),
    );
    let cold_probe_record = store.record_materialization(cold_probe).await?;
    assert_eq!(
        cold_probe_record.backend_generation,
        BackendGeneration::new(2)
    );

    let rewind = store
        .create_instance(create_instance_request(
            "idem-complete-wake-rewind",
            "instance-complete-wake-rewind",
            workload_class,
            vec![http_route("complete-wake-rewind.example.com", None)],
        ))
        .await?;
    let rewind_id = rewind.instance.id.clone();
    let rewind_route_binding_id = rewind.route_bindings[0].id.clone();
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            rewind_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let rewind_target =
        MaterializationTarget::new("cluster-complete", "rewind").expect("valid target");
    let mut old_ready = RecordMaterializationRequest::new(
        rewind_id.clone(),
        Generation::new(1),
        rewind_target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(9),
    );
    old_ready.backend = Some(BackendEndpoint::new("http://10.0.0.40:8080").expect("valid backend"));
    old_ready.rendered_objects = vec![RenderedObjectRef {
        api_version: "apps/v1".to_owned(),
        kind: "Deployment".to_owned(),
        namespace: "rewind".to_owned(),
        name: "old-ready".to_owned(),
    }];
    store.record_materialization(old_ready).await?;
    let rewind_error = store
        .complete_wake(CompleteWakeRequest::new(
            rewind_id.clone(),
            Generation::new(1),
            rewind_target.clone(),
            BackendEndpoint::new("http://10.0.0.41:8080").expect("valid backend"),
            BackendGeneration::new(8),
        ))
        .await
        .expect_err("complete_wake rejects backend generation rewinds");
    match rewind_error {
        StoreError::InvalidArgument { message } => {
            assert!(message.contains("backend generation rewind"));
        }
        other => panic!("expected backend rewind invalid argument, got {other}"),
    }
    let rewind_after = store
        .get_instance(GetInstanceRequest::new(rewind_id.clone()))
        .await?
        .expect("rewind complete_wake target still exists");
    assert_eq!(rewind_after.state, InstanceState::Waking);
    assert_eq!(rewind_after.generation, Generation::new(1));
    match store
        .resolve_route(ResolveRouteRequest::new(
            http_identity("complete-wake-rewind.example.com", None),
            MaterializationTarget::new("cluster-complete", "rewind").unwrap(),
        ))
        .await?
    {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(entry.route_binding_id, rewind_route_binding_id);
            assert_eq!(entry.instance_state, InstanceState::Waking);
            assert_eq!(entry.instance_generation, Generation::new(1));
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.40:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(9)));
        }
        RouteResolution::Miss { .. } => {
            panic!("route should still resolve to the unchanged old materialization")
        }
    }
    let lower_backend_generation = RecordMaterializationRequest::new(
        rewind_id,
        Generation::new(1),
        rewind_target,
        MaterializationState::Ready,
        BackendGeneration::new(8),
    );
    let lower_error = store
        .record_materialization(lower_backend_generation)
        .await
        .expect_err("old materialization backend generation remains newer");
    assert!(matches!(lower_error, StoreError::InvalidArgument { .. }));

    Ok(())
}

async fn exercise_route_bindings(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let route_instance = store
        .create_instance(create_instance_request(
            "idem-route-instance",
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
            "idem-route-exact-root",
            "route-exact-root",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::exact("App.Routes.Example.COM.").expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
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

    let replayed_exact = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-root",
            "route-exact-root",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::exact("app.routes.example.com").expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
        ))
        .await?;
    assert_eq!(replayed_exact, exact_root);

    let idempotency_conflict = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-root",
            "route-conflicting-replay",
            "instance-routes",
            http_identity("conflict.routes.example.com", None),
            ProtocolRoute::Http,
        ))
        .await
        .expect_err("same idempotency key with different route payload conflicts");
    assert!(matches!(
        idempotency_conflict,
        StoreError::IdempotencyConflict
    ));

    let duplicate_identity = store
        .create_route_binding(create_route_binding_request(
            "idem-route-duplicate-identity",
            "route-duplicate-identity",
            "instance-routes",
            http_identity("APP.ROUTES.EXAMPLE.COM.", None),
            ProtocolRoute::Http,
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
            "idem-route-duplicate-id",
            "route-exact-root",
            "instance-routes",
            http_identity("duplicate-id.routes.example.com", None),
            ProtocolRoute::Http,
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
            "idem-route-missing-instance",
            "route-missing-instance",
            "missing-instance",
            http_identity("missing.routes.example.com", None),
            ProtocolRoute::Http,
        ))
        .await
        .expect_err("route binding must point at an existing instance");
    assert!(matches!(
        missing_instance,
        StoreError::NotFound {
            resource: "referenced resource"
        }
    ));

    let protocol_mismatch = store
        .create_route_binding(create_route_binding_request(
            "idem-route-protocol-mismatch",
            "route-protocol-mismatch",
            "instance-routes",
            http_identity("mismatch.routes.example.com", None),
            ProtocolRoute::TlsSni,
        ))
        .await
        .expect_err("route identity and protocol must be compatible");
    assert!(matches!(
        protocol_mismatch,
        StoreError::InvalidArgument { .. }
    ));

    let exact_api = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-api",
            "route-exact-api",
            "instance-routes",
            http_identity("app.routes.example.com", Some("/api")),
            ProtocolRoute::Http,
        ))
        .await?;
    let exact_api_v1 = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-api-v1",
            "route-exact-api-v1",
            "instance-routes",
            http_identity("app.routes.example.com", Some("/api/v1")),
            ProtocolRoute::Http,
        ))
        .await?;
    let wildcard_broad = store
        .create_route_binding(create_route_binding_request(
            "idem-route-wildcard-broad",
            "route-wildcard-broad",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("routes.example.com").expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
        ))
        .await?;
    let wildcard_specific = store
        .create_route_binding(create_route_binding_request(
            "idem-route-wildcard-specific",
            "route-wildcard-specific",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("customer.routes.example.com")
                    .expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
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
            "idem-route-sni-exact",
            "route-sni-exact",
            "instance-routes",
            sni_identity("DB.Routes.Example.COM."),
            ProtocolRoute::TlsSni,
        ))
        .await?;
    let sni_wildcard = store
        .create_route_binding(create_route_binding_request(
            "idem-route-sni-wildcard",
            "route-sni-wildcard",
            "instance-routes",
            RouteIdentity::Sni {
                host: RouteHost::wildcard_suffix("routes.example.com").expect("valid host"),
            },
            ProtocolRoute::TlsSni,
        ))
        .await?;
    let duplicate_sni = store
        .create_route_binding(create_route_binding_request(
            "idem-route-sni-duplicate",
            "route-sni-duplicate",
            "instance-routes",
            sni_identity("db.routes.example.com"),
            ProtocolRoute::TlsSni,
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

    let miss = store
        .resolve_route(ResolveRouteRequest::new(
            http_identity("routes.example.com", None),
            MaterializationTarget::new("cluster-a", "default").unwrap(),
        ))
        .await?;
    match miss {
        RouteResolution::Miss { negative_cache } => {
            assert!(negative_cache.ttl() > Duration::from_secs(0));
        }
        RouteResolution::Resolved { entry, .. } => {
            panic!("base wildcard suffix should not match itself: {entry:?}")
        }
    }

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

async fn assert_resolves_to(
    store: &PostgresStore,
    identity: RouteIdentity,
    expected_route_binding_id: &RouteBindingId,
) -> Result<(), StoreError> {
    match store
        .resolve_route(ResolveRouteRequest::new(
            identity,
            MaterializationTarget::new("cluster-a", "default").unwrap(),
        ))
        .await?
    {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(&entry.route_binding_id, expected_route_binding_id);
            Ok(())
        }
        RouteResolution::Miss { .. } => panic!("route should resolve"),
    }
}

fn workload_class(class_id: &str, version: u64) -> WorkloadClassVersion {
    let image = format!("example/app:{version}");

    WorkloadClassVersion {
        reference: WorkloadClassVersionRef::new(
            WorkloadClassId::new(class_id).expect("valid workload class ID"),
            Generation::new(version),
        ),
        template_generation: Generation::new(1),
        template: workload_manifest_template(),
        default_values: BTreeMap::from([("image".to_owned(), image.clone())]),
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
        },
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: None,
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
    idempotency_key: &str,
    instance_id: &str,
    workload_class: WorkloadClassVersionRef,
    route_bindings: Vec<RouteBindingSpec>,
) -> CreateInstanceRequest {
    CreateInstanceRequest::new(
        IdempotencyKey::new(idempotency_key).expect("valid idempotency key"),
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
    idempotency_key: &str,
    route_binding_id: &str,
    instance_id: &str,
    identity: RouteIdentity,
    protocol: ProtocolRoute,
) -> CreateRouteBindingRequest {
    CreateRouteBindingRequest::new(
        IdempotencyKey::new(idempotency_key).expect("valid idempotency key"),
        RouteBindingId::new(route_binding_id).expect("valid route binding ID"),
        InstanceId::new(instance_id).expect("valid instance ID"),
        identity,
        protocol,
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

fn http_route(host: &str, path: Option<&str>) -> RouteBindingSpec {
    RouteBindingSpec::new(
        RouteIdentity::Http {
            host: RouteHost::exact(host).expect("valid host"),
            path: path.map(|path| PathPrefix::new(path).expect("valid path prefix")),
        },
        ProtocolRoute::Http,
    )
}

fn sni_route(host: &str) -> RouteBindingSpec {
    RouteBindingSpec::new(
        RouteIdentity::Sni {
            host: RouteHost::exact(host).expect("valid host"),
        },
        ProtocolRoute::TlsSni,
    )
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
    phase5_idempotency_lifetime(store, config, &class).await?;
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
    let store = PostgresStore::connect(&config).await?;
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
    let mut requests = Vec::new();
    for index in 0..26 {
        let id = format!("phase5-reservation-{index}");
        let instance = create_lifecycle_instance(store, class.reference.clone(), &id, &id)
            .await?
            .instance;
        let mut request = RecordMaterializationRequest::new(
            instance.id,
            instance.generation,
            MaterializationTarget::new("phase5", "apps")?,
            MaterializationState::Pending,
            BackendGeneration::new(1),
        );
        request.rendered_objects = vec![object_ref("v1", "Service", "apps", &id)];
        request.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", &id)];
        requests.push(request);
    }
    let held = store.record_materialization(requests[0].clone()).await?;
    let mut raw = raw_client(config).await?;
    let transaction = raw.transaction().await?;
    transaction.execute("UPDATE materializations SET updated_at_unix_millis = updated_at_unix_millis + 1 WHERE materialization_id = $1", &[&held.id.as_str()]).await?;
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
        tasks.spawn(async move { store.record_materialization(request).await });
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await?;
    let elapsed = started.elapsed();
    transaction.rollback().await?;
    // Two transactions racing to acquire one key must produce one owner.
    let mut left = requests[24].clone();
    let mut right = requests[25].clone();
    left.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", "shared-race")];
    right.exclusivity_keys = left.exclusivity_keys.clone();
    let (left, right) = tokio::join!(
        store.record_materialization(left),
        store.record_materialization(right)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(matches!(
        left.as_ref().err().or(right.as_ref().err()),
        Some(StoreError::ExclusivityConflict { .. })
    ));
    // An object has one Kubernetes API-group identity across API versions.
    let mut other_version = requests[25].clone();
    other_version.exclusivity_keys.clear();
    other_version.rendered_objects = vec![object_ref(
        "apps/v1",
        "Deployment",
        "apps",
        "shared-version",
    )];
    let mut owner = requests[24].clone();
    owner.exclusivity_keys.clear();
    owner.rendered_objects = vec![object_ref(
        "apps/v1beta1",
        "Deployment",
        "apps",
        "shared-version",
    )];
    store.record_materialization(owner).await?;
    assert!(matches!(
        store.record_materialization(other_version).await,
        Err(StoreError::InvalidArgument { .. })
    ));
    // Renewal and release preserve state age; retry queue placement is separate.
    let age_before: i64 = raw.query_one("SELECT state_entered_at_unix_millis FROM materializations WHERE materialization_id = $1", &[&held.id.as_str()]).await?.get(0);
    store
        .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
            held.id.clone(),
            "age-owner",
            Duration::from_secs(60),
        ))
        .await?
        .unwrap();
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

async fn phase5_idempotency_lifetime(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
    class: &WorkloadClassVersion,
) -> TestResult {
    let request = create_instance_request(
        "phase5-permanent",
        "phase5-permanent",
        class.reference.clone(),
        vec![],
    );
    let created = store.create_instance(request.clone()).await?;
    let deleting = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            created.instance.id.clone(),
            created.instance.generation,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    store
        .delete_instance(DeleteInstanceRequest::new(deleting.id))
        .await?;
    assert!(matches!(
        store.create_instance(request.clone()).await,
        Err(StoreError::IdempotencyResourceDeleted {
            resource: "instance"
        })
    ));
    let recreated = create_instance_request(
        "phase5-recreated",
        "phase5-permanent",
        class.reference.clone(),
        vec![],
    );
    store.create_instance(recreated).await?;
    assert!(
        matches!(
            store.create_instance(request).await,
            Err(StoreError::IdempotencyResourceDeleted { .. })
        ),
        "old key never replays a same-name replacement"
    );
    let route = create_route_binding_request(
        "phase5-route-key",
        "phase5-route",
        "phase5-permanent",
        http_identity("phase5.example.com", None),
        ProtocolRoute::Http,
    );
    store.create_route_binding(route.clone()).await?;
    store
        .delete_route_binding(DeleteRouteBindingRequest::new(
            route.route_binding_id.clone(),
        ))
        .await?;
    assert!(matches!(
        store.create_route_binding(route).await,
        Err(StoreError::IdempotencyResourceDeleted {
            resource: "route binding"
        })
    ));
    let mut finite = config.clone();
    finite.idempotency_retention = Some(Duration::from_secs(60));
    let finite_store = PostgresStore::connect(&finite).await?;
    let finite_request = create_instance_request(
        "phase5-finite",
        "phase5-finite",
        class.reference.clone(),
        vec![],
    );
    finite_store.create_instance(finite_request.clone()).await?;
    let raw = raw_client(config).await?;
    let before = raw.query_one("SELECT created_at_unix_millis, expires_at_unix_millis FROM idempotency_records WHERE idempotency_key = 'phase5-finite'", &[]).await?;
    let expiry: i64 = before.get(1);
    let created_at: i64 = before.get(0);
    assert!((59_999..=60_001).contains(&(expiry - created_at)));
    finite_store.create_instance(finite_request.clone()).await?;
    assert_eq!(expiry, raw.query_one("SELECT expires_at_unix_millis FROM idempotency_records WHERE idempotency_key = 'phase5-finite'", &[]).await?.get::<_, i64>(0));
    assert_eq!(finite_store.expire_idempotency_records(1).await?, 0);
    raw.execute("UPDATE idempotency_records SET expires_at_unix_millis = 0 WHERE idempotency_key IN ('phase5-finite', 'phase5-route-key')", &[]).await?;
    assert_eq!(store.expire_idempotency_records(1).await?, 1);
    assert_eq!(store.expire_idempotency_records(1).await?, 1);
    assert_eq!(store.expire_idempotency_records(1).await?, 0);
    assert_eq!(
        raw.query_one(
            "SELECT count(*) FROM idempotency_records WHERE idempotency_key = 'phase5-permanent'",
            &[]
        )
        .await?
        .get::<_, i64>(0),
        1
    );
    // A key is reusable after its explicit window expires, with a different
    // resource ID so live-resource uniqueness remains independently enforced.
    let changed = create_instance_request(
        "phase5-finite",
        "phase5-after-expiry",
        class.reference.clone(),
        vec![],
    );
    assert!(
        !finite_store
            .create_instance(changed)
            .await?
            .idempotency_replayed
    );
    eprintln!("phase5 idempotency_lifetime passed; permanent tombstones, fixed opt-in expiry, bounded GC, replay after ID replacement");
    Ok(())
}

async fn phase5_route_scale_snapshot_and_target(
    store: &PostgresStore,
    config: &PostgresStoreConfig,
    class: &WorkloadClassVersion,
) -> TestResult {
    let request = create_instance_request(
        "phase5-routing",
        "phase5-routing",
        class.reference.clone(),
        vec![],
    );
    let instance = store.create_instance(request).await?.instance;
    let mut raw = raw_client(config).await?;
    raw.execute("INSERT INTO route_bindings(route_binding_id, instance_id, identity_key, identity_kind, host_kind, host, protocol) SELECT 'bulk-'||n, $1, 'bulk-'||n, 'http', 'exact', 'bulk-'||n||'.example.net', 'http' FROM generate_series(1, 100000) n", &[&instance.id.as_str()]).await?;
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
                &format!("key-{name}"),
                name,
                instance.id.as_str(),
                identity.clone(),
                ProtocolRoute::Http,
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
            let identity = http_identity(host, path);
            let expected = rules
                .iter()
                .filter_map(|(name, rule)| {
                    control_plane::route::route_match_score(rule, &identity)
                        .map(|score| (score, *name))
                })
                .max_by_key(|(score, _)| *score);
            match store
                .resolve_route(ResolveRouteRequest::new(
                    identity,
                    MaterializationTarget::new("target", "apps")?,
                ))
                .await?
            {
                RouteResolution::Resolved { entry, .. } => assert_eq!(
                    Some(entry.route_binding_id.as_str()),
                    expected.map(|(_, name)| name)
                ),
                RouteResolution::Miss { .. } => assert!(expected.is_none()),
            }
        }
    }
    let running = wake_to_running(store, instance.id.clone()).await?;
    for (cluster, generation, uri) in [
        ("target", 1, "http://127.0.0.1:8081"),
        ("other", 99, "http://127.0.0.1:8082"),
    ] {
        let mut materialization = RecordMaterializationRequest::new(
            instance.id.clone(),
            running.generation,
            MaterializationTarget::new(cluster, "apps")?,
            MaterializationState::Ready,
            BackendGeneration::new(generation),
        );
        materialization.backend = Some(BackendEndpoint::new(uri)?);
        store.record_materialization(materialization).await?;
    }
    let lookup = ResolveRouteRequest::new(
        http_identity("app.customer.example.net", None),
        MaterializationTarget::new("target", "apps")?,
    );
    let resolution = store.resolve_route(lookup.clone()).await?;
    assert!(
        matches!(resolution, RouteResolution::Resolved { entry, .. } if entry.backend.as_ref().unwrap().uri() == "http://127.0.0.1:8081")
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
    assert!(matches!(
        store.resolve_route(lookup.clone()).await?,
        RouteResolution::Resolved { .. }
    ));
    deletion.commit().await?;
    assert!(matches!(
        store.resolve_route(lookup).await?,
        RouteResolution::Miss { .. }
    ));
    eprintln!("phase5 route_scale_snapshot_and_target passed; matcher parity over 100k routes, correct target backend, atomic delete/miss");
    Ok(())
}

#[tokio::test]
async fn postgres_migration_backfill_and_collision_rejection() -> TestResult {
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping Postgres migration backfill; SLEEPYPODS_POSTGRES_URL is unset");
        return Ok(());
    };
    let admin = raw_client(&PostgresStoreConfig::new(&base_url)?).await?;
    for conflicting in [false, true] {
        let schema = unique_schema_name();
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await?;
        let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
        let raw = raw_client(&config).await?;
        raw.batch_execute("CREATE TABLE control_plane_schema_migrations (version integer PRIMARY KEY, name text NOT NULL)").await?;
        for (version, name, sql) in [
            (
                1,
                "initial_control_plane_store",
                include_str!("../migrations/0001_initial_control_plane_store.sql"),
            ),
            (
                2,
                "workload_class_value_schema",
                include_str!("../migrations/0002_workload_class_value_schema.sql"),
            ),
            (
                3,
                "workload_class_manifest_template",
                include_str!("../migrations/0003_workload_class_manifest_template.sql"),
            ),
            (
                4,
                "workload_class_sleep_policy",
                include_str!("../migrations/0004_workload_class_sleep_policy.sql"),
            ),
            (
                5,
                "workload_class_exclusivity_keys",
                include_str!("../migrations/0005_workload_class_exclusivity_keys.sql"),
            ),
            (
                6,
                "materialization_reconciliation_leases",
                include_str!("../migrations/0006_materialization_reconciliation_leases.sql"),
            ),
        ] {
            raw.batch_execute(sql).await?;
            raw.execute(
                "INSERT INTO control_plane_schema_migrations VALUES ($1, $2)",
                &[&version, &name],
            )
            .await?;
        }
        raw.batch_execute("INSERT INTO workload_class_versions(class_id,version,template_generation,default_values,value_schema,manifest_template,sleep_policy,exclusivity_keys) VALUES ('legacy',1,1,'{}','{}','{}','{}','[]');
            INSERT INTO instances(instance_id,workload_class_id,workload_class_version,values,state,generation) VALUES ('legacy-owner','legacy',1,'{}','waking',1), ('legacy-contender','legacy',1,'{}','waking',1);
            INSERT INTO materializations(materialization_id,instance_id,instance_generation,cluster_id,namespace,state,backend_generation,rendered_objects,exclusivity_keys,updated_at_unix_millis) VALUES ('legacy-mat','legacy-owner',1,'cluster','one','deleting',1,'[{\"api_version\":\"v1\",\"kind\":\"PersistentVolume\",\"namespace\":\"one\",\"name\":\"disk\"}]','[{\"name\":\"disk\",\"value\":\"one\"},{\"name\":\"disk\",\"value\":\"one\"}]',(extract(epoch from clock_timestamp())*1000)::bigint+60000);
            INSERT INTO idempotency_records(idempotency_key,operation,request_fingerprint,resource_id) VALUES ('legacy-deleted','create_instance','{}','absent');").await?;
        raw.batch_execute("INSERT INTO instances(instance_id,workload_class_id,workload_class_version,values,state,generation) VALUES ('legacy-stale-pending','legacy',1,'{}','waking',3);
            INSERT INTO materializations(materialization_id,instance_id,instance_generation,cluster_id,namespace,state,backend_generation,rendered_objects,exclusivity_keys) VALUES ('legacy-stale-mat','legacy-stale-pending',1,'cluster','one','pending',1,'[]','[]')").await?;
        if conflicting {
            raw.batch_execute("INSERT INTO materializations(materialization_id,instance_id,instance_generation,cluster_id,namespace,state,backend_generation,rendered_objects,exclusivity_keys) VALUES ('conflicting-mat','legacy-contender',1,'cluster','two','pending',1,'[{\"api_version\":\"v1\",\"kind\":\"PersistentVolume\",\"namespace\":\"two\",\"name\":\"disk\"}]','[]')").await?;
            assert!(
                PostgresStore::connect(&config).await.is_err(),
                "backfill must reject duplicate PV ownership across namespaces"
            );
            let versions: i64 = raw
                .query_one("SELECT count(*) FROM control_plane_schema_migrations", &[])
                .await?
                .get(0);
            assert_eq!(versions, 6, "failed migration must not record completion");
            let table: Option<String> = raw
                .query_one(
                    "SELECT to_regclass('materialization_object_reservations')::text",
                    &[],
                )
                .await?
                .get(0);
            assert!(table.is_none(), "failed migration must roll back its DDL");
        } else {
            let store = PostgresStore::connect(&config).await?;
            let stale = raw.query_one("SELECT i.state, i.generation, m.state AS materialization_state, m.projection_generation FROM instances i JOIN materializations m USING(instance_id) WHERE i.instance_id = 'legacy-stale-pending'", &[]).await?;
            assert_eq!(stale.get::<_, String>("state"), "failed");
            assert_eq!(stale.get::<_, i64>("generation"), 4);
            assert_eq!(stale.get::<_, String>("materialization_state"), "deleting");
            assert_eq!(stale.get::<_, i64>("projection_generation"), 2);
            // The raw migration fixture uses skeletal class JSON. Attach the
            // ordinary typed class fixture before testing a real fresh wake.
            let recover_class = workload_class("legacy-recover", 1);
            store
                .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
                    recover_class.clone(),
                ))
                .await?;
            raw.execute("UPDATE instances SET workload_class_id = 'legacy-recover', values = '{\"tenant\":\"legacy-recovered\",\"image\":\"example/app:1\"}' WHERE instance_id = 'legacy-stale-pending'", &[]).await?;
            let recovered_store = std::sync::Arc::new(store.clone());
            let recover_target = MaterializationTarget::new("cluster", "one")?;
            let recover_materializer = control_plane::materializer::KubernetesMaterializer::new(
                LifecycleKubernetes::default(),
            );
            let driver = control_plane::MaterializationReconciler::new(
                recovered_store.clone(),
                recover_materializer.clone(),
                recover_target.clone(),
                Default::default(),
                sleepypods_observability::recorder::ObservabilityRecorder::noop(),
            );
            driver.run_once().await;
            assert!(store
                .load_active_materialization(LoadActiveMaterializationRequest::new(
                    InstanceId::new("legacy-stale-pending")?,
                    recover_target.clone()
                ))
                .await?
                .is_none());
            use control_plane::api::pb::proxy_control_plane_server::ProxyControlPlane;
            let recover_api = control_plane::api::StoreBackedProxyApi::new(
                recovered_store,
                recover_materializer,
                recover_target,
            );
            recover_api
                .wake_instance(tonic::Request::new(
                    control_plane::api::pb::ProxyWakeInstanceRequest {
                        instance_id: "legacy-stale-pending".to_owned(),
                        expected_generation: 4,
                        backend_generation: None,
                    },
                ))
                .await?;
            driver.run_once().await;
            assert_eq!(
                store
                    .get_instance(GetInstanceRequest::new(InstanceId::new(
                        "legacy-stale-pending"
                    )?))
                    .await?
                    .unwrap()
                    .state,
                InstanceState::Running
            );
            let legacy_orphan = raw.query_one("SELECT state, generation FROM instances WHERE instance_id = 'legacy-contender'", &[]).await?;
            assert_eq!(
                legacy_orphan.get::<_, String>("state"),
                "failed",
                "old unaccepted Waking gap becomes an explicit retryable terminal result"
            );
            assert_eq!(legacy_orphan.get::<_, i64>("generation"), 2);
            let legacy_projection: i64 = raw.query_one("SELECT projection_generation FROM materializations WHERE materialization_id = 'legacy-mat'", &[]).await?.get(0);
            assert_eq!(
                legacy_projection, 1,
                "legacy Deleting ownership stamp remains unchanged"
            );
            let retired = raw.execute("INSERT INTO instances(instance_id, workload_class_id, workload_class_version, values, state, generation) VALUES ('absent', 'legacy', 1, '{}', 'cold', 0)", &[]).await.expect_err("known legacy deleted ID is retired rather than assigned a guessable zero revision");
            assert_eq!(
                retired.as_db_error().unwrap().message(),
                "instance_id_retired"
            );
            let row = raw
                .query_one(
                    "SELECT namespace FROM materialization_object_reservations WHERE materialization_id = 'legacy-mat'",
                    &[],
                )
                .await?;
            assert_eq!(
                row.get::<_, String>(0),
                "",
                "PV reservations are cluster scoped"
            );
            assert_eq!(
                raw.query_one("SELECT count(*) FROM materialization_key_reservations", &[])
                    .await?
                    .get::<_, i64>(0),
                1,
                "duplicate refs within one materialization normalize"
            );
            assert!(raw.query_one("SELECT resource_deleted_at_unix_millis IS NOT NULL AND expires_at_unix_millis IS NULL FROM idempotency_records", &[]).await?.get::<_, bool>(0));
            let row = raw.query_one("SELECT drain_not_before_unix_millis > state_entered_at_unix_millis, next_attempt_at_unix_millis = drain_not_before_unix_millis FROM materializations WHERE materialization_id = 'legacy-mat'", &[]).await?;
            assert!(row.get::<_, bool>(0));
            assert!(row.get::<_, bool>(1));
            assert!(store
                .claim_materialization_reconciliation(
                    ClaimMaterializationReconciliationRequest::new(
                        control_plane::MaterializationId::new("legacy-mat")?,
                        "early",
                        Duration::from_secs(30),
                    )
                )
                .await?
                .is_none());
            raw.execute(
                "UPDATE control_plane_schema_migrations SET name = 'wrong-name' WHERE version = 7",
                &[],
            )
            .await?;
            assert!(
                store.run_migrations().await.is_err(),
                "migration metadata mismatch fails closed"
            );
        }
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await?;
    }
    Ok(())
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
                lifecycle_object_key(&control_plane::materializer::rendered_object_ref(object)),
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
    fn wait_for_pvc_bound<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> control_plane::materializer::KubernetesClientFuture<
        'a,
        control_plane::materializer::KubernetesClientResult<()>,
    > {
        Box::pin(async { Ok(()) })
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
    let store =
        Arc::new(control_plane::RetryingControlPlaneStore::with_default_policy(Arc::new(store)));
    let target = MaterializationTarget::new("cluster-a", "apps")?;
    let client = LifecycleKubernetes::default();
    let materializer = KubernetesMaterializer::new(client.clone());
    let api = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone());
    let create = |id: &str| {
        create_instance_request(
            &format!("create-{id}"),
            id,
            class.reference.clone(),
            Vec::new(),
        )
    };
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
            control_plane::MaterializationReconcilerConfig::default(),
            observations.recorder(),
        )
    };
    fresh_driver(target.clone()).run_once().await;
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

    let drain = store
        .begin_sleep(
            BeginSleepRequest::new(running.id.clone(), running.generation, target.clone())
                .with_drain_grace_timeout(Duration::from_secs(1)),
        )
        .await?;
    let api = StoreBackedProxyApi::new(store.clone(), materializer.clone(), target.clone());
    api.wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
        backend_generation: Some(1),
        instance_id: running.id.as_str().to_owned(),
        expected_generation: drain.instance.generation.get(),
    }))
    .await?;
    drop(api);
    let deletes_before = client.deletes.load(Ordering::SeqCst);
    fresh_driver(target.clone()).run_once().await;
    fresh_driver(target.clone())
        .reconcile_materialization(drain.materialization.unwrap())
        .await;
    assert_eq!(
        client.deletes.load(Ordering::SeqCst),
        deletes_before,
        "neither normal nor admin claim may skip persisted grace"
    );
    assert_eq!(
        store
            .get_instance(GetInstanceRequest::new(cold.id.clone()))
            .await?
            .unwrap()
            .state,
        InstanceState::Draining
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fresh_driver(target.clone()).run_once().await;
    assert_eq!(
        store
            .get_instance(GetInstanceRequest::new(cold.id.clone()))
            .await?
            .unwrap()
            .state,
        InstanceState::Waking,
        "deferred wake is promoted atomically with completed sleep: {:?}",
        observations.events()
    );
    fresh_driver(target.clone()).run_once().await;
    let rewoken = store
        .get_instance(GetInstanceRequest::new(cold.id.clone()))
        .await?
        .unwrap();
    assert_eq!(rewoken.state, InstanceState::Running);
    assert!(rewoken.generation > running.generation);
    assert_eq!(
        store
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                cold.id.clone(),
                target.clone()
            ))
            .await?
            .unwrap()
            .backend_generation,
        BackendGeneration::new(45),
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
                fresh_driver(target.clone()).run_once().await;
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
            assert!(
                store
                    .delete_instance(DeleteInstanceRequest::new(instance.id.clone()))
                    .await
                    .is_err(),
                "hard delete cannot release unresolved cleanup"
            );
        }
        fresh_driver(target.clone()).run_once().await;
        assert!(store
            .get_instance(GetInstanceRequest::new(instance.id.clone()))
            .await?
            .is_none());
        let replacement = store
            .create_instance(create_instance_request(
                &format!("replacement-{id}"),
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
    fresh_driver(target.clone()).run_once().await;
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
    cancel_api
        .wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
            instance_id: cancel.id.as_str().to_owned(),
            expected_generation: cancel_drain.instance.generation.get(),
            backend_generation: None,
        }))
        .await?;
    drop(cancel_api);
    store
        .request_instance_deletion(RequestInstanceDeletion {
            instance_id: cancel.id.clone(),
            expected_generation: cancel_drain.instance.generation,
        })
        .await?;
    fresh_driver(target.clone()).run_once().await;
    assert_eq!(
        store
            .get_instance(GetInstanceRequest::new(cancel.id.clone()))
            .await?
            .unwrap()
            .state,
        InstanceState::Deleting
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fresh_driver(target.clone()).run_once().await;
    assert!(
        store
            .get_instance(GetInstanceRequest::new(cancel.id))
            .await?
            .is_none(),
        "delete atomically cancels deferred wake and retains grace"
    );

    // All targets become deleting, but a driver may prove cleanup only for its own target.
    let other_target = MaterializationTarget::new("cluster-b", "apps")?;
    let second = RecordMaterializationRequest::new(
        rewoken.id.clone(),
        rewoken.generation,
        other_target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(rewoken.generation.get()),
    );
    store.record_materialization(second).await?;
    store
        .request_instance_deletion(RequestInstanceDeletion {
            instance_id: rewoken.id.clone(),
            expected_generation: rewoken.generation,
        })
        .await?;
    fresh_driver(target.clone()).run_once().await;
    assert!(
        store
            .get_instance(GetInstanceRequest::new(rewoken.id.clone()))
            .await?
            .is_some(),
        "local absence cannot finalize another cluster"
    );
    let other = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            rewoken.id.clone(),
            other_target.clone(),
        ))
        .await?
        .unwrap();
    assert_eq!(other.state, MaterializationState::Deleting);
    fresh_driver(target).reconcile_materialization(other).await;
    assert!(store
        .get_instance(GetInstanceRequest::new(rewoken.id.clone()))
        .await?
        .is_some());
    fresh_driver(other_target).run_once().await;
    assert!(store
        .get_instance(GetInstanceRequest::new(rewoken.id))
        .await?
        .is_none());
    eprintln!("phase6a durable acceptance, fresh-driver recovery, stable projection, grace, target-scoped cleanup, and ID-reuse fencing passed");
    Ok(())
}

#[tokio::test]
async fn postgres_effect_barriers_and_same_owner_attempt_fencing() -> TestResult {
    use control_plane::materialization::{
        AcknowledgeMaterializationEffectRequest, MaterializationEffectRequest,
    };
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
    let store = Arc::new(
        control_plane::RetryingControlPlaneStore::with_default_policy(Arc::new(raw_store)),
    );
    let (mut raw, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
    let raw_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result: TestResult = async {
        let class = workload_class("effect-class", 1);
        store.create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone())).await?;
        let cold = store.create_instance(create_instance_request("effect-create", "effect-instance", class.reference, vec![])).await?.instance;
        let waking = store.compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(cold.id.clone(), cold.generation, InstanceState::Waking, StateTransitionReason::WakeRequested)).await?;
        let target = MaterializationTarget::new("effects-cluster", "apps")?;
        let mut request = RecordMaterializationRequest::new(waking.id, waking.generation, target.clone(), MaterializationState::Pending, BackendGeneration::new(1));
        request.rendered_objects = vec![RenderedObjectRef { api_version: "v1".into(), kind: "Service".into(), namespace: "apps".into(), name: "effect-service".into() }];
        request.exclusivity_keys = vec![RenderedExclusivityKey::new("singleton", "effects")];
        let pending = store.record_materialization(request).await?;
        let claim = |owner: &str| ClaimMaterializationReconciliationRequest::new(pending.id.clone(), owner,
            Duration::from_secs(30),
        );
        let first = store.claim_materialization_reconciliation(claim("same-owner")).await?.unwrap();
        let first_attempt = first.reconciliation_lease.as_ref().unwrap().attempt;
        raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&pending.id.as_str()]).await?;
        let second = store.claim_materialization_reconciliation(claim("same-owner")).await?.unwrap();
        let second_attempt = second.reconciliation_lease.as_ref().unwrap().attempt;
        assert!(second_attempt > first_attempt);
        assert!(!store.renew_materialization_reconciliation_lease(RenewMaterializationReconciliationLeaseRequest::new(
pending.id.clone(),
"same-owner",
first_attempt,
pending.instance_generation, Duration::from_secs(30), MaterializationState::Pending)).await?);
        assert!(!store.release_materialization_reconciliation_lease(ReleaseMaterializationReconciliationLeaseRequest::new(
pending.id.clone(),
"same-owner",
first_attempt,
pending.instance_generation,
)).await?);
        assert!(matches!(store.complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(pending.id.clone(), "same-owner", first_attempt, complete_for_reconciled_pending(&pending, "http://ready:80"))).await, Err(StoreError::LeaseConflict { .. })));
        assert!(matches!(store.delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(pending.id.clone(), "same-owner", first_attempt, pending.state, pending.instance_id.clone(), pending.instance_generation, target)).await, Err(StoreError::LeaseConflict { .. })));
        let effect = |effect_id| MaterializationEffectRequest { effect_id, materialization_id: pending.id.clone(), owner: "same-owner".into(), attempt: second_attempt, instance_generation: pending.instance_generation, expected_state: pending.state, operation: "apply", object: pending.rendered_objects[0].clone(), precondition: None };
        let ack = |effect_id| AcknowledgeMaterializationEffectRequest { instance_generation: pending.instance_generation, effect_id, materialization_id: pending.id.clone(), owner: "same-owner".into(), attempt: second_attempt };
        let mut stale = effect(1); stale.attempt = first_attempt;
        assert!(!store.begin_materialization_effect(stale).await?);
        assert!(store.begin_materialization_effect(effect(1)).await?, "production retry wrapper forwards begin");
        assert!(store.begin_materialization_effect(effect(1)).await?, "exact begin replay is idempotent");
        let mut different = effect(1); different.object.name = "other".into();
        assert!(!store.begin_materialization_effect(different).await?, "same token with different operation is refused");
        assert!(store.acknowledge_materialization_effect(ack(1)).await?);
        assert!(store.begin_materialization_effect(effect(2)).await?);
        assert!(!store.acknowledge_materialization_effect(ack(1)).await?, "late ACK A cannot clear B");
        assert_eq!(raw.query_one("SELECT effect_id FROM materialization_effects WHERE materialization_id = $1", &[&pending.id.as_str()]).await?.get::<_,i64>(0), 2);
        assert!(!store.release_materialization_reconciliation_lease(ReleaseMaterializationReconciliationLeaseRequest::new(
pending.id.clone(),
"same-owner",
second_attempt,
pending.instance_generation,
)).await?);
        assert!(matches!(store.complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(pending.id.clone(), "same-owner", second_attempt, complete_for_reconciled_pending(&pending, "http://ready:80"))).await, Err(StoreError::LeaseConflict { .. })));
        raw.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&pending.id.as_str()]).await?;
        assert!(store.claim_materialization_reconciliation(claim("new-owner")).await?.is_none(), "unresolved create blocks transfer even if Kubernetes name is absent");
        assert!(store.list_materialization_reconciliation_candidates(ListMaterializationReconciliationCandidatesRequest::new(32)).await?.is_empty());
        // The old API response finally arrives: only exact acknowledgement removes
        // uncertainty. A fresh driver may then inspect/delete the late created UID.
        assert!(store.acknowledge_materialization_effect(ack(2)).await?);

        // Reproduce an uncommitted begin followed by a lease-expiry claim. Claim's
        // pre-lock snapshot sees no effect; its post-lock fresh statement must see it.
        let third = store.claim_materialization_reconciliation(claim("same-owner")).await?.unwrap();
        let third_attempt = third.reconciliation_lease.as_ref().unwrap().attempt as i64;
        let transaction = raw.transaction().await?;
        transaction.query_one("SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE", &[&pending.id.as_str()]).await?;
        transaction.execute("INSERT INTO materialization_effects(materialization_id,effect_id,lease_owner,lease_attempt,instance_generation,operation,object_ref) VALUES ($1,3,'same-owner',$2,$3,'apply','{}')", &[&pending.id.as_str(), &third_attempt, &(pending.instance_generation.get() as i64)]).await?;
        transaction.execute("UPDATE materializations SET reconcile_lease_expires_at_unix_millis = 1 WHERE materialization_id = $1", &[&pending.id.as_str()]).await?;
        let other_store = store.clone(); let takeover_request = claim("new-owner");
        let takeover = tokio::spawn(async move { other_store.claim_materialization_reconciliation(takeover_request).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%SELECT materialization_id FROM materializations%' AND pid <> pg_backend_pid())", &[]).await.unwrap().get(0);
                if waiting { break; }
                tokio::task::yield_now().await;
            }
        }).await?;
        transaction.commit().await?;
        assert!(takeover.await??.is_none(), "claim must recheck barrier after its row lock wait");
        let still_held = store.load_materialization(LoadMaterializationRequest::new(pending.id.clone())).await?.unwrap();
        assert_eq!(still_held.exclusivity_keys, pending.exclusivity_keys);
        // Known-not-dispatched ACK waits for a still-uncommitted begin. Its
        // fresh snapshot must clear the eventual commit rather than miss it.
        let mut third_ack = ack(3); third_ack.attempt = third_attempt as u64;
        assert!(store.acknowledge_materialization_effect(third_ack.clone()).await?);
        let fourth = store.claim_materialization_reconciliation(claim("same-owner")).await?.unwrap();
        let fourth_attempt = fourth.reconciliation_lease.as_ref().unwrap().attempt as i64;
        let transaction = raw.transaction().await?;
        transaction.query_one("SELECT materialization_id FROM materializations WHERE materialization_id = $1 FOR UPDATE", &[&pending.id.as_str()]).await?;
        transaction.execute("INSERT INTO materialization_effects(materialization_id,effect_id,lease_owner,lease_attempt,instance_generation,operation,object_ref) VALUES ($1,4,'same-owner',$2,$3,'apply','{}')", &[&pending.id.as_str(), &fourth_attempt, &(pending.instance_generation.get() as i64)]).await?;
        let mut fourth_ack = ack(4); fourth_ack.attempt = fourth_attempt as u64;
        let ack_store = store.clone(); let acknowledgement = tokio::spawn(async move { ack_store.acknowledge_materialization_effect(fourth_ack).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%SELECT materialization_id FROM materializations%' AND pid <> pg_backend_pid())", &[]).await.unwrap().get(0);
                if waiting { break; }
                tokio::task::yield_now().await;
            }
        }).await?;
        transaction.commit().await?;
        assert!(acknowledgement.await??, "ACK sees the begin commit after its row lock wait");
        assert!(raw.query_opt("SELECT 1 FROM materialization_effects WHERE materialization_id = $1", &[&pending.id.as_str()]).await?.is_none());
        // Leave another deliberately unresolved operation for force recovery.
        let mut unresolved = effect(5); unresolved.attempt = fourth_attempt as u64;
        assert!(store.begin_materialization_effect(unresolved).await?);
        // Existing force API is the explicitly audited recovery boundary after
        // operators have fenced the old process/request and cleaned the cluster.
        store.force_delete_materialization(control_plane::ForceDeleteMaterializationRequest::new(pending.id.clone(), "test-operator", "old driver and API request fenced; actual cluster cleanup independently confirmed")).await?;
        assert!(raw.query_opt("SELECT 1 FROM materialization_effects WHERE materialization_id = $1", &[&pending.id.as_str()]).await?.is_none());
        store.request_instance_deletion(control_plane::instance::RequestInstanceDeletion { instance_id: pending.instance_id.clone(), expected_generation: pending.instance_generation }).await?;
        store.finalize_instance_deletions(10).await?;
        assert!(store.get_instance(GetInstanceRequest::new(pending.instance_id.clone())).await?.is_none());
        let recreated = store.create_instance(create_instance_request("effect-recreate", pending.instance_id.as_str(), workload_class("effect-class",1).reference, vec![])).await?.instance;
        let recreated_waking = store.compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(recreated.id, recreated.generation, InstanceState::Waking, StateTransitionReason::WakeRequested)).await?;
        let mut new_request = RecordMaterializationRequest::new(recreated_waking.id, recreated_waking.generation, pending.target.clone(), MaterializationState::Pending, BackendGeneration::new(1));
        new_request.rendered_objects = pending.rendered_objects.clone(); new_request.exclusivity_keys = pending.exclusivity_keys.clone();
        let recreated_pending = store.record_materialization(new_request).await?;
        assert_eq!(recreated_pending.id, pending.id, "materialization names are deterministic across ID reuse");
        assert!(recreated_pending.instance_generation > pending.instance_generation);
        let reused_claim = store.claim_materialization_reconciliation(claim("same-owner")).await?.unwrap();
        assert_eq!(reused_claim.reconciliation_lease.as_ref().unwrap().attempt, first_attempt, "attempt counter may restart after row deletion");
        assert!(!store.renew_materialization_reconciliation_lease(RenewMaterializationReconciliationLeaseRequest::new(pending.id.clone(), "same-owner", first_attempt, pending.instance_generation, Duration::from_secs(30), MaterializationState::Pending)).await?);
        assert!(!store.release_materialization_reconciliation_lease(ReleaseMaterializationReconciliationLeaseRequest::new(pending.id.clone(), "same-owner", first_attempt, pending.instance_generation)).await?);
        let new_effect = MaterializationEffectRequest { instance_generation: recreated_pending.instance_generation, attempt: first_attempt, ..effect(1) };
        assert!(store.begin_materialization_effect(new_effect).await?);
        let old_ack = AcknowledgeMaterializationEffectRequest { attempt: first_attempt, ..ack(1) };
        assert!(!store.acknowledge_materialization_effect(old_ack).await?, "old incarnation ACK cannot erase same numeric effect token after ID reuse");
        assert!(raw.query_opt("SELECT 1 FROM materialization_effects WHERE materialization_id = $1", &[&pending.id.as_str()]).await?.is_some());
        Ok(())
    }.await;
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

#[path = "postgres_store/activation_idle.rs"]
mod activation_idle;

#[path = "postgres_store/cleanup_boundary.rs"]
mod cleanup_boundary;

#[path = "postgres_store/certificates.rs"]
mod certificates;

#[path = "postgres_store/crash_boundaries.rs"]
mod crash_boundaries;
