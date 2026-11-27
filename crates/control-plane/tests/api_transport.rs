#[macro_use]
#[path = "support/unexpected_store.rs"]
mod unexpected_store;
#[path = "api_transport/create_validation.rs"]
mod create_validation;
#[path = "api_transport/grpc_web_tls.rs"]
mod grpc_web_tls;
#[path = "api_transport/quiet_logging.rs"]
mod quiet_logging;
mod support;
use control_plane::api::pb::proxy_control_plane_server::ProxyControlPlane;
use support::TestStore as FakeInstanceStore;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{BufMut, Bytes, BytesMut};
use control_plane::api::{
    operator_grpc_service_with_store_and_route_events, operator_grpc_web_cors_layer,
    pb::{
        operator_control_plane_server::{OperatorControlPlane, OperatorControlPlaneServer},
        persistent_volume_source_template, route_identity, template_text_part,
        ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest, CreateRouteBindingRequest,
        CreateWorkloadClassVersionRequest, CsiSecretRefTemplate, CsiVolumeSourceTemplate,
        DeleteHttp01ChallengeRequest, DeleteHttp01ChallengeResponse, DeleteInstanceRequest,
        DeleteInstanceResponse, DeleteRouteBindingRequest, DeleteRouteBindingResponse,
        EnvVarTemplate, ForceDeleteMaterializationRequest, ForceReleaseExclusivityKeyRequest,
        GetInstanceRequest, GetRouteBindingRequest, GetWorkloadClassVersionRequest,
        HostPathVolumeSourceTemplate, Http01Challenge, Http01ChallengeKey, HttpRouteIdentity,
        IdleTimeoutOverridePolicy, Instance, InstanceRoute, InstanceState, ManifestTemplate,
        PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate,
        ProtocolRoute, PutHttp01ChallengeRequest, RawKubernetesManifestTemplate,
        ReconcileMaterializationRequest, ReconcileMaterializationResponse,
        ResolveHttp01ChallengeRequest, ResolveHttp01ChallengeResponse, RouteBinding, RouteHost,
        RouteHostKind, RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
        SniRouteIdentity, TemplateText, TemplateTextPart, VolumeTemplate, WorkloadClassVersion,
        WorkloadClassVersionRef, WorkloadExclusivityKey, WorkloadKind, WorkloadSleepPolicy,
        WorkloadTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
    },
    RouteSubscriptionBroker, StoreBackedOperatorApi, OPERATOR_SERVICE_NAME,
};
use control_plane::projection::{
    LiveObjectMetadata, ProjectionObjectInspection, ProjectionReadinessInspection,
    ANNOTATION_MATERIALIZATION_ID, LABEL_MANAGED_BY, LABEL_MANAGED_BY_VALUE,
};
use control_plane::{
    AuthConfig, BackendEndpoint, BackendGeneration, CallerRole, ControlPlaneAuth,
    ControlPlaneStore, Generation, InstanceId, InstanceRecord,
    InstanceState as DomainInstanceState, KubernetesClientError, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient,
    MaterializationId, MaterializationRecord, MaterializationState, MaterializationTarget,
    RenderedExclusivityKey, RenderedObjectRef, StaticBearerTokens,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{header, HeaderMap, HeaderName, HeaderValue, Method, Request, Version};
use tonic::server::NamedService;
use tonic::{Code, Response, Status};
use tower::{Layer, ServiceExt};

fn store_operator_api(
    store: Arc<dyn ControlPlaneStore>,
) -> StoreBackedOperatorApi<FakeKubernetesClient> {
    StoreBackedOperatorApi::new(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
    )
}

fn store_proxy_api(
    store: Arc<dyn ControlPlaneStore>,
) -> control_plane::api::StoreBackedProxyApi<FakeKubernetesClient> {
    control_plane::api::StoreBackedProxyApi::new(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
    )
}

fn store_operator_grpc_service(
    store: Arc<dyn ControlPlaneStore>,
) -> control_plane::api::server::StoreBackedOperatorGrpcService<FakeKubernetesClient> {
    operator_grpc_service_with_store_and_route_events(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
        RouteSubscriptionBroker::new(),
    )
}

/// The origin `grpc_web_operator_request` sends.
fn browser_origins() -> [HeaderValue; 1] {
    [HeaderValue::from_static("https://operator.example")]
}

fn auth_config() -> AuthConfig {
    AuthConfig::static_bearer_tokens(
        StaticBearerTokens::new("operator-token", "proxy-token", "sidecar-token")
            .expect("auth tokens are valid"),
    )
}

fn control_plane_auth() -> ControlPlaneAuth {
    ControlPlaneAuth::from_config(auth_config(), Default::default())
}

fn authenticated_operator_service(
    store: Arc<dyn ControlPlaneStore>,
) -> tonic::service::interceptor::InterceptedService<
    control_plane::api::server::StoreBackedOperatorGrpcService<FakeKubernetesClient>,
    control_plane::ControlPlaneAuthInterceptor,
> {
    tonic::service::interceptor::InterceptedService::new(
        store_operator_grpc_service(store),
        control_plane_auth().interceptor(OPERATOR_SERVICE_NAME, CallerRole::Operator),
    )
}

fn operator_materializer(
    client: FakeKubernetesClient,
) -> KubernetesMaterializer<FakeKubernetesClient> {
    KubernetesMaterializer::new(client)
}

fn target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("target is valid")
}

/// Runs the reconciler until every wake, cleanup and instance deletion it can
/// start has completed or recorded a failed attempt.
async fn reconcile_operator_work(store: Arc<FakeInstanceStore>, client: FakeKubernetesClient) {
    store.retry_failed_attempts();
    let observed = store.clone();
    support::reconcile_until(
        control_plane::MaterializationReconciler::new(
            store,
            operator_materializer(client),
            target(),
            support::reconciler_config(),
            sleepypods_observability::recorder::ObservabilityRecorder::noop(),
        ),
        || {
            let settled = observed.lifecycle_settled(&target());
            async move { settled }
        },
    )
    .await;
}

fn domain_instance(id: &str, state: DomainInstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id).expect("instance id is valid"),
        workload_class: control_plane::WorkloadClassVersionRef::new(
            control_plane::WorkloadClassId::new("class-1").expect("class id is valid"),
            Generation::new(1),
        ),
        values: Default::default(),
        state,
        generation: Generation::new(generation),
    }
}

fn ready_materialization(instance_id: &str, generation: u64) -> MaterializationRecord {
    materialization(
        instance_id,
        generation,
        target(),
        MaterializationState::Ready,
    )
}

fn materialization(
    instance_id: &str,
    generation: u64,
    target: MaterializationTarget,
    state: MaterializationState,
) -> MaterializationRecord {
    MaterializationRecord {
        pod_backend: None,
        id: MaterializationId::new(format!(
            "{}:{}:{}",
            instance_id,
            target.cluster_id(),
            target.namespace()
        ))
        .expect("materialization id is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance id is valid"),
        instance_generation: Generation::new(generation),
        projection_generation: Generation::new(generation),
        target,
        state,
        backend: Some(BackendEndpoint::new("http://example").expect("backend is valid")),
        backend_generation: BackendGeneration::new(generation),
        rendered_objects: vec![
            object_ref("v1", "Service", "apps", &format!("{instance_id}-svc")),
            object_ref("apps/v1", "Deployment", "apps", instance_id),
        ],
        exclusivity_keys: vec![],
        reconciliation_lease: None,
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

fn object_key(object: &RenderedObjectRef) -> String {
    format!(
        "{}|{}|{}|{}",
        object.api_version, object.kind, object.namespace, object.name
    )
}

fn owned_metadata(materialization: &MaterializationRecord) -> LiveObjectMetadata {
    LiveObjectMetadata {
        persistent_volume_reclaim_policy: Some("Retain".into()),
        identity: control_plane::projection::LiveObjectIdentity {
            uid: "test-uid".into(),
            resource_version: "1".into(),
        },
        labels: BTreeMap::from([
            (
                LABEL_MANAGED_BY.to_owned(),
                LABEL_MANAGED_BY_VALUE.to_owned(),
            ),
            (
                "sleepypods.io/instance-id".to_owned(),
                materialization.instance_id.as_str().to_owned(),
            ),
            (
                "sleepypods.io/instance-generation".to_owned(),
                materialization.instance_generation.to_string(),
            ),
        ]),
        annotations: BTreeMap::from([(
            ANNOTATION_MATERIALIZATION_ID.to_owned(),
            materialization.id.as_str().to_owned(),
        )]),
        deleting: false,
        finalizers: Vec::new(),
    }
}

#[test]
fn generated_api_contains_expected_v1_resource_shape() {
    let request = CreateInstanceRequest {
        instance_id: "instance-1".to_owned(),
        workload_class: None,
        values: [("tenant".to_owned(), "acme".to_owned())].into(),
        routes: Vec::new(),
    };
    let route_binding = CreateRouteBindingRequest {
        route_binding_id: "route-1".to_owned(),
        instance_id: request.instance_id.clone(),
        identity: None,
        protocol: ProtocolRoute::Http as i32,
    };
    let workload_class = CreateWorkloadClassVersionRequest {
        class_id: "class-1".to_owned(),
        version: 1,
        value_schema: Some(stateful_value_schema()),
        template: Some(stateful_manifest_template_proto()),
        sleep_policy: Some(sleep_policy_proto()),
        exclusivity_keys: vec![],
    };

    assert_eq!(request.values["tenant"], "acme");
    assert_eq!(route_binding.instance_id, "instance-1");
    assert!(
        workload_class
            .value_schema
            .as_ref()
            .expect("value schema is present")
            .fields["tenant"]
            .required
    );
    let template = workload_class
        .template
        .as_ref()
        .expect("template is present");
    assert_eq!(
        template
            .workload
            .as_ref()
            .expect("workload template is present")
            .kind,
        WorkloadKind::StatefulSet as i32
    );
    assert_eq!(
        template.volumes[0]
            .access_modes
            .first()
            .copied()
            .expect("access mode is generated"),
        PersistentVolumeAccessMode::ReadWriteOnce as i32
    );
    assert_eq!(InstanceState::Cold as i32, 1);
    assert_eq!(ProtocolRoute::Http as i32, 1);
    assert_eq!(RouteHostKind::WildcardSuffix as i32, 2);
    assert_eq!(
        workload_class
            .sleep_policy
            .as_ref()
            .expect("sleep policy is present")
            .idle_timeout_ms,
        300_000
    );
}

#[tokio::test]
async fn store_backed_instance_methods_create_get_and_delete_instances() {
    let store = store_with_class("class-1", 7);
    let service = store_operator_api(store.clone());

    let created = service
        .create_instance(tonic::Request::new(CreateInstanceRequest {
            instance_id: "instance-1".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-1".to_owned(),
                version: 7,
            }),
            values: [("tenant".to_owned(), "acme".to_owned())].into(),
            routes: Vec::new(),
        }))
        .await
        .expect("create instance succeeds")
        .into_inner();

    assert_eq!(created.instance_id, "instance-1");
    assert_eq!(created.generation, 0);
    assert_eq!(created.state, InstanceState::Cold as i32);
    assert_eq!(
        created
            .workload_class
            .as_ref()
            .expect("workload class is returned")
            .version,
        7
    );

    let loaded = service
        .get_instance(tonic::Request::new(GetInstanceRequest {
            instance_id: "instance-1".to_owned(),
        }))
        .await
        .expect("get instance succeeds")
        .into_inner();
    assert_eq!(loaded, created);

    let deleted = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-1".to_owned(),
            expected_generation: Some(0),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();
    assert!(deleted.accepted);
    assert!(store.instance_exists("instance-1"));
    let deleting = service
        .get_instance(tonic::Request::new(GetInstanceRequest {
            instance_id: "instance-1".to_owned(),
        }))
        .await
        .expect("accepted deletion remains queryable")
        .into_inner();
    assert_eq!(deleting.state, InstanceState::Deleting as i32);
    reconcile_operator_work(store, FakeKubernetesClient::default()).await;

    let missing = service
        .get_instance(tonic::Request::new(GetInstanceRequest {
            instance_id: "instance-1".to_owned(),
        }))
        .await
        .expect_err("deleted instance no longer loads");
    assert_eq!(missing.code(), Code::NotFound);
}

#[tokio::test]
async fn create_instance_creates_its_routes_with_the_instance() {
    let store = store_with_class("class-1", 7);
    let service = store_operator_api(store.clone());
    let request = |instance_id: &str, routes: Vec<InstanceRoute>| {
        tonic::Request::new(CreateInstanceRequest {
            instance_id: instance_id.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-1".to_owned(),
                version: 7,
            }),
            values: [("tenant".to_owned(), "acme".to_owned())].into(),
            routes,
        })
    };

    service
        .create_instance(request(
            "routed",
            vec![
                InstanceRoute {
                    route_binding_id: "routed-web".to_owned(),
                    identity: Some(http_identity("App.Example.com", Some("/api"))),
                },
                InstanceRoute {
                    route_binding_id: "routed-db".to_owned(),
                    identity: Some(sni_identity("db.example.com")),
                },
            ],
        ))
        .await
        .expect("create instance with routes succeeds");

    for (route_binding_id, identity) in [
        ("routed-web", http_identity("app.example.com", Some("/api"))),
        ("routed-db", sni_identity("db.example.com")),
    ] {
        let route = service
            .get_route_binding(tonic::Request::new(GetRouteBindingRequest {
                route_binding_id: route_binding_id.to_owned(),
            }))
            .await
            .expect("route created with the instance loads")
            .into_inner();
        assert_eq!(route.instance_id, "routed");
        assert_eq!(route.identity, Some(identity));
    }

    let error = service
        .create_instance(request(
            "unrouted",
            vec![InstanceRoute {
                route_binding_id: "unrouted-web".to_owned(),
                identity: None,
            }],
        ))
        .await
        .expect_err("a route without an identity is rejected");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(!store.instance_exists("unrouted"));
}

#[tokio::test]
async fn store_backed_create_instance_rejects_invalid_kubernetes_instance_id() {
    let store = Arc::new(FakeInstanceStore::default());
    let service = store_operator_api(store);

    let error = service
        .create_instance(tonic::Request::new(CreateInstanceRequest {
            instance_id: "Tenant_One".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-1".to_owned(),
                version: 7,
            }),
            values: [("tenant".to_owned(), "tenant-one".to_owned())].into(),
            routes: Vec::new(),
        }))
        .await
        .expect_err("invalid instance IDs are rejected at the API boundary");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("Kubernetes DNS label"));
}

#[tokio::test]
async fn operator_delete_requires_explicit_generation_before_accepting_intent() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "legacy-delete",
        DomainInstanceState::Cold,
        0,
    ));
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(FakeKubernetesClient::default()),
        target(),
    );
    let error = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "legacy-delete".to_owned(),
            expected_generation: None,
        }))
        .await
        .expect_err("unfenced legacy request is rejected");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(store.delete_requests().is_empty());
    assert!(store.instance_exists("legacy-delete"));
}

#[tokio::test]
async fn operator_delete_rejects_maximum_wire_generation_without_overflow() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "generation-overflow",
        DomainInstanceState::Deleting,
        12,
    ));
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(FakeKubernetesClient::default()),
        target(),
    );
    let error = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "generation-overflow".to_owned(),
            expected_generation: Some(u64::MAX),
        }))
        .await
        .expect_err("malformed public revision must never panic or wrap");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(store.delete_requests().is_empty());
}

#[tokio::test]
async fn operator_delete_cleans_active_materialization_before_store_delete() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-active",
        DomainInstanceState::Running,
        7,
    ));
    let materialization = ready_materialization("instance-delete-active", 7);
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-active".to_owned(),
            expected_generation: Some(7),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.accepted);
    assert!(
        client.deleted().is_empty(),
        "acceptance performs no Kubernetes cleanup"
    );
    reconcile_operator_work(store.clone(), client.clone()).await;
    assert!(!store.instance_exists("instance-delete-active"));
    assert!(store.materialization().is_none());
    assert_eq!(
        client.deleted(),
        vec![
            object_ref("v1", "Service", "apps", "instance-delete-active-svc"),
            object_ref("apps/v1", "Deployment", "apps", "instance-delete-active"),
        ]
    );
    assert_eq!(store.delete_requests().len(), 1);
}

#[tokio::test]
async fn operator_delete_without_active_materialization_deletes_store_only() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-cold",
        DomainInstanceState::Cold,
        0,
    ));
    let client = FakeKubernetesClient::default();
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-cold".to_owned(),
            expected_generation: Some(0),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.accepted);
    assert!(store.instance_exists("instance-delete-cold"));
    reconcile_operator_work(store.clone(), client.clone()).await;
    assert_eq!(client.deleted(), Vec::<RenderedObjectRef>::new());

    let missing = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-cold".to_owned(),
            expected_generation: Some(0),
        }))
        .await
        .expect("missing delete remains idempotent")
        .into_inner();

    assert!(!missing.accepted);
    assert_eq!(client.deleted(), Vec::<RenderedObjectRef>::new());
}

#[tokio::test]
async fn operator_delete_records_other_target_cleanup_without_touching_kubernetes() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-other-target",
        DomainInstanceState::Running,
        3,
    ));
    store.seed_materialization(materialization(
        "instance-delete-other-target",
        3,
        MaterializationTarget::new("cluster-b", "apps").expect("target is valid"),
        MaterializationState::Ready,
    ));
    let client = FakeKubernetesClient::default();
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-other-target".to_owned(),
            expected_generation: Some(3),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.accepted);
    assert!(store.instance_exists("instance-delete-other-target"));
    assert_eq!(
        store.materialization().unwrap().state,
        MaterializationState::Deleting
    );
    reconcile_operator_work(store.clone(), client.clone()).await;
    assert!(store.instance_exists("instance-delete-other-target"));
    assert_eq!(
        store.materialization().unwrap().state,
        MaterializationState::Deleting
    );
    assert_eq!(client.deleted(), Vec::<RenderedObjectRef>::new());
}

#[tokio::test]
async fn operator_delete_rejects_a_stale_generation_before_accepting_intent() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "recreated-instance",
        DomainInstanceState::Cold,
        12,
    ));
    let service = store_operator_api(store.clone());
    let error = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "recreated-instance".to_owned(),
            expected_generation: Some(0),
        }))
        .await
        .expect_err("old request cannot delete a newer incarnation");
    assert_eq!(error.code(), Code::FailedPrecondition);
    let loaded = service
        .get_instance(tonic::Request::new(GetInstanceRequest {
            instance_id: "recreated-instance".to_owned(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(loaded.state, InstanceState::Cold as i32);
    assert_eq!(loaded.generation, 12);
    assert!(store.delete_requests().is_empty());
}

#[tokio::test]
async fn operator_delete_cleans_stale_generation_active_materialization_for_target() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-stale-generation",
        DomainInstanceState::Running,
        9,
    ));
    let materialization = ready_materialization("instance-delete-stale-generation", 8);
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-stale-generation".to_owned(),
            expected_generation: Some(9),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.accepted);
    assert!(
        client.deleted().is_empty(),
        "acceptance performs no Kubernetes cleanup"
    );
    reconcile_operator_work(store.clone(), client.clone()).await;
    assert!(!store.instance_exists("instance-delete-stale-generation"));
    assert!(store.materialization().is_none());
    assert_eq!(
        client.deleted(),
        vec![
            object_ref(
                "v1",
                "Service",
                "apps",
                "instance-delete-stale-generation-svc"
            ),
            object_ref(
                "apps/v1",
                "Deployment",
                "apps",
                "instance-delete-stale-generation"
            ),
        ]
    );
}

#[tokio::test]
async fn operator_delete_kubernetes_failure_preserves_store_state_for_retry() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-fails",
        DomainInstanceState::Running,
        5,
    ));
    let materialization = ready_materialization("instance-delete-fails", 5);
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    client.fail_delete(object_ref(
        "v1",
        "Service",
        "apps",
        "instance-delete-fails-svc",
    ));
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-fails".to_owned(),
            expected_generation: Some(5),
        }))
        .await
        .expect("cleanup failure cannot reject durable acceptance")
        .into_inner();

    assert!(response.accepted);
    assert!(client.deleted().is_empty());
    reconcile_operator_work(store.clone(), client.clone()).await;
    assert!(store.instance_exists("instance-delete-fails"));
    assert_eq!(store.delete_requests(), Vec::new());
    assert_eq!(
        client.deleted(),
        vec![object_ref(
            "v1",
            "Service",
            "apps",
            "instance-delete-fails-svc"
        ),]
    );
}

#[tokio::test]
async fn operator_delete_unowned_live_ref_blocks_cleanup_without_delete() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-unowned",
        DomainInstanceState::Running,
        5,
    ));
    let materialization = ready_materialization("instance-delete-unowned", 5);
    store.seed_materialization(materialization);
    let client = FakeKubernetesClient::default();
    client.seed_unowned_object(object_ref(
        "v1",
        "Service",
        "apps",
        "instance-delete-unowned-svc",
    ));
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-unowned".to_owned(),
            expected_generation: Some(5),
        }))
        .await
        .expect("delete intent is accepted before cleanup")
        .into_inner();

    assert!(response.accepted);
    reconcile_operator_work(store.clone(), client.clone()).await;
    assert!(store.instance_exists("instance-delete-unowned"));
    assert_eq!(store.delete_requests(), Vec::new());
    assert_eq!(client.deleted(), Vec::<RenderedObjectRef>::new());
}

#[tokio::test]
async fn operator_delete_restart_replays_cleanup_then_finalizes_store_delete() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-retry",
        DomainInstanceState::Running,
        8,
    ));
    let materialization = ready_materialization("instance-delete-retry", 8);
    store.seed_materialization(materialization.clone());
    let failing_client = FakeKubernetesClient::default();
    failing_client.seed_materialization(&materialization);
    failing_client.fail_delete(object_ref(
        "v1",
        "Service",
        "apps",
        "instance-delete-retry-svc",
    ));
    let failing_service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(failing_client.clone()),
        target(),
    );
    failing_service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-retry".to_owned(),
            expected_generation: Some(8),
        }))
        .await
        .expect("delete intent is accepted");
    reconcile_operator_work(store.clone(), failing_client).await;
    assert!(store.instance_exists("instance-delete-retry"));

    let retry_client = FakeKubernetesClient::default();
    retry_client.seed_materialization(&materialization);
    // A fresh driver resumes durable work without another operator RPC.
    reconcile_operator_work(store.clone(), retry_client.clone()).await;
    assert!(!store.instance_exists("instance-delete-retry"));
    assert!(store.materialization().is_none());
    assert_eq!(
        retry_client.deleted(),
        vec![
            object_ref("v1", "Service", "apps", "instance-delete-retry-svc"),
            object_ref("apps/v1", "Deployment", "apps", "instance-delete-retry"),
        ]
    );
}

#[tokio::test]
async fn reconcile_materialization_surfaces_inspect_failed_projection_observation() {
    let store = Arc::new(FakeInstanceStore::default());
    let materialization = ready_materialization("instance-reconcile-inspect-fails", 8);
    let instance_id = materialization.instance_id.as_str().to_owned();
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.fail_inspect();
    let service = StoreBackedOperatorApi::new(store, operator_materializer(client), target());

    let reconciled = service
        .reconcile_materialization(tonic::Request::new(ReconcileMaterializationRequest {
            status_only: false,
            instance_id,
        }))
        .await
        .expect("reconcile materialization response surfaces inspect error")
        .into_inner();

    assert!(reconciled.found);
    assert!(!reconciled.attempted);
    assert_eq!(reconciled.state, "Ready");
    assert_eq!(reconciled.projection_observations.len(), 1);
    let observation = &reconciled.projection_observations[0];
    assert_eq!(observation.state, "inspect_failed");
    assert_eq!(observation.reason, "inspect_failed");
    assert_eq!(
        observation
            .r#ref
            .as_ref()
            .map(|object| object.name.as_str()),
        Some("instance-reconcile-inspect-fails-svc")
    );
}

#[tokio::test]
async fn reconcile_materialization_reports_ready_readiness_without_repairing_ready_row() {
    let store = Arc::new(FakeInstanceStore::default());
    let materialization = ready_materialization("instance-reconcile-ready-observed", 8);
    let instance_id = materialization.instance_id.as_str().to_owned();
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    client.set_readiness(ProjectionReadinessInspection::Ready(
        BackendEndpoint::new(
            "http://instance-reconcile-ready-observed-svc.apps.svc.cluster.local:80",
        )
        .expect("backend endpoint"),
    ));
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let reconciled = service
        .reconcile_materialization(tonic::Request::new(ReconcileMaterializationRequest {
            status_only: false,
            instance_id,
        }))
        .await
        .expect("reconcile materialization response includes readiness observation")
        .into_inner();

    assert!(reconciled.found);
    assert!(!reconciled.attempted);
    assert_eq!(reconciled.state, "Ready");
    assert_eq!(reconciled.projection_observations.len(), 3);
    let observation = reconciled
        .projection_observations
        .iter()
        .find(|observation| observation.state == "ready")
        .expect("ready projection observation returned");
    assert_eq!(
        observation
            .r#ref
            .as_ref()
            .map(|object| object.kind.as_str()),
        Some("Service")
    );
    assert_eq!(
        observation.backend_uri,
        "http://instance-reconcile-ready-observed-svc.apps.svc.cluster.local:80"
    );
    assert!(client.deleted().is_empty());
    assert_eq!(
        store
            .materialization()
            .expect("materialization remains present")
            .state,
        MaterializationState::Ready
    );
}

#[tokio::test]
async fn reconcile_materialization_reports_unready_readiness_without_repairing_ready_row() {
    let store = Arc::new(FakeInstanceStore::default());
    let materialization = ready_materialization("instance-reconcile-unready-observed", 8);
    let instance_id = materialization.instance_id.as_str().to_owned();
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    client.set_readiness(ProjectionReadinessInspection::Unready {
        reason: "no_ready_endpoints".to_owned(),
    });
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let reconciled = service
        .reconcile_materialization(tonic::Request::new(ReconcileMaterializationRequest {
            status_only: false,
            instance_id,
        }))
        .await
        .expect("reconcile materialization response includes unready observation")
        .into_inner();

    assert!(reconciled.found);
    assert!(!reconciled.attempted);
    assert_eq!(reconciled.state, "Ready");
    let observation = reconciled
        .projection_observations
        .iter()
        .find(|observation| observation.state == "unready")
        .expect("unready projection observation returned");
    assert_eq!(observation.reason, "no_ready_endpoints");
    assert!(observation.backend_uri.is_empty());
    assert!(client.deleted().is_empty());
    assert_eq!(
        store
            .materialization()
            .expect("materialization remains present")
            .state,
        MaterializationState::Ready
    );
}

#[tokio::test]
async fn reconcile_materialization_reports_ready_metadata_drift_and_finalizers_without_repair() {
    let store = Arc::new(FakeInstanceStore::default());
    let materialization = ready_materialization("instance-reconcile-ready-drift", 8);
    let instance_id = materialization.instance_id.as_str().to_owned();
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    client.seed_unowned_object(materialization.rendered_objects[0].clone());
    client.seed_deleting_owned_object(
        &materialization,
        materialization.rendered_objects[1].clone(),
        vec!["example.com/cleanup".to_owned()],
    );
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let reconciled = service
        .reconcile_materialization(tonic::Request::new(ReconcileMaterializationRequest {
            status_only: false,
            instance_id,
        }))
        .await
        .expect("reconcile materialization reports Ready metadata drift")
        .into_inner();

    assert!(reconciled.found);
    assert!(!reconciled.attempted);
    assert_eq!(reconciled.state, "Ready");
    assert_eq!(reconciled.projection_observations.len(), 2);
    let unowned = reconciled
        .projection_observations
        .iter()
        .find(|observation| observation.state == "present_unowned")
        .expect("unowned replacement is reported");
    assert_eq!(unowned.reason, "managed_by_mismatch");
    let deleting = reconciled
        .projection_observations
        .iter()
        .find(|observation| observation.state == "deleting_owned")
        .expect("deleting owned object is reported");
    assert_eq!(deleting.finalizers, vec!["example.com/cleanup"]);
    assert!(client.deleted().is_empty());
    assert_eq!(
        store
            .materialization()
            .expect("materialization remains present")
            .state,
        MaterializationState::Ready
    );
}

#[tokio::test]
async fn force_delete_materialization_returns_projection_observations_without_kubernetes_delete() {
    let store = Arc::new(FakeInstanceStore::default());
    let materialization = materialization(
        "instance-force-delete-observe",
        9,
        target(),
        MaterializationState::Deleting,
    );
    let instance_id = materialization.instance_id.as_str().to_owned();
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    let service =
        StoreBackedOperatorApi::new(store, operator_materializer(client.clone()), target());

    let force_deleted = service
        .force_delete_materialization(tonic::Request::new(ForceDeleteMaterializationRequest {
            instance_id,
            operator: "operator-a".to_owned(),
            reason: "manual cleanup already inspected".to_owned(),
        }))
        .await
        .expect("force-delete materialization succeeds")
        .into_inner();

    assert!(force_deleted.found);
    assert_eq!(force_deleted.observed_refs.len(), 2);
    assert_eq!(force_deleted.projection_observations.len(), 2);
    assert!(force_deleted
        .projection_observations
        .iter()
        .all(|observation| observation.state == "present_owned"));
    assert!(
        client.deleted().is_empty(),
        "force-delete response observation must not delete Kubernetes objects"
    );
}

#[tokio::test]
async fn force_release_exclusivity_key_returns_scoped_inspect_failed_projection_observation() {
    let store = Arc::new(FakeInstanceStore::default());
    let mut materialization = materialization(
        "instance-force-release-inspect-fails",
        4,
        target(),
        MaterializationState::Ready,
    );
    materialization.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", "disk-a")];
    store.seed_materialization(materialization);
    let client = FakeKubernetesClient::default();
    client.fail_inspect();
    let service = StoreBackedOperatorApi::new(store, operator_materializer(client), target());

    let force_release = service
        .force_release_exclusivity_key(tonic::Request::new(ForceReleaseExclusivityKeyRequest {
            cluster_id: "cluster-a".to_owned(),
            namespace: "apps".to_owned(),
            key_name: "disk".to_owned(),
            key_value: "disk-a".to_owned(),
            operator: "operator-a".to_owned(),
            reason: "singleton verified externally".to_owned(),
        }))
        .await
        .expect("force-release exclusivity key succeeds")
        .into_inner();

    assert_eq!(force_release.updated_materializations, 1);
    assert_eq!(force_release.projection_observations.len(), 1);
    let observation = &force_release.projection_observations[0];
    assert_eq!(observation.state, "inspect_failed");
    assert_eq!(observation.reason, "inspect_failed");
    assert_eq!(
        observation
            .r#ref
            .as_ref()
            .map(|object| object.name.as_str()),
        Some("instance-force-release-inspect-fails-svc")
    );
}

#[tokio::test]
async fn workload_class_api_rejects_zero_and_multiple_replicas() {
    let store = Arc::new(FakeInstanceStore::default());
    let service = store_operator_api(store);
    for kind in [
        control_plane::api::pb::WorkloadKind::Deployment,
        control_plane::api::pb::WorkloadKind::StatefulSet,
    ] {
        for replicas in [0, 2] {
            let mut template = stateful_manifest_template_proto();
            let workload = template.workload.as_mut().unwrap();
            workload.kind = kind as i32;
            workload.replicas = Some(replicas);
            let error = service
                .create_workload_class_version(tonic::Request::new(
                    CreateWorkloadClassVersionRequest {
                        class_id: format!("unsupported-{replicas}"),
                        version: 1,
                        value_schema: None,
                        template: Some(template),
                        sleep_policy: Some(sleep_policy_proto()),
                        exclusivity_keys: vec![],
                    },
                ))
                .await
                .expect_err("unsupported replica count rejected by operator API");
            assert_eq!(error.code(), Code::InvalidArgument);
            assert!(error.message().contains("exactly one replica"));
        }
    }
}

#[tokio::test]
async fn store_backed_operator_methods_cover_workload_routes_and_http01() {
    let store = Arc::new(FakeInstanceStore::default());
    let service = store_operator_api(store.clone());
    let template = stateful_manifest_template_proto();

    let created_class = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "class-1".to_owned(),
            version: 1,
            value_schema: Some(stateful_value_schema()),
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect("create workload class succeeds")
        .into_inner();
    assert_eq!(created_class.template, Some(template.clone()));
    assert_eq!(created_class.sleep_policy, Some(sleep_policy_proto()));

    let loaded_class = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "class-1".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class succeeds")
        .into_inner();
    assert_eq!(loaded_class, created_class);

    let missing_template = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "class-missing-template".to_owned(),
            version: 1,
            value_schema: None,
            template: None,
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("template is required");
    assert_eq!(missing_template.code(), Code::InvalidArgument);
    assert!(missing_template.message().contains("template is required"));

    let missing_policy = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "class-missing-policy".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(template.clone()),
            sleep_policy: None,
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("sleep policy is required");
    assert_eq!(missing_policy.code(), Code::InvalidArgument);
    assert!(missing_policy
        .message()
        .contains("sleep_policy is required"));

    let invalid_policy = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "class-invalid-policy".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(template.clone()),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 0,
                idle_retry_backoff_ms: 5_000,
                drain_grace_timeout_ms: 30_000,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("invalid sleep policy is rejected");
    assert_eq!(invalid_policy.code(), Code::InvalidArgument);
    assert!(invalid_policy
        .message()
        .contains("sleep_policy.idle_timeout_ms"));

    let created_route = service
        .create_route_binding(tonic::Request::new(CreateRouteBindingRequest {
            route_binding_id: "route-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            identity: Some(http_identity("App.Example.COM.", Some("/api"))),
            protocol: ProtocolRoute::Http as i32,
        }))
        .await
        .expect("create route binding succeeds")
        .into_inner();
    assert_eq!(created_route.route_binding_id, "route-1");
    let http = match created_route
        .identity
        .as_ref()
        .and_then(|identity| identity.kind.as_ref())
        .expect("route identity returned")
    {
        route_identity::Kind::Http(http) => http,
        route_identity::Kind::Sni(_) => panic!("expected HTTP route"),
    };
    assert_eq!(
        http.host.as_ref().expect("host returned").host,
        "app.example.com"
    );

    let loaded_route = service
        .get_route_binding(tonic::Request::new(GetRouteBindingRequest {
            route_binding_id: "route-1".to_owned(),
        }))
        .await
        .expect("get route binding succeeds")
        .into_inner();
    assert_eq!(loaded_route, created_route);
    let deleted_route = service
        .delete_route_binding(tonic::Request::new(DeleteRouteBindingRequest {
            route_binding_id: "route-1".to_owned(),
        }))
        .await
        .expect("delete route binding succeeds")
        .into_inner();
    assert!(deleted_route.deleted);

    let expires_at = UNIX_EPOCH + Duration::from_secs(4_102_444_800);
    let challenge_key = Http01ChallengeKey {
        host: "Acme.Example.COM.".to_owned(),
        token: "token-a".to_owned(),
    };
    let put_challenge = service
        .put_http01_challenge(tonic::Request::new(PutHttp01ChallengeRequest {
            key: Some(challenge_key.clone()),
            key_authorization: "key-auth-a".to_owned(),
            expires_at_unix_millis: 4_102_444_800_000,
        }))
        .await
        .expect("put HTTP-01 challenge succeeds")
        .into_inner();
    assert_eq!(
        put_challenge.key.as_ref().expect("key returned").host,
        "acme.example.com"
    );

    let resolved = store_proxy_api(store.clone())
        .resolve_http01_challenge(tonic::Request::new(ResolveHttp01ChallengeRequest {
            key: Some(challenge_key.clone()),
        }))
        .await
        .expect("resolve HTTP-01 challenge succeeds")
        .into_inner()
        .challenge
        .expect("challenge resolves");
    assert_eq!(resolved.key_authorization, "key-auth-a");

    let deleted = service
        .delete_http01_challenge(tonic::Request::new(DeleteHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "acme.example.com".to_owned(),
                token: "token-a".to_owned(),
            }),
        }))
        .await
        .expect("delete HTTP-01 challenge succeeds")
        .into_inner();
    assert!(deleted.deleted);
    assert!(expires_at > SystemTime::now());

    // begin_sleep stamps the deleting materialization with the Running
    // generation and moves the instance to Draining one generation later, so
    // the reconciler finalizes the drain only for that generation pairing.
    store.seed_instance(domain_instance(
        "instance-reconcile-operator",
        DomainInstanceState::Draining,
        4,
    ));
    let reconcile_materialization = materialization(
        "instance-reconcile-operator",
        3,
        target(),
        MaterializationState::Deleting,
    );
    let reconcile_materialization_id = reconcile_materialization.id.as_str().to_owned();
    let reconcile_instance_id = reconcile_materialization.instance_id.as_str().to_owned();
    store.seed_materialization(reconcile_materialization);
    let reconciled = service
        .reconcile_materialization(tonic::Request::new(ReconcileMaterializationRequest {
            status_only: false,
            instance_id: reconcile_instance_id.clone(),
        }))
        .await
        .expect("reconcile materialization succeeds")
        .into_inner();
    assert!(reconciled.found);
    assert!(reconciled.attempted);
    assert_eq!(reconciled.materialization_id, reconcile_materialization_id);
    assert_eq!(reconciled.state, "Deleting");
    assert_eq!(reconciled.observed_refs.len(), 2);
    assert!(
        store.reconciliation_claim_owners().is_empty(),
        "admin schedules without creating another driver"
    );
    assert_eq!(
        store
            .materialization()
            .expect("scheduled work remains")
            .state,
        MaterializationState::Deleting
    );

    let force_materialization = materialization(
        "instance-force-delete",
        1,
        target(),
        MaterializationState::Deleting,
    );
    let force_materialization_id = force_materialization.id.as_str().to_owned();
    let force_instance_id = force_materialization.instance_id.as_str().to_owned();
    store.seed_materialization(force_materialization);
    let force_deleted = service
        .force_delete_materialization(tonic::Request::new(ForceDeleteMaterializationRequest {
            instance_id: force_instance_id.clone(),
            operator: "operator-a".to_owned(),
            reason: "inspected cleanup completed".to_owned(),
        }))
        .await
        .expect("force-delete materialization succeeds")
        .into_inner();
    assert!(force_deleted.found);
    assert_eq!(force_deleted.materialization_id, force_materialization_id);
    assert_eq!(force_deleted.observed_refs.len(), 2);
    assert_eq!(force_deleted.projection_observations.len(), 2);
    assert!(force_deleted
        .projection_observations
        .iter()
        .all(|observation| observation.state == "missing"));

    let mut force_release_materialization = materialization(
        "instance-force-release",
        1,
        target(),
        MaterializationState::Ready,
    );
    force_release_materialization.exclusivity_keys =
        vec![RenderedExclusivityKey::new("disk", "disk-a")];
    store.seed_materialization(force_release_materialization);
    let force_release = service
        .force_release_exclusivity_key(tonic::Request::new(ForceReleaseExclusivityKeyRequest {
            cluster_id: "cluster-a".to_owned(),
            namespace: "apps".to_owned(),
            key_name: "disk".to_owned(),
            key_value: "disk-a".to_owned(),
            operator: "operator-a".to_owned(),
            reason: "emergency release after inspected cleanup".to_owned(),
        }))
        .await
        .expect("force-release exclusivity key succeeds")
        .into_inner();
    assert_eq!(force_release.updated_materializations, 1);
    assert_eq!(force_release.projection_observations.len(), 2);
    assert!(force_release
        .projection_observations
        .iter()
        .all(|observation| observation.state == "missing"));
}

#[tokio::test]
async fn store_backed_workload_class_api_round_trips_host_path_template() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let template = host_path_manifest_template_proto();

    let created = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "host-path-class".to_owned(),
            version: 1,
            value_schema: Some(stateful_value_schema()),
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect("create workload class with hostPath template succeeds")
        .into_inner();

    assert_eq!(created.template, Some(template.clone()));

    let loaded = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "host-path-class".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class with hostPath template succeeds")
        .into_inner();

    assert_eq!(loaded, created);
    let source = loaded.template.as_ref().expect("template returned").volumes[0]
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
        .expect("volume source returned");
    assert!(matches!(
        source,
        persistent_volume_source_template::Kind::HostPath(_)
    ));
}

#[tokio::test]
async fn store_backed_workload_class_api_round_trips_csi_secret_refs() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let template = stateful_manifest_template_proto();

    let created = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "csi-secret-ref-class".to_owned(),
            version: 1,
            value_schema: Some(stateful_value_schema()),
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect("create workload class with CSI secret ref template succeeds")
        .into_inner();

    assert_eq!(created.template, Some(template.clone()));

    let loaded = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "csi-secret-ref-class".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class with CSI secret ref template succeeds")
        .into_inner();

    assert_eq!(loaded, created);
    let source = loaded.template.as_ref().expect("template returned").volumes[0]
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
        .expect("volume source returned");
    let persistent_volume_source_template::Kind::Csi(csi) = source else {
        panic!("expected CSI source");
    };
    assert_eq!(
        csi.node_publish_secret_ref
            .as_ref()
            .and_then(|ref_| ref_.namespace.as_ref()),
        Some(&literal_text("storage-secrets"))
    );
}

#[tokio::test]
async fn store_backed_workload_class_api_round_trips_raw_manifest_templates() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let mut template = stateful_manifest_template_proto();
    template.raw_objects = vec![RawKubernetesManifestTemplate {
        manifest: Some(TemplateText {
            parts: vec![
                TemplateTextPart {
                    kind: Some(template_text_part::Kind::Literal(
                        "apiVersion: v1\nkind: Service\nmetadata:\n  name: raw-".to_owned(),
                    )),
                },
                TemplateTextPart {
                    kind: Some(template_text_part::Kind::InstanceValue("tenant".to_owned())),
                },
                TemplateTextPart {
                    kind: Some(template_text_part::Kind::Literal("-".to_owned())),
                },
                TemplateTextPart {
                    kind: Some(template_text_part::Kind::InstanceValue(
                        control_plane::manifest::INSTANCE_SUFFIX_VALUE.to_owned(),
                    )),
                },
            ],
        }),
    }];

    let created = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "raw-manifest-class".to_owned(),
            version: 1,
            value_schema: Some(stateful_value_schema()),
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect("create workload class with raw manifest template succeeds")
        .into_inner();

    assert_eq!(created.template, Some(template.clone()));

    let loaded = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "raw-manifest-class".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class with raw manifest template succeeds")
        .into_inner();

    assert_eq!(loaded, created);
    assert_eq!(
        loaded
            .template
            .as_ref()
            .expect("template returned")
            .raw_objects,
        template.raw_objects
    );
}

#[tokio::test]
async fn store_backed_workload_class_api_round_trips_exclusivity_keys() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let key = WorkloadExclusivityKey {
        name: "disk".to_owned(),
        value: Some(TemplateText {
            parts: vec![TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue(
                    "volume_handle".to_owned(),
                )),
            }],
        }),
    };

    let created = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "exclusive-class".to_owned(),
            version: 1,
            value_schema: Some(value_schema(&["tenant", "volume", "volume_handle"])),
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![key.clone()],
        }))
        .await
        .expect("create workload class with exclusivity key succeeds")
        .into_inner();

    assert_eq!(created.exclusivity_keys, vec![key.clone()]);

    let loaded = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "exclusive-class".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class with exclusivity key succeeds")
        .into_inner();

    assert_eq!(loaded, created);
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_invalid_exclusivity_keys() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let valid_value = TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::InstanceValue(
                "volume_handle".to_owned(),
            )),
        }],
    };

    let invalid_name = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "invalid-exclusive-name".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk/name".to_owned(),
                value: Some(valid_value.clone()),
            }],
        }))
        .await
        .expect_err("unsafe key name is rejected");
    assert_eq!(invalid_name.code(), Code::InvalidArgument);
    assert!(invalid_name.message().contains("exclusivity key"));

    let missing_value = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "missing-exclusive-value".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk".to_owned(),
                value: None,
            }],
        }))
        .await
        .expect_err("missing key value template is rejected");
    assert_eq!(missing_value.code(), Code::InvalidArgument);
    assert!(missing_value.message().contains("exclusivity_keys.value"));

    let empty_parts = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "empty-exclusive-value".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk".to_owned(),
                value: Some(TemplateText { parts: vec![] }),
            }],
        }))
        .await
        .expect_err("empty key value template is rejected");
    assert_eq!(empty_parts.code(), Code::InvalidArgument);
    assert!(empty_parts.message().contains("template text parts"));

    let blank_instance_value = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "blank-exclusive-value-field".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk".to_owned(),
                value: Some(TemplateText {
                    parts: vec![TemplateTextPart {
                        kind: Some(template_text_part::Kind::InstanceValue(" ".to_owned())),
                    }],
                }),
            }],
        }))
        .await
        .expect_err("blank key value field is rejected");
    assert_eq!(blank_instance_value.code(), Code::InvalidArgument);
    assert!(blank_instance_value
        .message()
        .contains("instance value field names"));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_empty_template_static_strings() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let mut template = stateful_manifest_template_proto();
    template
        .workload
        .as_mut()
        .expect("workload template exists")
        .app_container
        .as_mut()
        .expect("app container exists")
        .name
        .clear();

    let error = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "empty-container-name-class".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(template),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("empty static template names are rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("template.container.name"));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_empty_template_text_parts() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let mut template = stateful_manifest_template_proto();
    template
        .workload
        .as_mut()
        .expect("workload template exists")
        .name = Some(TemplateText { parts: Vec::new() });

    let error = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "empty-template-text-class".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(template),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("empty template text parts are rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("template text parts"));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_incomplete_csi_secret_ref() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let mut template = stateful_manifest_template_proto();
    let source = template.volumes[0]
        .source
        .as_mut()
        .and_then(|source| source.kind.as_mut())
        .expect("CSI source exists");
    let persistent_volume_source_template::Kind::Csi(csi) = source else {
        panic!("expected CSI source");
    };
    csi.node_publish_secret_ref = Some(CsiSecretRefTemplate {
        name: None,
        namespace: Some(literal_text("storage-secrets")),
    });

    let error = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "incomplete-csi-secret-ref-class".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(template),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("incomplete CSI secret ref is rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error
        .message()
        .contains("template.volumes.source.csi.node_publish_secret_ref.name is required"));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_incomplete_raw_manifest_template() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let mut template = stateful_manifest_template_proto();
    template.raw_objects = vec![RawKubernetesManifestTemplate { manifest: None }];

    let error = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            class_id: "incomplete-raw-manifest-class".to_owned(),
            version: 1,
            value_schema: None,
            template: Some(template),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("incomplete raw manifest template is rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error
        .message()
        .contains("template.raw_objects.manifest is required"));
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_create_instance() {
    let response = store_operator_grpc_service(store_with_class("class-transport", 3))
        .oneshot(grpc_create_instance_request(
            CreateInstanceRequest {
                instance_id: "instance-transport".to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "class-transport".to_owned(),
                    version: 3,
                }),
                values: [("tenant".to_owned(), "transport".to_owned())].into(),
                routes: Vec::new(),
            },
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("native gRPC request should route through store-backed service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("successful gRPC status is returned");

    assert_eq!(status, "0");

    let created = decode_grpc_instance_response(collected.to_bytes().as_ref());
    assert_eq!(created.instance_id, "instance-transport");
    assert_eq!(created.state, InstanceState::Cold as i32);
    assert_eq!(created.generation, 0);
    assert_eq!(created.values["tenant"], "transport");
}

#[tokio::test]
async fn native_grpc_operator_auth_rejects_missing_and_wrong_role_before_store_mutation() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "auth-delete-instance",
        DomainInstanceState::Cold,
        0,
    ));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let missing_response = authenticated_operator_service(Arc::clone(&store_for_service))
        .oneshot(grpc_operator_unary_request(
            DeleteInstanceRequest {
                instance_id: "auth-delete-instance".to_owned(),
                expected_generation: Some(0),
            },
            "DeleteInstance",
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("auth rejection is a gRPC response");
    let (headers, trailers, body) = collect_grpc_response_parts(missing_response).await;
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "16");
    assert!(store.instance_exists("auth-delete-instance"));
    assert_eq!(store.delete_requests(), Vec::new());

    let wrong_role_response = authenticated_operator_service(store_for_service)
        .oneshot(with_authorization(
            grpc_operator_unary_request(
                DeleteInstanceRequest {
                    instance_id: "auth-delete-instance".to_owned(),
                    expected_generation: Some(0),
                },
                "DeleteInstance",
                "application/grpc",
                Version::HTTP_2,
            ),
            "Bearer proxy-token",
        ))
        .await
        .expect("wrong-role rejection is a gRPC response");
    let (headers, trailers, body) = collect_grpc_response_parts(wrong_role_response).await;
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "7");
    assert!(store.instance_exists("auth-delete-instance"));
    assert_eq!(store.delete_requests(), Vec::new());
}

#[tokio::test]
async fn native_grpc_operator_auth_accepts_valid_operator_credentials() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "auth-valid-instance",
        DomainInstanceState::Cold,
        0,
    ));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let response = authenticated_operator_service(store_for_service)
        .oneshot(with_authorization(
            grpc_operator_unary_request(
                DeleteInstanceRequest {
                    instance_id: "auth-valid-instance".to_owned(),
                    expected_generation: Some(0),
                },
                "DeleteInstance",
                "application/grpc",
                Version::HTTP_2,
            ),
            "Bearer operator-token",
        ))
        .await
        .expect("valid credentials dispatch to handler");
    let (headers, trailers, body) = collect_grpc_response_parts(response).await;
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    let deleted = decode_grpc_message::<DeleteInstanceResponse>(body.as_ref());
    assert!(deleted.accepted);
    assert!(store.instance_exists("auth-valid-instance"));
    reconcile_operator_work(store.clone(), FakeKubernetesClient::default()).await;
    assert!(!store.instance_exists("auth-valid-instance"));
    assert_eq!(store.delete_requests().len(), 1);
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_create_route_binding() {
    let response = store_operator_grpc_service(Arc::new(FakeInstanceStore::default()))
        .oneshot(grpc_create_route_binding_request(
            CreateRouteBindingRequest {
                route_binding_id: "route-transport".to_owned(),
                instance_id: "instance-transport".to_owned(),
                identity: Some(sni_identity("DB.Example.COM.")),
                protocol: ProtocolRoute::TlsSni as i32,
            },
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("native gRPC request should route through store-backed service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("successful gRPC status is returned");

    assert_eq!(status, "0");

    let created = decode_grpc_route_binding_response(collected.to_bytes().as_ref());
    assert_eq!(created.route_binding_id, "route-transport");
    assert_eq!(created.protocol, ProtocolRoute::TlsSni as i32);
    let sni = match created
        .identity
        .as_ref()
        .and_then(|identity| identity.kind.as_ref())
        .expect("route identity returned")
    {
        route_identity::Kind::Sni(sni) => sni,
        route_identity::Kind::Http(_) => panic!("expected SNI route"),
    };
    assert_eq!(
        sni.host.as_ref().expect("host returned").host,
        "db.example.com"
    );
}

#[test]
fn native_grpc_server_can_be_constructed_with_store_backed_operator_service() {
    let _router = tonic::transport::Server::builder().add_service(store_operator_grpc_service(
        Arc::new(FakeInstanceStore::default()),
    ));

    assert_eq!(
        <control_plane::api::server::StoreBackedOperatorGrpcService<
            FakeKubernetesClient,
        > as NamedService>::NAME,
        OPERATOR_SERVICE_NAME
    );
}

#[tokio::test]
async fn grpc_web_store_backed_requests_cover_operator_api_parity() {
    let store: Arc<dyn ControlPlaneStore> = Arc::new(FakeInstanceStore::default());
    let template = stateful_manifest_template_proto();

    let created_class: WorkloadClassVersion = grpc_web_store_unary(
        Arc::clone(&store),
        "CreateWorkloadClassVersion",
        CreateWorkloadClassVersionRequest {
            class_id: "class-grpc-web".to_owned(),
            version: 11,
            value_schema: Some(stateful_value_schema()),
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        },
    )
    .await;

    let loaded_class: WorkloadClassVersion = grpc_web_store_unary(
        Arc::clone(&store),
        "GetWorkloadClassVersion",
        GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "class-grpc-web".to_owned(),
                version: 11,
            }),
        },
    )
    .await;
    assert_eq!(loaded_class, created_class);

    let created_instance: Instance = grpc_web_store_unary(
        Arc::clone(&store),
        "CreateInstance",
        CreateInstanceRequest {
            instance_id: "instance-grpc-web".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-grpc-web".to_owned(),
                version: 11,
            }),
            values: [
                ("tenant".to_owned(), "acme".to_owned()),
                ("volume".to_owned(), "vol-1".to_owned()),
            ]
            .into(),
            routes: Vec::new(),
        },
    )
    .await;
    assert_eq!(created_instance.instance_id, "instance-grpc-web");
    assert_eq!(created_instance.state, InstanceState::Cold as i32);

    let loaded_instance: Instance = grpc_web_store_unary(
        Arc::clone(&store),
        "GetInstance",
        GetInstanceRequest {
            instance_id: "instance-grpc-web".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_instance, created_instance);

    let created_route: RouteBinding = grpc_web_store_unary(
        Arc::clone(&store),
        "CreateRouteBinding",
        CreateRouteBindingRequest {
            route_binding_id: "route-grpc-web".to_owned(),
            instance_id: "instance-grpc-web".to_owned(),
            identity: Some(http_identity("App.Example.COM.", Some("/app"))),
            protocol: ProtocolRoute::Http as i32,
        },
    )
    .await;
    assert_eq!(created_route.route_binding_id, "route-grpc-web");

    let loaded_route: RouteBinding = grpc_web_store_unary(
        Arc::clone(&store),
        "GetRouteBinding",
        GetRouteBindingRequest {
            route_binding_id: "route-grpc-web".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_route, created_route);

    let challenge: Http01Challenge = grpc_web_store_unary(
        Arc::clone(&store),
        "PutHttp01Challenge",
        PutHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "Acme.Example.COM.".to_owned(),
                token: "token-grpc-web".to_owned(),
            }),
            key_authorization: "key-auth-grpc-web".to_owned(),
            expires_at_unix_millis: 4_102_444_800_000,
        },
    )
    .await;
    assert_eq!(
        challenge.key.as_ref().expect("HTTP-01 key returned").host,
        "acme.example.com"
    );

    let resolved: ResolveHttp01ChallengeResponse = grpc_proxy_store_unary(
        Arc::clone(&store),
        "ResolveHttp01Challenge",
        ResolveHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "acme.example.com".to_owned(),
                token: "token-grpc-web".to_owned(),
            }),
        },
    )
    .await;
    assert_eq!(
        resolved
            .challenge
            .as_ref()
            .expect("HTTP-01 challenge resolves")
            .key_authorization,
        "key-auth-grpc-web"
    );

    let deleted_challenge: DeleteHttp01ChallengeResponse = grpc_web_store_unary(
        Arc::clone(&store),
        "DeleteHttp01Challenge",
        DeleteHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "acme.example.com".to_owned(),
                token: "token-grpc-web".to_owned(),
            }),
        },
    )
    .await;
    assert!(deleted_challenge.deleted);

    let deleted_route: DeleteRouteBindingResponse = grpc_web_store_unary(
        Arc::clone(&store),
        "DeleteRouteBinding",
        DeleteRouteBindingRequest {
            route_binding_id: "route-grpc-web".to_owned(),
        },
    )
    .await;
    assert!(deleted_route.deleted);

    let deleted_instance: DeleteInstanceResponse = grpc_web_store_unary(
        store,
        "DeleteInstance",
        DeleteInstanceRequest {
            instance_id: "instance-grpc-web".to_owned(),
            expected_generation: Some(0),
        },
    )
    .await;
    assert!(deleted_instance.accepted);
}

#[tokio::test]
async fn native_grpc_store_backed_requests_cover_operator_api_parity() {
    let store: Arc<dyn ControlPlaneStore> = Arc::new(FakeInstanceStore::default());
    let template = stateful_manifest_template_proto();

    let created_class: WorkloadClassVersion = grpc_store_unary(
        Arc::clone(&store),
        "CreateWorkloadClassVersion",
        CreateWorkloadClassVersionRequest {
            class_id: "class-native".to_owned(),
            version: 13,
            value_schema: Some(stateful_value_schema()),
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        },
    )
    .await;

    let loaded_class: WorkloadClassVersion = grpc_store_unary(
        Arc::clone(&store),
        "GetWorkloadClassVersion",
        GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "class-native".to_owned(),
                version: 13,
            }),
        },
    )
    .await;
    assert_eq!(loaded_class, created_class);

    let created_instance: Instance = grpc_store_unary(
        Arc::clone(&store),
        "CreateInstance",
        CreateInstanceRequest {
            instance_id: "instance-native".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-native".to_owned(),
                version: 13,
            }),
            values: [
                ("tenant".to_owned(), "acme".to_owned()),
                ("volume".to_owned(), "vol-1".to_owned()),
            ]
            .into(),
            routes: Vec::new(),
        },
    )
    .await;
    assert_eq!(created_instance.instance_id, "instance-native");
    assert_eq!(created_instance.state, InstanceState::Cold as i32);

    let loaded_instance: Instance = grpc_store_unary(
        Arc::clone(&store),
        "GetInstance",
        GetInstanceRequest {
            instance_id: "instance-native".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_instance, created_instance);

    let created_route: RouteBinding = grpc_store_unary(
        Arc::clone(&store),
        "CreateRouteBinding",
        CreateRouteBindingRequest {
            route_binding_id: "route-native".to_owned(),
            instance_id: "instance-native".to_owned(),
            identity: Some(http_identity("Native.Example.COM.", Some("/app"))),
            protocol: ProtocolRoute::Http as i32,
        },
    )
    .await;
    assert_eq!(created_route.route_binding_id, "route-native");

    let loaded_route: RouteBinding = grpc_store_unary(
        Arc::clone(&store),
        "GetRouteBinding",
        GetRouteBindingRequest {
            route_binding_id: "route-native".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_route, created_route);

    let challenge: Http01Challenge = grpc_store_unary(
        Arc::clone(&store),
        "PutHttp01Challenge",
        PutHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "Native-Acme.Example.COM.".to_owned(),
                token: "token-native".to_owned(),
            }),
            key_authorization: "key-auth-native".to_owned(),
            expires_at_unix_millis: 4_102_444_800_000,
        },
    )
    .await;
    assert_eq!(
        challenge.key.as_ref().expect("HTTP-01 key returned").host,
        "native-acme.example.com"
    );

    let resolved: ResolveHttp01ChallengeResponse = grpc_proxy_store_unary(
        Arc::clone(&store),
        "ResolveHttp01Challenge",
        ResolveHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "native-acme.example.com".to_owned(),
                token: "token-native".to_owned(),
            }),
        },
    )
    .await;
    assert_eq!(
        resolved
            .challenge
            .as_ref()
            .expect("HTTP-01 challenge resolves")
            .key_authorization,
        "key-auth-native"
    );

    let deleted_challenge: DeleteHttp01ChallengeResponse = grpc_store_unary(
        Arc::clone(&store),
        "DeleteHttp01Challenge",
        DeleteHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "native-acme.example.com".to_owned(),
                token: "token-native".to_owned(),
            }),
        },
    )
    .await;
    assert!(deleted_challenge.deleted);

    let deleted_route: DeleteRouteBindingResponse = grpc_store_unary(
        Arc::clone(&store),
        "DeleteRouteBinding",
        DeleteRouteBindingRequest {
            route_binding_id: "route-native".to_owned(),
        },
    )
    .await;
    assert!(deleted_route.deleted);

    let deleted_instance: DeleteInstanceResponse = grpc_store_unary(
        store,
        "DeleteInstance",
        DeleteInstanceRequest {
            instance_id: "instance-native".to_owned(),
            expected_generation: Some(0),
        },
    )
    .await;
    assert!(deleted_instance.accepted);
}

#[tokio::test]
async fn grpc_web_cors_preflight_allows_browser_operator_headers() {
    let response = operator_grpc_web_cors_layer(&browser_origins())
        .layer(
            tonic_web::GrpcWebLayer::new().layer(store_operator_grpc_service(Arc::new(
                FakeInstanceStore::default(),
            ))),
        )
        .oneshot(grpc_web_preflight_request("CreateInstance"))
        .await
        .expect("CORS preflight should be handled by grpc-web router");

    assert!(response.status().is_success());
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("allow-origin is returned"),
        "https://operator.example"
    );
    let allow_methods = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_METHODS)
        .expect("allow-methods is returned")
        .to_str()
        .expect("allow-methods is valid");
    assert!(allow_methods.contains("POST"));
    assert!(allow_methods.contains("OPTIONS"));

    let allow_headers = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .expect("allow-headers is returned")
        .to_str()
        .expect("allow-headers is valid")
        .to_ascii_lowercase();
    assert!(allow_headers.contains("authorization"));
    assert!(allow_headers.contains("content-type"));
    assert!(allow_headers.contains("x-grpc-web"));
    assert!(allow_headers.contains("x-sleepypods-operator"));
}

#[tokio::test]
async fn grpc_web_preserves_operator_metadata_headers_at_api_boundary() {
    let captured_metadata = Arc::new(Mutex::new(Vec::new()));
    let service = tonic_web::GrpcWebLayer::new().layer(OperatorControlPlaneServer::new(
        MetadataCapturingOperatorApi::new(Arc::clone(&captured_metadata)),
    ));

    let response = service
        .oneshot(grpc_web_operator_request(
            "CreateInstance",
            CreateInstanceRequest {
                instance_id: "metadata-instance".to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "metadata-class".to_owned(),
                    version: 1,
                }),
                values: Default::default(),
                routes: Vec::new(),
            },
        ))
        .await
        .expect("gRPC-Web request with metadata should reach service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("metadata response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    let captured = captured_metadata
        .lock()
        .expect("captured metadata lock is available");
    assert!(captured
        .iter()
        .any(|(name, value)| { name == "authorization" && value == "Bearer operator-token" }));
    assert!(captured
        .iter()
        .any(|(name, value)| { name == "x-sleepypods-operator" && value == "tenant-a" }));
}

#[tokio::test]
async fn grpc_web_store_backed_errors_return_structured_status() {
    let response = operator_grpc_web_cors_layer(&browser_origins())
        .layer(
            tonic_web::GrpcWebLayer::new().layer(store_operator_grpc_service(Arc::new(
                FakeInstanceStore::default(),
            ))),
        )
        .oneshot(grpc_web_operator_request(
            "GetInstance",
            GetInstanceRequest {
                instance_id: "missing-instance".to_owned(),
            },
        ))
        .await
        .expect("gRPC-Web missing instance request should route to store-backed service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("error response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();

    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "5");
    let message = grpc_header_value(&headers, trailers.as_ref(), body.as_ref(), "grpc-message")
        .expect("structured grpc-message is returned");
    assert!(message.contains("instance%20not%20found"));
}

#[tokio::test]
async fn grpc_web_operator_auth_accepts_browser_authorization_and_rejects_failures_without_mutation(
) {
    let store = store_with_class("class-auth", 1);
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let valid_response = operator_grpc_web_cors_layer(&browser_origins())
        .layer(
            tonic_web::GrpcWebLayer::new().layer(authenticated_operator_service(Arc::clone(
                &store_for_service,
            ))),
        )
        .oneshot(grpc_web_operator_request(
            "CreateInstance",
            CreateInstanceRequest {
                instance_id: "grpc-web-auth-valid".to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "class-auth".to_owned(),
                    version: 1,
                }),
                values: Default::default(),
                routes: Vec::new(),
            },
        ))
        .await
        .expect("valid gRPC-Web auth response");
    let (headers, trailers, body) = collect_grpc_response_parts(valid_response).await;
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");
    assert!(store.instance_exists("grpc-web-auth-valid"));

    let mut missing_request = grpc_web_operator_request(
        "CreateInstance",
        CreateInstanceRequest {
            instance_id: "grpc-web-auth-missing".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-auth".to_owned(),
                version: 1,
            }),
            values: Default::default(),
            routes: Vec::new(),
        },
    );
    missing_request.headers_mut().remove(header::AUTHORIZATION);
    let missing_response = operator_grpc_web_cors_layer(&browser_origins())
        .layer(
            tonic_web::GrpcWebLayer::new().layer(authenticated_operator_service(Arc::clone(
                &store_for_service,
            ))),
        )
        .oneshot(missing_request)
        .await
        .expect("missing auth maps to gRPC-Web response");
    let (headers, trailers, body) = collect_grpc_response_parts(missing_response).await;
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "16");
    assert!(!store.instance_exists("grpc-web-auth-missing"));

    let wrong_response = operator_grpc_web_cors_layer(&browser_origins())
        .layer(
            tonic_web::GrpcWebLayer::new().layer(authenticated_operator_service(store_for_service)),
        )
        .oneshot(with_authorization(
            grpc_web_operator_request(
                "CreateInstance",
                CreateInstanceRequest {
                    instance_id: "grpc-web-auth-wrong".to_owned(),
                    workload_class: Some(WorkloadClassVersionRef {
                        class_id: "class-auth".to_owned(),
                        version: 1,
                    }),
                    values: Default::default(),
                    routes: Vec::new(),
                },
            ),
            "Bearer proxy-token",
        ))
        .await
        .expect("wrong role maps to gRPC-Web response");
    let (headers, trailers, body) = collect_grpc_response_parts(wrong_response).await;
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "7");
    assert!(!store.instance_exists("grpc-web-auth-wrong"));
}

#[tokio::test]
async fn operator_service_answers_unimplemented_for_proxy_and_sidecar_methods() {
    for path in [
        "/sleepypods.controlplane.v1.OperatorControlPlane/ResolveHttp01Challenge",
        "/sleepypods.controlplane.v1.OperatorControlPlane/WakeInstance",
        "/sleepypods.controlplane.v1.OperatorControlPlane/Subscribe",
        "/sleepypods.controlplane.v1.OperatorControlPlane/ReportIdle",
        "/sleepypods.controlplane.v1.ProxyControlPlane/Subscribe",
        "/sleepypods.controlplane.v1.SidecarControlPlane/ReportIdle",
    ] {
        let mut request = grpc_web_operator_request("GetInstance", GetInstanceRequest::default());
        *request.uri_mut() = path.parse().expect("method path parses");
        let response = tonic_web::GrpcWebLayer::new()
            .layer(store_operator_grpc_service(Arc::new(
                FakeInstanceStore::default(),
            )))
            .oneshot(request)
            .await
            .expect("gRPC-Web request reaches the operator service");
        let (headers, trailers, body) = collect_grpc_response_parts(response).await;
        assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "12");
    }
}

async fn grpc_web_store_unary<M, R>(
    store: Arc<dyn ControlPlaneStore>,
    method: &'static str,
    request: R,
) -> M
where
    M: Message + Default,
    R: Message,
{
    let response = operator_grpc_web_cors_layer(&browser_origins())
        .layer(tonic_web::GrpcWebLayer::new().layer(store_operator_grpc_service(store)))
        .oneshot(grpc_web_operator_request(method, request))
        .await
        .expect("gRPC-Web request should route through store-backed service");
    let headers = response.headers().clone();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("gRPC-Web content type")
        .clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("gRPC-Web response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();

    assert_eq!(content_type, "application/grpc-web+proto");
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    decode_grpc_message(body.as_ref())
}

async fn grpc_store_unary<M, R>(
    store: Arc<dyn ControlPlaneStore>,
    method: &'static str,
    request: R,
) -> M
where
    M: Message + Default,
    R: Message,
{
    let response = store_operator_grpc_service(store)
        .oneshot(grpc_operator_unary_request(
            request,
            method,
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("native gRPC request should route through store-backed service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native gRPC response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();

    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    decode_grpc_message(body.as_ref())
}

async fn grpc_proxy_store_unary<M, R>(
    store: Arc<dyn ControlPlaneStore>,
    method: &'static str,
    request: R,
) -> M
where
    M: Message + Default,
    R: Message,
{
    let mut request =
        grpc_operator_unary_request(request, method, "application/grpc", Version::HTTP_2);
    *request.uri_mut() = format!("/sleepypods.controlplane.v1.ProxyControlPlane/{method}")
        .parse()
        .unwrap();
    let response = control_plane::api::proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
    )
    .oneshot(request)
    .await
    .expect("native gRPC request should route through store-backed service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native gRPC response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();

    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    decode_grpc_message(body.as_ref())
}

fn grpc_create_instance_request(
    request: CreateInstanceRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_operator_unary_request(request, "CreateInstance", content_type, version)
}

fn grpc_create_route_binding_request(
    request: CreateRouteBindingRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_operator_unary_request(request, "CreateRouteBinding", content_type, version)
}

fn grpc_web_operator_request<M: Message>(method: &'static str, request: M) -> Request<Body> {
    let mut request = grpc_operator_unary_request(
        request,
        method,
        "application/grpc-web+proto",
        Version::HTTP_11,
    );
    request.headers_mut().insert(
        header::ORIGIN,
        HeaderValue::from_static("https://operator.example"),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-grpc-web"),
        HeaderValue::from_static("1"),
    );
    request.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer operator-token"),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-sleepypods-operator"),
        HeaderValue::from_static("tenant-a"),
    );
    request
}

fn with_authorization(mut request: Request<Body>, value: &'static str) -> Request<Body> {
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, HeaderValue::from_static(value));
    request
}

fn grpc_web_preflight_request(method: &'static str) -> Request<Body> {
    Request::builder()
        .version(Version::HTTP_11)
        .method(Method::OPTIONS)
        .uri(operator_method_uri(method))
        .header(header::ORIGIN, "https://operator.example")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            "authorization,content-type,x-grpc-web,x-sleepypods-operator",
        )
        .body(Body::new(Full::new(Bytes::new())))
        .expect("preflight request builds")
}

fn grpc_operator_unary_request<M: Message>(
    request: M,
    method: &'static str,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_unary_request(request, operator_method_uri(method), content_type, version)
}

fn grpc_unary_request<M: Message>(
    request: M,
    uri: String,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");

    let mut frame = BytesMut::with_capacity(5 + message.len());
    frame.put_u8(0);
    frame.put_u32(message.len() as u32);
    frame.extend_from_slice(&message);

    Request::builder()
        .version(version)
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(frame.freeze())))
        .expect("request builds")
}

fn operator_method_uri(method: &'static str) -> String {
    format!("/{OPERATOR_SERVICE_NAME}/{method}")
}

fn decode_grpc_instance_response(bytes: &[u8]) -> Instance {
    decode_grpc_message(bytes)
}

fn decode_grpc_route_binding_response(bytes: &[u8]) -> RouteBinding {
    decode_grpc_message(bytes)
}

fn decode_grpc_message<M: Message + Default>(bytes: &[u8]) -> M {
    assert_eq!(bytes.first(), Some(&0), "gRPC message is uncompressed");
    let length = u32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .expect("gRPC response frame has a length prefix"),
    ) as usize;
    M::decode(&bytes[5..5 + length]).expect("gRPC response decodes")
}

fn assert_grpc_status(
    headers: &HeaderMap,
    trailers: Option<&HeaderMap>,
    body: &[u8],
    expected: &str,
) {
    let status =
        grpc_header_value(headers, trailers, body, "grpc-status").expect("gRPC status is returned");
    assert_eq!(status, expected);
}

fn grpc_header_value(
    headers: &HeaderMap,
    trailers: Option<&HeaderMap>,
    body: &[u8],
    name: &str,
) -> Option<String> {
    headers
        .get(name)
        .or_else(|| trailers.and_then(|trailers| trailers.get(name)))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .or_else(|| grpc_web_trailer_value(body, name))
}

async fn collect_grpc_response_parts<B>(
    response: http::Response<B>,
) -> (HeaderMap, Option<HeaderMap>, Bytes)
where
    B: tonic::codegen::Body<Data = Bytes>,
    B::Error: std::fmt::Debug,
{
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("response body should collect");
    let trailers = collected.trailers().cloned();
    (headers, trailers, collected.to_bytes())
}

fn grpc_web_trailer_value(bytes: &[u8], name: &str) -> Option<String> {
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let frame_type = bytes[offset];
        let length = u32::from_be_bytes(
            bytes[offset + 1..offset + 5]
                .try_into()
                .expect("gRPC-Web frame has a length prefix"),
        ) as usize;
        offset += 5;
        if offset + length > bytes.len() {
            return None;
        }

        if frame_type & 0x80 != 0 {
            let trailers = std::str::from_utf8(&bytes[offset..offset + length]).ok()?;
            for line in trailers.split("\r\n").filter(|line| !line.is_empty()) {
                let (key, value) = line.split_once(':')?;
                if key.eq_ignore_ascii_case(name) {
                    return Some(value.trim_start().to_owned());
                }
            }
        }
        offset += length;
    }

    None
}

fn http_identity(host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
            host: Some(RouteHost {
                kind: RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
            path_prefix: path_prefix.map(str::to_owned),
        })),
    }
}

fn sni_identity(host: &str) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Sni(SniRouteIdentity {
            host: Some(RouteHost {
                kind: RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
        })),
    }
}

/// A schema that requires each named value.
fn value_schema(fields: &[&str]) -> WorkloadValueSchema {
    WorkloadValueSchema {
        fields: fields
            .iter()
            .map(|field| {
                (
                    (*field).to_owned(),
                    WorkloadValueFieldRule {
                        required: true,
                        default_value: None,
                    },
                )
            })
            .collect(),
        allow_extra: false,
    }
}

/// The values `stateful_manifest_template_proto` reads.
fn stateful_value_schema() -> WorkloadValueSchema {
    value_schema(&["tenant", "volume"])
}

/// A store holding one class whose instances render with any values.
fn store_with_class(class_id: &str, version: u64) -> Arc<FakeInstanceStore> {
    let store = FakeInstanceStore::default();
    let mut class = support::workload_class();
    class.reference = control_plane::WorkloadClassVersionRef::new(
        control_plane::WorkloadClassId::new(class_id).expect("valid class ID"),
        Generation::new(version),
    );
    store.seed_workload_class(class);
    Arc::new(store)
}

fn stateful_manifest_template_proto() -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::StatefulSet as i32,
            name: Some(composed_text("db-", "tenant")),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "postgres".to_owned(),
                image: Some(literal_text("postgres:17")),
                ports: vec![ContainerPortTemplate {
                    name: Some("postgres".to_owned()),
                    container_port: 5432,
                }],
                env: vec![EnvVarTemplate {
                    name: "TENANT".to_owned(),
                    value: Some(instance_value_text("tenant")),
                }],
            }),
            termination_grace_period_seconds: None,
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text("sleepypods/sidecar:test")),
            listen_port: 15000,
            mode: None,
            app_protocol: Default::default(),
        }),
        service: Some(ServiceTemplate {
            name: Some(composed_text("db-", "tenant")),
            ports: vec![ServicePortTemplate {
                name: Some("postgres".to_owned()),
                port: 5432,
                target_port: 5432,
            }],
        }),
        volumes: vec![VolumeTemplate {
            name: "data".to_owned(),
            mount_path: Some(literal_text("/var/lib/postgresql/data")),
            pv_name: Some(composed_text("pv-", "tenant")),
            pvc_name: Some(composed_text("pvc-", "tenant")),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
            capacity: Some(literal_text("10Gi")),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
            storage_class_name: Some(literal_text("manual")),
            source: Some(PersistentVolumeSourceTemplate {
                kind: Some(persistent_volume_source_template::Kind::Csi(
                    CsiVolumeSourceTemplate {
                        driver: Some(literal_text("csi.example.com")),
                        volume_handle: Some(instance_value_text("volume")),
                        fs_type: Some(literal_text("ext4")),
                        read_only: false,
                        volume_attributes: [("tenant".to_owned(), instance_value_text("tenant"))]
                            .into(),
                        controller_publish_secret_ref: None,
                        node_stage_secret_ref: None,
                        node_publish_secret_ref: Some(CsiSecretRefTemplate {
                            name: Some(composed_text("secret-", "tenant")),
                            namespace: Some(literal_text("storage-secrets")),
                        }),
                        controller_expand_secret_ref: None,
                        node_expand_secret_ref: None,
                    },
                )),
            }),
        }],
        raw_objects: vec![],
    }
}

fn host_path_manifest_template_proto() -> ManifestTemplate {
    let mut template = stateful_manifest_template_proto();
    template.volumes[0].source = Some(PersistentVolumeSourceTemplate {
        kind: Some(persistent_volume_source_template::Kind::HostPath(
            HostPathVolumeSourceTemplate {
                path: Some(composed_text("/var/local/sleepypods/", "tenant")),
                r#type: Some(literal_text("DirectoryOrCreate")),
            },
        )),
    });
    template
}

fn sleep_policy_proto() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms: 300_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 30_000,
        idle_timeout_override: Some(IdleTimeoutOverridePolicy {
            value_field: "idle_ms".to_owned(),
            min_idle_timeout_ms: 60_000,
            max_idle_timeout_ms: 600_000,
        }),
    }
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}

fn instance_value_text(field: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::InstanceValue(field.to_owned())),
        }],
    }
}

fn composed_text(prefix: &str, field: &str) -> TemplateText {
    TemplateText {
        parts: vec![
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(prefix.to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue(field.to_owned())),
            },
        ],
    }
}

#[derive(Clone)]
struct MetadataCapturingOperatorApi {
    captured: Arc<Mutex<Vec<(String, String)>>>,
}

impl MetadataCapturingOperatorApi {
    fn new(captured: Arc<Mutex<Vec<(String, String)>>>) -> Self {
        Self { captured }
    }

    fn capture_metadata<T>(&self, request: &tonic::Request<T>) {
        let mut captured = self
            .captured
            .lock()
            .expect("captured metadata lock is available");
        for name in ["authorization", "x-sleepypods-operator"] {
            if let Some(value) = request
                .metadata()
                .get(name)
                .and_then(|value| value.to_str().ok())
            {
                captured.push((name.to_owned(), value.to_owned()));
            }
        }
    }
}

#[tonic::async_trait]
impl OperatorControlPlane for MetadataCapturingOperatorApi {
    async fn publish_certificate(
        &self,
        _: tonic::Request<control_plane::api::pb::PublishCertificateRequest>,
    ) -> Result<Response<control_plane::api::pb::CertificateMetadata>, Status> {
        panic!("unexpected certificate operation")
    }

    async fn get_certificate_metadata(
        &self,
        _: tonic::Request<control_plane::api::pb::GetCertificateMetadataRequest>,
    ) -> Result<Response<control_plane::api::pb::CertificateMetadata>, Status> {
        panic!("unexpected certificate operation")
    }

    async fn set_tls_binding(
        &self,
        _: tonic::Request<control_plane::api::pb::SetTlsBindingRequest>,
    ) -> Result<Response<control_plane::api::pb::TlsBinding>, Status> {
        panic!("unexpected certificate operation")
    }

    async fn get_tls_binding(
        &self,
        _: tonic::Request<control_plane::api::pb::GetTlsBindingRequest>,
    ) -> Result<Response<control_plane::api::pb::TlsBinding>, Status> {
        panic!("unexpected certificate operation")
    }

    async fn remove_certificate(
        &self,
        _: tonic::Request<control_plane::api::pb::RemoveCertificateRequest>,
    ) -> Result<Response<control_plane::api::pb::CertificateMetadata>, Status> {
        panic!("unexpected certificate operation")
    }

    async fn reencrypt_certificate(
        &self,
        _: tonic::Request<control_plane::api::pb::ReencryptCertificateRequest>,
    ) -> Result<Response<control_plane::api::pb::CertificateMetadata>, Status> {
        panic!("unexpected certificate operation")
    }

    async fn reencrypt_certificates(
        &self,
        _: tonic::Request<control_plane::api::pb::ReencryptCertificatesRequest>,
    ) -> Result<Response<control_plane::api::pb::ReencryptCertificatesResponse>, Status> {
        panic!("unexpected certificate operation")
    }

    async fn create_workload_class_version(
        &self,
        _request: tonic::Request<CreateWorkloadClassVersionRequest>,
    ) -> Result<Response<WorkloadClassVersion>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn get_workload_class_version(
        &self,
        _request: tonic::Request<GetWorkloadClassVersionRequest>,
    ) -> Result<Response<WorkloadClassVersion>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn create_instance(
        &self,
        request: tonic::Request<CreateInstanceRequest>,
    ) -> Result<Response<Instance>, Status> {
        self.capture_metadata(&request);

        Ok(Response::new(Instance::default()))
    }

    async fn get_instance(
        &self,
        _request: tonic::Request<GetInstanceRequest>,
    ) -> Result<Response<Instance>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn delete_instance(
        &self,
        _request: tonic::Request<DeleteInstanceRequest>,
    ) -> Result<Response<DeleteInstanceResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn create_route_binding(
        &self,
        _request: tonic::Request<CreateRouteBindingRequest>,
    ) -> Result<Response<RouteBinding>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn get_route_binding(
        &self,
        _request: tonic::Request<GetRouteBindingRequest>,
    ) -> Result<Response<RouteBinding>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn delete_route_binding(
        &self,
        _request: tonic::Request<DeleteRouteBindingRequest>,
    ) -> Result<Response<DeleteRouteBindingResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn put_http01_challenge(
        &self,
        _request: tonic::Request<PutHttp01ChallengeRequest>,
    ) -> Result<Response<Http01Challenge>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn delete_http01_challenge(
        &self,
        _request: tonic::Request<DeleteHttp01ChallengeRequest>,
    ) -> Result<Response<DeleteHttp01ChallengeResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn reconcile_materialization(
        &self,
        _request: tonic::Request<ReconcileMaterializationRequest>,
    ) -> Result<Response<ReconcileMaterializationResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn force_delete_materialization(
        &self,
        _request: tonic::Request<ForceDeleteMaterializationRequest>,
    ) -> Result<Response<control_plane::api::pb::ForceDeleteMaterializationResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn force_release_exclusivity_key(
        &self,
        _request: tonic::Request<ForceReleaseExclusivityKeyRequest>,
    ) -> Result<Response<control_plane::api::pb::ForceReleaseExclusivityKeyResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }
}

#[derive(Clone, Debug, Default)]
struct FakeKubernetesClient {
    deleted: Arc<Mutex<Vec<RenderedObjectRef>>>,
    fail_delete: Arc<Mutex<Option<RenderedObjectRef>>>,
    fail_inspect: Arc<Mutex<bool>>,
    readiness: Arc<Mutex<ProjectionReadinessInspection>>,
    live_objects: Arc<Mutex<BTreeMap<String, ProjectionObjectInspection>>>,
}

impl FakeKubernetesClient {
    fn seed_materialization(&self, materialization: &MaterializationRecord) {
        let mut live_objects = self
            .live_objects
            .lock()
            .expect("fake client lock is available");
        for object in &materialization.rendered_objects {
            live_objects.insert(
                object_key(object),
                ProjectionObjectInspection::Present(owned_metadata(materialization)),
            );
        }
    }

    fn seed_unowned_object(&self, object: RenderedObjectRef) {
        self.live_objects
            .lock()
            .expect("fake client lock is available")
            .insert(
                object_key(&object),
                ProjectionObjectInspection::Present(LiveObjectMetadata {
                    persistent_volume_reclaim_policy: Some("Retain".into()),
                    identity: control_plane::projection::LiveObjectIdentity {
                        uid: "test-uid".into(),
                        resource_version: "1".into(),
                    },
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                    deleting: false,
                    finalizers: Vec::new(),
                }),
            );
    }

    fn seed_deleting_owned_object(
        &self,
        materialization: &MaterializationRecord,
        object: RenderedObjectRef,
        finalizers: Vec<String>,
    ) {
        self.live_objects
            .lock()
            .expect("fake client lock is available")
            .insert(
                object_key(&object),
                ProjectionObjectInspection::Present(
                    owned_metadata(materialization).deleting(finalizers),
                ),
            );
    }

    fn fail_delete(&self, object: RenderedObjectRef) {
        *self
            .fail_delete
            .lock()
            .expect("fake client lock is available") = Some(object);
    }

    fn fail_inspect(&self) {
        *self
            .fail_inspect
            .lock()
            .expect("fake client lock is available") = true;
    }

    fn set_readiness(&self, readiness: ProjectionReadinessInspection) {
        *self
            .readiness
            .lock()
            .expect("fake client lock is available") = readiness;
    }

    fn deleted(&self) -> Vec<RenderedObjectRef> {
        self.deleted
            .lock()
            .expect("fake client lock is available")
            .clone()
    }
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn apply_object<'a>(
        &'a self,
        _object: &'a control_plane::KubernetesObject,
        _precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        _precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            if self
                .fail_delete
                .lock()
                .expect("fake client lock is available")
                .as_ref()
                == Some(object)
            {
                self.deleted
                    .lock()
                    .expect("fake client lock is available")
                    .push(object.clone());
                return Err(KubernetesClientError::new("delete failed"));
            }

            self.live_objects
                .lock()
                .expect("fake client lock is available")
                .remove(&object_key(object));
            self.deleted
                .lock()
                .expect("fake client lock is available")
                .push(object.clone());
            Ok(())
        })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async {
            BackendEndpoint::new("http://example").map_err(|error| {
                KubernetesClientError::new(format!("invalid backend endpoint: {error}"))
            })
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            if *self
                .fail_inspect
                .lock()
                .expect("fake client lock is available")
            {
                return Err(KubernetesClientError::new("inspect failed"));
            }
            Ok(self
                .live_objects
                .lock()
                .expect("fake client lock is available")
                .get(&object_key(object))
                .cloned()
                .unwrap_or(ProjectionObjectInspection::Missing))
        })
    }

    fn inspect_readiness<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>> {
        Box::pin(async move {
            Ok(self
                .readiness
                .lock()
                .expect("fake client lock is available")
                .clone())
        })
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        _instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
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
