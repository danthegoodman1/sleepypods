#[macro_use]
#[path = "support/unexpected_store.rs"]
mod unexpected_store;
mod support;
use support::TestStore as FakeSidecarStore;

#[tokio::test]
async fn known_activation_hold_defers_before_kubernetes_membership_calls() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "activation-advisory",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("activation-advisory", 7));
    store.set_ready_age(std::time::Duration::from_secs(1));
    let kube = FakeKubernetesClient::default();
    *kube.membership_error.lock().unwrap() =
        Some("unexpected Kubernetes membership call during known hold".into());
    let service = sidecar_api(store.clone(), kube);
    let status = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".into(),
            instance_id: "activation-advisory".into(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .unwrap_err();
    assert!(
        status.message().starts_with("automatic sleep deferred"),
        "{status}"
    );
    assert_eq!(
        status
            .metadata()
            .get(sleepypods_api::IDLE_RETRY_AFTER_METADATA)
            .unwrap(),
        "189000"
    );
    assert!(
        store.sleep_requests().is_empty(),
        "advisory check avoids both Kube and the final mutation attempt"
    );
}

#[tokio::test]
async fn report_idle_supplies_atomic_floor_and_maps_expected_deferral_with_retry_hint() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "activation-pending",
        DomainInstanceState::Running,
        7,
    ));
    let materialization = ready_materialization("activation-pending", 7);
    store.seed_materialization(materialization.clone());
    store.defer_sleep_for(std::time::Duration::from_secs(185));
    let kube = FakeKubernetesClient::default();
    kube.seed_materialization(&materialization);
    let service = sidecar_api(store.clone(), kube);
    let status = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".into(),
            instance_id: "activation-pending".into(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        status
            .metadata()
            .get(sleepypods_api::IDLE_RETRY_AFTER_METADATA)
            .unwrap(),
        "185000"
    );
    assert_eq!(store.instance().state, DomainInstanceState::Running);
    assert_eq!(
        store.sleep_requests()[0].minimum_ready_age,
        Some(sleepypods_api::INITIAL_ACTIVATION_TIMEOUT)
    );
}
use support::{workload_class as fake_workload_class, FakeStoreError};

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bytes::{BufMut, Bytes, BytesMut};
use control_plane::api::{
    pb::{
        sidecar_control_plane_server::SidecarControlPlane, sidecar_report_idle_response,
        SidecarReportIdleRequest, SidecarReportIdleResponse, SidecarReportIdleUnavailableReason,
    },
    sidecar_grpc_service_with_store, StoreBackedSidecarApi, StoreBackedSidecarGrpcService,
    SIDECAR_SERVICE_NAME,
};
use control_plane::projection::{
    LiveObjectMetadata, ProjectionObjectInspection, ANNOTATION_MATERIALIZATION_ID,
    LABEL_MANAGED_BY, LABEL_MANAGED_BY_VALUE,
};
use control_plane::{
    AuthConfig, BackendEndpoint, CallerRole, ControlPlaneAuth, ControlPlaneStore, Generation,
    InstanceId, InstanceRecord, InstanceState as DomainInstanceState, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient,
    MaterializationId, MaterializationRecord, MaterializationState, MaterializationTarget,
    RenderedObjectRef, StateTransitionReason, StaticBearerTokens,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{header, HeaderMap, HeaderValue, Request, Version};
use tonic::server::NamedService;
use tonic::Code;
use tower::ServiceExt;

fn auth_config() -> AuthConfig {
    AuthConfig::static_bearer_tokens(
        StaticBearerTokens::new("operator-token", "proxy-token", "sidecar-token")
            .expect("auth tokens are valid"),
    )
}

fn control_plane_auth() -> ControlPlaneAuth {
    ControlPlaneAuth::from_config(auth_config(), Default::default())
}

fn authenticated_sidecar_service(
    store: Arc<dyn ControlPlaneStore>,
    client: FakeKubernetesClient,
) -> tonic::service::interceptor::InterceptedService<
    StoreBackedSidecarGrpcService<FakeKubernetesClient>,
    control_plane::ControlPlaneAuthInterceptor,
> {
    tonic::service::interceptor::InterceptedService::new(
        sidecar_grpc_service_with_store(store, KubernetesMaterializer::new(client), target()),
        control_plane_auth().interceptor(SIDECAR_SERVICE_NAME, CallerRole::Sidecar),
    )
}

#[test]
fn generated_api_contains_sidecar_report_idle_shape_without_operator_surface_change() {
    let request = SidecarReportIdleRequest {
        pod_uid: "test-pod".to_owned(),
        instance_id: "instance-1".to_owned(),
        expected_generation: 7,
        active_count: 0,
    };
    let response = SidecarReportIdleResponse {
        outcome: Some(sidecar_report_idle_response::Outcome::Accepted(
            control_plane::api::pb::SidecarReportIdleAccepted {
                instance_id: request.instance_id.clone(),
                instance_generation: request.expected_generation + 1,
            },
        )),
    };

    assert_eq!(request.instance_id, "instance-1");
    assert_eq!(request.active_count, 0);
    assert!(matches!(
        response.outcome,
        Some(sidecar_report_idle_response::Outcome::Accepted(_))
    ));
    assert_eq!(
        <StoreBackedSidecarGrpcService<FakeKubernetesClient> as NamedService>::NAME,
        SIDECAR_SERVICE_NAME
    );
}

#[tokio::test]
async fn report_idle_legacy_identity_or_unsupported_membership_never_starts_sleep() {
    for reason in [
        "legacy",
        "replicas",
        "membership",
        "stale_uid",
        "missing_materialization",
        "unready_materialization",
        "stale_materialization",
    ] {
        let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
        store.seed_instance(domain_instance(
            "instance-running",
            DomainInstanceState::Running,
            7,
        ));
        if reason != "missing_materialization" {
            let mut materialization = ready_materialization("instance-running", 7);
            if reason == "unready_materialization" {
                materialization.state = MaterializationState::Pending;
            }
            if reason == "stale_materialization" {
                materialization.instance_generation = Generation::new(6);
            }
            store.seed_materialization(materialization);
        }
        let client = FakeKubernetesClient::default();
        if reason == "replicas" {
            store.set_replicas(Some(2));
        }
        if reason == "membership" {
            *client.membership_error.lock().unwrap() = Some("observed two pods".to_owned());
        }
        let service = sidecar_api(store.clone(), client.clone());
        let error = service
            .report_idle(tonic::Request::new(SidecarReportIdleRequest {
                instance_id: "instance-running".to_owned(),
                expected_generation: 7,
                active_count: 0,
                pod_uid: match reason {
                    "legacy" => "",
                    "stale_uid" => "deleted-pod",
                    _ => "test-pod",
                }
                .to_owned(),
            }))
            .await
            .expect_err("unsupported report must fail closed");
        assert_eq!(error.code(), Code::FailedPrecondition, "{reason}");
        assert_eq!(
            store.instance().state,
            DomainInstanceState::Running,
            "{reason}"
        );
        assert!(store.transition_requests().is_empty());
        assert!(client.deleted_objects().is_empty());
    }
}

#[tokio::test]
async fn report_idle_running_instance_transitions_to_draining() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-running",
        DomainInstanceState::Running,
        7,
    ));
    let materialization = ready_materialization("instance-running", 7);
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    let service = sidecar_api(store.clone(), client.clone());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-running".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .expect("idle report succeeds")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_id, "instance-running");
    assert_eq!(accepted.instance_generation, 8);

    let instance = store.instance();
    assert_eq!(instance.state, DomainInstanceState::Draining);
    assert_eq!(instance.generation, Generation::new(8));
    let transitions = store.transition_requests();
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0].expected_generation, Generation::new(7));
    assert_eq!(transitions[0].next_state, DomainInstanceState::Draining);
    assert_eq!(transitions[0].reason, StateTransitionReason::IdleReported);
    assert_eq!(client.deleted_objects(), Vec::<RenderedObjectRef>::new());
    let materialization = store
        .materialization()
        .expect("materialization remains recorded");
    assert_eq!(materialization.state, MaterializationState::Deleting);
    assert_eq!(
        materialization.rendered_objects,
        vec![object_ref(
            "apps/v1",
            "Deployment",
            "apps",
            "instance-running"
        )]
    );
}

#[tokio::test]
async fn report_idle_uses_exact_persisted_projection_independent_of_cas_revision() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-rendered-generation",
        DomainInstanceState::Running,
        8,
    ));
    let mut materialization = ready_materialization("instance-rendered-generation", 8);
    materialization.projection_generation = Generation::new(7);
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    let service = sidecar_api(store.clone(), client.clone());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-rendered-generation".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .expect("idle report from rendered wake generation succeeds")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_id, "instance-rendered-generation");
    assert_eq!(accepted.instance_generation, 9);
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
    assert_eq!(store.instance().generation, Generation::new(9));
    let transitions = store.transition_requests();
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0].expected_generation, Generation::new(8));
    assert_eq!(transitions[0].next_state, DomainInstanceState::Draining);
    assert_eq!(client.deleted_objects(), Vec::<RenderedObjectRef>::new());
}

#[tokio::test]
async fn report_idle_never_infers_an_older_projection_from_revision_arithmetic() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "exact-projection",
        DomainInstanceState::Running,
        8,
    ));
    store.seed_materialization(ready_materialization("exact-projection", 8));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());
    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "exact-projection".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let conflict = expect_generation_conflict(response);
    assert_eq!(conflict.actual_generation, 8);
    assert!(store.transition_requests().is_empty());
}

#[tokio::test]
async fn report_idle_untrusted_maximum_generation_does_not_overflow_drain_replay() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "idle-overflow",
        DomainInstanceState::Draining,
        8,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());
    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "idle-overflow".to_owned(),
            expected_generation: u64::MAX,
            active_count: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(expect_generation_conflict(response).actual_generation, 8);
    assert!(store.transition_requests().is_empty());
}

#[tokio::test]
async fn report_idle_restart_during_sleep_resumes_cleanup_from_store_state() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-delete-retry",
        DomainInstanceState::Running,
        7,
    ));
    let materialization = ready_materialization("instance-delete-retry", 7);
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::failing_deletes(1);
    client.seed_materialization(&materialization);
    let service = sidecar_api(store.clone(), client.clone());
    let request = SidecarReportIdleRequest {
        pod_uid: "test-pod".to_owned(),
        instance_id: "instance-delete-retry".to_owned(),
        expected_generation: 7,
        active_count: 0,
    };

    let response = service
        .report_idle(tonic::Request::new(request.clone()))
        .await
        .expect("idle report does not delete Kubernetes objects")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_id, "instance-delete-retry");
    assert_eq!(accepted.instance_generation, 8);
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
    assert_eq!(store.instance().generation, Generation::new(8));
    let materialization = store
        .materialization()
        .expect("materialization remains after idle report");
    assert_eq!(materialization.state, MaterializationState::Deleting);
    assert_eq!(
        materialization.rendered_objects,
        vec![object_ref(
            "apps/v1",
            "Deployment",
            "apps",
            "instance-delete-retry"
        )]
    );
    assert_eq!(client.deleted_objects(), Vec::<RenderedObjectRef>::new());
}

#[tokio::test]
async fn report_idle_unowned_live_ref_blocks_cleanup_without_delete() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-unowned",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("instance-unowned", 7));
    let client = FakeKubernetesClient::default();
    client.seed_unowned_object(object_ref(
        "apps/v1",
        "Deployment",
        "apps",
        "instance-unowned",
    ));
    let service = sidecar_api(store.clone(), client.clone());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-unowned".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .expect("idle report does not inspect Kubernetes refs")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_id, "instance-unowned");
    assert_eq!(accepted.instance_generation, 8);
    assert_eq!(client.deleted_objects(), Vec::<RenderedObjectRef>::new());
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
    let materialization = store
        .materialization()
        .expect("materialization remains after idle report");
    assert_eq!(materialization.state, MaterializationState::Deleting);
}

#[tokio::test]
async fn report_idle_nonzero_active_count_returns_failed_precondition_without_mutation() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-active",
        DomainInstanceState::Running,
        7,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let error = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-active".to_owned(),
            expected_generation: 7,
            active_count: 1,
        }))
        .await
        .expect_err("active requests prevent idle report");

    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("active_count 0"));
    assert!(error.message().contains("found 1"));
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Running);
    assert_eq!(store.instance().generation, Generation::new(7));
}

#[tokio::test]
async fn report_idle_duplicate_at_next_draining_generation_is_idempotent() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-draining",
        DomainInstanceState::Draining,
        8,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-draining".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .expect("duplicate idle report is a domain response")
        .into_inner();

    let already = expect_already_draining(response);
    assert_eq!(already.instance_id, "instance-draining");
    assert_eq!(already.instance_generation, 8);
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
    assert_eq!(store.instance().generation, Generation::new(8));
}

#[tokio::test]
async fn report_idle_stale_generation_conflict_is_structured_response() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-conflict",
        DomainInstanceState::Draining,
        8,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-conflict".to_owned(),
            expected_generation: 6,
            active_count: 0,
        }))
        .await
        .expect("generation conflict is a domain response")
        .into_inner();

    let conflict = expect_generation_conflict(response);
    assert_eq!(conflict.instance_id, "instance-conflict");
    assert_eq!(conflict.expected_generation, 6);
    assert_eq!(conflict.actual_generation, 8);
    assert_eq!(store.transition_requests().len(), 0);
}

#[tokio::test]
async fn report_idle_non_running_matching_generation_returns_unavailable() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-waking",
        DomainInstanceState::Waking,
        4,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-waking".to_owned(),
            expected_generation: 4,
            active_count: 0,
        }))
        .await
        .expect("unavailable state is a domain response")
        .into_inner();

    let unavailable = expect_unavailable(response);
    assert_eq!(unavailable.instance_id, "instance-waking");
    assert_eq!(unavailable.instance_generation, 4);
    assert_eq!(
        unavailable.reason,
        SidecarReportIdleUnavailableReason::Waking as i32
    );
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Waking);
}

#[tokio::test]
async fn report_idle_invalid_instance_id_returns_invalid_argument() {
    let service = sidecar_api(
        Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class())),
        FakeKubernetesClient::default(),
    );

    let error = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "".to_owned(),
            expected_generation: 1,
            active_count: 0,
        }))
        .await
        .expect_err("empty instance id is invalid");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("must not be empty"));
}

#[tokio::test]
async fn report_idle_store_unavailable_returns_grpc_unavailable() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.fail_get_with(FakeStoreError::Unavailable("database is down"));
    let service = sidecar_api(store, FakeKubernetesClient::default());

    let error = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-store-error".to_owned(),
            expected_generation: 1,
            active_count: 0,
        }))
        .await
        .expect_err("store unavailability is a transport error");

    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(error.message(), "control-plane store is unavailable");
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_sidecar_report_idle() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-transport",
        DomainInstanceState::Running,
        2,
    ));
    store.seed_materialization(ready_materialization("instance-transport", 2));

    let response = sidecar_grpc_service_with_store(
        store.clone(),
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
    )
    .oneshot(grpc_sidecar_report_idle_request(
        SidecarReportIdleRequest {
            pod_uid: "test-pod".to_owned(),
            instance_id: "instance-transport".to_owned(),
            expected_generation: 2,
            active_count: 0,
        },
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("native gRPC request should route through sidecar service");
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
    let accepted = expect_accepted(decode_grpc_sidecar_report_idle_response(
        collected.to_bytes().as_ref(),
    ));
    assert_eq!(accepted.instance_id, "instance-transport");
    assert_eq!(accepted.instance_generation, 3);
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
}

#[tokio::test]
async fn native_grpc_sidecar_auth_rejects_report_idle_before_sleep_logic() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-auth-idle",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("instance-auth-idle", 7));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let missing_status = collect_grpc_status(
        authenticated_sidecar_service(
            Arc::clone(&store_for_service),
            FakeKubernetesClient::default(),
        )
        .oneshot(grpc_sidecar_report_idle_request(
            SidecarReportIdleRequest {
                pod_uid: "test-pod".to_owned(),
                instance_id: "instance-auth-idle".to_owned(),
                expected_generation: 7,
                active_count: 0,
            },
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("missing auth returns gRPC response"),
    )
    .await;
    assert_eq!(missing_status, "16");
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Running);

    let wrong_role_status = collect_grpc_status(
        authenticated_sidecar_service(store_for_service, FakeKubernetesClient::default())
            .oneshot(with_authorization(
                grpc_sidecar_report_idle_request(
                    SidecarReportIdleRequest {
                        pod_uid: "test-pod".to_owned(),
                        instance_id: "instance-auth-idle".to_owned(),
                        expected_generation: 7,
                        active_count: 0,
                    },
                    "application/grpc",
                    Version::HTTP_2,
                ),
                "Bearer proxy-token",
            ))
            .await
            .expect("wrong role returns gRPC response"),
    )
    .await;
    assert_eq!(wrong_role_status, "7");
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Running);
}

#[tokio::test]
async fn native_grpc_sidecar_auth_accepts_valid_report_idle_credentials() {
    let store = Arc::new(FakeSidecarStore::with_workload_class(fake_workload_class()));
    store.seed_instance(domain_instance(
        "instance-auth-valid-idle",
        DomainInstanceState::Running,
        7,
    ));
    let materialization = ready_materialization("instance-auth-valid-idle", 7);
    store.seed_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    client.seed_materialization(&materialization);
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let response = authenticated_sidecar_service(store_for_service, client)
        .oneshot(with_authorization(
            grpc_sidecar_report_idle_request(
                SidecarReportIdleRequest {
                    pod_uid: "test-pod".to_owned(),
                    instance_id: "instance-auth-valid-idle".to_owned(),
                    expected_generation: 7,
                    active_count: 0,
                },
                "application/grpc",
                Version::HTTP_2,
            ),
            "Bearer sidecar-token",
        ))
        .await
        .expect("valid sidecar credentials dispatch report idle");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    assert_eq!(grpc_status(&headers, trailers.as_ref()), "0");
    assert_eq!(store.transition_requests().len(), 1);
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
}

fn grpc_sidecar_report_idle_request(
    request: SidecarReportIdleRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    let mut body = BytesMut::new();
    encode_grpc_message(request, &mut body);

    Request::builder()
        .version(version)
        .method("POST")
        .uri("/sleepypods.controlplane.v1.SidecarControlPlane/ReportIdle")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(body.freeze())))
        .expect("request builds")
}

fn with_authorization(mut request: Request<Body>, value: &'static str) -> Request<Body> {
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, HeaderValue::from_static(value));
    request
}

fn encode_grpc_message<M: Message>(request: M, body: &mut BytesMut) {
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");

    body.put_u8(0);
    body.put_u32(message.len() as u32);
    body.extend_from_slice(&message);
}

fn decode_grpc_sidecar_report_idle_response(bytes: &[u8]) -> SidecarReportIdleResponse {
    assert_eq!(bytes.first(), Some(&0), "gRPC message is uncompressed");
    let length = u32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .expect("gRPC response frame has a length prefix"),
    ) as usize;
    SidecarReportIdleResponse::decode(&bytes[5..5 + length]).expect("gRPC response decodes")
}

async fn collect_grpc_status<B>(response: http::Response<B>) -> String
where
    B: tonic::codegen::Body<Data = Bytes>,
    B::Error: std::fmt::Debug,
{
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    grpc_status(&headers, trailers.as_ref())
}

fn grpc_status(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> String {
    trailers
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("gRPC status is returned")
        .to_str()
        .expect("gRPC status is valid")
        .to_owned()
}

fn sidecar_api(
    store: Arc<FakeSidecarStore>,
    client: FakeKubernetesClient,
) -> StoreBackedSidecarApi<FakeKubernetesClient> {
    StoreBackedSidecarApi::new(store, KubernetesMaterializer::new(client), target())
}

fn expect_accepted(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleAccepted {
    let Some(sidecar_report_idle_response::Outcome::Accepted(accepted)) = response.outcome else {
        panic!("expected accepted response");
    };
    accepted
}

fn expect_already_draining(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleAlreadyDraining {
    let Some(sidecar_report_idle_response::Outcome::AlreadyDraining(already_draining)) =
        response.outcome
    else {
        panic!("expected already-draining response");
    };
    already_draining
}

fn expect_generation_conflict(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleGenerationConflict {
    let Some(sidecar_report_idle_response::Outcome::GenerationConflict(conflict)) =
        response.outcome
    else {
        panic!("expected generation-conflict response");
    };
    conflict
}

fn expect_unavailable(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleUnavailable {
    let Some(sidecar_report_idle_response::Outcome::Unavailable(unavailable)) = response.outcome
    else {
        panic!("expected unavailable response");
    };
    unavailable
}

#[derive(Clone, Debug, Default)]
struct FakeKubernetesClient {
    deleted_objects: Arc<Mutex<Vec<RenderedObjectRef>>>,
    failing_deletes: Arc<Mutex<usize>>,
    live_objects: Arc<Mutex<BTreeMap<String, ProjectionObjectInspection>>>,
    membership_error: Arc<Mutex<Option<String>>>,
}

impl FakeKubernetesClient {
    fn failing_deletes(count: usize) -> Self {
        Self {
            deleted_objects: Arc::new(Mutex::new(Vec::new())),
            failing_deletes: Arc::new(Mutex::new(count)),
            live_objects: Arc::new(Mutex::new(BTreeMap::new())),
            membership_error: Arc::new(Mutex::new(None)),
        }
    }

    fn seed_materialization(&self, materialization: &MaterializationRecord) {
        let mut live_objects = self
            .live_objects
            .lock()
            .expect("fake kubernetes lock is available");
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
            .expect("fake kubernetes lock is available")
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

    fn deleted_objects(&self) -> Vec<RenderedObjectRef> {
        self.deleted_objects
            .lock()
            .expect("fake kubernetes lock is available")
            .clone()
    }
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn verify_idle_member<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        identity: &'a control_plane::materializer::IdleMemberIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            if let Some(error) = self.membership_error.lock().unwrap().as_ref() {
                return Err(control_plane::KubernetesClientError::new(error));
            }
            if identity.pod_uid != "test-pod" {
                return Err(control_plane::KubernetesClientError::new("unknown pod UID"));
            }
            Ok(())
        })
    }

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
            let mut failing_deletes = self
                .failing_deletes
                .lock()
                .expect("fake kubernetes lock is available");
            if *failing_deletes > 0 {
                *failing_deletes -= 1;
                return Err(control_plane::KubernetesClientError::new(
                    "transient delete failure",
                ));
            }
            drop(failing_deletes);

            self.live_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .remove(&object_key(object));
            self.deleted_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .push(object.clone());
            Ok(())
        })
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async {
            BackendEndpoint::new("http://example")
                .map_err(|error| control_plane::KubernetesClientError::new(error.to_string()))
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            Ok(self
                .live_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .get(&object_key(object))
                .cloned()
                .unwrap_or(ProjectionObjectInspection::Missing))
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

fn domain_instance(id: &str, state: DomainInstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id.to_owned()).expect("instance id is valid"),
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
    MaterializationRecord {
        id: MaterializationId::new(format!("{}:cluster-a:apps", instance_id))
            .expect("materialization id is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance id is valid"),
        instance_generation: Generation::new(generation),
        projection_generation: Generation::new(generation),
        target: target(),
        state: MaterializationState::Ready,
        backend: Some(BackendEndpoint::new("http://example").expect("backend is valid")),
        backend_generation: control_plane::BackendGeneration::new(generation),
        rendered_objects: vec![object_ref("apps/v1", "Deployment", "apps", instance_id)],
        exclusivity_keys: vec![],
        reconciliation_lease: None,
    }
}

fn target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("target is valid")
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
                materialization.projection_generation.to_string(),
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
