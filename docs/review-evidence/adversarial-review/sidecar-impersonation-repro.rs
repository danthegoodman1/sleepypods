//! Regression coverage for the adversarial review's F1.
//!
//! A sidecar's credential names exactly one instance, and `ReportIdle` takes the
//! instance from that credential rather than from the request. A compromised
//! workload that reads its own credential can therefore only put its own
//! instance to sleep, and there is no request field left for it to point at
//! somebody else.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bytes::{BufMut, BytesMut};
use control_plane::api::pb::{
    sidecar_report_idle_response, SidecarReportIdleRequest, SidecarReportIdleResponse,
};
use control_plane::api::{sidecar_grpc_service_with_store, SIDECAR_SERVICE_NAME};
use control_plane::ids::WorkloadClassId;
use control_plane::manifest::{
    ContainerTemplate, ManifestTemplate, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
    TemplateText, WorkloadKind, WorkloadTemplate,
};
use control_plane::{
    AuthConfig, BackendEndpoint, BeginSleepRequest, BeginSleepResult, CallerRole, ControlPlaneAuth,
    ControlPlaneStore, Generation, GetInstanceRequest, InstanceId, InstanceRecord, InstanceState,
    KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
    KubernetesMaterializerClient, LoadWorkloadClassVersionRequest, MaterializationTarget,
    RenderedObjectRef, SidecarTokenSigningKey, SidecarTokenVerifier, StoreError, StoreFuture,
    StoreResult, WorkloadClassVersion, WorkloadClassVersionRef, WorkloadSleepPolicy,
    WorkloadValueSchema,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{header, HeaderValue, Request, Version};
use tower::ServiceExt;

/// The original attack: hold a sidecar credential, name someone else's instance.
/// There is no longer a field to name them with, and the attacker's own
/// credential resolves only to the attacker's instance.
#[tokio::test]
async fn a_sidecar_credential_cannot_reach_another_tenants_instance() {
    let store = Arc::new(FakeStore::with_instance(running("victim-tenant", 7)));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let response = authenticated_sidecar_service(store_for_service)
        .oneshot(with_authorization(
            report_idle_request(SidecarReportIdleRequest {
                expected_generation: 7,
                active_count: 0,
            }),
            // The attacker presents the credential minted into its own pod.
            &bearer(&signing_key().mint(&instance_id("attacker-tenant"))),
        ))
        .await
        .expect("the request dispatches");

    // The credential resolves to "attacker-tenant", so the victim is never the
    // subject of the call and is left untouched.
    let collected = response.into_body().collect().await.expect("body collects");
    let decoded = decode_response(collected.to_bytes().as_ref());
    assert!(
        !matches!(
            decoded.outcome,
            Some(sidecar_report_idle_response::Outcome::Accepted(_))
        ),
        "the victim must not be accepted for sleep"
    );
    assert_eq!(store.instance().state, InstanceState::Running);
    assert_eq!(store.instance().generation, Generation::new(7));
}

/// A sidecar can still put its own instance to sleep, which is its whole job.
#[tokio::test]
async fn a_sidecar_credential_still_sleeps_its_own_instance() {
    let store = Arc::new(FakeStore::with_instance(running("victim-tenant", 7)));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let response = authenticated_sidecar_service(store_for_service)
        .oneshot(with_authorization(
            report_idle_request(SidecarReportIdleRequest {
                expected_generation: 7,
                active_count: 0,
            }),
            &bearer(&signing_key().mint(&instance_id("victim-tenant"))),
        ))
        .await
        .expect("the request dispatches");

    let collected = response.into_body().collect().await.expect("body collects");
    let decoded = decode_response(collected.to_bytes().as_ref());
    let Some(sidecar_report_idle_response::Outcome::Accepted(accepted)) = decoded.outcome else {
        panic!("a sidecar reporting its own instance idle is accepted");
    };
    assert_eq!(accepted.instance_id, "victim-tenant");
    assert_eq!(store.instance().state, InstanceState::Draining);
}

/// A credential minted under a different key is not a credential at all, so an
/// attacker cannot forge one naming an instance it does not own — and the
/// rejection costs no database work.
#[tokio::test]
async fn a_forged_credential_is_rejected_before_any_lookup() {
    let store = Arc::new(FakeStore::with_instance(running("victim-tenant", 7)));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();
    let forged = SidecarTokenSigningKey::new(b"attacker-key".to_vec()).expect("valid key");

    let response = authenticated_sidecar_service(store_for_service)
        .oneshot(with_authorization(
            report_idle_request(SidecarReportIdleRequest {
                expected_generation: 7,
                active_count: 0,
            }),
            &bearer(&forged.mint(&instance_id("victim-tenant"))),
        ))
        .await
        .expect("the request dispatches");

    let headers = response.headers().clone();
    let collected = response.into_body().collect().await.expect("body collects");
    let trailers = collected.trailers().cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("gRPC status is returned");

    // 16 is UNAUTHENTICATED.
    assert_eq!(status, "16");
    assert_eq!(store.instance().state, InstanceState::Running);
    assert_eq!(
        store.get_instance_calls(),
        0,
        "a forged credential is rejected without touching the store"
    );
}

// --------------------------------------------------------------------------
// Harness
// --------------------------------------------------------------------------

type AuthenticatedSidecarService = tonic::service::interceptor::InterceptedService<
    control_plane::api::StoreBackedSidecarGrpcService<NoopKubernetesClient>,
    control_plane::ControlPlaneAuthInterceptor,
>;

fn signing_key() -> SidecarTokenSigningKey {
    SidecarTokenSigningKey::new(b"control-plane-signing-key".to_vec()).expect("valid signing key")
}

fn authenticated_sidecar_service(store: Arc<dyn ControlPlaneStore>) -> AuthenticatedSidecarService {
    let auth = ControlPlaneAuth::from_config_with_sidecar_tokens(
        AuthConfig::static_bearer_tokens(
            control_plane::StaticBearerTokens::new("operator-token", "proxy-token")
                .expect("auth tokens are valid"),
        ),
        SidecarTokenVerifier::new(signing_key()),
        Default::default(),
    );

    tonic::service::interceptor::InterceptedService::new(
        sidecar_grpc_service_with_store(
            store,
            KubernetesMaterializer::new(NoopKubernetesClient),
            target(),
        ),
        auth.interceptor(SIDECAR_SERVICE_NAME, CallerRole::Sidecar),
    )
}

fn bearer(credential: &str) -> String {
    format!("Bearer {credential}")
}

fn report_idle_request(request: SidecarReportIdleRequest) -> Request<Body> {
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");
    let mut body = BytesMut::new();
    body.put_u8(0);
    body.put_u32(message.len() as u32);
    body.extend_from_slice(&message);

    Request::builder()
        .version(Version::HTTP_2)
        .method("POST")
        .uri("/sleepypods.controlplane.v1.SidecarControlPlane/ReportIdle")
        .header(header::CONTENT_TYPE, "application/grpc")
        .body(Body::new(Full::new(body.freeze())))
        .expect("request builds")
}

fn with_authorization(mut request: Request<Body>, value: &str) -> Request<Body> {
    request.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(value).expect("ascii credential"),
    );
    request
}

fn decode_response(bytes: &[u8]) -> SidecarReportIdleResponse {
    if bytes.len() < 5 {
        return SidecarReportIdleResponse { outcome: None };
    }
    let length = u32::from_be_bytes(bytes[1..5].try_into().expect("length prefix")) as usize;
    SidecarReportIdleResponse::decode(&bytes[5..5 + length]).expect("response decodes")
}

fn target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("valid target")
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("valid instance ID")
}

fn workload_class_ref() -> WorkloadClassVersionRef {
    WorkloadClassVersionRef::new(
        WorkloadClassId::new("web").expect("valid class ID"),
        Generation::new(1),
    )
}

fn running(id: &str, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: instance_id(id),
        workload_class: workload_class_ref(),
        values: BTreeMap::new(),
        state: InstanceState::Running,
        generation: Generation::new(generation),
    }
}

#[derive(Debug)]
struct FakeStore {
    instance: Mutex<InstanceRecord>,
    get_instance_calls: Mutex<usize>,
}

impl FakeStore {
    fn with_instance(instance: InstanceRecord) -> Self {
        Self {
            instance: Mutex::new(instance),
            get_instance_calls: Mutex::new(0),
        }
    }

    fn instance(&self) -> InstanceRecord {
        self.instance.lock().expect("lock").clone()
    }

    fn get_instance_calls(&self) -> usize {
        *self.get_instance_calls.lock().expect("lock")
    }
}

impl ControlPlaneStore for FakeStore {
    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move {
            *self.get_instance_calls.lock().expect("lock") += 1;
            let instance = self.instance();
            Ok((instance.id == request.instance_id).then_some(instance))
        })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        _request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        Box::pin(async {
            Ok(Some(WorkloadClassVersion {
                reference: workload_class_ref(),
                template_generation: Generation::new(1),
                template: manifest_template(),
                default_values: BTreeMap::new(),
                value_schema: WorkloadValueSchema::new(true),
                sleep_policy: WorkloadSleepPolicy {
                    idle_timeout_ms: 120_000,
                    idle_retry_backoff_ms: 5_000,
                    drain_grace_timeout_ms: 30_000,
                    idle_timeout_override: None,
                },
                exclusivity_keys: Vec::new(),
            }))
        })
    }

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        Box::pin(async move {
            let mut instance = self.instance.lock().expect("lock");
            if instance.id != request.instance_id {
                return Err(StoreError::NotFound {
                    resource: "instance",
                });
            }
            if instance.generation != request.expected_running_generation {
                return Err(StoreError::GenerationConflict {
                    expected: request.expected_running_generation,
                    actual: instance.generation,
                });
            }
            instance.state = InstanceState::Draining;
            instance.generation = instance.generation.next();
            Ok(BeginSleepResult {
                instance: instance.clone(),
                materialization: None,
            })
        })
    }

    fn list_route_bindings_for_instance<'a>(
        &'a self,
        _request: control_plane::ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<control_plane::RouteBindingRecord>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn manifest_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: TemplateText::literal("app"),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("example/app:1"),
                ports: Vec::new(),
                env: Vec::new(),
            },
        },
        service: Some(ServiceTemplate {
            name: TemplateText::literal("svc"),
            ports: vec![ServicePortTemplate {
                name: None,
                port: 80,
                target_port: 8080,
            }],
        }),
        sidecar: SidecarTemplate {
            name: "sleepypods".to_owned(),
            image: TemplateText::literal("example/sidecar:1"),
            listen_port: 15000,
            mode: None,
        },
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

#[derive(Clone, Debug)]
struct NoopKubernetesClient;

impl KubernetesMaterializerClient for NoopKubernetesClient {
    fn apply_object<'a>(
        &'a self,
        _object: &'a control_plane::manifest::KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn delete_object<'a>(
        &'a self,
        _object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
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
}
