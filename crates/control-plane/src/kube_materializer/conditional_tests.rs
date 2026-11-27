use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::{json, Value};
use tower::service_fn;

use super::{KubeMaterializerClient, KubeMaterializerClientConfig};
use crate::{
    ids::{BackendGeneration, Generation, InstanceId, MaterializationId, WorkloadClassId},
    instance::{InstanceRecord, InstanceState, InstanceValues},
    manifest::{
        render_manifests, ApplyOrder, ContainerPortTemplate, ContainerTemplate, KubernetesObject,
        ManifestTemplate, ObjectMeta, RawKubernetesObject, RenderManifestRequest, RenderedManifest,
        RenderedManifestObject, Service, ServicePort, ServicePortTemplate, ServiceSpec,
        ServiceTemplate, SidecarTemplate, TemplateText, WorkloadKind, WorkloadTemplate,
    },
    materialization::{
        BackendEndpoint, MaterializationRecord, MaterializationState, MaterializationTarget,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    projection::{
        LiveObjectIdentity, ProjectionError, ProjectionPlan, ProjectionReadinessInspection,
        ProjectionReconciler, APPLY_ROUNDS,
    },
    sleep_policy::ResolvedSleepPolicy,
    workload::WorkloadClassVersionRef,
};

fn plan() -> ProjectionPlan {
    let generation = Generation::new(2);
    let object = KubernetesObject::Service(Service {
        metadata: ObjectMeta {
            name: "app".into(),
            namespace: Some("apps".into()),
            labels: BTreeMap::from([(
                crate::manifest::LABEL_INSTANCE_GENERATION.into(),
                "2".into(),
            )]),
            annotations: BTreeMap::new(),
        },
        spec: ServiceSpec {
            selector: BTreeMap::new(),
            ports: vec![ServicePort {
                name: None,
                port: 80,
                target_port: 8080,
            }],
        },
    });
    let record = MaterializationRecord {
        pod_backend: None,
        id: MaterializationId::new("mat").unwrap(),
        instance_id: InstanceId::new("app").unwrap(),
        instance_generation: generation,
        projection_generation: generation,
        target: MaterializationTarget::new("cluster", "apps").unwrap(),
        state: MaterializationState::Pending,
        backend: None,
        backend_generation: BackendGeneration::new(1),
        rendered_objects: vec![],
        exclusivity_keys: vec![],
        reconciliation_lease: None,
    };
    ProjectionPlan::from_manifest(
        &record,
        &RenderedManifest {
            instance_generation: generation,
            template_generation: None,
            objects: vec![RenderedManifestObject {
                apply_order: ApplyOrder::Service,
                object,
            }],
        },
    )
    .unwrap()
}

fn response(status: StatusCode, body: Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}

fn status(code: u16, reason: &str) -> Value {
    json!({"apiVersion":"v1","kind":"Status","status":"Failure","reason":reason,"code":code,"message":"conditional fixture"})
}

fn not_found() -> Response<Full<Bytes>> {
    response(StatusCode::NOT_FOUND, status(404, "NotFound"))
}

async fn json_body(request: Request<kube::client::Body>) -> Value {
    serde_json::from_slice(&request.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

/// A rendered Deployment or StatefulSet with one replica.
fn rendered_workload(kind: WorkloadKind) -> KubernetesObject {
    let template = ManifestTemplate {
        workload: WorkloadTemplate {
            kind,
            name: TemplateText::literal("app"),
            replicas: Some(1),
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("example/app:1"),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: Vec::new(),
            },
        },
        service: Some(ServiceTemplate {
            name: TemplateText::literal("svc"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: None,
        },
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    };
    let instance = InstanceRecord {
        id: InstanceId::new("app").unwrap(),
        workload_class: WorkloadClassVersionRef::new(
            WorkloadClassId::new("class").unwrap(),
            Generation::new(1),
        ),
        values: InstanceValues::new(),
        state: InstanceState::Waking,
        generation: Generation::new(2),
    };
    render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance,
        sleep_policy: ResolvedSleepPolicy {
            idle_timeout_ms: 120_000,
            idle_retry_backoff_ms: 5_000,
            drain_grace_timeout_ms: 30_000,
        },
        namespace: "apps",
        template_generation: None,
    })
    .unwrap()
    .objects
    .into_iter()
    .map(|rendered| rendered.object)
    .find(|object| matches!(object.kind(), "Deployment" | "StatefulSet"))
    .unwrap()
}

/// A raw auxiliary Deployment that leaves its replica count to the API
/// server's default.
fn raw_deployment() -> KubernetesObject {
    KubernetesObject::Raw(RawKubernetesObject {
        api_version: "apps/v1".into(),
        kind: "Deployment".into(),
        metadata: ObjectMeta {
            name: "aux".into(),
            namespace: Some("apps".into()),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        },
        pod_template_metadata: None,
        value: json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"aux"},"spec":{"selector":{"matchLabels":{"app":"aux"}},"template":{"metadata":{"labels":{"app":"aux"}},"spec":{"containers":[{"name":"c","image":"example/aux:1"}]}}}}),
    })
}

/// Each request's method, path and body.
type RecordedRequests = Arc<Mutex<Vec<(Method, String, Value)>>>;

/// A client whose API server records each mutation and accepts it.
fn recording_client() -> (KubeMaterializerClient, RecordedRequests) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let service = service_fn(move |request: Request<kube::client::Body>| {
        let observed = observed.clone();
        async move {
            let method = request.method().clone();
            let path = request.uri().path().to_owned();
            let mut body = json_body(request).await;
            observed.lock().unwrap().push((method, path, body.clone()));
            body["metadata"]["uid"] = "stored".into();
            body["metadata"]["resourceVersion"] = "12".into();
            Ok::<_, Infallible>(response(StatusCode::OK, body))
        }
    });
    (
        KubeMaterializerClient::new(kube::Client::new(service, "apps")),
        requests,
    )
}

#[tokio::test]
async fn workloads_are_created_with_zero_replicas() {
    for (object, path) in [
        (
            rendered_workload(WorkloadKind::Deployment),
            "/apis/apps/v1/namespaces/apps/deployments",
        ),
        (
            rendered_workload(WorkloadKind::StatefulSet),
            "/apis/apps/v1/namespaces/apps/statefulsets",
        ),
        (
            raw_deployment(),
            "/apis/apps/v1/namespaces/apps/deployments",
        ),
    ] {
        assert_ne!(object.to_kubernetes_json()["spec"]["replicas"], 0);
        let (client, requests) = recording_client();

        client.apply_object(&object, None).await.unwrap();

        let requests = requests.lock().unwrap();
        let [(method, requested, body)] = &requests[..] else {
            panic!("one create is sent");
        };
        assert_eq!((method, requested.as_str()), (&Method::POST, path));
        assert_eq!(body["spec"]["replicas"], 0, "{}", object.kind());
        assert!(body["metadata"].get("uid").is_none());
        assert!(body["metadata"].get("resourceVersion").is_none());
    }
}

#[tokio::test]
async fn conditional_patch_scales_a_workload_to_its_rendered_replicas() {
    let identity = LiveObjectIdentity {
        uid: "observed-uid".into(),
        resource_version: "10".into(),
    };
    for object in [
        rendered_workload(WorkloadKind::Deployment),
        rendered_workload(WorkloadKind::StatefulSet),
        // A merge patch restores a defaulted count only when it names one.
        raw_deployment(),
    ] {
        let (client, requests) = recording_client();

        client.apply_object(&object, Some(&identity)).await.unwrap();

        let requests = requests.lock().unwrap();
        let [(method, path, body)] = &requests[..] else {
            panic!("one patch is sent");
        };
        assert_eq!(method, Method::PATCH);
        assert!(path.ends_with(&format!("/{}", object.name())), "{path}");
        assert_eq!(body["metadata"]["uid"], "observed-uid");
        assert_eq!(body["metadata"]["resourceVersion"], "10");
        assert_eq!(body["spec"]["replicas"], 1, "{}", object.kind());
    }
}

#[tokio::test]
async fn activation_scales_the_created_object_on_its_uid_alone() {
    let identity = LiveObjectIdentity {
        uid: "created-uid".into(),
        resource_version: "10".into(),
    };
    for object in [
        rendered_workload(WorkloadKind::Deployment),
        rendered_workload(WorkloadKind::StatefulSet),
        raw_deployment(),
    ] {
        let (client, requests) = recording_client();

        client.activate_workload(&object, &identity).await.unwrap();

        let requests = requests.lock().unwrap();
        let [(method, path, body)] = &requests[..] else {
            panic!("one patch is sent");
        };
        assert_eq!(method, Method::PATCH);
        assert!(path.ends_with(&format!("/{}", object.name())), "{path}");
        // The workload controller's status writes move the resourceVersion
        // right after creation, so the patch names the UID only.
        assert_eq!(
            *body,
            json!({ "metadata": { "uid": "created-uid" }, "spec": { "replicas": 1 } }),
            "{}",
            object.kind()
        );
    }
}

#[tokio::test]
async fn activation_of_a_replaced_object_must_be_inspected_again() {
    // The API server answers a UID that differs from the live object's with
    // 422: metadata.uid is immutable.
    let service = service_fn(|_request: Request<kube::client::Body>| async {
        Ok::<_, Infallible>(response(
            StatusCode::UNPROCESSABLE_ENTITY,
            status(422, "Invalid"),
        ))
    });
    let client = KubeMaterializerClient::new(kube::Client::new(service, "apps"));
    let identity = LiveObjectIdentity {
        uid: "created-uid".into(),
        resource_version: "10".into(),
    };

    let error = client
        .activate_workload(&rendered_workload(WorkloadKind::Deployment), &identity)
        .await
        .unwrap_err();

    assert!(!error.is_permanent() && !error.is_retryable(), "{error}");
}

#[tokio::test]
async fn service_is_created_as_rendered() {
    let plan = plan();
    let object = &plan.manifest().unwrap().objects[0].object;
    let (client, requests) = recording_client();

    client.apply_object(object, None).await.unwrap();

    let requests = requests.lock().unwrap();
    let [(method, path, body)] = &requests[..] else {
        panic!("one create is sent");
    };
    assert_eq!(
        (method, path.as_str()),
        (&Method::POST, "/api/v1/namespaces/apps/services")
    );
    assert_eq!(*body, object.to_kubernetes_json());
}

#[tokio::test(start_paused = true)]
async fn production_projection_rejects_replacement_between_observe_and_update_or_delete() {
    for delete in [false, true] {
        let plan = plan();
        let mut original = plan.manifest().unwrap().objects[0]
            .object
            .to_kubernetes_json();
        original["metadata"]["uid"] = "old-uid".into();
        original["metadata"]["resourceVersion"] = "10".into();
        let survivor = Arc::new(Mutex::new(original.clone()));
        let live = survivor.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let service = service_fn(move |request: Request<kube::client::Body>| {
            let live = live.clone();
            let observed = observed.clone();
            let original = original.clone();
            async move {
                let method = request.method().clone();
                observed.lock().unwrap().push(method.clone());
                if method == Method::GET {
                    // Observation A is returned while replacement B takes its name.
                    let mut replacement = live.lock().unwrap();
                    replacement["metadata"]["uid"] = "replacement-uid".into();
                    replacement["metadata"]["resourceVersion"] = "11".into();
                    return Ok::<_, Infallible>(response(StatusCode::OK, original));
                }
                // Every mutation, in every round, carries the observed identity.
                let body = json_body(request).await;
                if method == Method::PATCH {
                    assert_eq!(body["metadata"]["uid"], "old-uid");
                    assert_eq!(body["metadata"]["resourceVersion"], "10");
                } else {
                    assert_eq!(method, Method::DELETE);
                    assert_eq!(body["preconditions"]["uid"], "old-uid");
                    assert_eq!(body["preconditions"]["resourceVersion"], "10");
                    assert_eq!(body["propagationPolicy"], "Foreground");
                }
                Ok(response(StatusCode::CONFLICT, status(409, "Conflict")))
            }
        });
        let materializer = KubernetesMaterializer::new(KubeMaterializerClient::new(
            kube::Client::new(service, "apps"),
        ));
        let projection = ProjectionReconciler::new(&materializer);
        if delete {
            assert!(matches!(
                projection.delete_owned(&plan).await.unwrap_err(),
                ProjectionError::CleanupBlocked { .. }
            ));
        } else {
            assert!(matches!(
                projection.apply(&plan).await.unwrap_err(),
                ProjectionError::Apply { .. }
            ));
            assert_eq!(
                requests.lock().unwrap().len(),
                2 * APPLY_ROUNDS as usize,
                "each round inspects, then sends one conditional patch"
            );
        }
        assert_eq!(
            survivor.lock().unwrap()["metadata"]["uid"],
            "replacement-uid"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn production_projection_creates_only_when_absent_and_refuses_intervening_create() {
    for occupied in [false, true] {
        let plan = plan();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let service = service_fn(move |request: Request<kube::client::Body>| {
            let observed = observed.clone();
            async move {
                let method = request.method().clone();
                observed.lock().unwrap().push(method.clone());
                if method == Method::GET {
                    return Ok::<_, Infallible>(not_found());
                }
                assert_eq!(method, Method::POST);
                assert_eq!(request.uri().path(), "/api/v1/namespaces/apps/services");
                let mut body = json_body(request).await;
                assert!(body["metadata"].get("uid").is_none());
                assert!(body["metadata"].get("resourceVersion").is_none());
                body["metadata"]["uid"] = "created".into();
                body["metadata"]["resourceVersion"] = "12".into();
                Ok(if occupied {
                    response(StatusCode::CONFLICT, status(409, "AlreadyExists"))
                } else {
                    response(StatusCode::CREATED, body)
                })
            }
        });
        let materializer = KubernetesMaterializer::new(KubeMaterializerClient::new(
            kube::Client::new(service, "apps"),
        ));
        assert_eq!(
            ProjectionReconciler::new(&materializer)
                .apply(&plan)
                .await
                .is_err(),
            occupied
        );
        let rounds = if occupied { APPLY_ROUNDS as usize } else { 1 };
        let expected = (0..rounds)
            .flat_map(|_| [Method::GET, Method::POST])
            .collect::<Vec<_>>();
        assert_eq!(
            *requests.lock().unwrap(),
            expected,
            "a create follows only an inspection that found the name free"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn production_projection_adopts_an_object_whose_create_arrived_first() {
    let plan = plan();
    let mut stored = plan.manifest().unwrap().objects[0]
        .object
        .to_kubernetes_json();
    stored["metadata"]["uid"] = "late-create".into();
    stored["metadata"]["resourceVersion"] = "7".into();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let service = service_fn(move |request: Request<kube::client::Body>| {
        let observed = observed.clone();
        let stored = stored.clone();
        async move {
            let method = request.method().clone();
            let seen = {
                let mut observed = observed.lock().unwrap();
                observed.push(method.clone());
                observed.len()
            };
            Ok::<_, Infallible>(match (seen, method) {
                (1, Method::GET) => not_found(),
                (2, Method::POST) => response(StatusCode::CONFLICT, status(409, "AlreadyExists")),
                (3, Method::GET) => response(StatusCode::OK, stored),
                (4, Method::PATCH) => {
                    let body = json_body(request).await;
                    assert_eq!(body["metadata"]["uid"], "late-create");
                    assert_eq!(body["metadata"]["resourceVersion"], "7");
                    response(StatusCode::OK, body)
                }
                (seen, method) => panic!("unexpected request {seen}: {method}"),
            })
        }
    });
    let materializer = KubernetesMaterializer::new(KubeMaterializerClient::new(kube::Client::new(
        service, "apps",
    )));

    ProjectionReconciler::new(&materializer)
        .apply(&plan)
        .await
        .expect("the next round patches the object that took the name");

    assert_eq!(requests.lock().unwrap().len(), 4);
}

#[tokio::test(start_paused = true)]
async fn production_cleanup_keeps_ownership_until_terminating_pods_and_replica_sets_are_absent() {
    for remaining in ["pod", "replica_set", "none"] {
        let plan = plan();
        let service = service_fn(move |request: Request<kube::client::Body>| async move {
            assert_eq!(request.method(), Method::GET);
            let path = request.uri().path();
            let body = if path.ends_with("/services/app") {
                return Ok::<_, Infallible>(not_found());
            } else if path.ends_with("/pods") {
                assert!(request.uri().query().unwrap().contains("instance-id"));
                json!({"apiVersion":"v1","kind":"PodList","metadata":{},"items":if remaining=="pod" { vec![json!({"metadata":{"name":"old","uid":"old-pod","deletionTimestamp":"2026-09-07T00:00:00Z"}})] } else { vec![] }})
            } else {
                assert!(path.ends_with("/replicasets"));
                json!({"apiVersion":"apps/v1","kind":"ReplicaSetList","metadata":{},"items":if remaining=="replica_set" { vec![json!({"metadata":{"name":"old","uid":"old-rs"}})] } else { vec![] }})
            };
            Ok(response(StatusCode::OK, body))
        });
        let materializer = KubernetesMaterializer::new(KubeMaterializerClient::new(
            kube::Client::new(service, "apps"),
        ));
        assert_eq!(
            ProjectionReconciler::new(&materializer)
                .delete_owned(&plan)
                .await
                .is_err(),
            remaining != "none"
        );
    }
}

#[tokio::test]
async fn mutation_timeouts_require_inspection_and_read_timeouts_are_retryable() {
    let service = service_fn(|_: Request<kube::client::Body>| {
        std::future::pending::<Result<Response<Full<Bytes>>, Infallible>>()
    });
    let config = KubeMaterializerClientConfig {
        request_timeout: Duration::from_millis(5),
        readiness_timeout: Duration::from_millis(5),
        ..Default::default()
    };
    let client =
        KubeMaterializerClient::with_config(kube::Client::new(service, "apps"), config).unwrap();
    let plan = plan();
    let object = &plan.manifest().unwrap().objects[0].object;
    let identity = LiveObjectIdentity {
        uid: "a".into(),
        resource_version: "1".into(),
    };
    // The API server may still apply a mutation whose response never came.
    for unknown in [
        client.apply_object(object, None).await.unwrap_err(),
        client
            .apply_object(object, Some(&identity))
            .await
            .unwrap_err(),
        client
            .delete_object(&plan.object_refs()[0], &identity)
            .await
            .unwrap_err(),
    ] {
        assert!(!unknown.is_retryable() && !unknown.is_permanent());
    }
    assert!(client
        .inspect_object(&plan.object_refs()[0])
        .await
        .unwrap_err()
        .is_retryable());
    assert!(client
        .wait_for_readiness(&plan.object_refs())
        .await
        .unwrap_err()
        .is_retryable());
}

#[tokio::test]
async fn mutation_statuses_say_whether_to_resend_inspect_or_stop() {
    for (code, reason, retryable, permanent) in [
        (409, "AlreadyExists", false, false),
        (409, "Conflict", false, false),
        (500, "InternalError", false, false),
        (503, "ServiceUnavailable", false, false),
        (429, "TooManyRequests", true, false),
        (422, "Invalid", false, true),
    ] {
        let service = service_fn(move |_: Request<kube::client::Body>| async move {
            Ok::<_, Infallible>(response(
                StatusCode::from_u16(code).unwrap(),
                status(code, reason),
            ))
        });
        let client = KubeMaterializerClient::new(kube::Client::new(service, "apps"));
        let plan = plan();
        let object = &plan.manifest().unwrap().objects[0].object;
        let identity = LiveObjectIdentity {
            uid: "a".into(),
            resource_version: "1".into(),
        };
        for error in [
            client.apply_object(object, None).await.unwrap_err(),
            client
                .apply_object(object, Some(&identity))
                .await
                .unwrap_err(),
            client
                .delete_object(&plan.object_refs()[0], &identity)
                .await
                .unwrap_err(),
        ] {
            assert_eq!(
                (error.is_retryable(), error.is_permanent()),
                (retryable, permanent),
                "{code} {reason}"
            );
        }
    }
}

#[tokio::test]
async fn legacy_and_raw_storage_cleanup_refuses_before_any_pvc_delete() {
    for scenario in ["delete", "missing_policy", "unbound", "external_binding"] {
        let mut manifest = plan().manifest().unwrap().clone();
        manifest.objects.clear();
        for (kind, name, order) in [
            ("PersistentVolume", "disk", ApplyOrder::PersistentVolume),
            (
                "PersistentVolumeClaim",
                "claim",
                ApplyOrder::PersistentVolumeClaim,
            ),
        ] {
            let spec = if kind == "PersistentVolume" {
                match scenario {
                    "delete" => json!({"persistentVolumeReclaimPolicy":"Delete"}),
                    "missing_policy" => json!({}),
                    _ => json!({"persistentVolumeReclaimPolicy":"Retain"}),
                }
            } else {
                match scenario {
                    "unbound" => json!({}),
                    "external_binding" => json!({"volumeName":"foreign"}),
                    _ => json!({"volumeName":"disk"}),
                }
            };
            let value = json!({"apiVersion":"v1","kind":kind,"metadata":{"name":name},"spec":spec});
            manifest.objects.push(RenderedManifestObject {
                apply_order: order,
                object: KubernetesObject::Raw(RawKubernetesObject {
                    api_version: "v1".into(),
                    kind: kind.into(),
                    metadata: ObjectMeta {
                        name: name.into(),
                        namespace: (kind == "PersistentVolumeClaim").then(|| "apps".into()),
                        labels: BTreeMap::new(),
                        annotations: BTreeMap::new(),
                    },
                    pod_template_metadata: None,
                    value,
                }),
            });
        }
        let generation = Generation::new(2);
        let record = MaterializationRecord {
            pod_backend: None,
            id: MaterializationId::new("mat").unwrap(),
            instance_id: InstanceId::new("app").unwrap(),
            instance_generation: generation,
            projection_generation: generation,
            target: MaterializationTarget::new("cluster", "apps").unwrap(),
            state: MaterializationState::Deleting,
            backend: None,
            backend_generation: BackendGeneration::new(1),
            rendered_objects: vec![],
            exclusivity_keys: vec![],
            reconciliation_lease: None,
        };
        let plan = ProjectionPlan::from_manifest(&record, &manifest).unwrap();
        let mut values = plan
            .manifest()
            .unwrap()
            .objects
            .iter()
            .map(|rendered| rendered.object.to_kubernetes_json())
            .collect::<Vec<_>>();
        for value in &mut values {
            value["metadata"]["uid"] =
                format!("{}-uid", value["metadata"]["name"].as_str().unwrap()).into();
            value["metadata"]["resourceVersion"] = "10".into();
        }
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let service = service_fn(move |request: Request<kube::client::Body>| {
            observed.lock().unwrap().push(request.method().clone());
            assert_eq!(
                request.method(),
                Method::GET,
                "no object, especially the PVC, may be deleted before retained binding is proven"
            );
            let body = if request.uri().path().contains("persistentvolumeclaims") {
                values[1].clone()
            } else {
                values[0].clone()
            };
            async move { Ok::<_, Infallible>(response(StatusCode::OK, body)) }
        });
        let materializer = KubernetesMaterializer::new(KubeMaterializerClient::new(
            kube::Client::new(service, "apps"),
        ));
        assert!(
            ProjectionReconciler::new(&materializer)
                .delete_owned(&plan)
                .await
                .is_err(),
            "{scenario}"
        );
        assert!(requests
            .lock()
            .unwrap()
            .iter()
            .all(|method| *method == Method::GET));
    }
}

#[tokio::test(start_paused = true)]
async fn production_readiness_waits_for_current_service_nonterminating_ready_membership() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // All stale/unready snapshots share the requested Service name. Only the last
    // one belongs to the current Service and represents a ready nonterminating Pod.
    // Each watch ends at once, so every round reads the next snapshot.
    let snapshot = Arc::new(AtomicUsize::new(0));
    let advance = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let list_count = Arc::new(AtomicUsize::new(0));
    let snapshots = snapshot.clone();
    let advancing = advance.clone();
    let lists = list_count.clone();
    let service = service_fn(move |request: Request<kube::client::Body>| {
        assert_eq!(request.method(), Method::GET);
        let step = snapshots.load(Ordering::SeqCst);
        let body = if request.uri().path().ends_with("/services/app") {
            let mut service = json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"app","namespace":"apps","uid":"current-service"},"spec":{"ports":[{"port":80}]}});
            if step == 5 {
                service["metadata"].as_object_mut().unwrap().remove("uid");
            }
            if step == 6 {
                service["metadata"]["deletionTimestamp"] = json!("2026-09-07T00:00:00Z");
            }
            service.to_string()
        } else {
            assert!(request.uri().path().ends_with("/endpointslices"));
            let query = request.uri().query().unwrap();
            assert!(query.contains("service-name"));
            if query.contains("watch=true") {
                String::new()
            } else {
                lists.fetch_add(1, Ordering::SeqCst);
                let mut slice = ready_slice("current-service");
                match step {
                    0 => {
                        slice["metadata"]["ownerReferences"][0]["uid"] =
                            json!("old-same-name-service")
                    }
                    1 => slice["endpoints"][0]["conditions"]["ready"] = json!(false),
                    2 => slice["endpoints"][0]["conditions"]["terminating"] = json!(true),
                    3 => slice["metadata"]["ownerReferences"] = json!([]),
                    4 => slice["metadata"]["deletionTimestamp"] = json!("2026-09-07T00:00:00Z"),
                    7 => slice["endpoints"][0]["addresses"] = json!([]),
                    _ => {}
                }
                if advancing.load(Ordering::SeqCst) {
                    snapshots.fetch_add(1, Ordering::SeqCst);
                }
                json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{"resourceVersion":"1"},"items":[slice]}).to_string()
            }
        };
        async move {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from(body)))
                    .unwrap(),
            )
        }
    });
    let client = KubeMaterializerClient::with_config(
        kube::Client::new(service, "apps"),
        KubeMaterializerClientConfig {
            readiness_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    let refs = plan().object_refs();
    for step in 0..8 {
        snapshot.store(step, Ordering::SeqCst);
        assert_eq!(
            client.inspect_readiness(&refs).await.unwrap(),
            ProjectionReadinessInspection::Unready {
                reason: "no_ready_endpoints".to_owned()
            },
            "snapshot {step}"
        );
    }
    snapshot.store(0, Ordering::SeqCst);
    list_count.store(0, Ordering::SeqCst);
    advance.store(true, Ordering::SeqCst);
    let backend = client.wait_for_readiness(&refs).await.unwrap();
    assert_eq!(backend.uri(), "http://app.apps.svc.cluster.local:80");
    assert_eq!(
        list_count.load(Ordering::SeqCst),
        9,
        "every stale/unready stage was rejected before readiness"
    );
    assert!(matches!(
        client.inspect_readiness(&refs).await.unwrap(),
        ProjectionReadinessInspection::Ready(_)
    ));
}

#[tokio::test]
async fn readiness_returns_when_the_watch_delivers_a_ready_slice_the_service_owns() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let service = service_fn(move |request: Request<kube::client::Body>| {
        let observed = observed.clone();
        async move {
            assert_eq!(request.method(), Method::GET);
            let query = request.uri().query().unwrap_or_default().to_owned();
            let body = if request.uri().path().ends_with("/services/app") {
                observed.lock().unwrap().push("service");
                json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"app","namespace":"apps","uid":"current-service"},"spec":{"clusterIP":"10.96.0.10","ports":[{"port":80}]}}).to_string()
            } else if query.contains("watch=true") {
                observed.lock().unwrap().push("watch");
                // The watch resumes from the list it follows.
                assert!(query.contains("resourceVersion=41"), "{query}");
                assert!(query.contains("service-name"), "{query}");
                [
                    json!({"type":"ADDED","object":ready_slice("old-same-name-service")}),
                    json!({"type":"MODIFIED","object":ready_slice("current-service")}),
                ]
                .map(|event| format!("{event}\n"))
                .concat()
            } else {
                observed.lock().unwrap().push("list");
                json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{"resourceVersion":"41"},"items":[]}).to_string()
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from(body)))
                    .unwrap(),
            )
        }
    });
    let client = KubeMaterializerClient::new(kube::Client::new(service, "apps"));

    let backend = client
        .wait_for_readiness(&plan().object_refs())
        .await
        .unwrap();

    assert_eq!(backend.uri(), "http://10.96.0.10:80");
    assert_eq!(*requests.lock().unwrap(), ["service", "list", "watch"]);
}

#[tokio::test(start_paused = true)]
async fn readiness_paces_its_rounds_when_watches_end_at_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let lists = Arc::new(AtomicUsize::new(0));
    let listed = lists.clone();
    let service = service_fn(move |request: Request<kube::client::Body>| {
        let listed = listed.clone();
        async move {
            let query = request.uri().query().unwrap_or_default();
            let body = if request.uri().path().ends_with("/services/app") {
                json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"app","namespace":"apps","uid":"current-service"},"spec":{"ports":[{"port":80}]}}).to_string()
            } else if query.contains("watch=true") {
                String::new()
            } else {
                listed.fetch_add(1, Ordering::SeqCst);
                json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{"resourceVersion":"1"},"items":[]}).to_string()
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from(body)))
                    .unwrap(),
            )
        }
    });
    let client = KubeMaterializerClient::with_config(
        kube::Client::new(service, "apps"),
        KubeMaterializerClientConfig {
            readiness_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .unwrap();

    let error = client
        .wait_for_readiness(&plan().object_refs())
        .await
        .unwrap_err();

    assert!(error.is_retryable());
    assert!(
        lists.load(Ordering::SeqCst) <= 6,
        "one round per pause, not one per scheduler turn"
    );
}

/// An EndpointSlice of Service `app` with one ready endpoint, owned by the
/// Service with the given UID.
#[tokio::test]
async fn ready_pod_backend_is_the_ready_endpoint_of_a_slice_the_service_owns() {
    fn client(slices: Vec<Value>) -> KubeMaterializerClient {
        let service = service_fn(move |request: Request<kube::client::Body>| {
            let slices = slices.clone();
            async move {
                assert_eq!(request.method(), Method::GET);
                let body = if request.uri().path().ends_with("/services/app") {
                    json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"app","namespace":"apps","uid":"current-service"},"spec":{"clusterIP":"10.96.0.10","ports":[{"port":80}]}})
                } else {
                    json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{"resourceVersion":"41"},"items":slices})
                };
                Ok::<_, Infallible>(response(StatusCode::OK, body))
            }
        });
        KubeMaterializerClient::new(kube::Client::new(service, "apps"))
    }
    let slice = |owner: &str, address: &str| {
        let mut slice = ready_slice(owner);
        slice["endpoints"][0]["addresses"] = json!([address]);
        slice["ports"] = json!([{"port": 15000, "protocol": "TCP"}]);
        slice
    };
    let refs = plan().object_refs();

    let observed = client(vec![
        slice("old-same-name-service", "10.0.0.9"),
        slice("current-service", "10.0.0.1"),
    ])
    .ready_pod_backend(&refs)
    .await
    .unwrap();
    assert_eq!(
        observed.as_ref().map(BackendEndpoint::uri),
        Some("http://10.0.0.1:15000")
    );

    let mut unready = slice("current-service", "10.0.0.1");
    unready["endpoints"][0]["conditions"]["ready"] = false.into();
    for slices in [
        vec![],
        vec![slice("old-same-name-service", "10.0.0.9")],
        vec![unready],
    ] {
        assert_eq!(client(slices).ready_pod_backend(&refs).await.unwrap(), None);
    }
}

fn ready_slice(owner_uid: &str) -> Value {
    json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","addressType":"IPv4","metadata":{"name":"app-slice","namespace":"apps","labels":{"kubernetes.io/service-name":"app"},"ownerReferences":[{"apiVersion":"v1","kind":"Service","name":"app","uid":owner_uid,"controller":true}]},"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true,"terminating":false}}]})
}
