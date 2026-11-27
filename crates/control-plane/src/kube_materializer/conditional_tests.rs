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
    ids::{BackendGeneration, Generation, InstanceId, MaterializationId},
    manifest::{
        ApplyOrder, KubernetesObject, ObjectMeta, RenderedManifest, RenderedManifestObject,
        Service, ServicePort, ServiceSpec,
    },
    materialization::{MaterializationRecord, MaterializationState, MaterializationTarget},
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    projection::{LiveObjectIdentity, ProjectionError, ProjectionPlan, ProjectionReconciler},
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

fn status(code: u16) -> Value {
    json!({"apiVersion":"v1","kind":"Status","status":"Failure","reason":if code == 404 { "NotFound" } else { "Conflict" },"code":code,"message":"conditional fixture"})
}

#[tokio::test]
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
                let body: Value = serde_json::from_slice(
                    &request.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                if method == Method::PATCH {
                    assert_eq!(body["metadata"]["uid"], "old-uid");
                    assert_eq!(body["metadata"]["resourceVersion"], "10");
                } else {
                    assert_eq!(method, Method::DELETE);
                    assert_eq!(body["preconditions"]["uid"], "old-uid");
                    assert_eq!(body["preconditions"]["resourceVersion"], "10");
                    assert_eq!(body["propagationPolicy"], "Foreground");
                }
                Ok(response(StatusCode::CONFLICT, status(409)))
            }
        });
        let materializer = KubernetesMaterializer::new(KubeMaterializerClient::new(
            kube::Client::new(service, "apps"),
        ));
        let projection = ProjectionReconciler::new(&materializer);
        let error = if delete {
            projection.delete_owned(&plan).await
        } else {
            projection.apply(&plan).await
        }
        .unwrap_err();
        assert!(matches!(
            error,
            ProjectionError::Apply { .. } | ProjectionError::Delete { .. }
        ));
        assert_eq!(
            survivor.lock().unwrap()["metadata"]["uid"],
            "replacement-uid"
        );
        assert_eq!(
            requests.lock().unwrap().len(),
            2,
            "no unconditional fallback after conflict"
        );
    }
}

#[tokio::test]
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
                    return Ok::<_, Infallible>(response(StatusCode::NOT_FOUND, status(404)));
                }
                assert_eq!(method, Method::POST);
                assert_eq!(request.uri().path(), "/api/v1/namespaces/apps/services");
                let mut body: Value = serde_json::from_slice(
                    &request.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                assert!(body["metadata"].get("uid").is_none());
                assert!(body["metadata"].get("resourceVersion").is_none());
                body["metadata"]["uid"] = "created".into();
                body["metadata"]["resourceVersion"] = "12".into();
                Ok(if occupied {
                    response(StatusCode::CONFLICT, status(409))
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
        assert_eq!(*requests.lock().unwrap(), vec![Method::GET, Method::POST]);
    }
}

#[tokio::test]
async fn production_cleanup_keeps_ownership_until_terminating_pods_and_replica_sets_are_absent() {
    for remaining in ["pod", "replica_set", "none"] {
        let plan = plan();
        let service = service_fn(move |request: Request<kube::client::Body>| async move {
            assert_eq!(request.method(), Method::GET);
            let path = request.uri().path();
            let body = if path.ends_with("/services/app") {
                return Ok::<_, Infallible>(response(StatusCode::NOT_FOUND, status(404)));
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
async fn production_mutation_timeouts_are_uncertain_but_read_timeouts_are_recoverable() {
    let service = service_fn(|_: Request<kube::client::Body>| {
        std::future::pending::<Result<Response<Full<Bytes>>, Infallible>>()
    });
    let config = KubeMaterializerClientConfig {
        delete_timeout: Duration::from_millis(5),
        readiness_timeout: Duration::from_millis(5),
        pvc_bound_timeout: Duration::from_millis(5),
        ..Default::default()
    };
    let client =
        KubeMaterializerClient::with_config(kube::Client::new(service, "apps"), config).unwrap();
    let plan = plan();
    let object = &plan.manifest().unwrap().objects[0].object;
    assert!(client
        .apply_object(object, None)
        .await
        .unwrap_err()
        .outcome_uncertain());
    let identity = LiveObjectIdentity {
        uid: "a".into(),
        resource_version: "1".into(),
    };
    assert!(client
        .delete_object(&plan.object_refs()[0], &identity)
        .await
        .unwrap_err()
        .outcome_uncertain());
    let error = client
        .inspect_object(&plan.object_refs()[0])
        .await
        .unwrap_err();
    assert!(error.is_retryable());
    assert!(!error.outcome_uncertain());
    assert!(client
        .wait_for_readiness(&plan.object_refs())
        .await
        .unwrap_err()
        .is_retryable());
    assert!(client
        .wait_for_pvc_bound("apps", "pvc")
        .await
        .unwrap_err()
        .is_retryable());
}

#[tokio::test]
async fn legacy_and_raw_storage_cleanup_refuses_before_any_pvc_delete() {
    use crate::manifest::RawKubernetesObject;
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

#[tokio::test]
async fn production_readiness_waits_for_current_service_nonterminating_ready_membership() {
    use crate::projection::ProjectionReadinessInspection;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // All stale/unready snapshots share the requested Service name. Only the last
    // one belongs to the current Service and represents a ready nonterminating Pod.
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
            service
        } else {
            assert!(request.uri().path().ends_with("/endpointslices"));
            assert!(request.uri().query().unwrap().contains("service-name"));
            lists.fetch_add(1, Ordering::SeqCst);
            let mut slice = json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","addressType":"IPv4","metadata":{"name":"app-slice","namespace":"apps","labels":{"kubernetes.io/service-name":"app"},"ownerReferences":[{"apiVersion":"v1","kind":"Service","name":"app","uid":"current-service","controller":true}]},"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true,"terminating":false}}]});
            match step {
                0 => {
                    slice["metadata"]["ownerReferences"][0]["uid"] = json!("old-same-name-service")
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
            json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{},"items":[slice]})
        };
        async move { Ok::<_, Infallible>(response(StatusCode::OK, body)) }
    });
    let client = KubeMaterializerClient::with_config(
        kube::Client::new(service, "apps"),
        KubeMaterializerClientConfig {
            readiness_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(1),
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
