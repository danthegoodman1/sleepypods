use std::collections::BTreeMap;

use k8s_openapi::{
    api::{apps::v1::ReplicaSet, core::v1::Pod},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::api::{Api, DynamicObject, ListParams};

use super::{kube_error, KubeMaterializerClient};
use crate::{
    manifest::{LABEL_INSTANCE_GENERATION, LABEL_INSTANCE_ID},
    materialization::RenderedObjectRef,
    materializer::{IdleMemberIdentity, KubernetesClientError, KubernetesClientResult},
    projection::{ANNOTATION_MATERIALIZATION_ID, LABEL_MANAGED_BY, LABEL_MANAGED_BY_VALUE},
};

impl KubeMaterializerClient {
    pub(super) async fn verify_idle_member_snapshot(
        &self,
        objects: &[RenderedObjectRef],
        identity: &IdleMemberIdentity,
    ) -> KubernetesClientResult<()> {
        let mut workloads = objects.iter().filter(|object| {
            object.api_version == "apps/v1"
                && matches!(object.kind.as_str(), "Deployment" | "StatefulSet")
        });
        let workload = workloads
            .next()
            .ok_or_else(|| unsupported("workload is missing"))?;
        if workloads.next().is_some() {
            return Err(unsupported("multiple workload controllers are unsupported"));
        }
        let api = self.dynamic_api(workload)?;
        let controller = api.get(&workload.name).await.map_err(kube_error)?;
        validate_controller(&controller, &workload.kind, identity)?;

        // Include every generation and terminating/unready pod. Ignoring any of these
        // could mistake one idle member for the activity of a peer or predecessor.
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &workload.namespace);
        let pods = pods
            .list(&ListParams::default().labels(&format!(
                "{LABEL_INSTANCE_ID}={}",
                identity.instance_id.as_str(),
            )))
            .await
            .map_err(kube_error)?;
        let pod = validate_single_pod(&pods.items, identity)?;
        let controller_uid = controller
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| unsupported("controller UID is missing"))?;
        if workload.kind == "StatefulSet" {
            require_owner(&pod.metadata, "StatefulSet", controller_uid)?;
        } else {
            let owner = controller_owner(&pod.metadata, "ReplicaSet")?;
            let replica_sets: Api<ReplicaSet> =
                Api::namespaced(self.client.clone(), &workload.namespace);
            let replica_set = replica_sets.get(&owner.name).await.map_err(kube_error)?;
            if replica_set.metadata.uid.as_deref() != Some(owner.uid.as_str()) {
                return Err(unsupported("pod ReplicaSet was replaced"));
            }
            if replica_set.metadata.deletion_timestamp.is_some() {
                return Err(unsupported("pod ReplicaSet is terminating"));
            }
            require_owner(&replica_set.metadata, "Deployment", controller_uid)?;
        }

        // Detect controller replacement/scaling during the observation. This is not
        // an atomic transaction with Kubernetes: exclusive lifecycle ownership remains
        // required; external mutations after this check are outside the contract.
        let after = api.get(&workload.name).await.map_err(kube_error)?;
        if after.metadata.uid != controller.metadata.uid
            || after.metadata.resource_version != controller.metadata.resource_version
        {
            return Err(unsupported(
                "controller changed during membership inspection",
            ));
        }
        Ok(())
    }
}

fn validate_controller(
    controller: &DynamicObject,
    kind: &str,
    identity: &IdleMemberIdentity,
) -> KubernetesClientResult<()> {
    validate_metadata(&controller.metadata, identity)?;
    if controller
        .data
        .pointer("/spec/replicas")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
    {
        return Err(unsupported(
            "live workload must have exactly one desired replica",
        ));
    }
    if kind == "Deployment"
        && controller
            .data
            .pointer("/spec/strategy/type")
            .and_then(serde_json::Value::as_str)
            != Some("Recreate")
    {
        return Err(unsupported("Deployment must use Recreate strategy"));
    }
    Ok(())
}

fn validate_single_pod<'a>(
    pods: &'a [Pod],
    identity: &IdleMemberIdentity,
) -> KubernetesClientResult<&'a Pod> {
    let [pod] = pods else {
        return Err(unsupported(
            "expected exactly one observed pod, including terminating and unready members",
        ));
    };
    if identity.pod_uid.is_empty() || pod.metadata.uid.as_deref() != Some(identity.pod_uid.as_str())
    {
        return Err(unsupported("reporting pod UID is not the current member"));
    }
    validate_metadata(&pod.metadata, identity)?;
    let ready = pod.status.as_ref().is_some_and(|status| {
        status.phase.as_deref() == Some("Running")
            && status.conditions.as_ref().is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True")
            })
    });
    if !ready {
        return Err(unsupported("the reporting pod is not ready"));
    }
    Ok(pod)
}

fn validate_metadata(
    metadata: &ObjectMeta,
    identity: &IdleMemberIdentity,
) -> KubernetesClientResult<()> {
    if metadata.deletion_timestamp.is_some() {
        return Err(unsupported("a workload member is terminating"));
    }
    let matches = |values: &Option<BTreeMap<String, String>>, key: &str, expected: &str| {
        values
            .as_ref()
            .and_then(|values| values.get(key))
            .map(String::as_str)
            == Some(expected)
    };
    if !matches(
        &metadata.labels,
        LABEL_INSTANCE_ID,
        identity.instance_id.as_str(),
    ) || !matches(
        &metadata.labels,
        LABEL_INSTANCE_GENERATION,
        &identity.instance_generation.to_string(),
    ) || !matches(&metadata.labels, LABEL_MANAGED_BY, LABEL_MANAGED_BY_VALUE)
        || !matches(
            &metadata.annotations,
            ANNOTATION_MATERIALIZATION_ID,
            identity.materialization_id.as_str(),
        )
    {
        return Err(unsupported(
            "live member ownership or incarnation does not match the report",
        ));
    }
    Ok(())
}

fn controller_owner<'a>(
    metadata: &'a ObjectMeta,
    kind: &str,
) -> KubernetesClientResult<&'a k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference> {
    metadata
        .owner_references
        .as_ref()
        .and_then(|owners| {
            owners.iter().find(|owner| {
                owner.controller == Some(true)
                    && owner.kind == kind
                    && owner.api_version == "apps/v1"
            })
        })
        .ok_or_else(|| unsupported("pod has no supported controller owner"))
}

fn require_owner(metadata: &ObjectMeta, kind: &str, uid: &str) -> KubernetesClientResult<()> {
    if controller_owner(metadata, kind)?.uid != uid {
        return Err(unsupported("pod belongs to a different controller"));
    }
    Ok(())
}

fn unsupported(reason: &str) -> KubernetesClientError {
    KubernetesClientError::new(format!("automatic sleep membership check failed: {reason}"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        convert::Infallible,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytes::Bytes;
    use http::{Request, Response, StatusCode};
    use http_body_util::Full;
    use tower::service_fn;

    use super::*;
    use crate::ids::{Generation, InstanceId, MaterializationId};
    use crate::materializer::KubernetesMaterializerClient;
    use serde_json::json;

    const DEPLOYMENT_PATH: &str = "/apis/apps/v1/namespaces/apps/deployments/app";
    const STATEFULSET_PATH: &str = "/apis/apps/v1/namespaces/apps/statefulsets/app";
    const PODS_PATH: &str = "/api/v1/namespaces/apps/pods";
    const REPLICASET_PATH: &str = "/apis/apps/v1/namespaces/apps/replicasets/app-rs";

    type Reply = (&'static str, StatusCode, serde_json::Value);

    fn mock_client(replies: Vec<Reply>) -> (KubeMaterializerClient, Arc<Mutex<VecDeque<Reply>>>) {
        let remaining = Arc::new(Mutex::new(VecDeque::from(replies)));
        let queued = Arc::clone(&remaining);
        let service = service_fn(move |request: Request<kube::client::Body>| {
            let (path, status, body) = queued
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected Kubernetes call");
            assert_eq!(request.method(), http::Method::GET);
            assert_eq!(request.uri().path(), path);
            if path == PODS_PATH {
                assert_eq!(
                    request
                        .uri()
                        .query()
                        .map(|query| query.trim_start_matches('&')),
                    Some("labelSelector=sleepypods.io%2Finstance-id%3Dapp")
                );
            }
            async move {
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .body(Full::new(Bytes::from(body.to_string())))
                        .unwrap(),
                )
            }
        });
        (
            KubeMaterializerClient::new(kube::Client::new(service, "apps")),
            remaining,
        )
    }

    fn workload_ref(kind: &str) -> RenderedObjectRef {
        RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: kind.to_owned(),
            namespace: "apps".to_owned(),
            name: "app".to_owned(),
        }
    }

    fn controller_json(kind: &str) -> serde_json::Value {
        let mut result = json!({
            "apiVersion":"apps/v1", "kind":kind, "metadata":metadata(),
            "spec":{"replicas":1,"strategy":{"type":"Recreate"}},
        });
        result["metadata"]["uid"] = json!("controller-a");
        result["metadata"]["resourceVersion"] = json!("42");
        result
    }

    fn owner(kind: &str, name: &str, uid: &str) -> serde_json::Value {
        json!({ "apiVersion":"apps/v1", "kind":kind, "name":name, "uid":uid, "controller":true })
    }

    fn pod_list(kind: &str) -> serde_json::Value {
        let mut member = serde_json::to_value(pod()).unwrap();
        member["metadata"]["ownerReferences"] = if kind == "Deployment" {
            json!([owner("ReplicaSet", "app-rs", "rs-a")])
        } else {
            json!([owner("StatefulSet", "app", "controller-a")])
        };
        json!({ "apiVersion":"v1", "kind":"PodList", "metadata":{"resourceVersion":"43"}, "items":[member] })
    }

    fn replica_set() -> serde_json::Value {
        json!({ "apiVersion":"apps/v1", "kind":"ReplicaSet", "metadata": {
            "name":"app-rs", "uid":"rs-a", "ownerReferences":[owner("Deployment", "app", "controller-a")],
        }})
    }

    #[tokio::test]
    async fn production_membership_verifier_accepts_current_owned_deployment_and_statefulset() {
        for (kind, path) in [
            ("Deployment", DEPLOYMENT_PATH),
            ("StatefulSet", STATEFULSET_PATH),
        ] {
            let mut replies = vec![
                (path, StatusCode::OK, controller_json(kind)),
                (PODS_PATH, StatusCode::OK, pod_list(kind)),
            ];
            if kind == "Deployment" {
                replies.push((REPLICASET_PATH, StatusCode::OK, replica_set()));
            }
            replies.push((path, StatusCode::OK, controller_json(kind)));
            let (client, remaining) = mock_client(replies);
            client
                .verify_idle_member(&[workload_ref(kind)], &identity())
                .await
                .unwrap();
            assert!(remaining.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn production_membership_verifier_rejects_controller_replacement_and_scale_races() {
        for change in ["uid", "resourceVersion"] {
            let mut after = controller_json("StatefulSet");
            after["metadata"][change] = json!("changed");
            if change == "resourceVersion" {
                after["spec"]["replicas"] = json!(2);
            }
            let (client, remaining) = mock_client(vec![
                (
                    STATEFULSET_PATH,
                    StatusCode::OK,
                    controller_json("StatefulSet"),
                ),
                (PODS_PATH, StatusCode::OK, pod_list("StatefulSet")),
                (STATEFULSET_PATH, StatusCode::OK, after),
            ]);
            let error = client
                .verify_idle_member(&[workload_ref("StatefulSet")], &identity())
                .await
                .unwrap_err();
            assert!(error.to_string().contains("controller changed"), "{error}");
            assert!(remaining.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn production_membership_verifier_rejects_replaced_or_unowned_replicaset() {
        for change in ["uid", "owner", "terminating"] {
            let mut replica_set = replica_set();
            match change {
                "uid" => replica_set["metadata"]["uid"] = json!("replacement-rs"),
                "owner" => {
                    replica_set["metadata"]["ownerReferences"][0]["uid"] = json!("other-controller")
                }
                "terminating" => {
                    replica_set["metadata"]["deletionTimestamp"] = json!("2026-09-07T00:00:00Z")
                }
                _ => unreachable!(),
            }
            let (client, remaining) = mock_client(vec![
                (
                    DEPLOYMENT_PATH,
                    StatusCode::OK,
                    controller_json("Deployment"),
                ),
                (PODS_PATH, StatusCode::OK, pod_list("Deployment")),
                (REPLICASET_PATH, StatusCode::OK, replica_set),
            ]);
            assert!(
                client
                    .verify_idle_member(&[workload_ref("Deployment")], &identity())
                    .await
                    .is_err(),
                "{change}"
            );
            assert!(remaining.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn production_membership_verifier_rejects_missing_permissions_extra_controllers_and_stalls(
    ) {
        let (client, remaining) = mock_client(vec![]);
        assert!(client
            .verify_idle_member(
                &[workload_ref("Deployment"), workload_ref("StatefulSet")],
                &identity()
            )
            .await
            .is_err());
        assert!(remaining.lock().unwrap().is_empty());

        let (client, remaining) = mock_client(vec![
            (
                STATEFULSET_PATH,
                StatusCode::OK,
                controller_json("StatefulSet"),
            ),
            (
                PODS_PATH,
                StatusCode::FORBIDDEN,
                json!({
                    "apiVersion":"v1", "kind":"Status", "status":"Failure", "reason":"Forbidden", "message":"pods is forbidden", "code":403,
                }),
            ),
        ]);
        assert!(client
            .verify_idle_member(&[workload_ref("StatefulSet")], &identity())
            .await
            .unwrap_err()
            .to_string()
            .contains("forbidden"));
        assert!(remaining.lock().unwrap().is_empty());

        let service = service_fn(|_: Request<kube::client::Body>| {
            std::future::pending::<Result<Response<Full<Bytes>>, Infallible>>()
        });
        let config = super::super::KubeMaterializerClientConfig {
            readiness_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let client =
            KubeMaterializerClient::with_config(kube::Client::new(service, "apps"), config);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            client.verify_idle_member(&[workload_ref("StatefulSet")], &identity()),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error}");
    }

    fn identity() -> IdleMemberIdentity {
        IdleMemberIdentity {
            instance_id: InstanceId::new("app").unwrap(),
            instance_generation: Generation::new(7),
            materialization_id: MaterializationId::new("app:cluster:apps").unwrap(),
            pod_uid: "pod-a".to_owned(),
        }
    }

    fn metadata() -> serde_json::Value {
        json!({"uid":"pod-a", "labels": {
            LABEL_INSTANCE_ID: "app", LABEL_INSTANCE_GENERATION: "7", LABEL_MANAGED_BY: LABEL_MANAGED_BY_VALUE,
        }, "annotations": { ANNOTATION_MATERIALIZATION_ID: "app:cluster:apps" }})
    }

    fn pod() -> Pod {
        serde_json::from_value(json!({"metadata":metadata(), "status": {
            "phase":"Running", "conditions":[{"type":"Ready", "status":"True"}],
        }}))
        .unwrap()
    }

    #[test]
    fn membership_requires_exactly_one_ready_current_pod() {
        let identity = identity();
        assert!(validate_single_pod(&[pod()], &identity).is_ok());
        assert!(validate_single_pod(&[], &identity).is_err());
        assert!(validate_single_pod(&[pod(), pod()], &identity).is_err());
        let mut unready = pod();
        unready.status = None;
        assert!(validate_single_pod(&[unready.clone()], &identity).is_err());
        assert!(validate_single_pod(&[pod(), unready], &identity).is_err());
        let mut terminating = pod();
        terminating.metadata.deletion_timestamp =
            Some(serde_json::from_value(json!("2026-09-05T00:00:00Z")).unwrap());
        assert!(validate_single_pod(&[terminating.clone()], &identity).is_err());
        assert!(validate_single_pod(&[pod(), terminating], &identity).is_err());
    }

    #[test]
    fn membership_rejects_stale_uid_generation_and_projection() {
        let mut identity = identity();
        identity.pod_uid = "deleted-pod".to_owned();
        assert!(validate_single_pod(&[pod()], &identity).is_err());
        identity = super::tests::identity();
        identity.instance_generation = Generation::new(6);
        assert!(validate_single_pod(&[pod()], &identity).is_err());
        identity = super::tests::identity();
        identity.materialization_id = MaterializationId::new("new-projection").unwrap();
        assert!(validate_single_pod(&[pod()], &identity).is_err());
    }

    #[test]
    fn membership_rejects_controller_scale_and_rolling_updates() {
        for kind in ["Deployment", "StatefulSet"] {
            let mut controller: DynamicObject = serde_json::from_value(json!({
                "apiVersion":"apps/v1", "kind":kind, "metadata":metadata(),
                "spec":{"replicas":1,"strategy":{"type":"Recreate"}},
            }))
            .unwrap();
            assert!(validate_controller(&controller, kind, &identity()).is_ok());
            for replicas in [0, 2] {
                controller.data["spec"]["replicas"] = json!(replicas);
                assert!(validate_controller(&controller, kind, &identity()).is_err());
            }
            controller.data["spec"]["replicas"] = json!(1);
            if kind == "Deployment" {
                controller.data["spec"]["strategy"]["type"] = json!("RollingUpdate");
                assert!(validate_controller(&controller, kind, &identity()).is_err());
            }
        }
    }
}
