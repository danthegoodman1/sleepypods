#[cfg(test)]
mod conditional_tests;

mod idle_membership;

use std::time::Duration;

use k8s_openapi::api::{
    core::v1::{PersistentVolumeClaim, Service},
    discovery::v1::EndpointSlice,
};
use kube::{
    api::{
        Api, ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams,
        Preconditions, WatchEvent, WatchParams,
    },
    Client, Error as KubeError,
};
use tokio::time::timeout;
use tonic::codegen::tokio_stream::StreamExt;

use crate::{
    manifest::KubernetesObject,
    materialization::{BackendEndpoint, RenderedObjectRef},
    materializer::{
        is_workload_ref, KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializerClient,
    },
    projection::{LiveObjectMetadata, ProjectionObjectInspection, ProjectionReadinessInspection},
};

const DEFAULT_FIELD_MANAGER: &str = "sleepypods-control-plane";
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";
const BACKEND_SCHEME_ANNOTATION: &str = "sleepypods.io/backend-scheme";
/// The API server ends each readiness watch after this long, which bounds how
/// long a replaced Service goes unread.
const WATCH_ROUND_SECONDS: u32 = 30;
const WATCH_ROUND_PAUSE: Duration = Duration::from_millis(200);

#[derive(Clone)]
pub struct KubeMaterializerClient {
    client: Client,
    config: KubeMaterializerClientConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KubeMaterializerClientConfig {
    pub(crate) field_manager: String,
    pub(crate) backend_scheme: String,
    /// Bounds one mutation or short read.
    pub(crate) request_timeout: Duration,
    /// Bounds one wait for a ready endpoint.
    pub(crate) readiness_timeout: Duration,
}

impl KubeMaterializerClient {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            config: KubeMaterializerClientConfig::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_config(client: Client, config: KubeMaterializerClientConfig) -> Self {
        Self { client, config }
    }

    pub async fn try_default() -> Result<Self, KubernetesClientError> {
        Client::try_default()
            .await
            .map(Self::new)
            .map_err(kube_error)
    }

    fn dynamic_api(
        &self,
        object: &RenderedObjectRef,
    ) -> KubernetesClientResult<Api<DynamicObject>> {
        let resource = api_resource_for_ref(object)?;
        Ok(if object.namespace.is_empty() {
            Api::all_with(self.client.clone(), &resource)
        } else {
            Api::namespaced_with(self.client.clone(), &object.namespace, &resource)
        })
    }
}

impl KubeMaterializerClient {
    /// Returns once an EndpointSlice the Service owns has a ready endpoint.
    /// Each round reads the Service and its slices, then follows the slices by
    /// watch from that list's resourceVersion. A round ends when the watch
    /// does, so a replaced Service or an expired resourceVersion is read again.
    async fn watch_for_ready_endpoint(
        &self,
        service_ref: &RenderedObjectRef,
    ) -> KubernetesClientResult<BackendEndpoint> {
        let services: Api<Service> = Api::namespaced(self.client.clone(), &service_ref.namespace);
        let endpoint_slices: Api<EndpointSlice> =
            Api::namespaced(self.client.clone(), &service_ref.namespace);
        let selector = format!("{SERVICE_NAME_LABEL}={}", service_ref.name);
        loop {
            let service = services.get(&service_ref.name).await.map_err(kube_error)?;
            let backend = backend_endpoint_for_service(&service, &self.config)?;
            let listed = endpoint_slices
                .list(&ListParams::default().labels(&selector))
                .await
                .map_err(kube_error)?;
            if listed
                .iter()
                .any(|slice| endpoint_slice_has_ready_endpoint(&service, slice))
            {
                return Ok(backend);
            }
            let version = listed.metadata.resource_version.unwrap_or_default();
            let watch = endpoint_slices
                .watch(
                    &WatchParams::default()
                        .labels(&selector)
                        .timeout(WATCH_ROUND_SECONDS),
                    &version,
                )
                .await
                .map_err(kube_error)?;
            let mut watch = std::pin::pin!(watch);
            while let Some(event) = watch.next().await {
                match event {
                    Ok(WatchEvent::Added(slice) | WatchEvent::Modified(slice))
                        if endpoint_slice_has_ready_endpoint(&service, &slice) =>
                    {
                        return Ok(backend);
                    }
                    Ok(
                        WatchEvent::Added(_) | WatchEvent::Modified(_) | WatchEvent::Bookmark(_),
                    ) => {}
                    // A deleted slice, an expired version or a broken stream:
                    // read the current state again.
                    Ok(WatchEvent::Deleted(_) | WatchEvent::Error(_)) | Err(_) => break,
                }
            }
            // An API server that ends watches early is read at this pace.
            tokio::time::sleep(WATCH_ROUND_PAUSE).await;
        }
    }
}

impl KubernetesMaterializerClient for KubeMaterializerClient {
    fn verify_idle_member<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        identity: &'a crate::materializer::IdleMemberIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            timeout(
                self.config.readiness_timeout,
                self.verify_idle_member_snapshot(objects, identity),
            )
            .await
            .map_err(|_| KubernetesClientError::transient("idle membership inspection timed out"))?
        })
    }

    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a crate::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let object_ref = object.object_ref();
            let api = self.dynamic_api(&object_ref)?;
            let mut body = object.to_kubernetes_json();
            let operation = async {
                match precondition {
                    None => {
                        if is_workload_ref(&object_ref) {
                            body["spec"]["replicas"] = 0.into();
                        }
                        let object: DynamicObject = serde_json::from_value(body)
                            .map_err(|error| KubernetesClientError::new(error.to_string()))?;
                        api.create(
                            &PostParams {
                                field_manager: Some(self.config.field_manager.clone()),
                                ..PostParams::default()
                            },
                            &object,
                        )
                        .await
                        .map(|_| ())
                        .map_err(mutation_error)
                    }
                    Some(identity) => {
                        validate_identity(identity)?;
                        body["metadata"]["uid"] = identity.uid.clone().into();
                        body["metadata"]["resourceVersion"] =
                            identity.resource_version.clone().into();
                        let params = PatchParams {
                            field_manager: Some(self.config.field_manager.clone()),
                            ..PatchParams::default()
                        };
                        api.patch(&object_ref.name, &params, &Patch::Merge(&body))
                            .await
                            .map(|_| ())
                            .map_err(mutation_error)
                    }
                }
            };
            timeout(self.config.request_timeout, operation)
                .await
                .map_err(|_| {
                    KubernetesClientError::stale(
                        "Kubernetes mutation timed out; its outcome is unknown",
                    )
                })?
        })
    }

    fn activate_workload<'a>(
        &'a self,
        object: &'a KubernetesObject,
        identity: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            validate_identity(identity)?;
            let object_ref = object.object_ref();
            let api = self.dynamic_api(&object_ref)?;
            let body = serde_json::json!({
                "metadata": { "uid": identity.uid },
                "spec": { "replicas": object.to_kubernetes_json()["spec"]["replicas"] },
            });
            let params = PatchParams {
                field_manager: Some(self.config.field_manager.clone()),
                ..PatchParams::default()
            };
            let operation = async {
                api.patch(&object_ref.name, &params, &Patch::Merge(&body))
                    .await
                    .map(|_| ())
                    .map_err(|error| match &error {
                        // The API server refuses a UID that differs from the
                        // live object's as an invalid change to metadata.
                        KubeError::Api(status) if status.code == 422 => {
                            KubernetesClientError::stale(error.to_string())
                        }
                        _ => mutation_error(error),
                    })
            };
            timeout(self.config.request_timeout, operation)
                .await
                .map_err(|_| {
                    KubernetesClientError::stale(
                        "Kubernetes mutation timed out; its outcome is unknown",
                    )
                })?
        })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            validate_identity(precondition)?;
            let api = self.dynamic_api(object)?;
            if object.kind == "PersistentVolume" {
                let live = match timeout(self.config.request_timeout, api.get(&object.name))
                    .await
                    .map_err(|_| {
                        KubernetesClientError::transient("PV retention inspection timed out")
                    })? {
                    Ok(live) => live,
                    Err(error) if is_not_found(&error) => return Ok(()),
                    Err(error) => return Err(kube_error(error)),
                };
                if live.metadata.uid.as_deref() != Some(precondition.uid.as_str())
                    || live.metadata.resource_version.as_deref()
                        != Some(precondition.resource_version.as_str())
                {
                    return Err(KubernetesClientError::transient(
                        "PV changed since ownership/retention inspection",
                    ));
                }
                if live
                    .data
                    .pointer("/spec/persistentVolumeReclaimPolicy")
                    .and_then(serde_json::Value::as_str)
                    != Some("Retain")
                {
                    return Err(KubernetesClientError::new("managed static PV requires Retain before automatic cleanup; operator correction required"));
                }
            }
            let params = DeleteParams {
                preconditions: Some(Preconditions {
                    uid: Some(precondition.uid.clone()),
                    resource_version: Some(precondition.resource_version.clone()),
                }),
                ..DeleteParams::foreground()
            };
            match timeout(
                self.config.request_timeout,
                api.delete(&object.name, &params),
            )
            .await
            .map_err(|_| {
                KubernetesClientError::stale("Kubernetes delete timed out; its outcome is unknown")
            })? {
                Ok(_) => Ok(()),
                Err(error) if is_not_found(&error) => Ok(()),
                Err(error) => Err(mutation_error(error)),
            }
        })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async move {
            let service_ref = rendered_service_ref(objects)?;
            timeout(
                self.config.readiness_timeout,
                self.watch_for_ready_endpoint(service_ref),
            )
            .await
            .map_err(|_| {
                KubernetesClientError::transient(format!(
                    "timed out waiting for a ready endpoint of Service {}/{}",
                    service_ref.namespace, service_ref.name
                ))
            })?
        })
    }

    fn ready_pod_backend<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<Option<BackendEndpoint>>> {
        Box::pin(async move {
            timeout(self.config.request_timeout, async move {
                let service_ref = rendered_service_ref(objects)?;
                let services: Api<Service> =
                    Api::namespaced(self.client.clone(), &service_ref.namespace);
                let endpoint_slices: Api<EndpointSlice> =
                    Api::namespaced(self.client.clone(), &service_ref.namespace);
                let service = services.get(&service_ref.name).await.map_err(kube_error)?;
                let selector = format!("{SERVICE_NAME_LABEL}={}", service_ref.name);
                let slices = endpoint_slices
                    .list(&ListParams::default().labels(&selector))
                    .await
                    .map_err(kube_error)?;
                let backend = slices
                    .iter()
                    .find_map(|slice| ready_pod_backend_for_slice(&service, slice, &self.config));
                Ok(backend)
            })
            .await
            .map_err(|_| KubernetesClientError::transient("Kubernetes read timed out"))?
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            let api = self.dynamic_api(object)?;
            match timeout(self.config.request_timeout, api.get(&object.name))
                .await
                .map_err(|_| KubernetesClientError::transient("Kubernetes inspection timed out"))?
            {
                Ok(live) => Ok(ProjectionObjectInspection::Present(LiveObjectMetadata {
                    persistent_volume_reclaim_policy: live
                        .data
                        .pointer("/spec/persistentVolumeReclaimPolicy")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    identity: crate::projection::LiveObjectIdentity {
                        uid: live.metadata.uid.ok_or_else(|| {
                            KubernetesClientError::new("live Kubernetes object has no UID")
                        })?,
                        resource_version: live.metadata.resource_version.ok_or_else(|| {
                            KubernetesClientError::new(
                                "live Kubernetes object has no resourceVersion",
                            )
                        })?,
                    },
                    labels: live.metadata.labels.unwrap_or_default(),
                    annotations: live.metadata.annotations.unwrap_or_default(),
                    deleting: live.metadata.deletion_timestamp.is_some(),
                    finalizers: live.metadata.finalizers.unwrap_or_default(),
                })),
                Err(error) if is_not_found(&error) => Ok(ProjectionObjectInspection::Missing),
                Err(error) => Err(kube_error(error)),
            }
        })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            timeout(self.config.request_timeout, async {
                let pvs: Api<k8s_openapi::api::core::v1::PersistentVolume> =
                    Api::all(self.client.clone());
                for object in objects
                    .iter()
                    .filter(|object| object.kind == "PersistentVolumeClaim")
                {
                    let pvcs: Api<PersistentVolumeClaim> =
                        Api::namespaced(self.client.clone(), &object.namespace);
                    let pvc = match pvcs.get(&object.name).await {
                        Ok(pvc) => pvc,
                        Err(error) if is_not_found(&error) => continue,
                        Err(error) => return Err(kube_error(error)),
                    };
                    let volume_name = pvc
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.volume_name.as_deref())
                        .filter(|name| !name.is_empty())
                        .ok_or_else(|| {
                            KubernetesClientError::new(
                            "PVC has no proven static volume binding; automatic cleanup refused",
                        )
                        })?;
                    if !objects.iter().any(|recorded| {
                        recorded.kind == "PersistentVolume"
                            && recorded.name == volume_name
                            && recorded.namespace.is_empty()
                    }) {
                        return Err(KubernetesClientError::new(
                            "PVC binds a PV outside recorded inventory; automatic cleanup refused",
                        ));
                    }
                    let pv = pvs.get(volume_name).await.map_err(kube_error)?;
                    if pv
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.persistent_volume_reclaim_policy.as_deref())
                        != Some("Retain")
                    {
                        return Err(KubernetesClientError::new(
                            "PVC backing PV requires Retain before any automatic cleanup",
                        ));
                    }
                }
                Ok(())
            })
            .await
            .map_err(|_| {
                KubernetesClientError::transient("retained static binding inspection timed out")
            })?
        })
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            timeout(self.config.request_timeout, async {
                let namespaces = objects
                    .iter()
                    .map(|object| object.namespace.as_str())
                    .filter(|namespace| !namespace.is_empty())
                    .collect::<std::collections::BTreeSet<_>>();
                let selector = format!("{}={instance_id}", crate::manifest::LABEL_INSTANCE_ID);
                let params = ListParams::default().labels(&selector);
                for namespace in namespaces {
                    let pods: Api<k8s_openapi::api::core::v1::Pod> =
                        Api::namespaced(self.client.clone(), namespace);
                    if !pods
                        .list(&params)
                        .await
                        .map_err(kube_error)?
                        .items
                        .is_empty()
                    {
                        return Err(KubernetesClientError::transient(
                            "projection Pods still exist, including terminating members",
                        ));
                    }
                    let replica_sets: Api<k8s_openapi::api::apps::v1::ReplicaSet> =
                        Api::namespaced(self.client.clone(), namespace);
                    if !replica_sets
                        .list(&params)
                        .await
                        .map_err(kube_error)?
                        .items
                        .is_empty()
                    {
                        return Err(KubernetesClientError::transient(
                            "projection ReplicaSets still exist",
                        ));
                    }
                }
                Ok(())
            })
            .await
            .map_err(|_| {
                KubernetesClientError::transient("descendant absence inspection timed out")
            })?
        })
    }

    fn inspect_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>> {
        Box::pin(async move {
            timeout(self.config.request_timeout, async move {
                let service_ref = match rendered_service_ref(objects) {
                    Ok(service_ref) => service_ref,
                    Err(_) => return Ok(ProjectionReadinessInspection::NotObserved),
                };
                let services: Api<Service> =
                    Api::namespaced(self.client.clone(), &service_ref.namespace);
                let service = match services.get(&service_ref.name).await {
                    Ok(service) => service,
                    Err(error) if is_not_found(&error) => {
                        return Ok(ProjectionReadinessInspection::Unready {
                            reason: "service_missing".to_owned(),
                        });
                    }
                    Err(error) => return Err(kube_error(error)),
                };

                let endpoint_slices: Api<EndpointSlice> =
                    Api::namespaced(self.client.clone(), &service_ref.namespace);
                let selector = format!("{SERVICE_NAME_LABEL}={}", service_ref.name);
                let slices = endpoint_slices
                    .list(&ListParams::default().labels(&selector))
                    .await
                    .map_err(kube_error)?;

                readiness_inspection_for_service(&service, &slices.items, &self.config)
            })
            .await
            .map_err(|_| KubernetesClientError::transient("Kubernetes read/wait timed out"))?
        })
    }
}

impl Default for KubeMaterializerClientConfig {
    fn default() -> Self {
        Self {
            field_manager: DEFAULT_FIELD_MANAGER.to_owned(),
            backend_scheme: "http".to_owned(),
            request_timeout: Duration::from_secs(10),
            readiness_timeout: Duration::from_secs(120),
        }
    }
}

fn api_resource_for_ref(object: &RenderedObjectRef) -> KubernetesClientResult<ApiResource> {
    let (group, version) = split_api_version(&object.api_version);
    let plural = match (
        object.api_version.as_str(),
        object.kind.as_str(),
        object.namespace.is_empty(),
    ) {
        ("apps/v1", "Deployment", false) => "deployments",
        ("apps/v1", "StatefulSet", false) => "statefulsets",
        ("v1", "Service", false) => "services",
        ("v1", "Secret", false) => "secrets",
        ("v1", "PersistentVolume", true) => "persistentvolumes",
        ("v1", "PersistentVolumeClaim", false) => "persistentvolumeclaims",
        _ => {
            return Err(KubernetesClientError::new(format!(
                "unsupported rendered Kubernetes object {} {} {}/{}",
                object.api_version, object.kind, object.namespace, object.name
            )));
        }
    };

    Ok(ApiResource {
        group: group.to_owned(),
        version: version.to_owned(),
        api_version: object.api_version.clone(),
        kind: object.kind.clone(),
        plural: plural.to_owned(),
    })
}

fn backend_endpoint_for_service(
    service: &Service,
    config: &KubeMaterializerClientConfig,
) -> KubernetesClientResult<BackendEndpoint> {
    let backend_scheme = backend_scheme_for_service(service, config)?;

    let name = service
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| KubernetesClientError::new("live Service is missing metadata.name"))?;
    let namespace =
        service.metadata.namespace.as_deref().ok_or_else(|| {
            KubernetesClientError::new("live Service is missing metadata.namespace")
        })?;
    let port = service_port(service)?;
    // The ClusterIP is fixed for the Service's life, so a proxy connects
    // without a DNS lookup. A headless Service has only its DNS name.
    let host = match service
        .spec
        .as_ref()
        .and_then(|spec| spec.cluster_ip.as_deref())
        .and_then(|ip| ip.parse::<std::net::IpAddr>().ok())
    {
        Some(std::net::IpAddr::V4(ip)) => ip.to_string(),
        Some(std::net::IpAddr::V6(ip)) => format!("[{ip}]"),
        None => format!("{name}.{namespace}.svc.cluster.local"),
    };

    BackendEndpoint::new(format!("{backend_scheme}://{host}:{port}"))
        .map_err(|error| KubernetesClientError::new(error.to_string()))
}

fn backend_scheme_for_service<'a>(
    service: &'a Service,
    config: &'a KubeMaterializerClientConfig,
) -> KubernetesClientResult<&'a str> {
    if let Some(annotation) = service
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(BACKEND_SCHEME_ANNOTATION))
        .map(String::as_str)
    {
        if !is_valid_uri_scheme(annotation) {
            return Err(KubernetesClientError::new(format!(
                "Service backend scheme annotation {BACKEND_SCHEME_ANNOTATION} is invalid"
            )));
        }

        return Ok(annotation);
    }

    if !is_valid_uri_scheme(&config.backend_scheme) {
        return Err(KubernetesClientError::new(
            "Kubernetes materializer backend scheme is invalid",
        ));
    }

    Ok(&config.backend_scheme)
}

/// The address of a ready endpoint in a slice the Service owns, on the slice
/// port that backs the Service's first port.
fn ready_pod_backend_for_slice(
    service: &Service,
    slice: &EndpointSlice,
    config: &KubeMaterializerClientConfig,
) -> Option<BackendEndpoint> {
    if !endpoint_slice_has_ready_endpoint(service, slice) {
        return None;
    }
    let address = slice
        .endpoints
        .iter()
        .filter(|endpoint| endpoint_is_ready(endpoint))
        .find_map(|endpoint| endpoint.addresses.first())?
        .parse::<std::net::IpAddr>()
        .ok()?;
    let service_port = service.spec.as_ref()?.ports.as_ref()?.first()?;
    let ports = slice.ports.as_ref()?;
    let port = ports
        .iter()
        .find(|port| {
            port.name.as_deref().unwrap_or_default()
                == service_port.name.as_deref().unwrap_or_default()
        })
        .or(match ports.as_slice() {
            [only] => Some(only),
            _ => None,
        })?
        .port?;
    let scheme = backend_scheme_for_service(service, config).ok()?;
    BackendEndpoint::new(format!(
        "{scheme}://{}",
        std::net::SocketAddr::new(address, port as u16)
    ))
    .ok()
}

fn endpoint_is_ready(endpoint: &k8s_openapi::api::discovery::v1::Endpoint) -> bool {
    !endpoint.addresses.is_empty()
        && endpoint
            .conditions
            .as_ref()
            .and_then(|conditions| conditions.terminating)
            != Some(true)
        && endpoint
            .conditions
            .as_ref()
            .and_then(|conditions| conditions.ready)
            .unwrap_or(true)
}

fn endpoint_slice_has_ready_endpoint(service: &Service, slice: &EndpointSlice) -> bool {
    let Some(uid) = service
        .metadata
        .uid
        .as_deref()
        .filter(|uid| !uid.is_empty())
    else {
        return false;
    };
    if service.metadata.deletion_timestamp.is_some()
        || slice.metadata.deletion_timestamp.is_some()
        || !slice
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|owners| {
                owners.iter().any(|owner| {
                    owner.api_version == "v1"
                        && owner.kind == "Service"
                        && owner.uid == uid
                        && Some(owner.name.as_str()) == service.metadata.name.as_deref()
                        && owner.controller == Some(true)
                })
            })
    {
        return false;
    }
    slice.endpoints.iter().any(endpoint_is_ready)
}

fn readiness_inspection_for_service(
    service: &Service,
    slices: &[EndpointSlice],
    config: &KubeMaterializerClientConfig,
) -> KubernetesClientResult<ProjectionReadinessInspection> {
    let backend = match backend_endpoint_for_service(service, config) {
        Ok(backend) => backend,
        Err(_) => {
            return Ok(ProjectionReadinessInspection::Unready {
                reason: "backend_endpoint_invalid".to_owned(),
            });
        }
    };
    if slices
        .iter()
        .any(|slice| endpoint_slice_has_ready_endpoint(service, slice))
    {
        Ok(ProjectionReadinessInspection::Ready(backend))
    } else {
        Ok(ProjectionReadinessInspection::Unready {
            reason: "no_ready_endpoints".to_owned(),
        })
    }
}

fn is_valid_uri_scheme(scheme: &str) -> bool {
    let mut chars = scheme.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}

fn split_api_version(api_version: &str) -> (&str, &str) {
    api_version.rsplit_once('/').unwrap_or(("", api_version))
}

fn rendered_service_ref(
    objects: &[RenderedObjectRef],
) -> KubernetesClientResult<&RenderedObjectRef> {
    objects
        .iter()
        .find(|object| object.api_version == "v1" && object.kind == "Service")
        .ok_or_else(|| {
            KubernetesClientError::new(
                "rendered Kubernetes objects do not include a Service for readiness",
            )
        })
}

fn service_port(service: &Service) -> KubernetesClientResult<i32> {
    let ports = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .ok_or_else(|| KubernetesClientError::new("live Service has no spec.ports"))?;
    let port = ports
        .first()
        .ok_or_else(|| KubernetesClientError::new("live Service has no ports"))?
        .port;
    if port <= 0 {
        return Err(KubernetesClientError::new(format!(
            "live Service port {port} is not positive"
        )));
    }
    Ok(port)
}

fn is_not_found(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(status) if status.is_not_found())
}

/// Classifies a failed read.
fn kube_error(error: KubeError) -> KubernetesClientError {
    let message = error.to_string();
    if is_transient_kube_error(&error) {
        KubernetesClientError::transient(message)
    } else {
        KubernetesClientError::new(message)
    }
}

fn is_transient_kube_error(error: &KubeError) -> bool {
    match error {
        KubeError::Api(status) => {
            status.code == 409 || status.code == 429 || (500..=599).contains(&status.code)
        }
        KubeError::HyperError(_) | KubeError::Service(_) | KubeError::ReadEvents(_) => true,
        _ => false,
    }
}

fn validate_identity(
    identity: &crate::projection::LiveObjectIdentity,
) -> KubernetesClientResult<()> {
    if identity.uid.is_empty() || identity.resource_version.is_empty() {
        return Err(KubernetesClientError::new(
            "conditional mutation requires UID and resourceVersion",
        ));
    }
    Ok(())
}

/// Classifies a failed mutation. A 409 means the object exists or changed
/// since it was inspected. A 5xx or a broken connection leaves the outcome
/// unknown. Either way the caller inspects the object and decides again; every
/// request it may send is conditional or creates nothing that runs.
fn mutation_error(error: KubeError) -> KubernetesClientError {
    match &error {
        KubeError::Api(status) if status.code == 429 => {
            KubernetesClientError::transient(error.to_string())
        }
        KubeError::Api(status) if status.code == 409 || status.code >= 500 => {
            KubernetesClientError::stale(error.to_string())
        }
        KubeError::Api(_)
        | KubeError::BuildRequest(_)
        | KubeError::HttpError(_)
        | KubeError::Auth(_) => KubernetesClientError::new(error.to_string()),
        _ => KubernetesClientError::stale(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::{
        api::{
            core::v1::{Service, ServicePort, ServiceSpec},
            discovery::v1::{Endpoint, EndpointConditions, EndpointSlice},
        },
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };
    use kube::{core::Status, Error as KubeError};

    use crate::{
        materialization::{BackendEndpoint, RenderedObjectRef},
        projection::ProjectionReadinessInspection,
    };

    use super::{
        api_resource_for_ref, backend_endpoint_for_service, endpoint_slice_has_ready_endpoint,
        is_not_found, is_valid_uri_scheme, kube_error, mutation_error,
        readiness_inspection_for_service, rendered_service_ref, KubeMaterializerClientConfig,
    };

    #[test]
    fn maps_rendered_refs_to_api_resources() {
        let cases = [
            (
                object_ref("apps/v1", "Deployment", "apps", "app-a"),
                ("apps", "v1", "apps/v1", "Deployment", "deployments"),
            ),
            (
                object_ref("apps/v1", "StatefulSet", "data", "db-a"),
                ("apps", "v1", "apps/v1", "StatefulSet", "statefulsets"),
            ),
            (
                object_ref("v1", "Service", "apps", "svc-a"),
                ("", "v1", "v1", "Service", "services"),
            ),
            (
                object_ref("v1", "PersistentVolume", "", "pv-a"),
                ("", "v1", "v1", "PersistentVolume", "persistentvolumes"),
            ),
            (
                object_ref("v1", "PersistentVolumeClaim", "data", "pvc-a"),
                (
                    "",
                    "v1",
                    "v1",
                    "PersistentVolumeClaim",
                    "persistentvolumeclaims",
                ),
            ),
        ];

        for (object, expected) in cases {
            let resource = api_resource_for_ref(&object).expect("supported API resource");

            assert_eq!(
                (
                    resource.group.as_str(),
                    resource.version.as_str(),
                    resource.api_version.as_str(),
                    resource.kind.as_str(),
                    resource.plural.as_str(),
                ),
                expected
            );
        }
    }

    #[test]
    fn rejects_namespaced_persistent_volume_ref() {
        let error = api_resource_for_ref(&object_ref("v1", "PersistentVolume", "data", "pv-a"))
            .expect_err("PV is cluster scoped");

        assert!(error
            .to_string()
            .contains("unsupported rendered Kubernetes object"));
    }

    #[test]
    fn default_config_uses_http_service_backend_uri() {
        let service = service("api", "apps", [80, 8080]);
        let config = KubeMaterializerClientConfig::default();

        let backend =
            backend_endpoint_for_service(&service, &config).expect("service backend endpoint");

        assert_eq!(backend.uri(), "http://api.apps.svc.cluster.local:80");
    }

    #[test]
    fn configurable_scheme_changes_service_backend_uri() {
        let service = service("postgres", "data", [5432]);
        let config = KubeMaterializerClientConfig {
            backend_scheme: "tcp".to_owned(),
            ..KubeMaterializerClientConfig::default()
        };

        let backend =
            backend_endpoint_for_service(&service, &config).expect("service backend endpoint");

        assert_eq!(backend.uri(), "tcp://postgres.data.svc.cluster.local:5432");
    }

    #[test]
    fn service_annotation_overrides_default_backend_scheme() {
        let mut service = service("passthrough", "apps", [9443]);
        service.metadata.annotations = Some(BTreeMap::from([(
            "sleepypods.io/backend-scheme".to_owned(),
            "tcp".to_owned(),
        )]));
        let config = KubeMaterializerClientConfig::default();

        let backend =
            backend_endpoint_for_service(&service, &config).expect("service backend endpoint");

        assert_eq!(
            backend.uri(),
            "tcp://passthrough.apps.svc.cluster.local:9443"
        );
    }

    #[test]
    fn endpoint_slice_ready_defaults_match_kubernetes_semantics() {
        let service = service("api", "apps", [80]);
        assert!(endpoint_slice_has_ready_endpoint(
            &service,
            &endpoint_slice([None])
        ));
        assert!(endpoint_slice_has_ready_endpoint(
            &service,
            &endpoint_slice([Some(true)])
        ));
        assert!(!endpoint_slice_has_ready_endpoint(
            &service,
            &endpoint_slice([Some(false)])
        ));
    }

    #[test]
    fn readiness_inspection_reports_ready_endpoint_without_polling() {
        let service = service("api", "apps", [80]);
        let config = KubeMaterializerClientConfig::default();

        let inspection =
            readiness_inspection_for_service(&service, &[endpoint_slice([Some(true)])], &config)
                .expect("readiness inspection succeeds");

        assert_eq!(
            inspection,
            ProjectionReadinessInspection::Ready(
                BackendEndpoint::new("http://api.apps.svc.cluster.local:80")
                    .expect("backend endpoint")
            )
        );
    }

    #[test]
    fn readiness_inspection_reports_absent_ready_endpoints() {
        let service = service("api", "apps", [80]);
        let config = KubeMaterializerClientConfig::default();

        let inspection = readiness_inspection_for_service(
            &service,
            &[endpoint_slice([Some(false)]), endpoint_slice([])],
            &config,
        )
        .expect("readiness inspection succeeds");

        assert_eq!(
            inspection,
            ProjectionReadinessInspection::Unready {
                reason: "no_ready_endpoints".to_owned()
            }
        );
    }

    #[test]
    fn readiness_inspection_reports_invalid_service_backend_as_unready() {
        let service = service("api", "apps", [0]);
        let config = KubeMaterializerClientConfig::default();

        let inspection =
            readiness_inspection_for_service(&service, &[endpoint_slice([Some(true)])], &config)
                .expect("readiness inspection remains report-only");

        assert_eq!(
            inspection,
            ProjectionReadinessInspection::Unready {
                reason: "backend_endpoint_invalid".to_owned()
            }
        );
    }

    #[test]
    fn backend_uri_uses_the_service_cluster_ip() {
        let config = KubeMaterializerClientConfig::default();
        for (cluster_ip, expected) in [
            (Some("10.96.0.10"), "http://10.96.0.10:80"),
            (Some("fd00::10"), "http://[fd00::10]:80"),
            // A headless Service and one not yet allocated have only a name.
            (Some("None"), "http://api.apps.svc.cluster.local:80"),
            (None, "http://api.apps.svc.cluster.local:80"),
        ] {
            let mut service = service("api", "apps", [80]);
            service.spec.as_mut().unwrap().cluster_ip = cluster_ip.map(str::to_owned);

            let backend =
                backend_endpoint_for_service(&service, &config).expect("service backend endpoint");

            assert_eq!(backend.uri(), expected);
        }
    }

    #[test]
    fn accepts_uri_schemes() {
        for scheme in ["http", "tcp", "h2c+unix"] {
            assert!(is_valid_uri_scheme(scheme));
        }
        for scheme in ["", " ", "1http", "http://", "tcp:"] {
            assert!(!is_valid_uri_scheme(scheme));
        }
    }

    #[test]
    fn delete_treats_not_found_as_success() {
        let error = KubeError::Api(Box::new(Status {
            code: 404,
            reason: "NotFound".to_owned(),
            ..Status::default()
        }));

        assert!(is_not_found(&error));
    }

    #[test]
    fn kube_conflict_errors_are_retryable_materializer_errors() {
        let error = kube_error(KubeError::Api(Box::new(Status {
            code: 409,
            reason: "Conflict".to_owned(),
            message: "resource version conflict".to_owned(),
            ..Status::default()
        })));

        assert!(error.is_retryable());
        assert!(error.to_string().contains("resource version conflict"));
    }

    #[test]
    fn kube_invalid_errors_are_permanent_materializer_errors() {
        let error = kube_error(KubeError::Api(Box::new(Status {
            code: 422,
            reason: "Invalid".to_owned(),
            message: "manifest is invalid".to_owned(),
            ..Status::default()
        })));

        assert!(!error.is_retryable());
        assert!(error.to_string().contains("manifest is invalid"));
    }

    #[test]
    fn mutation_failures_say_whether_to_resend_inspect_or_stop() {
        let api = |code: u16, reason: &str| {
            mutation_error(KubeError::Api(Box::new(Status {
                code,
                reason: reason.to_owned(),
                ..Status::default()
            })))
        };

        // The object exists or changed, or the outcome is unknown: inspect it.
        for stale in [
            api(409, "AlreadyExists"),
            api(409, "Conflict"),
            api(500, "InternalError"),
            api(503, "ServiceUnavailable"),
            api(504, "Timeout"),
        ] {
            assert!(!stale.is_retryable() && !stale.is_permanent(), "{stale}");
        }
        assert!(api(429, "TooManyRequests").is_retryable());
        for permanent in [api(422, "Invalid"), api(403, "Forbidden")] {
            assert!(permanent.is_permanent(), "{permanent}");
        }
    }

    #[test]
    fn finds_rendered_service_ref_for_readiness() {
        let refs = [
            object_ref("apps/v1", "Deployment", "apps", "app-a"),
            object_ref("v1", "Service", "apps", "svc-a"),
        ];

        let service = rendered_service_ref(&refs).expect("service ref");

        assert_eq!(service.name, "svc-a");
    }

    fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
        RenderedObjectRef {
            api_version: api_version.to_owned(),
            kind: kind.to_owned(),
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }

    fn service<const N: usize>(name: &str, namespace: &str, ports: [i32; N]) -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(namespace.to_owned()),
                uid: Some("current-service".to_owned()),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(
                    ports
                        .into_iter()
                        .map(|port| ServicePort {
                            port,
                            ..ServicePort::default()
                        })
                        .collect(),
                ),
                ..ServiceSpec::default()
            }),
            ..Service::default()
        }
    }

    fn endpoint_slice<const N: usize>(ready_values: [Option<bool>; N]) -> EndpointSlice {
        EndpointSlice {
            metadata: ObjectMeta {
                owner_references: Some(vec![
                    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                        api_version: "v1".to_owned(),
                        kind: "Service".to_owned(),
                        name: "api".to_owned(),
                        uid: "current-service".to_owned(),
                        controller: Some(true),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
            endpoints: ready_values
                .into_iter()
                .map(|ready| Endpoint {
                    addresses: vec!["10.0.0.1".to_owned()],
                    conditions: Some(EndpointConditions {
                        ready,
                        ..EndpointConditions::default()
                    }),
                    ..Endpoint::default()
                })
                .collect(),
            ..EndpointSlice::default()
        }
    }
}
