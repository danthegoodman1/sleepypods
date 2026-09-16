#[cfg(test)]
mod conditional_tests;

mod idle_membership;

use std::{
    error::Error,
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use k8s_openapi::api::{
    core::v1::{PersistentVolumeClaim, Service},
    discovery::v1::{Endpoint, EndpointSlice},
};
use kube::{
    api::{
        Api, ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams,
        Preconditions,
    },
    Client, Error as KubeError,
};
use tokio::time::{sleep, timeout, Instant};

use crate::{
    manifest::KubernetesObject,
    materialization::{BackendAddress, BackendEndpoint, RenderedObjectRef},
    materializer::{
        rendered_object_ref, KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializerClient,
    },
    projection::{LiveObjectMetadata, ProjectionObjectInspection, ProjectionReadinessInspection},
};

const DEFAULT_FIELD_MANAGER: &str = "sleepypods-control-plane";
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";
const BACKEND_SCHEME_ANNOTATION: &str = "sleepypods.io/backend-scheme";

#[derive(Clone)]
pub struct KubeMaterializerClient {
    client: Client,
    config: KubeMaterializerClientConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubeMaterializerClientConfig {
    pub field_manager: String,
    pub backend_scheme: String,
    pub delete_timeout: Duration,
    pub pvc_bound_timeout: Duration,
    pub readiness_timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidKubeMaterializerClientConfig {
    field: &'static str,
}

impl KubeMaterializerClient {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            config: KubeMaterializerClientConfig::default(),
        }
    }

    pub fn with_config(
        client: Client,
        config: KubeMaterializerClientConfig,
    ) -> Result<Self, InvalidKubeMaterializerClientConfig> {
        config.validate()?;
        Ok(Self { client, config })
    }

    pub async fn try_default() -> Result<Self, KubernetesClientError> {
        Client::try_default()
            .await
            .map(Self::new)
            .map_err(kube_error)
    }

    pub fn config(&self) -> &KubeMaterializerClientConfig {
        &self.config
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
            let object_ref = rendered_object_ref(object);
            let api = self.dynamic_api(&object_ref)?;
            let mut body = object.to_kubernetes_json();
            let operation = async {
                match precondition {
                    None => {
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
            timeout(self.config.delete_timeout, operation)
                .await
                .map_err(|_| {
                    KubernetesClientError::uncertain(
                        "Kubernetes mutation timed out; effect outcome is unknown",
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
                let live = match timeout(self.config.delete_timeout, api.get(&object.name))
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
                self.config.delete_timeout,
                api.delete(&object.name, &params),
            )
            .await
            .map_err(|_| {
                KubernetesClientError::uncertain(
                    "Kubernetes delete timed out; effect outcome is unknown",
                )
            })? {
                Ok(_) => Ok(()),
                Err(error) if is_not_found(&error) => Ok(()),
                Err(error) => Err(mutation_error(error)),
            }
        })
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            timeout(self.config.pvc_bound_timeout, async move {
            let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), namespace);
            let deadline = Instant::now() + self.config.pvc_bound_timeout;

            loop {
                let pvc = pvcs.get(name).await.map_err(kube_error)?;
                if pvc_phase_is_bound(&pvc) {
                    return Ok(());
                }

                sleep_until_next_poll(
                    deadline,
                    self.config.poll_interval,
                    format!("timed out waiting for PersistentVolumeClaim {namespace}/{name} to become Bound"),
                )
                .await?;
            }
            }).await.map_err(|_| KubernetesClientError::transient("Kubernetes read/wait timed out"))?
        })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async move {
            timeout(self.config.readiness_timeout, async move {
                let service_ref = rendered_service_ref(objects)?;
                let services: Api<Service> =
                    Api::namespaced(self.client.clone(), &service_ref.namespace);
                let endpoint_slices: Api<EndpointSlice> =
                    Api::namespaced(self.client.clone(), &service_ref.namespace);
                let deadline = Instant::now() + self.config.readiness_timeout;

                loop {
                    let service = services.get(&service_ref.name).await.map_err(kube_error)?;
                    let selector = format!("{SERVICE_NAME_LABEL}={}", service_ref.name);
                    let slices = endpoint_slices
                        .list(&ListParams::default().labels(&selector))
                        .await
                        .map_err(kube_error)?;
                    let address = ready_backend_address(&service, &slices.items);
                    let backend = backend_endpoint_for_service(&service, &self.config, address)?;

                    if slices
                        .iter()
                        .any(|slice| endpoint_slice_has_ready_endpoint(&service, slice))
                    {
                        return Ok(backend);
                    }

                    sleep_until_next_poll(
                        deadline,
                        self.config.poll_interval,
                        format!(
                            "timed out waiting for ready EndpointSlice endpoints for Service {}/{}",
                            service_ref.namespace, service_ref.name
                        ),
                    )
                    .await?;
                }
            })
            .await
            .map_err(|_| KubernetesClientError::transient("Kubernetes read/wait timed out"))?
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            let api = self.dynamic_api(object)?;
            match timeout(self.config.delete_timeout, api.get(&object.name))
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
            timeout(self.config.delete_timeout, async {
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
            timeout(self.config.delete_timeout, async {
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
            timeout(self.config.delete_timeout, async move {
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
            delete_timeout: Duration::from_secs(10),
            pvc_bound_timeout: Duration::from_secs(120),
            readiness_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_secs(2),
        }
    }
}

impl KubeMaterializerClientConfig {
    pub fn validate(&self) -> Result<(), InvalidKubeMaterializerClientConfig> {
        if self.field_manager.trim().is_empty() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "field_manager",
            });
        }
        if !is_valid_uri_scheme(&self.backend_scheme) {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "backend_scheme",
            });
        }
        if self.delete_timeout.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "delete_timeout",
            });
        }
        if self.pvc_bound_timeout.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "pvc_bound_timeout",
            });
        }
        if self.readiness_timeout.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "readiness_timeout",
            });
        }
        if self.poll_interval.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "poll_interval",
            });
        }
        Ok(())
    }
}

impl InvalidKubeMaterializerClientConfig {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for InvalidKubeMaterializerClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Kubernetes materializer config {} is invalid",
            self.field
        )
    }
}

impl Error for InvalidKubeMaterializerClientConfig {}

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
    address: Option<BackendAddress>,
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

    let uri = format!("{backend_scheme}://{name}.{namespace}.svc.cluster.local:{port}");
    match address {
        Some(address) => BackendEndpoint::with_address(uri, address),
        None => BackendEndpoint::new(uri),
    }
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

fn endpoint_slice_has_ready_endpoint(service: &Service, slice: &EndpointSlice) -> bool {
    slice_belongs_to_service(service, slice) && first_ready_endpoint(slice).is_some()
}

fn slice_belongs_to_service(service: &Service, slice: &EndpointSlice) -> bool {
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
    true
}

fn first_ready_endpoint(slice: &EndpointSlice) -> Option<&Endpoint> {
    slice.endpoints.iter().find(|endpoint| {
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
    })
}

/// The address a ready endpoint was observed at, for callers that can route to
/// it without resolving the Service name. A dual-stack Service presents one
/// slice per address family, so the lowest (address type, slice name) pair wins
/// and repeated reads of an unchanged Service agree.
fn ready_backend_address(service: &Service, slices: &[EndpointSlice]) -> Option<BackendAddress> {
    let mut candidates: Vec<(&str, &str, BackendAddress)> = slices
        .iter()
        .filter(|slice| slice_belongs_to_service(service, slice))
        .filter_map(|slice| {
            let port = endpoint_slice_port(slice)?;
            let ip = first_ready_endpoint(slice)?
                .addresses
                .first()?
                .parse::<IpAddr>()
                .ok()?;
            let address = BackendAddress::new(SocketAddr::new(ip, port)).ok()?;

            Some((
                slice.address_type.as_str(),
                slice.metadata.name.as_deref().unwrap_or_default(),
                address,
            ))
        })
        .collect();
    candidates.sort_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));

    candidates.first().map(|(_, _, address)| *address)
}

fn endpoint_slice_port(slice: &EndpointSlice) -> Option<u16> {
    slice
        .ports
        .as_ref()?
        .iter()
        .find_map(|port| u16::try_from(port.port?).ok().filter(|port| *port != 0))
}

fn readiness_inspection_for_service(
    service: &Service,
    slices: &[EndpointSlice],
    config: &KubeMaterializerClientConfig,
) -> KubernetesClientResult<ProjectionReadinessInspection> {
    let address = ready_backend_address(service, slices);
    let backend = match backend_endpoint_for_service(service, config, address) {
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

fn pvc_phase_is_bound(pvc: &PersistentVolumeClaim) -> bool {
    pvc.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        == Some("Bound")
}

async fn sleep_until_next_poll(
    deadline: Instant,
    poll_interval: Duration,
    timeout_message: String,
) -> KubernetesClientResult<()> {
    let now = Instant::now();
    if now >= deadline {
        return Err(KubernetesClientError::new(timeout_message));
    }

    sleep(poll_interval.min(deadline - now)).await;
    Ok(())
}

fn is_not_found(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(status) if status.is_not_found())
}

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
            status.is_conflict() || status.code == 429 || (500..=599).contains(&status.code)
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

fn mutation_error(error: KubeError) -> KubernetesClientError {
    match &error {
        KubeError::Api(status) if status.code < 500 => kube_error(error),
        KubeError::BuildRequest(_) | KubeError::HttpError(_) | KubeError::Auth(_) => {
            KubernetesClientError::new(error.to_string())
        }
        _ => KubernetesClientError::uncertain(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use k8s_openapi::{
        api::{
            core::v1::{Service, ServicePort, ServiceSpec},
            discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice},
        },
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };
    use kube::{core::Status, Error as KubeError};

    use crate::{
        materialization::{BackendAddress, BackendEndpoint, RenderedObjectRef},
        projection::ProjectionReadinessInspection,
    };

    use super::{
        api_resource_for_ref, backend_endpoint_for_service, endpoint_slice_has_ready_endpoint,
        is_not_found, is_valid_uri_scheme, kube_error, readiness_inspection_for_service,
        ready_backend_address, rendered_service_ref, KubeMaterializerClientConfig,
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
            .message()
            .contains("unsupported rendered Kubernetes object"));
    }

    #[test]
    fn default_config_uses_http_service_backend_uri() {
        let service = service("api", "apps", [80, 8080]);
        let config = KubeMaterializerClientConfig::default();

        let backend = backend_endpoint_for_service(&service, &config, None)
            .expect("service backend endpoint");

        assert_eq!(backend.uri(), "http://api.apps.svc.cluster.local:80");
        assert_eq!(backend.address(), None);
    }

    #[test]
    fn configurable_scheme_changes_service_backend_uri() {
        let service = service("postgres", "data", [5432]);
        let config = KubeMaterializerClientConfig {
            backend_scheme: "tcp".to_owned(),
            ..KubeMaterializerClientConfig::default()
        };

        let backend = backend_endpoint_for_service(&service, &config, None)
            .expect("service backend endpoint");

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

        let backend = backend_endpoint_for_service(&service, &config, None)
            .expect("service backend endpoint");

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
    fn validates_timeout_configuration() {
        let config = KubeMaterializerClientConfig {
            poll_interval: Duration::ZERO,
            ..KubeMaterializerClientConfig::default()
        };

        let error = config.validate().expect_err("zero poll interval rejected");

        assert_eq!(error.field(), "poll_interval");
    }

    #[test]
    fn validates_backend_scheme_configuration() {
        assert!(is_valid_uri_scheme("http"));
        assert!(is_valid_uri_scheme("tcp"));
        assert!(is_valid_uri_scheme("h2c+unix"));

        for backend_scheme in ["", " ", "1http", "http://", "tcp:"] {
            let config = KubeMaterializerClientConfig {
                backend_scheme: backend_scheme.to_owned(),
                ..KubeMaterializerClientConfig::default()
            };

            let error = config
                .validate()
                .expect_err("invalid backend scheme rejected");

            assert_eq!(error.field(), "backend_scheme");
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
        assert!(error.message().contains("resource version conflict"));
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
        assert!(error.message().contains("manifest is invalid"));
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

    fn owned_endpoint_slice(
        name: &str,
        address_type: &str,
        addresses: &[&str],
        port: Option<i32>,
        ready: Option<bool>,
    ) -> EndpointSlice {
        EndpointSlice {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
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
            address_type: address_type.to_owned(),
            endpoints: vec![Endpoint {
                addresses: addresses.iter().map(|value| (*value).to_owned()).collect(),
                conditions: Some(EndpointConditions {
                    ready,
                    ..EndpointConditions::default()
                }),
                ..Endpoint::default()
            }],
            ports: port.map(|port| {
                vec![EndpointPort {
                    port: Some(port),
                    ..EndpointPort::default()
                }]
            }),
        }
    }

    fn backend_address(value: &str) -> BackendAddress {
        value
            .parse::<BackendAddress>()
            .expect("valid backend address")
    }

    #[test]
    fn ready_backend_address_uses_the_ready_endpoint_address_and_port() {
        let service = service("api", "apps", [80]);
        let slices = [owned_endpoint_slice(
            "api-v4",
            "IPv4",
            &["10.244.1.7"],
            Some(8080),
            Some(true),
        )];

        assert_eq!(
            ready_backend_address(&service, &slices),
            Some(backend_address("10.244.1.7:8080"))
        );
    }

    #[test]
    fn ready_backend_address_brackets_ipv6_endpoints() {
        let service = service("api", "apps", [80]);
        let slices = [owned_endpoint_slice(
            "api-v6",
            "IPv6",
            &["fd00::7"],
            Some(8080),
            Some(true),
        )];

        assert_eq!(
            ready_backend_address(&service, &slices).map(|address| address.to_string()),
            Some("[fd00::7]:8080".to_owned())
        );
    }

    #[test]
    fn ready_backend_address_needs_a_port_a_ready_endpoint_and_a_numeric_address() {
        let service = service("api", "apps", [80]);
        for slice in [
            owned_endpoint_slice("api-no-port", "IPv4", &["10.244.1.7"], None, Some(true)),
            owned_endpoint_slice(
                "api-unready",
                "IPv4",
                &["10.244.1.7"],
                Some(8080),
                Some(false),
            ),
            owned_endpoint_slice(
                "api-zero-port",
                "IPv4",
                &["10.244.1.7"],
                Some(0),
                Some(true),
            ),
            owned_endpoint_slice(
                "api-fqdn",
                "FQDN",
                &["api.apps.svc.cluster.local"],
                Some(8080),
                Some(true),
            ),
            owned_endpoint_slice("api-empty", "IPv4", &[], Some(8080), Some(true)),
        ] {
            assert_eq!(ready_backend_address(&service, &[slice]), None);
        }
    }

    #[test]
    fn ready_backend_address_ignores_slices_owned_by_another_service() {
        let service = service("other", "apps", [80]);
        let slices = [owned_endpoint_slice(
            "api-v4",
            "IPv4",
            &["10.244.1.7"],
            Some(8080),
            Some(true),
        )];

        assert_eq!(ready_backend_address(&service, &slices), None);
    }

    #[test]
    fn ready_backend_address_picks_the_same_dual_stack_slice_on_every_read() {
        let service = service("api", "apps", [80]);
        let v4 = owned_endpoint_slice("api-v4", "IPv4", &["10.244.1.7"], Some(8080), Some(true));
        let v6 = owned_endpoint_slice("api-v6", "IPv6", &["fd00::7"], Some(8080), Some(true));

        let forward = ready_backend_address(&service, &[v4.clone(), v6.clone()]);
        let reversed = ready_backend_address(&service, &[v6, v4]);

        assert_eq!(forward, reversed);
        assert_eq!(forward, Some(backend_address("10.244.1.7:8080")));
    }

    #[test]
    fn service_backend_endpoint_carries_the_observed_pod_address() {
        let service = service("api", "apps", [80]);
        let config = KubeMaterializerClientConfig::default();

        let backend = backend_endpoint_for_service(
            &service,
            &config,
            Some(backend_address("10.244.1.7:8080")),
        )
        .expect("service backend endpoint");

        assert_eq!(backend.uri(), "http://api.apps.svc.cluster.local:80");
        assert_eq!(backend.address(), Some(backend_address("10.244.1.7:8080")));
    }

    #[test]
    fn readiness_inspection_reports_the_ready_pod_address() {
        let service = service("api", "apps", [80]);
        let config = KubeMaterializerClientConfig::default();
        let slices = [owned_endpoint_slice(
            "api-v4",
            "IPv4",
            &["10.244.1.7"],
            Some(8080),
            Some(true),
        )];

        match readiness_inspection_for_service(&service, &slices, &config)
            .expect("readiness inspection")
        {
            ProjectionReadinessInspection::Ready(backend) => {
                assert_eq!(backend.address(), Some(backend_address("10.244.1.7:8080")));
            }
            other => panic!("expected a ready inspection, got {other:?}"),
        }
    }
}
