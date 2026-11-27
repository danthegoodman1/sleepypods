use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
};

use crate::{
    auth::BearerToken,
    kubernetes_name::is_dns_label,
    manifest::{
        KubernetesObject, RenderedManifest, RenderedManifestObject, ANNOTATION_TEMPLATE_GENERATION,
        LABEL_INSTANCE_GENERATION,
    },
    materialization::{BackendEndpoint, RenderedObjectRef},
    projection::{ProjectionObjectInspection, ProjectionReadinessInspection},
    retry::RetryPolicy,
};

pub type KubernetesClientFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type KubernetesClientResult<T> = Result<T, KubernetesClientError>;
type ObjectMetadataMaps<'a> = (&'a BTreeMap<String, String>, &'a BTreeMap<String, String>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleMemberIdentity {
    pub instance_id: crate::ids::InstanceId,
    pub instance_generation: crate::ids::Generation,
    pub materialization_id: crate::ids::MaterializationId,
    pub pod_uid: String,
}

pub trait KubernetesMaterializerClient: Send + Sync {
    /// Verify one ready, non-terminating member belongs to this active projection.
    /// Unknown membership must fail closed, including clients without inspection support.
    fn verify_idle_member<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        identity: &'a IdleMemberIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        let _ = (objects, identity);
        Box::pin(async {
            Err(KubernetesClientError::new(
                "idle membership inspection is not implemented by this client",
            ))
        })
    }

    /// Creates the object when `precondition` is absent, and otherwise patches
    /// it on condition that its UID and resourceVersion still match.
    ///
    /// A Deployment or StatefulSet is created with zero replicas, so a create
    /// that arrives late starts no Pod. The conditional patch scales it to its
    /// rendered replica count.
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a crate::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    /// Scales a workload that was just created with zero replicas up to its
    /// rendered count, on condition that it is still the object `identity`
    /// names.
    ///
    /// The UID alone fences this patch. Only the driver that saw the create
    /// succeed knows the UID, and cleanup removes that object before it
    /// proves absence. Leaving the resourceVersion out keeps the workload
    /// controller's first status write from refusing the patch.
    fn activate_workload<'a>(
        &'a self,
        object: &'a KubernetesObject,
        identity: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.apply_object(object, Some(identity))
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>>;

    /// The address of the ready Pod behind the rendered Service, when the
    /// client can observe one. The published backend stays the Service; this
    /// address is recorded beside it.
    fn ready_pod_backend<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<Option<BackendEndpoint>>> {
        let _ = objects;
        Box::pin(async { Ok(None) })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        let _ = object;
        Box::pin(async {
            Err(KubernetesClientError::new(
                "Kubernetes object inspection is not implemented by this client",
            ))
        })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            if objects
                .iter()
                .any(|object| object.kind == "PersistentVolumeClaim")
            {
                return Err(KubernetesClientError::new(
                    "retained static binding inspection is not implemented by this client",
                ));
            }
            Ok(())
        })
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        let _ = (objects, instance_id);
        Box::pin(async {
            Err(KubernetesClientError::new(
                "descendant absence inspection is not implemented by this client",
            ))
        })
    }

    fn inspect_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>> {
        let _ = objects;
        Box::pin(async { Ok(ProjectionReadinessInspection::NotObserved) })
    }
}

#[derive(Clone, Debug)]
pub struct KubernetesMaterializer<C> {
    client: C,
    sidecar_control_plane_token: Option<BearerToken>,
    sidecar_control_plane_endpoint: Option<String>,
    sidecar_control_plane_ca: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RetryingKubernetesMaterializerClient<C> {
    inner: C,
    policy: RetryPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubernetesClientError {
    message: String,
    kind: KubernetesClientErrorKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KubernetesClientErrorKind {
    /// Sending the same request again cannot succeed.
    Permanent,
    /// The request was refused or lost for a reason that passes; send it again.
    Transient,
    /// The object may differ from what the caller observed: its outcome is
    /// unknown, or the API server found the precondition stale. Inspect it and
    /// decide again.
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaterializerError {
    InvalidManifest {
        message: String,
    },
    Apply {
        object: Box<RenderedObjectRef>,
        source: KubernetesClientError,
    },
    Delete {
        object: Box<RenderedObjectRef>,
        source: KubernetesClientError,
    },
    ReadinessWait {
        rendered_objects: Vec<RenderedObjectRef>,
        source: KubernetesClientError,
    },
}

impl<C> KubernetesMaterializer<C>
where
    C: KubernetesMaterializerClient,
{
    pub fn new(client: C) -> Self {
        Self {
            client,
            sidecar_control_plane_token: None,
            sidecar_control_plane_endpoint: None,
            sidecar_control_plane_ca: None,
        }
    }

    pub(crate) fn with_sidecar_control_plane_transport(
        mut self,
        endpoint: Option<String>,
        ca: Option<String>,
    ) -> Self {
        self.sidecar_control_plane_endpoint = endpoint;
        self.sidecar_control_plane_ca = ca;
        self
    }
    pub(crate) fn render_options(&self) -> crate::manifest::RenderManifestOptions<'_> {
        crate::manifest::RenderManifestOptions {
            sidecar_control_plane_token: self.sidecar_control_plane_token.as_ref(),
            sidecar_control_plane_endpoint: self.sidecar_control_plane_endpoint.as_deref(),
            sidecar_control_plane_ca: self.sidecar_control_plane_ca.as_deref(),
        }
    }

    pub fn with_sidecar_control_plane_token(mut self, token: Option<BearerToken>) -> Self {
        self.sidecar_control_plane_token = token;
        self
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    #[cfg(test)]
    pub(crate) fn sidecar_control_plane_token(&self) -> Option<&BearerToken> {
        self.sidecar_control_plane_token.as_ref()
    }

    /// Applies every rendered object in apply order. `observations` carries
    /// the live identity of each object that already exists.
    pub(crate) async fn apply_manifest(
        &self,
        manifest: &RenderedManifest,
        observations: &[crate::projection::ProjectionObservation],
    ) -> Result<(), MaterializerError> {
        rendered_object_refs(manifest)?;
        for object in ordered_objects(manifest) {
            self.apply_object(&object.object, observations).await?;
        }
        Ok(())
    }

    pub async fn wait_for_readiness(
        &self,
        rendered_objects: &[RenderedObjectRef],
    ) -> Result<BackendEndpoint, MaterializerError> {
        self.client
            .wait_for_readiness(rendered_objects)
            .await
            .map_err(|source| MaterializerError::ReadinessWait {
                rendered_objects: rendered_objects.to_vec(),
                source,
            })
    }

    pub(crate) async fn delete_rendered_object_conditionally(
        &self,
        object: &RenderedObjectRef,
        precondition: &crate::projection::LiveObjectIdentity,
    ) -> Result<(), MaterializerError> {
        self.client
            .delete_object(object, precondition)
            .await
            .map_err(|source| MaterializerError::Delete {
                object: Box::new(object.clone()),
                source,
            })
    }

    async fn apply_object(
        &self,
        object: &KubernetesObject,
        observations: &[crate::projection::ProjectionObservation],
    ) -> Result<(), MaterializerError> {
        let object_ref = rendered_object_ref(object);
        let failed = |source| MaterializerError::Apply {
            object: Box::new(object_ref.clone()),
            source,
        };
        let observed = observations
            .iter()
            .find(|observation| observation.object_ref == object_ref)
            .and_then(|observation| observation.identity.clone());
        if let Some(identity) = observed {
            return self
                .client
                .apply_object(object, Some(&identity))
                .await
                .map_err(failed);
        }
        self.client
            .apply_object(object, None)
            .await
            .map_err(failed)?;
        if !is_workload_ref(&object_ref) {
            return Ok(());
        }
        // The workload exists with zero replicas. Read the identity the API
        // server gave it, then scale that object up.
        match self
            .client
            .inspect_object(&object_ref)
            .await
            .map_err(failed)?
        {
            ProjectionObjectInspection::Present(live) => self
                .client
                .activate_workload(object, &live.identity)
                .await
                .map_err(failed),
            ProjectionObjectInspection::Missing => Err(failed(KubernetesClientError::stale(
                "created workload was removed before activation",
            ))),
        }
    }
}

impl<C> RetryingKubernetesMaterializerClient<C> {
    pub fn new(inner: C, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }

    pub fn with_default_policy(inner: C) -> Self {
        Self::new(inner, RetryPolicy::default())
    }

    pub fn inner(&self) -> &C {
        &self.inner
    }

    pub fn policy(&self) -> RetryPolicy {
        self.policy
    }
}

impl<C> KubernetesMaterializerClient for RetryingKubernetesMaterializerClient<C>
where
    C: KubernetesMaterializerClient,
{
    fn verify_idle_member<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        identity: &'a IdleMemberIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.inner.verify_idle_member(objects, identity)
    }

    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a crate::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        let inner = &self.inner;
        let policy = self.policy;
        Box::pin(async move {
            policy
                .retry_if(
                    || inner.apply_object(object, precondition),
                    KubernetesClientError::is_retryable,
                )
                .await
        })
    }

    fn activate_workload<'a>(
        &'a self,
        object: &'a KubernetesObject,
        identity: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        let inner = &self.inner;
        let policy = self.policy;
        Box::pin(async move {
            policy
                .retry_if(
                    || inner.activate_workload(object, identity),
                    KubernetesClientError::is_retryable,
                )
                .await
        })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        let inner = &self.inner;
        let policy = self.policy;
        Box::pin(async move {
            policy
                .retry_if(
                    || inner.delete_object(object, precondition),
                    KubernetesClientError::is_retryable,
                )
                .await
        })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        let inner = &self.inner;
        let policy = self.policy;
        Box::pin(async move {
            policy
                .retry_if(
                    || inner.wait_for_readiness(objects),
                    KubernetesClientError::is_retryable,
                )
                .await
        })
    }

    fn ready_pod_backend<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<Option<BackendEndpoint>>> {
        self.inner.ready_pod_backend(objects)
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        let inner = &self.inner;
        let policy = self.policy;
        Box::pin(async move {
            policy
                .retry_if(
                    || inner.inspect_object(object),
                    KubernetesClientError::is_retryable,
                )
                .await
        })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.inner.verify_retained_bindings(objects)
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        self.inner.ensure_no_descendants(objects, instance_id)
    }

    fn inspect_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>> {
        let inner = &self.inner;
        let policy = self.policy;
        Box::pin(async move {
            policy
                .retry_if(
                    || inner.inspect_readiness(objects),
                    KubernetesClientError::is_retryable,
                )
                .await
        })
    }
}

pub fn rendered_object_ref(object: &KubernetesObject) -> RenderedObjectRef {
    let (api_version, namespace) = match object {
        KubernetesObject::Deployment(object) => ("apps/v1", object.metadata.namespace.clone()),
        KubernetesObject::StatefulSet(object) => ("apps/v1", object.metadata.namespace.clone()),
        KubernetesObject::Service(object) => ("v1", object.metadata.namespace.clone()),
        KubernetesObject::Secret(object) => ("v1", object.metadata.namespace.clone()),
        KubernetesObject::PersistentVolume(object) => ("v1", object.metadata.namespace.clone()),
        KubernetesObject::PersistentVolumeClaim(object) => {
            ("v1", object.metadata.namespace.clone())
        }
        KubernetesObject::Raw(object) => (
            object.api_version.as_str(),
            object.metadata.namespace.clone(),
        ),
    };

    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: object.kind().to_owned(),
        namespace: namespace.unwrap_or_default(),
        name: object.name().to_owned(),
    }
}

pub fn rendered_object_refs(
    manifest: &RenderedManifest,
) -> Result<Vec<RenderedObjectRef>, MaterializerError> {
    validate_manifest_generations(manifest)?;

    let rendered_objects = ordered_objects(manifest)
        .into_iter()
        .map(|object| rendered_object_ref(&object.object))
        .collect::<Vec<_>>();
    validate_rendered_object_refs(&rendered_objects)?;

    Ok(rendered_objects)
}

fn ordered_objects(manifest: &RenderedManifest) -> Vec<&RenderedManifestObject> {
    let mut objects = manifest.objects.iter().collect::<Vec<_>>();
    objects.sort_by_key(|object| object.apply_order);
    objects
}

pub(crate) fn delete_order(objects: &[RenderedObjectRef]) -> Vec<&RenderedObjectRef> {
    objects
        .iter()
        .rev()
        .filter(|object| !is_workload_ref(object))
        .chain(
            objects
                .iter()
                .rev()
                .filter(|object| is_workload_ref(object)),
        )
        .collect()
}

pub(crate) fn is_workload_ref(object: &RenderedObjectRef) -> bool {
    matches!(object.kind.as_str(), "Deployment" | "StatefulSet")
}

fn validate_rendered_object_refs(objects: &[RenderedObjectRef]) -> Result<(), MaterializerError> {
    let mut refs = BTreeSet::new();

    for object in objects {
        validate_rendered_object_ref(object)?;
        let key = (
            object.api_version.as_str(),
            object.kind.as_str(),
            object.namespace.as_str(),
            object.name.as_str(),
        );
        if !refs.insert(key) {
            return Err(MaterializerError::InvalidManifest {
                message: format!(
                    "duplicate rendered Kubernetes object ref {} {} {}/{}",
                    object.api_version, object.kind, object.namespace, object.name
                ),
            });
        }
    }

    Ok(())
}

fn validate_rendered_object_ref(object: &RenderedObjectRef) -> Result<(), MaterializerError> {
    if !is_dns_label(&object.name) {
        return Err(MaterializerError::InvalidManifest {
            message: format!(
                "{} {} {}/{} name must be a Kubernetes DNS label with at most 63 characters",
                object.api_version, object.kind, object.namespace, object.name
            ),
        });
    }

    match object.kind.as_str() {
        "PersistentVolume" => {
            if !object.namespace.is_empty() {
                return Err(MaterializerError::InvalidManifest {
                    message: format!(
                        "PersistentVolume {} must be cluster-scoped with an empty namespace",
                        object.name
                    ),
                });
            }
        }
        "Deployment" | "StatefulSet" | "Service" | "Secret" | "PersistentVolumeClaim" => {
            if !is_dns_label(&object.namespace) {
                return Err(MaterializerError::InvalidManifest {
                    message: format!(
                        "{} {} namespace {:?} must be a Kubernetes DNS label",
                        object.kind, object.name, object.namespace
                    ),
                });
            }
        }
        other => {
            return Err(MaterializerError::InvalidManifest {
                message: format!("unsupported rendered Kubernetes object kind {other}"),
            });
        }
    }

    Ok(())
}

impl KubernetesClientError {
    /// A failure the same request cannot recover from.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: KubernetesClientErrorKind::Permanent,
        }
    }

    /// A failure that passes; the same request can be sent again.
    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: KubernetesClientErrorKind::Transient,
        }
    }

    /// The object must be inspected again: the request's outcome is unknown,
    /// or its precondition no longer matches.
    pub fn stale(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: KubernetesClientErrorKind::Stale,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_retryable(&self) -> bool {
        self.kind == KubernetesClientErrorKind::Transient
    }

    pub fn is_permanent(&self) -> bool {
        self.kind == KubernetesClientErrorKind::Permanent
    }
}

impl fmt::Display for KubernetesClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for KubernetesClientError {}

impl fmt::Display for MaterializerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManifest { message } => write!(f, "invalid rendered manifest: {message}"),
            Self::Apply { object, source } => {
                write!(
                    f,
                    "failed to apply {} {}/{}: {}",
                    object.kind, object.namespace, object.name, source
                )
            }
            Self::Delete { object, source } => {
                write!(
                    f,
                    "failed to delete {} {}/{}: {}",
                    object.kind, object.namespace, object.name, source
                )
            }
            Self::ReadinessWait {
                rendered_objects,
                source,
            } => write!(
                f,
                "failed waiting for readiness across {} rendered Kubernetes objects: {source}",
                rendered_objects.len()
            ),
        }
    }
}

impl Error for MaterializerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidManifest { .. } => None,
            Self::Apply { source, .. }
            | Self::Delete { source, .. }
            | Self::ReadinessWait { source, .. } => Some(source),
        }
    }
}

fn validate_manifest_generations(manifest: &RenderedManifest) -> Result<(), MaterializerError> {
    let expected_instance_generation = manifest.instance_generation.to_string();
    let expected_template_generation = manifest
        .template_generation
        .map(|generation| generation.to_string());

    for rendered in &manifest.objects {
        let (labels, annotations) = object_metadata(&rendered.object);
        validate_metadata_generation(
            &rendered.object,
            labels,
            annotations,
            &expected_instance_generation,
            expected_template_generation.as_deref(),
            "metadata",
        )?;

        if let Some((labels, annotations)) = pod_template_metadata(&rendered.object) {
            validate_metadata_generation(
                &rendered.object,
                labels,
                annotations,
                &expected_instance_generation,
                expected_template_generation.as_deref(),
                "pod template metadata",
            )?;
        }
    }

    Ok(())
}

fn validate_metadata_generation(
    object: &KubernetesObject,
    labels: &BTreeMap<String, String>,
    annotations: &BTreeMap<String, String>,
    expected_instance_generation: &str,
    expected_template_generation: Option<&str>,
    context: &'static str,
) -> Result<(), MaterializerError> {
    validate_generation_value(
        object,
        context,
        LABEL_INSTANCE_GENERATION,
        labels.get(LABEL_INSTANCE_GENERATION),
        expected_instance_generation,
    )?;

    if let Some(expected) = expected_template_generation {
        validate_generation_value(
            object,
            context,
            ANNOTATION_TEMPLATE_GENERATION,
            annotations.get(ANNOTATION_TEMPLATE_GENERATION),
            expected,
        )?;
    }

    Ok(())
}

fn validate_generation_value(
    object: &KubernetesObject,
    context: &'static str,
    key: &'static str,
    actual: Option<&String>,
    expected: &str,
) -> Result<(), MaterializerError> {
    if actual.is_some_and(|actual| actual == expected) {
        return Ok(());
    }

    let object_ref = rendered_object_ref(object);
    Err(MaterializerError::InvalidManifest {
        message: format!(
            "{} {}/{} {context} {key} must be {expected:?}, got {:?}",
            object_ref.kind, object_ref.namespace, object_ref.name, actual
        ),
    })
}

fn object_metadata(object: &KubernetesObject) -> ObjectMetadataMaps<'_> {
    match object {
        KubernetesObject::Deployment(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::StatefulSet(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::Service(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::Secret(object) => (&object.metadata.labels, &object.metadata.annotations),
        KubernetesObject::PersistentVolume(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::PersistentVolumeClaim(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::Raw(object) => (&object.metadata.labels, &object.metadata.annotations),
    }
}

fn pod_template_metadata(object: &KubernetesObject) -> Option<ObjectMetadataMaps<'_>> {
    match object {
        KubernetesObject::Deployment(object) => Some((
            &object.spec.template.metadata.labels,
            &object.spec.template.metadata.annotations,
        )),
        KubernetesObject::StatefulSet(object) => Some((
            &object.spec.template.metadata.labels,
            &object.spec.template.metadata.annotations,
        )),
        KubernetesObject::Service(_)
        | KubernetesObject::Secret(_)
        | KubernetesObject::PersistentVolume(_)
        | KubernetesObject::PersistentVolumeClaim(_) => None,
        KubernetesObject::Raw(object) => object
            .pod_template_metadata
            .as_ref()
            .map(|metadata| (&metadata.labels, &metadata.annotations)),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use crate::{
        ids::{Generation, InstanceId, WorkloadClassId},
        instance::{InstanceRecord, InstanceState, InstanceValues},
        manifest::{
            render_manifests, ContainerPortTemplate, ContainerTemplate, EnvVarTemplate,
            KubernetesObject, ManifestTemplate, PersistentVolumeAccessMode,
            PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, RenderManifestRequest,
            ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart,
            VolumeTemplate, WorkloadKind, WorkloadTemplate,
        },
        projection::{
            LiveObjectIdentity, LiveObjectMetadata, ProjectionObjectInspection,
            ProjectionObservation, ProjectionReadinessInspection,
        },
        sleep_policy::ResolvedSleepPolicy,
        workload::WorkloadClassVersionRef,
    };

    use super::{
        delete_order, rendered_object_ref, rendered_object_refs, BackendEndpoint,
        KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializer, KubernetesMaterializerClient, MaterializerError, RenderedObjectRef,
        RetryPolicy, RetryingKubernetesMaterializerClient,
    };

    /// Records every call. A created object becomes live under a UID derived
    /// from its name, so an inspection after a create finds it.
    #[derive(Clone, Debug, Default)]
    struct FakeKubernetesClient {
        inner: Arc<Mutex<FakeState>>,
    }

    #[derive(Clone, Debug, Default)]
    struct FakeState {
        operations: Vec<FakeOperation>,
        created: Vec<KubernetesObject>,
        live: BTreeMap<String, LiveObjectIdentity>,
        removed_after_create: BTreeSet<String>,
        queued_failures: VecDeque<(FakeOperation, KubernetesClientError)>,
        readiness_inspection: Option<ProjectionReadinessInspection>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum FakeOperation {
        Create(RenderedObjectRef),
        Patch(RenderedObjectRef, LiveObjectIdentity),
        Inspect(RenderedObjectRef),
        Delete(RenderedObjectRef, LiveObjectIdentity),
        WaitReadiness(Vec<RenderedObjectRef>),
        InspectReadiness(Vec<RenderedObjectRef>),
    }

    #[test]
    fn kubernetes_materializer_client_trait_is_dyn_safe() {
        fn assert_dyn_safe<T: KubernetesMaterializerClient + ?Sized>() {}

        assert_dyn_safe::<dyn KubernetesMaterializerClient>();
    }

    #[test]
    fn client_error_kinds_say_whether_to_resend_inspect_or_stop() {
        let permanent = KubernetesClientError::new("invalid object");
        let transient = KubernetesClientError::transient("throttled");
        let stale = KubernetesClientError::stale("conflict");

        assert!(permanent.is_permanent() && !permanent.is_retryable());
        assert!(transient.is_retryable() && !transient.is_permanent());
        assert!(!stale.is_retryable() && !stale.is_permanent());
    }

    #[tokio::test]
    async fn applies_volume_claim_secret_and_service_before_the_workload() {
        let mut manifest = stateful_manifest();
        let metadata = manifest
            .objects
            .iter()
            .find_map(|rendered| match &rendered.object {
                KubernetesObject::Service(service) => Some(service.metadata.clone()),
                _ => None,
            })
            .map(|mut metadata| {
                metadata.name = "sidecar-token".into();
                metadata
            })
            .expect("service object");
        manifest
            .objects
            .push(crate::manifest::RenderedManifestObject {
                apply_order: crate::manifest::ApplyOrder::Secret,
                object: KubernetesObject::Secret(crate::manifest::Secret {
                    metadata,
                    type_: "Opaque".into(),
                    string_data: BTreeMap::from([("token".into(), "sidecar-test-token".into())]),
                }),
            });
        let client = FakeKubernetesClient::default();

        KubernetesMaterializer::new(client.clone())
            .apply_manifest(&manifest, &[])
            .await
            .expect("stateful manifest applies");

        let created = client
            .operations()
            .into_iter()
            .filter_map(|operation| match operation {
                FakeOperation::Create(object) => Some(object.kind),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            created,
            [
                "PersistentVolume",
                "PersistentVolumeClaim",
                "Secret",
                "Service",
                "StatefulSet"
            ]
        );
    }

    #[tokio::test]
    async fn missing_workload_is_created_inspected_then_applied_on_the_inspected_identity() {
        let client = FakeKubernetesClient::default();
        let workload = deployment_ref();

        KubernetesMaterializer::new(client.clone())
            .apply_manifest(&deployment_manifest(), &[])
            .await
            .expect("deployment applies");

        assert_eq!(
            client.operations_on(&workload),
            vec![
                FakeOperation::Create(workload.clone()),
                FakeOperation::Inspect(workload.clone()),
                FakeOperation::Patch(workload.clone(), created_identity(&workload)),
            ]
        );
    }

    #[tokio::test]
    async fn created_service_needs_no_second_apply() {
        let client = FakeKubernetesClient::default();
        let service = service_ref();

        KubernetesMaterializer::new(client.clone())
            .apply_manifest(&deployment_manifest(), &[])
            .await
            .expect("deployment applies");

        assert_eq!(
            client.operations_on(&service),
            vec![FakeOperation::Create(service)]
        );
    }

    #[tokio::test]
    async fn present_objects_are_patched_on_their_observed_identity() {
        let client = FakeKubernetesClient::default();
        let service = service_ref();
        let workload = deployment_ref();
        let observed = |object: &RenderedObjectRef, resource_version: &str| {
            let mut observation = ProjectionObservation::missing(object.clone());
            observation.identity = Some(LiveObjectIdentity {
                uid: format!("observed-{}", object.kind),
                resource_version: resource_version.to_owned(),
            });
            observation
        };
        let observations = [observed(&service, "11"), observed(&workload, "12")];

        KubernetesMaterializer::new(client.clone())
            .apply_manifest(&deployment_manifest(), &observations)
            .await
            .expect("deployment applies");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Patch(service, observations[0].identity.clone().unwrap()),
                FakeOperation::Patch(workload, observations[1].identity.clone().unwrap()),
            ]
        );
    }

    #[tokio::test]
    async fn workload_removed_between_create_and_inspect_must_be_inspected_again() {
        let client = FakeKubernetesClient::default();
        let workload = deployment_ref();
        client.remove_after_create(&workload);

        let error = KubernetesMaterializer::new(client.clone())
            .apply_manifest(&deployment_manifest(), &[])
            .await
            .expect_err("a removed workload cannot be activated");

        let MaterializerError::Apply { object, source } = error else {
            panic!("expected an apply error, got {error:?}");
        };
        assert_eq!(*object, workload);
        assert!(!source.is_retryable() && !source.is_permanent());
        assert_eq!(
            client.operations_on(&workload),
            vec![
                FakeOperation::Create(workload.clone()),
                FakeOperation::Inspect(workload),
            ]
        );
    }

    #[tokio::test]
    async fn apply_failure_names_the_failed_object_and_stops() {
        let client = FakeKubernetesClient::default();
        let service = service_ref();
        client.fail_next(
            FakeOperation::Create(service.clone()),
            KubernetesClientError::new("apply failed"),
        );

        let error = KubernetesMaterializer::new(client.clone())
            .apply_manifest(&deployment_manifest(), &[])
            .await
            .expect_err("service apply failure stops the apply");

        assert_eq!(
            error,
            MaterializerError::Apply {
                object: Box::new(service.clone()),
                source: KubernetesClientError::new("apply failed"),
            }
        );
        assert_eq!(client.operations(), vec![FakeOperation::Create(service)]);
    }

    #[tokio::test]
    async fn retrying_client_resends_a_transient_create() {
        let client = FakeKubernetesClient::default();
        let service = service_ref();
        client.fail_next(
            FakeOperation::Create(service.clone()),
            KubernetesClientError::transient("throttled"),
        );

        KubernetesMaterializer::new(retrying_client(client.clone()))
            .apply_manifest(&deployment_manifest(), &[])
            .await
            .expect("transient create failure is retried");

        assert_eq!(
            client.operations_on(&service),
            vec![
                FakeOperation::Create(service.clone()),
                FakeOperation::Create(service),
            ]
        );
    }

    #[tokio::test]
    async fn retrying_client_sends_a_permanent_or_stale_failure_once() {
        for failure in [
            KubernetesClientError::new("invalid object"),
            KubernetesClientError::stale("already exists"),
        ] {
            let client = FakeKubernetesClient::default();
            let service = service_ref();
            client.fail_next(FakeOperation::Create(service.clone()), failure.clone());

            let error = KubernetesMaterializer::new(retrying_client(client.clone()))
                .apply_manifest(&deployment_manifest(), &[])
                .await
                .expect_err("the failure is returned");

            assert_eq!(
                error,
                MaterializerError::Apply {
                    object: Box::new(service.clone()),
                    source: failure,
                }
            );
            assert_eq!(client.operations(), vec![FakeOperation::Create(service)]);
        }
    }

    #[tokio::test]
    async fn retrying_client_resends_a_transient_delete() {
        let client = FakeKubernetesClient::default();
        let workload = deployment_ref();
        let identity = created_identity(&workload);
        let delete = FakeOperation::Delete(workload.clone(), identity.clone());
        client.fail_next(
            delete.clone(),
            KubernetesClientError::transient("throttled"),
        );

        KubernetesMaterializer::new(retrying_client(client.clone()))
            .delete_rendered_object_conditionally(&workload, &identity)
            .await
            .expect("transient delete failure is retried");

        assert_eq!(client.operations(), vec![delete.clone(), delete]);
    }

    #[tokio::test]
    async fn delete_failure_names_the_object() {
        let client = FakeKubernetesClient::default();
        let service = service_ref();
        let identity = created_identity(&service);
        client.fail_next(
            FakeOperation::Delete(service.clone(), identity.clone()),
            KubernetesClientError::new("delete failed"),
        );

        let error = KubernetesMaterializer::new(client)
            .delete_rendered_object_conditionally(&service, &identity)
            .await
            .expect_err("delete failure is returned");

        assert_eq!(
            error,
            MaterializerError::Delete {
                object: Box::new(service),
                source: KubernetesClientError::new("delete failed"),
            }
        );
    }

    #[tokio::test]
    async fn retrying_client_resends_a_transient_inspection() {
        let client = FakeKubernetesClient::default();
        let service = service_ref();
        client.fail_next(
            FakeOperation::Inspect(service.clone()),
            KubernetesClientError::transient("api unavailable"),
        );

        let inspection = retrying_client(client.clone())
            .inspect_object(&service)
            .await
            .expect("transient inspection failure is retried");

        assert_eq!(inspection, ProjectionObjectInspection::Missing);
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Inspect(service.clone()),
                FakeOperation::Inspect(service),
            ]
        );
    }

    #[tokio::test]
    async fn retrying_client_resends_a_transient_readiness_wait() {
        let client = FakeKubernetesClient::default();
        let refs = vec![service_ref(), deployment_ref()];
        let wait = FakeOperation::WaitReadiness(refs.clone());
        client.fail_next(
            wait.clone(),
            KubernetesClientError::transient("endpoint slice list failed"),
        );

        let backend = KubernetesMaterializer::new(retrying_client(client.clone()))
            .wait_for_readiness(&refs)
            .await
            .expect("transient readiness failure is retried");

        assert_eq!(backend, default_backend_endpoint());
        assert_eq!(client.operations(), vec![wait.clone(), wait]);
    }

    #[tokio::test]
    async fn retrying_materializer_client_preserves_readiness_inspection() {
        let refs = vec![service_ref(), deployment_ref()];
        let cases = [
            ProjectionReadinessInspection::Ready(default_backend_endpoint()),
            ProjectionReadinessInspection::Unready {
                reason: "no_ready_endpoints".to_owned(),
            },
            ProjectionReadinessInspection::NotObserved,
        ];

        for expected in cases {
            let client = FakeKubernetesClient::default();
            client.set_readiness_inspection(expected.clone());
            let retrying = retrying_client(client.clone());

            let observed = retrying
                .inspect_readiness(&refs)
                .await
                .expect("readiness inspection forwards through retrying client");

            assert_eq!(observed, expected);
            assert_eq!(
                client.operations(),
                vec![FakeOperation::InspectReadiness(refs.clone())]
            );
        }
    }

    #[tokio::test]
    async fn readiness_wait_failure_returns_typed_error_with_rendered_refs() {
        let client = FakeKubernetesClient::default();
        let refs = vec![service_ref(), deployment_ref()];
        client.fail_next(
            FakeOperation::WaitReadiness(refs.clone()),
            KubernetesClientError::new("readiness wait failed"),
        );

        let error = KubernetesMaterializer::new(client)
            .wait_for_readiness(&refs)
            .await
            .expect_err("readiness wait failure is returned");

        assert_eq!(
            error,
            MaterializerError::ReadinessWait {
                rendered_objects: refs,
                source: KubernetesClientError::new("readiness wait failed"),
            }
        );
    }

    #[test]
    fn rendered_object_refs_follow_apply_order() {
        let refs = rendered_object_refs(&stateful_manifest()).expect("refs derive");

        assert_eq!(
            refs,
            vec![
                object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556"),
                object_ref("v1", "PersistentVolumeClaim", "data", "pvc-acme-2e1ac556"),
                object_ref("v1", "Service", "data", "db-acme-2e1ac556"),
                object_ref("apps/v1", "StatefulSet", "data", "db-acme-2e1ac556"),
            ]
        );
    }

    #[test]
    fn delete_order_reverses_apply_order_and_puts_workloads_last() {
        let refs = rendered_object_refs(&stateful_manifest()).expect("refs derive");

        let kinds = delete_order(&refs)
            .into_iter()
            .map(|object| object.kind.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            kinds,
            [
                "Service",
                "PersistentVolumeClaim",
                "PersistentVolume",
                "StatefulSet"
            ]
        );
    }

    #[tokio::test]
    async fn created_objects_carry_ownership_and_generation_labels() {
        let client = FakeKubernetesClient::default();

        KubernetesMaterializer::new(client.clone())
            .apply_manifest(&deployment_manifest(), &[])
            .await
            .expect("deployment applies");

        let created = client.created();
        assert_eq!(created.len(), 2);
        for object in created {
            let labels = object_labels(&object);
            assert_eq!(labels["sleepypods.io/instance-id"], "instance-a");
            assert_eq!(labels["sleepypods.io/instance-generation"], "7");
        }
    }

    #[tokio::test]
    async fn stale_object_generation_manifest_is_rejected_before_apply() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let mut manifest = deployment_manifest();
        let deployment = manifest
            .objects
            .iter_mut()
            .find_map(|object| match &mut object.object {
                KubernetesObject::Deployment(deployment) => Some(deployment),
                _ => None,
            })
            .expect("deployment object");
        deployment.metadata.labels.insert(
            "sleepypods.io/instance-generation".to_owned(),
            "6".to_owned(),
        );

        let error = materializer
            .apply_manifest(&manifest, &[])
            .await
            .expect_err("stale generation label is rejected");

        assert_invalid_manifest(
            error,
            "Deployment apps/app-acme-69856ec0 metadata sleepypods.io/instance-generation must be \"7\"",
        );
        assert!(
            client.operations().is_empty(),
            "stale manifest must not be applied"
        );
    }

    #[tokio::test]
    async fn stale_template_generation_manifest_is_rejected_before_apply() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let mut manifest = deployment_manifest();
        let deployment = manifest
            .objects
            .iter_mut()
            .find_map(|object| match &mut object.object {
                KubernetesObject::Deployment(deployment) => Some(deployment),
                _ => None,
            })
            .expect("deployment object");
        deployment.spec.template.metadata.annotations.insert(
            "sleepypods.io/template-generation".to_owned(),
            "2".to_owned(),
        );

        let error = materializer
            .apply_manifest(&manifest, &[])
            .await
            .expect_err("stale template generation annotation is rejected");

        assert_invalid_manifest(
            error,
            "Deployment apps/app-acme-69856ec0 pod template metadata sleepypods.io/template-generation must be \"3\"",
        );
        assert!(
            client.operations().is_empty(),
            "stale manifest must not be applied"
        );
    }

    #[tokio::test]
    async fn invalid_rendered_name_is_rejected_before_apply() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let mut manifest = deployment_manifest();
        let service = manifest
            .objects
            .iter_mut()
            .find_map(|object| match &mut object.object {
                KubernetesObject::Service(service) => Some(service),
                _ => None,
            })
            .expect("service object");
        service.metadata.name = "Invalid_Service_Name".to_owned();

        let error = materializer
            .apply_manifest(&manifest, &[])
            .await
            .expect_err("invalid rendered name is rejected");

        assert_invalid_manifest(
            error,
            "v1 Service apps/Invalid_Service_Name name must be a Kubernetes DNS label",
        );
        assert!(
            client.operations().is_empty(),
            "invalid rendered names must not be applied"
        );
    }

    #[tokio::test]
    async fn persistent_volume_namespace_is_rejected_before_apply() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let mut manifest = stateful_manifest();
        let pv = manifest
            .objects
            .iter_mut()
            .find_map(|object| match &mut object.object {
                KubernetesObject::PersistentVolume(pv) => Some(pv),
                _ => None,
            })
            .expect("persistent volume object");
        pv.metadata.namespace = Some("data".to_owned());

        let error = materializer
            .apply_manifest(&manifest, &[])
            .await
            .expect_err("cluster-scoped PV namespace is rejected");

        assert_invalid_manifest(
            error,
            "PersistentVolume pv-acme-2e1ac556 must be cluster-scoped with an empty namespace",
        );
        assert!(
            client.operations().is_empty(),
            "invalid rendered refs must not be applied"
        );
    }

    #[tokio::test]
    async fn duplicate_rendered_refs_are_rejected_before_apply() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let mut manifest = deployment_manifest();
        let duplicate = manifest.objects[0].clone();
        manifest.objects.push(duplicate);

        let error = materializer
            .apply_manifest(&manifest, &[])
            .await
            .expect_err("duplicate rendered refs are rejected");

        assert_invalid_manifest(
            error,
            "duplicate rendered Kubernetes object ref v1 Service apps/svc-acme-69856ec0",
        );
        assert!(
            client.operations().is_empty(),
            "duplicate rendered refs must not be applied"
        );
    }

    impl KubernetesMaterializerClient for FakeKubernetesClient {
        fn apply_object<'a>(
            &'a self,
            object: &'a KubernetesObject,
            precondition: Option<&'a LiveObjectIdentity>,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                let object_ref = rendered_object_ref(object);
                let mut inner = self.inner.lock().expect("fake client lock not poisoned");
                let operation = match precondition {
                    None => FakeOperation::Create(object_ref.clone()),
                    Some(identity) => FakeOperation::Patch(object_ref.clone(), identity.clone()),
                };
                inner.operations.push(operation.clone());
                if let Some(error) = take_queued_failure(&mut inner, &operation) {
                    return Err(error);
                }
                if precondition.is_none() {
                    inner.created.push(object.clone());
                    let key = live_key(&object_ref);
                    if !inner.removed_after_create.contains(&key) {
                        inner.live.insert(key, created_identity(&object_ref));
                    }
                }
                Ok(())
            })
        }

        fn delete_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
            precondition: &'a LiveObjectIdentity,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                let mut inner = self.inner.lock().expect("fake client lock not poisoned");
                let operation = FakeOperation::Delete(object.clone(), precondition.clone());
                inner.operations.push(operation.clone());
                if let Some(error) = take_queued_failure(&mut inner, &operation) {
                    return Err(error);
                }
                inner.live.remove(&live_key(object));
                Ok(())
            })
        }

        fn wait_for_readiness<'a>(
            &'a self,
            objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
            Box::pin(async move {
                let mut inner = self.inner.lock().expect("fake client lock not poisoned");
                let operation = FakeOperation::WaitReadiness(objects.to_vec());
                inner.operations.push(operation.clone());
                match take_queued_failure(&mut inner, &operation) {
                    Some(error) => Err(error),
                    None => Ok(default_backend_endpoint()),
                }
            })
        }

        fn inspect_readiness<'a>(
            &'a self,
            objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>>
        {
            Box::pin(async move {
                let mut inner = self.inner.lock().expect("fake client lock not poisoned");
                inner
                    .operations
                    .push(FakeOperation::InspectReadiness(objects.to_vec()));
                Ok(inner
                    .readiness_inspection
                    .clone()
                    .unwrap_or(ProjectionReadinessInspection::NotObserved))
            })
        }

        fn inspect_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>>
        {
            Box::pin(async move {
                let mut inner = self.inner.lock().expect("fake client lock not poisoned");
                let operation = FakeOperation::Inspect(object.clone());
                inner.operations.push(operation.clone());
                if let Some(error) = take_queued_failure(&mut inner, &operation) {
                    return Err(error);
                }
                Ok(match inner.live.get(&live_key(object)) {
                    Some(identity) => ProjectionObjectInspection::Present(LiveObjectMetadata {
                        persistent_volume_reclaim_policy: None,
                        identity: identity.clone(),
                        labels: Default::default(),
                        annotations: Default::default(),
                        deleting: false,
                        finalizers: Vec::new(),
                    }),
                    None => ProjectionObjectInspection::Missing,
                })
            })
        }
    }

    impl FakeKubernetesClient {
        fn operations(&self) -> Vec<FakeOperation> {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .operations
                .clone()
        }

        fn operations_on(&self, object: &RenderedObjectRef) -> Vec<FakeOperation> {
            self.operations()
                .into_iter()
                .filter(|operation| match operation {
                    FakeOperation::Create(target)
                    | FakeOperation::Patch(target, _)
                    | FakeOperation::Inspect(target)
                    | FakeOperation::Delete(target, _) => target == object,
                    FakeOperation::WaitReadiness(_) | FakeOperation::InspectReadiness(_) => false,
                })
                .collect()
        }

        fn created(&self) -> Vec<KubernetesObject> {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .created
                .clone()
        }

        fn remove_after_create(&self, object: &RenderedObjectRef) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .removed_after_create
                .insert(live_key(object));
        }

        fn set_readiness_inspection(&self, inspection: ProjectionReadinessInspection) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .readiness_inspection = Some(inspection);
        }

        fn fail_next(&self, operation: FakeOperation, error: KubernetesClientError) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .queued_failures
                .push_back((operation, error));
        }
    }

    fn take_queued_failure(
        inner: &mut FakeState,
        operation: &FakeOperation,
    ) -> Option<KubernetesClientError> {
        let (next_operation, _) = inner.queued_failures.front()?;
        if next_operation == operation {
            inner.queued_failures.pop_front().map(|(_, error)| error)
        } else {
            None
        }
    }

    fn live_key(object: &RenderedObjectRef) -> String {
        format!("{}/{}/{}", object.kind, object.namespace, object.name)
    }

    fn created_identity(object: &RenderedObjectRef) -> LiveObjectIdentity {
        LiveObjectIdentity {
            uid: format!("uid-{}-{}", object.kind, object.name),
            resource_version: "1".to_owned(),
        }
    }

    fn service_ref() -> RenderedObjectRef {
        object_ref("v1", "Service", "apps", "svc-acme-69856ec0")
    }

    fn deployment_ref() -> RenderedObjectRef {
        object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0")
    }

    fn retrying_client(
        client: FakeKubernetesClient,
    ) -> RetryingKubernetesMaterializerClient<FakeKubernetesClient> {
        RetryingKubernetesMaterializerClient::new(
            client,
            RetryPolicy::new(3, Duration::ZERO, Duration::ZERO),
        )
    }

    fn deployment_manifest() -> crate::manifest::RenderedManifest {
        render_manifests(RenderManifestRequest {
            template: &deployment_template(),
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: resolved_sleep_policy(),
            namespace: "apps",
            template_generation: Some(Generation::new(3)),
        })
        .expect("deployment renders")
    }

    fn stateful_manifest() -> crate::manifest::RenderedManifest {
        render_manifests(RenderManifestRequest {
            template: &stateful_template(),
            instance: &instance(
                "postgres-a",
                2,
                values([("tenant", "acme"), ("volume", "provider-vol-123")]),
            ),
            sleep_policy: resolved_sleep_policy(),
            namespace: "data",
            template_generation: None,
        })
        .expect("stateful workload renders")
    }

    fn resolved_sleep_policy() -> ResolvedSleepPolicy {
        ResolvedSleepPolicy {
            idle_timeout_ms: 300_000,
            idle_retry_backoff_ms: 5_000,
            drain_grace_timeout_ms: 30_000,
        }
    }

    fn deployment_template() -> ManifestTemplate {
        ManifestTemplate {
            workload: WorkloadTemplate {
                kind: WorkloadKind::Deployment,
                name: composed("app-", "tenant"),
                replicas: None,
                app_container: ContainerTemplate {
                    name: "app".to_owned(),
                    image: TemplateText::literal("example/app:1"),
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
            service: Some(ServiceTemplate {
                name: composed("svc-", "tenant"),
                ports: vec![ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            sidecar: sidecar_template(),
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        }
    }

    fn stateful_template() -> ManifestTemplate {
        ManifestTemplate {
            workload: WorkloadTemplate {
                kind: WorkloadKind::StatefulSet,
                name: composed("db-", "tenant"),
                replicas: Some(1),
                app_container: ContainerTemplate {
                    name: "postgres".to_owned(),
                    image: TemplateText::literal("postgres:17"),
                    ports: vec![ContainerPortTemplate {
                        name: Some("postgres".to_owned()),
                        container_port: 5432,
                    }],
                    env: Vec::new(),
                },
            },
            service: Some(ServiceTemplate {
                name: composed("db-", "tenant"),
                ports: vec![ServicePortTemplate {
                    name: Some("postgres".to_owned()),
                    port: 5432,
                    target_port: 5432,
                }],
            }),
            sidecar: sidecar_template(),
            volumes: vec![VolumeTemplate {
                name: "data".to_owned(),
                mount_path: TemplateText::literal("/var/lib/postgresql/data"),
                pv_name: composed("pv-", "tenant"),
                pvc_name: composed("pvc-", "tenant"),
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                capacity: TemplateText::literal("10Gi"),
                reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
                storage_class_name: Some(TemplateText::literal("manual")),
                source: PersistentVolumeSourceTemplate::Csi {
                    driver: TemplateText::literal("csi.example.com"),
                    volume_handle: TemplateText::instance_value("volume"),
                    fs_type: Some(TemplateText::literal("ext4")),
                    read_only: false,
                    volume_attributes: BTreeMap::new(),
                    controller_publish_secret_ref: None,
                    node_stage_secret_ref: None,
                    node_publish_secret_ref: None,
                    controller_expand_secret_ref: None,
                    node_expand_secret_ref: None,
                },
            }],
            raw_objects: Vec::new(),
        }
    }

    fn sidecar_template() -> SidecarTemplate {
        SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: None,
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

    fn backend_endpoint(uri: &str) -> BackendEndpoint {
        BackendEndpoint::new(uri).expect("valid backend endpoint")
    }

    fn default_backend_endpoint() -> BackendEndpoint {
        backend_endpoint("http://ready.backend")
    }

    fn object_labels(object: &KubernetesObject) -> &BTreeMap<String, String> {
        match object {
            KubernetesObject::Deployment(object) => &object.metadata.labels,
            KubernetesObject::StatefulSet(object) => &object.metadata.labels,
            KubernetesObject::Service(object) => &object.metadata.labels,
            KubernetesObject::Secret(object) => &object.metadata.labels,
            KubernetesObject::PersistentVolume(object) => &object.metadata.labels,
            KubernetesObject::PersistentVolumeClaim(object) => &object.metadata.labels,
            KubernetesObject::Raw(object) => &object.metadata.labels,
        }
    }

    fn assert_invalid_manifest(error: MaterializerError, expected_message: &str) {
        match error {
            MaterializerError::InvalidManifest { message } => {
                assert!(
                    message.contains(expected_message),
                    "message {message:?} should contain {expected_message:?}"
                );
            }
            other => panic!("expected invalid manifest error, got {other:?}"),
        }
    }

    fn composed(prefix: &str, field: &str) -> TemplateText {
        TemplateText::from_parts([
            TemplateTextPart::literal(prefix),
            TemplateTextPart::instance_value(field),
        ])
    }

    fn instance(id: &str, generation: u64, values: InstanceValues) -> InstanceRecord {
        InstanceRecord {
            id: InstanceId::new(id).expect("valid instance ID"),
            workload_class: WorkloadClassVersionRef::new(
                WorkloadClassId::new("web").expect("valid class ID"),
                Generation::new(1),
            ),
            values,
            state: InstanceState::Cold,
            generation: Generation::new(generation),
        }
    }

    fn values<const N: usize>(pairs: [(&str, &str); N]) -> InstanceValues {
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }
}
