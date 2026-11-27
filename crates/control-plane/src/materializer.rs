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
        ApplyOrder, KubernetesObject, RenderedManifest, RenderedManifestObject,
        ANNOTATION_TEMPLATE_GENERATION, LABEL_INSTANCE_GENERATION,
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

    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a crate::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a crate::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>>;

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
    retryable: bool,
    outcome_uncertain: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedMaterialization {
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub backend: BackendEndpoint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaterializerError {
    InvalidManifest {
        message: String,
    },
    Apply {
        object: Box<RenderedObjectRef>,
        applied_objects: Vec<RenderedObjectRef>,
        source: KubernetesClientError,
    },
    Delete {
        object: Box<RenderedObjectRef>,
        source: KubernetesClientError,
    },
    PvcBoundWait {
        namespace: String,
        name: String,
        applied_objects: Vec<RenderedObjectRef>,
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

    pub async fn apply_manifest_until_ready(
        &self,
        manifest: &RenderedManifest,
    ) -> Result<AppliedMaterialization, MaterializerError> {
        let rendered_objects = self.apply_manifest(manifest).await?;
        let backend = self.wait_for_readiness(&rendered_objects).await?;

        Ok(AppliedMaterialization {
            rendered_objects,
            backend,
        })
    }

    pub fn rendered_object_refs(
        &self,
        manifest: &RenderedManifest,
    ) -> Result<Vec<RenderedObjectRef>, MaterializerError> {
        rendered_object_refs(manifest)
    }

    pub async fn apply_manifest(
        &self,
        manifest: &RenderedManifest,
    ) -> Result<Vec<RenderedObjectRef>, MaterializerError> {
        let mut observations = Vec::new();
        for object_ref in rendered_object_refs(manifest)? {
            let mut observation =
                crate::projection::ProjectionObservation::missing(object_ref.clone());
            if let ProjectionObjectInspection::Present(live) = self
                .client
                .inspect_object(&object_ref)
                .await
                .map_err(|source| MaterializerError::Apply {
                    object: Box::new(object_ref.clone()),
                    applied_objects: Vec::new(),
                    source,
                })?
            {
                observation.identity = Some(live.identity);
            }
            observations.push(observation);
        }
        self.apply_manifest_with_preconditions(manifest, &observations)
            .await
    }

    pub(crate) async fn apply_manifest_with_preconditions(
        &self,
        manifest: &RenderedManifest,
        observations: &[crate::projection::ProjectionObservation],
    ) -> Result<Vec<RenderedObjectRef>, MaterializerError> {
        let rendered_objects = rendered_object_refs(manifest)?;
        let objects = ordered_objects(manifest);
        let mut applied_refs = Vec::with_capacity(objects.len());

        for object in objects
            .iter()
            .filter(|object| object.apply_order == ApplyOrder::PersistentVolume)
        {
            match self.apply_object(&object.object, observations).await {
                Ok(object_ref) => applied_refs.push(object_ref),
                Err(error) => {
                    return Err(error.with_applied_objects(&applied_refs));
                }
            }
        }

        let mut pvc_refs = Vec::new();
        for object in objects
            .iter()
            .filter(|object| object.apply_order == ApplyOrder::PersistentVolumeClaim)
        {
            let object_ref = match self.apply_object(&object.object, observations).await {
                Ok(object_ref) => object_ref,
                Err(error) => {
                    return Err(error.with_applied_objects(&applied_refs));
                }
            };
            pvc_refs.push(object_ref.clone());
            applied_refs.push(object_ref);
        }

        for pvc in pvc_refs {
            if let Err(error) = self.wait_for_pvc_bound(&pvc).await {
                return Err(error.with_applied_objects(&applied_refs));
            }
        }

        for apply_order in [
            ApplyOrder::Secret,
            ApplyOrder::Service,
            ApplyOrder::Workload,
        ] {
            for object in objects
                .iter()
                .filter(|object| object.apply_order == apply_order)
            {
                match self.apply_object(&object.object, observations).await {
                    Ok(object_ref) => applied_refs.push(object_ref),
                    Err(error) => {
                        return Err(error.with_applied_objects(&applied_refs));
                    }
                }
            }
        }

        Ok(rendered_objects)
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

    pub async fn delete_rendered_objects(
        &self,
        objects: &[RenderedObjectRef],
    ) -> Result<(), MaterializerError> {
        if let Some(pvc) = objects
            .iter()
            .find(|object| object.kind == "PersistentVolumeClaim")
        {
            self.client
                .verify_retained_bindings(objects)
                .await
                .map_err(|source| MaterializerError::Delete {
                    object: Box::new(pvc.clone()),
                    source,
                })?;
        }
        for object in objects
            .iter()
            .filter(|object| object.kind == "PersistentVolume")
        {
            let inspection = self.client.inspect_object(object).await.map_err(|source| {
                MaterializerError::Delete {
                    object: Box::new(object.clone()),
                    source,
                }
            })?;
            if let ProjectionObjectInspection::Present(live) = inspection {
                if live.persistent_volume_reclaim_policy.as_deref() != Some("Retain") {
                    return Err(MaterializerError::Delete {
                        object: Box::new(object.clone()),
                        source: KubernetesClientError::new(
                            "managed static PV requires Retain before any cleanup, including PVC deletion",
                        ),
                    });
                }
            }
        }
        for object in delete_order(objects) {
            self.delete_rendered_object(object).await?;
        }

        Ok(())
    }

    pub async fn delete_rendered_object(
        &self,
        object: &RenderedObjectRef,
    ) -> Result<(), MaterializerError> {
        let inspection = self.client.inspect_object(object).await.map_err(|source| {
            MaterializerError::Delete {
                object: Box::new(object.clone()),
                source,
            }
        })?;
        match inspection {
            ProjectionObjectInspection::Missing => Ok(()),
            ProjectionObjectInspection::Present(live) => {
                self.delete_rendered_object_conditionally(object, &live.identity)
                    .await
            }
        }
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
    ) -> Result<RenderedObjectRef, MaterializerError> {
        let object_ref = rendered_object_ref(object);
        let precondition = observations
            .iter()
            .find(|observation| observation.object_ref == object_ref)
            .and_then(|observation| observation.identity.as_ref());
        self.client
            .apply_object(object, precondition)
            .await
            .map_err(|source| MaterializerError::Apply {
                object: Box::new(object_ref.clone()),
                applied_objects: Vec::new(),
                source,
            })?;
        Ok(object_ref)
    }

    async fn wait_for_pvc_bound(&self, pvc: &RenderedObjectRef) -> Result<(), MaterializerError> {
        self.client
            .wait_for_pvc_bound(&pvc.namespace, &pvc.name)
            .await
            .map_err(|source| MaterializerError::PvcBoundWait {
                namespace: pvc.namespace.clone(),
                name: pvc.name.clone(),
                applied_objects: Vec::new(),
                source,
            })
    }
}

impl MaterializerError {
    pub fn applied_objects_before_failure(&self) -> Option<&[RenderedObjectRef]> {
        match self {
            Self::Apply {
                applied_objects, ..
            }
            | Self::PvcBoundWait {
                applied_objects, ..
            } => Some(applied_objects),
            Self::ReadinessWait {
                rendered_objects, ..
            } => Some(rendered_objects),
            Self::InvalidManifest { .. } | Self::Delete { .. } => None,
        }
    }

    fn with_applied_objects(self, applied_objects: &[RenderedObjectRef]) -> Self {
        match self {
            Self::Apply { object, source, .. } => Self::Apply {
                object,
                applied_objects: applied_objects.to_vec(),
                source,
            },
            Self::PvcBoundWait {
                namespace,
                name,
                source,
                ..
            } => Self::PvcBoundWait {
                namespace,
                name,
                applied_objects: applied_objects.to_vec(),
                source,
            },
            other => other,
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

    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        let inner = &self.inner;
        let policy = self.policy;
        Box::pin(async move {
            policy
                .retry_if(
                    || inner.wait_for_pvc_bound(namespace, name),
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

fn is_workload_ref(object: &RenderedObjectRef) -> bool {
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
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            outcome_uncertain: false,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            outcome_uncertain: false,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn uncertain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            outcome_uncertain: true,
        }
    }

    pub fn outcome_uncertain(&self) -> bool {
        self.outcome_uncertain
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
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
            Self::Apply { object, source, .. } => {
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
            Self::PvcBoundWait {
                namespace,
                name,
                source,
                ..
            } => write!(
                f,
                "failed waiting for PersistentVolumeClaim {namespace}/{name} to become Bound: {source}"
            ),
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
            | Self::PvcBoundWait { source, .. }
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
        collections::{BTreeMap, VecDeque},
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
        projection::ProjectionReadinessInspection,
        sleep_policy::ResolvedSleepPolicy,
        workload::WorkloadClassVersionRef,
    };

    use super::{
        rendered_object_ref, AppliedMaterialization, BackendEndpoint, KubernetesClientError,
        KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
        KubernetesMaterializerClient, MaterializerError, RenderedObjectRef, RetryPolicy,
        RetryingKubernetesMaterializerClient,
    };

    #[derive(Clone, Debug, Default)]
    struct FakeKubernetesClient {
        inner: Arc<Mutex<FakeState>>,
    }

    #[derive(Clone, Debug, Default)]
    struct FakeState {
        operations: Vec<FakeOperation>,
        applied_objects: Vec<KubernetesObject>,
        queued_failures: VecDeque<(FakeOperation, KubernetesClientError)>,
        readiness_backend: Option<BackendEndpoint>,
        readiness_inspection: Option<ProjectionReadinessInspection>,
        fail_pvc_wait: Option<(String, String)>,
        fail_delete: Option<RenderedObjectRef>,
        fail_apply: Option<RenderedObjectRef>,
        fail_readiness: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum FakeOperation {
        Apply(RenderedObjectRef),
        WaitPvcBound { namespace: String, name: String },
        WaitReadiness(Vec<RenderedObjectRef>),
        InspectReadiness(Vec<RenderedObjectRef>),
        Delete(RenderedObjectRef),
    }

    #[test]
    fn kubernetes_materializer_client_trait_is_dyn_safe() {
        fn assert_dyn_safe<T: KubernetesMaterializerClient + ?Sized>() {}

        assert_dyn_safe::<dyn KubernetesMaterializerClient>();
    }

    #[test]
    fn new_client_errors_are_permanent_by_default() {
        let error = KubernetesClientError::new("permanent");

        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn applies_sidecar_token_secret_before_the_workload() {
        let mut manifest = deployment_manifest();
        let metadata = match &manifest
            .objects
            .iter()
            .find(|rendered| matches!(rendered.object, KubernetesObject::Service(_)))
            .unwrap()
            .object
        {
            KubernetesObject::Service(service) => service.metadata.clone(),
            _ => unreachable!(),
        };
        let mut metadata = metadata;
        metadata.name = "sidecar-token".into();
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
            .apply_manifest(&manifest)
            .await
            .unwrap();
        let operations = client.operations();
        let secret = operations.iter().position(|operation| matches!(operation, FakeOperation::Apply(object) if object.kind == "Secret")).unwrap();
        let workload = operations.iter().position(|operation| matches!(operation, FakeOperation::Apply(object) if object.kind == "Deployment")).unwrap();
        assert!(secret < workload);
    }

    #[tokio::test]
    async fn retrying_materializer_client_retries_transient_apply_failure_until_success() {
        let client = FakeKubernetesClient::default();
        let service_ref = object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf");
        client.fail_next(
            FakeOperation::Apply(service_ref.clone()),
            KubernetesClientError::transient("apply conflict"),
        );
        let materializer = KubernetesMaterializer::new(retrying_client(client.clone()));

        let refs = materializer
            .apply_manifest(&deployment_manifest())
            .await
            .expect("transient apply failure is retried");

        assert_eq!(
            refs,
            vec![
                service_ref.clone(),
                object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf"),
            ]
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(service_ref.clone()),
                FakeOperation::Apply(service_ref),
                FakeOperation::Apply(object_ref(
                    "apps/v1",
                    "Deployment",
                    "apps",
                    "app-acme-69856ec0faae6daf"
                )),
            ]
        );
    }

    #[tokio::test]
    async fn retrying_materializer_client_retries_transient_delete_failure_until_success() {
        let client = FakeKubernetesClient::default();
        let service_ref = object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf");
        let workload_ref = object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf");
        client.fail_next(
            FakeOperation::Delete(workload_ref.clone()),
            KubernetesClientError::transient("delete timeout"),
        );
        let materializer = KubernetesMaterializer::new(retrying_client(client.clone()));

        materializer
            .delete_rendered_objects(&[service_ref.clone(), workload_ref.clone()])
            .await
            .expect("transient delete failure is retried");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Delete(service_ref),
                FakeOperation::Delete(workload_ref.clone()),
                FakeOperation::Delete(workload_ref),
            ]
        );
    }

    #[tokio::test]
    async fn retrying_materializer_client_retries_transient_pvc_wait_failure_until_success() {
        let client = FakeKubernetesClient::default();
        let pvc_wait = FakeOperation::WaitPvcBound {
            namespace: "data".to_owned(),
            name: "pvc-acme-2e1ac556311afdd5".to_owned(),
        };
        client.fail_next(
            pvc_wait.clone(),
            KubernetesClientError::transient("pvc get failed"),
        );
        let materializer = KubernetesMaterializer::new(retrying_client(client.clone()));

        materializer
            .apply_manifest(&stateful_manifest())
            .await
            .expect("transient pvc wait failure is retried");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(object_ref(
                    "v1",
                    "PersistentVolume",
                    "",
                    "pv-acme-2e1ac556311afdd5"
                )),
                FakeOperation::Apply(object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme-2e1ac556311afdd5"
                )),
                pvc_wait.clone(),
                pvc_wait,
                FakeOperation::Apply(object_ref(
                    "v1",
                    "Service",
                    "data",
                    "db-acme-2e1ac556311afdd5"
                )),
                FakeOperation::Apply(object_ref(
                    "apps/v1",
                    "StatefulSet",
                    "data",
                    "db-acme-2e1ac556311afdd5"
                )),
            ]
        );
    }

    #[tokio::test]
    async fn retrying_materializer_client_retries_transient_readiness_failure_until_success() {
        let client = FakeKubernetesClient::default();
        let backend =
            backend_endpoint("http://svc-acme-69856ec0faae6daf.apps.svc.cluster.local:80");
        client.set_readiness_backend(backend.clone());
        let refs = vec![
            object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf"),
        ];
        client.fail_next(
            FakeOperation::WaitReadiness(refs.clone()),
            KubernetesClientError::transient("endpoint slice list failed"),
        );
        let materializer = KubernetesMaterializer::new(retrying_client(client.clone()));

        let applied = materializer
            .apply_manifest_until_ready(&deployment_manifest())
            .await
            .expect("transient readiness failure is retried");

        assert_eq!(
            applied,
            AppliedMaterialization {
                rendered_objects: refs.clone(),
                backend
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(refs[0].clone()),
                FakeOperation::Apply(refs[1].clone()),
                FakeOperation::WaitReadiness(refs.clone()),
                FakeOperation::WaitReadiness(refs),
            ]
        );
    }

    #[tokio::test]
    async fn retrying_materializer_client_preserves_readiness_inspection() {
        let refs = vec![
            object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf"),
        ];
        let cases = [
            ProjectionReadinessInspection::Ready(backend_endpoint(
                "http://svc-acme-69856ec0faae6daf.apps.svc.cluster.local:80",
            )),
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
    async fn retrying_materializer_client_does_not_retry_permanent_apply_failure() {
        let client = FakeKubernetesClient::default();
        let service_ref = object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf");
        client.fail_next(
            FakeOperation::Apply(service_ref.clone()),
            KubernetesClientError::new("invalid object"),
        );
        let materializer = KubernetesMaterializer::new(retrying_client(client.clone()));

        let error = materializer
            .apply_manifest(&deployment_manifest())
            .await
            .expect_err("permanent apply failure is not retried");

        assert_eq!(
            error,
            MaterializerError::Apply {
                object: Box::new(service_ref.clone()),
                applied_objects: Vec::new(),
                source: KubernetesClientError::new("invalid object"),
            }
        );
        assert_eq!(client.operations(), vec![FakeOperation::Apply(service_ref)]);
    }

    #[tokio::test]
    async fn applies_stateful_manifest_with_pvc_bound_wait_before_service_and_workload() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = stateful_manifest();

        let refs = materializer
            .apply_manifest(&manifest)
            .await
            .expect("stateful manifest applies");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(object_ref(
                    "v1",
                    "PersistentVolume",
                    "",
                    "pv-acme-2e1ac556311afdd5"
                )),
                FakeOperation::Apply(object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme-2e1ac556311afdd5"
                )),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme-2e1ac556311afdd5".to_owned(),
                },
                FakeOperation::Apply(object_ref(
                    "v1",
                    "Service",
                    "data",
                    "db-acme-2e1ac556311afdd5"
                )),
                FakeOperation::Apply(object_ref(
                    "apps/v1",
                    "StatefulSet",
                    "data",
                    "db-acme-2e1ac556311afdd5"
                )),
            ]
        );
        assert_eq!(
            refs,
            vec![
                object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556311afdd5"),
                object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme-2e1ac556311afdd5"
                ),
                object_ref("v1", "Service", "data", "db-acme-2e1ac556311afdd5"),
                object_ref("apps/v1", "StatefulSet", "data", "db-acme-2e1ac556311afdd5"),
            ]
        );
    }

    #[tokio::test]
    async fn applies_deployment_service_before_workload_and_returns_refs_in_apply_order() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = deployment_manifest();

        let refs = materializer
            .apply_manifest(&manifest)
            .await
            .expect("deployment manifest applies");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(object_ref(
                    "v1",
                    "Service",
                    "apps",
                    "svc-acme-69856ec0faae6daf"
                )),
                FakeOperation::Apply(object_ref(
                    "apps/v1",
                    "Deployment",
                    "apps",
                    "app-acme-69856ec0faae6daf"
                )),
            ]
        );
        assert_eq!(
            refs,
            vec![
                object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf"),
                object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf"),
            ]
        );
    }

    #[tokio::test]
    async fn applies_deployment_manifest_until_ready_after_all_objects_and_returns_backend() {
        let client = FakeKubernetesClient::default();
        let backend =
            backend_endpoint("http://svc-acme-69856ec0faae6daf.apps.svc.cluster.local:80");
        client.set_readiness_backend(backend.clone());
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = deployment_manifest();
        let refs = vec![
            object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf"),
        ];

        let applied = materializer
            .apply_manifest_until_ready(&manifest)
            .await
            .expect("deployment manifest applies and becomes ready");

        assert_eq!(
            applied,
            AppliedMaterialization {
                rendered_objects: refs.clone(),
                backend
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(refs[0].clone()),
                FakeOperation::Apply(refs[1].clone()),
                FakeOperation::WaitReadiness(refs),
            ]
        );
    }

    #[tokio::test]
    async fn applies_stateful_manifest_until_ready_after_pvc_bound_service_and_workload() {
        let client = FakeKubernetesClient::default();
        let backend =
            backend_endpoint("tcp://db-acme-2e1ac556311afdd5.data.svc.cluster.local:5432");
        client.set_readiness_backend(backend.clone());
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = stateful_manifest();
        let refs = vec![
            object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556311afdd5"),
            object_ref(
                "v1",
                "PersistentVolumeClaim",
                "data",
                "pvc-acme-2e1ac556311afdd5",
            ),
            object_ref("v1", "Service", "data", "db-acme-2e1ac556311afdd5"),
            object_ref("apps/v1", "StatefulSet", "data", "db-acme-2e1ac556311afdd5"),
        ];

        let applied = materializer
            .apply_manifest_until_ready(&manifest)
            .await
            .expect("stateful manifest applies and becomes ready");

        assert_eq!(
            applied,
            AppliedMaterialization {
                rendered_objects: refs.clone(),
                backend
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(refs[0].clone()),
                FakeOperation::Apply(refs[1].clone()),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme-2e1ac556311afdd5".to_owned(),
                },
                FakeOperation::Apply(refs[2].clone()),
                FakeOperation::Apply(refs[3].clone()),
                FakeOperation::WaitReadiness(refs),
            ]
        );
    }

    #[tokio::test]
    async fn apply_failure_returns_typed_error_for_failed_object() {
        let client = FakeKubernetesClient::default();
        let service_ref = object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf");
        client.fail_apply(service_ref.clone());
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = materializer
            .apply_manifest(&deployment_manifest())
            .await
            .expect_err("service apply failure stops materialization");

        assert_eq!(
            error,
            MaterializerError::Apply {
                object: Box::new(service_ref.clone()),
                applied_objects: Vec::new(),
                source: KubernetesClientError::new("apply failed"),
            }
        );
        assert_eq!(client.operations(), vec![FakeOperation::Apply(service_ref)]);
    }

    #[tokio::test]
    async fn apply_failure_after_partial_stateful_apply_retains_inventory_for_guarded_cleanup() {
        let client = FakeKubernetesClient::default();
        let service_ref = object_ref("v1", "Service", "data", "db-acme-2e1ac556311afdd5");
        let pvc_ref = object_ref(
            "v1",
            "PersistentVolumeClaim",
            "data",
            "pvc-acme-2e1ac556311afdd5",
        );
        let pv_ref = object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556311afdd5");
        client.fail_apply(service_ref.clone());
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = materializer
            .apply_manifest(&stateful_manifest())
            .await
            .expect_err("service apply failure stops stateful materialization");

        assert_eq!(
            error,
            MaterializerError::Apply {
                object: Box::new(service_ref.clone()),
                applied_objects: vec![pv_ref.clone(), pvc_ref.clone()],
                source: KubernetesClientError::new("apply failed"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(pv_ref.clone()),
                FakeOperation::Apply(pvc_ref.clone()),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme-2e1ac556311afdd5".to_owned(),
                },
                FakeOperation::Apply(service_ref),
            ]
        );
    }

    #[tokio::test]
    async fn pvc_bound_wait_failure_prevents_service_and_workload_apply() {
        let client = FakeKubernetesClient::default();
        let pvc_ref = object_ref(
            "v1",
            "PersistentVolumeClaim",
            "data",
            "pvc-acme-2e1ac556311afdd5",
        );
        let pv_ref = object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556311afdd5");
        client.fail_pvc_wait("data", "pvc-acme-2e1ac556311afdd5");
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = materializer
            .apply_manifest(&stateful_manifest())
            .await
            .expect_err("pvc wait failure stops materialization");

        assert_eq!(
            error,
            MaterializerError::PvcBoundWait {
                namespace: "data".to_owned(),
                name: "pvc-acme-2e1ac556311afdd5".to_owned(),
                applied_objects: vec![pv_ref.clone(), pvc_ref.clone()],
                source: KubernetesClientError::new("pvc did not bind"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(pv_ref.clone()),
                FakeOperation::Apply(pvc_ref.clone()),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme-2e1ac556311afdd5".to_owned(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn pvc_bound_wait_failure_prevents_readiness_wait() {
        let client = FakeKubernetesClient::default();
        let pvc_ref = object_ref(
            "v1",
            "PersistentVolumeClaim",
            "data",
            "pvc-acme-2e1ac556311afdd5",
        );
        let pv_ref = object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556311afdd5");
        client.fail_pvc_wait("data", "pvc-acme-2e1ac556311afdd5");
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = materializer
            .apply_manifest_until_ready(&stateful_manifest())
            .await
            .expect_err("pvc wait failure stops ready materialization");

        assert_eq!(
            error,
            MaterializerError::PvcBoundWait {
                namespace: "data".to_owned(),
                name: "pvc-acme-2e1ac556311afdd5".to_owned(),
                applied_objects: vec![pv_ref.clone(), pvc_ref.clone()],
                source: KubernetesClientError::new("pvc did not bind"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(pv_ref.clone()),
                FakeOperation::Apply(pvc_ref.clone()),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme-2e1ac556311afdd5".to_owned(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn readiness_wait_failure_returns_typed_error_with_rendered_refs() {
        let client = FakeKubernetesClient::default();
        client.fail_readiness();
        let materializer = KubernetesMaterializer::new(client.clone());
        let refs = vec![
            object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf"),
        ];

        let error = materializer
            .apply_manifest_until_ready(&deployment_manifest())
            .await
            .expect_err("readiness wait failure stops ready materialization");

        assert_eq!(
            error,
            MaterializerError::ReadinessWait {
                rendered_objects: refs.clone(),
                source: KubernetesClientError::new("readiness wait failed"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(refs[0].clone()),
                FakeOperation::Apply(refs[1].clone()),
                FakeOperation::WaitReadiness(refs),
            ]
        );
    }

    #[tokio::test]
    async fn deletes_rendered_refs_with_workloads_last() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let refs = KubernetesMaterializer::new(FakeKubernetesClient::default())
            .apply_manifest(&stateful_manifest())
            .await
            .expect("refs are derived");

        materializer
            .delete_rendered_objects(&refs)
            .await
            .expect("refs delete");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Delete(object_ref(
                    "v1",
                    "Service",
                    "data",
                    "db-acme-2e1ac556311afdd5"
                )),
                FakeOperation::Delete(object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme-2e1ac556311afdd5"
                )),
                FakeOperation::Delete(object_ref(
                    "v1",
                    "PersistentVolume",
                    "",
                    "pv-acme-2e1ac556311afdd5"
                )),
                FakeOperation::Delete(object_ref(
                    "apps/v1",
                    "StatefulSet",
                    "data",
                    "db-acme-2e1ac556311afdd5"
                )),
            ]
        );
    }

    #[test]
    fn object_refs_use_expected_api_versions_kinds_names_and_namespaces() {
        let manifest = stateful_manifest();
        let refs = manifest
            .objects
            .iter()
            .map(|object| rendered_object_ref(&object.object))
            .collect::<Vec<_>>();

        assert_eq!(
            refs,
            vec![
                object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556311afdd5"),
                object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme-2e1ac556311afdd5"
                ),
                object_ref("v1", "Service", "data", "db-acme-2e1ac556311afdd5"),
                object_ref("apps/v1", "StatefulSet", "data", "db-acme-2e1ac556311afdd5"),
            ]
        );
    }

    #[test]
    fn rendered_object_refs_are_available_without_applying_or_waiting() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = stateful_manifest();

        let refs = materializer
            .rendered_object_refs(&manifest)
            .expect("refs derive");

        assert_eq!(
            refs,
            vec![
                object_ref("v1", "PersistentVolume", "", "pv-acme-2e1ac556311afdd5"),
                object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme-2e1ac556311afdd5"
                ),
                object_ref("v1", "Service", "data", "db-acme-2e1ac556311afdd5"),
                object_ref("apps/v1", "StatefulSet", "data", "db-acme-2e1ac556311afdd5"),
            ]
        );
        assert!(
            client.operations().is_empty(),
            "deriving rendered refs must not touch Kubernetes"
        );
    }

    #[tokio::test]
    async fn delete_failure_returns_typed_error_and_stops() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let service_ref = object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf");
        let workload_ref = object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf");
        client.fail_delete(service_ref.clone());

        let error = materializer
            .delete_rendered_objects(&[service_ref.clone(), workload_ref.clone()])
            .await
            .expect_err("delete failure stops cleanup");

        assert_eq!(
            error,
            MaterializerError::Delete {
                object: Box::new(service_ref.clone()),
                source: KubernetesClientError::new("delete failed"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![FakeOperation::Delete(service_ref)]
        );
    }

    #[tokio::test]
    async fn applied_objects_retain_ownership_and_generation_labels() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());

        materializer
            .apply_manifest(&deployment_manifest())
            .await
            .expect("deployment applies");

        for object in client.applied_objects() {
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
            .apply_manifest(&manifest)
            .await
            .expect_err("stale generation label is rejected");

        assert_invalid_manifest(
            error,
            "Deployment apps/app-acme-69856ec0faae6daf metadata sleepypods.io/instance-generation must be \"7\"",
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
            .apply_manifest(&manifest)
            .await
            .expect_err("stale template generation annotation is rejected");

        assert_invalid_manifest(
            error,
            "Deployment apps/app-acme-69856ec0faae6daf pod template metadata sleepypods.io/template-generation must be \"3\"",
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
            .apply_manifest(&manifest)
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
            .apply_manifest(&manifest)
            .await
            .expect_err("cluster-scoped PV namespace is rejected");

        assert_invalid_manifest(
            error,
            "PersistentVolume pv-acme-2e1ac556311afdd5 must be cluster-scoped with an empty namespace",
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
            .apply_manifest(&manifest)
            .await
            .expect_err("duplicate rendered refs are rejected");

        assert_invalid_manifest(
            error,
            "duplicate rendered Kubernetes object ref v1 Service apps/svc-acme-69856ec0faae6daf",
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
            _precondition: Option<&'a crate::projection::LiveObjectIdentity>,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            let object = object.clone();
            let client = self.clone();
            Box::pin(async move {
                let object_ref = rendered_object_ref(&object);
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                inner
                    .operations
                    .push(FakeOperation::Apply(object_ref.clone()));
                inner.applied_objects.push(object);
                if let Some(error) =
                    take_queued_failure(&mut inner, &FakeOperation::Apply(object_ref.clone()))
                {
                    return Err(error);
                }

                if inner.fail_apply.as_ref() == Some(&object_ref) {
                    Err(KubernetesClientError::new("apply failed"))
                } else {
                    Ok(())
                }
            })
        }

        fn delete_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
            _precondition: &'a crate::projection::LiveObjectIdentity,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            let object = object.clone();
            let client = self.clone();
            Box::pin(async move {
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                let operation = FakeOperation::Delete(object.clone());
                inner.operations.push(operation.clone());
                if let Some(error) = take_queued_failure(&mut inner, &operation) {
                    return Err(error);
                }

                if inner.fail_delete.as_ref() == Some(&object) {
                    Err(KubernetesClientError::new("delete failed"))
                } else {
                    Ok(())
                }
            })
        }

        fn wait_for_pvc_bound<'a>(
            &'a self,
            namespace: &'a str,
            name: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            let namespace = namespace.to_owned();
            let name = name.to_owned();
            let client = self.clone();
            Box::pin(async move {
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                let operation = FakeOperation::WaitPvcBound {
                    namespace: namespace.clone(),
                    name: name.clone(),
                };
                inner.operations.push(operation.clone());
                if let Some(error) = take_queued_failure(&mut inner, &operation) {
                    return Err(error);
                }

                if inner.fail_pvc_wait.as_ref() == Some(&(namespace, name)) {
                    Err(KubernetesClientError::new("pvc did not bind"))
                } else {
                    Ok(())
                }
            })
        }

        fn wait_for_readiness<'a>(
            &'a self,
            objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
            let objects = objects.to_vec();
            let client = self.clone();
            Box::pin(async move {
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                let operation = FakeOperation::WaitReadiness(objects);
                inner.operations.push(operation.clone());
                if let Some(error) = take_queued_failure(&mut inner, &operation) {
                    return Err(error);
                }

                if inner.fail_readiness {
                    Err(KubernetesClientError::new("readiness wait failed"))
                } else {
                    Ok(inner
                        .readiness_backend
                        .clone()
                        .unwrap_or_else(default_backend_endpoint))
                }
            })
        }

        fn inspect_readiness<'a>(
            &'a self,
            objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>>
        {
            let objects = objects.to_vec();
            let client = self.clone();
            Box::pin(async move {
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                let operation = FakeOperation::InspectReadiness(objects);
                inner.operations.push(operation.clone());
                if let Some(error) = take_queued_failure(&mut inner, &operation) {
                    return Err(error);
                }

                Ok(inner
                    .readiness_inspection
                    .clone()
                    .unwrap_or(ProjectionReadinessInspection::NotObserved))
            })
        }

        fn ensure_no_descendants<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
            _instance_id: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn inspect_object<'a>(
            &'a self,
            _object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<
            'a,
            KubernetesClientResult<crate::projection::ProjectionObjectInspection>,
        > {
            Box::pin(async {
                Ok(crate::projection::ProjectionObjectInspection::Present(
                    crate::projection::LiveObjectMetadata {
                        persistent_volume_reclaim_policy: Some("Retain".into()),
                        identity: crate::projection::LiveObjectIdentity {
                            uid: "test-uid".into(),
                            resource_version: "1".into(),
                        },
                        labels: Default::default(),
                        annotations: Default::default(),
                        deleting: false,
                        finalizers: Vec::new(),
                    },
                ))
            })
        }

        fn verify_retained_bindings<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> crate::materializer::KubernetesClientFuture<
            'a,
            crate::materializer::KubernetesClientResult<()>,
        > {
            Box::pin(async { Ok(()) })
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

        fn applied_objects(&self) -> Vec<KubernetesObject> {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .applied_objects
                .clone()
        }

        fn fail_pvc_wait(&self, namespace: &str, name: &str) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_pvc_wait = Some((namespace.to_owned(), name.to_owned()));
        }

        fn fail_apply(&self, object: RenderedObjectRef) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_apply = Some(object);
        }

        fn fail_delete(&self, object: RenderedObjectRef) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_delete = Some(object);
        }

        fn set_readiness_backend(&self, backend: BackendEndpoint) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .readiness_backend = Some(backend);
        }

        fn set_readiness_inspection(&self, inspection: ProjectionReadinessInspection) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .readiness_inspection = Some(inspection);
        }

        fn fail_readiness(&self) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_readiness = true;
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
