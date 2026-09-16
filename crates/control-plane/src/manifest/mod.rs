use std::{error::Error, fmt};

use crate::{instance::InstanceRecord, sleep_policy::ResolvedSleepPolicy};

mod bind;
mod objects;
mod render;
mod serialize;
mod template;

#[cfg(test)]
mod tests;

pub use objects::{
    ApplyOrder, Container, ContainerPort, CsiPersistentVolumeSource, CsiSecretReference,
    Deployment, DeploymentSpec, EnvVar, EnvVarSource, HostPathPersistentVolumeSource,
    HttpReadinessProbe, KubernetesObject, LabelSelector, ObjectMeta, PersistentVolume,
    PersistentVolumeClaim, PersistentVolumeClaimRef, PersistentVolumeClaimSpec,
    PersistentVolumeClaimVolumeSource, PersistentVolumeSource, PersistentVolumeSpec, PodSpec,
    PodTemplateMetadata, PodTemplateSpec, PodVolume, RawKubernetesObject, RenderedManifest,
    RenderedManifestObject, Secret, SecretKeyRef, Service, ServicePort, ServiceSpec, StatefulSet,
    StatefulSetSpec, VolumeMount, VolumeResourceRequirements,
};
pub use render::render_manifests;
pub(crate) use render::render_manifests_with_options;
pub use template::{
    ContainerPortTemplate, ContainerTemplate, CsiSecretRefTemplate, EnvVarTemplate,
    ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
    PersistentVolumeSourceTemplate, RawKubernetesManifestTemplate, ServicePortTemplate,
    ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart, VolumeTemplate, WorkloadKind,
    WorkloadTemplate,
};

pub(crate) const LABEL_INSTANCE_ID: &str = "sleepypods.io/instance-id";
pub(crate) const LABEL_INSTANCE_GENERATION: &str = "sleepypods.io/instance-generation";
pub(crate) const LABEL_WORKLOAD_CLASS_ID: &str = "sleepypods.io/workload-class-id";
pub(crate) const LABEL_WORKLOAD_CLASS_VERSION: &str = "sleepypods.io/workload-class-version";
pub(crate) const LABEL_WORKLOAD_NAME: &str = "sleepypods.io/workload-name";
pub(crate) const ANNOTATION_TEMPLATE_GENERATION: &str = "sleepypods.io/template-generation";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderManifestRequest<'a> {
    pub template: &'a ManifestTemplate,
    pub instance: &'a InstanceRecord,
    pub sleep_policy: ResolvedSleepPolicy,
    pub namespace: &'a str,
    pub template_generation: Option<crate::ids::Generation>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RenderManifestOptions<'a> {
    pub sidecar_control_plane_token: Option<&'a crate::auth::BearerToken>,
    pub sidecar_control_plane_endpoint: Option<&'a str>,
    pub sidecar_control_plane_ca: Option<&'a str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestRenderError {
    MissingInstanceValue {
        field: String,
    },
    InvalidName {
        field: &'static str,
        value: String,
    },
    InvalidField {
        field: &'static str,
        message: String,
    },
    InvalidReplicas {
        kind: WorkloadKind,
        replicas: u32,
        message: String,
    },
}

impl fmt::Display for ManifestRenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingInstanceValue { field } => {
                write!(f, "missing instance value {field:?}")
            }
            Self::InvalidName { field, value } => {
                write!(f, "{field} rendered invalid Kubernetes name {value:?}")
            }
            Self::InvalidField { field, message } => write!(f, "{field} is invalid: {message}"),
            Self::InvalidReplicas {
                kind,
                replicas,
                message,
            } => write!(f, "{kind:?} replicas {replicas} are invalid: {message}"),
        }
    }
}

impl Error for ManifestRenderError {}
