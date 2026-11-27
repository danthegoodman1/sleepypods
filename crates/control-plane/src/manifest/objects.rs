use std::collections::BTreeMap;

use serde_json::Value;

use crate::ids::Generation;

use super::{PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedManifest {
    pub instance_generation: Generation,
    pub objects: Vec<RenderedManifestObject>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedManifestObject {
    pub apply_order: ApplyOrder,
    pub object: KubernetesObject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ApplyOrder {
    PersistentVolume,
    PersistentVolumeClaim,
    Secret,
    Service,
    Workload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubernetesObject {
    Deployment(Deployment),
    StatefulSet(StatefulSet),
    Service(Service),
    Secret(Secret),
    PersistentVolume(Box<PersistentVolume>),
    PersistentVolumeClaim(PersistentVolumeClaim),
    Raw(RawKubernetesObject),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub name: String,
    pub namespace: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawKubernetesObject {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub pod_template_metadata: Option<PodTemplateMetadata>,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deployment {
    pub metadata: ObjectMeta,
    pub spec: DeploymentSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeploymentSpec {
    pub replicas: u32,
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatefulSet {
    pub metadata: ObjectMeta,
    pub spec: StatefulSetSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatefulSetSpec {
    pub replicas: u32,
    pub service_name: String,
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelSelector {
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodTemplateSpec {
    pub metadata: PodTemplateMetadata,
    pub spec: PodSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodTemplateMetadata {
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodSpec {
    pub containers: Vec<Container>,
    pub volumes: Vec<PodVolume>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Container {
    pub name: String,
    pub image: String,
    pub ports: Vec<ContainerPort>,
    pub env: Vec<EnvVar>,
    pub volume_mounts: Vec<VolumeMount>,
    pub readiness_probe: Option<HttpReadinessProbe>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpReadinessProbe {
    pub path: String,
    pub port: u16,
    pub period_seconds: u32,
    pub timeout_seconds: u32,
    pub failure_threshold: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerPort {
    pub name: Option<String>,
    pub container_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
    pub value_from: Option<EnvVarSource>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvVarSource {
    SecretKeyRef(SecretKeyRef),
    FieldRef { field_path: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretKeyRef {
    pub name: String,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodVolume {
    pub name: String,
    pub persistent_volume_claim: PersistentVolumeClaimVolumeSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaimVolumeSource {
    pub claim_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    pub metadata: ObjectMeta,
    pub spec: ServiceSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSpec {
    pub selector: BTreeMap<String, String>,
    pub ports: Vec<ServicePort>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Secret {
    pub metadata: ObjectMeta,
    pub type_: String,
    pub string_data: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServicePort {
    pub name: Option<String>,
    pub port: u16,
    pub target_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolume {
    pub metadata: ObjectMeta,
    pub spec: PersistentVolumeSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeSpec {
    pub capacity: String,
    pub access_modes: Vec<PersistentVolumeAccessMode>,
    pub persistent_volume_reclaim_policy: PersistentVolumeReclaimPolicy,
    pub storage_class_name: Option<String>,
    pub claim_ref: PersistentVolumeClaimRef,
    pub source: PersistentVolumeSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaimRef {
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PersistentVolumeSource {
    Csi(Box<CsiPersistentVolumeSource>),
    HostPath(HostPathPersistentVolumeSource),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsiPersistentVolumeSource {
    pub driver: String,
    pub volume_handle: String,
    pub fs_type: Option<String>,
    pub read_only: bool,
    pub volume_attributes: BTreeMap<String, String>,
    pub controller_publish_secret_ref: Option<CsiSecretReference>,
    pub node_stage_secret_ref: Option<CsiSecretReference>,
    pub node_publish_secret_ref: Option<CsiSecretReference>,
    pub controller_expand_secret_ref: Option<CsiSecretReference>,
    pub node_expand_secret_ref: Option<CsiSecretReference>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsiSecretReference {
    pub name: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPathPersistentVolumeSource {
    pub path: String,
    pub type_: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaim {
    pub metadata: ObjectMeta,
    pub spec: PersistentVolumeClaimSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaimSpec {
    pub access_modes: Vec<PersistentVolumeAccessMode>,
    pub resources: VolumeResourceRequirements,
    pub storage_class_name: Option<String>,
    pub volume_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeResourceRequirements {
    pub requests_storage: String,
}

impl KubernetesObject {
    pub fn kind(&self) -> &str {
        match self {
            Self::Deployment(_) => "Deployment",
            Self::StatefulSet(_) => "StatefulSet",
            Self::Service(_) => "Service",
            Self::Secret(_) => "Secret",
            Self::PersistentVolume(_) => "PersistentVolume",
            Self::PersistentVolumeClaim(_) => "PersistentVolumeClaim",
            Self::Raw(object) => object.kind.as_str(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Deployment(object) => &object.metadata.name,
            Self::StatefulSet(object) => &object.metadata.name,
            Self::Service(object) => &object.metadata.name,
            Self::Secret(object) => &object.metadata.name,
            Self::PersistentVolume(object) => &object.metadata.name,
            Self::PersistentVolumeClaim(object) => &object.metadata.name,
            Self::Raw(object) => &object.metadata.name,
        }
    }
}
