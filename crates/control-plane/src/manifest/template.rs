use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::instance::InstanceValues;

use super::ManifestRenderError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemplateText {
    parts: Vec<TemplateTextPart>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateTextPart {
    Literal(String),
    InstanceValue(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestTemplate {
    pub workload: WorkloadTemplate,
    pub sidecar: SidecarTemplate,
    pub service: Option<ServiceTemplate>,
    pub volumes: Vec<VolumeTemplate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub raw_objects: Vec<RawKubernetesManifestTemplate>,
}

impl ManifestTemplate {
    pub fn validate_storage_retention(&self) -> Result<(), ManifestRenderError> {
        if self
            .volumes
            .iter()
            .any(|volume| volume.reclaim_policy != PersistentVolumeReclaimPolicy::Retain)
        {
            return Err(ManifestRenderError::InvalidField { field: "volumes.reclaim_policy", message: "managed static volumes require Retain; provider-volume deletion is outside the lifecycle contract".into() });
        }
        for raw in &self.raw_objects {
            // Reading the manifest with a token in each instance value's place
            // puts a templated raw volume under the same retention checks as a
            // literal one. A token is not "Retain", so a class that lets an
            // instance choose the reclaim policy fails here.
            let document = super::bind::PlaceholderDocument::new(&raw.manifest);
            if let Ok(value) = serde_yaml::from_str::<serde_json::Value>(document.text()) {
                if value["kind"] == "PersistentVolume"
                    && value
                        .pointer("/spec/persistentVolumeReclaimPolicy")
                        .and_then(serde_json::Value::as_str)
                        != Some("Retain")
                {
                    return Err(ManifestRenderError::InvalidField {
                        field: "volumes.reclaim_policy",
                        message: "raw managed static PVs require explicit Retain".into(),
                    });
                }
                if value["kind"] == "PersistentVolumeClaim"
                    && value
                        .pointer("/spec/volumeName")
                        .and_then(serde_json::Value::as_str)
                        .is_none_or(str::is_empty)
                {
                    return Err(ManifestRenderError::InvalidField {
                        field: "volumes.static_binding",
                        message: "raw PVCs require explicit static bindings in managed inventory"
                            .into(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadTemplate {
    pub kind: WorkloadKind,
    pub name: TemplateText,
    pub replicas: Option<u32>,
    pub app_container: ContainerTemplate,
}

impl WorkloadTemplate {
    /// Every sidecar observation must describe the whole supported workload.
    pub fn validate_replicas(&self) -> Result<(), ManifestRenderError> {
        let replicas = self.replicas.unwrap_or(1);
        if replicas != 1 {
            return Err(ManifestRenderError::InvalidReplicas {
                kind: self.kind,
                replicas,
                message: "automatic sleep requires exactly one replica".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerTemplate {
    pub name: String,
    pub image: TemplateText,
    pub ports: Vec<ContainerPortTemplate>,
    pub env: Vec<EnvVarTemplate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerPortTemplate {
    pub name: Option<String>,
    pub container_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvVarTemplate {
    pub name: String,
    pub value: TemplateText,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarTemplate {
    pub name: String,
    pub image: TemplateText,
    pub listen_port: u16,
    pub mode: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceTemplate {
    pub name: TemplateText,
    pub ports: Vec<ServicePortTemplate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServicePortTemplate {
    pub name: Option<String>,
    pub port: u16,
    pub target_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeTemplate {
    pub name: String,
    pub mount_path: TemplateText,
    pub pv_name: TemplateText,
    pub pvc_name: TemplateText,
    pub access_modes: Vec<PersistentVolumeAccessMode>,
    pub capacity: TemplateText,
    pub reclaim_policy: PersistentVolumeReclaimPolicy,
    pub storage_class_name: Option<TemplateText>,
    pub source: PersistentVolumeSourceTemplate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawKubernetesManifestTemplate {
    pub manifest: TemplateText,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistentVolumeAccessMode {
    ReadWriteOnce,
    ReadOnlyMany,
    ReadWriteMany,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistentVolumeReclaimPolicy {
    Retain,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistentVolumeSourceTemplate {
    Csi {
        driver: TemplateText,
        volume_handle: TemplateText,
        fs_type: Option<TemplateText>,
        read_only: bool,
        volume_attributes: BTreeMap<String, TemplateText>,
        controller_publish_secret_ref: Option<Box<CsiSecretRefTemplate>>,
        node_stage_secret_ref: Option<Box<CsiSecretRefTemplate>>,
        node_publish_secret_ref: Option<Box<CsiSecretRefTemplate>>,
        controller_expand_secret_ref: Option<Box<CsiSecretRefTemplate>>,
        node_expand_secret_ref: Option<Box<CsiSecretRefTemplate>>,
    },
    HostPath {
        path: TemplateText,
        type_: Option<TemplateText>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsiSecretRefTemplate {
    pub name: TemplateText,
    pub namespace: TemplateText,
}

impl TemplateText {
    pub fn literal(value: impl Into<String>) -> Self {
        Self {
            parts: vec![TemplateTextPart::Literal(value.into())],
        }
    }

    pub fn instance_value(field: impl Into<String>) -> Self {
        Self {
            parts: vec![TemplateTextPart::InstanceValue(field.into())],
        }
    }

    pub fn from_parts(parts: impl Into<Vec<TemplateTextPart>>) -> Self {
        Self {
            parts: parts.into(),
        }
    }

    pub fn parts(&self) -> &[TemplateTextPart] {
        &self.parts
    }

    /// The text the class author fixed at the front, which renders the same for
    /// every instance. Empty when an instance value comes first.
    pub fn literal_prefix(&self) -> &str {
        match self.parts.first() {
            Some(TemplateTextPart::Literal(literal)) => literal,
            Some(TemplateTextPart::InstanceValue(_)) | None => "",
        }
    }

    pub fn render(&self, values: &InstanceValues) -> Result<String, ManifestRenderError> {
        let mut rendered = String::new();
        for part in &self.parts {
            match part {
                TemplateTextPart::Literal(value) => rendered.push_str(value),
                TemplateTextPart::InstanceValue(field) => {
                    let value = values.get(field).ok_or_else(|| {
                        ManifestRenderError::MissingInstanceValue {
                            field: field.clone(),
                        }
                    })?;
                    rendered.push_str(value);
                }
            }
        }
        Ok(rendered)
    }
}

impl TemplateTextPart {
    pub fn literal(value: impl Into<String>) -> Self {
        Self::Literal(value.into())
    }

    pub fn instance_value(field: impl Into<String>) -> Self {
        Self::InstanceValue(field.into())
    }
}
