use serde_json::{json, Map, Value};

use super::{
    Container, ContainerPort, CsiPersistentVolumeSource, CsiSecretReference, Deployment, EnvVar,
    HostPathPersistentVolumeSource, KubernetesObject, ObjectMeta, PersistentVolume,
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeReclaimPolicy,
    PersistentVolumeSource, PodTemplateMetadata, PodTemplateSpec, PodVolume, RawKubernetesObject,
    RenderedManifest, RenderedManifestObject, Secret, Service, ServicePort, StatefulSet,
    VolumeMount,
};

impl RenderedManifest {
    pub fn to_kubernetes_json_values(&self) -> Vec<Value> {
        self.objects
            .iter()
            .map(RenderedManifestObject::to_kubernetes_json)
            .collect()
    }
}

impl RenderedManifestObject {
    pub fn to_kubernetes_json(&self) -> Value {
        self.object.to_kubernetes_json()
    }
}

impl KubernetesObject {
    pub fn api_version(&self) -> &str {
        match self {
            Self::Deployment(_) | Self::StatefulSet(_) => "apps/v1",
            Self::Service(_)
            | Self::Secret(_)
            | Self::PersistentVolume(_)
            | Self::PersistentVolumeClaim(_) => "v1",
            Self::Raw(object) => object.api_version.as_str(),
        }
    }

    pub fn to_kubernetes_json(&self) -> Value {
        match self {
            Self::Deployment(object) => deployment_to_value(object),
            Self::StatefulSet(object) => stateful_set_to_value(object),
            Self::Service(object) => service_to_value(object),
            Self::Secret(object) => secret_to_value(object),
            Self::PersistentVolume(object) => persistent_volume_to_value(object),
            Self::PersistentVolumeClaim(object) => persistent_volume_claim_to_value(object),
            Self::Raw(object) => raw_object_to_value(object),
        }
    }
}

fn deployment_to_value(object: &Deployment) -> Value {
    object_to_value(
        "apps/v1",
        "Deployment",
        &object.metadata,
        json!({
            "replicas": object.spec.replicas,
            "strategy": { "type": "Recreate" },
            "selector": label_selector_to_value(&object.spec.selector.match_labels),
            "template": pod_template_to_value(&object.spec.template),
        }),
    )
}

fn stateful_set_to_value(object: &StatefulSet) -> Value {
    object_to_value(
        "apps/v1",
        "StatefulSet",
        &object.metadata,
        json!({
            "replicas": object.spec.replicas,
            "serviceName": object.spec.service_name,
            "selector": label_selector_to_value(&object.spec.selector.match_labels),
            "template": pod_template_to_value(&object.spec.template),
        }),
    )
}

fn service_to_value(object: &Service) -> Value {
    object_to_value(
        "v1",
        "Service",
        &object.metadata,
        json!({
            "selector": object.spec.selector,
            "ports": object.spec.ports.iter().map(service_port_to_value).collect::<Vec<_>>(),
        }),
    )
}

fn secret_to_value(object: &Secret) -> Value {
    let mut value = object_to_value("v1", "Secret", &object.metadata, Value::Object(Map::new()));
    value["type"] = json!(object.type_);
    value["stringData"] = json!(object.string_data);
    value.as_object_mut().expect("object value").remove("spec");
    value
}

fn persistent_volume_to_value(object: &PersistentVolume) -> Value {
    let mut spec = Map::new();
    spec.insert(
        "capacity".to_owned(),
        json!({
            "storage": object.spec.capacity,
        }),
    );
    spec.insert(
        "accessModes".to_owned(),
        access_modes_to_value(&object.spec.access_modes),
    );
    spec.insert(
        "persistentVolumeReclaimPolicy".to_owned(),
        json!(reclaim_policy_to_str(
            object.spec.persistent_volume_reclaim_policy
        )),
    );
    insert_optional_string(
        &mut spec,
        "storageClassName",
        object.spec.storage_class_name.as_deref(),
    );
    spec.insert(
        "claimRef".to_owned(),
        json!({
            "namespace": object.spec.claim_ref.namespace,
            "name": object.spec.claim_ref.name,
        }),
    );
    match &object.spec.source {
        PersistentVolumeSource::Csi(source) => {
            spec.insert("csi".to_owned(), csi_source_to_value(source));
        }
        PersistentVolumeSource::HostPath(source) => {
            spec.insert("hostPath".to_owned(), host_path_source_to_value(source));
        }
    }

    object_to_value(
        "v1",
        "PersistentVolume",
        &object.metadata,
        Value::Object(spec),
    )
}

fn persistent_volume_claim_to_value(object: &PersistentVolumeClaim) -> Value {
    let mut spec = Map::new();
    spec.insert(
        "accessModes".to_owned(),
        access_modes_to_value(&object.spec.access_modes),
    );
    spec.insert(
        "resources".to_owned(),
        json!({
            "requests": {
                "storage": object.spec.resources.requests_storage,
            },
        }),
    );
    insert_optional_string(
        &mut spec,
        "storageClassName",
        object.spec.storage_class_name.as_deref(),
    );
    spec.insert("volumeName".to_owned(), json!(object.spec.volume_name));

    object_to_value(
        "v1",
        "PersistentVolumeClaim",
        &object.metadata,
        Value::Object(spec),
    )
}

fn object_to_value(api_version: &str, kind: &str, metadata: &ObjectMeta, spec: Value) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": metadata_to_value(metadata),
        "spec": spec,
    })
}

fn metadata_to_value(metadata: &ObjectMeta) -> Value {
    let mut value = Map::new();
    value.insert("name".to_owned(), json!(metadata.name));
    insert_optional_string(&mut value, "namespace", metadata.namespace.as_deref());
    value.insert("labels".to_owned(), json!(metadata.labels));
    value.insert("annotations".to_owned(), json!(metadata.annotations));
    Value::Object(value)
}

fn raw_object_to_value(object: &RawKubernetesObject) -> Value {
    let mut value = object.value.clone();
    value["apiVersion"] = json!(object.api_version);
    value["kind"] = json!(object.kind);
    patch_raw_metadata(&mut value["metadata"], &object.metadata);
    if let Some(metadata) = &object.pod_template_metadata {
        patch_raw_pod_template_metadata(&mut value["spec"]["template"]["metadata"], metadata);
    }
    value
}

fn patch_raw_metadata(value: &mut Value, metadata: &ObjectMeta) {
    let object = ensure_object(value);
    object.insert("name".to_owned(), json!(metadata.name));
    if let Some(namespace) = metadata.namespace.as_deref() {
        object.insert("namespace".to_owned(), json!(namespace));
    } else {
        object.remove("namespace");
    }
    object.insert("labels".to_owned(), json!(metadata.labels));
    object.insert("annotations".to_owned(), json!(metadata.annotations));
}

fn patch_raw_pod_template_metadata(value: &mut Value, metadata: &PodTemplateMetadata) {
    let object = ensure_object(value);
    object.insert("labels".to_owned(), json!(metadata.labels));
    object.insert("annotations".to_owned(), json!(metadata.annotations));
}

fn ensure_object(value: &mut Value) -> &mut Map<String, Value> {
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }
    value.as_object_mut().expect("value was set to an object")
}

fn pod_template_to_value(template: &PodTemplateSpec) -> Value {
    json!({
        "metadata": pod_template_metadata_to_value(&template.metadata),
        "spec": {
            "containers": template.spec.containers.iter().map(container_to_value).collect::<Vec<_>>(),
            "volumes": template.spec.volumes.iter().map(pod_volume_to_value).collect::<Vec<_>>(),
            // A workload talks to the control plane over gRPC with the sidecar's
            // own credential, so it needs no ServiceAccount token and no syscalls
            // beyond the container runtime's default set.
            "automountServiceAccountToken": false,
            "securityContext": { "seccompProfile": { "type": "RuntimeDefault" } },
        },
    })
}

fn pod_template_metadata_to_value(metadata: &PodTemplateMetadata) -> Value {
    json!({
        "labels": metadata.labels,
        "annotations": metadata.annotations,
    })
}

fn container_to_value(container: &Container) -> Value {
    let mut value = json!({
        "name": container.name,
        "image": container.image,
        "ports": container.ports.iter().map(container_port_to_value).collect::<Vec<_>>(),
        "env": container.env.iter().map(env_var_to_value).collect::<Vec<_>>(),
        "volumeMounts": container.volume_mounts.iter().map(volume_mount_to_value).collect::<Vec<_>>(),
        // A container starts with the privileges its image needs and gains none
        // after that, so a setuid binary inside it stays at the same level.
        "securityContext": { "allowPrivilegeEscalation": false },
    });
    if let Some(probe) = &container.readiness_probe {
        value["readinessProbe"] = json!({
            "httpGet": {"path": probe.path, "port": probe.port},
            "periodSeconds": probe.period_seconds,
            "timeoutSeconds": probe.timeout_seconds,
            "failureThreshold": probe.failure_threshold,
        });
    }
    value
}

fn container_port_to_value(port: &ContainerPort) -> Value {
    let mut value = Map::new();
    insert_optional_string(&mut value, "name", port.name.as_deref());
    value.insert("containerPort".to_owned(), json!(port.container_port));
    Value::Object(value)
}

fn env_var_to_value(env: &EnvVar) -> Value {
    let mut value = Map::new();
    value.insert("name".to_owned(), json!(env.name));
    if let Some(source) = &env.value_from {
        value.insert(
            "valueFrom".to_owned(),
            match source {
                super::EnvVarSource::SecretKeyRef(secret) => json!({
                    "secretKeyRef": { "name": secret.name, "key": secret.key },
                }),
                super::EnvVarSource::FieldRef { field_path } => json!({
                    "fieldRef": { "apiVersion": "v1", "fieldPath": field_path },
                }),
            },
        );
    } else {
        value.insert("value".to_owned(), json!(env.value));
    }
    Value::Object(value)
}

fn volume_mount_to_value(mount: &VolumeMount) -> Value {
    json!({
        "name": mount.name,
        "mountPath": mount.mount_path,
    })
}

fn pod_volume_to_value(volume: &PodVolume) -> Value {
    json!({
        "name": volume.name,
        "persistentVolumeClaim": {
            "claimName": volume.persistent_volume_claim.claim_name,
        },
    })
}

fn service_port_to_value(port: &ServicePort) -> Value {
    let mut value = Map::new();
    insert_optional_string(&mut value, "name", port.name.as_deref());
    value.insert("port".to_owned(), json!(port.port));
    value.insert("targetPort".to_owned(), json!(port.target_port));
    Value::Object(value)
}

fn csi_source_to_value(source: &CsiPersistentVolumeSource) -> Value {
    let mut value = Map::new();
    value.insert("driver".to_owned(), json!(source.driver));
    value.insert("volumeHandle".to_owned(), json!(source.volume_handle));
    insert_optional_string(&mut value, "fsType", source.fs_type.as_deref());
    value.insert("readOnly".to_owned(), json!(source.read_only));
    value.insert(
        "volumeAttributes".to_owned(),
        json!(source.volume_attributes),
    );
    insert_optional_secret_ref(
        &mut value,
        "controllerPublishSecretRef",
        source.controller_publish_secret_ref.as_ref(),
    );
    insert_optional_secret_ref(
        &mut value,
        "nodeStageSecretRef",
        source.node_stage_secret_ref.as_ref(),
    );
    insert_optional_secret_ref(
        &mut value,
        "nodePublishSecretRef",
        source.node_publish_secret_ref.as_ref(),
    );
    insert_optional_secret_ref(
        &mut value,
        "controllerExpandSecretRef",
        source.controller_expand_secret_ref.as_ref(),
    );
    insert_optional_secret_ref(
        &mut value,
        "nodeExpandSecretRef",
        source.node_expand_secret_ref.as_ref(),
    );
    Value::Object(value)
}

fn insert_optional_secret_ref(
    object: &mut Map<String, Value>,
    key: &str,
    value: Option<&CsiSecretReference>,
) {
    if let Some(value) = value {
        object.insert(
            key.to_owned(),
            json!({
                "name": value.name,
                "namespace": value.namespace,
            }),
        );
    }
}

fn host_path_source_to_value(source: &HostPathPersistentVolumeSource) -> Value {
    let mut value = Map::new();
    value.insert("path".to_owned(), json!(source.path));
    insert_optional_string(&mut value, "type", source.type_.as_deref());
    Value::Object(value)
}

fn label_selector_to_value(match_labels: &std::collections::BTreeMap<String, String>) -> Value {
    json!({
        "matchLabels": match_labels,
    })
}

fn access_modes_to_value(access_modes: &[PersistentVolumeAccessMode]) -> Value {
    json!(access_modes
        .iter()
        .map(|mode| match mode {
            PersistentVolumeAccessMode::ReadWriteOnce => "ReadWriteOnce",
            PersistentVolumeAccessMode::ReadOnlyMany => "ReadOnlyMany",
            PersistentVolumeAccessMode::ReadWriteMany => "ReadWriteMany",
        })
        .collect::<Vec<_>>())
}

fn reclaim_policy_to_str(policy: PersistentVolumeReclaimPolicy) -> &'static str {
    match policy {
        PersistentVolumeReclaimPolicy::Retain => "Retain",
        PersistentVolumeReclaimPolicy::Delete => "Delete",
    }
}

fn insert_optional_string(map: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        map.insert(key.to_owned(), json!(value));
    }
}
