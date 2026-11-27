use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy, StatefulSet, StatefulSetSpec},
        core::v1::{
            CSIPersistentVolumeSource, Container, ContainerPort, EnvVar, EnvVarSource,
            HTTPGetAction, HostPathVolumeSource, ObjectFieldSelector, ObjectReference,
            PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimSpec,
            PersistentVolumeClaimVolumeSource, PersistentVolumeSpec, PodSecurityContext, PodSpec,
            PodTemplateSpec, Probe, SeccompProfile, Secret, SecretKeySelector, SecretReference,
            SecurityContext, Service, ServicePort, ServiceSpec, Volume, VolumeMount,
            VolumeResourceRequirements,
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{LabelSelector, ObjectMeta},
        util::intstr::IntOrString,
    },
};
use serde_json::{Map, Value};

use crate::{
    instance::InstanceRecord,
    kubernetes_name::{
        instance_suffix, is_dns_label, render_instance_scoped_name, STATEFUL_SET_NAME_MAX_LEN,
    },
};

use super::{
    bind::PlaceholderDocument, AppProtocol, ApplyOrder, ContainerTemplate, CsiSecretRefTemplate,
    KubernetesObject, ManifestRenderError, ManifestTemplate, PersistentVolumeReclaimPolicy,
    PersistentVolumeSourceTemplate, RawKubernetesManifestTemplate, RenderManifestRequest,
    RenderedManifest, RenderedManifestObject, ServiceTemplate, SidecarTemplate, TemplateText,
    VolumeTemplate, WorkloadKind, INSTANCE_SUFFIX_VALUE, LABEL_INSTANCE_GENERATION,
    LABEL_INSTANCE_ID, LABEL_WORKLOAD_CLASS_ID, LABEL_WORKLOAD_CLASS_VERSION, LABEL_WORKLOAD_NAME,
};

const CONTROL_PLANE_SERVICE_NAME: &str = "sleepypods-control-plane";
const CONTROL_PLANE_GRPC_PORT: u16 = 50051;
const ENV_LISTEN_PORT: &str = "SLEEPYPODS_LISTEN_PORT";
const ENV_SIDECAR_LISTEN_ADDR: &str = "SLEEPYPODS_SIDECAR_LISTEN_ADDR";
const ENV_SIDECAR_READINESS_LISTEN_ADDR: &str = "SLEEPYPODS_SIDECAR_READINESS_LISTEN_ADDR";
const ENV_APP_PORT: &str = "SLEEPYPODS_APP_PORT";
const ENV_INSTANCE_ID: &str = "SLEEPYPODS_INSTANCE_ID";
const ENV_INSTANCE_GENERATION: &str = "SLEEPYPODS_INSTANCE_GENERATION";
const ENV_CONTROL_PLANE_ENDPOINT: &str = "SLEEPYPODS_CONTROL_PLANE_ENDPOINT";
const ENV_IDLE_TIMEOUT_MS: &str = "SLEEPYPODS_IDLE_TIMEOUT_MS";
const ENV_IDLE_RETRY_BACKOFF_MS: &str = "SLEEPYPODS_IDLE_RETRY_BACKOFF_MS";
const ENV_DRAIN_GRACE_TIMEOUT_MS: &str = "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS";
const ENV_SIDECAR_MODE: &str = "SLEEPYPODS_SIDECAR_MODE";
const ENV_APP_PROTOCOL: &str = "SLEEPYPODS_APP_PROTOCOL";
const ENV_CONTROL_PLANE_SIDECAR_TOKEN: &str = "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN";
const SIDECAR_TOKEN_SECRET_KEY: &str = "token";
const ANNOTATION_BACKEND_SCHEME: &str = "sleepypods.io/backend-scheme";

pub fn render_manifests(
    request: RenderManifestRequest<'_>,
) -> Result<RenderedManifest, ManifestRenderError> {
    render_manifests_with_options(request, super::RenderManifestOptions::default())
}

pub(crate) fn render_manifests_with_options(
    request: RenderManifestRequest<'_>,
    options: super::RenderManifestOptions<'_>,
) -> Result<RenderedManifest, ManifestRenderError> {
    request.template.workload.validate_replicas()?;
    request.template.validate_storage_retention()?;
    validate_namespace(request.namespace)?;
    let termination_grace_period_seconds = request
        .template
        .workload
        .termination_grace_period_seconds()?;

    let suffix = instance_suffix(&request.instance.id);
    let mut instance = request.instance.clone();
    instance
        .values
        .insert(INSTANCE_SUFFIX_VALUE.to_owned(), suffix.clone());
    let instance = &instance;

    let workload_name =
        render_object_name("workload.name", &request.template.workload.name, instance)?;
    validate_kind_name(
        "workload.name",
        match request.template.workload.kind {
            WorkloadKind::Deployment => "Deployment",
            WorkloadKind::StatefulSet => "StatefulSet",
        },
        &workload_name,
    )?;
    let service_name = request
        .template
        .service
        .as_ref()
        .map(|service| {
            let name = render_object_name("service.name", &service.name, instance)?;
            validate_kind_name("service.name", "Service", &name)?;
            Ok(name)
        })
        .transpose()?;
    let selector_labels = selector_labels(instance, &workload_name)?;
    let metadata_labels = metadata_labels(instance, &workload_name)?;
    let sidecar = render_sidecar_config(request.template, instance)?;

    let rendered_volumes = request
        .template
        .volumes
        .iter()
        .map(|volume| render_volume(volume, instance, request.namespace))
        .collect::<Result<Vec<_>, _>>()?;

    let namespace = Some(request.namespace);
    let mut objects = Vec::new();
    for rendered in &rendered_volumes {
        objects.push(RenderedManifestObject {
            apply_order: ApplyOrder::PersistentVolume,
            object: PersistentVolume {
                metadata: object_meta(&rendered.pv_name, None, &metadata_labels, BTreeMap::new()),
                spec: Some(PersistentVolumeSpec {
                    capacity: Some(storage(&rendered.capacity)),
                    access_modes: Some(rendered.access_modes.clone()),
                    persistent_volume_reclaim_policy: Some(
                        rendered.reclaim_policy.as_str().to_owned(),
                    ),
                    storage_class_name: rendered.storage_class_name.clone(),
                    claim_ref: Some(ObjectReference {
                        namespace: Some(request.namespace.to_owned()),
                        name: Some(rendered.pvc_name.clone()),
                        ..ObjectReference::default()
                    }),
                    ..rendered.source.clone()
                }),
                status: None,
            }
            .into(),
        });
    }
    for rendered in &rendered_volumes {
        objects.push(RenderedManifestObject {
            apply_order: ApplyOrder::PersistentVolumeClaim,
            object: PersistentVolumeClaim {
                metadata: object_meta(
                    &rendered.pvc_name,
                    namespace,
                    &metadata_labels,
                    BTreeMap::new(),
                ),
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(rendered.access_modes.clone()),
                    resources: Some(VolumeResourceRequirements {
                        requests: Some(storage(&rendered.capacity)),
                        limits: None,
                    }),
                    storage_class_name: rendered.storage_class_name.clone(),
                    volume_name: Some(rendered.pv_name.clone()),
                    ..PersistentVolumeClaimSpec::default()
                }),
                status: None,
            }
            .into(),
        });
    }

    if let Some(service) = &request.template.service {
        let mut service_annotations = BTreeMap::new();
        if sidecar.mode.as_deref() == Some("tcp") {
            service_annotations.insert(ANNOTATION_BACKEND_SCHEME.to_owned(), "tcp".to_owned());
        }

        objects.push(RenderedManifestObject {
            apply_order: ApplyOrder::Service,
            object: Service {
                metadata: object_meta(
                    service_name.as_deref().expect("service name was rendered"),
                    namespace,
                    &metadata_labels,
                    service_annotations,
                ),
                spec: Some(ServiceSpec {
                    selector: Some(selector_labels.clone()),
                    ports: Some(render_service_ports(service, sidecar.listen_port)?),
                    ..ServiceSpec::default()
                }),
                status: None,
            }
            .into(),
        });
    }

    let sidecar_token_secret_name = if let Some(token) = options.sidecar_control_plane_token {
        let name = render_instance_scoped_name("sleepypods-sidecar-token", &instance.id);
        objects.push(RenderedManifestObject {
            apply_order: ApplyOrder::Secret,
            object: Secret {
                metadata: object_meta(&name, namespace, &metadata_labels, BTreeMap::new()),
                type_: Some("Opaque".to_owned()),
                string_data: Some(BTreeMap::from([(
                    SIDECAR_TOKEN_SECRET_KEY.to_owned(),
                    token.as_secret_str().to_owned(),
                )])),
                ..Secret::default()
            }
            .into(),
        });
        Some(name)
    } else {
        None
    };

    let pod_template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(metadata_labels.clone()),
            annotations: Some(BTreeMap::new()),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            containers: vec![
                render_app_container(
                    &request.template.workload.app_container,
                    instance,
                    &rendered_volumes,
                )?,
                render_sidecar_container(
                    &request.template.sidecar,
                    instance,
                    &sidecar,
                    request.namespace,
                    request.sleep_policy,
                    sidecar_token_secret_name.as_deref(),
                    options,
                )?,
            ],
            volumes: Some(
                rendered_volumes
                    .iter()
                    .map(|volume| Volume {
                        name: volume.volume_name.clone(),
                        persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                            claim_name: volume.pvc_name.clone(),
                            read_only: None,
                        }),
                        ..Volume::default()
                    })
                    .collect(),
            ),
            // A workload talks to the control plane over gRPC with the sidecar's
            // own credential, so it needs no ServiceAccount token and no syscalls
            // beyond the container runtime's default set.
            automount_service_account_token: Some(false),
            security_context: Some(PodSecurityContext {
                seccomp_profile: Some(SeccompProfile {
                    type_: "RuntimeDefault".to_owned(),
                    localhost_profile: None,
                }),
                ..PodSecurityContext::default()
            }),
            termination_grace_period_seconds: Some(i64::from(termination_grace_period_seconds)),
            ..PodSpec::default()
        }),
    };

    let workload_metadata =
        object_meta(&workload_name, namespace, &metadata_labels, BTreeMap::new());
    let selector = LabelSelector {
        match_labels: Some(selector_labels),
        match_expressions: None,
    };
    // validate_replicas admits exactly one replica.
    let replicas = Some(1);
    let workload: KubernetesObject = match request.template.workload.kind {
        WorkloadKind::Deployment => Deployment {
            metadata: workload_metadata,
            spec: Some(DeploymentSpec {
                replicas,
                strategy: Some(DeploymentStrategy {
                    type_: Some("Recreate".to_owned()),
                    rolling_update: None,
                }),
                selector,
                template: pod_template,
                ..DeploymentSpec::default()
            }),
            status: None,
        }
        .into(),
        WorkloadKind::StatefulSet => {
            let service_name = service_name.ok_or_else(|| ManifestRenderError::InvalidField {
                field: "stateful_set.service_name",
                message: "StatefulSet rendering requires a service template".to_owned(),
            })?;
            StatefulSet {
                metadata: workload_metadata,
                spec: Some(StatefulSetSpec {
                    replicas,
                    service_name: Some(service_name),
                    selector,
                    template: pod_template,
                    ..StatefulSetSpec::default()
                }),
                status: None,
            }
            .into()
        }
    };
    objects.push(RenderedManifestObject {
        apply_order: ApplyOrder::Workload,
        object: workload,
    });

    let mut auxiliary_labels = metadata_labels.clone();
    auxiliary_labels.remove(LABEL_WORKLOAD_NAME);
    for raw in &request.template.raw_objects {
        objects.push(render_raw_object(
            raw,
            instance,
            &suffix,
            request.namespace,
            &auxiliary_labels,
        )?);
    }

    objects.sort_by_key(|object| object.apply_order);
    validate_unique_rendered_refs(&objects)?;
    validate_retained_static_inventory(&objects)?;

    Ok(RenderedManifest {
        instance_generation: request.instance.generation,
        objects,
    })
}

fn render_raw_object(
    template: &RawKubernetesManifestTemplate,
    instance: &InstanceRecord,
    suffix: &str,
    namespace: &str,
    metadata_labels: &BTreeMap<String, String>,
) -> Result<RenderedManifestObject, ManifestRenderError> {
    let document = PlaceholderDocument::new(&template.manifest).with_values(&instance.values)?;
    if document.text().trim().is_empty() {
        return Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest",
            message: "rendered value must not be empty".to_owned(),
        });
    }
    let mut value: Value = serde_yaml::from_str(document.text()).map_err(|error| {
        ManifestRenderError::InvalidField {
            field: "raw_objects.manifest",
            message: format!("manifest must be valid YAML or JSON: {error}"),
        }
    })?;
    // Binding here puts every instance value inside a scalar of the parsed
    // document, so the checks below and the object that reaches Kubernetes both
    // see the values the tenant actually supplied.
    document.bind(&mut value);
    reject_raw_primary_selector(&value)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| ManifestRenderError::InvalidField {
            field: "raw_objects.manifest",
            message: "manifest must render to one Kubernetes object".to_owned(),
        })?;

    let api_version = required_string_field(object, "apiVersion", "raw_objects.manifest")?;
    let kind = required_string_field(object, "kind", "raw_objects.manifest")?;
    let apply_order = raw_apply_order(&api_version, &kind)?;
    let namespaced = raw_kind_is_namespaced(&kind);

    let metadata = object
        .entry("metadata")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.metadata",
            message: "metadata must be an object".to_owned(),
        })?;
    let name = required_string_field(metadata, "name", "raw_objects.manifest.metadata.name")?;
    validate_dns_label("raw_objects.manifest.metadata.name", &name)?;
    validate_kind_name("raw_objects.manifest.metadata.name", &kind, &name)?;
    if !name
        .strip_suffix(suffix)
        .is_some_and(|base| base.ends_with('-'))
    {
        return Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.metadata.name",
            message: format!(
                "name {name:?} must end with \"-\" and the instance suffix; \
                 read it from instance value {INSTANCE_SUFFIX_VALUE:?}"
            ),
        });
    }

    let effective_namespace = raw_effective_namespace(metadata, namespace, namespaced)?;
    if namespaced {
        metadata.insert("namespace".to_owned(), Value::String(effective_namespace));
    } else {
        metadata.remove("namespace");
    }

    merge_required_string_map(
        metadata,
        "labels",
        metadata_labels,
        "raw_objects.manifest.metadata.labels",
    )?;
    require_string_map(metadata, "labels", "raw_objects.manifest.metadata.labels")?;
    require_string_map(
        metadata,
        "annotations",
        "raw_objects.manifest.metadata.annotations",
    )?;

    let object = KubernetesObject::from_kubernetes_json(value).map_err(|error| {
        ManifestRenderError::InvalidField {
            field: "raw_objects.manifest",
            message: format!("{api_version} {kind} does not match its Kubernetes schema: {error}"),
        }
    })?;
    Ok(RenderedManifestObject {
        apply_order,
        object,
    })
}

// The workload-name label marks the structured workload and its Pods, which the
// generated Service selects. A raw object neither carries it nor selects by it.
fn reject_raw_primary_selector(value: &Value) -> Result<(), ManifestRenderError> {
    let explicit_label = [
        "/metadata/labels",
        "/spec/selector",
        "/spec/selector/matchLabels",
    ]
    .iter()
    .any(|path| {
        value
            .pointer(path)
            .and_then(|labels| labels.get(LABEL_WORKLOAD_NAME))
            .is_some()
    });
    let expression = value
        .pointer("/spec/selector/matchExpressions")
        .and_then(Value::as_array)
        .is_some_and(|expressions| {
            expressions.iter().any(|expression| {
                expression.get("key").and_then(Value::as_str) == Some(LABEL_WORKLOAD_NAME)
            })
        });
    if explicit_label || expression {
        return Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest",
            message: format!(
                "{LABEL_WORKLOAD_NAME} is reserved for the primary workload and Service selector"
            ),
        });
    }
    Ok(())
}

fn required_string_field(
    object: &Map<String, Value>,
    key: &str,
    field: &'static str,
) -> Result<String, ManifestRenderError> {
    match object.get(key).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        _ => Err(ManifestRenderError::InvalidField {
            field,
            message: format!("{key} must be a non-empty string"),
        }),
    }
}

fn raw_apply_order(api_version: &str, kind: &str) -> Result<ApplyOrder, ManifestRenderError> {
    match (api_version, kind) {
        ("v1", "PersistentVolume") => Ok(ApplyOrder::PersistentVolume),
        ("v1", "PersistentVolumeClaim") => Ok(ApplyOrder::PersistentVolumeClaim),
        ("v1", "Secret") => Ok(ApplyOrder::Secret),
        ("v1", "Service") => Ok(ApplyOrder::Service),
        (_, "Deployment" | "StatefulSet") => Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.kind",
            message: format!(
                "raw object {api_version} {kind} is not allowed: a class runs one workload, \
                 declared in template.workload, which carries the sidecar, Pod hardening \
                 and automatic sleep"
            ),
        }),
        _ => Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.kind",
            message: format!("raw object {api_version} {kind} is not in the V1 allow-list"),
        }),
    }
}

fn raw_kind_is_namespaced(kind: &str) -> bool {
    !matches!(kind, "PersistentVolume")
}

fn raw_effective_namespace(
    metadata: &Map<String, Value>,
    namespace: &str,
    namespaced: bool,
) -> Result<String, ManifestRenderError> {
    if !namespaced {
        if metadata.contains_key("namespace") {
            return Err(ManifestRenderError::InvalidField {
                field: "raw_objects.manifest.metadata.namespace",
                message: "PersistentVolume must be cluster-scoped with no namespace".to_owned(),
            });
        }
        return Ok(String::new());
    }

    let Some(raw_namespace) = metadata.get("namespace") else {
        return Ok(namespace.to_owned());
    };
    let Some(effective) = raw_namespace.as_str() else {
        return Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.metadata.namespace",
            message: "namespace must be a string".to_owned(),
        });
    };
    if effective.is_empty() {
        return Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.metadata.namespace",
            message: "namespace must be a non-empty string".to_owned(),
        });
    }
    validate_dns_label("raw_objects.manifest.metadata.namespace", effective)?;
    if effective != namespace {
        return Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.metadata.namespace",
            message: format!("namespace must be omitted or match target namespace {namespace:?}"),
        });
    }
    Ok(effective.to_owned())
}

fn merge_required_string_map(
    metadata: &mut Map<String, Value>,
    key: &'static str,
    required: &BTreeMap<String, String>,
    field: &'static str,
) -> Result<(), ManifestRenderError> {
    let map = metadata
        .entry(key)
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| ManifestRenderError::InvalidField {
            field,
            message: format!("{key} must be an object"),
        })?;
    for (label, value) in required {
        match map.get(label) {
            Some(Value::String(existing)) if existing == value => {}
            Some(Value::String(existing)) => {
                return Err(ManifestRenderError::InvalidField {
                    field,
                    message: format!(
                        "{label} must be {value:?}, got conflicting value {existing:?}"
                    ),
                });
            }
            Some(_) => {
                return Err(ManifestRenderError::InvalidField {
                    field,
                    message: format!("{label} must be {value:?}, got non-string value"),
                });
            }
            None => {
                map.insert(label.clone(), Value::String(value.clone()));
            }
        }
    }
    Ok(())
}

/// Requires `metadata[key]`, when present, to map strings to strings.
fn require_string_map(
    metadata: &Map<String, Value>,
    key: &'static str,
    field: &'static str,
) -> Result<(), ManifestRenderError> {
    let Some(value) = metadata.get(key) else {
        return Ok(());
    };
    let map = value
        .as_object()
        .ok_or_else(|| ManifestRenderError::InvalidField {
            field,
            message: format!("{key} must be an object"),
        })?;
    if map.values().all(Value::is_string) {
        Ok(())
    } else {
        Err(ManifestRenderError::InvalidField {
            field,
            message: "all values must be strings".to_owned(),
        })
    }
}

fn validate_unique_rendered_refs(
    objects: &[RenderedManifestObject],
) -> Result<(), ManifestRenderError> {
    let mut refs = BTreeSet::new();
    for rendered in objects {
        let object = &rendered.object;
        let key = (
            object.api_version(),
            object.kind(),
            object.namespace(),
            object.name(),
        );
        if !refs.insert(key) {
            return Err(ManifestRenderError::InvalidField {
                field: "template",
                message: format!(
                    "duplicate rendered Kubernetes object ref {} {} {}/{}",
                    key.0, key.1, key.2, key.3
                ),
            });
        }
    }
    Ok(())
}

struct RenderedVolume {
    volume_name: String,
    mount_path: String,
    pv_name: String,
    pvc_name: String,
    access_modes: Vec<String>,
    capacity: String,
    reclaim_policy: PersistentVolumeReclaimPolicy,
    storage_class_name: Option<String>,
    /// A PersistentVolume spec that sets only the volume source.
    source: PersistentVolumeSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SidecarRenderConfig {
    listen_port: u16,
    proxy_port_name: Option<String>,
    app_port: u16,
    readiness_port: u16,
    mode: Option<String>,
    /// Set in http mode, the only mode that speaks an application protocol.
    app_protocol: Option<AppProtocol>,
}

fn render_sidecar_config(
    template: &ManifestTemplate,
    instance: &InstanceRecord,
) -> Result<SidecarRenderConfig, ManifestRenderError> {
    validate_port("sidecar.listen_port", template.sidecar.listen_port)?;

    let service = template
        .service
        .as_ref()
        .ok_or_else(|| ManifestRenderError::InvalidField {
            field: "service",
            message: "sidecar-routed workloads require a service template".to_owned(),
        })?;
    if service.ports.len() != 1 {
        return Err(ManifestRenderError::InvalidField {
            field: "service.ports",
            message: "exactly one service port is supported for sidecar-routed workloads"
                .to_owned(),
        });
    }

    let app_port = service.ports[0].target_port;
    validate_port("service.ports.target_port", app_port)?;
    if app_port == template.sidecar.listen_port {
        return Err(ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "original app target port must differ from the sidecar listen port".to_owned(),
        });
    }
    let app_ports = &template.workload.app_container.ports;
    if app_ports.iter().all(|port| port.container_port != 0)
        && app_ports.iter().all(|port| port.container_port != app_port)
    {
        return Err(ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: format!("target port {app_port} must match an app container port"),
        });
    }
    render_non_empty("sidecar.image", &template.sidecar.image, instance)?;
    validate_dns_label("sidecar.name", &template.sidecar.name)?;
    if let Some(mode) = &template.sidecar.mode {
        if !matches!(mode.as_str(), "http" | "tcp") {
            return Err(ManifestRenderError::InvalidField {
                field: "sidecar.mode",
                message: "sidecar mode must be either http or tcp".to_owned(),
            });
        }
    }
    let tcp = template.sidecar.mode.as_deref() == Some("tcp");
    if tcp && template.sidecar.app_protocol != AppProtocol::default() {
        return Err(ManifestRenderError::InvalidField {
            field: "sidecar.app_protocol",
            message: "an app protocol applies to http mode; tcp mode relays bytes".to_owned(),
        });
    }

    Ok(SidecarRenderConfig {
        listen_port: template.sidecar.listen_port,
        proxy_port_name: (!app_ports
            .iter()
            .any(|port| port.name.as_deref() == Some("sleepypods")))
        .then(|| "sleepypods".to_owned()),
        app_port,
        readiness_port: readiness_port(template, instance)?,
        mode: template.sidecar.mode.clone(),
        app_protocol: (!tcp).then_some(template.sidecar.app_protocol),
    })
}

fn readiness_port(
    template: &ManifestTemplate,
    instance: &InstanceRecord,
) -> Result<u16, ManifestRenderError> {
    let mut occupied = template
        .workload
        .app_container
        .ports
        .iter()
        .map(|port| port.container_port)
        .collect::<BTreeSet<_>>();
    occupied.insert(template.sidecar.listen_port);
    for env in &template.workload.app_container.env {
        if matches!(
            env.name.as_str(),
            "SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR"
                | "SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR"
                | "SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR"
        ) {
            let value = env.value.render(&instance.values)?;
            if value.is_empty() {
                continue;
            }
            let addr = value.parse::<std::net::SocketAddr>().map_err(|_| {
                ManifestRenderError::InvalidField {
                    field: "container.env.metrics_listen_addr",
                    message: format!("{} must be a socket address", env.name),
                }
            })?;
            occupied.insert(addr.port());
        }
    }
    (15001..=u16::MAX)
        .chain(1024..15001)
        .find(|port| !occupied.contains(port))
        .ok_or_else(|| ManifestRenderError::InvalidField {
            field: "sidecar.readiness_port",
            message: "no unused unprivileged port remains for sidecar readiness".to_owned(),
        })
}

fn render_volume(
    template: &VolumeTemplate,
    instance: &InstanceRecord,
    namespace: &str,
) -> Result<RenderedVolume, ManifestRenderError> {
    validate_dns_label("volume.name", &template.name)?;
    if template.access_modes.is_empty() {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.access_modes",
            message: "at least one access mode is required".to_owned(),
        });
    }
    if template
        .access_modes
        .iter()
        .enumerate()
        .any(|(index, mode)| template.access_modes[..index].contains(mode))
    {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.access_modes",
            message: "access modes must not contain duplicates".to_owned(),
        });
    }

    let mount_path = render_non_empty("volume.mount_path", &template.mount_path, instance)?;
    if !mount_path.starts_with('/') {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.mount_path",
            message: format!("mount path {mount_path:?} must be absolute"),
        });
    }

    let pv_name = render_object_name("volume.pv_name", &template.pv_name, instance)?;
    let pvc_name = render_object_name("volume.pvc_name", &template.pvc_name, instance)?;
    let capacity = render_non_empty("volume.capacity", &template.capacity, instance)?;
    let storage_class_name = template
        .storage_class_name
        .as_ref()
        .map(|value| render_non_empty("volume.storage_class_name", value, instance))
        .transpose()?;

    let source = match &template.source {
        PersistentVolumeSourceTemplate::Csi {
            driver,
            volume_handle,
            fs_type,
            read_only,
            volume_attributes,
            controller_publish_secret_ref,
            node_stage_secret_ref,
            node_publish_secret_ref,
            controller_expand_secret_ref,
            node_expand_secret_ref,
        } => {
            let rendered_attributes = volume_attributes
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        render_non_empty("volume.source.csi.volume_attributes", value, instance)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, ManifestRenderError>>()?;
            PersistentVolumeSpec {
                csi: Some(CSIPersistentVolumeSource {
                    driver: render_non_empty("volume.source.csi.driver", driver, instance)?,
                    volume_handle: render_non_empty(
                        "volume.source.csi.volume_handle",
                        volume_handle,
                        instance,
                    )?,
                    fs_type: fs_type
                        .as_ref()
                        .map(|value| render_non_empty("volume.source.csi.fs_type", value, instance))
                        .transpose()?,
                    read_only: Some(*read_only),
                    volume_attributes: Some(rendered_attributes),
                    controller_publish_secret_ref: render_csi_secret_ref(
                        "volume.source.csi.controller_publish_secret_ref.name",
                        "volume.source.csi.controller_publish_secret_ref.namespace",
                        controller_publish_secret_ref.as_deref(),
                        instance,
                    )?,
                    node_stage_secret_ref: render_csi_secret_ref(
                        "volume.source.csi.node_stage_secret_ref.name",
                        "volume.source.csi.node_stage_secret_ref.namespace",
                        node_stage_secret_ref.as_deref(),
                        instance,
                    )?,
                    node_publish_secret_ref: render_csi_secret_ref(
                        "volume.source.csi.node_publish_secret_ref.name",
                        "volume.source.csi.node_publish_secret_ref.namespace",
                        node_publish_secret_ref.as_deref(),
                        instance,
                    )?,
                    controller_expand_secret_ref: render_csi_secret_ref(
                        "volume.source.csi.controller_expand_secret_ref.name",
                        "volume.source.csi.controller_expand_secret_ref.namespace",
                        controller_expand_secret_ref.as_deref(),
                        instance,
                    )?,
                    node_expand_secret_ref: render_csi_secret_ref(
                        "volume.source.csi.node_expand_secret_ref.name",
                        "volume.source.csi.node_expand_secret_ref.namespace",
                        node_expand_secret_ref.as_deref(),
                        instance,
                    )?,
                }),
                ..PersistentVolumeSpec::default()
            }
        }
        PersistentVolumeSourceTemplate::HostPath { path, type_ } => {
            let rendered = render_non_empty("volume.source.host_path.path", path, instance)?;
            validate_host_path(path, &rendered)?;
            PersistentVolumeSpec {
                host_path: Some(HostPathVolumeSource {
                    path: rendered,
                    type_: type_
                        .as_ref()
                        .map(|value| {
                            render_non_empty("volume.source.host_path.type", value, instance)
                        })
                        .transpose()?,
                }),
                ..PersistentVolumeSpec::default()
            }
        }
    };

    validate_namespace(namespace)?;

    Ok(RenderedVolume {
        volume_name: template.name.clone(),
        mount_path,
        pv_name,
        pvc_name,
        access_modes: template
            .access_modes
            .iter()
            .map(|mode| mode.as_str().to_owned())
            .collect(),
        capacity,
        reclaim_policy: template.reclaim_policy,
        storage_class_name,
        source,
    })
}

fn render_csi_secret_ref(
    name_field: &'static str,
    namespace_field: &'static str,
    template: Option<&CsiSecretRefTemplate>,
    instance: &InstanceRecord,
) -> Result<Option<SecretReference>, ManifestRenderError> {
    let Some(template) = template else {
        return Ok(None);
    };
    let name = render_non_empty(name_field, &template.name, instance)?;
    let namespace = render_non_empty(namespace_field, &template.namespace, instance)?;
    validate_dns_label(name_field, &name)?;
    validate_dns_label(namespace_field, &namespace)?;
    Ok(Some(SecretReference {
        name: Some(name),
        namespace: Some(namespace),
    }))
}

fn render_app_container(
    template: &ContainerTemplate,
    instance: &InstanceRecord,
    volumes: &[RenderedVolume],
) -> Result<Container, ManifestRenderError> {
    validate_dns_label("container.name", &template.name)?;
    Ok(Container {
        ports: Some(
            template
                .ports
                .iter()
                .map(|port| {
                    validate_port("container.ports.container_port", port.container_port)?;
                    Ok(container_port(port.name.clone(), port.container_port))
                })
                .collect::<Result<Vec<_>, ManifestRenderError>>()?,
        ),
        env: Some(
            template
                .env
                .iter()
                .map(|env| Ok(env_value(&env.name, env.value.render(&instance.values)?)))
                .collect::<Result<Vec<_>, ManifestRenderError>>()?,
        ),
        volume_mounts: Some(
            volumes
                .iter()
                .map(|volume| VolumeMount {
                    name: volume.volume_name.clone(),
                    mount_path: volume.mount_path.clone(),
                    ..VolumeMount::default()
                })
                .collect(),
        ),
        ..hardened_container(
            template.name.clone(),
            render_non_empty("container.image", &template.image, instance)?,
        )
    })
}

fn render_sidecar_container(
    template: &SidecarTemplate,
    instance: &InstanceRecord,
    config: &SidecarRenderConfig,
    namespace: &str,
    sleep_policy: crate::sleep_policy::ResolvedSleepPolicy,
    sidecar_token_secret_name: Option<&str>,
    options: super::RenderManifestOptions<'_>,
) -> Result<Container, ManifestRenderError> {
    let mut env = vec![
        env_from(
            "SLEEPYPODS_POD_UID",
            EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_owned()),
                    field_path: "metadata.uid".to_owned(),
                }),
                ..EnvVarSource::default()
            },
        ),
        env_value(ENV_LISTEN_PORT, config.listen_port.to_string()),
        env_value(
            ENV_SIDECAR_LISTEN_ADDR,
            format!("0.0.0.0:{}", config.listen_port),
        ),
        env_value(
            ENV_SIDECAR_READINESS_LISTEN_ADDR,
            format!("0.0.0.0:{}", config.readiness_port),
        ),
        env_value(ENV_APP_PORT, config.app_port.to_string()),
        env_value(ENV_INSTANCE_ID, instance.id.to_string()),
        env_value(ENV_INSTANCE_GENERATION, instance.generation.to_string()),
        env_value(
            ENV_CONTROL_PLANE_ENDPOINT,
            options
                .sidecar_control_plane_endpoint
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!(
                        "http://{CONTROL_PLANE_SERVICE_NAME}.{namespace}.svc.cluster.local:{CONTROL_PLANE_GRPC_PORT}"
                    )
                }),
        ),
        env_value(ENV_IDLE_TIMEOUT_MS, sleep_policy.idle_timeout_ms.to_string()),
        env_value(
            ENV_IDLE_RETRY_BACKOFF_MS,
            sleep_policy.idle_retry_backoff_ms.to_string(),
        ),
        env_value(
            ENV_DRAIN_GRACE_TIMEOUT_MS,
            sleep_policy.drain_grace_timeout_ms.to_string(),
        ),
    ];
    if let Some(ca) = options.sidecar_control_plane_ca {
        env.push(env_value(
            sleepypods_api::transport::CONTROL_PLANE_TLS_CA_PEM_ENV,
            ca,
        ));
    }
    if let Some(mode) = &config.mode {
        env.push(env_value(ENV_SIDECAR_MODE, mode.clone()));
    }
    if let Some(app_protocol) = config.app_protocol {
        env.push(env_value(ENV_APP_PROTOCOL, app_protocol.as_str()));
    }
    if let Some(secret_name) = sidecar_token_secret_name {
        env.push(env_from(
            ENV_CONTROL_PLANE_SIDECAR_TOKEN,
            EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret_name.to_owned(),
                    key: SIDECAR_TOKEN_SECRET_KEY.to_owned(),
                    optional: None,
                }),
                ..EnvVarSource::default()
            },
        ));
    }

    Ok(Container {
        ports: Some(vec![
            container_port(config.proxy_port_name.clone(), config.listen_port),
            container_port(None, config.readiness_port),
        ]),
        env: Some(env),
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/ready".to_owned()),
                port: IntOrString::Int(i32::from(config.readiness_port)),
                ..HTTPGetAction::default()
            }),
            period_seconds: Some(1),
            timeout_seconds: Some(1),
            failure_threshold: Some(1),
            ..Probe::default()
        }),
        ..hardened_container(
            template.name.clone(),
            render_non_empty("sidecar.image", &template.image, instance)?,
        )
    })
}

/// A container with no ports, environment or mounts yet. It starts with the
/// privileges its image needs and gains none after that, so a setuid binary
/// inside it stays at the same level.
fn hardened_container(name: String, image: String) -> Container {
    Container {
        name,
        image: Some(image),
        ports: Some(Vec::new()),
        env: Some(Vec::new()),
        volume_mounts: Some(Vec::new()),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            ..SecurityContext::default()
        }),
        ..Container::default()
    }
}

fn container_port(name: Option<String>, port: u16) -> ContainerPort {
    ContainerPort {
        name,
        container_port: i32::from(port),
        ..ContainerPort::default()
    }
}

fn env_value(name: &str, value: impl Into<String>) -> EnvVar {
    EnvVar {
        name: name.to_owned(),
        value: Some(value.into()),
        value_from: None,
    }
}

fn env_from(name: &str, source: EnvVarSource) -> EnvVar {
    EnvVar {
        name: name.to_owned(),
        value: None,
        value_from: Some(source),
    }
}

fn render_service_ports(
    template: &ServiceTemplate,
    sidecar_listen_port: u16,
) -> Result<Vec<ServicePort>, ManifestRenderError> {
    template
        .ports
        .iter()
        .map(|port| {
            validate_port("service.ports.port", port.port)?;
            validate_port("service.ports.target_port", port.target_port)?;
            Ok(ServicePort {
                name: port.name.clone(),
                port: i32::from(port.port),
                target_port: Some(IntOrString::Int(i32::from(sidecar_listen_port))),
                ..ServicePort::default()
            })
        })
        .collect()
}

fn object_meta(
    name: &str,
    namespace: Option<&str>,
    labels: &BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_owned()),
        namespace: namespace.map(str::to_owned),
        labels: Some(labels.clone()),
        annotations: Some(annotations),
        ..ObjectMeta::default()
    }
}

fn storage(capacity: &str) -> BTreeMap<String, Quantity> {
    BTreeMap::from([("storage".to_owned(), Quantity(capacity.to_owned()))])
}

fn render_object_name(
    field: &'static str,
    template: &TemplateText,
    instance: &InstanceRecord,
) -> Result<String, ManifestRenderError> {
    let base = render_non_empty(field, template, instance)?;
    let value = render_instance_scoped_name(&base, &instance.id);
    validate_dns_label(field, &value)?;
    Ok(value)
}

fn render_non_empty(
    field: &'static str,
    template: &TemplateText,
    instance: &InstanceRecord,
) -> Result<String, ManifestRenderError> {
    let value = template.render(&instance.values)?;
    if value.trim().is_empty() {
        return Err(ManifestRenderError::InvalidField {
            field,
            message: "rendered value must not be empty".to_owned(),
        });
    }
    Ok(value)
}

/// A hostPath names a directory on the node, so the class author fixes its root
/// and an instance may only choose a subdirectory beneath that root.
fn validate_host_path(template: &TemplateText, rendered: &str) -> Result<(), ManifestRenderError> {
    if !rendered.starts_with('/') {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: format!("hostPath path {rendered:?} must be absolute"),
        });
    }
    let prefix = template.literal_prefix();
    let reads_values = template.referenced_values().next().is_some();
    if !prefix.starts_with('/') || (reads_values && (prefix == "/" || !prefix.ends_with('/'))) {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: format!(
                "hostPath path {rendered:?} must start with a literal absolute directory \
                 ending in \"/\", so that an instance value chooses a subdirectory of it"
            ),
        });
    }
    if rendered.split('/').any(|segment| segment == "..") {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: format!(
                "hostPath path {rendered:?} must stay inside its directory, so no segment may be \"..\""
            ),
        });
    }
    Ok(())
}

/// Applies the name rules Kubernetes adds for a kind on top of a DNS label.
fn validate_kind_name(
    field: &'static str,
    kind: &str,
    name: &str,
) -> Result<(), ManifestRenderError> {
    let message = match kind {
        "StatefulSet" if name.len() > STATEFUL_SET_NAME_MAX_LEN => format!(
            "StatefulSet name {name:?} has {} characters; Kubernetes allows \
             {STATEFUL_SET_NAME_MAX_LEN} characters so that Pod labels fit",
            name.len()
        ),
        "Service" if !name.starts_with(|first: char| first.is_ascii_lowercase()) => {
            format!("Service name {name:?} must start with a letter")
        }
        _ => return Ok(()),
    };
    Err(ManifestRenderError::InvalidField { field, message })
}

fn validate_namespace(namespace: &str) -> Result<(), ManifestRenderError> {
    validate_dns_label("namespace", namespace)
}

fn validate_port(field: &'static str, value: u16) -> Result<(), ManifestRenderError> {
    if value == 0 {
        Err(ManifestRenderError::InvalidField {
            field,
            message: "port must be between 1 and 65535".to_owned(),
        })
    } else {
        Ok(())
    }
}

fn validate_dns_label(field: &'static str, value: &str) -> Result<(), ManifestRenderError> {
    if is_dns_label(value) {
        Ok(())
    } else {
        Err(ManifestRenderError::InvalidName {
            field,
            value: value.to_owned(),
        })
    }
}

fn validate_label_value(field: &'static str, value: &str) -> Result<(), ManifestRenderError> {
    let valid = value.len() <= 63
        && (value.is_empty()
            || (value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.'
            }) && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
                && value
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)));

    if valid {
        Ok(())
    } else {
        Err(ManifestRenderError::InvalidField {
            field,
            message: format!("label value {value:?} is not Kubernetes label-value-safe"),
        })
    }
}

fn label(
    field: &'static str,
    value: impl Into<String>,
) -> Result<(String, String), ManifestRenderError> {
    let value = value.into();
    validate_label_value(field, &value)?;
    Ok((field.to_owned(), value))
}

fn selector_labels(
    instance: &InstanceRecord,
    workload_name: &str,
) -> Result<BTreeMap<String, String>, ManifestRenderError> {
    Ok(BTreeMap::from([
        label(LABEL_INSTANCE_ID, instance.id.as_str())?,
        label(LABEL_WORKLOAD_NAME, workload_name)?,
    ]))
}

fn metadata_labels(
    instance: &InstanceRecord,
    workload_name: &str,
) -> Result<BTreeMap<String, String>, ManifestRenderError> {
    Ok(BTreeMap::from([
        label(LABEL_INSTANCE_ID, instance.id.as_str())?,
        label(LABEL_INSTANCE_GENERATION, instance.generation.to_string())?,
        label(
            LABEL_WORKLOAD_CLASS_ID,
            instance.workload_class.class_id.as_str(),
        )?,
        label(
            LABEL_WORKLOAD_CLASS_VERSION,
            instance.workload_class.version.to_string(),
        )?,
        label(LABEL_WORKLOAD_NAME, workload_name)?,
    ]))
}

fn validate_retained_static_inventory(
    objects: &[RenderedManifestObject],
) -> Result<(), ManifestRenderError> {
    let values = objects
        .iter()
        .map(|object| object.object.to_kubernetes_json())
        .collect::<Vec<_>>();
    for value in &values {
        match value["kind"].as_str() {
            Some("PersistentVolume")
                if value
                    .pointer("/spec/persistentVolumeReclaimPolicy")
                    .and_then(Value::as_str)
                    != Some("Retain") =>
            {
                return Err(ManifestRenderError::InvalidField {
                    field: "volumes.reclaim_policy",
                    message: "all managed static PVs, including raw PVs, require explicit Retain"
                        .into(),
                })
            }
            Some("PersistentVolumeClaim") => {
                let volume_name = value
                    .pointer("/spec/volumeName")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty());
                if volume_name.is_none()
                    || !values.iter().any(|pv| {
                        pv["kind"] == "PersistentVolume"
                            && pv["metadata"]["name"].as_str() == volume_name
                    })
                {
                    return Err(ManifestRenderError::InvalidField { field: "volumes.static_binding", message: "PVC must explicitly bind a retained PV in the same managed inventory; dynamic/external bindings are unsupported".into() });
                }
            }
            _ => {}
        }
    }
    Ok(())
}
