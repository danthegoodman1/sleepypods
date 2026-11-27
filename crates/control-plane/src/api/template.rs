use tonic::Status;

use crate::{api::pb, manifest as domain_manifest};

pub(super) fn manifest_template_from_proto(
    template: pb::ManifestTemplate,
) -> Result<domain_manifest::ManifestTemplate, Status> {
    Ok(domain_manifest::ManifestTemplate {
        workload: required_template_field(template.workload, "template.workload")
            .and_then(workload_template_from_proto)?,
        sidecar: required_template_field(template.sidecar, "template.sidecar")
            .and_then(sidecar_template_from_proto)?,
        service: template
            .service
            .map(service_template_from_proto)
            .transpose()?,
        volumes: template
            .volumes
            .into_iter()
            .map(volume_template_from_proto)
            .collect::<Result<_, _>>()?,
        raw_objects: template
            .raw_objects
            .into_iter()
            .map(raw_manifest_template_from_proto)
            .collect::<Result<_, _>>()?,
    })
}

fn workload_template_from_proto(
    template: pb::WorkloadTemplate,
) -> Result<domain_manifest::WorkloadTemplate, Status> {
    Ok(domain_manifest::WorkloadTemplate {
        kind: workload_kind_from_proto(template.kind)?,
        name: required_template_field(template.name, "template.workload.name")
            .and_then(template_text_from_proto)?,
        replicas: template.replicas,
        app_container: required_template_field(
            template.app_container,
            "template.workload.app_container",
        )
        .and_then(container_template_from_proto)?,
        termination_grace_period_seconds: template.termination_grace_period_seconds,
    })
}

fn container_template_from_proto(
    template: pb::ContainerTemplate,
) -> Result<domain_manifest::ContainerTemplate, Status> {
    Ok(domain_manifest::ContainerTemplate {
        name: required_non_empty_string(template.name, "template.container.name")?,
        image: required_template_field(template.image, "template.container.image")
            .and_then(template_text_from_proto)?,
        ports: template
            .ports
            .into_iter()
            .map(container_port_template_from_proto)
            .collect::<Result<_, _>>()?,
        env: template
            .env
            .into_iter()
            .map(env_var_template_from_proto)
            .collect::<Result<_, _>>()?,
    })
}

fn container_port_template_from_proto(
    template: pb::ContainerPortTemplate,
) -> Result<domain_manifest::ContainerPortTemplate, Status> {
    Ok(domain_manifest::ContainerPortTemplate {
        name: template.name,
        container_port: u16_from_proto(
            "template.container.ports.container_port",
            template.container_port,
        )?,
    })
}

fn env_var_template_from_proto(
    template: pb::EnvVarTemplate,
) -> Result<domain_manifest::EnvVarTemplate, Status> {
    Ok(domain_manifest::EnvVarTemplate {
        name: required_non_empty_string(template.name, "template.container.env.name")?,
        value: required_template_field(template.value, "template.container.env.value")
            .and_then(template_text_from_proto)?,
    })
}

fn sidecar_template_from_proto(
    template: pb::SidecarTemplate,
) -> Result<domain_manifest::SidecarTemplate, Status> {
    Ok(domain_manifest::SidecarTemplate {
        name: required_non_empty_string(template.name, "template.sidecar.name")?,
        image: required_template_field(template.image, "template.sidecar.image")
            .and_then(template_text_from_proto)?,
        listen_port: u16_from_proto("template.sidecar.listen_port", template.listen_port)?,
        mode: template.mode,
        app_protocol: match pb::AppProtocol::try_from(template.app_protocol)
            .map_err(|_| Status::invalid_argument("template.sidecar.app_protocol is invalid"))?
        {
            pb::AppProtocol::Http1 => domain_manifest::AppProtocol::Http1,
            pb::AppProtocol::H2c => domain_manifest::AppProtocol::H2c,
        },
    })
}

fn service_template_from_proto(
    template: pb::ServiceTemplate,
) -> Result<domain_manifest::ServiceTemplate, Status> {
    Ok(domain_manifest::ServiceTemplate {
        name: required_template_field(template.name, "template.service.name")
            .and_then(template_text_from_proto)?,
        ports: template
            .ports
            .into_iter()
            .map(service_port_template_from_proto)
            .collect::<Result<_, _>>()?,
    })
}

fn service_port_template_from_proto(
    template: pb::ServicePortTemplate,
) -> Result<domain_manifest::ServicePortTemplate, Status> {
    Ok(domain_manifest::ServicePortTemplate {
        name: template.name,
        port: u16_from_proto("template.service.ports.port", template.port)?,
        target_port: u16_from_proto("template.service.ports.target_port", template.target_port)?,
    })
}

fn volume_template_from_proto(
    template: pb::VolumeTemplate,
) -> Result<domain_manifest::VolumeTemplate, Status> {
    Ok(domain_manifest::VolumeTemplate {
        name: required_non_empty_string(template.name, "template.volumes.name")?,
        mount_path: required_template_field(template.mount_path, "template.volumes.mount_path")
            .and_then(template_text_from_proto)?,
        pv_name: required_template_field(template.pv_name, "template.volumes.pv_name")
            .and_then(template_text_from_proto)?,
        pvc_name: required_template_field(template.pvc_name, "template.volumes.pvc_name")
            .and_then(template_text_from_proto)?,
        access_modes: template
            .access_modes
            .into_iter()
            .map(access_mode_from_proto)
            .collect::<Result<_, _>>()?,
        capacity: required_template_field(template.capacity, "template.volumes.capacity")
            .and_then(template_text_from_proto)?,
        reclaim_policy: reclaim_policy_from_proto(template.reclaim_policy)?,
        storage_class_name: template
            .storage_class_name
            .map(template_text_from_proto)
            .transpose()?,
        source: required_template_field(template.source, "template.volumes.source")
            .and_then(volume_source_template_from_proto)?,
    })
}

fn volume_source_template_from_proto(
    template: pb::PersistentVolumeSourceTemplate,
) -> Result<domain_manifest::PersistentVolumeSourceTemplate, Status> {
    match template
        .kind
        .ok_or_else(|| Status::invalid_argument("template.volumes.source kind is required"))?
    {
        pb::persistent_volume_source_template::Kind::Csi(csi) => {
            Ok(domain_manifest::PersistentVolumeSourceTemplate::Csi {
                driver: required_template_field(csi.driver, "template.volumes.source.csi.driver")
                    .and_then(template_text_from_proto)?,
                volume_handle: required_template_field(
                    csi.volume_handle,
                    "template.volumes.source.csi.volume_handle",
                )
                .and_then(template_text_from_proto)?,
                fs_type: csi.fs_type.map(template_text_from_proto).transpose()?,
                read_only: csi.read_only,
                volume_attributes: csi
                    .volume_attributes
                    .into_iter()
                    .map(|(key, value)| Ok((key, template_text_from_proto(value)?)))
                    .collect::<Result<_, Status>>()?,
                controller_publish_secret_ref: csi_secret_ref_from_proto(
                    csi.controller_publish_secret_ref,
                    "template.volumes.source.csi.controller_publish_secret_ref.name",
                    "template.volumes.source.csi.controller_publish_secret_ref.namespace",
                )?
                .map(Box::new),
                node_stage_secret_ref: csi_secret_ref_from_proto(
                    csi.node_stage_secret_ref,
                    "template.volumes.source.csi.node_stage_secret_ref.name",
                    "template.volumes.source.csi.node_stage_secret_ref.namespace",
                )?
                .map(Box::new),
                node_publish_secret_ref: csi_secret_ref_from_proto(
                    csi.node_publish_secret_ref,
                    "template.volumes.source.csi.node_publish_secret_ref.name",
                    "template.volumes.source.csi.node_publish_secret_ref.namespace",
                )?
                .map(Box::new),
                controller_expand_secret_ref: csi_secret_ref_from_proto(
                    csi.controller_expand_secret_ref,
                    "template.volumes.source.csi.controller_expand_secret_ref.name",
                    "template.volumes.source.csi.controller_expand_secret_ref.namespace",
                )?
                .map(Box::new),
                node_expand_secret_ref: csi_secret_ref_from_proto(
                    csi.node_expand_secret_ref,
                    "template.volumes.source.csi.node_expand_secret_ref.name",
                    "template.volumes.source.csi.node_expand_secret_ref.namespace",
                )?
                .map(Box::new),
            })
        }
        pb::persistent_volume_source_template::Kind::HostPath(host_path) => {
            Ok(domain_manifest::PersistentVolumeSourceTemplate::HostPath {
                path: required_template_field(
                    host_path.path,
                    "template.volumes.source.host_path.path",
                )
                .and_then(template_text_from_proto)?,
                type_: host_path.r#type.map(template_text_from_proto).transpose()?,
            })
        }
    }
}

fn csi_secret_ref_from_proto(
    ref_: Option<pb::CsiSecretRefTemplate>,
    name_field: &'static str,
    namespace_field: &'static str,
) -> Result<Option<domain_manifest::CsiSecretRefTemplate>, Status> {
    let Some(ref_) = ref_ else {
        return Ok(None);
    };
    Ok(Some(domain_manifest::CsiSecretRefTemplate {
        name: required_template_field(ref_.name, name_field).and_then(template_text_from_proto)?,
        namespace: required_template_field(ref_.namespace, namespace_field)
            .and_then(template_text_from_proto)?,
    }))
}

fn raw_manifest_template_from_proto(
    template: pb::RawKubernetesManifestTemplate,
) -> Result<domain_manifest::RawKubernetesManifestTemplate, Status> {
    Ok(domain_manifest::RawKubernetesManifestTemplate {
        manifest: required_template_field(template.manifest, "template.raw_objects.manifest")
            .and_then(template_text_from_proto)?,
    })
}

pub(super) fn template_text_from_proto(
    text: pb::TemplateText,
) -> Result<domain_manifest::TemplateText, Status> {
    if text.parts.is_empty() {
        return Err(Status::invalid_argument(
            "template text parts must not be empty",
        ));
    }

    Ok(domain_manifest::TemplateText::from_parts(
        text.parts
            .into_iter()
            .map(template_text_part_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn template_text_part_from_proto(
    part: pb::TemplateTextPart,
) -> Result<domain_manifest::TemplateTextPart, Status> {
    match part
        .kind
        .ok_or_else(|| Status::invalid_argument("template text part kind is required"))?
    {
        pb::template_text_part::Kind::Literal(value) => {
            Ok(domain_manifest::TemplateTextPart::literal(value))
        }
        pb::template_text_part::Kind::InstanceValue(field) => {
            Ok(domain_manifest::TemplateTextPart::instance_value(field))
        }
    }
}

fn workload_kind_from_proto(kind: i32) -> Result<domain_manifest::WorkloadKind, Status> {
    match pb::WorkloadKind::try_from(kind)
        .map_err(|_| Status::invalid_argument("template.workload.kind is invalid"))?
    {
        pb::WorkloadKind::Deployment => Ok(domain_manifest::WorkloadKind::Deployment),
        pb::WorkloadKind::StatefulSet => Ok(domain_manifest::WorkloadKind::StatefulSet),
        pb::WorkloadKind::Unspecified => Err(Status::invalid_argument(
            "template.workload.kind is required",
        )),
    }
}

fn access_mode_from_proto(
    mode: i32,
) -> Result<domain_manifest::PersistentVolumeAccessMode, Status> {
    match pb::PersistentVolumeAccessMode::try_from(mode)
        .map_err(|_| Status::invalid_argument("template.volumes.access_modes is invalid"))?
    {
        pb::PersistentVolumeAccessMode::ReadWriteOnce => {
            Ok(domain_manifest::PersistentVolumeAccessMode::ReadWriteOnce)
        }
        pb::PersistentVolumeAccessMode::ReadOnlyMany => {
            Ok(domain_manifest::PersistentVolumeAccessMode::ReadOnlyMany)
        }
        pb::PersistentVolumeAccessMode::ReadWriteMany => {
            Ok(domain_manifest::PersistentVolumeAccessMode::ReadWriteMany)
        }
        pb::PersistentVolumeAccessMode::Unspecified => Err(Status::invalid_argument(
            "template.volumes.access_modes must not be unspecified",
        )),
    }
}

fn reclaim_policy_from_proto(
    policy: i32,
) -> Result<domain_manifest::PersistentVolumeReclaimPolicy, Status> {
    match pb::PersistentVolumeReclaimPolicy::try_from(policy)
        .map_err(|_| Status::invalid_argument("template.volumes.reclaim_policy is invalid"))?
    {
        pb::PersistentVolumeReclaimPolicy::Retain => {
            Ok(domain_manifest::PersistentVolumeReclaimPolicy::Retain)
        }
        pb::PersistentVolumeReclaimPolicy::Delete => {
            Ok(domain_manifest::PersistentVolumeReclaimPolicy::Delete)
        }
        pb::PersistentVolumeReclaimPolicy::Unspecified => Err(Status::invalid_argument(
            "template.volumes.reclaim_policy is required",
        )),
    }
}

fn required_template_field<T>(value: Option<T>, field: &'static str) -> Result<T, Status> {
    value.ok_or_else(|| Status::invalid_argument(format!("{field} is required")))
}

fn required_non_empty_string(value: String, field: &'static str) -> Result<String, Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{field} must not be empty"
        )));
    }

    Ok(value)
}

fn u16_from_proto(field: &'static str, value: u32) -> Result<u16, Status> {
    u16::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("{field} must fit in uint16")))
}

pub(super) fn manifest_template_to_proto(
    template: domain_manifest::ManifestTemplate,
) -> pb::ManifestTemplate {
    pb::ManifestTemplate {
        workload: Some(workload_template_to_proto(template.workload)),
        sidecar: Some(sidecar_template_to_proto(template.sidecar)),
        service: template.service.map(service_template_to_proto),
        volumes: template
            .volumes
            .into_iter()
            .map(volume_template_to_proto)
            .collect(),
        raw_objects: template
            .raw_objects
            .into_iter()
            .map(raw_manifest_template_to_proto)
            .collect(),
    }
}

fn workload_template_to_proto(template: domain_manifest::WorkloadTemplate) -> pb::WorkloadTemplate {
    pb::WorkloadTemplate {
        kind: workload_kind_to_proto(template.kind) as i32,
        name: Some(template_text_to_proto(template.name)),
        replicas: template.replicas,
        app_container: Some(container_template_to_proto(template.app_container)),
        termination_grace_period_seconds: template.termination_grace_period_seconds,
    }
}

fn container_template_to_proto(
    template: domain_manifest::ContainerTemplate,
) -> pb::ContainerTemplate {
    pb::ContainerTemplate {
        name: template.name,
        image: Some(template_text_to_proto(template.image)),
        ports: template
            .ports
            .into_iter()
            .map(container_port_template_to_proto)
            .collect(),
        env: template
            .env
            .into_iter()
            .map(env_var_template_to_proto)
            .collect(),
    }
}

fn container_port_template_to_proto(
    template: domain_manifest::ContainerPortTemplate,
) -> pb::ContainerPortTemplate {
    pb::ContainerPortTemplate {
        name: template.name,
        container_port: u32::from(template.container_port),
    }
}

fn env_var_template_to_proto(template: domain_manifest::EnvVarTemplate) -> pb::EnvVarTemplate {
    pb::EnvVarTemplate {
        name: template.name,
        value: Some(template_text_to_proto(template.value)),
    }
}

fn sidecar_template_to_proto(template: domain_manifest::SidecarTemplate) -> pb::SidecarTemplate {
    pb::SidecarTemplate {
        name: template.name,
        image: Some(template_text_to_proto(template.image)),
        listen_port: u32::from(template.listen_port),
        mode: template.mode,
        app_protocol: match template.app_protocol {
            domain_manifest::AppProtocol::Http1 => pb::AppProtocol::Http1,
            domain_manifest::AppProtocol::H2c => pb::AppProtocol::H2c,
        } as i32,
    }
}

fn service_template_to_proto(template: domain_manifest::ServiceTemplate) -> pb::ServiceTemplate {
    pb::ServiceTemplate {
        name: Some(template_text_to_proto(template.name)),
        ports: template
            .ports
            .into_iter()
            .map(service_port_template_to_proto)
            .collect(),
    }
}

fn service_port_template_to_proto(
    template: domain_manifest::ServicePortTemplate,
) -> pb::ServicePortTemplate {
    pb::ServicePortTemplate {
        name: template.name,
        port: u32::from(template.port),
        target_port: u32::from(template.target_port),
    }
}

fn volume_template_to_proto(template: domain_manifest::VolumeTemplate) -> pb::VolumeTemplate {
    pb::VolumeTemplate {
        name: template.name,
        mount_path: Some(template_text_to_proto(template.mount_path)),
        pv_name: Some(template_text_to_proto(template.pv_name)),
        pvc_name: Some(template_text_to_proto(template.pvc_name)),
        access_modes: template
            .access_modes
            .into_iter()
            .map(|mode| access_mode_to_proto(mode) as i32)
            .collect(),
        capacity: Some(template_text_to_proto(template.capacity)),
        reclaim_policy: reclaim_policy_to_proto(template.reclaim_policy) as i32,
        storage_class_name: template.storage_class_name.map(template_text_to_proto),
        source: Some(volume_source_template_to_proto(template.source)),
    }
}

fn volume_source_template_to_proto(
    template: domain_manifest::PersistentVolumeSourceTemplate,
) -> pb::PersistentVolumeSourceTemplate {
    let kind = match template {
        domain_manifest::PersistentVolumeSourceTemplate::Csi {
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
        } => pb::persistent_volume_source_template::Kind::Csi(pb::CsiVolumeSourceTemplate {
            driver: Some(template_text_to_proto(driver)),
            volume_handle: Some(template_text_to_proto(volume_handle)),
            fs_type: fs_type.map(template_text_to_proto),
            read_only,
            volume_attributes: volume_attributes
                .into_iter()
                .map(|(key, value)| (key, template_text_to_proto(value)))
                .collect(),
            controller_publish_secret_ref: controller_publish_secret_ref
                .map(|secret| csi_secret_ref_to_proto(*secret)),
            node_stage_secret_ref: node_stage_secret_ref
                .map(|secret| csi_secret_ref_to_proto(*secret)),
            node_publish_secret_ref: node_publish_secret_ref
                .map(|secret| csi_secret_ref_to_proto(*secret)),
            controller_expand_secret_ref: controller_expand_secret_ref
                .map(|secret| csi_secret_ref_to_proto(*secret)),
            node_expand_secret_ref: node_expand_secret_ref
                .map(|secret| csi_secret_ref_to_proto(*secret)),
        }),
        domain_manifest::PersistentVolumeSourceTemplate::HostPath { path, type_ } => {
            pb::persistent_volume_source_template::Kind::HostPath(
                pb::HostPathVolumeSourceTemplate {
                    path: Some(template_text_to_proto(path)),
                    r#type: type_.map(template_text_to_proto),
                },
            )
        }
    };

    pb::PersistentVolumeSourceTemplate { kind: Some(kind) }
}

fn csi_secret_ref_to_proto(
    ref_: domain_manifest::CsiSecretRefTemplate,
) -> pb::CsiSecretRefTemplate {
    pb::CsiSecretRefTemplate {
        name: Some(template_text_to_proto(ref_.name)),
        namespace: Some(template_text_to_proto(ref_.namespace)),
    }
}

fn raw_manifest_template_to_proto(
    template: domain_manifest::RawKubernetesManifestTemplate,
) -> pb::RawKubernetesManifestTemplate {
    pb::RawKubernetesManifestTemplate {
        manifest: Some(template_text_to_proto(template.manifest)),
    }
}

pub(super) fn template_text_to_proto(text: domain_manifest::TemplateText) -> pb::TemplateText {
    pb::TemplateText {
        parts: text
            .parts()
            .iter()
            .map(template_text_part_to_proto)
            .collect(),
    }
}

fn template_text_part_to_proto(part: &domain_manifest::TemplateTextPart) -> pb::TemplateTextPart {
    let kind = match part {
        domain_manifest::TemplateTextPart::Literal(value) => {
            pb::template_text_part::Kind::Literal(value.clone())
        }
        domain_manifest::TemplateTextPart::InstanceValue(field) => {
            pb::template_text_part::Kind::InstanceValue(field.clone())
        }
    };

    pb::TemplateTextPart { kind: Some(kind) }
}

fn workload_kind_to_proto(kind: domain_manifest::WorkloadKind) -> pb::WorkloadKind {
    match kind {
        domain_manifest::WorkloadKind::Deployment => pb::WorkloadKind::Deployment,
        domain_manifest::WorkloadKind::StatefulSet => pb::WorkloadKind::StatefulSet,
    }
}

fn access_mode_to_proto(
    mode: domain_manifest::PersistentVolumeAccessMode,
) -> pb::PersistentVolumeAccessMode {
    match mode {
        domain_manifest::PersistentVolumeAccessMode::ReadWriteOnce => {
            pb::PersistentVolumeAccessMode::ReadWriteOnce
        }
        domain_manifest::PersistentVolumeAccessMode::ReadOnlyMany => {
            pb::PersistentVolumeAccessMode::ReadOnlyMany
        }
        domain_manifest::PersistentVolumeAccessMode::ReadWriteMany => {
            pb::PersistentVolumeAccessMode::ReadWriteMany
        }
    }
}

fn reclaim_policy_to_proto(
    policy: domain_manifest::PersistentVolumeReclaimPolicy,
) -> pb::PersistentVolumeReclaimPolicy {
    match policy {
        domain_manifest::PersistentVolumeReclaimPolicy::Retain => {
            pb::PersistentVolumeReclaimPolicy::Retain
        }
        domain_manifest::PersistentVolumeReclaimPolicy::Delete => {
            pb::PersistentVolumeReclaimPolicy::Delete
        }
    }
}
