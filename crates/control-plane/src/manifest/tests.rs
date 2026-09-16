use std::collections::BTreeMap;

use serde_json::json;

use super::{
    render_manifests, render_manifests_with_options, ApplyOrder, ContainerPortTemplate,
    ContainerTemplate, CsiPersistentVolumeSource, CsiSecretRefTemplate, CsiSecretReference, EnvVar,
    EnvVarTemplate, HostPathPersistentVolumeSource, KubernetesObject, ManifestRenderError,
    ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
    PersistentVolumeSource, PersistentVolumeSourceTemplate, RawKubernetesManifestTemplate,
    RawKubernetesObject, RenderManifestOptions, RenderManifestRequest, RenderedManifest,
    ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart,
    VolumeTemplate, WorkloadKind, WorkloadTemplate, LABEL_INSTANCE_GENERATION, LABEL_INSTANCE_ID,
    LABEL_WORKLOAD_CLASS_ID, LABEL_WORKLOAD_CLASS_VERSION, LABEL_WORKLOAD_NAME,
};
use crate::materializer::{rendered_object_ref, rendered_object_refs};
use crate::{
    auth::BearerToken,
    ids::{Generation, InstanceId, WorkloadClassId},
    instance::{InstanceRecord, InstanceState, InstanceValues},
    materialization::RenderedObjectRef,
    sleep_policy::ResolvedSleepPolicy,
    workload::WorkloadClassVersionRef,
};

#[test]
fn template_text_renders_literal() {
    let rendered = TemplateText::literal("api").render(&InstanceValues::new());

    assert_eq!(rendered.expect("literal renders"), "api");
}

#[test]
fn template_text_renders_instance_value() {
    let rendered = TemplateText::instance_value("tenant").render(&values([("tenant", "acme")]));

    assert_eq!(rendered.expect("value renders"), "acme");
}

#[test]
fn template_text_renders_composed_parts() {
    let rendered = TemplateText::from_parts([
        TemplateTextPart::literal("app-"),
        TemplateTextPart::instance_value("tenant"),
        TemplateTextPart::literal("-v1"),
    ])
    .render(&values([("tenant", "acme")]));

    assert_eq!(rendered.expect("composition renders"), "app-acme-v1");
}

#[test]
fn template_text_reports_missing_instance_value() {
    let error = TemplateText::instance_value("tenant")
        .render(&InstanceValues::new())
        .expect_err("missing value is rejected");

    assert_eq!(
        error,
        ManifestRenderError::MissingInstanceValue {
            field: "tenant".to_owned()
        }
    );
}

#[test]
fn manifest_templates_survive_json_round_trips() {
    for template in [
        deployment_template(),
        stateful_template(),
        archil_static_csi_template(),
        host_path_stateful_template(),
    ] {
        let encoded = serde_json::to_value(&template).expect("manifest template encodes");
        let decoded: ManifestTemplate =
            serde_json::from_value(encoded).expect("manifest template decodes");

        assert_eq!(decoded, template);
    }
}

#[test]
fn renders_deployment_and_service_without_volumes() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("deployment renders");

    assert_eq!(rendered.objects.len(), 2);
    assert_eq!(rendered.objects[0].apply_order, ApplyOrder::Service);
    assert_eq!(rendered.objects[1].apply_order, ApplyOrder::Workload);

    let service = match &rendered.objects[0].object {
        KubernetesObject::Service(service) => service,
        other => panic!("expected Service, got {}", other.kind()),
    };
    assert_eq!(service.metadata.name, "svc-acme-69856ec0");
    assert_eq!(service.spec.selector[LABEL_INSTANCE_ID], "instance-a");
    assert_eq!(service.spec.ports[0].port, 80);
    assert_eq!(service.spec.ports[0].target_port, 15000);

    let deployment = match &rendered.objects[1].object {
        KubernetesObject::Deployment(deployment) => deployment,
        other => panic!("expected Deployment, got {}", other.kind()),
    };
    assert_eq!(deployment.metadata.name, "app-acme-69856ec0");
    assert_eq!(deployment.metadata.labels[LABEL_INSTANCE_ID], "instance-a");
    assert_eq!(deployment.metadata.labels[LABEL_INSTANCE_GENERATION], "7");
    assert_eq!(deployment.metadata.labels[LABEL_WORKLOAD_CLASS_ID], "web");
    assert_eq!(
        deployment.metadata.labels[LABEL_WORKLOAD_CLASS_VERSION],
        "1"
    );
    assert_eq!(
        deployment.metadata.annotations["sleepypods.io/template-generation"],
        "3"
    );
    assert_eq!(deployment.spec.replicas, 1);
    assert_eq!(
        deployment.spec.selector.match_labels, service.spec.selector,
        "service and workload selectors should target the same pods"
    );
    assert_eq!(
        deployment.spec.template.metadata.labels[LABEL_INSTANCE_ID],
        "instance-a"
    );
    assert_eq!(
        deployment.spec.template.metadata.annotations["sleepypods.io/template-generation"],
        "3"
    );
    assert_eq!(deployment.spec.template.spec.containers.len(), 2);
    assert_eq!(
        deployment.spec.template.spec.containers[0].image,
        "example/app:1"
    );
    assert_eq!(
        deployment.spec.template.spec.containers[0].ports[0].container_port,
        8080
    );
    assert_eq!(
        deployment.spec.template.spec.containers[1].name,
        "sleepypods-sidecar"
    );
    assert_eq!(
        deployment.spec.template.spec.containers[1].image,
        "sleepypods/sidecar:test"
    );
    assert_eq!(
        deployment.spec.template.spec.containers[1].ports[0].container_port,
        15000
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_LISTEN_PORT",
        "15000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_SIDECAR_LISTEN_ADDR",
        "0.0.0.0:15000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_APP_PORT",
        "8080",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_INSTANCE_ID",
        "instance-a",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_INSTANCE_GENERATION",
        "7",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
        "http://sleepypods-control-plane.apps.svc.cluster.local:50051",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_IDLE_TIMEOUT_MS",
        "120000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_IDLE_RETRY_BACKOFF_MS",
        "5000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS",
        "30000",
    );
    assert_env_absent(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN",
    );
    assert!(deployment.spec.template.spec.volumes.is_empty());
    assert!(deployment.spec.template.spec.containers[0]
        .volume_mounts
        .is_empty());
    assert!(deployment.spec.template.spec.containers[1]
        .volume_mounts
        .is_empty());
}

#[test]
fn private_render_options_inject_sidecar_control_plane_token() {
    let token = BearerToken::new("sidecar_token", "sidecar-secret").expect("valid token");
    let rendered = render_manifests_with_options(
        RenderManifestRequest {
            template: &deployment_template(),
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
            template_generation: Some(Generation::new(3)),
        },
        RenderManifestOptions {
            sidecar_control_plane_token: Some(&token),
            sidecar_control_plane_endpoint: Some("https://cp.platform.example:50051"),
            sidecar_control_plane_ca: Some("PUBLIC-CA-ONLY"),
        },
    )
    .expect("deployment renders");

    let secret = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Secret(secret) => Some(secret),
            _ => None,
        })
        .expect("sidecar token Secret renders");
    assert_eq!(secret.metadata.name, "sleepypods-sidecar-token-69856ec0");
    assert_eq!(secret.metadata.namespace.as_deref(), Some("apps"));
    assert_eq!(secret.string_data["token"], "sidecar-secret");

    let deployment = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Deployment(deployment) => Some(deployment),
            _ => None,
        })
        .expect("Deployment renders");

    let env = &deployment.spec.template.spec.containers[1].env;
    assert_eq!(
        env.iter()
            .find(|e| e.name == "SLEEPYPODS_CONTROL_PLANE_ENDPOINT")
            .unwrap()
            .value,
        "https://cp.platform.example:50051"
    );
    assert_eq!(
        env.iter()
            .find(|e| e.name == sleepypods_api::transport::CONTROL_PLANE_TLS_CA_PEM_ENV)
            .unwrap()
            .value,
        "PUBLIC-CA-ONLY"
    );
    assert!(!env
        .iter()
        .any(|e| e.name.contains("SEALING") || e.name.contains("TLS_KEY")));
    let token_env = deployment.spec.template.spec.containers[1]
        .env
        .iter()
        .find(|env| env.name == "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN")
        .expect("sidecar token env renders");
    let source = token_env
        .value_from
        .as_ref()
        .expect("sidecar token env uses valueFrom");
    let super::EnvVarSource::SecretKeyRef(source) = source else {
        panic!("expected secret reference")
    };
    assert_eq!(source.name, "sleepypods-sidecar-token-69856ec0");
    assert_eq!(source.key, "token");
    assert_eq!(token_env.value, "");
}

#[test]
fn rendered_names_use_hash_suffix_for_short_instance_id() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("abc", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect("deployment renders");

    assert_eq!(
        object_name(&rendered.objects[0].object),
        "svc-acme-ba7816bf"
    );
    assert_eq!(
        object_name(&rendered.objects[1].object),
        "app-acme-ba7816bf"
    );
}

#[test]
fn rendered_names_hash_full_long_instance_id() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("abcdefghijk", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect("deployment renders");

    assert_eq!(
        object_name(&rendered.objects[0].object),
        "svc-acme-ca2f2069"
    );
    assert_eq!(
        object_name(&rendered.objects[1].object),
        "app-acme-ca2f2069"
    );
}

#[test]
fn rendered_names_truncate_operator_base_before_instance_suffix() {
    let mut template = deployment_template();
    template.workload.name = TemplateText::literal("w".repeat(55));
    template.service.as_mut().expect("service").name = TemplateText::literal("s".repeat(55));

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect("deployment renders");

    let service_name = object_name(&rendered.objects[0].object);
    let workload_name = object_name(&rendered.objects[1].object);
    assert_eq!(service_name.len(), 63);
    assert_eq!(workload_name.len(), 63);
    assert!(service_name.ends_with("-69856ec0"));
    assert!(workload_name.ends_with("-69856ec0"));
    assert_eq!(&service_name[..54], "s".repeat(54));
    assert_eq!(&workload_name[..54], "w".repeat(54));
}

#[test]
fn rendered_names_do_not_add_kubernetes_kind_tokens() {
    let mut template = deployment_template();
    template.workload.name = TemplateText::literal("shared");
    template.service.as_mut().expect("service").name = TemplateText::literal("shared");

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect("deployment renders");

    assert_eq!(object_name(&rendered.objects[0].object), "shared-69856ec0");
    assert_eq!(object_name(&rendered.objects[1].object), "shared-69856ec0");
}

#[test]
fn rejects_invalid_custom_rendered_object_name() {
    let mut template = deployment_template();
    template.workload.name = TemplateText::literal("App");

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("invalid custom name is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidName {
            field: "workload.name",
            value: "App-69856ec0".to_owned(),
        }
    );
}

#[test]
fn renders_optional_sidecar_mode_env() {
    let mut template = deployment_template();
    template.sidecar.mode = Some("tcp".to_owned());

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("deployment renders");
    let service = match &rendered.objects[0].object {
        KubernetesObject::Service(service) => service,
        other => panic!("expected Service, got {}", other.kind()),
    };
    let deployment = match &rendered.objects[1].object {
        KubernetesObject::Deployment(deployment) => deployment,
        other => panic!("expected Deployment, got {}", other.kind()),
    };

    assert_eq!(
        service.metadata.annotations["sleepypods.io/backend-scheme"],
        "tcp"
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_SIDECAR_MODE",
        "tcp",
    );
}

#[test]
fn serializes_deployment_and_service_as_kubernetes_json() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("deployment renders");

    let objects = rendered.to_kubernetes_json_values();
    let service = &objects[0];
    assert_eq!(service["apiVersion"], json!("v1"));
    assert_eq!(service["kind"], json!("Service"));
    assert_eq!(service["metadata"]["name"], json!("svc-acme-69856ec0"));
    assert_eq!(service["metadata"]["namespace"], json!("apps"));
    assert_eq!(
        service["metadata"]["labels"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert_eq!(
        service["metadata"]["annotations"]["sleepypods.io/template-generation"],
        json!("3")
    );
    assert_eq!(
        service["spec"]["selector"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert_eq!(
        service["spec"]["ports"][0],
        json!({
            "name": "http",
            "port": 80,
            "targetPort": 15000,
        })
    );

    let deployment = &objects[1];
    assert_eq!(deployment["apiVersion"], json!("apps/v1"));
    assert_eq!(deployment["kind"], json!("Deployment"));
    assert_eq!(deployment["metadata"]["name"], json!("app-acme-69856ec0"));
    assert_eq!(deployment["metadata"]["namespace"], json!("apps"));
    assert_eq!(deployment["spec"]["replicas"], json!(1));
    assert_eq!(deployment["spec"]["strategy"]["type"], json!("Recreate"));
    let pod_uid = deployment["spec"]["template"]["spec"]["containers"][1]["env"]
        .as_array()
        .unwrap()
        .iter()
        .find(|env| env["name"] == "SLEEPYPODS_POD_UID")
        .unwrap();
    assert_eq!(
        pod_uid["valueFrom"]["fieldRef"]["fieldPath"],
        json!("metadata.uid")
    );
    assert!(pod_uid.get("value").is_none());
    assert_eq!(
        deployment["spec"]["selector"]["matchLabels"],
        service["spec"]["selector"]
    );
    assert_eq!(
        deployment["spec"]["template"]["metadata"]["labels"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert_eq!(
        deployment["spec"]["template"]["spec"]["containers"][0],
        json!({
            "name": "app",
            "image": "example/app:1",
            "ports": [{
                "name": "http",
                "containerPort": 8080,
            }],
            "env": [{
                "name": "TENANT",
                "value": "acme",
            }],
            "volumeMounts": [],
            "securityContext": { "allowPrivilegeEscalation": false },
        })
    );
    let pod_spec = &deployment["spec"]["template"]["spec"];
    assert_eq!(pod_spec["automountServiceAccountToken"], json!(false));
    assert_eq!(
        pod_spec["securityContext"],
        json!({ "seccompProfile": { "type": "RuntimeDefault" } })
    );
    assert_eq!(
        deployment["spec"]["template"]["spec"]["containers"][1]["ports"][0],
        json!({
            "name": "sleepypods",
            "containerPort": 15000,
        })
    );
    assert_eq!(deployment["spec"]["template"]["spec"]["volumes"], json!([]));
}

#[test]
fn renders_stateful_set_service_pv_and_pvc_with_bound_volume() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &stateful_template(),
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("stateful workload renders");

    assert_eq!(
        rendered
            .objects
            .iter()
            .map(|object| object.apply_order)
            .collect::<Vec<_>>(),
        vec![
            ApplyOrder::PersistentVolume,
            ApplyOrder::PersistentVolumeClaim,
            ApplyOrder::Service,
            ApplyOrder::Workload
        ]
    );

    let pv = match &rendered.objects[0].object {
        KubernetesObject::PersistentVolume(pv) => pv,
        other => panic!("expected PersistentVolume, got {}", other.kind()),
    };
    assert_eq!(pv.metadata.name, "pv-acme-2e1ac556");
    assert_eq!(pv.metadata.labels[LABEL_INSTANCE_ID], "postgres-a");
    assert_eq!(
        pv.spec.access_modes,
        vec![PersistentVolumeAccessMode::ReadWriteOnce]
    );
    assert_eq!(pv.spec.capacity, "10Gi");
    assert_eq!(
        pv.spec.persistent_volume_reclaim_policy,
        PersistentVolumeReclaimPolicy::Retain
    );
    assert_eq!(pv.spec.storage_class_name.as_deref(), Some("manual"));
    assert_eq!(pv.spec.claim_ref.namespace, "data");
    assert_eq!(pv.spec.claim_ref.name, "pvc-acme-2e1ac556");
    assert_eq!(
        pv.spec.source,
        PersistentVolumeSource::Csi(Box::new(CsiPersistentVolumeSource {
            driver: "csi.example.com".to_owned(),
            volume_handle: "provider-vol-123".to_owned(),
            fs_type: Some("ext4".to_owned()),
            read_only: false,
            volume_attributes: BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]),
            controller_publish_secret_ref: None,
            node_stage_secret_ref: None,
            node_publish_secret_ref: None,
            controller_expand_secret_ref: None,
            node_expand_secret_ref: None,
        }))
    );

    let pvc = match &rendered.objects[1].object {
        KubernetesObject::PersistentVolumeClaim(pvc) => pvc,
        other => panic!("expected PersistentVolumeClaim, got {}", other.kind()),
    };
    assert_eq!(pvc.metadata.name, "pvc-acme-2e1ac556");
    assert_eq!(pvc.metadata.namespace.as_deref(), Some("data"));
    assert_eq!(pvc.spec.volume_name, "pv-acme-2e1ac556");
    assert_eq!(pvc.spec.resources.requests_storage, "10Gi");

    let stateful_set = match &rendered.objects[3].object {
        KubernetesObject::StatefulSet(stateful_set) => stateful_set,
        other => panic!("expected StatefulSet, got {}", other.kind()),
    };
    assert_eq!(stateful_set.metadata.name, "db-acme-2e1ac556");
    assert_eq!(stateful_set.spec.service_name, "db-acme-2e1ac556");
    assert_eq!(stateful_set.spec.replicas, 1);
    assert_eq!(stateful_set.spec.template.spec.containers.len(), 2);
    assert_eq!(
        stateful_set.spec.template.spec.containers[0].ports[0].container_port,
        5432
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[1].name,
        "sleepypods-sidecar"
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[1].ports[0].container_port,
        15000
    );
    assert_env(
        &stateful_set.spec.template.spec.containers[1].env,
        "SLEEPYPODS_APP_PORT",
        "5432",
    );
    assert_eq!(
        stateful_set.spec.template.spec.volumes[0]
            .persistent_volume_claim
            .claim_name,
        "pvc-acme-2e1ac556"
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[0].volume_mounts[0].name,
        "data"
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[0].volume_mounts[0].mount_path,
        "/var/lib/postgresql/data"
    );
    assert!(stateful_set.spec.template.spec.containers[1]
        .volume_mounts
        .is_empty());
}

#[test]
fn renders_valid_raw_pv_pvc_service_and_stateful_set() {
    let mut template = deployment_template();
    template.raw_objects = vec![
        raw_manifest(
            r#"
apiVersion: v1
kind: PersistentVolume
metadata:
  name: raw-pv-acme
spec:
  capacity:
    storage: 1Gi
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    namespace: apps
    name: raw-pvc-acme
  hostPath:
    path: /tmp/raw-acme
"#,
        ),
        raw_manifest(
            r#"
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: raw-pvc-acme
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
  volumeName: raw-pv-acme
"#,
        ),
        RawKubernetesManifestTemplate {
            manifest: TemplateText::from_parts([
                TemplateTextPart::literal(
                    r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc-"#,
                ),
                TemplateTextPart::instance_value("tenant"),
                TemplateTextPart::literal(
                    r#"
  finalizers:
    - raw.example.com/protect
spec:
  type: ClusterIP
  selector:
    app: raw
  ports:
    - name: http
      port: 8080
      targetPort: 8080
"#,
                ),
            ]),
        },
        raw_manifest(
            r#"
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: raw-db-acme
spec:
  podManagementPolicy: Parallel
  serviceName: raw-svc-acme
  selector:
    matchLabels:
      app: raw
  template:
    metadata:
      name: raw-pod-template
      labels:
        app: raw
    spec:
      containers:
        - name: postgres
          image: postgres:17
"#,
        ),
    ];

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("raw objects render");

    assert_eq!(
        rendered
            .objects
            .iter()
            .map(|object| (object.apply_order, rendered_object_ref(&object.object)))
            .collect::<Vec<_>>(),
        vec![
            (
                ApplyOrder::PersistentVolume,
                object_ref("v1", "PersistentVolume", "", "raw-pv-acme")
            ),
            (
                ApplyOrder::PersistentVolumeClaim,
                object_ref("v1", "PersistentVolumeClaim", "apps", "raw-pvc-acme")
            ),
            (
                ApplyOrder::Service,
                object_ref("v1", "Service", "apps", "svc-acme-69856ec0")
            ),
            (
                ApplyOrder::Service,
                object_ref("v1", "Service", "apps", "raw-svc-acme")
            ),
            (
                ApplyOrder::Workload,
                object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0")
            ),
            (
                ApplyOrder::Workload,
                object_ref("apps/v1", "StatefulSet", "apps", "raw-db-acme")
            ),
        ]
    );

    let raw_service = rendered
        .objects
        .iter()
        .find(|object| object_name(&object.object) == "raw-svc-acme")
        .expect("raw service rendered")
        .to_kubernetes_json();
    let materialization = crate::materialization::MaterializationRecord {
        id: crate::ids::MaterializationId::new("instance-a:cluster:apps").unwrap(),
        instance_id: InstanceId::new("instance-a").unwrap(),
        instance_generation: Generation::new(7),
        projection_generation: Generation::new(7),
        target: crate::materialization::MaterializationTarget::new("cluster", "apps").unwrap(),
        state: crate::materialization::MaterializationState::Pending,
        backend: None,
        backend_generation: crate::ids::BackendGeneration::new(1),
        rendered_objects: vec![],
        exclusivity_keys: vec![],
        reconciliation_lease: None,
    };
    let plan =
        crate::projection::ProjectionPlan::from_manifest(&materialization, &rendered).unwrap();
    assert_eq!(
        plan.object_refs()
            .iter()
            .filter(|object| object.kind == "Service")
            .map(|object| object.name.as_str())
            .collect::<Vec<_>>(),
        ["svc-acme-69856ec0", "raw-svc-acme"],
        "stable apply ordering preserves the generated primary Service ahead of raw Services"
    );
    assert_eq!(
        raw_service["metadata"]["labels"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert!(raw_service["metadata"]["labels"][LABEL_WORKLOAD_NAME].is_null());
    assert_eq!(raw_service["spec"]["type"], json!("ClusterIP"));
    assert_eq!(
        raw_service["metadata"]["finalizers"],
        json!(["raw.example.com/protect"])
    );

    let raw_stateful_set = rendered
        .objects
        .iter()
        .find(|object| object_name(&object.object) == "raw-db-acme")
        .expect("raw stateful set rendered")
        .to_kubernetes_json();
    assert_eq!(
        raw_stateful_set["metadata"]["labels"][LABEL_INSTANCE_GENERATION],
        json!("7")
    );
    assert_eq!(
        raw_stateful_set["spec"]["template"]["metadata"]["labels"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert_eq!(
        raw_stateful_set["spec"]["template"]["metadata"]["name"],
        json!("raw-pod-template")
    );
    assert_eq!(
        raw_stateful_set["spec"]["podManagementPolicy"],
        json!("Parallel")
    );
}

#[test]
fn rejects_raw_manifest_with_disallowed_kind() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: batch/v1
kind: Job
metadata:
  name: raw-job
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("disallowed kind is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.kind",
        "not in the V1 allow-list",
    );
}

#[test]
fn rejects_invalid_raw_manifest_yaml() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest("apiVersion: [")];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("invalid YAML is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest",
        "manifest must be valid YAML or JSON",
    );
}

#[test]
fn rejects_raw_manifest_missing_name() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata: {}
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("missing name is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.name",
        "name must be a non-empty string",
    );
}

#[test]
fn rejects_raw_manifest_non_string_namespace() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
  namespace: 123
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("non-string namespace is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.namespace",
        "namespace must be a string",
    );
}

#[test]
fn rejects_raw_persistent_volume_with_namespace_key() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: PersistentVolume
metadata:
  name: raw-pv
  namespace: apps
spec:
  persistentVolumeReclaimPolicy: Retain
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("PersistentVolume namespace is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.namespace",
        "PersistentVolume must be cluster-scoped with no namespace",
    );
}

#[test]
fn rejects_duplicate_raw_and_typed_rendered_object_refs() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: svc-acme-69856ec0
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("duplicate ref is rejected");

    assert_invalid_field(
        error,
        "template",
        "duplicate rendered Kubernetes object ref v1 Service apps/svc-acme-69856ec0",
    );
}

#[test]
fn rejects_conflicting_raw_sleepypods_labels() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
  labels:
    sleepypods.io/instance-id: other-instance
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("conflicting ownership label is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.labels",
        "sleepypods.io/instance-id must be \"instance-a\"",
    );
}

#[test]
fn rejects_non_string_raw_sleepypods_labels() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
  labels:
    sleepypods.io/instance-id: 7
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("non-string ownership label is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.labels",
        "sleepypods.io/instance-id must be \"instance-a\", got non-string value",
    );
}

#[test]
fn rejects_non_string_raw_sleepypods_annotations() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
  annotations:
    sleepypods.io/template-generation: [3]
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect_err("non-string template annotation is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.annotations",
        "sleepypods.io/template-generation must be \"3\", got non-string value",
    );
}

#[test]
fn rejects_a_templated_raw_persistent_volume_that_omits_retain() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: PersistentVolume
metadata:
  name: raw-pv-"#,
        ),
        TemplateTextPart::instance_value("tenant"),
        TemplateTextPart::literal(
            r#"
spec:
  capacity:
    storage: 1Gi
  persistentVolumeReclaimPolicy: Delete
"#,
        ),
    ])];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect_err("a raw PV without Retain is rejected even when templated");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volumes.reclaim_policy",
            message: "raw managed static PVs require explicit Retain".to_owned(),
        }
    );
}

#[test]
fn rejects_a_raw_persistent_volume_whose_reclaim_policy_an_instance_chooses() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: PersistentVolume
metadata:
  name: raw-pv
spec:
  capacity:
    storage: 1Gi
  persistentVolumeReclaimPolicy: "#,
        ),
        TemplateTextPart::instance_value("reclaim"),
        TemplateTextPart::literal("\n"),
    ])];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "instance-a",
            7,
            values([("tenant", "acme"), ("reclaim", "Retain")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect_err("an instance may not choose the reclaim policy");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volumes.reclaim_policy",
            message: "raw managed static PVs require explicit Retain".to_owned(),
        }
    );
}

#[test]
fn renders_a_templated_raw_persistent_volume_that_declares_retain() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: PersistentVolume
metadata:
  name: raw-pv-"#,
        ),
        TemplateTextPart::instance_value("tenant"),
        TemplateTextPart::literal(
            r#"
spec:
  capacity:
    storage: 1Gi
  persistentVolumeReclaimPolicy: Retain
"#,
        ),
    ])];

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("a templated raw PV declaring Retain renders");

    assert_eq!(raw_object(&rendered).metadata.name, "raw-pv-acme");
}

#[test]
fn raw_manifest_binds_an_instance_value_that_spells_a_placeholder_once() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
  annotations:
    example.com/first: ""#,
        ),
        TemplateTextPart::instance_value("first"),
        TemplateTextPart::literal(
            r#""
    example.com/second: ""#,
        ),
        TemplateTextPart::instance_value("second"),
        TemplateTextPart::literal(
            r#""
spec:
  ports:
    - name: http
      port: 8080
      targetPort: 8080
"#,
        ),
    ])];

    // The first value spells the placeholder the second value uses, which a
    // second pass over the document would replace.
    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "instance-a",
            7,
            values([
                ("tenant", "acme"),
                ("first", "sleepypodsInstanceValue1-"),
                ("second", "plain"),
            ]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("raw object renders");

    let annotations = &raw_object(&rendered).value["metadata"]["annotations"];
    assert_eq!(
        annotations["example.com/first"],
        "sleepypodsInstanceValue1-"
    );
    assert_eq!(annotations["example.com/second"], "plain");
}

#[test]
fn raw_manifest_keeps_literal_text_that_looks_like_a_placeholder() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
  annotations:
    example.com/author: "sleepypodsInstanceValue0-"
    example.com/tenant: ""#,
        ),
        TemplateTextPart::instance_value("tenant"),
        TemplateTextPart::literal(
            r#""
spec:
  ports:
    - name: http
      port: 8080
      targetPort: 8080
"#,
        ),
    ])];

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("raw object renders");

    let annotations = &raw_object(&rendered).value["metadata"]["annotations"];
    assert_eq!(
        annotations["example.com/author"],
        "sleepypodsInstanceValue0-"
    );
    assert_eq!(annotations["example.com/tenant"], "acme");
}

#[test]
fn raw_manifest_binds_an_instance_value_used_as_a_key() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
  annotations:
    example.com/"#,
        ),
        TemplateTextPart::instance_value("tenant"),
        TemplateTextPart::literal(
            r#": owned
spec:
  ports:
    - name: http
      port: 8080
      targetPort: 8080
"#,
        ),
    ])];

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("raw object renders");

    assert_eq!(
        raw_object(&rendered).value["metadata"]["annotations"]["example.com/acme"],
        "owned"
    );
}

#[test]
fn rejects_host_path_whose_root_an_instance_value_chooses() {
    let mut template = host_path_stateful_template();
    template.volumes[0].source = PersistentVolumeSourceTemplate::HostPath {
        path: TemplateText::instance_value("data_dir"),
        type_: None,
    };

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("data_dir", "/")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("a hostPath root chosen by an instance is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: "hostPath path \"/\" must start with a literal absolute directory, so that \
                      an instance value chooses a subdirectory rather than the root"
                .to_owned(),
        }
    );
}

#[test]
fn rejects_host_path_that_climbs_out_of_the_authors_directory() {
    let error = render_manifests(RenderManifestRequest {
        template: &data_dir_host_path_template(),
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("data_dir", "acme/../../etc")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("a hostPath climbing out of its directory is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: "hostPath path \"/var/local/sleepypods/acme/../../etc\" must stay inside \
                      its directory, so no segment may be \"..\""
                .to_owned(),
        }
    );
}

#[test]
fn renders_host_path_subdirectory_whose_name_begins_with_dots() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &data_dir_host_path_template(),
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("data_dir", "..data")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("a subdirectory whose name begins with dots still renders");

    let volume = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::PersistentVolume(volume) => Some(volume),
            _ => None,
        })
        .expect("PersistentVolume was rendered");

    assert_eq!(
        volume.spec.source,
        PersistentVolumeSource::HostPath(HostPathPersistentVolumeSource {
            path: "/var/local/sleepypods/..data".to_owned(),
            type_: Some("DirectoryOrCreate".to_owned()),
        })
    );
}

#[test]
fn raw_rendered_object_refs_are_available_without_applying() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc
"#,
    )];

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("raw object renders");

    let refs = rendered_object_refs(&rendered).expect("refs derive before apply");

    assert_eq!(
        refs,
        vec![
            object_ref("v1", "Service", "apps", "svc-acme-69856ec0"),
            object_ref("v1", "Service", "apps", "raw-svc"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0"),
        ]
    );
}

#[test]
fn renders_static_archil_csi_volume_with_node_publish_secret_ref() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &archil_static_csi_template(),
        instance: &instance(
            "postgres-a",
            2,
            values([
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
                ("secret", "archil-node-secret"),
                ("secret_namespace", "storage-secrets"),
            ]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("Archil-style CSI workload renders");

    let pv = match &rendered.objects[0].object {
        KubernetesObject::PersistentVolume(pv) => pv,
        other => panic!("expected PersistentVolume, got {}", other.kind()),
    };
    assert_eq!(
        pv.spec.persistent_volume_reclaim_policy,
        PersistentVolumeReclaimPolicy::Retain
    );
    assert_eq!(pv.spec.claim_ref.namespace, "data");
    assert_eq!(pv.spec.claim_ref.name, "pvc-acme-2e1ac556");
    assert_eq!(
        pv.spec.source,
        PersistentVolumeSource::Csi(Box::new(CsiPersistentVolumeSource {
            driver: "csi.archil.com".to_owned(),
            volume_handle: "archil-volume-123".to_owned(),
            fs_type: None,
            read_only: false,
            volume_attributes: BTreeMap::from([("region".to_owned(), "us-west-2".to_owned())]),
            controller_publish_secret_ref: None,
            node_stage_secret_ref: None,
            node_publish_secret_ref: Some(CsiSecretReference {
                name: "archil-node-secret".to_owned(),
                namespace: "storage-secrets".to_owned(),
            }),
            controller_expand_secret_ref: None,
            node_expand_secret_ref: None,
        }))
    );

    let objects = rendered.to_kubernetes_json_values();
    assert_eq!(
        objects[0]["spec"]["csi"]["nodePublishSecretRef"],
        json!({
            "name": "archil-node-secret",
            "namespace": "storage-secrets",
        })
    );
    assert_eq!(
        objects[0]["spec"]["csi"]["volumeAttributes"]["region"],
        json!("us-west-2")
    );
    assert_eq!(objects[1]["spec"]["volumeName"], json!("pv-acme-2e1ac556"));
}

#[test]
fn renders_all_csi_secret_refs_to_kubernetes_keys() {
    let mut template = archil_static_csi_template();
    let PersistentVolumeSourceTemplate::Csi {
        controller_publish_secret_ref,
        node_stage_secret_ref,
        node_publish_secret_ref,
        controller_expand_secret_ref,
        node_expand_secret_ref,
        ..
    } = &mut template.volumes[0].source
    else {
        panic!("expected CSI source");
    };
    *controller_publish_secret_ref = Some(Box::new(csi_secret_ref("controller-publish-secret")));
    *node_stage_secret_ref = Some(Box::new(csi_secret_ref("node-stage-secret")));
    *node_publish_secret_ref = Some(Box::new(csi_secret_ref("node-publish-secret")));
    *controller_expand_secret_ref = Some(Box::new(csi_secret_ref("controller-expand-secret")));
    *node_expand_secret_ref = Some(Box::new(csi_secret_ref("node-expand-secret")));

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
            ]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("CSI workload with all secret refs renders");

    let pv = match &rendered.objects[0].object {
        KubernetesObject::PersistentVolume(pv) => pv,
        other => panic!("expected PersistentVolume, got {}", other.kind()),
    };
    let PersistentVolumeSource::Csi(csi) = &pv.spec.source else {
        panic!("expected CSI source");
    };

    let expected = [
        (
            "controllerPublishSecretRef",
            "controller-publish-secret",
            csi.controller_publish_secret_ref.as_ref(),
        ),
        (
            "nodeStageSecretRef",
            "node-stage-secret",
            csi.node_stage_secret_ref.as_ref(),
        ),
        (
            "nodePublishSecretRef",
            "node-publish-secret",
            csi.node_publish_secret_ref.as_ref(),
        ),
        (
            "controllerExpandSecretRef",
            "controller-expand-secret",
            csi.controller_expand_secret_ref.as_ref(),
        ),
        (
            "nodeExpandSecretRef",
            "node-expand-secret",
            csi.node_expand_secret_ref.as_ref(),
        ),
    ];
    let objects = rendered.to_kubernetes_json_values();
    let csi_json = &objects[0]["spec"]["csi"];

    for (key, name, rendered_ref) in expected {
        assert_eq!(
            rendered_ref.map(|ref_| (ref_.name.as_str(), ref_.namespace.as_str())),
            Some((name, "storage-secrets")),
            "{key}"
        );
        assert_eq!(
            csi_json[key],
            json!({
                "name": name,
                "namespace": "storage-secrets",
            }),
            "{key}"
        );
    }
}

#[test]
fn serializes_stateful_set_pv_and_pvc_as_kubernetes_json() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &stateful_template(),
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("stateful workload renders");

    let objects = rendered.to_kubernetes_json_values();
    let pv = &objects[0];
    assert_eq!(pv["apiVersion"], json!("v1"));
    assert_eq!(pv["kind"], json!("PersistentVolume"));
    assert_eq!(pv["metadata"]["name"], json!("pv-acme-2e1ac556"));
    assert!(
        pv["metadata"].get("namespace").is_none(),
        "PersistentVolumes are cluster-scoped"
    );
    assert_eq!(
        pv["spec"],
        json!({
            "capacity": {
                "storage": "10Gi",
            },
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "manual",
            "claimRef": {
                "namespace": "data",
                "name": "pvc-acme-2e1ac556",
            },
            "csi": {
                "driver": "csi.example.com",
                "volumeHandle": "provider-vol-123",
                "fsType": "ext4",
                "readOnly": false,
                "volumeAttributes": {
                    "tenant": "acme",
                },
            },
        })
    );

    let pvc = &objects[1];
    assert_eq!(pvc["apiVersion"], json!("v1"));
    assert_eq!(pvc["kind"], json!("PersistentVolumeClaim"));
    assert_eq!(pvc["metadata"]["name"], json!("pvc-acme-2e1ac556"));
    assert_eq!(pvc["metadata"]["namespace"], json!("data"));
    assert_eq!(
        pvc["spec"],
        json!({
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "10Gi",
                },
            },
            "storageClassName": "manual",
            "volumeName": "pv-acme-2e1ac556",
        })
    );

    let service = &objects[2];
    assert_eq!(service["kind"], json!("Service"));
    assert_eq!(service["spec"]["ports"][0]["targetPort"], json!(15000));

    let stateful_set = &objects[3];
    assert_eq!(stateful_set["apiVersion"], json!("apps/v1"));
    assert_eq!(stateful_set["kind"], json!("StatefulSet"));
    assert_eq!(stateful_set["metadata"]["name"], json!("db-acme-2e1ac556"));
    assert_eq!(
        stateful_set["spec"]["serviceName"],
        json!("db-acme-2e1ac556")
    );
    assert_eq!(stateful_set["spec"]["replicas"], json!(1));
    assert_eq!(
        stateful_set["spec"]["template"]["spec"]["volumes"][0],
        json!({
            "name": "data",
            "persistentVolumeClaim": {
                "claimName": "pvc-acme-2e1ac556",
            },
        })
    );
    assert_eq!(
        stateful_set["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][0],
        json!({
            "name": "data",
            "mountPath": "/var/lib/postgresql/data",
        })
    );
    assert_eq!(
        stateful_set["spec"]["template"]["spec"]["containers"][1]["volumeMounts"],
        json!([])
    );
}

#[test]
fn renders_host_path_persistent_volume_source() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &host_path_stateful_template(),
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("hostPath stateful workload renders");

    let pv = match &rendered.objects[0].object {
        KubernetesObject::PersistentVolume(pv) => pv,
        other => panic!("expected PersistentVolume, got {}", other.kind()),
    };

    assert_eq!(
        pv.spec.source,
        PersistentVolumeSource::HostPath(HostPathPersistentVolumeSource {
            path: "/var/local/sleepypods/acme".to_owned(),
            type_: Some("DirectoryOrCreate".to_owned()),
        })
    );
}

#[test]
fn serializes_host_path_persistent_volume_source_as_kubernetes_json() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &host_path_stateful_template(),
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("hostPath stateful workload renders");

    let pv = &rendered.to_kubernetes_json_values()[0];

    assert_eq!(
        pv["spec"]["hostPath"],
        json!({
            "path": "/var/local/sleepypods/acme",
            "type": "DirectoryOrCreate",
        })
    );
    assert!(
        pv["spec"].get("csi").is_none(),
        "hostPath PVs should not serialize a CSI source"
    );
}

#[test]
fn managed_storage_rejects_destructive_policy_at_class_admission_and_legacy_render() {
    let mut template = stateful_template();
    template.volumes[0].reclaim_policy = PersistentVolumeReclaimPolicy::Delete;
    let instance = instance(
        "retention",
        2,
        values([("tenant", "acme"), ("volume", "disk")]),
    );
    let class = crate::workload::WorkloadClassVersion {
        reference: instance.workload_class.clone(),
        template_generation: Generation::new(1),
        template: template.clone(),
        default_values: Default::default(),
        value_schema: crate::workload::WorkloadValueSchema::new(true),
        sleep_policy: crate::sleep_policy::WorkloadSleepPolicy::new(60_000, 1_000, 30_000).unwrap(),
        exclusivity_keys: vec![],
    };
    assert!(matches!(
        class.validate(),
        Err(crate::workload::WorkloadClassValidationError::StorageContract(_))
    ));
    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance,
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .unwrap_err();
    assert!(error.to_string().contains("Retain"));
}

#[test]
fn raw_storage_cannot_bypass_retention_or_static_binding_validation() {
    for raw in [
        r#"{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"disk"},"spec":{"persistentVolumeReclaimPolicy":"Delete"}}"#,
        r#"{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"disk"},"spec":{}}"#,
        r#"{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"claim"},"spec":{}}"#,
        r#"{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"claim"},"spec":{"volumeName":"external-unmanaged"}}"#,
    ] {
        let mut template = deployment_template();
        template.raw_objects = vec![raw_manifest(raw)];
        let result = render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("raw-storage", 2, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
            template_generation: None,
        });
        assert!(result.is_err(), "unsafe raw storage must be refused: {raw}");
    }
}

#[test]
fn rendering_legacy_classes_rejects_zero_and_multiple_replicas() {
    for kind in [WorkloadKind::Deployment, WorkloadKind::StatefulSet] {
        for replicas in [0, 2] {
            let mut template = stateful_template();
            template.workload.kind = kind;
            template.workload.replicas = Some(replicas);
            let error = render_manifests(RenderManifestRequest {
                template: &template,
                instance: &instance("old-app", 2, values([("tenant", "acme"), ("volume", "v1")])),
                sleep_policy: sleep_policy(),
                namespace: "apps",
                template_generation: None,
            })
            .expect_err("legacy unsupported replica count cannot create resources");
            assert!(matches!(error, ManifestRenderError::InvalidReplicas { .. }));
        }
    }
}

#[test]
fn rejects_stateful_set_scale_above_one() {
    let mut template = stateful_template();
    template.workload.replicas = Some(2);

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("scaled StatefulSet is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidReplicas {
            kind: WorkloadKind::StatefulSet,
            replicas: 2,
            message: "automatic sleep requires exactly one replica".to_owned(),
        }
    );
}

#[test]
fn instance_id_rejects_values_that_cannot_be_used_in_rendered_names() {
    for value in ["tenant/foo", "tenant_foo", "Tenant", "-tenant", "tenant-"] {
        let error = InstanceId::new(value).expect_err("invalid instance ID is rejected");

        assert_eq!(error.field(), "InstanceId");
        assert_eq!(error.value(), value);
    }
}

#[test]
fn rejects_workload_class_id_that_is_not_label_value_safe() {
    let overlong_class_id = "a".repeat(64);
    let error = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance_with_class(
            "instance-a",
            &overlong_class_id,
            7,
            values([("tenant", "acme")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("invalid workload class label is rejected");

    assert_invalid_field(error, LABEL_WORKLOAD_CLASS_ID, &overlong_class_id);
}

#[test]
fn rejects_zero_container_port() {
    let mut template = deployment_template();
    template.workload.app_container.ports[0].container_port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero container port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "container.ports.container_port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_zero_service_port() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero service port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_zero_service_target_port() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].target_port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero service target port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_zero_sidecar_listen_port() {
    let mut template = deployment_template();
    template.sidecar.listen_port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero sidecar listen port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "sidecar.listen_port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_invalid_sidecar_mode() {
    let mut template = deployment_template();
    template.sidecar.mode = Some("smtp".to_owned());

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("invalid sidecar mode is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "sidecar.mode",
            message: "sidecar mode must be either http or tcp".to_owned(),
        }
    );
}

#[test]
fn rejects_service_without_ports_for_sidecar_routing() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports = Vec::new();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("service port is required");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports",
            message: "exactly one service port is supported for sidecar-routed workloads"
                .to_owned(),
        }
    );
}

#[test]
fn rejects_missing_service_for_sidecar_routing() {
    let mut template = deployment_template();
    template.service = None;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("service is required");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service",
            message: "sidecar-routed workloads require a service template".to_owned(),
        }
    );
}

#[test]
fn rejects_multiple_service_ports_for_sidecar_routing() {
    let mut template = deployment_template();
    template
        .service
        .as_mut()
        .expect("service")
        .ports
        .push(ServicePortTemplate {
            name: Some("admin".to_owned()),
            port: 8081,
            target_port: 8081,
        });

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("ambiguous service ports are rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports",
            message: "exactly one service port is supported for sidecar-routed workloads"
                .to_owned(),
        }
    );
}

#[test]
fn rejects_app_target_port_matching_sidecar_listen_port() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].target_port = template.sidecar.listen_port;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("loop-prone sidecar target is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "original app target port must differ from the sidecar listen port".to_owned(),
        }
    );
}

#[test]
fn rejects_service_target_port_not_declared_on_app_container() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].target_port = 9090;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("service target port must be an app container port");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "target port 9090 must match an app container port".to_owned(),
        }
    );
}

#[test]
fn rejects_volume_without_access_modes() {
    let mut template = stateful_template();
    template.volumes[0].access_modes = Vec::new();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("empty access modes are rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.access_modes",
            message: "at least one access mode is required".to_owned(),
        }
    );
}

#[test]
fn rejects_duplicate_volume_access_modes() {
    let mut template = stateful_template();
    template.volumes[0].access_modes = vec![
        PersistentVolumeAccessMode::ReadWriteOnce,
        PersistentVolumeAccessMode::ReadWriteOnce,
    ];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("duplicate access modes are rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.access_modes",
            message: "access modes must not contain duplicates".to_owned(),
        }
    );
}

#[test]
fn rejects_unsupported_volume_access_mode_at_decode_boundary() {
    let mut encoded = serde_json::to_value(stateful_template()).expect("template encodes");
    encoded["volumes"][0]["access_modes"][0] = json!("ReadWriteOncePod");

    let error = serde_json::from_value::<ManifestTemplate>(encoded)
        .expect_err("unsupported access mode is rejected while decoding");

    assert!(
        error.to_string().contains("ReadWriteOncePod"),
        "decode error {error} should identify the unsupported access mode"
    );
}

#[test]
fn rejects_missing_csi_volume_handle_value() {
    let template = stateful_template();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("missing CSI volume handle is rejected");

    assert_eq!(
        error,
        ManifestRenderError::MissingInstanceValue {
            field: "volume".to_owned(),
        }
    );
}

#[test]
fn rejects_empty_csi_volume_handle() {
    let template = stateful_template();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "   ")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("empty CSI volume handle is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.source.csi.volume_handle",
            message: "rendered value must not be empty".to_owned(),
        }
    );
}

#[test]
fn rejects_invalid_csi_secret_ref_templates() {
    let cases = [
        (
            "missing name",
            vec![
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
                ("secret_namespace", "storage-secrets"),
            ],
            ManifestRenderError::MissingInstanceValue {
                field: "secret".to_owned(),
            },
        ),
        (
            "empty name",
            vec![
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
                ("secret", "   "),
                ("secret_namespace", "storage-secrets"),
            ],
            ManifestRenderError::InvalidField {
                field: "volume.source.csi.node_publish_secret_ref.name",
                message: "rendered value must not be empty".to_owned(),
            },
        ),
        (
            "invalid name",
            vec![
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
                ("secret", "Archil_Secret"),
                ("secret_namespace", "storage-secrets"),
            ],
            ManifestRenderError::InvalidName {
                field: "volume.source.csi.node_publish_secret_ref.name",
                value: "Archil_Secret".to_owned(),
            },
        ),
        (
            "missing namespace",
            vec![
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
                ("secret", "archil-node-secret"),
            ],
            ManifestRenderError::MissingInstanceValue {
                field: "secret_namespace".to_owned(),
            },
        ),
        (
            "empty namespace",
            vec![
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
                ("secret", "archil-node-secret"),
                ("secret_namespace", "   "),
            ],
            ManifestRenderError::InvalidField {
                field: "volume.source.csi.node_publish_secret_ref.namespace",
                message: "rendered value must not be empty".to_owned(),
            },
        ),
        (
            "invalid namespace",
            vec![
                ("tenant", "acme"),
                ("volume", "archil-volume-123"),
                ("region", "us-west-2"),
                ("secret", "archil-node-secret"),
                ("secret_namespace", "storage_secrets"),
            ],
            ManifestRenderError::InvalidName {
                field: "volume.source.csi.node_publish_secret_ref.namespace",
                value: "storage_secrets".to_owned(),
            },
        ),
    ];

    for (name, value_pairs, expected) in cases {
        let template = archil_static_csi_template();
        let values: InstanceValues = value_pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();

        let result = render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("postgres-a", 2, values),
            sleep_policy: sleep_policy(),
            namespace: "data",
            template_generation: None,
        });
        let error = match result {
            Ok(_) => panic!("{name} should be rejected"),
            Err(error) => error,
        };

        assert_eq!(error, expected, "{name}");
    }
}

#[test]
fn rejects_relative_host_path_persistent_volume_source() {
    let mut template = host_path_stateful_template();
    template.volumes[0].source = PersistentVolumeSourceTemplate::HostPath {
        path: TemplateText::literal("var/local/sleepypods/acme"),
        type_: None,
    };

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("relative hostPath source is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: "hostPath path \"var/local/sleepypods/acme\" must be absolute".to_owned(),
        }
    );
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
                volume_attributes: BTreeMap::from([(
                    "tenant".to_owned(),
                    TemplateText::instance_value("tenant"),
                )]),
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

fn archil_static_csi_template() -> ManifestTemplate {
    let mut template = stateful_template();
    template.volumes[0].source = PersistentVolumeSourceTemplate::Csi {
        driver: TemplateText::literal("csi.archil.com"),
        volume_handle: TemplateText::instance_value("volume"),
        fs_type: None,
        read_only: false,
        volume_attributes: BTreeMap::from([(
            "region".to_owned(),
            TemplateText::instance_value("region"),
        )]),
        controller_publish_secret_ref: None,
        node_stage_secret_ref: None,
        node_publish_secret_ref: Some(Box::new(CsiSecretRefTemplate {
            name: TemplateText::instance_value("secret"),
            namespace: TemplateText::instance_value("secret_namespace"),
        })),
        controller_expand_secret_ref: None,
        node_expand_secret_ref: None,
    };
    template
}

fn csi_secret_ref(name: &str) -> CsiSecretRefTemplate {
    CsiSecretRefTemplate {
        name: TemplateText::literal(name),
        namespace: TemplateText::literal("storage-secrets"),
    }
}

fn host_path_stateful_template() -> ManifestTemplate {
    let mut template = stateful_template();
    template.volumes[0].storage_class_name = Some(TemplateText::literal("manual-host-path"));
    template.volumes[0].source = PersistentVolumeSourceTemplate::HostPath {
        path: TemplateText::from_parts([
            TemplateTextPart::literal("/var/local/sleepypods/"),
            TemplateTextPart::instance_value("tenant"),
        ]),
        type_: Some(TemplateText::literal("DirectoryOrCreate")),
    };
    template
}

/// A hostPath template whose subdirectory comes from its own field, leaving the
/// workload name free to stay a valid Kubernetes name.
fn data_dir_host_path_template() -> ManifestTemplate {
    let mut template = host_path_stateful_template();
    template.volumes[0].source = PersistentVolumeSourceTemplate::HostPath {
        path: TemplateText::from_parts([
            TemplateTextPart::literal("/var/local/sleepypods/"),
            TemplateTextPart::instance_value("data_dir"),
        ]),
        type_: Some(TemplateText::literal("DirectoryOrCreate")),
    };
    template
}

fn sidecar_template() -> SidecarTemplate {
    SidecarTemplate {
        name: "sleepypods-sidecar".to_owned(),
        image: TemplateText::literal("sleepypods/sidecar:test"),
        listen_port: 15000,
        mode: None,
    }
}

fn sleep_policy() -> ResolvedSleepPolicy {
    ResolvedSleepPolicy {
        idle_timeout_ms: 120_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 30_000,
    }
}

fn assert_env(env: &[EnvVar], name: &str, expected: &str) {
    let value = env
        .iter()
        .find(|var| var.name == name)
        .unwrap_or_else(|| panic!("missing env var {name}"));

    assert_eq!(value.value, expected);
}

fn assert_env_absent(env: &[EnvVar], name: &str) {
    assert!(
        env.iter().all(|var| var.name != name),
        "env var {name} should be absent"
    );
}

fn object_name(object: &KubernetesObject) -> &str {
    match object {
        KubernetesObject::Deployment(object) => &object.metadata.name,
        KubernetesObject::StatefulSet(object) => &object.metadata.name,
        KubernetesObject::Service(object) => &object.metadata.name,
        KubernetesObject::Secret(object) => &object.metadata.name,
        KubernetesObject::PersistentVolume(object) => &object.metadata.name,
        KubernetesObject::PersistentVolumeClaim(object) => &object.metadata.name,
        KubernetesObject::Raw(object) => &object.metadata.name,
    }
}

fn composed(prefix: &str, field: &str) -> TemplateText {
    TemplateText::from_parts([
        TemplateTextPart::literal(prefix),
        TemplateTextPart::instance_value(field),
    ])
}

fn raw_manifest(manifest: &str) -> RawKubernetesManifestTemplate {
    RawKubernetesManifestTemplate {
        manifest: TemplateText::literal(manifest),
    }
}

fn raw_manifest_parts(parts: impl Into<Vec<TemplateTextPart>>) -> RawKubernetesManifestTemplate {
    RawKubernetesManifestTemplate {
        manifest: TemplateText::from_parts(parts),
    }
}

fn raw_object(rendered: &RenderedManifest) -> &RawKubernetesObject {
    rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Raw(raw) => Some(raw),
            _ => None,
        })
        .expect("raw object was rendered")
}

fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}

fn instance(id: &str, generation: u64, values: InstanceValues) -> InstanceRecord {
    instance_with_class(id, "web", generation, values)
}

fn instance_with_class(
    id: &str,
    class_id: &str,
    generation: u64,
    values: InstanceValues,
) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id).expect("valid instance ID"),
        workload_class: WorkloadClassVersionRef::new(
            WorkloadClassId::new(class_id).expect("valid class ID"),
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

fn assert_invalid_field(error: ManifestRenderError, field: &'static str, value: &str) {
    match error {
        ManifestRenderError::InvalidField {
            field: actual,
            message,
        } => {
            assert_eq!(actual, field);
            assert!(
                message.contains(value),
                "message {message:?} should identify invalid value {value:?}"
            );
        }
        other => panic!("expected InvalidField for {field}, got {other:?}"),
    }
}

#[test]
fn primary_http_and_tcp_workloads_have_private_transport_readiness() {
    for mode in [None, Some("tcp".to_owned())] {
        let mut template = deployment_template();
        template.sidecar.mode = mode;
        let rendered = render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
            template_generation: None,
        })
        .unwrap();
        let workload = rendered
            .objects
            .iter()
            .find(|object| object.object.kind() == "Deployment")
            .unwrap()
            .to_kubernetes_json();
        let containers = &workload["spec"]["template"]["spec"]["containers"];
        assert!(containers[0]["readinessProbe"].is_null());
        assert_eq!(
            containers[1]["readinessProbe"],
            json!({
                "httpGet": {"path":"/ready", "port":15001},
                "periodSeconds":1,"timeoutSeconds":1,"failureThreshold":1,
            })
        );
        assert!(containers[1]["env"].as_array().unwrap().contains(&json!({
            "name":"SLEEPYPODS_SIDECAR_READINESS_LISTEN_ADDR", "value":"0.0.0.0:15001"
        })));
        let service = rendered
            .objects
            .iter()
            .find(|object| object.object.kind() == "Service")
            .unwrap()
            .to_kubernetes_json();
        assert_eq!(service["spec"]["ports"].as_array().unwrap().len(), 1);
        assert_eq!(service["spec"]["ports"][0]["targetPort"], 15000);
    }
}

#[test]
fn readiness_port_skips_declared_app_proxy_and_metrics_ports() {
    let mut template = deployment_template();
    template.workload.app_container.ports[0].name = Some("sleepypods".to_owned());
    template.sidecar.listen_port = 15002;
    template
        .workload
        .app_container
        .ports
        .push(ContainerPortTemplate {
            name: Some("sp-readiness".to_owned()),
            container_port: 15001,
        });
    template.workload.app_container.env.push(EnvVarTemplate {
        name: "SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR".to_owned(),
        value: TemplateText::literal("127.0.0.1:15003"),
    });
    let render = |template: &ManifestTemplate| {
        render_manifests(RenderManifestRequest {
            template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
            template_generation: None,
        })
    };
    let rendered = render(&template).unwrap();
    let workload = rendered
        .objects
        .iter()
        .find(|object| object.object.kind() == "Deployment")
        .unwrap()
        .to_kubernetes_json();
    let containers = &workload["spec"]["template"]["spec"]["containers"];
    assert_eq!(containers[0]["ports"][0]["name"], "sleepypods");
    assert!(
        containers[1]["ports"][0]["name"].is_null(),
        "numeric proxy port cannot collide with an app port name"
    );
    assert_eq!(containers[0]["ports"][1]["name"], "sp-readiness");
    assert!(
        containers[1]["ports"][1]["name"].is_null(),
        "numeric health port cannot collide with an app port name"
    );
    assert_eq!(
        workload["spec"]["template"]["spec"]["containers"][1]["readinessProbe"]["httpGet"]["port"],
        15004
    );
    template
        .workload
        .app_container
        .env
        .last_mut()
        .unwrap()
        .value = TemplateText::literal("");
    let empty_metrics =
        render(&template).expect("empty optional metrics listener remains disabled");
    let workload = empty_metrics
        .objects
        .iter()
        .find(|object| object.object.kind() == "Deployment")
        .unwrap()
        .to_kubernetes_json();
    assert_eq!(
        workload["spec"]["template"]["spec"]["containers"][1]["readinessProbe"]["httpGet"]["port"],
        15003
    );
    template.workload.app_container.ports = (1024..=u16::MAX)
        .map(|container_port| ContainerPortTemplate {
            name: None,
            container_port,
        })
        .collect();
    assert!(matches!(
        render(&template),
        Err(ManifestRenderError::InvalidField {
            field: "sidecar.readiness_port",
            ..
        })
    ));
}

#[test]
fn raw_auxiliary_workloads_never_match_primary_service_selector() {
    let mut template = deployment_template();
    template.raw_objects = ["Deployment", "StatefulSet"]
        .map(|kind| {
            raw_manifest(&format!(
                r#"
apiVersion: apps/v1
kind: {kind}
metadata:
  name: auxiliary
spec:
  selector:
    matchLabels:
      app: auxiliary
  template:
    metadata:
      labels:
        app: auxiliary
    spec:
      containers:
      - name: auxiliary
        image: example/auxiliary:test
"#
            ))
        })
        .to_vec();
    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .unwrap();
    let service = rendered
        .objects
        .iter()
        .find(|object| object.object.kind() == "Service")
        .unwrap()
        .to_kubernetes_json();
    let selector = service["spec"]["selector"].as_object().unwrap();
    for object in rendered
        .objects
        .iter()
        .filter(|object| matches!(object.object, KubernetesObject::Raw(_)))
    {
        let value = object.to_kubernetes_json();
        let labels = value["spec"]["template"]["metadata"]["labels"]
            .as_object()
            .unwrap();
        assert_eq!(labels[LABEL_INSTANCE_ID], "instance-a");
        assert_eq!(labels[LABEL_INSTANCE_GENERATION], "7");
        assert!(!labels.contains_key(LABEL_WORKLOAD_NAME));
        assert!(!selector
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value)));
        assert_eq!(
            value["spec"]["selector"],
            json!({"matchLabels":{"app":"auxiliary"}})
        );
    }
}

#[test]
fn raw_auxiliaries_cannot_supply_reserved_primary_selector_labels_or_expressions() {
    for pointer in [
        "/metadata/labels",
        "/spec/template/metadata/labels",
        "/spec/selector/matchLabels",
        "/spec/selector/matchExpressions",
    ] {
        let mut raw = json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"auxiliary","labels":{}},"spec":{"selector":{"matchLabels":{},"matchExpressions":[]},"template":{"metadata":{"labels":{}},"spec":{"containers":[]}}}});
        if pointer.ends_with("matchExpressions") {
            *raw.pointer_mut(pointer).unwrap() =
                json!([{"key":LABEL_WORKLOAD_NAME,"operator":"Exists"}]);
        } else {
            raw.pointer_mut(pointer).unwrap()[LABEL_WORKLOAD_NAME] = json!("app-acme-69856ec0");
        }
        let mut template = deployment_template();
        template.raw_objects = vec![raw_manifest(&raw.to_string())];
        assert!(matches!(
            render_manifests(RenderManifestRequest {
                template: &template,
                instance: &instance("instance-a", 7, values([("tenant", "acme")])),
                sleep_policy: sleep_policy(),
                namespace: "apps",
                template_generation: None,
            }),
            Err(ManifestRenderError::InvalidField {
                field: "raw_objects.manifest",
                ..
            })
        ));
    }
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: unsupported",
    )];
    assert!(matches!(
        render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
            template_generation: None,
        }),
        Err(ManifestRenderError::InvalidField {
            field: "raw_objects.manifest.kind",
            ..
        })
    ));
}

#[test]
fn raw_service_named_proxy_target_remains_compatible_when_app_name_does_not_collide() {
    let mut template = deployment_template();
    template.raw_objects.push(raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: primary-alias
spec:
  selector:
    sleepypods.io/instance-id: instance-a
  ports:
  - port: 80
    targetPort: sleepypods
"#,
    ));
    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .unwrap();
    let workload = rendered
        .objects
        .iter()
        .find(|object| object.object.kind() == "Deployment")
        .unwrap()
        .to_kubernetes_json();
    assert_eq!(
        workload["spec"]["template"]["spec"]["containers"][1]["ports"][0]["name"],
        "sleepypods"
    );
    let alias = rendered
        .objects
        .iter()
        .find(|object| matches!(object.object, KubernetesObject::Raw(_)))
        .unwrap()
        .to_kubernetes_json();
    assert_eq!(alias["spec"]["ports"][0]["targetPort"], "sleepypods");
}
