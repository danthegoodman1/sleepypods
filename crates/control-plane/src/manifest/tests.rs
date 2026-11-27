use std::collections::BTreeMap;

use k8s_openapi::{
    api::core::v1::{
        CSIPersistentVolumeSource, HostPathVolumeSource, PersistentVolume, SecretReference,
    },
    Resource,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use super::{
    render_manifests, render_manifests_with_options, AppProtocol, ApplyOrder,
    ContainerPortTemplate, ContainerTemplate, CsiSecretRefTemplate, EnvVarTemplate,
    KubernetesObject, ManifestRenderError, ManifestTemplate, PersistentVolumeAccessMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, RawKubernetesManifestTemplate,
    RenderManifestOptions, RenderManifestRequest, RenderedManifest, ServicePortTemplate,
    ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart, VolumeTemplate, WorkloadKind,
    WorkloadTemplate, INSTANCE_SUFFIX_VALUE, LABEL_INSTANCE_GENERATION, LABEL_INSTANCE_ID,
    LABEL_WORKLOAD_CLASS_ID, LABEL_WORKLOAD_CLASS_VERSION, LABEL_WORKLOAD_NAME,
};
use crate::materializer::rendered_object_refs;
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
    })
    .expect("deployment renders");

    assert_eq!(rendered.objects.len(), 2);
    assert_eq!(rendered.objects[0].apply_order, ApplyOrder::Service);
    assert_eq!(rendered.objects[1].apply_order, ApplyOrder::Workload);

    let service = rendered.objects[0].object.to_kubernetes_json();
    assert_eq!(service["kind"], "Service");
    assert_eq!(service["metadata"]["name"], "svc-acme-69856ec0faae6daf");
    assert_eq!(service["spec"]["selector"][LABEL_INSTANCE_ID], "instance-a");
    assert_eq!(service["spec"]["ports"][0]["port"], 80);
    assert_eq!(service["spec"]["ports"][0]["targetPort"], 15000);

    let deployment = rendered.objects[1].object.to_kubernetes_json();
    assert_eq!(deployment["kind"], "Deployment");
    let labels = &deployment["metadata"]["labels"];
    assert_eq!(deployment["metadata"]["name"], "app-acme-69856ec0faae6daf");
    assert_eq!(labels[LABEL_INSTANCE_ID], "instance-a");
    assert_eq!(labels[LABEL_INSTANCE_GENERATION], "7");
    assert_eq!(labels[LABEL_WORKLOAD_CLASS_ID], "web");
    assert_eq!(labels[LABEL_WORKLOAD_CLASS_VERSION], "1");
    assert_eq!(deployment["spec"]["replicas"], 1);
    assert_eq!(
        deployment["spec"]["selector"]["matchLabels"], service["spec"]["selector"],
        "service and workload selectors should target the same pods"
    );
    let pod = &deployment["spec"]["template"];
    assert_eq!(pod["metadata"]["labels"][LABEL_INSTANCE_ID], "instance-a");
    let containers = &pod["spec"]["containers"];
    assert_eq!(containers.as_array().map(Vec::len), Some(2));
    assert_eq!(containers[0]["image"], "example/app:1");
    assert_eq!(containers[0]["ports"][0]["containerPort"], 8080);
    assert_eq!(containers[1]["name"], "sleepypods-sidecar");
    assert_eq!(containers[1]["image"], "sleepypods/sidecar:test");
    assert_eq!(containers[1]["ports"][0]["containerPort"], 15000);
    let env = &containers[1]["env"];
    for (name, expected) in [
        ("SLEEPYPODS_LISTEN_PORT", "15000"),
        ("SLEEPYPODS_SIDECAR_LISTEN_ADDR", "0.0.0.0:15000"),
        ("SLEEPYPODS_APP_PORT", "8080"),
        ("SLEEPYPODS_INSTANCE_ID", "instance-a"),
        ("SLEEPYPODS_INSTANCE_GENERATION", "7"),
        (
            "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
            "http://sleepypods-control-plane.apps.svc.cluster.local:50051",
        ),
        ("SLEEPYPODS_IDLE_TIMEOUT_MS", "120000"),
        ("SLEEPYPODS_IDLE_RETRY_BACKOFF_MS", "5000"),
        ("SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS", "30000"),
    ] {
        assert_env(env, name, expected);
    }
    assert_env_absent(env, "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN");
    assert_eq!(pod["spec"]["volumes"], json!([]));
    assert_eq!(containers[0]["volumeMounts"], json!([]));
    assert_eq!(containers[1]["volumeMounts"], json!([]));
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
        },
        RenderManifestOptions {
            sidecar_control_plane_token: Some(&token),
            sidecar_control_plane_endpoint: Some("https://cp.platform.example:50051"),
            sidecar_control_plane_ca: Some("PUBLIC-CA-ONLY"),
        },
    )
    .expect("deployment renders");

    let secret = json_of(&rendered, "Secret");
    assert_eq!(
        secret["metadata"]["name"],
        "sleepypods-sidecar-token-69856ec0faae6daf"
    );
    assert_eq!(secret["metadata"]["namespace"], "apps");
    assert_eq!(secret["stringData"]["token"], "sidecar-secret");

    let deployment = json_of(&rendered, "Deployment");
    let env = sidecar_env(&deployment);
    assert_env(
        env,
        "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
        "https://cp.platform.example:50051",
    );
    assert_env(
        env,
        sleepypods_api::transport::CONTROL_PLANE_TLS_CA_PEM_ENV,
        "PUBLIC-CA-ONLY",
    );
    assert!(!env.as_array().expect("env is a list").iter().any(|var| {
        let name = var["name"].as_str().expect("env name");
        name.contains("SEALING") || name.contains("TLS_KEY")
    }));
    let token_env = env_var(env, "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN");
    assert_eq!(
        token_env["valueFrom"],
        json!({
            "secretKeyRef": {
                "name": "sleepypods-sidecar-token-69856ec0faae6daf",
                "key": "token",
            },
        })
    );
    assert!(token_env.get("value").is_none());
}

#[test]
fn rendered_names_use_hash_suffix_for_short_instance_id() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("abc", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect("deployment renders");

    assert_eq!(
        rendered.objects[0].object.name(),
        "svc-acme-ba7816bf8f01cfea"
    );
    assert_eq!(
        rendered.objects[1].object.name(),
        "app-acme-ba7816bf8f01cfea"
    );
}

#[test]
fn rendered_names_hash_full_long_instance_id() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("abcdefghijk", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect("deployment renders");

    assert_eq!(
        rendered.objects[0].object.name(),
        "svc-acme-ca2f2069ea0c6e46"
    );
    assert_eq!(
        rendered.objects[1].object.name(),
        "app-acme-ca2f2069ea0c6e46"
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
    })
    .expect("deployment renders");

    let service_name = rendered.objects[0].object.name();
    let workload_name = rendered.objects[1].object.name();
    assert_eq!(service_name.len(), 63);
    assert_eq!(workload_name.len(), 63);
    assert!(service_name.ends_with("-69856ec0faae6daf"));
    assert!(workload_name.ends_with("-69856ec0faae6daf"));
    assert_eq!(&service_name[..46], "s".repeat(46));
    assert_eq!(&workload_name[..46], "w".repeat(46));
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
    })
    .expect("deployment renders");

    assert_eq!(rendered.objects[0].object.name(), "shared-69856ec0faae6daf");
    assert_eq!(rendered.objects[1].object.name(), "shared-69856ec0faae6daf");
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
    })
    .expect_err("invalid custom name is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidName {
            field: "workload.name",
            value: "App-69856ec0faae6daf".to_owned(),
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
    })
    .expect("deployment renders");
    let service = json_of(&rendered, "Service");
    let deployment = json_of(&rendered, "Deployment");

    assert_eq!(
        service["metadata"]["annotations"]["sleepypods.io/backend-scheme"],
        "tcp"
    );
    assert_env(sidecar_env(&deployment), "SLEEPYPODS_SIDECAR_MODE", "tcp");
}

#[test]
fn sidecar_carries_the_declared_app_protocol() {
    let sidecar_env = |mode: Option<&str>, app_protocol: AppProtocol| {
        let mut template = deployment_template();
        template.sidecar.mode = mode.map(str::to_owned);
        template.sidecar.app_protocol = app_protocol;
        render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
        })
        .map(|rendered| sidecar_env(&json_of(&rendered, "Deployment")).clone())
    };

    for (mode, app_protocol, expected) in [
        (None, AppProtocol::Http1, "http1"),
        (Some("http"), AppProtocol::Http1, "http1"),
        (None, AppProtocol::H2c, "h2c"),
        (Some("http"), AppProtocol::H2c, "h2c"),
    ] {
        let env = sidecar_env(mode, app_protocol).expect("http mode renders an app protocol");
        assert_env(&env, "SLEEPYPODS_APP_PROTOCOL", expected);
    }

    let env = sidecar_env(Some("tcp"), AppProtocol::Http1).expect("tcp mode renders");
    assert_env_absent(&env, "SLEEPYPODS_APP_PROTOCOL");
    assert_invalid_field(
        sidecar_env(Some("tcp"), AppProtocol::H2c)
            .expect_err("tcp mode has no application protocol to choose"),
        "sidecar.app_protocol",
        "http mode",
    );
}

#[test]
fn pod_carries_an_explicit_termination_grace_period() {
    let render = |seconds: Option<u32>| {
        let mut template = deployment_template();
        template.workload.termination_grace_period_seconds = seconds;
        render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
        })
        .map(|rendered| {
            json_values(&rendered)[1]["spec"]["template"]["spec"]["terminationGracePeriodSeconds"]
                .clone()
        })
    };

    assert_eq!(render(None).expect("default renders"), json!(30));
    assert_eq!(render(Some(0)).expect("zero renders"), json!(0));
    assert_eq!(render(Some(300)).expect("the maximum renders"), json!(300));
    assert_invalid_field(
        render(Some(301)).expect_err("a grace period over the maximum is rejected"),
        "workload.termination_grace_period_seconds",
        "exceeds the maximum of 300",
    );
}

#[test]
fn serializes_deployment_and_service_as_kubernetes_json() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect("deployment renders");

    let objects = json_values(&rendered);
    let service = &objects[0];
    assert_eq!(service["apiVersion"], json!("v1"));
    assert_eq!(service["kind"], json!("Service"));
    assert_eq!(
        service["metadata"]["name"],
        json!("svc-acme-69856ec0faae6daf")
    );
    assert_eq!(service["metadata"]["namespace"], json!("apps"));
    assert_eq!(
        service["metadata"]["labels"][LABEL_INSTANCE_ID],
        json!("instance-a")
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
    assert_eq!(
        deployment["metadata"]["name"],
        json!("app-acme-69856ec0faae6daf")
    );
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

    let pv = rendered.objects[0].object.to_kubernetes_json();
    assert_eq!(pv["metadata"]["name"], "pv-acme-2e1ac556311afdd5");
    assert_eq!(pv["metadata"]["labels"][LABEL_INSTANCE_ID], "postgres-a");
    assert_eq!(pv["spec"]["accessModes"], json!(["ReadWriteOnce"]));
    assert_eq!(pv["spec"]["capacity"]["storage"], "10Gi");
    assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Retain");
    assert_eq!(pv["spec"]["storageClassName"], "manual");
    assert_eq!(
        pv["spec"]["claimRef"],
        json!({"namespace": "data", "name": "pvc-acme-2e1ac556311afdd5"})
    );
    assert_eq!(
        persistent_volume(&rendered).csi,
        Some(CSIPersistentVolumeSource {
            driver: "csi.example.com".to_owned(),
            volume_handle: "provider-vol-123".to_owned(),
            fs_type: Some("ext4".to_owned()),
            read_only: Some(false),
            volume_attributes: Some(BTreeMap::from([("tenant".to_owned(), "acme".to_owned())])),
            ..CSIPersistentVolumeSource::default()
        })
    );

    let pvc = rendered.objects[1].object.to_kubernetes_json();
    assert_eq!(pvc["kind"], "PersistentVolumeClaim");
    assert_eq!(pvc["metadata"]["name"], "pvc-acme-2e1ac556311afdd5");
    assert_eq!(pvc["metadata"]["namespace"], "data");
    assert_eq!(pvc["spec"]["volumeName"], "pv-acme-2e1ac556311afdd5");
    assert_eq!(pvc["spec"]["resources"]["requests"]["storage"], "10Gi");

    let stateful_set = rendered.objects[3].object.to_kubernetes_json();
    assert_eq!(stateful_set["kind"], "StatefulSet");
    assert_eq!(stateful_set["metadata"]["name"], "db-acme-2e1ac556311afdd5");
    assert_eq!(
        stateful_set["spec"]["serviceName"],
        "db-acme-2e1ac556311afdd5"
    );
    assert_eq!(stateful_set["spec"]["replicas"], 1);
    let pod = &stateful_set["spec"]["template"]["spec"];
    let containers = &pod["containers"];
    assert_eq!(containers.as_array().map(Vec::len), Some(2));
    assert_eq!(containers[0]["ports"][0]["containerPort"], 5432);
    assert_eq!(containers[1]["name"], "sleepypods-sidecar");
    assert_eq!(containers[1]["ports"][0]["containerPort"], 15000);
    assert_env(sidecar_env(&stateful_set), "SLEEPYPODS_APP_PORT", "5432");
    assert_eq!(
        pod["volumes"][0]["persistentVolumeClaim"]["claimName"],
        "pvc-acme-2e1ac556311afdd5"
    );
    assert_eq!(
        containers[0]["volumeMounts"],
        json!([{"name": "data", "mountPath": "/var/lib/postgresql/data"}])
    );
    assert_eq!(containers[1]["volumeMounts"], json!([]));
}

#[test]
fn renders_valid_raw_pv_pvc_and_service() {
    let mut template = deployment_template();
    template.raw_objects = vec![
        raw_manifest(
            r#"
apiVersion: v1
kind: PersistentVolume
metadata:
  name: raw-pv-acme-{suffix}
spec:
  capacity:
    storage: 1Gi
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    namespace: apps
    name: raw-pvc-acme-{suffix}
  hostPath:
    path: /tmp/raw-acme
"#,
        ),
        raw_manifest(
            r#"
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: raw-pvc-acme-{suffix}
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
  volumeName: raw-pv-acme-{suffix}
"#,
        ),
        raw_manifest_parts([
            TemplateTextPart::literal(
                r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc-"#,
            ),
            TemplateTextPart::instance_value("tenant"),
            TemplateTextPart::literal(
                r#"-{suffix}
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
    ];

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect("raw objects render");

    assert_eq!(
        rendered
            .objects
            .iter()
            .map(|object| (object.apply_order, object.object.object_ref()))
            .collect::<Vec<_>>(),
        vec![
            (
                ApplyOrder::PersistentVolume,
                object_ref("v1", "PersistentVolume", "", "raw-pv-acme-69856ec0faae6daf")
            ),
            (
                ApplyOrder::PersistentVolumeClaim,
                object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "apps",
                    "raw-pvc-acme-69856ec0faae6daf"
                )
            ),
            (
                ApplyOrder::Service,
                object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf")
            ),
            (
                ApplyOrder::Service,
                object_ref("v1", "Service", "apps", "raw-svc-acme-69856ec0faae6daf")
            ),
            (
                ApplyOrder::Workload,
                object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf")
            ),
        ]
    );

    let raw_service = rendered
        .objects
        .iter()
        .find(|object| object.object.name() == "raw-svc-acme-69856ec0faae6daf")
        .expect("raw service rendered")
        .object
        .to_kubernetes_json();
    let materialization = crate::materialization::MaterializationRecord {
        pod_backend: None,
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
        ["svc-acme-69856ec0faae6daf", "raw-svc-acme-69856ec0faae6daf"],
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
}

#[test]
fn rejects_raw_workload_controllers() {
    for kind in ["Deployment", "StatefulSet"] {
        let mut template = deployment_template();
        template.raw_objects = vec![raw_manifest(&format!(
            r#"
apiVersion: apps/v1
kind: {kind}
metadata:
  name: aux-{{suffix}}
spec:
  serviceName: aux
  selector:
    matchLabels:
      app: aux
  template:
    metadata:
      labels:
        app: aux
    spec:
      containers:
        - name: aux
          image: example/aux:1
"#
        ))];

        let error = render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
        })
        .expect_err("a raw workload controller is rejected");

        assert_invalid_field(error, "raw_objects.manifest.kind", "template.workload");
    }
}

/// Each raw object is read as its Kubernetes kind, so a field of the wrong type
/// fails at render and the API server receives only fields the kind defines.
#[test]
fn raw_objects_follow_their_kubernetes_schema() {
    let render = |spec: &str| {
        let mut template = deployment_template();
        template.raw_objects = vec![raw_manifest(&format!(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: aux-{{suffix}}\nspec:\n{spec}"
        ))];
        render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
        })
    };

    assert_invalid_field(
        render("  ports:\n    - port: eighty\n").expect_err("a port is a number"),
        "raw_objects.manifest",
        "does not match its Kubernetes schema",
    );
    let rendered =
        render("  ports:\n    - port: 80\n  clusterIp: None\n").expect("an unknown field renders");
    let service = rendered
        .objects
        .iter()
        .find(|object| object.object.name() == "aux-69856ec0faae6daf")
        .expect("raw Service renders")
        .object
        .to_kubernetes_json();
    assert_eq!(service["spec"], json!({"ports": [{"port": 80}]}));
}

#[test]
fn rejects_raw_manifest_with_disallowed_kind() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: batch/v1
kind: Job
metadata:
  name: raw-job-{suffix}
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
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
  name: raw-svc-{suffix}
  namespace: 123
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
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
  name: raw-pv-{suffix}
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
  name: svc-acme-69856ec0faae6daf
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect_err("duplicate ref is rejected");

    assert_invalid_field(
        error,
        "template",
        "duplicate rendered Kubernetes object ref v1 Service apps/svc-acme-69856ec0faae6daf",
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
  name: raw-svc-{suffix}
  labels:
    sleepypods.io/instance-id: other-instance
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
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
  name: raw-svc-{suffix}
  labels:
    sleepypods.io/instance-id: 7
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect_err("non-string ownership label is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.labels",
        "sleepypods.io/instance-id must be \"instance-a\", got non-string value",
    );
}

#[test]
fn rejects_non_string_raw_annotations() {
    let mut template = deployment_template();
    template.raw_objects = vec![raw_manifest(
        r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc-{suffix}
  annotations:
    example.com/revision: [3]
"#,
    )];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect_err("non-string annotation is rejected");

    assert_invalid_field(
        error,
        "raw_objects.manifest.metadata.annotations",
        "all values must be strings",
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
            r#"-{suffix}
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
  name: raw-pv-{suffix}
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
            r#"-{suffix}
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
    })
    .expect("a templated raw PV declaring Retain renders");

    assert_eq!(
        rendered_kind(&rendered, "PersistentVolume").name(),
        "raw-pv-acme-69856ec0faae6daf"
    );
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
  name: raw-svc-{suffix}
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
    })
    .expect("raw object renders");

    let raw = json_named(&rendered, "raw-svc-69856ec0faae6daf");
    let annotations = &raw["metadata"]["annotations"];
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
  name: raw-svc-{suffix}
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
    })
    .expect("raw object renders");

    let raw = json_named(&rendered, "raw-svc-69856ec0faae6daf");
    let annotations = &raw["metadata"]["annotations"];
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
  name: raw-svc-{suffix}
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
    })
    .expect("raw object renders");

    assert_eq!(
        json_named(&rendered, "raw-svc-69856ec0faae6daf")["metadata"]["annotations"]
            ["example.com/acme"],
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
    })
    .expect_err("a hostPath root chosen by an instance is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: "hostPath path \"/\" must start with a literal absolute directory ending \
                      in \"/\", so that an instance value chooses a subdirectory of it"
                .to_owned(),
        }
    );
}

#[test]
fn rejects_host_path_whose_literal_prefix_leaves_its_last_segment_open() {
    // Without the closing slash the value extends the author's directory name
    // into a sibling directory.
    for (prefix, value) in [
        ("/var/local/sleepypods", "-backup/other-tenant"),
        ("/", "etc"),
    ] {
        let mut template = host_path_stateful_template();
        template.volumes[0].source = PersistentVolumeSourceTemplate::HostPath {
            path: TemplateText::from_parts([
                TemplateTextPart::literal(prefix),
                TemplateTextPart::instance_value("data_dir"),
            ]),
            type_: None,
        };

        let error = render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance(
                "postgres-a",
                2,
                values([("tenant", "acme"), ("data_dir", value)]),
            ),
            sleep_policy: sleep_policy(),
            namespace: "data",
        })
        .expect_err("an instance value may not extend the author's directory name");

        assert!(matches!(
            error,
            ManifestRenderError::InvalidField {
                field: "volume.source.host_path.path",
                ..
            }
        ));
    }
}

#[test]
fn raw_object_names_end_with_the_instance_suffix() {
    let render = |name: &str| {
        let mut template = deployment_template();
        template.raw_objects = vec![raw_manifest(&format!(
            "apiVersion: v1\nkind: Secret\nmetadata:\n  name: {name}\n"
        ))];
        render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
        })
    };

    let rendered = render("settings-{suffix}").expect("a suffixed raw name renders");
    assert_eq!(
        rendered_kind(&rendered, "Secret").name(),
        "settings-69856ec0faae6daf"
    );
    for name in ["settings", "settings-acme", "settings{suffix}", "{suffix}"] {
        assert_invalid_field(
            render(name).expect_err("a raw name needs the instance suffix"),
            "raw_objects.manifest.metadata.name",
            "instance suffix",
        );
    }
}

#[test]
fn stateful_set_and_service_names_follow_their_kubernetes_limits() {
    let render = |kind: WorkloadKind, workload: &str, service: &str| {
        let mut template = stateful_template();
        template.volumes.clear();
        template.workload.kind = kind;
        template.workload.name = TemplateText::literal(workload);
        template.service.as_mut().expect("service").name = TemplateText::literal(service);
        render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
            sleep_policy: sleep_policy(),
            namespace: "data",
        })
    };
    // The suffix takes 17 of a StatefulSet name's 52 characters.
    let fits = "d".repeat(35);
    let overlong = "d".repeat(36);

    render(WorkloadKind::StatefulSet, &fits, "db").expect("a 52-character name renders");
    assert_invalid_field(
        render(WorkloadKind::StatefulSet, &overlong, "db")
            .expect_err("a StatefulSet name over 52 characters is rejected"),
        "workload.name",
        "52 characters",
    );
    render(WorkloadKind::Deployment, &overlong, "db")
        .expect("a Deployment keeps the full DNS label length");
    assert_invalid_field(
        render(WorkloadKind::Deployment, "db", "9db")
            .expect_err("a Service name starts with a letter"),
        "service.name",
        "must start with a letter",
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
    })
    .expect("a subdirectory whose name begins with dots still renders");

    assert_eq!(
        persistent_volume(&rendered).host_path,
        Some(HostPathVolumeSource {
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
  name: raw-svc-{suffix}
"#,
    )];

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect("raw object renders");

    let refs = rendered_object_refs(&rendered).expect("refs derive before apply");

    assert_eq!(
        refs,
        vec![
            object_ref("v1", "Service", "apps", "svc-acme-69856ec0faae6daf"),
            object_ref("v1", "Service", "apps", "raw-svc-69856ec0faae6daf"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-69856ec0faae6daf"),
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
    })
    .expect("Archil-style CSI workload renders");

    let pv = persistent_volume(&rendered);
    assert_eq!(
        pv.persistent_volume_reclaim_policy.as_deref(),
        Some("Retain")
    );
    let claim = pv.claim_ref.as_ref().expect("the PV names its claim");
    assert_eq!(claim.namespace.as_deref(), Some("data"));
    assert_eq!(claim.name.as_deref(), Some("pvc-acme-2e1ac556311afdd5"));
    assert_eq!(
        pv.csi,
        Some(CSIPersistentVolumeSource {
            driver: "csi.archil.com".to_owned(),
            volume_handle: "archil-volume-123".to_owned(),
            read_only: Some(false),
            volume_attributes: Some(BTreeMap::from([(
                "region".to_owned(),
                "us-west-2".to_owned()
            )])),
            node_publish_secret_ref: Some(SecretReference {
                name: Some("archil-node-secret".to_owned()),
                namespace: Some("storage-secrets".to_owned()),
            }),
            ..CSIPersistentVolumeSource::default()
        })
    );

    let objects = json_values(&rendered);
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
    assert_eq!(
        objects[1]["spec"]["volumeName"],
        json!("pv-acme-2e1ac556311afdd5")
    );
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
    })
    .expect("CSI workload with all secret refs renders");

    let csi = persistent_volume(&rendered)
        .csi
        .expect("expected CSI source");

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
    let objects = json_values(&rendered);
    let csi_json = &objects[0]["spec"]["csi"];

    for (key, name, rendered_ref) in expected {
        assert_eq!(
            rendered_ref.map(|ref_| (ref_.name.as_deref(), ref_.namespace.as_deref())),
            Some((Some(name), Some("storage-secrets"))),
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
    })
    .expect("stateful workload renders");

    let objects = json_values(&rendered);
    let pv = &objects[0];
    assert_eq!(pv["apiVersion"], json!("v1"));
    assert_eq!(pv["kind"], json!("PersistentVolume"));
    assert_eq!(pv["metadata"]["name"], json!("pv-acme-2e1ac556311afdd5"));
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
                "name": "pvc-acme-2e1ac556311afdd5",
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
    assert_eq!(pvc["metadata"]["name"], json!("pvc-acme-2e1ac556311afdd5"));
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
            "volumeName": "pv-acme-2e1ac556311afdd5",
        })
    );

    let service = &objects[2];
    assert_eq!(service["kind"], json!("Service"));
    assert_eq!(service["spec"]["ports"][0]["targetPort"], json!(15000));

    let stateful_set = &objects[3];
    assert_eq!(stateful_set["apiVersion"], json!("apps/v1"));
    assert_eq!(stateful_set["kind"], json!("StatefulSet"));
    assert_eq!(
        stateful_set["metadata"]["name"],
        json!("db-acme-2e1ac556311afdd5")
    );
    assert_eq!(
        stateful_set["spec"]["serviceName"],
        json!("db-acme-2e1ac556311afdd5")
    );
    assert_eq!(stateful_set["spec"]["replicas"], json!(1));
    assert_eq!(
        stateful_set["spec"]["template"]["spec"]["volumes"][0],
        json!({
            "name": "data",
            "persistentVolumeClaim": {
                "claimName": "pvc-acme-2e1ac556311afdd5",
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
    })
    .expect("hostPath stateful workload renders");

    assert_eq!(
        persistent_volume(&rendered).host_path,
        Some(HostPathVolumeSource {
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
    })
    .expect("hostPath stateful workload renders");

    let pv = &json_values(&rendered)[0];

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
        template: template.clone(),
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
            termination_grace_period_seconds: None,
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
            termination_grace_period_seconds: None,
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
        app_protocol: Default::default(),
    }
}

fn sleep_policy() -> ResolvedSleepPolicy {
    ResolvedSleepPolicy {
        idle_timeout_ms: 120_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 30_000,
    }
}

fn json_values(rendered: &RenderedManifest) -> Vec<Value> {
    rendered
        .objects
        .iter()
        .map(|rendered| rendered.object.to_kubernetes_json())
        .collect()
}

/// The first rendered object of `kind`.
fn rendered_kind<'a>(rendered: &'a RenderedManifest, kind: &str) -> &'a KubernetesObject {
    &rendered
        .objects
        .iter()
        .find(|rendered| rendered.object.kind() == kind)
        .unwrap_or_else(|| panic!("a {kind} renders"))
        .object
}

fn json_of(rendered: &RenderedManifest, kind: &str) -> Value {
    rendered_kind(rendered, kind).to_kubernetes_json()
}

fn json_named(rendered: &RenderedManifest, name: &str) -> Value {
    rendered
        .objects
        .iter()
        .find(|rendered| rendered.object.name() == name)
        .unwrap_or_else(|| panic!("{name} renders"))
        .object
        .to_kubernetes_json()
}

/// The first rendered object of kind `K`, read back as its `k8s-openapi` type.
fn typed<K: Resource + DeserializeOwned>(rendered: &RenderedManifest) -> K {
    serde_json::from_value(json_of(rendered, K::KIND)).expect("rendered JSON fits its kind")
}

fn persistent_volume(
    rendered: &RenderedManifest,
) -> k8s_openapi::api::core::v1::PersistentVolumeSpec {
    typed::<PersistentVolume>(rendered)
        .spec
        .expect("a PersistentVolume has a spec")
}

/// The environment of the sidecar, the second container of a workload's Pod.
fn sidecar_env(workload: &Value) -> &Value {
    &workload["spec"]["template"]["spec"]["containers"][1]["env"]
}

fn env_var<'a>(env: &'a Value, name: &str) -> &'a Value {
    env.as_array()
        .expect("env is a list")
        .iter()
        .find(|var| var["name"] == name)
        .unwrap_or_else(|| panic!("missing env var {name}"))
}

fn assert_env(env: &Value, name: &str, expected: &str) {
    assert_eq!(env_var(env, name)["value"], expected);
}

fn assert_env_absent(env: &Value, name: &str) {
    assert!(
        env.as_array()
            .expect("env is a list")
            .iter()
            .all(|var| var["name"] != name),
        "env var {name} should be absent"
    );
}

fn composed(prefix: &str, field: &str) -> TemplateText {
    TemplateText::from_parts([
        TemplateTextPart::literal(prefix),
        TemplateTextPart::instance_value(field),
    ])
}

fn raw_manifest(manifest: &str) -> RawKubernetesManifestTemplate {
    raw_manifest_parts([TemplateTextPart::literal(manifest)])
}

/// `{suffix}` in a literal part reads the platform's instance suffix value.
fn raw_manifest_parts(parts: impl Into<Vec<TemplateTextPart>>) -> RawKubernetesManifestTemplate {
    let mut expanded = Vec::new();
    for part in parts.into() {
        let TemplateTextPart::Literal(literal) = part else {
            expanded.push(part);
            continue;
        };
        let mut pieces = literal.split("{suffix}");
        expanded.push(TemplateTextPart::literal(pieces.next().unwrap_or_default()));
        for piece in pieces {
            expanded.push(TemplateTextPart::instance_value(INSTANCE_SUFFIX_VALUE));
            expanded.push(TemplateTextPart::literal(piece));
        }
    }
    RawKubernetesManifestTemplate {
        manifest: TemplateText::from_parts(expanded),
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
        })
        .unwrap();
        let workload = json_of(&rendered, "Deployment");
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
        let service = json_of(&rendered, "Service");
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
        })
    };
    let rendered = render(&template).unwrap();
    let workload = json_of(&rendered, "Deployment");
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
    let workload = json_of(&empty_metrics, "Deployment");
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
fn raw_auxiliaries_cannot_supply_reserved_primary_selector_labels_or_expressions() {
    let service = json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"auxiliary","labels":{}},"spec":{"selector":{}}});
    let claim = json!({"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"auxiliary"},"spec":{"volumeName":"auxiliary","selector":{"matchLabels":{},"matchExpressions":[]}}});
    for (mut raw, pointer) in [
        (service.clone(), "/metadata/labels"),
        (service, "/spec/selector"),
        (claim.clone(), "/spec/selector/matchLabels"),
        (claim, "/spec/selector/matchExpressions"),
    ] {
        if pointer.ends_with("matchExpressions") {
            *raw.pointer_mut(pointer).unwrap() =
                json!([{"key":LABEL_WORKLOAD_NAME,"operator":"Exists"}]);
        } else {
            raw.pointer_mut(pointer).unwrap()[LABEL_WORKLOAD_NAME] =
                json!("app-acme-69856ec0faae6daf");
        }
        let mut template = deployment_template();
        template.raw_objects = vec![raw_manifest(&raw.to_string())];
        assert!(matches!(
            render_manifests(RenderManifestRequest {
                template: &template,
                instance: &instance("instance-a", 7, values([("tenant", "acme")])),
                sleep_policy: sleep_policy(),
                namespace: "apps",
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
  name: primary-alias-{suffix}
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
    })
    .unwrap();
    let workload = json_of(&rendered, "Deployment");
    assert_eq!(
        workload["spec"]["template"]["spec"]["containers"][1]["ports"][0]["name"],
        "sleepypods"
    );
    let alias = json_named(&rendered, "primary-alias-69856ec0faae6daf");
    assert_eq!(alias["spec"]["ports"][0]["targetPort"], "sleepypods");
}
