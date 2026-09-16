//! Reproductions for the adversarial security review.
//!
//! Each test demonstrates a control-plane weakness that is exploitable by a
//! party the platform is supposed to contain: an untrusted workload, or an
//! instance whose `values` come from a tenant.

use control_plane::ids::WorkloadClassId;
use control_plane::{
    manifest::{
        render_manifests, ContainerPortTemplate, ContainerTemplate, KubernetesObject,
        ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
        PersistentVolumeSourceTemplate, RawKubernetesManifestTemplate, RenderManifestRequest,
        ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart,
        VolumeTemplate, WorkloadKind, WorkloadTemplate,
    },
    Generation, InstanceId, InstanceRecord, InstanceState, InstanceValues, ResolvedSleepPolicy,
    WorkloadClassVersionRef, WorkloadValueFieldRule, WorkloadValueSchema,
};

// --------------------------------------------------------------------------
// F2b (fixed): instance values bind into the parsed document, so a value
// carrying YAML punctuation stays a scalar instead of becoming structure.
// --------------------------------------------------------------------------

#[test]
fn instance_value_cannot_inject_pod_spec_structure_into_a_raw_manifest() {
    // A WorkloadClass author writes what looks like a safe template: the tenant
    // only gets to pick the container image tag.
    let template = template_with_raw_object(TemplateText::from_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: raw-app
spec:
  selector:
    matchLabels:
      app: raw
  template:
    metadata:
      labels:
        app: raw
    spec:
      containers:
        - name: app
          image: "example/app:"#,
        ),
        TemplateTextPart::instance_value("tag"),
        TemplateTextPart::literal(
            r#""
"#,
        ),
    ]));

    // The tenant supplies a tag that closes the string and opens new keys.
    let malicious_tag = r#"1"
          securityContext:
            privileged: true
          volumeMounts:
            - name: host
              mountPath: /host
      hostPID: true
      hostNetwork: true
      serviceAccountName: sleepypods-control-plane
      volumes:
        - name: host
          hostPath:
            path: /
#"#;

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-evil", 1, values([("tag", malicious_tag)])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect("render still accepts the manifest");

    let raw = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Raw(raw) => Some(raw),
            _ => None,
        })
        .expect("raw object was rendered");

    let json = raw.value.clone();
    let pod_spec = &json["spec"]["template"]["spec"];

    // None of the injected keys exist: the structure is the class author's.
    assert!(pod_spec["containers"][0]["securityContext"].is_null());
    assert!(pod_spec["hostPID"].is_null());
    assert!(pod_spec["hostNetwork"].is_null());
    assert!(pod_spec["serviceAccountName"].is_null());
    assert!(pod_spec["volumes"].is_null());

    // The payload is inert: it landed in the scalar the author templated.
    let image = pod_spec["containers"][0]["image"]
        .as_str()
        .expect("image is a string scalar");
    assert!(image.starts_with("example/app:1"));
    assert!(image.contains("privileged: true"));
}

/// The escape hatch still works for what it is for: a class author's structure,
/// with instance values filling the scalars the author chose.
#[test]
fn raw_manifests_still_substitute_instance_values_into_scalars() {
    let template = template_with_raw_object(TemplateText::from_parts([
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
  annotations:
    example.com/owner: "team-"#,
        ),
        TemplateTextPart::instance_value("tenant"),
        TemplateTextPart::literal(
            r#""
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
    ]));

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-a", 1, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect("raw object with instance values renders");

    let raw = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Raw(raw) => Some(raw),
            _ => None,
        })
        .expect("raw object was rendered");

    assert_eq!(raw.metadata.name, "raw-svc-acme");
    assert_eq!(
        raw.value["metadata"]["annotations"]["example.com/owner"],
        "team-acme"
    );
    assert_eq!(raw.value["spec"]["ports"][0]["port"], 8080);
}

// --------------------------------------------------------------------------
// F2b: even with no injection, the renderer performs no pod-security
// validation on raw Deployment/StatefulSet objects.
// --------------------------------------------------------------------------

#[test]
fn raw_workload_manifest_is_applied_with_no_pod_security_validation() {
    let template = template_with_raw_object(TemplateText::literal(
        r#"
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: raw-db
spec:
  serviceName: raw-db
  selector:
    matchLabels:
      app: raw
  template:
    metadata:
      labels:
        app: raw
    spec:
      hostPID: true
      hostIPC: true
      hostNetwork: true
      serviceAccountName: sleepypods-control-plane
      automountServiceAccountToken: true
      containers:
        - name: escape
          image: example/escape:1
          securityContext:
            privileged: true
            allowPrivilegeEscalation: true
            runAsUser: 0
            capabilities:
              add: ["SYS_ADMIN"]
          volumeMounts:
            - name: host
              mountPath: /host
      volumes:
        - name: host
          hostPath:
            path: /
            type: Directory
"#,
    ));

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-evil", 1, values([("tag", "1")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect("render accepts a fully privileged raw workload");

    let raw = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Raw(raw) => Some(raw),
            _ => None,
        })
        .expect("raw object was rendered");

    // to_kubernetes_json is exactly what is sent to the API server.
    let applied = raw.value.clone();
    let pod_spec = &applied["spec"]["template"]["spec"];
    assert_eq!(pod_spec["hostPID"], true);
    assert_eq!(pod_spec["serviceAccountName"], "sleepypods-control-plane");
    assert_eq!(
        pod_spec["containers"][0]["securityContext"]["privileged"],
        true
    );
    assert_eq!(pod_spec["volumes"][0]["hostPath"]["path"], "/");
}

// --------------------------------------------------------------------------
// F6: the rendered (non-raw) pod spec has no hardening at all - no
// automountServiceAccountToken:false, no securityContext, no runtimeClassName.
// --------------------------------------------------------------------------

#[test]
fn rendered_pod_spec_has_no_sandbox_or_service_account_hardening() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &base_template(),
        instance: &instance("tenant-a", 1, values([("tag", "1")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect("deployment renders");

    let deployment = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Deployment(deployment) => Some(deployment),
            _ => None,
        })
        .expect("deployment was rendered");

    let json = KubernetesObject::Deployment(deployment.clone()).to_kubernetes_json();
    let pod_spec = &json["spec"]["template"]["spec"];

    // Only containers and volumes are emitted: the workload keeps the default
    // ServiceAccount token, no runtimeClass, and no seccomp/securityContext.
    assert!(pod_spec["automountServiceAccountToken"].is_null());
    assert!(pod_spec["runtimeClassName"].is_null());
    assert!(pod_spec["securityContext"].is_null());
    assert!(pod_spec["containers"][0]["securityContext"].is_null());
}

// --------------------------------------------------------------------------
// F7: the value schema validates presence only, so any string passes and can
// carry the YAML payload used above.
// --------------------------------------------------------------------------

#[test]
fn value_schema_accepts_yaml_payload_as_a_declared_field() {
    let schema = WorkloadValueSchema::new(false).with_field(
        "tag",
        WorkloadValueFieldRule {
            required: true,
            default: None,
        },
    );

    let validated = schema
        .validate_values(&values([(
            "tag",
            "1\"\n          securityContext:\n            privileged: true\n#",
        )]))
        .expect("schema validation only checks field presence");

    assert!(validated["tag"].contains("privileged: true"));
}

/// Binding after the parse means an instance value is always a string scalar.
/// A class that substitutes into a numeric or boolean field now renders a string
/// there and Kubernetes rejects it, which is the intended trade: a value that can
/// change a field's type can also change what the field means.
#[test]
fn instance_values_bind_as_strings_even_in_numeric_positions() {
    let template = template_with_raw_object(TemplateText::from_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: raw-app
spec:
  replicas: "#,
        ),
        TemplateTextPart::instance_value("replicas"),
        TemplateTextPart::literal(
            r#"
  selector:
    matchLabels:
      app: raw
  template:
    metadata:
      labels:
        app: raw
    spec:
      containers:
        - name: app
          image: example/app:1
"#,
        ),
    ]));

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-a", 1, values([("replicas", "3")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect("render succeeds");

    let raw = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::Raw(raw) => Some(raw),
            _ => None,
        })
        .expect("raw object was rendered");

    assert_eq!(raw.value["spec"]["replicas"], "3");
    assert!(raw.value["spec"]["replicas"].as_u64().is_none());
}

// --------------------------------------------------------------------------
// F2c (fixed): a hostPath must be rooted in a directory the class author fixed,
// and the rendered path may not climb back out of it.
// --------------------------------------------------------------------------

#[test]
fn host_path_fully_chosen_by_an_instance_value_is_rejected() {
    let template = host_path_template(TemplateText::instance_value("data_dir"));

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-evil", 1, values([("data_dir", "/")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect_err("an unrooted hostPath template is rejected");

    assert!(
        format!("{error}").contains("literal absolute directory"),
        "unexpected error: {error}"
    );
}

#[test]
fn host_path_traversal_out_of_the_authors_directory_is_rejected() {
    let template = host_path_template(TemplateText::from_parts([
        TemplateTextPart::literal("/srv/sleepypods/"),
        TemplateTextPart::instance_value("data_dir"),
    ]));

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-evil", 1, values([("data_dir", "../../../")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect_err("traversal out of the fixed directory is rejected");

    assert!(
        format!("{error}").contains("\"..\""),
        "unexpected error: {error}"
    );
}

/// The legitimate shape keeps working: the author fixes the root, the instance
/// selects a subdirectory beneath it. This is what `kind_e2e_stateful` uses.
#[test]
fn host_path_rooted_in_an_author_literal_still_renders_per_instance() {
    let template = host_path_template(TemplateText::from_parts([
        TemplateTextPart::literal("/srv/sleepypods/"),
        TemplateTextPart::instance_value("data_dir"),
    ]));

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-a", 1, values([("data_dir", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(1)),
    })
    .expect("a rooted per-instance hostPath still renders");

    let pv = rendered
        .objects
        .iter()
        .find_map(|object| match &object.object {
            KubernetesObject::PersistentVolume(pv) => Some(pv.clone()),
            _ => None,
        })
        .expect("PersistentVolume was rendered");
    let pv_json = KubernetesObject::PersistentVolume(pv).to_kubernetes_json();
    assert_eq!(pv_json["spec"]["hostPath"]["path"], "/srv/sleepypods/acme");
}

fn host_path_template(path: TemplateText) -> ManifestTemplate {
    ManifestTemplate {
        volumes: vec![VolumeTemplate {
            name: "data".to_owned(),
            mount_path: TemplateText::literal("/mnt/data"),
            pv_name: TemplateText::literal("pv"),
            pvc_name: TemplateText::literal("pvc"),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            capacity: TemplateText::literal("1Gi"),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
            storage_class_name: None,
            source: PersistentVolumeSourceTemplate::HostPath { path, type_: None },
        }],
        ..base_template()
    }
}

// --------------------------------------------------------------------------
// Fixtures
// --------------------------------------------------------------------------

fn base_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: TemplateText::literal("app"),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("example/app:1"),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: Vec::new(),
            },
        },
        service: Some(ServiceTemplate {
            name: TemplateText::literal("svc"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        sidecar: SidecarTemplate {
            name: "sleepypods".to_owned(),
            image: TemplateText::literal("example/sidecar:1"),
            listen_port: 15000,
            mode: None,
        },
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn template_with_raw_object(manifest: TemplateText) -> ManifestTemplate {
    ManifestTemplate {
        raw_objects: vec![RawKubernetesManifestTemplate { manifest }],
        ..base_template()
    }
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

fn sleep_policy() -> ResolvedSleepPolicy {
    ResolvedSleepPolicy {
        idle_timeout_ms: 120_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 30_000,
    }
}

// --------------------------------------------------------------------------
// F3 (fixed): serving an ACME challenge is a proxy read, so the internet-facing
// frontline no longer needs an operator credential.
// --------------------------------------------------------------------------

/// The operator surface keeps the HTTP-01 writes and gives up the read.
#[test]
fn http01_resolve_is_not_on_the_operator_surface() {
    use control_plane::api::OPERATOR_UNARY_METHODS;

    assert!(!OPERATOR_UNARY_METHODS.contains(&"ResolveHttp01Challenge"));

    // The writes stay: inserting and clearing a challenge is ACME-owner work.
    assert!(OPERATOR_UNARY_METHODS.contains(&"PutHttp01Challenge"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"DeleteHttp01Challenge"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"ExpireHttp01Challenges"));
}
