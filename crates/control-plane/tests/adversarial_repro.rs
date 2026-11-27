//! Reproductions for the adversarial security review.
//!
//! Each test demonstrates a control-plane weakness that is exploitable by a
//! party the platform is supposed to contain: an untrusted workload, or an
//! instance whose `values` come from a tenant.

use control_plane::ids::WorkloadClassId;
use control_plane::{
    manifest::{
        render_manifests, ContainerPortTemplate, ContainerTemplate, ManifestTemplate,
        PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate,
        RawKubernetesManifestTemplate, RenderManifestRequest, ServicePortTemplate, ServiceTemplate,
        SidecarTemplate, TemplateText, TemplateTextPart, VolumeTemplate, WorkloadKind,
        WorkloadTemplate,
    },
    Generation, InstanceId, InstanceRecord, InstanceState, InstanceValues, RenderedManifest,
    ResolvedSleepPolicy, WorkloadClassVersionRef, WorkloadValueFieldRule, WorkloadValueSchema,
};
use serde_json::Value;

// --------------------------------------------------------------------------
// F2b (fixed): instance values bind into the parsed document, so a value
// carrying YAML punctuation stays a scalar instead of becoming structure.
// --------------------------------------------------------------------------

#[test]
fn instance_value_cannot_inject_structure_into_a_raw_manifest() {
    // A WorkloadClass author writes what looks like a safe template: the tenant
    // only gets to pick the owning team.
    let template = template_with_raw_object(TemplateText::from_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc-{suffix}
  annotations:
    example.com/owner: "team-"#,
        ),
        TemplateTextPart::instance_value("team"),
        TemplateTextPart::literal(
            r#""
spec:
  selector:
    app: raw
  ports:
    - port: 8080
"#,
        ),
    ]));

    // The tenant supplies a team that closes the string and opens new keys.
    let malicious_team = r#"a"
    example.com/injected: "yes"
spec:
  type: LoadBalancer
  externalIPs:
    - 203.0.113.1
#"#;

    let rendered = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-evil", 1, values([("team", malicious_team)])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect("render still accepts the manifest");

    let json = rendered_named(&rendered, "raw-svc-");

    // None of the injected keys exist: the structure is the class author's.
    assert!(json["spec"]["type"].is_null());
    assert!(json["spec"]["externalIPs"].is_null());
    assert!(json["metadata"]["annotations"]["example.com/injected"].is_null());

    // The payload is inert: it landed in the scalar the author templated.
    let owner = json["metadata"]["annotations"]["example.com/owner"]
        .as_str()
        .expect("owner is a string scalar");
    assert!(owner.starts_with("team-a"));
    assert!(owner.contains("LoadBalancer"));
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
            r#"-{suffix}
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
    })
    .expect("raw object with instance values renders");

    let raw = rendered_named(&rendered, "raw-svc-");

    assert_eq!(raw["metadata"]["name"], "raw-svc-acme-80a707af7dc77ee1");
    assert_eq!(
        raw["metadata"]["annotations"]["example.com/owner"],
        "team-acme"
    );
    assert_eq!(raw["spec"]["ports"][0]["port"], 8080);
}

// --------------------------------------------------------------------------
// F2b (fixed): a raw object cannot be a Deployment or StatefulSet, so every Pod
// the platform starts comes from the structured, hardened workload.
// --------------------------------------------------------------------------

#[test]
fn raw_workload_manifest_is_rejected() {
    for kind in ["Deployment", "StatefulSet"] {
        let template = template_with_raw_object(TemplateText::literal(format!(
            r#"
apiVersion: apps/v1
kind: {kind}
metadata:
  name: raw-db-{{suffix}}
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
      hostNetwork: true
      serviceAccountName: sleepypods-control-plane
      automountServiceAccountToken: true
      containers:
        - name: escape
          image: example/escape:1
          securityContext:
            privileged: true
"#
        )));

        let error = render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance("tenant-evil", 1, values([("tag", "1")])),
            sleep_policy: sleep_policy(),
            namespace: "apps",
        })
        .expect_err("a raw workload controller is rejected");

        assert!(
            format!("{error}").contains("template.workload"),
            "unexpected error: {error}"
        );
    }
}

// --------------------------------------------------------------------------
// F6 (fixed): a rendered pod drops the ServiceAccount token it has no use for,
// runs under the runtime's default seccomp profile, and gains no privileges.
// --------------------------------------------------------------------------

#[test]
fn rendered_pod_spec_drops_its_service_account_token_and_sandboxes_the_workload() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &base_template(),
        instance: &instance("tenant-a", 1, values([("tag", "1")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect("deployment renders");

    let json = rendered
        .objects
        .iter()
        .find(|object| object.object.kind() == "Deployment")
        .expect("deployment was rendered")
        .object
        .to_kubernetes_json();
    let pod_spec = &json["spec"]["template"]["spec"];

    assert_eq!(pod_spec["automountServiceAccountToken"], false);
    assert_eq!(
        pod_spec["securityContext"]["seccompProfile"]["type"],
        "RuntimeDefault"
    );
    for container in pod_spec["containers"]
        .as_array()
        .expect("containers are an array")
    {
        assert_eq!(
            container["securityContext"]["allowPrivilegeEscalation"],
            false
        );
    }
}

// --------------------------------------------------------------------------
// F7 (fixed): an instance value stays printable text, so the newlines a YAML
// payload needs keep it out of the schema.
// --------------------------------------------------------------------------

#[test]
fn value_schema_rejects_a_multi_line_yaml_payload() {
    let schema = WorkloadValueSchema::new(false).with_field(
        "tag",
        WorkloadValueFieldRule {
            required: true,
            default: None,
        },
    );

    let error = schema
        .validate_values(&values([(
            "tag",
            "1\"\n          securityContext:\n            privileged: true\n#",
        )]))
        .expect_err("a value carrying newlines is rejected");

    assert!(
        format!("{error}").contains("control character"),
        "unexpected error: {error}"
    );
}

/// An ordinary value keeps working: printable text of any shape passes.
#[test]
fn value_schema_accepts_printable_text() {
    let schema = WorkloadValueSchema::new(false).with_field(
        "tag",
        WorkloadValueFieldRule {
            required: true,
            default: None,
        },
    );

    let validated = schema
        .validate_values(&values([("tag", "1.2.3-rc1+build.7")]))
        .expect("printable text is valid");

    assert_eq!(validated["tag"], "1.2.3-rc1+build.7");
}

/// Binding after the parse means an instance value is always a string scalar.
/// A class that substitutes into a numeric or boolean field renders a string
/// there, and the object's Kubernetes schema rejects it at render: a value that
/// can change a field's type can also change what the field means.
#[test]
fn instance_values_bind_as_strings_even_in_numeric_positions() {
    let template = template_with_raw_object(TemplateText::from_parts([
        TemplateTextPart::literal(
            r#"
apiVersion: v1
kind: Service
metadata:
  name: raw-svc-{suffix}
spec:
  ports:
    - port: "#,
        ),
        TemplateTextPart::instance_value("port"),
        TemplateTextPart::literal("\n"),
    ]));

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("tenant-a", 1, values([("port", "8080")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
    })
    .expect_err("a string port does not fit the Service schema");

    assert!(
        format!("{error}").contains("does not match its Kubernetes schema"),
        "unexpected error: {error}"
    );
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
    })
    .expect("a rooted per-instance hostPath still renders");

    let pv_json = rendered
        .objects
        .iter()
        .find(|object| object.object.kind() == "PersistentVolume")
        .expect("PersistentVolume was rendered")
        .object
        .to_kubernetes_json();
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
            termination_grace_period_seconds: None,
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
            app_protocol: Default::default(),
        },
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

/// `{suffix}` in a literal part reads the platform's instance suffix value.
fn template_with_raw_object(manifest: TemplateText) -> ManifestTemplate {
    let mut parts = Vec::new();
    for part in manifest.parts() {
        let TemplateTextPart::Literal(literal) = part else {
            parts.push(part.clone());
            continue;
        };
        let mut pieces = literal.split("{suffix}");
        parts.push(TemplateTextPart::literal(pieces.next().unwrap_or_default()));
        for piece in pieces {
            parts.push(TemplateTextPart::instance_value(
                control_plane::manifest::INSTANCE_SUFFIX_VALUE,
            ));
            parts.push(TemplateTextPart::literal(piece));
        }
    }
    ManifestTemplate {
        raw_objects: vec![RawKubernetesManifestTemplate {
            manifest: TemplateText::from_parts(parts),
        }],
        ..base_template()
    }
}

/// The JSON of the rendered object whose name starts with `prefix`.
fn rendered_named(rendered: &RenderedManifest, prefix: &str) -> Value {
    rendered
        .objects
        .iter()
        .find(|object| object.object.name().starts_with(prefix))
        .unwrap_or_else(|| panic!("an object named {prefix}... renders"))
        .object
        .to_kubernetes_json()
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
