//! The JSON the control plane sends to Kubernetes for a representative set of
//! classes, stamped as the projection plan stamps it. Each fixture pins every
//! rendered field and each object's rendered hash.
//!
//! After an intended change to rendered output, rewrite the fixtures with
//! `SLEEPYPODS_BLESS_GOLDEN=1 cargo test -p control-plane golden` and review
//! the diff.

use std::{collections::BTreeMap, path::PathBuf};

use serde_json::Value;

use super::{
    render_manifests_with_options, AppProtocol, ContainerPortTemplate, ContainerTemplate,
    CsiSecretRefTemplate, EnvVarTemplate, ManifestTemplate, PersistentVolumeAccessMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, RawKubernetesManifestTemplate,
    RenderManifestOptions, RenderManifestRequest, ServicePortTemplate, ServiceTemplate,
    SidecarTemplate, TemplateText, TemplateTextPart, VolumeTemplate, WorkloadKind,
    WorkloadTemplate, INSTANCE_SUFFIX_VALUE,
};
use crate::{
    auth::BearerToken,
    ids::{BackendGeneration, Generation, InstanceId, MaterializationId, WorkloadClassId},
    instance::{InstanceRecord, InstanceState},
    materialization::{MaterializationRecord, MaterializationState, MaterializationTarget},
    projection::ProjectionPlan,
    sleep_policy::ResolvedSleepPolicy,
    workload::WorkloadClassVersionRef,
};

const NAMESPACE: &str = "apps";

#[test]
fn stateless_class_with_private_sidecar_transport() {
    let token = BearerToken::new("sidecar_token", "sidecar-secret").expect("valid token");
    assert_golden(
        "stateless",
        &stateless_template(),
        RenderManifestOptions {
            sidecar_control_plane_token: Some(&token),
            sidecar_control_plane_endpoint: Some("https://cp.platform.example:50051"),
            sidecar_control_plane_ca: Some("PUBLIC-CA-ONLY"),
        },
    );
}

#[test]
fn stateful_class_with_csi_and_host_path_volumes() {
    assert_golden(
        "stateful",
        &stateful_template(),
        RenderManifestOptions::default(),
    );
}

#[test]
fn tcp_mode_sidecar() {
    let mut template = stateless_template();
    template.sidecar.mode = Some("tcp".to_owned());
    assert_golden("tcp", &template, RenderManifestOptions::default());
}

#[test]
fn h2c_app_behind_an_http_mode_sidecar() {
    let mut template = stateless_template();
    template.sidecar.mode = Some("http".to_owned());
    template.sidecar.app_protocol = AppProtocol::H2c;
    assert_golden("h2c", &template, RenderManifestOptions::default());
}

#[test]
fn raw_auxiliary_objects() {
    let mut template = stateless_template();
    template.raw_objects = vec![
        raw(&[
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: aux-{suffix}\n  annotations:\n    example.com/owner: team-",
            "@tenant",
            "\nspec:\n  type: ClusterIP\n  selector:\n    app: aux\n  ports:\n    - name: http\n      port: 8081\n      targetPort: http\n    - name: metrics\n      port: 9090\n      targetPort: 9090\n      protocol: TCP\n",
        ]),
        raw(&[
            "apiVersion: v1\nkind: Secret\nmetadata:\n  name: settings-{suffix}\n  labels:\n    example.com/tier: config\ntype: Opaque\nstringData:\n  tenant: ",
            "@tenant",
            "\ndata:\n  blob: aGVsbG8=\n",
        ]),
        raw(&[
            "apiVersion: v1\nkind: PersistentVolume\nmetadata:\n  name: aux-pv-{suffix}\nspec:\n  capacity:\n    storage: 1Gi\n  accessModes: [ReadWriteOnce]\n  volumeMode: Filesystem\n  storageClassName: manual\n  persistentVolumeReclaimPolicy: Retain\n  claimRef:\n    namespace: apps\n    name: aux-pvc-{suffix}\n  hostPath:\n    path: /var/local/aux\n    type: DirectoryOrCreate\n",
        ]),
        raw(&[
            "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: aux-pvc-{suffix}\nspec:\n  accessModes: [ReadWriteOnce]\n  volumeMode: Filesystem\n  storageClassName: manual\n  volumeName: aux-pv-{suffix}\n  resources:\n    requests:\n      storage: 1Gi\n",
        ]),
    ];
    assert_golden("raw_auxiliary", &template, RenderManifestOptions::default());
}

fn assert_golden(name: &str, template: &ManifestTemplate, options: RenderManifestOptions<'_>) {
    let instance = InstanceRecord {
        id: InstanceId::new("tenant-a").expect("valid instance ID"),
        workload_class: WorkloadClassVersionRef::new(
            WorkloadClassId::new("web").expect("valid class ID"),
            Generation::new(3),
        ),
        values: [("tenant", "acme"), ("volume", "vol-123"), ("image", "2.1")]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
        state: InstanceState::Waking,
        generation: Generation::new(7),
    };
    let manifest = render_manifests_with_options(
        RenderManifestRequest {
            template,
            instance: &instance,
            sleep_policy: ResolvedSleepPolicy {
                idle_timeout_ms: 120_000,
                idle_retry_backoff_ms: 5_000,
                drain_grace_timeout_ms: 30_000,
            },
            namespace: NAMESPACE,
        },
        options,
    )
    .expect("class renders");
    let materialization = MaterializationRecord {
        id: MaterializationId::new("tenant-a:cluster-a:apps").expect("valid materialization ID"),
        instance_id: instance.id.clone(),
        instance_generation: instance.generation,
        projection_generation: instance.generation,
        target: MaterializationTarget::new("cluster-a", NAMESPACE).expect("valid target"),
        state: MaterializationState::Pending,
        backend: None,
        pod_backend: None,
        backend_generation: BackendGeneration::new(1),
        rendered_objects: Vec::new(),
        exclusivity_keys: Vec::new(),
        reconciliation_lease: None,
    };
    let plan = ProjectionPlan::from_manifest(&materialization, &manifest).expect("plan");
    let actual = Value::Array(
        plan.manifest()
            .expect("plan carries the stamped manifest")
            .objects
            .iter()
            .map(|rendered| rendered.object.to_kubernetes_json())
            .collect(),
    );

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/manifest/golden")
        .join(format!("{name}.json"));
    if std::env::var_os("SLEEPYPODS_BLESS_GOLDEN").is_some() {
        let text = serde_json::to_string_pretty(&actual).expect("JSON serializes");
        std::fs::write(&path, text + "\n").expect("fixture is writable");
    }
    let expected: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path:?}: {error}")),
    )
    .expect("fixture is JSON");
    assert!(
        actual == expected,
        "rendered {name} differs from {path:?}:\n{}",
        serde_json::to_string_pretty(&actual).expect("JSON serializes")
    );
}

fn stateless_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: text(&["web-", "@tenant"]),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: text(&["example/web:", "@image"]),
                ports: vec![
                    ContainerPortTemplate {
                        name: Some("http".to_owned()),
                        container_port: 8080,
                    },
                    ContainerPortTemplate {
                        name: None,
                        container_port: 9090,
                    },
                ],
                env: vec![
                    EnvVarTemplate {
                        name: "TENANT".to_owned(),
                        value: text(&["@tenant"]),
                    },
                    EnvVarTemplate {
                        name: "EMPTY".to_owned(),
                        value: TemplateText::literal(""),
                    },
                ],
            },
            termination_grace_period_seconds: None,
        },
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: None,
            app_protocol: AppProtocol::default(),
        },
        service: Some(ServiceTemplate {
            name: text(&["web-", "@tenant"]),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn stateful_template() -> ManifestTemplate {
    let secret_ref = |name: &str| {
        Some(Box::new(CsiSecretRefTemplate {
            name: TemplateText::literal(name),
            namespace: TemplateText::literal("storage-secrets"),
        }))
    };
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::StatefulSet,
            name: text(&["db-", "@tenant"]),
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
            termination_grace_period_seconds: Some(90),
        },
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: Some("tcp".to_owned()),
            app_protocol: AppProtocol::default(),
        },
        service: Some(ServiceTemplate {
            name: text(&["db-", "@tenant"]),
            ports: vec![ServicePortTemplate {
                name: None,
                port: 5432,
                target_port: 5432,
            }],
        }),
        volumes: vec![
            VolumeTemplate {
                name: "data".to_owned(),
                mount_path: TemplateText::literal("/var/lib/postgresql/data"),
                pv_name: text(&["pv-", "@tenant"]),
                pvc_name: text(&["pvc-", "@tenant"]),
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                capacity: TemplateText::literal("10Gi"),
                reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
                storage_class_name: Some(TemplateText::literal("manual")),
                source: PersistentVolumeSourceTemplate::Csi {
                    driver: TemplateText::literal("csi.example.com"),
                    volume_handle: text(&["@volume"]),
                    fs_type: Some(TemplateText::literal("ext4")),
                    read_only: false,
                    volume_attributes: BTreeMap::from([("tenant".to_owned(), text(&["@tenant"]))]),
                    controller_publish_secret_ref: secret_ref("controller-publish"),
                    node_stage_secret_ref: secret_ref("node-stage"),
                    node_publish_secret_ref: secret_ref("node-publish"),
                    controller_expand_secret_ref: secret_ref("controller-expand"),
                    node_expand_secret_ref: secret_ref("node-expand"),
                },
            },
            VolumeTemplate {
                name: "scratch".to_owned(),
                mount_path: TemplateText::literal("/scratch"),
                pv_name: text(&["scratch-pv-", "@tenant"]),
                pvc_name: text(&["scratch-pvc-", "@tenant"]),
                access_modes: vec![
                    PersistentVolumeAccessMode::ReadWriteOnce,
                    PersistentVolumeAccessMode::ReadOnlyMany,
                ],
                capacity: TemplateText::literal("1Gi"),
                reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
                storage_class_name: None,
                source: PersistentVolumeSourceTemplate::HostPath {
                    path: text(&["/var/local/sleepypods/", "@tenant"]),
                    type_: Some(TemplateText::literal("DirectoryOrCreate")),
                },
            },
        ],
        raw_objects: Vec::new(),
    }
}

/// Template text from literal pieces; `@name` reads instance value `name`, and
/// `{suffix}` inside a literal reads the instance suffix.
fn text(pieces: &[&str]) -> TemplateText {
    let mut parts = Vec::new();
    for piece in pieces {
        if let Some(field) = piece.strip_prefix('@') {
            parts.push(TemplateTextPart::instance_value(field));
            continue;
        }
        let mut literals = piece.split("{suffix}");
        parts.push(TemplateTextPart::literal(
            literals.next().unwrap_or_default(),
        ));
        for literal in literals {
            parts.push(TemplateTextPart::instance_value(INSTANCE_SUFFIX_VALUE));
            parts.push(TemplateTextPart::literal(literal));
        }
    }
    TemplateText::from_parts(parts)
}

fn raw(pieces: &[&str]) -> RawKubernetesManifestTemplate {
    RawKubernetesManifestTemplate {
        manifest: text(pieces),
    }
}
