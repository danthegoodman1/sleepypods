#[path = "support/native_operator.rs"]
mod native_operator;
use native_operator::{connect as connect_operator, Operator};
use std::{collections::HashMap, env, error::Error, time::Duration};

use control_plane::api::pb::{
    route_identity, template_text_part, ContainerPortTemplate, ContainerTemplate,
    CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    EnvVarTemplate, GetInstanceRequest, Instance, InstanceState as PbInstanceState,
    ManifestTemplate, RouteHost, RouteHostKind, RouteIdentity, ServicePortTemplate,
    ServiceTemplate, SidecarTemplate, SniRouteIdentity, TemplateText, TemplateTextPart,
    WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate,
    WorkloadValueSchema,
};
use tokio::time::{sleep, Instant};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLASS_ID: &str = "libpq-sni-postgres";
const INSTANCE_ID: &str = "e2e-libpq-sni-postgres";
const ROUTE_ID: &str = "e2e-libpq-sni-postgres-route";
const WORKLOAD_NAME: &str = "e2e-libpq-sni-postgres";
const SIDECAR_PORT: u32 = 15_000;
const POSTGRES_PORT: u32 = 5432;

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-libpq-sni.sh or an equivalent kind deployment"]
async fn create_postgres_libpq_sni_workload_through_deployed_operator() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_LIBPQ_SNI").as_deref() != Ok("1") {
        eprintln!("skipping libpq-SNI kind E2E because SLEEPYPODS_KIND_E2E_LIBPQ_SNI=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_operator_resources(&mut operator, &config).await?;
    let created = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    assert_eq!(created.generation, 0);

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    operator_endpoint: String,
    route_host: String,
    postgres_image: String,
    sidecar_image: String,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "https://127.0.0.1:19751".to_owned()),
            route_host: env::var("SLEEPYPODS_E2E_LIBPQ_SNI_HOST")
                .unwrap_or_else(|_| "exact.sni.sleepypods.test".to_owned()),
            postgres_image: env::var("SLEEPYPODS_E2E_LIBPQ_SNI_IMAGE")
                .unwrap_or_else(|_| "sleepypods/libpq-sni-postgres:kind-e2e-libpq-sni".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-libpq-sni".to_owned()),
        })
    }
}

async fn create_operator_resources(operator: &mut Operator, config: &E2eConfig) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            class_id: CLASS_ID.to_owned(),
            version: 1,
            value_schema: Some(WorkloadValueSchema {
                fields: Default::default(),
                allow_extra: false,
            }),
            template: Some(manifest_template(config)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 900_000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        })
        .await?;

    operator
        .create_instance(CreateInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: HashMap::new(),
            routes: Vec::new(),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            route_binding_id: ROUTE_ID.to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            identity: Some(RouteIdentity {
                kind: Some(route_identity::Kind::Sni(SniRouteIdentity {
                    host: Some(RouteHost {
                        kind: RouteHostKind::Exact as i32,
                        host: config.route_host.clone(),
                    }),
                })),
            }),
        })
        .await?;

    Ok(())
}

async fn wait_for_instance_state(
    operator: &mut Operator,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let instance = operator
            .get_instance(GetInstanceRequest {
                instance_id: INSTANCE_ID.to_owned(),
            })
            .await?
            .into_inner();
        let actual =
            PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified);
        if actual == expected {
            return Ok(instance);
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for instance {INSTANCE_ID} to reach {expected:?}; last state was {actual:?} generation {}",
                instance.generation
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn manifest_template(config: &E2eConfig) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(literal_text(WORKLOAD_NAME)),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "postgres".to_owned(),
                image: Some(literal_text(&config.postgres_image)),
                ports: vec![ContainerPortTemplate {
                    name: Some("postgres".to_owned()),
                    container_port: POSTGRES_PORT,
                }],
                env: vec![
                    EnvVarTemplate {
                        name: "POSTGRES_USER".to_owned(),
                        value: Some(literal_text("libpq_sni")),
                    },
                    EnvVarTemplate {
                        name: "POSTGRES_PASSWORD".to_owned(),
                        value: Some(literal_text("libpq_sni_password")),
                    },
                    EnvVarTemplate {
                        name: "POSTGRES_DB".to_owned(),
                        value: Some(literal_text("libpq_sni")),
                    },
                ],
            }),
            termination_grace_period_seconds: None,
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(&config.sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: Some("tcp".to_owned()),
            app_protocol: Default::default(),
        }),
        service: Some(ServiceTemplate {
            name: Some(literal_text(WORKLOAD_NAME)),
            ports: vec![ServicePortTemplate {
                name: Some("postgres".to_owned()),
                port: POSTGRES_PORT,
                target_port: POSTGRES_PORT,
            }],
        }),
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}
