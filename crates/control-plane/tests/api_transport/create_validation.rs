//! Create RPCs accept only classes and instances that render, so the first
//! wake of an accepted instance meets no template error.
use super::*;

use control_plane::api::pb::{WorkloadValueFieldRule, WorkloadValueSchema};
use control_plane::{render_manifests, GetInstanceRequest as DomainGetInstanceRequest};

const SUFFIX_VALUE: &str = "sleepypods.instance_suffix";

fn service() -> (
    Arc<FakeInstanceStore>,
    StoreBackedOperatorApi<FakeKubernetesClient>,
) {
    let store = Arc::new(FakeInstanceStore::default());
    let api = store_operator_api(store.clone());
    (store, api)
}

fn field(required: bool, default_value: Option<&str>) -> WorkloadValueFieldRule {
    WorkloadValueFieldRule {
        required,
        default_value: default_value.map(str::to_owned),
    }
}

fn text(parts: &[(&str, bool)]) -> TemplateText {
    TemplateText {
        parts: parts
            .iter()
            .map(|(value, is_instance_value)| TemplateTextPart {
                kind: Some(if *is_instance_value {
                    template_text_part::Kind::InstanceValue((*value).to_owned())
                } else {
                    template_text_part::Kind::Literal((*value).to_owned())
                }),
            })
            .collect(),
    }
}

/// A Deployment class named `web-<tenant>` whose image has a default.
fn web_class(class_id: &str) -> CreateWorkloadClassVersionRequest {
    CreateWorkloadClassVersionRequest {
        class_id: class_id.to_owned(),
        version: 1,
        value_schema: Some(WorkloadValueSchema {
            fields: [
                ("tenant".to_owned(), field(true, None)),
                ("image".to_owned(), field(false, Some("example/web:1"))),
            ]
            .into(),
            allow_extra: false,
        }),
        template: Some(ManifestTemplate {
            workload: Some(WorkloadTemplate {
                kind: WorkloadKind::Deployment as i32,
                name: Some(composed_text("web-", "tenant")),
                replicas: None,
                app_container: Some(ContainerTemplate {
                    name: "app".to_owned(),
                    image: Some(instance_value_text("image")),
                    ports: vec![ContainerPortTemplate {
                        name: Some("http".to_owned()),
                        container_port: 8080,
                    }],
                    env: vec![],
                }),
            }),
            sidecar: Some(SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: Some(literal_text("sleepypods/sidecar:test")),
                listen_port: 15000,
                mode: None,
            }),
            service: Some(ServiceTemplate {
                name: Some(composed_text("web-", "tenant")),
                ports: vec![ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            volumes: vec![],
            raw_objects: vec![],
        }),
        sleep_policy: Some(sleep_policy_proto()),
        exclusivity_keys: vec![],
    }
}

fn template(request: &mut CreateWorkloadClassVersionRequest) -> &mut ManifestTemplate {
    request.template.as_mut().expect("class has a template")
}

fn host_path_volume(path: TemplateText) -> VolumeTemplate {
    VolumeTemplate {
        name: "data".to_owned(),
        mount_path: Some(literal_text("/data")),
        pv_name: Some(literal_text("pv")),
        pvc_name: Some(literal_text("pvc")),
        access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
        capacity: Some(literal_text("1Gi")),
        reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
        storage_class_name: None,
        source: Some(PersistentVolumeSourceTemplate {
            kind: Some(persistent_volume_source_template::Kind::HostPath(
                HostPathVolumeSourceTemplate {
                    path: Some(path),
                    r#type: None,
                },
            )),
        }),
    }
}

fn raw_object(manifest: TemplateText) -> RawKubernetesManifestTemplate {
    RawKubernetesManifestTemplate {
        manifest: Some(manifest),
    }
}

async fn create_class(
    api: &StoreBackedOperatorApi<FakeKubernetesClient>,
    request: CreateWorkloadClassVersionRequest,
) -> Result<(), Status> {
    api.create_workload_class_version(tonic::Request::new(request))
        .await
        .map(|_| ())
}

async fn create_instance(
    api: &StoreBackedOperatorApi<FakeKubernetesClient>,
    class_id: &str,
    instance_id: &str,
    values: &[(&str, &str)],
) -> Result<(), Status> {
    api.create_instance(tonic::Request::new(CreateInstanceRequest {
        idempotency_key: format!("create-{instance_id}"),
        instance_id: instance_id.to_owned(),
        workload_class: Some(WorkloadClassVersionRef {
            class_id: class_id.to_owned(),
            version: 1,
        }),
        values: values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
        routes: Vec::new(),
    }))
    .await
    .map(|_| ())
}

fn assert_rejected(result: Result<(), Status>, expected: &str) {
    let status = result.expect_err("the create RPC rejects the request");
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(
        status.message().contains(expected),
        "message {:?} should contain {expected:?}",
        status.message()
    );
}

#[tokio::test]
async fn class_and_instance_that_render_are_accepted() {
    let (store, api) = service();
    create_class(&api, web_class("web"))
        .await
        .expect("a renderable class is accepted");
    create_instance(&api, "web", "tenant-a", &[("tenant", "acme")])
        .await
        .expect("a renderable instance is accepted");
    assert!(store.instance_exists("tenant-a"));
}

#[tokio::test]
async fn class_creation_rejects_a_missing_service_template() {
    let (_, api) = service();
    let mut class = web_class("web");
    template(&mut class).service = None;

    assert_rejected(
        create_class(&api, class).await,
        "require a service template",
    );
}

#[tokio::test]
async fn class_creation_rejects_a_class_id_that_is_not_a_label_value() {
    let (_, api) = service();

    assert_rejected(
        create_class(&api, web_class("team/web")).await,
        "label value \"team/web\"",
    );
}

#[tokio::test]
async fn class_creation_rejects_an_instance_value_the_schema_leaves_undeclared() {
    let (_, api) = service();
    for rule in [None, Some(field(false, None))] {
        let mut class = web_class("web");
        let fields = &mut class.value_schema.as_mut().expect("schema").fields;
        fields.remove("image");
        if let Some(rule) = rule {
            fields.insert("image".to_owned(), rule);
        }

        assert_rejected(create_class(&api, class).await, "instance value \"image\"");
    }

    let mut class = web_class("web");
    class.exclusivity_keys = vec![WorkloadExclusivityKey {
        name: "disk".to_owned(),
        value: Some(instance_value_text("disk")),
    }];
    assert_rejected(create_class(&api, class).await, "instance value \"disk\"");
}

#[tokio::test]
async fn class_creation_rejects_a_host_path_without_a_directory_boundary() {
    let (_, api) = service();
    for prefix in ["/var/lib/tenants", "/"] {
        let mut class = web_class("web");
        template(&mut class).volumes = vec![host_path_volume(composed_text(prefix, "tenant"))];

        assert_rejected(
            create_class(&api, class).await,
            "volume.source.host_path.path",
        );
    }

    let mut class = web_class("web");
    template(&mut class).volumes = vec![host_path_volume(composed_text(
        "/var/lib/tenants/",
        "tenant",
    ))];
    create_class(&api, class)
        .await
        .expect("a literal directory ending in a slash bounds the instance value");
}

#[tokio::test]
async fn class_creation_rejects_raw_objects_that_cannot_render() {
    let (_, api) = service();
    let named = |kind: &str, name: &[(&str, bool)]| {
        let mut parts = vec![(
            format!("apiVersion: v1\nkind: {kind}\nmetadata:\n  name: "),
            false,
        )];
        parts.extend(
            name.iter()
                .map(|(value, flag)| ((*value).to_owned(), *flag)),
        );
        parts.push(("\n".to_owned(), false));
        let parts = parts
            .iter()
            .map(|(value, flag)| (value.as_str(), *flag))
            .collect::<Vec<_>>();
        raw_object(text(&parts))
    };
    let suffixed = [("settings-", false), (SUFFIX_VALUE, true)];

    for (raw, expected) in [
        (
            raw_object(literal_text("metadata: [unterminated")),
            "valid YAML or JSON",
        ),
        (named("ConfigMap", &suffixed), "not in the V1 allow-list"),
        (named("Secret", &[("settings", false)]), "instance suffix"),
        (
            named("Secret", &[("settings-", false), ("tenant", true)]),
            "instance suffix",
        ),
    ] {
        let mut class = web_class("web");
        template(&mut class).raw_objects = vec![raw];
        assert_rejected(create_class(&api, class).await, expected);
    }

    let mut class = web_class("web");
    template(&mut class).raw_objects = vec![named("Secret", &suffixed)];
    create_class(&api, class)
        .await
        .expect("a raw object named with the instance suffix is accepted");
}

#[tokio::test]
async fn class_and_instance_creation_reject_reserved_value_names() {
    let (_, api) = service();
    let mut class = web_class("web");
    class
        .value_schema
        .as_mut()
        .expect("schema")
        .fields
        .insert(SUFFIX_VALUE.to_owned(), field(false, Some("chosen")));
    assert_rejected(create_class(&api, class).await, "reserved");

    let mut class = web_class("web");
    class.value_schema.as_mut().expect("schema").allow_extra = true;
    create_class(&api, class).await.expect("class is accepted");
    assert_rejected(
        create_instance(
            &api,
            "web",
            "tenant-a",
            &[("tenant", "acme"), (SUFFIX_VALUE, "chosen")],
        )
        .await,
        "reserved",
    );
}

#[tokio::test]
async fn instance_creation_rejects_a_value_that_renders_an_invalid_name() {
    let (store, api) = service();
    create_class(&api, web_class("web"))
        .await
        .expect("class is accepted");

    assert_rejected(
        create_instance(&api, "web", "tenant-a", &[("tenant", "Acme_Corp")]).await,
        "invalid Kubernetes name",
    );
    assert!(!store.instance_exists("tenant-a"));
}

#[tokio::test]
async fn instance_creation_rejects_an_overlong_stateful_set_name() {
    let (_, api) = service();
    let mut class = web_class("db");
    template(&mut class)
        .workload
        .as_mut()
        .expect("workload")
        .kind = WorkloadKind::StatefulSet as i32;
    create_class(&api, class).await.expect("class is accepted");

    // "web-" plus 32 characters leaves the name one over the 52 a StatefulSet
    // may carry once the 17-character instance suffix is appended.
    let tenant = "t".repeat(32);
    assert_rejected(
        create_instance(&api, "db", "tenant-a", &[("tenant", &tenant)]).await,
        "52 characters",
    );
    create_instance(&api, "db", "tenant-b", &[("tenant", &tenant[1..])])
        .await
        .expect("a 52-character StatefulSet name is accepted");
}

#[tokio::test]
async fn instance_creation_rejects_a_digit_leading_service_name() {
    let (_, api) = service();
    let mut class = web_class("web");
    template(&mut class).service.as_mut().expect("service").name =
        Some(instance_value_text("tenant"));
    create_class(&api, class).await.expect("class is accepted");

    assert_rejected(
        create_instance(&api, "web", "tenant-a", &[("tenant", "42")]).await,
        "must start with a letter",
    );
}

#[tokio::test]
async fn instance_creation_requires_its_class() {
    let (_, api) = service();
    let status = create_instance(&api, "absent", "tenant-a", &[("tenant", "acme")])
        .await
        .expect_err("an instance needs a stored class version");
    assert_eq!(status.code(), Code::NotFound);
}

#[tokio::test]
async fn route_creation_rejects_hosts_no_request_can_carry() {
    let (_, api) = service();
    let create = |host: &'static str| {
        api.create_route_binding(tonic::Request::new(CreateRouteBindingRequest {
            idempotency_key: format!("route-{host}"),
            route_binding_id: format!("route-{host}"),
            instance_id: "tenant-a".to_owned(),
            identity: Some(http_identity(host, None)),
            protocol: ProtocolRoute::Http as i32,
        }))
    };
    for host in [
        "app.example.com:8443",
        "app.example.com/api",
        "bücher.example",
        "app example.com",
        "app..example.com",
        "-app.example.com",
        "[2001:db8::1]",
    ] {
        let status = create(host)
            .await
            .expect_err("a host frontline never looks up is rejected");
        assert_eq!(status.code(), Code::InvalidArgument, "{host}: {status:?}");
    }
    for host in ["App.Example.com.", "my_app.example.com", "192.0.2.10"] {
        create(host)
            .await
            .unwrap_or_else(|status| panic!("{host} is a host a request can carry: {status:?}"));
    }
}

/// xorshift64, so a failing case replays from its seed.
struct Cases(u64);

impl Cases {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn chance(&mut self, one_in: u64) -> bool {
        self.next().is_multiple_of(one_in)
    }

    fn pick<T: Copy>(&mut self, options: &[T]) -> T {
        options[(self.next() % options.len() as u64) as usize]
    }

    fn name(&mut self) -> TemplateText {
        let literal = self.pick(&[
            "web-",
            "web",
            "Web-",
            "9-",
            "a_b-",
            "-",
            "this-base-name-is-long-enough-to-fill-any-budget-",
        ]);
        match self.next() % 4 {
            0 => literal_text(literal),
            1 => instance_value_text("tenant"),
            2 => composed_text(literal, "tenant"),
            _ => composed_text(literal, "undeclared"),
        }
    }

    fn class(&mut self, class_id: &str) -> CreateWorkloadClassVersionRequest {
        let mut class = web_class(class_id);
        let (required, default_value) = self.pick(&[
            (true, None),
            (false, None),
            (false, Some("acme")),
            (false, Some("Acme_Corp")),
        ]);
        let schema = class.value_schema.as_mut().expect("schema");
        schema
            .fields
            .insert("tenant".to_owned(), field(required, default_value));
        schema.allow_extra = self.chance(2);
        if self.chance(8) {
            schema.fields.remove("image");
        }

        let kind = self.pick(&[WorkloadKind::Deployment, WorkloadKind::StatefulSet]);
        let workload_name = self.name();
        let service_name = self.name();
        let drop_service = self.chance(10);
        let sidecar_mode = self
            .chance(6)
            .then(|| self.pick(&["http", "tcp", "udp"]).to_owned());
        let volume = self.chance(4).then(|| {
            host_path_volume(composed_text(
                self.pick(&["/srv/tenants/", "/srv/tenants", "/", "relative/"]),
                "tenant",
            ))
        });
        let raw = self.chance(4).then(|| {
            let name: &[(&str, bool)] = match self.next() % 4 {
                0 => &[("settings-", false), (SUFFIX_VALUE, true)],
                1 => &[
                    ("settings-", false),
                    ("tenant", true),
                    ("-", false),
                    (SUFFIX_VALUE, true),
                ],
                2 => &[("settings", false)],
                _ => &[("settings-", false), ("tenant", true)],
            };
            let kind = self.pick(&["Secret", "Secret", "Service", "ConfigMap"]);
            let head = format!("apiVersion: v1\nkind: {kind}\nmetadata:\n  name: ");
            let mut parts = vec![(head.as_str(), false)];
            parts.extend_from_slice(name);
            parts.push(("\n", false));
            raw_object(text(&parts))
        });

        let manifest = template(&mut class);
        let workload = manifest.workload.as_mut().expect("workload");
        workload.kind = kind as i32;
        workload.name = Some(workload_name);
        manifest.sidecar.as_mut().expect("sidecar").mode = sidecar_mode;
        if drop_service {
            manifest.service = None;
        } else {
            manifest.service.as_mut().expect("service").name = Some(service_name);
        }
        manifest.volumes = volume.into_iter().collect();
        manifest.raw_objects = raw.into_iter().collect();
        class
    }

    fn values(&mut self) -> Vec<(&'static str, &'static str)> {
        let mut values = Vec::new();
        if !self.chance(5) {
            values.push((
                "tenant",
                self.pick(&[
                    "acme",
                    "tenant-7",
                    "Acme_Corp",
                    "42",
                    "a/../b",
                    "",
                    "a-tenant-slug-that-is-long-enough-to-overflow-a-name",
                ]),
            ));
        }
        if self.chance(5) {
            values.push(("image", "example/other:2"));
        }
        if self.chance(8) {
            values.push(("extra", "value"));
        }
        values
    }
}

#[tokio::test]
async fn every_accepted_instance_renders_at_wake() {
    let (store, api) = service();
    let mut cases = Cases(0x5eed_cafe_f00d_d00d);
    let (mut classes, mut instances) = (0, 0);
    for case in 0..2_000 {
        let class_id = format!("class-{case}");
        if create_class(&api, cases.class(&class_id)).await.is_err() {
            continue;
        }
        classes += 1;
        let class = store
            .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
                control_plane::WorkloadClassVersionRef::new(
                    control_plane::WorkloadClassId::new(class_id.clone()).expect("class ID"),
                    Generation::new(1),
                ),
            ))
            .await
            .expect("store answers")
            .expect("accepted class is stored");
        for attempt in 0..3 {
            let instance_id = format!("instance-{case}-{attempt}");
            let values = cases.values();
            if create_instance(&api, &class_id, &instance_id, &values)
                .await
                .is_err()
            {
                continue;
            }
            instances += 1;
            let mut instance = store
                .get_instance(DomainGetInstanceRequest::new(
                    InstanceId::new(instance_id).expect("instance ID"),
                ))
                .await
                .expect("store answers")
                .expect("accepted instance is stored");
            // A wake renders a later generation than the one create checked.
            instance.generation = Generation::new(7);
            let sleep_policy = class
                .sleep_policy
                .resolve(&instance.values)
                .expect("accepted instance resolves its sleep policy");
            if let Err(error) = render_manifests(control_plane::RenderManifestRequest {
                template: &class.template,
                instance: &instance,
                sleep_policy,
                namespace: target().namespace(),
            }) {
                panic!("case {case} values {values:?}: accepted instance fails to render: {error}");
            }
            class
                .render_exclusivity_keys(&instance.values)
                .expect("accepted instance renders its exclusivity keys");
        }
    }
    assert!(
        classes >= 100 && instances >= 100,
        "the generator must reach accepted cases, got {classes} classes and {instances} instances"
    );
}
