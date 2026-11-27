//! Final-image release driver. Mutations are sent once; observation retries are
//! bounded and always identify an exact Pod and exact expected public view.
use super::*;
use control_plane::api::pb;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, Preconditions};
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::timeout;
#[path = "../support/http_once.rs"]
mod http_once;
#[path = "peers.rs"]
mod peers;
#[path = "resource.rs"]
mod resource;
#[path = "sessions.rs"]
mod sessions;
use peers::*;
const HOST: &str = "dynamic.sleepypods.test";
const IDS: [&str; 2] = ["dynamic-a", "dynamic-b"];
const CP: &str = "sleepypods-control-plane";
const FP: &str = "sleepypods-frontline";

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
struct Artifact {
    path: PathBuf,
    events: Vec<serde_json::Value>,
}
impl Artifact {
    fn new() -> TestResult<Self> {
        Ok(Self {
            path: PathBuf::from(env::var("SLEEPYPODS_E2E_ARTIFACT_DIR")?)
                .join("dynamic-lifecycle.json"),
            events: Vec::new(),
        })
    }
    fn record(&mut self, event: &str, detail: serde_json::Value) -> TestResult<()> {
        println!("DYNAMIC_PASS {event} {detail}");
        self.events
            .push(serde_json::json!({"unix_millis":now(),"event":event,"detail":detail}));
        fs::write(&self.path, serde_json::to_vec_pretty(&self.events)?)?;
        Ok(())
    }
}
async fn publish(
    operator: &mut Operator,
    id: &str,
    version: u64,
    cert: &rcgen::CertifiedKey<rcgen::KeyPair>,
) -> TestResult<pb::CertificateMetadata> {
    let result = operator
        .publish_certificate(pb::PublishCertificateRequest {
            certificate_id: id.into(),
            expected_version: Some(version),
            bundle: Some(pb::CertificateBundle {
                chain_der: vec![cert.cert.der().to_vec()],
                private_key_pkcs8_der: cert.signing_key.serialize_der(),
            }),
        })
        .await?
        .into_inner();
    assert_eq!(result.version, version + 1);
    assert!(!result.deleted);
    Ok(result)
}
async fn bind(
    operator: &mut Operator,
    host: &str,
    revision: u64,
    id: Option<&str>,
) -> TestResult<u64> {
    let result = operator
        .set_tls_binding(pb::SetTlsBindingRequest {
            hostname: host.into(),
            expected_revision: Some(revision),
            certificate_id: id.map(str::to_owned),
        })
        .await?
        .into_inner();
    assert_eq!(result.certificate_id, id.map(str::to_owned));
    assert!(result.revision > revision);
    Ok(result.revision)
}
async fn instance(operator: &mut Operator, id: &str) -> TestResult<Instance> {
    Ok(operator
        .get_instance(GetInstanceRequest {
            instance_id: id.into(),
        })
        .await?
        .into_inner())
}
async fn create(operator: &mut Operator, config: &E2eConfig) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            class_id: "dynamic-web".into(),
            version: 1,
            value_schema: Some(WorkloadValueSchema {
                fields: HashMap::from([(
                    "target".into(),
                    WorkloadValueFieldRule {
                        required: true,
                        default_value: None,
                    },
                )]),
                allow_extra: false,
            }),
            template: Some(manifest_template(config, "http", None)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 1000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        })
        .await?;
    for (index, id) in IDS.iter().enumerate() {
        operator
            .create_instance(CreateInstanceRequest {
                idempotency_key: format!("create-{id}"),
                instance_id: (*id).into(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "dynamic-web".into(),
                    version: 1,
                }),
                values: HashMap::from([("target".into(), (*id).into())]),
                routes: Vec::new(),
            })
            .await?;
        let mut identity = http_route_identity(RouteHostKind::Exact as i32, HOST);
        if let Some(route_identity::Kind::Http(http)) = &mut identity.kind {
            http.path_prefix = Some(format!("/{}", if index == 0 { "a" } else { "b" }));
        }
        operator
            .create_route_binding(CreateRouteBindingRequest {
                idempotency_key: format!("route-{id}"),
                route_binding_id: format!("route-{id}"),
                instance_id: (*id).into(),
                identity: Some(identity),
                protocol: ProtocolRoute::Http as i32,
            })
            .await?;
    }
    Ok(())
}
async fn expect_state(
    operator: &mut Operator,
    state: PbInstanceState,
    generation: u64,
) -> TestResult<()> {
    for id in IDS {
        let current = instance(operator, id).await?;
        assert_eq!(current.state, state as i32);
        assert_eq!(current.generation, generation);
    }
    Ok(())
}
fn assert_h2_response(body: &str, instance: &str, path: &str) -> TestResult<()> {
    for expected in [
        "sleepypods-protocol-app".to_owned(),
        format!("instance={instance}"),
        "request_version=HTTP/2".to_owned(),
        format!("path={path}"),
    ] {
        if !body.lines().any(|line| line == expected) {
            return Err(format!("H2 backend response lacks exact marker {expected:?}").into());
        }
    }
    Ok(())
}
async fn direct_backend_h2(
    kube: Client,
    namespace: &str,
    artifact: &mut Artifact,
) -> TestResult<()> {
    let api: Api<Pod> = Api::namespaced(kube, namespace);
    let mut pods = api
        .list(&ListParams::default().labels("sleepypods.io/instance-id=dynamic-a"))
        .await?
        .items;
    if pods.len() != 1 {
        return Err("direct H2 proof requires the exact single current dynamic-a Pod".into());
    }
    let peer = Peer::start(namespace, pods.remove(0), &[APP_PORT as u16]).await?;
    let stream = timeout(
        Duration::from_secs(3),
        tokio::net::TcpStream::connect(peer.addr(APP_PORT as u16)),
    )
    .await??;
    let mut h2 = H2::connect(stream).await?;
    let response = h2.get(HOST, "/a/direct-h2-backend").await;
    h2.close().await;
    assert_h2_response(&response?, IDS[0], "/a/direct-h2-backend")?;
    artifact.record(
        "direct-backend-h2",
        serde_json::json!({"pod":peer.identity(),"request_version":"HTTP/2","instance":IDS[0],"path":"/a/direct-h2-backend"}),
    )
}
async fn denied(peers: &[Peer], host: &str, config: Arc<ClientConfig>) -> TestResult<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    for peer in peers {
        live_control(peer).await?;
        timeout_at(deadline, async {
            loop {
                if tls_refused(peer.addr(8443), host, config.clone()).await? {
                    return Ok::<_, Box<dyn Error + Send + Sync>>(());
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await??;
        live_control(peer).await?;
    }
    Ok(())
}
use tokio::time::timeout_at;
// This test is the sole writer of these infrastructure fields (no HPA).
// Merge only the owned field and immutable UID; a controller's status-only RV
// update cannot conflict and an object replacement cannot inherit the mutation.
async fn scale(kube: Client, namespace: &str, name: &str, replicas: i32) -> TestResult<()> {
    let api = Api::<Deployment>::namespaced(kube, namespace);
    let current = api.get(name).await?;
    api.patch(name,&PatchParams::default(),&Patch::Merge(serde_json::json!({"metadata":{"uid":current.metadata.uid},"spec":{"replicas":replicas}}))).await?;
    Ok(())
}
async fn absent(kube: Client, namespace: &str, name: &str) -> TestResult<()> {
    let api = Api::<Pod>::namespaced(kube, namespace);
    timeout(Duration::from_secs(90), async {
        loop {
            if api
                .list(&ListParams::default().labels(&format!("app.kubernetes.io/name={name}")))
                .await?
                .items
                .is_empty()
            {
                return Ok::<_, Box<dyn Error + Send + Sync>>(());
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await?
}

async fn sidecar_identities(
    kube: Client,
    namespace: &str,
    artifact: &mut Artifact,
    event: &str,
) -> TestResult<()> {
    let api = Api::<Pod>::namespaced(kube, namespace);
    let mut identities = Vec::new();
    for id in IDS {
        let pods = api
            .list(&ListParams::default().labels(&format!("sleepypods.io/instance-id={id}")))
            .await?
            .items;
        let current = pods
            .iter()
            .filter(|pod| pod.metadata.deletion_timestamp.is_none())
            .collect::<Vec<_>>();
        assert_eq!(
            current.len(),
            1,
            "expected exact single current workload Pod"
        );
        let pod = current[0];
        let sidecar = pod
            .status
            .as_ref()
            .and_then(|status| status.container_statuses.as_ref())
            .and_then(|statuses| {
                statuses
                    .iter()
                    .find(|status| status.name == "sleepypods-sidecar")
            })
            .ok_or("running sidecar status missing")?;
        assert!(
            sidecar.ready
                && sidecar
                    .state
                    .as_ref()
                    .is_some_and(|state| state.running.is_some())
        );
        assert!(!sidecar.image_id.is_empty());
        identities.push(serde_json::json!({"instance_id":id,"pod":pod.metadata.name,"uid":pod.metadata.uid,
            "node":pod.spec.as_ref().and_then(|spec|spec.node_name.as_ref()),"container":sidecar.name,
            "image":sidecar.image,"image_id":sidecar.image_id,"container_id":sidecar.container_id}));
    }
    artifact.record(event, serde_json::json!({"pods":identities}))
}
async fn delivery_service(kube: Client, namespace: &str, enabled: bool) -> TestResult<()> {
    let api = Api::<Service>::namespaced(kube, namespace);
    let current = api.get(CP).await?;
    let selector = if enabled {
        CP
    } else {
        "paused-dynamic-delivery"
    };
    api.patch(CP,&PatchParams::default(),&Patch::Merge(serde_json::json!({"metadata":{"uid":current.metadata.uid},"spec":{"selector":{"app.kubernetes.io/name":selector}}}))).await?;
    Ok(())
}
async fn no_delivery_endpoints(kube: Client, namespace: &str) -> TestResult<()> {
    let api = Api::<k8s_openapi::api::discovery::v1::EndpointSlice>::namespaced(kube, namespace);
    timeout(Duration::from_secs(15), async {
        loop {
            let slices = api
                .list(&ListParams::default().labels(&format!("kubernetes.io/service-name={CP}")))
                .await?;
            if slices.items.iter().all(|s| s.endpoints.is_empty()) {
                return Ok::<_, Box<dyn Error + Send + Sync>>(());
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await?
}
#[tokio::test]
#[ignore = "requires the owned final-image dynamic TLS kind deployment"]
async fn dynamic_certificate_lifecycle_through_exact_replicas() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_TLS").as_deref() != Ok("1") {
        return Err("dynamic release gate must be explicitly enabled".into());
    }
    control_plane::install_rustls_crypto_provider();
    let mut config = E2eConfig::from_env()?;
    // HTTP/2 is preserved upstream; use the existing H1/H2/WS backend fixture.
    config.app_image = env::var("SLEEPYPODS_E2E_PROTOCOL_APP_IMAGE")?;
    let kube = Client::try_default().await?;
    let mut artifact = Artifact::new()?;
    let result = timeout(
        Duration::from_secs(1200),
        run(&config, kube.clone(), &mut artifact),
    )
    .await;
    // The release fixture owns only this namespace. Restore its CP deployment
    // after an outage assertion fails so diagnostics remain possible.
    let restore = timeout(Duration::from_secs(15), async {
        delivery_service(kube.clone(), &config.namespace, true).await?;
        scale(kube, &config.namespace, CP, 2).await
    })
    .await;
    result??;
    restore??;
    artifact.record("complete", serde_json::json!({"status":"passed"}))?;
    Ok(())
}
async fn run(config: &E2eConfig, kube: Client, artifact: &mut Artifact) -> TestResult<()> {
    let mut cp = peers(kube.clone(), &config.namespace, CP, 2, &[50051]).await?;
    let mut fronts = peers(
        kube.clone(),
        &config.namespace,
        FP,
        2,
        &[8443, 9443, 8080, 9090],
    )
    .await?;
    artifact.record("exact-initial-pods",serde_json::json!({"control_planes":cp.iter().map(Peer::identity).collect::<Vec<_>>(),"frontlines":fronts.iter().map(Peer::identity).collect::<Vec<_>>()}))?;
    for peer in &fronts {
        let spec = peer.pod.spec.as_ref().ok_or("Pod spec missing")?;
        let container = spec
            .containers
            .iter()
            .find(|c| c.name == "frontline")
            .ok_or("Frontline container missing")?;
        assert!(
            container.volume_mounts.as_ref().is_none_or(Vec::is_empty),
            "Frontline has an unexpected volume mount"
        );
        assert!(container.env.as_ref().is_none_or(|env| env
            .iter()
            .all(|v| !v.name.contains("TLS_TERMINATION_CERTS"))));
    }
    let mut first =
        connect_operator(&format!("https://localhost:{}", cp[0].addr(50051).port())).await?;
    let mut second =
        connect_operator(&format!("https://localhost:{}", cp[1].addr(50051).port())).await?;
    create(&mut first, config).await?;
    expect_state(&mut second, PbInstanceState::Cold, 0).await?;
    let challenge = pb::Http01ChallengeKey {
        host: HOST.into(),
        token: "before-publication".into(),
    };
    let challenge_expiry = now() + 3000;
    first
        .put_http01_challenge(pb::PutHttp01ChallengeRequest {
            key: Some(challenge.clone()),
            key_authorization: "before-publication.proof".into(),
            expires_at_unix_millis: challenge_expiry,
        })
        .await?;
    for front in &fronts {
        let response = http_once::get_once(
            front.addr(8080),
            HOST,
            "/.well-known/acme-challenge/before-publication",
            Duration::from_secs(3),
        )
        .await?;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.body(), "before-publication.proof");
        let missing = http_once::get_once(
            front.addr(8080),
            HOST,
            "/.well-known/acme-challenge/absent",
            Duration::from_secs(3),
        )
        .await?;
        assert_eq!(missing.status(), http::StatusCode::NOT_FOUND);
        assert!(!missing.body().contains("proof"));
    }
    timeout(Duration::from_secs(5), async {
        while now() < challenge_expiry + 1 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    for front in &fronts {
        let response = http_once::get_once(
            front.addr(8080),
            HOST,
            "/.well-known/acme-challenge/before-publication",
            Duration::from_secs(3),
        )
        .await?;
        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        assert!(!response.body().contains("proof"));
    }
    expect_state(&mut second, PbInstanceState::Cold, 0).await?;
    artifact.record(
        "http01-before-publication-no-wake",
        serde_json::json!({"generation":0,"replicas":2}),
    )?;
    let a1 = rcgen::generate_simple_self_signed(vec![HOST.into()])?;
    let a2 = rcgen::generate_simple_self_signed(vec![HOST.into()])?;
    let a3 = rcgen::generate_simple_self_signed(vec![HOST.into()])?;
    let a4 = rcgen::generate_simple_self_signed(vec![HOST.into()])?;
    let b1 = rcgen::generate_simple_self_signed(vec![HOST.into()])?;
    let roots = vec![
        a1.cert.der().clone(),
        a2.cert.der().clone(),
        a3.cert.der().clone(),
        a4.cert.der().clone(),
        b1.cert.der().clone(),
    ];
    publish(&mut first, "dynamic-cert-a", 0, &a1).await?;
    bind(&mut second, HOST, 0, Some("dynamic-cert-a")).await?;
    converged(&fronts, HOST, &roots, a1.cert.der()).await?;
    expect_state(&mut first, PbInstanceState::Cold, 0).await?;
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        for front in &fronts {
            let stream = handshake(
                front.addr(8443),
                HOST,
                Arc::new(client_config(&roots, version, b"h2")?),
            )
            .await?;
            assert_peer(&stream, a1.cert.der(), version, b"h2")?;
            artifact.record("protocol-peer",serde_json::json!({"pod":front.name(),"uid":front.uid(),"fingerprint":fingerprint(a1.cert.der()),"protocol":format!("{:?}",version.version),"alpn":"h2","full_handshake":true}))?;
        }
    }
    artifact.record("publish-handshake-no-wake",serde_json::json!({"fingerprint":fingerprint(a1.cert.der()),"tls_versions":["1.2","1.3"],"alpn":"h2","generation":0}))?;
    for (i, front) in fronts.iter().enumerate() {
        let stream = handshake(
            front.addr(8443),
            HOST,
            Arc::new(client_config(&roots, &rustls::version::TLS13, b"http/1.1")?),
        )
        .await?;
        let response = http1(
            stream,
            HOST,
            if i == 0 { "/a/first" } else { "/b/first" },
            Duration::from_secs(140),
        )
        .await?;
        assert!(response.contains(&format!("instance={}\n", IDS[i])));
    }
    expect_state(&mut second, PbInstanceState::Running, 2).await?;
    sidecar_identities(
        kube.clone(),
        &config.namespace,
        artifact,
        "first-wake-sidecars",
    )
    .await?;
    direct_backend_h2(kube.clone(), &config.namespace, artifact).await?;
    let mut h2 = H2::connect(
        handshake(
            fronts[0].addr(8443),
            HOST,
            Arc::new(client_config(&roots, &rustls::version::TLS13, b"h2")?),
        )
        .await?,
    )
    .await?;
    let mut websocket = None;
    let live_result = async {
        assert_h2_response(
            &h2.get(HOST, "/a/live-before").await?,
            IDS[0],
            "/a/live-before",
        )?;
        let ws_stream = handshake(
            fronts[1].addr(8443),
            HOST,
            Arc::new(client_config(&roots, &rustls::version::TLS13, b"http/1.1")?),
        )
        .await?;
        let (ws, _) = timeout(
            Duration::from_secs(10),
            tokio_tungstenite::client_async(format!("wss://{HOST}/b/ws"), ws_stream),
        )
        .await??;
        websocket = Some(ws);
        sessions::echo(websocket.as_mut().unwrap(), "before-rotation").await?;
        publish(&mut second, "dynamic-cert-a", 1, &a2).await?;
        converged(&fronts, HOST, &roots, a2.cert.der()).await?;
        assert_h2_response(
            &h2.get(HOST, "/a/live-after").await?,
            IDS[0],
            "/a/live-after",
        )?;
        sessions::echo(websocket.as_mut().unwrap(), "after-rotation").await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    }
    .await;
    let close_result: TestResult<()> = if let Some(mut ws) = websocket {
        match timeout(Duration::from_secs(3), ws.close(None)).await {
            Ok(result) => result.map_err(Into::into),
            Err(error) => Err(error.into()),
        }
    } else {
        Ok(())
    };
    h2.close().await;
    live_result?;
    close_result?;
    artifact.record("rotation-preserves-live-h2-websocket",serde_json::json!({"old":fingerprint(a1.cert.der()),"new":fingerprint(a2.cert.der()),"replicas":2}))?;
    let invalid = first
        .publish_certificate(pb::PublishCertificateRequest {
            certificate_id: "dynamic-cert-a".into(),
            expected_version: Some(2),
            bundle: Some(pb::CertificateBundle {
                chain_der: vec![a3.cert.der().to_vec()],
                private_key_pkcs8_der: a1.signing_key.serialize_der(),
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
    converged(&fronts, HOST, &roots, a2.cert.der()).await?;
    publish(&mut first, "dynamic-cert-a", 2, &a3).await?;
    converged(&fronts, HOST, &roots, a3.cert.der()).await?;
    publish(&mut second, "dynamic-cert-b", 0, &b1).await?;
    let before_rebind = second
        .get_tls_binding(pb::GetTlsBindingRequest {
            hostname: HOST.into(),
        })
        .await?
        .into_inner();
    let mut revision = bind(
        &mut first,
        HOST,
        before_rebind.revision,
        Some("dynamic-cert-b"),
    )
    .await?;
    converged(&fronts, HOST, &roots, b1.cert.der()).await?;
    artifact.record("invalid-rotation-and-high-to-low-rebind",serde_json::json!({"from_version":3,"to_version":1,"binding_revision":revision,"fingerprint":fingerprint(b1.cert.der())}))?;
    sessions::offered(&fronts, HOST, &b1, &roots, false).await?;
    revision = bind(&mut second, HOST, revision, None).await?;
    denied(
        &fronts,
        HOST,
        Arc::new(client_config(&roots, &rustls::version::TLS13, b"h2")?),
    )
    .await?;
    sessions::offered(&fronts, HOST, &b1, &roots, true).await?;
    revision = bind(&mut first, HOST, revision, Some("dynamic-cert-b")).await?;
    converged(&fronts, HOST, &roots, b1.cert.der()).await?;
    second
        .remove_certificate(pb::RemoveCertificateRequest {
            certificate_id: "dynamic-cert-b".into(),
            expected_version: Some(1),
        })
        .await?;
    denied(
        &fronts,
        HOST,
        Arc::new(client_config(&roots, &rustls::version::TLS13, b"h2")?),
    )
    .await?;
    let binding = first
        .get_tls_binding(pb::GetTlsBindingRequest {
            hostname: HOST.into(),
        })
        .await?
        .into_inner();
    assert!(binding.revision > revision);
    assert!(binding.certificate_id.is_none());
    revision = bind(&mut first, HOST, binding.revision, Some("dynamic-cert-a")).await?;
    converged(&fronts, HOST, &roots, a3.cert.der()).await?;
    artifact.record(
        "unbind-remove-offered-session-denial",
        serde_json::json!({"binding_revision":revision,"tls_versions":["1.2","1.3"]}),
    )?;
    // Force an actual notification gap: first terminate every old CP, then
    // recreate CPs behind an empty service selector. Direct operator Pod paths
    // still work; proxies have no service endpoint through which to reconnect.
    scale(kube.clone(), &config.namespace, CP, 0).await?;
    absent(kube.clone(), &config.namespace, CP).await?;
    delivery_service(kube.clone(), &config.namespace, false).await?;
    scale(kube.clone(), &config.namespace, CP, 2).await?;
    let old_cp_uids = cp.iter().map(|p| p.uid().to_owned()).collect::<Vec<_>>();
    cp = peers(kube.clone(), &config.namespace, CP, 2, &[50051]).await?;
    assert!(cp
        .iter()
        .all(|p| !old_cp_uids.iter().any(|uid| uid == p.uid())));
    first = connect_operator(&format!("https://localhost:{}", cp[0].addr(50051).port())).await?;
    second = connect_operator(&format!("https://localhost:{}", cp[1].addr(50051).port())).await?;
    no_delivery_endpoints(kube.clone(), &config.namespace).await?;
    publish(&mut second, "dynamic-cert-a", 3, &a4).await?;
    for front in &fronts {
        let stream = handshake(
            front.addr(8443),
            HOST,
            Arc::new(client_config(&roots, &rustls::version::TLS13, b"h2")?),
        )
        .await?;
        assert_peer(&stream, a3.cert.der(), &rustls::version::TLS13, b"h2")?;
    }
    artifact.record("lost-update-barrier",serde_json::json!({"old_cp_uids":old_cp_uids,"new_control_planes":cp.iter().map(Peer::identity).collect::<Vec<_>>(),"service_endpoints":0,"retained_old":fingerprint(a3.cert.der()),"unreceived_new":fingerprint(a4.cert.der())}))?;
    delivery_service(kube.clone(), &config.namespace, true).await?;
    converged(&fronts, HOST, &roots, a4.cert.der()).await?;
    artifact.record(
        "replica-switch-snapshot-recovery",
        serde_json::json!({"fingerprint":fingerprint(a4.cert.der())}),
    )?;
    sessions::expiry(&mut first, &fronts).await?;
    resource::run(&mut first, &fronts, HOST, &roots, a4.cert.der(), artifact).await?;
    // Certificate-only handshakes and metrics reads do not keep application
    // activity alive. Wait for the real default Ready floor, never backdate it.
    for id in IDS {
        wait_for_instance_state(
            &mut second,
            id,
            PbInstanceState::Cold,
            Duration::from_secs(230),
        )
        .await?;
    }
    expect_state(&mut second, PbInstanceState::Cold, 4).await?;
    converged(&fronts, HOST, &roots, a4.cert.der()).await?;
    for (i, front) in fronts.iter().enumerate() {
        let stream = handshake(
            front.addr(8443),
            HOST,
            Arc::new(client_config(&roots, &rustls::version::TLS13, b"http/1.1")?),
        )
        .await?;
        assert!(http1(
            stream,
            HOST,
            if i == 0 { "/a/rewake" } else { "/b/rewake" },
            Duration::from_secs(140)
        )
        .await?
        .contains(&format!("instance={}\n", IDS[i])));
    }
    expect_state(&mut second, PbInstanceState::Running, 6).await?;
    sidecar_identities(kube.clone(), &config.namespace, artifact, "rewake-sidecars").await?;
    artifact.record("real-default-floor-sleep-one-shot-rewake",serde_json::json!({"cold_generation":4,"rewake_generation":6,"retained_fingerprint":fingerprint(a4.cert.der())}))?;
    // Removing all CP Pods is the explicit no-future-response barrier. The
    // original fixed cache lease may end earlier than five minutes from here.
    scale(kube.clone(), &config.namespace, CP, 0).await?;
    absent(kube.clone(), &config.namespace, CP).await?;
    let outage = Instant::now();
    artifact.record("all-cp-pods-terminated",serde_json::json!({"max_lease_millis":300000,"original_proxy_uids":fronts.iter().map(Peer::uid).collect::<Vec<_>>()}))?;
    for front in &fronts {
        let stream = handshake(
            front.addr(8443),
            HOST,
            Arc::new(client_config(&roots, &rustls::version::TLS13, b"h2")?),
        )
        .await?;
        assert_peer(&stream, a4.cert.der(), &rustls::version::TLS13, b"h2")?;
    }
    // Restart one exact proxy with CP unavailable, retain the other warm proxy.
    let old = fronts.pop().unwrap();
    Api::<Pod>::namespaced(kube.clone(), &config.namespace)
        .delete(
            old.name(),
            &DeleteParams {
                preconditions: Some(Preconditions {
                    uid: Some(old.uid().into()),
                    resource_version: None,
                }),
                ..Default::default()
            },
        )
        .await?;
    let old_uid = old.uid().to_owned();
    drop(old);
    let pod_api = Api::<Pod>::namespaced(kube.clone(), &config.namespace);
    // One immutable replacement deadline, independent of time spent deleting
    // the old Pod. Pin the first replacement identity; never substitute a later
    // Pod or infer startup failure from a restart counter alone.
    let replacement_deadline =
        (Instant::now() + Duration::from_secs(180)).min(outage + Duration::from_secs(250));
    let (replacement_name, replacement_uid) = timeout_at(replacement_deadline, async {
        loop {
            let pods = pod_api
                .list(&ListParams::default().labels(&format!("app.kubernetes.io/name={FP}")))
                .await?
                .items;
            if let Some(pod) = pods.into_iter().find(|pod| {
                pod.metadata.uid.as_deref() != Some(&old_uid)
                    && pod.metadata.uid.as_deref() != Some(fronts[0].uid())
                    && pod.metadata.deletion_timestamp.is_none()
            }) {
                return Ok::<_, Box<dyn Error + Send + Sync>>((
                    pod.metadata.name.ok_or("replacement name missing")?,
                    pod.metadata.uid.ok_or("replacement UID missing")?,
                ));
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;
    timeout_at(replacement_deadline,async {
        loop {
            let cold=pod_api.get(&replacement_name).await?;
            assert_eq!(cold.metadata.uid.as_deref(),Some(replacement_uid.as_str()));
            let status=cold.status.as_ref().ok_or("replacement status missing")?;
            assert!(!status.conditions.as_ref().is_some_and(|conditions|conditions.iter().any(|condition|condition.type_=="Ready"&&condition.status=="True")),"cold proxy became ready without CP");
            let container=status.container_statuses.as_ref().and_then(|statuses|statuses.iter().find(|container|container.name=="frontline"));
            if let Some(container)=container.filter(|container|container.restart_count>0) {
                let previous=container.last_state.as_ref().and_then(|state|state.terminated.as_ref()).ok_or("replacement's prior termination missing")?;
                let log=pod_api.logs(&replacement_name,&kube::api::LogParams{container:Some("frontline".into()),previous:true,limit_bytes:Some(131072),..Default::default()}).await?;
                assert!(log.contains("frontline control-plane startup exceeded 60 seconds"),"cold process did not report expected initial CP deadline");
                artifact.record("cold-start-setup-deadline",serde_json::json!({"pod":replacement_name,"uid":replacement_uid,"ready":false,"restart_count":container.restart_count,"prior_started_at":previous.started_at,"prior_finished_at":previous.finished_at,"prior_exit_code":previous.exit_code,"expected_deadline_log":true}))?;
                return Ok::<_,Box<dyn Error+Send+Sync>>(());
            }
            sleep(Duration::from_millis(100)).await;
        }
    }).await??;
    tokio::time::sleep_until(outage + Duration::from_secs(301)).await;
    let refusal_started = outage.elapsed().as_millis();
    for front in &fronts {
        live_control(front).await?;
        assert!(
            tls_refused(
                front.addr(8443),
                HOST,
                Arc::new(client_config(&roots, &rustls::version::TLS13, b"h2")?)
            )
            .await?,
            "warm proxy extended authorization beyond fixed maximum lease"
        );
    }
    assert!(
        refusal_started < 302000,
        "release driver missed the predeclared refusal observation"
    );
    artifact.record("default-lease-outage-refusal",serde_json::json!({"elapsed_millis":outage.elapsed().as_millis(),"warm_pod_uid":fronts[0].uid()}))?;
    scale(kube.clone(), &config.namespace, CP, 2).await?;
    cp = peers(kube.clone(), &config.namespace, CP, 2, &[50051]).await?;
    fronts = peers(
        kube.clone(),
        &config.namespace,
        FP,
        2,
        &[8443, 9443, 8080, 9090],
    )
    .await?;
    converged(&fronts, HOST, &roots, a4.cert.der()).await?;
    artifact.record("cp-return-cold-and-warm-recovery",serde_json::json!({"control_planes":cp.iter().map(Peer::identity).collect::<Vec<_>>(),"frontlines":fronts.iter().map(Peer::identity).collect::<Vec<_>>(),"fingerprint":fingerprint(a4.cert.der())}))?;
    Ok(())
}
