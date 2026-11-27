//! A successful RPC writes no stderr line; a rejected one writes its auth
//! decision. The fixture runs in a child process, which owns its stderr and its
//! process-wide observability sinks.
use super::*;

use control_plane::api::pb::operator_control_plane_client::OperatorControlPlaneClient;
use tonic::codegen::tokio_stream::wrappers::TcpListenerStream;

const FIXTURE_ENV: &str = "SLEEPYPODS_QUIET_LOGGING_FIXTURE";
const ACCEPTED_DONE: &str = "quiet-logging-fixture: accepted RPC returned";

#[test]
fn successful_rpc_writes_no_stderr_line() {
    for metrics in ["stderr-only", "with-metrics"] {
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "quiet_logging::rpc_logging_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env(FIXTURE_ENV, metrics)
            .output()
            .expect("fixture process runs");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{metrics} fixture failed: {stderr}"
        );

        let (accepted, rejected) = stderr
            .split_once(ACCEPTED_DONE)
            .unwrap_or_else(|| panic!("{metrics} fixture reports its accepted RPC: {stderr}"));
        assert!(
            !accepted.contains("observability.type="),
            "{metrics}: a successful RPC wrote to stderr: {accepted}"
        );
        let decisions = rejected
            .lines()
            .filter(|line| line.contains("observability.type="))
            .collect::<Vec<_>>();
        assert_eq!(decisions.len(), 1, "{metrics}: {rejected}");
        assert!(decisions[0].contains("event.name=control_plane.auth.decision"));
        assert!(decisions[0].contains("auth.decision=rejected"));
        assert!(decisions[0].contains("auth.reason=invalid"));
    }
}

#[tokio::test]
#[ignore = "runs in the child process of successful_rpc_writes_no_stderr_line"]
async fn rpc_logging_fixture() {
    let Ok(mode) = std::env::var(FIXTURE_ENV) else {
        return;
    };
    let prometheus = control_plane::runtime::install_runtime_observability(mode == "with-metrics");

    let store = store_with_class("class-1", 1);
    store.seed_instance(domain_instance("quiet", DomainInstanceState::Cold, 0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    let router = control_plane::runtime::operator_router_with_tls(
        store,
        operator_materializer(FakeKubernetesClient::default()),
        target(),
        auth_config(),
        RouteSubscriptionBroker::new(),
        None,
    )
    .expect("operator router builds");
    tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));

    let mut client = OperatorControlPlaneClient::connect(format!("http://{address}"))
        .await
        .expect("client connects");
    let request = |token: &'static str| {
        let mut request = tonic::Request::new(GetInstanceRequest {
            instance_id: "quiet".to_owned(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("header value"),
        );
        request
    };

    let instance = client
        .get_instance(request("operator-token"))
        .await
        .expect("the operator credential reads the instance")
        .into_inner();
    assert_eq!(instance.instance_id, "quiet");
    if let Some(prometheus) = prometheus {
        assert!(
            prometheus
                .render()
                .contains("sleepypods_control_plane_auth_decisions_total{outcome=\"success\"} 1"),
            "accepted decisions are counted: {}",
            prometheus.render()
        );
    }
    eprintln!("{ACCEPTED_DONE}");

    let status = client
        .get_instance(request("wrong-token"))
        .await
        .expect_err("an unknown credential is rejected");
    assert_eq!(status.code(), Code::Unauthenticated);
}
