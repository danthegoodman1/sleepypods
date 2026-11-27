#[path = "support/deletion_observation.rs"]
mod deletion_observation;

use bytes::Bytes;
use control_plane::api::pb::{self, operator_control_plane_client::OperatorControlPlaneClient};
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use prost::Message;
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, task::JoinSet};

type Body = BoxBody<Bytes, Infallible>;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
fn instance() -> pb::Instance {
    pb::Instance {
        instance_id: "member".into(),
        state: pb::InstanceState::Running as i32,
        generation: 2,
        ..Default::default()
    }
}
fn deleting() -> pb::Instance {
    pb::Instance {
        state: pb::InstanceState::Deleting as i32,
        generation: 3,
        ..instance()
    }
}
fn status() -> pb::ReconcileMaterializationResponse {
    pb::ReconcileMaterializationResponse {
        found: true,
        materialization_id: "member:target:apps".into(),
        state: "Deleting".into(),
        operation_deadline_unix_millis: now() + 10_000,
        ..Default::default()
    }
}

fn message<T: Message>(value: &T) -> http::Response<Body> {
    let encoded = value.encode_to_vec();
    let mut data = vec![0];
    data.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    data.extend_from_slice(&encoded);
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", "0".parse().unwrap());
    let body = StreamBody::new(futures_util::stream::iter([
        Ok(hyper::body::Frame::data(Bytes::from(data))),
        Ok(hyper::body::Frame::trailers(trailers)),
    ]))
    .boxed();
    http::Response::builder()
        .header("content-type", "application/grpc")
        .body(body)
        .unwrap()
}
fn failure(code: &str) -> http::Response<Body> {
    http::Response::builder()
        .header("content-type", "application/grpc")
        .header("grpc-status", code)
        .body(Full::new(Bytes::new()).boxed())
        .unwrap()
}
fn decode<T: Message + Default>(bytes: &Bytes) -> T {
    assert!(bytes.len() >= 5 && bytes.len() < 4096);
    assert_eq!(bytes[0], 0);
    assert_eq!(
        u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize,
        bytes.len() - 5
    );
    T::decode(bytes.slice(5..)).unwrap()
}

struct Scenario {
    status: Mutex<pb::ReconcileMaterializationResponse>,
    response: Mutex<pb::Instance>,
    deletes: AtomicUsize,
    statuses: AtomicUsize,
    gets: AtomicUsize,
    active_gets: AtomicUsize,
    not_found_after: usize,
    status_delay: Duration,
    status_error: bool,
    get_error: bool,
    block_get: bool,
}
impl Default for Scenario {
    fn default() -> Self {
        Self {
            status: Mutex::new(status()),
            response: Mutex::new(deleting()),
            deletes: AtomicUsize::new(0),
            statuses: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            active_gets: AtomicUsize::new(0),
            not_found_after: usize::MAX,
            status_delay: Duration::ZERO,
            status_error: false,
            get_error: false,
            block_get: false,
        }
    }
}
struct ActiveGet(Arc<Scenario>);
impl Drop for ActiveGet {
    fn drop(&mut self) {
        self.0.active_gets.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn server(
    scenario: Arc<Scenario>,
) -> (
    OperatorControlPlaneClient<tonic::transport::Channel>,
    JoinSet<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service =
            hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
                let scenario = scenario.clone();
                async move {
                    let path = request.uri().path().to_owned();
                    let bytes = request.into_body().collect().await.unwrap().to_bytes();
                    let response = if path.ends_with("/DeleteInstance") {
                        let request: pb::DeleteInstanceRequest = decode(&bytes);
                        assert_eq!(request.instance_id, "member");
                        assert_eq!(request.expected_generation, Some(2));
                        assert_eq!(
                            scenario.deletes.fetch_add(1, Ordering::SeqCst),
                            0,
                            "Delete must not replay"
                        );
                        message(&pb::DeleteInstanceResponse { accepted: true })
                    } else if path.ends_with("/ReconcileMaterialization") {
                        let request: pb::ReconcileMaterializationRequest = decode(&bytes);
                        assert!(request.status_only, "observation must never enqueue");
                        assert_eq!(request.instance_id, "member");
                        scenario.statuses.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(scenario.status_delay).await;
                        if scenario.status_error {
                            failure("14")
                        } else {
                            message(&*scenario.status.lock().unwrap())
                        }
                    } else if path.ends_with("/GetInstance") {
                        let request: pb::GetInstanceRequest = decode(&bytes);
                        assert_eq!(request.instance_id, "member");
                        let count = scenario.gets.fetch_add(1, Ordering::SeqCst);
                        scenario.active_gets.fetch_add(1, Ordering::SeqCst);
                        let _active = ActiveGet(scenario.clone());
                        if scenario.block_get {
                            std::future::pending::<()>().await;
                        }
                        if scenario.get_error {
                            failure("14")
                        } else if count >= scenario.not_found_after {
                            failure("5")
                        } else {
                            message(&*scenario.response.lock().unwrap())
                        }
                    } else {
                        panic!("unexpected RPC {path}")
                    };
                    Ok::<_, Infallible>(response)
                }
            });
        let _ = hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
            .await;
    });
    let client = OperatorControlPlaneClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    (client, tasks)
}
async fn finish(mut tasks: JoinSet<()>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn one_delete_and_one_status_allow_active_cleanup_until_authoritative_not_found() {
    let scenario = Arc::new(Scenario {
        not_found_after: 2,
        ..Default::default()
    });
    {
        let mut status = scenario.status.lock().unwrap();
        status.lease_owner = "live-worker".into();
        status.uncertain_effect = Some(Default::default());
        status.failure_kind = "uncertain".into();
    }
    let (mut client, tasks) = server(scenario.clone()).await;
    let result = deletion_observation::delete_with_original_deadline(
        &mut client,
        &instance(),
        "member:target:apps",
        Duration::from_secs(1),
    )
    .await;
    finish(tasks).await;
    result.unwrap();
    assert_eq!(scenario.deletes.load(Ordering::SeqCst), 1);
    assert_eq!(scenario.statuses.load(Ordering::SeqCst), 1);
    assert_eq!(scenario.gets.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn completion_race_requires_authoritative_absence_without_replaying_delete() {
    for found in [false, true] {
        for absent in [false, true] {
            let scenario = Arc::new(Scenario {
                not_found_after: if absent { 0 } else { usize::MAX },
                ..Default::default()
            });
            {
                let mut status = scenario.status.lock().unwrap();
                status.found = found;
                status.operation_deadline_unix_millis = 0;
                if !found {
                    status.state.clear();
                }
            }
            let (mut client, tasks) = server(scenario.clone()).await;
            let result = deletion_observation::delete_with_original_deadline(
                &mut client,
                &instance(),
                "member:target:apps",
                Duration::from_secs(1),
            )
            .await;
            finish(tasks).await;
            assert_eq!(
                result.is_ok(),
                absent,
                "found={found} absent={absent}: {result:?}"
            );
            assert_eq!(scenario.deletes.load(Ordering::SeqCst), 1);
            assert_eq!(scenario.statuses.load(Ordering::SeqCst), 1);
            assert_eq!(scenario.gets.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn identity_state_deadline_and_rpc_errors_are_fatal_without_delete_replay() {
    for case in 0..10 {
        let scenario = Scenario::default();
        match case {
            0 => scenario.response.lock().unwrap().instance_id = "replacement".into(),
            1 => scenario.response.lock().unwrap().generation = 4,
            2 => scenario.response.lock().unwrap().state = pb::InstanceState::Running as i32,
            3 => scenario.status.lock().unwrap().materialization_id = "replacement".into(),
            4 => {
                scenario
                    .status
                    .lock()
                    .unwrap()
                    .operation_deadline_unix_millis = now() - 1
            }
            5 => scenario.status.lock().unwrap().attempted = true,
            6 | 7 => {}
            8 => {
                let mut status = scenario.status.lock().unwrap();
                status.state = "Ready".into();
                status.operation_deadline_unix_millis = 0;
            }
            9 => {
                scenario
                    .status
                    .lock()
                    .unwrap()
                    .operation_deadline_unix_millis = -1
            }
            _ => unreachable!(),
        }
        let scenario = Arc::new(Scenario {
            status_error: case == 6,
            get_error: case == 7,
            ..scenario
        });
        let (mut client, tasks) = server(scenario.clone()).await;
        let result = deletion_observation::delete_with_original_deadline(
            &mut client,
            &instance(),
            "member:target:apps",
            Duration::from_secs(1),
        )
        .await;
        finish(tasks).await;
        assert!(result.is_err(), "case{case}");
        assert_eq!(scenario.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(scenario.statuses.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn original_deadline_and_delete_start_cap_are_immutable() {
    for persisted_limit in [true, false] {
        let scenario = Arc::new(Scenario {
            status_delay: Duration::from_millis(300),
            ..Default::default()
        });
        scenario
            .status
            .lock()
            .unwrap()
            .operation_deadline_unix_millis = if persisted_limit {
            now() + 600
        } else {
            i64::MAX
        };
        let (mut client, tasks) = server(scenario.clone()).await;
        let started = tokio::time::Instant::now();
        let cap = if persisted_limit {
            Duration::from_secs(2)
        } else {
            Duration::from_millis(600)
        };
        let result = deletion_observation::delete_with_original_deadline(
            &mut client,
            &instance(),
            "member:target:apps",
            cap,
        )
        .await;
        let elapsed = started.elapsed();
        finish(tasks).await;
        assert!(result.is_err());
        assert!(
            elapsed >= Duration::from_millis(450) && elapsed < Duration::from_millis(800),
            "status latency reset the deadline: {elapsed:?}"
        );
        assert_eq!(scenario.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(scenario.statuses.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn deadline_and_external_cancellation_cancel_the_pending_rpc() {
    for cancel in [false, true] {
        let scenario = Arc::new(Scenario {
            block_get: true,
            ..Default::default()
        });
        let (mut client, tasks) = server(scenario.clone()).await;
        // Keep the channel alive independently: cancellation must reset the
        // pending RPC rather than pass only because its whole connection closes.
        let _keep_channel_alive = client.clone();
        let cap = if cancel {
            Duration::from_secs(90)
        } else {
            Duration::from_millis(100)
        };
        let mut call = tokio::spawn(async move {
            deletion_observation::delete_with_original_deadline(
                &mut client,
                &instance(),
                "member:target:apps",
                cap,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while scenario.active_gets.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if cancel {
            call.abort();
            assert!(call.await.unwrap_err().is_cancelled());
        } else {
            assert!(tokio::time::timeout(Duration::from_secs(1), &mut call)
                .await
                .unwrap()
                .unwrap()
                .is_err());
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while scenario.active_gets.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        finish(tasks).await;
        assert_eq!(scenario.deletes.load(Ordering::SeqCst), 1);
    }
}
