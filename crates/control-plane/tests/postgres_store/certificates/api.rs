//! Real encrypted API plus the real encrypted persistence provider.
use super::*;
use control_plane::{
    api::{pb, RouteSubscriptionBroker},
    AuthConfig, KubeMaterializerClient, KubernetesMaterializer, StaticBearerTokens,
};
use pb::{
    operator_control_plane_client::OperatorControlPlaneClient,
    proxy_control_plane_client::ProxyControlPlaneClient,
};
use tonic::{
    transport::{Channel, Identity, ServerTlsConfig},
    Code,
};

#[derive(Clone)]
struct WebContentType(Channel);
impl tower::Service<http::Request<tonic::body::Body>> for WebContentType {
    type Response = <Channel as tower::Service<http::Request<tonic::body::Body>>>::Response;
    type Error = <Channel as tower::Service<http::Request<tonic::body::Body>>>::Error;
    type Future = <Channel as tower::Service<http::Request<tonic::body::Body>>>::Future;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }
    fn call(&mut self, mut request: http::Request<tonic::body::Body>) -> Self::Future {
        request.headers_mut().insert(
            "content-type",
            "application/grpc-web+proto".parse().unwrap(),
        );
        self.0.call(request)
    }
}

fn authorized<T>(value: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(value);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}
fn tokens() -> AuthConfig {
    AuthConfig::static_bearer_tokens(
        StaticBearerTokens::new("operator-secret", "proxy-secret", "sidecar-secret").unwrap(),
    )
}
fn query(known: Option<u64>) -> pb::ResolveTlsCertificateRequest {
    pb::ResolveTlsCertificateRequest {
        server_name: "app.example".into(),
        known_view_revision: known,
    }
}
/// The control plane keeps the operator service on its own listener, so a test
/// that speaks both roles connects to both.
struct TestServer {
    workload_url: String,
    operator_url: String,
    ca: String,
}

async fn server(
    store: impl ControlPlaneStore + 'static,
    auth: AuthConfig,
    tls: bool,
    tasks: &mut tokio::task::JoinSet<()>,
) -> TestResult<TestServer> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let ca = cert.pem();
    // No Kubernetes request is valid in this test; certificate/HTTP01 methods
    // must not route, wake or materialize any instance.
    let kube = kube::Client::try_from(kube::Config::new("http://127.0.0.1:1".parse()?))?;
    let store = Arc::new(store);
    let materializer = KubernetesMaterializer::new(KubeMaterializerClient::new(kube));
    let target = MaterializationTarget::new("test", "test")?;
    let route_events = RouteSubscriptionBroker::new();
    let identity = || {
        tls.then(|| {
            ServerTlsConfig::new()
                .identity(Identity::from_pem(ca.clone(), signing_key.serialize_pem()))
        })
    };

    let workload_url = serve_router(
        control_plane::runtime::workload_router_with_tls(
            store.clone(),
            materializer.clone(),
            target.clone(),
            auth.clone(),
            route_events.clone(),
            identity(),
        )?,
        tls,
        tasks,
    )
    .await?;
    let operator_url = serve_router(
        control_plane::runtime::operator_router_with_tls(
            store,
            materializer,
            target,
            auth,
            route_events,
            identity(),
        )?,
        tls,
        tasks,
    )
    .await?;

    Ok(TestServer {
        workload_url,
        operator_url,
        ca,
    })
}

async fn serve_router(
    router: control_plane::runtime::NativeControlPlaneRouter,
    tls: bool,
    tasks: &mut tokio::task::JoinSet<()>,
) -> TestResult<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tasks.spawn(async move {
        router
            .serve_with_incoming(
                tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });
    Ok(format!(
        "{}://localhost:{}",
        if tls { "https" } else { "http" },
        addr.port()
    ))
}
async fn channel(endpoint: String, ca: Option<&str>) -> TestResult<Channel> {
    Ok(sleepypods_api::transport::native_endpoint(endpoint, ca)?
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .connect()
        .await?)
}
#[tokio::test]
async fn postgres_certificate_native_tls_role_boundary_and_http01() -> TestResult {
    database_test(|store, raw, _config| async move {
        let mut tasks = tokio::task::JoinSet::new();
        let result = async {
            let served = server(store.clone(), tokens(), true, &mut tasks).await?;
            let TestServer {
                workload_url,
                operator_url: url,
                ca,
            } = served;
            let connection = channel(url.clone(), Some(&ca)).await?;
            let mut operator = OperatorControlPlaneClient::new(connection.clone());
            let mut proxy =
                ProxyControlPlaneClient::new(channel(workload_url, Some(&ca)).await?);
            let material = material(&["app.example"]);
            let publication = pb::PublishCertificateRequest {
                certificate_id: "native".into(),
                expected_version: Some(0),
                bundle: Some(pb::CertificateBundle {
                    chain_der: material.chain_der().to_vec(),
                    private_key_pkcs8_der: material.private_key_pkcs8_der().to_vec(),
                }),
            };
            let mut web_request = authorized(publication.clone(), "operator-secret");
            web_request
                .metadata_mut()
                .insert("x-forwarded-proto", "https".parse()?);
            let web_error = OperatorControlPlaneClient::new(WebContentType(connection.clone()))
                .publish_certificate(web_request)
                .await
                .unwrap_err();
            assert_eq!(
                web_error.code(),
                Code::PermissionDenied,
                "even genuinely encrypted web material calls are denied"
            );
            for token in ["proxy-secret", "sidecar-secret", "wrong-secret"] {
                let err = operator
                    .publish_certificate(authorized(publication.clone(), token))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    err.code(),
                    Code::PermissionDenied | Code::Unauthenticated
                ));
            }
            assert_eq!(
                operator
                    .publish_certificate(publication.clone())
                    .await
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated
            );
            let meta = operator
                .publish_certificate(authorized(publication.clone(), "operator-secret"))
                .await?
                .into_inner();
            assert_eq!(meta.version, 1);
            assert_eq!(
                meta.leaf_sha256,
                ::ring::digest::digest(&::ring::digest::SHA256, &material.chain_der()[0]).as_ref()
            );
            let bound = operator
                .set_tls_binding(authorized(
                    pb::SetTlsBindingRequest {
                        hostname: "app.example".into(),
                        expected_revision: Some(0),
                        certificate_id: Some("native".into()),
                    },
                    "operator-secret",
                ))
                .await?
                .into_inner();
            for token in ["operator-secret", "sidecar-secret", "wrong-secret"] {
                let error = proxy
                    .resolve_tls_certificate(authorized(query(None), token))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error.code(),
                    Code::PermissionDenied | Code::Unauthenticated
                ));
            }
            assert_eq!(
                proxy
                    .resolve_tls_certificate(query(None))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated
            );
            let found = proxy
                .resolve_tls_certificate(authorized(query(None), "proxy-secret"))
                .await?
                .into_inner();
            assert_eq!(found.view_revision, bound.revision);
            assert!(found.authorization_ttl_millis > 0 && found.authorization_ttl_millis <= 300_000);
            let Some(pb::resolve_tls_certificate_response::Value::Found(found)) = found.value else {
                panic!("expected Found")
            };
            let b = found.bundle.unwrap();
            assert_eq!(b.chain_der, material.chain_der());
            assert!(b.private_key_pkcs8_der == material.private_key_pkcs8_der());
            let unchanged = proxy
                .resolve_tls_certificate(authorized(query(Some(bound.revision)), "proxy-secret"))
                .await?
                .into_inner();
            assert!(matches!(
                unchanged.value,
                Some(pb::resolve_tls_certificate_response::Value::Unchanged(_))
            ));
            let missing = proxy
                .resolve_tls_certificate(authorized(
                    pb::ResolveTlsCertificateRequest {
                        server_name: "missing.example".into(),
                        known_view_revision: None,
                    },
                    "proxy-secret",
                ))
                .await?
                .into_inner();
            assert_eq!(missing.view_revision, 0);
            assert!(matches!(
                missing.value,
                Some(pb::resolve_tls_certificate_response::Value::Missing(_))
            ));
            // Invalid rotation and absent CAS preserve the authoritative version.
            let mut invalid = publication.clone();
            invalid.expected_version = Some(1);
            invalid.bundle.as_mut().unwrap().private_key_pkcs8_der = b"PRIVATE-ERROR-MARKER".to_vec();
            let err = operator
                .publish_certificate(authorized(invalid, "operator-secret"))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument);
            assert!(!format!("{err:?}").contains("PRIVATE-ERROR-MARKER"));
            let mut missing_cas = publication.clone();
            missing_cas.expected_version = None;
            assert_eq!(
                operator
                    .publish_certificate(authorized(missing_cas, "operator-secret"))
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
            assert_eq!(
                operator
                    .get_certificate_metadata(authorized(
                        pb::GetCertificateMetadataRequest {
                            certificate_id: "native".into()
                        },
                        "operator-secret"
                    ))
                    .await?
                    .into_inner()
                    .version,
                1
            );
            // Decode and bundle limits reject without committing another version.
            let mut huge = publication.clone();
            huge.expected_version = Some(1);
            huge.bundle.as_mut().unwrap().private_key_pkcs8_der = vec![1; 300 * 1024];
            let error = operator
                .publish_certificate(authorized(huge, "operator-secret"))
                .await
                .unwrap_err();
            assert!(matches!(
                error.code(),
                Code::OutOfRange | Code::ResourceExhausted
            ));
            // Actual decryption error stays an RPC error, including conditional reads.
            raw.execute("UPDATE certificates SET sealed_private_key=set_byte(sealed_private_key,0,get_byte(sealed_private_key,0)#1) WHERE certificate_id='native'",&[]).await?;
            assert_eq!(
                proxy
                    .resolve_tls_certificate(authorized(query(Some(bound.revision)), "proxy-secret"))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Internal
            );
            // HTTP01 works through the proxy service with no certificate or route.
            let key = pb::Http01ChallengeKey {
                host: "before.example".into(),
                token: "proof".into(),
            };
            operator
                .put_http01_challenge(authorized(
                    pb::PutHttp01ChallengeRequest {
                        key: Some(key.clone()),
                        key_authorization: "proof.challenge".into(),
                        expires_at_unix_millis: 2_000_000_000_000,
                    },
                    "operator-secret",
                ))
                .await?;
            let request = pb::ResolveHttp01ChallengeRequest {
                key: Some(key.clone()),
            };
            assert_eq!(
                proxy
                    .resolve_http01_challenge(authorized(request.clone(), "proxy-secret"))
                    .await?
                    .into_inner()
                    .challenge
                    .unwrap()
                    .key_authorization,
                "proof.challenge"
            );
            raw.execute("UPDATE http01_challenges SET expires_at_unix_millis=1 WHERE host='before.example' AND token='proof'",&[]).await?;
            assert!(
                proxy
                    .resolve_http01_challenge(authorized(request.clone(), "proxy-secret"))
                    .await?
                    .into_inner()
                    .challenge
                    .is_none(),
                "expired row must be hidden before garbage collection"
            );
            operator
                .expire_http01_challenges(authorized(
                    pb::ExpireHttp01ChallengesRequest {
                        now_unix_millis: 2_000_000_000_000,
                        limit: Some(1),
                    },
                    "operator-secret",
                ))
                .await?;
            assert!(proxy
                .resolve_http01_challenge(authorized(request, "proxy-secret"))
                .await?
                .into_inner()
                .challenge
                .is_none());
            raw.batch_execute("ALTER TABLE http01_challenges RENAME TO http01_unavailable_fixture")
                .await?;
            assert_eq!(
                proxy
                    .resolve_http01_challenge(authorized(
                        pb::ResolveHttp01ChallengeRequest {
                            key: Some(key.clone())
                        },
                        "proxy-secret"
                    ))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Internal,
                "lookup failure must not become Missing"
            );
            assert_eq!(
                raw.query_one("SELECT count(*) FROM instances", &[])
                    .await?
                    .get::<_, i64>(0),
                0
            );
            // Actual wrong trust and wrong verified endpoint hostname fail setup.
            assert!(channel(url.clone(), None).await.is_err());
            assert!(channel(url.replace("localhost", "127.0.0.1"), Some(&ca))
                .await
                .is_err());
            for (auth, tls) in [
                (AuthConfig::NoAuth, true),
                (tokens(), false),
                (AuthConfig::NoAuth, false),
            ] {
                let served = server(store.clone(), auth, tls, &mut tasks).await?;
                let ca = served.ca;
                let trust = tls.then_some(ca.as_str());
                let connection = channel(served.operator_url, trust).await?;
                let workload_connection = channel(served.workload_url, trust).await?;
                let mut client = OperatorControlPlaneClient::new(connection.clone());
                let mut request = authorized(publication.clone(), "operator-secret");
                request
                    .metadata_mut()
                    .insert("x-forwarded-proto", "https".parse()?);
                assert_eq!(
                    client
                        .publish_certificate(request)
                        .await
                        .unwrap_err()
                        .code(),
                    Code::PermissionDenied
                );
                assert_eq!(
                    ProxyControlPlaneClient::new(workload_connection)
                        .resolve_tls_certificate(authorized(query(None), "proxy-secret"))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::PermissionDenied
                );
            }
            Ok(())
        }
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    })
    .await
}

#[tokio::test]
async fn postgres_cancelled_certificate_sql_retains_capacity_until_drained() -> TestResult {
    database_test(|_store, raw, mut config| async move {
        config.max_connections = 2;
        let store = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let (mut blocker, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { connection.await.unwrap() });
        let pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let tx = blocker.transaction().await?;
        tx.query_one(
            "SELECT revision FROM tls_certificate_revision WHERE singleton FOR UPDATE",
            &[],
        )
        .await?;
        let mut writes = tokio::task::JoinSet::new();
        let writer = store.clone();
        writes.spawn(async move {
            writer
                .publish_certificate(publish("cancelled", 0, &["cancelled.example"]))
                .await
        });
        let queued = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let count: i64 = raw
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'",
                        &[&pid],
                    )
                    .await?
                    .get(0);
                if count == 1 {
                    return Ok::<_, Box<dyn Error + Send + Sync>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        writes.abort_all();
        while writes.join_next().await.is_some() {}
        let second = tokio::time::timeout(
            Duration::from_millis(200),
            store.get_certificate_metadata(id("never-created")),
        )
        .await;
        let ordinary = tokio::time::timeout(
            Duration::from_secs(1),
            store.get_instance(GetInstanceRequest::new(
                InstanceId::new("ordinary").unwrap(),
            )),
        )
        .await;
        // Release the precise blocker and all task ownership before assessing
        // the controlled red result; a failed assertion cannot retain a lock.
        tx.rollback().await?;
        drop(blocker);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        queued??;
        assert!(
            matches!(second, Ok(Err(StoreError::Unavailable { .. }))),
            "a cancelled sent SQL command must still own certificate capacity; observed {second:?}"
        );
        assert!(
            matches!(ordinary, Ok(Ok(None))),
            "ordinary database work must retain capacity"
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match store.get_certificate_metadata(id("cancelled")).await {
                    Ok(None) => return,
                    Err(StoreError::Unavailable { .. }) => tokio::task::yield_now().await,
                    other => {
                        panic!("cancelled publication unexpectedly committed or failed: {other:?}")
                    }
                }
            }
        })
        .await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_certificate_stalled_drain_discards_dirty_session() -> TestResult {
    database_test(|_store, raw, mut config| async move {
        config.max_connections = 2;
        let store = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let (mut blocker, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { connection.await.unwrap() });
        let pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let tx = blocker.transaction().await?;
        tx.query_one(
            "SELECT revision FROM tls_certificate_revision WHERE singleton FOR UPDATE",
            &[],
        )
        .await?;
        let mut writes = tokio::task::JoinSet::new();
        let writer = store.clone();
        writes.spawn(async move {
            writer
                .publish_certificate(publish("cancelled", 0, &["cancelled.example"]))
                .await
        });
        let queued = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let count: i64 = raw
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'",
                        &[&pid],
                    )
                    .await?
                    .get(0);
                if count == 1 {
                    return Ok::<_, Box<dyn Error + Send + Sync>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        writes.abort_all();
        while writes.join_next().await.is_some() {}
        let second = tokio::time::timeout(
            Duration::from_millis(200),
            store.get_certificate_metadata(id("never-created")),
        )
        .await;
        // Keep the SQL lock held past the five-second drain bound. No ordinary
        // idle client has been opened; recycling the dirty client would leave
        // this metadata query queued behind that same still-held lock.
        let recovered = tokio::time::timeout(Duration::from_secs(7), async {
            loop {
                match store.get_certificate_metadata(id("never-created")).await {
                    Ok(None) => return true,
                    Err(StoreError::Unavailable { .. }) => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    _ => return false,
                }
            }
        })
        .await;
        // Release the precise blocker and all task ownership before assessing
        // the controlled red result; a failed assertion cannot retain a lock.
        tx.rollback().await?;
        drop(blocker);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        queued??;
        assert!(
            matches!(second, Ok(Err(StoreError::Unavailable { .. }))),
            "a cancelled sent SQL command must still own certificate capacity; observed {second:?}"
        );
        assert!(
            matches!(recovered, Ok(true)),
            "stalled session must be discarded before capacity returns"
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match store.get_certificate_metadata(id("cancelled")).await {
                    Ok(None) => return,
                    Err(StoreError::Unavailable { .. }) => tokio::task::yield_now().await,
                    other => {
                        panic!("cancelled publication unexpectedly committed or failed: {other:?}")
                    }
                }
            }
        })
        .await?;
        Ok(())
    })
    .await
}

#[path = "api/watch.rs"]
mod watch;
