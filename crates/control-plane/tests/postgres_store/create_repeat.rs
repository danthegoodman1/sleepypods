//! A create compares with the stored resource: a request that describes it
//! returns it, and any other request for the ID is refused. These cases cover a
//! create after a delete, a repeat that races the first create, and the
//! operator RPC.
use super::*;
use control_plane::{
    api::{pb, RouteSubscriptionBroker},
    AuthConfig, KubeMaterializerClient, KubernetesMaterializer,
};
use std::{future::Future, sync::Arc};
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::wrappers::TcpListenerStream;

type CreateTask = JoinHandle<Result<control_plane::CreateInstanceResult, StoreError>>;

async fn with_store<F, Fut>(test: F) -> TestResult
where
    F: FnOnce(PostgresStore, PostgresStoreConfig, WorkloadClassVersion) -> Fut,
    Fut: Future<Output = TestResult>,
{
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping repeated-create conformance: run scripts/test-postgres-store.sh");
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base_url, NoTls).await?;
    let admin_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base_url, &schema))?;
    let result = async {
        let store = PostgresStore::connect(&config).await?;
        let class = workload_class("repeat-class", 1);
        store
            .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
            .await?;
        test(store, config.clone(), class).await
    }
    .await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    admin_task.abort();
    result
}

fn route(route_binding_id: &str, instance_id: &str, host: &str) -> CreateRouteBindingRequest {
    create_route_binding_request(
        route_binding_id,
        instance_id,
        http_identity(host, None),
        ProtocolRoute::Http,
    )
}

fn spawn_create(store: &PostgresStore, request: &CreateInstanceRequest) -> CreateTask {
    let (store, request) = (store.clone(), request.clone());
    tokio::spawn(async move { store.create_instance(request).await })
}

/// The lowest generation a new incarnation of the instance ID may take, less one.
async fn generation_floor(raw: &impl tokio_postgres::GenericClient, instance_id: &str) -> u64 {
    raw.query_one(
        "SELECT last_generation FROM instance_generation_watermarks WHERE instance_id = $1",
        &[&instance_id],
    )
    .await
    .expect("the instance ID has a generation floor")
    .get::<_, i64>(0) as u64
}

/// Holds the route table so that a create stays uncommitted after it has
/// inserted its instance, starts `creates`, and waits until one of them holds
/// that open transaction while another waits for it to finish.
async fn start_while_first_create_is_uncommitted(
    raw: &mut tokio_postgres::Client,
    creates: impl FnOnce() -> Vec<CreateTask>,
) -> TestResult<Vec<CreateTask>> {
    let hold = raw.transaction().await?;
    hold.batch_execute("LOCK TABLE route_bindings IN SHARE ROW EXCLUSIVE MODE")
        .await?;
    let creates = creates();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let waiting: i64 = hold
            .query_one(
                "
                SELECT count(*) FROM pg_locks waiter
                WHERE NOT waiter.granted AND waiter.locktype = 'transactionid'
                    AND waiter.transactionid IN (
                        SELECT first.transactionid FROM pg_locks first
                        JOIN pg_locks blocked ON blocked.pid = first.pid
                        WHERE first.locktype = 'transactionid' AND first.granted
                            AND NOT blocked.granted
                            AND blocked.relation = 'route_bindings'::regclass
                    )
                ",
                &[],
            )
            .await?
            .get(0);
        if waiting == 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("no create waited behind an uncommitted first create".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(creates.iter().all(|create| !create.is_finished()));
    hold.commit().await?;
    Ok(creates)
}

#[tokio::test]
async fn postgres_create_after_delete_starts_a_fenced_incarnation() -> TestResult {
    with_store(|store, config, class| async move {
        let request = create_instance_request(
            "recreated",
            class.reference.clone(),
            vec![http_identity("recreated.example.com", None)],
        );
        let created = store.create_instance(request.clone()).await?;
        let deleting = store
            .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
                created.instance.id.clone(),
                created.instance.generation,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ))
            .await?;

        // Until its deletion finishes, the instance holds its ID.
        assert_eq!(
            store.create_instance(request.clone()).await?.instance,
            deleting
        );
        assert!(
            store
                .delete_instance(DeleteInstanceRequest::new(deleting.id.clone()))
                .await?
        );

        let recreated = store.create_instance(request.clone()).await?;
        assert_eq!(recreated.instance.state, InstanceState::Cold);
        assert_eq!(recreated.instance.generation, deleting.generation.next());
        assert_eq!(recreated.route_bindings, created.route_bindings);
        for stale in [created.instance.generation, deleting.generation] {
            let acted = store
                .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
                    recreated.instance.id.clone(),
                    stale,
                    InstanceState::Deleting,
                    StateTransitionReason::DeleteRequested,
                ))
                .await;
            assert!(
                matches!(acted, Err(StoreError::GenerationConflict { .. })),
                "generation {stale:?} of the deleted incarnation acted on its replacement"
            );
        }

        // A repeat leaves the generation floor where the instance put it.
        let raw = raw_client(&config).await?;
        for _ in 0..3 {
            assert_eq!(store.create_instance(request.clone()).await?, recreated);
        }
        assert_eq!(
            generation_floor(&raw, "recreated").await,
            recreated.instance.generation.get()
        );

        let late = route("late-route", "recreated", "late.example.com");
        let bound = store.create_route_binding(late.clone()).await?;
        assert_eq!(store.create_route_binding(late.clone()).await?, bound);
        assert!(
            store
                .delete_route_binding(DeleteRouteBindingRequest::new(bound.id.clone()))
                .await?
        );
        assert_eq!(
            store.create_route_binding(late).await?,
            bound,
            "a create after a delete binds the route again"
        );

        // The instance now has a route the original request lacks.
        assert!(matches!(
            store.create_instance(request).await,
            Err(StoreError::AlreadyExists {
                resource: "instance"
            })
        ));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_repeat_racing_the_first_create_returns_its_instance() -> TestResult {
    with_store(|store, config, class| async move {
        let mut raw = raw_client(&config).await?;

        let request = create_instance_request(
            "raced",
            class.reference.clone(),
            vec![http_identity("raced.example.com", None)],
        );
        let creates = start_while_first_create_is_uncommitted(&mut raw, || {
            vec![
                spawn_create(&store, &request),
                spawn_create(&store, &request),
            ]
        })
        .await?;
        let mut results = Vec::new();
        for create in creates {
            results.push(create.await??);
        }
        assert_eq!(results[0], results[1]);
        assert_eq!(results[0].instance.generation, Generation::new(0));
        assert_eq!(results[0].route_bindings.len(), 1);
        assert_eq!(generation_floor(&raw, "raced").await, 0);

        // Two different requests race for one ID: one wins, and the other is
        // refused and binds nothing.
        let requests = ["contested-a.example.com", "contested-b.example.com"].map(|host| {
            create_instance_request(
                "contested",
                class.reference.clone(),
                vec![http_identity(host, None)],
            )
        });
        let creates = start_while_first_create_is_uncommitted(&mut raw, || {
            requests
                .iter()
                .map(|request| spawn_create(&store, request))
                .collect()
        })
        .await?;
        let mut winners = Vec::new();
        for (create, request) in creates.into_iter().zip(&requests) {
            match create.await? {
                Ok(created) => winners.push((created, request)),
                Err(StoreError::AlreadyExists {
                    resource: "instance",
                }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let [(winner, winning_request)] = &winners[..] else {
            return Err(format!("{} of two different creates won", winners.len()).into());
        };
        assert_eq!(
            store
                .list_route_bindings_for_instance(
                    control_plane::ListRouteBindingsForInstanceRequest::new(
                        winner.instance.id.clone()
                    )
                )
                .await?,
            winner.route_bindings
        );
        assert_eq!(
            store.create_instance((*winning_request).clone()).await?,
            *winner
        );
        assert_eq!(generation_floor(&raw, "contested").await, 0);

        // Unsynchronized repeats agree as well.
        let burst = create_instance_request(
            "burst",
            class.reference.clone(),
            vec![http_identity("burst.example.com", None)],
        );
        let creates = (0..16)
            .map(|_| spawn_create(&store, &burst))
            .collect::<Vec<_>>();
        let mut first = None;
        for create in creates {
            let created = create.await??;
            assert_eq!(first.get_or_insert_with(|| created.clone()), &created);
        }
        assert_eq!(generation_floor(&raw, "burst").await, 0);

        let binding = route("raced-route", "burst", "raced-route.example.com");
        let binds = (0..16)
            .map(|_| {
                let (store, binding) = (store.clone(), binding.clone());
                tokio::spawn(async move { store.create_route_binding(binding).await })
            })
            .collect::<Vec<_>>();
        let mut first = None;
        for bind in binds {
            let bound = bind.await??;
            assert_eq!(first.get_or_insert_with(|| bound.clone()), &bound);
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_operator_api_answers_repeated_creates() -> TestResult {
    with_store(|store, _config, class| async move {
        // No call in this test reaches Kubernetes.
        let kube = kube::Client::try_from(kube::Config::new("http://127.0.0.1:1".parse()?))?;
        let router = control_plane::runtime::operator_router_with_tls(
            Arc::new(store),
            KubernetesMaterializer::new(KubeMaterializerClient::new(kube)),
            MaterializationTarget::new("test", "apps")?,
            AuthConfig::NoAuth,
            RouteSubscriptionBroker::new(),
            None,
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
        let mut operator = pb::operator_control_plane_client::OperatorControlPlaneClient::new(
            tonic::transport::Channel::from_shared(endpoint)?
                .connect()
                .await?,
        );

        let request = |tenant: &str, hosts: &[&str]| pb::CreateInstanceRequest {
            instance_id: "api-instance".to_owned(),
            workload_class: Some(pb::WorkloadClassVersionRef {
                class_id: class.reference.class_id.as_str().to_owned(),
                version: class.reference.version.get(),
            }),
            values: [("tenant".to_owned(), tenant.to_owned())].into(),
            routes: hosts
                .iter()
                .enumerate()
                .map(|(index, host)| pb::InstanceRoute {
                    route_binding_id: format!("api-route-{index}"),
                    identity: Some(pb::RouteIdentity {
                        kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                            host: Some(pb::RouteHost {
                                kind: pb::RouteHostKind::Exact as i32,
                                host: (*host).to_owned(),
                            }),
                            path_prefix: None,
                        })),
                    }),
                })
                .collect(),
        };
        let original = request("api-instance", &["api.example.com"]);
        let created = operator
            .create_instance(original.clone())
            .await?
            .into_inner();
        assert_eq!(
            operator
                .create_instance(original.clone())
                .await?
                .into_inner(),
            created
        );
        for different in [
            request("other-tenant", &["api.example.com"]),
            request("api-instance", &[]),
            request("api-instance", &["other.example.com"]),
            request("api-instance", &["api.example.com", "more.example.com"]),
        ] {
            let refused = operator
                .create_instance(different)
                .await
                .expect_err("a different request for an existing instance ID is refused");
            assert_eq!(refused.code(), tonic::Code::AlreadyExists);
            assert_eq!(refused.message(), "instance already exists");
        }
        assert_eq!(
            operator.create_instance(original).await?.into_inner(),
            created
        );
        server.abort();
        Ok(())
    })
    .await
}
