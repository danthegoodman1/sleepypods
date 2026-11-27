//! Route creation keeps every name's rule set within one Subscribe answer: at
//! most `MAX_HOST_RULES` rules and `MAX_HOST_RULES_BYTES` counted bytes. A
//! name's set holds its exact bindings and the wildcard bindings on its
//! suffixes, so both kinds of binding count toward it.
use super::*;
use control_plane::{
    RouteKind, HOST_RULE_OVERHEAD_BYTES, MAX_HOST_RULES, MAX_HOST_RULES_BYTES, MAX_ROUTE_PATH_BYTES,
};
use std::future::Future;

const INSTANCE: &str = "bounded";

async fn with_store<F, Fut>(test: F) -> TestResult
where
    F: FnOnce(PostgresStore, WorkloadClassVersionRef) -> Fut,
    Fut: Future<Output = TestResult>,
{
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!("skipping host rule bounds: run scripts/test-postgres-store.sh");
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
        let class = workload_class("bounded-class", 1);
        store
            .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
            .await?;
        store
            .create_instance(create_instance_request(
                INSTANCE,
                class.reference.clone(),
                Vec::new(),
            ))
            .await?;
        test(store, class.reference).await
    }
    .await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    admin_task.abort();
    result
}

fn wildcard_http(suffix: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(suffix).expect("valid suffix"),
        path: Some(PathPrefix::new(path).expect("valid path prefix")),
    }
}

async fn bind(
    store: &PostgresStore,
    route_binding_id: &str,
    identity: RouteIdentity,
) -> Result<control_plane::RouteBindingRecord, StoreError> {
    store
        .create_route_binding(create_route_binding_request(
            route_binding_id,
            INSTANCE,
            identity,
        ))
        .await
}

async fn rule_count(store: &PostgresStore, host: &str) -> TestResult<usize> {
    let rules = store
        .resolve_host_rules(ResolveHostRulesRequest::new(
            RouteKind::Http,
            RouteHost::exact(host)?,
            MaterializationTarget::new("test", "apps")?,
        ))
        .await?;
    Ok(rules.len())
}

#[track_caller]
fn assert_refused<T: std::fmt::Debug>(result: Result<T, StoreError>, expected: &str) {
    match result {
        Err(StoreError::InvalidArgument { message }) => assert!(
            message.contains(expected),
            "message {message:?} should contain {expected:?}"
        ),
        other => panic!("expected a refusal containing {expected:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn exact_and_wildcard_bindings_share_each_name_rule_limit() -> TestResult {
    with_store(|store, class| async move {
        let wildcards = MAX_HOST_RULES - 56;
        for index in 0..wildcards {
            bind(
                &store,
                &format!("wild-{index}"),
                wildcard_http("example.com", &format!("/w{index}")),
            )
            .await?;
        }
        for index in 0..56 {
            bind(
                &store,
                &format!("app-{index}"),
                http_identity("app.example.com", Some(&format!("/e{index}"))),
            )
            .await?;
        }
        assert_eq!(rule_count(&store, "app.example.com").await?, MAX_HOST_RULES);

        let full = "would give host app.example.com more than 256 route rules";
        assert_refused(
            bind(
                &store,
                "app-56",
                http_identity("app.example.com", Some("/e56")),
            )
            .await,
            full,
        );
        assert_refused(
            bind(&store, "wild-more", wildcard_http("example.com", "/more")).await,
            full,
        );
        assert_refused(
            store
                .create_instance(create_instance_request(
                    "crowded",
                    class.clone(),
                    vec![http_identity("app.example.com", Some("/crowded"))],
                ))
                .await,
            full,
        );
        assert!(store
            .get_instance(GetInstanceRequest::new(InstanceId::new("crowded")?))
            .await?
            .is_none());

        // A repeat of a stored binding returns it, and other hosts and kinds
        // keep their own room.
        let stored = bind(
            &store,
            "app-0",
            http_identity("app.example.com", Some("/e0")),
        )
        .await?;
        assert_eq!(stored.id.as_str(), "app-0");
        bind(&store, "other-0", http_identity("other.example.com", None)).await?;
        bind(&store, "sni-app", sni_identity("app.example.com")).await?;

        // Wildcards below the suffix fill the names under them, bound or not.
        for index in 0..55 {
            bind(
                &store,
                &format!("deep-{index}"),
                wildcard_http("deep.example.com", &format!("/d{index}")),
            )
            .await?;
        }
        assert_eq!(
            rule_count(&store, "x.deep.example.com").await?,
            MAX_HOST_RULES - 1
        );
        bind(&store, "deep-55", wildcard_http("deep.example.com", "/d55")).await?;
        assert_eq!(
            rule_count(&store, "x.deep.example.com").await?,
            MAX_HOST_RULES
        );
        assert_refused(
            bind(&store, "deep-56", wildcard_http("deep.example.com", "/d56")).await,
            "would give the names under deep.example.com more than 256 route rules",
        );

        // Deleting a binding makes room for another.
        store
            .delete_route_binding(DeleteRouteBindingRequest::new(RouteBindingId::new(
                "app-55",
            )?))
            .await?;
        bind(
            &store,
            "app-56",
            http_identity("app.example.com", Some("/e56")),
        )
        .await?;
        assert_eq!(rule_count(&store, "app.example.com").await?, MAX_HOST_RULES);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_name_rule_set_counts_at_most_one_answer_of_bytes() -> TestResult {
    with_store(|store, _class| async move {
        let host = "bytes.example.com";
        let path = |index: usize| format!("/{index:a>width$}", width = MAX_ROUTE_PATH_BYTES - 1);
        let mut counted = 0;
        let mut index = 0;
        loop {
            let route_binding_id = format!("bytes-{index:03}");
            let bytes = route_binding_id.len()
                + INSTANCE.len()
                + host.len()
                + MAX_ROUTE_PATH_BYTES
                + HOST_RULE_OVERHEAD_BYTES;
            let created = bind(
                &store,
                &route_binding_id,
                http_identity(host, Some(&path(index))),
            )
            .await;
            if counted + bytes > MAX_HOST_RULES_BYTES {
                assert_refused(
                    created,
                    "would give host bytes.example.com route rules over 1048576 bytes",
                );
                break;
            }
            created?;
            counted += bytes;
            index += 1;
        }
        assert!(index < MAX_HOST_RULES, "{index} rules fit");
        assert_eq!(rule_count(&store, host).await?, index);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_creates_leave_a_name_within_its_rule_limit() -> TestResult {
    with_store(|store, _class| async move {
        let host = "raced.example.com";
        for index in 0..MAX_HOST_RULES - 1 {
            bind(
                &store,
                &format!("raced-{index}"),
                http_identity(host, Some(&format!("/r{index}"))),
            )
            .await?;
        }
        let creates = (0..16)
            .map(|racer| {
                let store = store.clone();
                tokio::spawn(async move {
                    bind(
                        &store,
                        &format!("racer-{racer}"),
                        http_identity(host, Some(&format!("/racer{racer}"))),
                    )
                    .await
                })
            })
            .collect::<Vec<_>>();
        let mut created = 0;
        for create in creates {
            match create.await? {
                Ok(_) => created += 1,
                refused => assert_refused(refused, "more than 256 route rules"),
            }
        }
        assert_eq!(created, 1);
        assert_eq!(rule_count(&store, host).await?, MAX_HOST_RULES);
        Ok(())
    })
    .await
}
