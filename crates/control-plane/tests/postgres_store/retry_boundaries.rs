//! Each run-once operation commits, a second writer replaces what it touched,
//! and only then does the caller lose the answer. A repeat would act on the
//! replacement, so the store reports the uncertain outcome instead.
use super::*;
use control_plane::{materialization::*, DeleteHttp01ChallengeRequest, RetryPolicy, StoreResult};
use std::future::Future;

/// A store whose sessions pass through a relay that can lose an answer after
/// the database has acted on the request.
struct Lossy {
    relay: postgres_relay::Relay,
    store: PostgresStore,
}

impl Lossy {
    /// Runs `operation` and loses the answer to its request that carries
    /// `trigger`. While the relay holds that answer, `replace` runs as a
    /// second writer would.
    async fn run<T: std::fmt::Debug>(
        &self,
        trigger: &[u8],
        operation: impl Future<Output = StoreResult<T>>,
        replace: impl Future<Output = StoreResult<()>>,
    ) -> TestResult<StoreResult<T>> {
        self.relay.lose_answer_to(trigger);
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => {
                return Err(format!("the operation answered before its answer was lost: {result:?}").into());
            }
            () = self.relay.wait_until_answer_held() => {}
        }
        replace.await?;
        self.relay.drop_held_answer();
        Ok(operation.await)
    }
}

fn assert_one_shot<T: std::fmt::Debug>(result: StoreResult<T>) {
    assert!(
        matches!(result, Err(StoreError::Unavailable { .. })),
        "the uncertain outcome reaches the caller: {result:?}"
    );
}

#[tokio::test]
async fn postgres_retry_boundaries_preserve_real_replacements() -> TestResult {
    let Ok(base) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!(
            "skipping Postgres retry boundary conformance; SLEEPYPODS_POSTGRES_URL is required"
        );
        return Ok(());
    };
    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base, NoTls).await?;
    let task = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let config = PostgresStoreConfig::new(connection_url_with_search_path(&base, &schema))?;
    let pg = PostgresStore::connect(&config).await?;
    let relay = postgres_relay::Relay::start(&base).await?;
    let mut relayed =
        PostgresStoreConfig::new(connection_url_with_search_path(&relay.url(&base)?, &schema))?;
    relayed.max_connections = 1;
    let lossy = Lossy {
        store: PostgresStore::connect(&relayed)
            .await?
            .with_retry_policy(RetryPolicy::new(3, Duration::ZERO, Duration::ZERO)),
        relay,
    };
    let result = replacement_checks(&pg, &lossy).await;
    drop((pg, lossy));
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    task.abort();
    result
}

async fn replacement_checks(pg: &PostgresStore, lossy: &Lossy) -> TestResult {
    super::transport_support::lifecycle_conformance(pg).await?;
    let class = workload_class("retry-boundary-class", 1);
    pg.create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    let owner = pg
        .create_instance(create_instance_request(
            "retry-owner",
            class.reference.clone(),
            vec![],
        ))
        .await?
        .instance;
    let target = MaterializationTarget::new("retry-cluster", "apps")?;

    // Force operations are ID or key scoped. A second writer installs a new
    // projection or key owner after commit, before the old caller observes
    // response loss; an automatic replay would act on that replacement.
    for operation in ["force-delete", "force-release"] {
        let instance = pg
            .create_instance(create_instance_request(
                operation,
                class.reference.clone(),
                vec![],
            ))
            .await?
            .instance;
        let key = vec![RenderedExclusivityKey {
            name: "storage".into(),
            value: operation.into(),
        }];
        let initial = accept_projection(pg, &instance, &target, vec![], key.clone()).await?;
        let replacement_refs = vec![RenderedObjectRef {
            api_version: "v1".into(),
            kind: "Service".into(),
            namespace: "apps".into(),
            name: format!("{operation}-replacement"),
        }];
        let replacement_owner = if operation == "force-delete" {
            instance.id.clone()
        } else {
            pg.create_instance(create_instance_request(
                &format!("{operation}-replacement"),
                class.reference.clone(),
                vec![],
            ))
            .await?
            .instance
            .id
        };
        let replace = async {
            let current = pg
                .get_instance(GetInstanceRequest::new(replacement_owner.clone()))
                .await?
                .expect("the replacement owner exists");
            let replacement =
                accept_projection(pg, &current, &target, replacement_refs, key.clone()).await?;
            claim_row(pg, &replacement, "replacement-owner").await?;
            Ok(())
        };
        let result = match operation {
            "force-delete" => lossy
                .run(
                    b"COMMIT",
                    lossy.store.force_delete_materialization(
                        ForceDeleteMaterializationRequest::new(
                            initial.id.clone(),
                            "operator",
                            "old process fenced",
                        ),
                    ),
                    replace,
                )
                .await?
                .map(|_| ()),
            "force-release" => lossy
                .run(
                    b"COMMIT",
                    lossy.store.force_release_exclusivity_key(
                        ForceReleaseExclusivityKeyRequest::new(
                            target.clone(),
                            "storage",
                            operation,
                            "operator",
                            "old process fenced",
                        ),
                    ),
                    replace,
                )
                .await?
                .map(|_| ()),
            _ => unreachable!(),
        };
        assert_one_shot(result);
        let current = pg
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                replacement_owner,
                target.clone(),
            ))
            .await?
            .expect("the replacement projection survives response loss");
        assert_eq!(current.state, MaterializationState::Pending);
        assert_eq!(
            current.rendered_objects[0].name,
            format!("{operation}-replacement")
        );
        assert_eq!(
            current.exclusivity_keys, key,
            "replacement ownership survives response loss"
        );
        assert_eq!(
            current.reconciliation_lease.unwrap().owner,
            "replacement-owner"
        );
    }

    let original_route = create_route_binding_request(
        "delete-route",
        owner.id.as_str(),
        http_identity("old.retry.example", None),
    );
    pg.create_route_binding(original_route.clone()).await?;
    let replacement_route = create_route_binding_request(
        "delete-route",
        owner.id.as_str(),
        http_identity("new.retry.example", None),
    );
    assert_one_shot(
        lossy
            .run(
                b"delete-route",
                lossy
                    .store
                    .delete_route_binding(DeleteRouteBindingRequest::new(
                        original_route.route_binding_id,
                    )),
                async {
                    pg.create_route_binding(replacement_route.clone()).await?;
                    Ok(())
                },
            )
            .await?,
    );
    assert_eq!(
        pg.get_route_binding(GetRouteBindingRequest::new(
            replacement_route.route_binding_id
        ))
        .await?
        .unwrap()
        .identity,
        replacement_route.identity
    );

    for operation in ["put", "delete"] {
        let key = Http01ChallengeKey::new("retry.example", operation)?;
        let now = SystemTime::now();
        let original = PutHttp01ChallengeRequest::new(
            key.clone(),
            "old-authorization",
            now + Duration::from_secs(300),
            now,
        )?;
        pg.put_http01_challenge(original.clone()).await?;
        let replacement = PutHttp01ChallengeRequest::new(
            key.clone(),
            "replacement-authorization",
            now + Duration::from_secs(600),
            now,
        )?;
        let replace = async {
            pg.put_http01_challenge(replacement).await?;
            Ok(())
        };
        if operation == "put" {
            assert_one_shot(
                lossy
                    .run(
                        b"old-authorization",
                        lossy.store.put_http01_challenge(original),
                        replace,
                    )
                    .await?,
            );
        } else {
            assert_one_shot(
                lossy
                    .run(
                        b"retry.example",
                        lossy
                            .store
                            .delete_http01_challenge(DeleteHttp01ChallengeRequest::new(
                                key.clone(),
                            )),
                        replace,
                    )
                    .await?,
            );
        }
        assert_eq!(
            pg.resolve_http01_challenge(key)
                .await?
                .unwrap()
                .key_authorization(),
            "replacement-authorization"
        );
    }

    // A repeated create makes a deleted resource again, so the store sends a
    // create once: a delete that lands after a lost answer stays in effect.
    let create = create_instance_request("create-lost", class.reference.clone(), vec![]);
    assert_one_shot(
        lossy
            .run(
                b"COMMIT",
                lossy.store.create_instance(create.clone()),
                async {
                    let current = pg
                        .get_instance(GetInstanceRequest::new(create.instance_id.clone()))
                        .await?
                        .unwrap();
                    pg.request_instance_deletion(
                        control_plane::instance::RequestInstanceDeletion {
                            instance_id: create.instance_id.clone(),
                            expected_generation: current.generation,
                        },
                    )
                    .await?;
                    pg.finalize_instance_deletions(1024).await?;
                    Ok(())
                },
            )
            .await?,
    );
    assert!(pg
        .get_instance(GetInstanceRequest::new(create.instance_id.clone()))
        .await?
        .is_none());
    let deliberately_recreated = pg.create_instance(create).await?.instance;
    assert!(
        deliberately_recreated.generation > Generation::new(0),
        "a caller's own repeat starts a new incarnation"
    );

    let create = create_route_binding_request(
        "create-route-lost",
        owner.id.as_str(),
        http_identity("create.retry.example", None),
    );
    assert_one_shot(
        lossy
            .run(
                b"COMMIT",
                lossy.store.create_route_binding(create.clone()),
                async {
                    pg.delete_route_binding(DeleteRouteBindingRequest::new(
                        create.route_binding_id.clone(),
                    ))
                    .await?;
                    Ok(())
                },
            )
            .await?,
    );
    assert!(pg
        .get_route_binding(GetRouteBindingRequest::new(create.route_binding_id.clone()))
        .await?
        .is_none());
    assert_eq!(
        pg.create_route_binding(create.clone()).await?.id,
        create.route_binding_id,
        "a caller's own repeat binds the route again"
    );
    exact_fence_retries(pg, lossy, &class, &target).await?;
    Ok(())
}

async fn exact_fence_retries(
    pg: &PostgresStore,
    lossy: &Lossy,
    class: &WorkloadClassVersion,
    target: &MaterializationTarget,
) -> TestResult {
    let instance = pg
        .create_instance(create_instance_request(
            "retry-fenced",
            class.reference.clone(),
            vec![],
        ))
        .await?
        .instance;
    let pending = accept_projection(pg, &instance, target, vec![], vec![]).await?;
    let claimed = claim_row(pg, &pending, "fenced-owner").await?;
    let attempt = claimed.reconciliation_lease.as_ref().unwrap().attempt;
    let failure = lossy.store.record_materialization_failure(
        control_plane::runtime_work::RecordMaterializationFailure {
            expected_state: control_plane::MaterializationState::Pending,
            materialization_id: pending.id.clone(),
            owner: "fenced-owner".into(),
            attempt,
            generation: pending.instance_generation,
            permanent: false,
            message: "temporary failure".into(),
        },
    );
    assert!(
        !lossy.run(b"COMMIT", failure, async { Ok(()) }).await??,
        "a repeated failure finds the lease it released and changes nothing"
    );
    assert_eq!(
        pg.load_materialization_work_status(pending.id)
            .await?
            .unwrap()
            .failure_count,
        1
    );
    Ok(())
}
