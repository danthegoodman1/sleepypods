//! Database round trips per lifecycle operation, counted at a relay in front
//! of Postgres once the session has served each operation before. Each budget
//! is half the count the store took before it prepared its statements and
//! folded its guards into the writes: 16, 20 and 14.
use super::lifecycle_recovery::{accepted, class, running, target, with_store, LEASE};
use super::*;
use control_plane::{
    api::{pb, StoreBackedProxyApi, StoreBackedSidecarApi},
    KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
    KubernetesMaterializerClient, KubernetesObject,
};
use pb::{
    proxy_control_plane_server::ProxyControlPlane,
    sidecar_control_plane_server::SidecarControlPlane,
};
use std::sync::Arc;

/// A Kubernetes client whose objects exist and whose reporting Pod is a
/// member of its projection.
#[derive(Clone, Default)]
struct MemberPods;

impl KubernetesMaterializerClient for MemberPods {
    fn apply_object<'a>(
        &'a self,
        _object: &'a KubernetesObject,
        _precondition: Option<&'a control_plane::projection::LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }
    fn delete_object<'a>(
        &'a self,
        _object: &'a RenderedObjectRef,
        _precondition: &'a control_plane::projection::LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }
    fn wait_for_readiness<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async { Ok(BackendEndpoint::new("http://ready.apps:80").unwrap()) })
    }
    fn verify_idle_member<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
        _identity: &'a control_plane::materializer::IdleMemberIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Round trips a proxy's wake of a Cold instance may take.
const WAKE_BUDGET: u64 = 8;
/// Round trips a sidecar's accepted idle report may take.
const IDLE_BUDGET: u64 = 10;
/// Round trips the lease check around one Kubernetes mutation may take.
const FENCING_BUDGET: u64 = 7;

/// Round trips `operation` takes on `relay`.
async fn round_trips<T>(
    relay: &postgres_relay::Relay,
    operation: impl std::future::Future<Output = T>,
) -> (u64, T) {
    let before = relay.round_trips();
    let value = operation.await;
    (relay.round_trips() - before, value)
}

#[tokio::test]
async fn postgres_lifecycle_operations_fit_their_round_trip_budgets() -> TestResult {
    with_store(|store, raw| async move {
        let base = std::env::var("SLEEPYPODS_POSTGRES_URL")?;
        let schema: String = raw
            .query_one("SELECT current_schema()::text", &[])
            .await?
            .get(0);
        let relay = postgres_relay::Relay::start(&base).await?;
        let mut relayed = PostgresStoreConfig::new(connection_url_with_search_path(
            &relay.url(&base)?,
            &schema,
        ))?;
        // One session serves every operation, so its prepared statements are
        // the ones each measurement reuses.
        relayed.max_connections = 1;
        let relayed: Arc<dyn ControlPlaneStore> = Arc::new(
            PostgresStore::connect(&relayed).await?,
        );
        let class = class(&store, "round-trip-class").await?;
        // Each operation has a target of its own, so a claim finds only its row.
        let (wake_target, idle_target, renew_target) = (
            target("round-trip-wake"),
            target("round-trip-idle"),
            target("round-trip-renew"),
        );
        let proxy = StoreBackedProxyApi::new(
            relayed.clone(),
            KubernetesMaterializer::new(MemberPods),
            wake_target,
        );
        let sidecar = StoreBackedSidecarApi::new(
            relayed.clone(),
            KubernetesMaterializer::new(MemberPods),
            idle_target.clone(),
        );

        let mut counts = Vec::new();
        for pass in ["warm", "measured"] {
            let cold = store
                .create_instance(create_instance_request(
                    &format!("round-trip-wake-{pass}"),
                    class.reference.clone(),
                    vec![],
                ))
                .await?
                .instance;
            let (wake, response) = round_trips(
                &relay,
                proxy.wake_instance(tonic::Request::new(pb::ProxyWakeInstanceRequest {
                    instance_id: cold.id.as_str().to_owned(),
                    expected_generation: cold.generation.get(),
                    backend_generation: None,
                })),
            )
            .await;
            response?;

            let awake =
                running(&store, &class, &idle_target, &format!("round-trip-idle-{pass}")).await?;
            raw.execute(
                "UPDATE materializations SET state_entered_at_unix_millis = state_entered_at_unix_millis - 600000 WHERE materialization_id = $1",
                &[&awake.materialization.id.as_str()],
            )
            .await?;
            let (idle, response) = round_trips(
                &relay,
                sidecar.report_idle(tonic::Request::new(pb::SidecarReportIdleRequest {
                    instance_id: awake.instance.id.as_str().to_owned(),
                    expected_generation: awake.materialization.projection_generation.get(),
                    active_count: 0,
                    pod_uid: "pod-uid".to_owned(),
                })),
            )
            .await;
            let response = response?.into_inner();
            assert!(
                matches!(
                    response.outcome,
                    Some(pb::sidecar_report_idle_response::Outcome::Accepted(_))
                ),
                "{response:?}"
            );

            let pending =
                accepted(&store, &class, &renew_target, &format!("round-trip-renew-{pass}"))
                    .await?;
            let claimed =
                claim_next(&store, MaterializationState::Pending, &renew_target, "driver", LEASE)
                .await?
                .ok_or("no Pending row is due")?;
            assert_eq!(claimed.id, pending.id);
            let (renew, renewed) = round_trips(
                &relay,
                relayed.renew_materialization_reconciliation_lease(
                    RenewMaterializationReconciliationLeaseRequest::new(
                        claimed.id.clone(),
                        "driver",
                        claimed.reconciliation_lease.as_ref().unwrap().attempt,
                        claimed.instance_generation,
                        LEASE,
                        MaterializationState::Pending,
                    ),
                ),
            )
            .await;
            assert!(renewed?);
            counts.push((pass, wake, idle, renew));
        }
        eprintln!("round trips (wake acceptance, idle report, fencing one mutation): {counts:?}");
        let (_, wake, idle, renew) = counts[1];
        assert!(wake <= WAKE_BUDGET, "wake acceptance took {wake} round trips");
        assert!(idle <= IDLE_BUDGET, "an idle report took {idle} round trips");
        assert!(
            renew <= FENCING_BUDGET,
            "fencing one mutation took {renew} round trips"
        );
        Ok(())
    })
    .await
}
