mod fenced_client;

use std::{
    cmp, fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use sleepypods_observability::{
    metrics::{
        KUBERNETES_OPERATIONS_TOTAL, KUBERNETES_OPERATION_DURATION_SECONDS,
        RECONCILER_CANDIDATES_TOTAL, RECONCILER_CLAIMS_TOTAL, RECONCILER_LEASE_RENEWALS_TOTAL,
        RECONCILER_RUNS_TOTAL, RECONCILER_RUN_DURATION_SECONDS,
        RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
    },
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
        EVENT_MATERIALIZATION_FAILURE,
    },
    Operation, Outcome,
};
use tokio::{sync::watch, task::JoinSet};

use crate::{
    api::RouteSubscriptionBroker,
    ids::InstanceId,
    instance::{GetInstanceRequest, InstanceRecord, InstanceState},
    manifest::{render_manifests_with_options, RenderManifestRequest},
    materialization::{
        BackendEndpoint, ClaimMaterializationReconciliationRequest,
        CompleteWakeReconciliationRequest, CompleteWakeRequest,
        DeleteMaterializationReconciliationRequest, FinalizeSleepReconciliationRequest,
        FinalizeSleepRequest, ListMaterializationReconciliationCandidatesRequest,
        MaterializationRecord, MaterializationState,
        ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient, MaterializerError},
    projection::{ProjectionError, ProjectionPlan, ProjectionReconciler},
    store::{ControlPlaneStore, StoreError},
    workload::LoadWorkloadClassVersionRequest,
};

pub const EVENT_MATERIALIZATION_RECONCILIATION: &str = "runtime.materialization.reconciliation";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationReconcilerConfig {
    pub owner: String,
    pub interval: Duration,
    pub lease_ttl: Duration,
    pub batch_size: usize,
    pub concurrency_limit: usize,
}

pub struct MaterializationReconciler<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: crate::materialization::MaterializationTarget,
    config: MaterializationReconcilerConfig,
    observability: ObservabilityRecorder,
    route_events: RouteSubscriptionBroker,
    cancellation: crate::runtime_work::Cancellation,
}

#[derive(Debug)]
pub enum MaterializationReconcileError {
    Store(StoreError),
    Materializer(MaterializerError),
    Projection(ProjectionError),
    Render(String),
    StaleDesiredRefs,
    LeaseLost,
    Cancelled,
    Deadline,
}

impl Default for MaterializationReconcilerConfig {
    fn default() -> Self {
        Self {
            owner: default_owner(),
            interval: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(60),
            batch_size: 32,
            concurrency_limit: 4,
        }
    }
}

impl<C> MaterializationReconciler<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: crate::materialization::MaterializationTarget,
        config: MaterializationReconcilerConfig,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            store,
            materializer,
            target,
            config,
            observability,
            route_events: RouteSubscriptionBroker::new(),
            cancellation: crate::runtime_work::Cancellation::new(),
        }
    }

    pub fn with_route_events(mut self, route_events: RouteSubscriptionBroker) -> Self {
        self.route_events = route_events;
        self
    }

    pub async fn run_until_shutdown(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), StoreError> {
        let cancelled = crate::runtime_work::Cancellation::new();
        let mut jobs = JoinSet::new();
        let mut active = std::collections::HashMap::new();
        let mut ticker =
            tokio::time::interval(self.interval_with_jitter().max(Duration::from_millis(10)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut scan_failures = 0;
        let outcome = loop {
            if *shutdown.borrow() {
                break Ok(());
            }
            tokio::select! {
                biased;
                _ = shutdown.changed() => { break Ok(()); }
                result = jobs.join_next(), if !jobs.is_empty() => {
                    match result.expect("active job") {
                        Ok(id) => { active.remove(&id); },
                        Err(error) => break Err(StoreError::unavailable(format!("reconciliation task failed: {error}"))),
                    }
                }
                _ = ticker.tick() => {
                    let scan_started = Instant::now();
                    if let Err(error) = self.store.finalize_instance_deletions(self.config.batch_size).await { self.record_reconciler_run(Outcome::Error, scan_started.elapsed()); break Err(error); }
                    let candidates = self.store.list_materialization_reconciliation_candidates(
                        ListMaterializationReconciliationCandidatesRequest::new(self.config.batch_size).for_target(self.target.clone())
                    ).await;
                    let candidates = match candidates {
                        Ok(candidates) => { scan_failures = 0; candidates },
                        Err(error) => { self.record_reconciler_run(Outcome::Error, scan_started.elapsed()); scan_failures += 1; if scan_failures >= 5 { break Err(error); } continue; }
                    };
                    for candidate in candidates {
                        self.record_candidate(candidate.state);
                        let capacity = self.config.concurrency_limit.max(2);
                        let waking = active.values().filter(|state| **state == MaterializationState::Pending).count();
                        if active.len() >= capacity || active.contains_key(&candidate.id) || (candidate.state == MaterializationState::Pending && waking >= capacity - 1) { continue; }
                        let id = candidate.id.clone();
                        active.insert(id.clone(), candidate.state);
                        let mut reconciler = self.clone();
                        reconciler.cancellation = crate::runtime_work::Cancellation::new();
                        let cancelled = cancelled.clone();
                        jobs.spawn(async move { reconciler.claim_and_reconcile_with_cancel(candidate, &cancelled).await; id });
                    }
                    // One production run is a discovery/scheduling scan; work latency is tracked separately.
                    self.record_reconciler_run(Outcome::Success, scan_started.elapsed());
                }
            }
        };
        cancelled.cancel();
        // Live cancellation gets an owned opportunity to ACK unsent effects.
        let drained = tokio::time::timeout(Duration::from_secs(20), async {
            let mut error = None;
            while let Some(result) = jobs.join_next().await {
                if let Err(failure) = result {
                    error.get_or_insert_with(|| StoreError::unavailable(failure.to_string()));
                }
            }
            error.map_or(Ok(()), Err)
        })
        .await;
        let drain_result = match drained {
            Ok(result) => result,
            Err(_) => {
                jobs.abort_all();
                while jobs.join_next().await.is_some() {}
                Err(StoreError::unavailable(
                    "reconciliation shutdown deadline exceeded",
                ))
            }
        };
        outcome.and(drain_result)
    }

    pub async fn run_once(&self) {
        let started = Instant::now();
        let _ = self
            .store
            .finalize_instance_deletions(self.config.batch_size)
            .await;
        let candidates = match self
            .store
            .list_materialization_reconciliation_candidates(
                ListMaterializationReconciliationCandidatesRequest::new(self.config.batch_size)
                    .for_target(self.target.clone()),
            )
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                self.record_reconciler_run(Outcome::Error, started.elapsed());
                self.record_outcome("scan", Outcome::Error, Some(&error.to_string()));
                return;
            }
        };

        let concurrency = cmp::max(1, self.config.concurrency_limit);
        let mut join_set = JoinSet::new();
        for candidate in candidates {
            self.record_candidate(candidate.state);
            while join_set.len() >= concurrency {
                let _ = join_set.join_next().await;
            }
            let reconciler = self.clone();
            join_set.spawn(async move {
                reconciler.claim_and_reconcile(candidate).await;
            });
        }
        while join_set.join_next().await.is_some() {}
        let _ = self
            .store
            .finalize_instance_deletions(self.config.batch_size)
            .await;
        self.record_reconciler_run(Outcome::Success, started.elapsed());
    }

    pub async fn reconcile_materialization(&self, materialization: MaterializationRecord) {
        self.claim_and_reconcile(materialization).await;
    }

    async fn claim_and_reconcile(&self, candidate: MaterializationRecord) {
        // Public one-shot entry points can share a reconciler or invoke it again.
        // Cancellation belongs to this claimed job, never its siblings/future calls.
        let mut job = self.clone();
        job.cancellation = crate::runtime_work::Cancellation::new();
        job.claim_and_reconcile_with_cancel(candidate, &crate::runtime_work::Cancellation::new())
            .await;
    }

    async fn claim_and_reconcile_with_cancel(
        &self,
        candidate: MaterializationRecord,
        cancelled: &crate::runtime_work::Cancellation,
    ) {
        if candidate.target != self.target {
            return;
        }
        let candidate_state = candidate.state;
        let claimed = match self
            .store
            .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
                candidate.id.clone(),
                self.config.owner.clone(),
                self.config.lease_ttl,
            ))
            .await
        {
            Ok(Some(claimed)) => {
                self.record_claim(candidate_state, Outcome::Success);
                claimed
            }
            Ok(None) => {
                self.record_claim(candidate_state, Outcome::Rejected);
                self.record_outcome("claim", Outcome::Rejected, None);
                return;
            }
            Err(error) => {
                self.record_claim(candidate_state, Outcome::Error);
                self.record_outcome("claim", Outcome::Error, Some(&error.to_string()));
                return;
            }
        };
        self.record_outcome("claim", Outcome::Success, None);
        if cancelled.is_cancelled() {
            self.cancellation.cancel();
        }

        let result = self.reconcile_with_heartbeat(&claimed, cancelled).await;

        match result {
            Ok(()) => {
                self.record_outcome("reconcile", Outcome::Success, None);
                return; // Completion already consumes the lease.
            }
            Err(MaterializationReconcileError::LeaseLost) => {
                self.record_outcome("lease_lost", Outcome::Rejected, None)
            }
            Err(MaterializationReconcileError::Cancelled) => {}
            Err(error) => {
                self.record_outcome("reconcile", Outcome::Error, Some(&error.to_string()));
                let recorded = self
                    .store
                    .record_materialization_failure(
                        crate::runtime_work::RecordMaterializationFailure {
                            expected_state: claimed.state,
                            materialization_id: claimed.id.clone(),
                            owner: self.config.owner.clone(),
                            attempt: claimed
                                .reconciliation_lease
                                .as_ref()
                                .expect("claimed lease")
                                .attempt,
                            generation: claimed.instance_generation,
                            permanent: error.permanent(),
                            message: error.to_string(),
                        },
                    )
                    .await;
                if matches!(recorded, Ok(true)) {
                    return; // Committed failure atomically consumes the lease.
                }
            }
        }

        // The owned operation has settled or been dropped after cooperative
        // cancellation. Release only this live stamp, and only without an
        // unresolved effect. Superseded Pending work must not retain the lease
        // until expiry or publish its old failure into accepted Deleting work.
        let _ = self
            .store
            .release_materialization_reconciliation_lease(
                ReleaseMaterializationReconciliationLeaseRequest::new(
                    claimed.id.clone(),
                    self.config.owner.clone(),
                    claimed
                        .reconciliation_lease
                        .as_ref()
                        .expect("claimed lease")
                        .attempt,
                    claimed.instance_generation,
                ),
            )
            .await;
    }

    async fn reconcile_with_heartbeat(
        &self,
        claimed: &MaterializationRecord,
        cancelled: &crate::runtime_work::Cancellation,
    ) -> Result<(), MaterializationReconcileError> {
        if cancelled.is_cancelled() {
            return Err(MaterializationReconcileError::Cancelled);
        }
        self.renew_or_lose(claimed).await?;
        let status_requested_at = tokio::time::Instant::now();
        let status = self
            .store
            .load_materialization_work_status(claimed.id.clone())
            .await
            .map_err(MaterializationReconcileError::Store)?;
        let remaining = operation_budget(status, status_requested_at.elapsed())?;
        if remaining.is_zero() {
            return Err(MaterializationReconcileError::Deadline);
        }
        let deadline = tokio::time::sleep(remaining);
        tokio::pin!(deadline);
        let work = async {
            match claimed.state {
                MaterializationState::Pending => self.reconcile_pending(claimed.clone()).await,
                MaterializationState::Deleting => self.reconcile_deleting(claimed.clone()).await,
                _ => Ok(()),
            }
        };
        tokio::pin!(work);
        let period = (self.config.lease_ttl / 3).max(Duration::from_millis(1));
        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Work may own the row lock required by renewal and need another
            // poll to commit it. Keep one owned renewal future alongside work;
            // awaiting it inside the tick branch would deadlock that transaction
            // and hide cancellation until the database statement timed out.
            let renewal = async {
                heartbeat.tick().await;
                self.renew_or_lose(claimed).await
            };
            tokio::pin!(renewal);
            tokio::select! {
                biased;
                result = &mut work => return result,
                _ = cancelled.cancelled() => {
                    self.cancellation.cancel();
                    let _ = tokio::time::timeout(Duration::from_secs(15), &mut work).await;
                    return Err(MaterializationReconcileError::Cancelled);
                }
                _ = &mut deadline => {
                    self.cancellation.cancel();
                    let _ = tokio::time::timeout(Duration::from_secs(15), &mut work).await;
                    return Err(MaterializationReconcileError::Deadline);
                }
                renewed = &mut renewal => {
                    if renewed.is_err() {
                        self.cancellation.cancel();
                        let _ = tokio::time::timeout(Duration::from_secs(15), &mut work).await;
                        return Err(MaterializationReconcileError::LeaseLost);
                    }
                },
            }
        }
    }

    fn fenced_materializer(
        &self,
        materialization: &MaterializationRecord,
    ) -> KubernetesMaterializer<fenced_client::FencedKubernetesClient<C>> {
        KubernetesMaterializer::new(fenced_client::FencedKubernetesClient {
            inner: self.materializer.client().clone(),
            cancellation: self.cancellation.clone(),
            store: self.store.clone(),
            materialization: materialization.clone(),
        })
    }

    async fn reconcile_pending(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let Some(instance) = self
            .load_instance(materialization.instance_id.clone())
            .await?
        else {
            return self.delete_refs_and_mark_deleted(materialization).await;
        };

        if instance.generation != materialization.instance_generation
            || matches!(
                instance.state,
                InstanceState::Deleting | InstanceState::Deleted
            )
        {
            return self.delete_refs_and_mark_deleted(materialization).await;
        }
        if instance.state != InstanceState::Waking {
            return Err(MaterializationReconcileError::Store(
                StoreError::invalid_argument(
                    "pending materialization is not attached to waking instance",
                ),
            ));
        }

        let mut projected_instance = instance.clone();
        projected_instance.state = InstanceState::Running;
        projected_instance.generation = materialization.projection_generation;
        let projected_materialization = materialization.clone();
        let manifest = self
            .render_current_manifest(&projected_instance, &materialization)
            .await?;
        let projection_plan = ProjectionPlan::from_manifest(&projected_materialization, &manifest)
            .map_err(MaterializationReconcileError::Materializer)?;
        let desired_refs = projection_plan.object_refs();
        if desired_refs != materialization.rendered_objects {
            return Err(MaterializationReconcileError::StaleDesiredRefs);
        }

        self.apply_projection_observed(&projection_plan, &materialization)
            .await?;
        let backend = self
            .wait_for_projection_readiness_observed(&projection_plan, &materialization)
            .await?;
        let observations = self.inspect_projection_observed(&projection_plan).await?;
        ProjectionReconciler::new(&self.materializer)
            .reject_unowned(&observations)
            .map_err(MaterializationReconcileError::Projection)?;
        if observations.iter().any(|observation| {
            !matches!(
                observation.state,
                crate::projection::ProjectionObservationState::PresentOwned
            )
        }) {
            return Err(MaterializationReconcileError::Projection(
                ProjectionError::Incomplete { observations },
            ));
        }
        self.renew_or_lose(&materialization).await?;

        let mut complete = CompleteWakeRequest::new(
            materialization.instance_id.clone(),
            materialization.instance_generation,
            materialization.target.clone(),
            backend,
            materialization.backend_generation,
        );
        complete.rendered_objects = materialization.rendered_objects.clone();
        complete.exclusivity_keys = materialization.exclusivity_keys.clone();
        let result = self
            .store
            .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
                materialization.id.clone(),
                self.config.owner.clone(),
                materialization
                    .reconciliation_lease
                    .as_ref()
                    .expect("claimed lease")
                    .attempt,
                complete,
            ))
            .await
            .map_err(MaterializationReconcileError::Store)?;
        self.notify_instance_routes_changed(result.instance.id)
            .await?;
        Ok(())
    }

    async fn reconcile_deleting(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let projection_plan = ProjectionPlan::from_recorded_refs(&materialization);
        self.delete_projection_observed(&projection_plan, &materialization)
            .await?;
        self.renew_or_lose(&materialization).await?;

        let Some(instance) = self
            .load_instance(materialization.instance_id.clone())
            .await?
        else {
            return self.mark_deleted(materialization).await;
        };
        // begin_sleep leaves the materialization stamped with the Running
        // generation and moves the instance to Draining at Running + 1, so
        // the drain this row belongs to is current exactly when the instance
        // is Draining one generation past the stamp. Anything else means the
        // instance moved on (re-woke, failed, or is being deleted) and the
        // row is just cleanup debris.
        if instance.state != InstanceState::Draining
            || instance.generation != materialization.instance_generation.next()
        {
            return self.mark_deleted(materialization).await;
        }

        let result = self
            .store
            .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
                materialization.id.clone(),
                self.config.owner.clone(),
                materialization
                    .reconciliation_lease
                    .as_ref()
                    .expect("claimed lease")
                    .attempt,
                FinalizeSleepRequest::new(instance.id, instance.generation, materialization.target),
            ))
            .await
            .map_err(MaterializationReconcileError::Store)?;
        self.notify_instance_routes_changed(result.instance.id)
            .await?;
        Ok(())
    }

    async fn notify_instance_routes_changed(
        &self,
        instance_id: InstanceId,
    ) -> Result<(), MaterializationReconcileError> {
        self.route_events.notify_instance_changed(instance_id);
        Ok(())
    }

    async fn delete_refs_and_mark_deleted(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let projection_plan = ProjectionPlan::from_recorded_refs(&materialization);
        self.delete_projection_observed(&projection_plan, &materialization)
            .await?;
        self.renew_or_lose(&materialization).await?;
        self.mark_deleted(materialization).await
    }

    async fn mark_deleted(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        self.store
            .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
                materialization.id.clone(),
                self.config.owner.clone(),
                materialization
                    .reconciliation_lease
                    .as_ref()
                    .expect("claimed lease")
                    .attempt,
                materialization.state,
                materialization.instance_id,
                materialization.instance_generation,
                materialization.target,
            ))
            .await
            .map(|_| ())
            .map_err(MaterializationReconcileError::Store)
    }

    async fn renew_or_lose(
        &self,
        materialization: &MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let renewed = match self
            .store
            .renew_materialization_reconciliation_lease(
                RenewMaterializationReconciliationLeaseRequest::new(
                    materialization.id.clone(),
                    self.config.owner.clone(),
                    materialization
                        .reconciliation_lease
                        .as_ref()
                        .expect("claimed lease")
                        .attempt,
                    materialization.instance_generation,
                    self.config.lease_ttl,
                    materialization.state,
                ),
            )
            .await
        {
            Ok(renewed) => renewed,
            Err(error) => {
                self.record_lease_renewal(Outcome::Error);
                return Err(MaterializationReconcileError::Store(error));
            }
        };
        if renewed {
            self.record_lease_renewal(Outcome::Success);
            Ok(())
        } else {
            self.record_lease_renewal(Outcome::Rejected);
            Err(MaterializationReconcileError::LeaseLost)
        }
    }

    async fn inspect_projection_observed(
        &self,
        plan: &ProjectionPlan,
    ) -> Result<Vec<crate::projection::ProjectionObservation>, MaterializationReconcileError> {
        ProjectionReconciler::new(&self.materializer)
            .inspect(plan)
            .await
            .map_err(MaterializationReconcileError::Projection)
    }

    async fn apply_projection_observed(
        &self,
        plan: &ProjectionPlan,
        materialization: &MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let started = Instant::now();
        let fenced = self.fenced_materializer(materialization);
        let result = ProjectionReconciler::new(&fenced).apply(plan).await;
        self.record_kubernetes_operation(
            Operation::Apply,
            outcome_for_result(&result),
            started.elapsed(),
        );
        result.map_err(MaterializationReconcileError::Projection)
    }

    async fn wait_for_projection_readiness_observed(
        &self,
        plan: &ProjectionPlan,
        materialization: &MaterializationRecord,
    ) -> Result<BackendEndpoint, MaterializationReconcileError> {
        let started = Instant::now();
        let fenced = self.fenced_materializer(materialization);
        let result = ProjectionReconciler::new(&fenced)
            .wait_for_readiness(plan)
            .await;
        self.record_kubernetes_operation(
            Operation::Readiness,
            outcome_for_result(&result),
            started.elapsed(),
        );
        result.map_err(MaterializationReconcileError::Projection)
    }

    async fn delete_projection_observed(
        &self,
        plan: &ProjectionPlan,
        materialization: &MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let started = Instant::now();
        let fenced = self.fenced_materializer(materialization);
        let result = ProjectionReconciler::new(&fenced).delete_owned(plan).await;
        self.record_kubernetes_operation(
            Operation::Delete,
            outcome_for_result(&result),
            started.elapsed(),
        );
        result.map_err(MaterializationReconcileError::Projection)
    }

    async fn load_instance(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<InstanceRecord>, MaterializationReconcileError> {
        self.store
            .get_instance(GetInstanceRequest::new(instance_id))
            .await
            .map_err(MaterializationReconcileError::Store)
    }

    async fn render_current_manifest(
        &self,
        instance: &InstanceRecord,
        materialization: &MaterializationRecord,
    ) -> Result<crate::manifest::RenderedManifest, MaterializationReconcileError> {
        let workload_class = self
            .store
            .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
                instance.workload_class.clone(),
            ))
            .await
            .map_err(MaterializationReconcileError::Store)?
            .ok_or({
                MaterializationReconcileError::Store(StoreError::NotFound {
                    resource: "workload class version",
                })
            })?;
        let sleep_policy = workload_class
            .sleep_policy
            .resolve(&instance.values)
            .map_err(|error| MaterializationReconcileError::Render(error.to_string()))?;
        render_manifests_with_options(
            RenderManifestRequest {
                template: &workload_class.template,
                instance,
                sleep_policy,
                namespace: materialization.target.namespace(),
                template_generation: Some(workload_class.template_generation),
            },
            self.materializer.render_options(),
        )
        .map_err(|error| MaterializationReconcileError::Render(error.to_string()))
    }

    fn interval_with_jitter(&self) -> Duration {
        let interval = self.config.interval;
        let jitter_bound = interval / 5;
        if jitter_bound.is_zero() {
            return interval;
        }
        let jitter_millis = stable_owner_hash(&self.config.owner) % jitter_bound.as_millis() as u64;
        interval + Duration::from_millis(jitter_millis)
    }

    fn record_outcome(&self, operation: &'static str, outcome: Outcome, error: Option<&str>) {
        self.observability.record_log(LifecycleLogEvent::new(
            EVENT_MATERIALIZATION_RECONCILIATION,
            vec![
                LogField::new("operation", operation),
                LogField::new("outcome", outcome.as_str()),
            ],
        ));
        if let Some(error) = error {
            self.observability.record_metric(MetricObservation::new(
                RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
                vec![
                    Operation::Materialize.metric_label(),
                    Outcome::Error.metric_label(),
                ],
                1.0,
            ));
            self.observability.record_log(LifecycleLogEvent::new(
                EVENT_MATERIALIZATION_FAILURE,
                vec![LogField::error_reason(error)],
            ));
        }
    }

    fn record_reconciler_run(&self, outcome: Outcome, duration: Duration) {
        self.observability.record_metric(MetricObservation::new(
            RECONCILER_RUNS_TOTAL,
            vec![outcome.metric_label()],
            1.0,
        ));
        self.observability.record_metric(MetricObservation::new(
            RECONCILER_RUN_DURATION_SECONDS,
            vec![outcome.metric_label()],
            duration.as_secs_f64(),
        ));
    }

    fn record_candidate(&self, state: MaterializationState) {
        self.observability.record_metric(MetricObservation::new(
            RECONCILER_CANDIDATES_TOTAL,
            vec![state.metric_label()],
            1.0,
        ));
    }

    fn record_claim(&self, state: MaterializationState, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RECONCILER_CLAIMS_TOTAL,
            vec![state.metric_label(), outcome.metric_label()],
            1.0,
        ));
    }

    fn record_lease_renewal(&self, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RECONCILER_LEASE_RENEWALS_TOTAL,
            vec![outcome.metric_label()],
            1.0,
        ));
    }

    fn record_kubernetes_operation(
        &self,
        operation: Operation,
        outcome: Outcome,
        duration: Duration,
    ) {
        self.observability.record_metric(MetricObservation::new(
            KUBERNETES_OPERATIONS_TOTAL,
            vec![operation.metric_label(), outcome.metric_label()],
            1.0,
        ));
        self.observability.record_metric(MetricObservation::new(
            KUBERNETES_OPERATION_DURATION_SECONDS,
            vec![operation.metric_label(), outcome.metric_label()],
            duration.as_secs_f64(),
        ));
    }
}

fn outcome_for_result<T, E>(result: &Result<T, E>) -> Outcome {
    if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Error
    }
}

#[cfg(test)]
fn projected_pending_wake_instance(instance: &InstanceRecord) -> InstanceRecord {
    let mut projected = instance.clone();
    projected.state = InstanceState::Running;
    projected.generation = instance.generation.next();
    projected
}
#[cfg(test)]
fn projected_pending_wake_materialization(
    materialization: &MaterializationRecord,
) -> MaterializationRecord {
    materialization.clone()
}

impl<C> Clone for MaterializationReconciler<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            materializer: self.materializer.clone(),
            target: self.target.clone(),
            config: self.config.clone(),
            observability: self.observability.clone(),
            route_events: self.route_events.clone(),
            cancellation: self.cancellation.clone(),
        }
    }
}

impl fmt::Display for MaterializationReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "store: {error}"),
            Self::Materializer(error) => write!(f, "materializer: {error}"),
            Self::Projection(error) => write!(f, "projection: {error}"),
            Self::Render(error) => write!(f, "render: {error}"),
            Self::StaleDesiredRefs => {
                f.write_str("rendered object refs no longer match persisted refs")
            }
            Self::Cancelled => write!(f, "reconciliation cancelled"),
            Self::Deadline => write!(f, "operation deadline expired"),
            Self::LeaseLost => f.write_str("reconciliation lease was lost"),
        }
    }
}

fn default_owner() -> String {
    format!(
        "pid-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default()
    )
}

fn stable_owner_hash(owner: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in owner.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn operation_budget(
    status: Option<crate::runtime_work::MaterializationWorkStatus>,
    request_elapsed: Duration,
) -> Result<Duration, MaterializationReconcileError> {
    let remaining = match status {
        Some(status) if status.operation_deadline_unix_millis > 0 => {
            status.operation_remaining.ok_or_else(|| {
                MaterializationReconcileError::Store(StoreError::internal(
                    "persisted operation deadline is missing its database-relative budget",
                ))
            })?
        }
        _ => Duration::from_secs(600),
    };
    // Request-start anchoring deducts pool wait, query/retry and response time.
    // It can stop conservatively early; only PostgreSQL decides whether failure
    // is terminal. Work never receives additional lifetime from an RPC delay.
    Ok(remaining.saturating_sub(request_elapsed))
}

impl MaterializationReconcileError {
    fn permanent(&self) -> bool {
        fn materializer(error: &MaterializerError) -> bool {
            match error {
                MaterializerError::InvalidManifest { .. } => true,
                MaterializerError::Apply { source, .. }
                | MaterializerError::Delete { source, .. } => {
                    !source.is_retryable() && !source.outcome_uncertain()
                }
                MaterializerError::PvcBoundWait { .. }
                | MaterializerError::ReadinessWait { .. } => false,
            }
        }
        match self {
            Self::Render(_)
            | Self::StaleDesiredRefs
            | Self::Store(StoreError::InvalidArgument { .. } | StoreError::NotFound { .. }) => true,
            Self::Materializer(error) => materializer(error),
            Self::Projection(
                ProjectionError::Apply { source, .. } | ProjectionError::Delete { source, .. },
            ) => materializer(source),
            Self::Projection(
                ProjectionError::MissingManifest | ProjectionError::OwnershipConflict { .. },
            ) => true,
            Self::Projection(ProjectionError::Inspect { source, .. }) => {
                !source.is_retryable() && !source.outcome_uncertain()
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn operation_budget_uses_database_duration_and_deducts_request_time() {
        for absolute in [1, 1_000, i64::MAX] {
            let status = crate::runtime_work::MaterializationWorkStatus {
                operation_deadline_unix_millis: absolute,
                operation_remaining: Some(std::time::Duration::from_millis(200)),
                ..Default::default()
            };
            assert_eq!(
                super::operation_budget(Some(status.clone()), std::time::Duration::from_millis(50))
                    .unwrap(),
                std::time::Duration::from_millis(150)
            );
            assert_eq!(
                super::operation_budget(Some(status), std::time::Duration::from_millis(250))
                    .unwrap(),
                std::time::Duration::ZERO
            );
        }
        assert!(super::operation_budget(
            Some(crate::runtime_work::MaterializationWorkStatus {
                operation_deadline_unix_millis: 1,
                ..Default::default()
            }),
            std::time::Duration::ZERO
        )
        .is_err());
        assert_eq!(
            super::operation_budget(None, std::time::Duration::from_secs(1)).unwrap(),
            std::time::Duration::from_secs(599)
        );
    }

    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex},
    };

    use crate::{
        ids::{BackendGeneration, Generation, InstanceId, MaterializationId, WorkloadClassId},
        instance::{InstanceState, InstanceValues},
        manifest::{
            render_manifests, ContainerPortTemplate, ContainerTemplate, EnvVarTemplate,
            ManifestTemplate, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
            WorkloadKind, WorkloadTemplate,
        },
        materialization::{
            BackendEndpoint, CompleteWakeResult, FinalizeSleepResult,
            MaterializationReconciliationLease, MaterializationTarget, RenderedObjectRef,
        },
        materializer::{KubernetesClientError, KubernetesClientFuture, KubernetesClientResult},
        projection::{LiveObjectMetadata, ProjectionObjectInspection},
        route::{ListRouteBindingsForInstanceRequest, RouteBindingRecord},
        store::{StoreFuture, StoreResult},
        workload::{
            RenderedExclusivityKey, WorkloadClassVersion, WorkloadClassVersionRef,
            WorkloadValueSchema,
        },
        WorkloadSleepPolicy,
    };
    use sleepypods_observability::recorder::{InMemoryObservability, ObservabilityEvent};

    use super::*;

    #[tokio::test]
    async fn deleting_reconciliation_deletes_refs_and_finalizes_with_current_lease() {
        let materialization = deleting_materialization("mat-delete-ok");
        let store = Arc::new(FakeReconcileStore::new(
            materialization.clone(),
            draining_instance("instance-reconcile"),
        ));
        let materializer = KubernetesMaterializer::new(
            FakeKubernetesClient::default().with_live_owned_refs(&materialization),
        );
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 1);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn deleting_reconciliation_keeps_keys_when_cleanup_fails() {
        let materialization = deleting_materialization("mat-delete-fail");
        let store = Arc::new(FakeReconcileStore::new(
            materialization.clone(),
            draining_instance("instance-reconcile"),
        ));
        let materializer = KubernetesMaterializer::new(
            FakeKubernetesClient::default()
                .with_live_owned_refs(&materialization)
                .with_delete_error(KubernetesClientError::transient("api unavailable")),
        );
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(store.release_calls(), 1);
    }

    #[tokio::test]
    async fn deleting_reconciliation_does_not_finalize_after_lease_loss() {
        let store = Arc::new(
            FakeReconcileStore::new(
                deleting_materialization("mat-delete-lease-lost"),
                draining_instance("instance-reconcile"),
            )
            .with_renew_result(false),
        );
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(
            store.release_calls(),
            1,
            "attempt exact conditional release after work stops"
        );
    }

    #[tokio::test]
    async fn deleting_reconciliation_records_lease_loss_metric() {
        let store = Arc::new(
            FakeReconcileStore::new(
                deleting_materialization("mat-delete-lease-metrics"),
                draining_instance("instance-reconcile"),
            )
            .with_renew_result(false),
        );
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let sink = InMemoryObservability::default();
        let reconciler = reconciler_with_observability(store, materializer, sink.recorder());

        reconciler.run_once().await;

        assert_metric(
            &sink.events(),
            RECONCILER_LEASE_RENEWALS_TOTAL.name(),
            &[("outcome", "rejected")],
        );
    }

    #[tokio::test]
    async fn deleting_reconciliation_records_kubernetes_delete_error_metric() {
        let materialization = deleting_materialization("mat-delete-error-metrics");
        let store = Arc::new(FakeReconcileStore::new(
            materialization.clone(),
            draining_instance("instance-reconcile"),
        ));
        let materializer = KubernetesMaterializer::new(
            FakeKubernetesClient::default()
                .with_live_owned_refs(&materialization)
                .with_delete_error(KubernetesClientError::transient("api unavailable")),
        );
        let sink = InMemoryObservability::default();
        let reconciler = reconciler_with_observability(store, materializer, sink.recorder());

        reconciler.run_once().await;

        assert_metric(
            &sink.events(),
            KUBERNETES_OPERATIONS_TOTAL.name(),
            &[("operation", "delete"), ("outcome", "error")],
        );
    }

    #[tokio::test]
    async fn pending_reconciliation_applies_waits_and_completes_current_wake() {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pending-complete", &instance);
        let store = Arc::new(FakeReconcileStore::new(materialization, instance));
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.complete_calls(), 1);
        assert_eq!(store.guarded_delete_calls(), 0);
        assert_eq!(store.materialization_state(), MaterializationState::Ready);
        assert_eq!(store.materialization_generation(), Generation::new(8));
        assert_eq!(client.apply_calls(), 2);
        assert_eq!(client.wait_readiness_calls(), 1);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn pending_reconciliation_resumes_running_projection_after_apply_before_complete_crash() {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pending-applied-crash", &instance);
        let projected = projected_pending_wake_materialization(&materialization);
        let projected_instance = projected_pending_wake_instance(&instance);
        let store = Arc::new(FakeReconcileStore::new(materialization, instance));
        let client = FakeKubernetesClient::default()
            .with_live_applied_projection(&projected, &projected_instance);
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.complete_calls(), 1);
        assert_eq!(store.guarded_delete_calls(), 0);
        assert_eq!(store.materialization_state(), MaterializationState::Ready);
        assert_eq!(store.materialization_generation(), Generation::new(8));
        assert_eq!(client.apply_calls(), 2);
        assert_eq!(client.wait_readiness_calls(), 1);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn pending_reconciliation_records_bounded_controller_metrics() {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pending-metrics", &instance);
        let store = Arc::new(FakeReconcileStore::new(materialization, instance));
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let sink = InMemoryObservability::default();
        let reconciler = reconciler_with_observability(store, materializer, sink.recorder());

        reconciler.run_once().await;

        let events = sink.events();
        assert_metric(
            &events,
            RECONCILER_RUNS_TOTAL.name(),
            &[("outcome", "success")],
        );
        assert_metric(
            &events,
            RECONCILER_CANDIDATES_TOTAL.name(),
            &[("state", "pending")],
        );
        assert_metric(
            &events,
            RECONCILER_CLAIMS_TOTAL.name(),
            &[("state", "pending"), ("outcome", "success")],
        );
        assert_metric(
            &events,
            RECONCILER_LEASE_RENEWALS_TOTAL.name(),
            &[("outcome", "success")],
        );
        assert_metric(
            &events,
            KUBERNETES_OPERATIONS_TOTAL.name(),
            &[("operation", "apply"), ("outcome", "success")],
        );
        assert_metric(
            &events,
            KUBERNETES_OPERATIONS_TOTAL.name(),
            &[("operation", "readiness"), ("outcome", "success")],
        );
    }

    #[tokio::test]
    async fn pending_pre_readiness_projection_fails_permanently_without_rewrite_and_can_be_cleaned()
    {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pre-readiness-upgrade", &instance);
        let class = workload_class();
        let mut projected_instance = instance.clone();
        projected_instance.state = InstanceState::Running;
        projected_instance.generation = materialization.projection_generation;
        let mut legacy_manifest = render_manifests(RenderManifestRequest {
            template: &class.template,
            instance: &projected_instance,
            sleep_policy: class.sleep_policy.resolve(&instance.values).unwrap(),
            namespace: "apps",
            template_generation: Some(class.template_generation),
        })
        .unwrap();
        for object in &mut legacy_manifest.objects {
            if let crate::manifest::KubernetesObject::Deployment(workload) = &mut object.object {
                let sidecar = &mut workload.spec.template.spec.containers[1];
                let health_port = sidecar.readiness_probe.take().unwrap().port;
                sidecar
                    .ports
                    .retain(|port| port.container_port != health_port);
                sidecar.ports[0].name = Some("sleepypods".to_owned());
                sidecar
                    .env
                    .retain(|env| env.name != "SLEEPYPODS_SIDECAR_READINESS_LISTEN_ADDR");
            }
        }
        let legacy_plan =
            ProjectionPlan::from_manifest(&materialization, &legacy_manifest).unwrap();
        let client = FakeKubernetesClient::default();
        for object in &legacy_plan.manifest().unwrap().objects {
            client.set_live(
                crate::materializer::rendered_object_ref(&object.object),
                ProjectionObjectInspection::Present(LiveObjectMetadata::from_rendered_object(
                    &object.object,
                )),
            );
        }
        let store = Arc::new(FakeReconcileStore::new(materialization.clone(), instance));
        let materializer = KubernetesMaterializer::new(client.clone());
        let driver = reconciler(store.clone(), materializer);
        driver.run_once().await;
        assert_eq!(
            *store.failures.lock().unwrap(),
            [true],
            "same-generation old hash is a permanent wake failure"
        );
        assert_eq!(store.complete_calls(), 0);
        assert_eq!(
            client.apply_calls(),
            0,
            "never merge new labels/probes into old same-generation projection"
        );
        // The production store queues terminal wakes for cleanup. Its recorded-ref
        // plan deliberately checks identity/generation without the obsolete hash.
        let materializer = KubernetesMaterializer::new(client.clone());
        let cleanup = ProjectionPlan::from_recorded_refs(&materialization);
        ProjectionReconciler::new(&materializer)
            .delete_owned(&cleanup)
            .await
            .unwrap();
        assert_eq!(
            client.deleted.lock().unwrap().len(),
            materialization.rendered_objects.len()
        );
        assert!(client.live.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pending_reconciliation_stops_on_unowned_live_ref() {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pending-unowned", &instance);
        let client = FakeKubernetesClient::default()
            .with_live_unowned_ref(materialization.rendered_objects[0].clone());
        let store = Arc::new(FakeReconcileStore::new(materialization, instance));
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(client.apply_calls(), 0);
        assert_eq!(store.complete_calls(), 0);
        assert_eq!(store.materialization_state(), MaterializationState::Pending);
        assert_eq!(store.release_calls(), 1);
    }

    // Leftovers stamped with an older generation belong to this instance's
    // failed prior attempt: the pending reconciliation supersedes them in
    // place and completes the wake instead of stalling forever.
    #[tokio::test]
    async fn pending_reconciliation_supersedes_older_live_stamp() {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pending-older-stamp", &instance);
        let client = FakeKubernetesClient::default();
        let mut stale = live_owned_metadata(&materialization);
        stale.labels.insert(
            crate::manifest::LABEL_INSTANCE_GENERATION.to_owned(),
            "6".to_owned(),
        );
        client.set_live(
            materialization.rendered_objects[0].clone(),
            ProjectionObjectInspection::Present(stale),
        );
        let store = Arc::new(FakeReconcileStore::new(materialization, instance));
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(client.apply_calls(), 2);
        assert_eq!(store.complete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Ready);
        assert_eq!(store.release_calls(), 0);
    }

    // Live objects stamped with a NEWER generation belong to a newer owner;
    // this stale plan must not touch them.
    #[tokio::test]
    async fn pending_reconciliation_stops_on_newer_live_stamp() {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pending-newer-stamp", &instance);
        let client = FakeKubernetesClient::default();
        let mut newer = live_owned_metadata(&materialization);
        newer.labels.insert(
            crate::manifest::LABEL_INSTANCE_GENERATION.to_owned(),
            "9".to_owned(),
        );
        client.set_live(
            materialization.rendered_objects[0].clone(),
            ProjectionObjectInspection::Present(newer),
        );
        let store = Arc::new(FakeReconcileStore::new(materialization, instance));
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(client.apply_calls(), 0);
        assert_eq!(store.complete_calls(), 0);
        assert_eq!(store.materialization_state(), MaterializationState::Pending);
        assert_eq!(store.release_calls(), 1);
    }

    #[tokio::test]
    async fn pending_stale_generation_deletes_refs_and_marks_deleted_after_cleanup() {
        let materialization =
            pending_materialization("mat-pending-stale", &waking_instance("instance-reconcile"));
        let store = Arc::new(FakeReconcileStore::new(
            materialization.clone(),
            running_instance("instance-reconcile", 8),
        ));
        let projected = projected_pending_wake_materialization(&materialization);
        let client = FakeKubernetesClient::default().with_live_owned_refs(&projected);
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.complete_calls(), 0);
        assert_eq!(store.guarded_delete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Deleted);
        assert_eq!(client.delete_calls(), 2);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn deleting_reconciliation_tolerates_missing_refs_and_finalizes() {
        let store = Arc::new(FakeReconcileStore::new(
            deleting_materialization("mat-delete-missing-refs"),
            draining_instance("instance-reconcile"),
        ));
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(client.delete_calls(), 0);
        assert_eq!(store.finalize_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Deleted);
    }

    #[tokio::test]
    async fn deleting_reconciliation_blocks_on_unowned_live_ref() {
        let materialization = deleting_materialization("mat-delete-unowned");
        let client = FakeKubernetesClient::default()
            .with_live_unowned_ref(materialization.rendered_objects[0].clone());
        let store = Arc::new(FakeReconcileStore::new(
            materialization,
            draining_instance("instance-reconcile"),
        ));
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(client.delete_calls(), 0);
        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(
            store.materialization_state(),
            MaterializationState::Deleting
        );
        assert_eq!(store.release_calls(), 1);
    }

    #[tokio::test]
    async fn deleting_reconciliation_blocks_on_owned_finalizer() {
        let materialization = deleting_materialization("mat-delete-finalizer");
        let client = FakeKubernetesClient::default().with_live_deleting_owned_ref(
            &materialization,
            materialization.rendered_objects[0].clone(),
            vec!["example.com/cleanup".to_owned()],
        );
        let store = Arc::new(FakeReconcileStore::new(
            materialization,
            draining_instance("instance-reconcile"),
        ));
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(client.delete_calls(), 0);
        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(
            store.materialization_state(),
            MaterializationState::Deleting
        );
        assert_eq!(store.release_calls(), 1);
    }

    #[tokio::test]
    async fn deleting_reconciliation_marks_deleted_after_generation_race_cleanup() {
        let store = Arc::new(FakeReconcileStore::new(
            deleting_materialization("mat-delete-race"),
            running_instance("instance-reconcile", 8),
        ));
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(store.guarded_delete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Deleted);
    }

    // A Draining instance whose generation is more than one past the
    // materialization stamp belongs to a later sleep cycle, so the stale
    // deleting row must be discarded without finalizing that newer drain.
    #[tokio::test]
    async fn deleting_reconciliation_skips_finalize_for_later_drain_cycle() {
        let mut later_drain = draining_instance("instance-reconcile");
        later_drain.generation = Generation::new(12);
        let store = Arc::new(FakeReconcileStore::new(
            deleting_materialization("mat-delete-later-drain"),
            later_drain,
        ));
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(store.guarded_delete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Deleted);
    }

    #[tokio::test]
    async fn stale_cleanup_does_not_delete_newer_same_id_materialization() {
        let stale =
            pending_materialization("mat-stable-id", &waking_instance("instance-reconcile"));
        let mut newer =
            pending_materialization("mat-stable-id", &waking_instance("instance-reconcile"));
        newer.instance_generation = Generation::new(stale.instance_generation.get() + 1);
        newer.backend_generation = BackendGeneration::new(newer.instance_generation.get());
        newer.exclusivity_keys = vec![RenderedExclusivityKey::new("singleton", "class-a")];
        let store = Arc::new(
            FakeReconcileStore::new(stale, running_instance("instance-reconcile", 8))
                .with_replace_before_guarded_delete(newer.clone()),
        );
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.guarded_delete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Pending);
        assert_eq!(
            store.materialization_generation(),
            newer.instance_generation
        );
        assert_eq!(store.exclusivity_keys(), newer.exclusivity_keys);
    }

    fn reconciler(
        store: Arc<FakeReconcileStore>,
        materializer: KubernetesMaterializer<FakeKubernetesClient>,
    ) -> MaterializationReconciler<FakeKubernetesClient> {
        reconciler_with_observability(store, materializer, ObservabilityRecorder::noop())
    }

    fn reconciler_with_observability(
        store: Arc<FakeReconcileStore>,
        materializer: KubernetesMaterializer<FakeKubernetesClient>,
        observability: ObservabilityRecorder,
    ) -> MaterializationReconciler<FakeKubernetesClient> {
        MaterializationReconciler::new(
            store,
            materializer,
            target(),
            MaterializationReconcilerConfig {
                owner: "test-owner".to_owned(),
                interval: Duration::from_secs(60),
                lease_ttl: Duration::from_secs(30),
                batch_size: 10,
                concurrency_limit: 1,
            },
            observability,
        )
    }

    fn assert_metric(events: &[ObservabilityEvent], name: &str, expected_labels: &[(&str, &str)]) {
        assert!(
            events.iter().any(|event| {
                let ObservabilityEvent::Metric(metric) = event else {
                    return false;
                };
                metric.name() == name
                    && expected_labels.iter().all(|(key, value)| {
                        metric
                            .labels()
                            .iter()
                            .any(|label| label.key().as_str() == *key && label.value() == *value)
                    })
            }),
            "expected metric {name} with labels {expected_labels:?}, got {events:?}"
        );
    }

    #[tokio::test]
    async fn continuous_scheduler_records_scan_and_candidate_metrics() {
        let instance = waking_instance("instance-reconcile");
        let pending = pending_materialization("scan-observation", &instance);
        let store = Arc::new(FakeReconcileStore::new(pending, instance));
        let sink = InMemoryObservability::default();
        let driver = reconciler_with_observability(
            store.clone(),
            KubernetesMaterializer::new(FakeKubernetesClient::default()),
            sink.recorder(),
        );
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(driver.run_until_shutdown(receiver));
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.complete_calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.send_replace(true);
        task.await.unwrap().unwrap();
        assert_metric(
            &sink.events(),
            RECONCILER_RUNS_TOTAL.name(),
            &[("outcome", "success")],
        );
        assert_metric(
            &sink.events(),
            RECONCILER_RUN_DURATION_SECONDS.name(),
            &[("outcome", "success")],
        );
        assert_metric(
            &sink.events(),
            RECONCILER_CANDIDATES_TOTAL.name(),
            &[("state", "pending")],
        );
    }

    #[tokio::test]
    async fn cooperative_shutdown_and_fatal_scan_ack_unsent_begin_before_releasing() {
        for fatal in [false, true] {
            let instance = waking_instance("instance-reconcile");
            let pending = pending_materialization("cancel-begin", &instance);
            let gate = Arc::new(tokio::sync::Notify::new());
            let mut fake = FakeReconcileStore::new(pending, instance);
            fake.begin_gate = Some(gate.clone());
            let store = Arc::new(fake);
            let client = FakeKubernetesClient::default();
            let mut driver = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
            driver.config.interval = Duration::from_millis(10);
            let (shutdown, receiver) = watch::channel(false);
            let task = tokio::spawn(driver.run_until_shutdown(receiver));
            tokio::time::timeout(Duration::from_secs(1), async {
                while store.effect.lock().unwrap().is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if fatal {
                store
                    .fatal_scan
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            } else {
                shutdown.send_replace(true);
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert!(
                !task.is_finished(),
                "owned begin must settle before shutdown finishes"
            );
            gate.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.is_err(), fatal);
            assert!(
                store.effect.lock().unwrap().is_none(),
                "known-unsent begin is exactly acknowledged"
            );
            assert_eq!(
                client.apply_calls(),
                0,
                "cancellation before dispatch never polls Kubernetes"
            );
            assert!(store
                .materialization
                .lock()
                .unwrap()
                .reconciliation_lease
                .is_none());
        }
    }

    /// A lease is requested as a lifetime. The reconciler must hand the store
    /// its configured TTL untouched: deriving an absolute expiry here would make
    /// the real lease length depend on this process's clock offset, so a
    /// skewed-fast reconciler would silently hold work past its configured TTL.
    #[tokio::test]
    async fn reconciler_requests_its_configured_lease_ttl_without_consulting_its_clock() {
        let instance = waking_instance("instance-reconcile");
        let pending = pending_materialization("lease-ttl", &instance);
        let store = Arc::new(FakeReconcileStore::new(pending, instance));
        let client = FakeKubernetesClient::default();
        let mut driver = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
        driver.config.lease_ttl = Duration::from_secs(47);
        let candidate = store.materialization.lock().unwrap().clone();

        driver
            .claim_and_reconcile_with_cancel(candidate, &crate::runtime_work::Cancellation::new())
            .await;

        let requested = store.requested_lease_ttls.lock().unwrap().clone();
        assert!(
            !requested.is_empty(),
            "the pass must claim, and therefore request a lease"
        );
        assert!(
            requested.iter().all(|ttl| *ttl == Duration::from_secs(47)),
            "every lease request carries the configured TTL verbatim: {requested:?}"
        );
    }

    #[tokio::test]
    async fn pending_supersession_acks_known_unsent_begin_before_exact_release() {
        let instance = waking_instance("instance-reconcile");
        let pending = pending_materialization("superseded-unsent", &instance);
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut fake = FakeReconcileStore::new(pending, instance);
        fake.begin_gate = Some(gate.clone());
        let store = Arc::new(fake);
        let client = FakeKubernetesClient::default();
        let mut driver = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
        driver.store = Arc::new(crate::RetryingControlPlaneStore::with_default_policy(
            store.clone(),
        ));
        driver.config.lease_ttl = Duration::from_millis(30);
        let cancellation = driver.cancellation.clone();
        let candidate = store.materialization.lock().unwrap().clone();
        let mut jobs = JoinSet::new();
        jobs.spawn(async move {
            driver
                .claim_and_reconcile_with_cancel(
                    candidate,
                    &crate::runtime_work::Cancellation::new(),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.effect.lock().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        store.materialization.lock().unwrap().state = MaterializationState::Deleting;
        tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
            .await
            .unwrap();
        assert_eq!(
            store.release_calls(),
            0,
            "must await the owned begin result"
        );
        assert_eq!(client.apply_calls(), 0);
        gate.notify_one();
        tokio::time::timeout(Duration::from_secs(1), jobs.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(client.apply_calls(), 0, "no dispatch after supersession");
        assert!(
            store.effect.lock().unwrap().is_none(),
            "exact unsent ACK settled"
        );
        assert!(store
            .materialization
            .lock()
            .unwrap()
            .reconciliation_lease
            .is_none());
        assert!(
            store.failures.lock().unwrap().is_empty(),
            "old work cannot publish a new failure"
        );
    }

    #[tokio::test]
    async fn public_one_shot_jobs_isolate_cancellation_and_allow_reuse() {
        let instance = waking_instance("instance-reconcile");
        let pending = pending_materialization("one-shot-isolation", &instance);
        let left_store = Arc::new(FakeReconcileStore::new(pending.clone(), instance.clone()));
        let right_store = Arc::new(FakeReconcileStore::new(pending, instance));
        let left_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let right_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let left_client = FakeKubernetesClient {
            readiness_gate: Some(left_gate.clone()),
            ..Default::default()
        };
        let right_client = FakeKubernetesClient {
            readiness_gate: Some(right_gate.clone()),
            ..Default::default()
        };
        let mut left = reconciler(
            left_store.clone(),
            KubernetesMaterializer::new(left_client.clone()),
        );
        left.config.lease_ttl = Duration::from_millis(30);
        // Model cloned public handles with separate fake backends so each test
        // job has independent state while retaining the original shared token.
        let mut right = left.clone();
        right.store = right_store.clone();
        right.materializer = KubernetesMaterializer::new(right_client.clone());
        let left_job = left.clone();
        let mut jobs = JoinSet::new();
        jobs.spawn(async move {
            left_job.run_once().await;
            "left"
        });
        jobs.spawn(async move {
            right.run_once().await;
            "right"
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while left_client.wait_readiness_calls() == 0
                || right_client.wait_readiness_calls() == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        *left_store.renew_result.lock().unwrap() = false;
        let finished = tokio::time::timeout(Duration::from_secs(1), jobs.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            finished, "left",
            "only the canceled job can settle before gate release"
        );
        assert_eq!(left_store.complete_calls(), 0);
        assert_eq!(
            right_store.complete_calls(),
            0,
            "sibling readiness remains held"
        );
        right_gate.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), jobs.join_next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            "right"
        );
        assert_eq!(
            right_store.complete_calls(),
            1,
            "sibling cancellation must stay local"
        );
        *left_store.renew_result.lock().unwrap() = true;
        left_gate.add_permits(1);
        let candidate = left_store.materialization.lock().unwrap().clone();
        left.reconcile_materialization(candidate).await;
        assert_eq!(
            left_store.complete_calls(),
            1,
            "future public calls receive a fresh token"
        );
    }

    #[tokio::test]
    async fn blocked_renewal_keeps_polling_transaction_and_cancellation() {
        for cancel in [false, true] {
            let instance = waking_instance("instance-reconcile");
            let pending = pending_materialization("renew-transaction", &instance);
            let progress = Arc::new(BeginRenewProgress {
                started: tokio::sync::Notify::new(),
                renewal_started: tokio::sync::Notify::new(),
                allow_commit: tokio::sync::Semaphore::new(0),
                committed: watch::channel(false).0,
            });
            let mut fake = FakeReconcileStore::new(pending.clone(), instance);
            fake.begin_renew_progress = Some(progress.clone());
            let store = Arc::new(fake);
            let client = FakeKubernetesClient::default();
            let mut driver = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
            driver.store = Arc::new(crate::RetryingControlPlaneStore::with_default_policy(
                store.clone(),
            ));
            driver.config.lease_ttl = Duration::from_millis(30);
            let cancelled = crate::runtime_work::Cancellation::new();
            let shutdown = cancelled.clone();
            let observed_cancellation = driver.cancellation.clone();
            let mut jobs = JoinSet::new();
            jobs.spawn(async move {
                driver
                    .claim_and_reconcile_with_cancel(pending, &cancelled)
                    .await;
            });
            tokio::time::timeout(Duration::from_secs(1), progress.started.notified())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), progress.renewal_started.notified())
                .await
                .unwrap();
            // Model the captured PG interleaving: begin holds a row lock, renewal
            // awaits it, and committing requires the work future to be polled.
            if cancel {
                shutdown.cancel();
                tokio::time::timeout(Duration::from_secs(1), observed_cancellation.cancelled())
                    .await
                    .expect("blocked renewal must not hide cancellation");
            }
            progress.allow_commit.add_permits(1);
            tokio::time::timeout(Duration::from_secs(1), jobs.join_next())
                .await
                .expect("work must commit while its renewal waits")
                .unwrap()
                .unwrap();
            assert_eq!(store.complete_calls(), usize::from(!cancel));
            assert!(store.effect.lock().unwrap().is_none());
            if cancel {
                assert_eq!(
                    client.apply_calls(),
                    0,
                    "known-unsent canceled begin must ACK before release"
                );
                assert!(store
                    .materialization
                    .lock()
                    .unwrap()
                    .reconciliation_lease
                    .is_none());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reconciler_anchors_database_budget_before_delayed_status() {
        for absolute in [1, i64::MAX] {
            for delay_millis in [50, 250] {
                let instance = waking_instance("instance-reconcile");
                let pending = pending_materialization("database-budget", &instance);
                let mut fake = FakeReconcileStore::new(pending, instance);
                fake.delayed_status = Some((
                    Duration::from_millis(delay_millis),
                    crate::runtime_work::MaterializationWorkStatus {
                        operation_deadline_unix_millis: absolute,
                        operation_remaining: Some(Duration::from_millis(200)),
                        ..Default::default()
                    },
                ));
                let store = Arc::new(fake);
                let client = FakeKubernetesClient {
                    readiness_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
                    ..Default::default()
                };
                let driver = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
                let start = tokio::time::Instant::now();
                let mut jobs = JoinSet::new();
                jobs.spawn(async move { driver.run_once().await });
                while !store
                    .status_requested
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
                assert_eq!(start.elapsed(), Duration::ZERO);
                tokio::time::advance(Duration::from_millis(delay_millis)).await;
                if delay_millis == 50 {
                    for _ in 0..100 {
                        if client.wait_readiness_calls() > 0 {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    assert_eq!(
                        client.wait_readiness_calls(),
                        1,
                        "real readiness must be held after status returns"
                    );
                    assert!(jobs.try_join_next().is_none());
                    tokio::time::advance(Duration::from_millis(149)).await;
                    for _ in 0..10 {
                        tokio::task::yield_now().await;
                    }
                    assert!(
                        jobs.try_join_next().is_none(),
                        "original 200ms budget is not yet exhausted"
                    );
                    // Tokio's timer wheel has millisecond granularity. At201ms
                    // the original200ms budget must cancel, well before250ms.
                    tokio::time::advance(Duration::from_millis(2)).await;
                }
                let mut finished = false;
                for _ in 0..100 {
                    if let Some(result) = jobs.try_join_next() {
                        result.unwrap();
                        finished = true;
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                assert!(finished, "status delay must not grant a new 200ms lifetime");
                assert_eq!(
                    start.elapsed(),
                    Duration::from_millis(if delay_millis == 50 { 201 } else { 250 })
                );
                if delay_millis == 250 {
                    assert_eq!(
                        client.apply_calls(),
                        0,
                        "fully consumed budget cannot dispatch"
                    );
                    assert_eq!(client.wait_readiness_calls(), 0);
                }
                assert_eq!(store.complete_calls(), 0);
                assert!(store.effect.lock().unwrap().is_none());
                assert_eq!(*store.failures.lock().unwrap(), [false]);
            }
        }
    }

    #[tokio::test]
    async fn readiness_wait_renews_lease_before_expiry_and_completes() {
        let instance = waking_instance("instance-reconcile");
        let pending = pending_materialization("heartbeat", &instance);
        let store = Arc::new(FakeReconcileStore::new(pending, instance));
        let client = FakeKubernetesClient {
            readiness_delay: Duration::from_millis(90),
            ..Default::default()
        };
        let mut driver = reconciler(store.clone(), KubernetesMaterializer::new(client));
        driver.config.lease_ttl = Duration::from_millis(30);
        driver.run_once().await;
        assert_eq!(store.complete_calls(), 1);
        assert!(
            *store.renew_calls.lock().unwrap() >= 4,
            "renewals must run during readiness, not only afterward"
        );
        assert!(
            store.effect.lock().unwrap().is_none(),
            "reads have no mutation barrier"
        );
    }

    #[tokio::test]
    async fn lease_loss_cancels_readiness_without_quarantining_read_only_work() {
        let instance = waking_instance("instance-reconcile");
        let pending = pending_materialization("heartbeat-loss", &instance);
        let store = Arc::new(FakeReconcileStore::new(pending, instance));
        let client = FakeKubernetesClient {
            readiness_delay: Duration::from_secs(1),
            ..Default::default()
        };
        let mut driver = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
        driver.config.lease_ttl = Duration::from_millis(30);
        let task = tokio::spawn(async move {
            driver.run_once().await;
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.wait_readiness_calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        *store.renew_result.lock().unwrap() = false;
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(store.complete_calls(), 0);
        assert!(store.effect.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn late_create_after_driver_cancellation_keeps_barrier_and_exclusivity() {
        let instance = waking_instance("instance-reconcile");
        let mut pending = pending_materialization("late-create", &instance);
        pending.exclusivity_keys = vec![RenderedExclusivityKey::new("disk", "singleton")];
        let store = Arc::new(FakeReconcileStore::new(pending, instance));
        let gate = Arc::new(tokio::sync::Notify::new());
        let client = FakeKubernetesClient {
            late_create: Some(gate.clone()),
            ..Default::default()
        };
        let mut driver = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
        driver.config.lease_ttl = Duration::from_millis(30);
        let task = tokio::spawn(async move {
            driver.run_once().await;
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.effect.lock().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        *store.renew_result.lock().unwrap() = false;
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client.apply_calls(), 0, "the API name is still absent");
        *store.renew_result.lock().unwrap() = true;
        let replacement = reconciler(store.clone(), KubernetesMaterializer::new(client.clone()));
        replacement.run_once().await;
        assert!(
            store.effect.lock().unwrap().is_some(),
            "absence cannot clear old dispatched create"
        );
        gate.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.apply_calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        replacement.run_once().await;
        assert_eq!(client.apply_calls(), 1);
        assert_eq!(client.delete_calls(), 0);
        assert_eq!(store.complete_calls(), 0);
        assert_eq!(
            store.exclusivity_keys(),
            vec![RenderedExclusivityKey::new("disk", "singleton")]
        );
        assert!(
            store.effect.lock().unwrap().is_some(),
            "lost acknowledgement requires explicit recovery"
        );
    }

    #[tokio::test]
    async fn uncertain_mutation_is_dispatched_once_through_production_retry_wrappers() {
        let instance = waking_instance("instance-reconcile");
        let pending = pending_materialization("uncertain-once", &instance);
        let store = Arc::new(FakeReconcileStore::new(pending, instance));
        let client = FakeKubernetesClient {
            uncertain_apply: true,
            ..Default::default()
        };
        let driver = MaterializationReconciler::new(
            Arc::new(crate::RetryingControlPlaneStore::with_default_policy(
                store.clone(),
            )),
            KubernetesMaterializer::new(
                crate::materializer::RetryingKubernetesMaterializerClient::with_default_policy(
                    client.clone(),
                ),
            ),
            target(),
            MaterializationReconcilerConfig::default(),
            ObservabilityRecorder::default(),
        );
        driver.run_once().await;
        driver.run_once().await;
        assert_eq!(
            client.apply_calls(),
            1,
            "neither client retries nor replacement scans replay ambiguous effects"
        );
        assert_eq!(store.complete_calls(), 0);
        assert!(store.effect.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn definite_kubernetes_response_recovers_from_ack_failure_before_or_after_commit() {
        for after_commit in [false, true] {
            let instance = waking_instance("instance-reconcile");
            let pending = pending_materialization("ack-retry", &instance);
            let store = Arc::new(FakeReconcileStore::new(pending, instance));
            *store.ack_failure.lock().unwrap() = Some(after_commit);
            let client = FakeKubernetesClient::default();
            let driver = MaterializationReconciler::new(
                Arc::new(crate::RetryingControlPlaneStore::with_default_policy(
                    store.clone(),
                )),
                KubernetesMaterializer::new(
                    crate::materializer::RetryingKubernetesMaterializerClient::with_default_policy(
                        client,
                    ),
                ),
                target(),
                MaterializationReconcilerConfig::default(),
                ObservabilityRecorder::default(),
            );
            driver.run_once().await;
            assert!(
                store.effect.lock().unwrap().is_none(),
                "definite response must not leave a quarantine after recoverable ACK failure"
            );
            if store.complete_calls() == 0 {
                driver.run_once().await;
            }
            assert_eq!(
                store.complete_calls(),
                1,
                "accepted work resumes without an operator action"
            );
        }
    }

    fn deleting_materialization(id: &str) -> MaterializationRecord {
        MaterializationRecord {
            id: MaterializationId::new(id).expect("valid materialization id"),
            instance_id: InstanceId::new("instance-reconcile").expect("valid instance id"),
            instance_generation: Generation::new(7),
            projection_generation: Generation::new(7),
            target: target(),
            state: MaterializationState::Deleting,
            backend: None,
            backend_generation: BackendGeneration::new(7),
            rendered_objects: vec![
                RenderedObjectRef {
                    api_version: "apps/v1".to_owned(),
                    kind: "Deployment".to_owned(),
                    namespace: "apps".to_owned(),
                    name: "instance-reconcile".to_owned(),
                },
                RenderedObjectRef {
                    api_version: "v1".to_owned(),
                    kind: "PersistentVolumeClaim".to_owned(),
                    namespace: "apps".to_owned(),
                    name: "instance-reconcile-data".to_owned(),
                },
            ],
            exclusivity_keys: vec![],
            reconciliation_lease: None,
        }
    }

    fn pending_materialization(id: &str, instance: &InstanceRecord) -> MaterializationRecord {
        let workload_class = workload_class();
        let manifest = render_manifests(RenderManifestRequest {
            template: &workload_class.template,
            instance,
            sleep_policy: workload_class
                .sleep_policy
                .resolve(&instance.values)
                .expect("sleep policy resolves"),
            namespace: "apps",
            template_generation: Some(workload_class.template_generation),
        })
        .expect("manifest renders");
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let rendered_objects = materializer
            .rendered_object_refs(&manifest)
            .expect("rendered refs derive");
        MaterializationRecord {
            id: MaterializationId::new(id).expect("valid materialization id"),
            instance_id: instance.id.clone(),
            instance_generation: instance.generation,
            projection_generation: instance.generation.next(),
            target: target(),
            state: MaterializationState::Pending,
            backend: None,
            backend_generation: BackendGeneration::new(instance.generation.get()),
            rendered_objects,
            exclusivity_keys: vec![],
            reconciliation_lease: None,
        }
    }

    // Draining sits one generation past the materialization stamp (7),
    // mirroring begin_sleep's CAS from Running to Draining.
    fn draining_instance(instance_id: &str) -> InstanceRecord {
        InstanceRecord {
            id: InstanceId::new(instance_id).expect("valid instance id"),
            workload_class: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            values: Default::default(),
            state: InstanceState::Draining,
            generation: Generation::new(8),
        }
    }

    fn waking_instance(instance_id: &str) -> InstanceRecord {
        instance(instance_id, InstanceState::Waking, 7)
    }

    fn running_instance(instance_id: &str, generation: u64) -> InstanceRecord {
        instance(instance_id, InstanceState::Running, generation)
    }

    fn instance(instance_id: &str, state: InstanceState, generation: u64) -> InstanceRecord {
        InstanceRecord {
            id: InstanceId::new(instance_id).expect("valid instance id"),
            workload_class: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            values: InstanceValues::new(),
            state,
            generation: Generation::new(generation),
        }
    }

    fn workload_class() -> WorkloadClassVersion {
        WorkloadClassVersion {
            reference: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            template_generation: Generation::new(3),
            template: ManifestTemplate {
                workload: WorkloadTemplate {
                    kind: WorkloadKind::Deployment,
                    name: TemplateText::literal("app"),
                    replicas: None,
                    app_container: ContainerTemplate {
                        name: "app".to_owned(),
                        image: TemplateText::literal("example/app:1"),
                        ports: vec![ContainerPortTemplate {
                            name: Some("http".to_owned()),
                            container_port: 8080,
                        }],
                        env: vec![EnvVarTemplate {
                            name: "TENANT".to_owned(),
                            value: TemplateText::literal("acme"),
                        }],
                    },
                },
                service: Some(ServiceTemplate {
                    name: TemplateText::literal("svc"),
                    ports: vec![ServicePortTemplate {
                        name: Some("http".to_owned()),
                        port: 80,
                        target_port: 8080,
                    }],
                }),
                sidecar: SidecarTemplate {
                    name: "sleepypods-sidecar".to_owned(),
                    image: TemplateText::literal("sleepypods/sidecar:test"),
                    listen_port: 15000,
                    mode: None,
                },
                volumes: Vec::new(),
                raw_objects: Vec::new(),
            },
            default_values: InstanceValues::new(),
            value_schema: WorkloadValueSchema::new(true),
            sleep_policy: WorkloadSleepPolicy::new(120_000, 5_000, 30_000)
                .expect("valid sleep policy"),
            exclusivity_keys: vec![],
        }
    }

    fn target() -> MaterializationTarget {
        MaterializationTarget::new("cluster-a", "apps").expect("valid target")
    }

    #[derive(Debug)]
    struct BeginRenewProgress {
        started: tokio::sync::Notify,
        renewal_started: tokio::sync::Notify,
        allow_commit: tokio::sync::Semaphore,
        committed: watch::Sender<bool>,
    }

    #[derive(Debug)]
    struct FakeReconcileStore {
        materialization: Mutex<MaterializationRecord>,
        instance: InstanceRecord,
        workload_class: WorkloadClassVersion,
        renew_result: Mutex<bool>,
        delayed_status: Option<(Duration, crate::runtime_work::MaterializationWorkStatus)>,
        status_requested: std::sync::atomic::AtomicBool,
        renew_calls: Mutex<usize>,
        ack_failure: Mutex<Option<bool>>,
        begin_gate: Option<Arc<tokio::sync::Notify>>,
        begin_renew_progress: Option<Arc<BeginRenewProgress>>,
        fatal_scan: std::sync::atomic::AtomicBool,
        failures: Mutex<Vec<bool>>,
        effect: Mutex<Option<crate::materialization::MaterializationEffectRequest>>,
        replace_before_guarded_delete: Mutex<Option<MaterializationRecord>>,
        finalize_calls: Mutex<usize>,
        complete_calls: Mutex<usize>,
        guarded_delete_calls: Mutex<usize>,
        release_calls: Mutex<usize>,
        requested_lease_ttls: Mutex<Vec<Duration>>,
    }

    impl FakeReconcileStore {
        fn new(materialization: MaterializationRecord, instance: InstanceRecord) -> Self {
            Self {
                materialization: Mutex::new(materialization),
                instance,
                workload_class: workload_class(),
                renew_result: Mutex::new(true),
                delayed_status: None,
                status_requested: std::sync::atomic::AtomicBool::new(false),
                renew_calls: Mutex::new(0),
                ack_failure: Mutex::new(None),
                begin_gate: None,
                begin_renew_progress: None,
                fatal_scan: std::sync::atomic::AtomicBool::new(false),
                failures: Mutex::new(Vec::new()),
                effect: Mutex::new(None),
                replace_before_guarded_delete: Mutex::new(None),
                finalize_calls: Mutex::new(0),
                complete_calls: Mutex::new(0),
                guarded_delete_calls: Mutex::new(0),
                release_calls: Mutex::new(0),
                requested_lease_ttls: Mutex::new(Vec::new()),
            }
        }

        fn with_renew_result(self, renew_result: bool) -> Self {
            *self.renew_result.lock().expect("renew lock") = renew_result;
            self
        }

        fn with_replace_before_guarded_delete(
            self,
            materialization: MaterializationRecord,
        ) -> Self {
            *self
                .replace_before_guarded_delete
                .lock()
                .expect("replace before guarded delete lock") = Some(materialization);
            self
        }

        fn finalize_calls(&self) -> usize {
            *self.finalize_calls.lock().expect("finalize lock")
        }

        fn complete_calls(&self) -> usize {
            *self.complete_calls.lock().expect("complete lock")
        }

        fn guarded_delete_calls(&self) -> usize {
            *self
                .guarded_delete_calls
                .lock()
                .expect("guarded delete lock")
        }

        fn release_calls(&self) -> usize {
            *self.release_calls.lock().expect("release lock")
        }

        fn materialization_state(&self) -> MaterializationState {
            self.materialization
                .lock()
                .expect("materialization lock")
                .state
        }

        fn materialization_generation(&self) -> Generation {
            self.materialization
                .lock()
                .expect("materialization lock")
                .instance_generation
        }

        fn exclusivity_keys(&self) -> Vec<RenderedExclusivityKey> {
            self.materialization
                .lock()
                .expect("materialization lock")
                .exclusivity_keys
                .clone()
        }
    }

    impl ControlPlaneStore for FakeReconcileStore {
        unexpected_store_methods!(
            publish_certificate,
            get_certificate_metadata,
            set_tls_binding,
            get_tls_binding,
            remove_certificate,
            resolve_tls_certificate,
            reencrypt_certificate,
            snapshot_tls_bindings,
            load_route_changes,
            load_route_change_revision,
            enqueue_materialization,
            maintain_runtime_records,
            accept_wake,
            request_instance_deletion,
            create_instance,
            delete_instance,
            create_workload_class_version,
            create_route_binding,
            get_route_binding,
            delete_route_binding,
            resolve_route,
            compare_and_swap_instance_state,
            record_materialization,
            load_ready_materialization,
            load_active_materialization,
            load_materialization,
            complete_wake,
            begin_sleep,
            finalize_sleep,
            load_materialization_operational_metrics,
            force_delete_materialization,
            force_release_exclusivity_key,
            lookup_route_dependencies,
            put_http01_challenge,
            resolve_http01_challenge,
            delete_http01_challenge,
            expire_http01_challenges
        );

        fn finalize_instance_deletions(
            &self,
            _limit: usize,
        ) -> StoreFuture<'_, StoreResult<usize>> {
            Box::pin(async {
                if self.fatal_scan.load(std::sync::atomic::Ordering::Relaxed) {
                    Err(StoreError::internal("injected controller failure"))
                } else {
                    Ok(0)
                }
            })
        }
        fn load_materialization_work_status(
            &self,
            _id: MaterializationId,
        ) -> StoreFuture<'_, StoreResult<Option<crate::runtime_work::MaterializationWorkStatus>>>
        {
            Box::pin(async move {
                self.status_requested
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                if let Some((delay, status)) = &self.delayed_status {
                    tokio::time::sleep(*delay).await;
                    Ok(Some(status.clone()))
                } else {
                    Ok(None)
                }
            })
        }
        fn record_materialization_failure(
            &self,
            request: crate::runtime_work::RecordMaterializationFailure,
        ) -> StoreFuture<'_, StoreResult<bool>> {
            if self.materialization.lock().unwrap().state != request.expected_state {
                return Box::pin(async { Ok(false) });
            }
            self.failures.lock().unwrap().push(request.permanent);
            self.release_materialization_reconciliation_lease(
                ReleaseMaterializationReconciliationLeaseRequest::new(
                    request.materialization_id,
                    request.owner,
                    request.attempt,
                    request.generation,
                ),
            )
        }
        fn get_instance<'a>(
            &'a self,
            _request: GetInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
            Box::pin(async move { Ok(Some(self.instance.clone())) })
        }

        fn load_workload_class_version<'a>(
            &'a self,
            _request: LoadWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
            Box::pin(async move { Ok(Some(self.workload_class.clone())) })
        }

        fn list_route_bindings_for_instance<'a>(
            &'a self,
            _request: ListRouteBindingsForInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<Vec<RouteBindingRecord>>> {
            Box::pin(async move { Ok(Vec::new()) })
        }

        fn list_materialization_reconciliation_candidates<'a>(
            &'a self,
            _request: ListMaterializationReconciliationCandidatesRequest,
        ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
            Box::pin(async move {
                Ok(vec![self
                    .materialization
                    .lock()
                    .expect("materialization lock")
                    .clone()])
            })
        }

        fn claim_materialization_reconciliation<'a>(
            &'a self,
            request: ClaimMaterializationReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            Box::pin(async move {
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                if materialization.id != request.materialization_id
                    || self.effect.lock().unwrap().is_some()
                {
                    return Ok(None);
                }
                self.requested_lease_ttls
                    .lock()
                    .unwrap()
                    .push(request.lease_ttl);
                // A store starts the lease from its own clock.
                materialization.reconciliation_lease = Some(MaterializationReconciliationLease {
                    owner: request.owner,
                    expires_at: SystemTime::now() + request.lease_ttl,
                    attempt: 1,
                });
                Ok(Some(materialization.clone()))
            })
        }

        fn renew_materialization_reconciliation_lease<'a>(
            &'a self,
            request: RenewMaterializationReconciliationLeaseRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            Box::pin(async move {
                *self.renew_calls.lock().unwrap() += 1;
                self.requested_lease_ttls
                    .lock()
                    .unwrap()
                    .push(request.lease_ttl);
                if let Some(progress) = &self.begin_renew_progress {
                    let mut committed = progress.committed.subscribe();
                    if self.effect.lock().unwrap().is_some() && !*committed.borrow_and_update() {
                        progress.renewal_started.notify_one();
                        while !*committed.borrow_and_update() {
                            committed.changed().await.expect("commit progress alive");
                        }
                    }
                }
                Ok(*self.renew_result.lock().expect("renew lock")
                    && self.materialization.lock().unwrap().state == request.expected_state)
            })
        }

        fn release_materialization_reconciliation_lease<'a>(
            &'a self,
            request: ReleaseMaterializationReconciliationLeaseRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            Box::pin(async move {
                *self.release_calls.lock().expect("release lock") += 1;
                if self.effect.lock().unwrap().is_some() {
                    return Ok(false);
                }
                let mut record = self.materialization.lock().unwrap();
                if record.instance_generation != request.instance_generation
                    || record.reconciliation_lease.as_ref().is_none_or(|lease| {
                        lease.owner != request.owner || lease.attempt != request.attempt
                    })
                {
                    return Ok(false);
                }
                record.reconciliation_lease = None;
                Ok(true)
            })
        }

        fn complete_wake_reconciliation<'a>(
            &'a self,
            request: CompleteWakeReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
            Box::pin(async move {
                *self.complete_calls.lock().expect("complete lock") += 1;
                let running_generation = request.complete.expected_waking_generation.next();
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                materialization.state = MaterializationState::Ready;
                materialization.instance_generation = running_generation;
                materialization.backend_generation = request.complete.backend_generation;
                materialization.backend = Some(
                    BackendEndpoint::new("http://svc.apps.svc.cluster.local:80")
                        .expect("backend endpoint"),
                );
                materialization.rendered_objects = request.complete.rendered_objects;
                materialization.exclusivity_keys = request.complete.exclusivity_keys;
                materialization.reconciliation_lease = None;
                let mut instance = self.instance.clone();
                instance.state = InstanceState::Running;
                instance.generation = running_generation;
                Ok(CompleteWakeResult {
                    instance,
                    materialization: materialization.clone(),
                })
            })
        }

        fn finalize_sleep_reconciliation<'a>(
            &'a self,
            _request: FinalizeSleepReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
            Box::pin(async move {
                *self.finalize_calls.lock().expect("finalize lock") += 1;
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                materialization.state = MaterializationState::Deleted;
                materialization.rendered_objects.clear();
                materialization.exclusivity_keys.clear();
                materialization.reconciliation_lease = None;
                let mut instance = self.instance.clone();
                instance.state = InstanceState::Cold;
                instance.generation = instance.generation.next();
                Ok(FinalizeSleepResult {
                    instance,
                    materialization: Some(materialization.clone()),
                })
            })
        }

        fn delete_materialization_reconciliation<'a>(
            &'a self,
            request: DeleteMaterializationReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            Box::pin(async move {
                *self
                    .guarded_delete_calls
                    .lock()
                    .expect("guarded delete lock") += 1;
                if let Some(replacement) = self
                    .replace_before_guarded_delete
                    .lock()
                    .expect("replace before guarded delete lock")
                    .take()
                {
                    *self.materialization.lock().expect("materialization lock") = replacement;
                }
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                if materialization.id != request.materialization_id
                    || materialization.state != request.expected_state
                    || materialization.instance_id != request.instance_id
                    || materialization.instance_generation != request.instance_generation
                    || materialization.target != request.target
                    || materialization
                        .reconciliation_lease
                        .as_ref()
                        .map(|lease| lease.owner.as_str())
                        != Some(request.lease_owner.as_str())
                {
                    return Ok(None);
                }
                let previous = materialization.clone();
                materialization.state = MaterializationState::Deleted;
                materialization.rendered_objects.clear();
                materialization.exclusivity_keys.clear();
                materialization.reconciliation_lease = None;
                Ok(Some(previous))
            })
        }

        fn begin_materialization_effect<'a>(
            &'a self,
            request: crate::materialization::MaterializationEffectRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            Box::pin(async move {
                {
                    let mut effect = self.effect.lock().unwrap();
                    if effect.is_some() {
                        return Ok(false);
                    }
                    *effect = Some(request);
                }
                if let Some(progress) = &self.begin_renew_progress {
                    if !*progress.committed.borrow() {
                        progress.started.notify_one();
                        progress
                            .allow_commit
                            .acquire()
                            .await
                            .expect("commit gate alive")
                            .forget();
                        progress.committed.send_replace(true);
                    }
                }
                if let Some(gate) = &self.begin_gate {
                    gate.notified().await;
                }
                Ok(true)
            })
        }
        fn acknowledge_materialization_effect<'a>(
            &'a self,
            request: crate::materialization::AcknowledgeMaterializationEffectRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            Box::pin(async move {
                let failure = self.ack_failure.lock().unwrap().take();
                if failure == Some(false) {
                    return Err(StoreError::Unavailable {
                        message: "transient ACK error before commit".into(),
                    });
                }
                let mut effect = self.effect.lock().unwrap();
                if effect.as_ref().is_some_and(|effect| {
                    effect.effect_id == request.effect_id
                        && effect.attempt == request.attempt
                        && effect.owner == request.owner
                        && effect.instance_generation == request.instance_generation
                }) {
                    *effect = None;
                    if failure == Some(true) {
                        return Err(StoreError::Unavailable {
                            message: "transient ACK reply loss after commit".into(),
                        });
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct FakeKubernetesClient {
        applied: Arc<Mutex<Vec<crate::manifest::KubernetesObject>>>,
        deleted: Arc<Mutex<Vec<RenderedObjectRef>>>,
        wait_readiness_calls: Arc<Mutex<usize>>,
        readiness_delay: Duration,
        readiness_gate: Option<Arc<tokio::sync::Semaphore>>,
        uncertain_apply: bool,
        late_create: Option<Arc<tokio::sync::Notify>>,
        delete_errors: Arc<Mutex<VecDeque<KubernetesClientError>>>,
        live: Arc<Mutex<BTreeMap<String, ProjectionObjectInspection>>>,
    }

    impl FakeKubernetesClient {
        fn with_delete_error(self, error: KubernetesClientError) -> Self {
            self.delete_errors
                .lock()
                .expect("delete errors lock")
                .push_back(error);
            self
        }

        fn with_live_owned_refs(self, materialization: &MaterializationRecord) -> Self {
            for object_ref in &materialization.rendered_objects {
                self.set_live(
                    object_ref.clone(),
                    ProjectionObjectInspection::Present(live_owned_metadata(materialization)),
                );
            }
            self
        }

        fn with_live_applied_projection(
            self,
            materialization: &MaterializationRecord,
            instance: &InstanceRecord,
        ) -> Self {
            let workload_class = workload_class();
            let manifest = render_manifests(RenderManifestRequest {
                template: &workload_class.template,
                instance,
                sleep_policy: workload_class
                    .sleep_policy
                    .resolve(&instance.values)
                    .expect("sleep policy resolves"),
                namespace: materialization.target.namespace(),
                template_generation: Some(workload_class.template_generation),
            })
            .expect("manifest renders");
            let plan = ProjectionPlan::from_manifest(materialization, &manifest)
                .expect("projection plan builds");
            for rendered in &plan.manifest().expect("projection manifest").objects {
                let object_ref = crate::materializer::rendered_object_ref(&rendered.object);
                self.set_live(
                    object_ref,
                    ProjectionObjectInspection::Present(LiveObjectMetadata::from_rendered_object(
                        &rendered.object,
                    )),
                );
            }
            self
        }

        fn with_live_unowned_ref(self, object_ref: RenderedObjectRef) -> Self {
            self.set_live(
                object_ref,
                ProjectionObjectInspection::Present(LiveObjectMetadata {
                    persistent_volume_reclaim_policy: Some("Retain".into()),
                    identity: crate::projection::LiveObjectIdentity {
                        uid: "test-uid".into(),
                        resource_version: "1".into(),
                    },
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                    deleting: false,
                    finalizers: Vec::new(),
                }),
            );
            self
        }

        fn with_live_deleting_owned_ref(
            self,
            materialization: &MaterializationRecord,
            object_ref: RenderedObjectRef,
            finalizers: Vec<String>,
        ) -> Self {
            self.set_live(
                object_ref,
                ProjectionObjectInspection::Present(
                    live_owned_metadata(materialization).deleting(finalizers),
                ),
            );
            self
        }

        fn set_live(&self, object_ref: RenderedObjectRef, inspection: ProjectionObjectInspection) {
            self.live
                .lock()
                .expect("live lock")
                .insert(object_key(&object_ref), inspection);
        }

        fn apply_calls(&self) -> usize {
            self.applied.lock().expect("applied lock").len()
        }

        fn delete_calls(&self) -> usize {
            self.deleted.lock().expect("deleted lock").len()
        }

        fn wait_readiness_calls(&self) -> usize {
            *self
                .wait_readiness_calls
                .lock()
                .expect("wait readiness lock")
        }
    }

    impl KubernetesMaterializerClient for FakeKubernetesClient {
        fn apply_object<'a>(
            &'a self,
            object: &'a crate::manifest::KubernetesObject,
            _precondition: Option<&'a crate::projection::LiveObjectIdentity>,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                let object_ref = crate::materializer::rendered_object_ref(object);
                if let Some(gate) = &self.late_create {
                    let gate = gate.clone();
                    let live = self.live.clone();
                    let applied = self.applied.clone();
                    let object = object.clone();
                    let (sender, receiver) = tokio::sync::oneshot::channel();
                    tokio::spawn(async move {
                        gate.notified().await;
                        applied.lock().unwrap().push(object.clone());
                        live.lock().unwrap().insert(
                            object_key(&object_ref),
                            ProjectionObjectInspection::Present(
                                LiveObjectMetadata::from_rendered_object(&object),
                            ),
                        );
                        let _ = sender.send(());
                    });
                    return receiver
                        .await
                        .map_err(|_| KubernetesClientError::uncertain("mock API reply lost"));
                }
                self.applied
                    .lock()
                    .expect("applied lock")
                    .push(object.clone());
                self.live.lock().expect("live lock").insert(
                    object_key(&object_ref),
                    ProjectionObjectInspection::Present(LiveObjectMetadata::from_rendered_object(
                        object,
                    )),
                );
                if self.uncertain_apply {
                    return Err(KubernetesClientError::uncertain(
                        "API reply lost after create",
                    ));
                }
                Ok(())
            })
        }

        fn delete_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
            _precondition: &'a crate::projection::LiveObjectIdentity,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                match self
                    .delete_errors
                    .lock()
                    .expect("delete errors lock")
                    .pop_front()
                {
                    Some(error) => Err(error),
                    None => {
                        self.deleted
                            .lock()
                            .expect("deleted lock")
                            .push(object.clone());
                        self.live
                            .lock()
                            .expect("live lock")
                            .remove(&object_key(object));
                        Ok(())
                    }
                }
            })
        }

        fn wait_for_pvc_bound<'a>(
            &'a self,
            _namespace: &'a str,
            _name: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn wait_for_readiness<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
            Box::pin(async move {
                *self
                    .wait_readiness_calls
                    .lock()
                    .expect("wait readiness lock") += 1;
                if let Some(gate) = &self.readiness_gate {
                    gate.acquire().await.expect("readiness gate open").forget();
                }
                tokio::time::sleep(self.readiness_delay).await;
                BackendEndpoint::new("http://svc.apps.svc.cluster.local:80")
                    .map_err(|error| KubernetesClientError::new(error.to_string()))
            })
        }

        fn inspect_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>>
        {
            Box::pin(async move {
                Ok(self
                    .live
                    .lock()
                    .expect("live lock")
                    .get(&object_key(object))
                    .cloned()
                    .unwrap_or(ProjectionObjectInspection::Missing))
            })
        }

        fn ensure_no_descendants<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
            _instance_id: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_retained_bindings<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> crate::materializer::KubernetesClientFuture<
            'a,
            crate::materializer::KubernetesClientResult<()>,
        > {
            Box::pin(async { Ok(()) })
        }
    }

    fn live_owned_metadata(materialization: &MaterializationRecord) -> LiveObjectMetadata {
        LiveObjectMetadata {
            persistent_volume_reclaim_policy: Some("Retain".into()),
            identity: crate::projection::LiveObjectIdentity {
                uid: "test-uid".into(),
                resource_version: "1".into(),
            },
            labels: BTreeMap::from([
                (
                    crate::projection::LABEL_MANAGED_BY.to_owned(),
                    crate::projection::LABEL_MANAGED_BY_VALUE.to_owned(),
                ),
                (
                    crate::manifest::LABEL_INSTANCE_ID.to_owned(),
                    materialization.instance_id.as_str().to_owned(),
                ),
                (
                    crate::manifest::LABEL_INSTANCE_GENERATION.to_owned(),
                    materialization.projection_generation.to_string(),
                ),
            ]),
            annotations: BTreeMap::from([(
                crate::projection::ANNOTATION_MATERIALIZATION_ID.to_owned(),
                materialization.id.as_str().to_owned(),
            )]),
            deleting: false,
            finalizers: Vec::new(),
        }
    }

    fn object_key(object_ref: &RenderedObjectRef) -> String {
        format!(
            "{}|{}|{}|{}",
            object_ref.api_version, object_ref.kind, object_ref.namespace, object_ref.name
        )
    }
}
