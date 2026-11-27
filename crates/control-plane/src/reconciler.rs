mod fenced_client;

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use sleepypods_observability::{
    metrics::{
        CONTROL_PLANE_STORE_AVAILABLE, KUBERNETES_OPERATIONS_TOTAL,
        KUBERNETES_OPERATION_DURATION_SECONDS, RECONCILER_CLAIMS_TOTAL,
        RECONCILER_LEASE_RENEWALS_TOTAL, RECONCILER_RUNS_TOTAL, RECONCILER_RUN_DURATION_SECONDS,
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
        BackendEndpoint, ClaimNextMaterializationRequest, CompleteWakeReconciliationRequest,
        CompleteWakeRequest, DeleteMaterializationReconciliationRequest,
        FinalizeSleepReconciliationRequest, FinalizeSleepRequest, MaterializationRecord,
        MaterializationState, ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient, MaterializerError},
    projection::{ProjectionError, ProjectionPlan, ProjectionReconciler},
    store::{ControlPlaneStore, StoreError, StoreResult},
    workload::LoadWorkloadClassVersionRequest,
};

pub const EVENT_MATERIALIZATION_RECONCILIATION: &str = "runtime.materialization.reconciliation";
pub const WAKE_LIMIT_ENV: &str = "SLEEPYPODS_RECONCILER_WAKE_LIMIT";
pub const CLEANUP_LIMIT_ENV: &str = "SLEEPYPODS_RECONCILER_CLEANUP_LIMIT";

/// Instances finalized per scan once their deletion's cleanup has finished.
const DELETION_BATCH: usize = 32;
/// The longest a failing store is left alone before the next scan.
const STORE_BACKOFF_MAX: Duration = Duration::from_secs(15);
/// How long cancelled work has to reach its next step and stop.
const WORK_STOP_GRACE: Duration = Duration::from_secs(15);
const LEASE_RETRY_MIN: Duration = Duration::from_millis(100);
const LEASE_RETRY_MAX: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationReconcilerConfig {
    pub owner: String,
    /// How often the reconciler looks for work no signal announced: work
    /// another replica accepted, a retry that came due, an elapsed drain.
    pub interval: Duration,
    pub lease_ttl: Duration,
    /// Wakes this replica drives at once. A wake holds its place while it
    /// waits for readiness, so the limit covers a burst of cold instances.
    pub wake_limit: usize,
    /// Cleanups this replica drives at once, counted apart from wakes.
    pub cleanup_limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidReconcilerLimit {
    name: &'static str,
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

/// A claimed lease, and the local time until which it is known to last.
///
/// The database decides ownership. The local expiry only bounds how long a
/// failing store is retried: until then the lease may still be held, so the
/// work continues; after it the lease can have passed to another driver.
pub(crate) struct HeldLease {
    store: Arc<dyn ControlPlaneStore>,
    renewal: RenewMaterializationReconciliationLeaseRequest,
    expires: Mutex<tokio::time::Instant>,
    observability: ObservabilityRecorder,
}

impl Default for MaterializationReconcilerConfig {
    fn default() -> Self {
        Self {
            owner: default_owner(),
            interval: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(60),
            wake_limit: 256,
            cleanup_limit: 64,
        }
    }
}

impl MaterializationReconcilerConfig {
    /// The defaults, with each limit the environment names.
    pub fn from_env() -> Result<Self, InvalidReconcilerLimit> {
        Self::from_values(|name| std::env::var(name).ok())
    }

    fn from_values(
        value: impl Fn(&'static str) -> Option<String>,
    ) -> Result<Self, InvalidReconcilerLimit> {
        let mut config = Self::default();
        for (name, limit) in [
            (WAKE_LIMIT_ENV, &mut config.wake_limit),
            (CLEANUP_LIMIT_ENV, &mut config.cleanup_limit),
        ] {
            if let Some(value) = value(name).filter(|value| !value.trim().is_empty()) {
                *limit = value
                    .trim()
                    .parse::<usize>()
                    .ok()
                    .filter(|limit| (1..=4096).contains(limit))
                    .ok_or(InvalidReconcilerLimit { name })?;
            }
        }
        Ok(config)
    }
}

impl fmt::Display for InvalidReconcilerLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} must be an integer from 1 to 4096", self.name)
    }
}

impl std::error::Error for InvalidReconcilerLimit {}

impl HeldLease {
    fn new(
        store: Arc<dyn ControlPlaneStore>,
        claimed: &MaterializationRecord,
        config: &MaterializationReconcilerConfig,
        claim_started: tokio::time::Instant,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            store,
            renewal: RenewMaterializationReconciliationLeaseRequest::new(
                claimed.id.clone(),
                config.owner.clone(),
                claimed
                    .reconciliation_lease
                    .as_ref()
                    .expect("claimed lease")
                    .attempt,
                claimed.instance_generation,
                config.lease_ttl,
                claimed.state,
            ),
            expires: Mutex::new(claim_started + config.lease_ttl),
            observability,
        }
    }

    fn attempt(&self) -> u64 {
        self.renewal.attempt
    }

    fn expires(&self) -> tokio::time::Instant {
        *self.expires.lock().expect("lease expiry")
    }

    /// Renews the lease. The store's refusal loses it. A store failure is
    /// retried until the lease's own expiry, and only then counts as loss.
    pub(crate) async fn renew(&self) -> Result<(), MaterializationReconcileError> {
        let mut pause = LEASE_RETRY_MIN;
        loop {
            let started = tokio::time::Instant::now();
            match self
                .store
                .renew_materialization_reconciliation_lease(self.renewal.clone())
                .await
            {
                Ok(true) => {
                    *self.expires.lock().expect("lease expiry") = started + self.renewal.lease_ttl;
                    self.record_renewal(Outcome::Success);
                    return Ok(());
                }
                Ok(false) => {
                    self.record_renewal(Outcome::Rejected);
                    return Err(MaterializationReconcileError::LeaseLost);
                }
                Err(_) => {
                    self.record_renewal(Outcome::Error);
                    if tokio::time::Instant::now() + pause >= self.expires() {
                        return Err(MaterializationReconcileError::LeaseLost);
                    }
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(LEASE_RETRY_MAX);
                }
            }
        }
    }

    /// Runs a store call, repeating it while the store is unavailable and the
    /// lease can still be held.
    async fn retry<T, F>(&self, mut call: impl FnMut() -> F) -> StoreResult<T>
    where
        F: Future<Output = StoreResult<T>>,
    {
        let mut pause = LEASE_RETRY_MIN;
        loop {
            match call().await {
                Err(error)
                    if error.is_retryable()
                        && tokio::time::Instant::now() + pause < self.expires() =>
                {
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(LEASE_RETRY_MAX);
                }
                result => return result,
            }
        }
    }

    fn record_renewal(&self, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RECONCILER_LEASE_RENEWALS_TOTAL,
            vec![outcome.metric_label()],
            1.0,
        ));
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

    /// Drives lifecycle work until shutdown.
    ///
    /// The loop claims every due materialization its limits allow, then waits
    /// for a reason to look again: a signal that this process committed work,
    /// a finished job, or the periodic scan that finds what another replica
    /// committed. While the store fails it backs off and keeps retrying.
    pub async fn run_until_shutdown(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), StoreError> {
        let cancelled = crate::runtime_work::Cancellation::new();
        let mut jobs = JoinSet::new();
        let mut active = HashMap::new();
        let mut work = crate::runtime_work::lifecycle_work();
        let mut ticker =
            tokio::time::interval(self.interval_with_jitter().max(Duration::from_millis(10)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut store_failures = 0_u32;
        let mut periodic = true;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let scan_started = Instant::now();
            match self
                .start_due_work(&mut jobs, &mut active, &cancelled, periodic)
                .await
            {
                Ok(()) => {
                    store_failures = 0;
                    self.record_store_available(true);
                    self.record_reconciler_run(Outcome::Success, scan_started.elapsed());
                }
                Err(error) => {
                    store_failures = store_failures.saturating_add(1);
                    self.record_store_available(false);
                    self.record_reconciler_run(Outcome::Error, scan_started.elapsed());
                    self.record_outcome("scan", Outcome::Error, Some(&error.to_string()));
                }
            }
            periodic = false;
            let backoff = store_backoff(self.config.interval, store_failures);
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                finished = jobs.join_next_with_id(), if !jobs.is_empty() => {
                    let id = match finished.expect("a job is running") {
                        Ok((id, ())) => id,
                        Err(error) => {
                            self.record_failed_attempt(&error.to_string());
                            error.id()
                        }
                    };
                    active.remove(&id);
                }
                // A signal says nothing about a failing store's recovery.
                _ = tokio::time::sleep(backoff.unwrap_or_default()), if backoff.is_some() => {
                    periodic = true;
                }
                _ = work.changed(), if backoff.is_none() => {}
                _ = ticker.tick(), if backoff.is_none() => {
                    periodic = true;
                }
            }
        }
        cancelled.cancel();
        // Each job stops at its next step and releases its lease.
        let drained = tokio::time::timeout(Duration::from_secs(20), async {
            while jobs.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            jobs.abort_all();
            while jobs.join_next().await.is_some() {}
            return Err(StoreError::unavailable(
                "reconciliation shutdown deadline exceeded",
            ));
        }
        Ok(())
    }

    /// Claims due wakes and due cleanups, each up to its own limit, and starts
    /// a job for every claim. The periodic scan also finalizes instance
    /// deletions whose cleanup has finished.
    async fn start_due_work(
        &self,
        jobs: &mut JoinSet<()>,
        active: &mut HashMap<tokio::task::Id, MaterializationState>,
        cancelled: &crate::runtime_work::Cancellation,
        periodic: bool,
    ) -> Result<(), StoreError> {
        if periodic {
            self.store
                .finalize_instance_deletions(DELETION_BATCH)
                .await?;
        }
        for (state, limit) in [
            (MaterializationState::Pending, self.config.wake_limit),
            (MaterializationState::Deleting, self.config.cleanup_limit),
        ] {
            while active.values().filter(|held| **held == state).count() < limit.max(1) {
                let claim_started = tokio::time::Instant::now();
                let Some(claimed) = self
                    .store
                    .claim_next_materialization_reconciliation(
                        ClaimNextMaterializationRequest::new(
                            state,
                            self.target.clone(),
                            self.config.owner.clone(),
                            self.config.lease_ttl,
                        ),
                    )
                    .await?
                else {
                    break;
                };
                self.record_claim(state);
                let mut job = self.clone();
                // Cancellation belongs to this claimed job, never its siblings.
                job.cancellation = crate::runtime_work::Cancellation::new();
                let cancelled = cancelled.clone();
                let handle = jobs.spawn(async move {
                    job.reconcile_claimed(claimed, claim_started, &cancelled)
                        .await;
                });
                active.insert(handle.id(), state);
            }
        }
        Ok(())
    }

    async fn reconcile_claimed(
        &self,
        claimed: MaterializationRecord,
        claim_started: tokio::time::Instant,
        cancelled: &crate::runtime_work::Cancellation,
    ) {
        let lease = Arc::new(HeldLease::new(
            self.store.clone(),
            &claimed,
            &self.config,
            claim_started,
            self.observability.clone(),
        ));
        match self
            .reconcile_with_heartbeat(&claimed, &lease, cancelled)
            .await
        {
            Ok(()) => {
                self.record_outcome("reconcile", Outcome::Success, None);
                return; // Completion consumes the lease.
            }
            // A renewal is refused when another driver holds the lease, and
            // also when an accepted Delete changed the state this attempt
            // claimed. The exact release below is a no-op in the first case
            // and hands the row to cleanup at once in the second.
            Err(MaterializationReconcileError::LeaseLost) => {
                self.record_outcome("lease_lost", Outcome::Rejected, None);
            }
            Err(MaterializationReconcileError::Cancelled) => {}
            Err(error) => {
                self.record_failed_attempt(&error.to_string());
                let failure = crate::runtime_work::RecordMaterializationFailure {
                    expected_state: claimed.state,
                    materialization_id: claimed.id.clone(),
                    owner: self.config.owner.clone(),
                    attempt: lease.attempt(),
                    generation: claimed.instance_generation,
                    permanent: error.permanent(claimed.state),
                    message: error.to_string(),
                };
                let recorded = lease
                    .retry(|| self.store.record_materialization_failure(failure.clone()))
                    .await;
                if matches!(recorded, Ok(true)) {
                    return; // A recorded failure consumes the lease.
                }
            }
        }

        // The work has stopped. Release this exact claim so the next attempt
        // need not wait for the lease to expire.
        let release = ReleaseMaterializationReconciliationLeaseRequest::new(
            claimed.id.clone(),
            self.config.owner.clone(),
            lease.attempt(),
            claimed.instance_generation,
        );
        let _ = lease
            .retry(|| {
                self.store
                    .release_materialization_reconciliation_lease(release.clone())
            })
            .await;
    }

    async fn reconcile_with_heartbeat(
        &self,
        claimed: &MaterializationRecord,
        lease: &Arc<HeldLease>,
        cancelled: &crate::runtime_work::Cancellation,
    ) -> Result<(), MaterializationReconcileError> {
        if cancelled.is_cancelled() {
            return Err(MaterializationReconcileError::Cancelled);
        }
        // A wake ends at its operation deadline. Cleanup has one acceptable
        // outcome and runs until it reaches it.
        let budget = match claimed.state {
            MaterializationState::Pending => Some(self.wake_budget(claimed, lease).await?),
            _ => None,
        };
        let deadline = tokio::time::sleep(budget.unwrap_or_default());
        tokio::pin!(deadline);
        let work = async {
            match claimed.state {
                MaterializationState::Pending => {
                    self.reconcile_pending(claimed.clone(), lease).await
                }
                MaterializationState::Deleting => {
                    self.reconcile_deleting(claimed.clone(), lease).await
                }
                _ => Ok(()),
            }
        };
        tokio::pin!(work);
        let period = (self.config.lease_ttl / 3).max(Duration::from_millis(1));
        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Renewal is polled alongside the work as one owned future, so
            // neither waits on the other.
            let renewal = async {
                heartbeat.tick().await;
                lease.renew().await
            };
            tokio::pin!(renewal);
            let stopped = tokio::select! {
                biased;
                result = &mut work => return result,
                _ = cancelled.cancelled() => MaterializationReconcileError::Cancelled,
                _ = &mut deadline, if budget.is_some() => MaterializationReconcileError::Deadline,
                renewed = &mut renewal => match renewed {
                    Ok(()) => continue,
                    Err(lost) => lost,
                },
            };
            // The work stops at its next step; a mutation already dispatched
            // runs to its own timeout first.
            self.cancellation.cancel();
            let _ = tokio::time::timeout(WORK_STOP_GRACE, &mut work).await;
            return Err(stopped);
        }
    }

    async fn wake_budget(
        &self,
        claimed: &MaterializationRecord,
        lease: &HeldLease,
    ) -> Result<Duration, MaterializationReconcileError> {
        let requested = tokio::time::Instant::now();
        let status = lease
            .retry(|| {
                self.store
                    .load_materialization_work_status(claimed.id.clone())
            })
            .await
            .map_err(MaterializationReconcileError::Store)?;
        let remaining = operation_budget(status, requested.elapsed())?;
        if remaining.is_zero() {
            return Err(MaterializationReconcileError::Deadline);
        }
        Ok(remaining)
    }

    fn fenced_materializer(
        &self,
        lease: &Arc<HeldLease>,
    ) -> KubernetesMaterializer<fenced_client::FencedKubernetesClient<C>> {
        KubernetesMaterializer::new(fenced_client::FencedKubernetesClient {
            inner: self.materializer.client().clone(),
            cancellation: self.cancellation.clone(),
            lease: lease.clone(),
        })
    }

    async fn reconcile_pending(
        &self,
        materialization: MaterializationRecord,
        lease: &Arc<HeldLease>,
    ) -> Result<(), MaterializationReconcileError> {
        let Some(instance) = self
            .load_instance(materialization.instance_id.clone(), lease)
            .await?
        else {
            return self
                .delete_refs_and_mark_deleted(materialization, lease)
                .await;
        };

        if instance.generation != materialization.instance_generation
            || matches!(
                instance.state,
                InstanceState::Deleting | InstanceState::Deleted
            )
        {
            return self
                .delete_refs_and_mark_deleted(materialization, lease)
                .await;
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
        let manifest = self
            .render_current_manifest(&projected_instance, &materialization, lease)
            .await?;
        let projection_plan = ProjectionPlan::from_manifest(&materialization, &manifest)
            .map_err(MaterializationReconcileError::Materializer)?;
        let desired_refs = projection_plan.object_refs();
        if desired_refs != materialization.rendered_objects {
            return Err(MaterializationReconcileError::StaleDesiredRefs);
        }

        self.apply_projection_observed(&projection_plan, lease)
            .await?;
        let backend = self
            .wait_for_projection_readiness_observed(&projection_plan, lease)
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

        let mut complete = CompleteWakeRequest::new(
            materialization.instance_id.clone(),
            materialization.instance_generation,
            materialization.target.clone(),
            backend,
            materialization.backend_generation,
        );
        // Recorded beside the Service address; a failed read leaves it unset.
        complete.pod_backend = self
            .materializer
            .client()
            .ready_pod_backend(&materialization.rendered_objects)
            .await
            .ok()
            .flatten();
        complete.rendered_objects = materialization.rendered_objects.clone();
        complete.exclusivity_keys = materialization.exclusivity_keys.clone();
        let request = CompleteWakeReconciliationRequest::new(
            materialization.id.clone(),
            self.config.owner.clone(),
            lease.attempt(),
            complete,
        );
        let result = lease
            .retry(|| self.store.complete_wake_reconciliation(request.clone()))
            .await
            .map_err(MaterializationReconcileError::Store)?;
        self.notify_instance_routes_changed(result.instance.id)
            .await?;
        Ok(())
    }

    async fn reconcile_deleting(
        &self,
        materialization: MaterializationRecord,
        lease: &Arc<HeldLease>,
    ) -> Result<(), MaterializationReconcileError> {
        let projection_plan = ProjectionPlan::from_recorded_refs(&materialization);
        self.delete_projection_observed(&projection_plan, lease)
            .await?;

        let Some(instance) = self
            .load_instance(materialization.instance_id.clone(), lease)
            .await?
        else {
            return self.mark_deleted(materialization, lease).await;
        };
        // begin_sleep leaves the materialization stamped with the Running
        // generation and moves the instance to Draining at Running + 1, so
        // the drain this row belongs to is current exactly when the instance
        // is Draining one generation past the stamp. Anything else means the
        // instance moved on (failed, or is being deleted) and the row is
        // cleanup debris.
        if instance.state != InstanceState::Draining
            || instance.generation != materialization.instance_generation.next()
        {
            return self.mark_deleted(materialization, lease).await;
        }

        let request = FinalizeSleepReconciliationRequest::new(
            materialization.id.clone(),
            self.config.owner.clone(),
            lease.attempt(),
            FinalizeSleepRequest::new(instance.id, instance.generation, materialization.target),
        );
        let result = lease
            .retry(|| self.store.finalize_sleep_reconciliation(request.clone()))
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
        lease: &Arc<HeldLease>,
    ) -> Result<(), MaterializationReconcileError> {
        let projection_plan = ProjectionPlan::from_recorded_refs(&materialization);
        self.delete_projection_observed(&projection_plan, lease)
            .await?;
        self.mark_deleted(materialization, lease).await
    }

    async fn mark_deleted(
        &self,
        materialization: MaterializationRecord,
        lease: &HeldLease,
    ) -> Result<(), MaterializationReconcileError> {
        let request = DeleteMaterializationReconciliationRequest::new(
            materialization.id.clone(),
            self.config.owner.clone(),
            lease.attempt(),
            materialization.state,
            materialization.instance_id,
            materialization.instance_generation,
            materialization.target,
        );
        lease
            .retry(|| {
                self.store
                    .delete_materialization_reconciliation(request.clone())
            })
            .await
            .map(|_| ())
            .map_err(MaterializationReconcileError::Store)
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
        lease: &Arc<HeldLease>,
    ) -> Result<(), MaterializationReconcileError> {
        let started = Instant::now();
        let fenced = self.fenced_materializer(lease);
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
        lease: &Arc<HeldLease>,
    ) -> Result<BackendEndpoint, MaterializationReconcileError> {
        let started = Instant::now();
        let fenced = self.fenced_materializer(lease);
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
        lease: &Arc<HeldLease>,
    ) -> Result<(), MaterializationReconcileError> {
        let started = Instant::now();
        let fenced = self.fenced_materializer(lease);
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
        lease: &HeldLease,
    ) -> Result<Option<InstanceRecord>, MaterializationReconcileError> {
        lease
            .retry(|| {
                self.store
                    .get_instance(GetInstanceRequest::new(instance_id.clone()))
            })
            .await
            .map_err(MaterializationReconcileError::Store)
    }

    async fn render_current_manifest(
        &self,
        instance: &InstanceRecord,
        materialization: &MaterializationRecord,
        lease: &HeldLease,
    ) -> Result<crate::manifest::RenderedManifest, MaterializationReconcileError> {
        let workload_class = lease
            .retry(|| {
                self.store
                    .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
                        instance.workload_class.clone(),
                    ))
            })
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
        let mut fields = vec![
            LogField::new("operation", operation),
            LogField::new("outcome", outcome.as_str()),
        ];
        fields.extend(error.map(LogField::error_reason));
        self.observability.record_log(LifecycleLogEvent::new(
            EVENT_MATERIALIZATION_RECONCILIATION,
            fields,
        ));
    }

    /// Counts an attempt that ended without finishing its work.
    fn record_failed_attempt(&self, error: &str) {
        self.record_outcome("reconcile", Outcome::Error, Some(error));
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

    /// One while the reconciler's last scan reached the store, zero while the
    /// store is unavailable and the scan is being retried.
    fn record_store_available(&self, available: bool) {
        self.observability.record_metric(MetricObservation::new(
            CONTROL_PLANE_STORE_AVAILABLE,
            vec![],
            if available { 1.0 } else { 0.0 },
        ));
    }

    fn record_claim(&self, state: MaterializationState) {
        self.observability.record_metric(MetricObservation::new(
            RECONCILER_CLAIMS_TOTAL,
            vec![state.metric_label(), Outcome::Success.metric_label()],
            1.0,
        ));
        self.record_outcome("claim", Outcome::Success, None);
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

/// How long to leave a failing store alone: the scan interval doubled per
/// consecutive failure, capped.
fn store_backoff(interval: Duration, failures: u32) -> Option<Duration> {
    (failures > 0).then(|| {
        interval
            .saturating_mul(1 << failures.min(8))
            .min(STORE_BACKOFF_MAX)
    })
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
    /// Whether another attempt at the same work cannot succeed.
    ///
    /// Cleanup retries until it succeeds: only an object another owner holds
    /// parks it. A wake that cannot succeed ends, so its instance becomes
    /// Failed and the next request starts a fresh wake.
    fn permanent(&self, state: MaterializationState) -> bool {
        fn materializer(error: &MaterializerError) -> bool {
            match error {
                MaterializerError::InvalidManifest { .. } => true,
                MaterializerError::Apply { source, .. }
                | MaterializerError::Delete { source, .. } => source.is_permanent(),
                MaterializerError::ReadinessWait { .. } => false,
            }
        }
        if state == MaterializationState::Deleting {
            return matches!(
                self,
                Self::Projection(ProjectionError::OwnershipConflict { .. })
            );
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
            Self::Projection(ProjectionError::Inspect { source, .. }) => source.is_permanent(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests;
