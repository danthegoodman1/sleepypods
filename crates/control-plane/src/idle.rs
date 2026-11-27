use std::time::Duration;

use crate::{
    ids::{Generation, InstanceId},
    instance::{GetInstanceRequest, InstanceRecord, InstanceState},
    materialization::{
        BeginSleepRequest, LoadActiveMaterializationRequest, MaterializationState,
        MaterializationTarget,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    store::{ControlPlaneStore, StoreError},
    workload::LoadWorkloadClassVersionRequest,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportIdleRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
    pub active_count: u64,
    pub pod_uid: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportIdleResult {
    Accepted {
        instance: InstanceRecord,
    },
    AlreadyDraining {
        instance: InstanceRecord,
    },
    Unavailable {
        instance: InstanceRecord,
        reason: ReportIdleUnavailableReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReportIdleUnavailableReason {
    Cold,
    Waking,
    Draining,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Debug)]
pub enum ReportIdleError {
    ActiveRequestsPresent {
        active_count: u64,
    },
    NotFound,
    WorkloadClassNotFound,
    SleepPolicy(String),
    UnsupportedSleep(String),
    GenerationConflict {
        expected: Generation,
        actual: Generation,
    },
    Store(StoreError),
}

impl ReportIdleRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_generation: Generation,
        active_count: u64,
    ) -> Self {
        Self {
            instance_id,
            expected_generation,
            active_count,
            pod_uid: String::new(),
        }
    }
}

pub async fn report_idle<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: MaterializationTarget,
    request: ReportIdleRequest,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    if request.active_count > 0 {
        return Err(ReportIdleError::ActiveRequestsPresent {
            active_count: request.active_count,
        });
    }

    let current = load_instance(store, request.instance_id.clone()).await?;
    match current.state {
        InstanceState::Running => {
            verify_idle_member(store, materializer, &target, &request, &current).await?;
            begin_running_sleep(store, target, request, &current).await
        }
        InstanceState::Draining => already_draining(store, &target, &request, current).await,
        state => Ok(ReportIdleResult::Unavailable {
            instance: current,
            reason: unavailable_reason_for_state(state),
        }),
    }
}

/// Answers a report for an instance that is already draining.
///
/// A sidecar holds its projection generation for the Pod's life, while the
/// instance's revision advances with every transition, a revive included. The
/// report is the draining Pod's own when the projection matches.
async fn already_draining<S>(
    store: &S,
    target: &MaterializationTarget,
    request: &ReportIdleRequest,
    current: InstanceRecord,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    let projection = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            current.id.clone(),
            target.clone(),
        ))
        .await
        .map_err(ReportIdleError::Store)?
        .map(|materialization| materialization.projection_generation);
    match projection {
        Some(projection) if projection == request.expected_generation => {
            Ok(ReportIdleResult::AlreadyDraining { instance: current })
        }
        Some(actual) => Err(ReportIdleError::GenerationConflict {
            expected: request.expected_generation,
            actual,
        }),
        None => Ok(ReportIdleResult::Unavailable {
            instance: current,
            reason: ReportIdleUnavailableReason::Draining,
        }),
    }
}

async fn begin_running_sleep<S>(
    store: &S,
    target: MaterializationTarget,
    request: ReportIdleRequest,
    current: &InstanceRecord,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    let sleep_policy = sleep_policy(store, current).await?;
    let minimum_ready_age = Duration::from_millis(sleep_policy.idle_timeout_ms)
        .max(sleepypods_api::INITIAL_ACTIVATION_TIMEOUT);
    let begin = match store
        .begin_sleep(
            BeginSleepRequest::new(
                request.instance_id.clone(),
                current.generation,
                target.clone(),
            )
            .with_drain_grace_timeout(Duration::from_millis(sleep_policy.drain_grace_timeout_ms))
            .with_minimum_ready_age(minimum_ready_age),
        )
        .await
    {
        Ok(begin) => Ok(begin),
        Err(StoreError::NotFound { .. }) => Err(ReportIdleError::NotFound),
        // Another report or lifecycle request moved the instance first.
        Err(StoreError::GenerationConflict { expected, actual }) => {
            let current = load_instance(store, request.instance_id.clone()).await?;
            if current.state == InstanceState::Draining {
                return already_draining(store, &target, &request, current).await;
            }
            Err(ReportIdleError::GenerationConflict { expected, actual })
        }
        Err(error) => Err(ReportIdleError::Store(error)),
    }?;

    Ok(ReportIdleResult::Accepted {
        instance: begin.instance,
    })
}

async fn verify_idle_member<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: &MaterializationTarget,
    request: &ReportIdleRequest,
    current: &InstanceRecord,
) -> Result<(), ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    if request.pod_uid.trim().is_empty() {
        return Err(ReportIdleError::UnsupportedSleep(
            "pod_uid is required; legacy sidecars cannot authorize automatic sleep".to_owned(),
        ));
    }
    let class = store
        .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
            current.workload_class.clone(),
        ))
        .await
        .map_err(ReportIdleError::Store)?
        .ok_or(ReportIdleError::WorkloadClassNotFound)?;
    class
        .validate()
        .map_err(|error| ReportIdleError::UnsupportedSleep(error.to_string()))?;
    let materialization = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            current.id.clone(),
            target.clone(),
        ))
        .await
        .map_err(ReportIdleError::Store)?
        .ok_or_else(|| {
            ReportIdleError::UnsupportedSleep(
                "automatic sleep requires an active Ready materialization".to_owned(),
            )
        })?;
    if materialization.state != MaterializationState::Ready
        || materialization.instance_generation != current.generation
    {
        return Err(ReportIdleError::UnsupportedSleep(
            "active materialization does not match the running generation".to_owned(),
        ));
    }
    if materialization.projection_generation != request.expected_generation {
        return Err(ReportIdleError::GenerationConflict {
            expected: request.expected_generation,
            actual: materialization.projection_generation,
        });
    }
    let policy = class
        .sleep_policy
        .resolve(&current.values)
        .map_err(|error| ReportIdleError::SleepPolicy(error.to_string()))?;
    let minimum_ready_age = Duration::from_millis(policy.idle_timeout_ms)
        .max(sleepypods_api::INITIAL_ACTIVATION_TIMEOUT);
    if let Some(age) = store
        .load_materialization_work_status(materialization.id.clone())
        .await
        .map_err(ReportIdleError::Store)?
        .and_then(|status| status.ready_age)
    {
        if age < minimum_ready_age {
            return Err(ReportIdleError::Store(StoreError::SleepDeferred {
                retry_after: minimum_ready_age - age,
            }));
        }
    }

    let identity = crate::materializer::IdleMemberIdentity {
        instance_id: current.id.clone(),
        instance_generation: request.expected_generation,
        materialization_id: materialization.id,
        pod_uid: request.pod_uid.clone(),
    };
    materializer
        .client()
        .verify_idle_member(&materialization.rendered_objects, &identity)
        .await
        .map_err(|error| ReportIdleError::UnsupportedSleep(error.to_string()))
}

async fn sleep_policy<S>(
    store: &S,
    instance: &InstanceRecord,
) -> Result<crate::sleep_policy::ResolvedSleepPolicy, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    let workload_class = store
        .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
            instance.workload_class.clone(),
        ))
        .await
        .map_err(ReportIdleError::Store)?
        .ok_or(ReportIdleError::WorkloadClassNotFound)?;
    let sleep_policy = workload_class
        .sleep_policy
        .resolve(&instance.values)
        .map_err(|error| ReportIdleError::SleepPolicy(error.to_string()))?;
    Ok(sleep_policy)
}

async fn load_instance<S>(
    store: &S,
    instance_id: InstanceId,
) -> Result<InstanceRecord, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    store
        .get_instance(GetInstanceRequest::new(instance_id))
        .await
        .map_err(ReportIdleError::Store)?
        .ok_or(ReportIdleError::NotFound)
}

fn unavailable_reason_for_state(state: InstanceState) -> ReportIdleUnavailableReason {
    match state {
        InstanceState::Cold => ReportIdleUnavailableReason::Cold,
        InstanceState::Waking => ReportIdleUnavailableReason::Waking,
        InstanceState::Running => {
            unreachable!("running instances are handled before unavailable mapping")
        }
        InstanceState::Draining => ReportIdleUnavailableReason::Draining,
        InstanceState::Failed => ReportIdleUnavailableReason::Failed,
        InstanceState::Deleting => ReportIdleUnavailableReason::Deleting,
        InstanceState::Deleted => ReportIdleUnavailableReason::Deleted,
    }
}
