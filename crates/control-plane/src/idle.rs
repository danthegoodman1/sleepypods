use std::time::Duration;

use crate::{
    ids::{Generation, InstanceId},
    instance::{GetInstanceRequest, InstanceRecord, InstanceState},
    materialization::{
        BeginSleepRequest, LoadActiveMaterializationRequest, MaterializationState,
        MaterializationTarget,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    store::{SleepStore, StoreError},
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
    S: SleepStore + ?Sized,
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
            let policy =
                verify_idle_member(store, materializer, &target, &request, &current).await?;
            begin_running_sleep(store, target, request, &current, policy).await
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
    S: SleepStore + ?Sized,
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

/// Begins the drain. The store refuses it, and names when to report again,
/// until the Ready projection has been up for the idle timeout and the initial
/// activation window.
async fn begin_running_sleep<S>(
    store: &S,
    target: MaterializationTarget,
    request: ReportIdleRequest,
    current: &InstanceRecord,
    sleep_policy: crate::sleep_policy::ResolvedSleepPolicy,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: SleepStore + ?Sized,
{
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

/// Checks that the report comes from a member of the running projection, and
/// resolves the sleep policy the drain follows.
async fn verify_idle_member<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: &MaterializationTarget,
    request: &ReportIdleRequest,
    current: &InstanceRecord,
) -> Result<crate::sleep_policy::ResolvedSleepPolicy, ReportIdleError>
where
    S: SleepStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    if request.pod_uid.trim().is_empty() {
        return Err(ReportIdleError::UnsupportedSleep(
            "pod_uid is required to authorize automatic sleep".to_owned(),
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
        .map_err(|error| ReportIdleError::UnsupportedSleep(error.to_string()))?;
    Ok(policy)
}

async fn load_instance<S>(
    store: &S,
    instance_id: InstanceId,
) -> Result<InstanceRecord, ReportIdleError>
where
    S: SleepStore + ?Sized,
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
