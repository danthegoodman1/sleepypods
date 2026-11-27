use std::{error::Error, fmt, time::Instant};

use sleepypods_observability::{
    metrics::{RUNTIME_MATERIALIZATION_FAILURES_TOTAL, RUNTIME_WAKE_LATENCY_SECONDS},
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
        EVENT_MATERIALIZATION_FAILURE, EVENT_WAKE,
    },
    Operation, Outcome,
};

use crate::{
    ids::{BackendGeneration, Generation, InstanceId},
    instance::{GetInstanceRequest, InstanceRecord, InstanceState},
    manifest::{render_manifests_with_options, ManifestRenderError, RenderManifestRequest},
    materialization::{
        LoadActiveMaterializationRequest, LoadReadyMaterializationRequest, MaterializationRecord,
        MaterializationState, MaterializationTarget, RecordMaterializationRequest,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient, MaterializerError},
    sleep_policy::SleepPolicyError,
    store::{ControlPlaneStore, StoreError},
    workload::LoadWorkloadClassVersionRequest,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeInstanceRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
    pub target: MaterializationTarget,
    pub backend_generation: Option<BackendGeneration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeInstanceResult {
    AlreadyRunning {
        instance: InstanceRecord,
        materialization: Box<MaterializationRecord>,
    },
    AlreadyWaking {
        instance: InstanceRecord,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeUnavailableReason {
    Deleting,
    Deleted,
}

#[derive(Debug)]
pub enum WakeInstanceError {
    NotFound,
    GenerationConflict {
        expected: Generation,
        actual: Generation,
    },
    Unavailable {
        instance: Box<InstanceRecord>,
        reason: WakeUnavailableReason,
    },
    ReadyMaterializationNotFound {
        instance: Box<InstanceRecord>,
        target: MaterializationTarget,
    },
    WorkloadClassNotFound {
        instance: Box<InstanceRecord>,
    },
    Store(StoreError),
    Render {
        instance: Box<InstanceRecord>,
        source: ManifestRenderError,
    },
    SleepPolicy {
        instance: Box<InstanceRecord>,
        source: SleepPolicyError,
    },
    Materializer {
        instance: Box<InstanceRecord>,
        source: MaterializerError,
    },
}

pub async fn wake_instance_with_observability<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    request: WakeInstanceRequest,
    observability: ObservabilityRecorder,
) -> Result<WakeInstanceResult, WakeInstanceError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    let started = Instant::now();
    let request_fields = vec![
        LogField::instance_id(request.instance_id.as_str()),
        LogField::generation(request.expected_generation.get()),
        LogField::cluster_id(request.target.cluster_id()),
        LogField::namespace(request.target.namespace()),
    ];
    let result = wake_instance(store, materializer, request).await;
    record_wake_observation(&observability, started, &request_fields, &result);
    result
}

async fn wake_instance<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    request: WakeInstanceRequest,
) -> Result<WakeInstanceResult, WakeInstanceError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    let instance = store
        .get_instance(GetInstanceRequest::new(request.instance_id.clone()))
        .await
        .map_err(map_store_error)?
        .ok_or(WakeInstanceError::NotFound)?;
    if instance.generation != request.expected_generation {
        return Err(WakeInstanceError::GenerationConflict {
            expected: request.expected_generation,
            actual: instance.generation,
        });
    }
    match instance.state {
        InstanceState::Running => {
            let materialization = store
                .load_ready_materialization(LoadReadyMaterializationRequest::new(
                    request.instance_id,
                    instance.generation,
                    request.target.clone(),
                ))
                .await
                .map_err(map_store_error)?
                .ok_or_else(|| WakeInstanceError::ReadyMaterializationNotFound {
                    instance: Box::new(instance.clone()),
                    target: request.target,
                })?;
            return Ok(WakeInstanceResult::AlreadyRunning {
                instance,
                materialization: Box::new(materialization),
            });
        }
        InstanceState::Waking => {
            let pending = store
                .load_active_materialization(LoadActiveMaterializationRequest::new(
                    instance.id.clone(),
                    request.target,
                ))
                .await
                .map_err(map_store_error)?;
            if !pending.is_some_and(|m| {
                m.state == MaterializationState::Pending
                    && m.instance_generation == instance.generation
            }) {
                return Err(WakeInstanceError::Store(StoreError::invalid_argument(
                    "no accepted wake exists for this target",
                )));
            }
            return Ok(WakeInstanceResult::AlreadyWaking { instance });
        }
        InstanceState::Deleting | InstanceState::Deleted => {
            let reason = if instance.state == InstanceState::Deleting {
                WakeUnavailableReason::Deleting
            } else {
                WakeUnavailableReason::Deleted
            };
            return Err(WakeInstanceError::Unavailable {
                instance: Box::new(instance),
                reason,
            });
        }
        InstanceState::Cold | InstanceState::Failed | InstanceState::Draining => {}
    }
    let class = store
        .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
            instance.workload_class.clone(),
        ))
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| WakeInstanceError::WorkloadClassNotFound {
            instance: Box::new(instance.clone()),
        })?;
    class
        .validate()
        .map_err(|e| WakeInstanceError::Store(StoreError::invalid_argument(e.to_string())))?;
    let policy = class
        .sleep_policy
        .resolve(&instance.values)
        .map_err(|source| WakeInstanceError::SleepPolicy {
            instance: Box::new(instance.clone()),
            source,
        })?;
    let waking_generation = if instance.state == InstanceState::Draining {
        instance.generation.next().next()
    } else {
        instance.generation.next()
    };
    // Select an immutable projected incarnation before committing any intent.
    // Rendering is pure; neither readiness nor Kubernetes I/O occurs in this API.
    let mut projected = instance.clone();
    projected.state = InstanceState::Running;
    projected.generation = waking_generation.next();
    let manifest = render_manifests_with_options(
        RenderManifestRequest {
            template: &class.template,
            instance: &projected,
            sleep_policy: policy,
            namespace: request.target.namespace(),
        },
        materializer.render_options(),
    )
    .map_err(|source| WakeInstanceError::Render {
        instance: Box::new(instance.clone()),
        source,
    })?;
    let refs = materializer
        .rendered_object_refs(&manifest)
        .map_err(|source| WakeInstanceError::Materializer {
            instance: Box::new(instance.clone()),
            source,
        })?;
    let keys = class
        .render_exclusivity_keys(&instance.values)
        .map_err(|source| WakeInstanceError::Render {
            instance: Box::new(instance.clone()),
            source,
        })?;
    let backend_generation = request
        .backend_generation
        .unwrap_or_else(|| BackendGeneration::new(waking_generation.get()));
    let mut pending = RecordMaterializationRequest::new(
        instance.id.clone(),
        waking_generation,
        request.target,
        MaterializationState::Pending,
        backend_generation,
    );
    pending.rendered_objects = refs;
    pending.exclusivity_keys = keys;
    let accepted = store
        .accept_wake(crate::materialization::AcceptWakeRequest {
            expected_generation: instance.generation,
            pending,
        })
        .await
        .map_err(map_store_error)?;
    Ok(WakeInstanceResult::AlreadyWaking { instance: accepted })
}

fn record_wake_observation(
    observability: &ObservabilityRecorder,
    started: Instant,
    request_fields: &[LogField],
    result: &Result<WakeInstanceResult, WakeInstanceError>,
) {
    let outcome = match result {
        Ok(WakeInstanceResult::AlreadyRunning { .. }) => Outcome::AlreadyRunning,
        Ok(WakeInstanceResult::AlreadyWaking { .. }) => Outcome::AlreadyWaking,
        Err(WakeInstanceError::GenerationConflict { .. })
        | Err(WakeInstanceError::Unavailable { .. }) => Outcome::Rejected,
        Err(_) => Outcome::Error,
    };
    observability.record_metric(MetricObservation::new(
        RUNTIME_WAKE_LATENCY_SECONDS,
        vec![outcome.metric_label()],
        started.elapsed().as_secs_f64(),
    ));

    let mut fields = request_fields.to_vec();
    match result {
        Ok(WakeInstanceResult::AlreadyRunning {
            materialization, ..
        }) => {
            append_acquired_exclusivity_fields(&mut fields, materialization);
        }
        Ok(WakeInstanceResult::AlreadyWaking { .. }) => {}
        Err(error) => {
            fields.push(LogField::error_reason(wake_error_reason(error)));
            append_exclusivity_conflict_fields(&mut fields, error);
        }
    }
    observability.record_log(LifecycleLogEvent::new(EVENT_WAKE, fields));

    let materialization_failure = match result {
        Err(WakeInstanceError::Materializer { source, instance }) => {
            Some((instance, source.to_string()))
        }
        _ => None,
    };
    if let Some((instance, source)) = materialization_failure {
        observability.record_metric(MetricObservation::new(
            RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
            vec![
                Operation::Materialize.metric_label(),
                Outcome::Error.metric_label(),
            ],
            1.0,
        ));
        observability.record_log(LifecycleLogEvent::new(
            EVENT_MATERIALIZATION_FAILURE,
            vec![
                LogField::instance_id(instance.id.as_str()),
                LogField::generation(instance.generation.get()),
                LogField::error_reason(source),
            ],
        ));
    }
}

fn wake_error_reason(error: &WakeInstanceError) -> &'static str {
    match error {
        WakeInstanceError::NotFound => "not_found",
        WakeInstanceError::GenerationConflict { .. } => "generation_conflict",
        WakeInstanceError::Unavailable { .. } => "unavailable",
        WakeInstanceError::ReadyMaterializationNotFound { .. } => "ready_materialization_not_found",
        WakeInstanceError::WorkloadClassNotFound { .. } => "workload_class_not_found",
        WakeInstanceError::Store(StoreError::ExclusivityConflict { .. }) => "exclusivity_conflict",
        WakeInstanceError::Store(_) => "store",
        WakeInstanceError::Render { .. } => "render",
        WakeInstanceError::SleepPolicy { .. } => "sleep_policy",
        WakeInstanceError::Materializer { .. } => "materializer",
    }
}

fn append_acquired_exclusivity_fields(
    fields: &mut Vec<LogField>,
    materialization: &MaterializationRecord,
) {
    if materialization.exclusivity_keys.is_empty() {
        return;
    }

    let key_names = materialization
        .exclusivity_keys
        .iter()
        .map(|key| key.name.as_str())
        .collect::<Vec<_>>()
        .join(",");
    fields.push(LogField::exclusivity_action("acquire"));
    fields.push(LogField::exclusivity_key_name(key_names));
}

fn append_exclusivity_conflict_fields(fields: &mut Vec<LogField>, error: &WakeInstanceError) {
    let WakeInstanceError::Store(StoreError::ExclusivityConflict {
        key_name,
        owner_instance_id,
        ..
    }) = error
    else {
        return;
    };

    fields.push(LogField::exclusivity_action("conflict"));
    fields.push(LogField::exclusivity_key_name(key_name));
    if let Some(owner_instance_id) = owner_instance_id {
        fields.push(LogField::exclusivity_owner_instance_id(owner_instance_id));
    }
}

impl WakeInstanceRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            expected_generation,
            target,
            backend_generation: None,
        }
    }

    pub fn with_backend_generation(mut self, backend_generation: BackendGeneration) -> Self {
        self.backend_generation = Some(backend_generation);
        self
    }
}

fn map_store_error(error: StoreError) -> WakeInstanceError {
    match error {
        StoreError::NotFound {
            resource: "instance",
        } => WakeInstanceError::NotFound,
        StoreError::GenerationConflict { expected, actual } => {
            WakeInstanceError::GenerationConflict { expected, actual }
        }
        other => WakeInstanceError::Store(other),
    }
}

impl fmt::Display for WakeInstanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("instance not found"),
            Self::GenerationConflict { expected, actual } => {
                write!(
                    f,
                    "wake generation conflict: expected generation {expected}, found {actual}"
                )
            }
            Self::Unavailable { reason, .. } => {
                write!(f, "instance is unavailable for wake: {reason:?}")
            }
            Self::ReadyMaterializationNotFound { target, .. } => write!(
                f,
                "ready materialization not found for target {}/{}",
                target.cluster_id(),
                target.namespace()
            ),
            Self::WorkloadClassNotFound { .. } => f.write_str("workload class version not found"),
            Self::Store(error) => write!(f, "wake store operation failed: {error}"),
            Self::Render { source, .. } => write!(f, "wake manifest render failed: {source}"),
            Self::SleepPolicy { source, .. } => {
                write!(f, "wake sleep policy resolution failed: {source}")
            }
            Self::Materializer { source, .. } => {
                write!(f, "wake materializer failed: {source}")
            }
        }
    }
}

impl Error for WakeInstanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Render { source, .. } => Some(source),
            Self::SleepPolicy { source, .. } => Some(source),
            Self::Materializer { source, .. } => Some(source),
            Self::NotFound
            | Self::GenerationConflict { .. }
            | Self::Unavailable { .. }
            | Self::ReadyMaterializationNotFound { .. }
            | Self::WorkloadClassNotFound { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
