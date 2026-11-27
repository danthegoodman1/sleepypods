pub use sleepypods_api::InstanceState;

use std::{collections::BTreeMap, error::Error, fmt};

use crate::{
    ids::{Generation, InstanceId, RouteBindingId},
    manifest::{
        render_manifests_with_options, ManifestRenderError, RenderManifestOptions,
        RenderManifestRequest,
    },
    route::{RouteBindingRecord, RouteIdentity},
    sleep_policy::SleepPolicyError,
    workload::{ValueSchemaError, WorkloadClassVersion, WorkloadClassVersionRef},
};

pub type InstanceValues = BTreeMap<String, String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceRecord {
    pub id: InstanceId,
    pub workload_class: WorkloadClassVersionRef,
    pub values: InstanceValues,
    pub state: InstanceState,
    pub generation: Generation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateInstanceRequest {
    pub instance_id: InstanceId,
    pub workload_class: WorkloadClassVersionRef,
    pub values: InstanceValues,
    pub route_bindings: Vec<InstanceRoute>,
}

/// A route binding created in the same transaction as its instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceRoute {
    pub id: RouteBindingId,
    pub identity: RouteIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateInstanceResult {
    pub instance: InstanceRecord,
    pub route_bindings: Vec<RouteBindingRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetInstanceRequest {
    pub instance_id: InstanceId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestInstanceDeletion {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteInstanceRequest {
    pub instance_id: InstanceId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompareAndSwapInstanceStateRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
    pub next_state: InstanceState,
    pub reason: StateTransitionReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateTransitionReason {
    OperatorRequested,
    WakeRequested,
    MaterializationReady,
    SleepRequested,
    IdleReported,
    DrainCompleted,
    FailureReported(String),
    DeleteRequested,
    DeleteFinalized,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidStateTransition {
    pub current: InstanceState,
    pub next: InstanceState,
    pub reason: StateTransitionReason,
}

pub fn validate_instance_state_transition(
    current: InstanceState,
    next: InstanceState,
    reason: &StateTransitionReason,
) -> Result<(), InvalidStateTransition> {
    let valid = matches!(
        (current, next, reason),
        (
            InstanceState::Cold,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested
        ) | (
            InstanceState::Cold,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Waking,
            InstanceState::Running,
            StateTransitionReason::MaterializationReady
        ) | (
            InstanceState::Waking,
            InstanceState::Failed,
            StateTransitionReason::FailureReported(_)
        ) | (
            InstanceState::Waking,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Running,
            InstanceState::Draining,
            StateTransitionReason::SleepRequested | StateTransitionReason::IdleReported
        ) | (
            InstanceState::Running,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Draining,
            InstanceState::Cold,
            StateTransitionReason::DrainCompleted
        ) | (
            InstanceState::Draining,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested
        ) | (
            InstanceState::Draining,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Failed,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested
        ) | (
            InstanceState::Failed,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Deleting,
            InstanceState::Deleted,
            StateTransitionReason::DeleteFinalized
        )
    );

    if valid {
        Ok(())
    } else {
        Err(InvalidStateTransition {
            current,
            next,
            reason: reason.clone(),
        })
    }
}

impl fmt::Display for InvalidStateTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid instance state transition: {:?} -> {:?} for {:?}",
            self.current, self.next, self.reason
        )
    }
}

impl Error for InvalidStateTransition {}

impl CreateInstanceRequest {
    pub fn new(instance_id: InstanceId, workload_class: WorkloadClassVersionRef) -> Self {
        Self {
            instance_id,
            workload_class,
            values: InstanceValues::new(),
            route_bindings: Vec::new(),
        }
    }

    pub fn with_values(mut self, values: InstanceValues) -> Self {
        self.values = values;
        self
    }

    pub fn with_route_bindings(mut self, route_bindings: Vec<InstanceRoute>) -> Self {
        self.route_bindings = route_bindings;
        self
    }

    pub fn validate_values_against(
        mut self,
        workload_class: &WorkloadClassVersion,
    ) -> Result<Self, CreateInstanceValidationError> {
        workload_class
            .validate()
            .map_err(CreateInstanceValidationError::WorkloadClass)?;
        self.values = workload_class.value_schema.validate_values(&self.values)?;
        workload_class.sleep_policy.resolve(&self.values)?;
        Ok(self)
    }

    /// Renders the instance as its first wake will. Call it on a request whose
    /// values `validate_values_against` has completed.
    pub(crate) fn validate_renders(
        &self,
        workload_class: &WorkloadClassVersion,
        namespace: &str,
        options: RenderManifestOptions<'_>,
    ) -> Result<(), CreateInstanceValidationError> {
        let instance = InstanceRecord {
            id: self.instance_id.clone(),
            workload_class: self.workload_class.clone(),
            values: self.values.clone(),
            state: InstanceState::Cold,
            generation: Generation::new(1),
        };
        render_manifests_with_options(
            RenderManifestRequest {
                template: &workload_class.template,
                instance: &instance,
                sleep_policy: workload_class.sleep_policy.resolve(&self.values)?,
                namespace,
            },
            options,
        )
        .map_err(CreateInstanceValidationError::Render)?;
        workload_class
            .render_exclusivity_keys(&self.values)
            .map_err(CreateInstanceValidationError::Render)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateInstanceValidationError {
    WorkloadClass(crate::workload::WorkloadClassValidationError),
    ValueSchema(ValueSchemaError),
    SleepPolicy(SleepPolicyError),
    Render(ManifestRenderError),
}

impl fmt::Display for CreateInstanceValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkloadClass(error) => error.fmt(f),
            Self::ValueSchema(error) => error.fmt(f),
            Self::SleepPolicy(error) => error.fmt(f),
            Self::Render(error) => error.fmt(f),
        }
    }
}

impl Error for CreateInstanceValidationError {}

impl From<ValueSchemaError> for CreateInstanceValidationError {
    fn from(error: ValueSchemaError) -> Self {
        Self::ValueSchema(error)
    }
}

impl From<SleepPolicyError> for CreateInstanceValidationError {
    fn from(error: SleepPolicyError) -> Self {
        Self::SleepPolicy(error)
    }
}

impl GetInstanceRequest {
    pub fn new(instance_id: InstanceId) -> Self {
        Self { instance_id }
    }
}

impl DeleteInstanceRequest {
    pub fn new(instance_id: InstanceId) -> Self {
        Self { instance_id }
    }
}

impl CompareAndSwapInstanceStateRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_generation: Generation,
        next_state: InstanceState,
        reason: StateTransitionReason,
    ) -> Self {
        Self {
            instance_id,
            expected_generation,
            next_state,
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ids::{Generation, InstanceId, WorkloadClassId},
        manifest::{
            ContainerPortTemplate, ContainerTemplate, ManifestTemplate, ServicePortTemplate,
            ServiceTemplate, SidecarTemplate, TemplateText, WorkloadKind, WorkloadTemplate,
        },
        sleep_policy::{IdleTimeoutOverridePolicy, WorkloadSleepPolicy},
        workload::{
            WorkloadClassVersion, WorkloadClassVersionRef, WorkloadValueFieldRule,
            WorkloadValueSchema,
        },
    };

    use super::{
        validate_instance_state_transition, CreateInstanceRequest, InstanceState, InstanceValues,
        StateTransitionReason,
    };

    #[test]
    fn create_instance_validation_applies_schema_defaults() {
        let request = create_request().with_values(values([("tenant", "acme")]));
        let validated = request
            .validate_values_against(&workload_class(
                WorkloadValueSchema::new(false)
                    .with_field("tenant", WorkloadValueFieldRule::required())
                    .with_field(
                        "image",
                        WorkloadValueFieldRule::optional_with_default("example/app:1"),
                    ),
            ))
            .expect("defaulted values are valid");

        assert_eq!(
            validated.values,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    #[test]
    fn replica_contract_rejects_zero_and_multiple_replicas_at_class_validation() {
        for kind in [
            crate::manifest::WorkloadKind::Deployment,
            crate::manifest::WorkloadKind::StatefulSet,
        ] {
            for replicas in [0, 2] {
                let mut class = workload_class(WorkloadValueSchema::new(true));
                class.template.workload.kind = kind;
                class.template.workload.replicas = Some(replicas);
                assert!(
                    class.validate().is_err(),
                    "{kind:?} replicas={replicas} must fail class validation"
                );
            }
        }
    }

    #[test]
    fn replica_contract_rejects_instances_pinned_to_legacy_invalid_classes() {
        for kind in [
            crate::manifest::WorkloadKind::Deployment,
            crate::manifest::WorkloadKind::StatefulSet,
        ] {
            for replicas in [0, 2] {
                let mut class = workload_class(WorkloadValueSchema::new(true));
                class.template.workload.kind = kind;
                class.template.workload.replicas = Some(replicas);
                assert!(create_request().validate_values_against(&class).is_err());
            }
        }
    }

    #[test]
    fn create_instance_validation_rejects_missing_required_values() {
        let error = create_request()
            .validate_values_against(&workload_class(
                WorkloadValueSchema::new(false)
                    .with_field("tenant", WorkloadValueFieldRule::required()),
            ))
            .expect_err("missing required value is rejected");

        assert_eq!(
            error.to_string(),
            "missing required instance value \"tenant\""
        );
    }

    #[test]
    fn create_instance_validation_rejects_unknown_values_when_extra_is_disallowed() {
        let error = create_request()
            .with_values(values([("extra", "value")]))
            .validate_values_against(&workload_class(WorkloadValueSchema::new(false)))
            .expect_err("unknown value is rejected");

        assert_eq!(error.to_string(), "unknown instance value \"extra\"");
    }

    #[test]
    fn create_instance_validation_allows_unknown_values_when_extra_is_allowed() {
        let validated = create_request()
            .with_values(values([("tenant", "acme")]))
            .validate_values_against(&workload_class(WorkloadValueSchema::new(true).with_field(
                "image",
                WorkloadValueFieldRule::optional_with_default("example/app:1"),
            )))
            .expect("unknown value is allowed");

        assert_eq!(
            validated.values,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    #[test]
    fn create_instance_validation_resolves_defaulted_sleep_policy_override() {
        let validated = create_request()
            .validate_values_against(&workload_class_with_sleep_policy(
                WorkloadValueSchema::new(false).with_field(
                    "idle_ms",
                    WorkloadValueFieldRule::optional_with_default("120000"),
                ),
                override_sleep_policy(),
            ))
            .expect("defaulted override is valid");

        assert_eq!(validated.values, values([("idle_ms", "120000")]));
    }

    #[test]
    fn create_instance_validation_rejects_malformed_sleep_policy_override() {
        let error = create_request()
            .with_values(values([("idle_ms", "two-minutes")]))
            .validate_values_against(&workload_class_with_sleep_policy(
                WorkloadValueSchema::new(false)
                    .with_field("idle_ms", WorkloadValueFieldRule::optional()),
                override_sleep_policy(),
            ))
            .expect_err("malformed override is rejected");

        assert_eq!(
            error.to_string(),
            "idle timeout override value \"idle_ms\"=\"two-minutes\" is not a decimal millisecond value"
        );
    }

    #[test]
    fn create_instance_validation_rejects_out_of_bounds_sleep_policy_override() {
        let error = create_request()
            .with_values(values([("idle_ms", "50000")]))
            .validate_values_against(&workload_class_with_sleep_policy(
                WorkloadValueSchema::new(false)
                    .with_field("idle_ms", WorkloadValueFieldRule::optional()),
                override_sleep_policy(),
            ))
            .expect_err("out-of-bounds override is rejected");

        assert_eq!(
            error.to_string(),
            "idle timeout override value \"idle_ms\"=50000 is outside allowed range 60000..=600000"
        );
    }

    #[test]
    fn instance_state_validator_accepts_lifecycle_edges() {
        for (current, next, reason) in [
            (
                InstanceState::Cold,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Cold,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Waking,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Waking,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Running,
                InstanceState::Draining,
                StateTransitionReason::SleepRequested,
            ),
            (
                InstanceState::Running,
                InstanceState::Draining,
                StateTransitionReason::IdleReported,
            ),
            (
                InstanceState::Running,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Draining,
                InstanceState::Cold,
                StateTransitionReason::DrainCompleted,
            ),
            (
                InstanceState::Draining,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Draining,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Failed,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Failed,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Deleting,
                InstanceState::Deleted,
                StateTransitionReason::DeleteFinalized,
            ),
        ] {
            validate_instance_state_transition(current, next, &reason)
                .expect("transition should be allowed");
        }

        validate_instance_state_transition(
            InstanceState::Waking,
            InstanceState::Failed,
            &StateTransitionReason::FailureReported("readiness timeout".to_owned()),
        )
        .expect("waking failures should be allowed");
    }

    #[test]
    fn instance_state_validator_rejects_nonsensical_edges() {
        for (current, next, reason) in [
            (
                InstanceState::Cold,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Running,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Failed,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Deleting,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Waking,
                InstanceState::Draining,
                StateTransitionReason::SleepRequested,
            ),
            (
                InstanceState::Deleted,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Deleting,
                InstanceState::Deleted,
                StateTransitionReason::DeleteRequested,
            ),
        ] {
            let error = validate_instance_state_transition(current, next, &reason)
                .expect_err("transition should be rejected");
            assert_eq!(error.current, current);
            assert_eq!(error.next, next);
            assert_eq!(error.reason, reason);
        }
    }

    fn create_request() -> CreateInstanceRequest {
        CreateInstanceRequest::new(
            InstanceId::new("instance-1").expect("valid instance ID"),
            WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-1").expect("valid class ID"),
                Generation::new(1),
            ),
        )
    }

    fn workload_class(value_schema: WorkloadValueSchema) -> WorkloadClassVersion {
        workload_class_with_sleep_policy(value_schema, default_sleep_policy())
    }

    fn workload_class_with_sleep_policy(
        value_schema: WorkloadValueSchema,
        sleep_policy: WorkloadSleepPolicy,
    ) -> WorkloadClassVersion {
        WorkloadClassVersion {
            reference: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-1").expect("valid class ID"),
                Generation::new(1),
            ),
            template: test_manifest_template(),
            value_schema,
            sleep_policy,
            exclusivity_keys: vec![],
        }
    }

    fn default_sleep_policy() -> WorkloadSleepPolicy {
        WorkloadSleepPolicy::new(300_000, 5_000, 30_000).expect("valid sleep policy")
    }

    fn override_sleep_policy() -> WorkloadSleepPolicy {
        default_sleep_policy()
            .with_idle_timeout_override(
                IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000)
                    .expect("valid override policy"),
            )
            .expect("override attaches")
    }

    fn test_manifest_template() -> ManifestTemplate {
        ManifestTemplate {
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
                    env: Vec::new(),
                },
                termination_grace_period_seconds: None,
            },
            sidecar: SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: TemplateText::literal("sleepypods/sidecar:test"),
                listen_port: 15000,
                mode: None,
                app_protocol: Default::default(),
            },
            service: Some(ServiceTemplate {
                name: TemplateText::literal("app"),
                ports: vec![ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        }
    }

    fn values<const N: usize>(pairs: [(&str, &str); N]) -> InstanceValues {
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }
}
