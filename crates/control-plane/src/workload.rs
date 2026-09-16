use std::{collections::BTreeMap, error::Error, fmt};

use crate::{
    ids::{Generation, WorkloadClassId},
    instance::InstanceValues,
    manifest::{ManifestRenderError, ManifestTemplate, TemplateText, TemplateTextPart},
    sleep_policy::{SleepPolicyError, WorkloadSleepPolicy},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadClassVersionRef {
    pub class_id: WorkloadClassId,
    pub version: Generation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadWorkloadClassVersionRequest {
    pub reference: WorkloadClassVersionRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateWorkloadClassVersionRequest {
    pub workload_class_version: WorkloadClassVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadClassVersion {
    pub reference: WorkloadClassVersionRef,
    pub template_generation: Generation,
    pub template: ManifestTemplate,
    pub default_values: InstanceValues,
    pub value_schema: WorkloadValueSchema,
    pub sleep_policy: WorkloadSleepPolicy,
    pub exclusivity_keys: Vec<WorkloadExclusivityKeyTemplate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadValueSchema {
    pub fields: BTreeMap<String, WorkloadValueFieldRule>,
    pub allow_extra: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadValueFieldRule {
    pub required: bool,
    pub default: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkloadExclusivityKeyTemplate {
    pub name: String,
    pub value: TemplateText,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedExclusivityKey {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueSchemaError {
    MissingRequiredField { field: String },
    UnknownField { field: String },
    ControlCharacter { field: String, character: char },
    TooLong { field: String, length: usize },
}

/// An instance value fills one scalar of a manifest, so it stays printable text
/// of a length a Kubernetes field can hold.
pub const MAX_INSTANCE_VALUE_LENGTH: usize = 4096;

fn validate_value_text(field: &str, value: &str) -> Result<(), ValueSchemaError> {
    if let Some(character) = value.chars().find(|character| character.is_control()) {
        return Err(ValueSchemaError::ControlCharacter {
            field: field.to_owned(),
            character,
        });
    }
    if value.len() > MAX_INSTANCE_VALUE_LENGTH {
        return Err(ValueSchemaError::TooLong {
            field: field.to_owned(),
            length: value.len(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkloadClassValidationError {
    SleepPolicy(SleepPolicyError),
    ReplicaContract(ManifestRenderError),
    StorageContract(ManifestRenderError),
    ExclusivityKey(WorkloadExclusivityKeyError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadExclusivityKeyError {
    index: usize,
    field: &'static str,
    message: &'static str,
}

impl WorkloadClassVersionRef {
    pub fn new(class_id: WorkloadClassId, version: Generation) -> Self {
        Self { class_id, version }
    }
}

impl CreateWorkloadClassVersionRequest {
    pub fn new(workload_class_version: WorkloadClassVersion) -> Self {
        Self {
            workload_class_version,
        }
    }
}

impl LoadWorkloadClassVersionRequest {
    pub fn new(reference: WorkloadClassVersionRef) -> Self {
        Self { reference }
    }
}

impl WorkloadClassVersion {
    pub fn validate(&self) -> Result<(), WorkloadClassValidationError> {
        self.template
            .workload
            .validate_replicas()
            .map_err(WorkloadClassValidationError::ReplicaContract)?;
        self.template
            .validate_storage_retention()
            .map_err(WorkloadClassValidationError::StorageContract)?;
        self.sleep_policy
            .validate()
            .map_err(WorkloadClassValidationError::SleepPolicy)?;

        for (index, key) in self.exclusivity_keys.iter().enumerate() {
            key.validate(index)
                .map_err(WorkloadClassValidationError::ExclusivityKey)?;
        }

        Ok(())
    }

    pub fn render_exclusivity_keys(
        &self,
        values: &InstanceValues,
    ) -> Result<Vec<RenderedExclusivityKey>, ManifestRenderError> {
        let mut rendered = self
            .exclusivity_keys
            .iter()
            .map(|key| {
                let value = key.value.render(values)?;
                if value.trim().is_empty() {
                    return Err(ManifestRenderError::InvalidField {
                        field: "exclusivity_keys.value",
                        message: "rendered value must not be empty".to_owned(),
                    });
                }

                Ok(RenderedExclusivityKey {
                    name: key.name.clone(),
                    value,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        rendered.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.value.cmp(&right.value))
        });
        rendered.dedup();

        Ok(rendered)
    }
}

impl WorkloadValueSchema {
    pub fn new(allow_extra: bool) -> Self {
        Self {
            fields: BTreeMap::new(),
            allow_extra,
        }
    }

    pub fn with_field(mut self, name: impl Into<String>, rule: WorkloadValueFieldRule) -> Self {
        self.fields.insert(name.into(), rule);
        self
    }

    pub fn validate_values(
        &self,
        values: &InstanceValues,
    ) -> Result<InstanceValues, ValueSchemaError> {
        if !self.allow_extra {
            for field in values.keys() {
                if !self.fields.contains_key(field) {
                    return Err(ValueSchemaError::UnknownField {
                        field: field.clone(),
                    });
                }
            }
        }

        for (field, value) in values {
            validate_value_text(field, value)?;
        }

        let mut validated = values.clone();
        for (field, rule) in &self.fields {
            if validated.contains_key(field) {
                continue;
            }

            if let Some(default) = &rule.default {
                validated.insert(field.clone(), default.clone());
            } else if rule.required {
                return Err(ValueSchemaError::MissingRequiredField {
                    field: field.clone(),
                });
            }
        }

        Ok(validated)
    }
}

impl Default for WorkloadValueSchema {
    fn default() -> Self {
        Self::new(false)
    }
}

impl WorkloadValueFieldRule {
    pub fn required() -> Self {
        Self {
            required: true,
            default: None,
        }
    }

    pub fn optional() -> Self {
        Self {
            required: false,
            default: None,
        }
    }

    pub fn optional_with_default(default: impl Into<String>) -> Self {
        Self {
            required: false,
            default: Some(default.into()),
        }
    }
}

impl WorkloadExclusivityKeyTemplate {
    pub fn new(name: impl Into<String>, value: TemplateText) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    fn validate(&self, index: usize) -> Result<(), WorkloadExclusivityKeyError> {
        if !is_exclusivity_key_name(&self.name) {
            return Err(WorkloadExclusivityKeyError {
                index,
                field: "name",
                message: "must use only ASCII letters, digits, '.', '_', or '-', start and end with a letter or digit, and be at most 128 characters",
            });
        }

        if self.value.parts().is_empty() {
            return Err(WorkloadExclusivityKeyError {
                index,
                field: "value",
                message: "template parts must not be empty",
            });
        }

        if self.value.parts().iter().any(|part| {
            matches!(part, TemplateTextPart::InstanceValue(field) if field.trim().is_empty())
        }) {
            return Err(WorkloadExclusivityKeyError {
                index,
                field: "value",
                message: "instance value field names must not be empty",
            });
        }

        Ok(())
    }
}

impl RenderedExclusivityKey {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

fn is_exclusivity_key_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

impl fmt::Display for ValueSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRequiredField { field } => {
                write!(f, "missing required instance value {field:?}")
            }
            Self::UnknownField { field } => write!(f, "unknown instance value {field:?}"),
            Self::ControlCharacter { field, character } => write!(
                f,
                "instance value {field:?} holds control character {character:?}; \
                 an instance value stays printable text"
            ),
            Self::TooLong { field, length } => write!(
                f,
                "instance value {field:?} is {length} bytes, over the \
                 {MAX_INSTANCE_VALUE_LENGTH} byte limit"
            ),
        }
    }
}

impl Error for ValueSchemaError {}

impl WorkloadExclusivityKeyError {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for WorkloadClassValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SleepPolicy(error) => write!(f, "{error}"),
            Self::ReplicaContract(error) | Self::StorageContract(error) => write!(f, "{error}"),
            Self::ExclusivityKey(error) => write!(f, "{error}"),
        }
    }
}

impl Error for WorkloadClassValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SleepPolicy(error) => Some(error),
            Self::ReplicaContract(error) | Self::StorageContract(error) => Some(error),
            Self::ExclusivityKey(error) => Some(error),
        }
    }
}

impl fmt::Display for WorkloadExclusivityKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "workload exclusivity key {} {} {}",
            self.index, self.field, self.message
        )
    }
}

impl Error for WorkloadExclusivityKeyError {}

#[cfg(test)]
mod tests {
    use super::{
        RenderedExclusivityKey, ValueSchemaError, WorkloadExclusivityKeyTemplate,
        WorkloadValueFieldRule, WorkloadValueSchema,
    };
    use crate::{instance::InstanceValues, manifest::TemplateText};

    #[test]
    fn value_schema_applies_defaults() {
        let schema = WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required())
            .with_field(
                "image",
                WorkloadValueFieldRule::optional_with_default("example/app:1"),
            );

        let validated = schema
            .validate_values(&values([("tenant", "acme")]))
            .expect("values are valid");

        assert_eq!(
            validated,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    #[test]
    fn value_schema_rejects_missing_required_fields() {
        let schema = WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required());

        let error = schema
            .validate_values(&InstanceValues::new())
            .expect_err("missing required field is rejected");

        assert_eq!(
            error,
            ValueSchemaError::MissingRequiredField {
                field: "tenant".to_owned()
            }
        );
    }

    #[test]
    fn value_schema_rejects_unknown_fields_when_extra_values_are_disallowed() {
        let schema = WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required());

        let error = schema
            .validate_values(&values([("extra", "value"), ("tenant", "acme")]))
            .expect_err("unknown field is rejected");

        assert_eq!(
            error,
            ValueSchemaError::UnknownField {
                field: "extra".to_owned()
            }
        );
    }

    #[test]
    fn value_schema_allows_unknown_fields_when_extra_values_are_enabled() {
        let schema = WorkloadValueSchema::new(true).with_field(
            "image",
            WorkloadValueFieldRule::optional_with_default("example/app:1"),
        );

        let validated = schema
            .validate_values(&values([("tenant", "acme")]))
            .expect("extra value is allowed");

        assert_eq!(
            validated,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    #[test]
    fn value_schema_output_order_is_deterministic() {
        let schema = WorkloadValueSchema::new(true)
            .with_field("zeta", WorkloadValueFieldRule::optional())
            .with_field("alpha", WorkloadValueFieldRule::optional_with_default("a"))
            .with_field("middle", WorkloadValueFieldRule::optional_with_default("m"));

        let validated = schema
            .validate_values(&values([("zeta", "z"), ("tenant", "acme")]))
            .expect("values are valid");
        let keys = validated.keys().cloned().collect::<Vec<_>>();

        assert_eq!(keys, vec!["alpha", "middle", "tenant", "zeta"]);
    }

    #[test]
    fn exclusivity_key_validation_rejects_unsafe_names() {
        let key = WorkloadExclusivityKeyTemplate::new("disk/name", TemplateText::literal("disk-1"));

        let error = key.validate(2).expect_err("unsafe name is rejected");

        assert_eq!(error.index(), 2);
        assert_eq!(error.field(), "name");
    }

    #[test]
    fn exclusivity_key_rendering_sorts_and_deduplicates() {
        let class = super::WorkloadClassVersion {
            reference: super::WorkloadClassVersionRef::new(
                crate::ids::WorkloadClassId::new("class-a").expect("valid class id"),
                crate::ids::Generation::new(1),
            ),
            template_generation: crate::ids::Generation::new(1),
            template: crate::manifest::ManifestTemplate {
                workload: crate::manifest::WorkloadTemplate {
                    kind: crate::manifest::WorkloadKind::Deployment,
                    name: TemplateText::literal("app"),
                    replicas: Some(1),
                    app_container: crate::manifest::ContainerTemplate {
                        name: "app".to_owned(),
                        image: TemplateText::literal("example/app:1"),
                        ports: vec![],
                        env: vec![],
                    },
                },
                sidecar: crate::manifest::SidecarTemplate {
                    name: "sleepypods-sidecar".to_owned(),
                    image: TemplateText::literal("example/sidecar:1"),
                    listen_port: 8080,
                    mode: None,
                },
                service: None,
                volumes: vec![],
                raw_objects: vec![],
            },
            default_values: InstanceValues::new(),
            value_schema: WorkloadValueSchema::new(true),
            sleep_policy: crate::sleep_policy::WorkloadSleepPolicy::new(60_000, 1_000, 30_000)
                .expect("valid sleep policy"),
            exclusivity_keys: vec![
                WorkloadExclusivityKeyTemplate::new("license", TemplateText::instance_value("l")),
                WorkloadExclusivityKeyTemplate::new("disk", TemplateText::instance_value("d")),
                WorkloadExclusivityKeyTemplate::new("disk", TemplateText::instance_value("d")),
            ],
        };

        let rendered = class
            .render_exclusivity_keys(&values([("d", "disk-a"), ("l", "license-a")]))
            .expect("keys render");

        assert_eq!(
            rendered,
            vec![
                RenderedExclusivityKey::new("disk", "disk-a"),
                RenderedExclusivityKey::new("license", "license-a"),
            ]
        );
    }

    fn values<const N: usize>(pairs: [(&str, &str); N]) -> InstanceValues {
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }
}
