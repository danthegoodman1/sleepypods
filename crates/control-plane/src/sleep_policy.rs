use std::{error::Error, fmt};

use crate::instance::InstanceValues;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadSleepPolicy {
    pub idle_timeout_ms: u64,
    pub idle_retry_backoff_ms: u64,
    pub drain_grace_timeout_ms: u64,
    pub idle_timeout_override: Option<IdleTimeoutOverridePolicy>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleTimeoutOverridePolicy {
    pub value_field: String,
    pub min_idle_timeout_ms: u64,
    pub max_idle_timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedSleepPolicy {
    pub idle_timeout_ms: u64,
    pub idle_retry_backoff_ms: u64,
    pub drain_grace_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SleepPolicyError {
    NonPositiveDuration {
        field: &'static str,
    },
    EmptyOverrideField,
    InvalidOverrideBounds {
        min_idle_timeout_ms: u64,
        max_idle_timeout_ms: u64,
    },
    MalformedOverrideValue {
        field: String,
        value: String,
    },
    OverrideOutOfBounds {
        field: String,
        value: u64,
        min_idle_timeout_ms: u64,
        max_idle_timeout_ms: u64,
    },
}

impl WorkloadSleepPolicy {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(
        idle_timeout_ms: u64,
        idle_retry_backoff_ms: u64,
        drain_grace_timeout_ms: u64,
    ) -> Result<Self, SleepPolicyError> {
        let policy = Self {
            idle_timeout_ms,
            idle_retry_backoff_ms,
            drain_grace_timeout_ms,
            idle_timeout_override: None,
        };
        policy.validate()?;
        Ok(policy)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_idle_timeout_override(
        mut self,
        override_policy: IdleTimeoutOverridePolicy,
    ) -> Result<Self, SleepPolicyError> {
        override_policy.validate()?;
        self.idle_timeout_override = Some(override_policy);
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), SleepPolicyError> {
        validate_positive_duration("sleep_policy.idle_timeout_ms", self.idle_timeout_ms)?;
        validate_positive_duration(
            "sleep_policy.idle_retry_backoff_ms",
            self.idle_retry_backoff_ms,
        )?;
        validate_positive_duration(
            "sleep_policy.drain_grace_timeout_ms",
            self.drain_grace_timeout_ms,
        )?;

        if let Some(override_policy) = &self.idle_timeout_override {
            override_policy.validate()?;
        }

        Ok(())
    }

    pub fn resolve(
        &self,
        values: &InstanceValues,
    ) -> Result<ResolvedSleepPolicy, SleepPolicyError> {
        self.validate()?;
        let idle_timeout_ms = self.resolve_idle_timeout(values)?;

        Ok(ResolvedSleepPolicy {
            idle_timeout_ms,
            idle_retry_backoff_ms: self.idle_retry_backoff_ms,
            drain_grace_timeout_ms: self.drain_grace_timeout_ms,
        })
    }

    fn resolve_idle_timeout(&self, values: &InstanceValues) -> Result<u64, SleepPolicyError> {
        let Some(override_policy) = &self.idle_timeout_override else {
            return Ok(self.idle_timeout_ms);
        };
        let Some(value) = values.get(&override_policy.value_field) else {
            return Ok(self.idle_timeout_ms);
        };

        let parsed = parse_decimal_millis(&override_policy.value_field, value)?;
        if parsed < override_policy.min_idle_timeout_ms
            || parsed > override_policy.max_idle_timeout_ms
        {
            return Err(SleepPolicyError::OverrideOutOfBounds {
                field: override_policy.value_field.clone(),
                value: parsed,
                min_idle_timeout_ms: override_policy.min_idle_timeout_ms,
                max_idle_timeout_ms: override_policy.max_idle_timeout_ms,
            });
        }

        Ok(parsed)
    }
}

impl IdleTimeoutOverridePolicy {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(
        value_field: impl Into<String>,
        min_idle_timeout_ms: u64,
        max_idle_timeout_ms: u64,
    ) -> Result<Self, SleepPolicyError> {
        let policy = Self {
            value_field: value_field.into(),
            min_idle_timeout_ms,
            max_idle_timeout_ms,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), SleepPolicyError> {
        if self.value_field.trim().is_empty() {
            return Err(SleepPolicyError::EmptyOverrideField);
        }
        validate_positive_duration(
            "sleep_policy.idle_timeout_override.min_idle_timeout_ms",
            self.min_idle_timeout_ms,
        )?;
        validate_positive_duration(
            "sleep_policy.idle_timeout_override.max_idle_timeout_ms",
            self.max_idle_timeout_ms,
        )?;
        if self.min_idle_timeout_ms > self.max_idle_timeout_ms {
            return Err(SleepPolicyError::InvalidOverrideBounds {
                min_idle_timeout_ms: self.min_idle_timeout_ms,
                max_idle_timeout_ms: self.max_idle_timeout_ms,
            });
        }

        Ok(())
    }
}

impl fmt::Display for SleepPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPositiveDuration { field } => {
                write!(f, "{field} must be a positive millisecond value")
            }
            Self::EmptyOverrideField => {
                f.write_str("sleep_policy.idle_timeout_override.value_field must not be empty")
            }
            Self::InvalidOverrideBounds {
                min_idle_timeout_ms,
                max_idle_timeout_ms,
            } => write!(
                f,
                "sleep_policy.idle_timeout_override bounds are invalid: min {min_idle_timeout_ms} exceeds max {max_idle_timeout_ms}"
            ),
            Self::MalformedOverrideValue { field, value } => write!(
                f,
                "idle timeout override value {field:?}={value:?} is not a decimal millisecond value"
            ),
            Self::OverrideOutOfBounds {
                field,
                value,
                min_idle_timeout_ms,
                max_idle_timeout_ms,
            } => write!(
                f,
                "idle timeout override value {field:?}={value} is outside allowed range {min_idle_timeout_ms}..={max_idle_timeout_ms}"
            ),
        }
    }
}

impl Error for SleepPolicyError {}

fn validate_positive_duration(field: &'static str, value: u64) -> Result<(), SleepPolicyError> {
    if value == 0 {
        Err(SleepPolicyError::NonPositiveDuration { field })
    } else {
        Ok(())
    }
}

fn parse_decimal_millis(field: &str, value: &str) -> Result<u64, SleepPolicyError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SleepPolicyError::MalformedOverrideValue {
            field: field.to_owned(),
            value: value.to_owned(),
        });
    }

    value
        .parse::<u64>()
        .map_err(|_| SleepPolicyError::MalformedOverrideValue {
            field: field.to_owned(),
            value: value.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{IdleTimeoutOverridePolicy, SleepPolicyError, WorkloadSleepPolicy};

    #[test]
    fn sleep_policy_rejects_zero_durations() {
        let error =
            WorkloadSleepPolicy::new(0, 5_000, 30_000).expect_err("idle timeout must be positive");

        assert_eq!(
            error,
            SleepPolicyError::NonPositiveDuration {
                field: "sleep_policy.idle_timeout_ms"
            }
        );
    }

    #[test]
    fn sleep_policy_rejects_invalid_override_bounds() {
        let error = IdleTimeoutOverridePolicy::new("idle_ms", 10_000, 5_000)
            .expect_err("min must not exceed max");

        assert_eq!(
            error,
            SleepPolicyError::InvalidOverrideBounds {
                min_idle_timeout_ms: 10_000,
                max_idle_timeout_ms: 5_000,
            }
        );
    }

    #[test]
    fn sleep_policy_resolves_class_idle_timeout_when_override_absent() {
        let policy = WorkloadSleepPolicy::new(300_000, 5_000, 30_000)
            .expect("valid policy")
            .with_idle_timeout_override(
                IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000).expect("valid override"),
            )
            .expect("override attaches");

        let resolved = policy
            .resolve(&BTreeMap::new())
            .expect("policy resolves without an override value");

        assert_eq!(resolved.idle_timeout_ms, 300_000);
        assert_eq!(resolved.idle_retry_backoff_ms, 5_000);
        assert_eq!(resolved.drain_grace_timeout_ms, 30_000);
    }

    #[test]
    fn sleep_policy_resolves_valid_instance_override() {
        let policy = WorkloadSleepPolicy::new(300_000, 5_000, 30_000)
            .expect("valid policy")
            .with_idle_timeout_override(
                IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000).expect("valid override"),
            )
            .expect("override attaches");

        let resolved = policy
            .resolve(&BTreeMap::from([(
                "idle_ms".to_owned(),
                "120000".to_owned(),
            )]))
            .expect("policy resolves with an override value");

        assert_eq!(resolved.idle_timeout_ms, 120_000);
    }

    #[test]
    fn sleep_policy_rejects_malformed_instance_override() {
        let policy = WorkloadSleepPolicy::new(300_000, 5_000, 30_000)
            .expect("valid policy")
            .with_idle_timeout_override(
                IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000).expect("valid override"),
            )
            .expect("override attaches");

        let error = policy
            .resolve(&BTreeMap::from([("idle_ms".to_owned(), "2m".to_owned())]))
            .expect_err("malformed override is rejected");

        assert_eq!(
            error,
            SleepPolicyError::MalformedOverrideValue {
                field: "idle_ms".to_owned(),
                value: "2m".to_owned(),
            }
        );
    }

    #[test]
    fn sleep_policy_rejects_out_of_bounds_instance_override() {
        let policy = WorkloadSleepPolicy::new(300_000, 5_000, 30_000)
            .expect("valid policy")
            .with_idle_timeout_override(
                IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000).expect("valid override"),
            )
            .expect("override attaches");

        let error = policy
            .resolve(&BTreeMap::from([(
                "idle_ms".to_owned(),
                "50000".to_owned(),
            )]))
            .expect_err("out-of-bounds override is rejected");

        assert_eq!(
            error,
            SleepPolicyError::OverrideOutOfBounds {
                field: "idle_ms".to_owned(),
                value: 50_000,
                min_idle_timeout_ms: 60_000,
                max_idle_timeout_ms: 600_000,
            }
        );
    }
}
