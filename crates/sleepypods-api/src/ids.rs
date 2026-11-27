//! Resource identifiers and generation counters shared by every component.
use std::{error::Error, fmt, str::FromStr};

/// The longest Kubernetes DNS label (RFC 1123).
pub const DNS_LABEL_MAX_LEN: usize = 63;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonEmptyString(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyStringError {
    field: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidInstanceIdError {
    message: &'static str,
}

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, EmptyStringError> {
                Ok(Self(
                    NonEmptyString::new(stringify!($name), value)?.into_inner(),
                ))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = EmptyStringError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

impl NonEmptyString {
    pub fn new(field: &'static str, value: impl Into<String>) -> Result<Self, EmptyStringError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(EmptyStringError { field });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl EmptyStringError {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for EmptyStringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} must not be empty", self.field)
    }
}

impl Error for EmptyStringError {}

string_newtype!(WorkloadClassId);
string_newtype!(RouteBindingId);
string_newtype!(MaterializationId);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstanceId(String);

impl InstanceId {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidInstanceIdError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(InvalidInstanceIdError {
                message: "must not be empty",
            });
        }
        if !is_dns_label(&value) {
            return Err(InvalidInstanceIdError {
                message: "must be a Kubernetes DNS label: lowercase a-z, digits, or hyphen, start and end alphanumeric, and at most 63 characters",
            });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl InvalidInstanceIdError {
    pub fn field(&self) -> &'static str {
        "InstanceId"
    }
}

impl fmt::Display for InvalidInstanceIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field(), self.message)
    }
}

impl Error for InvalidInstanceIdError {}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for InstanceId {
    type Err = InvalidInstanceIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Whether `value` is a Kubernetes DNS label: lowercase letters, digits and
/// `-`, starting and ending alphanumeric, at most [`DNS_LABEL_MAX_LEN`] bytes.
pub fn is_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= DNS_LABEL_MAX_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendGeneration(u64);

impl Generation {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl BackendGeneration {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for BackendGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{is_dns_label, Generation, InstanceId, RouteBindingId, DNS_LABEL_MAX_LEN};

    #[test]
    fn dns_label_validation_matches_kubernetes_name_budget() {
        assert!(is_dns_label("a"));
        assert!(is_dns_label("tenant-123"));
        assert!(is_dns_label(&"a".repeat(DNS_LABEL_MAX_LEN)));

        for value in ["", "A", "a_b", "a.b", "-a", "a-"] {
            assert!(!is_dns_label(value), "{value:?} should be invalid");
        }
        let overlong = "a".repeat(DNS_LABEL_MAX_LEN + 1);
        assert!(!is_dns_label(&overlong));
    }

    #[test]
    fn string_ids_reject_blank_values() {
        let error = RouteBindingId::new("  ").expect_err("blank IDs are invalid");

        assert_eq!(error.field(), "RouteBindingId");
    }

    #[test]
    fn instance_id_accepts_kubernetes_dns_labels() {
        for value in ["a", "instance-a", "tenant-123"] {
            assert_eq!(
                InstanceId::new(value).expect("valid instance ID").as_str(),
                value
            );
        }
        let max_len = "a".repeat(63);
        assert_eq!(
            InstanceId::new(max_len.clone())
                .expect("valid instance ID")
                .as_str(),
            max_len
        );
    }

    #[test]
    fn instance_id_rejects_values_that_are_not_kubernetes_dns_labels() {
        for value in [
            "",
            "  ",
            "Instance-A",
            "instance_a",
            "instance.a",
            "-instance",
            "instance-",
            "tenant/foo",
        ] {
            let error = InstanceId::new(value).expect_err("invalid instance ID");

            assert_eq!(error.field(), "InstanceId");
        }
        let overlong = "a".repeat(64);
        let error = InstanceId::new(overlong).expect_err("invalid instance ID");

        assert_eq!(error.field(), "InstanceId");
    }

    #[test]
    fn generation_advances_by_one() {
        assert_eq!(Generation::new(41).next(), Generation::new(42));
    }
}
