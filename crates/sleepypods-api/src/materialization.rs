use crate::ids::{EmptyStringError, NonEmptyString};
use std::{error::Error, fmt};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationTarget {
    cluster_id: String,
    namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendEndpoint {
    uri: NonEmptyString,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidMaterializationTarget {
    field: &'static str,
}

impl MaterializationTarget {
    pub fn new(
        cluster_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, InvalidMaterializationTarget> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty() {
            return Err(InvalidMaterializationTarget {
                field: "cluster_id",
            });
        }

        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(InvalidMaterializationTarget { field: "namespace" });
        }

        Ok(Self {
            cluster_id,
            namespace,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl BackendEndpoint {
    pub fn new(uri: impl Into<String>) -> Result<Self, EmptyStringError> {
        Ok(Self {
            uri: NonEmptyString::new("backend.uri", uri)?,
        })
    }

    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }
}

impl fmt::Display for InvalidMaterializationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "materialization target {} must not be empty", self.field)
    }
}

impl Error for InvalidMaterializationTarget {}

#[cfg(test)]
mod tests {
    use super::{BackendEndpoint, MaterializationTarget};

    #[test]
    fn target_requires_cluster_and_namespace() {
        let error = MaterializationTarget::new("cluster-a", "").expect_err("namespace required");

        assert_eq!(
            error.to_string(),
            "materialization target namespace must not be empty"
        );
    }

    #[test]
    fn target_exposes_validated_fields_by_accessor() {
        let target = MaterializationTarget::new("cluster-a", "default").expect("valid target");

        assert_eq!(target.cluster_id(), "cluster-a");
        assert_eq!(target.namespace(), "default");
    }

    #[test]
    fn backend_endpoint_requires_uri() {
        let error = BackendEndpoint::new(" ").expect_err("backend URI required");

        assert_eq!(error.field(), "backend.uri");
    }
}
