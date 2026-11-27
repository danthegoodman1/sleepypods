pub use sleepypods_types::*;

use sleepypods_api::MaterializationTarget;

/// The identifier of the one materialization an instance has in a target.
pub fn materialization_id(
    instance_id: &InstanceId,
    target: &MaterializationTarget,
) -> Result<MaterializationId, EmptyStringError> {
    MaterializationId::new(format!(
        "{}:{}:{}",
        instance_id.as_str(),
        target.cluster_id(),
        target.namespace()
    ))
}
