use std::{collections::BTreeMap, error::Error, fmt, time::Duration};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use crate::{
    ids::{Generation, InstanceId, MaterializationId},
    manifest::{KubernetesObject, RenderedManifest},
    materialization::{BackendEndpoint, MaterializationRecord, RenderedObjectRef},
    materializer::{
        delete_order, rendered_object_refs, KubernetesClientError, KubernetesMaterializer,
        KubernetesMaterializerClient, MaterializerError,
    },
};

pub const LABEL_MANAGED_BY: &str = "app.kubernetes.io/managed-by";
pub const LABEL_MANAGED_BY_VALUE: &str = "sleepypods";
pub const ANNOTATION_MATERIALIZATION_ID: &str = "sleepypods.io/materialization-id";
pub const ANNOTATION_RENDERED_HASH: &str = "sleepypods.io/rendered-hash";

const MAX_DETAIL_LEN: usize = 160;
const MAX_FINALIZERS: usize = 5;
/// Rounds of inspect-then-apply one attempt makes when a mutation reports that
/// the object must be inspected again.
pub(crate) const APPLY_ROUNDS: u32 = 4;
/// How long one cleanup attempt waits for deleted objects and their Pods to
/// disappear. A Pod has its termination grace period to stop.
pub const ABSENCE_WAIT: Duration = Duration::from_secs(60);
const ABSENCE_POLL_MIN: Duration = Duration::from_millis(100);
const ABSENCE_POLL_MAX: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionPlan {
    materialization_id: MaterializationId,
    instance_id: InstanceId,
    instance_generation: Generation,
    manifest: Option<RenderedManifest>,
    objects: Vec<ProjectionObjectPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionObjectPlan {
    object_ref: RenderedObjectRef,
    expected: ExpectedOwnershipStamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedOwnershipStamp {
    materialization_id: String,
    instance_id: String,
    instance_generation: String,
    rendered_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionObjectInspection {
    Missing,
    Present(LiveObjectMetadata),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ProjectionReadinessInspection {
    #[default]
    NotObserved,
    Ready(BackendEndpoint),
    Unready {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveObjectIdentity {
    pub uid: String,
    pub resource_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveObjectMetadata {
    pub persistent_volume_reclaim_policy: Option<String>,
    pub identity: LiveObjectIdentity,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub deleting: bool,
    pub finalizers: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionObservation {
    pub persistent_volume_reclaim_policy: Option<String>,
    pub identity: Option<LiveObjectIdentity>,
    pub object_ref: RenderedObjectRef,
    pub state: ProjectionObservationState,
    pub reason: Option<String>,
    pub finalizers: Vec<String>,
    pub backend_uri: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionObservationState {
    Missing,
    PresentOwned,
    PresentUnowned,
    DeletingOwned,
    DeletingUnowned,
    Ready,
    Unready,
    ApplyRejected,
    DeleteBlocked,
    InspectFailed,
}

#[derive(Debug)]
pub enum ProjectionError {
    Inspect {
        object_ref: Box<RenderedObjectRef>,
        observations: Vec<ProjectionObservation>,
        source: KubernetesClientError,
    },
    MissingManifest,
    OwnershipConflict {
        observations: Vec<ProjectionObservation>,
    },
    CleanupBlocked {
        observations: Vec<ProjectionObservation>,
    },
    Incomplete {
        observations: Vec<ProjectionObservation>,
    },
    Apply {
        observations: Vec<ProjectionObservation>,
        source: Box<MaterializerError>,
    },
    Delete {
        observations: Vec<ProjectionObservation>,
        source: Box<MaterializerError>,
    },
    Readiness {
        observations: Vec<ProjectionObservation>,
        source: Box<MaterializerError>,
    },
}

pub struct ProjectionReconciler<'a, C> {
    materializer: &'a KubernetesMaterializer<C>,
}

impl ProjectionPlan {
    pub fn from_manifest(
        materialization: &MaterializationRecord,
        manifest: &RenderedManifest,
    ) -> Result<Self, MaterializerError> {
        let mut manifest = manifest.clone();
        for rendered in &mut manifest.objects {
            stamp_object_base(&mut rendered.object, materialization);
            let rendered_hash = rendered_hash_for_object(&rendered.object);
            stamp_rendered_hash(&mut rendered.object, &rendered_hash);
        }

        let rendered_refs = rendered_object_refs(&manifest)?;
        let mut expected = BTreeMap::new();
        for rendered in &manifest.objects {
            let object_ref = rendered.object.object_ref();
            let rendered_hash = rendered
                .object
                .metadata()
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(ANNOTATION_RENDERED_HASH))
                .cloned();
            expected.insert(
                ref_key(&object_ref),
                ExpectedOwnershipStamp::new(
                    &materialization.id,
                    &materialization.instance_id,
                    materialization.projection_generation,
                    rendered_hash,
                ),
            );
        }

        let objects = rendered_refs
            .into_iter()
            .map(|object_ref| {
                let expected = expected
                    .remove(&ref_key(&object_ref))
                    .expect("rendered ref has an expected stamp");
                ProjectionObjectPlan {
                    object_ref,
                    expected,
                }
            })
            .collect();

        Ok(Self {
            materialization_id: materialization.id.clone(),
            instance_id: materialization.instance_id.clone(),
            instance_generation: materialization.projection_generation,
            manifest: Some(manifest),
            objects,
        })
    }

    pub fn from_recorded_refs(materialization: &MaterializationRecord) -> Self {
        let expected = ExpectedOwnershipStamp::new(
            &materialization.id,
            &materialization.instance_id,
            materialization.projection_generation,
            None,
        );
        let objects = materialization
            .rendered_objects
            .iter()
            .cloned()
            .map(|object_ref| ProjectionObjectPlan {
                object_ref,
                expected: expected.clone(),
            })
            .collect();

        Self {
            materialization_id: materialization.id.clone(),
            instance_id: materialization.instance_id.clone(),
            instance_generation: materialization.projection_generation,
            manifest: None,
            objects,
        }
    }

    pub fn materialization_id(&self) -> &MaterializationId {
        &self.materialization_id
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    pub fn instance_generation(&self) -> Generation {
        self.instance_generation
    }

    pub fn manifest(&self) -> Option<&RenderedManifest> {
        self.manifest.as_ref()
    }

    pub fn object_plans(&self) -> &[ProjectionObjectPlan] {
        &self.objects
    }

    pub fn object_refs(&self) -> Vec<RenderedObjectRef> {
        self.objects
            .iter()
            .map(|object| object.object_ref.clone())
            .collect()
    }
}

impl ProjectionObjectPlan {
    pub fn object_ref(&self) -> &RenderedObjectRef {
        &self.object_ref
    }

    pub fn expected(&self) -> &ExpectedOwnershipStamp {
        &self.expected
    }
}

impl ExpectedOwnershipStamp {
    fn new(
        materialization_id: &MaterializationId,
        instance_id: &InstanceId,
        instance_generation: Generation,
        rendered_hash: Option<String>,
    ) -> Self {
        Self {
            materialization_id: materialization_id.as_str().to_owned(),
            instance_id: instance_id.as_str().to_owned(),
            instance_generation: instance_generation.to_string(),
            rendered_hash,
        }
    }

    pub fn materialization_id(&self) -> &str {
        &self.materialization_id
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn instance_generation(&self) -> &str {
        &self.instance_generation
    }

    pub fn rendered_hash(&self) -> Option<&str> {
        self.rendered_hash.as_deref()
    }
}

impl LiveObjectMetadata {
    pub fn from_rendered_object(object: &KubernetesObject) -> Self {
        Self {
            persistent_volume_reclaim_policy: object
                .to_kubernetes_json()
                .pointer("/spec/persistentVolumeReclaimPolicy")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            identity: LiveObjectIdentity {
                uid: "test-uid".into(),
                resource_version: "1".into(),
            },
            labels: object.metadata().labels.clone().unwrap_or_default(),
            annotations: object.metadata().annotations.clone().unwrap_or_default(),
            deleting: false,
            finalizers: Vec::new(),
        }
    }

    pub fn deleting(mut self, finalizers: impl Into<Vec<String>>) -> Self {
        self.deleting = true;
        self.finalizers = finalizers.into();
        self
    }
}

impl ProjectionObservation {
    pub fn missing(object_ref: RenderedObjectRef) -> Self {
        Self::new(
            object_ref,
            ProjectionObservationState::Missing,
            None,
            Vec::new(),
        )
    }

    pub fn ready(object_ref: RenderedObjectRef, backend: &BackendEndpoint) -> Self {
        Self {
            persistent_volume_reclaim_policy: None,
            identity: None,
            object_ref,
            state: ProjectionObservationState::Ready,
            reason: None,
            finalizers: Vec::new(),
            backend_uri: Some(bound_detail(backend.uri())),
        }
    }

    pub fn unready(object_ref: RenderedObjectRef, reason: impl AsRef<str>) -> Self {
        Self::new(
            object_ref,
            ProjectionObservationState::Unready,
            Some(reason.as_ref()),
            Vec::new(),
        )
    }

    fn apply_rejected(object_ref: RenderedObjectRef, reason: impl AsRef<str>) -> Self {
        Self::new(
            object_ref,
            ProjectionObservationState::ApplyRejected,
            Some(reason.as_ref()),
            Vec::new(),
        )
    }

    fn delete_blocked(
        object_ref: RenderedObjectRef,
        reason: impl AsRef<str>,
        finalizers: &[String],
    ) -> Self {
        Self::new(
            object_ref,
            ProjectionObservationState::DeleteBlocked,
            Some(reason.as_ref()),
            finalizers.to_vec(),
        )
    }

    fn inspect_failed(object_ref: RenderedObjectRef, reason: impl AsRef<str>) -> Self {
        Self::new(
            object_ref,
            ProjectionObservationState::InspectFailed,
            Some(reason.as_ref()),
            Vec::new(),
        )
    }

    fn new(
        object_ref: RenderedObjectRef,
        state: ProjectionObservationState,
        reason: Option<&str>,
        finalizers: Vec<String>,
    ) -> Self {
        Self {
            persistent_volume_reclaim_policy: None,
            identity: None,
            object_ref,
            state,
            reason: reason.map(bound_detail),
            finalizers: bound_finalizers(finalizers),
            backend_uri: None,
        }
    }

    pub fn is_unowned_conflict(&self) -> bool {
        matches!(
            self.state,
            ProjectionObservationState::PresentUnowned
                | ProjectionObservationState::DeletingUnowned
        )
    }
}

impl ProjectionObservationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::PresentOwned => "present_owned",
            Self::PresentUnowned => "present_unowned",
            Self::DeletingOwned => "deleting_owned",
            Self::DeletingUnowned => "deleting_unowned",
            Self::Ready => "ready",
            Self::Unready => "unready",
            Self::ApplyRejected => "apply_rejected",
            Self::DeleteBlocked => "delete_blocked",
            Self::InspectFailed => "inspect_failed",
        }
    }
}

impl<'a, C> ProjectionReconciler<'a, C>
where
    C: KubernetesMaterializerClient,
{
    pub fn new(materializer: &'a KubernetesMaterializer<C>) -> Self {
        Self { materializer }
    }

    pub async fn inspect(
        &self,
        plan: &ProjectionPlan,
    ) -> Result<Vec<ProjectionObservation>, ProjectionError> {
        let mut observations = Vec::with_capacity(plan.object_plans().len());
        for object in plan.object_plans() {
            let object_ref = object.object_ref().clone();
            let inspection = self
                .materializer
                .client()
                .inspect_object(object.object_ref())
                .await
                .map_err(|source| ProjectionError::Inspect {
                    object_ref: Box::new(object_ref.clone()),
                    observations: vec![ProjectionObservation::inspect_failed(
                        object_ref,
                        "inspect_failed",
                    )],
                    source,
                })?;
            observations.push(classify_object(object, inspection));
        }
        Ok(observations)
    }

    pub async fn inspect_with_readiness(
        &self,
        plan: &ProjectionPlan,
    ) -> Result<Vec<ProjectionObservation>, ProjectionError> {
        let mut observations = self.inspect(plan).await?;
        match self
            .materializer
            .client()
            .inspect_readiness(&plan.object_refs())
            .await
        {
            Ok(ProjectionReadinessInspection::NotObserved) => {}
            Ok(ProjectionReadinessInspection::Ready(backend)) => {
                observations.push(ProjectionObservation::ready(
                    readiness_observation_ref(plan),
                    &backend,
                ));
            }
            Ok(ProjectionReadinessInspection::Unready { reason }) => {
                observations.push(ProjectionObservation::unready(
                    readiness_observation_ref(plan),
                    reason,
                ));
            }
            Err(_) => {
                observations.push(ProjectionObservation::unready(
                    readiness_observation_ref(plan),
                    "readiness_inspect_failed",
                ));
            }
        }
        Ok(observations)
    }

    pub fn reject_unowned(
        &self,
        observations: &[ProjectionObservation],
    ) -> Result<(), ProjectionError> {
        let conflicts = observations
            .iter()
            .filter(|observation| observation.is_unowned_conflict())
            .cloned()
            .collect::<Vec<_>>();
        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(ProjectionError::OwnershipConflict {
                observations: conflicts,
            })
        }
    }

    /// Brings every rendered object to its desired state. Each round inspects
    /// the live objects, then creates the missing ones and patches the rest on
    /// condition that they are unchanged. A mutation whose outcome is unknown,
    /// or whose precondition went stale, starts another round.
    pub async fn apply(&self, plan: &ProjectionPlan) -> Result<(), ProjectionError> {
        let manifest = plan.manifest().ok_or(ProjectionError::MissingManifest)?;
        let mut round = 0;
        loop {
            round += 1;
            let observations = self.inspect(plan).await?;
            self.reject_unowned(&observations)?;
            // A terminating leftover keeps its name until it is gone.
            if observations
                .iter()
                .any(|observation| observation.state == ProjectionObservationState::DeletingOwned)
            {
                return Err(ProjectionError::Incomplete { observations });
            }
            match self
                .materializer
                .apply_manifest(manifest, &observations)
                .await
            {
                Ok(()) => return Ok(()),
                Err(MaterializerError::Apply { source, .. })
                    if round < APPLY_ROUNDS && !source.is_permanent() =>
                {
                    tokio::time::sleep(ABSENCE_POLL_MIN * round).await;
                }
                Err(source) => {
                    return Err(ProjectionError::Apply {
                        observations: apply_rejected_observations(plan, &source),
                        source: Box::new(source),
                    });
                }
            }
        }
    }

    pub async fn wait_for_readiness(
        &self,
        plan: &ProjectionPlan,
    ) -> Result<BackendEndpoint, ProjectionError> {
        let object_refs = plan.object_refs();
        self.materializer
            .wait_for_readiness(&object_refs)
            .await
            .map_err(|source| ProjectionError::Readiness {
                observations: vec![ProjectionObservation::unready(
                    readiness_observation_ref(plan),
                    "readiness_wait_failed",
                )],
                source: Box::new(source),
            })
    }

    /// Deletes every recorded object this projection owns and returns once
    /// they and their Pods are gone. Each round inspects the live objects and
    /// deletes, on condition that it is unchanged, any that is still present.
    /// A terminating object or Pod is progress: the attempt waits for it up to
    /// `ABSENCE_WAIT`.
    pub async fn delete_owned(&self, plan: &ProjectionPlan) -> Result<(), ProjectionError> {
        let object_refs = plan.object_refs();
        let deadline = tokio::time::Instant::now() + ABSENCE_WAIT;
        let mut poll = ABSENCE_POLL_MIN;
        loop {
            let observed = self.inspect(plan).await?;
            self.reject_unowned(&observed)?;
            if observed
                .iter()
                .all(|observation| observation.state == ProjectionObservationState::Missing)
            {
                match self
                    .materializer
                    .client()
                    .ensure_no_descendants(&object_refs, plan.instance_id().as_str())
                    .await
                {
                    Ok(()) => return Ok(()),
                    // Pods are still stopping, or the read failed for a
                    // reason that passes.
                    Err(source) if source.is_retryable() => {}
                    Err(source) => {
                        return Err(ProjectionError::Inspect {
                            object_ref: Box::new(readiness_observation_ref(plan)),
                            observations: vec![ProjectionObservation::delete_blocked(
                                readiness_observation_ref(plan),
                                "descendants_remain_or_inspection_failed",
                                &[],
                            )],
                            source,
                        });
                    }
                }
            } else {
                self.delete_present(plan, &observed).await?;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ProjectionError::CleanupBlocked {
                    observations: observed
                        .into_iter()
                        .filter_map(delete_blocked_observation)
                        .collect(),
                });
            }
            tokio::time::sleep(poll).await;
            poll = (poll * 2).min(ABSENCE_POLL_MAX);
        }
    }

    /// Issues a conditional delete for each owned object that is present and
    /// not yet terminating.
    async fn delete_present(
        &self,
        plan: &ProjectionPlan,
        observed: &[ProjectionObservation],
    ) -> Result<(), ProjectionError> {
        let object_refs = plan.object_refs();
        let present = |object_ref: &RenderedObjectRef| {
            observed.iter().find(|observation| {
                observation.object_ref == *object_ref
                    && observation.state == ProjectionObservationState::PresentOwned
            })
        };
        if !object_refs
            .iter()
            .any(|object_ref| present(object_ref).is_some())
        {
            return Ok(());
        }
        // PVC deletion itself can trigger destructive PV reclamation. Validate
        // every live volume before issuing any teardown request.
        let unsafe_volumes = observed
            .iter()
            .filter(|observation| {
                observation.object_ref.kind == "PersistentVolume"
                    && observation.state != ProjectionObservationState::Missing
                    && observation.persistent_volume_reclaim_policy.as_deref() != Some("Retain")
            })
            .map(|observation| {
                ProjectionObservation::delete_blocked(
                    observation.object_ref.clone(),
                    "managed_volume_requires_retain_before_cleanup",
                    &observation.finalizers,
                )
            })
            .collect::<Vec<_>>();
        if !unsafe_volumes.is_empty() {
            return Err(ProjectionError::CleanupBlocked {
                observations: unsafe_volumes,
            });
        }
        self.materializer
            .client()
            .verify_retained_bindings(&object_refs)
            .await
            .map_err(|source| ProjectionError::Inspect {
                object_ref: Box::new(readiness_observation_ref(plan)),
                observations: vec![ProjectionObservation::delete_blocked(
                    readiness_observation_ref(plan),
                    "retained_static_binding_not_proven",
                    &[],
                )],
                source,
            })?;
        for object_ref in delete_order(&object_refs) {
            let Some(observation) = present(object_ref) else {
                continue;
            };
            match self
                .materializer
                .delete_rendered_object_conditionally(
                    object_ref,
                    observation
                        .identity
                        .as_ref()
                        .expect("owned inspection has identity"),
                )
                .await
            {
                Ok(()) => {}
                // The next round inspects the object and decides again.
                Err(MaterializerError::Delete { source, .. }) if !source.is_permanent() => {}
                Err(source) => {
                    return Err(ProjectionError::Delete {
                        observations: vec![ProjectionObservation::delete_blocked(
                            object_ref.clone(),
                            "delete_request_failed",
                            &[],
                        )],
                        source: Box::new(source),
                    });
                }
            }
        }
        Ok(())
    }
}

impl ProjectionError {
    pub fn observations(&self) -> &[ProjectionObservation] {
        match self {
            Self::OwnershipConflict { observations }
            | Self::CleanupBlocked { observations }
            | Self::Incomplete { observations }
            | Self::Apply { observations, .. }
            | Self::Delete { observations, .. }
            | Self::Readiness { observations, .. }
            | Self::Inspect { observations, .. } => observations,
            Self::MissingManifest => &[],
        }
    }
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inspect {
                object_ref, source, ..
            } => write!(
                f,
                "failed to inspect {} {}/{}: {}",
                object_ref.kind, object_ref.namespace, object_ref.name, source
            ),
            Self::MissingManifest => f.write_str("projection plan has no rendered manifest"),
            Self::OwnershipConflict { observations } => write!(
                f,
                "projection ownership conflict across {} object(s)",
                observations.len()
            ),
            Self::CleanupBlocked { observations } => write!(
                f,
                "projection cleanup blocked across {} object(s)",
                observations.len()
            ),
            Self::Incomplete { observations } => write!(
                f,
                "projection incomplete across {} object(s)",
                observations.len()
            ),
            Self::Apply { source, .. } => write!(f, "projection apply rejected: {source}"),
            Self::Delete { source, .. } => write!(f, "projection delete blocked: {source}"),
            Self::Readiness { source, .. } => write!(f, "projection readiness unready: {source}"),
        }
    }
}

impl Error for ProjectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Inspect { source, .. } => Some(source),
            Self::Apply { source, .. }
            | Self::Delete { source, .. }
            | Self::Readiness { source, .. } => Some(source.as_ref()),
            Self::MissingManifest
            | Self::OwnershipConflict { .. }
            | Self::Incomplete { .. }
            | Self::CleanupBlocked { .. } => None,
        }
    }
}

fn classify_object(
    object: &ProjectionObjectPlan,
    inspection: ProjectionObjectInspection,
) -> ProjectionObservation {
    let persistent_volume_reclaim_policy = match &inspection {
        ProjectionObjectInspection::Present(live) => live.persistent_volume_reclaim_policy.clone(),
        ProjectionObjectInspection::Missing => None,
    };
    let identity = match &inspection {
        ProjectionObjectInspection::Present(live) => Some(live.identity.clone()),
        ProjectionObjectInspection::Missing => None,
    };
    let mut observation = match inspection {
        ProjectionObjectInspection::Missing => {
            ProjectionObservation::missing(object.object_ref.clone())
        }
        ProjectionObjectInspection::Present(metadata) => {
            let ownership_reason = ownership_mismatch_reason(&metadata, object.expected());
            let state = match (metadata.deleting, ownership_reason.is_none()) {
                (false, true) => ProjectionObservationState::PresentOwned,
                (false, false) => ProjectionObservationState::PresentUnowned,
                (true, true) => ProjectionObservationState::DeletingOwned,
                (true, false) => ProjectionObservationState::DeletingUnowned,
            };
            ProjectionObservation::new(
                object.object_ref.clone(),
                state,
                ownership_reason,
                metadata.finalizers,
            )
        }
    };
    observation.identity = identity;
    observation.persistent_volume_reclaim_policy = persistent_volume_reclaim_policy;
    observation
}

fn ownership_mismatch_reason(
    metadata: &LiveObjectMetadata,
    expected: &ExpectedOwnershipStamp,
) -> Option<&'static str> {
    if metadata.labels.get(LABEL_MANAGED_BY).map(String::as_str) != Some(LABEL_MANAGED_BY_VALUE) {
        return Some("managed_by_mismatch");
    }
    if metadata
        .labels
        .get(crate::manifest::LABEL_INSTANCE_ID)
        .map(String::as_str)
        != Some(expected.instance_id())
    {
        return Some("instance_id_mismatch");
    }
    if metadata
        .annotations
        .get(ANNOTATION_MATERIALIZATION_ID)
        .map(String::as_str)
        != Some(expected.materialization_id())
    {
        return Some("materialization_id_mismatch");
    }

    let live_generation = metadata
        .labels
        .get(crate::manifest::LABEL_INSTANCE_GENERATION)
        .and_then(|value| value.parse::<u64>().ok());
    let expected_generation = expected
        .instance_generation()
        .parse::<u64>()
        .expect("expected generation stamp is a rendered u64");
    match live_generation {
        // Same-instance objects stamped with an OLDER generation are this
        // instance's stale leftovers (e.g. from a wake attempt that failed
        // before completing). A newer generation owns them: apply supersedes
        // them in place and cleanup may delete them. Without this, a wake
        // retry livelocks: every attempt bumps the generation, so the
        // leftovers never match exactly and every apply/cleanup is rejected
        // as an ownership conflict.
        Some(live) if live < expected_generation => return None,
        Some(live) if live == expected_generation => {}
        // A NEWER live generation means another driver already superseded
        // this plan; touching those objects would clobber the newer owner.
        _ => return Some("generation_mismatch"),
    }

    if let Some(expected_hash) = expected.rendered_hash() {
        if metadata
            .annotations
            .get(ANNOTATION_RENDERED_HASH)
            .map(String::as_str)
            != Some(expected_hash)
        {
            return Some("rendered_hash_mismatch");
        }
    }

    None
}

fn stamp_object_base(object: &mut KubernetesObject, materialization: &MaterializationRecord) {
    stamp_metadata_base(object.metadata_mut(), materialization);
    if let Some(metadata) = object.pod_template_metadata_mut() {
        stamp_metadata_base(metadata, materialization);
    }
}

fn stamp_rendered_hash(object: &mut KubernetesObject, rendered_hash: &str) {
    let stamp = |metadata: &mut ObjectMeta| {
        metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .insert(
                ANNOTATION_RENDERED_HASH.to_owned(),
                rendered_hash.to_owned(),
            );
    };
    stamp(object.metadata_mut());
    if let Some(metadata) = object.pod_template_metadata_mut() {
        stamp(metadata);
    }
}

fn stamp_metadata_base(metadata: &mut ObjectMeta, materialization: &MaterializationRecord) {
    let labels = metadata.labels.get_or_insert_with(BTreeMap::new);
    labels.insert(
        LABEL_MANAGED_BY.to_owned(),
        LABEL_MANAGED_BY_VALUE.to_owned(),
    );
    labels.insert(
        crate::manifest::LABEL_INSTANCE_ID.to_owned(),
        materialization.instance_id.as_str().to_owned(),
    );
    labels.insert(
        crate::manifest::LABEL_INSTANCE_GENERATION.to_owned(),
        materialization.projection_generation.to_string(),
    );
    let annotations = metadata.annotations.get_or_insert_with(BTreeMap::new);
    annotations.insert(
        ANNOTATION_MATERIALIZATION_ID.to_owned(),
        materialization.id.as_str().to_owned(),
    );
    annotations.remove(ANNOTATION_RENDERED_HASH);
}

fn rendered_hash_for_object(object: &KubernetesObject) -> String {
    let mut object = object.clone();
    let unstamp = |metadata: &mut ObjectMeta| {
        if let Some(annotations) = &mut metadata.annotations {
            annotations.remove(ANNOTATION_RENDERED_HASH);
        }
    };
    unstamp(object.metadata_mut());
    if let Some(metadata) = object.pod_template_metadata_mut() {
        unstamp(metadata);
    }
    format!(
        "{:016x}",
        fnv1a64(object.to_kubernetes_json().to_string().as_bytes())
    )
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn apply_rejected_observations(
    plan: &ProjectionPlan,
    error: &MaterializerError,
) -> Vec<ProjectionObservation> {
    match error {
        MaterializerError::Apply { object, .. } => vec![ProjectionObservation::apply_rejected(
            object.as_ref().clone(),
            "apply_rejected",
        )],
        MaterializerError::InvalidManifest { .. } => vec![ProjectionObservation::apply_rejected(
            fallback_observation_ref(plan),
            "invalid_manifest",
        )],
        MaterializerError::ReadinessWait { .. } => vec![ProjectionObservation::unready(
            readiness_observation_ref(plan),
            "readiness_wait_failed",
        )],
        MaterializerError::Delete { object, .. } => vec![ProjectionObservation::delete_blocked(
            object.as_ref().clone(),
            "delete_request_failed",
            &[],
        )],
    }
}

fn delete_blocked_observation(observation: ProjectionObservation) -> Option<ProjectionObservation> {
    match observation.state {
        ProjectionObservationState::PresentOwned => Some(ProjectionObservation::delete_blocked(
            observation.object_ref,
            "delete_pending",
            &observation.finalizers,
        )),
        ProjectionObservationState::DeletingOwned => {
            let reason = if observation.finalizers.is_empty() {
                "deletion_in_progress"
            } else {
                "finalizers_blocking"
            };
            Some(ProjectionObservation::delete_blocked(
                observation.object_ref,
                reason,
                &observation.finalizers,
            ))
        }
        ProjectionObservationState::Missing => None,
        ProjectionObservationState::PresentUnowned
        | ProjectionObservationState::DeletingUnowned
        | ProjectionObservationState::Ready
        | ProjectionObservationState::Unready
        | ProjectionObservationState::ApplyRejected
        | ProjectionObservationState::DeleteBlocked
        | ProjectionObservationState::InspectFailed => Some(observation),
    }
}

fn fallback_observation_ref(plan: &ProjectionPlan) -> RenderedObjectRef {
    plan.object_plans()
        .first()
        .map(|object| object.object_ref.clone())
        .unwrap_or_else(|| RenderedObjectRef {
            api_version: String::new(),
            kind: String::new(),
            namespace: String::new(),
            name: String::new(),
        })
}

fn readiness_observation_ref(plan: &ProjectionPlan) -> RenderedObjectRef {
    plan.object_plans()
        .iter()
        .find(|object| object.object_ref.kind == "Service" && object.object_ref.api_version == "v1")
        .map(|object| object.object_ref.clone())
        .unwrap_or_else(|| fallback_observation_ref(plan))
}

fn ref_key(object_ref: &RenderedObjectRef) -> (String, String, String, String) {
    (
        object_ref.api_version.clone(),
        object_ref.kind.clone(),
        object_ref.namespace.clone(),
        object_ref.name.clone(),
    )
}

fn bound_detail(value: impl AsRef<str>) -> String {
    value.as_ref().chars().take(MAX_DETAIL_LEN).collect()
}

fn bound_finalizers(mut finalizers: Vec<String>) -> Vec<String> {
    finalizers.sort();
    finalizers
        .into_iter()
        .take(MAX_FINALIZERS)
        .map(bound_detail)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use crate::{
        ids::{BackendGeneration, WorkloadClassId},
        instance::{InstanceRecord, InstanceState, InstanceValues},
        manifest::{
            render_manifests, ContainerPortTemplate, ContainerTemplate, EnvVarTemplate,
            ManifestTemplate, RenderManifestRequest, ServicePortTemplate, ServiceTemplate,
            SidecarTemplate, TemplateText, WorkloadKind, WorkloadTemplate,
        },
        materialization::{MaterializationState, MaterializationTarget},
        materializer::{KubernetesClientFuture, KubernetesClientResult},
        sleep_policy::ResolvedSleepPolicy,
        workload::WorkloadClassVersionRef,
    };

    use super::*;

    #[test]
    fn projection_plan_stamps_every_rendered_object_with_bounded_ownership() {
        let materialization = materialization_record();
        let plan =
            ProjectionPlan::from_manifest(&materialization, &manifest()).expect("projection plan");

        let manifest = plan.manifest().expect("stamped manifest");
        for rendered in &manifest.objects {
            let metadata = LiveObjectMetadata::from_rendered_object(&rendered.object);
            assert_eq!(metadata.labels[LABEL_MANAGED_BY], LABEL_MANAGED_BY_VALUE);
            assert_eq!(
                metadata.labels[crate::manifest::LABEL_INSTANCE_ID],
                "instance-a"
            );
            assert_eq!(
                metadata.labels[crate::manifest::LABEL_INSTANCE_GENERATION],
                "7"
            );
            assert_eq!(
                metadata.annotations[ANNOTATION_MATERIALIZATION_ID],
                "instance-a:cluster-a:apps"
            );
            assert_eq!(
                metadata.annotations[ANNOTATION_RENDERED_HASH].len(),
                16,
                "rendered hash should be fixed-size hex"
            );
        }
    }

    #[test]
    fn projection_classification_rejects_missing_malformed_stale_and_mismatched_stamps() {
        let materialization = materialization_record();
        let plan =
            ProjectionPlan::from_manifest(&materialization, &manifest()).expect("projection plan");
        let object = &plan.object_plans()[0];

        assert_eq!(
            classify_object(object, ProjectionObjectInspection::Missing).state,
            ProjectionObservationState::Missing
        );

        let mut live = LiveObjectMetadata::from_rendered_object(
            &plan.manifest().expect("manifest").objects[0].object,
        );
        assert_eq!(
            classify_object(object, ProjectionObjectInspection::Present(live.clone())).state,
            ProjectionObservationState::PresentOwned
        );

        live.labels.remove(LABEL_MANAGED_BY);
        let missing_managed_by =
            classify_object(object, ProjectionObjectInspection::Present(live.clone()));
        assert_eq!(
            missing_managed_by.state,
            ProjectionObservationState::PresentUnowned
        );
        assert_eq!(
            missing_managed_by.reason.as_deref(),
            Some("managed_by_mismatch")
        );

        // Older-generation stamps are stale leftovers of the same instance:
        // still owned, so a newer generation can supersede or delete them.
        let mut stale_generation = LiveObjectMetadata::from_rendered_object(
            &plan.manifest().expect("manifest").objects[0].object,
        );
        stale_generation.labels.insert(
            crate::manifest::LABEL_INSTANCE_GENERATION.to_owned(),
            "6".to_owned(),
        );
        let stale = classify_object(
            object,
            ProjectionObjectInspection::Present(stale_generation),
        );
        assert_eq!(stale.state, ProjectionObservationState::PresentOwned);

        // Newer-generation stamps belong to a newer owner and must not be
        // touched by this (stale) plan.
        let mut newer_generation = LiveObjectMetadata::from_rendered_object(
            &plan.manifest().expect("manifest").objects[0].object,
        );
        newer_generation.labels.insert(
            crate::manifest::LABEL_INSTANCE_GENERATION.to_owned(),
            "8".to_owned(),
        );
        let newer = classify_object(
            object,
            ProjectionObjectInspection::Present(newer_generation),
        );
        assert_eq!(newer.state, ProjectionObservationState::PresentUnowned);
        assert_eq!(newer.reason.as_deref(), Some("generation_mismatch"));

        let mut mutated = LiveObjectMetadata::from_rendered_object(
            &plan.manifest().expect("manifest").objects[0].object,
        );
        mutated
            .annotations
            .insert(ANNOTATION_RENDERED_HASH.to_owned(), "different".to_owned());
        let mutated = classify_object(object, ProjectionObjectInspection::Present(mutated));
        assert_eq!(mutated.state, ProjectionObservationState::PresentUnowned);
        assert_eq!(mutated.reason.as_deref(), Some("rendered_hash_mismatch"));
    }

    #[test]
    fn projection_classification_bounds_finalizer_details() {
        let materialization = materialization_record();
        let plan =
            ProjectionPlan::from_manifest(&materialization, &manifest()).expect("projection plan");
        let object = &plan.object_plans()[0];
        let live = LiveObjectMetadata::from_rendered_object(
            &plan.manifest().expect("manifest").objects[0].object,
        )
        .deleting(
            (0..8)
                .map(|index| format!("finalizer-{index}-{}", "x".repeat(200)))
                .collect::<Vec<_>>(),
        );

        let observation = classify_object(object, ProjectionObjectInspection::Present(live));

        assert_eq!(observation.state, ProjectionObservationState::DeletingOwned);
        assert_eq!(observation.finalizers.len(), MAX_FINALIZERS);
        assert!(observation
            .finalizers
            .iter()
            .all(|finalizer| finalizer.len() <= MAX_DETAIL_LEN));
    }

    #[tokio::test]
    async fn apply_creates_missing_objects_and_scales_the_workload_up() {
        let plan = plan();
        let cluster = FakeCluster::default();
        let materializer = KubernetesMaterializer::new(cluster.clone());

        ProjectionReconciler::new(&materializer)
            .apply(&plan)
            .await
            .expect("projection applies");

        let [service, workload] = &plan.object_refs()[..] else {
            panic!("the plan renders a Service and a Deployment");
        };
        assert_eq!(
            cluster.calls(),
            vec![
                Call::Create(service.clone()),
                Call::Create(workload.clone()),
                Call::Patch(workload.clone()),
            ]
        );
        let observations = ProjectionReconciler::new(&materializer)
            .inspect(&plan)
            .await
            .expect("inspection succeeds");
        assert!(observations
            .iter()
            .all(|observation| observation.state == ProjectionObservationState::PresentOwned));
    }

    #[tokio::test]
    async fn apply_rejects_unowned_live_refs_before_any_mutation() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert(
            &plan.object_refs()[0],
            LiveObjectMetadata {
                persistent_volume_reclaim_policy: None,
                identity: identity("foreign"),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                deleting: false,
                finalizers: Vec::new(),
            },
        );
        let materializer = KubernetesMaterializer::new(cluster.clone());

        let error = ProjectionReconciler::new(&materializer)
            .apply(&plan)
            .await
            .expect_err("unowned live refs block projection apply");

        assert!(matches!(error, ProjectionError::OwnershipConflict { .. }));
        assert!(cluster.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn apply_inspects_again_after_a_stale_mutation() {
        let plan = plan();
        let cluster = FakeCluster::default();
        // The create's response was lost after the API server stored the object.
        cluster.lose_next_create_response();
        let materializer = KubernetesMaterializer::new(cluster.clone());

        ProjectionReconciler::new(&materializer)
            .apply(&plan)
            .await
            .expect("the next round finds the object and continues");

        let [service, workload] = &plan.object_refs()[..] else {
            panic!("the plan renders a Service and a Deployment");
        };
        assert_eq!(
            cluster.calls(),
            vec![
                Call::Create(service.clone()),
                Call::Patch(service.clone()),
                Call::Create(workload.clone()),
                Call::Patch(workload.clone()),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn apply_gives_up_after_its_rounds_of_stale_mutations() {
        let plan = plan();
        let cluster = FakeCluster::default();
        for _ in 0..APPLY_ROUNDS {
            cluster.fail_next_apply(KubernetesClientError::stale("request timed out"));
        }
        let materializer = KubernetesMaterializer::new(cluster.clone());

        let error = ProjectionReconciler::new(&materializer)
            .apply(&plan)
            .await
            .expect_err("every round failed");

        assert!(matches!(error, ProjectionError::Apply { .. }));
        assert_eq!(cluster.calls().len(), APPLY_ROUNDS as usize);
    }

    #[tokio::test]
    async fn apply_stops_at_a_permanent_mutation_failure() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.fail_next_apply(KubernetesClientError::new("invalid object"));
        let materializer = KubernetesMaterializer::new(cluster.clone());

        let error = ProjectionReconciler::new(&materializer)
            .apply(&plan)
            .await
            .expect_err("a permanent failure ends the apply");

        assert!(matches!(error, ProjectionError::Apply { .. }));
        assert_eq!(
            cluster.calls(),
            vec![Call::Create(plan.object_refs()[0].clone())]
        );
    }

    #[tokio::test]
    async fn apply_waits_for_a_terminating_leftover_without_mutating() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        cluster.hold_with_finalizer(&plan.object_refs()[1]);
        cluster.mark_deleting(&plan.object_refs()[1]);
        let materializer = KubernetesMaterializer::new(cluster.clone());

        let error = ProjectionReconciler::new(&materializer)
            .apply(&plan)
            .await
            .expect_err("a terminating object keeps its name");

        assert!(matches!(error, ProjectionError::Incomplete { .. }));
        assert!(cluster.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn delete_owned_waits_for_terminating_objects_and_pods() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        cluster.terminate_after_inspections(3);
        cluster.find_pods_for_checks(2);
        let materializer = KubernetesMaterializer::new(cluster.clone());
        let started = tokio::time::Instant::now();

        ProjectionReconciler::new(&materializer)
            .delete_owned(&plan)
            .await
            .expect("termination is progress, not failure");

        let [service, workload] = &plan.object_refs()[..] else {
            panic!("the plan renders a Service and a Deployment");
        };
        assert_eq!(
            cluster.calls(),
            vec![
                Call::Delete(service.clone()),
                Call::Delete(workload.clone())
            ],
            "each object is deleted once and then awaited"
        );
        assert!(started.elapsed() < ABSENCE_WAIT);
        assert!(cluster.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn delete_owned_deletes_again_after_a_stale_delete() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        cluster.fail_next_delete(KubernetesClientError::stale("request timed out"));
        let materializer = KubernetesMaterializer::new(cluster.clone());

        ProjectionReconciler::new(&materializer)
            .delete_owned(&plan)
            .await
            .expect("the next round deletes what is still present");

        let [service, workload] = &plan.object_refs()[..] else {
            panic!("the plan renders a Service and a Deployment");
        };
        assert_eq!(
            cluster.calls(),
            vec![
                Call::Delete(service.clone()),
                Call::Delete(workload.clone()),
                Call::Delete(service.clone()),
            ]
        );
        assert!(cluster.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn delete_owned_reports_a_finalizer_that_outlasts_the_absence_wait() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        let workload = plan.object_refs()[1].clone();
        cluster.hold_with_finalizer(&workload);
        let materializer = KubernetesMaterializer::new(cluster.clone());
        let started = tokio::time::Instant::now();

        let error = ProjectionReconciler::new(&materializer)
            .delete_owned(&plan)
            .await
            .expect_err("a held object blocks cleanup");

        let ProjectionError::CleanupBlocked { observations } = error else {
            panic!("expected cleanup to be blocked");
        };
        assert!(started.elapsed() >= ABSENCE_WAIT);
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].object_ref, workload);
        assert_eq!(
            observations[0].reason.as_deref(),
            Some("finalizers_blocking")
        );
        assert_eq!(
            cluster
                .calls()
                .iter()
                .filter(|call| **call == Call::Delete(workload.clone()))
                .count(),
            1,
            "a terminating object is awaited, never deleted twice"
        );
    }

    #[tokio::test]
    async fn delete_owned_refuses_an_unowned_object_without_deleting() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        cluster.insert(
            &plan.object_refs()[0],
            LiveObjectMetadata {
                persistent_volume_reclaim_policy: None,
                identity: identity("foreign"),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                deleting: false,
                finalizers: Vec::new(),
            },
        );
        let materializer = KubernetesMaterializer::new(cluster.clone());

        let error = ProjectionReconciler::new(&materializer)
            .delete_owned(&plan)
            .await
            .expect_err("another owner's object is never deleted");

        assert!(matches!(error, ProjectionError::OwnershipConflict { .. }));
        assert!(cluster.calls().is_empty());
    }

    #[tokio::test]
    async fn delete_owned_requires_retain_before_any_delete() {
        let mut materialization = materialization_record();
        let volume = RenderedObjectRef {
            api_version: "v1".to_owned(),
            kind: "PersistentVolume".to_owned(),
            namespace: String::new(),
            name: "pv".to_owned(),
        };
        materialization.rendered_objects.insert(0, volume.clone());
        let plan = ProjectionPlan::from_recorded_refs(&materialization);
        let cluster = FakeCluster::default();
        cluster.insert_owned(&self::plan());
        let mut live = cluster.live(&plan.object_refs()[1]).expect("owned Service");
        live.persistent_volume_reclaim_policy = Some("Delete".to_owned());
        cluster.insert(&volume, live);
        let materializer = KubernetesMaterializer::new(cluster.clone());

        let error = ProjectionReconciler::new(&materializer)
            .delete_owned(&plan)
            .await
            .expect_err("a volume the cluster would reclaim blocks cleanup");

        let ProjectionError::CleanupBlocked { observations } = error else {
            panic!("expected cleanup to be blocked");
        };
        assert_eq!(observations[0].object_ref, volume);
        assert_eq!(
            observations[0].reason.as_deref(),
            Some("managed_volume_requires_retain_before_cleanup")
        );
        assert!(cluster.calls().is_empty());
    }

    #[tokio::test]
    async fn delete_owned_stops_at_a_permanent_delete_failure() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        cluster.fail_next_delete(KubernetesClientError::new("forbidden"));
        let materializer = KubernetesMaterializer::new(cluster.clone());

        let error = ProjectionReconciler::new(&materializer)
            .delete_owned(&plan)
            .await
            .expect_err("a permanent failure ends the attempt");

        assert!(matches!(error, ProjectionError::Delete { .. }));
        assert_eq!(cluster.calls().len(), 1);
    }

    #[tokio::test]
    async fn projection_inspect_with_readiness_appends_ready_observation() {
        let plan = plan();
        let backend =
            BackendEndpoint::new("http://svc.apps.svc.cluster.local:80").expect("backend endpoint");
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        cluster.set_readiness(ProjectionReadinessInspection::Ready(backend.clone()));
        let materializer = KubernetesMaterializer::new(cluster);

        let metadata_only = ProjectionReconciler::new(&materializer)
            .inspect(&plan)
            .await
            .expect("metadata inspection succeeds");
        let with_readiness = ProjectionReconciler::new(&materializer)
            .inspect_with_readiness(&plan)
            .await
            .expect("readiness inspection succeeds");

        assert_eq!(metadata_only.len(), plan.object_plans().len());
        assert_eq!(with_readiness.len(), plan.object_plans().len() + 1);
        let observation = with_readiness.last().expect("readiness observation");
        assert_eq!(observation.state, ProjectionObservationState::Ready);
        assert_eq!(
            observation.object_ref,
            readiness_observation_ref(&plan),
            "readiness observation is anchored to the Service ref"
        );
        assert_eq!(observation.backend_uri.as_deref(), Some(backend.uri()));
    }

    #[tokio::test]
    async fn projection_inspect_with_readiness_appends_bounded_unready_observation() {
        let plan = plan();
        let cluster = FakeCluster::default();
        cluster.insert_owned(&plan);
        cluster.set_readiness(ProjectionReadinessInspection::Unready {
            reason: "x".repeat(MAX_DETAIL_LEN + 32),
        });
        let materializer = KubernetesMaterializer::new(cluster);

        let observations = ProjectionReconciler::new(&materializer)
            .inspect_with_readiness(&plan)
            .await
            .expect("readiness inspection succeeds");

        let observation = observations.last().expect("readiness observation");
        assert_eq!(observation.state, ProjectionObservationState::Unready);
        assert_eq!(
            observation.reason.as_ref().expect("unready reason").len(),
            MAX_DETAIL_LEN
        );
        assert_eq!(observation.backend_uri, None);
    }

    /// A cluster in memory. A create stores the rendered object's stamps under
    /// a fresh UID, a conditional patch needs the stored identity, and a
    /// delete marks the object terminating: it disappears after the configured
    /// number of further inspections unless a finalizer holds it.
    #[derive(Clone, Default)]
    struct FakeCluster {
        state: Arc<Mutex<ClusterState>>,
    }

    #[derive(Default)]
    struct ClusterState {
        live: BTreeMap<RefKey, LiveObjectMetadata>,
        terminating: BTreeMap<RefKey, usize>,
        termination_inspections: usize,
        pod_checks: usize,
        calls: Vec<Call>,
        apply_failures: VecDeque<KubernetesClientError>,
        delete_failures: VecDeque<KubernetesClientError>,
        lose_create_response: bool,
        readiness: ProjectionReadinessInspection,
        created: u64,
    }

    type RefKey = (String, String, String, String);

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Call {
        Create(RenderedObjectRef),
        Patch(RenderedObjectRef),
        Delete(RenderedObjectRef),
    }

    impl FakeCluster {
        fn state(&self) -> std::sync::MutexGuard<'_, ClusterState> {
            self.state.lock().expect("fake cluster lock is available")
        }

        fn calls(&self) -> Vec<Call> {
            self.state().calls.clone()
        }

        fn is_empty(&self) -> bool {
            self.state().live.is_empty()
        }

        fn live(&self, object: &RenderedObjectRef) -> Option<LiveObjectMetadata> {
            self.state().live.get(&ref_key(object)).cloned()
        }

        fn insert(&self, object: &RenderedObjectRef, live: LiveObjectMetadata) {
            self.state().live.insert(ref_key(object), live);
        }

        /// Stores every object of the plan's manifest as this projection's own.
        fn insert_owned(&self, plan: &ProjectionPlan) {
            for rendered in &plan.manifest().expect("stamped manifest").objects {
                self.insert(
                    &rendered.object.object_ref(),
                    LiveObjectMetadata::from_rendered_object(&rendered.object),
                );
            }
        }

        fn hold_with_finalizer(&self, object: &RenderedObjectRef) {
            self.state()
                .live
                .get_mut(&ref_key(object))
                .expect("live object")
                .finalizers = vec!["example.com/hold".to_owned()];
        }

        fn mark_deleting(&self, object: &RenderedObjectRef) {
            self.state()
                .live
                .get_mut(&ref_key(object))
                .expect("live object")
                .deleting = true;
        }

        fn terminate_after_inspections(&self, inspections: usize) {
            self.state().termination_inspections = inspections;
        }

        fn find_pods_for_checks(&self, checks: usize) {
            self.state().pod_checks = checks;
        }

        fn fail_next_apply(&self, error: KubernetesClientError) {
            self.state().apply_failures.push_back(error);
        }

        fn fail_next_delete(&self, error: KubernetesClientError) {
            self.state().delete_failures.push_back(error);
        }

        fn lose_next_create_response(&self) {
            self.state().lose_create_response = true;
        }

        fn set_readiness(&self, readiness: ProjectionReadinessInspection) {
            self.state().readiness = readiness;
        }
    }

    impl KubernetesMaterializerClient for FakeCluster {
        fn apply_object<'a>(
            &'a self,
            object: &'a KubernetesObject,
            precondition: Option<&'a LiveObjectIdentity>,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                let object_ref = object.object_ref();
                let key = ref_key(&object_ref);
                let mut state = self.state();
                state.calls.push(match precondition {
                    None => Call::Create(object_ref),
                    Some(_) => Call::Patch(object_ref),
                });
                if let Some(error) = state.apply_failures.pop_front() {
                    return Err(error);
                }
                let mut stored = LiveObjectMetadata::from_rendered_object(object);
                match precondition {
                    None => {
                        if state.live.contains_key(&key) {
                            return Err(KubernetesClientError::stale("already exists"));
                        }
                        state.created += 1;
                        stored.identity = identity(&format!("created-{}", state.created));
                        state.live.insert(key, stored);
                        if std::mem::take(&mut state.lose_create_response) {
                            return Err(KubernetesClientError::stale("response lost"));
                        }
                    }
                    Some(expected) => {
                        let live = state.live.get_mut(&key);
                        let Some(live) = live.filter(|live| live.identity == *expected) else {
                            return Err(KubernetesClientError::stale("conflict"));
                        };
                        stored.identity = LiveObjectIdentity {
                            uid: expected.uid.clone(),
                            resource_version: format!("{}-patched", expected.resource_version),
                        };
                        *live = stored;
                    }
                }
                Ok(())
            })
        }

        fn delete_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
            precondition: &'a LiveObjectIdentity,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                let key = ref_key(object);
                let mut state = self.state();
                state.calls.push(Call::Delete(object.clone()));
                if let Some(error) = state.delete_failures.pop_front() {
                    return Err(error);
                }
                let inspections = state.termination_inspections;
                let Some(live) = state
                    .live
                    .get_mut(&key)
                    .filter(|live| live.identity == *precondition)
                else {
                    return Err(KubernetesClientError::stale("conflict"));
                };
                live.deleting = true;
                if live.finalizers.is_empty() {
                    state.terminating.insert(key, inspections);
                }
                Ok(())
            })
        }

        fn wait_for_readiness<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
            Box::pin(async {
                BackendEndpoint::new("http://example").map_err(|error| {
                    KubernetesClientError::new(format!("invalid backend endpoint: {error}"))
                })
            })
        }

        fn inspect_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>>
        {
            Box::pin(async move {
                let key = ref_key(object);
                let mut state = self.state();
                match state.terminating.get_mut(&key) {
                    Some(0) => {
                        state.terminating.remove(&key);
                        state.live.remove(&key);
                    }
                    Some(remaining) => *remaining -= 1,
                    None => {}
                }
                Ok(match state.live.get(&key) {
                    Some(live) => ProjectionObjectInspection::Present(live.clone()),
                    None => ProjectionObjectInspection::Missing,
                })
            })
        }

        fn inspect_readiness<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>>
        {
            Box::pin(async move { Ok(self.state().readiness.clone()) })
        }

        fn ensure_no_descendants<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
            _instance_id: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                let mut state = self.state();
                if state.pod_checks == 0 {
                    return Ok(());
                }
                state.pod_checks -= 1;
                Err(KubernetesClientError::transient(
                    "projection Pods still exist, including terminating members",
                ))
            })
        }

        fn verify_retained_bindings<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn identity(uid: &str) -> LiveObjectIdentity {
        LiveObjectIdentity {
            uid: uid.to_owned(),
            resource_version: "1".to_owned(),
        }
    }

    fn plan() -> ProjectionPlan {
        ProjectionPlan::from_manifest(&materialization_record(), &manifest())
            .expect("projection plan")
    }

    fn materialization_record() -> MaterializationRecord {
        let manifest = manifest();
        let rendered_objects = rendered_object_refs(&manifest).expect("refs");
        MaterializationRecord {
            pod_backend: None,
            id: MaterializationId::new("instance-a:cluster-a:apps")
                .expect("valid materialization id"),
            instance_id: InstanceId::new("instance-a").expect("valid instance id"),
            instance_generation: Generation::new(7),
            projection_generation: Generation::new(7),
            target: MaterializationTarget::new("cluster-a", "apps").expect("valid target"),
            state: MaterializationState::Pending,
            backend: None,
            backend_generation: BackendGeneration::new(7),
            rendered_objects,
            exclusivity_keys: Vec::new(),
            reconciliation_lease: None,
        }
    }

    fn manifest() -> RenderedManifest {
        let template = ManifestTemplate {
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
                    env: vec![EnvVarTemplate {
                        name: "TENANT".to_owned(),
                        value: TemplateText::literal("acme"),
                    }],
                },
                termination_grace_period_seconds: None,
            },
            service: Some(ServiceTemplate {
                name: TemplateText::literal("svc"),
                ports: vec![ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            sidecar: SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: TemplateText::literal("sleepypods/sidecar:test"),
                listen_port: 15000,
                mode: None,
                app_protocol: Default::default(),
            },
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        };
        let instance = InstanceRecord {
            id: InstanceId::new("instance-a").expect("valid instance id"),
            workload_class: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            values: InstanceValues::new(),
            state: InstanceState::Waking,
            generation: Generation::new(7),
        };
        render_manifests(RenderManifestRequest {
            template: &template,
            instance: &instance,
            sleep_policy: ResolvedSleepPolicy {
                idle_timeout_ms: 120_000,
                idle_retry_backoff_ms: 5_000,
                drain_grace_timeout_ms: 30_000,
            },
            namespace: "apps",
        })
        .expect("manifest renders")
    }
}
