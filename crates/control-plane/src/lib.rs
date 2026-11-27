//! Domain model and persistence contract for the SleepyPods control plane.

#[cfg(test)]
extern crate self as control_plane;
#[cfg(test)]
#[macro_use]
#[path = "../tests/support/unexpected_store.rs"]
mod unexpected_store;

pub mod api;
pub mod auth;
pub mod certificate;
pub mod config;
pub mod http01;
mod idle;
pub mod ids;
pub mod instance;
mod kube_materializer;
mod kubernetes_name;
pub mod manifest;
pub mod materialization;
pub mod materializer;
pub mod postgres;
pub mod projection;
pub mod reconciler;
pub mod retry;
pub mod route;
pub mod runtime;
mod runtime_io;
pub mod runtime_work;
pub mod sleep_policy;
pub mod store;
mod wake;
pub mod workload;

pub fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub use auth::{
    AuthConfig, AuthFailureReason, AuthProvider, BearerToken, CallerRole, ControlPlaneAuth,
    ControlPlaneAuthInterceptor, InvalidBearerToken, InvalidStaticBearerTokens,
    OptionalBearerTokenInterceptor, StaticBearerAuthProvider, StaticBearerTokens,
};
pub use config::{ControlPlaneConfig, PostgresStoreConfig};
pub use http01::{
    DeleteHttp01ChallengeRequest, Http01ChallengeKey, Http01ChallengeRecord,
    PutHttp01ChallengeRequest,
};
pub use ids::{
    materialization_id, BackendGeneration, Generation, IdempotencyKey, InstanceId,
    MaterializationId, NonEmptyString, RouteBindingId, WorkloadClassId,
};
pub use instance::{
    CompareAndSwapInstanceStateRequest, CreateInstanceRequest, CreateInstanceResult,
    CreateInstanceValidationError, DeleteInstanceRequest, GetInstanceRequest, InstanceRecord,
    InstanceRoute, InstanceState, InstanceValues, StateTransitionReason,
};
pub use kube_materializer::{
    InvalidKubeMaterializerClientConfig, KubeMaterializerClient, KubeMaterializerClientConfig,
};
pub use kubernetes_name::render_instance_scoped_name;
pub use manifest::{
    render_manifests, ApplyOrder, Container, ContainerPort, ContainerPortTemplate,
    ContainerTemplate, CsiPersistentVolumeSource, Deployment, DeploymentSpec, EnvVar,
    EnvVarTemplate, HostPathPersistentVolumeSource, KubernetesObject, LabelSelector,
    ManifestRenderError, ManifestTemplate, ObjectMeta, PersistentVolume,
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimRef,
    PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, PersistentVolumeReclaimPolicy,
    PersistentVolumeSource, PersistentVolumeSourceTemplate, PersistentVolumeSpec, PodSpec,
    PodTemplateMetadata, PodTemplateSpec, PodVolume, RenderManifestRequest, RenderedManifest,
    RenderedManifestObject, Service, ServicePort, ServicePortTemplate, ServiceSpec,
    ServiceTemplate, SidecarTemplate, StatefulSet, StatefulSetSpec, TemplateText, TemplateTextPart,
    VolumeMount, VolumeResourceRequirements, VolumeTemplate, WorkloadKind, WorkloadTemplate,
};
pub use materialization::{
    BackendEndpoint, BeginSleepRequest, BeginSleepResult,
    ClaimMaterializationReconciliationRequest, CompleteWakeReconciliationRequest,
    CompleteWakeRequest, CompleteWakeResult, DeleteMaterializationReconciliationRequest,
    FinalizeSleepReconciliationRequest, FinalizeSleepRequest, FinalizeSleepResult,
    ForceDeleteMaterializationRequest, ForceReleaseExclusivityKeyRequest,
    ForceReleaseExclusivityKeyResult, ListMaterializationReconciliationCandidatesRequest,
    LoadActiveMaterializationRequest, LoadMaterializationRequest,
    MaterializationReconciliationLease, MaterializationRecord, MaterializationState,
    MaterializationTarget, RecordMaterializationRequest,
    ReleaseMaterializationReconciliationLeaseRequest, RenderedObjectRef,
    RenewMaterializationReconciliationLeaseRequest,
};
pub use materializer::{
    rendered_object_ref, AppliedMaterialization, KubernetesClientError, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient,
    MaterializerError, RetryingKubernetesMaterializerClient,
};
pub use postgres::PostgresStore;
pub use reconciler::{MaterializationReconciler, MaterializationReconcilerConfig};
pub use retry::RetryPolicy;
pub use route::{
    CachePolicy, CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
    ListRouteBindingsForInstanceRequest, PathPrefix, ProtocolRoute, ResolveRouteRequest,
    RouteBindingRecord, RouteBindingSpec, RouteDependencyLookup, RouteDependencySet, RouteEntry,
    RouteHost, RouteHostKind, RouteIdentity, RouteResolution,
};
pub use sleep_policy::{
    IdleTimeoutOverridePolicy, ResolvedSleepPolicy, SleepPolicyError, WorkloadSleepPolicy,
};
pub use store::{
    ControlPlaneStore, RetryingControlPlaneStore, StoreError, StoreFuture, StoreResult,
};
pub use workload::{
    CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, RenderedExclusivityKey,
    ValueSchemaError, WorkloadClassValidationError, WorkloadClassVersion, WorkloadClassVersionRef,
    WorkloadExclusivityKeyError, WorkloadExclusivityKeyTemplate, WorkloadValueFieldRule,
    WorkloadValueSchema,
};

pub mod runtime_security;
