//! The database lease decides who drives a materialization; the API server
//! decides whether each mutation still applies.
//!
//! A driver renews its lease immediately before every mutation, so after
//! losing the lease it can land at most the one request already checked. That
//! request is a patch or delete conditional on a UID and resourceVersion,
//! which the API server refuses once the object changes, or a create, which
//! starts nothing: a workload is created with zero replicas and scaled up by a
//! patch conditional on the created object's UID. Cleanup proves every object
//! and Pod absent before it releases exclusivity, so a late conditional
//! request finds no object to act on.
use std::{sync::Arc, time::Duration};

use crate::{
    manifest::KubernetesObject,
    materialization::{BackendEndpoint, RenderedObjectRef},
    materializer::{
        KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializerClient,
    },
    projection::{LiveObjectIdentity, ProjectionObjectInspection, ProjectionReadinessInspection},
};

use super::HeldLease;

#[derive(Clone)]
pub(super) struct FencedKubernetesClient<C> {
    pub inner: C,
    pub cancellation: crate::runtime_work::Cancellation,
    pub lease: Arc<HeldLease>,
}

impl<C: KubernetesMaterializerClient> FencedKubernetesClient<C> {
    async fn read<T>(
        &self,
        duration: Duration,
        read: KubernetesClientFuture<'_, KubernetesClientResult<T>>,
    ) -> Result<KubernetesClientResult<T>, ()> {
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(()),
            result = tokio::time::timeout(duration, read) => result.map_err(|_| ()),
        }
    }

    /// Dispatches one mutation under a lease renewed for it. Once dispatched,
    /// the mutation runs to its own timeout: cancelling it would only hide what
    /// the object became.
    async fn mutate(
        &self,
        mutation: KubernetesClientFuture<'_, KubernetesClientResult<()>>,
    ) -> KubernetesClientResult<()> {
        if self.cancellation.is_cancelled() {
            return Err(KubernetesClientError::transient(
                "reconciliation cancelled before dispatch",
            ));
        }
        self.lease.renew().await.map_err(|_| {
            KubernetesClientError::transient("reconciliation lease was lost before dispatch")
        })?;
        if self.cancellation.is_cancelled() {
            return Err(KubernetesClientError::transient(
                "reconciliation cancelled before dispatch",
            ));
        }
        mutation.await
    }
}

impl<C: KubernetesMaterializerClient> KubernetesMaterializerClient for FencedKubernetesClient<C> {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(self.mutate(self.inner.apply_object(object, precondition)))
    }

    fn activate_workload<'a>(
        &'a self,
        object: &'a KubernetesObject,
        identity: &'a LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(self.mutate(self.inner.activate_workload(object, identity)))
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(self.mutate(self.inner.delete_object(object, precondition)))
    }

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async move {
            self.read(
                Duration::from_secs(120),
                self.inner.wait_for_readiness(objects),
            )
            .await
            .map_err(|_| KubernetesClientError::transient("readiness wait deadline expired"))?
        })
    }

    fn ready_pod_backend<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<Option<BackendEndpoint>>> {
        Box::pin(async move {
            self.read(
                Duration::from_secs(15),
                self.inner.ready_pod_backend(objects),
            )
            .await
            .map_err(|_| KubernetesClientError::transient("endpoint read deadline expired"))?
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            self.read(Duration::from_secs(15), self.inner.inspect_object(object))
                .await
                .map_err(|_| {
                    KubernetesClientError::transient("object inspection deadline expired")
                })?
        })
    }

    fn inspect_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionReadinessInspection>> {
        Box::pin(async move {
            self.read(
                Duration::from_secs(15),
                self.inner.inspect_readiness(objects),
            )
            .await
            .map_err(|_| {
                KubernetesClientError::transient("readiness inspection cancelled or timed out")
            })?
        })
    }

    fn verify_retained_bindings<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.read(
                Duration::from_secs(15),
                self.inner.verify_retained_bindings(objects),
            )
            .await
            .map_err(|_| {
                KubernetesClientError::transient("retained binding inspection deadline expired")
            })?
        })
    }

    fn ensure_no_descendants<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
        instance_id: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.read(
                Duration::from_secs(15),
                self.inner.ensure_no_descendants(objects, instance_id),
            )
            .await
            .map_err(|_| {
                KubernetesClientError::transient("descendant inspection deadline expired")
            })?
        })
    }
}
