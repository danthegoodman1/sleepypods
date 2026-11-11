//! The database lease gates dispatch; UID/resourceVersion gate Kubernetes effects.
//! A durable record bridges the otherwise unsafe gap between the two authorities.
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

static NEXT_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

use crate::{
    manifest::KubernetesObject,
    materialization::{
        AcknowledgeMaterializationEffectRequest, BackendEndpoint, MaterializationEffectRequest,
        MaterializationRecord, RenderedObjectRef,
    },
    materializer::{
        rendered_object_ref, KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializerClient,
    },
    projection::{LiveObjectIdentity, ProjectionObjectInspection, ProjectionReadinessInspection},
    store::ControlPlaneStore,
};

#[derive(Clone)]
pub(super) struct FencedKubernetesClient<C> {
    pub inner: C,
    pub cancellation: crate::runtime_work::Cancellation,
    pub store: Arc<dyn ControlPlaneStore>,
    pub materialization: MaterializationRecord,
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
    async fn mutate(
        &self,
        operation: &'static str,
        object: RenderedObjectRef,
        precondition: Option<LiveObjectIdentity>,
        effect: KubernetesClientFuture<'_, KubernetesClientResult<()>>,
    ) -> KubernetesClientResult<()> {
        if self.cancellation.is_cancelled() {
            return Err(KubernetesClientError::transient(
                "reconciliation cancelled before dispatch",
            ));
        }
        let lease = self
            .materialization
            .reconciliation_lease
            .as_ref()
            .ok_or_else(|| KubernetesClientError::new("mutation has no acquired lease"))?;
        // Acquired attempts are never resumed by a replacement process. The
        // process-wide sequence distinguishes every operation within that fence.
        let effect_id = NEXT_EFFECT_ID
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1).filter(|next| *next <= i64::MAX as u64)
            })
            .map_err(|_| KubernetesClientError::new("effect identity sequence exhausted"))?;
        let started = self
            .store
            .begin_materialization_effect(MaterializationEffectRequest {
                effect_id,
                materialization_id: self.materialization.id.clone(),
                owner: lease.owner.clone(),
                attempt: lease.attempt,
                instance_generation: self.materialization.instance_generation,
                expected_state: self.materialization.state,
                operation,
                object,
                precondition,
            })
            .await;
        let started = match started {
            Ok(started) => started,
            Err(error) => {
                let _ = self
                    .store
                    .acknowledge_materialization_effect(AcknowledgeMaterializationEffectRequest {
                        instance_generation: self.materialization.instance_generation,
                        effect_id,
                        materialization_id: self.materialization.id.clone(),
                        owner: lease.owner.clone(),
                        attempt: lease.attempt,
                    })
                    .await;
                return Err(KubernetesClientError::transient(format!(
                    "Kubernetes mutation was not dispatched: {error}"
                )));
            }
        };
        if !started {
            return Err(KubernetesClientError::transient(
                "lease lost or another Kubernetes effect is unresolved",
            ));
        }
        if self.cancellation.is_cancelled() {
            // Begin was awaited by this owned task. The effect future has never
            // been polled, so an exact ACK safely resolves the committed marker.
            self.store
                .acknowledge_materialization_effect(AcknowledgeMaterializationEffectRequest {
                    instance_generation: self.materialization.instance_generation,
                    effect_id,
                    materialization_id: self.materialization.id.clone(),
                    owner: lease.owner.clone(),
                    attempt: lease.attempt,
                })
                .await
                .map_err(|error| {
                    KubernetesClientError::uncertain(format!(
                        "unsent effect cancellation ACK failed: {error}"
                    ))
                })?;
            return Err(KubernetesClientError::transient(
                "reconciliation cancelled before dispatch",
            ));
        }
        // Dropping this future, including on shutdown or heartbeat failure, leaves
        // the durable barrier. Absence observations must never clear it.
        let result = tokio::select! {
            // Polling the effect makes dispatch possible; from here cancellation
            // cannot establish a definite outcome and must retain the marker.
            result = tokio::time::timeout(Duration::from_secs(15), effect) => result,
            _ = self.cancellation.cancelled() => return Err(KubernetesClientError::uncertain("dispatched effect cancelled")),
        }
            .map_err(|_| {
                KubernetesClientError::uncertain(
                    "mutation deadline expired; Kubernetes outcome is unknown",
                )
            })?;
        if result
            .as_ref()
            .is_err_and(|error| error.outcome_uncertain())
        {
            return result;
        }
        let acknowledged = self
            .store
            .acknowledge_materialization_effect(AcknowledgeMaterializationEffectRequest {
                instance_generation: self.materialization.instance_generation,
                effect_id,
                materialization_id: self.materialization.id.clone(),
                owner: lease.owner.clone(),
                attempt: lease.attempt,
            })
            .await
            .map_err(|error| {
                KubernetesClientError::uncertain(format!(
                    "definite Kubernetes response could not be acknowledged: {error}"
                ))
            })?;
        if !acknowledged {
            return Err(KubernetesClientError::transient(
                "Kubernetes effect acknowledgement is no longer current",
            ));
        }
        result
    }
}

impl<C: KubernetesMaterializerClient> KubernetesMaterializerClient for FencedKubernetesClient<C> {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
        precondition: Option<&'a LiveObjectIdentity>,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(self.mutate(
            "apply",
            rendered_object_ref(object),
            precondition.cloned(),
            self.inner.apply_object(object, precondition),
        ))
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
        precondition: &'a LiveObjectIdentity,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(self.mutate(
            "delete",
            object.clone(),
            Some(precondition.clone()),
            self.inner.delete_object(object, precondition),
        ))
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.read(
                Duration::from_secs(120),
                self.inner.wait_for_pvc_bound(namespace, name),
            )
            .await
            .map_err(|_| KubernetesClientError::transient("PVC wait deadline expired"))?
        })
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
