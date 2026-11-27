use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tonic::{Request, Response, Status};

use crate::{
    api::pb::{
        self,
        operator_control_plane_server::{OperatorControlPlane, OperatorControlPlaneServer},
    },
    http01 as domain_http01,
    ids::{InstanceId, MaterializationId, WorkloadClassId},
    instance::{self as domain_instance, InstanceState},
    materialization::{
        ForceDeleteMaterializationRequest, ForceReleaseExclusivityKeyRequest,
        LoadMaterializationRequest, MaterializationRecord, MaterializationState,
        MaterializationTarget, RenderedObjectRef,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    projection::{ProjectionObservation, ProjectionPlan, ProjectionReconciler},
    route as domain_route,
    sleep_policy::{IdleTimeoutOverridePolicy, WorkloadSleepPolicy},
    store::ControlPlaneStore,
    workload::{self as domain_workload, WorkloadClassVersion, WorkloadClassVersionRef},
};

use super::{
    store_error_to_status,
    template::{
        manifest_template_from_proto, manifest_template_to_proto, template_text_from_proto,
        template_text_to_proto,
    },
};

pub const OPERATOR_SERVICE_NAME: &str = "sleepypods.controlplane.v1.OperatorControlPlane";

#[derive(Clone)]
pub struct StoreBackedOperatorApi<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
}

impl<C> StoreBackedOperatorApi<C> {
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            store,
            materializer,
            target,
        }
    }
}

impl<C> StoreBackedOperatorApi<C>
where
    C: KubernetesMaterializerClient,
{
    /// The instance's materialization in this control plane's target.
    fn materialization_id(&self, instance_id: String) -> Result<MaterializationId, Status> {
        crate::ids::materialization_id(&parse_instance_id(instance_id)?, &self.target)
            .map_err(invalid_argument_status)
    }

    async fn projection_observations(
        &self,
        materialization: &MaterializationRecord,
    ) -> Vec<pb::ProjectionObservation> {
        let plan = ProjectionPlan::from_recorded_refs(materialization);
        match ProjectionReconciler::new(&self.materializer)
            .inspect_with_readiness(&plan)
            .await
        {
            Ok(observations) => observations
                .iter()
                .map(projection_observation_to_proto)
                .collect(),
            Err(error) => error
                .observations()
                .iter()
                .map(projection_observation_to_proto)
                .collect(),
        }
    }

    async fn projection_observations_for_materializations(
        &self,
        materializations: &[MaterializationRecord],
    ) -> Vec<pb::ProjectionObservation> {
        let mut observations = Vec::new();
        for materialization in materializations {
            observations.extend(self.projection_observations(materialization).await);
        }
        observations
    }
}
pub type StoreBackedOperatorGrpcService<C> = OperatorControlPlaneServer<StoreBackedOperatorApi<C>>;

pub fn operator_grpc_service_with_store<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> StoreBackedOperatorGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    OperatorControlPlaneServer::new(StoreBackedOperatorApi::new(store, materializer, target))
        .max_decoding_message_size(256 * 1024)
        .max_encoding_message_size(1024 * 1024)
}

#[tonic::async_trait]
impl<C> OperatorControlPlane for StoreBackedOperatorApi<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    async fn publish_certificate(
        &self,
        request: Request<pb::PublishCertificateRequest>,
    ) -> Result<Response<pb::CertificateMetadata>, Status> {
        super::certificates::publish(self.store.as_ref(), request).await
    }

    async fn get_certificate_metadata(
        &self,
        request: Request<pb::GetCertificateMetadataRequest>,
    ) -> Result<Response<pb::CertificateMetadata>, Status> {
        super::certificates::get_metadata(self.store.as_ref(), request).await
    }

    async fn set_tls_binding(
        &self,
        request: Request<pb::SetTlsBindingRequest>,
    ) -> Result<Response<pb::TlsBinding>, Status> {
        super::certificates::set_binding(self.store.as_ref(), request).await
    }

    async fn get_tls_binding(
        &self,
        request: Request<pb::GetTlsBindingRequest>,
    ) -> Result<Response<pb::TlsBinding>, Status> {
        super::certificates::get_binding(self.store.as_ref(), request).await
    }

    async fn remove_certificate(
        &self,
        request: Request<pb::RemoveCertificateRequest>,
    ) -> Result<Response<pb::CertificateMetadata>, Status> {
        super::certificates::remove(self.store.as_ref(), request).await
    }

    async fn reencrypt_certificate(
        &self,
        request: Request<pb::ReencryptCertificateRequest>,
    ) -> Result<Response<pb::CertificateMetadata>, Status> {
        super::certificates::reencrypt(self.store.as_ref(), request).await
    }

    async fn reencrypt_certificates(
        &self,
        request: Request<pb::ReencryptCertificatesRequest>,
    ) -> Result<Response<pb::ReencryptCertificatesResponse>, Status> {
        super::certificates::reencrypt_sealed_by(self.store.as_ref(), request).await
    }

    async fn create_workload_class_version(
        &self,
        request: Request<pb::CreateWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        let request = create_workload_class_request_from_proto(request.into_inner())?;
        request
            .workload_class_version
            .validate_renderable(self.target.namespace())
            .map_err(invalid_argument_status)?;
        let workload_class = self
            .store
            .create_workload_class_version(request)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(workload_class_to_proto(workload_class)))
    }

    async fn get_workload_class_version(
        &self,
        request: Request<pb::GetWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        let reference = request
            .into_inner()
            .reference
            .ok_or_else(|| Status::invalid_argument("reference is required"))
            .and_then(workload_class_ref_from_proto)?;
        let workload_class = self
            .store
            .load_workload_class_version(domain_workload::LoadWorkloadClassVersionRequest::new(
                reference,
            ))
            .await
            .map_err(store_error_to_status)?
            .ok_or_else(|| Status::not_found("workload class version not found"))?;

        Ok(Response::new(workload_class_to_proto(workload_class)))
    }

    async fn create_instance(
        &self,
        request: Request<pb::CreateInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        let request = create_instance_request_from_proto(request.into_inner())?;
        let workload_class = self
            .store
            .load_workload_class_version(domain_workload::LoadWorkloadClassVersionRequest::new(
                request.workload_class.clone(),
            ))
            .await
            .map_err(store_error_to_status)?
            .ok_or_else(|| Status::not_found("workload class version not found"))?;
        let request = request
            .validate_values_against(&workload_class)
            .map_err(invalid_argument_status)?;
        request
            .validate_renders(
                &workload_class,
                self.target.namespace(),
                self.materializer.render_options(),
            )
            .map_err(invalid_argument_status)?;
        let result = self
            .store
            .create_instance(request)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(instance_to_proto(result.instance)))
    }

    async fn get_instance(
        &self,
        request: Request<pb::GetInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        let request = domain_instance::GetInstanceRequest::new(parse_instance_id(
            request.into_inner().instance_id,
        )?);
        let instance = self
            .store
            .get_instance(request)
            .await
            .map_err(store_error_to_status)?
            .ok_or_else(|| Status::not_found("instance not found"))?;

        Ok(Response::new(instance_to_proto(instance)))
    }

    async fn delete_instance(
        &self,
        request: Request<pb::DeleteInstanceRequest>,
    ) -> Result<Response<pb::DeleteInstanceResponse>, Status> {
        let request = request.into_inner();
        let expected_generation = request
            .expected_generation
            .ok_or_else(|| Status::invalid_argument("expected_generation is required"))?;
        if expected_generation > i64::MAX as u64 {
            return Err(Status::invalid_argument(
                "expected_generation exceeds the supported revision range",
            ));
        }
        let request = domain_instance::RequestInstanceDeletion {
            instance_id: parse_instance_id(request.instance_id)?,
            expected_generation: crate::ids::Generation::new(expected_generation),
        };
        let deleted = self
            .store
            .request_instance_deletion(request)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(pb::DeleteInstanceResponse {
            accepted: deleted,
        }))
    }

    async fn create_route_binding(
        &self,
        request: Request<pb::CreateRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        let route_binding = self
            .store
            .create_route_binding(create_route_binding_request_from_proto(
                request.into_inner(),
            )?)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(route_binding_to_proto(route_binding)))
    }

    async fn get_route_binding(
        &self,
        request: Request<pb::GetRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        let request = domain_route::GetRouteBindingRequest::new(parse_route_binding_id(
            request.into_inner().route_binding_id,
        )?);
        let route_binding = self
            .store
            .get_route_binding(request)
            .await
            .map_err(store_error_to_status)?
            .ok_or_else(|| Status::not_found("route binding not found"))?;

        Ok(Response::new(route_binding_to_proto(route_binding)))
    }

    async fn delete_route_binding(
        &self,
        request: Request<pb::DeleteRouteBindingRequest>,
    ) -> Result<Response<pb::DeleteRouteBindingResponse>, Status> {
        let route_binding_id = parse_route_binding_id(request.into_inner().route_binding_id)?;
        let request = domain_route::DeleteRouteBindingRequest::new(route_binding_id);
        let deleted = self
            .store
            .delete_route_binding(request)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(pb::DeleteRouteBindingResponse { deleted }))
    }

    async fn put_http01_challenge(
        &self,
        request: Request<pb::PutHttp01ChallengeRequest>,
    ) -> Result<Response<pb::Http01Challenge>, Status> {
        let challenge = self
            .store
            .put_http01_challenge(put_http01_request_from_proto(request.into_inner())?)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(http01_to_proto(challenge)?))
    }

    async fn delete_http01_challenge(
        &self,
        request: Request<pb::DeleteHttp01ChallengeRequest>,
    ) -> Result<Response<pb::DeleteHttp01ChallengeResponse>, Status> {
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("key is required"))
            .and_then(http01_key_from_proto)?;
        let deleted = self
            .store
            .delete_http01_challenge(domain_http01::DeleteHttp01ChallengeRequest::new(key))
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(pb::DeleteHttp01ChallengeResponse { deleted }))
    }

    async fn reconcile_materialization(
        &self,
        request: Request<pb::ReconcileMaterializationRequest>,
    ) -> Result<Response<pb::ReconcileMaterializationResponse>, Status> {
        let request = request.into_inner();
        let materialization_id = self.materialization_id(request.instance_id)?;
        let before = self
            .store
            .load_materialization(LoadMaterializationRequest::new(materialization_id.clone()))
            .await
            .map_err(store_error_to_status)?;

        let Some(before) = before else {
            return Ok(Response::new(pb::ReconcileMaterializationResponse {
                found: false,
                materialization_id: materialization_id.as_str().to_owned(),
                state: String::new(),
                attempted: false,
                lease_owner: String::new(),
                lease_expires_at_unix_millis: 0,
                lease_attempt: 0,
                observed_refs: Vec::new(),
                projection_observations: Vec::new(),
                ..Default::default()
            }));
        };

        let attempted = if request.status_only {
            false
        } else {
            self.store
                .enqueue_materialization(materialization_id.clone())
                .await
                .map_err(store_error_to_status)?
        };
        let status = self
            .store
            .load_materialization_work_status(materialization_id.clone())
            .await
            .map_err(store_error_to_status)?;
        let after = self
            .store
            .load_materialization(LoadMaterializationRequest::new(materialization_id))
            .await
            .map_err(store_error_to_status)?
            .unwrap_or(before);
        let projection_observations = self.projection_observations(&after).await;

        let mut response =
            reconcile_materialization_response(&after, attempted, projection_observations);
        if let Some(status) = status {
            response.next_attempt_at_unix_millis = status.next_attempt_at_unix_millis;
            response.operation_deadline_unix_millis = status.operation_deadline_unix_millis;
            response.failure_count = status.failure_count;
            response.failure_kind = if status.uncertain_effect.is_some() {
                "uncertain".into()
            } else {
                status.failure_kind.unwrap_or_default()
            };
            response.failure_message = status.failure_message;
            response.wake_failure_message = status.wake_failure_message;
            response.uncertain_effect =
                status
                    .uncertain_effect
                    .map(|effect| pb::UncertainMaterializationEffect {
                        instance_generation: effect.generation.get(),
                        owner: effect.owner,
                        lease_attempt: effect.attempt,
                        effect_id: effect.effect_id,
                        operation: effect.operation,
                        object: Some(rendered_object_ref_to_proto(&effect.object)),
                        expected_uid: effect.expected_uid.unwrap_or_default(),
                        expected_resource_version: effect
                            .expected_resource_version
                            .unwrap_or_default(),
                        started_at_unix_millis: effect.started_at_unix_millis,
                    });
        }
        Ok(Response::new(response))
    }

    async fn force_delete_materialization(
        &self,
        request: Request<pb::ForceDeleteMaterializationRequest>,
    ) -> Result<Response<pb::ForceDeleteMaterializationResponse>, Status> {
        let request = request.into_inner();
        let request = ForceDeleteMaterializationRequest::new(
            self.materialization_id(request.instance_id)?,
            request.operator,
            request.reason,
        );
        let materialization_id = request.materialization_id.as_str().to_owned();
        let materialization = self
            .store
            .force_delete_materialization(request)
            .await
            .map_err(store_error_to_status)?;
        let observed_refs = materialization
            .as_ref()
            .map(|materialization| {
                materialization
                    .rendered_objects
                    .iter()
                    .map(rendered_object_ref_to_proto)
                    .collect()
            })
            .unwrap_or_default();
        let projection_observations = match materialization.as_ref() {
            Some(materialization) => self.projection_observations(materialization).await,
            None => Vec::new(),
        };

        Ok(Response::new(pb::ForceDeleteMaterializationResponse {
            found: materialization.is_some(),
            materialization_id,
            observed_refs,
            projection_observations,
        }))
    }

    async fn force_release_exclusivity_key(
        &self,
        request: Request<pb::ForceReleaseExclusivityKeyRequest>,
    ) -> Result<Response<pb::ForceReleaseExclusivityKeyResponse>, Status> {
        let result = self
            .store
            .force_release_exclusivity_key(force_release_exclusivity_key_request_from_proto(
                request.into_inner(),
            )?)
            .await
            .map_err(store_error_to_status)?;
        let projection_observations = self
            .projection_observations_for_materializations(&result.affected_materializations)
            .await;

        Ok(Response::new(pb::ForceReleaseExclusivityKeyResponse {
            updated_materializations: result.updated_materializations as u64,
            projection_observations,
        }))
    }
}

fn create_instance_request_from_proto(
    request: pb::CreateInstanceRequest,
) -> Result<domain_instance::CreateInstanceRequest, Status> {
    let workload_class = request
        .workload_class
        .ok_or_else(|| Status::invalid_argument("workload_class is required"))
        .and_then(workload_class_ref_from_proto)?;

    let route_bindings = request
        .routes
        .into_iter()
        .map(|route| {
            Ok(domain_instance::InstanceRoute {
                id: parse_route_binding_id(route.route_binding_id)?,
                identity: route
                    .identity
                    .ok_or_else(|| Status::invalid_argument("routes.identity is required"))
                    .and_then(route_identity_from_proto)?,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;

    Ok(domain_instance::CreateInstanceRequest::new(
        parse_instance_id(request.instance_id)?,
        workload_class,
    )
    .with_values(request.values.into_iter().collect())
    .with_route_bindings(route_bindings))
}

fn create_workload_class_request_from_proto(
    request: pb::CreateWorkloadClassVersionRequest,
) -> Result<domain_workload::CreateWorkloadClassVersionRequest, Status> {
    let workload_class = WorkloadClassVersion {
        reference: WorkloadClassVersionRef::new(
            WorkloadClassId::new(request.class_id).map_err(invalid_argument_status)?,
            crate::ids::Generation::new(request.version),
        ),
        template: request
            .template
            .ok_or_else(|| Status::invalid_argument("template is required"))
            .and_then(manifest_template_from_proto)?,
        value_schema: request
            .value_schema
            .map(workload_value_schema_from_proto)
            .transpose()?
            .unwrap_or_default(),
        sleep_policy: request
            .sleep_policy
            .ok_or_else(|| Status::invalid_argument("sleep_policy is required"))
            .and_then(workload_sleep_policy_from_proto)?,
        exclusivity_keys: request
            .exclusivity_keys
            .into_iter()
            .map(workload_exclusivity_key_from_proto)
            .collect::<Result<_, _>>()?,
    };
    Ok(domain_workload::CreateWorkloadClassVersionRequest::new(
        workload_class,
    ))
}

fn workload_exclusivity_key_from_proto(
    key: pb::WorkloadExclusivityKey,
) -> Result<domain_workload::WorkloadExclusivityKeyTemplate, Status> {
    Ok(domain_workload::WorkloadExclusivityKeyTemplate::new(
        key.name,
        key.value
            .ok_or_else(|| Status::invalid_argument("exclusivity_keys.value is required"))
            .and_then(template_text_from_proto)?,
    ))
}

fn create_route_binding_request_from_proto(
    request: pb::CreateRouteBindingRequest,
) -> Result<domain_route::CreateRouteBindingRequest, Status> {
    let identity = request
        .identity
        .ok_or_else(|| Status::invalid_argument("identity is required"))
        .and_then(route_identity_from_proto)?;

    Ok(domain_route::CreateRouteBindingRequest::new(
        parse_route_binding_id(request.route_binding_id)?,
        parse_instance_id(request.instance_id)?,
        identity,
    ))
}

fn workload_class_ref_from_proto(
    reference: pb::WorkloadClassVersionRef,
) -> Result<WorkloadClassVersionRef, Status> {
    Ok(WorkloadClassVersionRef::new(
        WorkloadClassId::new(reference.class_id).map_err(invalid_argument_status)?,
        crate::ids::Generation::new(reference.version),
    ))
}

fn workload_value_schema_from_proto(
    schema: pb::WorkloadValueSchema,
) -> Result<domain_workload::WorkloadValueSchema, Status> {
    Ok(domain_workload::WorkloadValueSchema {
        fields: schema
            .fields
            .into_iter()
            .map(|(field, rule)| {
                (
                    field,
                    domain_workload::WorkloadValueFieldRule {
                        required: rule.required,
                        default: rule.default_value,
                    },
                )
            })
            .collect(),
        allow_extra: schema.allow_extra,
    })
}

fn workload_sleep_policy_from_proto(
    policy: pb::WorkloadSleepPolicy,
) -> Result<WorkloadSleepPolicy, Status> {
    let policy = WorkloadSleepPolicy {
        idle_timeout_ms: policy.idle_timeout_ms,
        idle_retry_backoff_ms: policy.idle_retry_backoff_ms,
        drain_grace_timeout_ms: policy.drain_grace_timeout_ms,
        idle_timeout_override: policy.idle_timeout_override.map(|override_policy| {
            IdleTimeoutOverridePolicy {
                value_field: override_policy.value_field,
                min_idle_timeout_ms: override_policy.min_idle_timeout_ms,
                max_idle_timeout_ms: override_policy.max_idle_timeout_ms,
            }
        }),
    };
    policy.validate().map_err(invalid_argument_status)?;

    Ok(policy)
}

fn route_identity_from_proto(
    identity: pb::RouteIdentity,
) -> Result<domain_route::RouteIdentity, Status> {
    match identity
        .kind
        .ok_or_else(|| Status::invalid_argument("route identity kind is required"))?
    {
        pb::route_identity::Kind::Http(http) => Ok(domain_route::RouteIdentity::Http {
            host: http
                .host
                .ok_or_else(|| Status::invalid_argument("HTTP route host is required"))
                .and_then(route_host_from_proto)?,
            path: http
                .path_prefix
                .map(domain_route::PathPrefix::new)
                .transpose()
                .map_err(invalid_argument_status)?,
        }),
        pb::route_identity::Kind::Sni(sni) => Ok(domain_route::RouteIdentity::Sni {
            host: sni
                .host
                .ok_or_else(|| Status::invalid_argument("SNI route host is required"))
                .and_then(route_host_from_proto)?,
        }),
    }
}

/// A binding's host is a DNS name a request can carry: frontline looks up the
/// lowercased `Host` or SNI name without its port, so any other spelling would
/// store a route that matches nothing.
fn route_host_from_proto(host: pb::RouteHost) -> Result<domain_route::RouteHost, Status> {
    let host = match pb::RouteHostKind::try_from(host.kind)
        .map_err(|_| Status::invalid_argument("route host kind is invalid"))?
    {
        pb::RouteHostKind::Exact => {
            domain_route::RouteHost::exact(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::WildcardSuffix => {
            domain_route::RouteHost::wildcard_suffix(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::Unspecified => {
            Err(Status::invalid_argument("route host kind is required"))
        }
    }?;
    let is_label = |label: &str| {
        (1..=63).contains(&label.len())
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_".contains(&byte)
            })
    };
    if host.as_str().len() > 253 || !host.as_str().split('.').all(is_label) {
        return Err(Status::invalid_argument(
            "route host must be an ASCII DNS name or IPv4 address without a port or path",
        ));
    }
    Ok(host)
}

fn put_http01_request_from_proto(
    request: pb::PutHttp01ChallengeRequest,
) -> Result<domain_http01::PutHttp01ChallengeRequest, Status> {
    let key = request
        .key
        .ok_or_else(|| Status::invalid_argument("key is required"))
        .and_then(http01_key_from_proto)?;
    domain_http01::PutHttp01ChallengeRequest::new(
        key,
        request.key_authorization,
        system_time_from_unix_millis(request.expires_at_unix_millis),
        SystemTime::now(),
    )
    .map_err(invalid_argument_status)
}

pub(super) fn http01_key_from_proto(
    key: pb::Http01ChallengeKey,
) -> Result<domain_http01::Http01ChallengeKey, Status> {
    domain_http01::Http01ChallengeKey::new(key.host, key.token).map_err(invalid_argument_status)
}

fn force_release_exclusivity_key_request_from_proto(
    request: pb::ForceReleaseExclusivityKeyRequest,
) -> Result<ForceReleaseExclusivityKeyRequest, Status> {
    Ok(ForceReleaseExclusivityKeyRequest::new(
        MaterializationTarget::new(request.cluster_id, request.namespace)
            .map_err(invalid_argument_status)?,
        request.key_name,
        request.key_value,
        request.operator,
        request.reason,
    ))
}

fn reconcile_materialization_response(
    materialization: &MaterializationRecord,
    attempted: bool,
    projection_observations: Vec<pb::ProjectionObservation>,
) -> pb::ReconcileMaterializationResponse {
    let lease = materialization.reconciliation_lease.as_ref();
    pb::ReconcileMaterializationResponse {
        found: true,
        materialization_id: materialization.id.as_str().to_owned(),
        state: materialization_state_name(materialization.state).to_owned(),
        attempted,
        lease_owner: lease.map(|lease| lease.owner.clone()).unwrap_or_default(),
        lease_expires_at_unix_millis: lease
            .and_then(|lease| unix_millis_from_system_time(lease.expires_at).ok())
            .unwrap_or_default(),
        lease_attempt: lease.map(|lease| lease.attempt).unwrap_or_default(),
        observed_refs: materialization
            .rendered_objects
            .iter()
            .map(rendered_object_ref_to_proto)
            .collect(),
        projection_observations,
        ..Default::default()
    }
}

fn materialization_state_name(state: MaterializationState) -> &'static str {
    match state {
        MaterializationState::Pending => "Pending",
        MaterializationState::Ready => "Ready",
        MaterializationState::Failed => "Failed",
        MaterializationState::Deleting => "Deleting",
        MaterializationState::Deleted => "Deleted",
    }
}

fn instance_to_proto(instance: domain_instance::InstanceRecord) -> pb::Instance {
    pb::Instance {
        instance_id: instance.id.as_str().to_owned(),
        workload_class: Some(workload_class_ref_to_proto(&instance.workload_class)),
        values: instance.values.into_iter().collect(),
        state: instance_state_to_proto(instance.state) as i32,
        generation: instance.generation.get(),
    }
}

fn workload_class_to_proto(workload_class: WorkloadClassVersion) -> pb::WorkloadClassVersion {
    pb::WorkloadClassVersion {
        reference: Some(workload_class_ref_to_proto(&workload_class.reference)),
        value_schema: Some(workload_value_schema_to_proto(workload_class.value_schema)),
        template: Some(manifest_template_to_proto(workload_class.template)),
        sleep_policy: Some(workload_sleep_policy_to_proto(workload_class.sleep_policy)),
        exclusivity_keys: workload_class
            .exclusivity_keys
            .into_iter()
            .map(workload_exclusivity_key_to_proto)
            .collect(),
    }
}

fn workload_exclusivity_key_to_proto(
    key: domain_workload::WorkloadExclusivityKeyTemplate,
) -> pb::WorkloadExclusivityKey {
    pb::WorkloadExclusivityKey {
        name: key.name,
        value: Some(template_text_to_proto(key.value)),
    }
}

fn workload_value_schema_to_proto(
    schema: domain_workload::WorkloadValueSchema,
) -> pb::WorkloadValueSchema {
    pb::WorkloadValueSchema {
        fields: schema
            .fields
            .into_iter()
            .map(|(field, rule)| {
                (
                    field,
                    pb::WorkloadValueFieldRule {
                        required: rule.required,
                        default_value: rule.default,
                    },
                )
            })
            .collect(),
        allow_extra: schema.allow_extra,
    }
}

fn workload_sleep_policy_to_proto(policy: WorkloadSleepPolicy) -> pb::WorkloadSleepPolicy {
    pb::WorkloadSleepPolicy {
        idle_timeout_ms: policy.idle_timeout_ms,
        idle_retry_backoff_ms: policy.idle_retry_backoff_ms,
        drain_grace_timeout_ms: policy.drain_grace_timeout_ms,
        idle_timeout_override: policy.idle_timeout_override.map(|override_policy| {
            pb::IdleTimeoutOverridePolicy {
                value_field: override_policy.value_field,
                min_idle_timeout_ms: override_policy.min_idle_timeout_ms,
                max_idle_timeout_ms: override_policy.max_idle_timeout_ms,
            }
        }),
    }
}

fn workload_class_ref_to_proto(
    reference: &domain_workload::WorkloadClassVersionRef,
) -> pb::WorkloadClassVersionRef {
    pb::WorkloadClassVersionRef {
        class_id: reference.class_id.as_str().to_owned(),
        version: reference.version.get(),
    }
}

fn route_binding_to_proto(route_binding: domain_route::RouteBindingRecord) -> pb::RouteBinding {
    pb::RouteBinding {
        route_binding_id: route_binding.id.as_str().to_owned(),
        instance_id: route_binding.instance_id.as_str().to_owned(),
        identity: Some(route_identity_to_proto(route_binding.identity)),
    }
}

fn route_identity_to_proto(identity: domain_route::RouteIdentity) -> pb::RouteIdentity {
    let kind = match identity {
        domain_route::RouteIdentity::Http { host, path } => {
            pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(route_host_to_proto(host)),
                path_prefix: path.map(|path| path.as_str().to_owned()),
            })
        }
        domain_route::RouteIdentity::Sni { host } => {
            pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
                host: Some(route_host_to_proto(host)),
            })
        }
    };

    pb::RouteIdentity { kind: Some(kind) }
}

fn route_host_to_proto(host: domain_route::RouteHost) -> pb::RouteHost {
    pb::RouteHost {
        kind: route_host_kind_to_proto(host.kind()) as i32,
        host: host.as_str().to_owned(),
    }
}

fn route_host_kind_to_proto(kind: domain_route::RouteHostKind) -> pb::RouteHostKind {
    match kind {
        domain_route::RouteHostKind::Exact => pb::RouteHostKind::Exact,
        domain_route::RouteHostKind::WildcardSuffix => pb::RouteHostKind::WildcardSuffix,
    }
}

fn instance_state_to_proto(state: InstanceState) -> pb::InstanceState {
    match state {
        InstanceState::Cold => pb::InstanceState::Cold,
        InstanceState::Waking => pb::InstanceState::Waking,
        InstanceState::Running => pb::InstanceState::Running,
        InstanceState::Draining => pb::InstanceState::Draining,
        InstanceState::Failed => pb::InstanceState::Failed,
        InstanceState::Deleting => pb::InstanceState::Deleting,
        InstanceState::Deleted => pb::InstanceState::Deleted,
    }
}

pub(super) fn http01_to_proto(
    challenge: domain_http01::Http01ChallengeRecord,
) -> Result<pb::Http01Challenge, Status> {
    Ok(pb::Http01Challenge {
        key: Some(pb::Http01ChallengeKey {
            host: challenge.key().host().as_str().to_owned(),
            token: challenge.key().token().to_owned(),
        }),
        key_authorization: challenge.key_authorization().to_owned(),
        expires_at_unix_millis: unix_millis_from_system_time(challenge.expires_at())?,
    })
}

fn rendered_object_ref_to_proto(object: &RenderedObjectRef) -> pb::RenderedObjectRef {
    pb::RenderedObjectRef {
        api_version: object.api_version.clone(),
        kind: object.kind.clone(),
        namespace: object.namespace.clone(),
        name: object.name.clone(),
    }
}

fn projection_observation_to_proto(
    observation: &ProjectionObservation,
) -> pb::ProjectionObservation {
    pb::ProjectionObservation {
        r#ref: Some(rendered_object_ref_to_proto(&observation.object_ref)),
        state: observation.state.as_str().to_owned(),
        reason: observation.reason.clone().unwrap_or_default(),
        finalizers: observation.finalizers.clone(),
        backend_uri: observation.backend_uri.clone().unwrap_or_default(),
    }
}

fn unix_millis_from_system_time(value: SystemTime) -> Result<i64, Status> {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| Status::invalid_argument("system time does not fit in unix millis")),
        Err(error) => {
            let millis = i64::try_from(error.duration().as_millis())
                .map_err(|_| Status::invalid_argument("system time does not fit in unix millis"))?;
            Ok(-millis)
        }
    }
}

fn system_time_from_unix_millis(value: i64) -> SystemTime {
    if value >= 0 {
        UNIX_EPOCH + Duration::from_millis(value as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(value.unsigned_abs())
    }
}

fn parse_instance_id(value: String) -> Result<InstanceId, Status> {
    InstanceId::new(value).map_err(invalid_argument_status)
}

fn parse_route_binding_id(value: String) -> Result<crate::ids::RouteBindingId, Status> {
    crate::ids::RouteBindingId::new(value).map_err(invalid_argument_status)
}

fn invalid_argument_status(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}
