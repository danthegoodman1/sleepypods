//! Native-only certificate mappings. Error text never includes request/material data.
use super::pb;
use crate::{
    auth::CallerRole,
    certificate::*,
    store::{ControlPlaneStore, StoreError},
};
use tonic::{Request, Response, Status};

pub(crate) fn require_secure<T>(request: &Request<T>, role: CallerRole) -> Result<(), Status> {
    use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
    let tls = request
        .extensions()
        .get::<TlsConnectInfo<crate::runtime_io::ConnectionProgress>>()
        .is_some()
        || request
            .extensions()
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .is_some();
    let native = request
        .metadata()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| matches!(v, "application/grpc" | "application/grpc+proto"));
    if !tls || !native {
        return Err(Status::permission_denied(
            "certificate operations require native TLS",
        ));
    }
    if request
        .extensions()
        .get::<crate::auth::AuthenticatedRole>()
        .map(|r| r.0)
        != Some(role)
    {
        return Err(Status::permission_denied(
            "certificate operations require authenticated role credentials",
        ));
    }
    Ok(())
}
fn invalid(_: InvalidCertificateResource) -> Status {
    Status::invalid_argument("invalid certificate resource")
}
fn required_revision(value: Option<u64>) -> Result<CertificateRevision, Status> {
    CertificateRevision::new(
        value.ok_or_else(|| Status::invalid_argument("expected revision is required"))?,
    )
    .map_err(invalid)
}
fn store_error(error: StoreError) -> Status {
    match error {
        StoreError::InvalidArgument { .. } => {
            Status::invalid_argument("certificate validation failed")
        }
        StoreError::NotFound { .. } => Status::not_found("certificate not found"),
        StoreError::AlreadyExists { .. } => {
            Status::already_exists("certificate identity is retired")
        }
        StoreError::GenerationConflict { expected, actual } => Status::failed_precondition(
            format!("certificate revision conflict: expected {expected}, actual {actual}"),
        ),
        StoreError::Unavailable { .. } => Status::unavailable("certificate storage unavailable"),
        _ => Status::internal("certificate operation failed"),
    }
}
pub(crate) fn metadata(m: CertificateMetadata) -> pb::CertificateMetadata {
    pb::CertificateMetadata {
        certificate_id: m.id.as_str().to_owned(),
        version: m.version.get(),
        deleted: m.state == CertificateState::Deleted,
        not_before_unix_millis: m.not_before_unix_millis,
        not_after_unix_millis: m.not_after_unix_millis,
        dns_names: m.dns_names,
        leaf_sha256: m.leaf_sha256,
        sealing_key_id: m.sealing_key_id,
        sealing_revision: m.sealing_revision.get(),
    }
}
fn binding(b: TlsBinding) -> pb::TlsBinding {
    pb::TlsBinding {
        hostname: b.hostname.as_str().to_owned(),
        certificate_id: b.certificate_id.map(|id| id.as_str().to_owned()),
        revision: b.revision.get(),
        last_invalidating_revision: b.last_invalidating_revision.get(),
    }
}
pub(crate) async fn publish(
    store: &dyn ControlPlaneStore,
    request: Request<pb::PublishCertificateRequest>,
) -> Result<Response<pb::CertificateMetadata>, Status> {
    require_secure(&request, CallerRole::Operator)?;
    let r = request.into_inner();
    let b = r
        .bundle
        .ok_or_else(|| Status::invalid_argument("bundle is required"))?;
    let bundle = CertificateBundle::new(b.chain_der, b.private_key_pkcs8_der).map_err(invalid)?;
    let result = store
        .publish_certificate(PublishCertificateRequest {
            id: CertificateId::new(r.certificate_id).map_err(invalid)?,
            expected_version: required_revision(r.expected_version)?,
            bundle,
        })
        .await
        .map_err(store_error)?;
    Ok(Response::new(metadata(result)))
}
pub(crate) async fn get_metadata(
    store: &dyn ControlPlaneStore,
    request: Request<pb::GetCertificateMetadataRequest>,
) -> Result<Response<pb::CertificateMetadata>, Status> {
    require_secure(&request, CallerRole::Operator)?;
    let result = store
        .get_certificate_metadata(
            CertificateId::new(request.into_inner().certificate_id).map_err(invalid)?,
        )
        .await
        .map_err(store_error)?
        .ok_or_else(|| Status::not_found("certificate not found"))?;
    Ok(Response::new(metadata(result)))
}
pub(crate) async fn set_binding(
    store: &dyn ControlPlaneStore,
    request: Request<pb::SetTlsBindingRequest>,
) -> Result<Response<pb::TlsBinding>, Status> {
    require_secure(&request, CallerRole::Operator)?;
    let r = request.into_inner();
    let result = store
        .set_tls_binding(SetTlsBindingRequest {
            hostname: TlsHostname::new(r.hostname).map_err(invalid)?,
            expected_revision: required_revision(r.expected_revision)?,
            certificate_id: r
                .certificate_id
                .map(CertificateId::new)
                .transpose()
                .map_err(invalid)?,
        })
        .await
        .map_err(store_error)?;
    Ok(Response::new(binding(result)))
}
pub(crate) async fn get_binding(
    store: &dyn ControlPlaneStore,
    request: Request<pb::GetTlsBindingRequest>,
) -> Result<Response<pb::TlsBinding>, Status> {
    require_secure(&request, CallerRole::Operator)?;
    Ok(Response::new(binding(
        store
            .get_tls_binding(TlsHostname::new(request.into_inner().hostname).map_err(invalid)?)
            .await
            .map_err(store_error)?,
    )))
}
pub(crate) async fn remove(
    store: &dyn ControlPlaneStore,
    request: Request<pb::RemoveCertificateRequest>,
) -> Result<Response<pb::CertificateMetadata>, Status> {
    require_secure(&request, CallerRole::Operator)?;
    let r = request.into_inner();
    Ok(Response::new(metadata(
        store
            .remove_certificate(RemoveCertificateRequest {
                id: CertificateId::new(r.certificate_id).map_err(invalid)?,
                expected_version: required_revision(r.expected_version)?,
            })
            .await
            .map_err(store_error)?,
    )))
}
pub(crate) async fn reencrypt(
    store: &dyn ControlPlaneStore,
    request: Request<pb::ReencryptCertificateRequest>,
) -> Result<Response<pb::CertificateMetadata>, Status> {
    require_secure(&request, CallerRole::Operator)?;
    let r = request.into_inner();
    Ok(Response::new(metadata(
        store
            .reencrypt_certificate(ReencryptCertificateRequest {
                id: CertificateId::new(r.certificate_id).map_err(invalid)?,
                expected_version: required_revision(r.expected_version)?,
                expected_sealing_revision: required_revision(r.expected_sealing_revision)?,
            })
            .await
            .map_err(store_error)?,
    )))
}
pub(crate) async fn reencrypt_sealed_by(
    store: &dyn ControlPlaneStore,
    request: Request<pb::ReencryptCertificatesRequest>,
) -> Result<Response<pb::ReencryptCertificatesResponse>, Status> {
    require_secure(&request, CallerRole::Operator)?;
    let result = store
        .reencrypt_certificates_sealed_by(request.into_inner().sealing_key_id)
        .await
        .map_err(|error| match error {
            // The store's reasons name the key ring's state and carry no material.
            StoreError::InvalidArgument { message } => Status::invalid_argument(message),
            error => store_error(error),
        })?;
    Ok(Response::new(pb::ReencryptCertificatesResponse {
        reencrypted: result.reencrypted,
        remaining: result.remaining,
    }))
}
pub(crate) async fn resolve(
    store: &dyn ControlPlaneStore,
    request: Request<pb::ResolveTlsCertificateRequest>,
) -> Result<Response<pb::ResolveTlsCertificateResponse>, Status> {
    require_secure(&request, CallerRole::Proxy)?;
    let r = request.into_inner();
    let result = store
        .resolve_tls_certificate(ResolveTlsCertificateRequest {
            hostname: TlsHostname::new(r.server_name).map_err(invalid)?,
            known_view_revision: r
                .known_view_revision
                .map(CertificateRevision::new)
                .transpose()
                .map_err(invalid)?,
        })
        .await
        .map_err(store_error)?;
    use pb::resolve_tls_certificate_response::Value;
    let (value, ttl) = match result.value {
        TlsCertificateValue::Missing => (Value::Missing(pb::MissingTlsCertificate {}), 1000),
        TlsCertificateValue::Found {
            metadata: m,
            bundle,
        } => {
            let ttl = m
                .not_after_unix_millis
                .saturating_sub(result.observed_at_unix_millis)
                .clamp(0, 300_000) as u64;
            (
                Value::Found(pb::FoundTlsCertificate {
                    metadata: Some(metadata(m)),
                    bundle: Some(pb::CertificateBundle {
                        chain_der: bundle.chain_der().to_vec(),
                        private_key_pkcs8_der: bundle.private_key_pkcs8_der().to_vec(),
                    }),
                }),
                ttl,
            )
        }
        TlsCertificateValue::Unchanged { metadata: m } => {
            let ttl = m
                .not_after_unix_millis
                .saturating_sub(result.observed_at_unix_millis)
                .clamp(0, 300_000) as u64;
            (Value::Unchanged(metadata(m)), ttl)
        }
    };
    Ok(Response::new(pb::ResolveTlsCertificateResponse {
        server_name: result.hostname.as_str().to_owned(),
        view_revision: result.view_revision.get(),
        observed_at_unix_millis: result.observed_at_unix_millis,
        authorization_ttl_millis: ttl,
        value: Some(value),
    }))
}
