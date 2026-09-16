//! Shared wire, routing and client authentication contracts. No server implementation.
/// Initial activation protection: the default 130 s cold-route wait plus the
/// larger of the 10 s connect and 60 s initial HTTP-header setup budgets.
/// Frontline configurations must fit within this shared finite ceiling.
pub const INITIAL_ACTIVATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(190);

/// Bounded retry hint on an expected ReportIdle FailedPrecondition response.
pub const IDLE_RETRY_AFTER_METADATA: &str = "sleepypods-idle-retry-after-ms";

pub mod auth;
pub mod certificate;
pub mod http01;
pub mod instance;
pub mod materialization;
pub mod route;
pub mod transport;
pub mod ids {
    pub use sleepypods_types::*;
}
pub mod pb {
    tonic::include_proto!("sleepypods.controlplane.v1");
}
pub use auth::{BearerToken, InvalidBearerToken, OptionalBearerTokenInterceptor};
pub use certificate::*;
pub use http01::*;
pub use instance::InstanceState;
pub use materialization::{BackendAddress, BackendEndpoint, MaterializationTarget};
pub use route::*;
pub use sleepypods_types::*;

impl std::fmt::Debug for pb::CertificateBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateBundle")
            .field("chain_entries", &self.chain_der.len())
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod certificate_wire_debug_tests {
    use super::pb;

    #[test]
    fn enclosing_publication_and_found_debug_redact_private_key_bytes() {
        let marker = b"PRIVATE-PROTOBUF-DEBUG-MARKER";
        let bundle = pb::CertificateBundle {
            chain_der: vec![vec![1, 2, 3]],
            private_key_pkcs8_der: marker.to_vec(),
        };
        let publication = pb::PublishCertificateRequest {
            certificate_id: "debug-test".into(),
            expected_version: Some(0),
            bundle: Some(bundle.clone()),
        };
        let resolution = pb::ResolveTlsCertificateResponse {
            server_name: "app.example".into(),
            view_revision: 2,
            value: Some(pb::resolve_tls_certificate_response::Value::Found(
                pb::FoundTlsCertificate {
                    metadata: None,
                    bundle: Some(bundle),
                },
            )),
            ..Default::default()
        };
        let byte_debug = format!("{:?}", marker.as_slice());
        for debug in [format!("{publication:?}"), format!("{resolution:?}")] {
            assert!(debug.contains("[REDACTED]"));
            assert!(!debug.contains(std::str::from_utf8(marker).unwrap()));
            assert!(!debug.contains(&byte_debug));
        }
    }
}
