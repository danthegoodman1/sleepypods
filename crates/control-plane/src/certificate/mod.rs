//! Certificate validation has no store or sealing dependency. It can be reused
//! by a receiving TLS adapter without giving that adapter persistence access.
mod seal;
pub(crate) use seal::{validate_key_id, SealedPrivateKey};
pub use seal::{CertificateSealer, SealingKey};
pub use sleepypods_api::certificate::*;
pub use sleepypods_certificate::{validate_certificate, validate_hostname, ValidatedCertificate};

#[cfg(test)]
mod tests;

pub(crate) mod work;

/// The outcome of re-sealing a batch of the certificates held under one
/// sealing key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealingKeyReencryption {
    pub reencrypted: u32,
    /// Active certificates still sealed under the key.
    pub remaining: u64,
}
