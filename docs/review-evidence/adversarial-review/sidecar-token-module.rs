//! Per-instance sidecar credentials.
//!
//! A sidecar runs beside the workload it serves, which is the least trusted
//! place in the system. Giving every sidecar the same platform-wide secret would
//! mean one capture is authority over every instance, so each materialization
//! gets a credential that names exactly one instance and nothing else.
//!
//! The credential is a message authentication code over the instance ID, minted
//! and verified by the control plane with a shared signing key. Verification is
//! a hash and a constant-time compare, so a caller presenting garbage is
//! rejected without touching the database. That matters because the caller sets
//! the request rate: the sidecar shares a pod with code the platform does not
//! trust.
//!
//! The token binds the instance and nothing more. Generation and lifecycle
//! staleness stay with the compare-and-swap checks in the sleep path, which
//! already handle a sidecar reporting one generation behind the instance record.

use std::{error::Error, fmt};

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::ids::{InstanceId, InvalidInstanceIdError};

type HmacSha256 = Hmac<Sha256>;

/// Version marker so the format can change without ambiguity, and so a sidecar
/// credential is distinguishable from a static role token at a glance.
const TOKEN_PREFIX: &str = "sp1";
const TOKEN_SEPARATOR: char = '.';

/// A key used to mint and verify sidecar credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct SidecarTokenSigningKey {
    key: Vec<u8>,
}

/// Verifies sidecar credentials, accepting a previous key so a signing key can
/// be rotated without invalidating the credentials held by running pods.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SidecarTokenVerifier {
    current: SidecarTokenSigningKey,
    previous: Option<SidecarTokenSigningKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidSidecarTokenSigningKey {
    Empty,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidecarTokenError {
    Malformed,
    UnknownInstance(InvalidInstanceIdError),
    SignatureMismatch,
}

impl SidecarTokenSigningKey {
    pub fn new(key: impl Into<Vec<u8>>) -> Result<Self, InvalidSidecarTokenSigningKey> {
        let key = key.into();
        if key.is_empty() {
            return Err(InvalidSidecarTokenSigningKey::Empty);
        }

        Ok(Self { key })
    }

    /// A fixed key for `no-auth` deployments, which already accept any caller.
    /// It keeps local runs and tests on the same credential path as production
    /// without asking for configuration that would not be protecting anything.
    pub fn insecure_development_key() -> Self {
        Self {
            key: b"sleepypods-insecure-development-sidecar-key".to_vec(),
        }
    }

    /// Mints the credential injected into one instance's sidecar container.
    pub fn mint(&self, instance_id: &InstanceId) -> String {
        let payload = signed_payload(instance_id.as_str());
        let signature = self.sign(&payload);

        format!("{payload}{TOKEN_SEPARATOR}{signature}")
    }

    fn sign(&self, payload: &str) -> String {
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(&self.key)
            .expect("HMAC accepts any key length");
        mac.update(payload.as_bytes());

        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

impl SidecarTokenVerifier {
    pub fn new(current: SidecarTokenSigningKey) -> Self {
        Self {
            current,
            previous: None,
        }
    }

    pub fn with_previous_key(mut self, previous: Option<SidecarTokenSigningKey>) -> Self {
        self.previous = previous;
        self
    }

    /// Returns the instance the credential names, or why it is not usable.
    ///
    /// The signature is checked before the instance ID is interpreted, so an
    /// unauthenticated caller cannot reach the ID parsing path at all.
    pub fn verify(&self, token: &str) -> Result<InstanceId, SidecarTokenError> {
        let (payload, signature) = token
            .rsplit_once(TOKEN_SEPARATOR)
            .ok_or(SidecarTokenError::Malformed)?;
        let instance_id = payload
            .strip_prefix(TOKEN_PREFIX)
            .and_then(|rest| rest.strip_prefix(TOKEN_SEPARATOR))
            .ok_or(SidecarTokenError::Malformed)?;
        if instance_id.is_empty() {
            return Err(SidecarTokenError::Malformed);
        }

        if !self.signature_matches(payload, signature) {
            return Err(SidecarTokenError::SignatureMismatch);
        }

        InstanceId::new(instance_id).map_err(SidecarTokenError::UnknownInstance)
    }

    fn signature_matches(&self, payload: &str, signature: &str) -> bool {
        let mut matched =
            constant_time_eq(self.current.sign(payload).as_bytes(), signature.as_bytes());
        if let Some(previous) = &self.previous {
            // Both keys are always checked so verification time does not reveal
            // which key signed a credential.
            matched |= constant_time_eq(previous.sign(payload).as_bytes(), signature.as_bytes());
        }

        matched
    }
}

fn signed_payload(instance_id: &str) -> String {
    format!("{TOKEN_PREFIX}{TOKEN_SEPARATOR}{instance_id}")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

impl fmt::Debug for SidecarTokenSigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SidecarTokenSigningKey([redacted])")
    }
}

impl fmt::Display for InvalidSidecarTokenSigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("sidecar token signing key must not be empty"),
        }
    }
}

impl Error for InvalidSidecarTokenSigningKey {}

impl fmt::Display for SidecarTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => f.write_str("sidecar credential is malformed"),
            Self::UnknownInstance(error) => {
                write!(f, "sidecar credential names an invalid instance: {error}")
            }
            Self::SignatureMismatch => f.write_str("sidecar credential signature does not match"),
        }
    }
}

impl Error for SidecarTokenError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: &str) -> SidecarTokenSigningKey {
        SidecarTokenSigningKey::new(value.as_bytes().to_vec()).expect("valid key")
    }

    fn instance(value: &str) -> InstanceId {
        InstanceId::new(value).expect("valid instance ID")
    }

    #[test]
    fn minted_credential_round_trips_to_its_instance() {
        let signing = key("signing-key");
        let verifier = SidecarTokenVerifier::new(signing.clone());

        let token = signing.mint(&instance("tenant-a"));

        assert_eq!(verifier.verify(&token), Ok(instance("tenant-a")));
    }

    #[test]
    fn credential_for_one_instance_does_not_name_another() {
        let signing = key("signing-key");
        let verifier = SidecarTokenVerifier::new(signing.clone());

        let victim = verifier
            .verify(&signing.mint(&instance("victim")))
            .expect("victim credential verifies");
        let attacker = verifier
            .verify(&signing.mint(&instance("attacker")))
            .expect("attacker credential verifies");

        assert_ne!(victim, attacker);
    }

    #[test]
    fn rewriting_the_instance_in_a_credential_fails_verification() {
        let signing = key("signing-key");
        let verifier = SidecarTokenVerifier::new(signing.clone());
        let token = signing.mint(&instance("attacker"));
        let signature = token.rsplit_once('.').expect("token has a signature").1;

        let forged = format!("sp1.victim.{signature}");

        assert_eq!(
            verifier.verify(&forged),
            Err(SidecarTokenError::SignatureMismatch)
        );
    }

    #[test]
    fn credential_minted_with_another_key_is_rejected() {
        let verifier = SidecarTokenVerifier::new(key("signing-key"));

        let token = key("other-key").mint(&instance("tenant-a"));

        assert_eq!(
            verifier.verify(&token),
            Err(SidecarTokenError::SignatureMismatch)
        );
    }

    #[test]
    fn previous_key_keeps_running_pods_working_across_rotation() {
        let old = key("old-key");
        let new = key("new-key");
        let token = old.mint(&instance("tenant-a"));

        let without_previous = SidecarTokenVerifier::new(new.clone());
        assert_eq!(
            without_previous.verify(&token),
            Err(SidecarTokenError::SignatureMismatch)
        );

        let rotating = SidecarTokenVerifier::new(new).with_previous_key(Some(old));
        assert_eq!(rotating.verify(&token), Ok(instance("tenant-a")));
    }

    #[test]
    fn malformed_credentials_are_rejected_without_parsing_an_instance() {
        let verifier = SidecarTokenVerifier::new(key("signing-key"));

        for token in ["", "sp1", "sp1.tenant-a", "nope.tenant-a.abcd", "sp1..abcd"] {
            assert!(
                matches!(
                    verifier.verify(token),
                    Err(SidecarTokenError::Malformed | SidecarTokenError::SignatureMismatch)
                ),
                "{token:?} should not verify"
            );
        }
    }

    #[test]
    fn credential_is_usable_as_a_bearer_token() {
        let token = key("signing-key").mint(&instance("tenant-a"));

        assert!(token.is_ascii());
        assert!(!token.chars().any(char::is_whitespace));
        assert!(!token.chars().any(char::is_control));
    }

    #[test]
    fn signing_key_rejects_empty_material_and_redacts_debug_output() {
        assert_eq!(
            SidecarTokenSigningKey::new(Vec::new()),
            Err(InvalidSidecarTokenSigningKey::Empty)
        );
        assert!(!format!("{:?}", key("super-secret")).contains("super-secret"));
    }
}
