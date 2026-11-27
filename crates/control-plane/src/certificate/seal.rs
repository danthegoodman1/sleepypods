use super::{CertificateId, CertificateRevision, InvalidCertificateResource};
use ring::{
    aead::{self, Aad, LessSafeKey, Nonce, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use std::{collections::BTreeMap, fmt};
use zeroize::Zeroizing;

const FORMAT_VERSION: u8 = 1;
const MAX_SEALING_KEYS: usize = 8;

/// Deployment-provided key material. Never persisted by this module.
pub struct SealingKey {
    id: String,
    bytes: Zeroizing<[u8; 32]>,
}
impl SealingKey {
    pub fn new(id: impl Into<String>, bytes: [u8; 32]) -> Result<Self, InvalidCertificateResource> {
        let bytes = Zeroizing::new(bytes);
        let id = id.into();
        validate_key_id(&id)?;
        Ok(Self { id, bytes })
    }
}
impl fmt::Debug for SealingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealingKey")
            .field("id", &self.id)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// Small active/read keyring. Old keys may decrypt; only the active key seals.
/// Configuration owns these keys outside PostgreSQL. No fallback key exists.
pub struct CertificateSealer {
    active_id: String,
    keys: BTreeMap<String, LessSafeKey>,
}
impl fmt::Debug for CertificateSealer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertificateSealer")
            .field("active_id", &self.active_id)
            .field("key_count", &self.keys.len())
            .finish()
    }
}
#[derive(Clone, Debug)]
pub(crate) struct SealedPrivateKey {
    pub format_version: u8,
    pub key_id: String,
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
}
impl CertificateSealer {
    pub fn new(
        active_id: impl Into<String>,
        keys: Vec<SealingKey>,
    ) -> Result<Self, InvalidCertificateResource> {
        let active_id = active_id.into();
        validate_key_id(&active_id)?;
        if keys.is_empty() || keys.len() > MAX_SEALING_KEYS {
            return Err(InvalidCertificateResource(
                "sealing keyring requires 1–8 keys",
            ));
        }
        let mut result = BTreeMap::new();
        for key in keys {
            let cipher = LessSafeKey::new(
                UnboundKey::new(&aead::AES_256_GCM, key.bytes.as_ref())
                    .map_err(|_| InvalidCertificateResource("invalid sealing key"))?,
            );
            if result.insert(key.id.clone(), cipher).is_some() {
                return Err(InvalidCertificateResource("duplicate sealing key ID"));
            }
        }
        if !result.contains_key(&active_id) {
            return Err(InvalidCertificateResource(
                "active sealing key is unavailable",
            ));
        }
        Ok(Self {
            active_id,
            keys: result,
        })
    }
    /// The key new seals use.
    pub(crate) fn active_id(&self) -> &str {
        &self.active_id
    }

    /// Whether the ring can open a seal made with `key_id`.
    pub(crate) fn holds(&self, key_id: &str) -> bool {
        self.keys.contains_key(key_id)
    }

    pub(crate) fn key_ids(&self) -> Vec<String> {
        self.keys.keys().cloned().collect()
    }

    pub(crate) fn seal(
        &self,
        id: &CertificateId,
        version: CertificateRevision,
        chain_digest: &[u8],
        plaintext: &[u8],
    ) -> Result<SealedPrivateKey, InvalidCertificateResource> {
        let mut nonce = [0; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| InvalidCertificateResource("sealing nonce generation failed"))?;
        let aad = associated_data(id, version, chain_digest, &self.active_id);
        let mut ciphertext = Zeroizing::new(plaintext.to_vec());
        self.keys[&self.active_id]
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut *ciphertext,
            )
            .map_err(|_| InvalidCertificateResource("certificate sealing failed"))?;
        Ok(SealedPrivateKey {
            format_version: FORMAT_VERSION,
            key_id: self.active_id.clone(),
            nonce,
            ciphertext: ciphertext.to_vec(),
        })
    }
    pub(crate) fn open(
        &self,
        id: &CertificateId,
        version: CertificateRevision,
        chain_digest: &[u8],
        envelope: &SealedPrivateKey,
    ) -> Result<Zeroizing<Vec<u8>>, InvalidCertificateResource> {
        if envelope.format_version != FORMAT_VERSION {
            return Err(InvalidCertificateResource(
                "unsupported certificate sealing envelope",
            ));
        }
        let key = self
            .keys
            .get(&envelope.key_id)
            .ok_or(InvalidCertificateResource(
                "certificate sealing key is unavailable",
            ))?;
        let aad = associated_data(id, version, chain_digest, &envelope.key_id);
        let mut plaintext = Zeroizing::new(envelope.ciphertext.clone());
        let length = key
            .open_in_place(
                Nonce::assume_unique_for_key(envelope.nonce),
                Aad::from(aad),
                &mut plaintext,
            )
            .map_err(|_| InvalidCertificateResource("certificate sealing authentication failed"))?
            .len();
        plaintext.truncate(length);
        Ok(plaintext)
    }
}
fn associated_data(
    id: &CertificateId,
    version: CertificateRevision,
    chain_digest: &[u8],
    key_id: &str,
) -> Vec<u8> {
    let mut aad = b"sleepypods-certificate-private-key".to_vec();
    aad.push(FORMAT_VERSION);
    aad.extend_from_slice(&version.get().to_be_bytes());
    for part in [id.as_str().as_bytes(), key_id.as_bytes(), chain_digest] {
        aad.extend_from_slice(&(part.len() as u32).to_be_bytes());
        aad.extend_from_slice(part);
    }
    aad
}
pub(crate) fn validate_key_id(id: &str) -> Result<(), InvalidCertificateResource> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(InvalidCertificateResource(
            "sealing key ID requires 1–64 ASCII identifier characters",
        ));
    }
    Ok(())
}
