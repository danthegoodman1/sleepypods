use super::{error::map_postgres_error, sql, PostgresStore};
use crate::{
    certificate::*,
    store::{StoreError, StoreResult},
    Generation,
};
use deadpool_postgres::{GenericClient, Transaction};
use tokio_postgres::Row;

macro_rules! cert_columns {
    () => {
        "certificate_id, version, state, not_before_unix_millis, not_after_unix_millis, dns_names, leaf_sha256, chain_der, seal_format, seal_key_id, seal_nonce, sealed_private_key, sealing_revision"
    };
}

fn invalid(error: InvalidCertificateResource) -> StoreError {
    StoreError::invalid_argument(error.0)
}
fn rev(value: i64) -> StoreResult<CertificateRevision> {
    CertificateRevision::new(
        u64::try_from(value)
            .map_err(|_| StoreError::internal("invalid durable certificate revision"))?,
    )
    .map_err(invalid)
}
fn next(value: CertificateRevision) -> StoreResult<CertificateRevision> {
    CertificateRevision::new(
        value
            .get()
            .checked_add(1)
            .ok_or_else(|| StoreError::internal("certificate revision exhausted"))?,
    )
    .map_err(invalid)
}
fn check_revision(expected: CertificateRevision, actual: CertificateRevision) -> StoreResult<()> {
    if expected != actual {
        return Err(StoreError::GenerationConflict {
            expected: Generation::new(expected.get()),
            actual: Generation::new(actual.get()),
        });
    }
    Ok(())
}
fn metadata(row: &Row) -> StoreResult<CertificateMetadata> {
    Ok(CertificateMetadata {
        id: CertificateId::new(row.get::<_, String>("certificate_id")).map_err(invalid)?,
        version: rev(row.get("version"))?,
        state: match row.get::<_, &str>("state") {
            "active" => CertificateState::Active,
            "deleted" => CertificateState::Deleted,
            _ => return Err(StoreError::internal("invalid durable certificate state")),
        },
        not_before_unix_millis: row.get("not_before_unix_millis"),
        not_after_unix_millis: row.get("not_after_unix_millis"),
        dns_names: row.get("dns_names"),
        leaf_sha256: row
            .get::<_, Option<Vec<u8>>>("leaf_sha256")
            .unwrap_or_default(),
        sealing_key_id: row.get("seal_key_id"),
        sealing_revision: rev(row.get("sealing_revision"))?,
    })
}
fn chain_digest(chain: &[Vec<u8>]) -> Vec<u8> {
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    for cert in chain {
        digest.update(&(cert.len() as u64).to_be_bytes());
        digest.update(cert);
    }
    digest.finish().as_ref().to_vec()
}
fn bundle(
    sealer: &CertificateSealer,
    row: &Row,
    meta: &CertificateMetadata,
) -> StoreResult<CertificateBundle> {
    let chain: Vec<Vec<u8>> = row
        .get::<_, Option<_>>("chain_der")
        .ok_or_else(|| StoreError::internal("active certificate material is missing"))?;
    let envelope = SealedPrivateKey {
        format_version: row
            .get::<_, Option<i16>>("seal_format")
            .and_then(|n| u8::try_from(n).ok())
            .ok_or_else(|| StoreError::internal("invalid certificate envelope"))?,
        key_id: meta
            .sealing_key_id
            .clone()
            .ok_or_else(|| StoreError::internal("certificate sealing key ID is missing"))?,
        nonce: row
            .get::<_, Option<Vec<u8>>>("seal_nonce")
            .and_then(|n| n.try_into().ok())
            .ok_or_else(|| StoreError::internal("invalid certificate nonce"))?,
        ciphertext: row
            .get::<_, Option<Vec<u8>>>("sealed_private_key")
            .ok_or_else(|| StoreError::internal("sealed certificate key is missing"))?,
    };
    let key = sealer
        .open(&meta.id, meta.version, &chain_digest(&chain), &envelope)
        .map_err(|e| StoreError::internal(e.0))?;
    CertificateBundle::new(chain, key.to_vec()).map_err(invalid)
}
async fn lock(tx: &Transaction<'_>) -> StoreResult<()> {
    sql::query_one(
        tx,
        "SELECT revision FROM tls_certificate_revision WHERE singleton FOR UPDATE",
        &[],
    )
    .await?;
    Ok(())
}
async fn now(client: &impl GenericClient) -> StoreResult<i64> {
    Ok(sql::query_one(
        client,
        "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint",
        &[],
    )
    .await?
    .get(0))
}
async fn certificate(client: &impl GenericClient, id: &CertificateId) -> StoreResult<Option<Row>> {
    sql::query_opt(
        client,
        concat!(
            "SELECT ",
            cert_columns!(),
            " FROM certificates WHERE certificate_id=$1"
        ),
        &[&id.as_str()],
    )
    .await
}
async fn advance_revision(tx: &Transaction<'_>) -> StoreResult<i64> {
    // The mutation holds the singleton lock. Overflow aborts the transaction.
    Ok(sql::query_one(tx, "UPDATE tls_certificate_revision SET revision=revision+1 WHERE singleton RETURNING revision", &[]).await?.get(0))
}
async fn update_bound_views(
    tx: &Transaction<'_>,
    id: &CertificateId,
    remove: bool,
) -> StoreResult<()> {
    let revision = advance_revision(tx).await?;
    let changed = sql::execute(tx, "UPDATE tls_hostname_bindings SET revision=$2, last_invalidating_revision=CASE WHEN $3 THEN $2 ELSE last_invalidating_revision END, certificate_id=CASE WHEN $3 THEN NULL ELSE certificate_id END WHERE certificate_id=$1", &[&id.as_str(), &revision, &remove]).await?;
    if changed > MAX_CERTIFICATE_BINDINGS as u64 {
        return Err(StoreError::internal(
            "certificate binding inventory exceeds its bound",
        ));
    }
    Ok(())
}

pub(super) async fn publish(
    store: &PostgresStore,
    request: PublishCertificateRequest,
) -> StoreResult<CertificateMetadata> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let old = certificate(&tx, &request.id).await?;
    let old_meta = old.as_ref().map(metadata).transpose()?;
    check_revision(
        request.expected_version,
        old_meta
            .as_ref()
            .map_or(CertificateRevision::ZERO, |m| m.version),
    )?;
    if old_meta
        .as_ref()
        .is_some_and(|m| m.state == CertificateState::Deleted)
    {
        return Err(StoreError::AlreadyExists {
            resource: "retired certificate ID",
        });
    }
    let observed_at = now(&tx).await?;
    let hosts = sql::query(
        &tx,
        "SELECT hostname FROM tls_hostname_bindings WHERE certificate_id=$1 LIMIT 1025",
        &[&request.id.as_str()],
    )
    .await?;
    if hosts.len() > MAX_CERTIFICATE_BINDINGS {
        return Err(StoreError::internal(
            "certificate binding inventory exceeds its bound",
        ));
    }
    let version = next(request.expected_version)?;
    let sealing_revision = next(
        old_meta
            .as_ref()
            .map_or(CertificateRevision::ZERO, |m| m.sealing_revision),
    )?;
    let keyring = store
        .certificate_sealer
        .clone()
        .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))?;
    let (request, validated, sealed) = work
        .blocking(move || {
            let validated = validate_certificate(&request.bundle, observed_at).map_err(invalid)?;
            for row in hosts {
                validate_hostname(
                    request.bundle.chain_der(),
                    &TlsHostname::new(row.get::<_, String>(0)).map_err(invalid)?,
                )
                .map_err(invalid)?;
            }
            let sealed = keyring
                .seal(
                    &request.id,
                    version,
                    &chain_digest(request.bundle.chain_der()),
                    request.bundle.private_key_pkcs8_der(),
                )
                .map_err(|e| StoreError::internal(e.0))?;
            Ok((request, validated, sealed))
        })
        .await?;
    sql::execute(&tx, "INSERT INTO certificates(certificate_id,version,state,not_before_unix_millis,not_after_unix_millis,dns_names,leaf_sha256,chain_der,seal_format,seal_key_id,seal_nonce,sealed_private_key,sealing_revision) VALUES($1,$2,'active',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(certificate_id) DO UPDATE SET version=excluded.version,state=excluded.state,not_before_unix_millis=excluded.not_before_unix_millis,not_after_unix_millis=excluded.not_after_unix_millis,dns_names=excluded.dns_names,leaf_sha256=excluded.leaf_sha256,chain_der=excluded.chain_der,seal_format=excluded.seal_format,seal_key_id=excluded.seal_key_id,seal_nonce=excluded.seal_nonce,sealed_private_key=excluded.sealed_private_key,sealing_revision=excluded.sealing_revision", &[&request.id.as_str(),&(version.get() as i64),&validated.not_before_unix_millis,&validated.not_after_unix_millis,&validated.dns_names,&validated.leaf_sha256,&request.bundle.chain_der(),&(sealed.format_version as i16),&sealed.key_id,&&sealed.nonce[..],&sealed.ciphertext,&(sealing_revision.get() as i64)]).await?;
    update_bound_views(&tx, &request.id, false).await?;
    let result = metadata(&certificate(&tx, &request.id).await?.unwrap())?;
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(result)
}

pub(super) async fn get_metadata(
    store: &PostgresStore,
    id: CertificateId,
) -> StoreResult<Option<CertificateMetadata>> {
    let work = store.certificate_work.acquire()?;
    let client = work.client(store).await?;
    sql::query_opt(&*client, "SELECT certificate_id,version,state,not_before_unix_millis,not_after_unix_millis,dns_names,leaf_sha256,seal_key_id,sealing_revision FROM certificates WHERE certificate_id=$1", &[&id.as_str()]).await?.as_ref().map(metadata).transpose()
}
pub(super) async fn get_binding(
    store: &PostgresStore,
    hostname: TlsHostname,
) -> StoreResult<TlsBinding> {
    let work = store.certificate_work.acquire()?;
    let client = work.client(store).await?;
    let row = sql::query_opt(
        &*client,
        "SELECT certificate_id, revision, last_invalidating_revision FROM tls_hostname_bindings WHERE hostname=$1",
        &[&hostname.as_str()],
    )
    .await?;
    Ok(TlsBinding {
        hostname,
        certificate_id: row
            .as_ref()
            .and_then(|r| r.get::<_, Option<String>>(0))
            .map(CertificateId::new)
            .transpose()
            .map_err(invalid)?,
        revision: row
            .as_ref()
            .map(|r| rev(r.get(1)))
            .transpose()?
            .unwrap_or(CertificateRevision::ZERO),
        last_invalidating_revision: row
            .as_ref()
            .map(|r| rev(r.get(2)))
            .transpose()?
            .unwrap_or(CertificateRevision::ZERO),
    })
}
pub(super) async fn set_binding(
    store: &PostgresStore,
    request: SetTlsBindingRequest,
) -> StoreResult<TlsBinding> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let old = sql::query_opt(
        &tx,
        "SELECT revision FROM tls_hostname_bindings WHERE hostname=$1",
        &[&request.hostname.as_str()],
    )
    .await?;
    check_revision(
        request.expected_revision,
        old.map(|r| rev(r.get(0)))
            .transpose()?
            .unwrap_or(CertificateRevision::ZERO),
    )?;
    if let Some(id) = &request.certificate_id {
        let row = certificate(&tx, id).await?.ok_or(StoreError::NotFound {
            resource: "certificate",
        })?;
        let meta = metadata(&row)?;
        if meta.state != CertificateState::Active {
            return Err(StoreError::NotFound {
                resource: "active certificate",
            });
        }
        let keyring = store
            .certificate_sealer
            .clone()
            .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))?;
        let observed_at = now(&tx).await?;
        let hostname = request.hostname.clone();
        let checked_meta = meta.clone();
        work.blocking(move || {
            let material = bundle(&keyring, &row, &checked_meta)?;
            validate_certificate(&material, observed_at).map_err(invalid)?;
            validate_hostname(material.chain_der(), &hostname).map_err(invalid)?;
            Ok(())
        })
        .await?;
        let count: i64 = sql::query_one(
            &tx,
            "SELECT count(*) FROM tls_hostname_bindings WHERE certificate_id=$1 AND hostname<>$2",
            &[&id.as_str(), &request.hostname.as_str()],
        )
        .await?
        .get(0);
        if count >= MAX_CERTIFICATE_BINDINGS as i64 {
            return Err(StoreError::invalid_argument(
                "certificate exceeds its 1024 binding bound",
            ));
        }
    }
    let revision = advance_revision(&tx).await?;
    let id = request.certificate_id.as_ref().map(CertificateId::as_str);
    sql::execute(&tx, "INSERT INTO tls_hostname_bindings(hostname,certificate_id,revision,last_invalidating_revision) VALUES($1,$2,$3,$3) ON CONFLICT(hostname) DO UPDATE SET certificate_id=excluded.certificate_id, revision=excluded.revision,last_invalidating_revision=excluded.last_invalidating_revision", &[&request.hostname.as_str(),&id,&revision]).await?;
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(TlsBinding {
        hostname: request.hostname,
        certificate_id: request.certificate_id,
        revision: rev(revision)?,
        last_invalidating_revision: rev(revision)?,
    })
}

pub(super) async fn remove(
    store: &PostgresStore,
    request: RemoveCertificateRequest,
) -> StoreResult<CertificateMetadata> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let row = certificate(&tx, &request.id)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "certificate",
        })?;
    let meta = metadata(&row)?;
    check_revision(request.expected_version, meta.version)?;
    if meta.state != CertificateState::Active {
        return Err(StoreError::NotFound {
            resource: "active certificate",
        });
    }
    let version = next(meta.version)?;
    let sealing_revision = next(meta.sealing_revision)?;
    sql::execute(&tx, "UPDATE certificates SET version=$2,state='deleted',not_before_unix_millis=0,not_after_unix_millis=0,dns_names='{}',leaf_sha256=NULL,chain_der=NULL,seal_format=NULL,seal_key_id=NULL,seal_nonce=NULL,sealed_private_key=NULL,sealing_revision=$3 WHERE certificate_id=$1", &[&request.id.as_str(),&(version.get() as i64),&(sealing_revision.get() as i64)]).await?;
    update_bound_views(&tx, &request.id, true).await?;
    let result = metadata(&certificate(&tx, &request.id).await?.unwrap())?;
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(result)
}

pub(super) async fn resolve(
    store: &PostgresStore,
    request: ResolveTlsCertificateRequest,
) -> StoreResult<TlsCertificateResolution> {
    let work = store.certificate_work.acquire()?;
    // One statement gives a coherent binding + certificate + observed-time view.
    let client = work.client(store).await?;
    let row = sql::query_one(&*client, "SELECT b.revision AS view_revision,c.*,(extract(epoch from clock_timestamp())*1000)::bigint AS observed_at FROM (SELECT 1) anchor LEFT JOIN tls_hostname_bindings b ON b.hostname=$1 LEFT JOIN certificates c ON c.certificate_id=b.certificate_id", &[&request.hostname.as_str()]).await?;
    let view_revision = rev(row.get::<_, Option<i64>>("view_revision").unwrap_or(0))?;
    let observed_at_unix_millis = row.get("observed_at");
    let value = if row.get::<_, Option<String>>("certificate_id").is_none() {
        TlsCertificateValue::Missing
    } else {
        let metadata = metadata(&row)?;
        if metadata.state != CertificateState::Active {
            return Err(StoreError::internal(
                "TLS binding references a removed certificate",
            ));
        }
        let keyring = store
            .certificate_sealer
            .clone()
            .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))?;
        let checked_meta = metadata.clone();
        let hostname = request.hostname.clone();
        let bundle = work
            .blocking(move || {
                let bundle = bundle(&keyring, &row, &checked_meta)?;
                validate_certificate(&bundle, observed_at_unix_millis).map_err(invalid)?;
                validate_hostname(bundle.chain_der(), &hostname).map_err(invalid)?;
                Ok(bundle)
            })
            .await?;
        if request.known_view_revision == Some(view_revision) {
            TlsCertificateValue::Unchanged { metadata }
        } else {
            TlsCertificateValue::Found { metadata, bundle }
        }
    };
    Ok(TlsCertificateResolution {
        hostname: request.hostname,
        view_revision,
        observed_at_unix_millis,
        value,
    })
}

/// Opens a certificate's sealed key and seals it again under the active key.
fn reseal(
    keyring: &CertificateSealer,
    row: &Row,
    meta: &CertificateMetadata,
) -> StoreResult<SealedPrivateKey> {
    let material = bundle(keyring, row, meta)?;
    keyring
        .seal(
            &meta.id,
            meta.version,
            &chain_digest(material.chain_der()),
            material.private_key_pkcs8_der(),
        )
        .map_err(|e| StoreError::internal(e.0))
}
async fn store_seal(
    tx: &Transaction<'_>,
    meta: &CertificateMetadata,
    sealed: &SealedPrivateKey,
) -> StoreResult<()> {
    let sealing_revision = next(meta.sealing_revision)?;
    sql::execute(tx, "UPDATE certificates SET seal_format=$2,seal_key_id=$3,seal_nonce=$4,sealed_private_key=$5,sealing_revision=$6 WHERE certificate_id=$1", &[&meta.id.as_str(),&(sealed.format_version as i16),&sealed.key_id,&&sealed.nonce[..],&sealed.ciphertext,&(sealing_revision.get() as i64)]).await?;
    Ok(())
}
fn keyring(store: &PostgresStore) -> StoreResult<std::sync::Arc<CertificateSealer>> {
    store
        .certificate_sealer
        .clone()
        .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))
}

pub(super) async fn reencrypt(
    store: &PostgresStore,
    request: ReencryptCertificateRequest,
) -> StoreResult<CertificateMetadata> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let row = certificate(&tx, &request.id)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "certificate",
        })?;
    let meta = metadata(&row)?;
    check_revision(request.expected_version, meta.version)?;
    check_revision(request.expected_sealing_revision, meta.sealing_revision)?;
    if meta.state != CertificateState::Active {
        return Err(StoreError::NotFound {
            resource: "active certificate",
        });
    }
    let keyring = keyring(store)?;
    let checked_meta = meta.clone();
    let sealed = work
        .blocking(move || reseal(&keyring, &row, &checked_meta))
        .await?;
    store_seal(&tx, &meta, &sealed).await?;
    let result = metadata(&certificate(&tx, &request.id).await?.unwrap())?;
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(result)
}

/// Certificates one bulk call re-seals. The call holds the certificate lock
/// for the whole batch.
const REENCRYPT_BATCH: i64 = 16;

pub(super) async fn reencrypt_sealed_by(
    store: &PostgresStore,
    sealing_key_id: String,
) -> StoreResult<SealingKeyReencryption> {
    validate_key_id(&sealing_key_id).map_err(invalid)?;
    let keyring = keyring(store)?;
    if keyring.active_id() == sealing_key_id {
        return Err(StoreError::invalid_argument(
            "certificates under the active sealing key are current",
        ));
    }
    if !keyring.holds(&sealing_key_id) {
        return Err(StoreError::invalid_argument(
            "the sealing key ring lacks this key",
        ));
    }
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let rows = sql::query(&tx, concat!("SELECT ", cert_columns!(), " FROM certificates WHERE state='active' AND seal_key_id=$1 ORDER BY certificate_id LIMIT $2"),
            &[&sealing_key_id, &REENCRYPT_BATCH],).await?;
    let resealed = work
        .blocking(move || {
            rows.iter()
                .map(|row| {
                    let meta = metadata(row)?;
                    let sealed = reseal(&keyring, row, &meta)?;
                    Ok((meta, sealed))
                })
                .collect::<StoreResult<Vec<_>>>()
        })
        .await?;
    for (meta, sealed) in &resealed {
        store_seal(&tx, meta, sealed).await?;
    }
    let remaining: i64 = sql::query_one(
        &tx,
        "SELECT count(*) FROM certificates WHERE state='active' AND seal_key_id=$1",
        &[&sealing_key_id],
    )
    .await?
    .get(0);
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(SealingKeyReencryption {
        reencrypted: resealed.len() as u32,
        remaining: remaining as u64,
    })
}

/// Sealing key IDs that active certificates use and the configured ring lacks,
/// up to the ring's capacity.
pub(super) async fn missing_sealing_key_ids(store: &PostgresStore) -> StoreResult<Vec<String>> {
    let held = store
        .certificate_sealer
        .as_ref()
        .map(|keyring| keyring.key_ids())
        .unwrap_or_default();
    let client = store.client().await?;
    let rows = sql::query(&client, "SELECT DISTINCT seal_key_id FROM certificates WHERE state='active' AND NOT (seal_key_id = ANY($1)) ORDER BY seal_key_id LIMIT 8",
            &[&held],).await?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

// A single MVCC statement gates the scoped join on the private global revision.
// Stable polls do not evaluate the hostname subquery or load any certificate data.
const SNAPSHOT_SQL: &str = "SELECT c.revision, CASE WHEN $2::bigint IS DISTINCT FROM c.revision THEN COALESCE((SELECT jsonb_agg(jsonb_build_object('hostname', h.hostname, 'certificate_id', b.certificate_id, 'revision', COALESCE(b.revision,0), 'last_invalidating_revision', COALESCE(b.last_invalidating_revision,0)) ORDER BY h.ordinality) FROM unnest($1::text[]) WITH ORDINALITY h(hostname,ordinality) LEFT JOIN tls_hostname_bindings b ON b.hostname=h.hostname),'[]'::jsonb) END AS bindings FROM tls_certificate_revision c WHERE singleton";

pub(super) async fn snapshot(
    store: &PostgresStore,
    hostnames: Vec<TlsHostname>,
    known_revision: Option<CertificateRevision>,
) -> StoreResult<Option<TlsBindingSnapshot>> {
    if hostnames.len() > MAX_CERTIFICATE_BINDINGS {
        return Err(StoreError::invalid_argument(
            "TLS interests exceed 1024 hosts",
        ));
    }
    let hosts: Vec<_> = hostnames.iter().map(|h| h.as_str()).collect();
    let known = known_revision.map(|r| r.get() as i64);
    let work = store.certificate_work.acquire_watch().await?;
    let client = work.client(store).await?;
    let row = sql::query_one(&*client, SNAPSHOT_SQL, &[&hosts, &known]).await?;
    let Some(values) = row.get::<_, Option<serde_json::Value>>("bindings") else {
        return Ok(None);
    };
    let mut bindings = Vec::with_capacity(hosts.len());
    for v in values
        .as_array()
        .ok_or_else(|| StoreError::internal("invalid TLS snapshot"))?
    {
        bindings.push(TlsBinding {
            hostname: TlsHostname::new(
                v["hostname"]
                    .as_str()
                    .ok_or_else(|| StoreError::internal("invalid TLS snapshot hostname"))?,
            )
            .map_err(invalid)?,
            certificate_id: v["certificate_id"]
                .as_str()
                .map(CertificateId::new)
                .transpose()
                .map_err(invalid)?,
            revision: rev(v["revision"]
                .as_i64()
                .ok_or_else(|| StoreError::internal("invalid TLS snapshot revision"))?)?,
            last_invalidating_revision: rev(v["last_invalidating_revision"]
                .as_i64()
                .ok_or_else(|| StoreError::internal("invalid TLS invalidating revision"))?)?,
        });
    }
    Ok(Some(TlsBindingSnapshot {
        revision: rev(row.get("revision"))?,
        bindings,
    }))
}
