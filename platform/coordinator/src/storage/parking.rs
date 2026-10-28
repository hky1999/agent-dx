//! Parking orchestration has the same durable authority and epoch fence as budgets.
//! The host cache is a recovery witness, not another capacity ledger.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParkingJournal {
    pub owner_sha256: String,
    pub revision: u64,
    pub sha256: String,
    /// Bounded private controller protocol, not an Environment resource grant.
    pub body: serde_json::Value,
}
impl ParkingJournal {
    pub(super) fn validate(&self) -> Result<()> {
        if self.owner_sha256.len() != 64
            || !self
                .owner_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.revision == 0
            || !matches!(
                self.body.get("schema").and_then(serde_json::Value::as_u64),
                Some(1 | 2)
            )
            || self
                .body
                .get("revision")
                .and_then(serde_json::Value::as_u64)
                != Some(self.revision)
            || self.body.get("identity").is_none_or(|v| !v.is_object())
            || self
                .body
                .get("operations")
                .and_then(serde_json::Value::as_object)
                .is_none_or(|v| v.len() > 32)
        {
            return Err(Error::Invalid("invalid parking journal contract".into()));
        }
        history_seal(&self.body)?;
        let bytes = encode(&self.body)?;
        if bytes.len() > 2 << 20 || self.sha256 != format!("{:x}", Sha256::digest(bytes.as_bytes()))
        {
            return Err(Error::Invalid(
                "parking journal bound or digest differs".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistorySeal {
    index_sha256: String,
    through: u64,
    owner_reservation: String,
    owner_sha256: String,
    contract_sha256: String,
    resources: adx_core::Resources,
}

fn history_seal(body: &serde_json::Value) -> Result<Option<HistorySeal>> {
    if body.get("schema").and_then(serde_json::Value::as_u64) == Some(1) {
        if body.get("history_seal").is_some() {
            return Err(Error::Invalid(
                "a history seal requires the read-only schema".into(),
            ));
        }
        return Ok(None);
    }
    let seal: HistorySeal =
        serde_json::from_value(body.get("history_seal").cloned().ok_or_else(|| {
            Error::Invalid("a sealed catalog requires its storage ownership contract".into())
        })?)
        .map_err(|_| Error::Invalid("invalid history seal contract".into()))?;
    super::overhead::field(&seal.owner_reservation)?;
    for value in [
        &seal.index_sha256,
        &seal.owner_sha256,
        &seal.contract_sha256,
    ] {
        super::overhead::digest(value)?;
    }
    seal.resources.validate()?;
    let resources = body["history_seal"]["resources"]
        .as_object()
        .ok_or_else(|| Error::Invalid("complete history storage resources required".into()))?;
    if resources.len() != 3
        || seal.resources.memory_bytes == 0
        || seal.resources.disk_bytes == 0
        || seal.through == 0
        || body
            .get("batches")
            .and_then(|v| v.get("sequence"))
            .and_then(serde_json::Value::as_u64)
            != Some(seal.through)
    {
        return Err(Error::Invalid(
            "sealed catalog storage or sequence differs".into(),
        ));
    }
    let operations = body
        .get("operations")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| Error::Invalid("sealed catalog batches required".into()))?;
    if operations.is_empty()
        || body
            .get("batches")
            .and_then(|v| v.get("current"))
            .and_then(serde_json::Value::as_str)
            .is_none_or(|current| !operations.contains_key(current))
    {
        return Err(Error::Invalid(
            "the complete final catalog batch is required".into(),
        ));
    }
    for row in operations.values() {
        if row.get("phase").and_then(serde_json::Value::as_str) != Some("sealed")
            || row.get("journals").is_some()
            || row.get("private_collection").is_some_and(|value| {
                value.get("phase").and_then(serde_json::Value::as_str) != Some("complete")
            })
        {
            return Err(Error::Conflict);
        }
        super::overhead::digest(
            row.get("archive")
                .and_then(|value| value.get("sha256"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| Error::Invalid("original batch archive digest required".into()))?,
        )?;
    }
    if let Some(diagnostics) = body.get("diagnostics") {
        for diagnostic in diagnostics.as_object().ok_or(Error::Conflict)?.values() {
            if diagnostic.get("phase").and_then(serde_json::Value::as_str) != Some("terminal") {
                return Err(Error::Conflict);
            }
            super::overhead::digest(
                diagnostic
                    .get("retirement_sha256")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(Error::Conflict)?,
            )?;
        }
    }
    Ok(Some(seal))
}

fn check_history_storage(raw: &BTreeMap<String, String>, seal: &HistorySeal) -> Result<()> {
    let field = super::overhead::field(&seal.owner_reservation)?;
    let reservation: super::overhead::OverheadReservation =
        decode(raw.get(&field).ok_or(Error::Conflict)?)?;
    reservation.validate()?;
    if reservation.retirement_proof_sha256.is_some()
        || reservation.owner_sha256 != seal.owner_sha256
        || reservation.resources != seal.resources
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

const MAX_WRITABLE_JOURNALS: usize = 16;
const MAX_SEALED_CATALOGS: usize = 64;
const MAX_SEALED_CATALOG_BYTES: usize = 16 << 20;

pub(super) fn check_journal_capacity(
    raw: &BTreeMap<String, String>,
    replacement: Option<(&str, &ParkingJournal)>,
) -> Result<()> {
    let mut writable = 0;
    let mut sealed = 0;
    let mut sealed_bytes = 0_usize;
    let mut add = |journal: &ParkingJournal, serialized_bytes: usize| -> Result<()> {
        if journal
            .body
            .get("schema")
            .and_then(serde_json::Value::as_u64)
            == Some(2)
        {
            sealed += 1;
            sealed_bytes = sealed_bytes
                .checked_add(serialized_bytes)
                .ok_or(Error::NoCapacity)?;
        } else {
            writable += 1;
        }
        if writable > MAX_WRITABLE_JOURNALS
            || sealed > MAX_SEALED_CATALOGS
            || sealed_bytes > MAX_SEALED_CATALOG_BYTES
        {
            return Err(Error::NoCapacity);
        }
        Ok(())
    };
    // Account for the complete replacement once, within the same global CAS.
    // A closed catalog keeps its original bytes and immutable ownership seal.
    for (field, value) in raw {
        if field.starts_with("parking:") && replacement.is_none_or(|(key, _)| field != key) {
            let journal: ParkingJournal = decode(value)?;
            journal.validate()?;
            add(&journal, value.len())?;
        }
    }
    if let Some((_, desired)) = replacement {
        add(desired, encode(desired)?.len())?;
    }
    Ok(())
}

impl Session {
    pub async fn parking_journal(&self, id: &str) -> Result<Option<ParkingJournal>> {
        super::overhead::field(id)?;
        let raw = self.store.raw().await?;
        let header = self.header(&raw.get(HEADER).cloned())?;
        let Some(value) = raw.get(&format!("parking:{id}")) else {
            return Ok(None);
        };
        if !header.parking_journals || header.schema < 11 {
            return Err(Error::Conflict);
        }
        let journal: ParkingJournal = decode(value)?;
        journal.validate()?;
        Ok(Some(journal))
    }
    /// Publish before any external effect. Exact replies lost after commit replay
    /// idempotently; stale revisions, replaced owners and oversized bodies reject.
    pub async fn publish_parking_journal(
        &self,
        id: &str,
        owner_sha256: &str,
        expected_revision: u64,
        body: serde_json::Value,
    ) -> Result<()> {
        super::overhead::field(id)?;
        let revision = expected_revision.checked_add(1).ok_or(Error::Conflict)?;
        let desired = ParkingJournal {
            owner_sha256: owner_sha256.into(),
            revision,
            sha256: format!("{:x}", Sha256::digest(encode(&body)?.as_bytes())),
            body,
        };
        desired.validate()?;
        let seal = history_seal(&desired.body)?;
        let key = format!("parking:{id}");
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if header.host_budgets.is_none() {
                return Err(Error::Conflict);
            }
            if let Some(value) = raw.get(&key) {
                let old: ParkingJournal = decode(value)?;
                old.validate()?;
                if old == desired {
                    return Ok(());
                }
                if old.owner_sha256 != owner_sha256
                    || old.revision != expected_revision
                    || old.body.get("schema").and_then(serde_json::Value::as_u64) == Some(2)
                {
                    return Err(Error::Conflict);
                }
                if seal.is_some() {
                    let mut expected = old.body.clone();
                    expected["schema"] = serde_json::json!(2);
                    expected["revision"] = serde_json::json!(desired.revision);
                    expected["history_seal"] = desired.body["history_seal"].clone();
                    if expected != desired.body {
                        return Err(Error::Conflict);
                    }
                }
            } else if expected_revision != 0 || seal.is_some() {
                return Err(Error::Conflict);
            }
            if let Some(seal) = &seal {
                check_history_storage(&raw, seal)?;
            }
            check_journal_capacity(&raw, Some((&key, &desired)))?;
            header.parking_journals = true;
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&key, encode(&desired)?)),
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent parking journal publication; retry".into(),
        ))
    }
}
