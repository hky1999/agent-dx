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
            || self.body.get("schema").and_then(serde_json::Value::as_u64) != Some(1)
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
                if old.owner_sha256 != owner_sha256 || old.revision != expected_revision {
                    return Err(Error::Conflict);
                }
            } else if expected_revision != 0 {
                return Err(Error::Conflict);
            } else if raw.keys().filter(|k| k.starts_with("parking:")).count() >= 16 {
                return Err(Error::NoCapacity);
            }
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
