//! Trusted host service and checkpoint reservations share the execution CAS.
//! Retirement is an administrative attestation, not a timeout or liveness lease.
use super::*;
use adx_core::Resources;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverheadReservation {
    pub host_id: String,
    /// SHA256 of the immutable, independently retained physical ownership descriptor.
    pub owner_sha256: String,
    pub resources: Resources,
    /// Caller verifies all processes, temporary files and retained artifacts have retired.
    pub retirement_proof_sha256: Option<String>,
}
pub(super) fn digest(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
    {
        return Err(Error::Invalid("SHA256 required".into()));
    }
    Ok(())
}
pub(super) fn field(id: &str) -> Result<String> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || b"_-".contains(&v))
    {
        return Err(Error::Invalid("bounded reservation ID required".into()));
    }
    Ok(format!("overhead:{id}"))
}
impl OverheadReservation {
    pub(super) fn validate(&self) -> Result<()> {
        self.resources.validate()?;
        digest(&self.owner_sha256)?;
        if let Some(proof) = &self.retirement_proof_sha256 {
            digest(proof)?;
        }
        if self.host_id.is_empty()
            || self.host_id.len() > 128
            || self.host_id.trim() != self.host_id
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
impl RedisStore {
    /// Attach a trusted host administrator to an existing epoch without starting a
    /// Coordinator, changing ownership, initializing an empty store or fencing nodes.
    pub async fn attach_overhead_session(&self, expected_epoch: u64) -> Result<Session> {
        let raw = self.raw().await?;
        let header: Header = decode(raw.get(HEADER).ok_or(Error::NotFound)?)?;
        header.validate()?;
        if expected_epoch != header.epoch || header.host_budgets.is_none() {
            return Err(Error::Conflict);
        }
        Ok(Session {
            store: self.clone(),
            epoch: expected_epoch,
        })
    }
}
impl Session {
    /// Reserve before creating a service or checkpoint. Replay must match all bytes.
    /// Tombstones are bounded and cannot be reused, including after an uncertain reply.
    pub async fn reserve_overhead(&self, id: &str, reservation: OverheadReservation) -> Result<()> {
        reservation.validate()?;
        if reservation.retirement_proof_sha256.is_some() {
            return Err(Error::Conflict);
        }
        let key = field(id)?;
        for _ in 0..ATTEMPTS {
            let mut raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if let Some(value) = raw.get(&key) {
                let old: OverheadReservation = decode(value)?;
                return if old == reservation {
                    Ok(())
                } else {
                    Err(Error::Conflict)
                };
            }
            if header
                .host_budgets
                .as_ref()
                .is_none_or(|b| !b.contains_key(&reservation.host_id))
            {
                return Err(Error::Conflict);
            }
            if raw.keys().filter(|k| k.starts_with("overhead:")).count() >= 512 {
                return Err(Error::NoCapacity);
            }
            let encoded = encode(&reservation)?;
            raw.insert(key.clone(), encoded.clone());
            header.overhead_budget = true;
            budget::check_holds(&raw, &header, None)?;
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&key, encoded)),
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent overhead reservation; retry".into(),
        ))
    }
    /// Trusted host controller must verify exact cgroup retirement and all storage
    /// references before calling. Unknown results retain the complete reservation.
    pub async fn retire_overhead(
        &self,
        id: &str,
        owner_sha256: &str,
        proof_sha256: &str,
    ) -> Result<()> {
        let key = field(id)?;
        digest(owner_sha256)?;
        digest(proof_sha256)?;
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            let mut reservation: OverheadReservation =
                decode(raw.get(&key).ok_or(Error::NotFound)?)?;
            reservation.validate()?;
            if reservation.owner_sha256 != owner_sha256 {
                return Err(Error::Conflict);
            }
            if let Some(proof) = reservation.retirement_proof_sha256 {
                return if proof == proof_sha256 {
                    Ok(())
                } else {
                    Err(Error::Conflict)
                };
            }
            for (key, value) in &raw {
                if key.starts_with("environment:") {
                    let environment: StoredEnvironment = decode(value)?;
                    if environment.execution.as_ref().is_some_and(|e| {
                        e.pending && e.peak.as_ref().is_some_and(|p| p.reservation_id == id)
                    }) {
                        return Err(Error::Conflict);
                    }
                }
            }
            reservation.retirement_proof_sha256 = Some(proof_sha256.into());
            header.overhead_budget = true;
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&key, encode(&reservation)?)),
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent overhead retirement; retry".into(),
        ))
    }
    pub async fn overhead_reservations(&self) -> Result<BTreeMap<String, OverheadReservation>> {
        let raw = self.store.raw().await?;
        self.header(&raw.get(HEADER).cloned())?;
        let mut out = BTreeMap::new();
        for (key, value) in &raw {
            if let Some(id) = key.strip_prefix("overhead:") {
                field(id)?;
                let reservation: OverheadReservation = decode(value)?;
                reservation.validate()?;
                out.insert(id.into(), reservation);
            }
        }
        Ok(out)
    }
}
