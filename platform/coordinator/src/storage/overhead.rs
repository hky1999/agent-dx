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

/// Permanent storage fence for one original, undispatched reservation intent.
/// The caller separately attests that no physical effect can still be created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverheadWithdrawalFence {
    /// A complete retired row prevents a late exact reserve replay.
    RetiredReservation,
    /// The current bounded table cannot shrink without advancing its generation.
    FullHistoryGeneration,
    /// This strictly older identity generation can never be admitted again.
    PastHistoryGeneration,
}

/// Retain this receipt in the original caller journal before settling that intent.
/// A history fence proves permanent admission refusal, not archived ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverheadWithdrawal {
    pub reservation_id: String,
    pub reservation: OverheadReservation,
    pub retirement_proof_sha256: String,
    pub epoch: u64,
    pub history_generation: Option<u64>,
    pub observed_revision: u64,
    pub fence: OverheadWithdrawalFence,
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
            super::overhead_history::check_new_identity(&header, id)?;
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

    /// Close an original reservation after independently fencing its producer and
    /// proving no physical effect exists. A missing row alone is never that proof.
    ///
    /// Unlike ordinary retirement, an absent admissible ID gets a retired row,
    /// so a late reserve cannot commit after a lost reply. A full current history
    /// generation or an older identity generation is already a permanent fence:
    /// rows can only be removed by advancing history, which rejects the old ID.
    ///
    /// # Errors
    /// Invalid values, mismatched complete intents or proofs, unknown hosts,
    /// pending execution peak links and stale epochs are rejected. Before the
    /// first history generation, a full table returns NoCapacity; a future
    /// prefixed ID could otherwise become admissible after a later archive.
    /// Unavailable storage or uncertain CAS outcomes require retaining all holds.
    pub async fn withdraw_overhead_before_effect(
        &self,
        id: &str,
        reservation: OverheadReservation,
        proof_sha256: &str,
    ) -> Result<OverheadWithdrawal> {
        reservation.validate()?;
        digest(proof_sha256)?;
        let key = field(id)?;
        if reservation.retirement_proof_sha256.is_some() {
            return Err(Error::Conflict);
        }
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if header
                .host_budgets
                .as_ref()
                .is_none_or(|b| !b.contains_key(&reservation.host_id))
            {
                return Err(Error::Conflict);
            }
            snapshot(&raw)?;
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
            let receipt = |fence, revision| OverheadWithdrawal {
                reservation_id: id.to_owned(),
                reservation: reservation.clone(),
                retirement_proof_sha256: proof_sha256.to_owned(),
                epoch: self.epoch(),
                history_generation: header.overhead_history.as_ref().map(|h| h.generation),
                observed_revision: revision,
                fence,
            };
            if let Some(value) = raw.get(&key) {
                let mut old: OverheadReservation = decode(value)?;
                old.validate()?;
                let previous_proof = old.retirement_proof_sha256.take();
                if old != reservation {
                    return Err(Error::Conflict);
                }
                if let Some(previous) = previous_proof {
                    return if previous == proof_sha256 {
                        Ok(receipt(
                            OverheadWithdrawalFence::RetiredReservation,
                            header.revision,
                        ))
                    } else {
                        Err(Error::Conflict)
                    };
                }
            } else {
                if let Some(history) = &header.overhead_history {
                    if super::overhead_history::identity_generation(id)?
                        .is_some_and(|generation| generation < history.generation)
                    {
                        return Ok(receipt(
                            OverheadWithdrawalFence::PastHistoryGeneration,
                            header.revision,
                        ));
                    }
                }
                super::overhead_history::check_new_identity(&header, id)?;
                if raw.keys().filter(|k| k.starts_with("overhead:")).count() >= 512 {
                    if header.overhead_history.is_none() {
                        return Err(Error::NoCapacity);
                    }
                    return Ok(receipt(
                        OverheadWithdrawalFence::FullHistoryGeneration,
                        header.revision,
                    ));
                }
            }
            let mut closed = reservation.clone();
            closed.retirement_proof_sha256 = Some(proof_sha256.to_owned());
            // Build the receipt before mutating the header borrowed by the closure.
            let next_revision = header.revision.checked_add(1).ok_or(Error::Conflict)?;
            let result = receipt(OverheadWithdrawalFence::RetiredReservation, next_revision);
            header.overhead_budget = true;
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&key, encode(&closed)?)),
                )
                .await?
            {
                return Ok(result);
            }
        }
        Err(Error::Unavailable(
            "concurrent overhead withdrawal; retry".into(),
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
