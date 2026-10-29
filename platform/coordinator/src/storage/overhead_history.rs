//! Bounded hot overhead history with externally accepted, immutable receipts.
//! Allocation generations permanently fence archived IDs without an unbounded set.
use super::overhead::{digest, field};
use super::*;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverheadHistory {
    /// Only new IDs with `oh-{generation}-` may be admitted.
    pub generation: u64,
    pub retired_count: u64,
    pub protected_ids: BTreeSet<String>,
    pub last_archive_sha256: String,
    pub acceptance_sha256: String,
}

impl OverheadHistory {
    pub(super) fn validate(&self) -> Result<()> {
        if self.generation == 0 || self.retired_count == 0 {
            return Err(Error::Conflict);
        }
        if self.protected_ids.len() > 512 {
            return Err(Error::Conflict);
        }
        for id in &self.protected_ids {
            field(id)?;
        }
        digest(&self.last_archive_sha256)?;
        digest(&self.acceptance_sha256)
    }
}

/// Complete retired ownership records. Persist these canonical bytes and a funded
/// receiver acceptance before committing. The hash chain never expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverheadArchive {
    pub format: u32,
    pub authority: String,
    pub source_epoch: u64,
    pub generation: u64,
    pub previous: Option<OverheadHistory>,
    pub protected_ids: BTreeSet<String>,
    pub reservations: BTreeMap<String, OverheadReservation>,
}

impl OverheadArchive {
    fn validate(&self) -> Result<()> {
        if self.format != 1
            || self.source_epoch == 0
            || self.generation == 0
            || self.reservations.is_empty()
            || self.reservations.len() > 512
        {
            return Err(Error::Conflict);
        }
        if let Some(previous) = &self.previous {
            previous.validate()?;
            if previous.generation.checked_add(1) != Some(self.generation)
                || !previous.protected_ids.is_subset(&self.protected_ids)
            {
                return Err(Error::Conflict);
            }
        }
        if self.protected_ids.len() > 512 {
            return Err(Error::Conflict);
        }
        for id in &self.protected_ids {
            field(id)?;
            if self.reservations.contains_key(id) {
                return Err(Error::Conflict);
            }
        }
        for (id, reservation) in &self.reservations {
            field(id)?;
            reservation.validate()?;
            if reservation.retirement_proof_sha256.is_none() {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }

    /// Canonical UTF-8 JSON, with recursively sorted keys and no newline.
    /// Errors reject malformed receipts; callers must persist the complete bytes.
    pub fn canonical_json(&self) -> Result<String> {
        self.validate()?;
        let mut value = serde_json::to_value(self).map_err(|e| Error::Invalid(e.to_string()))?;
        fn canonical(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Object(map) => {
                    map.sort_keys();
                    for item in map.values_mut() {
                        canonical(item);
                    }
                }
                serde_json::Value::Array(items) => {
                    for item in items {
                        canonical(item);
                    }
                }
                _ => (),
            }
        }
        canonical(&mut value);
        encode(&value)
    }

    pub fn sha256(&self) -> Result<String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(self.canonical_json()?.as_bytes())
        ))
    }
}

pub(super) fn identity_generation(id: &str) -> Result<Option<u64>> {
    let Some((generation, suffix)) = id.strip_prefix("oh-").and_then(|s| s.split_once('-')) else {
        return Ok(None);
    };
    if suffix.is_empty()
        || generation.starts_with('0')
        || generation.is_empty()
        || !generation.bytes().all(|b| b.is_ascii_digit())
    {
        return Ok(None);
    }
    generation.parse().map(Some).map_err(|_| Error::Conflict)
}

pub(super) fn check_new_identity(header: &Header, id: &str) -> Result<()> {
    field(id)?;
    if let Some(history) = &header.overhead_history {
        history.validate()?;
        if identity_generation(id)? != Some(history.generation) {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

fn next_generation(raw: &BTreeMap<String, String>, header: &Header) -> Result<u64> {
    let highest = if let Some(history) = &header.overhead_history {
        history.generation
    } else {
        let mut highest = 0;
        for key in raw.keys() {
            if let Some(id) = key.strip_prefix("overhead:") {
                if let Some(generation) = identity_generation(id)? {
                    highest = highest.max(generation);
                }
            }
        }
        highest
    };
    highest.checked_add(1).ok_or(Error::Conflict)
}

impl Session {
    pub async fn overhead_history(&self) -> Result<Option<OverheadHistory>> {
        let raw = self.store.raw().await?;
        Ok(self.header(&raw.get(HEADER).cloned())?.overhead_history)
    }

    /// Read only. No field is removed and no admission generation is advanced.
    pub async fn prepare_overhead_archive(
        &self,
        protected_ids: &BTreeSet<String>,
    ) -> Result<OverheadArchive> {
        let raw = self.store.raw().await?;
        let header = self.header(&raw.get(HEADER).cloned())?;
        snapshot(&raw)?;
        let protected_ids: BTreeSet<_> = protected_ids
            .iter()
            .cloned()
            .chain(
                header
                    .overhead_history
                    .iter()
                    .flat_map(|h| h.protected_ids.iter().cloned()),
            )
            .collect();
        for id in &protected_ids {
            if !raw.contains_key(&field(id)?) {
                return Err(Error::Conflict);
            }
        }
        let mut reservations = BTreeMap::new();
        for (key, value) in &raw {
            if let Some(id) = key.strip_prefix("overhead:") {
                let row: OverheadReservation = decode(value)?;
                if row.retirement_proof_sha256.is_some() && !protected_ids.contains(id) {
                    reservations.insert(id.to_owned(), row);
                }
            }
        }
        let archive = OverheadArchive {
            format: 1,
            authority: self.store.key.clone(),
            source_epoch: self.epoch(),
            generation: next_generation(&raw, &header)?,
            previous: header.overhead_history,
            protected_ids,
            reservations,
        };
        archive.validate()?;
        Ok(archive)
    }

    /// The trusted host controller attests a complete, fsynced archive and its
    /// physical storage ownership. Unknown acceptance never permits removal.
    /// Active rows and execution peak links are preserved. An exact lost reply
    /// replays the latest accepted receipt, including after a Coordinator restart.
    pub async fn archive_overhead(
        &self,
        archive: OverheadArchive,
        acceptance_sha256: &str,
    ) -> Result<OverheadHistory> {
        archive.validate()?;
        digest(acceptance_sha256)?;
        if archive.authority != self.store.key {
            return Err(Error::Conflict);
        }
        let archive_sha256 = archive.sha256()?;
        for _ in 0..ATTEMPTS {
            let mut raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if let Some(history) = &header.overhead_history {
                if history.last_archive_sha256 == archive_sha256 {
                    return if history.acceptance_sha256 == acceptance_sha256 {
                        Ok(history.clone())
                    } else {
                        Err(Error::Conflict)
                    };
                }
            }
            if archive.source_epoch != self.epoch()
                || archive.previous != header.overhead_history
                || archive.generation != next_generation(&raw, &header)?
            {
                return Err(Error::Conflict);
            }
            snapshot(&raw)?;
            for id in &archive.protected_ids {
                if !raw.contains_key(&field(id)?) {
                    return Err(Error::Conflict);
                }
            }
            let mut retired = Vec::new();
            for (id, row) in &archive.reservations {
                let key = field(id)?;
                let old: OverheadReservation = decode(raw.get(&key).ok_or(Error::Conflict)?)?;
                if old != *row {
                    return Err(Error::Conflict);
                }
                retired.push(key);
            }
            for key in &retired {
                raw.remove(key);
            }
            let count = u64::try_from(retired.len()).map_err(|_| Error::Conflict)?;
            let history = OverheadHistory {
                generation: archive.generation,
                protected_ids: archive.protected_ids.clone(),
                retired_count: header
                    .overhead_history
                    .as_ref()
                    .map_or(0, |h| h.retired_count)
                    .checked_add(count)
                    .ok_or(Error::Conflict)?,
                last_archive_sha256: archive_sha256.clone(),
                acceptance_sha256: acceptance_sha256.to_owned(),
            };
            history.validate()?;
            header.overhead_history = Some(history.clone());
            header.advance()?;
            budget::check_holds(&raw, &header, None)?;
            let mut cmd = redis::cmd("EVAL");
            cmd.arg(include_str!("overhead_archive.lua"))
                .arg(1)
                .arg(&self.store.key)
                .arg(raw.get(HEADER).ok_or(Error::Conflict)?)
                .arg(encode(&header)?)
                .arg(retired.len())
                .arg(&retired);
            let applied: u8 = self.store.query(cmd).await?;
            if applied == 1 {
                self.store.notify_committed_change(header.revision, retired);
                return Ok(history);
            }
            if applied != 0 {
                return Err(Error::Unavailable("invalid overhead archive result".into()));
            }
        }
        Err(Error::Unavailable(
            "concurrent overhead archival; retry".into(),
        ))
    }
}
