//! Durable allocation closure. Ordering never replaces complete ownership checks.
use crate::{Assignment, EnvironmentSpec, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryIdentity {
    pub authority: String,
    pub environment_id: String,
    pub runtime_id: String,
    pub assignment_generation: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryProtection {
    pub assignment: Assignment,
    pub specification_sha256: String,
    pub minimum_runtime_revision: u64,
    pub environment_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryBoundary {
    pub authority: String,
    pub coordinator_epoch: u64,
    pub assignment_generation: u64,
    pub catalog_revision: u64,
    pub protected: BTreeMap<String, HistoryProtection>,
}
fn valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(|c| c.is_whitespace() || c.is_control())
}
pub fn specification_digest(spec: &EnvironmentSpec) -> Result<String> {
    let mut value = serde_json::to_value(spec).map_err(|e| Error::Invalid(e.to_string()))?;
    fn canonical(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(m) => {
                m.sort_keys();
                for v in m.values_mut() {
                    canonical(v)
                }
            }
            serde_json::Value::Array(a) => {
                for v in a {
                    canonical(v)
                }
            }
            _ => (),
        }
    }
    canonical(&mut value);
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).map_err(|e| Error::Invalid(e.to_string()))?)
    ))
}
impl HistoryIdentity {
    pub fn from_runtime(authority: String, runtime_id: String) -> Result<Self> {
        let base = runtime_id
            .rsplit_once("-r")
            .filter(|(_, suffix)| suffix.parse::<u64>().is_ok())
            .map_or(runtime_id.as_str(), |(base, _)| base);
        let (environment, generation) = base.rsplit_once('-').ok_or(Error::Conflict)?;
        let identity = Self {
            authority,
            environment_id: environment.into(),
            assignment_generation: generation.parse().map_err(|_| Error::Conflict)?,
            runtime_id,
        };
        identity.runtime_revision()?;
        Ok(identity)
    }
    pub fn runtime_revision(&self) -> Result<u64> {
        if !valid(&self.authority)
            || !valid(&self.environment_id)
            || self.assignment_generation == 0
        {
            return Err(Error::Conflict);
        }
        let base = format!("{}-{}", self.environment_id, self.assignment_generation);
        if self.runtime_id == base {
            return Ok(0);
        }
        let suffix = self
            .runtime_id
            .strip_prefix(&(base + "-r"))
            .ok_or(Error::Conflict)?;
        let revision = suffix.parse::<u64>().map_err(|_| Error::Conflict)?;
        if revision == 0 || revision.to_string() != suffix {
            return Err(Error::Conflict);
        };
        Ok(revision)
    }
    /// Stable UUIDv8 shared with sandboxd; all fields use unsigned big-endian lengths.
    pub fn operation_id(&self, fingerprint: &str) -> Result<String> {
        self.runtime_revision()?;
        if fingerprint.len() != 64
            || !fingerprint
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Error::Conflict);
        }
        let mut hash = Sha256::new();
        for value in [
            "sandboxd-history-start-v1",
            self.authority.as_str(),
            self.environment_id.as_str(),
            self.runtime_id.as_str(),
            &self.assignment_generation.to_string(),
            fingerprint,
        ] {
            hash.update(
                u64::try_from(value.len())
                    .map_err(|_| Error::Conflict)?
                    .to_be_bytes(),
            );
            hash.update(value.as_bytes());
        }
        let digest = hash.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",bytes[0],bytes[1],bytes[2],bytes[3],bytes[4],bytes[5],bytes[6],bytes[7],bytes[8],bytes[9],bytes[10],bytes[11],bytes[12],bytes[13],bytes[14],bytes[15]))
    }
}
impl HistoryBoundary {
    pub fn validate(&self) -> Result<()> {
        if !valid(&self.authority) || self.coordinator_epoch == 0 || self.protected.len() > 65536 {
            return Err(Error::Conflict);
        }
        for (id, p) in &self.protected {
            if id != &p.assignment.environment_id
                || !valid(id)
                || !valid(&p.assignment.node_id)
                || p.assignment.generation == 0
                || p.assignment.generation > self.assignment_generation
                || p.specification_sha256.len() != 64
                || !p
                    .specification_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
    pub fn check_advance(&self, next: &Self) -> Result<()> {
        self.validate()?;
        next.validate()?;
        if self.authority != next.authority
            || self.coordinator_epoch > next.coordinator_epoch
            || self.assignment_generation > next.assignment_generation
            || self.catalog_revision > next.catalog_revision
        {
            return Err(Error::Conflict);
        }
        if self.coordinator_epoch == next.coordinator_epoch
            && self.catalog_revision == next.catalog_revision
            && self != next
        {
            return Err(Error::Conflict);
        }
        for (id, p) in &next.protected {
            match self.protected.get(id) {
                Some(old)
                    if old.assignment.generation == p.assignment.generation
                        && (old.assignment != p.assignment
                            || old.environment_revision > p.environment_revision
                            || old.minimum_runtime_revision > p.minimum_runtime_revision
                            || (old.environment_revision == p.environment_revision
                                && old.specification_sha256 != p.specification_sha256)) =>
                {
                    return Err(Error::Conflict);
                }
                Some(old) if old.assignment.generation == p.assignment.generation => (),
                _ if p.assignment.generation <= self.assignment_generation => {
                    return Err(Error::Conflict)
                }
                _ => (),
            }
        }
        Ok(())
    }
    pub fn admit_assignment(&self, spec: &EnvironmentSpec, assignment: &Assignment) -> Result<()> {
        if assignment.environment_id != spec.id {
            return Err(Error::Conflict);
        }
        if assignment.generation > self.assignment_generation {
            return Ok(());
        }
        let p = self.protected.get(&spec.id).ok_or(Error::Conflict)?;
        if p.assignment != *assignment || p.specification_sha256 != specification_digest(spec)? {
            return Err(Error::Conflict);
        };
        Ok(())
    }
    pub fn is_closed(&self, identity: &HistoryIdentity) -> Result<bool> {
        let revision = identity.runtime_revision()?;
        if self.authority != identity.authority {
            return Err(Error::Conflict);
        }
        if identity.assignment_generation > self.assignment_generation {
            return Ok(false);
        }
        Ok(self
            .protected
            .get(&identity.environment_id)
            .filter(|p| p.assignment.generation == identity.assignment_generation)
            .is_none_or(|p| revision < p.minimum_runtime_revision))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn complete_identity_and_monotonic_fence() {
        let b = HistoryBoundary {
            authority: "test".into(),
            coordinator_epoch: 1,
            assignment_generation: 9,
            catalog_revision: 3,
            protected: BTreeMap::new(),
        };
        let old = HistoryIdentity::from_runtime("test".into(), "complex-id-7-r8".into()).unwrap();
        assert_eq!(old.runtime_revision().unwrap(), 8);
        assert!(b.is_closed(&old).unwrap());
        let mut changed = old.clone();
        changed.runtime_id = "foreign-7-r8".into();
        assert!(changed.runtime_revision().is_err());
        let mut next = b.clone();
        next.assignment_generation -= 1;
        assert!(b.check_advance(&next).is_err());
        let mut next = b.clone();
        next.authority = "foreign".into();
        assert!(b.check_advance(&next).is_err());
        let id = old.operation_id(&"a".repeat(64)).unwrap();
        assert_eq!(id.len(), 36);
        let vector =
            HistoryIdentity::from_runtime("unit-control".into(), "unit-env-7".into()).unwrap();
        assert_eq!(
            vector.operation_id(&"a".repeat(64)).unwrap(),
            "90ce0456-de69-803e-9644-bf02848f96c6"
        );
        changed = old.clone();
        changed.runtime_id = "complex-id-7-r9".into();
        assert_ne!(id, changed.operation_id(&"a".repeat(64)).unwrap());
    }
}
