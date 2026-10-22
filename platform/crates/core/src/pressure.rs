//! Read-only, short-lived host admission observations. These never authorize
//! physical resource changes or release existing reservations.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Operator-pinned identity, kept outside the Redis and node backup sets.
/// A new boot or replacement storage requires isolated recovery and a new pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PressureIdentity {
    pub host_id: String,
    pub authority: String,
    pub boot_id: String,
    pub parent_device: u64,
    pub parent_inode: u64,
    pub storage_identity_sha256: String,
}
impl PressureIdentity {
    pub fn validate(&self) -> Result<()> {
        if [&self.host_id, &self.authority, &self.boot_id]
            .iter()
            .any(|s| s.is_empty() || s.trim() != s.as_str() || s.len() > 128)
            || self.parent_inode == 0
            || self.storage_identity_sha256.len() != 64
            || !self
                .storage_identity_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(Error::Invalid("invalid pressure authority identity".into()));
        }
        Ok(())
    }
}

/// Metrics are evidence, never independently an authorization to resize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PressureMetrics {
    pub host_memory_per_mille: u16,
    pub parent_memory_per_mille: u16,
    pub host_memory_psi_per_mille: u16,
    pub parent_memory_psi_per_mille: u16,
    pub disk_used_per_mille: u16,
    pub parent_memory_current_bytes: u64,
    pub parent_memory_max_bytes: u64,
    pub disk_available_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PressureObservation {
    pub schema: u32,
    pub identity: PressureIdentity,
    pub sequence: u64,
    pub observed_unix_millis: u64,
    pub valid_until_unix_millis: u64,
    pub accepting: bool,
    pub reason: String,
    pub metrics: PressureMetrics,
}

/// The configured file is published by one trusted host observer and mounted
/// read-only into all member nodes and the Coordinator at this same path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PressureSource {
    pub path: PathBuf,
    pub identity: PressureIdentity,
    pub max_age_millis: u64,
}
impl PressureSource {
    pub fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if !self.path.is_absolute() || !(100..=60000).contains(&self.max_age_millis) {
            return Err(Error::Invalid(
                "absolute pressure path and bounded age required".into(),
            ));
        }
        Ok(())
    }

    /// Read and validate at the point of admission. Missing, expired, closed,
    /// replaced, oversized and malformed observations all return NoCapacity.
    /// Existing holds must be reconciled independently of this result.
    pub fn check(&self, now_unix_millis: u64) -> Result<Duration> {
        self.validate()?;
        // This file-based contract is for consumers on the same Linux host.
        // Check the actual boot independently of any still-unexpired old file.
        if std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .ok()
            .is_none_or(|boot| boot.trim() != self.identity.boot_id)
        {
            return Err(Error::NoCapacity);
        }
        let read = || -> Option<PressureObservation> {
            let before = std::fs::symlink_metadata(&self.path).ok()?;
            if !before.is_file() || before.len() > 8192 {
                return None;
            }
            let file = File::open(&self.path).ok()?;
            let opened = file.metadata().ok()?;
            if !opened.is_file() || opened.len() != before.len() {
                return None;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if (before.dev(), before.ino()) != (opened.dev(), opened.ino()) {
                    return None;
                }
            }
            let mut bytes = Vec::new();
            file.take(8193).read_to_end(&mut bytes).ok()?;
            if bytes.len() > 8192 {
                return None;
            }
            serde_json::from_slice(&bytes).ok()
        };
        let observation = read().ok_or(Error::NoCapacity)?;
        let m = &observation.metrics;
        if observation.schema != 1
            || observation.identity != self.identity
            || observation.sequence == 0
            || observation.observed_unix_millis > now_unix_millis
            || now_unix_millis >= observation.valid_until_unix_millis
            || observation.valid_until_unix_millis <= observation.observed_unix_millis
            || observation.valid_until_unix_millis - observation.observed_unix_millis
                > self.max_age_millis
            || !observation.accepting
            || observation.reason != "low_watermark"
            || [
                m.host_memory_per_mille,
                m.parent_memory_per_mille,
                m.host_memory_psi_per_mille,
                m.parent_memory_psi_per_mille,
                m.disk_used_per_mille,
            ]
            .iter()
            .any(|v| *v > 1000)
            || m.parent_memory_max_bytes == 0
            || m.parent_memory_current_bytes > m.parent_memory_max_bytes
        {
            return Err(Error::NoCapacity);
        }
        Ok(Duration::from_millis(
            observation.valid_until_unix_millis - now_unix_millis,
        ))
    }

    pub fn check_now(&self) -> Result<Duration> {
        let started = std::time::Instant::now();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::NoCapacity)?
            .as_millis();
        let lease = self.check(u64::try_from(now).map_err(|_| Error::NoCapacity)?)?;
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::NoCapacity)?
            .as_millis();
        if after < now || after - now >= lease.as_millis() {
            return Err(Error::NoCapacity);
        }
        lease
            .checked_sub(started.elapsed())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(Error::NoCapacity)
    }
}
