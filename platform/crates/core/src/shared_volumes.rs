//! Controlled application-volume identities and permission checks.
//!
//! These contracts do not resolve host paths, mount filesystems, or grant a
//! live writer lease. Coordinator storage must persist permissions and reserve
//! the single writer atomically before adxlet performs any mount side effect.
//! Restore must check current permissions again; a saved mount is not a grant.
//! Application-volume contents are external to private checkpoint rollback.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Bound the size of one authoritative volume permission record.
pub const MAX_SHARED_VOLUME_GRANTS: usize = 256;
const MAX_TENANT_BYTES: usize = 256;
const MAX_PATH_BYTES: usize = 4096;

/// An opaque, canonical, non-nil UUID allocated by the control plane.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SharedVolumeId(String);

impl SharedVolumeId {
    /// Reject noncanonical UUIDs, paths and the nil UUID.
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        value.into().try_into()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SharedVolumeId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&value)
            .map_err(|_| Error::Invalid("invalid shared volume ID".into()))?;
        if id.is_nil() || id.to_string() != value {
            return Err(Error::Invalid(
                "shared volume ID must be a canonical non-nil UUID".into(),
            ));
        }
        Ok(Self(value))
    }
}

impl From<SharedVolumeId> for String {
    fn from(value: SharedVolumeId) -> Self {
        value.0
    }
}

/// A normalized path relative to a controlled volume; empty means its root.
///
/// Lexical validation is not a symlink or mount-identity check. The node must
/// resolve this beneath the verified volume root without following escapes.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SharedVolumeSubpath(String);

impl SharedVolumeSubpath {
    /// Reject absolute paths, traversal, ambiguous separators and controls.
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        value.into().try_into()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn contains(&self, other: &Self) -> bool {
        self.0.is_empty()
            || self.0 == other.0
            || other
                .0
                .strip_prefix(&self.0)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

impl TryFrom<String> for SharedVolumeSubpath {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        if value.len() > MAX_PATH_BYTES || (!value.is_empty() && !normal_components(&value)) {
            return Err(Error::Invalid("invalid shared volume subpath".into()));
        }
        Ok(Self(value))
    }
}

impl From<SharedVolumeSubpath> for String {
    fn from(value: SharedVolumeSubpath) -> Self {
        value.0
    }
}

/// Permission ceiling, not a distributed writer reservation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedVolumeAccess {
    #[default]
    ReadOnly,
    ReadWrite,
}

impl SharedVolumeAccess {
    pub fn permits(self, requested: Self) -> bool {
        self == Self::ReadWrite || requested == Self::ReadOnly
    }
}

/// A controlled application mount; it cannot carry an arbitrary host path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedVolumeMount {
    pub volume_id: SharedVolumeId,
    #[serde(default)]
    pub subpath: SharedVolumeSubpath,
    pub target: String,
    #[serde(default)]
    pub access: SharedVolumeAccess,
}

impl SharedVolumeMount {
    /// Reject noncanonical guest paths and runtime-owned mount destinations.
    pub fn validate(&self) -> Result<()> {
        let components = self.target.strip_prefix('/').unwrap_or("");
        if self.target.len() > MAX_PATH_BYTES
            || !normal_components(components)
            || ["/proc", "/sys", "/dev", "/__adx"].iter().any(|reserved| {
                self.target == *reserved
                    || self
                        .target
                        .strip_prefix(reserved)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            })
        {
            return Err(Error::Invalid("invalid shared volume mount target".into()));
        }
        Ok(())
    }
}

/// Permission for one tenant and one subtree, including descendants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedVolumeGrant {
    pub tenant_id: String,
    #[serde(default)]
    pub subpath: SharedVolumeSubpath,
    pub access: SharedVolumeAccess,
}

/// Versioned permission state owned by Coordinator, not a new checkpoint catalog.
///
/// This pure model does not persist itself or admit a live writer. Storage must
/// compare the previous revision atomically when committing changes and writer
/// reservations. A successful permission check alone never authorizes removal,
/// a mount side effect, or reuse of a cached decision after restart/restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SharedVolumeRecordFields")]
pub struct SharedVolumeRecord {
    id: SharedVolumeId,
    owner_tenant_id: String,
    revision: u64,
    grants: Vec<SharedVolumeGrant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedVolumeRecordFields {
    id: SharedVolumeId,
    owner_tenant_id: String,
    revision: u64,
    grants: Vec<SharedVolumeGrant>,
}

impl TryFrom<SharedVolumeRecordFields> for SharedVolumeRecord {
    type Error = Error;

    fn try_from(fields: SharedVolumeRecordFields) -> Result<Self> {
        let record = Self {
            id: fields.id,
            owner_tenant_id: fields.owner_tenant_id,
            revision: fields.revision,
            grants: fields.grants,
        };
        record.validate()?;
        Ok(record)
    }
}

impl SharedVolumeRecord {
    /// Create an owner-controlled record; the owner has full subtree permission.
    pub fn new(id: SharedVolumeId, owner_tenant_id: String) -> Result<Self> {
        valid_tenant(&owner_tenant_id)?;
        Ok(Self {
            id,
            owner_tenant_id,
            revision: 1,
            grants: Vec::new(),
        })
    }

    pub fn id(&self) -> &SharedVolumeId {
        &self.id
    }

    pub fn owner_tenant_id(&self) -> &str {
        &self.owner_tenant_id
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn grants(&self) -> &[SharedVolumeGrant] {
        &self.grants
    }

    /// Reject corrupt, duplicate, oversized or unversioned permission state.
    pub fn validate(&self) -> Result<()> {
        valid_tenant(&self.owner_tenant_id)?;
        if self.revision == 0 || self.grants.len() > MAX_SHARED_VOLUME_GRANTS {
            return Err(Error::Invalid(
                "invalid shared volume permission record".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for grant in &self.grants {
            valid_tenant(&grant.tenant_id)?;
            if grant.tenant_id == self.owner_tenant_id
                || !seen.insert((grant.tenant_id.as_str(), grant.subpath.as_str()))
            {
                return Err(Error::Invalid("invalid shared volume grant".into()));
            }
        }
        Ok(())
    }

    /// Check current permission, not writer exclusivity or mount admission.
    ///
    /// Returns `NotFound` for a different volume or an unauthorized tenant/mode/
    /// subtree, without disclosing another tenant's volume. Malformed mount
    /// paths and tenant identities return `Invalid`.
    pub fn check_mount_permission(&self, tenant_id: &str, mount: &SharedVolumeMount) -> Result<()> {
        self.validate()?;
        valid_tenant(tenant_id)?;
        mount.validate()?;
        if mount.volume_id != self.id {
            return Err(Error::NotFound);
        }
        if tenant_id == self.owner_tenant_id
            || self.grants.iter().any(|grant| {
                grant.tenant_id == tenant_id
                    && grant.access.permits(mount.access)
                    && grant.subpath.contains(&mount.subpath)
            })
        {
            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    /// Owner-only, idempotent permission update; returns whether state changed.
    ///
    /// Returns `NotFound` for nonowners, `Invalid` for malformed/oversized grants,
    /// and `Conflict` on revision exhaustion. Errors do not partially mutate.
    pub fn grant(&mut self, caller_tenant_id: &str, grant: SharedVolumeGrant) -> Result<bool> {
        self.validate()?;
        self.require_owner(caller_tenant_id)?;
        valid_tenant(&grant.tenant_id)?;
        if grant.tenant_id == self.owner_tenant_id {
            return Err(Error::Invalid("owner permission is implicit".into()));
        }
        let index = self
            .grants
            .iter()
            .position(|old| old.tenant_id == grant.tenant_id && old.subpath == grant.subpath);
        if index.is_some_and(|index| self.grants[index] == grant) {
            return Ok(false);
        }
        if index.is_none() && self.grants.len() >= MAX_SHARED_VOLUME_GRANTS {
            return Err(Error::Invalid("too many shared volume grants".into()));
        }
        let revision = self.next_revision()?;
        if let Some(index) = index {
            self.grants[index] = grant;
        } else {
            self.grants.push(grant);
        }
        self.revision = revision;
        Ok(true)
    }

    /// Owner-only revocation of one exact tenant/subtree grant.
    ///
    /// New and restored mounts must recheck current permission. Revocation is
    /// not a stop receipt and does not prove existing processes released I/O.
    /// Returns `NotFound` for nonowners and `Conflict` on revision exhaustion.
    pub fn revoke(
        &mut self,
        caller_tenant_id: &str,
        tenant_id: &str,
        subpath: &SharedVolumeSubpath,
    ) -> Result<bool> {
        self.validate()?;
        self.require_owner(caller_tenant_id)?;
        valid_tenant(tenant_id)?;
        let Some(index) = self
            .grants
            .iter()
            .position(|grant| grant.tenant_id == tenant_id && &grant.subpath == subpath)
        else {
            return Ok(false);
        };
        let revision = self.next_revision()?;
        self.grants.remove(index);
        self.revision = revision;
        Ok(true)
    }

    fn require_owner(&self, tenant_id: &str) -> Result<()> {
        valid_tenant(tenant_id)?;
        if tenant_id == self.owner_tenant_id {
            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    fn next_revision(&self) -> Result<u64> {
        self.revision.checked_add(1).ok_or(Error::Conflict)
    }
}

pub(crate) fn valid_tenant(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_TENANT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(Error::Invalid("invalid shared volume tenant".into()));
    }
    Ok(())
}

fn normal_components(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('\\')
        && !value.chars().any(char::is_control)
        && value
            .split('/')
            .all(|part| !matches!(part, "" | "." | ".."))
}
