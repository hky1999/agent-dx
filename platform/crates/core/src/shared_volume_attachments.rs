//! Execution-owned application-volume reservations, separate from checkpoints.
use crate::EnvironmentRecord;
use crate::{
    runtime::RuntimeIdentity,
    shared_volumes::{
        valid_tenant, SharedVolumeAccess, SharedVolumeId, SharedVolumeMount, SharedVolumeSubpath,
    },
    Error, Result,
};
use serde::{Deserialize, Serialize};

/// Every stored field is required: an omitted RW mode must never become RO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedVolumeAttachment {
    pub volume_id: SharedVolumeId,
    pub subpath: SharedVolumeSubpath,
    pub target: String,
    pub access: SharedVolumeAccess,
    pub tenant_id: String,
    pub node_id: String,
    pub identity: RuntimeIdentity,
}

/// Derive all execution-owned mounts from the canonical Environment spec.
/// This is structural identity validation, not permission or liveness proof.
pub fn attachments_for_record(record: &EnvironmentRecord) -> Result<Vec<SharedVolumeAttachment>> {
    record.spec.execution.validate_shared_volumes()?;
    if record.assignment.environment_id != record.spec.id {
        return Err(Error::Conflict);
    }
    let attachments: Vec<_> = record
        .spec
        .execution
        .shared_volumes
        .iter()
        .map(|mount| SharedVolumeAttachment {
            volume_id: mount.volume_id.clone(),
            subpath: mount.subpath.clone(),
            target: mount.target.clone(),
            access: mount.access,
            tenant_id: record.spec.tenant_id.clone(),
            node_id: record.assignment.node_id.clone(),
            identity: RuntimeIdentity {
                environment_id: record.spec.id.clone(),
                runtime_id: record.runtime.id.clone(),
                ownership_generation: record.assignment.generation,
            },
        })
        .collect();
    for attachment in &attachments {
        attachment.validate()?;
    }
    Ok(attachments)
}

impl SharedVolumeAttachment {
    pub fn mount(&self) -> SharedVolumeMount {
        SharedVolumeMount {
            volume_id: self.volume_id.clone(),
            subpath: self.subpath.clone(),
            target: self.target.clone(),
            access: self.access,
        }
    }

    /// Validate identity and paths, not actual runtime liveness or permissions.
    pub fn validate(&self) -> Result<()> {
        valid_tenant(&self.tenant_id)?;
        self.identity.validate()?;
        self.mount().validate()?;
        if self.node_id.trim().is_empty()
            || self.node_id.trim() != self.node_id
            || self.node_id.len() > 256
            || self.node_id.chars().any(char::is_control)
            || !crate::valid_runtime_id(
                &self.identity.environment_id,
                self.identity.ownership_generation,
                &self.identity.runtime_id,
            )
        {
            return Err(Error::Invalid("invalid shared volume execution".into()));
        }
        Ok(())
    }

    pub fn same_execution(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.identity == other.identity
    }
}

/// A trusted node must obtain an execution-bound strong backend stop receipt.
///
/// As with CheckpointReaderRetirement, this validator checks receipt syntax.
/// The authenticated runtime adapter must verify confirmed exit and fencing of
/// delayed starts; arbitrary callers cannot manufacture this proof over an API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedVolumeAttachmentRetirement {
    pub attachment: SharedVolumeAttachment,
    pub backend_receipt: String,
}

impl SharedVolumeAttachmentRetirement {
    pub fn validate(&self) -> Result<()> {
        self.attachment.validate()?;
        if self.backend_receipt.is_empty()
            || self.backend_receipt.len() > 4096
            || self.backend_receipt.chars().any(char::is_control)
        {
            return Err(Error::Invalid("invalid volume retirement receipt".into()));
        }
        Ok(())
    }
}
