//! Internal execution occupancy; never project these receipts over the public API.
use crate::control as pb;
use adx_core::{
    runtime::RuntimeIdentity,
    shared_volume_attachments::{SharedVolumeAttachment, SharedVolumeAttachmentRetirement},
    Error, Result,
};
impl From<SharedVolumeAttachment> for pb::SharedVolumeAttachment {
    fn from(value: SharedVolumeAttachment) -> Self {
        Self {
            mount: Some(value.mount().into()),
            tenant_id: value.tenant_id,
            node_id: value.node_id,
            environment_id: value.identity.environment_id,
            runtime_id: value.identity.runtime_id,
            ownership_generation: value.identity.ownership_generation,
        }
    }
}
impl TryFrom<pb::SharedVolumeAttachment> for SharedVolumeAttachment {
    type Error = Error;
    fn try_from(value: pb::SharedVolumeAttachment) -> Result<Self> {
        let mount: adx_core::shared_volumes::SharedVolumeMount = value
            .mount
            .ok_or_else(|| Error::Invalid("shared volume mount required".into()))?
            .try_into()?;
        let attachment = Self {
            volume_id: mount.volume_id,
            subpath: mount.subpath,
            target: mount.target,
            access: mount.access,
            tenant_id: value.tenant_id,
            node_id: value.node_id,
            identity: RuntimeIdentity {
                environment_id: value.environment_id,
                runtime_id: value.runtime_id,
                ownership_generation: value.ownership_generation,
            },
        };
        attachment.validate()?;
        Ok(attachment)
    }
}
impl From<SharedVolumeAttachmentRetirement> for pb::SharedVolumeRetirement {
    fn from(value: SharedVolumeAttachmentRetirement) -> Self {
        Self {
            attachment: Some(value.attachment.into()),
            backend_receipt: value.backend_receipt,
        }
    }
}
impl TryFrom<pb::SharedVolumeRetirement> for SharedVolumeAttachmentRetirement {
    type Error = Error;
    fn try_from(value: pb::SharedVolumeRetirement) -> Result<Self> {
        let receipt = Self {
            attachment: value
                .attachment
                .ok_or_else(|| Error::Invalid("shared volume attachment required".into()))?
                .try_into()?,
            backend_receipt: value.backend_receipt,
        };
        receipt.validate()?;
        Ok(receipt)
    }
}
