//! Strong stop proof for all mounts owned by one exact execution.
use super::*;
use adx_core::shared_volume_attachments::{
    SharedVolumeAttachment, SharedVolumeAttachmentRetirement,
};

impl Sandboxd {
    pub(super) async fn retire_volume_execution(
        &self,
        attachments: &[SharedVolumeAttachment],
    ) -> Result<Vec<SharedVolumeAttachmentRetirement>> {
        if attachments.is_empty() {
            return Ok(Vec::new());
        }
        if attachments.len() > adx_core::execution::MAX_SHARED_VOLUME_MOUNTS {
            return Err(Error::Invalid(
                "bounded shared volume execution required".into(),
            ));
        }
        let first = &attachments[0];
        let mut ids = std::collections::BTreeSet::new();
        for attachment in attachments {
            attachment.validate()?;
            if attachment.node_id != self.config.node_id
                || !attachment.same_execution(first)
                || attachment.tenant_id != first.tenant_id
                || !ids.insert(attachment.volume_id.as_str().to_owned())
            {
                return Err(Error::Conflict);
            }
        }
        // The existing durable adapter verifies its operation/backend identity,
        // fences delayed Start, and confirms this resource generation stopped.
        let stopped = self
            .retire_identified_start(&first.identity.runtime_id)
            .await?;
        self.retire_prepared_volume_mounts(&first.identity.runtime_id)
            .await?;
        let proof = serde_json::json!({
            "protocol": "adx-shared-volume-execution-retirement/v1",
            "node_id": self.config.node_id,
            "identity": first.identity,
            "operation_id": stopped.operation_id,
            "sandbox_id": stopped.sandbox_id,
            "resource_generation": stopped.resource_generation,
            "retirement_receipt": stopped.retirement_receipt,
        });
        let backend_receipt = serde_json::to_string(&proof).map_err(unavailable)?;
        let receipts: Vec<_> = attachments
            .iter()
            .map(|attachment| SharedVolumeAttachmentRetirement {
                attachment: attachment.clone(),
                backend_receipt: backend_receipt.clone(),
            })
            .collect();
        for receipt in &receipts {
            receipt.validate()?;
        }
        Ok(receipts)
    }
}
