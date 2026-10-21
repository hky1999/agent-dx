//! Existing volume authority plus backend stop proof, not a filesystem backend.
use crate::Services;
use adx_core::{
    shared_volume_attachments::{
        attachments_for_record, SharedVolumeAttachment, SharedVolumeAttachmentRetirement,
    },
    EnvironmentRecord, EnvironmentState, Error, Result,
};
use std::collections::BTreeMap;

pub(crate) async fn reserve(services: &Services, record: &EnvironmentRecord) -> Result<()> {
    if record.spec.execution.shared_volumes.is_empty() {
        return Ok(());
    }
    if !services.shared_volume_lifecycle {
        return Err(Error::Unavailable(
            "shared volume lifecycle is not configured".into(),
        ));
    }
    services.sink.reserve_shared_volumes(record).await
}

async fn retire_attachments(
    services: &Services,
    attachments: &[SharedVolumeAttachment],
) -> Result<()> {
    if attachments.is_empty() {
        return Ok(());
    }
    let receipts: Vec<SharedVolumeAttachmentRetirement> = services
        .runtime
        .retire_shared_volume_attachments(attachments)
        .await?;
    if receipts.len() != attachments.len() {
        return Err(Error::Conflict);
    }
    for (receipt, attachment) in receipts.iter().zip(attachments) {
        receipt.validate()?;
        if &receipt.attachment != attachment {
            return Err(Error::Conflict);
        }
    }
    services.sink.release_shared_volumes(&receipts).await
}

pub(crate) async fn retire(services: &Services, record: &EnvironmentRecord) -> Result<()> {
    let attachments = attachments_for_record(record)?;
    if attachments.is_empty() {
        return Ok(());
    }
    if !services.shared_volume_lifecycle {
        return Err(Error::Unavailable(
            "shared volume lifecycle is not configured".into(),
        ));
    }
    // Also fences an admission RPC that is still queued after a create timeout.
    retire_attachments(services, &attachments).await
}

pub(crate) async fn reconcile(
    services: &Services,
    node_id: &str,
    catalog: &BTreeMap<String, EnvironmentRecord>,
) -> Result<()> {
    if !services.shared_volume_lifecycle {
        if catalog
            .values()
            .any(|r| !r.spec.execution.shared_volumes.is_empty())
        {
            return Err(Error::Unavailable(
                "shared volume lifecycle is not configured".into(),
            ));
        }
        return Ok(());
    }
    let attachments = services
        .sink
        .list_shared_volume_attachments(node_id)
        .await?;
    let mut groups: BTreeMap<(String, u64, String), Vec<SharedVolumeAttachment>> = BTreeMap::new();
    for attachment in &attachments {
        attachment.validate()?;
        if attachment.node_id != node_id {
            return Err(Error::Conflict);
        }
        let key = (
            attachment.identity.environment_id.clone(),
            attachment.identity.ownership_generation,
            attachment.identity.runtime_id.clone(),
        );
        let group = groups.entry(key).or_default();
        if group
            .iter()
            .any(|old| old.volume_id == attachment.volume_id)
        {
            return Err(Error::Conflict);
        }
        group.push(attachment.clone());
    }
    // Validate all current identities before stopping any obsolete execution.
    for record in catalog.values() {
        let expected = attachments_for_record(record)?;
        let owned: Vec<_> = attachments
            .iter()
            .filter(|a| {
                a.identity.environment_id == record.spec.id
                    && a.identity.ownership_generation == record.assignment.generation
                    && a.identity.runtime_id == record.runtime.id
            })
            .collect();
        if owned.iter().any(|a| !expected.contains(a)) {
            return Err(Error::Conflict);
        }
        if record.state == EnvironmentState::Running
            && record.resources_held
            && expected.iter().any(|a| !owned.contains(&a))
        {
            return Err(Error::Unavailable(
                "running execution lacks durable shared volume occupancy".into(),
            ));
        }
    }
    for ((environment, generation, runtime), group) in groups {
        let retained = catalog.get(&environment).is_some_and(|r| {
            r.assignment.generation == generation && r.runtime.id == runtime && r.resources_held
        });
        if !retained {
            tokio::time::timeout(
                services.operation_timeout,
                retire_attachments(services, &group),
            )
            .await
            .map_err(|_| {
                Error::Unavailable("obsolete shared volume execution retirement timed out".into())
            })??;
        }
    }
    Ok(())
}
