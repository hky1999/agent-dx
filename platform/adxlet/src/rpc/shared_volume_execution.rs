//! Volume lifetime RPCs always require cluster acknowledgement.
use super::*;
use adx_core::shared_volume_attachments::{
    attachments_for_record, SharedVolumeAttachment, SharedVolumeAttachmentRetirement,
};
impl CoordinatorStateSink {
    pub(super) async fn reserve_volumes(&self, record: &EnvironmentRecord) -> Result<()> {
        let expected = attachments_for_record(record)?;
        if expected.is_empty() {
            return Ok(());
        }
        let mut request = Request::new(pb::ReserveSharedVolumesRequest {
            record: Some(record.clone().try_into()?),
            node_session_id: self.session_id.clone(),
        });
        adx_observability::trace::inject_metadata(request.metadata_mut());
        request.set_timeout(self.timeout);
        let mut client = self
            .volumes
            .read()
            .map_err(|_| Error::Unavailable("volume authority client unavailable".into()))?
            .clone();
        let result = tokio::time::timeout(self.timeout, client.reserve_shared_volumes(request))
            .await
            .map_err(|_| Error::Unavailable("volume admission timed out".into()))?
            .map_err(dependency_status)?
            .into_inner();
        let accepted: Vec<SharedVolumeAttachment> = result
            .attachments
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()?;
        if result.coordinator_epoch == 0 || accepted != expected {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub(super) async fn release_volumes(
        &self,
        receipts: &[SharedVolumeAttachmentRetirement],
    ) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        for receipt in receipts {
            receipt.validate()?;
        }
        let mut request = Request::new(pb::ReleaseSharedVolumesRequest {
            retirements: receipts.iter().cloned().map(Into::into).collect(),
            node_session_id: self.session_id.clone(),
        });
        adx_observability::trace::inject_metadata(request.metadata_mut());
        request.set_timeout(self.timeout);
        let mut client = self
            .volumes
            .read()
            .map_err(|_| Error::Unavailable("volume authority client unavailable".into()))?
            .clone();
        let result = tokio::time::timeout(self.timeout, client.release_shared_volumes(request))
            .await
            .map_err(|_| Error::Unavailable("volume retirement timed out".into()))?
            .map_err(dependency_status)?
            .into_inner();
        let accepted: Vec<SharedVolumeAttachmentRetirement> = result
            .retirements
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()?;
        if result.coordinator_epoch == 0 || accepted != receipts {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub(super) async fn list_volumes(&self, node_id: &str) -> Result<Vec<SharedVolumeAttachment>> {
        let mut request = Request::new(pb::ListNodeSharedVolumeAttachmentsRequest {
            node_id: node_id.into(),
            node_session_id: self.session_id.clone(),
        });
        adx_observability::trace::inject_metadata(request.metadata_mut());
        request.set_timeout(self.timeout);
        let mut client = self
            .volumes
            .read()
            .map_err(|_| Error::Unavailable("volume authority client unavailable".into()))?
            .clone();
        let result = tokio::time::timeout(
            self.timeout,
            client.list_node_shared_volume_attachments(request),
        )
        .await
        .map_err(|_| Error::Unavailable("volume reconciliation timed out".into()))?
        .map_err(dependency_status)?
        .into_inner();
        let accepted: Vec<SharedVolumeAttachment> = result
            .attachments
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()?;
        if result.coordinator_epoch == 0 || accepted.iter().any(|a| a.node_id != node_id) {
            return Err(Error::Conflict);
        }
        Ok(accepted)
    }
}
