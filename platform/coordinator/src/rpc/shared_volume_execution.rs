//! Node mTLS and current-session gates for durable application-volume lifetime.
use super::*;
use adx_core::shared_volume_attachments::{
    attachments_for_record, SharedVolumeAttachmentRetirement,
};

#[tonic::async_trait]
impl pb::shared_volume_execution_service_server::SharedVolumeExecutionService for CoordinatorRpc {
    async fn reserve_shared_volumes(
        &self,
        request: Request<pb::ReserveSharedVolumesRequest>,
    ) -> std::result::Result<Response<pb::SharedVolumeExecutionResponse>, Status> {
        let principal = self.0.peers.authenticate(&request)?;
        let request = request.into_inner();
        let record: EnvironmentRecord = request
            .record
            .ok_or_else(|| Status::invalid_argument("execution record required"))?
            .try_into()
            .map_err(status)?;
        let expected = attachments_for_record(&record).map_err(status)?;
        if expected.is_empty() {
            return Err(Status::invalid_argument("shared volume mounts required"));
        }
        if principal != Principal::Node(record.assignment.node_id.clone()) {
            return Err(Status::permission_denied("execution node required"));
        }
        let mut state = self.0.state.lock().await;
        state
            .checkpoint_reader_node(
                &record.assignment.node_id,
                &request.node_session_id,
                self.0.heartbeat_timeout,
                true,
            )
            .await
            .map_err(status)?;
        let attachments = state
            .session
            .reserve_shared_volumes(&record, &request.node_session_id)
            .await
            .map_err(status)?;
        if attachments != expected {
            return Err(Status::internal("volume admission identity mismatch"));
        }
        Ok(Response::new(pb::SharedVolumeExecutionResponse {
            attachments: attachments.into_iter().map(Into::into).collect(),
            coordinator_epoch: state.session.epoch(),
        }))
    }
    async fn release_shared_volumes(
        &self,
        request: Request<pb::ReleaseSharedVolumesRequest>,
    ) -> std::result::Result<Response<pb::SharedVolumeRetirementResponse>, Status> {
        let principal = self.0.peers.authenticate(&request)?;
        let request = request.into_inner();
        if request.retirements.is_empty()
            || request.retirements.len() > adx_core::execution::MAX_SHARED_VOLUME_MOUNTS
        {
            return Err(Status::invalid_argument(
                "bounded retirement batch required",
            ));
        }
        let receipts: Vec<SharedVolumeAttachmentRetirement> = request
            .retirements
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()
            .map_err(status)?;
        let node_id = &receipts[0].attachment.node_id;
        if principal != Principal::Node(node_id.clone()) {
            return Err(Status::permission_denied("execution node required"));
        }
        let mut state = self.0.state.lock().await;
        state
            .checkpoint_reader_node(
                node_id,
                &request.node_session_id,
                self.0.heartbeat_timeout,
                false,
            )
            .await
            .map_err(status)?;
        state
            .session
            .release_shared_volumes(&receipts, &request.node_session_id)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::SharedVolumeRetirementResponse {
            retirements: receipts.into_iter().map(Into::into).collect(),
            coordinator_epoch: state.session.epoch(),
        }))
    }
    async fn list_node_shared_volume_attachments(
        &self,
        request: Request<pb::ListNodeSharedVolumeAttachmentsRequest>,
    ) -> std::result::Result<Response<pb::SharedVolumeExecutionResponse>, Status> {
        let principal = self.0.peers.authenticate(&request)?;
        let request = request.into_inner();
        if principal != Principal::Node(request.node_id.clone()) {
            return Err(Status::permission_denied("execution node required"));
        }
        let mut state = self.0.state.lock().await;
        state
            .checkpoint_reader_node(
                &request.node_id,
                &request.node_session_id,
                self.0.heartbeat_timeout,
                false,
            )
            .await
            .map_err(status)?;
        let attachments = state
            .session
            .node_shared_volume_attachments(&request.node_id, &request.node_session_id)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::SharedVolumeExecutionResponse {
            attachments: attachments.into_iter().map(Into::into).collect(),
            coordinator_epoch: state.session.epoch(),
        }))
    }
}
