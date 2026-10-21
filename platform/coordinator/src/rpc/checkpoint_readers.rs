//! Authenticated reader lifetime operations on the existing Environment authority.
use super::*;

impl State {
    pub(super) async fn checkpoint_reader_node(
        &mut self,
        node_id: &str,
        session_id: &str,
        heartbeat_timeout: Duration,
        starting: bool,
    ) -> Result<()> {
        self.recover_authoritative_state().await?;
        self.healthy()?;
        if self.overdue(node_id, heartbeat_timeout) {
            self.invalidate_node(node_id).await?;
        }
        let live = self.live.get(node_id).ok_or(Error::Conflict)?;
        if session_id.is_empty()
            || live.expired
            || live.session != session_id
            || (starting && live.report.reconciling)
            || self
                .nodes
                .get(node_id)
                .and_then(|node| node.session.as_ref())
                .is_none_or(|session| session.id != session_id)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

impl CoordinatorRpc {
    pub(super) async fn reserve_checkpoint_reader_rpc(
        &self,
        request: Request<pb::ReserveCheckpointReaderRequest>,
    ) -> std::result::Result<Response<pb::ReserveCheckpointReaderResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.reserve_checkpoint_reader", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let request = request.into_inner();
                let record: EnvironmentRecord = request
                    .record
                    .ok_or_else(|| Status::invalid_argument("restore record required"))?
                    .try_into()
                    .map_err(status)?;
                if principal != Principal::Node(record.assignment.node_id.clone()) {
                    return Err(Status::permission_denied(
                        "only restoring node may reserve its reader",
                    ));
                }
                let reader = record
                    .current_checkpoint_reader_entry()
                    .ok_or_else(|| Status::invalid_argument("restore reader required"))?
                    .clone();
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
                let stored = state
                    .session
                    .reserve_checkpoint_reader(&record, &request.node_session_id)
                    .await
                    .map_err(status)?;
                if !state
                    .scheduler
                    .snapshot()
                    .environments()
                    .contains_key(&stored.spec.id)
                {
                    if let Err(error) = state
                        .scheduler
                        .restore_assignment(&stored.spec, &stored.assignment)
                    {
                        state.needs_recovery = true;
                        return Err(status(error));
                    }
                }
                state.remember_environment(stored);
                let coordinator_epoch = state.session.epoch();
                self.0.changed.notify_waiters();
                Ok(Response::new(pb::ReserveCheckpointReaderResponse {
                    reader: Some(reader.into()),
                    coordinator_epoch,
                }))
            })
            .await
    }

    pub(super) async fn release_checkpoint_reader_rpc(
        &self,
        request: Request<pb::ReleaseCheckpointReaderRequest>,
    ) -> std::result::Result<Response<pb::ReleaseCheckpointReaderResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.release_checkpoint_reader", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let request = request.into_inner();
                let reader: adx_core::CheckpointReader = request
                    .reader
                    .ok_or_else(|| Status::invalid_argument("retiring reader required"))?
                    .try_into()
                    .map_err(status)?;
                if principal != Principal::Node(reader.node_id.clone()) {
                    return Err(Status::permission_denied(
                        "only reader's node may release it",
                    ));
                }
                let receipt = adx_core::CheckpointReaderRetirement {
                    reader,
                    backend_receipt: request.backend_receipt,
                };
                receipt.validate().map_err(status)?;
                let mut state = self.0.state.lock().await;
                state
                    .checkpoint_reader_node(
                        &receipt.reader.node_id,
                        &request.node_session_id,
                        self.0.heartbeat_timeout,
                        false,
                    )
                    .await
                    .map_err(status)?;
                let stored = state
                    .session
                    .release_checkpoint_reader(&receipt, &request.node_session_id)
                    .await
                    .map_err(status)?;
                if !stored.resources_held()
                    && state
                        .scheduler
                        .snapshot()
                        .environments()
                        .contains_key(&stored.spec.id)
                {
                    if let Err(error) = state.scheduler.release(&stored.assignment) {
                        state.needs_recovery = true;
                        return Err(status(error));
                    }
                }
                state.remember_environment(stored);
                let coordinator_epoch = state.session.epoch();
                self.0.changed.notify_waiters();
                Ok(Response::new(pb::ReleaseCheckpointReaderResponse {
                    reader: Some(receipt.reader.into()),
                    backend_receipt: receipt.backend_receipt,
                    coordinator_epoch,
                }))
            })
            .await
    }
}
