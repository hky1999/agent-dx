//! The authenticated Frontend supplies tenant identity; request bodies cannot
//! select an owner or obtain execution retirement / filesystem deletion power.
use super::*;
use adx_core::shared_volumes::{SharedVolumeId, SharedVolumeSubpath};

// Validation failures retain the exact RPC status while keeping synchronous
// helper results small; the required tonic boundary unwraps only on failure.
type VolumeCheck<T> = std::result::Result<T, Box<Status>>;

impl CoordinatorRpc {
    fn volume_caller<T>(&self, request: &Request<T>) -> VolumeCheck<()> {
        if self.0.peers.authenticate(request)? != Principal::ApiServer {
            return Err(Status::permission_denied("Frontend identity required").into());
        }
        Ok(())
    }
    async fn volume_session(&self) -> std::result::Result<Session, Status> {
        let state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        Ok(state.session.clone())
    }
}

fn caller(value: Option<pb::CallerContext>) -> VolumeCheck<String> {
    let value = value.ok_or_else(|| Status::unauthenticated("caller context required"))?;
    tenant(Some(&value), &value.tenant_id)?;
    // Administrator is not a volume-owner impersonation capability.
    Ok(value.tenant_id)
}
fn revision(value: u64) -> VolumeCheck<u64> {
    if value == 0 {
        Err(Status::invalid_argument("positive permission revision required").into())
    } else {
        Ok(value)
    }
}

#[tonic::async_trait]
impl pb::shared_volume_service_server::SharedVolumeService for CoordinatorRpc {
    async fn create_shared_volume(
        &self,
        request: Request<pb::CreateSharedVolumeRequest>,
    ) -> std::result::Result<Response<pb::SharedVolume>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.create", &request);
        trace
            .run_result(async {
                self.volume_caller(&request).map_err(|error| *error)?;
                let request = request.into_inner();
                let owner = caller(request.caller).map_err(|error| *error)?;
                let request_id = SharedVolumeId::parse(&request.request_id).map_err(status)?;
                let stored = self
                    .volume_session()
                    .await?
                    .create_shared_volume(&owner, &request_id)
                    .await
                    .map_err(status)?;
                Ok(Response::new((&stored.permissions).into()))
            })
            .await
    }
    async fn get_shared_volume(
        &self,
        request: Request<pb::GetSharedVolumeRequest>,
    ) -> std::result::Result<Response<pb::SharedVolume>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.get", &request);
        trace
            .run_result(async {
                self.volume_caller(&request).map_err(|error| *error)?;
                let request = request.into_inner();
                let owner = caller(request.caller).map_err(|error| *error)?;
                let id = SharedVolumeId::parse(&request.id).map_err(status)?;
                let stored = self
                    .volume_session()
                    .await?
                    .get_shared_volume(&id, &owner)
                    .await
                    .map_err(status)?;
                Ok(Response::new((&stored.permissions).into()))
            })
            .await
    }
    async fn grant_shared_volume(
        &self,
        request: Request<pb::GrantSharedVolumeRequest>,
    ) -> std::result::Result<Response<pb::SharedVolume>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.grant", &request);
        trace
            .run_result(async {
                self.volume_caller(&request).map_err(|error| *error)?;
                let request = request.into_inner();
                let owner = caller(request.caller).map_err(|error| *error)?;
                let id = SharedVolumeId::parse(&request.id).map_err(status)?;
                let expected =
                    revision(request.expected_permission_revision).map_err(|error| *error)?;
                let grant = request
                    .grant
                    .ok_or_else(|| Status::invalid_argument("grant required"))?
                    .try_into()
                    .map_err(status)?;
                let stored = self
                    .volume_session()
                    .await?
                    .grant_shared_volume(&id, &owner, expected, grant)
                    .await
                    .map_err(status)?;
                Ok(Response::new((&stored.permissions).into()))
            })
            .await
    }
    async fn revoke_shared_volume(
        &self,
        request: Request<pb::RevokeSharedVolumeRequest>,
    ) -> std::result::Result<Response<pb::SharedVolume>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.revoke", &request);
        trace
            .run_result(async {
                self.volume_caller(&request).map_err(|error| *error)?;
                let request = request.into_inner();
                let owner = caller(request.caller).map_err(|error| *error)?;
                let id = SharedVolumeId::parse(&request.id).map_err(status)?;
                let expected =
                    revision(request.expected_permission_revision).map_err(|error| *error)?;
                let subpath = SharedVolumeSubpath::parse(&request.subpath).map_err(status)?;
                let stored = self
                    .volume_session()
                    .await?
                    .revoke_shared_volume(&id, &owner, expected, &request.tenant_id, &subpath)
                    .await
                    .map_err(status)?;
                Ok(Response::new((&stored.permissions).into()))
            })
            .await
    }
}
