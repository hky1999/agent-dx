//! Owner-only metadata routes: never accept a host path or retirement receipt.
use crate::http::Api;
use adx_core::shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeSubpath};
use adx_observability::trace;
use adx_protocol::control as pb;
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::Status;

const ROOT: &str = "/api/environment/v1/shared-volumes";
pub(crate) fn is_route(path: &str) -> bool {
    path == ROOT
        || path
            .strip_prefix(ROOT)
            .is_some_and(|rest| rest.starts_with('/'))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Create {
    request_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Grant {
    expected_permission_revision: String,
    tenant_id: String,
    #[serde(default)]
    subpath: String,
    access: SharedVolumeAccess,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Revoke {
    expected_permission_revision: String,
    tenant_id: String,
    #[serde(default)]
    subpath: String,
}
fn invalid() -> Status {
    Status::invalid_argument("invalid shared volume request")
}
fn revision(value: &str) -> Result<u64, Status> {
    let parsed = value.parse::<u64>().map_err(|_| invalid())?;
    if parsed == 0 || parsed.to_string() != value {
        return Err(invalid());
    }
    Ok(parsed)
}
fn grant_request(
    id: String,
    input: Value,
    caller: pb::CallerContext,
) -> Result<pb::GrantSharedVolumeRequest, Status> {
    let input: Grant = serde_json::from_value(input).map_err(|_| invalid())?;
    SharedVolumeSubpath::parse(&input.subpath).map_err(adx_protocol::status)?;
    Ok(pb::GrantSharedVolumeRequest {
        id,
        caller: Some(caller),
        expected_permission_revision: revision(&input.expected_permission_revision)?,
        grant: Some(pb::SharedVolumeGrant {
            tenant_id: input.tenant_id,
            subpath: input.subpath,
            access: match input.access {
                SharedVolumeAccess::ReadOnly => pb::SharedVolumeAccess::ReadOnly,
                SharedVolumeAccess::ReadWrite => pb::SharedVolumeAccess::ReadWrite,
            } as i32,
        }),
    })
}
fn projection(
    value: pb::SharedVolume,
    expected: Option<&str>,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    SharedVolumeId::parse(&value.id).map_err(|_| Status::data_loss("invalid volume identity"))?;
    if expected.is_some_and(|id| id != value.id)
        || value.owner_tenant_id != caller.tenant_id
        || value.permission_revision == 0
        || value.grants.len() > 256
    {
        return Err(Status::data_loss("invalid volume permission response"));
    }
    let grants = value
        .grants
        .into_iter()
        .map(|grant| {
            SharedVolumeSubpath::parse(&grant.subpath)
                .map_err(|_| Status::data_loss("invalid grant"))?;
            let access = match pb::SharedVolumeAccess::try_from(grant.access) {
                Ok(pb::SharedVolumeAccess::ReadOnly) => "read_only",
                Ok(pb::SharedVolumeAccess::ReadWrite) => "read_write",
                _ => return Err(Status::data_loss("invalid grant access")),
            };
            if grant.tenant_id.trim().is_empty() {
                return Err(Status::data_loss("invalid grant tenant"));
            }
            Ok(json!({"tenantId":grant.tenant_id,"subpath":grant.subpath,"access":access}))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    // Strings preserve every u64 bit for non-Python JSON clients too.
    Ok(json!({"id":value.id,"ownerTenantId":value.owner_tenant_id,
        "permissionRevision":value.permission_revision.to_string(),"grants":grants}))
}
impl Api {
    pub(crate) async fn shared_volumes(
        &self,
        method: &str,
        path: &str,
        input: Value,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let mut client = pb::shared_volume_service_client::SharedVolumeServiceClient::new(
            self.clients.coordinator().await?,
        );
        if path == format!("{ROOT}/mount-contract") {
            if method != "GET" || input.as_object().is_none_or(|value| !value.is_empty()) {
                return Err(invalid());
            }
            // This advertises parsing/version support, NOT filesystem readiness.
            return Ok(json!({"version": 1}));
        }
        if path == ROOT {
            if method != "POST" {
                return Err(Status::unimplemented("unsupported volume method"));
            }
            let input: Create = serde_json::from_value(input).map_err(|_| invalid())?;
            SharedVolumeId::parse(&input.request_id).map_err(adx_protocol::status)?;
            let value = self
                .clients
                .rpc(
                    "apiserver.volume.create",
                    client.create_shared_volume(trace::inject(pb::CreateSharedVolumeRequest {
                        request_id: input.request_id,
                        caller: Some(caller.clone()),
                    })),
                )
                .await?;
            return projection(value, None, caller);
        }
        let rest = path
            .strip_prefix(ROOT)
            .and_then(|v| v.strip_prefix('/'))
            .ok_or_else(invalid)?;
        let (id, action) = rest.split_once('/').unwrap_or((rest, ""));
        SharedVolumeId::parse(id).map_err(adx_protocol::status)?;
        let value = match (method, action) {
            ("GET", "") => {
                if input.as_object().is_none_or(|value| !value.is_empty()) {
                    return Err(invalid());
                }
                self.clients
                    .rpc(
                        "apiserver.volume.get",
                        client.get_shared_volume(trace::inject(pb::GetSharedVolumeRequest {
                            id: id.into(),
                            caller: Some(caller.clone()),
                        })),
                    )
                    .await?
            }
            ("POST", "grant") => {
                self.clients
                    .rpc(
                        "apiserver.volume.grant",
                        client.grant_shared_volume(trace::inject(grant_request(
                            id.into(),
                            input,
                            caller.clone(),
                        )?)),
                    )
                    .await?
            }
            ("POST", "revoke") => {
                let input: Revoke = serde_json::from_value(input).map_err(|_| invalid())?;
                SharedVolumeSubpath::parse(&input.subpath).map_err(adx_protocol::status)?;
                self.clients
                    .rpc(
                        "apiserver.volume.revoke",
                        client.revoke_shared_volume(trace::inject(pb::RevokeSharedVolumeRequest {
                            id: id.into(),
                            caller: Some(caller.clone()),
                            expected_permission_revision: revision(
                                &input.expected_permission_revision,
                            )?,
                            tenant_id: input.tenant_id,
                            subpath: input.subpath,
                        })),
                    )
                    .await?
            }
            _ => return Err(Status::unimplemented("unsupported volume method")),
        };
        projection(value, Some(id), caller)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn caller() -> pb::CallerContext {
        pb::CallerContext {
            tenant_id: "owner".into(),
            administrator: false,
        }
    }
    #[test]
    fn route_requires_exact_segment_boundary() {
        assert!(is_route(ROOT));
        assert!(is_route(&format!("{ROOT}/id")));
        assert!(!is_route(&format!("{ROOT}-other")));
        assert!(!is_route("/api/environment/v1/environments"));
    }
    #[test]
    fn owner_and_host_path_are_not_create_inputs() {
        for input in [
            json!({"requestId":"x","ownerTenantId":"other"}),
            json!({"requestId":"x","hostPath":"/etc"}),
            json!({}),
        ] {
            assert!(serde_json::from_value::<Create>(input).is_err());
        }
    }
    #[test]
    fn permission_versions_never_round_through_float_or_default_zero() {
        assert_eq!(revision(&u64::MAX.to_string()).unwrap(), u64::MAX);
        for input in ["0", "01", "-1", "+1", "1.0", "18446744073709551616"] {
            assert!(revision(input).is_err());
        }
        assert!(grant_request(
            "id".into(),
            json!({"expectedPermissionRevision":1,
            "tenantId":"reader","access":"read_only"}),
            caller()
        )
        .is_err());
    }
    #[test]
    fn explicit_access_and_safe_subpath_are_required() {
        for input in [
            json!({"expectedPermissionRevision":"1","tenantId":"reader"}),
            json!({"expectedPermissionRevision":"1","tenantId":"reader","access":"read_only","subpath":"../x"}),
            json!({"expectedPermissionRevision":"1","tenantId":"reader","access":"unknown"}),
        ] {
            assert!(grant_request("id".into(), input, caller()).is_err());
        }
    }
    #[test]
    fn public_projection_preserves_revision_and_hides_internal_state() {
        let id = "00000000-0000-0000-0000-000000000001";
        let wire = pb::SharedVolume {
            id: id.into(),
            owner_tenant_id: "owner".into(),
            permission_revision: u64::MAX,
            grants: vec![],
        };
        let value = projection(wire.clone(), Some(id), &caller()).unwrap();
        assert_eq!(value["permissionRevision"], u64::MAX.to_string());
        assert_eq!(value.as_object().unwrap().len(), 4);
        assert!(projection(wire.clone(), Some("other"), &caller()).is_err());
        let mut other = caller();
        other.tenant_id = "other".into();
        other.administrator = true;
        assert!(projection(wire, Some(id), &other).is_err());
    }
}
