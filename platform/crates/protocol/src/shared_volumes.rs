//! Public permission projection deliberately excludes execution occupancy/receipts.
use crate::control as pb;
use adx_core::{
    shared_volumes::{
        SharedVolumeAccess as Access, SharedVolumeGrant as Grant, SharedVolumeRecord,
        SharedVolumeSubpath,
    },
    Error, Result,
};

impl From<&SharedVolumeRecord> for pb::SharedVolume {
    fn from(value: &SharedVolumeRecord) -> Self {
        Self {
            id: value.id().as_str().into(),
            owner_tenant_id: value.owner_tenant_id().into(),
            permission_revision: value.revision(),
            grants: value
                .grants()
                .iter()
                .map(|grant| pb::SharedVolumeGrant {
                    tenant_id: grant.tenant_id.clone(),
                    subpath: grant.subpath.as_str().into(),
                    access: match grant.access {
                        Access::ReadOnly => pb::SharedVolumeAccess::ReadOnly,
                        Access::ReadWrite => pb::SharedVolumeAccess::ReadWrite,
                    } as i32,
                })
                .collect(),
        }
    }
}

impl TryFrom<pb::SharedVolumeGrant> for Grant {
    type Error = Error;
    fn try_from(value: pb::SharedVolumeGrant) -> Result<Self> {
        Ok(Self {
            tenant_id: value.tenant_id,
            subpath: SharedVolumeSubpath::parse(&value.subpath)?,
            access: match pb::SharedVolumeAccess::try_from(value.access) {
                Ok(pb::SharedVolumeAccess::ReadOnly) => Access::ReadOnly,
                Ok(pb::SharedVolumeAccess::ReadWrite) => Access::ReadWrite,
                _ => {
                    return Err(Error::Invalid(
                        "explicit shared volume access required".into(),
                    ))
                }
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unspecified_access_is_not_a_silent_read_only_downgrade() {
        for access in [0, -1, 99] {
            assert!(Grant::try_from(pb::SharedVolumeGrant {
                tenant_id: "reader".into(),
                subpath: String::new(),
                access
            })
            .is_err());
        }
    }
    #[test]
    fn projection_preserves_full_width_revision_and_typed_permissions() {
        let mut record = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000001",
            "owner_tenant_id": "owner", "revision": u64::MAX,
            "grants": [{"tenant_id":"reader","subpath":"project","access":"read_only"}]
        });
        let record: SharedVolumeRecord = serde_json::from_value(record.take()).unwrap();
        let wire = pb::SharedVolume::from(&record);
        assert_eq!(wire.permission_revision, u64::MAX);
        assert_eq!(
            Grant::try_from(wire.grants[0].clone()).unwrap(),
            record.grants()[0]
        );
    }
}
