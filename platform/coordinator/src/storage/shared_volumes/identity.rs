//! Immutable backing-store identity in the existing shared-volume record.
use super::{Error, Result, StoredNode, StoredSharedVolume};

pub(super) const STORAGE_LABEL: &str = "ADX_SHARED_VOLUME_STORAGE";

pub(super) fn validate_storage_id(value: &str) -> Result<()> {
    let parsed = uuid::Uuid::parse_str(value)
        .map_err(|_| Error::Invalid("invalid shared volume storage identity".into()))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(Error::Invalid(
            "invalid shared volume storage identity".into(),
        ));
    }
    Ok(())
}

/// The node advertises this only after validating its directory provider.
/// The caller must retain the current-session and exact node-byte CAS guards.
pub(super) fn node_storage_id(node: &StoredNode) -> Result<&str> {
    let value = node.node.labels.get(STORAGE_LABEL).ok_or_else(|| {
        Error::Unavailable("node has no verified shared volume storage identity".into())
    })?;
    validate_storage_id(value)?;
    Ok(value)
}

impl StoredSharedVolume {
    pub(super) fn bind_storage(&mut self, value: &str) -> Result<()> {
        validate_storage_id(value)?;
        match self.storage_id.as_deref() {
            Some(existing) if existing != value => Err(Error::Conflict),
            Some(_) => Ok(()),
            None => {
                // Even pruned stop history leaves an extra attachment revision.
                // Old data cannot be assigned a new store merely because nobody
                // is currently attached. Reads/retirement deliberately stay open.
                if !self.attachments.is_empty()
                    || (!self.storage_binding_pending
                        && (!self.retirements.is_empty()
                            || self.revision != self.permissions.revision()))
                {
                    return Err(Error::Unavailable(
                        "shared volume storage identity migration required".into(),
                    ));
                }
                self.storage_id = Some(value.to_owned());
                self.storage_binding_pending = false;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::shared_volumes::{SharedVolumeId, SharedVolumeRecord};

    const A: &str = "49fc236e-3821-432c-8784-abc6b02c938e";
    const B: &str = "5cbd5f6e-72de-4cbb-8311-9bf9d62f6410";

    fn volume() -> StoredSharedVolume {
        StoredSharedVolume {
            permissions: SharedVolumeRecord::new(
                SharedVolumeId::parse("00000000-0000-4000-8000-000000000001").unwrap(),
                "owner".into(),
            )
            .unwrap(),
            revision: 1,
            storage_id: None,
            storage_binding_pending: false,
            attachments: vec![],
            retirements: vec![],
        }
    }

    #[test]
    fn storage_identity_is_a_canonical_non_nil_uuid() {
        validate_storage_id(A).unwrap();
        validate_storage_id(B).unwrap();
        for invalid in [
            "",
            "bad",
            "00000000-0000-0000-0000-000000000000",
            "49FC236E-3821-432C-8784-ABC6B02C938E",
            "49fc236e3821432c8784abc6b02c938e",
            " 49fc236e-3821-432c-8784-abc6b02c938e",
        ] {
            assert!(validate_storage_id(invalid).is_err());
        }
    }

    #[test]
    fn fresh_binding_is_idempotent_but_never_changes_stores() {
        let mut value = volume();
        value.bind_storage(A).unwrap();
        let bound = value.clone();
        value.bind_storage(A).unwrap();
        assert_eq!(value, bound);
        assert_eq!(value.bind_storage(B), Err(Error::Conflict));
        assert_eq!(value, bound);
        value.validate().unwrap();
    }

    #[test]
    fn used_unbound_records_require_migration_without_blocking_cleanup_decode() {
        let mut value = volume();
        value.revision += 1;
        value.validate().unwrap();
        assert!(matches!(value.bind_storage(A), Err(Error::Unavailable(_))));
        assert!(value.storage_id.is_none());
        value.storage_id = Some("bad".into());
        assert!(value.validate().is_err());
    }

    #[test]
    fn old_empty_records_decode_unbound_not_with_an_invented_store() {
        let original = volume();
        let wire = serde_json::to_value(&original).unwrap();
        assert!(wire.get("storage_id").is_none());
        let mut restored: StoredSharedVolume = serde_json::from_value(wire).unwrap();
        restored.validate().unwrap();
        restored.bind_storage(A).unwrap();
        assert_eq!(restored.storage_id.as_deref(), Some(A));
    }
}
