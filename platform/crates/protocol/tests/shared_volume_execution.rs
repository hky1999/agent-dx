//! Wire validation is not proof of authorization or backend mount execution.
use adx_core::{
    execution::{ExecutionOptions, MAX_SHARED_VOLUME_MOUNTS},
    shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeMount, SharedVolumeSubpath},
};
use adx_protocol::control as pb;

fn wire() -> pb::SharedVolumeMount {
    pb::SharedVolumeMount {
        volume_id: "00000000-0000-4000-8000-000000000001".into(),
        subpath: "project".into(),
        target: "/shared".into(),
        access: pb::SharedVolumeAccess::ReadWrite as i32,
    }
}

#[test]
fn mount_access_discriminants_remain_management_wire_compatible() {
    assert_eq!(pb::SharedVolumeAccess::ReadOnly as i32, 1);
    assert_eq!(pb::SharedVolumeAccess::ReadWrite as i32, 2);
}

#[test]
fn shared_volume_execution_roundtrip_preserves_ro_and_rw() {
    for access in [SharedVolumeAccess::ReadOnly, SharedVolumeAccess::ReadWrite] {
        let mount = SharedVolumeMount {
            volume_id: SharedVolumeId::parse(wire().volume_id).unwrap(),
            subpath: SharedVolumeSubpath::parse("project").unwrap(),
            target: "/shared".into(),
            access,
        };
        let options = ExecutionOptions {
            shared_volumes: vec![mount],
            ..Default::default()
        };
        let encoded: pb::ExecutionOptions = options.clone().into();
        assert_eq!(ExecutionOptions::try_from(encoded).unwrap(), options);
    }
}

#[test]
fn old_empty_execution_decodes_without_a_shared_mount() {
    let options = ExecutionOptions::try_from(pb::ExecutionOptions::default()).unwrap();
    assert!(options.shared_volumes.is_empty());
    let encoded: pb::ExecutionOptions = options.into();
    assert!(encoded.shared_volumes.is_empty());
}

#[test]
fn missing_or_unknown_access_never_defaults_to_read_only() {
    for access in [0, -1, 99] {
        let mut mount = wire();
        mount.access = access;
        assert!(SharedVolumeMount::try_from(mount).is_err());
    }
}

#[test]
fn wire_mount_rejects_invalid_ids_paths_and_reserved_targets() {
    for field in ["id", "subpath", "target"] {
        let mut mount = wire();
        match field {
            "id" => mount.volume_id = "../../host".into(),
            "subpath" => mount.subpath = "../secret".into(),
            _ => mount.target = "/__adx/private".into(),
        }
        assert!(SharedVolumeMount::try_from(mount).is_err());
    }
}

#[test]
fn execution_decoder_checks_duplicate_and_overlapping_mounts() {
    let first = wire();
    let mut duplicate_volume = wire();
    duplicate_volume.target = "/second".into();
    let mut overlap = wire();
    overlap.volume_id = "00000000-0000-4000-8000-000000000002".into();
    overlap.target = "/shared/sub".into();
    for second in [duplicate_volume, overlap] {
        assert!(ExecutionOptions::try_from(pb::ExecutionOptions {
            shared_volumes: vec![first.clone(), second],
            ..Default::default()
        })
        .is_err());
    }
}

#[test]
fn execution_decoder_rejects_unbounded_mount_lists() {
    assert!(ExecutionOptions::try_from(pb::ExecutionOptions {
        shared_volumes: vec![wire(); MAX_SHARED_VOLUME_MOUNTS + 1],
        ..Default::default()
    })
    .is_err());
}
