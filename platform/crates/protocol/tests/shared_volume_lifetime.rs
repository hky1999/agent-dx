use adx_core::{
    runtime::RuntimeIdentity,
    shared_volume_attachments::{SharedVolumeAttachment, SharedVolumeAttachmentRetirement},
    shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeSubpath},
};
use adx_protocol::control as pb;
fn attachment() -> SharedVolumeAttachment {
    SharedVolumeAttachment {
        volume_id: SharedVolumeId::parse("00000000-0000-4000-8000-000000000001").unwrap(),
        subpath: SharedVolumeSubpath::default(),
        target: "/shared".into(),
        access: SharedVolumeAccess::ReadWrite,
        tenant_id: "tenant".into(),
        node_id: "node".into(),
        identity: RuntimeIdentity {
            environment_id: "env".into(),
            runtime_id: "env-1".into(),
            ownership_generation: 1,
        },
    }
}
#[test]
fn occupancy_and_stop_receipt_roundtrip_preserve_exact_execution() {
    let original = attachment();
    let wire: pb::SharedVolumeAttachment = original.clone().into();
    assert_eq!(SharedVolumeAttachment::try_from(wire).unwrap(), original);
    let proof = SharedVolumeAttachmentRetirement {
        attachment: original,
        backend_receipt: "fixture-only".into(),
    };
    let wire: pb::SharedVolumeRetirement = proof.clone().into();
    assert_eq!(
        SharedVolumeAttachmentRetirement::try_from(wire).unwrap(),
        proof
    );
}
#[test]
fn lifetime_wire_rejects_missing_mount_unknown_access_and_execution_mismatch() {
    let good: pb::SharedVolumeAttachment = attachment().into();
    let mut missing = good.clone();
    missing.mount = None;
    let mut access = good.clone();
    access.mount.as_mut().unwrap().access = 0;
    let mut identity = good;
    identity.runtime_id = "different-1".into();
    for wire in [missing, access, identity] {
        assert!(SharedVolumeAttachment::try_from(wire).is_err());
    }
}
#[test]
fn lifetime_wire_rejects_missing_or_empty_retirement_proof() {
    assert!(
        SharedVolumeAttachmentRetirement::try_from(pb::SharedVolumeRetirement::default()).is_err()
    );
    assert!(
        SharedVolumeAttachmentRetirement::try_from(pb::SharedVolumeRetirement {
            attachment: Some(attachment().into()),
            backend_receipt: String::new()
        })
        .is_err()
    );
}
