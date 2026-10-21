mod common;
use adx_coordinator::{
    storage::{NodeSession, Session, StoredSharedVolume},
    Node,
};
use adx_core::{
    runtime::RuntimeIdentity,
    shared_volume_attachments::{SharedVolumeAttachment, SharedVolumeAttachmentRetirement},
    shared_volumes::{
        SharedVolumeAccess as Access, SharedVolumeGrant as Grant, SharedVolumeId as Id,
        SharedVolumeMount as Mount, SharedVolumeSubpath as Subpath,
    },
    Assignment, CheckpointReaders, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error,
    Resources, Runtime,
};
use serde_json::json;

fn id() -> Id {
    Id::parse(uuid::Uuid::new_v4().to_string()).unwrap()
}
fn spec(name: &str, tenant: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: name.into(),
        tenant_id: tenant.into(),
        image: "image".into(),
        runtime_class: "runsc".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
        execution: Default::default(),
    }
}
const STORAGE_A: &str = "49fc236e-3821-432c-8784-abc6b02c938e";
const STORAGE_B: &str = "5cbd5f6e-72de-4cbb-8311-9bf9d62f6410";

async fn node(session: &Session, name: &str, boot: &str) {
    node_storage(session, name, boot, STORAGE_A).await;
}
async fn node_storage(session: &Session, name: &str, boot: &str, storage: &str) {
    session
        .register_session(
            Node {
                id: name.into(),
                runtime_classes: vec!["runsc".into()],
                capacity: Resources {
                    cpu_millis: 100,
                    memory_bytes: 100,
                    disk_bytes: 0,
                },
                available: true,
                labels: [("ADX_SHARED_VOLUME_STORAGE".into(), storage.to_owned())].into(),
                devices: vec![],
            },
            "node:1".into(),
            "node:2".into(),
            Some(NodeSession {
                id: boot.into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
}
async fn execution(session: &Session, name: &str, tenant: &str, owner: &str) -> EnvironmentRecord {
    let generation = session.snapshot().await.unwrap().generation + 1;
    let assignment = Assignment {
        environment_id: name.into(),
        node_id: owner.into(),
        shard_id: 0,
        generation,
        devices: vec![],
    };
    let spec = spec(name, tenant);
    session
        .reserve(spec.clone(), assignment.clone())
        .await
        .unwrap();
    EnvironmentRecord {
        checkpoint_readers: CheckpointReaders::known_empty(),
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        runtime: Runtime {
            id: format!("{name}-{generation}"),
            ip: None,
        },
        spec,
        assignment,
        state: EnvironmentState::Starting,
        revision: 1,
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    }
}
fn mount(volume: &StoredSharedVolume, mode: Access) -> Mount {
    Mount {
        volume_id: volume.permissions.id().clone(),
        subpath: Subpath::default(),
        target: "/shared".into(),
        access: mode,
    }
}
fn receipt(attachment: &SharedVolumeAttachment) -> SharedVolumeAttachmentRetirement {
    // Trusted-node storage fixture ONLY; no claim of a real VMM stop.
    SharedVolumeAttachmentRetirement {
        attachment: attachment.clone(),
        backend_receipt: "test-only:strong-backend-stop".into(),
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn create_replay_is_atomic_tenant_scoped_and_survives_epoch_restart() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let first = db.begin(1).await.unwrap();
    let request = id();
    let (a, b) = tokio::join!(
        first.create_shared_volume("owner", &request),
        first.create_shared_volume("owner", &request)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let own = first.create_shared_volume("owner", &request).await.unwrap();
    let other = first.create_shared_volume("other", &request).await.unwrap();
    assert_ne!(own.permissions.id(), other.permissions.id());
    let next = db.begin(1).await.unwrap();
    assert_eq!(
        next.create_shared_volume("owner", &request).await.unwrap(),
        own
    );
    assert_eq!(
        first.get_shared_volume(own.permissions.id(), "owner").await,
        Err(Error::Conflict)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn owner_permissions_are_conditional_and_do_not_change_route_catalog() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let before = s.snapshot().await.unwrap();
    assert_eq!(
        s.get_shared_volume(v.permissions.id(), "intruder").await,
        Err(Error::NotFound)
    );
    let grant = Grant {
        tenant_id: "reader".into(),
        subpath: Subpath::default(),
        access: Access::ReadOnly,
    };
    assert_eq!(
        s.grant_shared_volume(v.permissions.id(), "intruder", 1, grant.clone())
            .await,
        Err(Error::NotFound)
    );
    let granted = s
        .grant_shared_volume(v.permissions.id(), "owner", 1, grant.clone())
        .await
        .unwrap();
    assert_eq!(granted.permissions.revision(), 2);
    assert_eq!(
        s.revoke_shared_volume(
            v.permissions.id(),
            "owner",
            1,
            "reader",
            &Subpath::default()
        )
        .await,
        Err(Error::Conflict)
    );
    let revoked = s
        .revoke_shared_volume(
            v.permissions.id(),
            "owner",
            2,
            "reader",
            &Subpath::default(),
        )
        .await
        .unwrap();
    assert!(revoked.permissions.grants().is_empty());
    assert_eq!(
        s.grant_shared_volume(v.permissions.id(), "owner", 1, grant)
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(s.snapshot().await.unwrap(), before);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn competing_writers_have_exactly_one_durable_winner() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let m = mount(&v, Access::ReadWrite);
    let a = execution(&s, "a", "owner", "n").await;
    let b = execution(&s, "b", "owner", "n").await;
    let (x, y) = tokio::join!(
        s.reserve_shared_volume(&a, "boot", &m),
        s.reserve_shared_volume(&b, "boot", &m)
    );
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    assert!(x.is_ok() || matches!(x, Err(Error::Conflict)));
    assert!(y.is_ok() || matches!(y, Err(Error::Conflict)));
    let stored = s
        .get_shared_volume(v.permissions.id(), "owner")
        .await
        .unwrap();
    assert_eq!(stored.attachments.len(), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn read_only_readers_coexist_but_cannot_upgrade_their_reservation() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let writer = execution(&s, "w", "owner", "n").await;
    let reader = execution(&s, "r", "reader", "n").await;
    let v = s
        .grant_shared_volume(
            v.permissions.id(),
            "owner",
            1,
            Grant {
                tenant_id: "reader".into(),
                subpath: Subpath::default(),
                access: Access::ReadOnly,
            },
        )
        .await
        .unwrap();
    s.reserve_shared_volume(&writer, "boot", &mount(&v, Access::ReadWrite))
        .await
        .unwrap();
    let active = s
        .reserve_shared_volume(&reader, "boot", &mount(&v, Access::ReadOnly))
        .await
        .unwrap();
    assert_eq!(active.attachments.len(), 2);
    assert_eq!(
        s.reserve_shared_volume(&reader, "boot", &mount(&v, Access::ReadOnly))
            .await
            .unwrap(),
        active
    );
    assert_eq!(
        s.reserve_shared_volume(&reader, "boot", &mount(&v, Access::ReadWrite))
            .await,
        Err(Error::NotFound)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn node_session_replacement_never_frees_a_live_writer() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let m = mount(&v, Access::ReadWrite);
    let a = execution(&s, "a", "owner", "n").await;
    let b = execution(&s, "b", "owner", "n").await;
    let held = s.reserve_shared_volume(&a, "boot", &m).await.unwrap();
    node(&s, "n", "new-boot").await;
    assert_eq!(
        s.release_shared_volume(&receipt(&held.attachments[0]), "boot")
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        s.reserve_shared_volume(&b, "new-boot", &m).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        s.get_shared_volume(v.permissions.id(), "owner")
            .await
            .unwrap(),
        held
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn exact_stop_receipt_transfers_writer_and_fences_delayed_reacquisition() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let m = mount(&v, Access::ReadWrite);
    let a = execution(&s, "a", "owner", "n").await;
    let b = execution(&s, "b", "owner", "n").await;
    let held = s.reserve_shared_volume(&a, "boot", &m).await.unwrap();
    let proof = receipt(&held.attachments[0]);
    let released = s.release_shared_volume(&proof, "boot").await.unwrap();
    assert!(released.attachments.is_empty());
    assert_eq!(
        s.release_shared_volume(&proof, "boot").await.unwrap(),
        released
    );
    assert_eq!(
        s.reserve_shared_volume(&a, "boot", &m).await,
        Err(Error::Conflict)
    );
    let next = s.reserve_shared_volume(&b, "boot", &m).await.unwrap();
    assert_eq!(next.attachments[0].identity.environment_id, "b");
    let mut wrong = proof;
    wrong.backend_receipt = "different-receipt".into();
    assert_eq!(
        s.release_shared_volume(&wrong, "boot").await,
        Err(Error::Conflict)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn permission_revocation_does_not_silently_release_an_old_writer() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let v = s
        .grant_shared_volume(
            v.permissions.id(),
            "owner",
            1,
            Grant {
                tenant_id: "agent".into(),
                subpath: Subpath::default(),
                access: Access::ReadWrite,
            },
        )
        .await
        .unwrap();
    let a = execution(&s, "a", "agent", "n").await;
    let b = execution(&s, "b", "owner", "n").await;
    let m = mount(&v, Access::ReadWrite);
    let held = s.reserve_shared_volume(&a, "boot", &m).await.unwrap();
    s.revoke_shared_volume(v.permissions.id(), "owner", 2, "agent", &Subpath::default())
        .await
        .unwrap();
    assert_eq!(
        s.reserve_shared_volume(&a, "boot", &m).await,
        Err(Error::NotFound)
    );
    assert_eq!(
        s.reserve_shared_volume(&b, "boot", &m).await,
        Err(Error::Conflict)
    );
    s.release_shared_volume(&receipt(&held.attachments[0]), "boot")
        .await
        .unwrap();
    s.reserve_shared_volume(&b, "boot", &m).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn ownership_invalidation_does_not_authorize_writer_reclamation() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "old", "boot").await;
    node(&s, "new", "new-boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let m = mount(&v, Access::ReadWrite);
    let a = execution(&s, "a", "owner", "old").await;
    let b = execution(&s, "b", "owner", "new").await;
    let held = s.reserve_shared_volume(&a, "boot", &m).await.unwrap();
    s.invalidate_node("old", "boot").await.unwrap();
    assert_eq!(
        s.reserve_shared_volume(&b, "new-boot", &m).await,
        Err(Error::Conflict)
    );
    s.release_shared_volume(&receipt(&held.attachments[0]), "boot")
        .await
        .unwrap();
    s.reserve_shared_volume(&b, "new-boot", &m).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn redis_aof_and_coordinator_takeover_preserve_writer_and_stop_history() {
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let m = mount(&v, Access::ReadWrite);
    let a = execution(&s, "a", "owner", "n").await;
    let held = s.reserve_shared_volume(&a, "boot", &m).await.unwrap();
    rig.crash();
    // Observe the unavailable old transport before asserting the new epoch fence.
    // The store clears failed connections; it never blindly replays a write.
    assert!(matches!(
        s.release_shared_volume(&receipt(&held.attachments[0]), "boot")
            .await,
        Err(Error::Unavailable(_))
    ));
    rig.start().await;
    let next = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        next.get_shared_volume(v.permissions.id(), "owner")
            .await
            .unwrap(),
        held
    );
    assert_eq!(
        s.release_shared_volume(&receipt(&held.attachments[0]), "boot")
            .await,
        Err(Error::Conflict)
    );
    let stopped = next
        .release_shared_volume(&receipt(&held.attachments[0]), "boot")
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let next = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        next.get_shared_volume(v.permissions.id(), "owner")
            .await
            .unwrap(),
        stopped
    );
    assert_eq!(
        next.reserve_shared_volume(&a, "boot", &m).await,
        Err(Error::Conflict)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn forged_environment_or_stop_receipt_is_rejected_without_state_change() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let m = mount(&v, Access::ReadWrite);
    let a = execution(&s, "a", "owner", "n").await;
    let mut forged = a.clone();
    forged.spec.tenant_id = "other".into();
    assert_eq!(
        s.reserve_shared_volume(&forged, "boot", &m).await,
        Err(Error::Conflict)
    );
    let held = s.reserve_shared_volume(&a, "boot", &m).await.unwrap();
    let mut wrong = receipt(&held.attachments[0]);
    wrong.attachment.identity.runtime_id.push_str("-r99");
    assert_eq!(
        s.release_shared_volume(&wrong, "boot").await,
        Err(Error::Conflict)
    );
    assert_eq!(
        s.get_shared_volume(v.permissions.id(), "owner")
            .await
            .unwrap(),
        held
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn omitted_attachment_mode_or_missing_history_is_corrupt_not_empty() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "n", "boot").await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let a = execution(&s, "a", "owner", "n").await;
    let held = s
        .reserve_shared_volume(&a, "boot", &mount(&v, Access::ReadWrite))
        .await
        .unwrap();
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    for field in ["mode", "history"] {
        let mut bad = serde_json::to_value(&held).unwrap();
        if field == "mode" {
            bad["attachments"][0]
                .as_object_mut()
                .unwrap()
                .remove("access");
        } else {
            bad.as_object_mut().unwrap().remove("retirements");
        }
        let _: usize = redis::cmd("HSET")
            .arg("adx:{test}:control:v1:shared-volumes")
            .arg(format!("volume:{}", v.permissions.id().as_str()))
            .arg(bad.to_string())
            .query_async(&mut c)
            .await
            .unwrap();
        assert!(s
            .get_shared_volume(v.permissions.id(), "owner")
            .await
            .is_err());
    }
}

#[test]
fn attachment_and_receipt_validation_do_not_invent_backend_stop_proof() {
    let attachment = SharedVolumeAttachment {
        volume_id: id(),
        subpath: Subpath::default(),
        target: "/shared".into(),
        access: Access::ReadWrite,
        tenant_id: "owner".into(),
        node_id: "node".into(),
        identity: RuntimeIdentity {
            environment_id: "e".into(),
            runtime_id: "e-1".into(),
            ownership_generation: 1,
        },
    };
    attachment.validate().unwrap();
    let mut raw = serde_json::to_value(&attachment).unwrap();
    raw.as_object_mut().unwrap().remove("access");
    assert!(serde_json::from_value::<SharedVolumeAttachment>(raw).is_err());
    for invalid in ["".to_owned(), "bad\nreceipt".to_owned(), "x".repeat(4097)] {
        assert!(SharedVolumeAttachmentRetirement {
            attachment: attachment.clone(),
            backend_receipt: invalid
        }
        .validate()
        .is_err());
    }
    let mut bad = attachment;
    bad.identity.runtime_id = "another-1".into();
    assert!(bad.validate().is_err());
    assert_eq!(
        serde_json::to_value(Access::ReadWrite).unwrap(),
        json!("read_write")
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn single_volume_admission_cannot_bypass_immutable_storage_binding() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "first", "boot").await;
    node_storage(&s, "other", "other-boot", STORAGE_B).await;
    let v = s.create_shared_volume("owner", &id()).await.unwrap();
    let first = execution(&s, "first", "owner", "first").await;
    let second = execution(&s, "second", "owner", "other").await;
    let held = s
        .reserve_shared_volume(&first, "boot", &mount(&v, Access::ReadOnly))
        .await
        .unwrap();
    assert_eq!(held.storage_id.as_deref(), Some(STORAGE_A));
    assert_eq!(
        s.reserve_shared_volume(&second, "other-boot", &mount(&v, Access::ReadOnly))
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        s.get_shared_volume(v.permissions.id(), "owner")
            .await
            .unwrap(),
        held
    );
    s.release_shared_volume(&receipt(&held.attachments[0]), "boot")
        .await
        .unwrap();
}
