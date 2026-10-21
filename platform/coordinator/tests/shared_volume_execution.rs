//! Real private Redis authority tests; fixture receipts are not VMM stop proof.
mod common;

use adx_coordinator::{
    storage::{NodeSession, Session},
    Node,
};
use adx_core::shared_volumes::SharedVolumeGrant;
use adx_core::{
    shared_volume_attachments::{attachments_for_record, SharedVolumeAttachmentRetirement},
    shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeMount, SharedVolumeSubpath},
    Assignment, CheckpointReaders, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error,
    Resources, Runtime,
};

const STORAGE_A: &str = "49fc236e-3821-432c-8784-abc6b02c938e";

/// A failed first mount must fence that execution without making a newly
/// created volume unusable for a later, authorized execution after restart.
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn fresh_volume_can_bind_after_denied_admission_and_stop_across_restart() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    node(&session, "boot").await;
    let id = volume(&session, "other").await;
    let denied = record(
        &session,
        "denied-first-mount",
        vec![mount(id.clone(), "/shared")],
    )
    .await;
    assert!(matches!(
        session.reserve_shared_volumes(&denied, "boot").await,
        Err(Error::NotFound)
    ));
    let stopped = receipts(&denied);
    session
        .release_shared_volumes(&stopped, "boot")
        .await
        .unwrap();
    let before = session.get_shared_volume(&id, "other").await.unwrap();
    assert_eq!(before.storage_id, None);
    assert!(before.attachments.is_empty());
    assert_eq!(before.retirements.len(), 1);
    session
        .grant_shared_volume(
            &id,
            "other",
            1,
            SharedVolumeGrant {
                tenant_id: "owner".into(),
                subpath: SharedVolumeSubpath::default(),
                access: SharedVolumeAccess::ReadWrite,
            },
        )
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    let allowed = record(
        &restarted,
        "authorized-successor",
        vec![mount(id.clone(), "/shared")],
    )
    .await;
    restarted
        .reserve_shared_volumes(&allowed, "boot")
        .await
        .unwrap();
    let bound = restarted.get_shared_volume(&id, "other").await.unwrap();
    assert_eq!(bound.storage_id.as_deref(), Some(STORAGE_A));
    assert_eq!(bound.attachments.len(), 1);
    assert!(bound.retirements.iter().any(|entry| entry == &stopped[0]));
    assert!(matches!(
        restarted.reserve_shared_volumes(&denied, "boot").await,
        Err(Error::Conflict)
    ));
    restarted
        .release_shared_volumes(&receipts(&allowed), "boot")
        .await
        .unwrap();
}
const STORAGE_B: &str = "5cbd5f6e-72de-4cbb-8311-9bf9d62f6410";

async fn node(s: &Session, boot: &str) {
    node_with_storage(s, "node", boot, Some(STORAGE_A)).await;
}
async fn node_with_storage(s: &Session, name: &str, boot: &str, storage: Option<&str>) {
    s.register_session(
        Node {
            id: name.into(),
            runtime_classes: vec!["runsc".into()],
            capacity: Resources {
                cpu_millis: 100,
                memory_bytes: 100,
                disk_bytes: 0,
            },
            available: true,
            labels: storage
                .map(|id| ("ADX_SHARED_VOLUME_STORAGE".into(), id.to_owned()))
                .into_iter()
                .collect(),
            devices: vec![],
        },
        "127.0.0.1:1".into(),
        "127.0.0.1:2".into(),
        Some(NodeSession {
            id: boot.into(),
            sequence: 1,
            routable: true,
        }),
    )
    .await
    .unwrap();
}
async fn volume(s: &Session, owner: &str) -> SharedVolumeId {
    s.create_shared_volume(
        owner,
        &SharedVolumeId::parse(uuid::Uuid::new_v4().to_string()).unwrap(),
    )
    .await
    .unwrap()
    .permissions
    .id()
    .clone()
}
fn mount(id: SharedVolumeId, target: &str) -> SharedVolumeMount {
    SharedVolumeMount {
        volume_id: id,
        subpath: SharedVolumeSubpath::default(),
        target: target.into(),
        access: SharedVolumeAccess::ReadWrite,
    }
}
async fn record(s: &Session, name: &str, mounts: Vec<SharedVolumeMount>) -> EnvironmentRecord {
    record_on(s, name, "node", mounts).await
}
async fn record_on(
    s: &Session,
    name: &str,
    node_id: &str,
    mounts: Vec<SharedVolumeMount>,
) -> EnvironmentRecord {
    let generation = s.snapshot().await.unwrap().generation + 1;
    let spec = EnvironmentSpec {
        id: name.into(),
        tenant_id: "owner".into(),
        image: "image".into(),
        runtime_class: "runsc".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        priority: 0,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        execution: adx_core::execution::ExecutionOptions {
            shared_volumes: mounts,
            ..Default::default()
        },
    };
    let assignment = Assignment {
        environment_id: name.into(),
        node_id: node_id.into(),
        shard_id: 0,
        generation,
        devices: vec![],
    };
    s.reserve(spec.clone(), assignment.clone()).await.unwrap();
    EnvironmentRecord {
        spec,
        assignment,
        runtime: Runtime {
            id: format!("{name}-{generation}"),
            ip: None,
        },
        state: EnvironmentState::Starting,
        revision: 1,
        resources_held: true,
        checkpoint: None,
        checkpoint_readers: CheckpointReaders::known_empty(),
        restart_attempts: 0,
        restart_pending: false,
        failure: None,
        last_operation: None,
    }
}
fn receipts(r: &EnvironmentRecord) -> Vec<SharedVolumeAttachmentRetirement> {
    attachments_for_record(r)
        .unwrap()
        .into_iter()
        .map(|attachment| SharedVolumeAttachmentRetirement {
            attachment,
            backend_receipt: "fixture-only:execution-stop".into(),
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn batch_admission_is_all_or_none_when_one_volume_is_denied() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let a = volume(&s, "owner").await;
    let b = volume(&s, "other").await;
    let r = record(&s, "a", vec![mount(a.clone(), "/one"), mount(b, "/two")]).await;
    assert_eq!(
        s.reserve_shared_volumes(&r, "boot").await,
        Err(Error::NotFound)
    );
    assert!(s
        .get_shared_volume(&a, "owner")
        .await
        .unwrap()
        .attachments
        .is_empty());
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn stop_before_admission_tombstones_the_late_request() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let v = volume(&s, "owner").await;
    let r = record(&s, "a", vec![mount(v.clone(), "/shared")]).await;
    let proof = receipts(&r);
    s.release_shared_volumes(&proof, "boot").await.unwrap();
    assert_eq!(
        s.reserve_shared_volumes(&r, "boot").await,
        Err(Error::Conflict)
    );
    let stored = s.get_shared_volume(&v, "owner").await.unwrap();
    assert!(stored.attachments.is_empty());
    assert_eq!(stored.retirements, proof);
    s.release_shared_volumes(&proof, "boot").await.unwrap();
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn concurrent_stop_and_admission_always_end_retired_not_attached() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    for i in 0..8 {
        let v = volume(&s, "owner").await;
        let r = record(&s, &format!("race{i}"), vec![mount(v.clone(), "/shared")]).await;
        let proof = receipts(&r);
        let (admit, stop) = tokio::join!(
            s.reserve_shared_volumes(&r, "boot"),
            s.release_shared_volumes(&proof, "boot")
        );
        assert!(admit.is_ok() || matches!(admit, Err(Error::Conflict)));
        stop.unwrap();
        let stored = s.get_shared_volume(&v, "owner").await.unwrap();
        assert!(stored.attachments.is_empty());
        assert_eq!(stored.retirements, proof);
        assert_eq!(
            s.reserve_shared_volumes(&r, "boot").await,
            Err(Error::Conflict)
        );
    }
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn an_atomic_multi_volume_stop_allows_only_the_next_execution() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let mounts = vec![
        mount(volume(&s, "owner").await, "/one"),
        mount(volume(&s, "owner").await, "/two"),
    ];
    let a = record(&s, "a", mounts.clone()).await;
    let b = record(&s, "b", mounts).await;
    let held = s.reserve_shared_volumes(&a, "boot").await.unwrap();
    assert_eq!(held.len(), 2);
    assert_eq!(
        s.reserve_shared_volumes(&b, "boot").await,
        Err(Error::Conflict)
    );
    s.release_shared_volumes(&receipts(&a), "boot")
        .await
        .unwrap();
    assert_eq!(
        s.reserve_shared_volumes(&a, "boot").await,
        Err(Error::Conflict)
    );
    assert_eq!(
        s.reserve_shared_volumes(&b, "boot").await.unwrap(),
        attachments_for_record(&b).unwrap()
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn node_session_and_coordinator_restart_preserve_old_occupancy() {
    let mut rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let r = record(&s, "a", vec![mount(volume(&s, "owner").await, "/shared")]).await;
    let held = s.reserve_shared_volumes(&r, "boot").await.unwrap();
    node(&s, "next-boot").await;
    assert_eq!(
        s.node_shared_volume_attachments("node", "boot").await,
        Err(Error::Conflict)
    );
    assert_eq!(
        s.node_shared_volume_attachments("node", "next-boot")
            .await
            .unwrap(),
        held
    );
    rig.crash();
    rig.start().await;
    let next = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        next.node_shared_volume_attachments("node", "next-boot")
            .await
            .unwrap(),
        held
    );
    assert!(s
        .release_shared_volumes(&receipts(&r), "boot")
        .await
        .is_err());
    next.release_shared_volumes(&receipts(&r), "next-boot")
        .await
        .unwrap();
    assert!(next
        .node_shared_volume_attachments("node", "next-boot")
        .await
        .unwrap()
        .is_empty());
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn forged_canonical_spec_or_mixed_execution_retirement_is_rejected() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let r = record(
        &s,
        "a",
        vec![
            mount(volume(&s, "owner").await, "/one"),
            mount(volume(&s, "owner").await, "/two"),
        ],
    )
    .await;
    let mut forged = r.clone();
    forged.spec.execution.shared_volumes[0].target = "/forged".into();
    assert_eq!(
        s.reserve_shared_volumes(&forged, "boot").await,
        Err(Error::Conflict)
    );
    let mut held = s.reserve_shared_volumes(&r, "boot").await.unwrap();
    held.sort_by_key(|a| a.volume_id.as_str().to_owned());
    let mut proof = receipts(&r);
    proof[1].attachment.identity.runtime_id = format!("{}-r99", r.runtime.id);
    assert!(s.release_shared_volumes(&proof, "boot").await.is_err());
    assert_eq!(
        s.node_shared_volume_attachments("node", "boot")
            .await
            .unwrap(),
        held
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn idempotent_admission_never_bypasses_current_permission() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let id = volume(&s, "other").await;
    s.grant_shared_volume(
        &id,
        "other",
        1,
        adx_core::shared_volumes::SharedVolumeGrant {
            tenant_id: "owner".into(),
            subpath: Default::default(),
            access: SharedVolumeAccess::ReadWrite,
        },
    )
    .await
    .unwrap();
    let r = record(&s, "a", vec![mount(id.clone(), "/shared")]).await;
    let held = s.reserve_shared_volumes(&r, "boot").await.unwrap();
    s.revoke_shared_volume(&id, "other", 2, "owner", &Default::default())
        .await
        .unwrap();
    assert_eq!(
        s.reserve_shared_volumes(&r, "boot").await,
        Err(Error::NotFound)
    );
    assert_eq!(
        s.node_shared_volume_attachments("node", "boot")
            .await
            .unwrap(),
        held
    );
    s.release_shared_volumes(&receipts(&r), "boot")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn storage_binding_survives_restart_and_blocks_drift_even_after_retirement() {
    let mut rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let v = volume(&s, "owner").await;
    let first = record(&s, "first", vec![mount(v.clone(), "/shared")]).await;
    s.reserve_shared_volumes(&first, "boot").await.unwrap();
    assert_eq!(
        s.get_shared_volume(&v, "owner")
            .await
            .unwrap()
            .storage_id
            .as_deref(),
        Some(STORAGE_A)
    );
    rig.crash();
    rig.start().await;
    let next = rig.store().await.begin(1).await.unwrap();
    node_with_storage(&next, "node", "changed", Some(STORAGE_B)).await;
    assert_eq!(
        next.reserve_shared_volumes(&first, "changed").await,
        Err(Error::Conflict)
    );
    next.release_shared_volumes(&receipts(&first), "changed")
        .await
        .unwrap();
    let stopped = next.get_shared_volume(&v, "owner").await.unwrap();
    assert!(stopped.attachments.is_empty());
    assert_eq!(stopped.storage_id.as_deref(), Some(STORAGE_A));
    let second = record(&next, "second", vec![mount(v.clone(), "/shared")]).await;
    assert_eq!(
        next.reserve_shared_volumes(&second, "changed").await,
        Err(Error::Conflict)
    );
    node_with_storage(&next, "node", "correct", Some(STORAGE_A)).await;
    next.reserve_shared_volumes(&second, "correct")
        .await
        .unwrap();
    next.release_shared_volumes(&receipts(&second), "correct")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn mismatched_store_does_not_partially_bind_an_atomic_volume_batch() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    node_with_storage(&s, "other", "other-boot", Some(STORAGE_B)).await;
    let fresh = volume(&s, "owner").await;
    let bound = volume(&s, "owner").await;
    let mut read = mount(bound.clone(), "/existing");
    read.access = SharedVolumeAccess::ReadOnly;
    let existing = record_on(&s, "existing", "other", vec![read.clone()]).await;
    s.reserve_shared_volumes(&existing, "other-boot")
        .await
        .unwrap();
    let before = s.get_shared_volume(&fresh, "owner").await.unwrap();
    let requested = record(&s, "batch", vec![mount(fresh.clone(), "/fresh"), read]).await;
    assert_eq!(
        s.reserve_shared_volumes(&requested, "boot").await,
        Err(Error::Conflict)
    );
    assert_eq!(s.get_shared_volume(&fresh, "owner").await.unwrap(), before);
    assert_eq!(
        s.get_shared_volume(&bound, "owner")
            .await
            .unwrap()
            .attachments
            .len(),
        1
    );
    s.release_shared_volumes(&receipts(&existing), "other-boot")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn admission_requires_the_current_nodes_verified_storage_label() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node_with_storage(&s, "node", "boot", None).await;
    let v = volume(&s, "owner").await;
    let r = record(&s, "unlabelled", vec![mount(v.clone(), "/shared")]).await;
    assert!(matches!(
        s.reserve_shared_volumes(&r, "boot").await,
        Err(Error::Unavailable(_))
    ));
    let stored = s.get_shared_volume(&v, "owner").await.unwrap();
    assert!(stored.storage_id.is_none() && stored.attachments.is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn competing_readers_in_different_stores_have_one_immutable_binding_winner() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    node_with_storage(&s, "other", "other-boot", Some(STORAGE_B)).await;
    let v = volume(&s, "owner").await;
    let mut read = mount(v.clone(), "/shared");
    read.access = SharedVolumeAccess::ReadOnly;
    let first = record(&s, "first", vec![read.clone()]).await;
    let second = record_on(&s, "second", "other", vec![read]).await;
    let (a, b) = tokio::join!(
        s.reserve_shared_volumes(&first, "boot"),
        s.reserve_shared_volumes(&second, "other-boot")
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(a.is_ok() || matches!(a, Err(Error::Conflict)));
    assert!(b.is_ok() || matches!(b, Err(Error::Conflict)));
    let (winner, boot, storage) = if a.is_ok() {
        (&first, "boot", STORAGE_A)
    } else {
        (&second, "other-boot", STORAGE_B)
    };
    let stored = s.get_shared_volume(&v, "owner").await.unwrap();
    assert_eq!(stored.storage_id.as_deref(), Some(storage));
    assert_eq!(stored.attachments, attachments_for_record(winner).unwrap());
    s.reserve_shared_volumes(winner, boot).await.unwrap();
    assert_eq!(s.get_shared_volume(&v, "owner").await.unwrap(), stored);
    s.release_shared_volumes(&receipts(winner), boot)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn legacy_unbound_used_volume_rejects_admission_but_allows_exact_stop() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    node(&s, "boot").await;
    let v = volume(&s, "owner").await;
    let first = record(&s, "legacy", vec![mount(v.clone(), "/shared")]).await;
    s.reserve_shared_volumes(&first, "boot").await.unwrap();
    let mut legacy = serde_json::to_value(s.get_shared_volume(&v, "owner").await.unwrap()).unwrap();
    legacy.as_object_mut().unwrap().remove("storage_id");
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: usize = redis::cmd("HSET")
        .arg("adx:{test}:control:v1:shared-volumes")
        .arg(format!("volume:{}", v.as_str()))
        .arg(legacy.to_string())
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(s
        .get_shared_volume(&v, "owner")
        .await
        .unwrap()
        .storage_id
        .is_none());
    let second = record(&s, "replacement", vec![mount(v.clone(), "/shared")]).await;
    assert!(matches!(
        s.reserve_shared_volumes(&second, "boot").await,
        Err(Error::Unavailable(_))
    ));
    s.release_shared_volumes(&receipts(&first), "boot")
        .await
        .unwrap();
    let retired = s.get_shared_volume(&v, "owner").await.unwrap();
    assert!(retired.storage_id.is_none() && retired.attachments.is_empty());
    assert_eq!(retired.retirements, receipts(&first));
    assert!(matches!(
        s.reserve_shared_volumes(&second, "boot").await,
        Err(Error::Unavailable(_))
    ));
}
