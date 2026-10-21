use super::*;
use adx_coordinator::storage::{NodeSession, Session};
use adx_core::{CheckpointReaderRetirement, CheckpointReaders, RestorePoint};

async fn fixture(session: &Session, state: EnvironmentState) -> EnvironmentRecord {
    session
        .register_session(
            node("source"),
            "source:1".into(),
            "source:2".into(),
            Some(NodeSession {
                id: "source-boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let assignment = Assignment {
        environment_id: "reader".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("reader"), assignment.clone())
        .await
        .unwrap();
    let mut paused = running(spec("reader"), assignment);
    paused.revision = 4;
    if state == EnvironmentState::Paused {
        paused.state = state;
        paused.resources_held = false;
        paused.runtime.ip = None;
    } else {
        assert_eq!(state, EnvironmentState::Running);
    }
    paused.checkpoint_readers = CheckpointReaders::known_empty();
    paused.checkpoint = Some(RestorePoint {
        id: "a".into(),
        source_runtime_id: "reader-1".into(),
        origin: None,
        expires_at_unix_seconds: None,
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "a".into(),
            size_bytes: 8,
        },
    });
    session.commit(paused.clone()).await.unwrap();
    paused
}

fn request(paused: &EnvironmentRecord) -> EnvironmentRecord {
    let mut record = paused.clone();
    record.state = EnvironmentState::Starting;
    record.revision += 1;
    record.resources_held = true;
    record.runtime.id = format!(
        "reader-{}-r{}",
        record.assignment.generation, record.revision
    );
    record
        .register_checkpoint_reader(record.checkpoint.clone().unwrap())
        .unwrap();
    record
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; no shared service"]
async fn reservation_survives_redis_and_coordinator_restart_before_runtime_result() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let paused = fixture(&session, EnvironmentState::Paused).await;
    let request = request(&paused);
    assert_eq!(
        session
            .reserve_checkpoint_reader(&request, "wrong-process")
            .await,
        Err(Error::Conflict)
    );
    let saved = session
        .reserve_checkpoint_reader(&request, "source-boot")
        .await
        .unwrap();
    assert_eq!(saved.result, Some(paused.clone()));
    assert!(saved.resources_held());
    assert_eq!(
        session
            .reserve_checkpoint_reader(&request, "source-boot")
            .await
            .unwrap(),
        saved
    );
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    let recovered = restarted.get("reader").await.unwrap();
    assert_eq!(recovered, saved);
    assert!(recovered
        .retained_checkpoint_points()
        .any(|point| Some(point) == paused.checkpoint.as_ref()));
    assert!(
        recovered
            .result
            .unwrap()
            .checkpoint_readers
            .readers
            .is_empty(),
        "reservation did not rewrite owner result"
    );
    assert_eq!(
        restarted.snapshot().await.unwrap().environments["reader"]
            .reader_reservations
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; no shared service"]
async fn ownership_transfer_keeps_reader_until_exact_authenticated_retirement() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let paused = fixture(&session, EnvironmentState::Paused).await;
    let request = request(&paused);
    let reader = request.current_checkpoint_reader_entry().unwrap().clone();
    session
        .reserve_checkpoint_reader(&request, "source-boot")
        .await
        .unwrap();
    session.request_resume(&paused.spec.id, 1).await.unwrap();
    session
        .invalidate_node("source", "source-boot")
        .await
        .unwrap();
    register(&session, "target").await;
    let replacement = Assignment {
        node_id: "target".into(),
        generation: 2,
        ..paused.assignment.clone()
    };
    let transferred = session
        .reserve_recovery(&paused.assignment, replacement, 1)
        .await
        .unwrap();
    assert_eq!(transferred.reader_reservations, vec![reader.clone()]);
    let owner_result = transferred.result.clone();
    session
        .register_session(
            node("source"),
            "source:1".into(),
            "source:2".into(),
            Some(NodeSession {
                id: "source-restarted".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let receipt = CheckpointReaderRetirement {
        reader: reader.clone(),
        backend_receipt: "test-only:backend-fenced-and-exited".into(),
    };
    assert_eq!(
        session
            .release_checkpoint_reader(&receipt, "source-boot")
            .await,
        Err(Error::Conflict)
    );
    let released = session
        .release_checkpoint_reader(&receipt, "source-restarted")
        .await
        .unwrap();
    assert!(released.reader_reservations.is_empty());
    assert_eq!(released.result, owner_result);
    assert_eq!(
        session
            .release_checkpoint_reader(&receipt, "source-restarted")
            .await
            .unwrap(),
        released
    );
    assert_eq!(
        session
            .reserve_checkpoint_reader(&request, "source-restarted")
            .await,
        Err(Error::Conflict)
    );
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted.get("reader").await.unwrap().reader_retirements,
        vec![receipt]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; no shared service"]
async fn failed_publish_local_rollback_reserves_before_replacing_running_result() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let running = fixture(&session, EnvironmentState::Running).await;
    let mut rollback = request(&running);
    rollback.runtime.ip = None;
    let point = rollback.checkpoint.as_mut().unwrap();
    point.id = "local-failed-upload".into();
    point.artifact.storage = "local".into();
    point.artifact.location = "node-local-staging".into();
    rollback.checkpoint_readers = CheckpointReaders::known_empty();
    rollback
        .register_checkpoint_reader(rollback.checkpoint.clone().unwrap())
        .unwrap();
    let saved = session
        .reserve_checkpoint_reader(&rollback, "source-boot")
        .await
        .unwrap();
    assert_eq!(saved.result, Some(running));
    assert_eq!(
        saved.reader_reservations[0].checkpoint.artifact.storage,
        "local"
    );
    let mut not_local = rollback.clone();
    not_local.runtime.id = "reader-1-r99".into();
    not_local.revision = 99;
    not_local.checkpoint_readers = CheckpointReaders::known_empty();
    not_local.checkpoint.as_mut().unwrap().artifact.storage = "unrelated-shared".into();
    not_local
        .register_checkpoint_reader(not_local.checkpoint.clone().unwrap())
        .unwrap();
    assert_eq!(
        session
            .reserve_checkpoint_reader(&not_local, "source-boot")
            .await,
        Err(Error::Conflict)
    );
}
