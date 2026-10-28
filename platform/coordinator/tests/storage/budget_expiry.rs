use super::*;

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_expiry_budget_keeps_unknown_execution_across_redis_restart() {
    for started in [false, true] {
        let mut rig = common::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        session
            .register_session(
                node("lost"),
                "lost:1".into(),
                "lost:2".into(),
                Some(adx_coordinator::storage::NodeSession {
                    id: "original-session".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        let assignment = Assignment {
            environment_id: "unknown".into(),
            node_id: "lost".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        session
            .reserve(spec("unknown"), assignment.clone())
            .await
            .unwrap();
        if started {
            session
                .commit(running(spec("unknown"), assignment.clone()))
                .await
                .unwrap();
        }
        session
            .invalidate_node("lost", "original-session")
            .await
            .unwrap();
        let expired = session.get("unknown").await.unwrap();
        assert!(expired.invalidated);
        assert!(
            expired.resources_held(),
            "loss of contact is not physical retirement"
        );
        assert!(expired.result.as_ref().unwrap().resources_held);
        assert_eq!(
            session
                .retire_invalidated_failed(&expired, u64::MAX)
                .await
                .unwrap_err(),
            Error::Conflict,
        );
        rig.crash();
        rig.start().await;
        let restarted = rig.store().await.begin(1).await.unwrap();
        let recovered = restarted.get("unknown").await.unwrap();
        assert!(
            recovered.resources_held(),
            "Redis restart must retain the original hold"
        );
        let scheduler =
            Coordinator::restore(&restarted.snapshot().await.unwrap(), Placement::Pack).unwrap();
        assert!(scheduler.snapshot().environments().contains_key("unknown"));
        let mut uncertain = recovered.result.unwrap();
        uncertain.revision += 1;
        restarted.commit(uncertain.clone()).await.unwrap();
        assert!(restarted.get("unknown").await.unwrap().resources_held());
        let mut wrong_owner = uncertain.clone();
        wrong_owner.assignment.generation += 1;
        wrong_owner.resources_held = false;
        wrong_owner.revision += 1;
        assert_eq!(restarted.commit(wrong_owner).await, Err(Error::Conflict));
        uncertain.resources_held = false;
        uncertain.revision += 1;
        restarted.commit(uncertain).await.unwrap();
        assert!(!restarted.get("unknown").await.unwrap().resources_held());
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_expiry_budget_blocks_recovery_until_source_retires() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .register_session(
            node("source"),
            "source:1".into(),
            "source:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "original-session".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    register(&session, "target").await;
    let assignment = Assignment {
        environment_id: "unknown".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("unknown"), assignment.clone())
        .await
        .unwrap();
    let mut record = running(spec("unknown"), assignment.clone());
    record.checkpoint = Some(adx_core::RestorePoint {
        source_resources: None,
        id: "shared-point".into(),
        origin: None,
        expires_at_unix_seconds: u64::MAX,
        source_runtime_id: record.runtime.id.clone(),
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    session.commit(record).await.unwrap();
    session
        .invalidate_node("source", "original-session")
        .await
        .unwrap();
    let replacement = Assignment {
        node_id: "target".into(),
        generation: 2,
        ..assignment.clone()
    };
    assert_eq!(
        session
            .reserve_recovery(&assignment, replacement.clone(), 1)
            .await,
        Err(Error::Conflict),
        "a checkpoint does not prove that its source execution has retired",
    );
    let mut retired = session.get("unknown").await.unwrap().result.unwrap();
    retired.resources_held = false;
    retired.revision += 1;
    session.commit(retired).await.unwrap();
    let held = session
        .reserve_recovery(&assignment, replacement, 1)
        .await
        .unwrap();
    assert!(held.resources_held());
    session
        .register_session(
            node("target"),
            "target:1".into(),
            "target:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "recovery-session".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    session
        .invalidate_node("target", "recovery-session")
        .await
        .unwrap();
    assert!(
        session.get("unknown").await.unwrap().resources_held(),
        "an unacknowledged recovery dispatch must retain its reservation on expiry",
    );
}
