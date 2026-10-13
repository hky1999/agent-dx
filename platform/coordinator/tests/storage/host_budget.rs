use super::*;
use adx_coordinator::storage::{ClaimOutcome, HostBudget, LocalClaim, NodeSession, Session};
use std::collections::BTreeMap;

fn budgets() -> BTreeMap<String, HostBudget> {
    BTreeMap::from([(
        "host".into(),
        HostBudget {
            capacity: spec("capacity").resources,
            nodes: ["first".into(), "second".into()].into(),
        },
    )])
}

async fn setup() -> (common::Redis, Session) {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session.configure_host_budgets(budgets()).await.unwrap();
    for id in ["first", "second", "unmapped"] {
        session
            .register_session(
                node(id),
                format!("{id}:1"),
                format!("{id}:2"),
                Some(NodeSession {
                    id: format!("boot-{id}"),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
    }
    (rig, session)
}
fn assignment(id: &str, node: &str, generation: u64) -> Assignment {
    Assignment {
        environment_id: id.into(),
        node_id: node.into(),
        shard_id: 0,
        generation,
        devices: vec![],
    }
}
fn candidate(node: &str) -> LocalClaim {
    LocalClaim {
        node_id: node.into(),
        node_session_id: format!("boot-{node}"),
        devices: vec![],
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_central_create_counts_shared_host_once() {
    let (_rig, session) = setup().await;
    session
        .reserve(spec("a"), assignment("a", "first", 1))
        .await
        .unwrap();
    assert_eq!(
        session
            .reserve(spec("b"), assignment("b", "second", 2))
            .await,
        Err(Error::NoCapacity),
        "a second node is not another physical capacity pool",
    );
    let status = session.host_budgets().await.unwrap();
    assert_eq!(status["host"].held, spec("a").resources);
    assert_eq!(status["host"].available, Resources::default());
    session
        .reserve(spec("a"), assignment("a", "first", 1))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_racing_claims_and_batch_share_central_reservation() {
    let (_rig, session) = setup().await;
    let first = candidate("first");
    let second = candidate("second");
    let (a, b) = tokio::join!(
        session.claim(spec("a"), &first),
        session.claim(spec("b"), &second),
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(a, Err(Error::NoCapacity)) || matches!(b, Err(Error::NoCapacity)));
    assert_eq!(
        session
            .reserve(spec("central"), assignment("central", "first", 99))
            .await,
        Err(Error::NoCapacity)
    );
    let (_rig2, session2) = setup().await;
    let batch = session2
        .claim_batch(vec![
            (spec("x"), first.clone()),
            (spec("y"), second),
            (spec("x"), first),
        ])
        .await
        .unwrap();
    assert!(matches!(&batch[0], Ok(ClaimOutcome::Owned(_))));
    assert_eq!(batch[1], Err(Error::NoCapacity));
    assert_eq!(
        batch[0], batch[2],
        "same-ID retry consumes no second reservation"
    );
    assert_eq!(
        session2.host_budgets().await.unwrap()["host"].held,
        spec("x").resources
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_unknown_hold_survives_expiry_and_redis_restart() {
    let (mut rig, session) = setup().await;
    session
        .reserve(spec("a"), assignment("a", "first", 1))
        .await
        .unwrap();
    session
        .invalidate_node("first", "boot-first")
        .await
        .unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].available,
        Resources::default()
    );
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted.host_budgets().await.unwrap()["host"].held,
        spec("a").resources
    );
    assert_eq!(
        restarted.claim(spec("b"), &candidate("second")).await,
        Err(Error::NoCapacity)
    );
    let mut retired = restarted.get("a").await.unwrap().result.unwrap();
    retired.resources_held = false;
    retired.revision += 1;
    restarted.commit(retired.clone()).await.unwrap();
    restarted.commit(retired).await.unwrap();
    assert_eq!(
        restarted.host_budgets().await.unwrap()["host"].available,
        spec("a").resources
    );
    restarted
        .claim(spec("b"), &candidate("second"))
        .await
        .unwrap();
    let revision = restarted.revision().await.unwrap();
    // The first request may encounter the connection broken by the real Redis
    // restart. It must fail; after reconnect the original epoch must conflict.
    let first = session.configure_host_budgets(budgets()).await;
    if matches!(first, Err(Error::Unavailable(_))) {
        assert_eq!(
            session.configure_host_budgets(budgets()).await,
            Err(Error::Conflict)
        );
    } else {
        assert_eq!(first, Err(Error::Conflict));
    }
    assert_eq!(restarted.revision().await.unwrap(), revision);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_rejects_untrusted_members_and_execution_limits() {
    let (_rig, session) = setup().await;
    assert!(session
        .claim(spec("unmapped"), &candidate("unmapped"))
        .await
        .is_err());
    let mut raised = spec("raised");
    raised.sandbox.limits.memory_bytes = 256;
    assert_eq!(
        session
            .reserve(raised, assignment("raised", "first", 1))
            .await,
        Err(Error::NoCapacity)
    );
    assert_eq!(session.snapshot().await.unwrap().environments.len(), 0);
    session.configure_host_budgets(budgets()).await.unwrap();
    let mut changed = budgets();
    changed.get_mut("host").unwrap().capacity.memory_bytes += 1;
    assert_eq!(
        session.configure_host_budgets(changed).await,
        Err(Error::Conflict)
    );
    let mut duplicate = budgets();
    duplicate.insert("duplicate".into(), duplicate["host"].clone());
    assert!(matches!(
        session.configure_host_budgets(duplicate).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_batch_counts_holds_outside_the_batch() {
    let (_rig, session) = setup().await;
    session
        .reserve(spec("outside"), assignment("outside", "first", 1))
        .await
        .unwrap();
    let before = session.snapshot().await.unwrap();
    let result = session
        .claim_batch(vec![(spec("new"), candidate("second"))])
        .await
        .unwrap();
    assert_eq!(result, vec![Err(Error::NoCapacity)]);
    assert_eq!(session.snapshot().await.unwrap(), before);
}

async fn retire(session: &Session, id: &str) {
    let stored = session.get(id).await.unwrap();
    let mut result = stored
        .result
        .unwrap_or_else(|| running(stored.spec, stored.assignment));
    result.state = EnvironmentState::Failed;
    result.runtime.ip = None;
    result.resources_held = false;
    result.revision += 1;
    session.commit(result).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_replacement_and_recovery_reserve_target_before_dispatch() {
    for recovering in [false, true] {
        let (_rig, session) = setup().await;
        let original = assignment("source", "first", 1);
        session
            .reserve(spec("source"), original.clone())
            .await
            .unwrap();
        if recovering {
            let mut running = running(spec("source"), original.clone());
            running.checkpoint = Some(adx_core::RestorePoint {
                id: "shared".into(),
                origin: None,
                expires_at_unix_seconds: u64::MAX,
                source_runtime_id: running.runtime.id.clone(),
                artifact: adx_core::CheckpointArtifact {
                    storage: "shared".into(),
                    location: "artifact".into(),
                    size_bytes: 8,
                },
            });
            session.commit(running).await.unwrap();
            session
                .invalidate_node("first", "boot-first")
                .await
                .unwrap();
        }
        retire(&session, "source").await;
        session
            .claim(spec("neighbor"), &candidate("second"))
            .await
            .unwrap();
        let replacement = assignment("source", "second", 3);
        let blocked = if recovering {
            session
                .reserve_recovery(&original, replacement.clone(), 1)
                .await
        } else {
            session
                .replace_rejected(&original, replacement.clone())
                .await
        };
        assert_eq!(blocked, Err(Error::NoCapacity));
        assert_eq!(session.get("source").await.unwrap().assignment, original);
        retire(&session, "neighbor").await;
        let granted = if recovering {
            session
                .reserve_recovery(&original, replacement.clone(), 1)
                .await
        } else {
            session
                .replace_rejected(&original, replacement.clone())
                .await
        }
        .unwrap();
        assert!(granted.resources_held());
        assert_eq!(granted.assignment, replacement);
        assert_eq!(
            session.host_budgets().await.unwrap()["host"].available,
            Resources::default()
        );
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_checked_integer_arithmetic_rejects_overflow() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let mut config = budgets();
    config.get_mut("host").unwrap().capacity = Resources {
        cpu_millis: 2,
        memory_bytes: u64::MAX,
        disk_bytes: u64::MAX,
    };
    session.configure_host_budgets(config).await.unwrap();
    session
        .register_session(
            node("first"),
            "first:1".into(),
            "first:2".into(),
            Some(NodeSession {
                id: "boot-first".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let mut huge = spec("huge");
    huge.resources = Resources {
        cpu_millis: 1,
        memory_bytes: u64::MAX / 2 + 1,
        disk_bytes: 0,
    };
    session
        .claim(huge.clone(), &candidate("first"))
        .await
        .unwrap();
    huge.id = "overflow".into();
    assert_eq!(
        session.claim(huge, &candidate("first")).await,
        Err(Error::NoCapacity)
    );
    assert_eq!(
        session.host_budgets().await.unwrap()["host"]
            .available
            .memory_bytes,
        u64::MAX / 2
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_upgrade_checks_existing_holds_and_fences_old_schema() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "first").await;
    register(&session, "second").await;
    session
        .reserve(spec("a"), assignment("a", "first", 1))
        .await
        .unwrap();
    session
        .reserve(spec("b"), assignment("b", "second", 2))
        .await
        .unwrap();
    assert_eq!(
        session.configure_host_budgets(budgets()).await,
        Err(Error::NoCapacity)
    );
    assert!(session.host_budgets().await.unwrap().is_empty());
    retire(&session, "b").await;
    session.configure_host_budgets(budgets()).await.unwrap();
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let raw: String = redis::cmd("HGET")
        .arg("adx:{test}:control:v1")
        .arg("header")
        .query_async(&mut connection)
        .await
        .unwrap();
    let header: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        header["schema"], 2,
        "schema 1 writers must reject this namespace"
    );
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted.host_budgets().await.unwrap()["host"].held,
        spec("a").resources
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_result_commit_cannot_silently_reacquire_capacity() {
    let (_rig, session) = setup().await;
    let allocation = assignment("paused", "first", 1);
    session
        .reserve(spec("paused"), allocation.clone())
        .await
        .unwrap();
    let mut result = running(spec("paused"), allocation);
    session.commit(result.clone()).await.unwrap();
    result.state = EnvironmentState::Paused;
    result.revision += 1;
    result.resources_held = false;
    result.runtime.ip = None;
    result.checkpoint = Some(adx_core::RestorePoint {
        id: "checkpoint".into(),
        origin: None,
        expires_at_unix_seconds: u64::MAX,
        source_runtime_id: result.runtime.id.clone(),
        artifact: adx_core::CheckpointArtifact {
            storage: "local".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    session.commit(result.clone()).await.unwrap();
    session
        .claim(spec("neighbor"), &candidate("second"))
        .await
        .unwrap();
    let paused = result.clone();
    result.state = EnvironmentState::Running;
    result.resources_held = true;
    result.runtime.id = format!("{}-r{}", result.runtime.id, result.revision);
    result.runtime.ip = Some("10.0.0.3".parse().unwrap());
    result.last_operation = Some(adx_core::CompletedOperation {
        id: "resume".into(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: result.revision,
    });
    result.revision += 1;
    assert_eq!(session.commit(result).await, Err(Error::NoCapacity));
    assert_eq!(session.get("paused").await.unwrap().result, Some(paused));
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        spec("neighbor").resources
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_configuration_race_cannot_publish_an_overdrawn_pool() {
    for _ in 0..8 {
        let rig = common::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        for id in ["first", "second"] {
            session
                .register_session(
                    node(id),
                    format!("{id}:1"),
                    format!("{id}:2"),
                    Some(NodeSession {
                        id: format!("boot-{id}"),
                        sequence: 1,
                        routable: true,
                    }),
                )
                .await
                .unwrap();
        }
        let first = candidate("first");
        let second = candidate("second");
        let (configured, a, b) = tokio::join!(
            session.configure_host_budgets(budgets()),
            session.claim(spec("a"), &first),
            session.claim(spec("b"), &second),
        );
        let succeeded = usize::from(a.is_ok()) + usize::from(b.is_ok());
        if configured.is_ok() {
            assert_eq!(
                succeeded, 1,
                "a narrow pre-configuration read cannot bypass the installed budget"
            );
            assert_eq!(
                session.host_budgets().await.unwrap()["host"].held,
                spec("a").resources
            );
        } else {
            assert_eq!(configured, Err(Error::NoCapacity));
            assert_eq!(
                succeeded, 2,
                "configuration can only fail here if both legacy claims already won"
            );
            assert!(
                session.host_budgets().await.unwrap().is_empty(),
                "failed installation must leave no partial budget"
            );
        }
        assert!(session.snapshot().await.is_ok());
    }
}
