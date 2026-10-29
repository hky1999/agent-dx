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
                source_resources: None,
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
        header["schema"], 7,
        "writers without persistence confirmation must reject this namespace"
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
        source_resources: None,
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
    assert_eq!(session.commit(result).await, Err(Error::Conflict));
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

async fn paused_execution(session: &Session, id: &str) -> EnvironmentRecord {
    let allocation = match session.claim(spec(id), &candidate("first")).await.unwrap() {
        ClaimOutcome::Owned(stored) => stored.assignment,
        other => panic!("a fresh paused fixture must own its allocated generation: {other:?}"),
    };
    let mut result = running(spec(id), allocation);
    session.commit(result.clone()).await.unwrap();
    result.state = EnvironmentState::Paused;
    result.revision += 1;
    result.resources_held = false;
    result.runtime.ip = None;
    result.checkpoint = Some(adx_core::RestorePoint {
        source_resources: None,
        id: "checkpoint".into(),
        origin: None,
        expires_at_unix_seconds: u64::MAX,
        source_runtime_id: result.runtime.id.clone(),
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    result.last_operation = Some(adx_core::CompletedOperation {
        id: "pause-source".into(),
        kind: adx_core::LifecycleKind::Pause,
        expected_revision: result.revision - 1,
    });
    session.commit(result.clone()).await.unwrap();
    result
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_rejects_resume_without_prior_intent() {
    let (_rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    let mut result = source.clone();
    result.runtime.id = format!("{}-r{}", source.runtime.id, source.revision + 1);
    result.runtime.ip = Some("10.0.0.3".parse().unwrap());
    result.state = EnvironmentState::Running;
    result.resources_held = true;
    result.revision += 2;
    result.last_operation = Some(adx_core::CompletedOperation {
        id: "resume".into(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: source.revision,
    });
    assert_eq!(
        session.commit(result).await,
        Err(Error::Conflict),
        "free capacity is not evidence of pre-execution authorization"
    );
    assert_eq!(session.get("paused").await.unwrap().result, Some(source));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_rejects_unknown_restore_without_prior_intent() {
    let (_rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    let mut result = source.clone();
    result.runtime.id = format!("{}-r{}", source.runtime.id, source.revision + 1);
    result.state = EnvironmentState::Failed;
    result.resources_held = true;
    result.revision += 2;
    assert_eq!(
        session.commit(result).await,
        Err(Error::Conflict),
        "an unknown restore must already own its durable reservation"
    );
    assert_eq!(session.get("paused").await.unwrap().result, Some(source));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_rejects_implicit_runtime_replacement() {
    let (_rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    let mut retired = source.clone();
    retired.state = EnvironmentState::Failed;
    retired.revision += 1;
    session.commit(retired.clone()).await.unwrap();
    let mut result = retired.clone();
    result.runtime.id = format!("{}-r{}", source.runtime.id, retired.revision + 1);
    result.revision += 2;
    assert_eq!(
        session.commit(result).await,
        Err(Error::Conflict),
        "even a retired replacement must name an authorized attempt"
    );
    assert_eq!(session.get("paused").await.unwrap().result, Some(retired));
}

fn execution_request(source: EnvironmentRecord) -> adx_core::execution::ExecutionRequest {
    adx_core::execution::ExecutionRequest {
        expected_revision: source.revision,
        source,
        kind: adx_core::execution::ExecutionKind::Resume,
        operation_id: "resume-operation".into(),
        timeout_millis: 10_000,
    }
}

fn completed_execution(grant: &adx_core::execution::ExecutionAuthorization) -> EnvironmentRecord {
    let mut result = grant.target.clone();
    result.state = EnvironmentState::Running;
    result.revision += 1;
    result.runtime.ip = Some("10.0.0.3".parse().unwrap());
    result.last_operation = Some(adx_core::CompletedOperation {
        id: grant.request.operation_id.clone(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: grant.request.expected_revision,
    });
    result
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_reserves_before_result_and_replays_exactly() {
    let (rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    let request = execution_request(source.clone());
    let grant = session
        .authorize_execution(request.clone(), "boot-first", 1_000)
        .await
        .unwrap();
    let revision = session.revision().await.unwrap();
    assert_eq!(grant.target.runtime.id, "paused-1-r4");
    assert_eq!(grant.target.state, EnvironmentState::Resuming);
    assert_eq!(grant.deadline_unix_millis, 11_000);
    assert_eq!(
        session
            .authorize_execution(request.clone(), "boot-first", 2_000)
            .await
            .unwrap(),
        grant
    );
    assert_eq!(session.revision().await.unwrap(), revision);
    let stored = session.get("paused").await.unwrap();
    assert_eq!(stored.result, Some(source));
    assert!(stored.resources_held());
    assert!(stored.execution.unwrap().pending);
    assert_eq!(
        session.claim(spec("neighbor"), &candidate("second")).await,
        Err(Error::NoCapacity)
    );
    for change in [0, 1, 2] {
        let mut changed = request.clone();
        match change {
            0 => changed.operation_id.push('x'),
            1 => changed.timeout_millis += 1,
            _ => changed
                .source
                .checkpoint
                .as_mut()
                .unwrap()
                .artifact
                .location
                .push('x'),
        }
        assert_eq!(
            session
                .authorize_execution(changed, "boot-first", 2_000)
                .await,
            Err(Error::Conflict)
        );
    }
    assert_eq!(
        session
            .authorize_execution(request, "old-session", 2_000)
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(session.revision().await.unwrap(), revision);
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
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&raw).unwrap()["schema"],
        7
    );
    let result = completed_execution(&grant);
    session.commit(result.clone()).await.unwrap();
    session.commit(result).await.unwrap();
    assert!(
        !session
            .get("paused")
            .await
            .unwrap()
            .execution
            .unwrap()
            .pending
    );
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        spec("paused").resources
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_competes_atomically_with_new_create() {
    for _ in 0..4 {
        let (_rig, session) = setup().await;
        let source = paused_execution(&session, "paused").await;
        let request = execution_request(source);
        let candidate = candidate("second");
        let (resume, create) = tokio::join!(
            session.authorize_execution(request, "boot-first", 1_000),
            session.claim(spec("new"), &candidate),
        );
        assert_eq!(usize::from(resume.is_ok()) + usize::from(create.is_ok()), 1);
        assert!(
            matches!(resume, Err(Error::NoCapacity)) || matches!(create, Err(Error::NoCapacity))
        );
        assert_eq!(
            session.host_budgets().await.unwrap()["host"].held,
            spec("one").resources
        );
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_expiry_and_unknown_outcome_do_not_release() {
    let (_rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    let request = execution_request(source.clone());
    let grant = session
        .authorize_execution(request.clone(), "boot-first", 1_000)
        .await
        .unwrap();
    let expired = session
        .authorize_execution(request.clone(), "boot-first", 11_000)
        .await
        .unwrap();
    assert_eq!(expired, grant, "replay cannot extend the dispatch deadline");
    assert!(matches!(
        expired.check_dispatch(11_000),
        Err(Error::Unavailable(_))
    ));
    // Replaying the pre-dispatch source cannot settle the unknown target.
    session.commit(source.clone()).await.unwrap();
    assert!(
        session
            .get("paused")
            .await
            .unwrap()
            .execution
            .unwrap()
            .pending
    );
    let mut wrong = source;
    wrong.state = EnvironmentState::Failed;
    wrong.revision = 100;
    assert_eq!(session.commit(wrong).await, Err(Error::Conflict));
    let mut unknown = grant.target.clone();
    unknown.state = EnvironmentState::Failed;
    unknown.revision += 1;
    session.commit(unknown.clone()).await.unwrap();
    assert_eq!(
        session.claim(spec("neighbor"), &candidate("second")).await,
        Err(Error::NoCapacity)
    );
    unknown.resources_held = false;
    unknown.revision += 1;
    session.commit(unknown.clone()).await.unwrap();
    let revision = session.revision().await.unwrap();
    session.commit(unknown).await.unwrap();
    assert_eq!(session.revision().await.unwrap(), revision);
    assert_eq!(
        session
            .authorize_execution(request, "boot-first", 2_000)
            .await,
        Err(Error::Conflict)
    );
    let mut late = completed_execution(&grant);
    late.revision = 200;
    assert_eq!(session.commit(late).await, Err(Error::Conflict));
    session
        .claim(spec("neighbor"), &candidate("second"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_restart_and_invalidation_preserve_target_identity() {
    let (mut rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    let request = execution_request(source.clone());
    let grant = session
        .authorize_execution(request.clone(), "boot-first", 1_000)
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted
            .authorize_execution(request, "boot-first", 2_000)
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        restarted.host_budgets().await.unwrap()["host"].held,
        spec("paused").resources
    );
    let saved = restarted
        .invalidate_node("first", "boot-first")
        .await
        .unwrap();
    let target = &saved.environments["paused"];
    assert!(target.resources_held());
    let mut cleanup = target.result.clone().unwrap();
    assert_eq!(cleanup.runtime.id, grant.target.runtime.id);
    assert_ne!(cleanup.runtime.id, source.runtime.id);
    assert_eq!(cleanup.state, EnvironmentState::Failed);
    assert!(target.recovery_point(1).is_none());
    assert_eq!(
        restarted.commit(completed_execution(&grant)).await,
        Err(Error::Conflict)
    );
    cleanup.revision += 1;
    cleanup.resources_held = false;
    restarted.commit(cleanup.clone()).await.unwrap();
    restarted.commit(cleanup).await.unwrap();
    assert_eq!(
        restarted.host_budgets().await.unwrap()["host"].available,
        spec("one").resources
    );
    assert!(restarted
        .get("paused")
        .await
        .unwrap()
        .recovery_point(1)
        .is_some());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_rejects_wrong_completion_and_cleanup_identity() {
    let (_rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    let grant = session
        .authorize_execution(execution_request(source), "boot-first", 1_000)
        .await
        .unwrap();
    let before = session.snapshot().await.unwrap();
    for change in [0, 1, 2, 3, 4] {
        let mut result = completed_execution(&grant);
        match change {
            0 => result.last_operation.as_mut().unwrap().id.push('x'),
            1 => result.runtime.id = "paused-1-r999".into(),
            2 => result.assignment.generation += 1,
            3 => result
                .checkpoint
                .as_mut()
                .unwrap()
                .artifact
                .location
                .push('x'),
            _ => result.revision = grant.target.revision,
        }
        assert_eq!(session.commit(result).await, Err(Error::Conflict));
        assert_eq!(session.snapshot().await.unwrap(), before);
    }
    let mut rolled_back = grant.target;
    rolled_back.state = EnvironmentState::Paused;
    rolled_back.resources_held = false;
    rolled_back.revision += 1;
    session.commit(rolled_back.clone()).await.unwrap();
    let mut next = execution_request(rolled_back);
    next.operation_id = "retry-after-retirement".into();
    let next = session
        .authorize_execution(next, "boot-first", 2_000)
        .await
        .unwrap();
    assert_eq!(next.target.runtime.id, "paused-1-r6");
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_cross_node_recovery_counts_one_reservation() {
    let (_rig, session) = setup().await;
    let source = paused_execution(&session, "paused").await;
    session
        .invalidate_node("first", "boot-first")
        .await
        .unwrap();
    let restored = session
        .reserve_recovery(&source.assignment, assignment("paused", "second", 2), 1)
        .await
        .unwrap();
    let grant = session
        .authorize_execution(
            execution_request(restored.result.unwrap()),
            "boot-second",
            1_000,
        )
        .await
        .unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        spec("one").resources
    );
    let mut failed = grant.target;
    failed.state = EnvironmentState::Paused;
    failed.resources_held = false;
    failed.revision += 1;
    session.commit(failed).await.unwrap();
    let stored = session.get("paused").await.unwrap();
    assert!(!stored.resources_held());
    assert!(!stored.recovery.unwrap().pending);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn execution_authorization_restart_policy_and_checkpoint_replacement_are_distinct() {
    use adx_core::execution::ExecutionKind;
    for kind in [
        ExecutionKind::Restart,
        ExecutionKind::Reload,
        ExecutionKind::Failover,
        ExecutionKind::PauseRollback,
    ] {
        let (_rig, session) = setup().await;
        let mut request_spec = spec("replacement");
        request_spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
            max_attempts: 2,
            initial_backoff_seconds: 1,
            max_backoff_seconds: 2,
        });
        request_spec.sandbox.failover = true;
        let assignment = assignment("replacement", "first", 1);
        session
            .reserve(request_spec.clone(), assignment.clone())
            .await
            .unwrap();
        let mut source = running(request_spec, assignment);
        source.state = if kind == ExecutionKind::PauseRollback {
            EnvironmentState::Paused
        } else {
            EnvironmentState::Failed
        };
        source.runtime.ip = None;
        source.resources_held = false;
        source.restart_pending = kind == ExecutionKind::Restart;
        source.checkpoint = Some(adx_core::RestorePoint {
            source_resources: None,
            id: "checkpoint".into(),
            origin: None,
            expires_at_unix_seconds: u64::MAX,
            source_runtime_id: source.runtime.id.clone(),
            artifact: adx_core::CheckpointArtifact {
                storage: "shared".into(),
                location: "artifact".into(),
                size_bytes: 8,
            },
        });
        session.commit(source.clone()).await.unwrap();
        let mut request = execution_request(source);
        request.kind = kind;
        let grant = session
            .authorize_execution(request, "boot-first", 1_000)
            .await
            .unwrap();
        assert_eq!(
            grant.target.restart_attempts,
            u32::from(kind == ExecutionKind::Restart)
        );
        let mut result = completed_execution(&grant);
        if kind == ExecutionKind::Reload {
            result.last_operation.as_mut().unwrap().kind = adx_core::LifecycleKind::Reload;
        }
        session.commit(result).await.unwrap();
        assert_eq!(
            session.host_budgets().await.unwrap()["host"].held,
            spec("one").resources
        );
    }
}

async fn queued_setup() -> (common::Redis, Session) {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let mut configured = budgets();
    configured.get_mut("host").unwrap().capacity = Resources {
        cpu_millis: 200,
        memory_bytes: 256,
        disk_bytes: 0,
    };
    session.configure_host_budgets(configured).await.unwrap();
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
    (rig, session)
}
fn restore_peak() -> adx_coordinator::storage::OverheadReservation {
    adx_coordinator::storage::OverheadReservation {
        host_id: "host".into(),
        owner_sha256: "a".repeat(64),
        resources: Resources {
            cpu_millis: 10,
            memory_bytes: 16,
            disk_bytes: 0,
        },
        retirement_proof_sha256: None,
    }
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_peak_is_atomic_and_replays_original_deadline() {
    let (_rig, session) = queued_setup().await;
    let request = execution_request(paused_execution(&session, "queued").await);
    let grant = session
        .authorize_execution_with_peak(
            request.clone(),
            "boot-first",
            1000,
            "peak-one",
            restore_peak(),
        )
        .await
        .unwrap();
    let revision = session.revision().await.unwrap();
    assert_eq!(
        session
            .authorize_execution_with_peak(
                request.clone(),
                "boot-first",
                1500,
                "peak-one",
                restore_peak()
            )
            .await
            .unwrap(),
        grant
    );
    assert_eq!(revision, session.revision().await.unwrap());
    let normal = session
        .authorize_execution(request.clone(), "boot-first", 2000)
        .await
        .unwrap();
    assert_eq!(normal, grant);
    let accepted_revision = session.revision().await.unwrap();
    assert_eq!(
        accepted_revision,
        revision + 1,
        "node acceptance is one durable CAS"
    );
    assert!(
        session
            .get("queued")
            .await
            .unwrap()
            .execution
            .unwrap()
            .peak
            .unwrap()
            .node_accepted
    );
    assert_eq!(
        session
            .authorize_execution(request.clone(), "boot-first", 3000)
            .await
            .unwrap(),
        grant
    );
    assert_eq!(
        session
            .authorize_execution_with_peak(request, "boot-first", 5000, "peak-one", restore_peak())
            .await
            .unwrap(),
        grant
    );
    assert_eq!(accepted_revision, session.revision().await.unwrap());
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        Resources {
            cpu_millis: 110,
            memory_bytes: 144,
            disk_bytes: 0
        }
    );
    assert_eq!(
        session
            .retire_overhead("peak-one", &"a".repeat(64), &"b".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    session.commit(completed_execution(&grant)).await.unwrap();
    session
        .retire_overhead("peak-one", &"a".repeat(64), &"b".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        spec("queued").resources
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_budget_refusal_leaves_neither_vm_nor_peak() {
    let (_rig, session) = queued_setup().await;
    let source = paused_execution(&session, "queued").await;
    let mut too_large = restore_peak();
    too_large.resources.memory_bytes = 256;
    let before = session.revision().await.unwrap();
    assert_eq!(
        session
            .authorize_execution_with_peak(
                execution_request(source),
                "boot-first",
                1000,
                "too-large",
                too_large
            )
            .await,
        Err(Error::NoCapacity)
    );
    assert_eq!(session.revision().await.unwrap(), before);
    assert!(session.overhead_reservations().await.unwrap().is_empty());
    assert!(session.get("queued").await.unwrap().execution.is_none());
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        Resources::default()
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_racing_peaks_cannot_oversubscribe_or_split_grants() {
    let (_rig, session) = queued_setup().await;
    let a = execution_request(paused_execution(&session, "one").await);
    let b = execution_request(paused_execution(&session, "two").await);
    let (a, b) = tokio::join!(
        session.authorize_execution_with_peak(a, "boot-first", 1000, "peak-a", restore_peak()),
        session.authorize_execution_with_peak(b, "boot-first", 1000, "peak-b", restore_peak())
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(a, Err(Error::NoCapacity)) || matches!(b, Err(Error::NoCapacity)));
    assert_eq!(session.overhead_reservations().await.unwrap().len(), 1);
    assert_eq!(
        session.host_budgets().await.unwrap()["host"]
            .held
            .cpu_millis,
        110
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_restart_preserves_peak_and_fences_old_epoch() {
    let (rig, session) = queued_setup().await;
    let request = execution_request(paused_execution(&session, "queued").await);
    session
        .authorize_execution_with_peak(request.clone(), "boot-first", 1000, "peak", restore_peak())
        .await
        .unwrap();
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted.host_budgets().await.unwrap()["host"]
            .held
            .cpu_millis,
        110
    );
    assert_eq!(
        session
            .authorize_execution(request, "boot-first", 2000)
            .await,
        Err(Error::Conflict)
    );
    assert!(restarted.overhead_reservations().await.unwrap()["peak"]
        .retirement_proof_sha256
        .is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_cancel_before_node_dispatch_keeps_peak_until_cleanup() {
    let (_rig, session) = queued_setup().await;
    let request = execution_request(paused_execution(&session, "cancel-queued").await);
    session
        .authorize_execution_with_peak(
            request.clone(),
            "boot-first",
            1000,
            "peak-cancel",
            restore_peak(),
        )
        .await
        .unwrap();
    let mut deleted = request.source;
    deleted.state = EnvironmentState::Deleted;
    deleted.revision += 2;
    deleted.checkpoint = None;
    deleted.last_operation = None;
    session.commit(deleted).await.unwrap();
    assert_eq!(session.get("cancel-queued").await, Err(Error::NotFound));
    assert_eq!(
        session.host_budgets().await.unwrap()["host"]
            .held
            .cpu_millis,
        10,
        "only the unconsumed VM authorization is closed by the owned deletion"
    );
    assert!(
        session.overhead_reservations().await.unwrap()["peak-cancel"]
            .retirement_proof_sha256
            .is_none()
    );
    session
        .retire_overhead("peak-cancel", &restore_peak().owner_sha256, &"b".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["host"]
            .held
            .cpu_millis,
        0
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_target_requires_node_acceptance_before_commit() {
    let (_rig, session) = queued_setup().await;
    let request = execution_request(paused_execution(&session, "accept-queued").await);
    let grant = session
        .authorize_execution_with_peak(
            request.clone(),
            "boot-first",
            1000,
            "peak-accept",
            restore_peak(),
        )
        .await
        .unwrap();
    let mut result = grant.target.clone();
    result.state = EnvironmentState::Running;
    result.revision += 1;
    result.runtime.ip = Some("10.0.0.3".parse().unwrap());
    result.last_operation = Some(adx_core::CompletedOperation {
        id: request.operation_id.clone(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: request.expected_revision,
    });
    assert_eq!(
        session.commit(result.clone()).await,
        Err(Error::Conflict),
        "metadata pre-reservation alone cannot publish an executed target"
    );
    let accepted = session
        .authorize_execution(request, "boot-first", 2000)
        .await
        .unwrap();
    assert_eq!(
        accepted, grant,
        "node acceptance preserves the original deadline and target"
    );
    session.commit(result).await.unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["host"]
            .held
            .cpu_millis,
        110
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_node_acceptance_racing_cancel_has_one_winner() {
    for _ in 0..8 {
        let (_rig, session) = queued_setup().await;
        let request = execution_request(paused_execution(&session, "race-cancel").await);
        session
            .authorize_execution_with_peak(
                request.clone(),
                "boot-first",
                1000,
                "peak-race",
                restore_peak(),
            )
            .await
            .unwrap();
        let mut deleted = request.source.clone();
        deleted.state = EnvironmentState::Deleted;
        deleted.revision += 2;
        deleted.checkpoint = None;
        deleted.last_operation = None;
        deleted.last_operation = None;
        let (accepted, cancelled) = tokio::join!(
            session.authorize_execution(request, "boot-first", 2000),
            session.commit(deleted)
        );
        assert_eq!(
            usize::from(accepted.is_ok()) + usize::from(cancelled.is_ok()),
            1,
            "a cancelled unconsumed grant cannot also be handed to a node"
        );
        assert_eq!(
            session.host_budgets().await.unwrap()["host"]
                .held
                .cpu_millis,
            if cancelled.is_ok() { 10 } else { 110 }
        );
        assert!(session.overhead_reservations().await.unwrap()["peak-race"]
            .retirement_proof_sha256
            .is_none());
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_snapshot_version_and_holds_share_one_concurrent_read() {
    use adx_coordinator::storage::OverheadReservation;
    let (_rig, session) = setup().await;
    let baseline = session.host_budget_snapshot().await.unwrap();
    assert_eq!(baseline.epoch, session.epoch());
    let base_revision = baseline.revision;
    let writer_session = session.clone();
    let writer = tokio::spawn(async move {
        for i in 0..64 {
            let id = format!("snapshot-hold-{i}");
            writer_session
                .reserve_overhead(
                    &id,
                    OverheadReservation {
                        host_id: "host".into(),
                        owner_sha256: "a".repeat(64),
                        resources: Resources {
                            cpu_millis: 1,
                            memory_bytes: 1,
                            disk_bytes: 0,
                        },
                        retirement_proof_sha256: None,
                    },
                )
                .await
                .unwrap();
            tokio::task::yield_now().await;
            writer_session
                .retire_overhead(&id, &"a".repeat(64), &"b".repeat(64))
                .await
                .unwrap();
        }
    });
    let mut reads = 0;
    loop {
        let value = session.host_budget_snapshot().await.unwrap();
        assert_eq!(value.epoch, baseline.epoch);
        let changes = value.revision.checked_sub(base_revision).unwrap();
        let held = &value.budgets["host"].held;
        assert_eq!(
            held.cpu_millis,
            changes % 2,
            "revision must identify the same atomic state as holds"
        );
        assert_eq!(held.memory_bytes, changes % 2);
        assert_eq!(held.disk_bytes, 0);
        reads += 1;
        if writer.is_finished() && reads >= 32 {
            break;
        }
        assert!(reads < 2048, "bounded concurrent snapshot verification");
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();
    let final_view = session.host_budget_snapshot().await.unwrap();
    assert_eq!(final_view.revision, base_revision + 128);
    assert_eq!(final_view.budgets["host"].held, Resources::default());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_snapshot_fences_old_session_and_keeps_positive_hold() {
    use adx_coordinator::storage::OverheadReservation;
    let (rig, session) = setup().await;
    session
        .reserve_overhead(
            "snapshot-restart",
            OverheadReservation {
                host_id: "host".into(),
                owner_sha256: "a".repeat(64),
                resources: Resources {
                    cpu_millis: 1,
                    memory_bytes: 1,
                    disk_bytes: 0,
                },
                retirement_proof_sha256: None,
            },
        )
        .await
        .unwrap();
    let before = session.host_budget_snapshot().await.unwrap();
    let current = rig.store().await.begin(1).await.unwrap();
    assert!(
        session.host_budget_snapshot().await.is_err(),
        "superseded epoch cannot supply a budget frame"
    );
    let after = current.host_budget_snapshot().await.unwrap();
    assert_eq!(after.epoch, before.epoch + 1);
    assert!(after.revision > before.revision);
    assert_eq!(
        after.budgets, before.budgets,
        "session replacement does not erase positive holds"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn host_budget_snapshot_versions_an_unconfigured_namespace_without_inventing_capacity() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let value = session.host_budget_snapshot().await.unwrap();
    assert_eq!(value.epoch, session.epoch());
    assert!(value.revision > 0);
    assert!(value.budgets.is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn queued_restore_peak_rejects_archived_and_old_generation_ids_atomically() {
    let (_rig, session) = queued_setup().await;
    session
        .reserve_overhead("old-peak", restore_peak())
        .await
        .unwrap();
    session
        .retire_overhead("old-peak", &"a".repeat(64), &"b".repeat(64))
        .await
        .unwrap();
    let archive = session
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    session
        .archive_overhead(archive, &"c".repeat(64))
        .await
        .unwrap();
    let request = execution_request(paused_execution(&session, "new").await);
    let before = session.host_budgets().await.unwrap();
    for id in ["old-peak", "unprefixed-new", "oh-0-new", "oh-2-new"] {
        assert_eq!(
            session
                .authorize_execution_with_peak(
                    request.clone(),
                    "boot-first",
                    1000,
                    id,
                    restore_peak()
                )
                .await,
            Err(Error::Conflict)
        );
        assert_eq!(session.host_budgets().await.unwrap(), before);
        assert!(session.get("new").await.unwrap().execution.is_none());
    }
    let grant = session
        .authorize_execution_with_peak(
            request.clone(),
            "boot-first",
            1000,
            "oh-1-new",
            restore_peak(),
        )
        .await
        .unwrap();
    session
        .reserve_overhead("oh-1-other", restore_peak())
        .await
        .unwrap();
    session
        .retire_overhead("oh-1-other", &"a".repeat(64), &"d".repeat(64))
        .await
        .unwrap();
    let next = session
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    assert_eq!(next.reservations.len(), 1);
    assert!(!next.reservations.contains_key("oh-1-new"));
    session
        .archive_overhead(next, &"e".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        session
            .authorize_execution_with_peak(request, "boot-first", 1500, "oh-1-new", restore_peak())
            .await
            .unwrap(),
        grant
    );
    assert!(session
        .retire_overhead("oh-1-new", &"a".repeat(64), &"f".repeat(64))
        .await
        .is_err());
}
