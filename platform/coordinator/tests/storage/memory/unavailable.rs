use super::*;
use adx_coordinator::storage::MemoryControlPolicy;

async fn elastic_birth() -> (common::Redis, Session, MemoryObservation) {
    let (rig, session, _, mut birth) = fixture_with_elastic(true).await;
    birth.memory_limit_bytes = 128 * MIB;
    birth.memory_high_bytes = 96 * MIB;
    (rig, session, birth)
}

async fn growing() -> (
    common::Redis,
    Session,
    MemoryObservation,
    MemoryAuthorization,
) {
    let (rig, session, birth) = elastic_birth().await;
    let grant = session
        .reconcile_memory(
            sample(&birth, 112, 1000),
            "boot-first",
            1000,
            Some(&MemoryControlPolicy::default()),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    assert_eq!(grant.request.target_limit_bytes, 224 * MIB);
    (rig, session, birth, grant)
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_completed_growth_settles_without_authorizing_more_growth() {
    for paused in [false, true] {
        let (rig, s, _, grant) = growing().await;
        s.set_node_scheduling("first", paused, false).await.unwrap();
        let actual = completed(&grant);
        let policy = MemoryControlPolicy::default();
        let result = s
            .reconcile_memory(
                sample(&actual, 200, 12000),
                "boot-first",
                12000,
                Some(&policy),
            )
            .await
            .expect("closed admission must allow the original growth to settle");
        assert!(result.authorization.is_none());
        assert_eq!(held(&s).await, 224 * MIB);
        let account = s.get("a").await.unwrap().memory.unwrap();
        assert!(account.pending.is_none());
        assert_eq!(account.confirmed, actual);
        assert_eq!(
            account.completed[&grant.request.operation_id].authorization,
            grant
        );

        let reopened = rig.store().await.begin(1).await.unwrap();
        assert!(reopened
            .reconcile_memory(
                sample(&actual, 200, 13000),
                "boot-first",
                13000,
                Some(&policy),
            )
            .await
            .unwrap()
            .authorization
            .is_none());
        assert_eq!(held(&reopened).await, 224 * MIB);
        reopened
            .set_node_scheduling("first", false, true)
            .await
            .unwrap();
        let next = reopened
            .reconcile_memory(
                sample(&actual, 200, 20000),
                "boot-first",
                20000,
                Some(&policy),
            )
            .await
            .unwrap()
            .authorization
            .expect("reopening admission permits a new budgeted operation");
        assert_eq!(next.request.target_limit_bytes, 256 * MIB);
        assert_ne!(next.request.operation_id, grant.request.operation_id);
        assert_eq!(held(&reopened).await, 256 * MIB);
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_unchanged_growth_returns_only_closed_unused_reservation() {
    let (rig, s, birth, grant) = growing().await;
    s.set_node_scheduling("first", false, false).await.unwrap();
    let policy = MemoryControlPolicy::default();
    let pending = s
        .reconcile_memory(
            sample(&birth, 16, 12000),
            "boot-first",
            12000,
            Some(&policy),
        )
        .await
        .expect("an expired unknown result must remain observable");
    assert_eq!(pending.authorization, Some(grant.clone()));
    assert_eq!(held(&s).await, 224 * MIB);

    let unchanged = grant.request.unchanged().unwrap();
    let settled = s
        .reconcile_memory(
            sample(&unchanged, 112, 12001),
            "boot-first",
            12001,
            Some(&policy),
        )
        .await
        .unwrap();
    assert!(settled.authorization.is_none());
    assert_eq!(held(&s).await, 128 * MIB);
    let reopened = rig.store().await.begin(1).await.unwrap();
    let account = reopened.get("a").await.unwrap().memory.unwrap();
    assert!(account.pending.is_none());
    assert!(account.completed[&grant.request.operation_id].unchanged);
    assert!(reopened
        .reconcile_memory(
            sample(&unchanged, 112, 13000),
            "boot-first",
            13000,
            Some(&policy),
        )
        .await
        .unwrap()
        .authorization
        .is_none());
    assert_eq!(held(&reopened).await, 128 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_completed_shrink_returns_budget_without_issuing_another_operation() {
    let (_rig, s, birth) = elastic_birth().await;
    s.reconcile_memory(sample(&birth, 32, 1000), "boot-first", 1000, None)
        .await
        .unwrap();
    let growth = s
        .authorize_memory(
            request(&birth, "grow-before-unavailable", 256),
            "boot-first",
            2000,
        )
        .await
        .unwrap();
    s.complete_memory(growth.clone(), completed(&growth), "boot-first")
        .await
        .unwrap();
    let shrink = s
        .authorize_memory(
            request(&completed(&growth), "shrink-before-unavailable", 128),
            "boot-first",
            4000,
        )
        .await
        .unwrap();
    s.set_node_scheduling("first", false, false).await.unwrap();
    assert_eq!(held(&s).await, 256 * MIB);
    let decision = s
        .reconcile_memory(
            sample(&completed(&shrink), 112, 12000),
            "boot-first",
            12000,
            Some(&MemoryControlPolicy::default()),
        )
        .await
        .expect("the original completed shrink must release its exact budget");
    assert!(decision.authorization.is_none());
    assert_eq!(held(&s).await, 128 * MIB);
    let account = s.get("a").await.unwrap().memory.unwrap();
    assert!(account.pending.is_none());
    assert!(!account.completed[&shrink.request.operation_id].unchanged);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_direct_completion_preserves_exact_proof_and_replay() {
    let (rig, s, _, grant) = growing().await;
    s.set_node_scheduling("first", false, false).await.unwrap();
    let actual = completed(&grant);
    for variant in 0..7 {
        let mut invalid = actual.clone();
        match variant {
            0 => invalid.identity.resource_generation = "foreign-generation".into(),
            1 => invalid.identity.runtime_id = "foreign-runtime".into(),
            2 => invalid.frozen = true,
            3 => invalid.operation_pending = true,
            4 => invalid.operation_id = "foreign-operation".into(),
            5 => invalid.resource_revision += 1,
            6 => invalid.memory_high_bytes -= 4096,
            _ => unreachable!(),
        }
        assert!(s
            .complete_memory(grant.clone(), invalid, "boot-first")
            .await
            .is_err());
        assert_eq!(held(&s).await, 224 * MIB);
    }
    s.complete_memory(grant.clone(), actual.clone(), "boot-first")
        .await
        .expect("current routable owner can complete its existing authorization");
    let reopened = rig.store().await.begin(1).await.unwrap();
    reopened
        .complete_memory(grant.clone(), actual, "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&reopened).await, 224 * MIB);
    assert!(reopened
        .get("a")
        .await
        .unwrap()
        .memory
        .unwrap()
        .pending
        .is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_birth_readback_does_not_create_a_new_authorization() {
    let (_rig, s, birth) = elastic_birth().await;
    s.set_node_scheduling("first", false, false).await.unwrap();
    assert_eq!(
        s.initialize_memory(birth.clone(), "boot-first").await,
        Err(Error::Conflict)
    );
    let decision = s
        .reconcile_memory(
            sample(&birth, 112, 1000),
            "boot-first",
            1000,
            Some(&MemoryControlPolicy::default()),
        )
        .await
        .expect("an approved execution can report its birth while admission is closed");
    assert!(decision.authorization.is_none());
    assert_eq!(held(&s).await, 128 * MIB);
    assert_eq!(s.get("a").await.unwrap().memory.unwrap().confirmed, birth);
    assert_eq!(
        s.authorize_memory(
            request(&birth, "new-unavailable-growth", 256),
            "boot-first",
            2000
        )
        .await,
        Err(Error::Conflict)
    );
    assert_eq!(held(&s).await, 128 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_low_usage_never_authorizes_a_new_shrink() {
    let (_rig, s, birth) = elastic_birth().await;
    s.reconcile_memory(sample(&birth, 32, 1000), "boot-first", 1000, None)
        .await
        .unwrap();
    let growth = s
        .authorize_memory(
            request(&birth, "grow-before-closed-low-window", 256),
            "boot-first",
            2000,
        )
        .await
        .unwrap();
    let actual = completed(&growth);
    s.complete_memory(growth, actual.clone(), "boot-first")
        .await
        .unwrap();
    s.set_node_scheduling("first", false, false).await.unwrap();
    let policy = MemoryControlPolicy::default();
    for now in [10000, 20000, 30000, 40000, 50000] {
        assert!(s
            .reconcile_memory(sample(&actual, 16, now), "boot-first", now, Some(&policy))
            .await
            .unwrap()
            .authorization
            .is_none());
        assert_eq!(held(&s).await, 256 * MIB);
    }
    assert_eq!(
        s.authorize_memory(
            request(&actual, "new-unavailable-shrink", 128),
            "boot-first",
            51000
        )
        .await,
        Err(Error::Conflict)
    );
    s.set_node_scheduling("first", false, true).await.unwrap();
    assert!(s
        .reconcile_memory(
            sample(&actual, 16, 51000),
            "boot-first",
            51000,
            Some(&policy)
        )
        .await
        .unwrap()
        .authorization
        .is_some());
    assert_eq!(
        held(&s).await,
        256 * MIB,
        "new shrink still needs physical completion"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_reconciliation_rejects_foreign_incomplete_and_stale_results() {
    let (_rig, s, _, grant) = growing().await;
    s.set_node_scheduling("first", false, false).await.unwrap();
    let actual = completed(&grant);
    for variant in 0..9 {
        let mut invalid = actual.clone();
        match variant {
            0 => invalid.identity.resource_generation = "foreign-generation".into(),
            1 => invalid.identity.sandbox_id = "foreign-sandbox".into(),
            2 => invalid.identity.assignment.generation += 1,
            3 => invalid.frozen = true,
            4 => invalid.operation_pending = true,
            5 => invalid.operation_id = "foreign-operation".into(),
            6 => invalid.resource_revision += 1,
            7 => invalid.memory_high_bytes -= 4096,
            8 => invalid.cpu_quota_usec += 1,
            _ => unreachable!(),
        }
        assert!(s
            .reconcile_memory(sample(&invalid, 100, 12000), "boot-first", 12000, None)
            .await
            .is_err());
        assert_eq!(held(&s).await, 224 * MIB);
        assert_eq!(
            s.get("a")
                .await
                .unwrap()
                .memory
                .unwrap()
                .pending
                .unwrap()
                .authorization,
            grant
        );
    }
    for (session, observed) in [("old-session", 12000), ("boot-first", 1000)] {
        assert!(s
            .reconcile_memory(sample(&actual, 100, observed), session, 12000, None)
            .await
            .is_err());
    }
    let decision = s
        .reconcile_memory(sample(&actual, 100, 12001), "boot-first", 12001, None)
        .await
        .unwrap();
    assert!(decision.authorization.is_none());
    assert_eq!(held(&s).await, 224 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_current_session_settles_predecessor_without_dispatching_old_grant() {
    let (_rig, s, _, grant) = growing().await;
    let saved = s.snapshot().await.unwrap().nodes["first"].clone();
    let mut node = saved.node;
    node.available = false;
    s.register_session(
        node,
        saved.address,
        saved.proxy_address,
        Some(NodeSession {
            id: "boot-new".into(),
            sequence: 1,
            routable: true,
        }),
    )
    .await
    .unwrap();
    let actual = completed(&grant);
    assert_eq!(
        s.complete_memory(grant.clone(), actual.clone(), "boot-first")
            .await,
        Err(Error::Conflict)
    );
    assert!(s
        .reconcile_memory(sample(&actual, 100, 12000), "boot-first", 12000, None)
        .await
        .is_err());
    let decision = s
        .reconcile_memory(sample(&actual, 100, 12001), "boot-new", 12001, None)
        .await
        .unwrap();
    assert!(decision.authorization.is_none());
    assert!(grant.check_dispatch(1001, "boot-new", s.epoch()).is_err());
    assert_eq!(held(&s).await, 224 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unavailable_unroutable_session_cannot_settle_an_existing_grant() {
    let (_rig, s, _, grant) = growing().await;
    let saved = s.snapshot().await.unwrap().nodes["first"].clone();
    let mut node = saved.node.clone();
    node.available = false;
    s.register_session(
        node.clone(),
        saved.address.clone(),
        saved.proxy_address.clone(),
        Some(NodeSession {
            id: "boot-first".into(),
            sequence: 2,
            routable: false,
        }),
    )
    .await
    .unwrap();
    let actual = completed(&grant);
    assert!(s
        .complete_memory(grant.clone(), actual.clone(), "boot-first")
        .await
        .is_err());
    assert!(s
        .reconcile_memory(sample(&actual, 100, 12000), "boot-first", 12000, None)
        .await
        .is_err());
    assert_eq!(held(&s).await, 224 * MIB);
    s.register_session(
        node,
        saved.address,
        saved.proxy_address,
        Some(NodeSession {
            id: "boot-first".into(),
            sequence: 3,
            routable: true,
        }),
    )
    .await
    .unwrap();
    s.complete_memory(grant, actual, "boot-first")
        .await
        .unwrap();
    assert!(s.get("a").await.unwrap().memory.unwrap().pending.is_none());
}
