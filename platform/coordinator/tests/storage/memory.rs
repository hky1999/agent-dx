use super::*;
use adx_coordinator::storage::{HostBudget, NodeSession, Session};
use adx_core::memory::{MemoryAuthorization, MemoryIdentity, MemoryObservation, MemoryRequest};
use std::collections::BTreeMap;
const MIB: u64 = 1024 * 1024;

async fn fixture() -> (common::Redis, Session, EnvironmentRecord, MemoryObservation) {
    fixture_with_elastic(false).await
}

async fn fixture_with_elastic(
    elastic: bool,
) -> (common::Redis, Session, EnvironmentRecord, MemoryObservation) {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let mut n = node("first");
    n.capacity = Resources {
        cpu_millis: 2000,
        memory_bytes: 512 * MIB,
        disk_bytes: 0,
    };
    session
        .configure_host_budgets(BTreeMap::from([(
            "host".into(),
            HostBudget {
                capacity: n.capacity,
                nodes: ["first".into()].into(),
            },
        )]))
        .await
        .unwrap();
    session
        .register_session(
            n,
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
    let mut s = spec("a");
    s.resources.cpu_millis = 500;
    s.resources.memory_bytes = 128 * MIB;
    s.sandbox.limits.memory_bytes = 256 * MIB;
    if elastic {
        s.runtime_class = "runsc".into();
        let mut value = serde_json::to_value(s).unwrap();
        value["sandbox"]["elastic_memory"] = serde_json::json!({"high_per_mille":750});
        s = serde_json::from_value(value).unwrap();
    }
    let assignment = Assignment {
        environment_id: s.id.clone(),
        node_id: "first".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(s.clone(), assignment.clone())
        .await
        .unwrap();
    let r = running(s, assignment);
    session.commit(r.clone()).await.unwrap();
    let observation = MemoryObservation {
        identity: MemoryIdentity {
            assignment: r.assignment.clone(),
            runtime_id: r.runtime.id.clone(),
            sandbox_id: "sbox-a".into(),
            resource_generation: "generation-a".into(),
        },
        cpu_quota_usec: 50000,
        cpu_period_usec: 100000,
        memory_limit_bytes: 256 * MIB,
        memory_high_bytes: 192 * MIB,
        resource_revision: 1,
        operation_id: String::new(),
        operation_pending: false,
        frozen: false,
    };
    (rig, session, r, observation)
}
fn request(source: &MemoryObservation, operation: &str, mib: u64) -> MemoryRequest {
    MemoryRequest {
        source: source.clone(),
        target_limit_bytes: mib * MIB,
        target_high_bytes: mib * 3 * MIB / 4,
        operation_id: operation.into(),
        timeout_millis: 1000,
    }
}
fn completed(grant: &MemoryAuthorization) -> MemoryObservation {
    let mut result = grant.request.source.clone();
    result.resource_revision += 1;
    result.operation_id = grant.request.operation_id.clone();
    result.memory_limit_bytes = grant.request.target_limit_bytes;
    result.memory_high_bytes = grant.request.target_high_bytes;
    result
}
async fn held(session: &Session) -> u64 {
    session.host_budgets().await.unwrap()["host"]
        .held
        .memory_bytes
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_shrink_returns_budget_only_after_exact_completion_and_growth_reserves_first() {
    let (_rig, s, record, observation) = fixture().await;
    let mut forged = observation.clone();
    forged.memory_limit_bytes = 128 * MIB;
    forged.memory_high_bytes = 96 * MIB;
    assert_eq!(
        s.initialize_memory(forged, "boot-first").await,
        Err(Error::Conflict)
    );
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let grant = s
        .authorize_memory(request(&observation, "shrink-a", 128), "boot-first", 1000)
        .await
        .unwrap();
    assert_eq!(held(&s).await, 256 * MIB);
    assert_eq!(
        s.authorize_memory(grant.request.clone(), "boot-first", 9000)
            .await
            .unwrap(),
        grant,
        "replay keeps the original expiry"
    );
    s.complete_memory(grant.clone(), completed(&grant), "boot-first")
        .await
        .unwrap();
    s.complete_memory(grant.clone(), completed(&grant), "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 128 * MIB);
    let grow = s
        .authorize_memory(
            request(&completed(&grant), "grow-a", 256),
            "boot-first",
            10000,
        )
        .await
        .unwrap();
    assert_eq!(
        held(&s).await,
        256 * MIB,
        "unacknowledged growth already owns its full target"
    );
    assert_eq!(
        s.get("a").await.unwrap().spec,
        record.spec,
        "grants do not rewrite demand or ceiling"
    );
    assert!(grow.check_dispatch(11000, "boot-first", s.epoch()).is_err());
    assert_eq!(
        held(&s).await,
        256 * MIB,
        "expiry is not a cancellation or retirement proof"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_unknown_growth_survives_redis_restart_and_fences_old_epoch_and_session() {
    let (mut rig, s, record, observation) = fixture().await;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let shrink = s
        .authorize_memory(request(&observation, "shrink-a", 128), "boot-first", 1000)
        .await
        .unwrap();
    s.complete_memory(shrink.clone(), completed(&shrink), "boot-first")
        .await
        .unwrap();
    let grow = s
        .authorize_memory(
            request(&completed(&shrink), "grow-a", 256),
            "boot-first",
            3000,
        )
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(held(&restarted).await, 256 * MIB);
    assert_eq!(
        restarted
            .authorize_memory(grow.request.clone(), "boot-first", 9000)
            .await
            .unwrap(),
        grow
    );
    assert!(grow
        .check_dispatch(3500, "boot-first", restarted.epoch())
        .is_err());
    assert!(restarted
        .complete_memory(grow.clone(), completed(&grow), "foreign-session")
        .await
        .is_err());
    restarted
        .complete_memory(grow.clone(), completed(&grow), "boot-first")
        .await
        .unwrap();
    let next = restarted
        .authorize_memory(
            request(&completed(&grow), "shrink-b", 128),
            "boot-first",
            10000,
        )
        .await
        .unwrap();
    restarted
        .invalidate_node("first", "boot-first")
        .await
        .unwrap();
    assert!(restarted
        .complete_memory(next.clone(), completed(&next), "boot-first")
        .await
        .is_err());
    assert_eq!(held(&restarted).await, 256 * MIB);
    let mut retired = restarted.get("a").await.unwrap().result.unwrap();
    assert_eq!(retired.runtime.id, record.runtime.id);
    retired.resources_held = false;
    retired.revision += 1;
    restarted.commit(retired).await.unwrap();
    assert_eq!(
        held(&restarted).await,
        0,
        "only the matching physical retirement clears unknown memory work"
    );
    assert!(restarted.get("a").await.unwrap().memory.is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_wrong_physical_proof_and_partial_or_frozen_outcome_never_release() {
    let (_rig, s, _, observation) = fixture().await;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let grant = s
        .authorize_memory(request(&observation, "shrink-a", 128), "boot-first", 1000)
        .await
        .unwrap();
    for case in 0..9 {
        let mut proof = completed(&grant);
        match case {
            0 => proof.identity.resource_generation = "other".into(),
            1 => proof.identity.sandbox_id = "other".into(),
            2 => proof.resource_revision -= 1,
            3 => proof.operation_id = "other".into(),
            4 => proof.operation_pending = true,
            5 => proof.frozen = true,
            6 => proof.memory_high_bytes += 4096,
            7 => proof.memory_limit_bytes += MIB,
            _ => proof.cpu_quota_usec += 1,
        }
        assert!(
            s.complete_memory(grant.clone(), proof, "boot-first")
                .await
                .is_err(),
            "case {case}"
        );
        assert_eq!(held(&s).await, 256 * MIB);
    }
    let mut changed = grant.request.clone();
    changed.target_high_bytes += 4096;
    assert_eq!(
        s.authorize_memory(changed, "boot-first", 2000).await,
        Err(Error::Conflict)
    );
    s.complete_memory(grant.clone(), completed(&grant), "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 128 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_growth_and_new_creation_compete_in_the_same_atomic_host_budget() {
    let (_rig, s, _, observation) = fixture().await;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let shrink = s
        .authorize_memory(request(&observation, "shrink-a", 128), "boot-first", 1000)
        .await
        .unwrap();
    s.complete_memory(shrink.clone(), completed(&shrink), "boot-first")
        .await
        .unwrap();
    let mut b = spec("b");
    b.resources.cpu_millis = 500;
    b.resources.memory_bytes = 384 * MIB;
    let assignment = Assignment {
        environment_id: "b".into(),
        node_id: "first".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    let (growth, creation) = tokio::join!(
        s.authorize_memory(
            request(&completed(&shrink), "grow-a", 256),
            "boot-first",
            3000
        ),
        s.reserve(b, assignment)
    );
    assert_eq!(
        usize::from(growth.is_ok()) + usize::from(creation.is_ok()),
        1
    );
    assert!(matches!(growth, Err(Error::NoCapacity)) || matches!(creation, Err(Error::NoCapacity)));
    assert!(held(&s).await <= 512 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_invalid_authorization_cannot_mutate_holds_or_reuse_completed_operation() {
    let (_rig, s, _, observation) = fixture().await;
    let initial = request(&observation, "shrink-a", 128);
    assert_eq!(
        s.authorize_memory(initial.clone(), "boot-first", 1000)
            .await,
        Err(Error::Conflict)
    );
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    for (candidate, now, session) in [
        (request(&observation, "below-floor", 64), 1000, "boot-first"),
        (
            request(&observation, "above-ceiling", 512),
            1000,
            "boot-first",
        ),
        (initial.clone(), u64::MAX, "boot-first"),
        (initial.clone(), i64::MAX as u64, "boot-first"),
        (initial.clone(), 1000, "foreign-session"),
    ] {
        assert_eq!(
            s.authorize_memory(candidate, session, now).await,
            Err(Error::Conflict)
        );
        assert_eq!(held(&s).await, 256 * MIB);
        assert!(s.get("a").await.unwrap().memory.unwrap().pending.is_none());
    }
    let shrink = s
        .authorize_memory(initial, "boot-first", 1000)
        .await
        .unwrap();
    assert!(shrink.check_dispatch(999, "boot-first", s.epoch()).is_err());
    s.complete_memory(shrink.clone(), completed(&shrink), "boot-first")
        .await
        .unwrap();
    assert_eq!(
        s.authorize_memory(
            request(&completed(&shrink), "shrink-a", 256),
            "boot-first",
            3000
        )
        .await,
        Err(Error::Conflict)
    );
    let growth = s
        .authorize_memory(
            request(&completed(&shrink), "grow-a", 256),
            "boot-first",
            3000,
        )
        .await
        .unwrap();
    s.complete_memory(shrink.clone(), completed(&shrink), "boot-first")
        .await
        .unwrap();
    assert_eq!(
        held(&s).await,
        256 * MIB,
        "old completed shrink cannot cancel pending growth"
    );
    assert_eq!(
        s.get("a")
            .await
            .unwrap()
            .memory
            .unwrap()
            .pending
            .unwrap()
            .authorization,
        growth
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_scheduler_restore_uses_settled_and_pending_holds() {
    let (_rig, s, _, observation) = fixture().await;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let shrink = s
        .authorize_memory(request(&observation, "shrink-a", 128), "boot-first", 1000)
        .await
        .unwrap();
    s.complete_memory(shrink.clone(), completed(&shrink), "boot-first")
        .await
        .unwrap();
    let scheduler = Coordinator::restore(&s.snapshot().await.unwrap(), Placement::Spread).unwrap();
    let (capacity, available) = scheduler.node_resources("first").unwrap();
    assert_eq!(
        capacity.memory_bytes - available.memory_bytes,
        128 * MIB,
        "scheduler must rebuild the settled allocation, not the immutable ceiling"
    );
    s.authorize_memory(
        request(&completed(&shrink), "grow-a", 256),
        "boot-first",
        3000,
    )
    .await
    .unwrap();
    let scheduler = Coordinator::restore(&s.snapshot().await.unwrap(), Placement::Spread).unwrap();
    let (capacity, available) = scheduler.node_resources("first").unwrap();
    assert_eq!(
        capacity.memory_bytes - available.memory_bytes,
        256 * MIB,
        "unknown growth remains held during scheduler restore"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_elastic_birth_and_recovery_reserve_different_bounds() {
    let (_rig, s, mut record, mut observation) = fixture_with_elastic(true).await;
    assert_eq!(
        held(&s).await,
        128 * MIB,
        "new elastic creation holds initial memory"
    );
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let grow = s
        .authorize_memory(request(&observation, "grow-a", 192), "boot-first", 1000)
        .await
        .unwrap();
    s.complete_memory(grow.clone(), completed(&grow), "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 192 * MIB);
    record.state = EnvironmentState::Paused;
    record.revision += 1;
    record.resources_held = false;
    record.runtime.ip = None;
    record.checkpoint = Some(adx_core::RestorePoint {
        id: "checkpoint".into(),
        origin: None,
        expires_at_unix_seconds: u64::MAX,
        source_runtime_id: record.runtime.id.clone(),
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    s.commit(record.clone()).await.unwrap();
    assert_eq!(held(&s).await, 0);
    let request = adx_core::execution::ExecutionRequest {
        expected_revision: record.revision,
        source: record,
        kind: adx_core::execution::ExecutionKind::Resume,
        operation_id: "resume-a".into(),
        timeout_millis: 1000,
    };
    let grant = s
        .authorize_execution(request, "boot-first", 3000)
        .await
        .unwrap();
    assert_eq!(
        held(&s).await,
        256 * MIB,
        "restore must reserve its possible saved limit before dispatch"
    );
    let scheduler = Coordinator::restore(&s.snapshot().await.unwrap(), Placement::Spread).unwrap();
    let (capacity, available) = scheduler.node_resources("first").unwrap();
    assert_eq!(capacity.memory_bytes - available.memory_bytes, 256 * MIB);
    let mut restored = grant.target.clone();
    restored.state = EnvironmentState::Running;
    restored.revision += 1;
    restored.runtime.ip = Some("10.0.0.3".parse().unwrap());
    restored.last_operation = Some(adx_core::CompletedOperation {
        id: grant.request.operation_id,
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: grant.request.expected_revision,
    });
    s.commit(restored.clone()).await.unwrap();
    assert_eq!(
        held(&s).await,
        256 * MIB,
        "Running without exact memory readback does not release recovery allowance"
    );
    assert!(
        s.initialize_memory(observation.clone(), "boot-first")
            .await
            .is_err(),
        "old Runtime cannot settle the new recovery"
    );
    observation.identity.runtime_id = restored.runtime.id;
    observation.identity.sandbox_id = "sbox-restored".into();
    observation.identity.resource_generation = "generation-restored".into();
    observation.memory_limit_bytes = 192 * MIB;
    observation.memory_high_bytes = 144 * MIB;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    assert_eq!(
        held(&s).await,
        192 * MIB,
        "complete birth proof settles only unused recovery allowance"
    );
    let scheduler = Coordinator::restore(&s.snapshot().await.unwrap(), Placement::Spread).unwrap();
    let (capacity, available) = scheduler.node_resources("first").unwrap();
    assert_eq!(capacity.memory_bytes - available.memory_bytes, 192 * MIB);
}
