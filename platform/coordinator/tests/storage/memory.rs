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
    fixture_with_timeout(elastic, std::time::Duration::from_millis(300)).await
}

async fn fixture_with_timeout(
    elastic: bool,
    timeout: std::time::Duration,
) -> (common::Redis, Session, EnvironmentRecord, MemoryObservation) {
    fixture_with_minimum(elastic, timeout, None, 1).await
}

async fn fixture_with_minimum(
    elastic: bool,
    timeout: std::time::Duration,
    minimum: Option<u64>,
    node_version: u32,
) -> (common::Redis, Session, EnvironmentRecord, MemoryObservation) {
    let rig = common::Redis::new().await;
    let session = adx_coordinator::storage::RedisStore::connect(&rig.url, "test", timeout)
        .await
        .unwrap()
        .begin(1)
        .await
        .unwrap();
    let mut n = node("first");
    n.memory_control_version = node_version;
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
        value["sandbox"]["elastic_memory"] =
            serde_json::json!({"high_per_mille":750,"minimum_bytes":minimum});
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

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_current_node_session_settles_predecessor_grant_but_old_writer_is_fenced() {
    let (_rig, s, _record, observation) = fixture().await;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let grant = s
        .authorize_memory(
            request(&observation, "shrink-before-restart", 128),
            "boot-first",
            1000,
        )
        .await
        .unwrap();
    let saved = s.snapshot().await.unwrap().nodes["first"].clone();
    s.register_session(
        saved.node,
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
    assert_eq!(
        s.complete_memory(grant.clone(), completed(&grant), "boot-first")
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(held(&s).await, 256 * MIB);
    s.complete_memory(grant.clone(), completed(&grant), "boot-new")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 128 * MIB);
    s.complete_memory(grant.clone(), completed(&grant), "boot-new")
        .await
        .unwrap();
    assert!(grant.check_dispatch(1001, "boot-new", s.epoch()).is_err());
}

fn sample(
    observation: &MemoryObservation,
    usage_mib: u64,
    now: u64,
) -> adx_core::memory::MemorySample {
    adx_core::memory::MemorySample {
        observation: observation.clone(),
        usage_bytes: usage_mib * MIB,
        peak_bytes: None,
        cpu_usage_ns: None,
        pressure: None,
        observed_unix_millis: now,
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_automatically_grows_then_shrinks_only_after_sustained_low_usage() {
    let (_rig, s, _record, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    let policy = adx_coordinator::storage::MemoryControlPolicy::default();
    let growth = s
        .reconcile_memory(
            sample(&observation, 112, 1000),
            "boot-first",
            1000,
            Some(&policy),
        )
        .await
        .unwrap();
    let grant = growth.authorization.unwrap();
    assert_eq!(grant.request.target_limit_bytes, 224 * MIB);
    assert_eq!(growth.held_resources.memory_bytes, 224 * MIB);
    assert_eq!(held(&s).await, 224 * MIB, "reserve before dispatch");
    let replay = s
        .reconcile_memory(
            sample(&observation, 112, 1100),
            "boot-first",
            1100,
            Some(&policy),
        )
        .await
        .unwrap();
    assert_eq!(replay.authorization, Some(grant.clone()));
    observation = grant.request.completion().unwrap();
    for now in [7000, 17000, 27000, 37000] {
        let reply = s
            .reconcile_memory(
                sample(&observation, 16, now),
                "boot-first",
                now,
                Some(&policy),
            )
            .await
            .unwrap();
        assert!(
            reply.authorization.is_none(),
            "need five distinct low observations"
        );
        assert_eq!(held(&s).await, 224 * MIB);
    }
    // Replayed samples cannot advance the low-usage window.
    for _ in 0..8 {
        assert!(s
            .reconcile_memory(
                sample(&observation, 16, 37000),
                "boot-first",
                37000,
                Some(&policy)
            )
            .await
            .unwrap()
            .authorization
            .is_none());
    }
    let shrink = s
        .reconcile_memory(
            sample(&observation, 16, 47000),
            "boot-first",
            47000,
            Some(&policy),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    assert_eq!(shrink.request.target_limit_bytes, 128 * MIB);
    assert_eq!(
        held(&s).await,
        224 * MIB,
        "proposal does not return physical budget"
    );
    let mut frozen = shrink.request.completion().unwrap();
    frozen.frozen = true;
    assert!(s
        .reconcile_memory(
            sample(&frozen, 16, 47001),
            "boot-first",
            47001,
            Some(&policy)
        )
        .await
        .is_err());
    assert_eq!(held(&s).await, 224 * MIB);
    let done = s
        .reconcile_memory(
            sample(&shrink.request.completion().unwrap(), 16, 47002),
            "boot-first",
            47002,
            Some(&policy),
        )
        .await
        .unwrap();
    assert!(done.authorization.is_none());
    assert_eq!(held(&s).await, 128 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_rejects_stale_foreign_and_changed_samples_and_keeps_expired_hold() {
    let (rig, s, _record, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    let policy = adx_coordinator::storage::MemoryControlPolicy::default();
    assert!(s
        .reconcile_memory(
            sample(&observation, 112, 1001),
            "boot-first",
            1000,
            Some(&policy)
        )
        .await
        .is_err());
    assert!(s
        .reconcile_memory(
            sample(&observation, 112, 1000),
            "boot-first",
            20000,
            Some(&policy)
        )
        .await
        .is_err());
    assert!(s
        .reconcile_memory(
            sample(&observation, 112, 1000),
            "wrong-session",
            1000,
            Some(&policy)
        )
        .await
        .is_err());
    let grant = s
        .reconcile_memory(
            sample(&observation, 112, 1000),
            "boot-first",
            1000,
            Some(&policy),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    let renewed = rig.store().await.begin(1).await.unwrap();
    let reply = renewed
        .reconcile_memory(
            sample(&observation, 112, 90000),
            "boot-first",
            90000,
            Some(&policy),
        )
        .await
        .unwrap();
    assert_eq!(reply.authorization, Some(grant.clone()));
    assert!(grant
        .check_dispatch(90000, "boot-first", reply.coordinator_epoch)
        .is_err());
    assert_eq!(held(&renewed).await, 224 * MIB);
    let mut foreign = grant.request.completion().unwrap();
    foreign.identity.resource_generation = "other".into();
    assert!(renewed
        .reconcile_memory(
            sample(&foreign, 16, 90001),
            "boot-first",
            90001,
            Some(&policy)
        )
        .await
        .is_err());
    assert_eq!(held(&renewed).await, 224 * MIB);
    renewed
        .reconcile_memory(
            sample(&grant.request.completion().unwrap(), 16, 90002),
            "boot-first",
            90002,
            None,
        )
        .await
        .unwrap();
    assert!(renewed
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
async fn memory_control_disabled_policy_observes_without_dispatch_and_pressure_does_not_authorize()
{
    let (_rig, s, _record, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    assert!(s
        .reconcile_memory(sample(&observation, 112, 1000), "boot-first", 1000, None)
        .await
        .unwrap()
        .authorization
        .is_none());
    let mut pressure = sample(&observation, 32, 2000);
    pressure.pressure = Some(adx_core::memory::MemoryPressure {
        high_events: 9999,
        oom_events: 1,
        oom_kill_events: 0,
    });
    assert!(s
        .reconcile_memory(
            pressure,
            "boot-first",
            2000,
            Some(&adx_coordinator::storage::MemoryControlPolicy::default())
        )
        .await
        .unwrap()
        .authorization
        .is_none());
    assert_eq!(held(&s).await, 128 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_growth_defers_when_neighbor_owns_capacity_without_changing_either_hold() {
    let (_rig, s, _, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    let mut neighbor = spec("neighbor");
    neighbor.resources.cpu_millis = 500;
    neighbor.resources.memory_bytes = 384 * MIB;
    let assignment = Assignment {
        environment_id: "neighbor".into(),
        node_id: "first".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    s.reserve(neighbor.clone(), assignment.clone())
        .await
        .unwrap();
    let before = s.get("neighbor").await.unwrap();
    let result = s
        .reconcile_memory(
            sample(&observation, 112, 1000),
            "boot-first",
            1000,
            Some(&adx_coordinator::storage::MemoryControlPolicy::default()),
        )
        .await
        .unwrap();
    assert!(result.authorization.is_none());
    assert_eq!(result.held_resources.memory_bytes, 128 * MIB);
    assert_eq!(held(&s).await, 512 * MIB);
    assert_eq!(s.get("neighbor").await.unwrap(), before);
    assert!(s.get("a").await.unwrap().memory.unwrap().pending.is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_rejects_elastic_admission_to_old_node_even_with_spoofed_label() {
    let (_rig, s, mut record, _) = fixture_with_elastic(true).await;
    let saved = s.snapshot().await.unwrap().nodes["first"].clone();
    let mut old = saved.node;
    old.memory_control_version = 0;
    old.labels
        .insert("memory_control_version".into(), "1".into());
    s.register_session(old, saved.address, saved.proxy_address, saved.session)
        .await
        .unwrap();
    record.spec.id = "old-node-attempt".into();
    record.assignment.environment_id = record.spec.id.clone();
    record.assignment.generation = 2;
    assert_eq!(
        s.reserve(record.spec, record.assignment).await,
        Err(Error::Conflict)
    );
    assert_eq!(held(&s).await, 128 * MIB);
    assert_eq!(s.get("old-node-attempt").await, Err(Error::NotFound));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_low_window_survives_center_restart_but_observation_gap_resets_it() {
    let (rig, s, _, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    let policy = adx_coordinator::storage::MemoryControlPolicy::default();
    let grow = s
        .reconcile_memory(
            sample(&observation, 112, 1000),
            "boot-first",
            1000,
            Some(&policy),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    observation = grow.request.completion().unwrap();
    for now in [7000, 17000, 27000] {
        assert!(s
            .reconcile_memory(
                sample(&observation, 16, now),
                "boot-first",
                now,
                Some(&policy)
            )
            .await
            .unwrap()
            .authorization
            .is_none());
    }
    let renewed = rig.store().await.begin(1).await.unwrap();
    // A missed observation window cannot be interpreted as continuous low usage.
    for now in [50000, 60000, 70000, 80000] {
        assert!(renewed
            .reconcile_memory(
                sample(&observation, 16, now),
                "boot-first",
                now,
                Some(&policy)
            )
            .await
            .unwrap()
            .authorization
            .is_none());
    }
    assert!(renewed
        .reconcile_memory(
            sample(&observation, 16, 90000),
            "boot-first",
            90000,
            Some(&policy)
        )
        .await
        .unwrap()
        .authorization
        .is_some());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_frequent_samples_count_distinct_seconds_and_window_survives_restart() {
    let (rig, s, _, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    let policy = adx_coordinator::storage::MemoryControlPolicy::default();
    let grow = s
        .reconcile_memory(
            sample(&observation, 112, 1000),
            "boot-first",
            1000,
            Some(&policy),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    observation = grow.request.completion().unwrap();
    for now in (7000..27000).step_by(100) {
        assert!(s
            .reconcile_memory(
                sample(&observation, 16, now),
                "boot-first",
                now,
                Some(&policy)
            )
            .await
            .unwrap()
            .authorization
            .is_none());
    }
    let renewed = rig.store().await.begin(1).await.unwrap();
    for now in (27000..37000).step_by(100) {
        assert!(renewed
            .reconcile_memory(
                sample(&observation, 16, now),
                "boot-first",
                now,
                Some(&policy)
            )
            .await
            .unwrap()
            .authorization
            .is_none());
    }
    assert!(renewed
        .reconcile_memory(
            sample(&observation, 16, 37000),
            "boot-first",
            37000,
            Some(&policy)
        )
        .await
        .unwrap()
        .authorization
        .is_some());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_settles_fenced_unchanged_growth_and_can_authorize_again() {
    let (rig, s, _record, mut birth) = fixture_with_elastic(true).await;
    birth.memory_limit_bytes = 128 * MIB;
    birth.memory_high_bytes = 96 * MIB;
    let policy = adx_coordinator::storage::MemoryControlPolicy::default();
    let first = s
        .reconcile_memory(sample(&birth, 112, 1000), "boot-first", 1000, Some(&policy))
        .await
        .unwrap();
    let grant = first.authorization.unwrap();
    assert_eq!(held(&s).await, 224 * MIB);
    // Expiry and identical old numbers cannot release an unclosed reservation.
    let pending = s
        .reconcile_memory(sample(&birth, 16, 12000), "boot-first", 12000, None)
        .await
        .unwrap();
    assert_eq!(pending.authorization, Some(grant.clone()));
    assert_eq!(held(&s).await, 224 * MIB);
    // The daemon has consumed the original revision and closed this operation,
    // independently proving unchanged, unfrozen physical limits.
    let mut unchanged = birth.clone();
    unchanged.resource_revision += 1;
    unchanged.operation_id = grant.request.operation_id.clone();
    let settled = s
        .reconcile_memory(sample(&unchanged, 16, 12001), "boot-first", 12001, None)
        .await
        .expect("closed unchanged result must end the pending operation");
    assert!(settled.authorization.is_none());
    assert_eq!(held(&s).await, 128 * MIB);
    let restarted = rig.store().await.begin(1).await.unwrap();
    let replay = restarted
        .reconcile_memory(sample(&unchanged, 16, 13000), "boot-first", 13000, None)
        .await
        .unwrap();
    assert!(replay.authorization.is_none());
    assert_eq!(held(&restarted).await, 128 * MIB);
    let next = restarted
        .reconcile_memory(
            sample(&unchanged, 112, 20000),
            "boot-first",
            20000,
            Some(&policy),
        )
        .await
        .unwrap();
    assert_ne!(
        next.authorization.unwrap().request.operation_id,
        grant.request.operation_id
    );
    assert_eq!(held(&restarted).await, 224 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_unchanged_shrink_preserves_budget_and_cannot_replay_target_success() {
    let (rig, s, _, mut birth) = fixture_with_elastic(true).await;
    birth.memory_limit_bytes = 128 * MIB;
    birth.memory_high_bytes = 96 * MIB;
    s.reconcile_memory(sample(&birth, 32, 1000), "boot-first", 1000, None)
        .await
        .unwrap();
    let grow = s
        .authorize_memory(
            request(&birth, "grow-before-failed-shrink", 256),
            "boot-first",
            2000,
        )
        .await
        .unwrap();
    s.complete_memory(grow.clone(), completed(&grow), "boot-first")
        .await
        .unwrap();
    let shrink = s
        .authorize_memory(
            request(&completed(&grow), "unchanged-shrink", 128),
            "boot-first",
            4000,
        )
        .await
        .unwrap();
    let actual = shrink.request.unchanged().unwrap();
    let decision = s
        .reconcile_memory(sample(&actual, 32, 6000), "boot-first", 6000, None)
        .await
        .unwrap();
    assert!(decision.authorization.is_none());
    assert_eq!(
        held(&s).await,
        256 * MIB,
        "unchanged shrink cannot return any bytes"
    );
    assert_eq!(
        s.complete_memory(shrink.clone(), completed(&shrink), "boot-first")
            .await,
        Err(Error::Conflict)
    );
    let reopened = rig.store().await.begin(1).await.unwrap();
    let stored = reopened.get("a").await.unwrap().memory.unwrap();
    assert!(stored.completed["unchanged-shrink"].unchanged);
    assert!(stored.pending.is_none());
    assert_eq!(held(&reopened).await, 256 * MIB);
    assert_eq!(
        reopened
            .complete_memory(shrink.clone(), completed(&shrink), "boot-first")
            .await,
        Err(Error::Conflict)
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_rejects_incomplete_unchanged_proof_without_affecting_neighbor() {
    let (_rig, s, record, mut birth) = fixture_with_elastic(true).await;
    birth.memory_limit_bytes = 128 * MIB;
    birth.memory_high_bytes = 96 * MIB;
    s.reconcile_memory(sample(&birth, 32, 1000), "boot-first", 1000, None)
        .await
        .unwrap();
    let mut neighbor = record.spec.clone();
    neighbor.id = "neighbor".into();
    let mut placement = record.assignment.clone();
    placement.environment_id = neighbor.id.clone();
    placement.generation = 2;
    s.reserve(neighbor, placement).await.unwrap();
    let before = s.get("neighbor").await.unwrap();
    let grow = s
        .authorize_memory(request(&birth, "unknown-growth", 256), "boot-first", 2000)
        .await
        .unwrap();
    assert_eq!(held(&s).await, 384 * MIB);
    let unchanged = grow.request.unchanged().unwrap();
    for case in 0..8 {
        let mut actual = unchanged.clone();
        match case {
            0 => actual.resource_revision -= 1,
            1 => actual.resource_revision += 1,
            2 => actual.operation_id = "foreign".into(),
            3 => actual.operation_pending = true,
            4 => actual.frozen = true,
            5 => actual.identity.resource_generation = "foreign".into(),
            6 => actual.memory_high_bytes = 192 * MIB,
            _ => actual.cpu_quota_usec += 1000,
        }
        assert!(
            s.reconcile_memory(sample(&actual, 32, 4000), "boot-first", 4000, None)
                .await
                .is_err(),
            "case {case}"
        );
        assert_eq!(held(&s).await, 384 * MIB);
        assert_eq!(s.get("neighbor").await.unwrap(), before);
    }
    for now in [4000, 4001] {
        s.reconcile_memory(sample(&unchanged, 32, now), "boot-first", now, None)
            .await
            .unwrap();
        assert_eq!(held(&s).await, 256 * MIB);
        assert_eq!(s.get("neighbor").await.unwrap(), before);
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_failed_partial_execution_keeps_hold_across_restart_until_exact_retirement() {
    let (rig, s, mut record, mut birth) = fixture_with_elastic(true).await;
    birth.memory_limit_bytes = 128 * MIB;
    birth.memory_high_bytes = 96 * MIB;
    s.reconcile_memory(sample(&birth, 32, 1000), "boot-first", 1000, None)
        .await
        .unwrap();
    let mut neighbor = record.spec.clone();
    neighbor.id = "neighbor".into();
    let mut placement = record.assignment.clone();
    placement.environment_id = neighbor.id.clone();
    placement.generation = 2;
    s.reserve(neighbor, placement).await.unwrap();
    let before = s.get("neighbor").await.unwrap();
    let grant = s
        .authorize_memory(request(&birth, "partial-growth", 256), "boot-first", 2000)
        .await
        .unwrap();
    assert_eq!(held(&s).await, 384 * MIB);
    record.state = EnvironmentState::Failed;
    record.revision += 1;
    record.runtime.ip = None;
    s.commit(record.clone()).await.unwrap();
    assert_eq!(
        held(&s).await,
        384 * MIB,
        "failure is not physical retirement"
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
        grant
    );
    let reopened = rig.store().await.begin(1).await.unwrap();
    assert_eq!(held(&reopened).await, 384 * MIB);
    let mut retired = record.clone();
    retired.resources_held = false;
    retired.revision += 1;
    let mut foreign = retired.clone();
    foreign.runtime.id = "a-1-99".into();
    assert!(reopened.commit(foreign).await.is_err());
    assert_eq!(held(&reopened).await, 384 * MIB);
    for _ in 0..2 {
        reopened.commit(retired.clone()).await.unwrap();
        assert_eq!(held(&reopened).await, 128 * MIB);
        assert!(reopened.get("a").await.unwrap().memory.is_none());
        assert_eq!(reopened.get("neighbor").await.unwrap(), before);
    }
}

#[path = "memory/unavailable.rs"]
mod unavailable;

async fn persistence_setting(rig: &common::Redis, name: &str, value: &str) {
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let result: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg(name)
        .arg(value)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(result, "OK");
}

async fn raw_control(rig: &common::Redis) -> BTreeMap<String, String> {
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("HGETALL")
        .arg("adx:{test}:control:v1")
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn persistence_fixture(
    timeout: std::time::Duration,
) -> (common::Redis, Session, MemoryObservation) {
    let (rig, session, _, observation) = fixture_with_timeout(false, timeout).await;
    session
        .initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let shrink = session
        .authorize_memory(
            request(&observation, "persistence-prepare", 128),
            "boot-first",
            1000,
        )
        .await
        .unwrap();
    let confirmed = completed(&shrink);
    session
        .complete_memory(shrink, confirmed.clone(), "boot-first")
        .await
        .unwrap();
    (rig, session, confirmed)
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_unconfirmed_fsync_cannot_authorize_new_growth() {
    let (rig, session, source) = persistence_fixture(std::time::Duration::from_millis(300)).await;
    let before = raw_control(&rig).await;
    persistence_setting(&rig, "appendfsync", "no").await;
    let result = session
        .authorize_memory(
            request(&source, "unconfirmed-growth", 192),
            "boot-first",
            2000,
        )
        .await;
    assert!(
        matches!(result, Err(Error::Unavailable(_))),
        "unconfirmed persistence must not issue a new physical authorization"
    );
    assert_eq!(
        raw_control(&rig).await,
        before,
        "rejected growth must retain the original account and header"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_original_grant_replay_cannot_bypass_confirmation() {
    let (rig, session, source) = persistence_fixture(std::time::Duration::from_millis(300)).await;
    let grant = session
        .authorize_memory(
            request(&source, "persistence-original-grant", 192),
            "boot-first",
            2000,
        )
        .await
        .unwrap();
    let before = raw_control(&rig).await;
    persistence_setting(&rig, "appendfsync", "no").await;
    let replay = session
        .authorize_memory(grant.request.clone(), "boot-first", 9000)
        .await;
    assert!(
        matches!(replay, Err(Error::Unavailable(_))),
        "read-only grant replay must also obtain persistence confirmation"
    );
    assert_eq!(raw_control(&rig).await, before);
    persistence_setting(&rig, "appendfsync", "always").await;
    assert_eq!(
        session
            .authorize_memory(grant.request.clone(), "boot-first", 9000)
            .await
            .unwrap(),
        grant
    );
    assert_eq!(
        held(&session).await,
        192 * MIB,
        "confirmation recovery retains the same grant and one hold"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_shrink_settlement_waits_for_confirmation() {
    let (rig, session, _, source) = fixture().await;
    session
        .initialize_memory(source.clone(), "boot-first")
        .await
        .unwrap();
    let grant = session
        .authorize_memory(
            request(&source, "unconfirmed-shrink", 128),
            "boot-first",
            1000,
        )
        .await
        .unwrap();
    let observation = completed(&grant);
    let before = raw_control(&rig).await;
    persistence_setting(&rig, "appendfsync", "no").await;
    let result = session
        .complete_memory(grant.clone(), observation.clone(), "boot-first")
        .await;
    assert!(
        matches!(result, Err(Error::Unavailable(_))),
        "unconfirmed shrink must not acknowledge a budget refund"
    );
    assert_eq!(
        raw_control(&rig).await,
        before,
        "the original pending reservation must remain unchanged"
    );
    persistence_setting(&rig, "appendfsync", "always").await;
    assert_eq!(held(&session).await, 256 * MIB);
    session
        .complete_memory(grant.clone(), observation.clone(), "boot-first")
        .await
        .unwrap();
    session
        .complete_memory(grant, observation, "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&session).await, 128 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_disabled_aof_cannot_authorize_growth() {
    let (rig, session, source) = persistence_fixture(std::time::Duration::from_millis(300)).await;
    let before = raw_control(&rig).await;
    persistence_setting(&rig, "appendonly", "no").await;
    let result = session
        .authorize_memory(
            request(&source, "disabled-aof-growth", 192),
            "boot-first",
            2000,
        )
        .await;
    assert!(
        matches!(result, Err(Error::Unavailable(_))),
        "volatile Redis storage must not authorize physical growth"
    );
    assert_eq!(raw_control(&rig).await, before);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_everysec_confirmation_and_restart_preserve_one_account() {
    let (mut rig, session, source) = persistence_fixture(std::time::Duration::from_secs(5)).await;
    persistence_setting(&rig, "appendfsync", "everysec").await;
    // The policy alone is not a rejection: a completed fsync can confirm the operation.
    persistence_setting(&rig, "no-appendfsync-on-rewrite", "yes").await;
    let grant = session
        .authorize_memory(
            request(&source, "everysec-confirmed-growth", 192),
            "boot-first",
            2000,
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .authorize_memory(grant.request.clone(), "boot-first", 9000)
            .await
            .unwrap(),
        grant
    );
    session
        .complete_memory(grant.clone(), completed(&grant), "boot-first")
        .await
        .unwrap();
    let before = raw_control(&rig).await;
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("adx:{test}:control:v1*")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        keys.len(),
        2,
        "one bounded persistence marker is retained alongside the control hash"
    );
    rig.crash();
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        raw_control(&rig).await["environment:a"],
        before["environment:a"]
    );
    assert_eq!(held(&recovered).await, 192 * MIB);
    // A process restart test is not a host power-loss test.
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_existing_budget_requires_confirmation_before_new_epoch() {
    let (rig, _, _) = persistence_fixture(std::time::Duration::from_millis(300)).await;
    let before = raw_control(&rig).await;
    persistence_setting(&rig, "appendfsync", "no").await;
    let store = rig.store().await;
    assert!(
        matches!(store.begin(1).await, Err(Error::Unavailable(_))),
        "a fresh connection must discover and confirm the persisted budget before advancing epoch"
    );
    assert_eq!(raw_control(&rig).await, before);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_installing_budget_requires_confirmation_before_write() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let before = raw_control(&rig).await;
    persistence_setting(&rig, "appendfsync", "no").await;
    let result = session
        .configure_host_budgets(BTreeMap::from([(
            "host".into(),
            HostBudget {
                capacity: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 512 * MIB,
                    disk_bytes: 0,
                },
                nodes: ["first".into()].into(),
            },
        )]))
        .await;
    assert!(
        matches!(result, Err(Error::Unavailable(_))),
        "budget installation must not acknowledge unconfirmed state"
    );
    assert_eq!(raw_control(&rig).await, before);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_unconfigured_namespace_keeps_operator_policy() {
    let rig = common::Redis::new().await;
    persistence_setting(&rig, "appendonly", "no").await;
    let session = rig.store().await.begin(1).await.unwrap();
    assert!(session.host_budgets().await.unwrap().is_empty());
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("adx:{test}:control:v1*")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(keys, ["adx:{test}:control:v1"]);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_retirement_does_not_refund_without_confirmation() {
    let (rig, session, mut record, source) = fixture().await;
    session
        .initialize_memory(source, "boot-first")
        .await
        .unwrap();
    record.revision += 1;
    record.state = EnvironmentState::Failed;
    record.resources_held = false;
    record.runtime.ip = None;
    let before = raw_control(&rig).await;
    persistence_setting(&rig, "appendfsync", "no").await;
    assert!(
        matches!(
            session.commit(record.clone()).await,
            Err(Error::Unavailable(_))
        ),
        "retirement must not acknowledge an unconfirmed refund"
    );
    assert_eq!(raw_control(&rig).await, before);
    persistence_setting(&rig, "appendfsync", "always").await;
    assert_eq!(held(&session).await, 256 * MIB);
    session.commit(record.clone()).await.unwrap();
    session.commit(record).await.unwrap();
    assert_eq!(held(&session).await, 0);
}

#[path = "../common/persistence_proxy.rs"]
mod persistence_proxy;

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_persistence_applied_write_without_confirmation_remains_one_unknown_hold() {
    let (rig, _original_session, source) =
        persistence_fixture(std::time::Duration::from_millis(300)).await;
    let proxy = persistence_proxy::FsyncFaultProxy::new(&rig.url, "post-write-unknown");
    let session = adx_coordinator::storage::RedisStore::connect(
        &proxy.url,
        "test",
        std::time::Duration::from_millis(300),
    )
    .await
    .unwrap()
    .begin(1)
    .await
    .unwrap();
    let original = raw_control(&rig).await;
    let request = request(&source, "post-write-unknown", 192);
    // The proxy disables fsync only after successful reads, immediately before
    // forwarding this operation's real control CAS to the dedicated Redis.
    let result = session
        .authorize_memory(request.clone(), "boot-first", 2000)
        .await;
    assert!(
        proxy.injected(),
        "the fault must be at the actual CAS boundary"
    );
    assert!(
        matches!(result, Err(Error::Unavailable(_))),
        "an applied CAS without fsync confirmation must not authorize physical execution"
    );
    let unknown = raw_control(&rig).await;
    assert_ne!(
        unknown["environment:a"], original["environment:a"],
        "the original CAS really applied before confirmation failed"
    );
    let environment: adx_coordinator::storage::StoredEnvironment =
        serde_json::from_str(&unknown["environment:a"]).unwrap();
    let account = environment.memory.unwrap();
    assert_eq!(account.confirmed.memory_limit_bytes, 128 * MIB);
    let grant = account.pending.unwrap().authorization;
    assert_eq!(grant.request, request);
    assert_eq!(grant.request.target_limit_bytes, 192 * MIB);
    persistence_setting(&rig, "appendfsync", "always").await;
    assert_eq!(
        session
            .authorize_memory(request, "boot-first", 9000)
            .await
            .unwrap(),
        grant,
        "reconciliation confirms the original applied grant without extending its deadline"
    );
    assert_eq!(held(&session).await, 192 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn completed_memory_history_is_bounded_and_forgotten_ids_cannot_change_source() {
    use sha2::{Digest, Sha256};
    let (rig, s, record, mut observation) = fixture().await;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let mut first = None;
    let mut latest = None;
    for index in 0..70 {
        let operation = format!(
            "memory-{}-{}-{:x}",
            s.epoch(),
            observation.resource_revision,
            Sha256::digest(serde_json::to_vec(&observation).unwrap())
        );
        let target = if observation.memory_limit_bytes == 256 * MIB {
            128
        } else {
            256
        };
        let grant = s
            .authorize_memory(
                request(&observation, &operation, target),
                "boot-first",
                1000 + index * 1001,
            )
            .await
            .unwrap();
        observation = completed(&grant);
        s.complete_memory(grant.clone(), observation.clone(), "boot-first")
            .await
            .unwrap();
        if first.is_none() {
            first = Some(grant.clone())
        };
        latest = Some(grant);
    }
    let snapshot = s.snapshot().await.unwrap();
    let account = snapshot.environments[&record.spec.id]
        .memory
        .as_ref()
        .unwrap();
    assert!(
        account.completed.len() <= 64,
        "completed memory history grew to {} entries instead of bounded 64",
        account.completed.len()
    );
    assert_eq!(account.confirmed, observation);
    assert!(account.pending.is_none());
    assert_eq!(held(&s).await, 256 * MIB);
    let first = first.unwrap();
    let mut rebound = first.request.clone();
    rebound.source = observation.clone();
    rebound.target_limit_bytes = 128 * MIB;
    rebound.target_high_bytes = 96 * MIB;
    assert_eq!(
        s.authorize_memory(rebound, "boot-first", 100000).await,
        Err(Error::Conflict),
        "a forgotten ID cannot bind a new source revision"
    );
    assert_eq!(
        s.complete_memory(first.clone(), completed(&first), "boot-first")
            .await,
        Err(Error::Conflict)
    );
    let last = latest.unwrap();
    assert_eq!(
        s.authorize_memory(last.request.clone(), "boot-first", 100000)
            .await
            .unwrap(),
        last
    );
    assert_eq!(held(&s).await, 256 * MIB);

    for target in [128, 256] {
        let operation = format!(
            "memory-{}-{}-{:x}",
            s.epoch(),
            observation.resource_revision,
            Sha256::digest(serde_json::to_vec(&observation).unwrap())
        );
        let grant = s
            .authorize_memory(
                request(&observation, &operation, target),
                "boot-first",
                110000 + target,
            )
            .await
            .unwrap();
        if target == 128 {
            observation = completed(&grant);
            s.complete_memory(grant, observation.clone(), "boot-first")
                .await
                .unwrap();
            continue;
        }
        assert_eq!(
            held(&s).await,
            256 * MIB,
            "an unknown growth still reserves the target"
        );
        let recovered = rig.store().await.begin(1).await.unwrap();
        assert_eq!(held(&recovered).await, 256 * MIB);
        let snapshot = recovered.snapshot().await.unwrap();
        let account = snapshot.environments[&record.spec.id]
            .memory
            .as_ref()
            .unwrap();
        assert_eq!(account.pending.as_ref().unwrap().authorization, grant);
        assert!(account.completed.len() <= 64);
        assert!(account.closed_source_revision.is_some());
        recovered
            .complete_memory(grant.clone(), completed(&grant), "boot-first")
            .await
            .unwrap();
        assert_eq!(held(&recovered).await, 256 * MIB);
    }
}

// Append to platform/coordinator/tests/storage/memory.rs only when the root
// agent starts the next test-first phase. This is an unexecuted external draft.
// Uses that module's existing real-Redis fixture, sample(), held(), and
// raw_control(); it never calls authorize_memory or complete_memory.
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_control_reconciliation_keeps_progress_after_history_floor_and_restart() {
    let (mut rig, session, record, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    let identity = observation.identity.clone();
    let policy = adx_coordinator::storage::MemoryControlPolicy {
        cooldown_millis: 1_000,
        low_usage_millis: 1_000,
        low_usage_samples: 2,
        operation_timeout_millis: 1_000,
        ..Default::default()
    };
    policy.validate().unwrap();
    let mut now = 1_000_u64;
    let initial = session
        .reconcile_memory(
            sample(&observation, 64, now),
            "boot-first",
            now,
            Some(&policy),
        )
        .await
        .unwrap();
    assert!(initial.authorization.is_none());
    assert_eq!(held(&session).await, 128 * MIB);

    for index in 1_u64..=70 {
        assert_eq!(observation.identity, identity);
        assert_eq!(observation.resource_revision, index);
        now += 2_000;
        let growing = observation.memory_limit_bytes == 128 * MIB;
        let usage_mib = if growing { 112 } else { 16 };
        if !growing {
            let first_low = session
                .reconcile_memory(
                    sample(&observation, usage_mib, now),
                    "boot-first",
                    now,
                    Some(&policy),
                )
                .await
                .unwrap();
            assert!(first_low.authorization.is_none());
            assert_eq!(held(&session).await, observation.memory_limit_bytes);
            now += 1_000;
        }
        let authorized = session
            .reconcile_memory(
                sample(&observation, usage_mib, now),
                "boot-first",
                now,
                Some(&policy),
            )
            .await
            .unwrap();
        let grant = authorized
            .authorization
            .unwrap_or_else(|| panic!("automatic operation {index} must be authorized"));
        assert_eq!(grant.request.source, observation);
        assert_eq!(
            grant.request.target_limit_bytes,
            if growing { 224 * MIB } else { 128 * MIB }
        );
        grant
            .check_dispatch(now, "boot-first", session.epoch())
            .unwrap();
        let reservation = observation
            .memory_limit_bytes
            .max(grant.request.target_limit_bytes);
        assert_eq!(authorized.held_resources.memory_bytes, reservation);
        assert_eq!(held(&session).await, reservation);

        // Dispatch is modeled as having started within the original deadline.
        // A later read still showing only the original confirmed source is
        // unknown, not a non-execution proof; expiry cannot release its hold.
        now = grant.deadline_unix_millis + 1;
        let unknown = session
            .reconcile_memory(
                sample(&observation, usage_mib, now),
                "boot-first",
                now,
                Some(&policy),
            )
            .await
            .unwrap();
        assert_eq!(unknown.authorization.as_ref(), Some(&grant));
        assert!(grant
            .check_dispatch(now, "boot-first", session.epoch())
            .is_err());
        assert_eq!(held(&session).await, reservation);
        let pending = session.get(&record.spec.id).await.unwrap().memory.unwrap();
        assert_eq!(pending.confirmed, observation);
        assert_eq!(pending.pending.as_ref().unwrap().authorization, grant);
        assert_eq!(
            pending.completed.len(),
            usize::try_from((index - 1).min(64)).unwrap()
        );

        let finished = grant.request.completion().unwrap();
        if index == 66 {
            // An unsettled physical report must not bypass the durable floor,
            // clear the pending grant, or refund a shrink's old reservation.
            let before = pending.clone();
            let mut unsettled = finished.clone();
            unsettled.operation_pending = true;
            assert_eq!(
                session
                    .reconcile_memory(
                        sample(&unsettled, 64, now),
                        "boot-first",
                        now,
                        Some(&policy)
                    )
                    .await,
                Err(Error::Conflict)
            );
            assert_eq!(
                session.get(&record.spec.id).await.unwrap().memory.unwrap(),
                before
            );
            assert_eq!(held(&session).await, reservation);
        }

        now += 1;
        let settled = match session
            .reconcile_memory(
                sample(&finished, finished.memory_limit_bytes / MIB / 2, now),
                "boot-first",
                now,
                Some(&policy),
            )
            .await
        {
            Ok(decision) => decision,
            Err(error) => {
                let stuck = session.get(&record.spec.id).await.unwrap().memory.unwrap();
                assert_eq!(stuck.confirmed, observation);
                assert_eq!(stuck.pending.as_ref().unwrap().authorization, grant);
                assert_eq!(held(&session).await, reservation);
                panic!(
                    "automatic completion {index} must settle through reconcile_memory: source={}, completion={}, retained={}, floor={:?}, held={}, error={error:?}",
                    observation.resource_revision,
                    finished.resource_revision,
                    stuck.completed.len(),
                    stuck.closed_source_revision,
                    reservation
                );
            }
        };
        assert!(settled.authorization.is_none());
        assert_eq!(settled.observed, finished);
        assert_eq!(
            settled.held_resources.memory_bytes,
            finished.memory_limit_bytes
        );
        assert_eq!(held(&session).await, finished.memory_limit_bytes);
        observation = finished;
        let account = session.get(&record.spec.id).await.unwrap().memory.unwrap();
        assert_eq!(account.confirmed, observation);
        assert!(account.pending.is_none());
        assert_eq!(
            account.completed.len(),
            usize::try_from(index.min(64)).unwrap()
        );
        let expected_floor = if index > 64 { Some(index - 64) } else { None };
        assert_eq!(account.closed_source_revision, expected_floor);
        let mut retained_sources = account
            .completed
            .values()
            .map(|operation| operation.authorization.request.source.resource_revision)
            .collect::<Vec<_>>();
        retained_sources.sort_unstable();
        assert_eq!(
            retained_sources,
            (index.saturating_sub(63).max(1)..=index).collect::<Vec<_>>()
        );
        if let Some(floor) = expected_floor {
            assert!(account.completed.values().all(|operation| operation
                .authorization
                .request
                .source
                .resource_revision
                > floor));
            assert_eq!(
                account.latest_completed_issued_unix_millis,
                grant.issued_unix_millis
            );
        }
        if index == 65 {
            assert_eq!(account.closed_source_revision, Some(1));
            assert_eq!(observation.resource_revision, 66);
        }
    }
    assert_eq!(observation.identity, identity);
    assert_eq!(observation.resource_revision, 71);
    assert_eq!(held(&session).await, 128 * MIB);

    // Add an unknown 71st automatic growth after the 70-completion window.
    // Only this private fixture Redis is crashed; no main environment is used.
    now += 2_000;
    let pending = session
        .reconcile_memory(
            sample(&observation, 112, now),
            "boot-first",
            now,
            Some(&policy),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    pending
        .check_dispatch(now, "boot-first", session.epoch())
        .unwrap();
    assert_eq!(pending.request.target_limit_bytes, 224 * MIB);
    assert_eq!(held(&session).await, 224 * MIB);
    let before = session.get(&record.spec.id).await.unwrap().memory.unwrap();
    assert_eq!(before.closed_source_revision, Some(6));
    assert_eq!(before.completed.len(), 64);
    assert_eq!(before.pending.as_ref().unwrap().authorization, pending);
    let field = format!("environment:{}", record.spec.id);
    let raw_before = raw_control(&rig).await[&field].clone();
    let persisted: serde_json::Value = serde_json::from_str(&raw_before).unwrap();
    assert_eq!(persisted["memory"]["closed_source_revision"], 6);
    assert_eq!(
        persisted["memory"]["completed"].as_object().unwrap().len(),
        64
    );

    rig.crash();
    rig.start().await;
    assert_eq!(raw_control(&rig).await[&field], raw_before);
    let recovered = rig.store().await.begin(1).await.unwrap();
    assert!(recovered.epoch() > session.epoch());
    assert_eq!(
        recovered
            .get(&record.spec.id)
            .await
            .unwrap()
            .memory
            .unwrap(),
        before
    );
    assert_eq!(held(&recovered).await, 224 * MIB);
    now = pending.deadline_unix_millis + 1;
    let still_unknown = recovered
        .reconcile_memory(
            sample(&observation, 112, now),
            "boot-first",
            now,
            Some(&policy),
        )
        .await
        .unwrap();
    assert_eq!(still_unknown.authorization.as_ref(), Some(&pending));
    assert!(pending
        .check_dispatch(now, "boot-first", recovered.epoch())
        .is_err());
    assert_eq!(held(&recovered).await, 224 * MIB);
    // A new observation may legitimately update the private policy window;
    // the exact authorization, closure facts and reservation must not change.
    let after_unknown = recovered
        .get(&record.spec.id)
        .await
        .unwrap()
        .memory
        .unwrap();
    assert_eq!(after_unknown.confirmed, before.confirmed);
    assert_eq!(after_unknown.pending, before.pending);
    assert_eq!(after_unknown.completed, before.completed);
    assert_eq!(
        after_unknown.closed_source_revision,
        before.closed_source_revision
    );
    assert_eq!(
        after_unknown.latest_completed_issued_unix_millis,
        before.latest_completed_issued_unix_millis
    );

    let saved = recovered.snapshot().await.unwrap().nodes["first"].clone();
    recovered
        .register_session(
            saved.node,
            saved.address,
            saved.proxy_address,
            Some(NodeSession {
                id: "boot-after-history-floor".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered
            .get(&record.spec.id)
            .await
            .unwrap()
            .memory
            .unwrap(),
        after_unknown
    );
    assert!(recovered
        .reconcile_memory(
            sample(&observation, 112, now),
            "boot-first",
            now,
            Some(&policy)
        )
        .await
        .is_err());
    assert!(session
        .reconcile_memory(
            sample(&observation, 112, now),
            "boot-after-history-floor",
            now,
            Some(&policy)
        )
        .await
        .is_err());
    assert_eq!(held(&recovered).await, 224 * MIB);

    // A fresh node session may settle the exact predecessor operation without
    // reauthorizing its expired grant or inventing a retirement/refund proof.
    observation = pending.request.completion().unwrap();
    now += 1;
    let settled = recovered
        .reconcile_memory(
            sample(&observation, 112, now),
            "boot-after-history-floor",
            now,
            Some(&policy),
        )
        .await
        .unwrap();
    assert!(settled.authorization.is_none());
    let after = recovered
        .get(&record.spec.id)
        .await
        .unwrap()
        .memory
        .unwrap();
    assert!(after.pending.is_none());
    assert_eq!(after.completed.len(), 64);
    assert_eq!(after.closed_source_revision, Some(7));
    assert_eq!(
        after.latest_completed_issued_unix_millis,
        pending.issued_unix_millis
    );
    assert_eq!(held(&recovered).await, 224 * MIB);

    // Prove the restarted Coordinator and new node session can authorize and
    // settle another new operation after the retained window advances again.
    now += 2_000;
    assert!(recovered
        .reconcile_memory(
            sample(&observation, 16, now),
            "boot-after-history-floor",
            now,
            Some(&policy)
        )
        .await
        .unwrap()
        .authorization
        .is_none());
    now += 1_000;
    let next = recovered
        .reconcile_memory(
            sample(&observation, 16, now),
            "boot-after-history-floor",
            now,
            Some(&policy),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    assert_eq!(next.request.target_limit_bytes, 128 * MIB);
    next.check_dispatch(now, "boot-after-history-floor", recovered.epoch())
        .unwrap();
    observation = next.request.completion().unwrap();
    now += 1;
    assert!(recovered
        .reconcile_memory(
            sample(&observation, 64, now),
            "boot-after-history-floor",
            now,
            Some(&policy)
        )
        .await
        .unwrap()
        .authorization
        .is_none());
    let after = recovered
        .get(&record.spec.id)
        .await
        .unwrap()
        .memory
        .unwrap();
    assert_eq!(after.confirmed, observation);
    assert!(after.pending.is_none());
    assert_eq!(after.completed.len(), 64);
    assert_eq!(after.closed_source_revision, Some(8));
    assert_eq!(
        after.latest_completed_issued_unix_millis,
        next.issued_unix_millis
    );
    assert_eq!(held(&recovered).await, 128 * MIB);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_pressure_preserves_pending_growth_allows_settlement_and_shrink() {
    let (_rig, s, _record, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    let (_root, source) = super::pressure::source();
    s.configure_host_pressure(BTreeMap::from([("host".into(), source.clone())]))
        .await
        .unwrap();
    let grant = s
        .authorize_memory(
            request(&observation, "grow-pressure", 192),
            "boot-first",
            10000,
        )
        .await
        .unwrap();
    let account = &s.host_budgets().await.unwrap()["host"].memory_accounts;
    assert_eq!(account.birth_bytes, 128 * MIB);
    assert_eq!(account.settled_extra_bytes, 0);
    assert_eq!(account.in_flight_extra_bytes, 64 * MIB);
    assert_eq!(account.unknown_result_bytes, 192 * MIB);
    assert_eq!(account.competing_ungranted_bytes, 64 * MIB);
    super::pressure::publish(&source, false);
    assert_eq!(held(&s).await, 192 * MIB);
    let grown = completed(&grant);
    s.complete_memory(grant, grown.clone(), "boot-first")
        .await
        .unwrap();
    assert_eq!(
        s.authorize_memory(request(&grown, "blocked-growth", 256), "boot-first", 11000)
            .await,
        Err(Error::NoCapacity)
    );
    let shrink = s
        .authorize_memory(request(&grown, "shrink-pressure", 128), "boot-first", 12000)
        .await
        .unwrap();
    let mut saved = s.snapshot().await.unwrap().nodes["first"].clone();
    saved.node.available = false;
    saved.session.as_mut().unwrap().sequence += 1;
    s.register_session(
        saved.node,
        saved.address,
        saved.proxy_address,
        saved.session,
    )
    .await
    .unwrap();
    assert_eq!(
        held(&s).await,
        192 * MIB,
        "unconfirmed shrink retains the source allowance"
    );
    s.complete_memory(shrink.clone(), completed(&shrink), "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 128 * MIB);
    let account = &s.host_budgets().await.unwrap()["host"].memory_accounts;
    assert_eq!(
        account.birth_bytes + account.settled_extra_bytes + account.in_flight_extra_bytes,
        held(&s).await
    );
    assert_eq!(account.unknown_result_bytes, 0);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn memory_pressure_blocks_restore_and_preserves_existing_restore_reservation() {
    let (_rig, s, mut record, mut observation) = fixture_with_elastic(true).await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    s.initialize_memory(observation, "boot-first")
        .await
        .unwrap();
    record.state = EnvironmentState::Paused;
    record.revision += 1;
    record.resources_held = false;
    record.runtime.ip = None;
    record.checkpoint = Some(adx_core::RestorePoint {
        id: "checkpoint-pressure".into(),
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
    let mut neighbor = record.spec.clone();
    neighbor.id = "neighbor-pressure".into();
    let mut assignment = record.assignment.clone();
    assignment.environment_id = neighbor.id.clone();
    assignment.generation = 2;
    s.reserve(neighbor, assignment).await.unwrap();
    let (_root, source) = super::pressure::source();
    s.configure_host_pressure(BTreeMap::from([("host".into(), source.clone())]))
        .await
        .unwrap();
    let request = adx_core::execution::ExecutionRequest {
        expected_revision: record.revision,
        source: record,
        kind: adx_core::execution::ExecutionKind::Resume,
        operation_id: "resume-pressure".into(),
        timeout_millis: 1000,
    };
    super::pressure::publish(&source, false);
    assert_eq!(
        s.authorize_execution(request.clone(), "boot-first", 10000)
            .await,
        Err(Error::NoCapacity)
    );
    assert_eq!(held(&s).await, 128 * MIB);
    super::pressure::publish(&source, true);
    let grant = s
        .authorize_execution(request.clone(), "boot-first", 11000)
        .await
        .unwrap();
    assert_eq!(held(&s).await, 384 * MIB);
    assert_eq!(
        s.host_budgets().await.unwrap()["host"]
            .memory_accounts
            .restore_in_flight_bytes,
        256 * MIB
    );
    std::fs::remove_file(&source.path).unwrap();
    assert_eq!(
        s.authorize_execution(request, "boot-first", 12000)
            .await
            .unwrap(),
        grant
    );
    assert_eq!(
        held(&s).await,
        384 * MIB,
        "unknown restore remains reserved when observation disappears"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn independent_minimum_budget_settles_only_after_exact_physical_completion() {
    let (_rig, s, record, mut observation) = fixture_with_minimum(
        true,
        std::time::Duration::from_millis(300),
        Some(64 * MIB),
        2,
    )
    .await;
    observation.memory_limit_bytes = 128 * MIB;
    observation.memory_high_bytes = 96 * MIB;
    assert_eq!(record.spec.execution_resources().memory_bytes, 128 * MIB);
    assert_eq!(record.spec.memory_floor_bytes(), 64 * MIB);
    s.initialize_memory(observation.clone(), "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 128 * MIB);
    let shrink = s
        .authorize_memory(
            request(&observation, "shrink-independent", 64),
            "boot-first",
            1000,
        )
        .await
        .unwrap();
    assert_eq!(held(&s).await, 128 * MIB);
    let mut unknown = completed(&shrink);
    unknown.resource_revision -= 1;
    assert!(s
        .complete_memory(shrink.clone(), unknown, "boot-first")
        .await
        .is_err());
    assert_eq!(held(&s).await, 128 * MIB);
    let after = completed(&shrink);
    s.complete_memory(shrink, after.clone(), "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 64 * MIB);
    assert!(s
        .authorize_memory(request(&after, "below-independent", 32), "boot-first", 2000)
        .await
        .is_err());
    assert_eq!(held(&s).await, 64 * MIB);
    let grow = s
        .authorize_memory(request(&after, "grow-independent", 256), "boot-first", 3000)
        .await
        .unwrap();
    assert_eq!(held(&s).await, 256 * MIB);
    s.complete_memory(grow.clone(), completed(&grow), "boot-first")
        .await
        .unwrap();
    assert_eq!(held(&s).await, 256 * MIB);
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn independent_minimum_persistent_admission_rejects_a_version_one_node() {
    let (_rig, s, record, _) = fixture_with_elastic(true).await;
    let mut next = record.spec.clone();
    next.id = "independent-next".into();
    next.sandbox.elastic_memory.as_mut().unwrap().minimum_bytes = Some(64 * MIB);
    let mut assignment = record.assignment.clone();
    assignment.environment_id = next.id.clone();
    // A fresh allocation generation keeps the capability check from being masked by an older generation fence.
    assignment.generation += 1;
    let prior = held(&s).await;
    assert_eq!(
        s.reserve(next.clone(), assignment.clone()).await,
        Err(Error::Conflict)
    );
    assert_eq!(held(&s).await, prior);
    let mut upgraded = node("first");
    upgraded.memory_control_version = 2;
    upgraded.capacity = Resources {
        cpu_millis: 2000,
        memory_bytes: 512 * MIB,
        disk_bytes: 0,
    };
    s.register_session(
        upgraded,
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
    s.reserve(next, assignment).await.unwrap();
    assert_eq!(held(&s).await, prior + 128 * MIB);
}
