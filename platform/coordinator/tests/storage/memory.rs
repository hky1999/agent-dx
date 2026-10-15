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
    n.memory_control_version = 1;
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
