use super::*;
use adx_coordinator::storage::{HostBudget, OverheadReservation};
use std::collections::BTreeMap;
fn hold(cpu: u64, memory: u64) -> OverheadReservation {
    OverheadReservation {
        host_id: "host".into(),
        owner_sha256: "a".repeat(64),
        resources: Resources {
            cpu_millis: cpu,
            memory_bytes: memory,
            disk_bytes: 0,
        },
        retirement_proof_sha256: None,
    }
}
async fn setup() -> (common::Redis, adx_coordinator::storage::Session) {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .configure_host_budgets(BTreeMap::from([(
            "host".into(),
            HostBudget {
                capacity: spec("pool").resources,
                nodes: ["node".into()].into(),
            },
        )]))
        .await
        .unwrap();
    session
        .register_session(
            node("node"),
            "node:1".into(),
            "node:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "session".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    (rig, session)
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_shares_allocation_cas_and_replays_without_second_hold() {
    let (_rig, session) = setup().await;
    let h = hold(20, 32);
    session.reserve_overhead("proxy", h.clone()).await.unwrap();
    session.reserve_overhead("proxy", h.clone()).await.unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        h.resources
    );
    assert_eq!(
        session.host_budgets().await.unwrap()["host"]
            .memory_accounts
            .overhead_bytes,
        32
    );
    assert_eq!(
        session
            .reserve(
                spec("vm"),
                Assignment {
                    environment_id: "vm".into(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: 1,
                    devices: vec![]
                }
            )
            .await,
        Err(Error::NoCapacity)
    );
    let mut changed = h.clone();
    changed.resources.cpu_millis = 1;
    assert_eq!(
        session.reserve_overhead("proxy", changed).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .retire_overhead("proxy", &"b".repeat(64), &"c".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    session
        .retire_overhead("proxy", &h.owner_sha256, &"c".repeat(64))
        .await
        .unwrap();
    session
        .retire_overhead("proxy", &h.owner_sha256, &"c".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        Resources::default()
    );
    assert_eq!(
        session.reserve_overhead("proxy", h).await,
        Err(Error::Conflict)
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_unknown_survives_storage_restart_and_old_epoch_is_fenced() {
    let (mut rig, session) = setup().await;
    let h = hold(100, 128);
    session
        .reserve_overhead("checkpoint", h.clone())
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let store = rig.store().await;
    let next = store.begin(1).await.unwrap();
    assert_eq!(next.host_budgets().await.unwrap()["host"].held, h.resources);
    let stale = session
        .retire_overhead("checkpoint", &h.owner_sha256, &"c".repeat(64))
        .await;
    assert!(matches!(
        stale,
        Err(Error::Conflict | Error::Unavailable(_))
    ));
    assert_eq!(
        session
            .retire_overhead("checkpoint", &h.owner_sha256, &"c".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(next.host_budgets().await.unwrap()["host"].held, h.resources);
    assert_eq!(
        store.attach_overhead_session(session.epoch()).await.err(),
        Some(Error::Conflict)
    );
    let attached = store.attach_overhead_session(next.epoch()).await.unwrap();
    attached
        .retire_overhead("checkpoint", &h.owner_sha256, &"c".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        next.host_budgets().await.unwrap()["host"].held,
        Resources::default()
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_races_compete_for_one_host_and_reject_unmapped_owner() {
    let (_rig, s) = setup().await;
    let (a, b) = tokio::join!(
        s.reserve_overhead("a", hold(100, 128)),
        s.reserve_overhead("b", hold(100, 128))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let mut bad = hold(1, 1);
    bad.host_id = "unknown".into();
    assert_eq!(s.reserve_overhead("bad", bad).await, Err(Error::Conflict));
    assert_eq!(
        s.retire_overhead("a", &"a".repeat(64), "no-proof").await,
        Err(Error::Invalid("SHA256 required".into()))
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_archive_reclaims_505_rows_but_never_reuses_old_ids() {
    let (_rig, s) = setup().await;
    for i in 0..505 {
        let id = format!("retired-{i}");
        s.reserve_overhead(&id, hold(1, 1)).await.unwrap();
        s.retire_overhead(&id, &"a".repeat(64), &"b".repeat(64))
            .await
            .unwrap();
    }
    for i in 0..7 {
        s.reserve_overhead(&format!("active-{i}"), hold(1, 1))
            .await
            .unwrap();
    }
    assert_eq!(
        s.reserve_overhead("full", hold(1, 1)).await,
        Err(Error::NoCapacity)
    );
    let before = s.host_budgets().await.unwrap();
    let archive = s
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    assert_eq!(archive.reservations.len(), 505);
    let canonical = archive.canonical_json().unwrap();
    assert_eq!(
        serde_json::from_str::<adx_coordinator::storage::OverheadArchive>(&canonical).unwrap(),
        archive
    );
    let history = s
        .archive_overhead(archive.clone(), &"c".repeat(64))
        .await
        .unwrap();
    assert_eq!(history.generation, 1);
    assert_eq!(history.retired_count, 505);
    assert_eq!(s.overhead_reservations().await.unwrap().len(), 7);
    assert_eq!(s.host_budgets().await.unwrap(), before);
    let revision = s.revision().await.unwrap();
    assert_eq!(
        s.archive_overhead(archive.clone(), &"c".repeat(64))
            .await
            .unwrap(),
        history
    );
    assert_eq!(s.revision().await.unwrap(), revision);
    assert_eq!(
        s.archive_overhead(archive, &"d".repeat(64)).await,
        Err(Error::Conflict)
    );
    for id in [
        "retired-0",
        "never-issued",
        "oh-0-new",
        "oh-01-new",
        "oh-2-new",
    ] {
        assert_eq!(
            s.reserve_overhead(id, hold(1, 1)).await,
            Err(Error::Conflict)
        );
    }
    s.reserve_overhead("active-0", hold(1, 1)).await.unwrap();
    s.reserve_overhead("oh-1-new", hold(1, 1)).await.unwrap();
    s.retire_overhead("oh-1-new", &"a".repeat(64), &"e".repeat(64))
        .await
        .unwrap();
    let next = s
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    assert_eq!(next.previous, Some(history));
    let second = s.archive_overhead(next, &"f".repeat(64)).await.unwrap();
    assert_eq!(second.generation, 2);
    assert_eq!(second.retired_count, 506);
    assert_eq!(
        s.reserve_overhead("oh-1-never-issued", hold(1, 1)).await,
        Err(Error::Conflict)
    );
    s.reserve_overhead("oh-2-new", hold(1, 1)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_archive_receipt_survives_aof_restart_and_fences_old_session() {
    let (mut rig, s) = setup().await;
    s.reserve_overhead("old", hold(1, 1)).await.unwrap();
    s.retire_overhead("old", &"a".repeat(64), &"b".repeat(64))
        .await
        .unwrap();
    let archive = s
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    let receipt = s
        .archive_overhead(archive.clone(), &"c".repeat(64))
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let next = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        next.overhead_history().await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        next.archive_overhead(archive.clone(), &"c".repeat(64))
            .await
            .unwrap(),
        receipt
    );
    // The first read may observe the retired Redis transport. Reconnection must
    // still fence this old session without changing the accepted chain or holds.
    let stale = s.archive_overhead(archive.clone(), &"c".repeat(64)).await;
    assert!(matches!(
        stale,
        Err(Error::Conflict | Error::Unavailable(_))
    ));
    assert_eq!(
        s.archive_overhead(archive, &"c".repeat(64)).await,
        Err(Error::Conflict)
    );
    assert_eq!(next.overhead_history().await.unwrap(), Some(receipt));
    assert_eq!(
        next.host_budgets().await.unwrap()["host"].held,
        Resources::default()
    );
    assert_eq!(
        next.reserve_overhead("old", hold(1, 1)).await,
        Err(Error::Conflict)
    );
    next.reserve_overhead("oh-1-new", hold(1, 1)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_archive_rejects_forgery_live_rows_and_incomplete_acceptance() {
    let (_rig, s) = setup().await;
    s.reserve_overhead("live", hold(1, 1)).await.unwrap();
    s.reserve_overhead("old", hold(1, 1)).await.unwrap();
    s.retire_overhead("old", &"a".repeat(64), &"b".repeat(64))
        .await
        .unwrap();
    let original = s
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    for i in 0..4 {
        let mut bad = original.clone();
        match i {
            0 => bad.authority.push_str("-other"),
            1 => {
                bad.reservations.insert("live".into(), hold(1, 1));
            }
            2 => {
                bad.reservations
                    .get_mut("old")
                    .unwrap()
                    .retirement_proof_sha256 = Some("d".repeat(64))
            }
            _ => bad.generation += 1,
        }
        assert_eq!(
            s.archive_overhead(bad, &"c".repeat(64)).await,
            Err(Error::Conflict)
        );
    }
    assert!(s.archive_overhead(original.clone(), "").await.is_err());
    assert_eq!(s.overhead_reservations().await.unwrap().len(), 2);
    assert_eq!(s.overhead_history().await.unwrap(), None);
    s.archive_overhead(original, &"c".repeat(64)).await.unwrap();
    assert_eq!(s.overhead_reservations().await.unwrap()["live"], hold(1, 1));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_archive_concurrent_acceptances_cannot_publish_two_chain_heads() {
    let (_rig, s) = setup().await;
    s.reserve_overhead("old", hold(1, 1)).await.unwrap();
    s.retire_overhead("old", &"a".repeat(64), &"b".repeat(64))
        .await
        .unwrap();
    let archive = s
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    let first_acceptance = "c".repeat(64);
    let second_acceptance = "d".repeat(64);
    let (a, b) = tokio::join!(
        s.archive_overhead(archive.clone(), &first_acceptance),
        s.archive_overhead(archive, &second_acceptance)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(s.overhead_reservations().await.unwrap().is_empty());
    assert_eq!(
        s.overhead_history().await.unwrap().unwrap().retired_count,
        1
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_archive_first_generation_is_above_every_legacy_prefixed_id() {
    let (_rig, s) = setup().await;
    s.reserve_overhead("oh-900-future", hold(1, 1))
        .await
        .unwrap();
    s.retire_overhead("oh-900-future", &"a".repeat(64), &"b".repeat(64))
        .await
        .unwrap();
    let archive = s
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    assert_eq!(archive.generation, 901);
    s.archive_overhead(archive, &"c".repeat(64)).await.unwrap();
    assert_eq!(
        s.reserve_overhead("oh-900-future", hold(1, 1)).await,
        Err(Error::Conflict)
    );
    s.reserve_overhead("oh-901-new", hold(1, 1)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_archive_protects_live_custody_predecessor_without_reopening_ids() {
    let (_rig, s) = setup().await;
    for id in ["cold-predecessor", "unreferenced"] {
        s.reserve_overhead(id, hold(1, 1)).await.unwrap();
        s.retire_overhead(id, &"a".repeat(64), &"b".repeat(64))
            .await
            .unwrap();
    }
    let protected = ["cold-predecessor".to_owned()].into();
    let archive = s.prepare_overhead_archive(&protected).await.unwrap();
    assert_eq!(archive.protected_ids, protected);
    assert_eq!(archive.reservations.len(), 1);
    assert!(archive.reservations.contains_key("unreferenced"));
    s.archive_overhead(archive, &"c".repeat(64)).await.unwrap();
    let rows = s.overhead_reservations().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows["cold-predecessor"].retirement_proof_sha256,
        Some("b".repeat(64))
    );
    assert_eq!(
        s.reserve_overhead("cold-predecessor", hold(1, 1)).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        s.overhead_history().await.unwrap().unwrap().protected_ids,
        protected
    );
    s.reserve_overhead("oh-1-unreferenced", hold(1, 1))
        .await
        .unwrap();
    s.retire_overhead("oh-1-unreferenced", &"a".repeat(64), &"d".repeat(64))
        .await
        .unwrap();
    let next = s
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    assert_eq!(next.protected_ids, protected);
    assert!(!next.reservations.contains_key("cold-predecessor"));
    let mut forged = next.clone();
    forged.protected_ids.clear();
    assert_eq!(
        s.archive_overhead(forged, &"e".repeat(64)).await,
        Err(Error::Conflict)
    );
    s.archive_overhead(next, &"e".repeat(64)).await.unwrap();
    assert!(s
        .overhead_reservations()
        .await
        .unwrap()
        .contains_key("cold-predecessor"));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_predispatch_withdrawal_closes_absent_reservation() {
    let (_rig, session) = setup().await;
    let original = hold(20, 32);
    let result = session
        .withdraw_overhead_before_effect("withdraw-absent", original.clone(), &"c".repeat(64))
        .await;
    assert!(
        result.is_ok(),
        "an absent pre-dispatch intent must be closed against a late reserve: {result:?}"
    );
    assert_eq!(
        session.reserve_overhead("withdraw-absent", original).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        Resources::default()
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_replays_full_intent_but_rejects_changed_proof_or_owner() {
    use adx_coordinator::storage::OverheadWithdrawalFence;
    let (_rig, session) = setup().await;
    let original = hold(20, 32);
    session
        .reserve_overhead("checkpoint-close", original.clone())
        .await
        .unwrap();
    let first = session
        .withdraw_overhead_before_effect("checkpoint-close", original.clone(), &"c".repeat(64))
        .await
        .unwrap();
    assert_eq!(first.fence, OverheadWithdrawalFence::RetiredReservation);
    let revision = session.revision().await.unwrap();
    assert_eq!(
        session
            .withdraw_overhead_before_effect("checkpoint-close", original.clone(), &"c".repeat(64))
            .await
            .unwrap(),
        first
    );
    assert_eq!(session.revision().await.unwrap(), revision);
    let mut changed = original.clone();
    changed.resources.memory_bytes += 1;
    assert_eq!(
        session
            .withdraw_overhead_before_effect("checkpoint-close", changed, &"c".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    let mut changed = original.clone();
    changed.owner_sha256 = "d".repeat(64);
    assert_eq!(
        session
            .withdraw_overhead_before_effect("checkpoint-close", changed, &"c".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .withdraw_overhead_before_effect("checkpoint-close", original.clone(), &"d".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session.reserve_overhead("checkpoint-close", original).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session.host_budgets().await.unwrap()["host"].held,
        Resources::default()
    );
    assert_eq!(session.revision().await.unwrap(), revision);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_never_needs_positive_capacity_or_changes_neighbors() {
    let (_rig, session) = setup().await;
    session
        .reserve_overhead("full-host", hold(100, 128))
        .await
        .unwrap();
    let before = session.host_budgets().await.unwrap();
    let row = session.overhead_reservations().await.unwrap()["full-host"].clone();
    assert_eq!(
        session
            .reserve_overhead("never-admitted", hold(20, 32))
            .await,
        Err(Error::NoCapacity)
    );
    session
        .withdraw_overhead_before_effect("never-admitted", hold(20, 32), &"c".repeat(64))
        .await
        .unwrap();
    assert_eq!(session.host_budgets().await.unwrap(), before);
    assert_eq!(
        session.overhead_reservations().await.unwrap()["full-host"],
        row
    );
    assert_eq!(
        session
            .reserve_overhead("never-admitted", hold(20, 32))
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .retire_overhead("still-absent", &"a".repeat(64), &"c".repeat(64))
            .await,
        Err(Error::NotFound),
        "ordinary retirement keeps its existing missing-row contract"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_validates_before_creating_any_closure() {
    let (_rig, session) = setup().await;
    let revision = session.revision().await.unwrap();
    let mut bad = hold(20, 32);
    bad.host_id = "foreign".into();
    assert_eq!(
        session
            .withdraw_overhead_before_effect("absent-invalid", bad, &"c".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    let mut bad = hold(20, 32);
    bad.retirement_proof_sha256 = Some("b".repeat(64));
    assert_eq!(
        session
            .withdraw_overhead_before_effect("absent-invalid", bad, &"c".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .withdraw_overhead_before_effect("absent-invalid", hold(20, 32), "unknown")
            .await,
        Err(Error::Invalid("SHA256 required".into()))
    );
    assert!(session
        .withdraw_overhead_before_effect("bad/id", hold(20, 32), &"c".repeat(64))
        .await
        .is_err());
    assert_eq!(session.revision().await.unwrap(), revision);
    assert!(session.overhead_reservations().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_wins_or_closes_concurrent_late_reserve() {
    let (_rig, session) = setup().await;
    for index in 0..8 {
        let id = format!("late-intent-{index}");
        let original = hold(20, 32);
        let proof = "c".repeat(64);
        let (late, closure) = tokio::join!(
            session.reserve_overhead(&id, original.clone()),
            session.withdraw_overhead_before_effect(&id, original.clone(), &proof)
        );
        assert!(matches!(late, Ok(()) | Err(Error::Conflict)), "{late:?}");
        let receipt = closure.unwrap();
        assert_eq!(receipt.reservation, original);
        assert_eq!(receipt.reservation_id, id);
        assert_eq!(
            session.overhead_reservations().await.unwrap()[&id]
                .retirement_proof_sha256
                .as_deref(),
            Some(proof.as_str())
        );
        assert_eq!(
            session.reserve_overhead(&id, original).await,
            Err(Error::Conflict)
        );
        assert_eq!(
            session.host_budgets().await.unwrap()["host"].held,
            Resources::default()
        );
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_is_durable_across_aof_and_epoch_restart() {
    let (mut rig, session) = setup().await;
    let original = hold(20, 32);
    let first = session
        .withdraw_overhead_before_effect("durable-withdraw", original.clone(), &"c".repeat(64))
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let store = rig.store().await;
    let attached = store
        .attach_overhead_session(session.epoch())
        .await
        .unwrap();
    assert_eq!(
        attached
            .withdraw_overhead_before_effect("durable-withdraw", original.clone(), &"c".repeat(64))
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        attached
            .reserve_overhead("durable-withdraw", original.clone())
            .await,
        Err(Error::Conflict)
    );
    let next = store.begin(1).await.unwrap();
    let stale = session
        .withdraw_overhead_before_effect("durable-withdraw", original.clone(), &"c".repeat(64))
        .await;
    assert!(matches!(
        stale,
        Err(Error::Conflict) | Err(Error::Unavailable(_))
    ));
    assert_eq!(
        session
            .withdraw_overhead_before_effect("durable-withdraw", original.clone(), &"c".repeat(64))
            .await,
        Err(Error::Conflict)
    );
    let replay = next
        .withdraw_overhead_before_effect("durable-withdraw", original.clone(), &"c".repeat(64))
        .await
        .unwrap();
    assert_eq!(replay.reservation, first.reservation);
    assert_eq!(replay.epoch, next.epoch());
    assert_ne!(replay.epoch, first.epoch);
    assert_eq!(
        next.reserve_overhead("durable-withdraw", original).await,
        Err(Error::Conflict)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_full_history_generation_stays_closed_after_archive() {
    use adx_coordinator::storage::OverheadWithdrawalFence;
    let (_rig, session) = setup().await;
    session
        .reserve_overhead("history-seed", hold(1, 1))
        .await
        .unwrap();
    session
        .retire_overhead("history-seed", &"a".repeat(64), &"b".repeat(64))
        .await
        .unwrap();
    let seed = session
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    session
        .archive_overhead(seed, &"c".repeat(64))
        .await
        .unwrap();
    for index in 0..512 {
        let id = format!("oh-1-full-{index}");
        session.reserve_overhead(&id, hold(1, 1)).await.unwrap();
        session
            .retire_overhead(&id, &"a".repeat(64), &"b".repeat(64))
            .await
            .unwrap();
    }
    let before = session.overhead_reservations().await.unwrap();
    let revision = session.revision().await.unwrap();
    let id = "oh-1-absent-original";
    let proof = "d".repeat(64);
    let closed = session
        .withdraw_overhead_before_effect(id, hold(20, 32), &proof)
        .await
        .unwrap();
    assert_eq!(closed.fence, OverheadWithdrawalFence::FullHistoryGeneration);
    assert_eq!(closed.history_generation, Some(1));
    assert_eq!(session.revision().await.unwrap(), revision);
    assert_eq!(session.overhead_reservations().await.unwrap(), before);
    assert_eq!(
        session.reserve_overhead(id, hold(20, 32)).await,
        Err(Error::NoCapacity)
    );
    let archive = session
        .prepare_overhead_archive(&Default::default())
        .await
        .unwrap();
    session
        .archive_overhead(archive, &"e".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        session.reserve_overhead(id, hold(20, 32)).await,
        Err(Error::Conflict)
    );
    let replay = session
        .withdraw_overhead_before_effect(id, hold(20, 32), &proof)
        .await
        .unwrap();
    assert_eq!(replay.fence, OverheadWithdrawalFence::PastHistoryGeneration);
    assert_eq!(replay.history_generation, Some(2));
    assert!(session.overhead_reservations().await.unwrap().is_empty());
    for invalid in [
        "oh-3-future",
        "oh-02-leading",
        "oh-0-zero",
        "legacy-unknown",
        "oh-1-",
    ] {
        assert_eq!(
            session
                .withdraw_overhead_before_effect(invalid, hold(20, 32), &proof)
                .await,
            Err(Error::Conflict)
        );
    }
    session
        .reserve_overhead("oh-2-next", hold(20, 32))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_full_legacy_table_does_not_claim_future_identity_closed() {
    let (_rig, session) = setup().await;
    for index in 0..512 {
        let id = format!("legacy-full-{index}");
        session.reserve_overhead(&id, hold(1, 1)).await.unwrap();
        session
            .retire_overhead(&id, &"a".repeat(64), &"b".repeat(64))
            .await
            .unwrap();
    }
    let revision = session.revision().await.unwrap();
    assert_eq!(
        session
            .withdraw_overhead_before_effect("oh-900-future", hold(20, 32), &"c".repeat(64))
            .await,
        Err(Error::NoCapacity)
    );
    assert_eq!(session.revision().await.unwrap(), revision);
    assert_eq!(session.overhead_reservations().await.unwrap().len(), 512);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn overhead_withdrawal_cli_closes_original_intent_without_advancing_epoch() {
    use std::process::{Command, Stdio};
    let (rig, session) = setup().await;
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({
            "redis_url": rig.url, "namespace": "test"
        }))
        .unwrap(),
    )
    .unwrap();
    let original = hold(20, 32);
    let command = serde_json::json!({
        "op":"withdraw_before_effect", "epoch":session.epoch(),
        "id":"cli-original", "reservation":original, "retirement_proof_sha256":"c".repeat(64)
    });
    let before_epoch = session.epoch();
    let mut process = Command::new(env!("CARGO_BIN_EXE_adx-overhead-budget"))
        .arg("--config")
        .arg(config)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(
        &mut process.stdin.take().unwrap(),
        &serde_json::to_vec(&command).unwrap(),
    )
    .unwrap();
    let output = process.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["epoch"], before_epoch);
    assert_eq!(result["withdrawal"]["fence"], "retired_reservation");
    assert_eq!(
        result["withdrawal"]["reservation"],
        serde_json::to_value(&original).unwrap()
    );
    assert_eq!(
        result["reservations"]["cli-original"]["retirement_proof_sha256"],
        "c".repeat(64)
    );
    assert_eq!(
        session.reserve_overhead("cli-original", original).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        rig.store()
            .await
            .attach_overhead_session(before_epoch)
            .await
            .unwrap()
            .epoch(),
        before_epoch
    );
}
