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
