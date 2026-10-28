use super::*;
use adx_coordinator::storage::HostBudget;
use std::collections::BTreeMap;
fn body(revision: u64) -> serde_json::Value {
    serde_json::json!({"schema":1,"revision":revision,"identity":{"boot":"boot","device":1,"inode":2},"operations":{}})
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn parking_journal_replay_and_storage_restart_preserve_one_authority() {
    let mut rig = common::Redis::new().await;
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
        .publish_parking_journal("controller", &"a".repeat(64), 0, body(1))
        .await
        .unwrap();
    session
        .publish_parking_journal("controller", &"a".repeat(64), 0, body(1))
        .await
        .unwrap();
    assert_eq!(
        session
            .parking_journal("controller")
            .await
            .unwrap()
            .unwrap()
            .body,
        body(1)
    );
    assert_eq!(
        session
            .publish_parking_journal("controller", &"b".repeat(64), 1, body(2))
            .await,
        Err(Error::Conflict)
    );
    let mut stale_body = body(1);
    stale_body["operations"]["stale"] = serde_json::json!({"phase":"dispatching"});
    assert_eq!(
        session
            .publish_parking_journal("controller", &"a".repeat(64), 0, stale_body)
            .await,
        Err(Error::Conflict)
    );
    rig.crash();
    rig.start().await;
    let next = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        next.parking_journal("controller")
            .await
            .unwrap()
            .unwrap()
            .body,
        body(1)
    );
    next.publish_parking_journal("controller", &"a".repeat(64), 1, body(2))
        .await
        .unwrap();
    assert_eq!(
        next.parking_journal("controller")
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn parking_journal_rejects_unbounded_or_inconsistent_state_before_commit() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    s.configure_host_budgets(BTreeMap::from([(
        "host".into(),
        HostBudget {
            capacity: spec("pool").resources,
            nodes: ["node".into()].into(),
        },
    )]))
    .await
    .unwrap();
    let mut invalid = body(1);
    invalid["schema"] = serde_json::json!(2);
    assert!(s
        .publish_parking_journal("controller", &"a".repeat(64), 0, invalid)
        .await
        .is_err());
    let mut invalid = body(1);
    invalid["huge"] = serde_json::json!("x".repeat(2 << 20));
    assert!(s
        .publish_parking_journal("controller", &"a".repeat(64), 0, invalid)
        .await
        .is_err());
    assert!(s.parking_journal("controller").await.unwrap().is_none());
    s.publish_parking_journal("controller", &"a".repeat(64), 0, body(1))
        .await
        .unwrap();
    assert!(s
        .publish_parking_journal("controller", &"a".repeat(64), 1, body(1))
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn parking_journal_seal_requires_live_storage_and_preserves_one_immutable_prefix() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .configure_host_budgets(BTreeMap::from([(
            "host".into(),
            HostBudget {
                capacity: Resources {
                    cpu_millis: 2000,
                    memory_bytes: 2 << 30,
                    disk_bytes: 2 << 30,
                },
                nodes: ["node".into()].into(),
            },
        )]))
        .await
        .unwrap();
    let mut source = body(1);
    source["batches"] = serde_json::json!({"sequence":1,"current":"b0001"});
    source["operations"]["b0001"] =
        serde_json::json!({"phase":"sealed","archive":{"sha256":"d".repeat(64)}});
    session
        .publish_parking_journal("catalog", &"a".repeat(64), 0, source.clone())
        .await
        .unwrap();
    let resources = Resources {
        cpu_millis: 1,
        memory_bytes: 512 << 20,
        disk_bytes: 512 << 20,
    };
    let mut closed = source.clone();
    closed["schema"] = serde_json::json!(2);
    closed["revision"] = serde_json::json!(2);
    closed["history_seal"] = serde_json::json!({
        "index_sha256":"e".repeat(64),"through":1,"owner_reservation":"cold-next",
        "owner_sha256":"b".repeat(64),"contract_sha256":"c".repeat(64),"resources":resources
    });
    assert!(session
        .publish_parking_journal("catalog", &"a".repeat(64), 1, closed.clone())
        .await
        .is_err());
    session
        .reserve_overhead(
            "cold-next",
            adx_coordinator::storage::OverheadReservation {
                host_id: "host".into(),
                owner_sha256: "b".repeat(64),
                resources,
                retirement_proof_sha256: None,
            },
        )
        .await
        .unwrap();
    let mut changed = closed.clone();
    changed["operations"]["b0001"]["archive"]["sha256"] = serde_json::json!("f".repeat(64));
    assert!(session
        .publish_parking_journal("catalog", &"a".repeat(64), 1, changed)
        .await
        .is_err());
    session
        .publish_parking_journal("catalog", &"a".repeat(64), 1, closed.clone())
        .await
        .unwrap();
    session
        .publish_parking_journal("catalog", &"a".repeat(64), 1, closed.clone())
        .await
        .unwrap();
    let mut reopened = source.clone();
    reopened["revision"] = serde_json::json!(3);
    assert_eq!(
        session
            .publish_parking_journal("catalog", &"a".repeat(64), 2, reopened)
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .parking_journal("catalog")
            .await
            .unwrap()
            .unwrap()
            .body,
        closed
    );
    let mut forged = closed.clone();
    forged["revision"] = serde_json::json!(1);
    assert!(session
        .publish_parking_journal("never-existed", &"a".repeat(64), 0, forged)
        .await
        .is_err());
    rig.crash();
    rig.start().await;
    let next = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        next.parking_journal("catalog").await.unwrap().unwrap().body,
        closed
    );
    next.publish_parking_journal("catalog", &"a".repeat(64), 1, closed)
        .await
        .unwrap();
}

async fn capacity_session() -> (common::Redis, adx_coordinator::storage::Session) {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .configure_host_budgets(BTreeMap::from([(
            "host".into(),
            HostBudget {
                capacity: Resources {
                    cpu_millis: 2000,
                    memory_bytes: 2 << 30,
                    disk_bytes: 2 << 30,
                },
                nodes: ["node".into()].into(),
            },
        )]))
        .await
        .unwrap();
    session
        .reserve_overhead(
            "cold-pool",
            adx_coordinator::storage::OverheadReservation {
                host_id: "host".into(),
                owner_sha256: "b".repeat(64),
                resources: Resources {
                    cpu_millis: 1,
                    memory_bytes: 512 << 20,
                    disk_bytes: 512 << 20,
                },
                retirement_proof_sha256: None,
            },
        )
        .await
        .unwrap();
    (rig, session)
}

fn catalog_source() -> serde_json::Value {
    let mut value = body(1);
    value["batches"] = serde_json::json!({"sequence":1,"current":"b0001"});
    value["operations"]["b0001"] =
        serde_json::json!({"phase":"sealed","archive":{"sha256":"d".repeat(64)}});
    value
}

fn closed_catalog(source: &serde_json::Value) -> serde_json::Value {
    let mut value = source.clone();
    value["schema"] = serde_json::json!(2);
    value["revision"] = serde_json::json!(2);
    value["history_seal"] = serde_json::json!({
        "index_sha256":"e".repeat(64),"through":1,"owner_reservation":"cold-pool",
        "owner_sha256":"b".repeat(64),"contract_sha256":"c".repeat(64),
        "resources":{"cpu_millis":1,"memory_bytes":512 << 20,"disk_bytes":512 << 20}
    });
    value
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn sealed_catalog_preserves_all_sixteen_writable_slots_after_restart() {
    let (mut rig, session) = capacity_session().await;
    let source = catalog_source();
    let closed = closed_catalog(&source);
    session
        .publish_parking_journal("sealed", &"a".repeat(64), 0, source)
        .await
        .unwrap();
    session
        .publish_parking_journal("sealed", &"a".repeat(64), 1, closed.clone())
        .await
        .unwrap();
    for ordinal in 0..16 {
        session
            .publish_parking_journal(&format!("writer-{ordinal}"), &"a".repeat(64), 0, body(1))
            .await
            .expect("the sealed catalog must leave all sixteen writable slots available");
    }
    assert_eq!(
        session
            .publish_parking_journal("overflow", &"a".repeat(64), 0, body(1))
            .await,
        Err(Error::NoCapacity)
    );
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted
            .parking_journal("sealed")
            .await
            .unwrap()
            .unwrap()
            .body,
        closed
    );
    for ordinal in 0..16 {
        assert_eq!(
            restarted
                .parking_journal(&format!("writer-{ordinal}"))
                .await
                .unwrap()
                .unwrap()
                .body,
            body(1)
        );
    }
    restarted
        .publish_parking_journal("writer-0", &"a".repeat(64), 1, body(2))
        .await
        .unwrap();
    restarted
        .publish_parking_journal("sealed", &"a".repeat(64), 1, closed)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .publish_parking_journal("still-full", &"a".repeat(64), 0, body(1))
            .await,
        Err(Error::NoCapacity)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn sealed_catalog_count_has_its_own_bound_and_failed_seal_keeps_the_source() {
    let (_rig, session) = capacity_session().await;
    let source = catalog_source();
    let closed = closed_catalog(&source);
    for ordinal in 0..64 {
        let id = format!("history-{ordinal}");
        session
            .publish_parking_journal(&id, &"a".repeat(64), 0, source.clone())
            .await
            .unwrap();
        session
            .publish_parking_journal(&id, &"a".repeat(64), 1, closed.clone())
            .await
            .expect("all sixty-four bounded sealed catalogs must remain readable");
    }
    session
        .publish_parking_journal("next-history", &"a".repeat(64), 0, source.clone())
        .await
        .unwrap();
    assert_eq!(
        session
            .publish_parking_journal("next-history", &"a".repeat(64), 1, closed.clone())
            .await,
        Err(Error::NoCapacity)
    );
    assert_eq!(
        session
            .parking_journal("next-history")
            .await
            .unwrap()
            .unwrap()
            .body,
        source
    );
    for ordinal in 0..64 {
        assert_eq!(
            session
                .parking_journal(&format!("history-{ordinal}"))
                .await
                .unwrap()
                .unwrap()
                .body,
            closed
        );
    }
    session
        .publish_parking_journal("history-0", &"a".repeat(64), 1, closed)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn sealed_catalog_bytes_have_a_cumulative_bound_before_the_count_limit() {
    let (_rig, session) = capacity_session().await;
    let mut source = catalog_source();
    source["padding"] = serde_json::json!("x".repeat((2 << 20) - 4096));
    let closed = closed_catalog(&source);
    assert!(serde_json::to_vec(&closed).unwrap().len() < 2 << 20);
    for ordinal in 0..8 {
        let id = format!("large-history-{ordinal}");
        session
            .publish_parking_journal(&id, &"a".repeat(64), 0, source.clone())
            .await
            .unwrap();
        session
            .publish_parking_journal(&id, &"a".repeat(64), 1, closed.clone())
            .await
            .unwrap();
    }
    session
        .publish_parking_journal("large-overflow", &"a".repeat(64), 0, source.clone())
        .await
        .unwrap();
    assert_eq!(
        session
            .publish_parking_journal("large-overflow", &"a".repeat(64), 1, closed.clone())
            .await,
        Err(Error::NoCapacity)
    );
    assert_eq!(
        session
            .parking_journal("large-overflow")
            .await
            .unwrap()
            .unwrap()
            .body,
        source
    );
    session
        .publish_parking_journal("large-history-0", &"a".repeat(64), 1, closed.clone())
        .await
        .unwrap();
    assert_eq!(
        session
            .parking_journal("large-history-0")
            .await
            .unwrap()
            .unwrap()
            .body,
        closed
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn sealed_catalog_does_not_duplicate_the_last_writable_slot_under_concurrency() {
    let (_rig, session) = capacity_session().await;
    let source = catalog_source();
    let closed = closed_catalog(&source);
    let owner = "a".repeat(64);
    session
        .publish_parking_journal("sealed", &owner, 0, source)
        .await
        .unwrap();
    session
        .publish_parking_journal("sealed", &owner, 1, closed.clone())
        .await
        .unwrap();
    for ordinal in 0..15 {
        session
            .publish_parking_journal(&format!("writer-{ordinal}"), &owner, 0, body(1))
            .await
            .unwrap();
    }
    let (first, second) = tokio::join!(
        session.publish_parking_journal("last-a", &owner, 0, body(1)),
        session.publish_parking_journal("last-b", &owner, 0, body(1))
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        usize::from(first == Err(Error::NoCapacity))
            + usize::from(second == Err(Error::NoCapacity)),
        1
    );
    let live = usize::from(session.parking_journal("last-a").await.unwrap().is_some())
        + usize::from(session.parking_journal("last-b").await.unwrap().is_some());
    assert_eq!(live, 1);
    assert_eq!(
        session
            .parking_journal("sealed")
            .await
            .unwrap()
            .unwrap()
            .body,
        closed
    );
}
