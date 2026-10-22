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
