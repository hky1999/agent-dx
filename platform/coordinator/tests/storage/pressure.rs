use super::*;
use adx_coordinator::storage::{HostBudget, NodeSession, Session};
use adx_core::pressure::PressureSource;
use serde_json::json;
use std::collections::BTreeMap;

pub(super) fn source() -> (tempfile::TempDir, PressureSource) {
    let root = tempfile::tempdir().unwrap();
    let source = serde_json::from_value(json!({"path":root.path().join("pressure.json"),
        "max_age_millis":10000,"identity":{"host_id":"host","authority":"authority",
        "boot_id":std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap().trim(),"parent_device":30,"parent_inode":42,"storage_identity_sha256":"a".repeat(64)}})).unwrap();
    publish(&source, true);
    (root, source)
}
pub(super) fn publish(source: &PressureSource, accepting: bool) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    std::fs::write(&source.path, json!({"schema":1,"identity":source.identity,"sequence":1,
        "observed_unix_millis":now,"valid_until_unix_millis":now+10000,
        "accepting":accepting,"reason":if accepting {"low_watermark"} else {"high_watermark"},
        "metrics":{"host_memory_per_mille":100,"parent_memory_per_mille":100,
            "host_memory_psi_per_mille":0,"parent_memory_psi_per_mille":0,"disk_used_per_mille":100,
            "parent_memory_current_bytes":100,"parent_memory_max_bytes":1000,"disk_available_bytes":900}}).to_string()).unwrap();
}
async fn setup() -> (common::Redis, Session) {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let capacity = Resources {
        cpu_millis: 2000,
        memory_bytes: 1024,
        disk_bytes: 0,
    };
    session
        .configure_host_budgets(BTreeMap::from([(
            "host".into(),
            HostBudget {
                capacity,
                nodes: ["first".into(), "second".into()].into(),
            },
        )]))
        .await
        .unwrap();
    for id in ["first", "second"] {
        let mut n = node(id);
        n.capacity = capacity;
        session
            .register_session(
                n,
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

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn pressure_shared_host_blocks_both_nodes_and_preserves_positive_holds() {
    let (_rig, s) = setup().await;
    let (_root, source) = source();
    let policy = BTreeMap::from([("host".into(), source.clone())]);
    s.configure_host_pressure(policy.clone()).await.unwrap();
    s.reserve(spec("a"), assignment("a", "first", 1))
        .await
        .unwrap();
    publish(&source, false);
    for (id, node) in [("b", "first"), ("c", "second")] {
        assert_eq!(
            s.reserve(spec(id), assignment(id, node, 2)).await,
            Err(Error::NoCapacity)
        );
    }
    assert_eq!(
        s.host_budgets().await.unwrap()["host"].held,
        spec("a").resources
    );
    assert_eq!(
        s.configure_host_pressure(BTreeMap::new()).await,
        Err(Error::Conflict)
    );
    s.configure_host_pressure(policy).await.unwrap();
    publish(&source, true);
    s.reserve(spec("b"), assignment("b", "second", 2))
        .await
        .unwrap();
    std::fs::write(&source.path, "malformed").unwrap();
    assert_eq!(
        s.reserve(spec("c"), assignment("c", "first", 3)).await,
        Err(Error::NoCapacity)
    );
    assert_eq!(
        s.host_budgets().await.unwrap()["host"].held.memory_bytes,
        256
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn pressure_contract_survives_coordinator_restart_and_refuses_omission() {
    let (rig, s) = setup().await;
    let (_root, source) = source();
    let policy = BTreeMap::from([("host".into(), source.clone())]);
    s.configure_host_pressure(policy.clone()).await.unwrap();
    s.reserve(spec("a"), assignment("a", "first", 1))
        .await
        .unwrap();
    let next = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        next.configure_host_pressure(BTreeMap::new()).await,
        Err(Error::Conflict)
    );
    next.configure_host_pressure(policy).await.unwrap();
    std::fs::remove_file(&source.path).unwrap();
    assert_eq!(
        next.reserve(spec("b"), assignment("b", "second", 2)).await,
        Err(Error::NoCapacity)
    );
    assert_eq!(
        next.host_budgets().await.unwrap()["host"].held.memory_bytes,
        128
    );
}
