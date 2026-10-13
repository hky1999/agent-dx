use super::*;

fn frame() -> pb::NodeDirectory {
    pb::NodeDirectory {
        epoch: 1,
        valid_for_millis: 1000,
        host_budget_configured: true,
        host_budgets: vec![pb::HostBudgetStatus {
            host_id: "physical".into(),
            nodes: vec!["node".into()],
            capacity: Some(pb::Resources {
                cpu_millis: 2000,
                memory_bytes: 4096,
                disk_bytes: 0,
            }),
            held: Some(pb::Resources {
                cpu_millis: 500,
                memory_bytes: 1024,
                disk_bytes: 0,
            }),
            available: Some(pb::Resources {
                cpu_millis: 1500,
                memory_bytes: 3072,
                disk_bytes: 0,
            }),
        }],
        ..Default::default()
    }
}

#[test]
fn host_budget_directory_rejects_incomplete_inconsistent_and_duplicate_views() {
    let mut invalid = Vec::new();
    let mut v = frame();
    v.host_budgets[0].held = None;
    invalid.push(v);
    let mut v = frame();
    v.host_budgets[0].available.as_mut().unwrap().cpu_millis += 1;
    invalid.push(v);
    let mut v = frame();
    v.host_budgets[0].held.as_mut().unwrap().memory_bytes = u64::MAX;
    invalid.push(v);
    let mut v = frame();
    v.host_budgets[0].nodes.push("node".into());
    invalid.push(v);
    let mut v = frame();
    v.host_budgets.push(v.host_budgets[0].clone());
    invalid.push(v);
    let mut v = frame();
    let mut second = v.host_budgets[0].clone();
    second.host_id = "other".into();
    v.host_budgets.push(second);
    invalid.push(v);
    let mut v = frame();
    v.host_budgets.clear();
    invalid.push(v);
    let mut v = frame();
    v.host_budget_configured = false;
    invalid.push(v);
    for bad in invalid {
        let mut directory = Directory::default();
        directory.update(frame()).unwrap();
        assert_eq!(
            directory.update(bad).unwrap_err().code(),
            tonic::Code::DataLoss
        );
        assert!(
            directory.snapshot().is_none(),
            "an invalid host view must revoke the previous one"
        );
    }
}

#[test]
fn host_budget_directory_expires_nodes_and_holds_together() {
    let mut directory = Directory::default();
    directory.update(frame()).unwrap();
    assert_eq!(
        directory.snapshot().unwrap().host_budgets[0]
            .held
            .unwrap()
            .cpu_millis,
        500
    );
    directory.valid_until = Some(Instant::now());
    assert!(directory.snapshot().is_none());
    assert!(directory.select().is_none());
    directory.update(frame()).unwrap();
    directory.clear();
    assert!(directory.snapshot().is_none());
}

#[test]
fn host_budget_directory_rejects_unmapped_admission_and_aggregate_overflow() {
    let mut unmapped = frame();
    unmapped.nodes.push(pb::NodeEndpoint {
        node_id: "unmapped".into(),
        address: "127.0.0.1:1".into(),
        relay_address: "127.0.0.1:2".into(),
        session_id: "boot".into(),
        accepting_allocations: true,
        capacity: Some(pb::Resources::default()),
        allocatable: Some(pb::Resources::default()),
        ..Default::default()
    });
    let mut directory = Directory::default();
    assert!(directory.update(unmapped.clone()).is_err());
    unmapped.nodes[0].accepting_allocations = false;
    directory.update(unmapped).unwrap();
    assert!(directory.select().is_none());
    let mut overflow = frame();
    let mut second = overflow.host_budgets[0].clone();
    second.host_id = "other".into();
    second.nodes = vec!["other-node".into()];
    second.capacity.as_mut().unwrap().cpu_millis = u64::MAX;
    second.held.as_mut().unwrap().cpu_millis = 0;
    second.available.as_mut().unwrap().cpu_millis = u64::MAX;
    overflow.host_budgets.push(second);
    assert!(directory.update(overflow).is_err());
    assert!(directory.snapshot().is_none());
}
