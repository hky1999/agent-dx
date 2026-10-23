use super::*;

fn fixture() -> (Vec<pb::NodeEndpoint>, Vec<pb::HostBudgetStatus>) {
    let capacity = pb::Resources {
        cpu_millis: 4000,
        memory_bytes: 8 * 1_048_576,
        disk_bytes: 16 * 1_048_576,
    };
    let held = pb::Resources {
        cpu_millis: 1000,
        memory_bytes: 2 * 1_048_576 + 17,
        disk_bytes: 4 * 1_048_576,
    };
    let available = pb::Resources {
        cpu_millis: 3000,
        memory_bytes: 6 * 1_048_576 - 17,
        disk_bytes: 12 * 1_048_576,
    };
    let nodes = ["first", "second"]
        .into_iter()
        .map(|id| pb::NodeEndpoint {
            node_id: id.into(),
            accepting_allocations: true,
            capacity: Some(capacity),
            allocatable: Some(available),
            ..Default::default()
        })
        .collect();
    let hosts = vec![pb::HostBudgetStatus {
        host_id: "physical".into(),
        capacity: Some(capacity),
        held: Some(held),
        available: Some(available),
        nodes: vec!["first".into(), "second".into()],
    }];
    (nodes, hosts)
}

#[test]
fn host_budget_view_counts_shared_capacity_once_and_keeps_byte_precision() {
    let (nodes, hosts) = fixture();
    let value = legacy_resource_view(nodes.clone(), &hosts, "request");
    assert_eq!(
        value["resource"]["capacity"]["resources"]["CPU"]["scalar"]["value"],
        4000
    );
    assert_eq!(
        value["resource"]["allocatable"]["resources"]["CPU"]["scalar"]["value"],
        3000
    );
    assert_eq!(
        value["resource"]["held"]["resources"]["CPU"]["scalar"]["value"],
        1000
    );
    let public = resource_view(nodes, &hosts);
    assert_eq!(public["host_budget_configured"], true);
    assert_eq!(
        public["host_budgets"][0]["held"]["memory_bytes"],
        2 * 1_048_576 + 17
    );
    assert_eq!(
        public["host_budgets"][0]["available"]["memory_bytes"],
        6 * 1_048_576 - 17
    );
    assert_eq!(public["items"][0]["host_id"], "physical");
    assert_eq!(public["items"][1]["host_id"], "physical");
}

#[test]
fn host_budget_view_keeps_offline_holds_and_limits_online_admission() {
    let (mut nodes, hosts) = fixture();
    nodes[0].allocatable.as_mut().unwrap().cpu_millis = 750;
    nodes[1].accepting_allocations = false;
    let value = legacy_resource_view(nodes.clone(), &hosts, "request");
    assert_eq!(
        value["resource"]["allocatable"]["resources"]["CPU"]["scalar"]["value"],
        750
    );
    nodes[0].accepting_allocations = false;
    for view in [
        resource_view(nodes.clone(), &hosts),
        resource_view(vec![], &hosts),
    ] {
        let host = &view["host_budgets"][0];
        assert_eq!(host["capacity"]["cpu_millis"], 4000);
        assert_eq!(host["held"]["cpu_millis"], 1000);
        assert_eq!(host["available"]["cpu_millis"], 3000);
        assert_eq!(host["allocatable"]["cpu_millis"], 0);
    }
    let value = legacy_resource_view(nodes, &hosts, "request");
    assert_eq!(
        value["resource"]["capacity"]["resources"]["CPU"]["scalar"]["value"],
        4000
    );
    assert_eq!(
        value["resource"]["available"]["resources"]["CPU"]["scalar"]["value"],
        3000
    );
    assert_eq!(
        value["resource"]["allocatable"]["resources"]["CPU"]["scalar"]["value"],
        0
    );
}

#[test]
fn host_budget_view_sums_distinct_hosts_and_excludes_unmapped_admission() {
    let (mut nodes, mut hosts) = fixture();
    let mut other = hosts[0].clone();
    other.host_id = "other-host".into();
    other.nodes = vec!["third".into()];
    let mut third = nodes[0].clone();
    third.node_id = "third".into();
    nodes.push(third);
    let mut unmapped = nodes[0].clone();
    unmapped.node_id = "unmapped".into();
    nodes.push(unmapped);
    hosts.push(other);
    let value = legacy_resource_view(nodes.clone(), &hosts, "request");
    assert_eq!(
        value["resource"]["capacity"]["resources"]["CPU"]["scalar"]["value"],
        8000
    );
    assert_eq!(
        value["resource"]["held"]["resources"]["CPU"]["scalar"]["value"],
        2000
    );
    assert_eq!(
        value["resource"]["allocatable"]["resources"]["CPU"]["scalar"]["value"],
        6000
    );
    let value = resource_view(nodes, &hosts);
    assert_eq!(value["items"][3]["status"], 1);
    assert_eq!(value["items"][3]["allocatable"]["CPU"], 0);
    assert!(value["items"][3]["host_id"].is_null());
}

#[tokio::test]
async fn both_resource_schemas_publish_snapshot_version_and_forbid_http_caching() {
    for legacy in [false, true] {
        let (nodes, hosts) = fixture();
        let frame = pb::NodeDirectory {
            epoch: 9,
            control_revision: 123,
            valid_for_millis: 1000,
            nodes,
            host_budgets: hosts,
            host_budget_configured: true,
        };
        let response = resource_response(frame, legacy.then_some("request"));
        assert_eq!(response.headers()["cache-control"], "no-store");
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let view = if legacy { &body["resource"] } else { &body };
        assert_eq!(view["budget_snapshot"], json!({"epoch":9,"revision":123}));
        assert_eq!(view["host_budgets"][0]["held"]["cpu_millis"], 1000);
    }
}
