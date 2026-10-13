use super::*;

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn process_installs_host_budget_and_preserves_it_across_replay_omission_and_rejection() {
    let redis = common::Redis::new().await;
    let directory = tempfile::tempdir().unwrap();
    let tls = PathBuf::from(std::env::var("ADX_TEST_TLS_DIR").unwrap());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let budget = serde_json::json!({"physical-host": {
        "capacity": {"cpu_millis": 3000, "memory_bytes": 12884901888_u64, "disk_bytes": 17179869184_u64},
        "nodes": ["node", "second-node"]
    }});
    let key_file = directory.path().join("bootstrap.key");
    std::fs::write(&key_file, "host-budget-test-administrator-key-".repeat(3)).unwrap();
    let base = serde_json::json!({
        "listen":address.to_string(), "redis_url":redis.url, "namespace":"host-budget-process",
        "scheduler_shards":1, "placement":"pack", "rpc_timeout_seconds":2,
        "tls":{"ca":tls.join("ca.pem"),"certificate":tls.join("coordinator.pem"),"private_key":tls.join("coordinator.key"),"server_name":"localhost","peers":{"apiserver":tls.join("apiserver.der"),"node:node":tls.join("node.der")}},
        "bootstrap_credentials":[{"key_file":key_file,"tenant_id":"admin","administrator":true,"expires_at_unix_seconds":0}]
    });
    let mut connection = redis::Client::open(redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for index in 0..5 {
        let mut config = base.clone();
        match index {
            0 | 2 => config["host_budgets"] = budget.clone(),
            1 => {}
            3 => {
                config["host_budgets"] = budget.clone();
                config["host_budgets"]["physical-host"]["capacity"]["memory_bytes"] =
                    serde_json::json!(25769803776_u64);
            }
            4 => config["host_budgets"] = serde_json::json!({}),
            _ => unreachable!(),
        }
        let path = directory.path().join("coordinator.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let log = std::fs::File::create(
            PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap())
                .join(format!("host-budget-process-{index}.log")),
        )
        .unwrap();
        let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_adx-coordinator"))
            .args(["--config", path.to_str().unwrap()])
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        if index < 3 {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    assert!(
                        process.try_wait().unwrap().is_none(),
                        "Coordinator startup failed; inspect host-budget process log"
                    );
                    let endpoint =
                        tonic::transport::Endpoint::from_shared(format!("https://{address}"))
                            .unwrap()
                            .tls_config(client_tls("apiserver"))
                            .unwrap();
                    if let Ok(channel) = endpoint.connect().await {
                        let mut client =
                            pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
                        if client.watch_nodes(pb::WatchNodesRequest {}).await.is_ok() {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        } else {
            let status = tokio::time::timeout(Duration::from_secs(5), process.wait())
                .await
                .unwrap()
                .unwrap();
            assert!(
                !status.success(),
                "changing or emptying a persisted host budget must stop startup"
            );
        }
        let header: String = redis::cmd("HGET")
            .arg("adx:{host-budget-process}:control:v1")
            .arg("header")
            .query_async(&mut connection)
            .await
            .unwrap();
        let header: serde_json::Value = serde_json::from_str(&header).unwrap();
        assert_eq!(header["host_budgets"], budget, "installed physical capacity must survive omission, replay and rejected reconfiguration");
        if index < 3 {
            process.kill().await.unwrap();
        }
    }
}
