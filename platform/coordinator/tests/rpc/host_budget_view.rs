use super::*;
use adx_coordinator::{
    storage::{HostBudget, NodeSession},
    Node,
};
use std::collections::BTreeMap;

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn host_budget_watch_preserves_offline_reservations_and_closes_on_storage_loss() {
    let mut redis = common::Redis::new().await;
    let store = redis.store().await;
    let session = store.begin(1).await.unwrap();
    let capacity = spec("capacity").resources;
    session
        .configure_host_budgets(BTreeMap::from([(
            "physical".into(),
            HostBudget {
                capacity,
                nodes: ["node".into(), "second".into()].into(),
            },
        )]))
        .await
        .unwrap();
    session
        .register_session(
            Node {
                id: "node".into(),
                capacity,
                available: true,
                runtime_classes: vec!["runc".into()],
                labels: Default::default(),
                devices: vec![],
            },
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            Some(NodeSession {
                id: "before-restart".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    session
        .reserve(
            spec("unknown"),
            adx_core::Assignment {
                environment_id: "unknown".into(),
                node_id: "node".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        )
        .await
        .unwrap();
    let session = store.begin(1).await.unwrap();
    let coordinator = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(coordinator))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );
    let mut watch = client
        .watch_nodes(pb::WatchNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    let frame = watch.message().await.unwrap().unwrap();
    assert!(
        frame.nodes.is_empty(),
        "a restarted Coordinator has no fresh node heartbeat"
    );
    assert_eq!(frame.epoch, session.epoch());
    assert!(frame.host_budget_configured);
    assert_eq!(frame.host_budgets.len(), 1);
    assert_eq!(frame.host_budgets[0].host_id, "physical");
    assert_eq!(frame.host_budgets[0].capacity, Some(capacity.into()));
    assert_eq!(frame.host_budgets[0].held, Some(capacity.into()));
    assert_eq!(
        frame.host_budgets[0].available,
        Some(pb::Resources::default())
    );
    assert_eq!(frame.host_budgets[0].nodes, ["node", "second"]);
    redis.crash();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match watch.message().await {
                Ok(Some(_)) => {}
                other => break other,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    redis.start().await;
    let status = session.host_budgets().await.unwrap();
    assert_eq!(status["physical"].held, capacity);
}
