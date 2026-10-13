use super::*;
use adx_coordinator::storage::HostBudget;
use std::collections::BTreeMap;

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn host_budget_rpc_rejects_before_execution_and_recovers_after_release() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    session
        .configure_host_budgets(BTreeMap::from([(
            "physical".into(),
            HostBudget {
                capacity: spec("capacity").resources,
                nodes: ["node".into()].into(),
            },
        )]))
        .await
        .unwrap();
    let coordinator = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = coordinator.clone();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    });
    let manager = Arc::new(Adxlet::new(
        "node".into(),
        backend.clone(),
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        Arc::new(
            CoordinatorStateSink::new(channel(address, "node").await, Duration::from_secs(2))
                .unwrap()
                .with_session("host-budget-boot".into()),
        ),
    ));
    // The advertised per-node capacity permits two. Only the durable host gate
    // can reject the second one before the runtime execution boundary.
    let capacity = Resources {
        cpu_millis: spec("capacity").resources.cpu_millis * 2,
        memory_bytes: spec("capacity").resources.memory_bytes * 2,
        disk_bytes: spec("capacity").resources.disk_bytes * 2,
    };
    manager
        .update_capacity(capacity, Duration::from_secs(60))
        .unwrap();
    let node = NodeRpc::new(manager, peers(), "host-budget-boot".into());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = listener.local_addr().unwrap();
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("node"))
            .unwrap()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let mut owner = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "node").await,
    );
    let mut report = pb::RegisterNodeRequest {
        runtime_classes: vec![spec("capacity").runtime_class],
        node_id: "node".into(),
        node_address: node_address.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(capacity.into()),
        session_id: "host-budget-boot".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        accepting_allocations: false,
        ..Default::default()
    };
    owner.register_node(report.clone()).await.unwrap();
    owner
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: report.session_id.clone(),
        })
        .await
        .unwrap();
    report.heartbeat_sequence += 1;
    report.reconciling = false;
    report.accepting_allocations = true;
    owner.register_node(report).await.unwrap();
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );
    let first = client
        .create_environment(create("first"))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            client
                .create_environment(create("second"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted,
        );
        assert_eq!(backend.started.load(Ordering::SeqCst), 1);
        assert_eq!(session.get("second").await, Err(adx_core::Error::NotFound));
        assert!(
            coordinator.metrics().await.is_ok(),
            "NoCapacity must not poison scheduler health"
        );
        assert_eq!(
            session.host_budgets().await.unwrap()["physical"].available,
            Resources::default()
        );
    }
    let mut lifecycle =
        pb::node_service_client::NodeServiceClient::new(channel(node_address, "apiserver").await);
    lifecycle
        .delete_environment(pb::DeleteEnvironmentRequest {
            assignment: first.assignment,
            caller: caller(),
            failed_revision: None,
            node_session_id: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"].available,
        spec("capacity").resources
    );
    let second = client
        .create_environment(create("second"))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(backend.started.load(Ordering::SeqCst), 2);
    lifecycle
        .delete_environment(pb::DeleteEnvironmentRequest {
            assignment: second.assignment,
            caller: caller(),
            failed_revision: None,
            node_session_id: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(backend.removed.load(Ordering::SeqCst), 2);
    assert!(backend.running.lock().unwrap().is_empty());
    assert!(session.snapshot().await.unwrap().environments.is_empty());
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"].available,
        spec("capacity").resources
    );
}
