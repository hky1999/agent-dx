use super::host_budget::ReplyLossSink;
use super::*;
use adx_coordinator::storage::HostBudget;
use std::collections::BTreeMap;

fn host_capacity() -> Resources {
    Resources {
        cpu_millis: 600,
        memory_bytes: 384,
        disk_bytes: 0,
    }
}
fn sized_create(id: &str, memory: u64) -> pb::CreateEnvironmentRequest {
    let mut value = spec(id);
    value.resources.memory_bytes = memory;
    pb::CreateEnvironmentRequest {
        spec: Some(value.into()),
        ..create(id)
    }
}
struct Fixture {
    _redis: common::Redis,
    _servers: Servers,
    _checkpoints: tempfile::TempDir,
    session: Session,
    coordinator: CoordinatorRpc,
    backend: Arc<Backend>,
    client: pb::coordinator_service_client::CoordinatorServiceClient<Channel>,
    owner: pb::coordinator_service_client::CoordinatorServiceClient<Channel>,
    report: pb::RegisterNodeRequest,
    lifecycle: pb::node_service_client::NodeServiceClient<Channel>,
}
impl Fixture {
    async fn new() -> Self {
        let redis = common::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        session
            .configure_host_budgets(BTreeMap::from([(
                "physical".into(),
                HostBudget {
                    capacity: host_capacity(),
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
        let checkpoint_root = tempfile::tempdir().unwrap();
        let checkpoint_store = Arc::new(
            adxlet::checkpoint::LocalCheckpointStore::new(checkpoint_root.path().into()).unwrap(),
        );
        let sink = Arc::new(ReplyLossSink {
            upstream: CoordinatorStateSink::new(
                channel(address, "node").await,
                Duration::from_secs(2),
            )
            .unwrap()
            .with_session("host-budget-boot".into()),
            lose_next: std::sync::atomic::AtomicBool::new(false),
        });
        let manager = Arc::new(
            Adxlet::new(
                "node".into(),
                backend.clone(),
                Arc::new(LocalChecks),
                Arc::new(LocalChecks),
                sink.clone(),
            )
            .with_checkpointing(checkpoint_store, Arc::new(LocalChecks))
            .unwrap(),
        );
        // The advertised per-node capacity permits two. Only the durable host gate
        // can reject the second one before the runtime execution boundary.
        let capacity = Resources {
            cpu_millis: 1200,
            memory_bytes: 768,
            disk_bytes: 0,
        };
        manager
            .update_capacity(capacity, Duration::from_secs(60))
            .unwrap();
        let node = NodeRpc::new(manager.clone(), peers(), "host-budget-boot".into());
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
            memory_control_version: 0,
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
                history_reclamation: false,
                include_history: false,
            })
            .await
            .unwrap();
        report.heartbeat_sequence += 1;
        report.reconciling = false;
        report.accepting_allocations = true;
        owner.register_node(report.clone()).await.unwrap();
        let client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            channel(address, "apiserver").await,
        );

        let lifecycle = pb::node_service_client::NodeServiceClient::new(
            channel(node_address, "apiserver").await,
        );
        Self {
            _redis: redis,
            _servers: servers,
            _checkpoints: checkpoint_root,
            session,
            coordinator,
            backend,
            client,
            owner,
            report,
            lifecycle,
        }
    }
    async fn pause(&mut self, record: pb::EnvironmentRecord) {
        self.lifecycle
            .pause_environment(pb::PauseEnvironmentRequest {
                assignment: record.assignment,
                caller: caller(),
                operation_id: "release-first".into(),
                expected_revision: record.revision,
                ttl_seconds: 600,
                timeout_seconds: 30,
                resources: None,
            })
            .await
            .unwrap();
    }
    async fn accepting(&mut self, value: bool) {
        self.report.heartbeat_sequence += 1;
        self.report.accepting_allocations = value;
        self.owner.register_node(self.report.clone()).await.unwrap();
    }
    async fn queued(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if self.coordinator.metrics().await.is_ok_and(|m| {
                    m.contains(&format!(
                        "adx_coordinator_queued_requests{{shard_id=\"0\"}} {expected}\n"
                    ))
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn host_budget_rejected_create_does_not_reappear_after_release() {
    let mut f = Fixture::new().await;
    let first = f
        .client
        .create_environment(sized_create("first", 256))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(
        f.client
            .create_environment(sized_create("rejected", 256))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    f.pause(first).await;
    // Exercise the background scheduler explicitly after the original hold is gone.
    for _ in 0..3 {
        f.coordinator.recover_environments().await.unwrap();
    }
    assert_eq!(
        f.session.get("rejected").await,
        Err(adx_core::Error::NotFound),
        "a definitive rejection must never create a later reservation"
    );
    assert_eq!(
        f.session.host_budgets().await.unwrap()["physical"].available,
        host_capacity()
    );
    assert_eq!(f.backend.started.load(Ordering::SeqCst), 1);
    f.queued(0).await;
    // A new explicit attempt with the same ID is allowed once capacity is free.
    f.client
        .create_environment(sized_create("rejected", 256))
        .await
        .unwrap();
    assert_eq!(f.backend.started.load(Ordering::SeqCst), 2);
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn host_budget_rejected_create_does_not_block_a_smaller_request() {
    let mut f = Fixture::new().await;
    f.client
        .create_environment(sized_create("first", 256))
        .await
        .unwrap();
    assert_eq!(
        f.client
            .create_environment(sized_create("rejected", 256))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    f.client
        .create_environment(sized_create("fits", 128))
        .await
        .expect("a rejected request must not reject another caller that fits");
    assert_eq!(
        f.session.get("rejected").await,
        Err(adx_core::Error::NotFound)
    );
    assert_eq!(
        f.session.host_budgets().await.unwrap()["physical"]
            .held
            .memory_bytes,
        384
    );
    assert_eq!(f.backend.started.load(Ordering::SeqCst), 2);
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn host_budget_round_returns_rejection_only_to_its_own_caller() {
    let mut f = Fixture::new().await;
    f.accepting(false).await;
    let mut first = f.client.clone();
    let first =
        tokio::spawn(async move { first.create_environment(sized_create("first", 384)).await });
    f.queued(1).await;
    let mut second = f.client.clone();
    let second = tokio::spawn(async move {
        second
            .create_environment(sized_create("rejected", 128))
            .await
    });
    f.queued(2).await;
    f.accepting(true).await;
    first
        .await
        .unwrap()
        .expect("a committed request cannot inherit a different request's rejection");
    assert_eq!(
        second.await.unwrap().unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        f.session.get("rejected").await,
        Err(adx_core::Error::NotFound)
    );
    assert_eq!(f.backend.started.load(Ordering::SeqCst), 1);
    f.queued(0).await;
}
