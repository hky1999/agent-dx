use super::*;
use adx_coordinator::storage::HostBudget;
use adxlet::StateSink;
use std::collections::BTreeMap;

struct ReplyLossSink {
    upstream: CoordinatorStateSink,
    lose_next: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl StateSink for ReplyLossSink {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<adxlet::Durability> {
        self.upstream.commit(record).await
    }
    async fn authorize_execution(
        &self,
        request: &adx_core::execution::ExecutionRequest,
    ) -> Result<adx_core::execution::ExecutionAuthorization> {
        let grant = self.upstream.authorize_execution(request).await?;
        if self.lose_next.swap(false, Ordering::SeqCst) {
            return Err(adx_core::Error::Unavailable(
                "injected lost authorization reply".into(),
            ));
        }
        Ok(grant)
    }
}

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
    let checkpoint_root = tempfile::tempdir().unwrap();
    let checkpoint_store = Arc::new(
        adxlet::checkpoint::LocalCheckpointStore::new(checkpoint_root.path().into()).unwrap(),
    );
    let sink = Arc::new(ReplyLossSink {
        upstream: CoordinatorStateSink::new(channel(address, "node").await, Duration::from_secs(2))
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
        cpu_millis: spec("capacity").resources.cpu_millis * 2,
        memory_bytes: spec("capacity").resources.memory_bytes * 2,
        disk_bytes: spec("capacity").resources.disk_bytes * 2,
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
    owner.register_node(report.clone()).await.unwrap();
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
    let paused = lifecycle
        .pause_environment(pb::PauseEnvironmentRequest {
            assignment: first.assignment.clone(),
            caller: caller(),
            operation_id: "pause-first".into(),
            expected_revision: first.revision,
            ttl_seconds: 600,
            timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner()
        .record
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
    let resume = pb::ResumeEnvironmentRequest {
        assignment: first.assignment.clone(),
        caller: caller(),
        operation_id: "resume-first".into(),
        expected_revision: paused.revision,
    };
    for _ in 0..2 {
        assert_eq!(
            lifecycle
                .resume_environment(resume.clone())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            backend.started.load(Ordering::SeqCst),
            2,
            "over-budget resume cannot cross the execution boundary"
        );
        assert!(coordinator.metrics().await.is_ok());
        assert_eq!(
            session
                .get("first")
                .await
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .state,
            adx_core::EnvironmentState::Paused
        );
    }
    lifecycle
        .delete_environment(pb::DeleteEnvironmentRequest {
            assignment: second.assignment,
            caller: caller(),
            failed_revision: None,
            node_session_id: String::new(),
        })
        .await
        .unwrap();
    let resumed = lifecycle
        .resume_environment(resume.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(backend.started.load(Ordering::SeqCst), 3);
    assert_eq!(
        lifecycle
            .resume_environment(resume)
            .await
            .unwrap()
            .into_inner(),
        resumed
    );
    assert_eq!(backend.started.load(Ordering::SeqCst), 3);
    let stored = session.get("first").await.unwrap();
    assert!(!stored.execution.as_ref().unwrap().pending);
    assert!(stored.resources_held());
    lifecycle
        .delete_environment(pb::DeleteEnvironmentRequest {
            assignment: first.assignment,
            caller: caller(),
            failed_revision: None,
            node_session_id: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(backend.removed.load(Ordering::SeqCst), 3);

    assert!(backend.running.lock().unwrap().is_empty());
    assert!(session.snapshot().await.unwrap().environments.is_empty());
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"].available,
        spec("capacity").resources
    );

    // Lose the authorization response after Redis has committed the target hold.
    let lost = client
        .create_environment(create("lost"))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let paused = lifecycle
        .pause_environment(pb::PauseEnvironmentRequest {
            assignment: lost.assignment.clone(),
            caller: caller(),
            operation_id: "pause-lost".into(),
            expected_revision: lost.revision,
            ttl_seconds: 600,
            timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    sink.lose_next.store(true, Ordering::SeqCst);
    assert_eq!(
        lifecycle
            .resume_environment(pb::ResumeEnvironmentRequest {
                assignment: lost.assignment.clone(),
                caller: caller(),
                operation_id: "resume-lost".into(),
                expected_revision: paused.revision,
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(backend.started.load(Ordering::SeqCst), 4);
    assert!(backend.running.lock().unwrap().is_empty());
    let unknown = session.get("lost").await.unwrap();
    let target = unknown
        .execution
        .as_ref()
        .unwrap()
        .authorization
        .target
        .clone();
    assert!(unknown.execution.as_ref().unwrap().pending);
    assert!(unknown.resources_held());
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"].available,
        Resources::default()
    );
    assert!(manager.sync_proxy().await.is_err());
    assert!(!manager.accepting_allocations());

    // Drive the same close/register/inspect/reconcile sequence as the node reporter.
    manager.pause_lifecycle().await;
    report.heartbeat_sequence += 1;
    report.reconciling = true;
    report.accepting_allocations = false;
    owner.register_node(report.clone()).await.unwrap();
    let catalog = owner
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: report.session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let records: Vec<EnvironmentRecord> = catalog
        .records
        .into_iter()
        .map(|record| record.try_into().unwrap())
        .collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], target);
    manager.reconcile(records).await.unwrap();
    manager.sync_proxy().await.unwrap();
    let settled = session.get("lost").await.unwrap();
    assert_eq!(
        settled.result.as_ref().unwrap().runtime.id,
        target.runtime.id
    );
    assert_eq!(
        settled.result.as_ref().unwrap().state,
        adx_core::EnvironmentState::Failed
    );
    assert!(!settled.execution.as_ref().unwrap().pending);
    assert!(!settled.resources_held());
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"].available,
        spec("capacity").resources
    );
    report.heartbeat_sequence += 1;
    report.reconciling = false;
    report.accepting_allocations = true;
    owner.register_node(report).await.unwrap();
    for _ in 0..2 {
        lifecycle
            .delete_environment(pb::DeleteEnvironmentRequest {
                assignment: lost.assignment.clone(),
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
    }
    assert_eq!(backend.started.load(Ordering::SeqCst), 4);
    assert!(backend.running.lock().unwrap().is_empty());
    assert!(session.snapshot().await.unwrap().environments.is_empty());
}
