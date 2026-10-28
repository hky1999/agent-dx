use super::*;
use adx_coordinator::storage::HostBudget;
use adxlet::StateSink;
use std::collections::BTreeMap;
const MIB: u64 = 1 << 20;
fn memory_capacity() -> Resources {
    Resources {
        cpu_millis: 2000,
        memory_bytes: 512 * MIB,
        disk_bytes: 0,
    }
}
#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn memory_rpc_authenticates_owner_reserves_settles_and_retires_without_stale_scheduler_hold()
{
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    session
        .configure_host_budgets(BTreeMap::from([(
            "physical".into(),
            HostBudget {
                capacity: memory_capacity(),
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
    .unwrap()
    .with_memory_policy(adx_coordinator::storage::MemoryControlPolicy::default())
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
    let sink = Arc::new(
        CoordinatorStateSink::new(channel(address, "node").await, Duration::from_secs(2))
            .unwrap()
            .with_session("host-budget-boot".into()),
    );
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
        cpu_millis: memory_capacity().cpu_millis * 2,
        memory_bytes: memory_capacity().memory_bytes * 2,
        disk_bytes: memory_capacity().disk_bytes * 2,
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
        runtime_classes: vec!["runsc".into()],
        memory_control_version: 1,
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
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );

    let mut request = create("memory");
    let mut environment = spec("memory");
    environment.runtime_class = "runsc".into();
    environment.resources = Resources {
        cpu_millis: 500,
        memory_bytes: 128 * MIB,
        disk_bytes: 0,
    };
    environment.sandbox.limits = adx_core::sandbox::ResourceLimits {
        cpu_millis: 500,
        memory_bytes: 256 * MIB,
        disk_bytes: 0,
    };
    environment.sandbox.elastic_memory = Some(adx_core::memory::ElasticMemoryPolicy {
        high_per_mille: 750,
        minimum_bytes: None,
    });
    request.spec = Some(environment.into());
    let created: EnvironmentRecord = client
        .create_environment(request)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap()
        .try_into()
        .unwrap();
    let now = || {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
    };
    let mut sample = adx_core::memory::MemorySample {
        observation: adx_core::memory::MemoryObservation {
            identity: adx_core::memory::MemoryIdentity {
                assignment: created.assignment.clone(),
                runtime_id: created.runtime.id.clone(),
                sandbox_id: "physical".into(),
                resource_generation: "generation".into(),
            },
            cpu_quota_usec: 50000,
            cpu_period_usec: 100000,
            memory_limit_bytes: 128 * MIB,
            memory_high_bytes: 96 * MIB,
            resource_revision: 1,
            operation_id: String::new(),
            operation_pending: false,
            frozen: false,
        },
        usage_bytes: 112 * MIB,
        peak_bytes: None,
        cpu_usage_ns: None,
        pressure: None,
        observed_unix_millis: now(),
    };
    let rpc_request = pb::ReconcileMemoryRequest {
        sample: Some(sample.clone().try_into().unwrap()),
        node_session_id: report.session_id.clone(),
    };
    assert_eq!(
        client
            .reconcile_memory(rpc_request.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut forged = rpc_request.clone();
    forged.node_session_id = "retired".into();
    assert_eq!(
        owner.reconcile_memory(forged).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"]
            .held
            .memory_bytes,
        128 * MIB
    );
    let decision = sink.reconcile_memory(&sample).await.unwrap();
    let grant = decision.authorization.unwrap();
    assert_eq!(grant.request.target_limit_bytes, 224 * MIB);
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"]
            .held
            .memory_bytes,
        224 * MIB
    );
    sample.observed_unix_millis = now();
    assert_eq!(
        sink.reconcile_memory(&sample).await.unwrap().authorization,
        Some(grant.clone())
    );
    // Closing admission preserves the current owner and its original pending grant.
    report.heartbeat_sequence += 1;
    report.accepting_allocations = false;
    owner.register_node(report.clone()).await.unwrap();
    assert!(
        !session.snapshot().await.unwrap().nodes["node"]
            .node
            .available
    );
    sample.observed_unix_millis = now();
    assert_eq!(
        sink.reconcile_memory(&sample).await.unwrap().authorization,
        Some(grant.clone())
    );
    sample.observation = grant.request.completion().unwrap();
    sample.usage_bytes = 32 * MIB;
    sample.observed_unix_millis = now();
    assert!(sink
        .reconcile_memory(&sample)
        .await
        .unwrap()
        .authorization
        .is_none());
    assert_eq!(
        session
            .get("memory")
            .await
            .unwrap()
            .memory
            .unwrap()
            .confirmed,
        sample.observation
    );
    assert!(
        !session.snapshot().await.unwrap().nodes["node"]
            .node
            .available
    );
    let mut closed_request = pb::ReconcileMemoryRequest {
        sample: Some(sample.clone().try_into().unwrap()),
        node_session_id: "retired".into(),
    };
    assert_eq!(
        owner
            .reconcile_memory(closed_request.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    closed_request.node_session_id = report.session_id.clone();
    assert_eq!(
        client
            .reconcile_memory(closed_request)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    report.heartbeat_sequence += 1;
    report.accepting_allocations = true;
    owner.register_node(report.clone()).await.unwrap();
    let mut lifecycle =
        pb::node_service_client::NodeServiceClient::new(channel(node_address, "apiserver").await);
    let delete = pb::DeleteEnvironmentRequest {
        assignment: Some(created.assignment.try_into().unwrap()),
        caller: caller(),
        ..Default::default()
    };
    lifecycle.delete_environment(delete.clone()).await.unwrap();
    lifecycle.delete_environment(delete).await.unwrap();
    assert_eq!(
        session.host_budgets().await.unwrap()["physical"].held,
        Resources::default()
    );
    assert_eq!(manager.used(), Resources::default());
    // A dynamic account must also disappear from the in-memory scheduler view.
    let mut next = create("after-memory");
    let mut next_spec = spec("after-memory");
    next_spec.runtime_class = "runsc".into();
    next_spec.resources = memory_capacity();
    next.spec = Some(next_spec.into());
    let next = client
        .create_environment(next)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    lifecycle
        .delete_environment(pb::DeleteEnvironmentRequest {
            assignment: next.assignment,
            caller: caller(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(coordinator.metrics().await.is_ok());
}
