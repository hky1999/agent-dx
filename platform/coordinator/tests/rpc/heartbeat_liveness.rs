use super::*;
use adx_coordinator::storage::{HostBudget, RedisStore};
use adx_core::{Assignment, EnvironmentState};
use std::collections::BTreeMap;

const HEARTBEAT: Duration = Duration::from_millis(400);

#[derive(Clone)]
struct ProbeInterceptor {
    observed: Arc<AtomicUsize>,
    paused: Arc<std::sync::atomic::AtomicBool>,
}
impl tonic::service::Interceptor for ProbeInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, tonic::Status> {
        self.observed.fetch_add(1, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while self.paused.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "test probe pause must be bounded"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(request)
    }
}

struct Fixture {
    _redis: common::Redis,
    session: Session,
    rpc: CoordinatorRpc,
    owner: pb::coordinator_service_client::CoordinatorServiceClient<Channel>,
    report: pb::RegisterNodeRequest,
    redis_client: redis::Client,
    probes: Arc<AtomicUsize>,
    probe_pause: Arc<std::sync::atomic::AtomicBool>,
    _servers: Servers,
}

async fn fixture(actual_node: &str, actual_session: &str) -> Fixture {
    let redis = common::Redis::new().await;
    let store = RedisStore::connect(&redis.url, "liveness", Duration::from_millis(800))
        .await
        .unwrap();
    let session = store.begin(1).await.unwrap();
    session
        .configure_host_budgets(BTreeMap::from([(
            "physical".into(),
            HostBudget {
                capacity: spec("held").resources,
                nodes: ["node".into()].into(),
            },
        )]))
        .await
        .unwrap();
    let rpc = CoordinatorRpc::with_heartbeat_timeout(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(1),
        HEARTBEAT,
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = rpc.clone();
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
        running: Mutex::new(BTreeSet::from(["held-1".into()])),
    });
    let manager = Arc::new(Adxlet::new(
        actual_node.into(),
        backend,
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        Arc::new(
            CoordinatorStateSink::new(channel(address, "node").await, Duration::from_secs(2))
                .unwrap()
                .with_session(actual_session.into()),
        ),
    ));
    let node = NodeRpc::new(manager, peers(), actual_session.into());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = listener.local_addr().unwrap();
    let probes = Arc::new(AtomicUsize::new(0));
    let observed = probes.clone();
    let probe_pause = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let paused = probe_pause.clone();
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("node"))
            .unwrap()
            .add_service(
                pb::node_service_server::NodeServiceServer::with_interceptor(
                    node,
                    ProbeInterceptor { observed, paused },
                ),
            )
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let mut owner = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "node").await,
    );
    let mut report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: node_address.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(spec("held").resources.into()),
        session_id: "owner-boot".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        accepting_allocations: false,
        ..Default::default()
    };
    owner.register_node(report.clone()).await.unwrap();
    owner
        .inspect_node(pb::InspectNodeRequest {
            node_id: report.node_id.clone(),
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
    let assignment = Assignment {
        environment_id: "held".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), assignment.clone())
        .await
        .unwrap();
    session
        .commit(EnvironmentRecord {
            spec: spec("held"),
            assignment,
            state: EnvironmentState::Running,
            revision: 2,
            runtime: adx_core::Runtime {
                id: "held-1".into(),
                ip: Some("10.0.0.2".parse().unwrap()),
            },
            resources_held: true,
            restart_attempts: 0,
            restart_pending: false,
            checkpoint: None,
            last_operation: None,
        })
        .await
        .unwrap();
    let redis_client = redis::Client::open(redis.url.clone()).unwrap();
    Fixture {
        _redis: redis,
        session,
        rpc,
        owner,
        report,
        redis_client,
        probes,
        probe_pause,
        _servers: servers,
    }
}

async fn stall_registration(f: &mut Fixture) {
    let mut conn = f
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1_400)
        .arg("WRITE")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    f.report.heartbeat_sequence += 1;
    let mut owner = f.owner.clone();
    let report = f.report.clone();
    let pending = tokio::spawn(async move { owner.register_node(report).await });
    tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if f.rpc.metrics().await.is_err() && !pending.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("registration must actually hold Coordinator state while Redis writes are paused");
    let failure = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.code(), tonic::Code::Unavailable);
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn storage_wait_preserves_reachable_execution_and_requires_reconciliation() {
    let mut f = fixture("node", "owner-boot").await;
    let before = f.session.get("held").await.unwrap();
    let budget = f.session.host_budgets().await.unwrap();
    stall_registration(&mut f).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(4), f.rpc.expire_nodes())
            .await
            .unwrap()
            .unwrap(),
        0,
        "a reachable original session must survive Coordinator storage wait"
    );
    assert_eq!(f.probes.load(Ordering::SeqCst), 1);
    let held = f.session.get("held").await.unwrap();
    assert!(!held.invalidated);
    assert_eq!(held.result, before.result);
    assert_eq!(held.assignment, before.assignment);
    assert_eq!(f.session.host_budgets().await.unwrap(), budget);

    f.report.heartbeat_sequence += 1;
    assert_eq!(
        f.owner
            .register_node(f.report.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
        "a liveness probe cannot reopen admission or replace reconciliation"
    );
    f.report.heartbeat_sequence += 1;
    f.report.reconciling = true;
    f.report.accepting_allocations = false;
    f.owner.register_node(f.report.clone()).await.unwrap();
    let catalog = f
        .owner
        .inspect_node(pb::InspectNodeRequest {
            node_id: f.report.node_id.clone(),
            session_id: f.report.session_id.clone(),
            history_reclamation: false,
            include_history: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(catalog.records.len(), 1);
    assert_eq!(
        catalog.records[0].state,
        pb::EnvironmentState::Running as i32
    );
    f.report.heartbeat_sequence += 1;
    f.report.reconciling = false;
    f.report.accepting_allocations = true;
    f.owner.register_node(f.report).await.unwrap();
    assert_eq!(f.session.get("held").await.unwrap().result, before.result);
    assert_eq!(f.session.host_budgets().await.unwrap(), budget);
}

async fn assert_owner_expires(actual_node: &str, actual_session: &str, queued: bool) {
    let mut f = fixture(actual_node, actual_session).await;
    let before = f.session.get("held").await.unwrap();
    let mut budget = f.session.host_budgets().await.unwrap();
    if queued {
        stall_registration(&mut f).await;
    } else {
        tokio::time::sleep(HEARTBEAT + Duration::from_millis(50)).await;
    }
    assert_eq!(f.rpc.expire_nodes().await.unwrap(), 1);
    assert_eq!(f.probes.load(Ordering::SeqCst), usize::from(queued));
    let held = f.session.get("held").await.unwrap();
    assert!(held.invalidated);
    assert!(held.resources_held());
    assert_eq!(held.assignment, before.assignment);
    assert_eq!(held.result.unwrap().state, EnvironmentState::Failed);
    // Holds and their partitions remain; the unknown-result overlap changes.
    for status in budget.values_mut() {
        assert_eq!(status.memory_accounts.unknown_result_bytes, 0);
        status.memory_accounts.unknown_result_bytes = status.held.memory_bytes;
    }
    assert_eq!(f.session.host_budgets().await.unwrap(), budget);
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn reachable_different_session_cannot_extend_old_execution() {
    assert_owner_expires("node", "different-boot", true).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn reachable_different_node_cannot_extend_old_execution() {
    assert_owner_expires("different-node", "owner-boot", true).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn reachable_owner_without_queued_heartbeat_still_expires() {
    assert_owner_expires("node", "owner-boot", false).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn consumed_receipt_cannot_be_replayed_to_keep_owner_alive() {
    let mut f = fixture("node", "owner-boot").await;
    let before = f.session.get("held").await.unwrap();
    let mut budget = f.session.host_budgets().await.unwrap();
    stall_registration(&mut f).await;
    assert_eq!(f.rpc.expire_nodes().await.unwrap(), 0);
    assert_eq!(f.probes.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.owner
            .register_node(f.report.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    tokio::time::sleep(HEARTBEAT + Duration::from_millis(50)).await;
    assert_eq!(f.rpc.expire_nodes().await.unwrap(), 1);
    assert_eq!(f.probes.load(Ordering::SeqCst), 1);
    let held = f.session.get("held").await.unwrap();
    assert!(held.invalidated);
    assert!(held.resources_held());
    assert_eq!(held.assignment, before.assignment);
    assert_eq!(held.result.unwrap().state, EnvironmentState::Failed);
    // Holds and their partitions remain; the unknown-result overlap changes.
    for status in budget.values_mut() {
        assert_eq!(status.memory_accounts.unknown_result_bytes, 0);
        status.memory_accounts.unknown_result_bytes = status.held.memory_bytes;
    }
    assert_eq!(f.session.host_budgets().await.unwrap(), budget);
}

// The service interceptor is synchronous. These two tests use two Tokio workers,
// a bounded pause, and an independent Drop release so assertions cannot strand it.
struct ProbePause(Arc<std::sync::atomic::AtomicBool>);
impl ProbePause {
    fn new(flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        flag.store(true, Ordering::SeqCst);
        Self(flag)
    }
}
impl Drop for ProbePause {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
async fn wait_for_probe(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.probes.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the original session probe must actually reach the node");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn concurrent_expiry_waits_for_original_session_probe() {
    let mut f = fixture("node", "owner-boot").await;
    let before = f.session.get("held").await.unwrap();
    let budget = f.session.host_budgets().await.unwrap();
    stall_registration(&mut f).await;
    let pause = ProbePause::new(f.probe_pause.clone());
    let service = f.rpc.clone();
    let first = tokio::spawn(async move { service.expire_nodes().await });
    wait_for_probe(&f).await;
    let service = f.rpc.clone();
    let second = tokio::spawn(async move { service.expire_nodes().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let returned_while_paused = second.is_finished();
    drop(pause);
    let first = tokio::time::timeout(Duration::from_secs(3), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(3), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        !returned_while_paused,
        "concurrent expiry must wait for the original in-flight probe"
    );
    assert_eq!(first, 0);
    assert_eq!(second, 0);
    assert_eq!(f.probes.load(Ordering::SeqCst), 1);
    let held = f.session.get("held").await.unwrap();
    assert!(!held.invalidated);
    assert_eq!(held.result, before.result);
    assert_eq!(held.assignment, before.assignment);
    assert_eq!(f.session.host_budgets().await.unwrap(), budget);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn cancelled_expiry_keeps_original_session_probe_running() {
    let mut f = fixture("node", "owner-boot").await;
    let before = f.session.get("held").await.unwrap();
    let budget = f.session.host_budgets().await.unwrap();
    stall_registration(&mut f).await;
    let pause = ProbePause::new(f.probe_pause.clone());
    let service = f.rpc.clone();
    let first = tokio::spawn(async move { service.expire_nodes().await });
    wait_for_probe(&f).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let service = f.rpc.clone();
    let second = tokio::spawn(async move { service.expire_nodes().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let returned_while_paused = second.is_finished();
    drop(pause);
    let second = tokio::time::timeout(Duration::from_secs(3), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        !returned_while_paused,
        "cancelling the observer must not discard the in-flight proof"
    );
    assert_eq!(second, 0);
    assert_eq!(f.probes.load(Ordering::SeqCst), 1);
    let held = f.session.get("held").await.unwrap();
    assert!(!held.invalidated);
    assert_eq!(held.result, before.result);
    assert_eq!(held.assignment, before.assignment);
    assert_eq!(f.session.host_budgets().await.unwrap(), budget);
}
