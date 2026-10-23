//! Resource reads must use a new bounded RPC even while routing cache is valid.
use super::*;
use futures_util::{stream, Stream, StreamExt};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex as StdMutex,
    },
};
use tonic::Request;

#[derive(Clone)]
enum Mode {
    Frame(pb::NodeDirectory),
    Unavailable,
    Empty,
    Silent,
}
#[derive(Clone)]
struct Source {
    mode: Arc<StdMutex<Mode>>,
    requests: Arc<AtomicUsize>,
}
macro_rules! service_impl {
    ($($name:ident($request:ident) -> $response:ident),* $(,)?) => {
        #[tonic::async_trait]
        impl pb::coordinator_service_server::CoordinatorService for Source {
            type WatchNodesStream = Pin<Box<dyn Stream<Item=Result<pb::NodeDirectory, Status>> + Send>>;
            async fn watch_nodes(&self, _: Request<pb::WatchNodesRequest>) -> Result<Response<Self::WatchNodesStream>, Status> {
                self.requests.fetch_add(1, Ordering::SeqCst);
                let mode = self.mode.lock().unwrap().clone();
                let value: Self::WatchNodesStream = match mode {
                    Mode::Frame(frame) => Box::pin(stream::once(async { Ok(frame) }).chain(stream::pending())),
                    Mode::Unavailable => return Err(Status::unavailable("controlled budget authority unavailable")),
                    Mode::Empty => Box::pin(stream::empty()),
                    Mode::Silent => Box::pin(stream::pending()),
                };
                Ok(Response::new(value))
            }
            $(async fn $name(&self, _: Request<pb::$request>) -> Result<Response<pb::$response>, Status> {
                Err(Status::unimplemented("unused resource test RPC"))
            })*
        }
    };
}
service_impl!(
    get_environment(GetEnvironmentRequest) -> GetEnvironmentResponse,
    get_scheduling_queue(GetSchedulingQueueRequest) -> GetSchedulingQueueResponse,
    set_node_scheduling(SetNodeSchedulingRequest) -> NodeSchedulingState,
    prepare_create(LocalEnvironmentCreateRequest) -> PreparedEnvironment,
    claim_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    forward_create(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    register_node(RegisterNodeRequest) -> RegisterNodeResponse,
    inspect_node(InspectNodeRequest) -> InspectNodeResponse,
    create_environment(CreateEnvironmentRequest) -> EnvironmentResult,
    commit_environment(CommitEnvironmentRequest) -> CommitEnvironmentResponse,
    authorize_execution(AuthorizeExecutionRequest) -> AuthorizeExecutionResponse,
    reconcile_memory(ReconcileMemoryRequest) -> ReconcileMemoryResponse,
);
fn frame(cpu: u64, epoch: u64) -> pb::NodeDirectory {
    let capacity = pb::Resources {
        cpu_millis: 3000,
        memory_bytes: 12 << 30,
        disk_bytes: 16 << 30,
    };
    let held = pb::Resources {
        cpu_millis: cpu,
        memory_bytes: cpu * 1024,
        disk_bytes: cpu * 2048,
    };
    let available = pb::Resources {
        cpu_millis: capacity.cpu_millis - cpu,
        memory_bytes: capacity.memory_bytes - held.memory_bytes,
        disk_bytes: capacity.disk_bytes - held.disk_bytes,
    };
    pb::NodeDirectory {
        epoch,
        control_revision: 17,
        valid_for_millis: 5000,
        host_budget_configured: true,
        nodes: vec![pb::NodeEndpoint {
            node_id: "node".into(),
            address: "127.0.0.1:9000".into(),
            relay_address: "127.0.0.1:9001".into(),
            session_id: "same-node-process".into(),
            accepting_allocations: true,
            capacity: Some(capacity),
            allocatable: Some(available),
            ..Default::default()
        }],
        host_budgets: vec![pb::HostBudgetStatus {
            host_id: "physical".into(),
            nodes: vec!["node".into()],
            capacity: Some(capacity),
            held: Some(held),
            available: Some(available),
        }],
    }
}
struct Fixture {
    clients: Arc<Clients>,
    source: Source,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let source = Source {
            mode: Arc::new(StdMutex::new(Mode::Frame(frame(1000, 7)))),
            requests: Arc::new(AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = source.clone();
        let server = tokio::spawn(async move {
            let incoming = stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let clients = Clients::new(
            serde_json::from_value(serde_json::json!({
                "listen":"127.0.0.1:0", "coordinator_address":format!("http://{address}"),
                "internal_security":"network", "ingress_mode":"standalone",
                "rpc_timeout_seconds":1, "cache_entries":8, "auth_cache_ttl_seconds":1
            }))
            .unwrap(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if clients.directory.lock().await.snapshot().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("initial routing frame must be cached before changing the authority");
        Self {
            clients,
            source,
            server,
        }
    }
    fn set(&self, mode: Mode) {
        *self.source.mode.lock().unwrap() = mode;
    }
    async fn assert_old_cache_is_still_valid(&self) {
        assert_eq!(
            self.clients
                .directory
                .lock()
                .await
                .snapshot()
                .unwrap()
                .host_budgets[0]
                .held
                .unwrap()
                .cpu_millis,
            1000
        );
    }
}
#[tokio::test]
async fn resource_read_observes_new_hold_and_epoch_without_waiting_for_watch_refresh() {
    let f = Fixture::new().await;
    f.set(Mode::Frame(frame(1040, 8)));
    let fresh = f.clients.nodes().await.unwrap();
    assert_eq!(
        fresh.host_budgets[0].held.unwrap().cpu_millis,
        1040,
        "a positive hold committed before this query cannot be hidden by a valid old routing frame"
    );
    assert_eq!(fresh.epoch, 8);
    assert_eq!(fresh.control_revision, 17);
    f.assert_old_cache_is_still_valid().await;
    assert!(f.source.requests.load(Ordering::SeqCst) >= 2);
}
#[tokio::test]
async fn resource_read_does_not_return_cached_holds_when_authority_is_unavailable() {
    let f = Fixture::new().await;
    f.set(Mode::Unavailable);
    assert_eq!(
        f.clients.nodes().await.unwrap_err().code(),
        Code::Unavailable
    );
    f.assert_old_cache_is_still_valid().await;
}
#[tokio::test]
async fn resource_read_rejects_empty_authoritative_stream_without_cache_fallback() {
    let f = Fixture::new().await;
    f.set(Mode::Empty);
    assert_eq!(
        f.clients.nodes().await.unwrap_err().code(),
        Code::Unavailable
    );
}
#[tokio::test]
async fn resource_read_rejects_malformed_new_budget_instead_of_old_valid_cache() {
    let f = Fixture::new().await;
    let mut bad = frame(1040, 8);
    bad.host_budgets[0].held = None;
    f.set(Mode::Frame(bad));
    assert_eq!(f.clients.nodes().await.unwrap_err().code(), Code::DataLoss);
}
#[tokio::test]
async fn resource_read_deadline_covers_the_first_stream_message() {
    let f = Fixture::new().await;
    f.set(Mode::Silent);
    let started = std::time::Instant::now();
    assert_eq!(
        f.clients.nodes().await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the resource read must have a bounded total deadline"
    );
}

#[tokio::test]
async fn resource_read_rejects_an_unversioned_frame_without_cache_fallback() {
    let f = Fixture::new().await;
    let mut bad = frame(1040, 8);
    bad.control_revision = 0;
    f.set(Mode::Frame(bad));
    assert_eq!(f.clients.nodes().await.unwrap_err().code(), Code::DataLoss);
}
