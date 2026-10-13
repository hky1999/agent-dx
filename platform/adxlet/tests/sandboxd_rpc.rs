//! Local gRPC/UDS contract tests against the pinned protocol, not sandboxd E2E.
mod execution_authority;
mod memory_elasticity;
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources};
use adxlet::sandboxd::proto::sandbox_service_server::SandboxService;
use adxlet::{
    sandboxd::{connect_when_ready, proto::*, start_request, Config, Sandboxd},
    Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UnixListener, sync::Semaphore};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct Server {
    requests: Arc<Mutex<Vec<String>>>,
    starts: Arc<Mutex<Vec<StartRequest>>>,
    network_updates: Arc<Mutex<Vec<SetNetworkPolicyRequest>>>,
    checkpoints: Arc<Mutex<Vec<CheckpointRequest>>>,
    conditional_deletes: Arc<Mutex<Vec<DeleteIfGenerationRequest>>>,
    conditional_checkpoints: Arc<Mutex<Vec<CheckpointIfGenerationRequest>>>,
    legacy_deletes: Arc<std::sync::atomic::AtomicUsize>,
    legacy_checkpoints: Arc<std::sync::atomic::AtomicUsize>,
    resource_generation: Arc<Mutex<String>>,
    omit_start_generation: bool,
    omit_inventory_generation: bool,
    delete_error: Option<tonic::Code>,
    checkpoint_error: Option<tonic::Code>,
    lose_delete_reply_once: Arc<std::sync::atomic::AtomicBool>,
    retired_generation: Option<String>,
    unready: Arc<std::sync::atomic::AtomicUsize>,
    capabilities_delay_millis: Arc<std::sync::atomic::AtomicU64>,
    running: Arc<Mutex<bool>>,
    xpu: Arc<Mutex<Vec<XpuAllocation>>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    ip: String,
    labels: Arc<Mutex<std::collections::HashMap<String, String>>>,
    transport_error: bool,
    transport_error_after_start: bool,
    invalid_argument: bool,
    unsettled_error: Option<tonic::Code>,
    settled_error: Option<tonic::Code>,
    settled_error_after_start: bool,
    retain_after_delete: bool,
    checkpoint_success: bool,
    untracked_start_service: bool,
    not_created_on_start: bool,
    hide_inventory: bool,
    drop_before_admission: bool,
    start_operations: Arc<Mutex<Vec<StartOperationRequest>>>,
    start_queries: Arc<Mutex<Vec<GetStartOperationRequest>>>,
    start_outcomes: Arc<Mutex<HashMap<String, StartOperationResponse>>>,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            requests: Arc::default(),
            starts: Arc::default(),
            network_updates: Arc::default(),
            checkpoints: Arc::default(),
            conditional_deletes: Arc::default(),
            conditional_checkpoints: Arc::default(),
            legacy_deletes: Arc::default(),
            legacy_checkpoints: Arc::default(),
            resource_generation: Arc::new(Mutex::new("physical-generation-1".into())),
            omit_start_generation: false,
            omit_inventory_generation: false,
            delete_error: None,
            checkpoint_error: None,
            lose_delete_reply_once: Arc::default(),
            retired_generation: None,
            unready: Arc::default(),
            capabilities_delay_millis: Arc::default(),
            running: Arc::default(),
            xpu: Arc::default(),
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(1)),
            ip: "10.0.0.2".into(),
            labels: Arc::default(),
            transport_error: false,
            transport_error_after_start: false,
            invalid_argument: false,
            unsettled_error: None,
            settled_error: None,
            settled_error_after_start: false,
            retain_after_delete: false,
            checkpoint_success: true,
            untracked_start_service: false,
            not_created_on_start: false,
            hide_inventory: false,
            drop_before_admission: false,
            start_operations: Arc::default(),
            start_queries: Arc::default(),
            start_outcomes: Arc::default(),
        }
    }
}
#[tonic::async_trait]
impl sandbox_service_server::SandboxService for Server {
    async fn pause(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        Err(Status::unimplemented("outside this lifecycle test"))
    }
    async fn resume(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        Err(Status::unimplemented("outside this lifecycle test"))
    }
    async fn restore(&self, _: Request<RestoreRequest>) -> Result<Response<StartResponse>, Status> {
        Err(Status::unimplemented("outside this lifecycle test"))
    }
    async fn delete_checkpoint(
        &self,
        _: Request<DeleteCheckpointRequest>,
    ) -> Result<Response<DeleteCheckpointResponse>, Status> {
        Err(Status::unimplemented("outside this lifecycle test"))
    }
    async fn resize_resources(
        &self,
        _: Request<ResizeResourcesRequest>,
    ) -> Result<Response<ResizeResourcesResponse>, Status> {
        Err(Status::unimplemented("outside this lifecycle test"))
    }
    async fn shrink_resources(
        &self,
        _: Request<ShrinkResourcesRequest>,
    ) -> Result<Response<ResizeResourcesResponse>, Status> {
        Err(Status::unimplemented("outside this lifecycle test"))
    }
    async fn start_operation(
        &self,
        request: Request<StartOperationRequest>,
    ) -> Result<Response<StartOperationResponse>, Status> {
        if self.untracked_start_service {
            return Err(Status::unimplemented("durable creation is unsupported"));
        }
        let request = request.into_inner();
        self.start_operations.lock().unwrap().push(request.clone());
        if self
            .start_outcomes
            .lock()
            .unwrap()
            .get(&request.operation_id)
            .is_some_and(|outcome| outcome.state == StartOperationState::NotCreated as i32)
        {
            return Err(Status::failed_precondition("late creation is sealed"));
        }
        if self.drop_before_admission {
            return Err(Status::unavailable("request lost before durable admission"));
        }
        let mut outcome = StartOperationResponse {
            operation_id: request.operation_id.clone(),
            request_fingerprint: request.request_fingerprint,
            deadline_unix_millis: request.deadline_unix_millis,
            state: StartOperationState::Pending as i32,
            result: None,
        };
        if self.not_created_on_start {
            outcome.state = StartOperationState::NotCreated as i32;
            self.start_outcomes
                .lock()
                .unwrap()
                .insert(request.operation_id, outcome.clone());
            return Ok(Response::new(outcome));
        }
        let response = self.start(Request::new(request.start.unwrap())).await;
        if let Ok(response) = &response {
            outcome.state = StartOperationState::Started as i32;
            outcome.result = Some(response.get_ref().clone());
        } else if *self.running.lock().unwrap() {
            // The daemon retains completion even if the caller loses its reply.
            outcome.state = StartOperationState::Started as i32;
            outcome.result = Some(StartResponse {
                id: "generated-backend-id".into(),
                sandbox_ip: self.ip.clone(),
                resource_generation: self.resource_generation.lock().unwrap().clone(),
                ..Default::default()
            });
        }
        self.start_outcomes
            .lock()
            .unwrap()
            .insert(request.operation_id, outcome.clone());
        response.map(|_| Response::new(outcome))
    }
    async fn get_start_operation(
        &self,
        request: Request<GetStartOperationRequest>,
    ) -> Result<Response<StartOperationResponse>, Status> {
        if self.untracked_start_service {
            return Err(Status::unimplemented("durable creation is unsupported"));
        }
        let request = request.into_inner();
        self.start_queries.lock().unwrap().push(request.clone());
        let mut outcome = self
            .start_outcomes
            .lock()
            .unwrap()
            .get(&request.operation_id)
            .cloned()
            .unwrap_or(StartOperationResponse {
                operation_id: request.operation_id,
                request_fingerprint: request.request_fingerprint,
                deadline_unix_millis: request.deadline_unix_millis,
                state: StartOperationState::Unknown as i32,
                result: None,
            });
        let expired = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            >= u128::from(request.deadline_unix_millis);
        if self.drop_before_admission && request.seal_missing && expired {
            outcome.state = StartOperationState::NotCreated as i32;
            self.start_outcomes
                .lock()
                .unwrap()
                .insert(outcome.operation_id.clone(), outcome.clone());
        }
        Ok(Response::new(outcome))
    }
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let request = request.into_inner();
        assert!(request.sandbox_id.is_empty());
        self.starts.lock().unwrap().push(request.clone());
        *self.labels.lock().unwrap() = request.labels.clone();
        *self.xpu.lock().unwrap() = request.xpu_allocations.clone();
        self.requests
            .lock()
            .unwrap()
            .push(format!("start:{}", request.sandbox_id));
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        if self.invalid_argument {
            return Err(Status::invalid_argument("invalid start request"));
        }
        if let Some(code) = self.unsettled_error {
            return Err(Status::new(
                code,
                "runtime start failed without settlement proof",
            ));
        }
        if self.transport_error {
            return Err(Status::unavailable("connection interrupted"));
        }
        if let Some(code) = self.settled_error {
            *self.running.lock().unwrap() = self.settled_error_after_start;
            let mut status = Status::new(code, "start failed and rollback completed");
            status
                .metadata_mut()
                .insert("sandboxd-start-settled", "true".parse().unwrap());
            return Err(status);
        }
        *self.running.lock().unwrap() = true;
        if self.transport_error_after_start {
            return Err(Status::unavailable("response interrupted after start"));
        }
        Ok(Response::new(StartResponse {
            id: "generated-backend-id".into(),
            sandbox_ip: self.ip.clone(),
            resource_generation: if self.omit_start_generation {
                String::new()
            } else {
                self.resource_generation.lock().unwrap().clone()
            },
            ..Default::default()
        }))
    }
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.legacy_deletes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.requests
            .lock()
            .unwrap()
            .push(format!("delete:{}", request.into_inner().id));
        if !self.retain_after_delete {
            *self.running.lock().unwrap() = false;
        }
        Ok(Response::new(DeleteResponse::default()))
    }
    async fn delete_if_generation(
        &self,
        request: Request<DeleteIfGenerationRequest>,
    ) -> Result<Response<DeleteIfGenerationResponse>, Status> {
        let request = request.into_inner();
        self.conditional_deletes
            .lock()
            .unwrap()
            .push(request.clone());
        if let Some(code) = self.delete_error {
            return Err(Status::new(code, "conditional delete did not complete"));
        }
        if request.expected_generation != *self.resource_generation.lock().unwrap() {
            return Err(Status::failed_precondition("physical generation changed"));
        }
        assert_eq!(request.id, "generated-backend-id");
        self.requests
            .lock()
            .unwrap()
            .push(format!("delete:{}", request.id));
        if !self.retain_after_delete {
            *self.running.lock().unwrap() = false;
        }
        if self
            .lose_delete_reply_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Status::unavailable("retired but reply lost"));
        }
        Ok(Response::new(DeleteIfGenerationResponse {
            retired_generation: self
                .retired_generation
                .clone()
                .unwrap_or(request.expected_generation),
        }))
    }
    async fn list(
        &self,
        request: Request<ListSandboxesRequest>,
    ) -> Result<Response<ListSandboxesResponse>, Status> {
        let request = request.into_inner();
        let id = request.id;
        self.requests.lock().unwrap().push(format!("list:{id}"));
        if self.hide_inventory
            || !*self.running.lock().unwrap()
            || (!id.is_empty() && id != "generated-backend-id")
        {
            if !id.is_empty() {
                return Err(Status::not_found("sandbox not found"));
            }
            return Ok(Response::new(ListSandboxesResponse::default()));
        }
        let mut labels = self.labels.lock().unwrap().clone();
        if !self.omit_inventory_generation {
            labels.insert(
                "akernel.scheduler/resource-generation".into(),
                self.resource_generation.lock().unwrap().clone(),
            );
        }
        if request
            .selector
            .iter()
            .any(|(k, v)| labels.get(k) != Some(v))
        {
            return Ok(Response::new(ListSandboxesResponse::default()));
        }
        Ok(Response::new(ListSandboxesResponse {
            sandboxes: vec![SandboxStatus {
                id: "generated-backend-id".into(),
                labels,
                state: SandboxState::Running as i32,
                ..Default::default()
            }],
        }))
    }
    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        assert_eq!(request.into_inner().id, "generated-backend-id");
        Ok(Response::new(StatsResponse {
            memory_usage_bytes: 123,
            cpu_usage_ns: 456,
            ..Default::default()
        }))
    }
    async fn checkpoint(
        &self,
        request: Request<CheckpointRequest>,
    ) -> Result<Response<CheckpointResponse>, Status> {
        self.legacy_checkpoints
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.save_checkpoint(request.into_inner())
            .map_err(|error| *error)
    }
    async fn checkpoint_if_generation(
        &self,
        request: Request<CheckpointIfGenerationRequest>,
    ) -> Result<Response<CheckpointResponse>, Status> {
        let request = request.into_inner();
        self.conditional_checkpoints
            .lock()
            .unwrap()
            .push(request.clone());
        if let Some(code) = self.checkpoint_error {
            return Err(Status::new(code, "conditional checkpoint unavailable"));
        }
        if request.expected_generation != *self.resource_generation.lock().unwrap() {
            return Err(Status::failed_precondition("physical generation changed"));
        }
        self.save_checkpoint(
            request
                .checkpoint
                .ok_or_else(|| Status::invalid_argument("checkpoint required"))?,
        )
        .map_err(|error| *error)
    }
    async fn wait(&self, _: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_available_runtimes(
        &self,
        _: Request<ListAvailableRuntimesRequest>,
    ) -> Result<Response<ListAvailableRuntimesResponse>, Status> {
        use std::sync::atomic::Ordering;
        tokio::time::sleep(Duration::from_millis(
            self.capabilities_delay_millis.load(Ordering::SeqCst),
        ))
        .await;
        if self
            .unready
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Status::unavailable("sandbox service not ready"));
        }
        Ok(Response::new(ListAvailableRuntimesResponse {
            runtime_classes: vec!["runsc".into()],
            runtimes: vec![RuntimeInfo {
                runtime_class: "runsc".into(),
                supports_checkpoint_restore: true,
                checkpoint_handoff_path: "/run/backend/checkpoint".into(),
                restore_env_path: "/run/backend/environment".into(),
            }],
        }))
    }
    async fn set_network_policy(
        &self,
        request: Request<SetNetworkPolicyRequest>,
    ) -> Result<Response<SetNetworkPolicyResponse>, Status> {
        self.network_updates
            .lock()
            .unwrap()
            .push(request.into_inner());
        Ok(Response::new(SetNetworkPolicyResponse {}))
    }
}
impl Server {
    fn save_checkpoint(
        &self,
        r: CheckpointRequest,
    ) -> Result<Response<CheckpointResponse>, Box<Status>> {
        assert_eq!(r.id, "generated-backend-id");
        self.checkpoints.lock().unwrap().push(r.clone());
        std::fs::create_dir(&r.checkpoint_dir).map_err(|_| {
            Box::new(Status::failed_precondition(
                "managed checkpoint needs absent destination",
            ))
        })?;
        self.requests
            .lock()
            .unwrap()
            .push(format!("checkpoint:leave_running={}", r.leave_running));
        assert_eq!(r.timeout_seconds, 60);
        assert_eq!(r.snapshot_type, "Full");
        std::fs::write(
            std::path::Path::new(&r.checkpoint_dir).join("memory"),
            b"state",
        )
        .unwrap();
        *self.running.lock().unwrap() = r.leave_running;
        Ok(Response::new(CheckpointResponse {
            success: self.checkpoint_success,
            ..Default::default()
        }))
    }
}
struct Harness {
    _directory: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn connect(server: Server) -> (Sandboxd, Harness) {
    connect_with_env(server, HashMap::new()).await
}
async fn connect_with_env(server: Server, env: HashMap<String, String>) -> (Sandboxd, Harness) {
    connect_with_timeout(server, env, Duration::from_secs(2)).await
}
async fn connect_with_timeout(
    server: Server,
    env: HashMap<String, String>,
    timeout: Duration,
) -> (Sandboxd, Harness) {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("rpc.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(sandbox_service_server::SandboxServiceServer::new(server))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    let adapter = Sandboxd::connect(
        socket,
        Config {
            env,
            operation_journal: Some(directory.path().join("operations.sqlite")),
            rpc_timeout: timeout,
            runtime_logs: Some(adxlet::runtime_logs::RuntimeLogPolicy {
                directory: directory.path().join("runtime-logs"),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (
        adapter,
        Harness {
            _directory: directory,
            task,
        },
    )
}
#[tokio::test]
async fn discovers_runtime_classes_from_sandboxd_uds() {
    let (adapter, _server) = connect(Server::default()).await;
    assert_eq!(
        adapter.available_runtime_classes().await.unwrap(),
        ["runsc"]
    );
}
#[tokio::test]
async fn refuses_untracked_start_service_without_creating_physical_execution() {
    let server = Server {
        untracked_start_service: true,
        ..Default::default()
    };
    let starts = server.starts.clone();
    let (adapter, _server) = connect(server).await;
    let result = adapter.start(&spec(), "i-1", 1, &[]).await;
    assert!(
        result.is_err(),
        "creation requires the durable operation RPC"
    );
    assert!(
        starts.lock().unwrap().is_empty(),
        "an unsupported service must never receive legacy Start"
    );
}

#[tokio::test]
async fn lost_start_reply_reopens_and_queries_original_identity_before_retirement() {
    let server = Server {
        transport_error_after_start: true,
        hide_inventory: true,
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    let original = server.start_operations.lock().unwrap()[0].clone();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    adapter.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
    let query = server.start_queries.lock().unwrap()[0].clone();
    assert_eq!(query.operation_id, original.operation_id);
    assert_eq!(query.request_fingerprint, original.request_fingerprint);
    assert_eq!(query.deadline_unix_millis, original.deadline_unix_millis);
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(
        server.conditional_deletes.lock().unwrap()[0].expected_generation,
        "physical-generation-1"
    );
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
}

#[tokio::test]
async fn durably_rejected_start_settles_and_remains_terminal_after_reopen() {
    let server = Server {
        not_created_on_start: true,
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    adapter.remove("i-1").await.unwrap();
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(server.starts.lock().unwrap().is_empty());
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
    assert_eq!(server.start_operations.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn start_query_mismatched_fingerprint_cannot_authorize_retirement() {
    let server = Server {
        transport_error_after_start: true,
        hide_inventory: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    for outcome in server.start_outcomes.lock().unwrap().values_mut() {
        outcome.request_fingerprint = "b".repeat(64);
    }
    assert!(adapter.remove("i-1").await.is_err());
    assert!(*server.running.lock().unwrap());
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_creation_is_sealed_after_deadline_and_late_dispatch_is_rejected() {
    let server = Server {
        drop_before_admission: true,
        ..Default::default()
    };
    let (adapter, _server) =
        connect_with_timeout(server.clone(), HashMap::new(), Duration::from_millis(30)).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    tokio::time::sleep(Duration::from_millis(40)).await;
    adapter.remove("i-1").await.unwrap();
    let request = server.start_operations.lock().unwrap()[0].clone();
    let error = server
        .start_operation(Request::new(request))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(server.starts.lock().unwrap().is_empty());
    assert!(server
        .start_queries
        .lock()
        .unwrap()
        .iter()
        .all(|query| query.seal_missing));
}

#[tokio::test]
async fn legacy_unknown_creation_cannot_use_new_protocol_missing_seal() {
    let server = Server {
        drop_before_admission: true,
        ..Default::default()
    };
    let (adapter, harness) =
        connect_with_timeout(server.clone(), HashMap::new(), Duration::from_millis(30)).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    drop(adapter);
    let db =
        rusqlite::Connection::open(harness._directory.path().join("operations.sqlite")).unwrap();
    let payload: String = db
        .query_row(
            "SELECT payload FROM operations WHERE runtime_id='i-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut record: serde_json::Value = serde_json::from_str(&payload).unwrap();
    record["start"]["kind"] = "start".into();
    db.execute(
        "UPDATE operations SET payload=?1 WHERE runtime_id='i-1'",
        [record.to_string()],
    )
    .unwrap();
    drop(db);
    let adapter = reconnect_with_journal(&harness).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(adapter.remove("i-1").await.is_err());
    assert!(!server.start_queries.lock().unwrap()[0].seal_missing);
    assert!(server.start_outcomes.lock().unwrap().is_empty());
}
fn spec() -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        id: "i".into(),
        tenant_id: "tenant".into(),
        image: "execd:test".into(),
        runtime_class: "runsc".into(),
        resources: Resources {
            cpu_millis: 1000,
            memory_bytes: 1 << 30,
            disk_bytes: 1 << 30,
        },
        priority: 0,
        sandbox: Default::default(),
    }
}

#[test]
fn kata_start_prepares_guest_pty_without_privileged_guest_capabilities() {
    let mut spec = spec();
    spec.runtime_class = "kata".into();
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    let pts = request
        .mounts
        .iter()
        .find(|mount| mount.target == "/dev/pts")
        .expect("guest-native PTY requires an OCI devpts mount");
    assert_eq!(pts.r#type, "devpts");
    assert_eq!(pts.source, Some(mount::Source::HostPath("devpts".into())));
    for option in [
        "newinstance",
        "ptmxmode=0666",
        "mode=0620",
        "gid=5",
        "nosuid",
        "noexec",
    ] {
        assert!(pts.options.iter().any(|value| value == option));
    }
}

#[test]
fn kata_start_preserves_explicit_terminal_mount() {
    let mut spec = spec();
    spec.runtime_class = "kata".into();
    spec.sandbox.mounts.push(adx_core::sandbox::Mount {
        kind: "bind".into(),
        target: "/dev/pts".into(),
        options: vec!["rbind".into()],
        source: adx_core::sandbox::StorageSource::Local("/runtime/pts".into()),
    });
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    let mounts: Vec<_> = request
        .mounts
        .iter()
        .filter(|mount| mount.target == "/dev/pts")
        .collect();
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].r#type, "bind");
    assert_eq!(
        mounts[0].source,
        Some(mount::Source::HostPath("/runtime/pts".into()))
    );
}

#[test]
fn other_runtimes_keep_their_terminal_preparation() {
    for runtime in ["runsc", "runc", "firecracker"] {
        let mut spec = spec();
        spec.runtime_class = runtime.into();
        let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
        assert!(!request
            .mounts
            .iter()
            .any(|mount| mount.target == "/dev/pts"));
    }
}

#[test]
fn start_maps_public_sandbox_contract_to_backend_protocol() {
    use adx_core::sandbox as model;
    let mut spec = spec();
    spec.sandbox = model::SandboxOptions {
        rootfs: Some(model::Rootfs {
            readonly: true,
            source: model::StorageSource::Image("registry.example/app:v1".into()),
        }),
        mounts: vec![model::Mount {
            kind: "bind".into(),
            target: "/workspace".into(),
            options: vec!["ro".into()],
            source: model::StorageSource::S3(model::S3Source {
                endpoint: "https://s3.example".into(),
                bucket: "artifacts".into(),
                object: "workspace.erofs".into(),
                access_key_id: "key".into(),
                access_key_secret: "secret".into(),
            }),
        }],
        network: Some(model::NetworkPolicy {
            traffic: Some(model::TrafficPolicy {
                ingress_default_action: model::NetworkAction::Deny,
                egress_default_action: model::NetworkAction::Allow,
                rules: vec![],
                mode: model::TrafficMode::Stateful,
            }),
            dns: None,
        }),
        ports: vec![8080],
        inherit_entrypoint: true,
        limits: model::ResourceLimits {
            cpu_millis: 1500,
            memory_bytes: 2 << 30,
            disk_bytes: 4 << 30,
        },
        extra_config: "runtime-option=true".into(),
        ..Default::default()
    };
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    let rootfs = request.rootfs.unwrap();
    assert_eq!(rootfs.r#type, RootfsSrcType::Image as i32);
    assert_eq!(
        rootfs.source,
        Some(rootfs_config::Source::ImageUrl(
            "registry.example/app:v1".into()
        ))
    );
    assert_eq!(request.mounts.len(), 1);
    assert!(matches!(
        request.mounts[0].source,
        Some(mount::Source::S3Config(_))
    ));
    assert_eq!(request.resources["CPU"], 1500.0);
    assert_eq!(request.resources["Memory"], 2048.0);
    assert_eq!(request.writable_layer_limit_bytes, 4 << 30);
    assert_eq!(request.extra_config, "runtime-option=true");
    assert_eq!(request.inject_entrypoint, "/etc/adx-image-process.json");
    assert_eq!(
        request.envs["ADX_IMAGE_PROCESS_CONFIG"],
        "/etc/adx-image-process.json"
    );
    let rules = &request.network_policy.unwrap().traffic.unwrap().rules;
    for port in [50090, 8080] {
        let rule = rules
            .iter()
            .find(|rule| {
                rule.sandbox_port_range
                    .as_ref()
                    .is_some_and(|range| range.first == port && range.last == port)
            })
            .expect("platform port must use a schema-v2 exact range");
        assert_eq!(rule.sandbox_port, 0);
        assert_eq!(rule.priority, u32::MAX);
    }
}

#[test]
fn start_maps_s3_rootfs_credentials() {
    use adx_core::sandbox as model;
    let mut spec = spec();
    spec.image.clear();
    spec.sandbox.rootfs = Some(model::Rootfs {
        readonly: false,
        source: model::StorageSource::S3(model::S3Source {
            endpoint: "https://s3.example".into(),
            bucket: "rootfs".into(),
            object: "app.erofs".into(),
            access_key_id: "key".into(),
            access_key_secret: "secret".into(),
        }),
    });
    let rootfs = start_request(&spec, "i-1", 1, &[], &Config::default())
        .unwrap()
        .rootfs
        .unwrap();
    let Some(rootfs_config::Source::S3Config(s3)) = rootfs.source else {
        panic!("S3 rootfs must reach sandboxd");
    };
    assert_eq!(s3.bucket, "rootfs");
    assert_eq!(s3.access_key_id, "key");
    assert_eq!(s3.access_key_secret, "secret");
}

#[tokio::test]
async fn dynamic_network_policy_uses_generated_backend_identity_and_can_clear() {
    use adx_core::sandbox as model;
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Allow,
            rules: vec![],
            mode: model::TrafficMode::Stateful,
        }),
        dns: None,
    };
    adapter
        .set_network_policy("i-1", Some(&policy), &[8080])
        .await
        .unwrap();
    adapter
        .set_network_policy("i-1", None, &[8080])
        .await
        .unwrap();
    let updates = server.network_updates.lock().unwrap();
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].sandbox_id, "generated-backend-id");
    let rules = &updates[0]
        .network_policy
        .as_ref()
        .unwrap()
        .traffic
        .as_ref()
        .unwrap()
        .rules;
    for port in [50090, 8080] {
        let rule = rules
            .iter()
            .find(|rule| {
                rule.sandbox_port_range
                    .as_ref()
                    .is_some_and(|range| range.first == port && range.last == port)
            })
            .expect("platform port must use a schema-v2 exact range");
        assert_eq!(rule.sandbox_port, 0);
        assert_eq!(rule.priority, u32::MAX);
    }
    assert!(updates[1].network_policy.is_none());
}

#[tokio::test]
async fn deleted_id_not_found_confirms_absence() {
    let server = Server::default();
    let (adapter, _server) = connect(server.clone()).await;
    assert_eq!(
        adapter
            .start(&spec(), "i-1", 1, &[])
            .await
            .unwrap()
            .to_string(),
        "10.0.0.2"
    );
    assert!(adapter.is_running("i-1").await.unwrap());
    {
        let starts = server.starts.lock().unwrap();
        assert!(starts[0].stdout.ends_with("/runtime-logs/i-1.out"));
        assert!(starts[0].stderr.ends_with("/runtime-logs/i-1.err"));
    }
    assert_eq!(adapter.stats("i-1").await.unwrap().memory_usage_bytes, 123);
    adapter.remove("i-1").await.unwrap();
    assert!(!adapter.is_running("i-1").await.unwrap());
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn cleanup_waits_for_start_after_caller_cancellation() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let start = {
        let adapter = adapter.clone();
        tokio::spawn(async move { adapter.start(&spec(), "i-1", 1, &[]).await })
    };
    server.entered.acquire().await.unwrap().forget();
    start.abort();
    let remove = {
        let adapter = adapter.clone();
        tokio::spawn(async move { adapter.remove("i-1").await })
    };
    // Start has not settled; no Delete RPC is allowed to overtake it.
    tokio::task::yield_now().await;
    assert_eq!(*server.requests.lock().unwrap(), vec!["start:"]);
    server.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), remove)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        *server.requests.lock().unwrap(),
        vec![
            "start:",
            "delete:generated-backend-id",
            "list:generated-backend-id"
        ]
    );
}

#[tokio::test]
async fn status_metadata_cannot_prove_absent_start_settlement() {
    for code in [tonic::Code::Unknown, tonic::Code::Internal] {
        for remains in [false, true] {
            let server = Server {
                settled_error: Some(code),
                settled_error_after_start: remains,
                ..Default::default()
            };
            let (adapter, _server) = connect(server.clone()).await;
            assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
            if remains {
                adapter.remove("i-1").await.unwrap();
            } else {
                assert!(adapter.remove("i-1").await.is_err());
            }
            assert!(!*server.running.lock().unwrap());
            assert_eq!(server.starts.lock().unwrap().len(), 1);
            if remains {
                assert!(server
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|x| x == "delete:generated-backend-id"));
            }
        }
    }
}

#[tokio::test]
async fn failed_start_holds_absent_backend_without_repeating_start() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(*server.requests.lock().unwrap(), vec!["start:", "list:"]);
}

#[tokio::test]
async fn late_backend_of_failed_start_is_deleted_not_adopted() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    *server.running.lock().unwrap() = true;
    // Reconciliation discovers and retires the original execution before its
    // reservation can be released. It never repeats Start for this execution.
    adapter.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|s| s == "delete:generated-backend-id"));
}

#[derive(Default)]
struct LifecycleHooks {
    expired_authorization: std::sync::atomic::AtomicBool,
    authorization_remaining_millis: std::sync::atomic::AtomicU64,
    records: Mutex<Vec<EnvironmentRecord>>,
    activations: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Readiness for LifecycleHooks {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Routes for LifecycleHooks {
    async fn activate(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        self.activations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn retire(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl StateSink for LifecycleHooks {
    async fn authorize_execution(
        &self,
        request: &adx_core::execution::ExecutionRequest,
    ) -> adx_core::Result<adx_core::execution::ExecutionAuthorization> {
        let mut authorization = execution_authority::authorize(request)?;
        if self
            .expired_authorization
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            authorization.issued_unix_millis = 0;
            authorization.deadline_unix_millis = request.timeout_millis;
        }
        let remaining = self
            .authorization_remaining_millis
            .load(std::sync::atomic::Ordering::SeqCst);
        if remaining > 0 {
            authorization.deadline_unix_millis = authorization
                .issued_unix_millis
                .checked_add(remaining)
                .unwrap();
            authorization.issued_unix_millis = authorization
                .deadline_unix_millis
                .checked_sub(request.timeout_millis)
                .unwrap();
        }
        Ok(authorization)
    }
    async fn commit(&self, record: &EnvironmentRecord) -> adx_core::Result<Durability> {
        self.records.lock().unwrap().push(record.clone());
        Ok(Durability::Published)
    }
}

fn lifecycle_node(adapter: Sandboxd, hooks: &Arc<LifecycleHooks>) -> Adxlet {
    let node = Adxlet::new(
        "n".into(),
        Arc::new(adapter),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
    );
    node.update_capacity(spec().resources, Duration::from_secs(30))
        .unwrap();
    node
}

fn lifecycle_assignment() -> Assignment {
    Assignment {
        environment_id: "i".into(),
        node_id: "n".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    }
}

fn bind_pending_creation(server: &Server) {
    for outcome in server.start_outcomes.lock().unwrap().values_mut() {
        outcome.state = StartOperationState::Pending as i32;
        outcome.result = Some(StartResponse {
            id: "generated-backend-id".into(),
            resource_generation: server.resource_generation.lock().unwrap().clone(),
            ..Default::default()
        });
    }
}

#[tokio::test]
async fn pending_creation_binding_drives_conditional_cleanup_before_budget_return() {
    let server = Server {
        transport_error: true,
        hide_inventory: true,
        ..Default::default()
    };
    let (adapter, _harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), spec().resources);
    bind_pending_creation(&server);

    node.monitor_environments().await.unwrap();

    assert_eq!(node.used(), Resources::default());
    let retired = environment.sync().await.unwrap().record;
    assert_eq!(retired.state, EnvironmentState::Failed);
    assert!(!retired.resources_held);
    let deletes = server.conditional_deletes.lock().unwrap().clone();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].id, "generated-backend-id");
    assert_eq!(deletes[0].expected_generation, "physical-generation-1");
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(
        server
            .legacy_deletes
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.state != EnvironmentState::Running));
    drop(deletes);
    node.monitor_environments().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    assert_eq!(server.conditional_deletes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn pending_creation_invalid_binding_keeps_budget_and_never_deletes() {
    for mode in [
        "missing-result",
        "missing-id",
        "missing-generation",
        "invalid-generation",
        "failed-code",
        "operation",
        "fingerprint",
        "deadline",
        "unknown",
    ] {
        let server = Server {
            transport_error: true,
            hide_inventory: true,
            ..Default::default()
        };
        let (adapter, _harness) = connect(server.clone()).await;
        let hooks = Arc::new(LifecycleHooks::default());
        let node = lifecycle_node(adapter, &hooks);
        let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
        assert!(environment.create().await.is_err());
        bind_pending_creation(&server);
        for outcome in server.start_outcomes.lock().unwrap().values_mut() {
            match mode {
                "missing-result" => outcome.result = None,
                "missing-id" => outcome.result.as_mut().unwrap().id.clear(),
                "missing-generation" => {
                    outcome.result.as_mut().unwrap().resource_generation.clear()
                }
                "invalid-generation" => {
                    outcome.result.as_mut().unwrap().resource_generation =
                        "invalid generation".into()
                }
                "failed-code" => outcome.result.as_mut().unwrap().code = -1,
                "operation" => outcome.operation_id = uuid::Uuid::new_v4().to_string(),
                "fingerprint" => outcome.request_fingerprint = "b".repeat(64),
                "deadline" => outcome.deadline_unix_millis += 1,
                "unknown" => outcome.state = StartOperationState::Unknown as i32,
                _ => unreachable!(),
            }
        }
        assert!(node.monitor_environments().await.is_err(), "{mode}");
        assert_eq!(node.used(), spec().resources, "{mode}");
        assert!(
            environment.sync().await.unwrap().record.resources_held,
            "{mode}"
        );
        assert!(
            server.conditional_deletes.lock().unwrap().is_empty(),
            "{mode}"
        );
        assert_eq!(
            server
                .legacy_deletes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}

#[tokio::test]
async fn pending_creation_lost_cleanup_reply_reopens_original_intent_before_budget_return() {
    let server = Server {
        transport_error: true,
        hide_inventory: true,
        lose_delete_reply_once: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    bind_pending_creation(&server);
    assert!(node.monitor_environments().await.is_err());
    assert_eq!(node.used(), spec().resources);
    let failed = environment.sync().await.unwrap().record;
    assert!(failed.resources_held);
    let original_delete = server.conditional_deletes.lock().unwrap()[0].clone();
    drop(environment);
    drop(node);

    let restored = reconnect_with_journal(&harness).await;
    let restarted = lifecycle_node(restored, &hooks);
    restarted.reconcile(vec![failed]).await.unwrap();
    assert_eq!(restarted.used(), Resources::default());
    let deletes = server.conditional_deletes.lock().unwrap();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[1], original_delete);
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(
        server.start_queries.lock().unwrap().len(),
        2,
        "reopen consumes the durable original binding without adopting another query"
    );
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.state != EnvironmentState::Running));
}

#[tokio::test]
async fn pending_creation_cleanup_failure_remains_held_after_reopen() {
    let server = Server {
        transport_error: true,
        hide_inventory: true,
        delete_error: Some(tonic::Code::Unavailable),
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    bind_pending_creation(&server);
    assert!(node.monitor_environments().await.is_err());
    let failed = environment.sync().await.unwrap().record;
    assert!(failed.resources_held);
    drop(environment);
    drop(node);
    let restored = reconnect_with_journal(&harness).await;
    let restarted = lifecycle_node(restored, &hooks);
    assert!(restarted.reconcile(vec![failed]).await.is_err());
    assert_eq!(restarted.used(), spec().resources);
    let deletes = server.conditional_deletes.lock().unwrap();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0], deletes[1]);
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.resources_held));
}

#[tokio::test]
async fn pending_creation_wrong_retirement_receipt_cannot_return_budget() {
    let server = Server {
        transport_error: true,
        hide_inventory: true,
        retired_generation: Some("neighbor-generation".into()),
        ..Default::default()
    };
    let (adapter, _harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    bind_pending_creation(&server);
    assert!(node.monitor_environments().await.is_err());
    assert_eq!(node.used(), spec().resources);
    assert!(environment.sync().await.unwrap().record.resources_held);
    assert_eq!(
        server.conditional_deletes.lock().unwrap()[0].expected_generation,
        "physical-generation-1"
    );
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.resources_held));
}

#[tokio::test]
async fn pending_creation_saved_binding_cannot_adopt_replacement_generation() {
    let server = Server {
        transport_error: true,
        hide_inventory: true,
        lose_delete_reply_once: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    bind_pending_creation(&server);
    assert!(node.monitor_environments().await.is_err());
    let failed = environment.sync().await.unwrap().record;
    *server.resource_generation.lock().unwrap() = "replacement-generation".into();
    *server.running.lock().unwrap() = true;
    bind_pending_creation(&server);
    drop(environment);
    drop(node);
    let restored = reconnect_with_journal(&harness).await;
    let restarted = lifecycle_node(restored, &hooks);
    assert!(restarted.reconcile(vec![failed]).await.is_err());
    assert_eq!(restarted.used(), spec().resources);
    assert!(
        *server.running.lock().unwrap(),
        "the replacement must survive old cleanup"
    );
    let deletes = server.conditional_deletes.lock().unwrap();
    assert_eq!(deletes.len(), 2);
    assert!(deletes
        .iter()
        .all(|r| r.expected_generation == "physical-generation-1"));
    assert_eq!(server.start_queries.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn pending_creation_journal_failure_prevents_cleanup_dispatch() {
    let server = Server {
        transport_error: true,
        hide_inventory: true,
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    bind_pending_creation(&server);
    let path = harness._directory.path().join("operations.sqlite");
    let saved = path.with_extension("sqlite.original");
    std::fs::rename(&path, &saved).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(node.monitor_environments().await.is_err());
    assert_eq!(node.used(), spec().resources);
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(saved, path).unwrap();
    node.monitor_environments().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    assert_eq!(server.conditional_deletes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn pending_creation_legacy_operation_cannot_authorize_cleanup() {
    let server = Server {
        transport_error: true,
        hide_inventory: true,
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    bind_pending_creation(&server);
    drop(adapter);
    let db =
        rusqlite::Connection::open(harness._directory.path().join("operations.sqlite")).unwrap();
    let payload: String = db
        .query_row(
            "SELECT payload FROM operations WHERE runtime_id='i-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut record: serde_json::Value = serde_json::from_str(&payload).unwrap();
    record["start"]["kind"] = "start".into();
    db.execute(
        "UPDATE operations SET payload=?1 WHERE runtime_id='i-1'",
        [record.to_string()],
    )
    .unwrap();
    drop(db);
    let restored = reconnect_with_journal(&harness).await;
    assert!(restored.remove("i-1").await.is_err());
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
    assert!(
        !server
            .start_queries
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .seal_missing
    );
}

#[tokio::test]
async fn unknown_start_holds_capacity_until_late_backend_is_retired() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), spec().resources);
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(failed.resources_held);
    let held_revision = failed.revision;
    // Empty inventory is only a point-in-time observation. The interrupted
    // daemon operation may still publish the original sandbox later.
    assert!(node.monitor_environments().await.is_err());
    assert_eq!(node.used(), spec().resources);
    *server.running.lock().unwrap() = true;
    node.monitor_environments().await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert_eq!(node.used(), Resources::default());
    let retired = environment.sync().await.unwrap().record;
    assert_eq!(retired.state, EnvironmentState::Failed);
    assert!(!retired.resources_held);
    assert!(retired.revision > held_revision);
    assert!(environment.create().await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(
        hooks.activations.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.state != EnvironmentState::Running));
}

#[tokio::test]
async fn durable_non_creation_returns_node_reservation_without_runtime_deletion() {
    let server = Server {
        not_created_on_start: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(!failed.resources_held);
    assert!(server.starts.lock().unwrap().is_empty());
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
    assert_eq!(
        hooks.activations.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn queried_start_with_lost_delete_reply_holds_budget_until_matching_receipt() {
    let server = Server {
        transport_error_after_start: true,
        hide_inventory: true,
        lose_delete_reply_once: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    assert!(!*server.running.lock().unwrap());
    assert_eq!(node.used(), spec().resources);
    assert!(environment.sync().await.unwrap().record.resources_held);
    node.monitor_environments().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    assert!(!environment.sync().await.unwrap().record.resources_held);
    let deletes = server.conditional_deletes.lock().unwrap();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0], deletes[1]);
    assert_eq!(deletes[0].expected_generation, "physical-generation-1");
    assert_eq!(server.start_queries.lock().unwrap().len(), 1);
    assert_eq!(
        hooks.activations.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn restart_unknown_start_keeps_reservation_until_late_retirement() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    let failed = environment.sync().await.unwrap().record;
    assert!(failed.resources_held);
    drop(environment);
    drop(node);

    let restored = Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config {
            operation_journal: Some(harness._directory.path().join("operations.sqlite")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let restarted = lifecycle_node(restored, &hooks);
    assert!(restarted.reconcile(vec![failed.clone()]).await.is_err());
    assert_eq!(restarted.used(), spec().resources);
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.resources_held));

    // The same operation can publish a physical execution after the first
    // complete inventory. It is cleanup-only, never a second creation.
    *server.running.lock().unwrap() = true;
    restarted.reconcile(vec![failed]).await.unwrap();
    assert_eq!(restarted.used(), Resources::default());
    assert!(!*server.running.lock().unwrap());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(server.conditional_deletes.lock().unwrap().len(), 1);
    assert_eq!(
        hooks.activations.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

async fn reconnect_with_journal(harness: &Harness) -> Sandboxd {
    Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config {
            operation_journal: Some(harness._directory.path().join("operations.sqlite")),
            env: HashMap::new(),
            runtime_logs: Some(adxlet::runtime_logs::RuntimeLogPolicy {
                directory: harness._directory.path().join("runtime-logs"),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn journal_reopen_replays_start_without_dispatch_and_rejects_changed_request() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let original = adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    for _ in 0..5 {
        assert_eq!(
            adapter.start(&spec(), "i-1", 1, &[]).await.unwrap(),
            original
        );
    }
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    assert_eq!(
        adapter.start(&spec(), "i-1", 1, &[]).await.unwrap(),
        original
    );
    let mut changed = spec();
    changed.env.insert(
        "PRIVATE_TEST_VALUE".into(),
        "synthetic-secret-not-to-persist".into(),
    );
    assert!(matches!(
        adapter.start(&changed, "i-1", 1, &[]).await,
        Err(adx_core::Error::Conflict)
    ));
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    adapter.remove("i-1").await.unwrap();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    adapter.remove("i-1").await.unwrap();
    assert!(matches!(
        adapter.start(&spec(), "i-1", 1, &[]).await,
        Err(adx_core::Error::Conflict)
    ));
    assert_eq!(server.starts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn journal_reopen_replays_lost_delete_receipt_with_original_physical_generation() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    server
        .lose_delete_reply_once
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(adapter.remove("i-1").await.is_err());
    assert!(!*server.running.lock().unwrap());
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    adapter.remove("i-1").await.unwrap();
    let deletes = server.conditional_deletes.lock().unwrap();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0], deletes[1]);
    assert_eq!(deletes[1].expected_generation, "physical-generation-1");
}

#[tokio::test]
async fn journal_reopen_keeps_unknown_checkpoint_and_never_reissues_it() {
    let server = Server {
        checkpoint_error: Some(tonic::Code::Unavailable),
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = harness._directory.path().join("checkpoint-unknown");
    assert!(adapter
        .checkpoint("i-1", &checkpoint, Duration::from_secs(1))
        .await
        .is_err());
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    assert!(adapter
        .checkpoint("i-1", &checkpoint, Duration::from_secs(1))
        .await
        .is_err());
    assert_eq!(server.conditional_checkpoints.lock().unwrap().len(), 1);
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn journal_intent_is_durable_before_start_and_contains_no_request_secrets() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, harness) = connect(server.clone()).await;
    let mut request = spec();
    request.env.insert(
        "PRIVATE_TEST_VALUE".into(),
        "synthetic-secret-not-to-persist".into(),
    );
    let start = tokio::spawn(async move { adapter.start(&request, "i-1", 1, &[]).await });
    server.entered.acquire().await.unwrap().forget();
    let database =
        rusqlite::Connection::open(harness._directory.path().join("operations.sqlite")).unwrap();
    let payload: String = database
        .query_row(
            "SELECT payload FROM operations WHERE runtime_id='i-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let record: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(record["state"], "StartUncertain");
    assert!(record["start"]["deadline_unix_millis"].as_u64().unwrap() > 0);
    assert_eq!(
        record["start"]["id"].as_str().unwrap(),
        server.labels.lock().unwrap()["akernel.scheduler/start-operation-id"]
    );
    assert!(!payload.contains("synthetic-secret-not-to-persist"));
    assert!(!payload.contains("PRIVATE_TEST_VALUE"));
    server.release.add_permits(1);
    start.await.unwrap().unwrap();
}

#[tokio::test]
async fn journal_failure_prevents_start_dispatch_and_missing_entry_cannot_prove_absence() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    assert!(adapter.remove("i-1").await.is_err());
    std::fs::remove_file(harness._directory.path().join("operations.sqlite")).unwrap();
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(server.starts.lock().unwrap().is_empty());
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn journal_second_owner_and_corruption_are_rejected_without_reset() {
    let (adapter, harness) = connect(Server::default()).await;
    let config = Config {
        operation_journal: Some(harness._directory.path().join("operations.sqlite")),
        ..Default::default()
    };
    assert!(
        Sandboxd::connect(harness._directory.path().join("rpc.sock"), config.clone())
            .await
            .is_err()
    );
    drop(adapter);
    let bytes = b"this is not a sqlite database";
    std::fs::write(harness._directory.path().join("operations.sqlite"), bytes).unwrap();
    assert!(
        Sandboxd::connect(harness._directory.path().join("rpc.sock"), config)
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(harness._directory.path().join("operations.sqlite")).unwrap(),
        bytes
    );
}

#[tokio::test]
async fn start_timeout_remains_failed_when_success_response_arrives_late() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks)
        .with_operation_timeout(Duration::from_millis(100))
        .unwrap();
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    let create = {
        let environment = environment.clone();
        tokio::spawn(async move { environment.create().await })
    };
    server.entered.acquire().await.unwrap().forget();
    assert!(tokio::time::timeout(Duration::from_secs(1), create)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(failed.resources_held);
    assert_eq!(node.used(), spec().resources);
    let held_revision = failed.revision;
    server.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), node.monitor_environments())
        .await
        .unwrap()
        .unwrap();
    assert!(!*server.running.lock().unwrap());
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(!failed.resources_held);
    assert!(failed.revision > held_revision);
    assert_eq!(node.used(), Resources::default());
    assert!(environment.create().await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(
        hooks.activations.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.state != EnvironmentState::Running));
}

#[tokio::test]
async fn transport_failure_can_clean_an_execution_discovered_by_labels() {
    let server = Server {
        transport_error_after_start: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());

    adapter.remove("i-1").await.unwrap();

    assert_eq!(
        *server.requests.lock().unwrap(),
        vec![
            "start:",
            "list:",
            "delete:generated-backend-id",
            "list:generated-backend-id",
        ]
    );
    assert!(!*server.running.lock().unwrap());
}

#[tokio::test]
async fn malformed_ip_can_be_cleaned_after_completed_start() {
    let (adapter, _server) = connect(Server {
        ip: String::new(),
        ..Default::default()
    })
    .await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn delete_ack_without_absence_is_not_success() {
    let (adapter, _server) = connect(Server {
        retain_after_delete: true,
        ..Default::default()
    })
    .await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    assert!(adapter.remove("i-1").await.is_err());
}

#[tokio::test]
async fn physical_fence_delete_never_falls_back_or_accepts_not_found() {
    for code in [
        tonic::Code::Unimplemented,
        tonic::Code::NotFound,
        tonic::Code::Unavailable,
    ] {
        let server = Server {
            delete_error: Some(code),
            ..Default::default()
        };
        let (adapter, _harness) = connect(server.clone()).await;
        adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
        assert!(adapter.remove("i-1").await.is_err(), "status {code:?}");
        assert!(*server.running.lock().unwrap());
        assert_eq!(
            server
                .legacy_deletes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}

#[tokio::test]
async fn physical_fence_mismatched_receipt_keeps_capacity_despite_absent_inventory() {
    let server = Server {
        retired_generation: Some("other-generation".into()),
        ..Default::default()
    };
    let (adapter, _harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    environment.create().await.unwrap();
    assert!(environment.delete().await.is_err());
    assert!(!*server.running.lock().unwrap());
    assert_eq!(node.used(), spec().resources);
    assert!(environment.sync().await.unwrap().record.resources_held);
}

#[tokio::test]
async fn physical_fence_reused_id_cannot_be_deleted_or_checkpointed() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    *server.resource_generation.lock().unwrap() = "replacement-generation".into();
    assert!(adapter.is_running("i-1").await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    let checkpoint = tempfile::tempdir().unwrap();
    assert!(adapter
        .checkpoint("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .is_err());
    assert!(*server.running.lock().unwrap());
    assert!(server.checkpoints.lock().unwrap().is_empty());
    assert_eq!(
        server
            .legacy_deletes
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        server
            .legacy_checkpoints
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        server.conditional_deletes.lock().unwrap()[0].expected_generation,
        "physical-generation-1"
    );
    assert_eq!(
        server.conditional_checkpoints.lock().unwrap()[0].expected_generation,
        "physical-generation-1"
    );
}

#[tokio::test]
async fn physical_fence_missing_start_and_inventory_generation_blocks_cleanup() {
    let server = Server {
        omit_start_generation: true,
        omit_inventory_generation: true,
        ..Default::default()
    };
    let (adapter, _harness) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    assert!(*server.running.lock().unwrap());
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
    assert_eq!(
        server
            .legacy_deletes
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn missing_start_generation_can_be_reconciled_from_matching_inventory() {
    let server = Server {
        omit_start_generation: true,
        ..Default::default()
    };
    let (adapter, _harness) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert_eq!(
        server.conditional_deletes.lock().unwrap()[0].expected_generation,
        "physical-generation-1"
    );
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn lost_delete_response_holds_budget_until_matching_receipt_is_replayed() {
    let server = Server {
        lose_delete_reply_once: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        ..Default::default()
    };
    let (adapter, _harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    environment.create().await.unwrap();
    assert!(environment.delete().await.is_err());
    assert!(!*server.running.lock().unwrap());
    assert_eq!(node.used(), spec().resources);
    node.monitor_environments().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    assert!(!environment.sync().await.unwrap().record.resources_held);
    let requests = server.conditional_deletes.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert_eq!(requests[0].expected_generation, "physical-generation-1");
    assert_eq!(server.starts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn conditional_checkpoint_unavailable_never_uses_legacy_mutation() {
    let server = Server {
        checkpoint_error: Some(tonic::Code::Unimplemented),
        ..Default::default()
    };
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    assert!(adapter
        .checkpoint("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .is_err());
    assert!(*server.running.lock().unwrap());
    assert!(server.checkpoints.lock().unwrap().is_empty());
    assert_eq!(
        server
            .legacy_checkpoints
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        server.conditional_checkpoints.lock().unwrap()[0].expected_generation,
        "physical-generation-1"
    );
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn concrete_gpu_and_npu_cards_reach_the_pinned_backend_protocol() {
    use adx_core::scheduling::*;
    let server = Server::default();
    let (adapter, _server) = connect(server.clone()).await;
    let mut spec = spec();
    spec.scheduling.devices = vec![
        DeviceRequest {
            kind: DeviceKind::Gpu,
            model: Some("a".into()),
            count: 1,
        },
        DeviceRequest {
            kind: DeviceKind::Npu,
            model: None,
            count: 1,
        },
    ];
    let cards = vec![
        DeviceAllocation {
            id: 3,
            kind: DeviceKind::Gpu,
            model: "a".into(),
        },
        DeviceAllocation {
            id: 7,
            kind: DeviceKind::Npu,
            model: "x".into(),
        },
    ];
    adapter.start(&spec, "i-1", 1, &cards).await.unwrap();
    assert_eq!(
        *server.xpu.lock().unwrap(),
        vec![
            XpuAllocation {
                r#type: "gpu".into(),
                device_ids: vec![3]
            },
            XpuAllocation {
                r#type: "npu".into(),
                device_ids: vec![7]
            }
        ]
    );
}

#[tokio::test]
async fn rejected_start_without_physical_receipt_remains_uncertain() {
    let (adapter, _server) = connect(Server {
        settled_error: Some(tonic::Code::InvalidArgument),
        ..Default::default()
    })
    .await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
}

#[tokio::test]
async fn every_start_status_requires_physical_settlement_proof() {
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::Unimplemented,
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
    ] {
        // Every failed create is terminal for this execution identity.
        // Neither a status code nor empty inventory proves physical settlement.
        let (adapter, _server) = connect(Server {
            unsettled_error: Some(code),
            ..Default::default()
        })
        .await;
        assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
        assert!(adapter.remove("i-1").await.is_err());
        assert!(
            adapter.start(&spec(), "i-1", 1, &[]).await.is_err(),
            "status {code:?}"
        );
    }
}

#[tokio::test]
async fn restart_recovers_generated_backend_id_from_labels() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    drop(adapter);
    let restored = Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config::default(),
    )
    .await
    .unwrap();
    let observed = restored.inventory().await.unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].runtime_id, "i-1");
    assert_eq!(observed[0].generation, 1);
    assert!(restored.is_running("i-1").await.unwrap());
    restored.stats("i-1").await.unwrap();
    restored.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v.starts_with("start:"))
            .count(),
        1
    );
}

#[tokio::test]
async fn daemon_reconnect_adopts_preserved_runtime_without_second_start() {
    let server = Server::default();
    let (adapter, mut harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with("start:"))
            .count(),
        1
    );

    let socket = harness._directory.path().join("rpc.sock");
    harness.task.abort();
    tokio::task::yield_now().await;
    std::fs::remove_file(&socket).unwrap();
    let listener = UnixListener::bind(&socket).unwrap();
    let service = server.clone();
    harness.task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(sandbox_service_server::SandboxServiceServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });

    let reconnected = connect_when_ready(
        socket,
        Config {
            rpc_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        Duration::from_secs(2),
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let observed = reconnected.inventory().await.unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].environment_id, "i");
    assert_eq!(observed[0].runtime_id, "i-1");
    assert_eq!(observed[0].generation, 1);
    assert!(reconnected.is_running("i-1").await.unwrap());
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with("start:"))
            .count(),
        1
    );
}

#[tokio::test]
async fn cleanup_after_restart_finds_uncommitted_runtime_by_labels() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let restored = Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config::default(),
    )
    .await
    .unwrap();
    restored.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
}

#[tokio::test]
async fn checkpoint_uses_physical_id_and_restore_generates_new_backend_id_with_fresh_environment() {
    let server = Server::default();
    let starts = server.starts.clone();
    server.release.add_permits(1);
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    adapter
        .checkpoint("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .unwrap();
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r == "checkpoint:leave_running=false"));
    adapter.remove("i-1").await.unwrap();
    adapter
        .restore(&spec(), "i-1-r5", 1, &[], checkpoint.path())
        .await
        .unwrap();
    assert!(adapter.is_running("i-1-r5").await.unwrap());
    let requests = starts.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|r| r.sandbox_id.is_empty()));
    assert_eq!(
        requests[0].envs["ADX_CHECKPOINT_HANDOFF_FILE"],
        "/run/backend/checkpoint"
    );
    assert_eq!(requests[1].envs["ADX_RUNTIME_ID"], "i-1-r5");
    assert_eq!(requests[1].envs["ADX_ENV_FILE"], "/run/backend/environment");
    assert_eq!(
        requests[1].checkpoint_info.as_ref().unwrap().checkpoint_dir,
        checkpoint.path().to_str().unwrap()
    );
}

#[tokio::test]
async fn node_startup_waits_for_backend_capabilities_readiness() {
    let server = Server::default();
    server.unready.store(2, std::sync::atomic::Ordering::SeqCst);
    let (adapter, _harness) = connect(server).await;
    adapter.wait_ready().await.unwrap();
}

#[tokio::test]
async fn node_startup_waits_when_sandboxd_socket_appears_late() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("late.sock");
    let waiting = tokio::spawn(connect_when_ready(
        socket.clone(),
        Config {
            rpc_timeout: Duration::from_millis(50),
            ..Default::default()
        },
        Duration::from_millis(50),
        Duration::from_millis(10),
    ));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!waiting.is_finished());

    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(sandbox_service_server::SandboxServiceServer::new(
                Server::default(),
            ))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn workload_checkpoint_keeps_backend_running() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    adapter
        .checkpoint_running("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .unwrap();
    assert!(adapter.is_running("i-1").await.unwrap());
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r == "checkpoint:leave_running=true"));
}

#[tokio::test]
async fn checkpoint_carries_managed_identity_and_writable_files() {
    use prost::Message;
    #[derive(Clone, PartialEq, Message)]
    struct ManagedCheckpointFields {
        #[prost(string, tag = "7")]
        checkpoint_id: String,
        #[prost(bool, tag = "8")]
        include_filesystem: bool,
    }
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    adapter
        .checkpoint("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .unwrap();
    let requests = server.checkpoints.lock().unwrap();
    let fields = ManagedCheckpointFields::decode(requests[0].encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        fields.checkpoint_id,
        checkpoint.path().file_name().unwrap().to_str().unwrap()
    );
    assert!(fields.include_filesystem);
}

#[tokio::test]
async fn checkpoint_releases_only_empty_reserved_destination() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let parent = tempfile::tempdir().unwrap();
    let checkpoint = parent.path().join("reserved-artifact");
    std::fs::create_dir(&checkpoint).unwrap();
    adapter
        .checkpoint("i-1", &checkpoint, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(std::fs::read(checkpoint.join("memory")).unwrap(), b"state");
    assert_eq!(server.checkpoints.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn checkpoint_preserves_nonempty_destination_without_rpc() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    std::fs::write(checkpoint.path().join("existing"), b"owned artifact").unwrap();
    assert!(adapter
        .checkpoint("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .is_err());
    assert_eq!(
        std::fs::read(checkpoint.path().join("existing")).unwrap(),
        b"owned artifact"
    );
    assert!(server.checkpoints.lock().unwrap().is_empty());
}

#[tokio::test]
async fn checkpoint_rejects_symlink_parent_without_removing_target() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let parent = tempfile::tempdir().unwrap();
    let actual = parent.path().join("actual");
    std::fs::create_dir(&actual).unwrap();
    std::fs::create_dir(actual.join("reserved")).unwrap();
    std::os::unix::fs::symlink(&actual, parent.path().join("redirect")).unwrap();
    assert!(adapter
        .checkpoint(
            "i-1",
            &parent.path().join("redirect/reserved"),
            Duration::from_secs(60)
        )
        .await
        .is_err());
    assert!(actual.join("reserved").is_dir());
    assert!(server.checkpoints.lock().unwrap().is_empty());
}

#[tokio::test]
async fn checkpoint_requires_confirmed_success() {
    let server = Server {
        checkpoint_success: false,
        ..Default::default()
    };
    let (adapter, _harness) = connect(server).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    assert!(adapter
        .checkpoint("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_rejects_directory_without_identity_before_rpc() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    assert!(adapter
        .checkpoint("i-1", std::path::Path::new("/"), Duration::from_secs(60))
        .await
        .is_err());
    assert!(server.checkpoints.lock().unwrap().is_empty());
}

#[tokio::test]
async fn stateless_policy_allows_execd_and_published_port_replies() {
    use adx_core::sandbox as model;
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateless,
        }),
        dns: None,
    };
    let mut spec = spec();
    spec.sandbox.network = Some(policy.clone());
    spec.sandbox.ports = vec![8080];
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec, "i-1", 1, &[]).await.unwrap();
    adapter
        .set_network_policy("i-1", Some(&policy), &[8080])
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    let updates = server.network_updates.lock().unwrap();
    for policy in [
        starts[0].network_policy.as_ref().unwrap(),
        updates[0].network_policy.as_ref().unwrap(),
    ] {
        let traffic = policy.traffic.as_ref().unwrap();
        for port in [50090, 8080] {
            assert!(
                traffic.rules.iter().any(|rule| {
                    rule.action == NetworkPolicyAction::Allow as i32
                        && rule.direction == NetworkDirection::Both as i32
                        && rule.protocol == NetworkProtocol::Tcp as i32
                        && rule.priority == u32::MAX
                        && rule
                            .sandbox_port_range
                            .as_ref()
                            .is_some_and(|range| range.first == port && range.last == port)
                }),
                "stateless TCP replies from platform service port {port} must be allowed"
            );
        }
    }
}

#[tokio::test]
async fn blocked_policy_preserves_only_node_control_endpoint_at_start_and_update() {
    use adx_core::sandbox as model;
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateless,
        }),
        dns: None,
    };
    let env = HashMap::from([
        (
            "ADX_RUNTIME_CONTROL_ADDRESS".into(),
            "http://10.231.16.1:19003".into(),
        ),
        (
            "ADX_RUNTIME_CONTROL_SECRET".into(),
            "0123456789abcdef0123456789abcdef".into(),
        ),
    ]);
    let mut spec = spec();
    spec.sandbox.network = Some(policy.clone());
    // A user-provided callback must never become a platform-owned ACL exception.
    spec.env.insert(
        "ADX_RUNTIME_CONTROL_ADDRESS".into(),
        "http://10.99.1.1:5555".into(),
    );
    let request = start_request(
        &spec,
        "i-1",
        1,
        &[],
        &Config {
            env: env.clone(),
            ..Config::default()
        },
    )
    .unwrap();
    let server = Server::default();
    let (adapter, _harness) = connect_with_env(server.clone(), env).await;
    adapter.start(&spec, "i-1", 1, &[]).await.unwrap();
    adapter
        .set_network_policy("i-1", Some(&policy), &[])
        .await
        .unwrap();
    let updates = server.network_updates.lock().unwrap();
    for policy in [
        request.network_policy.as_ref().unwrap(),
        updates[0].network_policy.as_ref().unwrap(),
    ] {
        let traffic = policy.traffic.as_ref().unwrap();
        assert_eq!(
            traffic.ingress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        assert_eq!(
            traffic.egress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        let control: Vec<_> = traffic
            .rules
            .iter()
            .filter(|rule| {
                rule.direction == NetworkDirection::Both as i32 && rule.sandbox_port_range.is_none()
            })
            .collect();
        assert_eq!(
            control.len(),
            1,
            "only the node control TCP peer endpoint bypasses user egress policy"
        );
        let rule = control[0];
        assert_eq!(rule.action, NetworkPolicyAction::Allow as i32);
        assert_eq!(rule.protocol, NetworkProtocol::Tcp as i32);
        assert_eq!(rule.priority, u32::MAX);
        let peer = rule.peer.as_ref().unwrap();
        assert_eq!(peer.cidr, "10.231.16.1/32");
        assert!(peer.address.is_empty() && peer.domain.is_empty());
        assert_eq!(peer.port, 0);
        let port = peer.port_range.as_ref().unwrap();
        assert_eq!((port.first, port.last), (19003, 19003));
        assert!(rule.sandbox_port_range.is_none());
    }
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    assert!(!request
        .network_policy
        .unwrap()
        .traffic
        .unwrap()
        .rules
        .iter()
        .any(|r| r.direction == NetworkDirection::Both as i32 && r.sandbox_port_range.is_none()));
}

#[tokio::test]
async fn stateful_policy_keeps_execd_replies_independent_of_conntrack_generation() {
    use adx_core::sandbox as model;
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateful,
        }),
        dns: None,
    };
    let mut spec = spec();
    spec.sandbox.network = Some(policy.clone());
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec, "i-1", 1, &[]).await.unwrap();
    adapter
        .set_network_policy("i-1", Some(&policy), &[])
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    let updates = server.network_updates.lock().unwrap();
    for policy in [
        starts[0].network_policy.as_ref().unwrap(),
        updates[0].network_policy.as_ref().unwrap(),
    ] {
        let traffic = policy.traffic.as_ref().unwrap();
        assert_eq!(
            traffic.ingress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        assert_eq!(
            traffic.egress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        assert!(
            traffic.rules.iter().any(|rule| {
                rule.action == NetworkPolicyAction::Allow as i32
                    && rule.direction == NetworkDirection::Both as i32
                    && rule.protocol == NetworkProtocol::Tcp as i32
                    && rule.priority == u32::MAX
                    && rule
                        .sandbox_port_range
                        .as_ref()
                        .is_some_and(|range| range.first == 50090 && range.last == 50090)
            }),
            "Execd replies must survive updates to the stateful policy generation"
        );
    }
}

#[tokio::test]
async fn expired_replacement_permission_settles_never_dispatched_target_with_journal() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let mut specification = spec();
    specification.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 1,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    let environment = node
        .environment(specification, lifecycle_assignment())
        .unwrap();
    environment.create().await.unwrap();
    *server.running.lock().unwrap() = false;
    node.monitor_environments().await.unwrap();
    hooks
        .expired_authorization
        .store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(node.monitor_environments().await.is_err());
    assert_eq!(
        server.starts.lock().unwrap().len(),
        1,
        "expired permission must never dispatch a replacement"
    );
    assert_eq!(
        node.used(),
        Resources::default(),
        "a durably unstarted target must settle without treating a missing journal as proof"
    );
    let settled = environment.sync().await.unwrap().record;
    assert!(!settled.resources_held);
    assert!(settled.runtime.id.contains("-r"));
}

fn prepared_execution_permission() -> adx_core::execution::ExecutionAuthorization {
    let mut specification = spec();
    specification.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 1,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    let request = adx_core::execution::ExecutionRequest {
        source: EnvironmentRecord {
            spec: specification,
            assignment: lifecycle_assignment(),
            runtime: adx_core::Runtime {
                id: "i-1".into(),
                ip: None,
            },
            state: EnvironmentState::Failed,
            revision: 1,
            resources_held: false,
            checkpoint: None,
            last_operation: None,
            restart_attempts: 0,
            restart_pending: true,
        },
        kind: adx_core::execution::ExecutionKind::Restart,
        operation_id: "test-replacement".into(),
        expected_revision: 1,
        timeout_millis: 5000,
    };
    execution_authority::authorize(&request).unwrap()
}
fn prepared_execution_target() -> EnvironmentRecord {
    prepared_execution_permission().target
}

#[tokio::test]
async fn journal_prepared_target_survives_restart_and_cancels_without_dispatch() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let mut target = prepared_execution_target();
    target.spec.env.insert(
        "PREPARED_TEST_SECRET".into(),
        "synthetic-private-value".into(),
    );
    adapter.prepare_execution(&target).await.unwrap();
    adapter.prepare_execution(&target).await.unwrap();
    let payload: String =
        rusqlite::Connection::open(harness._directory.path().join("operations.sqlite"))
            .unwrap()
            .query_row(
                "SELECT payload FROM operations WHERE runtime_id=?1",
                [&target.runtime.id],
                |row| row.get(0),
            )
            .unwrap();
    assert!(payload.contains("Prepared"));
    assert!(!payload.contains("PREPARED_TEST_SECRET"));
    assert!(!payload.contains("synthetic-private-value"));
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    adapter.remove(&target.runtime.id).await.unwrap();
    adapter.remove(&target.runtime.id).await.unwrap();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    assert_eq!(
        adapter.prepare_execution(&target).await,
        Err(adx_core::Error::Conflict)
    );
    assert_eq!(
        adapter
            .start(&target.spec, &target.runtime.id, 1, &[])
            .await,
        Err(adx_core::Error::Conflict)
    );
    assert!(server.starts.lock().unwrap().is_empty());
    assert!(server.conditional_deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn journal_prepared_target_binds_full_spec_and_can_start_once() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let permission = prepared_execution_permission();
    let target = permission.target.clone();
    adapter.prepare_execution(&target).await.unwrap();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    let mut changed = target.clone();
    changed.spec.env.insert("changed".into(), "value".into());
    assert_eq!(
        adapter.prepare_execution(&changed).await,
        Err(adx_core::Error::Conflict)
    );
    assert_eq!(
        adapter
            .start(&changed.spec, &target.runtime.id, 1, &[])
            .await,
        Err(adx_core::Error::Conflict)
    );
    assert!(server.starts.lock().unwrap().is_empty());
    assert_eq!(
        adapter
            .start(&target.spec, &target.runtime.id, 1, &[])
            .await,
        Err(adx_core::Error::Conflict)
    );
    let first = adapter.execute_authorized(&permission, None).await.unwrap();
    assert_eq!(
        adapter.execute_authorized(&permission, None).await.unwrap(),
        first
    );
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    adapter.remove(&target.runtime.id).await.unwrap();
}

#[tokio::test]
async fn journal_preparation_write_failure_prevents_dispatch() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let target = prepared_execution_target();
    std::fs::remove_file(harness._directory.path().join("operations.sqlite")).unwrap();
    assert!(adapter.prepare_execution(&target).await.is_err());
    assert!(adapter
        .start(&target.spec, &target.runtime.id, 1, &[])
        .await
        .is_err());
    assert!(adapter.remove(&target.runtime.id).await.is_err());
    assert!(server.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn replacement_permission_expiring_during_capabilities_never_dispatches() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let mut specification = spec();
    specification.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 1,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    let environment = node
        .environment(specification, lifecycle_assignment())
        .unwrap();
    environment.create().await.unwrap();
    *server.running.lock().unwrap() = false;
    node.monitor_environments().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    hooks
        .authorization_remaining_millis
        .store(100, std::sync::atomic::Ordering::SeqCst);
    server
        .capabilities_delay_millis
        .store(300, std::sync::atomic::Ordering::SeqCst);
    let result = node.monitor_environments().await;
    assert_eq!(
        server.start_operations.lock().unwrap().len(),
        1,
        "permission expired while preparing the request; replacement must not reach sandboxd"
    );
    assert!(result.is_err());
    assert_eq!(node.used(), Resources::default());
    assert!(!environment.sync().await.unwrap().record.resources_held);
}

#[tokio::test]
async fn authorized_start_persists_original_deadline_for_reopened_cleanup() {
    let server = Server {
        transport_error_after_start: true,
        hide_inventory: true,
        ..Default::default()
    };
    let (adapter, harness) =
        connect_with_timeout(server.clone(), HashMap::new(), Duration::from_secs(30)).await;
    let permission = prepared_execution_permission();
    let id = permission.target.runtime.id.clone();
    adapter.prepare_execution(&permission.target).await.unwrap();
    assert!(adapter.execute_authorized(&permission, None).await.is_err());
    let request = server.start_operations.lock().unwrap()[0].clone();
    assert_eq!(
        request.deadline_unix_millis, permission.deadline_unix_millis,
        "adapter must not extend the Coordinator deadline"
    );
    let payload: String =
        rusqlite::Connection::open(harness._directory.path().join("operations.sqlite"))
            .unwrap()
            .query_row(
                "SELECT payload FROM operations WHERE runtime_id=?1",
                [&id],
                |row| row.get(0),
            )
            .unwrap();
    let journal: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        journal["start"]["deadline_unix_millis"].as_u64().unwrap(),
        permission.deadline_unix_millis
    );
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    adapter.remove(&id).await.unwrap();
    let query = server.start_queries.lock().unwrap()[0].clone();
    assert_eq!(query.deadline_unix_millis, permission.deadline_unix_millis);
    assert_eq!(query.operation_id, request.operation_id);
    assert_eq!(query.request_fingerprint, request.request_fingerprint);
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert!(!*server.running.lock().unwrap());
}
