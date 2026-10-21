//! Local gRPC/UDS contract tests against the pinned protocol, not sandboxd E2E.
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources};
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
struct Operation {
    request: Option<StartWithOperationRequest>,
    status: StartOperationStatus,
    done: Arc<Semaphore>,
    error: Option<Status>,
}
#[derive(Clone)]
struct Server {
    operations: Arc<Mutex<HashMap<String, Operation>>>,
    backend_id: Arc<Mutex<String>>,
    reject_protocol: bool,
    bad_receipt: bool,
    bad_identity: bool,
    identity_error: Option<tonic::Code>,
    pending_reply: bool,
    unknown_outcome: bool,
    requests: Arc<Mutex<Vec<String>>>,
    starts: Arc<Mutex<Vec<StartRequest>>>,
    network_updates: Arc<Mutex<Vec<SetNetworkPolicyRequest>>>,
    unready: Arc<std::sync::atomic::AtomicUsize>,
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
    exit_message: Option<String>,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            operations: Arc::default(),
            backend_id: Arc::default(),
            identity_error: None,
            reject_protocol: false,
            bad_receipt: false,
            bad_identity: false,
            pending_reply: false,
            unknown_outcome: false,
            requests: Arc::default(),
            starts: Arc::default(),
            network_updates: Arc::default(),
            unready: Arc::default(),
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
            exit_message: None,
        }
    }
}
#[tonic::async_trait]
impl sandbox_service_server::SandboxService for Server {
    async fn start_with_operation(
        &self,
        request: Request<StartWithOperationRequest>,
    ) -> Result<Response<StartOperationStatus>, Status> {
        if self.reject_protocol {
            return Err(Status::unimplemented("identified protocol missing"));
        }
        let request = request.into_inner();
        let start = request
            .start
            .clone()
            .ok_or_else(|| Status::invalid_argument("start missing"))?;
        if start.sandbox_id != request.sandbox_id {
            return Err(Status::invalid_argument("physical binding"));
        }
        if let Some(checkpoint) = &start.checkpoint_info {
            let pin = request
                .restore_artifacts
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("pin missing"))?;
            if pin.checkpoint_dir != checkpoint.checkpoint_dir
                || pin.expected_root_digest != "a".repeat(64)
            {
                return Err(Status::failed_precondition("checkpoint pin mismatch"));
            }
        }
        let (operation, fresh) = {
            let mut operations = self.operations.lock().unwrap();
            if let Some(old) = operations.get(&request.operation_id) {
                if old.status.retirement_requested {
                    return Err(Status::failed_precondition("retired"));
                }
                if old.request.as_ref() != Some(&request) {
                    return Err(Status::failed_precondition("replay differs"));
                }
                (old.clone(), false)
            } else {
                let op = Operation {
                    request: Some(request.clone()),
                    error: None,
                    done: Arc::new(Semaphore::new(0)),
                    status: StartOperationStatus {
                        operation_id: request.operation_id.clone(),
                        sandbox_id: request.sandbox_id.clone(),
                        resource_generation: "test-resource-generation".into(),
                        state: StartOperationState::Running as i32,
                        ..Default::default()
                    },
                };
                operations.insert(request.operation_id.clone(), op.clone());
                (op, true)
            }
        };
        if fresh {
            let service = self.clone();
            let id = request.operation_id.clone();
            let mut start = start;
            start.labels.insert(
                "akernel.scheduler/resource-generation".into(),
                "test-resource-generation".into(),
            );
            tokio::spawn(async move {
                let result = service.start(Request::new(start)).await;
                let running = *service.running.lock().unwrap();
                let done = {
                    let mut records = service.operations.lock().unwrap();
                    let op = records.get_mut(&id).unwrap();
                    op.status.state = if running {
                        StartOperationState::Succeeded
                    } else {
                        StartOperationState::Failed
                    } as i32;
                    op.status.sandbox_ip = service.ip.clone();
                    if service.unknown_outcome {
                        op.status.state = StartOperationState::Unknown as i32;
                    }
                    op.error = result.err();
                    op.done.clone()
                };
                done.add_permits(1);
            });
        }
        if self.pending_reply {
            return Ok(Response::new(operation.status));
        }
        let permit = operation.done.acquire().await.unwrap();
        drop(permit);
        let op = self
            .operations
            .lock()
            .unwrap()
            .get(&request.operation_id)
            .unwrap()
            .clone();
        if let Some(error) = op.error {
            return Err(error);
        }
        Ok(Response::new(op.status))
    }
    async fn get_start_operation(
        &self,
        request: Request<GetStartOperationRequest>,
    ) -> Result<Response<StartOperationStatus>, Status> {
        if self.reject_protocol {
            return Err(Status::unimplemented("identified protocol missing"));
        }
        let id = request.into_inner().operation_id;
        self.requests.lock().unwrap().push(format!("get:{id}"));
        self.operations
            .lock()
            .unwrap()
            .get(&id)
            .map(|op| Response::new(op.status.clone()))
            .ok_or_else(|| Status::not_found("operation"))
    }
    async fn retire_start_operation(
        &self,
        request: Request<RetireStartOperationRequest>,
    ) -> Result<Response<RetireStartOperationResponse>, Status> {
        if self.reject_protocol {
            return Err(Status::unimplemented("identified protocol missing"));
        }
        let request = request.into_inner();
        let done = {
            let mut operations = self.operations.lock().unwrap();
            let op = operations
                .entry(request.operation_id.clone())
                .or_insert_with(|| Operation {
                    request: None,
                    error: None,
                    done: Arc::new(Semaphore::new(1)),
                    status: StartOperationStatus {
                        operation_id: request.operation_id.clone(),
                        sandbox_id: request.sandbox_id.clone(),
                        state: StartOperationState::Failed as i32,
                        resource_generation: "test-resource-generation".into(),
                        ..Default::default()
                    },
                });
            if op.status.sandbox_id != request.sandbox_id {
                return Err(Status::failed_precondition("wrong sandbox"));
            }
            op.status.retirement_requested = true;
            if op.status.state == StartOperationState::Unknown as i32 {
                return Err(Status::failed_precondition("unproven executor outcome"));
            }
            op.done.clone()
        };
        let permit = done.acquire().await.unwrap();
        drop(permit);
        if *self.running.lock().unwrap() {
            self.delete(Request::new(DeleteRequest {
                id: request.sandbox_id.clone(),
                timeout: 0,
            }))
            .await?;
        }
        if *self.running.lock().unwrap() {
            return Err(Status::failed_precondition("runtime remains"));
        }
        self.operations
            .lock()
            .unwrap()
            .get_mut(&request.operation_id)
            .unwrap()
            .status
            .retirement_complete = true;
        Ok(Response::new(RetireStartOperationResponse {
            operation_id: if self.bad_receipt {
                "wrong-operation".into()
            } else {
                request.operation_id
            },
            sandbox_id: request.sandbox_id,
            resource_generation: "test-resource-generation".into(),
            retirement_receipt: "positive-test-retirement-receipt".into(),
        }))
    }
    async fn delete_if_generation(
        &self,
        _: Request<DeleteIfGenerationRequest>,
    ) -> Result<Response<DeleteIfGenerationResponse>, Status> {
        Err(Status::unimplemented(
            "adapter must use operation retirement",
        ))
    }
    async fn describe_checkpoint_artifacts(
        &self,
        request: Request<DescribeCheckpointArtifactsRequest>,
    ) -> Result<Response<RestoreArtifactIdentity>, Status> {
        if self.reject_protocol {
            return Err(Status::unimplemented("identity query missing"));
        }
        let directory = request.into_inner().checkpoint_dir;
        self.requests
            .lock()
            .unwrap()
            .push(format!("describe:{directory}"));
        if let Some(code) = self.identity_error {
            return Err(Status::new(
                code,
                "checkpoint content root is not verifiable",
            ));
        }
        Ok(Response::new(RestoreArtifactIdentity {
            checkpoint_dir: if self.bad_identity {
                "/wrong/checkpoint".into()
            } else {
                directory
            },
            expected_root_digest: "a".repeat(64),
        }))
    }
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let request = request.into_inner();
        assert!(request.sandbox_id.starts_with("sbox-"));
        *self.backend_id.lock().unwrap() = request.sandbox_id.clone();
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
            id: self.backend_id.lock().unwrap().clone(),
            sandbox_ip: self.ip.clone(),
            ..Default::default()
        }))
    }
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.requests
            .lock()
            .unwrap()
            .push(format!("delete:{}", request.into_inner().id));
        if !self.retain_after_delete {
            *self.running.lock().unwrap() = false;
        }
        Ok(Response::new(DeleteResponse::default()))
    }
    async fn list(
        &self,
        request: Request<ListSandboxesRequest>,
    ) -> Result<Response<ListSandboxesResponse>, Status> {
        let request = request.into_inner();
        let id = request.id;
        self.requests.lock().unwrap().push(format!("list:{id}"));
        if (!*self.running.lock().unwrap() && self.exit_message.is_none())
            || (!id.is_empty() && id != *self.backend_id.lock().unwrap())
        {
            if !id.is_empty() {
                return Err(Status::not_found("environment not found"));
            }
            return Ok(Response::new(ListSandboxesResponse::default()));
        }
        let labels = self.labels.lock().unwrap().clone();
        if request
            .selector
            .iter()
            .any(|(k, v)| labels.get(k) != Some(v))
        {
            return Ok(Response::new(ListSandboxesResponse::default()));
        }
        Ok(Response::new(ListSandboxesResponse {
            sandboxes: vec![SandboxStatus {
                id: self.backend_id.lock().unwrap().clone(),
                labels,
                state: if *self.running.lock().unwrap() {
                    SandboxState::Running as i32
                } else {
                    SandboxState::Exited as i32
                },
                ..Default::default()
            }],
        }))
    }
    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        assert_eq!(request.into_inner().id, *self.backend_id.lock().unwrap());
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
        let r = request.into_inner();
        assert_eq!(r.id, *self.backend_id.lock().unwrap());
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
        Ok(Response::new(CheckpointResponse {}))
    }
    async fn wait(&self, request: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        self.requests
            .lock()
            .unwrap()
            .push(format!("wait:{}", request.into_inner().id));
        Ok(Response::new(WaitResponse {
            status: 0,
            exit_code: 137,
            message: self.exit_message.clone().unwrap_or_default(),
        }))
    }
    async fn list_available_runtimes(
        &self,
        _: Request<ListAvailableRuntimesRequest>,
    ) -> Result<Response<ListAvailableRuntimesResponse>, Status> {
        use std::sync::atomic::Ordering;
        if self
            .unready
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Status::unavailable("environment service not ready"));
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
            node_id: "n".into(),
            env,
            rpc_timeout: Duration::from_secs(2),
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
        execution: Default::default(),
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
fn shared_volume_references_cannot_bypass_durable_mount_admission() {
    use adx_core::shared_volumes::{
        SharedVolumeAccess, SharedVolumeId, SharedVolumeMount, SharedVolumeSubpath,
    };
    for runtime in ["runsc", "firecracker"] {
        for access in [SharedVolumeAccess::ReadOnly, SharedVolumeAccess::ReadWrite] {
            let mut spec = spec();
            spec.runtime_class = runtime.into();
            spec.execution.shared_volumes.push(SharedVolumeMount {
                volume_id: SharedVolumeId::parse("00000000-0000-4000-8000-000000000001").unwrap(),
                subpath: SharedVolumeSubpath::parse("project").unwrap(),
                target: "/shared".into(),
                access,
            });
            assert!(matches!(
                start_request(&spec, "i-1", 1, &[], &Config::default()),
                Err(adx_core::Error::Unavailable(message))
                    if message == "controlled shared volume mounting requires durable node attachment admission"
            ));
        }
    }
}

#[test]
fn kata_start_preserves_explicit_terminal_mount() {
    let mut spec = spec();
    spec.runtime_class = "kata".into();
    spec.execution.mounts.push(adx_core::execution::Mount {
        kind: "bind".into(),
        target: "/dev/pts".into(),
        options: vec!["rbind".into()],
        source: adx_core::execution::StorageSource::Local("/runtime/pts".into()),
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
fn start_maps_public_environment_contract_to_backend_protocol() {
    use adx_core::execution as model;
    let mut spec = spec();
    spec.execution = model::ExecutionOptions {
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
    use adx_core::execution as model;
    let mut spec = spec();
    spec.image.clear();
    spec.execution.rootfs = Some(model::Rootfs {
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
    use adx_core::execution as model;
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
    assert_eq!(updates[0].sandbox_id, *server.backend_id.lock().unwrap());
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
    assert!(!server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|v| v.starts_with("delete:")));
    server.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), remove)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!*server.running.lock().unwrap());
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|v| v == &format!("delete:{}", *server.backend_id.lock().unwrap())));
}

#[tokio::test]
async fn settled_start_errors_release_resources_after_confirmed_cleanup() {
    for code in [tonic::Code::Unknown, tonic::Code::Internal] {
        for remains in [false, true] {
            let server = Server {
                settled_error: Some(code),
                settled_error_after_start: remains,
                ..Default::default()
            };
            let (adapter, _server) = connect(server.clone()).await;
            assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
            adapter.remove("i-1").await.unwrap();
            assert!(!*server.running.lock().unwrap());
            assert_eq!(server.starts.lock().unwrap().len(), 1);
            if remains {
                assert!(server
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|x| x == &format!("delete:{}", *server.backend_id.lock().unwrap())));
            }
        }
    }
}

#[tokio::test]
async fn failed_start_releases_absent_backend_without_repeating_start() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert!(!*server.running.lock().unwrap());
}

#[tokio::test]
async fn late_backend_of_failed_start_is_deleted_not_adopted() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
    *server.running.lock().unwrap() = true;
    // Reconciliation discovers the original execution after its failed create
    // released capacity. It cleans that backend rather than repeating Start.
    adapter.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|s| s == &format!("delete:{}", *server.backend_id.lock().unwrap())));
}

#[derive(Default)]
struct LifecycleHooks {
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

#[tokio::test]
async fn failed_create_monitor_removes_late_backend_after_capacity_release() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks);
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
    assert_eq!(
        environment.sync().await.unwrap().record.state,
        EnvironmentState::Failed
    );
    *server.running.lock().unwrap() = true;
    node.monitor_environments().await.unwrap();
    assert!(!*server.running.lock().unwrap());
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
    assert!(!failed.resources_held);
    assert_eq!(node.used(), Resources::default());
    server.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), node.monitor_environments())
        .await
        .unwrap()
        .unwrap();
    assert!(!*server.running.lock().unwrap());
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(!failed.resources_held);
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

    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|v| v == &format!("delete:{}", *server.backend_id.lock().unwrap())));
    assert_eq!(server.starts.lock().unwrap().len(), 1);
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
async fn rejected_start_can_be_cleaned_without_uncertain_outcome() {
    let (adapter, _server) = connect(Server {
        settled_error: Some(tonic::Code::InvalidArgument),
        ..Default::default()
    })
    .await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn every_start_error_is_failed_and_absence_allows_cleanup() {
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::Unimplemented,
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
    ] {
        // Every failed create is terminal for this execution identity.
        // A backend arriving later is handled by reconciliation.
        let (adapter, _server) = connect(Server {
            unsettled_error: Some(code),
            ..Default::default()
        })
        .await;
        assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
        adapter.remove("i-1").await.unwrap();
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
        Config {
            node_id: "n".into(),
            ..Default::default()
        },
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
            node_id: "n".into(),
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
        Config {
            node_id: "n".into(),
            ..Default::default()
        },
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
    assert!(requests.iter().all(|r| !r.sandbox_id.is_empty()));
    assert_ne!(requests[0].sandbox_id, requests[1].sandbox_id);
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
            node_id: "n".into(),
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
async fn stateless_policy_allows_execd_and_published_port_replies() {
    use adx_core::execution as model;
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
    spec.execution.network = Some(policy.clone());
    spec.execution.ports = vec![8080];
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
    use adx_core::execution as model;
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
    spec.execution.network = Some(policy.clone());
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
    use adx_core::execution as model;
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
    spec.execution.network = Some(policy.clone());
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
async fn exited_backend_uses_wait_reason_before_deletion_and_not_exit_code_alone() {
    for (message, reason) in [
        (
            "sandbox was oom-killed by the kernel (memory cgroup limit exceeded)",
            adx_core::RuntimeFailureReason::OomKilled,
        ),
        ("", adx_core::RuntimeFailureReason::Exited),
    ] {
        let server = Server {
            exit_message: Some(message.into()),
            ..Default::default()
        };
        let (adapter, _guard) = connect(server.clone()).await;
        RuntimeDriver::start(&adapter, &spec(), "i-1", 1, &[])
            .await
            .unwrap();
        assert!(adapter.observe("i-1").await.unwrap().running);
        assert!(!server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.starts_with("wait:")));
        *server.running.lock().unwrap() = false;
        let status = adapter.observe("i-1").await.unwrap();
        assert!(!status.running);
        let failure = status.failure.unwrap();
        assert_eq!(failure.reason, reason);
        assert_eq!(failure.exit_code, Some(137));
        assert!(server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|v| v == &format!("wait:{}", *server.backend_id.lock().unwrap())));
    }
}

#[test]
fn constructor_cwd_is_owned_by_spec_and_does_not_change_execd_process_cwd() {
    let mut spec = spec();
    spec.env
        .insert("ADX_ENVIRONMENT_CWD".into(), "/spoofed".into());
    let mut config = Config::default();
    config
        .env
        .insert("ADX_ENVIRONMENT_CWD".into(), "/deployment".into());
    let omitted = start_request(&spec, "i-1", 1, &[], &config).unwrap();
    assert!(!omitted.envs.contains_key("ADX_ENVIRONMENT_CWD"));
    spec.execution.cwd = Some("/workspace/nested".into());
    let request = start_request(&spec, "i-1", 1, &[], &config).unwrap();
    assert_eq!(request.envs["ADX_ENVIRONMENT_CWD"], "/workspace/nested");
    assert_eq!(request.cwd, "/");
}

#[tokio::test]
async fn cold_start_uses_create_deadline_beyond_the_node_rpc_cap() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let task = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            adapter
                .start_with_deadline(
                    &spec(),
                    "i-1",
                    1,
                    &[],
                    tokio::time::Instant::now() + Duration::from_secs(5),
                )
                .await
        })
    };
    server.entered.acquire().await.unwrap().forget();
    // The fixture's node RPC cap is two seconds; cold import takes longer.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    server.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap().to_string(), "10.0.0.2");
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn expired_create_deadline_never_issues_start() {
    let server = Server::default();
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter
        .start_with_deadline(
            &spec(),
            "i-1",
            1,
            &[],
            tokio::time::Instant::now() - Duration::from_millis(1)
        )
        .await
        .is_err());
    assert!(server.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn controller_cold_preparation_uses_total_create_budget_then_readiness_budget() {
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
    let task = {
        let environment = environment.clone();
        tokio::spawn(async move {
            environment
                .create_with_deadline(tokio::time::Instant::now() + Duration::from_secs(3))
                .await
        })
    };
    server.entered.acquire().await.unwrap().forget();
    tokio::time::sleep(Duration::from_millis(200)).await;
    server.release.add_permits(1);
    assert_eq!(
        task.await.unwrap().unwrap().record.state,
        EnvironmentState::Running
    );
    assert_eq!(
        environment
            .create_with_deadline(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .record
            .state,
        EnvironmentState::Running
    );
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    environment.delete().await.unwrap();
}

fn volume_attachment() -> adx_core::shared_volume_attachments::SharedVolumeAttachment {
    use adx_core::shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeSubpath};
    let reader = protected_reader();
    adx_core::shared_volume_attachments::SharedVolumeAttachment {
        volume_id: SharedVolumeId::parse("00000000-0000-4000-8000-000000000001").unwrap(),
        subpath: SharedVolumeSubpath::default(),
        target: "/shared".into(),
        access: SharedVolumeAccess::ReadWrite,
        tenant_id: "tenant".into(),
        node_id: reader.node_id,
        identity: reader.identity,
    }
}

#[tokio::test]
async fn volume_stop_fences_a_late_start_across_adapter_restart() {
    let server = Server::default();
    let (adapter, _a) = connect(server.clone()).await;
    let attachment = volume_attachment();
    let receipts = adapter
        .retire_shared_volume_attachments(std::slice::from_ref(&attachment))
        .await
        .unwrap();
    assert_eq!(receipts[0].attachment, attachment);
    let (restarted, _b) = connect(server.clone()).await;
    assert!(restarted.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(server.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn volume_retirement_binds_the_actual_backend_resource_generation() {
    let server = Server::default();
    let (adapter, _a) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let receipt = adapter
        .retire_shared_volume_attachments(&[volume_attachment()])
        .await
        .unwrap()
        .remove(0);
    let proof: serde_json::Value = serde_json::from_str(&receipt.backend_receipt).unwrap();
    assert_eq!(
        proof["protocol"],
        "adx-shared-volume-execution-retirement/v1"
    );
    assert_eq!(proof["node_id"], "n");
    assert_eq!(proof["identity"]["runtime_id"], "i-1");
    for field in [
        "operation_id",
        "sandbox_id",
        "resource_generation",
        "retirement_receipt",
    ] {
        assert!(!proof[field].as_str().unwrap().is_empty(), "{field}");
    }
    assert!(!*server.running.lock().unwrap());
}

#[tokio::test]
async fn volume_retirement_rejects_foreign_node_without_a_backend_call() {
    let server = Server::default();
    let (adapter, _a) = connect(server.clone()).await;
    let mut attachment = volume_attachment();
    attachment.node_id = "other-node".into();
    assert!(adapter
        .retire_shared_volume_attachments(&[attachment])
        .await
        .is_err());
    assert!(server.operations.lock().unwrap().is_empty());
}

fn protected_reader() -> adx_core::CheckpointReader {
    adx_core::CheckpointReader {
        node_id: "n".into(),
        identity: adx_core::runtime::RuntimeIdentity {
            environment_id: "i".into(),
            runtime_id: "i-1".into(),
            ownership_generation: 1,
        },
        checkpoint: adx_core::RestorePoint {
            origin: None,
            id: "test-checkpoint".into(),
            artifact: adx_core::CheckpointArtifact {
                storage: "shared_fs".into(),
                location: "/test/checkpoint".into(),
                size_bytes: 5,
            },
            expires_at_unix_seconds: None,
            source_runtime_id: "i-1".into(),
        },
    }
}

#[tokio::test]
async fn restarted_adapter_cannot_confirm_retirement_before_old_start_settles() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (original, _a) = connect(server.clone()).await;
    let starting = {
        let original = original.clone();
        tokio::spawn(async move { original.start(&spec(), "i-1", 1, &[]).await })
    };
    tokio::time::timeout(Duration::from_secs(5), server.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let (restarted, _b) = connect(server.clone()).await;
    let mut retiring = tokio::spawn(async move { restarted.remove("i-1").await });
    let early = tokio::time::timeout(Duration::from_millis(100), &mut retiring).await;
    let early_success = matches!(early, Ok(Ok(Ok(()))));
    server.release.add_permits(1);
    let _ = tokio::time::timeout(Duration::from_secs(5), starting)
        .await
        .unwrap()
        .unwrap();
    if early.is_err() {
        retiring.await.unwrap().unwrap();
    }
    original.remove("i-1").await.unwrap();
    assert!(
        !*server.running.lock().unwrap(),
        "fixture cleanup must complete before assertion"
    );
    assert!(
        !early_success,
        "fresh adapter acknowledged while the admitted executor was still blocked"
    );
}

#[tokio::test]
async fn retirement_before_admission_fences_a_late_start_after_adapter_restart() {
    let server = Server::default();
    let (retiring, _a) = connect(server.clone()).await;
    retiring.remove("i-1").await.unwrap();
    let (late, _b) = connect(server.clone()).await;
    assert!(late.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(server.starts.lock().unwrap().is_empty());
    assert!(!*server.running.lock().unwrap());
}

#[tokio::test]
async fn reader_retirement_rejects_foreign_node_before_backend_rpc() {
    let server = Server::default();
    let (adapter, _a) = connect(server.clone()).await;
    let mut reader = protected_reader();
    reader.node_id = "other-node".into();
    assert!(adapter.retire_checkpoint_reader(&reader).await.is_err());
    assert!(server.operations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reader_retirement_returns_the_exact_reserved_reader_and_backend_binding() {
    let server = Server::default();
    let (adapter, _a) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let reader = protected_reader();
    let receipt = adapter.retire_checkpoint_reader(&reader).await.unwrap();
    assert_eq!(receipt.reader, reader);
    let binding: serde_json::Value = serde_json::from_str(&receipt.backend_receipt).unwrap();
    assert_eq!(binding["node_id"], "n");
    assert_eq!(binding["reader"]["identity"]["runtime_id"], "i-1");
    assert!(!binding["retirement_receipt"].as_str().unwrap().is_empty());
    assert!(!*server.running.lock().unwrap());
}

#[tokio::test]
async fn old_protocol_refusal_cannot_turn_empty_inventory_into_cleanup_proof() {
    let server = Server {
        reject_protocol: true,
        ..Default::default()
    };
    let (adapter, _a) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    assert!(adapter
        .retire_checkpoint_reader(&protected_reader())
        .await
        .is_err());
    assert!(server.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mismatched_retirement_receipt_is_not_positive_exit_proof() {
    let server = Server {
        bad_receipt: true,
        ..Default::default()
    };
    let (adapter, _a) = connect(server.clone()).await;
    assert!(adapter
        .retire_checkpoint_reader(&protected_reader())
        .await
        .is_err());
}

#[tokio::test]
async fn mismatched_checkpoint_identity_never_reaches_start_admission() {
    let server = Server {
        bad_identity: true,
        ..Default::default()
    };
    let (adapter, _a) = connect(server.clone()).await;
    let directory = tempfile::tempdir().unwrap();
    assert!(adapter
        .restore(&spec(), "i-1", 1, &[], directory.path())
        .await
        .is_err());
    assert!(server.operations.lock().unwrap().is_empty());
    assert!(server.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn running_operation_status_is_queried_until_terminal_outcome() {
    let server = Server {
        pending_reply: true,
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _a) = connect(server.clone()).await;
    let starting = {
        let adapter = adapter.clone();
        tokio::spawn(async move { adapter.start(&spec(), "i-1", 1, &[]).await })
    };
    server.entered.acquire().await.unwrap().forget();
    assert!(!starting.is_finished());
    server.release.add_permits(1);
    starting.await.unwrap().unwrap();
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|v| v.starts_with("get:")));
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn unknown_start_outcome_never_releases_a_reader_from_empty_observation() {
    let server = Server {
        unknown_outcome: true,
        ..Default::default()
    };
    let (adapter, _a) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    assert!(adapter
        .retire_checkpoint_reader(&protected_reader())
        .await
        .is_err());
    assert!(*server.running.lock().unwrap());
    assert!(server
        .operations
        .lock()
        .unwrap()
        .values()
        .all(|op| op.status.retirement_requested && !op.status.retirement_complete));
    // Owned in-memory fixture only; do not manufacture a successful receipt.
    *server.running.lock().unwrap() = false;
}

#[tokio::test]
async fn historical_success_does_not_adopt_an_exited_backend_or_repeat_execution() {
    let server = Server::default();
    let (original, _a) = connect(server.clone()).await;
    original.start(&spec(), "i-1", 1, &[]).await.unwrap();
    // Identical replay must first succeed, proving request equality rather
    // than accidentally testing different fixture log-directory parameters.
    original.start(&spec(), "i-1", 1, &[]).await.unwrap();
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    *server.running.lock().unwrap() = false;
    assert!(original.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    original.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn checkpoint_identity_validation_errors_are_permanent_before_start_admission() {
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::FailedPrecondition,
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Unknown,
    ] {
        let server = Server {
            identity_error: Some(code),
            ..Default::default()
        };
        let (adapter, _guard) = connect(server.clone()).await;
        let directory = tempfile::tempdir().unwrap();
        let error = adapter
            .restore(&spec(), "i-1", 1, &[], directory.path())
            .await
            .unwrap_err();
        if matches!(
            code,
            tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition
        ) {
            assert!(
                matches!(error, adx_core::Error::Invalid(_)),
                "{code:?} lost its permanent classification: {error:?}"
            );
        } else {
            assert!(
                matches!(error, adx_core::Error::Unavailable(_)),
                "{code:?} must remain retryable: {error:?}"
            );
        }
        assert!(server.starts.lock().unwrap().is_empty());
        assert!(server.operations.lock().unwrap().is_empty());
        assert_eq!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.starts_with("describe:"))
                .count(),
            1
        );
        adapter
            .retire_checkpoint_reader(&protected_reader())
            .await
            .unwrap();
        assert!(server
            .operations
            .lock()
            .unwrap()
            .values()
            .all(|operation| operation.status.retirement_complete));
    }
}
