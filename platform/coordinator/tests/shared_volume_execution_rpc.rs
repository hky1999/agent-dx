//! Real TLS/Redis/NodeRpc/JournalSink/controller integration.
//! The runtime is explicitly a control-protocol fixture, not a VM or JuiceFS mount.
mod common;
use adx_coordinator::{rpc::CoordinatorRpc, storage::Session, Placement};
use adx_core::{
    shared_volume_attachments::{SharedVolumeAttachment, SharedVolumeAttachmentRetirement},
    shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeMount, SharedVolumeSubpath},
    EnvironmentRecord, EnvironmentSpec, Error, Resources, Result,
};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use adxlet::{
    journal::JournalSink,
    rpc::{CoordinatorStateSink, NodeRpc},
    Adxlet, Readiness, Routes, RuntimeDriver, StateSink,
};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, Server, ServerTlsConfig};

fn file(name: &str) -> Vec<u8> {
    std::fs::read(PathBuf::from(std::env::var("ADX_TEST_TLS_DIR").unwrap()).join(name)).unwrap()
}
fn identity(name: &str) -> Identity {
    Identity::from_pem(file(&format!("{name}.pem")), file(&format!("{name}.key")))
}
fn tls(name: &str) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .identity(identity(name))
        .ca_certificate(Certificate::from_pem(file("ca.pem")))
        .domain_name("localhost")
}
fn server_tls(name: &str) -> ServerTlsConfig {
    ServerTlsConfig::new()
        .identity(identity(name))
        .client_ca_root(Certificate::from_pem(file("ca.pem")))
}
fn peers() -> Peers {
    Peers::new(
        [
            ("coordinator", Principal::Coordinator),
            ("node", Principal::Node("node".into())),
            ("apiserver", Principal::ApiServer),
            ("ingress", Principal::Ingress),
        ]
        .map(|(name, p)| (file(&format!("{name}.der")), p)),
    )
}
async fn channel(address: SocketAddr, role: &str) -> Channel {
    tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .tls_config(tls(role))
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap()
}
struct Backend {
    session: Session,
    running: Mutex<BTreeSet<String>>,
    stopped: Mutex<BTreeSet<String>>,
    stop_fails: AtomicBool,
}
#[async_trait::async_trait]
impl RuntimeDriver for Backend {
    async fn inventory(&self) -> Result<Vec<adxlet::RuntimeObservation>> {
        Ok(vec![])
    }
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        _generation: u64,
        _devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        if self.stopped.lock().unwrap().contains(id) {
            return Err(Error::Conflict);
        }
        for mount in &spec.execution.shared_volumes {
            let volume = self
                .session
                .get_shared_volume(&mount.volume_id, "tenant")
                .await?;
            assert!(
                volume
                    .attachments
                    .iter()
                    .any(|a| a.identity.runtime_id == id
                        && a.tenant_id == spec.tenant_id
                        && a.mount() == *mount),
                "execution crossed Start without durable occupancy"
            );
        }
        self.running.lock().unwrap().insert(id.into());
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self.running.lock().unwrap().contains(id))
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.running.lock().unwrap().remove(id);
        Ok(())
    }
    async fn retire_shared_volume_attachments(
        &self,
        attachments: &[SharedVolumeAttachment],
    ) -> Result<Vec<SharedVolumeAttachmentRetirement>> {
        if self.stop_fails.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("fixture stop unresolved".into()));
        }
        for attachment in attachments {
            self.stopped
                .lock()
                .unwrap()
                .insert(attachment.identity.runtime_id.clone());
            self.running
                .lock()
                .unwrap()
                .remove(&attachment.identity.runtime_id);
        }
        Ok(attachments
            .iter()
            .map(|attachment| SharedVolumeAttachmentRetirement {
                attachment: attachment.clone(),
                backend_receipt: format!(
                    "fixture-only:execution-stopped:{}",
                    attachment.identity.runtime_id
                ),
            })
            .collect())
    }
}
struct ReadyRoutes;
#[async_trait::async_trait]
impl Readiness for ReadyRoutes {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait::async_trait]
impl Routes for ReadyRoutes {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire_orphan(&self, _: &adxlet::RuntimeObservation) -> Result<()> {
        Ok(())
    }
}
struct Fixture {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    address: SocketAddr,
    node_address: SocketAddr,
    session: Session,
    manager: Arc<Adxlet>,
    backend: Arc<Backend>,
    journal: Arc<JournalSink>,
    _directory: tempfile::TempDir,
    _redis: common::Redis,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl Fixture {
    async fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let redis = common::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            peers(),
            tls("coordinator"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let co = tokio::spawn(async move {
            Server::builder().tls_config(server_tls("coordinator")).unwrap()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc.clone()))
                .add_service(pb::shared_volume_execution_service_server::SharedVolumeExecutionServiceServer::new(rpc))
                .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
        });
        let sink = Arc::new(
            CoordinatorStateSink::new(channel(address, "node").await, Duration::from_secs(2))
                .unwrap()
                .with_session("boot".into()),
        );
        let directory = tempfile::tempdir().unwrap();
        let journal = Arc::new(JournalSink::new(directory.path().join("journal.db"), sink));
        journal.recover(&[]).await.unwrap();
        let backend = Arc::new(Backend {
            session: session.clone(),
            running: Default::default(),
            stopped: Default::default(),
            stop_fails: AtomicBool::new(false),
        });
        let manager = Arc::new(
            Adxlet::new(
                "node".into(),
                backend.clone(),
                Arc::new(ReadyRoutes),
                Arc::new(ReadyRoutes),
                journal.clone(),
            )
            .with_shared_volume_lifecycle(true)
            .unwrap()
            .with_operation_timeout(Duration::from_secs(3))
            .unwrap(),
        );
        manager
            .update_capacity(
                Resources {
                    cpu_millis: 100,
                    memory_bytes: 100,
                    disk_bytes: 0,
                },
                Duration::from_secs(60),
            )
            .unwrap();
        manager
            .update_runtime_classes(vec!["runsc".into()])
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_address = listener.local_addr().unwrap();
        let node = NodeRpc::new(manager.clone(), peers(), "boot".into());
        let task = tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls("node"))
                .unwrap()
                .add_service(pb::node_service_server::NodeServiceServer::new(node))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let fixture = Self {
            tasks: vec![co, task],
            address,
            node_address,
            session,
            manager,
            backend,
            journal,
            _directory: directory,
            _redis: redis,
        };
        let mut client = fixture.coordinator("node").await;
        client.register_node(fixture.report(1, true)).await.unwrap();
        let catalog = client
            .inspect_node(pb::InspectNodeRequest {
                node_id: "node".into(),
                session_id: "boot".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(catalog.records.is_empty());
        fixture
            .manager
            .reconcile_retained_catalog(vec![], vec![], vec![], true)
            .await
            .unwrap();
        client
            .register_node(fixture.report(2, false))
            .await
            .unwrap();
        fixture
    }
    async fn coordinator(
        &self,
        role: &str,
    ) -> pb::coordinator_service_client::CoordinatorServiceClient<Channel> {
        pb::coordinator_service_client::CoordinatorServiceClient::new(
            channel(self.address, role).await,
        )
    }
    async fn execution(
        &self,
        role: &str,
    ) -> pb::shared_volume_execution_service_client::SharedVolumeExecutionServiceClient<Channel>
    {
        pb::shared_volume_execution_service_client::SharedVolumeExecutionServiceClient::new(
            channel(self.address, role).await,
        )
    }
    fn report(&self, sequence: u64, reconciling: bool) -> pb::RegisterNodeRequest {
        pb::RegisterNodeRequest {
            node_id: "node".into(),
            node_address: self.node_address.to_string(),
            proxy_address: "127.0.0.1:1".into(),
            capacity: Some(
                Resources {
                    cpu_millis: 100,
                    memory_bytes: 100,
                    disk_bytes: 0,
                }
                .into(),
            ),
            runtime_classes: vec!["runsc".into()],
            labels: [(
                "ADX_SHARED_VOLUME_STORAGE".into(),
                "49fc236e-3821-432c-8784-abc6b02c938e".into(),
            )]
            .into(),
            accepting_allocations: !reconciling,
            session_id: "boot".into(),
            heartbeat_sequence: sequence,
            reconciling,
            ..Default::default()
        }
    }
    async fn volume(&self) -> SharedVolumeId {
        self.session
            .create_shared_volume(
                "tenant",
                &SharedVolumeId::parse(uuid::Uuid::new_v4().to_string()).unwrap(),
            )
            .await
            .unwrap()
            .permissions
            .id()
            .clone()
    }
    async fn create(&self, id: &str, volume: SharedVolumeId) -> EnvironmentRecord {
        let spec = EnvironmentSpec {
            id: id.into(),
            tenant_id: "tenant".into(),
            image: "image".into(),
            runtime_class: "runsc".into(),
            resources: Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 0,
            },
            priority: 0,
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            execution: adx_core::execution::ExecutionOptions {
                shared_volumes: vec![SharedVolumeMount {
                    volume_id: volume,
                    subpath: SharedVolumeSubpath::default(),
                    target: "/shared".into(),
                    access: SharedVolumeAccess::ReadWrite,
                }],
                ..Default::default()
            },
        };
        let result = self
            .coordinator("apiserver")
            .await
            .create_environment(pb::CreateEnvironmentRequest {
                spec: Some(spec.into()),
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: false,
                }),
                create_timeout_seconds: 5,
                schedule_timeout_seconds: 5,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        result.record.unwrap().try_into().unwrap()
    }
    async fn list(&self) -> Vec<SharedVolumeAttachment> {
        self.execution("node")
            .await
            .list_node_shared_volume_attachments(pb::ListNodeSharedVolumeAttachmentsRequest {
                node_id: "node".into(),
                node_session_id: "boot".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .attachments
            .into_iter()
            .map(|a| a.try_into().unwrap())
            .collect()
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn real_node_and_journal_chain_reserves_before_start_and_releases_after_stop() {
    let fixture = Fixture::new().await;
    let volume = fixture.volume().await;
    let record = fixture.create("first", volume.clone()).await;
    assert_eq!(fixture.list().await.len(), 1);
    assert_eq!(fixture.journal.pending().await.unwrap(), 0);
    let handle = fixture
        .manager
        .environment(record.spec.clone(), record.assignment.clone())
        .unwrap();
    let deleted = handle.delete().await.unwrap();
    assert!(!deleted.record.resources_held);
    assert!(fixture.list().await.is_empty());
    assert!(fixture
        .backend
        .stopped
        .lock()
        .unwrap()
        .contains(&record.runtime.id));
    assert_eq!(
        fixture
            .session
            .get_shared_volume(&volume, "tenant")
            .await
            .unwrap()
            .retirements
            .len(),
        1
    );
    let next = fixture.create("next", volume).await;
    assert_eq!(fixture.list().await[0].identity.runtime_id, next.runtime.id);
    fixture
        .manager
        .environment(next.spec, next.assignment)
        .unwrap()
        .delete()
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn uncertain_stop_retains_occupancy_and_closed_reconciliation_still_permits_release() {
    let fixture = Fixture::new().await;
    let volume = fixture.volume().await;
    let record = fixture.create("first", volume).await;
    let handle = fixture
        .manager
        .environment(record.spec.clone(), record.assignment.clone())
        .unwrap();
    fixture.backend.stop_fails.store(true, Ordering::SeqCst);
    assert!(handle.delete().await.is_err());
    assert_eq!(fixture.list().await.len(), 1);
    assert!(fixture
        .backend
        .running
        .lock()
        .unwrap()
        .contains(&record.runtime.id));
    fixture
        .coordinator("node")
        .await
        .register_node(fixture.report(3, true))
        .await
        .unwrap();
    let error = fixture
        .execution("node")
        .await
        .reserve_shared_volumes(pb::ReserveSharedVolumesRequest {
            record: Some(record.try_into().unwrap()),
            node_session_id: "boot".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(fixture.list().await.len(), 1);
    fixture.backend.stop_fails.store(false, Ordering::SeqCst);
    handle.delete().await.unwrap();
    assert!(fixture.list().await.is_empty());
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn mtls_role_session_and_canonical_spec_are_enforced_before_occupancy_changes() {
    let fixture = Fixture::new().await;
    let volume = fixture.volume().await;
    let record = fixture.create("first", volume).await;
    for (role, node, session, code) in [
        ("apiserver", "node", "boot", tonic::Code::PermissionDenied),
        ("node", "other", "boot", tonic::Code::PermissionDenied),
        ("node", "node", "stale", tonic::Code::FailedPrecondition),
    ] {
        let error = fixture
            .execution(role)
            .await
            .list_node_shared_volume_attachments(pb::ListNodeSharedVolumeAttachmentsRequest {
                node_id: node.into(),
                node_session_id: session.into(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
    }
    let mut forged = record.clone();
    forged.spec.execution.shared_volumes[0].target = "/forged".into();
    assert_eq!(
        fixture
            .execution("node")
            .await
            .reserve_shared_volumes(pb::ReserveSharedVolumesRequest {
                record: Some(forged.try_into().unwrap()),
                node_session_id: "boot".into()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(fixture.list().await.len(), 1);
    fixture
        .manager
        .environment(record.spec, record.assignment)
        .unwrap()
        .delete()
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn local_degradation_journal_cannot_replace_a_cluster_volume_acknowledgement() {
    let mut fixture = Fixture::new().await;
    let volume = fixture.volume().await;
    let record = fixture.create("first", volume.clone()).await;
    // Aborting tonic's listener task does not prove that accepted connections
    // have stopped. Kill and wait for this fixture's private durable store.
    fixture._redis.crash();
    let result = fixture.journal.reserve_shared_volumes(&record).await;
    assert!(
        result.is_err(),
        "durable store is down, but reservation returned {result:?}"
    );
    assert_eq!(fixture.journal.pending().await.unwrap(), 0);
    assert!(fixture
        .backend
        .running
        .lock()
        .unwrap()
        .contains(&record.runtime.id));
    fixture._redis.start().await;
    let recovered = fixture._redis.store().await.begin(1).await.unwrap();
    let attachments = adx_core::shared_volume_attachments::attachments_for_record(&record).unwrap();
    assert_eq!(
        recovered
            .get_shared_volume(&volume, "tenant")
            .await
            .unwrap()
            .attachments,
        attachments
    );
    // The execution is a memory-only fixture, not a live process. Explicitly
    // retire it in the owned fixture before the private Redis is dropped.
    let receipts = fixture
        .backend
        .retire_shared_volume_attachments(&attachments)
        .await
        .unwrap();
    recovered
        .release_shared_volumes(&receipts, "boot")
        .await
        .unwrap();
}

fn snapshot58_template(volume: SharedVolumeId) -> EnvironmentSpec {
    EnvironmentSpec {
        id: "snapshot58-source".into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "runsc".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        priority: 0,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        execution: adx_core::execution::ExecutionOptions {
            shared_volumes: vec![SharedVolumeMount {
                volume_id: volume,
                subpath: SharedVolumeSubpath::parse("project").unwrap(),
                target: "/shared".into(),
                access: SharedVolumeAccess::ReadWrite,
            }],
            ..Default::default()
        },
    }
}

async fn snapshot58_fixture() -> (Fixture, EnvironmentSpec) {
    let fixture = Fixture::new().await;
    let template = snapshot58_template(fixture.volume().await);
    let snapshot = adx_core::snapshots::Snapshot::new(
        "snapshot58-id".into(),
        vec![],
        template.clone(),
        "node".into(),
        "snapshot58-source-1".into(),
        adx_core::CheckpointArtifact {
            storage: "shared_fs".into(),
            location: "snapshot58-fixture-artifact".into(),
            size_bytes: 1,
        },
    )
    .unwrap();
    fixture.session.publish_snapshot(snapshot).await.unwrap();
    (fixture, template)
}

async fn snapshot58_prepare(
    fixture: &Fixture,
    mut spec: EnvironmentSpec,
    name: &str,
) -> std::result::Result<pb::PreparedEnvironment, tonic::Status> {
    spec.id = name.into();
    spec.snapshot_id = Some("snapshot58-id".into());
    fixture
        .coordinator("node")
        .await
        .prepare_create(pb::LocalEnvironmentCreateRequest {
            create: Some(pb::CreateEnvironmentRequest {
                spec: Some(spec.into()),
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: false,
                }),
                create_timeout_seconds: 5,
                schedule_timeout_seconds: 5,
                ..Default::default()
            }),
            node_session_id: "boot".into(),
        })
        .await
        .map(|response| response.into_inner())
}

#[tokio::test]
#[ignore = "requires isolated real Redis and fixed task TLS fixture"]
async fn snapshot_shared_volume58_clone_inherits_default_mounts() {
    let (fixture, template) = snapshot58_fixture().await;
    let mut request: pb::EnvironmentSpec = template.clone().into();
    request.id = "snapshot58-default".into();
    request.snapshot_id = Some("snapshot58-id".into());
    request.execution = None;
    let response = fixture
        .coordinator("node")
        .await
        .prepare_create(pb::LocalEnvironmentCreateRequest {
            create: Some(pb::CreateEnvironmentRequest {
                spec: Some(request),
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: false,
                }),
                ..Default::default()
            }),
            node_session_id: "boot".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let normalized: EnvironmentSpec = response.spec.unwrap().try_into().unwrap();
    assert_eq!(
        normalized.execution.shared_volumes, template.execution.shared_volumes,
        "snapshot default clone lost shared volume mounts"
    );
    let mut empty_request = template.clone();
    empty_request.execution.shared_volumes.clear();
    let normalized: EnvironmentSpec =
        snapshot58_prepare(&fixture, empty_request, "snapshot58-empty")
            .await
            .unwrap()
            .spec
            .unwrap()
            .try_into()
            .unwrap();
    assert_eq!(
        normalized.execution.shared_volumes,
        template.execution.shared_volumes
    );
}

#[tokio::test]
#[ignore = "requires isolated real Redis and fixed task TLS fixture"]
async fn snapshot_shared_volume58_clone_rejects_explicit_mount_changes() {
    let (fixture, template) = snapshot58_fixture().await;
    snapshot58_prepare(&fixture, template.clone(), "snapshot58-identical")
        .await
        .unwrap();
    for field in ["volume", "subpath", "target", "access"] {
        let mut changed = template.clone();
        let mount = &mut changed.execution.shared_volumes[0];
        match field {
            "volume" => {
                mount.volume_id =
                    SharedVolumeId::parse("00000000-0000-4000-8000-000000000002").unwrap()
            }
            "subpath" => mount.subpath = SharedVolumeSubpath::parse("other").unwrap(),
            "target" => mount.target = "/other".into(),
            "access" => mount.access = SharedVolumeAccess::ReadOnly,
            _ => unreachable!(),
        }
        let result = snapshot58_prepare(&fixture, changed, &format!("snapshot58-{field}")).await;
        match result {
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
            Ok(_) => panic!("snapshot clone accepted explicit shared-volume {field} mismatch"),
        }
    }
}

async fn storage58_prepare_raw(
    fixture: &Fixture,
    request: pb::EnvironmentSpec,
) -> pb::EnvironmentSpec {
    fixture
        .coordinator("node")
        .await
        .prepare_create(pb::LocalEnvironmentCreateRequest {
            create: Some(pb::CreateEnvironmentRequest {
                spec: Some(request),
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: false,
                }),
                ..Default::default()
            }),
            node_session_id: "boot".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .spec
        .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated real Redis and fixed task TLS fixture"]
async fn snapshot_storage58_each_alternative_preserves_constraints_and_is_idempotent() {
    let (fixture, template) = snapshot58_fixture().await;
    let mut request: pb::EnvironmentSpec = template.into();
    request.id = "snapshot58-storage-alternatives".into();
    request.snapshot_id = Some("snapshot58-id".into());
    request.execution = None;
    let storage = pb::LabelRequirement {
        key: "ADX_SHARED_VOLUME_STORAGE".into(),
        op: pb::SelectorOp::Exists as i32,
        values: vec![],
    };
    let first = pb::LabelSelector {
        expressions: vec![
            pb::LabelRequirement {
                key: "NODE_ID".into(),
                op: pb::SelectorOp::In as i32,
                values: vec!["node".into()],
            },
            storage.clone(),
        ],
        ..Default::default()
    };
    let second = pb::LabelSelector {
        expressions: vec![
            pb::LabelRequirement {
                key: "NODE_ID".into(),
                op: pb::SelectorOp::In as i32,
                values: vec!["other-node".into()],
            },
            pb::LabelRequirement {
                key: "TEAM".into(),
                op: pb::SelectorOp::In as i32,
                values: vec!["test".into()],
            },
        ],
        ..Default::default()
    };
    let original = vec![first, second];
    request
        .scheduling
        .get_or_insert_with(Default::default)
        .required_node = original.clone();
    let normalized = storage58_prepare_raw(&fixture, request).await;
    let alternatives = &normalized.scheduling.as_ref().unwrap().required_node;
    assert_eq!(alternatives.len(), original.len());
    for (actual, before) in alternatives.iter().zip(&original) {
        assert!(before
            .expressions
            .iter()
            .all(|item| actual.expressions.contains(item)));
        assert_eq!(
            actual
                .expressions
                .iter()
                .filter(|item| **item == storage)
                .count(),
            1
        );
    }
    let again = storage58_prepare_raw(&fixture, normalized.clone()).await;
    assert_eq!(
        normalized, again,
        "normalization must not duplicate storage conditions"
    );
}

#[tokio::test]
#[ignore = "requires isolated real Redis and fixed task TLS fixture"]
async fn snapshot_storage58_default_execution_and_empty_alternative_remain_compatible() {
    let (fixture, template) = snapshot58_fixture().await;
    for absent in [true, false] {
        let mut request: pb::EnvironmentSpec = template.clone().into();
        request.id = format!("snapshot58-storage-default-{absent}");
        request.snapshot_id = Some("snapshot58-id".into());
        request.execution = if absent {
            None
        } else {
            Some(adx_core::execution::ExecutionOptions::default().into())
        };
        request.scheduling = None;
        let normalized = storage58_prepare_raw(&fixture, request).await;
        let policy = normalized.scheduling.as_ref().unwrap();
        assert_eq!(policy.required_node.len(), 1);
        assert_eq!(policy.required_node[0].expressions.len(), 1);
        let storage = &policy.required_node[0].expressions[0];
        assert_eq!(storage.key, "ADX_SHARED_VOLUME_STORAGE");
        assert_eq!(storage.op, pb::SelectorOp::Exists as i32);
        assert!(storage.values.is_empty());
        let core: EnvironmentSpec = normalized.try_into().unwrap();
        assert_eq!(
            core.execution,
            adx_core::execution::ExecutionOptions {
                shared_volumes: template.execution.shared_volumes.clone(),
                ..Default::default()
            }
        );
    }
}
