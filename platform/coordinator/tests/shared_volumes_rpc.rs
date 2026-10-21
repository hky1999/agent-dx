mod common;
use adx_coordinator::{rpc::CoordinatorRpc, storage::Session, Placement};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, Server, ServerTlsConfig};

// Reuse the control-rpc suite's generated CA/certificates and common owned Redis.
fn file(name: &str) -> Vec<u8> {
    std::fs::read(
        PathBuf::from(
            std::env::var("ADX_TEST_TLS_DIR")
                .expect("owned control-rpc test TLS directory required"),
        )
        .join(name),
    )
    .unwrap()
}
fn identity(name: &str) -> Identity {
    Identity::from_pem(file(&format!("{name}.pem")), file(&format!("{name}.key")))
}
fn tls(name: &str) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(file("ca.pem")))
        .identity(identity(name))
        .domain_name("localhost")
}
type Client = pb::shared_volume_service_client::SharedVolumeServiceClient<Channel>;
struct Fixture {
    task: tokio::task::JoinHandle<()>,
    address: SocketAddr,
    session: Session,
    _redis: common::Redis,
}
impl Fixture {
    async fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let redis = common::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let peers = Peers::new(
            [
                ("coordinator", Principal::Coordinator),
                ("apiserver", Principal::ApiServer),
                ("node", Principal::Node("node".into())),
                ("ingress", Principal::Ingress),
            ]
            .map(|(name, principal)| (file(&format!("{name}.der")), principal)),
        );
        let rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            peers,
            tls("coordinator"),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_tls = ServerTlsConfig::new()
            .identity(identity("coordinator"))
            .client_ca_root(Certificate::from_pem(file("ca.pem")));
        let task = tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls)
                .unwrap()
                .add_service(pb::shared_volume_service_server::SharedVolumeServiceServer::new(rpc))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            task,
            address,
            session,
            _redis: redis,
        }
    }
    async fn client(&self, role: &str) -> Client {
        let channel = tonic::transport::Endpoint::from_shared(format!("https://{}", self.address))
            .unwrap()
            .tls_config(tls(role))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .unwrap();
        Client::new(channel)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn actor(tenant: &str) -> Option<pb::CallerContext> {
    Some(pb::CallerContext {
        tenant_id: tenant.into(),
        administrator: false,
    })
}
fn create(tenant: &str, request_id: &str) -> pb::CreateSharedVolumeRequest {
    pb::CreateSharedVolumeRequest {
        request_id: request_id.into(),
        caller: actor(tenant),
    }
}
fn get(id: &str, tenant: &str) -> pb::GetSharedVolumeRequest {
    pb::GetSharedVolumeRequest {
        id: id.into(),
        caller: actor(tenant),
    }
}
fn grant(id: &str, revision: u64) -> pb::GrantSharedVolumeRequest {
    pb::GrantSharedVolumeRequest {
        id: id.into(),
        caller: actor("owner"),
        expected_permission_revision: revision,
        grant: Some(pb::SharedVolumeGrant {
            tenant_id: "reader".into(),
            subpath: "project".into(),
            access: pb::SharedVolumeAccess::ReadOnly as i32,
        }),
    }
}
fn request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn only_the_mtls_frontend_may_manage_volumes() {
    let fixture = Fixture::new().await;
    for role in ["node", "ingress", "coordinator"] {
        let error = fixture
            .client(role)
            .await
            .create_shared_volume(create("owner", &request_id()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    let mut client = fixture.client("apiserver").await;
    assert_eq!(
        client
            .create_shared_volume(create("owner", &request_id()))
            .await
            .unwrap()
            .into_inner()
            .owner_tenant_id,
        "owner"
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn caller_context_and_canonical_request_identity_are_mandatory() {
    let fixture = Fixture::new().await;
    let mut client = fixture.client("apiserver").await;
    let mut request = create("owner", &request_id());
    request.caller = None;
    assert_eq!(
        client
            .create_shared_volume(request)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        client
            .create_shared_volume(create("owner", "../host"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        client
            .create_shared_volume(create("", "00000000-0000-0000-0000-000000000001"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn concurrent_replay_is_scoped_to_the_verified_tenant() {
    let fixture = Fixture::new().await;
    let mut a = fixture.client("apiserver").await;
    let mut b = a.clone();
    let id = request_id();
    let (a, b) = tokio::join!(
        a.create_shared_volume(create("owner", &id)),
        b.create_shared_volume(create("owner", &id))
    );
    let a = a.unwrap().into_inner();
    assert_eq!(a, b.unwrap().into_inner());
    let other = fixture
        .client("apiserver")
        .await
        .create_shared_volume(create("other", &id))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(a.id, other.id);
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn an_administrator_bit_is_not_owner_impersonation() {
    let fixture = Fixture::new().await;
    let mut client = fixture.client("apiserver").await;
    let volume = client
        .create_shared_volume(create("owner", &request_id()))
        .await
        .unwrap()
        .into_inner();
    let mut request = get(&volume.id, "other");
    request.caller.as_mut().unwrap().administrator = true;
    assert_eq!(
        client.get_shared_volume(request).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .get_shared_volume(get(&volume.id, "owner"))
            .await
            .unwrap()
            .into_inner(),
        volume
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn grant_and_revoke_preserve_conditional_revision_semantics() {
    let fixture = Fixture::new().await;
    let mut client = fixture.client("apiserver").await;
    let volume = client
        .create_shared_volume(create("owner", &request_id()))
        .await
        .unwrap()
        .into_inner();
    let granted = client
        .grant_shared_volume(grant(&volume.id, 1))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(granted.permission_revision, 2);
    assert_eq!(granted.grants.len(), 1);
    assert_eq!(
        client
            .grant_shared_volume(grant(&volume.id, 2))
            .await
            .unwrap()
            .into_inner(),
        granted
    );
    let revoked = client
        .revoke_shared_volume(pb::RevokeSharedVolumeRequest {
            id: volume.id.clone(),
            caller: actor("owner"),
            expected_permission_revision: 2,
            tenant_id: "reader".into(),
            subpath: "project".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(revoked.permission_revision, 3);
    assert!(revoked.grants.is_empty());
    assert_eq!(
        client
            .grant_shared_volume(grant(&volume.id, 2))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        client
            .get_shared_volume(get(&volume.id, "owner"))
            .await
            .unwrap()
            .into_inner(),
        revoked
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn missing_mode_or_zero_revision_does_not_create_a_grant() {
    let fixture = Fixture::new().await;
    let mut client = fixture.client("apiserver").await;
    let volume = client
        .create_shared_volume(create("owner", &request_id()))
        .await
        .unwrap()
        .into_inner();
    for (revision, access) in [(0, 1), (1, 0), (1, 99)] {
        let mut request = grant(&volume.id, revision);
        request.grant.as_mut().unwrap().access = access;
        assert_eq!(
            client
                .grant_shared_volume(request)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(
        client
            .get_shared_volume(get(&volume.id, "owner"))
            .await
            .unwrap()
            .into_inner(),
        volume
    );
}
#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn a_new_epoch_fences_an_old_live_rpc_server_without_losing_permissions() {
    let fixture = Fixture::new().await;
    let mut client = fixture.client("apiserver").await;
    let volume = client
        .create_shared_volume(create("owner", &request_id()))
        .await
        .unwrap()
        .into_inner();
    let db = fixture._redis.store().await;
    let replacement = db.begin(1).await.unwrap();
    assert_eq!(
        client
            .grant_shared_volume(grant(&volume.id, 1))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let id = adx_core::shared_volumes::SharedVolumeId::parse(&volume.id).unwrap();
    let persisted = replacement.get_shared_volume(&id, "owner").await.unwrap();
    assert_eq!(persisted.permissions.revision(), 1);
    assert!(persisted.permissions.grants().is_empty());
    assert_ne!(fixture.session.epoch(), replacement.epoch());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and control-rpc TLS"]
async fn a_crashed_store_cannot_acknowledge_volume_management_operations() {
    let mut fixture = Fixture::new().await;
    let mut client = fixture.client("apiserver").await;
    let volume = client
        .create_shared_volume(create("owner", &request_id()))
        .await
        .unwrap()
        .into_inner();
    let granted = client
        .grant_shared_volume(grant(&volume.id, 1))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(granted.permission_revision, 2);
    fixture._redis.crash();
    assert_eq!(
        client
            .get_shared_volume(get(&volume.id, "owner"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        client
            .create_shared_volume(create("owner", &request_id()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let mut changed = grant(&volume.id, 2);
    changed.grant.as_mut().unwrap().access = pb::SharedVolumeAccess::ReadWrite as i32;
    assert_eq!(
        client
            .grant_shared_volume(changed)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        client
            .revoke_shared_volume(pb::RevokeSharedVolumeRequest {
                id: volume.id,
                caller: actor("owner"),
                expected_permission_revision: 2,
                tenant_id: "reader".into(),
                subpath: "project".into(),
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
}
