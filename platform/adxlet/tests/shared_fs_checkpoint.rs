use adx_core::lifecycle::CheckpointStorage;
use adxlet::checkpoint::{CheckpointStore, RemoteGcConfig, StorageConfig};
use std::path::{Path, PathBuf};

fn shared_store(
    shared: &std::path::Path,
    local: &std::path::Path,
) -> std::sync::Arc<dyn CheckpointStore> {
    StorageConfig::SharedFs {
        root: shared.to_path_buf(),
        local_root: local.to_path_buf(),
        volume_uuid: None,
        mount_root: None,
    }
    .build()
    .unwrap()
}

#[tokio::test]
async fn shared_policy_stages_on_shared_volume_and_publishes_immutable() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    store.validate_policy(CheckpointStorage::Shared).unwrap();
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    // Shared-policy staging must live on the shared volume, not the local root.
    assert!(staged.starts_with(shared.path()));
    std::fs::create_dir(staged.join("disk")).unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    std::fs::write(staged.join("disk/overlay"), b"saved disk").unwrap();
    let artifact = store.publish(&staged).await.unwrap();
    assert_eq!(artifact.storage, "shared_fs");
    assert_eq!(artifact.size_bytes, 22);
    assert_ne!(staged, shared.path().join(&artifact.location));
    // Publication renames staging into the immutable final directory.
    assert!(shared.path().join(&artifact.location).is_dir());
    assert!(!staged.exists());
    // Materialization serves the shared directory directly, with no download.
    let restored = store.materialize(&artifact).await.unwrap();
    assert_eq!(
        restored.as_ref() as &std::path::Path,
        shared.path().join(&artifact.location)
    );
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"saved memory"
    );
    // A reconstructed store sees the same published artifact.
    let reopened = shared_store(shared.path(), local.path());
    reopened.validate_artifact(&artifact).await.unwrap();
    // The published location is a bare single-component identifier.
    assert!(!artifact.location.contains('/'));
}

#[tokio::test]
async fn shared_publication_rejects_empty_symlink_special_and_foreign_staging() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    // Failed publications keep their staging for rollback and diagnosis;
    // each negative case removes only the offending entry.
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    assert!(store.publish(&staged).await.is_err());
    assert!(staged.exists());
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    std::os::unix::fs::symlink(staged.join("memory"), staged.join("alias")).unwrap();
    assert!(store.publish(&staged).await.is_err());
    assert!(staged.exists());
    std::fs::remove_file(staged.join("alias")).unwrap();
    let socket = std::os::unix::net::UnixListener::bind(staged.join("socket")).unwrap();
    assert!(store.publish(&staged).await.is_err());
    assert!(staged.exists());
    drop(socket);
    std::fs::remove_file(staged.join("socket")).unwrap();
    // Staging outside the shared root must not be publishable as shared.
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("memory"), b"saved memory").unwrap();
    assert!(store.publish(outside.path()).await.is_err());
}

#[tokio::test]
async fn failed_shared_publication_keeps_staging_and_publishes_nothing() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    // Break the staging so the durability walk fails: a dangling symlink.
    std::os::unix::fs::symlink("nowhere", staged.join("dangling")).unwrap();
    assert!(store.publish(&staged).await.is_err());
    // Staging bytes stay readable for the controller rollback path and later
    // diagnosis; deletion is an explicit discard decision by the caller.
    assert!(staged.join("memory").exists());
    assert!(
        shared
            .path()
            .read_dir()
            .unwrap()
            .filter(|entry| {
                !entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with('.')
            })
            .count()
            == 0,
        "no final directory may appear after a failed publication"
    );
    // After repairing the offender the same staging still serves a rollback.
    std::fs::remove_file(staged.join("dangling")).unwrap();
    let retained = store.retain_staged(&staged).await.unwrap();
    assert_eq!(retained.storage, "shared_fs");
    let restored = store.materialize(&retained).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"saved memory"
    );
    assert!(matches!(
        store.discard_staged(&staged).await,
        Err(adx_core::Error::Conflict)
    ));
    assert!(staged.join("memory").is_file());
    drop(restored);
    store.discard_staged(&staged).await.unwrap();
    assert!(!staged.exists());
}

#[tokio::test]
async fn local_policy_keeps_local_only_semantics_on_a_shared_node() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    store.validate_policy(CheckpointStorage::Local).unwrap();
    let staged = store
        .allocate_for_policy(CheckpointStorage::Local)
        .await
        .unwrap();
    assert!(staged.starts_with(local.path()));
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    let artifact = store.retain_staged(&staged).await.unwrap();
    assert_eq!(artifact.storage, "local");
    assert!(PathBuf::from(&artifact.location).starts_with(local.path()));
    let restored = store.materialize(&artifact).await.unwrap();
    assert!(restored.starts_with(local.path()));
    let _ = Path::new(&artifact.location);
}

#[tokio::test]
async fn publish_failure_fallback_retains_shared_staging_without_local_identity() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    // Mirror the controller rollback path: publish fails, staging is retained
    // as a recovery point for this node. It must never claim local-only
    // no-forced-writeback semantics while sitting on the shared volume.
    std::os::unix::fs::symlink("nowhere", staged.join("dangling")).unwrap();
    assert!(store.publish(&staged).await.is_err());
    assert!(staged.exists(), "failed publish keeps staging for rollback");
    store.discard_staged(&staged).await.unwrap();
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    let retained = store.retain_staged(&staged).await.unwrap();
    assert_eq!(retained.storage, "shared_fs");
    assert!(retained.location.starts_with(".staging-"));
    let restored = store.materialize(&retained).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"saved memory"
    );
    let _ = Path::new(&retained.location);
}

#[tokio::test]
async fn duplicate_creates_an_independent_writable_branch() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"branch source").unwrap();
    let artifact = store.publish(&staged).await.unwrap();
    let branch = store.duplicate(&artifact).await.unwrap();
    assert_eq!(branch.storage, "shared_fs");
    assert_ne!(branch.location, artifact.location);
    assert_eq!(branch.size_bytes, artifact.size_bytes);
    // Writes into the branch must not leak into the published source.
    let branch_path = store.materialize(&branch).await.unwrap();
    std::fs::write(branch_path.join("memory"), b"branch modified").unwrap();
    let source = store.materialize(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(source.join("memory")).unwrap(),
        b"branch source"
    );
    let _ = Path::new(&branch.location);
}

#[tokio::test]
async fn remove_refuses_while_a_materialized_reader_is_pinned() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    let artifact = store.publish(&staged).await.unwrap();
    let reader = store.materialize(&artifact).await.unwrap();
    assert!(matches!(
        store.remove(&artifact).await,
        Err(adx_core::Error::Conflict)
    ));
    assert!(shared.path().join(&artifact.location).is_dir());
    drop(reader);
    store.remove(&artifact).await.unwrap();
    assert!(!shared.path().join(&artifact.location).exists());
}

#[tokio::test]
async fn shared_artifacts_reject_path_traversal_and_size_drift() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    let artifact = store.publish(&staged).await.unwrap();
    for location in ["../escape", "sub/dir", ""] {
        let forged = adx_core::CheckpointArtifact {
            storage: "shared_fs".into(),
            location: location.into(),
            size_bytes: artifact.size_bytes,
        };
        assert!(store.materialize(&forged).await.is_err());
    }
    let drifted = adx_core::CheckpointArtifact {
        storage: "shared_fs".into(),
        location: artifact.location.clone(),
        size_bytes: artifact.size_bytes + 1,
    };
    assert!(store.materialize(&drifted).await.is_err());
    std::fs::write(
        shared.path().join(&artifact.location).join("memory"),
        b"tampered",
    )
    .unwrap();
    assert!(store.validate_artifact(&artifact).await.is_err());
}

#[tokio::test]
async fn shared_gc_stays_disabled_and_collects_nothing() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    let artifact = store.publish(&staged).await.unwrap();
    let mut retained = Vec::new();
    store.authorize_remote_gc(&retained).await.unwrap();
    retained.push(artifact.clone());
    let _ = retained;
    // Even with nothing retained, A0 must not reclaim shared directories.
    let collected = store.collect_remote_orphans().await.unwrap();
    assert_eq!(collected, 0);
    assert!(shared.path().join(&artifact.location).is_dir());
    // An explicitly enabled remote GC refuses to build the shared store.
    let enabled = RemoteGcConfig {
        enabled: true,
        ..RemoteGcConfig::default()
    };
    assert!(StorageConfig::SharedFs {
        root: shared.path().to_path_buf(),
        local_root: local.path().to_path_buf(),
        volume_uuid: None,
        mount_root: None,
    }
    .build_for_node("node".into(), "session".into(), enabled)
    .is_err());
}

#[tokio::test]
async fn local_recovery_points_on_a_shared_node_reconcile_their_own_root_only() {
    let shared = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store = shared_store(shared.path(), local.path());
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    let artifact = store.publish(&staged).await.unwrap();
    // Reconciliation is given no retained artifacts at all: shared directories
    // must survive regardless, because A0 never reclaims the shared volume.
    store.reconcile_local(&[]).await.unwrap();
    assert!(shared.path().join(&artifact.location).is_dir());
}

#[tokio::test]
async fn volume_uuid_binds_real_juicefs_config_shape_from_subdirectory_root() {
    let mount = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    // Real JuiceFS 1.4.x `.config` shape: top-level Meta/Format/Chunk/Security
    // sections with the volume UUID under Format.UUID, written at the mount
    // root while the checkpoint root is a nested subdirectory.
    let checkpoints = mount.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoints).unwrap();
    std::fs::write(
        mount.path().join(".config"),
        r#"{"Meta":{"URL":"redis://127.0.0.1:6379/1"},"Format":{"Name":"adx-a0","UUID":"11111111-2222-3333-4444-555555555555","BlockSize":4096},"Chunk":{"Compress":"none"},"Security":{}}"#,
    )
    .unwrap();
    // Upward discovery from the subdirectory root without mount_root.
    assert!(StorageConfig::SharedFs {
        root: checkpoints.clone(),
        local_root: local.path().to_path_buf(),
        volume_uuid: Some("11111111-2222-3333-4444-555555555555".into()),
        mount_root: None,
    }
    .build()
    .is_ok());
    // Explicit mount_root with containment also verifies.
    assert!(StorageConfig::SharedFs {
        root: checkpoints.clone(),
        local_root: local.path().to_path_buf(),
        volume_uuid: Some("11111111-2222-3333-4444-555555555555".into()),
        mount_root: Some(mount.path().to_path_buf()),
    }
    .build()
    .is_ok());
    // A foreign UUID is rejected.
    assert!(
        StorageConfig::SharedFs {
            root: checkpoints.clone(),
            local_root: local.path().to_path_buf(),
            volume_uuid: Some("99999999-9999-9999-9999-999999999999".into()),
            mount_root: Some(mount.path().to_path_buf()),
        }
        .build()
        .is_err(),
        "a foreign or stale mount must be rejected"
    );
    // A root outside the declared mount root is rejected.
    let elsewhere = tempfile::tempdir().unwrap();
    assert!(StorageConfig::SharedFs {
        root: elsewhere.path().to_path_buf(),
        local_root: local.path().to_path_buf(),
        volume_uuid: Some("11111111-2222-3333-4444-555555555555".into()),
        mount_root: Some(mount.path().to_path_buf()),
    }
    .build()
    .is_err());
}

#[tokio::test]
async fn volume_uuid_rejects_missing_identity_and_legacy_flat_shape() {
    let mount = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let checkpoints = mount.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoints).unwrap();
    // No `.config` anywhere above the checkpoint root: refuse to serve.
    assert!(StorageConfig::SharedFs {
        root: checkpoints.clone(),
        local_root: local.path().to_path_buf(),
        volume_uuid: Some("11111111-2222-3333-4444-555555555555".into()),
        mount_root: None,
    }
    .build()
    .is_err());
    // A flat `{"uuid": ...}` document is not the real JuiceFS shape and
    // must not pass identity verification.
    std::fs::write(
        mount.path().join(".config"),
        r#"{"uuid": "11111111-2222-3333-4444-555555555555"}"#,
    )
    .unwrap();
    assert!(
        StorageConfig::SharedFs {
            root: checkpoints.clone(),
            local_root: local.path().to_path_buf(),
            volume_uuid: Some("11111111-2222-3333-4444-555555555555".into()),
            mount_root: None,
        }
        .build()
        .is_err(),
        "only the real Format.UUID shape is accepted"
    );
    // A real-shaped document whose Format section lacks UUID is rejected.
    std::fs::write(
        mount.path().join(".config"),
        r#"{"Meta":{},"Format":{"Name":"adx-a0"}}"#,
    )
    .unwrap();
    assert!(StorageConfig::SharedFs {
        root: checkpoints,
        local_root: local.path().to_path_buf(),
        volume_uuid: Some("11111111-2222-3333-4444-555555555555".into()),
        mount_root: None,
    }
    .build()
    .is_err());
}
