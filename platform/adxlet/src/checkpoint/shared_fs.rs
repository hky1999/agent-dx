//! Shared-filesystem checkpoint store (JuiceFS-backed, A0).
//!
//! Configured and shared-policy captures stage directly on the mounted shared volume; the
//! runtime writes checkpoint files there and publication performs an explicit
//! durability barrier (per-file fsync, directory fsync, atomic rename into the
//! immutable final directory, root fsync) before acknowledging. Local-policy
//! recovery points keep their own node-local root and the existing no-forced-
//! writeback contract; a path on the shared volume is never presented as a
//! local artifact. Restores read the shared directory directly — there is no
//! download or cache — and hold a pin that blocks removal while the backend
//! may still be reading. Publication validation is metadata-level (symlink
//! and special-file rejection plus a byte-count check); there is no per-file
//! content digest. Reclamation of shared directories is disabled in A0
//! (see `collect_remote_orphans`). Stable filesystem leases serialize readers
//! and removal across clients; their registered inode is checked so a missing
//! or replaced lock cannot silently split ownership. Registration happens only
//! when a fresh staging directory is allocated, never while reading/deleting.
//! Unregistered historical artifacts are retained and fail closed until an
//! explicit migration; locks alone do not prove process-restart safety, so
//! reclamation still requires A1 durable-reference/failure acceptance.
//! `duplicate` builds an
//! independent writable branch with a byte-exact `copy_file_range` clone
//! per file (independent offsets, bounded requests, full-length check),
//! since FICLONE is unavailable on JuiceFS.
use super::{io_error, CheckpointStore, LocalCheckpointStore, MaterializedCheckpoint};
use adx_core::{CheckpointArtifact, Error, Result};
use async_trait::async_trait;
mod leases;
use leases::SharedFsLeases;
use std::{
    io::Write,
    os::unix::fs::PermissionsExt,
    os::unix::io::AsRawFd,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

pub(crate) const SHARED_FS_ALIAS: &str = "shared_fs";
const STAGING_PREFIX: &str = ".staging-";

pub struct SharedFsCheckpointStore {
    root: PathBuf,
    local: LocalCheckpointStore,
    leases: Arc<SharedFsLeases>,
}
/// Deployment-profile volume identity. When set, the store refuses to build
/// unless the mounted volume reports the same UUID, so a stale or foreign
/// mount cannot silently serve checkpoint traffic. JuiceFS writes the volume
/// UUID into the hidden `.config` JSON file at the **mount root** under the
/// real-shape key `Format.UUID` (the document also carries `Meta`, `Chunk`
/// and `Security` sections). A checkpoint `root` typically sits in a
/// subdirectory of the mount, so the identity file is located either at an
/// explicitly configured `mount_root` (preferred: deterministic and
/// containment-checked) or by walking up from `root` to the nearest
/// directory that carries `.config`.
pub const VOLUME_CONFIG: &str = ".config";
fn volume_uuid_in_config(config: &Path) -> Result<Option<String>> {
    let data = match std::fs::read_to_string(config) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    let parsed: serde_json::Value = serde_json::from_str(&data)
        .map_err(|e| invalid(format!("unreadable volume config: {e}")))?;
    match parsed
        .get("Format")
        .and_then(|format| format.get("UUID"))
        .and_then(|uuid| uuid.as_str())
    {
        Some(uuid) => Ok(Some(uuid.to_owned())),
        None => Err(invalid(
            "volume config has no Format.UUID field (real JuiceFS shape)",
        )),
    }
}
fn volume_uuid_at(root: &Path) -> Result<Option<String>> {
    volume_uuid_in_config(&root.join(VOLUME_CONFIG))
}
fn invalid(message: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("checkpoint shared_fs: {message}"))
}
impl SharedFsCheckpointStore {
    pub fn new(root: PathBuf, local_root: PathBuf) -> Result<Self> {
        Self::with_volume(root, local_root, None)
    }
    pub fn with_volume(
        root: PathBuf,
        local_root: PathBuf,
        volume_uuid: Option<&str>,
    ) -> Result<Self> {
        Self::with_mount(root, local_root, volume_uuid, None)
    }
    pub fn with_mount(
        root: PathBuf,
        local_root: PathBuf,
        volume_uuid: Option<&str>,
        mount_root: Option<&Path>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&root).map_err(io_error)?;
        let root = std::fs::canonicalize(&root).map_err(io_error)?;
        if !root.is_dir() {
            return Err(invalid("shared root is not a directory"));
        }
        if let Some(expected) = volume_uuid {
            let mount = match mount_root {
                Some(explicit) => {
                    let explicit = std::fs::canonicalize(explicit).map_err(io_error)?;
                    if !explicit.is_dir() {
                        return Err(invalid("mount_root is not a directory"));
                    }
                    if !root.starts_with(&explicit) {
                        return Err(invalid(
                            "shared root is not inside the configured mount_root",
                        ));
                    }
                    Some(explicit)
                }
                // Without an explicit mount root, walk up from the checkpoint
                // root to the nearest directory carrying the JuiceFS volume
                // config. The first `.config` found wins; a UUID mismatch
                // still rejects, so a stray file cannot impersonate the volume.
                None => {
                    let mut found = None;
                    let mut current = Some(root.as_path());
                    while let Some(dir) = current {
                        if dir.join(VOLUME_CONFIG).exists() {
                            found = Some(dir.to_path_buf());
                            break;
                        }
                        current = dir.parent();
                    }
                    found
                }
            };
            let reported = match mount {
                Some(mount) => volume_uuid_at(&mount)?
                    .ok_or_else(|| invalid("mount root has no readable volume identity"))?,
                None => {
                    return Err(invalid(
                        "no volume config found between shared root and filesystem root",
                    ))
                }
            };
            if reported != expected {
                return Err(invalid(format!(
                    "shared volume identity mismatch: expected {expected}, mounted {reported}"
                )));
            }
        }
        let leases = Arc::new(SharedFsLeases::new(&root)?);
        Ok(Self {
            local: LocalCheckpointStore::new(local_root)?,
            root,
            leases,
        })
    }
    fn shared_staging_path(&self) -> PathBuf {
        self.root
            .join(format!("{STAGING_PREFIX}{}", uuid::Uuid::new_v4()))
    }
    async fn shared_staging(&self) -> Result<PathBuf> {
        let path = self.shared_staging_path();
        let leases = self.leases.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir(&path).map_err(io_error)?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .map_err(io_error)?;
            let location = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| invalid("non UTF-8 staging identity"))?;
            leases.initialize(location)?;
            Ok(path)
        })
        .await
        .map_err(io_error)?
    }
    async fn lease(&self, location: &str, exclusive: bool) -> Result<Arc<std::fs::File>> {
        let leases = self.leases.clone();
        let location = location.to_owned();
        tokio::task::spawn_blocking(move || leases.lock(&location, exclusive))
            .await
            .map_err(io_error)?
    }
    async fn protected_io<G: Send + 'static, T: Send + 'static>(
        guard: G,
        work: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<(T, G)> {
        // The blocking job can outlive its cancelled async caller. Move the
        // protection into that job and return it only after I/O has finished.
        tokio::task::spawn_blocking(move || work().map(|value| (value, guard)))
            .await
            .map_err(io_error)?
    }
    fn under_shared(&self, path: &Path) -> Result<PathBuf> {
        let canonical = std::fs::canonicalize(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound
            } else {
                io_error(error)
            }
        })?;
        if canonical.parent() != Some(self.root.as_path()) {
            return Err(invalid("path outside shared storage root"));
        }
        Ok(canonical)
    }
    fn location_of(&self, path: &Path) -> Result<String> {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("non UTF-8 shared checkpoint name"))?;
        if name.starts_with('.') && !name.starts_with(STAGING_PREFIX) {
            return Err(invalid("reserved shared checkpoint name"));
        }
        Ok(name.to_owned())
    }
    fn shared_artifact(&self, path: &Path, size: u64) -> Result<CheckpointArtifact> {
        Ok(CheckpointArtifact {
            storage: SHARED_FS_ALIAS.into(),
            location: self.location_of(path)?,
            size_bytes: size,
        })
    }
    fn barrier(path: &Path) -> Result<()> {
        let meta = std::fs::symlink_metadata(path).map_err(io_error)?;
        if meta.is_symlink() {
            return Err(invalid("symlink in shared checkpoint"));
        }
        if meta.is_file() {
            let file = std::fs::File::open(path).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            return Ok(());
        }
        if !meta.is_dir() {
            return Err(invalid("special file in shared checkpoint"));
        }
        for entry in std::fs::read_dir(path).map_err(io_error)? {
            Self::barrier(&entry.map_err(io_error)?.path())?;
        }
        let dir = std::fs::File::open(path).map_err(io_error)?;
        dir.sync_all().map_err(io_error)?;
        Ok(())
    }
    fn fsync_root(&self) -> Result<()> {
        let dir = std::fs::File::open(&self.root).map_err(io_error)?;
        dir.sync_all().map_err(io_error)
    }
    fn clone_file(source: &Path, target: &Path) -> Result<()> {
        // The kernel clamps each request to its own MAX_RW_COUNT, so large
        // files exercise multiple rounds; correctness is covered by the
        // capped variant's regression tests below.
        Self::clone_file_capped(source, target, usize::MAX)
    }
    /// Byte-exact `copy_file_range` clone with independent input/output
    /// offsets. Each request is bounded by the remaining length (and `chunk`,
    /// which the kernel further clamps to its own per-call maximum), a zero
    /// return before reaching the expected end is an error rather than
    /// silence, and the final offsets must account for the complete length.
    fn clone_file_capped(source: &Path, target: &Path, chunk: usize) -> Result<()> {
        if chunk == 0 {
            return Err(invalid("clone chunk size must be positive"));
        }
        let input = std::fs::File::open(source).map_err(io_error)?;
        let mut output = std::fs::File::create(target).map_err(io_error)?;
        let stat = input.metadata().map_err(io_error)?;
        let mode = stat.permissions().mode();
        let total = stat.len();
        let mut in_offset: libc::off_t = 0;
        let mut out_offset: libc::off_t = 0;
        while (out_offset as u64) < total {
            let remaining = total - out_offset as u64;
            let len = remaining.min(chunk as u64).min(libc::off_t::MAX as u64) as usize;
            // SAFETY: copy_file_range reads from and writes to the file
            // descriptors owned by `input`/`output`; both remain valid for
            // the call duration and the offset pointers are distinct locals.
            let written = unsafe {
                libc::copy_file_range(
                    input.as_raw_fd(),
                    &mut in_offset,
                    output.as_raw_fd(),
                    &mut out_offset,
                    len,
                    0,
                )
            };
            if written < 0 {
                let error = std::io::Error::last_os_error();
                return Err(io_error(error));
            }
            if written == 0 {
                return Err(invalid(
                    "clone source ended before its recorded length (no progress at EOF)",
                ));
            }
        }
        if in_offset as u64 != total || out_offset as u64 != total {
            return Err(invalid(format!(
                "clone length mismatch: copied {} of {} bytes",
                out_offset, total
            )));
        }
        output.flush().map_err(io_error)?;
        output.sync_all().map_err(io_error)?;
        std::fs::set_permissions(target, std::fs::Permissions::from_mode(mode & 0o777))
            .map_err(io_error)?;
        Ok(())
    }
    fn clone_tree(source: &Path, target: &Path) -> Result<()> {
        std::fs::create_dir(target).map_err(io_error)?;
        std::fs::set_permissions(target, std::fs::Permissions::from_mode(0o700))
            .map_err(io_error)?;
        Self::clone_contents(source, target)
    }
    fn clone_contents(source: &Path, target: &Path) -> Result<()> {
        for entry in std::fs::read_dir(source).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let destination = target.join(entry.file_name());
            let kind = entry.file_type().map_err(io_error)?;
            if kind.is_dir() {
                Self::clone_tree(&entry.path(), &destination)?;
            } else if kind.is_file() {
                Self::clone_file(&entry.path(), &destination)?;
            } else {
                return Err(invalid("symlink or special file in shared checkpoint"));
            }
        }
        Ok(())
    }
}
#[async_trait]
impl CheckpointStore for SharedFsCheckpointStore {
    fn storage_alias(&self) -> &str {
        SHARED_FS_ALIAS
    }
    async fn allocate(&self) -> Result<PathBuf> {
        // Callers that have not been migrated to policy-aware allocation keep
        // the historical behavior of local staging.
        self.local.allocate().await
    }
    async fn allocate_for_policy(
        &self,
        policy: adx_core::lifecycle::CheckpointStorage,
    ) -> Result<PathBuf> {
        use adx_core::lifecycle::CheckpointStorage;
        match policy {
            CheckpointStorage::Configured | CheckpointStorage::Shared => {
                let staging = self.shared_staging().await?;
                Ok(staging)
            }
            CheckpointStorage::Local => self.local.allocate().await,
        }
    }
    async fn discard_staged(&self, staged: &Path) -> Result<()> {
        if staged.starts_with(&self.root) {
            let lease = self.lease(&self.location_of(staged)?, true).await?;
            let owned = self.under_shared(staged)?;
            tokio::task::spawn_blocking(move || {
                let _lease = lease;
                std::fs::remove_dir_all(owned)
            })
            .await
            .map_err(io_error)?
            .map_err(io_error)
        } else {
            self.local.discard_staged(staged).await
        }
    }
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact> {
        if staged.starts_with(&self.root) {
            let lease = self.lease(&self.location_of(staged)?, true).await?;
            // Fallback recovery point on the shared volume: complete bytes for
            // a same-node rollback, but the publication barrier did not finish.
            // It is recorded under its staging name and never claims local-only
            // no-forced-writeback semantics.
            let owned = self.under_shared(staged)?;
            let copy = owned.clone();
            let (size, _lease) =
                Self::protected_io(lease.clone(), move || super::scan(&copy)).await?;
            if size == 0 {
                return Err(invalid("empty shared checkpoint"));
            }
            let result = self.shared_artifact(&owned, size);
            drop(lease);
            return result;
        }
        self.local.retain_staged(staged).await
    }
    async fn publish(&self, staged: &Path) -> Result<CheckpointArtifact> {
        if !staged.starts_with(&self.root) {
            return Err(invalid("shared publication requires shared staging"));
        }
        let owned = self.under_shared(staged)?;
        let name = self.location_of(&owned)?;
        if !name.starts_with(STAGING_PREFIX) {
            return Err(invalid("shared publication requires a staging directory"));
        }
        let lease = self.lease(&name, true).await?;
        let result = async {
            let barrier_root = owned.clone();
            Self::protected_io(lease.clone(), move || Self::barrier(&barrier_root)).await?;
            let size = super::scan(&owned)?;
            if size == 0 {
                return Err(invalid("empty shared checkpoint"));
            }
            let final_name = name.strip_prefix(STAGING_PREFIX).unwrap_or(&name);
            if uuid::Uuid::parse_str(final_name).is_err() {
                return Err(invalid("shared staging identifier is not a UUID"));
            }
            let destination = self.root.join(final_name);
            if destination.try_exists().map_err(io_error)? {
                return Err(invalid("immutable shared checkpoint already exists"));
            }
            std::fs::rename(&owned, &destination).map_err(io_error)?;
            if let Err(error) = self.fsync_root() {
                // The rename already happened; surface the durability failure
                // instead of acknowledging a checkpoint we cannot persist.
                let _ = std::fs::rename(&destination, &owned);
                return Err(io_error(error));
            }
            self.shared_artifact(&destination, size)
        }
        .await;
        // A failed publication deliberately leaves staging in place: the
        // controller rollback path may still read it (retain_staged) and the
        // preserved directory keeps a diagnosable identity. Deletion is an
        // explicit discard_staged decision by the caller.
        drop(lease);
        result
    }
    async fn materialize(&self, artifact: &CheckpointArtifact) -> Result<MaterializedCheckpoint> {
        if artifact.storage == "local" {
            return self.local.materialize(artifact).await;
        }
        if artifact.storage != SHARED_FS_ALIAS || artifact.size_bytes == 0 {
            return Err(invalid("unsupported checkpoint storage"));
        }
        if Path::new(&artifact.location).components().count() != 1
            || !matches!(
                Path::new(&artifact.location).components().next(),
                Some(Component::Normal(_))
            )
        {
            return Err(invalid(
                "shared checkpoint location must be a single path component",
            ));
        }
        let pin = self.lease(&artifact.location, false).await?;
        let owned = self.under_shared(&self.root.join(&artifact.location))?;
        let copy = owned.clone();
        let (size, pin) = Self::protected_io(pin, move || super::scan(&copy)).await?;
        if size != artifact.size_bytes {
            return Err(invalid("shared checkpoint artifact size changed"));
        }
        Ok(MaterializedCheckpoint::pinned(owned, pin))
    }
    async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            return self.local.remove(artifact).await;
        }
        if artifact.storage != SHARED_FS_ALIAS {
            return Err(invalid("unsupported checkpoint storage"));
        }
        let leases = self.leases.clone();
        let location = artifact.location.clone();
        tokio::task::spawn_blocking(move || leases.remove(&location))
            .await
            .map_err(io_error)?
    }
    async fn duplicate(&self, artifact: &CheckpointArtifact) -> Result<CheckpointArtifact> {
        if artifact.storage == "local" {
            return self.local.duplicate(artifact).await;
        }
        if artifact.storage != SHARED_FS_ALIAS {
            return Err(invalid("unsupported checkpoint storage"));
        }
        let source = self.materialize(artifact).await?;
        let staged = self.shared_staging().await?;
        let result = async {
            let from = source.to_path_buf();
            let to = staged.clone();
            let ((), source) =
                Self::protected_io(source, move || Self::clone_contents(&from, &to)).await?;
            let published = self.publish(&staged).await;
            drop(source);
            published
        }
        .await;
        if result.is_err() {
            let staging = staged.clone();
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(staging)).await;
        }
        result
    }
    async fn committed(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            self.local.committed(artifact).await
        } else {
            // Shared publication has no separate upload staging to reclaim.
            Ok(())
        }
    }
    async fn collect_remote_orphans(&self) -> Result<usize> {
        // A0 keeps reclamation of the shared volume disabled: reader
        // protection and restart reconciliation arrive in A1.
        Ok(0)
    }
    async fn reconcile_local(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        // Only the node-private root is swept; shared directories, including
        // abandoned staging, are never reclaimed automatically in A0.
        self.local.reconcile_local(retained).await
    }
}

#[cfg(test)]
mod reader_lifecycle_tests {
    use super::*;
    use adx_core::lifecycle::CheckpointStorage;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("adx-shared-reader-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&root).unwrap();
            Self(root)
        }

        fn store(&self, name: &str) -> SharedFsCheckpointStore {
            SharedFsCheckpointStore::new(self.0.join("shared"), self.0.join(name)).unwrap()
        }

        async fn publish(&self) -> CheckpointArtifact {
            let publisher = self.store("publisher");
            let staged = publisher
                .allocate_for_policy(CheckpointStorage::Shared)
                .await
                .unwrap();
            std::fs::write(staged.join("memory"), b"live runtime checkpoint dependency").unwrap();
            publisher.publish(&staged).await.unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn independent_readers_block_removal_until_last_guard_is_released() {
        let fixture = Fixture::new();
        let artifact = fixture.publish().await;
        let reader_a = fixture.store("reader-a");
        let reader_b = fixture.store("reader-b");
        let collector = fixture.store("collector");
        let first = reader_a.materialize(&artifact).await.unwrap();
        let second = reader_b.materialize(&artifact).await.unwrap();

        assert!(
            matches!(collector.remove(&artifact).await, Err(Error::Conflict)),
            "a different store must not remove a live reader's files"
        );
        drop(first);
        assert!(
            matches!(collector.remove(&artifact).await, Err(Error::Conflict)),
            "the remaining independent reader must still block deletion"
        );
        assert_eq!(
            std::fs::read(second.join("memory")).unwrap(),
            b"live runtime checkpoint dependency"
        );
        drop(second);
        collector.remove(&artifact).await.unwrap();
        assert!(!fixture.0.join("shared").join(&artifact.location).exists());
    }

    #[tokio::test]
    async fn cancelled_async_caller_keeps_lease_until_blocking_io_finishes() {
        let fixture = Fixture::new();
        let artifact = fixture.publish().await;
        let reader = fixture.store("cancelled-reader");
        let collector = fixture.store("collector");
        let pin = reader.lease(&artifact.location, false).await.unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            SharedFsCheckpointStore::protected_io(pin, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
        });
        entered_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let removal_while_work_is_blocked = collector.remove(&artifact).await;
        // Always release before asserting so a RED failure cannot strand the
        // runtime shutdown behind this task's own blocking worker.
        release_tx.send(()).unwrap();
        assert!(
            matches!(removal_while_work_is_blocked, Err(Error::Conflict)),
            "cancelled async caller released the lease before blocking I/O finished"
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match collector.remove(&artifact).await {
                Ok(()) => break,
                Err(Error::Conflict) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "finished I/O kept its lease"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => panic!("unexpected removal error: {error}"),
            }
        }
    }

    #[tokio::test]
    async fn exclusive_lease_rejects_reads_and_clones_before_size_validation() {
        let fixture = Fixture::new();
        let artifact = fixture.publish().await;
        let reader = fixture.store("reader");
        let collector = fixture.store("collector");
        let claim = collector.lease(&artifact.location, true).await.unwrap();
        let mut mismatched = artifact.clone();
        mismatched.size_bytes += 1;
        assert!(matches!(
            reader.materialize(&mismatched).await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            reader.duplicate(&mismatched).await,
            Err(Error::Conflict)
        ));
        drop(claim);
        assert!(matches!(
            reader.materialize(&mismatched).await,
            Err(Error::Invalid(_))
        ));
        collector.remove(&artifact).await.unwrap();
    }

    #[tokio::test]
    async fn cloned_artifact_has_an_independent_registered_read_lease() {
        let fixture = Fixture::new();
        let artifact = fixture.publish().await;
        let reader = fixture.store("reader");
        let collector = fixture.store("collector");
        let cloned = reader.duplicate(&artifact).await.unwrap();
        let guard = reader.materialize(&cloned).await.unwrap();
        collector.remove(&artifact).await.unwrap();
        assert_eq!(
            std::fs::read(guard.join("memory")).unwrap(),
            b"live runtime checkpoint dependency"
        );
        assert!(matches!(
            collector.remove(&cloned).await,
            Err(Error::Conflict)
        ));
        drop(guard);
        collector.remove(&cloned).await.unwrap();
    }

    #[tokio::test]
    async fn artifact_removal_can_repeat_after_a_lost_acknowledgement() {
        let fixture = Fixture::new();
        let artifact = fixture.publish().await;
        fixture
            .store("collector-a")
            .remove(&artifact)
            .await
            .unwrap();
        assert!(
            fixture.store("collector-b").remove(&artifact).await.is_ok(),
            "the authoritative collector must be able to repeat a completed deletion"
        );
    }
}

#[cfg(test)]
mod clone_tests {
    use super::*;

    fn deterministic(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        let mut data = Vec::with_capacity(len);
        while data.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            data.extend_from_slice(&state.to_le_bytes());
        }
        data.truncate(len);
        data
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("adx-clone-{}-{}", name, uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn configured_shared_and_local_staging_match_publication_policy() {
        use adx_core::lifecycle::CheckpointStorage;

        let dir = scratch("policies");
        let shared = dir.join("shared");
        let local = dir.join("local");
        let store = SharedFsCheckpointStore::new(shared.clone(), local.clone())
            .expect("create policy fixture");
        for (policy, root, alias) in [
            (CheckpointStorage::Configured, &shared, "shared_fs"),
            (CheckpointStorage::Shared, &shared, "shared_fs"),
            (CheckpointStorage::Local, &local, "local"),
        ] {
            let staged = store
                .allocate_for_policy(policy)
                .await
                .expect("allocate policy staging");
            assert_eq!(
                staged.parent(),
                Some(root.as_path()),
                "staging root must match {policy:?}"
            );
            std::fs::write(staged.join("state.bin"), b"policy payload")
                .expect("write policy fixture");
            let artifact = store
                .publish_with_policy(&staged, policy)
                .await
                .expect("publish must agree with allocation policy");
            assert_eq!(artifact.storage, alias);
            let published = if alias == "local" {
                PathBuf::from(&artifact.location)
            } else {
                shared.join(&artifact.location)
            };
            assert_eq!(
                std::fs::read(published.join("state.bin")).expect("read published policy fixture"),
                b"policy payload".to_vec()
            );
        }
        std::fs::remove_dir_all(dir).expect("remove policy fixture");
    }

    #[test]
    fn multi_round_clone_is_byte_exact() {
        let dir = scratch("exact");
        let source = dir.join("source.bin");
        let target = dir.join("target.bin");
        let data = deterministic(256 * 1024, 0xA0A0);
        std::fs::write(&source, &data).unwrap();
        SharedFsCheckpointStore::clone_file_capped(&source, &target, 64 * 1024)
            .expect("multi-round clone succeeds");
        let copied = std::fs::read(&target).unwrap();
        assert_eq!(copied.len(), data.len(), "exact length");
        assert_eq!(
            copied, data,
            "every round advances by exactly its chunk; no skipped ranges"
        );
    }

    #[test]
    fn multi_round_clone_preserves_sparse_layout() {
        let dir = scratch("sparse");
        let source = dir.join("sparse.bin");
        let target = dir.join("sparse-clone.bin");
        let marker = deterministic(64 * 1024, 0x5EED);
        {
            use std::io::{Seek, Write};
            let mut file = std::fs::File::create(&source).unwrap();
            file.seek(std::io::SeekFrom::Start(1024 * 1024)).unwrap();
            file.write_all(&marker).unwrap();
        }
        SharedFsCheckpointStore::clone_file_capped(&source, &target, 64 * 1024)
            .expect("sparse multi-round clone succeeds");
        let meta = std::fs::metadata(&target).unwrap();
        assert_eq!(
            meta.len(),
            1024 * 1024 + 64 * 1024,
            "sparse length preserved across rounds"
        );
        assert_eq!(std::fs::read(&target).unwrap()[1024 * 1024..], marker);
    }
}
