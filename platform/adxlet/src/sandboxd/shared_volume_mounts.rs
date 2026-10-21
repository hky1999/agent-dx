//! Trusted JuiceFS directory sources for the existing sandboxd bind backends.
//! Staging is node-local mount state, never the shared-volume authority.
use super::{proto, unavailable};
use adx_core::{
    shared_volumes::{SharedVolumeAccess, SharedVolumeMount},
    Error, Result,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, VecDeque},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::Mutex,
};

const ROOT_MARKER: &str = ".adx-shared-volume-store";
const OWNER: &str = ".owner";
const FUSE_MAGIC: u64 = 0x6573_5546;
const RESOLVE_SAFE: u64 = 0x01 | 0x02 | 0x04 | 0x08;
const RETIRED_CACHE_LIMIT: usize = 4096;

fn default_limit() -> usize {
    1024
}

/// Deployment-owned configuration. No field is accepted from a Sandbox caller.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub root: PathBuf,
    pub staging_root: PathBuf,
    pub storage_id: String,
    #[serde(default = "default_limit")]
    pub max_staged_executions: usize,
}

#[derive(Default)]
struct State {
    retired: BTreeSet<String>,
    retired_order: VecDeque<String>,
}

pub(super) struct SharedVolumeMounts {
    root: File,
    staging: File,
    path: PathBuf,
    storage_id: String,
    node_id: String,
    limit: usize,
    serial: Mutex<State>,
}

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

fn failure(message: &str) -> Error {
    Error::Unavailable(format!("shared volume: {message}"))
}

fn open_directory(path: &Path) -> Result<File> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
        || fs::canonicalize(path).map_err(unavailable)? != path
    {
        return Err(Error::Invalid(
            "canonical deployment directory required".into(),
        ));
    }
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(unavailable)
}

fn owned(file: &File) -> Result<()> {
    let metadata = file.metadata().map_err(unavailable)?;
    // SAFETY: geteuid takes no pointers and has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
        return Err(Error::Invalid(
            "shared volume parent must be server-owned and mode 0700".into(),
        ));
    }
    Ok(())
}

/// Resolve and pin a directory before handing it to a path-based backend.
/// openat2 rejects symlinks, magic links, traversal and nested mounts. Never
/// fall back to realpath/open on kernels or filesystems that reject this call.
fn resolve(parent: &File, relative: &str, flags: i32, resolve: u64) -> Result<File> {
    let name =
        CString::new(relative).map_err(|_| Error::Invalid("invalid relative directory".into()))?;
    let how = OpenHow {
        flags: (flags | libc::O_CLOEXEC | libc::O_NOFOLLOW) as u64,
        mode: 0,
        resolve,
    };
    // SAFETY: name is NUL terminated; how points to its initialized 24-byte ABI
    // representation; parent owns a live directory FD for the syscall duration.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            name.as_ptr(),
            &how as *const OpenHow,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        return Err(unavailable(std::io::Error::last_os_error()));
    }
    // SAFETY: successful openat2 returned a new owned FD, not shared elsewhere.
    Ok(unsafe { File::from_raw_fd(fd as i32) })
}

fn beneath(parent: &File, relative: &str, flags: i32) -> Result<File> {
    resolve(parent, relative, flags, RESOLVE_SAFE)
}

fn child(parent: &File, name: &str, mode: u32, allow_staged_bind: bool) -> Result<(File, bool)> {
    let name_c = CString::new(name).map_err(|_| Error::Invalid("invalid directory key".into()))?;
    // SAFETY: parent is live, name_c is a valid C string and mode is a mode_t.
    let created =
        unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), mode as libc::mode_t) } == 0;
    if !created && std::io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST) {
        return Err(unavailable(std::io::Error::last_os_error()));
    }
    // A generated leaf below the protected local staging parent may already
    // be our bind mount. User-controlled source paths NEVER disable NO_XDEV.
    let flags = if allow_staged_bind {
        RESOLVE_SAFE & !0x01
    } else {
        RESOLVE_SAFE
    };
    let file = resolve(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, flags)?;
    if !file.metadata().map_err(unavailable)?.is_dir() {
        return Err(Error::Conflict);
    }
    Ok((file, created))
}

fn marker(file: &File, name: &str, expected: &[u8]) -> Result<()> {
    let marker = beneath(file, name, libc::O_RDONLY)?;
    let metadata = marker.metadata().map_err(unavailable)?;
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file() || metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
        return Err(Error::Invalid(
            "server-owned identity marker required".into(),
        ));
    }
    let mut bytes = Vec::new();
    marker
        .take((expected.len() + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(unavailable)?;
    if bytes != expected {
        return Err(Error::Conflict);
    }
    Ok(())
}

fn identity_bytes(storage_id: &str) -> Vec<u8> {
    format!("adx-shared-volume-store/v1\n{storage_id}\n").into_bytes()
}

fn fuse(file: &File) -> Result<bool> {
    let mut value = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: file owns a live FD and value points to writable statfs storage.
    if unsafe { libc::fstatfs(file.as_raw_fd(), value.as_mut_ptr()) } != 0 {
        return Err(unavailable(std::io::Error::last_os_error()));
    }
    // SAFETY: successful fstatfs initialized value.
    Ok(unsafe { value.assume_init() }.f_type as u64 == FUSE_MAGIC)
}

fn mount_capability() -> Result<()> {
    let status = fs::read_to_string("/proc/self/status").map_err(unavailable)?;
    let caps = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|value| u64::from_str_radix(value.trim(), 16).ok());
    if caps.is_none_or(|value| value & (1 << 21) == 0) {
        return Err(failure(
            "CAP_SYS_ADMIN in the task mount namespace is required",
        ));
    }
    Ok(())
}

fn hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn valid_key(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl SharedVolumeMounts {
    pub(super) fn new(config: Config, node_id: String) -> Result<Self> {
        let storage = uuid::Uuid::parse_str(&config.storage_id)
            .map_err(|_| Error::Invalid("canonical storage UUID required".into()))?;
        if storage.to_string() != config.storage_id
            || node_id.is_empty()
            || !(1..=65536).contains(&config.max_staged_executions)
        {
            return Err(Error::Invalid(
                "bounded shared volume deployment required".into(),
            ));
        }
        mount_capability()?;
        let root = open_directory(&config.root)?;
        owned(&root)?;
        if !fuse(&root)? {
            return Err(Error::Invalid(
                "JuiceFS FUSE root required; local fallback forbidden".into(),
            ));
        }
        let expected = identity_bytes(&config.storage_id);
        marker(&root, ROOT_MARKER, &expected)?;
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(&config.staging_root).map_err(unavailable)?;
        let staging = open_directory(&config.staging_root)?;
        owned(&staging)?;
        if fuse(&staging)?
            || root.metadata().map_err(unavailable)?.dev()
                == staging.metadata().map_err(unavailable)?.dev()
        {
            return Err(Error::Invalid(
                "node-local staging must be separate from JuiceFS".into(),
            ));
        }
        let marker_path = config.staging_root.join(ROOT_MARKER);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&marker_path)
        {
            Ok(mut file) => {
                file.write_all(&expected).map_err(unavailable)?;
                file.sync_all().map_err(unavailable)?;
                staging.sync_all().map_err(unavailable)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(unavailable(error)),
        }
        marker(&staging, ROOT_MARKER, &expected)?;
        Ok(Self {
            root,
            staging,
            path: config.staging_root,
            storage_id: config.storage_id,
            node_id,
            limit: config.max_staged_executions,
            serial: Mutex::default(),
        })
    }

    fn key(&self, runtime_id: &str) -> String {
        hash(&format!(
            "{}\0{}\0{runtime_id}",
            self.storage_id, self.node_id
        ))
    }

    fn owner(&self, runtime_id: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"protocol":"adx-volume-stage/v1",
            "storage_id":self.storage_id,"node_id":self.node_id,"runtime_id":runtime_id}))
        .expect("JSON strings")
    }

    fn execution(&self, runtime_id: &str) -> Result<(File, PathBuf)> {
        let key = self.key(runtime_id);
        let count =
            fs::read_dir(&self.path)
                .map_err(unavailable)?
                .try_fold(0usize, |count, item| {
                    let name = item.map_err(unavailable)?.file_name();
                    let name = name.to_str().ok_or(Error::Conflict)?;
                    if name == ROOT_MARKER {
                        return Ok(count);
                    }
                    if !valid_key(name) {
                        return Err(Error::Conflict);
                    }
                    Ok(count + 1)
                })?;
        if count >= self.limit && !self.path.join(&key).exists() {
            return Err(failure(
                "staged execution limit reached; no unsafe eviction",
            ));
        }
        let (directory, _) = child(&self.staging, &key, 0o700, false)?;
        owned(&directory)?;
        let path = self.path.join(key);
        let expected = self.owner(runtime_id);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path.join(OWNER))
        {
            Ok(mut file) => {
                file.write_all(&expected).map_err(unavailable)?;
                file.sync_all().map_err(unavailable)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(unavailable(error)),
        }
        marker(&directory, OWNER, &expected)?;
        Ok((directory, path))
    }

    pub(super) fn prepare(
        &self,
        runtime_id: &str,
        mounts: &[SharedVolumeMount],
    ) -> Result<Vec<proto::Mount>> {
        let state = self
            .serial
            .lock()
            .map_err(|_| failure("staging lock poisoned"))?;
        if state.retired.contains(&self.key(runtime_id)) {
            return Err(Error::Conflict);
        }
        let (execution, path) = self.execution(runtime_id)?;
        let mut result = Vec::with_capacity(mounts.len());
        for requested in mounts {
            let (volume, created) = child(&self.root, requested.volume_id.as_str(), 0o777, false)?;
            if created {
                // Only the protected parent can create/replace a volume root.
                // Access between sandboxes is governed by admission and RO/RW.
                volume
                    .set_permissions(fs::Permissions::from_mode(0o777))
                    .map_err(unavailable)?;
            }
            let source = if requested.subpath.as_str().is_empty() {
                volume
            } else {
                beneath(
                    &volume,
                    requested.subpath.as_str(),
                    libc::O_RDONLY | libc::O_DIRECTORY,
                )?
            };
            let source_id = source.metadata().map_err(unavailable)?;
            let key = hash(&format!(
                "{}\0{}",
                requested.volume_id.as_str(),
                requested.subpath.as_str()
            ));
            let (stage, _) = child(&execution, &key, 0o700, true)?;
            let stage_id = stage.metadata().map_err(unavailable)?;
            if (stage_id.dev(), stage_id.ino()) != (source_id.dev(), source_id.ino()) {
                // Never replace a different source behind an existing mount.
                if stage_id.dev() != execution.metadata().map_err(unavailable)?.dev() {
                    return Err(Error::Conflict);
                }
                owned(&stage)?;
                let from = CString::new(format!("/proc/self/fd/{}", source.as_raw_fd()))
                    .expect("decimal FD");
                let to = CString::new(path.join(&key).as_os_str().as_encoded_bytes())
                    .map_err(|_| Error::Invalid("invalid deployment staging path".into()))?;
                // SAFETY: both strings are live NUL-terminated paths. The source
                // FD pins the exact already-checked directory until mount returns.
                // The target parent is server-owned, never exported to a tenant.
                if unsafe {
                    libc::mount(
                        from.as_ptr(),
                        to.as_ptr(),
                        std::ptr::null(),
                        libc::MS_BIND,
                        std::ptr::null(),
                    )
                } != 0
                {
                    return Err(unavailable(std::io::Error::last_os_error()));
                }
            }
            result.push(proto::Mount {
                r#type: "bind".into(),
                target: requested.target.clone(),
                options: vec![
                    "rbind".into(),
                    match requested.access {
                        SharedVolumeAccess::ReadOnly => "ro",
                        SharedVolumeAccess::ReadWrite => "rw",
                    }
                    .into(),
                    "nodev".into(),
                    "nosuid".into(),
                ],
                source: Some(proto::mount::Source::HostPath(
                    path.join(key)
                        .to_str()
                        .ok_or_else(|| Error::Invalid("UTF-8 staging path required".into()))?
                        .into(),
                )),
            });
        }
        Ok(result)
    }

    /// Call only AFTER durable backend retirement proved the execution stopped.
    /// An uncertain proof must never enter this function.
    pub(super) fn retire(&self, runtime_id: &str) -> Result<()> {
        let mut state = self
            .serial
            .lock()
            .map_err(|_| failure("staging lock poisoned"))?;
        let key = self.key(runtime_id);
        if state.retired.insert(key.clone()) {
            state.retired_order.push_back(key.clone());
        }
        while state.retired_order.len() > RETIRED_CACHE_LIMIT {
            if let Some(old) = state.retired_order.pop_front() {
                state.retired.remove(&old);
            }
        }
        let path = self.path.join(&key);
        if !path.exists() {
            return Ok(());
        }
        let directory = beneath(&self.staging, &key, libc::O_RDONLY | libc::O_DIRECTORY)?;
        owned(&directory)?;
        marker(&directory, OWNER, &self.owner(runtime_id))?;
        for entry in fs::read_dir(&path).map_err(unavailable)? {
            let entry = entry.map_err(unavailable)?;
            let name = entry.file_name();
            let name = name.to_str().ok_or(Error::Conflict)?;
            if name == OWNER {
                continue;
            }
            if !valid_key(name) {
                return Err(Error::Conflict);
            }
            let target = CString::new(entry.path().as_os_str().as_encoded_bytes())
                .map_err(|_| Error::Conflict)?;
            // SAFETY: target is a generated child of this execution's protected
            // staging directory, authenticated above; no recursive/lazy unmount.
            if unsafe { libc::umount2(target.as_ptr(), 0) } != 0 {
                let error = std::io::Error::last_os_error();
                if !matches!(
                    error.raw_os_error(),
                    Some(libc::EINVAL) | Some(libc::ENOENT)
                ) {
                    return Err(unavailable(error));
                }
            }
            fs::remove_dir(entry.path()).map_err(unavailable)?;
        }
        fs::remove_file(path.join(OWNER)).map_err(unavailable)?;
        fs::remove_dir(path).map_err(unavailable)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn directory_resolution_rejects_links_and_escape_without_fallback() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("safe")).unwrap();
        symlink(outside.path(), root.path().join("escape")).unwrap();
        let dir = File::open(root.path()).unwrap();
        assert!(beneath(&dir, "safe", libc::O_RDONLY | libc::O_DIRECTORY).is_ok());
        for path in ["escape", "../outside", "/tmp", "safe/../escape"] {
            assert!(
                beneath(&dir, path, libc::O_RDONLY | libc::O_DIRECTORY).is_err(),
                "{path}"
            );
        }
    }

    #[test]
    fn an_open_directory_stays_pinned_after_a_rename_and_link_swap() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("safe")).unwrap();
        fs::write(root.path().join("safe/value"), b"inside").unwrap();
        fs::write(outside.path().join("value"), b"outside").unwrap();
        let dir = File::open(root.path()).unwrap();
        let pinned = beneath(&dir, "safe", libc::O_RDONLY | libc::O_DIRECTORY).unwrap();
        fs::rename(root.path().join("safe"), root.path().join("moved")).unwrap();
        symlink(outside.path(), root.path().join("safe")).unwrap();
        let mut bytes = Vec::new();
        beneath(&pinned, "value", libc::O_RDONLY)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, b"inside");
        assert!(beneath(&dir, "safe", libc::O_RDONLY | libc::O_DIRECTORY).is_err());
    }

    #[test]
    fn identity_marker_is_exact_and_cannot_be_a_link() {
        let root = tempfile::tempdir().unwrap();
        let dir = File::open(root.path()).unwrap();
        let expected = identity_bytes("49fc236e-3821-432c-8784-abc6b02c938e");
        let path = root.path().join(ROOT_MARKER);
        fs::write(&path, &expected).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        marker(&dir, ROOT_MARKER, &expected).unwrap();
        assert!(marker(&dir, ROOT_MARKER, b"different").is_err());
        fs::rename(&path, root.path().join("real")).unwrap();
        symlink("real", path).unwrap();
        assert!(marker(&dir, ROOT_MARKER, &expected).is_err());
    }

    #[test]
    fn cache_names_and_storage_identity_are_not_caller_paths() {
        assert!(valid_key(&hash("node\0execution")));
        for key in ["..", "/tmp", "../escape", "not-a-generated-key"] {
            assert!(!valid_key(key));
        }
        assert_ne!(hash("node-a\0execution"), hash("node-b\0execution"));
    }

    #[test]
    #[ignore = "requires an explicitly owned JuiceFS root and private mount namespace"]
    fn actual_bind_pins_a_renamed_juicefs_subdirectory_and_cleans_only_after_retirement() {
        let expected_ns =
            std::env::var("ADX_TEST_PRIVATE_MOUNT_NS").expect("owned namespace evidence");
        assert_eq!(
            fs::read_link("/proc/self/ns/mnt")
                .unwrap()
                .to_str()
                .unwrap(),
            expected_ns
        );
        let root = PathBuf::from(std::env::var("ADX_TEST_SHARED_VOLUME_ROOT").unwrap());
        let staging = PathBuf::from(std::env::var("ADX_TEST_SHARED_VOLUME_STAGING").unwrap());
        let provider = SharedVolumeMounts::new(
            Config {
                root: root.clone(),
                staging_root: staging,
                storage_id: std::env::var("ADX_TEST_SHARED_VOLUME_STORAGE_ID").unwrap(),
                max_staged_executions: 8,
            },
            "fixture-node".into(),
        )
        .unwrap();
        let id = adx_core::shared_volumes::SharedVolumeId::parse(uuid::Uuid::new_v4().to_string())
            .unwrap();
        let mut mount = SharedVolumeMount {
            volume_id: id.clone(),
            subpath: Default::default(),
            target: "/shared".into(),
            access: SharedVolumeAccess::ReadWrite,
        };
        provider
            .prepare("fixture-1", std::slice::from_ref(&mount))
            .unwrap();
        fs::create_dir(root.join(id.as_str()).join("project")).unwrap();
        fs::write(root.join(id.as_str()).join("project/value"), b"pinned").unwrap();
        mount.subpath = adx_core::shared_volumes::SharedVolumeSubpath::parse("project").unwrap();
        let first = provider
            .prepare("fixture-1", std::slice::from_ref(&mount))
            .unwrap();
        let second = provider
            .prepare("fixture-2", std::slice::from_ref(&mount))
            .unwrap();
        let source = |m: &proto::Mount| match m.source.as_ref().unwrap() {
            proto::mount::Source::HostPath(path) => PathBuf::from(path),
            _ => panic!("internal bind required"),
        };
        let first_path = source(&first[0]);
        let second_path = source(&second[0]);
        assert_ne!(first_path, second_path);
        fs::rename(
            root.join(id.as_str()).join("project"),
            root.join(id.as_str()).join("moved"),
        )
        .unwrap();
        symlink("/tmp", root.join(id.as_str()).join("project")).unwrap();
        assert_eq!(fs::read(first_path.join("value")).unwrap(), b"pinned");
        assert!(provider
            .prepare("fixture-3", std::slice::from_ref(&mount))
            .is_err());
        provider.retire("fixture-1").unwrap();
        assert!(!first_path.exists());
        assert_eq!(fs::read(second_path.join("value")).unwrap(), b"pinned");
        assert!(provider
            .prepare("fixture-1", std::slice::from_ref(&mount))
            .is_err());
        provider.retire("fixture-2").unwrap();
        provider.retire("fixture-3").unwrap();
        fs::remove_dir_all(root.join(id.as_str())).unwrap();
    }
}
