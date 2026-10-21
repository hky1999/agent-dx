//! Stable JuiceFS reader/remover locks, not a replacement for durable ADX references.
//!
//! Only fresh staging allocation creates the lock and its inode registration.
//! Readers and removers never recreate either file, and neither is unlinked.
//! A historical artifact without registration must be retained for an explicit
//! migration. Mixed-version writers/removers must be drained before rollout.

use super::{invalid, io_error, STAGING_PREFIX};
use adx_core::{Error, Result};
use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::Arc,
};

pub(super) struct SharedFsLeases {
    root: PathBuf,
    root_dir: File,
    directory: File,
}

impl SharedFsLeases {
    pub(super) fn new(root: &Path) -> Result<Self> {
        let open_directory = |path: &Path| {
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .map_err(io_error)
        };
        let root_dir = open_directory(root)?;
        let path = root.join(".leases");
        match std::fs::create_dir(&path) {
            Ok(()) => {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                    .map_err(io_error)?;
                root_dir.sync_all().map_err(io_error)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(io_error(error)),
        }
        let directory = open_directory(&path)?;
        let result = Self {
            root: root.to_path_buf(),
            root_dir,
            directory,
        };
        result.validate_directory()?;
        Ok(result)
    }

    fn key(location: &str) -> Result<String> {
        let mut components = Path::new(location).components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            return Err(invalid("lease location must be one normal path component"));
        }
        let key = location.strip_prefix(STAGING_PREFIX).unwrap_or(location);
        let uuid = uuid::Uuid::parse_str(key).map_err(|_| invalid("invalid lease UUID"))?;
        if uuid.to_string() != key {
            return Err(invalid("noncanonical lease UUID"));
        }
        Ok(key.to_owned())
    }

    fn validate_directory(&self) -> Result<()> {
        let path = self.root.join(".leases");
        for (file, path) in [
            (&self.root_dir, self.root.as_path()),
            (&self.directory, path.as_path()),
        ] {
            let opened = file.metadata().map_err(io_error)?;
            let current = std::fs::symlink_metadata(path).map_err(io_error)?;
            if !current.is_dir() || opened.dev() != current.dev() || opened.ino() != current.ino() {
                return Err(invalid("shared root or lease directory was replaced"));
            }
        }
        Ok(())
    }

    fn open(&self, name: &str, flags: libc::c_int) -> Result<File> {
        let name = CString::new(name).map_err(|_| invalid("NUL in lease identity"))?;
        // SAFETY: directory owns a live directory fd and name is NUL-terminated.
        // O_NOFOLLOW forbids a symlink leaf; mode is used only with O_CREAT.
        let fd = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io_error(std::io::Error::last_os_error()));
        }
        // SAFETY: successful openat returned a new owned fd; it is wrapped once.
        let file = unsafe { File::from_raw_fd(fd) };
        let meta = file.metadata().map_err(io_error)?;
        if !meta.is_file() || meta.nlink() != 1 {
            return Err(invalid(
                "lease must be a regular file with exactly one link",
            ));
        }
        Ok(file)
    }

    fn flock(file: &File, exclusive: bool) -> Result<()> {
        let flags = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        } | libc::LOCK_NB;
        // SAFETY: file owns the fd throughout the syscall; flock borrows it.
        if unsafe { libc::flock(file.as_raw_fd(), flags) } != 0 {
            let error = std::io::Error::last_os_error();
            return Err(if error.kind() == std::io::ErrorKind::WouldBlock {
                Error::Conflict
            } else {
                io_error(error)
            });
        }
        Ok(())
    }

    /// Register only a freshly allocated staging UUID, before exposing it.
    /// Partial failures are retained; this function never repairs an old pair.
    pub(super) fn initialize(&self, location: &str) -> Result<()> {
        if !location.starts_with(STAGING_PREFIX) {
            return Err(invalid("only fresh staging can register a lease"));
        }
        let key = Self::key(location)?;
        self.validate_directory()?;
        let file = self.open(&key, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;
        Self::flock(&file, true)?;
        let mut identity = self.open(
            &format!("{key}.identity"),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        )?;
        writeln!(
            identity,
            "adx-shared-lease-v1 {}",
            file.metadata().map_err(io_error)?.ino()
        )
        .map_err(io_error)?;
        identity.sync_all().map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        self.directory.sync_all().map_err(io_error)?;
        self.validate_directory()
    }

    /// Acquire before inspecting bytes. Missing/replaced registrations fail
    /// closed, including historical artifacts that have not been migrated.
    pub(super) fn lock(&self, location: &str, exclusive: bool) -> Result<Arc<File>> {
        let key = Self::key(location)?;
        self.validate_directory()?;
        let file = self.open(
            &key,
            if exclusive {
                libc::O_RDWR
            } else {
                libc::O_RDONLY
            },
        )?;
        Self::flock(&file, exclusive)?;
        let identity = self.open(&format!("{key}.identity"), libc::O_RDONLY)?;
        let mut data = Vec::new();
        identity
            .take(128)
            .read_to_end(&mut data)
            .map_err(io_error)?;
        let expected = format!(
            "adx-shared-lease-v1 {}\n",
            file.metadata().map_err(io_error)?.ino()
        );
        if data != expected.as_bytes() {
            return Err(invalid(
                "lease inode no longer matches its immutable registration",
            ));
        }
        self.validate_directory()?;
        Ok(Arc::new(file))
    }

    /// Repeat of an already finished removal succeeds. Invalid paths, changed
    /// mounts, active readers and missing registrations never authorize deletion.
    pub(super) fn remove(&self, location: &str) -> Result<()> {
        Self::key(location)?;
        self.validate_directory()?;
        let path = self.root.join(location);
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_dir() => (),
            Ok(_) => {
                return Err(invalid(
                    "shared checkpoint must be a directory, not a symlink",
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io_error(error)),
        }
        let lease = self.lock(location, true)?;
        match std::fs::remove_dir_all(&path) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(io_error(error)),
        }
        self.root_dir.sync_all().map_err(io_error)?;
        drop(lease);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SharedFsLeases;
    use adx_core::Error;
    use std::{
        os::unix::fs::{symlink, MetadataExt},
        path::PathBuf,
    };

    struct Fixture {
        root: PathBuf,
        key: String,
        staging: String,
        leases: SharedFsLeases,
    }
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("adx-lease-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&root).unwrap();
            let leases = SharedFsLeases::new(&root).unwrap();
            let key = uuid::Uuid::new_v4().to_string();
            let staging = format!(".staging-{key}");
            std::fs::create_dir(root.join(&staging)).unwrap();
            std::fs::write(root.join(&staging).join("memory"), b"still in use").unwrap();
            leases.initialize(&staging).unwrap();
            Self {
                root,
                key,
                staging,
                leases,
            }
        }
        fn lock_path(&self) -> PathBuf {
            self.root.join(".leases").join(&self.key)
        }
        fn identity_path(&self) -> PathBuf {
            self.root
                .join(".leases")
                .join(format!("{}.identity", self.key))
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn reader_and_remover_exclude_each_other_in_both_directions() {
        let f = Fixture::new();
        let reader = f.leases.lock(&f.staging, false).unwrap();
        assert!(matches!(
            f.leases.lock(&f.staging, true),
            Err(Error::Conflict)
        ));
        drop(reader);
        let remover = f.leases.lock(&f.staging, true).unwrap();
        assert!(matches!(
            f.leases.lock(&f.staging, false),
            Err(Error::Conflict)
        ));
        drop(remover);
        assert!(f.leases.lock(&f.staging, false).is_ok());
    }

    #[test]
    fn staging_and_published_names_share_the_same_lock() {
        let f = Fixture::new();
        let reader = f.leases.lock(&f.staging, false).unwrap();
        std::fs::rename(f.root.join(&f.staging), f.root.join(&f.key)).unwrap();
        assert!(matches!(f.leases.remove(&f.key), Err(Error::Conflict)));
        let another_reader = f.leases.lock(&f.key, false).unwrap();
        drop(reader);
        assert!(matches!(f.leases.remove(&f.key), Err(Error::Conflict)));
        drop(another_reader);
        f.leases.remove(&f.key).unwrap();
    }

    #[test]
    fn completed_removal_retains_the_lock_inode_and_is_repeatable() {
        let f = Fixture::new();
        let before = f.lock_path().metadata().unwrap().ino();
        f.leases.remove(&f.staging).unwrap();
        f.leases.remove(&f.staging).unwrap();
        assert_eq!(f.lock_path().metadata().unwrap().ino(), before);
        assert!(f.identity_path().is_file());
    }

    #[test]
    fn replaced_lock_inode_cannot_bypass_an_existing_reader() {
        let f = Fixture::new();
        let reader = f.leases.lock(&f.staging, false).unwrap();
        std::fs::rename(f.lock_path(), f.root.join(".leases/retired-lock")).unwrap();
        std::fs::write(f.lock_path(), b"").unwrap();
        assert!(matches!(
            f.leases.lock(&f.staging, false),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            f.leases.remove(&f.staging),
            Err(Error::Invalid(_))
        ));
        assert_eq!(
            std::fs::read(f.root.join(&f.staging).join("memory")).unwrap(),
            b"still in use"
        );
        drop(reader);
    }

    #[test]
    fn replaced_lease_directory_is_rejected() {
        let f = Fixture::new();
        std::fs::rename(f.root.join(".leases"), f.root.join("retired-leases")).unwrap();
        std::fs::create_dir(f.root.join(".leases")).unwrap();
        assert!(matches!(
            f.leases.lock(&f.staging, false),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            f.leases.remove(&f.staging),
            Err(Error::Invalid(_))
        ));
        assert!(f.root.join(&f.staging).join("memory").is_file());
    }

    #[test]
    fn missing_registration_is_never_recreated_by_read_or_delete() {
        let f = Fixture::new();
        std::fs::remove_file(f.identity_path()).unwrap();
        assert!(matches!(
            f.leases.lock(&f.staging, false),
            Err(Error::Unavailable(_))
        ));
        assert!(matches!(
            f.leases.remove(&f.staging),
            Err(Error::Unavailable(_))
        ));
        assert!(!f.identity_path().exists());
        assert!(f.root.join(&f.staging).join("memory").is_file());
        assert!(f.leases.initialize(&f.staging).is_err());
        assert!(!f.identity_path().exists());
    }

    #[test]
    fn historical_unregistered_artifacts_are_retained() {
        let f = Fixture::new();
        let legacy = uuid::Uuid::new_v4().to_string();
        std::fs::create_dir(f.root.join(&legacy)).unwrap();
        std::fs::write(f.root.join(&legacy).join("memory"), b"legacy").unwrap();
        assert!(matches!(
            f.leases.lock(&legacy, false),
            Err(Error::Unavailable(_))
        ));
        assert!(matches!(
            f.leases.remove(&legacy),
            Err(Error::Unavailable(_))
        ));
        assert!(!f.root.join(".leases").join(&legacy).exists());
        assert_eq!(
            std::fs::read(f.root.join(&legacy).join("memory")).unwrap(),
            b"legacy"
        );
    }

    #[test]
    fn malformed_or_symlink_artifact_paths_never_remove_data() {
        let f = Fixture::new();
        assert!(matches!(
            f.leases.remove("../outside"),
            Err(Error::Invalid(_))
        ));
        let alias = uuid::Uuid::new_v4().to_string();
        symlink(f.root.join(&f.staging), f.root.join(&alias)).unwrap();
        assert!(matches!(f.leases.remove(&alias), Err(Error::Invalid(_))));
        assert_eq!(
            std::fs::read(f.root.join(&f.staging).join("memory")).unwrap(),
            b"still in use"
        );
    }
}
