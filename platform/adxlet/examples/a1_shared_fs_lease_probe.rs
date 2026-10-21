//! Two-process, two-mount lease acceptance fixture; not a product controller.
use adx_core::{lifecycle::CheckpointStorage, CheckpointArtifact, Error as AdxError};
use adxlet::checkpoint::{CheckpointStore, SharedFsCheckpointStore};
use serde_json::{json, Value};
use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::Write,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn require(condition: bool, message: &str) -> ProbeResult<()> {
    if !condition {
        return Err(std::io::Error::other(message.to_owned()).into());
    }
    Ok(())
}

fn publish_flag(root: &Path, name: &str, nonce: &str, mut value: Value) -> ProbeResult<()> {
    value["nonce"] = json!(nonce);
    let destination = root.join(name);
    require(
        !destination.try_exists()?,
        "barrier already exists; use a fresh fixture",
    )?;
    let temporary = root.join(format!(".pending-{name}-{nonce}"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    serde_json::to_writer(&mut file, &value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(temporary, destination)?;
    File::open(root)?.sync_all()?;
    Ok(())
}

async fn wait_flag(root: &Path, name: &str, nonce: &str) -> ProbeResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match std::fs::read(root.join(name)) {
            Ok(data) => {
                let value: Value = serde_json::from_slice(&data)?;
                require(
                    value.get("nonce").and_then(Value::as_str) == Some(nonce),
                    "barrier owner mismatch",
                )?;
                return Ok(value);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }
        require(
            Instant::now() < deadline,
            "partner barrier timed out; retain fixture",
        )?;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn capture(
    store: &SharedFsCheckpointStore,
    payload: &[u8],
) -> ProbeResult<CheckpointArtifact> {
    let staged = store.allocate_for_policy(CheckpointStorage::Shared).await?;
    std::fs::write(staged.join("memory"), payload)?;
    Ok(store.publish(&staged).await?)
}

fn decode_artifact(value: &Value) -> ProbeResult<CheckpointArtifact> {
    Ok(serde_json::from_value(
        value.get("artifact").ok_or("missing artifact")?.clone(),
    )?)
}

fn deletion_claim(root: &Path, artifact: &CheckpointArtifact) -> ProbeResult<File> {
    let key = uuid::Uuid::parse_str(&artifact.location)?.to_string();
    require(key == artifact.location, "invalid published identity")?;
    let directory = root.join("artifacts/.leases");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join(&key))?;
    let expected = format!("adx-shared-lease-v1 {}\n", file.metadata()?.ino());
    require(
        std::fs::read(directory.join(format!("{key}.identity")))? == expected.as_bytes(),
        "lock inode registration mismatch",
    )?;
    // SAFETY: file owns this fd for the syscall and for the complete claim lifetime.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(file)
}

async fn source(root: &Path, nonce: &str) -> ProbeResult<()> {
    let store = SharedFsCheckpointStore::new(root.join("artifacts"), root.join("local-source"))?;
    let artifact = capture(&store, b"cross-client reader dependency").await?;
    let guard = store.materialize(&artifact).await?;
    publish_flag(
        root,
        "reader-ready.json",
        nonce,
        json!({"artifact":artifact}),
    )?;
    wait_flag(root, "reader-delete-blocked.json", nonce).await?;
    drop(guard);
    publish_flag(root, "source-reader-released.json", nonce, json!({}))?;
    wait_flag(root, "reader-phase-done.json", nonce).await?;

    let artifact = capture(&store, b"collector-first dependency").await?;
    let claim = deletion_claim(root, &artifact)?;
    publish_flag(
        root,
        "collector-ready.json",
        nonce,
        json!({"artifact":artifact}),
    )?;
    wait_flag(root, "collector-readers-blocked.json", nonce).await?;
    drop(claim);
    publish_flag(root, "collector-released.json", nonce, json!({}))?;
    wait_flag(root, "target-done.json", nonce).await?;
    println!(
        "{}",
        json!({"role":"source","status":"passed","nonce":nonce})
    );
    Ok(())
}

async fn target(root: &Path, nonce: &str) -> ProbeResult<()> {
    let ready = wait_flag(root, "reader-ready.json", nonce).await?;
    let artifact = decode_artifact(&ready)?;
    let reader =
        SharedFsCheckpointStore::new(root.join("artifacts"), root.join("local-target-reader"))?;
    let collector =
        SharedFsCheckpointStore::new(root.join("artifacts"), root.join("local-target-collector"))?;
    let guard = reader.materialize(&artifact).await?;
    require(
        matches!(collector.remove(&artifact).await, Err(AdxError::Conflict)),
        "collector bypassed live source/target readers",
    )?;
    require(
        std::fs::read(guard.join("memory"))? == b"cross-client reader dependency",
        "reader payload changed",
    )?;
    publish_flag(root, "reader-delete-blocked.json", nonce, json!({}))?;
    wait_flag(root, "source-reader-released.json", nonce).await?;
    require(
        matches!(collector.remove(&artifact).await, Err(AdxError::Conflict)),
        "last target reader did not retain protection",
    )?;
    drop(guard);
    collector.remove(&artifact).await?;
    collector.remove(&artifact).await?;
    require(
        root.join("artifacts/.leases")
            .join(&artifact.location)
            .is_file(),
        "successful removal unlinked stable lock",
    )?;
    publish_flag(root, "reader-phase-done.json", nonce, json!({}))?;

    let ready = wait_flag(root, "collector-ready.json", nonce).await?;
    let artifact = decode_artifact(&ready)?;
    require(
        matches!(reader.materialize(&artifact).await, Err(AdxError::Conflict)),
        "reader bypassed collector claim",
    )?;
    require(
        matches!(reader.duplicate(&artifact).await, Err(AdxError::Conflict)),
        "clone bypassed collector claim",
    )?;
    publish_flag(root, "collector-readers-blocked.json", nonce, json!({}))?;
    wait_flag(root, "collector-released.json", nonce).await?;
    let guard = reader.materialize(&artifact).await?;
    require(
        std::fs::read(guard.join("memory"))? == b"collector-first dependency",
        "post-claim original payload mismatch",
    )?;
    let cloned = reader.duplicate(&artifact).await?;
    let cloned_guard = reader.materialize(&cloned).await?;
    require(
        std::fs::read(cloned_guard.join("memory"))? == b"collector-first dependency",
        "independent clone payload mismatch",
    )?;
    drop(guard);
    collector.remove(&artifact).await?;
    require(
        matches!(collector.remove(&cloned).await, Err(AdxError::Conflict)),
        "collector bypassed cloned reader",
    )?;
    drop(cloned_guard);
    collector.remove(&cloned).await?;
    publish_flag(
        root,
        "target-done.json",
        nonce,
        json!({"readers_first":true,"collector_first":true,"clone_verified":true}),
    )?;
    println!(
        "{}",
        json!({"role":"target","status":"passed","nonce":nonce})
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ProbeResult<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    require(
        args.len() == 3,
        "usage: a1_shared_fs_lease_probe ROOT NONCE source|target",
    )?;
    let root = PathBuf::from(&args[0]);
    let nonce = uuid::Uuid::parse_str(&args[1])?.to_string();
    require(nonce == args[1], "nonce must be a canonical UUID")?;
    require(
        root.is_absolute()
            && root.file_name().and_then(|v| v.to_str()) == Some(&format!("lease-{nonce}")),
        "unexpected fixture path",
    )?;
    require(
        std::fs::symlink_metadata(&root)?.is_dir() && root.canonicalize()? == root,
        "fixture root must not be a symlink",
    )?;
    let owner: Value = serde_json::from_slice(&std::fs::read(root.join("owner.json"))?)?;
    require(
        owner.get("nonce").and_then(Value::as_str) == Some(nonce.as_str()),
        "fixture owner mismatch",
    )?;
    match args[2].as_str() {
        "source" => source(&root, &nonce).await,
        "target" => target(&root, &nonce).await,
        _ => Err(std::io::Error::other("role must be source or target").into()),
    }
}
