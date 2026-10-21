//! Environment-gated real-volume probe for `SharedFsCheckpointStore` (A0).
//!
//! Drives the **production** store implementation — no re-implemented copying,
//! no sandbox, no VM — against a real shared filesystem mount (JuiceFS):
//!
//! 1. builds the store through `StorageConfig::SharedFs` exactly as adxlet
//!    does (optionally verifying `Format.UUID` at an explicit `mount_root`);
//! 2. stages a sparse artifact file (`--file-gib`, default 3 GiB) with
//!    distinct 4 KiB markers written via `write_all_at` at offset 0, just
//!    below the 2 GiB single-round `copy_file_range` boundary, just above
//!    it, and at the end;
//! 3. hard-verifies via `stat` that the **largest single file** exceeds
//!    2 GiB — aggregate directory size is never accepted as evidence;
//! 4. publishes through the production barrier (`publish`) and branches
//!    through the production clone (`duplicate`);
//! 5. verifies the clone byte-exactly: full length, every marker at its
//!    exact offset, and hole regions reading back as zeros;
//! 6. mutates the clone and re-verifies the parent artifact is unchanged;
//! 7. on any failure prints `SCENE-PRESERVED` with the surviving paths and
//!    exits non-zero without deleting anything. `--cleanup` (success only)
//!    removes both artifacts through the production `remove`.
//!
//! This is the store-boundary probe only; it does not replace the ADX/runtime
//! A0 end-to-end probe and its results must not be presented as one.
//!
//! Modes:
//! - `full` (default): single-process publish + duplicate + verify + mutate.
//! - `publish`: source-context step — stage, stat-verify, publish through the
//!   barrier, print the artifact location, and RETAIN it (no cleanup). The
//!   marker bytes are a deterministic function of the offset, so an
//!   independent verifier can recompute every expectation.
//! - `verify`: target-context step — a completely independent client builds
//!   its own store handle against the same volume, materializes the named
//!   artifact read-only, and verifies length, every boundary marker and hole
//!   zeros without writing or removing anything.
//!
//! Usage (Root, on the real volume):
//!   cargo run -p adxlet --example shared_fs_volume_probe -- \
//!       --shared-root /mnt/juicefs/checkpoints [--local-root DIR] \
//!       [--volume-uuid UUID --mount-root /mnt/juicefs] \
//!       [--file-gib 3] [--cleanup] [--mode full|publish|verify --location UUID]

use adx_core::lifecycle::CheckpointStorage;
use adxlet::checkpoint::StorageConfig;
use std::{os::unix::fs::FileExt, path::PathBuf, process::exit};

const GIB: u64 = 1 << 30;
const MARKER_LEN: u64 = 4096;

fn usage() -> ! {
    eprintln!("usage: shared_fs_volume_probe --shared-root DIR [--local-root DIR] [--volume-uuid UUID --mount-root DIR] [--file-gib N] [--cleanup]");
    exit(2)
}

struct Args {
    shared_root: PathBuf,
    local_root: Option<PathBuf>,
    volume_uuid: Option<String>,
    mount_root: Option<PathBuf>,
    file_gib: u64,
    cleanup: bool,
    mode: Mode,
    location: Option<String>,
}

#[derive(PartialEq)]
enum Mode {
    Full,
    Publish,
    Verify,
}

fn parse_args() -> Args {
    let mut args = Args {
        shared_root: PathBuf::new(),
        local_root: None,
        volume_uuid: None,
        mount_root: None,
        file_gib: 3,
        cleanup: false,
        mode: Mode::Full,
        location: None,
    };
    let mut values = std::env::args().skip(1);
    while let Some(flag) = values.next() {
        fn value(values: &mut std::iter::Skip<std::env::Args>) -> String {
            values.next().unwrap_or_else(|| usage())
        }
        match flag.as_str() {
            "--shared-root" => args.shared_root = PathBuf::from(value(&mut values)),
            "--local-root" => args.local_root = Some(PathBuf::from(value(&mut values))),
            "--volume-uuid" => args.volume_uuid = Some(value(&mut values)),
            "--mount-root" => args.mount_root = Some(PathBuf::from(value(&mut values))),
            "--file-gib" => args.file_gib = value(&mut values).parse().unwrap_or_else(|_| usage()),
            "--cleanup" => args.cleanup = true,
            "--mode" => {
                args.mode = match value(&mut values).as_str() {
                    "full" => Mode::Full,
                    "publish" => Mode::Publish,
                    "verify" => Mode::Verify,
                    _ => usage(),
                }
            }
            "--location" => args.location = Some(value(&mut values)),
            _ => usage(),
        }
    }
    if args.shared_root.as_os_str().is_empty() {
        usage();
    }
    if args.mode != Mode::Verify && args.file_gib <= 2 {
        usage();
    }
    if args.mode == Mode::Verify && (args.location.is_none() || args.file_gib <= 2) {
        usage();
    }
    args
}

fn marker_bytes(offset: u64) -> Vec<u8> {
    // Offset-derived distinct pattern: a skip or double-advance in any
    // copy round changes the bytes compared at that offset.
    let mut state = (offset / MARKER_LEN) | 1;
    let mut data = Vec::with_capacity(MARKER_LEN as usize);
    while data.len() < MARKER_LEN as usize {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        data.extend_from_slice(&state.to_le_bytes());
    }
    data.truncate(MARKER_LEN as usize);
    data
}

fn read_at_exact(file: &std::fs::File, offset: u64, buffer: &mut [u8]) -> Result<(), String> {
    file.read_exact_at(buffer, offset)
        .map_err(|error| format!("read_at {offset}: {error}"))
}

fn verify_markers(file: &std::fs::File, len: u64, label: &str) -> Result<(), String> {
    let actual_len = file
        .metadata()
        .map_err(|e| format!("stat {label}: {e}"))?
        .len();
    if actual_len != len {
        return Err(format!("{label} length {actual_len} != expected {len}"));
    }
    for offset in [0, 2 * GIB - MARKER_LEN, 2 * GIB, len - MARKER_LEN] {
        let expected = marker_bytes(offset);
        let mut seen = vec![0u8; expected.len()];
        read_at_exact(file, offset, &mut seen)?;
        if seen != expected {
            return Err(format!(
                "{label} marker at offset {offset} mismatch (multi-round copy skipped or corrupted a range)"
            ));
        }
    }
    for hole in [GIB, 2 * GIB + GIB / 2] {
        if hole + MARKER_LEN < len - MARKER_LEN {
            let mut seen = vec![0u8; MARKER_LEN as usize];
            read_at_exact(file, hole, &mut seen)?;
            if seen.iter().any(|byte| *byte != 0) {
                return Err(format!("{label} hole at offset {hole} is not zero-filled"));
            }
        }
    }
    Ok(())
}

fn preserved(reason: &str, paths: &[String]) -> ! {
    eprintln!("SCENE-PRESERVED: {reason}");
    for path in paths {
        eprintln!("  retained: {path}");
    }
    exit(1)
}

fn main() {
    let args = parse_args();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run(args));
}

async fn run(args: Args) {
    let local_root = match &args.local_root {
        Some(path) => path.clone(),
        None => {
            std::env::temp_dir().join(format!("adx-volume-probe-local-{}", uuid::Uuid::new_v4()))
        }
    };
    let store = StorageConfig::SharedFs {
        root: args.shared_root.clone(),
        local_root,
        volume_uuid: args.volume_uuid.clone(),
        mount_root: args.mount_root.clone(),
    }
    .build()
    .unwrap_or_else(|error| preserved(&format!("store construction failed: {error}",), &[]));
    println!(
        "PASS store constructed (alias={}, root={})",
        store.storage_alias(),
        args.shared_root.display()
    );

    if args.mode == Mode::Verify {
        let location = args.location.clone().unwrap_or_else(|| usage());
        // The authoritative artifact record carries the byte count; the
        // verifier reconstructs it from --file-gib (the publisher prints
        // ARTIFACT_FILE_LEN for cross-checking).
        let artifact = adx_core::CheckpointArtifact {
            storage: "shared_fs".into(),
            location,
            size_bytes: args.file_gib * GIB,
        };
        let read = match store.materialize(&artifact).await {
            Ok(read) => read,
            Err(error) => preserved(&format!("verify materialize failed: {error}"), &[]),
        };
        let file = match std::fs::File::open(read.join("volume.bin")) {
            Ok(file) => file,
            Err(error) => preserved(&format!("verify open: {error}"), &[]),
        };
        let len = file
            .metadata()
            .unwrap_or_else(|error| preserved(&format!("verify stat: {error}"), &[]))
            .len();
        if len <= 2 * GIB {
            preserved(
                "verify: artifact single file does not exceed 2GiB",
                &[format!(
                    "{}/{}",
                    args.shared_root.display(),
                    artifact.location
                )],
            );
        }
        if let Err(error) = verify_markers(&file, len, "verify") {
            preserved(
                &format!("verify: {error}"),
                &[format!(
                    "{}/{}",
                    args.shared_root.display(),
                    artifact.location
                )],
            );
        }
        println!(
            "PASS independent-client read-only verify (location={}, single-file bytes={})",
            artifact.location, len
        );
        println!("PROBE-RESULT=PASS");
        return;
    }
    let staged = store
        .allocate_for_policy(CheckpointStorage::Shared)
        .await
        .unwrap_or_else(|error| preserved(&format!("staging allocation failed: {error}"), &[]));
    let mut retained: Vec<String> = vec![staged.display().to_string()];

    // Sparse artifact: set_len creates the holes; markers land at the copy
    // boundary offsets within the SAME single file.
    let volume = staged.join("volume.bin");
    let len = args.file_gib * GIB;
    let file = match std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&volume)
    {
        Ok(file) => file,
        Err(error) => preserved(&format!("create sparse artifact: {error}"), &retained),
    };
    if let Err(error) = file.set_len(len) {
        preserved(&format!("set_len {len}: {error}"), &retained);
    }
    for offset in [0, 2 * GIB - MARKER_LEN, 2 * GIB, len - MARKER_LEN] {
        let marker = marker_bytes(offset);
        if let Err(error) = file.write_all_at(&marker, offset) {
            preserved(&format!("write marker at {offset}: {error}"), &retained);
        }
    }
    drop(file);

    // Hard requirement: the largest single file must exceed 2GiB, verified
    // by stat — never the aggregate directory size.
    let stat = std::fs::metadata(&volume)
        .unwrap_or_else(|error| preserved(&format!("stat artifact: {error}"), &retained));
    println!(
        "PASS sparse artifact staged (single-file bytes={}, >2GiB={})",
        stat.len(),
        stat.len() > 2 * GIB
    );
    if stat.len() <= 2 * GIB {
        preserved("largest single artifact does not exceed 2GiB", &retained);
    }

    let artifact = match store.publish(&staged).await {
        Ok(artifact) => artifact,
        Err(error) => preserved(&format!("publish failed: {error}"), &retained),
    };
    if args.mode == Mode::Publish {
        println!("ARTIFACT_LOCATION={}", artifact.location);
        println!("ARTIFACT_FILE_LEN={len}");
        println!("PASS source-context publish retained for independent verify");
        println!("PROBE-RESULT=PASS");
        return;
    }
    retained = vec![format!(
        "{}/{}",
        args.shared_root.display(),
        artifact.location
    )];
    println!(
        "PASS publish barrier completed (location={}, size_bytes={})",
        artifact.location, artifact.size_bytes
    );

    // Production read path with pin semantics.
    let parent = match store.materialize(&artifact).await {
        Ok(parent) => parent,
        Err(error) => preserved(&format!("parent materialize: {error}"), &retained),
    };
    let parent_file = match std::fs::File::open(parent.join("volume.bin")) {
        Ok(file) => file,
        Err(error) => preserved(&format!("open parent: {error}"), &retained),
    };
    if let Err(error) = verify_markers(&parent_file, len, "parent") {
        preserved(&format!("parent verification: {error}"), &retained);
    }
    println!("PASS parent markers and holes verified");

    let clone_artifact = match store.duplicate(&artifact).await {
        Ok(clone) => clone,
        Err(error) => preserved(&format!("duplicate failed: {error}"), &retained),
    };
    retained.push(format!(
        "{}/{}",
        args.shared_root.display(),
        clone_artifact.location
    ));
    println!(
        "PASS duplicate completed through production copy_file_range (location={})",
        clone_artifact.location
    );

    let branch = match store.materialize(&clone_artifact).await {
        Ok(branch) => branch,
        Err(error) => preserved(&format!("clone materialize: {error}"), &retained),
    };
    let branch_file = match std::fs::File::open(branch.join("volume.bin")) {
        Ok(file) => file,
        Err(error) => preserved(&format!("open clone: {error}"), &retained),
    };
    if let Err(error) = verify_markers(&branch_file, len, "clone") {
        preserved(&format!("clone verification: {error}"), &retained);
    }
    println!("PASS clone length, boundary markers and hole zeros verified");

    // Mutate the clone inside a former hole; the parent must not change.
    let mutation_offset = GIB;
    let mutation = marker_bytes(mutation_offset ^ 0xDEAD_BEEF);
    let branch_writer = match std::fs::OpenOptions::new()
        .write(true)
        .open(branch.join("volume.bin"))
    {
        Ok(file) => file,
        Err(error) => preserved(&format!("reopen clone for mutation: {error}"), &retained),
    };
    if let Err(error) = branch_writer.write_all_at(&mutation, mutation_offset) {
        preserved(&format!("clone mutation write: {error}"), &retained);
    }
    // Push the mutation through the backend before comparing: the parent
    // check must hold against a durably written clone, not one that only
    // lives in this client's page cache.
    if let Err(error) = branch_writer.sync_all() {
        preserved(&format!("clone mutation sync: {error}"), &retained);
    }
    drop(branch_writer);
    // The mutation must actually exist in the clone: re-read the mutated
    // range from the branch file and compare byte-exactly.
    let branch_reader = match std::fs::File::open(branch.join("volume.bin")) {
        Ok(file) => file,
        Err(error) => preserved(&format!("reopen clone for read-back: {error}"), &retained),
    };
    let mut mutation_seen = vec![0u8; mutation.len()];
    if let Err(error) = read_at_exact(&branch_reader, mutation_offset, &mut mutation_seen) {
        preserved(&format!("clone mutation read-back: {error}"), &retained);
    }
    if mutation_seen != mutation {
        preserved("clone mutation not visible on re-read", &retained);
    }
    println!("PASS clone mutation durably written and read back");
    let mut parent_seen = vec![0u8; mutation.len()];
    if let Err(error) = read_at_exact(&parent_file, mutation_offset, &mut parent_seen) {
        preserved(&format!("parent re-read: {error}"), &retained);
    }
    if parent_seen.iter().any(|byte| *byte != 0) {
        preserved(
            "parent mutated after clone write (branch is not independent)",
            &retained,
        );
    }
    if let Err(error) = verify_markers(&parent_file, len, "parent-after-mutation") {
        preserved(&format!("parent re-verification: {error}"), &retained);
    }
    println!("PASS clone mutation left the parent artifact unchanged");

    // Drop production readers so pins release before any removal.
    drop(parent_file);
    drop(parent);
    drop(branch_file);
    drop(branch);

    if args.cleanup {
        let mut failed = false;
        for artifact in [&clone_artifact, &artifact] {
            if let Err(error) = store.remove(artifact).await {
                eprintln!("cleanup remove {} failed: {error}", artifact.location);
                failed = true;
            }
        }
        if failed {
            preserved("cleanup remove failed", &retained);
        }
        println!("PASS cleanup removed both artifacts through production remove");
    } else {
        println!(
            "artifacts retained for inspection (A0 default): {}",
            retained.join(", ")
        );
    }
    println!("PROBE-RESULT=PASS");
}
