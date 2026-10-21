# Shared-filesystem checkpoint reader contract

This document describes the A1 candidate implementation. It is not production
acceptance: automatic shared checkpoint reclamation remains disabled until the
durable-reference and runtime-restart fault matrix has passed.

## Ownership

Coordinator snapshot references and Environment records remain authoritative.
Filesystem locks supplement those decisions; they are not another catalog and
do not authorize removal on their own. A process dying releases its file locks,
not its durable runtime dependencies. A node must recover the complete catalog,
reconcile sandboxd inventory and reacquire needed guards before reopening its
lifecycle gate. Unknown state must retain data.

## Files and ordering

`SharedFsCheckpointStore` creates a fresh `.staging-<uuid>` directory and registers
`.leases/<uuid>` plus `.leases/<uuid>.identity` before returning the staging path.
The identity file records the lock inode in `adx-shared-lease-v1 <inode>\n`
format. The pair and its containing directory are synchronized before allocation
succeeds. Shared staging and final `<uuid>` locations use the same lock key.
JuiceFS clients must report the same inode for that file; device numbers are
mount-local and are not compared across clients.

Restore acquires a shared nonblocking `flock` before inspecting checkpoint bytes.
The returned `MaterializedCheckpoint` retains its file descriptor until the
runtime no longer reads the artifact. Clone keeps the same source protection
through copying and publication of its independently registered destination.
Publication, retained-staging inspection and staged discard use the exclusive
lock. Committed artifact removal holds the exclusive lock through directory
deletion and parent synchronization.

An active conflicting lock returns `Conflict`. Invalid locations, symlinks,
changed directory identity, a replacement lock inode or a malformed registration
are rejected. Missing files and I/O failures do not trigger lock reconstruction.
Removal of an already absent artifact succeeds after path and directory-identity
validation, allowing a collector to retry after a lost acknowledgement.

Lock files and their registrations are not unlinked after artifact removal.
Recreating a same-named file could otherwise establish two independent locks.
Only fresh staging allocation initializes a pair, using exclusive file creation;
readers, removers and retrying publishers never repair one.

## Rollout and historical artifacts

The earlier experimental A0 shared adapter did not register leases and ignored
other clients' readers. Mixed A0/A1 writers and removers are not a safe rolling
upgrade. Drain existing users before replacing that experimental adapter.

Unregistered A0 shared artifacts are retained, but the candidate refuses to read
or delete them. There is no automatic registration/migration on first access:
it cannot distinguish a historical artifact from missing safety metadata.
Keep the original reader and data for recovery. A controlled migration, with a
complete authoritative inventory and all old readers drained, remains required
before claiming transparent historical-format upgrade support. This does not
change the separate local/object-store checkpoint readers.

Metadata damage, session loss, process death, database restoration and stale
participants still require durable lifecycle recovery tests. Passing local
filesystem lock tests alone is insufficient to enable automatic GC. Retained
lock-metadata growth also needs an explicit operational policy before production.
