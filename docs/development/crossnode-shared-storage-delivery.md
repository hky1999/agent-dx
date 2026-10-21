# CrossNode shared-storage source delivery

This experimental integration keeps Environment scheduling and lifecycle in ADX. It adds SharedFs checkpoint handling, persisted checkpoint-reader reservations and execution-bound retirement, and controlled shared-volume admission through the existing public Environment API/SDK chain.

The runtime implementation remains in sandboxd and gVisor. The vendored sandboxd protocol is pinned by `third_party/sandboxd/source.json`; changing the runtime source version must preserve its protocol checksum. No runtime source is copied into this control-plane repository.

`platform/adxlet/src/checkpoint/shared_fs.rs` and its leases module own shared-filesystem publication/materialization behavior. Coordinator reader and attachment records must survive process restart. A timeout or disappearance alone is not a positive retirement proof and cannot authorize collection.

Public shared-volume metadata and execution admission are separate from private checkpoint snapshots. Permissions and storage identity must be rechecked before start/restore. A live shared directory is not rolled back when restoring private memory/filesystem state. Existing mounts are not proven stopped by permission revocation alone.

The accepted experiment uses two isolated logical nodes on one host with independent JuiceFS clients. It does not establish physical cross-host migration or production storage HA. Runtime compatibility pins, source commits, SDK probes and paired performance data are published separately in `hky1999/AKernel_CR`.

The Coordinator shutdown/stream-cancellation candidate and unrelated AFS edits are excluded. Strict whole-workspace optional-AFS checks require their system dependencies; disclose unavailable gates rather than replacing them with component results.
