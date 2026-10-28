# Environment template catalog

Environment templates are immutable, tenant-owned registrations of existing
reusable snapshots. They differ from Agent `TemplateVersion` and from node-local
seed references. This MVP implements import/get/list/delete and template-based
Environment creation; it does not build images, manage mutable aliases or export
an E2B-compatible API.

## HTTP contract

`GET /api/environment/v1/capabilities` advertises `environment-templates-v1`.
Clients must check it before template creation: older services may ignore unknown
create fields. Authentication, TLS, error envelopes and request IDs use the
existing Environment API contract.

| Method/path | Input | Result |
| --- | --- | --- |
| POST `/api/environment/v1/templates` | `templateId`, `snapshotId`, `name` | Ready registration; importing the identical record is idempotent |
| GET `/api/environment/v1/templates/{id}` | Opaque registration ID | `templateId`, `snapshotId`, `name`, `status=ready` |
| GET `/api/environment/v1/templates` | Optional `name`, `pageToken`, `pageSize` | `items`, `nextPageToken`; exact-name tenant filtering, page size 1..1000 |
| DELETE `/api/environment/v1/templates/{id}` | Registration ID | Empty result after unregister; repeated deletion of its tombstone succeeds |
| POST `/api/environment/v1/environments` | `templateId` plus ordinary restore options | A new Environment using the registered snapshot |

`templateId` and `snapshotId` in create are mutually exclusive. Template names
are display labels, not a lookup namespace. IDs use ASCII letters/digits, underscore and hyphen (1..128 bytes). The supplied
ID is immutable and globally unique; it cannot be retargeted or reused after deletion. Deleted and
foreign-tenant registrations are absent from get/list. Import rejects foreign
snapshots. The public gateway supplies the authenticated tenant, not the body.

Template creation resolves resource geometry and runtime from the snapshot and
reapplies its captured network policy. A different nonempty requested network
policy fails validation; omitted/empty policy does not clear the saved policy.
Existing direct `snapshotId` restore policy behavior remains unchanged. Named
creation retains existing reuse behavior; applying a template/policy is only
guaranteed on a newly created Environment, not on a returned existing identity. Local
snapshots retain source-node affinity, and all runtime/storage checks remain
authoritative at restore.

## Persistence and deletion

Coordinator owns the state. Registrations live in the Redis
`<prefix>:environment-templates` hash. Snapshot ownership uses
`Reference::CatalogTemplate { template_id }`; it does not invent a node identity
for the existing `Template { node_id, template_id }` reference.

Import and unregister CAS both the template record and the snapshot record in
one epoch-checked Redis Lua transaction. Snapshot pinning cannot be published
without metadata, and unregister cannot release a pin while leaving a Ready
registration. Deleted IDs remain tombstoned to prevent retargeting. This MVP
scans catalog records and performs tenant/name filtering before keyset paging;
it does not claim an indexed or point-in-time list at large catalog sizes.

New template admission and unregister use the same Coordinator state boundary.
Admission rechecks that the registration is Ready and still selects the same
snapshot, then acquires the persistent `Restore { environment_id }` reference
before releasing that boundary. After this handoff, unregister can release its
catalog pin without invalidating the child. A lookup followed by an unrelated
snapshot create is not used. Old epochs cannot write; a retry after restart must
revalidate the registration unless an existing durable Environment is replayed.

Delete unregisters only the template. Caller-owned snapshots and running children
remain alive; snapshot deletion keeps its existing `SNAPSHOT_IN_USE` protection
and asynchronous physical collection. Unregister is not a blanket revocation of
direct snapshot access for the same tenant.

## Deployment and verification

Build API Server, Coordinator and node components from the same source revision
for qualification. Catalog references extend the internal snapshot protocol;
mixed older nodes are not qualified for this feature. Runtime bundles and
checkpoint formats are unchanged by this catalog MVP. No default production
deployment, registry release or unrelated Environment is migrated by the tests.

Focused checks (debug regression uses a 16 MiB test-thread stack; the pinned
baseline also overflows the default stack in its restore/delete test):

```sh
RUST_MIN_STACK=16777216 cargo test -p adx-core -p adx-protocol
RUST_MIN_STACK=16777216 ADX_TEST_REDIS_SERVER=/path/to/redis-server \
  cargo test -p adx-coordinator --test templates --test snapshots -- --include-ignored
RUST_MIN_STACK=16777216 cargo test -p adx-apiserver
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Redis tests use private Unix sockets/AOF directories and real epoch/restart
recovery. AKernel's opt-in `test_templates.py` owns public SDK/CLI and real sandbox
integration validation; unit/component passes alone are not evidence of cluster
restore behavior or production performance.
