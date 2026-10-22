# ADX API Server

Rust public management HTTP service, binary `adx-apiserver`, in the root Cargo workspace. It owns API Key validation/cache, HTTP validation and compatibility responses, Environment ownership caching, immutable-target retries, snapshot/key endpoints and Agent route forwarding. Lifecycle state and scheduling stay in Adxlet and Coordinator.

Requests are converted directly to the generated Environment RPC types. There is no Go adapter, function payload, generic Signal dispatch or legacy protobuf dependency. Public Sandbox paths and base64 JSON response envelopes remain compatible with the Sandbox SDK. Existing `functionProxyId` in a resume response is a public compatibility field containing the node ID.

Sandbox management operations enter through `sandbox_service::SandboxService`; Platform remains the lifecycle authority. HTTP and embedded callers share `prepare_create`/`create_request` for Sandbox request validation, EnvironmentSpec conversion and deployment runtime injection. Agent-specific `CreateSandbox`/`ExecutionSpec` conversion lives in `adx_activator::sandbox_request`, shared by its HTTP and local adapters. The API Server implements the local `SandboxApplication` bridge over the same service `Arc`, without HTTP loopback. Ingress Sandbox v2 and Inline endpoints have been removed. Jiuwen downloads still use the old direct Platform runtime lookup pending a restricted API Server replacement; that lookup cannot read Execd credentials stored only in runtime_profile.env, so live download validation is required before deploying this refactor.

The API Server also owns the legacy-compatible scheduler HTTP paths while
Coordinator owns their state and decisions. `/global-scheduler/resources` reads the
watched in-memory node directory and renders the legacy `resource.fragment`
shape; `/api/sandbox/v1/resources` keeps the ADX `items` shape. Administrator-only
`/global-scheduler/scheduling_queue` reads the bounded, memory-only central
wait queue. `POST /global-scheduler/node/localschedulingstatus?node_id=...`
pauses new allocation to a node and `DELETE` removes that pause. Existing
Environments keep running. The pause is persisted by Coordinator and is not cleared by
Adxlet heartbeats; local pressure or reconciliation can still keep a
resumed node closed.
API Server performs the administrator check before either internal RPC. Coordinator
does not repeat user or component authorization so trusted internal controllers
can use the same operations directly.
两个资源接口保留节点分片的 CPU 毫核、Memory／Disk MiB 和整卡 GPU／NPU 字段。关闭准入的在线节点保留身份、标签及状态 1，调度资源显示为零。

配置可信宿主预算时，两个接口另外发布 `host_budget_configured` 和 `host_budgets`。标准接口位于顶层，兼容接口位于 `resource` 内。每个宿主包含 `id`、完整成员 `nodes` 及 `capacity`、`held`、`available`、`allocatable`；其标量字段使用精确的 `cpu_millis`、`memory_bytes`、`disk_bytes`。`held` 包括未知创建和待恢复预留；节点离线不会将其归零。`available` 是持久剩余额度，`allocatable` 是它与在线成员可调度资源之和的逐项最小值，只代表上界，实际申请仍需原子准入。

兼容接口的集群 CPU、内存和磁盘 `capacity` 按宿主各计一次，包括暂时离线的宿主；新增 `held`、`available` 保留兼容资源格式。兼容格式的 MiB 值向下取整，精确守恒核对使用宿主字节字段。节点分片标记 `host_id`，同宿主分片不能相加当作独立容量。未配置宿主预算时保留原节点汇总，并将集群 `held`、`available` 标为 null。GPU／NPU 仍是节点整卡视图，不属于本轮宿主标量预算。

Coordinator 每次从持久账本读取完整预算。存储失败、非法视图或缓存过期使资源查询返回不可用，不显示为预算归还。两端协议及程序须匹配部署；本模块测试不能替代实际节点和资源回读验收。

## Modules

| Module | Responsibility |
|---|---|
| `contract.rs` | Typed public JSON validation, resources, environment and affinity conversion |
| `http.rs` | REST routes, authentication boundary, JSON/SSE responses and Agent compatibility forwarding |
| `sandbox_service.rs` | Sandbox lifecycle semantics, create replay handling and durable-result checks |
| `activator.rs` | Composition bridge implementing Activator’s local Sandbox application interface; no Agent execution-spec conversion |
| `clients.rs` | mTLS RPC clients, Redis discovery, API Key validation and ownership reads |
| `ownership.rs` | Bounded LRU caches with fixed expiration |
| `operations.rs` | Pause/resume/delete/snapshot, pinned assignment/revision, durable-result validation |
| `ingress.rs` | Default in-process Ingress lifecycle; same service as the standalone binary |
| `config.rs`, `main.rs` | Configuration, HTTPS or literal-loopback HTTP, graceful shutdown |

```bash
cargo test --locked -p adx-apiserver -j 2
cargo build --locked -p adx-apiserver -j 2
./target/debug/adx-apiserver --config /path/to/api.json
```

The unified deployment role is `apiserver`. It embeds Ingress by default while preserving separate API Server and Ingress TLS identities and listeners. Set `ingress_mode: standalone` to run `adx-ingress` as a second process. Both modes use `gateway::ingress::IngressService`. Certificate identities trusted by Coordinator and Adxlet use `apiserver`. Certificates are supplied by deployment and loaded on process start. A TLS identity is still required for internal RPC when the public HTTP listener uses `loopback_http`.

The API always uses the verified tenant, not tenant fields supplied in a request. Existing-environment operations use the local ownership directory. Reload first queries Coordinator for the latest committed record, so a just-completed workload checkpoint does not depend on its directory update arriving first; lookup failure prevents reload execution. Ordinary directory misses do not trigger an authoritative lookup. A successful create is returned only after the cache contains the same assignment generation and at least the published Running revision; an older directory event is refreshed before the response. A retry preserves the original assignment and lifecycle revision, and rejects a changed ownership generation. Only Published results are acknowledged as successful; SQLite Journaled results remain unavailable until publication.

The API Server's legacy `/api/agent` compatibility routes forward to `agent_address` when configured and preserve streaming responses. The separate Agent v2 entrypoint in embedded Ingress may call an in-process Activator; Agent authorization and product rules remain in the Agent layer.

See [HTTP contract](docs/sandbox-lifecycle-api.md), [failure behavior](docs/sandbox-runtime-failure.md), [deployment](../../docs/testing/process-deployment.md), and [Rust migration status](../../docs/testing/rust-apiserver.md).

压力停车的暂停请求可以携带source对象，字段为runtimeId、assignmentGeneration及revision。该对象要求完整且只能用于pause；公开入口在派发前和再次读取归属后核对源运行实例及版本。重复完成操作只接受相同请求ID及原版本。不匹配返回冲突，不切换目标。带source的pause不进行网关内部未知结果重试，调用方必须持久对账；sandboxd仍执行自身resource generation条件检查及精确退役。原普通SDK接口可以省略source，但受控停车编排必须显式提供。
