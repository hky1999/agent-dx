# Environment 暂停与恢复

本阶段实现同节点、同 Environment ID 的暂停／恢复。adxlet 持有实例状态机，Coordinator 负责提交结果和更新调度／路由视图；执行后端仍为外部 sandboxd。

## 正常路径

1. API Server 从本地缓存解析归属，携带已认证身份、operation ID 和预期状态版本直达 adxlet。
2. adxlet 串行执行本实例的请求。先确认后端支持 checkpoint，使用 Execd 控制接口准备 checkpoint（配置 runtime_control 时走双向 gRPC，否则走 HTTP），再关闭本机路由。
3. adxlet 使用 `CheckpointIfGeneration` 请求 sandboxd 完成 `leave_running=false` 的完整 checkpoint，携带服务端分配的物理资源代际。当前调度器分支携带固定扩展字段 `checkpoint_id` 与 `include_filesystem=true`，检查点身份取存储分配目录的最后一级名称；只有明确返回 `success=true` 才确认 RPC 完成。未确认完成的调用保持结果未知。随后通过 `DeleteIfGeneration` 取得同一物理代际的退役回执，再确认源执行库存为空，才能释放本机资源。

本地存储先预留空目录，sandboxd 则要求发布目标不存在，以便把已经同步的检查点整体原子发布。适配器调用前只移除这个空目录；已有文件、非目录、符号链接及含符号链接的父路径均拒绝，不能删除已有检查点来换取成功。父目录必须真实存在且使用规范绝对路径。后端仍独立检查授权根目录及已发布产物身份。
4. 存储后端完成制品持久化；adxlet 将 Paused 状态、恢复点和过期时间一次提交给 Coordinator。Coordinator 写入 Redis、释放 Shard 占用并撤销发布路由，API Server 才返回暂停成功。
5. 恢复时检查过期时间和制品，进行本机资源准入，通过 sandboxd 的 `Start.checkpoint_info` 恢复。后端自行生成物理 ID。
6. Execd 确认新执行身份、重新开放服务，本机绑定完成后，adxlet 提交 Running。Coordinator 增量恢复 Shard 资源占用、发布新路由。

同节点恢复不改变 ownership generation，execution ID 使用增加的实例状态版本区分，例如 `instance-7` → `instance-7-r5`。Execd 校验执行版本前进，Ingress 和 Relay 校验路由版本；旧执行不会借用恢复后的绑定。

物理资源代际来自 `StartResponse.resource_generation`，重启后可以从 sandboxd 的 `akernel.scheduler/resource-generation` 标签恢复。它独立于上述 ownership generation。旧物理 ID 被重建、代际缺失、退役回执不匹配、条件 RPC 不可用或仅返回 NotFound 时，清理都不确认成功；不会退回无条件删除或暂停。

## 模块边界

| 模块 | 职责 |
| --- | --- |
| `core::checkpoint` | 恢复点、制品引用、最近成功操作的持久化模型 |
| `adxlet::controller::lifecycle` | 串行暂停／恢复、资源释放与重新准入、失败回滚 |
| `CheckpointCooperation` | Execd 控制 prepare／未启动 checkpoint 的 abort（gRPC／HTTP 共用状态机） |
| `RuntimeDriver` | 能力检查、checkpoint、restore；sandboxd 适配器负责物理 ID 和 RPC |
| `CheckpointStore` | 分配暂存、发布、物化、删除与节点本地对账清理；实现本地目录与 S3 对象存储 |
| `coordinator::storage` / Shard | Redis 版本校验、恢复点保存、增量资源记账 |
| `sandbox-api/controlbackend` | HTTP 兼容格式转换、缓存归属、固定目标与版本的重试 |

`CheckpointStore::publish` 是存储完成边界。对象存储在此完成上传，通过 `materialize` 下载；本地/S3、缓存引用和跨节点恢复已实现，见 [存储契约](snapshot-storage.md) 与 [跨节点恢复](firecracker-cross-node.md)。

## 配置与部署

adxlet 配置增加可选的 `checkpoint_dir`，例如：

```json
{"checkpoint_dir": "/opt/adx/data/checkpoints"}
```

统一部署配置中填写到 `role: adxlet` 的 `config`。也可设置互斥的 `checkpoint_storage` 选择本地或 S3；两者均未设置时不启用暂停／恢复。使用维护的 sandboxd `faf0210b2feada672930e4caf99a11a519364055` 时，该目录必须位于 sandboxd 的 `rootDir/checkpoints` 下，且为真实目录。该目录必须是 adxlet 与 sandboxd 都可读写、以相同绝对路径访问的节点目录；本地制品需在 adxlet 重启后保留。不要将该目录用作其他组件的文件仓库。

sandboxd 必须支持 checkpoint／restore，并通过 runtime capabilities 提供 Execd 的 checkpoint handoff 和 restore environment 路径。adxlet 从能力信息注入这两个路径。Firecracker 的 Linux/KVM、guest kernel、initrd 和网络条件由运行环境提供；本阶段不会替部署环境启动 sandboxd。

暂停 TTL 控制恢复点过期，不是实例最大存活时间。节点每 30 秒扫描一次，清理动作与实例操作串行：暂停实例过期后删除；已经恢复运行的实例只清理过期制品。显式删除也清理本地恢复点。

## 失败与重试

- API/RPC 断线不会取消已进入实例队列的操作。API Server 自动重试保持同一 operation ID、归属和原始预期版本。
- 正常结果记录只保留最近成功操作。Node RPC 拒绝旧预期版本；客户端不能在一个后续操作已改变状态后，把旧请求换成新版本重新执行。
- checkpoint 已完成而 Coordinator 提交失败时，节点按 [SQLite 降级契约](node-lifecycle.md) 保存待补交结果。同一操作重试只补交；Journaled 不返回集群成功，重启后先等待 Coordinator 权威对账。
- checkpoint 调用前，准备失败可调用 Execd abort；调用后不再假设 checkpoint 未启动。无法确认停止／清理时保留资源占用并报告失败。
- 发布制品失败时，从完整本地暂存尝试恢复运行，并返回暂停失败。回滚或清理失败不会伪装成暂停成功。
- 恢复失败但清理成功时保留恢复点并释放此次资源；清理不确定时保留资源，等待处理。
- Start 的 RPC 错误和状态码不能证明服务端没有创建或仍在回滚。库存暂时为空时保留预算，迟到的执行只清理，不能激活或重复 Start。只有匹配物理代际的退役回执加后续库存确认，才能结束这个清理。本地调用前的配置／能力检查失败没有派发 Start，可以按未创建路径清理。当前没有持久 Start 结果查询，未出现且未获退役证明的未知结果可能持续占用预算；节点重启后的操作意图和预留重建尚待实现。
- 节点重启后，从 Coordinator 获取完整目录；Paused 记录不会被当作缺失的 Running 实例重建。未持久化且不匹配的执行按既有对账原则清理。
- 替换、过期和删除恢复点时，先提交新元数据，再清理旧制品。提交或清理失败保留待清理记录，后续同步或同操作重试继续完成清理。
- 重启对账期间关闭准入，确认所有执行和结果已对账发布后，清理本地存储根目录中不再被引用的 UUID 制品目录。Coordinator 不可用或对账失败时不执行该清理；共享存储的目录引用与旧会话孤儿清理见 [存储契约](snapshot-storage.md)。

## 验证

组件测试覆盖状态与资源转换、丢失提交回复、过期清理、恢复失败、回滚、真实 sandboxd 协议的 UDS 调用、Execd HTTP 身份切换和 Ingress 版本检查。Redis 集成测试使用独立真实 Redis。

```bash
cargo test -p adxlet -p adx-coordinator -p adx-core -p adx-protocol
cargo test -p adx-execd --test control_http
cargo test -p data-plane-gateway --test coordinator_routes
python3 build/ci/run.py storage --jobs 2 --output out/ci/checkpoint-storage
```

真实 Firecracker 公共 SDK 用例见 [sdk_checkpoint.py](../../build/e2e/firecracker/sdk_checkpoint.py)。用安装了本次发布包 SDK 的 Python 执行：

```bash
python build/e2e/firecracker/sdk_checkpoint.py \
  --endpoint 127.0.0.1:8443 --token-file /path/to/api-key \
  --ca /path/to/ca.pem --image registry/execd@sha256:... \
  --output /path/to/new-evidence-directory
```

用例检查创建、命令与文件、暂停、恢复后的内存计数器和 PID、二进制文件、删除。可通过 `--restart-command` 指定选定 adxlet 的重启及就绪检查脚本，验证暂停期间重启。进程重启脚本属于部署测试夹具，不通过 SDK 实现。

真实 FC 本地验收与 Kubernetes Buildkite 验收分开记录；组件测试通过不表示后者已通过。

本分支对检查点身份、可写文件标志、未确认成功、无身份目录、原子发布的空目录契约、非空目录保护、符号链接保护、创建未知结果的预算保留，以及物理资源代际条件 RPC 增加本地测试。条件删除和暂停已接入源码，条件扩缩容与完整 Stats 已知标志仍待接入。组件测试不证明候选已经部署，也不证明物理故障恢复安全；目标环境验收及持久预算恢复必须分别记录。
