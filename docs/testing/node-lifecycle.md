# adxlet 生命周期与降级

adxlet 拥有实例串行状态机。Coordinator 提供归属校验、Redis 持久化和路由发布；现存实例的生命周期请求由 API Server 缓存命中后直达节点。

## 运行策略

`EnvironmentSpec.lifecycle` 包含 `idle_timeout_seconds` 和可选 `restart`。HTTP 保留 `idleTimeoutSeconds`，新增 `restartPolicy`；Python SDK 支持 `RestartPolicy(max_attempts=3, initial_backoff_seconds=1, max_backoff_seconds=30)`。

- 空闲超时为 0 时不回收。启用后，运行实例的 Execd 活跃客户端请求数与 Relay 流计数必须同时为 0，且活动版本在整个空闲窗口内未改变。后台命令仍计入 Execd 状态与活动版本，但客户端退出后不会仅因命令仍运行而阻止回收。采样失败或活动版本改变会重置窗口。adxlet 执行删除。
- RuntimeDriver 确认执行退出后，节点先退役路由、确认旧执行清理，再标记 Failed。启用重启时，按照指数退避和累计重试上限发布清理后的源记录，再申请本机资源及中心持久授权，并分配新的执行 ID，Environment ID 与归属代次保持。
- `restart_attempts` 和 `restart_pending` 随实例结果写入 Redis／降级日志。进程重启不重置次数。显式删除取消重启。清理未确认时保留资源，禁止启动替代执行。
- 可配置 `execd_health_failure_threshold`；省略时不以健康探测失败触发重启。连续失败达到阈值后，仍须先确认旧执行清理。健康检查只覆盖 Execd。
- 初次 Start 失败、超时或应答丢失均判定创建 Failed；不会触发运行中退出的自动重启策略。节点先退役路由并要求 RuntimeDriver 确认旧执行及迟到启动不再存在，再归还标量和设备预留并提交结果。清理失败或超时保留预留，Failed 结果也保留 `resources_held=true`。Failed 控制器继续重试清理，确认退役后提高 revision 并提交预算释放结果，不转成 Running。节点重启按权威目录对账。

## 资源与指标

`EnvironmentSpec.resources` 保存请求值，`sandbox.limits` 保存不可变允许上限；上限为零时采用对应请求值。默认固定资源配置使用 `EnvironmentSpec.execution_ceiling()` 的完整额度进行创建和预留。

内部 Environment 协议可显式配置 `sandbox.elastic_memory={"high_per_mille":750}`。此时 `execution_resources()` 的内存为请求值，作为出生额度和缩容下限；CPU 和磁盘仍使用完整上限。仅支持 runsc，请求和允许上限必须为整数 MiB，比例必须大于 0 且小于 1000，出生软限制至少为 4096 字节，CPU 上限至少为 10 毫核；数值超过 sandboxd 精度时拒绝。快照克隆暂不接受该选项。Start 写入 sandboxd 第一版弹性策略标签，CPU 的初值和上限相同，内存分别记录初值和允许上限；不通过修改原始规格记录后续额度。

恢复可能携带已经增长的内存上限，因此中心和节点在恢复前预留完整允许上限，Running 状态本身不能归还差额。Coordinator 从持久内存账户重建确认额度及待执行扩容额度；节点重启只有生命周期记录时保守保留允许上限。完整物理回读、节点内存账户同步和资源操作 RPC 尚未接通，节点保守预留尚不能自动收敛到实际额度。此选项当前没有公开 HTTP／Python SDK 入口，也不启用自动调整；期望的 memory.high 比例需要后续执行链路验证 sandboxd 配置。具体边界见 [宿主准入与执行授权](coordinator-storage.md)。

请求和磁盘上限均为零时仍不设置可写层配额，不能声称所有磁盘使用已经受预算约束。宿主共享预算已经有生产配置入口；独立内存授权的存储实现不等于物理动态资源或安全超卖已经验收。

`resource_source` 选择以下之一，也可保留既有 `capacity_file`。两者不能同时配置。

```json
{"kind":"sandboxd","socket":"/run/sandboxd/resource.sock","valid_for_seconds":30}
```

sandboxd 的 `/resource` 通过 Unix Socket 提供 CPU 核数、内存／存储字节数和整卡设备 ID；节点先验证 sandboxd 健康 RPC，再转换为调度器毫核单位。健康检查覆盖 sandboxd 内部资源刷新有效期。

```json
{"kind":"auto","disk_path":"/opt/adx/data","valid_for_seconds":30}
```

自动模式读取当前 Linux 进程的 CPU 可用并行度、cgroup v1/v2 层级限制、cpuset、内存上限与目标磁盘可用空间。它不虚构 GPU/NPU 清单；需要设备清单的部署选择 sandboxd 源或带设备信息的观测文件。

采集只更新容量，不重置已有资源预留。首次无有效样本时等待；后续失败沿用旧样本至其过期，过期后关闭新准入。`pressure` 提供内存和磁盘高／低阈值，任一达到高阈值关闭准入，两者都降到低阈值以下才恢复；压力采集失败也关闭准入。

配置 `metrics_listen` 后提供 `/metrics`。实例 CPU／内存经 RuntimeDriver Stats 定时采样，并携带采样年龄；资源预留和节点准入也作为指标导出。日志由部署环境收集。

## SQLite 降级契约

节点二进制还要求 `sandboxd_operation_journal`，示例为
`/opt/adx/data/operations/sandboxd.sqlite`。这是物理操作记录，不是第二份资源账本，
也不替代 Coordinator 的完整实例目录。Start、条件检查点和条件删除在派发前
同步保存操作 ID、完整请求的 SHA256、截止时间、执行状态及已知物理身份；
请求内容和环境变量值不入库。数据库使用 WAL 与 FULL 同步，文件权限为 0600，
同一路径仅允许一个节点进程持有写锁。

重启先恢复这些物理事实，再按 Coordinator 的完整目录重建本机占用和对账。
未知创建结果不会因空库存变成清理成功，迟到执行通过原 Start 操作标签匹配并
只用于清理。已知物理代际的删除回复丢失时，重启后仍用原物理身份查询条件退役
回执；回执匹配并确认库存不存在后才结算。日志损坏、记录缺失或写入失败均保留
未确认占用，不能删库、重置日志或重复创建来恢复服务。

这些记录保留当前事实与变化历史，重复相同事实不重复追加。历史的自动归档和
容量保留策略尚待实现。该机制不等于统一宿主预算、Redis 任意故障下的持久承诺，
也不提供 sandboxd 内部未知 Start 的最终结果查询；上述能力仍需独立开发验收。

`degradation_journal` 为可选本地 SQLite 路径。

1. 正常操作完成后，adxlet 经 Coordinator 提交结果。没有待补写日志时不创建本地数据库。
2. 只有 Coordinator 不可用才尝试 SQLite 落盘，WAL 使用 FULL 同步。明确的权限、归属和版本拒绝不会降级。落盘失败不会报告 Journaled。
3. SQLite 保存有序、去重的待补写结果，不保存完整实例目录。暂停后恢复等连续操作必须按序补写，不能只保留最后状态。
4. 本地操作已完成不等于集群可见。内部 RPC 返回 Journaled；公共暂停／恢复 API 返回暂不可用并说明集群发布待完成。Ingress 仍使用缓存路由，由 Relay 复核本机绑定。
5. 当前节点进程在已对账的状态下可继续处理删除、暂停和清理，并在心跳恢复后补写结果。恢复、重启等创建新执行的操作必须先发布原记录并取得 `AuthorizeExecution` 授权，不能降级为本地许可。补写成功后清理旧制品。
6. 节点进程重启后，先等 Coordinator 完整目录。日志记录仅在归属／请求规格匹配且版本更新时补写；已提交结果和退役归属被清除。补写后再次读取目录，再与真实 backend 对账，最后开放生命周期和新准入。
7. Coordinator 不可用时不能以 SQLite 缺记录为由清理或重建运行实例。SQLite 损坏也不能解释为空目录。

`JournalSink::authorize_execution` 先按序发布该实例的待补交结果，再调用中心；任何不可用都返回错误，不把授权请求追加为可执行的本地日志。

本地目录是降级可用性的条件；删除日志意味着丢失其中尚未进入 Redis 的结果。日志不是 Ingress 的发现或路由来源。

## 验证

Rust 用例位于 adxlet 的 `journal.rs`、`resources.rs`、`pause_resume.rs` 与 Coordinator 的 `storage.rs` 测试。真实 Redis 用例覆盖 Redis 进程中断、节点持久化组件重建、有序暂停／恢复补写与重启次数保护。

`build/e2e/firecracker/sdk_node_lifecycle.py` 在显式指定的 Linux 测试部署中通过公共 SDK 验证后端异常退出重启、Coordinator 失联空闲删除、SQLite 日志、节点进程重启等待、恢复补写和资源源断开。脚本中的 Redis／backend 查询是验收证据，生命周期正常操作仍使用公共 API。

后端首次启动使用独立的 `runtime_ready_timeout_seconds`（默认 120 秒）作为单轮就绪探测窗口。连接失败或一轮探测超时后，adxlet 按 `report_interval_seconds` 继续等待外部 sandboxd；等待期间不注册节点、不启动 RPC 服务，也不开放实例准入。配置错误仍立即退出。日常 `rpc_timeout_seconds` 只控制单次 RPC，避免后端资源初始化时间挤占日常故障降级预算。

## 本地验收（2026-09-16）

独立 Lima `adx-fc`，运行目录 `/var/lib/adx-pause-r7`，ARM64 原生 KVM + Firecracker，`package-v6`，源码包含未提交修改。继承同节点 checkpoint 5 项用例全部通过，新增 5 项真实运行时故障用例全部通过：

- 后端执行实例被删除后，按策略自动重启并使用新的执行身份。
- Coordinator 被暂停时，节点空闲删除写入 SQLite，Redis 保留旧状态，未冒充全局提交成功。
- 此时重启 adxlet，等待 Coordinator 对账，保留已有运行实例。
- Coordinator 恢复后，重放删除日志并恢复已有实例路由，SQLite 待同步记录归零。
- 资源采集 socket 暂时不可访问，超过有效期关闭准入；采集恢复后重新开放，已有实例继续运行。

同一轮最终目录中实例全部 Deleted，资源已释放，sandboxd 清单与 checkpoint 目录为空，服务正常停止。
证据位于 `out/ci/pause-resume/fc-r7/evidence/`，包括 `sdk/result.json`、`lifecycle/result.json`、`catalog-final.json`、`inventory-final.txt` 和 `result.json`；构建/部署完整日志位于 `out/ci/stage-2/{linux-2,package-2,fc-r7}.log`。回归日志为 `rust-5.log`（141 项测试，Clippy 通过）及 `clients-4.log`（Go 检查及 138 项 SDK 测试）。这些是本地证据，Kubernetes 验收尚未包含新增生命周期场景。

替换执行在申请中心授权之前，先在 sandboxd 适配器的持久日志记录 `Prepared` 目标指纹，不保存环境变量明文。未派发的目标可在重启后取消并结算；`Cancelled` 禁止同一 Runtime 迟到启动。缺失日志仍视为未知，不能证明执行不存在。实际 Start 保留独立的完整请求指纹、期限与物理代际日志。

授权回复未知时，节点停止新准入，心跳循环重新读取 Coordinator 的完整目录。对账排空旧控制器，清理精确的预留目标并发布终态。同一 Assignment 内替换执行只退役旧控制器，不给整个 Assignment 写退役标记。明确的容量拒绝保留源状态供重试。中断目标沿用既有 Failed 对账契约，不在自动对账期间悄悄重新执行。

`RuntimeDriver::execute_authorized` 接收完整授权和可选的已物化恢复输入。sandboxd 适配器在能力查询和请求准备后再次核对原期限，把较早的授权／本机 RPC 截止时间持久保存并传入 `StartOperation`，实际发送前再检查。已准备的替换目标不能经不带授权的普通 Start 入口启动。超时之前尚未派发的本机意图必须先持久封闭，才能按未创建结算；请求已经发出后仍按未知结果对账。
