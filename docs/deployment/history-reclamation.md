# Persistent history reclamation

The node agent adxlet in `platform/adxlet/` supports an explicit
`"history_reclamation": true` service configuration. The default is false.
This switch is independent of automatic memory control. Deploy matching
Coordinator, adxlet and sandboxd protocol implementations before enabling it.
The private sandboxd control socket remains a trusted control boundary.

## Authority and durable order

The authenticated `CoordinatorService.InspectNode` response can include a
`HistoryBoundary` from one atomic authoritative snapshot. It binds the Redis
control namespace, Coordinator epoch, allocation generation, catalog revision
and retained full Assignments, specification digests and minimum Runtime
revisions. `include_history` requests it during initial reconciliation;
`history_reclamation` requests a read-only refresh without marking the node
unreconciled. Only the registered node and its current session can request it.

adxlet installs the boundary before reconciling retained records. Every
30 seconds it schedules at most one independent history task. Under the
exclusive lifecycle gate it persists the boundary and a batch of at most
32 terminal operation records in SQLite before calling sandboxd
`CollectHistory`. Unknown results and referenced executions remain protected.
The daemon independently verifies original admissions and complete physical
retirement proofs, persists its fence and bounded deletion manifest, then
removes only matching historical files. Its reply acknowledges history
processing; it never proves physical retirement or releases budget.

A lost reply leaves the node pending batch durable. Reopening retries that
batch before selecting another. Capacity errors split the daemon requests
while retaining the complete node transaction. SQLite deletion and pending
completion commit together. Only after that commit are memory caches removed.
The original database and lock file identities must remain present; missing
or replaced files are errors and are never silently recreated.

Reconciliation may skip a reclaimed cleanup only when both the central and
local records already hold no resources, the durable boundary closes that
Runtime, no local current or pending operation remains, and fresh daemon
inventory confirms absence. This is log recovery; it cannot settle an unknown
resource holder. A stored physical retirement proof remains usable across a
crash before its final local state is recorded.

## Replay and storage boundaries

New start operation IDs are UUID version 8 values derived from the authority,
Environment ID, Runtime ID, Assignment generation and full request digest.
sandboxd independently recomputes this binding and verifies actual start
labels. Reclaimed IDs cannot be relabeled as new allocations. Existing legacy
admissions remain queryable; a missing legacy admission is refused once the
persistent fence exists. Missing history does not mean never created.

The authoritative budget header advances to schema 8 when history snapshots
or compacted memory history require it. Each active Runtime retains at most
64 completed memory operations, the current confirmation, any pending
operation, the closed source revision and a cooldown time watermark. A new
memory operation ID binds the entire source observation and revision. Old
IDs cannot acquire a new source after their detailed result is removed.

The node operation journal keeps at most eight audit transitions per Runtime;
terminal Runtime rows are removed only after the handshake. Its persistent
history schema is version 3. The daemon start index is version 2, with a
history sidecar bounded to 8 MiB, at most 32 pending operations and at most
4096 selected files. Original file/directory identity and SHA256 are checked
before unlinking. Reopening resumes the durable manifest. Missing or corrupt
fences and incompatible older storage readers fail closed.

These windows bound the specified histories. Current catalog entries,
referenced checkpoints, unresolved operations, revoked credentials and external
model responses retain their own owners and retention contracts. They are
not deleted by a history acknowledgment. Disk loss, a coordinated rollback to
an old backup and real host power loss require separate isolation and
durability guarantees. Turning the switch off does not erase an installed
persistent admission fence.

## Errors and validation

Regressing or equivocating boundaries, changed identities, changed requests
and reopening a closed allocation are conflicts. Missing or replaced local
state and incomplete acknowledgments are unavailable errors. Unknown physical
results retain their previous budget. A daemon manifest capacity rejection
does not publish an invalid sidecar and does not block unrelated new work.

Focused source checks include `cargo test -p adxlet --lib`,
`cargo test -p adxlet --test sandboxd_rpc`, and
`cargo test -p adx-coordinator --test storage
memory::completed_memory_history_is_bounded_and_forgotten_ids_cannot_change_source -- --exact`.
They cover lost acknowledgments, repeated reopening, catalog reconciliation,
bounded batches, durable retirement before final state, file identity,
closed revisions and unresolved growth. These component tests are distinct
from a matching deployment's physical fault acceptance.


## 缺失本地创建条目时的负向对账

节点重启可能收到已分配但尚未开始的Starting记录。本地SQLite缺少该Runtime的操作条目不能证明它从未创建。只有安装完整历史身份和持久操作日志时，节点才登记独立的`seal_runtime`负向查询意图，再调用sandboxd的`GetStartOperation`，同时设置`seal_missing`和`seal_runtime`。操作编号、完整历史身份、指纹和原查询期限先持久保存；中断后继续同一查询，不生成新期限或物理创建请求。

匹配的daemon在创建准入互斥锁内核对全部已登记事实，并要求节点物理准备和原生运行时库存完整且为空。已有同Runtime的其他操作、已登记事实缺失或损坏、未知库存或超过4096条检查上限时均拒绝。成功围栏以`runtime_fenced=true`和`NOT_CREATED`确认；该围栏跨daemon重启，拒绝该完整Runtime使用其他指纹的新建。它不删除或重分类任何原操作。旧daemon只返回按操作编号封禁时，新增确认默认为false，节点继续保留占用。

该接口只证明被封禁Runtime不能再开始物理执行。预算仍由节点后续正常生命周期结果提交与中央账目结算，围栏回执不能用于其他Assignment或Runtime。此能力要求匹配协议和程序；源码测试不替代目标部署的物理重启验收。


## 批次目录的不可逆封存

批次目录使用只读schema 2，普通控制器日志继续使用schema 1。具体候选的部署和物理验收范围以外层仓库版本台账及对应不可变证据为准。封存必须在原目录上按原owner和revision比较更新，保留全部原批次内容；唯一允许增加的是终态schema、下一revision和history_seal。不能创建一个不存在的目录来伪造历史前缀，也不能在封存时修改原归档摘要。

history_seal绑定index_sha256、through、owner_reservation、owner_sha256、contract_sha256和完整resources。中央在同一预算比较更新中核对新存储所有者仍持有对应资源、摘要相同且没有退役；旧批次必须全部sealed，没有未关闭子日志或未完成私有回收。只读目录的精确终态提交可以重放，任何后续修改或回到schema 1均拒绝。旧程序不支持schema 2，会关闭读取或写入，不能丢弃该边界继续使用旧目录。

封存仅证明元数据已进入不可逆终态，不直接证明物理库存为空，也不归还任何存储预算。外部控制器仍须取得新鲜完整物理负库存、原写锁和不可变文件索引，确认新所有权覆盖全部逻辑字节及分配块后，才能按原归属结算旧所有者。原权威目录和归档保留，后继目录引用原摘要并延续批次序号；不得通过重新initialize或提高文件数／字节上限取得容量。

## 可写日志与封闭目录的独立容量

中央协调器在同一预算比较更新中分别核对最多16条可写日志、最多64条封闭目录，以及封闭目录累计16MiB序列化元数据。累计字节按完整ParkingJournal记录计算，包含owner、revision和正文；每条正文原2MiB限制与元数据原持久预算保持。封闭目录不占可写日志名额，也不删除、缩写或改变原字节。

创建、更新及封存均先排除被替换的同名记录，再将拟提交记录计入一次；并发请求由同一中央比较更新串行裁决。任何数量或累计字节超限均返回NoCapacity，原记录与原可写来源保持不变。精确重放既有记录不重复计数；封存后的身份、所有权、revision和封印继续不可变。

本轮新增容量回归已在旧实现上实际运行并取得预期失败。上述容量实现仍是未部署候选，绿色质量门、候选构建、保留原目录身份的后继初始化恢复及目标环境物理验收尚未完成，不能据此宣布当前部署容量已改变。
