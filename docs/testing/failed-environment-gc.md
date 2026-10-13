# Failed Environment 自动回收

Coordinator 自动回收终态 `Failed`，默认保留 600 秒。资源释放与元数据回收分开：节点已经派发创建的资源占用不会因标记 Failed 而立即归还。只有创建操作明确未开始，或者原物理执行具有匹配的完成回执，节点才能归还预留；超时、回复丢失、缺失身份及清理失败继续保留 `resources_held`。保留期用于诊断和元数据清理，不是原执行不存在或已退役的证明。

节点通过原创建操作 ID、完整请求指纹及 deadline 查询结果。新的持久创建协议在 PENDING 查询中提供完整原沙箱和资源代际时，节点先保存这一绑定，再按原代际派发条件删除；PENDING 本身不授权归还预算。删除意图和完成回执分别持久保存，回执代际不匹配、写入失败或原执行仍可观察时保持占用，重启继续使用原绑定和原请求。未知结果、缺失字段以及旧协议的 PENDING 不能借用当前库存补造清理身份，也不能回退到无条件删除。相关实现位于 `platform/adxlet/src/sandboxd.rs`，本地 RPC 和节点预算用例位于 `platform/adxlet/tests/sandboxd_rpc.rs`；匹配部署与物理故障仍需单独验收。

节点物理操作日志不代替中心预算账本；新建、扩容、恢复及同宿主多个节点的统一持久预算仍需独立实现和验收。

## 配置与范围

在部署 YAML 中的 Coordinator 服务配置设置：

```yaml
services:
  - id: coordinator
    role: coordinator
    config:
      # 与 listen、redis_url、tls 等其他 Coordinator 参数一起设置
      failed_retention_seconds: 600
```

该值必须大于零；省略时使用 600 秒。Coordinator 每 5 秒检查一次，每轮至多处理 16 条 Failed，按游标轮转，不扫描全部运行实例。保留期到达不等于立即消失：积压、节点清理或存储故障会延迟回收。

失败时间随 Failed 结果持久化到 Redis；相同失败的重试和 Coordinator 重启不重置保留期。旧记录没有失败时间时，从首次巡检开始计时。等待自动重启、正在跨节点恢复，或失效归属仍有有效共享恢复点的记录暂不回收；共享恢复点过期且未恢复后可进入回收。

## 清理与隔离契约

- 所属节点健康时，Coordinator 调用 adxlet 删除接口，携带归属 generation、节点会话和期望 Failed revision。adxlet 在该 Environment 的串行控制任务中复核状态；已恢复为 Running、待重启、修订号或会话不匹配时拒绝 GC。
- 节点先清理后端和恢复点，再发布 Deleted。清理或持久化失败保留记录并在后续巡检重试；不会因为保留期到达就忽略仍在线节点的清理失败。
- 节点归属已失效且原清理结果已经确认不再占用资源时，Coordinator 可以原子退役 Failed。失联本身、保留期到达和共享检查点存在均不能释放资源或删除仍占用的记录。原节点返回后按权威目录清理并提交原身份结果；旧 generation 不能再发布 Running。
- 成功后移除 Redis 主目录、Coordinator 调度目录以及在线 adxlet 控制任务。仅保留现有 600 秒最小删除回执，不保留完整 Failed／Deleted 记录。同名创建获得新 generation；迟到提交不能恢复旧归属。

GC 在 mTLS 模式下仅允许 Coordinator 服务身份调用受控删除；普通 API Server DELETE 的用户权限检查保持不变。network 模式遵循既有内部网络信任边界。

## 验证

组件用例使用真实 Redis 和 mTLS，覆盖在线清理、失败重试、同名重建、离线归属退役、迟到 Running 拒绝、失败年龄跨重试与重启保持。adxlet 生命周期用例覆盖 Running／待重启保护、revision fencing 和后端清理失败后重试。

```sh
python3 build/ci/run.py storage --jobs 2 --output out/ci/failed-gc-storage
python3 build/ci/run.py control-rpc --jobs 2 --output out/ci/failed-gc-rpc
```

上述是组件测试；实际 runtime 的 standalone 验证结果另见 [PR78 回归记录](pr78-runtime-regressions.md)。
