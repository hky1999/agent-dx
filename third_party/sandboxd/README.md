# sandboxd 协议

- 上游仓库为 https://github.com/inclusionAI/sandboxd，原协议基线为第 56 号拉取请求的提交 `efc201531d7e2e9d69505da151eb66084b61eebf`。
- 调度器的协议扩展匹配独立仓库 `../sandboxd`（相对于 agent-dx 仓库根目录）中的维护源码。当前版本和完整提交登记在外层仓库 `VERSION.md`；本协议副本与服务端文件逐字一致，不能混用旧部署二进制。
- 服务端协议路径为 `api/runtime/v1/sandbox-api.proto`。Apache-2.0 许可证保留；本协议副本含下述调度器扩展，不是上游原件的逐字复制。

`StartResponse.sandbox_ip` 使用字段 4，服务端分配的 `resource_generation` 使用字段 5。adxlet 从本目录协议生成客户端，使用 `CheckpointIfGeneration` 和 `DeleteIfGeneration`，没有无条件修改的回退。检查点请求还携带字段 7／8，即 `checkpoint_id`、`include_filesystem`；检查点回复必须明确确认完成。

删除完成要求 `retired_generation` 匹配预期物理代际，再读取库存确认该执行已经不存在。状态缺失、RPC 状态码或库存暂时为空，均不能单独确认一个中断的 Start 已经结束。创建结果未知时保留预算，通过执行标签发现的迟到沙箱只清理。物理资源代际独立于 agent-dx 的归属 generation，重启后必须从守护进程分配的标签中恢复。

节点二进制要求 `sandboxd_operation_journal`，在 RPC 派发前保存操作意图，并保存未知状态、完整请求摘要及已知物理身份。创建使用独立 `StartOperation`，没有旧 `Start` 的回退；`akernel.scheduler/start-operation-id` 标签关联迟到执行。节点严格核对结果的操作 ID、指纹和原截止时间。成功创建的回复丢失且库存暂时为空时，可以通过 `GetStartOperation` 找回原始物理身份，再取得对应代际的条件删除回执；历史成功结果不能使失败创建重新进入运行状态。

新创建意图的 kind 为 `start_operation`。只有这些操作的查询允许 `seal_missing`：服务端在原截止时间后持久拒绝从未准入的迟到请求，并确认物理检查，节点才能保存 `NotCreated` 并在库存核对后结算。既有 kind 为 `start` 的旧协议未知操作不设置该标志；空库存仍然不能结算。删除回复丢失后继续保留原物理代际以回放退役回执。节点日志不替代 Coordinator 的预算权威。

协议副本还包含完整 Stats、资源调整及管理恢复类型，生成这些类型不代表节点已经接通对应能力。统一持久预留、完整 Stats 已知标志、资源条件扩缩容、失败创建的完整恢复对账及物理故障验收仍待完成。构建与物理测试必须分别记录匹配的守护进程提交，以及客户端和服务端协议身份。
