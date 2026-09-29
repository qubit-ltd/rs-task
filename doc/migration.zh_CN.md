# 迁移到任务服务 0.8

[English](migration.md) · [用户手册](user-guide.zh_CN.md)

Task 0.8 使用 Event Bus 0.17 的公开类型代际。应用的 `qubit-task`、
`qubit-event-bus` 和可选 Redis provider 依赖须一起更新为 0.8、0.17、0.5，
包括文档/IoC/application fixture 和 lockfile。没有兼容别名桥接新旧 EventBus
类型。未启用通知的应用仍遵循现有任务存储、恢复和调度合同。

## 迁移通知 codec

`EventCodec<TaskEvent>` 由应用提供，任务库不提供全局 schema registry 或
公开 task codec。decode 改为接收 `&EncodedPayload`，以 `payload.bytes()`
读取字节。默认元数据验证精确比较 content type 和可选 schema。

[指南中参与编译的 JSON codec](user-guide.zh_CN.md#通过-redis-streams-发布)
写入 `application/json` 和 `task-event-v1`；仅明确允许同一 content type 下
的该 schema 或历史 `None`。未知 schema 或其他 MIME 返回 `MetadataMismatch`，
停止 facade 接收，持久消息不结算。启动时注册迁移后的 codec；修复不兼容
consumer 后，用同一 group 创建新持久订阅恢复旧工作。Redis wire 版本 1
仍可读。普通 JSON `CodecError::Decode` 仍拒绝坏消息，不会按 schema 不匹配保留。

facade 编码发布/接收默认各 1 MiB，`PayloadLimits` 参数必须为正数。
Redis 独立限制 wire 8 MiB、payload 1 MiB、headers 64 KiB。历史记录需要
更大容量时同时配置两层，不要删除 pending 记录掩盖超限。

## 观察未知效果，不重做任务状态迁移

通知失败仍不回滚任务状态。`publish_error` 统计发布失败，
`uncertain_publish` 统计其中效果为 `MayHaveBeenAccepted` 的通知，它们可能
已经到达 provider。累计分类上属于子集，但各字段通过独立原子读取获得，
发布进行中可能来自不同瞬间，快照不是原子分区，不保证当时观察到
`uncertain_publish <= publish_error`。两者都不证明 subscriber 完成。

核心公开错误为 `PublishFailure`，包含原 EventId、聚合效果和结构化原因。
默认 `DuplicateRiskPolicy::Forbid` 禁止盲重试未知效果，自定义规则不能绕过。
取消已启动发布时结果可能未知；RetryPolicy 是软预算，不保证中断所有执行中
I/O。Redis 不按 EventId 自动去重。

消费者为每个 TaskId 保留最高 `state_version`，忽略同版本重复和旧事件，
发现缺口后查询服务。并发状态变更不保证按版本递增到达。尽力通知可以缺失，
需要可靠移交时由业务在本库之外实现事务 outbox。不能因通知失败重做已提交
状态迁移。facade 死信转发与源确认也不是原子操作，逻辑死信同样需要去重。

部署前运行应用编译、通知未知效果、schema 兼容、状态版本收敛、Redis 恢复
和关闭测试。更早的任务 API 迁移仍见用户手册。
