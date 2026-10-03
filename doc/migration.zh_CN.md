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

任务生命周期 `NotificationStats` 只统计当前进程：`queued` 表示已通知
publisher 的状态写入数，`published` 表示已接纳并从 SQLite 删除的事件数，
`failed` 表示 outbox 读取、发布或删除失败次数。由于已提交 outbox 行不会丢弃，`dropped`
始终为零。这些计数不会跨重启保留，也不表示 subscriber 已处理；持久积压
和最老行年龄应直接查询 SQLite。发布结果未知时保留 outbox 行并用稳定
EventId 重试，因此消费者可能收到重复事件。

核心公开错误为 `PublishFailure`，包含原 EventId、聚合效果和结构化原因。
默认 `DuplicateRiskPolicy::Forbid` 禁止盲重试未知效果，自定义规则不能绕过。
取消已启动发布时结果可能未知；RetryPolicy 是软预算，不保证中断所有执行中
I/O。Redis 不按 EventId 自动去重。

消费者为每个 TaskId 保留最高 `state_version`，忽略同版本重复和旧事件，
发现缺口后查询服务。并发状态变更不保证按版本递增到达。不能因通知失败
重做已提交状态迁移。facade 死信转发与源确认也不是原子操作，逻辑死信
同样需要去重。

## 启用持久化生命周期 outbox

启用 `event-bus` feature 后，配置 `TaskExecutionServiceBuilder::event_bus`，
并在传入的 `AsyncEventBus` 上注册应用提供的 `TaskEvent` codec。store 必须
实现持久 outbox 操作：`SqliteTaskStore` 支持，`MemoryTaskStore` 不支持；
使用后者构建服务会返回 `UnsupportedCapability`。

typed SQLite schema 版本 6 新增 `task_event_outbox`。打开版本 4 或 5 的
typed 数据库时会执行事务迁移，并保留原有任务记录。迁移不会生成历史
lifecycle 快照：只有服务启用 outbox 后发生的状态变化才会被记录，也不会
删除已有任务数据。旧 UUID schema 仍须先显式映射，才能由 typed API 打开。

生命周期状态写入与 outbox 插入处于同一个 SQLite 事务。后台 worker 异步
发布 outbox 行，只在 bus 接纳后删除记录，因此语义为至少一次：发布结果
不确定，或 bus 已接纳但进程在删除记录前崩溃，都可能造成重复。消费者应按
`(TaskId, state_version)` 去重，并通过任务服务查询权威状态。监控 outbox
行数和最老行年龄；使用 Redis Streams 时，还要检查 stream 的 `XLEN` 和
consumer group 的 `XPENDING`。关闭时会持续排空，直到队列清空或
`notification_shutdown_timeout` 到期；超时留下的行会在下次启动时重试。

部署前运行应用编译、通知未知效果、schema 兼容、状态版本收敛、Redis 恢复
和关闭测试。更早的任务 API 迁移仍见用户手册。
