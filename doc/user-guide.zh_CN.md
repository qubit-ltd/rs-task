# 用户指南

`qubit-task` 接受带类型和版本的 payload，并将任务交给已注册的处理器执行。本指南以 typed API 为准；旧的字节数组 `TaskRequest`、按 `(task_type, handler_version)` 路由、旧查询示例和闭包式服务 API 已不再是公开契约。

## 从这里开始

完整提交与处理器示例见[带类型任务 API 指南](typed-task-api.zh_CN.md)。其中介绍 `TaskId`、`kind_id`、`category`、`Payload<T>`、codec 注册、schema 兼容性、metadata 配额、资源准入、取消、异步进度，以及历史分页的排序契约。

可运行示例见 [`examples/task_service.rs`](../examples/task_service.rs) 和 [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs)。前者演示处理器注册与提交；后者演示带过滤条件的历史查询和运维处理。

## 核心概念

- **任务标识：** 每个已受理任务都有一个由 `TaskId` 包装的 `rs-id::Id`。注入 `IdGenerator`；跨进程使用 Snowflake 生成器时，各进程必须使用不同节点编号，并配置合适的时钟条件。
- **路由与分类：** `kind_id` 选择处理器，`category` 是独立的业务分类，用于查询过滤。
- **Payload 兼容性：** `Payload<T>` 将 `type_id`、`schema_version` 和 `codec_id` 与值绑定。一个处理器只接受一种 payload type ID，并显式声明支持的 schema 版本。一个 codec 可以服务多个 schema 版本。
- **资源准入：** CPU、GPU、内存、磁盘和自定义单位是并发配额，由执行引擎内部预留；它们不会固定 CPU、隔离 GPU，也不会强制限制操作系统层面的内存或磁盘使用。
- **生命周期与取消：** 排队中的任务可直接取消。运行中的处理器必须声明协作式取消能力或外部取消 hook；取消请求本身无法强行停止任意代码。
- **进度：** 处理器通过 `rs-progress::AsyncReporter` 汇报进度。`report_async()` 等待持久化完成，之后读取任务会包含阶段和指标快照。
- **历史：** typed 分页按 `(accepted_at_ms, numeric task id)` 升序排列。排他性的 `after` 游标是 keyset 游标；每次查询读取各自的存储快照。

## Features 与边界

`sqlite` feature 提供持久化任务历史与恢复。恢复采用至少一次语义：若任务在外部副作用完成后、终态写入前中断，恢复后可能再次运行，因此应用副作用需要幂等性或事务保护。服务只在单进程内调度，不提供分布式调度或业务副作用恰好一次保证。

## 发布任务生命周期变化

需要向外发送生命周期通知时，启用 `event-bus` feature，并将 `Arc<AsyncEventBus>` 传给 `TaskExecutionServiceBuilder::event_bus`。应用还需在 bus 上注册自己的 `TaskEvent` codec。服务要求 store 支持持久 outbox：`SqliteTaskStore` 提供此能力，`MemoryTaskStore` 则会在构建服务时返回 `UnsupportedCapability`。不配置 `event_bus` 时，服务不会记录生命周期通知。

publisher 启用后，每次生命周期状态提交都会与对应 outbox 行在同一个 SQLite 事务中写入。后台 worker 按稳定顺序将事件发往 `task.lifecycle`，确认接纳后再删除 outbox 行。服务启动时会重放已有待发事件；启用 outbox 之前提交的状态不会补发。投递语义为至少一次：发布结果不确定，或 bus 已接纳事件但进程在 SQLite 删除记录前崩溃，都可能产生重复。消费者应为每个 `TaskId` 保存最高 `state_version`，忽略重复和旧版本；发现版本缺口时查询任务服务。事件只用于通知，任务查询才是权威状态。

关闭时 worker 会持续排空，直到 outbox 清空或 `notification_shutdown_timeout` 到期。超时会返回错误，尚未发送的行仍保留在 SQLite，供下次启动重放。运维时应监控 SQLite outbox 行数和最老行年龄，并结合 Redis stream 的 `XLEN` 与 consumer group 的 `XPENDING`，区分 publisher 堵塞、stream 积压和消费者未确认等情况。

更多背景见[详细设计](task_execution_service_design.md)、[迁移指南](migration.zh_CN.md)和 [API 文档](https://docs.rs/qubit-task)。
