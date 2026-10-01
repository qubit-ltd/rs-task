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

可选的 `event-bus` 集成和 Redis provider fixture 演示 `TaskEvent` 传输与消费者处理。目前 typed execution service 尚未接入 typed 任务生命周期事件发布；消费者应以任务查询接口返回的记录为准。

更多背景见[详细设计](task_execution_service_design.md)、[迁移指南](migration-0.8.zh_CN.md)和 [API 文档](https://docs.rs/qubit-task)。
