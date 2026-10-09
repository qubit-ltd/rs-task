# 用户指南

[English](user-guide.md) · [0.10 迁移指南](migration.zh_CN.md) · [带类型任务 API](typed-task-api.zh_CN.md)

`qubit-task` 接受带类型和版本的 payload，并将任务交给已注册的处理器执行。本指南以 typed API 为准；旧的字节数组 `TaskRequest`、按 `(task_type, handler_version)` 路由、旧查询示例和闭包式服务 API 已不再是公开契约。

需要持久化任务历史时，使用 `SqliteTaskStore::open(path)` 打开 SQLite 后端。它会将 typed schema v4/v5 迁移到 v6 并保留现有任务记录；旧 UUID schema 会被拒绝且不会被改写。升级已有数据库前请先阅读[0.10 迁移指南](migration.zh_CN.md)。

## 从这里开始

完整提交与处理器示例见[带类型任务 API 指南](typed-task-api.zh_CN.md)。其中介绍 `TaskId`、`kind_id`、`category`、`Payload<T>`、codec 注册、schema 兼容性、metadata 配额、资源准入、取消、异步进度，以及历史分页的排序契约。

可运行示例见 [`examples/task_service.rs`](../examples/task_service.rs) 和 [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs)。前者演示处理器注册与提交；后者演示带过滤条件的历史查询和运维处理。

## 核心概念

- **任务标识：** 每个已受理任务都有一个由 `TaskId` 包装的 `rs-id::Id`。注入 `IdGenerator`；跨进程使用 Snowflake 生成器时，各进程必须使用不同节点编号，并配置合适的时钟条件。
- **路由与分类：** `kind_id` 选择处理器，`category` 是独立的业务分类，用于查询过滤。
- **Payload 兼容性：** `Payload<T>` 将 `type_id`、`schema_version` 和 `codec_id` 与值绑定。一个处理器只接受一种 payload type ID，并显式声明支持的 schema 版本。一个 codec 可以服务多个 schema 版本。
- **资源准入：** CPU、GPU、内存、磁盘和自定义单位是并发配额，由执行引擎内部预留；它们不会固定 CPU、隔离 GPU，也不会强制限制操作系统层面的内存或磁盘使用。
- **生命周期与取消：** 排队中的任务可直接取消。运行中的处理器必须声明协作式取消能力或外部取消 hook；取消请求本身无法强行停止任意代码。外部 hook 失败会保留可查询诊断，任务仍在运行时可再次调用 `cancel()` 重试。同一 attempt 的并发调用共享一次 hook，取消调用方不会停止 hook，`shutdown()` 会等待 hook 完成。hook 应对每个 (`TaskId`, attempt) 幂等。
- **进度：** 处理器通过 `rs-progress::AsyncReporter` 汇报进度。`report_async()` 等待持久化完成，之后读取任务会包含阶段和指标快照。
- **历史：** typed 分页按 `(accepted_at_ms, numeric task id)` 升序排列。排他性的 `after` 游标是 keyset 游标；每次查询读取各自的存储快照。

## Features 与边界

`sqlite` feature 提供持久化任务历史与恢复。恢复采用至少一次语义：若任务在外部副作用完成后、终态写入前中断，恢复后可能再次运行，因此应用副作用需要幂等性或事务保护。服务只在单进程内调度，不提供分布式调度或业务副作用恰好一次保证。

## 发布任务生命周期变化

需要向外发送生命周期通知时，启用 `event-bus` feature，并将 `Arc<AsyncEventBus>` 传给 `TaskExecutionServiceBuilder::event_bus`。应用还需在 bus 上注册自己的 `TaskEvent` codec。此功能要求 store 支持持久 outbox，并要求总线 provider 同时声明 `DurabilityCapability::Durable` 及至少 `PublishGuarantee::Accepted` 的成功发布保证。`SqliteTaskStore` 提供前一项能力，`MemoryTaskStore` 会在构建服务时返回 `UnsupportedCapability`；内置 local 总线是易失性的，不能用于此 publisher。不配置 `event_bus` 时，服务不会记录生命周期通知。

创建总线时明确要求持久保留消息及接纳保证。下面的启动片段假设 `facade` 已注册 `TaskEvent` codec，应用也已链接 Redis provider crate：

```rust
use std::sync::Arc;

use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::RequiredCapabilities;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_spi::ProviderSelection;

let config = EventBusConfig::default()
    .with_selection(ProviderSelection::named("redis-streams")?)
    .with_required_capabilities(
        RequiredCapabilities::new()
            .durable()
            .with_publish_guarantee(PublishGuarantee::Accepted),
    )
    .with_facade_config(facade);
let bus = Arc::new(AsyncEventBusRegistry::discover()?.create(&config).await?);
let service = builder.event_bus(bus.clone()).build().await?;
```

[可编译的装配示例](../tests/fixtures/doc-examples/src/main.rs)还包含 provider 配置和任务处理器。registry 会在创建总线时检查要求；任务服务也会在启用 outbox 前独立核对注入 facade 中缓存的持久性和发布保证，然后通过 facade 的 `check_publish_codec` 检查 `task.lifecycle` codec。易失性 provider 返回 `NotificationProviderNotDurable`；声明 `FireAndForget` 的持久 provider 返回 `NotificationProviderInsufficientGuarantee`；缺少 codec 返回 `NotificationCodecUnavailable`。这些失败不会启动发布或启用 outbox。codec 检查只确认 facade 配置中已注册 codec，并不能证明每个事件都能成功编码，也不能证明 Redis 可连接或已持久化。

publisher 启用后，每次生命周期状态提交都会与对应 outbox 行在同一个 SQLite 事务中写入。后台 worker 按稳定顺序将事件发往 `task.lifecycle`，确认接纳后再删除 outbox 行。服务启动时会重放已有待发事件；启用 outbox 之前提交的状态不会补发。投递语义为至少一次：发布结果不确定，或 bus 已接纳事件但进程在 SQLite 删除记录前崩溃，都可能产生重复。Redis `XADD` 成功表示 stream 已接纳记录，不能据此判断磁盘是否完成 `fsync`、副本是否收到记录，更不表示消费者已 ACK。`DurabilityCapability::Durable` 仅说明无订阅者时仍保留消息；`PublishGuarantee::Accepted` 描述 provider 的接纳边界，两者都不承诺后续阶段。Redis 回复接纳后若在持久化前故障，已从 SQLite outbox 删除的通知仍可能丢失。消费者应为每个 `TaskId` 保存最高 `state_version`，忽略重复和旧版本；发现版本缺口时查询任务服务。事件只用于通知，任务查询才是权威状态。

### 持久化消费者投影

在以 `TaskId` 为键的持久表中保存每个任务已处理的最高 `state_version` 和业务投影状态。一个 SQLite 事务内先读取 checkpoint，再决定是否修改业务投影。事件版本不高于 checkpoint 时直接忽略；首次收到版本 0 可以直接应用，首次收到高于 0 的版本，或后续事件跳过一个及以上版本时，必须调用 `TaskExecutionService::get` 查询权威状态和版本。查询失败或服务版本低于通知版本时终止事务。将选定的状态、checkpoint 与业务副作用写在同一事务中，最后提交。`tests/redis_task_outbox_tests.rs` 中的集成回归会关闭并重建 SQLite 消费者，再重放 Redis 中的重复记录。

只有事务提交成功后，handler 才返回成功并由 bus ACK。事务或服务查询失败时，返回配置为重新入队（`FailureDirective::Requeue`）的 handler 错误，让 Redis 消息留在 pending 状态等待 Retry。不能先 ACK 再补写 checkpoint。publisher 产生的稳定 `EventId` 仅供关联重复 wire 记录；Redis provider 不会按 `EventId` 去重。跨进程重启防止业务副作用重复依靠消费者的持久任务版本检查。

关闭时 worker 会持续排空，直到 outbox 清空或 `notification_shutdown_timeout` 到期。超时会返回错误，尚未发送的行仍保留在 SQLite，供下次启动重放。运维时应监控 SQLite outbox 行数和最老行年龄，并结合 Redis stream 的 `XLEN` 与 consumer group 的 `XPENDING`，区分 publisher 堵塞、stream 积压和消费者未确认等情况。

上线前和 Redis 故障转移后，还应根据部署恢复目标检查 `INFO persistence` 和 `INFO replication`，并查看 `XLEN <stream-key>`、`XINFO GROUPS <stream-key>`、`XPENDING <stream-key> <group>`（必要时翻页检查 pending 记录）。这些观察项有助于区分 Redis 持久化与副本状态、stream 增长、消费组进度和 consumer 恢复；不要脱离实际负载与恢复要求套用固定阈值。`check_publish_codec` 用于检查 facade 是否已为 `task.lifecycle` 注册 codec；`NotificationCodecUnavailable` 表示尚未注册。该就绪检查只证明 facade 配置，不证明每条事件都能成功编码、Redis 可连接、已接纳的 `XADD` 已 fsync 或复制，也不证明 consumer 已提交业务事务。命令说明和 provider 的持久性边界见 [Redis 部署就绪检查表](https://github.com/qubit-ltd/rs-event-bus-redis/blob/dev-starfish/doc/user_guide.zh_CN.md)。

`notification_stats()` 返回当前进程内排队、已发布和失败次数，不代表持久 backlog。可直接查询 SQLite 获取待处理行数和最老行年龄：

```sql
SELECT COUNT(*) AS pending,
       CASE WHEN MIN(created_at_ms) IS NULL THEN 0
            ELSE CAST(strftime('%s', 'now') AS INTEGER) * 1000 - MIN(created_at_ms)
       END AS oldest_age_ms
FROM task_event_outbox;
```

更多背景见[详细设计](task_execution_service_design.md)、[迁移指南](migration.zh_CN.md)和 [API 文档](https://docs.rs/qubit-task)。
