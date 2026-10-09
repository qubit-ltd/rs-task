# 迁移到 qubit-task 0.10

[English](migration.md) · [用户指南](user-guide.zh_CN.md) · [带类型任务 API](typed-task-api.zh_CN.md)

本文是 `qubit-task` 0.10 带类型任务 API 的当前迁移指南。旧的 UUID 字节接口不适用于本文中的迁移步骤。从 0.8 升级时，可查看[0.8 历史说明](migration-0.8.zh_CN.md)了解当时的变化；其中的示例和 API 名称不能直接作为 0.10 用法，请以本文为准。

## 更换 SQLite 构造入口

带类型 SQLite 存储使用 `SqliteTaskStore::open` 打开：

```rust,no_run
use qubit_task::store::SqliteTaskStore;

let store = SqliteTaskStore::open("tasks.sqlite")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

将 `SqliteTaskStore::open_next` 替换为 `SqliteTaskStore::open`。0.10 只提供一个带类型的 SQLite 入口；不要保留旧 UUID 存储作为回退路径。打开 typed schema v4 或 v5 时，程序会在事务中迁移到 v6，并保留任务记录。旧 UUID schema 会被明确拒绝，原数据库保持不变。请先由应用控制迁移过程，将旧记录映射到新的带类型数据库，再使用 0.10 打开。

## 实现当前 `TaskStore` 契约

公开的 `TaskStore` 是带类型的持久化契约，由 `MemoryTaskStore` 和 `SqliteTaskStore` 直接实现。自定义实现应使用当前 `qubit_task::model` 类型，并按所实现的方法提供带类型任务接纳、任务和摘要读取、带预期状态版本的生命周期迁移、进度更新、keyset 历史查询、owner fence 与终态清理。移除依赖旧 UUID 请求和恢复类型的适配层；`LegacyTaskStore`、`RecoveryPage` 和 `scan_unfinished` 不属于 0.10 公开契约。

任务身份使用 `TaskId`，处理器路由使用 `kind_id`，应用筛选使用 `category`；`Payload<T>` 负责绑定带类型 payload 的 `type_id`、`schema_version` 和 `codec_id`。历史分页采用排他 keyset 游标，顺序为 `(accepted_at_ms, numeric TaskId)`，不是 offset。完整流程和当前方法说明见[带类型任务 API 指南](typed-task-api.zh_CN.md)。

## 保留通知投递边界

启用生命周期通知时，SQLite 会在同一事务中写入任务状态迁移和 outbox 行。provider 确认接纳后，publisher 才会删除对应行。投递语义为至少一次：结果不确定，或 provider 已接纳但进程在删除行前崩溃，都可能造成重复。消费者应按 `(TaskId, state_version)` 去重，并通过任务服务查询权威状态。这不保证恰好一次投递，也不证明已接纳的事件已经落盘、复制或被消费者处理。

持久 outbox 由 `SqliteTaskStore` 提供；`MemoryTaskStore` 不支持。启用 publisher 不会补发启用前提交的生命周期状态。provider 要求、消费者 checkpoint、监控和关闭行为见[发布生命周期变化](user-guide.zh_CN.md#发布任务生命周期变化)。

## 验证应用迁移

请同时更新应用代码、自定义 store、示例和 lockfile，并针对 0.10 公开 API 编译。至少核对：带类型任务提交和处理器查找、带版本检查的状态更新、keyset 分页、迁移后 SQLite 数据库重开、旧 UUID 数据库被拒绝且内容不变，以及启用通知时消费者能够安全处理重复事件。不要把 0.8 历史文档中的 API 示例当作 0.10 用法。
