# 迁移到 qubit-task 0.11

[English](migration.md) · [用户指南](user-guide.zh_CN.md) · [带类型任务 API](typed-task-api.zh_CN.md)

本文是 `qubit-task` 0.11 的当前迁移指南。旧的 UUID 字节接口不适用于本文中的迁移步骤。从 0.8 升级时，可查看[0.8 历史说明](migration-0.8.zh_CN.md)了解当时的变化；其中的示例和 API 名称不能直接作为当前用法，请以本文为准。

## 确认带类型请求契约

`TaskRequest::new(kind_id, schema_version, codec_id, data)` 构造方法及其 `T: HasModelId` 约束已存在于 0.10.0 源码（经提交 `c01e38e` 核对）。使用该构造方法的 0.10 应用升级到 0.11 时无需更改参数签名。请为每种 payload 类型定义稳定的模型身份，并确保已注册处理器的 `payload_type_id` 与之匹配：

```rust,no_run
use qubit_codec::ValueCodecId;
use qubit_model_id::{HasModelId, ModelId};
use qubit_task::model::TaskRequest;

struct Input;
impl HasModelId for Input {
    const MODEL_ID: ModelId = ModelId::new("example.Input");
}

let request = TaskRequest::new("example.kind", 1, ValueCodecId::new("example.codec"), Input);
# let _ = request;
```

`schema_version` 和 `codec_id` 仍由应用提供；模型身份来自 `T::MODEL_ID`。crate 版本 `0.11` 与 SQLite typed schema 版本 `8` 是两个独立版本号。

## 更换 SQLite 构造入口

带类型 SQLite 存储使用 `SqliteTaskStore::open` 打开：

```rust,no_run
use qubit_task::store::SqliteTaskStore;

let store = SqliteTaskStore::open("tasks.sqlite")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

如果仍在使用旧入口，请将 `SqliteTaskStore::open_next` 替换为 `SqliteTaskStore::open`。带类型 API 只有一个 SQLite 入口；不要保留旧 UUID 存储作为回退路径。打开 typed schema v4 到 v7 时，程序会在原数据库中通过事务迁移到 v8，保留任务、活动 outbox 和隔离归档记录，并新增 `task_terminal_checkpoints`。归档行保留原始事件数据，不会随终态任务清理删除。旧 UUID schema 会被明确拒绝，原数据库保持不变。请先由应用控制迁移过程，将旧记录映射到新的带类型数据库，再打开数据库。0.8 历史迁移说明仍作为历史文档保留，本次 schema 变更不会改写它。schema 版本 8 与 crate 版本 0.11 相互独立。

## 实现当前 `TaskStore` 契约

公开的 `TaskStore` 是带类型的持久化契约，由 `MemoryTaskStore` 和 `SqliteTaskStore` 直接实现。自定义实现应使用当前 `qubit_task::model` 类型，并按所实现的方法提供带类型任务接纳、任务和摘要读取、带预期状态版本的生命周期迁移、进度更新、keyset 历史查询、owner fence 与终态清理。outbox 隔离能力有默认实现，会返回 `UnsupportedCapability`；自定义 store 还必须实现必需的 `get_state_checkpoint` 方法，在权威状态不可用时返回 `None`；自定义 store 只有实现原子归档并删除操作后，才支持操作员隔离。移除依赖旧 UUID 请求和恢复类型的适配层；`LegacyTaskStore`、`RecoveryPage` 和 `scan_unfinished` 不属于当前公开契约。

任务身份使用 `TaskId`，处理器路由使用 `kind_id`，应用筛选使用 `category`；`Payload<T>` 负责绑定带类型 payload 的 `type_id`、`schema_version` 和 `codec_id`。历史分页采用排他 keyset 游标，顺序为 `(accepted_at_ms, numeric TaskId)`，不是 offset。注入的 ID generator 必须保证跨进程重启唯一：SQLite 会在终态检查点存在期间拒绝复用，`MemoryTaskStore` 清理后会释放 ID。清理后可复用幂等键，但不能因此复用原 `TaskId`。持久化重试期限时，正延迟会向上取整到整毫秒，最多增加不足 1 毫秒，但不代表实际墙钟等待保证。已有唯一 ID generator 的应用无需迁移；检查点仍存在时复用 ID 会得到 `DuplicateTaskId`，不会自动修复历史冲突。完整流程和当前方法说明见[带类型任务 API 指南](typed-task-api.zh_CN.md)。

## 保留通知投递边界

启用生命周期通知时，SQLite 会在同一事务中写入任务状态迁移和 outbox 行。provider 接纳前，记录会保留并重试，因此可能重复发布；收到 `Accepted` 后，outbox 行会被删除。该响应不保证 Redis 已持久化或消费者已收到通知，因此不承诺消费者至少一次收到通知。这与任务 handler 的至少一次执行恢复不同。消费者应按 `(TaskId, state_version)` 去重，并在版本缺口时调用 `TaskExecutionService::get_state_checkpoint` 查询权威当前状态。结果为 `None`、查询错误或检查点版本低于通知版本时不得 ACK，应保留在 Retry 路径。检查点只能恢复当前状态，不能重放遗漏的状态迁移及其业务副作用。可用 `TaskExecutionService::notification_failure()` 检查失败头部，再用 `quarantine_failed_notification(...)` 显式归档隔离；隔离会有意丢失该通知，且不会自动补发。

持久 outbox 由 `SqliteTaskStore` 提供；`MemoryTaskStore` 不支持。启用 publisher 不会补发启用前提交的生命周期状态。清理会跳过仍有活动 outbox 行的任务，并在删除前原子保存最小终态检查点。检查点不会自动过期，数量和磁盘占用会随清理任务增长。检查点仍存在时，SQLite 会拒绝复用对应 `TaskId`；不会自动修复已经存在的身份冲突。provider 要求、消费者 checkpoint、监控和关闭行为见[发布生命周期变化](user-guide.zh_CN.md#发布任务生命周期变化)。

## 验证应用迁移

请同时更新应用代码、自定义 store、示例和 lockfile，并针对 0.11 公开 API 编译。至少核对：带类型任务提交和处理器查找、带版本检查的状态更新、keyset 分页、迁移后 SQLite 数据库重开、旧 UUID 数据库被拒绝且内容不变，以及启用通知时消费者能够安全处理重复事件。不要把 0.8 历史文档中的 API 示例当作当前用法。
