# Qubit Task（`rs-task`）

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-task` 将耗时的应用工作作为带类型、可查询的任务运行。应用提交带类型的 payload 后会得到稳定任务 ID，并可查询生命周期状态和实时进度。处理器声明任务 kind、payload 类型、支持的 schema 版本和取消模式。服务提供有界的进程内调度，并可选用 SQLite 持久化和至少一次恢复。

## 带类型 API

公开 API 将以下标识分别处理：

- `TaskId` 标识单个任务。
- `kind_id` 将任务路由到处理器。
- `category` 用于应用侧任务分类和查询。
- `Payload<T>` 将带类型的值与 `type_id`、`schema_version`、`codec_id` 绑定。

[带类型任务 API 指南](doc/typed-task-api.zh_CN.md)提供完整示例，以及处理器注册、codec 复用、资源配额、取消、进度汇报、metadata 限额和 keyset 分页契约。可运行示例见 [`examples/task_service.rs`](examples/task_service.rs) 和 [`examples/blocked_maintenance.rs`](examples/blocked_maintenance.rs)。

## 安装

```toml
[dependencies]
qubit-task = { version = "0.8", features = ["sqlite"] }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

`sqlite` feature 提供持久化任务历史和恢复。调度器按有界分页扫描 queued 摘要，运行中的 handler 数量不超过 `max_running_tasks`。只有 handler 错误的 `retryable` 为 true 时才会重试，重试期限会持久化；默认最多尝试三次，退避时间从 1 秒指数增长到 60 秒。修复配置后，可使用观察到的 state version 调用 `resume_blocked` 重新排队。恢复采用至少一次语义：任务若在外部副作用之后中断，恢复时可能再次运行，因此应用副作用需要幂等性或事务保护。调度仅在进程内执行；分布式调度和业务副作用恰好一次不属于本 crate 的保证。

启用可选 `event-bus` feature 后，可在 builder 上配置 `event_notifications(bus, topic, queue_capacity, shutdown_flush_timeout)`，发布已经持久化的生命周期快照。通知采用尽力而为语义：本地队列满时丢弃快照，provider 错误会计数，shutdown 只在配置的超时内排空。查询仍是权威状态；消费者应按 `(TaskId, state_version)` 对账。进度更新不发布生命周期事件。

## 文档

- [用户指南](doc/user-guide.zh_CN.md)
- [详细设计](doc/task_execution_service_design.md)
- [迁移指南](doc/migration-0.8.zh_CN.md)
- [API 文档](https://docs.rs/qubit-task)

## 任务通知投递

任务通知采用尽力而为语义，没有事务 outbox。`notification_stats()` 返回已入队、发布成功、丢弃和失败的快照计数。`shutdown()` 会在释放 store owner 前尝试排空队列；超时后丢弃剩余通知，但不会让任务 shutdown 失败。publish receipt 只说明 provider 接纳结果，不代表订阅者已处理或 handler 已完成。

调度器按 keyset 顺序扫描可运行的排队任务。暂时拿不到所需资源的任务会被跳过，让后续资源匹配的任务先启动，因此不保证严格 FIFO。应用可调用 `TaskStore::prune_terminal_before(finished_before_ms, max_rows)` 显式清理一批旧终态记录；清理也会释放幂等键供后续复用，Queued 和 Blocked 任务会保留。

## 检查

```bash
cargo test --all-features
./align-ci.sh
./ci-check.sh
```
