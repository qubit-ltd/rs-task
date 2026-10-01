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

`sqlite` feature 提供持久化任务历史和恢复；只需内存执行时可省略。恢复采用至少一次语义：任务若在外部副作用之后中断，恢复时可能再次运行，因此应用副作用需要幂等性或事务保护。调度仅在进程内执行；分布式调度和业务副作用恰好一次不属于本 crate 的保证。

可选的 `event-bus` feature 提供 `TaskEvent` 传输集成。typed execution service 目前尚未发布生命周期事件；请通过任务查询接口读取权威状态。

## 文档

- [用户指南](doc/user-guide.zh_CN.md)
- [详细设计](doc/task_execution_service_design.md)
- [迁移指南](doc/migration-0.8.zh_CN.md)
- [API 文档](https://docs.rs/qubit-task)

## 任务通知投递

任务通知采用尽力而为语义。publisher 的 `close` 成功表示本地队列 worker 已排空并停止；应检查通知统计以确认 provider 发布结果。不要把 close 成功视为目标接纳或 handler 完成。

## 检查

```bash
cargo test --all-features
./align-ci.sh
./ci-check.sh
```
