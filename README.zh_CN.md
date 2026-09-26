# qubit-task

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-task` 让 Rust 服务能够接收耗时较长的后台任务，按 CPU、GPU 和具名资源额度调度，并通过统一门面查询任务状态。它适合需要有界后台执行和任务历史、又不希望业务代码绑定特定存储或执行引擎的应用。存储、调度、执行引擎和版本化处理器既可直接装配，也可通过 `qubit-spi` 选择。

## 安装

```toml
[dependencies]
qubit-task = "0.6"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

## 从易失型本机任务开始

这个具名预设把任务状态保存在内存中。进程退出时未完成任务会丢失，终态历史最多保留 1024 条；非终态记录默认最多 2048 条，`Blocked` 也占用名额。每页历史查询最多返回 256 条。

```rust,no_run
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;
use qubit_task::service::LocalTaskOutcome;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::in_memory().await?;
    let handle = service.submit_local(|_| LocalTaskOutcome::<String, std::io::Error>::Succeeded {
        value: "完成".to_owned(),
        summary: TaskOutput { summary: "完成".as_bytes().to_vec() },
    }).await?;
    let value = handle.result().await??;
    assert_eq!(value, "完成");
    service.shutdown().await?;
    Ok(())
}
```

需要重启后恢复任务时，启用 `sqlite` feature，并使用 `TaskExecutionServiceBuilder::recoverable_sqlite(path)`。构建服务前，为每个已保存的 `(task_type, handler_version)` 注册对应处理器。

可选的生命周期通知使用有界队列，默认容量为 256；队列满时会丢弃新通知，关闭服务时通常会排空已入队通知，发布线程 panic 时队列中剩余通知可能丢失。同步事件总线 provider 可能阻塞专用发布线程，因此 provider 一直不返回时，服务关闭也可能一直等待。更多通知配置和统计说明见[用户指南](doc/user-guide.zh_CN.md)。

`LocalTaskHandle<R, E>` 返回仅存在于当前进程的完整值或业务错误。对于运行中任务，
只有处理器返回 `LocalTaskOutcome::Cancelled` 确认取消后，句柄才会以
`LocalTaskResultError::Cancelled` 报告取消；`cancel_requested` 只是请求。尚未开始执行的排队任务
由服务直接完成取消。可恢复任务使用带版本的
`TaskRequest`，其 `TaskRecord.output` 只保存摘要或引用，不保存完整结果。
第三方 `TaskStore` provider 必须实现 `count_states()`，一次聚合统计所有保留记录。
`stats()` 调用该聚合一次，再读取引擎资源，因此两部分是相邻但非原子的快照。

## 适用场景与能力边界

例如，接口收到数据导入请求后，可以提交任务并立即返回任务 ID；服务再按资源上限调用对应版本的处理器，调用方随后查询执行状态，无须让原始请求一直等待。若结果只需在当前进程内交还给调用代码，可使用 `submit_local`；若任务需要重建或在进程重启后恢复，应使用 `TaskRequest`。

本 crate 提供有界队列、资源感知调度、本地或可插拔执行、查询与取消接口，以及可选的 SQLite 恢复和生命周期通知。它不负责多节点分布式调度、工作流依赖、定时任务、强制中断任意代码，也不保证业务副作用恰好执行一次。

`TaskExecutionService::submit` 要求提供稳定且非空的幂等键。首次调用前生成并保存该键；
调用方停止等待后，可通过 `get_by_idempotency_key` 查找已受理任务。若查询返回 `None`，
应使用同一请求和同一键重试。键只在对应记录保留期间有效。内存服务最多保留 64 MiB
任务 payload，受理中最多 64 个提交，并共享 64 MiB 的受理 payload 预算；需要更长恢复窗口时
应使用持久化存储。`shutdown_until` 只限制调用方等待时间，超时后已受理任务仍会继续排空。
`submit_local` 的类型化结果只能通过返回的句柄取得。调用方等待 `submit_local` 时取消或超时后，
受理仍可能在后台继续，但调用方会失去句柄，无法找回原始类型化结果。调用方需要在请求停止等待后
继续定位任务时，应使用带稳定键的 `submit`。

## 项目文档

- [用户指南](doc/user-guide.zh_CN.md)
- [English README](README.md)
- [TaskExecutionService 详细设计](doc/task_execution_service_design.md)
- [English user guide](doc/user-guide.md)

## API 与存储契约

自动重试会持久化下次可运行时间，默认从 1 秒起步按指数退避，最高 60 秒；SQLite schema 0/1 数据库会在打开时事务性迁移到 schema 2。SQLite 将不可变请求 JSON 与生命周期 JSON 分开保存，状态变化不会重写大型 payload。

服务提供独立的 `max_running_tasks(NonZeroUsize)` 运行并发上限，零 CPU 槽请求也占用一个运行名额。重启时未完成记录不得超过 `queue_capacity + max_running_tasks`；超限会在保留记录的情况下使启动失败。`max_attempts` 统计同一任务跨进程启动的总次数；耗尽后任务进入 `Blocked`，`retry_blocked` 返回 `AttemptsExhausted`。

丢弃最后一个服务句柄会启动异步排空；需要观察排空结果时调用 `shutdown()`。调度器 panic 会返回 `SchedulerUnavailable`，且不会自动重启。自定义引擎契约和零 CPU I/O 配置见[用户指南](doc/user-guide.zh_CN.md)。

`TaskQuery.states` 使用 `TaskStateKind`；此前用带诊断内容的 `TaskState`
构造筛选条件的调用方需要迁移。请求文本上限按 UTF-8 字节计算：`task_type`
128、`handler_version` 64、关联键和幂等键各 256；metadata 最多 32 项，键
128、值 4096、键值合计 16384。持久化诊断类别最多 128 字节，消息最多
4096 字节；执行诊断会在 UTF-8 字符边界裁剪。SQLite 同时只执行一个阻塞
数据库操作。开始关闭后服务拒绝新的写入，SQLite 所有权释放后旧句柄不能写入。

## 测试

历史分页使用 `TaskCursor { accepted_at_ms, id }`，按受理时间、再按任务 ID
排序。SQLite 历史默认保留；调用方可显式调用 `prune_terminal_before`，并为每次
清理指定最大行数。被删除记录的幂等键可以重新使用。公开调度策略中的
`QueuedTask` 现在保存 `resources`，不再保存完整请求。应用可通过
`TaskExecutionServiceBuilder::runtime_handle` 指定服务后台任务使用的 runtime，
并须保证它至少存活到排空完成。第三方 `TaskStore` 必须实现
`has_unfinished_over_limit(limit)`，以便恢复预检无需解码 payload。最后句柄析构和调度器故障细节见用户指南。

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh` 格式化代码，运行 `./ci-check.sh` 对齐 CI 要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
