# qubit-task

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-task` 接受可重建的业务任务，根据 CPU、GPU 和具名资源额度排队执行，并通过统一门面提供状态查询。存储、调度、执行引擎和版本化处理器既可直接装配，也可通过 `qubit-spi` 选择。

## 安装

```toml
[dependencies]
qubit-task = "0.6"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

## 从易失型本机任务开始

这个具名预设把任务状态保存在内存中。进程退出时未完成任务会丢失，终态历史最多保留 1024 条。

```rust,no_run
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;
use qubit_task::service::LocalTaskOutcome;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::in_memory().await?;
    let handle = service.submit_local(|_| LocalTaskOutcome::<String, std::io::Error>::Succeeded {
        value: "完成".to_owned(),
        summary: TaskOutput { summary: b"完成".to_vec() },
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

## 项目文档

- [用户指南](doc/user-guide.zh_CN.md)
- [架构概览](doc/design.zh_CN.md)
- [TaskExecutionService 详细设计](doc/task_execution_service_design.md)
- [English user guide](doc/user-guide.md)

## 测试

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
Pull Request 前运行 `./align-ci.sh` 格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
