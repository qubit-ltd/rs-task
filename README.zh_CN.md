# qubit-task

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-task` 在 `qubit-executor` 和 `qubit-thread-pool` 之上提供面向任务的执行服务。

## 安装

README 示例还会直接使用任务 ID 类型，请在 `Cargo.toml` 中添加：

```toml
[dependencies]
qubit-task = "0.5"
qubit-id = "0.6"
```

`TaskExecutionService` 接收调用方提供的任务 ID，在线程池中运行同步 callable，并在内存中保留状态以便查询和执行前取消。取消排队任务成功返回时，任务已从队列移除且其捕获值已释放；worker 已开始执行 callable 后再取消会返回 `false`。返回的 `TaskHandle` 负责保存类型化结果。

```rust
use qubit_id::Id;
use qubit_task::service::{TaskExecutionService, TaskStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::builder()
        .completed_history_capacity(128)
        .build()?;
    let id = Id::new(42);
    let handle = service.submit_callable(id, || Ok::<u32, std::io::Error>(7))?;
    assert_eq!(handle.get()?, 7);
    assert_eq!(service.status(id), Some(TaskStatus::Succeeded));
    service.shutdown();
    service.wait_termination();
    Ok(())
}
```

服务默认最多保留 1024 个不同任务 ID 的终态。设置 `completed_history_capacity(0)` 可不保留终态。历史记录有界且不是持久化存储：记录被淘汰后，`status(id)` 返回 `None`。任务完成后可以复用任务 ID，新终态会替换旧记录，不会额外占用一个历史名额。`stats().total` 统计当前可见的已接受任务和保留的终态 ID，而不是累计提交次数。`thread_pool_stats()` 提供只读线程池指标快照，不会暴露可提交任务的线程池接口。

`wait_for_idle()` 和 `wait_for_current_tasks()` 等待注册表状态转换。它们返回时，结果可能仍在发布到句柄；需要结果时请调用 `TaskHandle::get()` 或等待该句柄。

## 延伸阅读

- [用户指南](docs/user-guide.zh_CN.md)
- [设计说明](docs/design.zh_CN.md)
- [API 文档](https://docs.rs/qubit-task)
- [English user guide](docs/user-guide.md)

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
Pull Request 前运行 `./align-ci.sh` 格式化代码，运行 `./ci-check.sh` 对齐 CI 要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
