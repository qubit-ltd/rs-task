# qubit-task

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-task` 在 `qubit-executor` 和 `qubit-thread-pool` 之上提供面向任务的执行服务。

`TaskExecutionService` 接收调用方提供的任务 ID，在线程池中运行同步 callable，并在内存中保留状态以便查询和执行前取消。返回的 `TaskHandle` 负责保存类型化结果。

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

服务默认保留最近 1024 个终态。设置 `completed_history_capacity(0)` 可不保留终态。历史记录有界且不是持久化存储：记录被淘汰后，`status(id)` 返回 `None`。任务完成后可以复用任务 ID，新提交会替换该 ID 的旧状态。`stats().total` 统计当前可见的已接受任务和保留终态，而不是所有历史提交次数。

`wait_for_idle()` 和 `wait_for_current_tasks()` 等待注册表状态转换。它们返回时，结果可能仍在发布到句柄；需要结果时请调用 `TaskHandle::get()` 或等待该句柄。

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
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
