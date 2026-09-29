# Qubit Task（`rs-task`）

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-task` 解决 Rust 服务里一个常见的难题：请求触发的工作远比请求本身耗时，例如把一个很大的 CSV 文件导入数据库。如果在请求处理函数里直接执行导入，连接会被长时间占用，用户看不到进度，进程一旦重启工作也随之丢失。使用本库时，请求处理函数只需把工作描述成带版本的 `TaskRequest`，交给同一个 `TaskExecutionService`，然后立刻把任务 ID 返回给调用方。服务会在有界队列、并发上限和资源额度约束下调用匹配的 `TaskHandler`，并根据所选存储的保留策略提供可查询的任务记录；搭配支持恢复的存储，还会在进程重启后协调已受理的未完成任务。本库只在单个进程内调度，也不会把业务副作用变成恰好一次的操作。

## 数据导入服务实战场景

租户管理员把 CSV 文件上传到对象存储后调用 `POST /imports`。API 把租户 ID 和对象键编码成一个 `csv-import` 任务，附上客户端生成的请求键提交，然后以 HTTP 202 返回任务 ID。`CsvImportV1` 处理器解码 payload，通过应用自己的仓储分批导入数据，并在收到取消请求时于两批之间停下。`GET /imports/{id}` 把任务状态映射为对外的导入状态。若使用 SQLite 存储且进程重启，排队中的导入会继续执行，被中断的运行中导入可能再次运行，因此仓储必须容忍同一批数据被重复处理。导入的数据行存放在应用数据库中，任务记录只保留一段简短摘要。

## 安装

```toml
[dependencies]
qubit-task = { version = "0.6", features = ["sqlite"] }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

`sqlite` feature 启用 `TaskExecutionServiceBuilder::recoverable_sqlite` 提供的重启恢复能力；只需要易失的内存执行时可以不启用。快速开始的示例还使用 `serde` 和 `serde_json` 编码任务 payload。

## 快速开始

完整可运行的程序位于 [`examples/task_service.rs`](examples/task_service.rs)（本地闭包、协作取消、带版本的请求）和 [`examples/blocked_maintenance.rs`](examples/blocked_maintenance.rs)（运维人员处理 Blocked 任务）。下面的片段展示导入功能如何与服务相接；`ImportRepository` 是应用自己的接口，由应用连接实际存储。

处理器模块负责 payload 格式和带版本的处理器：

```rust
// src/imports/handler.rs
use std::sync::Arc;

use qubit_task::handler::{TaskContext, TaskHandler, TaskHandlerDescriptor, TaskRunOutcome, TaskRunResult};
use qubit_task::model::{TaskOutput, TaskRunError};
use qubit_task::store::TaskFuture;
use serde::{Deserialize, Serialize};

// payload 只保存导入参数，CSV 文件本身留在对象存储中。
#[derive(Serialize, Deserialize)]
pub struct CsvImportJob {
    pub tenant_id: String,
    pub object_key: String,
}

// 由仓储分类过的应用错误。
pub struct ImportError {
    pub category: String,
    pub message: String,
    pub retryable: bool,
}

// 应用基于自己的对象存储和数据库实现该接口。
pub trait ImportRepository: Send + Sync {
    // 导入已完成 `imported_rows` 行之后的下一批数据；返回 `None` 表示文件已处理完。
    fn import_next_batch(&self, job: &CsvImportJob, imported_rows: usize)
        -> Result<Option<usize>, ImportError>;
}

pub struct CsvImportV1 {
    repository: Arc<dyn ImportRepository>,
}

impl CsvImportV1 {
    pub fn new(repository: Arc<dyn ImportRepository>) -> Self {
        Self { repository }
    }
}

impl From<ImportError> for TaskRunError {
    fn from(error: ImportError) -> Self {
        TaskRunError { category: error.category, message: error.message, retryable: error.retryable }
    }
}

impl TaskHandler for CsvImportV1 {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor { task_type: "csv-import".into(), version: "1".into() }
    }

    fn run<'a>(&'a self, payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let job: Arc<CsvImportJob> = serde_json::from_slice(payload)
                .map(Arc::new)
                .map_err(|error| TaskRunError {
                    category: "invalid_payload".into(),
                    message: error.to_string(),
                    retryable: false,
                })?;
            let mut imported_rows = 0_usize;
            loop {
                // 取消是协作式的：收到请求后在两批之间停止。
                if context.is_cancelled() {
                    return Ok(TaskRunOutcome::Cancelled);
                }
                let repository = Arc::clone(&self.repository);
                let job = Arc::clone(&job);
                // 解析和写库都是阻塞操作，不要放在异步工作线程上执行。
                let batch = tokio::task::spawn_blocking(move || repository.import_next_batch(&job, imported_rows))
                    .await
                    .map_err(|error| TaskRunError {
                        category: "import_worker".into(),
                        message: error.to_string(),
                        retryable: false,
                    })??;
                match batch {
                    Some(rows) => imported_rows += rows,
                    None => break,
                }
            }
            // 只有这段简短摘要会被持久化；导入的数据行保存在应用数据库中。
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: format!("已导入 {imported_rows} 行").into_bytes(),
            }))
        })
    }
}
```

API 模块负责提交任务，并把任务状态翻译给客户端。它只依赖 payload 类型，不依赖处理器：

```rust
// src/imports/api.rs
use qubit_task::TaskExecutionService;
use qubit_task::model::{TaskId, TaskRequest, TaskState};
use qubit_task::service::TaskServiceError;

use super::handler::CsvImportJob;

pub enum StartImport {
    // 把任务 ID 返回给客户端，供其轮询状态接口。
    Accepted { task_id: TaskId },
    // 等待队列已满：返回 HTTP 429，让客户端稍后重试。
    Busy,
}

pub enum ImportStatus {
    Pending,
    Running,
    Done { summary: String },
    Failed { category: String, message: String },
    NeedsOperator { reason: String },
    Cancelled,
    Unknown,
}

// `request_key` 由客户端为每次导入生成一次，重试时沿用同一个键。
pub async fn start_import(
    tasks: &TaskExecutionService,
    request_key: &str,
    job: &CsvImportJob,
) -> Result<StartImport, Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(job)?;
    let mut request = TaskRequest::new("csv-import", "1", payload)
        .with_idempotency_key(request_key);
    request.correlation_key = Some(job.tenant_id.clone());
    match tasks.submit(request).await {
        // 用同一个键重试完全相同的请求时，会返回最初受理的记录。
        Ok(record) => Ok(StartImport::Accepted { task_id: record.id }),
        Err(TaskServiceError::QueueFull) => Ok(StartImport::Busy),
        Err(error) => Err(error.into()),
    }
}

pub async fn import_status(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<ImportStatus, TaskServiceError> {
    // `get_summary` 不会加载 payload。
    let Some(summary) = tasks.get_summary(task_id).await? else {
        return Ok(ImportStatus::Unknown);
    };
    Ok(match summary.state {
        TaskState::Queued => ImportStatus::Pending,
        TaskState::Running => ImportStatus::Running,
        TaskState::Succeeded => ImportStatus::Done {
            summary: summary
                .output
                .map(|output| String::from_utf8_lossy(&output.summary).into_owned())
                .unwrap_or_default(),
        },
        TaskState::Failed { category, message } => ImportStatus::Failed { category, message },
        TaskState::Panicked { message } => ImportStatus::Failed { category: "panic".into(), message },
        TaskState::Blocked { reason } => ImportStatus::NeedsOperator { reason },
        TaskState::Cancelled => ImportStatus::Cancelled,
    })
}
```

应用启动时只构建一个服务，注册所有可能需要恢复的处理器版本，再把服务的克隆句柄交给各个请求处理函数。退出前显式关闭服务，才能观察到已受理任务排空的结果：

```rust
// src/main.rs（启动装配片段）
use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;

let tasks = TaskExecutionServiceBuilder::recoverable_sqlite("./state/tasks.sqlite")?
    .register_handler(Arc::new(CsvImportV1::new(repository)))?
    .max_running_tasks(NonZeroUsize::new(4).expect("positive limit"))
    .build()
    .await?;
let api_tasks = tasks.clone();
// ... 使用 `api_tasks` 对外提供 HTTP 服务 ...
tasks.shutdown().await?;
```

`repository` 是应用持有的 `Arc<dyn ImportRepository>`。`recoverable_sqlite` 会对数据库加操作系统级文件锁，在返回前扫描未完成的任务；加锁或恢复预检失败时直接让启动失败，不会退回内存存储。若只需要易失执行，可改用 `TaskExecutionServiceBuilder::in_memory()`，此时进程退出后未完成任务和历史都会丢失。

从客户端看到的效果：`start_import` 在请求内即可返回；导入在 `max_running_tasks` 以及每个请求默认一个 CPU 槽的约束下运行；`import_status` 先报告 `Pending`、`Running`，再进入终态。可重试的 `ImportError` 会按指数退避自动重试（初始 1 秒，上限 60 秒），默认最多 3 次尝试，用尽后任务进入 `Blocked` 等待运维处理。恢复出来的任务若找不到对应的 `(task_type, handler_version)` 处理器，同样会进入 `Blocked`；注册处理器、基于同一个数据库重新构建服务，再调用 `retry_blocked` 即可。`tasks.cancel(id)` 只是设置协作取消标记，处理器必须返回 `TaskRunOutcome::Cancelled`，任务才会以 `Cancelled` 结束。恢复执行是至少一次语义，因此 `ImportRepository` 必须让重复处理同一批数据是安全的。资源额度、本地闭包、通知和运维维护见[用户手册](doc/user-guide.zh_CN.md)。

## 能力与边界

- 带版本的 `TaskRequest`（任务类型、精确处理器版本、不透明 payload、资源需求、关联键与幂等键、少量 metadata），以及可查询的 `TaskRecord`/`TaskSummary` 生命周期，状态包括 `Queued`、`Running`、`Blocked`、`Succeeded`、`Failed`、`Panicked` 和 `Cancelled`。
- 统一的 `TaskExecutionService` 门面：`submit`、`submit_local`、`get`、`get_summary`、`get_by_idempotency_key`、`list`、`wait`、`cancel`、`retry_blocked`、`abandon_blocked`、`prune_terminal_before`、`stats`、`shutdown` 和 `shutdown_until`。
- 有界等待队列与 `QueueFull` 背压、公平 FIFO 调度策略、CPU 槽 / GPU / 具名资源额度，以及独立的 `max_running_tasks` 运行并发上限。
- 对可重试处理器错误的自动重试（退避时间持久化）、跨重启计数的尝试次数预算，以及针对处理器缺失、尝试耗尽或重试队列已满的 `Blocked` 记录。
- 面向易失工作的 `TaskExecutionService::in_memory()`，其中 `submit_local` 闭包通过类型化的 `LocalTaskHandle<R, E>` 返回结果；可选的 `sqlite` 存储提供重启恢复和 schema 迁移。
- 可插拔的 `TaskStore`、`SchedulingPolicy`、`TaskExecutionEngine` 和 `TaskHandler` provider，既可直接装配，也可通过 `qubit-spi` 发现。
- 可选的 `event-bus` feature：通过应用提供的 `qubit-event-bus` `EventBus` 以尽力而为的方式发布 `TaskEvent` 通知。

本库不提供多节点或分布式调度、工作流依赖、定时（cron）调度、对任意代码的强制中断，也不保证业务副作用恰好执行一次。存储声明支持重启恢复时，`submit_local` 不可用，因为闭包无法从数据库重建。通知是尽力而为的：队列（默认 256 条）满时会丢弃事件，发布失败也不会回滚任务状态。

影响部署的主要限额：内存预设的等待队列为 1024 个任务，终态记录 1024 条，非终态记录 2048 条（含 `Blocked`），请求 payload 总量 64 MiB；单个请求 payload 最多 16 MiB，提交共享 64 MiB 的受理中 payload 预算和 64 个受理中写操作，历史分页每页最多 256 条。请求文本上限按 UTF-8 字节计：`task_type` 128、`handler_version` 64、关联键和幂等键各 256；metadata 最多 32 项，键 128 字节、值 4096 字节、合计 16384 字节；持久化诊断类别不超过 128 字节，消息不超过 4096 字节。资源描述最多包含 32 个 GPU 标签和 32 个自定义资源名称，每项非空且不超过 128 个 UTF-8 字节；设置 GPU 标签时 `gpu_count` 必须大于零。重启时未完成记录数必须不超过 `queue_capacity + max_running_tasks`，否则构建失败并保留记录。SQLite 同一时刻只执行一个阻塞数据库操作，历史会一直保留，直到应用调用 `prune_terminal_before`；清理后对应的幂等键可以重新使用。`shutdown_until` 只限制调用方的等待时间；丢弃最后一个服务句柄会启动异步排空；调度器 panic 以 `SchedulerUnavailable` 报告，且不会自动重启。完整清单见[用户手册](doc/user-guide.zh_CN.md#运行限制)。

## 延伸阅读

- [用户手册](doc/user-guide.zh_CN.md)
- [详细设计](doc/task_execution_service_design.md)
- [API 文档](https://docs.rs/qubit-task)

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
