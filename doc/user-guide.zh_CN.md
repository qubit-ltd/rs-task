# Qubit Task 用户手册

[中文 README](../README.zh_CN.md) · [English user guide](user-guide.md) · [API 文档](https://docs.rs/qubit-task)

本文适用于 `qubit-task` 0.8.x，要求 Rust 1.94 或更高版本。面向那些会收到「工作比请求活得更久」的 Rust 服务开发者：导入、导出、报表生成、媒体处理以及类似的后台作业。读到[检查任务结果](#检查任务结果)，就足以受理这类工作、在有限并发下运行，并把状态回报给客户端。后续章节覆盖重启恢复、资源预算、取消、重试、历史维护、状态通知、组件组装与停机。若要自行实现存储、调度策略或执行引擎，请阅读[用 qubit-spi 组装组件](#用-qubit-spi-组装组件)以及[详细设计](task_execution_service_design.md)。

## 目录

- [它解决什么问题](#它解决什么问题)
- [从哪里开始](#从哪里开始)
- [接入 CSV 导入服务](#接入-csv-导入服务)
  - [定义任务 payload 与处理器](#定义任务-payload-与处理器)
  - [在请求处理器中提交](#在请求处理器中提交)
  - [向客户端报告状态](#向客户端报告状态)
  - [在启动时组装服务](#在启动时组装服务)
  - [多个任务类型与处理器注册表](#多个任务类型与处理器注册表)
  - [这条路径上的核心类型](#这条路径上的核心类型)
- [检查任务结果](#检查任务结果)
  - [成功时是什么样子](#成功时是什么样子)
  - [失败、Panic 与阻塞](#失败panic-与阻塞)
- [调用方停止等待后如何找到任务](#调用方停止等待后如何找到任务)
- [取消导入](#取消导入)
- [重试策略与尝试次数预算](#重试策略与尝试次数预算)
- [重启后恢复已接受的工作](#重启后恢复已接受的工作)
- [运行进程内闭包](#运行进程内闭包)
- [限制并发与资源](#限制并发与资源)
  - [CPU 槽位、GPU 与命名资源](#cpu-槽位gpu-与命名资源)
  - [运行中任务与等待队列](#运行中任务与等待队列)
- [浏览历史并保持有界](#浏览历史并保持有界)
- [发布状态变更](#发布状态变更)
  - [在进程内订阅](#在进程内订阅)
  - [通过 Redis Streams 发布](#通过-redis-streams-发布)
  - [通知计数与停机](#通知计数与停机)
- [用 qubit-spi 组装组件](#用-qubit-spi-组装组件)
- [生命周期与停机](#生命周期与停机)
- [错误、诊断与排障](#错误诊断与排障)
- [从 0.5 及更早版本迁移](#从-05-及更早版本迁移)
- [测试自定义存储与验证 crate 包](#测试自定义存储与验证-crate-包)
- [边界与实践清单](#边界与实践清单)
- [延伸阅读](#延伸阅读)

## 它解决什么问题

以一个租户管理 API 为例。管理员把包含数千行客户数据的 CSV 上传到对象存储，再请求服务执行导入。导入要解析文件、校验每一行并写入数据库，可能耗时数分钟。若 HTTP 处理器自己做完整个导入，连接会一直被占用，客户端看不到进度，负载均衡可能切断请求，进程重启后导入也会消失，且没有任何记录表明它曾被请求过。改用裸 `tokio::spawn` 只能解决占用连接的问题：并发导入数量不受限，状态无法供后续查询，重启后也无法恢复。

`qubit-task` 为服务提供一个 `TaskExecutionService`。HTTP 处理器把导入描述为 `TaskRequest`（任务类型 `csv-import`、处理器版本 `1`、携带租户 ID 与对象键的小 payload），提交后立即把任务 ID 返回客户端。服务排队请求，在运行槽位与所需资源空闲时启动已注册的 `CsvImportV1` 处理器，记录每一次状态变更，并用该记录回答 `GET /imports/{id}`。配合 SQLite 存储时，已接受的导入在重启后仍可恢复。导入行本身在应用数据库里；任务记录只保留简短摘要。

本 crate 在**单个进程内**调度工作。恢复语义是至少一次：运行中被打断的导入可能再次执行，因此仓储层必须能容忍重复批次。本 crate 不在多节点间分发工作、不串联任务为工作流、不提供 cron、不能强行中断正在运行的代码，也不能保证业务副作用恰好一次。下文在相关处会说明这些边界。

## 从哪里开始

1. [接入 CSV 导入服务](#接入-csv-导入服务)涵盖处理器、提交调用、状态端点与启动接线。
2. [检查任务结果](#检查任务结果)说明任务完成时长什么样，以及 `Failed`、`Panicked` 与 `Blocked` 的区别。基础接入到此为止。
3. 按需继续阅读：[调用方停止等待后如何找到任务](#调用方停止等待后如何找到任务)、[取消导入](#取消导入)、[重试策略与尝试次数预算](#重试策略与尝试次数预算)、[重启后恢复已接受的工作](#重启后恢复已接受的工作)、[限制并发与资源](#限制并发与资源)、[发布状态变更](#发布状态变更)或[生命周期与停机](#生命周期与停机)。

完整可运行示例见 [`examples/task_service.rs`](../examples/task_service.rs)（进程内闭包、协作式取消、带版本的请求）与 [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs)（运维人员处理 blocked 任务）。幂等提交段落对应的 [`idempotent_submit.rs`](../tests/fixtures/doc-examples/src/bin/idempotent_submit.rs) 会编译运行，并同时检查相同请求重放和同键冲突。

## 接入 CSV 导入服务

加入 crate、异步运行时与 payload 编解码依赖：

```toml
[dependencies]
qubit-task = { version = "0.8", features = ["sqlite"] }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

`sqlite` 特性启用重启恢复。若只需易失内存执行可省略；下文代码仅在 builder 调用处不同。`serde_json` 是应用选择的 payload 编码方式；crate 把 payload 当作不透明字节。

导入功能分三块：处理器模块负责 payload 格式与带版本的处理器；API 模块提交任务并把任务状态映射为面向客户端的状态；启动接线构建单一服务并共享。`ImportRepository` 是应用定义的、对接对象存储与数据库的接口；服务本身不接触它。

### 定义任务 payload 与处理器

```rust
// src/imports/handler.rs
use std::sync::Arc;

use qubit_task::handler::{TaskContext, TaskHandler, TaskHandlerDescriptor, TaskRunOutcome, TaskRunResult};
use qubit_task::model::{TaskOutput, TaskRunError};
use qubit_task::store::TaskFuture;
use serde::{Deserialize, Serialize};

// payload 保存导入参数；CSV 文件本身仍在对象存储中。
#[derive(Serialize, Deserialize)]
pub struct CsvImportJob {
    pub tenant_id: String,
    pub object_key: String,
}

// 由仓储层分类的应用错误。
pub struct ImportError {
    pub category: String,
    pub message: String,
    pub retryable: bool,
}

// 应用针对自己的对象存储与数据库实现此 trait。
pub trait ImportRepository: Send + Sync {
    // 在已导入 imported_rows 行之后导入下一批；None 表示文件已读完。
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
                // 取消是协作式的：收到请求时在批次之间停止。
                if context.is_cancelled() {
                    return Ok(TaskRunOutcome::Cancelled);
                }
                let repository = Arc::clone(&self.repository);
                let job = Arc::clone(&job);
                // 解析与数据库写入会阻塞；不要占满异步 worker。
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
            // 持久化的只有这份小摘要；已导入行在应用数据库中。
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: format!("imported {imported_rows} rows").into_bytes(),
            }))
        })
    }
}
```

`TaskHandlerDescriptor` 标明此处理器接受的精确 `(task_type, version)` 对。存储里的请求只会交给相同对的处理器，因此改 payload 格式应注册 `CsvImportV2` 并与 `CsvImportV1` 并存，而不是改旧处理器。`TaskHandler` 继承 `Send + Sync`：注册表里的每个处理器是长期存活的 `Arc`，同一键下并发运行的多条任务会在不同 worker 上同时调用 `run(&self, …)`，因此像 `CsvImportV1 { repository: Arc<dyn ImportRepository> }` 这样把依赖做成可共享、线程安全的句柄是常见写法，不要把「当前正在跑哪条任务」写在无同步的可变字段里。`run` 收到不透明 payload 与 `TaskContext`（含 `task_id()`、`attempt()`（从 1 起）、`assigned_resources()`、`is_cancelled()`）。future 在 Tokio 异步 worker 上运行；耗时的解析与数据库写入应经 `spawn_blocking`，避免拖慢其他任务。处理器决定 `ImportError` 是否可重试；服务只对标记为 `retryable: true` 的错误重试。`TaskOutput.summary` 是有界持久文本，不是业务结果本身。

### 在请求处理器中提交

```rust
// src/imports/api.rs
use qubit_task::TaskExecutionService;
use qubit_task::model::{TaskId, TaskRequest, TaskState};
use qubit_task::service::TaskServiceError;

use super::handler::CsvImportJob;

pub enum StartImport {
    // 把任务 ID 返回客户端，供其轮询状态端点。
    Accepted { task_id: TaskId },
    // 等待队列已满；应返回 HTTP 429，由客户端重试。
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

// request_key 由客户端为每次导入生成一次，重试时复用。
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
        // 相同键的完全重试会返回原有记录。
        Ok(record) => Ok(StartImport::Accepted { task_id: record.id }),
        Err(TaskServiceError::QueueFull) => Ok(StartImport::Busy),
        Err(error) => Err(error.into()),
    }
}
```

`TaskRequest::new` 默认占用一个 CPU 槽且不填可选字段。`submit` 要求幂等键非空，且须由客户端（或 API 层、且在做别的事之前）生成并保持不变，这样重试的 `POST /imports` 仍映射到同一任务。相同键的相同请求返回已有记录；相同键的不同请求会失败并返回 `StoreError::IdempotencyConflict`。`correlation_key` 是应用自定义值，用于日后查找相关任务；此处为租户 ID。`submit` 返回已接受的 `TaskRecord`，其 `id` 即面向客户端的句柄。`QueueFull` 表示背压而非请求失败，应对外映射为 HTTP 429，并由客户端用同一键重试。

### 向客户端报告状态

```rust
// src/imports/api.rs（续）
pub async fn import_status(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<ImportStatus, TaskServiceError> {
    // get_summary 从不加载 payload。
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

`get_summary` 返回 `TaskSummary`：不含 payload 的请求元数据、生命周期状态、`state_version`、`attempt`、时间戳、已分配资源与输出摘要。所有状态读取都应使用它。只有应用需要带 payload 的完整 `TaskRecord` 时才调用 `get`。`Unknown` 表示从未接受过的任务 ID，或记录已被 prune / 驱逐。

### 在启动时组装服务

```rust
// src/main.rs（启动片段）
use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;

let tasks = TaskExecutionServiceBuilder::recoverable_sqlite("./state/tasks.sqlite")?
    .register_handler(Arc::new(CsvImportV1::new(repository)))?
    .max_running_tasks(NonZeroUsize::new(4).expect("positive limit"))
    .build()
    .await?;
let api_tasks = tasks.clone();
// ... 用 api_tasks 处理 HTTP 请求 ...
tasks.shutdown().await?;
```

`repository` 是应用的 `Arc<dyn ImportRepository>`。应在 HTTP 监听器打开之前构建服务，并注册存储中仍可能存在的每个处理器版本；`build()` 返回后注册表固定，重复的 `(task_type, version)` 会以 `HandlerConflict` 失败。`recoverable_sqlite` 打开数据库、获取操作系统锁以防两进程执行同一库，并在返回前扫描未完成工作。`TaskExecutionService` 可 `Clone`；把克隆交给请求处理器，并保留一份用于 shutdown。若只需易失执行，使用 `TaskExecutionServiceBuilder::in_memory()`（或快捷方式 `TaskExecutionService::in_memory().await?`）：进程退出后 pending 工作与历史都会丢失。

### 多个任务类型与处理器注册表

一个 `TaskExecutionService` 对应一个 `TaskHandlerRegistry`，而不是「整个进程只能有一个 `TaskHandler`」。CSV 导入、报表导出、缩略图生成等不同业务，应各自实现 `TaskHandler`，在 `descriptor()` 里声明不同的 `task_type`（必要时同一类型下再区分 `version`，例如 `csv-import@1` 与 `csv-import@2` 并存）。提交时 `TaskRequest::new("csv-import", "1", payload)` 与 `TaskRequest::new("report.export", "1", payload)` 会分别匹配已注册的处理器；调度与恢复都只看请求里持久化的 `task_type` 和 `handler_version`，不会根据 payload 内容猜测。

提交时不要求处理器已经注册：请求会先被受理并持久化；若注册表中没有精确匹配的 `(task_type, handler_version)`，任务随后进入 `Blocked`。`build()` 后注册表固定。可恢复服务应在重建前补注册缺少的版本，启动后再对保留任务调用 `retry_blocked`。

启动时装配示例：

```rust
use qubit_task::TaskExecutionServiceBuilder;

let tasks = TaskExecutionServiceBuilder::recoverable_sqlite("./state/tasks.sqlite")?
    .register_handler(Arc::new(CsvImportV1::new(repository.clone())))?
    .register_handler(Arc::new(ReportExportV1::new(repository.clone())))?
    .register_handler(Arc::new(ThumbnailV2::new(media)))?
    .build()
    .await?;
```

也可先填充 `TaskHandlerRegistry`（例如从 `qubit-spi` 发现多个 provider 后统一 `register`），再 `.handlers(registry)` 交给 builder。规则要点：

| 规则 | 含义 |
| --- | --- |
| 精确键 | 只接受与 `TaskHandlerDescriptor` 完全相同的 `(task_type, version)`；没有「默认处理器」或前缀匹配。 |
| 一键一实例 | 同一 `(task_type, version)` 只能注册一次；该键下的所有任务共用同一个 `Arc<dyn TaskHandler>`，靠 `TaskContext::task_id()` 等区分单次执行。 |
| 并发共用 | 受 `max_running_tasks` 与资源额度约束时，多条同键任务可同时处于 `Running`；调度器对同一 `Arc` 克隆并并行调用 `run`，不是「每条任务 new 一个 handler」。 |
| 须线程安全 | trait 要求 `Send + Sync`；`run` 的 future 须 `Send` 以便跨 worker 执行。共享依赖用 `Arc`、连接池等已同步的组件；单次执行的变量放在 `run` 的 async 块内，或按 `task_id` 分区的内部状态。 |
| 恢复一致 | SQLite 等可恢复存储里未完成的任务仍携带提交时的键；重启后必须注册相同键，否则任务会 `Blocked` 并写明缺失的处理器。 |
| 与 `submit_local` 区分 | 进程内闭包走 `submit_local`，会为单次任务生成临时的 `local:{id}@1` 处理器，且不可用于声明了重启恢复的存储。 |

### 这条路径上的核心类型

| 类型 | 作用 |
| --- | --- |
| `TaskExecutionService` | 唯一门面：提交、查询、等待、取消、维护、停机。`clone` 到各模块。 |
| `TaskExecutionServiceBuilder` | 选择存储、处理器、容量、限制、重试策略与可选事件总线。 |
| `TaskRequest` | 可重建描述：任务类型、精确处理器版本、payload、资源需求、关联键与幂等键、元数据。 |
| `TaskHandler` / `TaskHandlerDescriptor` | 解释一种 payload 格式的带版本代码；`TaskHandler: Send + Sync`，同一注册实例供并发任务共用。 |
| `TaskContext` | 每次尝试的任务 ID、尝试序号、已分配资源与协作式取消标志。 |
| `TaskRunOutcome` / `TaskRunError` | 处理器结果：`Succeeded(TaskOutput)`、`Cancelled`，或带 `retryable` 的分类错误。 |
| `TaskRecord` / `TaskSummary` | 可查询的生命周期；摘要省略 payload。 |
| `TaskState` | `Queued`、`Running`、`Blocked { reason }`、`Succeeded`、`Failed { category, message }`、`Panicked { message }`、`Cancelled`。 |
| `TaskServiceError` | 门面错误，如 `QueueFull`、`Blocked`、`AttemptsExhausted`、`ShuttingDown`、`StoreUnavailable`、`SchedulerUnavailable`。 |

## 检查任务结果

`submit` 返回 `Ok(record)` 表示请求**已被接受并存储**，且 `state == Queued`。这不说明导入何时运行。接受之后的阶段可在 `TaskSummary` 中观察：

| 阶段 | 可观察字段 |
| --- | --- |
| 已接受 | `state == Queued`，`accepted_at_ms` 已设，`attempt == 0`。 |
| 已调度并启动 | `state == Running`，`started_at_ms` 已设，`attempt >= 1`，`assigned_resources` 已填。 |
| 等待重试 | `state == Queued`，`attempt >= 1`，`retry_not_before_ms` 已设。 |
| 终态 | `state.is_terminal()`，`finished_at_ms` 已设；仅 `Succeeded` 时有 `output`。 |
| 需人工介入 | `state == Blocked { reason }`；非终态、未在调度。 |

每次状态迁移都会增加 `state_version`。若消费者可能乱序收到事件或快照，应保留最高版本。

### 成功时是什么样子

若要阻塞直到导入完成（例如集成测试或同步批处理工具），使用 `wait`：

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::{TaskId, TaskState, TaskSummary};
use qubit_task::service::TaskServiceError;

pub async fn wait_for_import(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<TaskSummary, TaskServiceError> {
    match tasks.wait(task_id).await {
        Ok(summary) => {
            // 此处 summary.state 为 Succeeded、Failed、Panicked 或 Cancelled。
            if let TaskState::Succeeded = summary.state {
                let text = summary
                    .output
                    .as_ref()
                    .map(|output| String::from_utf8_lossy(&output.summary).into_owned())
                    .unwrap_or_default();
                println!("import {task_id} finished: {text}");
            }
            Ok(summary)
        }
        Err(TaskServiceError::Blocked) => {
            // 任务需要运维介入；用 get_summary 读取 reason。
            Err(TaskServiceError::Blocked)
        }
        Err(error) => Err(error),
    }
}
```

对导入示例，正常完成会打印 `import <id> finished: imported 4213 rows`，摘要显示 `state == Succeeded`、`attempt == 1` 且 `finished_at_ms` 已设。`wait` 在首个终态时返回，且只唤醒本进程内的等待者。任务进入 `Blocked` 时返回 `Err(Blocked)`；未知 ID 返回 `Err(Store(NotFound))`；服务自身故障时返回 `StoreUnavailable` 或 `SchedulerUnavailable`。长时间 HTTP 请求不应持有 `wait`；上文轮询端点是常态路径。

### 失败、Panic 与阻塞

- **`Failed { category, message }`**：处理器返回 `retryable: false` 的 `TaskRunError`，或成功处理器返回的输出摘要超过 64 KiB 上限。`category` 是处理器稳定的分类（`invalid_payload`、`import_worker` 或仓储层自选）；`message` 上限 4,096 字节并在 UTF-8 边界截断。完整原始错误应写入应用日志。
- **`Panicked { message }`**：处理器 future panic。引擎会报告，与 panic 发生在处理器何处无关。业务错误若 category 为字符串 `panic`，仍是 `Failed`。
- **`Blocked { reason }`**：服务无法在无介入下继续。原因包括恢复任务缺少 `(task_type, handler_version)` 处理器、尝试预算用尽、失败尝试重新入队时等待队列已满，或 `activate` 返回 `EngineError::Closed`。记录仍可查询；`retry_blocked` 见[重试策略与尝试次数预算](#重试策略与尝试次数预算)，`abandon_blocked` 见[浏览历史并保持有界](#浏览历史并保持有界)。
- **`Cancelled`**：任务启动前被取消，或处理器确认了取消请求。见[取消导入](#取消导入)。

尝试预算尚有余额时，可重试错误不会立刻进入终态。记录回到 `Queued` 并设置 `retry_not_before_ms`，退避后再启动下一次尝试。

基础接入到此结束。以下章节按读者下一步问题组织，均为可选。

## 调用方停止等待后如何找到任务

客户端可能在服务已接受请求后对 `POST /imports` 超时。重试时应复用同一个键并提交相同请求，存储会返回原任务 ID。不要只做预先的键查询后直接返回已有 ID，否则同一键对应不同请求时会被误认为完全相同的重放：

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskId;

pub async fn start_or_find_import(
    tasks: &TaskExecutionService,
    request_key: &str,
    job: &CsvImportJob,
) -> Result<Option<TaskId>, Box<dyn std::error::Error>> {
    // 已有相同键时，submit 会比较完整请求并拒绝冲突。
    match start_import(tasks, request_key, job).await? {
        StartImport::Accepted { task_id } => Ok(Some(task_id)),
        StartImport::Busy => Ok(None),
    }
}
```

`get_by_idempotency_key` 仍适合只读查询，返回不含 payload 的 `TaskSummary`，但不能替代重试时的提交。键仅在记录保留期间占用；prune 或内存驱逐后可复用于新任务。保留中的键若对应不同请求，会被 `StoreError::IdempotencyConflict` 拒绝；完全相同的重放则返回原记录，即使配置容量此后已降低。键最长 256 UTF-8 字节。

若要列出某租户的全部导入而非单个任务，按 `correlation_key` 过滤：

```rust
use qubit_task::model::{TaskQuery, TaskStateKind, TaskSummary};

pub async fn active_imports_for_tenant(
    tasks: &TaskExecutionService,
    tenant_id: &str,
) -> Result<Vec<TaskSummary>, TaskServiceError> {
    let mut cursor = None;
    let mut active = Vec::new();
    loop {
        let page = tasks
            .list(TaskQuery {
                states: vec![TaskStateKind::Queued, TaskStateKind::Running, TaskStateKind::Blocked],
                limit: 100,
                after: cursor,
                correlation_key: Some(tenant_id.to_owned()),
            })
            .await?;
        active.extend(page.records);
        cursor = page.next;
        if cursor.is_none() {
            return Ok(active);
        }
    }
}
```

`TaskQuery.states` 使用 `TaskStateKind`（无诊断信息的生命周期类别）。分页按 `(accepted_at_ms, id)` 排序，`limit` 不得超过 256（否则 `InvalidRequest`；0 当作 1）。游标不是并发写入下的一致性快照。

## 取消导入

取消的结果取决于任务状态，有两种不同结局：

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskId;
use qubit_task::service::{CancelOutcome, TaskServiceError};

pub enum CancelImport {
    Cancelled,
    Requested,
    AlreadyFinished,
}

pub async fn cancel_import(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<CancelImport, TaskServiceError> {
    Ok(match tasks.cancel(task_id).await? {
        // Queued 或 Blocked：任务立即变为 Cancelled，不会再运行。
        CancelOutcome::CancelledBeforeStart => CancelImport::Cancelled,
        // Running：已设标志；由处理器决定何时停止。
        CancelOutcome::CancellationRequested => CancelImport::Requested,
        CancelOutcome::AlreadyTerminal => CancelImport::AlreadyFinished,
    })
}
```

`Queued` 或 `Blocked` 任务会立刻变为 `Cancelled`。对 `Running` 任务，服务持久化 `cancel_requested = true` 并设置 `TaskContext::is_cancelled()` 读取的标志。不会强行中断。`CsvImportV1` 在批次之间检查标志并返回 `TaskRunOutcome::Cancelled`，记录随后变为 `Cancelled`。若处理器先完成并返回 `Succeeded` 或错误，该结果成立；迟到的取消请求不会覆盖。从不检查标志的处理器不会被取消。对未知 ID 调用 `cancel` 返回 `Store(NotFound)`。

## 重试策略与尝试次数预算

当 `import_next_batch` 返回 `ImportError { retryable: true, .. }`（例如数据库连接重置）时，服务会把任务重新入队并持久化到期时间，稍后再启动。默认初始间隔一秒、每次重试加倍、上限六十秒，总共三次尝试。在 builder 上同时配置：

```rust
use std::time::Duration;

use qubit_task::{RetryPolicy, TaskExecutionServiceBuilder};

let builder = TaskExecutionServiceBuilder::in_memory()
    .retry_policy(RetryPolicy::new(Duration::from_secs(5), Duration::from_secs(300))?)
    .max_attempts(5);
```

`RetryPolicy::new(initial, maximum)` 拒绝零初始延迟或小于初始延迟的上限。`max_attempts` 统计每次任务启动，**跨进程重启累计**。预算用尽后任务变为 `Blocked` 而非无限重试，`TaskContext::attempt()` 告诉处理器当前是第几次尝试。每次重试占用普通队列槽；失败尝试重新入队时若等待队列已满，任务会以队列容量原因变为 `Blocked`，而不是突破限制。

运维人员在修复原因（恢复数据库、安装缺失处理器、腾出队列容量）后，用 `retry_blocked` 重新入队：

```rust
use qubit_task::model::TaskState;

pub async fn retry_after_fix(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(summary) = tasks.get_summary(task_id).await? {
        if let TaskState::Blocked { reason } = &summary.state {
            eprintln!("import {task_id} is blocked: {reason}");
        }
    }
    match tasks.retry_blocked(task_id).await {
        Ok(summary) => {
            // 再次 Queued，重试到期时间已清除。
            let _ = summary;
            Ok(())
        }
        Err(TaskServiceError::AttemptsExhausted { attempts, limit }) => {
            Err(format!("used {attempts}/{limit} attempts; submit a new task").into())
        }
        Err(TaskServiceError::NotBlocked { actual }) => Err(format!("task is {actual:?}").into()),
        Err(error) => Err(error.into()),
    }
}
```

`retry_blocked` 清除到期时间并立即可调度。预算已尽时失败并返回 `AttemptsExhausted`；新的尝试预算需要新任务 ID。更早尝试的重复批次可能已写入，因此 `ImportRepository` 在重试下须幂等。只有重复操作安全时，处理器才应把错误标为可重试。

## 重启后恢复已接受的工作

三种存储选择决定重启后保留什么：

| 配置 | 已完成历史 | 重启后尚未完成的已接受工作 |
| --- | --- | --- |
| `TaskExecutionServiceBuilder::in_memory()` | 有界内存历史 | 进程退出后丢失 |
| 带持久历史的自定义 `TaskStore` | 持久 | 取决于存储声明的 `restart_recovery` 能力 |
| `TaskExecutionServiceBuilder::recoverable_sqlite(path)` | SQLite | 恢复排队工作；中断的运行工作可能再次执行 |

导入服务使用 SQLite。`recoverable_sqlite(path)` 打开（或创建）数据库，设置 `require_recovery(true)` 并返回 builder。`build()` 时服务取得所有权，检查未完成记录数是否不超过 `queue_capacity + max_running_tasks`，并把每条 `Queued` 与 `Running` 记录重新放入等待队列。恢复的 `Running` 记录视为被中断的尝试并会再次运行，因此仓储须容忍重复批次。恢复语义因此是**至少一次**。

可观察的启动结果：

- **正常**：`build()` 成功，恢复的任务为 `Queued`，队列可能暂时超过 `queue_capacity`。新提交在该积压消化前会收到 `QueueFull`。
- **未完成记录过多**：`build()` 以 `TaskServiceBuildError::RecoveryCapacityExceeded` 失败，记录保持完整。提高 `queue_capacity` 或 `max_running_tasks` 后再启动。
- **另一进程持有数据库**：SQLite 服务构建以存储错误失败，包括 `recoverable_sqlite(path)` 打开存储时。不会回退到内存；不要打开 HTTP 监听器。
- **恢复的任务没有已注册处理器**：`build()` 成功，该任务 `Blocked`，reason 标明缺失的 `(task_type, handler_version)`。注册处理器、对同一库 rebuild 并调用 `retry_blocked`。启动不会自动重新入队。
- **恢复的任务已用尽 `max_attempts`**：变为 `Blocked` 且不再启动；`retry_blocked` 报告 `AttemptsExhausted`。

重试到期时间与记录一并持久化，重启不会提前启动重试。声明重启恢复的存储上不可用 `submit_local`（`TaskServiceError::UnsupportedCapability`），因为闭包无法从数据库重建；`capabilities().submit_local` 会报告这一点。`capabilities().store` 报告实际装配存储的 `persistent_history` 与 `restart_recovery` 标志。

SQLite schema 3 把请求元数据、payload BLOB 与生命周期 JSON 分列存储；摘要读取与状态迁移从不 SELECT BLOB。打开 schema 0、1 或 2 的数据库会在单事务中迁移到 schema 3，并保留 payload、幂等键与生命周期值。更新的 schema 或未知记录格式会被明确拒绝。SQLite 在 Tokio 阻塞池上每次只跑一条阻塞数据库操作，调用方须在 Tokio 运行时上 poll。服务在 shutdown 释放所有权后，旧存储句柄无法再写入。

### 安全升级 SQLite

先停止所有旧服务，并等待排空成功，再备份数据库。部署新版本后，用新的 store 实例打开原数据库，检查恢复摘要，最后重新开放业务入口。关闭超时不能证明可以安全接管；禁止新旧进程并行操作同一数据库。锁文件在完整数据库文件名后追加 `.owner.lock`，例如 `jobs.sqlite.owner.lock`。Unix 与 Windows 会验证物理文件身份，存在多个硬链接的数据库会被拒绝。数据库目录和锁文件须可信且保持稳定。schema 0、1、2 仍可迁移到 schema 3，不会丢弃任务数据。

## 运行进程内闭包

有些工作很短、属于当前请求、重启后无意义：例如渲染上传 CSV 的前几行供管理员确认列映射。这类工作有进程内类型化结果，无需注册处理器。`submit_local` 接受闭包并返回 `LocalTaskHandle<R, E>`：

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;
use qubit_task::service::{LocalTaskOutcome, LocalTaskResultError};

pub async fn render_preview(
    tasks: &TaskExecutionService,
    csv_head: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let handle = tasks
        .submit_local(move |context| {
            let mut rendered = Vec::new();
            for line in csv_head.split(|byte| *byte == b'\n') {
                if context.is_cancelled() {
                    return LocalTaskOutcome::<Vec<u8>, String>::Cancelled;
                }
                rendered.extend_from_slice(line);
                rendered.push(b'\n');
            }
            LocalTaskOutcome::Succeeded {
                value: rendered,
                summary: TaskOutput { summary: b"preview rendered".to_vec() },
            }
        })
        .await?;
    match handle.result().await {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(message)) => Err(message.into()),
        Err(LocalTaskResultError::Cancelled) => Err("preview cancelled".into()),
        Err(error) => Err(error.to_string().into()),
    }
}
```

闭包在 Tokio 阻塞池运行，返回 `LocalTaskOutcome::Succeeded { value, summary }`、`Failed(E)` 或 `Cancelled`。`E` 须实现 `Display`，因为服务会把其文本持久化为失败诊断。`handle.result()` 产生 `Ok(Ok(value))`、带原始类型错误的 `Ok(Err(error))`，或表示取消、panic、blocked 或基础设施失败的 `Err(LocalTaskResultError)`。任务仍有记录：`handle.task_id()` 可用于 `get_summary` 与 `cancel`，`TaskRecord.output` 只保留 `summary`。类型化 `value` **仅**存在于该句柄。若等待 `submit_local` 的请求被取消或超时，接受仍可能在后台完成，但句柄已失，值无法取回。须日后查找到的工作应使用带键的 `submit`。

内存预设（`in_memory()`）使用本地执行、每可用核心一个 CPU 槽（或 1）、等待队列 1,024 条、终态历史 1,024 条、最多 2,048 条非终态记录（含 `Blocked`）。不探测 GPU。要改内存存储限制，构建 `MemoryTaskStore::with_limits(history_capacity, payload_budget, unfinished_limit)` 并传给 `TaskExecutionServiceBuilder::store(Arc::new(...))`；达到未完成上限返回 `UnfinishedRecordLimitExceeded`，已保留幂等任务的重放仍可成功。

## 限制并发与资源

### CPU 槽位、GPU 与命名资源

容量是一组引擎为每次运行尝试预留的预算，不是操作系统绑核或设备发现。builder 接受显式 `ResourceCapacity`；每个请求声明 `ResourceRequest`：

```rust
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::{ResourceCapacity, TaskRequest};

let capacity = ResourceCapacity {
    cpu_slots: 8,
    gpus: BTreeMap::from([
        ("gpu-0".into(), vec!["cuda".into()]),
        ("gpu-1".into(), vec!["cuda".into()]),
    ]),
    custom: BTreeMap::from([("import_db_connections".into(), 4)]),
};
let builder = TaskExecutionServiceBuilder::in_memory()
    .capacity(capacity)
    .max_running_tasks(NonZeroUsize::new(16).expect("positive limit"))
    .queue_capacity(4_096);

// 导入偏 I/O：不占 CPU 槽，但占用四个池化连接之一。
let mut import = TaskRequest::new("csv-import", "1", payload.clone());
import.resources.cpu_slots = 0;
import.resources.custom.insert("import_db_connections".into(), 1);

// 嵌入任务需要一块 CUDA 设备。
let mut embedding = TaskRequest::new("embedding", "2", payload);
embedding.resources.gpu_count = 1;
embedding.resources.gpu_labels = vec!["cuda".into()];
```

`cpu_slots` 是 CPU 密集型处理器的并发预算。`gpus` 把设备 ID 映射到标签；带 `gpu_count` 与 `gpu_labels` 的请求会分配到携带全部请求标签的设备，处理器从 `TaskContext::assigned_resources()` 读取。`custom` 存放部署一致定义的独占整数预算：连接池大小、许可证、内存单位等。永远无法满足配置容量的请求在 `submit` 时被 `Unsatisfiable` 拒绝；满足容量但暂无空闲资源的请求在队列等待。资源描述最多 32 个 GPU 标签与 32 个自定义名，各非空且最多 128 UTF-8 字节；GPU 标签要求 `gpu_count > 0`。

### 运行中任务与等待队列

另有两个与资源无关的限制。`max_running_tasks` 限制并发运行尝试数，即使请求零 CPU 槽；默认值为可用并行度，否则为 1。显式设置以限制上述导入这类并发网络或数据库工作。`queue_capacity`（默认 1,024）限制等待队列；队列满时新提交返回 `QueueFull`，API 映射为 HTTP 429。

默认公平 FIFO 策略允许当前空闲资源能容纳的任务越过队首 blocked 任务，同时对被绕过次数有界的任务提供保护，使其最终会运行。服务还限制进行中的写操作（默认 64，`OperationLimitExceeded`）与进行中的请求 payload 字节（默认 64 MiB，`PayloadBudgetExceeded`）；单条 payload 不得超过 16 MiB。`stats()` 返回当前 `queued`、`running`、`blocked`、`terminal` 计数以及空闲容量的 `ResourceSnapshot`；计数与快照依次读取，非原子快照。


`submit`、`submit_local`、`cancel`、`retry_blocked`、`abandon_blocked` 和 `prune_terminal_before` 共用 `max_inflight_operations`，默认 64 个名额。`cancel` 也可能返回 `OperationLimitExceeded`，调用方应退避后重试。操作一旦获准，取消调用方或等待超时只会停止等待响应，不会撤销服务 worker 的存储写入与取消信号；提交 payload 另有独立字节预算。

## 浏览历史并保持有界

历史分页来自 `list(TaskQuery)`，见[调用方停止等待后如何找到任务](#调用方停止等待后如何找到任务)。内存存储会在超过历史容量时驱逐最旧的终态记录。SQLite 保留历史直到应用删除。每日运行的维护任务可把旧记录归档到应用自有存储，再分批删除终态行：

```rust
use std::num::NonZeroUsize;

use qubit_task::TaskExecutionService;
use qubit_task::service::TaskServiceError;

pub async fn prune_old_terminal(
    tasks: &TaskExecutionService,
    now_ms: u64,
) -> Result<usize, TaskServiceError> {
    let cutoff = now_ms.saturating_sub(30 * 24 * 60 * 60 * 1_000);
    let batch = NonZeroUsize::new(100).expect("100 is nonzero");
    let mut total = 0;
    loop {
        let removed = tasks.prune_terminal_before(cutoff, batch).await?;
        total += removed;
        if removed < batch.get() {
            return Ok(total);
        }
    }
}
```

`prune_terminal_before(accepted_before_ms, max_rows)` 只删除 cutoff 之前接受的终态记录，且每次调用最多 `max_rows` 条。`Queued`、`Running`、`Blocked` 保留。删除记录也会释放其幂等键，该键的重试窗口随之结束。不支持 prune 的存储报告 `UnsupportedCapability`。

`Blocked` 记录需要决策而非 cutoff。运维流程列出超过阈值的 `Blocked` 摘要，对无人修复的条目用 `abandon_blocked(id, state_version)` 放弃，再在后续 pass 中 prune。版本检查使并发 `retry_blocked` 安全：若读页后任务已变，`abandon_blocked` 返回 `StoreError::Conflict`；若不再 blocked 则 `NotBlocked`。[`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs) 是完整流程。

## 发布状态变更

对许多客户端，轮询 `GET /imports/{id}` 已足够。当其他模块应响应状态变更（例如推送 WebSocket 或刷新租户仪表盘）时，启用 `event-bus` 特性并向 builder 提供 `qubit_event_bus::EventBus`。每次状态变更后，服务在主题 `task.lifecycle` 上发布 `TaskEvent { task_id, state_version, state, correlation_key }`。本版本使用 `qubit-event-bus` 0.16。

发布是尽力而为。它不会回滚任务迁移；事件可能延迟、重复或丢失；消费者应以服务自身的查询 API 为权威，并在覆盖较新状态前比较 `state_version`。

### 在进程内订阅

```rust
use std::sync::Arc;

use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::{SubscribeRequest, Topic};
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::event::TaskEvent;
use qubit_task::model::TaskState;

pub trait ImportStatusView: Send + Sync {
    fn record(&self, tenant_id: &str, task_id: &str, state: &TaskState, state_version: u64);
}

pub fn subscribe_status_view(
    bus: &EventBus,
    view: Arc<dyn ImportStatusView>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let topic = Topic::<TaskEvent>::new("task.lifecycle")?;
    let request = SubscribeRequest::new("import-status-view", topic)?;
    Ok(bus.subscribe(request, move |delivery| {
        let event = delivery.payload();
        if let Some(tenant_id) = &event.correlation_key {
            // 事件可能重复或迟到；视图保留最高的 state_version。
            view.record(tenant_id, &event.task_id.to_string(), &event.state, event.state_version);
        }
    })?)
}

// 启动：创建总线、订阅，再用总线构建服务。
let bus = EventBus::local(LocalEventBusConfig::default())?;
let status_subscription = subscribe_status_view(&bus, view)?;
let tasks = TaskExecutionServiceBuilder::in_memory()
    .event_bus(bus.clone())
    .build()
    .await?;
```

`view` 是应用的仪表盘存储。保留 `status_subscription` 并在 shutdown 时取消。内置 local 总线只在进程内投递；事件上的 `correlation_key` 是 `start_import` 中设置的租户 ID，因此无需加载任务即可做按租户视图。

### 通过 Redis Streams 发布

要通知其他进程，选择跨进程 provider，例如 `qubit-event-bus-redis`。`TaskEvent` 实现 serde，但事件总线门面要求显式 `EventCodec<TaskEvent>`；注册 JSON 编解码器并按名称选择 provider：

```rust
use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;
use qubit_task::event::TaskEvent;
use qubit_task::service::TaskExecutionServiceBuilder;

struct TaskEventJsonCodec {
    content_type: ContentType,
    schema_id: SchemaId,
}

impl TaskEventJsonCodec {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            content_type: ContentType::new("application/json")?,
            schema_id: SchemaId::new("task-event-v1")?,
        })
    }
}

impl EventCodec<TaskEvent> for TaskEventJsonCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        Some(&self.schema_id)
    }

    fn encode(&self, value: &TaskEvent) -> Result<Arc<[u8]>, CodecError> {
        serde_json::to_vec(value)
            .map(Arc::from)
            .map_err(|source| CodecError::Encode { source: Box::new(source) })
    }

    fn decode(&self, bytes: &[u8]) -> Result<TaskEvent, CodecError> {
        serde_json::from_slice(bytes).map_err(|source| CodecError::Decode { source: Box::new(source) })
    }
}

let mut codecs = CodecRegistry::new();
codecs.register::<TaskEvent>(Arc::new(TaskEventJsonCodec::new()?));
let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
let config = EventBusConfig::default()
    .with_selection(ProviderSelection::named("redis-streams")?)
    .with_provider_options([
        ("redis.url".into(), "redis://127.0.0.1/".into()),
        ("redis.namespace".into(), "task-service".into()),
    ].into())
    .with_facade_config(facade);
let bus = EventBusRegistry::discover()?.create(&config)?;
let tasks = TaskExecutionServiceBuilder::in_memory()
    .event_bus(bus.clone())
    .build()
    .await?;
```

Redis provider、`qubit-spi` 与 `serde_json` 是应用依赖；`use qubit_event_bus_redis as _;` 链接 provider 以便 `discover()` 能找到。Redis adapter 由应用额外引入；任务库自身也依赖 `qubit-spi` 和 `serde_json`。provider 回执成功只表示 Redis 接受了 publish 命令，不表示订阅者已处理。任务状态与事件发布不是同一事务；若须一起提交，使用事务性 outbox。默认 fixture 只装配并关闭 provider，不发布事件，运行成功不能证明 Redis 连通。验证实际发布前，先在 `redis://127.0.0.1:6379/` 启动 Redis，再提交任务并检查通知回执和订阅输出。该组装在 CI 中用 `cargo check --locked --manifest-path tests/fixtures/doc-examples/Cargo.toml` 编译，用 `cargo run --locked --manifest-path tests/fixtures/doc-examples/Cargo.toml` 运行。

### 通知计数与 shutdown

服务通过 `qubit-event-bus` 的 `NotificationPublisher` 发布：默认一条串行发布线程、有界队列 256 条（`event_bus_buffer_capacity(NonZeroUsize)`）。状态迁移调用 `try_publish`，从不等待总线；队列满时丢弃新事件，shutdown 关闭队列后尝试发布的事件也会丢弃。两者都不改变任务结果。发布在专用 OS 线程上运行，同步 provider 不会占用 Tokio worker。

配置总线时 `notification_stats()` 返回 `Some(TaskEventNotificationStats)`：

| 计数 | 含义 |
| --- | --- |
| `enqueued` | 已进入本地队列的事件。 |
| `queue_full`、`queue_closed` | 在队列边界被丢弃的事件。 |
| `accepted` | 至少一个接受目标的回执；`partial_rejection` 统计同时有拒绝的回执。 |
| `opaque_accepted` | provider 接受但未暴露目标（Redis）。 |
| `unaccepted` | 无接受目标的回执，含空目标列表与拦截器丢弃。 |
| `publish_error` | 返回错误的 publish 调用。 |
| `worker_panicked` | 发布线程 panic；队列中事件可能丢失。 |

这些是准入与 worker 计数，不是订阅者已执行的证明。单调递增，在 `u64::MAX` 饱和；单次快照的各字段不是同一瞬间。

`shutdown()` 在已接受工作 settle 后关闭通知入队，再排空队列。默认最多等待发布线程 30 秒（`event_bus_close_timeout(Duration)`）。超时时，`shutdown()` 返回 `TaskServiceError::NotificationClose`，线程仍会继续排空已持有内容。线程 panic 或 join 失败也返回 `NotificationClose`；剩余事件可能丢失。并发与后续的 `shutdown` 调用方收到相同存储结果。服务不会 shutdown 应用拥有的总线；应在服务之后由应用关闭。

## 用 qubit-spi 组装组件

可以扩展并替换核心组件，包括**自定义存储**（例如 Redis、PostgreSQL 或文件后端）：实现对应 trait 并装配进 `TaskExecutionServiceBuilder` 即可。`qubit-task` 在 `qubit_task::spi` 为下表四类能力定义 `qubit-spi` 服务族；应用也可跳过 SPI，直接把 `Arc<dyn …>` 传给 builder。

| 扩展点 | SPI 服务族 | 运行时 trait | 典型装配 |
| --- | --- | --- | --- |
| 任务历史与受理 | `TaskStoreSpec` | `TaskStore` | `from_components` 的 `store` 参数；或 registry 解析后传入 |
| 排队顺序 | `SchedulingPolicySpec` | `SchedulingPolicy` | `from_components` 的 `policy` 参数 |
| 资源预留与执行 | `TaskExecutionEngineSpec` | `TaskExecutionEngine` | `from_components` 的 `engine` 参数 |
| 业务处理器 | `TaskHandlerSpec` | `TaskHandler` | `register_handler` / `handlers(TaskHandlerRegistry)` |

**SPI 路径（可选 `inventory` feature）**：在独立 crate 中实现 `ServiceProvider<…Spec>`，声明稳定 provider ID，用 `submit_sync_provider!` 注册；把该 crate **链接**进最终二进制（不是运行时加载 `.so`），启动时调用 `discovered_*_registry()` 或内置 `memory_store_registry()` 等，用 `ProviderSelection::named(...)` 选中 provider，`create_configured(&config)` 得到 `Arc<dyn …>`，再 `from_components` 与注册 handler。存储扩展常用 `TaskStoreConfig::Custom(...)` 传入 provider 私有配置。

**直接路径**：在应用内实现 `TaskStore`（及其它 trait），`TaskExecutionServiceBuilder::from_components(Arc::new(yours), engine, policy)`，无需 `inventory`。`in_memory()` / `recoverable_sqlite()` 只是内置 provider 的快捷预设，不会因为你链接了第三方 provider 而自动切换。

生命周期通知走应用提供的 `qubit-event-bus` `EventBus`，由 builder 直接注入；**不在** `qubit-task` 里定义事件总线的 SPI 族。

`TaskExecutionServiceBuilder::in_memory()` 与 `recoverable_sqlite()` 是三个组件上的预设：`TaskStore`（接受、读、迁移）、`SchedulingPolicy`（下一个排队任务）、`TaskExecutionEngine`（资源预留与执行）。`from_components(store, engine, policy)` 接受任意实现：

```rust
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::scheduling::SchedulingPolicy;
use qubit_task::{TaskExecutionEngine, TaskStore};

pub fn assemble(
    store: Arc<dyn TaskStore>,
    engine: Arc<dyn TaskExecutionEngine>,
    policy: Arc<dyn SchedulingPolicy>,
) -> TaskExecutionServiceBuilder {
    TaskExecutionServiceBuilder::from_components(store, engine, policy)
}
```

`qubit-task` 在 `qubit_task::spi` 为四个扩展点定义 SPI 服务族（`TaskStoreSpec`、`SchedulingPolicySpec`、`TaskExecutionEngineSpec`、`TaskHandlerSpec`）。内置组件在那里有稳定 provider ID：`MEMORY_STORE_PROVIDER_ID`（`qubit.task.store.memory`）、`SQLITE_STORE_PROVIDER_ID`（`qubit.task.store.sqlite`）、`FAIR_FIFO_PROVIDER_ID`（`qubit.task.scheduler.fair-fifo`）、`LOCAL_ENGINE_PROVIDER_ID`（`qubit.task.engine.local`）。启用 `inventory` 特性可通过 `discovered_*_registry()` 从链接 crate 收集 provider 注册。应用仍须选择 provider、提供配置并创建 `Arc<dyn ...>`；发现不会猜测数据库路径、凭据或容量。在接受流量前拒绝 provider ID 冲突与重复处理器键。服务在生命周期内保持其组件不变。

第三方 `TaskStore` 实现者须提供聚合的 `count_states()`、不读 payload 字节的 `get_summary`、不解码 payload 的 `has_unfinished_over_limit(limit)`、返回 `TaskSummary` 的 `transition`，以及基于摘要的 `list` 分页；`prune_terminal_before` 与 `abandon_blocked` 可报告 `UnsupportedCapability`。`TaskExecutionEngine::try_prepare` 同步且须 promptly 预留资源，不得等待或运行处理器工作；`activate` 在服务记录尝试为 running 后启动处理器，且一旦开始工作须返回可跟踪的执行句柄。[详细设计](task_execution_service_design.md)描述这些契约。

`TaskStore::release_owner(epoch)` 成功返回是一道完成屏障：该 owner 先前准入的写操作必须全部结束，即使调用方已丢弃写入 future，旧写入也不能在释放后提交。新 owner 不得与尚未结束的旧写入重叠；释放失败不能视为已安全交接。聚合计数来自单个存储一致性边界，但 future 返回前仍可能被并发写入改变。

显式选择内置 SPI provider 的完整装配示例见 [`spi_selection.rs`](../tests/fixtures/doc-examples/src/bin/spi_selection.rs)，运行命令为 `cargo run --locked --manifest-path tests/fixtures/doc-examples/Cargo.toml --bin spi_selection`。第三方 provider 的链接注册由 provider 与 consumer fixture 验证。

## 生命周期与停机

启动顺序：若使用总线，先创建总线和订阅 → 构建服务并恢复存储工作 → 打开业务入口。停机顺序：停止接受业务请求 → shutdown 服务 → 取消事件订阅并 shutdown 总线。

```rust
use std::time::Duration;

use qubit_task::TaskExecutionService;
use qubit_task::service::TaskServiceError;
use tokio::time::Instant;

pub async fn stop(tasks: &TaskExecutionService) -> Result<(), Box<dyn std::error::Error>> {
    match tasks.shutdown_until(Instant::now() + Duration::from_secs(30)).await {
        Ok(()) => Ok(()),
        Err(TaskServiceError::ShutdownTimedOut) => {
            eprintln!("accepted imports are still draining in the background");
            Err(TaskServiceError::ShutdownTimedOut.into())
        }
        Err(TaskServiceError::NotificationClose(reason)) => {
            eprintln!("notifications did not close cleanly: {reason}");
            Err(TaskServiceError::NotificationClose(reason).into())
        }
        Err(error) => Err(error.into()),
    }
}
```

`shutdown()` 以 `ShuttingDown` 拒绝新写入，等待进行中的提交完成接受，等待运行尝试与调度结束，释放存储所有权，再排空通知。`Ok(())` 表示以上全部完成；随后下一进程可打开 SQLite 文件。`shutdown_until(deadline)` 启动相同 drain，但只限制**本调用方**的等待；`ShutdownTimedOut` 表示 drain 在后台继续，超时本身不能证明所有权已释放。shutdown 期间取消仍是协作式：忽略标志的处理器会拖住 drain。丢弃最后一个服务句柄也会启动异步 drain，但无人观察结果；若关心完成应调用 `shutdown()`。

`TaskExecutionServiceBuilder::runtime_handle(Handle)` 为服务拥有的后台任务选择运行时；须保持该运行时存活直到 shutdown 或 drain 结束。取消等待 `build()` 的 future 不会停止后台构建 worker：它会在恢复分页边界停止、释放已取得的 owner，且不会启动调度器。

两种失败会永久改变服务。存储失败将服务设为 `StoreUnavailable`：新写入失败，`wait` 与 local 句柄立刻收到错误，共享 shutdown 结果仍等待调度器与跟踪执行再释放所有权。调度器 panic 或 `try_prepare` 的 `EngineError::Closed` 将服务设为 `SchedulerUnavailable`；排队记录仍在存储中供下次启动，调度器不会重启。`last_store_error()` 与 `last_scheduler_error()` 暴露保留的诊断。若自定义引擎在启动未跟踪副作用后 panic，应通过应用 supervisor 终止并重启进程。

## 错误、诊断与排障

| 现象 | 检查项 |
| --- | --- |
| `submit` 返回 `QueueFull` | 等待队列已满，或重启 backlog 仍在消化。施加背压（HTTP 429）并用同一键重试。仅在部署能承载更多 pending 工作时提高 `queue_capacity`。 |
| `submit` 返回 `Unsatisfiable` | 请求超过配置的 `ResourceCapacity`，例如 GPU 标签无设备承载。修正请求或容量。 |
| `submit` 返回 `InvalidRequest` | 空的 task type 或 handler version、超大字段或无效资源描述。消息会指明限制。 |
| `submit` 返回 `Store(IdempotencyConflict)` | 键被不同请求复用。为新工作生成新键。 |
| `submit_local` 返回 `UnsupportedCapability` | 存储声明重启恢复。改用带已注册处理器的 `TaskRequest`。 |
| 重启后任务一直 `Blocked` | 读 `get_summary` 中的 reason；常见为缺失处理器或尝试预算用尽。注册处理器并 `retry_blocked`，或 `abandon_blocked`。 |
| `retry_blocked` 返回 `AttemptsExhausted` | 任务跨重启已用尽 `max_attempts`。提交新任务以获得新预算。 |
| `cancel` 后运行任务不停 | 取消是协作式。处理器须检查 `TaskContext::is_cancelled()` 并返回 `TaskRunOutcome::Cancelled`。 |
| `wait` 返回 `Blocked` | 任务需介入；不会自行完成。 |
| `build()` 失败 `RecoveryCapacityExceeded` | 未完成记录多于 `queue_capacity + max_running_tasks`。提高其一；记录完整。 |
| `build()` 失败 `SqliteFeatureDisabled` 或存储错误 | 启用 `sqlite` 特性；检查路径且无其他进程持有库。不会回退内存。 |
| 处处 `StoreUnavailable` 或 `SchedulerUnavailable` | 服务永久降级。读 `last_store_error()` / `last_scheduler_error()`，shutdown、修复原因、重启。 |
| 收不到状态事件 | 查 `notification_stats()` 的 `queue_full`、`publish_error`、`worker_panicked`；确认订阅主题为 `task.lifecycle`。事件是尽力而为；以查询服务为权威状态。 |
| `shutdown()` 返回 `NotificationClose` | 发布线程未在 `event_bus_close_timeout` 内结束，或 panic。任务状态不受影响。 |

持久化诊断有界：category 128 字节，message 与 blocked reason 4,096 字节，在 UTF-8 边界截断。在应用日志中记录原始错误及任务 ID、尝试序号与租户。

## 从 0.5 及更早版本迁移

0.6 移除了调用方提供的任务 ID、带闭包的 `submit`、线程池专用 builder 设置，以及旧 `TaskHandle<R, E>`。进程内闭包现用 `submit_local` 并收到 `LocalTaskHandle<R, E>`；可重建工作使用带稳定幂等键的 `TaskRequest`，服务生成 `TaskId`。没有泛型持久句柄。

查询与扩展契约也变了：`TaskQuery.states` 为 `Vec<TaskStateKind>`；历史游标为 `TaskCursor { accepted_at_ms, id }`；`SchedulingPolicy` 实现收到 `QueuedTask.resources` 而非完整 request；`TaskStore` 新增 `count_states()`、`get_summary()`、`has_unfinished_over_limit(limit)`、`prune_terminal_before` 与 `abandon_blocked`，且 `transition` 返回 `TaskSummary`。`max_attempts` 现统计跨进程重启的启动次数，因此在 limit 的恢复记录变为 `Blocked` 而非再次运行。shutdown 开始后拒绝服务写入，SQLite 写入由存储所有权围栏。请一并更新调用点与自定义存储，再运行应用的编译与恢复测试。

| 旧契约 | 当前契约 |
| --- | --- |
| 仅限制提交操作的在途名额 | 六种写操作共用 `max_inflight_operations`，超限返回 `OperationLimitExceeded` |
| `StoredTaskPage<StoredTask>` | 不含 payload 的 `RecoveryPage<TaskSummary>` |
| owner 释放未约定排空 | `release_owner` 是准入写入的完成屏障 |

当前版本使用 Event Bus 0.16 与 Redis adapter 0.4。任务通知使用的有界 `NotificationPublisher` 与 `AdmissionOutcome` API 在 Event Bus 0.14 中已存在，因此仅升级该集成的依赖版本不要求迁移应用调用点；升级 adapter 时仍须核对其专属版本说明。

## 边界与实践清单

- 服务在单进程内调度。多节点租约、分布式资源发现、工作流依赖、cron、强行中断任意代码、业务副作用恰好一次均不在范围内。未来分布式引擎可实现同一 `TaskExecutionEngine` 边界而不改门面。
- 恢复与重试是至少一次。使处理器幂等，或在标记错误可重试前用应用自身事务保护副作用。
- 保持 payload 小：参数与引用，不是文件。限制为单 payload 16 MiB、进行中 payload 64 MiB、64 个进行中写操作；内存存储 retained payload 64 MiB。UTF-8 字节文本限制：`task_type` 128、`handler_version` 64、键 256、元数据 32 项且键 128 字节、值 4,096 字节、合计 16,384 字节。
- 结果存在应用数据库，返回有界 `TaskOutput` 摘要或引用。
- 在首次 `submit` 前生成幂等键并保持到不再需要任务记录。
- 在 `build()` 前注册存储仍可能持有的每个处理器版本；缺失版本会 blocked 任务而非丢失。
- 把每个 `TaskHandler` 当作进程内长期存活、可被多线程并发调用的单例：依赖用 `Arc` 等线程安全方式注入，单次任务状态不要放在无同步的可变字段上。
- 用 `get_summary` 轮询而非 `get`，`list` 分页不超过 256。按计划 prune SQLite 历史并单独审查 `Blocked` 记录。
- 把发布回执、事件与任务迁移当作不同阶段。权威状态以查询服务为准。
- 测试完整路径：接受、运行、重试、block、取消、带恢复的 restart、以及有 deadline 的 shutdown。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [详细设计](task_execution_service_design.md) · [API 文档](https://docs.rs/qubit-task)
- [`examples/task_service.rs`](../examples/task_service.rs) · [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs)

## 测试自定义存储与验证 crate 包

backend 作者可在独立测试 crate 中运行复用套件：

```toml
[dev-dependencies]
qubit-task = { version = "0.8", default-features = false, features = ["conformance"] }
```

实现公开的 `qubit_task::conformance::StoreFixture`。使用一个全新 fixture 运行 `verify_core_contract`，再用另一个持久化 fixture 运行 `verify_recovery_contract`；recovery 会写入 513 条未完成任务，并要求反复打开同一隔离命名空间。等待套件完整结束，确保 store 句柄释放后再清理。内存存储无法通过 recovery conformance。仍需为取消中的写入与 `release_owner`、事务错误和进程崩溃保留受控测试，因为公开套件无法证明这些 backend 内部性质。

运行 `.infra/tools/verify-packaged-consumer.sh` 验证实际 0.8 archive 和基于解包包的 consumer。脚本通过配置的 Cargo registry 解析第三方依赖，不使用兄弟仓库路径覆盖，并运行不同 feature 的 consumer 构建。Cargo package verification 保持开启。registry 或网络失败表示检查受阻；不要改用路径覆盖或在未成功运行时报告通过。

恢复是至少一次：进程可能在外部副作用后、任务终态落盘前崩溃，造成下一次重试。请用应用幂等机制或事务/outbox 保护副作用；本 crate 不保证业务效果恰好一次。游标签名与代码更新见 [0.8 迁移说明](migration-0.8.zh_CN.md)。
