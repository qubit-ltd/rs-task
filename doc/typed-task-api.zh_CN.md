# 带类型的任务 API

[English](typed-task-api.md) · [用户指南](user-guide.zh_CN.md) · [0.10 迁移指南](migration.zh_CN.md)

带类型 API 会在接纳请求前保留应用值的类型。Payload 身份分成三个独立字段：`type_id` 标识模型，`schema_version` 标识 schema 版本，`codec_id` 标识字节编码。处理器按 `kind_id` 注册，只接受一个 payload `type_id`，并声明所支持的 schema 版本集合。一个 codec 可以支持多个 schema 版本；兼容范围属于处理器描述符。

`Payload<T>` 持有业务值。`TaskRequest<T>::encode` 从 `ValueBytesCodecRegistry` 查找 `ValueBytesCodecDescriptor` 并生成 `EncodedPayload<T>`；存储层接收类型擦除后的 `StoredPayload`。bytes 注册表使用 `ValueEncoder<T>` 和 `ValueDecoder<[u8]>`。任务 metadata 使用 `rs-metadata::Metadata`，任务请求限制最多 32 个条目、序列化后 16 KiB；同时遵守 `rs-metadata` 自身 wire 配额。

## 端到端示例

示例展示从带类型的值、codec 注册到处理器执行的完整路径。应用显式注入 ID generator。生产环境使用 Snowflake 类生成器时，不同进程必须分配不同节点 ID，并满足时钟配置要求。

```rust,no_run
use std::sync::Arc;
use qubit_codec::{ValueBytesCodecDescriptor, ValueBytesCodecRegistration, ValueBytesCodecRegistry, ValueCodecId, ValueCodecRegistration, ValueCodecRegistrationSource};
use qubit_model_id::{HasModelId, ModelId, ModelIdBuf};
use qubit_progress::{Metric, Stage};
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::{CancellationMode, TaskContext, TaskHandlerDescriptor};
use qubit_task::TaskHandler;
use qubit_task::model::{ResourceCapacity, TaskOutput};
use qubit_task::model::{ResourceRequest, TaskRequest};
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::store::{MemoryTaskStore, TaskFuture};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Resize { image: String, width: u32 }
impl HasModelId for Resize {
    const MODEL_ID: ModelId = ModelId::new("example.Resize");
}
#[derive(Default)]
struct JsonCodec;
impl qubit_codec::ValueEncoder<Resize> for JsonCodec {
    type Output = Vec<u8>; type Error = serde_json::Error;
    fn encode(&mut self, value: &Resize) -> Result<Vec<u8>, Self::Error> { serde_json::to_vec(value) }
}
impl qubit_codec::ValueDecoder<[u8]> for JsonCodec {
    type Output = Resize; type Error = serde_json::Error;
    fn decode(&mut self, bytes: &[u8]) -> Result<Resize, Self::Error> { serde_json::from_slice(bytes) }
}
static JSON_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<JsonCodec, Resize>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.resize.json"), &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("example", "typed_task", "guide", 1),
);

struct ResizeHandler;
impl TaskHandler<Resize> for ResizeHandler {
    fn run<'a>(&'a self, input: Resize, context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async move {
            let mut progress = context.progress_builder()
                .stage(Stage::new("resize", "Resize image"))
                .metric(Metric::new("images", "Images").total(1))
                .start_async().await.map_err(|e| qubit_task::model::TaskRunError {
                    category: "progress".into(), message: e.to_string(), retryable: false,
                })?;
            if context.is_cancelled() { return Ok(TaskRunOutcome::Cancelled); }
            let images = progress.metric("images").expect("configured metric");
            images.start(1).expect("one image starts");
            let _ = (input.image, input.width); // 在这里执行实际业务逻辑
            images.succeed(1).expect("one image succeeds");
            progress.report_async().await.map_err(|e| qubit_task::model::TaskRunError {
                category: "progress".into(), message: e.to_string(), retryable: false,
            })?;
            progress.finish_async().await.map_err(|e| qubit_task::model::TaskRunError {
                category: "progress".into(), message: e.to_string(), retryable: false,
            })?;
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

struct Ids(std::sync::atomic::AtomicU64);
impl qubit_id::IdGenerator for Ids {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed)))
    }
}

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(256)),
        Arc::new(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?),
        Arc::new(Ids(std::sync::atomic::AtomicU64::new(1))),
    ).capacity(ResourceCapacity {
        cpu_slots: 4, memory_bytes: Some(8_000_000_000), disk_bytes: Some(50_000_000_000),
        ..ResourceCapacity::default()
    });
    builder.handlers_mut().register::<Resize, _>(TaskHandlerDescriptor {
        kind_id: "images.resize".into(),
        payload_type_id: ModelIdBuf::try_from("example.Resize").unwrap(),
        accepted_schema_versions: vec![1, 2],
        cancellation_mode: CancellationMode::Cooperative,
    }, Arc::new(ResizeHandler))?;
    let tasks = builder.build().await?;
    let mut request = TaskRequest::new("images.resize", 2,
        ValueCodecId::new("example.resize.json"), Resize { image: "a.png".into(), width: 640 });
    request.category = Some("image-processing".into());
    request.resource_limit = ResourceRequest {
        cpu_slots: 1, memory_bytes: Some(256_000_000), disk_bytes: Some(64_000_000),
        ..ResourceRequest::default()
    };
    let accepted = tasks.submit(request).await?;
    let current = tasks.get(accepted.id).await?.expect("accepted task exists");
    println!("{} {:?}", current.id.to_padded_decimal(), current.state);
    // REST 接口可以调用 tasks.cancel(accepted.id)。
    Ok(())
}
```

## 运行契约

`TaskStore` 是公开的带类型持久化契约，由 `MemoryTaskStore` 和 `SqliteTaskStore` 直接实现。旧 UUID 适配层 `LegacyTaskStore` 及其 `RecoveryPage`/`scan_unfinished` API 不属于 0.10。使用 `SqliteTaskStore::open(path)` 打开 SQLite；typed schema v4/v5 会在事务中迁移到 v6，旧 UUID schema 则会被拒绝且不作修改。

- `TaskId` 包装 `rs-id::Id`；service 要求显式注入 `IdGenerator`。Snowflake 跨进程唯一性依赖节点 ID 不重复和时钟条件。`to_padded_decimal()` 输出定长十进制字符串，用于数据库字典序稳定排序。
- CPU slot、GPU 设备和标签、可选 memory/disk 字节数、自定义整数单位都是每次执行的接纳配额。它们限制并发预留量，不会绑核、在 OS 层发现或隔离 GPU，也不会限制进程实际内存或磁盘使用。超过配置容量的请求不可满足；暂时拿不到资源的任务会保留在队列中，调度器继续检查后续资源匹配的任务，因此不保证严格 FIFO。
- 排队或阻塞任务可直接取消。运行中任务使用 `CancellationMode::Cooperative` 时，`TaskContext::is_cancelled()` 会收到信号，处理器应在安全边界停止并返回 `TaskRunOutcome::Cancelled`。`ExternalHook` 需要注册外部 hook；`Unsupported` 不会停止运行中的尝试。
- `max_running_tasks` 限制活跃 handler 数，`scan_page_size` 限制每次 queued 摘要扫描量。待执行任务保留在 store 中，只有拿到运行名额后才创建执行协程。
- handler 的 `TaskRunError.retryable` 决定是否重试。默认 `max_attempts` 为 3，`RetryPolicy` 延迟从 1 秒增长到 60 秒。queued 状态和 `retry_not_before_ms` 一起保存，重启恢复仍遵守截止时间。不可重试错误、panic、取消和超过尝试上限都会结束任务。
- 调用 `resume_blocked(id, expected_state_version)` 可在修复配置并使用兼容 handler 或 codec 重启服务后重新排队。状态版本冲突时先重新读取摘要；存在待处理取消时不能恢复。
- 后台存储故障会被锁存：停止写入和调度，保留诊断读取；`shutdown()` 排空活跃工作后返回存储故障。SQLite typed schema 版本 4 和 5 会事务迁移到版本 6；旧 UUID schema 仍需显式映射数据。
- `TaskContext::progress_builder()` 使用 `rs-progress::AsyncReporter`。`report_async()` 等待进度持久化后才返回。后续查询可看到 stage 和 metric 快照，但它们不会改变生命周期 `state_version`；上报错误会返回处理器。
- typed 历史页按 `(accepted_at_ms, 数值 task id)` 升序排序。`after` 是排他键游标，不是 offset；每次查询读取自己的存储快照。只有存在 lookahead 记录时才有 `next`。过滤器包括 state、业务 `category` 和 correlation key；`kind_id` 用于处理器路由，与 `category` 独立。

需要持久生命周期通知时，启用 `event-bus`、注册应用提供的 `TaskEvent` codec，并设置 `TaskExecutionServiceBuilder::event_bus`。生命周期状态写入时会在同一事务内写入 SQLite outbox，再由后台异步重放。语义为至少一次：发布结果不确定，或在 outbox 删除前崩溃，都可能让消费者收到重复事件。消费者应按 `(TaskId, state_version)` 去重，并以任务查询为准。启用 publisher 不会补发此前已提交的历史状态。`MemoryTaskStore` 不支持持久 outbox。配置边界和监控方法见[用户指南](user-guide.zh_CN.md#发布任务生命周期变化)。

应用可通过维护作业调用 `TaskStore::prune_terminal_before(finished_before_ms, max_rows)`，删除最多 `max_rows` 条 finish 时间严格早于 cutoff 的终态记录。删除顺序为 finish 时间和 task ID，幂等键会在同一操作中删除，因此之后可复用；Queued 和 Blocked 不会清理。

## 提交拒绝与外部取消错误

`submit()` 会把预期的容量和重复请求拒绝映射为 `TaskServiceError::SubmissionCapacityExceeded`、`UnfinishedTaskLimitExceeded`、`IdempotencyConflict` 或 `DuplicateTaskId`。这些拒绝只影响当前请求，服务仍可继续使用。其他 store 故障仍按操作性错误处理，并可能使服务进入锁存不可用状态。

运行中任务使用 `CancellationMode::ExternalHook` 时，`cancel()` 会先持久化取消请求，再启动由服务托管的 hook 操作。同一 task `attempt` 的并发调用共享该操作；取消或中止等待 `cancel()` 的调用方不会停止 hook，`shutdown()` 会等待 hook 完成。hook 失败时，`cancel()` 返回 `TaskServiceError::ExternalCancellationFailed`，错误信息保存在 `TaskSummary.cancel_error`；任务仍在运行时，可再次调用 `cancel()` 重试。重试成功会清除该诊断，但 handler 仍负责报告最终状态。请确保 hook 对每个 (`TaskId`, `attempt`) 幂等，因为外部副作用可能已成功、但确认回执丢失。
