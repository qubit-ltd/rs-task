# 带类型的任务 API

[English](typed-task-api.md) · [用户指南](user-guide.zh_CN.md)

0.12 带类型 API 会将 payload 模型、任务 kind、schema 版本和 codec 绑定后再提交。处理器描述符声明可接收的模型和 schema 版本；注册时会核对载荷的 `HasModelId` 与描述符。一个 codec 可以支持多个 schema 版本，具体兼容范围由处理器描述符决定。

`TaskKind<T>` 从 `T: HasModelId` 获取稳定模型身份，并同时构造处理器描述符和类型化请求。`TaskRequest<T>::encode` 从 `ValueBytesCodecRegistry` 查找 `ValueBytesCodecDescriptor` 并生成 `EncodedPayload<T>`；存储层接收类型擦除后的 `StoredPayload`。bytes 注册表使用 `ValueEncoder<T>` 和 `ValueDecoder<[u8]>`。任务 metadata 使用 `rs-metadata::Metadata`，任务请求限制最多 32 个条目、序列化后 16 KiB；同时遵守 `rs-metadata` 自身 wire 配额。

16 KiB metadata 输出额度在 JSON 编码期间执行。返回 `TaskRequestEncodeError::MetadataTooLarge(n)` 时，`n` 是已知长度下界（当前为 16,385），不一定是完整编码后的精确长度。调用方应将它用于识别超限请求，不应把它显示为精确原始大小。这是明确的错误诊断语义变化；枚举形状、成功编码路径和 `StoredTaskRequest` wire 表示保持不变。

## 端到端示例

示例从类型化请求、codec 注册和 handler 执行，一直走到接纳结果与终态检查。它使用内存 builder 运行独立进程；需要持久恢复的应用应注入持久 store 和 ID generator。

```rust,no_run
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskKind;
use qubit_task::TaskState;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Serialize)]
struct EchoPayload {
    message: String,
}

impl HasModelId for EchoPayload {
    const MODEL_ID: ModelId = ModelId::new("example.EchoPayload");
}

#[derive(Default)]
struct JsonCodec;

impl ValueEncoder<EchoPayload> for JsonCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &EchoPayload) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(value)
    }
}

impl ValueDecoder<[u8]> for JsonCodec {
    type Output = EchoPayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

static JSON_DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<JsonCodec, EchoPayload>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.echo.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("example", "rs-task", "readme_quick_start", 1),
);

async fn run_echo(payload: EchoPayload, _context: TaskContext) -> TaskRunResult {
    println!("{}", payload.message);
    Ok(TaskRunOutcome::success())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let codecs = ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?;
    let echo = TaskKind::<EchoPayload>::new(
        "example.echo",
        1,
        ValueCodecId::new("example.echo.json"),
    );
    let mut builder = TaskExecutionServiceBuilder::in_memory(256, codecs);
    builder.register_handler_fn(echo.descriptor(), run_echo)?;

    let service = builder.build().await?;
    let accepted = service
        .submit(echo.request(EchoPayload {
            message: "typed task completed".into(),
        }))
        .await?;
    println!("accepted={} task={}", accepted.created, accepted.summary.id);

    let terminal = service
        .wait_terminal(accepted.summary.id, Duration::from_secs(5))
        .await?;
    assert_eq!(terminal.state, TaskState::Succeeded);
    println!("task {} is {:?}", terminal.id, terminal.state);

    service.shutdown().await?;
    Ok(())
}
```

## 运行契约

`TaskStore` 是公开的带类型持久化契约，由 `MemoryTaskStore` 和 `SqliteTaskStore` 直接实现。旧 UUID 适配层 `LegacyTaskStore` 及其 `RecoveryPage`/`scan_unfinished` API 不属于当前公开契约。使用 `SqliteTaskStore::open_async(path).await` 打开 SQLite；typed schema v4 到 v7 会在原数据库中通过事务迁移到 v8，保留任务、活动 outbox 行和隔离归档记录，并新增终态状态检查点。旧 UUID schema 会被拒绝且不作修改。

`TaskExecutionService::get_summary(id)` 和 typed 历史查询返回 `TaskSummary`，其中有生命周期、路由、metadata、资源和进度信息，但不会加载或解码持久化的 payload 字节。`get_state_checkpoint(id)` 是更精简的恢复查询，只返回修复消费者投影所需的当前状态和版本；SQLite 清理终态任务后仍可读取该检查点。检查点不是任务摘要，不能恢复 payload、进度或中间状态迁移。

- `TaskId` 包装 `rs-id::Id`；service 要求显式注入能跨进程重启保持唯一的 `IdGenerator`。Snowflake 跨进程唯一性依赖节点 ID 不重复和时钟条件。`to_padded_decimal()` 输出定长十进制字符串，用于数据库字典序稳定排序。
- CPU slot、GPU 设备和标签、可选 memory/disk 字节数、自定义整数单位都是每次执行的接纳配额。它们限制并发预留量，不会绑核、在 OS 层发现或隔离 GPU，也不会限制进程实际内存或磁盘使用。超过配置容量的请求不可满足；暂时拿不到资源的任务会保留在队列中，调度器继续检查后续资源匹配的任务，因此不保证严格 FIFO。
- 排队或阻塞任务可直接取消。运行中任务使用 `CancellationMode::Cooperative` 时，`TaskContext::is_cancelled()` 会收到信号，处理器应在安全边界停止并返回 `TaskRunOutcome::Cancelled`。`ExternalHook` 需要注册外部 hook；`Unsupported` 不会停止运行中的尝试。
- `max_running_tasks` 限制活跃 handler 数，`scan_page_size` 限制每次 queued 摘要扫描量。待执行任务保留在 store 中，只有拿到运行名额后才创建执行协程。
- 同步 payload codec 工作会放到 Tokio blocking pool：`submit()` 在任务接纳前编码；运行中的 attempt 先解码，再运行异步 handler。在编码请求交给 store 前取消会停止后续接纳，但无法中止已经开始的编码；编码结果可能被丢弃。接纳派发后，服务会持有 store 操作及 scheduler/通知唤醒；取消或超时 `submit()` 等待不会取消该操作，因此任务可能已接纳，也可能稍后才接纳，`shutdown()` 会等待它完成。首次提交前设置稳定的幂等键，并以完全相同的请求重提，可取回同一任务。若已知道任务 ID，可用 `get_summary()` 查询；要取消任务本身，请调用 `request_cancel()`。解码期间仍占用 running 名额和资源预留，`shutdown()` 会等待准备阶段和 handler 完成。启用 panic unwind 时，decoder panic 会记为该任务的 `Panicked` 终态。blocking 工作不能被强制取消，也没有硬超时；codec 卡住可能拖延 shutdown 和 runtime 退出。
- handler 的 `TaskRunError.retryable` 决定是否重试。默认 `max_attempts` 为 3，`RetryPolicy` 延迟从 1 秒增长到 60 秒。持久化正延迟时会向上取整到整毫秒，取整增加不足 1 毫秒，因此存储间隔可能略高于 `max_delay`；这不保证墙钟等待时长等于策略值。queued 状态和 `retry_not_before_ms` 一起保存，重启恢复仍遵守截止时间。不可重试错误、panic、取消和超过尝试上限都会结束任务。
- 调用 `resume_blocked(&observed_summary)` 可在修复配置并使用兼容 handler 或 codec 重启服务后重新排队。状态版本冲突时先重新读取摘要；存在待处理取消时不能恢复。
- 后台存储故障会被锁存：停止写入和调度，保留诊断读取；`shutdown()` 排空活跃工作后返回存储故障。SQLite typed schema v4 到 v7 会在原数据库中通过事务迁移到 v8；旧 UUID schema 仍需显式映射数据。
- `TaskContext::progress_builder()` 使用 `rs-progress::AsyncReporter`。`report_async()` 等待进度持久化后才返回。后续查询可看到 stage 和 metric 快照，但它们不会改变生命周期 `state_version`；上报错误会返回处理器。Reporter 在等待 store 前分配下一个进度版本，失败或取消后不会复用。取消等待时，异步 store 可能已经提交，因此后续版本允许出现空洞；进度版本与生命周期 `state_version` 独立。
- 每个运行 attempt 的空进度基线为版本 `0`；首次 `TaskStore::update_progress` 必须使用正版本（通常为 `1`）。同一 attempt 内版本必须严格递增，但允许跳号。此前直接向 `MemoryTaskStore` 写入版本 `0` 的调用方应改为从 `1` 开始；内置 reporter 已遵循此规则。
- typed 历史页按 `(accepted_at_ms, 数值 task id)` 升序排序。`after` 是排他键游标，不是 offset；每次查询读取自己的存储快照。只有存在 lookahead 记录时才有 `next`。过滤器包括 state、业务 `category` 和 correlation key；`kind_id` 用于处理器路由，与 `category` 独立。

service 持久化终态诊断前，会将 `Failed.category` 限制为 128 个 UTF-8 字节，并将其他终态诊断字段限制为 4096 个 UTF-8 字节。文本超限时，会在 UTF-8 字符边界截短，保留能够连同 `...` 放入上限的最长前缀。直接通过 `TaskStore` 写入时，超限诊断仍会被拒绝。service 返回的外部取消 hook 消息也限制为 4096 个 UTF-8 字节；直接 transition 写入更长的 `cancel_error` 会返回 `InvalidRequest`。已有 SQLite 长 hook 诊断仍可读取，service 在 recovery 或结束任务时再次写入该行会先规范化；需要保留完整文本时应由应用写入自己的诊断系统。启用 panic unwind 时，应用 payload decoder 在准备任务时发生 panic，会成为该任务的 `Panicked` 终态；handler panic 和此前一样限制在对应任务内。`panic=abort` 无法捕获。

需要持久生命周期通知时，启用 `event-bus`、注册应用提供的 `TaskEvent` codec，并设置 `TaskExecutionServiceBuilder::event_bus`。任务状态和 SQLite outbox 行会在同一事务中提交，再由后台异步重放。provider 确认 `Accepted` 前，outbox 行会保留并重试，因此发布可能重复；收到 `Accepted` 后，该行会被删除。接纳不保证 Redis 已持久化或消费者已收到，因此不保证通知至少一次送达。这与任务 handler 的至少一次执行不同。消费者应按 `(TaskId, state_version)` 去重，并以任务查询为准。启用 publisher 不会补发此前已提交的历史状态。`MemoryTaskStore` 不支持持久 outbox。配置边界和监控方法见[用户指南](user-guide.zh_CN.md#发布任务生命周期变化)。

应用可通过维护作业调用 `TaskStore::prune_terminal_before(finished_before_ms, max_rows)`，删除最多 `max_rows` 条 finish 时间严格早于 cutoff 的终态记录。SQLite 会跳过仍有活动 outbox 行的任务，并在删除任务前原子保存最小终态状态检查点。检查点只能恢复当前状态，不会自动过期，数量会随清理任务增长。候选按 finish 时间和 task ID 排序，幂等键会在同一操作中删除，因此之后可复用；对应 SQLite `TaskId` 在检查点存在期间仍保留，`MemoryTaskStore` 清理后会释放该 ID。Queued 和 Blocked 不会清理。

消费者遇到版本缺口时调用 `TaskExecutionService::get_state_checkpoint(id)`。任务仍保留时返回当前状态；SQLite 清理后返回终态检查点。若结果为 `None`、查询出错或检查点版本低于通知版本，必须保持未确认并走 Retry。检查点用于刷新当前状态投影，不能重放遗漏的状态迁移及其副作用。

## 0.12 服务、查询、取消与存储 API

`submit` 返回 `AcceptOutcome`，其中包含原子 store 操作返回的 `TaskSummary` 和 `created` 标志。命中幂等请求时，服务返回已有摘要且 `created == false`。`submit_checked` 会先检查当前 handler、model ID、schema 版本和 codec，再执行相同的接纳写入。检查发生在生成 ID 和接受任务之前，因此严格校验失败不会写 store 或发送通知。普通 `submit` 则有意允许提交当前无法路由的请求；调度器会将它记录为 `Blocked`。编码失败以 `TaskServiceError::RequestEncoding(TaskRequestEncodeError)` 保留结构化错误。

需要生命周期、metadata 和进度时调用 `get_summary(id)`；消费者只对账当前状态时使用 `get_state_checkpoint(id)`。`wait_terminal(id, timeout)` 会读取 checkpoint，直到观察到 Succeeded、Failed、Panicked 或 Cancelled。Blocked 不是终态。一个总超时覆盖读取和轮询等待；即使时长为零，也会尝试一次可立即完成的 checkpoint 读取。`Timeout` 表示截止前未观察到终态 checkpoint；`Unavailable` 表示当前没有可读 checkpoint。Memory store 的终态可能在下次轮询前被淘汰；SQLite 清理任务行后仍保留终态 checkpoint。若需超出 store 保留期限保存结果，应使用应用自己的持久业务记录。

`TaskQuery::default()` 默认读取 50 条。查询字段为私有；用 `with_state`、`with_states`、`with_category`、`with_correlation_key`、`after` 和只读 getter 配置或读取条件。`page_size(n)` 只接受 1 到 256。`service.pages(query)` 创建逐页读取的 `TaskPages`，通过 `next_page()` 取下一页。存储读取失败时游标不会前进，可在修复错误后重试；最后一个非空页会先返回，再报告 `None`。

`request_cancel(id)` 表达请求，不表示任务已经结束。Queued 和 Blocked 任务可返回 `CancelledBeforeRun`；运行中的 cooperative handler 返回 `CancellationRequested` 后，调用方可通过 `TaskContext::cancellation()` 获得只读 `TaskCancellation`。`is_requested()` 观察持久取消请求。对于外部取消，服务先持久化意图，再于调用 hook 前将意图发布到该 handle；hook 失败不会清除意图。`wait_requested()` 只等待 cooperative 取消信号，因此外部请求可查询，但不会使该 future 完成。运行中取消不受支持时返回 `CancellationUnsupported`。外部 hook 接收 `TaskAttempt`；同一 attempt 上并发重试会共享由服务托管的 hook 操作。

运维人员应将观察到的完整 `TaskSummary` 传给 `resume_blocked(&summary)`。服务会重新读取当前记录并执行条件更新，陈旧快照会产生冲突，不会覆盖新状态。通知隔离同样接收 `&NotificationFailure` 快照，并原子比较 task ID、state version 和 event ID。

`MemoryStoreOptions::new(history_capacity)` 设置具名配额；`max_payload_bytes` 和 `max_unfinished_records` 会拒绝零值。`SqliteTaskStore::open_async(path)` 将同步打开操作放到 Tokio blocking pool。自定义 store 包装器可以使用 `delegate_task_store!(inner; except [accept_encoded]);`，自动转发其他方法（包括 trait 默认实现），并拒绝未知或重复的例外。


## 提交拒绝与外部取消错误

`submit()` 会把预期的容量和重复请求拒绝映射为 `TaskServiceError::SubmissionCapacityExceeded`、`UnfinishedTaskLimitExceeded`、`IdempotencyConflict` 或 `DuplicateTaskId`。这些拒绝只影响当前请求，服务仍可继续使用。其他 store 故障仍按操作性错误处理，并可能使服务进入锁存不可用状态。

运行中任务使用 `CancellationMode::ExternalHook` 时，`request_cancel()` 会先持久化取消请求，再启动由服务托管的 hook 操作。同一 task `attempt` 的并发调用共享该操作；取消或中止等待 `request_cancel()` 的调用方不会停止 hook，`shutdown()` 会等待 hook 完成。hook 失败时，`request_cancel()` 返回 `TaskServiceError::ExternalCancellationFailed`，错误信息保存在 `TaskSummary.cancel_error`，上限为 4096 个 UTF-8 字节；任务仍在运行时，可再次调用 `request_cancel()` 重试。重试成功会清除该诊断，但 handler 仍负责报告最终状态。请确保 hook 对每个 (`TaskId`, `attempt`) 幂等，因为外部副作用可能已成功、但确认回执丢失。
