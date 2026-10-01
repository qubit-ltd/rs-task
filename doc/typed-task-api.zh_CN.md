# 带类型的任务 API

带类型 API 会在接纳请求前保留应用值的类型。Payload 身份分成三个独立字段：`type_id` 标识模型，`schema_version` 标识 schema 版本，`codec_id` 标识字节编码。处理器按 `kind_id` 注册，只接受一个 payload `type_id`，并声明所支持的 schema 版本集合。一个 codec 可以支持多个 schema 版本；兼容范围属于处理器描述符。

`Payload<T>` 持有业务值。`TaskRequest<T>::encode` 从 `ValueBytesCodecRegistry` 查找 `ValueBytesCodecDescriptor` 并生成 `EncodedPayload<T>`；存储层接收类型擦除后的 `StoredPayload`。bytes 注册表使用 `ValueEncoder<T>` 和 `ValueDecoder<[u8]>`。任务 metadata 使用 `rs-metadata::Metadata`，任务请求限制最多 32 个条目、序列化后 16 KiB；同时遵守 `rs-metadata` 自身 wire 配额。

## 端到端示例

示例展示从带类型的值、codec 注册到处理器执行的完整路径。应用显式注入 ID generator。生产环境使用 Snowflake 类生成器时，不同进程必须分配不同节点 ID，并满足时钟配置要求。

```rust,no_run
use std::sync::Arc;
use qubit_codec::{ValueBytesCodecDescriptor, ValueBytesCodecRegistration, ValueBytesCodecRegistry, ValueCodecId, ValueCodecRegistration, ValueCodecRegistrationSource};
use qubit_model_metadata::metadata::{ModelId, ModelIdBuf};
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
    let mut request = TaskRequest::new("images.resize", ModelId::new("example.Resize"), 2,
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

- `TaskId` 包装 `rs-id::Id`；service 要求显式注入 `IdGenerator`。Snowflake 跨进程唯一性依赖节点 ID 不重复和时钟条件。`to_padded_decimal()` 输出定长十进制字符串，用于数据库字典序稳定排序。
- CPU slot、GPU 设备和标签、可选 memory/disk 字节数、自定义整数单位都是每次执行的接纳配额。它们限制并发预留量，不会绑核、在 OS 层发现或隔离 GPU，也不会限制进程实际内存或磁盘使用。超过配置容量的请求不可满足；总量可满足但暂时被占用的请求会等待。
- 排队或阻塞任务可直接取消。运行中任务使用 `CancellationMode::Cooperative` 时，`TaskContext::is_cancelled()` 会收到信号，处理器应在安全边界停止并返回 `TaskRunOutcome::Cancelled`。`ExternalHook` 需要注册外部 hook；`Unsupported` 不会停止运行中的尝试。
- `TaskContext::progress_builder()` 使用 `rs-progress::AsyncReporter`。`report_async()` 等待进度持久化后才返回。后续查询可看到 stage 和 metric 快照，但它们不会改变生命周期 `state_version`；上报错误会返回处理器。
- typed 历史页按 `(accepted_at_ms, 数值 task id)` 升序排序。`after` 是排他键游标，不是 offset；每次查询读取自己的存储快照。只有存在 lookahead 记录时才有 `next`。过滤器包括 state、业务 `category` 和 correlation key；`kind_id` 用于处理器路由，与 `category` 独立。

Redis 通知 fixture 验证 typed `TaskEvent` 传输和消费者行为。目前生命周期事件尚未接入 typed execution service，因此消费者应通过服务查询接口读取权威状态。
