# Typed task API

The typed API keeps application values typed until acceptance. Payload identity has three independent parts: `type_id` identifies the model, `schema_version` identifies its schema, and `codec_id` identifies the bytes encoding. A handler is registered by `kind_id`, accepts exactly one payload `type_id`, and declares the schema versions it supports. A codec can support many schema versions; schema compatibility belongs to the handler descriptor.

`Payload<T>` carries the value. `TaskRequest<T>::encode` resolves a `ValueBytesCodecDescriptor` from `ValueBytesCodecRegistry` and creates an `EncodedPayload<T>`; storage receives its type-erased `StoredPayload`. The bytes registry uses `ValueEncoder<T>` and `ValueDecoder<[u8]>`. Task metadata uses `rs-metadata::Metadata` and is limited to 32 entries and 16 KiB serialized bytes by the task request, in addition to `rs-metadata`'s wire budgets.

## End-to-end outline

This example follows one request from a typed value through codec registration and handler execution. Application code injects the ID generator. In production, a Snowflake-style generator requires distinct node IDs for each process and suitable clock configuration across those processes.

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
            let _ = (input.image, input.width); // perform application work here
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
    // A REST endpoint may call tasks.cancel(accepted.id).
    Ok(())
}
```

## Runtime contracts

- `TaskId` wraps `rs-id::Id`; the service requires an injected `IdGenerator`. Snowflake cross-process uniqueness depends on distinct node IDs and clock conditions. `to_padded_decimal()` produces a fixed-width decimal key for stable lexical database ordering.
- CPU slots, GPU devices and labels, optional memory/disk bytes, and custom integer units are admission quotas reserved for each attempt. They account for concurrent reservations; they do not pin CPUs, discover or isolate GPUs at the OS level, or enforce actual process memory/disk usage. A request above configured capacity is unsatisfiable; a fitting request waits while capacity is occupied.
- Queued or blocked tasks can be cancelled directly. For running work, `CancellationMode::Cooperative` signals `TaskContext::is_cancelled()` and the handler must stop at a safe boundary and return `TaskRunOutcome::Cancelled`. `ExternalHook` requires a registered external hook. `Unsupported` cannot stop a running attempt.
- `TaskContext::progress_builder()` uses `rs-progress::AsyncReporter`. `report_async()` awaits persistence before returning. Current stage and metric snapshots appear on later task reads without changing lifecycle `state_version`; reporting errors are returned to the handler.
- Typed history pages sort ascending by `(accepted_at_ms, numeric task id)`. `after` is an exclusive key cursor, not an offset; each query observes its own storage snapshot. `next` is present only when a lookahead row exists. Filters include state, business `category`, and correlation key. `kind_id` routes to a handler and is independent of `category`.

The Redis notification fixture verifies typed `TaskEvent` transport and consumer behavior. Lifecycle publication is not currently wired into the typed execution service, so consumers must query the service for authoritative state.
