use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_model_metadata::metadata::ModelId;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;

#[derive(Default)]
struct U32Codec;

impl qubit_codec::ValueEncoder<u32> for U32Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &u32) -> Result<Vec<u8>, Self::Error> {
        Ok(value.to_le_bytes().to_vec())
    }
}

impl qubit_codec::ValueDecoder<[u8]> for U32Codec {
    type Output = u32;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<u32, Self::Error> {
        let raw: [u8; 4] = bytes.try_into()?;
        Ok(u32::from_le_bytes(raw))
    }
}

static CODEC_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<U32Codec, u32>();
static CODEC_REGISTRATION: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("qubit_task.typed_resource_tests.u32"),
    &CODEC_DESCRIPTOR,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "typed_resource_reservation_tests",
        "tests/service/typed_resource_reservation_tests.rs",
        1,
    ),
);

struct Ids(AtomicU64);

impl qubit_id::IdGenerator for Ids {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

fn descriptor() -> TaskHandlerDescriptor {
    TaskHandlerDescriptor {
        kind_id: "test.resource-reservation".into(),
        payload_type_id: ModelIdBuf::parse("test.ResourceReservationPayload").unwrap(),
        accepted_schema_versions: vec![1],
        cancellation_mode: CancellationMode::Unsupported,
    }
}

fn request(cpu_slots: u32) -> TaskRequest<u32> {
    let mut request = TaskRequest::new(
        "test.resource-reservation",
        ModelId::new("test.ResourceReservationPayload"),
        1,
        ValueCodecId::new("qubit_task.typed_resource_tests.u32"),
        7,
    );
    request.resource_limit = ResourceRequest {
        cpu_slots,
        ..ResourceRequest::default()
    };
    request
}

async fn wait_for(
    service: &qubit_task::TaskExecutionService,
    id: qubit_task::model::TaskId,
    predicate: impl Fn(&TaskState) -> bool,
) -> TaskState {
    for _ in 0..1000 {
        if let Some(summary) = service.get(id).await.unwrap()
            && predicate(&summary.state)
        {
            return summary.state;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("typed task did not reach the expected state")
}

struct GatedHandler {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    calls: Arc<AtomicUsize>,
}

impl TaskHandler<u32> for GatedHandler {
    fn run<'a>(&'a self, _value: u32, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.started.notify_one();
                self.release
                    .acquire()
                    .await
                    .expect("release permit remains open")
                    .forget();
            }
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

#[tokio::test]
async fn typed_resource_request_above_capacity_is_blocked() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(8)),
        codecs,
        Arc::new(Ids(AtomicU64::new(1))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    });
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();

    let accepted = service.submit(request(2)).await.unwrap();
    let state = wait_for(&service, accepted.id, |state| {
        matches!(state, TaskState::Blocked { .. })
    })
    .await;
    assert!(matches!(state, TaskState::Blocked { .. }));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_resource_reservation_waits_until_capacity_is_released() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(8)),
        codecs,
        Arc::new(Ids(AtomicU64::new(10))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    });
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                calls,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();

    let first = service.submit(request(1)).await.unwrap();
    started.notified().await;
    assert!(matches!(
        wait_for(&service, first.id, |state| matches!(state, TaskState::Running)).await,
        TaskState::Running
    ));

    let second = service.submit(request(1)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(matches!(
        service.get(second.id).await.unwrap().unwrap().state,
        TaskState::Queued
    ));

    release.add_permits(1);
    assert_eq!(
        wait_for(&service, first.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    assert_eq!(
        wait_for(&service, second.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    service.shutdown().await.unwrap();
}
