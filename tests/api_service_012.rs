use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use qubit_codec::{
    ValueBytesCodecDescriptor, ValueBytesCodecRegistration, ValueBytesCodecRegistry, ValueCodecId,
    ValueCodecRegistrationSource, ValueDecoder, ValueEncoder,
};
use qubit_id::{Id, IdGenerationError, IdGenerator};
use qubit_model_id::{HasModelId, ModelId};
use qubit_task::TaskStore;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::service::WaitTaskError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::{
    TaskExecutionServiceBuilder, TaskHandlerDescriptor, TaskId, TaskKind, TaskPages, TaskQuery,
};

#[derive(Clone)]
struct Input(u32);
impl HasModelId for Input {
    const MODEL_ID: ModelId = ModelId::new("example.Input");
}
#[derive(Default)]
struct Codec;
impl ValueEncoder<Input> for Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;
    fn encode(&mut self, v: &Input) -> Result<Vec<u8>, Self::Error> {
        Ok(v.0.to_le_bytes().to_vec())
    }
}
impl ValueDecoder<[u8]> for Codec {
    type Output = Input;
    type Error = std::io::Error;
    fn decode(&mut self, b: &[u8]) -> Result<Input, Self::Error> {
        Ok(Input(u32::from_le_bytes(
            b.try_into().map_err(std::io::Error::other)?,
        )))
    }
}
static DESC: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<Codec, Input>();
static CODEC: ValueBytesCodecRegistration = ValueBytesCodecRegistration::new(
    ValueCodecId::new("example.codec"),
    &DESC,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "api_service_012",
        "tests/api_service_012.rs",
        1,
    ),
);
struct Ids(AtomicU64);
impl IdGenerator<Id, IdGenerationError> for Ids {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

async fn service() -> (
    qubit_task::TaskExecutionService,
    Arc<MemoryTaskStore>,
    Arc<Ids>,
) {
    let store = Arc::new(MemoryTaskStore::new(16));
    let ids = Arc::new(Ids(AtomicU64::new(700)));
    let mut builder = TaskExecutionServiceBuilder::new(
        store.clone(),
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC]).unwrap()),
        ids.clone(),
    );
    builder
        .register_handler_fn::<Input, _, _>(
            TaskHandlerDescriptor::for_payload::<Input>("known", 1),
            |_, _| async { Ok(TaskRunOutcome::success()) },
        )
        .unwrap();
    (builder.build().await.unwrap(), store, ids)
}

#[tokio::test]
async fn public_service_exposes_paged_queries_and_terminal_wait_errors() {
    let (service, _, _) = service().await;
    let mut pages: TaskPages<'_> = service.pages(TaskQuery::default());
    assert!(pages.next_page().await.expect("query succeeds").is_none());
    let error = service
        .wait_terminal(TaskId::from_id(Id::new(999)), Duration::ZERO)
        .await
        .expect_err("unknown task is unavailable");
    assert!(matches!(error, WaitTaskError::Unavailable(_)));
    service.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test]
async fn strict_invalid_route_has_no_id_or_accept_side_effect_but_submit_blocks() {
    let (service, store, ids) = service().await;
    let invalid =
        TaskKind::<Input>::new("missing", 1, ValueCodecId::new("example.codec")).request(Input(3));
    assert!(matches!(
        service.submit_checked(invalid.clone()).await,
        Err(qubit_task::TaskServiceError::InvalidRoute(_))
    ));
    let zero_schema =
        TaskKind::<Input>::new("known", 0, ValueCodecId::new("example.codec")).request(Input(4));
    assert!(matches!(
        service.submit_checked(zero_schema).await,
        Err(qubit_task::TaskServiceError::InvalidRoute(
            qubit_task::TaskRouteError::UnsupportedSchemaVersion { version: 0 }
        ))
    ));
    assert_eq!(ids.0.load(Ordering::Relaxed), 700);
    assert!(
        service
            .query(TaskQuery::default())
            .await
            .unwrap()
            .records
            .is_empty()
    );
    let accepted = service
        .submit(invalid)
        .await
        .expect("ordinary submission is accepted");
    assert_eq!(ids.0.load(Ordering::Relaxed), 701);
    let id = accepted.summary.id;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if service
                .get_summary(id)
                .await
                .unwrap()
                .is_some_and(|s| matches!(s.state, qubit_task::TaskState::Blocked { .. }))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scheduler blocks invalid route");
    assert!(matches!(
        service.wait_terminal(id, Duration::from_millis(40)).await,
        Err(WaitTaskError::Timeout(_))
    ));
    assert!(store.get_summary(id).await.unwrap().is_some());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn pages_deliver_every_record_and_wait_terminal_accepts_immediate_terminal_reads() {
    let (service, _, _) = service().await;
    for n in 0..2 {
        let request = TaskKind::<Input>::new("known", 1, ValueCodecId::new("example.codec"))
            .request(Input(n));
        let accepted = service.submit(request).await.expect("request accepted");
        let checkpoint = service
            .wait_terminal(accepted.summary.id, Duration::from_secs(2))
            .await
            .expect("task reaches terminal state");
        assert!(checkpoint.state.is_terminal());
        let immediate = service
            .wait_terminal(accepted.summary.id, Duration::ZERO)
            .await
            .expect("zero timeout allows an immediate terminal read");
        assert_eq!(immediate.id, accepted.summary.id);
    }
    let query = TaskQuery::default().page_size(1).unwrap();
    let mut pages = service.pages(query);
    let first = pages
        .next_page()
        .await
        .unwrap()
        .expect("first nonempty page");
    let second = pages
        .next_page()
        .await
        .unwrap()
        .expect("last nonempty page is returned");
    assert_eq!(first.records.len(), 1);
    assert_eq!(second.records.len(), 1);
    assert!(pages.next_page().await.unwrap().is_none());
    service.shutdown().await.unwrap();
}
