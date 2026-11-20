// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::TaskId;
use qubit_task::TaskKind;
use qubit_task::TaskPages;
use qubit_task::TaskQuery;
use qubit_task::TaskStore;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::TaskCursor;
use qubit_task::service::WaitTaskError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::delegate_task_store;

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
        Ok(Input(u32::from_le_bytes(b.try_into().map_err(std::io::Error::other)?)))
    }
}
static DESC: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<Codec, Input>();
static CODEC: ValueBytesCodecRegistration = ValueBytesCodecRegistration::new(
    ValueCodecId::new("example.codec"),
    &DESC,
    ValueCodecRegistrationSource::new("qubit-task", "api_service_012", "tests/api_service_012_tests.rs", 1),
);
struct Ids(AtomicU64);
impl IdGenerator<Id, IdGenerationError> for Ids {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

async fn service() -> (qubit_task::TaskExecutionService, Arc<MemoryTaskStore>, Arc<Ids>) {
    let store = Arc::new(MemoryTaskStore::new(16));
    let ids = Arc::new(Ids(AtomicU64::new(700)));
    let mut builder = TaskExecutionServiceBuilder::new(
        store.clone(),
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC]).unwrap()),
        ids.clone(),
    );
    builder
        .register_handler_fn::<Input, _, _>(TaskHandlerDescriptor::for_payload::<Input>("known", 1), |_, _| async {
            Ok(TaskRunOutcome::success())
        })
        .unwrap();
    (builder.build().await.unwrap(), store, ids)
}

struct FailListOnceStore {
    inner: Arc<MemoryTaskStore>,
    calls: AtomicUsize,
    cursors: Mutex<Vec<Option<TaskCursor>>>,
}

impl qubit_task::store::TaskStore for FailListOnceStore {
    delegate_task_store!(inner; except [list_encoded]);

    fn list_encoded<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<qubit_task::model::TaskPage, StoreError>> {
        if query.limit() == 1 {
            self.cursors.lock().unwrap().push(query.cursor());
        }
        if query.limit() == 1 && self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            return Box::pin(async { Err(StoreError::Failure("controlled list failure".into())) });
        }
        self.inner.list_encoded(query)
    }
}

#[tokio::test]
async fn test_public_service_exposes_paged_queries_and_terminal_wait_errors() {
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
async fn test_strict_invalid_route_has_no_id_or_accept_side_effect_but_submit_blocks() {
    let (service, store, ids) = service().await;
    let invalid = TaskKind::<Input>::new("missing", 1, ValueCodecId::new("example.codec")).request(Input(3));
    assert!(matches!(
        service.submit_checked(invalid.clone()).await,
        Err(qubit_task::TaskServiceError::InvalidRoute(_))
    ));
    let zero_schema = TaskKind::<Input>::new("known", 0, ValueCodecId::new("example.codec")).request(Input(4));
    assert!(matches!(
        service.submit_checked(zero_schema).await,
        Err(qubit_task::TaskServiceError::InvalidRoute(
            qubit_task::TaskRouteError::UnsupportedSchemaVersion { version: 0 }
        ))
    ));
    assert_eq!(ids.0.load(Ordering::Relaxed), 700);
    assert!(service.query(TaskQuery::default()).await.unwrap().records.is_empty());
    let accepted = service.submit(invalid).await.expect("ordinary submission is accepted");
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
async fn test_pages_deliver_every_record_and_wait_terminal_accepts_immediate_terminal_reads() {
    let (service, _, _) = service().await;
    for n in 0..2 {
        let request = TaskKind::<Input>::new("known", 1, ValueCodecId::new("example.codec")).request(Input(n));
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
    let first = pages.next_page().await.unwrap().expect("first nonempty page");
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

#[tokio::test]
async fn test_pages_retry_after_store_error_reuses_the_observed_cursor() {
    let inner = Arc::new(MemoryTaskStore::new(16));
    let store = Arc::new(FailListOnceStore {
        inner,
        calls: AtomicUsize::new(0),
        cursors: Mutex::new(Vec::new()),
    });
    let ids = Arc::new(Ids(AtomicU64::new(900)));
    let mut builder = TaskExecutionServiceBuilder::new(
        store.clone(),
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC]).unwrap()),
        ids,
    );
    builder
        .register_handler_fn::<Input, _, _>(TaskHandlerDescriptor::for_payload::<Input>("known", 1), |_, _| async {
            Ok(TaskRunOutcome::success())
        })
        .unwrap();
    let service = builder.build().await.unwrap();
    for n in 0..2 {
        let request = TaskKind::<Input>::new("known", 1, ValueCodecId::new("example.codec")).request(Input(n));
        let accepted = service.submit(request).await.unwrap();
        service
            .wait_terminal(accepted.summary.id, Duration::from_secs(2))
            .await
            .unwrap();
    }
    let mut pages = service.pages(TaskQuery::default().page_size(1).unwrap());
    let first = pages.next_page().await.unwrap().expect("first page");
    assert_eq!(first.records.len(), 1);
    assert!(matches!(
        pages.next_page().await,
        Err(qubit_task::TaskServiceError::Store(StoreError::Failure(_)))
    ));
    let second = pages.next_page().await.unwrap().expect("retry returns the second page");
    assert_eq!(second.records.len(), 1);
    let cursors = store.cursors.lock().unwrap();
    assert_eq!(cursors.len(), 3);
    assert_eq!(cursors[1], cursors[2]);
    assert!(cursors[1].is_some());
    drop(cursors);
    service.shutdown().await.unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_wait_terminal_reads_checkpoint_after_sqlite_prunes_terminal_row() {
    let directory = std::env::temp_dir().join(format!(
        "qubit-task-api-service-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("tasks.sqlite");
    let store = Arc::new(qubit_task::store::SqliteTaskStore::open(&path).unwrap());
    let mut builder = TaskExecutionServiceBuilder::new(
        store.clone(),
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC]).unwrap()),
        Arc::new(Ids(AtomicU64::new(1_900))),
    );
    builder
        .register_handler_fn::<Input, _, _>(TaskHandlerDescriptor::for_payload::<Input>("known", 1), |_, _| async {
            Ok(TaskRunOutcome::success())
        })
        .unwrap();
    let service = builder.build().await.unwrap();
    let request = TaskKind::<Input>::new("known", 1, ValueCodecId::new("example.codec")).request(Input(9));
    let accepted = service.submit(request).await.unwrap();
    let terminal = service
        .wait_terminal(accepted.summary.id, Duration::from_secs(2))
        .await
        .unwrap();
    let cutoff_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 1;
    assert_eq!(
        store
            .prune_terminal_before(cutoff_ms, std::num::NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        1
    );
    assert!(store.get_summary(accepted.summary.id).await.unwrap().is_none());
    assert_eq!(
        service
            .wait_terminal(accepted.summary.id, Duration::ZERO)
            .await
            .unwrap(),
        terminal
    );
    service.shutdown().await.unwrap();
    drop(service);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}
