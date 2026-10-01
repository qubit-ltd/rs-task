use std::collections::HashMap;
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use parking_lot::Mutex;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_progress::AsyncReporter;

use crate::engine::EngineError;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::TaskRunOutcome;
use crate::handler::typed::CancellationMode;
use crate::handler::typed::TypedTaskContext;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::next::TaskId;
use crate::model::next::TaskPage;
use crate::model::next::TaskQuery;
use crate::model::next::TaskRequest;
use crate::model::next::TaskSummary;
use crate::model::next::TransitionCommand;
use crate::service::CancelOutcome;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

struct TypedServiceCore {
    store: Arc<dyn TaskStore>,
    codecs: Arc<ValueBytesCodecRegistry>,
    id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    engine: Arc<LocalTaskExecutionEngine>,
    handlers: TypedTaskHandlerRegistry,
    cancellations: Mutex<HashMap<(TaskId, u32), Arc<AtomicBool>>>,
    owner: OwnerEpoch,
    admission: tokio::sync::RwLock<()>,
    dispatch_lock: Mutex<HashSet<TaskId>>,
    in_flight: AtomicUsize,
    in_flight_changed: tokio::sync::Notify,
    shutting_down: AtomicBool,
    shutdown_lock: tokio::sync::Mutex<()>,
    owner_released: AtomicBool,
}

struct InFlightGuard(Arc<TypedServiceCore>, TaskId);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.dispatch_lock.lock().remove(&self.1);
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.0.in_flight_changed.notify_waiters();
    }
}

/// Typed task submit, dispatch, execution, and cancellation facade.
#[derive(Clone)]
pub struct TypedTaskExecutionService {
    core: Arc<TypedServiceCore>,
}

impl TypedTaskExecutionService {
    pub(super) async fn new(
        store: Arc<dyn TaskStore>,
        codecs: Arc<ValueBytesCodecRegistry>,
        id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
        engine: Arc<LocalTaskExecutionEngine>,
        handlers: TypedTaskHandlerRegistry,
    ) -> Result<Self, TaskServiceError> {
        let owner = store.acquire_owner().await?;
        let service = Self {
            core: Arc::new(TypedServiceCore {
                store,
                codecs,
                id_generator,
                engine,
                handlers,
                cancellations: Mutex::new(HashMap::new()),
                owner,
                admission: tokio::sync::RwLock::new(()),
                dispatch_lock: Mutex::new(HashSet::new()),
                in_flight: AtomicUsize::new(0),
                in_flight_changed: tokio::sync::Notify::new(),
                shutting_down: AtomicBool::new(false),
                shutdown_lock: tokio::sync::Mutex::new(()),
                owner_released: AtomicBool::new(false),
            }),
        };
        if let Err(error) = service.recover_unfinished().await {
            let release = service.core.store.release_owner(owner).await;
            if let Err(release_error) = release {
                return Err(TaskServiceError::Store(release_error));
            }
            return Err(error);
        }
        Ok(service)
    }

    /// Encodes, assigns an ID, durably accepts, and dispatches a typed request.
    pub async fn submit<T: Send + Sync + 'static>(
        &self,
        request: TaskRequest<T>,
    ) -> Result<TaskSummary, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        if self.core.shutting_down.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let stored = request
            .encode(&self.core.codecs)
            .map_err(|error| TaskServiceError::TypedRequest(error.to_string()))?;
        let id = TaskId::from_id(self.core.id_generator.generate()?);
        let accepted = self.core.store.accept_encoded(id, stored).await?;
        if accepted.created {
            self.dispatch(accepted.summary.id);
        }
        Ok(accepted.summary)
    }

    /// Returns a typed task's current persisted summary.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskSummary>, TaskServiceError> {
        Ok(self.core.store.get_encoded_task(id).await?.map(|task| task.summary))
    }

    /// Queries typed task summaries using category filters and numeric cursors.
    pub async fn query(&self, query: TaskQuery) -> Result<TaskPage, TaskServiceError> {
        Ok(self.core.store.list_encoded(query).await?)
    }

    /// Stops submissions and dispatch, waits for active attempts, then releases
    /// exclusive store ownership. Queued tasks remain recoverable for the next
    /// service owner.
    pub async fn shutdown(&self) -> Result<(), TaskServiceError> {
        let _shutdown = self.core.shutdown_lock.lock().await;
        if self.core.owner_released.load(Ordering::Acquire) {
            return Ok(());
        }
        {
            let _admission = self.core.admission.write().await;
            let _dispatch = self.core.dispatch_lock.lock();
            self.core.shutting_down.store(true, Ordering::Release);
        }
        loop {
            let notified = self.core.in_flight_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
        self.core.store.release_owner(self.core.owner).await?;
        self.core.owner_released.store(true, Ordering::Release);
        Ok(())
    }

    /// Requests cancellation, persisting intent before signalling a handler.
    pub async fn cancel(&self, id: TaskId) -> Result<CancelOutcome, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let Some(task) = self.core.store.get_encoded_task(id).await? else {
            return Err(TaskServiceError::Store(crate::store::StoreError::NotFound));
        };
        let summary = task.summary;
        if summary.state.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal);
        }
        if matches!(summary.state, TaskState::Queued | TaskState::Blocked { .. }) {
            let updated = self
                .core
                .store
                .transition_encoded(TransitionCommand {
                    id,
                    expected_state_version: summary.state_version,
                    expected_attempt: summary.attempt,
                    state: TaskState::Cancelled,
                    cancel_requested: false,
                    cancel_error: None,
                    finished_at_ms: Some(now_ms()),
                    output: None,
                })
                .await?;
            let _ = updated;
            return Ok(CancelOutcome::CancelledBeforeStart);
        }
        if summary.cancel_requested {
            if summary.cancel_error.is_some() {
                return Err(TaskServiceError::TypedRequest(
                    "external cancellation hook previously failed".into(),
                ));
            }
            return Ok(CancelOutcome::CancellationRequested);
        }
        let descriptor = self
            .core
            .handlers
            .descriptor(&summary.kind_id)
            .ok_or(TaskServiceError::CancellationUnsupported)?;
        if descriptor.cancellation_mode == CancellationMode::Unsupported {
            return Ok(CancelOutcome::CancellationUnsupported);
        }
        self.core
            .store
            .transition_encoded(TransitionCommand {
                id,
                expected_state_version: summary.state_version,
                expected_attempt: summary.attempt,
                state: TaskState::Running,
                cancel_requested: true,
                cancel_error: None,
                finished_at_ms: None,
                output: None,
            })
            .await?;
        match descriptor.cancellation_mode {
            CancellationMode::Unsupported => Ok(CancelOutcome::CancellationUnsupported),
            CancellationMode::Cooperative => {
                if let Some(signal) = self.core.cancellations.lock().get(&(id, summary.attempt)) {
                    signal.store(true, Ordering::Release);
                }
                Ok(CancelOutcome::CancellationRequested)
            }
            CancellationMode::ExternalHook => {
                let hook = self
                    .core
                    .handlers
                    .cancel_externally(&summary.kind_id, id, summary.attempt)
                    .ok_or(TaskServiceError::CancellationUnsupported)?;
                if let Err(error) = hook.await {
                    let diagnostic = error.message;
                    let latest = self
                        .core
                        .store
                        .get_encoded_task(id)
                        .await?
                        .ok_or(crate::store::StoreError::NotFound)?;
                    self.core
                        .store
                        .transition_encoded(TransitionCommand {
                            id,
                            expected_state_version: latest.summary.state_version,
                            expected_attempt: latest.summary.attempt,
                            state: latest.summary.state,
                            cancel_requested: true,
                            cancel_error: Some(diagnostic),
                            finished_at_ms: None,
                            output: None,
                        })
                        .await?;
                    return Err(TaskServiceError::TypedRequest(
                        "external cancellation hook failed".into(),
                    ));
                }
                Ok(CancelOutcome::CancellationRequested)
            }
        }
    }

    fn dispatch(&self, id: TaskId) {
        {
            let mut dispatched = self.core.dispatch_lock.lock();
            if self.core.shutting_down.load(Ordering::Acquire) || !dispatched.insert(id) {
                return;
            }
            self.core.in_flight.fetch_add(1, Ordering::AcqRel);
        }
        let service = self.clone();
        let core = Arc::clone(&self.core);
        tokio::spawn(async move {
            let _in_flight = InFlightGuard(core, id);
            service.run_one(id).await;
        });
    }

    async fn recover_unfinished(&self) -> Result<(), TaskServiceError> {
        let mut after = None;
        let mut dispatch = Vec::new();
        loop {
            let page = self
                .core
                .store
                .list_encoded(crate::model::next::TaskQuery {
                    states: vec![
                        crate::model::TaskStateKind::Queued,
                        crate::model::TaskStateKind::Running,
                    ],
                    category: None,
                    correlation_key: None,
                    after,
                    limit: 128,
                })
                .await?;
            for summary in page.records {
                if matches!(summary.state, TaskState::Running) {
                    let (state, keep_cancel) = if summary.cancel_requested {
                        (
                            TaskState::Blocked {
                                reason: "service stopped while cancellation was pending".into(),
                            },
                            true,
                        )
                    } else {
                        (TaskState::Queued, false)
                    };
                    let recovered = self
                        .core
                        .store
                        .transition_encoded(TransitionCommand {
                            id: summary.id,
                            expected_state_version: summary.state_version,
                            expected_attempt: summary.attempt,
                            state,
                            cancel_requested: keep_cancel,
                            cancel_error: summary.cancel_error.clone(),
                            finished_at_ms: None,
                            output: None,
                        })
                        .await?;
                    if matches!(recovered.state, TaskState::Queued) {
                        dispatch.push(recovered.id);
                    }
                } else {
                    dispatch.push(summary.id);
                }
            }
            after = page.next;
            if after.is_none() {
                for id in dispatch {
                    self.dispatch(id);
                }
                return Ok(());
            }
        }
    }

    async fn run_one(&self, id: TaskId) {
        'retry: loop {
            if self.core.shutting_down.load(Ordering::Acquire) {
                return;
            }
            let Ok(Some(task)) = self.core.store.get_encoded_task(id).await else {
                return;
            };
            let summary = task.summary;
            if !matches!(summary.state, TaskState::Queued) {
                return;
            }
            let Some(descriptor) = self.core.handlers.descriptor(&summary.kind_id) else {
                self.mark_blocked(&summary, "handler is not registered").await;
                return;
            };
            if summary.payload_type_id != descriptor.payload_type_id.as_str()
                || !descriptor
                    .accepted_schema_versions
                    .contains(&summary.payload_schema_version)
            {
                self.mark_blocked(
                    &summary,
                    "handler does not accept the payload identity or schema version",
                )
                .await;
                return;
            }
            if self.core.codecs.get(&summary.payload_codec_id).is_none() {
                self.mark_blocked(&summary, "payload bytes codec is not registered")
                    .await;
                return;
            }
            let reservation = match self
                .core
                .engine
                .try_prepare_typed(id, task.request.resource_limit.clone())
            {
                Ok(reservation) => reservation,
                Err(EngineError::Unsatisfiable) => {
                    self.mark_blocked(&summary, "requested resources exceed configured capacity")
                        .await;
                    return;
                }
                Err(EngineError::TemporarilyUnavailable) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue 'retry;
                }
                Err(error) => {
                    self.mark_blocked(&summary, &error.to_string()).await;
                    return;
                }
            };
            let admission = self.core.admission.read().await;
            if self.core.shutting_down.load(Ordering::Acquire) {
                drop(reservation);
                return;
            }
            let running = match self
                .core
                .store
                .start_encoded(crate::model::next::StartCommand {
                    id,
                    expected_state_version: summary.state_version,
                    started_at_ms: now_ms(),
                })
                .await
            {
                Ok(summary) => summary,
                Err(_) => return,
            };
            drop(admission);
            let prepared = match prepare_handler(
                &self.core.handlers,
                &self.core.codecs,
                &running.kind_id,
                task.request.payload,
            ) {
                Ok(prepared) => prepared,
                Err(message) => {
                    self.finish(
                        &running,
                        TaskState::Failed {
                            category: "payload_decode".into(),
                            message,
                        },
                        None,
                    )
                    .await;
                    return;
                }
            };
            let cancelled = Arc::new(AtomicBool::new(false));
            self.core
                .cancellations
                .lock()
                .insert((id, running.attempt), Arc::clone(&cancelled));
            let latest_cancel_requested = self
                .core
                .store
                .get_encoded_task(id)
                .await
                .ok()
                .flatten()
                .is_some_and(|task| task.summary.cancel_requested);
            if latest_cancel_requested {
                cancelled.store(true, Ordering::Release);
            }
            let reporter: Arc<dyn AsyncReporter> = Arc::new(super::task_progress_reporter::TaskProgressReporter::new(
                Arc::clone(&self.core.store),
                id,
                running.attempt,
            ));
            let context = TypedTaskContext::new(id, running.attempt, cancelled, reporter);
            let execution = AssertUnwindSafe(prepared.run(context)).catch_unwind().await;
            self.core.cancellations.lock().remove(&(id, running.attempt));
            drop(reservation);
            match execution {
                Ok(Ok(TaskRunOutcome::Succeeded(output))) => {
                    if output.summary.len() <= MAX_TASK_OUTPUT_SUMMARY_BYTES {
                        self.finish(&running, TaskState::Succeeded, Some(output)).await;
                    } else {
                        self.finish(
                            &running,
                            TaskState::Failed {
                                category: "task_output_too_large".into(),
                                message: format!(
                                    "task output summary exceeds the {MAX_TASK_OUTPUT_SUMMARY_BYTES}-byte limit"
                                ),
                            },
                            None,
                        )
                        .await;
                    }
                }
                Ok(Ok(TaskRunOutcome::Cancelled)) => self.finish(&running, TaskState::Cancelled, None).await,
                Ok(Err(error)) => {
                    self.finish(
                        &running,
                        TaskState::Failed {
                            category: error.category,
                            message: error.message,
                        },
                        None,
                    )
                    .await
                }
                Err(_) => {
                    self.finish(
                        &running,
                        TaskState::Panicked {
                            message: "typed task handler panicked".into(),
                        },
                        None,
                    )
                    .await
                }
            }
            return;
        }
    }

    async fn mark_blocked(&self, summary: &TaskSummary, reason: &str) {
        let _ = self
            .core
            .store
            .transition_encoded(TransitionCommand {
                id: summary.id,
                expected_state_version: summary.state_version,
                expected_attempt: summary.attempt,
                state: TaskState::Blocked { reason: reason.into() },
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: None,
                output: None,
            })
            .await;
    }

    async fn finish(&self, started: &TaskSummary, state: TaskState, output: Option<TaskOutput>) {
        let Ok(Some(latest)) = self.core.store.get_encoded_task(started.id).await else {
            return;
        };
        if latest.summary.state.is_terminal() {
            return;
        }
        let _ = self
            .core
            .store
            .transition_encoded(TransitionCommand {
                id: started.id,
                expected_state_version: latest.summary.state_version,
                expected_attempt: started.attempt,
                state,
                cancel_requested: latest.summary.cancel_requested,
                cancel_error: latest.summary.cancel_error,
                finished_at_ms: Some(now_ms()),
                output,
            })
            .await;
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn prepare_handler(
    handlers: &TypedTaskHandlerRegistry,
    codecs: &ValueBytesCodecRegistry,
    kind_id: &str,
    payload: crate::model::next::StoredPayload,
) -> Result<crate::handler::typed::PreparedTask, String> {
    handlers
        .prepare(kind_id, payload, codecs)
        .map_err(|error| error.to_string())
}
