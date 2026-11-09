// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Persists `rs-progress` events as bounded task progress snapshots.

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_progress::AsyncReporter;
use qubit_progress::Event;
use qubit_progress::ReportFuture;
use qubit_progress::ReporterError;

use crate::model::typed::ProgressCommand;
use crate::model::typed::TaskId;
use crate::store::TaskStore;

/// Asynchronous reporter that writes progress snapshots to a task store.
///
/// Each reporter belongs to one running task attempt. Progress versions are
/// allocated serially across all progress operations created by one task
/// context, starting above the store's empty baseline version of zero.
pub(in crate::service) struct TaskProgressReporter {
    /// Store that receives progress snapshots.
    store: Arc<dyn TaskStore>,
    /// Identifier of the task associated with this reporter.
    id: TaskId,
    /// Attempt number associated with the reporter's progress events.
    attempt: u32,
    /// Serializes version allocation and tracks the highest allocated version.
    progress_version: tokio::sync::Mutex<u64>,
}

impl TaskProgressReporter {
    /// Creates a reporter bound to one task attempt.
    ///
    /// # Parameters
    ///
    /// - `store`: Store that receives snapshots for the task.
    /// - `id`: Identifier of the task being reported.
    /// - `attempt`: Running attempt whose progress this reporter persists.
    ///
    /// # Returns
    ///
    /// A reporter with progress versioning initialized to the empty baseline.
    #[must_use]
    pub(in crate::service) fn new(store: Arc<dyn TaskStore>, id: TaskId, attempt: u32) -> Self {
        Self {
            store,
            id,
            attempt,
            progress_version: tokio::sync::Mutex::new(0),
        }
    }
}

impl AsyncReporter for TaskProgressReporter {
    /// Persists one progress event before reporting success to the caller.
    ///
    /// The mutex keeps version allocation and persistence ordered across
    /// concurrent operations that share this reporter. A version is consumed
    /// before the store write starts and is not reused after failure or
    /// cancellation, since an asynchronous store may commit before its future
    /// is cancelled.
    ///
    /// # Parameters
    ///
    /// - `event`: Complete progress event to persist.
    ///
    /// # Returns
    ///
    /// A future that resolves after persistence completes.
    ///
    /// # Errors
    ///
    /// The future returns an error if version allocation overflows or the
    /// underlying task store rejects the snapshot.
    fn report<'a>(&'a self, event: &'a Event) -> ReportFuture<'a> {
        Box::pin(async move {
            let mut current_version = self.progress_version.lock().await;
            let progress_version = current_version
                .checked_add(1)
                .ok_or_else(|| ReporterError::message("task progress version overflowed"))?;
            let command = ProgressCommand::new(
                self.id,
                self.attempt,
                progress_version,
                event.stage().cloned(),
                event.metrics().to_vec(),
                unix_time_ms(),
            );

            // The store can commit in a spawned worker before this future is
            // cancelled. Consume the version before awaiting so a later report
            // cannot reuse a version that may already be durable.
            *current_version = progress_version;

            // Await persistence so a successful report means the status query
            // can observe this snapshot. Event phases only describe progress;
            // task lifecycle transitions remain owned by the task service.
            self.store
                .update_progress(command)
                .await
                .map(|_| ())
                .map_err(ReporterError::new)
        })
    }
}

/// Returns the current Unix time in milliseconds, saturating at `u64::MAX`.
///
/// A system clock value before the Unix epoch is treated as zero so progress
/// snapshots can still be persisted with a valid non-negative timestamp.
///
/// # Returns
///
/// The current Unix timestamp in milliseconds, clamped to `u64::MAX`.
fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    #[cfg(feature = "sqlite")]
    use std::time::SystemTime;
    #[cfg(feature = "sqlite")]
    use std::time::UNIX_EPOCH;

    use qubit_progress::AsyncProgress;
    use qubit_progress::Metric;
    use qubit_progress::MetricDelta;
    use qubit_progress::Stage;
    #[cfg(feature = "sqlite")]
    use uuid::Uuid;

    use super::TaskProgressReporter;
    #[cfg(feature = "sqlite")]
    use super::unix_time_ms;
    use crate::model::TaskState;
    use crate::model::typed::ProgressCommand;
    use crate::model::typed::ResourceRequest;
    use crate::model::typed::StartCommand;
    use crate::model::typed::StoredPayload;
    use crate::model::typed::StoredTaskRequest;
    use crate::model::typed::TaskId;
    use crate::store::MemoryTaskStore;
    use crate::store::StoreError;
    use crate::store::TaskStore;

    /// Blocks a progress write after the inner store has committed it.
    struct CommitGateStore {
        inner: Arc<dyn TaskStore>,
        armed: AtomicBool,
        before_commit: AtomicBool,
        fail_next: AtomicBool,
        committed: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }

    impl CommitGateStore {
        fn new(inner: Arc<dyn TaskStore>) -> Self {
            Self {
                inner,
                armed: AtomicBool::new(false),
                before_commit: AtomicBool::new(false),
                fail_next: AtomicBool::new(false),
                committed: tokio::sync::Notify::new(),
                release: tokio::sync::Semaphore::new(0),
            }
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::Release);
        }

        fn release(&self) {
            self.release.add_permits(1);
        }

        fn gate_before_commit(&self) {
            self.before_commit.store(true, std::sync::atomic::Ordering::Release);
        }

        fn fail_next(&self) {
            self.fail_next.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    impl TaskStore for CommitGateStore {
        fn capabilities(&self) -> crate::model::StoreCapabilities {
            self.inner.capabilities()
        }
        fn accept_encoded<'a>(
            &'a self,
            id: TaskId,
            request: crate::model::typed::StoredTaskRequest,
        ) -> crate::store::TaskFuture<'a, Result<crate::model::typed::AcceptOutcome, StoreError>> {
            self.inner.accept_encoded(id, request)
        }
        fn get_encoded_task<'a>(
            &'a self,
            id: TaskId,
        ) -> crate::store::TaskFuture<'a, Result<Option<crate::model::typed::StoredTask>, StoreError>> {
            self.inner.get_encoded_task(id)
        }
        fn get_state_checkpoint<'a>(
            &'a self,
            id: TaskId,
        ) -> crate::store::TaskFuture<'a, Result<Option<crate::model::typed::TaskStateCheckpoint>, StoreError>>
        {
            self.inner.get_state_checkpoint(id)
        }
        fn start_encoded<'a>(
            &'a self,
            cmd: crate::model::typed::StartCommand,
        ) -> crate::store::TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
            self.inner.start_encoded(cmd)
        }
        fn transition_encoded<'a>(
            &'a self,
            cmd: crate::model::typed::TransitionCommand,
        ) -> crate::store::TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
            self.inner.transition_encoded(cmd)
        }
        fn update_progress<'a>(
            &'a self,
            cmd: ProgressCommand,
        ) -> crate::store::TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
            Box::pin(async move {
                if self.fail_next.swap(false, std::sync::atomic::Ordering::AcqRel) {
                    return Err(StoreError::Failure("injected progress failure".into()));
                }
                if self.before_commit.load(std::sync::atomic::Ordering::Acquire)
                    && self.armed.swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    self.committed.notify_one();
                    self.release.acquire().await.expect("test gate remains open").forget();
                }
                let result = self.inner.update_progress(cmd).await;
                if !self.before_commit.load(std::sync::atomic::Ordering::Acquire)
                    && self.armed.swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    self.committed.notify_one();
                    self.release.acquire().await.expect("test gate remains open").forget();
                }
                result
            })
        }
        fn list_encoded<'a>(
            &'a self,
            q: crate::model::typed::TaskQuery,
        ) -> crate::store::TaskFuture<'a, Result<crate::model::typed::TaskPage, StoreError>> {
            self.inner.list_encoded(q)
        }
        fn list_ready_queued<'a>(
            &'a self,
            after: Option<crate::model::typed::TaskCursor>,
            limit: NonZeroUsize,
            now: u64,
        ) -> crate::store::TaskFuture<'a, Result<crate::model::typed::TaskPage, StoreError>> {
            self.inner.list_ready_queued(after, limit, now)
        }
        fn next_retry_deadline<'a>(
            &'a self,
            now: u64,
        ) -> crate::store::TaskFuture<'a, Result<Option<u64>, StoreError>> {
            self.inner.next_retry_deadline(now)
        }
        fn prune_terminal_before<'a>(
            &'a self,
            before: u64,
            max: NonZeroUsize,
        ) -> crate::store::TaskFuture<'a, Result<usize, StoreError>> {
            self.inner.prune_terminal_before(before, max)
        }
        fn acquire_owner<'a>(&'a self) -> crate::store::TaskFuture<'a, Result<crate::model::OwnerEpoch, StoreError>> {
            self.inner.acquire_owner()
        }
        fn release_owner<'a>(
            &'a self,
            epoch: crate::model::OwnerEpoch,
        ) -> crate::store::TaskFuture<'a, Result<(), StoreError>> {
            self.inner.release_owner(epoch)
        }
    }

    fn task_id(value: u64) -> TaskId {
        TaskId::from_id(qubit_id::Id::new(value))
    }

    /// Creates and starts a stored task for progress reporter test scenarios.
    async fn running_task() -> (Arc<MemoryTaskStore>, TaskId) {
        let store = Arc::new(MemoryTaskStore::new(8));
        let id = task_id(410);
        let mut metadata = qubit_metadata::Metadata::new();
        metadata.insert("suite", "progress-reporter");
        let accepted = store
            .accept_encoded(
                id,
                StoredTaskRequest {
                    kind_id: "test.progress".to_owned(),
                    category: Some("test".to_owned()),
                    payload: StoredPayload {
                        type_id: qubit_model_id::ModelIdBuf::parse("qubit_task.tests.ProgressPayload")
                            .expect("model ID is valid"),
                        schema_version: 1,
                        codec_id: "qubit.test.bytes".to_owned(),
                        bytes: vec![1],
                    },
                    metadata,
                    resource_limit: ResourceRequest::default(),
                    correlation_key: None,
                    idempotency_key: None,
                },
            )
            .await
            .expect("typed task is accepted");
        store
            .start_encoded(StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: 1,
            })
            .await
            .expect("typed task starts");
        (store, id)
    }

    #[tokio::test]
    async fn test_commit_gate_store_forwards_task_store_operations() {
        let (inner, id) = running_task().await;
        let inner: Arc<dyn TaskStore> = inner;
        let store = CommitGateStore::new(inner);

        assert!(!store.capabilities().restart_recovery);
        let running = store.get_encoded_task(id).await.unwrap().unwrap();
        assert_eq!(running.summary.state, TaskState::Running);
        assert_eq!(
            store.get_state_checkpoint(id).await.unwrap().unwrap().state_version,
            running.summary.state_version
        );
        assert_eq!(
            store
                .list_encoded(crate::model::typed::TaskQuery::default())
                .await
                .unwrap()
                .records
                .len(),
            1
        );

        let queued_id = task_id(411);
        let queued = store.accept_encoded(queued_id, running.request).await.unwrap();
        assert_eq!(
            store
                .list_ready_queued(None, NonZeroUsize::new(1).unwrap(), 2)
                .await
                .unwrap()
                .records[0]
                .id,
            queued_id
        );
        let started = store
            .start_encoded(StartCommand {
                id: queued_id,
                expected_state_version: queued.summary.state_version,
                started_at_ms: 3,
            })
            .await
            .unwrap();
        store
            .update_progress(ProgressCommand::new(id, 1, 1, None, Vec::new(), 4))
            .await
            .unwrap();
        store
            .transition_encoded(crate::model::typed::TransitionCommand {
                id,
                expected_state_version: running.summary.state_version,
                expected_attempt: running.summary.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(5),
                output: None,
            })
            .await
            .unwrap();
        assert_eq!(store.next_retry_deadline(5).await.unwrap(), None);
        assert_eq!(
            store
                .prune_terminal_before(6, NonZeroUsize::new(1).unwrap())
                .await
                .unwrap(),
            1
        );
        let owner = store.acquire_owner().await.unwrap();
        store.release_owner(owner).await.unwrap();
        assert_eq!(started.attempt, 1);
    }

    #[cfg(feature = "sqlite")]
    async fn sqlite_running_task() -> (Arc<dyn TaskStore>, TaskId, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "qubit-task-progress-control-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let store: Arc<dyn TaskStore> = Arc::new(crate::store::SqliteTaskStore::open(&path).expect("SQLite opens"));
        let id = task_id(unix_time_ms());
        let (source_store, _) = running_task().await;
        let source = source_store.get_encoded_task(task_id(410)).await.unwrap().unwrap();
        let accepted = store.accept_encoded(id, source.request).await.unwrap();
        let started = store
            .start_encoded(crate::model::typed::StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: 1,
            })
            .await
            .unwrap();
        assert_eq!(started.attempt, 1);
        (store, id, path)
    }

    #[cfg(feature = "sqlite")]
    fn cleanup_sqlite(path: &std::path::Path) {
        for suffix in ["", ".owner.lock", "-wal", "-shm"] {
            let mut file = path.as_os_str().to_os_string();
            file.push(suffix);
            match std::fs::remove_file(std::path::PathBuf::from(file)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("failed to remove SQLite test artifact: {error}"),
            }
        }
    }

    #[tokio::test]
    async fn test_async_reporter_persists_stage_metrics_and_terminal_snapshot() {
        let (store, id) = running_task().await;
        let reporter: Arc<dyn qubit_progress::AsyncReporter> =
            Arc::new(TaskProgressReporter::new(store.clone(), id, 1));
        let mut progress = AsyncProgress::builder_arc(reporter)
            .stage(Stage::new("download", "Downloading").position(1, 2))
            .metric(Metric::new("bytes", "Bytes").total(10))
            .start_async()
            .await
            .expect("started event is persisted");

        let initial = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        let snapshot = initial.summary.progress.expect("started event is visible");
        assert_eq!(snapshot.attempt, 1);
        assert_eq!(snapshot.progress_version, 1);
        assert_eq!(snapshot.stage.as_ref().map(|stage| stage.id.as_str()), Some("download"));
        assert_eq!(snapshot.metrics[0].id, "bytes");
        assert_eq!(initial.summary.state, TaskState::Running);

        let metric = progress.metric("bytes").expect("metric is registered");
        metric
            .apply_delta(MetricDelta::new().started(1).succeeded(1))
            .expect("metric update is valid");
        progress
            .set_stage(Stage::new("index", "Indexing").position(2, 2))
            .expect("stage is valid");
        progress.report_async().await.expect("running event is persisted");

        let running = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        let snapshot = running.summary.progress.expect("running event is visible");
        assert_eq!(snapshot.progress_version, 2);
        assert_eq!(snapshot.stage.as_ref().map(|stage| stage.id.as_str()), Some("index"));
        assert_eq!(snapshot.metrics[0].completed, 1);
        assert_eq!(snapshot.metrics[0].succeeded, 1);
        assert_eq!(running.summary.state, TaskState::Running);

        progress
            .finish_unchecked_async()
            .await
            .expect("terminal event is persisted");
        let terminal = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(terminal.summary.state, TaskState::Running);
        assert_eq!(
            terminal.summary.progress.map(|snapshot| snapshot.progress_version),
            Some(3)
        );
    }

    #[tokio::test]
    async fn test_async_reporter_surfaces_attempt_conflicts() {
        let (store, id) = running_task().await;
        let reporter = Arc::new(TaskProgressReporter::new(store.clone(), id, 2));
        let error = match AsyncProgress::builder_arc(reporter)
            .stage(Stage::new("work", "Work"))
            .metric(Metric::new("items", "Items"))
            .start_async()
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("stale attempt must be reported to the handler"),
        };
        match error {
            qubit_progress::StartError::Delivery(delivery) => {
                assert!(matches!(
                    delivery.reporter_error().source_error().downcast_ref::<StoreError>(),
                    Some(StoreError::Conflict)
                ));
            }
            other => panic!("expected reporter delivery error, got: {other:?}"),
        }

        let task = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(task.summary.progress, None);
    }

    #[tokio::test]
    async fn test_async_reporter_surfaces_snapshot_size_limits() {
        let (store, id) = running_task().await;
        let reporter = Arc::new(TaskProgressReporter::new(store.clone(), id, 1));
        let mut builder = AsyncProgress::builder_arc(reporter);
        for index in 0..=crate::model::typed::MAX_TASK_PROGRESS_METRICS {
            builder = builder.metric(Metric::new(&format!("metric-{index}"), "Metric"));
        }
        let error = match builder.start_async().await {
            Err(error) => error,
            Ok(_) => panic!("too many metrics must exceed the persisted snapshot limit"),
        };
        match error {
            qubit_progress::StartError::Delivery(delivery) => {
                assert!(matches!(
                    delivery.reporter_error().source_error().downcast_ref::<StoreError>(),
                    Some(StoreError::InvalidRequest(_))
                ));
            }
            other => panic!("expected reporter delivery error, got: {other:?}"),
        }
        let task = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(task.summary.progress, None);
    }

    #[tokio::test]
    async fn test_progress_versions_are_shared_and_serialized_across_operations() {
        let (store, id) = running_task().await;
        let reporter: Arc<dyn qubit_progress::AsyncReporter> =
            Arc::new(TaskProgressReporter::new(store.clone(), id, 1));
        let first_builder = AsyncProgress::builder_arc(Arc::clone(&reporter)).metric(Metric::new("first", "First"));
        let second_builder = AsyncProgress::builder_arc(reporter).metric(Metric::new("second", "Second"));

        let (first, second) = tokio::join!(first_builder.start_async(), second_builder.start_async());
        let mut first = first.expect("first operation starts");
        let mut second = second.expect("second operation starts");
        let after_start = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(
            after_start.summary.progress.map(|progress| progress.progress_version),
            Some(2)
        );

        let (first_report, second_report) = tokio::join!(first.report_async(), second.report_async());
        first_report.expect("first running event persists");
        second_report.expect("second running event persists");
        let after_reports = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(
            after_reports.summary.progress.map(|progress| progress.progress_version),
            Some(4)
        );
    }

    #[tokio::test]
    async fn test_async_reporter_rejects_progress_version_overflow() {
        let (store, id) = running_task().await;
        let reporter = Arc::new(TaskProgressReporter::new(store.clone(), id, 1));
        *reporter.progress_version.lock().await = u64::MAX;

        let error = match AsyncProgress::builder_arc(reporter)
            .stage(Stage::new("work", "Work"))
            .metric(Metric::new("items", "Items"))
            .start_async()
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("an overflowing progress version must be rejected"),
        };
        match error {
            qubit_progress::StartError::Delivery(delivery) => assert!(
                delivery
                    .reporter_error()
                    .source_error()
                    .to_string()
                    .contains("progress version overflowed")
            ),
            other => panic!("expected progress delivery failure, got: {other:?}"),
        }

        let task = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(task.summary.progress, None);
    }

    async fn cancel_after_commit_scenario(inner: Arc<dyn TaskStore>, id: TaskId, before_commit: bool) {
        let gate = Arc::new(CommitGateStore::new(inner.clone()));
        if before_commit {
            gate.gate_before_commit();
        }
        let reporter = Arc::new(TaskProgressReporter::new(gate.clone(), id, 1));
        let mut progress = AsyncProgress::builder_arc(reporter)
            .stage(Stage::new("initial", "Initial"))
            .metric(Metric::new("items", "Items"))
            .start_async()
            .await
            .expect("initial snapshot commits");

        gate.arm();
        progress
            .set_stage(Stage::new("cancelled", "Cancelled"))
            .expect("stage is valid");
        {
            let first = progress.report_async();
            tokio::pin!(first);
            tokio::select! {
                result = &mut first => panic!("gated report returned before cancellation: {result:?}"),
                result = tokio::time::timeout(std::time::Duration::from_secs(5), gate.committed.notified()) => {
                    result.expect("first update commits and reaches the gate");
                }
            }
        }

        let committed = inner
            .get_encoded_task(id)
            .await
            .unwrap()
            .unwrap()
            .summary
            .progress
            .unwrap();
        assert_eq!(committed.progress_version, if before_commit { 1 } else { 2 });
        assert_eq!(
            committed.stage.as_ref().map(|s| s.id.as_str()),
            Some(if before_commit { "initial" } else { "cancelled" })
        );
        gate.release();

        progress
            .set_stage(Stage::new("second", "Second"))
            .expect("stage is valid");
        progress
            .report_async()
            .await
            .expect("report after cancellation succeeds");
        progress
            .set_stage(Stage::new("third", "Third"))
            .expect("stage is valid");
        progress.report_async().await.expect("third report succeeds");
        let final_progress = inner
            .get_encoded_task(id)
            .await
            .unwrap()
            .unwrap()
            .summary
            .progress
            .unwrap();
        assert_eq!(final_progress.progress_version, 4);
        assert_eq!(final_progress.stage.as_ref().map(|s| s.id.as_str()), Some("third"));
        assert_eq!(final_progress.metrics[0].id, "items");
        assert_eq!(final_progress.metrics[0].completed, 0);
    }

    #[tokio::test]
    async fn test_cancel_after_committed_progress_advances_version_memory() {
        let (store, id) = running_task().await;
        cancel_after_commit_scenario(store, id, false).await;
    }

    #[tokio::test]
    async fn test_cancel_before_progress_commit_leaves_snapshot_unchanged() {
        let (store, id) = running_task().await;
        cancel_after_commit_scenario(store, id, true).await;
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn test_cancel_before_progress_commit_leaves_sqlite_snapshot_unchanged() {
        let (store, id, path) = sqlite_running_task().await;
        cancel_after_commit_scenario(store.clone(), id, true).await;
        drop(store);
        cleanup_sqlite(&path);
    }

    #[tokio::test]
    async fn test_reporter_advances_version_after_store_error_and_recovers() {
        let (store, id) = running_task().await;
        let gate = Arc::new(CommitGateStore::new(store.clone()));
        let reporter = Arc::new(TaskProgressReporter::new(gate.clone(), id, 1));
        let mut progress = AsyncProgress::builder_arc(reporter)
            .stage(Stage::new("initial", "Initial"))
            .metric(Metric::new("items", "Items"))
            .start_async()
            .await
            .expect("initial snapshot commits");
        gate.fail_next();
        progress
            .set_stage(Stage::new("failed", "Failed"))
            .expect("stage is valid");
        let error = progress
            .report_async()
            .await
            .expect_err("injected store failure is returned");
        let delivery = match error {
            qubit_progress::EmissionError::Delivery(error) => error,
            other => panic!("expected store delivery failure, got: {other:?}"),
        };
        assert!(
            matches!(delivery.reporter_error().source_error().downcast_ref::<StoreError>(), Some(StoreError::Failure(message)) if message == "injected progress failure")
        );

        progress
            .set_stage(Stage::new("recovered", "Recovered"))
            .expect("stage is valid");
        progress
            .report_async()
            .await
            .expect("report after ordinary failure succeeds");
        let snapshot = store
            .get_encoded_task(id)
            .await
            .unwrap()
            .unwrap()
            .summary
            .progress
            .unwrap();
        assert_eq!(snapshot.progress_version, 3);
        assert_eq!(
            snapshot.stage.as_ref().map(|stage| stage.id.as_str()),
            Some("recovered")
        );
        assert_eq!(snapshot.metrics[0].id, "items");
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn test_sqlite_reporter_advances_version_after_store_error_and_recovers() {
        let (store, id, path) = sqlite_running_task().await;
        let gate = Arc::new(CommitGateStore::new(store.clone()));
        let reporter = Arc::new(TaskProgressReporter::new(gate.clone(), id, 1));
        let mut progress = AsyncProgress::builder_arc(reporter)
            .stage(Stage::new("initial", "Initial"))
            .metric(Metric::new("items", "Items"))
            .start_async()
            .await
            .expect("initial snapshot commits");

        gate.fail_next();
        progress
            .set_stage(Stage::new("failed", "Failed"))
            .expect("stage is valid");
        let error = progress
            .report_async()
            .await
            .expect_err("injected SQLite progress failure is returned");
        let delivery = match error {
            qubit_progress::EmissionError::Delivery(error) => error,
            other => panic!("expected delivery error, got: {other:?}"),
        };
        assert!(matches!(
            delivery
                .reporter_error()
                .source_error()
                .downcast_ref::<StoreError>(),
            Some(StoreError::Failure(message)) if message == "injected progress failure"
        ));
        let after_error = store
            .get_encoded_task(id)
            .await
            .unwrap()
            .unwrap()
            .summary
            .progress
            .unwrap();
        assert_eq!(after_error.progress_version, 1);
        assert_eq!(
            after_error.stage.as_ref().map(|stage| stage.id.as_str()),
            Some("initial")
        );
        assert_eq!(after_error.metrics[0].id, "items");
        assert_eq!(after_error.metrics[0].completed, 0);

        progress
            .set_stage(Stage::new("recovered", "Recovered"))
            .expect("stage is valid");
        progress
            .report_async()
            .await
            .expect("SQLite report after ordinary failure succeeds");
        let recovered = store
            .get_encoded_task(id)
            .await
            .unwrap()
            .unwrap()
            .summary
            .progress
            .unwrap();
        assert_eq!(recovered.progress_version, 3);
        assert_eq!(
            recovered.stage.as_ref().map(|stage| stage.id.as_str()),
            Some("recovered")
        );
        assert_eq!(recovered.metrics[0].id, "items");
        assert_eq!(recovered.metrics[0].completed, 0);
        drop(store);
        cleanup_sqlite(&path);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn test_cancel_after_committed_progress_advances_version_sqlite() {
        let path = std::env::temp_dir().join(format!("qubit-task-progress-cancel-{}.sqlite", Uuid::new_v4()));
        let store: Arc<dyn TaskStore> = Arc::new(crate::store::SqliteTaskStore::open(&path).expect("SQLite opens"));
        let id = task_id(unix_time_ms());
        let (base, _) = running_task().await;
        // Reuse the valid running record setup, then use the same task ID in SQLite.
        let source = base.get_encoded_task(task_id(410)).await.unwrap().unwrap();
        let accepted = store.accept_encoded(id, source.request).await.unwrap();
        let started = store
            .start_encoded(crate::model::typed::StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: 1,
            })
            .await
            .unwrap();
        assert_eq!(started.attempt, 1);
        cancel_after_commit_scenario(store.clone(), id, false).await;
        drop(store);
        cleanup_sqlite(&path);
    }
}
