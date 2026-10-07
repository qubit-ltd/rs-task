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

use crate::model::next::ProgressCommand;
use crate::model::next::TaskId;
use crate::store::TaskStore;

/// Asynchronous reporter that writes progress snapshots to a task store.
///
/// Each reporter belongs to one running task attempt. Progress versions are
/// allocated serially across all progress operations created by one task
/// context, starting above the store's empty baseline version of zero.
pub(super) struct TaskProgressReporter {
    store: Arc<dyn TaskStore>,
    id: TaskId,
    attempt: u32,
    progress_version: tokio::sync::Mutex<u64>,
}

impl TaskProgressReporter {
    /// Creates a reporter bound to one task attempt.
    pub(super) fn new(store: Arc<dyn TaskStore>, id: TaskId, attempt: u32) -> Self {
        Self {
            store,
            id,
            attempt,
            progress_version: tokio::sync::Mutex::new(0),
        }
    }
}

impl AsyncReporter for TaskProgressReporter {
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

            // Await persistence so a successful report means the status query
            // can observe this snapshot. Event phases only describe progress;
            // task lifecycle transitions remain owned by the task service.
            self.store
                .update_progress(command)
                .await
                .map(|_| {
                    *current_version = progress_version;
                })
                .map_err(ReporterError::new)
        })
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qubit_progress::AsyncProgress;
    use qubit_progress::Metric;
    use qubit_progress::MetricDelta;
    use qubit_progress::Stage;

    use super::TaskProgressReporter;
    use crate::model::TaskState;
    use crate::model::next::ResourceRequest;
    use crate::model::next::StartCommand;
    use crate::model::next::StoredPayload;
    use crate::model::next::StoredTaskRequest;
    use crate::model::next::TaskId;
    use crate::store::MemoryTaskStore;
    use crate::store::StoreError;
    use crate::store::TaskStore;

    fn task_id(value: u64) -> TaskId {
        TaskId::from_id(qubit_id::Id::new(value))
    }

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
                        type_id: qubit_model_id::ModelIdBuf::parse(
                            "qubit_task.tests.ProgressPayload",
                        )
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
    async fn async_reporter_persists_stage_metrics_and_terminal_snapshot() {
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
        assert_eq!(
            snapshot.stage.as_ref().map(|stage| stage.id.as_str()),
            Some("download")
        );
        assert_eq!(snapshot.metrics[0].id, "bytes");
        assert_eq!(initial.summary.state, TaskState::Running);

        let metric = progress.metric("bytes").expect("metric is registered");
        metric
            .apply_delta(MetricDelta::new().started(1).succeeded(1))
            .expect("metric update is valid");
        progress
            .set_stage(Stage::new("index", "Indexing").position(2, 2))
            .expect("stage is valid");
        progress
            .report_async()
            .await
            .expect("running event is persisted");

        let running = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        let snapshot = running.summary.progress.expect("running event is visible");
        assert_eq!(snapshot.progress_version, 2);
        assert_eq!(
            snapshot.stage.as_ref().map(|stage| stage.id.as_str()),
            Some("index")
        );
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
            terminal
                .summary
                .progress
                .map(|snapshot| snapshot.progress_version),
            Some(3)
        );
    }

    #[tokio::test]
    async fn async_reporter_surfaces_attempt_conflicts() {
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
                    delivery
                        .reporter_error()
                        .source_error()
                        .downcast_ref::<StoreError>(),
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
    async fn async_reporter_surfaces_snapshot_size_limits() {
        let (store, id) = running_task().await;
        let reporter = Arc::new(TaskProgressReporter::new(store.clone(), id, 1));
        let mut builder = AsyncProgress::builder_arc(reporter);
        for index in 0..=crate::model::next::MAX_TASK_PROGRESS_METRICS {
            builder = builder.metric(Metric::new(&format!("metric-{index}"), "Metric"));
        }
        let error = match builder.start_async().await {
            Err(error) => error,
            Ok(_) => panic!("too many metrics must exceed the persisted snapshot limit"),
        };
        match error {
            qubit_progress::StartError::Delivery(delivery) => {
                assert!(matches!(
                    delivery
                        .reporter_error()
                        .source_error()
                        .downcast_ref::<StoreError>(),
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
    async fn progress_versions_are_shared_and_serialized_across_operations() {
        let (store, id) = running_task().await;
        let reporter: Arc<dyn qubit_progress::AsyncReporter> =
            Arc::new(TaskProgressReporter::new(store.clone(), id, 1));
        let first_builder =
            AsyncProgress::builder_arc(Arc::clone(&reporter)).metric(Metric::new("first", "First"));
        let second_builder =
            AsyncProgress::builder_arc(reporter).metric(Metric::new("second", "Second"));

        let (first, second) =
            tokio::join!(first_builder.start_async(), second_builder.start_async());
        let mut first = first.expect("first operation starts");
        let mut second = second.expect("second operation starts");
        let after_start = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(
            after_start
                .summary
                .progress
                .map(|progress| progress.progress_version),
            Some(2)
        );

        let (first_report, second_report) =
            tokio::join!(first.report_async(), second.report_async());
        first_report.expect("first running event persists");
        second_report.expect("second running event persists");
        let after_reports = store
            .get_encoded_task(id)
            .await
            .expect("task lookup succeeds")
            .expect("task is retained");
        assert_eq!(
            after_reports
                .summary
                .progress
                .map(|progress| progress.progress_version),
            Some(4)
        );
    }
}
