// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync;

use crate::handler::TaskHandlerRegistry;
use crate::model::TaskCursor;
use crate::model::TaskState;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::scheduling::QueuedTask;
use crate::service::task_execution_service_builder::TaskExecutionService;
use crate::service::task_execution_service_builder::TaskServiceBuildError;
use crate::store::LegacyTaskStore;

/// Maximum number of rows allowed in one recovery scan page.
const RECOVERY_PAGE_LIMIT: usize = 256;

/// Validates the full recovery page before any lifecycle writes.
///
/// # Parameters
///
/// * `tasks` - Rows returned in the current page.
/// * `previous` - Cursor used to request this page.
/// * `next` - Cursor advertised for the next page.
///
/// # Returns
///
/// Success when recoverable rows strictly follow the exclusive cursor and
/// any next cursor equals the final row key.
///
/// # Errors
///
/// Returns an invalid-recovery-page error for oversized pages, invalid states,
/// duplicate/out-of-order rows, or inconsistent/non-advancing cursors.
fn validate_recovery_page(
    tasks: &[TaskSummary],
    previous: Option<TaskCursor>,
    next: Option<TaskCursor>,
) -> Result<(), TaskServiceBuildError> {
    if tasks.len() > RECOVERY_PAGE_LIMIT {
        return Err(TaskServiceBuildError::InvalidRecoveryPage(format!(
            "page contains {} records; maximum is {RECOVERY_PAGE_LIMIT}",
            tasks.len()
        )));
    }
    if next.is_some() && tasks.is_empty() {
        return Err(TaskServiceBuildError::InvalidRecoveryPage(
            "empty page returned a next cursor".into(),
        ));
    }
    let mut last = previous;
    for row in tasks {
        if !matches!(row.state, TaskState::Queued | TaskState::Running) {
            return Err(TaskServiceBuildError::InvalidRecoveryPage(
                "recovery page contains a non-recoverable state".into(),
            ));
        }
        let key = TaskCursor::from(row);
        if last.is_some_and(|prior| key <= prior) {
            return Err(TaskServiceBuildError::InvalidRecoveryPage(
                "recovery rows are not strictly ordered after the cursor".into(),
            ));
        }
        last = Some(key);
    }
    if next.is_some() && next != last {
        return Err(TaskServiceBuildError::InvalidRecoveryPage(
            "recovery next cursor must equal the last row key".into(),
        ));
    }
    Ok(())
}

/// Resets interrupted attempts and queues recoverable tasks with available
/// handlers, retaining only one store page at a time.
///
/// # Parameters
///
/// * `store` - Recoverable store to scan and update.
/// * `handlers` - Registered handlers available after restart.
/// * `max_attempts` - Attempt limit used to block exhausted tasks.
/// * `limit` - Maximum number of unfinished records to restore.
/// * `sender` - Build caller used to detect cancellation between pages.
///
/// # Returns
///
/// The bounded queue of records that are ready for execution.
///
/// # Errors
///
/// Returns a build error for failed scans, invalid pages, capacity overflow,
/// missing-handler transitions, or interrupted construction.
pub(in crate::service::task_execution_service_builder) async fn restore_tasks_paged(
    store: &Arc<dyn LegacyTaskStore>,
    handlers: &TaskHandlerRegistry,
    max_attempts: u32,
    limit: usize,
    sender: &sync::oneshot::Sender<Result<TaskExecutionService, TaskServiceBuildError>>,
) -> Result<VecDeque<QueuedTask>, TaskServiceBuildError> {
    let mut queue = VecDeque::new();
    let mut cursor = None;
    let mut count = 0_usize;
    loop {
        if sender.is_closed() {
            return Err(TaskServiceBuildError::InvalidConfiguration(
                "service construction caller was cancelled".into(),
            ));
        }
        let page = store.scan_unfinished(cursor).await?;
        validate_recovery_page(&page.tasks, cursor, page.next)?;
        count = count.checked_add(page.tasks.len()).ok_or_else(|| {
            TaskServiceBuildError::InvalidConfiguration("unfinished task count overflows usize".into())
        })?;
        if count > limit {
            return Err(TaskServiceBuildError::RecoveryCapacityExceeded { limit });
        }
        for mut record in page.tasks {
            if matches!(record.state, TaskState::Queued | TaskState::Running) && record.attempt >= max_attempts {
                store
                    .transition(TransitionCommand {
                        id: record.id,
                        expected_version: record.state_version,
                        expected_attempt: record.attempt,
                        state: TaskState::Blocked {
                            reason: format!(
                                "retry limit reached during recovery: {}/{} attempts used",
                                record.attempt, max_attempts
                            ),
                        },
                        retry_not_before_ms: None,
                        output: None,
                        assigned_resources: Vec::new(),
                        cancel_requested: record.cancel_requested,
                    })
                    .await?;
                continue;
            }
            if matches!(record.state, TaskState::Running) {
                record = store
                    .transition(TransitionCommand {
                        id: record.id,
                        expected_version: record.state_version,
                        expected_attempt: record.attempt,
                        state: TaskState::Queued,
                        retry_not_before_ms: None,
                        output: None,
                        assigned_resources: Vec::new(),
                        cancel_requested: false,
                    })
                    .await?;
            }
            if matches!(record.state, TaskState::Queued) {
                if handlers
                    .resolve(&record.request.task_type, &record.request.handler_version)
                    .is_none()
                {
                    store
                        .transition(TransitionCommand {
                            id: record.id,
                            expected_version: record.state_version,
                            expected_attempt: record.attempt,
                            state: TaskState::Blocked {
                                reason: format!(
                                    "missing handler {}@{} during recovery",
                                    record.request.task_type, record.request.handler_version
                                ),
                            },
                            retry_not_before_ms: None,
                            output: None,
                            assigned_resources: Vec::new(),
                            cancel_requested: false,
                        })
                        .await?;
                } else {
                    queue.push_back(QueuedTask {
                        id: record.id,
                        resources: record.request.resources.clone(),
                        retry_not_before_ms: record.retry_not_before_ms,
                        bypasses: 0,
                    });
                }
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    Ok(queue)
}
