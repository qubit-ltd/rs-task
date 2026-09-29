// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync;

use super::super::ExecutionOutcome;
use super::super::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
use super::super::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
use super::super::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use super::super::QueuedTask;
use super::super::ServiceCore;
use super::super::StoreError;
use super::super::TaskRecord;
use super::super::TaskRunOutcome;
use super::super::TaskState;
use super::super::finalize_local;
use super::super::now_ms;
use super::super::pause_on_store_fault;
use super::super::retry_deadline_ms;
use super::super::transition_with_deadline;
use super::super::truncate_utf8;
use super::RetryQueueReservation;

/// Persists an execution result, retry decision, and local-handle completion.
///
/// # Parameters
///
/// * `core_ref` - Weak service reference held through attempt completion.
/// * `running` - Record snapshot committed before handler activation.
/// * `receiver` - Completion result channel returned by the engine.
/// * `_running_permit` - Slot retained until finalization exits.
pub(in crate::service::task_execution_service) async fn finish_attempt(
    core_ref: std::sync::Weak<ServiceCore>,
    running: TaskRecord,
    receiver: sync::oneshot::Receiver<ExecutionOutcome>,
    _running_permit: sync::OwnedSemaphorePermit,
) {
    let outcome = receiver
        .await
        .unwrap_or_else(|_| ExecutionOutcome::WorkerStopped("execution worker stopped".into()));
    let Some(core) = core_ref.upgrade() else {
        return;
    };
    {
        let mut cancellations = core.cancellations.lock();
        if cancellations
            .get(&running.id)
            .is_some_and(|current| current.attempt == running.attempt)
        {
            cancellations.remove(&running.id);
        }
    }
    let state = match &outcome {
        ExecutionOutcome::Panicked(message) => TaskState::Panicked {
            message: truncate_utf8(message, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
        ExecutionOutcome::WorkerStopped(_) if running.attempt < core.max_attempts => TaskState::Queued,
        ExecutionOutcome::WorkerStopped(_) => TaskState::Blocked {
            reason: "execution worker stopped after retry limit".into(),
        },
        ExecutionOutcome::Returned(Ok(TaskRunOutcome::Succeeded(_))) => TaskState::Succeeded,
        ExecutionOutcome::Returned(Ok(TaskRunOutcome::Cancelled)) => TaskState::Cancelled,
        ExecutionOutcome::Returned(Err(error)) if error.retryable && running.attempt < core.max_attempts => {
            TaskState::Queued
        }
        ExecutionOutcome::Returned(Err(error)) if error.retryable => TaskState::Blocked {
            reason: truncate_utf8(
                &format!("retry limit reached: {}", error.message),
                MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES,
            ),
        },
        ExecutionOutcome::Returned(Err(error)) => TaskState::Failed {
            category: truncate_utf8(&error.category, MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES),
            message: truncate_utf8(&error.message, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
    };
    let output = match outcome {
        ExecutionOutcome::Returned(Ok(TaskRunOutcome::Succeeded(output))) => Some(output),
        _ => None,
    };
    let mut final_state = if output
        .as_ref()
        .is_some_and(|value| value.summary.len() > MAX_TASK_OUTPUT_SUMMARY_BYTES)
    {
        TaskState::Failed {
            category: "output_too_large".into(),
            message: "task output summary exceeded the 65536-byte limit".into(),
        }
    } else {
        state
    };
    let output = output.filter(|value| value.summary.len() <= MAX_TASK_OUTPUT_SUMMARY_BYTES);
    let mut retry_deadline = if matches!(final_state, TaskState::Queued) {
        Some(retry_deadline_ms(now_ms(), core.retry_policy, running.attempt))
    } else {
        None
    };
    let mut retry_reservation = None;
    if matches!(final_state, TaskState::Queued) {
        retry_reservation = RetryQueueReservation::try_new(&core);
        if retry_reservation.is_none() {
            retry_deadline = None;
            final_state = TaskState::Blocked {
                reason: "retry queue is full; call retry_blocked when capacity is available".into(),
            };
        }
    }
    loop {
        let latest = match core.store.get_summary(running.id).await {
            Ok(Some(record)) if matches!(record.state, TaskState::Running) && record.attempt == running.attempt => {
                record
            }
            Ok(None) => break,
            Ok(_) => break,
            Err(error) => {
                pause_on_store_fault(&core, error);
                break;
            }
        };
        match transition_with_deadline(
            &core,
            &latest,
            final_state.clone(),
            retry_deadline,
            output.clone(),
            latest.assigned_resources.clone(),
            latest.cancel_requested,
        )
        .await
        {
            Ok(updated) => {
                if matches!(final_state, TaskState::Queued) {
                    core.queue.lock().push(QueuedTask {
                        id: updated.id,
                        resources: updated.request.resources.clone(),
                        retry_not_before_ms: updated.retry_not_before_ms,
                        bypasses: 0,
                    });
                    if let Some(reservation) = retry_reservation.take() {
                        reservation.commit_to_queue();
                    }
                }
                core.changed.notify_waiters();
                if !matches!(updated.state, TaskState::Queued | TaskState::Running) {
                    finalize_local(&core, updated.id, Ok(updated.state));
                }
                return;
            }
            Err(StoreError::Conflict) => continue,
            Err(error) => {
                pause_on_store_fault(&core, error);
                break;
            }
        }
    }
}
