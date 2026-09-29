// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures::future::select;
use tokio::pin;
use tokio::time;

use super::super::EngineError;
use super::super::QueueSnapshot;
use super::super::RunningCancellation;
use super::super::ServiceCore;
use super::super::StoreError;
use super::super::TaskContext;
use super::super::TaskServiceError;
use super::super::TaskState;
use super::super::finalize_local;
use super::super::mark_blocked;
use super::super::now_ms;
use super::super::pause_on_store_fault;
use super::super::record_scheduler_fault;
use super::super::record_store_fault;
use super::super::release_core_queue_slot;
use super::super::task_stats;
use super::super::transition;
use super::QueueWindowGuard;
use super::finish_attempt;
use crate::scheduling::SchedulingPlan;

/// Selects queued work, reserves resources, and starts eligible task attempts.
///
/// # Parameters
///
/// * `core_ref` - Weak shared state upgraded for each scheduling pass.
pub(in crate::service::task_execution_service) async fn scheduler_loop(core_ref: std::sync::Weak<ServiceCore>) {
    let mut sweep_remaining = 0_usize;
    let mut skipped_ids = Vec::new();
    loop {
        let Some(core) = core_ref.upgrade() else {
            return;
        };
        let notified = core.changed.notified();
        pin!(notified);
        notified.as_mut().enable();
        if core.store_fault.lock().is_some() {
            return;
        }
        if sweep_remaining == 0 {
            if !skipped_ids.is_empty() {
                core.queue.lock().promote_ids_front(&skipped_ids);
                skipped_ids.clear();
            }
            sweep_remaining = core.queue.lock().len();
        }
        let now = now_ms();
        let window_tasks = core.queue.lock().take_window(core.scan_budget, now);
        let mut window = QueueWindowGuard::new(Arc::clone(&core), window_tasks);
        let queue = window.tasks_mut();
        let window_len = queue.len();
        if queue.is_empty() {
            if core.admission.is_closing() && core.admission.is_idle() && core.queue.lock().is_empty() {
                match task_stats(&core).await {
                    Ok(stats) if stats.queued == 0 && stats.running == 0 => return,
                    Err(error) => {
                        if let TaskServiceError::Store(store_error) = error {
                            record_store_fault(&core, store_error.to_string());
                        }
                        return;
                    }
                    _ => {}
                }
            }
            let deadline = core.queue.lock().next_deadline();
            let wait = deadline.map(|value| std::time::Duration::from_millis(value.saturating_sub(now_ms())));
            if let Some(wait) = wait {
                let _ = select(Box::pin(notified), Box::pin(time::sleep(wait))).await;
            } else {
                notified.await;
            }
            continue;
        }
        let snapshot = QueueSnapshot {
            tasks: queue.to_vec(),
            scan_budget: core.scan_budget,
        };
        let scheduling_plan = core.policy.order(&snapshot, &core.engine.capacity());
        if let Err(error) = validate_scheduling_plan(&snapshot, &scheduling_plan) {
            record_scheduler_fault(&core, format!("scheduling policy returned an invalid plan: {error}"));
            return;
        }
        let barrier = scheduling_plan.barrier;
        let order = scheduling_plan.order;
        let original_positions = queue
            .iter()
            .enumerate()
            .map(|(position, task)| (task.id, position))
            .collect::<HashMap<_, _>>();
        let mut activated_positions = Vec::new();
        let mut started = false;
        for id in order {
            if core.store_fault.lock().is_some() {
                return;
            }
            let Some(index) = queue.iter().position(|task| task.id == id) else {
                continue;
            };
            let mut task = queue.remove(index);
            let record = match core.store.get_summary(id).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    release_core_queue_slot(&core);
                    core.local_handlers.lock().remove(&id);
                    continue;
                }
                Err(error) => {
                    queue.push(task);
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            if core.store_fault.lock().is_some() {
                queue.push(task);
                return;
            }
            if matches!(record.state, TaskState::Queued)
                && record.retry_not_before_ms.is_some_and(|deadline| deadline > now_ms())
            {
                let deadline = record.retry_not_before_ms.expect("deadline checked above");
                task.retry_not_before_ms = Some(deadline);
                queue.push(task);
                if barrier == Some(id) {
                    break;
                }
                continue;
            }
            if !matches!(record.state, TaskState::Queued) {
                release_core_queue_slot(&core);
                core.local_handlers.lock().remove(&id);
                if record.state.is_terminal() || matches!(record.state, TaskState::Blocked { .. }) {
                    finalize_local(&core, id, Ok(record.state));
                }
                continue;
            }
            let handler = core.local_handlers.lock().get(&id).cloned().or_else(|| {
                core.handlers
                    .resolve(&record.request.task_type, &record.request.handler_version)
            });
            let Some(handler) = handler else {
                match mark_blocked(
                    &core,
                    &record,
                    format!(
                        "missing handler {}@{}",
                        record.request.task_type, record.request.handler_version
                    ),
                )
                .await
                {
                    Ok(()) | Err(StoreError::NotFound) => release_core_queue_slot(&core),
                    Err(StoreError::Conflict) => queue.push(task),
                    Err(error) => {
                        queue.push(task);
                        pause_on_store_fault(&core, error);
                        return;
                    }
                }
                continue;
            };
            let Ok(running_permit) = core.running_slots.clone().try_acquire_owned() else {
                queue.push(task);
                break;
            };
            let prepared = match core.engine.try_prepare(id, record.request.resources.clone()) {
                Ok(value) => value,
                Err(EngineError::TemporarilyUnavailable) => {
                    queue.push(task);
                    if barrier == Some(id) {
                        break;
                    }
                    continue;
                }
                Err(EngineError::Unsatisfiable) => {
                    match mark_blocked(&core, &record, "resource request is unsatisfiable".into()).await {
                        Ok(()) | Err(StoreError::NotFound) => release_core_queue_slot(&core),
                        Err(StoreError::Conflict) => queue.push(task),
                        Err(error) => {
                            queue.push(task);
                            pause_on_store_fault(&core, error);
                            return;
                        }
                    }
                    continue;
                }
                Err(EngineError::Closed) => {
                    queue.push(task);
                    record_scheduler_fault(&core, "execution engine closed during prepare".into());
                    return;
                }
                Err(EngineError::ReservationTokenExhausted) => {
                    queue.push(task);
                    record_scheduler_fault(&core, "execution engine exhausted reservation identifiers".into());
                    return;
                }
            };
            if core.store_fault.lock().is_some() {
                queue.push(task);
                return;
            }
            let mut current = match core.store.get(id).await {
                Ok(Some(current))
                    if current.state_version == record.state_version
                        && current.attempt == record.attempt
                        && matches!(current.state, TaskState::Queued) =>
                {
                    current
                }
                Ok(Some(_)) | Ok(None) => {
                    release_core_queue_slot(&core);
                    continue;
                }
                Err(error) => {
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            let assigned = prepared.assigned_resources().to_vec();
            let running = match transition(&core, &record, TaskState::Running, None, assigned.clone(), false).await {
                Ok(value) => value,
                Err(StoreError::Conflict | StoreError::NotFound) => {
                    release_core_queue_slot(&core);
                    continue;
                }
                Err(error) => {
                    queue.push(task);
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            // Keep payload ownership local to activation; the finalizer only
            // needs the lifecycle record and must not retain a payload copy.
            let payload = std::mem::take(&mut current.request.payload);
            let mut running_record = current.clone();
            running_record.state = running.state.clone();
            running_record.state_version = running.state_version;
            running_record.attempt = running.attempt;
            running_record.started_at_ms = running.started_at_ms;
            running_record.assigned_resources = running.assigned_resources.clone();
            release_core_queue_slot(&core);
            if core.store_fault.lock().is_some() {
                return;
            }
            match core.store.get_summary(id).await {
                Ok(Some(latest))
                    if latest.state_version == running.state_version
                        && latest.attempt == running.attempt
                        && matches!(latest.state, TaskState::Running)
                        && !latest.cancel_requested => {}
                Ok(Some(latest)) if latest.attempt == running.attempt && matches!(latest.state, TaskState::Running) => {
                    match transition(&core, &latest, TaskState::Cancelled, None, Vec::new(), false).await {
                        Ok(cancelled) => finalize_local(&core, id, Ok(cancelled.state)),
                        Err(error) => pause_on_store_fault(&core, error),
                    }
                    continue;
                }
                Ok(Some(_)) | Ok(None) => continue,
                Err(error) => {
                    pause_on_store_fault(&core, error);
                    return;
                }
            }
            core.local_handlers.lock().remove(&id);
            let cancelled = Arc::new(AtomicBool::new(false));
            let context = TaskContext::new(id, running.attempt, assigned, cancelled);
            match core.engine.activate(prepared, handler, payload, context).await {
                Ok(handle) => {
                    let cancellation_signal = Arc::clone(&handle.cancelled);
                    core.cancellations.lock().insert(
                        id,
                        RunningCancellation {
                            attempt: running.attempt,
                            signal: Arc::clone(&cancellation_signal),
                        },
                    );
                    core.attempts_in_flight.fetch_add(1, Ordering::AcqRel);
                    let weak = Arc::downgrade(&core);
                    core.runtime_handle
                        .spawn(finish_attempt(weak, running_record, handle.receiver, running_permit));
                    activated_positions.push(original_positions[&id]);
                    started = true;
                    match core.store.get_summary(id).await {
                        Ok(Some(record))
                            if record.attempt == running.attempt
                                && matches!(record.state, TaskState::Running)
                                && record.cancel_requested =>
                        {
                            cancellation_signal.store(true, Ordering::Release);
                        }
                        Ok(Some(_)) | Ok(None) => {}
                        Err(error) => {
                            core.cancellations.lock().remove(&id);
                            pause_on_store_fault(&core, error);
                            return;
                        }
                    }
                }
                Err(error) => {
                    let reason = format!("engine activation failed: {error}");
                    let mut latest = running_record.summary();
                    loop {
                        match mark_blocked(&core, &latest, reason.clone()).await {
                            Ok(()) => break,
                            Err(StoreError::Conflict) => match core.store.get_summary(id).await {
                                Ok(Some(record))
                                    if record.attempt == latest.attempt
                                        && matches!(record.state, TaskState::Running) =>
                                {
                                    latest = record;
                                }
                                Ok(_) => break,
                                Err(error) => {
                                    pause_on_store_fault(&core, error);
                                    return;
                                }
                            },
                            Err(error) => {
                                pause_on_store_fault(&core, error);
                                return;
                            }
                        }
                    }
                }
            }
        }
        for item in queue.iter_mut() {
            let Some(position) = original_positions.get(&item.id) else {
                continue;
            };
            if activated_positions
                .iter()
                .any(|started_position| started_position > position)
            {
                item.bypasses = item.bypasses.saturating_add(1);
            }
        }
        let waiting_at_barrier = barrier.is_some_and(|id| queue.iter().any(|task| task.id == id));
        if waiting_at_barrier {
            window.restore();
            sweep_remaining = 0;
            skipped_ids.clear();
        } else if started {
            window.restore();
            if !skipped_ids.is_empty() {
                core.queue.lock().record_bypass_and_promote_front(&skipped_ids);
                skipped_ids.clear();
            }
            sweep_remaining = 0;
        } else {
            skipped_ids.extend(
                queue
                    .iter()
                    .filter(|task| task.retry_not_before_ms.is_none())
                    .map(|task| task.id),
            );
            window.restore_back();
            sweep_remaining = sweep_remaining.saturating_sub(window_len);
            if sweep_remaining == 0 {
                core.queue.lock().promote_ids_front(&skipped_ids);
                skipped_ids.clear();
            }
        }
        if !started {
            if !waiting_at_barrier && sweep_remaining > 0 {
                continue;
            }
            let wait = core
                .queue
                .lock()
                .next_deadline()
                .map(|deadline| std::time::Duration::from_millis(deadline.saturating_sub(now_ms())));
            let sleep_for = wait.unwrap_or(std::time::Duration::from_millis(40));
            let notified = Box::pin(notified);
            let timer = Box::pin(time::sleep(sleep_for));
            let _ = select(notified, timer).await;
        } else {
            core.changed.notify_waiters();
        }
    }
}

/// Validates identifiers and the barrier returned by a scheduling extension.
///
/// # Parameters
///
/// * `snapshot` - Queue view passed to the scheduling policy.
/// * `plan` - Candidate order and optional protected task returned by it.
///
/// # Returns
///
/// Success when every candidate is unique and belongs to the snapshot and the
/// optional barrier appears in the candidate order.
///
/// # Errors
///
/// Returns a diagnostic naming an unknown or duplicate candidate, or a
/// barrier that is absent from the candidate order.
fn validate_scheduling_plan(snapshot: &QueueSnapshot, plan: &SchedulingPlan) -> Result<(), String> {
    let available = snapshot.tasks.iter().map(|task| task.id).collect::<HashSet<_>>();
    let mut seen = HashSet::with_capacity(plan.order.len());
    for id in &plan.order {
        if !available.contains(id) {
            return Err(format!("candidate {id} is not present in the queue snapshot"));
        }
        if !seen.insert(*id) {
            return Err(format!("candidate {id} appears more than once"));
        }
    }
    if let Some(barrier) = plan.barrier
        && !seen.contains(&barrier)
    {
        return Err(format!("barrier {barrier} is not present in the candidate order"));
    }
    Ok(())
}
