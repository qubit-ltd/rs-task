// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::ServiceCore;
use super::begin_shutdown_core;
use crate::model::TaskId;
use crate::model::TaskState;
use crate::service::local_task_result_error::LocalTaskResultError;
use crate::store::StoreError;

/// Sends final state or infrastructure failure to a process-local task handle.
///
/// # Parameters
///
/// * `core` - Service state owning local finalization senders.
/// * `id` - Task whose handle should receive the result.
/// * `result` - Final lifecycle state or infrastructure failure.
pub(in crate::service::task_execution_service) fn finalize_local(
    core: &ServiceCore,
    id: TaskId,
    result: Result<TaskState, LocalTaskResultError>,
) {
    let sender = core.local_finalizations.lock().remove(&id);
    if let Some(sender) = sender {
        let _ = sender.send(result);
    }
}

/// Latches a worker-side store error and suspends service admission.
///
/// # Parameters
///
/// * `core` - Service state to suspend.
/// * `error` - Store failure observed by a worker.
pub(in crate::service::task_execution_service) fn pause_on_store_fault(core: &Arc<ServiceCore>, error: StoreError) {
    record_store_fault(core, error.to_string());
}

/// Records the first storage failure and closes admission for all waiters.
///
/// # Parameters
///
/// * `core` - Service state to suspend.
/// * `diagnostic` - Error message retained for later callers.
pub(in crate::service::task_execution_service) fn record_store_fault(core: &Arc<ServiceCore>, diagnostic: String) {
    let (diagnostic, finalizations) = {
        let mut fault = core.store_fault.lock();
        let diagnostic = fault.get_or_insert(diagnostic).clone();
        let finalizations = core
            .local_finalizations
            .lock()
            .drain()
            .map(|(_, sender)| sender)
            .collect::<Vec<_>>();
        (diagnostic, finalizations)
    };
    core.local_handlers.lock().clear();
    for sender in finalizations {
        let _ = sender.send(Err(LocalTaskResultError::StoreUnavailable(diagnostic.clone())));
    }
    begin_shutdown_core(Arc::clone(core));
    core.changed.notify_waiters();
    core.wait_registry.notify_all();
}

/// Records the first scheduler panic and starts the shared shutdown
/// coordinator.
///
/// # Parameters
///
/// * `core` - Service state whose scheduler failure is retained.
/// * `diagnostic` - Panic or scheduler failure message.
pub(in crate::service::task_execution_service) fn record_scheduler_fault(core: &Arc<ServiceCore>, diagnostic: String) {
    let (diagnostic, finalizations) = {
        let mut fault = core.scheduler_fault.lock();
        let diagnostic = fault.get_or_insert(diagnostic).clone();
        let finalizations = core
            .local_finalizations
            .lock()
            .drain()
            .map(|(_, sender)| sender)
            .collect::<Vec<_>>();
        (diagnostic, finalizations)
    };
    core.local_handlers.lock().clear();
    for sender in finalizations {
        let _ = sender.send(Err(LocalTaskResultError::Infrastructure(diagnostic.clone())));
    }
    core.changed.notify_waiters();
    core.wait_registry.notify_all();
    begin_shutdown_core(Arc::clone(core));
}

/// Extracts a useful diagnostic from a caught background worker panic payload.
///
/// # Parameters
///
/// * `payload` - Panic payload returned by `catch_unwind`.
///
/// # Returns
///
/// A string message or a stable fallback for non-string payloads.
pub(in crate::service::task_execution_service) fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "background worker panicked with a non-string payload".into()
    }
}
