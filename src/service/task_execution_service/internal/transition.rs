// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::ServiceCore;
#[cfg(feature = "event-bus")]
use crate::event::TaskEvent;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::store::StoreError;

/// Enqueues a best-effort lifecycle event when event-bus support is enabled.
///
/// # Parameters
///
/// * `core` - Service state containing the optional event publisher.
/// * `record` - Payload-free lifecycle snapshot to publish.
pub(in crate::service::task_execution_service) fn publish_record(core: &ServiceCore, record: &TaskSummary) {
    #[cfg(feature = "event-bus")]
    if let Some(bus) = &core.event_bus {
        bus.enqueue(TaskEvent::from(record));
    }
    #[cfg(not(feature = "event-bus"))]
    let _ = (core, record);
}

/// Applies a version-checked store transition and publishes its new revision
/// before allowing shutdown to close the event publisher.
///
/// # Parameters
///
/// * `core` - Service state owning the store and event lock.
/// * `record` - Snapshot whose version and attempt guard the update.
/// * `state` - New lifecycle state.
/// * `output` - Optional bounded task result summary.
/// * `assigned_resources` - Resources assigned to the new state.
/// * `cancel_requested` - Whether cooperative cancellation is pending.
///
/// # Returns
///
/// The committed task summary.
///
/// # Errors
///
/// Returns the store error if the expected revision cannot be committed.
pub(in crate::service::task_execution_service) async fn transition(
    core: &ServiceCore,
    record: &TaskSummary,
    state: TaskState,
    output: Option<TaskOutput>,
    assigned_resources: Vec<String>,
    cancel_requested: bool,
) -> Result<TaskSummary, StoreError> {
    transition_with_deadline(core, record, state, None, output, assigned_resources, cancel_requested).await
}

/// Applies a transition with an optional retry deadline and publishes it.
///
/// # Parameters
///
/// * `core` - Service state owning the store and event lock.
/// * `record` - Snapshot whose version and attempt guard the update.
/// * `state` - New lifecycle state.
/// * `retry_not_before_ms` - Optional earliest retry timestamp.
/// * `output` - Optional bounded task result summary.
/// * `assigned_resources` - Resources assigned to the new state.
/// * `cancel_requested` - Whether cooperative cancellation is pending.
///
/// # Returns
///
/// The committed task summary.
///
/// # Errors
///
/// Returns the store error if the expected revision cannot be committed.
pub(in crate::service::task_execution_service) async fn transition_with_deadline(
    core: &ServiceCore,
    record: &TaskSummary,
    state: TaskState,
    retry_not_before_ms: Option<u64>,
    output: Option<TaskOutput>,
    assigned_resources: Vec<String>,
    cancel_requested: bool,
) -> Result<TaskSummary, StoreError> {
    let _guard = core.transition_event_lock.read().await;
    let updated = core
        .store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state,
            retry_not_before_ms,
            output,
            assigned_resources,
            cancel_requested,
        })
        .await?;
    publish_record(core, &updated);
    core.wait_registry.notify(updated.id);
    Ok(updated)
}
