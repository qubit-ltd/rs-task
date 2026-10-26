// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Control messages for isolating a failed notification.

use tokio::sync::oneshot;

use crate::model::typed::TaskId;
use crate::service::TaskServiceError;

/// Carries a request to isolate a specific failed outbox event.
///
/// The worker validates the expected task, state version, and event ID before
/// applying the quarantine reason, then reports completion through `response`.
pub(in crate::service::task_event_publisher) struct QuarantineCommand {
    /// Task whose failed outbox event is being isolated.
    pub(in crate::service::task_event_publisher) expected_task_id: TaskId,
    /// State revision of the event that must still match the failed head.
    pub(in crate::service::task_event_publisher) expected_state_version: u64,
    /// Event identifier of the failed outbox head.
    pub(in crate::service::task_event_publisher) expected_event_id: String,
    /// Operator-provided explanation retained with the quarantined event.
    pub(in crate::service::task_event_publisher) reason: String,
    /// Returns the worker's result to the request caller.
    pub(in crate::service::task_event_publisher) response: oneshot::Sender<Result<(), TaskServiceError>>,
}
