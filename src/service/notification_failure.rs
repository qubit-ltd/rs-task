// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Diagnostic snapshot for a lifecycle notification blocking ordered delivery.

use crate::model::typed::TaskId;

/// Identifies the current failed head of a task notification outbox.
///
/// This is a process-local diagnostic snapshot. It is cleared after successful
/// publication, explicit quarantine, or a change to the outbox head. Quarantine
/// intentionally creates a notification gap; the archived event is not
/// automatically replayed, so consumers should compare state versions and
/// query authoritative task state when they detect a gap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationFailure {
    /// Task whose lifecycle event is blocking ordered publication.
    pub task_id: TaskId,
    /// Lifecycle revision represented by the failed event.
    pub state_version: u64,
    /// Stable identity assigned to the failed event.
    pub event_id: String,
    /// Most recently observed decoding, publication, or outbox error.
    pub error: String,
}
