// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Immutable lifecycle snapshot awaiting confirmed event-bus admission.
use crate::model::typed::TaskId;

/// Holds the lifecycle event data persisted until the event bus confirms admission.
///
/// Consumers can use the task ID and state version to identify the committed state,
/// while the stable event ID lets them recognize retries of the same notification.
#[derive(Debug, Clone)]
pub struct EventOutboxEntry {
    /// Identifies the task whose lifecycle state was committed with this event.
    pub task_id: TaskId,
    /// Revision of the task lifecycle state represented by this event.
    pub state_version: u64,
    /// Event identity retained across publish attempts so retries represent the same event.
    pub event_id: String,
    /// Serialized event payload captured in the transaction that committed the lifecycle state.
    pub event_json: String,
}
