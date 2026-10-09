// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Immutable lifecycle snapshot awaiting confirmed event-bus admission.
use crate::model::typed::TaskId;

/// A durable notification. Consumers deduplicate by task ID and state version.
#[derive(Debug, Clone)]
pub struct EventOutboxEntry {
    /// Task whose state was committed.
    pub task_id: TaskId,
    /// Committed lifecycle revision.
    pub state_version: u64,
    /// Stable event identity reused by every publish attempt.
    pub event_id: String,
    /// JSON snapshot captured inside the lifecycle transaction.
    pub event_json: String,
}
