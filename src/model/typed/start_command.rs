// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskId;

/// Compare-and-set command that starts a queued typed task attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartCommand {
    /// Identifies the queued task whose attempt should start.
    pub id: TaskId,
    /// Lifecycle version the caller expects, used to reject a start if the task
    /// has changed since it was observed.
    pub expected_state_version: u64,
    /// Unix epoch time in milliseconds to record as the attempt start time.
    pub started_at_ms: u64,
}
