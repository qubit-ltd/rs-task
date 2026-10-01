// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskSummary;

/// Result of accepting a new encoded request or finding an idempotent match.
#[derive(Debug, Clone, PartialEq)]
pub struct AcceptOutcome {
    /// Summary of the retained task.
    pub summary: TaskSummary,
    /// Whether this call created a new task record.
    pub created: bool,
}
