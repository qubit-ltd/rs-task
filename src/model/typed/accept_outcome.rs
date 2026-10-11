// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskSummary;

/// Describes the task retained after accepting an encoded request.
///
/// An outcome with [`created`](Self::created) set to `false` represents an
/// idempotent match to a task that was already retained. In both cases,
/// [`summary`](Self::summary) identifies the retained task.
#[derive(Debug, Clone, PartialEq)]
pub struct AcceptOutcome {
    /// Payload-free view of the task record retained by the acceptance
    /// operation.
    pub summary: TaskSummary,
    /// Whether this call inserted a new task record; `false` means an existing
    /// task matched the request idempotently.
    pub created: bool,
}
