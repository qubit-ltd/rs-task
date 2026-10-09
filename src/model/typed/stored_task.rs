// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::StoredTaskRequest;
use super::TaskSummary;

/// Encoded task request and payload-free lifecycle view loaded from a store.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTask {
    /// Encoded request required to recover handler execution.
    pub request: StoredTaskRequest,
    /// Current queryable lifecycle summary.
    pub summary: TaskSummary,
}
