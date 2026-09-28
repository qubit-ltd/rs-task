// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use super::TaskId;
use super::TaskSummary;

/// Bounded payload-free projection of unfinished work used during recovery.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RecoveryPage {
    /// Unfinished task summaries in ascending task ID order.
    pub tasks: Vec<TaskSummary>,
    /// Last returned ID when another page is available.
    pub next: Option<TaskId>,
}
