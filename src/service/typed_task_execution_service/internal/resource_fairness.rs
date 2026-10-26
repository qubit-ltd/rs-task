// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::typed::TaskId;

/// Tracks the oldest resource-waiting task and successful newer starts.
#[derive(Default)]
pub(super) struct ResourceFairness {
    /// Oldest queued task currently waiting for resource capacity.
    pub(super) anchor: Option<(TaskId, u64)>,
    /// Number of newer tasks that have started while the anchor waits.
    pub(super) bypasses: usize,
}
