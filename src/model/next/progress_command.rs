// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_progress::MetricSnapshot;
use qubit_progress::Stage;

use super::TaskId;

/// Compare-and-set command for publishing one task progress snapshot.
///
/// `progress_version` is independent from the task lifecycle state revision.
/// A store accepts this command only for the matching running attempt and when
/// the supplied progress version is newer than the currently stored one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressCommand {
    /// Stable task identifier.
    pub id: TaskId,
    /// Attempt whose progress is being reported.
    pub expected_attempt: u32,
    /// Monotonically increasing version within this attempt.
    pub progress_version: u64,
    /// Current optional execution stage.
    pub stage: Option<Stage>,
    /// Immutable metrics describing the current execution progress.
    pub metrics: Vec<MetricSnapshot>,
    /// Unix epoch milliseconds when this snapshot was produced.
    pub updated_at_ms: u64,
}

impl ProgressCommand {
    /// Creates a progress update for the specified task attempt.
    #[must_use]
    pub fn new(
        id: TaskId,
        expected_attempt: u32,
        progress_version: u64,
        stage: Option<Stage>,
        metrics: Vec<MetricSnapshot>,
        updated_at_ms: u64,
    ) -> Self {
        Self {
            id,
            expected_attempt,
            progress_version,
            stage,
            metrics,
            updated_at_ms,
        }
    }
}
