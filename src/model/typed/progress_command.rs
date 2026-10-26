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
///
/// # Examples
///
/// ```
/// use qubit_progress::Stage;
/// use qubit_task::model::ProgressCommand;
/// use qubit_task::model::TaskId;
///
/// let command = ProgressCommand::new(
///     TaskId::from_id(qubit_id::Id::new(42)),
///     3,
///     7,
///     Some(Stage::new("download", "Download files")),
///     Vec::new(),
///     1_700_000_000_000,
/// );
///
/// assert_eq!(command.progress_version, 7);
/// assert_eq!(command.expected_attempt, 3);
/// ```
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
    ///
    /// # Parameters
    ///
    /// - `id`: stable identifier of the task being updated.
    /// - `expected_attempt`: running attempt that produced this update.
    /// - `progress_version`: monotonically increasing version within that
    ///   attempt.
    /// - `stage`: optional current execution stage.
    /// - `metrics`: immutable metric snapshots produced by the progress
    ///   operation.
    /// - `updated_at_ms`: Unix epoch milliseconds when the snapshot was
    ///   produced.
    ///
    /// # Returns
    ///
    /// A progress command containing the supplied task attempt snapshot.
    #[must_use]
    #[inline]
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
