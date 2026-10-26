// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::task_progress_snapshot::MAX_TASK_PROGRESS_METRICS;
use super::task_progress_snapshot::MAX_TASK_PROGRESS_SNAPSHOT_BYTES;

/// Snapshot validation failures returned before a store transaction begins.
///
/// # Examples
///
/// `TaskProgressSnapshot::from_command` returns this error when a progress
/// command cannot be represented as a bounded snapshot:
///
/// ```
/// use qubit_task::model::typed::ProgressCommand;
/// use qubit_task::model::typed::ProgressSnapshotError;
/// use qubit_task::model::typed::TaskProgressSnapshot;
///
/// fn validate_progress(command: ProgressCommand) {
///     match TaskProgressSnapshot::from_command(command) {
///         Ok(snapshot) => println!("validated progress version {}", snapshot.progress_version),
///         Err(error) => report_snapshot_error(error),
///     }
/// }
///
/// fn report_snapshot_error(error: ProgressSnapshotError) {
///     eprintln!("could not store progress snapshot: {error}");
/// }
/// ```
#[must_use]
#[derive(Debug, thiserror::Error)]
pub enum ProgressSnapshotError {
    /// The supplied metric vector exceeds the documented count limit.
    #[error("task progress has {0} metrics; the maximum is {MAX_TASK_PROGRESS_METRICS}")]
    TooManyMetrics(
        /// Number of metrics in the supplied progress command.
        usize,
    ),
    /// Stage identifier exceeds the task snapshot protocol limit.
    #[error("task progress stage ID has {0} bytes; the maximum is 128")]
    StageIdTooLarge(
        /// UTF-8 byte length of the stage identifier.
        usize,
    ),
    /// Stage name exceeds the task snapshot protocol limit.
    #[error("task progress stage name has {0} bytes; the maximum is 256")]
    StageNameTooLarge(
        /// UTF-8 byte length of the stage name.
        usize,
    ),
    /// JSON serialization of the snapshot failed.
    #[error("failed to encode progress snapshot: {0}")]
    Serialize(
        /// Serialization error returned while encoding the snapshot as JSON.
        #[source]
        serde_json::Error,
    ),
    /// The encoded snapshot exceeds the documented byte limit.
    #[error("task progress snapshot has {0} bytes; the maximum is {MAX_TASK_PROGRESS_SNAPSHOT_BYTES}")]
    TooLarge(
        /// Number of bytes in the serialized JSON snapshot.
        usize,
    ),
}
