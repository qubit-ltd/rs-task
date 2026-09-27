// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

/// Maximum number of bytes retained for a task output summary.
pub const MAX_TASK_OUTPUT_SUMMARY_BYTES: usize = 64 * 1024;

/// Small result summary saved with the task record.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskOutput;
///
/// let output = TaskOutput { summary: b"stored result key".to_vec() };
/// assert_eq!(output.summary, b"stored result key");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[must_use]
pub struct TaskOutput {
    /// Bounded opaque summary or external result reference.
    pub summary: Vec<u8>,
}
