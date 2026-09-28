// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

/// Payload-free category of a task lifecycle state.
///
/// Use this type for filters that should match a state regardless of its
/// diagnostic payload, such as every blocked task.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskStateKind;
///
/// let filter = vec![TaskStateKind::Queued, TaskStateKind::Blocked];
/// assert!(filter.contains(&TaskStateKind::Blocked));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskStateKind {
    /// Accepted and waiting for suitable resources.
    Queued,
    /// Resources have been reserved and execution has started.
    Running,
    /// Requires operator or business intervention before it can continue.
    Blocked,
    /// Handler returned successfully.
    Succeeded,
    /// Handler returned a non-retryable error.
    Failed,
    /// Handler panicked.
    Panicked,
    /// Cancellation was acknowledged by the handler or before execution.
    Cancelled,
}

impl TaskStateKind {
    /// Returns the stable SQLite state key for this lifecycle category.
    ///
    /// # Returns
    ///
    /// The case-sensitive state label persisted by SQLite.
    #[must_use]
    #[inline]
    #[cfg(feature = "sqlite")]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Running => "Running",
            Self::Blocked => "Blocked",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
            Self::Panicked => "Panicked",
            Self::Cancelled => "Cancelled",
        }
    }
}
