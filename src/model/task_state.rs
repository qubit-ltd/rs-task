// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use super::task_run_error::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
use super::task_run_error::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
use super::task_state_kind::TaskStateKind;

/// Observable lifecycle state for an accepted task.
///
/// A blocked task needs intervention before it can be requeued. Terminal
/// states cannot transition again, while a running task may return to the
/// queue after a retryable failure.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskState;
///
/// let state = TaskState::Queued;
/// assert!(!state.is_terminal());
/// assert!(state.allows_transition_to(&TaskState::Running));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    /// Accepted and waiting for suitable resources.
    Queued,
    /// Resources have been reserved and execution has started.
    Running,
    /// Requires operator or business intervention before it can continue.
    Blocked {
        /// Explains which external condition must be resolved before retrying.
        reason: String,
    },
    /// Handler returned successfully.
    Succeeded,
    /// Handler returned a non-retryable error.
    Failed {
        /// Stable error category used by callers for classification.
        category: String,
        /// Human-readable diagnostic for this attempt.
        message: String,
    },
    /// Handler panicked.
    Panicked {
        /// Panic diagnostic captured from the handler execution.
        message: String,
    },
    /// Cancellation was acknowledged by the handler or before execution.
    Cancelled,
}

impl TaskState {
    /// Returns the payload-free lifecycle category used by history filters.
    ///
    /// # Returns
    ///
    /// The lifecycle variant without any diagnostic strings.
    #[must_use]
    #[inline]
    pub fn kind(&self) -> TaskStateKind {
        match self {
            Self::Queued => TaskStateKind::Queued,
            Self::Running => TaskStateKind::Running,
            Self::Blocked { .. } => TaskStateKind::Blocked,
            Self::Succeeded => TaskStateKind::Succeeded,
            Self::Failed { .. } => TaskStateKind::Failed,
            Self::Panicked { .. } => TaskStateKind::Panicked,
            Self::Cancelled => TaskStateKind::Cancelled,
        }
    }

    /// Reports whether this state ends normal task execution.
    ///
    /// # Returns
    ///
    /// `true` for succeeded, failed, panicked, or cancelled states.
    #[must_use]
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed { .. } | Self::Panicked { .. } | Self::Cancelled
        )
    }

    /// Reports whether the lifecycle may advance to `next` under the service
    /// contract.
    ///
    /// # Parameters
    ///
    /// * `next` - Proposed next lifecycle state.
    ///
    /// # Returns
    ///
    /// Whether the service contract permits that transition.
    #[must_use]
    pub fn allows_transition_to(&self, next: &Self) -> bool {
        match self {
            Self::Queued => matches!(next, Self::Running | Self::Blocked { .. } | Self::Cancelled),
            Self::Running => matches!(
                next,
                Self::Running
                    | Self::Queued
                    | Self::Blocked { .. }
                    | Self::Succeeded
                    | Self::Failed { .. }
                    | Self::Panicked { .. }
                    | Self::Cancelled
            ),
            Self::Blocked { .. } => matches!(next, Self::Queued | Self::Cancelled),
            Self::Succeeded | Self::Failed { .. } | Self::Panicked { .. } | Self::Cancelled => false,
        }
    }

    /// Returns whether every persisted diagnostic field satisfies its byte
    /// limit.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the state can be persisted, or a static diagnostic naming
    /// the exceeded limit.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when a blocked reason, failure category, or
    /// lifecycle message exceeds its byte limit.
    pub(crate) fn validate_diagnostics(&self) -> Result<(), &'static str> {
        match self {
            Self::Blocked { reason } if reason.len() > MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES => {
                Err("blocked reason exceeds the 4096-byte limit")
            }
            Self::Failed { category, message }
                if category.len() > MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES
                    || message.len() > MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES =>
            {
                Err("failure diagnostic exceeds its byte limit")
            }
            Self::Panicked { message } if message.len() > MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES => {
                Err("panic diagnostic exceeds the 4096-byte limit")
            }
            _ => Ok(()),
        }
    }
}
