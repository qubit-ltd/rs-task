// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use super::ResourceRequest;
use super::TaskId;
use super::TaskOutput;
use super::TaskRequest;

/// Observable lifecycle state for an accepted task.
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
    /// Reports whether this state ends normal task execution.
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
}

/// Queryable task lifecycle snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    /// Stable service-generated identity.
    pub id: TaskId,
    /// Reconstructible work description.
    pub request: TaskRequest,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Monotonically increasing state revision.
    pub state_version: u64,
    /// Number of execution attempts started.
    pub attempt: u32,
    /// Milliseconds since Unix epoch when accepted.
    pub accepted_at_ms: u64,
    /// Milliseconds since Unix epoch when execution last started.
    pub started_at_ms: Option<u64>,
    /// Milliseconds since Unix epoch when execution became terminal.
    pub finished_at_ms: Option<u64>,
    /// Actual resources assigned to the current or last attempt.
    pub assigned_resources: Vec<String>,
    /// Small output summary for successful work.
    pub output: Option<TaskOutput>,
    /// True after cooperative cancellation has been requested.
    pub cancel_requested: bool,
}

/// Filters and bounds a task history query.
#[derive(Debug, Clone, Default)]
pub struct TaskQuery {
    /// Optional set of lifecycle states to include.
    pub states: Vec<TaskState>,
    /// Maximum number of records to return.
    pub limit: usize,
    /// Opaque cursor represented by a task ID.
    pub after: Option<TaskId>,
    /// Optional exact business correlation key.
    pub correlation_key: Option<String>,
}

/// One bounded page of task history.
#[derive(Debug, Clone, Default)]
pub struct TaskPage {
    /// Records selected by the query.
    pub records: Vec<TaskRecord>,
    /// Cursor for the next page, when more data may exist.
    pub next: Option<TaskId>,
}

/// Aggregate task counts suitable for service monitoring.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskStats {
    /// Number of tasks waiting for resources.
    pub queued: usize,
    /// Number of currently executing tasks.
    pub running: usize,
    /// Number of tasks requiring intervention.
    pub blocked: usize,
    /// Number of retained terminal records.
    pub terminal: usize,
    /// Resource snapshot at the time of collection.
    pub resources: super::ResourceSnapshot,
}

/// Complete request and record used to reconstruct an unfinished task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTask {
    /// Task lifecycle snapshot.
    pub record: TaskRecord,
}

/// Store-level capability claims made during service assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreCapabilities {
    /// Whether task history survives a process restart.
    pub persistent_history: bool,
    /// Whether accepted unfinished work can be recovered after restart.
    pub restart_recovery: bool,
}

/// Result of atomically accepting a task request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// A new task was accepted.
    Accepted(TaskRecord),
    /// An identical idempotent request already exists.
    Existing(TaskRecord),
}

/// Conditional task state update guarded by state version and attempt.
#[derive(Debug, Clone)]
pub struct TransitionCommand {
    /// Target task identity.
    pub id: TaskId,
    /// Expected previous state version.
    pub expected_version: u64,
    /// Expected execution generation.
    pub expected_attempt: u32,
    /// New observable state.
    pub state: TaskState,
    /// Optional result summary.
    pub output: Option<TaskOutput>,
    /// Assigned device identifiers.
    pub assigned_resources: Vec<String>,
    /// Whether this transition requests cancellation.
    pub cancel_requested: bool,
}

/// Exclusive store-owner generation for one running service process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerEpoch(pub u64);

/// Page of unfinished stored work returned during recovery.
#[derive(Debug, Clone, Default)]
pub struct StoredTaskPage {
    /// Reconstructible unfinished tasks.
    pub tasks: Vec<StoredTask>,
    /// Cursor for another recovery page.
    pub next: Option<TaskId>,
}

/// Resource request helper available without opening the original request.
impl TaskRecord {
    /// Returns the resource demand used to validate and schedule this task.
    #[must_use]
    #[inline]
    pub fn resource_request(&self) -> &ResourceRequest {
        &self.request.resources
    }
}
