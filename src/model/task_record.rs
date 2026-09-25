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
    /// Returns whether every persisted diagnostic field satisfies its byte
    /// limit.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the state can be persisted, or a static diagnostic naming
    /// the exceeded limit.
    pub(crate) fn validate_diagnostics(&self) -> Result<(), &'static str> {
        use super::task_request::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
        use super::task_request::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;

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
}

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
    #[must_use]
    #[inline]
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

/// Queryable task lifecycle snapshot.
///
/// The state version increases after every successful lifecycle transition.
/// Timestamps are Unix epoch milliseconds, and `attempt` counts starts rather
/// than submissions.
///
/// # Examples
///
/// ```
/// use qubit_task::TaskExecutionService;
/// use qubit_task::model::TaskRequest;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let service = TaskExecutionService::in_memory().await?;
///     let record = service.submit(TaskRequest::new("report", "1", vec![])).await?;
///     assert_eq!(record.attempt, 0);
///     service.shutdown().await?;
///     Ok(())
/// }
/// ```
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
///
/// Empty `states` matches every state. A zero `limit` is treated as one
/// record, and `after` is an exclusive task-ID cursor.
///
/// # Examples
///
/// ```
/// use qubit_task::model::{TaskQuery, TaskStateKind};
///
/// let query = TaskQuery { states: vec![TaskStateKind::Queued], limit: 20, ..TaskQuery::default() };
/// assert_eq!(query.limit, 20);
/// ```
#[derive(Debug, Clone, Default)]
pub struct TaskQuery {
    /// Optional set of lifecycle states to include.
    pub states: Vec<TaskStateKind>,
    /// Maximum number of records to return.
    pub limit: usize,
    /// Opaque cursor represented by a task ID.
    pub after: Option<TaskId>,
    /// Optional exact business correlation key.
    pub correlation_key: Option<String>,
}

/// One bounded page of task history.
///
/// `next` is present only when another page may exist; pass it as the next
/// query's exclusive `after` cursor.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskPage;
///
/// let page = TaskPage::default();
/// assert!(page.records.is_empty());
/// ```
#[derive(Debug, Clone, Default)]
pub struct TaskPage {
    /// Records selected by the query.
    pub records: Vec<TaskRecord>,
    /// Cursor for the next page, when more data may exist.
    pub next: Option<TaskId>,
}

/// Aggregate task counts suitable for service monitoring.
///
/// Terminal counts include every retained succeeded, failed, panicked, and
/// cancelled record.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskStateCounts;
///
/// let counts = TaskStateCounts::default();
/// assert_eq!(counts.queued + counts.running + counts.blocked + counts.terminal, 0);
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TaskStateCounts {
    /// Number of retained tasks waiting for resources.
    pub queued: usize,
    /// Number of retained tasks currently executing.
    pub running: usize,
    /// Number of retained tasks requiring intervention.
    pub blocked: usize,
    /// Number of retained terminal task records.
    pub terminal: usize,
}

/// Aggregate task counts and resource capacity suitable for service monitoring.
///
/// Store counts and resource usage are collected consecutively and are not an
/// atomic snapshot across both components.
///
/// # Examples
///
/// ```
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let service = qubit_task::TaskExecutionService::in_memory().await?;
///     let stats = service.stats().await?;
///     assert_eq!(stats.queued, 0);
///     service.shutdown().await?;
///     Ok(())
/// }
/// ```
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
///
/// Recovery pages contain only unfinished work; terminal history remains
/// available through the store's ordinary query interface.
///
/// # Examples
///
/// ```
/// use qubit_task::model::{StoredTask, TaskId, TaskRecord, TaskRequest, TaskState};
///
/// let task = StoredTask {
///     record: TaskRecord {
///         id: TaskId::generate(),
///         request: TaskRequest::new("report", "1", vec![]),
///         state: TaskState::Queued,
///         state_version: 0,
///         attempt: 0,
///         accepted_at_ms: 0,
///         started_at_ms: None,
///         finished_at_ms: None,
///         assigned_resources: Vec::new(),
///         output: None,
///         cancel_requested: false,
///     },
/// };
/// assert!(!task.record.state.is_terminal());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTask {
    /// Task lifecycle snapshot.
    pub record: TaskRecord,
}

/// Store-level capability claims made during service assembly.
///
/// `restart_recovery` implies that unfinished request descriptions can be
/// scanned after restart; it does not promise recovery of process-local values.
///
/// # Examples
///
/// ```
/// use qubit_task::model::StoreCapabilities;
///
/// let volatile = StoreCapabilities { persistent_history: false, restart_recovery: false };
/// assert!(!volatile.restart_recovery);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreCapabilities {
    /// Whether task history survives a process restart.
    pub persistent_history: bool,
    /// Whether accepted unfinished work can be recovered after restart.
    pub restart_recovery: bool,
}

/// Result of atomically accepting a task request.
///
/// `Existing` is returned for an identical request with the same idempotency
/// key, so callers should use the returned record in either case.
///
/// # Examples
///
/// ```
/// use qubit_task::model::{AcceptOutcome, TaskId, TaskRecord, TaskRequest, TaskState};
///
/// let outcome = AcceptOutcome::Accepted(TaskRecord {
///     id: TaskId::generate(),
///     request: TaskRequest::new("report", "1", vec![]),
///     state: TaskState::Queued,
///     state_version: 0,
///     attempt: 0,
///     accepted_at_ms: 0,
///     started_at_ms: None,
///     finished_at_ms: None,
///     assigned_resources: Vec::new(),
///     output: None,
///     cancel_requested: false,
/// });
/// let record = match outcome {
///     AcceptOutcome::Accepted(record) | AcceptOutcome::Existing(record) => record,
/// };
/// assert_eq!(record.state, TaskState::Queued);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// A new task was accepted.
    Accepted(TaskRecord),
    /// An identical idempotent request already exists.
    Existing(TaskRecord),
}

/// Conditional task state update guarded by state version and attempt.
///
/// Stores reject stale commands with a conflict, preventing a delayed worker
/// from overwriting a newer task lifecycle revision.
///
/// # Examples
///
/// ```
/// use qubit_task::model::{TaskId, TaskState, TransitionCommand};
///
/// let command = TransitionCommand {
///     id: TaskId::generate(),
///     expected_version: 0,
///     expected_attempt: 0,
///     state: qubit_task::model::TaskState::Running,
///     output: None,
///     assigned_resources: Vec::new(),
///     cancel_requested: false,
/// };
/// assert!(!command.cancel_requested);
/// ```
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
///
/// A recoverable store issues an epoch when a service acquires ownership and
/// requires the same epoch when that service releases it.
///
/// # Examples
///
/// ```
/// use qubit_task::model::OwnerEpoch;
///
/// let epoch = OwnerEpoch(7);
/// assert_eq!(epoch.0, 7);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerEpoch(pub u64);

/// Page of unfinished stored work returned during recovery.
///
/// The cursor can be passed back to `scan_unfinished` until `next` is `None`.
///
/// # Examples
///
/// ```
/// use qubit_task::model::StoredTaskPage;
///
/// let page = StoredTaskPage::default();
/// assert!(page.tasks.is_empty());
/// assert!(page.next.is_none());
/// ```
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
    ///
    /// # Returns
    ///
    /// A borrow of the resource request stored inside this record.
    #[must_use]
    #[inline]
    pub fn resource_request(&self) -> &ResourceRequest {
        &self.request.resources
    }
}
