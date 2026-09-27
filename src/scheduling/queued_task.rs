// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::ResourceRequest;
use crate::model::TaskId;

/// Queue entry metadata needed to keep a frequently bypassed request
/// progressing.
///
/// # Examples
///
/// ```
/// use qubit_task::model::ResourceRequest;
/// use qubit_task::model::TaskId;
/// use qubit_task::scheduling::QueuedTask;
///
/// let task = QueuedTask {
///     id: TaskId::generate(),
///     resources: ResourceRequest::default(),
///     retry_not_before_ms: None,
///     bypasses: 0,
/// };
/// assert_eq!(task.bypasses, 0);
/// ```
#[derive(Debug, Clone)]
pub struct QueuedTask {
    /// Stable task identity.
    pub id: TaskId,
    /// Resource requirements used to determine likely fit.
    pub resources: ResourceRequest,
    /// Earliest Unix epoch millisecond when a retry may be considered.
    pub retry_not_before_ms: Option<u64>,
    /// Number of scheduling cycles in which a later task started first.
    pub bypasses: u32,
}
