// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Storage errors distinguish unsupported capabilities from ordinary
/// persistence failures.
///
/// # Examples
///
/// ```
/// use qubit_task::store::StoreError;
///
/// let error = StoreError::NotFound;
/// assert_eq!(error.to_string(), "task was not found");
/// ```
#[derive(Debug, Error)]
#[must_use]
pub enum StoreError {
    /// The operation requires a capability that this store does not provide.
    #[error("the selected task store does not support the requested capability")]
    UnsupportedCapability,
    /// This platform cannot verify the SQLite file identity needed for locking.
    #[error("the SQLite database file identity cannot be verified on this platform")]
    UnsupportedDatabaseIdentity,
    /// Another owner holds the database lock or this store already owns it.
    #[error("the SQLite database already has an active owner")]
    OwnerConflict,
    /// A task ID already belongs to a different accepted request.
    #[error("task identifier already exists")]
    DuplicateTask,
    /// An idempotency key was reused with a different request.
    #[error("idempotency key was reused with a different task request")]
    IdempotencyConflict,
    /// The in-memory store cannot retain the request payload within its
    /// configured budget.
    #[error("task payload budget exceeded: requested {requested_bytes} bytes, {available_bytes} bytes available")]
    CapacityExceeded {
        /// Bytes in the request that could not be retained.
        requested_bytes: usize,
        /// Bytes available after evicting eligible terminal records.
        available_bytes: usize,
    },
    /// The in-memory store already retains its configured maximum number of
    /// nonterminal task records.
    #[error("unfinished task record limit exceeded ({limit})")]
    UnfinishedRecordLimitExceeded {
        /// Maximum number of nonterminal records this store retains.
        limit: usize,
    },
    /// The expected state revision or attempt no longer matches.
    #[error("task state changed before the requested transition")]
    Conflict,
    /// A request or persisted diagnostic violates a documented size limit.
    #[error("invalid task data: {0}")]
    InvalidRequest(
        /// Static diagnostic naming the invalid request or exceeded limit.
        &'static str,
    ),
    /// A valid revision attempted an illegal lifecycle transition.
    #[error("task lifecycle transition is not allowed")]
    InvalidTransition,
    /// No task with the requested identifier is retained.
    #[error("task was not found")]
    NotFound,
    /// Persistence implementation reported an operational failure.
    #[error("task store failure: {0}")]
    Failure(
        /// Backend diagnostic describing the persistence failure.
        String,
    ),
}
