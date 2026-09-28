// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Service construction error, including unsupported or unavailable recovery.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskServiceBuildError;
///
/// let error = TaskServiceBuildError::MissingStore;
/// assert!(error.to_string().contains("must be selected"));
/// ```
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum TaskServiceBuildError {
    /// A generic builder did not select a store explicitly.
    #[error("a task store must be selected explicitly")]
    MissingStore,
    /// Recovery was required but the selected store does not support it.
    #[error("restart recovery was required but the selected store does not support it")]
    RecoveryRequired,
    /// Store initialization or recovery scan failed.
    #[error(transparent)]
    Store(
        /// Store initialization or recovery error.
        #[from]
        crate::store::StoreError,
    ),
    /// Two handlers claimed the same task type and version.
    #[error("{0}")]
    HandlerConflict(
        /// Diagnostic identifying the conflicting handler registrations.
        String,
    ),
    /// SQLite support is disabled for this crate build.
    #[error("SQLite support requires the `sqlite` feature")]
    SqliteFeatureDisabled,
    /// Existing unfinished work is larger than the configured recovery bound.
    #[error("unfinished task count exceeds recovery capacity {limit}")]
    RecoveryCapacityExceeded {
        /// Maximum unfinished task count accepted by this configuration.
        limit: usize,
    },
    /// A task store returned an invalid recovery page.
    #[error("invalid recovery page: {0}")]
    InvalidRecoveryPage(
        /// Diagnostic describing the malformed recovery page.
        String,
    ),
    /// The selected queue and running capacities overflow the supported range.
    #[error("invalid service configuration: {0}")]
    InvalidConfiguration(
        /// Diagnostic describing the invalid service configuration.
        String,
    ),
    /// Construction worker panicked or stopped before returning a result.
    #[error("service construction worker stopped unexpectedly")]
    WorkerStopped,
    /// Construction failed and releasing the store owner also failed.
    #[error("{primary}; releasing store ownership also failed: {cleanup}")]
    CleanupFailed {
        /// Original construction failure.
        #[source]
        primary: Box<TaskServiceBuildError>,
        /// Failure while releasing the acquired owner.
        cleanup: crate::store::StoreError,
    },
    /// The dedicated lifecycle event publisher thread could not start.
    #[cfg(feature = "event-bus")]
    #[error("failed to start task event publisher thread: {0}")]
    EventPublisherThread(
        /// Operating system error returned while spawning the publisher.
        #[source]
        std::io::Error,
    ),
}
