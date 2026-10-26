// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::service::TaskServiceError;

/// Retains one shutdown result for callers waiting on an attempt.
#[derive(Clone)]
pub(in crate::service::typed_task_execution_service) enum ShutdownOutcome {
    /// Shutdown completed and the store owner was released.
    Complete,
    /// Store ownership could not be released or a service fault was latched.
    StoreUnavailable(String),
    /// The publisher failed to drain before shutdown completed.
    #[cfg(feature = "event-bus")]
    NotificationClose(String),
}

impl ShutdownOutcome {
    /// Reconstructs an owned public error for each independent shutdown waiter.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when shutdown completed, or the public error retained
    /// by this outcome.
    ///
    /// # Errors
    ///
    /// Returns `TaskServiceError::StoreUnavailable` when store ownership could
    /// not be released or a service fault was latched. With the `event-bus`
    /// feature enabled, returns `TaskServiceError::NotificationClose` when the
    /// publisher failed to drain.
    #[inline]
    pub(in crate::service::typed_task_execution_service) fn into_result(self) -> Result<(), TaskServiceError> {
        match self {
            Self::Complete => Ok(()),
            Self::StoreUnavailable(error) => Err(TaskServiceError::StoreUnavailable(error)),
            #[cfg(feature = "event-bus")]
            Self::NotificationClose(error) => Err(TaskServiceError::NotificationClose(error)),
        }
    }
}
