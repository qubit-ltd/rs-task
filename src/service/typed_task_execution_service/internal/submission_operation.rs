// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::panic::AssertUnwindSafe;
use std::panic::resume_unwind;
use std::sync::Arc;

use futures::FutureExt;

use super::ServiceRunner;
use super::TypedServiceCore;
use crate::model::typed::StoredTaskRequest;
use crate::model::typed::TaskId;
use crate::model::typed::TaskSummary;
use crate::service::TaskServiceError;

/// Service-owned final acceptance and result-notification operation.
///
/// The owned admission guard keeps shutdown from releasing store ownership
/// until acceptance, result mapping, and required notifications have finished.
pub(in crate::service::typed_task_execution_service) struct SubmissionOperation {
    /// Service state used to accept and notify the submission.
    core: Arc<TypedServiceCore>,
    /// Admission lock held until the operation finishes.
    admission: tokio::sync::OwnedRwLockReadGuard<()>,
    /// Identifier reserved for this submission.
    id: TaskId,
    /// Encoded request passed to the task store.
    request: StoredTaskRequest,
}

impl SubmissionOperation {
    /// Creates an operation that remains alive independently of the submit
    /// future once spawned.
    ///
    /// # Parameters
    ///
    /// - `core`: The service state that owns the task store and notifications.
    /// - `admission`: The guard that keeps shutdown from releasing the store.
    /// - `id`: The identifier reserved for the request.
    /// - `request`: The encoded task request to accept.
    pub(in crate::service::typed_task_execution_service) fn new(
        core: Arc<TypedServiceCore>,
        admission: tokio::sync::OwnedRwLockReadGuard<()>,
        id: TaskId,
        request: StoredTaskRequest,
    ) -> Self {
        Self {
            core,
            admission,
            id,
            request,
        }
    }

    /// Accepts the request, maps any store failure, wakes consumers, and then
    /// releases the admission guard.
    ///
    /// # Returns
    ///
    /// The accepted task summary or the corresponding service error.
    ///
    /// # Parameters
    ///
    /// This method takes ownership of the operation and has no parameters.
    ///
    /// # Errors
    ///
    /// Returns structured submission rejection errors or mapped store errors.
    pub(in crate::service::typed_task_execution_service) async fn run(
        self,
    ) -> Result<TaskSummary, TaskServiceError> {
        let Self {
            core,
            admission,
            id,
            request,
        } = self;
        let runner = ServiceRunner {
            core: Arc::clone(&core),
        };
        let accepted = AssertUnwindSafe(async { core.store.accept_encoded(id, request).await })
            .catch_unwind()
            .await;
        let result = match accepted {
            Ok(result) => runner.submission_store_result(result),
            Err(payload) => {
                runner.latch_fault("task submission store operation panicked");
                drop(admission);
                resume_unwind(payload)
            }
        };
        if let Ok(accepted) = &result {
            #[cfg(feature = "event-bus")]
            if let Some(publisher) = &core.publisher {
                if accepted.created {
                    publisher.notify();
                } else {
                    publisher.wake();
                }
            }
            core.scheduler_changed.notify_one();
            let summary = accepted.summary.clone();
            drop(admission);
            return Ok(summary);
        }
        drop(admission);
        result.map(|accepted| accepted.summary)
    }
}
