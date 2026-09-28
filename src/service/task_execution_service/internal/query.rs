// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::ServiceCore;
use crate::model::TaskStateCounts;
use crate::model::TaskStats;
use crate::service::TaskServiceError;

/// Reads store state counts and the current execution resource snapshot.
///
/// # Parameters
///
/// * `core` - Service state whose store and engine are observed.
///
/// # Returns
///
/// State counts and current resource reservations.
///
/// # Errors
///
/// Returns a service error if the store count query fails.
pub(in crate::service::task_execution_service) async fn task_stats(
    core: &ServiceCore,
) -> Result<TaskStats, TaskServiceError> {
    let TaskStateCounts {
        queued,
        running,
        blocked,
        terminal,
    } = core.store.count_states().await.map_err(TaskServiceError::Store)?;
    Ok(TaskStats {
        queued,
        running,
        blocked,
        terminal,
        resources: core.engine.capacity(),
    })
}
