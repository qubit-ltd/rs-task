// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::typed::TaskId;

/// Follow-up work that must complete after a service-owned local transition.
pub(in crate::service::task_execution_service) enum LocalTransitionAction {
    /// Wake the scheduler after making a task ready or terminal.
    WakeScheduler,
    /// Signal a cooperative cancellation for this task attempt.
    SignalCooperative(
        /// Identity and attempt number of the task to signal.
        (TaskId, u32),
    ),
    /// Perform no additional action after the store transition.
    None,
}
