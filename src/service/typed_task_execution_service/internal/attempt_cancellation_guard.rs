// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Scope guard for a registered task attempt cancellation state.

use std::sync::Arc;

use crate::handler::typed::attempt_cancellation::AttemptCancellation;
use crate::model::typed::TaskId;

use super::TypedServiceCore;

/// Removes only the exact state registered by this execution attempt.
pub(in crate::service::typed_task_execution_service) struct AttemptCancellationGuard {
    core: Arc<TypedServiceCore>,
    key: (TaskId, u32),
    state: Arc<AttemptCancellation>,
}

impl AttemptCancellationGuard {
    pub(in crate::service::typed_task_execution_service) fn new(
        core: Arc<TypedServiceCore>,
        key: (TaskId, u32),
        state: Arc<AttemptCancellation>,
    ) -> Self {
        Self { core, key, state }
    }
}

impl Drop for AttemptCancellationGuard {
    fn drop(&mut self) {
        let mut entries = self.core.cancellations.lock();
        if entries
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.state))
        {
            entries.remove(&self.key);
        }
    }
}
