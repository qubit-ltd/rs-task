// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Carries one external cancellation hook outcome to all waiting callers.
#[must_use]
#[derive(Clone)]
pub(in crate::service::typed_task_execution_service) enum ExternalCancelResult {
    /// The hook succeeded and cancellation intent was persisted.
    Success,
    /// The task attempt ended before the hook result could be applied.
    AlreadyTerminal,
    /// The hook failed and its error was persisted.
    HookFailed(String),
    /// A store or service operation failed while processing the hook.
    ServiceFailed(String),
}
