// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::allocation_ledger::AllocationLedger;
use crate::engine::local_task_execution_engine::internal::usage::Usage;

/// Aggregate usage and the resources assigned to active reservation tokens.
#[derive(Default)]
pub(in crate::engine::local_task_execution_engine) struct ResourceLedger {
    /// Aggregate resources currently held by active attempts.
    pub(in crate::engine::local_task_execution_engine) usage: Usage,
    /// Resources associated with each reservation token.
    pub(in crate::engine::local_task_execution_engine) allocations: AllocationLedger,
    /// Next unused token; `None` indicates that the token space is exhausted.
    pub(in crate::engine::local_task_execution_engine) next_token: Option<u64>,
}
