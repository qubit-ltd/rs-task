// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;

use super::allocation::Allocation;

/// Active reservations indexed by their release token.
pub(in crate::engine::local_task_execution_engine) type AllocationLedger = HashMap<u64, Allocation>;
