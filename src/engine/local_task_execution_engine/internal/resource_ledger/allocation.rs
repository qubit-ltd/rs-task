// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;

/// Resources held by one execution reservation.
///
/// The tuple stores CPU, memory, and disk amounts, followed by GPU identifiers
/// and custom resource amounts keyed by name.
pub(in crate::engine::local_task_execution_engine) type Allocation =
    (u32, u64, u64, Vec<String>, BTreeMap<String, u64>);
