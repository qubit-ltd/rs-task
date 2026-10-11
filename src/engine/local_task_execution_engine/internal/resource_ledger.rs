// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;
use std::collections::HashMap;

use parking_lot::Mutex;

use super::usage::Usage;

/// Resources held by one execution reservation.
///
/// The tuple stores CPU, memory, and disk amounts, followed by GPU identifiers
/// and custom resource amounts keyed by name.
pub(in crate::engine::local_task_execution_engine) type Allocation =
    (u32, u64, u64, Vec<String>, BTreeMap<String, u64>);
/// Active reservations indexed by their release token.
pub(in crate::engine::local_task_execution_engine) type AllocationLedger = HashMap<u64, Allocation>;

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

/// Removes one reservation and subtracts its resources from aggregate usage.
///
/// The reservation lookup and usage updates happen while holding the ledger
/// mutex. An unknown token leaves the ledger unchanged.
///
/// # Parameters
///
/// * `token` - Unique key of the reservation to release.
/// * `ledger` - Shared resource totals and reservation map.
pub(in crate::engine::local_task_execution_engine) fn release_reservation(token: u64, ledger: &Mutex<ResourceLedger>) {
    let mut ledger = ledger.lock();
    if let Some((cpu, memory, disk, gpus, custom)) = ledger.allocations.remove(&token) {
        ledger.usage.cpu = ledger.usage.cpu.saturating_sub(cpu);
        ledger.usage.memory = ledger.usage.memory.saturating_sub(memory);
        ledger.usage.disk = ledger.usage.disk.saturating_sub(disk);
        ledger.usage.gpus.retain(|id| !gpus.contains(id));
        for (name, amount) in custom {
            if let Some(value) = ledger.usage.custom.get_mut(&name) {
                *value = value.saturating_sub(amount);
                if *value == 0 {
                    ledger.usage.custom.remove(&name);
                }
            }
        }
    }
}
