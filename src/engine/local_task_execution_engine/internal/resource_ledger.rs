// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashSet;

use parking_lot::Mutex;

mod allocation;
mod allocation_ledger;
#[path = "resource_ledger/resource_ledger.rs"]
mod ledger;

pub(in crate::engine::local_task_execution_engine) use ledger::ResourceLedger;

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
        remove_reserved_gpus(&mut ledger.usage.gpus, &gpus);
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

/// Removes GPU identifiers assigned to a released reservation.
///
/// Borrowed set entries avoid cloning identifiers while replacing repeated
/// linear scans of the reservation list with average constant-time lookups.
fn remove_reserved_gpus(usage_gpus: &mut Vec<String>, reserved_gpus: &[String]) {
    let reserved_gpus: HashSet<&str> = reserved_gpus.iter().map(String::as_str).collect();
    usage_gpus.retain(|id| !reserved_gpus.contains(id.as_str()));
}
