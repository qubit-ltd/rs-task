// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

/// Resource limits available to this service instance.
///
/// # Examples
///
/// ```
/// use qubit_task::model::ResourceCapacity;
///
/// let capacity = ResourceCapacity {
///     cpu_slots: 4,
///     ..ResourceCapacity::default()
/// };
/// assert!(capacity.cpu_slots >= 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResourceCapacity {
    /// CPU concurrency slots available to tasks.
    pub cpu_slots: u32,
    /// Optional memory quota capacity in bytes.
    pub memory_bytes: Option<u64>,
    /// Optional disk quota capacity in bytes.
    pub disk_bytes: Option<u64>,
    /// GPU device identifiers and their labels.
    pub gpus: BTreeMap<String, Vec<String>>,
    /// Named integer resource limits.
    pub custom: BTreeMap<String, u64>,
}
