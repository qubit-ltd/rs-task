// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;

use super::ResourceCapacity;

/// Current resource usage exposed for health and metrics reporting.
///
/// # Examples
///
/// ```
/// use qubit_task::model::ResourceSnapshot;
///
/// let snapshot = ResourceSnapshot::default();
/// assert_eq!(snapshot.used_cpu_slots, 0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceSnapshot {
    /// Configured resource capacity.
    pub capacity: ResourceCapacity,
    /// Currently reserved CPU slots.
    pub used_cpu_slots: u32,
    /// Currently reserved GPU device identifiers.
    pub used_gpus: Vec<String>,
    /// Currently reserved custom resources.
    pub used_custom: BTreeMap<String, u64>,
}
