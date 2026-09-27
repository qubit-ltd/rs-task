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

/// Resource units that a task must hold for its entire execution.
///
/// # Examples
///
/// ```
/// use qubit_task::model::ResourceRequest;
///
/// let request = ResourceRequest {
///     cpu_slots: 2,
///     ..ResourceRequest::default()
/// };
/// assert_eq!(request.cpu_slots, 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResourceRequest {
    /// Number of CPU concurrency slots.
    pub cpu_slots: u32,
    /// Number of distinct GPU devices requested.
    pub gpu_count: u32,
    /// Minimum labels required from every assigned GPU.
    pub gpu_labels: Vec<String>,
    /// Named exclusive integer resources such as memory or licenses.
    pub custom: BTreeMap<String, u64>,
}
