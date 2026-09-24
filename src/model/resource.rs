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

/// Resource limits available to this service instance.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResourceCapacity {
    /// CPU concurrency slots available to tasks.
    pub cpu_slots: u32,
    /// GPU device identifiers and their labels.
    pub gpus: BTreeMap<String, Vec<String>>,
    /// Named integer resource limits.
    pub custom: BTreeMap<String, u64>,
}

/// Current resource usage exposed for health and metrics reporting.
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
