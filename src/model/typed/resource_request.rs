// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

/// Scheduling quota reserved for the complete task attempt.
///
/// These values constrain concurrent admission accounting; they do not enforce
/// operating-system limits on CPU, memory, or disk use.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResourceRequest {
    /// Number of CPU concurrency slots.
    pub cpu_slots: u32,
    /// Number of distinct GPU devices requested.
    pub gpu_count: u32,
    /// Optional memory quota in bytes.
    pub memory_bytes: Option<u64>,
    /// Optional disk quota in bytes.
    pub disk_bytes: Option<u64>,
    /// Minimum labels required from every assigned GPU.
    pub gpu_labels: Vec<String>,
    /// Named exclusive integer resources such as licenses.
    pub custom: BTreeMap<String, u64>,
}

impl ResourceRequest {
    /// Checks bounded resource labels and names before request encoding.
    pub(crate) fn validate_limits(&self) -> Result<(), &'static str> {
        const MAX_ENTRIES: usize = 32;
        const MAX_NAME_BYTES: usize = 128;
        const MAX_DESCRIPTION_BYTES: usize = 8192;

        if self.gpu_labels.len() > MAX_ENTRIES {
            return Err("GPU labels exceed the 32-entry limit");
        }
        let mut total = 0_usize;
        let mut labels = BTreeSet::new();
        for label in &self.gpu_labels {
            if label.is_empty() {
                return Err("GPU labels must not be empty");
            }
            if label.len() > MAX_NAME_BYTES {
                return Err("GPU label exceeds the 128-byte limit");
            }
            if !labels.insert(label) {
                return Err("GPU labels must be unique");
            }
            total = total.saturating_add(label.len());
        }
        if self.gpu_count == 0 && !self.gpu_labels.is_empty() {
            return Err("GPU labels require at least one requested GPU");
        }
        if self.custom.len() > MAX_ENTRIES {
            return Err("custom resources exceed the 32-entry limit");
        }
        for name in self.custom.keys() {
            if name.is_empty() {
                return Err("custom resource names must not be empty");
            }
            if name.len() > MAX_NAME_BYTES {
                return Err("custom resource name exceeds the 128-byte limit");
            }
            total = total.saturating_add(name.len());
        }
        if total > MAX_DESCRIPTION_BYTES {
            return Err("resource descriptions exceed the 8192-byte limit");
        }
        Ok(())
    }
}
