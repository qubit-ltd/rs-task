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
///
/// # Examples
///
/// ```
/// use qubit_task::ResourceRequest;
///
/// let request = ResourceRequest {
///     cpu_slots: 2,
///     gpu_count: 1,
///     gpu_labels: vec![String::from("cuda")],
///     ..ResourceRequest::default()
/// };
///
/// assert_eq!(request.cpu_slots, 2);
/// assert_eq!(request.gpu_labels, ["cuda"]);
/// ```
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
    /// Checks that resource labels and names satisfy encoding limits.
    ///
    /// The validation rejects empty or oversized labels and custom resource
    /// names, duplicate GPU labels, labels without a requested GPU, and totals
    /// that exceed the description byte limit. It does not modify the request.
    ///
    /// # Errors
    ///
    /// Returns a static description when there are more than 32 GPU labels or
    /// custom resources, a label or name is empty or exceeds 128 bytes, GPU
    /// labels are duplicated or requested without a GPU, or all label and name
    /// bytes together exceed 8192.
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
