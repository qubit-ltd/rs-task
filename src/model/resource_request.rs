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

use super::RequestValidationError;
use super::RequestValidationField;
use super::RequestValidationRule;

/// Maximum number of labels or custom resource names in one request.
pub const MAX_RESOURCE_NAME_ENTRIES: usize = 32;
/// Maximum UTF-8 byte length of a GPU label or custom resource name.
pub const MAX_RESOURCE_NAME_BYTES: usize = 128;
/// Maximum combined UTF-8 bytes used by GPU labels and custom resource names.
pub const MAX_RESOURCE_DESCRIPTION_BYTES: usize = 8192;

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
    /// Optional memory quota in bytes.
    pub memory_bytes: Option<u64>,
    /// Optional disk quota in bytes.
    pub disk_bytes: Option<u64>,
    /// Minimum labels required from every assigned GPU.
    pub gpu_labels: Vec<String>,
    /// Named exclusive integer resources such as memory or licenses.
    pub custom: BTreeMap<String, u64>,
}

impl ResourceRequest {
    /// Validates the bounded textual resource description.
    ///
    /// # Returns
    ///
    /// Success when all resource labels and names satisfy their constraints.
    ///
    /// # Errors
    ///
    /// Returns a static diagnostic when labels or custom names exceed their
    /// entry, byte, uniqueness, or GPU consistency limits.
    pub fn validate_limits(&self) -> Result<(), RequestValidationError> {
        if self.gpu_labels.len() > MAX_RESOURCE_NAME_ENTRIES {
            return Err(RequestValidationError::new(
                RequestValidationField::GpuLabels,
                RequestValidationRule::MaxEntries,
                Some(MAX_RESOURCE_NAME_ENTRIES),
                "GPU labels exceed the 32-entry limit",
            ));
        }
        let mut total = 0_usize;
        let mut labels = BTreeSet::new();
        for label in &self.gpu_labels {
            if label.is_empty() {
                return Err(RequestValidationError::new(
                    RequestValidationField::GpuLabels,
                    RequestValidationRule::Required,
                    None,
                    "GPU labels must not be empty",
                ));
            }
            if label.len() > MAX_RESOURCE_NAME_BYTES {
                return Err(RequestValidationError::new(
                    RequestValidationField::GpuLabels,
                    RequestValidationRule::MaxBytes,
                    Some(MAX_RESOURCE_NAME_BYTES),
                    "GPU label exceeds the 128-byte limit",
                ));
            }
            if !labels.insert(label) {
                return Err(RequestValidationError::new(
                    RequestValidationField::GpuLabels,
                    RequestValidationRule::Unique,
                    None,
                    "GPU labels must be unique",
                ));
            }
            total = total.saturating_add(label.len());
        }
        if self.gpu_count == 0 && !self.gpu_labels.is_empty() {
            return Err(RequestValidationError::new(
                RequestValidationField::GpuLabels,
                RequestValidationRule::RequiresGpu,
                Some(1),
                "GPU labels require at least one requested GPU",
            ));
        }
        if self.custom.len() > MAX_RESOURCE_NAME_ENTRIES {
            return Err(RequestValidationError::new(
                RequestValidationField::CustomResources,
                RequestValidationRule::MaxEntries,
                Some(MAX_RESOURCE_NAME_ENTRIES),
                "custom resources exceed the 32-entry limit",
            ));
        }
        for name in self.custom.keys() {
            if name.is_empty() {
                return Err(RequestValidationError::new(
                    RequestValidationField::CustomResources,
                    RequestValidationRule::Required,
                    None,
                    "custom resource names must not be empty",
                ));
            }
            if name.len() > MAX_RESOURCE_NAME_BYTES {
                return Err(RequestValidationError::new(
                    RequestValidationField::CustomResources,
                    RequestValidationRule::MaxBytes,
                    Some(MAX_RESOURCE_NAME_BYTES),
                    "custom resource name exceeds the 128-byte limit",
                ));
            }
            total = total.saturating_add(name.len());
        }
        if total > MAX_RESOURCE_DESCRIPTION_BYTES {
            return Err(RequestValidationError::new(
                RequestValidationField::CustomResources,
                RequestValidationRule::MaxBytes,
                Some(MAX_RESOURCE_DESCRIPTION_BYTES),
                "resource descriptions exceed the 8192-byte limit",
            ));
        }
        Ok(())
    }
}
