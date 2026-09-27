// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

/// Stable task family and exact handler version key.
///
/// Both components participate in exact matching; changing either creates a
/// distinct handler registration.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskHandlerDescriptor;
///
/// let descriptor = TaskHandlerDescriptor { task_type: "resize".into(), version: "2".into() };
/// assert_eq!(descriptor.version, "2");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskHandlerDescriptor {
    /// Stable business task family.
    pub task_type: String,
    /// Exact payload interpretation version.
    pub version: String,
}
