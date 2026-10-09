// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

/// Serializable stage projection from `rs-progress`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressStageSnapshot {
    /// Machine-readable stage identifier.
    pub id: String,
    /// Human-readable stage name.
    pub name: String,
    /// One-based stage position, when supplied.
    pub position: Option<u64>,
    /// Total number of stages, when supplied.
    pub total: Option<u64>,
}
