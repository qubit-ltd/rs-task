// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
/// A contract failure with a stable check identifier and diagnostic context.
#[derive(Debug, thiserror::Error)]
#[error("store contract {check} failed: {message}")]
pub struct ContractViolation {
    /// Stable identifier of the violated contract.
    pub check: &'static str,
    /// Human-readable diagnostic; backend error strings are not contract
    /// criteria.
    pub message: String,
}
