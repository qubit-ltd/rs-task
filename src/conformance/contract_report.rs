// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
/// Successfully verified black-box contracts; unsupported checks are never
/// reported as passed.
#[derive(Debug)]
pub struct ContractReport {
    /// Stable check identifiers for the verified contracts.
    pub checks: Vec<&'static str>,
}
