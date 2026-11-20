// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Invalid memory-store capacity configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MemoryStoreOptionsError {
    /// Payload budget must be positive.
    #[error("maximum payload bytes must be greater than zero")]
    ZeroPayloadBudget,
    /// Unfinished-record limit must be positive.
    #[error("maximum unfinished records must be greater than zero")]
    ZeroUnfinishedRecords,
}
