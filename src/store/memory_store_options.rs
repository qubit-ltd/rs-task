// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::num::NonZeroUsize;

use thiserror::Error;

use super::memory_task_store::DEFAULT_MAX_UNFINISHED_RECORDS;

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

/// Named retention and payload limits for [`super::MemoryTaskStore`].
#[derive(Debug, Clone, Copy)]
pub struct MemoryStoreOptions {
    pub(crate) history_capacity: usize,
    pub(crate) max_payload_bytes: NonZeroUsize,
    pub(crate) max_unfinished_records: NonZeroUsize,
}

impl MemoryStoreOptions {
    /// Creates options with default payload and unfinished-record budgets.
    #[must_use]
    pub fn new(history_capacity: usize) -> Self {
        Self {
            history_capacity,
            max_payload_bytes: NonZeroUsize::new(64 * 1024 * 1024).expect("default payload budget is positive"),
            max_unfinished_records: NonZeroUsize::new(DEFAULT_MAX_UNFINISHED_RECORDS)
                .expect("default limit is positive"),
        }
    }

    /// Sets the total payload budget.
    pub fn max_payload_bytes(mut self, bytes: usize) -> Result<Self, MemoryStoreOptionsError> {
        self.max_payload_bytes = NonZeroUsize::new(bytes).ok_or(MemoryStoreOptionsError::ZeroPayloadBudget)?;
        Ok(self)
    }

    /// Sets the maximum number of retained nonterminal tasks.
    pub fn max_unfinished_records(mut self, limit: usize) -> Result<Self, MemoryStoreOptionsError> {
        self.max_unfinished_records = NonZeroUsize::new(limit).ok_or(MemoryStoreOptionsError::ZeroUnfinishedRecords)?;
        Ok(self)
    }
}
