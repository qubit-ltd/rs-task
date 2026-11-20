// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use super::memory_store_options_error::MemoryStoreOptionsError;
use super::memory_task_store::DEFAULT_MAX_UNFINISHED_RECORDS;

/// Named retention and payload limits for [`super::MemoryTaskStore`].
///
/// # Examples
///
/// ```
/// use qubit_task::store::MemoryStoreOptions;
/// use qubit_task::store::MemoryTaskStore;
///
/// let options = MemoryStoreOptions::new(32)
///     .max_payload_bytes(1024 * 1024)?
///     .max_unfinished_records(128)?;
/// let store = MemoryTaskStore::with_options(options);
/// # let _ = store;
/// # Ok::<(), qubit_task::store::MemoryStoreOptionsError>(())
/// ```
#[derive(Debug, Clone, Copy)]
pub struct MemoryStoreOptions {
    /// Maximum number of completed task records retained in memory.
    pub(crate) history_capacity: usize,
    /// Maximum combined payload size retained by the store, in bytes.
    pub(crate) max_payload_bytes: NonZeroUsize,
    /// Maximum number of nonterminal task records retained by the store.
    pub(crate) max_unfinished_records: NonZeroUsize,
}

impl MemoryStoreOptions {
    /// Creates options with default payload and unfinished-record budgets.
    ///
    /// # Parameters
    ///
    /// * `history_capacity` - Maximum number of terminal records to retain.
    ///
    /// # Returns
    ///
    /// Options with the supplied history capacity and default resource limits.
    #[must_use]
    pub fn new(history_capacity: usize) -> Self {
        Self {
            history_capacity,
            max_payload_bytes: NonZeroUsize::new(64 * 1024 * 1024).expect("default payload budget is positive"),
            max_unfinished_records: NonZeroUsize::new(DEFAULT_MAX_UNFINISHED_RECORDS)
                .expect("default limit is positive"),
        }
    }

    /// Sets the maximum combined payload size retained by the store.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Positive payload budget, measured in bytes.
    ///
    /// # Returns
    ///
    /// Updated options when the budget is positive.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryStoreOptionsError::ZeroPayloadBudget`] when `bytes` is
    /// zero.
    pub fn max_payload_bytes(mut self, bytes: usize) -> Result<Self, MemoryStoreOptionsError> {
        self.max_payload_bytes = NonZeroUsize::new(bytes).ok_or(MemoryStoreOptionsError::ZeroPayloadBudget)?;
        Ok(self)
    }

    /// Sets the maximum number of nonterminal task records retained by the store.
    ///
    /// # Parameters
    ///
    /// * `limit` - Positive maximum number of unfinished records.
    ///
    /// # Returns
    ///
    /// Updated options when the limit is positive.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryStoreOptionsError::ZeroUnfinishedRecords`] when `limit`
    /// is zero.
    pub fn max_unfinished_records(mut self, limit: usize) -> Result<Self, MemoryStoreOptionsError> {
        self.max_unfinished_records = NonZeroUsize::new(limit).ok_or(MemoryStoreOptionsError::ZeroUnfinishedRecords)?;
        Ok(self)
    }
}
