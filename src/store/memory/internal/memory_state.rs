// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;

use crate::model::TaskId;
use crate::model::TaskRecord;

/// Records and accounting used to enforce bounded volatile-store retention.
pub(in crate::store::memory) struct MemoryState {
    /// Retained task records indexed by stable identifier.
    pub(in crate::store::memory) records: BTreeMap<TaskId, TaskRecord>,
    /// Retained idempotency keys mapped to their task IDs.
    pub(in crate::store::memory) idempotency: HashMap<String, TaskId>,
    /// Terminal task IDs in eviction order.
    pub(in crate::store::memory) terminal_order: VecDeque<TaskId>,
    /// Payload bytes held by all retained records.
    pub(in crate::store::memory) retained_payload_bytes: usize,
    /// Number of retained queued, running, or blocked records.
    pub(in crate::store::memory) unfinished_records: usize,
}

impl MemoryState {
    /// Removes a retained record and updates payload, key, and unfinished
    /// accounting.
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier of the retained record to remove.
    ///
    /// # Returns
    ///
    /// The removed record, or `None` when it was not retained.
    pub(in crate::store::memory) fn remove_record(&mut self, id: TaskId) -> Option<TaskRecord> {
        let record = self.records.remove(&id)?;
        self.retained_payload_bytes -= record.request.payload.len();
        if !record.state.is_terminal() {
            debug_assert!(self.unfinished_records > 0);
            self.unfinished_records -= 1;
        }
        if let Some(key) = &record.request.idempotency_key {
            self.idempotency.remove(key);
        }
        self.terminal_order.retain(|terminal_id| *terminal_id != id);
        Some(record)
    }
}
