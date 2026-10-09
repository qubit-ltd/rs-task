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

use crate::model::OwnerEpoch;
use crate::model::typed::StoredTask;
use crate::model::typed::TaskId as EncodedTaskId;

/// Records and accounting used to enforce bounded volatile-store retention.
pub(in crate::store::memory_task_store) struct MemoryState {
    /// Epoch held by the current in-process service owner, if any.
    pub(in crate::store::memory_task_store) owner_epoch: Option<OwnerEpoch>,
    /// Last issued in-process owner epoch.
    pub(in crate::store::memory_task_store) last_owner_epoch: u64,
    /// Encoded tasks retained by the new typed request path.
    pub(in crate::store::memory_task_store) encoded_tasks: BTreeMap<EncodedTaskId, StoredTask>,
    /// Encoded idempotency keys mapped to their task IDs.
    pub(in crate::store::memory_task_store) encoded_idempotency: HashMap<String, EncodedTaskId>,
    /// Encoded terminal task IDs in eviction order.
    pub(in crate::store::memory_task_store) encoded_terminal_order: VecDeque<EncodedTaskId>,
    /// Payload bytes held by all retained records.
    pub(in crate::store::memory_task_store) retained_payload_bytes: usize,
    /// Number of retained queued, running, or blocked records.
    pub(in crate::store::memory_task_store) unfinished_records: usize,
}

impl MemoryState {
    /// Evicts the oldest typed terminal task when payload capacity is needed.
    pub(in crate::store::memory_task_store) fn evict_oldest_encoded_terminal(&mut self) -> bool {
        match self.encoded_terminal_order.front().copied() {
            Some(id) => self.remove_encoded_task(id).is_some(),
            None => false,
        }
    }

    /// Removes an encoded record and updates shared retention accounting.
    pub(in crate::store::memory_task_store) fn remove_encoded_task(&mut self, id: EncodedTaskId) -> Option<StoredTask> {
        let task = self.encoded_tasks.remove(&id)?;
        self.retained_payload_bytes -= task.request.payload.bytes.len();
        if !task.summary.state.is_terminal() {
            debug_assert!(self.unfinished_records > 0);
            self.unfinished_records -= 1;
        }
        if let Some(key) = &task.request.idempotency_key {
            self.encoded_idempotency.remove(key);
        }
        self.encoded_terminal_order.retain(|terminal_id| *terminal_id != id);
        Some(task)
    }
}
