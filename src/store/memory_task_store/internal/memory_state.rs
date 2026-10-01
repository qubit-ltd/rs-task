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
#[cfg(test)]
use crate::model::TaskId;
#[cfg(test)]
use crate::model::TaskRecord;
use crate::model::next::StoredTask;
use crate::model::next::TaskId as EncodedTaskId;

/// Key identifying a terminal record across both memory-store APIs.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::store::memory_task_store) enum TerminalTaskId {
    /// Legacy UUID-backed task.
    #[cfg(test)]
    Legacy(TaskId),
    /// Encoded u64-backed task.
    Encoded(EncodedTaskId),
}

/// Records and accounting used to enforce bounded volatile-store retention.
pub(in crate::store::memory_task_store) struct MemoryState {
    /// Epoch held by the current in-process service owner, if any.
    pub(in crate::store::memory_task_store) owner_epoch: Option<OwnerEpoch>,
    /// Last issued in-process owner epoch.
    pub(in crate::store::memory_task_store) last_owner_epoch: u64,
    /// Retained task records indexed by stable identifier.
    #[cfg(test)]
    pub(in crate::store::memory_task_store) records: BTreeMap<TaskId, TaskRecord>,
    /// Encoded tasks retained by the new typed request path.
    pub(in crate::store::memory_task_store) encoded_tasks: BTreeMap<EncodedTaskId, StoredTask>,
    /// Retained idempotency keys mapped to their task IDs.
    #[cfg(test)]
    pub(in crate::store::memory_task_store) idempotency: HashMap<String, TaskId>,
    /// Encoded idempotency keys mapped to their task IDs.
    pub(in crate::store::memory_task_store) encoded_idempotency: HashMap<String, EncodedTaskId>,
    /// Terminal task IDs in eviction order.
    #[cfg(test)]
    pub(in crate::store::memory_task_store) terminal_order: VecDeque<TaskId>,
    /// Encoded terminal task IDs in eviction order.
    pub(in crate::store::memory_task_store) encoded_terminal_order: VecDeque<EncodedTaskId>,
    /// Legacy test eviction order shared with the typed store path.
    #[cfg(test)]
    pub(in crate::store::memory_task_store) terminal_order_all: VecDeque<TerminalTaskId>,
    /// Payload bytes held by all retained records.
    pub(in crate::store::memory_task_store) retained_payload_bytes: usize,
    /// Number of retained queued, running, or blocked records.
    pub(in crate::store::memory_task_store) unfinished_records: usize,
}

impl MemoryState {
    /// Evicts the oldest terminal task across both record stores.
    #[cfg(test)]
    pub(in crate::store::memory_task_store) fn evict_oldest_terminal(&mut self) -> bool {
        match self.terminal_order_all.pop_front() {
            Some(TerminalTaskId::Legacy(id)) => self.remove_record(id).is_some(),
            Some(TerminalTaskId::Encoded(id)) => self.remove_encoded_task(id).is_some(),
            None => false,
        }
    }

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
        #[cfg(test)]
        self.terminal_order_all
            .retain(|terminal_id| *terminal_id != TerminalTaskId::Encoded(id));
        Some(task)
    }

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
    #[cfg(test)]
    pub(in crate::store::memory_task_store) fn remove_record(&mut self, id: TaskId) -> Option<TaskRecord> {
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
        self.terminal_order_all
            .retain(|terminal_id| *terminal_id != TerminalTaskId::Legacy(id));
        Some(record)
    }
}
