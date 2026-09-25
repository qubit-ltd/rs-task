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

use parking_lot::Mutex;

use super::StoreError;
use super::TaskFuture;
use super::TaskStore;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::StoredTaskPage;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TransitionCommand;

struct MemoryState {
    records: BTreeMap<TaskId, TaskRecord>,
    idempotency: HashMap<String, (TaskRequest, TaskId)>,
    terminal_order: VecDeque<TaskId>,
}

/// Volatile task history with bounded retention for completed tasks.
pub struct MemoryTaskStore {
    history_capacity: usize,
    state: Mutex<MemoryState>,
}

impl MemoryTaskStore {
    /// Creates an in-memory store retaining at most `history_capacity` terminal
    /// records.
    #[must_use]
    pub fn new(history_capacity: usize) -> Self {
        Self {
            history_capacity,
            state: Mutex::new(MemoryState {
                records: BTreeMap::new(),
                idempotency: HashMap::new(),
                terminal_order: VecDeque::new(),
            }),
        }
    }
}

impl TaskStore for MemoryTaskStore {
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: false,
            restart_recovery: false,
        }
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        Box::pin(async move {
            request.validate_limits().map_err(StoreError::InvalidRequest)?;
            let mut state = self.state.lock();
            if let Some(key) = &request.idempotency_key
                && let Some((existing_request, existing_id)) = state.idempotency.get(key)
            {
                if existing_request != &request {
                    return Err(StoreError::IdempotencyConflict);
                }
                return state
                    .records
                    .get(existing_id)
                    .cloned()
                    .map(AcceptOutcome::Existing)
                    .ok_or(StoreError::NotFound);
            }
            if state.records.contains_key(&id) {
                return Err(StoreError::DuplicateTask);
            }
            let now = now_ms();
            let record = TaskRecord {
                id,
                request: request.clone(),
                state: TaskState::Queued,
                state_version: 0,
                attempt: 0,
                accepted_at_ms: now,
                started_at_ms: None,
                finished_at_ms: None,
                assigned_resources: Vec::new(),
                output: None,
                cancel_requested: false,
            };
            if let Some(key) = &request.idempotency_key {
                state.idempotency.insert(key.clone(), (request, id));
            }
            state.records.insert(id, record.clone());
            Ok(AcceptOutcome::Accepted(record))
        })
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskRecord, StoreError>> {
        Box::pin(async move {
            command
                .state
                .validate_diagnostics()
                .map_err(StoreError::InvalidRequest)?;
            if command
                .output
                .as_ref()
                .is_some_and(|output| output.summary.len() > crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES)
            {
                return Err(StoreError::InvalidRequest(
                    "task output summary exceeds the 65536-byte limit",
                ));
            }
            let mut state = self.state.lock();
            let record = state.records.get_mut(&command.id).ok_or(StoreError::NotFound)?;
            if record.state_version != command.expected_version || record.attempt != command.expected_attempt {
                return Err(StoreError::Conflict);
            }
            if !record.state.allows_transition_to(&command.state) {
                return Err(StoreError::InvalidTransition);
            }
            let starting = !matches!(record.state, TaskState::Running) && matches!(command.state, TaskState::Running);
            record.state = command.state;
            record.state_version += 1;
            if starting {
                record.attempt += 1;
                record.started_at_ms = Some(now_ms());
            }
            record.cancel_requested = command.cancel_requested;
            record.assigned_resources = command.assigned_resources;
            record.output = command.output;
            if record.state.is_terminal() {
                record.finished_at_ms = Some(now_ms());
                let updated = record.clone();
                state.terminal_order.push_back(command.id);
                while state.terminal_order.len() > self.history_capacity {
                    if let Some(oldest) = state.terminal_order.pop_front()
                        && let Some(removed) = state.records.remove(&oldest)
                        && let Some(key) = removed.request.idempotency_key
                    {
                        state.idempotency.remove(&key);
                    }
                }
                return Ok(updated);
            }
            state.records.get(&command.id).cloned().ok_or(StoreError::NotFound)
        })
    }

    fn find_idempotent<'a>(&'a self, request: TaskRequest) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move {
            let Some(key) = request.idempotency_key.as_ref() else {
                return Ok(None);
            };
            let state = self.state.lock();
            match state.idempotency.get(key) {
                Some((existing, id)) if existing == &request => Ok(state.records.get(id).cloned()),
                Some(_) => Err(StoreError::IdempotencyConflict),
                None => Ok(None),
            }
        })
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move { Ok(self.state.lock().records.get(&id).cloned()) })
    }

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            let mut records = state
                .records
                .values()
                .filter(|record| {
                    (query.states.is_empty() || query.states.contains(&record.state.kind()))
                        && query
                            .correlation_key
                            .as_ref()
                            .is_none_or(|key| record.request.correlation_key.as_ref() == Some(key))
                        && query.after.is_none_or(|after| record.id > after)
                })
                .take(query.limit.max(1) + 1)
                .cloned()
                .collect::<Vec<_>>();
            let has_more = records.len() > query.limit.max(1);
            if has_more {
                records.truncate(query.limit.max(1));
            }
            let next = has_more.then(|| records.last().map(|record| record.id)).flatten();
            Ok(TaskPage { records, next })
        })
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            let mut counts = TaskStateCounts::default();
            for record in state.records.values() {
                match record.state {
                    TaskState::Queued => counts.queued += 1,
                    TaskState::Running => counts.running += 1,
                    TaskState::Blocked { .. } => counts.blocked += 1,
                    TaskState::Succeeded
                    | TaskState::Failed { .. }
                    | TaskState::Panicked { .. }
                    | TaskState::Cancelled => {
                        counts.terminal += 1;
                    }
                }
            }
            Ok(counts)
        })
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    fn scan_unfinished<'a>(&'a self, _cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    fn release_owner<'a>(&'a self, _epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
