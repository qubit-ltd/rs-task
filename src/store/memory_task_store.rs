// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Owns the private record and retention accounting types used by this store.
mod internal;

use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::num::NonZeroUsize;

use internal::MemoryState;
use parking_lot::Mutex;

use super::StoreError;
use super::TaskFuture;
use super::TaskStore;
use crate::model::AcceptOutcome;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::RecoveryPage;
use crate::model::StoreCapabilities;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::model::checked_page_size;

/// Default number of nonterminal records retained by a memory store.
pub const DEFAULT_MAX_UNFINISHED_RECORDS: usize = 2_048;

/// Volatile task history with bounded retention for completed and unfinished
/// tasks.
///
/// # Examples
///
/// ```
/// use qubit_task::store::MemoryTaskStore;
/// use qubit_task::store::TaskStore;
///
/// let store = MemoryTaskStore::new(32);
/// assert!(!store.capabilities().persistent_history);
/// ```
pub struct MemoryTaskStore {
    /// Maximum retained terminal record count.
    history_capacity: usize,
    /// Maximum total payload bytes retained by records.
    max_payload_bytes: usize,
    /// Maximum retained nonterminal record count.
    max_unfinished_records: usize,
    /// Records and retention accounting protected by one mutex.
    state: Mutex<MemoryState>,
}

impl MemoryTaskStore {
    /// Creates an in-memory store retaining at most `history_capacity` terminal
    /// records and [`DEFAULT_MAX_UNFINISHED_RECORDS`] nonterminal records.
    ///
    /// # Parameters
    ///
    /// * `history_capacity` - Maximum number of terminal records to retain.
    ///
    /// # Returns
    ///
    /// A volatile store with the default payload and unfinished-record limits.
    #[must_use]
    pub fn new(history_capacity: usize) -> Self {
        Self::with_payload_budget(
            history_capacity,
            NonZeroUsize::new(64 * 1024 * 1024).expect("default budget is nonzero"),
        )
    }

    /// Creates an in-memory store with an explicit payload budget and the
    /// default nonterminal-record limit.
    ///
    /// # Parameters
    ///
    /// * `history_capacity` - Maximum number of terminal records to retain.
    /// * `max_payload_bytes` - Maximum total retained payload bytes.
    ///
    /// # Returns
    ///
    /// A volatile store with the supplied payload budget.
    #[must_use]
    pub fn with_payload_budget(history_capacity: usize, max_payload_bytes: NonZeroUsize) -> Self {
        Self::with_limits(
            history_capacity,
            max_payload_bytes,
            NonZeroUsize::new(DEFAULT_MAX_UNFINISHED_RECORDS).expect("default unfinished record limit is nonzero"),
        )
    }

    /// Creates an in-memory store with explicit history, payload, and
    /// nonterminal-record limits.
    ///
    /// Nonterminal records include queued, running, and blocked tasks. When the
    /// limit is reached, new distinct tasks are rejected until an existing
    /// task becomes terminal. Idempotent replays of retained tasks remain
    /// available.
    ///
    /// # Parameters
    ///
    /// * `history_capacity` - Maximum number of retained terminal records.
    /// * `max_payload_bytes` - Maximum retained bytes across task payloads.
    /// * `max_unfinished_records` - Maximum retained nonterminal records.
    ///
    /// # Returns
    ///
    /// A memory store with the supplied retention limits.
    #[must_use]
    pub fn with_limits(
        history_capacity: usize,
        max_payload_bytes: NonZeroUsize,
        max_unfinished_records: NonZeroUsize,
    ) -> Self {
        Self {
            history_capacity,
            max_payload_bytes: max_payload_bytes.get(),
            max_unfinished_records: max_unfinished_records.get(),
            state: Mutex::new(MemoryState {
                records: BTreeMap::new(),
                idempotency: HashMap::new(),
                terminal_order: VecDeque::new(),
                retained_payload_bytes: 0,
                unfinished_records: 0,
            }),
        }
    }
}

impl TaskStore for MemoryTaskStore {
    /// Reports that this store does not persist records across process
    /// restarts.
    ///
    /// # Returns
    ///
    /// Capabilities indicating volatile history without restart recovery.
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: false,
            restart_recovery: false,
        }
    }

    /// Retains a new task or returns an identical request already retained.
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier assigned to a new request.
    /// * `request` - Task request to validate and retain.
    ///
    /// # Returns
    ///
    /// A future resolving to the accepted or existing record.
    ///
    /// # Errors
    ///
    /// Resolves to validation, duplicate, idempotency, capacity, or retention
    /// limit errors.
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        Box::pin(async move {
            request
                .validate_limits()
                .map_err(|error| StoreError::InvalidRequest(error.message()))?;
            let mut state = self.state.lock();
            if let Some(key) = &request.idempotency_key
                && let Some(existing_id) = state.idempotency.get(key)
            {
                let existing = state.records.get(existing_id).ok_or(StoreError::NotFound)?;
                if existing.request != request {
                    return Err(StoreError::IdempotencyConflict);
                }
                return Ok(AcceptOutcome::Existing(existing.clone()));
            }
            if state.records.contains_key(&id) {
                return Err(StoreError::DuplicateTask);
            }
            if state.unfinished_records >= self.max_unfinished_records {
                return Err(StoreError::UnfinishedRecordLimitExceeded {
                    limit: self.max_unfinished_records,
                });
            }
            let requested_bytes = request.payload.len();
            let reclaimable_bytes = state
                .terminal_order
                .iter()
                .filter_map(|terminal_id| {
                    state
                        .records
                        .get(terminal_id)
                        .map(|record| record.request.payload.len())
                })
                .sum::<usize>();
            let minimum_retained = state.retained_payload_bytes.saturating_sub(reclaimable_bytes);
            if minimum_retained
                .checked_add(requested_bytes)
                .is_none_or(|total| total > self.max_payload_bytes)
            {
                let available_bytes = self.max_payload_bytes.saturating_sub(minimum_retained);
                return Err(StoreError::CapacityExceeded {
                    requested_bytes,
                    available_bytes,
                });
            }
            while state.retained_payload_bytes > self.max_payload_bytes - requested_bytes {
                let oldest = state
                    .terminal_order
                    .front()
                    .copied()
                    .ok_or(StoreError::CapacityExceeded {
                        requested_bytes,
                        available_bytes: self.max_payload_bytes.saturating_sub(state.retained_payload_bytes),
                    })?;
                state.remove_record(oldest);
            }
            let now = now_ms();
            let record = TaskRecord {
                id,
                request: request.clone(),
                state: TaskState::Queued,
                state_version: 0,
                attempt: 0,
                retry_not_before_ms: None,
                accepted_at_ms: now,
                started_at_ms: None,
                finished_at_ms: None,
                assigned_resources: Vec::new(),
                output: None,
                cancel_requested: false,
            };
            if let Some(key) = &request.idempotency_key {
                state.idempotency.insert(key.clone(), id);
            }
            state.retained_payload_bytes += requested_bytes;
            state.records.insert(id, record.clone());
            state.unfinished_records += 1;
            Ok(AcceptOutcome::Accepted(record))
        })
    }

    /// Applies a version-checked lifecycle transition and evicts old terminal
    /// records when required by retention limits.
    ///
    /// # Parameters
    ///
    /// * `command` - Desired state and expected version and attempt.
    ///
    /// # Returns
    ///
    /// A future resolving to the updated task summary.
    ///
    /// # Errors
    ///
    /// Resolves to an error when the record is missing, the revision conflicts,
    /// the transition is invalid, or diagnostics are invalid.
    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            command
                .state
                .validate_diagnostics()
                .map_err(StoreError::InvalidRequest)?;
            if command
                .output
                .as_ref()
                .is_some_and(|output| output.summary.len() > MAX_TASK_OUTPUT_SUMMARY_BYTES)
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
            if command.retry_not_before_ms.is_some() && !matches!(command.state, TaskState::Queued) {
                return Err(StoreError::InvalidRequest(
                    "only queued tasks may have a retry deadline",
                ));
            }
            let was_unfinished = !record.state.is_terminal();
            let becomes_terminal = command.state.is_terminal();
            let starting = !matches!(record.state, TaskState::Running) && matches!(command.state, TaskState::Running);
            record.state = command.state;
            record.retry_not_before_ms = command.retry_not_before_ms;
            record.state_version += 1;
            if starting {
                record.attempt += 1;
                record.started_at_ms = Some(now_ms());
            }
            record.cancel_requested = command.cancel_requested;
            record.assigned_resources = command.assigned_resources;
            record.output = command.output;
            if becomes_terminal {
                record.finished_at_ms = Some(now_ms());
                let updated = record.summary();
                if was_unfinished {
                    debug_assert!(state.unfinished_records > 0);
                    state.unfinished_records -= 1;
                }
                state.terminal_order.push_back(command.id);
                while state.terminal_order.len() > self.history_capacity {
                    if let Some(oldest) = state.terminal_order.front().copied() {
                        state.remove_record(oldest);
                    }
                }
                return Ok(updated);
            }
            state
                .records
                .get(&command.id)
                .map(TaskRecord::summary)
                .ok_or(StoreError::NotFound)
        })
    }

    /// Looks up a retained task by idempotency key.
    ///
    /// # Parameters
    ///
    /// * `key` - Caller-supplied idempotency key.
    ///
    /// # Returns
    ///
    /// A future resolving to the matching record, if retained.
    ///
    /// # Errors
    ///
    /// Resolves to a store error if the lookup cannot complete.
    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            match state.idempotency.get(key) {
                Some(id) => Ok(state.records.get(id).cloned()),
                None => Ok(None),
            }
        })
    }

    /// Loads the payload-free summary retained for an exact idempotency key.
    ///
    /// # Parameters
    ///
    /// * `key` - Caller-supplied idempotency key to look up.
    ///
    /// # Returns
    ///
    /// A future resolving to the matching summary, or `None` when absent.
    ///
    /// # Errors
    ///
    /// Resolves to a store error if the lookup cannot complete.
    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            Ok(state
                .idempotency
                .get(key)
                .and_then(|id| state.records.get(id))
                .map(TaskRecord::summary))
        })
    }

    /// Loads a retained task including its request payload.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identifier.
    ///
    /// # Returns
    ///
    /// A future resolving to the matching record, if retained.
    ///
    /// # Errors
    ///
    /// Resolves to a store error if the lookup cannot complete.
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move { Ok(self.state.lock().records.get(&id).cloned()) })
    }

    /// Loads lifecycle and request metadata without copying the payload.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identifier.
    ///
    /// # Returns
    ///
    /// A future resolving to the summary, if retained.
    ///
    /// # Errors
    ///
    /// Resolves to a store error if the lookup cannot complete.
    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        Box::pin(async move { Ok(self.state.lock().records.get(&id).map(TaskRecord::summary)) })
    }

    /// Cancels a blocked task only if its revision has not changed.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identifier.
    /// * `expected_version` - State version observed by the caller.
    ///
    /// # Returns
    ///
    /// A future resolving to the cancelled task summary.
    ///
    /// # Errors
    ///
    /// Resolves to `NotFound`, `Conflict`, or `InvalidTransition` when the
    /// task is unavailable or no longer blocked at that revision.
    fn abandon_blocked<'a>(
        &'a self,
        id: TaskId,
        expected_version: u64,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            let record = state.records.get_mut(&id).ok_or(StoreError::NotFound)?;
            if record.state_version != expected_version {
                return Err(StoreError::Conflict);
            }
            if !matches!(record.state, TaskState::Blocked { .. }) {
                return Err(StoreError::InvalidTransition);
            }
            record.state = TaskState::Cancelled;
            record.state_version += 1;
            record.retry_not_before_ms = None;
            record.finished_at_ms = Some(now_ms());
            record.cancel_requested = false;
            record.assigned_resources.clear();
            let summary = record.summary();
            debug_assert!(state.unfinished_records > 0);
            state.unfinished_records -= 1;
            state.terminal_order.push_back(id);
            while state.terminal_order.len() > self.history_capacity {
                if let Some(oldest) = state.terminal_order.front().copied() {
                    state.remove_record(oldest);
                }
            }
            Ok(summary)
        })
    }

    /// Lists retained summaries in acceptance order using a bounded cursor
    /// page.
    ///
    /// # Parameters
    ///
    /// * `query` - State, correlation, cursor, and page-size filters.
    ///
    /// # Returns
    ///
    /// A future resolving to matching summaries and a continuation cursor.
    ///
    /// # Errors
    ///
    /// Resolves to `InvalidRequest` when the page limit is invalid.
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        Box::pin(async move {
            let page_size = checked_page_size(query.limit)?;
            let fetch_limit = page_size
                .checked_add(1)
                .ok_or(StoreError::InvalidRequest("task history page limit is too large"))?;
            let state = self.state.lock();
            let mut candidates = BinaryHeap::with_capacity(fetch_limit);
            for record in state.records.values().filter(|record| {
                (query.states.is_empty() || query.states.contains(&record.state.kind()))
                    && query
                        .correlation_key
                        .as_ref()
                        .is_none_or(|key| record.request.correlation_key.as_ref() == Some(key))
                    && query
                        .after
                        .is_none_or(|after| (record.accepted_at_ms, record.id) > (after.accepted_at_ms, after.id))
            }) {
                let key = (record.accepted_at_ms, record.id);
                if candidates.len() < fetch_limit {
                    candidates.push(key);
                } else if candidates.peek().is_some_and(|largest| key < *largest) {
                    candidates.pop();
                    candidates.push(key);
                }
            }
            let mut candidates = candidates.into_vec();
            candidates.sort_unstable();
            let has_more = candidates.len() > page_size;
            if has_more {
                candidates.truncate(page_size);
            }
            let next = has_more
                .then(|| {
                    candidates.last().map(|(_, id)| {
                        TaskCursor::new(
                            state
                                .records
                                .get(id)
                                .expect("page candidate remains retained")
                                .accepted_at_ms,
                            *id,
                        )
                    })
                })
                .flatten();
            let records = candidates
                .into_iter()
                .map(|(_, id)| {
                    state
                        .records
                        .get(&id)
                        .expect("page candidate remains retained")
                        .summary()
                })
                .collect::<Vec<_>>();
            Ok(TaskPage { records, next })
        })
    }

    /// Counts each state among currently retained records.
    ///
    /// # Returns
    ///
    /// A future resolving to counts from one locked snapshot.
    ///
    /// # Errors
    ///
    /// Resolves to a store error if the count cannot be read.
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

    /// Deletes at most the requested number of old terminal records.
    ///
    /// # Parameters
    ///
    /// * `accepted_before_ms` - Exclusive acceptance-time cutoff.
    /// * `max_rows` - Maximum terminal records to remove.
    ///
    /// # Returns
    ///
    /// A future resolving to the number of removed records.
    ///
    /// # Errors
    ///
    /// Resolves to a store error if pruning cannot complete.
    fn prune_terminal_before<'a>(
        &'a self,
        accepted_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            let mut candidates = state
                .records
                .values()
                .filter(|record| record.state.is_terminal() && record.accepted_at_ms < accepted_before_ms)
                .collect::<Vec<_>>();
            candidates.sort_by_key(|record| (record.accepted_at_ms, record.id));
            let expired = candidates
                .into_iter()
                .take(max_rows.get())
                .map(|record| record.id)
                .collect::<Vec<_>>();
            for id in &expired {
                state.remove_record(*id);
            }
            Ok(expired.len())
        })
    }

    /// Reports that this volatile store cannot provide exclusive recovery
    /// ownership.
    ///
    /// # Returns
    ///
    /// A future resolving to `UnsupportedCapability`.
    ///
    /// # Errors
    ///
    /// Resolves to `UnsupportedCapability` because this volatile store has no
    /// persistent ownership lock.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Checks whether queued and running records strictly exceed a limit.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum unfinished records allowed by the caller.
    ///
    /// # Returns
    ///
    /// A future resolving to whether the count exceeds `limit`.
    ///
    /// # Errors
    ///
    /// Resolves to a store error if the count cannot be read.
    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            let mut count = 0_usize;
            for record in state.records.values() {
                if matches!(record.state, TaskState::Queued | TaskState::Running) {
                    count = count.saturating_add(1);
                    if count > limit {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })
    }

    /// Reports that this volatile store cannot scan persistent recovery rows.
    ///
    /// # Parameters
    ///
    /// * `_cursor` - Ignored because recovery scanning is unsupported.
    ///
    /// # Returns
    ///
    /// A future resolving to `UnsupportedCapability`.
    ///
    /// # Errors
    ///
    /// Resolves to `UnsupportedCapability` because this store retains no
    /// restart-recovery rows.
    fn scan_unfinished<'a>(&'a self, _cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Reports that no ownership lock is held by this volatile store.
    ///
    /// # Parameters
    ///
    /// * `_epoch` - Ignored because ownership acquisition is unsupported.
    ///
    /// # Returns
    ///
    /// A future resolving to `UnsupportedCapability`.
    ///
    /// # Errors
    ///
    /// Resolves to `UnsupportedCapability` because ownership is unsupported.
    fn release_owner<'a>(&'a self, _epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }
}

/// Reads the current Unix epoch time in milliseconds, defaulting on clock
/// error.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the system clock predates the epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::MemoryTaskStore;
    use crate::model::AcceptOutcome;
    use crate::model::TaskId;
    use crate::model::TaskQuery;
    use crate::model::TaskRequest;
    use crate::store::TaskStore;

    #[tokio::test]
    async fn test_same_millisecond_pages_use_task_id_as_tie_breaker() {
        let store = MemoryTaskStore::new(8);
        let first_id = TaskId::generate();
        let second_id = TaskId::generate();
        for id in [first_id, second_id] {
            let AcceptOutcome::Accepted(_) = store
                .accept(id, TaskRequest::new("cursor-test", "1", Vec::new()))
                .await
                .expect("task is accepted")
            else {
                panic!("each generated identifier is new")
            };
        }
        {
            let mut state = store.state.lock();
            for record in state.records.values_mut() {
                record.accepted_at_ms = 42;
            }
        }
        let first = store
            .list(TaskQuery {
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .expect("first page succeeds");
        assert_eq!(first.records[0].id, first_id.min(second_id));
        let second = store
            .list(TaskQuery {
                after: first.next,
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .expect("second page succeeds");
        assert_eq!(second.records[0].id, first_id.max(second_id));
    }
}
