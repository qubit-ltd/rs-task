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
#[cfg(test)]
use internal::TerminalTaskId;
use parking_lot::Mutex;

use super::LegacyTaskStore;
use super::StoreError;
use super::TaskFuture;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::TaskState;
#[cfg(test)]
use crate::model::legacy::AcceptOutcome;
#[cfg(test)]
use crate::model::legacy::RecoveryPage;
#[cfg(test)]
use crate::model::legacy::TaskCursor;
#[cfg(test)]
use crate::model::legacy::TaskId;
#[cfg(test)]
use crate::model::legacy::TaskPage;
#[cfg(test)]
use crate::model::legacy::TaskQuery;
#[cfg(test)]
use crate::model::legacy::TaskRecord;
#[cfg(test)]
use crate::model::legacy::TaskRequest;
#[cfg(test)]
#[cfg(test)]
use crate::model::legacy::TaskSummary;
#[cfg(test)]
use crate::model::legacy::TransitionCommand;
#[cfg(test)]
use crate::model::legacy::checked_page_size;
use crate::model::next::AcceptOutcome as EncodedAcceptOutcome;
use crate::model::next::ProgressCommand;
use crate::model::next::StartCommand;
use crate::model::next::StoredTask;
use crate::model::next::StoredTaskRequest;
use crate::model::next::TaskCursor as EncodedTaskCursor;
use crate::model::next::TaskId as EncodedTaskId;
use crate::model::next::TaskPage as EncodedTaskPage;
use crate::model::next::TaskProgressSnapshot;
use crate::model::next::TaskQuery as EncodedTaskQuery;

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
            NonZeroUsize::new(DEFAULT_MAX_UNFINISHED_RECORDS)
                .expect("default unfinished record limit is nonzero"),
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
                owner_epoch: None,
                last_owner_epoch: 0,
                #[cfg(test)]
                records: BTreeMap::new(),
                encoded_tasks: BTreeMap::new(),
                #[cfg(test)]
                idempotency: HashMap::new(),
                encoded_idempotency: HashMap::new(),
                #[cfg(test)]
                terminal_order: VecDeque::new(),
                encoded_terminal_order: VecDeque::new(),
                #[cfg(test)]
                terminal_order_all: VecDeque::new(),
                retained_payload_bytes: 0,
                unfinished_records: 0,
            }),
        }
    }
}

impl LegacyTaskStore for MemoryTaskStore {
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
    #[cfg(test)]
    fn accept<'a>(
        &'a self,
        id: TaskId,
        request: TaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
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
            if request
                .idempotency_key
                .as_ref()
                .is_some_and(|key| state.encoded_idempotency.contains_key(key))
            {
                return Err(StoreError::IdempotencyConflict);
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
                .sum::<usize>()
                + state
                    .encoded_terminal_order
                    .iter()
                    .filter_map(|terminal_id| state.encoded_tasks.get(terminal_id))
                    .map(|task| task.request.payload.bytes.len())
                    .sum::<usize>();
            let minimum_retained = state
                .retained_payload_bytes
                .saturating_sub(reclaimable_bytes);
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
                if !state.evict_oldest_terminal() {
                    return Err(StoreError::CapacityExceeded {
                        requested_bytes,
                        available_bytes: self
                            .max_payload_bytes
                            .saturating_sub(state.retained_payload_bytes),
                    });
                }
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

    /// Atomically retains a typed request after its payload has been encoded.
    fn accept_encoded<'a>(
        &'a self,
        id: EncodedTaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<EncodedAcceptOutcome, StoreError>> {
        Box::pin(async move {
            if request.kind_id.is_empty() || request.payload.codec_id.is_empty() {
                return Err(StoreError::InvalidRequest(
                    "task kind and payload codec must not be empty",
                ));
            }
            let mut state = self.state.lock();
            if let Some(key) = &request.idempotency_key
                && let Some(existing_id) = state.encoded_idempotency.get(key)
            {
                let existing = state
                    .encoded_tasks
                    .get(existing_id)
                    .ok_or(StoreError::NotFound)?;
                if existing.request != request {
                    return Err(StoreError::IdempotencyConflict);
                }
                return Ok(EncodedAcceptOutcome {
                    summary: existing.summary.clone(),
                    created: false,
                });
            }
            if state.encoded_tasks.contains_key(&id) {
                return Err(StoreError::DuplicateTask);
            }
            if state.unfinished_records >= self.max_unfinished_records {
                return Err(StoreError::UnfinishedRecordLimitExceeded {
                    limit: self.max_unfinished_records,
                });
            }
            let requested_bytes = request.payload.bytes.len();
            let reclaimable_bytes = state
                .encoded_terminal_order
                .iter()
                .filter_map(|terminal_id| state.encoded_tasks.get(terminal_id))
                .map(|task| task.request.payload.bytes.len())
                .sum::<usize>();
            let minimum_retained = state
                .retained_payload_bytes
                .saturating_sub(reclaimable_bytes);
            if minimum_retained
                .checked_add(requested_bytes)
                .is_none_or(|total| total > self.max_payload_bytes)
            {
                return Err(StoreError::CapacityExceeded {
                    requested_bytes,
                    available_bytes: self.max_payload_bytes.saturating_sub(minimum_retained),
                });
            }
            while state.retained_payload_bytes > self.max_payload_bytes - requested_bytes {
                if !state.evict_oldest_encoded_terminal() {
                    return Err(StoreError::CapacityExceeded {
                        requested_bytes,
                        available_bytes: self
                            .max_payload_bytes
                            .saturating_sub(state.retained_payload_bytes),
                    });
                }
            }
            let now = now_ms();
            let summary = crate::model::next::TaskSummary {
                id,
                kind_id: request.kind_id.clone(),
                category: request.category.clone(),
                payload_type_id: request.payload.type_id.to_string(),
                payload_schema_version: request.payload.schema_version,
                payload_codec_id: request.payload.codec_id.clone(),
                metadata: request.metadata.clone(),
                resource_limit: request.resource_limit.clone(),
                correlation_key: request.correlation_key.clone(),
                idempotency_key: request.idempotency_key.clone(),
                state: TaskState::Queued,
                cancel_requested: false,
                cancel_error: None,
                state_version: 0,
                attempt: 0,
                retry_not_before_ms: None,
                accepted_at_ms: now,
                started_at_ms: None,
                finished_at_ms: None,
                progress: None,
                output: None,
            };
            if let Some(key) = &request.idempotency_key {
                state.encoded_idempotency.insert(key.clone(), id);
            }
            state.retained_payload_bytes += requested_bytes;
            state.encoded_tasks.insert(
                id,
                StoredTask {
                    request,
                    summary: summary.clone(),
                },
            );
            state.unfinished_records += 1;
            Ok(EncodedAcceptOutcome {
                summary,
                created: true,
            })
        })
    }

    /// Loads one encoded request together with its current summary.
    fn get_encoded_task<'a>(
        &'a self,
        id: EncodedTaskId,
    ) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        Box::pin(async move { Ok(self.state.lock().encoded_tasks.get(&id).cloned()) })
    }

    /// Starts a queued encoded task attempt using a lifecycle revision CAS.
    fn start_encoded<'a>(
        &'a self,
        command: StartCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            let task = state
                .encoded_tasks
                .get_mut(&command.id)
                .ok_or(StoreError::NotFound)?;
            if task.summary.state_version != command.expected_state_version
                || !matches!(task.summary.state, TaskState::Queued)
            {
                return Err(StoreError::Conflict);
            }
            task.summary.state_version =
                task.summary
                    .state_version
                    .checked_add(1)
                    .ok_or(StoreError::Failure(
                        "task state version overflow".to_owned(),
                    ))?;
            task.summary.attempt =
                task.summary
                    .attempt
                    .checked_add(1)
                    .ok_or(StoreError::Failure(
                        "task attempt counter overflow".to_owned(),
                    ))?;
            task.summary.state = TaskState::Running;
            task.summary.retry_not_before_ms = None;
            task.summary.started_at_ms = Some(command.started_at_ms);
            task.summary.finished_at_ms = None;
            task.summary.progress = None;
            Ok(task.summary.clone())
        })
    }

    /// Applies a typed lifecycle transition with revision and attempt checks.
    fn transition_encoded<'a>(
        &'a self,
        command: crate::model::next::TransitionCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        Box::pin(async move {
            command
                .state
                .validate_diagnostics()
                .map_err(StoreError::InvalidRequest)?;
            validate_encoded_output(&command.state, command.output.as_ref())?;
            let mut state = self.state.lock();
            let (was_terminal, is_terminal, summary) = {
                let task = state
                    .encoded_tasks
                    .get_mut(&command.id)
                    .ok_or(StoreError::NotFound)?;
                let terminal_cancel_annotation = task.summary.state.is_terminal()
                    && task.summary.state == command.state
                    && command.cancel_requested
                    && command.cancel_error.is_some();
                if task.summary.state_version != command.expected_state_version
                    || task.summary.attempt != command.expected_attempt
                    || (!task.summary.state.allows_transition_to(&command.state)
                        && !terminal_cancel_annotation)
                {
                    return Err(StoreError::Conflict);
                }
                if command.output.is_some() && task.summary.state.is_terminal() {
                    return Err(StoreError::InvalidRequest(
                        "task output can only be written before the task becomes terminal",
                    ));
                }
                if command.retry_not_before_ms.is_some() && command.state != TaskState::Queued {
                    return Err(StoreError::InvalidRequest(
                        "retry deadline is only valid for queued tasks",
                    ));
                }
                let was_terminal = task.summary.state.is_terminal();
                let is_terminal = command.state.is_terminal();
                task.summary.state = command.state;
                task.summary.retry_not_before_ms = command.retry_not_before_ms;
                task.summary.state_version =
                    task.summary
                        .state_version
                        .checked_add(1)
                        .ok_or(StoreError::Failure(
                            "task state version overflow".to_owned(),
                        ))?;
                task.summary.cancel_requested = command.cancel_requested;
                task.summary.cancel_error = command.cancel_error;
                if let Some(output) = command.output {
                    task.summary.output = Some(output);
                }
                if is_terminal && command.finished_at_ms.is_some() {
                    task.summary.finished_at_ms = command.finished_at_ms;
                }
                (was_terminal, is_terminal, task.summary.clone())
            };
            if !was_terminal && is_terminal {
                state.unfinished_records = state.unfinished_records.saturating_sub(1);
                state.encoded_terminal_order.push_back(command.id);
                #[cfg(test)]
                state
                    .terminal_order_all
                    .push_back(TerminalTaskId::Encoded(command.id));
                while state.encoded_terminal_order.len() > self.history_capacity {
                    if !state.evict_oldest_encoded_terminal() {
                        break;
                    }
                }
            }
            Ok(summary)
        })
    }

    /// Persists a bounded progress snapshot using an attempt/version CAS.
    fn update_progress<'a>(
        &'a self,
        command: ProgressCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        Box::pin(async move {
            let snapshot = TaskProgressSnapshot::from_command(command.clone()).map_err(|_| {
                StoreError::InvalidRequest("task progress snapshot exceeds its limits")
            })?;
            let mut state = self.state.lock();
            let task = state
                .encoded_tasks
                .get_mut(&command.id)
                .ok_or(StoreError::NotFound)?;
            if !matches!(task.summary.state, TaskState::Running)
                || task.summary.attempt != command.expected_attempt
                || task
                    .summary
                    .progress
                    .as_ref()
                    .is_some_and(|current| current.progress_version >= command.progress_version)
            {
                return Err(StoreError::Conflict);
            }
            task.summary.progress = Some(snapshot);
            Ok(task.summary.clone())
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
    #[cfg(test)]
    fn transition<'a>(
        &'a self,
        command: TransitionCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
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
            let record = state
                .records
                .get_mut(&command.id)
                .ok_or(StoreError::NotFound)?;
            if record.state_version != command.expected_version
                || record.attempt != command.expected_attempt
            {
                return Err(StoreError::Conflict);
            }
            if !record.state.allows_transition_to(&command.state) {
                return Err(StoreError::InvalidTransition);
            }
            if command.retry_not_before_ms.is_some() && !matches!(command.state, TaskState::Queued)
            {
                return Err(StoreError::InvalidRequest(
                    "only queued tasks may have a retry deadline",
                ));
            }
            let was_unfinished = !record.state.is_terminal();
            let becomes_terminal = command.state.is_terminal();
            let starting = !matches!(record.state, TaskState::Running)
                && matches!(command.state, TaskState::Running);
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
                state
                    .terminal_order_all
                    .push_back(TerminalTaskId::Legacy(command.id));
                while state.terminal_order_all.len() > self.history_capacity {
                    if !state.evict_oldest_terminal() {
                        break;
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
    #[cfg(test)]
    fn get_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
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
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
    fn get_summary<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        Box::pin(async move { Ok(self.state.lock().records.get(&id).map(TaskRecord::summary)) })
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
    #[cfg(test)]
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        Box::pin(async move {
            let page_size = checked_page_size(query.limit)?;
            let fetch_limit = page_size.checked_add(1).ok_or(StoreError::InvalidRequest(
                "task history page limit is too large",
            ))?;
            let state = self.state.lock();
            let mut candidates = BinaryHeap::with_capacity(fetch_limit);
            for record in state.records.values().filter(|record| {
                (query.states.is_empty() || query.states.contains(&record.state.kind()))
                    && query
                        .correlation_key
                        .as_ref()
                        .is_none_or(|key| record.request.correlation_key.as_ref() == Some(key))
                    && query.after.is_none_or(|after| {
                        (record.accepted_at_ms, record.id) > (after.accepted_at_ms, after.id)
                    })
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

    /// Lists typed summaries using `(accepted_at_ms, numeric task ID)` order.
    fn list_encoded<'a>(
        &'a self,
        query: EncodedTaskQuery,
    ) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        Box::pin(async move {
            let page_size = query.checked_page_size()?;
            let fetch_limit = page_size.checked_add(1).ok_or(StoreError::InvalidRequest(
                "task history page limit is too large",
            ))?;
            let state = self.state.lock();
            let mut candidates = BinaryHeap::with_capacity(fetch_limit);
            for task in state.encoded_tasks.values().filter(|task| {
                (query.states.is_empty() || query.states.contains(&task.summary.state.kind()))
                    && query
                        .category
                        .as_ref()
                        .is_none_or(|category| task.summary.category.as_ref() == Some(category))
                    && query
                        .correlation_key
                        .as_ref()
                        .is_none_or(|key| task.summary.correlation_key.as_ref() == Some(key))
                    && query.after.is_none_or(|after| {
                        (task.summary.accepted_at_ms, task.summary.id)
                            > (after.accepted_at_ms, after.id)
                    })
            }) {
                let key = (task.summary.accepted_at_ms, task.summary.id);
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
            let records = candidates
                .iter()
                .map(|(_, id)| {
                    state
                        .encoded_tasks
                        .get(id)
                        .expect("page candidate remains retained")
                        .summary
                        .clone()
                })
                .collect::<Vec<_>>();
            let next = has_more
                .then(|| records.last().map(EncodedTaskCursor::from))
                .flatten();
            Ok(EncodedTaskPage { records, next })
        })
    }

    fn list_ready_queued<'a>(
        &'a self,
        after: Option<crate::model::next::TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        Box::pin(async move {
            if limit.get() > crate::model::next::MAX_TASK_QUERY_LIMIT {
                return Err(StoreError::InvalidRequest(
                    "ready task page limit exceeds 256",
                ));
            }
            let state = self.state.lock();
            let mut records = state
                .encoded_tasks
                .values()
                .filter(|task| {
                    task.summary.state == TaskState::Queued
                        && task
                            .summary
                            .retry_not_before_ms
                            .is_none_or(|deadline| deadline <= now_ms)
                        && after.is_none_or(|cursor| {
                            (task.summary.accepted_at_ms, task.summary.id)
                                > (cursor.accepted_at_ms, cursor.id)
                        })
                })
                .map(|task| task.summary.clone())
                .collect::<Vec<_>>();
            records.sort_unstable_by_key(|summary| (summary.accepted_at_ms, summary.id));
            let has_more = records.len() > limit.get();
            records.truncate(limit.get());
            let next = has_more
                .then(|| records.last().map(EncodedTaskCursor::from))
                .flatten();
            Ok(EncodedTaskPage { records, next })
        })
    }

    fn next_retry_deadline<'a>(
        &'a self,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .encoded_tasks
                .values()
                .filter(|task| task.summary.state == TaskState::Queued)
                .filter_map(|task| task.summary.retry_not_before_ms)
                .filter(|deadline| *deadline > now_ms)
                .min())
        })
    }

    fn prune_typed_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            let mut candidates = state
                .encoded_tasks
                .values()
                .filter_map(|task| {
                    let finished_at_ms = task.summary.finished_at_ms?;
                    (matches!(
                        task.summary.state,
                        TaskState::Succeeded
                            | TaskState::Failed { .. }
                            | TaskState::Panicked { .. }
                            | TaskState::Cancelled
                    ) && finished_at_ms < finished_before_ms)
                        .then_some((finished_at_ms, task.summary.id))
                })
                .collect::<Vec<_>>();
            candidates.sort_unstable();
            let expired = candidates
                .into_iter()
                .take(max_rows.get())
                .map(|(_, id)| id)
                .collect::<Vec<_>>();
            for id in &expired {
                state.remove_encoded_task(*id);
            }
            Ok(expired.len())
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
    #[cfg(test)]
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
                .filter(|record| {
                    record.state.is_terminal() && record.accepted_at_ms < accepted_before_ms
                })
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

    /// Acquires exclusive ownership within this in-process store instance.
    ///
    /// The epoch is local to this `MemoryTaskStore`; it does not imply restart
    /// recovery or coordination with another process.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            if state.owner_epoch.is_some() {
                return Err(StoreError::OwnerConflict);
            }
            let next_epoch = state
                .last_owner_epoch
                .checked_add(1)
                .ok_or_else(|| StoreError::Failure("memory owner epoch exhausted".into()))?;
            let epoch = OwnerEpoch(next_epoch);
            state.last_owner_epoch = next_epoch;
            state.owner_epoch = Some(epoch);
            Ok(epoch)
        })
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
    #[cfg(test)]
    fn has_unfinished_over_limit<'a>(
        &'a self,
        limit: usize,
    ) -> TaskFuture<'a, Result<bool, StoreError>> {
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
    #[cfg(test)]
    fn scan_unfinished<'a>(
        &'a self,
        _cursor: Option<TaskCursor>,
    ) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Releases the current in-process owner when its epoch matches.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            if state.owner_epoch != Some(epoch) {
                return Err(StoreError::OwnerConflict);
            }
            state.owner_epoch = None;
            Ok(())
        })
    }
}

/// Reads the current Unix epoch time in milliseconds, defaulting on clock
/// error.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the system clock predates the epoch.
fn validate_encoded_output(
    state: &TaskState,
    output: Option<&crate::model::TaskOutput>,
) -> Result<(), StoreError> {
    if output.is_some() && state != &TaskState::Succeeded {
        return Err(StoreError::InvalidRequest(
            "task output can only be stored with the succeeded state",
        ));
    }
    if output.is_some_and(|value| value.summary.len() > MAX_TASK_OUTPUT_SUMMARY_BYTES) {
        return Err(StoreError::InvalidRequest(
            "task output summary exceeds the 64 KiB limit",
        ));
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::MemoryTaskStore;
    use crate::model::legacy::AcceptOutcome;
    use crate::model::legacy::TaskId;
    use crate::model::legacy::TaskQuery;
    use crate::model::legacy::TaskRequest;
    use crate::store::LegacyTaskStore;

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
