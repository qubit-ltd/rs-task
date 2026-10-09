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
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::TaskState;
use crate::model::typed::AcceptOutcome as EncodedAcceptOutcome;
use crate::model::typed::ProgressCommand;
use crate::model::typed::StartCommand;
use crate::model::typed::StoredTask;
use crate::model::typed::StoredTaskRequest;
use crate::model::typed::TaskCursor as EncodedTaskCursor;
use crate::model::typed::TaskId as EncodedTaskId;
use crate::model::typed::TaskPage as EncodedTaskPage;
use crate::model::typed::TaskProgressSnapshot;
use crate::model::typed::TaskQuery as EncodedTaskQuery;

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
                owner_epoch: None,
                last_owner_epoch: 0,
                encoded_tasks: BTreeMap::new(),
                encoded_idempotency: HashMap::new(),
                encoded_terminal_order: VecDeque::new(),
                retained_payload_bytes: 0,
                unfinished_records: 0,
            }),
        }
    }
}

/// Reads the current Unix epoch time in milliseconds, defaulting on clock
/// error.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the system clock predates the epoch.
fn validate_encoded_output(state: &TaskState, output: Option<&crate::model::TaskOutput>) -> Result<(), StoreError> {
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

impl super::TaskStore for MemoryTaskStore {
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
                let existing = state.encoded_tasks.get(existing_id).ok_or(StoreError::NotFound)?;
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
            let minimum_retained = state.retained_payload_bytes.saturating_sub(reclaimable_bytes);
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
                        available_bytes: self.max_payload_bytes.saturating_sub(state.retained_payload_bytes),
                    });
                }
            }
            let now = now_ms();
            let summary = crate::model::typed::TaskSummary {
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
            Ok(EncodedAcceptOutcome { summary, created: true })
        })
    }

    /// Loads one encoded request together with its current summary.
    fn get_encoded_task<'a>(&'a self, id: EncodedTaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        Box::pin(async move { Ok(self.state.lock().encoded_tasks.get(&id).cloned()) })
    }

    /// Starts a queued encoded task attempt using a lifecycle revision CAS.
    fn start_encoded<'a>(
        &'a self,
        command: StartCommand,
    ) -> TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            let task = state.encoded_tasks.get_mut(&command.id).ok_or(StoreError::NotFound)?;
            if task.summary.state_version != command.expected_state_version
                || !matches!(task.summary.state, TaskState::Queued)
            {
                return Err(StoreError::Conflict);
            }
            task.summary.state_version = task
                .summary
                .state_version
                .checked_add(1)
                .ok_or(StoreError::Failure("task state version overflow".to_owned()))?;
            task.summary.attempt = task
                .summary
                .attempt
                .checked_add(1)
                .ok_or(StoreError::Failure("task attempt counter overflow".to_owned()))?;
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
        command: crate::model::typed::TransitionCommand,
    ) -> TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
        Box::pin(async move {
            command
                .state
                .validate_diagnostics()
                .map_err(StoreError::InvalidRequest)?;
            validate_encoded_output(&command.state, command.output.as_ref())?;
            let mut state = self.state.lock();
            let (was_terminal, is_terminal, summary) = {
                let task = state.encoded_tasks.get_mut(&command.id).ok_or(StoreError::NotFound)?;
                let terminal_cancel_annotation = task.summary.state.is_terminal()
                    && task.summary.state == command.state
                    && command.cancel_requested
                    && command.cancel_error.is_some();
                if task.summary.state_version != command.expected_state_version
                    || task.summary.attempt != command.expected_attempt
                    || (!task.summary.state.allows_transition_to(&command.state) && !terminal_cancel_annotation)
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
                task.summary.state_version = task
                    .summary
                    .state_version
                    .checked_add(1)
                    .ok_or(StoreError::Failure("task state version overflow".to_owned()))?;
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
    ) -> TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
        Box::pin(async move {
            let snapshot = TaskProgressSnapshot::from_command(command.clone())
                .map_err(|_| StoreError::InvalidRequest("task progress snapshot exceeds its limits"))?;
            let mut state = self.state.lock();
            let task = state.encoded_tasks.get_mut(&command.id).ok_or(StoreError::NotFound)?;
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

    /// Lists typed summaries using `(accepted_at_ms, numeric task ID)` order.
    fn list_encoded<'a>(&'a self, query: EncodedTaskQuery) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        Box::pin(async move {
            let page_size = query.checked_page_size()?;
            let fetch_limit = page_size
                .checked_add(1)
                .ok_or(StoreError::InvalidRequest("task history page limit is too large"))?;
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
                        (task.summary.accepted_at_ms, task.summary.id) > (after.accepted_at_ms, after.id)
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
            let next = has_more.then(|| records.last().map(EncodedTaskCursor::from)).flatten();
            Ok(EncodedTaskPage { records, next })
        })
    }

    fn list_ready_queued<'a>(
        &'a self,
        after: Option<crate::model::typed::TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        Box::pin(async move {
            if limit.get() > crate::model::typed::MAX_TASK_QUERY_LIMIT {
                return Err(StoreError::InvalidRequest("ready task page limit exceeds 256"));
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
                            (task.summary.accepted_at_ms, task.summary.id) > (cursor.accepted_at_ms, cursor.id)
                        })
                })
                .map(|task| task.summary.clone())
                .collect::<Vec<_>>();
            records.sort_unstable_by_key(|summary| (summary.accepted_at_ms, summary.id));
            let has_more = records.len() > limit.get();
            records.truncate(limit.get());
            let next = has_more.then(|| records.last().map(EncodedTaskCursor::from)).flatten();
            Ok(EncodedTaskPage { records, next })
        })
    }

    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
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

    fn prune_terminal_before<'a>(
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
