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
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::ops::Bound::Excluded;
use std::ops::Bound::Unbounded;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use internal::MemoryState;
use parking_lot::Mutex;

use super::StoreError;
use super::TaskFuture;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::typed::AcceptOutcome as EncodedAcceptOutcome;
use crate::model::typed::MAX_TASK_QUERY_LIMIT;
use crate::model::typed::ProgressCommand;
use crate::model::typed::StartCommand;
use crate::model::typed::StoredTask;
use crate::model::typed::StoredTaskRequest;
use crate::model::typed::TaskCursor;
use crate::model::typed::TaskCursor as EncodedTaskCursor;
use crate::model::typed::TaskId as EncodedTaskId;
use crate::model::typed::TaskPage as EncodedTaskPage;
use crate::model::typed::TaskProgressSnapshot;
use crate::model::typed::TaskQuery as EncodedTaskQuery;
use crate::model::typed::TaskStateCheckpoint;
use crate::model::typed::TaskSummary;
use crate::model::typed::TransitionCommand;

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
                queued_order: BTreeSet::new(),
                #[cfg(test)]
                ready_scan_visits: std::cell::Cell::new(0),
                encoded_idempotency: HashMap::new(),
                encoded_terminal_order: VecDeque::new(),
                retained_payload_bytes: 0,
                unfinished_records: 0,
            }),
        }
    }
}

/// Checks that output is only attached to a successful state and stays within
/// the configured summary-size limit.
///
/// # Errors
///
/// Returns an invalid-request error when output accompanies a non-success
/// state or its summary exceeds the limit.
///
/// # Parameters
///
/// * `state` - Lifecycle state the output would accompany.
/// * `output` - Optional output snapshot to validate.
fn validate_encoded_output(state: &TaskState, output: Option<&TaskOutput>) -> Result<(), StoreError> {
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

/// Reads the current Unix epoch time in milliseconds, defaulting on clock
/// error.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the system clock predates the epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
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
    ///
    /// Existing idempotency keys replay the original summary when the request
    /// matches. New requests may evict terminal records to satisfy configured
    /// retention limits; unfinished records are never evicted here.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for empty kind or codec identifiers or when
    /// payload capacity is insufficient, `NotFound` for a stale idempotency
    /// index, `IdempotencyConflict` for a key reused with different content,
    /// `DuplicateTask` for an existing ID, or
    /// `UnfinishedRecordLimitExceeded` when the unfinished limit is reached.
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier assigned to the request being accepted.
    /// * `request` - Encoded request and metadata to retain.
    ///
    /// # Returns
    ///
    /// Returns the accepted summary and whether this call created the task.
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
            let fits = state
                .retained_payload_bytes
                .checked_add(requested_bytes)
                .is_some_and(|total| total <= self.max_payload_bytes);
            if !fits {
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
            let summary = TaskSummary {
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
            state.queued_order.insert((now, id));
            state.unfinished_records += 1;
            Ok(EncodedAcceptOutcome { summary, created: true })
        })
    }

    /// Loads one retained encoded request together with its current summary.
    ///
    /// Returns `None` when the ID was never retained or its record was evicted
    /// or pruned. The returned value is an owned snapshot.
    ///
    /// # Errors
    ///
    /// This in-memory implementation does not produce read errors.
    ///
    /// # Returns
    ///
    /// Returns the stored request and summary, or `None` when the task is not
    /// retained.
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier of the retained task to retrieve.
    fn get_encoded_task<'a>(&'a self, id: EncodedTaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        Box::pin(async move { Ok(self.state.lock().encoded_tasks.get(&id).cloned()) })
    }

    /// Reads only the retained task summary, cloning no request payload bytes.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task to inspect.
    ///
    /// # Returns
    ///
    /// The summary when retained, or `None` when the task is absent.
    ///
    /// # Errors
    ///
    /// This in-memory implementation does not produce read errors.
    fn get_summary<'a>(&'a self, id: EncodedTaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            Ok(state.encoded_tasks.get(&id).map(|task| task.summary.clone()))
        })
    }

    /// Returns the retained task's authoritative lifecycle state and revision.
    ///
    /// A pruned or evicted task has no checkpoint in this volatile store.
    /// The checkpoint is an owned snapshot that can be used to reconcile a
    /// caller's lifecycle state.
    ///
    /// # Errors
    ///
    /// This in-memory implementation does not produce read errors.
    ///
    /// # Returns
    ///
    /// Returns the current checkpoint, or `None` when no task is retained.
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier of the task whose current checkpoint is requested.
    fn get_state_checkpoint<'a>(
        &'a self,
        id: EncodedTaskId,
    ) -> TaskFuture<'a, Result<Option<TaskStateCheckpoint>, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            Ok(state.encoded_tasks.get(&id).map(|task| TaskStateCheckpoint {
                id: task.summary.id,
                state_version: task.summary.state_version,
                state: task.summary.state.clone(),
            }))
        })
    }

    /// Starts a queued encoded task attempt using a lifecycle revision CAS.
    ///
    /// Successful starts increment both the attempt and state revision, record
    /// the supplied start time, and clear retry and progress data.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` if the task is not retained, `Conflict` unless it is
    /// queued at the expected revision, or `Failure` if a counter overflows.
    ///
    /// # Returns
    ///
    /// Returns the updated summary after the attempt starts.
    ///
    /// # Parameters
    ///
    /// * `command` - Start request containing the task, expected revision, and
    ///   start time.
    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            let (summary, queued_key) = {
                let task = state.encoded_tasks.get_mut(&command.id).ok_or(StoreError::NotFound)?;
                if task.summary.state_version != command.expected_state_version
                    || !matches!(task.summary.state, TaskState::Queued)
                {
                    return Err(StoreError::Conflict);
                }
                let next_version = task
                    .summary
                    .state_version
                    .checked_add(1)
                    .ok_or(StoreError::Failure("task state version overflow".to_owned()))?;
                let next_attempt = task
                    .summary
                    .attempt
                    .checked_add(1)
                    .ok_or(StoreError::Failure("task attempt counter overflow".to_owned()))?;
                let queued_key = (task.summary.accepted_at_ms, command.id);
                task.summary.state_version = next_version;
                task.summary.attempt = next_attempt;
                task.summary.state = TaskState::Running;
                task.summary.retry_not_before_ms = None;
                task.summary.started_at_ms = Some(command.started_at_ms);
                task.summary.finished_at_ms = None;
                task.summary.progress = None;
                (task.summary.clone(), queued_key)
            };
            let removed = state.queued_order.remove(&queued_key);
            debug_assert!(removed, "queued record must have an order index entry");
            Ok(summary)
        })
    }

    /// Applies a typed lifecycle transition with revision and attempt checks.
    ///
    /// A newly terminal task enters bounded history retention; the returned
    /// summary reflects the committed transition.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for invalid diagnostics, output, or retry
    /// deadline; `NotFound` for an unretained task; `Conflict` when its
    /// revision, attempt, or transition no longer matches; or `Failure` if
    /// the state revision overflows.
    ///
    /// # Returns
    ///
    /// Returns the summary after the transition is applied.
    ///
    /// # Parameters
    ///
    /// * `command` - Requested lifecycle transition and its expected revision
    ///   and attempt.
    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            command
                .state
                .validate_diagnostics()
                .map_err(StoreError::InvalidRequest)?;
            if command
                .cancel_error
                .as_ref()
                .is_some_and(|message| message.len() > crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES)
            {
                return Err(StoreError::InvalidRequest(
                    "cancel error exceeds the diagnostic message limit",
                ));
            }
            validate_encoded_output(&command.state, command.output.as_ref())?;
            let mut state = self.state.lock();
            let (was_terminal, is_terminal, was_queued, is_queued, accepted_at_ms, summary) = {
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
                let was_queued = task.summary.state == TaskState::Queued;
                let is_queued = command.state == TaskState::Queued;
                let next_version = task
                    .summary
                    .state_version
                    .checked_add(1)
                    .ok_or(StoreError::Failure("task state version overflow".to_owned()))?;
                let accepted_at_ms = task.summary.accepted_at_ms;
                task.summary.state = command.state;
                task.summary.retry_not_before_ms = command.retry_not_before_ms;
                task.summary.state_version = next_version;
                task.summary.cancel_requested = command.cancel_requested;
                task.summary.cancel_error = command.cancel_error;
                if let Some(output) = command.output {
                    task.summary.output = Some(output);
                }
                if is_terminal && command.finished_at_ms.is_some() {
                    task.summary.finished_at_ms = command.finished_at_ms;
                }
                (
                    was_terminal,
                    is_terminal,
                    was_queued,
                    is_queued,
                    accepted_at_ms,
                    task.summary.clone(),
                )
            };
            if was_queued {
                let removed = state.queued_order.remove(&(accepted_at_ms, command.id));
                debug_assert!(removed, "queued record must have an order index entry");
            }
            if is_queued {
                state.queued_order.insert((accepted_at_ms, command.id));
            }
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
    ///
    /// Progress is accepted only for a running task, its current attempt, and
    /// a version newer than the stored snapshot.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` when the snapshot exceeds its limits,
    /// `NotFound` when the task is not retained, or `Conflict` when the task
    /// is not running or the attempt/version is stale.
    ///
    /// # Returns
    ///
    /// Returns the summary containing the new progress snapshot.
    ///
    /// # Parameters
    ///
    /// * `command` - Progress snapshot and expected task attempt/version.
    fn update_progress<'a>(&'a self, command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            let snapshot = TaskProgressSnapshot::from_command(command.clone())
                .map_err(|_| StoreError::InvalidRequest("task progress snapshot exceeds its limits"))?;
            let mut state = self.state.lock();
            let task = state.encoded_tasks.get_mut(&command.id).ok_or(StoreError::NotFound)?;
            if !matches!(task.summary.state, TaskState::Running)
                || task.summary.attempt != command.expected_attempt
                || command.progress_version == 0
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
    ///
    /// Applies the query filters and returns at most the checked page size. A
    /// cursor is included only when another matching record exists.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` when the query page size is invalid or cannot
    /// be incremented safely.
    ///
    /// # Returns
    ///
    /// Returns the matching page and, when more records remain, its next
    /// cursor.
    ///
    /// # Parameters
    ///
    /// * `query` - Filters, exclusive cursor, and requested page size.
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

    /// Lists due queued summaries after an optional cursor, in acceptance
    /// order.
    ///
    /// Only tasks whose retry deadline is absent or no later than `now_ms`
    /// are returned. A cursor is included when another eligible task exists.
    /// Rejects limits above the public task-query maximum.
    ///
    /// The implementation scans queued-order keys after the exclusive cursor,
    /// stopping after finding `limit + 1` due tasks. Future retries are
    /// skipped, and at most `limit` summaries are cloned.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` when `limit` exceeds 256.
    ///
    /// # Returns
    ///
    /// Returns eligible queued summaries and a cursor when another page
    /// remains.
    ///
    /// # Parameters
    ///
    /// * `after` - Optional exclusive position after which to list tasks.
    /// * `limit` - Non-zero maximum number of summaries to return, up to 256.
    /// * `now_ms` - Current time used to determine retry eligibility.
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        Box::pin(async move {
            if limit.get() > MAX_TASK_QUERY_LIMIT {
                return Err(StoreError::InvalidRequest("ready task page limit exceeds 256"));
            }
            let state = self.state.lock();
            let keep = limit.get() + 1;
            let lower = after
                .map(|cursor| Excluded((cursor.accepted_at_ms, cursor.id)))
                .unwrap_or(Unbounded);
            let mut keys = Vec::with_capacity(keep);
            #[cfg(test)]
            state.ready_scan_visits.set(0);
            for key in state.queued_order.range((lower, Unbounded)) {
                #[cfg(test)]
                state.ready_scan_visits.set(state.ready_scan_visits.get() + 1);
                let task = state
                    .encoded_tasks
                    .get(&key.1)
                    .expect("queued order entry has a retained task");
                let summary = &task.summary;
                debug_assert_eq!(
                    summary.state,
                    TaskState::Queued,
                    "queued order contains only queued tasks"
                );
                if summary.retry_not_before_ms.is_some_and(|deadline| deadline > now_ms) {
                    continue;
                }
                keys.push(*key);
                if keys.len() == keep {
                    break;
                }
            }
            let has_more = keys.len() > limit.get();
            keys.truncate(limit.get());
            let records = keys
                .into_iter()
                .map(|(_, id)| {
                    state
                        .encoded_tasks
                        .get(&id)
                        .expect("queued order entry has a retained task")
                        .summary
                        .clone()
                })
                .collect::<Vec<_>>();
            let next = has_more.then(|| records.last().map(EncodedTaskCursor::from)).flatten();
            Ok(EncodedTaskPage { records, next })
        })
    }

    /// Finds the earliest queued retry deadline strictly later than `now_ms`.
    ///
    /// Returns `None` when no queued task has a future retry deadline.
    ///
    /// # Errors
    ///
    /// This in-memory implementation does not produce read errors.
    ///
    /// # Returns
    ///
    /// Returns the earliest future retry deadline, or `None` if there is none.
    ///
    /// # Parameters
    ///
    /// * `now_ms` - Current time; only deadlines strictly later are returned.
    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        Box::pin(async move {
            let state = self.state.lock();
            Ok(state
                .queued_order
                .iter()
                .map(|(_, id)| {
                    state
                        .encoded_tasks
                        .get(id)
                        .expect("queued order entry has a retained task")
                })
                .filter_map(|task| task.summary.retry_not_before_ms)
                .filter(|deadline| *deadline > now_ms)
                .min())
        })
    }

    /// Removes up to `max_rows` terminal records finished before the cutoff.
    ///
    /// Eligible records are removed in `(finished_at_ms, task ID)` order;
    /// unfinished records and records at the cutoff are retained.
    ///
    /// # Errors
    ///
    /// This in-memory implementation does not produce pruning errors.
    ///
    /// # Returns
    ///
    /// Returns the number of records removed.
    ///
    /// # Parameters
    ///
    /// * `finished_before_ms` - Exclusive completion-time cutoff.
    /// * `max_rows` - Non-zero upper bound on the number of records to remove.
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
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` if an owner already holds this store.
    ///
    /// # Returns
    ///
    /// Returns the newly acquired ownership epoch.
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
    ///
    /// A stale epoch cannot release ownership acquired by a later service.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` unless `epoch` is the current owner epoch.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after ownership is released.
    ///
    /// # Parameters
    ///
    /// * `epoch` - Ownership epoch that must match the current owner.
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::super::TaskStore;
    use super::EncodedTaskId;
    use super::MemoryTaskStore;
    use crate::model::TaskState;
    use crate::model::typed::AcceptOutcome;
    use crate::model::typed::StartCommand;
    use crate::model::typed::StoredTaskRequest;
    use crate::model::typed::TransitionCommand;

    /// Uses a fixed timestamp to verify numeric ID ordering for tied keys.
    #[tokio::test]
    async fn test_ready_queue_orders_equal_timestamps_by_numeric_id() {
        let store = MemoryTaskStore::new(8);
        for value in [3, 1, 2] {
            let id = EncodedTaskId::from_id(qubit_id::Id::new(value));
            let request = StoredTaskRequest {
                kind_id: "test".to_owned(),
                category: None,
                payload: crate::model::typed::StoredPayload {
                    type_id: qubit_model_id::ModelIdBuf::parse("test.Payload").expect("model ID is valid"),
                    schema_version: 1,
                    codec_id: "test.codec".to_owned(),
                    bytes: Vec::new(),
                },
                metadata: Default::default(),
                resource_limit: Default::default(),
                correlation_key: None,
                idempotency_key: None,
            };
            TaskStore::accept_encoded(&store, id, request)
                .await
                .expect("task is accepted");
        }
        {
            let mut state = store.state.lock();
            let keys = state
                .encoded_tasks
                .iter_mut()
                .map(|(id, task)| {
                    task.summary.accepted_at_ms = 5;
                    (*id, task.summary.accepted_at_ms)
                })
                .collect::<Vec<_>>();
            state.queued_order.clear();
            state
                .queued_order
                .extend(keys.into_iter().map(|(id, accepted_at_ms)| (accepted_at_ms, id)));
        }

        let page = TaskStore::list_ready_queued(
            &store,
            None,
            NonZeroUsize::new(2).expect("page size is positive"),
            u64::MAX,
        )
        .await
        .expect("ready page succeeds");
        let ids = page.records.into_iter().map(|summary| summary.id).collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                EncodedTaskId::from_id(qubit_id::Id::new(1)),
                EncodedTaskId::from_id(qubit_id::Id::new(2)),
            ]
        );
        assert!(page.next.is_some());
    }

    /// A ready page visits queued records only, even when terminal history is
    /// retained.
    #[tokio::test]
    async fn test_ready_queue_scan_skips_terminal_history() {
        let store = MemoryTaskStore::new(32);
        let request = || StoredTaskRequest {
            kind_id: "test".to_owned(),
            category: None,
            payload: crate::model::typed::StoredPayload {
                type_id: qubit_model_id::ModelIdBuf::parse("test.Payload").expect("model ID is valid"),
                schema_version: 1,
                codec_id: "test.codec".to_owned(),
                bytes: Vec::new(),
            },
            metadata: Default::default(),
            resource_limit: Default::default(),
            correlation_key: None,
            idempotency_key: None,
        };
        for value in 1..=12 {
            let id = EncodedTaskId::from_id(qubit_id::Id::new(value));
            let accepted = TaskStore::accept_encoded(&store, id, request())
                .await
                .expect("task accepted");
            let running = TaskStore::start_encoded(
                &store,
                super::StartCommand {
                    id,
                    expected_state_version: accepted.summary.state_version,
                    started_at_ms: 1,
                },
            )
            .await
            .expect("task starts");
            TaskStore::transition_encoded(
                &store,
                TransitionCommand {
                    id,
                    expected_state_version: running.state_version,
                    expected_attempt: running.attempt,
                    retry_not_before_ms: None,
                    state: TaskState::Succeeded,
                    cancel_requested: false,
                    cancel_error: None,
                    output: None,
                    finished_at_ms: Some(2),
                },
            )
            .await
            .expect("task finishes");
        }
        for value in 13..=14 {
            let id = EncodedTaskId::from_id(qubit_id::Id::new(value));
            TaskStore::accept_encoded(&store, id, request())
                .await
                .expect("queued task accepted");
        }

        let page = TaskStore::list_ready_queued(
            &store,
            None,
            NonZeroUsize::new(1).expect("positive page size"),
            u64::MAX,
        )
        .await
        .expect("ready page succeeds");
        assert_eq!(page.records.len(), 1);
        assert!(page.next.is_some());
        assert_eq!(store.state.lock().ready_scan_visits.get(), 2);
    }

    fn assert_queued_index_matches_records(store: &MemoryTaskStore) {
        let state = store.state.lock();
        let expected = state
            .encoded_tasks
            .values()
            .filter(|task| task.summary.state == TaskState::Queued)
            .map(|task| (task.summary.accepted_at_ms, task.summary.id))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(state.queued_order, expected);
    }

    fn lifecycle_request(id: u64) -> StoredTaskRequest {
        StoredTaskRequest {
            kind_id: "test.lifecycle".to_owned(),
            category: None,
            payload: crate::model::typed::StoredPayload {
                type_id: qubit_model_id::ModelIdBuf::parse("test.Payload").expect("model ID is valid"),
                schema_version: 1,
                codec_id: "test.codec".to_owned(),
                bytes: Vec::new(),
            },
            metadata: Default::default(),
            resource_limit: Default::default(),
            correlation_key: None,
            idempotency_key: Some(format!("lifecycle-{id}")),
        }
    }

    /// Maintains queued order across retries, blocking, cancellation,
    /// retention, and prune.
    #[tokio::test]
    async fn test_queued_index_tracks_lifecycle_and_removal_paths() {
        let store = MemoryTaskStore::new(2);
        let id1 = EncodedTaskId::from_id(qubit_id::Id::new(1));
        let id2 = EncodedTaskId::from_id(qubit_id::Id::new(2));
        let first = TaskStore::accept_encoded(&store, id1, lifecycle_request(1))
            .await
            .expect("accept succeeds");
        assert_queued_index_matches_records(&store);
        let replay = TaskStore::accept_encoded(
            &store,
            EncodedTaskId::from_id(qubit_id::Id::new(99)),
            lifecycle_request(1),
        )
        .await
        .expect("idempotent replay succeeds");
        assert!(matches!(replay, AcceptOutcome { created: false, .. }));
        assert_queued_index_matches_records(&store);

        let started1 = TaskStore::start_encoded(
            &store,
            StartCommand {
                id: id1,
                expected_state_version: first.summary.state_version,
                started_at_ms: 10,
            },
        )
        .await
        .expect("first task starts");
        assert_queued_index_matches_records(&store);
        let retry1 = TaskStore::transition_encoded(
            &store,
            TransitionCommand {
                id: id1,
                expected_state_version: started1.state_version,
                expected_attempt: started1.attempt,
                retry_not_before_ms: Some(100),
                state: TaskState::Queued,
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: None,
            },
        )
        .await
        .expect("first task is requeued for retry");
        assert_queued_index_matches_records(&store);
        assert_eq!(
            TaskStore::next_retry_deadline(&store, 0)
                .await
                .expect("retry deadline lookup succeeds"),
            Some(100)
        );

        let accepted2 = TaskStore::accept_encoded(&store, id2, lifecycle_request(2))
            .await
            .expect("second task accepted");
        assert_queued_index_matches_records(&store);
        let started2 = TaskStore::start_encoded(
            &store,
            StartCommand {
                id: id2,
                expected_state_version: accepted2.summary.state_version,
                started_at_ms: 11,
            },
        )
        .await
        .expect("second task starts");
        let blocked2 = TaskStore::transition_encoded(
            &store,
            TransitionCommand {
                id: id2,
                expected_state_version: started2.state_version,
                expected_attempt: started2.attempt,
                retry_not_before_ms: None,
                state: TaskState::Blocked {
                    reason: "pause".to_owned(),
                },
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: None,
            },
        )
        .await
        .expect("second task blocks");
        assert_queued_index_matches_records(&store);
        let resumed2 = TaskStore::transition_encoded(
            &store,
            TransitionCommand {
                id: id2,
                expected_state_version: blocked2.state_version,
                expected_attempt: blocked2.attempt,
                retry_not_before_ms: None,
                state: TaskState::Queued,
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: None,
            },
        )
        .await
        .expect("blocked task resumes into queue");
        assert_queued_index_matches_records(&store);

        let cancelled1 = TaskStore::transition_encoded(
            &store,
            TransitionCommand {
                id: id1,
                expected_state_version: retry1.state_version,
                expected_attempt: retry1.attempt,
                retry_not_before_ms: None,
                state: TaskState::Cancelled,
                cancel_requested: true,
                cancel_error: None,
                output: None,
                finished_at_ms: Some(20),
            },
        )
        .await
        .expect("queued task cancels");
        assert_eq!(cancelled1.state, TaskState::Cancelled);
        assert_queued_index_matches_records(&store);
        assert_eq!(
            TaskStore::next_retry_deadline(&store, 0)
                .await
                .expect("terminal task is skipped"),
            None
        );

        let finished2 = TaskStore::transition_encoded(
            &store,
            TransitionCommand {
                id: id2,
                expected_state_version: resumed2.state_version,
                expected_attempt: resumed2.attempt,
                retry_not_before_ms: None,
                state: TaskState::Cancelled,
                cancel_requested: true,
                cancel_error: None,
                output: None,
                finished_at_ms: Some(21),
            },
        )
        .await
        .expect("resumed queued task cancels");
        assert_queued_index_matches_records(&store);

        // Force terminal history eviction through the same removal helper.
        let id3 = EncodedTaskId::from_id(qubit_id::Id::new(3));
        let accepted3 = TaskStore::accept_encoded(&store, id3, lifecycle_request(3))
            .await
            .expect("third accepted");
        let started3 = TaskStore::start_encoded(
            &store,
            StartCommand {
                id: id3,
                expected_state_version: accepted3.summary.state_version,
                started_at_ms: 22,
            },
        )
        .await
        .expect("third starts");
        TaskStore::transition_encoded(
            &store,
            TransitionCommand {
                id: id3,
                expected_state_version: started3.state_version,
                expected_attempt: started3.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: Some(23),
            },
        )
        .await
        .expect("third finishes and evicts oldest history");
        assert_queued_index_matches_records(&store);
        assert!(
            TaskStore::get_encoded_task(&store, id1)
                .await
                .expect("lookup succeeds")
                .is_none()
        );

        let id4 = EncodedTaskId::from_id(qubit_id::Id::new(4));
        let accepted4 = TaskStore::accept_encoded(&store, id4, lifecycle_request(4))
            .await
            .expect("fourth accepted");
        let started4 = TaskStore::start_encoded(
            &store,
            StartCommand {
                id: id4,
                expected_state_version: accepted4.summary.state_version,
                started_at_ms: 24,
            },
        )
        .await
        .expect("fourth starts");
        TaskStore::transition_encoded(
            &store,
            TransitionCommand {
                id: id4,
                expected_state_version: started4.state_version,
                expected_attempt: started4.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: Some(25),
            },
        )
        .await
        .expect("fourth finishes");
        TaskStore::prune_terminal_before(&store, 26, NonZeroUsize::new(1).expect("positive prune limit"))
            .await
            .expect("pruning succeeds");
        assert_queued_index_matches_records(&store);
        assert_eq!(finished2.state, TaskState::Cancelled);
    }
}
