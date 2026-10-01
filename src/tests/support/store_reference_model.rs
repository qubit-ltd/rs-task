// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent lifecycle oracle. No production transition validator is used.

use std::collections::HashMap;

use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskQuery;
use crate::model::TaskRequest;
use crate::model::TaskRequestInfo;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskStateKind;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::store::StoreError;

pub const STATES: [TaskStateKind; 7] = [
    TaskStateKind::Queued,
    TaskStateKind::Running,
    TaskStateKind::Blocked,
    TaskStateKind::Succeeded,
    TaskStateKind::Failed,
    TaskStateKind::Panicked,
    TaskStateKind::Cancelled,
];

#[derive(Clone, Debug)]
pub enum Command {
    Accept {
        key: u8,
        variant: u8,
    },
    Transition {
        id_index: usize,
        version_delta: u8,
        attempt_delta: u8,
        legal: bool,
        state: u8,
    },
    RequestCancel {
        id_index: usize,
    },
    List {
        filter: u8,
        correlation: u8,
        after_index: usize,
        limit: usize,
    },
    Prune {
        cutoff_selector: usize,
        limit: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    Conflict,
    InvalidTransition,
    IdempotencyConflict,
    InvalidRequest,
}

/// Classifies only expected contract failures; infrastructure failures remain
/// failures.
pub fn error_kind(error: &StoreError) -> Option<ErrorKind> {
    match error {
        StoreError::NotFound => Some(ErrorKind::NotFound),
        StoreError::Conflict => Some(ErrorKind::Conflict),
        StoreError::InvalidTransition => Some(ErrorKind::InvalidTransition),
        StoreError::IdempotencyConflict => Some(ErrorKind::IdempotencyConflict),
        StoreError::InvalidRequest(_) => Some(ErrorKind::InvalidRequest),
        _ => None,
    }
}

/// Encodes the documented edges independently of `TaskState` helpers.
pub fn legal_edge(from: TaskStateKind, to: TaskStateKind) -> bool {
    match from {
        TaskStateKind::Queued => matches!(
            to,
            TaskStateKind::Running | TaskStateKind::Blocked | TaskStateKind::Cancelled
        ),
        TaskStateKind::Running => true,
        TaskStateKind::Blocked => matches!(to, TaskStateKind::Queued | TaskStateKind::Cancelled),
        _ => false,
    }
}

/// Returns a concrete diagnostic-bearing state for a generated state category.
pub fn concrete_state(kind: TaskStateKind) -> TaskState {
    match kind {
        TaskStateKind::Queued => TaskState::Queued,
        TaskStateKind::Running => TaskState::Running,
        TaskStateKind::Blocked => TaskState::Blocked {
            reason: "model blocked".into(),
        },
        TaskStateKind::Succeeded => TaskState::Succeeded,
        TaskStateKind::Failed => TaskState::Failed {
            category: "model".into(),
            message: "failure".into(),
        },
        TaskStateKind::Panicked => TaskState::Panicked {
            message: "model panic".into(),
        },
        TaskStateKind::Cancelled => TaskState::Cancelled,
    }
}

/// Defines terminal categories without using the production terminal predicate.
pub fn terminal(kind: TaskStateKind) -> bool {
    matches!(
        kind,
        TaskStateKind::Succeeded | TaskStateKind::Failed | TaskStateKind::Panicked | TaskStateKind::Cancelled
    )
}

/// Makes request variants differ in payload and observable immutable metadata.
pub fn request(key: u8, variant: u8) -> TaskRequest {
    let mut request = TaskRequest::new("model", "1", vec![variant]).with_idempotency_key(format!("model-key-{key}"));
    request.correlation_key = Some(format!("group-{}", key % 2));
    request.metadata.insert("variant".into(), variant.to_string());
    request
}

#[derive(Clone, Debug)]
pub struct ModelRecord {
    pub request: TaskRequest,
    pub summary: TaskSummary,
}

#[derive(Default)]
pub struct ReferenceModel {
    pub records: HashMap<TaskId, ModelRecord>,
    pub idempotency: HashMap<u8, TaskId>,
    /// Each acceptance attempt gets a stable logical ID, including rejected
    /// ones.
    pub ids: Vec<TaskId>,
    /// Keep cursors after pruning to exercise exclusive lower bounds through
    /// gaps.
    pub cursors: Vec<TaskCursor>,
}

/// Builds repeatable increasing IDs so a seed never depends on random UUID
/// ordering.
pub fn logical_id(index: usize) -> TaskId {
    serde_json::from_str(&format!("\"00000000-0000-0000-0000-{index:012x}\""))
        .expect("bounded logical index forms a UUID")
}

/// Forms the oracle cursor directly from fields rather than the production
/// conversion.
fn cursor(summary: &TaskSummary) -> TaskCursor {
    TaskCursor::new(summary.accepted_at_ms, summary.id)
}

impl ReferenceModel {
    /// Maps an index to a previously proposed ID or one repeatable unknown ID.
    /// Modulo mapping keeps small traces rich in operations on known records.
    pub fn id(&self, index: usize) -> TaskId {
        self.ids
            .get(index % (self.ids.len() + 1))
            .copied()
            .unwrap_or_else(|| logical_id(1_000_000 + index))
    }

    /// Constructs initial metadata independently; acceptance time comes from
    /// the store.
    pub fn accepted(&mut self, id: TaskId, key: u8, request: TaskRequest, accepted_at_ms: u64) {
        let info = TaskRequestInfo {
            task_type: request.task_type.clone(),
            handler_version: request.handler_version.clone(),
            resources: request.resources.clone(),
            correlation_key: request.correlation_key.clone(),
            idempotency_key: request.idempotency_key.clone(),
            metadata: request.metadata.clone(),
        };
        self.records.insert(
            id,
            ModelRecord {
                request,
                summary: TaskSummary {
                    id,
                    request: info,
                    state: TaskState::Queued,
                    state_version: 0,
                    attempt: 0,
                    retry_not_before_ms: None,
                    accepted_at_ms,
                    started_at_ms: None,
                    finished_at_ms: None,
                    assigned_resources: vec![],
                    output: None,
                    cancel_requested: false,
                },
            },
        );
        self.idempotency.insert(key, id);
        self.cursors.push(TaskCursor::new(accepted_at_ms, id));
    }

    /// Chooses a legal or illegal edge from the current oracle state before
    /// submission. If the requested edge class is empty (terminal/legal or
    /// running/illegal), the fallback is still classified by the oracle.
    pub fn transition_command(
        &self,
        id: TaskId,
        version_delta: u8,
        attempt_delta: u8,
        legal: bool,
        selector: u8,
    ) -> TransitionCommand {
        let record = self.records.get(&id);
        let from = record.map_or(TaskStateKind::Queued, |record| record.summary.state.kind());
        let candidates: Vec<_> = STATES.into_iter().filter(|to| legal_edge(from, *to) == legal).collect();
        let kind = candidates
            .get(usize::from(selector) % candidates.len().max(1))
            .copied()
            .unwrap_or(TaskStateKind::Running);
        TransitionCommand {
            id,
            expected_version: record
                .map_or(0, |record| record.summary.state_version)
                .saturating_add(u64::from(version_delta)),
            expected_attempt: record
                .map_or(0, |record| record.summary.attempt)
                .saturating_add(u32::from(attempt_delta)),
            state: concrete_state(kind),
            retry_not_before_ms: (kind == TaskStateKind::Queued).then_some(123_456),
            output: (kind == TaskStateKind::Succeeded).then_some(TaskOutput {
                summary: vec![selector],
            }),
            assigned_resources: vec![format!("device-{selector}")],
            cancel_requested: false,
        }
    }

    /// Requests cancellation by an allowed edge for unfinished records.
    pub fn cancel_command(&self, id: TaskId) -> TransitionCommand {
        let mut command = self.transition_command(id, 0, 0, true, 0);
        command.state = if self
            .records
            .get(&id)
            .is_some_and(|record| record.summary.state.kind() == TaskStateKind::Running)
        {
            TaskState::Running
        } else {
            TaskState::Cancelled
        };
        command.retry_not_before_ms = None;
        command.output = None;
        command.assigned_resources = self
            .records
            .get(&id)
            .map_or_else(Vec::new, |record| record.summary.assigned_resources.clone());
        command.cancel_requested = true;
        command
    }

    /// Applies the public CAS contract or returns its independently predicted
    /// error.
    pub fn transition(&mut self, command: &TransitionCommand) -> Result<TaskSummary, ErrorKind> {
        let record = self.records.get_mut(&command.id).ok_or(ErrorKind::NotFound)?;
        let summary = &mut record.summary;
        if summary.state_version != command.expected_version || summary.attempt != command.expected_attempt {
            return Err(ErrorKind::Conflict);
        }
        if !legal_edge(summary.state.kind(), command.state.kind()) {
            return Err(ErrorKind::InvalidTransition);
        }
        if summary.state.kind() != TaskStateKind::Running && command.state.kind() == TaskStateKind::Running {
            summary.attempt += 1;
            summary.started_at_ms = Some(1);
        }
        summary.state_version += 1;
        summary.state = command.state.clone();
        summary.retry_not_before_ms = command.retry_not_before_ms;
        summary.cancel_requested = command.cancel_requested;
        summary.assigned_resources = command.assigned_resources.clone();
        summary.output = command.output.clone();
        if terminal(summary.state.kind()) {
            summary.finished_at_ms = Some(1);
        }
        Ok(summary.clone())
    }

    /// Returns a stable ordered page and exact continuation cursor, or a limit
    /// error.
    pub fn list(&self, query: &TaskQuery) -> Result<(Vec<TaskSummary>, Option<TaskCursor>), ErrorKind> {
        if query.limit > 256 {
            return Err(ErrorKind::InvalidRequest);
        }
        let mut rows: Vec<_> = self
            .records
            .values()
            .map(|record| record.summary.clone())
            .filter(|row| query.states.is_empty() || query.states.contains(&row.state.kind()))
            .filter(|row| {
                query
                    .correlation_key
                    .as_ref()
                    .is_none_or(|key| row.request.correlation_key.as_ref() == Some(key))
            })
            .filter(|row| query.after.is_none_or(|after| cursor(row) > after))
            .collect();
        rows.sort_by_key(cursor);
        let limit = query.limit.max(1);
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next = if more { rows.last().map(cursor) } else { None };
        Ok((rows, next))
    }

    /// Removes the oldest eligible terminal records and releases their
    /// idempotency keys.
    pub fn prune(&mut self, cutoff: u64, limit: usize) -> usize {
        let mut candidates: Vec<_> = self
            .records
            .values()
            .filter(|record| terminal(record.summary.state.kind()) && record.summary.accepted_at_ms < cutoff)
            .map(|record| cursor(&record.summary))
            .collect();
        candidates.sort();
        candidates.truncate(limit);
        for cursor in &candidates {
            self.records.remove(&cursor.id);
            self.idempotency.retain(|_, id| *id != cursor.id);
        }
        candidates.len()
    }

    /// Aggregates retained state categories without production counting
    /// helpers.
    pub fn counts(&self) -> TaskStateCounts {
        let mut counts = TaskStateCounts::default();
        for record in self.records.values() {
            match record.summary.state.kind() {
                TaskStateKind::Queued => counts.queued += 1,
                TaskStateKind::Running => counts.running += 1,
                TaskStateKind::Blocked => counts.blocked += 1,
                _ => counts.terminal += 1,
            }
        }
        counts
    }
}

/// Normalizes real wall-clock values while retaining timestamp presence
/// assertions.
pub fn stable(mut summary: TaskSummary) -> TaskSummary {
    summary.started_at_ms = summary.started_at_ms.map(|_| 1);
    summary.finished_at_ms = summary.finished_at_ms.map(|_| 1);
    summary
}
