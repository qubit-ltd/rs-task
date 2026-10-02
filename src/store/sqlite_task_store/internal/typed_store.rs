// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! SQLite persistence for the typed task API during the storage cutover.

use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::params;
use serde::Deserialize;
use serde::Serialize;

use super::super::failure;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::next::AcceptOutcome;
use crate::model::next::ProgressCommand;
use crate::model::next::ResourceRequest;
use crate::model::next::StartCommand;
use crate::model::next::StoredPayload;
use crate::model::next::StoredTask;
use crate::model::next::StoredTaskRequest;
use crate::model::next::TaskCursor;
use crate::model::next::TaskId;
use crate::model::next::TaskPage;
use crate::model::next::TaskProgressSnapshot;
use crate::model::next::TaskQuery;
use crate::model::next::TaskSummary;
use crate::store::StoreError;

const RECORD_FORMAT_VERSION: i64 = 4;

/// Lists typed task summaries in deterministic acceptance order.
pub(in crate::store::sqlite_task_store) fn list_encoded(
    connection: &Connection,
    query: TaskQuery,
) -> Result<TaskPage, StoreError> {
    let page_size = query.checked_page_size()?;
    let built = super::query_sql::build_encoded_history_query(&query, page_size)?;
    let mut statement = connection.prepare(&built.sql).map_err(failure)?;
    let mut rows = statement
        .query(rusqlite::params_from_iter(built.params))
        .map_err(failure)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(failure)? {
        let id = row.get::<_, String>(0).map_err(failure)?;
        let request_json = row.get::<_, String>(1).map_err(failure)?;
        let lifecycle_json = row.get::<_, String>(2).map_err(failure)?;
        records.push(decode_summary(id, &request_json, &lifecycle_json)?);
    }
    let has_more = records.len() > page_size;
    if has_more {
        records.truncate(page_size);
    }
    let next = has_more.then(|| records.last().map(TaskCursor::from)).flatten();
    Ok(TaskPage { records, next })
}

/// Immutable typed request fields stored separately from payload bytes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredRequestInfo {
    kind_id: String,
    category: Option<String>,
    payload_type_id: String,
    payload_schema_version: u32,
    payload_codec_id: String,
    metadata: qubit_metadata::Metadata,
    resource_limit: ResourceRequest,
    correlation_key: Option<String>,
    idempotency_key: Option<String>,
}

impl From<&StoredTaskRequest> for StoredRequestInfo {
    fn from(request: &StoredTaskRequest) -> Self {
        Self {
            kind_id: request.kind_id.clone(),
            category: request.category.clone(),
            payload_type_id: request.payload.type_id.as_str().to_owned(),
            payload_schema_version: request.payload.schema_version,
            payload_codec_id: request.payload.codec_id.clone(),
            metadata: request.metadata.clone(),
            resource_limit: request.resource_limit.clone(),
            correlation_key: request.correlation_key.clone(),
            idempotency_key: request.idempotency_key.clone(),
        }
    }
}

impl StoredRequestInfo {
    fn into_request(self, bytes: Vec<u8>) -> Result<StoredTaskRequest, StoreError> {
        let type_id = self.payload_type_id.as_str().try_into().map_err(failure)?;
        Ok(StoredTaskRequest {
            kind_id: self.kind_id,
            category: self.category,
            payload: StoredPayload {
                type_id,
                schema_version: self.payload_schema_version,
                codec_id: self.payload_codec_id,
                bytes,
            },
            metadata: self.metadata,
            resource_limit: self.resource_limit,
            correlation_key: self.correlation_key,
            idempotency_key: self.idempotency_key,
        })
    }
}

/// Mutable lifecycle values kept independent of immutable request data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredTypedLifecycle {
    state: TaskState,
    #[serde(default)]
    cancel_requested: bool,
    #[serde(default)]
    cancel_error: Option<String>,
    state_version: u64,
    attempt: u32,
    #[serde(default)]
    retry_not_before_ms: Option<u64>,
    accepted_at_ms: u64,
    started_at_ms: Option<u64>,
    finished_at_ms: Option<u64>,
    progress: Option<TaskProgressSnapshot>,
    #[serde(default)]
    output: Option<TaskOutput>,
}

/// Atomically stores one encoded request, respecting ID and idempotency keys.
///
/// Returns `true` when a row was inserted and `false` for an identical
/// idempotent request. The duplicate checks and insert share one SQLite
/// transaction, with primary-key and unique-key constraints as final guards.
pub(in crate::store::sqlite_task_store) fn accept_encoded(
    connection: &Connection,
    id: TaskId,
    request: StoredTaskRequest,
    outbox_enabled: bool,
) -> Result<AcceptOutcome, StoreError> {
    let transaction = connection.unchecked_transaction().map_err(failure)?;
    let request_info = StoredRequestInfo::from(&request);
    let request_info_json = serde_json::to_string(&request_info).map_err(failure)?;
    if let Some(key) = &request.idempotency_key {
        let existing = transaction
            .query_row(
                "SELECT id,request_info_json,payload,lifecycle_json FROM tasks WHERE idempotency_key=?1",
                [key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(failure)?;
        if let Some((stored_id, stored_info, payload, lifecycle)) = existing {
            if stored_info != request_info_json || payload != request.payload.bytes {
                return Err(StoreError::IdempotencyConflict);
            }
            let summary = decode_summary(stored_id, &stored_info, &lifecycle)?;
            transaction.commit().map_err(failure)?;
            return Ok(AcceptOutcome {
                summary,
                created: false,
            });
        }
    }
    let id_key = id.to_padded_decimal();
    let duplicate: bool = transaction
        .query_row("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)", [&id_key], |row| {
            row.get(0)
        })
        .map_err(failure)?;
    if duplicate {
        return Err(StoreError::DuplicateTask);
    }
    let accepted_at_ms = now_ms();
    let lifecycle = StoredTypedLifecycle {
        state: TaskState::Queued,
        cancel_requested: false,
        cancel_error: None,
        state_version: 0,
        attempt: 0,
        retry_not_before_ms: None,
        accepted_at_ms,
        started_at_ms: None,
        finished_at_ms: None,
        progress: None,
        output: None,
    };
    let lifecycle_json = serde_json::to_string(&lifecycle).map_err(failure)?;
    transaction
        .execute(
            "INSERT INTO tasks (id,state_kind,accepted_at,kind_id,category,payload_type_id,payload_schema_version,codec_id,correlation_key,idempotency_key,request_info_json,payload,record_format_version,lifecycle_json,attempt,retry_not_before_ms,progress_attempt,progress_version,progress_json) VALUES (?1,'Queued',?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,0,NULL,NULL,0,NULL)",
            params![
                id_key,
                i64::try_from(accepted_at_ms).map_err(failure)?,
                request.kind_id,
                request.category,
                request.payload.type_id.as_str(),
                request.payload.schema_version,
                request.payload.codec_id,
                request.correlation_key,
                request.idempotency_key,
                request_info_json,
                request.payload.bytes,
                RECORD_FORMAT_VERSION,
                lifecycle_json,
            ],
        )
        .map_err(failure)?;
    let summary = decode_summary(id_key, &request_info_json, &lifecycle_json)?;
    if outbox_enabled { insert_event_outbox(&transaction, &summary)?; }
    transaction.commit().map_err(failure)?;
    Ok(AcceptOutcome { summary, created: true })
}

/// Loads a stored typed request and its lifecycle by numeric task identity.
pub(in crate::store::sqlite_task_store) fn get_encoded_task(
    connection: &Connection,
    id: TaskId,
) -> Result<Option<StoredTask>, StoreError> {
    let id_key = id.to_padded_decimal();
    let row = connection
        .query_row(
            "SELECT id,request_info_json,payload,lifecycle_json FROM tasks WHERE id=?1",
            [&id_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(failure)?;
    row.map(|(id, request_json, payload, lifecycle_json)| {
        let info: StoredRequestInfo = serde_json::from_str(&request_json).map_err(failure)?;
        let request = info.clone().into_request(payload)?;
        let summary = decode_summary(id, &request_json, &lifecycle_json)?;
        Ok(StoredTask { request, summary })
    })
    .transpose()
}

/// Starts a queued task attempt when its state revision still matches.
pub(in crate::store::sqlite_task_store) fn start_encoded(
    connection: &Connection,
    command: StartCommand,
    outbox_enabled: bool,
) -> Result<TaskSummary, StoreError> {
    let id_key = command.id.to_padded_decimal();
    let transaction = connection.unchecked_transaction().map_err(failure)?;
    let row = transaction
        .query_row(
            "SELECT request_info_json,lifecycle_json,state_kind,state_version FROM tasks WHERE id=?1",
            [&id_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            },
        )
        .optional()
        .map_err(failure)?
        .ok_or(StoreError::NotFound)?;
    let (request_json, lifecycle_json, state_kind, state_version) = row;
    if state_kind != "Queued" || state_version != command.expected_state_version {
        return Err(StoreError::Conflict);
    }
    let mut lifecycle: StoredTypedLifecycle = serde_json::from_str(&lifecycle_json).map_err(failure)?;
    lifecycle.state = TaskState::Running;
    lifecycle.state_version = lifecycle
        .state_version
        .checked_add(1)
        .ok_or_else(|| StoreError::Failure("task state version overflow".into()))?;
    lifecycle.attempt = lifecycle
        .attempt
        .checked_add(1)
        .ok_or_else(|| StoreError::Failure("task attempt counter overflow".into()))?;
    lifecycle.started_at_ms = Some(command.started_at_ms);
    lifecycle.finished_at_ms = None;
    lifecycle.retry_not_before_ms = None;
    lifecycle.progress = None;
    let lifecycle_json = serde_json::to_string(&lifecycle).map_err(failure)?;
    let changed = transaction
        .execute(
            "UPDATE tasks SET state_kind='Running',state_version=?2,attempt=?3,started_at=?4,retry_not_before_ms=NULL,progress_attempt=NULL,progress_version=0,progress_json=NULL,lifecycle_json=?5 WHERE id=?1 AND state_kind='Queued' AND state_version=?6",
            params![
                id_key,
                lifecycle.state_version,
                lifecycle.attempt,
                i64::try_from(command.started_at_ms).map_err(failure)?,
                lifecycle_json,
                command.expected_state_version,
            ],
        )
        .map_err(failure)?;
    if changed != 1 {
        return Err(StoreError::Conflict);
    }
    let summary = decode_summary(id_key, &request_json, &lifecycle_json)?;
    if outbox_enabled { insert_event_outbox(&transaction, &summary)?; }
    transaction.commit().map_err(failure)?;
    Ok(summary)
}

/// Applies a compare-and-set lifecycle transition to an encoded task.
pub(in crate::store::sqlite_task_store) fn transition_encoded(
    connection: &Connection,
    command: crate::model::next::TransitionCommand,
    outbox_enabled: bool,
) -> Result<TaskSummary, StoreError> {
    command
        .state
        .validate_diagnostics()
        .map_err(StoreError::InvalidRequest)?;
    validate_encoded_output(&command.state, command.output.as_ref())?;
    if command.retry_not_before_ms.is_some() && command.state != TaskState::Queued {
        return Err(StoreError::InvalidRequest(
            "retry deadline is only valid for queued tasks",
        ));
    }
    let id_key = command.id.to_padded_decimal();
    let transaction = connection.unchecked_transaction().map_err(failure)?;
    let row = transaction
        .query_row(
            "SELECT request_info_json,lifecycle_json,state_version FROM tasks WHERE id=?1",
            [&id_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(failure)?
        .ok_or(StoreError::NotFound)?;
    let (request_json, lifecycle_json, state_version) = row;
    let retry_deadline = command
        .retry_not_before_ms
        .map(i64::try_from)
        .transpose()
        .map_err(failure)?;
    let mut lifecycle: StoredTypedLifecycle = serde_json::from_str(&lifecycle_json).map_err(failure)?;
    let terminal_cancel_annotation = lifecycle.state.is_terminal()
        && lifecycle.state == command.state
        && command.cancel_requested
        && command.cancel_error.is_some();
    if state_version != command.expected_state_version
        || lifecycle.attempt != command.expected_attempt
        || (!lifecycle.state.allows_transition_to(&command.state) && !terminal_cancel_annotation)
    {
        return Err(StoreError::Conflict);
    }
    if command.output.is_some() && lifecycle.state.is_terminal() {
        return Err(StoreError::InvalidRequest(
            "task output can only be written before the task becomes terminal",
        ));
    }
    lifecycle.state = command.state;
    lifecycle.state_version = lifecycle
        .state_version
        .checked_add(1)
        .ok_or_else(|| StoreError::Failure("task state version overflow".into()))?;
    lifecycle.cancel_requested = command.cancel_requested;
    lifecycle.cancel_error = command.cancel_error;
    lifecycle.retry_not_before_ms = command.retry_not_before_ms;
    if lifecycle.state.is_terminal() && command.finished_at_ms.is_some() {
        lifecycle.finished_at_ms = command.finished_at_ms;
    }
    if let Some(output) = command.output {
        lifecycle.output = Some(output);
    }
    let lifecycle_json = serde_json::to_string(&lifecycle).map_err(failure)?;
    let changed = transaction
        .execute(
            "UPDATE tasks SET state_kind=?2,state_version=?3,lifecycle_json=?4,retry_not_before_ms=?6 WHERE id=?1 AND state_version=?5",
            params![
                id_key,
                lifecycle.state.kind().as_str(),
                lifecycle.state_version,
                lifecycle_json,
                state_version,
                retry_deadline
            ],
        )
        .map_err(failure)?;
    if changed != 1 {
        return Err(StoreError::Conflict);
    }
    let summary = decode_summary(id_key, &request_json, &lifecycle_json)?;
    if outbox_enabled { insert_event_outbox(&transaction, &summary)?; }
    transaction.commit().map_err(failure)?;
    Ok(summary)
}

/// Atomically persists progress only for the current running attempt and a
/// strictly newer progress version.
pub(in crate::store::sqlite_task_store) fn update_progress(
    connection: &Connection,
    command: ProgressCommand,
) -> Result<TaskSummary, StoreError> {
    let snapshot = TaskProgressSnapshot::from_command(command.clone())
        .map_err(|_| StoreError::InvalidRequest("task progress snapshot exceeds its configured limits"))?;
    let snapshot_json = serde_json::to_string(&snapshot).map_err(failure)?;
    let id_key = command.id.to_padded_decimal();
    let transaction = connection.unchecked_transaction().map_err(failure)?;
    let row = transaction
        .query_row(
            "SELECT request_info_json,lifecycle_json,state_kind,attempt,progress_attempt,progress_version FROM tasks WHERE id=?1",
            [&id_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u32>(3)?,
                    row.get::<_, Option<u32>>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()
        .map_err(failure)?
        .ok_or(StoreError::NotFound)?;
    let (request_json, lifecycle_json, state_kind, attempt, progress_attempt, progress_version) = row;
    if state_kind != "Running" || attempt != command.expected_attempt {
        return Err(StoreError::Conflict);
    }
    let previous_version = if progress_attempt == Some(command.expected_attempt) {
        u64::try_from(progress_version).map_err(failure)?
    } else {
        0
    };
    if command.progress_version <= previous_version {
        return Err(StoreError::Conflict);
    }
    let mut lifecycle: StoredTypedLifecycle = serde_json::from_str(&lifecycle_json).map_err(failure)?;
    lifecycle.progress = Some(snapshot);
    let lifecycle_json = serde_json::to_string(&lifecycle).map_err(failure)?;
    let changed = transaction
        .execute(
            "UPDATE tasks SET progress_attempt=?2,progress_version=?3,progress_json=?4,lifecycle_json=?5 WHERE id=?1 AND state_kind='Running' AND attempt=?2 AND (progress_attempt IS NULL OR progress_attempt!=?2 OR progress_version<?3)",
            params![
                id_key,
                command.expected_attempt,
                i64::try_from(command.progress_version).map_err(failure)?,
                snapshot_json,
                lifecycle_json,
            ],
        )
        .map_err(failure)?;
    if changed != 1 {
        return Err(StoreError::Conflict);
    }
    let summary = decode_summary(id_key, &request_json, &lifecycle_json)?;
    transaction.commit().map_err(failure)?;
    Ok(summary)
}

/// Builds the public summary from its independently stored request and state.
fn decode_summary(id: String, request_json: &str, lifecycle_json: &str) -> Result<TaskSummary, StoreError> {
    let request: StoredRequestInfo = serde_json::from_str(request_json).map_err(failure)?;
    let lifecycle: StoredTypedLifecycle = serde_json::from_str(lifecycle_json).map_err(failure)?;
    let value = id.parse::<u64>().map_err(failure)?;
    let numeric_id = qubit_id::Id::new(value);
    Ok(TaskSummary {
        id: TaskId::from_id(numeric_id),
        kind_id: request.kind_id,
        category: request.category,
        payload_type_id: request.payload_type_id,
        payload_schema_version: request.payload_schema_version,
        payload_codec_id: request.payload_codec_id,
        metadata: request.metadata,
        resource_limit: request.resource_limit,
        correlation_key: request.correlation_key,
        idempotency_key: request.idempotency_key,
        state: lifecycle.state,
        cancel_requested: lifecycle.cancel_requested,
        cancel_error: lifecycle.cancel_error,
        state_version: lifecycle.state_version,
        attempt: lifecycle.attempt,
        retry_not_before_ms: lifecycle.retry_not_before_ms,
        accepted_at_ms: lifecycle.accepted_at_ms,
        started_at_ms: lifecycle.started_at_ms,
        finished_at_ms: lifecycle.finished_at_ms,
        progress: lifecycle.progress,
        output: lifecycle.output,
    })
}

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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Inserts the immutable notification before committing its lifecycle mutation.
/// Snapshots larger than 128 KiB or SQLite failures roll back the surrounding transaction.
fn insert_event_outbox(transaction: &rusqlite::Transaction<'_>, summary: &TaskSummary) -> Result<(), StoreError> {
    let event = crate::event::TaskEvent::from_typed_summary(summary);
    let json = serde_json::to_string(&event).map_err(failure)?;
    if json.len() > 128 * 1024 { return Err(StoreError::InvalidRequest("task event snapshot exceeds 128 KiB")); }
    let event_id = format!("task:{}:{}", summary.id, summary.state_version);
    // A monotonic persisted ordering key also preserves lifecycle order across clock rollback.
    transaction.execute(
        "INSERT INTO task_event_outbox(task_id,state_version,event_id,event_json,created_at_ms) VALUES (?1,?2,?3,?4,MAX(?5,COALESCE((SELECT MAX(created_at_ms) FROM task_event_outbox),?5)))",
        params![summary.id.to_padded_decimal(), summary.state_version, event_id, json, i64::try_from(now_ms()).map_err(failure)?],
    ).map_err(failure)?;
    Ok(())
}
