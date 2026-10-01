// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use rusqlite::Result as SqliteResult;
use rusqlite::Row;

use super::super::RECORD_FORMAT_VERSION;
use super::super::failure;
use super::super::state_kind;
use super::StoredLifecycle;
use super::StoredSummaryRow;
use super::StoredTaskRow;
use crate::model::legacy::TaskRecord;
use crate::model::legacy::TaskRequest;
use crate::model::legacy::TaskRequestInfo;
use crate::model::legacy::TaskSummary;
use crate::store::StoreError;

/// Decodes the payload-free SQLite columns selected for a summary query.
///
/// # Parameters
///
/// * `row` - Current SQLite row returned by the summary projection.
///
/// # Returns
///
/// The typed row projection, or the SQLite conversion error.
///
/// # Errors
///
/// Returns a SQLite conversion error when a selected column has the wrong
/// type or cannot be decoded.
pub(in crate::store::sqlite_task_store) fn read_stored_summary_row(row: &Row<'_>) -> SqliteResult<StoredSummaryRow> {
    Ok(StoredSummaryRow {
        id: row.get(0)?,
        state_kind: row.get(1)?,
        accepted_at: row.get(2)?,
        correlation_key: row.get(3)?,
        idempotency_key: row.get(4)?,
        format_version: row.get(5)?,
        request_info_json: row.get(6)?,
        lifecycle_json: row.get(7)?,
    })
}

/// Reads every task column needed to validate a persisted row.
///
/// # Parameters
///
/// * `row` - Current SQLite row returned by the complete task projection.
///
/// # Returns
///
/// The typed task row, or the SQLite conversion error.
///
/// # Errors
///
/// Returns a SQLite conversion error when a selected column has the wrong
/// type or cannot be decoded.
pub(in crate::store::sqlite_task_store) fn read_stored_task_row(row: &Row<'_>) -> SqliteResult<StoredTaskRow> {
    Ok(StoredTaskRow {
        id: row.get(0)?,
        state_kind: row.get(1)?,
        accepted_at: row.get(2)?,
        correlation_key: row.get(3)?,
        idempotency_key: row.get(4)?,
        format_version: row.get(5)?,
        request_info_json: row.get(6)?,
        payload: row.get(7)?,
        lifecycle_json: row.get(8)?,
    })
}

/// Decodes a task record only when its persisted row format is supported.
///
/// # Parameters
///
/// * `row` - Complete typed row read from the task table.
///
/// # Returns
///
/// The decoded record after validating indexed columns against JSON values.
///
/// # Errors
///
/// Returns a store error for an unsupported format, malformed data, or
/// inconsistent indexed values.
pub(in crate::store::sqlite_task_store) fn decode_stored_task_row(
    row: StoredTaskRow,
) -> Result<TaskRecord, StoreError> {
    if row.format_version != RECORD_FORMAT_VERSION {
        return Err(StoreError::Failure(format!(
            "unsupported SQLite task record format version {}; supported version is {RECORD_FORMAT_VERSION}",
            row.format_version
        )));
    }
    let request_info: TaskRequestInfo = serde_json::from_str(&row.request_info_json).map_err(failure)?;
    let request = TaskRequest {
        task_type: request_info.task_type,
        handler_version: request_info.handler_version,
        payload: row.payload,
        resources: request_info.resources,
        correlation_key: request_info.correlation_key,
        idempotency_key: request_info.idempotency_key,
        metadata: request_info.metadata,
    };
    let lifecycle: StoredLifecycle = serde_json::from_str(&row.lifecycle_json).map_err(failure)?;
    let record = lifecycle.into_record(request);
    if record.id.to_string() != row.id
        || state_kind(&record.state) != row.state_kind
        || i64::try_from(record.accepted_at_ms).map_err(failure)? != row.accepted_at
        || record.request.correlation_key != row.correlation_key
        || record.request.idempotency_key != row.idempotency_key
    {
        return Err(StoreError::Failure(format!(
            "SQLite task row `{}` disagrees with its stored JSON",
            row.id
        )));
    }
    Ok(record)
}

/// Decodes lifecycle and immutable metadata without querying request payload.
///
/// # Parameters
///
/// * `row` - Indexed row projection returned by a summary query.
///
/// # Returns
///
/// A payload-free task summary matching every indexed column.
///
/// # Errors
///
/// Returns an error for unsupported formats, malformed JSON, or inconsistent
/// indexed and serialized values.
pub(in crate::store::sqlite_task_store) fn decode_stored_summary_row(
    row: StoredSummaryRow,
) -> Result<TaskSummary, StoreError> {
    if row.format_version != RECORD_FORMAT_VERSION {
        return Err(StoreError::Failure(format!(
            "unsupported SQLite task record format version {}; supported version is {RECORD_FORMAT_VERSION}",
            row.format_version
        )));
    }
    let request: TaskRequestInfo = serde_json::from_str(&row.request_info_json).map_err(failure)?;
    let lifecycle: StoredLifecycle = serde_json::from_str(&row.lifecycle_json).map_err(failure)?;
    let record = lifecycle.into_summary(request);
    if record.id.to_string() != row.id
        || state_kind(&record.state) != row.state_kind
        || i64::try_from(record.accepted_at_ms).map_err(failure)? != row.accepted_at
        || record.request.correlation_key != row.correlation_key
        || record.request.idempotency_key != row.idempotency_key
    {
        return Err(StoreError::Failure(format!(
            "SQLite task row `{}` disagrees with its stored JSON",
            row.id
        )));
    }
    Ok(record)
}

/// Serializes only the lifecycle fields that can change after acceptance.
///
/// # Parameters
///
/// * `record` - Complete record whose lifecycle is serialized.
///
/// # Returns
///
/// JSON containing the mutable lifecycle values.
///
/// # Errors
///
/// Returns a store error if serialization fails.
pub(in crate::store::sqlite_task_store) fn encode_lifecycle(record: &TaskRecord) -> Result<String, StoreError> {
    serde_json::to_string(&StoredLifecycle::from_record(record)).map_err(failure)
}

/// Serializes lifecycle fields from a payload-free task summary.
///
/// # Parameters
///
/// * `record` - Summary whose mutable lifecycle fields are encoded.
///
/// # Returns
///
/// JSON containing only lifecycle values.
///
/// # Errors
///
/// Returns an error if the lifecycle cannot be serialized.
#[cfg_attr(test, allow(dead_code))]
pub(in crate::store::sqlite_task_store) fn encode_summary_lifecycle(
    record: &TaskSummary,
) -> Result<String, StoreError> {
    serde_json::to_string(&StoredLifecycle::from_summary(record)).map_err(failure)
}

/// Decodes a pre schema 2 record stored as a single JSON object.
///
/// # Parameters
///
/// * `format_version` - Legacy serialized record format version.
/// * `json` - Serialized complete task record.
///
/// # Returns
///
/// The decoded legacy task record.
///
/// # Errors
///
/// Returns a store error when the format is unsupported or JSON is invalid.
pub(in crate::store::sqlite_task_store) fn decode_legacy_record(
    format_version: i64,
    json: &str,
) -> Result<TaskRecord, StoreError> {
    if format_version != 1 {
        return Err(StoreError::Failure(format!(
            "unsupported legacy SQLite task record format version {format_version}"
        )));
    }
    serde_json::from_str(json).map_err(failure)
}
