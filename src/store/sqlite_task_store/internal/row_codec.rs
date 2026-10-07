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

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::StoredSummaryRow;
    use super::StoredTaskRow;
    use super::decode_legacy_record;
    use super::decode_stored_summary_row;
    use super::decode_stored_task_row;
    use super::encode_lifecycle;
    use super::read_stored_summary_row;
    use super::read_stored_task_row;
    use crate::model::TaskState;
    use crate::model::legacy::TaskId;
    use crate::model::legacy::TaskRecord;
    use crate::model::legacy::TaskRequest;
    use crate::model::legacy::TaskRequestInfo;
    use crate::store::StoreError;

    fn record() -> TaskRecord {
        let mut request = TaskRequest::new("codec-test", "v1", b"payload".to_vec());
        request.correlation_key = Some("correlation".into());
        request.idempotency_key = Some("idempotency".into());
        TaskRecord {
            id: TaskId::generate(),
            request,
            state: TaskState::Queued,
            state_version: 7,
            attempt: 2,
            retry_not_before_ms: Some(1_700_000_000_123),
            accepted_at_ms: 1_700_000_000_000,
            started_at_ms: Some(1_700_000_000_100),
            finished_at_ms: None,
            assigned_resources: vec!["cpu:2".into()],
            output: None,
            cancel_requested: false,
        }
    }

    fn task_row(record: &TaskRecord) -> StoredTaskRow {
        StoredTaskRow {
            id: record.id.to_string(),
            state_kind: "Queued".into(),
            accepted_at: record.accepted_at_ms as i64,
            correlation_key: record.request.correlation_key.clone(),
            idempotency_key: record.request.idempotency_key.clone(),
            format_version: 3,
            request_info_json: serde_json::to_string(&TaskRequestInfo::from(&record.request))
                .expect("request metadata serializes"),
            payload: record.request.payload.clone(),
            lifecycle_json: encode_lifecycle(record).expect("lifecycle serializes"),
        }
    }

    fn summary_row(record: &TaskRecord) -> StoredSummaryRow {
        let row = task_row(record);
        StoredSummaryRow {
            id: row.id,
            state_kind: row.state_kind,
            accepted_at: row.accepted_at,
            correlation_key: row.correlation_key,
            idempotency_key: row.idempotency_key,
            format_version: row.format_version,
            request_info_json: row.request_info_json,
            lifecycle_json: row.lifecycle_json,
        }
    }

    #[test]
    fn test_decode_stored_task_row_round_trips_indexed_and_json_values() {
        let expected = record();
        let decoded = decode_stored_task_row(task_row(&expected)).expect("task row decodes");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_decode_stored_task_row_rejects_unsupported_format_and_corrupt_json() {
        let record = record();
        let mut unsupported = task_row(&record);
        unsupported.format_version = 99;
        assert!(
            matches!(decode_stored_task_row(unsupported), Err(StoreError::Failure(message)) if message.contains("unsupported SQLite task record format version 99"))
        );

        let mut malformed = task_row(&record);
        malformed.request_info_json = "{".into();
        assert!(matches!(decode_stored_task_row(malformed), Err(StoreError::Failure(_))));

        let mut malformed = task_row(&record);
        malformed.lifecycle_json = "{".into();
        assert!(matches!(decode_stored_task_row(malformed), Err(StoreError::Failure(_))));
    }

    #[test]
    fn test_decode_stored_task_row_rejects_indexed_state_that_disagrees_with_lifecycle() {
        let mut row = task_row(&record());
        row.state_kind = "Running".into();
        assert!(
            matches!(decode_stored_task_row(row), Err(StoreError::Failure(message)) if message.contains("disagrees with its stored JSON"))
        );
    }

    #[test]
    fn test_decode_stored_summary_row_round_trips_without_payload() {
        let source = record();
        let expected = source.summary();
        let decoded = decode_stored_summary_row(summary_row(&source)).expect("summary row decodes");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_decode_stored_summary_row_rejects_mismatched_request_key() {
        let mut row = summary_row(&record());
        row.idempotency_key = Some("different-key".into());
        assert!(
            matches!(decode_stored_summary_row(row), Err(StoreError::Failure(message)) if message.contains("disagrees with its stored JSON"))
        );
    }

    #[test]
    fn test_decode_stored_summary_row_rejects_unsupported_format() {
        let mut row = summary_row(&record());
        row.format_version = 2;
        assert!(
            matches!(decode_stored_summary_row(row), Err(StoreError::Failure(message)) if message.contains("unsupported SQLite task record format version 2"))
        );
    }

    #[test]
    fn test_row_readers_use_the_declared_sql_projection_order() {
        let record = record();
        let info = serde_json::to_string(&TaskRequestInfo::from(&record.request)).expect("metadata serializes");
        let lifecycle = encode_lifecycle(&record).expect("lifecycle serializes");
        let connection = Connection::open_in_memory().expect("in-memory database opens");
        let raw = connection
            .query_row(
                "SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9",
                rusqlite::params![
                    record.id.to_string(),
                    "Queued",
                    record.accepted_at_ms as i64,
                    record.request.correlation_key,
                    record.request.idempotency_key,
                    3_i64,
                    info,
                    record.request.payload,
                    lifecycle
                ],
                |row| read_stored_task_row(row),
            )
            .expect("complete task projection reads");
        assert_eq!(decode_stored_task_row(raw).expect("task decodes"), record);

        let info = serde_json::to_string(&TaskRequestInfo::from(&record.request)).expect("metadata serializes");
        let lifecycle = encode_lifecycle(&record).expect("lifecycle serializes");
        let summary = connection
            .query_row(
                "SELECT ?1,?2,?3,?4,?5,?6,?7,?8",
                rusqlite::params![
                    record.id.to_string(),
                    "Queued",
                    record.accepted_at_ms as i64,
                    record.request.correlation_key,
                    record.request.idempotency_key,
                    3_i64,
                    info,
                    lifecycle
                ],
                |row| read_stored_summary_row(row),
            )
            .expect("payload-free summary projection reads");
        assert_eq!(
            decode_stored_summary_row(summary).expect("summary decodes"),
            record.summary()
        );
    }

    #[test]
    fn test_decode_legacy_record_accepts_only_format_one() {
        let expected = record();
        let json = serde_json::to_string(&expected).expect("legacy record serializes");
        assert_eq!(decode_legacy_record(1, &json).expect("format one decodes"), expected);
        assert!(
            matches!(decode_legacy_record(2, &json), Err(StoreError::Failure(message)) if message.contains("unsupported legacy SQLite task record format version 2"))
        );
        assert!(matches!(decode_legacy_record(1, "{"), Err(StoreError::Failure(_))));
    }
}
