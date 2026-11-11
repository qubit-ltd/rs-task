// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded SQLite query for queued tasks whose retry deadline has elapsed.

mod lifecycle;
mod request_info;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_id::Id;
use rusqlite::Connection;
use rusqlite::ErrorCode;
use rusqlite::params;

use self::lifecycle::Lifecycle;
use self::request_info::RequestInfo;
use super::super::failure;
use crate::model::typed::TaskCursor;
use crate::model::typed::TaskId;
use crate::model::typed::TaskPage;
use crate::model::typed::TaskSummary;
use crate::store::StoreError;

/// Maximum page size accepted by the ready-task query.
const MAX_TASK_QUERY_LIMIT: usize = 256;
/// SQLite reports VM progress every 1,000 instructions while the acceptance
/// scan is attempted.
const ACCEPTANCE_PROGRESS_INTERVAL: i32 = 1_000;
/// Bounds the acceptance-order attempt to roughly 2,000 VM instructions.
const MAX_ACCEPTANCE_PROGRESS_CALLBACKS: usize = 2;

/// Reusable per-connection state for bounded ready-query attempts.
#[derive(Clone)]
pub(in crate::store::sqlite_task_store) struct ReadyQuery {
    /// Enables the VM budget only while an acceptance-order scan is active.
    active: Arc<AtomicBool>,
    /// Counts progress callbacks for the active acceptance-order scan.
    progress_calls: Arc<AtomicUsize>,
}

impl ReadyQuery {
    /// Installs a reusable SQLite progress callback on a connection.
    ///
    /// # Parameters
    ///
    /// - `connection`: SQLite connection on which to install the callback.
    ///
    /// # Returns
    ///
    /// The state used to enable bounded acceptance-order attempts.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite cannot install the callback.
    pub(in crate::store::sqlite_task_store) fn install(connection: &mut Connection) -> Result<Self, StoreError> {
        let active = Arc::new(AtomicBool::new(false));
        let progress_calls = Arc::new(AtomicUsize::new(0));
        let callback_active = Arc::clone(&active);
        let callback_calls = Arc::clone(&progress_calls);
        connection
            .progress_handler(
                ACCEPTANCE_PROGRESS_INTERVAL,
                Some(move || {
                    callback_active.load(Ordering::Relaxed)
                        && callback_calls.fetch_add(1, Ordering::Relaxed) + 1 >= MAX_ACCEPTANCE_PROGRESS_CALLBACKS
                }),
            )
            .map_err(failure)?;
        Ok(Self { active, progress_calls })
    }

    /// Resets the progress count and enables the scan budget.
    fn start(&self) {
        self.progress_calls.store(0, Ordering::Relaxed);
        self.active.store(true, Ordering::Relaxed);
    }

    /// Disables the scan budget after the acceptance query completes.
    fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

const READY_ACCEPTANCE_QUERY: &str = concat!(
    "SELECT id,request_info_json,lifecycle_json FROM tasks INDEXED BY tasks_queued_accepted_id ",
    "WHERE state_kind='Queued' AND (retry_not_before_ms IS NULL OR retry_not_before_ms<=?1) ",
    "AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ORDER BY accepted_at,id LIMIT ?4"
);

/// Lists queued task summaries in acceptance order when their retry deadline
/// is absent or no later than `now_ms`.
///
/// Attempts the acceptance-order scan with a SQLite VM budget. If the budget
/// interrupts that statement, retries the page with the retry-deadline index.
///
/// # Parameters
///
/// - `connection`: SQLite connection containing task rows.
/// - `after`: Optional exclusive acceptance-order cursor.
/// - `limit`: Maximum number of ready summaries to return.
/// - `now_ms`: Current time used to test retry eligibility.
///
/// # Returns
///
/// Returns the ready page and a continuation cursor when a lookahead row
/// confirms additional results.
///
/// # Errors
///
/// Returns `InvalidRequest` for unrepresentable timestamps or limits above
/// 256, and `Failure` for SQL errors or invalid stored summary JSON.
#[cfg(test)]
fn list_ready_queued(
    connection: &mut Connection,
    after: Option<TaskCursor>,
    limit: NonZeroUsize,
    now_ms: u64,
) -> Result<TaskPage, StoreError> {
    let budget = ReadyQuery::install(connection)?;
    list_ready_queued_with_budget(connection, after, limit, now_ms, &budget)
}

/// Lists ready queued tasks using the connection's reusable VM budget.
///
/// # Parameters
///
/// - `connection`: SQLite connection containing task rows.
/// - `after`: Optional exclusive acceptance-order cursor.
/// - `limit`: Maximum number of ready summaries to return.
/// - `now_ms`: Current time used to test retry eligibility.
/// - `budget`: Reusable VM budget callback state installed for this connection.
///
/// # Returns
///
/// The ready page and a continuation cursor when additional results exist.
///
/// # Errors
///
/// Returns `InvalidRequest` for invalid limits or timestamps and `Failure`
/// for SQLite or stored summary decoding errors.
pub(in crate::store::sqlite_task_store) fn list_ready_queued_with_budget(
    connection: &mut Connection,
    after: Option<TaskCursor>,
    limit: NonZeroUsize,
    now_ms: u64,
    budget: &ReadyQuery,
) -> Result<TaskPage, StoreError> {
    if limit.get() > MAX_TASK_QUERY_LIMIT {
        return Err(StoreError::InvalidRequest("ready task page limit exceeds 256"));
    }
    let now = i64::try_from(now_ms).map_err(|_| StoreError::InvalidRequest("task timestamp is too large"))?;
    let cursor = after
        .map(|cursor| {
            Ok::<_, StoreError>((
                i64::try_from(cursor.accepted_at_ms)
                    .map_err(|_| StoreError::InvalidRequest("task cursor timestamp is too large"))?,
                cursor.id.to_padded_decimal(),
            ))
        })
        .transpose()?;

    query_snapshot(connection, cursor.as_ref(), limit.get(), now, budget)
}

/// Attempts an acceptance-order read within a VM budget, then falls back to
/// the deadline-index split query if the budget interrupts that read.
///
/// # Parameters
///
/// - `connection`: SQLite connection containing task rows.
/// - `cursor`: Optional exclusive acceptance-order key.
/// - `page_size`: Maximum number of summaries to return.
/// - `now`: SQLite-representable retry deadline cutoff.
///
/// # Returns
///
/// The ordered ready page and a continuation cursor when a lookahead row
/// exists.
///
/// # Errors
///
/// Returns an error if SQLite fails or persisted summary data is invalid.
fn query_snapshot(
    connection: &mut Connection,
    cursor: Option<&(i64, String)>,
    page_size: usize,
    now: i64,
    budget: &ReadyQuery,
) -> Result<TaskPage, StoreError> {
    let fetch =
        i64::try_from(page_size + 1).map_err(|_| StoreError::InvalidRequest("ready task page limit is too large"))?;
    let (accepted, id) = cursor.map_or((None, None), |(accepted, id)| (Some(*accepted), Some(id.as_str())));
    budget.start();
    let acceptance_result = query_acceptance_rows(connection, now, accepted, id, fetch);
    budget.stop();
    let page_rows = match acceptance_result {
        Ok(rows) => rows,
        Err(rusqlite::Error::SqliteFailure(error, _)) if error.code == ErrorCode::OperationInterrupted => {
            query_split_rows(connection, now, accepted, id, fetch)?
        }
        Err(error) => return Err(failure(error)),
    };
    page_from_rows(page_rows, page_size)
}

/// Reads a page in acceptance order while filtering by retry eligibility.
///
/// # Parameters
///
/// - `connection`: SQLite connection containing task rows.
/// - `now`: Retry deadline cutoff.
/// - `accepted`: Optional cursor timestamp.
/// - `id`: Optional cursor task ID.
/// - `fetch`: Page size including the lookahead row.
///
/// # Returns
///
/// The ordered page rows, or a SQLite error including VM-budget interruption.
fn query_acceptance_rows(
    connection: &Connection,
    now: i64,
    accepted: Option<i64>,
    id: Option<&str>,
    fetch: i64,
) -> Result<Vec<(String, String, String)>, rusqlite::Error> {
    connection
        .prepare_cached(READY_ACCEPTANCE_QUERY)?
        .query_map(params![now, accepted, id, fetch], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
}

/// Reads the same ready page through disjoint immediate and due indexes.
///
/// # Parameters
///
/// - `connection`: SQLite connection containing task rows.
/// - `now`: Retry deadline cutoff.
/// - `accepted`: Optional cursor timestamp.
/// - `id`: Optional cursor task ID.
/// - `fetch`: Page size including the lookahead row.
///
/// # Returns
///
/// Ready rows in acceptance order, or a storage error.
fn query_split_rows(
    connection: &Connection,
    now: i64,
    accepted: Option<i64>,
    id: Option<&str>,
    fetch: i64,
) -> Result<Vec<(String, String, String)>, StoreError> {
    connection
        .prepare_cached(concat!(
            "WITH candidates(id,accepted_at) AS (",
            "SELECT id,accepted_at FROM (SELECT id,accepted_at FROM tasks INDEXED BY tasks_queued_immediate_accepted_id ",
            "WHERE state_kind='Queued' AND retry_not_before_ms IS NULL AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ",
            "ORDER BY accepted_at,id LIMIT ?4) ",
            "UNION ALL ",
            "SELECT id,accepted_at FROM (SELECT id,accepted_at FROM tasks INDEXED BY tasks_queued_retry_deadline ",
            "WHERE state_kind='Queued' AND retry_not_before_ms IS NOT NULL AND retry_not_before_ms<=?1 ",
            "AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ORDER BY accepted_at,id LIMIT ?4) ",
            "), page AS (SELECT id,accepted_at FROM candidates ORDER BY accepted_at,id LIMIT ?4) ",
            "SELECT tasks.id,tasks.request_info_json,tasks.lifecycle_json FROM page ",
            "JOIN tasks ON tasks.id=page.id ORDER BY page.accepted_at,page.id"
        ))
        .map_err(failure)?
        .query_map(params![now, accepted, id, fetch], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)
}

/// Decodes fetched rows and applies page truncation and cursor construction.
///
/// # Parameters
///
/// - `rows`: Ordered summary columns, with at most one lookahead row.
/// - `page_size`: Maximum number of records to return.
///
/// # Returns
///
/// A task page and continuation cursor when another row exists.
///
/// # Errors
///
/// Returns an error if stored summary fields are invalid.
fn page_from_rows(rows: Vec<(String, String, String)>, page_size: usize) -> Result<TaskPage, StoreError> {
    let mut records = decode_rows(rows)?;
    let has_more = records.len() > page_size;
    records.truncate(page_size);
    let next = has_more.then(|| records.last().map(TaskCursor::from)).flatten();
    Ok(TaskPage { records, next })
}

/// Decodes each row's summary columns.
///
/// # Parameters
///
/// - `rows`: Task ID, request metadata JSON, and lifecycle JSON tuples.
///
/// # Returns
///
/// The decoded task summaries.
///
/// # Errors
///
/// Returns an error if any stored summary field is invalid.
fn decode_rows(rows: Vec<(String, String, String)>) -> Result<Vec<TaskSummary>, StoreError> {
    rows.into_iter()
        .map(|(id, request, lifecycle)| decode_summary(id, &request, &lifecycle))
        .collect()
}

/// Decodes summary columns without reading the encoded payload bytes.
///
/// # Parameters
///
/// - `id`: Padded numeric task ID read from SQLite.
/// - `request_json`: Stored request metadata JSON.
/// - `lifecycle_json`: Stored lifecycle JSON.
///
/// # Returns
///
/// A task summary assembled from the stored summary columns.
///
/// # Errors
///
/// Returns an error if either JSON value or the task ID is invalid.
fn decode_summary(id: String, request_json: &str, lifecycle_json: &str) -> Result<TaskSummary, StoreError> {
    let request: RequestInfo = serde_json::from_str(request_json).map_err(failure)?;
    let lifecycle: Lifecycle = serde_json::from_str(lifecycle_json).map_err(failure)?;
    let numeric_id = id.parse::<u64>().map_err(failure)?;
    Ok(TaskSummary {
        id: TaskId::from_id(Id::new(numeric_id)),
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::Ordering;

    use qubit_id::Id;
    use rusqlite::Connection;
    use rusqlite::params;

    use super::Lifecycle;
    use super::MAX_ACCEPTANCE_PROGRESS_CALLBACKS;
    use super::ReadyQuery;
    use super::RequestInfo;
    use super::list_ready_queued;
    use super::list_ready_queued_with_budget;
    use crate::model::TaskState;
    use crate::model::typed::ResourceRequest;
    use crate::model::typed::TaskCursor;
    use crate::model::typed::TaskId;
    use crate::store::StoreError;

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(
            "CREATE TABLE tasks (
                id TEXT PRIMARY KEY, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL,
                request_info_json TEXT NOT NULL, lifecycle_json TEXT NOT NULL,
                retry_not_before_ms INTEGER
             );
             CREATE INDEX tasks_queued_accepted_id ON tasks(accepted_at,id) WHERE state_kind='Queued';
             CREATE INDEX tasks_queued_immediate_accepted_id ON tasks(accepted_at,id) WHERE state_kind='Queued' AND retry_not_before_ms IS NULL;
             CREATE INDEX tasks_queued_retry_deadline ON tasks(retry_not_before_ms) WHERE state_kind='Queued' AND retry_not_before_ms IS NOT NULL;",
        )
        .unwrap();
        connection
    }

    fn add_task(connection: &Connection, id: u64, accepted: u64, deadline: Option<u64>, state: &str) {
        let request = RequestInfo {
            kind_id: "ready.test".into(),
            category: None,
            payload_type_id: "test.Payload".into(),
            payload_schema_version: 1,
            payload_codec_id: "test.codec".into(),
            metadata: qubit_metadata::Metadata::default(),
            resource_limit: ResourceRequest::default(),
            correlation_key: None,
            idempotency_key: None,
        };
        let lifecycle = Lifecycle {
            state: if state == "Queued" {
                TaskState::Queued
            } else {
                TaskState::Running
            },
            cancel_requested: false,
            cancel_error: None,
            state_version: 0,
            attempt: 0,
            retry_not_before_ms: deadline,
            accepted_at_ms: accepted,
            started_at_ms: None,
            finished_at_ms: None,
            progress: None,
            output: None,
        };
        connection.execute(
            "INSERT INTO tasks(id,state_kind,accepted_at,request_info_json,lifecycle_json,retry_not_before_ms) VALUES (?1,?2,?3,?4,?5,?6)",
            params![format!("{id:020}"), state, accepted as i64, serde_json::to_string(&request).unwrap(), serde_json::to_string(&lifecycle).unwrap(), deadline.map(|value| value as i64)],
        ).unwrap();
    }

    #[test]
    fn test_filters_deadlines_and_pages_by_numeric_id() {
        let mut connection = connection();
        // Equal acceptance times make the numeric ID tie-breaker observable.
        add_task(&connection, 9, 10, None, "Queued");
        add_task(&connection, 2, 10, Some(100), "Queued");
        add_task(&connection, 1, 10, Some(101), "Queued");
        add_task(&connection, 5, 13, Some(101), "Queued");
        add_task(&connection, 6, 14, Some(102), "Queued");
        add_task(&connection, 4, 11, Some(100), "Queued");
        add_task(&connection, 3, 12, None, "Running");

        let limit = NonZeroUsize::new(2).unwrap();
        let first = list_ready_queued(&mut connection, None, limit, 100).unwrap();
        assert_eq!(
            first.records.iter().map(|row| row.id.to_string()).collect::<Vec<_>>(),
            ["2", "9"]
        );
        assert_eq!(first.next, Some(TaskCursor::from(first.records.last().unwrap())));
        let second = list_ready_queued(&mut connection, first.next, limit, 100).unwrap();
        assert_eq!(
            second.records.iter().map(|row| row.id.to_string()).collect::<Vec<_>>(),
            ["4"]
        );
        assert_eq!(second.next, None);
    }

    #[test]
    fn test_randomized_deadline_rows_match_sorted_reference_across_pages() {
        let mut connection = connection();
        let mut expected = Vec::new();
        for index in 0..80_u64 {
            let id = ((index * 37) % 81) + 1;
            let accepted = index / 4;
            let deadline = match index % 4 {
                0 | 1 => Some(18),
                2 => Some(17),
                _ => None,
            };
            let state = if index % 13 == 0 { "Running" } else { "Queued" };
            add_task(&connection, id, accepted, deadline, state);
            if state == "Queued" && deadline.is_none_or(|value| value <= 17) {
                expected.push((accepted, id));
            }
        }
        expected.sort_unstable();

        let mut actual = Vec::new();
        let mut cursor = None;
        loop {
            let page = list_ready_queued(&mut connection, cursor, NonZeroUsize::new(7).unwrap(), 17).unwrap();
            actual.extend(
                page.records
                    .iter()
                    .map(|summary| (summary.accepted_at_ms, summary.id.to_string().parse::<u64>().unwrap())),
            );
            let has_more = page.records.len() == 7 && page.next.is_some();
            cursor = page.next;
            if !has_more {
                assert_eq!(cursor, None);
                break;
            }
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_empty_ready_page_has_no_next_cursor() {
        let page = list_ready_queued(&mut connection(), None, NonZeroUsize::new(256).unwrap(), 0).unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.next, None);
    }

    #[test]
    fn test_rejects_invalid_limits_and_unrepresentable_timestamps() {
        let mut connection = connection();
        assert!(matches!(
            list_ready_queued(&mut connection, None, NonZeroUsize::new(257).unwrap(), 0),
            Err(StoreError::InvalidRequest("ready task page limit exceeds 256"))
        ));
        assert!(matches!(
            list_ready_queued(&mut connection, None, NonZeroUsize::new(1).unwrap(), u64::MAX),
            Err(StoreError::InvalidRequest("task timestamp is too large"))
        ));
        assert!(matches!(
            list_ready_queued(
                &mut connection,
                Some(TaskCursor::new(u64::MAX, TaskId::from_id(Id::new(1)))),
                NonZeroUsize::new(1).unwrap(),
                0,
            ),
            Err(StoreError::InvalidRequest("task cursor timestamp is too large"))
        ));
    }

    #[test]
    fn test_future_backlog_scan_steps_stay_bounded_as_rows_grow() {
        fn measure(size: usize) -> usize {
            let mut connection = connection();
            let transaction = connection.unchecked_transaction().unwrap();
            {
                let mut insert = transaction
                    .prepare("INSERT INTO tasks(id,state_kind,accepted_at,request_info_json,lifecycle_json,retry_not_before_ms) VALUES (?1,'Queued',?2,'{}','{}',1000)")
                    .unwrap();
                for index in 0..size {
                    insert.execute(params![format!("{index:020}"), index as i64]).unwrap();
                }
            }
            transaction.commit().unwrap();
            let budget = ReadyQuery::install(&mut connection).unwrap();
            let page = list_ready_queued_with_budget(&mut connection, None, NonZeroUsize::new(1).unwrap(), 0, &budget)
                .unwrap();
            assert!(page.records.is_empty());
            budget.progress_calls.load(Ordering::Relaxed)
        }

        let small_calls = measure(1_000);
        let large_calls = measure(100_000);
        assert_eq!(small_calls, MAX_ACCEPTANCE_PROGRESS_CALLBACKS);
        assert_eq!(large_calls, small_calls);
    }

    #[test]
    fn test_sparse_ready_prefix_uses_split_query() {
        let mut connection = connection();
        for index in 0..10_000_u64 {
            let deadline = if index == 0 { Some(0) } else { Some(i64::MAX as u64) };
            add_task(&connection, index, index, deadline, "Queued");
        }

        let page = list_ready_queued(&mut connection, None, NonZeroUsize::new(32).unwrap(), 1).unwrap();
        assert_eq!(page.records.len(), 1);
    }

    #[test]
    fn test_dense_ready_prefix_returns_without_unbounded_scan() {
        let mut connection = connection();
        for index in 0..1_000_u64 {
            let deadline = if index % 4 == 0 { Some(0) } else { Some(i64::MAX as u64) };
            add_task(&connection, index, index, deadline, "Queued");
        }

        let page = list_ready_queued(&mut connection, None, NonZeroUsize::new(32).unwrap(), 1).unwrap();
        assert_eq!(page.records.len(), 32);
    }

    #[test]
    fn test_reads_inside_existing_transaction_without_nested_begin() {
        let mut connection = connection();
        connection.execute_batch("BEGIN").unwrap();
        let page = list_ready_queued(&mut connection, None, NonZeroUsize::new(1).unwrap(), 0).unwrap();
        assert!(page.records.is_empty());
        connection.execute_batch("ROLLBACK").unwrap();
    }
}
