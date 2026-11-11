// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Bounded SQLite query for queued tasks whose retry deadline has elapsed.

use std::num::NonZeroUsize;

use qubit_id::Id;
use rusqlite::Connection;
use rusqlite::params;
use serde::Deserialize;
use serde::Serialize;

use super::super::failure;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::typed::ResourceRequest;
use crate::model::typed::TaskCursor;
use crate::model::typed::TaskId;
use crate::model::typed::TaskPage;
use crate::model::typed::TaskProgressSnapshot;
use crate::model::typed::TaskSummary;
use crate::store::StoreError;

const MAX_TASK_QUERY_LIMIT: usize = 256;
const MAX_READY_SAMPLE: usize = 65;

/// Lists queued task summaries in acceptance order when their retry deadline
/// is absent or no later than `now_ms`.
///
/// A bounded prefix probe chooses the acceptance-order scan when it can fill
/// a useful part of the page. Otherwise a deadline probe decides whether the
/// disjoint immediate/due branches are needed. Both checks share the query's
/// read snapshot; only selected rows have their JSON summary columns read.
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
pub(in crate::store::sqlite_task_store) fn list_ready_queued(
    connection: &Connection,
    after: Option<TaskCursor>,
    limit: NonZeroUsize,
    now_ms: u64,
) -> Result<TaskPage, StoreError> {
    if limit.get() > MAX_TASK_QUERY_LIMIT {
        return Err(StoreError::InvalidRequest(
            "ready task page limit exceeds 256",
        ));
    }
    let now = i64::try_from(now_ms)
        .map_err(|_| StoreError::InvalidRequest("task timestamp is too large"))?;
    let cursor = after
        .map(|cursor| {
            Ok::<_, StoreError>((
                i64::try_from(cursor.accepted_at_ms).map_err(|_| {
                    StoreError::InvalidRequest("task cursor timestamp is too large")
                })?,
                cursor.id.to_padded_decimal(),
            ))
        })
        .transpose()?;

    // Keep the deadline probe and selected page on the same read snapshot.
    if connection.is_autocommit() {
        let transaction = connection.unchecked_transaction().map_err(failure)?;
        let page = query_snapshot(&transaction, cursor.as_ref(), limit.get(), now)?;
        transaction.commit().map_err(failure)?;
        Ok(page)
    } else {
        query_snapshot(connection, cursor.as_ref(), limit.get(), now)
    }
}

fn query_snapshot(
    connection: &Connection,
    cursor: Option<&(i64, String)>,
    page_size: usize,
    now: i64,
) -> Result<TaskPage, StoreError> {
    let fetch = i64::try_from(page_size + 1)
        .map_err(|_| StoreError::InvalidRequest("ready task page limit is too large"))?;
    let (accepted, id) = cursor.map_or((None, None), |(accepted, id)| {
        (Some(*accepted), Some(id.as_str()))
    });
    let ready_threshold = (page_size / 16).max(1);
    let sample_limit = i64::try_from((page_size / 4).max(ready_threshold).min(MAX_READY_SAMPLE))
        .map_err(failure)?;
    let sampled_ready = sampled_ready_count(connection, now, accepted, id, sample_limit)?;
    let use_split = if sampled_ready < ready_threshold {
        has_future_deadline(connection, now)?
    } else {
        false
    };

    let sql = if use_split {
        concat!(
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
        )
    } else {
        concat!(
            "WITH page AS (SELECT id,accepted_at FROM tasks INDEXED BY tasks_queued_accepted_id ",
            "WHERE state_kind='Queued' AND (retry_not_before_ms IS NULL OR retry_not_before_ms<=?1) ",
            "AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ORDER BY accepted_at,id LIMIT ?4) ",
            "SELECT tasks.id,tasks.request_info_json,tasks.lifecycle_json FROM page ",
            "JOIN tasks ON tasks.id=page.id ORDER BY page.accepted_at,page.id"
        )
    };
    let rows = connection
        .prepare_cached(sql)
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
        .map_err(failure)?;
    let mut records = rows
        .into_iter()
        .map(|(id, request, lifecycle)| decode_summary(id, &request, &lifecycle))
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = records.len() > page_size;
    records.truncate(page_size);
    let next = has_more
        .then(|| records.last().map(TaskCursor::from))
        .flatten();
    Ok(TaskPage { records, next })
}

fn sampled_ready_count(
    connection: &Connection,
    now: i64,
    accepted: Option<i64>,
    id: Option<&str>,
    sample_limit: i64,
) -> Result<usize, StoreError> {
    let mut statement = connection
        .prepare_cached(concat!(
            "SELECT retry_not_before_ms FROM tasks INDEXED BY tasks_queued_accepted_id ",
            "WHERE state_kind='Queued' AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ",
            "ORDER BY accepted_at,id LIMIT ?4"
        ))
        .map_err(failure)?;
    let rows = statement
        .query_map(params![now, accepted, id, sample_limit], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .map_err(failure)?;
    let mut ready = 0;
    for deadline in rows {
        if deadline
            .map_err(failure)?
            .is_none_or(|deadline| deadline <= now)
        {
            ready += 1;
        }
    }
    Ok(ready)
}

fn has_future_deadline(connection: &Connection, now: i64) -> Result<bool, StoreError> {
    connection
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM tasks INDEXED BY tasks_queued_retry_deadline WHERE state_kind='Queued' AND retry_not_before_ms>?1)",
        )
        .map_err(failure)?
        .query_row([now], |row| row.get(0))
        .map_err(failure)
}

#[derive(Serialize, Deserialize)]
struct RequestInfo {
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

#[derive(Serialize, Deserialize)]
struct Lifecycle {
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

fn decode_summary(
    id: String,
    request_json: &str,
    lifecycle_json: &str,
) -> Result<TaskSummary, StoreError> {
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
    use super::*;
    use rusqlite::Connection;

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

    fn add_task(
        connection: &Connection,
        id: u64,
        accepted: u64,
        deadline: Option<u64>,
        state: &str,
    ) {
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
    fn branches_filter_deadlines_and_page_by_numeric_id() {
        let connection = connection();
        // Equal acceptance times make the numeric ID tie-breaker observable.
        add_task(&connection, 9, 10, None, "Queued");
        add_task(&connection, 2, 10, Some(100), "Queued");
        add_task(&connection, 1, 10, Some(101), "Queued");
        add_task(&connection, 5, 13, Some(101), "Queued");
        add_task(&connection, 6, 14, Some(102), "Queued");
        add_task(&connection, 4, 11, Some(100), "Queued");
        add_task(&connection, 3, 12, None, "Running");

        let limit = NonZeroUsize::new(2).unwrap();
        let first = list_ready_queued(&connection, None, limit, 100).unwrap();
        assert_eq!(
            first
                .records
                .iter()
                .map(|row| row.id.to_string())
                .collect::<Vec<_>>(),
            ["2", "9"]
        );
        assert_eq!(
            first.next,
            Some(TaskCursor::from(first.records.last().unwrap()))
        );
        let second = list_ready_queued(&connection, first.next, limit, 100).unwrap();
        assert_eq!(
            second
                .records
                .iter()
                .map(|row| row.id.to_string())
                .collect::<Vec<_>>(),
            ["4"]
        );
        assert_eq!(second.next, None);
    }

    #[test]
    fn randomized_deadline_rows_match_sorted_reference_across_pages() {
        let connection = connection();
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
            let page =
                list_ready_queued(&connection, cursor, NonZeroUsize::new(7).unwrap(), 17).unwrap();
            actual.extend(page.records.iter().map(|summary| {
                (
                    summary.accepted_at_ms,
                    summary.id.to_string().parse::<u64>().unwrap(),
                )
            }));
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
    fn empty_ready_page_has_no_next_cursor() {
        let page =
            list_ready_queued(&connection(), None, NonZeroUsize::new(256).unwrap(), 0).unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.next, None);
    }

    #[test]
    fn rejects_invalid_limits_and_unrepresentable_timestamps() {
        let connection = connection();
        assert!(matches!(
            list_ready_queued(&connection, None, NonZeroUsize::new(257).unwrap(), 0),
            Err(StoreError::InvalidRequest(
                "ready task page limit exceeds 256"
            ))
        ));
        assert!(matches!(
            list_ready_queued(&connection, None, NonZeroUsize::new(1).unwrap(), u64::MAX),
            Err(StoreError::InvalidRequest("task timestamp is too large"))
        ));
        assert!(matches!(
            list_ready_queued(
                &connection,
                Some(TaskCursor::new(u64::MAX, TaskId::from_id(Id::new(1)))),
                NonZeroUsize::new(1).unwrap(),
                0,
            ),
            Err(StoreError::InvalidRequest(
                "task cursor timestamp is too large"
            ))
        ));
    }

    #[test]
    fn future_backlog_scan_steps_stay_bounded_as_rows_grow() {
        use rusqlite::StatementStatus;

        fn vm_steps(connection: &Connection, sql: &str, params: impl rusqlite::Params) -> i32 {
            let mut statement = connection.prepare(sql).unwrap();
            let _ = statement
                .query_map(params, |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            statement.get_status(StatementStatus::VmStep)
        }

        fn sample_ready(connection: &Connection, now: i64, limit: i64) -> (usize, i32) {
            let mut statement = connection.prepare(
                "SELECT retry_not_before_ms FROM tasks INDEXED BY tasks_queued_accepted_id WHERE state_kind='Queued' ORDER BY accepted_at,id LIMIT ?1",
            ).unwrap();
            let rows = statement
                .query_map([limit], |row| row.get::<_, Option<i64>>(0))
                .unwrap();
            let mut ready = 0;
            for deadline in rows {
                if deadline.unwrap().is_none_or(|value| value <= now) {
                    ready += 1;
                }
            }
            (ready, statement.get_status(StatementStatus::VmStep))
        }

        fn future_probe(connection: &Connection, now: i64) -> (bool, i32) {
            let mut statement = connection.prepare(
                "SELECT EXISTS(SELECT 1 FROM tasks INDEXED BY tasks_queued_retry_deadline WHERE state_kind='Queued' AND retry_not_before_ms>?1)",
            ).unwrap();
            let exists = statement.query_row([now], |row| row.get(0)).unwrap();
            (exists, statement.get_status(StatementStatus::VmStep))
        }

        fn measure(size: usize) -> (i32, i32) {
            let connection = connection();
            let transaction = connection.unchecked_transaction().unwrap();
            {
                let mut insert = transaction
                    .prepare("INSERT INTO tasks(id,state_kind,accepted_at,request_info_json,lifecycle_json,retry_not_before_ms) VALUES (?1,'Queued',?2,'{}','{}',1000)")
                    .unwrap();
                for index in 0..size {
                    insert
                        .execute(params![format!("{index:020}"), index as i64])
                        .unwrap();
                }
            }
            transaction.commit().unwrap();
            let legacy_steps = vm_steps(
                &connection,
                "SELECT id FROM tasks INDEXED BY tasks_queued_accepted_id WHERE state_kind='Queued' AND (retry_not_before_ms IS NULL OR retry_not_before_ms<=?1) ORDER BY accepted_at,id LIMIT ?2",
                params![0_i64, 2_i64],
            );
            let threshold = 1;
            let (sampled_ready, sample_steps) = sample_ready(&connection, 0, 2);
            let (use_split, future_steps) = if sampled_ready < threshold {
                let (future, steps) = future_probe(&connection, 0);
                (future, steps)
            } else {
                (false, 0)
            };
            let ready_steps = vm_steps(
                &connection,
                if use_split {
                    "WITH candidates(id,accepted_at) AS (SELECT id,accepted_at FROM (SELECT id,accepted_at FROM tasks INDEXED BY tasks_queued_immediate_accepted_id WHERE state_kind='Queued' AND retry_not_before_ms IS NULL AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ORDER BY accepted_at,id LIMIT ?4) UNION ALL SELECT id,accepted_at FROM (SELECT id,accepted_at FROM tasks INDEXED BY tasks_queued_retry_deadline WHERE state_kind='Queued' AND retry_not_before_ms IS NOT NULL AND retry_not_before_ms<=?1 AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ORDER BY accepted_at,id LIMIT ?4)), page AS (SELECT id,accepted_at FROM candidates ORDER BY accepted_at,id LIMIT ?4) SELECT tasks.id FROM page JOIN tasks ON tasks.id=page.id ORDER BY page.accepted_at,page.id"
                } else {
                    "SELECT id FROM tasks INDEXED BY tasks_queued_accepted_id WHERE state_kind='Queued' AND (retry_not_before_ms IS NULL OR retry_not_before_ms<=?1) AND (?2 IS NULL OR (accepted_at,id)>(?2,?3)) ORDER BY accepted_at,id LIMIT ?4"
                },
                params![0_i64, Option::<i64>::None, Option::<String>::None, 2_i64],
            );
            (legacy_steps, sample_steps + future_steps + ready_steps)
        }

        let (small_legacy, small_split) = measure(1_000);
        let (large_legacy, large_split) = measure(100_000);
        assert!(
            large_legacy > small_legacy * 50,
            "legacy VM steps: {small_legacy} -> {large_legacy}"
        );
        assert!(
            large_split <= small_split * 3,
            "split VM steps: {small_split} -> {large_split}"
        );
    }

    #[test]
    fn reads_inside_existing_transaction_without_nested_begin() {
        let mut connection = connection();
        let transaction = connection.transaction().unwrap();
        let page = list_ready_queued(&transaction, None, NonZeroUsize::new(1).unwrap(), 0).unwrap();
        assert!(page.records.is_empty());
        transaction.rollback().unwrap();
    }
}
