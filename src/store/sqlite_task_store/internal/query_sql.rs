// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Builds parameterized payload-free SQLite queries.

use rusqlite::types::Value;

use super::super::SUMMARY_COLUMNS;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskQuery;
use crate::store::StoreError;

/// SQL text and bound values for one SQLite query.
pub(in crate::store::sqlite_task_store) struct QuerySql {
    /// Internal SQL fragments with numbered placeholders.
    pub(in crate::store::sqlite_task_store) sql: String,
    /// Values in the order of their one-based placeholders.
    pub(in crate::store::sqlite_task_store) params: Vec<Value>,
}

/// Builds a history query for a normalized page size.
///
/// Binds all cursor/filter values and the lookahead limit. Returns
/// `InvalidRequest` when the timestamp or limit exceeds SQLite's integer
/// domain.
pub(in crate::store::sqlite_task_store) fn build_history_query(
    query: &TaskQuery,
    page_size: usize,
) -> Result<QuerySql, StoreError> {
    let fetch_limit = page_size
        .checked_add(1)
        .and_then(|limit| i64::try_from(limit).ok())
        .ok_or(StoreError::InvalidRequest("task history page limit is too large"))?;
    let mut built = QuerySql {
        sql: format!("SELECT {SUMMARY_COLUMNS} FROM tasks"),
        params: Vec::new(),
    };
    let mut has_predicate = false;
    if let Some(after) = query.after {
        append_cursor(
            &mut built,
            &mut has_predicate,
            after,
            "task history cursor timestamp is too large",
        )?;
    }
    if let Some(key) = &query.correlation_key {
        append_predicate(&mut built.sql, &mut has_predicate, "correlation_key = ");
        let placeholder = bind_text(&mut built.params, key.clone());
        built.sql.push_str(&placeholder);
    }
    let mut states = Vec::new();
    for state in &query.states {
        if !states.contains(state) {
            states.push(*state);
        }
    }
    if !states.is_empty() {
        append_predicate(&mut built.sql, &mut has_predicate, "state_kind IN (");
        for (index, state) in states.iter().enumerate() {
            if index != 0 {
                built.sql.push(',');
            }
            let placeholder = bind_text(&mut built.params, state.as_str().into());
            built.sql.push_str(&placeholder);
        }
        built.sql.push(')');
    }
    let limit = bind_value(&mut built.params, Value::Integer(fetch_limit));
    built.sql.push_str(&format!(" ORDER BY accepted_at, id LIMIT {limit}"));
    Ok(built)
}

/// Builds the unfinished-task page using the partial index's literal predicate.
///
/// `after` is an exclusive cursor; `None` omits the lower bound. Returns
/// `InvalidRequest` when the timestamp exceeds SQLite's integer domain.
pub(in crate::store::sqlite_task_store) fn build_recovery_query(
    after: Option<TaskCursor>,
) -> Result<QuerySql, StoreError> {
    let mut built = QuerySql {
        sql: format!("SELECT {SUMMARY_COLUMNS} FROM tasks"),
        params: Vec::new(),
    };
    let mut has_predicate = false;
    // Unary plus preserves TEXT membership while excluding unordered probes
    // of the full state indexes. The partial index uses this exact predicate.
    append_predicate(
        &mut built.sql,
        &mut has_predicate,
        "+state_kind IN ('Queued','Running')",
    );
    if let Some(after) = after {
        append_cursor(
            &mut built,
            &mut has_predicate,
            after,
            "recovery cursor timestamp exceeds the SQLite integer range",
        )?;
    }
    built.sql.push_str(" ORDER BY accepted_at, id LIMIT 257");
    Ok(built)
}

/// Appends only an internal constant predicate prefix with WHERE/AND
/// punctuation.
fn append_predicate(sql: &mut String, has_predicate: &mut bool, fragment: &'static str) {
    sql.push_str(if *has_predicate { " AND " } else { " WHERE " });
    sql.push_str(fragment);
    *has_predicate = true;
}

/// Appends a bound tuple cursor, returning the supplied overflow diagnostic.
fn append_cursor(
    built: &mut QuerySql,
    has_predicate: &mut bool,
    cursor: TaskCursor,
    overflow_message: &'static str,
) -> Result<(), StoreError> {
    let time = bind_timestamp(&mut built.params, cursor.accepted_at_ms, overflow_message)?;
    let id = bind_task_id(&mut built.params, cursor.id);
    append_predicate(&mut built.sql, has_predicate, "(accepted_at, id) > (");
    built.sql.push_str(&format!("{time}, {id})"));
    Ok(())
}

/// Binds one value and returns its one-based numbered placeholder.
fn bind_value(params: &mut Vec<Value>, value: Value) -> String {
    params.push(value);
    format!("?{}", params.len())
}

/// Checks a timestamp's SQLite integer domain and binds it, or returns
/// InvalidRequest.
fn bind_timestamp(
    params: &mut Vec<Value>,
    timestamp: u64,
    overflow_message: &'static str,
) -> Result<String, StoreError> {
    let timestamp = i64::try_from(timestamp).map_err(|_| StoreError::InvalidRequest(overflow_message))?;
    Ok(bind_value(params, Value::Integer(timestamp)))
}

/// Binds the task ID's existing UUID text representation.
fn bind_task_id(params: &mut Vec<Value>, id: TaskId) -> String {
    bind_text(params, id.to_string())
}

/// Binds owned text without inserting it into the SQL string.
fn bind_text(params: &mut Vec<Value>, value: String) -> String {
    bind_value(params, Value::Text(value))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use rusqlite::params_from_iter;
    use rusqlite::types::Value;

    use super::super::schema::initialize_schema;
    use super::QuerySql;
    use super::build_history_query;
    use super::build_recovery_query;
    use crate::model::TaskCursor;
    use crate::model::TaskQuery;
    use crate::model::TaskStateKind;
    use crate::store::StoreError;

    /// Creates indexed, representative terminal history with sparse unfinished
    /// rows.
    fn query_database() -> Connection {
        let mut connection = Connection::open_in_memory().expect("temporary database opens");
        initialize_schema(&mut connection).expect("schema initializes");
        connection.execute_batch(
            "WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<2000)
             INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,request_info_json,payload,lifecycle_json)
             SELECT printf('00000000-0000-0000-0000-%012x',n),CASE WHEN n%100=0 THEN 'Queued' WHEN n%100=1 THEN 'Running' ELSE 'Succeeded' END,
                    n,printf('key-%d',n%11),'{}',X'','{}' FROM fixture;
             ANALYZE;"
        ).expect("plan dataset creates");
        connection
    }

    /// Runs EXPLAIN on the actual builder output and bound values.
    fn explain(connection: &Connection, built: QuerySql) -> Vec<String> {
        connection
            .prepare(&format!("EXPLAIN QUERY PLAN {}", built.sql))
            .expect("query plan prepares")
            .query_map(params_from_iter(built.params), |row| row.get::<_, String>(3))
            .expect("query plan runs")
            .collect::<Result<Vec<_>, _>>()
            .expect("plan details read")
    }

    /// Requires the chosen simple shape to seek its intended compound index.
    fn assert_search(plan: &[String], index: &str) {
        assert!(
            plan.iter()
                .any(|line| line.contains("SEARCH tasks USING INDEX") && line.contains(index)),
            "query must SEARCH {index}: {plan:?}"
        );
    }

    /// Uses a deep bound with the real UUID storage encoding.
    fn deep_cursor() -> TaskCursor {
        TaskCursor {
            accepted_at_ms: 1800,
            id: serde_json::from_str("\"ffffffff-ffff-ffff-ffff-ffffffffffff\"").expect("fixed cursor ID decodes"),
        }
    }

    /// Requires a deep history cursor to seek through the acceptance index.
    #[test]
    fn test_query_sql_deep_cursor_uses_search() {
        let connection = query_database();
        let query = TaskQuery {
            after: Some(deep_cursor()),
            limit: 32,
            ..TaskQuery::default()
        };
        assert_search(
            &explain(&connection, build_history_query(&query, 32).expect("query builds")),
            "tasks_accepted_id",
        );
    }

    /// Correlation and one-state cursor queries use their corresponding
    /// indexes.
    #[test]
    fn test_query_sql_filtered_deep_cursor_uses_search() {
        let connection = query_database();
        let correlation = TaskQuery {
            after: Some(deep_cursor()),
            correlation_key: Some("key-4".into()),
            limit: 32,
            ..TaskQuery::default()
        };
        assert_search(
            &explain(
                &connection,
                build_history_query(&correlation, 32).expect("correlation query builds"),
            ),
            "tasks_correlation_accepted_id",
        );
        let state = TaskQuery {
            after: Some(deep_cursor()),
            states: vec![TaskStateKind::Running],
            limit: 32,
            ..TaskQuery::default()
        };
        assert_search(
            &explain(
                &connection,
                build_history_query(&state, 32).expect("state query builds"),
            ),
            "tasks_state_accepted_id",
        );
    }

    /// Recovery seeks the sparse unfinished partial index and retains exact
    /// encodings.
    #[test]
    fn test_query_sql_recovery_uses_partial_index() {
        let connection = recovery_database_without_statistics(2000, 0);
        let built = build_recovery_query(Some(deep_cursor())).expect("recovery query builds");
        assert!(built.sql.contains("state_kind IN ('Queued','Running')"));
        assert_eq!(TaskStateKind::Queued.as_str(), "Queued");
        assert_eq!(TaskStateKind::Running.as_str(), "Running");
        assert_search(&explain(&connection, built), "tasks_unfinished_accepted_id");
    }

    /// Every external value is bound once; duplicate states do not shift
    /// placeholders.
    #[test]
    fn test_query_sql_parameter_positions_and_static_predicates() {
        let cursor = deep_cursor();
        let key = "业务' OR 1=1 --";
        let query = TaskQuery {
            after: Some(cursor),
            correlation_key: Some(key.into()),
            states: vec![TaskStateKind::Queued, TaskStateKind::Running, TaskStateKind::Queued],
            limit: 32,
        };
        let built = build_history_query(&query, 32).expect("query builds");
        assert_eq!(
            built.params,
            vec![
                Value::Integer(1800),
                Value::Text(cursor.id.to_string()),
                Value::Text(key.into()),
                Value::Text("Queued".into()),
                Value::Text("Running".into()),
                Value::Integer(33)
            ]
        );
        assert!(built.sql.contains("(accepted_at, id) > (?1, ?2)"));
        assert!(built.sql.contains("correlation_key = ?3"));
        assert!(built.sql.contains("state_kind IN (?4,?5)"));
        assert!(built.sql.ends_with("LIMIT ?6"));
        assert!(!built.sql.contains(key));
        assert!(!built.sql.contains(&cursor.id.to_string()));
        assert!(!built.sql.contains(" OR "));
        assert!(!built.sql.contains("OFFSET"));
        assert!(!built.sql.contains("payload"));
        let first = build_history_query(&TaskQuery::default(), 1).expect("first page builds");
        assert!(!first.sql.contains("WHERE"));
        assert!(!first.sql.contains("(accepted_at, id) >"));
        assert_eq!(first.params, vec![Value::Integer(2)]);
        let filtered_first = build_history_query(
            &TaskQuery {
                correlation_key: Some(key.into()),
                states: query.states,
                ..TaskQuery::default()
            },
            1,
        )
        .expect("filtered first page builds");
        assert!(
            filtered_first
                .sql
                .contains("WHERE correlation_key = ?1 AND state_kind IN (?2,?3)")
        );
        assert_eq!(
            filtered_first.params,
            vec![
                Value::Text(key.into()),
                Value::Text("Queued".into()),
                Value::Text("Running".into()),
                Value::Integer(2)
            ]
        );
        let recovery_first = build_recovery_query(None).expect("recovery first page builds");
        assert!(!recovery_first.sql.contains("(accepted_at, id) >"));
        assert!(!recovery_first.sql.contains(" OR "));
        assert!(!recovery_first.sql.contains("OFFSET"));
        assert!(!recovery_first.sql.contains("payload"));
        assert!(recovery_first.params.is_empty());
    }

    /// Rejects u64/usize inputs that cannot be stored as SQLite signed
    /// integers.
    #[test]
    fn test_query_sql_integer_domain_checks() {
        let cursor = TaskCursor {
            accepted_at_ms: u64::MAX,
            ..deep_cursor()
        };
        assert!(matches!(
            build_history_query(
                &TaskQuery {
                    after: Some(cursor),
                    ..TaskQuery::default()
                },
                1
            ),
            Err(StoreError::InvalidRequest("task history cursor timestamp is too large"))
        ));
        assert!(matches!(
            build_history_query(&TaskQuery::default(), usize::MAX),
            Err(StoreError::InvalidRequest("task history page limit is too large"))
        ));
        assert!(matches!(
            build_recovery_query(Some(cursor)),
            Err(StoreError::InvalidRequest(
                "recovery cursor timestamp exceeds the SQLite integer range"
            ))
        ));
        let maximum = TaskCursor {
            accepted_at_ms: u64::try_from(i64::MAX).expect("maximum fits unsigned"),
            ..deep_cursor()
        };
        assert_eq!(
            build_recovery_query(Some(maximum))
                .expect("SQLite maximum is valid")
                .params[0],
            Value::Integer(i64::MAX)
        );
    }

    /// Initializes fresh, schema-2 or old schema-3 indexes without statistics.
    fn recovery_database_without_statistics(size: usize, version: i64) -> Connection {
        let mut connection = Connection::open_in_memory().expect("unanalysed database opens");
        if version == 2 {
            connection.execute_batch("CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL,state_kind TEXT NOT NULL,accepted_at INTEGER NOT NULL,correlation_key TEXT,idempotency_key TEXT UNIQUE,request_json TEXT NOT NULL,record_format_version INTEGER NOT NULL DEFAULT 2,lifecycle_json TEXT NOT NULL); CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL,value INTEGER NOT NULL); PRAGMA user_version=2;").expect("legacy schema two creates");
        } else if version == 3 {
            connection.execute_batch("CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL,state_kind TEXT NOT NULL,accepted_at INTEGER NOT NULL,correlation_key TEXT,idempotency_key TEXT UNIQUE,request_info_json TEXT NOT NULL,payload BLOB NOT NULL,record_format_version INTEGER NOT NULL DEFAULT 3,lifecycle_json TEXT NOT NULL); CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL,value INTEGER NOT NULL); CREATE INDEX tasks_state_accepted ON tasks(state_kind,accepted_at); CREATE INDEX tasks_accepted_id ON tasks(accepted_at,id); CREATE INDEX tasks_correlation_accepted_id ON tasks(correlation_key,accepted_at,id); CREATE INDEX tasks_state_accepted_id ON tasks(state_kind,accepted_at,id); CREATE INDEX tasks_unfinished_accepted_id ON tasks(accepted_at,id) WHERE state_kind IN ('Queued','Running'); PRAGMA user_version=3;").expect("old schema three creates");
        }
        if version != 3 {
            initialize_schema(&mut connection).expect("schema initializes or upgrades");
        }
        connection.execute(&format!("WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<{size}) INSERT INTO tasks (id,state_kind,accepted_at,request_info_json,payload,lifecycle_json) SELECT printf('00000000-0000-0000-0000-%012x',n),CASE WHEN n%10=0 THEN 'Queued' WHEN n%10=1 THEN 'Running' ELSE 'Succeeded' END,n,'{{}}',X'','{{}}' FROM fixture"), []).expect("VM fixture creates");
        if version == 3 {
            let started = std::time::Instant::now();
            initialize_schema(&mut connection).expect("populated old partial index upgrades");
            eprintln!(
                "old_v3_rows={size} index_rebuild_elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
        let statistics: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name LIKE 'sqlite_stat%')",
                [],
                |row| row.get(0),
            )
            .expect("statistics absence reads");
        assert!(!statistics, "real initialization must not require ANALYZE");
        connection
    }

    /// Executes the builder's complete prepared query and reads actual VM
    /// counters.
    fn recovery_vm_steps(connection: &Connection, after: Option<TaskCursor>) -> i32 {
        let built = build_recovery_query(after).expect("recovery query builds");
        let mut statement = connection.prepare(&built.sql).expect("VM query prepares");
        let mut rows = statement.query(params_from_iter(built.params)).expect("VM query runs");
        let mut count = 0;
        while rows.next().expect("VM row steps").is_some() {
            count += 1;
        }
        drop(rows);
        assert_eq!(count, 257, "both sizes return the same full lookahead page");
        statement.get_status(rusqlite::StatementStatus::VmStep)
    }

    /// Fresh and both upgrade paths use the ordered partial index without
    /// ANALYZE.
    #[test]
    fn test_query_sql_recovery_fresh_and_upgraded_without_statistics() {
        for version in [0, 2, 3] {
            let connection = recovery_database_without_statistics(2000, version);
            for after in [None, Some(deep_cursor())] {
                let plan = explain(&connection, build_recovery_query(after).expect("query builds"));
                eprintln!("version={version} after={after:?} plan={plan:?}");
                assert!(
                    plan.iter().any(|line| line.contains("tasks_unfinished_accepted_id")),
                    "unanalysed recovery must use partial index: {plan:?}"
                );
                assert!(
                    !plan.iter().any(|line| line.contains("TEMP B-TREE")),
                    "recovery must avoid retained-history sorting: {plan:?}"
                );
                if after.is_some() {
                    assert_search(&plan, "tasks_unfinished_accepted_id");
                } else {
                    assert!(
                        plan.iter()
                            .any(|line| line.contains("SCAN tasks USING INDEX tasks_unfinished_accepted_id"))
                    );
                }
            }
        }
    }

    /// Bounds real recovery VM work as retained history grows fivefold without
    /// stats.
    #[test]
    fn test_query_sql_recovery_vm_work_without_statistics() {
        for version in [0, 2, 3] {
            let mut steps = Vec::new();
            for size in [20_000, 100_000] {
                let connection = recovery_database_without_statistics(size, version);
                let first = recovery_vm_steps(&connection, None);
                let after = Some(TaskCursor {
                    accepted_at_ms: u64::try_from(size * 9 / 10).expect("fixture cursor fits"),
                    id: serde_json::from_str("\"ffffffff-ffff-ffff-ffff-ffffffffffff\"")
                        .expect("fixed cursor ID decodes"),
                });
                let deep = recovery_vm_steps(&connection, after);
                eprintln!("version={version} size={size} first_steps={first} deep_steps={deep}");
                assert!(
                    first <= 8000 && deep <= 8000,
                    "257 rows must have bounded work: first={first}, deep={deep}"
                );
                steps.push(deep);
            }
            let [small, large] = steps.as_slice() else {
                panic!("two sizes measured")
            };
            assert!(
                *large <= *small * 3,
                "fivefold retained history must not cause linear VM growth: {steps:?}"
            );
        }
    }

    /// Unary plus preserves exact TEXT membership, including unknown/boundary
    /// values.
    #[test]
    fn test_query_sql_recovery_unary_text_membership_is_unchanged() {
        let connection = Connection::open_in_memory().expect("semantic probe opens");
        connection
            .execute_batch("CREATE TABLE states (value TEXT);")
            .expect("TEXT state fixture creates");
        for value in [
            "Queued",
            "Running",
            "Blocked",
            "Succeeded",
            "Failed",
            "Panicked",
            "Cancelled",
            "queued",
            "Running ",
            " Queued",
            "Queued\0",
            "Running\0",
            "",
            "0",
            "1",
            "未知",
        ] {
            connection
                .execute("INSERT INTO states VALUES (?1)", [value])
                .expect("TEXT value inserts");
        }
        let mut statement = connection
            .prepare("SELECT value,value IN ('Queued','Running'),+value IN ('Queued','Running') FROM states")
            .expect("membership comparison prepares");
        for row in statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?, row.get::<_, bool>(2)?))
            })
            .expect("membership comparison runs")
        {
            let (value, old, new) = row.expect("membership comparison reads");
            assert_eq!(new, old, "unary plus must preserve TEXT semantics for {value:?}");
            assert_eq!(new, matches!(value.as_str(), "Queued" | "Running"));
        }
    }
}
