// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

use std::process::Command;

use rusqlite::Connection;
use rusqlite::params;

const EN: &str = include_str!("../doc/user-guide.md");
const ZH: &str = include_str!("../doc/user-guide.zh_CN.md");

const MAINTENANCE_SQL: &str = "BEGIN IMMEDIATE;\nDELETE FROM task_event_outbox_quarantine\nWHERE (task_id, state_version) IN (\n    SELECT task_id, state_version\n    FROM task_event_outbox_quarantine\n    WHERE quarantined_at_ms < :cutoff_ms\n    ORDER BY quarantined_at_ms, task_id, state_version\n    LIMIT :batch_size\n);\nSELECT changes() AS deleted_rows;\nCOMMIT;";

fn maintenance_block(document: &str) -> &str {
    let start = document
        .find("```sql\nBEGIN IMMEDIATE;\nDELETE FROM task_event_outbox_quarantine")
        .expect("maintenance SQL block exists");
    let body_start = start + "```sql\n".len();
    let end = document[body_start..].find("\n```").expect("SQL block closes") + body_start;
    &document[body_start..end]
}

fn schema(connection: &Connection) {
    connection
        .execute_batch(
            "CREATE TABLE task_event_outbox_quarantine (
             task_id TEXT NOT NULL, state_version INTEGER NOT NULL,
             quarantined_at_ms INTEGER NOT NULL,
             PRIMARY KEY(task_id, state_version));
         CREATE TABLE task_event_outbox(task_id TEXT PRIMARY KEY);
         CREATE TABLE tasks(task_id TEXT PRIMARY KEY);
         CREATE TABLE task_terminal_checkpoints(task_id TEXT PRIMARY KEY);",
        )
        .unwrap();
}

fn run_batch(connection: &Connection, sql: &str, cutoff_ms: i64, batch_size: i64) -> i64 {
    let mut deleted_rows = None;
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        if statement.starts_with("DELETE") {
            connection
                .execute(
                    statement,
                    rusqlite::named_params! { ":cutoff_ms": cutoff_ms, ":batch_size": batch_size },
                )
                .unwrap();
        } else if statement.starts_with("SELECT changes()") {
            deleted_rows = Some(connection.query_row(statement, [], |row| row.get(0)).unwrap());
        } else {
            connection.execute_batch(&format!("{statement};")).unwrap();
        }
    }
    deleted_rows.expect("SQL reads the batch change count")
}

#[test]
fn test_guides_share_the_executable_bounded_sql_block() {
    assert_eq!(maintenance_block(EN), MAINTENANCE_SQL);
    assert_eq!(maintenance_block(ZH), MAINTENANCE_SQL);
}

#[test]
fn test_maintenance_sql_deletes_only_old_quarantine_rows_in_bounded_stable_batches() {
    let connection = Connection::open_in_memory().unwrap();
    schema(&connection);
    for (id, version, time) in [("b", 2, 1), ("a", 2, 1), ("a", 1, 1), ("edge", 1, 10), ("new", 1, 11)] {
        connection
            .execute(
                "INSERT INTO task_event_outbox_quarantine VALUES (?1, ?2, ?3)",
                params![id, version, time],
            )
            .unwrap();
    }
    connection
        .execute("INSERT INTO task_event_outbox VALUES ('active')", [])
        .unwrap();
    connection.execute("INSERT INTO tasks VALUES ('task')", []).unwrap();
    connection
        .execute("INSERT INTO task_terminal_checkpoints VALUES ('checkpoint')", [])
        .unwrap();

    let sql = maintenance_block(EN);
    assert_eq!(run_batch(&connection, sql, 10, 2), 2);
    let remaining: Vec<(String, i64)> = connection.prepare("SELECT task_id, state_version FROM task_event_outbox_quarantine ORDER BY quarantined_at_ms, task_id, state_version").unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        remaining,
        [("b".to_owned(), 2), ("edge".to_owned(), 1), ("new".to_owned(), 1)]
    );
    assert_eq!(run_batch(&connection, sql, 10, 2), 1);
    assert_eq!(run_batch(&connection, sql, 10, 2), 0);
    let retained: i64 = connection
        .query_row("SELECT count(*) FROM task_event_outbox_quarantine", [], |r| r.get(0))
        .unwrap();
    assert_eq!(retained, 2);
    for table in ["task_event_outbox", "tasks", "task_terminal_checkpoints"] {
        let count: i64 = connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "{table} sentinel remains");
    }
}

#[test]
fn test_rollback_restores_rows_and_python_argument_contract_rejects_invalid_values() {
    let connection = Connection::open_in_memory().unwrap();
    schema(&connection);
    connection
        .execute("INSERT INTO task_event_outbox_quarantine VALUES ('old', 1, 1)", [])
        .unwrap();
    let sql = maintenance_block(EN);
    let statements = sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    connection.execute_batch(&format!("{};", statements[0])).unwrap();
    connection
        .execute(
            statements[1],
            rusqlite::named_params! { ":cutoff_ms": 10_i64, ":batch_size": 2_i64 },
        )
        .unwrap();
    connection.execute_batch("ROLLBACK;").unwrap();
    let count: i64 = connection
        .query_row("SELECT count(*) FROM task_event_outbox_quarantine", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    for doc in [EN, ZH] {
        assert!(doc.contains("type(cutoff_ms) is not int or not 0 <= cutoff_ms <= 2**63 - 1"));
        assert!(doc.contains("type(batch_size) is not int or not 1 <= batch_size <= 2**63 - 1"));
        assert!(doc.contains("connection.execute(\"BEGIN IMMEDIATE;\")"));
        assert!(doc.contains("connection.execute(\"SELECT changes() AS deleted_rows\")"));
        assert!(doc.contains("connection.execute(\"COMMIT;\")"));
        assert!(doc.contains("connection.execute(\"ROLLBACK;\")"));
        let maintenance = doc
            .split("### Maintain history and quarantine archives")
            .nth(1)
            .unwrap_or_else(|| {
                doc.split("### 维护历史与隔离归档")
                    .nth(1)
                    .expect("maintenance section exists")
            });
        let validation = maintenance
            .split("```python\n")
            .nth(1)
            .expect("Python example exists")
            .split("\n\ntry:")
            .next()
            .expect("validation precedes transaction")
            .lines()
            .map(|line| format!("    {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        for (cutoff, batch) in [
            ("True", "1"),
            ("1.5", "1"),
            ("-1", "1"),
            ("2**63", "1"),
            ("0", "False"),
            ("0", "0"),
            ("0", "-1"),
            ("0", "2**63"),
        ] {
            let script = format!(
                "class Connection:\n    called = False\n    def execute(self, *_): self.called = True\nconnection = Connection()\ncutoff_ms = {cutoff}\nbatch_size = {batch}\ntry:\n{validation}\n    connection.execute('BEGIN')\nexcept ValueError:\n    assert not connection.called\nelse:\n    raise AssertionError('invalid values reached SQL')"
            );
            let output = Command::new("python3")
                .args(["-c", &script])
                .output()
                .expect("python3 is available for validating documented inputs");
            assert!(
                output.status.success(),
                "invalid input reached SQL or was accepted: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
