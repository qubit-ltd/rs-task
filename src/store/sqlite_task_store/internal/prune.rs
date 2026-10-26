// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal task checkpoint persistence used by history pruning.

use rusqlite::Transaction;
use rusqlite::params;
use serde_json::Value;
use serde_json::from_str;
use serde_json::from_value;
use serde_json::to_string;

use crate::model::TaskState;
use crate::store::StoreError;
use crate::store::sqlite_task_store::failure;

/// Persists each terminal checkpoint before deleting its retained task row.
///
/// Rows are processed in query order. Any invalid lifecycle or failed SQL
/// operation returns immediately, leaving the caller's transaction uncommitted.
///
/// # Parameters
///
/// * `transaction` - The transaction that owns checkpoint and task changes.
/// * `rows` - Selected task IDs, lifecycle JSON, and finish times.
///
/// # Returns
///
/// Returns success after every checkpoint is stored and its task row removed.
///
/// # Errors
///
/// Returns `Failure` for malformed lifecycle data or SQLite errors, and
/// `Conflict` if a selected task row is unexpectedly absent during deletion.
pub(super) fn prune_terminal_rows(
    transaction: &Transaction<'_>,
    rows: &[(String, String, i64)],
) -> Result<(), StoreError> {
    for (id, lifecycle_json, finished_at_ms) in rows {
        let lifecycle: Value = from_str(lifecycle_json).map_err(failure)?;
        let state: TaskState = from_value(
            lifecycle
                .get("state")
                .cloned()
                .ok_or_else(|| StoreError::Failure("task lifecycle is missing its state".into()))?,
        )
        .map_err(failure)?;
        if !state.is_terminal() {
            return Err(StoreError::Failure(
                "terminal task has a nonterminal lifecycle state".into(),
            ));
        }
        let state_json = to_string(&state).map_err(failure)?;
        let state_version = lifecycle
            .get("state_version")
            .and_then(Value::as_i64)
            .ok_or_else(|| StoreError::Failure("task lifecycle has an invalid state version".into()))?;
        transaction
            .execute(
                "INSERT INTO task_terminal_checkpoints(task_id,state_version,state_json,finished_at_ms) VALUES (?1,?2,?3,?4) ON CONFLICT(task_id) DO UPDATE SET state_version=excluded.state_version,state_json=excluded.state_json,finished_at_ms=excluded.finished_at_ms",
                params![id, state_version, state_json, finished_at_ms],
            )
            .map_err(failure)?;
        let deleted = transaction
            .execute("DELETE FROM tasks WHERE id=?1", [id])
            .map_err(failure)?;
        if deleted != 1 {
            return Err(StoreError::Conflict);
        }
    }
    Ok(())
}
