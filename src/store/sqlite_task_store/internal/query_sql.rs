// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builds parameterized SQLite history queries for typed task rows.

use rusqlite::types::Value;

use crate::model::typed::TaskQuery;
use crate::store::StoreError;

/// SQL text and bound values for one SQLite query.
pub(in crate::store::sqlite_task_store) struct QuerySql {
    /// Internal SQL fragments with numbered placeholders.
    pub(in crate::store::sqlite_task_store) sql: String,
    /// Values in the order of their one-based placeholders.
    pub(in crate::store::sqlite_task_store) params: Vec<Value>,
}

/// Builds a parameterized history query over typed task rows.
pub(in crate::store::sqlite_task_store) fn build_encoded_history_query(
    query: &TaskQuery,
    page_size: usize,
) -> Result<QuerySql, StoreError> {
    let fetch_limit = page_size
        .checked_add(1)
        .and_then(|limit| i64::try_from(limit).ok())
        .ok_or(StoreError::InvalidRequest("task history page limit is too large"))?;
    let mut built = QuerySql {
        sql: "SELECT id,request_info_json,lifecycle_json FROM tasks".to_owned(),
        params: Vec::new(),
    };
    let mut has_predicate = false;
    if let Some(after) = query.after {
        let time = bind_timestamp(
            &mut built.params,
            after.accepted_at_ms,
            "task history cursor timestamp is too large",
        )?;
        let id = bind_text(&mut built.params, after.id.to_padded_decimal());
        append_predicate(&mut built.sql, &mut has_predicate, "(accepted_at, id) > (");
        built.sql.push_str(&format!("{time}, {id})"));
    }
    if let Some(category) = &query.category {
        append_predicate(&mut built.sql, &mut has_predicate, "category = ");
        built.sql.push_str(&bind_text(&mut built.params, category.clone()));
    }
    if let Some(key) = &query.correlation_key {
        append_predicate(&mut built.sql, &mut has_predicate, "correlation_key = ");
        built.sql.push_str(&bind_text(&mut built.params, key.clone()));
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
            built.sql.push_str(&bind_text(&mut built.params, state.as_str().into()));
        }
        built.sql.push(')');
    }
    let limit = bind_value(&mut built.params, Value::Integer(fetch_limit));
    built.sql.push_str(&format!(" ORDER BY accepted_at, id LIMIT {limit}"));
    Ok(built)
}

fn append_predicate(sql: &mut String, has_predicate: &mut bool, fragment: &'static str) {
    sql.push_str(if *has_predicate { " AND " } else { " WHERE " });
    sql.push_str(fragment);
    *has_predicate = true;
}

fn bind_value(params: &mut Vec<Value>, value: Value) -> String {
    params.push(value);
    format!("?{}", params.len())
}

fn bind_timestamp(
    params: &mut Vec<Value>,
    timestamp: u64,
    overflow_message: &'static str,
) -> Result<String, StoreError> {
    let timestamp = i64::try_from(timestamp).map_err(|_| StoreError::InvalidRequest(overflow_message))?;
    Ok(bind_value(params, Value::Integer(timestamp)))
}

fn bind_text(params: &mut Vec<Value>, value: String) -> String {
    bind_value(params, Value::Text(value))
}
