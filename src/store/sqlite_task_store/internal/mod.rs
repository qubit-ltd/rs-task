// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! File identity and process ownership helpers for the SQLite store.

// Identifies the canonical database file before acquiring its owner lock.
mod database_identity;
// Acquires the process lock associated with the database identity.
mod owner_lock;
// Initializes the schema and migrations within the caller transaction.
mod schema;
// Builds parameterized history and recovery queries.
mod query_sql;
// Reads and writes the persisted row representation.
mod row_codec;
// Holds the process lock and epoch for one open store.
mod sqlite_owner_state;
// Encodes the mutable lifecycle independently of request payloads.
mod stored_lifecycle;
// Represents a row read that includes the payload.
mod stored_task_row;
// Represents a summary projection without the payload.
mod stored_summary_row;
// Tracks blocking workers in unit tests.
#[cfg(test)]
mod worker_counts;
// Releases one test worker count when blocking work exits.
#[cfg(test)]
mod worker_guard;

pub(super) use database_identity::DatabaseIdentity;
pub(super) use owner_lock::acquire_owner_lock;
pub(super) use query_sql::build_history_query;
pub(super) use query_sql::build_recovery_query;
pub(super) use row_codec::decode_stored_summary_row;
pub(super) use row_codec::decode_stored_task_row;
pub(super) use row_codec::encode_lifecycle;
pub(super) use row_codec::encode_summary_lifecycle;
pub(super) use row_codec::read_stored_summary_row;
pub(super) use row_codec::read_stored_task_row;
pub(super) use schema::initialize_schema;
pub(super) use sqlite_owner_state::SqliteOwnerState;
pub(super) use stored_lifecycle::StoredLifecycle;
pub(super) use stored_summary_row::StoredSummaryRow;
pub(super) use stored_task_row::StoredTaskRow;
#[cfg(test)]
pub(super) use worker_counts::WorkerCounts;
#[cfg(test)]
pub(super) use worker_guard::WorkerGuard;
