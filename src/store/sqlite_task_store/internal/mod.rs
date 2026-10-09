// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! File identity, typed schema, and process ownership helpers for SQLite
//! storage.

mod database_identity;
mod owner_lock;
mod query_sql;
mod schema;
mod sqlite_owner_state;
mod typed_store;
#[cfg(test)]
mod worker_counts;
#[cfg(test)]
mod worker_guard;

pub(super) use database_identity::DatabaseIdentity;
pub(super) use owner_lock::acquire_owner_lock;
pub(super) use schema::initialize_next_schema;
pub(super) use sqlite_owner_state::SqliteOwnerState;
pub(super) use typed_store::accept_encoded;
pub(super) use typed_store::get_encoded_task;
pub(super) use typed_store::list_encoded;
pub(super) use typed_store::list_ready_queued;
pub(super) use typed_store::next_retry_deadline;
pub(super) use typed_store::start_encoded;
pub(super) use typed_store::transition_encoded;
pub(super) use typed_store::update_progress;
#[cfg(test)]
pub(super) use worker_counts::WorkerCounts;
#[cfg(test)]
pub(super) use worker_guard::WorkerGuard;
