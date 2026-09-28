// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Raw task columns duplicated for indexed SQLite lookup and consistency
/// checks.
pub(in crate::store::sqlite) struct StoredTaskRow {
    /// UUID text stored in the indexed identity column.
    pub(in crate::store::sqlite) id: String,
    /// Indexed lifecycle category.
    pub(in crate::store::sqlite) state_kind: String,
    /// Indexed acceptance timestamp.
    pub(in crate::store::sqlite) accepted_at: i64,
    /// Indexed correlation key.
    pub(in crate::store::sqlite) correlation_key: Option<String>,
    /// Indexed idempotency key.
    pub(in crate::store::sqlite) idempotency_key: Option<String>,
    /// Version of the serialized row representation.
    pub(in crate::store::sqlite) format_version: i64,
    /// Immutable request metadata encoded as JSON.
    pub(in crate::store::sqlite) request_info_json: String,
    /// Opaque request payload bytes.
    pub(in crate::store::sqlite) payload: Vec<u8>,
    /// Mutable task lifecycle encoded as JSON.
    pub(in crate::store::sqlite) lifecycle_json: String,
}
