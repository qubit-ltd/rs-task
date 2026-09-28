// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! File identity and process ownership helpers for the SQLite store.

mod database_identity;
mod owner_lock;

pub(super) use database_identity::DatabaseIdentity;
pub(super) use owner_lock::acquire_owner_lock;
