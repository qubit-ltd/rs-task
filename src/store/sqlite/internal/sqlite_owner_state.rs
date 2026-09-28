// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fs::File;

use crate::model::OwnerEpoch;

/// Process-lock ownership and epoch retained by the SQLite store.
pub(in crate::store::sqlite) struct SqliteOwnerState {
    /// Exclusive lock file retained for the active store owner.
    pub(in crate::store::sqlite) lock_file: Option<File>,
    /// Epoch issued to the current service owner.
    pub(in crate::store::sqlite) epoch: Option<OwnerEpoch>,
}
