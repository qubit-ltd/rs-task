// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::env;
use std::path::PathBuf;
use std::process;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use super::internal::acquire_owner_lock;
use crate::store::StoreError;

/// Returns a unique path for an owner-lock filesystem failure test.
fn unique_path() -> PathBuf {
    env::temp_dir().join(format!(
        "qubit-task-owner-lock-{}-{}",
        process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ))
}

/// Confirms filesystem open failures remain storage failures.
#[test]
fn test_owner_lock_reports_filesystem_open_failures() {
    let missing_parent = unique_path().join("missing").join("tasks.sqlite");

    assert!(matches!(
        acquire_owner_lock(&missing_parent),
        Err(StoreError::Failure(_))
    ));
}
