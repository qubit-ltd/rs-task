// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;

/// Returns the SQLite owner lock path by appending its suffix to the full name.
///
/// # Parameters
///
/// * `path` - Database file path used by the test.
///
/// # Returns
///
/// The lock path associated with that exact database name.
pub(crate) fn owner_lock_path(path: &Path) -> PathBuf {
    let mut lock_name: OsString = path.as_os_str().to_owned();
    lock_name.push(".owner.lock");
    lock_name.into()
}
