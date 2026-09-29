// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::ffi::OsString;
use std::fs::File;
use std::path::Path;

use fs2::FileExt;

use crate::store::StoreError;

/// Acquires an exclusive lock for the canonical database path.
///
/// The suffix is appended to the complete path so databases with different
/// extensions never resolve to the same lock filename.
///
/// # Parameters
///
/// * `database_path` - Canonical path of a database with one physical link.
///
/// # Returns
///
/// The locked file handle, kept alive by the store owner state.
///
/// # Errors
///
/// Returns `OwnerConflict` if another service owns this file and `Failure` for
/// filesystem access errors.
pub(crate) fn acquire_owner_lock(database_path: &Path) -> Result<File, StoreError> {
    let mut lock_name: OsString = database_path.as_os_str().to_owned();
    lock_name.push(".owner.lock");
    let lock = File::options()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(Path::new(&lock_name))
        .map_err(failure)?;
    lock.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            StoreError::OwnerConflict
        } else {
            failure(error)
        }
    })?;
    Ok(lock)
}

/// Converts an operating-system error into a store diagnostic.
///
/// # Parameters
///
/// * `error` - Filesystem operation error to retain.
///
/// # Returns
///
/// A store failure containing the operating-system diagnostic.
fn failure(error: std::io::Error) -> StoreError {
    StoreError::Failure(error.to_string())
}
