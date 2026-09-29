// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

use crate::store::StoreError;

/// Canonical path and operating-system identity of one SQLite database file.
pub(crate) struct DatabaseIdentity {
    /// Canonical path used by both SQLite and its owner lock.
    path: PathBuf,
    /// Stable identity obtained from the opened database file handle.
    file_key: (u64, u64),
}

impl DatabaseIdentity {
    /// Creates a missing database file if needed and resolves its canonical
    /// identity.
    ///
    /// # Parameters
    ///
    /// * `path` - Caller supplied relative or absolute database path.
    ///
    /// # Returns
    ///
    /// The canonical database identity and an open handle retained through
    /// owner-lock acquisition.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent cannot be created, the path is not a
    /// file, the identity cannot be read, or the file has multiple hard links.
    pub(crate) fn open(path: &Path) -> Result<(Self, File), StoreError> {
        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir().map_err(failure)?.join(path)
        };
        let file_name = absolute_path
            .file_name()
            .ok_or_else(|| StoreError::Failure("SQLite database path must name a file".into()))?;
        let parent = absolute_path
            .parent()
            .ok_or_else(|| StoreError::Failure("SQLite database path must have a parent directory".into()))?;
        std::fs::create_dir_all(parent).map_err(failure)?;
        let canonical_parent = parent.canonicalize().map_err(failure)?;
        let unresolved_path = canonical_parent.join(file_name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&unresolved_path)
            .map_err(failure)?;
        let path = unresolved_path.canonicalize().map_err(failure)?;
        let (file_key, links) = file_identity(&file)?;
        ensure_single_link(links)?;
        let identity = Self { path, file_key };
        identity.verify()?;
        Ok((identity, file))
    }

    /// Returns the canonical database path.
    ///
    /// # Returns
    ///
    /// The path shared by SQLite and the corresponding owner-lock filename.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Confirms the canonical path still identifies the opened database file.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedDatabaseIdentity` for multiple hard links or
    /// unsupported identity metadata, and a storage error if the path changed.
    pub(crate) fn verify(&self) -> Result<(), StoreError> {
        let canonical_path = self.path.canonicalize().map_err(failure)?;
        if canonical_path != self.path {
            return Err(StoreError::Failure(
                "SQLite database path identity changed while opening".into(),
            ));
        }
        let file = File::open(&self.path).map_err(failure)?;
        let (file_key, links) = file_identity(&file)?;
        ensure_single_link(links)?;
        if file_key != self.file_key {
            return Err(StoreError::Failure(
                "SQLite database file was replaced while opening".into(),
            ));
        }
        Ok(())
    }
}

/// Rejects a file that can be reached through an alias without its owner lock.
///
/// # Parameters
///
/// * `links` - Physical link count reported for the opened database file.
///
/// # Returns
///
/// Success only when exactly one filesystem link names the file.
///
/// # Errors
///
/// Returns `UnsupportedDatabaseIdentity` when the file has multiple or no
/// links.
fn ensure_single_link(links: u64) -> Result<(), StoreError> {
    if links != 1 {
        return Err(StoreError::UnsupportedDatabaseIdentity);
    }
    Ok(())
}

/// Returns stable file identity and link count using Unix metadata.
///
/// # Parameters
///
/// * `file` - Open database handle whose filesystem metadata is inspected.
///
/// # Returns
///
/// The device/inode pair and physical link count.
///
/// # Errors
///
/// Returns a store error if metadata access fails.
#[cfg(unix)]
fn file_identity(file: &File) -> Result<((u64, u64), u64), StoreError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().map_err(failure)?;
    Ok(((metadata.dev(), metadata.ino()), metadata.nlink()))
}

/// Returns stable file identity and link count using Windows handle metadata.
///
/// # Parameters
///
/// * `file` - Open database handle whose filesystem metadata is inspected.
///
/// # Returns
///
/// The volume/file-index pair and physical link count.
///
/// # Errors
///
/// Returns `UnsupportedDatabaseIdentity` when Windows cannot supply identity
/// metadata, or a store error if metadata access fails.
#[cfg(windows)]
fn file_identity(file: &File) -> Result<((u64, u64), u64), StoreError> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION;
    use windows_sys::Win32::Storage::FileSystem::GetFileInformationByHandle;

    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: `file` owns a valid open handle, and `information` is writable
    // memory with the size and layout required by the Windows API.
    let succeeded = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
    if succeeded == 0 {
        return Err(failure(std::io::Error::last_os_error()));
    }
    let file_index = (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow);
    Ok((
        (u64::from(information.dwVolumeSerialNumber), file_index),
        u64::from(information.nNumberOfLinks),
    ))
}

/// Reports unsupported platforms rather than claiming an unverifiable lock.
///
/// # Parameters
///
/// * `_file` - Open database handle; identity metadata is unavailable here.
///
/// # Returns
///
/// This implementation always returns an identity error.
///
/// # Errors
///
/// Always returns `UnsupportedDatabaseIdentity`.
#[cfg(not(any(unix, windows)))]
fn file_identity(_file: &File) -> Result<((u64, u64), u64), StoreError> {
    Err(StoreError::UnsupportedDatabaseIdentity)
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
