// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

use std::ffi::OsString;
use std::path::Path;

use qubit_task::model::OwnerEpoch;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

/// Creates a unique database path for an owner identity test.
fn test_database_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("qubit-task-owner-{}.sqlite", uuid::Uuid::new_v4()))
}

/// Appends the owner-lock suffix without replacing the database extension.
fn owner_lock_path(path: &Path) -> std::path::PathBuf {
    let mut name: OsString = path.as_os_str().to_owned();
    name.push(".owner.lock");
    name.into()
}

/// Removes disposable files created by an owner identity test.
fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(owner_lock_path(path));
    let mut wal_name = path.as_os_str().to_owned();
    wal_name.push("-wal");
    let _ = std::fs::remove_file(Path::new(&wal_name));
    let mut shm_name = path.as_os_str().to_owned();
    shm_name.push("-shm");
    let _ = std::fs::remove_file(Path::new(&shm_name));
}

/// Prevents a symbolic link from creating a second owner lock for one file.
#[cfg(unix)]
#[tokio::test]
async fn test_symlink_alias_uses_the_same_owner_lock() {
    use std::os::unix::fs::symlink;

    let path = test_database_path();
    let alias = path.with_file_name(format!("{}-alias.sqlite", uuid::Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("canonical database opens");
    symlink(&path, &alias).expect("database alias is created");

    let alias_result = SqliteTaskStore::open(&alias);
    assert!(
        matches!(alias_result, Err(StoreError::OwnerConflict)),
        "the canonical owner lock rejects a second open"
    );

    drop(store);
    std::fs::remove_file(&alias).expect("test alias is removed");
    remove_database(&path);
}

/// Keeps distinct database files independent even when their stems match.
#[tokio::test]
async fn test_different_extensions_use_distinct_owner_locks() {
    let stem = std::env::temp_dir().join(format!("qubit-task-owner-{}", uuid::Uuid::new_v4()));
    let sqlite_path = stem.with_extension("sqlite");
    let db_path = stem.with_extension("db");
    let sqlite_store = SqliteTaskStore::open(&sqlite_path).expect("sqlite file opens");
    let db_store = SqliteTaskStore::open(&db_path).expect("different database file opens");

    drop(sqlite_store);
    drop(db_store);
    remove_database(&sqlite_path);
    remove_database(&db_path);
}

/// Rejects a database inode that can be reached without its owner lock.
#[tokio::test]
async fn test_hard_linked_database_is_rejected() {
    let path = test_database_path();
    let alias = path.with_file_name(format!("{}-hardlink.sqlite", uuid::Uuid::new_v4()));
    drop(SqliteTaskStore::open(&path).expect("database is initialized"));
    std::fs::hard_link(&path, &alias).expect("database hard link is created");

    assert!(matches!(
        SqliteTaskStore::open(&path),
        Err(StoreError::UnsupportedDatabaseIdentity)
    ));

    std::fs::remove_file(&alias).expect("test hard link is removed");
    remove_database(&path);
}

/// Prevents a Windows symbolic link from creating a second owner lock.
#[cfg(windows)]
#[tokio::test]
async fn test_symlink_alias_uses_the_same_owner_lock() {
    use std::os::windows::fs::symlink_file;

    let path = test_database_path();
    let alias = path.with_file_name(format!("{}-alias.sqlite", uuid::Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("canonical database opens");
    symlink_file(&path, &alias).expect("database alias is created");

    assert!(
        matches!(SqliteTaskStore::open(&alias), Err(StoreError::OwnerConflict)),
        "an alias cannot acquire a second lock"
    );

    drop(store);
    std::fs::remove_file(&alias).expect("test alias is removed");
    remove_database(&path);
}

/// Refuses to issue another owner epoch while this store already owns one.
#[tokio::test]
async fn test_repeated_owner_acquisition_is_rejected() {
    let path = test_database_path();
    let store = SqliteTaskStore::open(&path).expect("database opens");
    let epoch = store.acquire_owner().await.expect("owner is acquired");

    assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));

    store.release_owner(epoch).await.expect("owner is released");
    drop(store);
    remove_database(&path);
}

/// Verifies that an invalid epoch cannot release the held process lock.
#[tokio::test]
async fn test_stale_owner_release_keeps_the_database_locked() {
    let path = test_database_path();
    let store = SqliteTaskStore::open(&path).expect("database opens");
    let epoch = store.acquire_owner().await.expect("owner is acquired");

    assert!(matches!(
        store.release_owner(OwnerEpoch(epoch.0.saturating_add(1))).await,
        Err(StoreError::OwnerConflict)
    ));
    assert!(
        matches!(SqliteTaskStore::open(&path), Err(StoreError::OwnerConflict)),
        "stale release keeps the lock held"
    );

    store.release_owner(epoch).await.expect("matching owner releases lock");
    drop(store);
    remove_database(&path);
}
