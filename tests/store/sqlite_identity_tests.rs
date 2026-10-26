// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;

fn test_directory() -> PathBuf {
    PathBuf::from(format!(
        "qubit-task-database-identity-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ))
}

#[test]
fn test_database_identity_creates_parent_and_canonicalizes_relative_path() {
    let directory = test_directory();
    let nested = directory.join("nested");
    let relative_path = nested.join("tasks.sqlite");
    let absolute_path = std::env::current_dir()
        .expect("current directory is available")
        .join(&relative_path);

    let store = SqliteTaskStore::open(&relative_path).expect("database store opens");
    assert!(nested.exists(), "missing parent directories are created");
    assert!(
        absolute_path.exists(),
        "relative path resolves from the current directory"
    );
    assert!(
        matches!(SqliteTaskStore::open(&absolute_path), Err(StoreError::OwnerConflict)),
        "relative and absolute paths share the canonical owner lock"
    );

    drop(store);
    std::fs::remove_dir_all(directory).expect("test directory is removed");
}

#[test]
fn test_database_identity_rejects_non_file_path() {
    let directory = test_directory();
    std::fs::create_dir_all(&directory).expect("directory is created");

    assert!(
        matches!(SqliteTaskStore::open(&directory), Err(StoreError::Failure(_))),
        "opening a directory as a database returns a store failure"
    );

    std::fs::remove_dir_all(directory).expect("test directory is removed");
}

#[cfg(unix)]
#[test]
fn test_database_identity_rejects_hard_link_aliases() {
    let directory = test_directory();
    std::fs::create_dir_all(&directory).expect("directory is created");
    let path = directory.join("tasks.sqlite");
    let alias = directory.join("alias.sqlite");
    let store = SqliteTaskStore::open(&path).expect("database store opens");
    std::fs::hard_link(&path, &alias).expect("hard-link alias is created");

    assert!(
        matches!(
            SqliteTaskStore::open(&alias),
            Err(StoreError::UnsupportedDatabaseIdentity)
        ),
        "hard-link aliases are rejected because they bypass the owner lock"
    );

    drop(store);
    std::fs::remove_dir_all(directory).expect("test directory is removed");
}
