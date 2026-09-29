// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Runs the public store conformance suites from an independent crate.

#[cfg(all(feature = "sqlite", feature = "conformance"))]
use std::path::PathBuf;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use std::sync::Arc;

#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::conformance::StoreFixture;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::conformance::verify_core_contract;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::conformance::verify_recovery_contract;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::model::TaskId;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::store::SqliteTaskStore;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::store::StoreError;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::store::TaskFuture;
#[cfg(all(feature = "sqlite", feature = "conformance"))]
use qubit_task::store::TaskStore;

#[cfg(all(feature = "sqlite", feature = "conformance"))]
struct SqliteFixture {
    directory: PathBuf,
    database: PathBuf,
}

#[cfg(all(feature = "sqlite", feature = "conformance"))]
impl SqliteFixture {
    fn new() -> std::io::Result<Self> {
        let directory = std::env::temp_dir().join(format!("rs-task-conformance-consumer-{id}", id = TaskId::generate()));
        std::fs::create_dir(&directory)?;
        let database = directory.join("tasks.sqlite");
        Ok(Self { directory, database })
    }
}

#[cfg(all(feature = "sqlite", feature = "conformance"))]
impl StoreFixture for SqliteFixture {
    fn open<'a>(&'a self) -> TaskFuture<'a, Result<Arc<dyn TaskStore>, StoreError>> {
        Box::pin(async move { Ok(Arc::new(SqliteTaskStore::open(&self.database)?) as Arc<dyn TaskStore>) })
    }
}

#[cfg(all(feature = "sqlite", feature = "conformance"))]
impl Drop for SqliteFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).expect("remove this fixture's disposable database directory");
    }
}

#[cfg(all(feature = "sqlite", feature = "conformance"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let core = SqliteFixture::new()?;
    let core_report = verify_core_contract(&core).await?;
    assert!(!core_report.checks.is_empty(), "core suite must report checks");
    drop(core);

    let recovery = SqliteFixture::new()?;
    let recovery_report = verify_recovery_contract(&recovery).await?;
    assert!(!recovery_report.checks.is_empty(), "recovery suite must report checks");
    println!("core checks: {}; recovery checks: {}", core_report.checks.len(), recovery_report.checks.len());
    Ok(())
}

#[cfg(not(all(feature = "sqlite", feature = "conformance")))]
fn main() {}
