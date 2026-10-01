// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Opt-in black-box contract checks for third-party task stores.
//!
//! Enable the `conformance` feature in a backend's test dependency. Each suite
//! needs its own fresh fixture namespace. The fixture cleans up that namespace
//! after the suite returns and all store handles have been dropped; it must
//! never point at production data. Await suites to completion rather than
//! cancelling their futures while they hold ownership.
//!
//! # Memory fixture
//!
//! The same public API is usable from an independent backend crate:
//!
//! ```
//! use std::sync::Arc;
//! use qubit_task::conformance::{StoreFixture, verify_core_contract};
//! use qubit_task::store::{MemoryTaskStore, StoreError, TaskFuture, TaskStore};
//!
//! struct MemoryFixture;
//! impl StoreFixture for MemoryFixture {
//!     fn open<'a>(&'a self)
//!         -> TaskFuture<'a, Result<Arc<dyn TaskStore>, StoreError>>
//!     {
//!         Box::pin(async {
//!             Ok(Arc::new(MemoryTaskStore::new(32)) as Arc<dyn TaskStore>)
//!         })
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let report = verify_core_contract(&MemoryFixture).await?;
//! assert!(report.checks.contains(&"atomic_idempotency"));
//! # Ok(())
//! # }
//! ```
//!
//! Memory deliberately cannot pass [`verify_recovery_contract`]: unsupported
//! recovery returns a violation instead of adding a skipped check to a report.
//! Core fixtures retain twelve records including at most four terminal records.
//!
//! # Durable fixture and reopen
//!
//! A SQLite fixture creates one disposable directory and stores its database
//! path. Every `open()` calls `SqliteTaskStore::open` on that same path. Run
//! core and recovery against separate fixture instances. Recovery writes 513
//! unfinished records and two excluded records, releases the first owner,
//! reopens the same database, and compares all persisted snapshots and pages.
//! A competing instance may reject either `open()` or `acquire_owner()` with
//! `StoreError::OwnerConflict`. Successful reports list the checks actually
//! run.
//!
//! # Additional backend tests
//!
//! These suites verify observable results after awaited operations. They do
//! not prove the cancellation/write-draining barrier of `release_owner`, nor
//! crash durability or transaction interruption. Backend authors must add
//! controlled write gates, cancelled-caller and transaction-failure tests.
//! The built-in reference tests are `tests/owner_release_barrier_tests.rs`,
//! `tests/ownership_lifecycle_tests.rs`, and `tests/recovery_bounds_tests.rs`
//! in the [rs-task repository](https://github.com/qubit-ltd/rs-task/tree/main/tests).
//! Implement equivalent fault fixtures for the real backend; passing a
//! delegating wrapper cannot establish its internal commit barrier.

mod contract_report;
mod contract_violation;
mod core_contract;
mod recovery_contract;
mod store_fixture;

#[cfg(test)]
mod tests;

pub use contract_report::ContractReport;
pub use contract_violation::ContractViolation;
#[cfg(test)]
pub(crate) use core_contract::verify_core_contract;
#[cfg(test)]
pub(crate) use recovery_contract::verify_recovery_contract;
pub use store_fixture::StoreFixture;
