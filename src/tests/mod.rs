//! Crate-internal tests for legacy storage invariants not exposed by typed
//! APIs.

#[cfg(feature = "sqlite")]
pub(crate) mod common {
    pub(crate) mod sqlite_paths {
        use std::ffi::OsString;
        use std::path::Path;
        use std::path::PathBuf;

        /// Returns the SQLite owner-lock sidecar path for a test database.
        pub(crate) fn owner_lock_path(path: &Path) -> PathBuf {
            let mut lock_name: OsString = path.as_os_str().to_owned();
            lock_name.push(".owner.lock");
            lock_name.into()
        }
    }
}
#[cfg(feature = "sqlite")]
pub(crate) mod delayed_write_store_fixture;

mod legacy_history_retention_tests;
mod legacy_owner_release_barrier_tests;
mod legacy_recovery_bounds_tests;
mod legacy_recovery_order_tests;
mod legacy_request_validation_tests;
mod legacy_sqlite_query_tests;
mod legacy_state_filter_tests;
mod legacy_store_counts_tests;
mod store_state_machine_tests;
