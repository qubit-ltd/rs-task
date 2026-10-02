//! Owns only the uniquely named SQLite files created by one integration test.
use std::io;
use std::path::{Path, PathBuf};

/// Removes a test database, its WAL/SHM sidecars and its ownership lock on drop.
/// Declare this guard before store/service values so their handles drop first.
pub struct TemporaryDatabase {
    path: PathBuf,
}

impl TemporaryDatabase {
    /// Allocates a unique path under the OS temporary directory without creating a database.
    pub fn new() -> Self {
        Self {
            path: std::env::temp_dir()
                .join(format!("redis-task-outbox-{}.sqlite", uuid::Uuid::new_v4())),
        }
    }

    /// Returns the path retained until this owning guard is dropped.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Removes only this fixture's files; reports unexpected filesystem failures.
    fn cleanup(&self) -> io::Result<()> {
        let mut failure = None;
        for suffix in ["", "-wal", "-shm", ".owner.lock"] {
            let mut name = self.path.as_os_str().to_owned();
            name.push(suffix);
            if let Err(error) = std::fs::remove_file(Path::new(&name)) {
                if error.kind() != io::ErrorKind::NotFound && failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for TemporaryDatabase {
    /// Cleans files even after early test return; fails a passing test if cleanup fails.
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            if std::thread::panicking() {
                eprintln!(
                    "temporary SQLite cleanup failed for {}: {error}",
                    self.path.display()
                );
            } else {
                panic!(
                    "temporary SQLite cleanup failed for {}: {error}",
                    self.path.display()
                );
            }
        }
    }
}
