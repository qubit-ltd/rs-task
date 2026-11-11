// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runs deferred store ownership releases on a dedicated thread.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::Duration;

use futures::FutureExt;

use super::internal::ReleaseJob;
use crate::model::OwnerEpoch;
use crate::store::StoreError;
use crate::store::TaskStore;

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(25);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

/// A process-wide, pre-started cleanup thread for guards dropped while armed.
#[derive(Clone)]
pub(in crate::service) struct OwnerReleaseWorker {
    /// Sends release requests to the process-wide cleanup thread.
    sender: tokio::sync::mpsc::UnboundedSender<ReleaseJob>,
}

impl OwnerReleaseWorker {
    /// Starts the cleanup runtime before owner acquisition and returns its
    /// shared worker. A failed thread or runtime startup leaves the caller
    /// free to fail the build.
    ///
    /// # Returns
    ///
    /// The shared cleanup worker after its thread and runtime are ready.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic if locking the singleton or starting the thread or
    /// runtime fails.
    pub(in crate::service) fn shared() -> Result<Self, String> {
        static WORKER: OnceLock<Mutex<Option<OwnerReleaseWorker>>> = OnceLock::new();
        let slot = WORKER.get_or_init(|| Mutex::new(None));
        let mut slot = slot
            .lock()
            .map_err(|error| format!("owner cleanup worker lock poisoned: {error}"))?;
        if let Some(worker) = slot.as_ref() {
            return Ok(worker.clone());
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
        std::thread::Builder::new()
            .name("task-owner-release".into())
            .spawn(move || run_cleanup_worker(receiver, ready_sender))
            .map_err(|error| format!("failed to start owner cleanup worker: {error}"))?;
        ready_receiver
            .recv()
            .map_err(|error| format!("owner cleanup worker stopped before ready: {error}"))??;
        let worker = Self { sender };
        *slot = Some(worker.clone());
        Ok(worker)
    }

    /// Queues a store release for cleanup after the guard is dropped.
    ///
    /// # Parameters
    ///
    /// * `store` is the store whose ownership must be released.
    /// * `epoch` identifies that ownership.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the cleanup channel accepted the request.
    ///
    /// # Errors
    ///
    /// Returns the channel error if the cleanup receiver has stopped.
    pub(super) fn queue_release(&self, store: Arc<dyn TaskStore>, epoch: OwnerEpoch) -> Result<(), String> {
        self.sender
            .send(ReleaseJob { store, epoch })
            .map_err(|error| error.to_string())
    }
}

/// Creates the cleanup runtime, acknowledges readiness, then processes jobs.
///
/// # Parameters
///
/// * `receiver` supplies release requests until all senders are dropped.
/// * `ready` reports whether runtime initialization succeeded.
fn run_cleanup_worker(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<ReleaseJob>,
    ready: mpsc::SyncSender<Result<(), String>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready.send(Err(format!("failed to create owner cleanup runtime: {error}")));
            return;
        }
    };
    if ready.send(Ok(())).is_err() {
        return;
    }
    runtime.block_on(async move {
        while let Some(ReleaseJob { store, epoch }) = receiver.recv().await {
            tokio::spawn(async move {
                run_release_job(store, epoch).await;
            });
        }
    });
}

/// Releases one queued owner epoch, retrying operational failures
/// independently.
///
/// The job retains the original store and epoch across retries. `Failure` uses
/// exponential backoff without a retry limit; `OwnerConflict`, other errors,
/// and panics terminate this job because retrying cannot safely establish that
/// the old ownership is still applicable. A permanently failing release can
/// therefore keep the store reference alive until the process exits.
///
/// # Parameters
///
/// * `store` is retained for each attempt until this job completes.
/// * `epoch` identifies the original ownership and is reused for every retry.
async fn run_release_job(store: Arc<dyn TaskStore>, epoch: OwnerEpoch) {
    let mut delay = INITIAL_RETRY_DELAY;
    let mut failure_count = 0_u32;
    loop {
        let release = async { store.release_owner(epoch).await };
        match AssertUnwindSafe(release).catch_unwind().await {
            Ok(Ok(())) => {
                if failure_count > 0 {
                    eprintln!("owner cleanup recovered epoch {epoch:?} after {failure_count} failures");
                }
                return;
            }
            Ok(Err(StoreError::Failure(message))) => {
                let previous_count = failure_count;
                failure_count = failure_count.saturating_add(1);
                if should_log_retry_failure(previous_count, failure_count) {
                    eprintln!(
                        "owner cleanup failed for epoch {epoch:?} on attempt {failure_count}: {message}; retrying in {delay:?}"
                    );
                }
                tokio::time::sleep(delay).await;
                delay = next_cleanup_delay(delay);
            }
            Ok(Err(StoreError::OwnerConflict)) => {
                eprintln!("owner cleanup stopped for epoch {epoch:?}: ownership epoch is no longer active");
                return;
            }
            Ok(Err(error)) => {
                eprintln!("owner cleanup stopped for epoch {epoch:?} after terminal error: {error}");
                return;
            }
            Err(_) => {
                eprintln!("owner cleanup worker panicked while releasing epoch {epoch:?}");
                return;
            }
        }
    }
}

/// Returns the next exponentially doubled cleanup delay, capped at five
/// seconds.
///
/// # Parameters
///
/// * `delay` is the delay used for the previous failed attempt.
fn next_cleanup_delay(delay: Duration) -> Duration {
    delay.saturating_mul(2).min(MAX_RETRY_DELAY)
}

/// Logs the first failure and powers of two, excluding a saturated counter.
///
/// # Parameters
///
/// * `previous_count` is the failure count before this attempt.
/// * `failure_count` is the saturated count after this attempt.
fn should_log_retry_failure(previous_count: u32, failure_count: u32) -> bool {
    failure_count > previous_count && (failure_count == 1 || failure_count.is_power_of_two())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::INITIAL_RETRY_DELAY;
    use super::MAX_RETRY_DELAY;
    use super::next_cleanup_delay;
    use super::should_log_retry_failure;

    #[test]
    fn test_cleanup_delay_doubles_and_stays_capped_without_overflow() {
        let mut delay = INITIAL_RETRY_DELAY;
        let expected = [
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
            Duration::from_millis(1_600),
            Duration::from_millis(3_200),
            MAX_RETRY_DELAY,
            MAX_RETRY_DELAY,
        ];
        for expected_delay in expected {
            delay = next_cleanup_delay(delay);
            assert_eq!(delay, expected_delay);
        }
        assert_eq!(next_cleanup_delay(Duration::MAX), MAX_RETRY_DELAY);
        assert_eq!(next_cleanup_delay(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn test_cleanup_failure_logging_is_first_and_power_of_two_only() {
        for count in 1..64 {
            assert_eq!(
                should_log_retry_failure(count - 1, count),
                count.is_power_of_two(),
                "count {count}"
            );
        }
        assert!(should_log_retry_failure((1 << 31) - 1, 1 << 31));
        assert!(!should_log_retry_failure(u32::MAX - 1, u32::MAX));
        assert!(!should_log_retry_failure(u32::MAX, u32::MAX));
    }
}
