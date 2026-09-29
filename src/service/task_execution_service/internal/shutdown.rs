// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;

use tokio::pin;

use super::super::ServiceCore;
use super::super::TaskServiceError;
use super::super::record_store_fault;
use super::super::task_stats;
use super::super::wait_for_attempts;
use super::super::wait_scheduler_finished;
use super::panic_message;

/// Combines service convergence and notification worker shutdown results.
///
/// A notification close failure becomes the close result when task
/// convergence succeeded. If both fail, the service error stays primary and
/// the notification error is appended to its diagnostic.
///
/// # Parameters
///
/// * `primary` - Result of draining service work and releasing ownership.
/// * `notification` - Result of closing the optional notification worker.
///
/// # Returns
///
/// The combined shutdown result, preserving the service error as primary.
///
/// # Errors
///
/// Returns the service failure, the notification close failure, or a combined
/// diagnostic when both operations fail.
pub(in crate::service::task_execution_service) fn combine_shutdown_results(
    primary: Result<(), TaskServiceError>,
    notification: Result<(), TaskServiceError>,
) -> Result<(), TaskServiceError> {
    match (primary, notification) {
        (Ok(()), result) => result,
        (Err(primary), Ok(())) => Err(primary),
        (Err(TaskServiceError::StoreUnavailable(store)), Err(TaskServiceError::NotificationClose(close))) => Err(
            TaskServiceError::StoreUnavailable(format!("{store}; notification close failed: {close}")),
        ),
        (Err(TaskServiceError::SchedulerUnavailable(scheduler)), Err(TaskServiceError::NotificationClose(close))) => {
            Err(TaskServiceError::SchedulerUnavailable(format!(
                "{scheduler}; notification close failed: {close}"
            )))
        }
        (Err(primary), Err(close)) => Err(TaskServiceError::StoreUnavailable(format!(
            "{primary}; notification close failed: {close}"
        ))),
    }
}

/// Closes admission once and starts the shared asynchronous drain coordinator.
///
/// # Parameters
///
/// * `core` - Shared service state to close and drain.
pub(in crate::service::task_execution_service) fn begin_shutdown_core(core: Arc<ServiceCore>) {
    if core.admission.close() {
        core.changed.notify_waiters();
        let runtime_handle = core.runtime_handle.clone();
        runtime_handle.spawn(async move {
            let primary = match AssertUnwindSafe(coordinate_shutdown(&core)).catch_unwind().await {
                Ok(result) => result,
                Err(payload) => {
                    let diagnostic = format!("shutdown coordinator panicked: {}", panic_message(payload));
                    record_store_fault(&core, diagnostic.clone());
                    let primary = core.store_fault.lock().clone().unwrap_or(diagnostic);
                    Err(TaskServiceError::StoreUnavailable(primary))
                }
            };
            let primary = release_owner_after_drain(&core, primary).await;
            let notification = supervise_notification_close(close_notification_publisher(&core)).await;
            let result = combine_shutdown_results(primary, notification);
            core.admission.finish_close(result);
            core.changed.notify_waiters();
        });
    }
}

/// Drains accepted work until it settles or a service fault is observed.
///
/// Waits for admitted writes before examining counts. Store errors stop the
/// scheduler and are returned as the primary shutdown failure. Panics are
/// handled by the coordinator's caller; ownership is released separately only
/// after scheduler and attempt barriers have completed.
async fn coordinate_shutdown(core: &Arc<ServiceCore>) -> Result<(), TaskServiceError> {
    core.admission.wait_idle().await;
    loop {
        let notified = core.changed.notified();
        pin!(notified);
        notified.as_mut().enable();
        if let Some(error) = core.store_fault.lock().clone() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        if let Some(error) = core.scheduler_fault.lock().clone() {
            return Err(TaskServiceError::SchedulerUnavailable(error));
        }
        let stats = match task_stats(core).await {
            Ok(stats) => stats,
            Err(error) => {
                record_store_fault(core, error.to_string());
                return Err(TaskServiceError::StoreUnavailable(error.to_string()));
            }
        };
        if stats.queued == 0 && stats.running == 0 {
            return Ok(());
        }
        notified.await;
    }
}

/// Releases ownership exactly once after every possible writer has drained.
///
/// `primary` retains the convergence failure, if any. A blocked admission,
/// scheduler, or finalizer keeps this future pending and retains ownership.
/// Release errors and panics are recorded and appended to the primary failure;
/// cleanup never restarts the coordinator or retries release.
async fn release_owner_after_drain(
    core: &Arc<ServiceCore>,
    primary: Result<(), TaskServiceError>,
) -> Result<(), TaskServiceError> {
    core.admission.wait_idle().await;
    wait_scheduler_finished(core).await;
    wait_for_attempts(core).await;
    let _transition_guard = core.transition_event_lock.write().await;
    // A worker may have faulted while the normal drain was awaiting its exit.
    let primary = primary.and_then(|()| {
        if let Some(error) = core.store_fault.lock().clone() {
            Err(TaskServiceError::StoreUnavailable(error))
        } else if let Some(error) = core.scheduler_fault.lock().clone() {
            Err(TaskServiceError::SchedulerUnavailable(error))
        } else {
            Ok(())
        }
    });
    let release = AssertUnwindSafe(async {
        if let Some(epoch) = core.owner {
            core.store.release_owner(epoch).await?;
        }
        Ok::<(), crate::store::StoreError>(())
    }).catch_unwind().await;
    let diagnostic = match release {
        Ok(Ok(())) => return primary,
        Ok(Err(error)) => format!("owner release failed: {error}"),
        Err(payload) => format!("owner release panicked: {}", panic_message(payload)),
    };
    record_store_fault(core, diagnostic.clone());
    match primary {
        Ok(()) => Err(TaskServiceError::StoreUnavailable(diagnostic)),
        Err(TaskServiceError::StoreUnavailable(error)) => {
            Err(TaskServiceError::StoreUnavailable(format!("{error}; {diagnostic}")))
        }
        Err(TaskServiceError::SchedulerUnavailable(error)) => {
            Err(TaskServiceError::SchedulerUnavailable(format!("{error}; {diagnostic}")))
        }
        Err(error) => Err(TaskServiceError::StoreUnavailable(format!("{error}; {diagnostic}"))),
    }
}

/// Stops and drains the service-owned notification worker before shutdown
/// publishes its shared result.
///
/// With the `event-bus` feature, this waits on the publisher through
/// `spawn_blocking`, bounded by the configured timeout. A timeout is reported
/// through `NotificationClose`; the worker continues processing accepted
/// notifications. The injected `EventBus` remains application owned.
///
/// # Parameters
///
/// * `core` - Service state containing the optional publisher.
///
/// # Returns
///
/// Success when no publisher exists or it has stopped.
///
/// # Errors
///
/// Returns `NotificationClose` when the worker times out, fails to join, or
/// panics.
async fn close_notification_publisher(core: &Arc<ServiceCore>) -> Result<(), TaskServiceError> {
    #[cfg(feature = "event-bus")]
    if let Some(publisher) = &core.event_bus {
        publisher
            .close(&core.runtime_handle)
            .await
            .map_err(|error| TaskServiceError::NotificationClose(error.to_string()))?;
    }
    #[cfg(not(feature = "event-bus"))]
    let _ = core;
    Ok(())
}

/// Polls the notification close future inside its own panic boundary.
///
/// A panicked close is a notification failure, not a storage fault, and must
/// still allow the coordinator to publish the shared close result.
async fn supervise_notification_close(
    close: impl Future<Output = Result<(), TaskServiceError>>,
) -> Result<(), TaskServiceError> {
    match AssertUnwindSafe(close).catch_unwind().await {
        Ok(result) => result,
        Err(payload) => Err(TaskServiceError::NotificationClose(format!(
            "notification publisher close panicked: {}", panic_message(payload)
        ))),
    }
}

#[cfg(all(test, feature = "event-bus"))]
mod tests {
    use super::supervise_notification_close;
    use crate::service::TaskServiceError;

    #[tokio::test]
    async fn test_shutdown_notification_close_panic_is_classified() {
        let error = supervise_notification_close(async {
            panic!("injected close future panic");
        }).await.expect_err("notification fault");
        assert!(matches!(error, TaskServiceError::NotificationClose(message)
            if message.contains("injected close future panic")));
    }
}
