use std::sync::Arc;

use tokio::pin;

use super::super::ServiceCore;
use super::super::TaskServiceError;
use super::super::combine_shutdown_results;
use super::super::record_store_fault;
use super::super::task_stats;
use super::super::wait_for_attempts;
use super::super::wait_scheduler_finished;
use crate::store::StoreError;

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
            let primary = coordinate_shutdown(Arc::clone(&core)).await;
            let notification = close_notification_publisher(&core).await;
            let result = combine_shutdown_results(primary, notification);
            core.admission.finish_close(result);
            core.changed.notify_waiters();
        });
    }
}

/// Drains accepted work and releases store ownership after admission becomes
/// idle.
///
/// # Parameters
///
/// * `core` - Service state whose accepted work must settle.
///
/// # Returns
///
/// Success after workers stop and ownership is released.
///
/// # Errors
///
/// Returns a latched service or store cleanup error.
async fn coordinate_shutdown(core: Arc<ServiceCore>) -> Result<(), TaskServiceError> {
    core.admission.wait_idle().await;
    let initial_store_fault = { core.store_fault.lock().clone() };
    if let Some(error) = initial_store_fault {
        return finish_failed_shutdown(&core, TaskServiceError::StoreUnavailable(error)).await;
    }
    let scheduler_fault = { core.scheduler_fault.lock().clone() };
    if let Some(error) = scheduler_fault {
        wait_scheduler_finished(&core).await;
        wait_for_attempts(&core).await;
        if let Some(epoch) = core.owner
            && let Err(release_error) = core.store.release_owner(epoch).await
        {
            return Err(TaskServiceError::SchedulerUnavailable(format!(
                "{error}; owner release failed: {release_error}"
            )));
        }
        return Err(TaskServiceError::SchedulerUnavailable(error));
    }
    let transition_guard = loop {
        let notified = core.changed.notified();
        pin!(notified);
        notified.as_mut().enable();
        let store_fault = { core.store_fault.lock().clone() };
        if let Some(error) = store_fault {
            return finish_failed_shutdown(&core, TaskServiceError::StoreUnavailable(error)).await;
        }
        let scheduler_fault = { core.scheduler_fault.lock().clone() };
        if let Some(error) = scheduler_fault {
            wait_scheduler_finished(&core).await;
            wait_for_attempts(&core).await;
            if let Some(epoch) = core.owner
                && let Err(store_error) = core.store.release_owner(epoch).await
            {
                record_store_fault(&core, store_error.to_string());
                return Err(TaskServiceError::Store(store_error));
            }
            return Err(TaskServiceError::SchedulerUnavailable(error));
        }
        let stats_result = task_stats(&core).await.inspect_err(|error| {
            if let TaskServiceError::Store(store_error) = error
                && matches!(store_error, StoreError::Failure(_))
            {
                record_store_fault(&core, store_error.to_string());
            }
        });
        let stats = match stats_result {
            Ok(stats) => stats,
            Err(_error) if core.store_fault.lock().is_some() => continue,
            Err(error) => return Err(error),
        };
        if stats.queued == 0 && stats.running == 0 {
            wait_scheduler_finished(&core).await;
            wait_for_attempts(&core).await;
            let transition_guard = core.transition_event_lock.write().await;
            let settled_result = task_stats(&core).await.inspect_err(|error| {
                if let TaskServiceError::Store(store_error) = error
                    && matches!(store_error, StoreError::Failure(_))
                {
                    record_store_fault(&core, store_error.to_string());
                }
            });
            let settled_stats = match settled_result {
                Ok(stats) => stats,
                Err(_error) if core.store_fault.lock().is_some() => {
                    drop(transition_guard);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let scheduler_fault = { core.scheduler_fault.lock().clone() };
            if let Some(error) = scheduler_fault {
                drop(transition_guard);
                wait_scheduler_finished(&core).await;
                wait_for_attempts(&core).await;
                if let Some(epoch) = core.owner
                    && let Err(store_error) = core.store.release_owner(epoch).await
                {
                    record_store_fault(&core, store_error.to_string());
                    return Err(TaskServiceError::Store(store_error));
                }
                return Err(TaskServiceError::SchedulerUnavailable(error));
            }
            if settled_stats.queued == 0 && settled_stats.running == 0 {
                break transition_guard;
            }
        }
        notified.await;
    };
    if let Some(epoch) = core.owner
        && let Err(error) = core.store.release_owner(epoch).await
    {
        record_store_fault(&core, error.to_string());
        wait_scheduler_finished(&core).await;
        wait_for_attempts(&core).await;
        return Err(TaskServiceError::StoreUnavailable(error.to_string()));
    }
    drop(transition_guard);
    Ok(())
}

/// Finishes shutdown after a service fault and attempts owner cleanup.
///
/// # Parameters
///
/// * `core` - Service state whose workers must stop.
/// * `primary` - Fault that caused shutdown.
///
/// # Returns
///
/// The primary failure after all workers stop.
///
/// # Errors
///
/// Returns the primary service failure, enriched when owner release also fails.
async fn finish_failed_shutdown(core: &Arc<ServiceCore>, primary: TaskServiceError) -> Result<(), TaskServiceError> {
    wait_scheduler_finished(core).await;
    wait_for_attempts(core).await;
    if let Some(epoch) = core.owner
        && let Err(release_error) = core.store.release_owner(epoch).await
    {
        let diagnostic = core.store_fault.lock().clone().unwrap_or_else(|| primary.to_string());
        record_store_fault(core, format!("{diagnostic}; owner release failed: {release_error}"));
        return Err(TaskServiceError::StoreUnavailable(format!(
            "{diagnostic}; owner release failed: {release_error}"
        )));
    }
    Err(primary)
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
