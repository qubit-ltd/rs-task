// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::atomic::Ordering;

use super::ServiceCore;
use crate::model::ResourceCapacity;
use crate::model::TaskRequest;
use crate::service::RetryPolicy;
use crate::service::TaskServiceError;

/// Reads the current Unix epoch time in milliseconds, saturating to `u64`.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the clock predates the epoch.
#[must_use]
pub(in crate::service::task_execution_service) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

/// Computes the next retry timestamp using a saturating addition.
///
/// # Parameters
///
/// * `now_ms` - Current Unix epoch time in milliseconds.
/// * `policy` - Retry delay policy.
/// * `attempt` - One-based attempt number that just failed.
///
/// # Returns
///
/// The earliest next attempt timestamp, saturated at `u64::MAX`.
#[must_use]
pub(in crate::service::task_execution_service) fn retry_deadline_ms(
    now_ms: u64,
    policy: RetryPolicy,
    attempt: u32,
) -> u64 {
    now_ms.saturating_add(policy.delay_for_attempt(attempt).as_millis().min(u64::MAX as u128) as u64)
}

/// Validates request syntax and size limits before storage access.
///
/// # Parameters
///
/// * `request` - Request metadata and resource demand.
///
/// # Returns
///
/// Success when fields meet their documented syntax and size limits.
///
/// # Errors
///
/// Returns `InvalidRequest` for malformed fields or oversized request data.
pub(in crate::service::task_execution_service) fn validate_request_format(
    request: &TaskRequest,
) -> Result<(), TaskServiceError> {
    request
        .validate_limits()
        .map_err(|error| TaskServiceError::InvalidRequest(error.to_string()))?;
    if request.resources.custom.keys().any(String::is_empty)
        || request.resources.gpu_labels.iter().any(String::is_empty)
    {
        return Err(TaskServiceError::InvalidRequest(
            "resource names and GPU labels must not be empty".into(),
        ));
    }
    Ok(())
}

/// Validates whether the configured engine can satisfy a new request.
///
/// # Parameters
///
/// * `request` - Request whose resource demand is checked.
/// * `capacity` - Total resources configured for the service.
///
/// # Returns
///
/// Success when the engine capacity can satisfy the request.
///
/// # Errors
///
/// Returns `Unsatisfiable` when configured capacity cannot meet the request.
pub(in crate::service::task_execution_service) fn validate_request_capacity(
    request: &TaskRequest,
    capacity: &ResourceCapacity,
) -> Result<(), TaskServiceError> {
    let matching_gpus = capacity
        .gpus
        .values()
        .filter(|labels| request.resources.gpu_labels.iter().all(|label| labels.contains(label)))
        .count();
    if request.resources.cpu_slots > capacity.cpu_slots
        || request.resources.gpu_count as usize > matching_gpus
        || request
            .resources
            .custom
            .iter()
            .any(|(key, value)| capacity.custom.get(key).is_none_or(|limit| value > limit))
    {
        return Err(TaskServiceError::Unsatisfiable);
    }
    Ok(())
}

/// Validates request syntax, size, and resource bounds before local submission.
///
/// # Parameters
///
/// * `request` - Request metadata and resource demand.
/// * `capacity` - Total resources configured for the service.
///
/// # Returns
///
/// Success when request bounds and resource requirements are valid.
///
/// # Errors
///
/// Returns `InvalidRequest` for malformed fields or `Unsatisfiable` when
/// configured capacity cannot meet the request.
pub(in crate::service::task_execution_service) fn validate_request(
    request: &TaskRequest,
    capacity: &ResourceCapacity,
) -> Result<(), TaskServiceError> {
    validate_request_format(request)?;
    validate_request_capacity(request, capacity)
}

/// Truncates diagnostics at a UTF-8 boundary so persisted values stay valid.
///
/// # Parameters
///
/// * `value` - Diagnostic string to retain.
/// * `max_bytes` - Maximum encoded byte length.
///
/// # Returns
///
/// An owned string no longer than `max_bytes` bytes.
#[must_use]
pub(in crate::service::task_execution_service) fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

/// Releases one queue slot reserved by a scheduler worker.
///
/// # Parameters
///
/// * `core` - Service state whose occupied queue count is decremented.
pub(in crate::service::task_execution_service) fn release_core_queue_slot(core: &ServiceCore) {
    let _ = core
        .queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            Some(count.saturating_sub(1))
        });
}

/// Attempts to reserve a queue slot for an automatic retry.
///
/// # Parameters
///
/// * `core` - Service state whose queue limit and count are checked.
///
/// # Returns
///
/// Whether one queue slot was reserved.
pub(in crate::service::task_execution_service) fn try_reserve_core_queue_slot(core: &ServiceCore) -> bool {
    core.queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < core.queue_capacity).then_some(count + 1)
        })
        .is_ok()
}
