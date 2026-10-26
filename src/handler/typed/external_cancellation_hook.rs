// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use crate::model::TaskRunError;
use crate::model::typed::TaskId;
use crate::store::TaskFuture;

/// A thread-safe callback that performs an optional external cancellation
/// action for a handler kind.
///
/// The callback receives the task identifier and its external cancellation
/// token, then returns a `'static` task that reports cancellation failures as
/// [`TaskRunError`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_task::handler::ExternalCancellationHook;
/// use qubit_task::model::typed::TaskId;
///
/// async fn invoke_cancellation_hook(
///     hook: &ExternalCancellationHook,
///     task_id: TaskId,
/// ) {
///     assert!(hook(task_id, 1).await.is_ok());
/// }
///
/// #[tokio::main]
/// async fn main() {
/// let hook: ExternalCancellationHook =
///     Arc::new(|_task_id, _token| Box::pin(async { Ok(()) }));
/// invoke_cancellation_hook(&hook, TaskId::from_id(qubit_id::Id::new(42))).await;
/// }
/// ```
///
/// The registry receives this hook when a handler is registered with
/// [`TaskHandlerRegistry::register_with_cancellation_hook`], then invokes
/// it through its external cancellation flow.
///
/// [`TaskHandlerRegistry::register_with_cancellation_hook`]: crate::handler::TaskHandlerRegistry::register_with_cancellation_hook
pub type ExternalCancellationHook =
    Arc<dyn Fn(TaskId, u32) -> TaskFuture<'static, Result<(), TaskRunError>> + Send + Sync>;
