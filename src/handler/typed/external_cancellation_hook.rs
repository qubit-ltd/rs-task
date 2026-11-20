// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use crate::model::TaskAttempt;
use crate::model::TaskRunError;
use crate::store::TaskFuture;

/// A thread-safe callback that performs an optional external cancellation
/// action for a handler kind.
///
/// The callback receives the task ID and attempt number as one value, then
/// returns a `'static` task that reports cancellation failures as
/// [`TaskRunError`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_task::handler::ExternalCancellationHook;
/// use qubit_task::model::TaskAttempt;
/// use qubit_task::model::typed::TaskId;
///
/// async fn invoke_cancellation_hook(
///     hook: &ExternalCancellationHook,
///     attempt: TaskAttempt,
/// ) {
///     assert!(hook(attempt).await.is_ok());
/// }
///
/// #[tokio::main]
/// async fn main() {
/// let hook: ExternalCancellationHook = Arc::new(|_attempt| Box::pin(async { Ok(()) }));
/// invoke_cancellation_hook(&hook, TaskAttempt::new(TaskId::from_id(qubit_id::Id::new(42)), 1)).await;
/// }
/// ```
///
/// The registry receives this hook through
/// [`HandlerOptions::with_external_cancellation`].
pub type ExternalCancellationHook =
    Arc<dyn Fn(TaskAttempt) -> TaskFuture<'static, Result<(), TaskRunError>> + Send + Sync>;
