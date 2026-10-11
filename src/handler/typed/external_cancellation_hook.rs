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
pub type ExternalCancellationHook =
    Arc<dyn Fn(TaskId, u32) -> TaskFuture<'static, Result<(), TaskRunError>> + Send + Sync>;
