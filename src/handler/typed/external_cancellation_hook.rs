// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::TaskRunError;
use crate::model::next::TaskId;
use crate::store::TaskFuture;

/// Optional external cancellation action registered for a handler kind.
pub type ExternalCancellationHook =
    std::sync::Arc<dyn Fn(TaskId, u32) -> TaskFuture<'static, Result<(), TaskRunError>> + Send + Sync>;
