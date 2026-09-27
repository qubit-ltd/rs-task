// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::store::TaskFuture;

/// Async unit type alias retained for component factory uniformity.
///
/// # Type Parameters
///
/// * `'a` - Lifetime of borrows captured by the future.
/// * `T` - Value produced by the future.
///
/// # Examples
///
/// ```
/// use qubit_task::scheduling::SchedulingFuture;
///
/// let _future: SchedulingFuture<'_, u8> = Box::pin(async { 7 });
/// ```
pub type SchedulingFuture<'a, T> = TaskFuture<'a, T>;
