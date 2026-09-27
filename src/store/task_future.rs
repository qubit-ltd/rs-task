// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::pin::Pin;

/// Sendable boxed future used by object-safe asynchronous component APIs.
///
/// # Type Parameters
///
/// * `'a` - Lifetime of borrows captured by the future.
/// * `T` - Value produced when the future completes.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use qubit_task::store::TaskFuture;
///
/// let future: TaskFuture<'_, u8> = Box::pin(async { 7 });
/// assert_eq!(future.await, 7);
/// # }
/// ```
pub type TaskFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
