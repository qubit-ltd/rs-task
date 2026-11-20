// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::marker::PhantomData;

use crate::handler::TaskRunResult;
use crate::handler::typed::TypedTaskContext;
use crate::handler::typed::TypedTaskHandler;
use crate::store::TaskFuture;

/// Adapts an async function and its future type to the typed handler contract.
///
/// The registry retains the callback and constructs its future only when a
/// prepared task is dispatched.
///
/// # Type Parameters
///
/// * `F` is the application callback type.
/// * `Fut` is the future returned by that callback.
pub(super) struct FunctionHandler<F, Fut> {
    /// Application callback invoked for each prepared payload.
    function: F,
    /// Retains the future type without storing a future before dispatch.
    marker: PhantomData<fn() -> Fut>,
}

impl<F, Fut> FunctionHandler<F, Fut> {
    /// Wraps a callback for registry dispatch.
    ///
    /// # Parameters
    ///
    /// * `function` is the application callback used to run a decoded payload.
    ///
    /// # Returns
    ///
    /// A handler adapter that invokes the callback when dispatched.
    pub(super) fn new(function: F) -> Self {
        Self {
            function,
            marker: PhantomData,
        }
    }
}

impl<T, F, Fut> TypedTaskHandler<T> for FunctionHandler<F, Fut>
where
    T: Send + Sync + 'static,
    F: Fn(T, TypedTaskContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = TaskRunResult> + Send + 'static,
{
    /// Calls the application function and boxes its returned future.
    ///
    /// # Parameters
    ///
    /// * `input` is the decoded payload for this attempt.
    /// * `context` supplies this attempt's identity and runtime facilities.
    ///
    /// # Returns
    ///
    /// A boxed future that completes with the handler result.
    fn run<'a>(&'a self, input: T, context: TypedTaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin((self.function)(input, context))
    }
}
