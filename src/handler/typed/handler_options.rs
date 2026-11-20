// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::sync::Arc;

use super::CancellationMode;
use super::ExternalCancellationHook;
use crate::model::TaskAttempt;
use crate::model::TaskRunError;
use crate::store::TaskFuture;

/// Configures diagnostic and cancellation behavior for one handler registration.
///
/// Options are consumed during registration. The source label is retained for
/// duplicate-kind diagnostics, while cancellation settings determine which
/// requests the handler can observe.
///
/// # Examples
///
/// ```
/// use qubit_task::HandlerOptions;
///
/// let options = HandlerOptions::new()
///     .with_source("mail worker")
///     .with_cooperative_cancellation();
/// let _ = options;
/// ```
#[derive(Clone)]
pub struct HandlerOptions {
    pub(crate) source: String,
    pub(crate) cancellation_mode: CancellationMode,
    pub(crate) external_cancellation_hook: Option<ExternalCancellationHook>,
}

impl HandlerOptions {
    /// Creates default options with no cancellation support.
    ///
    /// # Returns
    ///
    /// Options with the default diagnostic source and cancellation disabled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the diagnostic source label for duplicate-kind errors.
    ///
    /// # Parameters
    ///
    /// * `source` identifies the registration in duplicate-kind diagnostics.
    ///
    /// # Returns
    ///
    /// The updated options.
    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }

    /// Enables cooperative cancellation for this handler.
    ///
    /// # Returns
    ///
    /// Options that allow the handler to await cooperative cancellation.
    #[must_use]
    pub fn with_cooperative_cancellation(mut self) -> Self {
        self.cancellation_mode = CancellationMode::Cooperative;
        self.external_cancellation_hook = None;
        self
    }

    /// Sets an external cancellation hook and enables that cancellation mode.
    ///
    /// The hook runs when the service requests external cancellation and
    /// receives the active task attempt identity.
    ///
    /// # Type Parameters
    ///
    /// * `F` is the hook closure type.
    /// * `Fut` is the future returned by the hook.
    ///
    /// # Parameters
    ///
    /// * `hook` performs application-specific cancellation for an attempt.
    ///
    /// # Returns
    ///
    /// Options that invoke the supplied hook for external cancellation.
    #[must_use]
    pub fn with_external_cancellation<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(TaskAttempt) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), TaskRunError>> + Send + 'static,
    {
        self.cancellation_mode = CancellationMode::ExternalHook;
        self.external_cancellation_hook = Some(Arc::new(move |attempt| {
            let future = hook(attempt);
            Box::pin(future) as TaskFuture<'static, Result<(), TaskRunError>>
        }));
        self
    }
}

impl Default for HandlerOptions {
    fn default() -> Self {
        Self {
            source: "direct registration".to_owned(),
            cancellation_mode: CancellationMode::Unsupported,
            external_cancellation_hook: None,
        }
    }
}
