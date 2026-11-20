// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::future::Future;
use std::sync::Arc;

use super::CancellationMode;
use super::ExternalCancellationHook;
use crate::model::TaskAttempt;
use crate::model::TaskRunError;
use crate::store::TaskFuture;

/// Per-registration source and cancellation behavior.
#[derive(Clone)]
pub struct HandlerOptions {
    pub(crate) source: String,
    pub(crate) cancellation_mode: CancellationMode,
    pub(crate) external_cancellation_hook: Option<ExternalCancellationHook>,
}

impl HandlerOptions {
    /// Creates default options with no cancellation support.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the diagnostic source label for duplicate-kind errors.
    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }

    /// Enables cooperative cancellation for this handler.
    #[must_use]
    pub fn with_cooperative_cancellation(mut self) -> Self {
        self.cancellation_mode = CancellationMode::Cooperative;
        self.external_cancellation_hook = None;
        self
    }

    /// Sets an external cancellation hook and enables that cancellation mode.
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
