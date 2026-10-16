// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::TaskHandler;
use super::TaskHandlerDescriptor;

/// Historical provider API retained as an uncompiled source snapshot.
///
/// This file is not declared by `handler/mod.rs`, so the trait is not part of
/// the crate's current public API. Its `task_type`/`version` descriptor model
/// predates the typed `kind_id` and payload-schema contract. The example was
/// removed because it used the unavailable `LocalTaskHandler` API.
pub trait TaskHandlerProvider: Send + Sync {
    /// Returns the stable handler type and version supplied by this provider.
    ///
    /// # Returns
    ///
    /// The descriptor used to select the handler for stored requests.
    #[must_use]
    fn descriptor(&self) -> TaskHandlerDescriptor;

    /// Builds the handler instance during application assembly.
    ///
    /// # Returns
    ///
    /// The constructed handler or a provider diagnostic.
    ///
    /// # Errors
    ///
    /// Returns an error when provider configuration prevents handler creation.
    fn create(&self) -> Result<Arc<dyn TaskHandler>, String>;
}
