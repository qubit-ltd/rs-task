// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use crate::handler::TaskHandler;

/// Handler instance and diagnostic source stored under one exact descriptor.
pub(crate) struct RegisteredHandler {
    /// Handler used to execute matching requests.
    pub(crate) handler: Arc<dyn TaskHandler>,
    /// Registration source included in duplicate diagnostics.
    pub(crate) source: String,
}
