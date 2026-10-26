// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Describes a deferred owner release sent to the cleanup worker.

use std::sync::Arc;

use crate::model::OwnerEpoch;
use crate::store::TaskStore;

/// Carries one owner release request to the cleanup worker.
pub(in crate::service) struct ReleaseJob {
    /// Store whose ownership must be released.
    pub(in crate::service::owner_release_guard) store: Arc<dyn TaskStore>,
    /// Generation token identifying the acquired ownership.
    pub(in crate::service::owner_release_guard) epoch: OwnerEpoch,
}
