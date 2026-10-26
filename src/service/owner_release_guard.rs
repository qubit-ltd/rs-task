// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Releases store ownership when construction or shutdown is cancelled.

mod internal;
mod owner_release_guard;
mod owner_release_worker;

pub(super) use owner_release_guard::OwnerReleaseGuard;
pub(super) use owner_release_worker::OwnerReleaseWorker;
