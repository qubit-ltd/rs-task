// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Releases store ownership when construction or shutdown is cancelled.

#[path = "owner_release_guard/owner_release_guard.rs"]
mod guard_impl;
mod internal;
mod owner_release_worker;

pub(in crate::service) use guard_impl::OwnerReleaseGuard;
pub(in crate::service) use owner_release_worker::OwnerReleaseWorker;
