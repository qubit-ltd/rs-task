// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Tracks whether an attempt has no cancellation request, a persisted request,
/// or a cooperative request that handlers can await.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CancellationState {
    /// No cancellation has been requested.
    Clear,
    /// Cancellation was recorded without a cooperative notification.
    Requested,
    /// Cancellation was requested and can be observed by cooperative handlers.
    CooperativeRequested,
}
