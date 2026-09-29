// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Lifecycle phase controlling whether new service operations may enter.
pub(in crate::service::admission_gate) enum Phase {
    /// New operations may enter.
    Open,
    /// No new operations may enter; existing permits are draining.
    Closing,
    /// Shutdown has published its final result.
    Closed,
}
