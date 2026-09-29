// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::CloseFailure;
use super::Phase;

/// Admission phase, permit count, and shared shutdown result under one mutex.
pub(in crate::service::admission_gate) struct GateState {
    /// Current admission lifecycle phase.
    pub(in crate::service::admission_gate) phase: Phase,
    /// Operations admitted before shutdown that have not finished.
    pub(in crate::service::admission_gate) active: usize,
    /// Shared final shutdown outcome, once published.
    pub(in crate::service::admission_gate) close_result: Option<Result<(), CloseFailure>>,
}
