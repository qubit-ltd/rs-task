// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Engine errors distinguish temporary contention from invalid capacity
/// requests.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum EngineError {
    /// Resources are valid but currently reserved by other tasks.
    #[error("requested resources are temporarily unavailable")]
    TemporarilyUnavailable,
    /// Request exceeds configured capacity or requires unknown resources.
    #[error("requested resources cannot be satisfied by this engine")]
    Unsatisfiable,
    /// The engine exhausted its non-reusable reservation identifiers.
    #[error("task execution reservation identifier space is exhausted")]
    ReservationTokenExhausted,
}
