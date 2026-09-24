// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Failure to obtain a typed local result after a task was accepted.
#[derive(Debug, thiserror::Error)]
pub enum LocalTaskResultError {
    /// Execution was cancelled before or during the handler.
    #[error("local task was cancelled")]
    Cancelled,
    /// Execution panicked.
    #[error("local task panicked: {0}")]
    Panicked(String),
    /// Execution cannot currently continue.
    #[error("local task is blocked: {0}")]
    Blocked(String),
    /// The engine failed without a typed application error.
    #[error("local task infrastructure failed: {0}")]
    Infrastructure(String),
    /// A task store failure prevented authoritative finalization.
    #[error("local task store is unavailable: {0}")]
    StoreUnavailable(String),
    /// The typed result channel closed unexpectedly.
    #[error("local task result channel closed")]
    ResultChannelClosed,
    /// The authoritative finalization channel closed unexpectedly.
    #[error("local task finalization channel closed")]
    FinalizationChannelClosed,
}
