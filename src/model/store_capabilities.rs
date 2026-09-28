// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Store-level capability claims made during service assembly.
///
/// `restart_recovery` implies that unfinished request descriptions can be
/// scanned after restart; it does not promise recovery of process-local values.
///
/// # Examples
///
/// ```
/// use qubit_task::model::StoreCapabilities;
///
/// let volatile = StoreCapabilities { persistent_history: false, restart_recovery: false };
/// assert!(!volatile.restart_recovery);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreCapabilities {
    /// Whether task history survives a process restart.
    pub persistent_history: bool,
    /// Whether accepted unfinished work can be recovered after restart.
    pub restart_recovery: bool,
}
