// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Exclusive store-owner generation for one running service process.
///
/// A recoverable store issues an epoch when a service acquires ownership and
/// requires the same epoch when that service releases it.
///
/// # Examples
///
/// ```
/// use qubit_task::model::OwnerEpoch;
///
/// let epoch = OwnerEpoch(7);
/// assert_eq!(epoch.0, 7);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerEpoch(
    /// Monotonically increasing generation issued to the current store owner.
    pub u64,
);
