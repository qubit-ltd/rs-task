// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;

/// Generates monotonically increasing identifiers for the consumer fixture.
pub(super) struct FixtureIds {
    /// Holds the next identifier to allocate.
    next_id: AtomicU64,
}

impl FixtureIds {
    /// Creates a generator whose first identifier uses `first_id`.
    #[must_use]
    #[inline]
    pub(super) fn new(first_id: u64) -> Self {
        Self {
            next_id: AtomicU64::new(first_id),
        }
    }
}

impl IdGenerator<Id, IdGenerationError> for FixtureIds {
    /// Returns the next identifier using a relaxed atomic increment.
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.next_id.fetch_add(1, Ordering::Relaxed)))
    }
}
