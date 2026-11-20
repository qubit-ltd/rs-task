// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Monotonic ID generation for the in-memory service builder.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;

/// Generates monotonically increasing IDs for an in-memory builder.
pub(in crate::service::task_execution_service_builder) struct InMemoryIdGenerator {
    /// Next sequence position retained across concurrent generation calls.
    sequence: AtomicU64,
}

impl InMemoryIdGenerator {
    /// Creates a generator whose first issued ID has value one.
    pub(in crate::service::task_execution_service_builder) fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
        }
    }
}

impl IdGenerator<Id, IdGenerationError> for InMemoryIdGenerator {
    /// Advances the sequence once, returning an overflow error without
    /// wrapping.
    fn generate(&self) -> Result<Id, IdGenerationError> {
        self.sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| sequence.checked_add(1))
            .map(|previous| Id::new(previous + 1))
            .map_err(|sequence| IdGenerationError::SequenceOverflow {
                sequence,
                max: u64::MAX - 1,
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use qubit_id::IdGenerationError;
    use qubit_id::IdGenerator;

    use super::InMemoryIdGenerator;

    #[test]
    fn test_sequence_never_wraps_or_repeats_after_overflow() {
        let generator = InMemoryIdGenerator {
            sequence: AtomicU64::new(u64::MAX - 1),
        };
        assert_eq!(generator.generate().unwrap().value(), u64::MAX);
        assert!(matches!(
            generator.generate(),
            Err(IdGenerationError::SequenceOverflow { .. })
        ));
        assert!(matches!(
            generator.generate(),
            Err(IdGenerationError::SequenceOverflow { .. })
        ));
    }
}
