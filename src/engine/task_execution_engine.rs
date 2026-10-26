// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::engine::EngineError;
use crate::engine::TypedResourceReservation;
use crate::model::typed::ResourceRequest;
use crate::model::typed::TaskId;

/// Reserves resources for a typed task attempt.
pub trait TaskExecutionEngine: Send + Sync {
    /// Reserves resources and returns a guard that releases them when dropped.
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier of the task requesting resources.
    /// * `request` - Resources required for one task attempt.
    ///
    /// # Returns
    ///
    /// A reservation guard that releases its resources when dropped.
    ///
    /// # Errors
    ///
    /// Returns an error when capacity cannot satisfy the request, resources
    /// are temporarily unavailable, or reservation identifiers are exhausted.
    fn try_prepare_typed(&self, id: TaskId, request: ResourceRequest) -> Result<TypedResourceReservation, EngineError>;
}
