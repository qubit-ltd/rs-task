// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Request field that failed a documented validation rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestValidationField {
    /// Required task type identifier.
    TaskType,
    /// Required handler version identifier.
    HandlerVersion,
    /// Serialized task input.
    Payload,
    /// GPU labels.
    GpuLabels,
    /// Custom resource names.
    CustomResources,
    /// Correlation key.
    CorrelationKey,
    /// Idempotency key.
    IdempotencyKey,
    /// Task metadata entries.
    Metadata,
}
