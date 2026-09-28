// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Kind of request rule violated by a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestValidationRule {
    /// A required value was empty.
    Required,
    /// A collection contains duplicate values.
    Unique,
    /// A collection contains too many entries.
    MaxEntries,
    /// A value exceeds its UTF-8 byte limit.
    MaxBytes,
    /// GPU labels require a positive GPU count.
    RequiresGpu,
}
