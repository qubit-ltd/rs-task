// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

use super::RequestValidationField;
use super::RequestValidationRule;

/// Structured failure returned by request and resource limit validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestValidationError {
    /// Field that failed validation.
    pub field: RequestValidationField,
    /// Rule that was violated.
    pub rule: RequestValidationRule,
    /// Maximum value, when the rule is a bounded limit.
    pub limit: Option<usize>,
    message: &'static str,
}

impl RequestValidationError {
    /// Creates a validation error with a stable human-readable diagnostic.
    pub(crate) const fn new(
        field: RequestValidationField,
        rule: RequestValidationRule,
        limit: Option<usize>,
        message: &'static str,
    ) -> Self {
        Self {
            field,
            rule,
            limit,
            message,
        }
    }

    /// Returns the stable diagnostic used by display and store errors.
    #[must_use]
    pub const fn message(self) -> &'static str {
        self.message
    }
}

impl fmt::Display for RequestValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for RequestValidationError {}
