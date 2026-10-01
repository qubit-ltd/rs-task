// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

use super::legacy::RequestValidationField;
use super::legacy::RequestValidationRule;

/// Structured failure returned by request and resource limit validation.
///
/// # Examples
///
/// ```
/// use qubit_task::model::RequestValidationField;
/// use qubit_task::model::TaskRequest;
///
/// let error = TaskRequest::new("", "1", Vec::new())
///     .validate_limits()
///     .expect_err("task type is required");
/// assert_eq!(error.field, RequestValidationField::TaskType);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct RequestValidationError {
    /// Field that failed validation.
    pub field: RequestValidationField,
    /// Rule that was violated.
    pub rule: RequestValidationRule,
    /// Maximum value, when the rule is a bounded limit.
    pub limit: Option<usize>,
    /// Stable human-readable diagnostic used by display and store errors.
    message: &'static str,
}

impl RequestValidationError {
    /// Creates a validation error with a stable human-readable diagnostic.
    ///
    /// # Parameters
    ///
    /// * `field` - Request field that failed validation.
    /// * `rule` - Validation rule violated by the field.
    /// * `limit` - Applicable numeric limit, when the rule has one.
    /// * `message` - Stable diagnostic shown to callers.
    ///
    /// # Returns
    ///
    /// A structured validation error for the failed request field.
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
    ///
    /// # Returns
    ///
    /// The static human-readable diagnostic for this validation failure.
    #[must_use]
    #[inline]
    pub const fn message(self) -> &'static str {
        self.message
    }
}

impl fmt::Display for RequestValidationError {
    /// Writes the stable validation diagnostic without allocating a new string.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Destination formatter supplied by the caller.
    ///
    /// # Returns
    ///
    /// The result of writing the diagnostic to the formatter.
    ///
    /// # Errors
    ///
    /// Returns [`fmt::Error`] if the destination formatter rejects the write.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for RequestValidationError {}
