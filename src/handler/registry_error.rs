// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Describes a handler registry conflict.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::RegistryError;
///
/// let error = RegistryError::InvalidDescriptor;
/// assert!(error.to_string().contains("must not be empty"));
/// ```
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum RegistryError {
    /// Another provider already registered the same task type and version.
    #[error("duplicate handler for `{task_type}` version `{version}` from `{first_source}` and `{second_source}`")]
    Duplicate {
        /// Task family claimed by both handlers.
        task_type: String,
        /// Payload version claimed by both handlers.
        version: String,
        /// Source that registered the handler first.
        first_source: String,
        /// Source that attempted the conflicting registration.
        second_source: String,
    },
    /// A handler declared an empty task type or version.
    #[error("handler task type and version must not be empty")]
    InvalidDescriptor,
}
