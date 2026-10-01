// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Error raised while validating or registering a typed handler.
#[derive(Debug, thiserror::Error)]
pub enum HandlerRegistrationError {
    /// Handler routing key is empty.
    #[error("handler kind_id must not be empty")]
    EmptyKindId,
    /// Handler must enumerate at least one supported payload schema version.
    #[error("handler must accept at least one payload schema version")]
    NoAcceptedSchemaVersions,
    /// The descriptor repeats one schema version.
    #[error("schema version {0} is listed more than once")]
    DuplicateSchemaVersion(u32),
    /// The kind is already registered, regardless of payload versions.
    #[error(
        "handler kind_id `{kind_id}` is already registered by `{first_source}`; conflicting registration came from `{second_source}`"
    )]
    DuplicateKindId {
        /// Conflicting kind identifier.
        kind_id: String,
        /// Source that registered the kind first.
        first_source: String,
        /// Source that attempted the duplicate registration.
        second_source: String,
    },
    /// An external-hook cancellation mode has no hook implementation.
    #[error("handler `{0}` declares external cancellation but has no hook")]
    MissingExternalHook(String),
    /// A hook was provided for a mode that does not call external hooks.
    #[error("handler `{0}` provides an external cancellation hook for a non-external mode")]
    UnexpectedExternalHook(String),
}
