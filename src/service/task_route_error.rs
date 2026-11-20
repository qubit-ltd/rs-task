// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors raised when a task request cannot be routed to a registered handler.

use qubit_codec::ValueBytesCodecRegistry;
use thiserror::Error;

use crate::handler::typed::TypedTaskHandlerRegistry;

/// Describes why a typed task request does not match the current handler route.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskRouteError;
///
/// let error = TaskRouteError::MissingHandler {
///     kind_id: "image.resize".to_owned(),
/// };
/// assert!(matches!(error, TaskRouteError::MissingHandler { .. }));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[must_use]
pub enum TaskRouteError {
    /// No handler is registered for the requested task kind.
    #[error("no handler is registered for task kind '{kind_id}'")]
    MissingHandler {
        /// Task kind requested by the submission.
        kind_id: String,
    },
    /// The registered handler expects a different payload model.
    #[error("handler expects payload model '{expected}', got '{actual}'")]
    PayloadTypeMismatch {
        /// Payload model required by the registered handler.
        expected: String,
        /// Payload model supplied by the submitted request.
        actual: String,
    },
    /// The handler does not accept the request's schema version.
    #[error("handler does not accept payload schema version {version}")]
    UnsupportedSchemaVersion {
        /// Schema version supplied by the request.
        version: u32,
    },
    /// The codec required to decode the stored payload is not registered.
    #[error("payload codec '{codec_id}' is not registered")]
    MissingCodec {
        /// Codec required to decode the request payload.
        codec_id: String,
    },
}

/// Checks that a request's route identifiers and schema are registered.
///
/// # Parameters
///
/// * `handlers` - Registry used to resolve the task kind and accepted schema.
/// * `codecs` - Registry used to resolve the payload codec.
/// * `kind_id` - Task kind selected by the submitted request.
/// * `type_id` - Payload model identifier stored with the request.
/// * `schema_version` - Payload schema version stored with the request.
/// * `codec_id` - Codec identifier stored with the request.
///
/// # Returns
///
/// Returns `Ok(())` when all route metadata matches registered capabilities.
///
/// # Errors
///
/// Returns the first route mismatch in handler, payload type, schema, then
/// codec order.
pub(crate) fn validate_task_route(
    handlers: &TypedTaskHandlerRegistry,
    codecs: &ValueBytesCodecRegistry,
    kind_id: &str,
    type_id: &str,
    schema_version: u32,
    codec_id: &str,
) -> Result<(), TaskRouteError> {
    let descriptor = handlers
        .descriptor(kind_id)
        .ok_or_else(|| TaskRouteError::MissingHandler {
            kind_id: kind_id.to_owned(),
        })?;
    if type_id != descriptor.payload_type_id().as_str() {
        return Err(TaskRouteError::PayloadTypeMismatch {
            expected: descriptor.payload_type_id().to_string(),
            actual: type_id.to_owned(),
        });
    }
    if schema_version == 0 || !descriptor.accepted_schema_versions().contains(&schema_version) {
        return Err(TaskRouteError::UnsupportedSchemaVersion {
            version: schema_version,
        });
    }
    if codecs.get(codec_id).is_none() {
        return Err(TaskRouteError::MissingCodec {
            codec_id: codec_id.to_owned(),
        });
    }
    Ok(())
}
