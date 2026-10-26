// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::MAX_TASK_METADATA_BYTES;
use super::MAX_TASK_METADATA_ENTRIES;

/// Errors raised while converting a request into stored form.
///
/// # Examples
///
/// ```
/// use qubit_codec::ValueBytesCodecRegistry;
/// use qubit_model_id::HasModelId;
/// use qubit_model_id::ModelId;
/// use qubit_task::TaskRequest;
/// use qubit_task::model::TaskRequestEncodeError;
///
/// struct Input;
///
/// impl HasModelId for Input {
///     const MODEL_ID: ModelId = ModelId::new("example.Input");
/// }
///
/// fn encode_request(
///     request: TaskRequest<Input>,
///     registry: &ValueBytesCodecRegistry,
/// ) {
///     match request.encode(registry) {
///         Ok(_stored) => {}
///         Err(error) => match error {
///             TaskRequestEncodeError::EmptyKindId => eprintln!("missing task kind"),
///             other => eprintln!("request encoding failed: {other}"),
///         },
///     }
/// }
///
/// // The caller supplies the request and registry; `encode` produces the error.
/// let _encode_request = encode_request;
/// ```
#[must_use]
#[derive(Debug, thiserror::Error)]
pub enum TaskRequestEncodeError {
    /// The handler routing identity is empty or contains only whitespace.
    #[error("task kind_id must not be empty")]
    EmptyKindId,
    /// Metadata contains more entries than the request-level limit.
    #[error("task metadata has {0} entries; the maximum is {MAX_TASK_METADATA_ENTRIES}")]
    TooManyMetadataEntries(
        /// Number of metadata entries on the request that exceeded the limit.
        usize,
    ),
    /// Metadata's bounded JSON representation exceeds the request-level limit.
    #[error("task metadata has {0} encoded bytes; the maximum is {MAX_TASK_METADATA_BYTES}")]
    MetadataTooLarge(
        /// Number of encoded metadata bytes that exceeded the limit.
        usize,
    ),
    /// Encoded payload exceeds the service's reconstructable payload limit.
    #[error("encoded task payload has {0} bytes; the maximum is {max}", max = crate::model::MAX_TASK_PAYLOAD_BYTES)]
    PayloadTooLarge(
        /// Number of encoded payload bytes that exceeded the limit.
        usize,
    ),
    /// `rs-metadata` rejected serialization under its own JSON budget.
    #[error("task metadata serialization failed: {0}")]
    Metadata(
        /// Serialization failure returned by `rs-metadata`.
        #[source]
        qubit_metadata::MetadataWireEncodeError,
    ),
    /// The payload codec could not be resolved or failed to encode the value.
    #[error(transparent)]
    Payload(
        /// Failure to resolve or run the payload codec.
        #[from]
        super::PayloadEncodeError,
    ),
    /// Scheduling resource names or labels exceed their wire limits.
    #[error("task resource request is invalid: {0}")]
    Resource(
        /// Description of the invalid scheduling resource request.
        String,
    ),
}
