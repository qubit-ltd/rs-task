// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Errors raised while resolving or executing a typed payload codec.
///
/// # Examples
///
/// ```
/// use qubit_task::model::typed::PayloadEncodeError;
///
/// fn report_encoding_failure(error: PayloadEncodeError) {
///     match error {
///         PayloadEncodeError::MissingCodec(codec_id) => {
///             eprintln!("no codec is registered for {codec_id}");
///         }
///         PayloadEncodeError::TypeMismatch => {
///             eprintln!("the registered codec targets another value type");
///         }
///         PayloadEncodeError::Codec(source) => {
///             eprintln!("the codec failed: {source}");
///         }
///     }
/// }
/// ```
///
/// The value is supplied by [`Payload::encode`](super::Payload::encode).
#[must_use]
#[derive(Debug, Error)]
pub enum PayloadEncodeError {
    /// The registry has no codec registered under the payload codec ID.
    #[error("no bytes codec is registered for {0}")]
    MissingCodec(#[doc = "The codec identifier that was not registered."] String),
    /// The selected codec expects a value type different from `T`.
    #[error("the selected bytes codec is registered for a different value type")]
    TypeMismatch,
    /// The selected codec rejected the value or could not encode it.
    #[error("payload codec failed: {0}")]
    Codec(
        #[doc = "The underlying codec execution failure."]
        #[from]
        qubit_codec::ValueCodecExecutionError,
    ),
}
