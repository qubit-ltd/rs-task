// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Errors raised while resolving or executing a typed payload codec.
#[derive(Debug, Error)]
pub enum PayloadEncodeError {
    /// The registry has no codec registered under the payload codec ID.
    #[error("no bytes codec is registered for {0}")]
    MissingCodec(String),
    /// The selected codec expects a value type different from `T`.
    #[error("the selected bytes codec is registered for a different value type")]
    TypeMismatch,
    /// The selected codec rejected the value or could not encode it.
    #[error("payload codec failed: {0}")]
    Codec(#[from] qubit_codec::ValueCodecExecutionError),
}
