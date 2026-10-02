// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_model_id::ModelIdBuf;

/// Encoded payload bytes stored for recovery without retaining Rust types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPayload {
    /// Stable identity of the payload model.
    pub type_id: ModelIdBuf,
    /// Version of the payload schema, independent of the codec version.
    pub schema_version: u32,
    /// Stable identity of the bytes codec.
    pub codec_id: String,
    /// Encoded payload bytes.
    pub bytes: Vec<u8>,
}
