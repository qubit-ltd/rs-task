// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::marker::PhantomData;

use qubit_codec::ValueCodecId;
use qubit_model_metadata::metadata::ModelIdBuf;

use super::StoredPayload;

/// Encoded bytes that still carry the compile-time payload type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPayload<T> {
    /// Stable identity of the payload model.
    pub type_id: ModelIdBuf,
    /// Version of the payload schema, independent of the codec version.
    pub schema_version: u32,
    /// Stable identity of the bytes codec.
    pub codec_id: ValueCodecId,
    /// Encoded payload bytes.
    pub bytes: Vec<u8>,
    marker: PhantomData<fn() -> T>,
}

impl<T> EncodedPayload<T> {
    /// Creates an encoded payload from validated metadata and bytes.
    pub fn new(type_id: ModelIdBuf, schema_version: u32, codec_id: ValueCodecId, bytes: Vec<u8>) -> Self {
        Self {
            type_id,
            schema_version,
            codec_id,
            bytes,
            marker: PhantomData,
        }
    }

    /// Erases the compile-time value type for persistence.
    pub fn into_stored(self) -> StoredPayload {
        StoredPayload {
            type_id: self.type_id,
            schema_version: self.schema_version,
            codec_id: self.codec_id.as_str().to_owned(),
            bytes: self.bytes,
        }
    }
}
