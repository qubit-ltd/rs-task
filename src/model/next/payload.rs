// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::Any;

use qubit_codec::ValueBytesCodecRegistry;
use qubit_model_metadata::metadata::ModelId;

use super::EncodedPayload;
use super::PayloadEncodeError;

/// Typed input payload submitted by application code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload<T> {
    /// Stable identity of the payload model.
    pub type_id: ModelId,
    /// Version of the payload schema, independent of the codec version.
    pub schema_version: u32,
    /// Stable identity of the bytes codec.
    pub codec_id: qubit_codec::ValueCodecId,
    /// Application value encoded when the task is accepted.
    pub data: T,
}

impl<T> Payload<T> {
    /// Creates a payload with its stable model, schema, and codec identities.
    pub fn new(type_id: ModelId, schema_version: u32, codec_id: qubit_codec::ValueCodecId, data: T) -> Self {
        Self {
            type_id,
            schema_version,
            codec_id,
            data,
        }
    }
}

impl<T: 'static> Payload<T> {
    /// Encodes the value with its registered bytes codec.
    ///
    /// # Errors
    ///
    /// Returns `MissingCodec` when the codec ID is absent, `TypeMismatch` when
    /// the registration targets another Rust type, or `Codec` for encoder
    /// errors.
    pub fn encode(self, registry: &ValueBytesCodecRegistry) -> Result<EncodedPayload<T>, PayloadEncodeError> {
        let registration = registry
            .get(self.codec_id.as_str())
            .ok_or_else(|| PayloadEncodeError::MissingCodec(self.codec_id.as_str().to_owned()))?;
        let descriptor = registration.descriptor();
        if descriptor.value_type_id() != std::any::TypeId::of::<T>() {
            return Err(PayloadEncodeError::TypeMismatch);
        }
        let bytes = descriptor.encode(&self.data as &dyn Any)?;
        Ok(EncodedPayload::new(
            self.type_id.into(),
            self.schema_version,
            self.codec_id,
            bytes,
        ))
    }
}
