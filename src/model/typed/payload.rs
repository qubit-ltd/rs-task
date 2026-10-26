// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::Any;
use std::any::TypeId;

use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_model_id::ModelIdBuf;

use super::EncodedPayload;
use super::PayloadEncodeError;

/// Typed input payload submitted by application code.
///
/// `Payload` keeps the schema and codec identities beside the value so the
/// payload can be checked against a registered encoder before persistence.
///
/// # Type Parameters
///
/// - `T`: Application value type, which must have a stable [`HasModelId`]
///   implementation when constructing or encoding the payload.
///
/// # Examples
///
/// ```
/// use qubit_codec::ValueCodecId;
/// use qubit_model_id::HasModelId;
/// use qubit_model_id::ModelId;
/// use qubit_task::Payload;
///
/// struct Input(u32);
///
/// impl HasModelId for Input {
///     const MODEL_ID: ModelId = ModelId::new("example.Input");
/// }
///
/// let payload = Payload::new(1, ValueCodecId::new("example.u32"), Input(42));
/// assert_eq!(payload.type_id(), ModelId::new("example.Input"));
/// assert_eq!(payload.data.0, 42);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload<T> {
    /// Version of the payload schema, independent of the codec version.
    pub schema_version: u32,
    /// Stable identity of the bytes codec.
    pub codec_id: ValueCodecId,
    /// Application value encoded when the task is accepted.
    pub data: T,
}

impl<T: HasModelId> Payload<T> {
    /// Creates a payload with its stable model, schema, and codec identities.
    ///
    /// # Parameters
    ///
    /// - `schema_version`: version of the payload schema.
    /// - `codec_id`: stable identity of the codec that will encode the value.
    /// - `data`: application value to carry in the payload.
    ///
    /// # Returns
    ///
    /// A payload containing the supplied schema version, codec identity, and
    /// application value.
    #[must_use]
    #[inline]
    pub fn new(schema_version: u32, codec_id: ValueCodecId, data: T) -> Self {
        Self {
            schema_version,
            codec_id,
            data,
        }
    }

    /// Returns the stable identity of the payload's Rust model type.
    ///
    /// # Returns
    ///
    /// The model ID declared by `T`'s [`HasModelId`] implementation.
    #[inline]
    pub const fn type_id(&self) -> ModelId {
        T::MODEL_ID
    }
}

impl<T: HasModelId + 'static> Payload<T> {
    /// Encodes the value with its registered bytes codec.
    ///
    /// # Parameters
    ///
    /// - `self`: The payload whose value should be encoded.
    /// - `registry`: Registry containing the codec identified by `codec_id`.
    ///
    /// # Returns
    ///
    /// An encoded payload retaining the model type marker and payload
    /// metadata alongside the codec-produced bytes.
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
        if descriptor.value_type_id() != TypeId::of::<T>() {
            return Err(PayloadEncodeError::TypeMismatch);
        }
        let bytes = descriptor.encode(&self.data as &dyn Any)?;
        Ok(EncodedPayload::new(
            ModelIdBuf::from(T::MODEL_ID),
            self.schema_version,
            self.codec_id,
            bytes,
        ))
    }
}
