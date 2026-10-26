// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::marker::PhantomData;

use qubit_codec::ValueCodecId;
use qubit_model_id::ModelIdBuf;

use super::StoredPayload;

/// Encoded bytes that still carry the compile-time payload type.
///
/// `Payload::encode` produces this value after looking up the selected codec
/// in the bytes-codec registry. Convert it to [`StoredPayload`] when the bytes
/// must be persisted without retaining the Rust type.
///
/// # Type Parameters
///
/// - `T`: Rust payload type associated with the encoded bytes.
///
/// # Examples
///
/// ```
/// use core::convert::Infallible;
///
/// use qubit_codec::register_value_bytes_codec;
/// use qubit_codec::ValueBytesCodecRegistry;
/// use qubit_codec::ValueCodecId;
/// use qubit_codec::ValueDecoder;
/// use qubit_codec::ValueEncoder;
/// use qubit_model_id::HasModelId;
/// use qubit_model_id::ModelId;
/// use qubit_task::model::EncodedPayload;
/// use qubit_task::model::Payload;
///
/// #[derive(Debug, PartialEq, Eq)]
/// struct Counter(u32);
///
/// impl HasModelId for Counter {
///     const MODEL_ID: ModelId = ModelId::new("example.Counter");
/// }
///
/// #[derive(Default)]
/// struct CounterCodec;
///
/// impl ValueEncoder<Counter> for CounterCodec {
///     type Error = Infallible;
///     type Output = Vec<u8>;
///
///     fn encode(&mut self, value: &Counter) -> Result<Vec<u8>, Infallible> {
///         Ok(value.0.to_le_bytes().to_vec())
///     }
/// }
///
/// impl ValueDecoder<[u8]> for CounterCodec {
///     type Error = Infallible;
///     type Output = Counter;
///
///     fn decode(&mut self, bytes: &[u8]) -> Result<Counter, Infallible> {
///         let bytes: [u8; 4] = bytes.try_into().expect("counter has four bytes");
///         Ok(Counter(u32::from_le_bytes(bytes)))
///     }
/// }
///
/// register_value_bytes_codec!(
///     id = "example.counter",
///     codec = CounterCodec,
///     value = Counter,
/// );
///
/// fn encode_counter() -> Result<EncodedPayload<Counter>, Box<dyn std::error::Error>> {
///     let registry = ValueBytesCodecRegistry::try_global()?;
///     let payload = Payload::new(1, ValueCodecId::new("example.counter"), Counter(7));
///     Ok(payload.encode(registry)?)
/// }
///
/// let encoded = encode_counter().expect("registered counter codec encodes");
/// assert_eq!(encoded.schema_version, 1);
/// assert_eq!(encoded.bytes, 7_u32.to_le_bytes().to_vec());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPayload<T> {
    /// Stable identity of the payload model.
    type_id: ModelIdBuf,
    /// Version of the payload schema, independent of the codec version.
    pub schema_version: u32,
    /// Stable identity of the bytes codec.
    pub codec_id: ValueCodecId,
    /// Encoded payload bytes.
    pub bytes: Vec<u8>,
    /// Associates the encoded bytes with `T` without storing a value.
    marker: PhantomData<fn() -> T>,
}

impl<T> EncodedPayload<T> {
    /// Creates an encoded payload from validated metadata and bytes.
    ///
    /// The caller must ensure `type_id` and `bytes` describe a value of `T`
    /// encoded with `codec_id` and `schema_version`.
    ///
    /// # Parameters
    ///
    /// - `type_id`: Stable model identity associated with `T`.
    /// - `schema_version`: Schema version used to encode the value.
    /// - `codec_id`: Codec identity used to produce `bytes`.
    /// - `bytes`: Encoded representation owned by this payload.
    ///
    /// # Returns
    ///
    /// An encoded payload carrying the supplied metadata and bytes.
    pub(crate) fn new(type_id: ModelIdBuf, schema_version: u32, codec_id: ValueCodecId, bytes: Vec<u8>) -> Self {
        Self {
            type_id,
            schema_version,
            codec_id,
            bytes,
            marker: PhantomData,
        }
    }

    /// Returns the stable payload model identity.
    ///
    /// # Returns
    ///
    /// The validated model identity borrowed from this payload.
    #[must_use]
    #[inline]
    pub fn type_id(&self) -> &ModelIdBuf {
        &self.type_id
    }

    /// Erases the compile-time value type for persistence.
    ///
    /// # Returns
    ///
    /// A stored payload containing the model identity, schema version, codec
    /// ID string, and encoded bytes. Converting the codec ID to an owned
    /// string allocates.
    #[must_use]
    pub fn into_stored(self) -> StoredPayload {
        StoredPayload {
            type_id: self.type_id,
            schema_version: self.schema_version,
            codec_id: self.codec_id.as_str().to_owned(),
            bytes: self.bytes,
        }
    }
}
