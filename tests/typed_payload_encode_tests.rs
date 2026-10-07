// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public contract tests for typed payload and task request encoding failures.

use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::Payload;
use qubit_task::ResourceRequest;
use qubit_task::TaskRequest;
use qubit_task::model::PayloadEncodeError;
use qubit_task::model::TaskRequestEncodeError;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Counter(u32);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Bytes(Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Text(String);

impl HasModelId for Counter {
    const MODEL_ID: ModelId = ModelId::new("qubit_task.tests.Payload");
}
impl HasModelId for Bytes {
    const MODEL_ID: ModelId = ModelId::new("qubit_task.tests.Bytes");
}
impl HasModelId for Text {
    const MODEL_ID: ModelId = ModelId::new("qubit_task.tests.Text");
}

#[derive(Default)]
struct U32Codec;

impl ValueEncoder<Counter> for U32Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &Counter) -> Result<Self::Output, Self::Error> {
        Ok(value.0.to_le_bytes().to_vec())
    }
}

impl ValueDecoder<[u8]> for U32Codec {
    type Output = Counter;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        Ok(Counter(u32::from_le_bytes(bytes.try_into()?)))
    }
}

qubit_codec::register_value_bytes_codec!(
    id = "qubit_task.tests.typed_encode_u32",
    codec = U32Codec,
    value = Counter
);

#[derive(Default)]
struct FailingCodec;

impl ValueEncoder<Counter> for FailingCodec {
    type Output = Vec<u8>;
    type Error = std::io::Error;

    fn encode(&mut self, _value: &Counter) -> Result<Self::Output, Self::Error> {
        Err(std::io::Error::other("intentional test encoder failure"))
    }
}

impl ValueDecoder<[u8]> for FailingCodec {
    type Output = Counter;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        Ok(Counter(u32::from_le_bytes(bytes.try_into()?)))
    }
}

qubit_codec::register_value_bytes_codec!(
    id = "qubit_task.tests.typed_encode_failing",
    codec = FailingCodec,
    value = Counter
);

#[derive(Default)]
struct BytesCodec;

impl ValueEncoder<Bytes> for BytesCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &Bytes) -> Result<Self::Output, Self::Error> {
        Ok(value.0.clone())
    }
}

impl ValueDecoder<[u8]> for BytesCodec {
    type Output = Bytes;
    type Error = std::convert::Infallible;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        Ok(Bytes(bytes.to_vec()))
    }
}

qubit_codec::register_value_bytes_codec!(
    id = "qubit_task.tests.typed_encode_bytes",
    codec = BytesCodec,
    value = Bytes
);

fn request<T: HasModelId>(data: T, codec_id: &'static str) -> TaskRequest<T> {
    TaskRequest::new("example.encode", 1, ValueCodecId::new(codec_id), data)
}

#[test]
fn payload_encode_reports_missing_codec_and_rust_type_mismatch() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let error = Payload::new(1, ValueCodecId::new("qubit_task.tests.absent"), Counter(7))
        .encode(registry)
        .unwrap_err();
    assert!(matches!(error, PayloadEncodeError::MissingCodec(id) if id == "qubit_task.tests.absent"));

    let error = Payload::new(
        1,
        ValueCodecId::new("qubit_task.tests.typed_encode_u32"),
        Text(String::from("not a u32")),
    )
    .encode(registry)
    .unwrap_err();
    assert!(matches!(error, PayloadEncodeError::TypeMismatch));
}

#[test]
fn payload_encode_preserves_codec_execution_errors() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let error = Payload::new(
        1,
        ValueCodecId::new("qubit_task.tests.typed_encode_failing"),
        Counter(7),
    )
    .encode(registry)
    .unwrap_err();
    assert!(matches!(error, PayloadEncodeError::Codec(_)));
}

#[test]
fn task_request_encode_reports_invalid_resource_and_payload_errors() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let mut bad_resource_request = request(Counter(7), "qubit_task.tests.typed_encode_u32");
    bad_resource_request.resource_limit = ResourceRequest {
        gpu_labels: vec!["requires-gpu".into()],
        ..ResourceRequest::default()
    };
    assert!(matches!(
        bad_resource_request.encode(registry),
        Err(TaskRequestEncodeError::Resource(_))
    ));

    assert!(matches!(
        request(Counter(7), "qubit_task.tests.absent").encode(registry),
        Err(TaskRequestEncodeError::Payload(PayloadEncodeError::MissingCodec(_)))
    ));
}

#[test]
fn typed_payload_identity_comes_from_the_rust_type() {
    let request = TaskRequest::new(
        "example.encode",
        1,
        ValueCodecId::new("qubit_task.tests.typed_encode_u32"),
        Counter(7),
    );
    assert_eq!(request.payload.type_id(), Counter::MODEL_ID);
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let encoded = Payload::new(1, ValueCodecId::new("qubit_task.tests.typed_encode_u32"), Counter(7))
        .encode(registry)
        .expect("typed payload encodes");
    assert_eq!(encoded.type_id().as_str(), "qubit_task.tests.Payload");
    let stored = encoded.into_stored();
    assert_eq!(stored.type_id.as_str(), "qubit_task.tests.Payload");
}

#[test]
fn task_request_encode_rejects_encoded_payload_above_storage_limit() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let oversized = vec![0_u8; qubit_task::model::MAX_TASK_PAYLOAD_BYTES + 1];
    assert!(matches!(
        request(Bytes(oversized), "qubit_task.tests.typed_encode_bytes").encode(registry),
        Err(TaskRequestEncodeError::PayloadTooLarge(size))
            if size == qubit_task::model::MAX_TASK_PAYLOAD_BYTES + 1
    ));
}
