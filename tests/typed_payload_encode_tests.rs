//! Public contract tests for typed payload and task request encoding failures.

use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_metadata::metadata::ModelId;
use qubit_task::Payload;
use qubit_task::ResourceRequest;
use qubit_task::TaskRequest;
use qubit_task::model::PayloadEncodeError;
use qubit_task::model::TaskRequestEncodeError;

#[derive(Default)]
struct U32Codec;

impl ValueEncoder<u32> for U32Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &u32) -> Result<Self::Output, Self::Error> {
        Ok(value.to_le_bytes().to_vec())
    }
}

impl ValueDecoder<[u8]> for U32Codec {
    type Output = u32;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        Ok(u32::from_le_bytes(bytes.try_into()?))
    }
}

qubit_codec::register_value_bytes_codec!(id = "qubit_task.tests.typed_encode_u32", codec = U32Codec, value = u32);

#[derive(Default)]
struct FailingCodec;

impl ValueEncoder<u32> for FailingCodec {
    type Output = Vec<u8>;
    type Error = std::io::Error;

    fn encode(&mut self, _value: &u32) -> Result<Self::Output, Self::Error> {
        Err(std::io::Error::other("intentional test encoder failure"))
    }
}

impl ValueDecoder<[u8]> for FailingCodec {
    type Output = u32;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        Ok(u32::from_le_bytes(bytes.try_into()?))
    }
}

qubit_codec::register_value_bytes_codec!(
    id = "qubit_task.tests.typed_encode_failing",
    codec = FailingCodec,
    value = u32
);

#[derive(Default)]
struct BytesCodec;

impl ValueEncoder<Vec<u8>> for BytesCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &Vec<u8>) -> Result<Self::Output, Self::Error> {
        Ok(value.clone())
    }
}

impl ValueDecoder<[u8]> for BytesCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        Ok(bytes.to_vec())
    }
}

qubit_codec::register_value_bytes_codec!(id = "qubit_task.tests.typed_encode_bytes", codec = BytesCodec, value = Vec<u8>);

fn request<T>(data: T, codec_id: &'static str) -> TaskRequest<T> {
    TaskRequest::new(
        "example.encode",
        ModelId::new("qubit_task.tests.Payload"),
        1,
        ValueCodecId::new(codec_id),
        data,
    )
}

#[test]
fn payload_encode_reports_missing_codec_and_rust_type_mismatch() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let error = Payload::new(
        ModelId::new("qubit_task.tests.Payload"),
        1,
        ValueCodecId::new("qubit_task.tests.absent"),
        7_u32,
    )
    .encode(registry)
    .unwrap_err();
    assert!(matches!(error, PayloadEncodeError::MissingCodec(id) if id == "qubit_task.tests.absent"));

    let error = Payload::new(
        ModelId::new("qubit_task.tests.Payload"),
        1,
        ValueCodecId::new("qubit_task.tests.typed_encode_u32"),
        String::from("not a u32"),
    )
    .encode(registry)
    .unwrap_err();
    assert!(matches!(error, PayloadEncodeError::TypeMismatch));
}

#[test]
fn payload_encode_preserves_codec_execution_errors() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let error = Payload::new(
        ModelId::new("qubit_task.tests.Payload"),
        1,
        ValueCodecId::new("qubit_task.tests.typed_encode_failing"),
        7_u32,
    )
    .encode(registry)
    .unwrap_err();
    assert!(matches!(error, PayloadEncodeError::Codec(_)));
}

#[test]
fn task_request_encode_reports_invalid_resource_and_payload_errors() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let mut bad_resource_request = request(7_u32, "qubit_task.tests.typed_encode_u32");
    bad_resource_request.resource_limit = ResourceRequest {
        gpu_labels: vec!["requires-gpu".into()],
        ..ResourceRequest::default()
    };
    assert!(matches!(
        bad_resource_request.encode(registry),
        Err(TaskRequestEncodeError::Resource(_))
    ));

    assert!(matches!(
        request(7_u32, "qubit_task.tests.absent").encode(registry),
        Err(TaskRequestEncodeError::Payload(PayloadEncodeError::MissingCodec(_)))
    ));
}

#[test]
fn task_request_encode_rejects_encoded_payload_above_storage_limit() {
    let registry = ValueBytesCodecRegistry::try_global().expect("codec registry builds");
    let oversized = vec![0_u8; qubit_task::model::MAX_TASK_PAYLOAD_BYTES + 1];
    assert!(matches!(
        request(oversized, "qubit_task.tests.typed_encode_bytes").encode(registry),
        Err(TaskRequestEncodeError::PayloadTooLarge(size))
            if size == qubit_task::model::MAX_TASK_PAYLOAD_BYTES + 1
    ));
}
