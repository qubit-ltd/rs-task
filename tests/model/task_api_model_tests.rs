use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_metadata::metadata::ModelId;
use qubit_model_metadata::metadata::ModelIdBuf;

use crate::model::next::EncodedPayload;
use crate::model::next::Payload;
use crate::model::next::ResourceRequest;
use crate::model::next::TaskId;
use crate::model::next::TaskRequest;
use crate::model::next::TaskRequestEncodeError;

#[derive(Default)]
struct U32LeCodec;

impl ValueEncoder<u32> for U32LeCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, input: &u32) -> Result<Self::Output, Self::Error> {
        Ok(input.to_le_bytes().to_vec())
    }
}

impl ValueDecoder<[u8]> for U32LeCodec {
    type Output = u32;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, input: &[u8]) -> Result<Self::Output, Self::Error> {
        let bytes: [u8; 4] = input.try_into()?;
        Ok(u32::from_le_bytes(bytes))
    }
}

qubit_codec::register_value_bytes_codec!(id = "qubit_task.tests.u32_le", codec = U32LeCodec, value = u32);

#[test]
fn test_typed_payload_encodes_and_erases_for_storage() {
    let registry = ValueBytesCodecRegistry::try_global().expect("registry builds");
    let payload = Payload {
        type_id: ModelId::new("qubit_task.tests.Counter"),
        schema_version: 3,
        codec_id: ValueCodecId::new("qubit_task.tests.u32_le"),
        data: 0x1234_u32,
    };

    let encoded = payload.encode(registry).expect("payload encodes");
    assert_eq!(encoded.type_id.as_str(), "qubit_task.tests.Counter");
    assert_eq!(encoded.schema_version, 3);
    assert_eq!(encoded.bytes, 0x1234_u32.to_le_bytes());

    let stored = encoded.into_stored();
    assert_eq!(stored.type_id.as_str(), "qubit_task.tests.Counter");
    assert_eq!(stored.codec_id, "qubit_task.tests.u32_le");
    assert_eq!(stored.bytes, 0x1234_u32.to_le_bytes());
}

#[test]
fn test_encoded_payload_preserves_type_and_schema_without_codec_version() {
    let encoded: EncodedPayload<u32> = EncodedPayload::new(
        ModelIdBuf::parse("qubit_task.tests.Counter").expect("valid model ID"),
        7,
        ValueCodecId::new("qubit_task.tests.u32_le"),
        vec![1, 2, 3, 4],
    );

    let stored = encoded.into_stored();
    assert_eq!(stored.schema_version, 7);
    assert_eq!(stored.codec_id, "qubit_task.tests.u32_le");
}

#[test]
fn test_task_id_uses_numeric_identity_and_padded_storage_key() {
    let id = TaskId::from_id(qubit_id::Id::new(u64::MAX));

    assert_eq!(id.into_id().value(), u64::MAX);
    assert_eq!(id.to_padded_decimal(), "18446744073709551615");
}

#[test]
fn test_resource_request_exposes_optional_memory_and_disk_quotas() {
    let request = ResourceRequest {
        memory_bytes: Some(0),
        disk_bytes: Some(4096),
        ..ResourceRequest::default()
    };

    assert_eq!(request.memory_bytes, Some(0));
    assert_eq!(request.disk_bytes, Some(4096));
}

#[test]
fn test_task_request_rejects_metadata_over_entry_budget() {
    let mut request = TaskRequest::new(
        "example.count",
        ModelId::new("qubit_task.tests.Counter"),
        1,
        ValueCodecId::new("qubit_task.tests.u32_le"),
        1_u32,
    );
    for index in 0..=crate::model::next::MAX_TASK_METADATA_ENTRIES {
        request.metadata.insert(&format!("key_{index}"), "value");
    }

    let registry = ValueBytesCodecRegistry::try_global().expect("registry builds");
    assert!(matches!(
        request.encode(registry),
        Err(TaskRequestEncodeError::TooManyMetadataEntries(_))
    ));
}

#[test]
fn test_task_request_rejects_empty_kind_id_before_encoding_payload() {
    let registry = ValueBytesCodecRegistry::try_global().expect("registry builds");

    for kind_id in ["", " \t\n"] {
        let request = TaskRequest::new(
            kind_id,
            ModelId::new("qubit_task.tests.Counter"),
            1,
            ValueCodecId::new("qubit_task.tests.u32_le"),
            1_u32,
        );

        assert!(matches!(
            request.encode(registry),
            Err(TaskRequestEncodeError::EmptyKindId)
        ));
    }
}

#[test]
fn test_task_request_rejects_metadata_over_serialized_byte_budget() {
    let mut request = TaskRequest::new(
        "example.count",
        ModelId::new("qubit_task.tests.Counter"),
        1,
        ValueCodecId::new("qubit_task.tests.u32_le"),
        1_u32,
    );
    request.metadata.insert("notes", "x".repeat(16 * 1024));

    let registry = ValueBytesCodecRegistry::try_global().expect("registry builds");
    assert!(matches!(
        request.encode(registry),
        Err(TaskRequestEncodeError::MetadataTooLarge(_))
    ));
}
