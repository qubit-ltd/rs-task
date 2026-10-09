// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;

use crate::model::typed::EncodedPayload;
use crate::model::typed::Payload;
use crate::model::typed::ResourceRequest;
use crate::model::typed::TaskId;
use crate::model::typed::TaskRequest;
use crate::model::typed::TaskRequestEncodeError;

struct Counter(u32);

impl HasModelId for Counter {
    const MODEL_ID: ModelId = ModelId::new("qubit_task.tests.Counter");
}

#[derive(Default)]
struct U32LeCodec;

impl ValueEncoder<Counter> for U32LeCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, input: &Counter) -> Result<Self::Output, Self::Error> {
        Ok(input.0.to_le_bytes().to_vec())
    }
}

impl ValueDecoder<[u8]> for U32LeCodec {
    type Output = Counter;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, input: &[u8]) -> Result<Self::Output, Self::Error> {
        let bytes: [u8; 4] = input.try_into()?;
        Ok(Counter(u32::from_le_bytes(bytes)))
    }
}

qubit_codec::register_value_bytes_codec!(id = "qubit_task.tests.u32_le", codec = U32LeCodec, value = Counter);

#[test]
fn test_typed_payload_encodes_and_erases_for_storage() {
    let registry = ValueBytesCodecRegistry::try_global().expect("registry builds");
    let payload = Payload::new(3, ValueCodecId::new("qubit_task.tests.u32_le"), Counter(0x1234));

    let encoded = payload.encode(registry).expect("payload encodes");
    assert_eq!(encoded.type_id().as_str(), "qubit_task.tests.Counter");
    assert_eq!(encoded.schema_version, 3);
    assert_eq!(encoded.bytes, 0x1234_u32.to_le_bytes());

    let stored = encoded.into_stored();
    assert_eq!(stored.type_id.as_str(), "qubit_task.tests.Counter");
    assert_eq!(stored.codec_id, "qubit_task.tests.u32_le");
    assert_eq!(stored.bytes, 0x1234_u32.to_le_bytes());
}

#[test]
fn test_encoded_payload_preserves_type_and_schema_without_codec_version() {
    let registry = ValueBytesCodecRegistry::try_global().expect("registry builds");
    let encoded: EncodedPayload<Counter> = Payload::new(
        7,
        ValueCodecId::new("qubit_task.tests.u32_le"),
        Counter(u32::from_le_bytes([1, 2, 3, 4])),
    )
    .encode(registry)
    .expect("payload encodes");

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
        1,
        ValueCodecId::new("qubit_task.tests.u32_le"),
        Counter(1),
    );
    for index in 0..=crate::model::typed::MAX_TASK_METADATA_ENTRIES {
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
            1,
            ValueCodecId::new("qubit_task.tests.u32_le"),
            Counter(1),
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
        1,
        ValueCodecId::new("qubit_task.tests.u32_le"),
        Counter(1),
    );
    request.metadata.insert("notes", "x".repeat(16 * 1024));

    let registry = ValueBytesCodecRegistry::try_global().expect("registry builds");
    assert!(matches!(
        request.encode(registry),
        Err(TaskRequestEncodeError::MetadataTooLarge(_))
    ));
}

#[test]
fn test_progress_snapshot_projects_stage_and_metrics_and_round_trips() {
    use qubit_progress::Metric;
    use qubit_progress::MetricDelta;
    use qubit_progress::NoopReporter;
    use qubit_progress::Progress;
    use qubit_progress::Stage;

    use crate::model::typed::ProgressCommand;
    use crate::model::typed::TaskProgressSnapshot;

    let reporter = NoopReporter;
    let progress = Progress::builder(&reporter)
        .metric(Metric::new("items", "Items").total(10))
        .start()
        .expect("progress starts");
    let metric = progress.metric("items").expect("metric exists");
    metric
        .apply_delta(MetricDelta::new().started(4).succeeded(3).failed(1))
        .expect("metric updates");
    let command = ProgressCommand::new(
        TaskId::from_id(qubit_id::Id::new(7)),
        2,
        9,
        Some(Stage::new("parse", "Parse input").position(2, 5)),
        vec![metric.snapshot()],
        123,
    );

    let snapshot = TaskProgressSnapshot::from_command(command).expect("snapshot is valid");
    assert_eq!(snapshot.attempt, 2);
    assert_eq!(snapshot.progress_version, 9);
    assert_eq!(snapshot.updated_at_ms, 123);
    let stage = snapshot.stage.as_ref().expect("stage is retained");
    assert_eq!((stage.id.as_str(), stage.name.as_str()), ("parse", "Parse input"));
    assert_eq!((stage.position, stage.total), (Some(2), Some(5)));
    assert_eq!(snapshot.metrics.len(), 1);
    assert_eq!(snapshot.metrics[0].completed, 4);
    assert_eq!(snapshot.metrics[0].succeeded, 3);
    assert_eq!(snapshot.metrics[0].failed, 1);

    let json = serde_json::to_vec(&snapshot).expect("snapshot serializes");
    let decoded: TaskProgressSnapshot = serde_json::from_slice(&json).expect("snapshot deserializes");
    assert_eq!(decoded, snapshot);
}

#[test]
fn test_progress_snapshot_rejects_metric_count_stage_size_and_encoded_size() {
    use qubit_progress::Metric;
    use qubit_progress::NoopReporter;
    use qubit_progress::Progress;
    use qubit_progress::Stage;

    use crate::model::typed::ProgressCommand;
    use crate::model::typed::ProgressSnapshotError;
    use crate::model::typed::TaskProgressSnapshot;

    let id = TaskId::from_id(qubit_id::Id::new(1));
    let reporter = NoopReporter;
    let metric_names: Vec<_> = (0..=crate::model::typed::MAX_TASK_PROGRESS_METRICS)
        .map(|index| format!("metric-{index}"))
        .collect();
    let mut builder = Progress::builder(&reporter);
    for name in &metric_names {
        builder = builder.metric(Metric::new(name, "Metric"));
    }
    let progress = builder.start().expect("progress starts");
    let too_many_metrics = metric_names
        .iter()
        .map(|name| progress.metric(name).expect("metric exists").snapshot())
        .collect();
    assert!(matches!(
        TaskProgressSnapshot::from_command(ProgressCommand::new(id, 1, 1, None, too_many_metrics, 1)),
        Err(ProgressSnapshotError::TooManyMetrics(_))
    ));

    assert!(matches!(
        TaskProgressSnapshot::from_command(ProgressCommand::new(
            id,
            1,
            1,
            Some(Stage::new(&"x".repeat(129), "Stage")),
            Vec::new(),
            1,
        )),
        Err(ProgressSnapshotError::StageIdTooLarge(129))
    ));
    assert!(matches!(
        TaskProgressSnapshot::from_command(ProgressCommand::new(
            id,
            1,
            1,
            Some(Stage::new("stage", &"x".repeat(257))),
            Vec::new(),
            1,
        )),
        Err(ProgressSnapshotError::StageNameTooLarge(257))
    ));

    let names: Vec<_> = (0..9).map(|index| format!("{}-{index}", "x".repeat(2_000))).collect();
    let mut builder = Progress::builder(&reporter);
    for name in &names {
        builder = builder.metric(Metric::new(name, name));
    }
    let progress = builder.start().expect("large progress metrics start");
    let large_metrics = names
        .iter()
        .map(|name| progress.metric(name).expect("metric exists").snapshot())
        .collect();
    assert!(matches!(
        TaskProgressSnapshot::from_command(ProgressCommand::new(id, 1, 1, None, large_metrics, 1)),
        Err(ProgressSnapshotError::TooLarge(_))
    ));
}
