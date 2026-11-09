// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Keeps the bilingual user guides aligned with the typed task API contract.

/// Checks that both guides link to their typed API guide and describe durable
/// lifecycle event publication.
#[test]
fn test_typed_task_guides_link_and_describe_lifecycle_events() {
    let english = include_str!("../doc/user-guide.md");
    let chinese = include_str!("../doc/user-guide.zh_CN.md");

    assert!(
        english.contains("[typed task API guide](typed-task-api.md)"),
        "English user guide must link to the typed task API guide"
    );
    assert!(
        chinese.contains("[带类型任务 API 指南](typed-task-api.zh_CN.md)"),
        "Chinese user guide must link to the typed task API guide"
    );

    assert!(
        english.contains("TaskExecutionServiceBuilder::event_bus")
            && english.contains("SQLite outbox")
            && english.contains("at-least-once"),
        "English user guide must document durable lifecycle publication"
    );
    assert!(
        chinese.contains("TaskExecutionServiceBuilder::event_bus")
            && chinese.contains("SQLite outbox")
            && chinese.contains("至少一次"),
        "Chinese user guide must document durable lifecycle publication"
    );
}

/// Keeps the current migration instructions tied to a compiled typed request.
#[test]
fn test_migration_guides_match_the_current_typed_request_contract() {
    let english = include_str!("../doc/migration.md");
    let chinese = include_str!("../doc/migration.zh_CN.md");
    let fixture = include_str!("fixtures/doc-examples/src/typed_support.rs");

    for guide in [english, chinese] {
        assert!(guide.contains("TaskRequest::new(kind_id, schema_version, codec_id, data)"));
        assert!(guide.contains("HasModelId"));
        assert!(guide.contains("payload_type_id"));
        assert!(guide.contains("schema_version") && guide.contains("codec_id"));
    }
    assert!(fixture.contains("impl HasModelId for ExamplePayload"));
    assert!(fixture.contains("let mut request = TaskRequest::new("));
}
