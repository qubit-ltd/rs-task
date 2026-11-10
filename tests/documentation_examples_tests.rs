// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Keeps the bilingual user guides aligned with the typed task API contract.

use std::path::Path;
use std::process::Command;

fn section<'a>(text: &'a str, heading: &str) -> &'a str {
    let marker = format!("## {heading}\n");
    let start = text
        .find(&marker)
        .unwrap_or_else(|| panic!("missing section {heading}"))
        + marker.len();
    let end = text[start..].find("\n## ").map_or(text.len(), |offset| start + offset);
    &text[start..end]
}

fn fenced_block<'a>(text: &'a str, language: &str) -> &'a str {
    let opening = format!("```{language}\n");
    let start = text
        .find(&opening)
        .unwrap_or_else(|| panic!("missing {language} block"))
        + opening.len();
    let end = start
        + text[start..]
            .find("```\n")
            .unwrap_or_else(|| panic!("unterminated {language} block"));
    &text[start..end]
}

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

/// Keeps README dependencies and source identical to the consumer fixture and
/// compiles the exact README content as standalone consumers.
#[test]
fn test_readme_quick_start_matches_and_compiles_as_a_consumer() {
    let english = include_str!("../README.md");
    let chinese = include_str!("../README.zh_CN.md");
    let fixture_manifest = include_str!("fixtures/readme-quick-start/Cargo.toml");
    let fixture_source = include_str!("fixtures/readme-quick-start/src/main.rs");
    let english_dependencies = fenced_block(section(english, "Installation"), "toml");
    let chinese_dependencies = fenced_block(section(chinese, "安装"), "toml");
    assert_eq!(english_dependencies, chinese_dependencies);
    for dependency in [
        "qubit-task = { version = \"0.11\", features = [\"sqlite\"] }",
        "qubit-codec = { version = \"0.17\", features = [\"registry\"] }",
        "qubit-id = { version = \"0.7\", features = [\"serde\"] }",
        "qubit-model-id = \"0.1\"",
        "serde = { version = \"1.0\", features = [\"derive\"] }",
        "serde_json = \"1.0\"",
        "tokio = { version = \"1.54\", features = [\"macros\", \"rt-multi-thread\", \"time\"] }",
    ] {
        assert!(english_dependencies.contains(dependency), "missing {dependency}");
    }
    assert!(fixture_manifest.contains("version = \"^0.11\""));
    assert!(fixture_manifest.contains("features = [\"sqlite\"]"));
    for dependency in [
        "qubit-codec = { version = \"^0.17\", features = [\"registry\"] }",
        "qubit-id = { version = \"^0.7\", features = [\"serde\"] }",
        "qubit-model-id = { path = \"../../../../rs-model-metadata/model-id\", version = \"^0.1\" }",
        "serde = { version = \"1.0\", features = [\"derive\"] }",
        "serde_json = \"1.0\"",
    ] {
        assert!(fixture_manifest.contains(dependency), "fixture missing {dependency}");
    }
    assert!(
        fixture_manifest
            .contains("tokio = { version = \"1.54\", features = [\"macros\", \"rt-multi-thread\", \"time\"] }")
    );
    let english_source = fenced_block(section(english, "Quick start"), "rust");
    let chinese_source = fenced_block(section(chinese, "快速开始"), "rust");
    assert_eq!(english_source, chinese_source);
    assert_eq!(english_source, fixture_source);

    let project = Path::new(env!("CARGO_MANIFEST_DIR"));
    let script = project.join("tests/readme_consumer_check.py");
    let output = Command::new("python3")
        .arg(script)
        .arg("--mode")
        .arg("local")
        .current_dir(project)
        .output()
        .expect("Python 3 must run the README consumer check");
    assert!(
        output.status.success(),
        "README consumer check failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
