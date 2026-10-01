// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Keeps the bilingual user guides aligned with the typed task API contract.

/// Checks that both guides link to their typed API guide and describe event
/// authority.
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
        english.contains("Typed task lifecycle publication is not currently wired into the typed execution service")
            && english.contains("consumers should not treat transport events as the authoritative task record"),
        "English user guide must state that task queries are authoritative until lifecycle publication is wired"
    );
    assert!(
        chinese.contains("typed execution service 尚未接入 typed 任务生命周期事件发布")
            && chinese.contains("消费者应以任务查询接口返回的记录为准"),
        "Chinese user guide must state that task queries are authoritative until lifecycle publication is wired"
    );
}
