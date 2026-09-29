// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Keeps the bilingual task codec snippets equal to the compiled fixture.

/// Extracts one explicitly delimited source region, rejecting missing markers.
fn extract_region<'a>(text: &'a str, start: &str, end: &str) -> &'a str {
    text.split_once(start)
        .expect("snippet start marker")
        .1
        .split_once(end)
        .expect("snippet end marker")
        .0
        .trim()
}

/// Compares both complete Rust snippets to the actual compiled codec module.
#[test]
fn test_task_event_codec_guides_match_compiled_fixture() {
    let source = include_str!("fixtures/doc-examples/src/task_event_codec.rs");
    let expected = extract_region(source, "// guide-codec-start", "// guide-codec-end");
    for guide in [
        include_str!("../doc/user-guide.md"),
        include_str!("../doc/user-guide.zh_CN.md"),
    ] {
        let region = extract_region(
            guide,
            "<!-- task-event-codec:start -->",
            "<!-- task-event-codec:end -->",
        );
        let snippet = extract_region(region, "```rust", "```");
        assert_eq!(snippet, expected, "guide codec must match the compiled source exactly");
    }
}
