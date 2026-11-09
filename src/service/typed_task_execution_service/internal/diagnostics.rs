// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
use crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
use crate::model::TaskState;

/// Bounds a diagnostic by bytes while preserving valid UTF-8.
///
/// # Parameters
///
/// - `value`: Diagnostic text to bound.
/// - `max_bytes`: Maximum output length in bytes.
///
/// # Returns
///
/// Returns the original text when it fits, or a UTF-8-safe truncated prefix
/// within `max_bytes`.
pub(super) fn bound_diagnostic(value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let marker = if max_bytes >= 3 { "..." } else { "" };
    let content_limit = max_bytes - marker.len();
    let mut boundary = content_limit.min(value.len());
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let mut result = String::with_capacity(boundary + marker.len());
    result.push_str(&value[..boundary]);
    result.push_str(marker);
    result
}

/// Bounds every diagnostic field accepted by a task state.
///
/// # Parameters
///
/// - `state`: Task state whose persisted diagnostic fields need bounding.
///
/// # Returns
///
/// Returns the state with each persisted diagnostic bounded by its field limit.
pub(super) fn bound_state_diagnostics(state: TaskState) -> TaskState {
    match state {
        TaskState::Failed { category, message } => TaskState::Failed {
            category: bound_diagnostic(category, MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES),
            message: bound_diagnostic(message, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
        TaskState::Blocked { reason } => TaskState::Blocked {
            reason: bound_diagnostic(reason, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
        TaskState::Panicked { message } => TaskState::Panicked {
            message: bound_diagnostic(message, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::bound_diagnostic;

    #[test]
    fn test_bound_diagnostic_preserves_utf8_and_reserves_marker() {
        assert_eq!(bound_diagnostic(String::new(), 0), "");
        assert_eq!(bound_diagnostic("abc".into(), 3), "abc");
        assert_eq!(bound_diagnostic("abcdef".into(), 5), "ab...");
        assert_eq!(bound_diagnostic("abcdef".into(), 2), "ab");
        assert_eq!(bound_diagnostic("界🙂x".into(), 5), "...");
        assert_eq!(bound_diagnostic("界🙂x".into(), 7), "界...");
        let output = bound_diagnostic("🙂x".into(), 4);
        assert!(output.len() <= 4);
        assert!(output.is_char_boundary(output.len()));
    }
}
