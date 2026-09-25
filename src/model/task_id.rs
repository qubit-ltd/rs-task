// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Identifies one accepted task submission.
///
/// The service generates IDs when it accepts requests. Callers should keep an
/// ID to query, cancel, or wait for that same submission; business identifiers
/// belong in a request's correlation key.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskId;
///
/// let id = TaskId::generate();
/// assert!(!id.to_string().is_empty());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(Uuid);

impl TaskId {
    /// Generates an unpredictable identifier for a new task submission.
    ///
    /// # Returns
    ///
    /// A randomly generated UUID-backed task identifier.
    #[must_use]
    #[inline]
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for TaskId {
    #[inline]
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
