// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

/// Serializable projection of a stage reported by `rs-progress`.
///
/// Optional position and stage-count values remain absent when the source does
/// not provide them; the snapshot does not infer either value.
///
/// # Examples
///
/// ```
/// use qubit_task::model::ProgressStageSnapshot;
///
/// let stage = ProgressStageSnapshot {
///     id: "download".to_owned(),
///     name: "Download files".to_owned(),
///     position: Some(1),
///     total: Some(3),
/// };
///
/// let encoded = serde_json::to_string(&stage).expect("serialize stage snapshot");
/// let decoded: ProgressStageSnapshot =
///     serde_json::from_str(&encoded).expect("deserialize stage snapshot");
/// assert_eq!(decoded, stage);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressStageSnapshot {
    /// Machine-readable identifier supplied for the stage.
    pub id: String,
    /// Human-readable name supplied for display or reporting.
    pub name: String,
    /// One-based position of this stage in the sequence, if supplied by the
    /// source.
    pub position: Option<u64>,
    /// Number of stages in the sequence, if supplied by the source.
    pub total: Option<u64>,
}
