// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;

/// Mutable aggregate of resources currently reserved by active attempts.
#[derive(Default)]
pub(in crate::engine::local) struct Usage {
    /// Reserved CPU slots.
    pub(in crate::engine::local) cpu: u32,
    /// Reserved GPU identifiers.
    pub(in crate::engine::local) gpus: Vec<String>,
    /// Reserved custom resource amounts.
    pub(in crate::engine::local) custom: BTreeMap<String, u64>,
}
