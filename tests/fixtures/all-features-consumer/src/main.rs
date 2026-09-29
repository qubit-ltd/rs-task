// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Verifies that an external crate can enable every published feature.

use qubit_task::TaskId;

fn main() {
    println!("all-feature consumer task id: {}", TaskId::generate());
}
