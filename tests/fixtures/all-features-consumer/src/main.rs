// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies that an external crate can enable every published feature.

use qubit_id::Id;
use qubit_task::model::TaskId;

fn main() {
    let id = TaskId::from_id(Id::new(42));
    assert_eq!(id.to_padded_decimal(), "00000000000000000042");
    println!("all-feature consumer task id: {id}");
}
