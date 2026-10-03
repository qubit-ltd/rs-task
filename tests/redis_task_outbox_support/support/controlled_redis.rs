// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis proxy that can hold a reply after the upstream command has completed.

#[path = "controlled_redis/gate.rs"]
mod gate;
#[path = "controlled_redis/proxy.rs"]
pub mod proxy;
