// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared fixtures for the Redis-backed task outbox integration test.
#![allow(dead_code)]

#[path = "controlled_redis.rs"]
pub mod controlled_redis;
#[path = "interrupt_before_mark.rs"]
pub mod interrupt_before_mark;
pub use crate::support::redis_server;
pub use crate::support::task_event_codec;
#[path = "temporary_database.rs"]
pub mod temporary_database;
#[path = "../../fixtures/doc-examples/src/typed_support.rs"]
pub mod typed_support;
