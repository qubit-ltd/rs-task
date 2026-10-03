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
#[path = "redis_server.rs"]
pub mod redis_server;
#[path = "../../fixtures/doc-examples/src/task_event_codec.rs"]
pub mod task_event_codec;
#[path = "temporary_database.rs"]
pub mod temporary_database;
#[path = "../../fixtures/doc-examples/src/typed_support.rs"]
pub mod typed_support;
