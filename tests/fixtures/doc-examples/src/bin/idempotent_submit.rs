// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates idempotent replay with typed, encoded requests.

#[path = "../typed_support.rs"]
mod typed_support;

use typed_support::register_handler;
use typed_support::request;
use typed_support::service_builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = service_builder()?;
    register_handler(&mut builder)?;
    let service = builder.build().await?;

    let original = request(serde_json::json!({"object": "imports/42.csv"}), "request-42");
    let first = service.submit(original.clone()).await?;
    let replay = service.submit(original).await?;
    assert_eq!(replay.id, first.id, "an identical retry keeps its task ID");

    let conflict = service
        .submit(request(serde_json::json!({"object": "imports/43.csv"}), "request-42"))
        .await;
    assert!(conflict.is_err(), "a changed request cannot reuse an idempotency key");

    service.shutdown().await?;
    Ok(())
}
