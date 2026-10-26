// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Holds the private erased dispatch contract and its typed adapter.
mod erased_handler;
mod registration;
mod typed_adapter;

pub(super) use erased_handler::ErasedHandler;
pub(super) use registration::Registration;
pub(super) use typed_adapter::TypedAdapter;
