// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state for bounded task event publication.

mod counters;

pub(in crate::service::task_event_publisher) use counters::Counters;
