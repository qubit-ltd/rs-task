// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Holds per-task notification state and the scoped subscription type.
mod entry;
mod wait_subscription;

pub(in crate::service::task_wait_registry) use entry::Entry;
pub(in crate::service) use wait_subscription::WaitSubscription;
