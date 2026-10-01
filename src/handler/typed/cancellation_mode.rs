// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Cancellation support declared by a handler registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancellationMode {
    /// The handler cannot stop an in-flight attempt on request.
    Unsupported,
    /// The handler observes the per-attempt cancellation signal in context.
    Cooperative,
    /// The service can invoke a registered external cancellation hook.
    ExternalHook,
}
