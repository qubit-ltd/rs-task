// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::SchedulingPolicy;

/// Factory contract used by the application or SPI assembly.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_task::scheduling::FairFifoPolicy;
/// use qubit_task::scheduling::SchedulingPolicy;
/// use qubit_task::scheduling::SchedulingPolicyProvider;
///
/// struct Provider;
/// impl SchedulingPolicyProvider for Provider {
///     fn create(&self) -> Result<Arc<dyn SchedulingPolicy>, String> {
///         Ok(Arc::new(FairFifoPolicy::default()))
///     }
/// }
///
/// assert!(Provider.create().is_ok());
/// ```
pub trait SchedulingPolicyProvider: Send + Sync {
    /// Creates one scheduling policy instance.
    ///
    /// # Returns
    ///
    /// The configured policy, or a diagnostic explaining why creation failed.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot construct its policy.
    fn create(&self) -> Result<Arc<dyn SchedulingPolicy>, String>;
}
