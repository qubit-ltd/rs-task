// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::Any;
use std::sync::Arc;

/// Runtime configuration selected for a task store provider.
///
/// # Examples
///
/// ```
/// use qubit_task::spi::TaskStoreConfig;
///
/// let config = TaskStoreConfig::default();
/// assert!(matches!(config, TaskStoreConfig::Memory { .. }));
/// ```
#[derive(Clone)]
pub enum TaskStoreConfig {
    /// Volatile records with a bounded terminal history.
    Memory {
        /// Maximum retained terminal records.
        history_capacity: usize,
    },
    /// SQLite records with restart recovery.
    #[cfg(feature = "sqlite")]
    Sqlite {
        /// Database path owned by the service.
        path: std::path::PathBuf,
    },
    /// Provider-specific runtime settings for an application or extension
    /// crate.
    Custom(
        /// Type-erased configuration inspected by its selected provider.
        Arc<dyn Any + Send + Sync>,
    ),
}

impl Default for TaskStoreConfig {
    /// Selects in-memory storage with the standard history bound.
    ///
    /// # Returns
    ///
    /// A memory-store configuration retaining up to 1,024 terminal records.
    fn default() -> Self {
        Self::Memory { history_capacity: 1024 }
    }
}

impl TaskStoreConfig {
    /// Wraps a typed extension configuration for one selected provider.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Concrete configuration type inspected by the provider.
    ///
    /// # Parameters
    ///
    /// * `config` - Provider-specific settings value.
    ///
    /// # Returns
    ///
    /// A custom configuration holding the value behind shared type erasure.
    #[must_use]
    pub fn custom<T: Any + Send + Sync>(config: T) -> Self {
        Self::Custom(Arc::new(config))
    }

    /// Borrows extension configuration when its concrete type matches `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Concrete type requested by the selected provider.
    ///
    /// # Returns
    ///
    /// A reference to the stored value when it has type `T`, or `None` for a
    /// different configuration variant or concrete type.
    #[must_use]
    #[inline]
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        match self {
            Self::Custom(config) => config.downcast_ref(),
            _ => None,
        }
    }
}
