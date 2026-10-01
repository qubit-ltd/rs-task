// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::sync::Arc;

use crate::store::LegacyTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;
/// Opens stores in one fresh, isolated namespace; repeated opens address the
/// same namespace. The fixture owns disposable namespace cleanup after all
/// returned stores are dropped.
pub trait StoreFixture: Send + Sync {
    /// Opens a store, returning backend errors including `OwnerConflict` for an
    /// active owner.
    fn open<'a>(&'a self) -> TaskFuture<'a, Result<Arc<dyn LegacyTaskStore>, StoreError>>;
}
