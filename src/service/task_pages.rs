// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cursor-based pagination over task query results.

use crate::model::typed::TaskPage;
use crate::model::typed::TaskQuery;
use crate::service::TaskExecutionService;
use crate::service::TaskServiceError;

/// Iterates through query results by requesting one page at a time.
///
/// Each page uses the cursor returned by the preceding page. The iterator
/// retains the service borrow and query state, and stops after the final page.
///
/// # Examples
///
/// ```
/// use qubit_task::model::typed::TaskQuery;
/// use qubit_task::service::TaskExecutionService;
/// use qubit_task::service::TaskPages;
/// use qubit_task::service::TaskServiceError;
///
/// async fn consume_pages(
///     service: &TaskExecutionService,
///     query: TaskQuery,
/// ) -> Result<(), TaskServiceError> {
///     let mut pages: TaskPages<'_> = service.pages(query);
///     while let Some(page) = pages.next_page().await? {
///         let _record_count = page.records.len();
///     }
///     Ok(())
/// }
/// ```
#[must_use]
pub struct TaskPages<'a> {
    /// Service used to fetch each page while this iterator remains borrowed.
    service: &'a TaskExecutionService,
    /// Query with the next cursor, or `None` after the final page is fetched.
    query: Option<TaskQuery>,
}

impl<'a> TaskPages<'a> {
    /// Creates a paginator for a query originating from the borrowed service.
    ///
    /// # Parameters
    ///
    /// * `service` - Service whose store provides each requested page.
    /// * `query` - Initial filters, cursor, and page size.
    ///
    /// # Returns
    ///
    /// A paginator that retains the service borrow and advances the query
    /// cursor as pages are fetched.
    pub(crate) fn new(service: &'a TaskExecutionService, query: TaskQuery) -> Self {
        Self {
            service,
            query: Some(query),
        }
    }

    /// Fetches the next non-empty page, returning `None` for an empty or
    /// exhausted page.
    ///
    /// A query failure is returned without advancing the cursor, so callers
    /// may retry the same page request. If an empty page includes a continuation
    /// cursor, another call continues from that cursor.
    ///
    /// # Errors
    ///
    /// Returns the service error produced while querying the current cursor.
    pub async fn next_page(&mut self) -> Result<Option<TaskPage>, TaskServiceError> {
        let Some(query) = self.query.clone() else {
            return Ok(None);
        };
        let page = self.service.query(query.clone()).await?;
        if let Some(next) = page.next {
            self.query = Some(query.after(next));
        } else {
            self.query = None;
        }
        Ok((!page.records.is_empty()).then_some(page))
    }
}
