use crate::model::typed::{TaskPage, TaskQuery};
use crate::service::{TaskExecutionService, TaskServiceError};

pub struct TaskPages<'a> {
    service: &'a TaskExecutionService,
    query: Option<TaskQuery>,
}

impl<'a> TaskPages<'a> {
    pub(crate) fn new(service: &'a TaskExecutionService, query: TaskQuery) -> Self {
        Self {
            service,
            query: Some(query),
        }
    }

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
