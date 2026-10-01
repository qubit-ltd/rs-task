//! Typed handler contracts and their runtime payload adapter.

mod cancellation_mode;
mod external_cancellation_hook;
mod handler_dispatch_error;
mod handler_registration_error;
mod prepared_task;
mod task_handler_descriptor;
mod typed_task_context;
mod typed_task_handler;
mod typed_task_handler_registry;

#[cfg(not(test))]
pub use cancellation_mode::CancellationMode;
#[cfg(not(test))]
pub use external_cancellation_hook::ExternalCancellationHook;
#[cfg(not(test))]
pub use handler_dispatch_error::HandlerDispatchError;
#[cfg(not(test))]
pub use handler_registration_error::HandlerRegistrationError;
pub(crate) use prepared_task::PreparedTask;
#[cfg(not(test))]
pub use task_handler_descriptor::TaskHandlerDescriptor;
#[cfg(not(test))]
pub use typed_task_context::TypedTaskContext;
#[cfg(not(test))]
pub use typed_task_handler::TypedTaskHandler;
#[cfg(not(test))]
pub use typed_task_handler_registry::TypedTaskHandlerRegistry;
