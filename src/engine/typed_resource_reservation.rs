/// RAII lease for resources reserved by the typed task execution path.
#[must_use]
pub struct TypedResourceReservation {
    release: Option<Box<dyn FnOnce() + Send>>,
}

impl TypedResourceReservation {
    /// Creates a reservation lease that releases its resources when dropped.
    pub(crate) fn new(release: Box<dyn FnOnce() + Send>) -> Self {
        Self { release: Some(release) }
    }
}

impl Drop for TypedResourceReservation {
    /// Releases resources when the lease is dropped.
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}
