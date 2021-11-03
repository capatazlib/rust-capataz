/// A callback function that cleans up resources that were allocated in the
/// construction of a supervision tree via the `capataz::Spec::new_with_cleanup`
/// method.
///
pub(crate) struct CleanupFn(Box<dyn FnOnce() -> Result<(), anyhow::Error> + Send + Sync + 'static>);

impl CleanupFn {
    /// Creates a `capataz::CleanupFn` value. This function must be used when
    /// invoking the `capataz::Spec::new_with_cleanup` method.
    ///
    pub(crate) fn new<F>(cleanup_fn: F) -> Self
    where
        F: FnOnce() -> Result<(), anyhow::Error> + Send + Sync + 'static,
    {
        CleanupFn(Box::new(cleanup_fn))
    }

    /// Creates a `capataz::CleanupFn` that does nothing and always succeeds.
    /// This function is used from public APIs that don't offer the option to
    /// provide a `capataz::CleanupFn`.
    pub(crate) fn empty() -> Self {
        CleanupFn(Box::new(|| Ok(())))
    }

    /// Invokes the cleanup business logic provided by API users on a
    /// `capataz::CleanupFn::new` call.
    pub(crate) fn call(self) -> Result<(), anyhow::Error> {
        self.0()
    }
}
