use super::spec::*;
use crate::context::Context;

/// Internal representation of the closure function used by the
/// `supervisor::Spec::new_with_cleanup` method.
///
pub(crate) struct BuildNodesFn(
    Box<dyn (FnMut(Context) -> Result<Nodes, anyhow::Error>) + Send + Sync + 'static>,
);

impl BuildNodesFn {
    /// Executes the build nodes function.
    pub(crate) fn call(&mut self, ctx: Context) -> Result<Nodes, anyhow::Error> {
        self.0(ctx)
    }
    /// Creates a builder function that returns new nodes. The Nodes value may
    /// also contain a cleanup function to deallocate resources.
    pub(crate) fn new<F>(build_nodes_fn: F) -> Self
    where
        F: FnMut(Context) -> Result<Nodes, anyhow::Error> + Send + Sync + 'static,
    {
        BuildNodesFn(Box::new(build_nodes_fn))
    }
}
