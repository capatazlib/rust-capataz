use crate::node;
use crate::task;

/// Represents a configuration option for a `capataz::Node` of type worker.
///
/// Since: 0.0.0
pub struct Opt(
    Box<
        dyn FnMut(&mut task::TaskSpec<String, anyhow::Error, node::TerminationMessage>)
            + Send
            + Sync
            + 'static,
    >,
);

impl Opt {
    pub(crate) fn new<F>(opt_fn: F) -> Self
    where
        F: FnMut(&mut task::TaskSpec<String, anyhow::Error, node::TerminationMessage>)
            + Send
            + Sync
            + 'static,
    {
        Self(Box::new(opt_fn))
    }

    pub(crate) fn call(
        &mut self,
        spec: &mut task::TaskSpec<String, anyhow::Error, node::TerminationMessage>,
    ) {
        self.0(spec)
    }
}
