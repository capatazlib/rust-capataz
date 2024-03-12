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

/// Changes the restart strategy of a Node
pub fn with_restart(restart: task::Restart) -> Opt {
    return Opt::new(move |spec| spec.with_restart(restart.clone()));
}

/// Specifies how long a client API is willing to wait for the start of this
/// `capataz::Node`.
///
/// If this configuration option is not specified, there is no timeout for
/// the node start.
///
/// Since: 0.0.0
pub fn with_start_timeout(duration: std::time::Duration) -> Opt {
    Opt::new(move |task| {
        task.with_startup(task::Startup::Timeout(duration.clone()));
    })
}

/// Specifies how long a client API is willing to wait for the termination
/// of this `capataz::Node`.
///
/// If this configuration option is not specified, there is no timeout
/// for the node termination.
///
/// Since: 0.0.0
pub fn with_termination_timeout(duration: std::time::Duration) -> Opt {
    Opt::new(move |task| {
        task.with_shutdown(task::Shutdown::Timeout(duration.clone()));
    })
}
