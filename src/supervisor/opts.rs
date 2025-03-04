use super::spec::*;
use crate::node::Strategy;
use tokio::time::Duration;

/// Configuration value used to indicate in which order the child nodes of a
/// `supervisor::Spec` should start.
///
/// Since: 0.0.0
#[derive(Clone, Debug)]
pub enum StartOrder {
    /// Indicates that children nodes should be started from left to right
    LeftToRight,
    /// Indicates that children nodes should be started from right to left
    RightToLeft,
}

/// Represents a configuration option that can be set on a
/// `supervisor::Spec`.
///
/// Since: 0.0.0
pub struct Opt(Box<dyn FnMut(&mut Spec) + Send + Sync + 'static>);

impl Opt {
    pub(crate) fn new<F>(opt_fn: F) -> Self
    where
        F: FnMut(&mut Spec) + Send + Sync + 'static,
    {
        Self(Box::new(opt_fn))
    }

    pub(crate) fn call(&mut self, spec: &mut Spec) {
        self.0(spec)
    }
}

/// Specifies the start and termination order in of the child nodes for a
/// given `supervisor::Spec`.
///
/// If this configuration option is not specified, the default value
/// is `supervisor::StartOrder::LeftToRight`
///
/// Since: 0.0.0
pub fn with_start_order(order: StartOrder) -> Opt {
    Opt::new(move |spec| spec.start_order = order.clone())
}

/// Specifies the restart tolerance for the child nodes of a
/// given `supervisor::Spec`.
///
/// If this configuration option is not specified, the default value is one
/// error every five seconds.
///
/// Since: 0.0.0
pub fn with_restart_tolerance(max_allowed_restarts: u32, restart_window: Duration) -> Opt {
    Opt::new(move |spec| {
        spec.max_allowed_restarts = max_allowed_restarts;
        spec.restart_window = restart_window.clone();
    })
}

/// Specifies how children nodes of a given `supervisor::Spec`. get
/// restarted when one of the nodes fails.
///
/// If this configuration option is not specified, the default value is a
/// "one for one" restart strategy.
///
/// Since: 0.0.0
pub fn with_strategy(restart: Strategy) -> Opt {
    Opt::new(move |spec| spec.strategy = restart.clone())
}
