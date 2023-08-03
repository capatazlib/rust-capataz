use super::spec::*;

/// Configuration value used to indicate in which order the child nodes of a
/// `capataz::SupervisorSpec` should start.
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
/// `capataz::SupervisorSpec`.
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
