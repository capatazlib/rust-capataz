use std::sync::Arc;
use thiserror::Error;

use crate::node;
use crate::supervisor;

/// Represents an error reported by one of the child nodes at start time, it may
/// also include some termination error if the previously started nodes fail to
/// terminate on the start rollback procedure.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("supervisor failed to start: {start_err}")]
pub struct StartFailed {
    runtime_name: String,
    start_err: anyhow::Error,
    termination_err: Option<TerminationMessage>,
}

/// Represents an error reported by the capataz API that indicates a `supervisor::Spec`
/// could not build the nodes because of an allocation resource error. This is usually
/// seen when using `supervisor::Spec::new_with_cleanup`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("supervisor failed to build nodes: {build_err}")]
pub struct BuildFailed {
    runtime_name: String,
    build_err: anyhow::Error,
}

/// Unifies all possible errors that are reported when starting a
/// `supervisor::Spec`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
pub enum StartError {
    /// returns when a supervised worker failed to start running
    #[error("{0}")]
    StartFailed(Arc<StartFailed>),
    /// returns when the supervisor's worker builder function returns an error
    #[error("{0}")]
    BuildFailed(Arc<BuildFailed>),
}

impl StartError {
    /// Returns a start failed error wrapped in a
    /// `capataz::node::subtree::StartError`.
    pub(crate) fn start_failed(
        runtime_name: &str,
        start_err: anyhow::Error,
        termination_err: Option<TerminationMessage>,
    ) -> Self {
        StartError::StartFailed(Arc::new(StartFailed {
            runtime_name: runtime_name.to_owned(),
            start_err,
            termination_err,
        }))
    }

    /// Returns a `capataz::node::subtree::BuildFailed` error wrapped in a
    /// `capataz::node::subtree::StartError`.
    pub(crate) fn build_failed(runtime_name: &str, build_err: anyhow::Error) -> Self {
        StartError::BuildFailed(Arc::new(BuildFailed {
            runtime_name: runtime_name.to_owned(),
            build_err,
        }))
    }
}

/// Represents an error returned by one of the child nodes, when termination
/// logic is executing. There could be more than one termination error as the
/// termination procedure will continue despite the fact a previous child node
/// failed to terminate. This error may also include a cleanup error if the
/// resource deallocator fails with an error.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("supervisor failed to terminate")]
pub struct TerminationFailed {
    runtime_name: String,
    termination_err: Vec<node::TerminationMessage>,
    cleanup_err: Option<anyhow::Error>,
}

impl TerminationFailed {
    /// returns the runtime name of the supervisor that failed
    pub fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }
}

/// Unifies all possible errors that are reported when terminating a
/// `capataz::SupervisorSpec`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
pub enum TerminationMessage {
    /// returned when the supervisor failed to terminate
    #[error("{0}")]
    TerminationFailed(Arc<TerminationFailed>),
    #[error("{0}")]
    /// returned when the supervisor failed too many times
    TooManyRestarts(Arc<supervisor::TooManyRestarts>),
    #[error("{0}")]
    /// returned when a worker failed to restart
    StartErrorOnRestart(Arc<node::StartError>),
    /// returned when a start error was already returned. This is an implementation detail
    #[error("start error already reported")]
    StartErrorAlreadyReported,
}

impl TerminationMessage {
    /// returns the name of the supervisor that failed to terminate
    pub fn get_runtime_name(&self) -> &str {
        match &self {
            Self::TerminationFailed(termination_err) => termination_err.get_runtime_name(),
            Self::TooManyRestarts(too_many_restarts) => too_many_restarts.get_runtime_name(),
            Self::StartErrorOnRestart(start_err) => start_err.get_runtime_name(),
            Self::StartErrorAlreadyReported => {
                unreachable!("invalid implementation; start error is never delivered via sup_chan")
            }
        }
    }

    /// returns the original error that caused the supervisor to fail
    pub fn get_cause_err(self) -> anyhow::Error {
        match self {
            Self::TerminationFailed(termination_err) => anyhow::Error::new(termination_err),
            Self::TooManyRestarts(too_many_restarts) => anyhow::Error::new(too_many_restarts),
            Self::StartErrorOnRestart(start_err) => anyhow::Error::new(start_err),
            Self::StartErrorAlreadyReported => {
                unreachable!("invalid implementation; start error is never delivered via sup_chan")
            }
        }
    }

    pub(crate) fn restart_failed(start_err: node::StartError) -> Self {
        Self::StartErrorOnRestart(Arc::new(start_err))
    }

    pub(crate) fn termination_failed(
        runtime_name: &str,
        termination_err: Vec<node::TerminationMessage>,
        cleanup_err: Option<anyhow::Error>,
    ) -> Self {
        Self::TerminationFailed(Arc::new(TerminationFailed {
            runtime_name: runtime_name.to_owned(),
            termination_err,
            cleanup_err,
        }))
    }
}
