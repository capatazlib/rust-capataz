use std::sync::Arc;
use thiserror::Error;

/// Represents an error reported by some bussiness logic API in a
/// `capataz::Node` when trying to spawn a task (green thread).
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("worker failed to start: {start_err}")]
pub struct StartFailed {
    runtime_name: String,
    start_err: anyhow::Error,
}

impl StartFailed {
    pub(crate) fn new<S>(runtime_name: S, start_err: anyhow::Error) -> Self
    where
        S: Into<String>,
    {
        Self {
            runtime_name: runtime_name.into(),
            start_err,
        }
    }
}

/// Represents an error reported by the capataz API that indicates a
/// `capataz::Node` takes longer than allowed to get started.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("worker timed out on start")]
pub struct StartTimedOut {
    runtime_name: String,
}

impl StartTimedOut {
    pub(crate) fn new<S>(runtime_name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            runtime_name: runtime_name.into(),
        }
    }
}

/// Unifies all possible errors that are reported when starting a
/// `capataz::Node`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
pub enum StartError {
    #[error("{0}")]
    StartTimedOut(Arc<StartTimedOut>),
    #[error("{0}")]
    StartFailed(Arc<StartFailed>),
}

/// Represents an error returned by a `capataz::Node` business logic, it usually
/// indicates an expected error scenario. This error the is propagated to
/// supervisors which then would trigger a restart procedure.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("worker failed to terminate: {termination_err}")]
pub struct TerminationFailed {
    runtime_name: String,
    termination_err: anyhow::Error,
}

impl TerminationFailed {
    pub(crate) fn new<S>(runtime_name: S, termination_err: anyhow::Error) -> Self
    where
        S: Into<String>,
    {
        Self {
            runtime_name: runtime_name.into(),
            termination_err,
        }
    }
    pub fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }
}

/// Represents an error reported by the capataz API that indicates a
/// `capataz::Node` takes too long than allowed to terminate.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("worker took too long to terminate")]
pub struct TerminationTimedOut {
    runtime_name: String,
}

impl TerminationTimedOut {
    pub(crate) fn new<S>(runtime_name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            runtime_name: runtime_name.into(),
        }
    }

    pub fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }
}

/// Represents an unexpected error in a `capataz::Node` business logic. This
/// error is propagated to supervisors which then would trigger a restart
/// procedure.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("worker panicked at runtime")]
// TODO: add panic metadata
pub struct TerminationPanicked {
    runtime_name: String,
}

impl TerminationPanicked {
    pub(crate) fn new<S>(runtime_name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            runtime_name: runtime_name.into(),
        }
    }

    pub fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }
}

/// Unifies all possible errors that are reported when terminating a
/// `capataz::Node`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
pub enum TerminationMessage {
    #[error("{0}")]
    TerminationFailed(Arc<TerminationFailed>),
    #[error("{0}")]
    TerminationTimedOut(Arc<TerminationTimedOut>),
    #[error("{0}")]
    TerminationPanicked(Arc<TerminationPanicked>),
}

impl TerminationMessage {
    pub fn get_runtime_name(&self) -> &str {
        match &self {
            TerminationMessage::TerminationFailed(termination_err) => {
                termination_err.get_runtime_name()
            }
            TerminationMessage::TerminationPanicked(termination_err) => {
                termination_err.get_runtime_name()
            }
            TerminationMessage::TerminationTimedOut(termination_err) => {
                termination_err.get_runtime_name()
            }
        }
    }

    pub fn get_cause_err(&self) -> anyhow::Error {
        match &self {
            TerminationMessage::TerminationFailed(termination_err) => {
                anyhow::Error::new(termination_err.clone())
            }
            TerminationMessage::TerminationPanicked(termination_err) => {
                anyhow::Error::new(termination_err.clone())
            }
            TerminationMessage::TerminationTimedOut(termination_err) => {
                anyhow::Error::new(termination_err.clone())
            }
        }
    }

    pub(crate) fn from_task_error<S>(runtime_name: S, task_err: anyhow::Error) -> Self
    where
        S: Into<String>,
    {
        TerminationMessage::TerminationFailed(Arc::new(TerminationFailed::new(
            runtime_name.into(),
            task_err,
        )))
    }
}
