use std::sync::Arc;

use crate::events::EventNotifier;
use crate::{node, task};

use super::errors::*;
use super::opts::*;
use super::spec::*;

/// Represents an error returned by the business logic from capataz API users
type BusinessStartError = anyhow::Error;

/// Represents a leaf node that has an existing spawned task in runtime.
pub(crate) struct RunningLeaf {
    name: String,
    runtime_name: String,
    task: task::RunningTask<node::RuntimeName, BusinessStartError, node::TerminationMessage>,
    opts: Vec<Opt>,
}

impl std::fmt::Debug for RunningLeaf {
    fn fmt(&self, format: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(format, "RunningLeaf [{} {}]", self.name, self.runtime_name,)
    }
}

impl RunningLeaf {
    pub(crate) fn new(
        name: String,
        runtime_name: String,
        task: task::RunningTask<node::RuntimeName, BusinessStartError, node::TerminationMessage>,
        opts: Vec<Opt>,
    ) -> Self {
        Self {
            name,
            runtime_name,
            task,
            opts,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    // pub(crate) fn get_runtime_name(&self) -> &str {
    //     &self.runtime_name
    // }

    pub(crate) fn get_restart(&self) -> task::Restart {
        self.task.get_restart()
    }

    /// Executes the termination logic of a `capataz::Node`. This method will
    /// block until the spawned task is guaranteed to be terminated.
    pub(crate) async fn terminate(
        self,
        mut ev_notifier: EventNotifier,
    ) -> (Result<(), TerminationMessage>, Spec) {
        let runtime_name = self.runtime_name.clone();

        let (result, task_spec) = self.task.terminate().await;
        let spec = Spec::from_running_leaf(self.name, self.opts, task_spec);

        // Unfortunately, due to limitations of the mpsc API, we need to return
        // a general `node::TerminationMessage` from the task API. Because of
        // this, you'll see in the patterns bellow the usage of
        // `node::TerminationMessage` rather than `TerminationMessage` type of this
        // module.
        match result {
            // The termination failed with an expected business error.
            Err(task::TerminationMessage::TaskFailed { err, .. }) => {
                match err {
                    node::TerminationMessage::Leaf(TerminationMessage::TerminationFailed(
                        termination_err,
                    )) => {
                        ev_notifier
                            .worker_termination_failed(&runtime_name, termination_err.clone())
                            .await;
                        let termination_err =
                            TerminationMessage::TerminationFailed(termination_err);
                        (Err(termination_err), spec)
                    }
                    // The scenario bellow should never happen, as the body of a
                    // leaf node never returns `subtree::TerminationMessage`
                    // values; if it does we have an implementation error.
                    other_err => {
                        unreachable!(
                            "implementation error; leaf nodes should never return these errors from a task. error: {:?}",
                            other_err,
                        )
                    }
                }
            }
            // When the task gets forced killed, is because it took too long to
            // finish. Transform this signal to a TimedOut error.
            Err(task::TerminationMessage::TaskForcedKilled { .. }) => {
                let termination_err = Arc::new(TerminationTimedOut::new(&runtime_name));
                ev_notifier
                    .worker_termination_timed_out(&runtime_name, termination_err.clone())
                    .await;
                let termination_err = TerminationMessage::TerminationTimedOut(termination_err);
                (Err(termination_err), spec)
            }
            // When the task panics, the task API returns this error. Transform
            // this signal to a TerminationPanicked error.
            Err(task::TerminationMessage::TaskPanic { .. }) => {
                // TODO: add panic metadata
                let termination_err = Arc::new(TerminationPanicked::new(&runtime_name));
                ev_notifier
                    .worker_termination_panicked(&runtime_name, termination_err.clone())
                    .await;
                let termination_err = TerminationMessage::TerminationPanicked(termination_err);
                (Err(termination_err), spec)
            }
            // When a parent supervisor restarts a node, it invokes it's
            // termination first. Because we already sent the error to the
            // supervisor, it is valid to return an Ok result here.
            //
            // Also, we do not need to do a notification of failure, because
            // that was already done when the error was sent.
            Err(task::TerminationMessage::TaskFailureNotified) => (Ok(()), spec),
            Err(task::TerminationMessage::TaskAborted) => {
                unreachable!("validate this is being visited")
            }
            // Happy path.
            Ok(_) => {
                ev_notifier.worker_terminated(&runtime_name).await;
                (Ok(()), spec)
            }
        }
    }
}
