use crate::events::EventNotifier;
use crate::node::{self, leaf, root};
use crate::task;

use super::errors::*;
use super::spec::*;

/// A subtree that has a spawned task executing at runtime.
pub(crate) struct RunningSubtree {
    runtime_name: String,
    root_spec: root::Spec,
    task: task::RunningTask<String, StartError, node::TerminationMessage>,
    opts: Vec<leaf::Opt>,
    restart_strategy: node::Restart,
}

/// Internal value to allow RunningLeaf to implement the Debug trait
#[derive(Debug)]
struct RunningSubtreeDebug {
    runtime_name: String,
    root_spec: root::Spec,
    restart_strategy: node::Restart,
}

impl std::fmt::Debug for RunningSubtree {
    fn fmt(&self, format: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let data = RunningSubtreeDebug {
            runtime_name: self.runtime_name.clone(),
            root_spec: self.root_spec.clone(),
            restart_strategy: self.restart_strategy.clone(),
        };
        data.fmt(format)
    }
}

impl RunningSubtree {
    pub(crate) fn new(
        runtime_name: String,
        root_spec: root::Spec,
        task: task::RunningTask<String, StartError, node::TerminationMessage>,
        opts: Vec<leaf::Opt>,
        restart_strategy: node::Restart,
    ) -> Self {
        Self {
            runtime_name,
            root_spec,
            task,
            opts,
            restart_strategy,
        }
    }

    pub(crate) fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.root_spec.get_name()
    }

    /// Executes the termination logic of a supervision tree.
    pub(crate) async fn terminate(
        self,
        mut ev_notifier: EventNotifier,
    ) -> (Result<(), TerminationMessage>, Spec) {
        let runtime_name = self.get_runtime_name().to_owned();
        // Discard the previous task_spec as the capataz recreates the
        // supervision tree node using the provided build nodes function.
        let (result, _task_spec) = self.task.terminate().await;
        let spec = Spec::from_running_subtree(self.root_spec, self.opts, self.restart_strategy);

        // Unfortunately, due to limitations of the mpsc API, we need to return
        // a general `node::TerminationMessage` from the task API. Because of
        // this, you'll see in the patterns bellow the usage of
        // `node::TerminationMessage` rather than `TerminationMessage` type of this
        // module.
        match result {
            // Ignore this case as the error has been notified elsewhere.
            Err(task::TerminationMessage::TaskFailureNotified { .. }) => {
                let termination_err = TerminationMessage::StartErrorAlreadyReported.into();
                (Err(termination_err), spec)
            }
            // SAFETY: This will never happen because supervisors are never aborted
            Err(task::TerminationMessage::TaskAborted { .. }) => {
                unreachable!("invalid implementation; supervisor logic is never aborted")
            }
            // SAFETY: This will never happen because supervisors don't have a termination timeout
            Err(task::TerminationMessage::TaskForcedKilled { .. }) => {
                unreachable!("invalid implementation; supervisor logic has infinite timeout")
            }
            // SAFETY: This will never happen because supervisor logic must be panic free
            Err(task::TerminationMessage::TaskPanic { .. }) => {
                unreachable!("invalid implementation; supervisor logic should never panic")
            }
            // Supervisor gave up on restart logic
            Err(task::TerminationMessage::TaskFailed { err, .. }) => {
                match err {
                    node::TerminationMessage::Subtree(TerminationMessage::TerminationFailed(
                        termination_err,
                    )) => {
                        // Report the supervisor termination failure
                        ev_notifier
                            .supervisor_termination_failed(&runtime_name, termination_err.clone())
                            .await;
                        let termination_err =
                            TerminationMessage::TerminationFailed(termination_err);
                        (Err(termination_err), spec)
                    }
                    node::TerminationMessage::Subtree(TerminationMessage::ToManyRestarts(
                        to_many_restarts_err,
                    )) => {
                        // Report the supervisor termination failure
                        ev_notifier
                            .supervisor_restarted_to_many_times(
                                &runtime_name,
                                to_many_restarts_err.clone(),
                            )
                            .await;
                        let termination_err =
                            TerminationMessage::ToManyRestarts(to_many_restarts_err);
                        (Err(termination_err), spec)
                    }
                    other_err => {
                        // SAFETY: The scenario bellow should never happen, as
                        // the task body never returns values with a variant
                        // other than subtree::TerminationFailed.
                        unreachable!(
                            "implementation error; subtree nodes should never return these errors from a task. error: {:?}",
                            other_err,
                        )
                    }
                }
            }
            // The supervisor was terminated without any errors.
            Ok(_) => {
                ev_notifier.supervisor_terminated(&runtime_name).await;
                (Ok(()), spec)
            }
        }
    }

    pub(crate) fn get_restart_strategy(&self) -> &node::Restart {
        &self.restart_strategy
    }
}
