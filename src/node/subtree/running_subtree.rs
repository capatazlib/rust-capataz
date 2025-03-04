use std::fmt;

use crate::events::EventNotifier;
use crate::node::{self, leaf};
use crate::supervisor;
use crate::task;

use super::errors::*;
use super::spec::*;

/// A subtree that has a spawned task executing at runtime.
pub(crate) struct RunningSubtree {
    runtime_name: String,
    spec: supervisor::Spec,
    task: task::RunningTask<node::RuntimeName, StartError, node::TerminationMessage>,
    opts: Vec<leaf::Opt>,
    strategy: node::Strategy,
}

impl fmt::Debug for RunningSubtree {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        fmt.debug_struct("RunningSubtree")
            .field("runtime_name", &self.runtime_name)
            .field("spec", &self.spec)
            .field("strategy", &self.strategy)
            .finish()
    }
}

impl RunningSubtree {
    pub(crate) fn new(
        runtime_name: String,
        spec: supervisor::Spec,
        task: task::RunningTask<node::RuntimeName, StartError, node::TerminationMessage>,
        opts: Vec<leaf::Opt>,
        strategy: node::Strategy,
    ) -> Self {
        Self {
            runtime_name,
            spec,
            task,
            opts,
            strategy,
        }
    }

    pub(crate) fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.spec.get_name()
    }

    pub(crate) fn get_restart(&self) -> task::Restart {
        self.task.get_restart()
    }

    pub(crate) async fn wait(
        self,
        mut ev_notifier: EventNotifier,
    ) -> (Result<(), TerminationMessage>, Spec) {
        let runtime_name = self.get_runtime_name().to_owned();
        // Discard the previous task_spec as the capataz library recreates the
        // supervision tree node using the provided build nodes function.
        let (result, _task_spec) = self.task.wait().await;
        let spec = Spec::from_running_subtree(self.spec, self.opts, self.strategy);
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
                    node::TerminationMessage::Subtree(TerminationMessage::TooManyRestarts(
                        too_many_restarts_err,
                    )) => {
                        // Report the supervisor termination failure
                        ev_notifier
                            .supervisor_restarted_too_many_times(
                                &runtime_name,
                                too_many_restarts_err.clone(),
                            )
                            .await;
                        let termination_err =
                            TerminationMessage::TooManyRestarts(too_many_restarts_err);
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

    /// Executes the termination logic of a supervision tree.
    pub(crate) async fn terminate(
        self,
        mut ev_notifier: EventNotifier,
    ) -> (Result<(), TerminationMessage>, Spec) {
        let runtime_name = self.get_runtime_name().to_owned();
        // Discard the previous task_spec as the capataz library recreates the
        // supervision tree node using the provided build nodes function.
        let (result, _task_spec) = self.task.terminate().await;
        let spec = Spec::from_running_subtree(self.spec, self.opts, self.strategy);

        // Unfortunately, due to limitations of the mpsc API, we need to return
        // a general `node::TerminationMessage` from the task API. Because of
        // this, you'll see in the patterns bellow the usage of
        // `node::TerminationMessage` rather than `TerminationMessage` type of
        // this module.
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
                    node::TerminationMessage::Subtree(TerminationMessage::TooManyRestarts(
                        too_many_restarts_err,
                    )) => {
                        // Report the supervisor termination failure
                        ev_notifier
                            .supervisor_restarted_too_many_times(
                                &runtime_name,
                                too_many_restarts_err.clone(),
                            )
                            .await;
                        let termination_err =
                            TerminationMessage::TooManyRestarts(too_many_restarts_err);
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
}
