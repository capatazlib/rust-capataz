use futures::future::{BoxFuture, FutureExt};
use std::sync::Arc;
use thiserror::Error;

use crate::context::Context;
use crate::events::EventNotifier;
use crate::node::{self, leaf, root};
use crate::notifier;
use crate::task;

/// Specific instance that reports successful starts or start error in a subtree
type StartNotifier = notifier::StartNotifier<StartError>;

/// Specific instance that reports termination errors in a subtree.
type TerminationNotifier =
    notifier::TerminationNotifier<task::TerminationError<node::TerminationError>>;

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
    termination_err: Option<TerminationError>,
}

/// Represents an error reported by the capataz API that indicates a SupervisorSpec
/// could not build the nodes because of an allocation resource error. This is usually
/// seen when using `capataz::SupervisorSpec::new_with_cleanup`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("supervisor failed to build nodes: {build_err}")]
pub struct BuildFailed {
    runtime_name: String,
    build_err: anyhow::Error,
}

/// Unifies all possible errors that are reported when starting a
/// `capataz::SupervisorSpec`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
pub enum StartError {
    #[error("{0}")]
    StartFailed(Arc<StartFailed>),
    #[error("{0}")]
    BuildFailed(Arc<BuildFailed>),
}

impl StartError {
    /// Returns a start failed error wrapped in a
    /// `capataz::node::subtree::StartError`.
    pub(crate) fn start_failed(
        runtime_name: &str,
        start_err: anyhow::Error,
        termination_err: Option<TerminationError>,
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
    termination_err: Vec<node::TerminationError>,
    cleanup_err: Option<anyhow::Error>,
}

/// Unifies all possible errors that are reported when terminating a
/// `capataz::SupervisorSpec`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
pub enum TerminationError {
    #[error("{0}")]
    TerminationFailed(Arc<TerminationFailed>),
    #[error("start error already reported")]
    StartErrorAlreadyReported,
}

impl TerminationError {
    pub(crate) fn termination_failed(
        runtime_name: &str,
        termination_err: Vec<node::TerminationError>,
        cleanup_err: Option<anyhow::Error>,
    ) -> Self {
        TerminationError::TerminationFailed(Arc::new(TerminationFailed {
            runtime_name: runtime_name.to_owned(),
            termination_err,
            cleanup_err,
        }))
    }
}

pub(crate) struct Spec {
    spec: root::Spec,
    opts: Vec<leaf::Opt>,
}

/// A subtree that has a spawned task executing at runtime.
#[derive(Debug)]
pub(crate) struct RunningSubtree(task::RunningTask<(), node::TerminationError>);

impl Spec {
    pub(crate) fn new(spec: root::Spec, opts: Vec<leaf::Opt>) -> Self {
        Self { spec, opts }
    }

    // Executes the `capataz::SupervisorSpec` run logic in a new spawned task.
    pub(crate) fn start(
        self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        parent_name: String,
        parent_chan: Option<TerminationNotifier>,
    ) -> BoxFuture<'static, Result<RunningSubtree, StartError>> {
        // The function must return a boxed future rather than using async fn
        // because this is a co-recursive function; `subtree::Spec::start`
        // relies on `Node::start` which relies on `subtree::Spec::start`.
        //
        // For more details see:
        // https://rust-lang.github.io/async-book/07_workarounds/04_recursion.html
        let Self { spec, .. } = self;
        async move {
            // Get all metadata needed for the subtree task execution
            let subtree_name = spec.get_name().to_owned();
            let subtree_parent_name = parent_name.clone();
            let subtree_ev_notifier = ev_notifier.clone();

            // Build an async task for the supervision tree.
            let task_spec = task::TaskSpec::new_with_start(
                subtree_name,
                move |ctx: Context, start_notifier: StartNotifier| async move {
                    spec.run(
                        ctx,
                        subtree_ev_notifier.clone(),
                        start_notifier,
                        &subtree_parent_name,
                    )
                    .await
                },
            );

            // Start the supervision tree.
            let runtime_name = task_spec.build_runtime_name(&parent_name);
            let result = task_spec.start(&ctx, &parent_name, parent_chan).await;

            match result {
                // SAFETY: The only way this error occurs is if we implemented
                // the supervision logic wrong.
                Err(task::StartError::StartRecvError(_)) => {
                    unreachable!("invalid implementation; supervisors always listen to channel")
                }
                // SAFETY: The only way this error occurs is if we implemented
                // the supervision logic wrong.
                Err(task::StartError::StartTimeoutError(_)) => {
                    unreachable!("invalid implementation; supervisors don't have a start timeout")
                }
                // A start error notification was signalled via the provided
                // `start_notifier`, return the error to our creator.
                Err(task::StartError::BusinessLogicFailed(StartError::BuildFailed(build_err))) => {
                    ev_notifier
                        .supervisor_build_failed(&runtime_name, build_err.clone())
                        .await;
                    Err(StartError::BuildFailed(build_err))
                }
                // A start error notification was signalled via the provided
                // `start_notifier`, return the error to our creator.
                Err(task::StartError::BusinessLogicFailed(StartError::StartFailed(start_err))) => {
                    ev_notifier
                        .supervisor_start_failed(&runtime_name, start_err.clone())
                        .await;
                    Err(StartError::StartFailed(start_err))
                }
                // The supervisor (including it's child nodes) started without
                // any errors
                Ok(running_supervisor) => {
                    ev_notifier.supervisor_started(&runtime_name).await;
                    Ok(RunningSubtree(running_supervisor))
                }
            }
        }
        .boxed()
    }
}

impl RunningSubtree {
    /// Executes the termination logic of a supervision tree.
    pub(crate) async fn terminate(
        self,
        mut ev_notifier: EventNotifier,
    ) -> Result<(), TerminationError> {
        let runtime_name = self.0.get_runtime_name().to_owned();
        let result = self.0.terminate().await;

        // Unfortunately, due to limitations of the mpsc API, we need to return
        // a general `node::TerminationError` from the task API. Because of
        // this, you'll see in the patterns bellow the usage of
        // `node::TerminationError` rather than `TerminationError` type of this
        // module.
        match result {
            // Ignore this case as the error has been notified elsewhere.
            Err(task::TerminationError::TaskFailureNotified { .. }) => {
                let termination_err = TerminationError::StartErrorAlreadyReported.into();
                Err(termination_err)
            }
            // SAFETY: This will never happen because supervisors are never aborted
            Err(task::TerminationError::TaskAborted { .. }) => {
                unreachable!("invalid implementation; supervisor logic is never aborted")
            }
            // SAFETY: This will never happen because supervisors don't have a termination timeout
            Err(task::TerminationError::TaskForcedKilled { .. }) => {
                unreachable!("invalid implementation; supervisor logic has infinite timeout")
            }
            // SAFETY: This will never happen because supervisor logic must be panic free
            Err(task::TerminationError::TaskPanic { .. }) => {
                unreachable!("invalid implementation; supervisor logic should never panic")
            }
            // Supervisor gave up on restart logic
            Err(task::TerminationError::TaskFailed { err, .. }) => {
                match err {
                    node::TerminationError::Subtree(TerminationError::TerminationFailed(
                        termination_err,
                    )) => {
                        // Report the supervisor termination failure
                        ev_notifier
                            .supervisor_termination_failed(&runtime_name, termination_err.clone())
                            .await;
                        Err(TerminationError::TerminationFailed(termination_err))
                    }
                    other_err => {
                        // SAFETY: The scenario bellow should never happen, as the task body never
                        // returns values with a variant other than subtree::TerminationFailed.
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
                Ok(())
            }
        }
    }
}
