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

impl TerminationFailed {
    pub fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }
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
    pub fn get_runtime_name(&self) -> &str {
        match &self {
            Self::TerminationFailed(termination_err) => termination_err.get_runtime_name(),
            Self::StartErrorAlreadyReported => {
                unreachable!("invalid implementation; start error is never delivered via sup_chan")
            }
        }
    }

    pub fn get_cause_err(self) -> anyhow::Error {
        match self {
            Self::TerminationFailed(termination_err) => anyhow::Error::new(termination_err),
            Self::StartErrorAlreadyReported => {
                unreachable!("invalid implementation; start error is never delivered via sup_chan")
            }
        }
    }

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
    root_spec: root::Spec,
    opts: Vec<leaf::Opt>,
    restart_strategy: node::Restart,
}

/// A subtree that has a spawned task executing at runtime.
pub(crate) struct RunningSubtree {
    runtime_name: String,
    root_spec: root::Spec,
    task: task::RunningTask<(), StartError, node::TerminationError>,
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

impl Spec {
    pub(crate) fn new(spec: root::Spec, opts: Vec<leaf::Opt>) -> Self {
        Self {
            root_spec: spec,
            opts,
            restart_strategy: node::Restart::OneForOne,
        }
    }

    // Executes the `capataz::SupervisorSpec` run logic in a new spawned task.
    pub(crate) fn start(
        self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        parent_name: String,
        parent_chan: Option<TerminationNotifier>,
    ) -> BoxFuture<'static, Result<RunningSubtree, (StartError, Self)>> {
        // The function must return a boxed future rather than using async fn
        // because this is a co-recursive function; `subtree::Spec::start`
        // relies on `Node::start` which relies on `subtree::Spec::start`.
        //
        // For more details see:
        // https://rust-lang.github.io/async-book/07_workarounds/04_recursion.html
        let Self {
            root_spec,
            opts,
            restart_strategy,
            ..
        } = self;

        // Clone all the required metadata to spawn a new node without
        // invalidating variables.
        let subtree_name = root_spec.get_name().to_owned();
        let subtree_parent_name = parent_name.clone();
        let subtree_ev_notifier = ev_notifier.clone();
        let subtree_spec = root_spec.clone();

        async move {
            // Build task that contains the supervision tree logic.
            let task_spec = task::TaskSpec::new_with_start(
                move |ctx: Context, start_notifier: StartNotifier| {
                    // Move ownership to the FnMut first, otherwise the compiler
                    // complains with E0507.
                    let subtree_spec = subtree_spec.clone();
                    let subtree_ev_notifier = subtree_ev_notifier.clone();
                    let subtree_parent_name = subtree_parent_name.clone();
                    async move {
                        // Finally, run the supervision tree logic.
                        subtree_spec
                            .run(
                                ctx,
                                subtree_ev_notifier,
                                start_notifier,
                                &subtree_parent_name,
                            )
                            .await
                    }
                },
            );

            // Start the supervision tree.
            let runtime_name = node::build_runtime_name(&parent_name, &subtree_name);
            let result = task_spec.start(&ctx, parent_chan).await;

            match result {
                // SAFETY: The only way this error occurs is if we implemented
                // the supervision logic wrong.
                Err((task::StartError::StartRecvError(_), _)) => {
                    unreachable!("invalid implementation; supervisors always listen to channel")
                }
                // SAFETY: The only way this error occurs is if we implemented
                // the supervision logic wrong.
                Err((task::StartError::StartTimeoutError(_), _)) => {
                    unreachable!("invalid implementation; supervisors don't have a start timeout")
                }
                // A start error notification was signalled via the provided
                // `start_notifier`, return the error to our creator.
                Err((
                    task::StartError::BusinessLogicFailed(StartError::BuildFailed(build_err)),
                    _task_spec,
                )) => {
                    ev_notifier
                        .supervisor_build_failed(&runtime_name, build_err.clone())
                        .await;
                    let spec = Spec {
                        root_spec,
                        opts,
                        restart_strategy,
                    };
                    Err((StartError::BuildFailed(build_err), spec))
                }
                // A start error notification was signalled via the provided
                // `start_notifier`, return the error to our creator.
                Err((
                    task::StartError::BusinessLogicFailed(StartError::StartFailed(start_err)),
                    _task_spec,
                )) => {
                    ev_notifier
                        .supervisor_start_failed(&runtime_name, start_err.clone())
                        .await;
                    let spec = Spec {
                        root_spec,
                        opts,
                        restart_strategy,
                    };
                    Err((StartError::StartFailed(start_err), spec))
                }
                // The supervisor (including it's child nodes) started without
                // any errors
                Ok(running_supervisor) => {
                    ev_notifier.supervisor_started(&runtime_name).await;
                    Ok(RunningSubtree {
                        root_spec,
                        runtime_name,
                        task: running_supervisor,
                        opts,
                        restart_strategy,
                    })
                }
            }
        }
        .boxed()
    }

    pub(crate) fn get_restart_strategy(&self) -> &node::Restart {
        &self.restart_strategy
    }
}

impl RunningSubtree {
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
    ) -> (Result<(), TerminationError>, Spec) {
        let runtime_name = self.get_runtime_name().to_owned();
        // Discard the previous task_spec as the capataz recreates the
        // supervision tree node using the provided build nodes function.
        let (result, _task_spec) = self.task.terminate().await;
        let spec = Spec {
            root_spec: self.root_spec,
            opts: self.opts,
            restart_strategy: self.restart_strategy,
        };

        // Unfortunately, due to limitations of the mpsc API, we need to return
        // a general `node::TerminationError` from the task API. Because of
        // this, you'll see in the patterns bellow the usage of
        // `node::TerminationError` rather than `TerminationError` type of this
        // module.
        match result {
            // Ignore this case as the error has been notified elsewhere.
            Err(task::TerminationError::TaskFailureNotified { .. }) => {
                let termination_err = TerminationError::StartErrorAlreadyReported.into();
                (Err(termination_err), spec)
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
                        let termination_err = TerminationError::TerminationFailed(termination_err);
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
