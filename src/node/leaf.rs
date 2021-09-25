use std::sync::Arc;

use futures::future::{Future, FutureExt};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::events::EventNotifier;
use crate::node;
use crate::notifier;
use crate::task;

/// Responsible of providing a start notification to a parent supervisor. Users
/// of the `capataz::Worker::new_with_start` receive a value of this type and must
/// use it to indicate that the start either succeeded or failed.
///
/// See `capataz::Worker::new_with_start` for more details.
///
/// Since: 0.0.0
pub type StartNotifier = notifier::StartNotifier<anyhow::Error>;

type TerminationNotifier =
    notifier::TerminationNotifier<task::TerminationError<node::TerminationError>>;

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

/// Represents an error reported by the capataz API that indicates a
/// `capataz::Node` takes longer than allowed to get started.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("worker timed out on start")]
pub struct StartTimedOut {
    runtime_name: String,
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
    pub fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }
}

/// Unifies all possible errors that are reported when terminating a
/// `capataz::Node`.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
pub enum TerminationError {
    #[error("{0}")]
    TerminationFailed(Arc<TerminationFailed>),
    #[error("{0}")]
    TerminationTimedOut(Arc<TerminationTimedOut>),
    #[error("{0}")]
    TerminationPanicked(Arc<TerminationPanicked>),
}

impl TerminationError {
    pub fn get_runtime_name(&self) -> &str {
        match &self {
            TerminationError::TerminationFailed(termination_err) => {
                termination_err.get_runtime_name()
            }
            TerminationError::TerminationPanicked(termination_err) => {
                termination_err.get_runtime_name()
            }
            TerminationError::TerminationTimedOut(termination_err) => {
                termination_err.get_runtime_name()
            }
        }
    }

    pub fn get_cause_err(&self) -> anyhow::Error {
        match &self {
            TerminationError::TerminationFailed(termination_err) => {
                anyhow::Error::new(termination_err.clone())
            }
            TerminationError::TerminationPanicked(termination_err) => {
                anyhow::Error::new(termination_err.clone())
            }
            TerminationError::TerminationTimedOut(termination_err) => {
                anyhow::Error::new(termination_err.clone())
            }
        }
    }
}

/// Represents the specification of a worker `capataz::Node` in the supervision
/// tree.
///
/// Since: 0.0.0
pub struct Spec {
    name: String,
    opts: Vec<Opt>,
    task_spec: task::TaskSpec<(), anyhow::Error, node::TerminationError>,
    restart_strategy: node::Restart,
}

/// Represents a configuration option for a `capataz::Node` of type worker.
///
/// Since: 0.0.0
pub struct Opt(
    Box<
        dyn FnMut(&mut task::TaskSpec<(), anyhow::Error, node::TerminationError>)
            + Send
            + Sync
            + 'static,
    >,
);

impl Spec {
    /// Transforms a general business logic error into a TerminationError that
    /// later can be handled by a subtree node (Supervisor).
    fn to_leaf_error(runtime_name: &str, termination_err: anyhow::Error) -> node::TerminationError {
        node::TerminationError::Leaf(TerminationError::TerminationFailed(Arc::new(
            TerminationFailed {
                runtime_name: runtime_name.to_owned(),
                termination_err,
            },
        )))
    }

    /// Creates the specification of a worker `capataz::Node` (leaf node) in the
    /// supervision tree. This specification is used by the Capataz API to spawn
    /// an asynchronous future task that executes the given anonymous function.
    /// When creating an specification, the API caller can provide options that
    /// change how the restart logic behaves.
    ///
    /// The given anonymous function receives a `Context` parameter that API
    /// consumers should use in their business logic (usually in a
    /// `tokio::select!` call) to verify that the node has been signaled for
    /// termination (due to a restart of the parent supervisor or an application
    /// termination signal).
    ///
    /// Failure to use the `Context` parameter and respect termination signals
    /// from the parent Supervisor may result in hard kills on your business
    /// logic.
    ///
    /// Since: 0.0.0
    pub fn new<S, F, O>(name: S, mut opts: Vec<Opt>, f: F) -> node::Node
    where
        S: Into<String>,
        F: (FnMut(Context) -> O) + Send + Sync + 'static,
        O: Future<Output = Result<(), anyhow::Error>> + FutureExt + Send + Sized + 'static,
    {
        let f_lock = Arc::new(Mutex::new(f));
        let name = name.into();
        let task_name = name.clone();
        let mut task_spec =
            task::TaskSpec::new_with_start(move |ctx: Context, start_notifier: StartNotifier| {
                let parent_name = ctx.get_parent_name();
                let runtime_name = node::build_runtime_name(parent_name, &task_name);
                let f_lock = f_lock.clone();
                async move {
                    let mut f = f_lock.lock().await;
                    start_notifier.success();
                    f(ctx)
                        .await
                        .map_err(|err| Spec::to_leaf_error(&runtime_name, err))
                }
            });

        for opt_fn in &mut opts {
            opt_fn.0(&mut task_spec);
        }

        node::Node(node::NodeSpec::Leaf(Spec {
            name,
            task_spec,
            opts,
            restart_strategy: node::Restart::OneForOne,
        }))
    }

    /// Creates the specification of a worker `capataz::Node` (leaf node) in the
    /// supervision tree. This method enhances the existing
    /// `capataz::Worker::new` method by receiving an an extra parameter of type
    /// `capataz::StartNotifier` in the provided annonymous function.
    ///
    /// The `capataz::StartNotifier` parameter must be used by API consumers to
    /// signal to the parent supervisor that the node is ready. A client may
    /// want to use this variant when their business logic requires the
    /// allocation of resources that do not initialize instantaneously. Failure
    /// to use the `capataz::StartNotifier` to signal a strat may result in
    /// runtime errors indicating the supervision tree failed to start.
    ///
    /// Since: 0.0.0
    pub fn new_with_start<S, F, O>(name: S, mut opts: Vec<Opt>, f: F) -> node::Node
    where
        S: Into<String>,
        F: (FnMut(Context, StartNotifier) -> O) + Send + 'static,
        O: Future<Output = Result<(), anyhow::Error>> + FutureExt + Send + Sized + 'static,
    {
        let f_lock = Arc::new(Mutex::new(f));
        let name = name.into();
        let task_name = name.clone();
        let mut task_spec =
            task::TaskSpec::new_with_start(move |ctx: Context, start_notifier: StartNotifier| {
                let f_lock = f_lock.clone();
                let parent_name = ctx.get_parent_name();
                let runtime_name = node::build_runtime_name(parent_name, &task_name);
                async move {
                    let mut f = f_lock.lock().await;
                    f(ctx, start_notifier)
                        .await
                        .map_err(|err| Spec::to_leaf_error(&runtime_name, err))
                }
            });

        for opt_fn in &mut opts {
            opt_fn.0(&mut task_spec);
        }
        node::Node(node::NodeSpec::Leaf(Spec {
            name,
            restart_strategy: node::Restart::OneForOne,
            task_spec,
            opts,
        }))
    }

    /// Executes the bootstrap logic for a leaf node. This method will spawn a
    /// new future task (green thread), and it will block until the business
    /// logic has signalled that the node is ready or a timeout error occurs.
    pub(crate) async fn start(
        self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        parent_name: &str,
        termination_notifier: TerminationNotifier,
    ) -> Result<RunningLeaf, (StartError, Self)> {
        let Self {
            name,
            opts,
            task_spec,
            restart_strategy,
        } = self;

        let runtime_name = node::build_runtime_name(parent_name, &name);

        // Spawn the supervised task.
        let result = task_spec.start(&ctx, Some(termination_notifier)).await;

        match result {
            // SAFETY: The only way this error occurs is if we implemented the
            // supervision logic wrong.
            Err((task::StartError::StartRecvError(_), _)) => {
                unreachable!("invalid implementation; supervisors always listen to channel")
            }
            // The supervised task took too long to signal a start happened, so
            // we return a StartError to our parent node, this will ultimately
            // result in the `SupervisorSpec` start method to fail.
            Err((task::StartError::StartTimeoutError(_), task_spec)) => {
                let start_err = Arc::new(StartTimedOut {
                    runtime_name: runtime_name.clone(),
                });
                // Signal the event system that the worker failed to get started
                // due to a timeout.
                ev_notifier
                    .worker_start_timed_out(&runtime_name, start_err.clone())
                    .await;

                // Return the start error to the API caller.
                let start_err = StartError::StartTimedOut(start_err);
                let spec = Spec {
                    name,
                    opts,
                    task_spec,
                    restart_strategy,
                };
                Err((start_err, spec))
            }
            // API consumer signaled via the `StartNotifier` that the Worker
            // could not initialize correctly, this usually means the API
            // consumer was not able to allocate a required resource.
            Err((task::StartError::BusinessLogicFailed(start_err), task_spec)) => {
                let start_err = Arc::new(StartFailed {
                    runtime_name: runtime_name.clone(),
                    start_err,
                });
                // Signal the event system that the worker failed to get started
                // due to a registered error via the StartNotifier.
                ev_notifier
                    .worker_start_failed(&runtime_name, start_err.clone())
                    .await;
                // Return the start error to the API caller.
                let spec = Spec {
                    name,
                    opts,
                    task_spec,
                    restart_strategy,
                };
                Err((StartError::StartFailed(start_err), spec))
            }
            Ok(running_worker) => {
                // Signal the event system that the worker started without
                // errors.
                ev_notifier.worker_started(&runtime_name).await;
                // Return the running worker to the API caller.
                Ok(RunningLeaf {
                    name,
                    runtime_name,
                    task: running_worker,
                    opts,
                    restart_strategy,
                })
            }
        }
    }

    pub(crate) fn get_restart_strategy(&self) -> &node::Restart {
        &self.restart_strategy
    }

    /// Specifies how long a client API is willing to wait for the start of this
    /// `capataz::Node`.
    ///
    /// If this configuration option is not specified, there is no timeout for
    /// the node start.
    ///
    /// Since: 0.0.0
    pub fn with_start_timeout(duration: std::time::Duration) -> Opt {
        Opt(Box::new(move |task| {
            task.with_startup(task::Startup::Timeout(duration.clone()));
        }))
    }

    /// Specifies how long a client API is willing to wait for the termination
    /// of this `capataz::Node`.
    ///
    /// If this configuration option is not specified, there is no timeout
    /// for the node termination.
    ///
    /// Since: 0.0.0
    pub fn with_termination_timeout(duration: std::time::Duration) -> Opt {
        Opt(Box::new(move |task| {
            task.with_shutdown(task::Shutdown::Timeout(duration.clone()));
        }))
    }
}

/// Represents a leaf node that has an existing spawned task in runtime.
pub(crate) struct RunningLeaf {
    name: String,
    runtime_name: String,
    task: task::RunningTask<(), anyhow::Error, node::TerminationError>,
    opts: Vec<Opt>,
    restart_strategy: node::Restart,
}

/// Internal value to allow RunningLeaf to implement the Debug trait
#[derive(Debug)]
struct RunningLeafDebug {
    name: String,
    runtime_name: String,
    restart_strategy: node::Restart,
}

impl std::fmt::Debug for RunningLeaf {
    fn fmt(&self, format: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let data = RunningLeafDebug {
            name: self.name.clone(),
            runtime_name: self.runtime_name.clone(),
            restart_strategy: self.restart_strategy.clone(),
        };
        data.fmt(format)
    }
}

impl RunningLeaf {
    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    pub(crate) fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }

    /// Executes the termination logic of a `capataz::Node`. This method will
    /// block until the spawned task is guaranteed to be terminated.
    pub(crate) async fn terminate(
        self,
        mut ev_notifier: EventNotifier,
    ) -> (Result<(), TerminationError>, Spec) {
        let runtime_name = self.runtime_name.clone();

        let (result, task_spec) = self.task.terminate().await;
        let spec = Spec {
            task_spec,
            name: self.name,
            opts: self.opts,
            restart_strategy: self.restart_strategy,
        };

        // Unfortunately, due to limitations of the mpsc API, we need to return
        // a general `node::TerminationError` from the task API. Because of
        // this, you'll see in the patterns bellow the usage of
        // `node::TerminationError` rather than `TerminationError` type of this
        // module.
        match result {
            // The termination failed with an expected business error.
            Err(task::TerminationError::TaskFailed { err, .. }) => {
                match err {
                    node::TerminationError::Leaf(TerminationError::TerminationFailed(
                        termination_err,
                    )) => {
                        ev_notifier
                            .worker_termination_failed(&runtime_name, termination_err.clone())
                            .await;
                        let termination_err = TerminationError::TerminationFailed(termination_err);
                        (Err(termination_err), spec)
                    }
                    // The scenario bellow should never happen, as the body of a
                    // leaf node never returns `subtree::TerminationError`
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
            Err(task::TerminationError::TaskForcedKilled { .. }) => {
                let termination_err = Arc::new(TerminationTimedOut {
                    runtime_name: runtime_name.to_owned(),
                });
                ev_notifier
                    .worker_termination_timed_out(&runtime_name, termination_err.clone())
                    .await;
                let termination_err = TerminationError::TerminationTimedOut(termination_err);
                (Err(termination_err), spec)
            }
            // When the task panics, the task API returns this error. Transform
            // this signal to a TerminationPanicked error.
            Err(task::TerminationError::TaskPanic { .. }) => {
                // TODO: add panic metadata
                let termination_err = Arc::new(TerminationPanicked {
                    runtime_name: runtime_name.to_owned(),
                });
                ev_notifier
                    .worker_termination_panicked(&runtime_name, termination_err.clone())
                    .await;
                let termination_err = TerminationError::TerminationPanicked(termination_err);
                (Err(termination_err), spec)
            }
            // In the situation we get other kind of error, panic
            Err(task::TerminationError::TaskFailureNotified) => {
                // When a parent supervisor restarts a node, it invokes it's
                // termination first. Because we already sent the error to the
                // supervisor, it is valid to return an Ok result here.
                //
                // Also, we do not need to do a notification of failure, because that was
                // already done when the error was sent.
                (Ok(()), spec)
            }
            Err(task::TerminationError::TaskAborted) => {
                unreachable!("validate this is being visited")
            }
            // Happy path.
            Ok(_) => {
                ev_notifier.worker_terminated(&runtime_name).await;
                (Ok(()), spec)
            }
        }
    }

    pub(crate) fn get_restart_strategy(&self) -> &node::Restart {
        &self.restart_strategy
    }
}
