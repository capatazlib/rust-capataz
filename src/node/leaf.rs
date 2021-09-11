use futures::future::{Future, FutureExt};
use std::sync::Arc;
use thiserror::Error;

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

/// Represents an error reported by the capataz API that indicates a
/// `capataz::Node` takes too long than allowed to terminate.
///
/// Since: 0.0.0
#[derive(Debug, Error)]
#[error("worker took too long to terminate")]
pub struct TerminationTimedOut {
    runtime_name: String,
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

/// Represents the specification of a worker `capataz::Node` in the supervision
/// tree.
///
/// Since: 0.0.0
pub struct Spec {
    task_spec: task::TaskSpec<(), anyhow::Error, node::TerminationError>,
    opts: Vec<Opt>,
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
    fn to_leaf_error(termination_err: anyhow::Error) -> node::TerminationError {
        node::TerminationError::Leaf(TerminationError::TerminationFailed(Arc::new(
            TerminationFailed {
                // TODO: solve this predicament
                runtime_name: "".to_owned(),
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
        F: (FnOnce(Context) -> O) + Send + 'static,
        O: Future<Output = Result<(), anyhow::Error>> + FutureExt + Send + Sized + 'static,
    {
        let mut task_spec = task::TaskSpec::new_with_start(
            name,
            |ctx: Context, start_notifier: StartNotifier| async {
                start_notifier.success();
                f(ctx).await.map_err(Spec::to_leaf_error)
            },
        );

        for opt_fn in &mut opts {
            opt_fn.0(&mut task_spec);
        }

        node::Node(node::NodeSpec::Leaf(Spec { task_spec, opts }))
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
        F: (FnOnce(Context, StartNotifier) -> O) + Send + 'static,
        O: Future<Output = Result<(), anyhow::Error>> + FutureExt + Send + Sized + 'static,
    {
        let mut task_spec = task::TaskSpec::new_with_start(
            name,
            |ctx: Context, start_notifier: StartNotifier| async {
                f(ctx, start_notifier).await.map_err(Spec::to_leaf_error)
            },
        );

        for opt_fn in &mut opts {
            opt_fn.0(&mut task_spec);
        }
        node::Node(node::NodeSpec::Leaf(Spec { task_spec, opts }))
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
    ) -> Result<RunningLeaf, StartError> {
        let runtime_name = self.task_spec.build_runtime_name(parent_name);

        // Spawn the supervised task.
        let result = self
            .task_spec
            .start(&ctx, parent_name, Some(termination_notifier))
            .await;

        match result {
            // SAFETY: The only way this error occurs is if we implemented the
            // supervision logic wrong.
            Err(task::StartError::StartRecvError(_)) => {
                unreachable!("invalid implementation; supervisors always listen to channel")
            }
            // The supervised task took too long to signal a start happened, so
            // we return a StartError to our parent node, this will ultimately
            // result in the `SupervisorSpec` start method to fail.
            Err(task::StartError::StartTimeoutError(_)) => {
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
                Err(start_err)
            }
            // API consumer signaled via the `StartNotifier` that the Worker
            // could not initialize correctly, this usually means the API
            // consumer was not able to allocate a required resource.
            Err(task::StartError::BusinessLogicFailed(start_err)) => {
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
                Err(StartError::StartFailed(start_err))
            }
            Ok(running_worker) => {
                // Signal the event system that the worker started without
                // errors.
                ev_notifier.worker_started(&runtime_name).await;
                // Return the running worker to the API caller.
                Ok(RunningLeaf(running_worker))
            }
        }
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
#[derive(Debug)]
pub(crate) struct RunningLeaf(task::RunningTask<(), node::TerminationError>);

impl RunningLeaf {
    /// Executes the termination logic of a `capataz::Node`. This method will
    /// block until the spawned task is guaranteed to be terminated.
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
            // The termination failed with an expected business error.
            Err(task::TerminationError::TaskFailed { err, .. }) => {
                match err {
                    node::TerminationError::Leaf(TerminationError::TerminationFailed(
                        termination_err,
                    )) => {
                        ev_notifier
                            .worker_termination_failed(&runtime_name, termination_err.clone())
                            .await;
                        Err(TerminationError::TerminationFailed(termination_err))
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
                Err(TerminationError::TerminationTimedOut(termination_err))
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
                Err(TerminationError::TerminationPanicked(termination_err))
            }
            // Happy path.
            Ok(_) => {
                ev_notifier.worker_terminated(&runtime_name).await;
                Ok(())
            }
            // In the situation we get other kind of error, panic
            _ => {
                panic!("implementation is not handling new leaf::TerminationError variants")
            }
        }
    }
}
