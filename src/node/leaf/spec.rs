use futures::future::{Future, FutureExt};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::events::EventNotifier;
use crate::node;
use crate::notifier;
use crate::task;

use super::errors::*;
use super::opts::*;
use super::running_leaf::*;

/// Responsible of providing a start notification to a parent supervisor. Users
/// of the `capataz::Worker::new_with_start` receive a value of this type and must
/// use it to indicate that the start either succeeded or failed.
///
/// See `capataz::Worker::new_with_start` for more details.
///
/// Since: 0.0.0
pub type StartNotifier = notifier::StartNotifier<anyhow::Error>;

type TerminationNotifier =
    notifier::TerminationNotifier<String, task::TerminationMessage<node::TerminationMessage>>;

/// Represents the specification of a worker `capataz::Node` in the supervision
/// tree.
///
/// Since: 0.0.0
pub struct Spec {
    name: String,
    opts: Vec<Opt>,
    task_spec: task::TaskSpec<String, anyhow::Error, node::TerminationMessage>,
    restart_strategy: node::Restart,
}

impl Spec {
    pub(crate) fn from_running_leaf(
        name: String,
        opts: Vec<Opt>,
        task_spec: task::TaskSpec<String, anyhow::Error, node::TerminationMessage>,
        restart_strategy: node::Restart,
    ) -> Self {
        Self {
            name,
            opts,
            task_spec,
            restart_strategy,
        }
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
                        .map(|_| {
                            // Transform a unit return into a String with the
                            // runtime_name of the node, this way we can track the
                            // leaf termination on the supervisor.
                            runtime_name.clone()
                        })
                        .map_err(|err| {
                            // Transform a general business logic error into a
                            // TerminationMessage that later can be handled by a
                            // subtree node (Supervisor).
                            node::TerminationMessage::Leaf(TerminationMessage::from_task_error(
                                &runtime_name,
                                err,
                            ))
                        })
                }
            });

        for opt_fn in &mut opts {
            opt_fn.call(&mut task_spec);
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
                        .map(|_| {
                            // Transform a unit return into a String with the
                            // runtime_name of the node, this way we can track the
                            // leaf termination on the supervisor.
                            runtime_name.clone()
                        })
                        .map_err(|err| {
                            // Transform a general business logic error into a
                            // TerminationMessage that later can be handled by a
                            // subtree node (Supervisor).
                            node::TerminationMessage::Leaf(TerminationMessage::from_task_error(
                                &runtime_name,
                                err,
                            ))
                        })
                }
            });

        for opt_fn in &mut opts {
            opt_fn.call(&mut task_spec);
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
                let start_err = Arc::new(StartTimedOut::new(&runtime_name));
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
                let start_err = Arc::new(StartFailed::new(&runtime_name, start_err));
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
                Ok(RunningLeaf::new(
                    name,
                    runtime_name,
                    running_worker,
                    opts,
                    restart_strategy,
                ))
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
        Opt::new(move |task| {
            task.with_startup(task::Startup::Timeout(duration.clone()));
        })
    }

    /// Specifies how long a client API is willing to wait for the termination
    /// of this `capataz::Node`.
    ///
    /// If this configuration option is not specified, there is no timeout
    /// for the node termination.
    ///
    /// Since: 0.0.0
    pub fn with_termination_timeout(duration: std::time::Duration) -> Opt {
        Opt::new(move |task| {
            task.with_shutdown(task::Shutdown::Timeout(duration.clone()));
        })
    }
}
