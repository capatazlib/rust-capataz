use std::fmt;
use std::sync::Arc;

use lazy_static::lazy_static;
use tokio::sync::{mpsc, Mutex};
use tokio::time;

use crate::context::Context;
use crate::events::{EventListener, EventNotifier};
use crate::node::{self, leaf, subtree, Node, Strategy};
use crate::notifier;
use crate::supervisor::RestartManager;
use crate::task;

use super::cleanup::*;
use super::node_builder::*;
use super::opts::*;
use super::running_supervisor::*;

lazy_static! {
    /// Default buffer size for a supervisor error channel.
    static ref SUPERVISOR_DEF_CHAN_BUFFER_SIZE: usize = 100;
    /// Default root name
    static ref SUPERVISOR_ROOT_NAME: &'static str = "";
}

/// Specific instance that reports termination errors in the root tree.
pub(crate) type TerminationNotifier =
    notifier::TerminationNotifier<String, task::TerminationMessage<node::TerminationMessage>>;

/// Specific instance for the child node error listener.
type TerminationListener =
    mpsc::Receiver<Result<String, task::TerminationMessage<node::TerminationMessage>>>;

/// Specific instance that reports successful starts or start error in the root
/// tree
type StartNotifier = notifier::StartNotifier<subtree::StartError>;

/// Represents the specification of a static supervision tree; it serves as a
/// template for the construction of a runtime supervision tree.
///
/// With a `supervisor::Spec` value you may configure settings like:
///
/// * The child nodes (workers or sub-supervisors) you want to spawn when
///   your application starts.
///
/// * The order in which the supervised child nodes get started.
///
/// * How to restart a faling child node (and, if specified all its siblings as well)
///   when a they fail.
///
/// Since: 0.0.0
#[derive(Clone)]
pub struct Spec {
    name: String,
    build_nodes_fn: Arc<Mutex<BuildNodesFn>>,
    pub(crate) start_order: StartOrder,
    pub(crate) strategy: Strategy,
    pub(crate) max_allowed_restarts: u32,
    pub(crate) restart_window: time::Duration,
}

impl fmt::Debug for Spec {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        fmt.debug_struct("Spec")
            .field("name", &self.name)
            .field("start_order", &self.start_order)
            .field("max_allowed_restarts", &self.max_allowed_restarts)
            .field("restart_window", &self.restart_window)
            .finish()
    }
}

impl Spec {
    /// Creates a `supervisor::Spec` value using an anonymous function that
    /// build the nodes that the `capataz::Supervisor` will run and monitor.
    ///
    /// If you need to allocate a resource that is going to be shared across
    /// multiple child nodes (e.g. a database connection), you may want to use
    /// `supervisor::Spec::new_with_cleanup` instead.
    ///
    /// Since: 0.0.0
    pub fn new<S, F>(name: S, mut opts: Vec<Opt>, mut build_nodes_fn: F) -> Self
    where
        S: Into<String>,
        F: (FnMut() -> Vec<Node>) + Send + Sync + 'static,
    {
        let name = name.into();
        let build_nodes_fn = move || {
            let nodes = build_nodes_fn();
            Ok(Nodes::new(nodes))
        };
        let build_nodes_fn = Arc::new(Mutex::new(BuildNodesFn::new(build_nodes_fn)));

        let mut spec = Self {
            name,
            build_nodes_fn,
            start_order: StartOrder::LeftToRight,
            strategy: Strategy::OneForOne,
            max_allowed_restarts: 1,
            restart_window: time::Duration::from_secs(5),
        };

        for opt_fn in &mut opts {
            opt_fn.call(&mut spec);
        }

        spec
    }

    /// Similar to `supervisor::Spec::new`, this function creates a
    /// `supervisor::Spec` using an anonymous function that builds the
    /// nodes that is going to run and monitor, as well as a resource allocation
    /// with a proper cleanup strategy.
    ///
    /// The given anonymous function must return a tuple that contains a
    /// `Vec<Node>` and another annonymous function that executes the resource
    /// de-allocation. The tuple must be wrapped in a `Result::Ok` value.
    ///
    /// For error handling when allocating resources, this function must return
    /// any type that is an Error (delagating error handling to the `anyhow`
    /// API).
    ///
    /// #### Example
    ///
    /// The following (hypothetical) example creates a supervision tree with two
    /// child nodes that use a shared Redis client.
    ///
    /// ```ignore
    /// use capataz::prelude::*;
    ///
    /// supervisor::Spec::new_with_cleanup("my-supervisor", vec![], || {
    ///   // Create the redis client. Note the `?` to deal with errors.
    ///   let client = std::sync::Arc::new(redis::Client::open("redis://127.0.0.1")?);
    ///
    ///   // Create application nodes
    ///   let nodes = vec![
    ///     // Node that runs some server (HTTP, gRPC, etc).
    ///     crate::app::NewServer(client.clone()),
    ///
    ///     // Node that performs queries to the database
    ///     // to publish some metrics
    ///     crate::app::DBAnalyzer(client.clone()),
    ///   ];
    ///
    ///   // Create a cleanup routine (silly example)
    ///   let cleanup = move || { std::mem::drop(client) }
    ///
    ///   // Return sub-nodes and cleanup for this tree
    ///   Ok((nodes, cleanup))
    /// })
    /// ```
    ///
    /// Since: 0.0.0
    pub fn new_with_cleanup<S, F, R>(name: S, mut opts: Vec<Opt>, mut build_nodes_fn: F) -> Self
    where
        S: Into<String>,
        R: Into<Nodes>,
        F: FnMut() -> Result<R, anyhow::Error> + Send + Sync + 'static,
    {
        let build_nodes_fn = move || -> Result<Nodes, anyhow::Error> {
            let result = build_nodes_fn()?;
            Ok(result.into())
        };

        let build_nodes_fn = Arc::new(Mutex::new(BuildNodesFn::new(build_nodes_fn)));

        let mut spec = Self {
            name: name.into(),
            build_nodes_fn,

            start_order: StartOrder::LeftToRight,
            strategy: Strategy::OneForOne,
            max_allowed_restarts: 1,
            restart_window: time::Duration::from_secs(5),
        };

        for opt_fn in &mut opts {
            opt_fn.call(&mut spec);
        }

        spec
    }

    /// Transforms this `supervisor::Spec` into a subtree `capataz::Node`
    /// with some node configuration.
    ///
    /// Since: 0.0.0
    pub fn subtree(self, opts: Vec<leaf::Opt>) -> node::Node {
        subtree::Spec::new(self, opts).to_node()
    }

    /// Returns the name associated to this `supervisor::Spec`.
    ///
    /// Since: 0.0.0
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Executes the child nodes supervision logic.
    async fn run_supervision_loop(
        &mut self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        mut restart_manager: RestartManager,
        // sup_chan is the channel used by *this* supervision tree
        // to listen to it's child nodes
        mut sup_chan: TerminationListener,
        // parent_chan is the channel used by the *parent* supervision tree
        // to listen to errors on this supervisor
        parent_chan: TerminationNotifier,
        runtime_name: &str,
        mut running_nodes: RunningNodes,
    ) -> Result<node::RuntimeName, subtree::TerminationMessage> {
        loop {
            tokio::select! {
                // A signal for termination is given by our parent or the API caller.
                _ = ctx.done() => {
                    // Close the supervision channel so that termination errors
                    // from child nodes don't get sent to this supervisor.
                    sup_chan.close();

                    // Terminate children of this supervisor and return.
                    running_nodes.terminate(ev_notifier.clone(), &runtime_name).await?;

                    // Termination of all the child nodes did not fail. Break
                    // the loop and finish.
                    return Ok(runtime_name.to_owned());
                },
                // An error was reported by a child node
                node_err  = sup_chan.recv() => {
                    match node_err {
                        None => {
                            // The sup chan was closed, and there are no more
                            // notifications sent. This scenario implies
                            // termination is upon us.
                            continue
                        }
                        Some(Err(task::TerminationMessage::TaskForcedKilled { .. })) => {
                            unreachable!(
                                "invalid implementation; forced kill should not be sent to parent_chan"
                            )
                        }
                        Some(Err(task::TerminationMessage::TaskAborted { .. })) => {
                            unreachable!(
                                "invalid implementation; abort task should not be sent to parent_chan"
                            )
                        }
                        Some(Err(task::TerminationMessage::TaskFailureNotified { .. })) => {
                            unreachable!(
                                "invalid implementation; failure notified should not be sent to parent_chan"
                            )
                        }
                        Some(Ok(node_runtime_name)) => {
                            let node_name = node::to_node_name(&node_runtime_name);
                            ev_notifier.worker_terminated(&node_runtime_name).await;
                            let has_any_children = running_nodes
                                .handle_node_termination(
                                    false,
                                    ev_notifier.clone(),
                                    runtime_name,
                                    parent_chan.clone(),
                                    self.strategy.clone(),
                                    &node_name,
                                )
                                .await?;
                            if !has_any_children {
                                sup_chan.close();
                                return Ok(runtime_name.to_owned());
                            }
                        }
                        Some(Err(task::TerminationMessage::TaskPanic { .. })) => {
                            todo!("panic handling logic pending")
                        }
                        Some(Err(task::TerminationMessage::TaskFailed { err, .. })) => {
                            // Send notification of termination error; this call
                            // will dynamically dispatch to either leaf or
                            // subtree implementations.
                            err.notify_runtime_error(ev_notifier.clone()).await;
                            let node_name = node::to_node_name(err.get_runtime_name());
                            match restart_manager.register_error(anyhow::Error::new(err)) {
                                Ok(new_restart_manager) => {
                                    let was_error = true;
                                    restart_manager = new_restart_manager;
                                    running_nodes
                                        .handle_node_termination(
                                            was_error,
                                            ev_notifier.clone(),
                                            runtime_name,
                                            parent_chan.clone(),
                                            self.strategy.clone(),
                                            &node_name,
                                        )
                                        .await?;
                                    continue;
                                },
                                Err(too_many_restarts_err) => {
                                    // Close the supervision channel so that termination errors
                                    // from child nodes don't get sent to this supervisor.
                                    sup_chan.close();

                                    // Terminate all child nodes before failing this supervisor
                                    running_nodes.terminate(ev_notifier.clone(), &runtime_name).await?;
                                    let termination_err =
                                        subtree::TerminationMessage::TooManyRestarts(
                                            Arc::new(too_many_restarts_err),
                                        );
                                    return Err(termination_err);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Initializes and executes the supervision logic in the current task.
    pub(crate) async fn run(
        mut self,
        ctx: Context,
        ev_notifier: EventNotifier,
        start_notifier: StartNotifier,
        parent_name: &str,
    ) -> Result<node::RuntimeName, node::TerminationMessage> {
        // Build the name that is going to be used in all the supervision system
        // telemetry.
        let runtime_name = node::build_runtime_name(parent_name, &self.name);
        let restart_manager = RestartManager::new(
            &runtime_name,
            self.max_allowed_restarts,
            self.restart_window,
        );

        // Build the channels that are going to be used by the supervised
        // workers to notify the supervisor that something went wrong, and allow
        // the supervisor to restart or fail.
        let (parent_chan, sup_chan) = mpsc::channel(*SUPERVISOR_DEF_CHAN_BUFFER_SIZE);
        let parent_chan = notifier::TerminationNotifier::from_mpsc(parent_chan);

        // Build the subtree nodes of this supervisor.
        let mut build_nodes_fn = self.build_nodes_fn.lock().await;
        let result = build_nodes_fn.call();
        std::mem::drop(build_nodes_fn);

        match result {
            // An error was returned by the node factory function, it could be:
            //
            // * The provided build nodes function failed with an error
            //
            // * A child worker failed with an error
            //
            Err(build_err) => {
                let build_err = subtree::StartError::build_failed(&runtime_name, build_err);
                // Notify the supervisor parent that the start failed, is important
                // to have this dedicated notifier, as the behavior of start errors is
                // different from regular supervision.
                start_notifier.failed(build_err);
                // We have dealt with the error on the start_notifier call, we
                // we cannot provide it because we moved it already.
                Err(subtree::TerminationMessage::StartErrorAlreadyReported.into())
            }
            // The node build (anonymous) function worked as expected.
            Ok(nodes) => {
                // Start child nodes in the specified order.
                // TODO: add LeftToRight and RightToLeft support
                let node_ctx = Context::new().with_parent_name(&runtime_name);

                let result = nodes
                    .start(
                        // It is imperative we use a new context here, otherwise the termination signal will start
                        // in a indeterminate order, we want to ensure termination starts from the top-down
                        node_ctx,
                        ev_notifier.clone(),
                        &runtime_name,
                        &self.start_order,
                        &self.strategy,
                        sup_chan,
                        parent_chan.clone(),
                    )
                    .await;

                match result {
                    // One of the child nodes failed to start
                    Err(start_err) => {
                        start_notifier.failed(start_err);
                        Err(subtree::TerminationMessage::StartErrorAlreadyReported.into())
                    }
                    // All child nodes started without errors
                    Ok((running_nodes, sup_chan)) => {
                        // Notify our caller that we have started so that
                        // Supervisor record can be returned from a start call.
                        start_notifier.success();

                        // Run the supervision logic until restart tolerance is
                        // reached or termination is signalled.
                        let result = self
                            .run_supervision_loop(
                                ctx,
                                ev_notifier,
                                restart_manager,
                                sup_chan,
                                parent_chan,
                                &runtime_name,
                                running_nodes,
                            )
                            .await;

                        // Return the subtree::TerminationMessage wrapped in a
                        // node::TerminationMessage
                        result.map_err(node::TerminationMessage::Subtree)
                    }
                }
            }
        }
    }

    /// Spawns all the nodes in this `supervisor::Spec` and returns a
    /// `capataz::Supervisor`.
    ///
    /// A `capataz::Supervisor` is a tree of workers and/or supervisors
    /// (sub-trees). This method spawns the workers (leaf) tasks first and then
    /// continues spawning nodes up in the tree heriarchy. Depending on the
    /// `supervisor::Spec` configuration, the start order will be in
    /// pre-order (left to right) or post-order (right to left).
    ///
    /// ### Tree initialization
    ///
    /// Once all tasks get started without errors, the `capataz::Supervisor`
    /// monitors any error that gets reported by the child nodes. Invoking this
    /// method will block the caller until all child nodes have been started.
    ///
    /// ### Failures on child node initialization
    ///
    /// In the scenario that one of the child nodes fails to start, the start
    /// algorithm is going to abort the start procedure and is going to
    /// terminate in reverse order all the child nodes that have been started,
    /// finally returning all registered errors.
    ///
    /// Since: 0.0.0
    pub async fn start(
        self,
        ctx: Context,
        ev_listener: EventListener,
    ) -> Result<Supervisor, subtree::StartError> {
        // Create a copy of this supervisor and automatically transform into a
        // subtree to re-utilize the start subtree logic.
        let root_spec = self.clone().subtree(Vec::new());
        let ev_notifier = EventNotifier::new(ev_listener);

        let result = root_spec
            .start_root(ctx, ev_notifier.clone(), *SUPERVISOR_ROOT_NAME)
            .await;

        match result {
            Err((node::StartError::Leaf(_), _spec)) => {
                unreachable!("invalid implementation; subtree code is returning a leaf error")
            }
            Ok(node::RunningNode::Leaf(_)) => {
                unreachable!("invalid implementation; subtree code is returning a leaf value")
            }
            Err((node::StartError::Subtree(start_err), _spec)) => Err(start_err),
            Ok(node::RunningNode::Subtree(running_subtree)) => {
                Ok(Supervisor::new(self, running_subtree, ev_notifier))
            }
        }
    }
}

impl<F> std::convert::From<(Vec<Node>, F)> for Nodes
where
    F: FnOnce() -> Result<(), anyhow::Error> + Send + Sync + 'static,
{
    fn from(input: (Vec<Node>, F)) -> Nodes {
        Nodes::new_with_cleanup(input.0, input.1)
    }
}

// Skip this implementation, as we want to enforce using the method
// `supervisor::Spec::new` when creating `supervisor::Spec` values that do not
// require a cleanup logic.
//
// impl std::convert::From<Vec<Node>> for Nodes {
//     fn from(input: Vec<Node>) -> Nodes {
//         Nodes::new(input)
//     }
// }

/// Represents the internal components of a supervision tree (inner nodes +
/// resource cleanup strategy). API clients usually won't create values of this
/// type directly, but rather use it's multiple `From` instances.
///
/// See `supervisor::Spec::new_with_cleanup` for more details.
///
/// Since: 0.0.0
pub struct Nodes {
    nodes: Vec<Node>,
    cleanup: Option<CleanupFn>,
}

impl Nodes {
    /// Creates a Nodes value that doesn't have a cleanup logic.
    pub fn new(nodes: Vec<Node>) -> Self {
        Self {
            nodes,
            cleanup: None,
        }
    }

    /// Creates a Nodes value with a cleanup strategy.
    pub fn new_with_cleanup<F>(nodes: Vec<Node>, cleanup: F) -> Self
    where
        F: FnOnce() -> Result<(), anyhow::Error> + Send + Sync + 'static,
    {
        Self {
            nodes,
            cleanup: Some(CleanupFn::new(cleanup)),
        }
    }

    pub(crate) async fn start(
        self,
        ctx: Context,
        ev_notifier: EventNotifier,
        runtime_name: &str,
        start_order: &StartOrder,
        restart: &Strategy,
        mut sup_chan: TerminationListener,
        parent_chan: TerminationNotifier,
    ) -> Result<(RunningNodes, TerminationListener), subtree::StartError> {
        let Self { nodes, cleanup, .. } = self;
        let cleanup = cleanup.unwrap_or(CleanupFn::empty());

        // Create a runtime node per node spec
        let mut running_nodes = Vec::new();

        // Order child nodes in the desired start order
        let nodes_it: Box<dyn Iterator<Item = Node> + Send> = match start_order {
            StartOrder::LeftToRight => Box::new(nodes.into_iter()),
            StartOrder::RightToLeft => Box::new(nodes.into_iter().rev()),
        };

        for node in nodes_it {
            // Execute the start logic for the given node
            let result = node
                .start(
                    ctx.clone(),
                    ev_notifier.clone(),
                    runtime_name,
                    parent_chan.clone(),
                )
                .await;

            match result {
                Ok(running_node) => {
                    // When the node started without errors, add it to the vec of runtime_nodes
                    running_nodes.push(running_node);
                }
                Err((start_err, _node_spec)) => {
                    //
                    sup_chan.close();

                    // A child node failed to start, we need to perform a cleanup operation and stop
                    // all the previous children that got started in reversed order
                    let start_err = anyhow::Error::new(start_err);
                    let running_nodes = RunningNodes::new(
                        start_order.clone(),
                        restart.clone(),
                        running_nodes,
                        cleanup,
                    );

                    // When terminating children, some of them may have a
                    // termination failure, register them and bubble them up.
                    let result = running_nodes
                        .terminate(ev_notifier.clone(), runtime_name)
                        .await;

                    let termination_err: Option<subtree::TerminationMessage> = result.err();

                    let start_err =
                        subtree::StartError::start_failed(runtime_name, start_err, termination_err);

                    return Err(start_err);
                }
            }
        }

        let running_nodes =
            RunningNodes::new(start_order.clone(), restart.clone(), running_nodes, cleanup);

        // Happy path, all nodes were started without errors.
        Ok((running_nodes, sup_chan))
    }
}
