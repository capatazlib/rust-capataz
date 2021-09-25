use std::collections::HashMap;
use std::sync::Arc;

use lazy_static::lazy_static;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::events::{EventListener, EventNotifier};
use crate::node::subtree;
use crate::node::{self, leaf, Node, RunningNode};
use crate::notifier;
use crate::task;

lazy_static! {
    /// Default buffer size for a supervisor error channel.
    static ref SUPERVISOR_DEF_CHAN_BUFFER_SIZE: usize = 100;
    /// Default root name
    static ref SUPERVISOR_ROOT_NAME: &'static str = "";
}

/// Specific instance that reports successful starts or start error in the root
/// tree
type StartNotifier = notifier::StartNotifier<subtree::StartError>;

/// Specific instance that reports termination errors in the root tree.
type TerminationNotifier =
    notifier::TerminationNotifier<task::TerminationError<node::TerminationError>>;

/// Specific instance for the child node error listener.
type TerminationListener = mpsc::Receiver<task::TerminationError<node::TerminationError>>;

impl<F> std::convert::From<(Vec<Node>, F)> for Nodes
where
    F: FnOnce() -> Result<(), anyhow::Error> + Send + Sync + 'static,
{
    fn from(input: (Vec<Node>, F)) -> Nodes {
        Nodes::new_with_cleanup(input.0, input.1)
    }
}

// Skip this implementation, as we want to enforce using the method
// `SupervisorSpec::new` when creating `SupervisorSpec` values that do not
// require a cleanup logic.
//
// impl std::convert::From<Vec<Node>> for Nodes {
//     fn from(input: Vec<Node>) -> Nodes {
//         Nodes::new(input)
//     }
// }

/// A callback function that cleans up resources that were allocated in the
/// construction of a supervision tree via the `capataz::Spec::new_with_cleanup`
/// method.
///
pub(crate) struct CleanupFn(Box<dyn FnOnce() -> Result<(), anyhow::Error> + Send + Sync + 'static>);

impl CleanupFn {
    /// Creates a `capataz::CleanupFn` value. This function must be used when
    /// invoking the `capataz::Spec::new_with_cleanup` method.
    ///
    pub(crate) fn new<F>(cleanup_fn: F) -> Self
    where
        F: FnOnce() -> Result<(), anyhow::Error> + Send + Sync + 'static,
    {
        CleanupFn(Box::new(cleanup_fn))
    }

    /// Creates a `capataz::CleanupFn` that does nothing and always succeeds.
    /// This function is used from public APIs that don't offer the option to
    /// provide a `capataz::CleanupFn`.
    pub(crate) fn empty() -> Self {
        CleanupFn(Box::new(|| Ok(())))
    }

    /// Invokes the cleanup business logic provided by API users on a
    /// `capataz::CleanupFn::new` call.
    pub(crate) fn call(self) -> Result<(), anyhow::Error> {
        self.0()
    }
}

/// Represents the internal components of a supervision tree (inner nodes +
/// resource cleanup strategy). API clients usually won't create values of this
/// type directly, but rather use it's multiple `From` instances.
///
/// See `SupervisorSpec::new_with_cleanup` for more details.
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
        parent_chan: TerminationNotifier,
    ) -> Result<RunningNodes, subtree::StartError> {
        let Self { mut nodes, cleanup } = self;
        let cleanup = cleanup.unwrap_or(CleanupFn::empty());

        // Create a runtime node per node spec
        let mut running_nodes = Vec::new();

        // Order child nodes in the desired start order
        let nodes = match start_order {
            StartOrder::LeftToRight => nodes,
            StartOrder::RightToLeft => {
                nodes.reverse();
                nodes
            }
        };

        for node in nodes {
            // Execute the start logic for the given node
            let result = node
                .0
                .start(
                    ctx.clone(),
                    ev_notifier.clone(),
                    runtime_name,
                    Some(parent_chan.clone()),
                )
                .await;

            match result {
                Ok(runtime_node) => {
                    // When the node started without errors, add it to the vec of runtime_nodes
                    running_nodes.push(runtime_node);
                }
                Err((start_err, _node_spec)) => {
                    // A child node failed to start, we need to perform a cleanup operation and stop
                    // all the previous children that got started in reversed order
                    let start_err = anyhow::Error::new(start_err);
                    let running_nodes = RunningNodes::new(running_nodes, cleanup);

                    // When terminating children, some of them may have a
                    // termination failure, register them and bubble them up.
                    let result = running_nodes
                        .terminate(ev_notifier.clone(), runtime_name)
                        .await;

                    let termination_err: Option<subtree::TerminationError> = result.err();

                    let start_err =
                        subtree::StartError::start_failed(runtime_name, start_err, termination_err);

                    return Err(start_err);
                }
            }
        }

        let runtime_nodes = RunningNodes::new(running_nodes, cleanup);

        // Happy path, all nodes were started without errors.
        Ok(runtime_nodes)
    }
}

/// Internal representation of the closure function used by the
/// `capataz::SupervisorSpec::new_with_cleanup` method.
///
pub(crate) struct BuildNodesFn(
    Box<dyn FnMut() -> Result<Nodes, anyhow::Error> + Send + Sync + 'static>,
);

impl BuildNodesFn {
    /// Executes the build nodes function.
    pub(crate) fn call(&mut self) -> Result<Nodes, anyhow::Error> {
        self.0()
    }
    /// Creates a builder function that returns new nodes. The Nodes value may
    /// also contain a cleanup function to deallocate resources.
    pub(crate) fn new<F>(build_nodes_fn: F) -> Self
    where
        F: FnMut() -> Result<Nodes, anyhow::Error> + Send + Sync + 'static,
    {
        BuildNodesFn(Box::new(build_nodes_fn))
    }
}

/// Configuration value used to indicate in which order the child nodes of a
/// `capataz::SupervisorSpec` should start.
///
/// Since: 0.0.0
#[derive(Clone, Debug)]
pub enum StartOrder {
    /// BLAH BLAH BLAH
    LeftToRight,
    /// BLAH BLAH BLAH
    RightToLeft,
}

/// Represents a configuration option that can be set on a
/// `capataz::SupervisorSpec`.
///
/// Since: 0.0.0
pub struct Opt(Box<dyn FnMut(&mut Spec) + Send + Sync + 'static>);

pub struct RunningNodes {
    // vector to keep track of the initial node order
    nodes_ix: Vec<String>,
    // hashmap with nodes mapped by names, used for easy lookup
    // of nodes when dealing with errors
    running_nodes: HashMap<String, RunningNode>,
    // cleanup function for the existing running nodes
    cleanup: CleanupFn,
}

impl RunningNodes {
    pub(crate) fn new(input_nodes: Vec<RunningNode>, cleanup: CleanupFn) -> Self {
        let mut nodes_ix = Vec::new();
        let mut running_nodes = HashMap::new();

        for node in input_nodes {
            nodes_ix.push(node.get_name().to_owned());
            running_nodes.insert(node.get_name().to_owned(), node);
        }

        Self {
            nodes_ix,
            running_nodes,
            cleanup,
        }
    }

    pub(crate) async fn restart_node(
        &mut self,
        ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: TerminationNotifier,
        node_name: &str,
    ) -> Result<(), subtree::TerminationError> {
        // Fetch the collection of running nodes and take ownership of the
        // failed node to restart.
        if let Some(running_node) = self.running_nodes.remove(node_name) {
            // Execute the restart logic depending on the RestartStrategy of
            // this node.
            let restart_strategy = running_node.get_restart_strategy().clone();
            match restart_strategy {
                node::Restart::OneForOne => {
                    // Terminate node to get back the original spec. This spec
                    // allows us to create a new running node (effective
                    // restart).
                    let (_, mut node_spec) = running_node.terminate(ev_notifier.clone()).await;

                    // Create a new context with the appropiate parent_name
                    let ctx = Context::new();
                    let ctx = ctx.with_parent_name(parent_name);

                    // Loop on the worker creation until surpassing error
                    // tolerance or the node starts without an error.
                    loop {
                        let result = node_spec
                            .start(
                                ctx.clone(),
                                ev_notifier.clone(),
                                parent_name,
                                parent_chan.clone(),
                            )
                            .await;

                        match result {
                            Err((_start_err, failed_node_spec)) => {
                                // Recover the node_spec to do another iteration
                                // in the loop.
                                node_spec = failed_node_spec;
                                continue;
                            }
                            Ok(running_node) => {
                                // Re-assign ownership of the running node back
                                // to the RunningNodes record.
                                let _ = self
                                    .running_nodes
                                    .insert(node_name.to_owned(), running_node);
                                return Ok(());
                            }
                        }
                    }
                }
                restart => todo!("pending restart strategy implementation: {:?}", restart),
            }
        }

        // In the situation the node is not found in the running_nodes, ignore.
        todo!("handle node is not present in running_nodes")
    }

    /// Iterates over all running nodes in reversed order (relative to the start
    /// order). This function is used when terminating or restarting a
    /// supervision tree.
    ///
    /// Note, a child node having a termination error won't abort the
    /// termination procedure of the remaining running nodes.
    pub(crate) async fn terminate(
        self,
        ev_notifier: EventNotifier,
        parent_name: &str,
    ) -> Result<(), subtree::TerminationError> {
        let Self {
            nodes_ix,
            mut running_nodes,
            cleanup,
            ..
        } = self;

        // Accumulate all the termination errors from our child nodes.
        let mut node_termination_errors = Vec::new();

        // let mut w = std::fs::File::create("/tmp/test.txt").unwrap();

        // Terminate workers in reverse order.
        // TODO: take into account start order option.
        for node_name in nodes_ix.into_iter().rev() {
            if let Some(node) = running_nodes.remove(&node_name) {
                let result = node.terminate(ev_notifier.clone()).await;
                match result {
                    // Note, we do not return the node_specs as we do not need them again to
                    // restart.
                    (Err(worker_err), _node_spec) => {
                        // Append error if child node failed to terminate.
                        node_termination_errors.push(worker_err);
                    }
                    _ => (),
                }
            }
        }

        // Next, cleanup resources allocated by the supervisor
        let cleanup_err = cleanup.call().err();

        // Report an error if the child nodes failed to terminate or there was a
        // cleanup error
        if node_termination_errors.len() > 0 || cleanup_err.is_some() {
            let termination_err = subtree::TerminationError::termination_failed(
                parent_name,
                node_termination_errors,
                cleanup_err,
            );
            Err(termination_err)
        } else {
            Ok(())
        }
    }
}

/// Represents the root of a tree of tasks. A Supervisor may have leaf or
/// sub-tree child nodes, where each of the nodes in the tree represent a task
/// that gets automatic restart abilities as soon as the parent supervisor
/// detects an error has occured. A Supervisor will always be generated from a
/// `capataz::Spec`
///
/// Since: 0.0.0
pub struct Root {
    spec: Spec,
    running_subtree: subtree::RunningSubtree,
    ev_notifier: EventNotifier,
}

impl Root {
    /// Executes the termination logic of the supervision tree. This function
    /// will halt execution on the current thread until all nodes on the tree
    /// are terminated in the desired order.
    ///
    /// Since: 0.0.0
    pub async fn terminate(self) -> (Result<(), subtree::TerminationError>, Spec) {
        let spec = self.spec;
        // Every subtree restart re-creates all the child nodes, let us ignore
        // the previously created subtree spec.
        let (result, _subtree_spec) = self.running_subtree.terminate(self.ev_notifier).await;
        (result, spec)
    }
}

/// Represents the specification of a static supervision tree; it serves as a
/// template for the construction of a runtime supervision tree.
///
/// With a `capataz::SupervisorSpec` value you may configure settings like:
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
    start_order: StartOrder,
}

#[derive(Debug)]
struct SpecDebug {
    name: String,
    start_order: StartOrder,
}

impl std::fmt::Debug for Spec {
    fn fmt(&self, format: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let debug = SpecDebug {
            name: self.name.clone(),
            start_order: self.start_order.clone(),
        };
        debug.fmt(format)
    }
}

impl Spec {
    /// Creates a `capataz::SupervisorSpec` value using an anonymous function
    /// that build the nodes that the `capataz::Supervisor` will run and
    /// monitor.
    ///
    /// If you need to allocate a resource that is going to be shared across
    /// multiple child nodes (e.g. a database connection), you may want to use
    /// `capataz::SupervisorSpec::new_with_cleanup` instead.
    ///
    /// Since: 0.0.0
    pub fn new<S, F>(name: S, mut opts: Vec<Opt>, mut build_nodes_fn: F) -> Self
    where
        S: Into<String>,
        F: FnMut() -> Vec<Node> + Send + Sync + 'static,
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
        };

        for opt_fn in &mut opts {
            opt_fn.0(&mut spec);
        }

        spec
    }

    /// Similar to `capataz::SupervisorSpec::new`, this function creates a
    /// `capataz::SupervisorSpec` using an anonymous function that builds the
    /// nodes that is going to run and monitor, as well as a resource allocation
    /// with a proper cleanup strategy.
    ///
    /// The given anonymous function must return a tuple that contains a
    /// `Vec<Node>` and another annonymous function that executes the resource
    /// de-allocation. The tuple must be wrapped in a `Result::Ok` value.
    ///
    /// For error handling when allocating resources, this function any type
    /// that is an Error (delagating error handling to the `anyhow` API).
    ///
    /// #### Example
    ///
    /// The following (hypothetical) example creates a supervision tree with two
    /// child nodes that use a shared Redis client.
    ///
    /// ```ignore
    /// capataz::SupervisorSpec::new_with_cleanup("my-supervisor", vec![], || {
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
        };

        for opt_fn in &mut opts {
            opt_fn.0(&mut spec);
        }

        spec
    }

    /// Transforms this `capataz::SupervisorSpec` into a subtree `capataz::Node`
    /// with some node configuration.
    ///
    /// Since: 0.0.0
    pub fn subtree(self, opts: Vec<leaf::Opt>) -> Node {
        let subtree_spec = subtree::Spec::new(self, opts);
        node::Node(node::NodeSpec::Subtree(subtree_spec))
    }

    /// Returns the name associated to this `capataz::SupervisorSpec`.
    ///
    /// Since: 0.0.0
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Executes the child nodes supervision logic.
    async fn run_supervision_loop(
        &mut self,
        ctx: Context,
        ev_notifier: EventNotifier,
        mut sup_chan: TerminationListener,
        parent_chan: TerminationNotifier,
        runtime_name: &str,
        mut running_nodes: RunningNodes,
    ) -> Result<(), subtree::TerminationError> {
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
                    return Ok(());
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
                        Some(task::TerminationError::TaskForcedKilled { .. }) => {
                            unreachable!(
                                "invalid implementation; forced kill should not be sent to parent_chan"
                            )
                        }
                        Some(task::TerminationError::TaskAborted { .. }) => {
                            unreachable!(
                                "invalid implementation; abort task should not be sent to parent_chan"
                            )
                        }
                        Some(task::TerminationError::TaskFailureNotified { .. }) => {
                            unreachable!("invalid implementation; failure notified should not be sent to parent_chan")
                        }
                        Some(task::TerminationError::TaskPanic { .. }) => {
                            todo!("panic handling logic pending")
                        }
                        Some(task::TerminationError::TaskFailed { err, .. }) => {
                            err.notify_error(ev_notifier.clone()).await;
                            let node_name = node::to_node_name(err.get_runtime_name());
                            running_nodes
                                .restart_node(
                                    ev_notifier.clone(),
                                    runtime_name,
                                    parent_chan.clone(),
                                    node_name,
                                )
                                .await?;
                            continue;
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
    ) -> Result<(), node::TerminationError> {
        // Build the name that is going to be used in all the supervision system
        // telemetry.
        let runtime_name = node::build_runtime_name(parent_name, &self.name);

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
                Err(subtree::TerminationError::StartErrorAlreadyReported.into())
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
                        parent_chan.clone(),
                    )
                    .await;

                match result {
                    // One of the child nodes failed to start
                    Err(start_err) => {
                        start_notifier.failed(start_err);
                        Err(subtree::TerminationError::StartErrorAlreadyReported.into())
                    }
                    // All child nodes started without errors
                    Ok(running_nodes) => {
                        // Notify our caller that we have started so that
                        // Supervisor record can be returned from a start call.
                        start_notifier.success();

                        // Run the supervision logic until restart tolerance is
                        // reached or termination is signalled.
                        let result = self
                            .run_supervision_loop(
                                ctx,
                                ev_notifier,
                                sup_chan,
                                parent_chan,
                                &runtime_name,
                                running_nodes,
                            )
                            .await;

                        // Return the subtree::TerminationError wrapped in a
                        // node::TerminationError
                        result.map_err(node::TerminationError::Subtree)
                    }
                }
            }
        }
    }

    /// Spawns all the nodes in this `capataz::SupervisorSpec` and returns a
    /// `capataz::Supervisor`.
    ///
    /// A `capataz::Supervisor` is a tree of workers and/or supervisors
    /// (sub-trees). This method spawns the workers (leaf) tasks first and then
    /// continues spawning nodes up in the tree heriarchy. Depending on the
    /// `capataz::SupervisorSpec` configuration, the start order will be in
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
    ) -> Result<Root, subtree::StartError> {
        // Create a copy of this supervisor and automatically transform into a
        // subtree to re-utilize the start subtree logic.
        let root_spec = self.clone().subtree(Vec::new());
        let ev_notifier = EventNotifier::new(ev_listener);

        let result = root_spec
            .0
            .start(ctx, ev_notifier.clone(), *SUPERVISOR_ROOT_NAME, None)
            .await;

        match result {
            Err((node::StartError::Leaf(_), _spec)) => {
                unreachable!("invalid implementation; subtree code is returning a leaf error")
            }
            Ok(node::RunningNode::Leaf(_)) => {
                unreachable!("invalid implementation; subtree code is returning a leaf value")
            }
            Err((node::StartError::Subtree(start_err), _spec)) => Err(start_err),
            Ok(node::RunningNode::Subtree(running_subtree)) => Ok(Root {
                spec: self,
                running_subtree,
                ev_notifier,
            }),
        }
    }

    /// Specifies the start and termination order in of the child nodes for a
    /// given `capataz::SupervisorSpec`.
    ///
    /// If this configuration option is not specified, the default value
    /// is `capataz::StartOrder::LeftToRight`
    ///
    /// Since: 0.0.0
    pub fn with_start_order(order: StartOrder) -> Opt {
        Opt(Box::new(move |spec| spec.start_order = order.clone()))
    }
}
