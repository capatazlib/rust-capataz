use std::fmt;
use std::sync::Arc;

use anyhow::anyhow;
use futures::future::{self, AbortHandle};
use futures::FutureExt;
use lazy_static::lazy_static;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};

use crate::context::Context;
use crate::events::{EventListener, EventNotifier};
use crate::notifier;
use crate::worker;

lazy_static! {
    /// Default buffer size for a supervisor error channel.
    static ref SUPERVISOR_DEF_CHAN_BUFFER_SIZE: usize = 100;
    /// Default root name
    static ref SUPERVISOR_ROOT_NAME: &'static str = "";
}

pub type StartNotifier = notifier::StartNotifier<anyhow::Error>;

pub type WorkerSpec = worker::Spec<(), anyhow::Error, anyhow::Error>;

pub type RuntimeWorker = worker::Worker<(), anyhow::Error, anyhow::Error>;

pub type RuntimeSubtree = worker::Worker<Spec, Arc<StartError>, anyhow::Error>;

/// A general purpose start error for workers.
pub type WorkerStartError = worker::StartError<anyhow::Error>;

/// A general purpose termination error for workers.
pub type WorkerTerminationError = worker::TerminationError<anyhow::Error>;

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("{0}")]
    StartError(#[from] Arc<StartError>),
    #[error("{0}")]
    TerminationError(#[from] TerminationError),
    #[error("{0}")]
    RuntimeError(#[from] RuntimeError),
    #[error("start error already reported")]
    StartErrorAlreadyReported,
}

/// An error reported while a supervised node is running
#[derive(Debug, Error)]
#[error("worker failed on startup")]
pub struct RuntimeError(#[from] anyhow::Error);

/// An error reported when starting a supervisor.
#[derive(Debug, Error)]
pub enum StartError {
    #[error("worker failed to start: {0}")]
    WorkerStartFailed(#[from] Arc<WorkerStartError>),
    #[error("supervisor failed to start: {start_err}")]
    SupervisorStartFailed {
        start_err: Arc<StartError>,
        termination_err: Option<TerminationError>,
    },
    #[error("supervisor failed on build: {0}")]
    SupervisorBuildFailed(#[from] BuildError),
}

/// An error reported when terminating a supervisor.
#[derive(Debug, Error)]
pub enum TerminationError {
    #[error("worker failed to terminate")]
    WorkerTerminationFailed(#[from] Arc<WorkerTerminationError>),
    #[error("supervisor failed to terminate")]
    SupervisorTerminationFailed(String, Vec<TerminationError>),
}

/// An error reported when cleaning up resources allocated when a supervisor got
/// started.
#[derive(Debug, Error)]
pub enum CleanupError {
    #[error("supervisor failed to cleanup allocated resources")]
    CleanupError(anyhow::Error),
}

/// An error reported when constructing the nodes of a supervisor.
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("{0}")]
    BuildError(#[from] anyhow::Error),
}

/// The specification of a Supervisor.
pub struct Spec {
    name: String,
    build_tree: Box<dyn FnMut() -> Result<BuiltNodes, BuildError> + Send + Sync + 'static>,
}

/// Represents the constructed tree nodes accompanied with a cleanup routine in
/// the situation there was an allocation done in the build tree function.
struct BuiltNodes {
    nodes: Vec<Node>,
    cleanup: Option<Cleanup>,
}

/// A running Supervisor.
pub struct Supervisor {
    ev_notifier: EventNotifier,
    abort_handle: AbortHandle,
    join_handle: JoinHandle<Result<Spec, SupervisorError>>,
}

/// Contains the specification of a component from a supervision tree. It can
/// either be the specification for a Leaf (worker) or a Subtree (supervisor).
pub enum Node {
    Leaf(WorkerSpec),
    Subtree(Spec),
}

/// The runtime representation of a Node. Can only be obtained by calling the
/// `Node::start` method. Each of these values represents a task at runtime.
pub(crate) enum RuntimeNode {
    Leaf(RuntimeWorker),
    Subtree(RuntimeSubtree),
}

/// Allows us to create a supervision tree worker (leaf).
pub struct Worker;

/// An option for a Worker of a supervision tree
pub struct WorkerOpt(Box<dyn FnOnce(&mut worker::Spec<(), anyhow::Error, anyhow::Error>)>);

/// Contains the logic to cleanup a resource allocated when starting a
/// supervision tree.
pub struct Cleanup(Box<dyn FnOnce() -> CleanupError + Send + Sync + 'static>);

impl Supervisor {
    /// Stops the supervision tree
    pub async fn terminate(self) -> Result<(), SupervisorError> {
        // signal the runtime context to abort
        self.abort_handle.abort();
        let result = self.join_handle.await;
        match result {
            // SAFETY: Received a JoinError, this should never happen because
            // our library code should not throw panics or have an invalid
            // implementation.
            Err(_) => unreachable!("this should never happen; invalid implementation"),
            // SAFETY: Received a start error on a terminate call, this should
            // never happen because when we have a start error, we do not create
            // a Supervisor record.
            Ok(Err(SupervisorError::StartErrorAlreadyReported)) => {
                unreachable!("this should never happen; invalid implementation")
            }
            // Recevied a termination error.
            Ok(Err(err)) => Err(err),
            // Receiving back the spec, given this is the parent supervisor, we
            // ignore this in favor of using the Node factory again.
            Ok(Ok(_spec)) => Ok(()),
        }
    }
}

impl RuntimeNode {
    pub(crate) async fn terminate(
        self,
        mut ev_notifier: EventNotifier,
    ) -> Result<(), TerminationError> {
        match self {
            RuntimeNode::Leaf(worker) => {
                let runtime_name = worker.runtime_name.clone();
                let result = worker.terminate().await;
                match result {
                    Err(err) => {
                        let err = Arc::new(err);
                        ev_notifier
                            .worker_termination_failed(&runtime_name, err.clone())
                            .await;
                        Err(TerminationError::WorkerTerminationFailed(err))
                    }
                    Ok(_) => {
                        ev_notifier.worker_terminated(&runtime_name).await;
                        Ok(())
                    }
                }
            }
            _ => {
                // terminate_child_nodes(supervisor.ev_notifier, )
                todo!("sub-tree termination not implemented!");
            }
        }
    }
}

impl WorkerOpt {
    /// apply calls the function representing the option update
    fn apply(self, spec: &mut WorkerSpec) {
        self.0(spec)
    }
}

/// An option that specifies the shutdown procedure for a specific worker in a
/// supervision tree
pub fn with_shutdown(shutdown: worker::Shutdown) -> WorkerOpt {
    WorkerOpt(Box::new(move |spec| {
        spec.with_shutdown(shutdown);
    }))
}

impl Worker {
    /// Creates a `Node` that represents a supervised concurrent task. The
    /// function that contains the task execution requires a name, which is used
    /// for runtime tracing.
    pub fn new<S, F, O>(name: S, mut routine: F, opts: Vec<WorkerOpt>) -> Node
    where
        S: Into<String>,
        F: (FnMut(Context) -> O) + Send + 'static,
        O: future::Future<Output = Result<(), anyhow::Error>> + FutureExt + Send + Sized + 'static,
    {
        let routine1 = move |ctx, start_fn: StartNotifier| {
            start_fn.success();
            routine(ctx)
        };
        Self::new_with_start(name, opts, routine1)
    }

    /// Creates a `Node` that represents a supervised concurrent task. The
    /// function that contains the task execution requires two arguments: a
    /// name, and a `StartNotifier`
    ///
    /// ### The name argument
    ///
    /// A name argument must not be empty nor contain forward slash characters
    /// (e.g. /).
    ///
    /// ### The `StartNotifier` argument
    ///
    /// A value that signals to the supervision tree API that the task is
    /// initialized and started. You may want to use this when initializing
    /// resources that take a while to allocate.
    ///
    pub fn new_with_start<S, F, O>(name: S, opts: Vec<WorkerOpt>, routine: F) -> Node
    where
        S: Into<String>,
        F: (FnMut(Context, notifier::StartNotifier<anyhow::Error>) -> O) + Send + 'static,
        O: future::Future<Output = Result<(), anyhow::Error>> + FutureExt + Send + Sized + 'static,
    {
        let mut spec = worker::Spec::new_with_start(name, routine);
        for opt in opts {
            opt.apply(&mut spec);
        }
        Node::Leaf(spec)
    }
}

// Doesn't work because of FnOnce vs FnMut; we need a factory of Spec
//
// async fn start_subtree(
//     ctx: Context,
//     ev_notifier: EventNotifier,
//     parent_name: String,
//     parent_chan: mpsc::Sender<WorkerTerminationError>,
//     subtree_spec: Spec,
// ) -> Result<RuntimeSubtree, StartError> {
//     let subtree_spec_name = subtree_spec.name.clone();
//     let task_parent_name = parent_name.clone();
//     let task_ev_notifier = ev_notifier.clone();

//     // Build a new worker::Spec for the given supervision sub-tree specification.
//     let worker_spec: worker::Spec<Spec, Arc<StartError>, anyhow::Error> =
//         worker::Spec::new_with_start(
//             subtree_spec_name,
//             move |ctx: Context, start_notifier: notifier::StartNotifier<Arc<StartError>>| {
//                 let parent_name = task_parent_name.clone();
//                 let ev_notifier = task_ev_notifier.clone();
//                 async move {
//                     let subtree_worker_spec = subtree_spec
//                         .run(ctx, ev_notifier.clone(), start_notifier, &parent_name)
//                         .await?;
//                     Ok(subtree_worker_spec)
//                 }
//             },
//         );

//     // Start the sub-tree
//     let result = worker_spec.start(&ctx, &parent_name, parent_chan).await;
//     match result {
//         Err(worker::StartError::StartTimeoutError(_)) => {
//             unreachable!("sub-trees have an indefinite start timeout");
//         }
//         Err(worker::StartError::StartRecvError(_)) => {
//             unreachable!("this happens only if the library has an implementation bug");
//         }
//         Err(worker::StartError::ClientError(start_err)) => Err(StartError::SupervisorStartFailed {
//             start_err,
//             termination_err: None,
//         }),
//         Ok(subtree) => Ok(subtree),
//     }
// }

impl Node {
    /// Creates a concurrent task with the Node's specification
    pub(crate) async fn start(
        self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: mpsc::Sender<WorkerTerminationError>,
    ) -> Result<RuntimeNode, StartError> {
        match self {
            Node::Leaf(worker_spec) => {
                let runtime_name = worker_spec.build_runtime_name(parent_name);
                let result = worker_spec.start(&ctx, parent_name, parent_chan).await;
                match result {
                    Err(err) => {
                        let err = Arc::new(err);
                        ev_notifier
                            .worker_start_failed(&runtime_name, err.clone())
                            .await;
                        Err(StartError::WorkerStartFailed(err))
                    }
                    Ok(worker) => {
                        ev_notifier.worker_started(&runtime_name).await;
                        Ok(RuntimeNode::Leaf(worker))
                    }
                }
            }
            Node::Subtree(subtree_spec) => {
                todo!()
                // let subtree_parent_name = parent_name.to_owned();
                // // Build a new worker::Spec for the given supervision sub-tree specification.
                // let worker_spec: worker::Spec<Spec, Arc<StartError>, anyhow::Error> =
                //     worker::Spec::new_with_start(
                //         subtree_spec.name,
                //         |ctx: Context, start_notifier: notifier::StartNotifier<Arc<StartError>>| async move {
                //             let subtree_spec = subtree_spec
                //                 .run(ctx, ev_notifier, start_notifier, &subtree_parent_name)
                //                 .await?;
                //             Ok(subtree_spec)
                //         });

                // // Start the sub-tree
                // let result = worker_spec.start(&ctx, parent_name, parent_chan).await;
                // match result {
                //     Err(worker::StartError::StartTimeoutError(_)) => {
                //         unreachable!("sub-trees have an indefinite start timeout");
                //     }
                //     Err(worker::StartError::StartRecvError(_)) => {
                //         unreachable!("this happens only if the library has an implementation bug");
                //     }
                //     Err(worker::StartError::ClientError(start_err)) => {
                //         Err(StartError::SupervisorStartFailed {
                //             start_err,
                //             termination_err: None,
                //         })
                //     }
                //     Ok(subtree) => Ok(RuntimeNode::Subtree(subtree)),
                // }
            }
        }
    }
}

/// Iterates over all child nodes in reversed order (relative to the start
/// order). This function is used when terminating or restarting a supervision
/// tree. A child node having a termination error won't stop the termination of
/// the remaining nodes.
pub(crate) async fn terminate_child_nodes(
    ev_notifier: EventNotifier,
    parent_name: &str,
    nodes: Vec<RuntimeNode>,
) -> Result<(), TerminationError> {
    // Accumulate all the termination errors from our child nodes.
    let mut worker_termination_errors = Vec::new();

    // Terminate workers in reverse order.
    // TODO: take into account start order option.
    for node in nodes.into_iter().rev() {
        if let Err(worker_err) = node.terminate(ev_notifier.clone()).await {
            // Append error if child node failed to terminate.
            worker_termination_errors.push(worker_err);
        }
    }
    if worker_termination_errors.len() > 0 {
        // When there are termination errors, we must return all the errors.
        Err(TerminationError::SupervisorTerminationFailed(
            parent_name.to_owned(),
            worker_termination_errors,
        ))
    } else {
        Ok(())
    }
}

impl Cleanup {
    /// Creates a new `Cleanup` value.
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce() -> CleanupError + Send + Sync + 'static,
    {
        Cleanup(Box::new(f))
    }
}

impl Spec {
    /// Creates a new supervisor spec from a factory method. The factory method
    /// must return the child nodes that get spawned as well as the cleanup
    /// routine. The cleanup routine is intended to be used in situations where
    /// you needto allocate some resource (db connection, etc). The capataz API
    /// executes the cleanup operation when this supervisor gets terminated or
    /// restarted.
    ///
    /// If you do not need a cleanup operation, use `Spec::new` instead.
    pub(crate) fn new_with_cleanup<S, CF, E>(name_input: S, mut build_nodes: CF) -> Self
    where
        S: Into<String>,
        CF: FnMut() -> Result<(Vec<Node>, Cleanup), E> + Send + Sync + 'static,
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        let build_tree = move || {
            // Use the build_nodes function. This function is easier to handle
            // for API clients, as they don't need to interact with the TreeSpec
            // type.
            let (nodes, cleanup) = build_nodes().map_err(|err| anyhow!(err))?;

            // Transform the values returned by the build_nodes function into a
            // TreeSpec.
            Ok(BuiltNodes {
                nodes,
                cleanup: Some(cleanup),
            })
        };

        Spec {
            name: name_input.into(),
            build_tree: Box::new(build_tree),
        }
    }

    /// Creates a new supervisor spec from a factory method that builds nodes.
    /// We need factory method because it will be called every time a supervisor
    /// gets restarted.
    pub fn new<S, F>(name_input: S, mut build_nodes: F) -> Self
    where
        S: Into<String>,
        F: FnMut() -> Vec<Node> + Send + Sync + 'static,
    {
        let build_tree = move || {
            let nodes = build_nodes();
            Ok(BuiltNodes {
                nodes,
                cleanup: None,
            })
        };
        Spec {
            name: name_input.into(),
            build_tree: Box::new(build_tree),
        }
    }

    /// Responsible of the supervision logic of the supervisor task.
    async fn run_supervision_loop(
        &mut self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        mut sup_chan: mpsc::Receiver<WorkerTerminationError>,
        runtime_name: &str,
        children: Vec<RuntimeNode>,
    ) -> Result<(), SupervisorError> {
        loop {
            tokio::select! {
                _ = ctx.done() => {
                    // Received stop signal, terminate children of this
                    // supervisor and return.
                    terminate_child_nodes(ev_notifier.clone(), runtime_name, children).await?;
                    ev_notifier.supervisor_terminated(&runtime_name).await;
                    return Ok(());
                },
                Some(_worker_err)  = sup_chan.recv() => {
                    todo!("execute restart logic here")
                }
            }
        }
    }

    /// Builds and starts the Supervisor child nodes in the specified order.
    async fn start_child_nodes(
        &mut self,
        ctx: Context,
        ev_notifier: EventNotifier,
        runtime_name: &str,
        parent_chan: mpsc::Sender<WorkerTerminationError>,
    ) -> Result<Vec<RuntimeNode>, StartError> {
        // Build the nodes that this supervision thread is going to be
        // monitoring.
        let built_nodes = (self.build_tree)()?;

        // Create a runtime node per node spec
        let mut runtime_nodes = Vec::new();
        for node in built_nodes.nodes {
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
                Ok(runtime_node) => {
                    // When the node started without errors, add it to the vec of runtime_nodes
                    runtime_nodes.push(runtime_node);
                }
                Err(start_err) => {
                    // A child node failed to start, we need to perform a cleanup operation and stop
                    // all the previous children that got started in reversed order
                    let start_err = Arc::new(start_err);

                    if let Err(termination_err) =
                        terminate_child_nodes(ev_notifier.clone(), runtime_name, runtime_nodes)
                            .await
                    {
                        // When terminating children, some of them may have a
                        // termination failure, register them and bubble them up.
                        return Err(StartError::SupervisorStartFailed {
                            start_err,
                            termination_err: Some(termination_err),
                        });
                    } else {
                        // Otherwise, return supervisor start failure with the
                        // start_error alone.
                        return Err(StartError::SupervisorStartFailed {
                            start_err,
                            termination_err: None,
                        });
                    }
                }
            }
        }
        // Happy path, all nodes were started without errors.
        Ok(runtime_nodes)
    }

    /// Creates the name for the supervisor, taking special consideration for
    /// the supervisor root name.
    fn build_runtime_name(&self, parent_name: &str) -> String {
        if parent_name == *SUPERVISOR_ROOT_NAME {
            format!("/{}", self.name)
        } else {
            format!("{}/{}", parent_name, self.name)
        }
    }

    /// Initializes and starts executes the supervision logic in the current task.
    pub(crate) async fn run(
        mut self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        start_notifier: notifier::StartNotifier<Arc<StartError>>,
        parent_name: &str,
    ) -> Result<Self, SupervisorError> {
        // Build the name that is going to be used in all the supervision system
        // telemetry.
        let runtime_name = self.build_runtime_name(parent_name);

        // Build the channels that are going to be used by the supervised
        // workers to notify the supervisor that something went wrong, and allow
        // the supervisor to restart or fail.
        let (parent_chan, sup_chan) = mpsc::channel(*SUPERVISOR_DEF_CHAN_BUFFER_SIZE);

        // Build the child nodes and start them.
        // TODO: allow the start ordering setting to be used here.
        let result = self
            .start_child_nodes(ctx.clone(), ev_notifier.clone(), &runtime_name, parent_chan)
            .await;

        match result {
            // We could navigate this branch for a few reasons:
            //
            // * The provided build nodes function failed with an error
            //
            // * A child worker failed with an error
            //
            Err(start_err) => {
                let start_err = Arc::new(start_err);
                // Notify the supervisor parent that the start failed, is important
                // to have this dedicated notifier, as the behavior of start errors is
                // different from regular supervision.
                start_notifier.failed(start_err.clone());
                // Notify the event system that the supervisor failed.
                ev_notifier
                    .supervisor_start_failed(&runtime_name, start_err)
                    .await;
                // We have dealt with the error on the start_notifier call, we
                // we cannot provide it because we moved it already.
                Err(SupervisorError::StartErrorAlreadyReported)
            }
            // Everything went on the happy paht
            Ok(runtime_nodes) => {
                // Notify our caller that we have started so that Supervisor
                // record can be returned from a start call.
                start_notifier.success();
                // Notify to the event system that the supervisor has started.
                ev_notifier.supervisor_started(&runtime_name).await;
                // Run the children supervision logic until restart tolerance is
                // reached or termination is signalled.
                self.run_supervision_loop(ctx, ev_notifier, sup_chan, &runtime_name, runtime_nodes)
                    .await?;
                // Return the spec so that we can restart from it again.
                Ok(self)
            }
        }
    }

    /// Runs this `Spec` as the root supervision tree.
    pub async fn start(
        self,
        ctx: Context,
        ev_listener: EventListener,
    ) -> Result<Supervisor, SupervisorError> {
        // Create an abort_handle from the given context call, so that we can
        // terminate only this Supervisor on `terminate`.
        let (ctx, abort_handle) = ctx.with_cancel();

        // Create an EventNotifier from the provided EventListener
        let ev_notifier = EventNotifier::new(ev_listener);

        // Build a channel that singals the Supervisor task started without any
        // errors; we do not have to wait for the termination of the
        // Supervisor's JoinHandle to figure this out.
        let (notify_started, wait_started) = oneshot::channel::<Result<(), Arc<StartError>>>();
        let start_notifier = notifier::StartNotifier::from_oneshot(notify_started);

        // Clone the EventNotifier so that the supervision thread can use it
        let ev_notifier0 = ev_notifier.clone();

        // Create the JoinHandle that will contain the supervisor loop task
        let join_handle = task::spawn(async move {
            self.run(ctx, ev_notifier0, start_notifier, &SUPERVISOR_ROOT_NAME)
                .await
        });

        // The supervisor task will signal when all the children had started.
        //
        // SAFETY: The first expect call is error machinery from oneshot, given
        // the implementation of the supervision logic is either right or wrong,
        // we can remove that error check.
        //
        // The ? is for the start error that may come from the initialization of
        // the supervisor Spec
        wait_started
            .await
            .expect("sender or receiver got dropped; implementation error")?;

        // Everything started correctly, return the Supervisor record.
        Ok(Supervisor {
            ev_notifier,
            abort_handle,
            join_handle,
        })
    }

    // pub fn start_without_listener(&self) -> Result<Supervisor, StartError> {
    //     self.start(EventListener::empty())
    // }
}

/// Transforms an `Spec` to a sub-tree `Node`
pub fn subtree(spec: Spec) -> Node {
    Node::Subtree(spec)
}

mod tests {
    use anyhow::anyhow;

    use crate::context::Context;
    use crate::events::{
        new_testing_listener, supervisor_start_failed, supervisor_started, supervisor_terminated,
        worker_start_failed, worker_started, worker_terminated, EventBufferCollector,
        EventListener,
    };
    use crate::supervisor::{Node, Spec, StartNotifier, Worker, WorkerOpt};

    fn wait_done_worker(name: &str, options: Vec<WorkerOpt>) -> Node {
        Worker::new(
            name,
            |ctx: Context| async move {
                let _ = ctx.done().await;
                Ok(())
            },
            options,
        )
    }

    fn fail_start_worker(name: &str, options: Vec<WorkerOpt>) -> Node {
        Worker::new_with_start(
            name,
            vec![],
            |_ctx: Context, start_notifier: StartNotifier| async move {
                let err = anyhow!("failing worker");
                start_notifier.failed(err);
                Ok(())
            },
        )
    }

    #[tokio::test]
    async fn test_single_worker() {
        let spec = Spec::new("root", || vec![wait_done_worker("worker", vec![])]);

        let (ev_listener, ev_buffer) = new_testing_listener().await;
        let sup = spec
            .start(Context::new(), ev_listener)
            .await
            .expect("supervisor should start with no error");
        sup.terminate()
            .await
            .expect("supervisor should terminate without errors");

        ev_buffer
            .assert_exact(vec![
                worker_started("/root/worker"),
                supervisor_started("/root"),
                worker_terminated("/root/worker"),
                supervisor_terminated("/root"),
            ])
            .await;
    }

    #[tokio::test]
    async fn test_multiple_single_level_workers() {
        let spec = Spec::new("root", || {
            vec![
                wait_done_worker("worker1", vec![]),
                wait_done_worker("worker2", vec![]),
                wait_done_worker("worker3", vec![]),
            ]
        });

        let (ev_listener, ev_buffer) = new_testing_listener().await;
        let sup = spec
            .start(Context::new(), ev_listener)
            .await
            .expect("supervisor should start with no error");
        sup.terminate()
            .await
            .expect("supervisor should terminate without errors");

        ev_buffer
            .assert_exact(vec![
                worker_started("/root/worker1"),
                worker_started("/root/worker2"),
                worker_started("/root/worker3"),
                supervisor_started("/root"),
                worker_terminated("/root/worker3"),
                worker_terminated("/root/worker2"),
                worker_terminated("/root/worker1"),
                supervisor_terminated("/root"),
            ])
            .await;
    }

    #[tokio::test]
    async fn test_multiple_single_level_failing_worker() {
        let spec = Spec::new("root", || {
            vec![
                wait_done_worker("worker1", vec![]),
                wait_done_worker("worker2", vec![]),
                fail_start_worker("worker3", vec![]),
                // The start procedure never arrives to this worker
                wait_done_worker("worker4", vec![]),
            ]
        });

        let (ev_listener, ev_buffer) = new_testing_listener().await;
        let result = spec.start(Context::new(), ev_listener).await;

        match result {
            Err(start_err) => assert_eq!(
                "supervisor failed to start: worker failed to start: failing worker",
                format!("{}", start_err)
            ),
            Ok(_) => assert!(false, "expecting error, got started supervisor"),
        };

        // worker4 never gets started/terminated because we start from left to
        // right
        ev_buffer
            .assert_exact(vec![
                worker_started("/root/worker1"),
                worker_started("/root/worker2"),
                worker_start_failed("/root/worker3"),
                worker_terminated("/root/worker2"),
                worker_terminated("/root/worker1"),
                supervisor_start_failed("/root"),
            ])
            .await;
    }

    #[tokio::test]
    async fn test_single_level_build_error() {
        let spec = Spec::new_with_cleanup("root", || Err(anyhow!("build failure")));

        let (ev_listener, ev_buffer) = new_testing_listener().await;
        let result = spec.start(Context::new(), ev_listener).await;

        match result {
            Err(start_err) => assert_eq!(
                "supervisor failed on build: build failure",
                format!("{}", start_err)
            ),
            Ok(_) => assert!(false, "expecting error, got started supervisor"),
        };
    }

    /*

    #[tokio::test]
    async fn test_cleanup_error()

    #[tokio::test]
    async fn test_two_level_start_and_termination()

    #[tokio::test]
    async fn test_multi_level_start_and_termination()

    #[tokio::test]
    async fn test_multi_level_build_error()

    #[tokio::test]
    async fn test_single_level_worker_permanent_restart()

    #[tokio::test]
    async fn test_single_level_worker_transient_restart()

    #[tokio::test]
    async fn test_single_level_worker_temporary_restart()

    */
}
