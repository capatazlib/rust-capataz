#[cfg(test)]
pub(crate) mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::AbortHandle;
// use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};

use crate::context::{self, Context};
use crate::events::EventNotifier;
use crate::notifier::StartNotifier;
use crate::worker::{self, Worker};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Order {
    LeftToRight,
    RightToLeft,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Lifecycle {
    Start,
    Termination,
}

impl Order {
    fn sort<A: Send + 'static>(
        &self,
        lifecycle: Lifecycle,
        children: Vec<A>,
    ) -> Box<dyn DoubleEndedIterator<Item = A> + Send> {
        let iterator = children.into_iter();
        match (self, lifecycle) {
            (Order::LeftToRight, Lifecycle::Start)
            | (Order::RightToLeft, Lifecycle::Termination) => Box::new(iterator),
            (Order::LeftToRight, Lifecycle::Termination)
            | (Order::RightToLeft, Lifecycle::Start) => Box::new(iterator.rev()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Strategy {
    OneForOne,
}

#[derive(Debug)]
pub struct StartError {
    pub failing_child_runtime_name: String,
    pub failing_child_error: Arc<worker::StartError>,
    pub failing_sibling_termination_error: Option<TerminationError>,
}

#[derive(Debug)]
pub struct TerminationError(HashMap<String, Arc<worker::TerminationError>>);

#[derive(Clone)]
pub struct SpecMeta {
    pub name: String,
    pub ev_notifier: EventNotifier,
    pub order: Order,
    pub strategy: Strategy,
}

pub struct Spec {
    pub children: Vec<worker::Spec>,

    pub meta: SpecMeta,
}

/// A value that gets returned when running a Supervisor record
enum SupervisorResult {
    // gets returned when the supervisor failed to start
    ErrStart {
        spec: Spec,
        err: Arc<StartError>,
    },
    // gets returned when the supervisor failed to start, and the
    // error is sent to a spawner thread via a start_notifier
    ErrStartHandled,
    // gets returned when a supervisor stopped running because of a termination
    // that did not fail
    OkTermination {
        spec: Spec,
    },
    // gets returned when a supervisor stopped running because of a termination
    // but it did find errors in the process
    ErrTermination {
        spec: Spec,
        err: Arc<TerminationError>,
    },
}

pub struct Supervisor {
    pub runtime_name: String,
    pub meta: SpecMeta,
    join_handle: JoinHandle<SupervisorResult>,
    termination_handle: AbortHandle,
}

// terminate_prev_siblings gets called when a child in a supervision tree fails
// to start, and all it's previous siblings need to get terminated.
async fn terminate_prev_siblings<I: DoubleEndedIterator<Item = worker::Spec> + Send>(
    ev_notifier: &mut EventNotifier,
    order: Order,
    failed_child_spec: worker::Spec,
    failed_child_start_err: Arc<worker::StartError>,
    pending_children_specs: I,
    started_children: Vec<Worker>,
) -> (Vec<worker::Spec>, StartError) {
    let (prev_specs, sibling_termination_err) =
        terminate_children(ev_notifier, started_children.into_iter().rev()).await;

    let start_err = StartError {
        failing_child_runtime_name: "".to_owned(),
        failing_child_error: failed_child_start_err,
        failing_sibling_termination_error: sibling_termination_err.err(),
    };

    // regenerate the original vector of worker specs so that the parent
    // supervisor can try again if it is allowed to do it

    let children_iterator = prev_specs
        .into_iter()
        .chain(std::iter::once(failed_child_spec))
        .chain(pending_children_specs);

    // if we started from right to left, the list must be reversed

    let children_spec = match order {
        Order::LeftToRight => children_iterator.collect(),
        Order::RightToLeft => children_iterator.rev().collect(),
    };

    (children_spec, start_err)
}

// start_child enhances the worker::Spec start logic, by also performing
// notifications to the supervision system.
async fn start_child(
    ctx: &Context,
    mut ev_notifier: EventNotifier,
    sup_runtime_name: &str,
    // TODO: receive error notifier parameter
    child_spec: worker::Spec,
) -> Result<worker::Worker, (worker::Spec, Arc<worker::StartError>)> {
    match child_spec.start(ctx, sup_runtime_name).await {
        // when the child_spec starts without hiccups
        Ok(started_worker) => {
            // send event notification indicating that the worker was started
            ev_notifier
                .worker_started(&started_worker.runtime_name)
                .await;
            Ok(started_worker)
        }
        // when the child_spec failed to start
        Err((failed_worker_spec, start_error0)) => {
            let child_runtime_name = format!("{}/{}", sup_runtime_name, failed_worker_spec.name);
            let start_error = Arc::new(start_error0);
            // send event notification indicating that the worker failed to start
            ev_notifier
                .worker_start_failed(child_runtime_name, start_error.clone())
                .await;
            Err((failed_worker_spec, start_error))
        }
    }
}

// start_children bootstrap the children of a supervision tree, it will:
//
// * Execute start logic per worker
// * Halt start sequence if one of the workers fails to start (due to start error, timeout, etc.)
// * Terminate previously started workers in case there is a failure
// * Trigger supervision system events for any start, stop or error detected.
//
async fn start_children<'a>(
    ctx: &'a Context,
    mut ev_notifier: EventNotifier,
    sup_runtime_name: &str,
    sup_order: Order,
    spec_children: Vec<worker::Spec>,
    // TODO: receive error notifier parameter
) -> Result<Vec<Worker>, (Vec<worker::Spec>, StartError)> {
    // let mut start_result = StartResult::new(spec_children.len());

    let mut started_children = Vec::with_capacity(spec_children.len());
    let mut pending_children = sup_order.sort(Lifecycle::Start, spec_children);
    let mut err = None;

    for child_spec in &mut pending_children {
        match start_child(ctx, ev_notifier.clone(), sup_runtime_name, child_spec).await {
            Ok(worker) => {
                started_children.push(worker);
            }
            Err(err_result) => {
                err = Some(err_result);
                break;
            }
        }
    }

    match err {
        None => Ok(started_children),
        Some((failed_worker_spec, start_error)) => {
            let termination_result = terminate_prev_siblings(
                &mut ev_notifier,
                sup_order,
                failed_worker_spec,
                start_error,
                pending_children,
                started_children,
            )
            .await;
            Err(termination_result)
        }
    }
}

// terminate_children terminates each worker::Worker record. In case any of the
// children fail to terminate a supervisor::TerminationError is returned.
//
// In the scenario a child worker fails to terminate, this function will
// continue the termination of the rest of it's siblings in the correct order.
//
async fn terminate_children<'a>(
    ev_notifier: &mut EventNotifier,
    runtime_children: impl Iterator<Item = Worker>,
) -> (Vec<worker::Spec>, Result<(), TerminationError>) {
    let mut spec_children = Vec::new();
    let mut termination_errors: HashMap<String, Arc<worker::TerminationError>> = HashMap::new();
    for runtime_child in runtime_children {
        let child_runtime_name = runtime_child.runtime_name.to_owned();
        match runtime_child.terminate().await {
            Err((failed_child_spec, termination_err)) => {
                println!("(1) DEBUG -- worker failed");
                let termination_err = Arc::new(termination_err);
                ev_notifier
                    .worker_termination_failed(child_runtime_name.clone(), termination_err.clone())
                    .await;
                termination_errors.insert(child_runtime_name, termination_err);
                spec_children.push(failed_child_spec);
            }
            Ok(failed_child_spec) => {
                println!("(2) DEBUG -- worker terminated");
                ev_notifier.worker_terminated(child_runtime_name).await;
                spec_children.push(failed_child_spec);
            }
        }
    }
    if termination_errors.is_empty() {
        (spec_children, Ok(()))
    } else {
        (spec_children, Err(TerminationError(termination_errors)))
    }
}

/// terminate_supervisor_monitor triggers the termination of children, and sends
/// a notification to the client API that spawned the supervisor
async fn terminate_supervisor_monitor(
    meta: SpecMeta,
    sup_runtime_name: String,
    sup_runtime_children: Vec<Worker>,
) -> SupervisorResult {
    // initialize event notifier in this function
    let mut ev_notifier = meta.ev_notifier.clone();

    // sort runtime children using the spec ordering (LeftToRight, RightToLeft)
    let runtime_children = meta
        .order
        .sort(Lifecycle::Termination, sup_runtime_children);

    let (children_spec, err) = terminate_children(&mut ev_notifier, runtime_children).await;

    // build back Spec to return it to caller
    let spec = Spec {
        meta,
        children: children_spec,
    };

    match err {
        Ok(()) => {
            ev_notifier.supervisor_terminated(sup_runtime_name).await;
            SupervisorResult::OkTermination { spec }
        }
        Err(termination_err) => {
            let err = Arc::new(termination_err);
            ev_notifier
                .supervisor_termination_failed(sup_runtime_name, err.clone())
                .await;
            SupervisorResult::ErrTermination { spec, err }
        }
    }
}

/// Executes the monitoring logic of the supervisor. It expectes a
/// start_notifier to signal the supervisor when it has started.
///
/// ### Calling this function on current thread (`start_notifier` is None)
///
/// When this function gets called in sync fashion, is because we are running in
/// a subtree. In this scenario, we want to return the start failures to signal a
/// failure to our parent supervisor.
///
/// ### Calling this function on new spawned thread (`start_notifier` given)
///
/// When this function gets executed on a spawned task, it is because we are the
/// root supervisor. In this scenario, we want to signal the supervisor that we
/// got started without having to `.await` on the join_handle result from the
/// spawned task.
///
async fn run_supervisor_monitor(
    parent_ctx: context::Context,
    meta: SpecMeta,
    children_spec: Vec<worker::Spec>,
    mstart_notifier: Option<StartNotifier<(), (Spec, Arc<StartError>)>>,
    sup_runtime_name: String,
) -> SupervisorResult {
    let (ctx, _terminate_supervisor) = Context::with_cancel(&parent_ctx);

    // notify is used to keep track of errors from children
    // let (notify_send, notify_recv) = broadcast::channel(children_count);

    let mut ev_notifier = meta.ev_notifier.clone();

    let result = start_children(
        &ctx,
        meta.ev_notifier.clone(),
        &sup_runtime_name,
        meta.order,
        children_spec,
    )
    .await;

    match result {
        // notify supervisor start failure
        Err((spec_children, err)) => {
            let err = Arc::new(err);
            ev_notifier
                .supervisor_start_failed(sup_runtime_name, err.clone())
                .await;
            let spec = Spec {
                meta,
                children: spec_children,
            };
            match mstart_notifier {
                Some(start_notifier) => {
                    // if we call this function inside a `task::spawn`, the
                    // spawner should receive back the spec and error
                    start_notifier.failed((spec, err));
                    // this implementation was forced by the compiler, start_result cannot
                    // belong to both a spawner and a caller, rust compile, thank you <3
                    return SupervisorResult::ErrStartHandled;
                }
                None => {
                    // if we call this function on the same thread, the function
                    // caller should receive back the spec and error
                    return SupervisorResult::ErrStart { spec, err };
                }
            }
        }
        // notify supervisor of start success
        Ok(sup_runtime_children) => {
            // here we signal that the start was successful, which will unblock
            // the bootstrap logic to spawn the next sibling
            ev_notifier.supervisor_started(&sup_runtime_name).await;
            if let Some(start_notifier) = mstart_notifier {
                start_notifier.success(());
            }

            // after startup, we initialize the monitor loop where we listen
            // to children errors or public API calls
            loop {
                tokio::select! {
                    // if our context is done, it means the termination_handle
                    // got called; signaling a shutdown of the whole supervision
                    // tree.
                    _ = ctx.done.clone() => {
                        let supervisor_result =
                            terminate_supervisor_monitor(
                                meta,
                                sup_runtime_name,
                                sup_runtime_children,
                            ).await;

                        // Ok means we did not fail to start, but
                        // termination_result contains a Result that indicates
                        // if we failed to terminate or not
                        return supervisor_result;
                    },
                    // TODO: monotiring children
                    // ch_notification = notify_recv.receive() => {
                    //
                    // }
                    // TODO: control cmd listening
                    // ctrl_msg = ctrl_recv.receive() => {
                    //
                    // }
                }
            }
        }
    }
}

impl Spec {
    pub fn new(
        name: impl Into<String>,
        children: Vec<worker::Spec>,
        ev_notifier: EventNotifier,
    ) -> Spec {
        let name = name.into();
        Spec {
            meta: SpecMeta {
                ev_notifier,
                name,
                order: Order::LeftToRight,
                strategy: Strategy::OneForOne,
            },
            children,
        }
    }

    /// run executes the supervisor monitoring logic on the existing task. This function:
    ///
    /// 1. spawns a new routine for the supervision loop
    ///
    /// 2. spawns each node routine in the correct order
    ///
    /// 3. stops all the spawned nodes in the correct order once it gets a
    /// stop signal
    ///
    /// 4. it monitors and reacts to errors reported by the supervised nodes
    ///
    async fn run(self, parent_ctx: context::Context, parent_name: &str) -> SupervisorResult {
        let Spec { meta, children } = self;
        let sup_runtime_name = format!("{}/{}", parent_name, meta.name);
        run_supervisor_monitor(parent_ctx.clone(), meta, children, None, sup_runtime_name).await
    }

    /// start is future that contains the main logic of a Supervisor. This
    /// function:
    ///
    /// 1. spawns a new routine for the supervision loop
    ///
    /// 2. spawns each node routine in the correct order
    ///
    /// 3. stops all the spawned nodes in the correct order once it gets a
    /// stop signal
    ///
    /// 4. it monitors and reacts to errors reported by the supervised nodes
    ///
    pub async fn start(
        self,
        parent_ctx: &context::Context,
    ) -> Result<Supervisor, (Spec, Arc<StartError>)> {
        // start can only be called from a root supervisor, ergo, parent_name is blank
        let parent_name = "".to_owned();

        // build sup_runtime_name for tracing purposes
        let sup_runtime_name = format!("{}/{}", parent_name, self.meta.name);

        // cancel_fn is used when Supervisor#terminate() is called
        let (ctx, termination_handle) = parent_ctx.with_cancel();

        // // minimum buffer size is 10, otherwise use the number of children
        // let children_count = std::cmp::min(10, self.children.len());

        // TODO
        // // notify is used to listen to errors on child nodes
        // let (notify_send, notify_recv) = broadcast::channel(children_count);

        // TODO
        // // ctrl is used to keep track of requests from client APIs (e.g. spawn child)
        // let (ctrl_send, ctrl_recv) = broadcast::channel(children_count);

        // started is used when waiting for the worker to signal it has started
        let (started_send, started_recv) = oneshot::channel();
        let start_notifier = StartNotifier::from_oneshot(started_send);

        let spec_children = self.children;

        // clone all variables that we are going to move to the monitor loop routine
        let meta = self.meta.clone();
        let sup_runtime_name1 = sup_runtime_name.clone();

        let join_handle = task::spawn(run_supervisor_monitor(
            ctx,
            meta,
            spec_children,
            Some(start_notifier),
            sup_runtime_name1,
        ));

        // We wait for the supervisor to get started, and from there we return a
        // Supervisor record to the client, which can terminate the supervisor,
        // or wait for it to finish
        match started_recv.await {
            // there is no way the monitor loop is going to drop the start
            // notifier before exiting, if we reach this branch, is an
            // implementation error
            Err(_recv_err) => unreachable!(),
            Ok(Err(start_err)) => Err(start_err),
            Ok(Ok(_)) => {
                let meta = self.meta;
                Ok(Supervisor {
                    meta,
                    runtime_name: sup_runtime_name.clone(),
                    join_handle,
                    termination_handle,
                })
            }
        }
    }
}

impl Supervisor {
    /// terminate executes the shutdown of all the supervision tree. In case
    /// there is an error on the termination, the second value on the returned
    /// tuple is going to contain the cause of the error
    pub async fn terminate(self) -> (Spec, Result<(), Arc<TerminationError>>) {
        let Supervisor {
            runtime_name,
            meta,
            join_handle,
            termination_handle,
        } = self;

        // signal to the supervisor monitor is time to shutdown
        termination_handle.abort();

        // wait for the result of the termination procedure
        let termination_result0 = join_handle.await;

        match termination_result0 {
            Err(join_handle_err) => {
                eprintln!("{:?}", join_handle_err);
                panic!("supervisor monitor loop had a join_handle error; invalid implementation")
            }

            // ErrStart means we got an start error, this has been dealt
            // with, and so it must never happen
            Ok(SupervisorResult::ErrStartHandled) => unreachable!(),
            Ok(SupervisorResult::ErrStart { .. }) => unreachable!(),

            // we perform a transformation to more generic types so that clients don't
            // get coupled with out enums
            Ok(SupervisorResult::OkTermination { spec }) => (spec, Ok(())),
            Ok(SupervisorResult::ErrTermination { spec, err }) => (spec, Err(err)),
        }
    }
}
