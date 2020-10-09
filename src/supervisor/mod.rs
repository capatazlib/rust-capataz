#[cfg(test)]
pub(crate) mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use crate::context::{self, Context};
use crate::events::EventNotifier;
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
    fn sort<A: 'static>(
        &self,
        lifecycle: Lifecycle,
        children: Vec<A>,
    ) -> Box<dyn DoubleEndedIterator<Item = A>> {
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

pub struct Supervisor {
    pub runtime_name: String,
    pub children: Vec<Worker>,

    pub meta: SpecMeta,
}

// terminate_prev_siblings gets called when a child in a supervision tree fails
// to start, and all it's previous siblings need to get terminated.
async fn terminate_prev_siblings<I: DoubleEndedIterator<Item = worker::Spec>>(
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
    // TODO: receive error notifier parameter
    children_specs: Vec<worker::Spec>,
) -> Result<Vec<Worker>, (Vec<worker::Spec>, StartError)> {
    // let mut start_result = StartResult::new(children_specs.len());

    let mut started_children = Vec::with_capacity(children_specs.len());
    let mut pending_children = sup_order.sort(Lifecycle::Start, children_specs);
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
    let mut children_specs = Vec::new();
    let mut termination_errors: HashMap<String, Arc<worker::TerminationError>> = HashMap::new();
    for runtime_child in runtime_children {
        let child_runtime_name = runtime_child.runtime_name.to_owned();
        match runtime_child.terminate().await {
            Err((failed_child_spec, termination_err)) => {
                let termination_err = Arc::new(termination_err);
                ev_notifier
                    .worker_termination_failed(child_runtime_name.clone(), termination_err.clone())
                    .await;
                termination_errors.insert(child_runtime_name, termination_err);
                children_specs.push(failed_child_spec);
            }
            Ok(failed_child_spec) => {
                ev_notifier.worker_terminated(child_runtime_name).await;
                children_specs.push(failed_child_spec);
            }
        }
    }
    if termination_errors.is_empty() {
        (children_specs, Ok(()))
    } else {
        (children_specs, Err(TerminationError(termination_errors)))
    }
}

impl Spec {
    pub fn new(
        name: impl Into<String>,
        children: Vec<worker::Spec>,
        ev_notifier: EventNotifier,
    ) -> Spec {
        Spec {
            meta: SpecMeta {
                name: name.into(),
                ev_notifier,
                order: Order::LeftToRight,
                strategy: Strategy::OneForOne,
            },
            children,
        }
    }

    // TODO: once we start with the monitoring logic, change this method to
    // run_supervisor
    async fn start_supervisor<'a>(
        self,
        parent_ctx: &context::Context,
        parent_name: &'a str,
    ) -> Result<Supervisor, (Spec, Arc<StartError>)> {
        let (ctx, _terminate_supervisor) = Context::with_cancel(parent_ctx);

        let mut ev_notifier = self.meta.ev_notifier.clone();
        let meta = self.meta;

        let sup_runtime_name = format!("{}/{}", parent_name, meta.name);
        let result = start_children(
            &ctx,
            meta.ev_notifier.clone(),
            &sup_runtime_name,
            meta.order,
            self.children,
        )
        .await;

        // TODO: monitoring should start here
        match result {
            Err((children_specs, err)) => {
                let err = Arc::new(err);
                ev_notifier
                    .supervisor_start_failed(&sup_runtime_name, err.clone())
                    .await;
                let spec = Spec {
                    meta,
                    children: children_specs,
                };
                return Err((spec, err));
            }
            Ok(children) => {
                ev_notifier.supervisor_started(&sup_runtime_name).await;
                Ok(Supervisor {
                    runtime_name: sup_runtime_name,
                    children,
                    meta,
                })
            }
        }
    }

    pub async fn start(
        self,
        parent_ctx: &context::Context,
    ) -> Result<Supervisor, (Spec, Arc<StartError>)> {
        let root_name = "";
        // TODO: Create Supervisor record here, with JoinHandle that would stop
        // execution of future run_supervisor
        self.start_supervisor(parent_ctx, root_name).await
    }
}

impl Supervisor {
    pub async fn terminate(self) -> (Spec, Result<(), Arc<TerminationError>>) {
        let Supervisor {
            runtime_name,
            meta,
            children,
        } = self;
        let mut ev_notifier = meta.ev_notifier.clone();
        let runtime_children = meta.order.sort(Lifecycle::Termination, children);

        let (children_spec, result) = terminate_children(&mut ev_notifier, runtime_children).await;

        let spec = Spec {
            meta,
            children: children_spec,
        };

        match result {
            Ok(_) => {
                ev_notifier.supervisor_terminated(runtime_name).await;
                (spec, Ok(()))
            }
            Err(err0) => {
                let err = Arc::new(err0);
                ev_notifier
                    .supervisor_termination_failed(runtime_name, err.clone())
                    .await;
                (spec, Err(err))
            }
        }
    }
}