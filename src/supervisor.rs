use std::collections::HashMap;
use std::sync::Arc;

use crate::context::{self, Context};
use crate::events::EventNotifier;
use crate::worker::{self, Worker};

#[derive(Clone, PartialEq, Debug)]
pub enum Order {
    LeftToRight,
    RightToLeft,
}

#[derive(Clone, PartialEq, Debug)]
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

pub struct Spec {
    pub name: String,
    pub ev_notifier: EventNotifier,
    pub order: Order,
    pub strategy: Strategy,
    pub children: Vec<worker::Spec>,
}

pub struct Supervisor {
    pub runtime_name: String,
    pub children: Vec<Worker>,

    pub name: String,
    pub ev_notifier: EventNotifier,
    pub order: Order,
    pub strategy: Strategy,
}

fn ordered_children_on_start(
    order: &Order,
    children_specs: Vec<worker::Spec>,
) -> Box<dyn Iterator<Item = worker::Spec>> {
    match order {
        Order::LeftToRight => Box::new(children_specs.into_iter()),
        Order::RightToLeft => Box::new(children_specs.into_iter().rev()),
    }
}

fn ordered_children_on_termination(
    order: &Order,
    children: Vec<Worker>,
) -> Box<dyn Iterator<Item = Worker>> {
    match order {
        Order::LeftToRight => Box::new(children.into_iter().rev()),
        Order::RightToLeft => Box::new(children.into_iter()),
    }
}

// terminate_prev_siblings gets called when a child in a supervision tree fails
// to start, and all it's previous siblings need to get terminated.
async fn terminate_prev_siblings<'a, 'b, 'c>(
    ev_notifier: &mut EventNotifier,
    failed_child_spec: worker::Spec,
    failed_child_start_err: Arc<worker::StartError>,
    pending_children_specs: Vec<worker::Spec>,
    started_children: Vec<Worker>,
) -> (Vec<worker::Spec>, StartError) {
    let prev_sibling_termination_result =
        terminate_children(ev_notifier, started_children.into_iter().rev()).await;

    let (prev_specs, failing_sibling_termination_error) = match prev_sibling_termination_result {
        Err((prev_specs, sibling_err)) => (prev_specs, Some(sibling_err)),
        Ok(prev_specs) => (prev_specs, None),
    };

    let start_err = StartError {
        failing_child_runtime_name: "".to_owned(),
        failing_child_error: failed_child_start_err,
        failing_sibling_termination_error,
    };

    // regenerate the original vector of worker
    // specs so that the parent supervisor can try
    // again if it is able to do it
    let children_spec: Vec<worker::Spec> = prev_specs
        .into_iter()
        .chain(std::iter::once(failed_child_spec))
        .chain(pending_children_specs.into_iter())
        .collect();

    (children_spec, start_err)
}

// StartResult is an internal state machine used when starting workers in a
// supervisor
enum StartResult {
    SuccessfulStart {
        started_workers: Vec<Worker>,
        pending_count: usize,
    },
    FailedStart {
        prev_started_workers: Vec<Worker>,
        failed_worker_spec: worker::Spec,
        start_error: Arc<worker::StartError>,
        pending_workers: Vec<worker::Spec>,
    },
}

impl StartResult {
    fn new(pending_count: usize) -> Self {
        StartResult::SuccessfulStart {
            started_workers: Vec::with_capacity(pending_count),
            pending_count,
        }
    }

    async fn start_child(
        self,
        ctx: &Context,
        mut ev_notifier: EventNotifier,
        sup_runtime_name: &str,
        child_spec: worker::Spec,
    ) -> Self {
        match self {
            StartResult::SuccessfulStart {
                mut started_workers,
                pending_count,
            } => {
                match child_spec.start(ctx, sup_runtime_name).await {
                    // when the child_spec starts without hiccups
                    Ok(started_worker) => {
                        // send event notification indicating that the worker was started
                        ev_notifier
                            .worker_started(&started_worker.runtime_name)
                            .await;
                        // append the worker to the started workers vector
                        started_workers.push(started_worker);
                        // keep pending_count up to date
                        let pending_count: usize = pending_count - 1;
                        //
                        StartResult::SuccessfulStart {
                            started_workers,
                            pending_count,
                        }
                    }
                    // when the child_spec failed to start
                    Err((failed_worker_spec, start_error0)) => {
                        let child_runtime_name =
                            format!("{}/{}", sup_runtime_name, failed_worker_spec.name);
                        let start_error = Arc::new(start_error0);
                        // send event notification indicating that the worker failed to start
                        ev_notifier
                            .worker_start_failed(child_runtime_name, start_error.clone())
                            .await;
                        // transform current start to FailedStart
                        StartResult::FailedStart {
                            failed_worker_spec,
                            start_error,
                            // store the already started workers so that we can
                            // later terminate them
                            prev_started_workers: started_workers,
                            // use the updated pending_count entry to get the
                            // remaining pending_workers vec size
                            pending_workers: Vec::with_capacity(pending_count),
                        }
                    }
                }
            }
            // If we already have a failed start, we just append entries to the
            // pending_workers vec
            StartResult::FailedStart {
                failed_worker_spec,
                start_error,
                prev_started_workers,
                mut pending_workers,
            } => {
                pending_workers.push(child_spec);
                StartResult::FailedStart {
                    failed_worker_spec,
                    start_error,
                    prev_started_workers,
                    pending_workers,
                }
            }
        }
    }
}

async fn start_children<'a, 'b>(
    ctx: &'a Context,
    mut ev_notifier: EventNotifier,
    sup_runtime_name: &str,
    sup_order: &'b Order,
    // TODO: receive error notifier parameter
    children_specs0: Vec<worker::Spec>,
) -> Result<Vec<Worker>, (Vec<worker::Spec>, StartError)> {
    let mut start_result = StartResult::new(children_specs0.len());
    let pending_children: Box<dyn Iterator<Item = worker::Spec>> =
        ordered_children_on_start(sup_order, children_specs0);

    for failed_child_spec in pending_children {
        start_result = start_result
            .start_child(
                ctx,
                ev_notifier.clone(),
                sup_runtime_name,
                failed_child_spec,
            )
            .await;
    }

    match start_result {
        StartResult::SuccessfulStart {
            started_workers, ..
        } => Ok(started_workers),
        StartResult::FailedStart {
            failed_worker_spec,
            start_error,
            prev_started_workers,
            pending_workers,
        } => {
            let termination_result = terminate_prev_siblings(
                &mut ev_notifier,
                failed_worker_spec,
                start_error,
                pending_workers,
                prev_started_workers,
            )
            .await;
            Err(termination_result)
        }
    }
}

// terminate_children terminates each worker::Worker record. In case any of the
// children fail to terminate a supervisor::TerminationError is returned.
//
async fn terminate_children<'a>(
    ev_notifier: &mut EventNotifier,
    runtime_children: impl Iterator<Item = Worker>,
) -> Result<Vec<worker::Spec>, (Vec<worker::Spec>, TerminationError)> {
    let mut children_specs = Vec::new();
    let mut termination_errors: HashMap<String, Arc<worker::TerminationError>> = HashMap::new();
    for runtime_child in runtime_children {
        let child_runtime_name = runtime_child.runtime_name.to_owned();
        match runtime_child.terminate().await {
            Err((failed_child_spec, termination_err0)) => {
                let termination_err = Arc::new(termination_err0);
                ev_notifier
                    .worker_termination_failed(child_runtime_name.clone(), termination_err.clone())
                    .await;
                termination_errors.insert(child_runtime_name, termination_err);
                children_specs.push(failed_child_spec);
            }
            Ok(failed_child_spec) => {
                ev_notifier
                    .worker_terminated(child_runtime_name.clone())
                    .await;
                children_specs.push(failed_child_spec);
            }
        }
    }
    if termination_errors.is_empty() {
        Ok(children_specs)
    } else {
        Err((children_specs, TerminationError(termination_errors)))
    }
}

impl Spec {
    pub fn new(
        name: impl Into<String>,
        children: Vec<worker::Spec>,
        ev_notifier: EventNotifier,
    ) -> Spec {
        Spec {
            name: name.into(),
            ev_notifier,
            children,

            order: Order::LeftToRight,
            strategy: Strategy::OneForOne,
        }
    }

    async fn start_supervisor<'a>(
        self,
        parent_ctx: &context::Context,
        parent_name: &'a str,
    ) -> Result<Supervisor, (Spec, Arc<StartError>)> {
        let (ctx, _terminate_supervisor) = Context::with_cancel(parent_ctx);

        let name = self.name.clone();
        let mut ev_notifier = self.ev_notifier.clone();
        let order = self.order.clone();
        let strategy = self.strategy.clone();

        let sup_runtime_name = format!("{}/{}", parent_name, name);
        let result = start_children(
            &ctx,
            ev_notifier.clone(),
            &sup_runtime_name,
            &order,
            self.children,
        )
        .await;

        match result {
            Err((children_specs, err)) => {
                let err = Arc::new(err);
                ev_notifier
                    .supervisor_start_failed(&sup_runtime_name, err.clone())
                    .await;
                let spec = Spec {
                    name,
                    ev_notifier,
                    order,
                    strategy,
                    children: children_specs,
                };
                return Err((spec, err));
            }
            Ok(children) => {
                ev_notifier.supervisor_started(&sup_runtime_name).await;
                Ok(Supervisor {
                    runtime_name: sup_runtime_name.to_owned(),
                    children,

                    name,
                    ev_notifier,
                    order,
                    strategy,
                })
            }
        }
    }

    pub async fn start(
        self,
        parent_ctx: &context::Context,
    ) -> Result<Supervisor, (Spec, Arc<StartError>)> {
        let root_name = "";
        self.start_supervisor(parent_ctx, root_name).await
    }
}

impl Supervisor {
    pub async fn terminate(self) -> Result<Spec, (Spec, Arc<TerminationError>)> {
        let Supervisor { 
            name, 
            runtime_name, 
            order, strategy, 
            mut ev_notifier, 
            children 
        } = self;
        let runtime_children = ordered_children_on_termination(&order, children);

        let result = terminate_children(&mut ev_notifier, runtime_children).await;
        match result {
            Ok(children_spec) => {
                ev_notifier.supervisor_terminated(runtime_name).await;
                Ok(Spec {
                    name,
                    order,
                    ev_notifier,
                    strategy,
                    children: children_spec,
                })
            }
            Err((children_spec, err0)) => {
                let err = Arc::new(err0);
                ev_notifier
                    .supervisor_termination_failed(runtime_name, err.clone())
                    .await;
                let spec = Spec {
                    name,
                    order,
                    ev_notifier,
                    strategy,
                    children: children_spec,
                };
                Err((spec, err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time;

    use crate::context::*;
    use crate::events::{
        supervisor_started, testing_event_notifier, worker_start_failed, worker_started,
        worker_terminated, worker_termination_failed,
    };
    use crate::supervisor;
    use crate::worker::tests::{
        start_err_worker, start_timeout_worker, termination_failed_worker, wait_done_worker,
    };

    #[tokio::test]
    async fn test_simple_start_and_stop_with_one_child() {
        let (event_notifier, event_buffer) = testing_event_notifier().await;
        let spec = supervisor::Spec::new("root", vec![wait_done_worker("one")], event_notifier);

        let ctx = Context::new();

        let start_result = spec.start(&ctx).await;
        let sup = start_result
            .map_err(|err| err.1)
            .expect("successful supervisor start");

        let terminate_result = sup.terminate().await;
        let _ = terminate_result
            .map_err(|err| err.1)
            .expect("successful supervisor termination");

        event_buffer
            .assert_exact(vec![
                worker_started("/root/one"),
                supervisor_started("/root"),
                worker_terminated("/root/one"),
            ])
            .await;
    }

    #[tokio::test]
    async fn test_simple_start_and_stop_with_multiple_children() {
        let (event_notifier, event_buffer) = testing_event_notifier().await;
        let spec = supervisor::Spec::new(
            "root",
            vec![
                wait_done_worker("one"),
                wait_done_worker("two"),
                wait_done_worker("three"),
            ],
            event_notifier,
        );

        let ctx = Context::new();

        let start_result = spec.start(&ctx).await;
        let sup = start_result
            .map_err(|err| err.1)
            .expect("successful supervisor start");

        let terminate_result = sup.terminate().await;
        let _spec = terminate_result
            .map_err(|err| err.1)
            .expect("successful supervisor termination");

        event_buffer
            .assert_exact(vec![
                worker_started("/root/one"),
                worker_started("/root/two"),
                worker_started("/root/three"),
                supervisor_started("/root"),
                worker_terminated("/root/three"),
                worker_terminated("/root/two"),
                worker_terminated("/root/one"),
            ])
            .await;
    }

    #[tokio::test]
    async fn test_failing_start_with_multiple_children() {
        let (event_notifier, event_buffer) = testing_event_notifier().await;
        let spec = supervisor::Spec::new(
            "root",
            vec![
                wait_done_worker("one"),
                start_err_worker("two", "boom"),
                wait_done_worker("three"),
            ],
            event_notifier,
        );

        let ctx = Context::new();

        let start_result = spec.start(&ctx).await;
        let start_err = start_result
            .map(|_| ())
            .map_err(|err| err.1)
            .expect_err("supervisor start must fail");

        assert!(start_err.failing_child_error.is_worker_init_error());

        event_buffer
            .assert_exact(vec![
                worker_started("/root/one"),
                worker_start_failed("/root/two"),
                worker_terminated("/root/one"),
                // supervisor_start_failed("/root"),
            ])
            .await;
    }

    #[tokio::test]
    async fn test_timeout_start_with_multiple_children() {
        let (event_notifier, event_buffer) = testing_event_notifier().await;
        let spec = supervisor::Spec::new(
            "root",
            vec![
                wait_done_worker("one"),
                wait_done_worker("two"),
                start_timeout_worker(
                    "three",
                    Duration::from_secs(1), // timeout
                    Duration::from_secs(3), // delay
                ),
            ],
            event_notifier,
        );

        let ctx = Context::new();

        time::pause();

        let start_fut = spec.start(&ctx);

        time::advance(Duration::from_secs(4)).await;

        let start_result = start_fut.await;
        let start_err = start_result
            .map(|_| ())
            .map_err(|err| err.1)
            .expect_err("supervisor start must fail");

        assert!(start_err.failing_child_error.is_timeout_error());

        event_buffer
            .assert_exact(vec![
                worker_started("/root/one"),
                worker_started("/root/two"),
                worker_start_failed("/root/three"),
                worker_terminated("/root/two"),
                worker_terminated("/root/one"),
                // supervisor_start_failed("/root"),
            ])
            .await;
    }

    #[tokio::test]
    async fn test_start_timeout_and_termination_timeout_with_multiple_children() {
        let (event_notifier, event_buffer) = testing_event_notifier().await;
        let spec = supervisor::Spec::new(
            "root",
            vec![
                wait_done_worker("one"),
                termination_failed_worker("two"),
                start_timeout_worker(
                    "three",
                    Duration::from_secs(1), // timeout
                    Duration::from_secs(3), // delay
                ),
            ],
            event_notifier,
        );

        let ctx = Context::new();

        time::pause();

        let start_fut = spec.start(&ctx);

        time::advance(Duration::from_secs(4)).await;

        let start_result = start_fut.await;
        let start_err = start_result
            .map(|_| ())
            .map_err(|err| err.1)
            .expect_err("supervisor start must fail");

        event_buffer
            .assert_exact(vec![
                worker_started("/root/one"),
                worker_started("/root/two"),
                worker_start_failed("/root/three"),
                worker_termination_failed("/root/two"),
                worker_terminated("/root/one"),
                // supervisor_start_failed("/root"),
            ])
            .await;

        // there is a timeout error
        assert!(start_err.failing_child_error.is_timeout_error());
        // there is an entry in the termination error
        let mtermination_err = start_err.failing_sibling_termination_error.as_ref();
        match mtermination_err {
            None => panic!("termination error should be present"),
            Some(ref termination_err) => {
                assert!(
                    !termination_err.0.is_empty(),
                    "termination error map must not be empty"
                );
                termination_err
                    .0
                    .get("/root/two")
                    .expect("termination error must be in '/root/two' child");
            }
        }
    }
}
