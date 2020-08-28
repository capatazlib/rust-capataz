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

impl Spec {
    pub fn new(name: &str, children: Vec<worker::Spec>, ev_notifier: EventNotifier) -> Spec {
        Spec {
            name: name.to_owned(),
            ev_notifier: ev_notifier,
            order: Order::LeftToRight,
            strategy: Strategy::OneForOne,
            children,
        }
    }

    async fn start_children<'a>(
        self,
        ctx: &context::Context,
        sup_name: &'a str,
    ) -> Result<Vec<Worker>, Arc<anyhow::Error>> {
        let mut ev_notifier = self.ev_notifier.clone();
        let mut children = Vec::with_capacity(self.children.len());

        // ORDERING
        let children_spec = match &self.order {
            Order::LeftToRight => {
                Box::new(self.children.into_iter()) as Box<dyn Iterator<Item = worker::Spec>>
            }
            Order::RightToLeft => {
                Box::new(self.children.into_iter().rev()) as Box<dyn Iterator<Item = worker::Spec>>
            }
        };

        // START EACH (WORKER) SPEC
        for ch_spec in children_spec {
            let ch_name = ch_spec.name.clone();
            let result = ch_spec.start(ctx, &sup_name).await;
            match result {
                // TODO:
                // * Enhance error with context from supervisor start
                // * Rollback/Stop started siblings
                Err(err0) => {
                    let err = Arc::new(err0);
                    ev_notifier
                        .worker_start_failed(&format!("{}/{}", sup_name, ch_name), err.clone())
                        .await;
                    return Err(err);
                }
                Ok(ch) => {
                    ev_notifier.worker_started(&ch.runtime_name).await;
                    children.push(ch);
                }
            }
        }

        Ok(children)
    }

    async fn start_supervisor(
        self,
        parent_ctx: &context::Context,
        parent_name: &'static str,
    ) -> Result<Supervisor, Arc<anyhow::Error>> {
        let (ctx, _terminate_supervisor) = Context::with_cancel(parent_ctx);

        let name = self.name.clone();
        let mut ev_notifier = self.ev_notifier.clone();
        let order = self.order.clone();
        let strategy = self.strategy.clone();

        let runtime_name = format!("{}/{}", parent_name, name);
        let result = self.start_children(&ctx, &runtime_name).await;

        match result {
            Err(err) => {
                ev_notifier
                    .supervisor_start_failed(&runtime_name, err.clone())
                    .await;
                return Err(err);
            }
            Ok(children) => {
                ev_notifier.supervisor_started(&runtime_name).await;
                Ok(Supervisor {
                    runtime_name,
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
    ) -> Result<Supervisor, Arc<anyhow::Error>> {
        let root_name = "";
        self.start_supervisor(parent_ctx, root_name).await
    }

    // pub async fn stop(
    //     self,

    // ) -> Result<Spec, Arc<anyhow::Error>> {

    // }
}

impl Supervisor {
    pub async fn terminate(self) -> Result<Spec, Arc<anyhow::Error>> {
        let runtime_name = self.runtime_name.clone();
        let name = self.name.clone();
        let mut ev_notifier = self.ev_notifier.clone();
        let order = self.order.clone();
        let strategy = self.strategy.clone();
        let mut children_spec = Vec::with_capacity(self.children.len());

        // ORDERING
        let children = match &self.order {
            Order::LeftToRight => {
                Box::new(self.children.into_iter().rev()) as Box<dyn Iterator<Item = Worker>>
            }
            Order::RightToLeft => {
                Box::new(self.children.into_iter()) as Box<dyn Iterator<Item = Worker>>
            }
        };

        // STOP EACH WORKER
        let mut stop_err = None;
        for ch in children {
            let ch_name = ch.runtime_name.clone();
            let result = ch.terminate().await;
            match result {
                Err(err) => {
                    ev_notifier
                        .worker_termination_failed(&ch_name, err.clone())
                        .await;
                    stop_err = Some(err);
                    // TODO:
                    // * Accumulate errors and return StopError
                    continue;
                }
                Ok(spec) => {
                    ev_notifier.worker_terminated(&ch_name).await;
                    children_spec.push(spec);
                }
            }
        }

        match stop_err {
            None => {
                ev_notifier.supervisor_terminated(&runtime_name).await;
                Ok(Spec {
                    name,
                    ev_notifier,
                    order,
                    strategy,
                    children: children_spec,
                })
            }
            Some(err) => Err(err),
        }
    }
}
