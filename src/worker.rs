use std::sync::Arc;
use std::time::Duration;

use futures::future::{AbortHandle, Abortable, Aborted, BoxFuture, Future, FutureExt};

use chrono::{DateTime, Utc};
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};
use tokio::time;

use crate::context::{self, Context};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Restart {
    Permanent,
    Transient,
    Temporary,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Shutdown {
    Indefinitely,
    Timeout(Duration),
}

pub struct StartNotifier(Box<dyn FnOnce(Option<anyhow::Error>) + Send>);

impl StartNotifier {
    pub fn from_oneshot(sender: oneshot::Sender<Option<anyhow::Error>>) -> StartNotifier {
        StartNotifier(Box::new(move |err| {
            let _ = sender.send(err);
        }))
    }

    fn call(self, err: Option<anyhow::Error>) {
        self.0(err)
    }

    pub fn success(self) {
        self.call(None)
    }

    pub fn failed(self, err: anyhow::Error) {
        self.call(Some(err))
    }
}

pub struct Spec {
    pub name: String,
    pub restart: Restart,
    pub shutdown: Shutdown,
    routine:
        Box<dyn FnMut(Context, StartNotifier) -> BoxFuture<'static, Result<(), anyhow::Error>>>,
}

pub struct Worker {
    spec: Spec,
    pub runtime_name: String,
    pub created_at: DateTime<Utc>,
    pub join_handle: JoinHandle<Result<anyhow::Result<()>, Aborted>>,
    cancel_handle: context::CancelHandle,
    abort_handle: AbortHandle,
}

impl Spec {
    pub fn new<F, O>(name: &str, mut routine0: F) -> Spec
    where
        F: FnMut(Context) -> O + 'static,
        O: Future<Output = anyhow::Result<()>> + FutureExt + Send + Sized + 'static,
    {
        let routine1 = move |ctx: Context, on_start: StartNotifier| {
            on_start.call(None);
            routine0(ctx).boxed()
        };
        Spec {
            name: name.to_owned(),
            shutdown: Shutdown::Indefinitely,
            restart: Restart::Permanent,
            routine: Box::new(routine1),
        }
    }

    pub fn new_with_start<F, O>(name: &str, mut routine0: F) -> Spec
    where
        F: FnMut(Context, StartNotifier) -> O + 'static,
        O: Future<Output = anyhow::Result<()>> + FutureExt + Send + Sized + 'static,
    {
        let routine1 = move |ctx: Context, on_start: StartNotifier| routine0(ctx, on_start).boxed();
        Spec {
            name: name.to_owned(),
            shutdown: Shutdown::Indefinitely,
            restart: Restart::Permanent,
            routine: Box::new(routine1),
        }
    }

    pub async fn start(
        mut self,
        parent_ctx: &Context,
        parent_name: &str,
    ) -> anyhow::Result<Worker> {
        let runtime_name = format!("{}/{}", parent_name, self.name);
        let created_at = Utc::now();

        // ON_START setup
        let (started_tx, started_rx) = oneshot::channel::<Option<anyhow::Error>>();
        let start_notifier = StartNotifier::from_oneshot(started_tx);

        // CANCEL setup
        let (ctx, cancel_handle) = Context::with_cancel(parent_ctx);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let future = (*self.routine)(ctx, start_notifier);
        let routine = Abortable::new(future, abort_registration);

        // SPAWN -- actually spawn concurrent worker future
        let join_handle = task::spawn(routine);

        // ON_START blocking -- block before moving to the next start
        let err = started_rx.await;

        match err {
            // On worker initialization failed, we need to signal
            // this to starter
            Err(err) => Err(anyhow::Error::new(err)),
            Ok(Some(err)) => Err(err),

            // Happy path
            Ok(None) => Ok(Worker {
                spec: self,
                runtime_name,
                created_at,
                join_handle,
                cancel_handle,
                abort_handle,
            }),
        }
    }
}

impl Worker {
    pub async fn terminate(self) -> Result<Spec, Arc<anyhow::Error>> {
        self.cancel_handle.cancel();

        let mut delay = time::delay_for(Duration::from_millis(1000));
        tokio::select! {
           _ = &mut delay => {
               self.abort_handle.abort();
               return Ok(self.spec)
           },
           result = self.join_handle => {
               match result {
                   Ok(Ok(_)) => return Ok(self.spec),
                   Ok(Err(_future_err)) => {
                       // TODO:
                       // * deal with future error
                       return Ok(self.spec);
                   },
                   Err(_abort_err) => {
                       // TODO:
                       // * deal with abort error
                       return Ok(self.spec);
                   },
               }
           }
        }
    }
}
