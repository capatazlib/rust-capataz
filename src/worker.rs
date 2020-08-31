use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::{AbortHandle, Abortable, Aborted, BoxFuture, Future, FutureExt};
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};
use tokio::time;

use crate::context::Context;

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

/// StartNotifier offers a convenient way to notify a worker spawner (a
/// Supervisor in the general case) that the worker got started (or failed) to
/// start.
pub struct StartNotifier(Box<dyn FnOnce(Result<(), anyhow::Error>) + Send>);

impl StartNotifier {
    pub fn from_oneshot(sender: oneshot::Sender<Result<(), anyhow::Error>>) -> Self {
        StartNotifier(Box::new(move |err| {
            let _ = sender.send(err);
        }))
    }

    fn call(self, err: Result<(), anyhow::Error>) {
        self.0(err)
    }

    pub fn success(self) {
        self.call(Ok(()))
    }

    pub fn failed(self, err: anyhow::Error) {
        self.call(Err(err))
    }
}

pub struct Spec {
    name: String,
    restart: Restart,
    shutdown: Shutdown,
    routine:
        Box<dyn FnMut(Context, StartNotifier) -> BoxFuture<'static, Result<(), anyhow::Error>>>,
}

/// Worker represents an routine that got started from a worker::Spec.
/// It contains metadata and also offers an API to provide graceful and
/// forceful termination
pub struct Worker {
    spec: Spec,
    runtime_name: String,
    created_at: DateTime<Utc>,
    join_handle: JoinHandle<Result<anyhow::Result<()>, Aborted>>,
    termination_handle: AbortHandle,
    kill_handle: AbortHandle,
}

impl Spec {
    /// new creates a worker routine. It requires two arguments: a name that is
    /// used for runtime tracing and a start function that returns a routine
    /// that is going to be executed in a Future task concurrently.
    ///
    /// ### The name argument
    ///
    /// A name argument must not be empty nor contain forward slash characters
    ///
    /// ### The start function
    ///
    /// This function is where your business logic should be located. it will be
    /// running on a new supervised routine.
    ///
    /// The start function will receive a Context record that **must** be used
    /// inside your business logic to accept stop signals from the supervisor
    /// that manages this routine life-cycle.
    pub fn new<F, O>(name: &str, mut routine0: F) -> Self
    where
        F: FnMut(Context) -> O + 'static,
        O: Future<Output = anyhow::Result<()>> + FutureExt + Send + Sized + 'static,
    {
        let routine1 = move |ctx: Context, on_start: StartNotifier| {
            on_start.success();
            routine0(ctx).boxed()
        };
        Spec {
            name: name.to_owned(),
            shutdown: Shutdown::Indefinitely,
            restart: Restart::Permanent,
            routine: Box::new(routine1),
        }
    }

    /// new_with_start accomplishes the same goal as new with the addition of
    /// passing an extra argument to the start function, a StartNotifier record.
    ///
    /// ### The StartNotifier argument in the start function
    ///
    /// Sometimes you want to consider a routine started after certain
    /// initialization is done; like doing a read from a Database or API, or
    /// when some socket is bound, etc. The StartNotifier allows the spawned
    /// worker routine to signal when it has initialized.
    ///
    /// It is essential to call the API from the StartNotifier function in your
    /// business logic as soon as you consider the worker is initialized,
    /// otherwise the parent supervisor will block and eventually fail with a
    /// timeout.
    ///
    /// ### Report a start error with the StartNotifier
    ///
    /// If for some reason, a worker is not able to start correctly (e.g. DB
    /// connection fails, network is kaput, etc.), the worker may call the
    /// `StartNotifier#failed` function with the impending error as a parameter.
    /// This will cause the whole supervision system start procedure to abort
    /// and fail fast.
    ///
    pub fn new_with_start<F, O>(name: &str, mut routine0: F) -> Self
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

    /// restart is a builder method that allows to modify the Restart settings
    /// for the spawned worker
    pub fn restart(mut self, restart: Restart) -> Self {
        self.restart = restart;
        self
    }

    /// shutdown is a builder method that allows to modify the Shutdown settings
    /// for the spawned worker
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// start spawns a new routine that executes the start function of this
    /// Spec.
    ///
    /// ### Blocking on start function
    ///
    /// This function will block and wait until a notification from the
    /// start function (executing on a newly spawned routine) is received; this
    /// notification indicates if the start was successful or not.
    ///
    /// This blocking is necessary to ensure that supervised worker trees get
    /// spawned in the specified/expected order.
    pub async fn start(
        mut self,
        parent_ctx: &Context,
        parent_name: &str,
    ) -> anyhow::Result<Worker> {
        let runtime_name = format!("{}/{}", parent_name, self.name);
        let created_at = Utc::now();

        // ON_START setup
        let (started_tx, started_rx) = oneshot::channel::<Result<(), anyhow::Error>>();
        let start_notifier = StartNotifier::from_oneshot(started_tx);

        // CANCEL setup
        let (ctx, termination_handle) = Context::with_cancel(parent_ctx);
        let (kill_handle, kill_registration) = AbortHandle::new_pair();
        let future = (*self.routine)(ctx, start_notifier);
        let routine = Abortable::new(future, kill_registration);

        // SPAWN -- actually spawn concurrent worker future
        let join_handle = task::spawn(routine);

        // ON_START blocking -- block before moving to the next start
        let err = started_rx.await;

        match err {
            // On worker initialization failed, we need to signal
            // this to starter
            Err(err) => Err(anyhow::Error::new(err)),
            Ok(Err(err)) => Err(err),

            // Happy path
            Ok(Ok(_)) => Ok(Worker {
                spec: self,
                runtime_name,
                created_at,
                join_handle,
                termination_handle,
                kill_handle,
            }),
        }
    }
}

impl Worker {
    /// terminate tries to stop gracefuly a worker routine. In the scenario that
    /// the routine doesn't stop after a timeout, it is brutally killed.
    pub async fn terminate(self) -> Result<Spec, Arc<anyhow::Error>> {
        self.termination_handle.abort();

        let mut delay = time::delay_for(Duration::from_millis(1000));
        tokio::select! {
           _ = &mut delay => {
               self.termination_handle.abort();
               return Ok(self.spec)
           },
           result = self.join_handle => {
               match result {
                   Ok(Ok(_)) => return Ok(self.spec),
                   Ok(Err(_future_err)) => {
                       // TODO:
                       // * deal with termination error
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

#[cfg(test)]
mod tests {

    use tokio::sync::mpsc;

    use crate::context::*;
    use crate::worker::{self, StartNotifier};

    fn start_err_worker(name: &str, err_msg: &'static str) -> worker::Spec {
        worker::Spec::new_with_start(name, move |_: Context, start: StartNotifier| async move {
            start.failed(anyhow::Error::msg(err_msg.to_owned()));
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_worker_simple_start() {
        let (before_tx, mut before_rx) = mpsc::channel(1);
        let (after_tx, mut after_rx) = mpsc::channel(1);

        let routine = move |ctx: Context| {
            let mut before_tx = before_tx.clone();
            let mut after_tx = after_tx.clone();
            async move {
                // wait for worker to be done
                let _ = before_tx.send(()).await;
                ctx.done.await;
                let _ = after_tx.send(()).await;
                Ok(())
            }
        };

        let spec = worker::Spec::new("child1", routine);

        let ctx = Context::new();
        let start_result = spec.start(&ctx, "root").await;
        let worker = start_result.expect("successful worker creation");

        before_rx.recv().await;
        let stop_result = worker.terminate().await;
        let _ = stop_result.expect("successful worker termination");
        after_rx.recv().await;
    }

    #[tokio::test]
    async fn test_worker_start_success() {
        let (after_tx, mut after_rx) = mpsc::channel(1);

        let routine = move |ctx: Context, start: StartNotifier| {
            let mut after_tx = after_tx.clone();
            async move {
                // wait for worker to be done
                start.success();
                ctx.done.await;
                let _ = after_tx.send(()).await;
                Ok(())
            }
        };

        let spec = worker::Spec::new_with_start("child1", routine);

        let ctx = Context::new();
        let result = spec.start(&ctx, "root").await;
        let worker = result.expect("expecting successful worker creation");

        let _ = worker.terminate().await;
        after_rx.recv().await;
    }

    #[tokio::test]
    async fn test_worker_start_failure() {
        let spec = start_err_worker("child1", "boom");

        let ctx = Context::new();
        let result = spec.start(&ctx, "root").await;
        match result {
            Err(start_err1) => assert_eq!("boom", format!("{:?}", start_err1)),
            Ok(_) => panic!("expecting error, got result"),
        }
    }
}
