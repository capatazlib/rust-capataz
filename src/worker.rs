use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::{AbortHandle, Abortable, Aborted, BoxFuture, Future, FutureExt};
use tokio::sync::oneshot;
use tokio::task::{self, JoinError, JoinHandle};
use tokio::time;

use crate::context::Context;

// TODO: replace with OnceCell
lazy_static! {
    static ref WORKER_START_TIMEOUT: Duration = Duration::from_secs(1);
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TerminationTimeout {
    Infinity,
    Timeout(Duration),
}

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

#[derive(Debug)]
pub enum TerminationError {
    TimeoutError(time::Elapsed),
    CancelError(JoinError),
    PanicError(JoinError),
    KillError(Aborted),
    ClientError(anyhow::Error),
}

impl std::fmt::Display for TerminationError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TerminationError::*;
        match self {
            TimeoutError(_) => write!(fmt, "worker routine termination timeout"),
            CancelError(_) => write!(fmt, "worker routine canceled"),
            PanicError(_) => write!(fmt, "worker routine terminated by panic"),
            KillError(_) => write!(fmt, "worker routine terminated by hard kill"),
            ClientError(_) => write!(fmt, "worker routine finished with error"),
        }
    }
}

impl std::error::Error for TerminationError {}

/// StartNotifier offers a convenient way to notify a worker spawner (a
/// Supervisor in the general case) that the worker got started or that it
/// failed to start.
pub struct StartNotifier(Box<dyn FnOnce(Result<(), anyhow::Error>) + Send>);

impl StartNotifier {
    fn from_oneshot(sender: oneshot::Sender<Result<(), anyhow::Error>>) -> Self {
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
    start_timeout: Duration,
    termination_timeout: TerminationTimeout,
    restart: Restart,
    shutdown: Shutdown,
    routine:
        Box<dyn FnMut(Context, StartNotifier) -> BoxFuture<'static, Result<(), anyhow::Error>>>,
}

/// Worker represents a routine that got started from a `worker::Spec`. It
/// contains metadata and also offers an API to perform graceful and forceful
/// termination
pub struct Worker {
    spec: Spec,
    runtime_name: String,
    created_at: DateTime<Utc>,
    join_handle: JoinHandle<Result<anyhow::Result<()>, Aborted>>,
    termination_handle: AbortHandle,
    kill_handle: AbortHandle,
}

impl Spec {
    /// new creates a worker routine spec. It requires two arguments, a name
    /// that is used for runtime tracing and a start function that returns a
    /// routine that is going to be executed in a Future task concurrently.
    ///
    /// ### The name argument
    ///
    /// A name argument must not be empty nor contain forward slash characters.
    /// It is used for runtime telemetry.
    ///
    /// ### The start function
    ///
    /// This function is where your business logic should be located; it will be
    /// running on a new supervised routine.
    ///
    /// The start function will receive a Context record that **must** be used
    /// inside your business logic to accept stop signals from the supervisor
    /// that manages this routine life-cycle.
    ///
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
            start_timeout: *WORKER_START_TIMEOUT,
            termination_timeout: TerminationTimeout::Infinity,
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
    /// otherwise the parent supervisor will fail fast.
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
            start_timeout: *WORKER_START_TIMEOUT,
            termination_timeout: TerminationTimeout::Infinity,
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

    /// start_timeout is a builder method that modifies the timeout for starting
    /// a worker routine. Defaults to 1 second.
    pub fn start_timeout(mut self, timeout: Duration) -> Self {
        self.start_timeout = timeout;
        self
    }

    /// termination_timeout is a builder method that modifies the timeout for
    /// terminating a worker routine. Defaults to infinity.
    pub fn termination_timeout(mut self, dur: Duration) -> Self {
        self.termination_timeout = TerminationTimeout::Timeout(dur);
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
        // TODO: pending parent channel to notify errors that happened
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

        // TODO: decorate routine to wait for client routine result, and

        // if it is an error, send the error to the supervisor through
        // the given channel
        // SPAWN -- actually spawn concurrent worker future
        let join_handle = task::spawn(routine);

        // ON_START blocking -- block before moving to the next start
        let err = time::timeout(self.start_timeout, started_rx).await;

        match err {
            // On worker initialization failed, we need to signal
            // this to starter
            Err(start_timeout_err) => Err(anyhow::Error::new(start_timeout_err)),

            Ok(Err(start_notify_not_invoked_err)) => {
                Err(anyhow::Error::new(start_notify_not_invoked_err))
            }

            Ok(Ok(Err(routine_err))) => Err(routine_err),

            // Happy path
            Ok(Ok(Ok(_))) => Ok(Worker {
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
    pub async fn terminate(self) -> (Spec, Option<Arc<TerminationError>>) {
        self.termination_handle.abort();

        let result = match self.spec.termination_timeout {
            TerminationTimeout::Infinity => {
                let result = self.join_handle.await;
                Ok(result)
            }
            TerminationTimeout::Timeout(ref termination_timeout) => {
                time::timeout(*termination_timeout, self.join_handle).await
            }
        };

        match result {
            Err(termination_timeout_err) => {
                self.kill_handle.abort();
                (
                    self.spec,
                    Some(Arc::new(TerminationError::TimeoutError(
                        termination_timeout_err,
                    ))),
                )
            }

            //
            Ok(Err(join_handle_err)) => {
                if join_handle_err.is_panic() {
                    (
                        self.spec,
                        Some(Arc::new(TerminationError::PanicError(join_handle_err))),
                    )
                } else {
                    (
                        self.spec,
                        Some(Arc::new(TerminationError::CancelError(join_handle_err))),
                    )
                }
            }

            Ok(Ok(Err(kill_err))) => (
                self.spec,
                Some(Arc::new(TerminationError::KillError(kill_err))),
            ),

            Ok(Ok(Ok(Err(termination_err)))) => (
                self.spec,
                Some(Arc::new(TerminationError::ClientError(termination_err))),
            ),

            // happy path
            Ok(Ok(Ok(Ok(_)))) => (self.spec, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time;

    use crate::context::*;
    use crate::worker::{self, StartNotifier};

    fn start_err_worker(name: &str, err_msg: &'static str) -> worker::Spec {
        worker::Spec::new_with_start(name, move |_: Context, start: StartNotifier| async move {
            start.failed(anyhow::Error::msg(err_msg.to_owned()));
            Err(anyhow::Error::msg("should not see this"))
        })
    }

    fn start_timeout_worker(name: &str, start_timeout: Duration, delay: Duration) -> worker::Spec {
        let spec = worker::Spec::new_with_start(
            name,
            move |_: Context, start: StartNotifier| async move {
                // we want to advance time so that timeout on the caller side expires
                time::delay_for(delay).await;
                start.success();
                Err(anyhow::Error::msg("should not see this"))
            },
        );
        spec.start_timeout(start_timeout)
    }

    fn no_start_worker(name: &str) -> worker::Spec {
        worker::Spec::new_with_start(name, move |ctx: Context, _: StartNotifier| async move {
            // we want to advance time so that timeout on the caller side expires
            ctx.done.await;
            Err(anyhow::Error::msg("should not see this"))
        })
    }

    fn termination_success_worker(name: &str) -> worker::Spec {
        worker::Spec::new(name, move |ctx: Context| async move {
            ctx.done.await;
            Ok(())
        })
    }

    fn termination_failed_worker(name: &str) -> worker::Spec {
        worker::Spec::new(name, move |ctx: Context| async move {
            ctx.done.await;
            Err(anyhow::Error::msg("termination_failed_worker"))
        })
    }

    fn termination_timeout_worker(
        name: &str,
        termination_timeout: Duration,
        delay: Duration,
    ) -> worker::Spec {
        let spec = worker::Spec::new(name, move |ctx: Context| async move {
            ctx.done.await;
            time::delay_for(delay).await;
            Err(anyhow::Error::msg("termination_timeout_worker"))
        });
        spec.termination_timeout(termination_timeout)
    }

    fn panic_worker(name: &str, panic_msg0: &str) -> worker::Spec {
        let panic_msg1 = panic_msg0.to_owned();
        worker::Spec::new(name, move |_: Context| {
            let panic_msg = panic_msg1.clone();
            async move {
                panic!(panic_msg);
            }
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
        let (_spec, stop_err) = worker.terminate().await;
        assert!(stop_err.is_none());
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

    #[tokio::test]
    async fn test_worker_start_timeout_failure() {
        time::pause();

        let spec = start_timeout_worker(
            "child1",
            Duration::from_secs(1), // timeout
            Duration::from_secs(3), // delay
        );

        let ctx = Context::new();
        let worker_fut = spec.start(&ctx, "root");

        time::advance(Duration::from_secs(4)).await;

        let result = worker_fut.await;

        match result {
            Err(start_timeout_err) => {
                // TODO: create well-defined capataz error that describes the error
                // succintly
                assert_eq!("deadline has elapsed", format!("{:?}", start_timeout_err))
            }
            Ok(_) => panic!("expecting error, got result"),
        }
    }

    #[tokio::test]
    async fn test_worker_start_not_invoked() {
        let spec = no_start_worker("child1");

        let ctx = Context::new();
        let result = spec.start(&ctx, "root").await;

        match result {
            Err(start_not_called_err) => {
                // TODO: create well-defined capataz error that describes the error
                // succintly
                assert_eq!("channel closed", format!("{:?}", start_not_called_err))
            }
            Ok(_) => panic!("expecting error, got result"),
        }
    }

    #[tokio::test]
    async fn test_worker_termination_with_panic() {
        let spec = panic_worker("child1", "panic boom");
        let ctx = Context::new();
        let worker = spec.start(&ctx, "root").await.expect("should match worker");

        let (_, result) = worker.terminate().await;

        match result {
            Some(panic_err) => {
                // TODO: create well-defined capataz error that describes the error
                // succintly
                assert_eq!(
                    "PanicError(JoinError::Panic(...))",
                    format!("{:?}", panic_err)
                )
            }
            None => panic!("expecting error, got result"),
        }
    }

    #[tokio::test]
    async fn test_worker_termination_success() {
        let spec = termination_success_worker("child1");

        let ctx = Context::new();
        let worker = spec
            .start(&ctx, "root")
            .await
            .expect("worker should be returned");

        let (_, result) = worker.terminate().await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_worker_termination_failure() {
        let spec = termination_failed_worker("child1");

        let ctx = Context::new();
        let worker = spec
            .start(&ctx, "root")
            .await
            .expect("worker should be returned");

        let (_, result) = worker.terminate().await;

        assert!(result.is_some());
        assert_eq!(
            "ClientError(termination_failed_worker)",
            format!("{:?}", result.unwrap())
        );
    }

    #[tokio::test]
    async fn test_worker_termination_timeout_failure() {
        time::pause();

        let spec = termination_timeout_worker(
            "child1",
            Duration::from_secs(1), // timeout
            Duration::from_secs(3), // delay
        );

        let ctx = Context::new();
        let worker = spec
            .start(&ctx, "root")
            .await
            .expect("should return worker");

        let termination_fut = worker.terminate();

        time::advance(Duration::from_secs(4)).await;

        let (_, result) = termination_fut.await;

        match result {
            Some(termination_timeout_err) => {
                // TODO: create well-defined capataz error that describes the error
                // succintly
                assert_eq!(
                    "TimeoutError(Elapsed(()))",
                    format!("{:?}", termination_timeout_err)
                );
                // NOTE: not sure how to validate the abort logic got executed,
                // and what the side-effects of those are. @RadicalZephyr, if
                // you have any ideas, I'm all ears.
            }
            None => panic!("expecting error, got none"),
        }
    }
}
